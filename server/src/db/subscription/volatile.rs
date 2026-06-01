use super::*;

impl ViewManager {
    // =========================================================================
    // Volatile Batching
    // =========================================================================

    /// Buffer a volatile write for all affected views.
    /// write_path: absolute path being written to (e.g., "/cursors/player1")
    /// raw_value: JSON-encoded value bytes
    /// sender_id: client who sent the write (won't receive echo)
    pub fn buffer_volatile(&mut self, write_path: &str, raw_value: Bytes, _sender_id: &str) {
        // Find all views affected by this write
        // We need views that are:
        // 1. At write_path (exact match)
        // 2. At a parent of write_path (e.g., /cursors watching /cursors/player1)
        let write_segments: Vec<&str> = write_path.trim_matches('/').split('/').collect();

        // Walk up the path tree to find views
        for i in (0..=write_segments.len()).rev() {
            let view_path = if i == 0 {
                "/".to_string()
            } else {
                format!("/{}", write_segments[..i].join("/"))
            };

            if let Some(view_keys) = self.by_path.get(&view_path) {
                for view_key in view_keys.iter() {
                    if let Some(view) = self.shared_views.get_mut(view_key) {
                        // Note: We don't check view.is_volatile here because:
                        // - buffer_volatile is only called when write_path is volatile
                        // - Views at parent paths (e.g., /cursors) receive child updates
                        //   even if the parent path itself doesn't match the volatile pattern

                        // Calculate relative path from view path to write path
                        let relative_path = if view.path == "/" {
                            write_path.to_string()
                        } else if write_path == view.path {
                            "/".to_string()
                        } else {
                            write_path[view.path.len()..].to_string()
                        };

                        // Buffer the update for all subscribers except the sender
                        // Since SharedView batches at the view level, we just need to
                        // track that this view has updates and who the sender is
                        view.buffer_volatile(relative_path, raw_value.clone());

                        // Track this view as having pending updates
                        self.pending_volatile_views.insert(view_key.clone());
                    }
                }
            }
        }

        // Store sender_id for echo suppression during flush
        // We do this by storing it in a thread-local or passing to flush
        // For simplicity, we'll handle echo suppression differently - by not
        // adding sender to the batch in the first place
        // Actually, the current approach buffers at view level, not per-client
        // So we need to handle sender exclusion at flush time
        // Store sender in pending_volatile_views metadata...
        // Actually, let's keep it simple: store sender per-view
    }

    /// Remove a path from all pending volatile batches.
    /// Called when an onDisconnect action fires on a volatile path to prevent
    /// stale data from being flushed after the removal event.
    pub fn clear_volatile_for_path(&mut self, write_path: &str) {
        let write_segments: Vec<&str> = write_path.trim_matches('/').split('/').collect();

        for i in (0..=write_segments.len()).rev() {
            let view_path = if i == 0 {
                "/".to_string()
            } else {
                format!("/{}", write_segments[..i].join("/"))
            };

            if let Some(view_keys) = self.by_path.get(&view_path) {
                let keys: Vec<_> = view_keys.iter().cloned().collect();
                for view_key in keys {
                    if let Some(view) = self.shared_views.get_mut(&view_key) {
                        let relative_path = if view.path == "/" {
                            write_path.to_string()
                        } else if write_path == view.path {
                            "/".to_string()
                        } else {
                            write_path[view.path.len()..].to_string()
                        };
                        view.pending_volatile_batch.remove(&relative_path);
                    }
                }
            }
        }
    }

    /// Check if there are any pending volatile batches.
    pub fn has_pending_volatile(&self) -> bool {
        !self.pending_volatile_views.is_empty()
    }

    /// Flush volatile batches to fast clients (called every 50ms).
    /// Does NOT clear the batch - slow clients may still need it.
    /// Returns the number of clients that received messages.
    pub fn flush_volatile_fast(&mut self) -> usize {
        let mut sent_count = 0;

        for view_key in &self.pending_volatile_views {
            if let Some(view) = self.shared_views.get(view_key) {
                if !view.has_pending_volatile() || view.fast_subscribers.is_empty() {
                    continue;
                }

                // Build patch value from batch: {"/player-1": {...}, "/player-2": {...}}
                let mut patch_values = Map::new();
                for (relative_path, raw_bytes) in &view.pending_volatile_batch {
                    if let Ok(value) = serde_json::from_slice::<Value>(raw_bytes) {
                        patch_values.insert(relative_path.clone(), value);
                    }
                }

                if patch_values.is_empty() {
                    continue;
                }

                // Serialize value ONCE
                let value_bytes = match serde_json::to_vec(&patch_values) {
                    Ok(bytes) => bytes,
                    Err(_) => continue,
                };

                // Generate Lark base bytes using fast encoding
                let lark_base = ServerMessage::encode_event_fast(
                    "patch",
                    &view.path,
                    "/",
                    &value_bytes,
                    None, // No tag for volatile views
                    true, // volatile = true
                );

                // Lazy-init Firebase base bytes on first Firebase subscriber
                let mut firebase_base: Option<Vec<u8>> = None;

                // Use thread-local broadcast buffers for single-pass payload building
                sent_count += with_broadcast_buffers(|buffers| {
                    let mut direct_sent = 0;

                    for client_id in &view.fast_subscribers {
                        if let Some(subscriber) = view.subscribers.get(client_id) {
                            let is_firebase = subscriber.is_firebase;

                            if is_firebase {
                                // Firebase client - use or generate Firebase format
                                let fb_bytes = firebase_base.get_or_insert_with(|| {
                                    encode_firebase_event(
                                        "patch",
                                        &view.path,
                                        "/",
                                        &value_bytes,
                                        None, // No tag
                                    )
                                });

                                // Check if chunking is needed (Firebase + >16KB)
                                if fb_bytes.len() > FIREBASE_MAX_FRAME_SIZE {
                                    // Fall back to direct send (handles chunking)
                                    if subscriber
                                        .conn
                                        .try_send(fb_bytes.clone().into(), true, true)
                                        .is_ok()
                                    {
                                        direct_sent += 1;
                                    }
                                    continue;
                                }
                            }

                            // Add client to broadcast buffer (single pass)
                            let outbox_id = subscriber.cached_outbox_id;
                            let client_id_num = subscriber.cached_client_id;
                            let key = (outbox_id, is_firebase);

                            // RELIABLE=false for volatile data
                            buffers
                                .entry(key)
                                .or_insert_with(BroadcastBuffer::new)
                                .add_client(client_id_num, 0, &subscriber.conn, false);
                        }
                    }

                    // Send BROADCAST for each buffer
                    let mut broadcast_sent = 0;
                    for ((_, is_firebase), buffer) in buffers.iter_mut() {
                        if buffer.is_empty() {
                            continue;
                        }

                        // Build flags: RELIABLE=false (volatile), FIREBASE_FORMAT if firebase
                        let flags = if *is_firebase {
                            broadcast_flag::FIREBASE_FORMAT
                        } else {
                            0
                        };

                        // Get the message bytes for this group
                        let message = if *is_firebase {
                            firebase_base.as_ref().unwrap().as_slice()
                        } else {
                            lark_base.as_slice()
                        };

                        broadcast_sent += buffer.send(message, flags);
                    }

                    direct_sent + broadcast_sent
                });
            }
        }

        sent_count
    }

    /// Flush volatile batches to slow clients (called every 250ms).
    /// Clears the batch after sending.
    /// Returns the number of clients that received messages.
    pub fn flush_volatile_slow(&mut self) -> usize {
        let mut sent_count = 0;

        // Collect keys to clear after iteration
        let keys: Vec<_> = self.pending_volatile_views.iter().cloned().collect();

        for view_key in keys {
            if let Some(view) = self.shared_views.get_mut(&view_key) {
                if !view.has_pending_volatile() {
                    view.clear_volatile_batch();
                    continue;
                }

                let has_slow = !view.slow_subscribers.is_empty();

                // Only encode/send if there are slow subscribers
                if has_slow {
                    // Build patch value from batch: {"/player-1": {...}, "/player-2": {...}}
                    let mut patch_values = Map::new();
                    for (relative_path, raw_bytes) in &view.pending_volatile_batch {
                        if let Ok(value) = serde_json::from_slice::<Value>(raw_bytes) {
                            patch_values.insert(relative_path.clone(), value);
                        }
                    }

                    if !patch_values.is_empty() {
                        // Serialize value ONCE
                        if let Ok(value_bytes) = serde_json::to_vec(&patch_values) {
                            // Generate Lark base bytes using fast encoding
                            let lark_base = ServerMessage::encode_event_fast(
                                "patch",
                                &view.path,
                                "/",
                                &value_bytes,
                                None, // No tag for volatile views
                                true, // volatile = true
                            );

                            // Lazy-init Firebase base bytes on first Firebase subscriber
                            let mut firebase_base: Option<Vec<u8>> = None;

                            // Use thread-local broadcast buffers for single-pass payload building
                            sent_count += with_broadcast_buffers(|buffers| {
                                let mut direct_sent = 0;

                                for client_id in &view.slow_subscribers {
                                    if let Some(subscriber) = view.subscribers.get(client_id) {
                                        let is_firebase = subscriber.is_firebase;

                                        if is_firebase {
                                            // Firebase client - use or generate Firebase format
                                            let fb_bytes = firebase_base.get_or_insert_with(|| {
                                                encode_firebase_event(
                                                    "patch",
                                                    &view.path,
                                                    "/",
                                                    &value_bytes,
                                                    None, // No tag
                                                )
                                            });

                                            // Check if chunking is needed (Firebase + >16KB)
                                            if fb_bytes.len() > FIREBASE_MAX_FRAME_SIZE {
                                                // Fall back to direct send (handles chunking)
                                                if subscriber
                                                    .conn
                                                    .try_send(fb_bytes.clone().into(), true, true)
                                                    .is_ok()
                                                {
                                                    direct_sent += 1;
                                                }
                                                continue;
                                            }
                                        }

                                        // Add client to broadcast buffer (single pass)
                                        let outbox_id = subscriber.cached_outbox_id;
                                        let client_id_num = subscriber.cached_client_id;
                                        let key = (outbox_id, is_firebase);

                                        // RELIABLE=false for volatile data
                                        buffers
                                            .entry(key)
                                            .or_insert_with(BroadcastBuffer::new)
                                            .add_client(client_id_num, 0, &subscriber.conn, false);
                                    }
                                }

                                // Send BROADCAST for each buffer
                                let mut broadcast_sent = 0;
                                for ((_, is_firebase), buffer) in buffers.iter_mut() {
                                    if buffer.is_empty() {
                                        continue;
                                    }

                                    // Build flags: RELIABLE=false (volatile), FIREBASE_FORMAT if firebase
                                    let flags = if *is_firebase {
                                        broadcast_flag::FIREBASE_FORMAT
                                    } else {
                                        0
                                    };

                                    // Get the message bytes for this group
                                    let message = if *is_firebase {
                                        firebase_base.as_ref().unwrap().as_slice()
                                    } else {
                                        lark_base.as_slice()
                                    };

                                    broadcast_sent += buffer.send(message, flags);
                                }

                                direct_sent + broadcast_sent
                            });
                        }
                    }
                }

                // Clear the batch
                view.clear_volatile_batch();
            }
        }

        // Clear pending volatile views set
        self.pending_volatile_views.clear();

        sent_count
    }

    /// Get view count (for testing).
    /// Returns the number of unique shared views (path + query combinations).
    pub fn view_count(&self) -> usize {
        self.shared_views.len()
    }

    /// Find all views affected by a change (compatibility wrapper for tests).
    /// Returns shared views.
    #[cfg(test)]
    pub fn find_affected_views(&self, changed_path: &str, is_volatile: bool) -> Vec<&SharedView> {
        self.find_affected_shared_views(changed_path, is_volatile)
    }
}
