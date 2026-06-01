use super::*;

impl ViewManager {
    /// Find all shared views affected by a change at the given path.
    /// This is the key optimization: returns shared views instead of per-client views.
    pub fn find_affected_shared_views(
        &self,
        changed_path: &str,
        is_volatile: bool,
    ) -> Vec<&SharedView> {
        let mut result = Vec::new();
        let mut seen = HashSet::new();

        // Walk up the path tree by truncating at each '/' — zero allocations.
        // e.g. "/users/alice/score" → "/users/alice" → "/users" → "/"
        let path = changed_path.trim_end_matches('/');
        let mut current = path;
        loop {
            let lookup: &str = if current.is_empty() { "/" } else { current };
            if let Some(view_keys) = self.by_path.get(lookup) {
                for view_key in view_keys {
                    if seen.insert(view_key.clone())
                        && let Some(shared_view) = self.shared_views.get(view_key)
                    {
                        result.push(shared_view);
                    }
                }
            }
            if lookup == "/" {
                break;
            }
            // Move up: find the last '/' and truncate
            match current.rfind('/') {
                Some(0) | None => {
                    current = "";
                } // next iteration checks "/"
                Some(pos) => {
                    current = &path[..pos];
                }
            }
        }

        // Also find views to descendants (they are affected too).
        // BTreeMap range scan starting just past "{path}/", breaking when keys no longer match.
        // Skip for volatile writes - volatile paths are typically leaf nodes.
        if !is_volatile {
            use std::ops::Bound;
            for (view_path, view_keys) in self
                .by_path
                .range::<str, _>((Bound::Excluded(path), Bound::Unbounded))
            {
                // Check if view_path starts with "{path}/" — if not, we're past all descendants
                if !(view_path.len() > path.len()
                    && view_path.starts_with(path)
                    && view_path.as_bytes()[path.len()] == b'/')
                {
                    break;
                }
                for view_key in view_keys {
                    if seen.insert(view_key.clone())
                        && let Some(shared_view) = self.shared_views.get(view_key)
                    {
                        result.push(shared_view);
                    }
                }
            }
        }

        result
    }

    /// Check if `descendant` is a descendant of `ancestor`.
    /// Send events directly to subscribers without creating ClientEvent objects.
    /// This is the optimized path for high-fanout scenarios (100k+ subscribers).
    ///
    /// Returns the number of events sent.
    ///
    /// OPTIMIZATION: Instead of creating Vec<ClientEvent> and then iterating to send,
    /// we generate the message once, encode once, and send directly to each subscriber's
    /// stored connection. This eliminates:
    /// - 100k message clones
    /// - 100k ClientEvent allocations/deallocations
    /// - 100k HashMap lookups
    pub fn send_events(&mut self, event: &MutationEvent, tree: &Tree) -> usize {
        let shared_views = self.find_affected_shared_views(&event.path, event.volatile);
        if shared_views.is_empty() {
            return 0;
        }

        let mut total_sent = 0;

        // Collect shared view info (can't hold refs while mutating)
        let view_infos: Vec<_> = shared_views
            .iter()
            .map(|v| {
                (
                    v.path.clone(),
                    v.query_id.clone(),
                    v.has_query(),
                    v.is_volatile,
                )
            })
            .collect();

        for (view_path, query_id, has_query, is_volatile) in view_infos {
            // For non-query views, generate and send directly
            if !has_query {
                total_sent += self.send_events_for_shared_view(
                    &view_path,
                    &query_id,
                    event,
                    tree,
                    is_volatile,
                );
            } else {
                // For query views, use optimized send with tag prefix insertion
                total_sent += self.send_events_for_query_shared_view(
                    &view_path,
                    &query_id,
                    event,
                    tree,
                    is_volatile,
                );
            }
        }

        total_sent
    }

    /// Collect info about affected views without processing them.
    /// Used for batched processing with yields between batches.
    pub fn collect_affected_view_infos(&self, event: &MutationEvent) -> Vec<AffectedViewInfo> {
        let shared_views = self.find_affected_shared_views(&event.path, event.volatile);
        shared_views
            .iter()
            .map(|v| AffectedViewInfo {
                path: v.path.clone(),
                query_id: v.query_id.clone(),
                has_query: v.has_query(),
                is_volatile: v.is_volatile,
            })
            .collect()
    }

    /// Send events for a batch of affected views.
    /// Returns the number of events sent.
    pub fn send_events_for_views(
        &mut self,
        view_infos: &[AffectedViewInfo],
        event: &MutationEvent,
        tree: &Tree,
    ) -> usize {
        let mut total_sent = 0;

        for info in view_infos {
            if !info.has_query {
                total_sent += self.send_events_for_shared_view(
                    &info.path,
                    &info.query_id,
                    event,
                    tree,
                    info.is_volatile,
                );
            } else {
                total_sent += self.send_events_for_query_shared_view(
                    &info.path,
                    &info.query_id,
                    event,
                    tree,
                    info.is_volatile,
                );
            }
        }

        total_sent
    }

    /// Send events directly for a shared non-query view.
    /// Returns the number of events sent.
    ///
    /// Uses fast string-concatenation encoding to avoid JSON serialization overhead.
    /// The value is serialized once, then formats are generated
    /// via cheap string concatenation.
    fn send_events_for_shared_view(
        &self,
        view_path: &str,
        query_id: &str,
        event: &MutationEvent,
        tree: &Tree,
        is_volatile: bool,
    ) -> usize {
        let view_key = ViewKey::new(view_path, query_id);
        let shared_view = match self.shared_views.get(&view_key) {
            Some(v) => v,
            None => return 0,
        };

        let view_segments: Vec<&str> = view_path.trim_matches('/').split('/').collect();
        let mutation_segments: Vec<&str> = event.path.trim_matches('/').split('/').collect();

        let view_len = if view_segments.len() == 1 && view_segments[0].is_empty() {
            0
        } else {
            view_segments.len()
        };
        let mutation_len = if mutation_segments.len() == 1 && mutation_segments[0].is_empty() {
            0
        } else {
            mutation_segments.len()
        };

        // Determine event type, relative path, and value
        let (event_type, relative_path, value): (&str, String, Value) = if view_len > mutation_len {
            // View is below the mutation path (view is a descendant)
            let view_path_obj = Path::parse(view_path);
            let value = tree.get_value(&view_path_obj).unwrap_or(Value::Null);
            ("put", "/".to_string(), value)
        } else {
            // Check if view path is prefix of mutation path
            let is_prefix = view_len == 0
                || (mutation_len >= view_len && {
                    let view_segs = if view_len == 0 {
                        &[] as &[&str]
                    } else {
                        &view_segments[..view_len]
                    };
                    let mut_segs = &mutation_segments[..view_len];
                    view_segs == mut_segs
                });

            if !is_prefix {
                return 0;
            }

            let remaining_segments: Vec<&str> = if view_len == 0 {
                mutation_segments.clone()
            } else {
                mutation_segments[view_len..].to_vec()
            };

            // For update operations with Updates map, use patch
            if event.mutation_type == "update" {
                if let Some(ref updates) = event.updates {
                    let prefix = if remaining_segments.is_empty() {
                        "/".to_string()
                    } else {
                        format!("/{}/", remaining_segments.join("/"))
                    };

                    let mut patch_values = Map::new();
                    for (update_path, update_value) in updates {
                        let full_path = format!("{}{}", prefix, update_path);
                        patch_values.insert(full_path, update_value.clone());
                    }

                    ("patch", "/".to_string(), Value::Object(patch_values))
                } else {
                    // Fallback to put
                    let relative_path = if remaining_segments.is_empty() {
                        "/".to_string()
                    } else {
                        format!("/{}", remaining_segments.join("/"))
                    };
                    let value = if event.mutation_type == "remove" {
                        Value::Null
                    } else {
                        event.new_value.clone().unwrap_or(Value::Null)
                    };
                    ("put", relative_path, value)
                }
            } else {
                // For other operations, send PUT
                let relative_path = if remaining_segments.is_empty() {
                    "/".to_string()
                } else {
                    format!("/{}", remaining_segments.join("/"))
                };
                let value = if event.mutation_type == "remove" {
                    Value::Null
                } else {
                    event.new_value.clone().unwrap_or(Value::Null)
                };
                ("put", relative_path, value)
            }
        };

        // Serialize value ONCE
        let value_bytes = match serde_json::to_vec(&value) {
            Ok(bytes) => bytes,
            Err(_) => return 0,
        };

        // Generate Lark base bytes (no tag for simple views)
        let lark_base: Vec<u8> = ServerMessage::encode_event_fast(
            event_type,
            view_path,
            &relative_path,
            &value_bytes,
            None, // No tag for simple views
            is_volatile,
        );

        // Lazy-init Firebase base bytes on first Firebase subscriber
        let mut firebase_base: Option<Vec<u8>> = None;

        // Use thread-local broadcast buffers for single-pass payload building

        with_broadcast_buffers(|buffers| {
            let mut direct_sent = 0;
            let reliable = !is_volatile;

            for subscriber in shared_view.subscribers.values() {
                let is_firebase = subscriber.is_firebase;

                if is_firebase {
                    // Firebase client - use or generate Firebase format
                    let fb_bytes = firebase_base.get_or_insert_with(|| {
                        encode_firebase_event(
                            event_type,
                            view_path,
                            &relative_path,
                            &value_bytes,
                            None, // No tag for simple views
                        )
                    });

                    // Check if chunking is needed (Firebase + >16KB)
                    if fb_bytes.len() > FIREBASE_MAX_FRAME_SIZE {
                        // Fall back to direct send (handles chunking)
                        if subscriber
                            .conn
                            .try_send(fb_bytes.clone().into(), is_volatile, true)
                            .is_ok()
                        {
                            direct_sent += 1;
                        }
                        continue;
                    }
                }

                // Add client directly to broadcast buffer (single pass - no intermediate Vec)
                // Use cached values to avoid virtual dispatch overhead
                let outbox_id = subscriber.cached_outbox_id;
                let client_id = subscriber.cached_client_id;
                let key = (outbox_id, is_firebase);

                buffers
                    .entry(key)
                    .or_insert_with(BroadcastBuffer::new)
                    .add_client(client_id, 0, &subscriber.conn, reliable); // Tag = 0 for simple views
            }

            // Send BROADCAST for each buffer
            let mut broadcast_sent = 0;
            for ((_, is_firebase), buffer) in buffers.iter_mut() {
                if buffer.is_empty() {
                    continue;
                }

                // Build flags
                let mut flags: u8 = 0;
                if buffer.has_reliable {
                    flags |= broadcast_flag::RELIABLE;
                }
                if *is_firebase {
                    flags |= broadcast_flag::FIREBASE_FORMAT;
                }

                // Get the message bytes for this group
                let message = if *is_firebase {
                    firebase_base.as_ref().unwrap().as_slice()
                } else {
                    lark_base.as_slice()
                };

                broadcast_sent += buffer.send(message, flags);
            }

            direct_sent + broadcast_sent
        })
    }

    /// Send events directly for a shared query view.
    /// Uses fast string-concat encoding and tag insertion to avoid per-subscriber overhead.
    /// Returns the number of events sent.
    fn send_events_for_query_shared_view(
        &mut self,
        view_path: &str,
        query_id: &str,
        event: &MutationEvent,
        tree: &Tree,
        is_volatile: bool,
    ) -> usize {
        let view_key = ViewKey::new(view_path, query_id);

        // Get first subscriber's client_id for generate_events_for_view (only 1 String clone)
        let first_client_id = {
            let shared_view = match self.shared_views.get(&view_key) {
                Some(v) => v,
                None => return 0,
            };
            match shared_view.subscribers.iter().next() {
                Some((cid, _)) => cid.clone(),
                None => return 0,
            }
        };

        // Generate events using the first subscriber as "representative"
        // This handles the query state update (ordered_keys, etc.)
        let base_events =
            self.generate_events_for_view(&first_client_id, view_path, query_id, event, tree);

        if base_events.is_empty() {
            return 0;
        }

        let mut total_sent = 0;

        for base_event in &base_events {
            // Extract event components from ServerMessage
            let event_type = base_event.message.event.as_deref().unwrap_or("put");
            let subscription_path = base_event
                .message
                .subscription_path
                .as_deref()
                .unwrap_or("");
            let relative_path = base_event.message.path.as_deref().unwrap_or("/");

            // Serialize value ONCE (directly from MessageValue/ArcValue, no intermediate clone)
            let value_bytes = match base_event.message.value.as_ref() {
                Some(v) => match serde_json::to_vec(v) {
                    Ok(bytes) => bytes,
                    Err(_) => continue,
                },
                None => b"null".to_vec(),
            };

            // Generate Lark base bytes WITHOUT tag (we'll prepend tags per-subscriber)
            let lark_base: Vec<u8> = ServerMessage::encode_event_fast(
                event_type,
                subscription_path,
                relative_path,
                &value_bytes,
                None, // No tag - will be added per-subscriber
                is_volatile,
            );

            // Lazy-init Firebase base bytes on first Firebase subscriber
            let mut firebase_base: Option<Vec<u8>> = None;

            // Re-borrow shared_view to iterate subscribers directly (no Vec allocation)
            let shared_view = match self.shared_views.get(&view_key) {
                Some(v) => v,
                None => return total_sent,
            };

            // Use thread-local broadcast buffers for single-pass payload building
            let event_sent = with_broadcast_buffers(|buffers| {
                let mut direct_sent = 0;
                let reliable = !is_volatile;

                // For each subscriber, add to broadcast buffer with their tag
                for subscriber in shared_view.subscribers.values() {
                    let is_firebase = subscriber.is_firebase;

                    if is_firebase {
                        // Firebase client
                        let fb_base = firebase_base.get_or_insert_with(|| {
                            encode_firebase_event(
                                event_type,
                                subscription_path,
                                relative_path,
                                &value_bytes,
                                None, // No tag - proxy will insert per-subscriber
                            )
                        });

                        // Check if chunking is needed (Firebase + >16KB)
                        if fb_base.len() > FIREBASE_MAX_FRAME_SIZE {
                            // Fall back to direct send (handles chunking)
                            let encoded: Bytes = if let Some(t) = subscriber.tag {
                                insert_firebase_tag(fb_base, t).into()
                            } else {
                                fb_base.clone().into()
                            };
                            if subscriber.conn.try_send(encoded, is_volatile, true).is_ok() {
                                direct_sent += 1;
                            }
                            continue;
                        }
                    }

                    // Add client to broadcast buffer with tag
                    // Tag = 0 means no tag modification, otherwise proxy inserts tag
                    // Use cached values to avoid virtual dispatch overhead
                    let outbox_id = subscriber.cached_outbox_id;
                    let client_id = subscriber.cached_client_id;
                    let tag = subscriber.tag.unwrap_or(0);
                    let key = (outbox_id, is_firebase);

                    buffers
                        .entry(key)
                        .or_insert_with(BroadcastBuffer::new)
                        .add_client(client_id, tag, &subscriber.conn, reliable);
                }

                // Send BROADCAST for each buffer
                let mut broadcast_sent = 0;
                for ((_, is_firebase), buffer) in buffers.iter_mut() {
                    if buffer.is_empty() {
                        continue;
                    }

                    // Build flags
                    let mut flags: u8 = 0;
                    if buffer.has_reliable {
                        flags |= broadcast_flag::RELIABLE;
                    }
                    if *is_firebase {
                        flags |= broadcast_flag::FIREBASE_FORMAT;
                    }

                    // Get the message bytes for this group (without tags - proxy inserts them)
                    let message = if *is_firebase {
                        firebase_base.as_ref().unwrap().as_slice()
                    } else {
                        lark_base.as_slice()
                    };

                    broadcast_sent += buffer.send(message, flags);
                }

                direct_sent + broadcast_sent
            });

            total_sent += event_sent;
        }

        total_sent
    }

    /// Generate events for a single view.
    /// This is used for query views which need per-view state management.
    fn generate_events_for_view(
        &mut self,
        client_id: &str,
        view_path: &str,
        query_id: &str,
        event: &MutationEvent,
        tree: &Tree,
    ) -> Vec<ClientEvent> {
        let view_segments: Vec<&str> = view_path.trim_matches('/').split('/').collect();
        let mutation_segments: Vec<&str> = event.path.trim_matches('/').split('/').collect();

        // Get view info from shared view
        let (has_query, is_writer_echo, tag) = {
            let view = match self.get_view(client_id, view_path, query_id) {
                Some(v) => v,
                None => return Vec::new(),
            };
            let is_echo = event.writer_client_id.as_deref() == Some(view.client_id());
            (view.has_query(), is_echo, view.tag())
        };

        // Check if view path is a prefix of mutation path
        let view_len = if view_segments.len() == 1 && view_segments[0].is_empty() {
            0
        } else {
            view_segments.len()
        };
        let mutation_len = if mutation_segments.len() == 1 && mutation_segments[0].is_empty() {
            0
        } else {
            mutation_segments.len()
        };

        if view_len > mutation_len {
            // View is below the mutation path (view is a descendant)
            // Get the current value at the view's path
            let view_path_obj = Path::parse(view_path);
            let value = tree.get_value(&view_path_obj).unwrap_or(Value::Null);

            let mut msg = ServerMessage::put_event(view_path, "/", value, event.volatile);
            if let Some(t) = tag {
                msg.tag = Some(t);
            }

            let ev = ClientEvent::new(
                client_id.to_string(),
                view_path.to_string(),
                query_id.to_string(),
                msg,
                event.volatile,
                EventCategory::Initial,
            );

            return vec![ev];
        }

        // Check if view path is prefix of mutation path
        let is_prefix = view_len == 0
            || (mutation_len >= view_len && {
                let view_segs = if view_len == 0 {
                    &[] as &[&str]
                } else {
                    &view_segments[..view_len]
                };
                let mut_segs = &mutation_segments[..view_len];
                view_segs == mut_segs
            });

        if !is_prefix {
            return Vec::new();
        }

        // Remaining segments after view path
        let remaining_segments: Vec<&str> = if view_len == 0 {
            mutation_segments.clone()
        } else {
            mutation_segments[view_len..].to_vec()
        };

        // For non-query views, just send the delta
        if !has_query {
            return self.generate_simple_view_event(
                client_id,
                view_path,
                query_id,
                event,
                &remaining_segments,
                is_writer_echo,
                tag,
            );
        }

        // For query views, handle complex logic
        self.generate_query_view_events(
            client_id,
            view_path,
            query_id,
            event,
            &remaining_segments,
            tree,
            is_writer_echo,
            tag,
        )
    }

    /// Generate a simple delta event for a non-query view.
    #[allow(clippy::too_many_arguments)]
    fn generate_simple_view_event(
        &self,
        client_id: &str,
        view_path: &str,
        query_id: &str,
        event: &MutationEvent,
        remaining_segments: &[&str],
        _is_writer_echo: bool,
        tag: Option<i32>,
    ) -> Vec<ClientEvent> {
        // Verify view exists
        if self.get_view(client_id, view_path, query_id).is_none() {
            return Vec::new();
        }

        // Determine event category
        let category = self.determine_simple_event_category(event, remaining_segments);

        let relative_path = if remaining_segments.is_empty() {
            "/".to_string()
        } else {
            format!("/{}", remaining_segments.join("/"))
        };

        // For update operations with Updates map, send PATCH
        if event.mutation_type == "update"
            && let Some(ref updates) = event.updates
        {
            let prefix = if remaining_segments.is_empty() {
                "/".to_string()
            } else {
                format!("/{}/", remaining_segments.join("/"))
            };

            let mut patch_values = Map::new();
            for (update_path, update_value) in updates {
                let full_path = format!("{}{}", prefix, update_path);
                patch_values.insert(full_path, update_value.clone());
            }

            let mut msg = ServerMessage::patch_event(view_path, "/", patch_values, event.volatile);
            if let Some(t) = tag {
                msg.tag = Some(t);
            }

            let ev = ClientEvent::new(
                client_id.to_string(),
                view_path.to_string(),
                query_id.to_string(),
                msg,
                event.volatile,
                category,
            );

            return vec![ev];
        }

        // For other operations, send PUT
        let value = if event.mutation_type == "remove" {
            Value::Null
        } else {
            event.new_value.clone().unwrap_or(Value::Null)
        };

        let mut msg = ServerMessage::put_event(view_path, &relative_path, value, event.volatile);
        if let Some(t) = tag {
            msg.tag = Some(t);
        }

        let ev = ClientEvent::new(
            client_id.to_string(),
            view_path.to_string(),
            query_id.to_string(),
            msg,
            event.volatile,
            category,
        );

        vec![ev]
    }

    /// Determine the event category for a simple view event.
    fn determine_simple_event_category(
        &self,
        event: &MutationEvent,
        remaining_segments: &[&str],
    ) -> EventCategory {
        // Mutation AT the subscription path
        if remaining_segments.is_empty() {
            return EventCategory::Initial;
        }

        let is_direct_child = remaining_segments.len() == 1;

        match event.mutation_type.as_str() {
            "remove" => EventCategory::Remove,
            "update" => EventCategory::Change,
            "set" | "push" => {
                // set(null) at direct child level is a removal
                if is_direct_child && event.new_value.as_ref().is_none_or(|v| v.is_null()) {
                    EventCategory::Remove
                } else if is_direct_child {
                    // Direct child set - check old_value to determine add vs change
                    if event.old_value.is_none()
                        || event.old_value.as_ref().is_some_and(|v| v.is_null())
                    {
                        EventCategory::Add
                    } else {
                        EventCategory::Change
                    }
                } else {
                    // Nested set - treat as change (modifying existing data)
                    EventCategory::Change
                }
            }
            _ => EventCategory::Change,
        }
    }
}
