use super::*;

impl Database {
    // =========================================================================
    // OnDisconnect
    // =========================================================================

    pub(super) async fn handle_on_disconnect(
        &mut self,
        client_id: &str,
        msg: &ClientMessage,
    ) -> Option<ServerMessage> {
        let request_id = msg.request_id.as_deref().unwrap_or("");
        let path_str = msg.path.as_deref().unwrap_or("/");
        let action = msg.action.as_deref().unwrap_or("s");

        match action {
            "s" | "u" | "d" => {
                // Deferred writes are applied directly to the tree + WAL on
                // disconnect (handle_disconnect), bypassing the live write
                // handlers — so the same checks must happen here, at registration:

                // 1. Path/key validity (empty/odd segments, control chars,
                //    `$ # [ ] /`, literal-slash value keys, >768-byte keys).
                let keys_ok = crate::db::validate_path(path_str).is_ok()
                    && match (action, &msg.value) {
                        ("s", Some(v)) => validate_value_keys(v).is_ok(),
                        ("u", Some(Value::Object(map))) => map.iter().all(|(k, val)| {
                            crate::db::validate_path(&format!(
                                "{}/{}",
                                path_str.trim_end_matches('/'),
                                k
                            ))
                            .is_ok()
                                && validate_value_keys(val).is_ok()
                        }),
                        _ => true,
                    };
                if !keys_ok {
                    return Some(ServerMessage::nack(
                        request_id,
                        error::INVALID_DATA,
                        "invalid path or key",
                    ));
                }

                // 1b. Total depth: path + value nesting must stay within the cap,
                //     same as the live SET/UPDATE handlers. Otherwise a deferred
                //     write could land nodes deeper than a read can address.
                let depth_ok = match (action, &msg.value) {
                    ("s", Some(v)) => {
                        crate::db::path_depth(path_str) + json_value_depth(v)
                            <= crate::db::MAX_PATH_DEPTH
                    }
                    ("u", Some(Value::Object(map))) => map.iter().all(|(k, val)| {
                        let full = format!("{}/{}", path_str.trim_end_matches('/'), k);
                        crate::db::path_depth(&full) + json_value_depth(val)
                            <= crate::db::MAX_PATH_DEPTH
                    }),
                    _ => true,
                };
                if !depth_ok {
                    return Some(ServerMessage::nack(
                        request_id,
                        error::INVALID_DATA,
                        "write exceeds maximum path depth",
                    ));
                }

                // 2. Security rules. Evaluate onDisconnect writes
                //    against rules when they're established, using the
                //    registering client's auth — do the same so a deferred write
                //    can't reach a path the client isn't allowed to write.
                let new_data = match (action, msg.value.clone()) {
                    ("s", Some(v)) => Some(NewData::from_set(path_str.to_string(), v)),
                    ("u", Some(Value::Object(map))) => {
                        Some(NewData::from_update(path_str.to_string(), map))
                    }
                    _ => None,
                };
                if !self.can_write(client_id, path_str, new_data).await {
                    self.metrics.record_permission_denial();
                    return Some(ServerMessage::nack(
                        request_id,
                        error::PERMISSION_DENIED,
                        "write permission denied",
                    ));
                }

                // Bound the per-client onDisconnect state — both action count
                // and aggregate payload bytes. These live in memory until the
                // client disconnects, so an unbounded client is an asymmetric
                // memory sink whose OOM aborts the whole core (audit M-3).
                let new_bytes = path_str.len()
                    + action.len()
                    + msg.value.as_ref().map_or(0, estimate_value_bytes);
                let (existing_count, existing_bytes) =
                    self.on_disconnect.get(client_id).map_or((0, 0), |actions| {
                        let bytes: usize = actions
                            .iter()
                            .map(|a| {
                                a.path.len()
                                    + a.action.len()
                                    + a.value.as_ref().map_or(0, estimate_value_bytes)
                            })
                            .sum();
                        (actions.len(), bytes)
                    });
                if existing_count >= MAX_ON_DISCONNECT_ACTIONS_PER_CLIENT
                    || existing_bytes + new_bytes > MAX_ON_DISCONNECT_BYTES_PER_CLIENT
                {
                    debug!(
                        "NACK {}: onDisconnect rejected for client {} — at cap ({} actions / {} bytes)",
                        self.id, client_id, existing_count, existing_bytes
                    );
                    return Some(ServerMessage::nack(
                        request_id,
                        error::PAYLOAD_TOO_LARGE,
                        &format!(
                            "onDisconnect limit reached ({} actions or {} bytes per connection)",
                            MAX_ON_DISCONNECT_ACTIONS_PER_CLIENT,
                            MAX_ON_DISCONNECT_BYTES_PER_CLIENT
                        ),
                    ));
                }

                let disconnect_action = DisconnectAction {
                    path: path_str.to_string(),
                    action: action.to_string(),
                    value: msg.value.clone(),
                };

                self.on_disconnect
                    .entry(client_id.to_string())
                    .or_default()
                    .push(disconnect_action);
            }
            "c" => {
                // Cancel - remove disconnect hooks for this path
                if let Some(actions) = self.on_disconnect.get_mut(client_id) {
                    actions.retain(|a| a.path != path_str);
                }
            }
            _ => {}
        }

        Some(ServerMessage::ack(request_id))
    }

    // =========================================================================
    // Event Broadcasting
    // =========================================================================

    /// Broadcast a mutation to all affected views.
    pub(super) async fn broadcast_mutation(
        &mut self,
        path: &str,
        mutation_type: &str,
        new_value: Option<Value>,
        updates: Option<serde_json::Map<String, Value>>,
        volatile: bool,
        writer_client_id: Option<&str>,
    ) {
        // Canonicalize `event.path` before any downstream consumer sees it.
        // `find_affected_shared_views` does raw string-prefix matching against
        // `by_path` keys (which are themselves normalized at subscribe time);
        // both sides must use the same canonical form or the prefix match
        // silently misses. Tree storage canonicalizes via `Path::parse`, so a
        // non-normalized callsite (e.g. firebase_adapter's translate_merge
        // building `"//posts/X"` from `format!("/{}", "/posts/X")` when the
        // base path is "/" and the key is `/`-prefixed would still store correctly but
        // never notify its subscribers.
        let path = crate::db::normalize_path(path);

        let event = MutationEvent {
            mutation_type: mutation_type.to_string(),
            path,
            old_value: None, // We don't track old values for now
            new_value,
            updates,
            volatile,
            writer_client_id: writer_client_id.map(|s| s.to_string()),
        };

        // OPTIMIZATION: Send events directly to subscribers without creating ClientEvent objects.
        // This eliminates:
        // - 100k message clones (in high-fanout scenarios)
        // - 100k ClientEvent allocations/deallocations
        // - 100k HashMap lookups (connections are stored in subscribers)
        //
        // Rate limiting is done at the VIEW level inside send_events.
        //
        // FAIRNESS: Process views in batches of 10, yielding between batches.
        // This prevents a database with many unique views (e.g., 200k CCU with different
        // query params) from starving other databases on the same core.
        const VIEWS_PER_BATCH: usize = 10;

        // 1. Collect affected views (quick, needs tree briefly)
        let view_infos = self.view_manager.collect_affected_view_infos(&event);

        // 2. Deep promote view paths for query views that may need to recompute.
        //    recompute_query_view reads all children from the tree, so the entire
        //    subtree must be Sentinel-free.
        for info in &view_infos {
            if info.has_query {
                let _ = self.promote_path_deep(&info.path).await;
            }
        }

        // 3. Process in batches, yielding between
        let mut event_count = 0;
        for (batch_idx, chunk) in view_infos.chunks(VIEWS_PER_BATCH).enumerate() {
            // Acquire lock only for this batch
            let batch_sent = {
                let tree = self.tree.read().unwrap();
                self.view_manager
                    .send_events_for_views(chunk, &event, &tree)
            }; // Lock released before yield
            event_count += batch_sent;

            // Yield after each batch (except the first) to allow other tasks to run
            if batch_idx > 0 {
                glommio::yield_if_needed().await;
            }
        }

        // Record events sent
        if event_count > 0 {
            self.metrics.record_events_sent(event_count as u64);
        }
    }

    pub(super) async fn send_to_client(
        &self,
        client_id: &str,
        msg: &ServerMessage,
        volatile: bool,
    ) {
        let client = match self.clients.get(client_id) {
            Some(c) => c,
            None => return,
        };

        match msg.encode() {
            Ok(data) => {
                // Record outbound bytes (not read count - this includes events, acks, etc.)
                self.metrics.record_outbound_bytes(data.len());

                // Use try_send to avoid blocking the database task if client is slow
                if let Err(e) = client.conn.try_send(data.into(), volatile, false) {
                    trace!(
                        "Failed to send to client {} (dropping message): {:?}",
                        client_id, e
                    );
                }
            }
            Err(e) => {
                // Encoding failed. The only known trigger is an ArcValue::Sentinel
                // leaking into a response, but treat this as a generic internal
                // error. If the original message was a response to a request (has
                // a request_id), convert to a NACK so the client fails fast rather
                // than waiting for a response that will never come. Pure events
                // (put/patch deltas) have no request_id — log loudly and drop.
                //
                // Diagnostic: if the message carries an ArcValue payload, walk it
                // to find the offending Sentinel's path so we know exactly which
                // node leaked. This is server-side only — the client NACK stays
                // generic.
                let sentinel_path = [&msg.value, &msg.once_value]
                    .iter()
                    .find_map(|opt| match opt.as_ref() {
                        Some(crate::protocol::MessageValue::Arc(v)) => v.find_first_sentinel_path(),
                        _ => None,
                    });
                let req_path = msg.path.as_deref().unwrap_or("");
                let req_id = msg.request_id().unwrap_or("");
                warn!(
                    "Database {} failed to encode message for client {} (req_id={}, req_path={:?}, sentinel_at={:?}): {}",
                    self.id, client_id, req_id, req_path, sentinel_path, e
                );
                if let Some(request_id) = msg.request_id() {
                    let nack =
                        ServerMessage::nack(request_id, error::INTERNAL, "Internal encoding error");
                    match nack.encode() {
                        Ok(data) => {
                            let _ = client.conn.try_send(data.into(), volatile, false);
                        }
                        Err(nack_err) => {
                            warn!(
                                "Database {} also failed to encode NACK for client {}: {}",
                                self.id, client_id, nack_err
                            );
                        }
                    }
                }
            }
        }
    }

    // =========================================================================
    // Volatile Path Helpers
    // =========================================================================

    /// Check if the current rules reference query.* variables.
    pub(super) fn rules_use_query(&self) -> bool {
        self.evaluator.as_ref().is_some_and(|e| e.uses_query())
    }

    /// Check if a path is configured as volatile.
    pub(super) fn is_volatile_path(&self, path: &str) -> bool {
        for pattern in &self.volatile_paths {
            if path_matches_pattern(path, pattern) {
                return true;
            }
        }
        false
    }

    /// Flush volatile batches for high-frequency clients (KCP/WebTransport) - 20Hz.
    pub(super) fn flush_volatile_fast(&mut self) {
        if !self.view_manager.has_pending_volatile() {
            return;
        }

        // Send directly via stored connections
        let event_count = self.view_manager.flush_volatile_fast();

        // Record events sent
        if event_count > 0 {
            self.metrics.record_events_sent(event_count as u64);
        }
    }

    /// Flush volatile batches for slow clients (WebSocket) - 4Hz.
    /// This also clears the batch after sending.
    pub(super) fn flush_volatile_slow(&mut self) {
        if !self.view_manager.has_pending_volatile() {
            return;
        }

        // Send directly via stored connections and clear batch
        let event_count = self.view_manager.flush_volatile_slow();

        // Record events sent
        if event_count > 0 {
            self.metrics.record_events_sent(event_count as u64);
        }
    }
}
