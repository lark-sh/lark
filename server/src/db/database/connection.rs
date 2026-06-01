use super::*;

impl Database {
    /// Handle JOIN message - acknowledges the join and returns volatile paths.
    pub(super) fn handle_join(
        &self,
        client_id: &str,
        msg: &ClientMessage,
    ) -> Option<ServerMessage> {
        let request_id = msg.request_id.as_deref().unwrap_or("");

        // Get connection ID from client info
        let connection_id = self
            .clients
            .get(client_id)
            .map(|c| c.connection_id.clone())
            .unwrap_or_default();

        Some(ServerMessage::join_ack(
            request_id,
            self.volatile_paths.clone(),
            &connection_id,
        ))
    }

    /// Handle TRANSACTION message - atomic multi-path operations.
    pub(super) async fn handle_transaction(
        &mut self,
        client_id: &str,
        msg: &ClientMessage,
    ) -> Option<ServerMessage> {
        let request_id = msg.request_id.as_deref().unwrap_or("");

        // Check for tainted write (depends on a nacked write) - silently ignore
        if self.is_write_tainted(client_id, &msg.pending_writes) {
            return None; // Silently ignore tainted writes
        }

        // Check for duplicate write (deduplication)
        if self.is_write_processed(client_id, request_id) {
            // Already processed - return ack without doing anything
            if !request_id.is_empty() {
                return Some(ServerMessage::ack(request_id));
            }
            return None;
        }

        // NACK if WAL I/O has failed
        if self.is_wal_failed() {
            self.record_nacked_write(client_id, request_id);
            return Some(ServerMessage::nack(
                request_id,
                error::UNAVAILABLE,
                "Storage unavailable (WAL I/O failure)",
            ));
        }

        let operations = match &msg.operations {
            Some(ops) => ops,
            None => {
                self.record_nacked_write(client_id, request_id);
                return Some(ServerMessage::nack(
                    request_id,
                    error::INVALID_DATA,
                    "missing operations",
                ));
            }
        };

        if operations.is_empty() {
            self.record_nacked_write(client_id, request_id);
            return Some(ServerMessage::nack(
                request_id,
                error::INVALID_DATA,
                "empty transaction",
            ));
        }

        // Cap operations per transaction. Each condition op below promotes a
        // path (blob read + WAL replay) on this database's single inbox, so an
        // oversized transaction would serialize many disk round trips and stall
        // every client on the database. See audit M-2.
        if operations.len() > MAX_TRANSACTION_OPS {
            debug!(
                "NACK {}: transaction has {} ops, exceeds cap {}",
                self.id,
                operations.len(),
                MAX_TRANSACTION_OPS
            );
            self.record_nacked_write(client_id, request_id);
            return Some(ServerMessage::nack(
                request_id,
                error::PAYLOAD_TOO_LARGE,
                &format!("transaction exceeds {} operations", MAX_TRANSACTION_OPS),
            ));
        }

        // Reject transactions at the size cap. Deletes still go through
        // handle_remove for recovery. See MAX_DATABASE_SIZE_BYTES.
        if self.is_at_size_cap() {
            self.record_nacked_write(client_id, request_id);
            return Some(ServerMessage::nack(
                request_id,
                error::DATABASE_FULL,
                "database is at its size limit",
            ));
        }

        // Validate every operation's paths AND the keys inside its value before
        // anything else. Both the op path and the object field-names in the value
        // become storage keys, so the same key rules (validate_key: non-empty, no
        // control chars / `$ # [ ] /`, `.` only as a leading char, ≤768 bytes)
        // must hold for all of them. Rejecting up front means malformed keys can't
        // reach the rules evaluator or the WAL/blob writers, and it closes the
        // rules-vs-storage tokenizer divergence (e.g. `users//abc` has an empty
        // segment → rejected here, before the two tokenizers can disagree about
        // where the write lands).
        for op in operations {
            let check = || -> Result<(), crate::db::KeyError> {
                crate::db::validate_path(&op.path)?;
                match (op.op.as_str(), &op.value) {
                    // SET: the value's object keys become storage keys.
                    ("s" | "set", Some(value)) => validate_value_keys(value)?,
                    // UPDATE: each map key is a relative path appended to op.path
                    // (validate the full landing path), and each update value's
                    // own object keys become storage keys too.
                    ("u" | "update", Some(Value::Object(map))) => {
                        for (key, val) in map {
                            let full = format!("{}/{}", op.path.trim_end_matches('/'), key);
                            crate::db::validate_path(&full)?;
                            validate_value_keys(val)?;
                        }
                    }
                    _ => {}
                }
                Ok(())
            };
            if let Err(e) = check() {
                debug!(
                    "NACK {}: invalid path/key in op at {:?}: {}",
                    self.id, op.path, e
                );
                self.record_nacked_write(client_id, request_id);
                return Some(ServerMessage::nack(
                    request_id,
                    error::INVALID_DATA,
                    "invalid path or key",
                ));
            }

            // Reject ops whose landing path + value nesting would exceed the
            // depth cap (same rule the single-op SET/UPDATE handlers enforce).
            let too_deep = match (op.op.as_str(), &op.value) {
                ("s" | "set", Some(value)) => {
                    crate::db::path_depth(&op.path) + json_value_depth(value)
                        > crate::db::MAX_PATH_DEPTH
                }
                ("u" | "update", Some(Value::Object(map))) => map.iter().any(|(key, val)| {
                    let full = format!("{}/{}", op.path.trim_end_matches('/'), key);
                    crate::db::path_depth(&full) + json_value_depth(val) > crate::db::MAX_PATH_DEPTH
                }),
                _ => false,
            };
            if too_deep {
                return self.nack_too_deep(client_id, request_id);
            }
        }

        // First, check permissions for all write operations
        for op in operations {
            if op.op == "c" {
                continue; // Conditions don't need write permission
            }

            // Check write permission. Build the appropriate `NewData` for
            // each op type — SET-style for "s" with a value, UPDATE-style
            // for "u", and None for "d" (delete).
            let new_data = match (op.op.as_str(), op.value.clone()) {
                ("s", Some(v)) => Some(NewData::from_set(op.path.clone(), v)),
                ("u", Some(Value::Object(map))) => Some(NewData::from_update(op.path.clone(), map)),
                _ => None,
            };
            if !self.can_write(client_id, &op.path, new_data).await {
                let auth_summary = self.get_auth_summary(client_id);
                debug!(
                    "NACK {}: TRANSACTION permission denied at {} for client {} | auth: {}",
                    self.id, op.path, client_id, auth_summary
                );
                self.metrics.record_permission_denial();
                self.record_nacked_write(client_id, request_id);
                return Some(ServerMessage::nack(
                    request_id,
                    error::PERMISSION_DENIED,
                    "write permission denied",
                ));
            }
        }

        // Validate all conditions. Promotion is idempotent, so dedup repeated
        // condition paths within the transaction — promoting a path twice is
        // wasted disk work and an avoidable amplification vector (audit M-2).
        let mut promoted: std::collections::HashSet<&str> = std::collections::HashSet::new();
        for op in operations {
            if op.op == "c" {
                // Promote from blob if needed for accurate condition check.
                // Use deep promotion so container values are fully loaded —
                // shallow promotion leaves Sentinel children which would
                // serialize to null and break value/hash comparisons.
                if promoted.insert(op.path.as_str())
                    && let Err(e) = self.promote_path_deep(&op.path).await
                {
                    warn!(
                        "NACK TRANSACTION: promotion failed for condition at {}: {}",
                        op.path, e
                    );
                    return Some(ServerMessage::nack(
                        request_id,
                        error::UNAVAILABLE,
                        &format!("Failed to load data for condition check: {}", e),
                    ));
                }

                let path = Path::parse(&op.path);
                let current_value = self.tree.read().unwrap().get(&path).map(|n| n.to_value());

                if let Some(ref expected) = op.value {
                    // Value-based condition
                    let current_val = current_value.as_ref().unwrap_or(&Value::Null);
                    if current_val != expected {
                        return Some(ServerMessage::nack(
                            request_id,
                            error::CONDITION_FAILED,
                            "condition not met",
                        ));
                    }
                } else if let Some(ref hash) = op.hash {
                    // Hash-based condition check
                    let current_val = current_value.as_ref().unwrap_or(&Value::Null);
                    let current_hash = compute_value_hash(current_val);
                    if &current_hash != hash {
                        return Some(ServerMessage::nack(
                            request_id,
                            error::CONDITION_FAILED,
                            "hash mismatch",
                        ));
                    }
                } else {
                    // No value and no hash means expecting null/non-existent
                    if current_value.is_some() {
                        return Some(ServerMessage::nack(
                            request_id,
                            error::CONDITION_FAILED,
                            "expected null but path exists",
                        ));
                    }
                }
            }
        }

        // Validate .value/.priority patterns for all set/update operations
        for op in operations {
            match op.op.as_str() {
                "s" | "set" => {
                    if let Some(ref value) = op.value
                        && let Err(e) = validate_value_priority(value, &op.path)
                    {
                        return Some(ServerMessage::nack(request_id, error::INVALID_DATA, &e));
                    }
                }
                "u" | "update" => {
                    if let Some(Value::Object(map)) = &op.value {
                        for (key, val) in map {
                            let child_path = format!("{}/{}", op.path.trim_end_matches('/'), key);
                            if let Err(e) = validate_value_priority(val, &child_path) {
                                return Some(ServerMessage::nack(
                                    request_id,
                                    error::INVALID_DATA,
                                    &e,
                                ));
                            }
                        }
                    }
                }
                _ => {}
            }
        }

        // Collect changes for subscriber notifications
        #[allow(clippy::type_complexity)] // (path, key, value, fields) change tuples
        let mut changes: Vec<(
            String,
            String,
            Option<Value>,
            Option<serde_json::Map<String, Value>>,
        )> = Vec::new();

        // Charge the whole transaction's bytes against the write-rate limiter
        // before applying any op, so a reject leaves the tree untouched.
        if let Some(nack) = self.check_write_rate(msg.payload_size, client_id, request_id) {
            return Some(nack);
        }

        // Apply all operations
        // Note: We need to collect WAL entries separately because we can't hold the tree lock while writing to WAL.
        // Each arm acquires/releases the tree lock as needed so we can call &mut self
        // helpers (remove_sentinel_paths_below, track_sentinels_after_write) between writes.
        let mut wal_entries: Vec<(String, String, Option<Value>)> = Vec::new();
        let blob_backed = self.is_blob_backed();
        for op in operations {
            let path = Path::parse(&op.path);
            match op.op.as_str() {
                "s" | "set" => {
                    if let Some(ref value) = op.value {
                        let processed =
                            match process_server_values(value.clone(), &op.path, &self.tree) {
                                Ok((v, _)) => v,
                                Err(e) => {
                                    return Some(ServerMessage::nack(
                                        request_id,
                                        error::INVALID_DATA,
                                        &e,
                                    ));
                                }
                            };
                        // For blob-backed DBs, use set_lazy so intermediate nodes
                        // are Sentinels (not empty Objects). Empty Object intermediates
                        // would lie about being "fully loaded" and cause subsequent
                        // reads to short-circuit promotion, returning partial data.
                        if blob_backed {
                            self.remove_sentinel_paths_below(&op.path);
                            self.tree
                                .write()
                                .unwrap()
                                .set_lazy(&path, processed.clone());
                            self.track_sentinels_after_write(&op.path);
                        } else {
                            self.tree.write().unwrap().set(&path, processed.clone());
                        }
                        wal_entries.push((
                            op.path.clone(),
                            "set".to_string(),
                            Some(processed.clone()),
                        ));
                        changes.push((op.path.clone(), "set".to_string(), Some(processed), None));
                    }
                }
                "u" | "update" => {
                    if let Some(Value::Object(map)) = &op.value {
                        let mut processed_map = serde_json::Map::new();
                        for (key, val) in map {
                            let child_path = format!("{}/{}", op.path.trim_end_matches('/'), key);
                            let processed =
                                match process_server_values(val.clone(), &child_path, &self.tree) {
                                    Ok((v, _)) => v,
                                    Err(e) => {
                                        return Some(ServerMessage::nack(
                                            request_id,
                                            error::INVALID_DATA,
                                            &e,
                                        ));
                                    }
                                };
                            processed_map.insert(key.clone(), processed);
                        }
                        // For blob-backed DBs, use update_lazy so the merge writes
                        // through Sentinel intermediates instead of clobbering them
                        // into Objects. Subsequent reads will promote on demand.
                        if blob_backed {
                            self.tree
                                .write()
                                .unwrap()
                                .update_lazy(&path, &processed_map);
                            // Track Sentinels created by each leaf write — track_sentinels_after_write
                            // walks ancestors of its argument, so passing each leaf path catches
                            // the update-path itself (which became a Sentinel container).
                            let base = op.path.trim_end_matches('/');
                            for key in processed_map.keys() {
                                let leaf_path = format!("{}/{}", base, key);
                                self.track_sentinels_after_write(&leaf_path);
                            }
                        } else {
                            self.tree.write().unwrap().update(&path, &processed_map);
                        }
                        wal_entries.push((
                            op.path.clone(),
                            "update".to_string(),
                            Some(Value::Object(processed_map.clone())),
                        ));
                        changes.push((
                            op.path.clone(),
                            "update".to_string(),
                            None,
                            Some(processed_map),
                        ));
                    }
                }
                "d" | "remove" => {
                    self.tree.write().unwrap().remove(&path);
                    // Clear sentinel tracking at and below the deleted path —
                    // those nodes are gone (matches handle_remove).
                    self.remove_sentinel_paths_below(&op.path);
                    wal_entries.push((op.path.clone(), "remove".to_string(), None));
                    changes.push((op.path.clone(), "remove".to_string(), None, None));
                }
                "c" => {
                    // Condition - already checked above
                }
                _ => {}
            }
        }

        // Write to WAL (tree lock is released now) - async to avoid blocking
        for (path, op_type, value) in wal_entries {
            match op_type.as_str() {
                "set" => {
                    if let Some(v) = value {
                        self.wal_write_set(&path, &v);
                    }
                }
                "update" => {
                    if let Some(Value::Object(map)) = value {
                        self.wal_write_update(&path, &map);
                    }
                }
                "remove" => {
                    self.wal_write_delete(&path);
                }
                _ => {}
            }
        }

        // Notify subscribers of changes
        for (path, mutation_type, new_value, updates) in changes {
            self.broadcast_mutation(
                &path,
                &mutation_type,
                new_value,
                updates,
                false,
                Some(client_id),
            )
            .await;
        }

        // Record for deduplication
        self.record_processed_write(client_id, request_id);

        // Record transaction metrics
        self.metrics.record_transaction();

        Some(ServerMessage::ack(request_id))
    }

    // =========================================================================
    // Client Management
    // =========================================================================

    pub(super) fn add_client_internal(
        &mut self,
        client_id: &str,
        auth: Option<AuthInfo>,
        connection_id: &str,
        conn: Arc<dyn ConnectionSender>,
    ) {
        debug!("Client {} joined database {}", client_id, self.id);
        let rules_auth = auth.as_ref().map(Self::convert_auth_to_rules);
        self.clients.insert(
            client_id.to_string(),
            ClientInfo {
                id: client_id.to_string(),
                auth,
                rules_auth,
                connection_id: connection_id.to_string(),
                auth_complete: false,
                conn,
            },
        );

        // Update CCU metric
        self.metrics.increment_ccu();
    }

    pub(super) async fn handle_disconnect(&mut self, client_id: &str) {
        debug!(
            "Client {} disconnected from database {}",
            client_id, self.id
        );

        // Execute disconnect hooks
        if let Some(actions) = self.on_disconnect.remove(client_id) {
            for action in actions {
                let path = Path::parse(&action.path);
                match action.action.as_str() {
                    "set" | "s" => {
                        if let Some(value) = action.value {
                            if self.is_volatile_path(&action.path) {
                                self.view_manager.clear_volatile_for_path(&action.path);
                            }
                            self.remove_sentinel_paths_below(&action.path);
                            if self.is_blob_backed() {
                                self.tree.write().unwrap().set_lazy(&path, value.clone());
                                self.track_sentinels_after_write(&action.path);
                            } else {
                                self.tree.write().unwrap().set(&path, value.clone());
                            }
                            self.wal_write_set(&action.path, &value);
                            self.broadcast_mutation(
                                &action.path,
                                "set",
                                Some(value),
                                None,
                                false,
                                None,
                            )
                            .await;
                        }
                    }
                    "update" | "u" => {
                        if let Some(Value::Object(updates)) = action.value {
                            if self.is_volatile_path(&action.path) {
                                self.view_manager.clear_volatile_for_path(&action.path);
                            }
                            // For blob-backed DBs, use update_lazy so the merge writes
                            // through Sentinel intermediates instead of creating empty
                            // Objects that would lie about being fully loaded.
                            if self.is_blob_backed() {
                                self.tree.write().unwrap().update_lazy(&path, &updates);
                                let base = action.path.trim_end_matches('/');
                                for key in updates.keys() {
                                    let leaf_path = format!("{}/{}", base, key);
                                    self.track_sentinels_after_write(&leaf_path);
                                }
                            } else {
                                self.tree.write().unwrap().update(&path, &updates);
                            }
                            self.wal_write_update(&action.path, &updates);
                            self.broadcast_mutation(
                                &action.path,
                                "update",
                                None,
                                Some(updates),
                                false,
                                None,
                            )
                            .await;
                        }
                    }
                    "remove" | "d" => {
                        // Clear from volatile batch first to prevent stale data
                        // from being flushed after the removal event
                        if self.is_volatile_path(&action.path) {
                            self.view_manager.clear_volatile_for_path(&action.path);
                        }
                        self.tree.write().unwrap().remove(&path);
                        self.remove_sentinel_paths_below(&action.path);
                        self.wal_write_delete(&action.path);
                        self.broadcast_mutation(&action.path, "remove", None, None, false, None)
                            .await;
                    }
                    _ => {}
                }
            }
        }

        // Remove all subscriptions for this client
        self.view_manager.unsubscribe_all(client_id);

        // Note: We intentionally keep processed_writes/nacked_writes entries
        // for the connection_id. If the client reconnects with the same
        // connection_id, we need this history for deduplication.
        // Memory is bounded by MAX_WRITES_PER_CONNECTION per connection.

        // Remove client
        self.clients.remove(client_id);

        // Update CCU metric
        self.metrics.decrement_ccu();
    }

    pub(super) async fn handle_auth_update(&mut self, client_id: &str, auth: Option<AuthInfo>) {
        match self.clients.get_mut(client_id) {
            Some(client) => {
                client.rules_auth = auth.as_ref().map(Self::convert_auth_to_rules);
                client.auth = auth;
                client.auth_complete = true;
            }
            None => return,
        }

        // The client's auth just changed. Any subscription that was authorized
        // under the previous auth must be re-checked: a sign-out, or a token
        // refresh to a different uid, can make a once-allowed read now fail.
        self.revoke_unauthorized_subscriptions(client_id).await;
    }

    /// Re-evaluate one client's active subscriptions against the current rules
    /// and auth, silently unsubscribing any that no longer pass `can_read`.
    ///
    /// The server-side unsubscribe is the security control; we don't send a
    /// client-facing cancel (cooperative SDKs already tear down their listeners
    /// on auth change, and a malicious client would ignore a notification). Each
    /// revocation is logged for observability.
    async fn revoke_unauthorized_subscriptions(&mut self, client_id: &str) {
        // No ruleset means `can_read` always allows — nothing can be revoked, so
        // skip the work (and the allocation) entirely.
        if self.evaluator.is_none() {
            return;
        }

        let subs = self.view_manager.list_client_subscriptions(client_id);
        let mut revoked = 0usize;
        for (path, query_id, rules_query) in subs {
            if !self.can_read(client_id, &path, rules_query).await {
                debug!(
                    "{}: revoking subscription {} (query {}) for client {} — no longer authorized",
                    self.id, path, query_id, client_id
                );
                self.view_manager
                    .unsubscribe_with_query(client_id, &path, &query_id);
                self.metrics.record_permission_denial();
                revoked += 1;
            }
        }

        if revoked > 0 {
            self.metrics
                .set_subscriptions(self.view_manager.subscription_count() as u32);
        }
    }

    /// Re-evaluate ALL active subscriptions against the current rules, revoking
    /// any that no longer pass `can_read`. Called after a CONFIG_PUSH rules
    /// change so a tightened ruleset stops streaming to now-unauthorized
    /// listeners. O(active subscriptions) rule evaluations — acceptable because
    /// a rules change is a rare admin operation, not a per-write hot path.
    pub(super) async fn revoke_all_unauthorized_subscriptions(&mut self) {
        if self.evaluator.is_none() {
            return;
        }

        let subs = self.view_manager.list_all_subscriptions();
        let mut revoked = 0usize;
        for (client_id, path, query_id, rules_query) in subs {
            if !self.can_read(&client_id, &path, rules_query).await {
                debug!(
                    "{}: revoking subscription {} (query {}) for client {} — denied by new rules",
                    self.id, path, query_id, client_id
                );
                self.view_manager
                    .unsubscribe_with_query(&client_id, &path, &query_id);
                self.metrics.record_permission_denial();
                revoked += 1;
            }
        }

        if revoked > 0 {
            self.metrics
                .set_subscriptions(self.view_manager.subscription_count() as u32);
        }
    }

    pub fn client_count(&self) -> usize {
        self.clients.len()
    }

    // =========================================================================
    // Write Operations
    // =========================================================================

    /// Record and build the NACK for a write whose path or value keys are
    /// invalid (empty/oversized segment, control char, `$ # [ ] /`,
    /// dot-in-middle, or a literal-slash object key). Shared by every single-op
    /// write handler so the same key invariant is enforced on each entry point,
    /// not just inside `handle_transaction`.
    pub(super) fn nack_invalid_key(
        &mut self,
        client_id: &str,
        request_id: &str,
    ) -> Option<ServerMessage> {
        self.record_nacked_write(client_id, request_id);
        Some(ServerMessage::nack(
            request_id,
            error::INVALID_DATA,
            "invalid path or key",
        ))
    }

    /// Record and build the NACK for a write whose landing path plus value
    /// nesting would exceed [`crate::db::MAX_PATH_DEPTH`]. Without this, a deeply
    /// nested value at a shallow path could create tree nodes whose own path
    /// exceeds the depth cap — writable but never readable back by path.
    pub(super) fn nack_too_deep(
        &mut self,
        client_id: &str,
        request_id: &str,
    ) -> Option<ServerMessage> {
        self.record_nacked_write(client_id, request_id);
        Some(ServerMessage::nack(
            request_id,
            error::INVALID_DATA,
            "write exceeds maximum path depth",
        ))
    }
}
