use super::*;

impl Database {
    pub(super) async fn handle_set(
        &mut self,
        client_id: &str,
        msg: &ClientMessage,
        volatile: bool,
    ) -> Option<ServerMessage> {
        let request_id = msg.request_id.as_deref().unwrap_or("");
        let path_str = msg.path.as_deref().unwrap_or("/");

        // Reject malformed paths or value keys before any work, the rules
        // evaluator, or the WAL/blob writers. SET stores its value object as-is,
        // so its field-names become storage keys and must pass validate_key too.
        if crate::db::validate_path(path_str).is_err()
            || msg
                .value
                .as_ref()
                .is_some_and(|v| validate_value_keys(v).is_err())
        {
            return self.nack_invalid_key(client_id, request_id);
        }

        // The path is within the depth cap (validate_path), but the value lands
        // *under* it — reject if path + value nesting would exceed the cap, so
        // nothing gets written that a same-depth read couldn't later reach.
        if let Some(v) = &msg.value
            && crate::db::path_depth(path_str) + json_value_depth(v) > crate::db::MAX_PATH_DEPTH
        {
            return self.nack_too_deep(client_id, request_id);
        }

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

        // NACK if WAL I/O has failed (non-volatile writes only)
        if !volatile && self.is_wal_failed() {
            self.record_nacked_write(client_id, request_id);
            return Some(ServerMessage::nack(
                request_id,
                error::UNAVAILABLE,
                "Storage unavailable (WAL I/O failure)",
            ));
        }

        // Reject this write once the database is at its size cap — including
        // volatile writes (we don't exempt them; keeps the check trivial). Deletes
        // still go through handle_remove so the owner can recover by freeing
        // space. Should never happen in practice. See MAX_DATABASE_SIZE_BYTES.
        if self.is_at_size_cap() {
            self.record_nacked_write(client_id, request_id);
            return Some(ServerMessage::nack(
                request_id,
                error::DATABASE_FULL,
                "database is at its size limit",
            ));
        }

        let value = match &msg.value {
            Some(v) => v.clone(),
            None => Value::Null,
        };

        // Validate .value/.priority patterns
        if let Err(e) = validate_value_priority(&value, path_str) {
            if !volatile {
                self.record_nacked_write(client_id, request_id);
                return Some(ServerMessage::nack(request_id, error::INVALID_DATA, &e));
            }
            return None; // Swallow NACK for volatile writes
        }

        // Process server values (like {".sv": "timestamp"} or {".sv": {"increment": 10}})
        let value = match process_server_values(value, path_str, &self.tree) {
            Ok((processed, _)) => processed,
            Err(e) => {
                if !volatile {
                    self.record_nacked_write(client_id, request_id);
                    return Some(ServerMessage::nack(request_id, error::INVALID_DATA, &e));
                }
                return None; // Swallow NACK for volatile writes
            }
        };

        // Check write permission
        if !self
            .can_write(
                client_id,
                path_str,
                Some(NewData::from_set(path_str.to_string(), value.clone())),
            )
            .await
        {
            let auth_summary = self.get_auth_summary(client_id);
            debug!(
                "NACK {}: SET permission denied at {} for client {} | auth: {}",
                self.id, path_str, client_id, auth_summary
            );
            self.metrics.record_permission_denial();
            if !volatile {
                self.record_nacked_write(client_id, request_id);
                return Some(ServerMessage::nack(
                    request_id,
                    error::PERMISSION_DENIED,
                    "Permission denied",
                ));
            }
            return None; // Swallow NACK for volatile writes
        }

        // Check compare-and-swap hash if provided (Firebase transaction support)
        let hash = msg.hash.as_deref().unwrap_or("");
        let hash_provided = msg.hash_provided.unwrap_or(false);
        if !hash.is_empty() || hash_provided {
            // Promote path to get accurate data for hash comparison
            if let Err(e) = self.promote_path(path_str).await {
                warn!(
                    "NACK SET {}: promotion failed for hash check: {}",
                    path_str, e
                );
                self.record_nacked_write(client_id, request_id);
                return Some(ServerMessage::nack(
                    request_id,
                    error::UNAVAILABLE,
                    &format!("Failed to load data for hash verification: {}", e),
                ));
            }

            let path_obj = Path::parse(path_str);
            let old_value = self.tree.read().unwrap().get_value(&path_obj);

            if !hash.is_empty() {
                // Compare hash of current value
                let current_hash = if is_firebase_hash(hash) {
                    // Firebase hash: SHA-1 + base64
                    compute_firebase_hash(&old_value.clone().unwrap_or(Value::Null))
                } else {
                    // Lark hash: JCS + SHA-256 + hex
                    compute_value_hash(&old_value.clone().unwrap_or(Value::Null))
                };

                if current_hash != hash {
                    // Hash mismatch - data changed since client read it
                    // Don't record as nacked - condition_failed is retryable
                    return Some(ServerMessage::nack(
                        request_id,
                        error::CONDITION_FAILED,
                        "data changed since read (hash mismatch)",
                    ));
                }
            } else if hash_provided && old_value.as_ref().is_some_and(|v| !v.is_null()) {
                // Empty hash with hash_provided=true means speculative transaction
                // (client has no cached data). Only accept if path has no existing data.
                //
                // `old_value.is_some()` alone is wrong: a path that's been
                // promoted as "loaded, doesn't exist" sits in the tree as
                // `Some(Value::Null)` (the marker `promote_path_unchecked`
                // installs on PathNotFound) — semantics treat null
                // as "doesn't exist," so a speculative write against it must
                // succeed, not fail. The check has to look at the value
                // itself, not just whether the lookup returned Some.
                //
                // Without this, a client whose listener received a
                // null snapshot and then ran `transaction()` got rejected on
                // the speculative first attempt, retried with the same
                // payload, and looped to MAXRETRY without progress.
                return Some(ServerMessage::nack(
                    request_id,
                    error::CONDITION_FAILED,
                    "data exists (speculative write rejected)",
                ));
            }
        }

        // Determine if this path is volatile based on RULES (don't trust client flag)
        let is_volatile = self.is_volatile_path(path_str);

        // Cap volatile writes at MAX_VOLATILE_WRITE_SIZE. The threat is fan-out
        // amplification: volatile writes skip WAL but broadcast to every subscriber,
        // and they're exempt from the per-DB byte rate limiter (no durable cost) —
        // so without this, one client could blast 16MB (SDK) or 256MB (REST) of
        // payload at N subscribers per write. Volatile writes don't get NACKs;
        // silently drop, matching how this path swallows other errors above.
        if is_volatile && estimate_value_bytes(&value) > MAX_VOLATILE_WRITE_SIZE {
            return None;
        }

        // Check if this is a volatile path - use ViewManager batching, skip persistence
        if is_volatile {
            // Fast path: buffer in ViewManager for batch sending
            let value_bytes = Bytes::from(serde_json::to_vec(&value).unwrap_or_default());
            self.view_manager
                .buffer_volatile(path_str, value_bytes, client_id);

            // Record write metrics for volatile writes too
            self.metrics.record_write(msg.payload_size);

            // No ack for volatile writes
            return None;
        }

        // Durable write: charge the per-database write-rate limiter before
        // committing (volatile writes returned above; ephemeral DBs are exempt).
        if let Some(nack) = self.check_write_rate(msg.payload_size, client_id, request_id) {
            return Some(nack);
        }

        // Regular write path
        let path = Path::parse(path_str);

        // Set the value in tree. For blob-backed DBs, use set_lazy so
        // intermediate nodes are Sentinels (no eager loading needed for SET).
        if self.is_blob_backed() {
            // Clear stale descendant sentinel entries — set_lazy replaces the subtree
            self.remove_sentinel_paths_below(path_str);
            self.tree.write().unwrap().set_lazy(&path, value.clone());
            self.track_sentinels_after_write(path_str);
        } else {
            self.tree.write().unwrap().set(&path, value.clone());
        }

        // Write to WAL for durability (async)
        self.wal_write_set(path_str, &value);

        // Broadcast to subscribers via ViewManager
        self.broadcast_mutation(
            path_str,
            "set",
            Some(value),
            None,
            is_volatile,
            Some(client_id),
        )
        .await;

        // Record for deduplication (skip volatile writes - they don't need deduplication)
        if !is_volatile {
            self.record_processed_write(client_id, request_id);
        }

        // Record write metrics using raw payload size captured at parse time
        self.metrics.record_write(msg.payload_size);

        // Return ack (only if not volatile and has request_id)
        if !msg.is_volatile() && !request_id.is_empty() {
            Some(ServerMessage::ack(request_id))
        } else {
            None
        }
    }

    pub(super) async fn handle_update(
        &mut self,
        client_id: &str,
        msg: &ClientMessage,
        volatile: bool,
    ) -> Option<ServerMessage> {
        let request_id = msg.request_id.as_deref().unwrap_or("");
        let path_str = msg.path.as_deref().unwrap_or("/");
        let path = Path::parse(path_str);

        // Reject malformed paths or value keys before any work. Each UPDATE child
        // key is a relative path appended to the base, and each child value's
        // object keys become storage keys — validate the full landing paths and
        // those keys.
        if crate::db::validate_path(path_str).is_err() {
            return self.nack_invalid_key(client_id, request_id);
        }
        if let Some(Value::Object(map)) = &msg.value {
            for (key, val) in map {
                let full = format!("{}/{}", path_str.trim_end_matches('/'), key);
                if crate::db::validate_path(&full).is_err() || validate_value_keys(val).is_err() {
                    return self.nack_invalid_key(client_id, request_id);
                }
                // Each child value lands under `full`; reject if the deepest leaf
                // would exceed the depth cap (see handle_set).
                if crate::db::path_depth(&full) + json_value_depth(val) > crate::db::MAX_PATH_DEPTH
                {
                    return self.nack_too_deep(client_id, request_id);
                }
            }
        }

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

        // NACK if WAL I/O has failed (non-volatile writes only)
        if !volatile && self.is_wal_failed() {
            self.record_nacked_write(client_id, request_id);
            return Some(ServerMessage::nack(
                request_id,
                error::UNAVAILABLE,
                "Storage unavailable (WAL I/O failure)",
            ));
        }

        // Cap volatile writes at MAX_VOLATILE_WRITE_SIZE — same fan-out
        // amplification threat as handle_set. UPDATE's wire-flag gate matches
        // its WAL-skip decision (handle_set uses is_volatile_path; that
        // divergence is tracked separately). Silently drop, no NACK.
        if volatile
            && let Some(ref v) = msg.value
            && estimate_value_bytes(v) > MAX_VOLATILE_WRITE_SIZE
        {
            return None;
        }

        // Reject this write at the size cap, including volatile writes (not
        // exempt). Deletes still go through handle_remove for recovery.
        // See MAX_DATABASE_SIZE_BYTES.
        if self.is_at_size_cap() {
            self.record_nacked_write(client_id, request_id);
            return Some(ServerMessage::nack(
                request_id,
                error::DATABASE_FULL,
                "database is at its size limit",
            ));
        }

        let updates = match &msg.value {
            Some(Value::Object(map)) => map.clone(),
            _ => {
                self.record_nacked_write(client_id, request_id);
                return Some(ServerMessage::nack(
                    request_id,
                    error::INVALID_DATA,
                    "update requires an object value",
                ));
            }
        };

        // Validate .value/.priority patterns for each update value
        for (key, value) in &updates {
            let child_path = format!("{}/{}", path_str.trim_end_matches('/'), key);
            if let Err(e) = validate_value_priority(value, &child_path) {
                self.record_nacked_write(client_id, request_id);
                return Some(ServerMessage::nack(request_id, error::INVALID_DATA, &e));
            }
        }

        // Process server values for each update value
        let mut processed_updates = serde_json::Map::new();
        for (key, value) in updates {
            let child_path = format!("{}/{}", path_str.trim_end_matches('/'), key);
            let processed = match process_server_values(value, &child_path, &self.tree) {
                Ok((v, _)) => v,
                Err(e) => {
                    self.record_nacked_write(client_id, request_id);
                    return Some(ServerMessage::nack(request_id, error::INVALID_DATA, &e));
                }
            };
            processed_updates.insert(key, processed);
        }
        let updates = processed_updates;

        // No eager `promote_path` — `can_write` builds a `NewData::Update`
        // and the rules engine constructs `LazyUpdateSnapshot`s on demand.
        // Anything a rule actually reads (`data.*`, sibling-of-write under
        // `newData.*`) goes through `NeedsPromotion` and the retry loop
        // loads exactly that path. Untouched siblings are never fetched.

        // First, check at the UPDATE path level with `NewData::Update` —
        // the snapshot will be built lazily over (tree, path, updates).
        let update_path_allowed = self
            .can_write(
                client_id,
                path_str,
                Some(NewData::from_update(path_str.to_string(), updates.clone())),
            )
            .await;

        if !update_path_allowed {
            // Parent rule didn't grant access, check each child path individually
            // (Firebase allows children to grant their own access even if parent denies)
            for (key, value) in &updates {
                let child_path = format!("{}/{}", path_str.trim_end_matches('/'), key);
                if !self
                    .can_write(
                        client_id,
                        &child_path,
                        Some(NewData::from_set(child_path.clone(), value.clone())),
                    )
                    .await
                {
                    let auth_summary = self.get_auth_summary(client_id);
                    debug!(
                        "NACK {}: UPDATE permission denied at {} for client {} | auth: {}",
                        self.id, child_path, client_id, auth_summary
                    );
                    self.metrics.record_permission_denial();
                    self.record_nacked_write(client_id, request_id);
                    return Some(ServerMessage::nack(
                        request_id,
                        error::PERMISSION_DENIED,
                        "Permission denied",
                    ));
                }
            }
        }

        // Durable update: charge the rate limiter before mutating the tree so a
        // reject leaves it untouched. Volatile updates skip the WAL (below), so
        // they skip the charge too; ephemeral DBs are exempt (in check_write_rate).
        if !volatile
            && let Some(nack) = self.check_write_rate(msg.payload_size, client_id, request_id)
        {
            return Some(nack);
        }

        // Perform update (shallow merge at path).
        //
        // For blob-backed DBs, use `update_lazy` so intermediate nodes that
        // don't yet exist become Sentinels (signal "not loaded") instead of
        // empty Objects (signal "fully loaded"). The non-lazy `tree.update`
        // here would silently turn the parent into a real Object containing
        // only the new keys whenever a prior `promote_path_shallow` had
        // collapsed the parent to Null on PathNotFound — and `promote_path_deep`
        // would then short-circuit reads of the destroyed siblings via its
        // "Object parent → child definitively absent" check, returning Null
        // for data that's still present in the WAL/blob until the next restart.
        //
        // Mirrors the pattern in `handle_set` and `handle_transaction`'s UPDATE
        // arm.
        if self.is_blob_backed() {
            self.tree.write().unwrap().update_lazy(&path, &updates);
            // Track Sentinel intermediates created by each leaf write —
            // `track_sentinels_after_write` walks ancestors of its argument,
            // so passing each leaf path catches the update-path itself
            // (which became a Sentinel container).
            let base = path_str.trim_end_matches('/');
            for key in updates.keys() {
                let leaf_path = format!("{}/{}", base, key);
                self.track_sentinels_after_write(&leaf_path);
            }
        } else {
            self.tree.write().unwrap().update(&path, &updates);
        }

        // Write to WAL for durability (non-volatile writes only, async)
        if !volatile {
            self.wal_write_update(path_str, &updates);
        }

        // Broadcast to subscribers
        self.broadcast_mutation(
            path_str,
            "update",
            None,
            Some(updates),
            volatile,
            Some(client_id),
        )
        .await;

        // Record for deduplication (skip volatile writes)
        if !volatile {
            self.record_processed_write(client_id, request_id);
        }

        // Record write metrics using raw payload size captured at parse time
        self.metrics.record_write(msg.payload_size);

        // Return ack
        if !msg.is_volatile() && !request_id.is_empty() {
            Some(ServerMessage::ack(request_id))
        } else {
            None
        }
    }

    pub(super) async fn handle_remove(
        &mut self,
        client_id: &str,
        msg: &ClientMessage,
        volatile: bool,
    ) -> Option<ServerMessage> {
        let request_id = msg.request_id.as_deref().unwrap_or("");
        let path_str = msg.path.as_deref().unwrap_or("/");
        let path = Path::parse(path_str);

        // Reject malformed remove paths before any work (same key invariant as
        // SET/UPDATE; a remove can't plant keys but still must not diverge between
        // the rules matcher and storage on empty/odd segments).
        if crate::db::validate_path(path_str).is_err() {
            return self.nack_invalid_key(client_id, request_id);
        }

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

        // NACK if WAL I/O has failed (non-volatile writes only)
        if !volatile && self.is_wal_failed() {
            self.record_nacked_write(client_id, request_id);
            return Some(ServerMessage::nack(
                request_id,
                error::UNAVAILABLE,
                "Storage unavailable (WAL I/O failure)",
            ));
        }

        // Check write permission (remove = write null)
        if !self.can_write(client_id, path_str, None).await {
            let auth_summary = self.get_auth_summary(client_id);
            debug!(
                "NACK {}: DELETE permission denied at {} for client {} | auth: {}",
                self.id, path_str, client_id, auth_summary
            );
            self.metrics.record_permission_denial();
            self.record_nacked_write(client_id, request_id);
            return Some(ServerMessage::nack(
                request_id,
                error::PERMISSION_DENIED,
                "Permission denied",
            ));
        }

        // Remove the value from tree (no need to pre-load from blob for delete)
        self.tree.write().unwrap().remove(&path);
        // Clear sentinel tracking at and below the deleted path — those nodes are gone
        self.remove_sentinel_paths_below(path_str);

        // Write to WAL for durability (non-volatile writes only, async)
        if !volatile {
            self.wal_write_delete(path_str);
        }

        // Broadcast deletion to subscribers
        self.broadcast_mutation(path_str, "remove", None, None, volatile, Some(client_id))
            .await;

        // Record for deduplication (skip volatile writes)
        if !volatile {
            self.record_processed_write(client_id, request_id);
        }

        // Record write metrics (remove is 0 bytes)
        self.metrics.record_write(0);

        // Return ack
        if !request_id.is_empty() {
            Some(ServerMessage::ack(request_id))
        } else {
            None
        }
    }
}
