use super::broadcast::is_below;
use super::*;

/// True if writing `value` stores nothing and so deletes what was there: null, or
/// containers that hold only such values.
fn stores_nothing(value: &Value) -> bool {
    match value {
        Value::Null => true,
        Value::Object(map) => map.values().all(stores_nothing),
        Value::Array(items) => items.iter().all(stores_nothing),
        _ => false,
    }
}

impl Database {
    // =========================================================================
    // Subscribe/Unsubscribe
    // =========================================================================

    pub(super) async fn handle_subscribe(
        &mut self,
        client_id: &str,
        msg: &ClientMessage,
    ) -> Option<ServerMessage> {
        let request_id = msg.request_id.as_deref().unwrap_or("");
        let path_str = msg.path.as_deref().unwrap_or("/");
        let path = Path::parse(path_str);

        // Reject malformed read paths so the rules matcher and the tree can't
        // tokenize them differently. (No write impact, but keeps read-side auth
        // consistent with the write paths.)
        if crate::db::validate_path(path_str).is_err() {
            return Some(ServerMessage::nack(
                request_id,
                error::INVALID_DATA,
                "invalid path",
            ));
        }

        // Parse query parameters (only build rules query HashMap if rules use query.*)
        let query_params = QueryParams::from_message(msg);
        let rules_query = if self.rules_use_query() {
            query_params.as_ref().map(|qp| qp.to_rules_query())
        } else {
            None
        };

        // Check read permission (with query context for query-based rules)
        if !self.can_read(client_id, path_str, rules_query).await {
            let auth_summary = self.get_auth_summary(client_id);
            debug!(
                "NACK {}: SUBSCRIBE permission denied at {} for client {} | auth: {}",
                self.id, path_str, client_id, auth_summary
            );
            self.metrics.record_permission_denial();
            return Some(ServerMessage::nack(
                request_id,
                error::PERMISSION_DENIED,
                "Permission denied",
            ));
        }

        // Pre-check: if the path needs promotion and the blob subtree is massive,
        // reject before loading into memory.
        if self.has_sentinel_at_or_below(path_str)
            && self.blob_subtree_exceeds_limit(path_str).await
        {
            warn!(
                "NACK {}: read of {} rejected for client {} — blob subtree exceeds size limit",
                self.id, path_str, client_id
            );
            self.metrics.record_size_rejection();
            return Some(ServerMessage::nack(
                request_id,
                error::RESPONSE_TOO_LARGE,
                "Subtree too large to read",
            ));
        }

        // Deep promote: ensure the entire subtree is Sentinel-free.
        // Subscribe sends the full snapshot to the client, so all descendants must be real.
        if let Err(e) = self.promote_path_deep(path_str).await {
            warn!("NACK SUBSCRIBE {}: promotion failed: {}", path_str, e);
            return Some(ServerMessage::nack(
                request_id,
                error::UNAVAILABLE,
                &format!("Failed to load data for subscription: {}", e),
            ));
        }

        // Get client connection for storing in the subscription
        let conn = match self.clients.get(client_id) {
            Some(client) => client.conn.clone(),
            None => {
                return Some(ServerMessage::nack(
                    request_id,
                    error::INTERNAL,
                    "Client not found",
                ));
            }
        };

        // Add subscription via view manager
        let query_id =
            match self
                .view_manager
                .subscribe(client_id, path_str, query_params.as_ref(), conn)
            {
                Ok(id) => id,
                Err(SubscribeError::Query(QueryError::LimitTooLarge(n))) => {
                    return Some(ServerMessage::nack(
                        request_id,
                        error::INVALID_DATA,
                        &format!("Query limit {} exceeds maximum allowed (10000)", n),
                    ));
                }
                Err(SubscribeError::TooManySubscriptions { limit }) => {
                    warn!(
                        "NACK {}: SUBSCRIBE rejected for client {} — at subscription cap ({})",
                        self.id, client_id, limit
                    );
                    return Some(ServerMessage::nack(
                        request_id,
                        error::TOO_MANY_SUBSCRIPTIONS,
                        &format!("subscription limit reached ({} per connection)", limit),
                    ));
                }
            };

        // Get initial value for snapshot
        // OPTIMIZATION: Use ArcValue directly to avoid to_value() copy in all cases.
        // OPTIMIZATION: If another client already subscribed to this exact query,
        // reuse the cached ordered_keys instead of re-sorting.
        let (arc_value, tag, keys) = if let Some(params) = &query_params {
            // Query subscription - check if we can reuse cached keys from shared view
            let cached_keys = self
                .view_manager
                .get_shared_view(path_str, &query_id)
                .filter(|v| !v.ordered_keys.is_empty())
                .map(|v| v.ordered_keys.clone());

            if let Some(keys) = cached_keys {
                // Reuse cached keys - skip expensive sorting!
                let arc_value = self.get_result_from_cached_keys(&path, &keys);
                (arc_value, params.tag, None) // None = don't re-initialize
            } else {
                // First subscriber - compute full query result
                let query = match params.to_query() {
                    Ok(q) => q,
                    Err(e) => {
                        self.view_manager
                            .unsubscribe_with_query(client_id, path_str, &query_id);
                        return Some(ServerMessage::nack(
                            request_id,
                            error::INVALID_DATA,
                            &format!("Invalid query: {:?}", e),
                        ));
                    }
                };
                let (arc_value, keys) = self.get_query_result_with_keys(&path, &query);
                (arc_value, params.tag, Some(keys))
            }
        } else {
            // Simple subscription - use ArcValue directly (avoids to_value() conversion)
            let arc_value = self
                .tree
                .read()
                .unwrap()
                .get_arc(&path)
                .unwrap_or(ArcValue::Null);
            (arc_value, None, None)
        };

        // Check response size limit (256MB for all clients)
        let estimated_size = arc_value.estimate_size() as usize;
        let max_response = crate::protocol::max_response_size();
        if estimated_size > max_response {
            warn!(
                "NACK {}: subscribe to {} rejected for client {} — initial snapshot {} bytes exceeds {}",
                self.id, path_str, client_id, estimated_size, max_response
            );
            // Remove the subscription we just added
            self.view_manager
                .unsubscribe_with_query(client_id, path_str, &query_id);
            self.metrics.record_size_rejection();
            return Some(ServerMessage::nack(
                request_id,
                error::RESPONSE_TOO_LARGE,
                &format!(
                    "Initial snapshot size {} exceeds maximum allowed ({} bytes)",
                    estimated_size, max_response
                ),
            ));
        }

        // Update subscription count metric
        self.metrics
            .set_subscriptions(self.view_manager.subscription_count() as u32);

        // Initialize query view with ordered keys (if query subscription)
        if let Some(keys) = keys {
            self.view_manager
                .initialize_query_view(client_id, path_str, &query_id, keys);
        }

        let mut event_msg = ServerMessage::put_event_arc(path_str, "/", arc_value, false);
        if let Some(tag) = tag {
            event_msg.tag = Some(tag);
        }

        self.send_to_client(client_id, &event_msg, false, Some(path_str))
            .await;

        // Now send ack
        Some(ServerMessage::ack(request_id))
    }

    pub(super) fn handle_unsubscribe(
        &mut self,
        client_id: &str,
        msg: &ClientMessage,
    ) -> Option<ServerMessage> {
        let request_id = msg.request_id.as_deref().unwrap_or("");
        let path_str = msg.path.as_deref().unwrap_or("/");

        // Parse query params to get the correct query ID
        let query_params = QueryParams::from_message(msg);
        let query_id = query_params
            .as_ref()
            .map(|p| p.identifier())
            .unwrap_or_else(|| "default".to_string());

        // Remove subscription from view manager
        self.view_manager
            .unsubscribe_with_query(client_id, path_str, &query_id);

        // Update subscription count metric
        self.metrics
            .set_subscriptions(self.view_manager.subscription_count() as u32);

        Some(ServerMessage::ack(request_id))
    }

    // =========================================================================
    // Once (single read)
    // =========================================================================

    pub(super) async fn handle_once(
        &mut self,
        client_id: &str,
        msg: &ClientMessage,
    ) -> Option<ServerMessage> {
        let request_id = msg.request_id.as_deref().unwrap_or("");
        let path_str = msg.path.as_deref().unwrap_or("/");
        let path = Path::parse(path_str);

        // Reject malformed read paths (see handle_subscribe).
        if crate::db::validate_path(path_str).is_err() {
            return Some(ServerMessage::nack(
                request_id,
                error::INVALID_DATA,
                "invalid path",
            ));
        }

        // Only build query HashMap if rules reference query.*
        let rules_query = if self.rules_use_query() {
            QueryParams::from_message(msg).map(|qp| qp.to_rules_query())
        } else {
            None
        };

        // Check read permission (with query context for query-based rules)
        if !self.can_read(client_id, path_str, rules_query).await {
            let auth_summary = self.get_auth_summary(client_id);
            debug!(
                "NACK {}: ONCE permission denied at {} for client {} | auth: {}",
                self.id, path_str, client_id, auth_summary
            );
            self.metrics.record_permission_denial();
            return Some(ServerMessage::nack(
                request_id,
                error::PERMISSION_DENIED,
                "Permission denied",
            ));
        }

        // Shallow read: return only immediate child keys as {"key": true, ...}
        // without loading any child data from the blob.
        if msg.shallow == Some(true) {
            return self.handle_once_shallow(request_id, path_str, &path).await;
        }

        // Pre-check: if the path needs promotion and the blob subtree is massive,
        // reject before loading into memory.
        if self.has_sentinel_at_or_below(path_str)
            && self.blob_subtree_exceeds_limit(path_str).await
        {
            warn!(
                "NACK {}: read of {} rejected for client {} — blob subtree exceeds size limit",
                self.id, path_str, client_id
            );
            self.metrics.record_size_rejection();
            return Some(ServerMessage::nack(
                request_id,
                error::RESPONSE_TOO_LARGE,
                "Subtree too large to read",
            ));
        }

        // Deep promote: ensure the entire subtree is Sentinel-free.
        // ONCE sends the full value to the client, so all descendants must be real.
        if let Err(e) = self.promote_path_deep(path_str).await {
            warn!("NACK ONCE {}: promotion failed: {}", path_str, e);
            return Some(ServerMessage::nack(
                request_id,
                error::UNAVAILABLE,
                &format!("Failed to load data for read: {}", e),
            ));
        }

        // Parse query parameters
        let query_params = QueryParams::from_message(msg);

        // OPTIMIZATION: Use ArcValue directly to avoid to_value() copy in all cases.
        let arc_value = if let Some(params) = &query_params {
            // Validate and convert query params
            let query = match params.to_query() {
                Ok(q) => q,
                Err(QueryError::LimitTooLarge(n)) => {
                    return Some(ServerMessage::nack(
                        request_id,
                        error::INVALID_DATA,
                        &format!("Query limit {} exceeds maximum allowed (10000)", n),
                    ));
                }
            };
            // Query read - apply filtering, returns ArcValue directly (O(1) child clones)
            let (arc_value, _keys) = self.get_query_result_with_keys(&path, &query);
            arc_value
        } else {
            // Simple read - use ArcValue directly (avoids to_value() conversion)
            self.tree
                .read()
                .unwrap()
                .get_arc(&path)
                .unwrap_or(ArcValue::Null)
        };

        // Check response size limit (256MB for all clients)
        let estimated_size = arc_value.estimate_size() as usize;
        let max_response = crate::protocol::max_response_size();
        if estimated_size > max_response {
            warn!(
                "NACK {}: read of {} rejected for client {} — response {} bytes exceeds {}",
                self.id, path_str, client_id, estimated_size, max_response
            );
            self.metrics.record_size_rejection();
            return Some(ServerMessage::nack(
                request_id,
                error::RESPONSE_TOO_LARGE,
                &format!(
                    "Response size {} exceeds maximum allowed ({} bytes)",
                    estimated_size, max_response
                ),
            ));
        }

        // Record read operation (bytes tracked in send_to_client)
        self.metrics.record_read();

        Some(ServerMessage::once_response_arc(request_id, arc_value))
    }

    /// Handle a shallow once read.
    ///
    /// Returns a map of immediate children at the given path. Each child value is:
    /// - **Primitive child**: the actual value (string, number, bool, null)
    /// - **Container child**: `{".sz": <byte_size>}` — the proxy can convert this
    ///   to `true` for Firebase REST clients or keep the size for Lark v2 clients.
    ///
    /// If the path itself is a primitive, returns the value directly.
    ///
    /// For blob-backed databases, uses `BlobSession::read_shallow` which reads only
    /// the container header + child index (plus tiny reads for primitive child values).
    /// No full subtree data is ever loaded.
    async fn handle_once_shallow(
        &mut self,
        request_id: &str,
        path_str: &str,
        path: &Path,
    ) -> Option<ServerMessage> {
        // Helper: build {".sz": size} marker for a container child.
        fn size_marker(size: u64) -> ArcValue {
            let mut m = HashMap::new();
            m.insert(".sz".to_string(), ArcValue::from(size as i64));
            ArcValue::Object(Arc::new(m))
        }

        let mut children: HashMap<String, ArcValue> = HashMap::new();

        // Check if the data is already in the tree (non-Sentinel)
        let tree_has_data = {
            let tree = self.tree.read().unwrap();
            match tree.get(path) {
                Some(node) if !node.is_sentinel() => {
                    if !node.is_object() {
                        // Path is a primitive in the tree — return it directly
                        let val = node.clone();
                        drop(tree);
                        self.metrics.record_read();
                        return Some(ServerMessage::once_response_arc(request_id, val));
                    }
                    // An unloaded child could be a primitive or a container, and
                    // only the blob knows, so read this level from there instead.
                    let all_loaded = node
                        .keys()
                        .all(|key| !node.get(key).is_some_and(ArcValue::is_sentinel));
                    if all_loaded {
                        for key in node.keys() {
                            if let Some(child) = node.get(key) {
                                let shallow_val = if child.is_primitive() {
                                    child.clone()
                                } else {
                                    size_marker(child.estimate_size() as u64)
                                };
                                children.insert(key.to_string(), shallow_val);
                            }
                        }
                    }
                    all_loaded
                }
                _ => false,
            }
        };

        if !tree_has_data {
            if self.blob_session.is_some() {
                let segments: Vec<&str> = path.segments().iter().map(|s| s.as_ref()).collect();
                let blob_path = if path.is_root() { vec![] } else { segments };

                let blob_result = {
                    // Inner scope keeps the borrow short so it drops before the
                    // `&mut self` uses below; an outer `if let` would extend it.
                    #[allow(clippy::unnecessary_unwrap)]
                    let session = self.blob_session.as_ref().unwrap();
                    session.read_shallow(&blob_path).await
                };

                // The blob's own sizes, for containers the WAL leaves in place.
                let mut sizes: HashMap<String, u64> = HashMap::new();
                let base = match blob_result {
                    Ok(ShallowValue::Primitive(val)) => val,
                    Ok(ShallowValue::Children(blob_children)) => {
                        let mut map = HashMap::new();
                        for child in blob_children {
                            let val = child.value.unwrap_or_else(ArcValue::empty_sentinel);
                            sizes.insert(child.key.clone(), child.size);
                            map.insert(child.key, val);
                        }
                        ArcValue::Object(Arc::new(map))
                    }
                    Err(BlobError::PathNotFound(_)) => ArcValue::Null,
                    Err(e) => {
                        warn!(
                            "NACK shallow ONCE {}: blob read_shallow failed: {}",
                            path_str, e
                        );
                        return Some(ServerMessage::nack(
                            request_id,
                            error::UNAVAILABLE,
                            &format!("Failed to read shallow data: {}", e),
                        ));
                    }
                };

                match self.replay_wal_lazy(path_str, base) {
                    ArcValue::Object(map) | ArcValue::Sentinel(map) => {
                        for (key, child) in map.iter() {
                            if child.is_null() {
                                continue;
                            }
                            let shallow_val = if child.is_primitive() {
                                child.clone()
                            } else {
                                let size = sizes.get(key).copied();
                                size_marker(size.unwrap_or(child.estimate_size() as u64))
                            };
                            children.insert(key.clone(), shallow_val);
                        }
                    }
                    primitive => {
                        self.metrics.record_read();
                        return Some(ServerMessage::once_response_arc(request_id, primitive));
                    }
                }
            } else {
                // Not blob-backed — promote (shallow) and read from tree
                if let Err(e) = self.promote_path(path_str).await {
                    warn!("NACK shallow ONCE {}: promotion failed: {}", path_str, e);
                    return Some(ServerMessage::nack(
                        request_id,
                        error::UNAVAILABLE,
                        &format!("Failed to load data for read: {}", e),
                    ));
                }
                let tree = self.tree.read().unwrap();
                if let Some(node) = tree.get(path) {
                    if !node.is_object() {
                        let val = node.clone();
                        drop(tree);
                        self.metrics.record_read();
                        return Some(ServerMessage::once_response_arc(request_id, val));
                    }
                    for key in node.keys() {
                        if let Some(child) = node.get(key) {
                            let shallow_val = if child.is_primitive() {
                                child.clone()
                            } else {
                                size_marker(child.estimate_size() as u64)
                            };
                            children.insert(key.to_string(), shallow_val);
                        }
                    }
                }
            }
        }

        // A pending delete may have removed everything in a container child, which
        // the child's own marker can't show.
        if self.blob_session.is_some() {
            let prefix = path_str.trim_end_matches('/').to_string();
            let containers: Vec<String> = children
                .iter()
                .filter(|(_, value)| value.is_object())
                .map(|(key, _)| key.clone())
                .collect();
            for key in containers {
                let child_path = normalize_path_key(&format!("{prefix}/{key}"));
                if !self.has_pending_delete_within(&child_path) {
                    continue;
                }
                match self.holds_data_after_pending_deletes(&child_path).await {
                    Ok(true) => {}
                    Ok(false) => {
                        children.remove(&key);
                    }
                    Err(e) => {
                        warn!(
                            "NACK shallow ONCE {}: blob read_shallow failed: {}",
                            child_path, e
                        );
                        return Some(ServerMessage::nack(
                            request_id,
                            error::UNAVAILABLE,
                            &format!("Failed to read shallow data: {}", e),
                        ));
                    }
                }
            }
        }

        // Build the response
        let shallow_value = if children.is_empty() {
            ArcValue::Null
        } else {
            ArcValue::Object(Arc::new(children))
        };

        self.metrics.record_read();
        Some(ServerMessage::once_response_arc(request_id, shallow_value))
    }

    /// True if `path` still holds data once pending deletes are applied. Reads one
    /// level at a time, going deeper only along paths with a pending delete, and
    /// loads nothing into the tree.
    async fn holds_data_after_pending_deletes(&self, path: &str) -> Result<bool, BlobError> {
        let Some(session) = &self.blob_session else {
            return Ok(false);
        };
        let mut to_check = vec![path.to_string()];
        while let Some(current) = to_check.pop() {
            let segments: Vec<&str> = current.split('/').filter(|s| !s.is_empty()).collect();
            let base = match session.read_shallow(&segments).await {
                Ok(ShallowValue::Primitive(value)) => value,
                Ok(ShallowValue::Children(blob_children)) => {
                    let map = blob_children
                        .into_iter()
                        .map(|c| (c.key, c.value.unwrap_or_else(ArcValue::empty_sentinel)))
                        .collect();
                    ArcValue::Object(Arc::new(map))
                }
                Err(BlobError::PathNotFound(_)) => ArcValue::Null,
                Err(e) => return Err(e),
            };
            let children = match self.replay_wal_lazy(&current, base) {
                ArcValue::Null => continue,
                ArcValue::Object(map) | ArcValue::Sentinel(map) => map,
                _ => return Ok(true),
            };
            for (key, child) in children.iter() {
                if child.is_null() {
                    continue;
                }
                let child_path = normalize_path_key(&format!("{current}/{key}"));
                if child.is_primitive() || !self.has_pending_delete_within(&child_path) {
                    return Ok(true);
                }
                to_check.push(child_path);
            }
        }
        Ok(false)
    }

    /// True if a pending WAL entry deletes something at or below `path`.
    fn has_pending_delete_within(&self, path: &str) -> bool {
        let within = |target: &str| target == path || is_below(target, path);
        self.wal_index.find_affecting(path).iter().any(|&idx| {
            let entry = &self.pending_wal_entries[idx];
            let entry_path = normalize_path_key(&entry.path);
            match (&entry.op, &entry.value) {
                (WalOp::Delete, _) => within(&entry_path),
                (WalOp::Set, value) => {
                    let deletes = value.as_ref().is_none_or(stores_nothing);
                    deletes && within(&entry_path)
                }
                (WalOp::Update, Some(Value::Object(updates))) => {
                    let base = entry_path.trim_end_matches('/');
                    updates.iter().any(|(key, value)| {
                        let target = normalize_path_key(&format!("{base}/{key}"));
                        stores_nothing(value) && within(&target)
                    })
                }
                (WalOp::Update, _) => false,
            }
        })
    }

    /// Apply a query to get filtered/sorted results and the ordered keys.
    ///
    /// OPTIMIZATION: Uses lightweight SortEntry to filter/sort first, then only
    /// fetches full values for keys that pass the query. This avoids calling
    /// to_value() on children that will be filtered out.
    ///
    /// Returns (value, ordered_keys) where ordered_keys is the list of keys in sorted order.
    /// Get query result with ordered keys.
    /// OPTIMIZATION: Returns ArcValue directly, using O(1) Arc clones for child values.
    fn get_query_result_with_keys(
        &self,
        path: &Path,
        query: &crate::db::query::Query,
    ) -> (ArcValue, Vec<String>) {
        use crate::db::query::{SortEntry, apply_query_to_sort_entries};
        use std::sync::Arc;

        let tree = self.tree.read().unwrap();
        let node = match tree.get(path) {
            Some(n) => n,
            None => return (ArcValue::Null, Vec::new()),
        };

        // Get children keys
        let children_keys: Vec<String> = node.keys().map(|s| s.to_string()).collect();

        if children_keys.is_empty() {
            // Not an object node, return the value directly (O(1) clone)
            return (node.clone(), Vec::new());
        }

        // Build lightweight sort entries (key + sort value only, no full value copy)
        let sort_entries: Vec<SortEntry> = children_keys
            .iter()
            .filter_map(|key| {
                let child = node.get(key)?;
                // Only extract sort value, not full value
                let sort_value = child.get_sort_value(&query.order_by);
                Some(SortEntry::new(key.clone(), sort_value))
            })
            .collect();

        // Apply query to get filtered/sorted keys
        let filtered_keys = apply_query_to_sort_entries(sort_entries, query);

        // Now fetch full values only for keys in the result
        // OPTIMIZATION: Build ArcValue::Object using O(1) child clones instead of to_value()
        if filtered_keys.is_empty() {
            (ArcValue::Null, Vec::new())
        } else {
            let mut result = HashMap::new();
            for key in &filtered_keys {
                if let Some(child) = node.get(key) {
                    // O(1) Arc clone instead of O(n) to_value()
                    result.insert(key.clone(), child.clone());
                }
            }
            (ArcValue::Object(Arc::new(result)), filtered_keys)
        }
    }

    /// Get query result using pre-computed keys (from a shared view).
    /// This avoids re-sorting when another client already computed the result.
    fn get_result_from_cached_keys(&self, path: &Path, keys: &[String]) -> ArcValue {
        use std::sync::Arc;

        if keys.is_empty() {
            return ArcValue::Null;
        }

        let tree = self.tree.read().unwrap();
        let node = match tree.get(path) {
            Some(n) => n,
            None => return ArcValue::Null,
        };

        let mut result = HashMap::new();
        for key in keys {
            if let Some(child) = node.get(key) {
                result.insert(key.clone(), child.clone());
            }
        }

        if result.is_empty() {
            ArcValue::Null
        } else {
            ArcValue::Object(Arc::new(result))
        }
    }
}
