use super::*;

impl ViewManager {
    /// Create a new view manager.
    pub fn new() -> Self {
        Self {
            shared_views: HashMap::new(),
            by_path: BTreeMap::new(),
            by_client: HashMap::new(),
            total_subscriptions: 0,
            volatile_paths: Vec::new(),
            pending_volatile_views: HashSet::new(),
        }
    }

    /// Get total number of active subscriptions (O(1)).
    pub fn subscription_count(&self) -> usize {
        self.total_subscriptions
    }

    /// Set volatile path patterns.
    pub fn set_volatile_paths(&mut self, patterns: Vec<String>) {
        self.volatile_paths = patterns;
    }

    /// Check if a path matches a volatile pattern.
    fn is_volatile_path(&self, path: &str) -> bool {
        let path_segments: Vec<&str> = path.trim_matches('/').split('/').collect();

        for pattern in &self.volatile_paths {
            let pattern_segments: Vec<&str> = pattern.trim_matches('/').split('/').collect();
            if Self::matches_pattern(&path_segments, &pattern_segments) {
                return true;
            }
        }
        false
    }

    fn matches_pattern(path_segments: &[&str], pattern_segments: &[&str]) -> bool {
        // Path must have at least as many segments as pattern (exact match or child).
        // Volatile cascades: children of volatile paths are also volatile.
        if path_segments.len() < pattern_segments.len() {
            return false;
        }
        // Check that the pattern segments match the beginning of the path
        for (seg, pat) in path_segments.iter().zip(pattern_segments.iter()) {
            if *pat != "*" && !pat.starts_with('$') && *pat != *seg {
                return false;
            }
        }
        true
    }

    /// Subscribe a client to a path with optional query parameters.
    ///
    /// Returns the query_id on success, or a QueryError if the query parameters are invalid.
    /// If another client already has the same (path, query), they share the same SharedView.
    pub fn subscribe(
        &mut self,
        client_id: &str,
        path: &str,
        query_params: Option<&QueryParams>,
        conn: Arc<dyn ConnectionSender>,
    ) -> Result<String, SubscribeError> {
        // Canonicalize the subscribe path so it indexes into `by_path` under
        // the same form `broadcast_mutation` normalizes mutation paths to.
        // Without this, a mutation broadcast at `"/foo"` would miss a subscriber
        // who subscribed via `"/foo/"` (trailing slash) or `"foo"` (no leading
        // slash) — the raw-string prefix match in `find_affected_shared_views`
        // is exact on the key form, so both sides must agree on canonical.
        let path_owned = crate::db::path::normalize_path(path);
        let path = path_owned.as_str();

        let query = match query_params {
            Some(p) => p.to_query()?,
            None => Query::default(),
        };
        // Build the rules-query context now so the view can be re-evaluated
        // against rules later (auth/rules change) without the SUBSCRIBE message.
        // Built unconditionally (not gated on whether the *current* rules use
        // query.*) so it stays correct if a later rules change starts using it.
        let rules_query = query_params.map(|p| p.to_rules_query());
        let is_volatile = self.is_volatile_path(path);
        let tag = query.tag;
        let query_id = query.identifier();
        let view_key = ViewKey::new(path, &query_id);

        // Check if this client is already subscribed to this exact view
        let already_subscribed = self
            .by_client
            .get(client_id)
            .is_some_and(|keys| keys.contains(&view_key));

        if !already_subscribed {
            // Enforce the per-client subscription cap before registering a new
            // view. Idempotent re-subscribes (already_subscribed) are exempt so
            // a client at the cap can still refresh an existing listener.
            let current = self.by_client.get(client_id).map_or(0, |keys| keys.len());
            if current >= MAX_SUBSCRIPTIONS_PER_CLIENT {
                return Err(SubscribeError::TooManySubscriptions {
                    limit: MAX_SUBSCRIPTIONS_PER_CLIENT,
                });
            }

            // Get or create the shared view
            let shared_view = self
                .shared_views
                .entry(view_key.clone())
                .or_insert_with(|| {
                    SharedView::new(path.to_string(), query, is_volatile, rules_query)
                });

            // Add this client as a subscriber
            shared_view.add_subscriber(client_id.to_string(), tag, conn);

            // Update by_path index
            self.by_path
                .entry(path.to_string())
                .or_default()
                .insert(view_key.clone());

            // Update by_client index
            self.by_client
                .entry(client_id.to_string())
                .or_default()
                .insert(view_key);

            // Increment subscription count
            self.total_subscriptions += 1;
        }

        Ok(query_id)
    }

    /// Initialize a query view with its ordered keys.
    /// This is called after the initial snapshot is sent to set up query state.
    pub fn initialize_query_view(
        &mut self,
        _client_id: &str,
        path: &str,
        query_id: &str,
        keys: Vec<String>,
    ) {
        let view_key = ViewKey::new(path, query_id);
        if let Some(view) = self.shared_views.get_mut(&view_key) {
            view.ordered_keys = keys;
        }
    }

    /// Unsubscribe a client from a path (default query).
    pub fn unsubscribe(&mut self, client_id: &str, path: &str) {
        self.unsubscribe_with_query(client_id, path, "default");
    }

    /// Unsubscribe a client from a path with a specific query.
    pub fn unsubscribe_with_query(&mut self, client_id: &str, path: &str, query_id: &str) {
        let view_key = ViewKey::new(path, query_id);
        let mut removed = false;
        let mut view_empty = false;

        // Remove subscriber from shared view
        if let Some(shared_view) = self.shared_views.get_mut(&view_key)
            && shared_view.remove_subscriber(client_id)
        {
            removed = true;
            view_empty = shared_view.is_empty();
        }

        // If shared view is now empty, remove it entirely
        if view_empty {
            self.shared_views.remove(&view_key);
            // Clean up by_path index
            if let Some(keys) = self.by_path.get_mut(path) {
                keys.remove(&view_key);
                if keys.is_empty() {
                    self.by_path.remove(path);
                }
            }
        }

        // Remove from by_client index
        if let Some(keys) = self.by_client.get_mut(client_id) {
            keys.remove(&view_key);
            if keys.is_empty() {
                self.by_client.remove(client_id);
            }
        }

        // Decrement counter if we actually removed something
        if removed {
            self.total_subscriptions = self.total_subscriptions.saturating_sub(1);
        }
    }

    /// List a client's active subscriptions as `(path, query_id, rules_query)`.
    ///
    /// Used to re-evaluate a client's live views against `can_read` after an
    /// auth or rules change. `rules_query` is the query context captured at
    /// subscribe time (see [`SharedView::rules_query`]), so the re-check matches
    /// the original permission decision's query inputs.
    pub fn list_client_subscriptions(&self, client_id: &str) -> Vec<ClientSubscription> {
        let Some(view_keys) = self.by_client.get(client_id) else {
            return Vec::new();
        };
        view_keys
            .iter()
            .map(|vk| {
                let rules_query = self
                    .shared_views
                    .get(vk)
                    .and_then(|sv| sv.rules_query.clone());
                (vk.path.clone(), vk.query_id.clone(), rules_query)
            })
            .collect()
    }

    /// List every (client_id, path, query_id, rules_query) across all active
    /// subscriptions. Used to re-evaluate all views after a rules change.
    pub fn list_all_subscriptions(&self) -> Vec<GlobalSubscription> {
        let mut out = Vec::new();
        for (client_id, view_keys) in &self.by_client {
            for vk in view_keys {
                let rules_query = self
                    .shared_views
                    .get(vk)
                    .and_then(|sv| sv.rules_query.clone());
                out.push((
                    client_id.clone(),
                    vk.path.clone(),
                    vk.query_id.clone(),
                    rules_query,
                ));
            }
        }
        out
    }

    /// Unsubscribe a client from all paths.
    pub fn unsubscribe_all(&mut self, client_id: &str) {
        if let Some(view_keys) = self.by_client.remove(client_id) {
            let removed_count = view_keys.len();

            for view_key in view_keys {
                // Remove subscriber from shared view
                let mut view_empty = false;
                if let Some(shared_view) = self.shared_views.get_mut(&view_key) {
                    shared_view.remove_subscriber(client_id);
                    view_empty = shared_view.is_empty();
                }

                // If shared view is now empty, remove it entirely
                if view_empty {
                    self.shared_views.remove(&view_key);
                    // Clean up by_path index
                    if let Some(keys) = self.by_path.get_mut(&view_key.path) {
                        keys.remove(&view_key);
                        if keys.is_empty() {
                            self.by_path.remove(&view_key.path);
                        }
                    }
                }
            }

            // Decrement counter by the number of subscriptions removed
            self.total_subscriptions = self.total_subscriptions.saturating_sub(removed_count);
        }
    }

    /// Get a shared view by path and query ID.
    pub fn get_shared_view(&self, path: &str, query_id: &str) -> Option<&SharedView> {
        let view_key = ViewKey::new(path, query_id);
        self.shared_views.get(&view_key)
    }

    /// Get a mutable shared view by path and query ID.
    fn get_shared_view_mut(&mut self, path: &str, query_id: &str) -> Option<&mut SharedView> {
        let view_key = ViewKey::new(path, query_id);
        self.shared_views.get_mut(&view_key)
    }

    // Legacy compatibility: get_view returns a View-like accessor
    // This is used by some methods that need per-client info (like tag)
    /// Get a view by client, path, and query ID.
    /// Returns None if the client is not subscribed to this view.
    pub fn get_view(&self, client_id: &str, path: &str, query_id: &str) -> Option<ViewRef<'_>> {
        let view_key = ViewKey::new(path, query_id);
        let shared_view = self.shared_views.get(&view_key)?;
        let subscriber = shared_view.subscribers.get(client_id)?;
        Some(ViewRef {
            shared_view,
            subscriber,
        })
    }

    /// Get a mutable view by client, path, and query ID.
    pub(super) fn get_view_mut(
        &mut self,
        _client_id: &str,
        path: &str,
        query_id: &str,
    ) -> Option<&mut SharedView> {
        self.get_shared_view_mut(path, query_id)
    }
}
