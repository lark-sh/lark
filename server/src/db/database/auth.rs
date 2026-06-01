use super::*;

impl Database {
    /// Get a handle to send messages to this database.
    pub fn handle(&self) -> DatabaseHandle {
        DatabaseHandle {
            id: self.id.clone(),
            inbox: self.inbox_sender.clone(),
        }
    }

    /// Set volatile path patterns (called when rules are loaded).
    pub fn set_volatile_paths(&mut self, patterns: Vec<String>) {
        self.volatile_paths = patterns.clone();
        self.view_manager.set_volatile_paths(patterns);
    }

    /// Set security rules for this database.
    pub fn set_rules(&mut self, rules: RuleSet) {
        // Extract volatile paths from rules
        let volatile_paths = rules.get_volatile_paths();
        self.set_volatile_paths(volatile_paths);

        // Set the evaluator
        self.evaluator = Some(Rc::new(Evaluator::new(rules)));
    }

    /// Set the evaluator directly (used when evaluator already exists in config).
    pub fn set_evaluator(&mut self, evaluator: Evaluator) {
        // Extract volatile paths from the evaluator's rules
        let volatile_paths = evaluator.get_volatile_paths();
        self.set_volatile_paths(volatile_paths);

        // Set the evaluator
        self.evaluator = Some(Rc::new(evaluator));
    }

    /// Check if a read is allowed for the given client and path.
    ///
    /// This is async because rules evaluation may hit sentinel/unloaded data that needs
    /// to be fetched from blob storage. Each fetch is async and yields to other databases.
    ///
    /// The retry loop (MAX_PROMOTION_RETRIES) handles rules that access many
    /// unloaded paths - each path is loaded from blob and we retry evaluation.
    pub(super) async fn can_read(
        &mut self,
        client_id: &str,
        path: &str,
        query: Option<Arc<HashMap<String, serde_json::Value>>>,
    ) -> bool {
        let evaluator = match self.evaluator.clone() {
            Some(e) => e,
            None => return true, // No rules = allow all
        };

        let client = match self.clients.get(client_id) {
            Some(c) => c,
            None => return false, // Unknown client
        };

        // Use cached rules_auth to avoid repeated conversion
        let auth = client.rules_auth.clone();

        // Create tree accessor for lazy data access in rules (data.*, root.*)
        let tree_accessor: Arc<dyn TreeGetter> =
            Arc::new(TreeAccessor::new(self.tree.clone(), self.is_blob_backed()));

        let ctx = RulesContext {
            auth,
            root_tree: Some(tree_accessor),
            path: path.to_string(),
            new_data: None,
            is_volatile: self.is_volatile_path(path),
            database_id: self.pure_database_id.clone(),
            project_id: self.project_id.clone(),
            query,
        };

        // Retry loop for rules that access unloaded blob data.
        // Each iteration loads one path from blob and retries evaluation.
        for _attempt in 0..MAX_PROMOTION_RETRIES {
            match evaluator.can_read(&ctx) {
                Ok(allowed) => return allowed,
                Err(needs) => {
                    match self.load_from_blob(&needs.path).await {
                        Ok(did_promote) => {
                            if did_promote {
                                trace!(
                                    path = %needs.path,
                                    "Loading blob data for rules eval (read)"
                                );
                            }
                            continue; // Retry evaluation
                        }
                        Err(e) => {
                            warn!(
                                "Failed to load blob data at {} for read {}: {}",
                                needs.path, path, e
                            );
                            return false;
                        }
                    }
                }
            }
        }

        warn!(path, "Rules eval exceeded max retries for read");
        false
    }

    /// Get a human-readable summary of a client's auth state for logging.
    pub(super) fn get_auth_summary(&self, client_id: &str) -> String {
        let client = match self.clients.get(client_id) {
            Some(c) => c,
            None => return "unknown client".to_string(),
        };

        match &client.rules_auth {
            None => "unauthenticated".to_string(),
            Some(auth) => {
                let mut parts = vec![];

                if let Some(ref uid) = auth.uid {
                    parts.push(format!("uid={}", uid));
                }

                if let Some(ref provider) = auth.provider {
                    parts.push(format!("provider={}", provider));
                }

                if auth.is_true_admin {
                    parts.push("is_admin=true".to_string());
                }

                // Include custom token claims if present
                if let Some(ref token) = auth.token {
                    for (key, value) in token.iter() {
                        // Skip standard JWT claims, only show custom ones
                        if !["uid", "provider", "iat", "exp", "aud", "iss", "sub"]
                            .contains(&key.as_str())
                        {
                            let val_str = match value {
                                serde_json::Value::String(s) => s.clone(),
                                serde_json::Value::Bool(b) => b.to_string(),
                                serde_json::Value::Number(n) => n.to_string(),
                                _ => format!("{}", value),
                            };
                            parts.push(format!("{}={}", key, val_str));
                        }
                    }
                }

                if parts.is_empty() {
                    "authenticated (no claims)".to_string()
                } else {
                    parts.join(", ")
                }
            }
        }
    }

    /// Check if a write is allowed for the given client and path.
    ///
    /// This is async because rules evaluation may hit sentinel/unloaded data that needs
    /// to be fetched from blob storage. Each fetch is async and yields to other databases.
    ///
    /// The retry loop (MAX_PROMOTION_RETRIES) handles rules that access many
    /// unloaded paths - each path is loaded from blob and we retry evaluation.
    pub(super) async fn can_write(
        &mut self,
        client_id: &str,
        path: &str,
        new_data: Option<NewData>,
    ) -> bool {
        let evaluator = match self.evaluator.clone() {
            Some(e) => e,
            None => return true, // No rules = allow all
        };

        let client = match self.clients.get(client_id) {
            Some(c) => c,
            None => {
                trace!(
                    "can_write DENIED: unknown client {} for path {} in {}",
                    client_id, path, self.id
                );
                return false;
            }
        };

        // Use cached rules_auth to avoid repeated conversion
        let auth = client.rules_auth.clone();

        // Create tree accessor for lazy data access in rules (data.*, root.*)
        let tree_accessor: Arc<dyn TreeGetter> =
            Arc::new(TreeAccessor::new(self.tree.clone(), self.is_blob_backed()));

        let ctx = RulesContext {
            auth: auth.clone(),
            root_tree: Some(tree_accessor),
            path: path.to_string(),
            new_data,
            is_volatile: self.is_volatile_path(path),
            database_id: self.pure_database_id.clone(),
            project_id: self.project_id.clone(),
            query: None, // Writes don't use query-based rules
        };

        // Retry loop for rules that access unloaded blob data.
        // Each iteration loads one path from blob and retries evaluation.
        for _attempt in 0..MAX_PROMOTION_RETRIES {
            match evaluator.can_write(&ctx) {
                Ok(allowed) => {
                    if !allowed {
                        let auth_uid = auth
                            .as_ref()
                            .and_then(|a| a.uid.as_ref())
                            .map(|s| s.as_str())
                            .unwrap_or("<none>");
                        let auth_provider = auth
                            .as_ref()
                            .and_then(|a| a.provider.as_ref())
                            .map(|s| s.as_str())
                            .unwrap_or("<none>");
                        trace!(
                            "can_write DENIED by rules: path={} auth.uid={} auth.provider={} in {}",
                            path, auth_uid, auth_provider, self.id
                        );
                    }
                    return allowed;
                }
                Err(needs) => {
                    match self.load_from_blob(&needs.path).await {
                        Ok(did_promote) => {
                            if did_promote {
                                trace!(
                                    path = %needs.path,
                                    "Loading blob data for rules eval (write)"
                                );
                            }
                            continue; // Retry evaluation
                        }
                        Err(e) => {
                            warn!(
                                "Failed to load blob data at {} for write {}: {}",
                                needs.path, path, e
                            );
                            return false;
                        }
                    }
                }
            }
        }

        warn!(path, "Rules eval exceeded max retries for write");
        false
    }

    /// Convert database AuthInfo to rules AuthInfo.
    /// This is a static function so it can be called when caching rules_auth.
    /// The returned AuthInfo has its JSON representation pre-computed for efficient rules evaluation.
    /// Wrapped in Arc for O(1) cloning during rules evaluation.
    pub(super) fn convert_auth_to_rules(auth: &AuthInfo) -> Arc<RulesAuthInfo> {
        let mut token = serde_json::Map::new();
        for (k, v) in &auth.token {
            token.insert(k.clone(), v.clone());
        }

        // Normalize an empty uid to absent. Firebase Legacy Tokens authenticate
        // with uid == "" (identity lives in the `d` claims), so the principal is
        // still authenticated via its token, but `auth.uid` must read as null —
        // otherwise a rule like `auth.uid === $uid` would spuriously match an
        // empty captured path segment. See convert_auth (truly anonymous users
        // are already dropped there).
        let uid = if auth.uid.is_empty() {
            None
        } else {
            Some(auth.uid.clone())
        };

        Arc::new(RulesAuthInfo::new(
            uid,
            Some(auth.provider.clone()),
            if token.is_empty() { None } else { Some(token) },
            auth.is_admin,
        ))
    }
}
