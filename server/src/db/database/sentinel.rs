use super::*;

impl Database {
    // =========================================================================
    // Sentinel Path Tracking
    // =========================================================================

    /// Lazy-tree invariant: `sentinel_paths` must be a superset of every
    /// actual `Sentinel` node in the in-memory tree. Stale-extra entries are
    /// tolerated (waste reads); missing entries cause skipped promotions and
    /// silent wrong reads.
    ///
    /// Walks the tree and returns every path whose tree node is a `Sentinel`
    /// (empty or with-children) but whose path is NOT in `sentinel_paths`.
    /// An empty return value means the invariant holds for this snapshot.
    ///
    /// O(tree size). Intended as a test-only safety net — callers should
    /// invoke it after a mutation sequence and assert the result is empty:
    ///
    /// ```ignore
    /// // somewhere in a test, after a sequence of writes/promotions:
    /// let violations = db.find_sentinel_tracking_violations();
    /// assert!(violations.is_empty(), "sentinel tracking violation: {:?}", violations);
    /// ```
    ///
    /// Exposed unconditionally (rather than gated on `cfg(test)`) so chaos-monkey
    /// and integration tests can call it the same way unit tests do.
    #[doc(hidden)]
    pub fn find_sentinel_tracking_violations(&self) -> Vec<String> {
        let mut violations = Vec::new();
        let tree = self.tree.read().unwrap();
        let mut path_buf = String::new();
        Self::walk_tree_for_sentinels(
            tree.root(),
            &mut path_buf,
            &self.sentinel_paths,
            &mut violations,
        );
        violations
    }

    /// Recursive helper for `find_sentinel_tracking_violations`. Walks the
    /// tree, accumulating the path, and records a violation whenever a
    /// `Sentinel` node's path is missing from `sentinel_paths`. Walks into
    /// both `Object` and `Sentinel` containers (Sentinels-with-children also
    /// have descendants worth checking).
    fn walk_tree_for_sentinels(
        node: &ArcValue,
        path_buf: &mut String,
        sentinel_paths: &BTreeSet<String>,
        violations: &mut Vec<String>,
    ) {
        if matches!(node, ArcValue::Sentinel(_)) {
            let normalized = if path_buf.is_empty() {
                "/".to_string()
            } else {
                path_buf.clone()
            };
            if !sentinel_paths.contains(&normalized) {
                violations.push(normalized);
            }
        }
        if let ArcValue::Object(map) | ArcValue::Sentinel(map) = node {
            let base_len = path_buf.len();
            for (key, child) in map.iter() {
                path_buf.push('/');
                path_buf.push_str(key);
                Self::walk_tree_for_sentinels(child, path_buf, sentinel_paths, violations);
                path_buf.truncate(base_len);
            }
        }
    }

    /// Walk `node` and insert the path of every `Sentinel` found into `out`.
    /// `path_buf` accumulates the path being walked; pass an empty string for
    /// root, or the path to `node` for a subtree. Insertions use the canonical
    /// form: `"/"` for root, `"/a/b/c"` otherwise.
    ///
    /// Used by `promote_path_shallow` to keep `sentinel_paths` a superset of
    /// every Sentinel in the newly-promoted subtree, including deep
    /// intermediates created by lazy WAL replay.
    pub(super) fn collect_sentinel_paths(
        node: &ArcValue,
        path_buf: &mut String,
        out: &mut BTreeSet<String>,
    ) {
        if matches!(node, ArcValue::Sentinel(_)) {
            let canonical = if path_buf.is_empty() {
                "/".to_string()
            } else {
                path_buf.clone()
            };
            out.insert(canonical);
        }
        if let ArcValue::Object(map) | ArcValue::Sentinel(map) = node {
            let base_len = path_buf.len();
            for (key, child) in map.iter() {
                path_buf.push('/');
                path_buf.push_str(key);
                Self::collect_sentinel_paths(child, path_buf, out);
                path_buf.truncate(base_len);
            }
        }
    }

    /// Check if there are any sentinels at or below `path` in the tree.
    /// O(log n) BTreeSet range query instead of O(tree_size) recursive walk.
    pub(super) fn has_sentinel_at_or_below(&self, path: &str) -> bool {
        // Exact match
        if self.sentinel_paths.contains(path) {
            return true;
        }
        // Check for any descendant: entries starting with "{path}/"
        let prefix = if path == "/" {
            "/".to_string()
        } else {
            format!("{}/", path)
        };
        self.sentinel_paths
            .range::<String, _>(&prefix..)
            .next()
            .is_some_and(|p| p.starts_with(&prefix))
    }

    /// After a `set_lazy` write, walk the ancestors of the written path
    /// and record any that are Sentinels in `sentinel_paths`.
    /// O(depth) — typically 3-5 tree lookups.
    pub(super) fn track_sentinels_after_write(&mut self, path_str: &str) {
        let tree = self.tree.read().unwrap();
        let segments: Vec<&str> = path_str.split('/').filter(|s| !s.is_empty()).collect();

        // Check each ancestor (not the leaf itself — that has the real value).
        let mut current = String::new();
        for seg in &segments[..segments.len().saturating_sub(1)] {
            current.push('/');
            current.push_str(seg);
            let path_obj = Path::parse(&current);
            if let Some(node) = tree.get(&path_obj)
                && node.is_sentinel()
            {
                self.sentinel_paths.insert(current.clone());
            }
        }

        // Also check root — it may be a Sentinel
        if tree.root().is_sentinel() {
            self.sentinel_paths.insert("/".to_string());
        }
    }

    /// Remove sentinel tracking for a path and all its descendants (range removal).
    /// Used after deep/unchecked promotion replaces a full subtree with real data.
    pub(super) fn remove_sentinel_paths_below(&mut self, path: &str) {
        self.sentinel_paths.remove(path);
        if path == "/" {
            self.sentinel_paths.clear();
        } else {
            let prefix = format!("{}/", path);
            let to_remove: Vec<String> = self
                .sentinel_paths
                .range::<String, _>(&prefix..)
                .take_while(|p| p.starts_with(&prefix))
                .cloned()
                .collect();
            for p in to_remove {
                self.sentinel_paths.remove(&p);
            }
        }
    }
}
