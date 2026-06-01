use super::*;

impl Database {
    /// Evict promoted paths that have been idle for longer than the eviction timeout.
    /// Replaces the node with an empty Sentinel, freeing the in-memory subtree.
    /// Re-promotion from blob + WAL replay restores the data on next access.
    pub(super) fn evict_idle_paths(&mut self) {
        let now = Instant::now();
        let idle_timeout =
            Duration::from_secs(EVICTION_IDLE_SECS.load(std::sync::atomic::Ordering::Relaxed));

        // Partition into idle and hot
        let mut idle_paths = Vec::new();
        let mut hot_paths = std::collections::HashSet::new();

        for (path, last_promoted) in &self.promoted_paths {
            if now.duration_since(*last_promoted) >= idle_timeout {
                idle_paths.push(path.clone());
            } else {
                hot_paths.insert(path.clone());
            }
        }

        if idle_paths.is_empty() {
            return;
        }

        let mut tree = self.tree.write().unwrap();
        let mut evicted_count = 0usize;

        for path in &idle_paths {
            // Check if any hot path is at or under this path
            let has_hot_descendant = if path == "/" {
                // Any hot path is a descendant of root
                !hot_paths.is_empty()
            } else {
                hot_paths.iter().any(|hp| is_path_descendant(path, hp))
            };

            if !has_hot_descendant {
                // Safe to evict entirely — replace with Sentinel
                let path_obj = Path::parse(path);
                tree.set_arc_uncleaned_lazy(&path_obj, ArcValue::empty_sentinel());
                // Clear stale descendant entries — the entire subtree is now one sentinel
                if path == "/" {
                    self.sentinel_paths.clear();
                } else {
                    let prefix = format!("{}/", path);
                    let stale: Vec<String> = self
                        .sentinel_paths
                        .range::<String, _>(&prefix..)
                        .take_while(|p| p.starts_with(&prefix))
                        .cloned()
                        .collect();
                    for p in stale {
                        self.sentinel_paths.remove(&p);
                    }
                }
                self.sentinel_paths.insert(path.clone());
                evicted_count += 1;
            } else {
                // Has hot descendants — prune only cold branches
                evicted_count += Self::selective_evict_children(
                    &mut tree,
                    &mut path.clone(),
                    &hot_paths,
                    &mut self.sentinel_paths,
                );
            }
        }
        drop(tree);

        // Remove idle paths from tracking
        for path in &idle_paths {
            self.promoted_paths.remove(path);
        }

        if evicted_count > 0 {
            info!(
                "[Eviction] {}: Evicted {} subtree(s) ({} idle path(s) processed)",
                self.id,
                evicted_count,
                idle_paths.len()
            );
        }
    }

    /// Walk children of a node and replace cold branches with Sentinels.
    /// A branch is "hot" if any path in `hot_paths` is at or under it.
    /// Returns the number of subtrees replaced with Sentinels.
    fn selective_evict_children(
        tree: &mut Tree,
        path: &mut String,
        hot_paths: &std::collections::HashSet<String>,
        sentinel_paths: &mut BTreeSet<String>,
    ) -> usize {
        // For each immediate child key, classify it as one of:
        //   - hot leaf:     the child path itself is a hot path → preserve as-is.
        //   - hot ancestor: the child is on the path to a deeper hot path → recurse.
        //   - cold:         neither → replace with empty Sentinel.
        //
        // The distinction matters because recursing into a hot leaf would walk
        // its primitive fields and Sentinel-clobber them (they have no further
        // hot descendants from the recursion's point of view).
        let path_prefix = if path == "/" { "/" } else { path.as_str() };
        let mut hot_leaf_children: std::collections::HashSet<&str> =
            std::collections::HashSet::new();
        let mut hot_ancestor_children: std::collections::HashSet<&str> =
            std::collections::HashSet::new();
        for hp in hot_paths {
            let suffix = if path_prefix == "/" {
                hp.strip_prefix('/')
            } else {
                hp.strip_prefix(path_prefix)
                    .and_then(|s| s.strip_prefix('/'))
            };
            if let Some(rest) = suffix
                && let Some(seg) = rest.split('/').next()
                && !seg.is_empty()
            {
                if rest.len() == seg.len() {
                    // hp is exactly path/seg — child IS the hot path
                    hot_leaf_children.insert(seg);
                } else {
                    // hp is path/seg/... — child is on the way to a deeper hot path
                    hot_ancestor_children.insert(seg);
                }
            }
        }

        // Collect child keys (must drop tree borrow before mutating)
        let path_obj = Path::parse(path);
        let child_keys: Vec<String> = match tree.get(&path_obj) {
            Some(ArcValue::Object(map) | ArcValue::Sentinel(map)) => map.keys().cloned().collect(),
            _ => return 0,
        };

        let base_len = path.len();
        let mut evicted = 0;
        for key in child_keys {
            // Build child path in-place to avoid allocations
            if base_len == 1 {
                // path is "/"
                path.push_str(&key);
            } else {
                path.push('/');
                path.push_str(&key);
            }

            if hot_leaf_children.contains(key.as_str()) {
                // Child IS a hot path — preserve its subtree untouched.
            } else if hot_ancestor_children.contains(key.as_str()) {
                // Hot descendant somewhere below — recurse to prune selectively
                evicted += Self::selective_evict_children(tree, path, hot_paths, sentinel_paths);
            } else {
                // Cold branch — replace with Sentinel (frees all descendants)
                let child_path_obj = Path::parse(path);
                tree.set_arc_uncleaned_lazy(&child_path_obj, ArcValue::empty_sentinel());
                // Clear stale descendant entries before inserting the new one
                let prefix = format!("{}/", &path);
                let stale: Vec<String> = sentinel_paths
                    .range::<String, _>(&prefix..)
                    .take_while(|p| p.starts_with(&prefix))
                    .cloned()
                    .collect();
                for p in stale {
                    sentinel_paths.remove(&p);
                }
                sentinel_paths.insert(path.clone());
                evicted += 1;
            }

            // Restore path buffer for next sibling
            path.truncate(base_len);
        }

        evicted
    }

    /// Force-evict ALL promoted paths immediately (ignoring idle timeout).
    /// Used for testing eviction/re-promotion edge cases.
    pub(super) fn force_evict_all_paths(&mut self) {
        if self.promoted_paths.is_empty() {
            return;
        }

        let paths: Vec<String> = self.promoted_paths.keys().cloned().collect();
        let mut tree = self.tree.write().unwrap();

        for path in &paths {
            let path_obj = Path::parse(path);
            tree.set_arc_uncleaned_lazy(&path_obj, ArcValue::empty_sentinel());
            // Clear stale descendant entries
            if path == "/" {
                self.sentinel_paths.clear();
            } else {
                let prefix = format!("{}/", path);
                let stale: Vec<String> = self
                    .sentinel_paths
                    .range::<String, _>(&prefix..)
                    .take_while(|p| p.starts_with(&prefix))
                    .cloned()
                    .collect();
                for p in stale {
                    self.sentinel_paths.remove(&p);
                }
            }
            self.sentinel_paths.insert(path.clone());
            debug!("[Eviction] {}: Force-evicted path {}", self.id, path);
        }
        drop(tree);

        self.promoted_paths.clear();

        info!(
            "[Eviction] {}: Force-evicted {} path(s)",
            self.id,
            paths.len()
        );
    }
}
