use super::*;

impl Database {
    // =========================================================================
    // Blob Data Loading
    // =========================================================================

    /// Ensure the data at `path` is materialized (not a Sentinel or unknown).
    ///
    /// If the node at `path` is already real data, this is a no-op.
    /// Otherwise, reads from blob, replays in-memory WAL entries, and inserts
    /// the result into the tree.
    ///
    /// Returns Ok(true) if data was loaded, Ok(false) if no loading needed.
    /// Returns Err if blob I/O fails.
    pub(super) async fn promote_path(&mut self, path: &str) -> Result<bool, String> {
        // If not blob-backed, nothing to promote
        if self.blob_session.is_none() {
            return Ok(false);
        }

        // Check if this node already has real (non-Sentinel) data in the tree.
        // If so, no promotion needed. (This is a shallow check — only the top node.
        // For full-subtree guarantees, use promote_path_deep.)
        {
            let tree = self.tree.read().unwrap();
            let path_obj = Path::parse(path);
            match tree.get(&path_obj) {
                Some(node) if !node.is_sentinel() => {
                    drop(tree);
                    if let Some(ts) = self.promoted_paths.get_mut(&normalize_path_key(path)) {
                        *ts = Instant::now();
                    }
                    return Ok(false);
                }
                None => {
                    // Node is absent (not even a Sentinel). Check if the parent is
                    // a loaded container (Object). If so, the parent has complete
                    // knowledge of its children — an absent child definitively does
                    // not exist. No blob read needed.
                    //
                    // Why this is safe:
                    // - A non-Sentinel parent was either promoted (all children loaded
                    //   from blob + WAL) or written via SET (full replacement).
                    // - If a child were evicted, it would be a Sentinel, not absent.
                    // - If a child were deleted, it is correctly absent.
                    //
                    // IMPORTANT: only write the Null marker if the parent is an Object.
                    // If the parent is a primitive (Null/Bool/Number/String),
                    // the child definitively doesn't exist, but `set_arc_uncleaned_lazy`
                    // would clobber the primitive into a Sentinel container (see
                    // ArcValue::set_path_mut_sentinel's primitive branch), corrupting
                    // the tree. Skip the marker write in that case — the next read
                    // will do the same cheap check and arrive at the same answer.
                    if let Some(parent) = path_obj.parent()
                        && let Some(parent_node) = tree.get(&parent)
                    {
                        if parent_node.is_object() {
                            // Drop read lock, insert Null to mark "we checked"
                            drop(tree);
                            let mut tree = self.tree.write().unwrap();
                            tree.set_arc_uncleaned_lazy(&path_obj, ArcValue::Null);
                            return Ok(false);
                        }
                        if !parent_node.is_sentinel() {
                            // Parent is a primitive — child can't exist.
                            // No marker write (would corrupt parent). No blob read.
                            return Ok(false);
                        }
                    }
                    // Parent is Sentinel or absent — need to load from blob
                }
                _ => {
                    // Sentinel — need to load from blob
                }
            }
        }

        self.promote_path_shallow(path).await
    }

    /// Deep promotion: ensures the entire subtree at `path` is Sentinel-free.
    ///
    /// Used by subscribe(), once(), and query view recompute — operations that
    /// Check the blob's on-disk subtree size at a path before promoting.
    /// Returns true if the subtree is too large to serve (exceeds the response size limit).
    /// Uses blob `navigate()` which only reads headers — no data is loaded.
    ///
    /// The blob subtree_size is the binary on-disk size, which is smaller than the
    /// JSON serialization size. We use a 1.5x multiplier on MAX_RESPONSE_SIZE as the
    /// threshold: if the raw blob bytes alone exceed that, the JSON response will
    /// certainly exceed the limit.
    pub(super) async fn blob_subtree_exceeds_limit(&self, path: &str) -> bool {
        let session = match &self.blob_session {
            Some(s) => s,
            None => return false, // Ephemeral DB, no blob to check
        };

        let path_obj = Path::parse(path);
        let segments: Vec<&str> = path_obj.segments().iter().map(|s| s.as_ref()).collect();
        let blob_path = if path == "/" { vec![] } else { segments };

        match session.navigate(&blob_path).await {
            Ok(location) => {
                let limit = crate::protocol::MAX_RESPONSE_SIZE as u64 * 3 / 2;
                if location.subtree_size > limit {
                    warn!(
                        "[Size Check] {}: blob subtree at {} is {} bytes (limit {}), rejecting before promotion",
                        self.id, path, location.subtree_size, limit
                    );
                    return true;
                }
                false
            }
            Err(BlobError::PathNotFound(_)) => false, // Doesn't exist in blob, can't be too large
            Err(_) => false, // Navigate failed, let promotion handle the error
        }
    }

    /// need to serialize or iterate the full subtree. Unlike `promote_path()`,
    /// this checks for Sentinel descendants (not just the top node) and does a
    /// full blob read + WAL replay if any are found.
    pub(super) async fn promote_path_deep(&mut self, path: &str) -> Result<bool, String> {
        // If not blob-backed, nothing to promote
        if self.blob_session.is_none() {
            return Ok(false);
        }

        // Check if any Sentinel exists at or below this path — O(log n) BTreeSet
        // range query instead of the old O(tree_size) recursive contains_sentinel() walk.
        let needs_promotion = if self.has_sentinel_at_or_below(path) {
            true
        } else {
            // No sentinels in this subtree. But the node might be absent —
            // check if the parent is loaded so we can definitively say "doesn't exist."
            let tree = self.tree.read().unwrap();
            let path_obj = Path::parse(path);
            match tree.get(&path_obj) {
                Some(node) if !node.is_sentinel() => false, // Node exists and has no sentinels below — fully loaded
                Some(_) => {
                    // I3 invariant violation: the tree has a Sentinel at this
                    // path but `sentinel_paths` doesn't track it. We promote
                    // defensively so the read returns correct data, but warn
                    // loudly — some mutation site is creating a Sentinel
                    // without keeping `sentinel_paths` in sync. Each repeated
                    // hit on the same path means the same upstream bug, so
                    // include enough context to chase it.
                    drop(tree);
                    warn!(
                        db = %self.id,
                        path = %path,
                        "I3 invariant violation: untracked Sentinel at {} (tree has Sentinel, \
                         sentinel_paths does not). Promoting defensively. Find the mutation site \
                         that created this Sentinel without calling track_sentinels_after_write \
                         or otherwise updating sentinel_paths.",
                        path
                    );
                    true
                }
                None => {
                    // Same parent-container check as in promote_path: only write the
                    // Null marker when the parent is an Object. A primitive
                    // parent means the child definitively doesn't exist, but writing
                    // Null through `set_path_mut_sentinel` would clobber the parent
                    // into a Sentinel — see comment in promote_path for the full
                    // story.
                    if let Some(parent) = path_obj.parent()
                        && let Some(parent_node) = tree.get(&parent)
                    {
                        if parent_node.is_object() {
                            drop(tree);
                            let mut tree = self.tree.write().unwrap();
                            tree.set_arc_uncleaned_lazy(&path_obj, ArcValue::Null);
                            return Ok(false);
                        }
                        if !parent_node.is_sentinel() {
                            // Primitive/Array parent — child can't exist. Skip.
                            return Ok(false);
                        }
                    }
                    // Parent is Sentinel or absent — need to load
                    true
                }
            }
        };

        if !needs_promotion {
            if let Some(ts) = self.promoted_paths.get_mut(&normalize_path_key(path)) {
                *ts = Instant::now();
            }
            return Ok(false);
        }

        // Force a full promotion: read from blob + replay WAL, replacing the subtree.
        // This is the same logic as promote_path but without the early bail-out.
        self.promote_path_unchecked(path).await
    }

    /// Unconditional promotion: always reads from blob + replays WAL at the given path.
    /// Used by `promote_path_deep` when Sentinels are detected, and by `promote_path`
    /// when the top-level node is Sentinel.
    async fn promote_path_unchecked(&mut self, path: &str) -> Result<bool, String> {
        let promote_start = Instant::now();

        let session = match &self.blob_session {
            Some(s) => s,
            _ => return Ok(false),
        };

        // Step 1: Read subtree from blob
        let read_start = Instant::now();
        let _ = session.io().take_read_stats(); // reset counters
        let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
        let blob_value = match session.read_subtree(&segments).await {
            Ok(value) => value,
            Err(BlobError::PathNotFound(_)) => {
                // Path doesn't exist in blob — start with Null
                ArcValue::Null
            }
            Err(e) => return Err(format!("Blob read failed at {}: {}", path, e)),
        };
        let read_elapsed = read_start.elapsed();
        let io_stats = session.io().take_read_stats();

        // Step 2: Build a temporary tree from the blob data and replay matching WAL entries
        let path_obj = Path::parse(path);
        let mut temp_tree = Tree::new();
        temp_tree.set_arc_uncleaned(&path_obj, blob_value);

        // Use the indexed lookup instead of scanning all entries
        let matching_indices = self.wal_index.find_affecting(path);
        for &idx in &matching_indices {
            let entry = &self.pending_wal_entries[idx];
            let entry_path = Path::parse(&entry.path);
            match entry.op {
                WalOp::Set => {
                    // `value: None` means SET-to-null (serde collapses
                    // `{"v":null}` into `None`). Modern writers canonicalize
                    // this to `WalOp::Delete` in `wal_write_set`, but old WAL
                    // entries on disk may still have the SET-with-null form;
                    // map None → Null so `tree.set` cleans it to a delete.
                    let value = entry.value.clone().unwrap_or(Value::Null);
                    temp_tree.set(&entry_path, value);
                }
                WalOp::Update => {
                    if let Some(Value::Object(ref updates)) = entry.value {
                        temp_tree.update(&entry_path, updates);
                    }
                }
                WalOp::Delete => {
                    temp_tree.remove(&entry_path);
                }
            }
        }

        // Step 3: Extract the promoted value and set it in the real tree.
        let promoted_value = temp_tree.get_arc(&path_obj).unwrap_or(ArcValue::Null);

        {
            let mut tree = self.tree.write().unwrap();
            tree.set_arc_uncleaned_lazy(&path_obj, promoted_value);
        }

        // set_arc_uncleaned_lazy may have created Sentinel intermediates along the
        // path (via set_path_mut_sentinel). Track those in sentinel_paths.
        self.track_sentinels_after_write(path);

        // Track this promotion for eviction timing
        self.promoted_paths
            .insert(normalize_path_key(path), Instant::now());

        // Remove sentinel tracking for this path and all descendants —
        // the subtree has been fully replaced with real data from blob + WAL.
        self.remove_sentinel_paths_below(path);

        // Record promotion stats
        let total_elapsed = promote_start.elapsed();
        self.promotion_stats
            .record(total_elapsed, read_elapsed, io_stats);

        Ok(true)
    }

    /// Shallow promotion: reads only immediate children from blob, not the full subtree.
    ///
    /// For primitive values at `path`, inserts the value directly.
    /// For containers, inserts primitive children as real values and container
    /// children as Sentinels (to be loaded on demand later).
    ///
    /// WAL entries are replayed on top to ensure the shallow view is up-to-date.
    /// This is much cheaper than `promote_path_unchecked` because it avoids
    /// allocating the full BTreeMap hierarchy for deep subtrees.
    pub(super) async fn promote_path_shallow(&mut self, path: &str) -> Result<bool, String> {
        let promote_start = Instant::now();

        let session = match &self.blob_session {
            Some(s) => s,
            _ => return Ok(false),
        };

        // Step 1: Shallow read from blob
        let read_start = Instant::now();
        let _ = session.io().take_read_stats(); // reset counters
        let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
        let shallow_result = match session.read_shallow(&segments).await {
            Ok(value) => value,
            Err(BlobError::PathNotFound(_)) => {
                // Path doesn't exist in blob. Delegate to `promote_path_unchecked`,
                // which seeds an empty subtree with `Null` and replays any
                // affecting WAL entries on top. That handles both cases
                // correctly:
                //   - WAL has writes → the in-memory data those writes
                //     produced is preserved (a bare Null marker would
                //     clobber it via `set_path_mut_sentinel`'s leaf
                //     assignment, replacing the WAL-built Sentinel
                //     container with Null).
                //   - WAL has nothing → temp_tree stays Null, and the
                //     install is `tree.set_arc_uncleaned_lazy(path, Null)`
                //     — same effect as the old marker write.
                //
                // Primitive-parent guard: `set_path_mut_sentinel` walks
                // through any non-Object/Sentinel ancestor by replacing it
                // with a fresh Sentinel container. If a concurrent SET has
                // turned an ancestor into a primitive between this path's
                // tree-state check and now, walking through it (whether
                // from `promote_path_unchecked` or a bare marker write)
                // would silently destroy the primitive's value. Walk up
                // first; if any ancestor is primitive, skip the
                // promotion entirely. The next read re-evaluates and
                // arrives at the right answer via the WAL/blob.
                //
                // Regression tests:
                //   - test_blob_update_create_then_update_player_permissions
                //   - test_promote_path_shallow_pathnotfound_preserves_primitive_parent
                let path_obj = Path::parse(path);
                let safe_to_write = match path_obj.parent() {
                    None => false, // root — would replace the whole tree
                    Some(start) => {
                        let tree = self.tree.read().unwrap();
                        let mut current = Some(start);
                        let mut clobber_risk = false;
                        while let Some(p) = current {
                            let node = if p.is_root() {
                                Some(tree.root())
                            } else {
                                tree.get(&p)
                            };
                            if let Some(n) = node {
                                if !(n.is_object() || n.is_sentinel()) {
                                    clobber_risk = true;
                                }
                                break;
                            }
                            current = p.parent();
                        }
                        !clobber_risk
                    }
                };

                if !safe_to_write {
                    // Same as the pre-delegation behavior: skip, let the
                    // next read re-evaluate. The primitive parent edge
                    // case is rare enough that the rules-eval retry loop
                    // can spend a slot here without exhausting.
                    self.promoted_paths
                        .insert(normalize_path_key(path), Instant::now());
                    self.sentinel_paths.remove(path);
                    self.promotion_stats.record(
                        promote_start.elapsed(),
                        read_start.elapsed(),
                        session.io().take_read_stats(),
                    );
                    return Ok(true);
                }

                self.promotion_stats.record(
                    promote_start.elapsed(),
                    read_start.elapsed(),
                    session.io().take_read_stats(),
                );
                return self.promote_path_unchecked(path).await;
            }
            Err(e) => return Err(format!("Blob shallow read failed at {}: {}", path, e)),
        };
        let read_elapsed = read_start.elapsed();
        let io_stats = session.io().take_read_stats();

        // Step 2: Convert shallow result to an ArcValue
        let blob_value = match shallow_result {
            ShallowValue::Primitive(value) => value,
            ShallowValue::Children(children) => {
                let mut map = std::collections::HashMap::new();
                for child in children {
                    match child.value {
                        Some(prim) => {
                            // Primitive child — insert real value
                            map.insert(child.key, prim);
                        }
                        None => {
                            // Container child — insert empty Sentinel
                            map.insert(child.key, ArcValue::empty_sentinel());
                        }
                    }
                }
                ArcValue::Object(Arc::new(map))
            }
        };

        // Step 3: Build temp tree with blob data and replay WAL entries.
        //
        // Use the *lazy* set/update variants here. The shallow blob read seeded
        // `temp_tree` with empty Sentinel children for each container (the
        // "needs promotion" signal). The non-lazy `tree.set` / `tree.update`
        // walk through Sentinels via `set_path_mut`, which inserts plain
        // `empty_object` for any missing intermediate — so a deep WAL write
        // like `accounts/{a}/characters/{c}/last_played_ms` would tunnel
        // through the Sentinel-rooted children and leave a chain of real
        // Objects holding only the keys the WAL touched. The subtree then
        // reports as "fully loaded" to subsequent reads, which return the
        // partial WAL data instead of triggering a fresh blob read.
        //
        // `set_lazy` / `update_lazy` use `set_path_mut_sentinel`, which keeps
        // missing intermediates as `empty_sentinel` so the tree continues to
        // flag every path on the chain as needing promotion. The leaves still
        // get their WAL values, but the Sentinel signal survives.
        //
        // Regression test: tests/integration_blob.rs
        // `test_blob_root_multipath_update_replay_preserves_sentinel_intermediates`.
        let path_obj = Path::parse(path);
        let mut temp_tree = Tree::new();
        temp_tree.set_arc_uncleaned(&path_obj, blob_value);

        let matching_indices = self.wal_index.find_affecting(path);
        for &idx in &matching_indices {
            let entry = &self.pending_wal_entries[idx];
            let entry_path = Path::parse(&entry.path);
            match entry.op {
                WalOp::Set => {
                    // See note in `promote_path_unchecked`: SET with None
                    // is the historical SET-to-null form. `set_lazy` cleans
                    // Null to a delete via `from_value_cleaned`.
                    let value = entry.value.clone().unwrap_or(Value::Null);
                    temp_tree.set_lazy(&entry_path, value);
                }
                WalOp::Update => {
                    if let Some(Value::Object(ref updates)) = entry.value {
                        temp_tree.update_lazy(&entry_path, updates);
                    }
                }
                WalOp::Delete => {
                    temp_tree.remove(&entry_path);
                }
            }
        }

        // Step 4: Extract the promoted value and set it in the real tree
        let promoted_value = temp_tree.get_arc(&path_obj).unwrap_or(ArcValue::Null);

        {
            let mut tree = self.tree.write().unwrap();
            tree.set_arc_uncleaned_lazy(&path_obj, promoted_value.clone());
        }

        // Track Sentinel ancestors above `path` (promotion only replaces the
        // subtree AT path; ancestors keep whatever Sentinel state they had).
        self.track_sentinels_after_write(path);

        // Track this promotion for eviction timing
        self.promoted_paths
            .insert(normalize_path_key(path), Instant::now());

        // `promoted_value` replaces the entire subtree at `path`. WAL replay
        // can create Sentinel intermediates *deeper* than immediate children:
        // e.g. an `update_lazy` at root with key `characters/<cid>/core` walks
        // through the `characters` Sentinel and creates a `<cid>` Sentinel
        // intermediate inside it. Walking only immediate children would miss
        // `<cid>` and violate the I3 invariant (`sentinel_paths` must be a
        // superset of every Sentinel actually in the tree). Clear the old
        // subtree's entries and walk the full new value.
        self.remove_sentinel_paths_below(path);
        let mut prefix = if path == "/" {
            String::new()
        } else {
            path.to_string()
        };
        Self::collect_sentinel_paths(&promoted_value, &mut prefix, &mut self.sentinel_paths);

        // Record promotion stats
        let total_elapsed = promote_start.elapsed();
        self.promotion_stats
            .record(total_elapsed, read_elapsed, io_stats);

        Ok(true)
    }

    /// Legacy load_from_blob — delegates to promote_path.
    pub(super) async fn load_from_blob(&mut self, path: &str) -> Result<bool, String> {
        self.promote_path(path).await
    }
}
