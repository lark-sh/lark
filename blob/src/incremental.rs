//! Incremental compaction: apply updates to an existing blob via tombstone + append.
//!
//! Supports:
//! - Updating existing values (in-place or tombstone + append)
//! - Deleting values (rewrite parent object without the child)
//! - Inserting new keys into existing objects (rewrite parent with new child)

use crate::arc_value::ArcValue;
use crate::error::Result;
use crate::io::BlobIO;
use std::collections::HashMap;

/// A tree of pending updates built from a flat (path, value) list.
/// Later entries override earlier ones at the same path.
pub enum UpdateNode {
    /// Replace this path with the given value.
    Set(ArcValue),
    /// Delete this path.
    Delete,
    /// Merge child updates into the existing container on disk.
    Merge(HashMap<String, UpdateNode>),
}

impl UpdateNode {
    /// Build an UpdateTree from a flat list of (path, Option<ArcValue>) updates.
    ///
    /// Rules (processed in order, later entries override):
    /// - `(["a", "b"], Some(val))` → Merge at "a", Set at "b"
    /// - `(["a"], None)` → Delete at "a" (replaces existing Set/Merge)
    /// - `(["a"], Some(val))` → Set at "a" (replaces any existing Merge subtree)
    /// - If a Set already exists at a prefix and a deeper path is added,
    ///   the Set's ArcValue is modified via `set_path_mut`.
    pub fn build(updates: &[(Vec<String>, Option<ArcValue>)]) -> HashMap<String, UpdateNode> {
        let mut root: HashMap<String, UpdateNode> = HashMap::new();

        for (path, value) in updates {
            if path.is_empty() {
                continue;
            }
            Self::insert_into_tree(&mut root, path, value.clone());
        }

        root
    }

    fn insert_into_tree(
        tree: &mut HashMap<String, UpdateNode>,
        path: &[String],
        value: Option<ArcValue>,
    ) {
        debug_assert!(!path.is_empty());

        if path.len() == 1 {
            // Leaf: set or delete
            let key = &path[0];
            match value {
                Some(v) => {
                    tree.insert(key.clone(), UpdateNode::Set(v));
                }
                None => {
                    tree.insert(key.clone(), UpdateNode::Delete);
                }
            }
            return;
        }

        // path.len() >= 2: need to descend into path[0]
        let key = &path[0];
        let rest = &path[1..];

        match tree.get_mut(key) {
            Some(UpdateNode::Set(existing_val)) => {
                // A Set already exists at this prefix. Modify the ArcValue directly
                // for the remaining segments.
                //
                // Critical: deletes (None) and SET-null (Some(Null)) are
                // "remove this path" in our semantics. If the path
                // doesn't exist within `existing_val` (e.g. existing_val is
                // a primitive and rest doesn't navigate into it),
                // `remove_path_mut`'s primitive arm is a no-op — preserving
                // the primitive. Using `set_path_mut(_, Null)` here would
                // instead clobber the primitive into `Object{leaf: Null}`,
                // losing the primitive value (regression test:
                // test_update_node_build_set_primitive_then_delete_descendant_is_noop).
                let remaining_refs: Vec<&str> = rest.iter().map(|s| s.as_str()).collect();
                let is_delete = match &value {
                    None => true,
                    Some(v) => v.is_null(),
                };
                if is_delete {
                    existing_val.remove_path_mut(&remaining_refs);
                } else {
                    existing_val.set_path_mut(&remaining_refs, value.unwrap());
                }
            }
            Some(UpdateNode::Merge(children)) => {
                Self::insert_into_tree(children, rest, value);
            }
            Some(UpdateNode::Delete) | None => {
                // Create a new Merge node and descend
                let mut children = HashMap::new();
                Self::insert_into_tree(&mut children, rest, value);
                tree.insert(key.clone(), UpdateNode::Merge(children));
            }
        }
    }
}

/// Stats returned after applying incremental updates.
#[derive(Debug, Default)]
pub struct IncrementalStats {
    pub updates_applied: u32,
    pub in_place_updates: u32,
    pub forward_updates: u32,
    pub parent_rewrites: u32,
    /// Collection child inserts that rewrite the entire collection header+index+keys.
    /// These are expensive (10s of KB) but counted separately from scalar in_place_updates.
    pub collection_inserts: u32,
    pub bytes_appended: u64,
    pub pread_count: u64,
    pub bytes_read: u64,
    /// Number of pread calls served from CachedIO cache.
    pub cache_hits: u64,
    /// Bytes served from CachedIO cache.
    pub cache_hit_bytes: u64,
    /// Number of container header reads that missed the CachedIO cache (disk round-trip).
    pub cache_header_misses: u64,
    /// Set when the dictionary was rebuilt at a new offset (old reserved space
    /// exhausted). Readers should re-read the header to pick up the new dict_offset.
    pub dict_rebuilt: bool,
    /// Bytes written to free list regions instead of EOF.
    pub bytes_reused: u64,
    /// Snapshot of available free regions at end of batch.
    pub free_regions_available: usize,
    /// Cumulative dead space too small to reuse (only full compaction reclaims it).
    pub bytes_wasted: u64,
}

/// Info about a node discovered during navigation, before forward resolution.
#[derive(Clone)]
pub(crate) struct TargetInfo {
    /// Offset of the node (after resolving forwards).
    pub(crate) resolved_offset: u64,
    /// Offset before resolving forwards (same as resolved if not forwarded).
    pub(crate) original_offset: u64,
    /// Absolute position of this node's entry in its parent's index.
    /// None for the root node (which has no parent).
    pub(crate) parent_index_entry_pos: Option<u64>,
    /// Parent container type (TYPE_COLLECTION or TYPE_ARRAY).
    pub(crate) parent_tag: Option<u8>,
    /// Subtree size from the parent's index entry. Used as a hint
    /// for read_container to enable smarter pre-caching.
    /// 0 for the root node (unknown).
    pub(crate) subtree_size: u64,
    /// Whether this node is forwarded from its parent (is_forwarded flag set
    /// in the parent's index entry). Inline (non-forwarded) nodes live within
    /// their parent's contiguous subtree range — their space is owned by the
    /// parent and must not be freed individually.
    pub(crate) is_forwarded: bool,
}

/// Apply a batch of updates to an existing blob.
///
/// Convenience wrapper that opens a BlobSession, applies updates, and returns stats.
/// Used by benchmarks and tests that don't need the full session lifecycle.
///
/// Each update is (path, Option<ArcValue>):
/// - Some(value): set the path to the new value
/// - None: delete the path
pub async fn apply_updates<IO: BlobIO>(
    io: &IO,
    updates: &[(Vec<String>, Option<ArcValue>)],
) -> Result<IncrementalStats> {
    let owned_io = io.clone_for_reading().await?;
    let mut session = crate::session::BlobSession::open(owned_io).await?;
    let result = session.apply_updates(updates).await?;
    Ok(match result {
        crate::session::ApplyResult::Applied(stats) => stats,
    })
}

// Standalone function implementations removed — all incremental operations
// are now methods on BlobSession in session_incremental.rs.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cached_io::CachedIO;
    use crate::io::{MemBlobIO, read_exact};
    use crate::session::BlobSession;
    use crate::session_reader::{navigate_raw, read_dictionary, read_header};
    use crate::writer::write_blob;
    use serde_json::json;

    fn block_on<F: std::future::Future>(f: F) -> F::Output {
        futures::executor::block_on(f)
    }

    async fn setup_blob(value: serde_json::Value) -> CachedIO<MemBlobIO> {
        let tree = ArcValue::from_value(value);
        let io = CachedIO::new(MemBlobIO::new());
        write_blob(&io, &tree).await.unwrap();
        io
    }

    async fn read_value_at_path<IO: BlobIO>(io: &IO, path: &[&str]) -> ArcValue {
        let session = BlobSession::open(io.clone_for_reading().await.unwrap())
            .await
            .unwrap();
        session.read_subtree(path).await.unwrap()
    }

    async fn read_root<IO: BlobIO>(io: &IO) -> ArcValue {
        let session = BlobSession::open(io.clone_for_reading().await.unwrap())
            .await
            .unwrap();
        session.read_subtree(&[]).await.unwrap()
    }

    #[test]
    fn test_update_scalar_in_place() {
        block_on(async {
            // Number is 9 bytes. Updating to another number fits in place.
            let io = setup_blob(json!({"hp": 100, "name": "Hero"})).await;

            let updates = vec![(vec!["hp".to_string()], Some(ArcValue::from(200i64)))];
            let stats = apply_updates(&io, &updates).await.unwrap();

            assert_eq!(stats.updates_applied, 1);
            assert_eq!(stats.in_place_updates, 1);
            assert_eq!(stats.forward_updates, 0);

            let hp = read_value_at_path(&io, &["hp"]).await;
            assert_eq!(hp.as_i64(), Some(200));

            // Other fields unchanged
            let name = read_value_at_path(&io, &["name"]).await;
            assert_eq!(name.as_str(), Some("Hero"));
        });
    }

    #[test]
    fn test_update_scalar_tombstone_append() {
        block_on(async {
            // Short string -> long string requires tombstone + append
            let io = setup_blob(json!({"msg": "hi"})).await;

            let updates = vec![(
                vec!["msg".to_string()],
                Some(ArcValue::from(
                    "this is a much longer message that won't fit in place",
                )),
            )];
            let stats = apply_updates(&io, &updates).await.unwrap();

            assert_eq!(stats.updates_applied, 1);
            assert_eq!(stats.forward_updates, 1);

            let msg = read_value_at_path(&io, &["msg"]).await;
            assert_eq!(
                msg.as_str(),
                Some("this is a much longer message that won't fit in place")
            );
        });
    }

    #[test]
    fn test_update_same_node_twice_no_chains() {
        block_on(async {
            // Update the same node twice: should update the existing forward, not chain
            let io = setup_blob(json!({"msg": "hi"})).await;

            // First update: creates a forward
            let updates1 = vec![(
                vec!["msg".to_string()],
                Some(ArcValue::from("a longer message here")),
            )];
            apply_updates(&io, &updates1).await.unwrap();

            let size_after_first = io.size().await.unwrap();

            // Second update: should update existing forward pointer, not create chain
            let updates2 = vec![(
                vec!["msg".to_string()],
                Some(ArcValue::from("an even longer message now!!")),
            )];
            let stats2 = apply_updates(&io, &updates2).await.unwrap();
            assert_eq!(stats2.forward_updates, 1);

            let msg = read_value_at_path(&io, &["msg"]).await;
            assert_eq!(msg.as_str(), Some("an even longer message now!!"));

            // Verify no forwarding chain: the blob grew by exactly the new message size,
            // not by a new forward pointer
            let size_after_second = io.size().await.unwrap();
            assert!(size_after_second > size_after_first);
        });
    }

    #[test]
    fn test_update_multiple_nodes() {
        block_on(async {
            let io = setup_blob(json!({
                "characters": {
                    "abc": {"hp": 100, "name": "Hero"},
                    "def": {"hp": 50, "name": "Villain"}
                }
            }))
            .await;

            let updates = vec![
                (
                    vec![
                        "characters".to_string(),
                        "abc".to_string(),
                        "hp".to_string(),
                    ],
                    Some(ArcValue::from(200i64)),
                ),
                (
                    vec![
                        "characters".to_string(),
                        "def".to_string(),
                        "hp".to_string(),
                    ],
                    Some(ArcValue::from(75i64)),
                ),
            ];
            let stats = apply_updates(&io, &updates).await.unwrap();
            assert_eq!(stats.updates_applied, 2);

            let abc_hp = read_value_at_path(&io, &["characters", "abc", "hp"]).await;
            assert_eq!(abc_hp.as_i64(), Some(200));

            let def_hp = read_value_at_path(&io, &["characters", "def", "hp"]).await;
            assert_eq!(def_hp.as_i64(), Some(75));

            // Names unchanged
            let abc_name = read_value_at_path(&io, &["characters", "abc", "name"]).await;
            assert_eq!(abc_name.as_str(), Some("Hero"));
        });
    }

    #[test]
    fn test_delete_node() {
        block_on(async {
            let io = setup_blob(json!({
                "a": 1,
                "b": 2,
                "c": 3
            }))
            .await;

            // Delete "b"
            let updates = vec![(vec!["b".to_string()], None)];
            let stats = apply_updates(&io, &updates).await.unwrap();
            assert_eq!(stats.updates_applied, 1);

            // Read root — "b" should be gone
            let root = read_root(&io).await;
            assert!(root.get("b").is_none());
            assert_eq!(root.get("a").unwrap().as_i64(), Some(1));
            assert_eq!(root.get("c").unwrap().as_i64(), Some(3));
        });
    }

    #[test]
    fn test_insert_new_key() {
        block_on(async {
            // Start with a blob that has "c" in the dictionary (so we can re-insert it)
            let io = setup_blob(json!({
                "a": 1,
                "b": 2,
                "c": {"nested": true}
            }))
            .await;

            // Delete "c" first, then re-insert it
            let delete_updates = vec![(vec!["c".to_string()], None)];
            apply_updates(&io, &delete_updates).await.unwrap();

            let root = read_root(&io).await;
            assert!(root.get("c").is_none());

            // Now insert "c" back with a different value
            let insert_updates = vec![(vec!["c".to_string()], Some(ArcValue::from(42i64)))];
            let stats = apply_updates(&io, &insert_updates).await.unwrap();
            assert_eq!(stats.updates_applied, 1);

            let root = read_root(&io).await;
            assert_eq!(root.get("a").unwrap().as_i64(), Some(1));
            assert_eq!(root.get("b").unwrap().as_i64(), Some(2));
            assert_eq!(root.get("c").unwrap().as_i64(), Some(42));
        });
    }

    #[test]
    fn test_update_bool_small_node() {
        block_on(async {
            // Bool is 2 bytes — too small for a 9-byte Forward.
            // Should update parent's rel_offset instead.
            let io = setup_blob(json!({"flag": true, "name": "test"})).await;

            let updates = vec![(
                vec!["flag".to_string()],
                Some(ArcValue::from("now a string instead of bool")),
            )];
            let stats = apply_updates(&io, &updates).await.unwrap();
            assert_eq!(stats.updates_applied, 1);

            let flag = read_value_at_path(&io, &["flag"]).await;
            assert_eq!(flag.as_str(), Some("now a string instead of bool"));

            // Other fields still work
            let name = read_value_at_path(&io, &["name"]).await;
            assert_eq!(name.as_str(), Some("test"));
        });
    }

    #[test]
    fn test_update_subtree() {
        block_on(async {
            // Replace an entire subtree
            let io = setup_blob(json!({
                "config": {"mode": "light", "theme": "default"}
            }))
            .await;

            let new_config = ArcValue::from_value(json!({"mode": "dark", "theme": "midnight"}));
            let updates = vec![(vec!["config".to_string()], Some(new_config))];
            let stats = apply_updates(&io, &updates).await.unwrap();
            assert_eq!(stats.updates_applied, 1);

            let config = read_value_at_path(&io, &["config"]).await;
            assert_eq!(config.get("mode").unwrap().as_str(), Some("dark"));
            assert_eq!(config.get("theme").unwrap().as_str(), Some("midnight"));
        });
    }

    #[test]
    fn test_no_op_delete_nonexistent() {
        block_on(async {
            let io = setup_blob(json!({"a": 1})).await;

            let updates = vec![(vec!["nonexistent".to_string()], None)];
            let stats = apply_updates(&io, &updates).await.unwrap();
            assert_eq!(stats.updates_applied, 0);

            // Original data unchanged
            let a = read_value_at_path(&io, &["a"]).await;
            assert_eq!(a.as_i64(), Some(1));
        });
    }

    #[test]
    fn test_create_intermediate_objects() {
        block_on(async {
            // Start with a simple blob, then insert at a path that doesn't exist yet
            let io = setup_blob(json!({"config": {"mode": "dark"}})).await;

            // Insert /chat/-abc123 — "chat" doesn't exist, needs to be created
            let msg = ArcValue::from_value(
                json!({"author": "Alice", "content": "hello", "timestamp": 123}),
            );
            let updates = vec![(vec!["chat".to_string(), "-abc123".to_string()], Some(msg))];
            let stats = apply_updates(&io, &updates).await.unwrap();
            assert_eq!(stats.updates_applied, 1);

            // Verify the intermediate structure was created correctly
            let author = read_value_at_path(&io, &["chat", "-abc123", "author"]).await;
            assert_eq!(author.as_str(), Some("Alice"));

            let content = read_value_at_path(&io, &["chat", "-abc123", "content"]).await;
            assert_eq!(content.as_str(), Some("hello"));

            let ts = read_value_at_path(&io, &["chat", "-abc123", "timestamp"]).await;
            assert_eq!(ts.as_i64(), Some(123));

            // Original data unchanged
            let mode = read_value_at_path(&io, &["config", "mode"]).await;
            assert_eq!(mode.as_str(), Some("dark"));
        });
    }

    #[test]
    fn test_create_deep_intermediate_objects() {
        block_on(async {
            // Insert at a path where multiple levels don't exist
            let io = setup_blob(json!({"existing": true})).await;

            // Insert /a/b/c/d = 42 — none of a, b, c exist
            let updates = vec![(
                vec![
                    "a".to_string(),
                    "b".to_string(),
                    "c".to_string(),
                    "d".to_string(),
                ],
                Some(ArcValue::from(42i64)),
            )];
            let stats = apply_updates(&io, &updates).await.unwrap();
            assert_eq!(stats.updates_applied, 1);

            let d = read_value_at_path(&io, &["a", "b", "c", "d"]).await;
            assert_eq!(d.as_i64(), Some(42));

            // Original data unchanged
            let existing = read_value_at_path(&io, &["existing"]).await;
            assert_eq!(existing.as_bool(), Some(true));
        });
    }

    #[test]
    fn test_new_field_names_in_value_subtree() {
        block_on(async {
            // Insert a value that contains structural field names not in the original dictionary
            let io = setup_blob(json!({"hp": 100})).await;

            // Update hp with an object containing new field names
            let updates = vec![(
                vec!["hp".to_string()],
                Some(ArcValue::from_value(
                    json!({"current": 80, "max": 100, "temp": 10}),
                )),
            )];
            let stats = apply_updates(&io, &updates).await.unwrap();
            assert_eq!(stats.updates_applied, 1);

            // Verify the new structure
            let current = read_value_at_path(&io, &["hp", "current"]).await;
            assert_eq!(current.as_i64(), Some(80));

            let max = read_value_at_path(&io, &["hp", "max"]).await;
            assert_eq!(max.as_i64(), Some(100));
        });
    }

    #[test]
    fn test_chat_message_scenario() {
        block_on(async {
            // Realistic scenario: start with a game blob, add chat messages
            let io = setup_blob(json!({
                "characters": {
                    "-Mabc123": {"hp": 100, "name": "Hero"}
                }
            }))
            .await;

            // First chat message — creates /chat and /-msg001 with new structural fields
            let updates = vec![(
                vec!["chat".to_string(), "-msg001".to_string()],
                Some(ArcValue::from_value(json!({
                    "author": "Alice",
                    "content": "Hello world!",
                    "timestamp": 1700000000
                }))),
            )];
            apply_updates(&io, &updates).await.unwrap();

            // Second chat message
            let updates = vec![(
                vec!["chat".to_string(), "-msg002".to_string()],
                Some(ArcValue::from_value(json!({
                    "author": "Bob",
                    "content": "Hey there!",
                    "timestamp": 1700000001
                }))),
            )];
            apply_updates(&io, &updates).await.unwrap();

            // Verify both messages exist
            let msg1_author = read_value_at_path(&io, &["chat", "-msg001", "author"]).await;
            assert_eq!(msg1_author.as_str(), Some("Alice"));

            let msg2_content = read_value_at_path(&io, &["chat", "-msg002", "content"]).await;
            assert_eq!(msg2_content.as_str(), Some("Hey there!"));

            // Original character data unchanged
            let hero_hp = read_value_at_path(&io, &["characters", "-Mabc123", "hp"]).await;
            assert_eq!(hero_hp.as_i64(), Some(100));

            // Compact and verify everything survives
            let dst = CachedIO::new(crate::io::MemBlobIO::new());
            crate::compact::full_compact(&io, &dst).await.unwrap();

            let msg1 = read_value_at_path(&dst, &["chat", "-msg001", "author"]).await;
            assert_eq!(msg1.as_str(), Some("Alice"));

            let msg2 = read_value_at_path(&dst, &["chat", "-msg002", "content"]).await;
            assert_eq!(msg2.as_str(), Some("Hey there!"));

            let hero = read_value_at_path(&dst, &["characters", "-Mabc123", "name"]).await;
            assert_eq!(hero.as_str(), Some("Hero"));
        });
    }

    #[test]
    fn test_delete_nonexistent_deep_path_is_noop() {
        block_on(async {
            // Deleting a path where intermediates don't exist should be a no-op
            let io = setup_blob(json!({"a": 1})).await;

            let updates = vec![(
                vec!["x".to_string(), "y".to_string(), "z".to_string()],
                None,
            )];
            let stats = apply_updates(&io, &updates).await.unwrap();
            assert_eq!(stats.updates_applied, 0);

            // Original data unchanged
            let a = read_value_at_path(&io, &["a"]).await;
            assert_eq!(a.as_i64(), Some(1));
        });
    }

    /// SET-null at a path whose parent is a primitive must be a no-op
    /// (matches intended semantics: `set(null)` is a delete; deleting
    /// a path that doesn't exist preserves the surrounding data). The bug
    /// this regresses against turned `/items` from primitive `5004` into
    /// `Object{level: null}` because `apply_single_update` only
    /// short-circuited for `None` (true Delete) but let `Some(Null)`
    /// (SET-with-null-value, which is what the WAL→blob path produces for
    /// SET-null operations) fall through into the "build wrapper container"
    /// branch and clobber the primitive.
    #[test]
    fn test_set_null_at_child_of_primitive_is_noop() {
        block_on(async {
            let io = setup_blob(json!({"items": 5004})).await;

            let updates = vec![(
                vec!["items".to_string(), "level".to_string()],
                Some(ArcValue::Null),
            )];
            let stats = apply_updates(&io, &updates).await.unwrap();
            assert_eq!(
                stats.updates_applied, 0,
                "SET-null at unreachable path should be a no-op"
            );

            let items = read_value_at_path(&io, &["items"]).await;
            assert_eq!(
                items.as_i64(),
                Some(5004),
                "primitive at /items must be preserved (got {:?})",
                items
            );
        });
    }

    #[test]
    fn test_large_update_still_readable() {
        block_on(async {
            // Create a blob with a small object, then do a tombstone+append
            // that exceeds the root's original subtree_size. The root accumulates
            // fragmentation but data stays readable via forward pointers.
            let io = setup_blob(json!({"a": "x", "b": "y"})).await;
            let header = read_header(&io).await.unwrap();

            // Read root's subtree_size for reference
            let ss_data = read_exact(&io, header.root_offset + 1, 8).await.unwrap();
            let root_subtree_size = u64::from_le_bytes(ss_data.try_into().unwrap());

            // Replace "a" with something much larger than root_subtree_size / 2
            let big_value = "z".repeat(root_subtree_size as usize);
            let updates = vec![(
                vec!["a".to_string()],
                Some(ArcValue::from(big_value.as_str())),
            )];
            apply_updates(&io, &updates).await.unwrap();

            // Root is still a TYPE_COLLECTION (no automatic compaction).
            let tag = read_exact(&io, header.root_offset, 1).await.unwrap()[0];
            assert_eq!(
                tag,
                crate::format::TYPE_COLLECTION,
                "root should still be TYPE_COLLECTION"
            );

            // Data should still be fully readable via forward pointers on children
            let a = read_value_at_path(&io, &["a"]).await;
            assert_eq!(a.as_str(), Some(big_value.as_str()));
            let b = read_value_at_path(&io, &["b"]).await;
            assert_eq!(b.as_str(), Some("y"));
        });
    }

    #[test]
    fn test_collection_insert_exhausts_reserved_uses_structural_copy() {
        block_on(async {
            // Create a collection with very few reserved slots, then insert enough
            // children to exhaust them. The fallback should use structural_copy_collection_with_insert.
            let io = setup_blob(json!({
                "items": {
                    "-Maaa001": {"v": 1},
                    "-Maaa002": {"v": 2}
                }
            }))
            .await;

            // Insert many children to exhaust the reserved slots (default: max(20, n/4) = 20)
            for i in 0..25 {
                let key = format!("-Mnew{:03}", i);
                let updates = vec![(
                    vec!["items".to_string(), key],
                    Some(ArcValue::from_value(json!({"v": i}))),
                )];
                apply_updates(&io, &updates).await.unwrap();
            }

            // All 27 children should be readable
            let v1 = read_value_at_path(&io, &["items", "-Maaa001", "v"]).await;
            assert_eq!(v1.as_i64(), Some(1));
            let v2 = read_value_at_path(&io, &["items", "-Maaa002", "v"]).await;
            assert_eq!(v2.as_i64(), Some(2));
            let v_new = read_value_at_path(&io, &["items", "-Mnew012", "v"]).await;
            assert_eq!(v_new.as_i64(), Some(12));

            // Full compact should produce a clean blob
            let dst = CachedIO::new(crate::io::MemBlobIO::new());
            crate::compact::full_compact(&io, &dst).await.unwrap();

            let v1 = read_value_at_path(&dst, &["items", "-Maaa001", "v"]).await;
            assert_eq!(v1.as_i64(), Some(1));
            let v_new = read_value_at_path(&dst, &["items", "-Mnew024", "v"]).await;
            assert_eq!(v_new.as_i64(), Some(24));
        });
    }

    #[test]
    fn test_collection_key_into_empty_object() {
        block_on(async {
            // Regression test: inserting a collection key directly into an empty
            // root (fresh database).
            let io = setup_blob(json!({})).await;

            let updates = vec![(
                vec!["-Ok_5YVPnZyEWB4EsBwk".to_string()],
                Some(ArcValue::from_value(json!({"hp": 100, "name": "Hero"}))),
            )];
            apply_updates(&io, &updates).await.unwrap();

            // Root should now be a TYPE_COLLECTION with the one child
            let hp = read_value_at_path(&io, &["-Ok_5YVPnZyEWB4EsBwk", "hp"]).await;
            assert_eq!(hp.as_i64(), Some(100));

            let name = read_value_at_path(&io, &["-Ok_5YVPnZyEWB4EsBwk", "name"]).await;
            assert_eq!(name.as_str(), Some("Hero"));
        });
    }

    #[test]
    fn test_collection_key_into_nonempty_object() {
        block_on(async {
            // Root with existing structural keys, then a collection key is inserted.
            let io = setup_blob(json!({"config": {"mode": "dark"}})).await;

            let updates = vec![(
                vec!["-Mabc123".to_string()],
                Some(ArcValue::from_value(json!({"hp": 50}))),
            )];
            apply_updates(&io, &updates).await.unwrap();

            // Both the original structural key and the new collection key should be readable
            let mode = read_value_at_path(&io, &["config", "mode"]).await;
            assert_eq!(mode.as_str(), Some("dark"));

            let hp = read_value_at_path(&io, &["-Mabc123", "hp"]).await;
            assert_eq!(hp.as_i64(), Some(50));
        });
    }

    #[test]
    fn test_collection_child_delete_basic() {
        block_on(async {
            // Delete a child from a TYPE_COLLECTION. Should overwrite the child's
            // tag byte with TYPE_NULL — no structural copy, no cascade.
            let io = setup_blob(json!({
                "characters": {
                    "-Mabc123": {"hp": 100, "name": "Hero"},
                    "-Mdef456": {"hp": 50, "name": "Sidekick"}
                }
            }))
            .await;

            let updates = vec![(vec!["characters".to_string(), "-Mabc123".to_string()], None)];
            let stats = apply_updates(&io, &updates).await.unwrap();
            assert_eq!(stats.updates_applied, 1);
            assert_eq!(stats.in_place_updates, 1); // TYPE_NULL overwrite is in-place
            assert_eq!(stats.parent_rewrites, 0); // no structural copy

            // Deleted child is not navigable (tombstone)
            {
                let s = BlobSession::open(io.clone_for_reading().await.unwrap())
                    .await
                    .unwrap();
                assert!(s.read_subtree(&["characters", "-Mabc123"]).await.is_err());
            }

            // Sibling is untouched
            let hp = read_value_at_path(&io, &["characters", "-Mdef456", "hp"]).await;
            assert_eq!(hp.as_i64(), Some(50));

            // Deleted child is excluded from read_subtree
            let root = read_root(&io).await;
            let chars = root.get("characters").unwrap();
            assert!(chars.get("-Mabc123").is_none());
            assert!(chars.get("-Mdef456").is_some());
        });
    }

    #[test]
    fn test_collection_child_delete_then_reinsert() {
        block_on(async {
            // Delete a collection child, then re-insert at the same key.
            // The re-insert should work because the index entry is still there
            // and the existing update path handles TYPE_NULL as a 1-byte node.
            let io = setup_blob(json!({
                "items": {
                    "-Maaa001": {"v": 1},
                    "-Maaa002": {"v": 2},
                    "-Maaa003": {"v": 3}
                }
            }))
            .await;

            // Delete
            let updates = vec![(vec!["items".to_string(), "-Maaa002".to_string()], None)];
            apply_updates(&io, &updates).await.unwrap();
            // Deleted child is not navigable
            {
                let s = BlobSession::open(io.clone_for_reading().await.unwrap())
                    .await
                    .unwrap();
                assert!(s.read_subtree(&["items", "-Maaa002"]).await.is_err());
            }

            // Re-insert at the same key
            let updates = vec![(
                vec!["items".to_string(), "-Maaa002".to_string()],
                Some(ArcValue::from_value(json!({"v": 99, "new_field": true}))),
            )];
            apply_updates(&io, &updates).await.unwrap();

            let v = read_value_at_path(&io, &["items", "-Maaa002", "v"]).await;
            assert_eq!(v.as_i64(), Some(99));
            let nf = read_value_at_path(&io, &["items", "-Maaa002", "new_field"]).await;
            assert_eq!(nf.as_bool(), Some(true));

            // Other children intact
            let v1 = read_value_at_path(&io, &["items", "-Maaa001", "v"]).await;
            assert_eq!(v1.as_i64(), Some(1));
            let v3 = read_value_at_path(&io, &["items", "-Maaa003", "v"]).await;
            assert_eq!(v3.as_i64(), Some(3));
        });
    }

    #[test]
    fn test_collection_child_delete_multiple() {
        block_on(async {
            // Delete several children from a collection. All should become TYPE_NULL.
            let io = setup_blob(json!({
                "chat": {
                    "-Mmsg001": {"text": "hello"},
                    "-Mmsg002": {"text": "world"},
                    "-Mmsg003": {"text": "foo"},
                    "-Mmsg004": {"text": "bar"}
                }
            }))
            .await;

            let updates = vec![
                (vec!["chat".to_string(), "-Mmsg001".to_string()], None),
                (vec!["chat".to_string(), "-Mmsg003".to_string()], None),
            ];
            let stats = apply_updates(&io, &updates).await.unwrap();
            assert_eq!(stats.updates_applied, 2);
            assert_eq!(stats.in_place_updates, 2);

            // Deleted children are not navigable (tombstones)
            {
                let s = BlobSession::open(io.clone_for_reading().await.unwrap())
                    .await
                    .unwrap();
                assert!(s.read_subtree(&["chat", "-Mmsg001"]).await.is_err());
                assert!(s.read_subtree(&["chat", "-Mmsg003"]).await.is_err());
            }

            // Surviving children intact
            let m2 = read_value_at_path(&io, &["chat", "-Mmsg002", "text"]).await;
            assert_eq!(m2.as_str(), Some("world"));
            let m4 = read_value_at_path(&io, &["chat", "-Mmsg004", "text"]).await;
            assert_eq!(m4.as_str(), Some("bar"));
        });
    }

    #[test]
    fn test_collection_child_delete_idempotent() {
        block_on(async {
            // Deleting a child that's already TYPE_NULL should be a harmless no-op.
            let io = setup_blob(json!({
                "items": {
                    "-Maaa001": {"v": 1},
                    "-Maaa002": {"v": 2}
                }
            }))
            .await;

            // Delete once
            let updates = vec![(vec!["items".to_string(), "-Maaa001".to_string()], None)];
            apply_updates(&io, &updates).await.unwrap();

            // Delete again — should succeed without error
            let updates = vec![(vec!["items".to_string(), "-Maaa001".to_string()], None)];
            let stats = apply_updates(&io, &updates).await.unwrap();
            assert_eq!(stats.updates_applied, 1);

            // Sibling still intact
            let v2 = read_value_at_path(&io, &["items", "-Maaa002", "v"]).await;
            assert_eq!(v2.as_i64(), Some(2));
        });
    }

    #[test]
    fn test_collection_child_delete_forwarded_child() {
        block_on(async {
            // Update a collection child so it gets forwarded (data at EOF,
            // parent index has is_forwarded=true), then delete it.
            let io = setup_blob(json!({
                "items": {
                    "-Maaa001": {"v": "short"},
                    "-Maaa002": {"v": "other"}
                }
            }))
            .await;

            // Force a forward by updating with a much larger value
            let updates = vec![(
                vec!["items".to_string(), "-Maaa001".to_string()],
                Some(ArcValue::from_value(json!({
                    "v": "a much longer string that forces tombstone and append to EOF"
                }))),
            )];
            apply_updates(&io, &updates).await.unwrap();

            // Now delete that forwarded child
            let updates = vec![(vec!["items".to_string(), "-Maaa001".to_string()], None)];
            let stats = apply_updates(&io, &updates).await.unwrap();
            assert_eq!(stats.updates_applied, 1);
            assert_eq!(stats.in_place_updates, 1);

            // Deleted child is not navigable (tombstone)
            {
                let s = BlobSession::open(io.clone_for_reading().await.unwrap())
                    .await
                    .unwrap();
                assert!(s.read_subtree(&["items", "-Maaa001"]).await.is_err());
            }

            // Sibling intact
            let v2 = read_value_at_path(&io, &["items", "-Maaa002", "v"]).await;
            assert_eq!(v2.as_str(), Some("other"));
        });
    }

    #[test]
    fn test_collection_child_delete_all_children() {
        block_on(async {
            // Delete every child from a collection. All entries become TYPE_NULL.
            let io = setup_blob(json!({
                "items": {
                    "-Maaa001": {"v": 1},
                    "-Maaa002": {"v": 2}
                }
            }))
            .await;

            let updates = vec![
                (vec!["items".to_string(), "-Maaa001".to_string()], None),
                (vec!["items".to_string(), "-Maaa002".to_string()], None),
            ];
            apply_updates(&io, &updates).await.unwrap();

            // Deleted children are not navigable (tombstones)
            {
                let s = BlobSession::open(io.clone_for_reading().await.unwrap())
                    .await
                    .unwrap();
                assert!(s.read_subtree(&["items", "-Maaa001"]).await.is_err());
                assert!(s.read_subtree(&["items", "-Maaa002"]).await.is_err());
            }

            // Compact: collection should now have zero children
            let dst = CachedIO::new(crate::io::MemBlobIO::new());
            crate::compact::full_compact(&io, &dst).await.unwrap();

            // After compaction, the items collection should be empty.
            // Reading a deleted key from the compacted blob should fail (key
            // no longer exists in the index).
            let root = read_root(&dst).await;
            let items = root.get("items").unwrap();
            // Empty collection serializes as an object with no children
            match items {
                ArcValue::Object(map) => assert!(map.is_empty()),
                _ => panic!("expected empty object/collection, got {:?}", items),
            }
        });
    }

    #[test]
    fn test_collection_child_delete_compact_reclaims_space() {
        block_on(async {
            // Delete a collection child, then full_compact. The compacted blob
            // should not contain the deleted child (space reclaimed).
            let io = setup_blob(json!({
                "chat": {
                    "-Mmsg001": {"text": "hello", "author": "Alice"},
                    "-Mmsg002": {"text": "world", "author": "Bob"},
                    "-Mmsg003": {"text": "bye", "author": "Charlie"}
                }
            }))
            .await;

            let updates = vec![(vec!["chat".to_string(), "-Mmsg002".to_string()], None)];
            apply_updates(&io, &updates).await.unwrap();

            let pre_compact_size = io.size().await.unwrap();

            let dst = CachedIO::new(crate::io::MemBlobIO::new());
            crate::compact::full_compact(&io, &dst).await.unwrap();

            // Compacted blob should be smaller (deleted child's data reclaimed)
            let post_compact_size = dst.size().await.unwrap();
            assert!(
                post_compact_size < pre_compact_size,
                "compacted blob ({post_compact_size}) should be smaller than pre-compact ({pre_compact_size})"
            );

            // Surviving children present
            let m1 = read_value_at_path(&dst, &["chat", "-Mmsg001", "text"]).await;
            assert_eq!(m1.as_str(), Some("hello"));
            let m3 = read_value_at_path(&dst, &["chat", "-Mmsg003", "text"]).await;
            assert_eq!(m3.as_str(), Some("bye"));

            // Deleted child is gone from the compacted blob (not just null)
            let root = read_root(&dst).await;
            let chat = root.get("chat").unwrap();
            match chat {
                ArcValue::Object(map) => {
                    assert!(
                        map.get("-Mmsg002").is_none(),
                        "deleted child should not exist after compaction"
                    );
                    assert_eq!(map.len(), 2);
                }
                _ => panic!("expected object, got {:?}", chat),
            }
        });
    }

    #[test]
    fn test_collection_child_delete_then_insert_triggers_fallback() {
        block_on(async {
            // Delete some children from a collection, then insert enough new ones
            // to exhaust reserved space. The structural_copy_collection_with_insert
            // fallback should skip the null children.
            let io = setup_blob(json!({
                "items": {
                    "-Maaa001": {"v": 1},
                    "-Maaa002": {"v": 2},
                    "-Maaa003": {"v": 3}
                }
            }))
            .await;

            // Delete one child
            let updates = vec![(vec!["items".to_string(), "-Maaa002".to_string()], None)];
            apply_updates(&io, &updates).await.unwrap();

            // Insert enough new children to exhaust reserved space (default: max(20, n/4) = 20)
            for i in 0..25 {
                let key = format!("-Mnew{:03}", i);
                let updates = vec![(
                    vec!["items".to_string(), key],
                    Some(ArcValue::from_value(json!({"v": i}))),
                )];
                apply_updates(&io, &updates).await.unwrap();
            }

            // Original surviving children
            let v1 = read_value_at_path(&io, &["items", "-Maaa001", "v"]).await;
            assert_eq!(v1.as_i64(), Some(1));
            let v3 = read_value_at_path(&io, &["items", "-Maaa003", "v"]).await;
            assert_eq!(v3.as_i64(), Some(3));

            // New children
            let v_new = read_value_at_path(&io, &["items", "-Mnew012", "v"]).await;
            assert_eq!(v_new.as_i64(), Some(12));

            // Full compact should produce a clean blob without the deleted child
            let dst = CachedIO::new(crate::io::MemBlobIO::new());
            crate::compact::full_compact(&io, &dst).await.unwrap();

            let root = read_root(&dst).await;
            let items = root.get("items").unwrap();
            match items {
                ArcValue::Object(map) => {
                    assert!(
                        map.get("-Maaa002").is_none(),
                        "deleted child should not exist after compaction"
                    );
                    assert!(map.get("-Maaa001").is_some());
                    assert!(map.get("-Maaa003").is_some());
                    assert!(map.get("-Mnew012").is_some());
                }
                _ => panic!("expected object, got {:?}", items),
            }
        });
    }

    #[test]
    fn test_collection_key_intermediate_objects_on_empty_blob() {
        block_on(async {
            // Regression test: multi-segment path where the first segment is a
            // collection key and the root is an empty collection.
            let io = setup_blob(json!({})).await;

            let updates = vec![(
                vec!["-Mabc123".to_string(), "hp".to_string()],
                Some(ArcValue::from(100i64)),
            )];
            apply_updates(&io, &updates).await.unwrap();

            let hp = read_value_at_path(&io, &["-Mabc123", "hp"]).await;
            assert_eq!(hp.as_i64(), Some(100));
        });
    }

    // -----------------------------------------------------------------------
    // Large container tests (subtree_size > 4KB) — exercise compaction
    // with containers that have many forwarded children.
    // -----------------------------------------------------------------------

    /// Build a collection with enough children that its subtree_size > 4KB.
    fn large_collection_json(num_children: usize) -> serde_json::Value {
        let mut items = serde_json::Map::new();
        for i in 0..num_children {
            let key = format!("-Mitem{:04}", i);
            items.insert(
                key,
                json!({
                    "name": format!("Item number {} with a decently long name for padding", i),
                    "description": format!("Description for item {} - this text adds bulk to push the container over 4KB", i),
                    "value": i,
                    "active": true
                }),
            );
        }
        json!({ "items": items })
    }

    #[test]
    fn test_large_collection_defrag_compaction() {
        block_on(async {
            // Create a blob with a collection whose subtree_size > 4KB.
            // Then trigger enough tombstone+appends to exceed the 50% threshold,
            // which should invoke compact_container with CompactOp::Defrag on the
            // large-container in-memory compaction path.
            let io = setup_blob(large_collection_json(15)).await;

            // Verify the items collection is indeed > 4KB
            let header = read_header(&io).await.unwrap();
            let dict = read_dictionary(&io, &header).await.unwrap();
            let items_loc = navigate_raw(&io, &header, &dict, &["items"]).await.unwrap();
            assert!(
                items_loc.subtree_size > 4096,
                "items subtree_size should be > 4KB, got {}",
                items_loc.subtree_size
            );

            // Repeatedly update children with larger values to accumulate appended_bytes.
            // Each update replaces a small object with a bigger one → tombstone+append.
            for i in 0..15 {
                let key = format!("-Mitem{:04}", i);
                let big_desc = format!(
                    "Updated description for item {} that is significantly longer than the original to force tombstone+append updates {}",
                    i,
                    "x".repeat(200)
                );
                let updates = vec![(
                    vec!["items".to_string(), key, "description".to_string()],
                    Some(ArcValue::from(big_desc.as_str())),
                )];
                apply_updates(&io, &updates).await.unwrap();
            }

            // Verify all data is still readable
            for i in 0..15 {
                let key = format!("-Mitem{:04}", i);
                let val = read_value_at_path(&io, &["items", &key, "value"]).await;
                assert_eq!(val.as_i64(), Some(i as i64));
                let name = read_value_at_path(&io, &["items", &key, "name"]).await;
                assert!(
                    name.as_str()
                        .unwrap()
                        .contains(&format!("Item number {}", i))
                );
            }

            // Full compact should produce a clean readable blob
            let dst = CachedIO::new(MemBlobIO::new());
            crate::compact::full_compact(&io, &dst).await.unwrap();
            for i in [0, 7, 14] {
                let key = format!("-Mitem{:04}", i);
                let val = read_value_at_path(&dst, &["items", &key, "value"]).await;
                assert_eq!(val.as_i64(), Some(i as i64));
            }
        });
    }

    #[test]
    fn test_large_collection_insert_fallback() {
        block_on(async {
            // Create a collection > 4KB, then insert enough children to exhaust
            // reserved slots. The fallback uses compact_container with
            // CompactOp::InsertCollection on the large-container path.
            let io = setup_blob(large_collection_json(15)).await;

            let header = read_header(&io).await.unwrap();
            let dict = read_dictionary(&io, &header).await.unwrap();
            let items_loc = navigate_raw(&io, &header, &dict, &["items"]).await.unwrap();
            assert!(items_loc.subtree_size > 4096);

            // Insert enough new children to exhaust reserved slots (default max(20, n/4)).
            // For 15 children, reserved = max(20, 3) = 20. Insert 25 to be sure.
            for i in 0..25 {
                let key = format!("-Mnew_{:04}", i);
                let updates = vec![(
                    vec!["items".to_string(), key],
                    Some(ArcValue::from_value(json!({
                        "name": format!("New item {} with enough text to be realistic", i),
                        "value": 1000 + i
                    }))),
                )];
                apply_updates(&io, &updates).await.unwrap();
            }

            // All 40 children should be readable (15 original + 25 new)
            for i in 0..15 {
                let key = format!("-Mitem{:04}", i);
                let val = read_value_at_path(&io, &["items", &key, "value"]).await;
                assert_eq!(val.as_i64(), Some(i as i64));
            }
            for i in 0..25 {
                let key = format!("-Mnew_{:04}", i);
                let val = read_value_at_path(&io, &["items", &key, "value"]).await;
                assert_eq!(val.as_i64(), Some(1000 + i as i64));
            }

            // Full compact should produce a clean blob
            let dst = CachedIO::new(MemBlobIO::new());
            crate::compact::full_compact(&io, &dst).await.unwrap();
            let val = read_value_at_path(&dst, &["items", "-Mnew_0012", "value"]).await;
            assert_eq!(val.as_i64(), Some(1012));
        });
    }

    #[test]
    fn test_large_object_rewrite_insert_and_remove() {
        block_on(async {
            // Create an object with enough fields that its subtree_size > 4KB.
            // Then insert a new field (triggers compact_object with InsertObject)
            // and remove a field (triggers compact_object with RemoveObject).
            let mut fields = serde_json::Map::new();
            let padding = "x".repeat(150);
            for i in 0..30 {
                fields.insert(
                    format!("field_{:03}", i),
                    json!(format!("value {} {}", i, padding)),
                );
            }
            let io = setup_blob(serde_json::Value::Object(fields)).await;

            // Verify root object is > 4KB
            let header = read_header(&io).await.unwrap();
            let root_data = crate::io::read_exact(&io, header.root_offset + 1, 8)
                .await
                .unwrap();
            let root_subtree_size = u64::from_le_bytes(root_data.try_into().unwrap());
            assert!(
                root_subtree_size > 4096,
                "root subtree_size should be > 4KB, got {}",
                root_subtree_size
            );

            // Insert a new field — triggers rewrite_parent_with_new_child → compact_object
            // with InsertObject on the large path.
            let updates = vec![(
                vec!["new_field".to_string()],
                Some(ArcValue::from("a brand new field value")),
            )];
            apply_updates(&io, &updates).await.unwrap();
            let val = read_value_at_path(&io, &["new_field"]).await;
            assert_eq!(val.as_str(), Some("a brand new field value"));

            // Existing fields still readable
            let val = read_value_at_path(&io, &["field_015"]).await;
            assert!(val.as_str().unwrap().starts_with("value 15"));

            // Delete a field — triggers rewrite_parent_without_child → compact_object
            // with RemoveObject on the large path.
            let updates = vec![(vec!["field_010".to_string()], None)];
            apply_updates(&io, &updates).await.unwrap();

            // Deleted field should be gone (reading it should fail or return from
            // a different path). Other fields still readable.
            let val = read_value_at_path(&io, &["field_020"]).await;
            assert!(val.as_str().unwrap().starts_with("value 20"));
            let val = read_value_at_path(&io, &["new_field"]).await;
            assert_eq!(val.as_str(), Some("a brand new field value"));

            // Full compact produces clean blob
            let dst = CachedIO::new(MemBlobIO::new());
            crate::compact::full_compact(&io, &dst).await.unwrap();
            let val = read_value_at_path(&dst, &["field_029"]).await;
            assert!(val.as_str().unwrap().starts_with("value 29"));
            let val = read_value_at_path(&dst, &["new_field"]).await;
            assert_eq!(val.as_str(), Some("a brand new field value"));
        });
    }

    #[test]
    fn test_subcontainer_compaction_then_read() {
        // Simulates the full production lifecycle with multi-level cascading
        // compaction across multiple WAL batches and file rotations.
        //
        // Uses BlobSession::apply_updates (just like the real compactor) so
        // rotation happens naturally. Keeps applying batches AFTER rotation
        // to stress the post-rotation blob too.
        //
        // Character objects are >4KB so they trigger sub-container compaction.
        // Updates are number→number (same type, same size) to match the real
        // load test pattern where in-place scalar updates accumulate no
        // fragmentation, but tombstone+append updates (larger values) do.
        // We mix both: some in-place, some tombstone+append.
        block_on(async {
            use crate::session::BlobSession;

            let num_chars: usize = 30;
            let mut chars = serde_json::Map::new();
            let padding = "x".repeat(800);
            for i in 0..num_chars {
                let key = format!("-Mchar{:04}", i);
                chars.insert(
                    key,
                    json!({
                        "hp": 100,
                        "mp": 50,
                        "name": format!("Character {} - {}", i, padding),
                        "bio": format!("Biography for character {} - {}", i, padding),
                        "stats": format!("Stats block for character {} - {}", i, padding),
                        "inventory": format!("Inventory for character {} - {}", i, padding),
                        "notes": format!("DM notes for character {} - {}", i, padding),
                        "x": 0.0,
                        "y": 0.0,
                        "layer": "objects",
                    }),
                );
            }
            let initial = json!({
                "characters": chars,
                "config": { "mode": "dark", "grid": true }
            });

            let io = setup_blob(initial).await;
            let mut session = BlobSession::open(io.clone()).await.unwrap();

            // Verify character objects are >4KB (above MIN_COMPACT_BYTES)
            let header = read_header(&io).await.unwrap();
            let dict = read_dictionary(&io, &header).await.unwrap();
            let char0_loc = navigate_raw(&io, &header, &dict, &["characters", "-Mchar0000"])
                .await
                .unwrap();
            eprintln!("single character subtree_size = {}", char0_loc.subtree_size);
            assert!(
                char0_loc.subtree_size > 4096,
                "character subtree_size should be > 4KB, got {}",
                char0_loc.subtree_size
            );

            // Apply many batches — sub-container compaction will trigger
            // as characters accumulate fragmentation. Root fragmentation
            // accumulates too (no automatic root compaction).

            // Track which chars are currently deleted so we don't update them
            let mut deleted_chars: std::collections::HashSet<usize> =
                std::collections::HashSet::new();

            for batch_num in 0..300 {
                let mut updates: Vec<(Vec<String>, Option<ArcValue>)> = Vec::new();

                // Mixed batch: updates + deletes + re-inserts all together,
                // just like a real production WAL batch.
                for j in 0..10 {
                    let char_idx = (batch_num * 10 + j) % num_chars;
                    if deleted_chars.contains(&char_idx) {
                        continue;
                    }
                    let key = format!("-Mchar{:04}", char_idx);

                    // hp: grows with each batch → always tombstone+append
                    let big_hp = format!(
                        "HP updated batch {} char {} - {}",
                        batch_num,
                        char_idx,
                        "z".repeat(200 + batch_num * 50)
                    );
                    updates.push((
                        vec!["characters".to_string(), key.clone(), "hp".to_string()],
                        Some(ArcValue::from(big_hp.as_str())),
                    ));
                    // x: number → number (same size, in-place)
                    updates.push((
                        vec!["characters".to_string(), key, "x".to_string()],
                        Some(ArcValue::from((batch_num as f64) + (j as f64) * 0.1)),
                    ));
                }

                // Every 5th batch: also delete 2 characters and re-insert
                // previously deleted ones, mixed into the same batch
                if batch_num % 5 == 2 {
                    for d in 0..2 {
                        let del_idx = (batch_num * 3 + d * 11) % num_chars;
                        if !deleted_chars.contains(&del_idx) {
                            let del_key = format!("-Mchar{:04}", del_idx);
                            updates.push((vec!["characters".to_string(), del_key], None));
                            deleted_chars.insert(del_idx);
                        }
                    }
                }
                if batch_num % 5 == 3 && !deleted_chars.is_empty() {
                    let reinsert: Vec<usize> = deleted_chars.iter().copied().collect();
                    let padding = "x".repeat(800);
                    for idx in reinsert {
                        let key = format!("-Mchar{:04}", idx);
                        updates.push((
                            vec!["characters".to_string(), key.clone(), "hp".to_string()],
                            Some(ArcValue::from(100i64)),
                        ));
                        updates.push((
                            vec!["characters".to_string(), key.clone(), "name".to_string()],
                            Some(ArcValue::from(
                                format!("Reinserted char {} - {}", idx, padding).as_str(),
                            )),
                        ));
                        updates.push((
                            vec!["characters".to_string(), key.clone(), "bio".to_string()],
                            Some(ArcValue::from(
                                format!("Bio reinserted {} - {}", idx, padding).as_str(),
                            )),
                        ));
                        updates.push((
                            vec!["characters".to_string(), key, "notes".to_string()],
                            Some(ArcValue::from(
                                format!("Notes reinserted {} - {}", idx, padding).as_str(),
                            )),
                        ));
                    }
                    deleted_chars.clear();
                }

                if updates.is_empty() {
                    continue;
                }

                session.apply_updates(&updates).await.unwrap();
            }

            eprintln!("Completed 300 batches");

            // Clear stale cached data from initial setup — io's cache has
            // containers from before the 300 batches of updates.
            io.clear_read_cache().await;

            // === VERIFY FINAL BLOB IS VALID ===

            // 1. Fresh BlobSession navigates to every character
            eprintln!("--- Verifying via fresh BlobSession navigation ---");
            let verify_session = BlobSession::open(io.clone()).await.unwrap();
            for i in 0..num_chars {
                let key = format!("-Mchar{:04}", i);
                let result = verify_session.navigate(&["characters", &key, "name"]).await;
                assert!(
                    result.is_ok(),
                    "navigate to characters/{}/name failed: {:?}",
                    key,
                    result.err()
                );
            }

            // 2. Read leaf values via raw IO (simulating a separate reader process)
            for i in 0..num_chars {
                let key = format!("-Mchar{:04}", i);
                let name = read_value_at_path(&io, &["characters", &key, "name"]).await;
                assert!(
                    name.as_str().is_some(),
                    "characters/{}/name should be a string, got {:?}",
                    key,
                    name
                );
            }

            // 3. Config still readable
            let mode = read_value_at_path(&io, &["config", "mode"]).await;
            assert_eq!(mode.as_str(), Some("dark"));

            // 4. full_compact should produce a valid blob
            eprintln!("--- Verifying via full_compact ---");
            let dst = CachedIO::new(MemBlobIO::new());
            let compact_result = crate::compact::full_compact(&io, &dst).await;
            assert!(
                compact_result.is_ok(),
                "full_compact failed: {:?}",
                compact_result.err()
            );

            for i in [0, 10, 20, num_chars - 1] {
                let key = format!("-Mchar{:04}", i);
                let name = read_value_at_path(&dst, &["characters", &key, "name"]).await;
                let name_str = name.as_str().unwrap();
                assert!(
                    name_str.contains(&format!("Character {}", i))
                        || name_str.contains(&format!("Reinserted char {}", i)),
                    "char {} name wrong in compacted blob",
                    i
                );
            }

            eprintln!("All verifications passed!");
        });
    }

    /// Reproduce the production corruption bug using the real blob + WAL files
    /// from corrupted/. Applies WAL files 1-11 exactly as the compactor would,
    /// one file at a time with coalescing and 1000-entry chunking.
    /// The verify_compacted_container check should catch the corruption.
    #[test]
    #[ignore] // Run with: cargo test test_corrupted_wal_replay -- --ignored --nocapture
    fn test_corrupted_wal_replay() {
        block_on(async {
            let base = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("corrupted");
            let blob_src = base.join("blob.0.lark");
            let wal_dir = base.join("wal");

            if !blob_src.exists() {
                eprintln!("SKIP: corrupted/blob.0.lark not found");
                return;
            }

            // Copy blob so we don't corrupt the original
            let dir = std::env::temp_dir().join("lark_blob_corruption_repro");
            std::fs::create_dir_all(&dir).ok();
            let blob_path = dir.join("blob.0.lark");

            eprintln!("Copying blob to {:?}...", blob_path);
            std::fs::copy(&blob_src, &blob_path).expect("failed to copy blob");
            eprintln!(
                "Blob copied ({:.1} GB)",
                blob_path.metadata().unwrap().len() as f64 / 1_073_741_824.0
            );

            let raw_io = crate::io::StdBlobIO::open(&blob_path).unwrap();
            let io = crate::cached_io::CachedIO::new(raw_io);
            let mut session = crate::session::BlobSession::open(io).await.unwrap();
            eprintln!("Session opened (with CachedIO)");

            // Process WAL files 1 through 22 (or until we hit the bug)
            for wal_seq in 1..=22 {
                let wal_file = wal_dir.join(format!("{:06}.wal", wal_seq));
                if !wal_file.exists() {
                    eprintln!("WAL {:06} not found, stopping", wal_seq);
                    break;
                }

                let content = std::fs::read_to_string(&wal_file).expect("failed to read WAL");
                let entries: Vec<serde_json::Value> = content
                    .lines()
                    .filter(|l| !l.trim().is_empty())
                    .map(|l| serde_json::from_str(l).expect("invalid WAL JSON"))
                    .collect();

                eprintln!("WAL {:06}: {} entries", wal_seq, entries.len());

                // Convert to updates (replicating coalesce_wal_entries from lark-server)
                let updates = coalesce_wal_json(&entries);
                eprintln!("  Coalesced to {} updates", updates.len());

                // Apply the entire WAL file as one batch — UpdateNode::build
                // coalesces duplicates internally for maximum dedup.
                let result = session.apply_updates(&updates).await;

                match result {
                    Ok(crate::session::ApplyResult::Applied(stats)) => {
                        eprintln!(
                            "  {} updates (fwd={} inplace={} coll_ins={} rewrites={} reads={})",
                            stats.updates_applied,
                            stats.forward_updates,
                            stats.in_place_updates,
                            stats.collection_inserts,
                            stats.parent_rewrites,
                            stats.pread_count
                        );
                    }
                    Err(e) => {
                        panic!("apply_updates failed at WAL {}: {:?}", wal_seq, e);
                    }
                }
            }

            // Cleanup
            std::fs::remove_file(&blob_path).ok();
            std::fs::remove_dir_all(&dir).ok();
        });
    }

    fn summarize(v: &ArcValue) -> String {
        match v {
            ArcValue::String(s) => {
                if s.len() > 60 {
                    format!("String(len={}, {:?}...)", s.len(), &s[..30])
                } else {
                    format!("String({:?})", s)
                }
            }
            ArcValue::Number(n) => format!("Number({})", n),
            ArcValue::Object(map) => format!("Object({} keys)", map.len()),
            _ => format!("{:?}", v),
        }
    }

    /// Replay chaos monkey WAL files from ~/lark-blob/wal against a fresh blob.
    /// Tracks all written values and verifies after each batch.
    /// Reproduces: /burst/-item-abcdefg-206/data wrong content (String same len).
    /// Check if two ArcValues are equal, treating -0.0 and 0.0 as identical.
    fn values_equal_ignore_neg_zero(a: &ArcValue, b: &ArcValue) -> bool {
        if a == b {
            return true;
        }
        match (a, b) {
            (ArcValue::Number(na), ArcValue::Number(nb)) => {
                // -0.0 and 0.0: both as_f64() == 0.0 but serde_json treats them as different
                na.as_f64() == Some(0.0) && nb.as_f64() == Some(0.0)
            }
            (ArcValue::Object(oa), ArcValue::Object(ob)) => {
                oa.len() == ob.len()
                    && oa.iter().all(|(k, v)| {
                        ob.get(k)
                            .is_some_and(|v2| values_equal_ignore_neg_zero(v, v2))
                    })
            }
            _ => false,
        }
    }

    #[test]
    #[ignore] // Run with: cargo test test_wal_replay_fresh -- --ignored --nocapture
    fn test_wal_replay_fresh() {
        use crate::cached_io::CachedIO;
        use crate::error::BlobError;
        use crate::io::StdBlobIO;

        block_on(async {
            let wal_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("wal");
            if !wal_dir.exists() {
                eprintln!("SKIP: wal/ directory not found");
                return;
            }

            // Fresh blob on real filesystem (like production)
            let dir = std::env::temp_dir().join("lark-blob-test-wal-replay");
            std::fs::create_dir_all(&dir).ok();
            for entry in std::fs::read_dir(&dir).into_iter().flatten().flatten() {
                std::fs::remove_file(entry.path()).ok();
            }
            let blob_path = dir.join("blob.lark");
            let raw_io = StdBlobIO::create(&blob_path).unwrap();
            let io = CachedIO::new(raw_io);
            let sidecar_io = io.create_related("sidecar").await.unwrap();
            let mut session = crate::session::BlobSession::init(io).await.unwrap();

            // Track expected values: path -> ArcValue
            let mut expected: std::collections::HashMap<Vec<String>, ArcValue> =
                std::collections::HashMap::new();

            // Replay WAL files 1 through 24 (production processed 24)
            for wal_seq in 1..=39 {
                let wal_file = wal_dir.join(format!("{:06}.wal", wal_seq));
                if !wal_file.exists() {
                    eprintln!("WAL {:06} not found, stopping", wal_seq);
                    break;
                }

                let content = std::fs::read_to_string(&wal_file).expect("failed to read WAL");
                let entries: Vec<serde_json::Value> = content
                    .lines()
                    .filter(|l| !l.trim().is_empty())
                    .map(|l| serde_json::from_str(l).expect("invalid WAL JSON"))
                    .collect();

                eprintln!("WAL {:06}: {} entries", wal_seq, entries.len());

                // Track expected values from WAL entries
                for entry in &entries {
                    let op = entry.get("o").and_then(|v| v.as_str()).unwrap_or("");
                    let path = entry.get("p").and_then(|v| v.as_str()).unwrap_or("");
                    let segments = split_wal_path(path);

                    match op {
                        "s" => {
                            if let Some(val) = entry.get("v") {
                                if val.is_null() {
                                    // SET path = null — in our semantics this deletes
                                    // the path and all children under it
                                    let prefix = segments.clone();
                                    expected.retain(|k, _| !k.starts_with(&prefix));
                                } else {
                                    // "set" — track the full value and also leaf fields
                                    let arc_val = ArcValue::from_value(val.clone());
                                    // When setting an object, first remove all existing children
                                    // under this path (the SET replaces the entire subtree)
                                    let prefix = segments.clone();
                                    expected.retain(|k, _| {
                                        // Remove children strictly under this path, but not the path itself
                                        !(k.len() > prefix.len() && k.starts_with(&prefix))
                                    });
                                    // Track leaf fields within the object
                                    if let serde_json::Value::Object(map) = val {
                                        for (key, field_val) in map {
                                            let mut field_path = segments.clone();
                                            field_path.push(key.clone());
                                            expected.insert(
                                                field_path,
                                                ArcValue::from_value(field_val.clone()),
                                            );
                                        }
                                    }
                                    expected.insert(segments, arc_val);
                                }
                            } else {
                                // set with no value = delete
                                // Remove this path and all children
                                let prefix = segments.clone();
                                expected.retain(|k, _| !k.starts_with(&prefix));
                            }
                        }
                        "d" => {
                            let prefix = segments.clone();
                            expected.retain(|k, _| !k.starts_with(&prefix));
                        }
                        "u" => {
                            // "update" — merge fields into existing object
                            if let Some(serde_json::Value::Object(map)) = entry.get("v") {
                                for (key, val) in map {
                                    let mut field_path = segments.clone();
                                    field_path.push(key.clone());
                                    expected.insert(field_path, ArcValue::from_value(val.clone()));
                                }
                                // The parent object is now stale (has new fields)
                                // so remove the parent-level expected
                                expected.remove(&segments);
                            }
                        }
                        _ => {}
                    }
                }

                let updates = coalesce_wal_json(&entries);
                eprintln!("  Coalesced to {} updates", updates.len());

                let result = session
                    .apply_updates_with_sidecar(&updates, Some(&sidecar_io))
                    .await;
                match result {
                    Ok(crate::session::ApplyResult::Applied(stats)) => {
                        eprintln!(
                            "  applied: {} updates, fwd={} inplace={} coll_ins={} rewrites={}",
                            stats.updates_applied,
                            stats.forward_updates,
                            stats.in_place_updates,
                            stats.collection_inserts,
                            stats.parent_rewrites,
                        );
                    }
                    Err(e) => {
                        eprintln!("  ERROR at WAL {:06}: {:?}", wal_seq, e);
                        if let BlobError::NotAContainer(offset, tag) = &e {
                            eprintln!("  NotAContainer: offset={}, tag=0x{:02x}", offset, tag);
                            let raw = session.io.pread(*offset, 64).await;
                            if let Ok(raw) = raw {
                                eprintln!("  raw bytes at offset {}: {:02x?}", offset, raw);
                            }
                            // Find which path leads to this offset by diagnosing
                            // all unique leaf paths in the WAL
                            for entry in &entries {
                                let path_str =
                                    entry.get("p").and_then(|v| v.as_str()).unwrap_or("");
                                let op = entry.get("o").and_then(|v| v.as_str()).unwrap_or("");
                                let segments = split_wal_path(path_str);
                                let refs: Vec<&str> = segments.iter().map(|s| s.as_str()).collect();
                                let diag = session.diagnose_path(&refs).await;
                                // Only print if the problematic offset appears
                                if diag.contains(&format!("offset={}", offset)) {
                                    eprintln!("  FOUND path hitting offset {}:", offset);
                                    eprintln!("  WAL entry: op={} path={}", op, path_str);
                                    eprintln!("{}", diag);
                                }
                            }
                        }
                        panic!("apply_updates failed at WAL {:06}: {:?}", wal_seq, e);
                    }
                }

                // Verify cache consistency (after flush_write_back, write-back is off)
                if !session.io.verify_cache_consistency().await {
                    panic!("CACHE INCONSISTENCY after WAL {:06}", wal_seq);
                }

                // For early WALs: verify via fresh disk session (no cache)
                if wal_seq <= 3 {
                    session.io.sync().await.unwrap();
                    sidecar_io.sync().await.unwrap();
                    let check_raw = StdBlobIO::open(&blob_path).unwrap();
                    let check_sidecar = check_raw.open_related("sidecar").await.unwrap();
                    let check_session = crate::session::BlobSession::open_with_sidecar(
                        check_raw,
                        Some(&check_sidecar),
                    )
                    .await
                    .unwrap_or_else(|e| {
                        panic!(
                            "check session open failed after WAL {:06}: {:?}",
                            wal_seq, e
                        )
                    });
                    let mut disk_violations = 0;
                    for (path, expected_val) in &expected {
                        let path_refs: Vec<&str> = path.iter().map(|s| s.as_str()).collect();
                        match check_session.read_subtree(&path_refs).await {
                            Ok(actual) => {
                                if !values_equal_ignore_neg_zero(&actual, expected_val) {
                                    eprintln!(
                                        "  DISK VIOLATION after WAL {:06}: /{}",
                                        wal_seq,
                                        path.join("/")
                                    );
                                    disk_violations += 1;
                                }
                            }
                            Err(e) => {
                                eprintln!(
                                    "  DISK ERROR after WAL {:06}: /{} — {:?}",
                                    wal_seq,
                                    path.join("/"),
                                    e
                                );
                                disk_violations += 1;
                            }
                        }
                    }
                    if disk_violations > 0 {
                        eprintln!(
                            "  {} DISK violations from fresh session after WAL {:06}",
                            disk_violations, wal_seq
                        );
                    } else {
                        eprintln!(
                            "  0 disk violations from fresh session after WAL {:06}",
                            wal_seq
                        );
                    }
                }

                // Full verification after every WAL — stop on first violation with detail
                let mut violations = 0;
                for (path, expected_val) in &expected {
                    let path_refs: Vec<&str> = path.iter().map(|s| s.as_str()).collect();
                    match session.read_subtree(&path_refs).await {
                        Ok(actual) => {
                            if !values_equal_ignore_neg_zero(&actual, expected_val) {
                                eprintln!(
                                    "  VIOLATION after WAL {:06}: /{} — expected {}, got {}",
                                    wal_seq,
                                    path.join("/"),
                                    summarize(expected_val),
                                    summarize(&actual),
                                );
                                // Detailed byte-level diagnostics for string mismatches
                                if let (ArcValue::String(exp_s), ArcValue::String(act_s)) =
                                    (expected_val, &actual)
                                {
                                    let exp_bytes = exp_s.as_bytes();
                                    let act_bytes = act_s.as_bytes();
                                    eprintln!(
                                        "    expected len={}, actual len={}",
                                        exp_bytes.len(),
                                        act_bytes.len()
                                    );
                                    // Find divergence point
                                    let mut diverge_at = None;
                                    for j in 0..exp_bytes.len().min(act_bytes.len()) {
                                        if exp_bytes[j] != act_bytes[j] {
                                            diverge_at = Some(j);
                                            break;
                                        }
                                    }
                                    if let Some(d) = diverge_at {
                                        let ctx_start = d.saturating_sub(16);
                                        let ctx_end =
                                            (d + 32).min(exp_bytes.len()).min(act_bytes.len());
                                        eprintln!("    DIVERGES at byte offset {}", d);
                                        eprintln!(
                                            "    expected[{}..{}]: {:?}",
                                            ctx_start,
                                            ctx_end,
                                            std::str::from_utf8(&exp_bytes[ctx_start..ctx_end])
                                                .unwrap_or("<non-utf8>")
                                        );
                                        eprintln!(
                                            "    actual  [{}..{}]: {:?}",
                                            ctx_start,
                                            ctx_end,
                                            std::str::from_utf8(&act_bytes[ctx_start..ctx_end])
                                                .unwrap_or("<non-utf8>")
                                        );
                                    }
                                }
                                // Navigate to the exact offset
                                let nav = session.navigate(&path_refs).await;
                                eprintln!("    navigate result: {:?}", nav);
                                // Full path diagnosis
                                let diag = session.diagnose_path(&path_refs).await;
                                eprintln!("{}", diag);
                                violations += 1;
                                if violations >= 3 {
                                    panic!("Stopping after 3 violations at WAL {:06}", wal_seq);
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!(
                                "  VIOLATION after WAL {:06}: /{} — read error: {:?}",
                                wal_seq,
                                path.join("/"),
                                e,
                            );
                            violations += 1;
                            if violations >= 3 {
                                panic!("Stopping after 3 violations at WAL {:06}", wal_seq);
                            }
                        }
                    }
                }
                if violations > 0 {
                    panic!(
                        "{} violations after WAL {:06} ({} paths checked)",
                        violations,
                        wal_seq,
                        expected.len()
                    );
                }
                eprintln!(
                    "  verified {}/{} paths OK after WAL {:06}",
                    expected.len(),
                    expected.len(),
                    wal_seq
                );
            }

            // Final: open a FRESH session from disk and verify
            eprintln!("=== Opening fresh session from disk for final verification ===");
            session.io.sync().await.unwrap();
            sidecar_io.sync().await.unwrap();
            let fresh_raw = StdBlobIO::open(&blob_path).unwrap();
            let fresh_io = CachedIO::new(fresh_raw);
            let fresh_sidecar = fresh_io.open_related("sidecar").await.unwrap();
            let fresh_session =
                crate::session::BlobSession::open_with_sidecar(fresh_io, Some(&fresh_sidecar))
                    .await
                    .unwrap_or_else(|e| panic!("fresh session open failed: {:?}", e));

            let mut violations = 0;
            for (path, expected_val) in &expected {
                let path_refs: Vec<&str> = path.iter().map(|s| s.as_str()).collect();
                match fresh_session.read_subtree(&path_refs).await {
                    Ok(actual) => {
                        if !values_equal_ignore_neg_zero(&actual, expected_val) {
                            eprintln!(
                                "  FRESH VIOLATION: /{} — expected {}, got {}",
                                path.join("/"),
                                summarize(expected_val),
                                summarize(&actual),
                            );
                            let diag = fresh_session.diagnose_path(&path_refs).await;
                            eprintln!("{}", diag);
                            violations += 1;
                        }
                    }
                    Err(e) => {
                        eprintln!(
                            "  FRESH VIOLATION: /{} — read error: {:?}",
                            path.join("/"),
                            e,
                        );
                        violations += 1;
                    }
                }
            }
            if violations > 0 {
                panic!(
                    "{} FRESH violations ({} paths checked)",
                    violations,
                    expected.len()
                );
            }
            eprintln!(
                "=== All {} paths verified OK from fresh session ===",
                expected.len()
            );
        });
    }

    /// Exactly replicate how lark-server's StorageWorker processes WAL files.
    ///
    /// In production:
    /// 1. Database creates blob via BlobSession::init() (empty root object)
    /// 2. StorageWorker opens it via BlobSession::open_with_sidecar()
    /// 3. For each compaction cycle, StorageWorker reads completed WAL files,
    ///    coalesces them (no dedup, UPDATE expanded to per-key SETs),
    ///    and calls apply_updates_with_sidecar() once per cycle.
    ///
    /// The chaos-monkey log shows 1 WAL file per compaction cycle, so we
    /// process each WAL file as a separate apply_updates_with_sidecar() call.
    #[test]
    #[ignore] // Run with: cargo test test_wal_replay_storage_worker -- --ignored --nocapture
    fn test_wal_replay_storage_worker() {
        use crate::cached_io::CachedIO;
        use crate::io::StdBlobIO;

        block_on(async {
            let wal_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("wal");
            if !wal_dir.exists() {
                eprintln!("SKIP: wal/ directory not found");
                return;
            }

            // === Step 1: Database creates the blob (BlobSession::init) ===
            // Matches lark-server: BlobSession::init(CachedIO::new(...)), keep same session.
            let dir = std::env::temp_dir().join("lark-blob-test-wal-replay-worker");
            std::fs::create_dir_all(&dir).ok();
            for entry in std::fs::read_dir(&dir).into_iter().flatten().flatten() {
                std::fs::remove_file(entry.path()).ok();
            }
            let blob_path = dir.join("blob.lark");
            let sidecar_path = dir.join("sidecar.lark");

            let raw_io = StdBlobIO::create(&blob_path).unwrap();
            let io = CachedIO::new(raw_io);
            let mut session = crate::session::BlobSession::init(io).await.unwrap();
            session.io().sync().await.unwrap();

            // Sidecar as a separate raw IO (not via create_related, matching StorageWorker)
            let sidecar_io = StdBlobIO::create(&sidecar_path).unwrap();

            // === Step 3: Process WAL files one at a time (1 per compaction cycle) ===
            for wal_seq in 1..=39 {
                let wal_file = wal_dir.join(format!("{:06}.wal", wal_seq));
                if !wal_file.exists() {
                    eprintln!("WAL {:06} not found, stopping", wal_seq);
                    break;
                }

                let content = std::fs::read_to_string(&wal_file).expect("failed to read WAL");
                let entries: Vec<serde_json::Value> = content
                    .lines()
                    .filter(|l| !l.trim().is_empty())
                    .map(|l| serde_json::from_str(l).expect("invalid WAL JSON"))
                    .collect();

                eprintln!("WAL {:06}: {} entries", wal_seq, entries.len());

                let updates = coalesce_wal_json(&entries);
                eprintln!("  Coalesced to {} updates", updates.len());

                let result = session
                    .apply_updates_with_sidecar(&updates, Some(&sidecar_io))
                    .await;
                match result {
                    Ok(crate::session::ApplyResult::Applied(stats)) => {
                        let blob_size = session.io().size().await.unwrap_or(0);
                        eprintln!(
                            "  applied: {} updates, fwd={} inplace={} coll_ins={} rewrites={}, blob {:.1}KB",
                            stats.updates_applied,
                            stats.forward_updates,
                            stats.in_place_updates,
                            stats.collection_inserts,
                            stats.parent_rewrites,
                            blob_size as f64 / 1024.0,
                        );
                    }
                    Err(e) => {
                        panic!("apply_updates failed at WAL {:06}: {:?}", wal_seq, e);
                    }
                }
            }

            // Report final sizes
            let blob_size = session.io().size().await.unwrap_or(0);
            eprintln!("\n=== Final: blob {:.1}KB ===", blob_size as f64 / 1024.0);

            // List all files in the output directory
            for entry in std::fs::read_dir(&dir).into_iter().flatten().flatten() {
                let meta = entry.metadata().unwrap();
                eprintln!(
                    "  {:?}: {:.1}KB",
                    entry.file_name(),
                    meta.len() as f64 / 1024.0
                );
            }
        });
    }

    /// Replicate lark-server's coalesce_wal_entries: no deduplication.
    /// All entries are passed through in order. UpdateNode::build handles
    /// coalescing internally (later entries override earlier ones).
    fn coalesce_wal_json(entries: &[serde_json::Value]) -> Vec<(Vec<String>, Option<ArcValue>)> {
        let mut result: Vec<(Vec<String>, Option<ArcValue>)> = Vec::new();

        for entry in entries {
            let op = entry.get("o").and_then(|v| v.as_str()).unwrap_or("");
            let path = entry.get("p").and_then(|v| v.as_str()).unwrap_or("");

            match op {
                "s" => {
                    let segments = split_wal_path(path);
                    let value = entry.get("v").map(|v| ArcValue::from_value(v.clone()));
                    result.push((segments, value));
                }
                "d" => {
                    let segments = split_wal_path(path);
                    result.push((segments, None));
                }
                "u" => {
                    if let Some(serde_json::Value::Object(map)) = entry.get("v") {
                        for (key, val) in map {
                            let expanded = format!("{}/{}", path, key);
                            let segments = split_wal_path(&expanded);
                            result.push((segments, Some(ArcValue::from_value(val.clone()))));
                        }
                    }
                }
                _ => {}
            }
        }

        result
    }

    fn split_wal_path(path: &str) -> Vec<String> {
        path.trim_start_matches('/')
            .split('/')
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect()
    }

    // -----------------------------------------------------------------------
    // UpdateNode::build tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_update_node_build_single_set() {
        let updates = vec![(vec!["a".to_string()], Some(ArcValue::from(42i64)))];
        let tree = UpdateNode::build(&updates);
        assert_eq!(tree.len(), 1);
        match tree.get("a").unwrap() {
            UpdateNode::Set(v) => assert_eq!(v.as_i64(), Some(42)),
            _ => panic!("expected Set"),
        }
    }

    #[test]
    fn test_update_node_build_single_delete() {
        let updates = vec![(vec!["a".to_string()], None)];
        let tree = UpdateNode::build(&updates);
        assert_eq!(tree.len(), 1);
        assert!(matches!(tree.get("a").unwrap(), UpdateNode::Delete));
    }

    #[test]
    fn test_update_node_build_nested_path() {
        let updates = vec![(
            vec!["a".to_string(), "b".to_string(), "c".to_string()],
            Some(ArcValue::from(1i64)),
        )];
        let tree = UpdateNode::build(&updates);
        assert_eq!(tree.len(), 1);
        match tree.get("a").unwrap() {
            UpdateNode::Merge(children) => match children.get("b").unwrap() {
                UpdateNode::Merge(grandchildren) => match grandchildren.get("c").unwrap() {
                    UpdateNode::Set(v) => assert_eq!(v.as_i64(), Some(1)),
                    _ => panic!("expected Set at c"),
                },
                _ => panic!("expected Merge at b"),
            },
            _ => panic!("expected Merge at a"),
        }
    }

    #[test]
    fn test_update_node_build_coalesce_later_overrides() {
        // Later write to the same path overrides the earlier one
        let updates = vec![
            (vec!["a".to_string()], Some(ArcValue::from(1i64))),
            (vec!["a".to_string()], Some(ArcValue::from(2i64))),
        ];
        let tree = UpdateNode::build(&updates);
        match tree.get("a").unwrap() {
            UpdateNode::Set(v) => assert_eq!(v.as_i64(), Some(2)),
            _ => panic!("expected Set"),
        }
    }

    #[test]
    fn test_update_node_build_set_then_deeper_set() {
        // SET /a/b then SET /a/b/c → modifies the pending Set's ArcValue
        let updates = vec![
            (
                vec!["a".to_string(), "b".to_string()],
                Some(ArcValue::from_value(json!({"x": 1}))),
            ),
            (
                vec!["a".to_string(), "b".to_string(), "c".to_string()],
                Some(ArcValue::from(99i64)),
            ),
        ];
        let tree = UpdateNode::build(&updates);
        match tree.get("a").unwrap() {
            UpdateNode::Merge(children) => {
                match children.get("b").unwrap() {
                    UpdateNode::Set(v) => {
                        // The ArcValue should have both "x" and "c"
                        assert_eq!(v.get("x").unwrap().as_i64(), Some(1));
                        assert_eq!(v.get("c").unwrap().as_i64(), Some(99));
                    }
                    _ => panic!("expected Set at b"),
                }
            }
            _ => panic!("expected Merge at a"),
        }
    }

    /// SET /a = primitive followed by DELETE /a/b in the same batch must
    /// leave the tree as `Set(primitive)` — the delete of a path that
    /// doesn't exist (because the parent is a primitive) is a no-op.
    /// Previously the coalescing called `set_path_mut(["b"], Null)` on the
    /// primitive, which clobbered it into `Object{b: Null}` (regression
    /// triggered by chaos-monkey: SET /players/names/alice = 1485 followed
    /// by TX-Delete /players/names/alice/scores produced
    /// `Object{scores: Null}` in the blob).
    #[test]
    fn test_update_node_build_set_primitive_then_delete_descendant_is_noop() {
        let updates = vec![
            (vec!["a".to_string()], Some(ArcValue::from(1485i64))),
            (vec!["a".to_string(), "b".to_string()], None),
        ];
        let tree = UpdateNode::build(&updates);
        match tree.get("a").unwrap() {
            UpdateNode::Set(v) => assert_eq!(
                v.as_i64(),
                Some(1485),
                "primitive must be preserved (got {:?})",
                v
            ),
            _ => panic!("expected Set(1485), got something else"),
        }
    }

    /// Same shape as above but the second op is a SET-with-null-value
    /// (Some(ArcValue::Null)) rather than a true delete (None). Both are
    /// "delete this path" in our semantics and must be no-ops when
    /// the parent is a primitive.
    #[test]
    fn test_update_node_build_set_primitive_then_set_null_descendant_is_noop() {
        let updates = vec![
            (vec!["a".to_string()], Some(ArcValue::from(1485i64))),
            (vec!["a".to_string(), "b".to_string()], Some(ArcValue::Null)),
        ];
        let tree = UpdateNode::build(&updates);
        match tree.get("a").unwrap() {
            UpdateNode::Set(v) => assert_eq!(
                v.as_i64(),
                Some(1485),
                "primitive must be preserved (got {:?})",
                v
            ),
            _ => panic!("expected Set(1485), got something else"),
        }
    }

    #[test]
    fn test_update_node_build_deeper_set_then_parent_set() {
        // SET /a/b/c then SET /a/b → Set replaces the Merge
        let updates = vec![
            (
                vec!["a".to_string(), "b".to_string(), "c".to_string()],
                Some(ArcValue::from(1i64)),
            ),
            (
                vec!["a".to_string(), "b".to_string()],
                Some(ArcValue::from(2i64)),
            ),
        ];
        let tree = UpdateNode::build(&updates);
        match tree.get("a").unwrap() {
            UpdateNode::Merge(children) => match children.get("b").unwrap() {
                UpdateNode::Set(v) => assert_eq!(v.as_i64(), Some(2)),
                _ => panic!("expected Set at b (parent set should replace child merge)"),
            },
            _ => panic!("expected Merge at a"),
        }
    }

    #[test]
    fn test_update_node_build_delete_replaces_merge() {
        // Build a merge tree, then delete the parent — Merge is replaced with Delete
        let updates = vec![
            (
                vec!["a".to_string(), "b".to_string()],
                Some(ArcValue::from(1i64)),
            ),
            (
                vec!["a".to_string(), "c".to_string()],
                Some(ArcValue::from(2i64)),
            ),
            (vec!["a".to_string()], None),
        ];
        let tree = UpdateNode::build(&updates);
        assert!(matches!(tree.get("a").unwrap(), UpdateNode::Delete));
    }

    #[test]
    fn test_update_node_build_multiple_siblings() {
        // Multiple siblings under the same parent merge correctly
        let updates = vec![
            (
                vec!["chat".to_string(), "-msg001".to_string()],
                Some(ArcValue::from_value(json!({"text": "hello"}))),
            ),
            (
                vec!["chat".to_string(), "-msg002".to_string()],
                Some(ArcValue::from_value(json!({"text": "world"}))),
            ),
            (
                vec!["chat".to_string(), "-msg003".to_string()],
                Some(ArcValue::from_value(json!({"text": "!"}))),
            ),
        ];
        let tree = UpdateNode::build(&updates);
        match tree.get("chat").unwrap() {
            UpdateNode::Merge(children) => {
                assert_eq!(children.len(), 3);
                assert!(matches!(
                    children.get("-msg001").unwrap(),
                    UpdateNode::Set(_)
                ));
                assert!(matches!(
                    children.get("-msg002").unwrap(),
                    UpdateNode::Set(_)
                ));
                assert!(matches!(
                    children.get("-msg003").unwrap(),
                    UpdateNode::Set(_)
                ));
            }
            _ => panic!("expected Merge at chat"),
        }
    }

    #[test]
    fn test_update_node_build_empty_path_ignored() {
        let updates = vec![
            (vec![], Some(ArcValue::from(1i64))),
            (vec!["a".to_string()], Some(ArcValue::from(2i64))),
        ];
        let tree = UpdateNode::build(&updates);
        assert_eq!(tree.len(), 1);
        assert!(tree.contains_key("a"));
    }

    #[test]
    fn test_update_node_build_delete_then_deeper_creates_merge() {
        // Delete at /a, then set at /a/b — the Delete is replaced by a Merge
        let updates = vec![
            (vec!["a".to_string()], None),
            (
                vec!["a".to_string(), "b".to_string()],
                Some(ArcValue::from(1i64)),
            ),
        ];
        let tree = UpdateNode::build(&updates);
        match tree.get("a").unwrap() {
            UpdateNode::Merge(children) => {
                assert_eq!(children.len(), 1);
                match children.get("b").unwrap() {
                    UpdateNode::Set(v) => assert_eq!(v.as_i64(), Some(1)),
                    _ => panic!("expected Set at b"),
                }
            }
            _ => panic!("expected Merge at a (deeper set should replace delete)"),
        }
    }

    // -----------------------------------------------------------------------
    // Batch insert into collection tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_batch_insert_100_children() {
        block_on(async {
            // Create a collection, then batch-insert 100 children at once
            let io = setup_blob(json!({
                "items": {
                    "-Mseed001": {"v": 0}
                }
            }))
            .await;

            // Build a batch of 100 inserts into the same collection
            let mut updates: Vec<(Vec<String>, Option<ArcValue>)> = Vec::new();
            for i in 0..100 {
                let key = format!("-Mbatch{:04}", i);
                updates.push((
                    vec!["items".to_string(), key],
                    Some(ArcValue::from_value(
                        json!({"v": i, "label": format!("item-{}", i)}),
                    )),
                ));
            }
            let stats = apply_updates(&io, &updates).await.unwrap();
            assert_eq!(stats.updates_applied, 100);

            // Verify all 101 children are readable (1 seed + 100 new)
            let seed = read_value_at_path(&io, &["items", "-Mseed001", "v"]).await;
            assert_eq!(seed.as_i64(), Some(0));

            for i in 0..100 {
                let key = format!("-Mbatch{:04}", i);
                let v = read_value_at_path(&io, &["items", &key, "v"]).await;
                assert_eq!(v.as_i64(), Some(i as i64), "wrong value for {}", key);
                let label = read_value_at_path(&io, &["items", &key, "label"]).await;
                assert_eq!(label.as_str(), Some(format!("item-{}", i).as_str()));
            }

            // Full compact should produce a clean, valid blob
            let dst = CachedIO::new(MemBlobIO::new());
            crate::compact::full_compact(&io, &dst).await.unwrap();
            for i in [0, 25, 50, 75, 99] {
                let key = format!("-Mbatch{:04}", i);
                let v = read_value_at_path(&dst, &["items", &key, "v"]).await;
                assert_eq!(v.as_i64(), Some(i as i64));
            }
        });
    }

    #[test]
    fn test_batch_insert_mixed_new_and_existing() {
        block_on(async {
            // Some keys already exist, some are new — all in one batch
            let io = setup_blob(json!({
                "items": {
                    "-Maaa001": {"v": 1},
                    "-Maaa002": {"v": 2},
                    "-Maaa003": {"v": 3}
                }
            }))
            .await;

            let updates = vec![
                // Update existing key
                (
                    vec!["items".to_string(), "-Maaa001".to_string()],
                    Some(ArcValue::from_value(json!({"v": 100}))),
                ),
                // Insert new keys
                (
                    vec!["items".to_string(), "-Mnew001".to_string()],
                    Some(ArcValue::from_value(json!({"v": 10}))),
                ),
                (
                    vec!["items".to_string(), "-Mnew002".to_string()],
                    Some(ArcValue::from_value(json!({"v": 20}))),
                ),
                // Update another existing key
                (
                    vec!["items".to_string(), "-Maaa003".to_string()],
                    Some(ArcValue::from_value(json!({"v": 300}))),
                ),
            ];
            apply_updates(&io, &updates).await.unwrap();

            // Existing keys updated
            let v1 = read_value_at_path(&io, &["items", "-Maaa001", "v"]).await;
            assert_eq!(v1.as_i64(), Some(100));
            let v3 = read_value_at_path(&io, &["items", "-Maaa003", "v"]).await;
            assert_eq!(v3.as_i64(), Some(300));

            // Untouched existing key
            let v2 = read_value_at_path(&io, &["items", "-Maaa002", "v"]).await;
            assert_eq!(v2.as_i64(), Some(2));

            // New keys inserted
            let n1 = read_value_at_path(&io, &["items", "-Mnew001", "v"]).await;
            assert_eq!(n1.as_i64(), Some(10));
            let n2 = read_value_at_path(&io, &["items", "-Mnew002", "v"]).await;
            assert_eq!(n2.as_i64(), Some(20));
        });
    }

    #[test]
    fn test_batch_insert_with_deletes() {
        block_on(async {
            // Mix of inserts and deletes in the same batch
            let io = setup_blob(json!({
                "chat": {
                    "-Mmsg001": {"text": "hello"},
                    "-Mmsg002": {"text": "world"},
                    "-Mmsg003": {"text": "foo"}
                }
            }))
            .await;

            let updates = vec![
                // Delete an existing message
                (vec!["chat".to_string(), "-Mmsg002".to_string()], None),
                // Insert new messages
                (
                    vec!["chat".to_string(), "-Mmsg004".to_string()],
                    Some(ArcValue::from_value(json!({"text": "new1"}))),
                ),
                (
                    vec!["chat".to_string(), "-Mmsg005".to_string()],
                    Some(ArcValue::from_value(json!({"text": "new2"}))),
                ),
            ];
            apply_updates(&io, &updates).await.unwrap();

            // Deleted message should be PathNotFound (tombstone)
            {
                let s = BlobSession::open(io.clone_for_reading().await.unwrap())
                    .await
                    .unwrap();
                let m2 = s.read_subtree(&["chat", "-Mmsg002"]).await;
                assert!(m2.is_err(), "deleted entry should be PathNotFound");
            }

            // Original messages intact
            let m1 = read_value_at_path(&io, &["chat", "-Mmsg001", "text"]).await;
            assert_eq!(m1.as_str(), Some("hello"));
            let m3 = read_value_at_path(&io, &["chat", "-Mmsg003", "text"]).await;
            assert_eq!(m3.as_str(), Some("foo"));

            // New messages inserted
            let m4 = read_value_at_path(&io, &["chat", "-Mmsg004", "text"]).await;
            assert_eq!(m4.as_str(), Some("new1"));
            let m5 = read_value_at_path(&io, &["chat", "-Mmsg005", "text"]).await;
            assert_eq!(m5.as_str(), Some("new2"));
        });
    }

    #[test]
    fn test_batch_insert_into_empty_collection() {
        block_on(async {
            // First insert a collection key to create the collection,
            // then batch-insert many children into it
            let io = setup_blob(json!({
                "chat": {
                    "-Mseed": {"text": "seed"}
                }
            }))
            .await;

            // Batch of 20 new messages
            let mut updates: Vec<(Vec<String>, Option<ArcValue>)> = Vec::new();
            for i in 0..20 {
                let key = format!("-Mmsg{:04}", i);
                updates.push((
                    vec!["chat".to_string(), key],
                    Some(ArcValue::from_value(json!({"text": format!("msg-{}", i)}))),
                ));
            }
            apply_updates(&io, &updates).await.unwrap();

            // All messages readable
            for i in 0..20 {
                let key = format!("-Mmsg{:04}", i);
                let text = read_value_at_path(&io, &["chat", &key, "text"]).await;
                assert_eq!(text.as_str(), Some(format!("msg-{}", i).as_str()));
            }

            // Seed still there
            let seed = read_value_at_path(&io, &["chat", "-Mseed", "text"]).await;
            assert_eq!(seed.as_str(), Some("seed"));
        });
    }

    #[test]
    fn test_batch_insert_across_multiple_collections() {
        block_on(async {
            // Batch that touches multiple collections in one apply_updates call
            let io = setup_blob(json!({
                "characters": {
                    "-Mchar001": {"hp": 100}
                },
                "chat": {
                    "-Mmsg001": {"text": "hi"}
                }
            }))
            .await;

            let updates = vec![
                (
                    vec!["characters".to_string(), "-Mchar002".to_string()],
                    Some(ArcValue::from_value(json!({"hp": 50}))),
                ),
                (
                    vec!["characters".to_string(), "-Mchar003".to_string()],
                    Some(ArcValue::from_value(json!({"hp": 75}))),
                ),
                (
                    vec!["chat".to_string(), "-Mmsg002".to_string()],
                    Some(ArcValue::from_value(json!({"text": "hello"}))),
                ),
                (
                    vec!["chat".to_string(), "-Mmsg003".to_string()],
                    Some(ArcValue::from_value(json!({"text": "world"}))),
                ),
            ];
            apply_updates(&io, &updates).await.unwrap();

            let c2 = read_value_at_path(&io, &["characters", "-Mchar002", "hp"]).await;
            assert_eq!(c2.as_i64(), Some(50));
            let c3 = read_value_at_path(&io, &["characters", "-Mchar003", "hp"]).await;
            assert_eq!(c3.as_i64(), Some(75));
            let m2 = read_value_at_path(&io, &["chat", "-Mmsg002", "text"]).await;
            assert_eq!(m2.as_str(), Some("hello"));
            let m3 = read_value_at_path(&io, &["chat", "-Mmsg003", "text"]).await;
            assert_eq!(m3.as_str(), Some("world"));

            // Original data intact
            let c1 = read_value_at_path(&io, &["characters", "-Mchar001", "hp"]).await;
            assert_eq!(c1.as_i64(), Some(100));
            let m1 = read_value_at_path(&io, &["chat", "-Mmsg001", "text"]).await;
            assert_eq!(m1.as_str(), Some("hi"));
        });
    }

    #[test]
    fn test_batch_insert_with_deep_updates() {
        block_on(async {
            // Batch that has both collection inserts AND deep path updates
            // into existing children (Merge subtrees, not just Set leaves)
            let io = setup_blob(json!({
                "characters": {
                    "-Mchar001": {"hp": 100, "name": "Hero"},
                    "-Mchar002": {"hp": 50, "name": "Villain"}
                }
            }))
            .await;

            let updates = vec![
                // Deep update to existing child's scalar
                (
                    vec![
                        "characters".to_string(),
                        "-Mchar001".to_string(),
                        "hp".to_string(),
                    ],
                    Some(ArcValue::from(200i64)),
                ),
                // Insert new children
                (
                    vec!["characters".to_string(), "-Mchar003".to_string()],
                    Some(ArcValue::from_value(json!({"hp": 75, "name": "Sidekick"}))),
                ),
                (
                    vec!["characters".to_string(), "-Mchar004".to_string()],
                    Some(ArcValue::from_value(json!({"hp": 25, "name": "NPC"}))),
                ),
                // Deep update to another existing child's scalar
                (
                    vec![
                        "characters".to_string(),
                        "-Mchar002".to_string(),
                        "name".to_string(),
                    ],
                    Some(ArcValue::from("Antihero")),
                ),
            ];
            apply_updates(&io, &updates).await.unwrap();

            // Deep updates applied
            let hp1 = read_value_at_path(&io, &["characters", "-Mchar001", "hp"]).await;
            assert_eq!(hp1.as_i64(), Some(200));
            let name2 = read_value_at_path(&io, &["characters", "-Mchar002", "name"]).await;
            assert_eq!(name2.as_str(), Some("Antihero"));

            // New children inserted
            let hp3 = read_value_at_path(&io, &["characters", "-Mchar003", "hp"]).await;
            assert_eq!(hp3.as_i64(), Some(75));
            let hp4 = read_value_at_path(&io, &["characters", "-Mchar004", "hp"]).await;
            assert_eq!(hp4.as_i64(), Some(25));

            // Untouched fields preserved
            let name1 = read_value_at_path(&io, &["characters", "-Mchar001", "name"]).await;
            assert_eq!(name1.as_str(), Some("Hero"));
        });
    }

    #[test]
    fn test_batch_insert_exhausts_reserved_slots() {
        block_on(async {
            // Insert enough children in one batch to exhaust reserved slots,
            // forcing the fallback to compact_container with InsertCollectionBatch
            let io = setup_blob(json!({
                "items": {
                    "-Mseed001": {"v": 0},
                    "-Mseed002": {"v": 0}
                }
            }))
            .await;

            // Reserved slots = max(20, 2/4) = 20. Insert 30 in one batch.
            let mut updates: Vec<(Vec<String>, Option<ArcValue>)> = Vec::new();
            for i in 0..30 {
                let key = format!("-Mbig{:04}", i);
                updates.push((
                    vec!["items".to_string(), key],
                    Some(ArcValue::from_value(json!({"v": i}))),
                ));
            }
            apply_updates(&io, &updates).await.unwrap();

            // All 32 children readable
            let seed = read_value_at_path(&io, &["items", "-Mseed001", "v"]).await;
            assert_eq!(seed.as_i64(), Some(0));
            for i in 0..30 {
                let key = format!("-Mbig{:04}", i);
                let v = read_value_at_path(&io, &["items", &key, "v"]).await;
                assert_eq!(v.as_i64(), Some(i as i64), "wrong value for {}", key);
            }

            // Full compact produces valid blob
            let dst = CachedIO::new(MemBlobIO::new());
            crate::compact::full_compact(&io, &dst).await.unwrap();
            let v = read_value_at_path(&dst, &["items", "-Mbig0015", "v"]).await;
            assert_eq!(v.as_i64(), Some(15));
        });
    }

    #[test]
    fn test_batch_insert_data_integrity_vs_sequential() {
        block_on(async {
            // Verify batch insert produces the same data as sequential inserts
            let initial = json!({
                "items": {
                    "-Mseed": {"v": 0}
                }
            });

            // Sequential: insert one at a time
            let io_seq = setup_blob(initial.clone()).await;
            for i in 0..20 {
                let key = format!("-Mseq{:04}", i);
                let updates = vec![(
                    vec!["items".to_string(), key],
                    Some(ArcValue::from_value(
                        json!({"v": i, "name": format!("item-{}", i)}),
                    )),
                )];
                apply_updates(&io_seq, &updates).await.unwrap();
            }

            // Batch: insert all at once
            let io_batch = setup_blob(initial).await;
            let mut updates: Vec<(Vec<String>, Option<ArcValue>)> = Vec::new();
            for i in 0..20 {
                let key = format!("-Mseq{:04}", i);
                updates.push((
                    vec!["items".to_string(), key],
                    Some(ArcValue::from_value(
                        json!({"v": i, "name": format!("item-{}", i)}),
                    )),
                ));
            }
            apply_updates(&io_batch, &updates).await.unwrap();

            // Compare: both should produce identical read results
            // (Compact both to normalize layout differences)
            let dst_seq = CachedIO::new(MemBlobIO::new());
            crate::compact::full_compact(&io_seq, &dst_seq)
                .await
                .unwrap();
            let dst_batch = CachedIO::new(MemBlobIO::new());
            crate::compact::full_compact(&io_batch, &dst_batch)
                .await
                .unwrap();

            let tree_seq = read_root(&dst_seq).await;
            let tree_batch = read_root(&dst_batch).await;
            assert_eq!(
                tree_seq, tree_batch,
                "batch and sequential should produce same data"
            );
        });
    }

    #[test]
    fn test_batch_insert_session_many_batches() {
        block_on(async {
            // Batch inserts through BlobSession — many large batches that
            // accumulate fragmentation. Verifies data integrity throughout.
            use crate::session::BlobSession;

            let io = setup_blob(json!({
                "items": {
                    "-Mseed": {"v": 0}
                }
            }))
            .await;
            let mut session = BlobSession::open(io.clone()).await.unwrap();

            // Insert large batches
            for batch in 0..20 {
                let mut updates: Vec<(Vec<String>, Option<ArcValue>)> = Vec::new();
                for i in 0..20 {
                    let key = format!("-Mb{:02}i{:04}", batch, i);
                    let big_val = format!("value-{}-{}-{}", batch, i, "x".repeat(200));
                    updates.push((
                        vec!["items".to_string(), key],
                        Some(ArcValue::from_value(json!({"v": big_val}))),
                    ));
                }

                session.apply_updates(&updates).await.unwrap();
            }

            // Verify data is still readable
            let seed = read_value_at_path(&io, &["items", "-Mseed", "v"]).await;
            assert_eq!(seed.as_i64(), Some(0));

            // Spot check some batch items
            let root = read_root(&io).await;
            let items = root.get("items").unwrap();
            match items {
                ArcValue::Object(map) => {
                    assert!(map.len() > 20, "should have many items after batch inserts");
                }
                _ => panic!("expected object for items"),
            }
        });
    }
}
