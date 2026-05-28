//! Tree - the root of a database's JSON data structure.
//!
//! Uses ArcValue internally for O(1) cloning and copy-on-write mutations.

use serde_json::Value;

use super::path::Path;
use super::pushid::generate_push_id;
use lark_blob::ArcValue;

/// Tree represents the root of a database tree.
///
/// Internally uses ArcValue for efficient copy-on-write operations.
/// Cloning a Tree is O(1) - it just increments the Arc reference count.
#[derive(Debug, Clone)]
pub struct Tree {
    root: ArcValue,
}

impl Tree {
    /// Create a new empty tree.
    pub fn new() -> Self {
        Self {
            root: ArcValue::empty_object(),
        }
    }

    /// Create a tree with a Sentinel root (blob-backed, nothing loaded yet).
    pub fn new_sentinel() -> Self {
        Self {
            root: ArcValue::empty_sentinel(),
        }
    }

    /// Create a tree from a serde_json::Value.
    pub fn from_value(value: Value) -> Self {
        Self {
            root: ArcValue::from_value(value),
        }
    }

    /// Get a reference to the root value.
    pub fn root(&self) -> &ArcValue {
        &self.root
    }

    /// Get a reference to the value at the given path, or None if not found.
    pub fn get(&self, path: &Path) -> Option<&ArcValue> {
        if path.is_root() {
            return Some(&self.root);
        }
        let segments = path.segments();
        let refs: Vec<&str> = segments.iter().map(|s| s.as_ref()).collect();
        self.root.get_path(&refs)
    }

    /// Get a reference to the value at the given path string.
    pub fn get_str(&self, path: &str) -> Option<&ArcValue> {
        self.get(&Path::parse(path))
    }

    /// Get a clone of the value at the given path (O(1) Arc::clone).
    pub fn get_arc(&self, path: &Path) -> Option<ArcValue> {
        self.get(path).cloned()
    }

    /// Get the value at the given path as a serde_json::Value.
    /// Prefer get_arc() when possible to avoid the conversion cost.
    pub fn get_value(&self, path: &Path) -> Option<Value> {
        self.get(path).map(|v| v.to_value())
    }

    /// Get the value at the given path string as a serde_json::Value.
    pub fn get_value_str(&self, path: &str) -> Option<Value> {
        self.get_value(&Path::parse(path))
    }

    /// Set a value at the given path, creating intermediate nodes as needed.
    /// Values are cleaned before storage: null, {}, and [] are treated as deletions.
    /// Returns true if a value was set, false if it was deleted/cleaned away.
    pub fn set(&mut self, path: &Path, value: Value) -> bool {
        // Clean the value first
        let cleaned = match ArcValue::from_value_cleaned(value) {
            Some(v) => v,
            None => {
                // Cleaned to nothing = deletion
                self.remove(path);
                return false;
            }
        };

        if path.is_root() {
            self.root = cleaned;
            return true;
        }

        // Use mut set_path for in-place mutation when refcount == 1
        let segments = path.segments();
        let refs: Vec<&str> = segments.iter().map(|s| s.as_ref()).collect();
        self.root.set_path_mut(&refs, cleaned);
        true
    }

    /// Set an already-cleaned ArcValue at the given path.
    /// WARNING: Caller must ensure the value is already cleaned (no null/empty children).
    /// Use this when you've already called `from_value_cleaned()` to avoid double-cleaning.
    pub fn set_arc_uncleaned(&mut self, path: &Path, value: ArcValue) -> bool {
        if path.is_root() {
            self.root = value;
            return true;
        }

        let segments = path.segments();
        let refs: Vec<&str> = segments.iter().map(|s| s.as_ref()).collect();
        self.root.set_path_mut(&refs, value);
        true
    }

    /// Set a value at the given path string.
    pub fn set_str(&mut self, path: &str, value: Value) -> bool {
        self.set(&Path::parse(path), value)
    }

    /// Set a value at the given path, creating Sentinel intermediates instead of Object.
    /// Used for blob-backed databases where we don't want to eagerly create real nodes.
    pub fn set_lazy(&mut self, path: &Path, value: Value) -> bool {
        let cleaned = match ArcValue::from_value_cleaned(value) {
            Some(v) => v,
            None => {
                self.remove(path);
                return false;
            }
        };

        if path.is_root() {
            self.root = cleaned;
            return true;
        }

        let segments = path.segments();
        let refs: Vec<&str> = segments.iter().map(|s| s.as_ref()).collect();
        self.root.set_path_mut_sentinel(&refs, cleaned);
        true
    }

    /// Set an already-cleaned ArcValue at the given path using Sentinel intermediates.
    pub fn set_arc_uncleaned_lazy(&mut self, path: &Path, value: ArcValue) -> bool {
        if path.is_root() {
            self.root = value;
            return true;
        }

        let segments = path.segments();
        let refs: Vec<&str> = segments.iter().map(|s| s.as_ref()).collect();
        self.root.set_path_mut_sentinel(&refs, value);
        true
    }

    /// Update performs a partial update at the given path.
    /// Only updates specified keys, leaves others untouched.
    /// Keys containing "/" are treated as relative paths.
    pub fn update(&mut self, path: &Path, updates: &serde_json::Map<String, Value>) {
        for (key, value) in updates {
            // Build the full path for this update
            // Keys may contain "/" and are treated as relative paths
            let update_path = path.join(key);

            // Clean and set/remove the value
            // Use set_arc_uncleaned since from_value_cleaned already cleans
            match ArcValue::from_value_cleaned(value.clone()) {
                Some(cleaned) => {
                    self.set_arc_uncleaned(&update_path, cleaned);
                }
                None => {
                    self.remove(&update_path);
                }
            }
        }
    }

    /// Update at the given path string.
    pub fn update_str(&mut self, path: &str, updates: &serde_json::Map<String, Value>) {
        self.update(&Path::parse(path), updates)
    }

    /// Update at the given path, preserving Sentinel ancestors (for blob-backed DBs).
    /// Each updated key is set via `set_arc_uncleaned_lazy`, so intermediates that
    /// don't yet exist become Sentinels rather than empty Objects — preserving the
    /// "needs promotion" signal so subsequent reads correctly load blob data.
    pub fn update_lazy(&mut self, path: &Path, updates: &serde_json::Map<String, Value>) {
        for (key, value) in updates {
            let update_path = path.join(key);
            match ArcValue::from_value_cleaned(value.clone()) {
                Some(cleaned) => {
                    self.set_arc_uncleaned_lazy(&update_path, cleaned);
                }
                None => {
                    self.remove(&update_path);
                }
            }
        }
    }

    /// Remove the node at the given path.
    /// Also auto-prunes empty parent nodes up the tree.
    /// Returns true if something was removed.
    pub fn remove(&mut self, path: &Path) -> bool {
        if path.is_root() {
            // Can't remove root, but can clear it
            self.root = ArcValue::empty_object();
            return true;
        }

        // Check if path exists first
        if self.get(path).is_none() {
            return false;
        }

        // Use mut remove_path for in-place mutation when refcount == 1
        let segments = path.segments();
        let refs: Vec<&str> = segments.iter().map(|s| s.as_ref()).collect();
        self.root.remove_path_mut(&refs);

        // Auto-prune empty parents by walking up and removing empty containers
        self.prune_empty_parents(path);

        true
    }

    /// Remove at the given path string.
    pub fn remove_str(&mut self, path: &str) -> bool {
        self.remove(&Path::parse(path))
    }

    /// Walk up from the given path and remove any empty parent nodes.
    fn prune_empty_parents(&mut self, path: &Path) {
        let mut current_path = path.parent();

        while let Some(parent_path) = current_path {
            if parent_path.is_root() {
                break;
            }

            // Check if the node at current_path is empty
            let is_empty = self
                .get(&parent_path)
                .map(|n| n.is_empty_container())
                .unwrap_or(false);

            if !is_empty {
                break;
            }

            // Remove the empty container using mut version
            let segments = parent_path.segments();
            let refs: Vec<&str> = segments.iter().map(|s| s.as_ref()).collect();
            self.root.remove_path_mut(&refs);

            current_path = parent_path.parent();
        }
    }

    /// Returns true if a value exists at the given path.
    pub fn exists(&self, path: &Path) -> bool {
        self.get(path).map(|v| v.exists()).unwrap_or(false)
    }

    /// Returns true if a value exists at the given path string.
    pub fn exists_str(&self, path: &str) -> bool {
        self.exists(&Path::parse(path))
    }

    /// Returns true if the node at the given path is a Sentinel (needs promotion).
    pub fn is_sentinel(&self, path: &Path) -> bool {
        self.get(path).map(|v| v.is_sentinel()).unwrap_or(false)
    }

    /// Push creates a new child with an auto-generated push ID.
    /// Returns the generated ID.
    pub fn push(&mut self, path: &Path, value: Value) -> String {
        let id = generate_push_id();
        let full_path = path.join(&id);
        self.set(&full_path, value);
        id
    }

    /// Push at the given path string.
    pub fn push_str(&mut self, path: &str, value: Value) -> String {
        self.push(&Path::parse(path), value)
    }
}

impl Default for Tree {
    fn default() -> Self {
        Self::new()
    }
}

// Implement TreeGetter for rules evaluation
impl crate::rules::TreeGetter for Tree {
    fn get_value(&self, path: &str) -> Option<serde_json::Value> {
        let path = Path::parse(path);
        self.get(&path).map(|v| v.to_value())
    }

    fn get_node_value(&self, path: &str) -> Option<serde_json::Value> {
        let path = Path::parse(path);
        self.get(&path).map(|v| v.to_value())
    }

    fn node_exists(&self, path: &str) -> bool {
        let path = Path::parse(path);
        self.get(&path).map(|v| v.exists()).unwrap_or(false)
    }

    fn node_has_child(&self, path: &str, child_name: &str) -> bool {
        let path = Path::parse(path);
        self.get(&path)
            .and_then(|v| v.get(child_name))
            .map(|c| c.exists())
            .unwrap_or(false)
    }

    fn node_is_loaded(&self, path: &str) -> bool {
        let path = Path::parse(path);
        // get() returns Some for any node in the tree (including Null).
        // It returns None only if the path isn't in the tree at all.
        // A Sentinel is "in the tree" but not "loaded" — it's a placeholder.
        match self.get(&path) {
            Some(node) => !node.is_sentinel(),
            None => {
                // Node is absent. If its parent is loaded (non-Sentinel), the parent
                // has complete knowledge of its children — an absent child definitively
                // does not exist. This avoids needless blob reads for paths like new
                // push IDs under an already-subscribed collection.
                if let Some(parent) = path.parent()
                    && let Some(parent_node) = self.get(&parent)
                {
                    return !parent_node.is_sentinel();
                }
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // =========================================================================
    // Basic Set/Get tests (ported from Go TestTreeSetAndGet)
    // =========================================================================

    #[test]
    fn test_tree_set_and_get() {
        let mut tree = Tree::new();

        // Set a simple value
        tree.set_str("/players/abc/name", json!("Alice"));

        // Get it back
        let got = tree.get_value_str("/players/abc/name");
        assert_eq!(got, Some(json!("Alice")));

        // Get parent object
        let player = tree.get_value_str("/players/abc").unwrap();
        assert!(player.is_object());
        assert_eq!(player.get("name"), Some(&json!("Alice")));
    }

    #[test]
    fn test_tree_set_overwrite() {
        let mut tree = Tree::new();

        tree.set_str("/foo", json!("bar"));
        tree.set_str("/foo", json!("baz"));

        let got = tree.get_value_str("/foo");
        assert_eq!(got, Some(json!("baz")));
    }

    #[test]
    fn test_tree_set_nested_object() {
        let mut tree = Tree::new();

        tree.set_str(
            "/players/abc",
            json!({
                "name": "Alice",
                "score": 100.0,
                "position": {
                    "x": 1.0,
                    "y": 2.0,
                    "z": 3.0
                }
            }),
        );

        // Check nested access
        let x = tree.get_value_str("/players/abc/position/x");
        assert_eq!(x, Some(json!(1.0)));

        // Check full object
        let pos = tree.get_value_str("/players/abc/position").unwrap();
        assert_eq!(pos.get("x"), Some(&json!(1.0)));
        assert_eq!(pos.get("y"), Some(&json!(2.0)));
        assert_eq!(pos.get("z"), Some(&json!(3.0)));
    }

    // =========================================================================
    // Update tests (ported from Go TestTreeUpdate)
    // =========================================================================

    #[test]
    fn test_tree_update() {
        let mut tree = Tree::new();

        // Set initial data
        tree.set_str(
            "/players/abc",
            json!({
                "name": "Alice",
                "score": 100.0,
                "hp": 50.0
            }),
        );

        // Partial update - only score changes
        let mut updates = serde_json::Map::new();
        updates.insert("score".to_string(), json!(150.0));
        tree.update_str("/players/abc", &updates);

        // Check score was updated
        assert_eq!(tree.get_value_str("/players/abc/score"), Some(json!(150.0)));

        // Check name is still there
        assert_eq!(
            tree.get_value_str("/players/abc/name"),
            Some(json!("Alice"))
        );

        // Check hp is still there
        assert_eq!(tree.get_value_str("/players/abc/hp"), Some(json!(50.0)));
    }

    #[test]
    fn test_tree_update_with_delete() {
        let mut tree = Tree::new();

        tree.set_str(
            "/players/abc",
            json!({
                "name": "Alice",
                "score": 100.0
            }),
        );

        // Update with null to delete
        let mut updates = serde_json::Map::new();
        updates.insert("score".to_string(), Value::Null);
        tree.update_str("/players/abc", &updates);

        // Score should be gone
        assert_eq!(tree.get_value_str("/players/abc/score"), None);

        // Name should still exist
        assert_eq!(
            tree.get_value_str("/players/abc/name"),
            Some(json!("Alice"))
        );
    }

    // =========================================================================
    // Remove tests (ported from Go TestTreeRemove)
    // =========================================================================

    #[test]
    fn test_tree_remove() {
        let mut tree = Tree::new();

        tree.set_str("/players/abc/name", json!("Alice"));
        tree.set_str("/players/def/name", json!("Alex"));

        // Remove one player
        let removed = tree.remove_str("/players/abc");
        assert!(removed);

        // abc should be gone
        assert!(!tree.exists_str("/players/abc"));

        // def should still be there
        assert!(tree.exists_str("/players/def"));
    }

    #[test]
    fn test_tree_remove_non_existent() {
        let mut tree = Tree::new();

        let removed = tree.remove_str("/does/not/exist");
        assert!(!removed);
    }

    // =========================================================================
    // Push tests (ported from Go TestTreePush)
    // =========================================================================

    #[test]
    fn test_tree_push() {
        let mut tree = Tree::new();

        let id1 = tree.push_str("/chat", json!({"text": "hello"}));
        let id2 = tree.push_str("/chat", json!({"text": "world"}));

        // IDs should be different
        assert_ne!(id1, id2);

        // IDs should be 20 characters
        assert_eq!(id1.len(), 20);
        assert_eq!(id2.len(), 20);

        // ID2 should sort after ID1 (chronological)
        assert!(id2 > id1);

        // Data should be there
        let msg1 = tree.get_value_str(&format!("/chat/{}", id1)).unwrap();
        assert_eq!(msg1.get("text"), Some(&json!("hello")));
    }

    // =========================================================================
    // Non-existent path tests (ported from Go TestTreeGetNonExistent)
    // =========================================================================

    #[test]
    fn test_tree_get_non_existent() {
        let tree = Tree::new();

        assert!(tree.get_str("/does/not/exist").is_none());
        assert!(tree.get_value_str("/does/not/exist").is_none());
    }

    // =========================================================================
    // Root operations tests (ported from Go TestTreeRootOperations)
    // =========================================================================

    #[test]
    fn test_tree_root_operations() {
        let mut tree = Tree::new();

        // Set at root
        tree.set_str("/", json!({"foo": "bar"}));

        let foo = tree.get_value_str("/foo");
        assert_eq!(foo, Some(json!("bar")));

        // Get root
        let root = tree.get_value_str("/").unwrap();
        assert!(root.is_object());
        assert_eq!(root.get("foo"), Some(&json!("bar")));
    }

    // =========================================================================
    // CleanValue tests (ported from Go TestCleanValueBasic)
    // =========================================================================

    #[test]
    fn test_set_empty_value() {
        let mut tree = Tree::new();

        // Setting {} should result in nothing being stored
        tree.set_str("/foo", json!({}));
        assert!(!tree.exists_str("/foo"));

        // Setting null should result in nothing being stored
        tree.set_str("/bar", Value::Null);
        assert!(!tree.exists_str("/bar"));

        // Setting [] should result in nothing being stored
        tree.set_str("/baz", json!([]));
        assert!(!tree.exists_str("/baz"));
    }

    #[test]
    fn test_set_nested_empty_value() {
        let mut tree = Tree::new();

        // set("/foo", {a: {b: {}}}) should result in nothing stored
        tree.set_str("/foo", json!({"a": {"b": {}}}));
        assert!(!tree.exists_str("/foo"));
    }

    #[test]
    fn test_set_empty_deletes_existing() {
        let mut tree = Tree::new();

        // First set a real value
        tree.set_str("/foo/bar", json!("hello"));
        assert!(tree.exists_str("/foo/bar"));

        // Now set it to empty - should delete
        tree.set_str("/foo/bar", json!({}));
        assert!(!tree.exists_str("/foo/bar"));

        // Parent should also be pruned since it's now empty
        assert!(!tree.exists_str("/foo"));
    }

    #[test]
    fn test_remove_auto_prunes_parents() {
        let mut tree = Tree::new();

        // Create nested structure
        tree.set_str("/users/alice/name", json!("Alice"));

        // Verify structure exists
        assert!(tree.exists_str("/users/alice/name"));
        assert!(tree.exists_str("/users/alice"));
        assert!(tree.exists_str("/users"));

        // Remove the leaf node
        tree.remove_str("/users/alice/name");

        // All parents should be auto-pruned
        assert!(!tree.exists_str("/users/alice/name"));
        assert!(!tree.exists_str("/users/alice"));
        assert!(!tree.exists_str("/users"));
    }

    #[test]
    fn test_remove_preserves_non_empty_siblings() {
        let mut tree = Tree::new();

        // Create structure with siblings
        tree.set_str("/users/alice/name", json!("Alice"));
        tree.set_str("/users/bob/name", json!("Bob"));

        // Remove Alice
        tree.remove_str("/users/alice/name");

        // Alice should be pruned
        assert!(!tree.exists_str("/users/alice"));

        // But /users should still exist (has bob)
        assert!(tree.exists_str("/users"));

        // Bob should be unaffected
        assert!(tree.exists_str("/users/bob/name"));
    }

    // =========================================================================
    // Update with null/empty tests
    // =========================================================================

    #[test]
    fn test_update_with_null_deletes() {
        let mut tree = Tree::new();

        tree.set_str(
            "/users/alice",
            json!({
                "name": "Alice",
                "age": 30
            }),
        );

        // Update with null to delete age
        let mut updates = serde_json::Map::new();
        updates.insert("age".to_string(), Value::Null);
        tree.update_str("/users/alice", &updates);

        // age should be deleted
        assert!(!tree.exists_str("/users/alice/age"));

        // name should still exist
        assert_eq!(
            tree.get_value_str("/users/alice/name"),
            Some(json!("Alice"))
        );
    }

    #[test]
    fn test_update_with_empty_deletes() {
        let mut tree = Tree::new();

        tree.set_str(
            "/users/alice",
            json!({
                "profile": {"bio": "Hello"},
                "name": "Alice"
            }),
        );

        // Update with empty object to delete profile
        let mut updates = serde_json::Map::new();
        updates.insert("profile".to_string(), json!({}));
        tree.update_str("/users/alice", &updates);

        // profile should be deleted
        assert!(!tree.exists_str("/users/alice/profile"));

        // name should still exist
        assert!(tree.exists_str("/users/alice/name"));
    }

    #[test]
    fn test_update_prunes_when_empty() {
        let mut tree = Tree::new();

        // Set initial data with single field
        tree.set_str("/users/alice/name", json!("Alice"));

        // Delete the only field via update
        let mut updates = serde_json::Map::new();
        updates.insert("name".to_string(), Value::Null);
        tree.update_str("/users/alice", &updates);

        // alice and users should be auto-pruned
        assert!(!tree.exists_str("/users/alice"));
        assert!(!tree.exists_str("/users"));
    }

    // =========================================================================
    // Update with path keys (ported from Go TestUpdateWithPathKeys)
    // =========================================================================

    #[test]
    fn test_update_with_path_keys() {
        let mut tree = Tree::new();

        // Update with path-like key should create nested structure
        let mut updates = serde_json::Map::new();
        updates.insert("child/grandchild".to_string(), json!("value1"));
        tree.update_str("/node", &updates);

        // Should be accessible via the full path
        assert_eq!(
            tree.get_value_str("/node/child/grandchild"),
            Some(json!("value1"))
        );

        // Intermediate node should exist
        assert!(tree.exists_str("/node/child"));
    }

    #[test]
    fn test_update_with_path_keys_preserves_siblings() {
        let mut tree = Tree::new();

        // Set initial data
        tree.set_str("/node/existing", json!("keep-me"));

        // Update with path-like key should preserve siblings
        let mut updates = serde_json::Map::new();
        updates.insert("child/grandchild".to_string(), json!("new-value"));
        tree.update_str("/node", &updates);

        // New path should exist
        assert_eq!(
            tree.get_value_str("/node/child/grandchild"),
            Some(json!("new-value"))
        );

        // Existing sibling should still be there
        assert_eq!(tree.get_value_str("/node/existing"), Some(json!("keep-me")));
    }

    #[test]
    fn test_update_with_deep_path_keys() {
        let mut tree = Tree::new();

        // Deep path-like key
        let mut updates = serde_json::Map::new();
        updates.insert("a/b/c/d/e".to_string(), json!(42.0));
        tree.update_str("/root", &updates);

        assert_eq!(tree.get_value_str("/root/a/b/c/d/e"), Some(json!(42.0)));

        // All intermediate nodes should exist
        assert!(tree.exists_str("/root/a"));
        assert!(tree.exists_str("/root/a/b"));
        assert!(tree.exists_str("/root/a/b/c"));
        assert!(tree.exists_str("/root/a/b/c/d"));
    }

    #[test]
    fn test_update_with_mixed_keys() {
        let mut tree = Tree::new();

        // Mix of regular keys and path-like keys
        let mut updates = serde_json::Map::new();
        updates.insert("simple".to_string(), json!("value1"));
        updates.insert("nested/child".to_string(), json!("value2"));
        tree.update_str("/node", &updates);

        assert_eq!(tree.get_value_str("/node/simple"), Some(json!("value1")));
        assert_eq!(
            tree.get_value_str("/node/nested/child"),
            Some(json!("value2"))
        );
    }

    #[test]
    fn test_update_with_path_keys_deletion() {
        let mut tree = Tree::new();

        // Set initial nested data
        tree.set_str("/node/child/grandchild", json!("initial"));
        tree.set_str("/node/child/sibling", json!("keep"));

        // Delete via path-like key
        let mut updates = serde_json::Map::new();
        updates.insert("child/grandchild".to_string(), Value::Null);
        tree.update_str("/node", &updates);

        // grandchild should be gone
        assert!(!tree.exists_str("/node/child/grandchild"));

        // sibling should still exist
        assert_eq!(
            tree.get_value_str("/node/child/sibling"),
            Some(json!("keep"))
        );
    }

    #[test]
    fn test_update_with_path_keys_overwrite() {
        let mut tree = Tree::new();

        // Set initial value
        tree.set_str("/node/child/value", json!("old"));

        // Overwrite via path-like key
        let mut updates = serde_json::Map::new();
        updates.insert("child/value".to_string(), json!("new"));
        tree.update_str("/node", &updates);

        assert_eq!(tree.get_value_str("/node/child/value"), Some(json!("new")));
    }

    #[test]
    fn test_update_server_value_with_path_keys() {
        let mut tree = Tree::new();

        // First update sets initial value at nested path
        let mut updates = serde_json::Map::new();
        updates.insert("child/counter".to_string(), json!(1.0));
        tree.update_str("/node", &updates);

        // Verify it's at the correct path
        assert_eq!(tree.get_value_str("/node/child/counter"), Some(json!(1.0)));

        // Now a second update should be able to read the current value
        let current_val = tree.get_value_str("/node/child/counter");
        assert_eq!(current_val, Some(json!(1.0)));

        // Update the value (simulating increment result)
        let mut updates2 = serde_json::Map::new();
        updates2.insert("child/counter".to_string(), json!(42.0));
        tree.update_str("/node", &updates2);

        assert_eq!(tree.get_value_str("/node/child/counter"), Some(json!(42.0)));
    }

    // =========================================================================
    // ArcValue-specific tests
    // =========================================================================

    #[test]
    fn test_tree_clone_is_cheap() {
        let mut tree = Tree::new();
        tree.set_str(
            "/data",
            json!({"large": "object", "with": "many", "keys": [1, 2, 3]}),
        );

        // Clone should share the same underlying data
        let tree2 = tree.clone();
        assert!(tree.root().ptr_eq(tree2.root()));
    }

    #[test]
    fn test_tree_cow_on_mutation() {
        let mut tree = Tree::new();
        tree.set_str("/users/alice", json!({"score": 100}));
        tree.set_str("/config", json!({"version": 1}));

        // Clone before mutation
        let tree_before = tree.clone();

        // Mutate only alice's score
        tree.set_str("/users/alice/score", json!(200));

        // Root should be different (mutated)
        assert!(!tree.root().ptr_eq(tree_before.root()));

        // But config subtree should still be shared!
        let config_before = tree_before.get_str("/config").unwrap();
        let config_after = tree.get_str("/config").unwrap();
        assert!(config_before.ptr_eq(config_after));
    }

    #[test]
    fn test_get_arc_is_cheap() {
        let mut tree = Tree::new();
        tree.set_str("/data", json!({"big": "value"}));

        // get_arc should return a clone that shares data
        let arc1 = tree.get_arc(&Path::parse("/data")).unwrap();
        let arc2 = tree.get_arc(&Path::parse("/data")).unwrap();

        assert!(arc1.ptr_eq(&arc2));
    }

    // =========================================================================
    // Lazy tree / Sentinel tests
    // =========================================================================

    #[test]
    fn test_new_sentinel_creates_sentinel_root() {
        let tree = Tree::new_sentinel();

        // Root is a Sentinel
        assert!(tree.root().is_sentinel());

        // Root is not visible to normal reads
        assert!(!tree.exists(&Path::parse("/")));

        // Getting unknown children returns None (not a Sentinel)
        assert!(tree.get(&Path::parse("/anything")).is_none());
    }

    #[test]
    fn test_set_lazy_creates_sentinel_intermediates() {
        let mut tree = Tree::new_sentinel();
        tree.set_lazy(&Path::parse("/users/alice/score"), json!(100));

        // The written leaf is reachable
        assert_eq!(tree.get_value_str("/users/alice/score"), Some(json!(100)));

        // Intermediates are Sentinels
        assert!(tree.is_sentinel(&Path::parse("/")));
        assert!(tree.is_sentinel(&Path::parse("/users")));
        assert!(tree.is_sentinel(&Path::parse("/users/alice")));

        // The leaf itself is NOT a Sentinel
        assert!(!tree.is_sentinel(&Path::parse("/users/alice/score")));
    }

    #[test]
    fn test_set_lazy_multiple_writes_preserved() {
        let mut tree = Tree::new_sentinel();

        // Write two different leaves through Sentinel intermediates
        tree.set_lazy(&Path::parse("/users/alice"), json!("Alice"));
        tree.set_lazy(&Path::parse("/users/bob"), json!("Bob"));

        // Both reachable
        assert_eq!(tree.get_value_str("/users/alice"), Some(json!("Alice")));
        assert_eq!(tree.get_value_str("/users/bob"), Some(json!("Bob")));

        // Write in a completely different subtree
        tree.set_lazy(&Path::parse("/config/version"), json!(1));
        assert_eq!(tree.get_value_str("/config/version"), Some(json!(1)));

        // Previous writes still intact
        assert_eq!(tree.get_value_str("/users/alice"), Some(json!("Alice")));
    }

    #[test]
    fn test_set_lazy_overwrites_existing_leaf() {
        let mut tree = Tree::new_sentinel();

        tree.set_lazy(&Path::parse("/key"), json!("old"));
        tree.set_lazy(&Path::parse("/key"), json!("new"));

        assert_eq!(tree.get_value_str("/key"), Some(json!("new")));
    }

    #[test]
    fn test_set_lazy_with_null_removes() {
        let mut tree = Tree::new_sentinel();

        tree.set_lazy(&Path::parse("/key"), json!("value"));
        assert_eq!(tree.get_value_str("/key"), Some(json!("value")));

        // Setting null should remove via the remove path
        let result = tree.set_lazy(&Path::parse("/key"), Value::Null);
        assert!(!result); // returns false for null/empty

        // Value should be gone
        assert!(tree.get(&Path::parse("/key")).is_none());
    }

    #[test]
    fn test_is_sentinel_at_various_paths() {
        let mut tree = Tree::new_sentinel();

        // Root Sentinel
        assert!(tree.is_sentinel(&Path::parse("/")));

        // Unknown child of Sentinel root -> None, not Sentinel
        assert!(!tree.is_sentinel(&Path::parse("/unknown")));

        // After writing, intermediates are Sentinels
        tree.set_lazy(&Path::parse("/a/b/c"), json!(true));
        assert!(tree.is_sentinel(&Path::parse("/a")));
        assert!(tree.is_sentinel(&Path::parse("/a/b")));
        assert!(!tree.is_sentinel(&Path::parse("/a/b/c"))); // leaf is real
    }

    #[test]
    fn test_node_is_loaded_semantics() {
        use crate::rules::TreeGetter;

        let mut tree = Tree::new_sentinel();

        // Sentinel root: not loaded
        assert!(!tree.node_is_loaded("/"));

        // Absent path: not loaded
        assert!(!tree.node_is_loaded("/missing"));

        // Write through Sentinel
        tree.set_lazy(&Path::parse("/users/alice"), json!("Alice"));

        // Sentinel intermediate: not loaded
        assert!(!tree.node_is_loaded("/users"));

        // Real leaf: loaded
        assert!(tree.node_is_loaded("/users/alice"));

        // Still absent sibling: not loaded
        assert!(!tree.node_is_loaded("/users/bob"));
    }

    #[test]
    fn test_node_is_loaded_with_null_value() {
        use crate::rules::TreeGetter;

        let mut tree = Tree::new_sentinel();

        // Insert a real Null at a path (simulating promoted-but-absent data)
        tree.set_arc_uncleaned_lazy(&Path::parse("/checked"), ArcValue::Null);

        // Null IS loaded (we checked the blob and it's not there)
        assert!(tree.node_is_loaded("/checked"));
    }

    #[test]
    fn test_node_is_loaded_on_normal_tree() {
        use crate::rules::TreeGetter;

        let mut tree = Tree::new();
        tree.set_str("/key", json!("value"));

        // Normal tree values are loaded
        assert!(tree.node_is_loaded("/key"));

        // Absent paths under a loaded parent are considered loaded
        // (the parent has complete knowledge of its children)
        assert!(tree.node_is_loaded("/missing"));
    }

    #[test]
    fn test_sentinel_root_get_returns_none_for_unknown_children() {
        let tree = Tree::new_sentinel();

        // Key insight: get() on Sentinel root for unknown child returns None,
        // NOT a Sentinel. None means "not in tree" = "not loaded".
        assert!(tree.get(&Path::parse("/anything")).is_none());
        assert!(tree.get(&Path::parse("/deep/path/here")).is_none());
    }

    #[test]
    fn test_tree_getter_node_exists_through_sentinels() {
        use crate::rules::TreeGetter;

        let mut tree = Tree::new_sentinel();
        tree.set_lazy(&Path::parse("/users/alice"), json!("Alice"));

        // Sentinel root: exists() is false (sentinel is invisible)
        assert!(!tree.node_exists("/"));

        // Sentinel intermediate: exists() is false
        assert!(!tree.node_exists("/users"));

        // Real leaf: exists
        assert!(tree.node_exists("/users/alice"));

        // Absent path: does not exist
        assert!(!tree.node_exists("/users/bob"));
    }
}
