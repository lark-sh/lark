//! Snapshot types for rules evaluation.
//!
//! LazySnapshot provides lazy access to tree data - navigation is free, only val() triggers lookup.
//! Snapshot provides eager access for pre-materialized values (used for newData).

use super::expr::{OBJECT_SENTINEL_MARKER, Snapshot as SnapshotTrait};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::sync::Arc;

/// Error returned when rules evaluation hits data that hasn't been loaded from blob storage.
/// The caller should load the data from BlobSession and retry evaluation.
#[derive(Debug, Clone)]
pub struct NeedsPromotion {
    /// The path that needs to be loaded from blob storage.
    pub path: String,
}

impl std::fmt::Display for NeedsPromotion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "path needs promotion from blob: {}", self.path)
    }
}

impl std::error::Error for NeedsPromotion {}

/// Maximum size (in bytes) of data that val() will return.
/// Larger values return null to prevent memory exhaustion in rules.
/// This protects against rules like: root.child('hugeCollection').val()
const MAX_VAL_SIZE: usize = 100 * 1024; // 100KB

/// TreeGetter provides lazy access to tree values.
/// Implemented by Tree to avoid circular dependencies.
pub trait TreeGetter: Send + Sync {
    /// Returns the value at the given path, or None if not found.
    /// This materializes the value (creates JsonValue from the tree).
    fn get_value(&self, path: &str) -> Option<JsonValue>;

    /// Returns the raw node value at path without deep-copying children.
    /// For objects, can be used to check type/existence.
    fn get_node_value(&self, path: &str) -> Option<JsonValue>;

    /// Returns true if a node exists at the given path.
    fn node_exists(&self, path: &str) -> bool;

    /// Returns true if the node at path has a child with the given name.
    fn node_has_child(&self, path: &str, child_name: &str) -> bool;

    /// Returns true if a node has been loaded at this path — even if it's Null.
    /// This distinguishes "not loaded yet" (false) from "loaded and empty" (true).
    /// Default returns same as node_exists for backwards compat.
    fn node_is_loaded(&self, path: &str) -> bool {
        self.node_exists(path)
    }

    /// Returns true if this tree is backed by blob storage (data may need promotion).
    fn is_blob_backed(&self) -> bool {
        false
    }
}

/// LazySnapshot provides lazy access to tree data for rules evaluation.
///
/// Unlike Snapshot which holds a pre-materialized value, LazySnapshot only
/// fetches data from the tree when val() is called. This avoids deep-copying
/// the entire database on every write just to create the "root" snapshot.
///
/// When the tree contains Sentinel nodes (data not yet loaded from blob),
/// LazySnapshot returns NeedsPromotion errors so the database can load
/// from BlobSession and retry.
#[derive(Clone)]
pub struct LazySnapshot {
    tree: Arc<dyn TreeGetter>,
    path: String,
}

impl std::fmt::Debug for LazySnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LazySnapshot")
            .field("path", &self.path)
            .finish()
    }
}

impl LazySnapshot {
    /// Creates a lazy snapshot that only materializes on val() calls.
    pub fn new(tree: Arc<dyn TreeGetter>, path: String) -> Self {
        Self { tree, path }
    }

    /// Check if accessing this path requires promotion (loading from blob).
    /// Returns Err(NeedsPromotion) if the data hasn't been loaded yet.
    fn check_promotion(&self) -> Result<(), NeedsPromotion> {
        // If not blob-backed, data is always available
        if !self.tree.is_blob_backed() {
            return Ok(());
        }

        // If data has been loaded at this path (even Null), no promotion needed
        if self.tree.node_is_loaded(&self.path) {
            return Ok(());
        }

        // Data not loaded yet — request promotion
        Err(NeedsPromotion {
            path: self.path.clone(),
        })
    }

    /// Build the canonical path string for a child of `self.path`. Mirrors
    /// `LazySnapshot::child`'s path-joining rules.
    fn child_path(&self, name: &str) -> String {
        if self.path.is_empty() {
            name.to_string()
        } else if name.is_empty() {
            self.path.clone()
        } else {
            format!("{}/{}", self.path, name)
        }
    }

    /// Check if a *child* of self.path needs promotion before
    /// `node_has_child` can produce a correct answer. Required because
    /// `tree.node_has_child` calls `c.exists()` on the immediate child
    /// node, and `exists()` on a `Sentinel` returns `false` regardless of
    /// whether the child has data in the blob — so without this guard a
    /// freshly-shallow-loaded container child would report as missing.
    fn check_child_promotion(&self, name: &str) -> Result<(), NeedsPromotion> {
        if !self.tree.is_blob_backed() {
            return Ok(());
        }
        let child = self.child_path(name);
        if self.tree.node_is_loaded(&child) {
            return Ok(());
        }
        Err(NeedsPromotion { path: child })
    }

    /// Returns the value at this path, materializing it from the tree.
    /// Returns None if the value exceeds MAX_VAL_SIZE to prevent memory exhaustion.
    /// For objects/arrays, returns a sentinel marker instead of the actual data.
    fn val_internal(&self) -> Option<JsonValue> {
        let value = self.tree.get_value(&self.path)?;

        // Return sentinel for objects/arrays
        if value.is_object() || value.is_array() {
            return Some(JsonValue::String(OBJECT_SENTINEL_MARKER.to_string()));
        }

        // Check size before returning to prevent huge data from being loaded
        if estimate_size(&value, 0, MAX_VAL_SIZE) > MAX_VAL_SIZE {
            return None; // Too large, return null
        }

        Some(value)
    }

    /// Returns the value at this path.
    pub fn val(&self) -> Option<JsonValue> {
        self.val_internal()
    }

    /// Returns true if a node exists at this path.
    pub fn exists(&self) -> bool {
        self.tree.node_exists(&self.path)
    }

    /// Checks if the node at this path has a specific child.
    pub fn has_child(&self, name: &str) -> bool {
        self.tree.node_has_child(&self.path, name)
    }

    /// Checks if the node has all the specified children.
    pub fn has_children(&self, names: &[String]) -> bool {
        names
            .iter()
            .all(|name| self.tree.node_has_child(&self.path, name))
    }

    /// Returns a lazy snapshot for a child path.
    /// This is a cheap operation - no tree access occurs.
    pub fn child(&self, child_path: &str) -> LazySnapshot {
        let new_path = if self.path.is_empty() {
            child_path.to_string()
        } else if child_path.is_empty() {
            self.path.clone()
        } else {
            format!("{}/{}", self.path, child_path)
        };

        LazySnapshot {
            tree: Arc::clone(&self.tree),
            path: new_path,
        }
    }

    /// Returns a lazy snapshot for the parent path.
    pub fn parent(&self) -> LazySnapshot {
        let new_path = match self.path.rfind('/') {
            Some(idx) if idx > 0 => self.path[..idx].to_string(),
            _ => String::new(),
        };

        LazySnapshot {
            tree: Arc::clone(&self.tree),
            path: new_path,
        }
    }

    /// Returns true if the value at this path is a string.
    pub fn is_string(&self) -> bool {
        self.tree
            .get_node_value(&self.path)
            .map(|v| v.is_string())
            .unwrap_or(false)
    }

    /// Returns true if the value at this path is a number.
    pub fn is_number(&self) -> bool {
        self.tree
            .get_node_value(&self.path)
            .map(|v| v.is_number())
            .unwrap_or(false)
    }

    /// Returns true if the value at this path is a boolean.
    pub fn is_boolean(&self) -> bool {
        self.tree
            .get_node_value(&self.path)
            .map(|v| v.is_boolean())
            .unwrap_or(false)
    }
}

/// Snapshot represents a data snapshot with pre-materialized value.
/// Used for newData where the value is already known.
#[derive(Debug, Clone)]
pub struct Snapshot {
    value: Option<JsonValue>,
    path: String,
}

impl Snapshot {
    /// Creates a new snapshot from a value.
    pub fn new(value: Option<JsonValue>, path: String) -> Self {
        Self { value, path }
    }

    /// Returns the raw value of this snapshot.
    /// Returns None if the value exceeds MAX_VAL_SIZE.
    pub fn val(&self) -> Option<JsonValue> {
        let value = self.value.as_ref()?;

        // Check size before returning
        if estimate_size(value, 0, MAX_VAL_SIZE) > MAX_VAL_SIZE {
            return None;
        }

        Some(value.clone())
    }

    /// Returns true if the value exists (is not null/None).
    pub fn exists(&self) -> bool {
        self.value.is_some() && !self.value.as_ref().unwrap().is_null()
    }

    /// Checks if the snapshot has all the specified children.
    pub fn has_children(&self, names: &[String]) -> bool {
        match &self.value {
            Some(JsonValue::Object(obj)) => names.iter().all(|name| obj.contains_key(name)),
            _ => false,
        }
    }

    /// Checks if the snapshot has a specific child.
    pub fn has_child(&self, name: &str) -> bool {
        match &self.value {
            Some(JsonValue::Object(obj)) => obj.contains_key(name),
            _ => false,
        }
    }

    /// Returns a snapshot for a child path.
    pub fn child(&self, child_path: &str) -> Snapshot {
        let mut value = self.value.clone();

        // Traverse path segments
        for segment in child_path.split('/').filter(|s| !s.is_empty()) {
            value = match value {
                Some(JsonValue::Object(obj)) => obj.get(segment).cloned(),
                _ => None,
            };
        }

        let new_path = if self.path.is_empty() {
            child_path.to_string()
        } else {
            format!("{}/{}", self.path, child_path)
        };

        Snapshot {
            value,
            path: new_path,
        }
    }

    /// Returns a snapshot for the parent path.
    /// Note: This returns a snapshot with None value since we don't track parent values.
    pub fn parent(&self) -> Snapshot {
        let new_path = match self.path.rfind('/') {
            Some(idx) if idx > 0 => self.path[..idx].to_string(),
            _ => String::new(),
        };

        Snapshot {
            value: None,
            path: new_path,
        }
    }

    /// Returns true if the value is a string.
    pub fn is_string(&self) -> bool {
        matches!(&self.value, Some(JsonValue::String(_)))
    }

    /// Returns true if the value is a number.
    pub fn is_number(&self) -> bool {
        matches!(&self.value, Some(JsonValue::Number(_)))
    }

    /// Returns true if the value is a boolean.
    pub fn is_boolean(&self) -> bool {
        matches!(&self.value, Some(JsonValue::Bool(_)))
    }
}

// Implement the expression Snapshot trait for LazySnapshot
impl SnapshotTrait for LazySnapshot {
    fn val(&self) -> Result<Option<JsonValue>, NeedsPromotion> {
        self.check_promotion()?;
        Ok(self.val_internal())
    }

    fn exists(&self) -> Result<bool, NeedsPromotion> {
        self.check_promotion()?;
        Ok(self.tree.node_exists(&self.path))
    }

    fn has_child(&self, name: &str) -> Result<bool, NeedsPromotion> {
        self.check_promotion()?;
        self.check_child_promotion(name)?;
        Ok(self.tree.node_has_child(&self.path, name))
    }

    fn has_children(&self, names: &[String]) -> Result<bool, NeedsPromotion> {
        self.check_promotion()?;
        for name in names {
            self.check_child_promotion(name)?;
        }
        Ok(names
            .iter()
            .all(|name| self.tree.node_has_child(&self.path, name)))
    }

    fn child(&self, path: &str) -> Box<dyn SnapshotTrait> {
        Box::new(LazySnapshot::child(self, path))
    }

    fn parent(&self) -> Box<dyn SnapshotTrait> {
        Box::new(LazySnapshot::parent(self))
    }

    fn is_string(&self) -> Result<bool, NeedsPromotion> {
        self.check_promotion()?;
        Ok(self.is_string())
    }

    fn is_number(&self) -> Result<bool, NeedsPromotion> {
        self.check_promotion()?;
        Ok(self.is_number())
    }

    fn is_boolean(&self) -> Result<bool, NeedsPromotion> {
        self.check_promotion()?;
        Ok(self.is_boolean())
    }

    fn get_priority(&self) -> Result<Option<JsonValue>, NeedsPromotion> {
        // Priority is stored as a .priority child
        // Must use the trait method to get Result return type
        <Self as SnapshotTrait>::child(self, ".priority").val()
    }
}

// Implement the expression Snapshot trait for Snapshot
impl SnapshotTrait for Snapshot {
    fn val(&self) -> Result<Option<JsonValue>, NeedsPromotion> {
        Ok(Snapshot::val(self))
    }

    fn exists(&self) -> Result<bool, NeedsPromotion> {
        Ok(Snapshot::exists(self))
    }

    fn has_child(&self, name: &str) -> Result<bool, NeedsPromotion> {
        Ok(Snapshot::has_child(self, name))
    }

    fn has_children(&self, names: &[String]) -> Result<bool, NeedsPromotion> {
        Ok(Snapshot::has_children(self, names))
    }

    fn child(&self, path: &str) -> Box<dyn SnapshotTrait> {
        Box::new(Snapshot::child(self, path))
    }

    fn parent(&self) -> Box<dyn SnapshotTrait> {
        Box::new(Snapshot::parent(self))
    }

    fn is_string(&self) -> Result<bool, NeedsPromotion> {
        Ok(Snapshot::is_string(self))
    }

    fn is_number(&self) -> Result<bool, NeedsPromotion> {
        Ok(Snapshot::is_number(self))
    }

    fn is_boolean(&self) -> Result<bool, NeedsPromotion> {
        Ok(Snapshot::is_boolean(self))
    }

    fn get_priority(&self) -> Result<Option<JsonValue>, NeedsPromotion> {
        // Priority is stored as a .priority child
        // Must use the trait method to get Result return type
        <Self as SnapshotTrait>::child(self, ".priority").val()
    }
}

/// LazyUpdateSnapshot is the `newData` view for an UPDATE operation.
///
/// It overlays the in-memory `updates` map onto the live tree without
/// materializing the merge. Field accesses resolve to one of three regions:
///
/// 1. **Inside an update path**: `view_path` is at or below
///    `base_path/<update_key>` — value comes from the `updates` map (no
///    tree access).
/// 2. **At or above `base_path` (overlay region)**: `view_path` covers
///    `base_path` — children are the union of tree-children-at-`view_path`
///    and the next segment of any update path. Container reads return the
///    object marker; descending takes you back into
///    one of the three regions.
/// 3. **Outside the update region**: tree-only — same semantics as
///    `LazySnapshot`. Returns `NeedsPromotion` if data isn't loaded.
///
/// Path convention: empty string == root. Non-root paths have a leading
/// "/" (`"/a/b"`). Matches the convention used by `LazySnapshot`.
///
/// # Known limitation: empty-container pruning is not modeled
///
/// Lark prunes empty containers — after a write, a container with no
/// non-null leaves is removed (see `prune_empty_parents` in `db/tree.rs`).
/// `exists()` and `has_child()` here do **not** account for that
/// pruning. Concretely: if the tree has only `/foo/bar` and the UPDATE
/// is `{"foo/bar": null}`, the post-write tree would have `/foo` pruned
/// (now empty), but `newData.foo.exists()` and `newData.has_child("foo")`
/// will still report `true` because they only check exact-match deletes
/// at the immediate child level, not recursive emptiness.
///
/// This is a Firebase-semantics gap. The fix needs:
///   - a `TreeGetter::node_children(path)` method to enumerate tree
///     children, and
///   - a recursive `exists()` that descends through children and stops
///     at the first surviving (non-deleted) leaf.
#[derive(Clone)]
pub struct LazyUpdateSnapshot {
    tree: Arc<dyn TreeGetter>,
    /// The path the UPDATE is targeted at.
    base_path: Arc<str>,
    /// The UPDATE's key/value map. Keys may contain '/' (multi-path).
    /// `Arc<Map>` so cheap to clone when constructing child snapshots.
    updates: Arc<serde_json::Map<String, JsonValue>>,
    /// The path being snapshotted. Always absolute (or empty for root).
    view_path: String,
}

impl std::fmt::Debug for LazyUpdateSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LazyUpdateSnapshot")
            .field("base_path", &self.base_path)
            .field("view_path", &self.view_path)
            .field("update_keys", &self.updates.keys().collect::<Vec<_>>())
            .finish()
    }
}

/// What region the snapshot's `view_path` falls in relative to the
/// `(base_path, updates)` overlay.
#[derive(Debug)]
enum UpdateRegion<'a> {
    /// `view_path` is exactly at the leaf of an update path.
    AtUpdateLeaf { value: &'a JsonValue },
    /// `view_path` is strictly inside an update value: trailing path
    /// remains to descend into `value`.
    InsideUpdateValue {
        value: &'a JsonValue,
        trailing: &'a str,
    },
    /// `view_path` is in the "overlay region" — at, above, or
    /// alongside `base_path`. The merged view is the tree at `view_path`
    /// plus whatever the updates contribute via paths underneath.
    Overlay,
    /// `view_path` is unrelated to the overlay. Tree-only.
    TreeOnly,
}

impl LazyUpdateSnapshot {
    /// Creates a snapshot at a specific `view_path`. Used by the rules
    /// engine via `NewData::snapshot_at` to materialize ancestor or
    /// descendant snapshots without materializing the merge.
    pub fn with_view_path(
        tree: Arc<dyn TreeGetter>,
        base_path: String,
        updates: Arc<serde_json::Map<String, JsonValue>>,
        view_path: String,
    ) -> Self {
        Self {
            tree,
            base_path: Arc::from(normalize_path(&base_path)),
            updates,
            view_path: normalize_path(&view_path).to_string(),
        }
    }

    /// Build the absolute path of an update entry. e.g. `base="/a"`,
    /// `key="b/c"` → `"/a/b/c"`. `base="" or "/"` is root.
    fn full_update_path(&self, update_key: &str) -> String {
        let key = update_key.trim_matches('/');
        let base = self.base_path.as_ref();
        if base.is_empty() || base == "/" {
            if key.is_empty() {
                String::new()
            } else {
                format!("/{}", key)
            }
        } else if key.is_empty() {
            base.to_string()
        } else {
            format!("{}/{}", base, key)
        }
    }

    /// Classify `view_path` relative to the overlay.
    ///
    /// Walks every update entry — O(updates.len() * avg_key_depth). For
    /// the multi-path PATCH workloads this is small (a handful of keys);
    /// if it ever grows we can index by first segment.
    fn region(&self) -> UpdateRegion<'_> {
        let view = self.view_path.as_str();

        // Check whether `view` lands inside one of the update paths.
        for (key, value) in self.updates.iter() {
            let full = self.full_update_path(key);
            if view == full {
                return UpdateRegion::AtUpdateLeaf { value };
            }
            if let Some(trailing) = view.strip_prefix(&full).and_then(|t| t.strip_prefix('/')) {
                return UpdateRegion::InsideUpdateValue { value, trailing };
            }
        }

        // Overlay = view is at-or-above base_path, OR at-or-above any
        // update path. These are the paths that "see" the merged structure
        // from above. Anything else (including descendants of base_path
        // that aren't on an update path) defers to the tree.
        let base = self.base_path.as_ref();
        if is_path_at_or_above(view, base) {
            return UpdateRegion::Overlay;
        }
        for key in self.updates.keys() {
            let full = self.full_update_path(key);
            if is_path_at_or_above(view, &full) {
                return UpdateRegion::Overlay;
            }
        }

        UpdateRegion::TreeOnly
    }

    /// Build a child snapshot at `view_path/name`.
    fn child_snapshot(&self, name: &str) -> LazyUpdateSnapshot {
        let new_view = if self.view_path.is_empty() {
            format!("/{}", name)
        } else {
            format!("{}/{}", self.view_path, name)
        };
        LazyUpdateSnapshot {
            tree: Arc::clone(&self.tree),
            base_path: Arc::clone(&self.base_path),
            updates: Arc::clone(&self.updates),
            view_path: new_view,
        }
    }

    fn parent_snapshot(&self) -> LazyUpdateSnapshot {
        let new_view = match self.view_path.rfind('/') {
            Some(idx) if idx > 0 => self.view_path[..idx].to_string(),
            _ => String::new(),
        };
        LazyUpdateSnapshot {
            tree: Arc::clone(&self.tree),
            base_path: Arc::clone(&self.base_path),
            updates: Arc::clone(&self.updates),
            view_path: new_view,
        }
    }

    /// Tree-side `node_is_loaded` check, for paths that need a tree
    /// fetch. Returns Err(NeedsPromotion) if blob-backed and unloaded.
    fn check_tree_promotion(&self, path: &str) -> Result<(), NeedsPromotion> {
        if !self.tree.is_blob_backed() {
            return Ok(());
        }
        if self.tree.node_is_loaded(path) {
            return Ok(());
        }
        Err(NeedsPromotion {
            path: path.to_string(),
        })
    }
}

/// Normalize a path for `LazyUpdateSnapshot`: root becomes empty string,
/// otherwise has a single leading '/' and no trailing '/'.
fn normalize_path(p: &str) -> String {
    let trimmed = p.trim_matches('/');
    if trimmed.is_empty() {
        String::new()
    } else {
        format!("/{}", trimmed)
    }
}

/// Returns true if `a` is at or above `b` in the path hierarchy.
/// Root ("") is at-or-above every path.
fn is_path_at_or_above(a: &str, b: &str) -> bool {
    if a.is_empty() {
        return true;
    }
    if a == b {
        return true;
    }
    b.starts_with(a) && b.as_bytes().get(a.len()) == Some(&b'/')
}

/// Descend into `value` following the `path` (slash-separated). Returns
/// the leaf if path is empty, or None if the descent leaves the JSON
/// (e.g. tries to walk into a primitive).
fn descend_into<'a>(value: &'a JsonValue, path: &str) -> Option<&'a JsonValue> {
    let mut current = value;
    for seg in path.split('/').filter(|s| !s.is_empty()) {
        current = match current {
            JsonValue::Object(obj) => obj.get(seg)?,
            _ => return None,
        };
    }
    Some(current)
}

impl SnapshotTrait for LazyUpdateSnapshot {
    fn val(&self) -> Result<Option<JsonValue>, NeedsPromotion> {
        match self.region() {
            UpdateRegion::AtUpdateLeaf { value } => {
                // Update value is in-memory and bounded by the write payload —
                // return it directly (matching eager `Snapshot::val()`), so
                // rules like `newData.hasChildren()` see an actual Object,
                // not the lazy-tree marker.
                if value.is_null() || estimate_size(value, 0, MAX_VAL_SIZE) > MAX_VAL_SIZE {
                    Ok(None)
                } else {
                    Ok(Some(value.clone()))
                }
            }
            UpdateRegion::InsideUpdateValue { value, trailing } => {
                let leaf = descend_into(value, trailing);
                match leaf {
                    None => Ok(None),
                    Some(v) if v.is_null() => Ok(None),
                    Some(v) => {
                        if estimate_size(v, 0, MAX_VAL_SIZE) > MAX_VAL_SIZE {
                            return Ok(None);
                        }
                        Ok(Some(v.clone()))
                    }
                }
            }
            UpdateRegion::Overlay => {
                // Overlay region: the merged view is tree-children +
                // update-key contributions. Materializing it would defeat
                // the laziness, so return the container marker: intended
                // newData semantics for ancestor containers. Rules that
                // need to introspect specific children should use
                // `has_child(name)`, which we resolve without materializing.
                Ok(Some(JsonValue::String(OBJECT_SENTINEL_MARKER.to_string())))
            }
            UpdateRegion::TreeOnly => {
                self.check_tree_promotion(&self.view_path)?;
                let v = self.tree.get_value(&self.view_path);
                match v {
                    None => Ok(None),
                    Some(v) if v.is_object() || v.is_array() => {
                        Ok(Some(JsonValue::String(OBJECT_SENTINEL_MARKER.to_string())))
                    }
                    Some(v) => {
                        if estimate_size(&v, 0, MAX_VAL_SIZE) > MAX_VAL_SIZE {
                            return Ok(None);
                        }
                        Ok(Some(v))
                    }
                }
            }
        }
    }

    fn exists(&self) -> Result<bool, NeedsPromotion> {
        match self.region() {
            UpdateRegion::AtUpdateLeaf { value } => Ok(!value.is_null()),
            UpdateRegion::InsideUpdateValue { value, trailing } => {
                Ok(matches!(descend_into(value, trailing), Some(v) if !v.is_null()))
            }
            UpdateRegion::Overlay => {
                // Exists if either the tree has the path OR any update
                // writes a non-null value at-or-below view_path.
                self.check_tree_promotion(&self.view_path)?;
                if self.tree.node_exists(&self.view_path) {
                    return Ok(true);
                }
                let view = self.view_path.as_str();
                for (key, value) in self.updates.iter() {
                    if value.is_null() {
                        continue;
                    }
                    let full = self.full_update_path(key);
                    if full == view
                        || full.starts_with(view)
                            && (view.is_empty() || full.as_bytes().get(view.len()) == Some(&b'/'))
                    {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            UpdateRegion::TreeOnly => {
                self.check_tree_promotion(&self.view_path)?;
                Ok(self.tree.node_exists(&self.view_path))
            }
        }
    }

    fn has_child(&self, name: &str) -> Result<bool, NeedsPromotion> {
        let view = self.view_path.as_str();
        let child_path = if view.is_empty() {
            format!("/{}", name)
        } else {
            format!("{}/{}", view, name)
        };

        match self.region() {
            UpdateRegion::AtUpdateLeaf { value } => {
                // Children come from the update value itself.
                Ok(value
                    .as_object()
                    .and_then(|o| o.get(name))
                    .is_some_and(|v| !v.is_null()))
            }
            UpdateRegion::InsideUpdateValue { value, trailing } => {
                let leaf = descend_into(value, trailing);
                Ok(leaf
                    .and_then(|v| v.as_object())
                    .and_then(|o| o.get(name))
                    .is_some_and(|v| !v.is_null()))
            }
            UpdateRegion::Overlay | UpdateRegion::TreeOnly => {
                // Two effects from updates:
                //   - explicit_delete: an update at exactly child_path with
                //     null value removes the child from the merged view
                //     (overrides tree).
                //   - any non-null update at-or-below child_path makes the
                //     child structurally exist (overrides absence in tree).
                let mut explicit_delete = false;
                for (key, value) in self.updates.iter() {
                    let full = self.full_update_path(key);
                    if full == child_path {
                        if value.is_null() {
                            explicit_delete = true;
                        } else {
                            return Ok(true);
                        }
                    } else if is_path_at_or_above(&child_path, &full) {
                        // child_path is an ancestor of full → update writes
                        // beneath child_path, contributing structure to it.
                        if !value.is_null() {
                            return Ok(true);
                        }
                    }
                    // Otherwise full is unrelated or above child_path; for
                    // the "above" case the AtUpdateLeaf/InsideUpdateValue
                    // region check earlier would have handled it.
                }

                if explicit_delete {
                    return Ok(false);
                }

                // Defer to tree.
                self.check_tree_promotion(view)?;
                self.check_tree_promotion(&child_path)?;
                Ok(self.tree.node_has_child(view, name))
            }
        }
    }

    fn has_children(&self, names: &[String]) -> Result<bool, NeedsPromotion> {
        for name in names {
            if !self.has_child(name)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn child(&self, path: &str) -> Box<dyn SnapshotTrait> {
        // Walk segment-by-segment so ".." stays out and multi-segment
        // paths like "a/b/c" land at the right place.
        let mut current = self.clone();
        for seg in path.split('/').filter(|s| !s.is_empty()) {
            current = current.child_snapshot(seg);
        }
        Box::new(current)
    }

    fn parent(&self) -> Box<dyn SnapshotTrait> {
        Box::new(self.parent_snapshot())
    }

    fn is_string(&self) -> Result<bool, NeedsPromotion> {
        Ok(matches!(self.val()?, Some(JsonValue::String(_))))
    }

    fn is_number(&self) -> Result<bool, NeedsPromotion> {
        Ok(matches!(self.val()?, Some(JsonValue::Number(_))))
    }

    fn is_boolean(&self) -> Result<bool, NeedsPromotion> {
        Ok(matches!(self.val()?, Some(JsonValue::Bool(_))))
    }

    fn get_priority(&self) -> Result<Option<JsonValue>, NeedsPromotion> {
        <Self as SnapshotTrait>::child(self, ".priority").val()
    }
}

/// `EmptyTree` is a no-op `TreeGetter` used as a fallback when a context
/// has no `root_tree` (e.g. tests that exercise rules without a backing
/// tree). All accessors return "nothing here" — `node_exists` is false,
/// `get_value` is None, `is_blob_backed` is false (so no NeedsPromotion
/// errors). Production paths always have a real tree.
pub struct EmptyTree;

impl TreeGetter for EmptyTree {
    fn get_value(&self, _: &str) -> Option<JsonValue> {
        None
    }
    fn get_node_value(&self, _: &str) -> Option<JsonValue> {
        None
    }
    fn node_exists(&self, _: &str) -> bool {
        false
    }
    fn node_has_child(&self, _: &str, _: &str) -> bool {
        false
    }
    fn node_is_loaded(&self, _: &str) -> bool {
        true
    }
    fn is_blob_backed(&self) -> bool {
        false
    }
}

/// `NewData` represents the "what's being written" side of a rules
/// evaluation, in a form that can produce a `Box<dyn SnapshotTrait>` at
/// any view_path on demand. Replaces the old eager
/// `Option<JsonValue>` / `compute_new_data_at_ancestor` pattern.
///
/// Two flavors:
///   - **Set**: a SET write at `set_path` with a single value.
///   - **Update**: an UPDATE write at `base_path` with a (possibly
///     multi-path) updates map.
///
/// Both flavors produce snapshots via `snapshot_at(tree, view_path)`,
/// which always returns a `LazyUpdateSnapshot`. SET is internally
/// represented as a single-key updates map at root — the snapshot regions
/// then handle the SET semantics correctly (children at view_path =
/// set_path come from the value; descendants inside the value are served
/// from it; siblings of set_path at any ancestor defer to the tree).
#[derive(Clone)]
pub enum NewData {
    Set {
        /// The absolute path the SET targets. Stored for diagnostic /
        /// validate_children use; the snapshot uses the synthesized
        /// `backing` map.
        set_path: String,
        /// One-key map `{set_path_no_leading_slash: value}` at root,
        /// pre-built so `snapshot_at` is allocation-free for the map.
        backing: Arc<serde_json::Map<String, JsonValue>>,
    },
    Update {
        /// The path the UPDATE is targeted at.
        base_path: String,
        /// The UPDATE's key/value map. Keys may contain '/' (multi-path).
        updates: Arc<serde_json::Map<String, JsonValue>>,
    },
}

impl std::fmt::Debug for NewData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NewData::Set { set_path, .. } => f
                .debug_struct("NewData::Set")
                .field("set_path", set_path)
                .finish(),
            NewData::Update { base_path, updates } => f
                .debug_struct("NewData::Update")
                .field("base_path", base_path)
                .field("update_keys", &updates.keys().collect::<Vec<_>>())
                .finish(),
        }
    }
}

impl NewData {
    /// Build NewData for a SET write.
    pub fn from_set(set_path: String, value: JsonValue) -> Self {
        let key = set_path.trim_start_matches('/').to_string();
        let mut map = serde_json::Map::new();
        map.insert(key, value);
        Self::Set {
            set_path,
            backing: Arc::new(map),
        }
    }

    /// Build NewData for an UPDATE write.
    pub fn from_update(base_path: String, updates: serde_json::Map<String, JsonValue>) -> Self {
        Self::Update {
            base_path,
            updates: Arc::new(updates),
        }
    }

    /// Produce a Snapshot-trait view at `view_path`. Always returns a
    /// `LazyUpdateSnapshot` — no eager materialization.
    pub fn snapshot_at(
        &self,
        tree: Arc<dyn TreeGetter>,
        view_path: &str,
    ) -> Box<dyn SnapshotTrait> {
        let (base, updates) = match self {
            NewData::Set { backing, .. } => (String::new(), Arc::clone(backing)),
            NewData::Update { base_path, updates } => (base_path.clone(), Arc::clone(updates)),
        };
        Box::new(LazyUpdateSnapshot::with_view_path(
            tree,
            base,
            updates,
            view_path.to_string(),
        ))
    }

    /// Iterate the children at `view_path` that this write is actually
    /// modifying. Returns `(child_name, partial_value)` pairs.
    ///
    /// Used by `validate_children` to fire `.validate` rules only on
    /// children that are being written — matches intended semantics
    /// ("validate runs on writes") rather than the eager-merge behavior
    /// of the previous `materialize_at` path which iterated *every*
    /// merged child including untouched tree-existing siblings.
    ///
    /// Behavior:
    ///   - SET at `set_path` with value `V`:
    ///     - `view_path == set_path`: yields `V`'s children.
    ///     - `view_path` is below `set_path`: descends into `V`, yields
    ///       the descendant's children.
    ///     - `view_path` is above `set_path`: yields the next path
    ///       segment of `set_path` past `view_path`, with a partial
    ///       value built by wrapping `V` in the remaining path.
    ///   - UPDATE at `base_path` with `updates`:
    ///     - For each update entry whose full path is at-or-below
    ///       `view_path`, contribute the relevant child name and
    ///       partial value. Multiple updates writing under the same
    ///       child (e.g. `{"a/b": 1, "a/c": 2}`) merge into one entry
    ///       (`("a", {b: 1, c: 2})`) — last-write-wins for collisions.
    pub fn writes_at(&self, view_path: &str) -> Vec<(String, JsonValue)> {
        let view = normalize_path(view_path);
        let view = view.trim_start_matches('/');

        let mut by_child: std::collections::HashMap<String, JsonValue> =
            std::collections::HashMap::new();

        let merge_into = |by_child: &mut std::collections::HashMap<String, JsonValue>,
                          name: String,
                          value: JsonValue| {
            match by_child.remove(&name) {
                Some(existing) => {
                    by_child.insert(name, json_merge(existing, value));
                }
                None => {
                    by_child.insert(name, value);
                }
            }
        };

        match self {
            NewData::Set { set_path, backing } => {
                let value = match backing.values().next() {
                    Some(v) => v,
                    None => return Vec::new(),
                };
                let setp = set_path.trim_matches('/');
                if view == setp {
                    if let Some(obj) = value.as_object() {
                        for (k, v) in obj {
                            by_child.insert(k.clone(), v.clone());
                        }
                    }
                } else if let Some(rest) = view.strip_prefix(setp).and_then(|s| {
                    if setp.is_empty() {
                        Some(s)
                    } else {
                        s.strip_prefix('/')
                    }
                }) {
                    // view below set_path → descend into value.
                    if let Some(sub) = descend_into(value, rest)
                        && let Some(obj) = sub.as_object()
                    {
                        for (k, v) in obj {
                            by_child.insert(k.clone(), v.clone());
                        }
                    }
                } else if let Some(rest) = setp.strip_prefix(view).and_then(|s| {
                    if view.is_empty() {
                        Some(s)
                    } else {
                        s.strip_prefix('/')
                    }
                }) {
                    // view above set_path → next segment of set_path is the
                    // child being written.
                    let child_name = rest.split('/').next().unwrap_or("");
                    if !child_name.is_empty() {
                        let descend_after_child = &rest[child_name.len()..].trim_start_matches('/');
                        let child_value = if descend_after_child.is_empty() {
                            value.clone()
                        } else {
                            let segs: Vec<&str> = descend_after_child
                                .split('/')
                                .filter(|s| !s.is_empty())
                                .collect();
                            wrap_in_path(value.clone(), &segs)
                        };
                        by_child.insert(child_name.to_string(), child_value);
                    }
                }
            }
            NewData::Update { base_path, updates } => {
                for (key, value) in updates.iter() {
                    let full = full_update_path(base_path, key);
                    let full = full.trim_matches('/');
                    if view != full
                        && !(view.is_empty()
                            || full.strip_prefix(view).is_some_and(|s| s.starts_with('/')))
                    {
                        continue;
                    }
                    let rel = if view.is_empty() {
                        full.to_string()
                    } else if view == full {
                        String::new()
                    } else {
                        full[view.len() + 1..].to_string()
                    };
                    let segs: Vec<&str> = rel.split('/').filter(|s| !s.is_empty()).collect();
                    if segs.is_empty() {
                        // Update writes exactly at view. Treat its value's
                        // children as the children being written.
                        if let Some(obj) = value.as_object() {
                            for (k, v) in obj {
                                merge_into(&mut by_child, k.clone(), v.clone());
                            }
                        }
                    } else {
                        let child_name = segs[0].to_string();
                        let child_partial = if segs.len() == 1 {
                            value.clone()
                        } else {
                            wrap_in_path(value.clone(), &segs[1..])
                        };
                        merge_into(&mut by_child, child_name, child_partial);
                    }
                }
            }
        }

        by_child.into_iter().collect()
    }
}

/// Wrap `value` in nested Objects following `segs`. e.g. `wrap_in_path(V,
/// &["a", "b"])` returns `{"a": {"b": V}}`. Used by `writes_at` to build
/// partial values for multi-path UPDATE keys.
fn wrap_in_path(value: JsonValue, segs: &[&str]) -> JsonValue {
    let mut current = value;
    for seg in segs.iter().rev() {
        let mut map = serde_json::Map::new();
        map.insert((*seg).to_string(), current);
        current = JsonValue::Object(map);
    }
    current
}

/// Merge `b` into `a`, last-write-wins for primitive collisions.
/// Recursively merges Objects key-by-key. Used by `writes_at` to combine
/// multiple updates that contribute to the same child (e.g.
/// `{"a/b": 1, "a/c": 2}` → `("a", {b: 1, c: 2})`).
fn json_merge(a: JsonValue, b: JsonValue) -> JsonValue {
    match (a, b) {
        (JsonValue::Object(mut a_map), JsonValue::Object(b_map)) => {
            for (k, v) in b_map {
                let existing = a_map.remove(&k);
                let merged = match existing {
                    Some(prev) => json_merge(prev, v),
                    None => v,
                };
                a_map.insert(k, merged);
            }
            JsonValue::Object(a_map)
        }
        (_, b) => b,
    }
}

fn full_update_path(base_path: &str, update_key: &str) -> String {
    let key = update_key.trim_matches('/');
    let base = base_path.trim_matches('/');
    if base.is_empty() {
        if key.is_empty() {
            String::new()
        } else {
            format!("/{}", key)
        }
    } else if key.is_empty() {
        format!("/{}", base)
    } else {
        format!("/{}/{}", base, key)
    }
}

/// Estimates the JSON-serialized size of a value.
/// Stops early once max_size is exceeded to avoid wasting time on huge values.
fn estimate_size(value: &JsonValue, current: usize, max_size: usize) -> usize {
    if current > max_size {
        return current; // Already exceeded, bail out early
    }

    match value {
        JsonValue::Null => current + 4,                // "null"
        JsonValue::Bool(_) => current + 5,             // "true" or "false"
        JsonValue::String(s) => current + s.len() + 2, // quotes + content
        JsonValue::Number(_) => current + 20,          // conservative estimate for numbers
        JsonValue::Array(arr) => {
            let mut size = current + 2; // []
            for v in arr {
                size = estimate_size(v, size, max_size);
                if size > max_size {
                    return size;
                }
            }
            size
        }
        JsonValue::Object(obj) => {
            let mut size = current + 2; // {}
            for (k, v) in obj {
                size += k.len() + 3; // "key":
                size = estimate_size(v, size, max_size);
                if size > max_size {
                    return size;
                }
            }
            size
        }
    }
}

/// AuthInfo holds authentication information for rules evaluation.
#[derive(Debug, Clone, Default)]
pub struct AuthInfo {
    /// User ID (uid claim from token).
    pub uid: Option<String>,
    /// Auth provider (password, anonymous, google, etc.).
    pub provider: Option<String>,
    /// ID token claims.
    pub token: Option<serde_json::Map<String, JsonValue>>,
    /// True if token was signed with admin_secret_key (bypasses rules).
    pub is_true_admin: bool,
    /// Cached JSON representation for rules evaluation (computed once, reused many times).
    /// Stored as Arc<HashMap> so cloning is O(1) - critical for per-write rules evaluation.
    cached_json: Option<Arc<HashMap<String, JsonValue>>>,
}

impl AuthInfo {
    /// Creates a new AuthInfo with all fields, pre-computing the cached JSON.
    pub fn new(
        uid: Option<String>,
        provider: Option<String>,
        token: Option<serde_json::Map<String, JsonValue>>,
        is_true_admin: bool,
    ) -> Self {
        let mut info = Self {
            uid,
            provider,
            token,
            is_true_admin,
            cached_json: None,
        };
        info.cached_json = info.compute_json();
        info
    }

    /// Creates a new AuthInfo with the given UID.
    pub fn with_uid(uid: String) -> Self {
        Self::new(Some(uid), None, None, false)
    }

    /// Creates an admin AuthInfo that bypasses all rules.
    pub fn admin() -> Self {
        Self {
            is_true_admin: true,
            ..Default::default()
        }
    }

    /// Pre-compute and cache the JSON representation.
    /// Call this after constructing AuthInfo to avoid repeated computation during rules evaluation.
    pub fn with_cached_json(mut self) -> Self {
        self.cached_json = self.compute_json();
        self
    }

    /// Converts auth info to a JSON value for use in expressions.
    /// Returns cached value if available, otherwise computes it.
    /// Token claims are hoisted to the top level:
    /// - auth.player_id (hoisted from token)
    /// - auth.token.player_id (original location)
    ///
    /// Returns Arc<HashMap> so cloning is O(1) for per-write rules evaluation.
    pub fn to_json(&self) -> Option<Arc<HashMap<String, JsonValue>>> {
        // Return cached value - Arc::clone is O(1)
        if let Some(ref cached) = self.cached_json {
            return Some(Arc::clone(cached));
        }
        self.compute_json()
    }

    /// Compute the JSON representation (internal helper).
    /// Returns Arc<HashMap> for O(1) cloning during rules evaluation.
    fn compute_json(&self) -> Option<Arc<HashMap<String, JsonValue>>> {
        if self.uid.is_none() && self.token.is_none() {
            return None;
        }

        let mut map = HashMap::new();

        if let Some(ref uid) = self.uid {
            map.insert("uid".to_string(), JsonValue::String(uid.clone()));
        }

        if let Some(ref provider) = self.provider {
            map.insert("provider".to_string(), JsonValue::String(provider.clone()));
        }

        if let Some(ref token) = self.token {
            // Hoist token claims to top level
            // This allows rules to use both auth.player_id and auth.token.player_id
            for (k, v) in token {
                map.insert(k.clone(), v.clone());
            }
            // Also keep them at auth.token for explicit access
            map.insert("token".to_string(), JsonValue::Object(token.clone()));
        }

        Some(Arc::new(map))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// Test implementation of TreeGetter
    struct TestTree {
        data: HashMap<String, JsonValue>,
    }

    impl TestTree {
        fn new() -> Self {
            Self {
                data: HashMap::new(),
            }
        }

        fn set(&mut self, path: &str, value: JsonValue) {
            self.data.insert(path.to_string(), value);
        }
    }

    impl TreeGetter for TestTree {
        fn get_value(&self, path: &str) -> Option<JsonValue> {
            self.data.get(path).cloned()
        }

        fn get_node_value(&self, path: &str) -> Option<JsonValue> {
            self.data.get(path).cloned()
        }

        fn node_exists(&self, path: &str) -> bool {
            self.data.contains_key(path)
        }

        fn node_has_child(&self, path: &str, child_name: &str) -> bool {
            let child_path = if path.is_empty() {
                child_name.to_string()
            } else {
                format!("{}/{}", path, child_name)
            };
            self.data.contains_key(&child_path)
        }
    }

    #[test]
    fn test_lazy_snapshot_val() {
        let mut tree = TestTree::new();
        tree.set("users/abc", serde_json::json!({"name": "Alice"}));
        tree.set("users/abc/name", serde_json::json!("Alice"));

        let tree: Arc<dyn TreeGetter> = Arc::new(tree);
        let snap = LazySnapshot::new(tree.clone(), "users/abc".to_string());

        // Objects return a sentinel marker instead of actual data
        let val = snap.val().unwrap();
        assert_eq!(val, serde_json::json!(OBJECT_SENTINEL_MARKER));

        // Primitive values are returned directly
        let snap_name = LazySnapshot::new(tree, "users/abc/name".to_string());
        let name_val = snap_name.val().unwrap();
        assert_eq!(name_val, serde_json::json!("Alice"));
    }

    #[test]
    fn test_lazy_snapshot_exists() {
        let mut tree = TestTree::new();
        tree.set("users/abc", serde_json::json!({"name": "Alice"}));

        let tree: Arc<dyn TreeGetter> = Arc::new(tree);
        let snap1 = LazySnapshot::new(Arc::clone(&tree), "users/abc".to_string());
        let snap2 = LazySnapshot::new(Arc::clone(&tree), "users/xyz".to_string());

        assert!(snap1.exists());
        assert!(!snap2.exists());
    }

    #[test]
    fn test_lazy_snapshot_child() {
        let mut tree = TestTree::new();
        tree.set("users/abc/name", JsonValue::String("Alice".to_string()));

        let tree: Arc<dyn TreeGetter> = Arc::new(tree);
        let snap = LazySnapshot::new(tree, "users".to_string());
        let child = snap.child("abc").child("name");

        assert_eq!(child.val().unwrap(), JsonValue::String("Alice".to_string()));
    }

    #[test]
    fn test_lazy_snapshot_parent() {
        let tree: Arc<dyn TreeGetter> = Arc::new(TestTree::new());
        let snap = LazySnapshot::new(tree, "users/abc/name".to_string());

        let parent = snap.parent();
        assert_eq!(parent.path, "users/abc");

        let grandparent = parent.parent();
        assert_eq!(grandparent.path, "users");

        let root = grandparent.parent();
        assert_eq!(root.path, "");
    }

    #[test]
    fn test_snapshot_val() {
        let snap = Snapshot::new(
            Some(serde_json::json!({"name": "Alice"})),
            "users/abc".to_string(),
        );
        let val = snap.val().unwrap();
        assert_eq!(val, serde_json::json!({"name": "Alice"}));
    }

    #[test]
    fn test_snapshot_exists() {
        let snap1 = Snapshot::new(Some(serde_json::json!({"name": "Alice"})), "".to_string());
        let snap2 = Snapshot::new(None, "".to_string());
        let snap3 = Snapshot::new(Some(JsonValue::Null), "".to_string());

        assert!(snap1.exists());
        assert!(!snap2.exists());
        assert!(!snap3.exists());
    }

    #[test]
    fn test_snapshot_child() {
        let snap = Snapshot::new(
            Some(serde_json::json!({
                "users": {
                    "abc": {"name": "Alice"}
                }
            })),
            "".to_string(),
        );

        let child = snap.child("users").child("abc").child("name");
        assert_eq!(child.val().unwrap(), JsonValue::String("Alice".to_string()));
    }

    #[test]
    fn test_snapshot_has_child() {
        let snap = Snapshot::new(
            Some(serde_json::json!({
                "name": "Alice",
                "age": 30
            })),
            "".to_string(),
        );

        assert!(snap.has_child("name"));
        assert!(snap.has_child("age"));
        assert!(!snap.has_child("email"));
    }

    #[test]
    fn test_snapshot_has_children() {
        let snap = Snapshot::new(
            Some(serde_json::json!({
                "name": "Alice",
                "age": 30
            })),
            "".to_string(),
        );

        assert!(snap.has_children(&["name".to_string(), "age".to_string()]));
        assert!(!snap.has_children(&["name".to_string(), "email".to_string()]));
    }

    #[test]
    fn test_snapshot_type_checks() {
        let str_snap = Snapshot::new(Some(JsonValue::String("hello".to_string())), "".to_string());
        let num_snap = Snapshot::new(Some(serde_json::json!(42)), "".to_string());
        let bool_snap = Snapshot::new(Some(JsonValue::Bool(true)), "".to_string());

        assert!(str_snap.is_string());
        assert!(!str_snap.is_number());

        assert!(num_snap.is_number());
        assert!(!num_snap.is_string());

        assert!(bool_snap.is_boolean());
        assert!(!bool_snap.is_string());
    }

    #[test]
    fn test_estimate_size() {
        let small = serde_json::json!({"name": "Alice"});
        assert!(estimate_size(&small, 0, MAX_VAL_SIZE) < MAX_VAL_SIZE);

        // Create a large value
        let large_string = "x".repeat(MAX_VAL_SIZE + 1000);
        let large = JsonValue::String(large_string);
        assert!(estimate_size(&large, 0, MAX_VAL_SIZE) > MAX_VAL_SIZE);
    }

    #[test]
    fn test_auth_info_to_json() {
        let auth = AuthInfo::new(
            Some("user123".to_string()),
            Some("password".to_string()),
            None,
            false,
        );

        let json = auth.to_json().unwrap();
        assert_eq!(json.get("uid").unwrap(), "user123");
        assert_eq!(json.get("provider").unwrap(), "password");
    }

    #[test]
    fn test_auth_info_null_when_empty() {
        let auth = AuthInfo::default();
        assert!(auth.to_json().is_none());
    }

    #[test]
    fn test_auth_info_token_hoisting() {
        // Token claims should be available at both auth.X and auth.token.X
        let mut token = serde_json::Map::new();
        token.insert(
            "player_id".to_string(),
            JsonValue::String("-abc123".to_string()),
        );
        token.insert("is_gm".to_string(), JsonValue::Bool(true));

        let auth = AuthInfo::new(
            Some("user123".to_string()),
            Some("custom".to_string()),
            Some(token),
            false,
        );

        let json = auth.to_json().unwrap();

        // Standard fields
        assert_eq!(json.get("uid").unwrap(), "user123");
        assert_eq!(json.get("provider").unwrap(), "custom");

        // Hoisted token claims at top level (auth.player_id)
        assert_eq!(json.get("player_id").unwrap(), "-abc123");
        assert_eq!(json.get("is_gm").unwrap(), true);

        // Also available at auth.token.X
        let token_obj = json.get("token").unwrap().as_object().unwrap();
        assert_eq!(token_obj.get("player_id").unwrap(), "-abc123");
        assert_eq!(token_obj.get("is_gm").unwrap(), true);
    }

    #[test]
    fn test_lazy_snapshot_without_sentinel_never_needs_promotion() {
        // Without any sentinels, all paths should be accessible
        let mut test_tree = TestTree::new();
        test_tree.set("users/-abc123/name", serde_json::json!("Alice"));

        let tree: Arc<dyn TreeGetter> = Arc::new(test_tree);
        let snap = LazySnapshot::new(tree, "users/-abc123/name".to_string());

        // val() should work fine
        let result = <LazySnapshot as SnapshotTrait>::val(&snap);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some(serde_json::json!("Alice")));
    }

    // =========================================================================
    // LazyUpdateSnapshot tests
    // =========================================================================

    /// TreeGetter that simulates blob-backed mode: paths in `unloaded`
    /// trigger NeedsPromotion via `node_is_loaded` returning false.
    struct LazyTestTree {
        loaded: HashMap<String, JsonValue>,
        unloaded: std::collections::HashSet<String>,
        blob_backed: bool,
    }

    impl LazyTestTree {
        fn new() -> Self {
            Self {
                loaded: HashMap::new(),
                unloaded: std::collections::HashSet::new(),
                blob_backed: true,
            }
        }
        #[allow(dead_code)] // mock parity with the loaded-tree helper above
        fn set(&mut self, path: &str, value: JsonValue) {
            self.loaded.insert(path.to_string(), value);
        }
        fn mark_unloaded(&mut self, path: &str) {
            self.unloaded.insert(path.to_string());
        }
    }

    impl TreeGetter for LazyTestTree {
        fn get_value(&self, path: &str) -> Option<JsonValue> {
            self.loaded.get(path).cloned()
        }
        fn get_node_value(&self, path: &str) -> Option<JsonValue> {
            self.loaded.get(path).cloned()
        }
        fn node_exists(&self, path: &str) -> bool {
            self.loaded.contains_key(path)
        }
        fn node_has_child(&self, path: &str, child_name: &str) -> bool {
            let child_path = if path.is_empty() {
                format!("/{}", child_name)
            } else {
                format!("{}/{}", path, child_name)
            };
            self.loaded.contains_key(&child_path)
        }
        fn node_is_loaded(&self, path: &str) -> bool {
            !self.unloaded.contains(path)
        }
        fn is_blob_backed(&self) -> bool {
            self.blob_backed
        }
    }

    fn updates_arc(items: &[(&str, JsonValue)]) -> Arc<serde_json::Map<String, JsonValue>> {
        let mut map = serde_json::Map::new();
        for (k, v) in items {
            map.insert((*k).to_string(), v.clone());
        }
        Arc::new(map)
    }

    #[test]
    fn lazy_update_at_leaf_returns_update_value() {
        // base="/", updates={"foo": 5}. view_path="/foo" lands AtUpdateLeaf.
        let tree: Arc<dyn TreeGetter> = Arc::new(TestTree::new());
        let updates = updates_arc(&[("foo", serde_json::json!(5))]);
        let snap =
            LazyUpdateSnapshot::with_view_path(tree, "/".to_string(), updates, "/foo".to_string());

        assert_eq!(snap.val().unwrap(), Some(serde_json::json!(5)));
        assert!(snap.exists().unwrap());
        assert!(snap.is_number().unwrap());
    }

    #[test]
    fn lazy_update_at_leaf_container_returns_value() {
        // val() on an AtUpdateLeaf container returns the actual Object —
        // the value is in-memory and bounded by payload, so we don't need
        // the lazy-tree marker. This is what makes `newData.hasChildren()`
        // (no-arg) work on update values.
        let tree: Arc<dyn TreeGetter> = Arc::new(TestTree::new());
        let updates = updates_arc(&[("core", serde_json::json!({"level": 30}))]);
        let snap = LazyUpdateSnapshot::with_view_path(
            tree,
            "/characters/abc".to_string(),
            updates,
            "/characters/abc/core".to_string(),
        );

        assert_eq!(snap.val().unwrap(), Some(serde_json::json!({"level": 30})));
        assert!(snap.exists().unwrap());
        assert!(snap.has_child("level").unwrap());
        assert!(!snap.has_child("missing").unwrap());
    }

    #[test]
    fn lazy_update_inside_update_value() {
        // base="/", updates={"core": {"level": 30, "zone_id": "g"}}.
        // view_path="/core/level" descends into the update value.
        let tree: Arc<dyn TreeGetter> = Arc::new(TestTree::new());
        let updates = updates_arc(&[(
            "core",
            serde_json::json!({"level": 30, "zone_id": "greenhollow"}),
        )]);
        let snap = LazyUpdateSnapshot::with_view_path(
            tree,
            "/".to_string(),
            updates,
            "/core/level".to_string(),
        );

        assert_eq!(snap.val().unwrap(), Some(serde_json::json!(30)));
        assert!(snap.exists().unwrap());
        assert!(snap.is_number().unwrap());
        assert!(!snap.is_string().unwrap());
    }

    #[test]
    fn lazy_update_multi_path_keys() {
        // Production shape: base="/", updates with multi-path keys
        // "characters/abc/core" -> {full core}.
        let tree: Arc<dyn TreeGetter> = Arc::new(TestTree::new());
        let updates = updates_arc(&[(
            "characters/abc/core",
            serde_json::json!({"level": 30, "zone_id": "greenhollow"}),
        )]);

        // At the deep path: container, returns the actual value (in-memory,
        // bounded by payload), and reports its children.
        let at_core = LazyUpdateSnapshot::with_view_path(
            Arc::clone(&tree),
            "/".to_string(),
            Arc::clone(&updates),
            "/characters/abc/core".to_string(),
        );
        assert_eq!(
            at_core.val().unwrap(),
            Some(serde_json::json!({"level": 30, "zone_id": "greenhollow"}))
        );
        assert!(at_core.has_child("level").unwrap());
        assert!(at_core.has_child("zone_id").unwrap());

        // Inside the update value: leaf primitive.
        let at_level = LazyUpdateSnapshot::with_view_path(
            Arc::clone(&tree),
            "/".to_string(),
            Arc::clone(&updates),
            "/characters/abc/core/level".to_string(),
        );
        assert_eq!(at_level.val().unwrap(), Some(serde_json::json!(30)));
    }

    #[test]
    fn lazy_update_overlay_root_has_child_from_updates() {
        // base="/", updates={"characters/abc/core": ...}. view_path="/" is
        // overlay; should have child "characters" (from update key).
        let tree: Arc<dyn TreeGetter> = Arc::new(TestTree::new());
        let updates = updates_arc(&[("characters/abc/core", serde_json::json!({"level": 30}))]);
        let snap =
            LazyUpdateSnapshot::with_view_path(tree, "/".to_string(), updates, "".to_string());

        assert!(snap.has_child("characters").unwrap());
        assert!(!snap.has_child("missing").unwrap());
    }

    #[test]
    fn lazy_update_overlay_descends_into_update_path() {
        // From the root snapshot, navigate down into an update path via child().
        let tree: Arc<dyn TreeGetter> = Arc::new(TestTree::new());
        let updates = updates_arc(&[("characters/abc/core", serde_json::json!({"level": 30}))]);
        let root =
            LazyUpdateSnapshot::with_view_path(tree, "/".to_string(), updates, "".to_string());
        let level = root
            .child("characters")
            .child("abc")
            .child("core")
            .child("level");
        assert_eq!(level.val().unwrap(), Some(serde_json::json!(30)));
    }

    #[test]
    fn lazy_update_outside_update_defers_to_tree() {
        // Untouched-by-update sibling: defer to tree.
        let mut tree = TestTree::new();
        tree.set("/sibling", serde_json::json!("untouched"));
        let tree: Arc<dyn TreeGetter> = Arc::new(tree);
        let updates = updates_arc(&[("foo", serde_json::json!(5))]);

        let snap = LazyUpdateSnapshot::with_view_path(
            tree,
            "/".to_string(),
            updates,
            "/sibling".to_string(),
        );

        // base="/", updates only at "/foo". "/sibling" is in the overlay
        // region (root contains it) — has_child for "sibling" goes to tree;
        // val() at /sibling returns the tree value.
        assert_eq!(snap.val().unwrap(), Some(serde_json::json!("untouched")));
    }

    #[test]
    fn lazy_update_delete_via_null() {
        // updates={"foo": null} represents a delete of /foo.
        let tree: Arc<dyn TreeGetter> = Arc::new(TestTree::new());
        let updates = updates_arc(&[("foo", JsonValue::Null)]);
        let snap = LazyUpdateSnapshot::with_view_path(
            Arc::clone(&tree),
            "/".to_string(),
            Arc::clone(&updates),
            "/foo".to_string(),
        );

        assert!(!snap.exists().unwrap());
        assert_eq!(snap.val().unwrap(), None);

        // And the parent should NOT see "foo" as a child.
        let root =
            LazyUpdateSnapshot::with_view_path(tree, "/".to_string(), updates, "".to_string());
        assert!(!root.has_child("foo").unwrap());
    }

    #[test]
    fn lazy_update_blob_backed_unloaded_path_returns_needs_promotion() {
        // Reading a tree-only path in a blob-backed tree where data isn't
        // loaded should bubble up NeedsPromotion.
        let mut tree = LazyTestTree::new();
        tree.mark_unloaded("/sibling");
        let tree: Arc<dyn TreeGetter> = Arc::new(tree);

        let updates = updates_arc(&[("foo", serde_json::json!(5))]);
        let snap = LazyUpdateSnapshot::with_view_path(
            tree,
            "/".to_string(),
            updates,
            "/sibling".to_string(),
        );

        let err = snap.val().unwrap_err();
        assert_eq!(err.path, "/sibling");
    }

    #[test]
    fn lazy_update_blob_backed_inside_update_no_promotion() {
        // Inside the update region, no tree access happens — should never
        // trigger NeedsPromotion even with everything unloaded.
        let mut tree = LazyTestTree::new();
        tree.mark_unloaded("");
        tree.mark_unloaded("/characters");
        tree.mark_unloaded("/characters/abc");
        tree.mark_unloaded("/characters/abc/core");
        let tree: Arc<dyn TreeGetter> = Arc::new(tree);

        let updates = updates_arc(&[("characters/abc/core", serde_json::json!({"level": 30}))]);
        let snap = LazyUpdateSnapshot::with_view_path(
            tree,
            "/".to_string(),
            updates,
            "/characters/abc/core/level".to_string(),
        );

        // Reading a value entirely inside the update should succeed without
        // any promotion errors.
        assert_eq!(snap.val().unwrap(), Some(serde_json::json!(30)));
        assert!(snap.exists().unwrap());
    }

    #[test]
    fn lazy_update_parent_navigation() {
        let tree: Arc<dyn TreeGetter> = Arc::new(TestTree::new());
        let updates = updates_arc(&[("foo/bar", serde_json::json!(5))]);
        let snap = LazyUpdateSnapshot::with_view_path(
            tree,
            "/".to_string(),
            updates,
            "/foo/bar".to_string(),
        );

        // parent() walks up one segment.
        let parent = snap.parent();
        // The parent's val() should be the container marker.
        assert_eq!(
            parent.val().unwrap(),
            Some(JsonValue::String(OBJECT_SENTINEL_MARKER.to_string()))
        );
    }

    #[test]
    fn lazy_update_has_children_aggregates() {
        let tree: Arc<dyn TreeGetter> = Arc::new(TestTree::new());
        let updates = updates_arc(&[("core", serde_json::json!({"level": 30, "zone_id": "g"}))]);
        let snap =
            LazyUpdateSnapshot::with_view_path(tree, "/".to_string(), updates, "/core".to_string());

        assert!(
            snap.has_children(&["level".to_string(), "zone_id".to_string()])
                .unwrap()
        );
        assert!(
            !snap
                .has_children(&["level".to_string(), "missing".to_string()])
                .unwrap()
        );
    }

    #[test]
    fn lazy_update_overlay_exists_when_updates_create_path() {
        // /characters doesn't exist in tree, but updates writes
        // /characters/abc/core. exists() at /characters should be true.
        let tree: Arc<dyn TreeGetter> = Arc::new(TestTree::new());
        let updates = updates_arc(&[("characters/abc/core", serde_json::json!({"level": 30}))]);
        let snap = LazyUpdateSnapshot::with_view_path(
            tree,
            "/".to_string(),
            updates,
            "/characters".to_string(),
        );

        assert!(snap.exists().unwrap());
    }

    #[test]
    fn lazy_update_normalize_path() {
        assert_eq!(normalize_path(""), "");
        assert_eq!(normalize_path("/"), "");
        assert_eq!(normalize_path("a/b"), "/a/b");
        assert_eq!(normalize_path("/a/b"), "/a/b");
        assert_eq!(normalize_path("/a/b/"), "/a/b");
    }

    #[test]
    fn lazy_update_descend_into() {
        let value = serde_json::json!({"a": {"b": {"c": 42}}});
        assert_eq!(descend_into(&value, ""), Some(&value));
        assert_eq!(descend_into(&value, "a/b/c"), Some(&serde_json::json!(42)));
        assert_eq!(descend_into(&value, "a/missing"), None);
        // Walking through a primitive returns None.
        assert_eq!(descend_into(&serde_json::json!(5), "a"), None);
    }

    // -------------------------------------------------------------
    // NewData::writes_at tests
    // -------------------------------------------------------------

    fn writes_sorted(mut writes: Vec<(String, JsonValue)>) -> Vec<(String, JsonValue)> {
        writes.sort_by(|a, b| a.0.cmp(&b.0));
        writes
    }

    #[test]
    fn writes_at_set_at_path() {
        // SET at /orders/o1 with value V; view = /orders/o1 → V's children.
        let nd = NewData::from_set(
            "/orders/o1".to_string(),
            serde_json::json!({"items": {"a": 1}, "total": 99}),
        );
        let writes = writes_sorted(nd.writes_at("/orders/o1"));
        assert_eq!(
            writes,
            vec![
                ("items".to_string(), serde_json::json!({"a": 1})),
                ("total".to_string(), serde_json::json!(99)),
            ]
        );
    }

    #[test]
    fn writes_at_set_view_above() {
        // SET at /a/b/c with value V; view = /a → child "b" with {c: V}.
        let nd = NewData::from_set("/a/b/c".to_string(), serde_json::json!(42));
        let writes = writes_sorted(nd.writes_at("/a"));
        assert_eq!(
            writes,
            vec![("b".to_string(), serde_json::json!({"c": 42}))]
        );
    }

    #[test]
    fn writes_at_set_view_below() {
        // SET at /a with V = {b: {c: 7}}; view = /a/b → descend, yields c=7.
        let nd = NewData::from_set("/a".to_string(), serde_json::json!({"b": {"c": 7, "d": 8}}));
        let writes = writes_sorted(nd.writes_at("/a/b"));
        assert_eq!(
            writes,
            vec![
                ("c".to_string(), serde_json::json!(7)),
                ("d".to_string(), serde_json::json!(8)),
            ]
        );
    }

    #[test]
    fn writes_at_update_only_touched_children() {
        // UPDATE at /a/b with {x: 5, y: 7}; view = /a/b → only x and y,
        // not whatever else might exist in the tree at /a/b.
        let nd = NewData::from_update(
            "/a/b".to_string(),
            serde_json::Map::from_iter([
                ("x".to_string(), serde_json::json!(5)),
                ("y".to_string(), serde_json::json!(7)),
            ]),
        );
        let writes = writes_sorted(nd.writes_at("/a/b"));
        assert_eq!(
            writes,
            vec![
                ("x".to_string(), serde_json::json!(5)),
                ("y".to_string(), serde_json::json!(7)),
            ]
        );
    }

    #[test]
    fn writes_at_update_multipath_groups_under_same_child() {
        // UPDATE at "" with multi-path keys that share a first segment.
        // view = "/" → child "a" partial = {b: 1, c: 2}.
        let nd = NewData::from_update(
            String::new(),
            serde_json::Map::from_iter([
                ("a/b".to_string(), serde_json::json!(1)),
                ("a/c".to_string(), serde_json::json!(2)),
            ]),
        );
        let writes = writes_sorted(nd.writes_at(""));
        assert_eq!(
            writes,
            vec![("a".to_string(), serde_json::json!({"b": 1, "c": 2}))]
        );
    }

    #[test]
    fn writes_at_update_view_above_base() {
        // UPDATE at /a/b with {x: 5}; view = /a → child "b" partial = {x: 5}.
        let nd = NewData::from_update(
            "/a/b".to_string(),
            serde_json::Map::from_iter([("x".to_string(), serde_json::json!(5))]),
        );
        let writes = writes_sorted(nd.writes_at("/a"));
        assert_eq!(writes, vec![("b".to_string(), serde_json::json!({"x": 5}))]);
    }

    #[test]
    fn writes_at_update_view_below_returns_inside_partial() {
        // UPDATE at "" with {"a/b/c": V}; view = /a/b → child "c" with V.
        let nd = NewData::from_update(
            String::new(),
            serde_json::Map::from_iter([("a/b/c".to_string(), serde_json::json!({"k": 1}))]),
        );
        let writes = writes_sorted(nd.writes_at("/a/b"));
        assert_eq!(writes, vec![("c".to_string(), serde_json::json!({"k": 1}))]);
    }

    #[test]
    fn writes_at_no_overlap_yields_nothing() {
        // UPDATE at /a/b; view = /unrelated → no children.
        let nd = NewData::from_update(
            "/a/b".to_string(),
            serde_json::Map::from_iter([("x".to_string(), serde_json::json!(5))]),
        );
        assert!(nd.writes_at("/unrelated").is_empty());
    }

    #[test]
    fn writes_at_helpers() {
        assert_eq!(
            wrap_in_path(serde_json::json!(7), &["a", "b"]),
            serde_json::json!({"a": {"b": 7}})
        );
        assert_eq!(
            wrap_in_path(serde_json::json!(7), &[]),
            serde_json::json!(7)
        );

        assert_eq!(
            json_merge(
                serde_json::json!({"a": 1, "shared": 1}),
                serde_json::json!({"b": 2, "shared": 99})
            ),
            serde_json::json!({"a": 1, "b": 2, "shared": 99})
        );
        // Non-Object on the right replaces.
        assert_eq!(
            json_merge(serde_json::json!({"a": 1}), serde_json::json!("primitive")),
            serde_json::json!("primitive")
        );
    }

    #[test]
    fn lazy_update_path_relations() {
        assert!(is_path_at_or_above("", "/a"));
        assert!(is_path_at_or_above("/a", "/a"));
        assert!(is_path_at_or_above("/a", "/a/b"));
        assert!(!is_path_at_or_above("/a", "/b"));
        assert!(!is_path_at_or_above("/a", "/ab"));
    }
}
