//! ArcValue: An immutable, reference-counted JSON value with copy-on-write semantics.
//!
//! Canonical source for both lark-blob and lark-server. lark-server should depend
//! on this crate and use `lark_blob::ArcValue` instead of maintaining a copy.

use serde::ser::{Error as SerError, SerializeMap, SerializeSeq};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Map, Number, Value};
use std::collections::HashMap;
use std::sync::Arc;

/// An immutable, reference-counted JSON value with copy-on-write semantics.
///
/// Primitives (Null, Bool, Number) are stored inline with no Arc overhead.
/// Strings, Arrays, and Objects are wrapped in Arc for O(1) cloning.
///
/// `Sentinel` is an in-memory-only marker meaning "this node exists in blob storage
/// but its value hasn't been loaded into memory." It is never serialized to blob or JSON.
/// Used by lark-server's Tree for lazy loading: absent → sentinel → real value → (evict) → sentinel.
#[derive(Clone, Debug, Default)]
pub enum ArcValue {
    #[default]
    Null,
    Bool(bool),
    Number(Number),
    String(Arc<str>),
    Object(Arc<HashMap<String, ArcValue>>),
    /// Marker: node exists in blob storage but full value hasn't been loaded.
    /// May contain children from in-memory writes that pass through this node.
    /// On promotion, blob data is loaded and merged with existing children,
    /// then this becomes an Object.
    /// `exists()` returns true. Never serialized — panics if attempted.
    Sentinel(Arc<HashMap<String, ArcValue>>),
}

impl ArcValue {
    pub fn from_value(value: Value) -> Self {
        match value {
            Value::Null => ArcValue::Null,
            Value::Bool(b) => ArcValue::Bool(b),
            Value::Number(n) => ArcValue::Number(n),
            Value::String(s) => ArcValue::String(Arc::from(s)),
            // Arrays are stored as integer-keyed maps, keyed by element index;
            // null elements are dropped, leaving their index as a gap.
            Value::Array(arr) => {
                let converted: HashMap<String, ArcValue> = arr
                    .into_iter()
                    .enumerate()
                    .filter(|(_, v)| !v.is_null())
                    .map(|(i, v)| (i.to_string(), ArcValue::from_value(v)))
                    .collect();
                ArcValue::Object(Arc::new(converted))
            }
            Value::Object(map) => {
                let converted: HashMap<String, ArcValue> = map
                    .into_iter()
                    .map(|(k, v)| (k, ArcValue::from_value(v)))
                    .collect();
                ArcValue::Object(Arc::new(converted))
            }
        }
    }

    pub fn to_value(&self) -> Value {
        match self {
            ArcValue::Null | ArcValue::Sentinel(_) => Value::Null, // Sentinel: caller must promote first
            ArcValue::Bool(b) => Value::Bool(*b),
            ArcValue::Number(n) => Value::Number(n.clone()),
            ArcValue::String(s) => Value::String(s.to_string()),
            ArcValue::Object(map) => object_to_value(map),
        }
    }

    pub fn is_sentinel(&self) -> bool {
        matches!(self, ArcValue::Sentinel(_))
    }

    /// Returns true if this value is a Sentinel OR contains any Sentinel descendants.
    /// Used by subscribe/once to determine if a full subtree promotion is needed.
    pub fn contains_sentinel(&self) -> bool {
        match self {
            ArcValue::Sentinel(_) => true,
            ArcValue::Object(map) => map.values().any(|v| v.contains_sentinel()),
            _ => false,
        }
    }

    /// Diagnostic helper: walk this value and return the relative path of the
    /// first Sentinel found (e.g. "/characters/abc/core"), or `None` if there
    /// are no Sentinels. Used by the encoding-error log path to identify which
    /// node leaked a Sentinel into a serialized response.
    pub fn find_first_sentinel_path(&self) -> Option<String> {
        fn walk(value: &ArcValue, path: &mut String) -> bool {
            match value {
                ArcValue::Sentinel(_) => true,
                ArcValue::Object(map) => {
                    for (k, v) in map.iter() {
                        let len = path.len();
                        path.push('/');
                        path.push_str(k);
                        if walk(v, path) {
                            return true;
                        }
                        path.truncate(len);
                    }
                    false
                }
                _ => false,
            }
        }

        let mut path = String::new();
        if walk(self, &mut path) {
            if path.is_empty() {
                Some("/".to_string())
            } else {
                Some(path)
            }
        } else {
            None
        }
    }

    pub fn empty_object() -> Self {
        ArcValue::Object(Arc::new(HashMap::new()))
    }

    pub fn empty_sentinel() -> Self {
        ArcValue::Sentinel(Arc::new(HashMap::new()))
    }

    /// Get the children map if this is an Object or Sentinel.
    /// Used for navigation that should work through both types.
    pub fn children_map(&self) -> Option<&HashMap<String, ArcValue>> {
        match self {
            ArcValue::Object(map) | ArcValue::Sentinel(map) => Some(map.as_ref()),
            _ => None,
        }
    }

    pub fn get(&self, key: &str) -> Option<&ArcValue> {
        match self {
            ArcValue::Object(map) | ArcValue::Sentinel(map) => map.get(key),
            _ => None,
        }
    }

    pub fn get_path(&self, path: &[&str]) -> Option<&ArcValue> {
        let mut current = self;
        for segment in path {
            current = current.get(segment)?;
        }
        Some(current)
    }

    pub fn is_null(&self) -> bool {
        matches!(self, ArcValue::Null)
    }

    pub fn is_object(&self) -> bool {
        matches!(self, ArcValue::Object(_))
    }

    pub fn is_primitive(&self) -> bool {
        matches!(
            self,
            ArcValue::Null | ArcValue::Bool(_) | ArcValue::Number(_) | ArcValue::String(_)
        )
    }

    pub fn is_empty_container(&self) -> bool {
        match self {
            ArcValue::Object(map) => map.is_empty(),
            ArcValue::Null => true,
            // Sentinel is never "empty" — it represents data in blob
            _ => false,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            ArcValue::Object(map) => map.len(),
            _ => 0, // Sentinel returns 0 — caller must promote first
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn keys(&self) -> impl Iterator<Item = &str> {
        match self {
            ArcValue::Object(map) => ObjectKeysIter::Some(map.keys()),
            _ => ObjectKeysIter::None, // Sentinel returns empty — caller must promote first
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &ArcValue)> {
        match self {
            ArcValue::Object(map) => ObjectIter::Some(map.iter()),
            _ => ObjectIter::None, // Sentinel returns empty — caller must promote first
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            ArcValue::Bool(b) => Some(*b),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            ArcValue::Number(n) => n.as_i64(),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            ArcValue::Number(n) => n.as_f64(),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            ArcValue::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_object(&self) -> Option<&HashMap<String, ArcValue>> {
        match self {
            ArcValue::Object(map) => Some(map),
            _ => None,
        }
    }

    pub fn ptr_eq(&self, other: &ArcValue) -> bool {
        match (self, other) {
            (ArcValue::Null, ArcValue::Null) => true,
            (ArcValue::Bool(a), ArcValue::Bool(b)) => a == b,
            (ArcValue::Number(a), ArcValue::Number(b)) => a == b,
            (ArcValue::String(a), ArcValue::String(b)) => Arc::ptr_eq(a, b),
            (ArcValue::Object(a), ArcValue::Object(b)) => Arc::ptr_eq(a, b),
            (ArcValue::Sentinel(a), ArcValue::Sentinel(b)) => Arc::ptr_eq(a, b),
            _ => false,
        }
    }

    pub fn get_path_owned(&self, path: &[String]) -> Option<&ArcValue> {
        let mut current = self;
        for segment in path {
            current = current.get(segment)?;
        }
        Some(current)
    }

    pub fn set_path(&self, path: &[&str], value: ArcValue) -> ArcValue {
        if path.is_empty() {
            return value;
        }

        let key = path[0];
        let rest = &path[1..];

        match self {
            ArcValue::Object(map) | ArcValue::Sentinel(map) => {
                let mut new_map = (**map).clone();

                if rest.is_empty() {
                    new_map.insert(key.to_string(), value);
                } else {
                    let child = map.get(key).cloned().unwrap_or_else(ArcValue::empty_object);
                    let new_child = child.set_path(rest, value);
                    new_map.insert(key.to_string(), new_child);
                }

                // Preserve the variant
                if self.is_sentinel() {
                    ArcValue::Sentinel(Arc::new(new_map))
                } else {
                    ArcValue::Object(Arc::new(new_map))
                }
            }
            _ => {
                let mut new_map = HashMap::new();

                if rest.is_empty() {
                    new_map.insert(key.to_string(), value);
                } else {
                    let child = ArcValue::empty_object().set_path(rest, value);
                    new_map.insert(key.to_string(), child);
                }

                ArcValue::Object(Arc::new(new_map))
            }
        }
    }

    pub fn set_path_owned(&self, path: &[String], value: ArcValue) -> ArcValue {
        let refs: Vec<&str> = path.iter().map(|s| s.as_str()).collect();
        self.set_path(&refs, value)
    }

    pub fn set_path_mut(&mut self, path: &[&str], value: ArcValue) {
        if path.is_empty() {
            *self = value;
            return;
        }

        let key = path[0];
        let rest = &path[1..];

        match self {
            ArcValue::Object(map) | ArcValue::Sentinel(map) => {
                let map_mut = Arc::make_mut(map);

                if rest.is_empty() {
                    map_mut.insert(key.to_string(), value);
                } else {
                    let child = map_mut
                        .entry(key.to_string())
                        .or_insert_with(ArcValue::empty_object);
                    child.set_path_mut(rest, value);
                }
            }
            _ => {
                let mut new_map = HashMap::new();

                if rest.is_empty() {
                    new_map.insert(key.to_string(), value);
                } else {
                    let mut child = ArcValue::empty_object();
                    child.set_path_mut(rest, value);
                    new_map.insert(key.to_string(), child);
                }

                *self = ArcValue::Object(Arc::new(new_map));
            }
        }
    }

    /// Like set_path_mut, but creates Sentinel intermediates instead of empty Objects.
    /// Used by blob-backed databases: intermediates are "unloaded" nodes that need
    /// promotion before they can be treated as authoritative.
    ///
    /// **WARNING — primitive clobber:** if any node along `path` (including `self`)
    /// is a primitive (Null/Bool/Number/String) or Array, it is silently replaced
    /// with a new `Sentinel` container to hold the write. This is the correct
    /// Firebase SET semantic for client writes (writing `/a/b/c` replaces `/a/b`
    /// if it was a primitive), but is dangerous for internal "we checked" marker
    /// writes — the newly-created Sentinel is not recorded in any tracking set,
    /// so a subsequent read of the primitive's path returns the untracked Sentinel
    /// and fails to serialize. See `Database::promote_path` / `promote_path_deep`
    /// for the call-site guard (only write markers when the parent is an Object).
    pub fn set_path_mut_sentinel(&mut self, path: &[&str], value: ArcValue) {
        if path.is_empty() {
            *self = value;
            return;
        }

        let key = path[0];
        let rest = &path[1..];

        match self {
            ArcValue::Object(map) | ArcValue::Sentinel(map) => {
                let map_mut = Arc::make_mut(map);

                if rest.is_empty() {
                    map_mut.insert(key.to_string(), value);
                } else {
                    let child = map_mut
                        .entry(key.to_string())
                        .or_insert_with(ArcValue::empty_sentinel);
                    child.set_path_mut_sentinel(rest, value);
                }
            }
            _ => {
                let mut new_map = HashMap::new();

                if rest.is_empty() {
                    new_map.insert(key.to_string(), value);
                } else {
                    let mut child = ArcValue::empty_sentinel();
                    child.set_path_mut_sentinel(rest, value);
                    new_map.insert(key.to_string(), child);
                }

                *self = ArcValue::Sentinel(Arc::new(new_map));
            }
        }
    }

    pub fn remove_path(&self, path: &[&str]) -> ArcValue {
        if path.is_empty() {
            return ArcValue::Null;
        }

        let key = path[0];
        let rest = &path[1..];

        match self {
            ArcValue::Object(map) | ArcValue::Sentinel(map) => {
                let mut new_map = (**map).clone();

                if rest.is_empty() {
                    new_map.remove(key);
                } else if let Some(child) = map.get(key) {
                    let new_child = child.remove_path(rest);
                    if !new_child.is_empty_container() {
                        new_map.insert(key.to_string(), new_child);
                    } else {
                        new_map.remove(key);
                    }
                }

                // Preserve the variant (Object stays Object, Sentinel stays Sentinel)
                if self.is_sentinel() {
                    ArcValue::Sentinel(Arc::new(new_map))
                } else {
                    ArcValue::Object(Arc::new(new_map))
                }
            }
            _ => self.clone(),
        }
    }

    pub fn remove_path_mut(&mut self, path: &[&str]) {
        if path.is_empty() {
            *self = ArcValue::Null;
            return;
        }

        let key = path[0];
        let rest = &path[1..];

        match self {
            ArcValue::Object(map) | ArcValue::Sentinel(map) => {
                let map_mut = Arc::make_mut(map);

                if rest.is_empty() {
                    map_mut.remove(key);
                } else if let Some(child) = map_mut.get_mut(key) {
                    child.remove_path_mut(rest);
                    if child.is_empty_container() {
                        map_mut.remove(key);
                    }
                }
            }
            _ => {}
        }
    }

    pub fn update_at_path(&self, path: &[&str], updates: &HashMap<String, ArcValue>) -> ArcValue {
        if path.is_empty() {
            match self {
                ArcValue::Object(map) | ArcValue::Sentinel(map) => {
                    let mut new_map = (**map).clone();
                    for (key, value) in updates {
                        if value.is_null() {
                            new_map.remove(key);
                        } else {
                            new_map.insert(key.clone(), value.clone());
                        }
                    }
                    // Preserve the variant
                    if self.is_sentinel() {
                        ArcValue::Sentinel(Arc::new(new_map))
                    } else {
                        ArcValue::Object(Arc::new(new_map))
                    }
                }
                _ => {
                    let mut new_map = HashMap::new();
                    for (key, value) in updates {
                        if !value.is_null() {
                            new_map.insert(key.clone(), value.clone());
                        }
                    }
                    ArcValue::Object(Arc::new(new_map))
                }
            }
        } else {
            let key = path[0];
            let rest = &path[1..];

            match self {
                ArcValue::Object(map) | ArcValue::Sentinel(map) => {
                    let mut new_map = (**map).clone();
                    let child = map.get(key).cloned().unwrap_or_else(ArcValue::empty_object);
                    let new_child = child.update_at_path(rest, updates);
                    new_map.insert(key.to_string(), new_child);
                    // Preserve the variant
                    if self.is_sentinel() {
                        ArcValue::Sentinel(Arc::new(new_map))
                    } else {
                        ArcValue::Object(Arc::new(new_map))
                    }
                }
                _ => {
                    let mut new_map = HashMap::new();
                    let child = ArcValue::empty_object().update_at_path(rest, updates);
                    new_map.insert(key.to_string(), child);
                    ArcValue::Object(Arc::new(new_map))
                }
            }
        }
    }

    pub fn exists(&self) -> bool {
        match self {
            ArcValue::Null => false,
            ArcValue::Object(map) => !map.is_empty(),
            ArcValue::Sentinel(_) => false, // Caller must promote before checking existence
            _ => true,
        }
    }

    pub fn clean(self) -> Option<ArcValue> {
        match self {
            ArcValue::Null => None,
            ArcValue::Object(map) => {
                if map.is_empty() {
                    return None;
                }
                let needs_cleaning = map.values().any(|v| v.is_null() || v.is_empty_container());
                if !needs_cleaning {
                    return Some(ArcValue::Object(map));
                }
                let cleaned: HashMap<String, ArcValue> = map
                    .iter()
                    .filter_map(|(k, v)| v.clone().clean().map(|cv| (k.clone(), cv)))
                    .collect();
                if cleaned.is_empty() {
                    None
                } else {
                    Some(ArcValue::Object(Arc::new(cleaned)))
                }
            }
            ArcValue::Sentinel(_) => Some(self), // Sentinel is never cleaned away
            other => Some(other),
        }
    }

    pub fn from_value_cleaned(value: Value) -> Option<ArcValue> {
        match value {
            Value::Null => None,
            Value::Bool(b) => Some(ArcValue::Bool(b)),
            Value::Number(n) => Some(ArcValue::Number(n)),
            Value::String(s) => Some(ArcValue::String(Arc::from(s))),
            // Stored as an integer-keyed map; null/empty elements are dropped and
            // their indices become gaps.
            Value::Array(arr) => {
                let cleaned: HashMap<String, ArcValue> = arr
                    .into_iter()
                    .enumerate()
                    .filter_map(|(i, v)| {
                        ArcValue::from_value_cleaned(v).map(|cv| (i.to_string(), cv))
                    })
                    .collect();
                if cleaned.is_empty() {
                    None
                } else {
                    Some(ArcValue::Object(Arc::new(cleaned)))
                }
            }
            Value::Object(map) => {
                if map.is_empty() {
                    return None;
                }
                let cleaned: HashMap<String, ArcValue> = map
                    .into_iter()
                    .filter_map(|(k, v)| ArcValue::from_value_cleaned(v).map(|cv| (k, cv)))
                    .collect();
                if cleaned.is_empty() {
                    None
                } else {
                    Some(ArcValue::Object(Arc::new(cleaned)))
                }
            }
        }
    }

    pub fn estimate_size(&self) -> i64 {
        match self {
            ArcValue::Null | ArcValue::Sentinel(_) => 4,
            ArcValue::Bool(_) => 5,
            ArcValue::Number(_) => 12,
            ArcValue::String(s) => s.len() as i64 + 2,
            ArcValue::Object(map) => {
                let mut size: i64 = 2;
                let mut first = true;
                for (k, child) in map.iter() {
                    if !first {
                        size += 1;
                    }
                    first = false;
                    size += k.len() as i64 + 3;
                    size += child.estimate_size();
                }
                size
            }
        }
    }

    pub fn get_nested_primitive(&self, path: &str) -> Option<Value> {
        let mut current = self;
        for segment in path.split('/').filter(|s| !s.is_empty()) {
            current = current.get(segment)?;
        }
        Some(current.to_value())
    }
}

enum ObjectKeysIter<'a> {
    Some(std::collections::hash_map::Keys<'a, String, ArcValue>),
    None,
}

impl<'a> Iterator for ObjectKeysIter<'a> {
    type Item = &'a str;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            ObjectKeysIter::Some(iter) => iter.next().map(|s| s.as_str()),
            ObjectKeysIter::None => None,
        }
    }
}

enum ObjectIter<'a> {
    Some(std::collections::hash_map::Iter<'a, String, ArcValue>),
    None,
}

impl<'a> Iterator for ObjectIter<'a> {
    type Item = (&'a str, &'a ArcValue);
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            ObjectIter::Some(iter) => iter.next().map(|(k, v)| (k.as_str(), v)),
            ObjectIter::None => None,
        }
    }
}

impl PartialEq for ArcValue {
    fn eq(&self, other: &Self) -> bool {
        if self.ptr_eq(other) {
            return true;
        }
        match (self, other) {
            (ArcValue::Null, ArcValue::Null) => true,
            (ArcValue::Sentinel(a), ArcValue::Sentinel(b)) => a == b,
            (ArcValue::Bool(a), ArcValue::Bool(b)) => a == b,
            (ArcValue::Number(a), ArcValue::Number(b)) => a == b,
            (ArcValue::String(a), ArcValue::String(b)) => a == b,
            (ArcValue::Object(a), ArcValue::Object(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for ArcValue {}

impl Serialize for ArcValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            ArcValue::Null => serializer.serialize_none(),
            ArcValue::Bool(b) => serializer.serialize_bool(*b),
            ArcValue::Number(n) => n.serialize(serializer),
            ArcValue::String(s) => serializer.serialize_str(s),
            ArcValue::Object(map) => match array_max_index(map) {
                Some(max) => {
                    let len = (max as usize) + 1;
                    let mut slots: Vec<Option<&ArcValue>> = vec![None; len];
                    for (k, v) in map.iter() {
                        let i: usize = k.parse().expect("canonical integer key");
                        slots[i] = Some(v);
                    }
                    let mut seq = serializer.serialize_seq(Some(len))?;
                    for slot in slots {
                        match slot {
                            Some(v) => seq.serialize_element(v)?,
                            None => seq.serialize_element(&Value::Null)?,
                        }
                    }
                    seq.end()
                }
                None => {
                    let mut obj = serializer.serialize_map(Some(map.len()))?;
                    for (k, v) in map.iter() {
                        obj.serialize_entry(k, v)?;
                    }
                    obj.end()
                }
            },
            ArcValue::Sentinel(_) => Err(S::Error::custom(
                "attempted to serialize ArcValue::Sentinel — sentinels are in-memory only",
            )),
        }
    }
}

impl<'de> Deserialize<'de> for ArcValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        Ok(ArcValue::from_value(value))
    }
}

/// Convert a stored object map to JSON, rendering it as an array when it is
/// non-empty, every key is a canonical non-negative integer, and
/// `maxKey < 2 * numKeys`. Otherwise it renders as an object. When rendered as
/// an array, absent indices in `[0, maxKey]` are filled with `null`.
fn object_to_value(map: &HashMap<String, ArcValue>) -> Value {
    match array_max_index(map) {
        Some(max) => {
            let mut arr = vec![Value::Null; (max as usize) + 1];
            for (k, v) in map.iter() {
                // array_max_index guarantees every key parses as an index.
                let i: usize = k.parse().expect("canonical integer key");
                arr[i] = v.to_value();
            }
            Value::Array(arr)
        }
        None => {
            let converted: Map<String, Value> =
                map.iter().map(|(k, v)| (k.clone(), v.to_value())).collect();
            Value::Object(converted)
        }
    }
}

/// Returns `Some(maxKey)` when `map` should render as an array, else `None`.
/// A key is a canonical integer only if it equals the plain decimal form of its
/// value (rejects leading zeros, signs, non-numeric, and empty keys).
fn array_max_index(map: &HashMap<String, ArcValue>) -> Option<u64> {
    if map.is_empty() {
        return None;
    }
    let mut max: u64 = 0;
    for k in map.keys() {
        let n: u64 = k.parse().ok()?;
        if *k != n.to_string() {
            return None;
        }
        max = max.max(n);
    }
    ((max as u128) < 2 * (map.len() as u128)).then_some(max)
}

impl From<Value> for ArcValue {
    fn from(value: Value) -> Self {
        ArcValue::from_value(value)
    }
}

impl From<&Value> for ArcValue {
    fn from(value: &Value) -> Self {
        ArcValue::from_value(value.clone())
    }
}

impl From<ArcValue> for Value {
    fn from(arc_value: ArcValue) -> Self {
        arc_value.to_value()
    }
}

impl From<&ArcValue> for Value {
    fn from(arc_value: &ArcValue) -> Self {
        arc_value.to_value()
    }
}

impl From<bool> for ArcValue {
    fn from(b: bool) -> Self {
        ArcValue::Bool(b)
    }
}

impl From<i64> for ArcValue {
    fn from(n: i64) -> Self {
        ArcValue::Number(Number::from(n))
    }
}

impl From<f64> for ArcValue {
    fn from(n: f64) -> Self {
        Number::from_f64(n).map_or(ArcValue::Null, ArcValue::Number)
    }
}

impl From<&str> for ArcValue {
    fn from(s: &str) -> Self {
        ArcValue::String(Arc::from(s))
    }
}

impl From<String> for ArcValue {
    fn from(s: String) -> Self {
        ArcValue::String(Arc::from(s))
    }
}

impl<T: Into<ArcValue>> From<HashMap<String, T>> for ArcValue {
    fn from(map: HashMap<String, T>) -> Self {
        ArcValue::Object(Arc::new(
            map.into_iter().map(|(k, v)| (k, v.into())).collect(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_from_value_primitives() {
        assert!(matches!(ArcValue::from_value(json!(null)), ArcValue::Null));
        assert!(matches!(
            ArcValue::from_value(json!(true)),
            ArcValue::Bool(true)
        ));
        assert!(matches!(
            ArcValue::from_value(json!(42)),
            ArcValue::Number(_)
        ));
        assert!(matches!(
            ArcValue::from_value(json!("hello")),
            ArcValue::String(_)
        ));
    }

    #[test]
    fn test_from_value_object() {
        let v = ArcValue::from_value(json!({"name": "Riley", "score": 100}));
        assert!(v.is_object());
        assert_eq!(v.get("name").unwrap().as_str(), Some("Riley"));
        assert_eq!(v.get("score").unwrap().as_i64(), Some(100));
    }

    #[test]
    fn test_from_value_array() {
        // Arrays are stored as integer-keyed objects, and render back as arrays.
        let v = ArcValue::from_value(json!([1, 2, 3]));
        assert!(v.is_object());
        assert_eq!(v.get("0").unwrap().as_i64(), Some(1));
        assert_eq!(v.get("2").unwrap().as_i64(), Some(3));
        assert_eq!(v.to_value(), json!([1, 2, 3]));
    }

    #[test]
    fn test_to_value_roundtrip() {
        let original = json!({"users": {"alice": {"score": 100}}, "config": {"enabled": true}});
        let arc = ArcValue::from_value(original.clone());
        let back = arc.to_value();
        assert_eq!(original, back);
    }

    #[test]
    fn test_get_path() {
        let v = ArcValue::from_value(json!({
            "users": {
                "alice": {"score": 100},
                "bob": {"score": 200}
            }
        }));
        assert_eq!(
            v.get_path(&["users", "alice", "score"]).unwrap().as_i64(),
            Some(100)
        );
        assert!(v.get_path(&["users", "charlie"]).is_none());
    }

    #[test]
    fn test_serialize_deserialize() {
        let v = ArcValue::from_value(json!({"name": "test", "values": [1, 2, 3]}));
        let json_str = serde_json::to_string(&v).unwrap();
        let v2: ArcValue = serde_json::from_str(&json_str).unwrap();
        assert_eq!(v, v2);
    }

    // =========================================================================
    // Sentinel tests
    // =========================================================================

    #[test]
    fn test_sentinel_basic_properties() {
        let s = ArcValue::empty_sentinel();

        assert!(s.is_sentinel());
        assert!(!s.is_object());
        assert!(!s.is_null());
        assert!(!s.is_primitive());

        // Sentinel is invisible to reads
        assert!(!s.exists());
        assert_eq!(s.to_value(), Value::Null);
        assert_eq!(s.len(), 0);

        // But NOT an empty container (prevents pruning)
        assert!(!s.is_empty_container());
    }

    #[test]
    fn test_sentinel_with_children_still_invisible() {
        // A Sentinel holding real children is still invisible to exists/len/to_value
        let mut s = ArcValue::empty_sentinel();
        s.set_path_mut_sentinel(&["child"], ArcValue::from_value(json!("hello")));

        assert!(s.is_sentinel());
        assert!(!s.exists());
        assert_eq!(s.to_value(), Value::Null);
        assert_eq!(s.len(), 0);
        assert!(!s.is_empty_container());
    }

    #[test]
    fn test_sentinel_get_navigates_children() {
        // get() should reach children inside a Sentinel
        let mut s = ArcValue::empty_sentinel();
        s.set_path_mut_sentinel(&["greeting"], ArcValue::from_value(json!("hello")));

        let child = s.get("greeting");
        assert!(child.is_some());
        assert_eq!(child.unwrap().to_value(), json!("hello"));
    }

    #[test]
    fn test_sentinel_get_path_navigates_deeply() {
        let mut s = ArcValue::empty_sentinel();
        s.set_path_mut_sentinel(
            &["users", "alice", "score"],
            ArcValue::from_value(json!(100)),
        );

        // Deep navigation works
        let score = s.get_path(&["users", "alice", "score"]);
        assert!(score.is_some());
        assert_eq!(score.unwrap().to_value(), json!(100));

        // Intermediate nodes are Sentinels
        let users = s.get("users").unwrap();
        assert!(users.is_sentinel());

        let alice = s.get_path(&["users", "alice"]).unwrap();
        assert!(alice.is_sentinel());
    }

    #[test]
    fn test_set_path_mut_sentinel_creates_sentinel_intermediates() {
        let mut root = ArcValue::empty_sentinel();
        root.set_path_mut_sentinel(&["a", "b", "c"], ArcValue::from_value(json!(42)));

        // Root is Sentinel
        assert!(root.is_sentinel());

        // Intermediate "a" is Sentinel
        let a = root.get("a").unwrap();
        assert!(a.is_sentinel());

        // Intermediate "a/b" is Sentinel
        let b = root.get_path(&["a", "b"]).unwrap();
        assert!(b.is_sentinel());

        // Leaf "a/b/c" is a real value
        let c = root.get_path(&["a", "b", "c"]).unwrap();
        assert!(!c.is_sentinel());
        assert_eq!(c.to_value(), json!(42));
    }

    #[test]
    fn test_set_path_mut_sentinel_preserves_existing_children() {
        let mut root = ArcValue::empty_sentinel();

        // Write two leaves through Sentinel intermediates
        root.set_path_mut_sentinel(&["users", "alice"], ArcValue::from_value(json!("Alice")));
        root.set_path_mut_sentinel(&["users", "bob"], ArcValue::from_value(json!("Bob")));

        // Both should be reachable
        assert_eq!(
            root.get_path(&["users", "alice"]).unwrap().to_value(),
            json!("Alice")
        );
        assert_eq!(
            root.get_path(&["users", "bob"]).unwrap().to_value(),
            json!("Bob")
        );

        // "users" intermediate is still Sentinel
        assert!(root.get("users").unwrap().is_sentinel());
    }

    #[test]
    fn test_set_path_mut_sentinel_overwrites_leaf() {
        let mut root = ArcValue::empty_sentinel();

        root.set_path_mut_sentinel(&["key"], ArcValue::from_value(json!("old")));
        root.set_path_mut_sentinel(&["key"], ArcValue::from_value(json!("new")));

        assert_eq!(root.get("key").unwrap().to_value(), json!("new"));
    }

    #[test]
    fn test_sentinel_children_map() {
        let mut s = ArcValue::empty_sentinel();
        s.set_path_mut_sentinel(&["a"], ArcValue::from_value(json!(1)));
        s.set_path_mut_sentinel(&["b"], ArcValue::from_value(json!(2)));

        // children_map works for Sentinels
        let map = s.children_map().unwrap();
        assert_eq!(map.len(), 2);
        assert!(map.contains_key("a"));
        assert!(map.contains_key("b"));
    }

    #[test]
    fn test_object_is_not_sentinel() {
        let obj = ArcValue::from_value(json!({"key": "value"}));
        assert!(!obj.is_sentinel());
        assert!(obj.is_object());
        assert!(obj.exists());
    }
}
