//! Runtime value types for expression evaluation.

use regex::Regex;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::sync::Arc;

use crate::rules::NeedsPromotion;

/// Value kind enumeration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueKind {
    Null,
    Bool,
    Number,
    String,
    Object,
    Array,
    Snapshot,
    ObjectSentinel,
}

/// Snapshot interface for data/newData/root values.
/// This matches the methods needed by rules evaluation.
///
/// Methods that access data return `Result<T, NeedsPromotion>` because they may
/// hit cold segments that need to be promoted before data can be accessed.
/// Navigation methods (`child`, `parent`) are free and don't access data.
pub trait Snapshot: std::fmt::Debug {
    /// Get the value at this path. Returns an ObjectSentinel marker for objects/arrays.
    fn val(&self) -> Result<Option<JsonValue>, NeedsPromotion>;
    /// Check if a value exists at this path.
    fn exists(&self) -> Result<bool, NeedsPromotion>;
    /// Check if this node has a child with the given name.
    fn has_child(&self, name: &str) -> Result<bool, NeedsPromotion>;
    /// Check if this node has all the specified children.
    fn has_children(&self, names: &[String]) -> Result<bool, NeedsPromotion>;
    /// Navigate to a child path. This is free (no data access).
    fn child(&self, path: &str) -> Box<dyn Snapshot>;
    /// Navigate to the parent path. This is free (no data access).
    fn parent(&self) -> Box<dyn Snapshot>;
    /// Check if the value is a string.
    fn is_string(&self) -> Result<bool, NeedsPromotion>;
    /// Check if the value is a number.
    fn is_number(&self) -> Result<bool, NeedsPromotion>;
    /// Check if the value is a boolean.
    fn is_boolean(&self) -> Result<bool, NeedsPromotion>;
    /// Get the priority value (.priority child).
    fn get_priority(&self) -> Result<Option<JsonValue>, NeedsPromotion>;
}

/// Marker value returned when val() is called on an object or array.
/// This is used because val() returns a sentinel for objects,
/// not the actual children.
pub const OBJECT_SENTINEL_MARKER: &str = "__lark_object_sentinel__";

/// Runtime value in expression evaluation.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Number(f64),
    String(String),
    /// Object values use Arc for O(1) cloning - critical for auth access in hot path.
    Object(Arc<HashMap<String, JsonValue>>),
    Array(Vec<JsonValue>),
    Snapshot(Box<dyn Snapshot>),
    /// Sentinel value for objects/arrays from val().
    /// - Truthy (to_bool() → true)
    /// - ObjectSentinel !== null → true
    /// - ObjectSentinel === <any primitive> → false
    /// - ObjectSentinel === ObjectSentinel → false (never equals anything)
    ObjectSentinel,
    /// Pre-compiled regex (compiled at parse time, used by matches()).
    Regex(CachedRegex),
}

/// Wrapper around regex::Regex that implements PartialEq (always false, like Snapshot).
#[derive(Debug, Clone)]
pub struct CachedRegex(pub Regex);

impl PartialEq for CachedRegex {
    fn eq(&self, _other: &Self) -> bool {
        false
    }
}

// Manually implement Clone for Box<dyn Snapshot>
impl Clone for Box<dyn Snapshot> {
    fn clone(&self) -> Self {
        // Create a null snapshot as a placeholder - real cloning happens through proper APIs
        Box::new(NullSnapshot)
    }
}

// Manually implement PartialEq for Box<dyn Snapshot>
// Snapshots are never equal to each other (reference semantics)
impl PartialEq for Box<dyn Snapshot> {
    fn eq(&self, _other: &Self) -> bool {
        false
    }
}

/// A null snapshot that returns null for everything.
#[derive(Debug, Clone)]
pub struct NullSnapshot;

impl Snapshot for NullSnapshot {
    fn val(&self) -> Result<Option<JsonValue>, NeedsPromotion> {
        Ok(None)
    }
    fn exists(&self) -> Result<bool, NeedsPromotion> {
        Ok(false)
    }
    fn has_child(&self, _name: &str) -> Result<bool, NeedsPromotion> {
        Ok(false)
    }
    fn has_children(&self, _names: &[String]) -> Result<bool, NeedsPromotion> {
        Ok(false)
    }
    fn child(&self, _path: &str) -> Box<dyn Snapshot> {
        Box::new(NullSnapshot)
    }
    fn parent(&self) -> Box<dyn Snapshot> {
        Box::new(NullSnapshot)
    }
    fn is_string(&self) -> Result<bool, NeedsPromotion> {
        Ok(false)
    }
    fn is_number(&self) -> Result<bool, NeedsPromotion> {
        Ok(false)
    }
    fn is_boolean(&self) -> Result<bool, NeedsPromotion> {
        Ok(false)
    }
    fn get_priority(&self) -> Result<Option<JsonValue>, NeedsPromotion> {
        Ok(None)
    }
}

impl Value {
    /// Create a null value.
    pub fn null() -> Self {
        Value::Null
    }

    /// Create a boolean value.
    pub fn bool(b: bool) -> Self {
        Value::Bool(b)
    }

    /// Create a number value.
    pub fn number(n: f64) -> Self {
        Value::Number(n)
    }

    /// Create a string value.
    pub fn string(s: impl Into<String>) -> Self {
        Value::String(s.into())
    }

    /// Create an object value.
    pub fn object(o: HashMap<String, JsonValue>) -> Self {
        Value::Object(Arc::new(o))
    }

    /// Create an object value from an existing Arc (O(1) clone).
    pub fn object_arc(o: Arc<HashMap<String, JsonValue>>) -> Self {
        Value::Object(o)
    }

    /// Create an array value.
    pub fn array(a: Vec<JsonValue>) -> Self {
        Value::Array(a)
    }

    /// Create a snapshot value.
    pub fn snapshot(s: Box<dyn Snapshot>) -> Self {
        Value::Snapshot(s)
    }

    /// Create from a JSON value.
    pub fn from_json(v: &JsonValue) -> Self {
        match v {
            JsonValue::Null => Value::Null,
            JsonValue::Bool(b) => Value::Bool(*b),
            JsonValue::Number(n) => Value::Number(n.as_f64().unwrap_or(0.0)),
            JsonValue::String(s) => Value::String(s.clone()),
            JsonValue::Object(o) => {
                let map: HashMap<String, JsonValue> =
                    o.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                Value::Object(Arc::new(map))
            }
            JsonValue::Array(a) => Value::Array(a.clone()),
        }
    }

    /// Create from any Go-style value.
    pub fn from_any(v: Option<&JsonValue>) -> Self {
        match v {
            Some(val) => Self::from_json(val),
            None => Value::Null,
        }
    }

    /// Get the kind of this value.
    pub fn kind(&self) -> ValueKind {
        match self {
            Value::Null => ValueKind::Null,
            Value::Bool(_) => ValueKind::Bool,
            Value::Number(_) => ValueKind::Number,
            Value::String(_) | Value::Regex(_) => ValueKind::String,
            Value::Object(_) => ValueKind::Object,
            Value::Array(_) => ValueKind::Array,
            Value::Snapshot(_) => ValueKind::Snapshot,
            Value::ObjectSentinel => ValueKind::ObjectSentinel,
        }
    }

    /// Check if this is a null value.
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Convert to boolean (JavaScript truthiness rules).
    pub fn to_bool(&self) -> bool {
        match self {
            Value::Null => false,
            Value::Bool(b) => *b,
            Value::Number(n) => *n != 0.0,
            Value::String(s) => !s.is_empty(),
            Value::Object(_) | Value::Array(_) | Value::Snapshot(_) | Value::Regex(_) => true,
            Value::ObjectSentinel => true, // ObjectSentinel is truthy
        }
    }

    /// Convert to number.
    pub fn to_number(&self) -> f64 {
        match self {
            Value::Null => 0.0,
            Value::Bool(b) if *b => 1.0,
            Value::Number(n) => *n,
            Value::String(_) => 0.0, // Don't parse strings to numbers
            _ => 0.0,
        }
    }

    /// Convert to string.
    pub fn to_string_val(&self) -> String {
        match self {
            Value::Null => "null".to_string(),
            Value::Bool(b) => {
                if *b {
                    "true".to_string()
                } else {
                    "false".to_string()
                }
            }
            Value::Number(_) => String::new(), // Numbers don't convert meaningfully
            Value::String(s) => s.clone(),
            _ => String::new(),
        }
    }

    /// Get as string if this is a string.
    pub fn as_string(&self) -> &str {
        match self {
            Value::String(s) => s,
            _ => "",
        }
    }

    /// Get as number if this is a number.
    pub fn as_number(&self) -> f64 {
        match self {
            Value::Number(n) => *n,
            _ => 0.0,
        }
    }

    /// Get a property from an object or snapshot.
    pub fn get_property(&self, name: &str) -> Value {
        match self {
            Value::Object(o) => match o.get(name) {
                Some(v) => Value::from_json(v),
                None => Value::Null,
            },
            Value::String(s) => {
                if name == "length" {
                    Value::Number(s.len() as f64)
                } else {
                    Value::Null
                }
            }
            Value::Array(a) => {
                if name == "length" {
                    Value::Number(a.len() as f64)
                } else {
                    Value::Null
                }
            }
            Value::Snapshot(_) => {
                // Snapshots don't have direct properties - they have methods
                Value::Null
            }
            _ => Value::Null,
        }
    }

    /// Call a method on this value.
    /// Returns Result to propagate NeedsPromotion from snapshot methods.
    pub fn call_method(&self, name: &str, args: &[Value]) -> Result<Value, NeedsPromotion> {
        match self {
            Value::Snapshot(s) => call_snapshot_method(s.as_ref(), name, args),
            Value::String(s) => Ok(call_string_method(s, name, args)),
            Value::Array(a) => Ok(call_array_method(a, name, args)),
            Value::Object(o) => Ok(call_object_method(o, name, args)),
            _ => Ok(Value::Null),
        }
    }
}

/// Check if a JSON value is the object sentinel marker.
pub fn is_object_sentinel(v: &JsonValue) -> bool {
    matches!(v, JsonValue::String(s) if s == OBJECT_SENTINEL_MARKER)
}

fn call_snapshot_method(
    snap: &dyn Snapshot,
    name: &str,
    args: &[Value],
) -> Result<Value, NeedsPromotion> {
    match name {
        "val" => match snap.val()? {
            Some(v) if is_object_sentinel(&v) => Ok(Value::ObjectSentinel),
            Some(v) => Ok(Value::from_json(&v)),
            None => Ok(Value::Null),
        },
        "exists" => Ok(Value::Bool(snap.exists()?)),
        "hasChild" => {
            let child_name = args.first().map(|a| a.as_string()).unwrap_or("");
            Ok(Value::Bool(snap.has_child(child_name)?))
        }
        "hasChildren" => {
            if let Some(Value::Array(arr)) = args.first() {
                // hasChildren(['a', 'b']) - check if all specified children exist
                let names: Vec<String> = arr
                    .iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect();
                Ok(Value::Bool(snap.has_children(&names)?))
            } else {
                // hasChildren() with no args - check if value is an object with any children
                // Returns true if this DataSnapshot contains any children
                match snap.val()? {
                    Some(serde_json::Value::Object(map)) => Ok(Value::Bool(!map.is_empty())),
                    _ => Ok(Value::Bool(false)),
                }
            }
        }
        "child" => {
            let path = args.first().map(|a| a.as_string()).unwrap_or("");
            Ok(Value::Snapshot(snap.child(path)))
        }
        "parent" => Ok(Value::Snapshot(snap.parent())),
        "isString" => Ok(Value::Bool(snap.is_string()?)),
        "isNumber" => Ok(Value::Bool(snap.is_number()?)),
        "isBoolean" => Ok(Value::Bool(snap.is_boolean()?)),
        "getPriority" => match snap.get_priority()? {
            Some(v) => Ok(Value::from_json(&v)),
            None => Ok(Value::Null),
        },
        _ => Ok(Value::Null),
    }
}

fn call_string_method(s: &str, name: &str, args: &[Value]) -> Value {
    match name {
        "contains" => {
            let substr = args.first().map(|a| a.as_string()).unwrap_or("");
            Value::Bool(s.contains(substr))
        }
        "beginsWith" | "startsWith" => {
            let prefix = args.first().map(|a| a.as_string()).unwrap_or("");
            Value::Bool(s.starts_with(prefix))
        }
        "endsWith" => {
            let suffix = args.first().map(|a| a.as_string()).unwrap_or("");
            Value::Bool(s.ends_with(suffix))
        }
        "matches" => {
            match args.first() {
                Some(Value::Regex(cached)) => Value::Bool(cached.0.is_match(s)),
                _ => {
                    // Fallback: compile at runtime (for dynamically constructed patterns)
                    let pattern = args.first().map(|a| a.as_string()).unwrap_or("");
                    match Regex::new(pattern) {
                        Ok(re) => Value::Bool(re.is_match(s)),
                        Err(_) => Value::Bool(false),
                    }
                }
            }
        }
        "replace" => {
            if args.len() < 2 {
                return Value::String(s.to_string());
            }
            let from = args[0].as_string();
            let to = args[1].as_string();
            // Replace ALL occurrences
            Value::String(s.replace(from, to))
        }
        "toLowerCase" => Value::String(s.to_lowercase()),
        "toUpperCase" => Value::String(s.to_uppercase()),
        "trim" => Value::String(s.trim().to_string()),
        _ => Value::Null,
    }
}

fn call_array_method(a: &[JsonValue], name: &str, args: &[Value]) -> Value {
    match name {
        "includes" => {
            if let Some(target) = args.first() {
                for elem in a {
                    if strict_equals(&Value::from_json(elem), target) {
                        return Value::Bool(true);
                    }
                }
            }
            Value::Bool(false)
        }
        _ => Value::Null,
    }
}

fn call_object_method(o: &HashMap<String, JsonValue>, name: &str, args: &[Value]) -> Value {
    match name {
        "hasOwnProperty" => {
            let key = args.first().map(|a| a.as_string()).unwrap_or("");
            Value::Bool(o.contains_key(key))
        }
        _ => Value::Null,
    }
}

/// JavaScript === comparison.
pub fn strict_equals(a: &Value, b: &Value) -> bool {
    // ObjectSentinel never equals anything, not even itself
    if matches!(a, Value::ObjectSentinel) || matches!(b, Value::ObjectSentinel) {
        return false;
    }

    if a.kind() != b.kind() {
        return false;
    }
    match (a, b) {
        (Value::Null, Value::Null) => true,
        (Value::Bool(a), Value::Bool(b)) => a == b,
        (Value::Number(a), Value::Number(b)) => a == b,
        (Value::String(a), Value::String(b)) => a == b,
        // Objects and arrays use reference equality - different instances are never equal
        (Value::Object(_), Value::Object(_)) => false,
        (Value::Array(_), Value::Array(_)) => false,
        (Value::Snapshot(_), Value::Snapshot(_)) => false,
        _ => false,
    }
}

/// JavaScript == comparison (with type coercion).
pub fn loose_equals(a: &Value, b: &Value) -> bool {
    // If same type, use strict equals
    if a.kind() == b.kind() {
        return strict_equals(a, b);
    }

    // null == null
    if matches!((a, b), (Value::Null, Value::Null)) {
        return true;
    }

    // Number comparisons with type coercion
    match (a, b) {
        (Value::Number(n), Value::String(_)) => *n == b.to_number(),
        (Value::String(_), Value::Number(n)) => a.to_number() == *n,
        (Value::Bool(_), _) => loose_equals(&Value::Number(a.to_number()), b),
        (_, Value::Bool(_)) => loose_equals(a, &Value::Number(b.to_number())),
        _ => false,
    }
}

/// Compare two values, returning -1, 0, or 1.
pub fn compare(a: &Value, b: &Value) -> i32 {
    // If both are numbers
    if let (Value::Number(na), Value::Number(nb)) = (a, b) {
        return if na < nb {
            -1
        } else if na > nb {
            1
        } else {
            0
        };
    }

    // If both are strings
    if let (Value::String(sa), Value::String(sb)) = (a, b) {
        return match sa.cmp(sb) {
            std::cmp::Ordering::Less => -1,
            std::cmp::Ordering::Greater => 1,
            std::cmp::Ordering::Equal => 0,
        };
    }

    // Mixed types - convert to numbers
    let an = a.to_number();
    let bn = b.to_number();
    if an < bn {
        -1
    } else if an > bn {
        1
    } else {
        0
    }
}

/// Add two values (JavaScript + semantics).
pub fn add(a: &Value, b: &Value) -> Value {
    // String concatenation takes precedence
    if matches!(a, Value::String(_)) || matches!(b, Value::String(_)) {
        return Value::String(a.to_string_val() + &b.to_string_val());
    }
    // Numeric addition
    Value::Number(a.to_number() + b.to_number())
}

/// Subtract two values.
pub fn subtract(a: &Value, b: &Value) -> Value {
    Value::Number(a.to_number() - b.to_number())
}

/// Multiply two values.
pub fn multiply(a: &Value, b: &Value) -> Value {
    Value::Number(a.to_number() * b.to_number())
}

/// Divide two values.
pub fn divide(a: &Value, b: &Value) -> Value {
    let bv = b.to_number();
    if bv == 0.0 {
        return Value::Number(0.0); // Simplified - real JS returns Infinity or NaN
    }
    Value::Number(a.to_number() / bv)
}

/// Modulo of two values.
pub fn modulo(a: &Value, b: &Value) -> Value {
    let bv = b.to_number();
    if bv == 0.0 {
        return Value::Number(0.0);
    }
    let av = a.to_number();
    Value::Number((av as i64 % bv as i64) as f64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_truthiness() {
        assert!(!Value::Null.to_bool());
        assert!(!Value::Bool(false).to_bool());
        assert!(Value::Bool(true).to_bool());
        assert!(!Value::Number(0.0).to_bool());
        assert!(Value::Number(1.0).to_bool());
        assert!(!Value::String(String::new()).to_bool());
        assert!(Value::String("hello".to_string()).to_bool());
    }

    #[test]
    fn test_strict_equals() {
        assert!(strict_equals(&Value::Null, &Value::Null));
        assert!(strict_equals(&Value::Bool(true), &Value::Bool(true)));
        assert!(!strict_equals(&Value::Bool(true), &Value::Bool(false)));
        assert!(strict_equals(&Value::Number(42.0), &Value::Number(42.0)));
        assert!(strict_equals(
            &Value::String("a".to_string()),
            &Value::String("a".to_string())
        ));
        // Different types
        assert!(!strict_equals(&Value::Number(1.0), &Value::Bool(true)));
    }

    #[test]
    fn test_string_methods() {
        let s = "hello world";
        assert_eq!(
            call_string_method(s, "contains", &[Value::String("world".to_string())]),
            Value::Bool(true)
        );
        assert_eq!(
            call_string_method(s, "startsWith", &[Value::String("hello".to_string())]),
            Value::Bool(true)
        );
        assert_eq!(
            call_string_method(s, "endsWith", &[Value::String("world".to_string())]),
            Value::Bool(true)
        );
    }

    #[test]
    fn test_string_matches() {
        let s = "hello123";
        assert_eq!(
            call_string_method(s, "matches", &[Value::String("[a-z]+[0-9]+".to_string())]),
            Value::Bool(true)
        );
        assert_eq!(
            call_string_method(s, "matches", &[Value::String("^[0-9]+$".to_string())]),
            Value::Bool(false)
        );
    }

    #[test]
    fn test_compare() {
        assert_eq!(compare(&Value::Number(1.0), &Value::Number(2.0)), -1);
        assert_eq!(compare(&Value::Number(2.0), &Value::Number(1.0)), 1);
        assert_eq!(compare(&Value::Number(1.0), &Value::Number(1.0)), 0);
    }

    #[test]
    fn test_arithmetic() {
        assert!(matches!(
            add(&Value::Number(1.0), &Value::Number(2.0)),
            Value::Number(n) if n == 3.0
        ));
        assert!(matches!(
            subtract(&Value::Number(5.0), &Value::Number(3.0)),
            Value::Number(n) if n == 2.0
        ));
        assert!(matches!(
            multiply(&Value::Number(3.0), &Value::Number(4.0)),
            Value::Number(n) if n == 12.0
        ));
        assert!(matches!(
            divide(&Value::Number(10.0), &Value::Number(2.0)),
            Value::Number(n) if n == 5.0
        ));
    }

    #[test]
    fn test_string_concatenation() {
        assert!(matches!(
            add(&Value::String("hello".to_string()), &Value::String(" world".to_string())),
            Value::String(s) if s == "hello world"
        ));
    }

    // =========================================================================
    // Tests for ObjectSentinel (val() behavior for objects)
    // =========================================================================

    #[test]
    fn test_object_sentinel_truthiness() {
        // ObjectSentinel should be truthy (objects exist and are truthy)
        assert!(Value::ObjectSentinel.to_bool());
    }

    #[test]
    fn test_object_sentinel_never_equals_anything() {
        // ObjectSentinel === ObjectSentinel should be false
        assert!(!strict_equals(
            &Value::ObjectSentinel,
            &Value::ObjectSentinel
        ));

        // ObjectSentinel === null should be false
        assert!(!strict_equals(&Value::ObjectSentinel, &Value::Null));

        // ObjectSentinel === any primitive should be false
        assert!(!strict_equals(&Value::ObjectSentinel, &Value::Bool(true)));
        assert!(!strict_equals(&Value::ObjectSentinel, &Value::Number(42.0)));
        assert!(!strict_equals(
            &Value::ObjectSentinel,
            &Value::String("test".to_string())
        ));
    }

    #[test]
    fn test_object_sentinel_not_equal_to_null() {
        // ObjectSentinel !== null should be true (objects exist)
        // This is the inverse of strict_equals
        assert!(!strict_equals(&Value::ObjectSentinel, &Value::Null));
    }

    #[test]
    fn test_object_sentinel_kind() {
        let sentinel = Value::ObjectSentinel;
        assert_eq!(sentinel.kind(), ValueKind::ObjectSentinel);
    }

    #[test]
    fn test_is_object_sentinel_function() {
        // Test the helper function that checks JSON values
        let sentinel_json = serde_json::json!(OBJECT_SENTINEL_MARKER);
        assert!(is_object_sentinel(&sentinel_json));

        // Non-sentinel values
        assert!(!is_object_sentinel(&serde_json::json!("regular string")));
        assert!(!is_object_sentinel(&serde_json::json!(42)));
        assert!(!is_object_sentinel(&serde_json::json!(null)));
        assert!(!is_object_sentinel(&serde_json::json!({"key": "value"})));
        assert!(!is_object_sentinel(&serde_json::json!([1, 2, 3])));
    }

    #[test]
    fn test_object_sentinel_marker_constant() {
        // Verify the marker string is what we expect
        assert_eq!(OBJECT_SENTINEL_MARKER, "__lark_object_sentinel__");
    }

    #[test]
    fn test_replace_all_occurrences() {
        // Test that replace() replaces ALL occurrences, not just the first
        let s = "hello hello hello";
        let result = call_string_method(
            s,
            "replace",
            &[
                Value::String("hello".to_string()),
                Value::String("hi".to_string()),
            ],
        );
        assert_eq!(result, Value::String("hi hi hi".to_string()));
    }

    #[test]
    fn test_replace_single_occurrence() {
        let s = "hello world";
        let result = call_string_method(
            s,
            "replace",
            &[
                Value::String("world".to_string()),
                Value::String("there".to_string()),
            ],
        );
        assert_eq!(result, Value::String("hello there".to_string()));
    }

    #[test]
    fn test_replace_no_match() {
        let s = "hello world";
        let result = call_string_method(
            s,
            "replace",
            &[
                Value::String("xyz".to_string()),
                Value::String("abc".to_string()),
            ],
        );
        assert_eq!(result, Value::String("hello world".to_string()));
    }
}
