//! Firebase Hash Algorithm
//!
//! Firebase uses a custom hash algorithm for transaction compare-and-swap:
//! - SHA-1 hash, base64 encoded output
//! - Custom string format for input:
//!   - LeafNode: [priority:]<type>:<value>
//!   - ChildrenNode: [priority:][:key:childHash]...
//! - Numbers use IEEE 754 64-bit binary representation as hex
//! - Children are iterated in priority order (by priority value, then by key)
//!
//! This differs from Lark's native hash (JCS + SHA-256 + hex).

use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::Value;
use sha1::{Digest, Sha1};
use std::cmp::Ordering;

/// Compute the Firebase-compatible hash of a value.
/// Returns base64-encoded SHA-1 hash, or empty string for empty/null values.
pub fn compute_firebase_hash(value: &Value) -> String {
    let to_hash = build_firebase_hash_string(value, None);
    if to_hash.is_empty() {
        return String::new();
    }
    let mut hasher = Sha1::new();
    hasher.update(to_hash.as_bytes());
    let result = hasher.finalize();
    STANDARD.encode(result)
}

/// Returns true if the hash appears to be a Firebase hash (base64)
/// rather than a Lark hash (lowercase hex).
pub fn is_firebase_hash(hash: &str) -> bool {
    if hash.is_empty() {
        return false;
    }

    // Lark hashes are 64 hex chars
    if hash.len() == 64 {
        // Check if all lowercase hex
        for c in hash.chars() {
            if !matches!(c, '0'..='9' | 'a'..='f') {
                return true; // Contains non-hex char, must be base64
            }
        }
        return false; // All hex, it's a Lark hash
    }

    // Firebase base64 SHA-1 hashes are ~28 chars
    // Anything else is assumed to be Firebase
    true
}

/// Build the string to be hashed for a value.
fn build_firebase_hash_string(value: &Value, priority: Option<&Value>) -> String {
    match value {
        Value::Null => String::new(),
        Value::Object(map) => {
            // Check for .value wrapper (LeafNode with priority)
            if let Some(inner_val) = map.get(".value") {
                let pri = map.get(".priority");
                return build_leaf_hash_string(inner_val, pri);
            }

            // This is a ChildrenNode (object with children)
            let node_priority = map.get(".priority");
            build_children_hash_string(map, node_priority)
        }
        Value::Array(arr) => {
            // Arrays hash as integer-keyed children (index -> element); null
            // elements are gaps and contribute nothing to the hash.
            let map: serde_json::Map<String, Value> = arr
                .iter()
                .enumerate()
                .map(|(i, v)| (i.to_string(), v.clone()))
                .collect();
            build_children_hash_string(&map, priority)
        }
        // Primitive value (LeafNode without priority wrapper)
        _ => build_leaf_hash_string(value, priority),
    }
}

/// Build hash string for a leaf node (primitive value).
/// Format: [priority:<priorityHashText>:]<type>:<value>
fn build_leaf_hash_string(value: &Value, priority: Option<&Value>) -> String {
    if value.is_null() {
        return String::new();
    }

    let mut result = String::new();

    // Add priority prefix if present
    if let Some(pri) = priority
        && !pri.is_null()
    {
        result.push_str("priority:");
        result.push_str(&priority_hash_text(pri));
        result.push(':');
    }

    // Add type and value
    match value {
        Value::Bool(b) => {
            result.push_str("boolean:");
            result.push_str(if *b { "true" } else { "false" });
        }
        Value::Number(n) => {
            result.push_str("number:");
            let f = n.as_f64().unwrap_or(0.0);
            result.push_str(&double_to_ieee754_string(f));
        }
        Value::String(s) => {
            result.push_str("string:");
            result.push_str(s);
        }
        _ => return String::new(),
    }

    result
}

/// Build hash string for a children node (object).
/// Format: [priority:<priorityHashText>:][:key:childHash]...
fn build_children_hash_string(
    map: &serde_json::Map<String, Value>,
    priority: Option<&Value>,
) -> String {
    // Filter out .priority from children
    let children: Vec<(&String, &Value)> = map.iter().filter(|(k, _)| *k != ".priority").collect();

    if children.is_empty() {
        return String::new();
    }

    let mut result = String::new();

    // Add priority prefix if present
    if let Some(pri) = priority
        && !pri.is_null()
    {
        result.push_str("priority:");
        result.push_str(&priority_hash_text(pri));
        result.push(':');
    }

    // Sort children by priority order
    let mut sorted_children = children;
    sorted_children.sort_by(|(k1, v1), (k2, v2)| {
        let p1 = v1.as_object().and_then(|m| m.get(".priority"));
        let p2 = v2.as_object().and_then(|m| m.get(".priority"));
        compare_priorities(p1, k1, p2, k2)
    });

    // Build hash string from children
    for (key, child_value) in sorted_children {
        let child_hash = compute_firebase_hash(child_value);
        if !child_hash.is_empty() {
            result.push(':');
            result.push_str(key);
            result.push(':');
            result.push_str(&child_hash);
        }
    }

    result
}

/// Compare two (priority, key) pairs using Firebase ordering.
fn compare_priorities(p1: Option<&Value>, k1: &str, p2: Option<&Value>, k2: &str) -> Ordering {
    let cmp = compare_priority_values(p1, p2);
    if cmp != Ordering::Equal {
        return cmp;
    }
    compare_keys(k1, k2)
}

/// Compare two priority values.
/// Order: null < numbers < strings
fn compare_priority_values(p1: Option<&Value>, p2: Option<&Value>) -> Ordering {
    let t1 = priority_type(p1);
    let t2 = priority_type(p2);

    if t1 != t2 {
        return t1.cmp(&t2);
    }

    // Same type
    match t1 {
        0 => Ordering::Equal, // null
        1 => {
            // number
            let n1 = p1.and_then(|v| v.as_f64()).unwrap_or(0.0);
            let n2 = p2.and_then(|v| v.as_f64()).unwrap_or(0.0);
            n1.partial_cmp(&n2).unwrap_or(Ordering::Equal)
        }
        2 => {
            // string
            let s1 = p1.and_then(|v| v.as_str()).unwrap_or("");
            let s2 = p2.and_then(|v| v.as_str()).unwrap_or("");
            s1.cmp(s2)
        }
        _ => Ordering::Equal,
    }
}

/// Returns 0 for null, 1 for number, 2 for string
fn priority_type(p: Option<&Value>) -> i32 {
    match p {
        None | Some(Value::Null) => 0,
        Some(Value::Number(_)) => 1,
        Some(Value::String(_)) => 2,
        _ => 0,
    }
}

/// Compare keys using Firebase's key ordering (integers sorted numerically before strings).
fn compare_keys(k1: &str, k2: &str) -> Ordering {
    let n1 = k1.parse::<i64>();
    let n2 = k2.parse::<i64>();

    match (n1, n2) {
        (Ok(a), Ok(b)) => a.cmp(&b),
        (Ok(_), Err(_)) => Ordering::Less, // numbers before strings
        (Err(_), Ok(_)) => Ordering::Greater, // strings after numbers
        (Err(_), Err(_)) => k1.cmp(k2),    // lexicographic for strings
    }
}

/// Format a priority value for hashing.
fn priority_hash_text(priority: &Value) -> String {
    match priority {
        Value::Number(n) => {
            let f = n.as_f64().unwrap_or(0.0);
            format!("number:{}", double_to_ieee754_string(f))
        }
        Value::String(s) => format!("string:{}", s),
        _ => String::new(),
    }
}

/// Convert a float64 to its IEEE 754 64-bit binary representation as a 16-character hex string.
fn double_to_ieee754_string(v: f64) -> String {
    let bits = v.to_bits();
    format!("{:016x}", bits)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_compute_firebase_hash_null() {
        assert_eq!(compute_firebase_hash(&Value::Null), "");
    }

    #[test]
    fn test_compute_firebase_hash_string() {
        let hash = compute_firebase_hash(&json!("hello"));
        assert!(!hash.is_empty());
        assert!(is_firebase_hash(&hash));
    }

    #[test]
    fn test_compute_firebase_hash_number() {
        let hash = compute_firebase_hash(&json!(42));
        assert!(!hash.is_empty());
    }

    #[test]
    fn test_compute_firebase_hash_object() {
        let hash = compute_firebase_hash(&json!({"a": 1, "b": 2}));
        assert!(!hash.is_empty());
    }

    #[test]
    fn test_array_hashes_as_integer_keyed_object() {
        // An array hashes identically to the integer-keyed object it represents.
        let arr = compute_firebase_hash(&json!(["cat", "horse"]));
        assert!(!arr.is_empty());
        assert_eq!(arr, compute_firebase_hash(&json!({"0": "cat", "1": "horse"})));

        // Null elements are gaps: they contribute nothing, matching the sparse
        // object form.
        assert_eq!(
            compute_firebase_hash(&json!(["a", null, "c"])),
            compute_firebase_hash(&json!({"0": "a", "2": "c"}))
        );
    }

    #[test]
    fn test_is_firebase_hash() {
        // Lark hash (64 hex chars)
        let lark_hash = "a".repeat(64);
        assert!(!is_firebase_hash(&lark_hash));

        // Firebase hash (base64, ~28 chars)
        let firebase_hash = "AAAAAAAAAAAAAAAAAAAAAAAAAAAA";
        assert!(is_firebase_hash(firebase_hash));

        // Empty hash
        assert!(!is_firebase_hash(""));
    }

    #[test]
    fn test_double_to_ieee754_string() {
        // Test 0.0
        assert_eq!(double_to_ieee754_string(0.0), "0000000000000000");

        // Test 1.0
        assert_eq!(double_to_ieee754_string(1.0), "3ff0000000000000");

        // Test -1.0
        assert_eq!(double_to_ieee754_string(-1.0), "bff0000000000000");
    }
}
