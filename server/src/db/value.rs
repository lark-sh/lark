//! Value comparison and sorting.
//!
//! Specific ordering rules for mixed-type values:
//! 1. null
//! 2. false
//! 3. true
//! 4. Numbers (ascending)
//! 5. Strings (lexicographic)
//! 6. Objects/Arrays (sorted by key only)
//!
//! For key comparison (orderByKey), treat 32-bit integer keys specially,
//! sorting them numerically before string keys.

use lark_blob::ArcValue;
use serde_json::Value;
use std::cmp::Ordering;
use std::sync::Arc;

use super::query::OrderBy;

/// Extension trait for ArcValue sort-related methods (Lark-specific).
pub trait ArcValueSortExt {
    fn get_sort_value(&self, order_by: &OrderBy) -> Option<SortKey>;
    fn get_nested_primitive_for_sort(&self, path: &str) -> Option<SortKey>;
    fn to_sort_key(&self) -> Option<SortKey>;
}

impl ArcValueSortExt for ArcValue {
    fn get_sort_value(&self, order_by: &OrderBy) -> Option<SortKey> {
        match order_by {
            OrderBy::Key => None,
            OrderBy::Value => self.to_sort_key(),
            OrderBy::Child(path) => self.get_nested_primitive_for_sort(path),
            OrderBy::Priority => self.get_nested_primitive_for_sort(".priority"),
        }
    }

    fn get_nested_primitive_for_sort(&self, path: &str) -> Option<SortKey> {
        let mut current = self;
        for segment in path.split('/').filter(|s| !s.is_empty()) {
            current = current.get(segment)?;
        }
        match current {
            ArcValue::Null => Some(SortKey::Null),
            ArcValue::Bool(b) => Some(SortKey::Bool(*b)),
            ArcValue::Number(n) => Some(SortKey::Number(n.as_f64().unwrap_or(0.0))),
            ArcValue::String(s) => Some(SortKey::String(s.clone())),
            ArcValue::Array(_) | ArcValue::Object(_) | ArcValue::Sentinel(_) => {
                Some(SortKey::Object)
            }
        }
    }

    fn to_sort_key(&self) -> Option<SortKey> {
        match self {
            ArcValue::Null => Some(SortKey::Null),
            ArcValue::Bool(b) => Some(SortKey::Bool(*b)),
            ArcValue::Number(n) => Some(SortKey::Number(n.as_f64().unwrap_or(0.0))),
            ArcValue::String(s) => Some(SortKey::String(s.clone())),
            ArcValue::Array(_) | ArcValue::Object(_) | ArcValue::Sentinel(_) => {
                Some(SortKey::Object)
            }
        }
    }
}

/// A lightweight sort key for efficient sorting without allocation.
///
/// Unlike serde_json::Value, SortKey stores strings as Arc<str> which can be
/// cloned in O(1) from ArcValue::String. This avoids allocating new strings
/// during sorting operations.
#[derive(Debug, Clone, PartialEq)]
pub enum SortKey {
    Null,
    Bool(bool),
    /// Stores f64 directly for fast comparison (no Number wrapper)
    Number(f64),
    /// Stores Arc<str> for O(1) cloning from ArcValue::String
    String(Arc<str>),
    /// Objects/arrays sort last
    Object,
}

impl SortKey {
    /// Returns the type rank for value ordering.
    /// Lower ranks sort first: null < false < true < numbers < strings < objects
    #[inline]
    fn type_rank(&self) -> u8 {
        match self {
            SortKey::Null => 0,
            SortKey::Bool(false) => 1,
            SortKey::Bool(true) => 2,
            SortKey::Number(_) => 3,
            SortKey::String(_) => 4,
            SortKey::Object => 5,
        }
    }
}

/// Compare two SortKeys using ordering rules.
#[inline]
pub fn compare_sort_keys(a: &SortKey, b: &SortKey) -> Ordering {
    let rank_a = a.type_rank();
    let rank_b = b.type_rank();

    match rank_a.cmp(&rank_b) {
        Ordering::Equal => {
            // Same type, compare within type
            match (a, b) {
                (SortKey::Number(na), SortKey::Number(nb)) => {
                    na.partial_cmp(nb).unwrap_or(Ordering::Equal)
                }
                (SortKey::String(sa), SortKey::String(sb)) => sa.cmp(sb),
                // null, bools, objects are equal within their rank
                _ => Ordering::Equal,
            }
        }
        other => other,
    }
}

/// Compare a SortKey against a serde_json::Value (for range bound comparisons).
/// The Value comes from query parameters which are parsed from JSON.
#[inline]
pub fn compare_sort_key_to_value(sort_key: &SortKey, value: &Value) -> Ordering {
    let rank_a = sort_key.type_rank();
    let rank_b = type_rank(value);

    match rank_a.cmp(&rank_b) {
        Ordering::Equal => {
            // Same type, compare within type
            match (sort_key, value) {
                (SortKey::Number(na), Value::Number(nb)) => {
                    let fb = nb.as_f64().unwrap_or(0.0);
                    na.partial_cmp(&fb).unwrap_or(Ordering::Equal)
                }
                (SortKey::String(sa), Value::String(sb)) => sa.as_ref().cmp(sb.as_str()),
                // null, bools, objects are equal within their rank
                _ => Ordering::Equal,
            }
        }
        other => other,
    }
}

/// Returns the type rank for value ordering.
/// Lower ranks sort first.
pub fn type_rank(value: &Value) -> u8 {
    match value {
        Value::Null => 0,
        Value::Bool(false) => 1,
        Value::Bool(true) => 2,
        Value::Number(_) => 3,
        Value::String(_) => 4,
        Value::Array(_) | Value::Object(_) => 5,
    }
}

/// Compare two values using ordering rules.
/// Returns Ordering for use in sort operations.
pub fn compare_values(a: &Value, b: &Value) -> Ordering {
    let rank_a = type_rank(a);
    let rank_b = type_rank(b);

    match rank_a.cmp(&rank_b) {
        Ordering::Equal => {
            // Same type, compare within type
            match (a, b) {
                (Value::Number(na), Value::Number(nb)) => {
                    let fa = na.as_f64().unwrap_or(0.0);
                    let fb = nb.as_f64().unwrap_or(0.0);
                    fa.partial_cmp(&fb).unwrap_or(Ordering::Equal)
                }
                (Value::String(sa), Value::String(sb)) => sa.cmp(sb),
                // null, bools, objects are equal within their rank
                _ => Ordering::Equal,
            }
        }
        other => other,
    }
}

/// A pre-parsed key for efficient sorting.
/// Caches whether the key is an int32 (with its value) or a string,
/// avoiding repeated parsing during O(N log N) sort comparisons.
#[derive(Debug, Clone)]
pub enum ParsedKey {
    /// Key is a valid 32-bit integer
    Int(i32),
    /// Key is a string (stored for lexicographic comparison)
    Str(String),
}

impl ParsedKey {
    /// Parse a key string into a ParsedKey.
    /// This is O(1) for the subsequent comparisons.
    pub fn parse(s: &str) -> Self {
        if let Some(i) = try_parse_int32_key(s) {
            ParsedKey::Int(i)
        } else {
            ParsedKey::Str(s.to_string())
        }
    }
}

/// Compare two ParsedKeys using orderByKey rules.
/// This is much faster than compare_keys() when called repeatedly.
pub fn compare_parsed_keys(a: &ParsedKey, b: &ParsedKey) -> Ordering {
    match (a, b) {
        // Both integers: compare numerically
        (ParsedKey::Int(ai), ParsedKey::Int(bi)) => ai.cmp(bi),
        // Integer comes before string
        (ParsedKey::Int(_), ParsedKey::Str(_)) => Ordering::Less,
        (ParsedKey::Str(_), ParsedKey::Int(_)) => Ordering::Greater,
        // Both strings: compare lexicographically
        (ParsedKey::Str(as_), ParsedKey::Str(bs)) => as_.cmp(bs),
    }
}

/// Try to parse a key as an int32. Returns Some(value) if valid, None otherwise.
fn try_parse_int32_key(s: &str) -> Option<i32> {
    if s.is_empty() {
        return None;
    }

    let bytes = s.as_bytes();

    // Check for leading zeros (except "0" itself)
    if bytes.len() > 1 && bytes[0] == b'0' {
        return None;
    }

    // Handle negative numbers
    if bytes[0] == b'-' {
        if bytes.len() == 1 {
            return None; // Just "-" is not valid
        }
        // "-0" is treated as a string, not an integer
        if s == "-0" {
            return None;
        }
        // Check for leading zeros after minus: "-007"
        if bytes.len() > 2 && bytes[1] == b'0' {
            return None;
        }
    }

    // Try to parse as i64 and check range, then convert to i32
    match s.parse::<i64>() {
        Ok(i) if i >= i32::MIN as i64 && i <= i32::MAX as i64 => Some(i as i32),
        _ => None,
    }
}

/// Check if a key should be treated as a 32-bit integer for sorting.
/// Treat keys that:
/// 1. Can be parsed as base-10 integer
/// 2. Fit in signed 32-bit integer range
/// 3. Have no leading zeros (except "0" itself)
/// 4. "-0" is treated as a string (not an integer)
pub fn is_int32_key(s: &str) -> bool {
    try_parse_int32_key(s).is_some()
}

/// Compare two keys using orderByKey rules.
/// Integer keys (32-bit) sort numerically before string keys.
pub fn compare_keys(a: &str, b: &str) -> Ordering {
    let a_is_int = is_int32_key(a);
    let b_is_int = is_int32_key(b);

    // Integers come before strings
    if a_is_int && !b_is_int {
        return Ordering::Less;
    }
    if !a_is_int && b_is_int {
        return Ordering::Greater;
    }

    // Both integers: compare numerically
    if a_is_int && b_is_int {
        let a_int: i64 = a.parse().unwrap();
        let b_int: i64 = b.parse().unwrap();
        return a_int.cmp(&b_int);
    }

    // Both strings: compare lexicographically
    a.cmp(b)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ==========================================================================
    // Type Rank Tests
    // ==========================================================================

    #[test]
    fn test_type_rank_ordering() {
        assert!(type_rank(&json!(null)) < type_rank(&json!(false)));
        assert!(type_rank(&json!(false)) < type_rank(&json!(true)));
        assert!(type_rank(&json!(true)) < type_rank(&json!(42)));
        assert!(type_rank(&json!(42)) < type_rank(&json!("hello")));
        assert!(type_rank(&json!("hello")) < type_rank(&json!({"a": 1})));
        assert_eq!(type_rank(&json!({"a": 1})), type_rank(&json!([1, 2, 3])));
    }

    // ==========================================================================
    // Value Comparison Tests
    // ==========================================================================

    #[test]
    fn test_compare_values_different_types() {
        // null < false
        assert_eq!(compare_values(&json!(null), &json!(false)), Ordering::Less);
        // false < true
        assert_eq!(compare_values(&json!(false), &json!(true)), Ordering::Less);
        // true < number
        assert_eq!(compare_values(&json!(true), &json!(0)), Ordering::Less);
        // number < string
        assert_eq!(compare_values(&json!(100), &json!("a")), Ordering::Less);
        // string < object
        assert_eq!(compare_values(&json!("z"), &json!({})), Ordering::Less);
    }

    #[test]
    fn test_compare_values_numbers() {
        assert_eq!(compare_values(&json!(-100), &json!(0)), Ordering::Less);
        assert_eq!(compare_values(&json!(0), &json!(100)), Ordering::Less);
        assert_eq!(compare_values(&json!(100), &json!(100)), Ordering::Equal);
        assert_eq!(compare_values(&json!(1.5), &json!(2)), Ordering::Less);
        assert_eq!(compare_values(&json!(2), &json!(1.5)), Ordering::Greater);
    }

    #[test]
    fn test_compare_values_strings() {
        assert_eq!(compare_values(&json!("a"), &json!("b")), Ordering::Less);
        assert_eq!(
            compare_values(&json!("apple"), &json!("apple")),
            Ordering::Equal
        );
        assert_eq!(compare_values(&json!("b"), &json!("a")), Ordering::Greater);
        // Lexicographic: "10" < "2" (string comparison)
        assert_eq!(compare_values(&json!("10"), &json!("2")), Ordering::Less);
    }

    #[test]
    fn test_compare_values_objects_equal() {
        // Objects are equal within rank (sort by key instead)
        assert_eq!(
            compare_values(&json!({"a": 1}), &json!({"b": 2})),
            Ordering::Equal
        );
        assert_eq!(
            compare_values(&json!([1, 2]), &json!([3, 4])),
            Ordering::Equal
        );
    }

    #[test]
    fn test_compare_values_mixed_type_ordering() {
        // Full ordering test
        let values = vec![
            json!(null),
            json!(false),
            json!(true),
            json!(-5),
            json!(0),
            json!(10),
            json!("apple"),
            json!("banana"),
            json!({"nested": "object"}),
        ];

        for i in 0..values.len() {
            for j in i + 1..values.len() {
                assert_eq!(
                    compare_values(&values[i], &values[j]),
                    Ordering::Less,
                    "Expected {:?} < {:?}",
                    values[i],
                    values[j]
                );
            }
        }
    }

    // ==========================================================================
    // Key Comparison Tests (orderByKey)
    // ==========================================================================

    #[test]
    fn test_is_int32_key_valid() {
        assert!(is_int32_key("0"));
        assert!(is_int32_key("1"));
        assert!(is_int32_key("42"));
        assert!(is_int32_key("-1"));
        assert!(is_int32_key("-100"));
        assert!(is_int32_key("2147483647")); // Max int32
        assert!(is_int32_key("-2147483648")); // Min int32
    }

    #[test]
    fn test_is_int32_key_invalid() {
        assert!(!is_int32_key("")); // Empty
        assert!(!is_int32_key("007")); // Leading zero
        assert!(!is_int32_key("00")); // Leading zeros
        assert!(!is_int32_key("-007")); // Leading zero after minus
        assert!(!is_int32_key("-0")); // Negative zero
        assert!(!is_int32_key("1.5")); // Float
        assert!(!is_int32_key("1e10")); // Scientific notation
        assert!(!is_int32_key("2147483648")); // Exceeds int32 max
        assert!(!is_int32_key("-2147483649")); // Exceeds int32 min
        assert!(!is_int32_key("abc")); // Non-numeric
        assert!(!is_int32_key("-Lxyz123")); // Push ID format
        assert!(!is_int32_key("-")); // Just minus
    }

    #[test]
    fn test_compare_keys_integers() {
        assert_eq!(compare_keys("-10", "-1"), Ordering::Less);
        assert_eq!(compare_keys("-1", "0"), Ordering::Less);
        assert_eq!(compare_keys("0", "1"), Ordering::Less);
        assert_eq!(compare_keys("1", "2"), Ordering::Less);
        assert_eq!(compare_keys("2", "10"), Ordering::Less);
        assert_eq!(compare_keys("10", "10"), Ordering::Equal);
    }

    #[test]
    fn test_compare_keys_strings() {
        // String keys sort lexicographically
        assert_eq!(compare_keys("007", "abc"), Ordering::Less);
        assert_eq!(compare_keys("abc", "def"), Ordering::Less);
        assert_eq!(compare_keys("abc", "abc"), Ordering::Equal);
    }

    #[test]
    fn test_compare_keys_integers_before_strings() {
        // Integer keys come before string keys
        assert_eq!(compare_keys("1", "abc"), Ordering::Less);
        assert_eq!(compare_keys("10", "007"), Ordering::Less); // 10 is int, 007 is string
        assert_eq!(compare_keys("-1", "abc"), Ordering::Less);
    }

    #[test]
    fn test_compare_keys_full_ordering() {
        // Input keys: ["10", "2", "1", "-1", "007", "abc", "-10", "0", ""]
        // Expected: ["-10", "-1", "0", "1", "2", "10", "", "007", "abc"]
        let mut keys = vec!["10", "2", "1", "-1", "007", "abc", "-10", "0", ""];
        keys.sort_by(|a, b| compare_keys(a, b));
        assert_eq!(
            keys,
            vec!["-10", "-1", "0", "1", "2", "10", "", "007", "abc"]
        );
    }

    #[test]
    fn test_compare_keys_edge_cases() {
        // Input: ["2147483647", "2147483648", "-2147483648", "-2147483649", "007", "-007"]
        // Expected integers: -2147483648, 2147483647
        // Expected strings: -007, -2147483649, 007, 2147483648
        let mut keys = vec![
            "2147483647",
            "2147483648",
            "-2147483648",
            "-2147483649",
            "007",
            "-007",
        ];
        keys.sort_by(|a, b| compare_keys(a, b));
        assert_eq!(
            keys,
            vec![
                "-2147483648",
                "2147483647",
                "-007",
                "-2147483649",
                "007",
                "2147483648"
            ]
        );
    }
}
