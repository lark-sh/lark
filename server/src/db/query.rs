//! Query types and evaluation.
//!
//! Queries allow filtering and ordering data when subscribing or reading.
//! Supports:
//! - orderByKey, orderByChild, orderByValue, orderByPriority
//! - limitToFirst, limitToLast
//! - startAt, startAfter, endAt, endBefore, equalTo

use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

/// Maximum allowed query limit to prevent OOM from malicious queries.
/// Client sending limitToFirst(2147483647) would otherwise attempt to
/// return 2 billion results.
pub const MAX_QUERY_LIMIT: i32 = 10_000;

/// Errors that can occur when parsing query parameters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryError {
    /// Limit value exceeds MAX_QUERY_LIMIT.
    LimitTooLarge(i32),
}
use std::cmp::Ordering;

use crate::db::value::{
    ParsedKey, SortKey, compare_keys, compare_parsed_keys, compare_sort_key_to_value,
    compare_sort_keys,
};
use crate::protocol::ClientMessage;

/// Query ordering type.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum OrderBy {
    /// Sort by child keys (special integer key handling).
    Key,
    /// Sort by direct value of each child.
    Value,
    /// Sort by a nested child value (e.g., "score" or "stats/hp").
    Child(String),
    /// Sort by .priority meta-field (normalized to Child(".priority")).
    ///
    /// This is the "no query" default - it means no explicit orderBy was
    /// specified. Data without explicit orderBy sorts by key, but for query
    /// identifier purposes, Priority means "default" (no "i" field in identifier).
    #[default]
    Priority,
}

/// Query limit type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Limit {
    /// Return the first N items (lowest by sort order).
    First(usize),
    /// Return the last N items (highest by sort order).
    Last(usize),
}

/// A bound value for range queries.
#[derive(Debug, Clone, PartialEq)]
pub struct RangeBound {
    /// The value to compare against.
    pub value: Value,
    /// Optional key tie-breaker.
    pub key: Option<String>,
    /// Whether this bound is inclusive.
    pub inclusive: bool,
}

/// Range constraints for queries.
#[derive(Debug, Clone, Default)]
pub struct Range {
    /// Start bound (startAt or startAfter).
    pub start: Option<RangeBound>,
    /// End bound (endAt or endBefore).
    pub end: Option<RangeBound>,
    /// Equal to filter (combines start and end).
    pub equal_to: Option<RangeBound>,
}

impl Range {
    /// Check if any range constraints are set.
    pub fn has_constraints(&self) -> bool {
        self.start.is_some() || self.end.is_some() || self.equal_to.is_some()
    }
}

/// Query parameters for subscriptions and reads.
#[derive(Debug, Clone, Default)]
pub struct Query {
    /// Ordering method.
    pub order_by: OrderBy,
    /// Limit constraint.
    pub limit: Option<Limit>,
    /// Range constraints.
    pub range: Range,
    /// Client-provided tag for routing events.
    pub tag: Option<i32>,
}

impl Query {
    /// Check if this query has any constraints.
    /// Priority is the true default ordering - Key/Value/Child are explicit constraints.
    pub fn has_constraints(&self) -> bool {
        // Priority is the default, so Key/Value/Child all count as constraints
        !matches!(self.order_by, OrderBy::Priority)
            || self.limit.is_some()
            || self.range.has_constraints()
    }

    /// Check if this query has a limit.
    pub fn has_limit(&self) -> bool {
        self.limit.is_some()
    }

    /// Generate a unique identifier for this query.
    /// Used to distinguish multiple views on the same path with different queries.
    pub fn identifier(&self) -> String {
        if !self.has_constraints() {
            return "default".to_string();
        }

        // Build a JSON-like representation for the query identifier
        // This matches the Go implementation's QueryIdentifier format
        let mut parts = Vec::new();

        // Index (orderBy) - priority is default, don't include
        match &self.order_by {
            OrderBy::Key => parts.push("\"i\":\".key\"".to_string()),
            OrderBy::Value => parts.push("\"i\":\".value\"".to_string()),
            OrderBy::Child(path) => parts.push(format!("\"i\":\".{}\"", path)),
            OrderBy::Priority => {} // Don't add anything for priority (default)
        }

        // Start constraints
        if let Some(ref bound) = self.range.start {
            parts.push(format!("\"sin\":{}", bound.inclusive));
            parts.push(format!(
                "\"sp\":{}",
                serde_json::to_string(&bound.value).unwrap_or_default()
            ));
            if let Some(ref key) = bound.key {
                parts.push(format!("\"sn\":\"{}\"", key));
            }
        }

        // End constraints
        if let Some(ref bound) = self.range.end {
            parts.push(format!("\"ein\":{}", bound.inclusive));
            parts.push(format!(
                "\"ep\":{}",
                serde_json::to_string(&bound.value).unwrap_or_default()
            ));
            if let Some(ref key) = bound.key {
                parts.push(format!("\"en\":\"{}\"", key));
            }
        }

        // Equal to (combines start and end)
        if let Some(ref bound) = self.range.equal_to {
            let val = serde_json::to_string(&bound.value).unwrap_or_default();
            parts.push("\"ein\":true".to_string());
            parts.push(format!("\"ep\":{}", val));
            parts.push("\"sin\":true".to_string());
            parts.push(format!("\"sp\":{}", val));
            if let Some(ref key) = bound.key {
                parts.push(format!("\"en\":\"{}\"", key));
                parts.push(format!("\"sn\":\"{}\"", key));
            }
        }

        // Limit
        match self.limit {
            Some(Limit::First(n)) => {
                parts.push(format!("\"l\":{}", n));
                parts.push("\"vf\":\"l\"".to_string());
            }
            Some(Limit::Last(n)) => {
                parts.push(format!("\"l\":{}", n));
                parts.push("\"vf\":\"r\"".to_string());
            }
            None => {}
        }

        // Sort parts for consistent ordering
        parts.sort();

        format!("{{{}}}", parts.join(","))
    }
}

/// Query parameters for creating/matching views.
/// This is a flattened struct for easier construction from ClientMessage.
#[derive(Debug, Clone, Default)]
pub struct QueryParams {
    pub order_by: Option<String>,
    pub order_by_child: Option<String>,
    pub limit_to_first: Option<i32>,
    pub limit_to_last: Option<i32>,
    pub start_at: Option<Value>,
    pub start_at_key: Option<String>,
    pub start_after: Option<Value>,
    pub start_after_key: Option<String>,
    pub end_at: Option<Value>,
    pub end_at_key: Option<String>,
    pub end_before: Option<Value>,
    pub end_before_key: Option<String>,
    pub equal_to: Option<Value>,
    pub equal_to_key: Option<String>,
    pub tag: Option<i32>,
}

impl QueryParams {
    /// Create QueryParams from a ClientMessage.
    pub fn from_message(msg: &ClientMessage) -> Option<Self> {
        // Check if any query params are present
        if msg.order_by.is_none()
            && msg.order_by_child.is_none()
            && msg.limit_to_first.is_none()
            && msg.limit_to_last.is_none()
            && msg.start_at.is_none()
            && msg.start_at_key.is_none()
            && msg.start_after.is_none()
            && msg.start_after_key.is_none()
            && msg.end_at.is_none()
            && msg.end_at_key.is_none()
            && msg.end_before.is_none()
            && msg.end_before_key.is_none()
            && msg.equal_to.is_none()
            && msg.equal_to_key.is_none()
            && msg.tag.is_none()
        {
            return None;
        }

        Some(Self {
            order_by: msg.order_by.clone(),
            order_by_child: msg.order_by_child.clone(),
            limit_to_first: msg.limit_to_first,
            limit_to_last: msg.limit_to_last,
            start_at: msg.start_at.clone(),
            start_at_key: msg.start_at_key.clone(),
            start_after: msg.start_after.clone(),
            start_after_key: msg.start_after_key.clone(),
            end_at: msg.end_at.clone(),
            end_at_key: msg.end_at_key.clone(),
            end_before: msg.end_before.clone(),
            end_before_key: msg.end_before_key.clone(),
            equal_to: msg.equal_to.clone(),
            equal_to_key: msg.equal_to_key.clone(),
            tag: msg.tag,
        })
    }

    /// Convert to a Query.
    ///
    /// Returns an error if query parameters exceed safety limits (e.g., limit > 10,000).
    pub fn to_query(&self) -> Result<Query, QueryError> {
        // Validate limits before processing
        if let Some(n) = self.limit_to_first
            && n > MAX_QUERY_LIMIT
        {
            return Err(QueryError::LimitTooLarge(n));
        }
        if let Some(n) = self.limit_to_last
            && n > MAX_QUERY_LIMIT
        {
            return Err(QueryError::LimitTooLarge(n));
        }

        // Determine order by
        let order_by = if let Some(ref child) = self.order_by_child {
            OrderBy::Child(child.clone())
        } else if let Some(ref order) = self.order_by {
            match order.as_str() {
                "key" => OrderBy::Key,
                "value" => OrderBy::Value,
                "priority" => OrderBy::Child(".priority".to_string()), // Normalize priority
                _ => OrderBy::Key,
            }
        } else {
            // Default behavior: Priority is the true "no explicit orderBy" default.
            // This maps to "default" in the query identifier.
            OrderBy::Priority
        };

        // Determine limit
        let limit = if let Some(n) = self.limit_to_first {
            Some(Limit::First(n as usize))
        } else {
            self.limit_to_last.map(|n| Limit::Last(n as usize))
        };

        // Determine range
        let start = if self.start_at.is_some() || self.start_at_key.is_some() {
            Some(RangeBound {
                value: self.start_at.clone().unwrap_or(Value::Null),
                key: self.start_at_key.clone(),
                inclusive: true,
            })
        } else if self.start_after.is_some() || self.start_after_key.is_some() {
            Some(RangeBound {
                value: self.start_after.clone().unwrap_or(Value::Null),
                key: self.start_after_key.clone(),
                inclusive: false,
            })
        } else {
            None
        };

        let end = if self.end_at.is_some() || self.end_at_key.is_some() {
            Some(RangeBound {
                value: self.end_at.clone().unwrap_or(Value::Null),
                key: self.end_at_key.clone(),
                inclusive: true,
            })
        } else if self.end_before.is_some() || self.end_before_key.is_some() {
            Some(RangeBound {
                value: self.end_before.clone().unwrap_or(Value::Null),
                key: self.end_before_key.clone(),
                inclusive: false,
            })
        } else {
            None
        };

        let equal_to = if self.equal_to.is_some() || self.equal_to_key.is_some() {
            Some(RangeBound {
                value: self.equal_to.clone().unwrap_or(Value::Null),
                key: self.equal_to_key.clone(),
                inclusive: true,
            })
        } else {
            None
        };

        Ok(Query {
            order_by,
            limit,
            range: Range {
                start,
                end,
                equal_to,
            },
            tag: self.tag,
        })
    }

    /// Generate a unique identifier for these query params.
    /// Returns "default" if query parsing fails (shouldn't happen for valid params).
    pub fn identifier(&self) -> String {
        self.to_query()
            .map(|q| q.identifier())
            .unwrap_or_else(|_| "default".to_string())
    }

    /// Build a query object for use in rules evaluation (query.orderByChild, etc.).
    /// Returns the properties as a HashMap matching query-based rules variables.
    pub fn to_rules_query(&self) -> Arc<HashMap<String, Value>> {
        let mut map = HashMap::new();

        // orderBy* — booleans for key/value/priority, string for child
        if let Some(ref child) = self.order_by_child {
            map.insert("orderByChild".to_string(), Value::String(child.clone()));
        } else if let Some(ref order) = self.order_by {
            match order.as_str() {
                "key" => {
                    map.insert("orderByKey".to_string(), Value::Bool(true));
                }
                "value" => {
                    map.insert("orderByValue".to_string(), Value::Bool(true));
                }
                "priority" => {
                    map.insert("orderByPriority".to_string(), Value::Bool(true));
                }
                _ => {}
            }
        }

        // Limits
        if let Some(n) = self.limit_to_first {
            map.insert("limitToFirst".to_string(), Value::Number(n.into()));
        }
        if let Some(n) = self.limit_to_last {
            map.insert("limitToLast".to_string(), Value::Number(n.into()));
        }

        // Range bounds — use the value directly
        if let Some(ref v) = self.start_at {
            map.insert("startAt".to_string(), v.clone());
        } else if let Some(ref v) = self.start_after {
            map.insert("startAt".to_string(), v.clone());
        }
        if let Some(ref v) = self.end_at {
            map.insert("endAt".to_string(), v.clone());
        } else if let Some(ref v) = self.end_before {
            map.insert("endAt".to_string(), v.clone());
        }
        if let Some(ref v) = self.equal_to {
            map.insert("equalTo".to_string(), v.clone());
        }

        Arc::new(map)
    }

    #[allow(dead_code)]
    fn has_any_constraint(&self) -> bool {
        self.limit_to_first.is_some()
            || self.limit_to_last.is_some()
            || self.start_at.is_some()
            || self.start_at_key.is_some()
            || self.start_after.is_some()
            || self.start_after_key.is_some()
            || self.end_at.is_some()
            || self.end_at_key.is_some()
            || self.end_before.is_some()
            || self.end_before_key.is_some()
            || self.equal_to.is_some()
            || self.equal_to_key.is_some()
    }
}

/// A lightweight entry for sorting (key + sort value only, no full value).
/// Used for efficient query evaluation without copying entire child values.
///
/// OPTIMIZATION: Uses SortKey instead of Value to avoid string allocation.
/// SortKey::String holds Arc<str> which is O(1) to clone.
#[derive(Debug, Clone)]
pub struct SortEntry {
    pub key: String,
    pub parsed_key: ParsedKey,
    pub sort_value: Option<SortKey>,
}

impl SortEntry {
    pub fn new(key: String, sort_value: Option<SortKey>) -> Self {
        let parsed_key = ParsedKey::parse(&key);
        Self {
            key,
            parsed_key,
            sort_value,
        }
    }
}

/// Apply a query to sort entries (lightweight), returning filtered and sorted keys.
/// This is much more efficient than apply_query because it doesn't require full values.
pub fn apply_query_to_sort_entries(entries: Vec<SortEntry>, query: &Query) -> Vec<String> {
    let mut entries = entries;

    // Sort entries
    sort_sort_entries(&mut entries, &query.order_by);

    // Apply range filter
    if query.range.has_constraints() {
        entries = filter_sort_entries_by_range(entries, query);
    }

    // Apply limit
    if let Some(limit) = query.limit {
        entries = apply_sort_entry_limit(entries, limit);
    }

    // Return just the keys
    entries.into_iter().map(|e| e.key).collect()
}

/// Sort SortEntries according to the query's order by clause.
fn sort_sort_entries(entries: &mut [SortEntry], order_by: &OrderBy) {
    entries.sort_by(|a, b| compare_sort_entries(a, b, order_by));
}

/// Compare two SortEntries according to the order by clause.
/// Uses cached ParsedKey for O(1) key comparisons instead of re-parsing.
fn compare_sort_entries(a: &SortEntry, b: &SortEntry, order_by: &OrderBy) -> Ordering {
    match order_by {
        OrderBy::Key => compare_parsed_keys(&a.parsed_key, &b.parsed_key),
        OrderBy::Value | OrderBy::Child(_) | OrderBy::Priority => {
            // Sort by sort_value, then by key
            let cmp = match (&a.sort_value, &b.sort_value) {
                (None, None) => Ordering::Equal,
                (None, Some(SortKey::Null)) => Ordering::Equal,
                (Some(SortKey::Null), None) => Ordering::Equal,
                (None, Some(_)) => Ordering::Less,
                (Some(_), None) => Ordering::Greater,
                (Some(va), Some(vb)) => compare_sort_keys(va, vb),
            };
            if cmp == Ordering::Equal {
                compare_parsed_keys(&a.parsed_key, &b.parsed_key)
            } else {
                cmp
            }
        }
    }
}

/// Filter SortEntries by range constraints.
fn filter_sort_entries_by_range(entries: Vec<SortEntry>, query: &Query) -> Vec<SortEntry> {
    entries
        .into_iter()
        .filter(|entry| sort_entry_in_range(entry, query))
        .collect()
}

/// Check if a SortEntry passes the range filter.
/// OPTIMIZATION: Uses compare_sort_key_value for efficient SortKey comparison.
fn sort_entry_in_range(entry: &SortEntry, query: &Query) -> bool {
    is_in_range(entry.sort_value.as_ref(), &entry.key, query)
}

/// Check if an item with the given sort value and key passes the range filter.
/// This is the core range-checking logic used for both full query evaluation
/// and incremental view updates.
pub fn is_in_range(sort_value: Option<&SortKey>, key: &str, query: &Query) -> bool {
    let key_sort = query.order_by == OrderBy::Key;

    // Check equalTo first
    if let Some(ref equal_to) = query.range.equal_to {
        if key_sort {
            if key != equal_to.value.as_str().unwrap_or("") {
                return false;
            }
            if let Some(ref eq_key) = equal_to.key
                && key != eq_key
            {
                return false;
            }
        } else {
            let cmp = compare_sort_key_value(sort_value, &equal_to.value);
            if cmp != Ordering::Equal {
                return false;
            }
            if let Some(ref eq_key) = equal_to.key
                && key != eq_key
            {
                return false;
            }
        }
        return true;
    }

    // Check start bound
    if let Some(ref start) = query.range.start {
        if key_sort {
            if start.value.is_null()
                && let Some(start_bound_key) = start.key.as_ref()
            {
                let cmp = compare_keys(key, start_bound_key);
                if start.inclusive {
                    if cmp == Ordering::Less {
                        return false;
                    }
                } else if cmp != Ordering::Greater {
                    return false;
                }
            } else if let Some(start_key) = start.value.as_str() {
                let cmp = compare_keys(key, start_key);
                if start.inclusive {
                    if cmp == Ordering::Less {
                        return false;
                    }
                } else if cmp != Ordering::Greater {
                    return false;
                }
            }
        } else {
            let cmp = compare_sort_key_value(sort_value, &start.value);
            if start.inclusive {
                if cmp == Ordering::Less {
                    return false;
                }
                if cmp == Ordering::Equal
                    && let Some(ref start_bound_key) = start.key
                    && compare_keys(key, start_bound_key) == Ordering::Less
                {
                    return false;
                }
            } else {
                if cmp == Ordering::Less {
                    return false;
                }
                if cmp == Ordering::Equal {
                    if let Some(ref start_bound_key) = start.key {
                        if compare_keys(key, start_bound_key) != Ordering::Greater {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
            }
        }
    }

    // Check end bound
    if let Some(ref end) = query.range.end {
        if key_sort {
            if end.value.is_null()
                && let Some(end_bound_key) = end.key.as_ref()
            {
                let cmp = compare_keys(key, end_bound_key);
                if end.inclusive {
                    if cmp == Ordering::Greater {
                        return false;
                    }
                } else if cmp != Ordering::Less {
                    return false;
                }
            } else if let Some(end_key) = end.value.as_str() {
                let cmp = compare_keys(key, end_key);
                if end.inclusive {
                    if cmp == Ordering::Greater {
                        return false;
                    }
                } else if cmp != Ordering::Less {
                    return false;
                }
            }
        } else {
            let cmp = compare_sort_key_value(sort_value, &end.value);
            if end.inclusive {
                if cmp == Ordering::Greater {
                    return false;
                }
                if cmp == Ordering::Equal
                    && let Some(ref end_bound_key) = end.key
                    && compare_keys(key, end_bound_key) == Ordering::Greater
                {
                    return false;
                }
            } else {
                if cmp == Ordering::Greater {
                    return false;
                }
                if cmp == Ordering::Equal {
                    if let Some(ref end_bound_key) = end.key {
                        if compare_keys(key, end_bound_key) != Ordering::Less {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
            }
        }
    }

    true
}

/// Apply a limit to SortEntries.
fn apply_sort_entry_limit(entries: Vec<SortEntry>, limit: Limit) -> Vec<SortEntry> {
    match limit {
        Limit::First(n) => entries.into_iter().take(n).collect(),
        Limit::Last(n) => {
            let len = entries.len();
            if n >= len {
                entries
            } else {
                entries.into_iter().skip(len - n).collect()
            }
        }
    }
}

/// Compare a SortKey (which may be None/null) against a bound value.
/// OPTIMIZATION: Uses SortKey to avoid allocation during sorting.
/// Used by SortEntry (the lightweight version).
fn compare_sort_key_value(sort_value: Option<&SortKey>, bound_value: &Value) -> Ordering {
    match sort_value {
        None => {
            if bound_value.is_null() {
                Ordering::Equal
            } else {
                Ordering::Less // Missing values sort first
            }
        }
        Some(SortKey::Null) => {
            if bound_value.is_null() {
                Ordering::Equal
            } else {
                Ordering::Less // Null sorts first
            }
        }
        Some(sk) => compare_sort_key_to_value(sk, bound_value),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ==========================================================================
    // Query Identifier Tests
    // ==========================================================================

    #[test]
    fn test_query_identifier_default() {
        let query = Query::default();
        assert_eq!(query.identifier(), "default");
    }

    #[test]
    fn test_query_identifier_with_order_by() {
        let query = Query {
            order_by: OrderBy::Child("score".to_string()),
            ..Default::default()
        };
        assert!(query.identifier().contains("\"i\":\".score\""));
    }

    #[test]
    fn test_query_identifier_with_limit() {
        let query = Query {
            order_by: OrderBy::Key,
            limit: Some(Limit::First(10)),
            ..Default::default()
        };
        let id = query.identifier();
        assert!(id.contains("\"l\":10"));
        assert!(id.contains("\"vf\":\"l\""));
    }

    // ==========================================================================
    // Query Params Tests
    // ==========================================================================

    #[test]
    fn test_query_params_from_message() {
        let msg = ClientMessage {
            op: "sb".to_string(),
            order_by_child: Some("score".to_string()),
            limit_to_first: Some(10),
            ..Default::default()
        };

        let params = QueryParams::from_message(&msg).unwrap();
        assert_eq!(params.order_by_child, Some("score".to_string()));
        assert_eq!(params.limit_to_first, Some(10));
    }

    #[test]
    fn test_query_params_to_query() {
        let params = QueryParams {
            order_by_child: Some("score".to_string()),
            limit_to_first: Some(5),
            start_at: Some(json!(100)),
            ..Default::default()
        };

        let query = params.to_query().unwrap();
        assert_eq!(query.order_by, OrderBy::Child("score".to_string()));
        assert_eq!(query.limit, Some(Limit::First(5)));
        assert!(query.range.start.is_some());
    }

    #[test]
    fn test_query_params_limit_too_large() {
        let params = QueryParams {
            limit_to_first: Some(MAX_QUERY_LIMIT + 1),
            ..Default::default()
        };

        assert!(matches!(
            params.to_query(),
            Err(QueryError::LimitTooLarge(_))
        ));

        let params = QueryParams {
            limit_to_last: Some(i32::MAX),
            ..Default::default()
        };

        assert!(matches!(
            params.to_query(),
            Err(QueryError::LimitTooLarge(_))
        ));
    }

    #[test]
    fn test_query_params_none_for_empty() {
        let msg = ClientMessage {
            op: "sb".to_string(),
            ..Default::default()
        };

        let params = QueryParams::from_message(&msg);
        assert!(params.is_none());
    }

    // ==========================================================================
    // Apply Query Tests (using SortEntry - the production code path)
    // ==========================================================================

    #[test]
    fn test_apply_query_order_by_key() {
        let entries = vec![
            SortEntry::new("b".to_string(), None),
            SortEntry::new("a".to_string(), None),
            SortEntry::new("c".to_string(), None),
        ];

        let query = Query {
            order_by: OrderBy::Key,
            ..Default::default()
        };

        let keys = apply_query_to_sort_entries(entries, &query);
        assert_eq!(keys, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_apply_query_order_by_value() {
        let entries = vec![
            SortEntry::new("a".to_string(), Some(SortKey::Number(30.0))),
            SortEntry::new("b".to_string(), Some(SortKey::Number(10.0))),
            SortEntry::new("c".to_string(), Some(SortKey::Number(20.0))),
        ];

        let query = Query {
            order_by: OrderBy::Value,
            ..Default::default()
        };

        let keys = apply_query_to_sort_entries(entries, &query);
        assert_eq!(keys, vec!["b", "c", "a"]); // 10, 20, 30
    }

    #[test]
    fn test_apply_query_order_by_child() {
        let entries = vec![
            SortEntry::new("alice".to_string(), Some(SortKey::Number(200.0))),
            SortEntry::new("bob".to_string(), Some(SortKey::Number(100.0))),
            SortEntry::new("charlie".to_string(), Some(SortKey::Number(150.0))),
        ];

        let query = Query {
            order_by: OrderBy::Child("score".to_string()),
            ..Default::default()
        };

        let keys = apply_query_to_sort_entries(entries, &query);
        assert_eq!(keys, vec!["bob", "charlie", "alice"]); // 100, 150, 200
    }

    #[test]
    fn test_apply_query_limit_to_first() {
        let entries = vec![
            SortEntry::new("a".to_string(), None),
            SortEntry::new("b".to_string(), None),
            SortEntry::new("c".to_string(), None),
        ];

        let query = Query {
            order_by: OrderBy::Key,
            limit: Some(Limit::First(2)),
            ..Default::default()
        };

        let keys = apply_query_to_sort_entries(entries, &query);
        assert_eq!(keys, vec!["a", "b"]);
    }

    #[test]
    fn test_apply_query_limit_to_last() {
        let entries = vec![
            SortEntry::new("a".to_string(), None),
            SortEntry::new("b".to_string(), None),
            SortEntry::new("c".to_string(), None),
        ];

        let query = Query {
            order_by: OrderBy::Key,
            limit: Some(Limit::Last(2)),
            ..Default::default()
        };

        let keys = apply_query_to_sort_entries(entries, &query);
        assert_eq!(keys, vec!["b", "c"]);
    }

    #[test]
    fn test_apply_query_start_at() {
        let entries = vec![
            SortEntry::new("a".to_string(), Some(SortKey::Number(10.0))),
            SortEntry::new("b".to_string(), Some(SortKey::Number(20.0))),
            SortEntry::new("c".to_string(), Some(SortKey::Number(30.0))),
        ];

        let query = Query {
            order_by: OrderBy::Child("value".to_string()),
            range: Range {
                start: Some(RangeBound {
                    value: json!(20),
                    key: None,
                    inclusive: true,
                }),
                ..Default::default()
            },
            ..Default::default()
        };

        let keys = apply_query_to_sort_entries(entries, &query);
        assert_eq!(keys, vec!["b", "c"]); // 20, 30 (>= 20)
    }

    #[test]
    fn test_apply_query_end_at() {
        let entries = vec![
            SortEntry::new("a".to_string(), Some(SortKey::Number(10.0))),
            SortEntry::new("b".to_string(), Some(SortKey::Number(20.0))),
            SortEntry::new("c".to_string(), Some(SortKey::Number(30.0))),
        ];

        let query = Query {
            order_by: OrderBy::Child("value".to_string()),
            range: Range {
                end: Some(RangeBound {
                    value: json!(20),
                    key: None,
                    inclusive: true,
                }),
                ..Default::default()
            },
            ..Default::default()
        };

        let keys = apply_query_to_sort_entries(entries, &query);
        assert_eq!(keys, vec!["a", "b"]); // 10, 20 (<= 20)
    }

    #[test]
    fn test_apply_query_equal_to() {
        let entries = vec![
            SortEntry::new("a".to_string(), Some(SortKey::Number(100.0))),
            SortEntry::new("b".to_string(), Some(SortKey::Number(100.0))),
            SortEntry::new("c".to_string(), Some(SortKey::Number(200.0))),
        ];

        let query = Query {
            order_by: OrderBy::Child("value".to_string()),
            range: Range {
                equal_to: Some(RangeBound {
                    value: json!(100),
                    key: None,
                    inclusive: true,
                }),
                ..Default::default()
            },
            ..Default::default()
        };

        let keys = apply_query_to_sort_entries(entries, &query);
        assert_eq!(keys, vec!["a", "b"]); // Only entries with value 100
    }

    #[test]
    fn test_apply_query_mixed_types() {
        // Test type hierarchy: null < false < true < numbers < strings < objects
        let entries = vec![
            SortEntry::new("a".to_string(), Some(SortKey::Null)),
            SortEntry::new("b".to_string(), Some(SortKey::Bool(false))),
            SortEntry::new("c".to_string(), Some(SortKey::Bool(true))),
            SortEntry::new("d".to_string(), Some(SortKey::Number(10.0))),
            SortEntry::new("e".to_string(), Some(SortKey::String("hello".into()))),
            SortEntry::new("f".to_string(), Some(SortKey::Object)), // Object value
            SortEntry::new("j".to_string(), None),                  // Missing score = null
        ];

        let query = Query {
            order_by: OrderBy::Child("score".to_string()),
            ..Default::default()
        };

        let keys = apply_query_to_sort_entries(entries, &query);
        // missing/null (a, j sorted by key), false (b), true (c), number (d), string (e), object (f)
        assert_eq!(keys, vec!["a", "j", "b", "c", "d", "e", "f"]);
    }

    #[test]
    fn test_apply_query_tie_breaker_by_key() {
        let entries = vec![
            SortEntry::new("charlie".to_string(), Some(SortKey::Number(100.0))),
            SortEntry::new("alice".to_string(), Some(SortKey::Number(100.0))),
            SortEntry::new("bob".to_string(), Some(SortKey::Number(100.0))),
        ];

        let query = Query {
            order_by: OrderBy::Child("score".to_string()),
            ..Default::default()
        };

        let keys = apply_query_to_sort_entries(entries, &query);
        // Same score, sorted by key
        assert_eq!(keys, vec!["alice", "bob", "charlie"]);
    }

    #[test]
    fn test_apply_query_integer_keys() {
        let entries = vec![
            SortEntry::new("10".to_string(), None),
            SortEntry::new("2".to_string(), None),
            SortEntry::new("1".to_string(), None),
            SortEntry::new("abc".to_string(), None),
        ];

        let query = Query {
            order_by: OrderBy::Key,
            ..Default::default()
        };

        let keys = apply_query_to_sort_entries(entries, &query);
        // Integer keys sorted numerically, then string keys lexicographically
        assert_eq!(keys, vec!["1", "2", "10", "abc"]);
    }
}
