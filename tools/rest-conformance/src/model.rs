//! A plain JSON model of the database, and what a client should read from it.
//!
//! Stored values have no arrays, nulls or empty objects. `render` produces the
//! JSON a client sees, including array coercion.

use serde_json::{json, Map, Value};
use std::cmp::Ordering;

/// What a written value is stored as: arrays become objects keyed by index, and
/// nulls and the containers they leave empty disappear.
pub fn store(value: &Value) -> Value {
    let map: Map<String, Value> = match value {
        Value::Object(map) => map
            .iter()
            .map(|(k, v)| (k.clone(), store(v)))
            .filter(|(_, v)| !v.is_null())
            .collect(),
        Value::Array(items) => items
            .iter()
            .enumerate()
            .map(|(i, v)| (i.to_string(), store(v)))
            .filter(|(_, v)| !v.is_null())
            .collect(),
        primitive => return primitive.clone(),
    };
    if map.is_empty() {
        Value::Null
    } else {
        Value::Object(map)
    }
}

/// Writes a stored value at a path. Null deletes, and a node left with no
/// children goes with it. Writing below a primitive replaces it; deleting below
/// one changes nothing.
pub fn write(node: &mut Value, segments: &[String], value: &Value) {
    let Some((first, rest)) = segments.split_first() else {
        *node = value.clone();
        return;
    };
    if !node.is_object() {
        if value.is_null() {
            return;
        }
        *node = json!({});
    }
    let map = node.as_object_mut().unwrap();
    let child = map.entry(first.clone()).or_insert(Value::Null);
    write(child, rest, value);
    if child.is_null() {
        map.remove(first);
    }
    if map.is_empty() {
        *node = Value::Null;
    }
}

pub fn read<'a>(root: &'a Value, segments: &[String]) -> &'a Value {
    let mut node = root;
    for segment in segments {
        match node.get(segment) {
            Some(child) => node = child,
            None => return &Value::Null,
        }
    }
    node
}

fn array_index(key: &str) -> Option<usize> {
    if key == "0" || (!key.starts_with('0') && key.bytes().all(|b| b.is_ascii_digit())) {
        key.parse().ok()
    } else {
        None
    }
}

/// The JSON a client reads for a stored value. An object reads as an array when
/// every key is a non-negative integer and more than half of the indexes up to
/// the largest one are present.
pub fn render(value: &Value) -> Value {
    let Some(map) = value.as_object() else {
        return value.clone();
    };
    let indexes: Option<Vec<usize>> = map.keys().map(|k| array_index(k)).collect();
    if let Some(indexes) = indexes {
        let max = indexes.iter().copied().max().unwrap_or(0);
        if max < 2 * indexes.len() {
            let mut items = vec![Value::Null; max + 1];
            for (key, child) in map {
                items[array_index(key).unwrap()] = render(child);
            }
            return Value::Array(items);
        }
    }
    Value::Object(map.iter().map(|(k, v)| (k.clone(), render(v))).collect())
}

/// Undoes array coercion on the top level of a response, for results whose shape
/// the docs leave open.
pub fn as_object(value: &Value) -> Value {
    match value {
        Value::Array(items) => Value::Object(
            items
                .iter()
                .enumerate()
                .filter(|(_, v)| !v.is_null())
                .map(|(i, v)| (i.to_string(), v.clone()))
                .collect(),
        ),
        other => other.clone(),
    }
}

/// `$key` order: 32-bit integers ascending, then strings.
pub fn compare_keys(a: &str, b: &str) -> Ordering {
    match (a.parse::<i32>().ok(), b.parse::<i32>().ok()) {
        (Some(a), Some(b)) => a.cmp(&b),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => a.cmp(b),
    }
}

fn type_rank(value: &Value) -> u8 {
    match value {
        Value::Null => 0,
        Value::Bool(false) => 1,
        Value::Bool(true) => 2,
        Value::Number(_) => 3,
        Value::String(_) => 4,
        _ => 5,
    }
}

/// Value order: null, false, true, numbers, strings, objects.
pub fn compare_values(a: &Value, b: &Value) -> Ordering {
    type_rank(a).cmp(&type_rank(b)).then_with(|| match (a, b) {
        (Value::Number(a), Value::Number(b)) => a
            .as_f64()
            .unwrap()
            .partial_cmp(&b.as_f64().unwrap())
            .unwrap(),
        (Value::String(a), Value::String(b)) => a.cmp(b),
        _ => Ordering::Equal,
    })
}

#[derive(Debug, Clone, PartialEq)]
pub enum OrderBy {
    Key,
    Value,
    Child(String),
}

impl OrderBy {
    pub fn parameter(&self) -> String {
        match self {
            Self::Key => "\"$key\"".to_string(),
            Self::Value => "\"$value\"".to_string(),
            Self::Child(key) => format!("\"{key}\""),
        }
    }

    fn sort_value<'a>(&self, child: &'a Value) -> &'a Value {
        match self {
            Self::Key | Self::Value => child,
            Self::Child(key) => child.get(key).unwrap_or(&Value::Null),
        }
    }

    /// Compares a child with a query bound.
    fn compare_to_bound(&self, key: &str, child: &Value, bound: &Value) -> Ordering {
        match (self, bound) {
            (Self::Key, Value::String(bound)) => compare_keys(key, bound),
            _ => compare_values(self.sort_value(child), bound),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Query {
    pub order: OrderBy,
    pub start: Option<Value>,
    pub end: Option<Value>,
    pub equal: Option<Value>,
    /// True for limitToFirst, false for limitToLast.
    pub limit: Option<(bool, usize)>,
}

impl Query {
    /// The children a query selects from a stored object, as an object.
    pub fn select(&self, node: &Value) -> Value {
        let empty = Map::new();
        let map = node.as_object().unwrap_or(&empty);
        let mut keys: Vec<&String> = map
            .keys()
            .filter(|k| {
                let child = &map[k.as_str()];
                let compare = |bound: &Value| self.order.compare_to_bound(k, child, bound);
                self.start.as_ref().is_none_or(|b| compare(b).is_ge())
                    && self.end.as_ref().is_none_or(|b| compare(b).is_le())
                    && self.equal.as_ref().is_none_or(|b| compare(b).is_eq())
            })
            .collect();
        keys.sort_by(|a, b| {
            let ordered = match self.order {
                OrderBy::Key => Ordering::Equal,
                _ => compare_values(
                    self.order.sort_value(&map[a.as_str()]),
                    self.order.sort_value(&map[b.as_str()]),
                ),
            };
            ordered.then_with(|| compare_keys(a, b))
        });
        match self.limit {
            Some((true, n)) => keys.truncate(n),
            Some((false, n)) => {
                let skip = keys.len().saturating_sub(n);
                keys.drain(..skip);
            }
            None => {}
        }
        Value::Object(
            keys.into_iter()
                .map(|k| (k.clone(), render(&map[k])))
                .collect(),
        )
    }

    pub fn parameters(&self) -> Vec<(&'static str, String)> {
        let mut parameters = vec![("orderBy", self.order.parameter())];
        for (name, bound) in [
            ("startAt", &self.start),
            ("endAt", &self.end),
            ("equalTo", &self.equal),
        ] {
            if let Some(bound) = bound {
                parameters.push((name, bound.to_string()));
            }
        }
        match self.limit {
            Some((true, n)) => parameters.push(("limitToFirst", n.to_string())),
            Some((false, n)) => parameters.push(("limitToLast", n.to_string())),
            None => {}
        }
        parameters
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_store_drops_nulls_and_empties() {
        assert_eq!(store(&json!({"a": null, "b": {"c": {}}})), Value::Null);
        assert_eq!(store(&json!({"a": [], "b": 1})), json!({"b": 1}));
        assert_eq!(store(&json!([null, 1])), json!({"1": 1}));
    }

    #[test]
    fn test_render_coerces_arrays_when_dense_enough() {
        assert_eq!(render(&json!({"0": "a", "1": "b"})), json!(["a", "b"]));
        assert_eq!(render(&json!({"0": 1, "2": 3})), json!([1, null, 3]));
        assert_eq!(render(&json!({"1": "x"})), json!([null, "x"]));
        assert_eq!(render(&json!({"3": "x"})), json!({"3": "x"}));
        assert_eq!(render(&json!({"0": 1, "x": 2})), json!({"0": 1, "x": 2}));
        assert_eq!(render(&json!({"01": 1})), json!({"01": 1}));
    }

    #[test]
    fn test_write_below_primitive() {
        let mut root = json!({"a": 1});
        write(&mut root, &["a".to_string(), "b".to_string()], &Value::Null);
        assert_eq!(root, json!({"a": 1}));
        write(&mut root, &["a".to_string(), "b".to_string()], &json!(2));
        assert_eq!(root, json!({"a": {"b": 2}}));
    }

    #[test]
    fn test_query_orders_and_limits() {
        let node = json!({"b": 2, "a": 2, "c": 1, "10": "x", "9": false});
        let query = Query {
            order: OrderBy::Value,
            start: None,
            end: None,
            equal: None,
            limit: Some((true, 3)),
        };
        assert_eq!(query.select(&node), json!({"9": false, "c": 1, "a": 2}));
        let query = Query {
            order: OrderBy::Key,
            start: Some(json!("10")),
            end: Some(json!("a")),
            equal: None,
            limit: None,
        };
        assert_eq!(query.select(&node), json!({"10": "x", "a": 2}));
    }
}
