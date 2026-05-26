//! RuleSet and RuleNode parsing from JSON.

use super::expr::{Expr, parse as parse_expr};
use serde_json::Value as JsonValue;
use std::collections::HashMap;

/// RuleSet represents compiled security rules for a database.
#[derive(Debug, Clone)]
pub struct RuleSet {
    root: Option<RuleNode>,
    /// True if any rule expression in this set references `query.`.
    /// Used to skip building the query HashMap when no rules need it.
    pub uses_query: bool,
}

/// RuleNode represents a node in the rules tree.
/// Each node can have .read, .write, .validate expressions and child nodes.
#[derive(Debug, Clone, Default)]
pub struct RuleNode {
    /// Compiled .read expression (None if not specified at this node).
    pub read: Option<CompiledExpr>,
    /// Compiled .write expression.
    pub write: Option<CompiledExpr>,
    /// Compiled .validate expression.
    pub validate: Option<CompiledExpr>,
    /// Volatile flag - if true, this path uses unreliable transport.
    pub volatile: bool,
    /// True if this node or any descendant has a .validate rule.
    /// Used to skip the recursive validate_children walk when no validation exists.
    pub has_validate_below: bool,
    /// Child nodes by exact key.
    pub children: HashMap<String, RuleNode>,
    /// Wildcard child (e.g., "$playerId") - captures the path segment.
    pub wildcard: Option<Box<RuleNode>>,
    /// The wildcard variable name without $ (e.g., "playerId").
    pub wildcard_name: Option<String>,
}

/// CompiledExpr holds a pre-compiled expression.
#[derive(Debug, Clone)]
pub struct CompiledExpr {
    /// Original expression text.
    pub source: String,
    /// Compiled AST.
    pub ast: Expr,
    /// True if the expression only uses auth, $wildcards, and literals.
    /// Simple expressions don't access newData, data, or root, so we can skip
    /// creating expensive Snapshot objects when evaluating them.
    pub is_simple: bool,
    /// True if the expression references newData.
    /// Used to skip expensive newData computation at ancestor levels when not needed.
    pub uses_new_data: bool,
}

impl RuleSet {
    /// Parses a rules JSON string and compiles all expressions.
    /// The input should be the value of the "rules" key from a rules file.
    /// Supports JavaScript-style comments (// and /* */)
    pub fn parse(rules_json: &str) -> Result<Self, String> {
        // Strip comments before parsing
        let stripped = strip_comments(rules_json);

        let raw: JsonValue =
            serde_json::from_str(&stripped).map_err(|e| format!("parse rules JSON: {}", e))?;

        let raw_obj = raw
            .as_object()
            .ok_or_else(|| "rules must be a JSON object".to_string())?;

        // Check for "rules" wrapper
        let rules_obj = if let Some(JsonValue::Object(rules_map)) = raw_obj.get("rules") {
            rules_map
        } else {
            raw_obj
        };

        let mut root = parse_node(rules_obj, "/")?;
        compute_has_validate_below(&mut root);
        let uses_query = node_uses_query(&root);

        Ok(RuleSet {
            root: Some(root),
            uses_query,
        })
    }

    /// Creates an empty rule set (allows all operations).
    pub fn empty() -> Self {
        RuleSet {
            root: None,
            uses_query: false,
        }
    }

    /// Returns the root rule node.
    pub fn root(&self) -> Option<&RuleNode> {
        self.root.as_ref()
    }

    /// Traverses the rule tree following a path and returns the node
    /// along with captured wildcard values.
    pub fn get_node(&self, path_segments: &[&str]) -> (Option<&RuleNode>, HashMap<String, String>) {
        let mut captures = HashMap::new();

        let root = match &self.root {
            Some(r) => r,
            None => return (None, captures),
        };

        let mut node = root;

        for segment in path_segments {
            if segment.is_empty() {
                continue;
            }

            // Try exact match first
            if let Some(child) = node.children.get(*segment) {
                node = child;
                continue;
            }

            // Try wildcard
            if let Some(ref wildcard) = node.wildcard {
                if let Some(ref name) = node.wildcard_name {
                    captures.insert(name.clone(), segment.to_string());
                }
                node = wildcard;
                continue;
            }

            // No match - return last matched node
            return (Some(node), captures);
        }

        (Some(node), captures)
    }

    /// Returns all rule nodes on the path from root to the target,
    /// along with wildcard captures. This is used to evaluate cascading .read/.write rules.
    pub fn find_rules_on_path(
        &self,
        path_segments: &[&str],
    ) -> (Vec<&RuleNode>, HashMap<String, String>) {
        let mut nodes = Vec::with_capacity(path_segments.len() + 1);
        let mut captures = HashMap::new();

        let root = match &self.root {
            Some(r) => r,
            None => return (nodes, captures),
        };

        nodes.push(root);
        let mut node = root;

        for segment in path_segments {
            if segment.is_empty() {
                continue;
            }

            // Try exact match first
            if let Some(child) = node.children.get(*segment) {
                node = child;
                nodes.push(node);
                continue;
            }

            // Try wildcard
            if let Some(ref wildcard) = node.wildcard {
                if let Some(ref name) = node.wildcard_name {
                    captures.insert(name.clone(), segment.to_string());
                }
                node = wildcard;
                nodes.push(node);
                continue;
            }

            // No more rule nodes on this path
            break;
        }

        (nodes, captures)
    }

    /// Checks if a path is volatile (the path itself or any ancestor is marked `.volatile`).
    /// Volatile cascades downward like `.read`/`.write` rules.
    pub fn is_volatile(&self, path_segments: &[&str]) -> bool {
        let root = match &self.root {
            Some(r) => r,
            None => return false,
        };

        if root.volatile {
            return true;
        }

        let mut node = root;
        for segment in path_segments {
            if segment.is_empty() {
                continue;
            }

            // Try exact match first, then wildcard
            if let Some(child) = node.children.get(*segment) {
                node = child;
            } else if let Some(ref wildcard) = node.wildcard {
                node = wildcard;
            } else {
                // Can't navigate deeper — no ancestor was volatile
                return false;
            }

            if node.volatile {
                return true;
            }
        }

        false
    }

    /// Returns all paths marked as volatile in the rules.
    /// Wildcard segments (like $playerId) are converted to "*" for pattern matching.
    pub fn get_volatile_paths(&self) -> Vec<String> {
        let mut paths = Vec::new();
        if let Some(ref root) = self.root {
            collect_volatile_paths(root, String::new(), &mut paths);
        }
        paths
    }
}

/// Recursively collects volatile paths from the rule tree.
fn collect_volatile_paths(node: &RuleNode, current_path: String, paths: &mut Vec<String>) {
    if node.volatile {
        // Trim leading slash for cleaner patterns
        let path = current_path.trim_start_matches('/');
        if path.is_empty() {
            paths.push("*".to_string()); // Root is volatile (unusual but handle it)
        } else {
            paths.push(path.to_string());
        }
    }

    // Visit exact children
    for (key, child) in &node.children {
        let child_path = format!("{}/{}", current_path, key);
        collect_volatile_paths(child, child_path, paths);
    }

    // Visit wildcard child (convert $varName to *)
    if let Some(ref wildcard) = node.wildcard {
        let child_path = format!("{}/*", current_path);
        collect_volatile_paths(wildcard, child_path, paths);
    }
}

/// Recursively parses a rules node from JSON.
fn parse_node(data: &serde_json::Map<String, JsonValue>, path: &str) -> Result<RuleNode, String> {
    let mut node = RuleNode::default();

    for (key, value) in data {
        match key.as_str() {
            ".read" => {
                let expr_str = get_expr_string(value, path, ".read")?;
                let compiled = compile(&expr_str, path, ".read")?;
                node.read = Some(compiled);
            }
            ".write" => {
                let expr_str = get_expr_string(value, path, ".write")?;
                let compiled = compile(&expr_str, path, ".write")?;
                node.write = Some(compiled);
            }
            ".validate" => {
                let expr_str = get_expr_string(value, path, ".validate")?;
                let compiled = compile(&expr_str, path, ".validate")?;
                node.validate = Some(compiled);
            }
            ".volatile" => {
                node.volatile = value
                    .as_bool()
                    .ok_or_else(|| format!("{}.volatile: expected boolean", path))?;
            }
            ".indexOn" => {
                // Ignored (no-op) for now
            }
            _ => {
                // Child node
                if key.starts_with('.') {
                    return Err(format!("{}: unknown rule directive {:?}", path, key));
                }

                let child_data = value
                    .as_object()
                    .ok_or_else(|| format!("{}.{}: expected object", path, key))?;

                let child_path = format!("{}{}/", path, key);
                let child_node = parse_node(child_data, &child_path)?;

                if let Some(wildcard_name) = key.strip_prefix('$') {
                    // Wildcard child
                    if node.wildcard.is_some() {
                        return Err(format!("{}: multiple wildcard children not allowed", path));
                    }
                    node.wildcard = Some(Box::new(child_node));
                    node.wildcard_name = Some(wildcard_name.to_string());
                } else {
                    node.children.insert(key.clone(), child_node);
                }
            }
        }
    }

    Ok(node)
}

/// Bottom-up pass: sets `has_validate_below` on each node.
/// True if the node itself has a `.validate` rule or any descendant does.
fn compute_has_validate_below(node: &mut RuleNode) -> bool {
    let mut any = node.validate.is_some();
    for child in node.children.values_mut() {
        any |= compute_has_validate_below(child);
    }
    if let Some(ref mut wc) = node.wildcard {
        any |= compute_has_validate_below(wc);
    }
    node.has_validate_below = any;
    any
}

/// Check if any expression in the rule tree references `query.`.
fn node_uses_query(node: &RuleNode) -> bool {
    let expr_uses_query = |expr: &Option<CompiledExpr>| -> bool {
        expr.as_ref().is_some_and(|e| e.source.contains("query."))
    };

    if expr_uses_query(&node.read)
        || expr_uses_query(&node.write)
        || expr_uses_query(&node.validate)
    {
        return true;
    }
    for child in node.children.values() {
        if node_uses_query(child) {
            return true;
        }
    }
    if let Some(ref wc) = node.wildcard
        && node_uses_query(wc)
    {
        return true;
    }
    false
}

/// Extracts expression string from JSON value (string or boolean).
fn get_expr_string(value: &JsonValue, path: &str, directive: &str) -> Result<String, String> {
    match value {
        JsonValue::String(s) => Ok(s.clone()),
        JsonValue::Bool(true) => Ok("true".to_string()),
        JsonValue::Bool(false) => Ok("false".to_string()),
        _ => Err(format!("{}{}: expected string or boolean", path, directive)),
    }
}

/// Creates a compiled expression.
fn compile(expr_str: &str, path: &str, directive: &str) -> Result<CompiledExpr, String> {
    let ast = parse_expr(expr_str).map_err(|e| {
        format!(
            "{}{}: compile expression {:?}: {}",
            path, directive, expr_str, e
        )
    })?;

    Ok(CompiledExpr {
        source: expr_str.to_string(),
        ast,
        is_simple: is_simple_expr(expr_str),
        uses_new_data: expr_str.contains("newData"),
    })
}

/// Returns true if the expression only uses auth, $wildcards, and literals.
/// Simple expressions don't require creating expensive Snapshot objects.
///
/// Expensive constructs that make an expression NOT simple:
/// - data.*    : requires tree lookup, could promote cold segments
/// - root.*    : requires access to entire tree, could promote cold segments
///
/// newData.* is allowed because it's already in memory (the value being written).
fn is_simple_expr(expr_str: &str) -> bool {
    // Check for expensive constructs (data. and root. access)
    if expr_str.contains("data.") {
        return false;
    }
    if expr_str.contains("root.") {
        return false;
    }
    true
}

/// Strips JavaScript-style comments from JSON.
/// Handles // line comments and /* block comments */.
/// Preserves strings containing comment-like sequences.
fn strip_comments(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut result = Vec::with_capacity(bytes.len());
    let mut i = 0;
    let n = bytes.len();

    while i < n {
        // Check for string start
        if bytes[i] == b'"' {
            // Copy the entire string including quotes
            result.push(bytes[i]);
            i += 1;
            while i < n {
                if bytes[i] == b'\\' && i + 1 < n {
                    // Escaped character - copy both
                    result.push(bytes[i]);
                    result.push(bytes[i + 1]);
                    i += 2;
                } else if bytes[i] == b'"' {
                    // End of string
                    result.push(bytes[i]);
                    i += 1;
                    break;
                } else {
                    result.push(bytes[i]);
                    i += 1;
                }
            }
        } else if i + 1 < n && bytes[i] == b'/' && bytes[i + 1] == b'/' {
            // Line comment - skip until newline
            i += 2;
            while i < n && bytes[i] != b'\n' {
                i += 1;
            }
            // Keep the newline for line number preservation
            if i < n {
                result.push(bytes[i]);
                i += 1;
            }
        } else if i + 1 < n && bytes[i] == b'/' && bytes[i + 1] == b'*' {
            // Block comment - skip until */
            i += 2;
            while i + 1 < n {
                if bytes[i] == b'*' && bytes[i + 1] == b'/' {
                    i += 2;
                    break;
                }
                // Preserve newlines for line number preservation
                if bytes[i] == b'\n' {
                    result.push(b'\n');
                }
                i += 1;
            }
        } else {
            // Regular character
            result.push(bytes[i]);
            i += 1;
        }
    }

    String::from_utf8(result).unwrap_or_else(|_| input.to_string())
}

/// Splits a path into segments.
pub fn parse_path(path: &str) -> Vec<&str> {
    let trimmed = path.trim_matches('/');
    if trimmed.is_empty() {
        return Vec::new();
    }
    trimmed.split('/').collect()
}

/// Creates a permissive rule set that allows all reads and writes.
pub fn default_rules() -> RuleSet {
    RuleSet::parse(r#"{"rules": {".read": true, ".write": true}}"#)
        .expect("default rules should always parse")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_rules() {
        let rules = RuleSet::parse(
            r#"{
            "rules": {
                ".read": true,
                ".write": false
            }
        }"#,
        )
        .unwrap();

        let root = rules.root().unwrap();
        assert!(root.read.is_some());
        assert!(root.write.is_some());
        assert!(root.read.as_ref().unwrap().source == "true");
        assert!(root.write.as_ref().unwrap().source == "false");
    }

    #[test]
    fn test_parse_expression_rules() {
        let rules = RuleSet::parse(
            r#"{
            "rules": {
                ".read": "auth !== null",
                ".write": "auth.uid === 'admin'"
            }
        }"#,
        )
        .unwrap();

        let root = rules.root().unwrap();
        assert_eq!(root.read.as_ref().unwrap().source, "auth !== null");
        assert_eq!(root.write.as_ref().unwrap().source, "auth.uid === 'admin'");
    }

    #[test]
    fn test_parse_nested_rules() {
        let rules = RuleSet::parse(
            r#"{
            "rules": {
                "users": {
                    "$userId": {
                        ".read": "auth.uid === $userId",
                        ".write": "auth.uid === $userId"
                    }
                }
            }
        }"#,
        )
        .unwrap();

        let (node, captures) = rules.get_node(&["users", "abc123"]);
        assert!(node.is_some());
        assert_eq!(captures.get("userId"), Some(&"abc123".to_string()));
    }

    #[test]
    fn test_find_rules_on_path() {
        let rules = RuleSet::parse(
            r#"{
            "rules": {
                ".read": "auth !== null",
                "users": {
                    ".read": "true",
                    "$userId": {
                        ".read": "auth.uid === $userId"
                    }
                }
            }
        }"#,
        )
        .unwrap();

        let (nodes, captures) = rules.find_rules_on_path(&["users", "abc123"]);
        assert_eq!(nodes.len(), 3); // root, users, $userId
        assert_eq!(captures.get("userId"), Some(&"abc123".to_string()));
    }

    #[test]
    fn test_volatile_flag() {
        let rules = RuleSet::parse(
            r#"{
            "rules": {
                "players": {
                    "$playerId": {
                        "position": {
                            ".volatile": true,
                            ".write": "auth.uid === $playerId"
                        }
                    }
                }
            }
        }"#,
        )
        .unwrap();

        assert!(rules.is_volatile(&["players", "abc", "position"]));
        // Children of volatile paths inherit volatility
        assert!(rules.is_volatile(&["players", "abc", "position", "x"]));
        assert!(!rules.is_volatile(&["players", "abc"]));
    }

    #[test]
    fn test_volatile_cascades_from_parent() {
        let rules = RuleSet::parse(
            r#"{
            "rules": {
                "cursors": {
                    ".volatile": true
                }
            }
        }"#,
        )
        .unwrap();

        assert!(rules.is_volatile(&["cursors"]));
        assert!(rules.is_volatile(&["cursors", "player1"]));
        assert!(rules.is_volatile(&["cursors", "player1", "x", "y"]));
        assert!(!rules.is_volatile(&["other"]));
    }

    #[test]
    fn test_get_volatile_paths() {
        let rules = RuleSet::parse(
            r#"{
            "rules": {
                "players": {
                    "$playerId": {
                        "position": { ".volatile": true },
                        "rotation": { ".volatile": true }
                    }
                }
            }
        }"#,
        )
        .unwrap();

        let paths = rules.get_volatile_paths();
        assert_eq!(paths.len(), 2);
        assert!(paths.contains(&"players/*/position".to_string()));
        assert!(paths.contains(&"players/*/rotation".to_string()));
    }

    #[test]
    fn test_strip_comments() {
        let input = r#"{
            // This is a comment
            "rules": {
                ".read": true, // inline comment
                /* block comment */
                ".write": false
            }
        }"#;

        let stripped = strip_comments(input);
        assert!(!stripped.contains("//"));
        assert!(!stripped.contains("/*"));
        assert!(stripped.contains("\"rules\""));
    }

    #[test]
    fn test_strip_comments_preserves_strings() {
        let input = r#"{"url": "http://example.com"}"#;
        let stripped = strip_comments(input);
        assert_eq!(stripped, input);
    }

    #[test]
    fn test_is_simple_expr() {
        assert!(is_simple_expr("auth !== null"));
        assert!(is_simple_expr("auth.uid === $userId"));
        assert!(is_simple_expr("true"));
        assert!(is_simple_expr("newData.exists()"));

        assert!(!is_simple_expr("data.exists()"));
        assert!(!is_simple_expr("root.child('users').exists()"));
    }

    #[test]
    fn test_parse_path() {
        assert_eq!(parse_path("/users/abc/name"), vec!["users", "abc", "name"]);
        assert_eq!(parse_path("users/abc"), vec!["users", "abc"]);
        assert!(parse_path("/").is_empty());
        assert!(parse_path("").is_empty());
    }

    #[test]
    fn test_multiple_wildcards_error() {
        let result = RuleSet::parse(
            r#"{
            "rules": {
                "$a": {},
                "$b": {}
            }
        }"#,
        );

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("multiple wildcard"));
    }

    #[test]
    fn test_unknown_directive_error() {
        let result = RuleSet::parse(
            r#"{
            "rules": {
                ".unknown": true
            }
        }"#,
        );

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("unknown rule directive"));
    }

    #[test]
    fn test_index_on_ignored() {
        let rules = RuleSet::parse(
            r#"{
            "rules": {
                ".read": true,
                ".write": true,
                "messages": {
                    ".indexOn": ["timestamp", "author"]
                }
            }
        }"#,
        );

        assert!(
            rules.is_ok(),
            "Rules with .indexOn should parse successfully"
        );
    }

    #[test]
    fn test_validate_rule() {
        let rules = RuleSet::parse(
            r#"{
            "rules": {
                "messages": {
                    "$msgId": {
                        ".validate": "newData.hasChild('text')"
                    }
                }
            }
        }"#,
        )
        .unwrap();

        let (node, _) = rules.get_node(&["messages", "msg1"]);
        assert!(node.unwrap().validate.is_some());
    }

    #[test]
    fn test_default_rules() {
        let rules = default_rules();
        let root = rules.root().unwrap();
        assert!(root.read.is_some());
        assert!(root.write.is_some());
    }
}
