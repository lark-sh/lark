//! Rules Evaluator
//!
//! # Architecture Overview
//!
//! Each Database has its own Evaluator instance. The Evaluator uses a pure Rust
//! expression interpreter to execute rule expressions like:
//!
//! `"auth.uid !== null && data.child('owner').val() === auth.uid"`
//!
//! Rule expressions have access to these variables:
//! - auth: Current user's authentication info (uid, provider, token claims)
//! - data: Snapshot of current value at the path (before write)
//! - newData: Snapshot of new value at the path (after write)
//! - root: Snapshot of entire database root (for cross-path checks)
//! - now: Server timestamp in milliseconds
//! - $wildcards: Captured path segments (e.g., $userId from /users/$userId)
//! - lark.databaseId: Current database ID
//! - lark.projectId: Current project ID
//!
//! # Performance Optimizations
//!
//! ## Volatile Path Fast Path
//! Paths marked as `.volatile` in rules are optimized for high-frequency writes:
//! - **Simple rules only**: Volatile paths can only use `auth.*`, `$captures`, and `newData.*`
//!   Rules using `data.*` or `root.*` are denied (would require expensive tree lookups)
//! - **Skip snapshot creation**: For simple volatile rules, no Snapshot objects are created
//! - **No tree access**: Volatile writes bypass Tree mutation entirely
//!
//! ## Arc-based Auth Caching
//! Auth info is converted to a HashMap once per client session and wrapped in Arc:
//! - `RulesContext.auth`: `Option<Arc<AuthInfo>>` - O(1) to clone when creating child contexts
//! - `AuthInfo.cached_json`: `Arc<HashMap<String, JsonValue>>` - O(1) to access in expressions
//! - Eliminates BTreeMap cloning that was previously 8% of CPU on hot paths
//!
//! ## Lazy Data Access
//! - `data` and `root` use `LazySnapshot` - only materializes values when `val()` is called
//! - `exists()`, `hasChild()`, etc. check tree structure without copying data
//! - Reduces allocation from ~200MB/write (eager) to ~1KB/write (lazy) for large databases

use super::expr::{EvalContext, EvalError, eval_bool};
use super::parser::{CompiledExpr, RuleSet, parse_path};
use super::snapshot::{AuthInfo, EmptyTree, LazySnapshot, NeedsPromotion, NewData, TreeGetter};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::sync::Arc;

/// Context for evaluating rules against an operation.
#[derive(Default)]
pub struct RulesContext {
    /// Current user's auth (None for anonymous without uid).
    /// Wrapped in Arc for O(1) cloning when creating child contexts.
    pub auth: Option<Arc<AuthInfo>>,
    /// Lazy access to database tree for data.* and root.* expressions.
    /// LazySnapshot is created from this to avoid materializing data until needed.
    pub root_tree: Option<Arc<dyn TreeGetter>>,
    /// Path being accessed.
    pub path: String,
    /// What's being written (None for reads / deletes). The same `NewData`
    /// is reused across the whole rules cascade — `eval_expr` constructs
    /// a snapshot at `ctx.path` for each level via `NewData::snapshot_at`,
    /// without materializing any tree+updates merge.
    pub new_data: Option<NewData>,
    /// If true, root access is disabled for performance.
    pub is_volatile: bool,
    /// Current database ID (for lark.databaseId).
    pub database_id: String,
    /// Current project ID (for lark.projectId).
    pub project_id: String,
    /// Query parameters for query-based rules (reads/subscribes only).
    /// Pre-built as a HashMap for direct use in expression evaluation.
    pub query: Option<Arc<HashMap<String, JsonValue>>>,
}

/// Evaluator evaluates rules against data and auth context.
#[derive(Clone)]
pub struct Evaluator {
    rules: Option<RuleSet>,
}

impl Evaluator {
    /// Creates a new rules evaluator.
    pub fn new(rules: RuleSet) -> Self {
        Self { rules: Some(rules) }
    }

    /// Creates an evaluator with no rules (allows all operations).
    pub fn allow_all() -> Self {
        Self { rules: None }
    }

    /// Returns the underlying rule set.
    pub fn rules(&self) -> Option<&RuleSet> {
        self.rules.as_ref()
    }

    /// Checks if the auth context can read the given path.
    /// .read rules cascade - if granted at any ancestor, the read is allowed.
    /// Returns Err(NeedsPromotion) if blob data needs to be loaded before evaluation can complete.
    pub fn can_read(&self, ctx: &RulesContext) -> Result<bool, NeedsPromotion> {
        // Admin bypass
        if is_admin(&ctx.auth) {
            return Ok(true);
        }

        let rules = match &self.rules {
            Some(r) => r,
            None => return Ok(true), // No rules = allow all
        };

        let segments: Vec<&str> = parse_path(&ctx.path);
        let (nodes, captures) = rules.find_rules_on_path(&segments);

        // Check each node on the path for a .read rule
        // If any .read rule returns true, access is granted (cascading)
        // Each rule at level N is evaluated with data at level N
        for (node_idx, node) in nodes.iter().enumerate() {
            if let Some(ref read_expr) = node.read {
                // Compute the ancestor path for this node
                let ancestor_segments = if node_idx == 0 {
                    &segments[0..0] // empty slice for root
                } else {
                    &segments[0..node_idx]
                };

                // Compute the ancestor path for this rule level
                let ancestor_path = if ancestor_segments.is_empty() {
                    String::new()
                } else {
                    format!("/{}", ancestor_segments.join("/"))
                };

                // Create context for this ancestor level (no newData for reads)
                let level_ctx = RulesContext {
                    auth: ctx.auth.clone(),
                    root_tree: ctx.root_tree.clone(),
                    path: ancestor_path,
                    new_data: None,
                    is_volatile: ctx.is_volatile,
                    database_id: ctx.database_id.clone(),
                    project_id: ctx.project_id.clone(),
                    query: ctx.query.clone(),
                };

                let allowed = self.eval_expr(read_expr, &level_ctx, &captures)?;
                if allowed {
                    return Ok(true);
                }
            }
        }

        // No .read rule returned true
        Ok(false)
    }

    /// Checks if the auth context can write to the given path.
    /// .write rules cascade - if granted at any ancestor, the write is allowed.
    /// .validate rules do NOT cascade - all applicable .validate rules must pass.
    /// Returns Err(NeedsPromotion) if blob data needs to be loaded before evaluation can complete.
    pub fn can_write(&self, ctx: &RulesContext) -> Result<bool, NeedsPromotion> {
        // Admin bypass
        if is_admin(&ctx.auth) {
            return Ok(true);
        }

        let rules = match &self.rules {
            Some(r) => r,
            None => return Ok(true), // No rules = allow all
        };

        let segments: Vec<&str> = parse_path(&ctx.path);
        let (nodes, captures) = rules.find_rules_on_path(&segments);

        // Check .write rules (cascading - any true grants access)
        // Each rule at level N is evaluated with data/newData at level N
        let mut write_allowed = false;
        for (node_idx, node) in nodes.iter().enumerate() {
            if let Some(ref write_expr) = node.write {
                // Compute the ancestor path for this node
                // node_idx 0 = root (empty path)
                // node_idx 1 = segments[0]
                // node_idx N = segments[0..N-1]
                let ancestor_segments = if node_idx == 0 {
                    &segments[0..0] // empty slice for root
                } else {
                    &segments[0..node_idx]
                };

                // Compute the ancestor path for this rule level
                let ancestor_path = if ancestor_segments.is_empty() {
                    String::new()
                } else {
                    format!("/{}", ancestor_segments.join("/"))
                };

                // Only carry newData into the level if the rule actually uses
                // it — avoids the cost of `eval_expr` constructing a snapshot
                // for rules like "auth !== null". The same `NewData` is reused
                // across every level; the level snapshot is built lazily by
                // `eval_expr` at the level's `ctx.path`.
                let ancestor_new_data = if write_expr.uses_new_data {
                    ctx.new_data.clone()
                } else {
                    None
                };

                // Create context for this ancestor level
                let level_ctx = RulesContext {
                    auth: ctx.auth.clone(),
                    root_tree: ctx.root_tree.clone(),
                    path: ancestor_path,
                    new_data: ancestor_new_data,
                    is_volatile: ctx.is_volatile,
                    database_id: ctx.database_id.clone(),
                    project_id: ctx.project_id.clone(),
                    query: ctx.query.clone(),
                };

                let allowed = self.eval_expr(write_expr, &level_ctx, &captures)?;
                if allowed {
                    write_allowed = true;
                    break;
                }
            }
        }

        if !write_allowed {
            return Ok(false);
        }

        // Check .validate rules (non-cascading - all must pass)
        // Skip validation entirely for deletes (new_data is None)
        if ctx.new_data.is_some() {
            for (node_idx, node) in nodes.iter().enumerate() {
                if let Some(ref validate_expr) = node.validate {
                    // Compute context at this level (same logic as .write)
                    let ancestor_segments = if node_idx == 0 {
                        &segments[0..0]
                    } else {
                        &segments[0..node_idx]
                    };

                    let ancestor_path = if ancestor_segments.is_empty() {
                        String::new()
                    } else {
                        format!("/{}", ancestor_segments.join("/"))
                    };

                    // Same lazy carry as the .write cascade above.
                    let ancestor_new_data = if validate_expr.uses_new_data {
                        ctx.new_data.clone()
                    } else {
                        None
                    };

                    let level_ctx = RulesContext {
                        auth: ctx.auth.clone(),
                        root_tree: ctx.root_tree.clone(),
                        path: ancestor_path,
                        new_data: ancestor_new_data,
                        is_volatile: ctx.is_volatile,
                        database_id: ctx.database_id.clone(),
                        project_id: ctx.project_id.clone(),
                        query: ctx.query.clone(),
                    };

                    let valid = self.eval_expr(validate_expr, &level_ctx, &captures)?;
                    if !valid {
                        return Ok(false);
                    }
                }
            }

            // Also validate all children being written — but only if there are
            // .validate rules somewhere below the deepest matched rule node.
            let has_validate_below = nodes.last().is_some_and(|n| n.has_validate_below);
            if has_validate_below && !self.validate_children(ctx, &segments, &captures)? {
                return Ok(false);
            }
        }

        Ok(true)
    }

    /// Recursively validates the children at `ctx.path` that are actually
    /// being written. Unlike the previous merged-Object iteration, this
    /// matches intended semantics: `.validate` fires only on writes, not
    /// on tree-existing siblings that the UPDATE doesn't touch.
    ///
    /// Children are sourced from `NewData::writes_at(ctx.path)`. Each
    /// child is recursed-into as a SET-style `NewData` containing the
    /// partial value contributed by the writes — that handles the
    /// multi-path UPDATE case (e.g. `{"a/b": v1, "a/c": v2}` produces a
    /// single `("a", {b: v1, c: v2})` entry whose recursion validates
    /// `b` and `c` as nested writes).
    fn validate_children(
        &self,
        ctx: &RulesContext,
        path_segments: &[&str],
        captures: &HashMap<String, String>,
    ) -> Result<bool, NeedsPromotion> {
        let new_data = match &ctx.new_data {
            Some(nd) => nd,
            None => return Ok(true),
        };

        let rules = match &self.rules {
            Some(r) => r,
            None => return Ok(true),
        };

        let (node, _) = rules.get_node(path_segments);
        let node = match node {
            Some(n) => n,
            None => return Ok(true),
        };

        let writes = new_data.writes_at(&ctx.path);
        for (key, child_value) in writes {
            let child_path = format!("{}/{}", ctx.path, key);
            let mut child_segments: Vec<&str> = path_segments.to_vec();
            child_segments.push(key.as_str());

            // Find the rule node for this child
            let (child_node, child_captures) = if let Some(child) = node.children.get(&key) {
                (Some(child), captures.clone())
            } else if let Some(ref wildcard) = node.wildcard {
                let mut new_captures = captures.clone();
                if let Some(ref name) = node.wildcard_name {
                    new_captures.insert(name.clone(), key.clone());
                }
                (Some(wildcard.as_ref()), new_captures)
            } else {
                (None, captures.clone())
            };

            if let Some(child_rule_node) = child_node {
                if let Some(ref validate_expr) = child_rule_node.validate {
                    let child_ctx = RulesContext {
                        auth: ctx.auth.clone(),
                        root_tree: ctx.root_tree.clone(),
                        path: child_path.clone(),
                        new_data: Some(NewData::from_set(child_path.clone(), child_value.clone())),
                        is_volatile: ctx.is_volatile,
                        database_id: ctx.database_id.clone(),
                        project_id: ctx.project_id.clone(),
                        query: ctx.query.clone(),
                    };

                    let valid = self.eval_expr(validate_expr, &child_ctx, &child_captures)?;
                    if !valid {
                        return Ok(false);
                    }
                }

                // Recurse into children only if there are .validate rules deeper
                if child_rule_node.has_validate_below {
                    let child_ctx = RulesContext {
                        auth: ctx.auth.clone(),
                        root_tree: ctx.root_tree.clone(),
                        path: child_path.clone(),
                        new_data: Some(NewData::from_set(child_path, child_value)),
                        is_volatile: ctx.is_volatile,
                        database_id: ctx.database_id.clone(),
                        project_id: ctx.project_id.clone(),
                        query: ctx.query.clone(),
                    };

                    if !self.validate_children(&child_ctx, &child_segments, &child_captures)? {
                        return Ok(false);
                    }
                }
            }
        }

        Ok(true)
    }

    /// Evaluates a compiled expression with the given context.
    /// Returns Ok(bool) for successful evaluation, or Err(NeedsPromotion) if blob data needs loading.
    fn eval_expr(
        &self,
        compiled_expr: &CompiledExpr,
        ctx: &RulesContext,
        captures: &HashMap<String, String>,
    ) -> Result<bool, NeedsPromotion> {
        // Fast path for volatile writes: if the rule uses expensive constructs
        // (data., root.), deny immediately without evaluation.
        if ctx.is_volatile && !compiled_expr.is_simple {
            return Ok(false);
        }

        // Build evaluation context
        let mut eval_ctx = EvalContext::new();

        // Copy captures
        for (k, v) in captures {
            eval_ctx.captures.insert(k.clone(), v.clone());
        }

        // Set up auth - to_json() returns Arc<HashMap> for O(1) cloning
        if let Some(ref auth) = ctx.auth {
            eval_ctx.auth = auth.to_json();
        }

        // Set up lark.* variables
        eval_ctx.database_id = ctx.database_id.clone();
        eval_ctx.project_id = ctx.project_id.clone();

        // Set up query.* variables (O(1) Arc clone)
        eval_ctx.query = ctx.query.clone();

        // For simple volatile rules, skip creating expensive Snapshot objects
        // for data.* and root.* (which require blob I/O), but always set up
        // newData since it's already in memory (the value being written).
        if ctx.is_volatile && compiled_expr.is_simple {
            // Only set up newData - skip data and root to avoid blob access
            if let Some(ref new_data) = ctx.new_data {
                eval_ctx.new_data = Some(new_data.snapshot_at(
                    ctx.root_tree.clone().unwrap_or_else(|| Arc::new(EmptyTree)),
                    &ctx.path,
                ));
            }
        } else {
            // Set up data snapshot (uses LazySnapshot for on-demand materialization)
            if let Some(ref root_tree) = ctx.root_tree {
                let lazy = LazySnapshot::new(
                    Arc::clone(root_tree),
                    ctx.path.trim_start_matches('/').to_string(),
                );
                eval_ctx.data = Some(Box::new(lazy));
            }

            // Set up newData snapshot — lazy overlay of `updates` on tree.
            if let Some(ref new_data) = ctx.new_data {
                eval_ctx.new_data = Some(new_data.snapshot_at(
                    ctx.root_tree.clone().unwrap_or_else(|| Arc::new(EmptyTree)),
                    &ctx.path,
                ));
            }

            // Set up root snapshot (uses LazySnapshot for on-demand materialization)
            // For volatile paths, disable root access to prevent expensive cross-path lookups
            if !ctx.is_volatile
                && let Some(ref root_tree) = ctx.root_tree
            {
                let lazy = LazySnapshot::new(Arc::clone(root_tree), String::new());
                eval_ctx.root = Some(Box::new(lazy));
            }
        }

        // Evaluate the expression
        match eval_bool(&compiled_expr.ast, &eval_ctx) {
            Ok(result) => Ok(result),
            Err(EvalError::NeedsPromotion(needs)) => {
                // Propagate blob data loading request
                Err(needs)
            }
            Err(EvalError::Error(_)) => {
                // For regular errors (like null.contains()), treat as denial rather than error.
                Ok(false)
            }
        }
    }

    /// Returns true if any rule in this evaluator references `query.*`.
    pub fn uses_query(&self) -> bool {
        self.rules.as_ref().is_some_and(|r| r.uses_query)
    }

    /// Checks if a path is marked as volatile.
    pub fn is_volatile(&self, path: &str) -> bool {
        match &self.rules {
            Some(r) => {
                let segments: Vec<&str> = parse_path(path);
                r.is_volatile(&segments)
            }
            None => false,
        }
    }

    /// Returns all paths marked as volatile in the rules.
    pub fn get_volatile_paths(&self) -> Vec<String> {
        match &self.rules {
            Some(r) => r.get_volatile_paths(),
            None => Vec::new(),
        }
    }
}

/// Checks if the auth context grants admin access (bypasses all rules).
fn is_admin(auth: &Option<Arc<AuthInfo>>) -> bool {
    match auth {
        Some(a) => a.is_true_admin,
        None => false,
    }
}

/// Creates a permissive rule set that allows all reads and writes.
pub fn default_rules() -> RuleSet {
    super::parser::default_rules()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_rules(json: &str) -> Evaluator {
        let rules = RuleSet::parse(json).expect("failed to parse rules");
        Evaluator::new(rules)
    }

    /// Test helper: build an `Option<NewData>` for a SET write at `path`
    /// with the given value. Mirrors how production callers wrap a SET
    /// before passing to `can_write`.
    fn set_at(path: &str, value: JsonValue) -> Option<NewData> {
        Some(NewData::from_set(path.to_string(), value))
    }

    #[test]
    fn test_allow_all_read() {
        let eval = make_rules(r#"{"rules": {".read": true}}"#);
        let ctx = RulesContext {
            path: "/users/abc".to_string(),
            ..Default::default()
        };
        assert!(eval.can_read(&ctx).unwrap());
    }

    #[test]
    fn test_deny_all_read() {
        let eval = make_rules(r#"{"rules": {".read": false}}"#);
        let ctx = RulesContext {
            path: "/users/abc".to_string(),
            ..Default::default()
        };
        assert!(!eval.can_read(&ctx).unwrap());
    }

    #[test]
    fn test_auth_required_read() {
        let eval = make_rules(r#"{"rules": {".read": "auth !== null"}}"#);

        // Without auth
        let ctx = RulesContext {
            path: "/users/abc".to_string(),
            ..Default::default()
        };
        assert!(!eval.can_read(&ctx).unwrap());

        // With auth
        let ctx = RulesContext {
            path: "/users/abc".to_string(),
            auth: Some(Arc::new(AuthInfo::with_uid("user123".to_string()))),
            ..Default::default()
        };
        assert!(eval.can_read(&ctx).unwrap());
    }

    #[test]
    fn test_wildcard_capture() {
        let eval = make_rules(
            r#"{
            "rules": {
                "users": {
                    "$userId": {
                        ".read": "auth.uid === $userId"
                    }
                }
            }
        }"#,
        );

        // Matching user
        let ctx = RulesContext {
            path: "/users/abc123".to_string(),
            auth: Some(Arc::new(AuthInfo::with_uid("abc123".to_string()))),
            ..Default::default()
        };
        assert!(eval.can_read(&ctx).unwrap());

        // Different user
        let ctx = RulesContext {
            path: "/users/abc123".to_string(),
            auth: Some(Arc::new(AuthInfo::with_uid("xyz789".to_string()))),
            ..Default::default()
        };
        assert!(!eval.can_read(&ctx).unwrap());
    }

    #[test]
    fn test_write_with_validate() {
        let eval = make_rules(
            r#"{
            "rules": {
                "messages": {
                    ".write": "auth !== null",
                    ".validate": "newData.hasChild('text')"
                }
            }
        }"#,
        );

        // Valid write
        let ctx = RulesContext {
            path: "/messages".to_string(),
            auth: Some(Arc::new(AuthInfo::with_uid("user123".to_string()))),
            new_data: set_at("/messages", serde_json::json!({"text": "hello"})),
            ..Default::default()
        };
        assert!(eval.can_write(&ctx).unwrap());

        // Invalid write (missing 'text')
        let ctx = RulesContext {
            path: "/messages".to_string(),
            auth: Some(Arc::new(AuthInfo::with_uid("user123".to_string()))),
            new_data: set_at("/messages", serde_json::json!({"foo": "bar"})),
            ..Default::default()
        };
        assert!(!eval.can_write(&ctx).unwrap());
    }

    #[test]
    fn test_admin_bypass() {
        let eval = make_rules(r#"{"rules": {".read": false, ".write": false}}"#);

        let ctx = RulesContext {
            path: "/anything".to_string(),
            auth: Some(Arc::new(AuthInfo::admin())),
            ..Default::default()
        };

        assert!(eval.can_read(&ctx).unwrap());
        assert!(eval.can_write(&ctx).unwrap());
    }

    #[test]
    fn test_cascading_read() {
        let eval = make_rules(
            r#"{
            "rules": {
                ".read": "auth !== null",
                "public": {
                    ".read": true
                }
            }
        }"#,
        );

        // Root requires auth, but /public allows all
        let ctx = RulesContext {
            path: "/public/data".to_string(),
            ..Default::default()
        };
        assert!(eval.can_read(&ctx).unwrap());

        // Other paths still require auth
        let ctx = RulesContext {
            path: "/private/data".to_string(),
            ..Default::default()
        };
        assert!(!eval.can_read(&ctx).unwrap());
    }

    #[test]
    fn test_volatile_simple_expr_allowed() {
        let eval = make_rules(
            r#"{
            "rules": {
                "players": {
                    "$pid": {
                        "position": {
                            ".volatile": true,
                            ".write": "auth.uid === $pid"
                        }
                    }
                }
            }
        }"#,
        );

        let ctx = RulesContext {
            path: "/players/abc/position".to_string(),
            auth: Some(Arc::new(AuthInfo::with_uid("abc".to_string()))),
            new_data: set_at("/players/abc/position", serde_json::json!({"x": 1, "y": 2})),
            is_volatile: true,
            ..Default::default()
        };

        assert!(eval.can_write(&ctx).unwrap());
    }

    #[test]
    fn test_volatile_complex_expr_denied() {
        let eval = make_rules(
            r#"{
            "rules": {
                "players": {
                    "$pid": {
                        "position": {
                            ".volatile": true,
                            ".write": "data.exists()"
                        }
                    }
                }
            }
        }"#,
        );

        let ctx = RulesContext {
            path: "/players/abc/position".to_string(),
            auth: Some(Arc::new(AuthInfo::with_uid("abc".to_string()))),
            new_data: set_at("/players/abc/position", serde_json::json!({"x": 1, "y": 2})),
            is_volatile: true,
            ..Default::default()
        };

        // Should be denied because data.exists() is not a simple expression
        assert!(!eval.can_write(&ctx).unwrap());
    }

    #[test]
    fn test_volatile_validate_with_new_data() {
        let eval = make_rules(
            r#"{
            "rules": {
                "cursors": {
                    ".volatile": true,
                    ".read": true,
                    ".write": true,
                    "$cursorId": {
                        ".validate": "newData.hasChildren(['x', 'y'])"
                    }
                }
            }
        }"#,
        );

        // Valid write: has both x and y
        let ctx = RulesContext {
            path: "/cursors/vppd7fi9".to_string(),
            auth: Some(Arc::new(AuthInfo::with_uid("user1".to_string()))),
            new_data: set_at(
                "/cursors/vppd7fi9",
                serde_json::json!({"x": -835, "y": 173}),
            ),
            is_volatile: true,
            ..Default::default()
        };
        assert!(eval.can_write(&ctx).unwrap());

        // Invalid write: missing y
        let ctx2 = RulesContext {
            path: "/cursors/vppd7fi9".to_string(),
            auth: Some(Arc::new(AuthInfo::with_uid("user1".to_string()))),
            new_data: set_at("/cursors/vppd7fi9", serde_json::json!({"x": -835})),
            is_volatile: true,
            ..Default::default()
        };
        assert!(!eval.can_write(&ctx2).unwrap());
    }

    #[test]
    fn test_lark_variables() {
        let eval = make_rules(
            r#"{
            "rules": {
                ".read": "lark.databaseId === 'db1'"
            }
        }"#,
        );

        // Matching database
        let ctx = RulesContext {
            path: "/anything".to_string(),
            database_id: "db1".to_string(),
            ..Default::default()
        };
        assert!(eval.can_read(&ctx).unwrap());

        // Different database
        let ctx = RulesContext {
            path: "/anything".to_string(),
            database_id: "db2".to_string(),
            ..Default::default()
        };
        assert!(!eval.can_read(&ctx).unwrap());
    }

    #[test]
    fn test_no_rules_allows_all() {
        let eval = Evaluator::allow_all();

        let ctx = RulesContext {
            path: "/anything".to_string(),
            ..Default::default()
        };

        assert!(eval.can_read(&ctx).unwrap());
        assert!(eval.can_write(&ctx).unwrap());
    }

    #[test]
    fn test_is_volatile() {
        let eval = make_rules(
            r#"{
            "rules": {
                "players": {
                    "$pid": {
                        "position": { ".volatile": true },
                        "name": {}
                    }
                }
            }
        }"#,
        );

        assert!(eval.is_volatile("/players/abc/position"));
        // Children of volatile paths inherit volatility
        assert!(eval.is_volatile("/players/abc/position/x"));
        assert!(eval.is_volatile("/players/abc/position/x/y/z"));
        assert!(!eval.is_volatile("/players/abc/name"));
        assert!(!eval.is_volatile("/players/abc"));
    }

    #[test]
    fn test_is_volatile_cascades_from_parent() {
        let eval = make_rules(
            r#"{
            "rules": {
                "cursors": {
                    ".volatile": true
                }
            }
        }"#,
        );

        assert!(eval.is_volatile("/cursors"));
        assert!(eval.is_volatile("/cursors/player1"));
        assert!(eval.is_volatile("/cursors/player1/x"));
        assert!(!eval.is_volatile("/other"));
    }

    #[test]
    fn test_get_volatile_paths() {
        let eval = make_rules(
            r#"{
            "rules": {
                "players": {
                    "$pid": {
                        "position": { ".volatile": true },
                        "rotation": { ".volatile": true }
                    }
                }
            }
        }"#,
        );

        let paths = eval.get_volatile_paths();
        assert_eq!(paths.len(), 2);
        assert!(paths.contains(&"players/*/position".to_string()));
        assert!(paths.contains(&"players/*/rotation".to_string()));
    }

    // Test that root.* expressions can access other parts of the tree
    #[test]
    fn test_root_cross_path_access() {
        use std::collections::HashMap;

        // Create a mock tree that stores data by path
        struct TestTree {
            data: HashMap<String, JsonValue>,
        }

        impl TreeGetter for TestTree {
            fn get_value(&self, path: &str) -> Option<JsonValue> {
                // Normalize path (remove leading slash)
                let normalized = path.strip_prefix('/').unwrap_or(path);
                self.data.get(normalized).cloned()
            }

            fn get_node_value(&self, path: &str) -> Option<JsonValue> {
                self.get_value(path)
            }

            fn node_exists(&self, path: &str) -> bool {
                self.get_value(path).is_some()
            }

            fn node_has_child(&self, path: &str, child_name: &str) -> bool {
                let child_path =
                    format!("{}/{}", path.strip_prefix('/').unwrap_or(path), child_name);
                self.data.contains_key(&child_path)
            }
        }

        // Rule: Can only write to messages if user has canPost=true in permissions
        let eval = make_rules(
            r#"{
            "rules": {
                "messages": {
                    "$messageId": {
                        ".write": "root.child('permissions').child(auth.uid).child('canPost').val() === true"
                    }
                }
            }
        }"#,
        );

        // Set up tree with permissions data
        let mut tree_data = HashMap::new();
        tree_data.insert(
            "permissions/user1/canPost".to_string(),
            serde_json::json!(true),
        );
        tree_data.insert(
            "permissions/user2/canPost".to_string(),
            serde_json::json!(false),
        );

        let tree = TestTree { data: tree_data };
        let tree_arc: Arc<dyn TreeGetter> = Arc::new(tree);

        // User1 can write (has canPost=true)
        let ctx = RulesContext {
            path: "/messages/msg123".to_string(),
            auth: Some(Arc::new(AuthInfo::with_uid("user1".to_string()))),
            root_tree: Some(tree_arc.clone()),
            new_data: set_at("/messages/msg123", serde_json::json!({"text": "hello"})),
            ..Default::default()
        };
        assert!(
            eval.can_write(&ctx).unwrap(),
            "user1 should be able to write"
        );

        // User2 cannot write (has canPost=false)
        let ctx = RulesContext {
            path: "/messages/msg123".to_string(),
            auth: Some(Arc::new(AuthInfo::with_uid("user2".to_string()))),
            root_tree: Some(tree_arc.clone()),
            new_data: set_at("/messages/msg123", serde_json::json!({"text": "hello"})),
            ..Default::default()
        };
        assert!(
            !eval.can_write(&ctx).unwrap(),
            "user2 should not be able to write"
        );

        // User3 cannot write (no permission entry)
        let ctx = RulesContext {
            path: "/messages/msg123".to_string(),
            auth: Some(Arc::new(AuthInfo::with_uid("user3".to_string()))),
            root_tree: Some(tree_arc.clone()),
            new_data: set_at("/messages/msg123", serde_json::json!({"text": "hello"})),
            ..Default::default()
        };
        assert!(
            !eval.can_write(&ctx).unwrap(),
            "user3 should not be able to write"
        );
    }

    #[test]
    fn test_query_based_rules_order_by_child_equal_to() {
        // Classic pattern: restrict reads to user's own items
        let eval = make_rules(
            r#"{
            "rules": {
                "items": {
                    ".read": "auth.uid != null && query.orderByChild == 'uid' && query.equalTo == auth.uid"
                }
            }
        }"#,
        );

        // Correct query: orderByChild='uid', equalTo=auth.uid
        let mut query_map = HashMap::new();
        query_map.insert(
            "orderByChild".to_string(),
            JsonValue::String("uid".to_string()),
        );
        query_map.insert(
            "equalTo".to_string(),
            JsonValue::String("user1".to_string()),
        );

        let ctx = RulesContext {
            path: "/items".to_string(),
            auth: Some(Arc::new(AuthInfo::with_uid("user1".to_string()))),
            query: Some(Arc::new(query_map)),
            ..Default::default()
        };
        assert!(eval.can_read(&ctx).unwrap());

        // Wrong equalTo value (different user)
        let mut wrong_query = HashMap::new();
        wrong_query.insert(
            "orderByChild".to_string(),
            JsonValue::String("uid".to_string()),
        );
        wrong_query.insert(
            "equalTo".to_string(),
            JsonValue::String("user2".to_string()),
        );

        let ctx2 = RulesContext {
            path: "/items".to_string(),
            auth: Some(Arc::new(AuthInfo::with_uid("user1".to_string()))),
            query: Some(Arc::new(wrong_query)),
            ..Default::default()
        };
        assert!(!eval.can_read(&ctx2).unwrap());

        // No query at all — denied
        let ctx3 = RulesContext {
            path: "/items".to_string(),
            auth: Some(Arc::new(AuthInfo::with_uid("user1".to_string()))),
            query: None,
            ..Default::default()
        };
        assert!(!eval.can_read(&ctx3).unwrap());
    }

    #[test]
    fn test_query_based_rules_limit() {
        // Restrict reads to max 1000 results
        let eval = make_rules(
            r#"{
            "rules": {
                "messages": {
                    ".read": "query.orderByKey && query.limitToFirst <= 1000"
                }
            }
        }"#,
        );

        // Within limit
        let mut query_map = HashMap::new();
        query_map.insert("orderByKey".to_string(), JsonValue::Bool(true));
        query_map.insert("limitToFirst".to_string(), JsonValue::Number(100.into()));

        let ctx = RulesContext {
            path: "/messages".to_string(),
            query: Some(Arc::new(query_map)),
            ..Default::default()
        };
        assert!(eval.can_read(&ctx).unwrap());

        // Over limit
        let mut over_limit = HashMap::new();
        over_limit.insert("orderByKey".to_string(), JsonValue::Bool(true));
        over_limit.insert("limitToFirst".to_string(), JsonValue::Number(5000.into()));

        let ctx2 = RulesContext {
            path: "/messages".to_string(),
            query: Some(Arc::new(over_limit)),
            ..Default::default()
        };
        assert!(!eval.can_read(&ctx2).unwrap());

        // No limit — null <= 1000 is true in JS semantics (null coerces to 0)
        let mut no_limit = HashMap::new();
        no_limit.insert("orderByKey".to_string(), JsonValue::Bool(true));

        let ctx3 = RulesContext {
            path: "/messages".to_string(),
            query: Some(Arc::new(no_limit)),
            ..Default::default()
        };
        assert!(eval.can_read(&ctx3).unwrap());

        // No query at all — denied (query is null, query.orderByKey is null = falsy)
        let ctx4 = RulesContext {
            path: "/messages".to_string(),
            query: None,
            ..Default::default()
        };
        assert!(!eval.can_read(&ctx4).unwrap());
    }
}
