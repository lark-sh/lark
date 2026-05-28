//! Expression evaluator.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

use super::ast::{BinaryOp, Expr, LiteralValue, UnaryOp};
use super::value::{
    CachedRegex, Snapshot, Value, add, compare, divide, loose_equals, modulo, multiply,
    strict_equals, subtract,
};
use crate::rules::NeedsPromotion;
use serde_json::Value as JsonValue;

/// Evaluation context provides variables available during expression evaluation.
pub struct EvalContext {
    /// Auth object (uid, provider, token, custom claims).
    /// Stored as Arc for O(1) cloning - critical for per-write rules evaluation.
    pub auth: Option<Arc<HashMap<String, JsonValue>>>,
    /// Current data at path (before write).
    pub data: Option<Box<dyn Snapshot>>,
    /// New data at path (after write).
    pub new_data: Option<Box<dyn Snapshot>>,
    /// Root of the database.
    pub root: Option<Box<dyn Snapshot>>,
    /// Server timestamp in milliseconds.
    pub now: i64,
    /// Wildcard captures ($userId, etc.).
    pub captures: HashMap<String, String>,
    /// Current database ID (for lark.databaseId).
    pub database_id: String,
    /// Current project ID (for lark.projectId).
    pub project_id: String,
    /// Query parameters for query-based rules (query.orderByChild, etc.).
    pub query: Option<Arc<HashMap<String, JsonValue>>>,
}

impl Default for EvalContext {
    fn default() -> Self {
        Self::new()
    }
}

impl EvalContext {
    /// Create a new evaluation context.
    pub fn new() -> Self {
        Self {
            auth: None,
            data: None,
            new_data: None,
            root: None,
            now: chrono::Utc::now().timestamp_millis(),
            captures: HashMap::new(),
            database_id: String::new(),
            project_id: String::new(),
            query: None,
        }
    }
}

/// Error type for expression evaluation.
/// Can be either a regular error string or a NeedsPromotion request.
#[derive(Debug)]
pub enum EvalError {
    /// Regular evaluation error (e.g., invalid operation).
    Error(String),
    /// Blob data needs loading before evaluation can continue.
    NeedsPromotion(NeedsPromotion),
}

impl From<String> for EvalError {
    fn from(s: String) -> Self {
        EvalError::Error(s)
    }
}

impl From<NeedsPromotion> for EvalError {
    fn from(n: NeedsPromotion) -> Self {
        EvalError::NeedsPromotion(n)
    }
}

impl std::fmt::Display for EvalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EvalError::Error(s) => write!(f, "{}", s),
            EvalError::NeedsPromotion(n) => write!(f, "needs promotion: {}", n.path),
        }
    }
}

/// Evaluate an expression and return its value.
pub fn eval(expr: &Expr, ctx: &EvalContext) -> Result<Value, EvalError> {
    match expr {
        Expr::Literal(lit) => Ok(eval_literal(lit)),
        Expr::Ident(name) => Ok(eval_ident(name, ctx)),
        Expr::Member { object, property } => eval_member(object, property, ctx),
        Expr::Call { callee, args } => eval_call(callee, args, ctx),
        Expr::Binary { op, left, right } => eval_binary(*op, left, right, ctx),
        Expr::Unary { op, operand } => eval_unary(*op, operand, ctx),
        Expr::Ternary {
            condition,
            then_branch,
            else_branch,
        } => eval_ternary(condition, then_branch, else_branch, ctx),
        Expr::Array(elements) => eval_array(elements, ctx),
    }
}

/// Evaluate an expression and return its boolean value.
pub fn eval_bool(expr: &Expr, ctx: &EvalContext) -> Result<bool, EvalError> {
    let v = eval(expr, ctx)?;
    Ok(v.to_bool())
}

fn eval_literal(lit: &LiteralValue) -> Value {
    match lit {
        LiteralValue::Null => Value::Null,
        LiteralValue::Bool(b) => Value::Bool(*b),
        LiteralValue::Number(n) => Value::Number(*n),
        LiteralValue::String(s) => Value::String(s.clone()),
        LiteralValue::Regex(re) => Value::Regex(CachedRegex(re.clone())),
    }
}

fn eval_ident(name: &str, ctx: &EvalContext) -> Value {
    match name {
        "auth" => {
            if let Some(ref auth) = ctx.auth {
                // Arc::clone is O(1) - just increments refcount
                Value::Object(Arc::clone(auth))
            } else {
                Value::Null
            }
        }

        "data" => {
            if let Some(ref snap) = ctx.data {
                Value::Snapshot(snap.child("")) // Return snapshot wrapper
            } else {
                Value::Null
            }
        }

        "newData" => {
            if let Some(ref snap) = ctx.new_data {
                Value::Snapshot(snap.child(""))
            } else {
                Value::Null
            }
        }

        "root" => {
            if let Some(ref snap) = ctx.root {
                Value::Snapshot(snap.child(""))
            } else {
                Value::Null
            }
        }

        "now" => Value::Number(ctx.now as f64),

        "query" => {
            if let Some(ref query) = ctx.query {
                Value::Object(Arc::clone(query))
            } else {
                Value::Null
            }
        }

        "lark" => {
            let mut map = HashMap::new();
            map.insert(
                "databaseId".to_string(),
                JsonValue::String(ctx.database_id.clone()),
            );
            map.insert(
                "projectId".to_string(),
                JsonValue::String(ctx.project_id.clone()),
            );
            Value::Object(Arc::new(map))
        }

        "true" => Value::Bool(true),
        "false" => Value::Bool(false),
        "null" => Value::Null,

        _ => {
            // Check for wildcard captures ($userId, etc.)
            let capture_name = name.strip_prefix('$').unwrap_or(name);

            if let Some(val) = ctx.captures.get(capture_name) {
                Value::String(val.clone())
            } else if let Some(val) = ctx.captures.get(name) {
                Value::String(val.clone())
            } else {
                Value::Null
            }
        }
    }
}

fn eval_member(object: &Expr, property: &str, ctx: &EvalContext) -> Result<Value, EvalError> {
    let obj = eval(object, ctx)?;
    Ok(obj.get_property(property))
}

fn eval_call(callee: &Expr, args: &[Expr], ctx: &EvalContext) -> Result<Value, EvalError> {
    // Evaluate arguments first
    let mut arg_values = Vec::with_capacity(args.len());
    for arg in args {
        arg_values.push(eval(arg, ctx)?);
    }

    // Check if callee is a member expression (method call)
    if let Expr::Member { object, property } = callee {
        let obj = eval(object, ctx)?;
        // call_method returns Result to propagate both NeedsPromotion and
        // evaluation errors (e.g. an invalid runtime regex in matches() — see L-4).
        return obj.call_method(property, &arg_values);
    }

    // Otherwise it's a direct function call - not supported in rules
    Ok(Value::Null)
}

fn eval_binary(
    op: BinaryOp,
    left: &Expr,
    right: &Expr,
    ctx: &EvalContext,
) -> Result<Value, EvalError> {
    // Short-circuit evaluation for && and ||
    if op == BinaryOp::And {
        let left_val = eval(left, ctx)?;
        if !left_val.to_bool() {
            return Ok(Value::Bool(false));
        }
        let right_val = eval(right, ctx)?;
        return Ok(Value::Bool(right_val.to_bool()));
    }

    if op == BinaryOp::Or {
        let left_val = eval(left, ctx)?;
        if left_val.to_bool() {
            return Ok(Value::Bool(true));
        }
        let right_val = eval(right, ctx)?;
        return Ok(Value::Bool(right_val.to_bool()));
    }

    // Evaluate both sides for other operators
    let left_val = eval(left, ctx)?;
    let right_val = eval(right, ctx)?;

    let result = match op {
        BinaryOp::StrictEq => Value::Bool(strict_equals(&left_val, &right_val)),
        BinaryOp::StrictNotEq => Value::Bool(!strict_equals(&left_val, &right_val)),
        BinaryOp::Eq => Value::Bool(loose_equals(&left_val, &right_val)),
        BinaryOp::NotEq => Value::Bool(!loose_equals(&left_val, &right_val)),
        // `compare` returns None when operands are incomparable (NaN, e.g. a
        // non-numeric string) — every relational operator is false then, as in JS.
        BinaryOp::Lt => Value::Bool(compare(&left_val, &right_val) == Some(Ordering::Less)),
        BinaryOp::Gt => Value::Bool(compare(&left_val, &right_val) == Some(Ordering::Greater)),
        BinaryOp::Lte => Value::Bool(matches!(
            compare(&left_val, &right_val),
            Some(Ordering::Less | Ordering::Equal)
        )),
        BinaryOp::Gte => Value::Bool(matches!(
            compare(&left_val, &right_val),
            Some(Ordering::Greater | Ordering::Equal)
        )),
        BinaryOp::Add => add(&left_val, &right_val),
        BinaryOp::Sub => subtract(&left_val, &right_val),
        BinaryOp::Mul => multiply(&left_val, &right_val),
        BinaryOp::Div => divide(&left_val, &right_val),
        BinaryOp::Mod => modulo(&left_val, &right_val),
        BinaryOp::And | BinaryOp::Or => unreachable!(), // Handled above
    };

    Ok(result)
}

fn eval_unary(op: UnaryOp, operand: &Expr, ctx: &EvalContext) -> Result<Value, EvalError> {
    let val = eval(operand, ctx)?;

    let result = match op {
        UnaryOp::Not => Value::Bool(!val.to_bool()),
        UnaryOp::Neg => Value::Number(-val.to_number()),
    };

    Ok(result)
}

fn eval_ternary(
    condition: &Expr,
    then_branch: &Expr,
    else_branch: &Expr,
    ctx: &EvalContext,
) -> Result<Value, EvalError> {
    let cond_val = eval(condition, ctx)?;

    if cond_val.to_bool() {
        eval(then_branch, ctx)
    } else {
        eval(else_branch, ctx)
    }
}

fn eval_array(elements: &[Expr], ctx: &EvalContext) -> Result<Value, EvalError> {
    let mut values = Vec::with_capacity(elements.len());

    for elem in elements {
        let val = eval(elem, ctx)?;
        // Convert Value to JsonValue for the array
        let json_val = match val {
            Value::Null => JsonValue::Null,
            Value::Bool(b) => JsonValue::Bool(b),
            Value::Number(n) => JsonValue::Number(
                serde_json::Number::from_f64(n).unwrap_or(serde_json::Number::from(0)),
            ),
            Value::String(s) => JsonValue::String(s),
            _ => JsonValue::Null,
        };
        values.push(json_val);
    }

    Ok(Value::Array(values))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rules::expr::parser::parse;

    fn eval_expr(src: &str) -> Result<Value, String> {
        let expr = parse(src)?;
        let ctx = EvalContext::new();
        eval(&expr, &ctx).map_err(|e| e.to_string())
    }

    fn eval_expr_bool(src: &str) -> Result<bool, String> {
        let expr = parse(src)?;
        let ctx = EvalContext::new();
        eval_bool(&expr, &ctx).map_err(|e| e.to_string())
    }

    #[test]
    fn test_eval_literal_true() {
        assert!(eval_expr_bool("true").unwrap());
    }

    #[test]
    fn test_eval_literal_false() {
        assert!(!eval_expr_bool("false").unwrap());
    }

    #[test]
    fn test_eval_literal_null() {
        assert!(!eval_expr_bool("null").unwrap());
    }

    #[test]
    fn test_eval_number() {
        let val = eval_expr("42").unwrap();
        match val {
            Value::Number(n) => assert_eq!(n, 42.0),
            _ => panic!("expected number"),
        }
    }

    #[test]
    fn test_eval_string() {
        let val = eval_expr("'hello'").unwrap();
        match val {
            Value::String(s) => assert_eq!(s, "hello"),
            _ => panic!("expected string"),
        }
    }

    #[test]
    fn test_matches_invalid_regex_denies_even_negated() {
        // Audit L-4: a dynamically-constructed regex that fails to compile must
        // surface as an evaluation error (which the evaluator maps to deny),
        // both directly and — critically — under negation, where returning a
        // bare `false` used to flip to `true` and grant.
        assert!(eval_expr_bool("'x'.matches('[bad')").is_err());
        assert!(eval_expr_bool("!'x'.matches('[bad')").is_err());
        // A valid dynamic pattern still evaluates normally.
        assert!(eval_expr_bool("'x'.matches('[a-z]')").unwrap());
    }

    #[test]
    fn test_eval_strict_eq() {
        assert!(eval_expr_bool("1 === 1").unwrap());
        assert!(!eval_expr_bool("1 === 2").unwrap());
        assert!(eval_expr_bool("'a' === 'a'").unwrap());
        assert!(!eval_expr_bool("1 === true").unwrap()); // Different types
    }

    #[test]
    fn test_eval_strict_not_eq() {
        assert!(eval_expr_bool("1 !== 2").unwrap());
        assert!(!eval_expr_bool("1 !== 1").unwrap());
    }

    #[test]
    fn test_eval_and() {
        assert!(eval_expr_bool("true && true").unwrap());
        assert!(!eval_expr_bool("true && false").unwrap());
        assert!(!eval_expr_bool("false && true").unwrap());
    }

    #[test]
    fn test_eval_or() {
        assert!(eval_expr_bool("true || false").unwrap());
        assert!(eval_expr_bool("false || true").unwrap());
        assert!(!eval_expr_bool("false || false").unwrap());
    }

    #[test]
    fn test_eval_not() {
        assert!(!eval_expr_bool("!true").unwrap());
        assert!(eval_expr_bool("!false").unwrap());
        assert!(eval_expr_bool("!null").unwrap());
    }

    #[test]
    fn test_eval_comparison() {
        assert!(eval_expr_bool("1 < 2").unwrap());
        assert!(eval_expr_bool("2 > 1").unwrap());
        assert!(eval_expr_bool("1 <= 1").unwrap());
        assert!(eval_expr_bool("1 >= 1").unwrap());
    }

    #[test]
    fn test_eval_arithmetic() {
        let val = eval_expr("1 + 2").unwrap();
        assert!(matches!(val, Value::Number(n) if n == 3.0));

        let val = eval_expr("5 - 3").unwrap();
        assert!(matches!(val, Value::Number(n) if n == 2.0));

        let val = eval_expr("3 * 4").unwrap();
        assert!(matches!(val, Value::Number(n) if n == 12.0));

        let val = eval_expr("10 / 2").unwrap();
        assert!(matches!(val, Value::Number(n) if n == 5.0));
    }

    #[test]
    fn test_eval_ternary() {
        let val = eval_expr("true ? 1 : 2").unwrap();
        assert!(matches!(val, Value::Number(n) if n == 1.0));

        let val = eval_expr("false ? 1 : 2").unwrap();
        assert!(matches!(val, Value::Number(n) if n == 2.0));
    }

    #[test]
    fn test_eval_string_method() {
        assert!(eval_expr_bool("'hello'.contains('ell')").unwrap());
        assert!(!eval_expr_bool("'hello'.contains('xyz')").unwrap());
        assert!(eval_expr_bool("'hello'.startsWith('hel')").unwrap());
        assert!(eval_expr_bool("'hello'.endsWith('llo')").unwrap());
    }

    #[test]
    fn test_eval_string_length() {
        let val = eval_expr("'hello'.length").unwrap();
        assert!(matches!(val, Value::Number(n) if n == 5.0));
    }

    #[test]
    fn test_eval_string_length_utf16_semantics() {
        // Rules see JS string `.length` = UTF-16 code units.
        // Bytes or codepoints would let multibyte input bypass min-length checks
        // (e.g., a `length >= 8` validator satisfied by 4 emoji = 8 bytes).
        let cases = [
            ("'é'.length", 1.0),        // one BMP code unit (UTF-8: 2 bytes)
            ("'🔥'.length", 2.0),       // surrogate pair (UTF-8: 4 bytes)
            ("'🔥🔥🔥🔥'.length", 8.0), // four emoji = 8 code units
            ("'café'.length", 4.0),     // mixed ASCII + accented
        ];
        for (expr, want) in cases {
            let val = eval_expr(expr).unwrap_or_else(|e| panic!("{expr}: {e:?}"));
            assert!(
                matches!(val, Value::Number(n) if n == want),
                "{expr} expected {want}, got {val:?}"
            );
        }
    }

    #[test]
    fn test_eval_auth_null() {
        // Auth is null by default
        assert!(!eval_expr_bool("auth !== null").unwrap());
        assert!(eval_expr_bool("auth === null").unwrap());
    }

    #[test]
    fn test_eval_with_auth() {
        let expr = parse("auth.uid === 'user123'").unwrap();
        let mut ctx = EvalContext::new();
        let mut auth = HashMap::new();
        auth.insert("uid".to_string(), JsonValue::String("user123".to_string()));
        ctx.auth = Some(Arc::new(auth));

        assert!(eval_bool(&expr, &ctx).unwrap());
    }

    #[test]
    fn test_eval_with_captures() {
        let expr = parse("$userId === 'abc'").unwrap();
        let mut ctx = EvalContext::new();
        ctx.captures.insert("userId".to_string(), "abc".to_string());

        assert!(eval_bool(&expr, &ctx).unwrap());
    }

    #[test]
    fn test_eval_lark_variables() {
        let expr = parse("lark.databaseId === 'db1'").unwrap();
        let mut ctx = EvalContext::new();
        ctx.database_id = "db1".to_string();

        assert!(eval_bool(&expr, &ctx).unwrap());
    }

    #[test]
    fn test_eval_now() {
        let val = eval_expr("now").unwrap();
        match val {
            Value::Number(n) => assert!(n > 0.0),
            _ => panic!("expected number"),
        }
    }

    #[test]
    fn test_eval_array() {
        let val = eval_expr("['a', 'b', 'c']").unwrap();
        match val {
            Value::Array(arr) => assert_eq!(arr.len(), 3),
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_eval_short_circuit_and() {
        // If left is false, right should not be evaluated
        // (In real usage, this prevents errors when right would fail)
        assert!(!eval_expr_bool("false && invalid").unwrap());
    }

    #[test]
    fn test_eval_short_circuit_or() {
        // If left is true, right should not be evaluated
        assert!(eval_expr_bool("true || invalid").unwrap());
    }

    #[test]
    fn test_eval_complex_expression() {
        // (a && b) || c
        assert!(eval_expr_bool("(true && false) || true").unwrap());
        assert!(!eval_expr_bool("(true && false) || false").unwrap());
    }

    #[test]
    fn test_eval_nested_ternary() {
        let val = eval_expr("true ? (false ? 1 : 2) : 3").unwrap();
        assert!(matches!(val, Value::Number(n) if n == 2.0));
    }

    #[test]
    fn test_eval_string_matches() {
        assert!(eval_expr_bool("'hello123'.matches('[a-z]+[0-9]+')").unwrap());
        assert!(!eval_expr_bool("'hello'.matches('^[0-9]+$')").unwrap());
    }

    #[test]
    fn test_eval_string_matches_regex_literal() {
        // Regex literal syntax
        assert!(eval_expr_bool("'hello123'.matches(/[a-z]+[0-9]+/)").unwrap());
        assert!(!eval_expr_bool("'hello'.matches(/^[0-9]+$/)").unwrap());
        assert!(eval_expr_bool("'foo'.matches(/^foo/)").unwrap());
        assert!(!eval_expr_bool("'bar'.matches(/^foo/)").unwrap());
    }

    #[test]
    fn test_eval_string_matches_case_insensitive() {
        // /i flag for case-insensitive matching
        assert!(eval_expr_bool("'Hello'.matches(/^hello$/i)").unwrap());
        assert!(!eval_expr_bool("'Hello'.matches(/^hello$/)").unwrap());
        assert!(eval_expr_bool("'FOO@BAR.COM'.matches(/^[a-z]+@[a-z]+\\.[a-z]+$/i)").unwrap());
    }

    #[test]
    fn test_eval_string_matches_regex_escaped_slash() {
        // \/ in regex literal = literal /
        assert!(eval_expr_bool(r"'http://x'.matches(/http:\/\//)").unwrap());
    }

    // =========================================================================
    // Tests for EvalError and NeedsPromotion propagation
    // =========================================================================

    use crate::rules::snapshot::NeedsPromotion;

    /// A mock Snapshot that always returns NeedsPromotion (simulates unloaded blob data).
    #[derive(Debug)]
    struct NeedsPromotionSnapshot {
        path: String,
    }

    impl Snapshot for NeedsPromotionSnapshot {
        fn val(&self) -> Result<Option<JsonValue>, NeedsPromotion> {
            Err(NeedsPromotion {
                path: self.path.clone(),
            })
        }

        fn exists(&self) -> Result<bool, NeedsPromotion> {
            Err(NeedsPromotion {
                path: self.path.clone(),
            })
        }

        fn has_child(&self, _name: &str) -> Result<bool, NeedsPromotion> {
            Err(NeedsPromotion {
                path: self.path.clone(),
            })
        }

        fn has_children(&self, _names: &[String]) -> Result<bool, NeedsPromotion> {
            Err(NeedsPromotion {
                path: self.path.clone(),
            })
        }

        fn child(&self, _path: &str) -> Box<dyn Snapshot> {
            Box::new(NeedsPromotionSnapshot {
                path: self.path.clone(),
            })
        }

        fn parent(&self) -> Box<dyn Snapshot> {
            Box::new(NeedsPromotionSnapshot {
                path: self.path.clone(),
            })
        }

        fn is_string(&self) -> Result<bool, NeedsPromotion> {
            Err(NeedsPromotion {
                path: self.path.clone(),
            })
        }

        fn is_number(&self) -> Result<bool, NeedsPromotion> {
            Err(NeedsPromotion {
                path: self.path.clone(),
            })
        }

        fn is_boolean(&self) -> Result<bool, NeedsPromotion> {
            Err(NeedsPromotion {
                path: self.path.clone(),
            })
        }

        fn get_priority(&self) -> Result<Option<JsonValue>, NeedsPromotion> {
            Err(NeedsPromotion {
                path: self.path.clone(),
            })
        }
    }

    #[test]
    fn test_eval_error_from_string() {
        let err = EvalError::from("some error".to_string());
        match err {
            EvalError::Error(s) => assert_eq!(s, "some error"),
            _ => panic!("expected EvalError::Error"),
        }
    }

    #[test]
    fn test_eval_error_from_needs_promotion() {
        let needs = NeedsPromotion {
            path: "/users/-abc123".to_string(),
        };
        let err = EvalError::from(needs);
        match err {
            EvalError::NeedsPromotion(n) => assert_eq!(n.path, "/users/-abc123"),
            _ => panic!("expected EvalError::NeedsPromotion"),
        }
    }

    #[test]
    fn test_eval_error_display() {
        let err1 = EvalError::Error("something went wrong".to_string());
        assert_eq!(err1.to_string(), "something went wrong");

        let err2 = EvalError::NeedsPromotion(NeedsPromotion {
            path: "/items/-xyz".to_string(),
        });
        assert_eq!(err2.to_string(), "needs promotion: /items/-xyz");
    }

    #[test]
    fn test_snapshot_val_needs_promotion_propagates() {
        // Create a context with a unloaded blob data snapshot as 'data'
        let expr = parse("data.val()").unwrap();
        let mut ctx = EvalContext::new();
        ctx.data = Some(Box::new(NeedsPromotionSnapshot {
            path: "/users/-abc123".to_string(),
        }));

        let result = eval(&expr, &ctx);
        assert!(result.is_err());

        match result.unwrap_err() {
            EvalError::NeedsPromotion(n) => {
                assert_eq!(n.path, "/users/-abc123");
            }
            _ => panic!("expected NeedsPromotion error"),
        }
    }

    #[test]
    fn test_snapshot_exists_needs_promotion_propagates() {
        let expr = parse("data.exists()").unwrap();
        let mut ctx = EvalContext::new();
        ctx.data = Some(Box::new(NeedsPromotionSnapshot {
            path: "/items/-xyz789".to_string(),
        }));

        let result = eval(&expr, &ctx);
        assert!(result.is_err());

        match result.unwrap_err() {
            EvalError::NeedsPromotion(n) => {
                assert_eq!(n.path, "/items/-xyz789");
            }
            _ => panic!("expected NeedsPromotion error"),
        }
    }

    #[test]
    fn test_snapshot_has_child_needs_promotion_propagates() {
        let expr = parse("data.hasChild('name')").unwrap();
        let mut ctx = EvalContext::new();
        ctx.data = Some(Box::new(NeedsPromotionSnapshot {
            path: "/users/-def456".to_string(),
        }));

        let result = eval(&expr, &ctx);
        assert!(result.is_err());

        match result.unwrap_err() {
            EvalError::NeedsPromotion(n) => {
                assert_eq!(n.path, "/users/-def456");
            }
            _ => panic!("expected NeedsPromotion error"),
        }
    }

    #[test]
    fn test_snapshot_has_children_needs_promotion_propagates() {
        let expr = parse("data.hasChildren(['name', 'age'])").unwrap();
        let mut ctx = EvalContext::new();
        ctx.data = Some(Box::new(NeedsPromotionSnapshot {
            path: "/users/-ghi789".to_string(),
        }));

        let result = eval(&expr, &ctx);
        assert!(result.is_err());

        match result.unwrap_err() {
            EvalError::NeedsPromotion(n) => {
                assert_eq!(n.path, "/users/-ghi789");
            }
            _ => panic!("expected NeedsPromotion error"),
        }
    }

    #[test]
    fn test_snapshot_is_string_needs_promotion_propagates() {
        let expr = parse("data.isString()").unwrap();
        let mut ctx = EvalContext::new();
        ctx.data = Some(Box::new(NeedsPromotionSnapshot {
            path: "/data/-cold".to_string(),
        }));

        let result = eval(&expr, &ctx);
        assert!(result.is_err());

        match result.unwrap_err() {
            EvalError::NeedsPromotion(n) => {
                assert_eq!(n.path, "/data/-cold");
            }
            _ => panic!("expected NeedsPromotion error"),
        }
    }

    #[test]
    fn test_snapshot_get_priority_needs_promotion_propagates() {
        let expr = parse("data.getPriority()").unwrap();
        let mut ctx = EvalContext::new();
        ctx.data = Some(Box::new(NeedsPromotionSnapshot {
            path: "/priority/-test".to_string(),
        }));

        let result = eval(&expr, &ctx);
        assert!(result.is_err());

        match result.unwrap_err() {
            EvalError::NeedsPromotion(n) => {
                assert_eq!(n.path, "/priority/-test");
            }
            _ => panic!("expected NeedsPromotion error"),
        }
    }

    #[test]
    fn test_short_circuit_and_avoids_unloaded_blob() {
        // If left side is false, right side should NOT be evaluated
        // This tests that short-circuit evaluation prevents unnecessary unloaded blob data access
        let expr = parse("false && data.val()").unwrap();
        let mut ctx = EvalContext::new();
        ctx.data = Some(Box::new(NeedsPromotionSnapshot {
            path: "/never/-accessed".to_string(),
        }));

        // Should succeed because data.val() is never called
        let result = eval_bool(&expr, &ctx);
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    #[test]
    fn test_short_circuit_or_avoids_unloaded_blob() {
        // If left side is true, right side should NOT be evaluated
        let expr = parse("true || data.exists()").unwrap();
        let mut ctx = EvalContext::new();
        ctx.data = Some(Box::new(NeedsPromotionSnapshot {
            path: "/never/-accessed".to_string(),
        }));

        // Should succeed because data.exists() is never called
        let result = eval_bool(&expr, &ctx);
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[test]
    fn test_ternary_condition_false_avoids_unloaded_blob_in_then() {
        // When condition is false, 'then' branch should NOT be evaluated
        let expr = parse("false ? data.val() : 'default'").unwrap();
        let mut ctx = EvalContext::new();
        ctx.data = Some(Box::new(NeedsPromotionSnapshot {
            path: "/never/-accessed".to_string(),
        }));

        let result = eval(&expr, &ctx);
        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), Value::String(s) if s == "default"));
    }

    #[test]
    fn test_ternary_condition_true_avoids_unloaded_blob_in_else() {
        // When condition is true, 'else' branch should NOT be evaluated
        let expr = parse("true ? 'value' : data.val()").unwrap();
        let mut ctx = EvalContext::new();
        ctx.data = Some(Box::new(NeedsPromotionSnapshot {
            path: "/never/-accessed".to_string(),
        }));

        let result = eval(&expr, &ctx);
        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), Value::String(s) if s == "value"));
    }

    #[test]
    fn test_child_navigation_does_not_trigger_promotion() {
        // child() is free - just path manipulation
        let expr = parse("data.child('foo')").unwrap();
        let mut ctx = EvalContext::new();
        ctx.data = Some(Box::new(NeedsPromotionSnapshot {
            path: "/users/-abc".to_string(),
        }));

        // child() should succeed (returns new Snapshot)
        let result = eval(&expr, &ctx);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parent_navigation_does_not_trigger_promotion() {
        // parent() is free - just path manipulation
        let expr = parse("data.parent()").unwrap();
        let mut ctx = EvalContext::new();
        ctx.data = Some(Box::new(NeedsPromotionSnapshot {
            path: "/users/-abc".to_string(),
        }));

        // parent() should succeed (returns new Snapshot)
        let result = eval(&expr, &ctx);
        assert!(result.is_ok());
    }
}
