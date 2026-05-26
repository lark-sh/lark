//! AST node types for rule expressions.

/// Expression node in the AST.
#[derive(Debug, Clone)]
pub enum Expr {
    /// Literal value (string, number, boolean, null).
    Literal(LiteralValue),

    /// Identifier (variable name like `auth`, `$userId`).
    Ident(String),

    /// Member access (obj.prop).
    Member { object: Box<Expr>, property: String },

    /// Function/method call (fn(args)).
    Call { callee: Box<Expr>, args: Vec<Expr> },

    /// Binary operation (left op right).
    Binary {
        op: BinaryOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },

    /// Unary operation (op operand).
    Unary { op: UnaryOp, operand: Box<Expr> },

    /// Ternary expression (cond ? then : else).
    Ternary {
        condition: Box<Expr>,
        then_branch: Box<Expr>,
        else_branch: Box<Expr>,
    },

    /// Array literal ([a, b, c]).
    Array(Vec<Expr>),
}

impl Expr {
    /// Count the total number of nodes in this expression tree.
    /// Used for compile-time complexity limits.
    pub fn count_nodes(&self) -> usize {
        match self {
            Expr::Literal(_) => 1,
            Expr::Ident(_) => 1,
            Expr::Member { object, .. } => 1 + object.count_nodes(),
            Expr::Call { callee, args } => {
                1 + callee.count_nodes() + args.iter().map(|a| a.count_nodes()).sum::<usize>()
            }
            Expr::Binary { left, right, .. } => 1 + left.count_nodes() + right.count_nodes(),
            Expr::Unary { operand, .. } => 1 + operand.count_nodes(),
            Expr::Ternary {
                condition,
                then_branch,
                else_branch,
            } => {
                1 + condition.count_nodes() + then_branch.count_nodes() + else_branch.count_nodes()
            }
            Expr::Array(elements) => 1 + elements.iter().map(|e| e.count_nodes()).sum::<usize>(),
        }
    }
}

/// Literal value types.
#[derive(Debug, Clone)]
pub enum LiteralValue {
    Null,
    Bool(bool),
    Number(f64),
    String(String),
    /// Pre-compiled regex pattern (compiled at parse time for matches() calls).
    Regex(regex::Regex),
}

/// Binary operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    // Equality
    StrictEq,    // ===
    StrictNotEq, // !==
    Eq,          // ==
    NotEq,       // !=

    // Comparison
    Lt,  // <
    Gt,  // >
    Lte, // <=
    Gte, // >=

    // Logical
    And, // &&
    Or,  // ||

    // Arithmetic
    Add, // +
    Sub, // -
    Mul, // *
    Div, // /
    Mod, // %
}

impl BinaryOp {
    /// Get operator from string.
    // Returns Option rather than Result, so it intentionally doesn't implement FromStr.
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "===" => Some(BinaryOp::StrictEq),
            "!==" => Some(BinaryOp::StrictNotEq),
            "==" => Some(BinaryOp::Eq),
            "!=" => Some(BinaryOp::NotEq),
            "<" => Some(BinaryOp::Lt),
            ">" => Some(BinaryOp::Gt),
            "<=" => Some(BinaryOp::Lte),
            ">=" => Some(BinaryOp::Gte),
            "&&" => Some(BinaryOp::And),
            "||" => Some(BinaryOp::Or),
            "+" => Some(BinaryOp::Add),
            "-" => Some(BinaryOp::Sub),
            "*" => Some(BinaryOp::Mul),
            "/" => Some(BinaryOp::Div),
            "%" => Some(BinaryOp::Mod),
            _ => None,
        }
    }

    /// Get operator string representation.
    pub fn as_str(&self) -> &'static str {
        match self {
            BinaryOp::StrictEq => "===",
            BinaryOp::StrictNotEq => "!==",
            BinaryOp::Eq => "==",
            BinaryOp::NotEq => "!=",
            BinaryOp::Lt => "<",
            BinaryOp::Gt => ">",
            BinaryOp::Lte => "<=",
            BinaryOp::Gte => ">=",
            BinaryOp::And => "&&",
            BinaryOp::Or => "||",
            BinaryOp::Add => "+",
            BinaryOp::Sub => "-",
            BinaryOp::Mul => "*",
            BinaryOp::Div => "/",
            BinaryOp::Mod => "%",
        }
    }
}

/// Unary operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Not, // !
    Neg, // -
}

impl UnaryOp {
    pub fn as_str(&self) -> &'static str {
        match self {
            UnaryOp::Not => "!",
            UnaryOp::Neg => "-",
        }
    }
}
