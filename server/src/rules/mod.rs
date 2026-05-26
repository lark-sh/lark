//! Security rules engine.
//!
//! This module implements security rules evaluation for Lark databases.
//! Rules are JSON objects with `.read`, `.write`, and `.validate` expressions
//! that control access to data.
//!
//! # Example Rules
//!
//! ```json
//! {
//!   "rules": {
//!     "users": {
//!       "$uid": {
//!         ".read": "auth.uid === $uid",
//!         ".write": "auth.uid === $uid"
//!       }
//!     }
//!   }
//! }
//! ```

mod evaluator;
pub mod expr;
mod parser;
mod snapshot;

pub use evaluator::{Evaluator, RulesContext, default_rules};
pub use parser::{CompiledExpr, RuleNode, RuleSet, parse_path};
pub use snapshot::{
    AuthInfo, EmptyTree, LazySnapshot, NeedsPromotion, NewData, Snapshot, TreeGetter,
};

/// Parse rules from a JSON value.
pub fn parse_rules(rules_json: &serde_json::Value) -> Result<RuleSet, String> {
    let json_str = serde_json::to_string(rules_json)
        .map_err(|e| format!("failed to serialize rules: {}", e))?;
    RuleSet::parse(&json_str)
}
