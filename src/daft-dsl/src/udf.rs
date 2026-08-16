//! Helpers for detecting Python UDFs inside expression trees.
//!
//! These are used by the standalone runtime to decide whether a plan can be
//! executed by the pure-Rust execution engine or must be handed to a Python
//! worker (which owns the interpreter needed to run cloudpickled UDFs).

use common_treenode::{TreeNode, TreeNodeRecursion};

use crate::{
    AggExpr, Expr, ExprRef,
    functions::{FunctionExpr, scalar::ScalarFn},
};

/// Returns true if `expr` contains any Python UDF anywhere in its subtree.
///
/// This covers:
/// * legacy `@daft.udf` expressions (`FunctionExpr::Python`),
/// * new `@daft.func` / `@daft.cls` scalar functions (`ScalarFn::Python`),
/// * `map_groups` aggregations (both legacy and new batch UDFs),
/// * VLLM expressions, which are executed through an external Python
///   service and are also routed to the Python worker.
pub fn expr_contains_python_udf(expr: &ExprRef) -> bool {
    let mut found = false;
    let _ = expr.apply(|node| {
        let hit = match node.as_ref() {
            Expr::Function {
                func: FunctionExpr::Python(_),
                ..
            } => true,
            Expr::ScalarFn(ScalarFn::Python(_)) => true,
            Expr::Agg(AggExpr::MapGroups { .. }) => true,
            Expr::VLLM(_) => true,
            _ => false,
        };
        if hit {
            found = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    found
}
