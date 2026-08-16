//! Helpers for detecting Python UDFs anywhere inside a logical plan.
//!
//! The standalone runtime uses this to decide whether a serialized plan can be
//! executed by the pure-Rust execution engine, or must be handed to a Python
//! worker subprocess (which owns the interpreter needed to unpickle and run
//! cloudpickled UDFs).

use std::sync::Arc;

use daft_dsl::{Expr, ExprRef, WindowExpr, udf::expr_contains_python_udf};

use crate::{LogicalPlan, LogicalPlanRef};

/// Returns true if any node in the plan tree contains a Python UDF.
///
/// This walks every plan node (including both sides of joins and set
/// operations) and inspects every expression owned by that node:
///
/// * projections / filters / aggregates / joins / sorts / window functions
/// * batch ``UDFProject`` nodes
/// * ``VLLMProject`` nodes, which are always executed through the Python worker
pub fn plan_contains_python_udf(plan: &LogicalPlanRef) -> bool {
    plan_node_contains_python_udf(plan.as_ref())
}

fn plan_node_contains_python_udf(plan: &LogicalPlan) -> bool {
    node_exprs_contain_python_udf(plan)
        || plan
            .children()
            .iter()
            .any(|child| plan_node_contains_python_udf(child))
}

/// Check only the expressions owned directly by `plan` (no recursion).
fn node_exprs_contain_python_udf(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Project(project) => exprs_contain_python_udf(&project.projection),
        LogicalPlan::UDFProject(udf_project) => {
            expr_contains_python_udf(&udf_project.expr)
                || exprs_contain_python_udf(&udf_project.passthrough_columns)
        }
        LogicalPlan::Filter(filter) => expr_contains_python_udf(&filter.predicate),
        LogicalPlan::Aggregate(aggregate) => {
            exprs_contain_python_udf(&aggregate.aggregations)
                || exprs_contain_python_udf(&aggregate.groupby)
        }
        LogicalPlan::Join(join) => join.on.inner().is_some_and(expr_contains_python_udf),
        LogicalPlan::Window(window) => {
            window_exprs_contain_python_udf(&window.window_functions)
                || exprs_contain_python_udf(&window.window_spec.partition_by)
                || exprs_contain_python_udf(&window.window_spec.order_by)
        }
        LogicalPlan::Explode(explode) => exprs_contain_python_udf(&explode.to_explode),
        LogicalPlan::Pivot(pivot) => {
            exprs_contain_python_udf(&pivot.group_by)
                || expr_contains_python_udf(&pivot.pivot_column)
                || expr_contains_python_udf(&pivot.value_column)
                || expr_contains_python_udf(&Arc::new(Expr::Agg(pivot.aggregation.clone())))
        }
        LogicalPlan::Sort(sort) => exprs_contain_python_udf(&sort.sort_by),
        LogicalPlan::TopN(top_n) => exprs_contain_python_udf(&top_n.sort_by),
        LogicalPlan::VLLMProject(_) => true,
        LogicalPlan::Source(_) | LogicalPlan::Sink(_) => false,
        // Nodes without expression payloads (shard, limit, repartition,
        // concat, union, sample, shuffle, subquery alias, ...) carry no UDFs
        // of their own; the recursion into `children()` still visits them.
        _ => false,
    }
}

fn exprs_contain_python_udf(exprs: &[ExprRef]) -> bool {
    exprs.iter().any(expr_contains_python_udf)
}

fn window_exprs_contain_python_udf(window_exprs: &[WindowExpr]) -> bool {
    window_exprs.iter().any(|window_expr| match window_expr {
        WindowExpr::Agg(agg_expr) => {
            expr_contains_python_udf(&Arc::new(Expr::Agg(agg_expr.clone())))
        }
        WindowExpr::Offset { input, default, .. } => {
            expr_contains_python_udf(input) || default.as_ref().is_some_and(expr_contains_python_udf)
        }
        WindowExpr::RowNumber | WindowExpr::Rank | WindowExpr::DenseRank => false,
    })
}
