use super::*;
pub(crate) fn plan_ref_to_proto(plan_ref: &PlanRef) -> proto::PlanRef {
    use proto::plan_ref::PlanRef as P;
    let plan_ref = match plan_ref {
        PlanRef::Alias(alias) => P::Alias(alias.to_string()),
        PlanRef::Unqualified => P::Unqualified(true),
        PlanRef::Id(id) => P::Id(*id as u64),
    };
    proto::PlanRef {
        plan_ref: Some(plan_ref),
    }
}

pub(crate) fn plan_ref_from_proto(plan_ref: proto::PlanRef) -> DaftResult<PlanRef> {
    use proto::plan_ref::PlanRef as P;
    match required(plan_ref.plan_ref, "PlanRef oneof")? {
        P::Alias(alias) => Ok(PlanRef::Alias(alias.into())),
        P::Unqualified(_) => Ok(PlanRef::Unqualified),
        P::Id(id) => Ok(PlanRef::Id(id as usize)),
    }
}

pub(crate) fn column_to_proto(column: &Column) -> DaftResult<proto::Column> {
    use proto::column::Column as P;
    let column = match column {
        Column::Unresolved(UnresolvedColumn {
            name,
            plan_ref,
            plan_schema,
        }) => P::Unresolved(proto::UnresolvedColumn {
            name: name.to_string(),
            plan_ref: Some(plan_ref_to_proto(plan_ref)),
            plan_schema: match plan_schema {
                Some(schema) => Some(schema.to_proto()?),
                None => None,
            },
        }),
        Column::Resolved(resolved) => match resolved {
            ResolvedColumn::Basic(name) => P::Resolved(proto::ResolvedColumn {
                resolved: Some(proto::resolved_column::Resolved::Basic(name.to_string())),
            }),
            ResolvedColumn::JoinSide(field, side) => P::Resolved(proto::ResolvedColumn {
                resolved: Some(proto::resolved_column::Resolved::JoinSide(
                    proto::JoinSideColumn {
                        field: Some(field_to_proto(field)?),
                        side: join_side_to_proto(*side) as i32,
                    },
                )),
            }),
            ResolvedColumn::OuterRef(field, plan_ref) => P::Resolved(proto::ResolvedColumn {
                resolved: Some(proto::resolved_column::Resolved::OuterRef(
                    proto::OuterRefColumn {
                        field: Some(field_to_proto(field)?),
                        plan_ref: Some(plan_ref_to_proto(plan_ref)),
                    },
                )),
            }),
        },
        Column::Bound(BoundColumn { index, field }) => P::Bound(proto::BoundColumn {
            index: *index as u64,
            field: Some(field_to_proto(field)?),
        }),
    };
    Ok(proto::Column {
        column: Some(column),
    })
}

#[allow(deprecated)]
pub(crate) fn column_from_proto(column: proto::Column) -> DaftResult<Column> {
    use proto::column::Column as P;
    match required(column.column, "Column oneof")? {
        P::Unresolved(unresolved) => Ok(Column::Unresolved(UnresolvedColumn {
            name: unresolved.name.into(),
            plan_ref: plan_ref_from_proto(required(
                unresolved.plan_ref,
                "UnresolvedColumn.plan_ref",
            )?)?,
            plan_schema: match unresolved.plan_schema {
                Some(schema) => Some(schema.into_daft()?),
                None => None,
            },
        })),
        P::Resolved(resolved) => match required(resolved.resolved, "ResolvedColumn oneof")? {
            proto::resolved_column::Resolved::Basic(name) => {
                Ok(Column::Resolved(ResolvedColumn::Basic(name.into())))
            }
            proto::resolved_column::Resolved::JoinSide(join_side) => {
                Ok(Column::Resolved(ResolvedColumn::JoinSide(
                    proto_field_into_daft(required(join_side.field, "JoinSideColumn.field")?)?,
                    join_side_from_proto(
                        proto::JoinSide::try_from(join_side.side)
                            .unwrap_or(proto::JoinSide::Unspecified),
                    )?,
                )))
            }
            proto::resolved_column::Resolved::OuterRef(outer_ref) => {
                Ok(Column::Resolved(ResolvedColumn::OuterRef(
                    proto_field_into_daft(required(outer_ref.field, "OuterRefColumn.field")?)?,
                    plan_ref_from_proto(required(outer_ref.plan_ref, "OuterRefColumn.plan_ref")?)?,
                )))
            }
        },
        P::Bound(bound) => Ok(Column::Bound(BoundColumn {
            index: bound.index as usize,
            field: proto_field_into_daft(required(bound.field, "BoundColumn.field")?)?,
        })),
    }
}

pub fn expr_to_proto(expr: &ExprRef) -> DaftResult<proto::Expression> {
    use proto::expression::Expr as P;
    let expr = expr.as_ref();
    let expr = match expr {
        Expr::Column(column) => P::Column(column_to_proto(column)?),
        Expr::Alias(child, name) => P::Alias(Box::new(proto::AliasExpr {
            child: Some(Box::new(expr_to_proto(child)?)),
            name: name.to_string(),
        })),
        Expr::Agg(agg) => P::Agg(Box::new(agg_expr_to_proto(agg)?)),
        Expr::BinaryOp { op, left, right } => P::BinaryOp(Box::new(proto::BinaryOpExpr {
            op: operator_to_proto(*op) as i32,
            left: Some(Box::new(expr_to_proto(left)?)),
            right: Some(Box::new(expr_to_proto(right)?)),
        })),
        Expr::Cast(child, dtype) => P::Cast(Box::new(proto::CastExpr {
            child: Some(Box::new(expr_to_proto(child)?)),
            dtype: Some(dtype.to_proto()?),
        })),
        Expr::Function { func, inputs } => P::Function(function_expr_to_proto(func, inputs)?),
        Expr::Over(window_expr, spec) => P::Over(Box::new(proto::OverExpr {
            window_expr: Some(Box::new(window_expr_to_proto(window_expr)?)),
            window_spec: Some(window_spec_to_proto(spec)?),
        })),
        Expr::WindowFunction(window_expr) => {
            P::WindowFunction(Box::new(window_expr_to_proto(window_expr)?))
        }
        Expr::Not(child) => P::Not(Box::new(expr_to_proto(child)?)),
        Expr::IsNull(child) => P::IsNull(Box::new(expr_to_proto(child)?)),
        Expr::NotNull(child) => P::NotNull(Box::new(expr_to_proto(child)?)),
        Expr::FillNull(child, value) => P::FillNull(Box::new(proto::FillNullExpr {
            child: Some(Box::new(expr_to_proto(child)?)),
            value: Some(Box::new(expr_to_proto(value)?)),
        })),
        Expr::IsIn(child, values) => P::IsIn(Box::new(proto::IsInExpr {
            child: Some(Box::new(expr_to_proto(child)?)),
            values: values
                .iter()
                .map(expr_to_proto)
                .collect::<DaftResult<_>>()?,
        })),
        Expr::Between(child, lower, upper) => P::Between(Box::new(proto::BetweenExpr {
            child: Some(Box::new(expr_to_proto(child)?)),
            lower: Some(Box::new(expr_to_proto(lower)?)),
            upper: Some(Box::new(expr_to_proto(upper)?)),
        })),
        Expr::List(exprs) => P::List(proto::ExpressionList {
            items: exprs
                .iter()
                .map(expr_to_proto)
                .collect::<DaftResult<_>>()?,
        }),
        Expr::Literal(literal) => P::Literal(literal.to_proto()?),
        Expr::IfElse {
            if_true,
            if_false,
            predicate,
        } => P::IfElse(Box::new(proto::IfElseExpr {
            if_true: Some(Box::new(expr_to_proto(if_true)?)),
            if_false: Some(Box::new(expr_to_proto(if_false)?)),
            predicate: Some(Box::new(expr_to_proto(predicate)?)),
        })),
        Expr::ScalarFn(scalar_fn) => P::ScalarFn(scalar_fn_to_proto(scalar_fn)?),
        Expr::Subquery(_) => {
            return unsupported("Expr::Subquery (internal plan refs are not serializable)")
        }
        Expr::InSubquery(..) => {
            return unsupported("Expr::InSubquery (internal plan refs are not serializable)")
        }
        Expr::Exists(_) => {
            return unsupported("Expr::Exists (internal plan refs are not serializable)")
        }
        Expr::Coalesce(exprs) => P::Coalesce(proto::ExpressionList {
            items: exprs
                .iter()
                .map(expr_to_proto)
                .collect::<DaftResult<_>>()?,
        }),
        Expr::VLLM(vllm) => P::Vllm(Box::new(vllm_expr_to_proto(vllm)?)),
    };
    Ok(proto::Expression { expr: Some(expr) })
}

pub fn expr_from_proto(expr: proto::Expression) -> DaftResult<ExprRef> {
    use proto::expression::Expr as P;
    let expr = match required(expr.expr, "Expression oneof")? {
        P::Column(column) => Expr::Column(column_from_proto(column)?),
        P::Alias(alias) => Expr::Alias(
            required_expr(alias.child, "AliasExpr.child")?,
            alias.name.into(),
        ),
        P::Agg(agg) => Expr::Agg(agg_expr_from_proto(*agg)?),
        P::BinaryOp(binary_op) => Expr::BinaryOp {
            op: operator_from_proto(
                proto::Operator::try_from(binary_op.op).unwrap_or(proto::Operator::Unspecified),
            )?,
            left: required_expr(binary_op.left, "BinaryOpExpr.left")?,
            right: required_expr(binary_op.right, "BinaryOpExpr.right")?,
        },
        P::Cast(cast) => Expr::Cast(
            required_expr(cast.child, "CastExpr.child")?,
            required_dtype(cast.dtype, "CastExpr.dtype")?,
        ),
        P::Function(function) => {
            let (func, inputs) = function_expr_from_proto(function)?;
            Expr::Function { func, inputs }
        }
        P::Over(over) => Expr::Over(
            window_expr_from_proto(*required(
                over.window_expr,
                "OverExpr.window_expr",
            )?)?,
            Arc::new(window_spec_from_proto(required(
                over.window_spec,
                "OverExpr.window_spec",
            )?)?),
        ),
        P::WindowFunction(window) => Expr::WindowFunction(window_expr_from_proto(*window)?),
        P::Not(child) => Expr::Not(expr_from_proto(*child)?),
        P::IsNull(child) => Expr::IsNull(expr_from_proto(*child)?),
        P::NotNull(child) => Expr::NotNull(expr_from_proto(*child)?),
        P::FillNull(fill_null) => Expr::FillNull(
            required_expr(fill_null.child, "FillNullExpr.child")?,
            required_expr(fill_null.value, "FillNullExpr.value")?,
        ),
        P::IsIn(is_in) => Expr::IsIn(
            required_expr(is_in.child, "IsInExpr.child")?,
            is_in
                .values
                .into_iter()
                .map(expr_from_proto)
                .collect::<DaftResult<_>>()?,
        ),
        P::Between(between) => Expr::Between(
            required_expr(between.child, "BetweenExpr.child")?,
            required_expr(between.lower, "BetweenExpr.lower")?,
            required_expr(between.upper, "BetweenExpr.upper")?,
        ),
        P::List(list) => Expr::List(
            list.items
                .into_iter()
                .map(expr_from_proto)
                .collect::<DaftResult<_>>()?,
        ),
        P::Literal(literal) => Expr::Literal(literal.into_daft()?),
        P::IfElse(if_else) => Expr::IfElse {
            if_true: required_expr(if_else.if_true, "IfElseExpr.if_true")?,
            if_false: required_expr(if_else.if_false, "IfElseExpr.if_false")?,
            predicate: required_expr(if_else.predicate, "IfElseExpr.predicate")?,
        },
        P::ScalarFn(scalar_fn) => Expr::ScalarFn(scalar_fn_from_proto(scalar_fn)?),
        P::Subquery(_) => {
            return unsupported("Expr::Subquery (internal plan refs are not serializable)")
        }
        P::InSubquery(_) => {
            return unsupported("Expr::InSubquery (internal plan refs are not serializable)")
        }
        P::Exists(_) => {
            return unsupported("Expr::Exists (internal plan refs are not serializable)")
        }
        P::Coalesce(coalesce) => Expr::Coalesce(
            coalesce
                .items
                .into_iter()
                .map(expr_from_proto)
                .collect::<DaftResult<_>>()?,
        ),
        P::Vllm(vllm) => Expr::VLLM(vllm_expr_from_proto(*vllm)?),
    };
    Ok(Arc::new(expr))
}

// ---------------------------------------------------------------------------
// Logical plan tree
// ---------------------------------------------------------------------------

