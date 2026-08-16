use super::*;
pub(crate) fn agg_expr_to_proto(agg: &AggExpr) -> DaftResult<proto::AggExpression> {
    use proto::agg_expression::Agg as P;
    let agg = match agg {
        AggExpr::Count(child, mode) => P::Count(Box::new(proto::CountAgg {
            child: Some(Box::new(expr_to_proto(child)?)),
            mode: count_mode_to_proto(*mode) as i32,
        })),
        AggExpr::CountDistinct(child) => P::CountDistinct(Box::new(expr_to_proto(child)?)),
        AggExpr::Sum(child) => P::Sum(Box::new(expr_to_proto(child)?)),
        AggExpr::Product(child) => P::Product(Box::new(expr_to_proto(child)?)),
        AggExpr::ApproxPercentile(ApproxPercentileParams {
            child,
            percentiles,
            force_list_output,
        }) => P::ApproxPercentile(Box::new(proto::ApproxPercentileAgg {
            child: Some(Box::new(expr_to_proto(child)?)),
            percentiles: percentiles.iter().map(|p| p.0).collect(),
            force_list_output: *force_list_output,
        })),
        AggExpr::ApproxCountDistinct(child) => {
            P::ApproxCountDistinct(Box::new(expr_to_proto(child)?))
        }
        AggExpr::ApproxSketch(child, sketch_type) => {
            P::ApproxSketch(Box::new(sketch_agg_to_proto(child, *sketch_type)?))
        }
        AggExpr::MergeSketch(child, sketch_type) => {
            P::MergeSketch(Box::new(sketch_agg_to_proto(child, *sketch_type)?))
        }
        AggExpr::Mean(child) => P::Mean(Box::new(expr_to_proto(child)?)),
        AggExpr::Stddev(child, ddof) => P::Stddev(Box::new(ddof_agg_to_proto(child, *ddof)?)),
        AggExpr::Var(child, ddof) => P::Var(Box::new(ddof_agg_to_proto(child, *ddof)?)),
        AggExpr::Min(child) => P::Min(Box::new(expr_to_proto(child)?)),
        AggExpr::Max(child) => P::Max(Box::new(expr_to_proto(child)?)),
        AggExpr::BoolAnd(child) => P::BoolAnd(Box::new(expr_to_proto(child)?)),
        AggExpr::BoolOr(child) => P::BoolOr(Box::new(expr_to_proto(child)?)),
        AggExpr::AnyValue(child, ignore_nulls) => {
            P::AnyValue(Box::new(proto::AnyValueAgg {
                child: Some(Box::new(expr_to_proto(child)?)),
                ignore_nulls: *ignore_nulls,
            }))
        }
        AggExpr::List(child) => P::List(Box::new(expr_to_proto(child)?)),
        AggExpr::Set(child) => P::Set(Box::new(expr_to_proto(child)?)),
        AggExpr::Concat(child, delimiter) => P::Concat(Box::new(proto::ConcatAgg {
            child: Some(Box::new(expr_to_proto(child)?)),
            delimiter: delimiter.clone(),
        })),
        AggExpr::Skew(child) => P::Skew(Box::new(expr_to_proto(child)?)),
        AggExpr::MapGroups { func, inputs } => P::MapGroups(proto::MapGroupsAgg {
            func: Some(map_groups_fn_to_proto(func)?),
            inputs: inputs
                .iter()
                .map(expr_to_proto)
                .collect::<DaftResult<_>>()?,
        }),
    };
    Ok(proto::AggExpression { agg: Some(agg) })
}

pub(crate) fn sketch_agg_to_proto(child: &ExprRef, sketch_type: SketchType) -> DaftResult<proto::SketchAgg> {
    Ok(proto::SketchAgg {
        child: Some(Box::new(expr_to_proto(child)?)),
        sketch_type: sketch_type_to_proto(sketch_type) as i32,
    })
}

pub(crate) fn ddof_agg_to_proto(child: &ExprRef, ddof: usize) -> DaftResult<proto::DdofAgg> {
    Ok(proto::DdofAgg {
        child: Some(Box::new(expr_to_proto(child)?)),
        ddof: ddof as u64,
    })
}

pub(crate) fn agg_expr_from_proto(agg: proto::AggExpression) -> DaftResult<AggExpr> {
    use proto::agg_expression::Agg as P;
    match required(agg.agg, "AggExpression oneof")? {
        P::Count(count) => Ok(AggExpr::Count(
            required_expr(count.child, "CountAgg.child")?,
            count_mode_from_proto(
                proto::CountMode::try_from(count.mode).unwrap_or(proto::CountMode::Unspecified),
            )?,
        )),
        P::CountDistinct(child) => Ok(AggExpr::CountDistinct(expr_from_proto(*child)?)),
        P::Sum(child) => Ok(AggExpr::Sum(expr_from_proto(*child)?)),
        P::Product(child) => Ok(AggExpr::Product(expr_from_proto(*child)?)),
        P::ApproxPercentile(approx) => Ok(AggExpr::ApproxPercentile(
            ApproxPercentileParams {
                child: required_expr(approx.child, "ApproxPercentileAgg.child")?,
                percentiles: approx
                    .percentiles
                    .into_iter()
                    .map(FloatWrapper)
                    .collect(),
                force_list_output: approx.force_list_output,
            },
        )),
        P::ApproxCountDistinct(child) => {
            Ok(AggExpr::ApproxCountDistinct(expr_from_proto(*child)?))
        }
        P::ApproxSketch(sketch) => Ok(AggExpr::ApproxSketch(
            required_expr(sketch.child, "SketchAgg.child")?,
            sketch_type_from_proto(
                proto::SketchType::try_from(sketch.sketch_type)
                    .unwrap_or(proto::SketchType::Unspecified),
            )?,
        )),
        P::MergeSketch(sketch) => Ok(AggExpr::MergeSketch(
            required_expr(sketch.child, "SketchAgg.child")?,
            sketch_type_from_proto(
                proto::SketchType::try_from(sketch.sketch_type)
                    .unwrap_or(proto::SketchType::Unspecified),
            )?,
        )),
        P::Mean(child) => Ok(AggExpr::Mean(expr_from_proto(*child)?)),
        P::Stddev(ddof) => Ok(AggExpr::Stddev(
            required_expr(ddof.child, "DdofAgg.child")?,
            ddof.ddof as usize,
        )),
        P::Var(ddof) => Ok(AggExpr::Var(
            required_expr(ddof.child, "DdofAgg.child")?,
            ddof.ddof as usize,
        )),
        P::Min(child) => Ok(AggExpr::Min(expr_from_proto(*child)?)),
        P::Max(child) => Ok(AggExpr::Max(expr_from_proto(*child)?)),
        P::BoolAnd(child) => Ok(AggExpr::BoolAnd(expr_from_proto(*child)?)),
        P::BoolOr(child) => Ok(AggExpr::BoolOr(expr_from_proto(*child)?)),
        P::AnyValue(any_value) => Ok(AggExpr::AnyValue(
            required_expr(any_value.child, "AnyValueAgg.child")?,
            any_value.ignore_nulls,
        )),
        P::List(child) => Ok(AggExpr::List(expr_from_proto(*child)?)),
        P::Set(child) => Ok(AggExpr::Set(expr_from_proto(*child)?)),
        P::Concat(concat) => Ok(AggExpr::Concat(
            required_expr(concat.child, "ConcatAgg.child")?,
            concat.delimiter,
        )),
        P::Skew(child) => Ok(AggExpr::Skew(expr_from_proto(*child)?)),
        P::MapGroups(map_groups) => Ok(AggExpr::MapGroups {
            func: map_groups_fn_from_proto(required(map_groups.func, "MapGroupsAgg.func")?)?,
            inputs: map_groups
                .inputs
                .into_iter()
                .map(expr_from_proto)
                .collect::<DaftResult<_>>()?,
        }),
    }
}

pub(crate) fn map_groups_fn_to_proto(func: &MapGroupsFn) -> DaftResult<proto::MapGroupsFn> {
    use proto::map_groups_fn::Func as P;
    let func = match func {
        MapGroupsFn::Legacy(udf) => P::Legacy(legacy_python_udf_to_proto(udf)?),
        MapGroupsFn::Python(py_fn) => P::Python(py_scalar_fn_to_proto(py_fn)?),
    };
    Ok(proto::MapGroupsFn { func: Some(func) })
}

pub(crate) fn map_groups_fn_from_proto(func: proto::MapGroupsFn) -> DaftResult<MapGroupsFn> {
    use proto::map_groups_fn::Func as P;
    match required(func.func, "MapGroupsFn oneof")? {
        P::Legacy(udf) => Ok(MapGroupsFn::Legacy(legacy_python_udf_from_proto(udf)?)),
        P::Python(py_fn) => Ok(MapGroupsFn::Python(py_scalar_fn_from_proto(py_fn)?)),
    }
}

