use super::*;
pub(crate) fn partitioning_expr_to_proto(partitioning: &DslPartitioningExpr) -> proto::PartitioningExpr {
    use proto::partitioning_expr::Partitioning as P;
    let partitioning = match partitioning {
        DslPartitioningExpr::Years => P::Years(true),
        DslPartitioningExpr::Months => P::Months(true),
        DslPartitioningExpr::Days => P::Days(true),
        DslPartitioningExpr::Hours => P::Hours(true),
        DslPartitioningExpr::IcebergBucket(n) => P::IcebergBucket(*n),
        DslPartitioningExpr::IcebergTruncate(w) => P::IcebergTruncate(*w),
    };
    proto::PartitioningExpr {
        partitioning: Some(partitioning),
    }
}

pub(crate) fn partitioning_expr_from_proto(
    partitioning: proto::PartitioningExpr,
) -> DaftResult<DslPartitioningExpr> {
    use proto::partitioning_expr::Partitioning as P;
    match required(partitioning.partitioning, "PartitioningExpr oneof")? {
        P::Years(_) => Ok(DslPartitioningExpr::Years),
        P::Months(_) => Ok(DslPartitioningExpr::Months),
        P::Days(_) => Ok(DslPartitioningExpr::Days),
        P::Hours(_) => Ok(DslPartitioningExpr::Hours),
        P::IcebergBucket(n) => Ok(DslPartitioningExpr::IcebergBucket(n)),
        P::IcebergTruncate(w) => Ok(DslPartitioningExpr::IcebergTruncate(w)),
    }
}

pub(crate) fn function_expr_to_proto(
    func: &DslFunctionExpr,
    inputs: &[ExprRef],
) -> DaftResult<proto::FunctionExpr> {
    use proto::function_expr::Func as P;
    let func = match func {
        DslFunctionExpr::Map(DslMapExpr::Get) => P::Map(proto::MapExpr { get: true }),
        DslFunctionExpr::Sketch(DslSketchExpr::Percentile {
            percentiles,
            force_list_output,
        }) => P::Sketch(proto::SketchExpr {
            percentile: Some(proto::PercentileSketch {
                percentiles: percentiles.0.clone(),
                force_list_output: *force_list_output,
            }),
        }),
        DslFunctionExpr::Struct(DslStructExpr::Get(name)) => {
            P::Struct(proto::StructExpr { get: name.clone() })
        }
        DslFunctionExpr::Python(udf) => P::Python(legacy_python_udf_to_proto(udf)?),
        DslFunctionExpr::Partitioning(partitioning) => {
            P::Partitioning(partitioning_expr_to_proto(partitioning))
        }
    };
    Ok(proto::FunctionExpr {
        inputs: inputs
            .iter()
            .map(expr_to_proto)
            .collect::<DaftResult<_>>()?,
        func: Some(func),
    })
}

pub(crate) fn function_expr_from_proto(func: proto::FunctionExpr) -> DaftResult<(DslFunctionExpr, Vec<ExprRef>)> {
    use proto::function_expr::Func as P;
    let inputs = func
        .inputs
        .into_iter()
        .map(expr_from_proto)
        .collect::<DaftResult<_>>()?;
    let func = match required(func.func, "FunctionExpr oneof")? {
        P::Map(map) => {
            if !map.get {
                return invalid("MapExpr with get=false");
            }
            DslFunctionExpr::Map(DslMapExpr::Get)
        }
        P::Sketch(sketch) => {
            let percentile = required(sketch.percentile, "SketchExpr.percentile")?;
            DslFunctionExpr::Sketch(DslSketchExpr::Percentile {
                percentiles: HashableVecPercentiles(percentile.percentiles),
                force_list_output: percentile.force_list_output,
            })
        }
        P::Struct(struct_) => DslFunctionExpr::Struct(DslStructExpr::Get(struct_.get)),
        P::Python(udf) => DslFunctionExpr::Python(legacy_python_udf_from_proto(udf)?),
        P::Partitioning(partitioning) => {
            DslFunctionExpr::Partitioning(partitioning_expr_from_proto(partitioning)?)
        }
    };
    Ok((func, inputs))
}

pub(crate) fn function_arg_to_proto(arg: &FunctionArg<ExprRef>) -> DaftResult<proto::FunctionArg> {
    Ok(proto::FunctionArg {
        name: match arg {
            FunctionArg::Named { name, .. } => name.to_string(),
            FunctionArg::Unnamed(_) => String::new(),
        },
        arg: Some(expr_to_proto(arg.inner())?),
    })
}

pub(crate) fn function_arg_from_proto(arg: proto::FunctionArg) -> DaftResult<FunctionArg<ExprRef>> {
    let arg_expr = expr_from_proto(required(arg.arg, "FunctionArg.arg")?)?;
    if arg.name.is_empty() {
        Ok(FunctionArg::Unnamed(arg_expr))
    } else {
        Ok(FunctionArg::Named {
            name: arg.name.into(),
            arg: arg_expr,
        })
    }
}

pub(crate) fn scalar_fn_to_proto(func: &DslScalarFn) -> DaftResult<proto::ScalarFn> {
    use proto::scalar_fn::Func as P;
    let func = match func {
        DslScalarFn::Builtin(builtin) => P::Builtin(proto::BuiltinScalarFn {
            name: builtin.name().to_string(),
            args: builtin
                .inputs
                .iter()
                .map(function_arg_to_proto)
                .collect::<DaftResult<_>>()?,
        }),
        DslScalarFn::Python(py_fn) => P::Python(py_scalar_fn_to_proto(py_fn)?),
    };
    Ok(proto::ScalarFn { func: Some(func) })
}

pub(crate) fn scalar_fn_from_proto(func: proto::ScalarFn) -> DaftResult<DslScalarFn> {
    use proto::scalar_fn::Func as P;
    match required(func.func, "ScalarFn oneof")? {
        P::Builtin(builtin) => {
            let args = builtin
                .args
                .into_iter()
                .map(function_arg_from_proto)
                .collect::<DaftResult<Vec<_>>>()?;
            let factory = {
                let registry = FUNCTION_REGISTRY
                    .read()
                    .map_err(|_| DaftError::ValueError("function registry lock poisoned".into()))?;
                registry
                    .get(&builtin.name)
                    .ok_or_else(|| {
                        DaftError::ValueError(format!(
                            "unknown registered scalar function `{}`",
                            builtin.name
                        ))
                    })?
            };
            let func = factory
                .get_function(FunctionArgs::new_unchecked(args.clone()), &Schema::empty())
                .map_err(|e| {
                    DaftError::ValueError(format!(
                        "failed to rebuild scalar function `{}`: {e}",
                        builtin.name
                    ))
                })?;
            Ok(DslScalarFn::Builtin(BuiltinScalarFn {
                func,
                inputs: FunctionArgs::new_unchecked(args),
            }))
        }
        P::Python(py_fn) => Ok(DslScalarFn::Python(py_scalar_fn_from_proto(py_fn)?)),
    }
}

