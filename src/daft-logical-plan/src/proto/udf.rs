use super::*;
pub(crate) fn legacy_python_udf_to_proto(udf: &LegacyPythonUDF) -> DaftResult<proto::LegacyPythonUdf> {
    use proto::maybe_initialized_udf::State;
    let state = match &udf.func {
        MaybeInitializedUDF::Initialized(obj) => {
            State::Initialized(runtime_py_object_to_bytes(obj)?)
        }
        MaybeInitializedUDF::Uninitialized { inner, init_args } => State::Uninitialized(
            proto::UninitializedUdf {
                inner: runtime_py_object_to_bytes(inner)?,
                init_args: runtime_py_object_to_bytes(init_args)?,
            },
        ),
    };
    Ok(proto::LegacyPythonUdf {
        name: udf.name.to_string(),
        func: Some(proto::MaybeInitializedUdf { state: Some(state) }),
        bound_args: runtime_py_object_to_bytes(&udf.bound_args)?,
        num_expressions: udf.num_expressions as u64,
        return_dtype: Some(udf.return_dtype.to_proto()?),
        resource_request: resource_request_to_bytes(&udf.resource_request)?,
        batch_size: udf.batch_size.map(|b| b as u64),
        concurrency: udf.concurrency.map(|c| c.get() as u64),
        use_process: udf.use_process,
        ray_options: optional_runtime_py_object_to_bytes(&udf.ray_options)?,
    })
}

pub(crate) fn legacy_python_udf_from_proto(udf: proto::LegacyPythonUdf) -> DaftResult<LegacyPythonUDF> {
    use proto::maybe_initialized_udf::State;
    let func = match required(udf.func, "LegacyPythonUdf.func")?.state {
        Some(State::Initialized(obj)) => {
            MaybeInitializedUDF::Initialized(runtime_py_object_from_bytes(&obj)?)
        }
        Some(State::Uninitialized(uninitialized)) => MaybeInitializedUDF::Uninitialized {
            inner: runtime_py_object_from_bytes(&uninitialized.inner)?,
            init_args: runtime_py_object_from_bytes(&uninitialized.init_args)?,
        },
        None => return invalid("MaybeInitializedUdf state missing"),
    };
    Ok(LegacyPythonUDF {
        name: Arc::new(udf.name),
        func,
        bound_args: runtime_py_object_from_bytes(&udf.bound_args)?,
        num_expressions: udf.num_expressions as usize,
        return_dtype: required_dtype(udf.return_dtype, "LegacyPythonUdf.return_dtype")?,
        resource_request: resource_request_from_bytes(&udf.resource_request)?,
        batch_size: udf.batch_size.map(|b| b as usize),
        concurrency: udf
            .concurrency
            .map(|c| std::num::NonZeroUsize::new(c as usize).unwrap_or_else(|| {
                std::num::NonZeroUsize::new(1).expect("1 is non-zero")
            })),
        use_process: udf.use_process,
        ray_options: optional_runtime_py_object_from_bytes(&udf.ray_options)?,
    })
}

pub(crate) fn row_wise_py_fn_to_proto(func: &RowWisePyFn) -> DaftResult<proto::RowWisePyFn> {
    Ok(proto::RowWisePyFn {
        func_id: func.func_id.to_string(),
        function_name: func.function_name.to_string(),
        cls: runtime_py_object_to_bytes(&func.cls)?,
        method: runtime_py_object_to_bytes(&func.method)?,
        builtin_name: func.builtin_name,
        is_async: func.is_async,
        return_dtype: Some(func.return_dtype.to_proto()?),
        original_args: runtime_py_object_to_bytes(&func.original_args)?,
        args: func
            .args
            .iter()
            .map(expr_to_proto)
            .collect::<DaftResult<_>>()?,
        cpus: func.cpus.as_ref().map(|c| c.0),
        gpus: func.gpus.0,
        use_process: func.use_process,
        max_concurrency: func.max_concurrency.map(|c| c.get() as u64),
        max_retries: func.max_retries.map(|r| r as u64),
        on_error: on_error_to_proto(func.on_error) as i32,
        ray_options: optional_runtime_py_object_to_bytes(&func.ray_options)?,
    })
}

pub(crate) fn row_wise_py_fn_from_proto(func: proto::RowWisePyFn) -> DaftResult<RowWisePyFn> {
    Ok(RowWisePyFn {
        func_id: func.func_id.into(),
        function_name: func.function_name.into(),
        cls: runtime_py_object_from_bytes(&func.cls)?,
        method: runtime_py_object_from_bytes(&func.method)?,
        builtin_name: func.builtin_name,
        is_async: func.is_async,
        return_dtype: required_dtype(func.return_dtype, "RowWisePyFn.return_dtype")?,
        original_args: runtime_py_object_from_bytes(&func.original_args)?,
        args: func
            .args
            .into_iter()
            .map(expr_from_proto)
            .collect::<DaftResult<_>>()?,
        cpus: func.cpus.map(FloatWrapper),
        gpus: FloatWrapper(func.gpus),
        use_process: func.use_process,
        max_concurrency: func
            .max_concurrency
            .map(|c| std::num::NonZeroUsize::new(c as usize).unwrap_or_else(|| {
                std::num::NonZeroUsize::new(1).expect("1 is non-zero")
            })),
        max_retries: func.max_retries.map(|r| r as usize),
        on_error: on_error_from_proto(
            proto::OnError::try_from(func.on_error).unwrap_or(proto::OnError::Unspecified),
        )?,
        ray_options: optional_runtime_py_object_from_bytes(&func.ray_options)?,
    })
}

pub(crate) fn batch_py_fn_to_proto(func: &BatchPyFn) -> DaftResult<proto::BatchPyFn> {
    Ok(proto::BatchPyFn {
        func_id: func.func_id.to_string(),
        function_name: func.function_name.to_string(),
        cls: runtime_py_object_to_bytes(&func.cls)?,
        method: runtime_py_object_to_bytes(&func.method)?,
        builtin_name: func.builtin_name,
        is_async: func.is_async,
        return_dtype: Some(func.return_dtype.to_proto()?),
        cpus: func.cpus.as_ref().map(|c| c.0),
        gpus: func.gpus.0,
        use_process: func.use_process,
        max_concurrency: func.max_concurrency.map(|c| c.get() as u64),
        batch_size: func.batch_size.map(|b| b as u64),
        original_args: runtime_py_object_to_bytes(&func.original_args)?,
        args: func
            .args
            .iter()
            .map(expr_to_proto)
            .collect::<DaftResult<_>>()?,
        max_retries: func.max_retries.map(|r| r as u64),
        on_error: on_error_to_proto(func.on_error) as i32,
        ray_options: optional_runtime_py_object_to_bytes(&func.ray_options)?,
    })
}

pub(crate) fn batch_py_fn_from_proto(func: proto::BatchPyFn) -> DaftResult<BatchPyFn> {
    Ok(BatchPyFn {
        func_id: func.func_id.into(),
        function_name: func.function_name.into(),
        cls: runtime_py_object_from_bytes(&func.cls)?,
        method: runtime_py_object_from_bytes(&func.method)?,
        builtin_name: func.builtin_name,
        is_async: func.is_async,
        return_dtype: required_dtype(func.return_dtype, "BatchPyFn.return_dtype")?,
        cpus: func.cpus.map(FloatWrapper),
        gpus: FloatWrapper(func.gpus),
        use_process: func.use_process,
        max_concurrency: func
            .max_concurrency
            .map(|c| std::num::NonZeroUsize::new(c as usize).unwrap_or_else(|| {
                std::num::NonZeroUsize::new(1).expect("1 is non-zero")
            })),
        batch_size: func.batch_size.map(|b| b as usize),
        original_args: runtime_py_object_from_bytes(&func.original_args)?,
        args: func
            .args
            .into_iter()
            .map(expr_from_proto)
            .collect::<DaftResult<_>>()?,
        max_retries: func.max_retries.map(|r| r as usize),
        on_error: on_error_from_proto(
            proto::OnError::try_from(func.on_error).unwrap_or(proto::OnError::Unspecified),
        )?,
        ray_options: optional_runtime_py_object_from_bytes(&func.ray_options)?,
    })
}

pub(crate) fn py_scalar_fn_to_proto(func: &DslPyScalarFn) -> DaftResult<proto::PyScalarFn> {
    use proto::py_scalar_fn::Func as P;
    let func = match func {
        DslPyScalarFn::RowWise(row_wise) => P::RowWise(row_wise_py_fn_to_proto(row_wise)?),
        DslPyScalarFn::Batch(batch) => P::Batch(batch_py_fn_to_proto(batch)?),
    };
    Ok(proto::PyScalarFn { func: Some(func) })
}

pub(crate) fn py_scalar_fn_from_proto(func: proto::PyScalarFn) -> DaftResult<DslPyScalarFn> {
    use proto::py_scalar_fn::Func as P;
    match required(func.func, "PyScalarFn oneof")? {
        P::RowWise(row_wise) => Ok(DslPyScalarFn::RowWise(row_wise_py_fn_from_proto(row_wise)?)),
        P::Batch(batch) => Ok(DslPyScalarFn::Batch(batch_py_fn_from_proto(batch)?)),
    }
}

pub(crate) fn vllm_expr_to_proto(vllm: &VLLMExpr) -> DaftResult<proto::VllmExpr> {
    Ok(proto::VllmExpr {
        model: vllm.model.clone(),
        input: Some(Box::new(expr_to_proto(&vllm.input)?)),
        concurrency: vllm.concurrency as u64,
        gpus_per_actor: vllm.gpus_per_actor as u64,
        do_prefix_routing: vllm.do_prefix_routing,
        max_buffer_size: vllm.max_buffer_size as u64,
        min_bucket_size: vllm.min_bucket_size as u64,
        prefix_match_threshold: vllm.prefix_match_threshold.0,
        load_balance_threshold: vllm.load_balance_threshold as u64,
        batch_size: vllm.batch_size.map(|b| b as u64),
        engine_args: runtime_py_object_to_bytes(&vllm.engine_args)?,
        generate_args: runtime_py_object_to_bytes(&vllm.generate_args)?,
    })
}

pub(crate) fn vllm_expr_from_proto(vllm: proto::VllmExpr) -> DaftResult<VLLMExpr> {
    Ok(VLLMExpr {
        model: vllm.model,
        input: required_expr(vllm.input, "VllmExpr.input")?,
        concurrency: vllm.concurrency as usize,
        gpus_per_actor: vllm.gpus_per_actor as usize,
        do_prefix_routing: vllm.do_prefix_routing,
        max_buffer_size: vllm.max_buffer_size as usize,
        min_bucket_size: vllm.min_bucket_size as usize,
        prefix_match_threshold: FloatWrapper(vllm.prefix_match_threshold),
        load_balance_threshold: vllm.load_balance_threshold as usize,
        batch_size: vllm.batch_size.map(|b| b as usize),
        engine_args: runtime_py_object_from_bytes(&vllm.engine_args)?,
        generate_args: runtime_py_object_from_bytes(&vllm.generate_args)?,
    })
}

pub(crate) fn udf_properties_to_proto(properties: &UDFProperties) -> DaftResult<proto::UdfProperties> {
    Ok(proto::UdfProperties {
        name: properties.name.clone(),
        resource_request: resource_request_to_bytes(&properties.resource_request)?,
        batch_size: properties.batch_size.map(|batch_size| batch_size as u64),
        concurrency: properties.concurrency.map(|concurrency| concurrency.get() as u64),
        use_process: properties.use_process,
        max_retries: properties.max_retries.map(|max_retries| max_retries as u64),
        builtin_name: properties.builtin_name,
        is_async: properties.is_async,
        is_scalar: properties.is_scalar,
        on_error: properties
            .on_error
            .map(on_error_to_proto)
            .unwrap_or(proto::OnError::Unspecified) as i32,
        ray_options: optional_runtime_py_object_to_bytes(&properties.ray_options)?,
    })
}

pub(crate) fn udf_properties_from_proto(properties: proto::UdfProperties) -> DaftResult<UDFProperties> {
    Ok(UDFProperties {
        name: properties.name,
        resource_request: resource_request_from_bytes(&properties.resource_request)?,
        batch_size: properties.batch_size.map(|batch_size| batch_size as usize),
        concurrency: properties
            .concurrency
            .map(|concurrency| {
                NonZeroUsize::new(concurrency as usize).ok_or_else(|| {
                    DaftError::ValueError("UdfProperties.concurrency must be non-zero".into())
                })
            })
            .transpose()?,
        use_process: properties.use_process,
        max_retries: properties.max_retries.map(|max_retries| max_retries as usize),
        builtin_name: properties.builtin_name,
        is_async: properties.is_async,
        is_scalar: properties.is_scalar,
        on_error: on_error_from_proto(
            proto::OnError::try_from(properties.on_error).unwrap_or(proto::OnError::Unspecified),
        )?
        .into(),
        ray_options: optional_runtime_py_object_from_bytes(&properties.ray_options)?,
    })
}

