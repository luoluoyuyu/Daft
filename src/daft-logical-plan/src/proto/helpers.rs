use super::*;
pub(crate) fn unsupported<T>(what: impl std::fmt::Display) -> DaftResult<T> {
    Err(DaftError::ValueError(format!(
        "plan protobuf conversion does not support {what}"
    )))
}

pub(crate) fn invalid<T>(what: impl std::fmt::Display) -> DaftResult<T> {
    Err(DaftError::ValueError(format!(
        "invalid plan protobuf payload: {what}"
    )))
}

// ---------------------------------------------------------------------------
// TimeUnit / ImageMode / MediaType
// ---------------------------------------------------------------------------
pub(crate) fn split_i128(value: i128) -> (i64, i64) {
    let low = value as u64 as i64;
    let high = (value >> 64) as i64;
    (low, high)
}

pub(crate) fn join_i128(low: i64, high: i64) -> i128 {
    ((high as i128) << 64) | (low as u64 as i128)
}

// ---------------------------------------------------------------------------
// Expression
// ---------------------------------------------------------------------------

pub(crate) fn required<T>(value: Option<T>, what: &str) -> DaftResult<T> {
    value.ok_or_else(|| DaftError::ValueError(format!("{what} missing")))
}

pub(crate) fn required_expr(expr: Option<Box<proto::Expression>>, what: &str) -> DaftResult<ExprRef> {
    expr.map(|e| expr_from_proto(*e))
        .transpose()?
        .ok_or_else(|| DaftError::ValueError(format!("{what} missing")))
}

pub(crate) fn required_expr_direct(expr: Option<proto::Expression>, what: &str) -> DaftResult<ExprRef> {
    required(expr, what).and_then(expr_from_proto)
}

pub(crate) fn required_dtype(dtype: Option<proto::DataType>, what: &str) -> DaftResult<DataType> {
    required(dtype, what)?.into_daft()
}
pub(crate) fn node_ids_to_proto(plan_id: &Option<usize>, node_id: &Option<usize>) -> (Option<u64>, Option<u64>) {
    (plan_id.map(|id| id as u64), node_id.map(|id| id as u64))
}

pub(crate) fn node_ids_from_proto(plan_id: Option<u64>, node_id: Option<u64>) -> (Option<usize>, Option<usize>) {
    (plan_id.map(|id| id as usize), node_id.map(|id| id as usize))
}
