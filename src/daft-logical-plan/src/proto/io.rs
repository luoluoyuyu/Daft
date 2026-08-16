use super::*;
pub(crate) fn pushdowns_to_proto(pushdowns: &Pushdowns) -> DaftResult<proto::Pushdowns> {
    Ok(proto::Pushdowns {
        filters: pushdowns
            .filters
            .as_ref()
            .map(expr_to_proto)
            .transpose()?,
        partition_filters: pushdowns
            .partition_filters
            .as_ref()
            .map(expr_to_proto)
            .transpose()?,
        columns: pushdowns.columns.as_ref().map(|columns| proto::StringList {
            values: columns.as_ref().clone(),
        }),
        limit: pushdowns.limit.map(|limit| limit as u64),
        sharder: pushdowns.sharder.as_ref().map(sharder_to_proto),
        pushed_filters: pushdowns
            .pushed_filters
            .as_ref()
            .map(|filters| -> DaftResult<proto::ExpressionList> {
                Ok(proto::ExpressionList {
                    items: filters
                        .iter()
                        .map(expr_to_proto)
                        .collect::<DaftResult<_>>()?,
                })
            })
            .transpose()?,
        aggregation: pushdowns
            .aggregation
            .as_ref()
            .map(expr_to_proto)
            .transpose()?,
    })
}

pub(crate) fn pushdowns_from_proto(pushdowns: proto::Pushdowns) -> DaftResult<Pushdowns> {
    Ok(Pushdowns {
        filters: pushdowns
            .filters
            .map(expr_from_proto)
            .transpose()?,
        partition_filters: pushdowns
            .partition_filters
            .map(expr_from_proto)
            .transpose()?,
        columns: pushdowns.columns.map(|columns| Arc::new(columns.values)),
        limit: pushdowns.limit.map(|limit| limit as usize),
        sharder: pushdowns.sharder.map(sharder_from_proto).transpose()?,
        pushed_filters: pushdowns
            .pushed_filters
            .map(|filters| {
                filters
                    .items
                    .into_iter()
                    .map(expr_from_proto)
                    .collect::<DaftResult<_>>()
            })
            .transpose()?,
        aggregation: pushdowns
            .aggregation
            .map(expr_from_proto)
            .transpose()?,
    })
}

pub(crate) fn io_config_to_bytes(io_config: &Option<Box<IOConfig>>) -> Vec<u8> {
    match io_config {
        None => Vec::new(),
        Some(config) => bincode::serde::encode_to_vec(config.as_ref(), bincode::config::legacy())
            .unwrap_or_else(|e| {
                log::warn!("failed to bincode-encode IOConfig: {e}; sending empty payload");
                Vec::new()
            }),
    }
}

pub(crate) fn io_config_from_bytes(bytes: &[u8]) -> DaftResult<Option<Box<IOConfig>>> {
    if bytes.is_empty() {
        return Ok(None);
    }
    let config = bincode::serde::decode_from_slice(bytes, bincode::config::legacy())
        .map_err(|e| DaftError::ValueError(format!("failed to bincode-decode IOConfig: {e}")))?
        .0;
    Ok(Some(Box::new(config)))
}

// ---------------------------------------------------------------------------
// Physical scans / scan tasks
// ---------------------------------------------------------------------------

pub(crate) fn opt_bincode_to_bytes<T: serde::Serialize>(value: &Option<T>, what: &str) -> Vec<u8> {
    match value {
        None => Vec::new(),
        Some(value) => {
            bincode::serde::encode_to_vec(value, bincode::config::legacy()).unwrap_or_else(|e| {
                log::warn!("failed to bincode-encode {what}: {e}; sending empty payload");
                Vec::new()
            })
        }
    }
}

pub(crate) fn opt_bincode_from_bytes<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
    what: &str,
) -> DaftResult<Option<T>> {
    if bytes.is_empty() {
        return Ok(None);
    }
    let value = bincode::serde::decode_from_slice(bytes, bincode::config::legacy())
        .map_err(|e| DaftError::ValueError(format!("failed to bincode-decode {what}: {e}")))?
        .0;
    Ok(Some(value))
}

