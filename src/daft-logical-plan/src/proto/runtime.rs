use super::*;
pub(crate) fn runtime_py_object_to_bytes(value: &RuntimePyObject) -> DaftResult<Vec<u8>> {
    bincode::serde::encode_to_vec(value, bincode::config::legacy()).map_err(|e| {
        DaftError::ValueError(format!("failed to bincode-encode RuntimePyObject: {e}"))
    })
}

pub(crate) fn runtime_py_object_from_bytes(bytes: &[u8]) -> DaftResult<RuntimePyObject> {
    bincode::serde::decode_from_slice(bytes, bincode::config::legacy())
        .map(|(value, _)| value)
        .map_err(|e| {
            DaftError::ValueError(format!("failed to bincode-decode RuntimePyObject: {e}"))
        })
}

pub(crate) fn optional_runtime_py_object_to_bytes(value: &Option<RuntimePyObject>) -> DaftResult<Vec<u8>> {
    match value {
        Some(obj) => runtime_py_object_to_bytes(obj),
        None => Ok(Vec::new()),
    }
}

pub(crate) fn optional_runtime_py_object_from_bytes(bytes: &[u8]) -> DaftResult<Option<RuntimePyObject>> {
    if bytes.is_empty() {
        Ok(None)
    } else {
        runtime_py_object_from_bytes(bytes).map(Some)
    }
}

pub(crate) fn resource_request_to_bytes(value: &Option<ResourceRequest>) -> DaftResult<Vec<u8>> {
    bincode::serde::encode_to_vec(value, bincode::config::legacy()).map_err(|e| {
        DaftError::ValueError(format!("failed to bincode-encode ResourceRequest: {e}"))
    })
}

pub(crate) fn resource_request_from_bytes(bytes: &[u8]) -> DaftResult<Option<ResourceRequest>> {
    if bytes.is_empty() {
        Ok(None)
    } else {
        bincode::serde::decode_from_slice(bytes, bincode::config::legacy())
            .map(|(value, _)| value)
            .map_err(|e| {
                DaftError::ValueError(format!("failed to bincode-decode ResourceRequest: {e}"))
            })
    }
}

