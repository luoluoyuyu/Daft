//! Wire protocol types for the Daft standalone runtime.
//!
//! The schema lives in ``proto/daft/v1/*.proto`` and is compiled to Rust by
//! ``build.rs`` (prost). The same schema is compiled to Python bindings
//! (``daft/runtime/daft_proto/``) so that the Python client, the Python
//! fallback server, and the Python UDF worker all speak byte-for-byte the same
//! protocol as the Rust server.
//!
//! # Design
//!
//! * The *wire envelope* (HTTP control plane, worker stdin/stdout) is
//!   protobuf. JSON is never used on the wire.
//! * The *logical plan payload* (``JobSubmitRequest.logical_plan`` /
//!   ``WorkerRequest.logical_plan``) is a protobuf message modeled by
//!   ``plan.proto`` and produced by ``LogicalPlanBuilder::to_bytes``. It
//!   already carries cloudpickled Python UDF bytes when the plan contains
//!   UDFs; the pure-Rust runtime treats those bytes as opaque and only the
//!   Python worker unpickles them.

#![allow(clippy::module_name_repetitions)]

/// Generated protobuf types for the ``daft.v1`` package.
pub mod daft {
    pub mod v1 {
        include!(concat!(env!("OUT_DIR"), "/daft.v1.rs"));
    }
}

/// Encode any prost message to its protobuf wire bytes.
pub fn encode<M: prost::Message>(message: &M) -> Vec<u8> {
    let mut buf = Vec::with_capacity(message.encoded_len());
    message
        .encode(&mut buf)
        .expect("protobuf encoding into a Vec cannot fail");
    buf
}

/// Decode a prost message from protobuf wire bytes.
pub fn decode<M: prost::Message + Default>(bytes: &[u8]) -> Result<M, prost::DecodeError> {
    M::decode(bytes)
}

/// Append a 4-byte little-endian length prefix and the message payload.
///
/// Used to frame single protobuf messages over pipes (worker stdin/stdout).
pub fn encode_length_prefixed<M: prost::Message>(message: &M) -> Vec<u8> {
    let payload = encode(message);
    let mut framed = Vec::with_capacity(4 + payload.len());
    framed.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    framed.extend_from_slice(&payload);
    framed
}

/// Read exactly one length-prefixed protobuf message from a byte slice,
/// returning the message and the number of bytes consumed.
pub fn decode_length_prefixed<M: prost::Message + Default>(
    data: &[u8],
) -> Result<(M, usize), String> {
    if data.len() < 4 {
        return Err(format!(
            "truncated length prefix: expected 4 bytes, got {}",
            data.len()
        ));
    }
    let len = u32::from_le_bytes(data[..4].try_into().expect("4-byte slice")) as usize;
    let end = 4usize
        .checked_add(len)
        .ok_or_else(|| "length prefix overflow".to_string())?;
    let payload = data
        .get(4..end)
        .ok_or_else(|| format!("truncated message: declared {len} bytes, got {}", data.len()))?;
    let message = M::decode(payload).map_err(|e| format!("protobuf decode failed: {e}"))?;
    Ok((message, end))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::daft::v1::{
        worker_request, worker_response, ExecutePlanRequest, ExecutePlanResponse, JobState,
        JobStatus, JobSubmitRequest, JobSubmitResponse, UdfDescriptor, UdfKind, WorkerRequest,
        WorkerResponse,
    };

    fn sample_submit_request() -> JobSubmitRequest {
        JobSubmitRequest {
            logical_plan: vec![0xde, 0xad, 0xbe, 0xef],
            partition_sets: [("in-memory-1".to_string(), vec![1, 2, 3, 4])]
                .into_iter()
                .collect(),
            udfs: vec![UdfDescriptor {
                udf_id: "func-1".to_string(),
                kind: UdfKind::ScalarRowWise as i32,
                name: "double".to_string(),
                return_dtype: vec![0x08, 0x03],
                num_inputs: 1,
                input_dtypes: vec![vec![0x08, 0x03]],
                code: vec![0x80, 0x02, 0x00],
                method: Vec::new(),
                init_args: Vec::new(),
                bound_args: Vec::new(),
                original_args: Vec::new(),
                batch_size: None,
                concurrency: None,
                use_process: None,
                max_retries: None,
                max_concurrency: None,
                builtin_name: false,
                is_async: false,
                is_scalar: true,
                on_error: 0,
                resource_request: Vec::new(),
                ray_options: Vec::new(),
                python_version: "3.11.15".to_string(),
                arg_names: vec!["x".to_string()],
                model: String::new(),
                engine_args: Vec::new(),
                generate_args: Vec::new(),
            }],
            udf_artifact_ids: vec!["sha256:abc".to_string()],
            python_version: "3.11.15".to_string(),
        }
    }

    #[test]
    fn job_submit_round_trips() {
        let request = sample_submit_request();
        let bytes = encode(&request);
        let decoded = decode::<JobSubmitRequest>(&bytes).unwrap();
        assert_eq!(request, decoded);
    }

    #[test]
    fn job_status_round_trips() {
        let status = JobStatus {
            job_id: "3f2504e0-4f89-41d3-9a0c-0305e82c3301".to_string(),
            state: JobState::Succeeded as i32,
            error: String::new(),
        };
        let decoded = decode::<JobStatus>(&encode(&status)).unwrap();
        assert_eq!(status, decoded);
    }

    #[test]
    fn length_prefixed_framing_round_trips() {
        let response = WorkerResponse {
            request_id: 7,
            result: Some(worker_response::Result::Execute(ExecutePlanResponse {
                result: vec![0xda, 0x66, 0x74],
                error: String::new(),
            })),
        };
        let framed = encode_length_prefixed(&response);
        let (decoded, consumed) = decode_length_prefixed::<WorkerResponse>(&framed).unwrap();
        assert_eq!(decoded, response);
        assert_eq!(consumed, framed.len());
    }

    #[test]
    fn length_prefixed_rejects_truncated_input() {
        let framed = encode_length_prefixed(&WorkerRequest {
            request_id: 1,
            command: Some(worker_request::Command::Execute(ExecutePlanRequest {
                logical_plan: vec![1, 2, 3],
                partition_sets: Default::default(),
                extra_paths: Vec::new(),
                udfs: Vec::new(),
                python_version: String::new(),
            })),
        });
        let truncated = &framed[..framed.len() - 2];
        assert!(decode_length_prefixed::<WorkerRequest>(truncated).is_err());
    }

    #[test]
    fn submit_response_round_trips() {
        let response = JobSubmitResponse {
            job_id: "job-42".to_string(),
        };
        let decoded = decode::<JobSubmitResponse>(&encode(&response)).unwrap();
        assert_eq!(response, decoded);
    }
}
