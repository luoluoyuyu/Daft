//! Pure-Rust execution of plans that contain no Python UDFs.
//!
//! The plan arrives as a protobuf payload (see `daft-protocol`), is decoded
//! into a `LogicalPlan`, optimized, lowered to the local physical plan, and
//! executed by the daft-local-execution engine. Results are encoded as the
//! same `DAFTRES1` IPC envelope used by the Python-side executor. Partition
//! sets arrive as raw Arrow IPC blobs from the protobuf wire protocol.

use std::{
    collections::HashMap,
    sync::Arc,
};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use daft_local_execution::{NativeExecutor, block_on_global};
use daft_local_plan::translate;
use daft_logical_plan::LogicalPlanBuilder;
use daft_micropartition::{MicroPartition, MicroPartitionRef};

pub const RESULT_MAGIC: &[u8; 8] = b"DAFTRES1";

/// Execute a serialized, UDF-free logical plan entirely in Rust.
///
/// `partition_sets` maps partition-set cache keys to raw partition blobs
/// (see [`decode_partition_set`] for the framing).
pub fn execute_plan_native(
    plan_bytes: Vec<u8>,
    partition_sets: HashMap<String, Vec<u8>>,
) -> Result<Vec<u8>, String> {
    let proto_plan = daft_protocol::decode::<daft_protocol::daft::v1::LogicalPlan>(&plan_bytes)
        .map_err(|e| format!("failed to decode logical plan: {e}"))?;
    let plan = daft_logical_plan::proto::plan_from_proto(proto_plan)
        .map_err(|e| format!("failed to decode logical plan: {e}"))?;

    let mut psets: HashMap<String, Vec<MicroPartitionRef>> =
        HashMap::with_capacity(partition_sets.len());
    for (key, blob) in partition_sets {
        let partitions = decode_partition_set(&blob)
            .map_err(|e| format!("failed to decode partition set {key}: {e}"))?;
        psets.insert(key, partitions);
    }

    let builder = LogicalPlanBuilder::new(plan, None);
    block_on_global(async move {
        let optimized = builder
            .optimize_async(Arc::new(DaftExecutionConfig::default()))
            .await
            .map_err(|e| format!("failed to optimize logical plan: {e}"))?;

        let (physical_plan, inputs) = translate(&optimized.plan, &psets)
            .map_err(|e| format!("failed to lower logical plan: {e}"))?;

        let exec_cfg = Arc::new(DaftExecutionConfig::default());
        let mut executor = NativeExecutor::new(false, "");
        let (fingerprint, enqueue_future) = executor
            .run(
                &physical_plan,
                exec_cfg,
                Vec::new(),
                None,
                inputs,
                0,
                true,
            )
            .map_err(|e| format!("failed to start execution: {e}"))?;

        let mut result = enqueue_future
            .await
            .map_err(|e| format!("execution failed: {e}"))?;
        let mut partitions: Vec<MicroPartition> = Vec::new();
        while let Some(partition) = result.next_partition().await {
            partitions.push(partition);
        }
        executor
            .try_finish(fingerprint, 0)
            .map_err(|e| format!("failed to start finish: {e}"))?
            .await
            .map_err(|e| format!("failed to finish execution: {e}"))?;

        encode_result_envelope(&partitions).map_err(|e| e.to_string())
    })
}

/// Encode a list of partitions as the `DAFTRES1` IPC envelope.
pub fn encode_result_envelope(partitions: &[MicroPartition]) -> DaftResult<Vec<u8>> {
    let mut envelope = Vec::new();
    envelope.extend_from_slice(RESULT_MAGIC);
    envelope.extend_from_slice(&(partitions.len() as u32).to_le_bytes());
    for partition in partitions {
        let stream = partition.write_to_ipc_stream()?;
        envelope.extend_from_slice(&(stream.len() as u64).to_le_bytes());
        envelope.extend_from_slice(&stream);
    }
    Ok(envelope)
}

/// Decode a partition-set blob:
/// `u32 LE` count, then per partition `u64 LE` length + Arrow IPC stream bytes.
fn decode_partition_set(blob: &[u8]) -> Result<Vec<MicroPartitionRef>, String> {
    let mut offset = 0usize;
    let count = read_u32(blob, &mut offset)?;
    let mut partitions = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let len = read_u64(blob, &mut offset)?;
        let len = usize::try_from(len).map_err(|_| "partition stream length overflow")?;
        let slice = blob
            .get(offset..offset + len)
            .ok_or("truncated partition set payload")?;
        let partition = MicroPartition::read_from_ipc_stream(slice)
            .map_err(|e| format!("failed to decode IPC partition: {e}"))?;
        partitions.push(Arc::new(partition));
        offset += len;
    }
    Ok(partitions)
}

fn read_u32(data: &[u8], offset: &mut usize) -> Result<u32, String> {
    let bytes = data
        .get(*offset..*offset + 4)
        .ok_or("truncated u32 in partition set payload")?;
    *offset += 4;
    Ok(u32::from_le_bytes(bytes.try_into().expect("slice is 4 bytes")))
}

fn read_u64(data: &[u8], offset: &mut usize) -> Result<u64, String> {
    let bytes = data
        .get(*offset..*offset + 8)
        .ok_or("truncated u64 in partition set payload")?;
    *offset += 8;
    Ok(u64::from_le_bytes(bytes.try_into().expect("slice is 8 bytes")))
}
