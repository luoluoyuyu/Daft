//! Pure-Rust execution of plans that contain no Python UDFs.
//!
//! The plan arrives as a protobuf payload (see `daft-protocol`), is decoded
//! into a `LogicalPlan`, optimized, lowered to the local physical plan, and
//! executed by the daft-local-execution engine. Results are encoded as the
//! same `DAFTRES1` IPC envelope used by the Python-side executor. Partition
//! sets arrive as raw Arrow IPC blobs from the protobuf wire protocol.

use std::sync::Arc;

use common_error::DaftResult;
use daft_micropartition::{MicroPartition, MicroPartitionRef};

pub const RESULT_MAGIC: &[u8; 8] = b"DAFTRES1";

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

/// Concatenate multiple `DAFTRES1` envelopes into a single envelope.
///
/// The scheduler uses this to merge per-task final results back into the one
/// envelope the Python client expects from a job. The per-envelope magic and
/// count headers are dropped; partition IPC streams are copied verbatim.
pub fn concat_result_envelopes(envelopes: &[&[u8]]) -> Result<Vec<u8>, String> {
    let mut total: u32 = 0;
    for envelope in envelopes {
        let mut offset = 0usize;
        if envelope.get(..RESULT_MAGIC.len()) != Some(RESULT_MAGIC.as_slice()) {
            return Err("invalid DAFTRES1 envelope: bad magic".to_string());
        }
        offset += RESULT_MAGIC.len();
        total += read_u32(envelope, &mut offset)?;
    }
    let mut combined = Vec::new();
    combined.extend_from_slice(RESULT_MAGIC);
    combined.extend_from_slice(&total.to_le_bytes());
    for envelope in envelopes {
        // Skip the per-envelope magic, then read the count, and keep the
        // length-prefixed per-partition IPC streams.
        let mut offset = RESULT_MAGIC.len();
        let count = read_u32(envelope, &mut offset)?;
        for _ in 0..count {
            let len = read_u64(envelope, &mut offset)?;
            let len = usize::try_from(len)
                .map_err(|_| "partition stream length overflow".to_string())?;
            let slice = envelope
                .get(offset..offset + len)
                .ok_or_else(|| "truncated DAFTRES1 envelope".to_string())?;
            combined.extend_from_slice(&(len as u64).to_le_bytes());
            combined.extend_from_slice(slice);
            offset += len;
        }
    }
    Ok(combined)
}

/// Decode a partition-set blob:
/// `u32 LE` count, then per partition `u64 LE` length + Arrow IPC stream bytes.
pub(crate) fn decode_partition_set(blob: &[u8]) -> Result<Vec<MicroPartitionRef>, String> {
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
