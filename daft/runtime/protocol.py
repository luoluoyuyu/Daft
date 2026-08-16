from __future__ import annotations

import struct
from dataclasses import dataclass
from enum import Enum
from typing import BinaryIO

from daft.runtime.daft_proto.daft_runtime_proto.v1 import runtime_pb2

# HTTP content type used for every control-plane message. JSON is never used
# on the wire; all messages are protobuf.
PROTOBUF_CONTENT_TYPE = "application/x-protobuf"


class JobState(str, Enum):
    """Python-side job lifecycle state (kept as a stable string API).

    The wire protocol uses the integer enum ``daft.v1.JobState``; this enum is
    the idiomatic Python mirror and maps 1:1 onto it.
    """

    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELED = "canceled"


def job_state_to_proto(state: JobState) -> int:
    mapping = {
        JobState.PENDING: runtime_pb2.JOB_STATE_PENDING,
        JobState.RUNNING: runtime_pb2.JOB_STATE_RUNNING,
        JobState.SUCCEEDED: runtime_pb2.JOB_STATE_SUCCEEDED,
        JobState.FAILED: runtime_pb2.JOB_STATE_FAILED,
        JobState.CANCELED: runtime_pb2.JOB_STATE_CANCELED,
    }
    return mapping[state]


def job_state_from_proto(value: int) -> JobState:
    mapping = {
        runtime_pb2.JOB_STATE_PENDING: JobState.PENDING,
        runtime_pb2.JOB_STATE_RUNNING: JobState.RUNNING,
        runtime_pb2.JOB_STATE_SUCCEEDED: JobState.SUCCEEDED,
        runtime_pb2.JOB_STATE_FAILED: JobState.FAILED,
        runtime_pb2.JOB_STATE_CANCELED: JobState.CANCELED,
    }
    try:
        return mapping[value]
    except KeyError as e:
        raise ValueError(f"unknown job state {value!r}") from e


@dataclass(frozen=True)
class JobStatus:
    job_id: str
    state: JobState
    error: str | None = None

    @classmethod
    def from_proto(cls, status: runtime_pb2.JobStatus) -> JobStatus:
        return cls(
            job_id=status.job_id,
            state=job_state_from_proto(status.state),
            error=status.error or None,
        )

    def to_proto(self) -> runtime_pb2.JobStatus:
        return runtime_pb2.JobStatus(
            job_id=self.job_id,
            state=job_state_to_proto(self.state),
            error=self.error or "",
        )


def encode_length_prefixed(message) -> bytes:
    """Frame a protobuf message with a 4-byte little-endian length prefix.

    Mirrors ``daft_protocol::encode_length_prefixed`` used on the Rust side of
    the worker stdin/stdout channel.
    """
    payload = message.SerializeToString()
    return struct.pack("<I", len(payload)) + payload


def decode_length_prefixed(data: bytes):
    """Parse one length-prefixed protobuf message from ``data``.

    Returns ``(message_bytes, consumed)`` where ``consumed`` is the number of
    bytes belonging to the length prefix and payload. Raises ``ValueError`` on
    truncated or over-long frames.
    """
    if len(data) < 4:
        raise ValueError(f"truncated length prefix: expected 4 bytes, got {len(data)}")
    (length,) = struct.unpack_from("<I", data, 0)
    end = 4 + length
    if end > len(data):
        raise ValueError(
            f"truncated message: declared {length} bytes, got {len(data) - 4}"
        )
    return data[4:end], end


def read_length_prefixed(stream: BinaryIO) -> bytes:
    """Read one length-prefixed protobuf message from a binary stream.

    Blocks until a full frame is available. Raises ``EOFError`` when the
    stream closes before any bytes are read and ``ValueError`` on truncated
    frames. Mirrors ``daft_protocol::encode_length_prefixed`` used on the Rust
    side of the worker stdin/stdout channel.
    """
    prefix = stream.read(4)
    if not prefix:
        raise EOFError("stream closed")
    if len(prefix) != 4:
        raise ValueError(
            f"truncated length prefix: expected 4 bytes, got {len(prefix)}"
        )
    (length,) = struct.unpack("<I", prefix)
    payload = stream.read(length)
    if len(payload) != length:
        raise ValueError(
            f"truncated message: declared {length} bytes, got {len(payload)}"
        )
    return payload
