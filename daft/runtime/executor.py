"""Python-side execution helper used by the standalone runtime.

The Rust ``daft-runtime`` binary never embeds CPython. Plans that contain
Python UDFs are executed by the :mod:`daft.runtime.worker` subprocess, which
delegates to :func:`execute_plan_bytes`. Results are encoded as an IPC
envelope::

    8 bytes  magic      b"DAFTRES1"
    4 bytes  u32 LE     number of partitions
    repeated per partition:
        8 bytes  u64 LE  IPC stream length
        N bytes          Arrow IPC stream (see MicroPartition.to_ipc_stream)

The envelope is parsed by :meth:`daft.runtime.client.RuntimeClient.result`.
"""

from __future__ import annotations

import struct
import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable

RESULT_MAGIC = b"DAFTRES1"


def execute_plan_bytes(
    plan_bytes: bytes,
    extra_paths: Iterable[str] = (),
    partition_sets: dict[str, bytes] | None = None,
) -> bytes:
    """Deserialize ``plan_bytes``, run it with the native runner, and return the IPC envelope.

    ``extra_paths`` are directories (e.g. materialized UDF artifacts) that are
    prepended to ``sys.path`` for the duration of the call. ``partition_sets``
    carries the in-memory inputs as raw Arrow IPC blobs (see
    :func:`daft.plan_transport.serialize_plan_parts`).
    """
    for path in extra_paths:
        if path and path not in sys.path:
            sys.path.insert(0, path)

    from daft.plan_transport import deserialize_plan_parts
    from daft.runners.native_runner import NativeRunner

    dataframe = deserialize_plan_parts(plan_bytes, partition_sets)
    partitions = list(NativeRunner().run_iter_tables(dataframe._get_current_builder()))
    return encode_partitions(partitions)


def encode_partitions(partitions: Iterable[object]) -> bytes:
    """Encode a list of MicroPartitions into the IPC result envelope."""
    envelope = bytearray(RESULT_MAGIC)
    streams = [partition.to_ipc_stream() for partition in partitions]
    envelope += struct.pack("<I", len(streams))
    for stream in streams:
        envelope += struct.pack("<Q", len(stream))
        envelope += stream
    return bytes(envelope)


def decode_partitions(envelope: bytes) -> list[object]:
    """Decode the IPC result envelope into a list of MicroPartitions."""
    from daft.recordbatch import MicroPartition

    if not envelope.startswith(RESULT_MAGIC):
        raise ValueError("Not a Daft runtime result envelope")
    offset = len(RESULT_MAGIC)
    (count,) = struct.unpack_from("<I", envelope, offset)
    offset += 4
    partitions = []
    for _ in range(count):
        (length,) = struct.unpack_from("<Q", envelope, offset)
        offset += 8
        partitions.append(MicroPartition.from_ipc_stream(envelope[offset : offset + length]))
        offset += length
    return partitions
