"""Transport helpers for shipping lazy Daft plans to the standalone runtime.

The Python side of the client/server split only builds the lazy plan and
sends it to the ``daft-runtime`` server. :func:`serialize_plan_parts` packs a
plan into three transport parts:

* ``plan_bytes`` -- the protobuf logical plan produced by
  ``LogicalPlanBuilder.to_bytes()``
  (``src/daft-protocol/proto/daft/v1/plan.proto``);
* ``execution`` -- a :class:`PlanExecution` value declaring how the runtime
  must execute the plan. It is computed on the Python side while the
  interpreter is available: UDF detection, parsing, and validation happen
  here (see :mod:`daft.runtime.udf_descriptor`), and the runtime simply
  honors the declaration instead of re-deriving it from the opaque plan
  bytes;
* ``partition_sets`` -- in-memory inputs encoded as raw Arrow IPC blobs.

:func:`deserialize_plan_parts` is used by the Python UDF worker to restore
the plan (and its in-memory inputs) so the interpreter can execute it.
Nothing in this module executes a plan; execution happens either in the Rust
runtime or in the Python worker subprocess it spawns.
"""

from __future__ import annotations

import struct
import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING

from daft.runners.runner import LOCAL_PARTITION_SET_CACHE

if TYPE_CHECKING:
    from daft.dataframe.dataframe import DataFrame
    from daft.runtime.daft_proto.daft_runtime_proto.v1 import udf_pb2


@dataclass(frozen=True)
class PlanExecution:
    """Complete, self-contained declaration of how a plan must be executed.

    Computed on the Python side (which owns the interpreter used to build the
    plan) and sent inside ``JobSubmitRequest.udfs`` so the runtime never has
    to guess an execution path from opaque plan bytes.
    """

    # Parsed Python UDF descriptors (daft.v1.UdfDescriptor). An empty tuple
    # means the plan contains no Python UDFs and is executed entirely by the
    # pure-Rust engine.
    udfs: tuple[udf_pb2.UdfDescriptor, ...] = ()
    # Interpreter version used to build the plan (e.g. "3.11.9"). The runtime
    # verifies it against the Python UDF worker's handshake and fails the job
    # on mismatch, because cloudpickled UDF closures are interpreter-specific.
    python_version: str = ""

    @classmethod
    def native(cls) -> "PlanExecution":
        """Execution entirely by the pure-Rust engine (no Python needed)."""
        return cls()

    @classmethod
    def udf(
        cls,
        udfs: list[udf_pb2.UdfDescriptor],
        *,
        python_version: str,
    ) -> "PlanExecution":
        """Execution by the Python UDF worker, with parsed descriptors."""
        return cls(udfs=tuple(udfs), python_version=python_version)

    @property
    def is_udf(self) -> bool:
        return bool(self.udfs)

    @property
    def requires_udf(self) -> bool:
        """Backwards-compatible bool view: any descriptor => UDF plan."""
        return bool(self.udfs)


def encode_partition_sets(
    psets: dict[str, object],
) -> dict[str, bytes]:
    """Encode in-memory partition sets as raw IPC blobs for transport.

    Each blob mirrors the framing used by the Rust runtime: ``u32 LE`` count,
    then per partition ``u64 LE`` length followed by the Arrow IPC stream.
    """
    encoded: dict[str, bytes] = {}
    for key, pset in psets.items():
        partitions = [result.micropartition() for result in pset.values()]
        blob = bytearray()
        blob += struct.pack("<I", len(partitions))
        for partition in partitions:
            stream = partition.to_ipc_stream()
            blob += struct.pack("<Q", len(stream))
            blob += stream
        encoded[key] = bytes(blob)
    return encoded


def decode_partition_sets(payload: dict[str, bytes]) -> list[object]:
    """Decode raw partition-set blobs into ``LOCAL_PARTITION_SET_CACHE``.

    Returns the list of partition cache entries so callers can keep the cache
    alive for the lifetime of the restored plan.
    """
    from daft.runners.partitioning import LocalMaterializedResult, LocalPartitionSet

    cache_entries = []
    for key, blob in payload.items():
        offset = 0
        (count,) = struct.unpack_from("<I", blob, offset)
        offset += 4
        partition_set = LocalPartitionSet()
        for idx in range(count):
            (length,) = struct.unpack_from("<Q", blob, offset)
            offset += 8
            from daft.recordbatch import MicroPartition

            partition = MicroPartition.from_ipc_stream(blob[offset : offset + length])
            partition_set.set_partition(idx, LocalMaterializedResult(partition))
            offset += length
        cache_entries.append(
            LOCAL_PARTITION_SET_CACHE.put_partition_set_with_key(key, partition_set)
        )
    return cache_entries


def serialize_plan_parts(df: DataFrame) -> tuple[bytes, PlanExecution, dict[str, bytes]]:
    """Serialize a plan for the standalone runtime as three transport parts.

    Returns ``(plan_bytes, execution, partition_sets)`` where ``plan_bytes``
    is the protobuf logical plan produced by ``LogicalPlanBuilder.to_bytes``,
    ``execution`` declares the required execution path (:class:`PlanExecution`;
    UDF detection and interpreter-version pinning are validated here, on the
    Python side, while the interpreter is available), and ``partition_sets``
    carries the in-memory inputs as raw Arrow IPC blobs (see
    :func:`encode_partition_sets`).
    """
    from daft.dataframe.dataframe import DataFrame

    if not isinstance(df, DataFrame):
        raise TypeError(f"serialize_plan_parts expects a DataFrame, got {type(df)!r}")
    builder = df._get_current_builder()
    plan_bytes = builder._builder.to_bytes()
    # UDF parsing happens here, on the Python client, while the interpreter
    # is available. The runtime receives the finished descriptors and never
    # parses the plan itself to discover UDFs.
    from daft.runtime.udf_descriptor import extract_udf_descriptors_from_bytes

    descriptors = extract_udf_descriptors_from_bytes(plan_bytes)
    if descriptors:
        execution = PlanExecution.udf(
            descriptors,
            python_version=sys.version.split()[0],
        )
    else:
        execution = PlanExecution.native()
    partition_sets = encode_partition_sets(
        LOCAL_PARTITION_SET_CACHE.get_all_partition_sets()
    )
    return plan_bytes, execution, partition_sets


def deserialize_plan_parts(
    plan_bytes: bytes,
    partition_sets: dict[str, bytes] | None = None,
) -> DataFrame:
    """Restore a :class:`DataFrame` from :func:`serialize_plan_parts` bytes."""
    from daft.dataframe.dataframe import DataFrame
    from daft.daft import LogicalPlanBuilder as NativeLogicalPlanBuilder
    from daft.logical.builder import LogicalPlanBuilder

    cache_entries = decode_partition_sets(partition_sets or {})
    builder = LogicalPlanBuilder(NativeLogicalPlanBuilder.from_bytes(plan_bytes))
    dataframe = DataFrame(builder)
    dataframe._transport_cache_entries = cache_entries
    return dataframe
