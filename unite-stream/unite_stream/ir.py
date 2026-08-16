"""Cloudpickle-able intermediate representation of compiled Daft plans.

Both Daft physical plan types ship with Rust-side bincode + serde state
serializers (``impl_bincode_py_state_serialization!``) and therefore satisfy
Python's pickle protocol via ``__getstate__`` / ``__reduce__``. UniteStream
wraps them in :class:`PhysicalPlanEnvelope` so the **native** and **Ray**
execution paths share one symmetric carrier:

::

    UniteStreamCompiler.compile_to_ir(script, runner=...)
        → bytes  (cloudpickled list[PhysicalPlanEnvelope])

    UniteStreamRuntime.execute_ir(bytes)
        → list[ExecutionResult]

The envelope itself is an ordinary ``@dataclass(frozen=True)`` so cloudpickle
serializes it without any custom machinery — the Rust plans inside are
opaque blobs that pickle/unpickle through their own state hooks.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any


class PlanKind(str, enum.Enum):
    """Which Daft physical plan species is wrapped in an envelope.

    The compiler emits ``LOCAL`` for the native runner path and
    ``DISTRIBUTED`` for the Ray runner path. The runtime dispatches on this
    field to pick the right executor (``NativeExecutor`` vs
    ``DistributedPhysicalPlanRunner``).
    """

    LOCAL = "local"
    DISTRIBUTED = "distributed"


@dataclass(frozen=True)
class PhysicalPlanEnvelope:
    """Cloudpickle-able container around a Daft physical plan.

    The envelope captures everything :class:`daft.daft.NativeExecutor` /
    :class:`daft.daft.DistributedPhysicalPlanRunner` needs to execute the
    plan independently — including the **in-memory source data** that
    ``daft.from_pydict`` / ``daft.from_arrow`` / etc. register into Daft's
    partition cache (without it the pipeline's source operators have no
    inputs and silently stall).

    Attributes:
        stream_index: 0-based index in ``OUTPUT_STREAMS``.
        kind: ``LOCAL`` (LocalPhysicalPlan) or ``DISTRIBUTED``
            (DistributedPhysicalPlan).
        plan: The underlying ``daft.daft.LocalPhysicalPlan`` or
            ``daft.daft.DistributedPhysicalPlan`` instance. Both implement
            ``__getstate__`` / ``__reduce__`` so the envelope is fully
            cloudpickle-able.
        inputs: ``source_id → input`` map produced by
            ``LocalPhysicalPlan.from_logical_plan_builder`` and consumed by
            ``NativeExecutor.run``. Empty / ignored for distributed plans.
        psets: Snapshot of Daft's process-global partition_set_cache
            (``partition_set_id → list[PyMicroPartition]``). Required by the
            native executor to feed source operators when the plan was built
            from in-memory data. Empty / ignored for distributed plans.
        output_path: Parquet write target bound by the compiler. ``None``
            when no write binding was applied (parse-only plans).
        job_namespace: Sub-directory name under the compiler's target dir.
            ``None`` when no namespace was assigned.
    """

    stream_index: int
    kind: PlanKind
    plan: Any
    inputs: dict[int, Any] = field(default_factory=dict)
    psets: dict[str, Any] = field(default_factory=dict)
    output_path: str | None = None
    job_namespace: str | None = None

    def is_local(self) -> bool:
        return self.kind is PlanKind.LOCAL

    def is_distributed(self) -> bool:
        return self.kind is PlanKind.DISTRIBUTED


@dataclass(frozen=True)
class ExecutionResult:
    """One row of the runtime's per-stream execution outcome.

    Attributes:
        stream_index: 0-based stream index (matches the envelope).
        kind: Which executor produced this result.
        output_path: Parquet write target the executor flushed to.
        stats: Best-effort runner stats (``PyExecutionStats`` /
            ``ExecutionMetadata`` / ``None`` if the runner does not expose
            one).
        num_partitions: Number of materialized partitions, ``None`` if
            unknown.
    """

    stream_index: int
    kind: PlanKind
    output_path: str | None
    stats: Any | None
    num_partitions: int | None = None
