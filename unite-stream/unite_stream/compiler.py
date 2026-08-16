"""Compiler stage of UniteStream.

The compiler sits on top of :class:`unite_stream.UniteStreamParser` and offers
**two symmetric** outputs — the only difference between the local and Ray
paths is the physical plan species inside the envelope:

1. :meth:`UniteStreamCompiler.compile_to_plans` — *in-process debug* path.
   Returns a :class:`CompiledPlans` bundle of lazy ``daft.DataFrame`` plans
   (still bound to ``write_parquet``). Useful when you want to inspect the
   plan locally or hand it straight to ``.collect()``; no serialization
   happens.

2. :meth:`UniteStreamCompiler.compile_to_ir` — *transportable IR* for both
   runners. The compiler first lowers each plan to either a
   ``LocalPhysicalPlan`` (native runner) or a ``DistributedPhysicalPlan``
   (Ray runner), wraps it in a :class:`PhysicalPlanEnvelope`, and
   cloudpickles the list. The runtime accepts the resulting ``bytes`` and
   dispatches to the matching executor.

   Both plan species ship with Rust-side bincode + serde state hooks
   (``impl_bincode_py_state_serialization!``), so they round-trip through
   ``cloudpickle`` cleanly.

The legacy :meth:`compile_and_extract_ir` alias is retained as a thin shim
on top of ``compile_to_ir(runner=RunnerType.RAY)``.

Thread safety
-------------
The compiler holds a single (thread-safe) parser and is otherwise immutable.
A single instance can be shared across threads; each call writes to its own
``job_namespace`` sub-directory, so concurrent compiles never collide on
disk.
"""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from typing import Any, Final

from daft.dataframe.dataframe import DataFrame

from unite_stream.errors import CompilationError
from unite_stream.ir import PhysicalPlanEnvelope, PlanKind
from unite_stream.parser import UniteStreamParser
from unite_stream.plan_bundle import CompiledPlans


# --------------------------------------------------------------------------- #
# Lowering helpers                                                            #
# --------------------------------------------------------------------------- #


def _execution_config() -> Any:
    """Return Daft's active ``PyDaftExecutionConfig`` (raises on failure)."""
    try:
        from daft.context import get_context
    except ImportError as exc:  # pragma: no cover — declared dependency
        raise CompilationError(
            "[编译失败] 无法导入 daft.context"
        ) from exc

    try:
        return get_context().daft_execution_config
    except Exception as exc:  # noqa: BLE001
        raise CompilationError(
            f"[编译失败] 无法获取 Daft execution config: {exc}"
        ) from exc


def _snapshot_partition_set_cache() -> dict[str, list[Any]]:
    """Snapshot the process-global partition cache into a cloudpickle-friendly dict.

    Daft's ``from_pydict`` / ``from_arrow`` / etc. register in-memory data
    into the process-global ``LOCAL_PARTITION_SET_CACHE``. The native
    executor's source operators look these up by id at run time, so we
    must capture them in the IR (the bare ``LocalPhysicalPlan`` only
    carries id references, not the data itself).
    """
    try:
        from daft.runners.runner import LOCAL_PARTITION_SET_CACHE
    except ImportError as exc:  # pragma: no cover — declared dependency
        raise CompilationError(
            "[编译失败] 无法导入 daft.runners.runner.LOCAL_PARTITION_SET_CACHE"
        ) from exc

    return {
        partition_id: [
            materialized.micropartition()._micropartition
            for materialized in pset.values()
        ]
        for partition_id, pset in LOCAL_PARTITION_SET_CACHE.get_all_partition_sets().items()
    }


def _lower_to_local_plan(
    plan: DataFrame,
    *,
    stream_index: int,
    execution_config: Any,
    psets: dict[str, Any],
) -> tuple[Any, dict[int, Any]]:
    """Compile a ``DataFrame`` to ``LocalPhysicalPlan`` + its inputs map.

    The builder is *optimized* first (mirroring what ``NativeRunner`` does),
    then lowered via ``LocalPhysicalPlan.from_logical_plan_builder(builder,
    psets)``. The returned ``inputs`` dict maps ``source_id`` to in-memory
    input handles that the native executor needs alongside ``psets``.
    """
    try:
        from daft.daft import LocalPhysicalPlan
    except ImportError as exc:
        raise CompilationError(
            "[编译失败] 当前 Daft 未暴露 LocalPhysicalPlan，"
            "native IR 路径不可用。"
        ) from exc

    builder_wrapper = plan._builder
    try:
        optimized = builder_wrapper.optimize(execution_config)
        inner_builder = optimized._builder
    except Exception as exc:  # noqa: BLE001
        raise CompilationError(
            f"[编译失败] plans[{stream_index}] 优化 logical plan 失败: "
            f"{type(exc).__name__}: {exc}"
        ) from exc

    try:
        local_plan, inputs = LocalPhysicalPlan.from_logical_plan_builder(
            inner_builder, psets
        )
    except Exception as exc:  # noqa: BLE001
        raise CompilationError(
            f"[编译失败] plans[{stream_index}] 转换为 LocalPhysicalPlan 失败: "
            f"{type(exc).__name__}: {exc}"
        ) from exc
    return local_plan, dict(inputs)


def _lower_to_distributed_plan(
    plan: DataFrame, *, stream_index: int, execution_config: Any
) -> Any:
    """Compile a ``DataFrame`` to ``DistributedPhysicalPlan``."""
    try:
        from daft.daft import DistributedPhysicalPlan
    except ImportError as exc:
        raise CompilationError(
            "[编译失败] 当前 Daft 未暴露 DistributedPhysicalPlan，"
            "Ray IR 路径不可用。"
        ) from exc

    try:
        inner_builder = plan._builder._builder
    except AttributeError as exc:
        raise CompilationError(
            f"[编译失败] plans[{stream_index}] 无法访问 ``_builder._builder``: {exc}"
        ) from exc

    query_id = f"unite-stream-{uuid.uuid4().hex}"
    try:
        return DistributedPhysicalPlan.from_logical_plan_builder(
            inner_builder, query_id, execution_config
        )
    except Exception as exc:  # noqa: BLE001
        raise CompilationError(
            f"[编译失败] plans[{stream_index}] 转换为 DistributedPhysicalPlan 失败: "
            f"{type(exc).__name__}: {exc}"
        ) from exc


def _serialize_envelopes(envelopes: list[PhysicalPlanEnvelope]) -> bytes:
    """Cloudpickle a list of envelopes; both plan kinds are pickle-friendly."""
    try:
        import cloudpickle
    except ImportError as exc:  # pragma: no cover — declared dependency
        raise CompilationError(
            "[编译失败] cloudpickle 不可用，无法打包 IR。"
        ) from exc

    try:
        return cloudpickle.dumps(envelopes)
    except Exception as exc:  # noqa: BLE001
        raise CompilationError(
            f"[编译失败] IR cloudpickle 失败: {type(exc).__name__}: {exc}"
        ) from exc


def _resolve_plan_kind(runner: Any) -> PlanKind:
    """Translate a :class:`RunnerType` / ``str`` / ``None`` into a :class:`PlanKind`.

    ``None`` means "use the runner currently configured in Daft" — we probe
    ``daft.get_or_infer_runner_type`` and map ``ray`` → ``DISTRIBUTED`` and
    everything else → ``LOCAL``.
    """
    if runner is None:
        try:
            import daft

            current = daft.get_or_infer_runner_type()
        except Exception:  # noqa: BLE001 — best-effort probe
            current = None
        if current and str(current).lower() == "ray":
            return PlanKind.DISTRIBUTED
        return PlanKind.LOCAL

    value = runner.value if hasattr(runner, "value") else str(runner)
    value = value.lower()
    if value in ("ray", "distributed"):
        return PlanKind.DISTRIBUTED
    if value in ("native", "local"):
        return PlanKind.LOCAL
    raise CompilationError(
        f"[编译失败] 未知 runner 名称: {runner!r}; "
        f"支持的值: native | local | ray | distributed"
    )


# --------------------------------------------------------------------------- #
# Public compiler                                                             #
# --------------------------------------------------------------------------- #


class UniteStreamCompiler:
    """Parse + bind write paths, optionally lowering to a cross-process IR.

    Args:
        system_target_dir: Root directory under which every job writes its
            ``stream_job_<i>.parquet`` outputs. Each call lands in its own
            ``job_namespace`` sub-directory.
        extra_globals: Optional read-only mapping of additional static names
            injected into every user-script run.
        parser: Optional pre-built :class:`UniteStreamParser`. When omitted,
            a fresh parser is constructed from ``extra_globals``.
    """

    __slots__ = ("_target_dir", "_parser")

    def __init__(
        self,
        system_target_dir: str,
        *,
        extra_globals: Mapping[str, Any] | None = None,
        parser: UniteStreamParser | None = None,
    ) -> None:
        self._target_dir: Final[str] = system_target_dir.rstrip("/")
        self._parser: Final[UniteStreamParser] = parser or UniteStreamParser(
            extra_globals=extra_globals
        )

    @property
    def target_dir(self) -> str:
        return self._target_dir

    @property
    def parser(self) -> UniteStreamParser:
        return self._parser

    # ------------------------------------------------------------------ #
    # Parsing                                                            #
    # ------------------------------------------------------------------ #

    def parse(self, user_python_code: str) -> list[DataFrame]:
        """Shortcut for :meth:`UniteStreamParser.parse` (no write binding)."""
        return self._parser.parse(user_python_code)

    # ------------------------------------------------------------------ #
    # In-process binding (debug / direct-collect path)                   #
    # ------------------------------------------------------------------ #

    def compile_to_plans(
        self,
        user_python_code: str,
        *,
        job_namespace: str | None = None,
    ) -> CompiledPlans:
        """Parse + bind ``write_parquet`` paths; return a :class:`CompiledPlans`.

        Use this when you want to keep the *lazy* ``daft.DataFrame`` plans
        on the same process — for example to inspect them or call
        ``.collect()`` directly. For cross-process transport prefer
        :meth:`compile_to_ir`.
        """
        plans = self._parser.parse(user_python_code)
        namespace = job_namespace or f"job_{uuid.uuid4().hex}"
        bound = [
            plan.write_parquet(
                f"{self._target_dir}/{namespace}/stream_job_{index}.parquet"
            )
            for index, plan in enumerate(plans)
        ]
        return CompiledPlans(
            bound,
            job_namespace=namespace,
            target_dir=self._target_dir,
        )

    # ------------------------------------------------------------------ #
    # Transportable IR (native + Ray, symmetric)                         #
    # ------------------------------------------------------------------ #

    def compile_to_ir(
        self,
        user_python_code: str,
        *,
        runner: Any = None,
        job_namespace: str | None = None,
    ) -> bytes:
        """Parse + bind + lower to physical plans + cloudpickle.

        Both runners share the *same* IR shape — a cloudpickled
        ``list[PhysicalPlanEnvelope]``. The compiler picks the plan species
        per ``runner``:

        * ``RunnerType.NATIVE`` / ``"native"`` / ``"local"`` →
          ``LocalPhysicalPlan`` envelopes.
        * ``RunnerType.RAY`` / ``"ray"`` / ``"distributed"`` →
          ``DistributedPhysicalPlan`` envelopes.
        * ``None`` → infer from the currently configured Daft runner.

        Args:
            user_python_code: User script source.
            runner: Target runner type. ``None`` infers from Daft.
            job_namespace: Sub-directory under ``system_target_dir`` for
                this job's outputs.

        Returns:
            ``bytes`` deserializable by :meth:`UniteStreamRuntime.execute_ir`.

        Raises:
            CompilationError: If parsing / lowering / pickling fails.
        """
        bundle = self.compile_to_plans(
            user_python_code, job_namespace=job_namespace
        )
        kind = _resolve_plan_kind(runner)
        execution_config = _execution_config()

        # Snapshot the global partition cache *once* up front. All native
        # plans within this submission share the same cache view (Daft's
        # NativeRunner also passes the full snapshot, not a per-plan slice).
        psets = _snapshot_partition_set_cache() if kind is PlanKind.LOCAL else {}

        envelopes: list[PhysicalPlanEnvelope] = []
        for index, plan in enumerate(bundle):
            output_path = (
                f"{self._target_dir}/{bundle.job_namespace}/stream_job_{index}.parquet"
            )
            if kind is PlanKind.LOCAL:
                local_plan, inputs = _lower_to_local_plan(
                    plan,
                    stream_index=index,
                    execution_config=execution_config,
                    psets=psets,
                )
                envelopes.append(
                    PhysicalPlanEnvelope(
                        stream_index=index,
                        kind=PlanKind.LOCAL,
                        plan=local_plan,
                        inputs=inputs,
                        psets=psets,
                        output_path=output_path,
                        job_namespace=bundle.job_namespace,
                    )
                )
            else:
                dist_plan = _lower_to_distributed_plan(
                    plan,
                    stream_index=index,
                    execution_config=execution_config,
                )
                envelopes.append(
                    PhysicalPlanEnvelope(
                        stream_index=index,
                        kind=PlanKind.DISTRIBUTED,
                        plan=dist_plan,
                        output_path=output_path,
                        job_namespace=bundle.job_namespace,
                    )
                )

        return _serialize_envelopes(envelopes)

    def compile_and_extract_ir(
        self,
        user_python_code: str,
        *,
        runner: Any = None,
        job_namespace: str | None = None,
    ) -> bytes:
        """Alias of :meth:`compile_to_ir` retained for backwards compatibility."""
        return self.compile_to_ir(
            user_python_code,
            runner=runner,
            job_namespace=job_namespace,
        )
