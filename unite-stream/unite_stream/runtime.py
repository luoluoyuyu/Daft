"""Runtime stage of UniteStream.

The runtime materializes a payload produced by :class:`UniteStreamCompiler`.
Two **symmetric** entry points cover both Daft runners:

+----------------------+---------------------------------------------------+
| Method               | Input  →  Output                                  |
+======================+===================================================+
| ``execute_ir``       | ``bytes`` (cloudpickled                           |
|                      | ``list[PhysicalPlanEnvelope]``)                   |
|                      |  →  ``list[ExecutionResult]``                     |
|                      |                                                   |
|                      | Dispatches per-envelope based on ``PlanKind``:    |
|                      |  * ``LOCAL``  → :class:`daft.daft.NativeExecutor` |
|                      |  * ``DISTRIBUTED`` →                              |
|                      |    :class:`DistributedPhysicalPlanRunner`         |
+----------------------+---------------------------------------------------+
| ``execute_plans``    | :class:`CompiledPlans` or                         |
|                      | ``list[daft.DataFrame]``                          |
|                      |  →  ``list[daft.DataFrame]``                      |
|                      |                                                   |
|                      | In-process debug path. ``.collect()`` is          |
|                      | dispatched by Daft itself.                        |
+----------------------+---------------------------------------------------+

Runner configuration
--------------------
``daft.set_runner_native`` / ``daft.set_runner_ray`` are *process-global* and
Daft refuses to switch the runner once locked. The runtime probes
``daft.get_or_infer_runner_type`` first and only sets the runner when
needed; failures are downgraded to a debug log so multiple
``UniteStreamRuntime`` instances can coexist in the same process.

Thread safety
-------------
``execute_plans`` and ``execute_ir`` do not mutate instance state, so a
single :class:`UniteStreamRuntime` may be shared across threads. The global
Daft runner is process-wide, however — do not flip ``distributed_mode``
from multiple threads on the same process.
"""

from __future__ import annotations

import asyncio
import enum
import logging
from typing import Any, Final

import cloudpickle
import daft
from daft.dataframe.dataframe import DataFrame

from unite_stream.errors import RuntimeExecutionError
from unite_stream.ir import ExecutionResult, PhysicalPlanEnvelope, PlanKind
from unite_stream.plan_bundle import CompiledPlans

logger = logging.getLogger("unite_stream.runtime")


class RunnerType(str, enum.Enum):
    """Available Daft runner backends."""

    NATIVE = "native"
    RAY = "ray"


class UniteStreamRuntime:
    """Execute UniteStream IR payloads or raw Daft plans.

    Args:
        distributed_mode: Convenience boolean — ``True`` selects the Ray
            runner, ``False`` the native runner. Ignored when ``runner`` is
            given.
        runner: Explicit override for ``distributed_mode``. If supplied,
            takes precedence.
        configure_runner: If ``True`` (default) the global Daft runner is set
            on construction. Set ``False`` to leave the current runner
            untouched.
    """

    __slots__ = ("_runner",)

    def __init__(
        self,
        distributed_mode: bool = False,
        *,
        runner: RunnerType | None = None,
        configure_runner: bool = True,
    ) -> None:
        resolved = runner or (RunnerType.RAY if distributed_mode else RunnerType.NATIVE)
        self._runner: Final[RunnerType] = resolved

        if configure_runner:
            self._configure_global_runner(resolved)

    # ------------------------------------------------------------------ #
    # Runner configuration                                               #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _configure_global_runner(target: RunnerType) -> None:
        """Set Daft's process-global runner, tolerating already-configured state."""
        try:
            current = daft.get_or_infer_runner_type()
        except Exception:  # noqa: BLE001 — never let probing kill us
            current = None

        if current and str(current).lower() == target.value:
            return

        try:
            if target is RunnerType.RAY:
                daft.set_runner_ray()
            else:
                daft.set_runner_native()
        except Exception as exc:  # noqa: BLE001
            logger.debug(
                "[Runtime] set_runner_%s skipped: %s", target.value, exc
            )

    @property
    def runner(self) -> RunnerType:
        return self._runner

    @property
    def is_distributed(self) -> bool:
        return self._runner is RunnerType.RAY

    # ------------------------------------------------------------------ #
    # IR-driven execution (symmetric: native + Ray)                      #
    # ------------------------------------------------------------------ #

    def execute_ir(self, ir_bytes: bytes) -> list[ExecutionResult]:
        """Deserialize a cloudpickled IR payload and execute every envelope.

        The payload is expected to be a cloudpickled
        ``list[PhysicalPlanEnvelope]`` produced by
        :meth:`UniteStreamCompiler.compile_to_ir`. Each envelope is
        dispatched to the executor that matches its ``kind``:

        * ``PlanKind.LOCAL``        → ``daft.daft.NativeExecutor``
        * ``PlanKind.DISTRIBUTED``  → ``DistributedPhysicalPlanRunner``
          (Ray must be available).

        Args:
            ir_bytes: A cloudpickled list of :class:`PhysicalPlanEnvelope`.

        Returns:
            One :class:`ExecutionResult` per envelope, in submission order.

        Raises:
            RuntimeExecutionError: If the payload is malformed, an envelope's
                kind does not match the current runtime configuration, or
                any plan fails to execute.
        """
        try:
            envelopes = cloudpickle.loads(ir_bytes)
        except Exception as exc:  # noqa: BLE001
            raise RuntimeExecutionError(
                f"[Runtime] IR 反序列化失败: {type(exc).__name__}: {exc}"
            ) from exc

        if not isinstance(envelopes, list):
            raise RuntimeExecutionError(
                f"[Runtime] IR payload 类型异常: 期望 list, "
                f"got {type(envelopes).__name__}"
            )

        results: list[ExecutionResult] = []
        for index, envelope in enumerate(envelopes):
            if not isinstance(envelope, PhysicalPlanEnvelope):
                raise RuntimeExecutionError(
                    f"[Runtime] envelopes[{index}] 不是 PhysicalPlanEnvelope: "
                    f"{type(envelope).__name__}"
                )

            if envelope.kind is PlanKind.LOCAL:
                results.append(self._execute_local_envelope(envelope))
            elif envelope.kind is PlanKind.DISTRIBUTED:
                results.append(self._execute_distributed_envelope(envelope))
            else:  # pragma: no cover — exhaustive enum
                raise RuntimeExecutionError(
                    f"[Runtime] envelopes[{index}] 未知 PlanKind: {envelope.kind!r}"
                )

        return results

    # ------------------------------------------------------------------ #
    # In-process execution (debug path)                                  #
    # ------------------------------------------------------------------ #

    def execute_plans(
        self, plans: CompiledPlans | list[DataFrame]
    ) -> list[DataFrame]:
        """Materialize a list / bundle of lazy plans via ``.collect()``.

        Accepts either a :class:`CompiledPlans` bundle (preferred) or a raw
        ``list[daft.DataFrame]``. ``DataFrame.collect`` is dispatched by Daft
        according to the currently configured runner, so this path works for
        both **native** and **Ray** modes.

        Raises:
            RuntimeExecutionError: If any element is not a DataFrame or
                ``.collect`` raises.
        """
        if isinstance(plans, CompiledPlans):
            bundle = plans
        else:
            try:
                bundle = CompiledPlans(plans)
            except TypeError as exc:
                raise RuntimeExecutionError(
                    f"[Runtime] execute_plans 入参非法: {exc}"
                ) from exc

        results: list[DataFrame] = []
        for index, plan in enumerate(bundle):
            try:
                materialized = plan.collect(num_preview_rows=None)
            except Exception as exc:  # noqa: BLE001
                raise RuntimeExecutionError(
                    f"[Runtime] 执行 stream_job_{index} 失败 "
                    f"(runner={self._runner.value}, "
                    f"namespace={bundle.job_namespace!r}): "
                    f"{type(exc).__name__}: {exc}"
                ) from exc
            results.append(materialized)
            logger.debug(
                "stream_job_%d done, runner=%s, namespace=%s, rows=%d",
                index,
                self._runner.value,
                bundle.job_namespace,
                len(materialized),
            )
        return results

    # ------------------------------------------------------------------ #
    # Per-envelope dispatch                                              #
    # ------------------------------------------------------------------ #

    def _execute_local_envelope(
        self, envelope: PhysicalPlanEnvelope
    ) -> ExecutionResult:
        """Drive a ``LocalPhysicalPlan`` envelope via :class:`NativeExecutor`.

        The envelope carries everything the executor needs to run
        independently of the originating compile process:

        * ``plan`` — the cloudpickled :class:`LocalPhysicalPlan`.
        * ``inputs`` — the ``source_id → input`` map produced by
          ``LocalPhysicalPlan.from_logical_plan_builder``.
        * ``psets`` — a snapshot of Daft's partition_set_cache so source
          operators reading ``from_pydict`` / ``from_arrow`` data can find
          their in-memory partitions.

        We replay the minimal subset of :class:`NativeRunner`'s lifecycle
        (query-id emission, query-start / optimization / query-end
        notifications, heartbeat) so dashboard subscribers see a clean
        query, then stream every :class:`LocalMaterializedResult` out of
        the executor and return an :class:`ExecutionResult` with the
        final :class:`PyExecutionStats`.
        """
        if self._runner is not RunnerType.NATIVE:
            raise RuntimeExecutionError(
                f"[Runtime] envelope[{envelope.stream_index}] kind=LOCAL "
                f"需要 native 模式，当前 runner={self._runner.value}"
            )

        try:
            import platform

            import daft as _daft
            from daft.context import get_context
            from daft.daft import PyQueryMetadata, PyQueryResult, PySchema, QueryEndState
            from daft.execution.native_executor import NativeExecutor
            from daft.naming import generate_query_name
        except ImportError as exc:
            raise RuntimeExecutionError(
                "[Runtime] daft 子模块缺失，无法执行 LocalPhysicalPlan: " f"{exc}"
            ) from exc

        ctx = get_context()
        query_id = generate_query_name()

        empty_schema = PySchema.from_field_name_and_types([])
        try:
            ctx._notify_query_start(
                query_id,
                PyQueryMetadata(
                    empty_schema,
                    "{}",
                    "Native (Swordfish) [unite-stream]",
                    None,
                    "unite-stream",
                    platform.python_version(),
                    _daft.get_version(),
                ),
            )
        except Exception as exc:  # noqa: BLE001 — notifications best-effort
            logger.debug("[Runtime] notify_query_start skipped: %s", exc)

        heartbeat: Any | None = None
        try:
            from daft.runners.heartbeat import Heartbeat as _Heartbeat

            heartbeat = _Heartbeat(10.0, ctx, query_id)
            heartbeat.start()
        except ImportError:
            pass

        num_partitions = 0
        stats: Any | None = None
        try:
            try:
                ctx._notify_optimization_start(query_id)
                ctx._notify_optimization_end(query_id, "{}")
            except Exception as exc:  # noqa: BLE001
                logger.debug("[Runtime] optimization notifications skipped: %s", exc)

            executor = NativeExecutor()
            try:
                results_gen = executor.run(
                    envelope.plan,
                    envelope.inputs or {},
                    ctx,
                    {"query_id": query_id},
                )
                while True:
                    try:
                        _result = next(results_gen)
                        num_partitions += 1
                    except StopIteration as stop:
                        value = stop.value
                        if isinstance(value, tuple) and len(value) == 2:
                            _query_plan, stats = value
                        else:
                            stats = value
                        break
            except Exception as exc:  # noqa: BLE001
                try:
                    ctx._notify_query_end(
                        query_id,
                        PyQueryResult(QueryEndState.Failed, f"{type(exc).__name__}: {exc}"),
                    )
                except Exception:  # noqa: BLE001
                    pass
                raise RuntimeExecutionError(
                    f"[Runtime] 执行 local stream_job_{envelope.stream_index} 失败: "
                    f"{type(exc).__name__}: {exc}"
                ) from exc

            try:
                ctx._notify_query_end(
                    query_id, PyQueryResult(QueryEndState.Finished, "Query finished")
                )
            except Exception as exc:  # noqa: BLE001
                logger.debug("[Runtime] notify_query_end skipped: %s", exc)
        finally:
            if heartbeat is not None:
                heartbeat.stop()

        logger.debug(
            "stream_job_%d (local) finished, partitions=%d, namespace=%s, query_id=%s",
            envelope.stream_index,
            num_partitions,
            envelope.job_namespace,
            query_id,
        )
        return ExecutionResult(
            stream_index=envelope.stream_index,
            kind=PlanKind.LOCAL,
            output_path=envelope.output_path,
            stats=stats,
            num_partitions=num_partitions,
        )

    def _execute_distributed_envelope(
        self, envelope: PhysicalPlanEnvelope
    ) -> ExecutionResult:
        """Drive a ``DistributedPhysicalPlan`` envelope via the Ray runner."""
        if self._runner is not RunnerType.RAY:
            raise RuntimeExecutionError(
                f"[Runtime] envelope[{envelope.stream_index}] kind=DISTRIBUTED "
                f"需要 Ray 模式（distributed_mode=True / runner=RunnerType.RAY），"
                f"当前 runner={self._runner.value}"
            )

        try:
            from daft.daft import DistributedPhysicalPlanRunner
        except ImportError as exc:
            raise RuntimeExecutionError(
                "[Runtime] 当前 Daft 未暴露 DistributedPhysicalPlanRunner"
            ) from exc

        runner = DistributedPhysicalPlanRunner()
        num_partitions = 0
        stats: Any | None = None

        async def _drain(plan: Any) -> tuple[int, Any]:
            count = 0
            local_stats: Any | None = None
            stream = runner.run_plan(plan, {})
            try:
                async for _partition_ref in stream:
                    count += 1
            finally:
                try:
                    local_stats = stream.finish()
                except Exception:  # noqa: BLE001 — stats are best-effort
                    local_stats = None
            return count, local_stats

        try:
            num_partitions, stats = asyncio.run(_drain(envelope.plan))
        except Exception as exc:  # noqa: BLE001
            raise RuntimeExecutionError(
                f"[Runtime] 执行 distributed stream_job_{envelope.stream_index} 失败: "
                f"{type(exc).__name__}: {exc}"
            ) from exc

        logger.debug(
            "stream_job_%d (distributed) finished, partitions=%d, namespace=%s",
            envelope.stream_index,
            num_partitions,
            envelope.job_namespace,
        )
        return ExecutionResult(
            stream_index=envelope.stream_index,
            kind=PlanKind.DISTRIBUTED,
            output_path=envelope.output_path,
            stats=stats,
            num_partitions=num_partitions,
        )
