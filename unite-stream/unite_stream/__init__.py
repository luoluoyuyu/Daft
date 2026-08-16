"""UniteStream: a Parser / Runtime split for Daft.

Public API (layered)
--------------------

The package exposes two execution pathways with the same Parser/Compiler
front-end but different transports. Both support the **native** and **Ray**
Daft runners (within their respective constraints):

::

                          user script
                              │
                              ▼
   ┌───────────────────────────────────────────────────────────┐
   │ UniteStreamParser.parse(script)                           │
   │   → list[daft.DataFrame]   (raw unbound plans)            │
   └───────────────────────────────────────────────────────────┘
                              │
              ┌───────────────┴────────────────┐
              ▼                                 ▼
   ┌──────────────────────────┐    ┌──────────────────────────┐
   │ Compiler.compile_to_ir   │    │ Compiler.compile_to_plans│
   │ (bind + lower + pickle)  │    │ (bind only, keep lazy)   │
   │                          │    │                          │
   │ runner=NATIVE  → Local   │    │  → CompiledPlans         │
   │ runner=RAY     → Distr.  │    │    (debug / direct       │
   │  → bytes                 │    │     .collect())          │
   │ (cloudpickled            │    └──────────────────────────┘
   │  list[PhysicalPlanEnv.]) │                  │
   └──────────────────────────┘                  ▼
              │                       ┌──────────────────────────┐
              ▼                       │ Runtime.execute_plans    │
   ┌──────────────────────────┐       │ (CompiledPlans|list[DF]) │
   │ Runtime.execute_ir       │       │   .collect() in-process  │
   │ (bytes)                  │       │   → list[daft.DataFrame] │
   │                          │       └──────────────────────────┘
   │ LOCAL  → NativeExecutor  │
   │ DISTR. → DistributedPhys.│
   │          PlanRunner      │
   │  → list[ExecutionResult] │
   └──────────────────────────┘

                 UniteStreamClient.submit(script, use_ir=True|False)
                 ─────────────────────────────────────────────────
                          one-shot façade over both paths

Top-level shortcut functions :func:`parse` and :func:`submit` are also
provided for the common cases.

Script execution guarantees
---------------------------
User scripts need **no** ``import`` statements: ``UniteStream``, the UDF
decorators (``func`` / ``cls`` / ``method`` / ``udf`` / ``metrics``) and the
``daft`` module are auto-injected. ``OUTPUT_STREAMS`` is allocated fresh per
call as a method-local list, so concurrent parses on a single instance never
interfere with each other. The script runs with full Python builtins and is
free to ``import`` modules or use any dynamic-code primitive it needs.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from daft.dataframe.dataframe import DataFrame

from unite_stream.client import UniteStreamClient
from unite_stream.compiler import UniteStreamCompiler
from unite_stream.dataframe import UniteStreamDataFrame
from unite_stream.errors import (
    CompilationError,
    RuntimeExecutionError,
    UniteStreamError,
)
from unite_stream.grouped import UniteStreamGroupedDataFrame
from unite_stream.ir import ExecutionResult, PhysicalPlanEnvelope, PlanKind
from unite_stream.module import UniteStream, UniteStreamModule, UniteStreamNamespace
from unite_stream.parser import UniteStreamParser
from unite_stream.plan_bundle import CompiledPlans
from unite_stream.runtime import RunnerType, UniteStreamRuntime
from unite_stream.udf import UDF, cls, func, method, metrics, udf

__version__ = "0.1.0"


def parse(
    user_python_code: str,
    *,
    extra_globals: Mapping[str, Any] | None = None,
) -> list[DataFrame]:
    """Parse a script and return the raw Daft plans in ``OUTPUT_STREAMS``.

    Convenience wrapper around :class:`UniteStreamParser`. Use this when you
    just need lazy ``daft.DataFrame`` plans and want to handle binding /
    execution yourself.
    """
    return UniteStreamParser(extra_globals=extra_globals).parse(user_python_code)


def submit(
    user_python_code: str,
    *,
    system_target_dir: str,
    distributed_mode: bool = False,
    runner: RunnerType | None = None,
    configure_runner: bool = True,
    job_namespace: str | None = None,
    extra_globals: Mapping[str, Any] | None = None,
    use_ir: bool | None = None,
) -> list[Any]:
    """Parse, compile, and execute a script end-to-end.

    Args:
        user_python_code: User script source.
        system_target_dir: Root directory for ``stream_job_<i>.parquet`` outputs.
        distributed_mode: ``True`` selects the Ray runner, ``False`` native.
        runner: Explicit runner override (takes precedence over
            ``distributed_mode``).
        configure_runner: Whether to set the global Daft runner on init.
        job_namespace: Sub-directory under ``system_target_dir`` for this job.
        extra_globals: Optional static names to inject into the script globals.
        use_ir: Force the IR pipeline (``True``) or the in-process plan
            pipeline (``False``); ``None`` (default) uses the IR pipeline
            for both runners — both ``LocalPhysicalPlan`` and
            ``DistributedPhysicalPlan`` envelopes round-trip through
            ``cloudpickle`` and execute end-to-end.

    Returns:
        ``list[ExecutionResult]`` for the IR path, or
        ``list[daft.DataFrame]`` for the in-process plan path.
    """
    client = UniteStreamClient(
        system_target_dir,
        distributed_mode=distributed_mode,
        runner=runner,
        configure_runner=configure_runner,
        extra_globals=extra_globals,
    )
    return client.submit(
        user_python_code,
        job_namespace=job_namespace,
        use_ir=use_ir,
    )


__all__ = [
    "__version__",
    "CompilationError",
    "CompiledPlans",
    "ExecutionResult",
    "PhysicalPlanEnvelope",
    "PlanKind",
    "RunnerType",
    "RuntimeExecutionError",
    "UDF",
    "UniteStream",
    "UniteStreamClient",
    "UniteStreamCompiler",
    "UniteStreamDataFrame",
    "UniteStreamError",
    "UniteStreamGroupedDataFrame",
    "UniteStreamModule",
    "UniteStreamNamespace",
    "UniteStreamParser",
    "UniteStreamRuntime",
    "cls",
    "func",
    "method",
    "metrics",
    "parse",
    "submit",
    "udf",
]
