"""High-level façade tying Parser + Compiler + Runtime together.

Use :class:`UniteStreamClient` when you want the simplest possible API. It
composes the lower-level stages internally and exposes two clean execution
paths so you can pick the right one for your deployment:

* **IR** (default for ``submit``) — :meth:`compile` lowers each plan into a
  ``LocalPhysicalPlan``, wraps it in a :class:`PhysicalPlanEnvelope`, and
  cloudpickles the list. :meth:`execute` drives each envelope through
  ``NativeExecutor``. This is the transportable pipeline.

* **In-process plans** — :meth:`compile_to_plans` keeps the lazy
  ``daft.DataFrame`` plans on the same process; :meth:`execute_plans`
  drives them through ``.collect()``. Useful for local debugging.

Each client owns one compiler and one runtime, so the same client can be
reused across many submissions. The compiler is thread-safe; the runtime
shares the *process-wide* Daft runner — see
:class:`unite_stream.UniteStreamRuntime`.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Final

from daft.dataframe.dataframe import DataFrame

from unite_stream.compiler import UniteStreamCompiler
from unite_stream.ir import ExecutionResult
from unite_stream.parser import UniteStreamParser
from unite_stream.plan_bundle import CompiledPlans
from unite_stream.runtime import RunnerType, UniteStreamRuntime


class UniteStreamClient:
    """One-stop façade over the UniteStream Parser / Compiler / Runtime stack.

    Args:
        system_target_dir: Root directory for ``stream_job_<i>.parquet`` outputs.
        runner: Daft runner to use; only ``RunnerType.NATIVE`` is supported.
        configure_runner: Whether to set the global Daft runner on init.
        extra_globals: Optional static names to inject into every script run.
    """

    __slots__ = ("_compiler", "_runtime")

    def __init__(
        self,
        system_target_dir: str,
        *,
        runner: RunnerType = RunnerType.NATIVE,
        configure_runner: bool = True,
        extra_globals: Mapping[str, Any] | None = None,
    ) -> None:
        self._compiler: Final[UniteStreamCompiler] = UniteStreamCompiler(
            system_target_dir, extra_globals=extra_globals
        )
        self._runtime: Final[UniteStreamRuntime] = UniteStreamRuntime(
            runner=runner,
            configure_runner=configure_runner,
        )

    @property
    def compiler(self) -> UniteStreamCompiler:
        return self._compiler

    @property
    def runtime(self) -> UniteStreamRuntime:
        return self._runtime

    @property
    def parser(self) -> UniteStreamParser:
        return self._compiler.parser

    @property
    def runner(self) -> RunnerType:
        return self._runtime.runner

    # ------------------------------------------------------------------ #
    # Parser layer                                                       #
    # ------------------------------------------------------------------ #

    def parse(self, user_python_code: str) -> list[DataFrame]:
        """Parse-only: return raw Daft plans without binding or executing."""
        return self._compiler.parse(user_python_code)

    # ------------------------------------------------------------------ #
    # Compiler layer (in-process plans + cross-process IR)               #
    # ------------------------------------------------------------------ #

    def compile_to_plans(
        self,
        user_python_code: str,
        *,
        job_namespace: str | None = None,
    ) -> CompiledPlans:
        """Parse + bind ``write_parquet`` paths; return a :class:`CompiledPlans`.

        Hand the returned bundle to :meth:`execute_plans` (or iterate it and
        call ``.collect()`` on each plan directly) to run on the native
        Daft runner.
        """
        return self._compiler.compile_to_plans(
            user_python_code, job_namespace=job_namespace
        )

    def compile(
        self,
        user_python_code: str,
        *,
        job_namespace: str | None = None,
        runner: RunnerType | None = None,
    ) -> bytes:
        """Parse + bind + lower to physical plans + cloudpickle.

        Emits ``LocalPhysicalPlan`` envelopes for the native runner.
        ``runner=None`` (default) uses *this client's* runtime runner.
        """
        return self._compiler.compile_to_ir(
            user_python_code,
            runner=runner or self._runtime.runner,
            job_namespace=job_namespace,
        )

    # ------------------------------------------------------------------ #
    # Runtime layer                                                      #
    # ------------------------------------------------------------------ #

    def execute_plans(
        self, plans: CompiledPlans | list[DataFrame]
    ) -> list[DataFrame]:
        """Execute a :class:`CompiledPlans` bundle (or a raw plan list).

        Accepts both forms so callers can wire directly from
        :meth:`UniteStreamParser.parse` (raw list) or from
        :meth:`compile_to_plans` (bundle).
        """
        return self._runtime.execute_plans(plans)

    def execute(self, ir_bytes: bytes) -> list[ExecutionResult]:
        """Deserialize and execute an IR payload via the configured runner."""
        return self._runtime.execute_ir(ir_bytes)

    # ------------------------------------------------------------------ #
    # End-to-end convenience                                             #
    # ------------------------------------------------------------------ #

    def submit(
        self,
        user_python_code: str,
        *,
        job_namespace: str | None = None,
        use_ir: bool | None = None,
    ) -> list[Any]:
        """Parse + bind + execute end-to-end.

        Two execution pipelines exist:

        * **IR path**: :meth:`compile` → cloudpickled
          ``list[PhysicalPlanEnvelope]`` → :meth:`UniteStreamRuntime.execute_ir`.
          The compiler always emits ``LocalPhysicalPlan`` envelopes. Returns
          a ``list[ExecutionResult]``.

        * **In-process plans path**: :meth:`compile_to_plans` → lazy
          ``daft.DataFrame`` plans → :meth:`execute_plans`. Returns the
          materialized ``list[daft.DataFrame]``.

        ``use_ir=None`` (default) routes through the IR pipeline — the
        ``LocalPhysicalPlan`` envelope is cloudpickle-safe and executable
        end-to-end. Pass ``use_ir=False`` to skip serialization and drive
        the lazy ``daft.DataFrame`` plans directly (useful for in-process
        debugging or to bypass ``cloudpickle`` overhead).

        Args:
            user_python_code: User script source.
            job_namespace: Sub-directory under ``system_target_dir`` for
                outputs of this job. Defaults to a fresh UUID.
            use_ir: Force IR / in-process path; ``None`` auto-selects.

        Returns:
            ``list[ExecutionResult]`` for the IR path, or
            ``list[daft.DataFrame]`` for the in-process plan path.
        """
        if use_ir is None:
            use_ir = True

        if use_ir:
            ir_bytes = self.compile(
                user_python_code, job_namespace=job_namespace
            )
            return self.execute(ir_bytes)

        bundle = self.compile_to_plans(
            user_python_code, job_namespace=job_namespace
        )
        return self.execute_plans(bundle)
