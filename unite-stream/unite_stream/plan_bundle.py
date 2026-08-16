"""In-process carrier type for compiled UniteStream plans.

The cross-process pipeline uses ``bytes`` (cloudpickled
``list[PhysicalPlanEnvelope]``) as the transport between
:class:`UniteStreamCompiler` and :class:`UniteStreamRuntime`. For symmetry,
the in-process pipeline uses :class:`CompiledPlans` — a thin, named wrapper
around ``list[daft.DataFrame]`` that additionally carries the job metadata
(``job_namespace``, ``target_dir``) so the Runtime can log / validate /
debug the payload without re-parsing the source.

This gives both pathways the same shape::

    compile_to_plans(script)         -> CompiledPlans          # in-process
    compile_and_extract_ir(script)   -> bytes                  # cross-process

    runtime.execute_plans(CompiledPlans)  -> list[daft.DataFrame]
    runtime.execute_ir(bytes)             -> list[PyExecutionStats]
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from typing import Final

from daft.dataframe.dataframe import DataFrame


class CompiledPlans:
    """Bundle of bound, in-process :class:`daft.DataFrame` plans.

    Produced by :meth:`UniteStreamCompiler.compile_to_plans` and consumed by
    :meth:`UniteStreamRuntime.execute_plans`. The bundle is immutable after
    construction (the internal list is copied) so it is safe to pass around
    between threads.

    Args:
        plans: Iterable of lazy :class:`daft.DataFrame` plans, one per output
            stream, in the order they were appended to ``OUTPUT_STREAMS``.
        job_namespace: Sub-directory name (under ``target_dir``) where each
            plan's ``write_parquet`` output lives. ``None`` when no write
            binding has been applied.
        target_dir: Root directory under which ``job_namespace`` lives.
            ``None`` when no write binding has been applied.
    """

    __slots__ = ("_plans", "_job_namespace", "_target_dir")

    def __init__(
        self,
        plans: Iterable[DataFrame],
        *,
        job_namespace: str | None = None,
        target_dir: str | None = None,
    ) -> None:
        materialized: list[DataFrame] = []
        for index, plan in enumerate(plans):
            if not isinstance(plan, DataFrame):
                raise TypeError(
                    f"CompiledPlans: plans[{index}] 必须是 daft.DataFrame, "
                    f"got {type(plan).__name__}"
                )
            materialized.append(plan)
        self._plans: Final[tuple[DataFrame, ...]] = tuple(materialized)
        self._job_namespace: Final[str | None] = job_namespace
        self._target_dir: Final[str | None] = target_dir

    # ------------------------------------------------------------------ #
    # Read accessors                                                     #
    # ------------------------------------------------------------------ #

    @property
    def plans(self) -> list[DataFrame]:
        """Return a *copy* of the underlying plan list (defensive copy)."""
        return list(self._plans)

    @property
    def job_namespace(self) -> str | None:
        return self._job_namespace

    @property
    def target_dir(self) -> str | None:
        return self._target_dir

    @property
    def num_streams(self) -> int:
        return len(self._plans)

    # ------------------------------------------------------------------ #
    # Python container protocols                                         #
    # ------------------------------------------------------------------ #

    def __len__(self) -> int:
        return len(self._plans)

    def __iter__(self) -> Iterator[DataFrame]:
        return iter(self._plans)

    def __getitem__(self, index: int) -> DataFrame:
        return self._plans[index]

    def __repr__(self) -> str:
        return (
            f"CompiledPlans(num_streams={self.num_streams}, "
            f"job_namespace={self._job_namespace!r}, "
            f"target_dir={self._target_dir!r})"
        )
