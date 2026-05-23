"""Serialize and execute lazy Daft plans across separate parser and runtime processes.

Use :meth:`DataFrame.to_plan_bytes` / :func:`serialize_plan` on the parser side after
building a lazy plan (before ``collect``, ``show``, or materializing writes). Use
:meth:`DataFrame.from_plan_bytes` / :func:`deserialize_plan` / :func:`execute_plan`
on the runtime side to restore and run the plan locally or on Ray.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from daft.pickle.pickle import dumps, loads
from daft.runners import set_runner_native, set_runner_ray

if TYPE_CHECKING:
    from daft.dataframe.dataframe import DataFrame
    from daft.logical.builder import LogicalPlanBuilder


def serialize_plan(df: DataFrame) -> bytes:
    """Serialize a lazy :class:`DataFrame` plan to bytes (cloudpickle).

    The plan is dehydrated from the current logical builder only; any materialized
    result cache on ``df`` is not included.
    """
    from daft.dataframe.dataframe import DataFrame

    if not isinstance(df, DataFrame):
        raise TypeError(f"serialize_plan expects a DataFrame, got {type(df)!r}")
    return dumps(DataFrame(df._get_current_builder()))


def deserialize_plan(plan_bytes: bytes) -> DataFrame:
    """Restore a lazy :class:`DataFrame` from :func:`serialize_plan` bytes."""
    from daft.dataframe.dataframe import DataFrame
    from daft.logical.builder import LogicalPlanBuilder

    obj = loads(plan_bytes)
    if isinstance(obj, DataFrame):
        return obj
    if isinstance(obj, LogicalPlanBuilder):
        return DataFrame(obj)
    raise TypeError(f"Expected DataFrame or LogicalPlanBuilder after unpickle, got {type(obj)!r}")


def execute_plan(
    plan_bytes: bytes,
    *,
    use_ray: bool = False,
    num_preview_rows: int | None = 8,
) -> DataFrame:
    """Deserialize a plan and materialize it with :meth:`DataFrame.collect`."""
    if use_ray:
        set_runner_ray()
    else:
        set_runner_native()
    return deserialize_plan(plan_bytes).collect(num_preview_rows=num_preview_rows)


# Alias for parser/runtime split naming.
execute_from_bytes = execute_plan
