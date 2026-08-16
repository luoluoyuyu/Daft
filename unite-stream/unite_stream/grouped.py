"""Explicit per-method mirror of :class:`daft.dataframe.dataframe.GroupedDataFrame`."""
from __future__ import annotations

from typing import Any, Iterable, Union

from daft.dataframe.dataframe import DataFrame, GroupedDataFrame
from daft.expressions import Expression

from unite_stream.dataframe import UniteStreamDataFrame
from unite_stream.wrap_util import unwrap_call_args, wrap_result


class UniteStreamGroupedDataFrame:
    """Hand-written-style explicit wrapper: one method per Daft GroupedDataFrame API."""

    def __init__(self, grouped: GroupedDataFrame) -> None:
        self._grouped = grouped


    def agg(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._grouped, "agg"), args, kwargs)
        return wrap_result(getattr(self._grouped, "agg")(*u_args, **u_kwargs))


    def any_value(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._grouped, "any_value"), args, kwargs)
        return wrap_result(getattr(self._grouped, "any_value")(*u_args, **u_kwargs))


    def count(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._grouped, "count"), args, kwargs)
        return wrap_result(getattr(self._grouped, "count")(*u_args, **u_kwargs))


    def list_agg(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._grouped, "list_agg"), args, kwargs)
        return wrap_result(getattr(self._grouped, "list_agg")(*u_args, **u_kwargs))


    def list_agg_distinct(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._grouped, "list_agg_distinct"), args, kwargs)
        return wrap_result(getattr(self._grouped, "list_agg_distinct")(*u_args, **u_kwargs))


    def map_groups(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._grouped, "map_groups"), args, kwargs)
        return wrap_result(getattr(self._grouped, "map_groups")(*u_args, **u_kwargs))


    def max(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._grouped, "max"), args, kwargs)
        return wrap_result(getattr(self._grouped, "max")(*u_args, **u_kwargs))


    def mean(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._grouped, "mean"), args, kwargs)
        return wrap_result(getattr(self._grouped, "mean")(*u_args, **u_kwargs))


    def min(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._grouped, "min"), args, kwargs)
        return wrap_result(getattr(self._grouped, "min")(*u_args, **u_kwargs))


    def skew(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._grouped, "skew"), args, kwargs)
        return wrap_result(getattr(self._grouped, "skew")(*u_args, **u_kwargs))


    def stddev(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._grouped, "stddev"), args, kwargs)
        return wrap_result(getattr(self._grouped, "stddev")(*u_args, **u_kwargs))


    def string_agg(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._grouped, "string_agg"), args, kwargs)
        return wrap_result(getattr(self._grouped, "string_agg")(*u_args, **u_kwargs))


    def sum(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._grouped, "sum"), args, kwargs)
        return wrap_result(getattr(self._grouped, "sum")(*u_args, **u_kwargs))


    def var(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._grouped, "var"), args, kwargs)
        return wrap_result(getattr(self._grouped, "var")(*u_args, **u_kwargs))


    def __getitem__(self, item: int | str | slice | Iterable[str | int]) -> Union[Expression, UniteStreamDataFrame]:
        result = self._grouped[item]
        return wrap_result(result) if isinstance(result, DataFrame) else result
