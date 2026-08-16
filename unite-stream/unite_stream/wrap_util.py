"""Bidirectional adapters between UniteStream wrappers and raw Daft types.

These helpers are used by every generated method in
:mod:`unite_stream.dataframe` and :mod:`unite_stream.grouped` to:

1. ``unwrap``  ``UniteStream*`` wrappers in *arguments* back to the raw Daft
   types that the underlying ``daft.DataFrame`` / ``GroupedDataFrame`` expects;
2. ``wrap``    raw return values back into their ``UniteStream*`` counterparts
   so users never accidentally hold on to a non-wrapped ``daft.DataFrame``.

All functions are pure and free of module-level state, so they are safe to
call concurrently from multiple threads.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from daft.dataframe.dataframe import DataFrame, GroupedDataFrame


def unwrap_value(value: Any) -> Any:
    """Recursively strip any ``UniteStream*`` wrappers from ``value``."""
    # Local imports avoid a circular import: ``dataframe`` imports this module.
    from unite_stream.dataframe import UniteStreamDataFrame
    from unite_stream.grouped import UniteStreamGroupedDataFrame

    if isinstance(value, UniteStreamDataFrame):
        return value._df
    if isinstance(value, UniteStreamGroupedDataFrame):
        return value._grouped
    if isinstance(value, list):
        return [unwrap_value(v) for v in value]
    if isinstance(value, tuple):
        return tuple(unwrap_value(v) for v in value)
    return value


def unwrap_call_args(
    _func: Callable[..., Any],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
) -> tuple[tuple[Any, ...], dict[str, Any]]:
    """Unwrap positional and keyword arguments before forwarding to Daft."""
    return (
        tuple(unwrap_value(a) for a in args),
        {k: unwrap_value(v) for k, v in kwargs.items()},
    )


def wrap_result(result: Any) -> Any:
    """Wrap a raw ``DataFrame`` / ``GroupedDataFrame`` in its UniteStream type."""
    from unite_stream.dataframe import UniteStreamDataFrame
    from unite_stream.grouped import UniteStreamGroupedDataFrame

    if isinstance(result, DataFrame):
        return UniteStreamDataFrame(result)
    if isinstance(result, GroupedDataFrame):
        return UniteStreamGroupedDataFrame(result)
    return result
