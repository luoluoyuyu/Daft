#!/usr/bin/env python3
"""Regenerate explicit UniteStream mirrors of all Daft public APIs.

Run from repo root::

    python unite-stream/scripts/regenerate_api_mirror.py

Output files (overwritten):
- ``unite_stream/dataframe.py``  — :class:`UniteStreamDataFrame`
- ``unite_stream/grouped.py``    — :class:`UniteStreamGroupedDataFrame`
- ``unite_stream/module.py``     — :class:`UniteStreamNamespace` (top-level ``daft`` mirror)
"""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PKG = ROOT / "unite_stream"

# ---------------------------------------------------------------------------
# DataFrame surface
# ---------------------------------------------------------------------------

# Methods that must never run inside a user script: materialization, export,
# system-bound writes, or anything that depends on collecting the plan.
FORBIDDEN = sorted(
    {
        "collect",
        "show",
        "count_rows",
        "explain",
        "to_plan_bytes",
        "from_plan_bytes",
        "write_sql",
        "write_parquet",
        "write_csv",
        "write_json",
        "write_iceberg",
        "write_paimon",
        "write_deltalake",
        "write_sink",
        "write_lance",
        "write_turbopuffer",
        "write_clickhouse",
        "write_huggingface",
        "write_bigtable",
        "write_table",
        "to_pandas",
        "to_arrow",
        "to_pydict",
        "to_pylist",
        "to_torch_map_dataset",
        "to_torch_iter_dataset",
        "to_arrow_iter",
        "iter_rows",
        "iter_partitions",
        "__iter__",
        "metrics",
        "num_partitions",
        "pivot",
    }
)

# Lazy DataFrame transforms — forwarded to the underlying ``daft.DataFrame``.
DF_METHODS = sorted(
    {
        "agg",
        "agg_concat",
        "agg_list",
        "agg_set",
        "any_value",
        "concat",
        "count",
        "describe",
        "distinct",
        "drop_duplicates",
        "drop_nan",
        "drop_null",
        "except_all",
        "except_distinct",
        "exclude",
        "explode",
        "filter",
        "intersect",
        "intersect_all",
        "into_batches",
        "into_partitions",
        "join",
        "limit",
        "max",
        "min",
        "mean",
        "melt",
        "offset",
        "pipe",
        "repartition",
        "sample",
        "select",
        "shuffle",
        "skew",
        "skip_existing",
        "sort",
        "stddev",
        "sum",
        "summarize",
        "transform",
        "union",
        "union_all",
        "union_all_by_name",
        "union_by_name",
        "unique",
        "unpivot",
        "var",
        "where",
        "with_column",
        "with_columns",
        "with_column_renamed",
        "with_columns_renamed",
    }
)

GDF_METHODS = sorted(
    {
        "agg",
        "any_value",
        "count",
        "list_agg",
        "list_agg_distinct",
        "map_groups",
        "max",
        "mean",
        "min",
        "skew",
        "stddev",
        "string_agg",
        "sum",
        "var",
    }
)

# ---------------------------------------------------------------------------
# Top-level ``daft`` surface
# ---------------------------------------------------------------------------

# Each entry below mirrors a name in ``daft.__all__`` (or one of its modules).
# Names intentionally NOT mirrored (Parser/Runtime separation boundaries):
#   serialize_plan, deserialize_plan, execute_plan, execute_from_bytes,
#   set_runner_native, get_or_create_runner,
#   get_or_infer_runner_type, write_table  (Session-level materialization)

# Functions that take args and return a DataFrame (need ``wrap_result``).
# Note: SQL entrypoints (``daft.sql`` / ``daft.read_sql``) are intentionally
# omitted — UniteStream only exposes the DataFrame surface.
DF_RETURNING_FUNCS = sorted(
    {
        # io.read_* / io.from_*
        "read_parquet",
        "read_csv",
        "read_json",
        "read_text",
        "read_warc",
        "read_iceberg",
        "read_deltalake",
        "read_hudi",
        "read_paimon",
        "read_kafka",
        "read_huggingface",
        "read_mcap",
        "read_video_frames",
        "read_lance",
        "from_files",
        "from_glob_path",
        "from_pydict",
        "from_pylist",
        "from_arrow",
        "from_pandas",
        # session-level reader
        "read_table",
        # range generator
        "range",
    }
)

# Expression / value helpers (return an Expression).
# ``sql_expr`` is intentionally omitted (no SQL support).
EXPR_HELPERS = sorted(["col", "lit", "element", "interval"])

# UDF / metrics decorators.
UDF_HELPERS = sorted(["func", "cls", "method", "udf", "metrics"])

# Class objects exposed as attributes (no execution, just types/builders).
CLASS_REFS = sorted(
    [
        "AudioFile",
        "Catalog",
        "DataFrame",
        "DataType",
        "Expression",
        "File",
        "IOConfig",
        "Identifier",
        "ImageFormat",
        "ImageMode",
        "ImageProperty",
        "MediaType",
        "ResourceRequest",
        "Schema",
        "Series",
        "Session",
        "Table",
        "TimeUnit",
        "VideoFile",
        "Window",
    ]
)

# Stateless passthrough functions (return arbitrary objects, no DataFrame wrap).
PASSTHRU_FUNCS = sorted(
    [
        # context / config
        "get_context",
        "attach_subscriber",
        "detach_subscriber",
        "set_execution_config",
        "set_planning_config",
        "with_subscriber",
        "execution_config_ctx",
        "planning_config_ctx",
        # session core
        "session",
        "set_session",
        "current_session",
        "current_catalog",
        "current_namespace",
        "current_provider",
        "current_model",
        # attach / detach
        "attach",
        "attach_catalog",
        "attach_function",
        "attach_provider",
        "attach_table",
        "detach_catalog",
        "detach_function",
        "detach_provider",
        "detach_table",
        # namespace / table lifecycle
        "create_namespace",
        "create_namespace_if_not_exists",
        "create_table",
        "create_table_if_not_exists",
        "create_temp_table",
        "drop_namespace",
        "drop_table",
        # catalog / function / provider lookups
        "get_catalog",
        "get_function",
        "get_provider",
        "get_table",
        "has_catalog",
        "has_namespace",
        "has_provider",
        "has_table",
        "list_catalogs",
        "list_tables",
        "load_extension",
        "set_catalog",
        "set_model",
        "set_namespace",
        "set_provider",
        # misc
        "register_viz_hook",
        "refresh_logger",
        "get_version",
        "get_build_type",
    ]
)

# Submodules exposed as attributes (carefully chosen; ``runners`` / ``io`` /
# ``datasets`` / ``context`` are NOT exposed because they expose execution or
# raw DataFrame paths bypassing the UniteStream wrapper).
SUBMODULES = sorted(["functions"])


# ---------------------------------------------------------------------------
# DataFrame / GroupedDataFrame generators
# ---------------------------------------------------------------------------


def _forbidden_method(name: str, cls: str = "UniteStreamDataFrame") -> str:
    return f'''
    def {name}(self, *args, **kwargs):
        raise AttributeError(
            "{cls} 不允许调用 '{name}'；请提交到 OUTPUT_STREAMS，由系统绑定写入并在 Runtime 执行。"
        )
'''


def _df_forward(name: str) -> str:
    if name == "groupby":
        return '''
    def groupby(self, *args, **kwargs) -> "UniteStreamGroupedDataFrame":
        from unite_stream.grouped import UniteStreamGroupedDataFrame

        u_args, u_kwargs = unwrap_call_args(getattr(self._df, "groupby"), args, kwargs)
        return UniteStreamGroupedDataFrame(getattr(self._df, "groupby")(*u_args, **u_kwargs))
'''
    return f'''
    def {name}(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._df, "{name}"), args, kwargs)
        return wrap_result(getattr(self._df, "{name}")(*u_args, **u_kwargs))
'''


def _gdf_forward(name: str) -> str:
    return f'''
    def {name}(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._grouped, "{name}"), args, kwargs)
        return wrap_result(getattr(self._grouped, "{name}")(*u_args, **u_kwargs))
'''


def gen_dataframe() -> str:
    parts = [
        '"""Explicit per-method mirror of :class:`daft.DataFrame` (see ``scripts/regenerate_api_mirror.py``)."""',
        "from __future__ import annotations",
        "",
        "from typing import TYPE_CHECKING, Any, Iterable, Union",
        "",
        "from daft.dataframe.dataframe import DataFrame",
        "from daft.expressions import Expression",
        "from daft.schema import Schema",
        "",
        "from unite_stream.wrap_util import unwrap_call_args, wrap_result",
        "",
        "if TYPE_CHECKING:",
        "    from unite_stream.grouped import UniteStreamGroupedDataFrame",
        "",
        "",
        "class UniteStreamDataFrame:",
        '    """Hand-written-style explicit wrapper: one method per Daft DataFrame API."""',
        "",
        "    def __init__(self, daft_df: DataFrame) -> None:",
        "        self._df = daft_df",
        "",
        "    @property",
        "    def _inner(self) -> DataFrame:",
        "        return self._df",
        "",
    ]
    for name in FORBIDDEN:
        parts.append(_forbidden_method(name))
    parts.append(_df_forward("groupby"))
    for name in DF_METHODS:
        if name == "groupby":
            continue
        parts.append(_df_forward(name))
    parts += [
        "",
        "    @property",
        "    def column_names(self) -> list[str]:",
        "        return self._df.column_names",
        "",
        "    @property",
        "    def columns(self) -> list[Expression]:",
        "        return self._df.columns",
        "",
        "    def schema(self) -> Schema:",
        "        return self._df.schema()",
        "",
        "    def __getitem__(self, item: int | str | slice | Iterable[str | int]) -> Union[Expression, UniteStreamDataFrame]:",
        "        result = self._df[item]",
        "        return wrap_result(result) if isinstance(result, DataFrame) else result",
        "",
        "    def __repr__(self) -> str:",
        '        return "<UniteStreamDataFrame>"',
        "",
    ]
    return "\n".join(parts)


def gen_grouped() -> str:
    parts = [
        '"""Explicit per-method mirror of :class:`daft.dataframe.dataframe.GroupedDataFrame`."""',
        "from __future__ import annotations",
        "",
        "from typing import Any, Iterable, Union",
        "",
        "from daft.dataframe.dataframe import DataFrame, GroupedDataFrame",
        "from daft.expressions import Expression",
        "",
        "from unite_stream.dataframe import UniteStreamDataFrame",
        "from unite_stream.wrap_util import unwrap_call_args, wrap_result",
        "",
        "",
        "class UniteStreamGroupedDataFrame:",
        '    """Hand-written-style explicit wrapper: one method per Daft GroupedDataFrame API."""',
        "",
        "    def __init__(self, grouped: GroupedDataFrame) -> None:",
        "        self._grouped = grouped",
        "",
    ]
    for name in GDF_METHODS:
        parts.append(_gdf_forward(name))
    parts += [
        "",
        "    def __getitem__(self, item: int | str | slice | Iterable[str | int]) -> Union[Expression, UniteStreamDataFrame]:",
        "        result = self._grouped[item]",
        "        return wrap_result(result) if isinstance(result, DataFrame) else result",
        "",
    ]
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Top-level module generator
# ---------------------------------------------------------------------------

# daft.range / daft.read_lance are exposed via ``__getattr__``; access via
# ``daft.read_lance`` / ``daft.range`` works at call time.
_TOPLEVEL_CALLABLE_SOURCE = "daft"


def gen_module() -> str:
    parts = [
        '"""Explicit mirror of top-level ``daft`` public API.',
        "",
        "Generated by ``scripts/regenerate_api_mirror.py``. Do not edit by hand.",
        '"""',
        "from __future__ import annotations",
        "",
        "from typing import Any",
        "",
        "import daft",
        "import daft.functions as _daft_functions",
        "",
        "from unite_stream.dataframe import UniteStreamDataFrame",
        "from unite_stream.udf import cls, func, method, metrics, udf",
        "from unite_stream.wrap_util import wrap_result",
        "",
        "",
        "class UniteStreamNamespace:",
        '    """One explicit static method / class attribute per public ``daft`` API."""',
        "",
    ]

    # 1. DataFrame-returning functions (need wrap).
    for name in DF_RETURNING_FUNCS:
        parts += [
            "    @staticmethod",
            f"    def {name}(*args: Any, **kwargs: Any) -> UniteStreamDataFrame:",
            f"        return wrap_result({_TOPLEVEL_CALLABLE_SOURCE}.{name}(*args, **kwargs))",
            "",
        ]

    # 2. Expression helpers (no wrap).
    for name in EXPR_HELPERS:
        parts += [
            "    @staticmethod",
            f"    def {name}(*args: Any, **kwargs: Any) -> Any:",
            f"        return {_TOPLEVEL_CALLABLE_SOURCE}.{name}(*args, **kwargs)",
            "",
        ]

    # 3. UDF / metrics decorators (already imported above).
    for name in UDF_HELPERS:
        parts += [
            "    @staticmethod",
            f"    def {name}(*args: Any, **kwargs: Any) -> Any:",
            f"        return {name}(*args, **kwargs)",
            "",
        ]

    # 4. Passthrough functions (context, session, catalog, misc).
    for name in PASSTHRU_FUNCS:
        parts += [
            "    @staticmethod",
            f"    def {name}(*args: Any, **kwargs: Any) -> Any:",
            f"        return {_TOPLEVEL_CALLABLE_SOURCE}.{name}(*args, **kwargs)",
            "",
        ]

    parts.append("")

    # 5. Class references (assigned as class attributes after the class body).
    parts.append("")
    for name in CLASS_REFS:
        parts.append(f"UniteStreamNamespace.{name} = daft.{name}")

    # 6. Submodules.
    parts.append("")
    for name in SUBMODULES:
        if name == "functions":
            parts.append("UniteStreamNamespace.functions = _daft_functions")
        else:
            parts.append(f"UniteStreamNamespace.{name} = daft.{name}")

    parts += [
        "",
        "UniteStream = UniteStreamNamespace()",
        "UniteStreamModule = UniteStreamNamespace",
        "",
    ]
    return "\n".join(parts)


def main() -> None:
    (PKG / "dataframe.py").write_text(gen_dataframe())
    (PKG / "grouped.py").write_text(gen_grouped())
    (PKG / "module.py").write_text(gen_module())
    total_top = (
        len(DF_RETURNING_FUNCS)
        + len(EXPR_HELPERS)
        + len(UDF_HELPERS)
        + len(PASSTHRU_FUNCS)
        + len(CLASS_REFS)
        + len(SUBMODULES)
    )
    print(
        "Regenerated:"
        f" {len(FORBIDDEN)} df-forbidden,"
        f" {len(DF_METHODS)} df-forward,"
        f" {len(GDF_METHODS)} grouped,"
        f" {len(DF_RETURNING_FUNCS)} df-returning,"
        f" {len(EXPR_HELPERS)} expr,"
        f" {len(UDF_HELPERS)} udf,"
        f" {len(PASSTHRU_FUNCS)} passthru,"
        f" {len(CLASS_REFS)} classes,"
        f" {len(SUBMODULES)} submodules"
        f" (top-level total: {total_top})"
    )


if __name__ == "__main__":
    main()
