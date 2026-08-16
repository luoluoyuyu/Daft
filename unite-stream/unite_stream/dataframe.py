"""Explicit per-method mirror of :class:`daft.DataFrame` (see ``scripts/regenerate_api_mirror.py``)."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Iterable, Union

from daft.dataframe.dataframe import DataFrame
from daft.expressions import Expression
from daft.schema import Schema

from unite_stream.wrap_util import unwrap_call_args, wrap_result

if TYPE_CHECKING:
    from unite_stream.grouped import UniteStreamGroupedDataFrame


class UniteStreamDataFrame:
    """Hand-written-style explicit wrapper: one method per Daft DataFrame API."""

    def __init__(self, daft_df: DataFrame) -> None:
        self._df = daft_df

    @property
    def _inner(self) -> DataFrame:
        return self._df


    def __iter__(self, *args, **kwargs):
        raise AttributeError(
            "UniteStreamDataFrame 不允许调用 '__iter__'；请提交到 OUTPUT_STREAMS，由系统绑定写入并在 Runtime 执行。"
        )


    def collect(self, *args, **kwargs):
        raise AttributeError(
            "UniteStreamDataFrame 不允许调用 'collect'；请提交到 OUTPUT_STREAMS，由系统绑定写入并在 Runtime 执行。"
        )


    def count_rows(self, *args, **kwargs):
        raise AttributeError(
            "UniteStreamDataFrame 不允许调用 'count_rows'；请提交到 OUTPUT_STREAMS，由系统绑定写入并在 Runtime 执行。"
        )


    def explain(self, *args, **kwargs):
        raise AttributeError(
            "UniteStreamDataFrame 不允许调用 'explain'；请提交到 OUTPUT_STREAMS，由系统绑定写入并在 Runtime 执行。"
        )


    def from_plan_bytes(self, *args, **kwargs):
        raise AttributeError(
            "UniteStreamDataFrame 不允许调用 'from_plan_bytes'；请提交到 OUTPUT_STREAMS，由系统绑定写入并在 Runtime 执行。"
        )


    def iter_partitions(self, *args, **kwargs):
        raise AttributeError(
            "UniteStreamDataFrame 不允许调用 'iter_partitions'；请提交到 OUTPUT_STREAMS，由系统绑定写入并在 Runtime 执行。"
        )


    def iter_rows(self, *args, **kwargs):
        raise AttributeError(
            "UniteStreamDataFrame 不允许调用 'iter_rows'；请提交到 OUTPUT_STREAMS，由系统绑定写入并在 Runtime 执行。"
        )


    def metrics(self, *args, **kwargs):
        raise AttributeError(
            "UniteStreamDataFrame 不允许调用 'metrics'；请提交到 OUTPUT_STREAMS，由系统绑定写入并在 Runtime 执行。"
        )


    def num_partitions(self, *args, **kwargs):
        raise AttributeError(
            "UniteStreamDataFrame 不允许调用 'num_partitions'；请提交到 OUTPUT_STREAMS，由系统绑定写入并在 Runtime 执行。"
        )


    def pivot(self, *args, **kwargs):
        raise AttributeError(
            "UniteStreamDataFrame 不允许调用 'pivot'；请提交到 OUTPUT_STREAMS，由系统绑定写入并在 Runtime 执行。"
        )


    def show(self, *args, **kwargs):
        raise AttributeError(
            "UniteStreamDataFrame 不允许调用 'show'；请提交到 OUTPUT_STREAMS，由系统绑定写入并在 Runtime 执行。"
        )


    def to_arrow(self, *args, **kwargs):
        raise AttributeError(
            "UniteStreamDataFrame 不允许调用 'to_arrow'；请提交到 OUTPUT_STREAMS，由系统绑定写入并在 Runtime 执行。"
        )


    def to_arrow_iter(self, *args, **kwargs):
        raise AttributeError(
            "UniteStreamDataFrame 不允许调用 'to_arrow_iter'；请提交到 OUTPUT_STREAMS，由系统绑定写入并在 Runtime 执行。"
        )


    def to_dask_dataframe(self, *args, **kwargs):
        raise AttributeError(
            "UniteStreamDataFrame 不允许调用 'to_dask_dataframe'；请提交到 OUTPUT_STREAMS，由系统绑定写入并在 Runtime 执行。"
        )


    def to_pandas(self, *args, **kwargs):
        raise AttributeError(
            "UniteStreamDataFrame 不允许调用 'to_pandas'；请提交到 OUTPUT_STREAMS，由系统绑定写入并在 Runtime 执行。"
        )


    def to_plan_bytes(self, *args, **kwargs):
        raise AttributeError(
            "UniteStreamDataFrame 不允许调用 'to_plan_bytes'；请提交到 OUTPUT_STREAMS，由系统绑定写入并在 Runtime 执行。"
        )


    def to_pydict(self, *args, **kwargs):
        raise AttributeError(
            "UniteStreamDataFrame 不允许调用 'to_pydict'；请提交到 OUTPUT_STREAMS，由系统绑定写入并在 Runtime 执行。"
        )


    def to_pylist(self, *args, **kwargs):
        raise AttributeError(
            "UniteStreamDataFrame 不允许调用 'to_pylist'；请提交到 OUTPUT_STREAMS，由系统绑定写入并在 Runtime 执行。"
        )


    def to_ray_dataset(self, *args, **kwargs):
        raise AttributeError(
            "UniteStreamDataFrame 不允许调用 'to_ray_dataset'；请提交到 OUTPUT_STREAMS，由系统绑定写入并在 Runtime 执行。"
        )


    def to_torch_iter_dataset(self, *args, **kwargs):
        raise AttributeError(
            "UniteStreamDataFrame 不允许调用 'to_torch_iter_dataset'；请提交到 OUTPUT_STREAMS，由系统绑定写入并在 Runtime 执行。"
        )


    def to_torch_map_dataset(self, *args, **kwargs):
        raise AttributeError(
            "UniteStreamDataFrame 不允许调用 'to_torch_map_dataset'；请提交到 OUTPUT_STREAMS，由系统绑定写入并在 Runtime 执行。"
        )


    def write_bigtable(self, *args, **kwargs):
        raise AttributeError(
            "UniteStreamDataFrame 不允许调用 'write_bigtable'；请提交到 OUTPUT_STREAMS，由系统绑定写入并在 Runtime 执行。"
        )


    def write_clickhouse(self, *args, **kwargs):
        raise AttributeError(
            "UniteStreamDataFrame 不允许调用 'write_clickhouse'；请提交到 OUTPUT_STREAMS，由系统绑定写入并在 Runtime 执行。"
        )


    def write_csv(self, *args, **kwargs):
        raise AttributeError(
            "UniteStreamDataFrame 不允许调用 'write_csv'；请提交到 OUTPUT_STREAMS，由系统绑定写入并在 Runtime 执行。"
        )


    def write_deltalake(self, *args, **kwargs):
        raise AttributeError(
            "UniteStreamDataFrame 不允许调用 'write_deltalake'；请提交到 OUTPUT_STREAMS，由系统绑定写入并在 Runtime 执行。"
        )


    def write_huggingface(self, *args, **kwargs):
        raise AttributeError(
            "UniteStreamDataFrame 不允许调用 'write_huggingface'；请提交到 OUTPUT_STREAMS，由系统绑定写入并在 Runtime 执行。"
        )


    def write_iceberg(self, *args, **kwargs):
        raise AttributeError(
            "UniteStreamDataFrame 不允许调用 'write_iceberg'；请提交到 OUTPUT_STREAMS，由系统绑定写入并在 Runtime 执行。"
        )


    def write_json(self, *args, **kwargs):
        raise AttributeError(
            "UniteStreamDataFrame 不允许调用 'write_json'；请提交到 OUTPUT_STREAMS，由系统绑定写入并在 Runtime 执行。"
        )


    def write_lance(self, *args, **kwargs):
        raise AttributeError(
            "UniteStreamDataFrame 不允许调用 'write_lance'；请提交到 OUTPUT_STREAMS，由系统绑定写入并在 Runtime 执行。"
        )


    def write_paimon(self, *args, **kwargs):
        raise AttributeError(
            "UniteStreamDataFrame 不允许调用 'write_paimon'；请提交到 OUTPUT_STREAMS，由系统绑定写入并在 Runtime 执行。"
        )


    def write_parquet(self, *args, **kwargs):
        raise AttributeError(
            "UniteStreamDataFrame 不允许调用 'write_parquet'；请提交到 OUTPUT_STREAMS，由系统绑定写入并在 Runtime 执行。"
        )


    def write_sink(self, *args, **kwargs):
        raise AttributeError(
            "UniteStreamDataFrame 不允许调用 'write_sink'；请提交到 OUTPUT_STREAMS，由系统绑定写入并在 Runtime 执行。"
        )


    def write_sql(self, *args, **kwargs):
        raise AttributeError(
            "UniteStreamDataFrame 不允许调用 'write_sql'；请提交到 OUTPUT_STREAMS，由系统绑定写入并在 Runtime 执行。"
        )


    def write_table(self, *args, **kwargs):
        raise AttributeError(
            "UniteStreamDataFrame 不允许调用 'write_table'；请提交到 OUTPUT_STREAMS，由系统绑定写入并在 Runtime 执行。"
        )


    def write_turbopuffer(self, *args, **kwargs):
        raise AttributeError(
            "UniteStreamDataFrame 不允许调用 'write_turbopuffer'；请提交到 OUTPUT_STREAMS，由系统绑定写入并在 Runtime 执行。"
        )


    def groupby(self, *args, **kwargs) -> "UniteStreamGroupedDataFrame":
        from unite_stream.grouped import UniteStreamGroupedDataFrame

        u_args, u_kwargs = unwrap_call_args(getattr(self._df, "groupby"), args, kwargs)
        return UniteStreamGroupedDataFrame(getattr(self._df, "groupby")(*u_args, **u_kwargs))


    def agg(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._df, "agg"), args, kwargs)
        return wrap_result(getattr(self._df, "agg")(*u_args, **u_kwargs))


    def agg_concat(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._df, "agg_concat"), args, kwargs)
        return wrap_result(getattr(self._df, "agg_concat")(*u_args, **u_kwargs))


    def agg_list(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._df, "agg_list"), args, kwargs)
        return wrap_result(getattr(self._df, "agg_list")(*u_args, **u_kwargs))


    def agg_set(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._df, "agg_set"), args, kwargs)
        return wrap_result(getattr(self._df, "agg_set")(*u_args, **u_kwargs))


    def any_value(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._df, "any_value"), args, kwargs)
        return wrap_result(getattr(self._df, "any_value")(*u_args, **u_kwargs))


    def concat(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._df, "concat"), args, kwargs)
        return wrap_result(getattr(self._df, "concat")(*u_args, **u_kwargs))


    def count(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._df, "count"), args, kwargs)
        return wrap_result(getattr(self._df, "count")(*u_args, **u_kwargs))


    def describe(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._df, "describe"), args, kwargs)
        return wrap_result(getattr(self._df, "describe")(*u_args, **u_kwargs))


    def distinct(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._df, "distinct"), args, kwargs)
        return wrap_result(getattr(self._df, "distinct")(*u_args, **u_kwargs))


    def drop_duplicates(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._df, "drop_duplicates"), args, kwargs)
        return wrap_result(getattr(self._df, "drop_duplicates")(*u_args, **u_kwargs))


    def drop_nan(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._df, "drop_nan"), args, kwargs)
        return wrap_result(getattr(self._df, "drop_nan")(*u_args, **u_kwargs))


    def drop_null(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._df, "drop_null"), args, kwargs)
        return wrap_result(getattr(self._df, "drop_null")(*u_args, **u_kwargs))


    def except_all(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._df, "except_all"), args, kwargs)
        return wrap_result(getattr(self._df, "except_all")(*u_args, **u_kwargs))


    def except_distinct(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._df, "except_distinct"), args, kwargs)
        return wrap_result(getattr(self._df, "except_distinct")(*u_args, **u_kwargs))


    def exclude(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._df, "exclude"), args, kwargs)
        return wrap_result(getattr(self._df, "exclude")(*u_args, **u_kwargs))


    def explode(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._df, "explode"), args, kwargs)
        return wrap_result(getattr(self._df, "explode")(*u_args, **u_kwargs))


    def filter(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._df, "filter"), args, kwargs)
        return wrap_result(getattr(self._df, "filter")(*u_args, **u_kwargs))


    def intersect(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._df, "intersect"), args, kwargs)
        return wrap_result(getattr(self._df, "intersect")(*u_args, **u_kwargs))


    def intersect_all(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._df, "intersect_all"), args, kwargs)
        return wrap_result(getattr(self._df, "intersect_all")(*u_args, **u_kwargs))


    def into_batches(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._df, "into_batches"), args, kwargs)
        return wrap_result(getattr(self._df, "into_batches")(*u_args, **u_kwargs))


    def into_partitions(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._df, "into_partitions"), args, kwargs)
        return wrap_result(getattr(self._df, "into_partitions")(*u_args, **u_kwargs))


    def join(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._df, "join"), args, kwargs)
        return wrap_result(getattr(self._df, "join")(*u_args, **u_kwargs))


    def limit(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._df, "limit"), args, kwargs)
        return wrap_result(getattr(self._df, "limit")(*u_args, **u_kwargs))


    def max(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._df, "max"), args, kwargs)
        return wrap_result(getattr(self._df, "max")(*u_args, **u_kwargs))


    def mean(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._df, "mean"), args, kwargs)
        return wrap_result(getattr(self._df, "mean")(*u_args, **u_kwargs))


    def melt(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._df, "melt"), args, kwargs)
        return wrap_result(getattr(self._df, "melt")(*u_args, **u_kwargs))


    def min(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._df, "min"), args, kwargs)
        return wrap_result(getattr(self._df, "min")(*u_args, **u_kwargs))


    def offset(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._df, "offset"), args, kwargs)
        return wrap_result(getattr(self._df, "offset")(*u_args, **u_kwargs))


    def pipe(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._df, "pipe"), args, kwargs)
        return wrap_result(getattr(self._df, "pipe")(*u_args, **u_kwargs))


    def repartition(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._df, "repartition"), args, kwargs)
        return wrap_result(getattr(self._df, "repartition")(*u_args, **u_kwargs))


    def sample(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._df, "sample"), args, kwargs)
        return wrap_result(getattr(self._df, "sample")(*u_args, **u_kwargs))


    def select(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._df, "select"), args, kwargs)
        return wrap_result(getattr(self._df, "select")(*u_args, **u_kwargs))


    def shuffle(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._df, "shuffle"), args, kwargs)
        return wrap_result(getattr(self._df, "shuffle")(*u_args, **u_kwargs))


    def skew(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._df, "skew"), args, kwargs)
        return wrap_result(getattr(self._df, "skew")(*u_args, **u_kwargs))


    def skip_existing(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._df, "skip_existing"), args, kwargs)
        return wrap_result(getattr(self._df, "skip_existing")(*u_args, **u_kwargs))


    def sort(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._df, "sort"), args, kwargs)
        return wrap_result(getattr(self._df, "sort")(*u_args, **u_kwargs))


    def stddev(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._df, "stddev"), args, kwargs)
        return wrap_result(getattr(self._df, "stddev")(*u_args, **u_kwargs))


    def sum(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._df, "sum"), args, kwargs)
        return wrap_result(getattr(self._df, "sum")(*u_args, **u_kwargs))


    def summarize(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._df, "summarize"), args, kwargs)
        return wrap_result(getattr(self._df, "summarize")(*u_args, **u_kwargs))


    def transform(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._df, "transform"), args, kwargs)
        return wrap_result(getattr(self._df, "transform")(*u_args, **u_kwargs))


    def union(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._df, "union"), args, kwargs)
        return wrap_result(getattr(self._df, "union")(*u_args, **u_kwargs))


    def union_all(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._df, "union_all"), args, kwargs)
        return wrap_result(getattr(self._df, "union_all")(*u_args, **u_kwargs))


    def union_all_by_name(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._df, "union_all_by_name"), args, kwargs)
        return wrap_result(getattr(self._df, "union_all_by_name")(*u_args, **u_kwargs))


    def union_by_name(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._df, "union_by_name"), args, kwargs)
        return wrap_result(getattr(self._df, "union_by_name")(*u_args, **u_kwargs))


    def unique(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._df, "unique"), args, kwargs)
        return wrap_result(getattr(self._df, "unique")(*u_args, **u_kwargs))


    def unpivot(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._df, "unpivot"), args, kwargs)
        return wrap_result(getattr(self._df, "unpivot")(*u_args, **u_kwargs))


    def var(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._df, "var"), args, kwargs)
        return wrap_result(getattr(self._df, "var")(*u_args, **u_kwargs))


    def where(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._df, "where"), args, kwargs)
        return wrap_result(getattr(self._df, "where")(*u_args, **u_kwargs))


    def with_column(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._df, "with_column"), args, kwargs)
        return wrap_result(getattr(self._df, "with_column")(*u_args, **u_kwargs))


    def with_column_renamed(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._df, "with_column_renamed"), args, kwargs)
        return wrap_result(getattr(self._df, "with_column_renamed")(*u_args, **u_kwargs))


    def with_columns(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._df, "with_columns"), args, kwargs)
        return wrap_result(getattr(self._df, "with_columns")(*u_args, **u_kwargs))


    def with_columns_renamed(self, *args, **kwargs) -> "UniteStreamDataFrame":
        u_args, u_kwargs = unwrap_call_args(getattr(self._df, "with_columns_renamed"), args, kwargs)
        return wrap_result(getattr(self._df, "with_columns_renamed")(*u_args, **u_kwargs))


    @property
    def column_names(self) -> list[str]:
        return self._df.column_names

    @property
    def columns(self) -> list[Expression]:
        return self._df.columns

    def schema(self) -> Schema:
        return self._df.schema()

    def __getitem__(self, item: int | str | slice | Iterable[str | int]) -> Union[Expression, UniteStreamDataFrame]:
        result = self._df[item]
        return wrap_result(result) if isinstance(result, DataFrame) else result

    def __repr__(self) -> str:
        return "<UniteStreamDataFrame>"
