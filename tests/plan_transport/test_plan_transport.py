from __future__ import annotations

import daft
from daft import Window, col
from daft.functions import rank
from daft.plan_transport import deserialize_plan_parts, serialize_plan_parts


def _roundtrip(df: daft.DataFrame) -> daft.DataFrame:
    """Serialize the plan proto and restore it via deserialize_plan_parts."""
    plan_bytes, execution, partition_sets = serialize_plan_parts(df)
    assert execution.requires_udf is False
    return deserialize_plan_parts(plan_bytes, partition_sets)


def test_serialize_deserialize_lazy_plan() -> None:
    source = daft.from_pydict({"id": [1, 1000, 2000], "x": ["a", "b", "c"]})
    plan = source.filter(col("id") > 500)

    restored = _roundtrip(plan)
    assert restored.collect(num_preview_rows=None).to_pydict() == plan.collect(num_preview_rows=None).to_pydict()


def test_serialize_deserialize_expression_roundtrip() -> None:
    df = daft.from_pydict({"n": [1, 2, 3]}).select((col("n") + 1).alias("m"))
    result = _roundtrip(df).collect(num_preview_rows=None)
    assert result.to_pydict() == {"m": [2, 3, 4]}


def test_serialize_join_union_window_agg_roundtrip() -> None:
    left = daft.from_pydict({"k": [1, 2, 3], "a": ["x", "y", "z"]})
    right = daft.from_pydict({"k": [2, 3, 4], "b": ["p", "q", "r"]})
    joined = _roundtrip(left.join(right, on="k"))
    assert sorted(joined.collect(num_preview_rows=None).to_pydict()["k"]) == [2, 3]

    unioned = _roundtrip(
        daft.from_pydict({"n": [1, 2]}).union(daft.from_pydict({"n": [3, 4]}))
    )
    assert sorted(unioned.collect(num_preview_rows=None).to_pydict()["n"]) == [1, 2, 3, 4]

    windowed = _roundtrip(
        daft.from_pydict({"g": ["a", "a", "b"], "v": [1, 2, 3]}).with_column(
            "rank", rank().over(Window().partition_by("g").order_by("v"))
        )
    )
    assert sorted(windowed.collect(num_preview_rows=None).to_pydict()["rank"]) == [1, 1, 2]

    aggregated = _roundtrip(
        daft.from_pydict({"g": ["a", "a", "b"], "v": [1, 2, 3]})
        .groupby("g")
        .agg(col("v").sum())
    )
    pydict = aggregated.collect(num_preview_rows=None).to_pydict()
    assert dict(zip(pydict["g"], pydict["v"])) == {"a": 3, "b": 3}
