from __future__ import annotations

import daft
from daft import col


def test_serialize_deserialize_lazy_plan() -> None:
    source = daft.from_pydict({"id": [1, 1000, 2000], "x": ["a", "b", "c"]})
    plan = source.filter(col("id") > 500)

    plan_bytes = plan.to_plan_bytes()
    restored = daft.DataFrame.from_plan_bytes(plan_bytes)

    assert restored.collect(num_preview_rows=None).to_pydict() == plan.collect(num_preview_rows=None).to_pydict()


def test_execute_plan_roundtrip() -> None:
    df = daft.from_pydict({"n": [1, 2, 3]}).select((col("n") + 1).alias("m"))
    plan_bytes = daft.serialize_plan(df)
    result = daft.execute_plan(plan_bytes, use_ray=False, num_preview_rows=None)
    assert result.to_pydict() == {"m": [2, 3, 4]}
