from __future__ import annotations

import pytest

import daft
from daft import col
from daft.functions import format
from tests.utils import clean_explain_output, explain_to_text


@pytest.fixture(scope="session")
def small_df(tmp_path_factory):
    df = daft.range(start=0, end=1000, partitions=10)
    df = df.with_columns(
        {
            "s_name": format("user_{}", df["id"]),
            "s_email": format("user_{}@daft.ai", df["id"]),
        }
    )

    tmp_path = str(tmp_path_factory.mktemp("small"))
    df.write_parquet(tmp_path)
    return daft.read_parquet(tmp_path)


@pytest.fixture(scope="session")
def large_df(tmp_path_factory):
    df = daft.range(start=0, end=9999, partitions=100)
    df = df.with_columns(
        {
            "l_name": format("user_{}", df["id"]),
            "l_email": format("user_{}@daft.ai", df["id"]),
        }
    )

    tmp_path = str(tmp_path_factory.mktemp("large"))
    df.write_parquet(tmp_path)
    return daft.read_parquet(tmp_path)


def test_explain_with_cross_join(small_df, large_df):
    df = small_df.join(other=large_df, how="cross")
    expected = """
    * Cross Join
    |   Stream Side = Right
        """
    assert clean_explain_output(expected) in clean_explain_output(explain_to_text(df, only_physical_plan=True))

    df = large_df.join(other=small_df, how="cross")
    expected = """
    * Cross Join
    |   Stream Side = Left
        """
    assert clean_explain_output(expected) in clean_explain_output(explain_to_text(df, only_physical_plan=True))


@pytest.mark.parametrize(
    "write_fn,kwargs",
    [
        ("write_csv", {"write_mode": "overwrite"}),
        ("write_parquet", {"write_mode": "overwrite"}),
        ("write_json", {"write_mode": "overwrite"}),
    ],
)
def test_explain_after_write_preserves_upstream_plan(tmp_path, write_fn, kwargs):
    output_path = str(tmp_path / "written")
    write_df = getattr(daft.from_pydict({"a": [1, 2, 3]}).filter(col("a") > 1), write_fn)(output_path, **kwargs)
    explain_text = explain_to_text(write_df)
    assert "Result is cached and will skip computation" in explain_text
    assert "However here is the logical plan used to produce this result" in explain_text
    assert "Filter:" in explain_text


def test_explain_with_hash_join(small_df, large_df):
    df = small_df.join(other=large_df, left_on="s_name", right_on="l_name", strategy="hash", how="left")
    expected = """
    * Hash Join (Left):
    |   Build on left: true
        """
    assert clean_explain_output(expected) in clean_explain_output(explain_to_text(df, only_physical_plan=True))

    df = large_df.join(other=small_df, left_on="l_name", right_on="s_name", strategy="hash", how="right")
    expected = """
    * Hash Join (Right):
    |   Build on left: false
        """
    assert clean_explain_output(expected) in clean_explain_output(explain_to_text(df, only_physical_plan=True))


def test_explain_with_explode_index_column():
    df = daft.from_pydict({"nested": [[1, 2], [3, 4]]})
    df = df.explode("nested", index_column="idx")
    output = explain_to_text(df)
    assert "Explode" in output
    assert "Index column = idx" in output


def test_explain_when_join_with_download():
    df1 = daft.from_pydict(
        {
            "name": ["a", "b"],
            "url": ["https://www.daft.ai/"] * 2,
        }
    ).with_column("bytes", col("url").download())

    df2 = daft.from_pydict(
        {
            "name": ["a", "b"],
            "value": [1, 2],
        }
    )

    df = df1.join(df2, on="name", prefix="df2_")

    output = explain_to_text(df, only_physical_plan=True)
    assert "url_download" in output
    assert "Join" in output
