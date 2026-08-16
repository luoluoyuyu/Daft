from __future__ import annotations

import pytest

import daft
from daft import DataType as dt

# TODO chore: make an asset fixture for all tests (beyond just sql).


@pytest.fixture
def sample_schema():
    return {"a": daft.DataType.float32(), "b": daft.DataType.string()}


def assert_eq(actual, expect):
    actual.to_pydict() == expect.to_pydict()


def to_sql_array(paths: list[str]) -> str:
    return "[ " + ", ".join([f"'{p}'" for p in paths]) + " ]"


def test_sql_read_parquet():
    actual = daft.sql("SELECT * FROM read_parquet('tests/assets/parquet-data/mvp.parquet')")
    expect = daft.read_parquet("tests/assets/parquet-data/mvp.parquet")
    assert_eq(actual, expect)


def test_sql_read_parquet_path():
    actual = daft.sql("SELECT * FROM 'tests/assets/parquet-data/mvp.parquet'")
    expect = daft.read_parquet("tests/assets/parquet-data/mvp.parquet")
    assert_eq(actual, expect)


def test_sql_read_parquet_paths():
    paths = [
        "tests/assets/parquet-data/mvp.parquet",
        "tests/assets/parquet-data/parquet-with-schema-metadata.parquet",
    ]
    actual = daft.sql(f"SELECT * FROM read_parquet({to_sql_array(paths)})")
    expect = daft.read_parquet(paths)
    assert_eq(actual, expect)


def test_sql_read_path_no_alias():
    # don't allow using paths as table names
    with pytest.raises(Exception, match="Table not found"):
        daft.sql(""" SELECT "tests/assets/parquet-data/mvp.parquet".* FROM 'tests/assets/parquet-data/mvp.parquet' """)
