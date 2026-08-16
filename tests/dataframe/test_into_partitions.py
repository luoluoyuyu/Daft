from __future__ import annotations

import pytest

import daft


def test_into_partitions_some_empty(make_df) -> None:
    data = {"foo": [1, 2, 3]}
    df = make_df(data).into_partitions(32).collect()
    partitions = list(df.iter_partitions())

    values = list(v.to_pydict() for v in partitions)
    assert values[0] == {"foo": [1]}
    assert values[1] == {"foo": [2]}
    assert values[2] == {"foo": [3]}
    for i in range(3, 32):
        assert values[i] == {"foo": []}


def test_into_partitions_split(make_df) -> None:
    data = {"foo": list(range(100))}
    parts = list(make_df(data).into_partitions(20).iter_partitions())
    assert len(parts) == 20
    values = set(v for p in parts for v in p.to_pydict()["foo"])
    assert values == set(range(100))


def test_into_partitions_coalesce() -> None:
    df = daft.range(100, partitions=10).into_partitions(2)
    parts = list(df.iter_partitions())
    assert len(parts) == 2
    values = set(v for p in parts for v in p.to_pydict()["id"])
    assert values == set(range(100))


def test_into_partitions_split_and_coalesce(make_df) -> None:
    data = {"foo": list(range(100))}
    df = make_df(data).into_partitions(20).into_partitions(1).collect()
    assert df.to_pydict() == data


def test_into_partitions_some_no_split(make_df) -> None:
    data = {"foo": [1, 2, 3]}

    # Materialize as 3 partitions
    df = make_df(data).into_partitions(3).collect()

    # Attempt to split into 4 partitions, so only 1 split occurs
    df = df.into_partitions(4).collect()

    assert df.to_pydict() == data
