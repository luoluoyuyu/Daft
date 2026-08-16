"""Pytest suite for Daft skip_existing permissive missing-path behavior.

The key-filtering (Ray actor-backed) execution paths were removed together with
the Ray runner; only the permissive missing-path handling, which is pure
client-side logic, remains covered here.
"""

from __future__ import annotations

import daft


def test_skip_existing_missing_path_processes_all_rows(tmp_path):
    """A missing path returns all rows without building a key filter."""
    ckpt_dir = tmp_path / "nonexistent_path"
    df = daft.from_pydict({"id": [1, 2, 3], "val": ["a", "b", "c"]})

    result = df.skip_existing(existing_path=ckpt_dir, key_column="id", file_format="parquet").collect()
    assert result.select("id").to_pydict()["id"] == [1, 2, 3]


def test_skip_existing_missing_path_returns_self(tmp_path):
    """A missing path returns the original DataFrame without building a join plan."""
    ckpt_dir = tmp_path / "nonexistent_path"
    df = daft.from_pydict({"id": [1, 2, 3], "val": ["a", "b", "c"]})

    result_df = df.skip_existing(existing_path=ckpt_dir, key_column="id", file_format="parquet")
    # No join node should be appended -- should be the exact same DataFrame object
    assert result_df is df
