from __future__ import annotations

import os
import shutil
import tempfile

import pyarrow as pa
import pyarrow.parquet as papq
import pytest

import daft
from daft import DataFrame


@pytest.fixture(scope="module", params=[(1, 64), (8, 8), (64, 1)], ids=["1x64mib", "8x8mib", "64x1mib"])
def gen_simple_parquets(request) -> str:
    """Creates some Parquet files in a directory. Returns the name of the directory."""
    num_files, mibs_per_file = request.param

    rows_per_mib = 1024 * 128
    num_rows_per_file = mibs_per_file * rows_per_mib

    with tempfile.TemporaryDirectory() as tmpdirname:
        # Make one Parquet file of the correct size.
        file_path = os.path.join(tmpdirname, "file.parquet")
        table = pa.table({"A": ["aaa"] * num_rows_per_file, "B": [1] * num_rows_per_file})
        papq.write_table(table, file_path)

        # Copy it to get the remaining number of desired files.
        for i in range(1, num_files):
            shutil.copyfile(
                src=os.path.join(tmpdirname, "file.parquet"),
                dst=os.path.join(tmpdirname, f"file{i}.parquet"),
            )

        yield tmpdirname, num_files * num_rows_per_file


@pytest.mark.benchmark(group="file_read")
def test_parquet_read(gen_simple_parquets, benchmark):
    parquet_dir, num_rows = gen_simple_parquets

    def bench() -> DataFrame:
        df = daft.read_parquet(parquet_dir)
        return df.collect()

    df = benchmark(bench)

    assert len(df) == num_rows


@pytest.mark.benchmark(group="file_read")
@pytest.mark.parametrize("prune", [True, False])
def test_s3_parquet_read_1x64mb(benchmark, prune):
    parquet_glob = "s3://daft-oss-public-data/test_fixtures/parquet/95c7fba0-265d-440b-88cb-2897047fc5f9-0.parquet"
    expected_rows = 1500000

    def bench() -> DataFrame:
        df = daft.read_parquet(parquet_glob)
        if prune:
            df = df.select(df["O_SHIPPRIORITY"])  # rightmost int64 column
        return df.collect()

    df = benchmark(bench)
    assert len(df.to_pandas()) == expected_rows


@pytest.mark.benchmark(group="file_read")
@pytest.mark.parametrize("prune", [True, False])
def test_s3_parquet_read_32x2mb(benchmark, prune):
    parquet_glob = "s3://daft-oss-public-data/test_fixtures/parquet_small/*"
    expected_rows = 2000000

    def bench() -> DataFrame:
        df = daft.read_parquet(parquet_glob)
        if prune:
            df = df.select(df["P_SIZE"])  # rightmost int64 column
        return df.collect()

    df = benchmark(bench)
    assert len(df.to_pandas()) == expected_rows
