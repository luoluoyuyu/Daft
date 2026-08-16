from __future__ import annotations

import contextlib
import io
import os
import subprocess
import sys

import pytest

import daft


@contextlib.contextmanager
def with_null_env():
    old_daft_runner = os.getenv("DAFT_RUNNER")
    del os.environ["DAFT_RUNNER"]

    try:
        yield
    finally:
        if old_daft_runner is not None:
            os.environ["DAFT_RUNNER"] = old_daft_runner


def test_explicit_set_runner_native():
    """Test that a freshly imported context doesn't have a runner config set and can be set explicitly to Native."""
    explicit_set_runner_script_native = """
import daft
print(daft.runners._get_runner())
daft.set_runner_native()
print(daft.runners._get_runner().name)
    """

    with with_null_env():
        result = subprocess.run(
            [sys.executable, "-c", explicit_set_runner_script_native],
            capture_output=True,
        )
        assert result.stdout.decode().strip() == "None\nnative"


def test_implicit_set_runner_native():
    """Test that a freshly imported context doesn't have a runner config set and is set implicitly to Native."""
    implicit_set_runner_script = """
import daft
print(daft.runners._get_runner())
df = daft.from_pydict({"foo": [1, 2, 3]})
print(daft.runners._get_runner().name)
    """

    with with_null_env():
        # Use a clean env without RAY_* vars so Ray auto-detection doesn't trigger
        clean_env = {k: v for k, v in os.environ.items() if not k.startswith("RAY_")}
        result = subprocess.run([sys.executable, "-c", implicit_set_runner_script], capture_output=True, env=clean_env)
        assert result.stdout.decode().strip() == "None\nnative"


@pytest.mark.parametrize("daft_runner_envvar", ["native"])
def test_env_var(daft_runner_envvar):
    """Test that environment variables are correctly picked up."""
    autodetect_script = """
import daft
df = daft.from_pydict({"foo": [1, 2, 3]})
print(daft.runners._get_runner().name)
    """

    with with_null_env():
        result = subprocess.run(
            [sys.executable, "-c", autodetect_script],
            capture_output=True,
            env={"DAFT_RUNNER": daft_runner_envvar},
        )
        assert result.stdout.decode().strip() == daft_runner_envvar


def test_get_or_create_runner_from_multiple_threads():
    concurrent_get_or_create_runner_script = """
import concurrent.futures
from daft.runners import get_or_create_runner

with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
    futures = [
        executor.submit(lambda: get_or_create_runner()) for _ in range(10)
    ]

    results = [future.result() for future in concurrent.futures.as_completed(futures)]
    print("ok")
    """

    with with_null_env():
        result = subprocess.run(
            [sys.executable, "-c", concurrent_get_or_create_runner_script],
            capture_output=True,
        )
        assert result.stdout.decode().strip() == "ok"


@pytest.mark.parametrize("daft_runner_envvar", ["native"])
def test_get_or_infer_runner_type_from_env(daft_runner_envvar):
    get_or_infer_runner_type_py_script = """
import daft

print(daft.runners.get_or_infer_runner_type())

@daft.func.batch(return_dtype=daft.DataType.string())
def my_udf(foo):
    runner_type = daft.runners.get_or_infer_runner_type()
    return [f"{runner_type}_{f}" for f in foo]

df = daft.from_pydict({"foo": [7]})
pd = df.with_column(column_name="bar", expr=my_udf(df["foo"])).to_pydict()
print(pd["bar"][0])
    """

    with with_null_env():
        result = subprocess.run(
            [sys.executable, "-c", get_or_infer_runner_type_py_script],
            capture_output=True,
            env={"DAFT_RUNNER": daft_runner_envvar},
        )

        assert result.stdout.decode().strip() == f"{daft_runner_envvar}\n{daft_runner_envvar}_7"


def test_get_or_infer_runner_type_with_set_runner_native():
    get_or_infer_runner_type_py_script = """
import daft

daft.set_runner_native()

print(daft.runners.get_or_infer_runner_type())

@daft.func.batch(return_dtype=daft.DataType.string())
def my_udf(foo):
    runner_type = daft.runners.get_or_infer_runner_type()
    return [f"{runner_type}_{f}" for f in foo]

df = daft.from_pydict({"foo": [7]})
pd = df.with_column(column_name="bar", expr=my_udf(df["foo"])).to_pydict()
print(pd["bar"][0])
    """

    with with_null_env():
        result = subprocess.run([sys.executable, "-c", get_or_infer_runner_type_py_script], capture_output=True)
        assert result.stdout.decode().strip() == "native\nnative_7"


def test_use_default_scantask_max_parallelism():
    with with_null_env():
        str_io = io.StringIO()
        df = daft.range(start=0, end=1024, partitions=10)
        df.explain(show_all=True, file=str_io)
        assert "Num Parallel Scan Tasks = 8" in str_io.getvalue().strip()


def test_set_scantask_max_parallelism_less_than_partition_num():
    with daft.execution_config_ctx(scantask_max_parallel=7):
        str_io = io.StringIO()
        df = daft.range(start=0, end=1024, partitions=10)
        df.explain(show_all=True, file=str_io)
        assert "Num Parallel Scan Tasks = 7" in str_io.getvalue().strip()


def test_set_scantask_max_parallelism_greater_than_partition_num():
    with daft.execution_config_ctx(scantask_max_parallel=17):
        str_io = io.StringIO()
        df = daft.range(start=0, end=1024, partitions=10)
        df.explain(show_all=True, file=str_io)
        assert "Num Parallel Scan Tasks = 17" in str_io.getvalue().strip()


def test_set_worker_startup_timeout():
    original_timeout = daft.context.get_context().daft_execution_config.worker_startup_timeout

    with daft.execution_config_ctx(worker_startup_timeout=321):
        assert daft.context.get_context().daft_execution_config.worker_startup_timeout == 321

    assert daft.context.get_context().daft_execution_config.worker_startup_timeout == original_timeout
