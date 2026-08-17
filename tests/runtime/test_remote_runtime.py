from __future__ import annotations

import os
import shutil
import socket
import subprocess
import tempfile
import time
from pathlib import Path

import pytest

import daft
from daft import col
from daft.plan_transport import serialize_plan_parts
from daft.runtime.client import RuntimeClient


REPO_ROOT = Path(__file__).resolve().parents[2]
RUNTIME_BINARY = REPO_ROOT / "target" / "debug" / "daft-runtime"


@pytest.fixture(autouse=True)
def _reset_remote_runner():
    from daft.runners import _reset_remote_runner_for_testing

    _reset_remote_runner_for_testing()
    yield
    _reset_remote_runner_for_testing()


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _wait_for_port(port: int, timeout_s: float = 15.0) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.1)
    raise TimeoutError(f"daft-runtime did not start listening on port {port}")


def _wait_for_registered(proc: subprocess.Popen, timeout_s: float = 15.0) -> None:
    """Wait until the executor has registered with the scheduler."""
    deadline = time.monotonic() + timeout_s
    collected: list[str] = []
    while time.monotonic() < deadline:
        assert proc.stdout is not None
        line = proc.stdout.readline()
        if not line:
            time.sleep(0.05)
            continue
        collected.append(line)
        if "registered with scheduler" in line:
            return
    raise TimeoutError(
        f"executor did not register with the scheduler; output:\n{''.join(collected)}"
    )


@pytest.fixture
def rust_runtime():
    if not RUNTIME_BINARY.is_file():
        pytest.skip("daft-runtime binary not built; run `cargo build -p daft-runtime`")
    port = _free_port()
    scheduler = subprocess.Popen(
        [str(RUNTIME_BINARY)],
        env={
            **os.environ,
            "DAFT_RUNTIME_ADDRESS": f"127.0.0.1:{port}",
            "DAFT_RUNTIME_TOKEN": "test-secret",
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        _wait_for_port(port)
        executor = subprocess.Popen(
            [str(RUNTIME_BINARY), "--executor"],
            env={
                **os.environ,
                "DAFT_EXECUTOR": "1",
                "DAFT_SCHEDULER_ADDRESS": f"http://127.0.0.1:{port}",
                "DAFT_FLIGHT_IP": "127.0.0.1",
                "DAFT_RUNTIME_TOKEN": "test-secret",
            },
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        try:
            _wait_for_registered(executor)
            yield f"http://127.0.0.1:{port}", "test-secret"
        finally:
            executor.terminate()
            try:
                executor.wait(timeout=5)
            except subprocess.TimeoutExpired:
                executor.kill()
                executor.wait(timeout=5)
    finally:
        scheduler.terminate()
        try:
            scheduler.wait(timeout=5)
        except subprocess.TimeoutExpired:
            scheduler.kill()
            scheduler.wait(timeout=5)


def _udf_triple() -> daft.udf.Udf:
    @daft.func
    def triple(x: int) -> int:
        return x * 3

    return triple


_ARTIFACT_HELPER_SRC = (
    "def multiply(x, factor):\n"
    "    return x * factor\n"
)


def _udf_via_artifact() -> daft.udf.Udf:
    @daft.func
    def via_artifact(x: int) -> int:
        import helper_mod

        return helper_mod.multiply(x, 7)

    return via_artifact


def _upload_helper_artifact(client: RuntimeClient) -> str:
    return client.upload_udf_artifact(
        _ARTIFACT_HELPER_SRC.encode(),
        entrypoint="helper_mod:multiply",
        filename="helper_mod.py",
    )


def test_rust_runtime_native_path_does_not_need_python(rust_runtime) -> None:
    endpoint, token = rust_runtime
    client = RuntimeClient(endpoint, token=token)

    import pyarrow as pa
    import pyarrow.parquet as papq

    parquet_path = Path(__file__).parent / "data.parquet"
    parquet_path.parent.mkdir(exist_ok=True)
    papq.write_table(
        pa.table(
            {
                "id": [1, 2, 3],
                "name": ["a", "b", "c"],
            }
        ),
        parquet_path,
    )

    # In-memory source + filter/projection/select (no UDF).
    df = daft.from_pydict({"id": [1, 2, 3], "name": ["a", "b", "c"]}).filter(
        col("id") > 1
    )
    plan, execution, partition_sets = serialize_plan_parts(df)
    assert execution.requires_udf is False
    result = client.submit(
        plan, execution=execution, partition_sets=partition_sets
    ).result()
    assert result[0].to_pydict() == {"id": [2, 3], "name": ["b", "c"]}

    # File scan + join against an in-memory table (no UDF).
    left = daft.read_parquet(str(parquet_path))
    right = daft.from_pydict({"id": [1, 2, 3], "score": [0.5, 0.6, 0.7]})
    joined = left.join(right, on="id")
    plan2, execution2, partition_sets2 = serialize_plan_parts(joined)
    assert execution2.requires_udf is False
    result2 = client.submit(
        plan2, execution=execution2, partition_sets=partition_sets2
    ).result()
    assert result2[0].to_pydict() == {
        "id": [1, 2, 3],
        "name": ["a", "b", "c"],
        "score": [0.5, 0.6, 0.7],
    }


def test_rust_runtime_streams_multistage_results(rust_runtime) -> None:
    """A repartition barrier feeds final partitions through the result stream."""
    endpoint, token = rust_runtime
    client = RuntimeClient(endpoint, token=token, timeout_s=10.0)

    df = daft.from_pydict({"id": list(range(12))}).into_partitions(3)
    plan, execution, partition_sets = serialize_plan_parts(df)
    job = client.submit(plan, execution=execution, partition_sets=partition_sets)

    try:
        partitions = list(job.iter_result_partitions())
    except TimeoutError:
        pytest.fail(f"result stream timed out with job status: {job.status()}")
    values = sorted(value for partition in partitions for value in partition.to_pydict()["id"])
    assert values == list(range(12))
    assert len(partitions) == 3


def test_rust_runtime_sql_execution(rust_runtime) -> None:
    """SQL is parsed and planned on the server; the client only ships the text."""
    endpoint, token = rust_runtime
    from daft.runners import set_runner_remote

    set_runner_remote(endpoint, token=token)

    df = daft.from_pydict({"id": [1, 2, 3], "name": ["a", "b", "c"]})
    result = daft.sql("SELECT id, name FROM df WHERE id > 1", df=df).to_pydict()
    assert result == {"id": [2, 3], "name": ["b", "c"]}

    # Joins between bound DataFrames work too: the SQL planner resolves both
    # table names server-side.
    left = daft.from_pydict({"id": [1, 2, 3], "name": ["a", "b", "c"]})
    right = daft.from_pydict({"id": [1, 2, 3], "score": [0.5, 0.6, 0.7]})
    joined = daft.sql(
        "SELECT left.id, left.name, right.score FROM left JOIN right ON left.id = right.id",
        left=left,
        right=right,
    ).to_pydict()
    assert joined == {
        "id": [1, 2, 3],
        "name": ["a", "b", "c"],
        "score": [0.5, 0.6, 0.7],
    }


def test_rust_runtime_udf_goes_through_python_worker(rust_runtime) -> None:
    endpoint, token = rust_runtime
    client = RuntimeClient(endpoint, token=token)

    df = daft.from_pydict({"x": [1, 2, 3]}).with_column("t", _udf_triple()(col("x")))
    plan, execution, partition_sets = serialize_plan_parts(df)
    assert execution.requires_udf is True
    result = client.submit(
        plan, execution=execution, partition_sets=partition_sets
    ).result()
    assert result[0].to_pydict() == {"x": [1, 2, 3], "t": [3, 6, 9]}

    # An in-memory join where the UDF output is aggregated (UDF + aggregation).
    triple = _udf_triple()
    grouped = (
        daft.from_pydict({"k": ["a", "a", "b"], "v": [1, 2, 3]})
        .with_column("t", triple(col("v")))
        .groupby("k")
        .agg(daft.col("t").sum().alias("total"))
    )
    plan2, execution2, partition_sets2 = serialize_plan_parts(grouped)
    assert execution2.requires_udf is True
    result2 = client.submit(
        plan2, execution=execution2, partition_sets=partition_sets2
    ).result()
    pydict = result2[0].to_pydict()
    rows = sorted(zip(pydict["k"], pydict["total"]))
    assert rows == [("a", 9), ("b", 9)]


def test_rust_runtime_routes_on_requires_udf_flag(rust_runtime) -> None:
    endpoint, token = rust_runtime
    client = RuntimeClient(endpoint, token=token)

    # Run scheduler + executor from a directory outside the repo with no
    # Python available (DAFT_RUNTIME_PYTHON points at a missing interpreter
    # and PATH has none). The pure-Rust path must still execute on the
    # executor; the UDF path must fail because the executor cannot resolve a
    # Python worker.
    with tempfile.TemporaryDirectory() as tmp:
        isolated_bin = Path(tmp) / "daft-runtime"
        shutil.copy2(RUNTIME_BINARY, isolated_bin)
        port = _free_port()
        isolated_env = {
            "DAFT_RUNTIME_PYTHON": "/nonexistent/python",
            "PATH": tmp,
            "HOME": tmp,
        }
        scheduler = subprocess.Popen(
            [str(isolated_bin)],
            cwd=tmp,
            env={
                **isolated_env,
                "DAFT_RUNTIME_ADDRESS": f"127.0.0.1:{port}",
            },
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        try:
            _wait_for_port(port)
            executor = subprocess.Popen(
                [str(isolated_bin), "--executor"],
                cwd=tmp,
                env={
                    **isolated_env,
                    "DAFT_EXECUTOR": "1",
                    "DAFT_SCHEDULER_ADDRESS": f"http://127.0.0.1:{port}",
                    "DAFT_FLIGHT_IP": "127.0.0.1",
                },
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
            try:
                _wait_for_registered(executor)
                isolated_client = RuntimeClient(f"http://127.0.0.1:{port}")

                # Pure-Rust execution must not require Python at all.
                df = daft.from_pydict({"id": [1, 2, 3]}).filter(col("id") > 1)
                plan, execution, partition_sets = serialize_plan_parts(df)
                assert execution.requires_udf is False
                result = isolated_client.submit(
                    plan, execution=execution, partition_sets=partition_sets
                ).result()
                assert result[0].to_pydict() == {"id": [2, 3]}

                # A plan flagged as requiring a Python worker must fail to
                # resolve the interpreter on the executor.
                udf_df = daft.from_pydict({"x": [1, 2, 3]}).with_column(
                    "t", _udf_triple()(col("x"))
                )
                plan2, execution2, partition_sets2 = serialize_plan_parts(udf_df)
                assert execution2.requires_udf is True
                job = isolated_client.submit(
                    plan2, execution=execution2, partition_sets=partition_sets2
                )
                with pytest.raises(Exception, match="no Python interpreter found"):
                    job.result()
            finally:
                executor.terminate()
                try:
                    executor.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    executor.kill()
                    executor.wait(timeout=5)
        finally:
            scheduler.terminate()
            try:
                scheduler.wait(timeout=5)
            except subprocess.TimeoutExpired:
                scheduler.kill()
                scheduler.wait(timeout=5)


def test_rust_runtime_udf_artifact_endpoints(rust_runtime) -> None:
    """UDF artifact endpoints against the Rust binary (protobuf, no JSON)."""
    endpoint, token = rust_runtime
    client = RuntimeClient(endpoint, token=token)

    artifact_id = client.upload_udf_artifact(
        b"rust-artifact",
        entrypoint="module:function",
        python_version="3.11",
        requirements=["pandas"],
    )
    assert artifact_id.startswith("sha256:")

    metadata = client.udf_artifact_metadata(artifact_id)
    assert metadata.entrypoint == "module:function"
    assert metadata.python_version == "3.11"
    assert metadata.size_bytes == len(b"rust-artifact")
    assert list(metadata.requirements) == ["pandas"]
    assert metadata.sha256 == artifact_id.removeprefix("sha256:")

    assert client.download_udf_artifact(artifact_id) == b"rust-artifact"

    with pytest.raises(Exception, match="unknown artifact"):
        client.udf_artifact_metadata("sha256:deadbeef")
    with pytest.raises(Exception, match="unknown job"):
        client.status("00000000-0000-0000-0000-000000000000")
    with pytest.raises(Exception, match="unknown job"):
        client.cancel("00000000-0000-0000-0000-000000000000")


def test_rust_runtime_udf_artifact_referenced_in_plan(rust_runtime) -> None:
    """Full chain on the Rust binary: upload -> materialize -> worker import."""
    endpoint, token = rust_runtime
    client = RuntimeClient(endpoint, token=token)
    artifact_id = _upload_helper_artifact(client)

    df = daft.from_pydict({"x": [1, 2, 3]}).with_column(
        "y", _udf_via_artifact()(col("x"))
    )
    plan, execution, partition_sets = serialize_plan_parts(df)
    assert execution.requires_udf is True
    job = client.submit(
        plan,
        execution=execution,
        partition_sets=partition_sets,
        udf_artifact_ids=[artifact_id],
    )
    assert job.result()[0].to_pydict() == {"x": [1, 2, 3], "y": [7, 14, 21]}

    with pytest.raises(Exception, match="unknown UDF artifact"):
        client.submit(
            plan,
            execution=execution,
            partition_sets=partition_sets,
            udf_artifact_ids=["sha256:deadbeef"],
        )


def test_rust_runtime_remote_runner_end_to_end(rust_runtime) -> None:
    """High-level user path against the real Rust binary: connect -> collect."""
    endpoint, token = rust_runtime

    daft.connect(endpoint, token=token)
    df = (
        daft.from_pydict({"id": [1, 2, 3, 4], "name": ["a", "b", "c", "d"]})
        .filter(col("id") > 1)
        .select("id", "name")
    )
    assert df.collect(num_preview_rows=None).to_pydict() == {
        "id": [2, 3, 4],
        "name": ["b", "c", "d"],
    }

    # UDF plan through the high-level path: the remote runner must route it to
    # the Python worker and return the protobuf-wrapped JobResult.
    udf_df = daft.from_pydict({"x": [1, 2]}).with_column(
        "t", _udf_triple()(col("x"))
    )
    assert udf_df.collect(num_preview_rows=None).to_pydict() == {
        "x": [1, 2],
        "t": [3, 6],
    }
