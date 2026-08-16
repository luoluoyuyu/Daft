from __future__ import annotations

import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import cloudpickle
import daft
import pytest
import unite_stream
from unite_stream import (
    CompilationError,
    CompiledPlans,
    ExecutionResult,
    PhysicalPlanEnvelope,
    PlanKind,
    RunnerType,
    RuntimeExecutionError,
    UniteStream,
    UniteStreamClient,
    UniteStreamCompiler,
    UniteStreamDataFrame,
    UniteStreamError,
    UniteStreamParser,
    UniteStreamRuntime,
)

# --------------------------------------------------------------------------- #
# Wrapper basics + parser error semantics                                     #
# --------------------------------------------------------------------------- #


def test_wrapper_blocks_collect() -> None:
    wrapped = UniteStreamDataFrame(daft.from_pydict({"x": [1]}))
    with pytest.raises(AttributeError, match="collect"):
        wrapped.collect()  # type: ignore[attr-defined]


def test_parser_allows_eval_and_exec_in_user_script() -> None:
    """No AST gate: dynamic-code primitives are allowed inside user scripts."""
    compiler = UniteStreamCompiler(system_target_dir="/tmp/out")
    script = """
value = eval('1 + 2')
df = UniteStream.from_pydict({"v": [value]})
OUTPUT_STREAMS.append(df)
"""
    bundle = compiler.compile_to_plans(script)
    assert len(bundle) == 1


def test_parser_wraps_empty_script_as_compilation_error() -> None:
    compiler = UniteStreamCompiler(system_target_dir="/tmp/out")
    with pytest.raises(CompilationError, match="OUTPUT_STREAMS"):
        compiler.compile_to_plans("   \n  ")


def test_parser_wraps_syntax_error_as_compilation_error() -> None:
    compiler = UniteStreamCompiler(system_target_dir="/tmp/out")
    with pytest.raises(CompilationError, match="SyntaxError"):
        compiler.compile_to_plans("this is not valid python !!!")


def test_compilation_error_when_no_outputs() -> None:
    compiler = UniteStreamCompiler(system_target_dir="/tmp/out")
    with pytest.raises(CompilationError, match="OUTPUT_STREAMS"):
        compiler.compile_to_plans("x = 1\n")


def test_compilation_error_when_invalid_stream_object() -> None:
    compiler = UniteStreamCompiler(system_target_dir="/tmp/out")
    with pytest.raises(CompilationError, match="UniteStreamDataFrame"):
        compiler.compile_to_plans("OUTPUT_STREAMS.append(42)\n")


def test_uniteStream_error_is_base_class() -> None:
    assert issubclass(CompilationError, UniteStreamError)
    assert issubclass(RuntimeExecutionError, UniteStreamError)


# --------------------------------------------------------------------------- #
# Auto-injection of names in user scripts                                     #
# --------------------------------------------------------------------------- #


def test_user_script_needs_no_imports() -> None:
    """The parser auto-injects ``UniteStream``, ``func``, ``cls``, ``method``."""
    script = """
@func
def plus_one(x: int) -> int:
    return int(x) + 1

df = UniteStream.from_pydict({"v": [1, 2]})
OUTPUT_STREAMS.append(df.with_column("w", plus_one(UniteStream.col("v"))))
"""
    plans = UniteStreamParser().parse(script)
    results = UniteStreamRuntime().execute_plans(plans)
    assert results[0].to_pydict() == {"v": [1, 2], "w": [2, 3]}


def test_user_script_can_import_stdlib() -> None:
    script = """
import math

df = UniteStream.from_pydict({"x": [1.0, 4.0, 9.0]})
OUTPUT_STREAMS.append(df.with_column("sqrt_x", UniteStream.col("x").apply(math.sqrt, return_dtype=UniteStream.DataType.float64())))
"""
    plans = UniteStreamParser().parse(script)
    UniteStreamRuntime().execute_plans(plans)


# --------------------------------------------------------------------------- #
# Wrapper forwarding                                                          #
# --------------------------------------------------------------------------- #


def test_groupby_agg_forwards_to_daft() -> None:
    wrapped = UniteStreamDataFrame(daft.from_pydict({"k": ["a", "a", "b"], "v": [1, 2, 3]}))
    out = wrapped.groupby("k").agg(daft.col("v").sum().alias("s"))
    assert isinstance(out, UniteStreamDataFrame)
    assert out._df.schema().column_names() == ["k", "s"]


def test_join_forwards_to_daft() -> None:
    left = UniteStreamDataFrame(daft.from_pydict({"id": [1, 2], "x": [10, 20]}))
    right = UniteStreamDataFrame(daft.from_pydict({"id": [1, 3], "y": [100, 300]}))
    joined = left.join(right, on="id", how="inner")
    assert isinstance(joined, UniteStreamDataFrame)
    assert joined._df.schema().column_names() == ["id", "x", "y"]


# --------------------------------------------------------------------------- #
# End-to-end                                                                  #
# --------------------------------------------------------------------------- #


def test_parser_runtime_roundtrip() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        target = Path(tmp) / "lake" / "batch"
        compiler = UniteStreamCompiler(system_target_dir=str(target))
        script = """
source = UniteStream.from_pydict({"user_id": [1, 2, 3], "age": [18, 25, 30]})
clean = source.filter(UniteStream.col("age") >= 21).select("user_id", "age")
OUTPUT_STREAMS.append(clean)
"""
        bundle = compiler.compile_to_plans(script, job_namespace="roundtrip")
        UniteStreamRuntime().execute_plans(bundle)
        out_dir = target / "roundtrip" / "stream_job_0.parquet"
        assert list(out_dir.glob("**/*.parquet"))


def test_udf_func_in_parser_script() -> None:
    script = """
@func
def double_it(x: int) -> int:
    return x * 2

source = UniteStream.from_pydict({"n": [1, 2, 3]})
OUTPUT_STREAMS.append(source.with_column("m", double_it(UniteStream.col("n"))))
"""
    plans = UniteStreamParser().parse(script)
    results = UniteStreamRuntime().execute_plans(plans)
    assert results[0].to_pydict() == {"n": [1, 2, 3], "m": [2, 4, 6]}


# --------------------------------------------------------------------------- #
# Thread isolation                                                            #
# --------------------------------------------------------------------------- #


_THREAD_SCRIPT_TEMPLATE = """
import math

@func
def shift(x: int) -> int:
    return int(x) + {offset}

df = UniteStream.from_pydict({{"v": [1, 2, 3]}})
OUTPUT_STREAMS.append(df.with_column("out", shift(UniteStream.col("v"))))
"""


def test_compiler_compiles_concurrently_with_isolated_namespaces() -> None:
    """Concurrent compiles do not raise and produce distinct job namespaces.

    Note: the UniteStream compiler itself (parsing + write-path binding +
    physical-plan lowering) is thread-safe — it allocates no shared mutable
    state and ``OUTPUT_STREAMS`` is a per-call local list. Daft's underlying
    runners (e.g. the global partition_set_cache populated by
    ``daft.from_pydict``) are **not** thread-safe in 0.7.14, so we only
    assert isolation at the compile level here.
    """
    compiler = UniteStreamCompiler(system_target_dir="/tmp/concurrent_compile")
    offsets = [10, 100, 1000, 10000, 100000]

    def compile_job(offset: int) -> CompiledPlans:
        script = _THREAD_SCRIPT_TEMPLATE.format(offset=offset)
        return compiler.compile_to_plans(
            script, job_namespace=f"thread_{offset}"
        )

    with ThreadPoolExecutor(max_workers=len(offsets)) as pool:
        bundles: dict[int, CompiledPlans] = {}
        futures = {pool.submit(compile_job, o): o for o in offsets}
        for fut in as_completed(futures):
            bundles[futures[fut]] = fut.result()

    namespaces = {b.job_namespace for b in bundles.values()}
    assert namespaces == {f"thread_{o}" for o in offsets}, namespaces
    for offset, bundle in bundles.items():
        assert bundle.num_streams == 1
        assert bundle.target_dir.endswith("concurrent_compile")


def test_default_job_namespace_is_unique_per_call() -> None:
    """Two compiles without an explicit namespace use distinct UUID sub-dirs."""
    compiler = UniteStreamCompiler(system_target_dir="/tmp/uniqueness")
    script = """
OUTPUT_STREAMS.append(UniteStream.from_pydict({"x": [1]}))
"""
    bundle_a = compiler.compile_to_plans(script)
    bundle_b = compiler.compile_to_plans(script)
    assert bundle_a.job_namespace != bundle_b.job_namespace


def test_output_streams_is_per_call_local_list() -> None:
    """Two consecutive compiles must each see their own fresh ``OUTPUT_STREAMS``."""
    captured_lists: list[object] = []

    def remember_list(obj: object) -> None:
        captured_lists.append(obj)

    script = """
remember_list(OUTPUT_STREAMS)
OUTPUT_STREAMS.append(UniteStream.from_pydict({"x": [1]}))
"""
    compiler = UniteStreamCompiler(
        system_target_dir="/tmp/per_call_local",
        extra_globals={"remember_list": remember_list},
    )
    compiler.compile_to_plans(script, job_namespace="call_a")
    compiler.compile_to_plans(script, job_namespace="call_b")

    assert len(captured_lists) == 2
    assert captured_lists[0] is not captured_lists[1], (
        "OUTPUT_STREAMS must be a fresh list per compile call"
    )


# --------------------------------------------------------------------------- #
# Namespace surface                                                           #
# --------------------------------------------------------------------------- #


def test_namespace_exposes_classes_and_modules() -> None:
    assert UniteStream.DataType is daft.DataType
    assert UniteStream.Expression is daft.Expression
    assert UniteStream.Schema is daft.Schema
    assert UniteStream.Window is daft.Window
    assert UniteStream.functions is daft.functions


def test_namespace_does_not_expose_sql() -> None:
    for name in ("sql", "sql_expr", "read_sql"):
        assert not hasattr(UniteStream, name), name


def test_namespace_does_not_expose_runtime_or_plan_transport() -> None:
    for name in (
        "serialize_plan",
        "deserialize_plan",
        "execute_plan",
        "execute_from_bytes",
        "set_runner_native",
        "get_or_create_runner",
        "get_or_infer_runner_type",
        "write_table",
    ):
        assert not hasattr(UniteStream, name), name


def test_runner_enum_round_trip() -> None:
    rt = UniteStreamRuntime()
    assert rt.runner is RunnerType.NATIVE

    rt2 = UniteStreamRuntime(runner=RunnerType.NATIVE, configure_runner=False)
    assert rt2.runner is RunnerType.NATIVE


# --------------------------------------------------------------------------- #
# Layered API: Parser / Compiler / Runtime / Client                           #
# --------------------------------------------------------------------------- #


_SAMPLE_SCRIPT = """
df = UniteStream.from_pydict({"id": [1, 2, 3], "age": [18, 25, 30]})
OUTPUT_STREAMS.append(df.filter(UniteStream.col("age") >= 21))
"""


def test_parser_returns_raw_daft_plans() -> None:
    parser = UniteStreamParser()
    plans = parser.parse(_SAMPLE_SCRIPT)
    assert len(plans) == 1
    assert isinstance(plans[0], daft.DataFrame)
    # The parser does *not* execute, but the lazy plan is usable directly:
    materialized = plans[0].collect(num_preview_rows=None)
    assert materialized.to_pydict() == {"id": [2, 3], "age": [25, 30]}


def test_compiler_exposes_parser_property() -> None:
    compiler = UniteStreamCompiler(system_target_dir="/tmp/layer")
    assert isinstance(compiler.parser, UniteStreamParser)
    plans = compiler.parse(_SAMPLE_SCRIPT)
    assert len(plans) == 1 and isinstance(plans[0], daft.DataFrame)


def test_client_submit_end_to_end_via_ir() -> None:
    """``submit`` default path goes through the IR pipeline for both runners.

    Native client → ``LocalPhysicalPlan`` envelopes → ``NativeExecutor`` →
    one :class:`ExecutionResult` per stream, plus on-disk parquet output.
    """
    with tempfile.TemporaryDirectory() as tmp:
        target = Path(tmp) / "client_lake"
        client = UniteStreamClient(system_target_dir=str(target))
        results = client.submit(_SAMPLE_SCRIPT, job_namespace="job1")
        assert len(results) == 1
        result = results[0]
        assert isinstance(result, ExecutionResult)
        assert result.kind is PlanKind.LOCAL
        assert (target / "job1" / "stream_job_0.parquet").exists()


def test_client_submit_use_ir_false_returns_dataframes() -> None:
    """``submit(use_ir=False)`` bypasses cloudpickle and returns DataFrames."""
    with tempfile.TemporaryDirectory() as tmp:
        target = Path(tmp) / "client_no_ir"
        client = UniteStreamClient(system_target_dir=str(target))
        results = client.submit(
            _SAMPLE_SCRIPT, job_namespace="job_no_ir", use_ir=False
        )
        assert len(results) == 1
        assert "path" in results[0].schema().column_names()
        assert (target / "job_no_ir" / "stream_job_0.parquet").exists()


def test_client_parse_compile_inspect_separately() -> None:
    """Parse / compile / inspect the IR without executing it."""
    with tempfile.TemporaryDirectory() as tmp:
        target = Path(tmp) / "separated"
        client = UniteStreamClient(system_target_dir=str(target))

        plans = client.parse(_SAMPLE_SCRIPT)
        assert isinstance(plans[0], daft.DataFrame)

        ir_bytes = client.compile(_SAMPLE_SCRIPT, job_namespace="manual")
        assert isinstance(ir_bytes, bytes) and len(ir_bytes) > 0

        # Native client → LocalPhysicalPlan envelopes
        envelopes = cloudpickle.loads(ir_bytes)
        assert isinstance(envelopes, list) and len(envelopes) == 1
        assert isinstance(envelopes[0], PhysicalPlanEnvelope)
        assert envelopes[0].kind is PlanKind.LOCAL
        assert envelopes[0].job_namespace == "manual"


def test_client_execute_plans_on_raw_parser_output() -> None:
    """Calling :meth:`execute_plans` on raw parser output skips write binding."""
    client = UniteStreamClient(system_target_dir="/tmp/no_io")
    plans = client.parse(_SAMPLE_SCRIPT)
    results = client.execute_plans(plans)
    assert results[0].to_pydict() == {"id": [2, 3], "age": [25, 30]}


def test_module_level_parse_shortcut() -> None:
    plans = unite_stream.parse(_SAMPLE_SCRIPT)
    assert len(plans) == 1 and isinstance(plans[0], daft.DataFrame)


# --------------------------------------------------------------------------- #
# Physical-plan IR (native carrier)                                          #
# --------------------------------------------------------------------------- #


def test_compile_to_ir_emits_local_physical_plan_for_native_runner() -> None:
    """Native compile path lowers each plan to a cloudpickleable
    ``LocalPhysicalPlan`` envelope and survives a full round-trip."""
    with tempfile.TemporaryDirectory() as tmp:
        compiler = UniteStreamCompiler(system_target_dir=str(Path(tmp) / "ir_local"))
        ir_bytes = compiler.compile_to_ir(
            _SAMPLE_SCRIPT,
            runner=RunnerType.NATIVE,
            job_namespace="ir_local_job",
        )
        assert isinstance(ir_bytes, bytes) and len(ir_bytes) > 0

        envelopes = cloudpickle.loads(ir_bytes)
        assert len(envelopes) == 1
        env = envelopes[0]
        assert isinstance(env, PhysicalPlanEnvelope)
        assert env.kind is PlanKind.LOCAL
        assert env.is_local()
        assert env.stream_index == 0
        assert env.job_namespace == "ir_local_job"
        # The wrapped plan is the actual native physical plan
        assert type(env.plan).__name__ == "LocalPhysicalPlan"
        # Pickle round-trip preserves the plan as the same Daft type
        roundtripped = cloudpickle.loads(cloudpickle.dumps(env))
        assert type(roundtripped.plan).__name__ == "LocalPhysicalPlan"


def test_compile_to_ir_default_runner_matches_global_daft_config() -> None:
    """``runner=None`` infers from the current Daft runner."""
    compiler = UniteStreamCompiler(system_target_dir="/tmp/ir_infer")
    # Native is the default in the test session
    ir_bytes = compiler.compile_to_ir(_SAMPLE_SCRIPT)
    envelopes = cloudpickle.loads(ir_bytes)
    assert envelopes[0].kind is PlanKind.LOCAL


def test_execute_ir_local_runs_via_native_executor() -> None:
    """Native IR (cloudpickled ``LocalPhysicalPlan`` envelopes) executes end-to-end.

    The envelope carries the ``inputs`` map and partition_set_cache snapshot
    alongside the plan, so :class:`NativeExecutor` can rehydrate the source
    operators and stream every partition to disk. We assert:

    * ``execute_ir`` returns one :class:`ExecutionResult` per stream.
    * Each result reports ``kind=LOCAL``, the bound parquet path, the
      partition count, and the executor's :class:`PyExecutionStats`.
    * The parquet files actually land on disk.
    """
    with tempfile.TemporaryDirectory() as tmp:
        target = Path(tmp) / "ir_local_exec"
        compiler = UniteStreamCompiler(system_target_dir=str(target))
        ir_bytes = compiler.compile_to_ir(
            _SAMPLE_SCRIPT, runner=RunnerType.NATIVE, job_namespace="local_run"
        )
        runtime = UniteStreamRuntime(runner=RunnerType.NATIVE, configure_runner=False)
        results = runtime.execute_ir(ir_bytes)

        assert len(results) == 1
        result = results[0]
        assert isinstance(result, ExecutionResult)
        assert result.kind is PlanKind.LOCAL
        assert result.stream_index == 0
        assert result.num_partitions and result.num_partitions >= 1
        assert result.output_path is not None
        assert "local_run/stream_job_0.parquet" in result.output_path
        assert result.stats is not None

        out_dir = target / "local_run" / "stream_job_0.parquet"
        assert out_dir.exists()
        assert list(out_dir.glob("**/*.parquet"))


def test_execute_ir_local_survives_cross_process_pickle_roundtrip() -> None:
    """LOCAL IR round-trips through ``cloudpickle.dumps + loads`` and still executes.

    Simulates the cross-process path: the compiler hands out ``bytes``, the
    bytes are stored / shipped somewhere (here, just reloaded in memory),
    and the runtime later resurrects and runs them.
    """
    with tempfile.TemporaryDirectory() as tmp:
        target = Path(tmp) / "ir_local_xprocess"
        compiler = UniteStreamCompiler(system_target_dir=str(target))
        ir_bytes = compiler.compile_to_ir(
            _SAMPLE_SCRIPT, runner=RunnerType.NATIVE, job_namespace="xproc"
        )

        # Round-trip the IR bytes once more — proves the envelope payload
        # (plan + inputs + psets) is self-contained.
        rehydrated_envelopes = cloudpickle.loads(ir_bytes)
        re_serialized = cloudpickle.dumps(rehydrated_envelopes)
        assert re_serialized  # still bytes

        runtime = UniteStreamRuntime(runner=RunnerType.NATIVE, configure_runner=False)
        results = runtime.execute_ir(re_serialized)
        assert results[0].kind is PlanKind.LOCAL
        assert (target / "xproc" / "stream_job_0.parquet").exists()


def test_compile_and_extract_ir_is_alias_of_compile_to_ir() -> None:
    """Backwards-compat alias preserved."""
    compiler = UniteStreamCompiler(system_target_dir="/tmp/ir_alias")
    a = compiler.compile_to_ir(_SAMPLE_SCRIPT, runner=RunnerType.NATIVE, job_namespace="alias")
    b = compiler.compile_and_extract_ir(_SAMPLE_SCRIPT, runner=RunnerType.NATIVE, job_namespace="alias")
    # IR bytes can vary across runs due to embedded query ids; assert shape only
    env_a = cloudpickle.loads(a)
    env_b = cloudpickle.loads(b)
    assert env_a[0].kind == env_b[0].kind == PlanKind.LOCAL
    assert env_a[0].job_namespace == env_b[0].job_namespace == "alias"


# --------------------------------------------------------------------------- #
# CompiledPlans (in-process carrier, symmetric to IR bytes)                   #
# --------------------------------------------------------------------------- #


def test_compile_to_plans_returns_compiled_plans_bundle() -> None:
    """`compile_to_plans` returns a `CompiledPlans` symmetric to the IR `bytes`.

    Verifies the in-process carrier carries metadata (namespace + target_dir),
    iterates lazily, and is a thin tuple-backed wrapper.
    """
    with tempfile.TemporaryDirectory() as tmp:
        target = Path(tmp) / "bundle_lake"
        compiler = UniteStreamCompiler(system_target_dir=str(target))
        bundle = compiler.compile_to_plans(_SAMPLE_SCRIPT, job_namespace="bundle_job")

        assert isinstance(bundle, CompiledPlans)
        assert bundle.num_streams == 1
        assert bundle.job_namespace == "bundle_job"
        assert bundle.target_dir == str(target)
        assert "CompiledPlans" in repr(bundle)
        assert "bundle_job" in repr(bundle)

        # Iteration / indexing
        assert isinstance(bundle[0], daft.DataFrame)
        for plan in bundle:
            assert isinstance(plan, daft.DataFrame)

        # `.plans` returns a defensive copy
        plans_a = bundle.plans
        plans_b = bundle.plans
        assert plans_a == plans_b
        plans_a.clear()
        assert bundle.num_streams == 1, "internal state should be immutable"


def test_runtime_execute_plans_accepts_compiled_plans_and_raw_list() -> None:
    """Symmetric carriers: Runtime should consume both shapes interchangeably."""
    with tempfile.TemporaryDirectory() as tmp:
        target = Path(tmp) / "carrier"
        compiler = UniteStreamCompiler(system_target_dir=str(target))
        runtime = UniteStreamRuntime()

        # Bound CompiledPlans → write metadata
        bundle = compiler.compile_to_plans(_SAMPLE_SCRIPT, job_namespace="from_bundle")
        results_a = runtime.execute_plans(bundle)
        assert "path" in results_a[0].schema().column_names()
        assert (target / "from_bundle" / "stream_job_0.parquet").exists()

        # Raw parser plans (no write binding) → actual data
        raw_plans = compiler.parse(_SAMPLE_SCRIPT)
        results_b = runtime.execute_plans(raw_plans)
        assert results_b[0].to_pydict() == {"id": [2, 3], "age": [25, 30]}


def test_compiled_plans_rejects_non_dataframe_entries() -> None:
    with pytest.raises(TypeError, match="daft.DataFrame"):
        CompiledPlans([42])


def test_compiled_plans_metadata_defaults_to_none_for_unbound_plans() -> None:
    """A bundle built directly (e.g. from a parser) carries no namespace info."""
    raw_plans = unite_stream.parse(_SAMPLE_SCRIPT)
    bundle = CompiledPlans(raw_plans)
    assert bundle.job_namespace is None
    assert bundle.target_dir is None
    assert bundle.num_streams == 1


def test_module_level_submit_shortcut() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        target = Path(tmp) / "shortcut"
        results = unite_stream.submit(
            _SAMPLE_SCRIPT, system_target_dir=str(target), job_namespace="s"
        )
        assert len(results) == 1
        assert isinstance(results[0], ExecutionResult)
        assert results[0].kind is PlanKind.LOCAL
        assert (target / "s" / "stream_job_0.parquet").exists()
