"""End-to-end UniteStream demo (Parser / Runtime split + concurrency).

Highlights:
    * **Zero ``import``** in user scripts — ``UniteStream``, ``func``, ``cls``,
      ``method``, ``udf``, ``metrics`` and ``daft`` are auto-injected by the
      Parser sandbox.
    * **Zero ``daft`` import in the orchestrator** — only ``unite_stream`` is
      needed externally.
    * **Thread-safe Parser** — a single :class:`UniteStreamCompiler` is shared
      across worker threads, each with its own ``job_namespace`` for isolated
      on-disk outputs.

Run::

    python examples/unite_stream_orchestrator.py
"""

from __future__ import annotations

import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import NamedTuple

from unite_stream import (
    CompilationError,
    UniteStreamClient,
    UniteStreamCompiler,
    UniteStreamParser,
    UniteStreamRuntime,
    parse,
    submit,
)

# --------------------------------------------------------------------------- #
# User scripts                                                                #
#                                                                             #
# Note: NO imports of ``daft`` / ``unite_stream`` are needed inside scripts.  #
# ``UniteStream``, ``OUTPUT_STREAMS`` and the UDF decorators are auto-bound.  #
# --------------------------------------------------------------------------- #

BASIC_SCRIPT = """
df = UniteStream.from_pydict({"id": [1, 2, 3], "age": [18, 25, 30]})
OUTPUT_STREAMS.append(df.filter(UniteStream.col("age") >= 21))
"""

UDF_SCRIPT = """
import math

@func
def score(age):
    return float(math.log1p(int(age) + 1))

df = UniteStream.from_pydict({"id": [1, 2, 3], "age": [18, 25, 30]})
OUTPUT_STREAMS.append(df.with_column("score", score(UniteStream.col("age"))))
"""

GROUPBY_SCRIPT = """
orders = UniteStream.from_pydict({"user_id": [1, 1, 2, 3], "amount": [10, 20, 30, 40]})
agg = orders.groupby("user_id").agg(UniteStream.col("amount").sum().alias("total"))
OUTPUT_STREAMS.append(agg)
"""

CLASS_UDF_SCRIPT = """
@cls
class Badge:
    def __init__(self):
        self.suffix = "-vip"

    @method.batch(return_dtype=UniteStream.DataType.string())
    def label(self, name):
        return [n + self.suffix for n in name.to_pylist()]

users = UniteStream.from_pydict({"name": ["amy", "ben", "cody"]})
OUTPUT_STREAMS.append(users.with_column("tag", Badge().label(UniteStream.col("name"))))
"""

WINDOW_SCRIPT = """
df = UniteStream.from_pydict({
    "country": ["us", "us", "cn", "cn"],
    "amount":  [10, 30, 20, 50],
})
window = UniteStream.Window().partition_by("country").order_by("amount", desc=True)
ranked = df.with_column("rk", UniteStream.functions.rank().over(window))
OUTPUT_STREAMS.append(ranked)
"""


class Job(NamedTuple):
    name: str
    script: str


# --------------------------------------------------------------------------- #
# Demos                                                                       #
# --------------------------------------------------------------------------- #


def run_sequential(target_dir: Path) -> None:
    """Compile and execute a small catalogue of scripts back-to-back."""
    print("== Sequential demo ==")
    compiler = UniteStreamCompiler(system_target_dir=str(target_dir))
    runtime = UniteStreamRuntime(distributed_mode=False)

    jobs = [
        Job("basic", BASIC_SCRIPT),
        Job("udf", UDF_SCRIPT),
        Job("groupby", GROUPBY_SCRIPT),
        Job("class_udf", CLASS_UDF_SCRIPT),
        Job("window", WINDOW_SCRIPT),
    ]
    for job in jobs:
        ir_bytes = compiler.compile_and_extract_ir(job.script, job_namespace=job.name)
        results = runtime.execute_ir(ir_bytes)
        print(f"  [seq] {job.name}: streams={len(results)}, first preview:")
        print(results[0])


def run_concurrent(target_dir: Path) -> None:
    """Compile many scripts in parallel via :class:`ThreadPoolExecutor`."""
    print("\n== Concurrent demo (4 threads, isolated job namespaces) ==")
    compiler = UniteStreamCompiler(system_target_dir=str(target_dir))
    runtime = UniteStreamRuntime(distributed_mode=False)

    jobs = [
        Job("basic_a", BASIC_SCRIPT),
        Job("udf_b", UDF_SCRIPT),
        Job("group_c", GROUPBY_SCRIPT),
        Job("window_d", WINDOW_SCRIPT),
    ]

    def compile_and_run(job: Job) -> tuple[str, int]:
        ir_bytes = compiler.compile_and_extract_ir(
            job.script, job_namespace=job.name
        )
        materialized = runtime.execute_ir(ir_bytes)
        return job.name, len(materialized[0])

    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {pool.submit(compile_and_run, j): j for j in jobs}
        for fut in as_completed(futures):
            try:
                name, rows = fut.result()
            except CompilationError as exc:
                print(f"  [thread] 编译失败: {exc}")
            else:
                print(f"  [thread] {name}: rows={rows}")


def run_layered_api(target_dir: Path) -> None:
    """Show the four entry points: Parser / Compiler / Runtime / Client."""
    print("\n== Layered API demo ==")

    # 1. Parser only: get raw daft.DataFrame plans, do anything with them.
    parser_plans = UniteStreamParser().parse(BASIC_SCRIPT)
    print(f"  [parser] raw plans: {len(parser_plans)}; type={type(parser_plans[0]).__name__}")

    # 2. Compiler: parse + bind write paths + cloudpickle to IR bytes.
    compiler = UniteStreamCompiler(system_target_dir=str(target_dir))
    ir_bytes = compiler.compile_and_extract_ir(BASIC_SCRIPT, job_namespace="layered_ir")
    print(f"  [compiler] IR bytes size: {len(ir_bytes)}")

    # 3. Runtime: execute IR or raw plans.
    runtime = UniteStreamRuntime(distributed_mode=False)
    materialized = runtime.execute_ir(ir_bytes)
    print(f"  [runtime] materialized rows: {len(materialized[0])}")

    # 4. Client façade: one-shot submit.
    client = UniteStreamClient(system_target_dir=str(target_dir))
    submitted = client.submit(GROUPBY_SCRIPT, job_namespace="layered_submit")
    print(f"  [client] submit groupby rows: {len(submitted[0])}")

    # 5. Module-level shortcuts.
    shortcut_plans = parse(BASIC_SCRIPT)
    print(f"  [parse() shortcut] plans: {len(shortcut_plans)}")
    shortcut_results = submit(
        BASIC_SCRIPT,
        system_target_dir=str(target_dir),
        job_namespace="layered_shortcut",
    )
    print(f"  [submit() shortcut] rows: {len(shortcut_results[0])}")


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        target_dir = Path(tmp) / "production_data_lake" / "20260521"
        run_sequential(target_dir)
        run_concurrent(target_dir)
        run_layered_api(target_dir)

        print("\n生成的目录：")
        for sub in sorted(target_dir.glob("*")):
            print(f"  {sub}")


if __name__ == "__main__":
    main()
