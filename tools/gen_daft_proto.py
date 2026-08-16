#!/usr/bin/env python3
"""Regenerate the Python protobuf bindings for the Daft runtime protocol.

The wire protocol schema lives in ``src/daft-protocol/proto/daft/v1``. Rust
builds it with prost (``src/daft-protocol/build.rs``); this script compiles the
same schema to Python (``daft/runtime/daft_proto``) so that the client and the
Python UDF worker speak byte-for-byte the same protocol as the Rust
``daft-runtime`` binary.

``runtime.proto``, ``worker.proto``, ``udf.proto`` and ``plan.proto`` are all
compiled to Python. The Python client parses the protobuf logical plan
(``plan.proto``) to extract self-contained ``UdfDescriptor`` messages
(``udf.proto``) while the interpreter is available, so the Rust runtime never
has to parse the plan to discover UDFs.

Usage::

    .venv/bin/python tools/gen_daft_proto.py
    PROTOC=/path/to/protoc .venv/bin/python tools/gen_daft_proto.py

``--check`` regenerates into a staging directory and verifies the checked-in
bindings are byte-for-byte identical, failing with a non-zero exit code on
drift. This is the mode CI should use to catch schema/gencode desync.

The script verifies that the generated gencode is compatible with the installed
``google.protobuf`` runtime: protoc releases newer than the runtime produce
bindings that fail to import (``VersionError``). Use a protoc whose release
matches the runtime (e.g. protobuf 5.29.x runtime <-> protoc 29.x).
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import google.protobuf

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PROTO_DIR = REPO_ROOT / "src" / "daft-protocol" / "proto"
DEFAULT_OUT_DIR = REPO_ROOT / "daft" / "runtime" / "daft_proto"
# The generated bindings use absolute imports derived from the protobuf
# package (``daft.v1``). ``daft`` is also the name of the top-level Daft
# package, so the generated tree is renamed to this unique top-level package
# and every ``from daft.v1 import ...`` statement is rewritten to point at it.
GEN_PACKAGE = "daft_runtime_proto"
PROTO_FILES = [
    "daft/v1/runtime.proto",
    "daft/v1/worker.proto",
    "daft/v1/udf.proto",
    "daft/v1/plan.proto",
    "daft/v1/distributed.proto",
]


def _version_tuple(version: str) -> tuple[int, ...]:
    return tuple(int(part) for part in re.split(r"[.\-+]", version)[:3])


def _resolve_protoc() -> list[str]:
    """Return the argv prefix for the grpc_tools-bundled protoc, if present."""
    try:
        import grpc_tools.protoc  # noqa: PLC0415
    except ImportError as e:
        return []
    return [
        sys.executable,
        "-c",
        "import sys; from grpc_tools import protoc; sys.exit(protoc.main(sys.argv[1:]))",
    ]


def _protoc_candidates(explicit: str | None) -> list[list[str]]:
    """Ordered protoc argv prefixes, most preferred first."""
    candidates: list[list[str]] = []
    if explicit:
        candidates.append([explicit])
    from_env = os.environ.get("PROTOC")
    if from_env and not explicit:
        candidates.append([from_env])
    bundled = _resolve_protoc()
    if bundled:
        candidates.append(bundled)
    if not explicit and not from_env:
        configured = shutil.which("protoc")
        if configured:
            candidates.append([configured])
    return candidates


def _generate_with(protoc: list[str], proto_dir: Path, proto_files: list[Path], staging_dir: Path) -> None:
    """Run protoc; raises SystemExit when the compiler is unusable."""
    # Run from the proto root with ``-I.`` and file names relative to it. The
    # grpc_tools-bundled protoc rejects absolute ``-I`` paths and can only
    # match ``daft/v1/*.proto`` imports against the file list when the names
    # are relative to the proto root; the system protoc accepts both forms.
    relative_proto_files = [os.path.relpath(path, proto_dir) for path in proto_files]
    subprocess.run(
        [
            *protoc,
            "-I.",
            f"--python_out={staging_dir}",
            *relative_proto_files,
        ],
        cwd=proto_dir,
        check=True,
    )


def _relocate_generated(staging_dir: Path) -> None:
    """Rename the generated ``daft`` package to ``GEN_PACKAGE``.

    protoc writes bindings under ``<python_out>/daft/v1`` because the proto
    declares ``package daft.v1``. That collides with the real ``daft`` Python
    package, so the whole tree is renamed and the absolute imports inside the
    generated modules are rewritten to the unique package name.
    """
    generated_pkg = staging_dir / "daft"
    if not generated_pkg.is_dir():
        raise SystemExit(f"expected generated package at {generated_pkg}")
    target_pkg = staging_dir / GEN_PACKAGE
    for pb2 in generated_pkg.rglob("*_pb2.py"):
        text = pb2.read_text(encoding="utf-8")
        # ``daft_runtime_proto`` is a subpackage of ``daft.runtime.daft_proto``,
        # so the rewritten imports must use the full dotted path (a top-level
        # ``from daft_runtime_proto.v1 import ...`` would not resolve).
        absolute = f"daft.runtime.daft_proto.{GEN_PACKAGE}.v1"
        text = text.replace(
            f"from daft.v1 import",
            f"from {absolute} import",
        )
        text = text.replace(
            f"import daft.v1",
            f"import {absolute}",
        )
        pb2.write_text(text, encoding="utf-8")
    generated_pkg.rename(target_pkg)
    (target_pkg / "__init__.py").touch()
    (target_pkg / "v1" / "__init__.py").touch()


def _check_runtime_compatibility(generated: Path) -> None:
    """Fail when generated bindings require a newer protobuf than installed."""
    header = re.compile(r"Protobuf Python Version:\s*([0-9][0-9.]*)")
    versions: list[tuple[int, ...]] = []
    for pb2 in generated.glob("**/*_pb2.py"):
        match = header.search(pb2.read_text(encoding="utf-8"))
        if match:
            versions.append(_version_tuple(match.group(1)))
    if not versions:
        return
    runtime = _version_tuple(google.protobuf.__version__)
    newest = max(versions)
    if newest > runtime:
        raise SystemExit(
            "protoc is newer than the installed protobuf runtime: generated "
            f"gencode requires protobuf >= {'.'.join(map(str, newest))} but "
            f"google.protobuf is {google.protobuf.__version__}. Use a matching "
            "protoc (e.g. protobuf 5.29.x runtime <-> protoc 29.x)."
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--protoc",
        default=None,
        help="path to protoc (default: PROTOC env or protoc on PATH)",
    )
    parser.add_argument(
        "--proto-dir",
        type=Path,
        default=DEFAULT_PROTO_DIR,
        help="directory containing daft/v1/*.proto",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=DEFAULT_OUT_DIR,
        help="output directory for generated *_pb2.py",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="verify checked-in bindings match the schema; do not write anything",
    )
    args = parser.parse_args()

    proto_files = [args.proto_dir / path for path in PROTO_FILES]
    for proto_file in proto_files:
        if not proto_file.is_file():
            raise SystemExit(f"missing proto file: {proto_file}")

    # Generate into a staging directory first so a version mismatch never
    # leaves broken bindings on disk.
    generated = False
    with tempfile.TemporaryDirectory() as staging:
        for protoc in _protoc_candidates(args.protoc):
            staging_dir = Path(staging)
            for stale in staging_dir.iterdir():
                if stale.is_dir():
                    shutil.rmtree(stale)
                else:
                    stale.unlink()
            try:
                _generate_with(protoc, args.proto_dir, proto_files, staging_dir)
            except (subprocess.CalledProcessError, OSError):
                continue
            try:
                _check_runtime_compatibility(staging_dir)
            except SystemExit:
                # Compiler too new for the installed runtime; try the next
                # candidate (e.g. the grpc_tools-bundled protoc).
                continue
            generated = True
            break
        if not generated:
            raise SystemExit(
                "no usable protoc found: tried explicit --protoc, PROTOC env, "
                "grpc_tools, and protoc on PATH. Install grpcio-tools (pip "
                "install grpcio-tools) or point PROTOC at a compiler whose "
                "release matches the installed google.protobuf runtime."
            )
        staging_dir = Path(staging)
        _relocate_generated(staging_dir)
        if args.check:
            mismatches = []
            for generated in sorted(staging_dir.rglob("*_pb2.py")):
                relative = generated.relative_to(staging_dir)
                target = args.out_dir / relative
                if not target.is_file() or target.read_bytes() != generated.read_bytes():
                    mismatches.append(str(relative))
            if mismatches:
                raise SystemExit(
                    "checked-in protobuf bindings are out of sync with the schema: "
                    f"{', '.join(mismatches)}. Run `tools/gen_daft_proto.py` to "
                    "regenerate."
                )
            print(
                f"Bindings are in sync with {len(PROTO_FILES)} proto files "
                f"(protoc {protoc})"
            )
            return
        for generated in staging_dir.rglob("*_pb2.py"):
            relative = generated.relative_to(staging_dir)
            target = args.out_dir / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(generated, target)

    # Import + round-trip smoke test against the installed runtime.
    sys.path.insert(0, str(REPO_ROOT))
    from daft.runtime.daft_proto.daft_runtime_proto.v1 import (
        distributed_pb2,
        plan_pb2,
        runtime_pb2,
        udf_pb2,
        worker_pb2,
    )

    plan = plan_pb2.LogicalPlan()
    plan.project.plan_id = 1
    plan.project.node_id = 2
    assert plan_pb2.LogicalPlan.FromString(plan.SerializeToString()) == plan

    descriptor = udf_pb2.UdfDescriptor(
        udf_id="func-1",
        kind=udf_pb2.UDF_KIND_SCALAR_ROW_WISE,
        name="triple",
        return_dtype=b"\x08\x01",
        num_inputs=1,
        code=b"cloudpickle-bytes",
        python_version="3.11.9",
    )
    assert udf_pb2.UdfDescriptor.FromString(descriptor.SerializeToString()) == descriptor

    native = runtime_pb2.JobSubmitRequest(
        logical_plan=b"\x00\x01\x02", python_version="3.11.9"
    )
    native.partition_sets["k"] = b"\x04\x00\x00\x00"
    native.udfs.append(descriptor)
    native.udf_artifact_ids.extend(["sha256:abc"])
    assert runtime_pb2.JobSubmitRequest.FromString(native.SerializeToString()) == native

    worker = worker_pb2.WorkerRequest(
        request_id=7,
        execute=worker_pb2.ExecutePlanRequest(logical_plan=b"p"),
    )
    assert worker_pb2.WorkerRequest.FromString(worker.SerializeToString()) == worker

    envelope = worker_pb2.WorkerEnvelope(
        hello=worker_pb2.WorkerHello(
            pid=1234, python_version="3.11.9", daft_version="0.4.0"
        )
    )
    assert (
        worker_pb2.WorkerEnvelope.FromString(envelope.SerializeToString()) == envelope
    )

    task = distributed_pb2.TaskDefinition(
        job_id="job-1",
        stage_id=2,
        task_id=3,
        logical_plan=b"\x0a\x02\x08\x01",
        partition_idx=0,
        num_partitions=4,
    )
    assert (
        distributed_pb2.TaskDefinition.FromString(task.SerializeToString()) == task
    )

    print(f"Regenerated {len(PROTO_FILES)} proto files with {protoc}")
    print(f"Compatible with google.protobuf {google.protobuf.__version__}")
    print(f"Output: {args.out_dir}")


if __name__ == "__main__":
    main()
