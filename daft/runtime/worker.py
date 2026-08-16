"""Per-job Python UDF worker subprocess launched by ``daft-runtime``.

The Rust runtime binary never embeds CPython. Plans that contain Python UDFs
are executed by this process. The server spawns one fresh worker per job,
sends it exactly one execute request carrying that job's parsed UDF
descriptors, closes stdin, and the worker exits once it has answered. The
descriptors apply only to this job; there is no worker pooling, reuse, or
management yet (that is intentionally deferred).

Protocol (both directions, 4-byte little-endian length prefix framing)::

    worker -> server : WorkerEnvelope{hello}   (handshake, once at startup)
    server -> worker : WorkerRequest{request_id, command}
    worker -> server : WorkerEnvelope{response{request_id, ...}}

The worker restores each plan and its in-memory inputs, executes it with the
native runner (the interpreter is already present in this process), and
returns the ``DAFTRES1`` result envelope or an error message. Commands also
include ping (health probe) and shutdown (graceful stop). JSON is never used
on this channel.
"""

from __future__ import annotations

import os
import platform
import sys
import traceback
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if REPO_ROOT.is_dir() and str(REPO_ROOT) not in sys.path:
    # ``python -m daft.runtime.worker`` may be launched from a cwd outside the
    # repository. When the interpreter has no editable install of ``daft``,
    # fall back to the package checkout itself (parent of ``daft/runtime``).
    sys.path.insert(0, str(REPO_ROOT))

import daft  # noqa: E402
from daft.runtime.daft_proto.daft_runtime_proto.v1 import worker_pb2  # noqa: E402
from daft.runtime.executor import execute_plan_bytes  # noqa: E402
from daft.runtime.protocol import (  # noqa: E402
    encode_length_prefixed,
    read_length_prefixed,
)


def _handle_request(request: worker_pb2.WorkerRequest) -> worker_pb2.WorkerResponse:
    """Run one command and build the matching response."""
    response = worker_pb2.WorkerResponse(request_id=request.request_id)
    command = request.WhichOneof("command")
    if command == "execute":
        execute = request.execute
        try:
            # The server already verified our interpreter against
            # ``python_version`` during the handshake; re-check cheaply here
            # because cloudpickled closures are interpreter-specific.
            if execute.python_version:
                worker_version = platform.python_version()
                if worker_version != execute.python_version:
                    raise RuntimeError(
                        f"Python worker interpreter mismatch: plan built with "
                        f"Python {execute.python_version}, worker runs "
                        f"{worker_version}"
                    )
            if len(execute.udfs) == 0 and execute.logical_plan:
                # Pure-Rust plans never reach the worker; this is defensive.
                raise RuntimeError(
                    "worker received a plan without any UDF descriptors"
                )
            result = execute_plan_bytes(
                execute.logical_plan,
                extra_paths=execute.extra_paths,
                partition_sets=dict(execute.partition_sets),
            )
            response.execute.result = result
        except Exception as e:  # noqa: BLE001 - report any failure to the parent
            response.execute.error = traceback.format_exc()
    elif command == "ping":
        response.pong.SetInParent()
    elif command == "shutdown":
        response.shutdown.SetInParent()
    else:
        response.execute.error = f"unknown worker command: {command!r}"
    return response


def main() -> int:
    hello = worker_pb2.WorkerEnvelope(
        hello=worker_pb2.WorkerHello(
            pid=os.getpid(),
            python_version=platform.python_version(),
            daft_version=daft.__version__,
        )
    )
    stdout = sys.stdout.buffer
    stdout.write(encode_length_prefixed(hello))
    stdout.flush()

    while True:
        try:
            payload = read_length_prefixed(sys.stdin.buffer)
        except EOFError:
            # The parent closed the pipe; exit cleanly.
            return 0
        request = worker_pb2.WorkerRequest()
        request.ParseFromString(payload)
        response = _handle_request(request)
        stdout.write(encode_length_prefixed(response))
        stdout.flush()
        if request.WhichOneof("command") == "shutdown":
            return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:  # noqa: BLE001 - report any failure to the parent
        # Report the failure over the protocol channel so the Rust runtime can
        # surface a structured error. Exit non-zero so the parent treats the
        # worker as unhealthy and restarts it.
        try:
            response = worker_pb2.WorkerEnvelope(
                response=worker_pb2.WorkerResponse(
                    request_id=0,
                    execute=worker_pb2.ExecutePlanResponse(
                        result=b"", error=traceback.format_exc()
                    ),
                )
            )
            sys.stdout.buffer.write(encode_length_prefixed(response))
            sys.stdout.buffer.flush()
        except Exception:  # noqa: BLE001 - channel itself is broken
            print(f"daft runtime worker failed: {e}", file=sys.stderr)
            traceback.print_exc()
        sys.exit(1)
