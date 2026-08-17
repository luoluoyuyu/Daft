from __future__ import annotations

import struct
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import TYPE_CHECKING

import google.protobuf.message

from daft.runtime.daft_proto.daft_runtime_proto.v1 import runtime_pb2
from daft.runtime.executor import decode_partitions
from daft.runtime.protocol import (
    PROTOBUF_CONTENT_TYPE,
    JobState,
    JobStatus,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from daft.recordbatch import MicroPartition
    from daft.plan_transport import PlanExecution


class RuntimeError(Exception):
    pass


class RuntimeClient:
    def __init__(self, endpoint: str, *, token: str | None = None, timeout_s: float = 30.0) -> None:
        self.endpoint = endpoint.rstrip("/")
        self.token = token
        self.timeout_s = timeout_s

    def _request(
        self,
        method: str,
        path: str,
        payload: object | None = None,
    ) -> bytes:
        headers = {"Accept": PROTOBUF_CONTENT_TYPE}
        data = None
        if payload is not None:
            data = payload.SerializeToString()
            headers["Content-Type"] = PROTOBUF_CONTENT_TYPE
        if self.token is not None:
            headers["Authorization"] = f"Bearer {self.token}"
        request = urllib.request.Request(f"{self.endpoint}{path}", data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_s) as response:
                return response.read()
        except urllib.error.HTTPError as e:
            body = e.read()
            raise RuntimeError(
                f"Runtime request failed ({e.code}): {_decode_error_body(body)}"
            ) from e
        except urllib.error.URLError as e:
            raise RuntimeError(f"Unable to reach Daft runtime at {self.endpoint}: {e.reason}") from e

    def submit(
        self,
        plan: bytes,
        *,
        execution: PlanExecution,
        partition_sets: dict[str, bytes] | None = None,
        udf_artifact_ids: Sequence[str] = (),
    ) -> Job:
        """Submit a serialized plan to the runtime.

        ``execution`` is the client-side declaration computed by
        :func:`daft.plan_transport.serialize_plan_parts`: it selects the
        execution path via the parsed ``UdfDescriptor`` messages it carries
        (empty list => pure-Rust ``native``, non-empty => Python worker) and
        pins the interpreter version for UDF plans. ``partition_sets`` carries
        the in-memory inputs as raw Arrow IPC blobs keyed by partition-set
        cache key. ``udf_artifact_ids`` lists content-addressed UDF artifacts
        previously uploaded with :meth:`upload_udf_artifact`.
        """
        request = runtime_pb2.JobSubmitRequest(
            logical_plan=plan,
            python_version=execution.python_version,
        )
        request.udfs.extend(execution.udfs)
        request.udf_artifact_ids.extend(udf_artifact_ids)
        for key, blob in (partition_sets or {}).items():
            request.partition_sets[key] = blob
        response = runtime_pb2.JobSubmitResponse()
        response.ParseFromString(self._request("POST", "/v1/jobs", request))
        return Job(response.job_id, self)

    def submit_sql(
        self,
        sql: str,
        bindings: dict[str, bytes],
        *,
        partition_sets: dict[str, bytes] | None = None,
    ) -> Job:
        """Submit a SQL statement plus named DataFrame bindings to the runtime.

        The SQL text is parsed *on the server*: ``bindings`` maps the table
        names visible to the statement to their serialized logical plans
        (``LogicalPlanBuilder.to_bytes()``). SQL runs on the pure-Rust engine
        only (no Python UDFs yet), and ``partition_sets`` carries in-memory
        inputs as Arrow IPC blobs.
        """
        request = runtime_pb2.SqlSubmitRequest(
            sql=sql,
        )
        for name, plan in bindings.items():
            request.bindings[name] = plan
        for key, blob in (partition_sets or {}).items():
            request.partition_sets[key] = blob
        response = runtime_pb2.JobSubmitResponse()
        response.ParseFromString(self._request("POST", "/v1/sql", request))
        return Job(response.job_id, self)

    def status(self, job_id: str) -> JobStatus:
        status = runtime_pb2.JobStatus()
        status.ParseFromString(self._request("GET", f"/v1/jobs/{job_id}"))
        return JobStatus.from_proto(status)

    def cancel(self, job_id: str) -> JobStatus:
        status = runtime_pb2.JobStatus()
        status.ParseFromString(self._request("DELETE", f"/v1/jobs/{job_id}"))
        return JobStatus.from_proto(status)

    def result(self, job_id: str) -> list[MicroPartition]:
        result = runtime_pb2.JobResult()
        result.ParseFromString(self._request("GET", f"/v1/jobs/{job_id}/result"))
        return decode_partitions(result.payload)

    def result_stream(self, job_id: str):
        """Yield final task partitions as they become available.

        The response is a length-prefixed protobuf stream. Each
        ``JobResultChunk`` contains one task's DAFTRES1 payload; task chunks
        are emitted in partition order by the manager. The stream is bounded
        server-side, so a slow consumer applies backpressure to final-task
        status delivery.
        """
        headers = {"Accept": "application/x-protobuf-stream"}
        if self.token is not None:
            headers["Authorization"] = f"Bearer {self.token}"
        request = urllib.request.Request(
            f"{self.endpoint}/v1/jobs/{job_id}/result-stream",
            headers=headers,
            method="GET",
        )
        try:
            response = urllib.request.urlopen(request, timeout=self.timeout_s)
        except urllib.error.HTTPError as e:
            raise RuntimeError(
                f"Runtime result stream failed ({e.code}): {_decode_error_body(e.read())}"
            ) from e
        # urllib applies the timeout only while connecting; propagate it to
        # the underlying socket so a stalled manager/worker stream cannot
        # leave the iterator blocked forever.
        raw_socket = getattr(getattr(response, "fp", None), "raw", None)
        raw_socket = getattr(raw_socket, "_sock", None)
        if raw_socket is not None:
            raw_socket.settimeout(self.timeout_s)
        with response:
            while True:
                prefix = response.read(4)
                if not prefix:
                    return
                if len(prefix) != 4:
                    raise RuntimeError("truncated result stream length prefix")
                (length,) = struct.unpack("<I", prefix)
                payload = response.read(length)
                if len(payload) != length:
                    raise RuntimeError("truncated result stream protobuf frame")
                chunk = runtime_pb2.JobResultChunk()
                chunk.ParseFromString(payload)
                if chunk.error:
                    raise RuntimeError(chunk.error)
                if chunk.end_of_stream:
                    return
                yield from decode_partitions(chunk.payload)

    def upload_udf_artifact(
        self,
        payload: bytes,
        *,
        entrypoint: str = "",
        python_version: str = "",
        requirements: Sequence[str] = (),
        filename: str | None = None,
    ) -> str:
        """Upload a UDF artifact and return its content-addressed artifact ID.

        The server computes ``sha256:...`` from the payload; the returned ID
        can be passed to :meth:`submit` via ``udf_artifact_ids``.

        ``filename`` is the basename under which the runtime materializes the
        payload so the Python worker can import it (a ``.py`` module, zip, or
        wheel). When omitted, the server derives ``<module>.py`` from
        ``entrypoint`` (the part before ``:``).
        """
        request = runtime_pb2.UploadUdfArtifactRequest(payload=payload)
        request.metadata.runtime = runtime_pb2.UDF_RUNTIME_PYTHON
        request.metadata.entrypoint = entrypoint
        request.metadata.python_version = python_version
        request.metadata.requirements.extend(requirements)
        if filename is not None:
            request.metadata.filename = filename
        response = runtime_pb2.UploadUdfArtifactResponse()
        response.ParseFromString(
            self._request("POST", "/v1/udf-artifacts", request)
        )
        return response.metadata.artifact_id

    def udf_artifact_metadata(self, artifact_id: str) -> runtime_pb2.UdfArtifactMetadata:
        """Return stored metadata for a content-addressed UDF artifact."""
        artifact = runtime_pb2.UdfArtifact()
        artifact.ParseFromString(
            self._request("GET", f"/v1/udf-artifacts/{artifact_id}")
        )
        return artifact.metadata

    def download_udf_artifact(self, artifact_id: str) -> bytes:
        """Download the payload of a content-addressed UDF artifact."""
        artifact = runtime_pb2.UdfArtifact()
        artifact.ParseFromString(
            self._request("GET", f"/v1/udf-artifacts/{artifact_id}/payload")
        )
        return artifact.payload


def _decode_error_body(body: bytes) -> str:
    """Best-effort decode of a server error body (protobuf ``Error`` or text)."""
    error = runtime_pb2.Error()
    try:
        consumed = error.ParseFromString(body)
        if consumed == len(body) and error.message:
            return error.message
    except google.protobuf.message.DecodeError:
        pass
    return body.decode("utf-8", errors="replace")


@dataclass(frozen=True)
class Job:
    job_id: str
    client: RuntimeClient

    def status(self) -> JobStatus:
        return self.client.status(self.job_id)

    def cancel(self) -> JobStatus:
        return self.client.cancel(self.job_id)

    def wait(self, *, poll_interval_s: float = 0.05, timeout_s: float | None = None) -> JobStatus:
        started = time.monotonic()
        while True:
            status = self.status()
            if status.state in {JobState.SUCCEEDED, JobState.FAILED, JobState.CANCELED}:
                if status.state == JobState.FAILED:
                    raise RuntimeError(status.error or f"Job {self.job_id} failed")
                return status
            if timeout_s is not None and time.monotonic() - started >= timeout_s:
                raise TimeoutError(f"Timed out waiting for Daft job {self.job_id}")
            time.sleep(poll_interval_s)

    def result(self) -> list[MicroPartition]:
        self.wait()
        return self.client.result(self.job_id)

    def iter_result_partitions(self):
        """Stream final partitions without waiting for the whole Job."""
        yield from self.client.result_stream(self.job_id)
