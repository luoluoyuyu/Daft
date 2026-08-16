//! Python UDF worker subprocess for plans that contain Python UDFs.
//!
//! The runtime binary itself never embeds CPython. When a job carries one or
//! more parsed [`UdfDescriptor`]s the server spawns
//! ``python -m daft.runtime.worker`` and exchanges one
//! ``daft.v1.WorkerRequest`` / ``daft.v1.WorkerResponse`` pair over the
//! subprocess's stdin/stdout. Each message is framed by a 4-byte little-endian
//! length prefix (see ``daft_protocol::encode_length_prefixed``).
//!
//! One worker process is spawned *per job* and carries only that job's
//! descriptor list: it is never reused across jobs and never outlives the
//! job. The server sends a single execute request, closes stdin, and the
//! worker exits cleanly once it has answered. There is intentionally no UDF
//! worker management (persistence, pooling, health-checked reuse) yet.
//!
//! The worker is a *pure UDF runtime*: it initializes the cloudpickled
//! descriptors it receives and executes the plan inside the interpreter it
//! already owns. The Rust server never unpickles or inspects UDF payloads and
//! never re-parses the logical plan to discover UDFs; the descriptors in
//! ``JobSubmitRequest.udfs`` (computed by the Python client) are forwarded
//! verbatim.
//!
//! Channel shape (both directions, length-prefixed protobuf)::
//!
//!     worker -> server : WorkerEnvelope{hello}              (startup handshake)
//!     server -> worker : WorkerRequest{request_id, execute}
//!     worker -> server : WorkerResponse{request_id, execute}

use std::{
    collections::HashMap,
    env,
    io::{Read, Write},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
};

use daft_protocol::{
    daft::v1::{
        worker_envelope, worker_request, worker_response, ExecutePlanRequest, UdfDescriptor,
        WorkerEnvelope, WorkerHello, WorkerRequest, WorkerResponse,
    },
    encode_length_prefixed,
};
use prost::Message;

/// Execute a serialized plan containing Python UDFs in a Python worker
/// subprocess and return the ``DAFTRES1`` IPC result envelope.
///
/// ``udfs`` is the parsed UDF declaration computed by the Python client; the
/// worker initializes each descriptor before executing the plan. ``python_version``
/// is verified against the worker's own interpreter during the startup
/// handshake so a cloudpickled closure is never unpickled by the wrong Python.
///
/// This function spawns a *fresh* worker per call: the descriptors apply only
/// to this one job, and the worker process exits after the job finishes.
pub fn execute_plan_with_python_worker(
    plan_bytes: Vec<u8>,
    partition_sets: HashMap<String, Vec<u8>>,
    extra_paths: Vec<String>,
    udfs: Vec<UdfDescriptor>,
    python_version: String,
) -> Result<Vec<u8>, String> {
    let python = resolve_python().ok_or_else(|| {
        "no Python interpreter found; set DAFT_RUNTIME_PYTHON or add python3/python to PATH"
            .to_string()
    })?;

    let request = WorkerRequest {
        request_id: 1,
        command: Some(worker_request::Command::Execute(ExecutePlanRequest {
            logical_plan: plan_bytes,
            partition_sets,
            extra_paths,
            udfs,
            python_version: python_version.clone(),
        })),
    };
    let request_bytes = encode_length_prefixed(&request);

    let mut child = Command::new(&python)
        .args(["-m", "daft.runtime.worker"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("failed to spawn Python worker ({python:?}): {e}"))?;

    // 1. Handshake: the worker writes WorkerEnvelope{hello} immediately after
    //    startup. Verify the interpreter version before sending any work so a
    //    cloudpickled closure is never handed to the wrong Python.
    let hello = read_worker_hello(&mut child)?;
    if !python_version.is_empty() && !hello.python_version.is_empty() {
        if hello.python_version != python_version {
            // Kill the mismatched worker and report a structured error.
            let _ = child.kill();
            let _ = child.wait();
            return Err(format!(
                "Python worker interpreter mismatch: plan built with Python \
                 {python_version}, worker runs {}",
                hello.python_version
            ));
        }
    }

    // 2. Send the execute request, then close stdin. The worker answers once
    //    and exits cleanly on the resulting EOF.
    {
        let stdin = child
            .stdin
            .as_mut()
            .ok_or("failed to open Python worker stdin")?;
        stdin
            .write_all(&request_bytes)
            .map_err(|e| format!("failed to write worker request: {e}"))?;
        stdin
            .flush()
            .map_err(|e| format!("failed to flush worker request: {e}"))?;
    }
    drop(child.stdin.take());

    // 3. Read the response frame.
    let response = read_worker_response(&mut child)?;
    let execute = match response.result {
        Some(worker_response::Result::Execute(execute)) => execute,
        other => {
            let _ = child.kill();
            let _ = child.wait();
            return Err(format!(
                "Python worker did not answer with an execute response: {other:?}"
            ));
        }
    };

    // 4. Reap the child; surface any interpreter-side failure (stderr) even if
    //    a response frame was produced.
    let status = child
        .wait()
        .map_err(|e| format!("failed to wait for Python worker: {e}"))?;
    if !status.success() {
        let stderr = read_child_stderr(&mut child);
        return Err(format!(
            "Python worker failed (exit {:?}): {}",
            status.code(),
            stderr.trim()
        ));
    }

    if !execute.error.is_empty() {
        return Err(format!("Python worker error: {}", execute.error));
    }
    if execute.result.is_empty() {
        return Err("Python worker produced an empty result".to_string());
    }
    Ok(execute.result)
}

/// Read the worker's startup handshake frame from stdout.
fn read_worker_hello(child: &mut Child) -> Result<WorkerHello, String> {
    let stdout = child
        .stdout
        .as_mut()
        .ok_or("failed to open Python worker stdout")?;
    let frame = read_length_prefixed_frame(stdout).map_err(|e| {
        let stderr = read_child_stderr(child);
        format!("failed to read Python worker handshake: {e}; stderr: {stderr}")
    })?;
    let envelope = WorkerEnvelope::decode(frame.as_slice())
        .map_err(|e| format!("failed to decode worker handshake: {e}"))?;
    match envelope.payload {
        Some(worker_envelope::Payload::Hello(hello)) => Ok(hello),
        other => Err(format!(
            "expected WorkerEnvelope{{hello}} handshake, got {other:?}"
        )),
    }
}

/// Read exactly one length-prefixed `WorkerResponse` frame from stdout.
fn read_worker_response(child: &mut Child) -> Result<WorkerResponse, String> {
    let stdout = child
        .stdout
        .as_mut()
        .ok_or("failed to open Python worker stdout")?;
    let frame = read_length_prefixed_frame(stdout).map_err(|e| {
        let stderr = read_child_stderr(child);
        format!("failed to read Python worker response: {e}; stderr: {stderr}")
    })?;
    WorkerResponse::decode(frame.as_slice())
        .map_err(|e| format!("failed to decode Python worker response: {e}"))
}

/// Read one 4-byte-little-endian-length-prefixed protobuf frame.
///
/// Mirrors ``daft_protocol::decode_length_prefixed`` but reads incrementally
/// from a pipe so the worker's handshake and response can be consumed without
/// waiting for the process to exit.
fn read_length_prefixed_frame<R: Read>(reader: &mut R) -> Result<Vec<u8>, String> {
    let mut prefix = [0u8; 4];
    reader
        .read_exact(&mut prefix)
        .map_err(|e| format!("failed to read frame length prefix: {e}"))?;
    let len = u32::from_le_bytes(prefix) as usize;
    let mut payload = vec![0u8; len];
    reader
        .read_exact(&mut payload)
        .map_err(|e| format!("failed to read {len}-byte frame payload: {e}"))?;
    Ok(payload)
}

/// Drain the child's stderr pipe (after the child has exited).
fn read_child_stderr(child: &mut Child) -> String {
    child
        .stderr
        .as_mut()
        .map(|stderr| {
            let mut buf = String::new();
            let _ = stderr.read_to_string(&mut buf);
            buf
        })
        .unwrap_or_default()
}

/// Resolve the Python interpreter used for UDF execution.
fn resolve_python() -> Option<PathBuf> {
    if let Ok(python) = env::var("DAFT_RUNTIME_PYTHON") {
        let path = PathBuf::from(&python);
        if path.is_file() {
            return Some(path);
        }
        // Allow relative paths like `.venv/bin/python` resolved against the repo root.
        if path.is_relative()
            && let Some(repo_root) = repo_root()
            && let Some(from_repo) = path_if_file(repo_root.join(&python))
        {
            return Some(from_repo);
        }
    }
    if let Some(repo_root) = repo_root()
        && let Some(venv_python) = path_if_file(repo_root.join(".venv").join("bin").join("python"))
    {
        return Some(venv_python);
    }
    for candidate in ["python3", "python"] {
        if let Some(path) = which(candidate) {
            return Some(path);
        }
    }
    None
}

fn repo_root() -> Option<PathBuf> {
    let exe = env::current_exe().ok()?;
    exe.ancestors()
        .find(|dir| dir.join("daft").join("__init__.py").is_file())
        .map(Path::to_path_buf)
}

fn path_if_file(path: PathBuf) -> Option<PathBuf> {
    path.is_file().then_some(path)
}

fn which(candidate: &str) -> Option<PathBuf> {
    if Path::new(candidate).is_file() {
        return Some(PathBuf::from(candidate));
    }
    env::var("PATH")
        .ok()?
        .split(':')
        .filter(|dir| !dir.is_empty())
        .map(|dir| PathBuf::from(dir).join(candidate))
        .find(|path| path.is_file())
}
