//! Standalone Daft runtime server (single-machine, no distributed workers).
//!
//! The binary serves the HTTP control plane from Rust. Every endpoint speaks
//! the protobuf wire protocol defined in the ``daft-protocol`` crate
//! (``application/x-protobuf``); JSON is never used on the wire. Plans that do
//! **not** contain Python UDFs are deserialized and executed entirely by the
//! pure-Rust execution engine (``daft-local-execution``); plans that *do*
//! contain Python UDFs are handed to a short-lived Python worker subprocess
//! (see ``python_worker``) which owns the interpreter needed to
//! unpickle/run cloudpickled UDFs. Results are always returned as an Arrow IPC
//! envelope to the Python client.

#![allow(clippy::too_many_arguments)]

use std::{
    collections::HashMap,
    net::SocketAddr,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use axum::{
    Router,
    body::Bytes,
    extract::{Path, Request, State},
    http::{StatusCode, header},
    middleware::{self, Next},
    response::Response,
    routing::{get, post},
};
use daft_protocol::{
    daft::v1::{
        Error as ProtoError, JobState as ProtoJobState, JobStatus, JobSubmitRequest,
        JobSubmitResponse, JobResult, UdfArtifact, UdfArtifactMetadata,
        UploadUdfArtifactRequest, UploadUdfArtifactResponse, WorkerInfo, WorkerList,
    },
    encode,
};
use prost::Message;
use sha2::{Digest, Sha256};
use tokio::sync::RwLock;
use uuid::Uuid;

mod native;
mod python_worker;

const PROTOBUF_CONTENT_TYPE: &str = "application/x-protobuf";

/// Internal job lifecycle state (mirrors ``daft.v1.JobState``).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum JobState {
    Pending,
    Running,
    Succeeded,
    Failed,
    Canceled,
}

impl JobState {
    fn as_proto(self) -> ProtoJobState {
        match self {
            Self::Pending => ProtoJobState::Pending,
            Self::Running => ProtoJobState::Running,
            Self::Succeeded => ProtoJobState::Succeeded,
            Self::Failed => ProtoJobState::Failed,
            Self::Canceled => ProtoJobState::Canceled,
        }
    }
}

#[derive(Clone, Debug)]
struct StoredUdfArtifact {
    metadata: UdfArtifactMetadata,
    payload: Arc<[u8]>,
}

#[derive(Clone, Debug)]
struct JobRecord {
    job_id: Uuid,
    state: JobState,
    error: Option<String>,
    result: Option<Arc<[u8]>>,
}

impl JobRecord {
    fn status(&self) -> JobStatus {
        JobStatus {
            job_id: self.job_id.to_string(),
            state: self.state.as_proto() as i32,
            error: self.error.clone().unwrap_or_default(),
        }
    }
}

#[derive(Default, Clone)]
pub struct RuntimeState {
    jobs: Arc<Mutex<HashMap<Uuid, JobRecord>>>,
    workers: Arc<RwLock<HashMap<String, WorkerInfo>>>,
    udf_artifacts: Arc<RwLock<HashMap<String, StoredUdfArtifact>>>,
    token: Option<String>,
}

impl RuntimeState {
    pub fn with_token(token: Option<String>) -> Self {
        Self {
            token,
            ..Default::default()
        }
    }
}

impl RuntimeState {
    pub async fn submit(&self, request: JobSubmitRequest) -> Result<JobSubmitResponse, String> {
        let plan_bytes = request.logical_plan;

        // Snapshot artifact (filename, payload) pairs now so the executor
        // thread does not need to hold the async lock. The filename decides
        // how the artifact is materialized on disk so the Python worker can
        // import it (see `resolve_artifact_filename`).
        let mut artifact_files: Vec<(String, Vec<u8>)> =
            Vec::with_capacity(request.udf_artifact_ids.len());
        {
            let artifacts = self.udf_artifacts.read().await;
            for artifact_id in &request.udf_artifact_ids {
                let artifact = artifacts
                    .get(artifact_id)
                    .ok_or_else(|| format!("unknown UDF artifact {artifact_id}"))?;
                artifact_files.push((
                    resolve_artifact_filename(&artifact.metadata),
                    artifact.payload.to_vec(),
                ));
            }
        }

        let id = Uuid::new_v4();
        self.jobs.lock().unwrap().insert(
            id,
            JobRecord {
                job_id: id,
                state: JobState::Pending,
                error: None,
                result: None,
            },
        );

        let state = self.clone();
        let udfs = request.udfs;
        let python_version = request.python_version;
        let partition_sets = request.partition_sets;
        let artifact_ids = request.udf_artifact_ids.clone();
        std::thread::spawn(move || {
            state.execute_job(
                id,
                plan_bytes,
                partition_sets,
                udfs,
                python_version,
                artifact_ids,
                artifact_files,
            );
        });
        Ok(JobSubmitResponse {
            job_id: id.to_string(),
        })
    }

    /// Run a submitted job to completion on a dedicated thread.
    fn execute_job(
        &self,
        id: Uuid,
        plan_bytes: Vec<u8>,
        partition_sets: HashMap<String, Vec<u8>>,
        udfs: Vec<daft_protocol::daft::v1::UdfDescriptor>,
        python_version: String,
        artifact_ids: Vec<String>,
        artifact_files: Vec<(String, Vec<u8>)>,
    ) {
        {
            let mut jobs = self.jobs.lock().unwrap();
            if let Some(record) = jobs.get_mut(&id) {
                record.state = JobState::Running;
            }
        }
        let execution = (|| {
            let artifact_dir = if artifact_ids.is_empty() {
                None
            } else {
                Some(materialize_artifacts(id, &artifact_files)?)
            };
            let extra_paths = artifact_dir
                .iter()
                .map(|dir| dir.to_string_lossy().into_owned())
                .collect();
            let result = if udfs.is_empty() {
                native::execute_plan_native(plan_bytes, partition_sets)
            } else {
                python_worker::execute_plan_with_python_worker(
                    plan_bytes,
                    partition_sets,
                    extra_paths,
                    udfs,
                    python_version,
                )
            };
            // Best-effort cleanup of the per-job artifact staging directory.
            if let Some(dir) = artifact_dir {
                let _ = std::fs::remove_dir_all(&dir);
            }
            result
        })();

        let mut jobs = self.jobs.lock().unwrap();
        let record = jobs.get_mut(&id).expect("job record exists while running");
        match execution {
            Ok(result) => {
                record.state = JobState::Succeeded;
                record.result = Some(result.into());
            }
            Err(error) => {
                record.state = JobState::Failed;
                record.error = Some(error);
            }
        }
    }

    pub async fn status(&self, id: Uuid) -> Option<JobStatus> {
        self.jobs.lock().unwrap().get(&id).map(JobRecord::status)
    }

    pub async fn cancel(&self, id: Uuid) -> Option<JobStatus> {
        let mut jobs = self.jobs.lock().unwrap();
        let job = jobs.get_mut(&id)?;
        if job.state == JobState::Pending {
            job.state = JobState::Canceled;
        }
        Some(job.status())
    }

    pub async fn result(&self, id: Uuid) -> Option<Result<Arc<[u8]>, String>> {
        let jobs = self.jobs.lock().unwrap();
        let record = jobs.get(&id)?;
        match record.state {
            JobState::Succeeded => Some(
                record
                    .result
                    .clone()
                    .ok_or_else(|| "job succeeded without a result".to_string()),
            ),
            JobState::Failed => Some(Err(record
                .error
                .clone()
                .unwrap_or_else(|| "job failed".to_string()))),
            _ => None,
        }
    }

    pub async fn register_worker(&self, worker: WorkerInfo) {
        self.workers
            .write()
            .await
            .insert(worker.worker_id.clone(), worker);
    }

    pub async fn workers(&self) -> Vec<WorkerInfo> {
        self.workers.read().await.values().cloned().collect()
    }

    pub async fn upload_udf_artifact(
        &self,
        request: UploadUdfArtifactRequest,
    ) -> Result<UdfArtifactMetadata, String> {
        let payload = request.payload;
        let request_metadata = request.metadata.unwrap_or_default();
        let sha256 = format!("{:x}", Sha256::digest(&payload));
        let artifact_id = format!("sha256:{sha256}");
        let metadata = UdfArtifactMetadata {
            artifact_id: artifact_id.clone(),
            runtime: request_metadata.runtime,
            entrypoint: request_metadata.entrypoint,
            python_version: request_metadata.python_version,
            requirements: request_metadata.requirements,
            size_bytes: payload.len() as u64,
            sha256,
            filename: request_metadata.filename,
        };
        self.udf_artifacts.write().await.insert(
            artifact_id,
            StoredUdfArtifact {
                metadata: metadata.clone(),
                payload: payload.into(),
            },
        );
        Ok(metadata)
    }

    pub async fn udf_artifact(
        &self,
        artifact_id: &str,
        include_payload: bool,
    ) -> Option<UdfArtifact> {
        let artifacts = self.udf_artifacts.read().await;
        let artifact = artifacts.get(artifact_id)?;
        Some(UdfArtifact {
            metadata: Some(artifact.metadata.clone()),
            payload: include_payload.then(|| artifact.payload.to_vec()).unwrap_or_default(),
        })
    }
}

/// Build a protobuf HTTP response with ``application/x-protobuf`` content type.
fn proto_response<M: Message>(status: StatusCode, message: &M) -> Response {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, PROTOBUF_CONTENT_TYPE)
        .body(axum::body::Body::from(encode(message)))
        .expect("valid response")
}

async fn submit(
    State(state): State<RuntimeState>,
    body: Bytes,
) -> Response {
    let request = match JobSubmitRequest::decode(body) {
        Ok(request) => request,
        Err(e) => {
            return proto_response(
                StatusCode::BAD_REQUEST,
                &ProtoError {
                    message: format!("invalid protobuf request: {e}"),
                },
            );
        }
    };
    match state.submit(request).await {
        Ok(response) => proto_response(StatusCode::ACCEPTED, &response),
        Err(e) => proto_response(StatusCode::BAD_REQUEST, &ProtoError { message: e }),
    }
}

async fn status(
    Path(id): Path<Uuid>,
    State(state): State<RuntimeState>,
) -> Response {
    state
        .status(id)
        .await
        .map(|status| proto_response(StatusCode::OK, &status))
        .unwrap_or_else(|| {
            proto_response(
                StatusCode::NOT_FOUND,
                &ProtoError {
                    message: "unknown job".to_string(),
                },
            )
        })
}

async fn result(
    Path(id): Path<Uuid>,
    State(state): State<RuntimeState>,
) -> Response {
    match state.result(id).await {
        Some(Ok(bytes)) => proto_response(
            StatusCode::OK,
            &JobResult {
                payload: bytes.to_vec(),
            },
        ),
        Some(Err(error)) => proto_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            &ProtoError { message: error },
        ),
        None => proto_response(
            StatusCode::CONFLICT,
            &ProtoError {
                message: "job is not finished".to_string(),
            },
        ),
    }
}

async fn cancel(
    Path(id): Path<Uuid>,
    State(state): State<RuntimeState>,
) -> Response {
    state
        .cancel(id)
        .await
        .map(|status| proto_response(StatusCode::OK, &status))
        .unwrap_or_else(|| {
            proto_response(
                StatusCode::NOT_FOUND,
                &ProtoError {
                    message: "unknown job".to_string(),
                },
            )
        })
}

async fn workers(State(state): State<RuntimeState>) -> Response {
    let workers = WorkerList {
        workers: state.workers().await,
    };
    proto_response(StatusCode::OK, &workers)
}

async fn upload_udf_artifact(
    State(state): State<RuntimeState>,
    body: Bytes,
) -> Response {
    let request = match UploadUdfArtifactRequest::decode(body) {
        Ok(request) => request,
        Err(e) => {
            return proto_response(
                StatusCode::BAD_REQUEST,
                &ProtoError {
                    message: format!("invalid protobuf request: {e}"),
                },
            );
        }
    };
    match state.upload_udf_artifact(request).await {
        Ok(metadata) => proto_response(
            StatusCode::CREATED,
            &UploadUdfArtifactResponse {
                metadata: Some(metadata),
            },
        ),
        Err(e) => proto_response(StatusCode::BAD_REQUEST, &ProtoError { message: e }),
    }
}

async fn udf_artifact_metadata(
    Path(artifact_id): Path<String>,
    State(state): State<RuntimeState>,
) -> Response {
    state
        .udf_artifact(&artifact_id, false)
        .await
        .map(|artifact| proto_response(StatusCode::OK, &artifact))
        .unwrap_or_else(|| {
            proto_response(
                StatusCode::NOT_FOUND,
                &ProtoError {
                    message: "unknown artifact".to_string(),
                },
            )
        })
}

async fn download_udf_artifact(
    Path(artifact_id): Path<String>,
    State(state): State<RuntimeState>,
) -> Response {
    state
        .udf_artifact(&artifact_id, true)
        .await
        .map(|artifact| proto_response(StatusCode::OK, &artifact))
        .unwrap_or_else(|| {
            proto_response(
                StatusCode::NOT_FOUND,
                &ProtoError {
                    message: "unknown artifact".to_string(),
                },
            )
        })
}

async fn authorize(
    State(state): State<RuntimeState>,
    request: Request,
    next: Next,
) -> Response {
    if let Some(expected) = &state.token {
        let provided = request
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok());
        if provided != Some(&format!("Bearer {expected}")) {
            return proto_response(
                StatusCode::UNAUTHORIZED,
                &ProtoError {
                    message: "unauthorized".to_string(),
                },
            );
        }
    }
    next.run(request).await
}

pub fn router(state: RuntimeState) -> Router {
    let auth_state = state.clone();
    Router::new()
        .route("/v1/jobs", post(submit))
        .route("/v1/jobs/{id}", get(status).delete(cancel))
        .route("/v1/jobs/{id}/result", get(result))
        .route("/v1/workers", get(workers))
        .route("/v1/udf-artifacts", post(upload_udf_artifact))
        .route(
            "/v1/udf-artifacts/{artifact_id}",
            get(udf_artifact_metadata),
        )
        .route(
            "/v1/udf-artifacts/{artifact_id}/payload",
            get(download_udf_artifact),
        )
        .with_state(state)
        .layer(middleware::from_fn_with_state(auth_state, authorize))
}

pub async fn serve(addr: SocketAddr, token: Option<String>) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, router(RuntimeState::with_token(token))).await
}

/// Write UDF artifact payloads to a per-job temp directory and return its path.
///
/// Each artifact is written under its (sanitized) filename, so the directory
/// can be prepended to the Python worker's ``sys.path`` and the artifact
/// imported by name (module, zip, or wheel).
fn materialize_artifacts(job_id: Uuid, artifacts: &[(String, Vec<u8>)]) -> Result<PathBuf, String> {
    // Reject duplicate filenames before creating anything, so a mislabeled
    // artifact set can never silently overwrite one file with another.
    let mut seen = std::collections::HashSet::new();
    for (filename, _) in artifacts {
        let name = sanitize_artifact_filename(filename);
        if !seen.insert(name.clone()) {
            return Err(format!("duplicate artifact filename in job: {name}"));
        }
    }
    let dir = std::env::temp_dir().join(format!("daft-runtime-{job_id}"));
    if let Err(e) = std::fs::create_dir_all(&dir) {
        return Err(format!("failed to create artifact dir: {e}"));
    }
    for (filename, payload) in artifacts {
        let name = sanitize_artifact_filename(filename);
        let path = dir.join(&name);
        if let Err(e) = std::fs::write(&path, payload) {
            let _ = std::fs::remove_dir_all(&dir);
            return Err(format!("failed to write artifact {name}: {e}"));
        }
    }
    Ok(dir)
}

/// Choose the basename under which an uploaded artifact is materialized.
///
/// Prefers the client-supplied ``filename``; falls back to deriving
/// ``<module>.py`` from the ``entrypoint`` ("module:function" or "module");
/// finally falls back to a content-addressed name.
fn resolve_artifact_filename(metadata: &UdfArtifactMetadata) -> String {
    let explicit = metadata.filename.trim();
    if !explicit.is_empty() {
        return explicit.to_string();
    }
    let module = metadata
        .entrypoint
        .split(':')
        .next()
        .unwrap_or_default()
        .trim();
    if !module.is_empty() {
        // A dotted module cannot be represented by a single ``.py`` file, so
        // flatten it to an importable single-file name.
        return format!("{}.py", module.replace('.', "_"));
    }
    format!("artifact-{}.py", metadata.sha256)
}

/// Make a client-supplied filename safe for use inside the staging directory.
fn sanitize_artifact_filename(filename: &str) -> String {
    let base = filename
        .rsplit(['/', '\\'])
        .next()
        .unwrap_or_default()
        .trim();
    if base.is_empty() || base == "." || base == ".." {
        "artifact.bin".to_string()
    } else {
        base.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use daft_protocol::daft::v1::UdfRuntime;

    #[tokio::test]
    async fn stores_udf_artifacts_by_content_digest() {
        let state = RuntimeState::default();
        let metadata = state
            .upload_udf_artifact(UploadUdfArtifactRequest {
                metadata: Some(UdfArtifactMetadata {
                    artifact_id: String::new(),
                    runtime: UdfRuntime::Python as i32,
                    entrypoint: "module:function".to_string(),
                    python_version: "3.11".to_string(),
                    requirements: vec!["numpy==2.2.6".to_string()],
                    size_bytes: 0,
                    sha256: String::new(),
                    filename: "my_module.py".to_string(),
                }),
                payload: b"artifact".to_vec(),
            })
            .await
            .unwrap();
        assert!(metadata.artifact_id.starts_with("sha256:"));
        assert_eq!(metadata.filename, "my_module.py");
        assert_eq!(
            state
                .udf_artifact(&metadata.artifact_id, true)
                .await
                .unwrap()
                .payload,
            b"artifact".to_vec()
        );
    }

    #[tokio::test]
    async fn submit_and_status_round_trip() {
        let state = RuntimeState::default();
        let response = state
            .submit(JobSubmitRequest {
                logical_plan: vec![1, 2, 3],
                partition_sets: Default::default(),
                udfs: Vec::new(),
                udf_artifact_ids: Vec::new(),
                python_version: String::new(),
            })
            .await
            .unwrap();
        let id: Uuid = response.job_id.parse().unwrap();
        let status = state.status(id).await.unwrap();
        assert_eq!(status.job_id, response.job_id);
        assert!(matches!(
            status.state,
            x if x == ProtoJobState::Pending as i32
                || x == ProtoJobState::Running as i32
                || x == ProtoJobState::Succeeded as i32
                || x == ProtoJobState::Failed as i32
        ));
    }

    #[test]
    fn internal_state_maps_to_proto() {
        assert_eq!(JobState::Pending.as_proto(), ProtoJobState::Pending);
        assert_eq!(JobState::Running.as_proto(), ProtoJobState::Running);
        assert_eq!(JobState::Succeeded.as_proto(), ProtoJobState::Succeeded);
        assert_eq!(JobState::Failed.as_proto(), ProtoJobState::Failed);
        assert_eq!(JobState::Canceled.as_proto(), ProtoJobState::Canceled);
    }

    #[test]
    fn content_type_helpers() {
        assert_eq!(PROTOBUF_CONTENT_TYPE, "application/x-protobuf");
    }

    #[test]
    fn artifact_filename_resolution() {
        let metadata = |entrypoint: &str, filename: &str| UdfArtifactMetadata {
            artifact_id: String::new(),
            runtime: UdfRuntime::Python as i32,
            entrypoint: entrypoint.to_string(),
            python_version: String::new(),
            requirements: Vec::new(),
            size_bytes: 0,
            sha256: "abc".to_string(),
            filename: filename.to_string(),
        };
        // Explicit filename wins.
        assert_eq!(
            resolve_artifact_filename(&metadata("mod:fn", "bundle.zip")),
            "bundle.zip"
        );
        // Derived from the entrypoint module.
        assert_eq!(
            resolve_artifact_filename(&metadata("mod:fn", "")),
            "mod.py"
        );
        // Dotted modules flatten to an importable single-file name.
        assert_eq!(
            resolve_artifact_filename(&metadata("pkg.mod:fn", "")),
            "pkg_mod.py"
        );
        // No entrypoint: content-addressed fallback.
        assert_eq!(
            resolve_artifact_filename(&metadata("", "")),
            "artifact-abc.py"
        );
    }

    #[test]
    fn artifact_filename_sanitization() {
        assert_eq!(sanitize_artifact_filename("mod.py"), "mod.py");
        assert_eq!(
            sanitize_artifact_filename("../evil.py"),
            "evil.py"
        );
        assert_eq!(
            sanitize_artifact_filename("a/b/mod.py"),
            "mod.py"
        );
        assert_eq!(sanitize_artifact_filename(".."), "artifact.bin");
        assert_eq!(sanitize_artifact_filename(""), "artifact.bin");
    }

    #[test]
    fn duplicate_artifact_filenames_rejected() {
        let id = Uuid::new_v4();
        let artifacts = vec![
            ("helper_mod.py".to_string(), b"a".to_vec()),
            ("helper_mod.py".to_string(), b"b".to_vec()),
        ];
        let err = materialize_artifacts(id, &artifacts).unwrap_err();
        assert!(err.contains("duplicate artifact filename"), "{err}");
        // No partial staging directory may be left behind.
        let dir = std::env::temp_dir().join(format!("daft-runtime-{id}"));
        assert!(!dir.exists());
    }
}
