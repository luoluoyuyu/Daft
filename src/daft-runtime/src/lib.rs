//! Daft runtime: distributed scheduler + HTTP control plane.
//!
//! The binary serves the HTTP control plane from Rust. Every endpoint speaks
//! the protobuf wire protocol defined in the ``daft-protocol`` crate
//! (``application/x-protobuf``); JSON is never used on the wire.
//!
//! Jobs (direct logical plans or SQL statements planned server-side) are
//! split by the scheduler ([`scheduler`]) into a DAG of stages at the
//! `Repartition`/`IntoPartitions` boundaries of the optimized logical plan,
//! then into one task per stage x partition slice. Executors
//! ([`executor`]) register over HTTP, poll for tasks, execute intermediate
//! stages with the pure-Rust engine (writing shuffle partitions over Arrow
//! Flight) and final stages natively or via a short-lived Python worker
//! subprocess when the job contains UDFs. Results are returned as an Arrow
//! IPC envelope to the Python client.

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
        poll_work_response, Error as ProtoError, ExecutorHeartbeat,
        ExecutorHeartbeatResponse, ExecutorRegistration, ExecutorRegistrationResponse,
        ExecutorTaskStatusResponse, DistributedJobStatus, JobState as ProtoJobState, JobStatus,
        JobSubmitRequest, JobSubmitResponse, JobResult, PollWorkRequest, PollWorkResponse,
        PurgeShuffle, SqlSubmitRequest, TaskStatus, UdfArtifact, UdfArtifactMetadata, UdfDescriptor,
        UploadUdfArtifactRequest, UploadUdfArtifactResponse, WorkerInfo, WorkerList,
    },
    encode,
};
use prost::Message;
use sha2::{Digest, Sha256};
use tokio::sync::RwLock;
use uuid::Uuid;

pub mod executor;
mod native;
mod python_worker;
mod scheduler;
mod sql;

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

/// Scheduler-side view of one registered executor.
#[derive(Clone, Debug)]
struct ExecutorInfo {
    registration: ExecutorRegistration,
    last_heartbeat_ms: u64,
}

#[derive(Default, Clone)]
pub struct RuntimeState {
    jobs: Arc<Mutex<HashMap<Uuid, JobRecord>>>,
    workers: Arc<RwLock<HashMap<String, WorkerInfo>>>,
    udf_artifacts: Arc<RwLock<HashMap<String, StoredUdfArtifact>>>,
    executors: Arc<Mutex<HashMap<String, ExecutorInfo>>>,
    distributed_jobs: Arc<Mutex<HashMap<Uuid, scheduler::DistributedJob>>>,
    /// Shuffle-cache purge instructions queued for executors, keyed by the
    /// executor's Flight address. Filled when a job reaches a terminal state
    /// (its intermediate shuffle caches are dead weight after that) and
    /// drained by [`RuntimeState::poll_work`], which piggybacks them onto the
    /// next poll response for the owning executor.
    pending_purges: Arc<Mutex<HashMap<String, Vec<PurgeShuffle>>>>,
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
    /// Submit a pre-built logical plan for distributed execution.
    ///
    /// The plan bytes (``daft.v1.LogicalPlan``), in-memory partition sets,
    /// parsed UDF descriptors, and UDF artifact files are snapshotted into a
    /// distributed job. The scheduler thread builds the stage DAG and fills
    /// the task queue; executors poll it from then on. No job is ever
    /// executed on the scheduler process.
    pub async fn submit(&self, request: JobSubmitRequest) -> Result<JobSubmitResponse, String> {
        let plan_bytes = request.logical_plan;
        let artifact_files = self.snapshot_artifacts(&request.udf_artifact_ids).await?;
        let id = self.create_job_record();
        let state = self.clone();
        let udfs = request.udfs;
        let python_version = request.python_version;
        let partition_sets = request.partition_sets;
        let artifact_ids = request.udf_artifact_ids;
        let artifact_files: HashMap<String, Vec<u8>> = artifact_files.into_iter().collect();
        std::thread::spawn(move || {
            state.schedule_job(
                id,
                plan_bytes,
                partition_sets,
                udfs,
                artifact_ids,
                artifact_files,
                python_version,
            );
        });
        Ok(JobSubmitResponse {
            job_id: id.to_string(),
        })
    }

    /// Submit a SQL statement for server-side parsing and execution.
    ///
    /// The client ships the raw statement text plus the serialized logical
    /// plans of its DataFrame bindings; parsing happens entirely in Rust (see
    /// [`crate::sql`]). The produced plan follows the exact same distributed
    /// scheduling path as a directly submitted plan. SQL does not support
    /// Python UDFs yet, so the job carries no descriptors or artifacts.
    pub async fn submit_sql(
        &self,
        request: SqlSubmitRequest,
    ) -> Result<JobSubmitResponse, String> {
        if request.sql.trim().is_empty() {
            return Err("SqlSubmitRequest.sql must not be empty".to_string());
        }
        let id = self.create_job_record();
        let state = self.clone();
        let partition_sets = request.partition_sets;
        let sql_text = request.sql;
        let bindings = request.bindings;
        std::thread::spawn(move || {
            let plan_bytes = match crate::sql::plan_sql_job(&sql_text, bindings) {
                Ok(bytes) => bytes,
                Err(error) => {
                    state.fail_job(id, error);
                    return;
                }
            };
            state.schedule_job(
                id,
                plan_bytes,
                partition_sets,
                Vec::new(),
                Vec::new(),
                HashMap::new(),
                String::new(),
            );
        });
        Ok(JobSubmitResponse {
            job_id: id.to_string(),
        })
    }

    /// Build the stage DAG for a submitted job and insert it into the
    /// scheduler state. Runs on a dedicated thread because plan optimization
    /// can be slow. On failure the durable job record is marked Failed.
    fn schedule_job(
        &self,
        id: Uuid,
        plan_bytes: Vec<u8>,
        partition_sets: HashMap<String, Vec<u8>>,
        udfs: Vec<UdfDescriptor>,
        artifact_ids: Vec<String>,
        artifact_files: HashMap<String, Vec<u8>>,
        python_version: String,
    ) {
        let result = scheduler::build_distributed_job(
            id,
            plan_bytes,
            partition_sets,
            udfs,
            artifact_ids,
            artifact_files,
            python_version,
        );
        eprintln!("[scheduler] build_distributed_job for {id}: {}", if result.is_ok() { "ok" } else { "error" });
        match result {
            Ok(job) => {
                eprintln!(
                    "[scheduler] job {id} inserted with {} stages",
                    job.num_stages()
                );
                let mut jobs = self.jobs.lock().unwrap();
                if let Some(record) = jobs.get_mut(&id) {
                    record.state = JobState::Running;
                }
                self.distributed_jobs.lock().unwrap().insert(id, job);
            }
            Err(error) => self.fail_job(id, error),
        }
    }

    /// Mark a durable job record Failed. Used when planning or scheduling
    /// fails before any task is dispatched.
    fn fail_job(&self, id: Uuid, error: String) {
        let mut jobs = self.jobs.lock().unwrap();
        if let Some(record) = jobs.get_mut(&id) {
            record.state = JobState::Failed;
            record.error = Some(error);
        }
    }

    /// Queue every shuffle cache this job wrote for purge on its holding
    /// executor. Called when the job reaches a terminal state (success or
    /// failure): the caches are no longer readable by any task, so keeping
    /// them would make the executor's Flight server accumulate partition
    /// files forever.
    fn enqueue_purges(&self, id: Uuid) {
        let purges = {
            let jobs = self.distributed_jobs.lock().unwrap();
            match jobs.get(&id) {
                Some(job) => job.purge_requests(),
                None => return,
            }
        };
        if purges.is_empty() {
            return;
        }
        let count = purges.len();
        let mut pending = self.pending_purges.lock().unwrap();
        for (shuffle_id, flight_address, cache_ids) in purges {
            pending
                .entry(flight_address)
                .or_default()
                .push(PurgeShuffle {
                    shuffle_id,
                    cache_ids,
                });
        }
        eprintln!(
            "[scheduler] enqueued {} purge requests for job {id}",
            count
        );
    }

    /// Snapshot the (filename, payload) pairs of the requested UDF artifacts
    /// so the executor thread does not need to hold the async lock. The
    /// filename decides how the artifact is materialized on disk so the
    /// Python worker can import it (see `resolve_artifact_filename`).
    async fn snapshot_artifacts(
        &self,
        artifact_ids: &[String],
    ) -> Result<Vec<(String, Vec<u8>)>, String> {
        let mut artifact_files: Vec<(String, Vec<u8>)> = Vec::with_capacity(artifact_ids.len());
        let artifacts = self.udf_artifacts.read().await;
        for artifact_id in artifact_ids {
            let artifact = artifacts
                .get(artifact_id)
                .ok_or_else(|| format!("unknown UDF artifact {artifact_id}"))?;
            artifact_files.push((
                resolve_artifact_filename(&artifact.metadata),
                artifact.payload.to_vec(),
            ));
        }
        Ok(artifact_files)
    }

    /// Create a new pending job record and return its id.
    fn create_job_record(&self) -> Uuid {
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
        id
    }

    /// Register an executor with the scheduler.
    pub async fn register_executor(
        &self,
        registration: ExecutorRegistration,
    ) -> ExecutorRegistrationResponse {
        if registration.worker_id.trim().is_empty() {
            return ExecutorRegistrationResponse {
                accepted: false,
                message: "worker_id must not be empty".to_string(),
            };
        }
        self.executors.lock().unwrap().insert(
            registration.worker_id.clone(),
            ExecutorInfo {
                registration,
                last_heartbeat_ms: now_ms(),
            },
        );
        ExecutorRegistrationResponse {
            accepted: true,
            message: "registered".to_string(),
        }
    }

    /// Update an executor's liveness timestamp.
    pub async fn executor_heartbeat(
        &self,
        heartbeat: ExecutorHeartbeat,
    ) -> ExecutorHeartbeatResponse {
        let mut executors = self.executors.lock().unwrap();
        match executors.get_mut(&heartbeat.worker_id) {
            Some(info) => {
                info.last_heartbeat_ms = heartbeat.timestamp_ms.max(now_ms());
                ExecutorHeartbeatResponse { ok: true }
            }
            None => ExecutorHeartbeatResponse { ok: false },
        }
    }

    /// Return one ready task for the requesting executor, or `no_work`.
    ///
    /// Stages become ready as their upstream shuffles complete; within a
    /// stage, tasks are dispatched in partition order. Jobs are scanned
    /// round-robin so a busy job cannot starve later ones.
    pub async fn poll_work(
        &self,
        request: PollWorkRequest,
    ) -> Result<PollWorkResponse, String> {
        // Drain this executor's queued purge instructions and piggyback them
        // onto its next response. Executors are identified by worker id on
        // the wire, but purges are keyed by Flight address, so resolve the
        // address from the registration first.
        let purge_shuffles = {
            let flight_address = self
                .executors
                .lock()
                .unwrap()
                .get(&request.worker_id)
                .map(|info| info.registration.flight_address.clone());
            match flight_address {
                Some(address) => self
                    .pending_purges
                    .lock()
                    .unwrap()
                    .remove(&address)
                    .unwrap_or_default(),
                None => Vec::new(),
            }
        };
        if !purge_shuffles.is_empty() {
            eprintln!(
                "[scheduler] handing {} purge requests to executor {}",
                purge_shuffles.len(),
                request.worker_id
            );
        }
        let mut jobs = self.distributed_jobs.lock().unwrap();
        for job in jobs.values_mut() {
            if let Some(task) = job.poll(&request.worker_id) {
                eprintln!(
                    "[scheduler] poll: dispatched task {}/{} to {}",
                    task.job_id, task.task_id, request.worker_id
                );
                return Ok(PollWorkResponse {
                    work: Some(poll_work_response::Work::Task(task)),
                    purge_shuffles,
                });
            }
        }
        Ok(PollWorkResponse {
            work: Some(poll_work_response::Work::NoWork(true)),
            purge_shuffles,
        })
    }

    /// Apply a task-status report to its distributed job and update the
    /// durable job record when the job reaches a terminal state.
    pub async fn report_task_status(
        &self,
        status: TaskStatus,
    ) -> Result<ExecutorTaskStatusResponse, String> {
        eprintln!(
            "[scheduler] task-status: job={} stage={} task={} state={} flight={} caches={:?} result={} bytes",
            status.job_id,
            status.stage_id,
            status.task_id,
            status.state,
            status.flight_address,
            status.cache_ids,
            status.result.len()
        );
        let job_id: Uuid = status
            .job_id
            .parse()
            .map_err(|_| format!("invalid job id {}", status.job_id))?;
        let outcome = {
            let mut jobs = self.distributed_jobs.lock().unwrap();
            let job = jobs
                .get_mut(&job_id)
                .ok_or_else(|| format!("unknown distributed job {job_id}"))?;
            job.on_task_status(&status)
        };
        let mut records = self.jobs.lock().unwrap();
        let record = records
            .get_mut(&job_id)
            .ok_or_else(|| format!("unknown job {job_id}"))?;
        match outcome {
            scheduler::JobOutcome::Succeeded(result) => {
                record.state = JobState::Succeeded;
                record.result = Some(result.into());
                self.enqueue_purges(job_id);
                self.distributed_jobs.lock().unwrap().remove(&job_id);
                Ok(ExecutorTaskStatusResponse {
                    accepted: true,
                    message: "job succeeded".to_string(),
                })
            }
            scheduler::JobOutcome::Failed(error) => {
                record.state = JobState::Failed;
                record.error = Some(error);
                self.enqueue_purges(job_id);
                self.distributed_jobs.lock().unwrap().remove(&job_id);
                Ok(ExecutorTaskStatusResponse {
                    accepted: true,
                    message: "job failed".to_string(),
                })
            }
            scheduler::JobOutcome::InProgress => Ok(ExecutorTaskStatusResponse {
                accepted: true,
                message: String::new(),
            }),
        }
    }

    pub async fn status(&self, id: Uuid) -> Option<JobStatus> {
        self.jobs.lock().unwrap().get(&id).map(JobRecord::status)
    }

    /// Scheduler-side introspection of a running distributed job: its stage
    /// count and how many tasks have completed so far.
    pub async fn distributed_job_status(&self, id: Uuid) -> Option<DistributedJobStatus> {
        let jobs = self.distributed_jobs.lock().unwrap();
        let job = jobs.get(&id)?;
        let state = match job.state {
            scheduler::DistJobState::Pending => ProtoJobState::Pending,
            scheduler::DistJobState::Running => ProtoJobState::Running,
            scheduler::DistJobState::Succeeded => ProtoJobState::Succeeded,
            scheduler::DistJobState::Failed => ProtoJobState::Failed,
        };
        Some(DistributedJobStatus {
            job_id: id.to_string(),
            state: state as i32,
            error: job.error.clone().unwrap_or_default(),
            num_stages: job.num_stages(),
            completed_tasks: job.completed_tasks(),
            num_tasks: job.num_tasks(),
        })
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

async fn submit_sql(
    State(state): State<RuntimeState>,
    body: Bytes,
) -> Response {
    let request = match SqlSubmitRequest::decode(body) {
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
    match state.submit_sql(request).await {
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

async fn distributed_job_status(
    Path(id): Path<Uuid>,
    State(state): State<RuntimeState>,
) -> Response {
    state
        .distributed_job_status(id)
        .await
        .map(|status| proto_response(StatusCode::OK, &status))
        .unwrap_or_else(|| {
            proto_response(
                StatusCode::NOT_FOUND,
                &ProtoError {
                    message: "unknown distributed job".to_string(),
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

async fn register_executor(
    State(state): State<RuntimeState>,
    body: Bytes,
) -> Response {
    let request = match ExecutorRegistration::decode(body) {
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
    let response = state.register_executor(request).await;
    proto_response(StatusCode::OK, &response)
}

async fn executor_heartbeat(
    State(state): State<RuntimeState>,
    body: Bytes,
) -> Response {
    let request = match ExecutorHeartbeat::decode(body) {
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
    let response = state.executor_heartbeat(request).await;
    proto_response(StatusCode::OK, &response)
}

async fn poll_work(
    State(state): State<RuntimeState>,
    body: Bytes,
) -> Response {
    let request = match PollWorkRequest::decode(body) {
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
    match state.poll_work(request).await {
        Ok(response) => proto_response(StatusCode::OK, &response),
        Err(e) => proto_response(StatusCode::BAD_REQUEST, &ProtoError { message: e }),
    }
}

async fn report_task_status(
    State(state): State<RuntimeState>,
    body: Bytes,
) -> Response {
    let request = match TaskStatus::decode(body) {
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
    match state.report_task_status(request).await {
        Ok(response) => proto_response(StatusCode::OK, &response),
        Err(e) => proto_response(StatusCode::BAD_REQUEST, &ProtoError { message: e }),
    }
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
        .route("/v1/sql", post(submit_sql))
        .route("/v1/jobs/{id}", get(status).delete(cancel))
        .route("/v1/jobs/{id}/result", get(result))
        .route("/v1/distributed-jobs/{id}", get(distributed_job_status))
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
        .route("/v1/executors/register", post(register_executor))
        .route("/v1/executors/heartbeat", post(executor_heartbeat))
        .route("/v1/executors/poll", post(poll_work))
        .route("/v1/executors/task-status", post(report_task_status))
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
pub(crate) fn materialize_artifacts(
    job_id: Uuid,
    artifacts: &[(String, Vec<u8>)],
) -> Result<PathBuf, String> {
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

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
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
