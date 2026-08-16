//! Distributed executor: registers with the scheduler, polls for tasks, and
//! executes them against the local (pure-Rust) engine.
//!
//! The executor binary never links CPython. Plans that contain Python UDFs
//! are handed to a short-lived ``python -m daft.runtime.worker`` subprocess
//! (see [`crate::python_worker`]); every other stage executes natively.
//! Intermediate stages write shuffle partitions into the executor's local
//! Arrow Flight server and report the cache ids back; the final stage returns
//! a ``DAFTRES1`` IPC envelope. All control-plane traffic is protobuf over
//! plain HTTP (``application/x-protobuf``) — JSON is never used on the wire.

use std::{
    collections::HashMap,
    sync::Arc,
};

use common_daft_config::DaftExecutionConfig;
use common_treenode::{Transformed, TreeNode};
use daft_logical_plan::{
    InMemoryInfo, LogicalPlan, LogicalPlanRef, SourceInfo,
    ops::{ShuffleRead, Source},
    proto::{plan_from_proto, plan_to_proto},
};
use daft_local_execution::NativeExecutor;
use daft_local_plan::translate::translate_distributed;
use daft_micropartition::{MicroPartition, MicroPartitionRef};
use daft_protocol::{
    daft::v1::{
        poll_work_response, ExecutorHeartbeat, ExecutorHeartbeatResponse, ExecutorRegistration,
        ExecutorRegistrationResponse, ExecutorTaskStatusResponse, PollWorkRequest,
        PollWorkResponse, TaskDefinition, TaskState, TaskStatus,
    },
    decode, encode,
};
use prost::Message;
use reqwest::header::CONTENT_TYPE;
use tokio::time::{sleep, Duration};
use uuid::Uuid;

use crate::{native, python_worker};

const PROTOBUF_CONTENT_TYPE: &str = "application/x-protobuf";

/// Result of executing one task on the executor.
enum TaskOutcome {
    /// Intermediate stage: shuffle written to the local Flight server.
    Shuffle {
        flight_address: String,
        cache_ids: Vec<u32>,
    },
    /// Final stage: DAFTRES1 envelope of this task's output partitions.
    Result(Vec<u8>),
}

/// Run the executor loop until the process is terminated.
///
/// ``scheduler_address`` is the control-plane base URL (e.g.
/// ``http://127.0.0.1:8080``); ``flight_ip`` is the local interface the Arrow
/// Flight shuffle server binds to. ``token`` authenticates control-plane
/// requests when the scheduler enables ``DAFT_RUNTIME_TOKEN``.
pub async fn run(
    scheduler_address: &str,
    flight_ip: &str,
    token: Option<String>,
) -> Result<(), String> {
    let client = reqwest::Client::new();
    let worker_id = Uuid::new_v4().to_string();

    // A single Flight server (and a single NativeExecutor) is reused for every
    // task this process runs: intermediate stages register shuffle caches on
    // it and downstream stages fetch them over Flight.
    // `NativeExecutor::new` binds the Flight listener and waits for the port
    // with a blocking receive, which panics on an async runtime thread; start
    // it on a blocking thread instead.
    let flight_ip_owned = flight_ip.to_string();
    let mut native = tokio::task::spawn_blocking(move || NativeExecutor::new(true, &flight_ip_owned))
        .await
        .map_err(|e| format!("failed to start executor flight server: {e}"))?;
    let flight_address = native
        .shuffle_address()
        .ok_or_else(|| "executor failed to start its Flight shuffle server".to_string())?;
    println!(
        "[executor {worker_id}] Flight shuffle server at {flight_address}; scheduler at {scheduler_address}"
    );

    let control_address = format!("{scheduler_address}/v1/executors");
    let registration = ExecutorRegistration {
        worker_id: worker_id.clone(),
        address: control_address.clone(),
        flight_address: flight_address.clone(),
        cpu_capacity: std::thread::available_parallelism()
            .map(|n| n.get() as f64)
            .unwrap_or(1.0),
        memory_bytes: 0,
        python_version: String::new(),
    };
    let accepted = register(&client, &control_address, &registration, token.as_deref()).await?;
    println!(
        "[executor {worker_id}] registered with scheduler: accepted={accepted}",
    );

    // Heartbeat the scheduler on its own task so a long-running task cannot
    // stall liveness reporting.
    let heartbeat_client = client.clone();
    let heartbeat_worker_id = worker_id.clone();
    let heartbeat_address = control_address.clone();
    let heartbeat_token = token.clone();
    tokio::spawn(async move {
        loop {
            sleep(Duration::from_secs(3)).await;
            let heartbeat = ExecutorHeartbeat {
                worker_id: heartbeat_worker_id.clone(),
                timestamp_ms: now_ms(),
            };
            let response: Result<ExecutorHeartbeatResponse, String> = post_protobuf(
                &heartbeat_client,
                &format!("{heartbeat_address}/heartbeat"),
                &heartbeat,
                heartbeat_token.as_deref(),
            )
            .await;
            if let Err(error) = response {
                eprintln!("[executor] heartbeat failed: {error}");
            }
        }
    });

    // Poll for work and execute one task at a time. Concurrency across tasks
    // is provided by running more executors; a single sequential loop keeps
    // the shared Flight server and NativeExecutor free of cross-task races.
    loop {
        let request = PollWorkRequest {
            worker_id: worker_id.clone(),
        };
        let response: PollWorkResponse = match post_protobuf(
            &client,
            &format!("{control_address}/poll"),
            &request,
            token.as_deref(),
        )
        .await
        {
            Ok(response) => response,
            Err(error) => {
                eprintln!("[executor] poll failed: {error}");
                sleep(Duration::from_millis(500)).await;
                continue;
            }
        };

        // Drop caches the scheduler no longer needs (completed jobs). Apply
        // these before running the next task so purge I/O cannot race with a
        // new shuffle that happens to reuse the same directories.
        for purge in &response.purge_shuffles {
            let Some(shuffle_server) = native.shuffle_server() else {
                eprintln!("[executor] cannot purge shuffle: no flight server");
                continue;
            };
            match shuffle_server
                .purge_shuffle_caches(purge.shuffle_id, &purge.cache_ids)
                .await
            {
                Ok(()) => eprintln!(
                    "[executor] purged shuffle {} caches {:?}",
                    purge.shuffle_id, purge.cache_ids
                ),
                Err(error) => eprintln!(
                    "[executor] failed to purge shuffle {}: {error}",
                    purge.shuffle_id
                ),
            }
        }

        let Some(task) = response.work.and_then(|work| match work {
            poll_work_response::Work::Task(task) => Some(task),
            poll_work_response::Work::NoWork(_) => None,
        }) else {
            sleep(Duration::from_millis(200)).await;
            continue;
        };

        println!(
            "[executor {worker_id}] executing task {}:{}/{} (stage {})",
            task.job_id, task.stage_id, task.task_id, task.partition_idx
        );
        let outcome = execute_task(&mut native, &task).await;
        let status = match outcome {
            Ok(TaskOutcome::Shuffle {
                flight_address,
                cache_ids,
            }) => TaskStatus {
                job_id: task.job_id.clone(),
                stage_id: task.stage_id,
                task_id: task.task_id,
                task_attempt: task.task_attempt,
                state: TaskState::Succeeded as i32,
                error: String::new(),
                result: Vec::new(),
                flight_address,
                cache_ids,
            },
            Ok(TaskOutcome::Result(result)) => TaskStatus {
                job_id: task.job_id.clone(),
                stage_id: task.stage_id,
                task_id: task.task_id,
                task_attempt: task.task_attempt,
                state: TaskState::Succeeded as i32,
                error: String::new(),
                result,
                flight_address: String::new(),
                cache_ids: Vec::new(),
            },
            Err(error) => {
                eprintln!(
                    "[executor {worker_id}] task {}:{}/{} failed: {error}",
                    task.job_id, task.stage_id, task.task_id
                );
                TaskStatus {
                    job_id: task.job_id.clone(),
                    stage_id: task.stage_id,
                    task_id: task.task_id,
                    task_attempt: task.task_attempt,
                    state: TaskState::Failed as i32,
                    error,
                    result: Vec::new(),
                    flight_address: String::new(),
                    cache_ids: Vec::new(),
                }
            }
        };

        let accepted: Result<ExecutorTaskStatusResponse, String> = post_protobuf(
            &client,
            &format!("{control_address}/task-status"),
            &status,
            token.as_deref(),
        )
        .await;
        if let Err(error) = accepted {
            eprintln!("[executor] failed to report task status: {error}");
        } else {
            eprintln!(
                "[executor] task {}:{}/{} status reported",
                task.job_id, task.stage_id, task.task_id
            );
        }
    }
}

/// POST the registration payload and return whether the scheduler accepted it.
async fn register(
    client: &reqwest::Client,
    control_address: &str,
    registration: &ExecutorRegistration,
    token: Option<&str>,
) -> Result<bool, String> {
    let response: ExecutorRegistrationResponse =
        post_protobuf(client, &format!("{control_address}/register"), registration, token).await?;
    if !response.accepted {
        return Err(format!(
            "scheduler rejected executor registration: {}",
            response.message
        ));
    }
    Ok(true)
}

/// Execute one task definition and produce the status payload contents.
async fn execute_task(
    native: &mut NativeExecutor,
    task: &TaskDefinition,
) -> Result<TaskOutcome, String> {
    if task.shuffle_id.is_some() && !task.udfs.is_empty() {
        return Err(
            "Python UDFs are only supported in the final stage; intermediate shuffle stages must be UDF-free"
                .to_string(),
        );
    }

    let proto_plan = decode::<daft_protocol::daft::v1::LogicalPlan>(&task.logical_plan)
        .map_err(|e| format!("failed to decode task logical plan: {e}"))?;
    let plan = plan_from_proto(proto_plan)
        .map_err(|e| format!("failed to deserialize task logical plan: {e}"))?;
    let plan = fill_shuffle_dirs(plan, task.task_id);

    let mut partition_sets = decode_partition_sets(&task.partition_sets)?;
    // A stage task owns one contiguous slice of the upstream in-memory
    // partitions (physical scan sources were already sliced by the scheduler
    // when it built the task plan). Without this, every task of the stage
    // would process the whole dataset and duplicate the shuffle output.
    slice_partition_sets(
        &mut partition_sets,
        task.partition_idx as usize,
        task.num_partitions as usize,
    );
    let shuffle_locations = decode_upstream_shuffles(&task.upstream_shuffles);

    if let Some(shuffle_id) = task.shuffle_id {
        // Intermediate stage: translate and run natively. The repartition
        // sink writes shuffle files and registers caches on our Flight
        // server; await the pipeline finish, then read the cache ids.
        let (physical_plan, inputs) =
            translate_distributed(&plan, &partition_sets, &shuffle_locations)
                .map_err(|e| format!("failed to lower task plan: {e}"))?;
        let exec_cfg = Arc::new(DaftExecutionConfig::default());
        let (fingerprint, enqueue_future) = native
            .run(
                &physical_plan,
                exec_cfg,
                Vec::new(),
                None,
                inputs,
                task.task_id as u32,
                true,
            )
            .map_err(|e| format!("failed to start task execution: {e}"))?;
        let result = enqueue_future
            .await
            .map_err(|e| format!("task execution failed: {e}"))?;
        let shuffle_metadata = result.into_shuffle_metadata().await;
        if shuffle_metadata.is_none() {
            return Err("intermediate task produced no shuffle metadata".to_string());
        }
        native
            .try_finish(fingerprint, task.task_id as u32)
            .map_err(|e| format!("failed to start task finish: {e}"))?
            .await
            .map_err(|e| format!("failed to finish task execution: {e}"))?;
        let flight_address = native
            .shuffle_address()
            .ok_or_else(|| "executor lost its Flight shuffle server".to_string())?;
        let cache_ids = native
            .shuffle_server()
            .ok_or_else(|| "executor has no Flight shuffle server".to_string())?
            .cache_ids(shuffle_id)
            .await;
        if cache_ids.is_empty() {
            return Err(format!(
                "intermediate stage wrote no shuffle cache for shuffle {shuffle_id}"
            ));
        }
        Ok(TaskOutcome::Shuffle {
            flight_address,
            cache_ids,
        })
    } else if task.udfs.is_empty() {
        // Final stage without Python UDFs: run natively and return the
        // DAFTRES1 envelope.
        let (physical_plan, inputs) =
            translate_distributed(&plan, &partition_sets, &shuffle_locations)
                .map_err(|e| format!("failed to lower task plan: {e}"))?;
        let exec_cfg = Arc::new(DaftExecutionConfig::default());
        let (fingerprint, enqueue_future) = native
            .run(
                &physical_plan,
                exec_cfg,
                Vec::new(),
                None,
                inputs,
                task.task_id as u32,
                true,
            )
            .map_err(|e| format!("failed to start task execution: {e}"))?;
        let mut result = enqueue_future
            .await
            .map_err(|e| format!("task execution failed: {e}"))?;
        let mut partitions: Vec<MicroPartition> = Vec::new();
        while let Some(partition) = result.next_partition().await {
            partitions.push(partition);
        }
        native
            .try_finish(fingerprint, task.task_id as u32)
            .map_err(|e| format!("failed to start task finish: {e}"))?
            .await
            .map_err(|e| format!("failed to finish task execution: {e}"))?;
        let envelope = native::encode_result_envelope(&partitions)
            .map_err(|e| format!("failed to encode task result: {e}"))?;
        Ok(TaskOutcome::Result(envelope))
    } else {
        // Final stage with Python UDFs. The stage plan may contain ShuffleRead
        // leaves (when an earlier repartition fed this stage); the Python
        // worker does not know the shuffle locations, so materialize each
        // distinct shuffle locally into in-memory partition sets, rewrite the
        // reads into InMemory sources, and hand the rewritten plan to the
        // short-lived Python worker.
        let job_id: Uuid = task
            .job_id
            .parse()
            .map_err(|_| format!("invalid job id {}", task.job_id))?;
        let (rewritten, extra_partition_sets) =
            materialize_shuffle_reads(native, &plan, &shuffle_locations).await?;
        // Re-encode the (already partition-sliced) in-memory inputs so the
        // worker receives exactly this task's slice.
        let mut merged_partition_sets: HashMap<String, Vec<u8>> = partition_sets
            .iter()
            .map(|(key, partitions)| {
                encode_partition_set_refs(partitions).map(|blob| (key.clone(), blob))
            })
            .collect::<Result<_, _>>()?;
        merged_partition_sets.extend(extra_partition_sets);

        let proto_plan = plan_to_proto(&rewritten)
            .map_err(|e| format!("failed to serialize rewritten UDF plan: {e}"))?;
        let plan_bytes = encode(&proto_plan);
        let artifact_files = task
            .udf_artifacts
            .iter()
            .map(|(filename, payload)| (filename.clone(), payload.clone()))
            .collect::<Vec<_>>();

        let udfs = task.udfs.clone();
        let python_version = task.python_version.clone();
        let result = tokio::task::spawn_blocking(move || {
            let artifact_dir = if artifact_files.is_empty() {
                None
            } else {
                Some(crate::materialize_artifacts(job_id, &artifact_files)?)
            };
            let extra_paths = artifact_dir
                .iter()
                .map(|dir| dir.to_string_lossy().into_owned())
                .collect();
            let result = python_worker::execute_plan_with_python_worker(
                plan_bytes,
                merged_partition_sets,
                extra_paths,
                udfs,
                python_version,
            );
            if let Some(dir) = artifact_dir {
                let _ = std::fs::remove_dir_all(&dir);
            }
            result
        })
        .await
        .map_err(|e| format!("UDF worker task panicked: {e}"))??;
        Ok(TaskOutcome::Result(result))
    }
}

/// Collect every distinct shuffle read in ``plan``, fetch its partition data
/// over Flight with the local native executor, and return
///
/// 1. the plan with every `ShuffleRead` node rewritten into an `InMemory`
///    source whose ``cache_key`` names the materialized partition set, and
/// 2. the new partition-set blobs keyed by those cache keys.
///
/// All reads of the same shuffle within one task share the same partition
/// index (the task's partition slice), so one fetch per shuffle id suffices.
async fn materialize_shuffle_reads(
    native: &mut NativeExecutor,
    plan: &LogicalPlanRef,
    shuffle_locations: &HashMap<u64, HashMap<String, Vec<u32>>>,
) -> Result<(LogicalPlanRef, HashMap<String, Vec<u8>>), String> {
    let mut reads: HashMap<u64, ShuffleRead> = HashMap::new();
    collect_shuffle_reads(plan, &mut reads);

    let mut partition_sets = HashMap::with_capacity(reads.len());
    let exec_cfg = Arc::new(DaftExecutionConfig::default());

    for (shuffle_id, read) in &reads {
        let read_plan: LogicalPlanRef = Arc::new(LogicalPlan::ShuffleRead(read.clone()));
        let (physical_plan, inputs) =
            translate_distributed(&read_plan, &HashMap::new(), shuffle_locations)
                .map_err(|e| format!("failed to lower shuffle read {shuffle_id}: {e}"))?;
        let (fingerprint, enqueue_future) = native
            .run(
                &physical_plan,
                exec_cfg.clone(),
                Vec::new(),
                None,
                inputs,
                0,
                true,
            )
            .map_err(|e| format!("failed to start shuffle read {shuffle_id}: {e}"))?;
        let mut result = enqueue_future
            .await
            .map_err(|e| format!("shuffle read {shuffle_id} failed: {e}"))?;
        let mut partitions: Vec<MicroPartition> = Vec::new();
        while let Some(partition) = result.next_partition().await {
            partitions.push(partition);
        }
        native
            .try_finish(fingerprint, 0)
            .map_err(|e| format!("failed to finish shuffle read {shuffle_id}: {e}"))?
            .await
            .map_err(|e| format!("failed to finish shuffle read {shuffle_id}: {e}"))?;
        if partitions.is_empty() {
            return Err(format!(
                "shuffle read {shuffle_id} produced no partitions for task partition {}",
                read.partition_idx
            ));
        }
        let blob = encode_partition_set(&partitions)
            .map_err(|e| format!("failed to encode shuffle read {shuffle_id}: {e}"))?;
        let cache_key = shuffle_cache_key(*shuffle_id, read.partition_idx);
        partition_sets.insert(cache_key, blob);
    }

    let rewritten = rewrite_shuffle_reads(plan);
    Ok((rewritten, partition_sets))
}

/// Collect the first `ShuffleRead` node encountered per shuffle id.
fn collect_shuffle_reads(plan: &LogicalPlanRef, reads: &mut HashMap<u64, ShuffleRead>) {
    if let LogicalPlan::ShuffleRead(read) = plan.as_ref() {
        reads
            .entry(read.shuffle_id)
            .or_insert_with(|| read.clone());
        return;
    }
    for child in plan.as_ref().children() {
        collect_shuffle_reads(&Arc::new(child.clone()), reads);
    }
}

/// Fill empty ``shuffle_dirs`` on every `ShuffleWrite` in the stage plan.
///
/// The scheduler builds intermediate stages with no shuffle directories
/// because each executor must write to its own local disk. Resolve the local
/// scratch directory here (``DAFT_SHUFFLE_DIR`` env var, falling back to the
/// system temp dir) so the Flight repartition sink has somewhere to write.
/// Fill in the local shuffle write directory on every `ShuffleWrite` node.
///
/// The base directory is namespaced by task id so that distinct intermediate
/// tasks on the same executor never overwrite each other's cache files (the
/// shuffle cache path is derived from the directory and a per-input cache id
/// that is identical across tasks).
fn fill_shuffle_dirs(plan: LogicalPlanRef, task_id: u64) -> LogicalPlanRef {
    let Some(dir) = std::env::var_os("DAFT_SHUFFLE_DIR")
        .map(std::path::PathBuf::from)
        .or_else(|| std::env::temp_dir().into())
    else {
        return plan;
    };
    let dir = format!("{}/task-{task_id}", dir.to_string_lossy());
    plan.clone().transform_up(|node: LogicalPlanRef| {
        if let LogicalPlan::ShuffleWrite(write) = node.as_ref() {
            let mut write = write.clone();
            if write.shuffle_dirs.is_empty() {
                write.shuffle_dirs = vec![dir.clone()];
            }
            Ok(Transformed::yes(Arc::new(LogicalPlan::ShuffleWrite(write))))
        } else {
            Ok(Transformed::no(node))
        }
    })
    .map(|transformed| transformed.data)
    .unwrap_or(plan)
}

/// Replace every `ShuffleRead` leaf with an `InMemory` source whose cache key
/// names the partition set materialized by [`materialize_shuffle_reads`].
fn rewrite_shuffle_reads(plan: &LogicalPlanRef) -> LogicalPlanRef {
    if let LogicalPlan::ShuffleRead(read) = plan.as_ref() {
        let in_memory = InMemoryInfo::new(
            read.output_schema.clone(),
            shuffle_cache_key(read.shuffle_id, read.partition_idx),
            None,
            1,
            0,
            0,
            None,
            None,
        );
        return Arc::new(LogicalPlan::Source(Source::new(
            read.output_schema.clone(),
            Arc::new(SourceInfo::InMemory(in_memory)),
        )));
    }
    let children: Vec<LogicalPlanRef> = plan
        .as_ref()
        .children()
        .into_iter()
        .map(|child| Arc::new(child.clone()))
        .collect();
    if children.is_empty() {
        return plan.clone();
    }
    let new_children = children
        .into_iter()
        .map(|child| rewrite_shuffle_reads(&child))
        .collect::<Vec<_>>();
    Arc::new(plan.as_ref().with_new_children(&new_children))
}

/// Deterministic partition-set cache key for a materialized shuffle partition.
fn shuffle_cache_key(shuffle_id: u64, partition_idx: usize) -> String {
    format!("shuffle:{shuffle_id}:{partition_idx}")
}

/// Encode partitions as a partition-set blob:
/// `u32 LE` count, then per partition `u64 LE` length + Arrow IPC stream bytes.
/// This is the inverse of [`native::decode_partition_set`].
fn encode_partition_set(partitions: &[MicroPartition]) -> Result<Vec<u8>, String> {
    let mut blob = Vec::new();
    blob.extend_from_slice(&(partitions.len() as u32).to_le_bytes());
    for partition in partitions {
        let stream = partition
            .write_to_ipc_stream()
            .map_err(|e| format!("failed to serialize partition: {e}"))?;
        blob.extend_from_slice(&(stream.len() as u64).to_le_bytes());
        blob.extend_from_slice(&stream);
    }
    Ok(blob)
}

/// [`encode_partition_set`] over shared partition references.
fn encode_partition_set_refs(partitions: &[MicroPartitionRef]) -> Result<Vec<u8>, String> {
    let mut blob = Vec::new();
    blob.extend_from_slice(&(partitions.len() as u32).to_le_bytes());
    for partition in partitions {
        let stream = partition
            .write_to_ipc_stream()
            .map_err(|e| format!("failed to serialize partition: {e}"))?;
        blob.extend_from_slice(&(stream.len() as u64).to_le_bytes());
        blob.extend_from_slice(&stream);
    }
    Ok(blob)
}

/// Decode task ``partition_sets`` (raw Arrow IPC blobs) into
/// ``cache_key -> partitions`` for the local execution engine.
fn decode_partition_sets(
    partition_sets: &HashMap<String, Vec<u8>>,
) -> Result<HashMap<String, Vec<MicroPartitionRef>>, String> {
    let mut psets = HashMap::with_capacity(partition_sets.len());
    for (key, blob) in partition_sets {
        let partitions = native::decode_partition_set(blob)
            .map_err(|e| format!("failed to decode partition set {key}: {e}"))?;
        psets.insert(key.clone(), partitions);
    }
    Ok(psets)
}

/// Keep only the task's balanced slice of each in-memory partition set.
fn slice_partition_sets(
    partition_sets: &mut HashMap<String, Vec<MicroPartitionRef>>,
    partition_idx: usize,
    num_partitions: usize,
) {
    for partitions in partition_sets.values_mut() {
        let len = partitions.len();
        let (start, end) = slice_bounds(len, partition_idx, num_partitions.max(1));
        partitions.drain(end..);
        partitions.drain(..start);
    }
}

/// Bounds of one of `n` contiguous, balanced slices of `len` items. Mirrors
/// the scheduler's scan-task slicing so both source kinds line up.
fn slice_bounds(len: usize, idx: usize, n: usize) -> (usize, usize) {
    if len == 0 {
        return (0, 0);
    }
    let base = len / n;
    let rem = len % n;
    let start = idx * base + idx.min(rem);
    let end = start + base + usize::from(idx < rem);
    (start, end)
}

/// Convert the protobuf upstream-shuffle map into the native engine's
/// ``shuffle id -> (flight address -> cache ids)`` map.
fn decode_upstream_shuffles(
    upstream: &HashMap<u64, daft_protocol::daft::v1::ShuffleLocation>,
) -> HashMap<u64, HashMap<String, Vec<u32>>> {
    upstream
        .iter()
        .map(|(shuffle_id, location)| {
            let servers = location
                .servers
                .iter()
                .map(|(address, cache)| (address.clone(), cache.cache_ids.clone()))
                .collect();
            (*shuffle_id, servers)
        })
        .collect()
}

/// POST one protobuf message and decode the protobuf response.
async fn post_protobuf<Req: Message, Resp: Message + prost::Message + Default>(
    client: &reqwest::Client,
    url: &str,
    request: &Req,
    token: Option<&str>,
) -> Result<Resp, String> {
    let mut builder = client
        .post(url)
        .header(CONTENT_TYPE, PROTOBUF_CONTENT_TYPE)
        .body(encode(request));
    if let Some(token) = token {
        builder = builder.bearer_auth(token);
    }
    let response = builder
        .send()
        .await
        .map_err(|e| format!("request to {url} failed: {e}"))?;
    let status = response.status();
    let body = response
        .bytes()
        .await
        .map_err(|e| format!("failed to read response from {url}: {e}"))?;
    if !status.is_success() {
        let message = String::from_utf8_lossy(&body).into_owned();
        return Err(format!("{url} returned {status}: {message}"));
    }
    Resp::decode(body.as_ref()).map_err(|e| format!("failed to decode response from {url}: {e}"))
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
