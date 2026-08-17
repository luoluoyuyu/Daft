//! Distributed scheduler: logical-plan stage DAG construction, task building,
//! and per-job task dispatch / shuffle-location bookkeeping.
//!
//! The scheduler never executes plans itself. Executors register with the
//! control plane, poll for [`TaskDefinition`]s and report [`TaskStatus`]es.
//! The scheduler cuts an optimized logical plan into stages at the explicit
//! `Repartition` / `IntoPartitions` boundaries (the logical plan carries these
//! nodes explicitly, unlike DataFusion where the physical planner has to hunt
//! for `RepartitionExec`), builds one task per stage x partition slice, and
//! assembles the final `DAFTRES1` envelope once every final-stage task has
//! succeeded.
//!
//! Wire format: every control-plane message is protobuf over HTTP
//! (``application/x-protobuf``); plan payloads are ``daft.v1.LogicalPlan``
//! protobuf. JSON is never used on the wire.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use common_daft_config::DaftExecutionConfig;
use common_error::{DaftError, DaftResult};
use daft_logical_plan::{
    LogicalPlan, LogicalPlanBuilder, LogicalPlanRef, SourceInfo,
    ops::{ShuffleRead, ShuffleWrite, Source},
    partitioning::RepartitionSpec,
    proto::{plan_from_proto, plan_to_proto},
};
use daft_protocol::{
    daft::v1::{
        PartitionCache, ShuffleConfig, ShuffleLocation, ShuffleReadTransport, TaskDefinition,
        TaskState, UdfDescriptor,
    },
    decode, encode,
};
use daft_scan::ScanState;
use uuid::Uuid;

use crate::native;

/// Global counter of shuffle-id blocks handed out to jobs.
///
/// Executor Flight servers accumulate shuffle caches until they are purged,
/// and the shuffle-id namespace is global to a server, so two jobs must never
/// reuse the same shuffle id (restarting ids at 0 for every job made a later
/// job's final stage read stale caches from an earlier one with the same id).
/// Each job reserves a fresh block of ids.
static NEXT_SHUFFLE_BLOCK: AtomicU64 = AtomicU64::new(0);

/// Number of shuffle ids reserved per job. Each stage that ends in a
/// `ShuffleWrite` consumes exactly one; this is far more than any real job
/// produces.
const SHUFFLE_IDS_PER_JOB: u64 = 1 << 20;

/// Lifecycle state of a distributed job (scheduler-side mirror of
/// ``daft.v1.JobState``).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DistJobState {
    Pending,
    Running,
    Succeeded,
    Failed,
}

/// Outcome of a task-status callback that the caller must apply to the
/// durable job record.
pub enum JobOutcome {
    /// The job finished: the fully assembled ``DAFTRES1`` envelope.
    Succeeded {
        result: Vec<u8>,
        task_id: u64,
        chunk: Vec<u8>,
    },
    /// The job failed: a human-readable error.
    Failed(String),
    /// One final-stage task completed and can be published immediately.
    ResultReady { task_id: u64, chunk: Vec<u8> },
    /// Nothing terminal happened yet.
    InProgress,
}

/// A single schedulable stage: a logical sub-plan that contains no
/// repartition boundary. Intermediate stages end with a `ShuffleWrite` node;
/// the final stage produces the job's result partitions.
#[derive(Clone)]
pub struct Stage {
    pub stage_id: u64,
    pub plan: LogicalPlanRef,
    pub num_partitions: usize,
    /// Set for intermediate stages: the shuffle id this stage writes.
    pub shuffle_id: Option<u64>,
    /// Shuffle ids this stage reads from (its dependency edges).
    pub upstream_shuffle_ids: Vec<u64>,
    pub is_final: bool,
}

/// A per-stage bundle of ready-to-dispatch protobuf task definitions.
struct StageTasks {
    stage: Stage,
    /// Serialized task definitions, one per partition slice.
    tasks: Vec<TaskDefinition>,
    /// Number of tasks this stage must complete (the dispatch queue in
    /// ``tasks`` is consumed as work is handed out, so this count is kept
    /// separately for completion bookkeeping and result assembly).
    num_tasks: usize,
    /// Whether the stage has been enqueued into the ready queue.
    enqueued: bool,
    /// Number of succeeded tasks.
    completed: usize,
}

#[derive(Clone)]
struct TaskLease {
    task: TaskDefinition,
    worker_id: String,
    deadline_ms: u64,
}

const MAX_TASK_ATTEMPTS: u64 = 3;

/// The complete scheduler-side state of one distributed job.
pub struct DistributedJob {
    pub job_id: Uuid,
    pub state: DistJobState,
    pub error: Option<String>,
    stages: Vec<StageTasks>,
    /// shuffle id -> (flight address -> cache ids), filled as intermediate
    /// tasks succeed.
    shuffle_locations: HashMap<u64, HashMap<String, Vec<u32>>>,
    /// Stage ids whose tasks are dispatchable, round-robin across jobs.
    ready_stages: VecDeque<u64>,
    /// Final-stage results: (stage_id, task_id) -> DAFTRES1 envelope.
    final_results: HashMap<(u64, u64), Vec<u8>>,
    /// Assembled job result envelope, set on success.
    result: Option<Vec<u8>>,
    leases: HashMap<(u64, u64), TaskLease>,
    succeeded_tasks: HashSet<(u64, u64)>,
}

impl DistributedJob {
    pub fn new(
        job_id: Uuid,
        stages: Vec<Stage>,
        partition_sets: HashMap<String, Vec<u8>>,
        udfs: Vec<UdfDescriptor>,
        udf_artifact_ids: Vec<String>,
        udf_artifacts: HashMap<String, Vec<u8>>,
        python_version: String,
        shuffle: ShuffleConfig,
    ) -> Result<Self, String> {
        let mut ready_stages = VecDeque::new();
        let mut stage_tasks = Vec::with_capacity(stages.len());
        for stage in stages {
            let mut tasks = build_stage_tasks(
                &stage,
                &partition_sets,
                &udfs,
                &udf_artifact_ids,
                &udf_artifacts,
                &python_version,
                &shuffle,
            )?;
            for task in &mut tasks {
                task.job_id = job_id.to_string();
            }
            let has_upstream = !stage.upstream_shuffle_ids.is_empty();
            if !has_upstream {
                ready_stages.push_back(stage.stage_id);
            }
            let num_tasks = tasks.len();
            stage_tasks.push(StageTasks {
                stage,
                tasks,
                num_tasks,
                // Stages with upstream shuffles are not dispatchable yet;
                // `advance_ready_stages` flips this once every dependency has
                // been satisfied by a completed intermediate stage.
                enqueued: false,
                completed: 0,
            });
        }
        Ok(Self {
            job_id,
            // A job is Pending until its first task is dispatched by `poll`;
            // it flips to Running the moment execution starts.
            state: DistJobState::Pending,
            error: None,
            stages: stage_tasks,
            shuffle_locations: HashMap::new(),
            ready_stages,
            final_results: HashMap::new(),
            result: None,
            leases: HashMap::new(),
            succeeded_tasks: HashSet::new(),
        })
    }

    /// Pop one ready task for an executor, or `None` when nothing is ready.
    pub fn lease_task(
        &mut self,
        worker_id: &str,
        now_ms: u64,
        lease_timeout_ms: u64,
    ) -> Option<TaskDefinition> {
        self.expire_leases(now_ms);
        if self.state == DistJobState::Pending {
            self.state = DistJobState::Running;
        }
        // Round-robin across ready stages: pop a stage, take one task, put the
        // stage back if it still has work.
        while let Some(stage_id) = self.ready_stages.pop_front() {
            let index = self
                .stages
                .iter()
                .position(|s| s.stage.stage_id == stage_id)
                .expect("ready stage exists in job");
            let mut task = {
                let stage_tasks = &mut self.stages[index];
                match stage_tasks.tasks.pop() {
                    Some(task) => task,
                    None => continue, // Stage exhausted: leave it out of the ready queue.
                }
            };
            // Fill in the shuffle locations known so far. Task definitions
            // are built once when the job starts; locations only become
            // available as upstream intermediate stages report success.
            for (shuffle_id, location) in &mut task.upstream_shuffles {
                if let Some(servers) = self.shuffle_locations.get(shuffle_id) {
                    location.servers = servers
                        .iter()
                        .map(|(address, cache_ids)| {
                            (
                                address.clone(),
                                PartitionCache {
                                    cache_ids: cache_ids.clone(),
                                },
                            )
                        })
                        .collect();
                }
            }
            if !self.stages[index].tasks.is_empty() {
                self.ready_stages.push_back(stage_id);
            }
            self.leases.insert(
                (task.stage_id, task.task_id),
                TaskLease {
                    task: task.clone(),
                    worker_id: worker_id.to_string(),
                    deadline_ms: now_ms.saturating_add(lease_timeout_ms),
                },
            );
            return Some(task);
        }
        None
    }

    pub fn reject_lease(&mut self, task: &TaskDefinition) {
        let key = (task.stage_id, task.task_id);
        if self
            .leases
            .get(&key)
            .is_some_and(|lease| lease.task.task_attempt == task.task_attempt)
        {
            let lease = self.leases.remove(&key).expect("checked above");
            self.requeue(lease.task, "worker rejected task launch");
        }
    }

    pub fn expire_worker(&mut self, worker_id: &str) {
        let expired: Vec<_> = self
            .leases
            .iter()
            .filter(|(_, lease)| lease.worker_id == worker_id)
            .map(|(key, _)| *key)
            .collect();
        for key in expired {
            if let Some(lease) = self.leases.remove(&key) {
                self.requeue(lease.task, "worker heartbeat expired");
            }
        }
    }

    pub fn expire_leases(&mut self, now_ms: u64) {
        let expired: Vec<_> = self
            .leases
            .iter()
            .filter(|(_, lease)| lease.deadline_ms <= now_ms)
            .map(|(key, _)| *key)
            .collect();
        for key in expired {
            if let Some(lease) = self.leases.remove(&key) {
                self.requeue(lease.task, "task lease expired");
            }
        }
    }

    fn requeue(&mut self, mut task: TaskDefinition, error: &str) {
        task.task_attempt = task.task_attempt.saturating_add(1);
        if task.task_attempt >= MAX_TASK_ATTEMPTS {
            self.state = DistJobState::Failed;
            self.error = Some(format!(
                "task {}/{}/{} exhausted {} attempts; last error: {}",
                task.job_id,
                task.stage_id,
                task.task_id,
                MAX_TASK_ATTEMPTS,
                if error.is_empty() { "unknown task failure" } else { error }
            ));
            return;
        }
        if let Some(stage) = self
            .stages
            .iter_mut()
            .find(|stage| stage.stage.stage_id == task.stage_id)
        {
            stage.tasks.push(task);
            if !self.ready_stages.contains(&stage.stage.stage_id) {
                self.ready_stages.push_back(stage.stage.stage_id);
            }
        }
    }

    /// Collect every shuffle location this job produced:
    /// `(shuffle_id, flight_address, cache_ids)`. Called when the job reaches
    /// a terminal state so the scheduler can tell the holding executors to
    /// drop the caches.
    pub fn purge_requests(&self) -> Vec<(u64, String, Vec<u32>)> {
        self.shuffle_locations
            .iter()
            .flat_map(|(shuffle_id, servers)| {
                servers
                    .iter()
                    .map(|(address, cache_ids)| (*shuffle_id, address.clone(), cache_ids.clone()))
            })
            .collect()
    }

    /// Apply a task-status report. Returns `Some` when the job reaches a
    /// terminal state.
    pub fn on_task_status(&mut self, status: &daft_protocol::daft::v1::TaskStatus) -> JobOutcome {
        if matches!(self.state, DistJobState::Succeeded | DistJobState::Failed) {
            return JobOutcome::InProgress;
        }

        let key = (status.stage_id, status.task_id);
        let Some(lease) = self.leases.get(&key) else {
            return JobOutcome::InProgress;
        };
        if lease.task.task_attempt != status.task_attempt {
            return JobOutcome::InProgress;
        }
        if status.state == TaskState::Running as i32 {
            return JobOutcome::InProgress;
        }
        let lease = self.leases.remove(&key).expect("validated above");
        if status.state == TaskState::Failed as i32 {
            self.requeue(lease.task, &status.error);
            if self.state == DistJobState::Failed {
                return JobOutcome::Failed(
                    self.error.clone().unwrap_or_else(|| status.error.clone()),
                );
            }
            return JobOutcome::InProgress;
        }
        if status.state != TaskState::Succeeded as i32 {
            return JobOutcome::InProgress;
        }

        let Some(stage_index) = self
            .stages
            .iter()
            .position(|s| s.stage.stage_id == status.stage_id)
        else {
            return JobOutcome::InProgress;
        };

        if !self.succeeded_tasks.insert(key) {
            return JobOutcome::InProgress;
        }
        let stage = self.stages[stage_index].stage.clone();
        self.stages[stage_index].completed += 1;
        let stage_tasks = &mut self.stages[stage_index];

        if let Some(shuffle_id) = stage.shuffle_id {
            if status.flight_address.is_empty() || status.cache_ids.is_empty() {
                self.state = DistJobState::Failed;
                self.error = Some(format!(
                    "intermediate stage {} task {} reported no shuffle location",
                    stage.stage_id, status.task_id
                ));
                return JobOutcome::Failed(self.error.clone().expect("set above"));
            }
            // A single executor may run several tasks of the same
            // intermediate stage, each registering its own cache under the
            // same flight address. Merge (dedup) the ids instead of
            // overwriting, otherwise earlier caches become unreachable.
            let locations = self.shuffle_locations.entry(shuffle_id).or_default();
            let ids = locations.entry(status.flight_address.clone()).or_default();
            for cache_id in &status.cache_ids {
                if !ids.contains(cache_id) {
                    ids.push(*cache_id);
                }
            }

            // Once every task of this stage has succeeded, downstream stages
            // whose dependencies are all satisfied become dispatchable.
            if stage_tasks.completed >= stage_tasks.num_tasks {
                self.advance_ready_stages();
            }
        } else {
            // Final stage: accumulate result envelopes.
            if status.result.is_empty() {
                self.state = DistJobState::Failed;
                self.error = Some(format!(
                    "final stage {} task {} reported an empty result",
                    stage.stage_id, status.task_id
                ));
                return JobOutcome::Failed(self.error.clone().expect("set above"));
            }
            self.final_results
                .insert((stage.stage_id, status.task_id), status.result.clone());
            eprintln!(
                "[scheduler] job {} final task {}/{} result {} bytes",
                self.job_id,
                stage.stage_id,
                status.task_id,
                status.result.len()
            );

            if stage_tasks.completed >= stage_tasks.num_tasks {
                let assembled = self.assemble_final_result();
                eprintln!(
                    "[scheduler] job {} assembled final result: {:?} bytes",
                    self.job_id,
                    assembled.as_ref().map(|b| b.len())
                );
                return match assembled {
                    Ok(result) => {
                        self.state = DistJobState::Succeeded;
                        self.result = Some(result.clone());
                        JobOutcome::Succeeded {
                            result,
                            task_id: status.task_id,
                            chunk: status.result.clone(),
                        }
                    }
                    Err(error) => {
                        self.state = DistJobState::Failed;
                        self.error = Some(error.clone());
                        JobOutcome::Failed(error)
                    }
                };
            } else {
                return JobOutcome::ResultReady {
                    task_id: status.task_id,
                    chunk: status.result.clone(),
                };
            }
        }
        JobOutcome::InProgress
    }

    /// Enqueue every non-enqueued stage whose upstream shuffles are present.
    fn advance_ready_stages(&mut self) {
        for index in 0..self.stages.len() {
            let stage_tasks = &mut self.stages[index];
            if stage_tasks.enqueued || stage_tasks.stage.upstream_shuffle_ids.is_empty() {
                continue;
            }
            let ready = stage_tasks
                .stage
                .upstream_shuffle_ids
                .iter()
                .all(|shuffle_id| self.shuffle_locations.contains_key(shuffle_id));
            if ready {
                stage_tasks.enqueued = true;
                self.ready_stages.push_back(stage_tasks.stage.stage_id);
            }
        }
    }

    /// Concatenate all final-stage envelopes (in task order) into one
    /// `DAFTRES1` envelope.
    fn assemble_final_result(&self) -> Result<Vec<u8>, String> {
        let final_stage_id = self.final_stage_id();
        let envelopes: Vec<&[u8]> = self
            .stages
            .iter()
            .find(|s| s.stage.is_final)
            .map(|s| (0..s.num_tasks).map(|i| i as u64).collect::<Vec<_>>())
            .map(|task_ids| {
                task_ids
                    .iter()
                    .filter_map(|task_id| self.final_results.get(&(final_stage_id, *task_id)))
                    .map(|v| v.as_slice())
                    .collect()
            })
            .unwrap_or_default();
        native::concat_result_envelopes(&envelopes)
    }

    fn final_stage_id(&self) -> u64 {
        self.stages
            .iter()
            .find(|s| s.stage.is_final)
            .map(|s| s.stage.stage_id)
            .unwrap_or(u64::MAX)
    }

    /// Number of tasks across every stage of this job.
    pub fn num_tasks(&self) -> u64 {
        self.stages.iter().map(|s| s.num_tasks as u64).sum()
    }

    /// Number of stages in this job's stage DAG.
    pub fn num_stages(&self) -> u64 {
        self.stages.len() as u64
    }

    /// Number of tasks reported successful so far.
    pub fn completed_tasks(&self) -> u64 {
        self.stages.iter().map(|s| s.completed as u64).sum()
    }
}

/// Decode, optimize, materialize and split a submitted plan into a
/// [`DistributedJob`] whose initial stages are ready to dispatch.
pub fn build_distributed_job(
    job_id: Uuid,
    plan_bytes: Vec<u8>,
    partition_sets: HashMap<String, Vec<u8>>,
    udfs: Vec<UdfDescriptor>,
    udf_artifact_ids: Vec<String>,
    udf_artifacts: HashMap<String, Vec<u8>>,
    python_version: String,
    shuffle: ShuffleConfig,
) -> Result<DistributedJob, String> {
    let proto_plan = decode::<daft_protocol::daft::v1::LogicalPlan>(&plan_bytes)
        .map_err(|e| format!("failed to decode logical plan: {e}"))?;
    let plan = plan_from_proto(proto_plan)
        .map_err(|e| format!("failed to deserialize logical plan: {e}"))?;
    let builder = LogicalPlanBuilder::new(plan, None);
    let optimized = daft_local_execution::block_on_global(async move {
        builder
            .optimize_async(Arc::new(DaftExecutionConfig::default()))
            .await
    })
    .map_err(|e| format!("failed to optimize logical plan: {e}"))?;

    // Materialize scan operators into scan tasks and strip in-memory partition
    // cache entries (same preparation as LogicalPlanBuilder::to_bytes) so the
    // stage DAG can be sliced and transported to executors.
    use daft_logical_plan::optimization::rules::{MaterializeScans, OptimizerRule};
    let plan = MaterializeScans::new()
        .try_optimize(optimized.plan.clone())
        .map_err(|e| format!("failed to materialize scans: {e}"))?
        .data;
    let plan = daft_logical_plan::transport::strip_partition_cache_entries(plan)
        .map_err(|e| format!("failed to strip partition cache entries: {e}"))?;

    let mut ctx = SplitCtx::default();
    // Fresh, globally-unique shuffle-id block so this job's caches can never
    // collide with those of an earlier (completed) job on an executor's
    // Flight server.
    ctx.next_shuffle_id = NEXT_SHUFFLE_BLOCK.fetch_add(SHUFFLE_IDS_PER_JOB, Ordering::Relaxed);
    let root = split(plan, &mut ctx, &shuffle)
        .map_err(|e| format!("failed to split plan into stages: {e}"))?;
    eprintln!(
        "[scheduler] build_distributed_job: split produced {} intermediate stages",
        root.stages.len()
    );
    let mut stages = root.stages;
    let num_partitions = plan_partition_count(&root.plan, &ctx.shuffle_partition_counts);
    let upstream_shuffle_ids = collect_upstream_shuffle_ids(&root.plan);
    eprintln!(
        "[scheduler] build_distributed_job: final stage partitions={num_partitions}, upstream={upstream_shuffle_ids:?}"
    );
    stages.push(Stage {
        stage_id: ctx.next_stage_id,
        plan: root.plan,
        num_partitions,
        shuffle_id: None,
        upstream_shuffle_ids,
        is_final: true,
    });

    DistributedJob::new(
        job_id,
        stages,
        partition_sets,
        udfs,
        udf_artifact_ids,
        udf_artifacts,
        python_version,
        shuffle,
    )
}

/// Mutable context for the recursive stage splitter.
#[derive(Default)]
struct SplitCtx {
    next_stage_id: u64,
    next_shuffle_id: u64,
    /// shuffle id -> number of partitions written by its stage.
    shuffle_partition_counts: HashMap<u64, usize>,
}

/// Result of cutting one subtree: the transformed sub-plan (with shuffle-read
/// leaves at every cut below) plus the intermediate stages introduced.
struct SplitResult {
    plan: LogicalPlanRef,
    stages: Vec<Stage>,
}

/// Recursively cut a plan at `Repartition` / `IntoPartitions` boundaries.
///
/// A boundary node is replaced in the parent with a `ShuffleRead` leaf and the
/// upstream subtree is wrapped in a `ShuffleWrite` node that becomes a new
/// intermediate stage. Multi-input nodes (Join/Concat/Intersect/Union) recurse
/// into each child independently, so each side keeps its own stages.
fn split(
    plan: LogicalPlanRef,
    ctx: &mut SplitCtx,
    shuffle: &ShuffleConfig,
) -> DaftResult<SplitResult> {
    match plan.as_ref() {
        LogicalPlan::Repartition(repartition) => {
            let input_result = split(repartition.input.clone(), ctx, shuffle)?;
            let upstream_n =
                plan_partition_count(&input_result.plan, &ctx.shuffle_partition_counts);
            let num_partitions = repartition
                .repartition_spec
                .to_clustering_spec(upstream_n)
                .num_partitions();
            split_boundary(
                input_result,
                Some(repartition.repartition_spec.clone()),
                num_partitions,
                ctx,
                shuffle,
            )
        }
        LogicalPlan::IntoPartitions(into_partitions) => {
            let input_result = split(into_partitions.input.clone(), ctx, shuffle)?;
            split_boundary(
                input_result,
                None,
                into_partitions.num_partitions,
                ctx,
                shuffle,
            )
        }
        _ => {
            let children: Vec<LogicalPlanRef> = plan
                .as_ref()
                .children()
                .into_iter()
                .map(|child| Arc::new(child.clone()))
                .collect();
            if children.is_empty() {
                return Ok(SplitResult {
                    plan,
                    stages: Vec::new(),
                });
            }
            let mut stages = Vec::new();
            let mut new_children = Vec::with_capacity(children.len());
            for child in children {
                let child_result = split(child, ctx, shuffle)?;
                stages.extend(child_result.stages);
                new_children.push(child_result.plan);
            }
            let new_plan = Arc::new(plan.as_ref().with_new_children(&new_children));
            Ok(SplitResult {
                plan: new_plan,
                stages,
            })
        }
    }
}

/// Wrap a transformed input subtree in a `ShuffleWrite` intermediate stage and
/// hand the parent a `ShuffleRead` leaf.
fn split_boundary(
    input_result: SplitResult,
    spec: Option<RepartitionSpec>,
    num_partitions: usize,
    ctx: &mut SplitCtx,
    shuffle: &ShuffleConfig,
) -> DaftResult<SplitResult> {
    let shuffle_id = ctx.next_shuffle_id;
    ctx.next_shuffle_id += 1;
    let stage_id = ctx.next_stage_id;
    ctx.next_stage_id += 1;

    let stage_plan: LogicalPlanRef = Arc::new(LogicalPlan::ShuffleWrite(
        ShuffleWrite::new(
            input_result.plan.clone(),
            shuffle_id,
            num_partitions,
            spec,
            Vec::new(),
            None,
        )
        .with_storage_uri(shuffle.spill_uri.clone()),
    ));

    // UDFs are only allowed in the final stage (the Python worker cannot
    // execute shuffle reads/writes yet).
    if daft_logical_plan::udf::plan_contains_python_udf(&stage_plan) {
        return Err(DaftError::ValueError(format!(
            "Python UDFs are only supported in the final stage, but stage {stage_id} \
             (shuffle {shuffle_id}) contains one; split the plan so the UDF is above \
             the last repartition"
        )));
    }

    let schema = stage_plan.schema();
    let mut read = ShuffleRead::new(schema, shuffle_id, 0);
    read.read_transport = if shuffle.read_transport == ShuffleReadTransport::Unspecified as i32 {
        ShuffleReadTransport::FileAndFlight as i32
    } else {
        shuffle.read_transport
    };
    read.fetch_retries = shuffle.fetch_retries;
    read.max_bytes_in_flight = shuffle.max_bytes_in_flight;
    read.max_concurrency_per_address = shuffle.max_concurrency_per_address;
    let read_leaf: LogicalPlanRef = Arc::new(LogicalPlan::ShuffleRead(read));
    ctx.shuffle_partition_counts
        .insert(shuffle_id, num_partitions);

    let mut stages = input_result.stages;
    stages.push(Stage {
        stage_id,
        plan: stage_plan.clone(),
        num_partitions,
        shuffle_id: Some(shuffle_id),
        upstream_shuffle_ids: collect_upstream_shuffle_ids(&stage_plan),
        is_final: false,
    });

    Ok(SplitResult {
        plan: read_leaf,
        stages,
    })
}

/// Max partition count over the leaves of a plan: physical scan tasks, in-memory
/// partition sets, or upstream shuffle partitions.
fn plan_partition_count(plan: &LogicalPlanRef, shuffle_counts: &HashMap<u64, usize>) -> usize {
    match plan.as_ref() {
        LogicalPlan::Source(source) => match source.source_info.as_ref() {
            SourceInfo::Physical(info) => match &info.scan_state {
                ScanState::Tasks(tasks) => tasks.len().max(1),
                ScanState::Operator(_) => 1,
            },
            SourceInfo::InMemory(info) => info.num_partitions.max(1),
            _ => 1,
        },
        LogicalPlan::ShuffleRead(read) => {
            shuffle_counts.get(&read.shuffle_id).copied().unwrap_or(1)
        }
        _ => {
            let mut max = 1usize;
            for child in plan.as_ref().children() {
                max = max.max(plan_partition_count(
                    &Arc::new(child.clone()),
                    shuffle_counts,
                ));
            }
            max
        }
    }
}

/// Collect the shuffle ids read by `ShuffleRead` leaves in a plan (deduped,
/// sorted).
fn collect_upstream_shuffle_ids(plan: &LogicalPlanRef) -> Vec<u64> {
    fn walk(plan: &LogicalPlanRef, ids: &mut Vec<u64>) {
        if let LogicalPlan::ShuffleRead(read) = plan.as_ref() {
            ids.push(read.shuffle_id);
        }
        for child in plan.as_ref().children() {
            walk(&Arc::new(child.clone()), ids);
        }
    }
    let mut ids = Vec::new();
    walk(plan, &mut ids);
    ids.sort_unstable();
    ids.dedup();
    ids
}

/// Build the serialized [`TaskDefinition`]s for one stage (one per partition
/// slice).
fn build_stage_tasks(
    stage: &Stage,
    partition_sets: &HashMap<String, Vec<u8>>,
    udfs: &[UdfDescriptor],
    udf_artifact_ids: &[String],
    udf_artifacts: &HashMap<String, Vec<u8>>,
    python_version: &str,
    shuffle: &ShuffleConfig,
) -> Result<Vec<TaskDefinition>, String> {
    let mut tasks = Vec::with_capacity(stage.num_partitions);
    // UDF descriptors, artifacts and the interpreter version only matter for
    // the final stage: the Python worker is a final-stage-only runtime, so
    // intermediate shuffle tasks must never carry UDF payloads (the executor
    // rejects them as a safety net).
    let task_udfs = if stage.is_final {
        udfs.to_vec()
    } else {
        Vec::new()
    };
    let task_udf_artifact_ids = if stage.is_final {
        udf_artifact_ids.to_vec()
    } else {
        Vec::new()
    };
    let task_udf_artifacts = if stage.is_final {
        udf_artifacts.clone()
    } else {
        HashMap::new()
    };
    let task_python_version = if stage.is_final {
        python_version.to_string()
    } else {
        String::new()
    };
    for partition_idx in 0..stage.num_partitions {
        let task_plan =
            transform_for_task(&stage.plan, partition_idx, stage.num_partitions, shuffle)
                .map_err(|e| format!("failed to restrict plan to partition slice: {e}"))?;
        let proto_plan = plan_to_proto(&task_plan)
            .map_err(|e| format!("failed to serialize stage plan: {e}"))?;
        let plan_bytes = encode(&proto_plan);

        let upstream_shuffles = stage
            .upstream_shuffle_ids
            .iter()
            .map(|shuffle_id| {
                let servers = HashMap::new();
                (*shuffle_id, ShuffleLocation { servers })
            })
            .collect::<HashMap<_, _>>();

        tasks.push(TaskDefinition {
            job_id: String::new(), // filled by DistributedJob::new
            stage_id: stage.stage_id,
            task_id: partition_idx as u64,
            task_attempt: 0,
            logical_plan: plan_bytes,
            partition_sets: partition_sets.clone(),
            partition_idx: partition_idx as u64,
            num_partitions: stage.num_partitions as u64,
            udfs: task_udfs.clone(),
            udf_artifact_ids: task_udf_artifact_ids.clone(),
            udf_artifacts: task_udf_artifacts.clone(),
            python_version: task_python_version.clone(),
            shuffle_id: stage.shuffle_id,
            shuffle_dirs: Vec::new(),
            compression: None,
            upstream_shuffles,
            is_final_stage: stage.is_final,
        });
    }
    Ok(tasks)
}

/// Restrict a stage plan to one partition slice: physical scan sources keep
/// only their slice of scan tasks and `ShuffleRead` leaves read the slice's
/// partition index.
fn transform_for_task(
    plan: &LogicalPlanRef,
    partition_idx: usize,
    num_partitions: usize,
    shuffle: &ShuffleConfig,
) -> DaftResult<LogicalPlanRef> {
    match plan.as_ref() {
        LogicalPlan::Source(source) => match source.source_info.as_ref() {
            SourceInfo::Physical(info) => match &info.scan_state {
                ScanState::Tasks(tasks) => {
                    let (start, end) = slice_bounds(tasks.len(), partition_idx, num_partitions);
                    let sliced: Vec<_> = tasks[start..end].to_vec();
                    let mut new_info = info.clone();
                    new_info.scan_state = ScanState::Tasks(Arc::new(sliced));
                    Ok(Arc::new(LogicalPlan::Source(Source::new(
                        source.output_schema.clone(),
                        Arc::new(SourceInfo::Physical(new_info)),
                    ))))
                }
                _ => Ok(plan.clone()),
            },
            _ => Ok(plan.clone()),
        },
        LogicalPlan::ShuffleRead(read) => {
            let mut task_read = ShuffleRead::new(
                read.output_schema.clone(),
                read.shuffle_id,
                partition_idx % num_partitions.max(1),
            );
            task_read.read_transport = read.read_transport;
            task_read.coalesce_partitions = read.coalesce_partitions.clone();
            task_read.broadcast = read.broadcast;
            task_read.fetch_retries = shuffle.fetch_retries;
            task_read.max_bytes_in_flight = shuffle.max_bytes_in_flight;
            task_read.max_concurrency_per_address = shuffle.max_concurrency_per_address;
            Ok(Arc::new(LogicalPlan::ShuffleRead(task_read)))
        }
        _ => {
            let children: Vec<LogicalPlanRef> = plan
                .as_ref()
                .children()
                .into_iter()
                .map(|child| Arc::new(child.clone()))
                .collect();
            if children.is_empty() {
                return Ok(plan.clone());
            }
            let mut new_children = Vec::with_capacity(children.len());
            for child in children {
                new_children.push(transform_for_task(
                    &child,
                    partition_idx,
                    num_partitions,
                    shuffle,
                )?);
            }
            Ok(Arc::new(plan.as_ref().with_new_children(&new_children)))
        }
    }
}

/// Bounds of one of `n` contiguous, balanced slices of `len` items.
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
