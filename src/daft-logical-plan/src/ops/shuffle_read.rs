use daft_schema::schema::SchemaRef;
use daft_protocol::daft::v1::ShuffleReadTransport;
use serde::{Deserialize, Serialize};

use crate::stats::StatsState;

/// Distributed execution boundary: reads one partition of an upstream shuffle.
///
/// The scheduler's stage splitter introduces these nodes when it cuts a plan
/// at `Repartition`/`IntoPartitions` boundaries. A downstream stage plan is a
/// tree whose leaves are `ShuffleRead` nodes; the executor lowers each leaf to
/// a Flight `ShuffleRead` physical operator. `partition_idx` is filled per-task
/// by the scheduler (0 in the stage-DAG placeholder).
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct ShuffleRead {
    pub plan_id: Option<usize>,
    pub node_id: Option<usize>,
    pub output_schema: SchemaRef,
    pub shuffle_id: u64,
    pub partition_idx: usize,
    /// Partitions of the upstream shuffle this task reads. Empty means the
    /// task reads exactly ``partition_idx``; filled by the scheduler when the
    /// stage coalesces upstream partitions (AQE-style) or broadcasts a small
    /// shuffle.
    pub coalesce_partitions: Vec<u64>,
    /// Read-side transport for this node, applied by the scheduler from the
    /// job's ``ShuffleConfig``. Stored as the protobuf enum value so the node
    /// keeps plain serde; use [`Self::read_transport`] for the typed value.
    pub read_transport: i32,
    /// True when the scheduler marked this shuffle for broadcast read: every
    /// reduce-side task reads all partitions into one stream.
    pub broadcast: bool,
    pub fetch_retries: u64,
    pub max_bytes_in_flight: u64,
    pub max_concurrency_per_address: u64,
    pub stats_state: StatsState,
}

impl ShuffleRead {
    pub fn new(
        output_schema: SchemaRef,
        shuffle_id: u64,
        partition_idx: usize,
    ) -> Self {
        Self {
            plan_id: None,
            node_id: None,
            output_schema,
            shuffle_id,
            partition_idx,
            coalesce_partitions: Vec::new(),
            read_transport: ShuffleReadTransport::Unspecified as i32,
            broadcast: false,
            fetch_retries: 0,
            max_bytes_in_flight: 0,
            max_concurrency_per_address: 0,
            stats_state: StatsState::NotMaterialized,
        }
    }

    pub fn read_transport(&self) -> ShuffleReadTransport {
        ShuffleReadTransport::try_from(self.read_transport)
            .unwrap_or(ShuffleReadTransport::Unspecified)
    }

    pub fn with_coalesce_partitions(mut self, partitions: Vec<u64>) -> Self {
        self.coalesce_partitions = partitions;
        self
    }

    pub fn with_read_transport(mut self, transport: ShuffleReadTransport) -> Self {
        self.read_transport = transport as i32;
        self
    }

    pub fn with_broadcast(mut self, broadcast: bool) -> Self {
        self.broadcast = broadcast;
        self
    }

    pub fn with_plan_id(mut self, plan_id: usize) -> Self {
        self.plan_id = Some(plan_id);
        self
    }

    pub fn with_node_id(mut self, node_id: usize) -> Self {
        self.node_id = Some(node_id);
        self
    }

    pub(crate) fn with_materialized_stats(mut self) -> Self {
        // A shuffle partition is exactly one partition of the upstream shuffle.
        // We cannot know its cardinality without executing, so we mark the
        // stats as unknown but keep the schema.
        self.stats_state = StatsState::NotMaterialized;
        self
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!(
            "ShuffleRead: ShuffleId = {}, PartitionIdx = {}",
            self.shuffle_id, self.partition_idx,
        ));
        if !self.coalesce_partitions.is_empty() {
            res.push(format!(
                "CoalescePartitions = {:?}",
                self.coalesce_partitions
            ));
        }
        if self.broadcast {
            res.push("Broadcast = true".to_string());
        }
        if self.read_transport() != ShuffleReadTransport::Unspecified {
            res.push(format!("ReadTransport = {:?}", self.read_transport()));
        }
        if let StatsState::Materialized(stats) = &self.stats_state {
            res.push(format!("Stats = {}", stats));
        }
        res
    }
}
