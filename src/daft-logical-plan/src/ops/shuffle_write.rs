use std::sync::Arc;

use daft_schema::schema::SchemaRef;
use serde::{Deserialize, Serialize};

use crate::{LogicalPlan, partitioning::RepartitionSpec, stats::StatsState};

/// Distributed execution boundary: writes the input to a shuffle.
///
/// The scheduler's stage splitter wraps the tail of every non-final stage in
/// one of these nodes. The executor lowers it to a Flight `RepartitionWrite`
/// physical operator that writes one partition per task into the executor's
/// local Flight shuffle server, then reports the server address + cache ids
/// to the scheduler. `num_partitions` is the target partition count of the
/// shuffle (the downstream stage's task count).
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct ShuffleWrite {
    pub plan_id: Option<usize>,
    pub node_id: Option<usize>,
    // Upstream node.
    pub input: Arc<LogicalPlan>,
    pub shuffle_id: u64,
    pub num_partitions: usize,
    pub repartition_spec: Option<RepartitionSpec>,
    pub output_schema: SchemaRef,
    pub shuffle_dirs: Vec<String>,
    pub compression: Option<String>,
    /// Optional object-store URI (e.g. "s3://bucket/prefix") where this
    /// shuffle's partition files are spilled. Empty means executor-local
    /// disk, served over the writer's Flight server for remote readers.
    pub storage_uri: String,
    pub stats_state: StatsState,
}

impl ShuffleWrite {
    pub fn new(
        input: Arc<LogicalPlan>,
        shuffle_id: u64,
        num_partitions: usize,
        repartition_spec: Option<RepartitionSpec>,
        shuffle_dirs: Vec<String>,
        compression: Option<String>,
    ) -> Self {
        let output_schema = input.schema();
        Self {
            plan_id: None,
            node_id: None,
            input,
            shuffle_id,
            num_partitions,
            repartition_spec,
            output_schema,
            shuffle_dirs,
            compression,
            storage_uri: String::new(),
            stats_state: StatsState::NotMaterialized,
        }
    }

    pub fn with_storage_uri(mut self, storage_uri: String) -> Self {
        self.storage_uri = storage_uri;
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
        // Shuffle writing does not change the input cardinality.
        let input_stats = self.input.materialized_stats();
        self.stats_state = StatsState::Materialized(input_stats.clone().into());
        self
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!(
            "ShuffleWrite: ShuffleId = {}, NumPartitions = {}",
            self.shuffle_id, self.num_partitions,
        ));
        if let Some(spec) = &self.repartition_spec {
            res.push(format!("Scheme = {}", spec.var_name()));
            res.extend(spec.multiline_display());
        }
        if let StatsState::Materialized(stats) = &self.stats_state {
            res.push(format!("Stats = {}", stats));
        }
        res
    }
}
