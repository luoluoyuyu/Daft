use daft_schema::schema::SchemaRef;
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
            stats_state: StatsState::NotMaterialized,
        }
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
        if let StatsState::Materialized(stats) = &self.stats_state {
            res.push(format!("Stats = {}", stats));
        }
        res
    }
}
