//! Helpers that make logical plans transportable between processes.

use std::sync::Arc;

use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode};

use crate::{
    LogicalPlan, LogicalPlanRef, SourceInfo,
    ops::Source,
    source_info::InMemoryInfo,
};

/// Remove in-memory partition cache entries from a plan so the serialized form
/// is byte-for-byte compatible between Python and pure-Rust builds.
///
/// ``PartitionCacheEntry`` has a ``Python(Arc<Py<PyAny>>)`` variant that only
/// exists when the ``python`` feature is enabled, so a plan holding a live
/// cache entry cannot be decoded by the pure-Rust runtime. The partition data
/// is transported separately (keyed by ``cache_key``), so the cache entry is
/// never needed after serialization.
pub fn strip_partition_cache_entries(plan: LogicalPlanRef) -> DaftResult<LogicalPlanRef> {
    plan.transform_up(|node| {
        let LogicalPlan::Source(source) = &*node else {
            return Ok(Transformed::no(node));
        };
        let SourceInfo::InMemory(info) = &*source.source_info else {
            return Ok(Transformed::no(node));
        };
        if info.cache_entry.is_none() {
            return Ok(Transformed::no(node));
        }
        let new_info = InMemoryInfo::new(
            info.source_schema.clone(),
            info.cache_key.clone(),
            None,
            info.num_partitions,
            info.size_bytes,
            info.num_rows,
            info.clustering_spec.clone(),
            info.source_stage_id,
        );
        let new_source = Source::new(
            source.output_schema.clone(),
            Arc::new(SourceInfo::InMemory(new_info)),
        );
        Ok(Transformed::yes(Arc::new(new_source.into())))
    })
    .map(|transformed| transformed.data)
}
