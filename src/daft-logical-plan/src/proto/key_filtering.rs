use super::*;

pub(crate) fn key_filtering_config_to_proto(
    config: &crate::ops::KeyFilteringConfig,
) -> proto::KeyFilteringConfig {
    proto::KeyFilteringConfig {
        left_key_columns: config.left_key_columns.clone(),
        right_key_columns: config.right_key_columns.clone(),
        num_workers: config.num_workers.map(|v| v as u64),
        cpus_per_worker: config.cpus_per_worker.as_ref().map(|v| v.0),
        keys_load_batch_size: config.keys_load_batch_size.map(|v| v as u64),
        max_concurrency_per_worker: config.max_concurrency_per_worker.map(|v| v as u64),
        filter_batch_size: config.filter_batch_size.map(|v| v as u64),
    }
}

pub(crate) fn key_filtering_config_from_proto(
    config: proto::KeyFilteringConfig,
) -> DaftResult<crate::ops::KeyFilteringConfig> {
    Ok(crate::ops::KeyFilteringConfig {
        left_key_columns: config.left_key_columns,
        right_key_columns: config.right_key_columns,
        num_workers: config.num_workers.map(|v| v as usize),
        cpus_per_worker: config.cpus_per_worker.map(FloatWrapper),
        keys_load_batch_size: config.keys_load_batch_size.map(|v| v as usize),
        max_concurrency_per_worker: config.max_concurrency_per_worker.map(|v| v as usize),
        filter_batch_size: config.filter_batch_size.map(|v| v as usize),
    })
}
