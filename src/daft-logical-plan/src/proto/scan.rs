use super::*;
pub(crate) fn scan_source_to_proto(source: &ScanSource) -> DaftResult<proto::ScanSource> {
    use proto::scan_source_kind::Kind;
    let kind = match &source.kind {
        ScanSourceKind::File {
            path,
            chunk_spec,
            iceberg_delete_files,
            parquet_metadata,
        } => Kind::File(proto::FileSource {
            path: path.clone(),
            chunk_spec: chunk_spec.as_ref().map(chunk_spec_to_proto),
            iceberg_delete_files: iceberg_delete_files.clone().unwrap_or_default(),
            parquet_metadata: match parquet_metadata {
                Some(metadata) => bincode::serde::encode_to_vec(
                    metadata.as_ref(),
                    bincode::config::legacy(),
                )
                .map_err(|e| {
                    DaftError::ValueError(format!(
                        "failed to bincode-encode DaftParquetMetadata: {e}"
                    ))
                })?,
                None => Vec::new(),
            },
        }),
        ScanSourceKind::Database { path } => {
            Kind::Database(proto::DatabaseSource { path: path.clone() })
        }
        #[cfg(feature = "python")]
        ScanSourceKind::PythonFactoryFunction {
            module,
            func_name,
            func_args,
        } => {
            let func_args = bincode::serde::encode_to_vec(func_args, bincode::config::legacy())
                .map_err(|e| {
                    DaftError::ValueError(format!(
                        "failed to bincode-encode PythonTablesFactoryArgs: {e}"
                    ))
                })?;
            Kind::PythonFactory(proto::PythonFactoryFunction {
                module: module.clone(),
                func_name: func_name.clone(),
                func_args,
            })
        }
    };
    Ok(proto::ScanSource {
        size_bytes: source.size_bytes,
        metadata: opt_bincode_to_bytes(&source.metadata, "TableMetadata"),
        statistics: opt_bincode_to_bytes(&source.statistics, "TableStatistics"),
        partition_spec: opt_bincode_to_bytes(&source.partition_spec, "PartitionSpec"),
        kind: Some(proto::ScanSourceKind { kind: Some(kind) }),
    })
}

pub(crate) fn scan_source_from_proto(source: proto::ScanSource) -> DaftResult<ScanSource> {
    use proto::scan_source_kind::Kind;
    let kind = match required(
        required(source.kind, "ScanSource.kind")?.kind,
        "ScanSourceKind.kind",
    )? {
        Kind::File(file) => {
            let parquet_metadata = if file.parquet_metadata.is_empty() {
                None
            } else {
                let metadata = bincode::serde::decode_from_slice::<DaftParquetMetadata, _>(
                    &file.parquet_metadata,
                    bincode::config::legacy(),
                )
                .map_err(|e| {
                    DaftError::ValueError(format!(
                        "failed to bincode-decode DaftParquetMetadata: {e}"
                    ))
                })?
                .0;
                Some(Arc::new(metadata))
            };
            ScanSourceKind::File {
                path: file.path,
                chunk_spec: file
                    .chunk_spec
                    .map(chunk_spec_from_proto)
                    .transpose()?,
                iceberg_delete_files: if file.iceberg_delete_files.is_empty() {
                    None
                } else {
                    Some(file.iceberg_delete_files)
                },
                parquet_metadata,
            }
        }
        Kind::Database(database) => ScanSourceKind::Database { path: database.path },
        Kind::PythonFactory(factory) => {
            #[cfg(feature = "python")]
            {
                let func_args =
                    bincode::serde::decode_from_slice(&factory.func_args, bincode::config::legacy())
                        .map_err(|e| {
                            DaftError::ValueError(format!(
                                "failed to bincode-decode PythonTablesFactoryArgs: {e}"
                            ))
                        })?
                        .0;
                ScanSourceKind::PythonFactoryFunction {
                    module: factory.module,
                    func_name: factory.func_name,
                    func_args,
                }
            }
            #[cfg(not(feature = "python"))]
            {
                return unsupported(
                    "ScanSourceKind::PythonFactoryFunction in a pure-Rust build",
                );
            }
        }
    };
    Ok(ScanSource {
        size_bytes: source.size_bytes,
        metadata: opt_bincode_from_bytes(&source.metadata, "TableMetadata")?,
        statistics: opt_bincode_from_bytes(&source.statistics, "TableStatistics")?,
        partition_spec: opt_bincode_from_bytes(&source.partition_spec, "PartitionSpec")?,
        kind,
    })
}

pub(crate) fn scan_task_to_proto(task: &ScanTask) -> DaftResult<proto::ScanTask> {
    Ok(proto::ScanTask {
        sources: task
            .sources
            .iter()
            .map(scan_source_to_proto)
            .collect::<DaftResult<_>>()?,
        schema: Some(task.schema.to_proto()?),
        source_config: bincode::serde::encode_to_vec(
            task.source_config.as_ref(),
            bincode::config::legacy(),
        )
        .map_err(|e| DaftError::ValueError(format!("failed to bincode-encode SourceConfig: {e}")))?,
        storage_config: bincode::serde::encode_to_vec(
            task.storage_config.as_ref(),
            bincode::config::legacy(),
        )
        .map_err(|e| {
            DaftError::ValueError(format!("failed to bincode-encode StorageConfig: {e}"))
        })?,
        pushdowns: Some(pushdowns_to_proto(&task.pushdowns)?),
        size_bytes_on_disk: task.size_bytes_on_disk,
        metadata: opt_bincode_to_bytes(&task.metadata, "TableMetadata"),
        statistics: opt_bincode_to_bytes(&task.statistics, "TableStatistics"),
        generated_fields: task
            .generated_fields
            .as_ref()
            .map(|schema| schema.to_proto())
            .transpose()?,
    })
}

pub(crate) fn scan_task_from_proto(task: proto::ScanTask) -> DaftResult<Arc<ScanTask>> {
    let source_config = bincode::serde::decode_from_slice::<SourceConfig, _>(
        &task.source_config,
        bincode::config::legacy(),
    )
    .map_err(|e| DaftError::ValueError(format!("failed to bincode-decode SourceConfig: {e}")))?
    .0;
    let storage_config = bincode::serde::decode_from_slice::<StorageConfig, _>(
        &task.storage_config,
        bincode::config::legacy(),
    )
    .map_err(|e| {
        DaftError::ValueError(format!("failed to bincode-decode StorageConfig: {e}"))
    })?
    .0;
    Ok(Arc::new(ScanTask {
        sources: task
            .sources
            .into_iter()
            .map(scan_source_from_proto)
            .collect::<DaftResult<_>>()?,
        schema: schema_from_proto_required(task.schema, "ScanTask.schema")?,
        source_config: Arc::new(source_config),
        storage_config: Arc::new(storage_config),
        pushdowns: pushdowns_from_proto(required_or_default(task.pushdowns, "ScanTask.pushdowns"))?,
        size_bytes_on_disk: task.size_bytes_on_disk,
        metadata: opt_bincode_from_bytes(&task.metadata, "TableMetadata")?,
        statistics: opt_bincode_from_bytes(&task.statistics, "TableStatistics")?,
        generated_fields: task
            .generated_fields
            .map(|schema| schema_from_proto_required(Some(schema), "ScanTask.generated_fields"))
            .transpose()?,
    }))
}

pub(crate) fn scan_state_to_proto(state: &ScanState) -> DaftResult<proto::ScanState> {
    use proto::scan_state::State;
    let state = match state {
        ScanState::Tasks(tasks) => State::Tasks(proto::ScanTaskList {
            tasks: tasks
                .iter()
                .map(|task| scan_task_to_proto(task))
                .collect::<DaftResult<_>>()?,
        }),
        ScanState::Operator(_) => {
            return unsupported(
                "ScanState::Operator (run MaterializeScans before serialization)",
            )
        }
    };
    Ok(proto::ScanState { state: Some(state) })
}

pub(crate) fn scan_state_from_proto(state: proto::ScanState) -> DaftResult<ScanState> {
    use proto::scan_state::State;
    match required(state.state, "ScanState.state")? {
        State::Tasks(list) => Ok(ScanState::Tasks(Arc::new(
            list.tasks
                .into_iter()
                .map(scan_task_from_proto)
                .collect::<DaftResult<Vec<_>>>()?,
        ))),
        State::Operator(_) => unsupported("ScanState::Operator (expected materialized tasks)"),
    }
}

pub(crate) fn physical_scan_info_to_proto(info: &PhysicalScanInfo) -> DaftResult<proto::PhysicalScanInfo> {
    Ok(proto::PhysicalScanInfo {
        scan_state: Some(scan_state_to_proto(&info.scan_state)?),
        source_schema: Some(info.source_schema.to_proto()?),
        partitioning_keys: info
            .partitioning_keys
            .iter()
            .map(partition_field_to_proto)
            .collect::<DaftResult<_>>()?,
        pushdowns: Some(pushdowns_to_proto(&info.pushdowns)?),
    })
}

pub(crate) fn physical_scan_info_from_proto(info: proto::PhysicalScanInfo) -> DaftResult<PhysicalScanInfo> {
    Ok(PhysicalScanInfo {
        scan_state: scan_state_from_proto(required(
            info.scan_state,
            "PhysicalScanInfo.scan_state",
        )?)?,
        source_schema: schema_from_proto_required(
            info.source_schema,
            "PhysicalScanInfo.source_schema",
        )?,
        partitioning_keys: info
            .partitioning_keys
            .into_iter()
            .map(partition_field_from_proto)
            .collect::<DaftResult<_>>()?,
        pushdowns: pushdowns_from_proto(required_or_default(
            info.pushdowns,
            "PhysicalScanInfo.pushdowns",
        ))?,
    })
}

pub(crate) fn source_info_to_proto(source_info: &SourceInfo) -> DaftResult<proto::SourceInfo> {
    use proto::source_info::Source;
    let source = match source_info {
        SourceInfo::InMemory(info) => Source::InMemory(proto::InMemoryInfo {
            source_schema: Some(info.source_schema.to_proto()?),
            cache_key: info.cache_key.clone(),
            num_partitions: info.num_partitions as u64,
            size_bytes: info.size_bytes as u64,
            num_rows: info.num_rows as u64,
            clustering_spec: info
                .clustering_spec
                .as_ref()
                .map(|spec| clustering_spec_to_proto(spec.as_ref()))
                .transpose()?,
            source_stage_id: info.source_stage_id.map(|id| id as u64),
        }),
        SourceInfo::GlobScan(info) => Source::GlobScan(proto::GlobScanInfo {
            glob_paths: info.glob_paths.as_ref().clone(),
            schema: Some(info.schema.to_proto()?),
            pushdowns: Some(pushdowns_to_proto(&info.pushdowns)?),
            io_config: io_config_to_bytes(&info.io_config),
        }),
        SourceInfo::PlaceHolder(info) => Source::Placeholder(proto::PlaceHolderInfo {
            source_schema: Some(info.source_schema.to_proto()?),
            clustering_spec: Some(clustering_spec_to_proto(&info.clustering_spec)?),
        }),
        SourceInfo::Physical(info) => Source::Physical(physical_scan_info_to_proto(info)?),
    };
    Ok(proto::SourceInfo { source: Some(source) })
}

pub(crate) fn source_info_from_proto(source_info: proto::SourceInfo) -> DaftResult<Arc<SourceInfo>> {
    use proto::source_info::Source;
    let source_info = match required(source_info.source, "SourceInfo oneof")? {
        Source::InMemory(info) => SourceInfo::InMemory(InMemoryInfo {
            source_schema: schema_from_proto_required(
                info.source_schema,
                "InMemoryInfo.source_schema",
            )?,
            cache_key: info.cache_key,
            // cache_entry is always stripped before serialization.
            cache_entry: None,
            num_partitions: info.num_partitions as usize,
            size_bytes: info.size_bytes as usize,
            num_rows: info.num_rows as usize,
            clustering_spec: info
                .clustering_spec
                .map(clustering_spec_from_proto)
                .transpose()?,
            source_stage_id: info.source_stage_id.map(|id| id as usize),
        }),
        Source::GlobScan(info) => SourceInfo::GlobScan(GlobScanInfo {
            glob_paths: Arc::new(info.glob_paths),
            schema: schema_from_proto_required(info.schema, "GlobScanInfo.schema")?,
            pushdowns: pushdowns_from_proto(required_or_default(
                info.pushdowns,
                "GlobScanInfo.pushdowns",
            ))?,
            io_config: io_config_from_bytes(&info.io_config)?,
        }),
        Source::Placeholder(info) => SourceInfo::PlaceHolder(PlaceHolderInfo {
            source_schema: schema_from_proto_required(
                info.source_schema,
                "PlaceHolderInfo.source_schema",
            )?,
            clustering_spec: clustering_spec_from_proto(required(
                info.clustering_spec,
                "PlaceHolderInfo.clustering_spec",
            )?)?,
        }),
        Source::Physical(info) => SourceInfo::Physical(physical_scan_info_from_proto(info)?),
    };
    Ok(Arc::new(source_info))
}

pub(crate) fn source_to_proto(source: &crate::ops::Source) -> DaftResult<proto::SourceNode> {
    Ok(proto::SourceNode {
        plan_id: source.plan_id.map(|id| id as u64),
        node_id: source.node_id.map(|id| id as u64),
        output_schema: Some(source.output_schema.to_proto()?),
        source_info: Some(source_info_to_proto(&source.source_info)?),
        stats: Some(stats_state_to_proto(&source.stats_state)),
    })
}

pub(crate) fn source_from_proto(source: proto::SourceNode) -> DaftResult<LogicalPlan> {
    let (plan_id, node_id) = node_ids_from_proto(source.plan_id, source.node_id);
    Ok(LogicalPlan::Source(crate::ops::Source {
        plan_id,
        node_id,
        output_schema: schema_from_proto_required(
            source.output_schema,
            "SourceNode.output_schema",
        )?,
        source_info: source_info_from_proto(required(
            source.source_info,
            "SourceNode.source_info",
        )?)?,
        stats_state: stats_state_from_proto(required_or_default(source.stats, "SourceNode.stats")),
    }))
}
