use super::*;
pub(crate) fn repartition_spec_to_proto(spec: &RepartitionSpec) -> DaftResult<proto::RepartitionSpec> {
    use proto::repartition_spec::Spec;
    let spec = match spec {
        RepartitionSpec::Hash(HashRepartitionConfig { num_partitions, by }) => {
            Spec::Hash(proto::HashRepartition {
                num_partitions: num_partitions.map(|n| n as u64),
                by: by.iter().map(expr_to_proto).collect::<DaftResult<_>>()?,
            })
        }
        RepartitionSpec::Random(RandomShuffleConfig { num_partitions, seed }) => {
            Spec::Random(proto::RandomRepartition {
                num_partitions: num_partitions.map(|n| n as u64),
                seed: *seed,
            })
        }
        RepartitionSpec::Range(RangeRepartitionConfig {
            num_partitions,
            boundaries,
            by,
            descending,
        }) => Spec::Range(proto::RangeRepartition {
            num_partitions: num_partitions.map(|n| n as u64),
            boundaries: record_batch_to_ipc(boundaries)?,
            by: by
                .iter()
                .map(|bound| expr_to_proto(bound.inner()))
                .collect::<DaftResult<_>>()?,
            descending: descending.clone(),
        }),
    };
    Ok(proto::RepartitionSpec { spec: Some(spec) })
}

pub(crate) fn repartition_spec_from_proto(spec: proto::RepartitionSpec) -> DaftResult<RepartitionSpec> {
    use proto::repartition_spec::Spec;
    match required(spec.spec, "RepartitionSpec.spec")? {
        Spec::Hash(hash) => Ok(RepartitionSpec::Hash(HashRepartitionConfig {
            num_partitions: hash.num_partitions.map(|n| n as usize),
            by: hash
                .by
                .into_iter()
                .map(expr_from_proto)
                .collect::<DaftResult<_>>()?,
        })),
        Spec::Random(random) => Ok(RepartitionSpec::Random(RandomShuffleConfig {
            num_partitions: random.num_partitions.map(|n| n as usize),
            seed: random.seed,
        })),
        Spec::Range(range) => {
            let by = range
                .by
                .into_iter()
                .map(expr_from_proto)
                .collect::<DaftResult<Vec<_>>>()?;
            Ok(RepartitionSpec::Range(RangeRepartitionConfig {
                num_partitions: range.num_partitions.map(|n| n as usize),
                boundaries: record_batch_from_ipc(&range.boundaries)?,
                by: by.into_iter().map(BoundExpr::new_unchecked).collect(),
                descending: range.descending,
            }))
        }
    }
}

pub(crate) fn clustering_spec_to_proto(spec: &ClusteringSpec) -> DaftResult<proto::ClusteringSpec> {
    use proto::clustering_spec::Spec;
    let spec = match spec {
        ClusteringSpec::Range(range) => Spec::Range(proto::RangeClustering {
            num_partitions: range.num_partitions as u64,
            by: range.by.iter().map(expr_to_proto).collect::<DaftResult<_>>()?,
            descending: range.descending.clone(),
        }),
        ClusteringSpec::Hash(hash) => Spec::Hash(proto::HashClustering {
            num_partitions: hash.num_partitions as u64,
            by: hash.by.iter().map(expr_to_proto).collect::<DaftResult<_>>()?,
        }),
        ClusteringSpec::Random(random) => Spec::Random(proto::RandomClustering {
            num_partitions: random.num_partitions() as u64,
        }),
        ClusteringSpec::Unknown(unknown) => Spec::Unknown(proto::UnknownClustering {
            num_partitions: spec.num_partitions() as u64,
        }),
    };
    Ok(proto::ClusteringSpec { spec: Some(spec) })
}

pub(crate) fn clustering_spec_from_proto(spec: proto::ClusteringSpec) -> DaftResult<ClusteringSpecRef> {
    use proto::clustering_spec::Spec;
    let spec = match required(spec.spec, "ClusteringSpec oneof")? {
        Spec::Range(range) => ClusteringSpec::Range(RangeClusteringConfig {
            num_partitions: range.num_partitions as usize,
            by: range.by.into_iter().map(expr_from_proto).collect::<DaftResult<_>>()?,
            descending: range.descending,
        }),
        Spec::Hash(hash) => ClusteringSpec::Hash(HashClusteringConfig {
            num_partitions: hash.num_partitions as usize,
            by: hash.by.into_iter().map(expr_from_proto).collect::<DaftResult<_>>()?,
        }),
        Spec::Random(random) => {
            ClusteringSpec::Random(RandomClusteringConfig::new(random.num_partitions as usize))
        }
        Spec::Unknown(unknown) => {
            ClusteringSpec::Unknown(UnknownClusteringConfig::new(unknown.num_partitions as usize))
        }
    };
    Ok(Arc::new(spec))
}

pub(crate) fn chunk_spec_to_proto(chunk_spec: &ChunkSpec) -> proto::ChunkSpec {
    use proto::chunk_spec::Chunk;
    let chunk = match chunk_spec {
        ChunkSpec::Parquet(values) => Chunk::Parquet(proto::RepeatedSint64 {
            values: values.clone(),
        }),
        ChunkSpec::Bytes { start, end } => Chunk::Bytes(proto::ByteRange {
            start: *start as u64,
            end: *end as u64,
        }),
    };
    proto::ChunkSpec { chunk: Some(chunk) }
}

pub(crate) fn chunk_spec_from_proto(chunk_spec: proto::ChunkSpec) -> DaftResult<ChunkSpec> {
    use proto::chunk_spec::Chunk;
    match required(chunk_spec.chunk, "ChunkSpec.chunk")? {
        Chunk::Parquet(values) => Ok(ChunkSpec::Parquet(values.values)),
        Chunk::Bytes(range) => Ok(ChunkSpec::Bytes {
            start: range.start as usize,
            end: range.end as usize,
        }),
    }
}

pub(crate) fn partition_transform_to_proto(transform: &PartitionTransform) -> proto::PartitionTransform {
    use proto::partition_transform::Transform;
    let transform = match transform {
        PartitionTransform::Identity => Transform::Identity(true),
        PartitionTransform::IcebergBucket(buckets) => Transform::IcebergBucket(*buckets),
        PartitionTransform::IcebergTruncate(width) => Transform::IcebergTruncate(*width),
        PartitionTransform::Year => Transform::Year(true),
        PartitionTransform::Month => Transform::Month(true),
        PartitionTransform::Day => Transform::Day(true),
        PartitionTransform::Hour => Transform::Hour(true),
        PartitionTransform::Void => Transform::Void(true),
    };
    proto::PartitionTransform {
        transform: Some(transform),
    }
}

pub(crate) fn partition_transform_from_proto(
    transform: proto::PartitionTransform,
) -> DaftResult<PartitionTransform> {
    use proto::partition_transform::Transform;
    match required(transform.transform, "PartitionTransform.transform")? {
        Transform::Identity(_) => Ok(PartitionTransform::Identity),
        Transform::IcebergBucket(buckets) => Ok(PartitionTransform::IcebergBucket(buckets)),
        Transform::IcebergTruncate(width) => Ok(PartitionTransform::IcebergTruncate(width)),
        Transform::Year(_) => Ok(PartitionTransform::Year),
        Transform::Month(_) => Ok(PartitionTransform::Month),
        Transform::Day(_) => Ok(PartitionTransform::Day),
        Transform::Hour(_) => Ok(PartitionTransform::Hour),
        Transform::Void(_) => Ok(PartitionTransform::Void),
    }
}

pub(crate) fn partition_field_to_proto(field: &PartitionField) -> DaftResult<proto::PartitionField> {
    Ok(proto::PartitionField {
        field: Some(field_to_proto(&field.field)?),
        source_field: field.source_field.as_ref().map(field_to_proto).transpose()?,
        transform: field.transform.as_ref().map(partition_transform_to_proto),
    })
}

pub(crate) fn partition_field_from_proto(field: proto::PartitionField) -> DaftResult<PartitionField> {
    Ok(PartitionField {
        field: proto_field_into_daft(required(field.field, "PartitionField.field")?)?,
        source_field: field
            .source_field
            .map(proto_field_into_daft)
            .transpose()?,
        transform: field
            .transform
            .map(partition_transform_from_proto)
            .transpose()?,
    })
}

