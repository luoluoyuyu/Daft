use super::*;
pub fn plan_to_proto(plan: &LogicalPlan) -> DaftResult<proto::LogicalPlan> {
    use proto::logical_plan::Node;
    let node = match plan {
        LogicalPlan::Source(source) => Node::Source(source_to_proto(source)?),
        LogicalPlan::Shard(shard) => {
            let (plan_id, node_id) = node_ids_to_proto(&shard.plan_id, &shard.node_id);
            Node::Shard(Box::new(proto::ShardNode {
                plan_id,
                node_id,
                input: Some(Box::new(plan_to_proto(&shard.input)?)),
                sharder: Some(sharder_to_proto(&shard.sharder)),
                stats: Some(stats_state_to_proto(&shard.stats_state)),
            }))
        }
        LogicalPlan::Project(project) => {
            let (plan_id, node_id) = node_ids_to_proto(&project.plan_id, &project.node_id);
            Node::Project(Box::new(proto::ProjectNode {
                plan_id,
                node_id,
                input: Some(Box::new(plan_to_proto(&project.input)?)),
                projection: project
                    .projection
                    .iter()
                    .map(|expr| expr_to_proto(expr))
                    .collect::<DaftResult<_>>()?,
                projected_schema: Some(project.projected_schema.to_proto()?),
                stats: Some(stats_state_to_proto(&project.stats_state)),
            }))
        }
        LogicalPlan::Filter(filter) => {
            let (plan_id, node_id) = node_ids_to_proto(&filter.plan_id, &filter.node_id);
            Node::Filter(Box::new(proto::FilterNode {
                plan_id,
                node_id,
                input: Some(Box::new(plan_to_proto(&filter.input)?)),
                predicate: Some(expr_to_proto(&filter.predicate)?),
                stats: Some(stats_state_to_proto(&filter.stats_state)),
            }))
        }
        LogicalPlan::IntoBatches(into_batches) => {
            let (plan_id, node_id) =
                node_ids_to_proto(&into_batches.plan_id, &into_batches.node_id);
            Node::IntoBatches(Box::new(proto::IntoBatchesNode {
                plan_id,
                node_id,
                input: Some(Box::new(plan_to_proto(&into_batches.input)?)),
                batch_size: into_batches.batch_size as u64,
                stats: Some(stats_state_to_proto(&into_batches.stats_state)),
            }))
        }
        LogicalPlan::Limit(limit) => {
            let (plan_id, node_id) = node_ids_to_proto(&limit.plan_id, &limit.node_id);
            Node::Limit(Box::new(proto::LimitNode {
                plan_id,
                node_id,
                input: Some(Box::new(plan_to_proto(&limit.input)?)),
                limit: limit.limit,
                offset: limit.offset,
                eager: limit.eager,
                stats: Some(stats_state_to_proto(&limit.stats_state)),
            }))
        }
        LogicalPlan::Offset(offset) => {
            let (plan_id, node_id) = node_ids_to_proto(&offset.plan_id, &offset.node_id);
            Node::Offset(Box::new(proto::OffsetNode {
                plan_id,
                node_id,
                input: Some(Box::new(plan_to_proto(&offset.input)?)),
                offset: offset.offset,
                stats: Some(stats_state_to_proto(&offset.stats_state)),
            }))
        }
        LogicalPlan::UDFProject(udf_project) => {
            let (plan_id, node_id) = node_ids_to_proto(&udf_project.plan_id, &udf_project.node_id);
            Node::UdfProject(Box::new(proto::UdfProjectNode {
                plan_id,
                node_id,
                input: Some(Box::new(plan_to_proto(&udf_project.input)?)),
                expr: Some(expr_to_proto(&udf_project.expr)?),
                udf_properties: Some(udf_properties_to_proto(&udf_project.udf_properties)?),
                passthrough_columns: udf_project
                    .passthrough_columns
                    .iter()
                    .map(expr_to_proto)
                    .collect::<DaftResult<_>>()?,
                projected_schema: Some(udf_project.projected_schema.to_proto()?),
                stats: Some(stats_state_to_proto(&udf_project.stats_state)),
            }))
        }
        LogicalPlan::Explode(explode) => {
            let (plan_id, node_id) = node_ids_to_proto(&explode.plan_id, &explode.node_id);
            Node::Explode(Box::new(proto::ExplodeNode {
                plan_id,
                node_id,
                input: Some(Box::new(plan_to_proto(&explode.input)?)),
                to_explode: explode
                    .to_explode
                    .iter()
                    .map(expr_to_proto)
                    .collect::<DaftResult<_>>()?,
                ignore_empty_and_null: explode.ignore_empty_and_null,
                index_column: explode.index_column.clone(),
                exploded_schema: Some(explode.exploded_schema.to_proto()?),
                stats: Some(stats_state_to_proto(&explode.stats_state)),
            }))
        }
        LogicalPlan::Unpivot(unpivot) => {
            let (plan_id, node_id) = node_ids_to_proto(&unpivot.plan_id, &unpivot.node_id);
            Node::Unpivot(Box::new(proto::UnpivotNode {
                plan_id,
                node_id,
                input: Some(Box::new(plan_to_proto(&unpivot.input)?)),
                ids: unpivot
                    .ids
                    .iter()
                    .map(expr_to_proto)
                    .collect::<DaftResult<_>>()?,
                values: unpivot
                    .values
                    .iter()
                    .map(expr_to_proto)
                    .collect::<DaftResult<_>>()?,
                variable_name: unpivot.variable_name.clone(),
                value_name: unpivot.value_name.clone(),
                output_schema: Some(unpivot.output_schema.to_proto()?),
                stats: Some(stats_state_to_proto(&unpivot.stats_state)),
            }))
        }
        LogicalPlan::Sort(sort) => {
            let (plan_id, node_id) = node_ids_to_proto(&sort.plan_id, &sort.node_id);
            Node::Sort(Box::new(proto::SortNode {
                plan_id,
                node_id,
                input: Some(Box::new(plan_to_proto(&sort.input)?)),
                sort_by: sort
                    .sort_by
                    .iter()
                    .map(expr_to_proto)
                    .collect::<DaftResult<_>>()?,
                descending: sort.descending.clone(),
                nulls_first: sort.nulls_first.clone(),
                stats: Some(stats_state_to_proto(&sort.stats_state)),
            }))
        }
        LogicalPlan::Repartition(repartition) => {
            let (plan_id, node_id) =
                node_ids_to_proto(&repartition.plan_id, &repartition.node_id);
            Node::Repartition(Box::new(proto::RepartitionNode {
                plan_id,
                node_id,
                input: Some(Box::new(plan_to_proto(&repartition.input)?)),
                spec: Some(repartition_spec_to_proto(&repartition.repartition_spec)?),
                stats: Some(stats_state_to_proto(&repartition.stats_state)),
            }))
        }
        LogicalPlan::IntoPartitions(into_partitions) => {
            let (plan_id, node_id) =
                node_ids_to_proto(&into_partitions.plan_id, &into_partitions.node_id);
            Node::IntoPartitions(Box::new(proto::IntoPartitionsNode {
                plan_id,
                node_id,
                input: Some(Box::new(plan_to_proto(&into_partitions.input)?)),
                num_partitions: into_partitions.num_partitions as u64,
                stats: Some(stats_state_to_proto(&into_partitions.stats_state)),
            }))
        }
        LogicalPlan::ShuffleRead(shuffle_read) => {
            let (plan_id, node_id) =
                node_ids_to_proto(&shuffle_read.plan_id, &shuffle_read.node_id);
            Node::ShuffleRead(proto::ShuffleReadNode {
                plan_id,
                node_id,
                output_schema: Some(shuffle_read.output_schema.to_proto()?),
                shuffle_id: shuffle_read.shuffle_id,
                partition_idx: shuffle_read.partition_idx as u64,
                stats: Some(stats_state_to_proto(&shuffle_read.stats_state)),
            })
        }
        LogicalPlan::ShuffleWrite(shuffle_write) => {
            let (plan_id, node_id) =
                node_ids_to_proto(&shuffle_write.plan_id, &shuffle_write.node_id);
            Node::ShuffleWrite(Box::new(proto::ShuffleWriteNode {
                plan_id,
                node_id,
                input: Some(Box::new(plan_to_proto(&shuffle_write.input)?)),
                shuffle_id: shuffle_write.shuffle_id,
                num_partitions: shuffle_write.num_partitions as u64,
                spec: shuffle_write
                    .repartition_spec
                    .as_ref()
                    .map(repartition_spec_to_proto)
                    .transpose()?,
                output_schema: Some(shuffle_write.output_schema.to_proto()?),
                shuffle_dirs: shuffle_write.shuffle_dirs.clone(),
                compression: shuffle_write.compression.clone(),
                stats: Some(stats_state_to_proto(&shuffle_write.stats_state)),
            }))
        }
        LogicalPlan::Distinct(distinct) => {
            let (plan_id, node_id) = node_ids_to_proto(&distinct.plan_id, &distinct.node_id);
            Node::Distinct(Box::new(proto::DistinctNode {
                plan_id,
                node_id,
                input: Some(Box::new(plan_to_proto(&distinct.input)?)),
                columns: distinct
                    .columns
                    .as_ref()
                    .map(|columns| -> DaftResult<proto::ExpressionList> {
                        Ok(proto::ExpressionList {
                            items: columns
                                .iter()
                                .map(expr_to_proto)
                                .collect::<DaftResult<_>>()?,
                        })
                    })
                    .transpose()?,
                stats: Some(stats_state_to_proto(&distinct.stats_state)),
            }))
        }
        LogicalPlan::Aggregate(aggregate) => {
            let (plan_id, node_id) = node_ids_to_proto(&aggregate.plan_id, &aggregate.node_id);
            Node::Aggregate(Box::new(proto::AggregateNode {
                plan_id,
                node_id,
                input: Some(Box::new(plan_to_proto(&aggregate.input)?)),
                aggregations: aggregate
                    .aggregations
                    .iter()
                    .map(expr_to_proto)
                    .collect::<DaftResult<_>>()?,
                groupby: aggregate
                    .groupby
                    .iter()
                    .map(expr_to_proto)
                    .collect::<DaftResult<_>>()?,
                output_schema: Some(aggregate.output_schema.to_proto()?),
                stats: Some(stats_state_to_proto(&aggregate.stats_state)),
            }))
        }
        LogicalPlan::Pivot(pivot) => {
            let (plan_id, node_id) = node_ids_to_proto(&pivot.plan_id, &pivot.node_id);
            Node::Pivot(Box::new(proto::PivotNode {
                plan_id,
                node_id,
                input: Some(Box::new(plan_to_proto(&pivot.input)?)),
                group_by: pivot
                    .group_by
                    .iter()
                    .map(expr_to_proto)
                    .collect::<DaftResult<_>>()?,
                pivot_column: Some(expr_to_proto(&pivot.pivot_column)?),
                value_column: Some(expr_to_proto(&pivot.value_column)?),
                aggregation: Some(agg_expr_to_proto(&pivot.aggregation)?),
                names: pivot.names.clone(),
                output_schema: Some(pivot.output_schema.to_proto()?),
                stats: Some(stats_state_to_proto(&pivot.stats_state)),
            }))
        }
        LogicalPlan::Concat(concat) => {
            let (plan_id, node_id) = node_ids_to_proto(&concat.plan_id, &concat.node_id);
            Node::Concat(Box::new(proto::ConcatNode {
                plan_id,
                node_id,
                input: Some(Box::new(plan_to_proto(&concat.input)?)),
                other: Some(Box::new(plan_to_proto(&concat.other)?)),
                stats: Some(stats_state_to_proto(&concat.stats_state)),
            }))
        }
        LogicalPlan::Intersect(intersect) => {
            let (plan_id, node_id) = node_ids_to_proto(&intersect.plan_id, &intersect.node_id);
            Node::Intersect(Box::new(proto::IntersectNode {
                plan_id,
                node_id,
                lhs: Some(Box::new(plan_to_proto(&intersect.lhs)?)),
                rhs: Some(Box::new(plan_to_proto(&intersect.rhs)?)),
                is_all: intersect.is_all,
            }))
        }
        LogicalPlan::Union(union) => {
            let (plan_id, node_id) = node_ids_to_proto(&union.plan_id, &union.node_id);
            Node::Union(Box::new(proto::UnionNode {
                plan_id,
                node_id,
                lhs: Some(Box::new(plan_to_proto(&union.lhs)?)),
                rhs: Some(Box::new(plan_to_proto(&union.rhs)?)),
                quantifier: set_quantifier_to_proto(union.quantifier) as i32,
                strategy: union_strategy_to_proto(union.strategy) as i32,
            }))
        }
        LogicalPlan::Join(join) => {
            let (plan_id, node_id) = node_ids_to_proto(&join.plan_id, &join.node_id);
            Node::Join(Box::new(proto::JoinNode {
                plan_id,
                node_id,
                left: Some(Box::new(plan_to_proto(&join.left)?)),
                right: Some(Box::new(plan_to_proto(&join.right)?)),
                on: join
                    .on
                    .inner()
                    .map(|pred| expr_to_proto(pred))
                    .transpose()?,
                join_type: join_type_to_proto(join.join_type) as i32,
                join_strategy: join.join_strategy.map(|s| join_strategy_to_proto(s) as i32),
                output_schema: Some(join.output_schema.to_proto()?),
                stats: Some(stats_state_to_proto(&join.stats_state)),
                key_filtering_config: join
                    .key_filtering_config
                    .as_ref()
                    .map(key_filtering_config_to_proto),
            }))
        }
        LogicalPlan::Sink(sink) => {
            let (plan_id, node_id) = node_ids_to_proto(&sink.plan_id, &sink.node_id);
            Node::Sink(Box::new(proto::SinkNode {
                plan_id,
                node_id,
                input: Some(Box::new(plan_to_proto(&sink.input)?)),
                schema: Some(sink.schema.to_proto()?),
                sink_info: Some(sink_info_to_proto(&sink.sink_info)?),
                stats: Some(stats_state_to_proto(&sink.stats_state)),
            }))
        }
        LogicalPlan::Sample(sample) => {
            let (plan_id, node_id) = node_ids_to_proto(&sample.plan_id, &sample.node_id);
            Node::Sample(Box::new(proto::SampleNode {
                plan_id,
                node_id,
                input: Some(Box::new(plan_to_proto(&sample.input)?)),
                fraction: sample.fraction,
                size: sample.size.map(|size| size as u64),
                with_replacement: sample.with_replacement,
                seed: sample.seed,
                stats: Some(stats_state_to_proto(&sample.stats_state)),
            }))
        }
        LogicalPlan::Shuffle(shuffle) => {
            let (plan_id, node_id) = node_ids_to_proto(&shuffle.plan_id, &shuffle.node_id);
            Node::Shuffle(Box::new(proto::ShuffleNode {
                plan_id,
                node_id,
                input: Some(Box::new(plan_to_proto(&shuffle.input)?)),
                seed: shuffle.seed,
                stats: Some(stats_state_to_proto(&shuffle.stats_state)),
            }))
        }
        LogicalPlan::MonotonicallyIncreasingId(mono) => {
            let (plan_id, node_id) = node_ids_to_proto(&mono.plan_id, &mono.node_id);
            Node::MonotonicallyIncreasingId(Box::new(proto::MonotonicallyIncreasingIdNode {
                plan_id,
                node_id,
                input: Some(Box::new(plan_to_proto(&mono.input)?)),
                schema: Some(mono.schema.to_proto()?),
                column_name: mono.column_name.clone(),
                starting_offset: mono.starting_offset,
                stats: Some(stats_state_to_proto(&mono.stats_state)),
            }))
        }
        LogicalPlan::SubqueryAlias(subquery_alias) => {
            let (plan_id, node_id) =
                node_ids_to_proto(&subquery_alias.plan_id, &subquery_alias.node_id);
            Node::SubqueryAlias(Box::new(proto::SubqueryAliasNode {
                plan_id,
                node_id,
                input: Some(Box::new(plan_to_proto(&subquery_alias.input)?)),
                name: subquery_alias.name.to_string(),
                stats: None,
            }))
        }
        LogicalPlan::Window(window) => {
            let (plan_id, node_id) = node_ids_to_proto(&window.plan_id, &window.node_id);
            Node::Window(Box::new(proto::WindowNode {
                plan_id,
                node_id,
                input: Some(Box::new(plan_to_proto(&window.input)?)),
                window_functions: window
                    .window_functions
                    .iter()
                    .map(window_expr_to_proto)
                    .collect::<DaftResult<_>>()?,
                aliases: window.aliases.clone(),
                window_spec: Some(window_spec_to_proto(&window.window_spec)?),
                schema: Some(window.schema.to_proto()?),
                stats: Some(stats_state_to_proto(&window.stats_state)),
            }))
        }
        LogicalPlan::TopN(top_n) => {
            let (plan_id, node_id) = node_ids_to_proto(&top_n.plan_id, &top_n.node_id);
            Node::TopN(Box::new(proto::TopNNode {
                plan_id,
                node_id,
                input: Some(Box::new(plan_to_proto(&top_n.input)?)),
                sort_by: top_n
                    .sort_by
                    .iter()
                    .map(expr_to_proto)
                    .collect::<DaftResult<Vec<_>>>()?,
                descending: top_n.descending.clone(),
                nulls_first: top_n.nulls_first.clone(),
                limit: top_n.limit,
                offset: top_n.offset,
                stats: Some(stats_state_to_proto(&top_n.stats_state)),
            }))
        }
        LogicalPlan::VLLMProject(vllm) => {
            let (plan_id, node_id) = node_ids_to_proto(&vllm.plan_id, &vllm.node_id);
            Node::VllmProject(Box::new(proto::VllmProjectNode {
                plan_id,
                node_id,
                expr: Some(vllm_expr_to_proto(&vllm.expr)?),
                input: Some(Box::new(plan_to_proto(&vllm.input)?)),
                output_column_name: vllm.output_column_name.to_string(),
                output_schema: Some(vllm.output_schema.to_proto()?),
                stats: Some(stats_state_to_proto(&vllm.stats_state)),
            }))
        }
    };
    Ok(proto::LogicalPlan { node: Some(node) })
}

pub fn plan_from_proto(plan: proto::LogicalPlan) -> DaftResult<Arc<LogicalPlan>> {
    use proto::logical_plan::Node;
    let plan = match required(plan.node, "LogicalPlan oneof")? {
        Node::Source(source) => source_from_proto(source)?,
        Node::Shard(shard) => {
            let (plan_id, node_id) = node_ids_from_proto(shard.plan_id, shard.node_id);
            LogicalPlan::Shard(Shard {
                plan_id,
                node_id,
                input: plan_from_proto(required_boxed_plan(shard.input, "ShardNode.input")?)?,
                sharder: sharder_from_proto(required(shard.sharder, "ShardNode.sharder")?)?,
                stats_state: stats_state_from_proto(required_or_default(
                    shard.stats,
                    "ShardNode.stats",
                )),
            })
        }
        Node::Project(project) => {
            let (plan_id, node_id) = node_ids_from_proto(project.plan_id, project.node_id);
            LogicalPlan::Project(Project {
                plan_id,
                node_id,
                input: plan_from_proto(required_boxed_plan(
                    project.input,
                    "ProjectNode.input",
                )?)?,
                projection: project
                    .projection
                    .into_iter()
                    .map(expr_from_proto)
                    .collect::<DaftResult<_>>()?,
                projected_schema: schema_from_proto_required(
                    project.projected_schema,
                    "ProjectNode.projected_schema",
                )?,
                stats_state: stats_state_from_proto(required_or_default(
                    project.stats,
                    "ProjectNode.stats",
                )),
            })
        }
        Node::Filter(filter) => {
            let (plan_id, node_id) = node_ids_from_proto(filter.plan_id, filter.node_id);
            LogicalPlan::Filter(Filter {
                plan_id,
                node_id,
                input: plan_from_proto(required_boxed_plan(filter.input, "FilterNode.input")?)?,
                predicate: required_expr_direct(filter.predicate, "FilterNode.predicate")?,
                stats_state: stats_state_from_proto(required_or_default(
                    filter.stats,
                    "FilterNode.stats",
                )),
            })
        }
        Node::IntoBatches(into_batches) => {
            let (plan_id, node_id) =
                node_ids_from_proto(into_batches.plan_id, into_batches.node_id);
            LogicalPlan::IntoBatches(IntoBatches {
                plan_id,
                node_id,
                input: plan_from_proto(required_boxed_plan(
                    into_batches.input,
                    "IntoBatchesNode.input",
                )?)?,
                batch_size: into_batches.batch_size as usize,
                stats_state: stats_state_from_proto(required_or_default(
                    into_batches.stats,
                    "IntoBatchesNode.stats",
                )),
            })
        }
        Node::Limit(limit) => {
            let (plan_id, node_id) = node_ids_from_proto(limit.plan_id, limit.node_id);
            LogicalPlan::Limit(Limit {
                plan_id,
                node_id,
                input: plan_from_proto(required_boxed_plan(limit.input, "LimitNode.input")?)?,
                limit: limit.limit,
                offset: limit.offset,
                eager: limit.eager,
                stats_state: stats_state_from_proto(required_or_default(
                    limit.stats,
                    "LimitNode.stats",
                )),
            })
        }
        Node::Offset(offset) => {
            let (plan_id, node_id) = node_ids_from_proto(offset.plan_id, offset.node_id);
            LogicalPlan::Offset(Offset {
                plan_id,
                node_id,
                input: plan_from_proto(required_boxed_plan(offset.input, "OffsetNode.input")?)?,
                offset: offset.offset,
                stats_state: stats_state_from_proto(required_or_default(
                    offset.stats,
                    "OffsetNode.stats",
                )),
            })
        }
        Node::UdfProject(udf_project) => {
            let (plan_id, node_id) = node_ids_from_proto(udf_project.plan_id, udf_project.node_id);
            LogicalPlan::UDFProject(UDFProject {
                plan_id,
                node_id,
                input: plan_from_proto(required_boxed_plan(
                    udf_project.input,
                    "UdfProjectNode.input",
                )?)?,
                expr: expr_from_proto(required(udf_project.expr, "UdfProjectNode.expr")?)?,
                udf_properties: udf_properties_from_proto(required(
                    udf_project.udf_properties,
                    "UdfProjectNode.udf_properties",
                )?)?,
                passthrough_columns: udf_project
                    .passthrough_columns
                    .into_iter()
                    .map(expr_from_proto)
                    .collect::<DaftResult<_>>()?,
                projected_schema: schema_from_proto_required(
                    udf_project.projected_schema,
                    "UdfProjectNode.projected_schema",
                )?,
                stats_state: stats_state_from_proto(required_or_default(
                    udf_project.stats,
                    "UdfProjectNode.stats",
                )),
            })
        }
        Node::Explode(explode) => {
            let (plan_id, node_id) = node_ids_from_proto(explode.plan_id, explode.node_id);
            LogicalPlan::Explode(Explode {
                plan_id,
                node_id,
                input: plan_from_proto(required_boxed_plan(explode.input, "ExplodeNode.input")?)?,
                to_explode: explode
                    .to_explode
                    .into_iter()
                    .map(expr_from_proto)
                    .collect::<DaftResult<_>>()?,
                ignore_empty_and_null: explode.ignore_empty_and_null,
                index_column: explode.index_column,
                exploded_schema: schema_from_proto_required(
                    explode.exploded_schema,
                    "ExplodeNode.exploded_schema",
                )?,
                stats_state: stats_state_from_proto(required_or_default(
                    explode.stats,
                    "ExplodeNode.stats",
                )),
            })
        }
        Node::Unpivot(unpivot) => {
            let (plan_id, node_id) = node_ids_from_proto(unpivot.plan_id, unpivot.node_id);
            LogicalPlan::Unpivot(Unpivot {
                plan_id,
                node_id,
                input: plan_from_proto(required_boxed_plan(unpivot.input, "UnpivotNode.input")?)?,
                ids: unpivot
                    .ids
                    .into_iter()
                    .map(expr_from_proto)
                    .collect::<DaftResult<_>>()?,
                values: unpivot
                    .values
                    .into_iter()
                    .map(expr_from_proto)
                    .collect::<DaftResult<_>>()?,
                variable_name: unpivot.variable_name,
                value_name: unpivot.value_name,
                output_schema: schema_from_proto_required(
                    unpivot.output_schema,
                    "UnpivotNode.output_schema",
                )?,
                stats_state: stats_state_from_proto(required_or_default(
                    unpivot.stats,
                    "UnpivotNode.stats",
                )),
            })
        }
        Node::Sort(sort) => {
            let (plan_id, node_id) = node_ids_from_proto(sort.plan_id, sort.node_id);
            LogicalPlan::Sort(Sort {
                plan_id,
                node_id,
                input: plan_from_proto(required_boxed_plan(sort.input, "SortNode.input")?)?,
                sort_by: sort
                    .sort_by
                    .into_iter()
                    .map(expr_from_proto)
                    .collect::<DaftResult<_>>()?,
                descending: sort.descending,
                nulls_first: sort.nulls_first,
                stats_state: stats_state_from_proto(required_or_default(
                    sort.stats,
                    "SortNode.stats",
                )),
            })
        }
        Node::Repartition(repartition) => {
            let (plan_id, node_id) =
                node_ids_from_proto(repartition.plan_id, repartition.node_id);
            LogicalPlan::Repartition(Repartition {
                plan_id,
                node_id,
                input: plan_from_proto(required_boxed_plan(
                    repartition.input,
                    "RepartitionNode.input",
                )?)?,
                repartition_spec: repartition_spec_from_proto(required(
                    repartition.spec,
                    "RepartitionNode.spec",
                )?)?,
                stats_state: stats_state_from_proto(required_or_default(
                    repartition.stats,
                    "RepartitionNode.stats",
                )),
            })
        }
        Node::IntoPartitions(into_partitions) => {
            let (plan_id, node_id) =
                node_ids_from_proto(into_partitions.plan_id, into_partitions.node_id);
            LogicalPlan::IntoPartitions(IntoPartitions {
                plan_id,
                node_id,
                input: plan_from_proto(required_boxed_plan(
                    into_partitions.input,
                    "IntoPartitionsNode.input",
                )?)?,
                num_partitions: into_partitions.num_partitions as usize,
                stats_state: stats_state_from_proto(required_or_default(
                    into_partitions.stats,
                    "IntoPartitionsNode.stats",
                )),
            })
        }
        Node::ShuffleRead(shuffle_read) => {
            let (plan_id, node_id) =
                node_ids_from_proto(shuffle_read.plan_id, shuffle_read.node_id);
            LogicalPlan::ShuffleRead(ShuffleRead {
                plan_id,
                node_id,
                output_schema: schema_from_proto_required(
                    shuffle_read.output_schema,
                    "ShuffleReadNode.output_schema",
                )?,
                shuffle_id: shuffle_read.shuffle_id,
                partition_idx: shuffle_read.partition_idx as usize,
                stats_state: stats_state_from_proto(required_or_default(
                    shuffle_read.stats,
                    "ShuffleReadNode.stats",
                )),
            })
        }
        Node::ShuffleWrite(shuffle_write) => {
            let (plan_id, node_id) =
                node_ids_from_proto(shuffle_write.plan_id, shuffle_write.node_id);
            LogicalPlan::ShuffleWrite(ShuffleWrite {
                plan_id,
                node_id,
                input: plan_from_proto(required_boxed_plan(
                    shuffle_write.input,
                    "ShuffleWriteNode.input",
                )?)?,
                shuffle_id: shuffle_write.shuffle_id,
                num_partitions: shuffle_write.num_partitions as usize,
                repartition_spec: shuffle_write
                    .spec
                    .map(repartition_spec_from_proto)
                    .transpose()?,
                output_schema: schema_from_proto_required(
                    shuffle_write.output_schema,
                    "ShuffleWriteNode.output_schema",
                )?,
                shuffle_dirs: shuffle_write.shuffle_dirs,
                compression: shuffle_write.compression,
                stats_state: stats_state_from_proto(required_or_default(
                    shuffle_write.stats,
                    "ShuffleWriteNode.stats",
                )),
            })
        }
        Node::Distinct(distinct) => {
            let (plan_id, node_id) = node_ids_from_proto(distinct.plan_id, distinct.node_id);
            LogicalPlan::Distinct(Distinct {
                plan_id,
                node_id,
                input: plan_from_proto(required_boxed_plan(distinct.input, "DistinctNode.input")?)?,
                stats_state: stats_state_from_proto(required_or_default(
                    distinct.stats,
                    "DistinctNode.stats",
                )),
                columns: distinct
                    .columns
                    .map(|columns| {
                        columns
                            .items
                            .into_iter()
                            .map(expr_from_proto)
                            .collect::<DaftResult<Vec<_>>>()
                    })
                    .transpose()?,
            })
        }
        Node::Aggregate(aggregate) => {
            let (plan_id, node_id) = node_ids_from_proto(aggregate.plan_id, aggregate.node_id);
            LogicalPlan::Aggregate(Aggregate {
                plan_id,
                node_id,
                input: plan_from_proto(required_boxed_plan(
                    aggregate.input,
                    "AggregateNode.input",
                )?)?,
                aggregations: aggregate
                    .aggregations
                    .into_iter()
                    .map(expr_from_proto)
                    .collect::<DaftResult<_>>()?,
                groupby: aggregate
                    .groupby
                    .into_iter()
                    .map(expr_from_proto)
                    .collect::<DaftResult<_>>()?,
                output_schema: schema_from_proto_required(
                    aggregate.output_schema,
                    "AggregateNode.output_schema",
                )?,
                stats_state: stats_state_from_proto(required_or_default(
                    aggregate.stats,
                    "AggregateNode.stats",
                )),
            })
        }
        Node::Pivot(pivot) => {
            let (plan_id, node_id) = node_ids_from_proto(pivot.plan_id, pivot.node_id);
            LogicalPlan::Pivot(Pivot {
                plan_id,
                node_id,
                input: plan_from_proto(required_boxed_plan(pivot.input, "PivotNode.input")?)?,
                group_by: pivot
                    .group_by
                    .into_iter()
                    .map(expr_from_proto)
                    .collect::<DaftResult<_>>()?,
                pivot_column: expr_from_proto(required(
                    pivot.pivot_column,
                    "PivotNode.pivot_column",
                )?)?,
                value_column: expr_from_proto(required(
                    pivot.value_column,
                    "PivotNode.value_column",
                )?)?,
                aggregation: agg_expr_from_proto(required(
                    pivot.aggregation,
                    "PivotNode.aggregation",
                )?)?,
                names: pivot.names,
                output_schema: schema_from_proto_required(
                    pivot.output_schema,
                    "PivotNode.output_schema",
                )?,
                stats_state: stats_state_from_proto(required_or_default(
                    pivot.stats,
                    "PivotNode.stats",
                )),
            })
        }
        Node::Concat(concat) => {
            let (plan_id, node_id) = node_ids_from_proto(concat.plan_id, concat.node_id);
            LogicalPlan::Concat(Concat {
                plan_id,
                node_id,
                input: plan_from_proto(required_boxed_plan(concat.input, "ConcatNode.input")?)?,
                other: plan_from_proto(required_boxed_plan(concat.other, "ConcatNode.other")?)?,
                stats_state: stats_state_from_proto(required_or_default(
                    concat.stats,
                    "ConcatNode.stats",
                )),
            })
        }
        Node::Intersect(intersect) => {
            let (plan_id, node_id) = node_ids_from_proto(intersect.plan_id, intersect.node_id);
            LogicalPlan::Intersect(crate::ops::Intersect {
                plan_id,
                node_id,
                lhs: plan_from_proto(required_boxed_plan(intersect.lhs, "IntersectNode.lhs")?)?,
                rhs: plan_from_proto(required_boxed_plan(intersect.rhs, "IntersectNode.rhs")?)?,
                is_all: intersect.is_all,
            })
        }
        Node::Union(union) => {
            let (plan_id, node_id) = node_ids_from_proto(union.plan_id, union.node_id);
            LogicalPlan::Union(crate::ops::Union {
                plan_id,
                node_id,
                lhs: plan_from_proto(required_boxed_plan(union.lhs, "UnionNode.lhs")?)?,
                rhs: plan_from_proto(required_boxed_plan(union.rhs, "UnionNode.rhs")?)?,
                quantifier: set_quantifier_from_proto(
                    proto::SetQuantifier::try_from(union.quantifier)
                        .map_err(|_| DaftError::ValueError("invalid SetQuantifier".to_string()))?,
                )?,
                strategy: union_strategy_from_proto(
                    proto::UnionStrategy::try_from(union.strategy).map_err(|_| {
                        DaftError::ValueError("invalid UnionStrategy".to_string())
                    })?,
                )?,
            })
        }
        Node::Join(join) => {
            let (plan_id, node_id) = node_ids_from_proto(join.plan_id, join.node_id);
            LogicalPlan::Join(crate::ops::Join {
                plan_id,
                node_id,
                left: plan_from_proto(required_boxed_plan(join.left, "JoinNode.left")?)?,
                right: plan_from_proto(required_boxed_plan(join.right, "JoinNode.right")?)?,
                on: crate::ops::join::JoinPredicate::try_new(
                    join.on.map(expr_from_proto).transpose()?,
                )?,
                join_type: join_type_from_proto(
                    proto::JoinType::try_from(join.join_type)
                        .map_err(|_| DaftError::ValueError("invalid JoinType".to_string()))?,
                )?,
                join_strategy: join
                    .join_strategy
                    .map(|s| {
                        join_strategy_from_proto(
                            proto::JoinStrategy::try_from(s).map_err(|_| {
                                DaftError::ValueError("invalid JoinStrategy".to_string())
                            })?,
                        )
                    })
                    .transpose()?,
                output_schema: schema_from_proto_required(
                    join.output_schema,
                    "JoinNode.output_schema",
                )?,
                stats_state: stats_state_from_proto(required_or_default(
                    join.stats,
                    "JoinNode.stats",
                )),
                key_filtering_config: join
                    .key_filtering_config
                    .map(key_filtering_config_from_proto)
                    .transpose()?,
            })
        }
        Node::Sink(sink) => {
            let (plan_id, node_id) = node_ids_from_proto(sink.plan_id, sink.node_id);
            LogicalPlan::Sink(crate::ops::Sink {
                plan_id,
                node_id,
                input: plan_from_proto(required_boxed_plan(sink.input, "SinkNode.input")?)?,
                schema: schema_from_proto_required(sink.schema, "SinkNode.schema")?,
                sink_info: Arc::new(sink_info_from_proto(required(
                    sink.sink_info,
                    "SinkNode.sink_info",
                )?)?),
                stats_state: stats_state_from_proto(required_or_default(
                    sink.stats,
                    "SinkNode.stats",
                )),
            })
        }
        Node::Sample(sample) => {
            let (plan_id, node_id) = node_ids_from_proto(sample.plan_id, sample.node_id);
            LogicalPlan::Sample(crate::ops::Sample {
                plan_id,
                node_id,
                input: plan_from_proto(required_boxed_plan(sample.input, "SampleNode.input")?)?,
                fraction: sample.fraction,
                size: sample.size.map(|size| size as usize),
                with_replacement: sample.with_replacement,
                seed: sample.seed,
                stats_state: stats_state_from_proto(required_or_default(
                    sample.stats,
                    "SampleNode.stats",
                )),
            })
        }
        Node::Shuffle(shuffle) => {
            let (plan_id, node_id) = node_ids_from_proto(shuffle.plan_id, shuffle.node_id);
            LogicalPlan::Shuffle(crate::ops::Shuffle {
                plan_id,
                node_id,
                input: plan_from_proto(required_boxed_plan(
                    shuffle.input,
                    "ShuffleNode.input",
                )?)?,
                seed: shuffle.seed,
                stats_state: stats_state_from_proto(required_or_default(
                    shuffle.stats,
                    "ShuffleNode.stats",
                )),
            })
        }
        Node::MonotonicallyIncreasingId(mono) => {
            let (plan_id, node_id) = node_ids_from_proto(mono.plan_id, mono.node_id);
            LogicalPlan::MonotonicallyIncreasingId(crate::ops::MonotonicallyIncreasingId {
                plan_id,
                node_id,
                input: plan_from_proto(required_boxed_plan(
                    mono.input,
                    "MonotonicallyIncreasingIdNode.input",
                )?)?,
                schema: schema_from_proto_required(
                    mono.schema,
                    "MonotonicallyIncreasingIdNode.schema",
                )?,
                column_name: mono.column_name,
                starting_offset: mono.starting_offset,
                stats_state: stats_state_from_proto(required_or_default(
                    mono.stats,
                    "MonotonicallyIncreasingIdNode.stats",
                )),
            })
        }
        Node::SubqueryAlias(subquery_alias) => {
            let (plan_id, node_id) =
                node_ids_from_proto(subquery_alias.plan_id, subquery_alias.node_id);
            LogicalPlan::SubqueryAlias(crate::logical_plan::SubqueryAlias {
                plan_id,
                node_id,
                input: plan_from_proto(required_boxed_plan(
                    subquery_alias.input,
                    "SubqueryAliasNode.input",
                )?)?,
                name: subquery_alias.name.into(),
            })
        }
        Node::Window(window) => {
            let (plan_id, node_id) = node_ids_from_proto(window.plan_id, window.node_id);
            LogicalPlan::Window(crate::ops::Window {
                plan_id,
                node_id,
                input: plan_from_proto(required_boxed_plan(window.input, "WindowNode.input")?)?,
                window_functions: window
                    .window_functions
                    .into_iter()
                    .map(window_expr_from_proto)
                    .collect::<DaftResult<Vec<_>>>()?,
                aliases: window.aliases,
                window_spec: Arc::new(window_spec_from_proto(required(
                    window.window_spec,
                    "WindowNode.window_spec",
                )?)?),
                schema: schema_from_proto_required(window.schema, "WindowNode.schema")?,
                stats_state: stats_state_from_proto(required_or_default(
                    window.stats,
                    "WindowNode.stats",
                )),
            })
        }
        Node::TopN(top_n) => {
            let (plan_id, node_id) = node_ids_from_proto(top_n.plan_id, top_n.node_id);
            LogicalPlan::TopN(crate::ops::TopN {
                plan_id,
                node_id,
                input: plan_from_proto(required_boxed_plan(top_n.input, "TopNNode.input")?)?,
                sort_by: top_n
                    .sort_by
                    .into_iter()
                    .map(expr_from_proto)
                    .collect::<DaftResult<Vec<_>>>()?,
                descending: top_n.descending,
                nulls_first: top_n.nulls_first,
                limit: top_n.limit,
                offset: top_n.offset,
                stats_state: stats_state_from_proto(required_or_default(
                    top_n.stats,
                    "TopNNode.stats",
                )),
            })
        }
        Node::VllmProject(vllm) => {
            let (plan_id, node_id) = node_ids_from_proto(vllm.plan_id, vllm.node_id);
            LogicalPlan::VLLMProject(crate::ops::VLLMProject {
                plan_id,
                node_id,
                expr: vllm_expr_from_proto(required(vllm.expr, "VllmProjectNode.expr")?)?,
                input: plan_from_proto(required_boxed_plan(
                    vllm.input,
                    "VllmProjectNode.input",
                )?)?,
                output_column_name: vllm.output_column_name.into(),
                output_schema: schema_from_proto_required(
                    vllm.output_schema,
                    "VllmProjectNode.output_schema",
                )?,
                stats_state: stats_state_from_proto(required_or_default(
                    vllm.stats,
                    "VllmProjectNode.stats",
                )),
            })
        }
    };
    Ok(Arc::new(plan))
}

pub(crate) fn required_boxed_plan(plan: Option<Box<proto::LogicalPlan>>, what: &str) -> DaftResult<proto::LogicalPlan> {
    required(plan, what).map(|plan| *plan)
}

pub(crate) fn required_or_default<T: Default>(value: Option<T>, what: &str) -> T {
    value.unwrap_or_else(|| {
        log::warn!("plan protobuf payload missing {what}, using default");
        T::default()
    })
}

pub(crate) fn schema_from_proto_required(schema: Option<proto::Schema>, what: &str) -> DaftResult<SchemaRef> {
    required(schema, what)?.into_daft()
}
