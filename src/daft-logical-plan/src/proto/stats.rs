use super::*;
pub(crate) fn stats_state_to_proto(stats: &StatsState) -> proto::StatsState {
    match stats {
        StatsState::NotMaterialized => proto::StatsState {
            materialized: false,
            stats: None,
        },
        StatsState::Materialized(stats) => proto::StatsState {
            materialized: true,
            stats: Some(proto::ApproxStats {
                num_rows: stats.approx_stats.num_rows as u64,
                size_bytes: stats.approx_stats.size_bytes as u64,
                acc_selectivity: stats.approx_stats.acc_selectivity,
            }),
        },
    }
}

pub(crate) fn stats_state_from_proto(stats: proto::StatsState) -> StatsState {
    if !stats.materialized {
        return StatsState::NotMaterialized;
    }
    let Some(stats) = stats.stats else {
        return StatsState::NotMaterialized;
    };
    StatsState::Materialized(
        PlanStats::new(ApproxStats {
            num_rows: stats.num_rows as usize,
            size_bytes: stats.size_bytes as usize,
            acc_selectivity: stats.acc_selectivity,
        })
        .into(),
    )
}

pub(crate) fn sharder_to_proto(sharder: &Sharder) -> proto::Sharder {
    proto::Sharder {
        strategy: match sharder.strategy() {
            daft_scan::ShardingStrategy::File => proto::ShardingStrategy::File as i32,
        },
        world_size: sharder.world_size() as u64,
        rank: sharder.rank() as u64,
    }
}

pub(crate) fn sharder_from_proto(sharder: proto::Sharder) -> DaftResult<Sharder> {
    let strategy = match proto::ShardingStrategy::try_from(sharder.strategy)
        .unwrap_or(proto::ShardingStrategy::Unspecified)
    {
        proto::ShardingStrategy::File => daft_scan::ShardingStrategy::File,
        proto::ShardingStrategy::Unspecified => {
            return invalid("Sharder missing strategy");
        }
    };
    Ok(Sharder::new(
        strategy,
        sharder.world_size as usize,
        sharder.rank as usize,
    ))
}

