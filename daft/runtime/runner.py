from __future__ import annotations

import os
from typing import TYPE_CHECKING

from daft.plan_transport import serialize_plan_parts
from daft.recordbatch import MicroPartition
from daft.runners.partitioning import LocalMaterializedResult, LocalPartitionSet
from daft.runners.runner import LOCAL_PARTITION_SET_CACHE, Runner
from daft.runners.runner_io import RunnerIO
from daft.runtime.client import Job, RuntimeClient

if TYPE_CHECKING:
    from collections.abc import Iterator

    from daft.dataframe.dataframe import DataFrame
    from daft.logical.builder import LogicalPlanBuilder
    from daft.runners.partitioning import MaterializedResult, PartitionCacheEntry, PartitionSetCache


class RemoteRunnerIO(RunnerIO):
    def glob_paths_details(self, *args: object, **kwargs: object) -> object:
        raise RuntimeError("Filesystem discovery must be performed by the Daft runtime")


class RemoteRunner(Runner[MicroPartition]):
    name = "remote"

    def __init__(self, endpoint: str, *, token: str | None = None) -> None:
        super().__init__()
        self.client = RuntimeClient(endpoint, token=token)

    def initialize_partition_set_cache(self) -> PartitionSetCache:
        return LOCAL_PARTITION_SET_CACHE

    def runner_io(self) -> RunnerIO:
        return RemoteRunnerIO()

    def submit(self, builder: LogicalPlanBuilder) -> Job:
        from daft.dataframe.dataframe import DataFrame

        plan_bytes, execution, partition_sets = serialize_plan_parts(DataFrame(builder))
        return self.client.submit(
            plan_bytes,
            execution=execution,
            partition_sets=partition_sets,
        )

    def run_iter(
        self, builder: LogicalPlanBuilder, results_buffer_size: int | None = None
    ) -> Iterator[MaterializedResult[MicroPartition]]:
        del results_buffer_size
        for partition in self.submit(builder).result():
            yield LocalMaterializedResult(partition)

    def run_iter_tables(
        self, builder: LogicalPlanBuilder, results_buffer_size: int | None = None
    ) -> Iterator[MicroPartition]:
        for result in self.run_iter(builder, results_buffer_size):
            yield result.partition()

    def run(self, builder: LogicalPlanBuilder) -> tuple[PartitionCacheEntry, None]:
        result_set = LocalPartitionSet()
        for idx, result in enumerate(self.run_iter(builder)):
            result_set.set_partition(idx, result)
        return self.put_partition_set_into_cache(result_set), None


def connect(endpoint: str | None = None, *, token: str | None = None) -> RemoteRunner:
    from daft.runners import set_runner_remote

    resolved_endpoint = endpoint or os.environ.get("DAFT_RUNTIME_URL")
    if not resolved_endpoint:
        raise ValueError("A runtime endpoint is required; pass endpoint or set DAFT_RUNTIME_URL")
    return set_runner_remote(resolved_endpoint, token=token)


def submit(df: DataFrame) -> Job:
    from daft.runners import get_or_create_runner

    runner = get_or_create_runner()
    if not isinstance(runner, RemoteRunner):
        raise RuntimeError("Daft is not connected to a remote runtime")
    return runner.submit(df._get_current_builder())
