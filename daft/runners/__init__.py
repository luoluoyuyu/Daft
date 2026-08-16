from __future__ import annotations

from typing import TYPE_CHECKING
from daft.daft import get_runner as _get_runner_internal
from daft.daft import get_or_create_runner as _get_or_create_runner
from daft.daft import get_or_infer_runner_type as _get_or_infer_runner_type
from daft.daft import set_runner_native as _set_runner_native

if TYPE_CHECKING:
    from daft.runners.runner import Runner
    from daft.runners.partitioning import PartitionT

_REMOTE_RUNNER: Runner[PartitionT] | None = None


def _get_runner() -> Runner[PartitionT] | None:
    """Internal testing function to check the currently set runner."""
    return _get_runner_internal()


def get_or_create_runner() -> Runner[PartitionT]:
    """Get or create the current runner instance.

    If a runner has already been set, returns it. Otherwise, creates a new
    runner using the default configuration (native) and locks it in.

    Returns:
        Runner[PartitionT]: The current runner instance.

    Note:
        After calling this function, the runner cannot be changed for the
        lifetime of the process. Use ``get_or_infer_runner_type`` to check the
        runner type without this side effect.
    """
    if _REMOTE_RUNNER is not None:
        return _REMOTE_RUNNER
    return _get_or_create_runner()


def set_runner_remote(endpoint: str, *, token: str | None = None) -> Runner[PartitionT]:
    """Route all materializing operations to a standalone Daft runtime."""
    global _REMOTE_RUNNER
    if _REMOTE_RUNNER is not None:
        return _REMOTE_RUNNER
    from daft.runtime.runner import RemoteRunner

    _REMOTE_RUNNER = RemoteRunner(endpoint, token=token)
    return _REMOTE_RUNNER


def _reset_remote_runner_for_testing() -> None:
    global _REMOTE_RUNNER
    _REMOTE_RUNNER = None


def get_or_infer_runner_type() -> str:
    """Get or infer the runner type.

    This API will get or infer the currently used runner type according to the following strategies:
    1. If the `runner` has been set, return its type directly;
    2. Try to determine whether it's currently running on a ray cluster. If so, consider it to be a ray type;
    3. Try to determine based on `DAFT_RUNNER` env variable.

    Returns:
        str: The runner type ("native" or "ray").
    """
    return _get_or_infer_runner_type()


def set_runner_native(num_threads: int | None = None) -> Runner[PartitionT]:
    """Configure Daft to execute dataframes using native multi-threaded processing.

    This is the default execution mode for Daft.

    Returns:
        Runner[PartitionT]: A runner object with the native runner's configuration.

    Note:
        Can also be configured via environment variable: DAFT_RUNNER=native
    """
    return _set_runner_native(num_threads)

