"""Exception hierarchy for the :mod:`unite_stream` package."""

from __future__ import annotations


class UniteStreamError(Exception):
    """Base class for all UniteStream-specific errors."""


class CompilationError(UniteStreamError):
    """Raised when the parser cannot build a valid IR from a user script.

    Covers empty / malformed OUTPUT_STREAMS, exceptions raised during ``exec``
    of the user script, and serialization failures of the resulting plans.
    """


class RuntimeExecutionError(UniteStreamError):
    """Raised when the runtime fails to materialize a plan from cloudpickled IR."""
