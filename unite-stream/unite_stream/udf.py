"""Convenience re-exports of the Daft UDF decorators.

These names are *also* auto-injected into every Parser script run by
:class:`unite_stream.UniteStreamCompiler`, so user scripts may call
``@func`` / ``@cls`` / ``@method`` / ``@udf`` / ``metrics`` directly without
any ``import`` statement.
"""

from __future__ import annotations

from daft.udf import cls, func, method, metrics
from daft.udf.legacy import UDF, udf

__all__ = ["UDF", "cls", "func", "method", "metrics", "udf"]
