"""Parser stage of UniteStream.

The Parser is the lowest layer of the pipeline: it executes a user script
inside a fresh, isolated globals namespace and returns the **raw, unbound
:class:`daft.DataFrame` plans** that the script appended to
``OUTPUT_STREAMS``. No write path is bound, no execution happens, no IR
serialization is performed — that is the job of higher layers
(:class:`unite_stream.UniteStreamCompiler`,
:class:`unite_stream.UniteStreamRuntime`,
:class:`unite_stream.UniteStreamClient`).

Execution semantics
-------------------
* The script runs with **full Python builtins** and may freely ``import``
  modules, define classes/functions, register UDFs, and use dynamic-code
  primitives. UniteStream does no static validation of the script — the
  user is trusted to supply correct code.
* The following names are auto-injected, so user scripts need **no**
  ``import`` statement to use them: ``UniteStream``, ``func``, ``cls``,
  ``method``, ``udf``, ``metrics``, ``daft``, full Python ``__builtins__``,
  and any caller-supplied ``extra_globals``.
* ``OUTPUT_STREAMS`` is a **per-call method-local Python list** exposed to
  the script as a script-global name. Each ``parse`` invocation allocates a
  fresh list, so two concurrent calls on the same :class:`UniteStreamParser`
  cannot see each other's outputs.

Thread safety
-------------
:class:`UniteStreamParser` is immutable after construction (``__slots__``)
and stores no per-call state, so a single instance may be shared across
threads or asyncio tasks.
"""

from __future__ import annotations

import builtins
from collections.abc import Mapping
from typing import Any, Final

import daft
from daft.dataframe.dataframe import DataFrame

from unite_stream.dataframe import UniteStreamDataFrame
from unite_stream.errors import CompilationError
from unite_stream.module import UniteStream
from unite_stream.udf import cls, func, method, metrics, udf

_NAMESPACE_NAME: Final[str] = "UniteStream"
_OUTPUT_NAME: Final[str] = "OUTPUT_STREAMS"
_SCRIPT_FILENAME: Final[str] = "<unite-stream-script>"


def _build_static_script_globals() -> dict[str, Any]:
    """Build the **static** name bindings injected into every Parser run.

    Only names that are safe to share across compiles live here (the
    ``UniteStream`` namespace, UDF decorators, the ``daft`` module, and a
    *copy* of ``__builtins__``). Per-call state (``OUTPUT_STREAMS``) is added
    by :meth:`UniteStreamParser.parse`.
    """
    return {
        _NAMESPACE_NAME: UniteStream,
        "daft": daft,
        "func": func,
        "cls": cls,
        "method": method,
        "udf": udf,
        "metrics": metrics,
        "__builtins__": builtins.__dict__.copy(),
        "__name__": "__main__",
        "__file__": _SCRIPT_FILENAME,
    }


class UniteStreamParser:
    """Parse a user script and return the raw Daft plans it produces.

    Args:
        extra_globals: Optional read-only mapping of additional **static**
            names injected into every script run. Copied on construction.
    """

    __slots__ = ("_extra_globals",)

    def __init__(self, *, extra_globals: Mapping[str, Any] | None = None) -> None:
        self._extra_globals: Final[Mapping[str, Any]] = (
            dict(extra_globals) if extra_globals else {}
        )

    def parse(self, user_python_code: str) -> list[DataFrame]:
        """Execute a user script, return its ``OUTPUT_STREAMS`` plans.

        Args:
            user_python_code: Source of the user script. Must ``append`` at
                least one :class:`UniteStreamDataFrame` to ``OUTPUT_STREAMS``.

        Returns:
            A list of **raw, unbound** :class:`daft.DataFrame` lazy plans, in
            the order the script appended them. Callers may consume them
            however they want — execute directly, bind to write paths,
            serialize for transport, etc.

        Raises:
            CompilationError: If the script fails to compile/execute, produces
                no streams, or yields a non-:class:`UniteStreamDataFrame`
                object in ``OUTPUT_STREAMS``.
        """
        # ``script_outputs`` is a fresh method-local list per call — it is
        # exposed to the script as the global name ``OUTPUT_STREAMS`` but
        # never leaks across concurrent calls.
        script_outputs: list[UniteStreamDataFrame] = []

        script_globals = _build_static_script_globals()
        script_globals.update(self._extra_globals)
        script_globals[_OUTPUT_NAME] = script_outputs

        try:
            compiled = compile(user_python_code, _SCRIPT_FILENAME, "exec")
            exec(compiled, script_globals)  # noqa: S102 — user-supplied script
        except CompilationError:
            raise
        except Exception as exc:  # noqa: BLE001 — wrap any user-script error
            raise CompilationError(
                f"[编译失败] 用户脚本执行抛出异常: {type(exc).__name__}: {exc}"
            ) from exc

        if not script_outputs:
            raise CompilationError(
                f"[编译中断] 脚本未向 {_OUTPUT_NAME} 提交任何输出流"
            )

        plans: list[DataFrame] = []
        for index, stream in enumerate(script_outputs):
            if not isinstance(stream, UniteStreamDataFrame):
                raise CompilationError(
                    f"[越权警报] OUTPUT_STREAMS[{index}] 不是 UniteStreamDataFrame: "
                    f"got {type(stream).__name__}"
                )
            plans.append(stream._df)

        return plans
