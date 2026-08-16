"""PythonClient-side parsing of Python UDFs from a serialized logical plan.

UDF *parsing* happens exactly once, here, while the interpreter that built the
plan is available. The Rust runtime never parses the plan to discover UDFs and
never unpickles cloudpickle payloads: it receives the finished
``daft.v1.UdfDescriptor`` messages and forwards them verbatim to the Python
UDF worker, which initializes and runs them.

The extractor walks the protobuf ``LogicalPlan`` (``plan.proto``) and maps
every UDF-carrying node/expression onto a self-contained
``daft.v1.UdfDescriptor`` (``udf.proto``):

* ``Expression.function.python`` (``LegacyPythonUdf``)      -> LEGACY
* ``Expression.scalar_fn.python`` (``PyScalarFn``):
  * ``row_wise`` (``RowWisePyFn``)                          -> SCALAR_ROW_WISE
  * ``batch`` (``BatchPyFn``)                               -> SCALAR_BATCH
* ``Expression.agg.map_groups`` (``MapGroupsAgg``)          -> MAP_GROUPS
* ``Expression.vllm`` / ``VllmProjectNode.expr``            -> VLLM

Return/input dtypes, argument names, cloudpickle payloads, and execution
properties are all copied out of the plan proto here, so a descriptor is the
complete declaration of one Python UDF.
"""

from __future__ import annotations

import hashlib
import platform
from typing import Callable, Iterable

from daft.runtime.daft_proto.daft_runtime_proto.v1 import (
    plan_pb2,
    udf_pb2,
)


def extract_udf_descriptors(
    plan: plan_pb2.LogicalPlan,
    *,
    python_version: str | None = None,
) -> list[udf_pb2.UdfDescriptor]:
    """Walk ``plan`` and return one ``UdfDescriptor`` per distinct Python UDF.

    Descriptors are deduplicated: the same UDF appearing in several plan
    nodes produces a single descriptor, because the Python worker initializes
    one instance per descriptor and then executes the whole plan.

    ``python_version`` pins the interpreter the descriptors (and their
    cloudpickle payloads) were built against; defaults to the running
    interpreter.
    """
    if python_version is None:
        python_version = platform.python_version()
    collector = _Collector(python_version)
    for node in _iter_plan_nodes(plan):
        _collect_node(node, collector)
    return collector.descriptors()


def extract_udf_descriptors_from_bytes(
    plan_bytes: bytes,
    *,
    python_version: str | None = None,
) -> list[udf_pb2.UdfDescriptor]:
    """Parse ``plan_bytes`` (a serialized ``daft.v1.LogicalPlan``) and extract."""
    plan = plan_pb2.LogicalPlan()
    plan.ParseFromString(plan_bytes)
    return extract_udf_descriptors(plan, python_version=python_version)


def plan_contains_udf(plan: plan_pb2.LogicalPlan) -> bool:
    """True when the plan carries any Python UDF."""
    return bool(extract_udf_descriptors(plan))


class _Collector:
    def __init__(self, python_version: str) -> None:
        self._python_version = python_version
        self._seen: dict[tuple, udf_pb2.UdfDescriptor] = {}
        self._order: list[udf_pb2.UdfDescriptor] = []

    def add(self, descriptor: udf_pb2.UdfDescriptor) -> None:
        key = _descriptor_key(descriptor)
        if key in self._seen:
            return
        self._seen[key] = descriptor
        self._order.append(descriptor)

    def descriptors(self) -> list[udf_pb2.UdfDescriptor]:
        return list(self._order)

    def version(self) -> str:
        return self._python_version


def _descriptor_key(descriptor: udf_pb2.UdfDescriptor) -> tuple:
    code_digest = hashlib.sha256(descriptor.code).hexdigest()[:16]
    method_digest = hashlib.sha256(descriptor.method).hexdigest()[:16]
    return (
        descriptor.kind,
        descriptor.udf_id,
        descriptor.name,
        code_digest,
        method_digest,
    )


# ---------------------------------------------------------------------------
# Plan tree traversal
# ---------------------------------------------------------------------------

_CHILD_PLAN_FIELDS: dict[str, tuple[str, ...]] = {
    "shard": ("input",),
    "project": ("input",),
    "udf_project": ("input",),
    "filter": ("input",),
    "into_batches": ("input",),
    "limit": ("input",),
    "offset": ("input",),
    "explode": ("input",),
    "unpivot": ("input",),
    "sort": ("input",),
    "repartition": ("input",),
    "into_partitions": ("input",),
    "distinct": ("input",),
    "aggregate": ("input",),
    "pivot": ("input",),
    "concat": ("input", "other"),
    "intersect": ("lhs", "rhs"),
    "union": ("lhs", "rhs"),
    "join": ("left", "right"),
    "sink": ("input",),
    "sample": ("input",),
    "shuffle": ("input",),
    "monotonically_increasing_id": ("input",),
    "subquery_alias": ("input",),
    "window": ("input",),
    "top_n": ("input",),
    "vllm_project": ("input",),
}


def _iter_plan_nodes(plan: plan_pb2.LogicalPlan) -> Iterable[plan_pb2.LogicalPlan]:
    stack = [plan]
    while stack:
        node = stack.pop()
        yield node
        kind = node.WhichOneof("node")
        if kind is None:
            continue
        message = getattr(node, kind)
        for field in _CHILD_PLAN_FIELDS.get(kind, ()):
            child = getattr(message, field, None)
            if child is not None and child.WhichOneof("node") is not None:
                stack.append(child)


def _collect_node(node: plan_pb2.LogicalPlan, collector: _Collector) -> None:
    kind = node.WhichOneof("node")
    if kind is None:
        return
    message = getattr(node, kind)
    if kind == "udf_project":
        # The whole node is one legacy ``@daft.udf`` application; its
        # ``UdfProperties`` refine the descriptor taken from the expression.
        _walk_expr(message.expr, collector, udf_properties=message.udf_properties)
        for expr in message.passthrough_columns:
            _walk_expr(expr, collector)
    elif kind == "project":
        for expr in message.projection:
            _walk_expr(expr, collector)
    elif kind == "filter":
        _walk_expr(message.predicate, collector)
    elif kind == "explode":
        for expr in message.to_explode:
            _walk_expr(expr, collector)
    elif kind == "unpivot":
        for expr in message.ids:
            _walk_expr(expr, collector)
        for expr in message.values:
            _walk_expr(expr, collector)
    elif kind == "sort":
        for expr in message.sort_by:
            _walk_expr(expr, collector)
    elif kind == "distinct":
        if message.columns is not None:
            for expr in message.columns.items:
                _walk_expr(expr, collector)
    elif kind == "aggregate":
        for expr in message.aggregations:
            _walk_expr(expr, collector)
        for expr in message.groupby:
            _walk_expr(expr, collector)
    elif kind == "pivot":
        for expr in message.group_by:
            _walk_expr(expr, collector)
        _walk_expr(message.pivot_column, collector)
        _walk_expr(message.value_column, collector)
        _walk_agg(message.aggregation, collector)
    elif kind == "join":
        if message.on is not None:
            _walk_expr(message.on, collector)
    elif kind == "window":
        for window_expr in message.window_functions:
            _walk_window_expr(window_expr, collector)
    elif kind == "top_n":
        for expr in message.sort_by:
            _walk_expr(expr, collector)
    elif kind == "vllm_project":
        _collect_vllm(message.expr, collector, return_dtype=_schema_dtype_bytes(message.output_schema))


# ---------------------------------------------------------------------------
# Expression traversal
# ---------------------------------------------------------------------------

def _walk_expr(
    expr: plan_pb2.Expression,
    collector: _Collector,
    *,
    udf_properties: plan_pb2.UdfProperties | None = None,
) -> None:
    """Depth-first walk of an expression tree, collecting UDF descriptors."""
    stack: list[plan_pb2.Expression] = [expr]
    while stack:
        current = stack.pop()
        kind = current.WhichOneof("expr")
        if kind is None:
            continue
        if kind == "function":
            function = current.function
            if function.WhichOneof("func") == "python":
                _collect_legacy(function.python, function.inputs, collector, udf_properties)
            for input_expr in function.inputs:
                stack.append(input_expr)
        elif kind == "scalar_fn":
            scalar_fn = current.scalar_fn
            if scalar_fn.WhichOneof("func") == "python":
                _collect_py_scalar_fn(scalar_fn.python, collector)
        elif kind == "agg":
            _walk_agg(current.agg, collector)
        elif kind == "vllm":
            _collect_vllm(current.vllm, collector)
        elif kind == "alias":
            stack.append(current.alias.child)
        elif kind == "binary_op":
            stack.append(current.binary_op.left)
            stack.append(current.binary_op.right)
        elif kind == "cast":
            stack.append(current.cast.child)
        elif kind in ("not", "is_null", "not_null"):
            stack.append(getattr(current, kind))
        elif kind == "fill_null":
            stack.append(current.fill_null.child)
            stack.append(current.fill_null.value)
        elif kind == "is_in":
            stack.append(current.is_in.child)
            stack.extend(current.is_in.values)
        elif kind == "between":
            stack.append(current.between.child)
            stack.append(current.between.lower)
            stack.append(current.between.upper)
        elif kind in ("list", "coalesce"):
            stack.extend(getattr(current, kind).items)
        elif kind == "if_else":
            stack.append(current.if_else.if_true)
            stack.append(current.if_else.if_false)
            stack.append(current.if_else.predicate)
        elif kind == "over":
            _walk_window_expr(current.over.window_expr, collector)
            _walk_window_spec(current.over.window_spec, collector)
        elif kind == "window_function":
            _walk_window_expr(current.window_function, collector)
        elif kind == "in_subquery":
            stack.append(current.in_subquery.child)
        # ``subquery`` / ``exists`` are declared but never serialized.


def _walk_agg(agg: plan_pb2.AggExpression, collector: _Collector) -> None:
    kind = agg.WhichOneof("agg")
    if kind is None:
        return
    if kind == "map_groups":
        map_groups = agg.map_groups
        _collect_map_groups(map_groups, collector)
        for input_expr in map_groups.inputs:
            _walk_expr(input_expr, collector)
        return
    if kind == "count":
        _walk_expr(agg.count.child, collector)
        return
    if kind == "approx_percentile":
        _walk_expr(agg.approx_percentile.child, collector)
        return
    if kind in ("approx_sketch", "merge_sketch"):
        _walk_expr(getattr(agg, kind).child, collector)
        return
    if kind in ("stddev", "var"):
        _walk_expr(getattr(agg, kind).child, collector)
        return
    if kind == "any_value":
        _walk_expr(agg.any_value.child, collector)
        return
    if kind == "concat":
        _walk_expr(agg.concat.child, collector)
        return
    # The remaining agg variants are plain wrapped expressions.
    _walk_expr(getattr(agg, kind), collector)


def _walk_window_expr(window_expr: plan_pb2.WindowExpr, collector: _Collector) -> None:
    kind = window_expr.WhichOneof("window")
    if kind == "agg":
        _walk_agg(window_expr.agg, collector)
    elif kind == "offset":
        _walk_expr(window_expr.offset.input, collector)
        if window_expr.offset.default is not None:
            _walk_expr(window_expr.offset.default, collector)


def _walk_window_spec(spec: plan_pb2.WindowSpec, collector: _Collector) -> None:
    if spec is None:
        return
    for expr in spec.partition_by:
        _walk_expr(expr, collector)
    for expr in spec.order_by:
        _walk_expr(expr, collector)


# ---------------------------------------------------------------------------
# UDF message -> descriptor mapping
# ---------------------------------------------------------------------------

def _new_descriptor(collector: _Collector) -> udf_pb2.UdfDescriptor:
    descriptor = udf_pb2.UdfDescriptor()
    descriptor.python_version = collector.version()
    return descriptor


def _collect_legacy(
    legacy: plan_pb2.LegacyPythonUdf,
    inputs: Iterable[plan_pb2.Expression],
    collector: _Collector,
    udf_properties: plan_pb2.UdfProperties | None = None,
) -> None:
    descriptor = _new_descriptor(collector)
    descriptor.kind = udf_pb2.UDF_KIND_LEGACY
    descriptor.udf_id = legacy.name
    descriptor.name = legacy.name
    descriptor.return_dtype = legacy.return_dtype.SerializeToString()
    descriptor.num_inputs = legacy.num_expressions or len(list(inputs))
    code, init_args = _maybe_initialized(legacy.func)
    descriptor.code = code
    descriptor.init_args = init_args
    descriptor.bound_args = legacy.bound_args
    descriptor.input_dtypes.extend(_input_dtypes(inputs))
    descriptor.arg_names.extend(_arg_names(inputs))
    descriptor.resource_request = legacy.resource_request
    descriptor.batch_size = legacy.batch_size or 0
    descriptor.concurrency = legacy.concurrency or 0
    descriptor.use_process = bool(legacy.use_process)
    descriptor.ray_options = legacy.ray_options
    if udf_properties is not None:
        _apply_udf_properties(descriptor, udf_properties)
    _validate(descriptor)
    collector.add(descriptor)


def _collect_py_scalar_fn(
    py_fn: plan_pb2.PyScalarFn,
    collector: _Collector,
) -> None:
    kind = py_fn.WhichOneof("func")
    if kind == "row_wise":
        _collect_row_wise(py_fn.row_wise, collector)
    elif kind == "batch":
        _collect_batch(py_fn.batch, collector)


def _collect_row_wise(fn: plan_pb2.RowWisePyFn, collector: _Collector) -> None:
    descriptor = _new_descriptor(collector)
    descriptor.kind = udf_pb2.UDF_KIND_SCALAR_ROW_WISE
    descriptor.udf_id = fn.func_id
    descriptor.name = fn.function_name
    descriptor.return_dtype = fn.return_dtype.SerializeToString()
    descriptor.num_inputs = len(fn.args)
    descriptor.code = fn.cls
    descriptor.method = fn.method
    descriptor.original_args = fn.original_args
    descriptor.input_dtypes.extend(_input_dtypes(fn.args))
    descriptor.arg_names.extend(_arg_names(fn.args))
    descriptor.builtin_name = fn.builtin_name
    descriptor.is_async = fn.is_async
    descriptor.use_process = bool(fn.use_process)
    descriptor.max_concurrency = fn.max_concurrency or 0
    descriptor.max_retries = fn.max_retries or 0
    descriptor.on_error = _on_error(fn.on_error)
    descriptor.ray_options = fn.ray_options
    _validate(descriptor)
    collector.add(descriptor)


def _collect_batch(fn: plan_pb2.BatchPyFn, collector: _Collector) -> None:
    descriptor = _new_descriptor(collector)
    descriptor.kind = udf_pb2.UDF_KIND_SCALAR_BATCH
    descriptor.udf_id = fn.func_id
    descriptor.name = fn.function_name
    descriptor.return_dtype = fn.return_dtype.SerializeToString()
    descriptor.num_inputs = len(fn.args)
    descriptor.code = fn.cls
    descriptor.method = fn.method
    descriptor.original_args = fn.original_args
    descriptor.input_dtypes.extend(_input_dtypes(fn.args))
    descriptor.arg_names.extend(_arg_names(fn.args))
    descriptor.builtin_name = fn.builtin_name
    descriptor.is_async = fn.is_async
    descriptor.use_process = bool(fn.use_process)
    descriptor.batch_size = fn.batch_size or 0
    descriptor.max_concurrency = fn.max_concurrency or 0
    descriptor.max_retries = fn.max_retries or 0
    descriptor.on_error = _on_error(fn.on_error)
    descriptor.ray_options = fn.ray_options
    _validate(descriptor)
    collector.add(descriptor)


def _collect_map_groups(map_groups: plan_pb2.MapGroupsAgg, collector: _Collector) -> None:
    func = map_groups.func
    if func is None:
        return
    kind = func.WhichOneof("func")
    if kind == "legacy":
        descriptor = _new_descriptor(collector)
        descriptor.kind = udf_pb2.UDF_KIND_MAP_GROUPS
        legacy = func.legacy
        descriptor.udf_id = legacy.name
        descriptor.name = legacy.name
        descriptor.return_dtype = legacy.return_dtype.SerializeToString()
        descriptor.num_inputs = len(map_groups.inputs) or legacy.num_expressions
        code, init_args = _maybe_initialized(legacy.func)
        descriptor.code = code
        descriptor.init_args = init_args
        descriptor.bound_args = legacy.bound_args
        descriptor.resource_request = legacy.resource_request
        descriptor.batch_size = legacy.batch_size or 0
        descriptor.concurrency = legacy.concurrency or 0
        descriptor.use_process = bool(legacy.use_process)
        descriptor.ray_options = legacy.ray_options
        descriptor.input_dtypes.extend(_input_dtypes(map_groups.inputs))
        descriptor.arg_names.extend(_arg_names(map_groups.inputs))
        _validate(descriptor)
        collector.add(descriptor)
    elif kind == "python":
        _collect_map_groups_python(func.python, map_groups.inputs, collector)


def _collect_map_groups_python(
    py_fn: plan_pb2.PyScalarFn,
    inputs: Iterable[plan_pb2.Expression],
    collector: _Collector,
) -> None:
    kind = py_fn.WhichOneof("func")
    if kind == "row_wise":
        fn = py_fn.row_wise
        descriptor = _new_descriptor(collector)
        descriptor.kind = udf_pb2.UDF_KIND_MAP_GROUPS
        descriptor.udf_id = fn.func_id
        descriptor.name = fn.function_name
        descriptor.return_dtype = fn.return_dtype.SerializeToString()
        descriptor.num_inputs = len(fn.args) or len(list(inputs))
        descriptor.code = fn.cls
        descriptor.method = fn.method
        descriptor.original_args = fn.original_args
        descriptor.input_dtypes.extend(_input_dtypes(fn.args))
        descriptor.arg_names.extend(_arg_names(fn.args))
        descriptor.builtin_name = fn.builtin_name
        descriptor.is_async = fn.is_async
        descriptor.use_process = bool(fn.use_process)
        descriptor.max_concurrency = fn.max_concurrency or 0
        descriptor.max_retries = fn.max_retries or 0
        descriptor.on_error = _on_error(fn.on_error)
        descriptor.ray_options = fn.ray_options
        _validate(descriptor)
        collector.add(descriptor)
    elif kind == "batch":
        fn = py_fn.batch
        descriptor = _new_descriptor(collector)
        descriptor.kind = udf_pb2.UDF_KIND_MAP_GROUPS
        descriptor.udf_id = fn.func_id
        descriptor.name = fn.function_name
        descriptor.return_dtype = fn.return_dtype.SerializeToString()
        descriptor.num_inputs = len(fn.args) or len(list(inputs))
        descriptor.code = fn.cls
        descriptor.method = fn.method
        descriptor.original_args = fn.original_args
        descriptor.input_dtypes.extend(_input_dtypes(fn.args))
        descriptor.arg_names.extend(_arg_names(fn.args))
        descriptor.builtin_name = fn.builtin_name
        descriptor.is_async = fn.is_async
        descriptor.use_process = bool(fn.use_process)
        descriptor.batch_size = fn.batch_size or 0
        descriptor.max_concurrency = fn.max_concurrency or 0
        descriptor.max_retries = fn.max_retries or 0
        descriptor.on_error = _on_error(fn.on_error)
        descriptor.ray_options = fn.ray_options
        _validate(descriptor)
        collector.add(descriptor)


def _collect_vllm(
    vllm: plan_pb2.VllmExpr,
    collector: _Collector,
    *,
    return_dtype: bytes = b"",
) -> None:
    descriptor = _new_descriptor(collector)
    descriptor.kind = udf_pb2.UDF_KIND_VLLM
    descriptor.udf_id = vllm.model
    descriptor.name = vllm.model
    descriptor.return_dtype = return_dtype
    descriptor.num_inputs = 1
    if vllm.input is not None:
        descriptor.arg_names.extend(_arg_names([vllm.input]))
        descriptor.input_dtypes.extend(_input_dtypes([vllm.input]))
    descriptor.concurrency = vllm.concurrency
    descriptor.batch_size = vllm.batch_size or 0
    descriptor.model = vllm.model
    descriptor.engine_args = vllm.engine_args
    descriptor.generate_args = vllm.generate_args
    collector.add(descriptor)


def _apply_udf_properties(
    descriptor: udf_pb2.UdfDescriptor,
    properties: plan_pb2.UdfProperties,
) -> None:
    descriptor.batch_size = properties.batch_size or 0
    descriptor.concurrency = properties.concurrency or 0
    descriptor.use_process = bool(properties.use_process)
    descriptor.max_retries = properties.max_retries or 0
    descriptor.builtin_name = properties.builtin_name
    descriptor.is_async = properties.is_async
    descriptor.is_scalar = properties.is_scalar
    descriptor.on_error = _on_error(properties.on_error)
    descriptor.resource_request = properties.resource_request
    descriptor.ray_options = properties.ray_options


# ---------------------------------------------------------------------------
# Field helpers
# ---------------------------------------------------------------------------

def _maybe_initialized(udf: plan_pb2.MaybeInitializedUdf) -> tuple[bytes, bytes]:
    if udf is None:
        return b"", b""
    state = udf.WhichOneof("state")
    if state == "initialized":
        return udf.initialized, b""
    if state == "uninitialized":
        return udf.uninitialized.inner, udf.uninitialized.init_args
    return b"", b""


def _on_error(value: int) -> int:
    """Map plan.proto ``OnError`` to udf.proto ``UdfOnError`` (same numbering)."""
    if value == plan_pb2.ON_ERROR_RAISE:
        return udf_pb2.UDF_ON_ERROR_RAISE
    if value == plan_pb2.ON_ERROR_LOG:
        return udf_pb2.UDF_ON_ERROR_LOG
    if value == plan_pb2.ON_ERROR_IGNORE:
        return udf_pb2.UDF_ON_ERROR_IGNORE
    return udf_pb2.UDF_ON_ERROR_UNSPECIFIED


def _arg_names(exprs: Iterable[plan_pb2.Expression]) -> list[str]:
    names: list[str] = []
    for expr in exprs:
        if expr.WhichOneof("expr") != "column":
            names.append("")
            continue
        column = expr.column
        kind = column.WhichOneof("column")
        if kind == "unresolved":
            names.append(column.unresolved.name)
        elif kind == "bound":
            names.append(column.bound.field.name)
        elif kind == "resolved":
            resolved = column.resolved
            resolved_kind = resolved.WhichOneof("resolved")
            if resolved_kind == "basic":
                names.append(resolved.basic)
            elif resolved_kind == "join_side":
                names.append(resolved.join_side.field.name)
            elif resolved_kind == "outer_ref":
                names.append(resolved.outer_ref.field.name)
            else:
                names.append("")
        else:
            names.append("")
    return names


def _input_dtypes(exprs: Iterable[plan_pb2.Expression]) -> list[bytes]:
    dtypes: list[bytes] = []
    for expr in exprs:
        dtype = _expression_dtype_bytes(expr)
        dtypes.append(dtype if dtype is not None else b"")
    return dtypes


def _expression_dtype_bytes(expr: plan_pb2.Expression) -> bytes | None:
    if expr.WhichOneof("expr") != "column":
        return None
    return _column_dtype_bytes(expr.column)


def _column_dtype_bytes(column: plan_pb2.Column) -> bytes | None:
    kind = column.WhichOneof("column")
    if kind == "unresolved":
        schema = column.unresolved.plan_schema
        name = column.unresolved.name
        if schema is None:
            return None
        for field in schema.fields:
            if field.name == name and field.dtype is not None:
                return field.dtype.SerializeToString()
        return None
    if kind == "bound":
        return _field_dtype_bytes(column.bound.field)
    if kind == "resolved":
        resolved = column.resolved
        resolved_kind = resolved.WhichOneof("resolved")
        if resolved_kind == "join_side":
            return _field_dtype_bytes(resolved.join_side.field)
        if resolved_kind == "outer_ref":
            return _field_dtype_bytes(resolved.outer_ref.field)
    return None


def _field_dtype_bytes(field: plan_pb2.Field | None) -> bytes | None:
    if field is None or field.dtype is None:
        return None
    return field.dtype.SerializeToString()


def _schema_dtype_bytes(schema: plan_pb2.Schema | None) -> bytes:
    if schema is None or not schema.fields:
        return b""
    dtype = _field_dtype_bytes(schema.fields[0])
    return dtype if dtype is not None else b""


def _validate(descriptor: udf_pb2.UdfDescriptor) -> None:
    """Client-side UDF validation: a descriptor must be runnable as-is."""
    if not descriptor.udf_id and not descriptor.name:
        raise ValueError(f"UDF descriptor missing identity: {descriptor}")
    if not descriptor.code and descriptor.kind not in (udf_pb2.UDF_KIND_VLLM,):
        raise ValueError(
            f"UDF {descriptor.name!r} has no executable code payload; "
            "the plan was built without serializable UDF closures"
        )
