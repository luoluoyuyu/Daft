//! Protobuf (de)serialization of the logical plan AST.
//!
//! The wire schema lives in ``src/daft-protocol/proto/daft/v1/plan.proto`` and
//! is compiled to Rust by the ``daft-protocol`` crate (prost). This module
//! converts between the in-memory ``daft-logical-plan`` AST and the generated
//! ``daft.v1`` messages so that plans can be transported to the pure-Rust
//! ``daft-runtime`` server as protobuf — never bincode and never JSON.
//!
//! Each logical structure lives in its own file:
//! * ``helpers`` — shared error / required / byte-splitting helpers
//! * ``datatype`` — DataType / Field / Schema
//! * ``series`` — Series / RecordBatch Arrow IPC payloads
//! * ``literal`` — Literal / IntervalValue / FileReference
//! * ``runtime`` — RuntimePyObject / ResourceRequest
//! * ``enums`` — small operator / join / sketch / on-error enums
//! * ``expression`` — Expr / Column / PlanRef trees
//! * ``agg`` — AggExpr / MapGroupsFn
//! * ``window`` — WindowExpr / WindowBoundary / WindowFrame / WindowSpec
//! * ``function`` — scalar / partitioning function expressions
//! * ``udf`` — Python UDF closures and UDFProperties
//! * ``stats`` — StatsState / Sharder
//! * ``partition`` — RepartitionSpec / ClusteringSpec / partition fields
//! * ``io`` — Pushdowns / IOConfig (bincode bytes)
//! * ``scan`` — ScanSource / ScanTask / ScanState / PhysicalScanInfo / SourceInfo
//! * ``plan`` — top-level LogicalPlan tree conversion
//!
//! Binary leaf policy (the only bincode/Arrow IPC left in the protocol):
//! * ``Series`` / ``RecordBatch`` payloads (literals, range-repartition
//!   boundaries) are Arrow IPC streams.
//! * Opaque configuration / metadata structs (``IOConfig``, ``SourceConfig``,
//!   ``StorageConfig``, ``TableMetadata``, ``TableStatistics``,
//!   ``PartitionSpec``, ``DaftParquetMetadata``, ``ResourceRequest``, image
//!   payloads) are bincode-encoded bytes.
//! * Python objects (UDF closures, bound args, ray options, vLLM engine args,
//!   catalog sinks) are cloudpickle bytes produced by the Python-side
//!   serializer; the pure-Rust runtime treats them as opaque and only the
//!   Python worker unpickles them.

pub mod agg;
pub mod datatype;
pub mod enums;
pub mod expression;
pub mod function;
pub mod helpers;
pub mod io;
pub mod key_filtering;
pub mod literal;
pub mod partition;
pub mod plan;
pub mod runtime;
pub mod scan;
pub mod series;
pub mod sink;
pub mod stats;
pub mod udf;
pub mod window;

// Shared imports re-exported so every submodule can `use super::*;`.
pub(crate) use std::{num::NonZeroUsize, sync::Arc};

pub(crate) use common_error::{DaftError, DaftResult};
pub(crate) use common_hashable_float_wrapper::FloatWrapper;
pub(crate) use common_io_config::IOConfig;
pub(crate) use common_resource_request::ResourceRequest;
pub(crate) use daft_core::{
    datatypes::IntervalValue,
    file::FileReference,
    join::JoinSide,
    prelude::*,
    series::Series,
};
pub(crate) use daft_dsl::{
    AggExpr, ApproxPercentileParams, Column, Expr, ExprRef, PlanRef, ResolvedColumn, SketchType,
    UnresolvedColumn, WindowExpr,
    expr::BoundColumn,
    expr::bound_expr::BoundExpr,
    expr::{MapGroupsFn, VLLMExpr},
    expr::window::{WindowBoundary, WindowFrame, WindowSpec},
    functions::{
        FunctionArg, FunctionArgs, FUNCTION_REGISTRY,
        FunctionExpr as DslFunctionExpr,
        map::MapExpr as DslMapExpr,
        partitioning::PartitioningExpr as DslPartitioningExpr,
        python::{LegacyPythonUDF, MaybeInitializedUDF, OnError, RuntimePyObject, UDFProperties},
        scalar::{BuiltinScalarFn, BuiltinScalarFnVariant, ScalarFn as DslScalarFn},
        sketch::{HashableVecPercentiles, SketchExpr as DslSketchExpr},
        struct_::StructExpr as DslStructExpr,
    },
    python_udf::{BatchPyFn, PyScalarFn as DslPyScalarFn, RowWisePyFn},
};
pub(crate) use daft_protocol::daft::v1 as proto;
pub(crate) use daft_parquet::DaftParquetMetadata;
pub(crate) use daft_recordbatch::RecordBatch;
pub(crate) use daft_schema::{
    dtype::DataType,
    field::{Field, Metadata},
    image_mode::ImageMode,
    media_type::MediaType,
    schema::{Schema, SchemaRef},
    time_unit::TimeUnit,
};
pub(crate) use daft_scan::{
    ChunkSpec, PartitionField, PartitionTransform, PhysicalScanInfo, Pushdowns, ScanSource,
    ScanSourceKind, ScanState, ScanTask, Sharder, SourceConfig,
    storage_config::StorageConfig,
};
pub(crate) use daft_stats::{PartitionSpec, TableMetadata, TableStatistics};
pub(crate) use indexmap::IndexMap;

pub(crate) use crate::{
    ops::{
        Aggregate, Concat, Distinct, Explode, Filter, IntoBatches, IntoPartitions, Limit, Offset,
        Project, Pivot, Repartition, Shard, ShuffleRead, ShuffleWrite, Sort, UDFProject, Unpivot,
    },
    partitioning::{
        ClusteringSpec, ClusteringSpecRef, HashClusteringConfig, HashRepartitionConfig,
        RandomClusteringConfig, RandomShuffleConfig, RangeClusteringConfig,
        RangeRepartitionConfig, RepartitionSpec, UnknownClusteringConfig,
    },
    source_info::{GlobScanInfo, InMemoryInfo, PlaceHolderInfo, SourceInfo},
    stats::{AlwaysSame, ApproxStats, PlanStats, StatsState},
    LogicalPlan,
};

// Re-export submodule items so sibling files can `use super::*;`.
pub(crate) use agg::*;
pub(crate) use datatype::*;
pub(crate) use enums::*;
pub(crate) use expression::*;
pub(crate) use function::*;
pub(crate) use helpers::*;
pub(crate) use io::*;
pub(crate) use key_filtering::*;
pub(crate) use literal::*;
pub(crate) use partition::*;
pub(crate) use plan::*;
pub(crate) use runtime::*;
pub(crate) use scan::*;
pub(crate) use series::*;
pub(crate) use sink::*;
pub(crate) use stats::*;
pub(crate) use udf::*;
pub(crate) use window::*;

// Public API surface.
pub use datatype::{DataTypeProto, ProtoDataType, ProtoSchema, SchemaProto, field_to_proto};
pub use expression::{expr_from_proto, expr_to_proto};
pub use literal::{LiteralProto, ProtoLiteral};
pub use plan::{plan_from_proto, plan_to_proto};
pub use series::{series_from_ipc, series_to_ipc};
