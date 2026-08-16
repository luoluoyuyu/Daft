use std::sync::Arc;

use daft_dsl::expr::{lit, resolved_col};
use daft_schema::{dtype::DataType, field::Field};

use crate::builder::LogicalPlanBuilder;
use crate::test::{dummy_scan_node, dummy_scan_operator};

fn assert_roundtrip(builder: &LogicalPlanBuilder) {
    let bytes = builder.to_bytes().unwrap();
    let restored = LogicalPlanBuilder::from_bytes(&bytes).unwrap();
    assert_eq!(restored.schema(), builder.schema());

    // Round-trip is idempotent: serializing the restored plan succeeds and
    // yields the same schema again.
    let bytes2 = restored.to_bytes().unwrap();
    let restored2 = LogicalPlanBuilder::from_bytes(&bytes2).unwrap();
    assert_eq!(restored2.schema(), restored.schema());
}

#[test]
fn plan_proto_roundtrip() {
    let fields = vec![
        Field::new("id", DataType::Int64),
        Field::new("x", DataType::Utf8),
    ];
    let builder = dummy_scan_node(dummy_scan_operator(fields));
    let builder = builder.filter(resolved_col("id").gt(lit(5))).unwrap();
    let builder = builder
        .select(vec![resolved_col("id"), resolved_col("x")])
        .unwrap();

    assert_roundtrip(&builder);
}

#[test]
fn join_proto_roundtrip() {
    use daft_core::join::JoinType;

    let fields = vec![
        Field::new("id", DataType::Int64),
        Field::new("x", DataType::Utf8),
    ];
    let left = dummy_scan_node(dummy_scan_operator(fields.clone()));
    let right = dummy_scan_node(dummy_scan_operator(fields));
    let builder = left
        .join(
            right.plan.clone(),
            None,
            vec!["id".to_string()],
            JoinType::Inner,
            None,
            crate::ops::join::JoinOptions {
                prefix: Some("left".to_string()),
                suffix: Some("right".to_string()),
            },
        )
        .unwrap();

    assert_roundtrip(&builder);
}

#[test]
fn union_proto_roundtrip() {
    use crate::ops::{SetQuantifier, UnionStrategy};

    let fields = vec![
        Field::new("id", DataType::Int64),
        Field::new("x", DataType::Utf8),
    ];
    let left = dummy_scan_node(dummy_scan_operator(fields.clone()));
    let right = dummy_scan_node(dummy_scan_operator(fields));
    let builder = left
        .union(&right, SetQuantifier::All, UnionStrategy::Positional)
        .unwrap();

    assert_roundtrip(&builder);
}

#[test]
fn window_proto_roundtrip() {
    use daft_dsl::expr::window::WindowSpec;
    use daft_dsl::WindowExpr;
    use std::sync::Arc;

    let fields = vec![
        Field::new("group", DataType::Int64),
        Field::new("value", DataType::Int64),
    ];
    let input = dummy_scan_node(dummy_scan_operator(fields)).plan.clone();

    let mut window_spec = WindowSpec::default();
    window_spec.partition_by = vec![resolved_col("group")];
    let window_expr: WindowExpr = resolved_col("value").min().try_into().unwrap();

    let window_op = crate::ops::Window::try_new(
        input,
        vec![window_expr],
        vec!["min_value".to_string()],
        Arc::new(window_spec),
    )
    .unwrap();
    let plan: crate::LogicalPlan = window_op.into();
    let builder = LogicalPlanBuilder::new(Arc::new(plan), None);

    assert_roundtrip(&builder);
}

#[test]
fn sink_proto_roundtrip() {
    use common_file_formats::{FileFormat, WriteMode};

    let fields = vec![Field::new("id", DataType::Int64)];
    let input = dummy_scan_node(dummy_scan_operator(fields)).plan.clone();
    let sink_info = crate::sink_info::OutputFileInfo::new(
        "/tmp/out".to_string(),
        WriteMode::Append,
        FileFormat::Csv,
        Some(crate::sink_info::FormatSinkOption::Csv(
            crate::sink_info::CsvFormatOption::default(),
        )),
        None,
        Some("gzip".to_string()),
        None,
        true,
    );
    let sink_op = crate::ops::Sink::try_new(input, Arc::new(crate::sink_info::SinkInfo::OutputFileInfo(sink_info))).unwrap();
    let plan: crate::LogicalPlan = sink_op.into();
    let builder = LogicalPlanBuilder::new(Arc::new(plan), None);

    assert_roundtrip(&builder);
}

#[test]
fn vllm_project_proto_roundtrip() {
    use common_hashable_float_wrapper::FloatWrapper;
    use daft_dsl::expr::VLLMExpr;
    use daft_dsl::functions::python::RuntimePyObject;

    let fields = vec![Field::new("id", DataType::Utf8)];
    let input = dummy_scan_node(dummy_scan_operator(fields)).plan.clone();
    let expr = VLLMExpr {
        model: "test-model".to_string(),
        input: resolved_col("id"),
        concurrency: 1,
        gpus_per_actor: 1,
        do_prefix_routing: false,
        max_buffer_size: 10,
        min_bucket_size: 2,
        prefix_match_threshold: FloatWrapper(0.5),
        load_balance_threshold: 2,
        batch_size: None,
        engine_args: RuntimePyObject::new_none(),
        generate_args: RuntimePyObject::new_none(),
    };
    let project_op = crate::ops::VLLMProject::new(input, expr, "output".into());
    let plan = crate::LogicalPlan::VLLMProject(project_op);
    let builder = LogicalPlanBuilder::new(Arc::new(plan), None);

    assert_roundtrip(&builder);
}
