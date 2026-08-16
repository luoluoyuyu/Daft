//! Server-side SQL execution.
//!
//! The Python client ships the SQL statement text plus the named DataFrame
//! bindings it resolved from the caller's scope; it never parses the SQL.
//! This module does all planning in Rust: it decodes each binding into a
//! logical plan, registers them with a fresh session, lets ``daft-sql`` build
//! the query plan, and then executes that plan through the exact same native
//! path as a plan submitted directly by the client. SQL does not support
//! Python UDFs yet; every SQL job runs on the pure-Rust engine.

use std::collections::HashMap;

use daft_logical_plan::LogicalPlanBuilder;
use daft_session::Session;

use crate::native;

/// Parse a SQL statement plus named DataFrame bindings and execute it.
///
/// ``bindings`` maps table names to serialized ``daft.v1.LogicalPlan`` bytes
/// (``LogicalPlanBuilder.to_bytes()``). The final plan is serialized back to
/// protobuf and then executed by the pure-Rust engine. The result is the
/// ``DAFTRES1`` Arrow IPC envelope.
pub fn execute_sql_job(
    sql: &str,
    bindings: HashMap<String, Vec<u8>>,
    partition_sets: HashMap<String, Vec<u8>>,
) -> Result<Vec<u8>, String> {
    // 1. Decode the named bindings into logical plan builders. These are the
    //    "CTE" tables the SQL text can reference by name.
    let mut ctes = HashMap::with_capacity(bindings.len());
    for (name, bytes) in bindings {
        let proto_plan =
            daft_protocol::decode::<daft_protocol::daft::v1::LogicalPlan>(&bytes)
                .map_err(|e| format!("failed to decode binding {name}: {e}"))?;
        let plan = daft_logical_plan::proto::plan_from_proto(proto_plan)
            .map_err(|e| format!("failed to decode binding {name}: {e}"))?;
        ctes.insert(name, LogicalPlanBuilder::new(plan, None));
    }

    // 2. Plan the statement with the pure-Rust SQL planner. Each request gets
    //    a fresh session: bindings are per-job and never outlive the job.
    let session = Session::empty();
    let plan = daft_sql::exec::execute_statement(&session, sql, ctes)
        .map_err(|e| format!("failed to plan SQL: {e}"))?;

    // 3. Statements without a result set (e.g. USE) yield an empty result.
    let Some(plan) = plan else {
        return native::encode_result_envelope(&[]).map_err(|e| e.to_string());
    };

    // 4. Serialize exactly like the client would (materialize scans, strip
    //    partition cache entries) so the execution paths stay shared.
    let plan_bytes = LogicalPlanBuilder::new(plan, None)
        .to_bytes()
        .map_err(|e| format!("failed to serialize SQL plan: {e}"))?;

    // 5. SQL executes on the pure-Rust engine only (no Python UDFs yet).
    native::execute_plan_native(plan_bytes, partition_sets)
}
