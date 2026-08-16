//! Server-side SQL planning.
//!
//! The Python client ships the SQL statement text plus the named DataFrame
//! bindings it resolved from the caller's scope; it never parses the SQL.
//! This module does all planning in Rust: it decodes each binding into a
//! logical plan, registers them with a fresh session, lets ``daft-sql`` build
//! the query plan, and returns the serialized logical plan bytes. The
//! scheduler then splits that plan into distributed stages exactly like a
//! plan submitted directly by the client. SQL does not support Python UDFs
//! yet; every SQL job runs on the pure-Rust engine.

use std::collections::HashMap;

use daft_logical_plan::LogicalPlanBuilder;
use daft_session::Session;

/// Parse a SQL statement plus named DataFrame bindings and return the
/// serialized ``daft.v1.LogicalPlan`` bytes of the resulting plan.
///
/// ``bindings`` maps table names to serialized ``daft.v1.LogicalPlan`` bytes
/// (``LogicalPlanBuilder.to_bytes()``). Serialization materializes physical
/// scans and strips partition-cache entries exactly like the client path, so
/// the scheduler can slice the plan into per-partition tasks.
pub fn plan_sql_job(sql: &str, bindings: HashMap<String, Vec<u8>>) -> Result<Vec<u8>, String> {
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
        return Err("SQL statement produced no plan".to_string());
    };

    // 4. Serialize exactly like the client would.
    LogicalPlanBuilder::new(plan, None)
        .to_bytes()
        .map_err(|e| format!("failed to serialize SQL plan: {e}"))
}
