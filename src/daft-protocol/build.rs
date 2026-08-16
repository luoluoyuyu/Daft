//! Compile the Daft wire-protocol protobuf schemas into Rust types with prost.
//!
//! Requires a ``protoc`` compiler on PATH, or the ``PROTOC`` environment
//! variable pointing at a protoc binary. The generated code is emitted into
//! OUT_DIR and included by ``src/lib.rs``.

fn main() {
    let protos = [
        "proto/daft/v1/plan.proto",
        "proto/daft/v1/runtime.proto",
        "proto/daft/v1/udf.proto",
        "proto/daft/v1/worker.proto",
    ];
    println!("cargo:rerun-if-changed=proto/daft/v1/plan.proto");
    println!("cargo:rerun-if-changed=proto/daft/v1/runtime.proto");
    println!("cargo:rerun-if-changed=proto/daft/v1/udf.proto");
    println!("cargo:rerun-if-changed=proto/daft/v1/worker.proto");

    prost_build::Config::new()
        .compile_protos(&protos, &["proto"])
        .expect("failed to compile daft-protocol protobuf schemas");
}
