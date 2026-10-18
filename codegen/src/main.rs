//! Regenerates the Perfetto protobuf bindings in `src/generated/` from `protos/`.
//!
//! The bindings are committed so that building `tracing-perfetto` needs neither `protoc` nor a
//! build script.
//! Run `task protos` after updating `protos/perfetto_trace.proto` or bumping `prost`.

use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .ok_or("codegen must live inside the tracing-perfetto workspace")?;
    let protos = root.join("protos");

    prost_build::Config::new()
        .format(true)
        .out_dir(root.join("src/generated"))
        .compile_protos(&[protos.join("perfetto_trace.proto")], &[protos])?;
    Ok(())
}
