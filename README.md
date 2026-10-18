# tracing-perfetto

A [`tracing`](https://docs.rs/tracing) layer that writes spans, events and process memory as a
[Perfetto](https://perfetto.dev) trace, ready to open in the [Perfetto UI](https://ui.perfetto.dev).

This crate started as a fork of [csmoe/tracing-perfetto](https://github.com/csmoe/tracing-perfetto)
and adds:

- A `process.rss` counter track that samples the process's resident set size, with a pluggable
  `ProcessMemorySource`.
- Named tracks through a `perfetto.track_name` span field, inherited by child spans.
- Slice names from `perfetto.slice_name` or OpenTelemetry's `otel.name`.
- A `FinishHandle` that ends the trace while the layer is still installed in a global dispatcher.
- Complete traces from every layer: each trace carries its own process and thread descriptors, so
  several layers or successive trace files in one process all open correctly.

## Usage

The crate is not published on crates.io yet, so depend on it through git:

```toml
[dependencies]
tracing-perfetto = { git = "https://github.com/romnn/tracing-perfetto.git", branch = "main" }
```

```rust
use tracing_perfetto::PerfettoLayer;
use tracing_subscriber::prelude::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let file = std::fs::File::create("trace.pftrace")?;
    let layer = PerfettoLayer::new(std::sync::Mutex::new(file))
        .with_debug_annotations(true)
        .with_process_memory_rss(std::time::Duration::from_millis(100));
    tracing_subscriber::registry().with(layer).init();

    let _span = tracing::info_span!("work", perfetto.track_name = "jobs").entered();
    tracing::info!("working");
    Ok(())
}
```

## Development

The Perfetto bindings in `src/generated/` are committed, so building the crate needs neither
`protoc` nor a build script.
`task protos:update` fetches the trace proto of the Perfetto release named in `taskfile.yaml`
and regenerates them; `task protos` regenerates them from the current `protos/`.

## License

MIT.
The vendored `protos/perfetto_trace.proto` is part of Perfetto and licensed under the Apache License
2.0; see `protos/LICENSE`.
