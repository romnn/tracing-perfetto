# tracing-perfetto

A [`tracing`](https://docs.rs/tracing) layer that writes spans, events and process memory as a
[Perfetto](https://perfetto.dev) trace, ready to open in the [Perfetto UI](https://ui.perfetto.dev).

It provides:

- A `process.rss` counter track that samples the process's resident set size, with a pluggable
  `ProcessMemorySource`.
- A track model for concurrent code: every root span gets its own track, sequential children
  nest on their parent's track, and overlapping children get sub-tracks, so slices always nest.
- Thread slices: each entry of a span, such as each poll of an instrumented future, is a slice
  on the track of the thread that ran it, linked to the span's lifetime slice.
- Streaming: slice begins and events are written as they happen, so spans that are still open,
  or never close, appear in the trace.
- Named tracks through a `perfetto.track_name` span field.
- Slice names from `perfetto.slice_name` or OpenTelemetry's `otel.name`.
- Span and event fields as debug annotations, and a marker filter that records only the spans and
  events with a chosen field.
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

## Tracks

In the Perfetto UI, spans and events appear on these tracks below the process:

- Each root span has a track named after the span, or after its `perfetto.track_name` field.
  Tracks of the same name merge into one visual track with a lane for each span open at the same
  time, so 50 concurrent `handle` tasks show as one `handle` track with up to 50 lanes.
- A child span nests beneath its parent's slice on the parent's track.
  A child that overlaps an open sibling, as with `join!`, moves to a sub-track named after it.
- Events inside a span are instants on the span's track; other events are instants on the
  recording thread's track.
- Each thread's track shows where spans actually ran: every entry of a span is a slice from the
  enter to the exit, so each poll of an instrumented future is one slice on its worker's track.
  A span's entered slices and its lifetime slice share a `correlation_id` of the span's own,
  which links them across threads.

`with_thread_slices(false)` turns entered slices off.
`with_span_lifetimes(false)` turns lifetime slices off, which leaves a synchronous program a plain
flame chart per thread.

## Development

The Perfetto bindings in `src/generated/` are committed, so building the crate needs neither
`protoc` nor a build script.
`task protos:update` fetches the trace proto of the Perfetto release named in `taskfile.yaml`
and regenerates them; `task protos` regenerates them from the current `protos/`.

## License

MIT.
The vendored `protos/perfetto_trace.proto` is part of Perfetto and licensed under the Apache License
2.0; see `protos/LICENSE`.

## Acknowledgements

This crate grew out of [csmoe/tracing-perfetto](https://github.com/csmoe/tracing-perfetto), which
is licensed under MIT.
