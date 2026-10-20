# tracing-perfetto

A [`tracing`](https://docs.rs/tracing) layer that writes spans, events and process memory as a
[Perfetto](https://perfetto.dev) trace, ready to open in the [Perfetto UI](https://ui.perfetto.dev).

It provides:

- A track model for concurrent code: every root span gets its own track, sequential children
  nest on their parent's track, and overlapping children get sub-tracks, so slices always nest.
- Thread slices: each entry of a span, such as each poll of an instrumented future, is a slice
  on the track of the thread that ran it, linked to the span's lifetime slice.
- Streaming: slice begins and events are written as they happen, so spans that are still open,
  or never close, appear in the trace.
- Named tracks through a `perfetto.track_name` span field.
- Slice names from `perfetto.slice_name` or OpenTelemetry's `otel.name`.
- Span and event fields as debug annotations.
- Counter tracks from numeric `counter.<name>` event fields, with an optional unit, as in
  `counter.buffered.bytes`.
- A `process.rss` counter track that samples the process's resident set size, with a pluggable
  `ProcessMemorySource`.
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
    // The global dispatcher never drops the layer, so the handle ends the trace.
    let finish = layer.finish_handle();
    tracing_subscriber::registry().with(layer).init();

    tracing::info_span!("work", perfetto.track_name = "jobs").in_scope(|| {
        tracing::info!("working");
    });
    // Ends the trace, flushes the writer, and reports the first failed write or flush.
    finish.finish()?;
    Ok(())
}
```

Debug annotations are off by default, because formatting every field costs time on every span
and event, and the fields recorded on a span stay in memory until it closes.

## Installing

Install the subscriber that holds the layer as the global default, with `init` or
`set_global_default`, so that spans close under it on every thread that sets no default of its
own.

A scoped default from `with_default` or `set_default` applies only on the thread that sets it.
tracing-subscriber 0.3.23 releases a closing span's hold on its parent through the default of
the thread it closes on, not through the span's own dispatcher ([tokio-rs/tracing#2870]), so a
span that closes under another default breaks its parent's slice ([#9]):

- With no default there, the parent and its ancestors never close.
  Their lifetime slices have no end, and the parent's later siblings move to sub-tracks.
- With another registry there, the close panics, or closes that registry's span of the
  parent's ID.

Under a scoped default, drop spans only where their dispatcher is the default.
`WithSubscriber::with_subscriber` sets the default only while it polls the future, not while the
future is dropped.
A span that the future still holds then, such as the span that `Instrument::instrument` attaches,
closes outside the default even after the future completes, and a cancelled future closes all of
its spans there.
For futures that run on other threads, use the global default, or a wrapper or executor that sets
the default both while it polls a future and while it drops it.
Set the default as well on threads that drop spans handed to them.

## Tracks

In the Perfetto UI, spans and events appear on these tracks below the process:

- Each root span, and each span with a `perfetto.track_name` field, has a track of its own named
  after the track name or else after the span.
  Tracks of the same name merge into one visual track with a lane for each span open at the same
  time, so 50 concurrent `handle` tasks show as one `handle` track with up to 50 lanes.
- Any other child span nests beneath its parent's slice on the parent's track.
  A child that overlaps an open sibling, as with `join!`, moves to a sub-track named after it.
- Events inside a span are instants on the span's track; other events are instants on the
  recording thread's track.
- Each thread's track shows where spans actually ran: every entry of a span is a slice from the
  enter to the exit, so each poll of an instrumented future is one slice on its worker's track.
  A span's entered slices and its lifetime slice share a `correlation_id` of the span's own,
  which links them across threads.
- A thread's track is the thread track of its kernel thread ID.
  Where that ID is unknown, as on Linux without procfs, the track is named after the thread and
  sits below the process track instead, so it never claims another thread's ID or merges with
  another thread's track.

`with_thread_slices(false)` turns entered slices off.
`with_span_lifetimes(false)` turns lifetime slices off, which leaves a synchronous program a plain
flame chart per thread.

## Counters

An event field named `counter.<name>` with a numeric value is a value on a counter track named
`<name>`, at the time of the event:

```rust
tracing::info!(counter.queue_depth = 3, counter.buffered.bytes = 4096_u64, "enqueued");
```

- When the rest of the field's name has more than one `.`-segment, the last one is the counter's
  unit: `bytes`, `ns` and `count` are the counter units that Perfetto knows, and any other unit is
  shown as written.
- Each counter track sits below the process track, so the values of one counter from every thread
  are one series, and the unit of the counter's first value is the unit of its track.
- An integer that fits an `i64` is written as an integer, and any other number as the nearest
  double, so a `u64` above `i64::MAX` keeps its magnitude but may lose its last digits.
- The event is still an instant, which carries its other fields as debug annotations when
  `with_debug_annotations(true)` enables them.
  A `counter.*` field of another type, such as a string or a boolean, is then a debug annotation
  like any other field.

## Filtering

The layer records every span and event that reaches it.
To record only some, give it a per-layer filter through `Layer::with_filter`, for example on a
target or on a field that marks the spans and events to record:

```rust
let layer = PerfettoLayer::new(writer).with_filter(tracing_subscriber::filter::filter_fn(
    |metadata| metadata.fields().field("perfetto").is_some(),
));
```

A field filter reads the callsite's metadata, so it also selects spans that declare the field
without a value, as `#[instrument(fields(perfetto))]` does.
A span that the filter rejects has no slices, and the layer does not see it:

- Its recorded children nest on the track of its nearest recorded ancestor.
- An event inside it goes on the track of the innermost recorded span entered on the recording
  thread, or else on the thread's track.
  That span is the nearest recorded ancestor when the rejected span is entered directly inside
  it, but it can be an unrelated span that happens to be entered.
- An event that names the rejected span as its explicit parent goes on the thread's track.

If no layer of the registry enables the rejected span's callsite, as with the single filtered
layer above, the span is `Span::none`.
A span created inside a task that it instruments on another thread, and a span that names it as
its explicit parent, then become roots.
Another layer that enables every callsite keeps the rejected span in the registry and with it the
nesting, at the cost of creating every span.
Issue [#11] tracks routing the events of a rejected span to its nearest recorded ancestor.

## Writers

The layer writes each encoded `Trace` message as it happens, through one `write_all` call, so a
trace cut short still holds everything written so far.
A `Mutex<File>` is the usual writer, and a `Mutex<BufWriter<File>>` writes fewer, larger chunks.
`FinishHandle::finish` is the end of the trace: it waits for the writes under way, flushes the
writer, and the layer writes nothing afterwards.
Dropping the layer ends the trace the same way, but a global dispatcher never drops its layer, so
call `finish()` before the process exits.
After the first failed write the layer writes nothing more, so the file stays readable up to that
write, and `finish()` returns the error, or else an error of the flush.
A writer that panics fails the trace the same way: the panic reaches the code that recorded the
span or event, the layer never calls the writer again, not even to flush it, and `finish()`
returns an error that says that the writer panicked.
A panic on the process-memory sampler thread ends the sampler instead, and a panic while another
panic unwinds is caught, since a second panic there aborts the process.

- `tracing_appender::non_blocking` drops messages when its queue is full unless it is built with
  `lossy(false)`, and a dropped message can leave the rest of the trace unreadable.
  Its flush does not wait for the queue, so drop its `WorkerGuard` after `finish()`: dropping the
  guard flushes the queue, but it waits about a second at most, so a large backlog can still lose
  its tail.
- Spans and events that the writer records while the layer calls it, such as through a panic
  hook that records the writer's panic, are not written: the layer calls the writer while
  holding the lock that every write of the trace takes, which their writes cannot wait for.
  This holds only for the writer's own layer, so a writer must not record into another
  tracing-perfetto layer, whose writer may wait for this one in the same way.
  Nor may a writer enter, exit or drop spans that the application created, since their begin or
  end would be dropped and their slices would not balance.
- The writer may call `finish()` from inside its own write, on the thread that the layer calls
  it on, which returns at once and ends the trace when that write returns, but it must not wait
  for a `finish()` on another thread, including one that it spawns and joins, which waits for the
  write in progress.
- Do not call `finish()` or drop the layer while holding the writer's lock, such as the `Mutex`
  of a `Mutex<BufWriter<File>>`, or anything else the writer or the process-memory source needs,
  because both wait for the write in progress and then flush the writer themselves.

## Known limitations

- With tracing-subscriber 0.3.23, a child span that closes on a thread whose default dispatcher
  is another one leaves its parent's lifetime slice without an end, or panics (see
  [Installing](#installing), [#9]).

[#9]: https://github.com/romnn/tracing-perfetto/issues/9
[tokio-rs/tracing#2870]: https://github.com/tokio-rs/tracing/issues/2870
[#11]: https://github.com/romnn/tracing-perfetto/issues/11

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
