//! A [`tracing_subscriber::Layer`] that writes spans and events as a [Perfetto] trace.
//!
//! [`PerfettoLayer`] records each span as a lifetime slice from its creation to its close, each
//! entry of a span as an entered slice on the entering thread's track, and each event as an
//! instant.
//! It streams them as encoded `Trace` protobuf packets to a [`PerfettoWriter`] as they happen, so
//! a slice's begin is in the trace while the span is still open.
//! Open the output in the [Perfetto UI].
//!
//! # Tracks
//!
//! Every root span gets its own track below the process track, named after the span, which holds
//! its lifetime slice.
//! A child span nests on its parent's track, unless another child of that parent is still open
//! there.
//! A child that overlaps such a sibling gets a sub-track of its parent's track, named after the
//! child.
//! An event inside a span is an instant on that span's track, and any other event is an instant
//! on the track of the thread that records it.
//!
//! Each thread's track shows where spans actually ran: every entry of a span is a slice from
//! the enter to the exit, so each poll of an instrumented future is one slice on the worker that
//! polled it.
//! A span's entered slices and its lifetime slice share a `correlation_id` of the span's own, which
//! links them across threads.
//! A thread's track is the thread track of its kernel thread ID.
//! Where that ID is unknown, as on Linux without procfs, the track is named after the thread and
//! sits below the process track instead, so it never claims another thread's ID or merges with
//! another thread's track.
//!
//! The Perfetto UI merges tracks of the same name into one visual track, with a lane for each
//! span that is open at the same time.
//! Fifty concurrent tasks instrumented as `handle` show as one `handle` track with up to fifty
//! lanes, each task's sequential children nested beneath its slice, and children that run
//! concurrently, as in `join!`, on sub-tracks.
//!
//! Span fields, read when the span is created, control where its slice appears:
//!
//! - `perfetto.track_name` gives the span its own track below the process track, under this
//!   name.
//! - `perfetto.slice_name` overrides the slice name, which otherwise falls back to `otel.name` and
//!   then to the span's name.
//!
//! Give these fields, `otel.name` included, a string or a `%`-formatted value.
//! A `?`-formatted value names the track or slice after its `Debug` rendering without the
//! surrounding quotes, so the escapes inside a string remain.
//! Recording either field later leaves the slice where it is.
//!
//! [`PerfettoLayer::with_thread_slices`] turns entered slices off.
//! [`PerfettoLayer::with_span_lifetimes`] turns lifetime slices off, which leaves a synchronous
//! program a plain flame chart per thread.
//!
//! # Filtering
//!
//! The layer records every span and event that reaches it.
//! To record only some, give it a [per-layer filter], which leaves the other layers of the
//! registry unaffected.
//! A span that the filter rejects has no slices, and the layer does not see it:
//!
//! - Its recorded children nest on the track of its nearest recorded ancestor.
//! - An event inside it goes on the track of the innermost recorded span entered on the
//!   recording thread, or else on the thread's track.
//!   That span is the nearest recorded ancestor when the rejected span is entered directly inside
//!   it, but it can be an unrelated span that happens to be entered.
//! - An event that names the rejected span as its explicit parent goes on the thread's track.
//!
//! If no layer of the registry enables the rejected span's callsite, as with a single filtered
//! layer, the span is [`Span::none`](tracing::Span::none).
//! A span created inside a task that it instruments on another thread, and a span that names it
//! as its explicit parent, then become roots.
//! Another layer that enables every callsite keeps the rejected span in the registry and with it
//! the nesting, at the cost of creating every span.
//! Issue [#11] tracks routing the events of a rejected span to its nearest recorded ancestor.
//!
//! A filter can select by target or level, or by a field that marks the spans and events to
//! record:
//!
//! ```
//! use tracing_perfetto::PerfettoLayer;
//! use tracing_subscriber::filter::filter_fn;
//! use tracing_subscriber::prelude::*;
//!
//! let layer = PerfettoLayer::new(std::io::sink)
//!     .with_filter(filter_fn(|metadata| metadata.fields().field("perfetto").is_some()));
//! tracing::subscriber::with_default(tracing_subscriber::registry().with(layer), || {
//!     // Recorded, because the span declares a `perfetto` field.
//!     tracing::info_span!("recorded", perfetto = true).in_scope(|| {
//!         // Not recorded, because the event declares no `perfetto` field.
//!         tracing::info!("unmarked");
//!     });
//! });
//! ```
//!
//! The filter reads the metadata of the span's callsite, so a declared field counts even
//! without a value, as in `#[instrument(fields(perfetto))]`.
//!
//! [per-layer filter]: tracing_subscriber::Layer::with_filter
//! [#11]: https://github.com/romnn/tracing-perfetto/issues/11
//!
//! # Process memory and finishing
//!
//! [`PerfettoLayer::with_process_memory_rss`] adds a counter track that samples the process's
//! resident set size.
//! A [`FinishHandle`] ends the trace while the layer is still installed, for example in a global
//! dispatcher, which never drops its layer.
//! [`FinishHandle::finish`] is the end of the trace: it waits for the writes under way, flushes
//! the writer, and nothing is written afterwards.
//! Dropping the layer ends the trace the same way.
//! [`FinishHandle::finish`] returns the writer's first error, after which the layer wrote nothing
//! more, so the output stays readable up to the failed write.
//!
//! # Installing
//!
//! Install the subscriber that holds the layer as the global default, with
//! [`SubscriberInitExt::init`] or [`tracing::subscriber::set_global_default`], so that spans close
//! under it on every thread that sets no default of its own.
//!
//! A scoped default from [`tracing::subscriber::with_default`] or
//! [`tracing::subscriber::set_default`] applies only on the thread that sets it.
//! tracing-subscriber 0.3.23 releases a closing span's hold on its parent through the default of
//! the thread it closes on, not through the span's own dispatcher ([tokio-rs/tracing#2870]), so
//! a span that closes under another default breaks its parent's slice ([#9]):
//!
//! - With no default there, the parent and its ancestors never close.
//!   Their lifetime slices have no end, and the parent's later siblings move to sub-tracks.
//! - With another registry there, the close panics, or closes that registry's span of the
//!   parent's ID.
//!
//! Under a scoped default, drop spans only where their dispatcher is the default.
//! [`WithSubscriber::with_subscriber`] sets the default only while it polls the future, not while
//! the future is dropped.
//! A span that the future still holds then, such as the span that [`Instrument::instrument`]
//! attaches, closes outside the default even after the future completes, and a cancelled future
//! closes all of its spans there.
//! For futures that run on other threads, use the global default, or a wrapper or executor that
//! sets the default both while it polls a future and while it drops it.
//! Set the default as well on threads that drop spans handed to them.
//!
//! [`SubscriberInitExt::init`]: tracing_subscriber::util::SubscriberInitExt::init
//! [`WithSubscriber::with_subscriber`]: tracing::instrument::WithSubscriber::with_subscriber
//! [`Instrument::instrument`]: tracing::instrument::Instrument::instrument
//! [tokio-rs/tracing#2870]: https://github.com/tokio-rs/tracing/issues/2870
//!
//! # Limitations
//!
//! - With tracing-subscriber 0.3.23, a child span that closes on a thread whose default
//!   dispatcher is another one leaves its parent's lifetime slice without an end, or panics (see
//!   [Installing](#installing), [#9]).
//!
//! [#9]: https://github.com/romnn/tracing-perfetto/issues/9
//!
//! # Examples
//!
//! ```
//! use tracing_perfetto::PerfettoLayer;
//! use tracing_subscriber::layer::SubscriberExt;
//!
//! // A `std::sync::Mutex<std::fs::File>` writes the trace to a file instead.
//! let layer = PerfettoLayer::new(std::io::sink)
//!     .with_debug_annotations(true)
//!     .with_process_memory_rss(std::time::Duration::from_millis(100));
//!
//! // The subscriber, and with it the layer, is dropped at the end of the scope, which ends the
//! // trace.
//! tracing::subscriber::with_default(tracing_subscriber::registry().with(layer), || {
//!     let _span = tracing::info_span!("work", perfetto.track_name = "jobs").entered();
//!     tracing::info!("working");
//! });
//! ```
//!
//! [Perfetto]: https://perfetto.dev
//! [Perfetto UI]: https://ui.perfetto.dev

#[allow(
    clippy::all,
    clippy::pedantic,
    clippy::allow_attributes_without_reason,
    unreachable_pub,
    missing_docs,
    unused,
    reason = "generated protobuf sources are not written to this workspace's lint policy"
)]
#[rustfmt::skip]
mod idl {
    include!("generated/perfetto.protos.rs");
}

mod decode;
mod fields;
mod layer;
mod lifecycle;
mod packets;
pub mod process_memory;
mod sequence;
mod sink;
mod span_state;
mod thread_slices;
mod thread_state;
mod thread_track;
mod writer;

#[cfg(test)]
mod tests;

pub use decode::{DecodeError, decode_track_event_names};
pub use layer::PerfettoLayer;
pub use lifecycle::FinishHandle;
pub use writer::PerfettoWriter;
