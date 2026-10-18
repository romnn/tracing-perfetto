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
//! A span that the filter rejects has no slices, and its children and events go on the track of
//! its nearest recorded ancestor instead.
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
//!
//! # Process memory and finishing
//!
//! [`PerfettoLayer::with_process_memory_rss`] adds a counter track that samples the process's
//! resident set size.
//! A [`FinishHandle`] ends the trace while the layer is still installed, for example in a global
//! dispatcher that is never dropped.
//!
//! # Examples
//!
//! ```
//! use tracing_perfetto::PerfettoLayer;
//! use tracing_subscriber::layer::SubscriberExt;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let file = std::fs::File::create(std::env::temp_dir().join("example.pftrace"))?;
//! let layer = PerfettoLayer::new(std::sync::Mutex::new(file))
//!     .with_debug_annotations(true)
//!     .with_process_memory_rss(std::time::Duration::from_millis(100));
//! let finish = layer.finish_handle();
//!
//! tracing::subscriber::with_default(tracing_subscriber::registry().with(layer), || {
//!     let _span = tracing::info_span!("work", perfetto.track_name = "jobs").entered();
//!     tracing::info!("working");
//! });
//! finish.finish();
//! # Ok(())
//! # }
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
