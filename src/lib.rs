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

use bytes::BytesMut;
use idl_helpers::{DebugAnnotations, create_event, current_thread_uuid};
use prost::Message;
use sequence::TraceSequence;
use std::borrow::Cow;
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tracing::Event;
use tracing::Id;
use tracing::Metadata;
use tracing::Subscriber;
use tracing::field::Field;
use tracing::field::Visit;
use tracing::span;
use tracing_subscriber::Layer;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::{ExtensionsMut, LookupSpan};

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

mod idl_helpers;
mod sequence;
#[cfg(test)]
mod test_support;
#[cfg(test)]
mod thread_slice_tests;
mod thread_slices;
#[cfg(test)]
mod track_tests;

pub mod process_memory;

/// What a [`PerfettoLayer`] keeps of a span it records.
struct PerfettoSpanState {
    /// The name of the span's lifetime slice and of its entered slices.
    slice_name: Cow<'static, str>,
    /// The ID that links the span's entered slices to each other and to its lifetime slice.
    ///
    /// It is random and the span's own, so it never links a span to a parent whose track it
    /// shares.
    correlation_id: u64,
    /// The span's lifetime slice, if span lifetimes are recorded.
    lifetime: Option<LifetimeSlice>,
    /// The fields given at the span's creation, written on each entered slice's begin.
    ///
    /// This stays empty while span lifetimes are recorded, because the lifetime slice's begin
    /// carries these fields instead.
    creation_annotations: Vec<idl::DebugAnnotation>,
    /// The fields recorded after the span's creation.
    ///
    /// They are written on the lifetime slice's end, or, without span lifetimes, on the begin of
    /// each slice entered after the record.
    recorded_annotations: Vec<idl::DebugAnnotation>,
}

/// The lifetime slice of a span that a [`PerfettoLayer`] has begun and not yet ended.
struct LifetimeSlice {
    /// The track that holds the span's slice.
    track_uuid: u64,
    /// The number of the span's open children whose slices nest on the span's track.
    ///
    /// A new child nests on the track only while this is 0, so the span's slice is then the
    /// innermost open slice there.
    open_children_on_track: usize,
    /// The span whose track this span's slice nests on, if any.
    ///
    /// This span holds one count of that span's `open_children_on_track` until it closes.
    track_parent: Option<Id>,
}

/// Where a new span's slice goes.
struct SpanPlacement {
    track_uuid: u64,
    /// The descriptor of a track created for this span, written with the slice's begin.
    new_track: Option<idl::TrackDescriptor>,
    track_parent: Option<Id>,
}

impl SpanPlacement {
    /// Places the slice on a new track below `parent_uuid`.
    ///
    /// The track's UUID is never reused, so concurrent spans of one name never share a track.
    fn new_track(parent_uuid: u64, name: &str) -> Self {
        let track_uuid = idl_helpers::unique_uuid();
        Self {
            track_uuid,
            new_track: Some(idl_helpers::create_track_descriptor(
                Some(track_uuid),
                Some(parent_uuid),
                Some(name),
                None,
                None,
                None,
            )),
            track_parent: None,
        }
    }
}

/// The span states of every [`PerfettoLayer`] on a registry, stored as one span extension.
///
/// [`ExtensionsMut::insert`] panics when a span already holds a value of the same type, so the
/// layers share this container and each one touches only the entry with its own [`LayerId`].
struct PerfettoSpanStates {
    entries: Vec<LayerSpanState>,
}

struct LayerSpanState {
    layer_id: LayerId,
    state: PerfettoSpanState,
}

impl PerfettoSpanStates {
    /// Adds `state` as the entry of `layer_id`, creating the container on the span's first entry.
    fn insert(extensions: &mut ExtensionsMut<'_>, layer_id: LayerId, state: PerfettoSpanState) {
        let entry = LayerSpanState { layer_id, state };
        if let Some(states) = extensions.get_mut::<Self>() {
            states.entries.push(entry);
        } else {
            extensions.insert(Self {
                entries: vec![entry],
            });
        }
    }

    fn get(&self, layer_id: LayerId) -> Option<&PerfettoSpanState> {
        self.entries
            .iter()
            .find(|entry| entry.layer_id == layer_id)
            .map(|entry| &entry.state)
    }

    fn get_mut(&mut self, layer_id: LayerId) -> Option<&mut PerfettoSpanState> {
        self.entries
            .iter_mut()
            .find(|entry| entry.layer_id == layer_id)
            .map(|entry| &mut entry.state)
    }

    fn remove(&mut self, layer_id: LayerId) -> Option<PerfettoSpanState> {
        let index = self
            .entries
            .iter()
            .position(|entry| entry.layer_id == layer_id)?;
        Some(self.entries.swap_remove(index).state)
    }
}

/// Identifies one [`PerfettoLayer`] among all layers in the process.
///
/// IDs are never reused, so state keyed by a dropped layer's ID never applies to a later layer.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct LayerId(u64);

impl LayerId {
    fn next() -> Self {
        static NEXT_LAYER_ID: AtomicU64 = AtomicU64::new(0);
        Self(NEXT_LAYER_ID.fetch_add(1, Ordering::Relaxed))
    }
}

/// A [`Layer`] that streams spans as slices and events as instants into a Perfetto trace.
///
/// A span's lifetime slice begins when the span is created and ends when it closes, whichever
/// thread closes it, on a track chosen as the [crate documentation](crate#tracks) describes.
/// Each time a thread enters the span, an entered slice runs on that thread's track until the
/// span exits, so each poll of an instrumented future is one slice on the worker that ran it.
/// Every packet is written when the layer observes it, so a trace cut short still holds every
/// slice begun so far.
///
/// Timestamps come from a monotonic clock that the trace maps onto wall-clock time.
/// Several layers can share one registry, and each writes a trace that opens on its own.
pub struct PerfettoLayer<W = fn() -> std::io::Stdout> {
    layer_id: LayerId,
    sequence: Arc<TraceSequence>,
    writer: Arc<W>,
    config: Config,

    process_memory_rss_track_uuid: TrackUuid,
    lifecycle: Arc<TraceLifecycle>,
}

/// The state a [`FinishHandle`] shares with its layer.
#[derive(Default)]
struct TraceLifecycle {
    finished: AtomicBool,
    process_memory_sampler: parking_lot::Mutex<Option<ProcessMemorySampler>>,
}

impl TraceLifecycle {
    fn finish(&self) {
        self.finished.store(true, Ordering::Release);
        if let Some(sampler) = self.process_memory_sampler.lock().take() {
            sampler.stop();
        }
    }

    fn is_finished(&self) -> bool {
        self.finished.load(Ordering::Acquire)
    }
}

/// Ends a [`PerfettoLayer`]'s trace while something else still owns the layer.
///
/// A layer inside a process-global dispatcher is never dropped, so its owner finishes the trace
/// through this handle instead.
pub struct FinishHandle {
    lifecycle: Arc<TraceLifecycle>,
}

impl FinishHandle {
    /// Stops the layer from writing and joins its process-memory sampler.
    ///
    /// Spans and events recorded afterwards are discarded, so the trace stays complete as written.
    /// Finishing again has no further effect.
    pub fn finish(&self) {
        self.lifecycle.finish();
    }
}

/// The error returned when a buffer is not a valid perfetto `Trace` protobuf.
#[derive(Debug)]
pub struct DecodeError(prost::DecodeError);

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("invalid perfetto trace")
    }
}

impl std::error::Error for DecodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}

/// Decodes a perfetto trace buffer and returns the names of all contained track events.
///
/// Events without a name, such as slice ends, are skipped.
///
/// # Errors
///
/// Returns [`DecodeError`] if the buffer does not contain a valid perfetto `Trace` protobuf.
pub fn decode_track_event_names(buf: &[u8]) -> Result<Vec<String>, DecodeError> {
    let trace = idl::Trace::decode(buf).map_err(DecodeError)?;
    let mut names = Vec::new();
    for packet in trace.packet {
        let Some(idl::trace_packet::Data::TrackEvent(event)) = packet.data else {
            continue;
        };

        let Some(idl::track_event::NameField::Name(name)) = event.name_field else {
            continue;
        };
        names.push(name);
    }
    Ok(names)
}

/// Writes encoded records into provided instance.
///
/// This is implemented for types implements [`MakeWriter`].
pub trait PerfettoWriter {
    /// Writes an encoded perfetto `Trace` buffer.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying writer fails.
    fn write_log(&self, buf: BytesMut) -> std::io::Result<()>;
}

impl<W: for<'writer> MakeWriter<'writer> + 'static> PerfettoWriter for W {
    fn write_log(&self, buf: BytesMut) -> std::io::Result<()> {
        self.make_writer().write_all(&buf)
    }
}

struct Config {
    debug_annotations: bool,
    span_lifetimes: bool,
    thread_slices: bool,
    filter: Option<fn(&str) -> bool>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            debug_annotations: false,
            span_lifetimes: true,
            thread_slices: true,
            filter: None,
        }
    }
}

impl<W: PerfettoWriter> PerfettoLayer<W> {
    /// Creates a layer that writes encoded trace packets to `writer`.
    ///
    /// Spans are recorded as lifetime and entered slices, debug annotations and the field-name
    /// filter start disabled, and no process memory is sampled.
    #[must_use]
    pub fn new(writer: W) -> Self {
        Self {
            layer_id: LayerId::next(),
            sequence: Arc::new(TraceSequence::new()),
            writer: Arc::new(writer),
            config: Config::default(),

            process_memory_rss_track_uuid: TrackUuid::new(idl_helpers::unique_uuid()),
            lifecycle: Arc::default(),
        }
    }

    /// Returns a handle that ends this layer's trace without dropping the layer.
    #[must_use]
    pub fn finish_handle(&self) -> FinishHandle {
        FinishHandle {
            lifecycle: Arc::clone(&self.lifecycle),
        }
    }

    /// Configures whether or not spans/events should be recorded with their metadata and fields.
    #[must_use]
    pub fn with_debug_annotations(mut self, value: bool) -> Self {
        self.config.debug_annotations = value;
        self
    }

    /// Configures whether spans are recorded as slices from their creation to their close.
    ///
    /// Enabled by default.
    /// When disabled, spans get no tracks of their own and every event is an instant on the
    /// track of the thread that records it.
    /// Entered slices then carry the span's fields and source location, and with
    /// [`Self::with_thread_slices`] left on, a synchronous program gets a plain flame chart per
    /// thread.
    #[must_use]
    pub fn with_span_lifetimes(mut self, value: bool) -> Self {
        self.config.span_lifetimes = value;
        self
    }

    /// Configures whether spans are recorded as slices on the tracks of the threads that enter
    /// them.
    ///
    /// Enabled by default.
    /// Each entry of a span, such as one poll of an instrumented future, is a slice from the
    /// enter to the exit on the entering thread's track.
    /// Every entered slice of a span carries the span's own `correlation_id`, as its lifetime
    /// slice does, so the Perfetto UI can link exactly that span's slices across threads.
    /// When disabled, thread tracks hold only events.
    #[must_use]
    pub fn with_thread_slices(mut self, value: bool) -> Self {
        self.config.thread_slices = value;
        self
    }

    /// Configures whether or not spans/events be recorded based on the occurrence of a field name.
    ///
    /// Sometimes, not all the events/spans should be treated as perfetto trace, you can append a
    /// field to indicate that this even/span should be captured into trace:
    ///
    /// ```rust
    /// use tracing_perfetto::PerfettoLayer;
    /// use tracing_subscriber::{layer::SubscriberExt, Registry, prelude::*};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let file = std::fs::File::create(std::env::temp_dir().join("test.pftrace"))?;
    /// let layer = PerfettoLayer::new(file)
    ///     .with_filter_by_marker(|field_name| field_name == "perfetto");
    /// tracing_subscriber::registry().with(layer).init();
    ///
    /// // this event will be record, as it contains a `perfetto` field
    /// tracing::info!(perfetto = true, my_bool = true);
    ///
    /// // this span will be record, as it contains a `perfetto` field
    /// #[tracing::instrument(fields(perfetto = true))]
    /// fn to_instr() {
    ///   // this event will be ignored
    ///   tracing::info!(my_bool = true);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn with_filter_by_marker(mut self, filter: fn(&str) -> bool) -> Self {
        self.config.filter = Some(filter);
        self
    }

    /// Samples the process's resident set size every `interval` onto a `process.rss` counter track.
    ///
    /// The samples come from [`process_memory::default_process_memory_source`].
    /// On a platform it does not support, the layer is returned unchanged.
    /// The sampler runs on its own thread until the layer is dropped or its trace is finished
    /// through a [`FinishHandle`].
    #[must_use]
    pub fn with_process_memory_rss(self, interval: Duration) -> Self
    where
        W: Send + Sync + 'static,
    {
        let Some(source) = process_memory::default_process_memory_source() else {
            return self;
        };
        self.with_process_memory_source(interval, source)
    }

    /// Samples the resident set size from `source` every `interval` onto a `process.rss` counter
    /// track.
    ///
    /// Intervals in which `source` reports no value are skipped.
    /// Calling this again stops and joins the previous sampler before the new one takes over.
    #[must_use]
    pub fn with_process_memory_source(
        self,
        interval: Duration,
        source: Box<dyn process_memory::ProcessMemorySource>,
    ) -> Self
    where
        W: Send + Sync + 'static,
    {
        let writer = Arc::clone(&self.writer);
        let sequence = Arc::clone(&self.sequence);
        let counter_track_uuid = self.process_memory_rss_track_uuid.get();
        let sampler = ProcessMemorySampler::spawn(ProcessMemorySamplerConfig {
            writer,
            sequence,
            counter_track_uuid,
            interval,
            source,
        });
        let replaced = self
            .lifecycle
            .process_memory_sampler
            .lock()
            .replace(sampler);
        if let Some(replaced) = replaced {
            replaced.stop();
        }
        self
    }

    /// Writes `packets` as one `Trace` message, unless the trace is finished.
    fn write_packets(&self, packets: Vec<idl::TracePacket>) {
        if self.lifecycle.is_finished() {
            return;
        }
        self.sequence
            .write(self.writer.as_ref(), &idl::Trace { packet: packets });
    }

    /// Writes `events` on the calling thread's track, which is described on its first use.
    ///
    /// The events share one timestamp, so slices that a non-LIFO exit ends and begins again
    /// resume exactly where they ended.
    fn write_on_thread_track(&self, events: Vec<idl::TrackEvent>) {
        if self.lifecycle.is_finished() {
            return;
        }
        let track_uuid = current_thread_uuid();
        let mut packets = Vec::with_capacity(events.len() + 1);
        if idl_helpers::first_thread_track_use(self.layer_id) {
            packets.push(track_descriptor_packet(
                idl_helpers::current_thread_track_descriptor(),
            ));
        }
        let timestamp = self.sequence.now();
        packets.extend(events.into_iter().map(|mut event| {
            event.track_uuid = Some(track_uuid);
            self.event_packet_at(event, timestamp)
        }));
        self.write_packets(packets);
    }

    /// Wraps `event` in a packet timestamped now.
    fn event_packet(&self, event: idl::TrackEvent) -> idl::TracePacket {
        self.event_packet_at(event, self.sequence.now())
    }

    fn event_packet_at(&self, event: idl::TrackEvent, timestamp: u64) -> idl::TracePacket {
        idl::TracePacket {
            data: Some(idl::trace_packet::Data::TrackEvent(event)),
            ..self.sequence.packet_at(timestamp)
        }
    }

    /// Writes the begin of a new span's lifetime slice and returns where that slice lies.
    fn begin_lifetime_slice<S>(
        &self,
        span: &tracing_subscriber::registry::SpanRef<'_, S>,
        own_track_name: Option<&str>,
        slice_name: &str,
        correlation_id: u64,
        debug_annotations: DebugAnnotations,
    ) -> LifetimeSlice
    where
        S: for<'a> LookupSpan<'a>,
    {
        // The begin is timestamped after the placement, so it follows the end of any sibling
        // whose close freed the track.
        let placement = self.place_span(span, own_track_name);
        let mut event = create_event(
            placement.track_uuid,
            Some(slice_name),
            span.metadata().file().zip(span.metadata().line()),
            debug_annotations,
            Some(idl::track_event::Type::SliceBegin),
        );
        event.correlation_id_field = Some(idl::track_event::CorrelationIdField::CorrelationId(
            correlation_id,
        ));
        let mut packets = Vec::with_capacity(2);
        packets.extend(placement.new_track.map(track_descriptor_packet));
        packets.push(self.event_packet(event));
        self.write_packets(packets);

        LifetimeSlice {
            track_uuid: placement.track_uuid,
            open_children_on_track: 0,
            track_parent: placement.track_parent,
        }
    }

    /// Builds the begin of a slice of `span_state` entered on the calling thread.
    ///
    /// A lifetime slice carries the span's fields and source location, so an entered slice
    /// repeats them only when span lifetimes are off.
    fn entered_slice_begin(
        &self,
        metadata: &'static Metadata<'static>,
        span_state: &PerfettoSpanState,
    ) -> idl::TrackEvent {
        let with_details = !self.config.span_lifetimes;
        let annotations = if with_details {
            span_state
                .creation_annotations
                .iter()
                .chain(&span_state.recorded_annotations)
                .cloned()
                .collect()
        } else {
            Vec::new()
        };
        let location = with_details
            .then(|| metadata.file().zip(metadata.line()))
            .flatten();
        let mut event = create_event(
            current_thread_uuid(),
            Some(&span_state.slice_name),
            location,
            DebugAnnotations { annotations },
            Some(idl::track_event::Type::SliceBegin),
        );
        event.correlation_id_field = Some(idl::track_event::CorrelationIdField::CorrelationId(
            span_state.correlation_id,
        ));
        event
    }

    /// Chooses the track of a new span's slice.
    ///
    /// A span nests on the track of its nearest recorded ancestor while no other child of that
    /// ancestor is open there.
    /// The ancestor's slice is then the innermost open slice on the track, because a span on a
    /// track holds its ancestor's count until it closes and a child closes before its parent.
    fn place_span<S>(
        &self,
        span: &tracing_subscriber::registry::SpanRef<'_, S>,
        own_track_name: Option<&str>,
    ) -> SpanPlacement
    where
        S: for<'a> LookupSpan<'a>,
    {
        let process_track_uuid = self.sequence.process_track_uuid();
        if let Some(track_name) = own_track_name {
            return SpanPlacement::new_track(process_track_uuid, track_name);
        }

        for ancestor in span.scope().skip(1) {
            let mut extensions = ancestor.extensions_mut();
            let Some(ancestor_state) = extensions
                .get_mut::<PerfettoSpanStates>()
                .and_then(|states| states.get_mut(self.layer_id))
                .and_then(|state| state.lifetime.as_mut())
            else {
                continue;
            };

            // Nest on the ancestor's track
            if ancestor_state.open_children_on_track == 0 {
                ancestor_state.open_children_on_track = 1;
                return SpanPlacement {
                    track_uuid: ancestor_state.track_uuid,
                    new_track: None,
                    track_parent: Some(ancestor.id()),
                };
            }

            // Overlap a sibling
            return SpanPlacement::new_track(ancestor_state.track_uuid, span.name());
        }

        // Root span
        SpanPlacement::new_track(process_track_uuid, span.name())
    }
}

impl<W> Drop for PerfettoLayer<W> {
    fn drop(&mut self) {
        self.lifecycle.finish();
    }
}

struct ProcessMemorySamplerConfig<W> {
    writer: Arc<W>,
    sequence: Arc<TraceSequence>,
    counter_track_uuid: u64,
    interval: Duration,
    source: Box<dyn process_memory::ProcessMemorySource>,
}

struct ProcessMemorySampler {
    stop: Arc<AtomicBool>,
    join_handle: parking_lot::Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl ProcessMemorySampler {
    fn spawn<W>(config: ProcessMemorySamplerConfig<W>) -> Self
    where
        W: PerfettoWriter + Send + Sync + 'static,
    {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_for_thread = Arc::clone(&stop);

        let join_handle = std::thread::Builder::new()
            .name("perfetto-process-memory".to_string())
            .spawn(move || {
                let mut wrote_descriptor = false;
                while !stop_for_thread.load(Ordering::Relaxed) {
                    if let Some(rss_bytes) = config.source.resident_set_size_bytes() {
                        write_process_memory_rss_sample(&config, rss_bytes, !wrote_descriptor);
                        wrote_descriptor = true;
                    }
                    std::thread::sleep(config.interval);
                }
            })
            .ok();

        Self {
            stop,
            join_handle: parking_lot::Mutex::new(join_handle),
        }
    }

    fn stop(self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.join_handle.lock().take() {
            let _ = handle.join();
        }
    }
}

fn write_process_memory_rss_sample<W: PerfettoWriter>(
    config: &ProcessMemorySamplerConfig<W>,
    rss_bytes: u64,
    include_descriptor: bool,
) {
    let mut packet = config.sequence.timestamped_packet();

    let counter_value = i64::try_from(rss_bytes).unwrap_or(i64::MAX);
    let mut event = idl::TrackEvent {
        track_uuid: Some(config.counter_track_uuid),
        counter_value_field: Some(idl::track_event::CounterValueField::CounterValue(
            counter_value,
        )),
        ..Default::default()
    };
    event.set_type(idl::track_event::Type::Counter);
    packet.data = Some(idl::trace_packet::Data::TrackEvent(event));

    let counter_track_descriptor = include_descriptor.then(|| {
        let mut counter_descriptor = idl::CounterDescriptor::default();
        counter_descriptor.set_unit(idl::counter_descriptor::Unit::SizeBytes);
        idl_helpers::create_track_descriptor(
            Some(config.counter_track_uuid),
            Some(config.sequence.process_track_uuid()),
            Some("process.rss"),
            None,
            None,
            Some(counter_descriptor),
        )
    });

    let mut trace = idl::Trace {
        packet: Vec::with_capacity(2),
    };
    if let Some(counter_track_descriptor) = counter_track_descriptor {
        trace.packet.push(idl::TracePacket {
            data: Some(idl::trace_packet::Data::TrackDescriptor(
                counter_track_descriptor,
            )),
            ..Default::default()
        });
    }
    trace.packet.push(packet);

    config.sequence.write(config.writer.as_ref(), &trace);
}

fn track_descriptor_packet(descriptor: idl::TrackDescriptor) -> idl::TracePacket {
    idl::TracePacket {
        data: Some(idl::trace_packet::Data::TrackDescriptor(descriptor)),
        ..Default::default()
    }
}

struct TrackUuid(u64);

impl TrackUuid {
    fn new(n: u64) -> Self {
        Self(n)
    }

    fn get(&self) -> u64 {
        self.0
    }
}

struct TrackNameVisitor<'a> {
    user_track_name: &'a mut Option<String>,
    slice_name_override: &'a mut Option<String>,
}

impl Visit for TrackNameVisitor<'_> {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "perfetto.track_name" {
            let rendered = format!("{value:?}");
            let rendered = rendered
                .strip_prefix('"')
                .and_then(|s| s.strip_suffix('"'))
                .unwrap_or(rendered.as_str());
            let rendered = rendered.to_string();
            *self.user_track_name = Some(rendered);
            return;
        }

        if field.name() == "perfetto.slice_name" {
            let rendered = format!("{value:?}");
            let rendered = rendered
                .strip_prefix('"')
                .and_then(|s| s.strip_suffix('"'))
                .unwrap_or(rendered.as_str());
            *self.slice_name_override = Some(rendered.to_string());
            return;
        }

        if field.name() == "otel.name" && self.slice_name_override.is_none() {
            let rendered = format!("{value:?}");
            let rendered = rendered
                .strip_prefix('"')
                .and_then(|s| s.strip_suffix('"'))
                .unwrap_or(rendered.as_str());
            *self.slice_name_override = Some(rendered.to_string());
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "perfetto.track_name" {
            *self.user_track_name = Some(value.to_string());
            return;
        }

        if field.name() == "perfetto.slice_name" {
            *self.slice_name_override = Some(value.to_string());
            return;
        }

        if field.name() == "otel.name" && self.slice_name_override.is_none() {
            *self.slice_name_override = Some(value.to_string());
        }
    }
}
struct PerfettoVisitor {
    perfetto: bool,
    filter: fn(&str) -> bool,
}

impl PerfettoVisitor {
    fn new(filter: fn(&str) -> bool) -> PerfettoVisitor {
        Self {
            filter,
            perfetto: false,
        }
    }
}

impl Visit for PerfettoVisitor {
    fn record_debug(&mut self, field: &Field, _value: &dyn std::fmt::Debug) {
        if (self.filter)(field.name()) {
            self.perfetto = true;
        }
    }
}

impl<W, S: Subscriber> Layer<S> for PerfettoLayer<W>
where
    S: for<'a> LookupSpan<'a>,
    W: PerfettoWriter + 'static,
{
    fn on_new_span(&self, attrs: &span::Attributes<'_>, id: &span::Id, ctx: Context<'_, S>) {
        if !self.config.span_lifetimes && !self.config.thread_slices {
            return;
        }
        let Some(span) = ctx.span(id) else {
            return;
        };

        let enabled = self.config.filter.is_none_or(|filter| {
            let mut visitor = PerfettoVisitor::new(filter);
            attrs.record(&mut visitor);
            visitor.perfetto
        });

        if !enabled {
            return;
        }

        let mut debug_annotations = DebugAnnotations::default();
        if self.config.debug_annotations {
            attrs.record(&mut debug_annotations);
        }

        let mut user_track_name = None;
        let mut slice_name_override = None;
        let mut visitor = TrackNameVisitor {
            user_track_name: &mut user_track_name,
            slice_name_override: &mut slice_name_override,
        };
        attrs.record(&mut visitor);

        let slice_name = slice_name_override.map_or(Cow::Borrowed(span.name()), Cow::Owned);

        // Without a lifetime slice to carry them, the creation fields go on every entered slice.
        let creation_annotations = if self.config.span_lifetimes {
            Vec::new()
        } else {
            std::mem::take(&mut debug_annotations.annotations)
        };
        let correlation_id = idl_helpers::unique_uuid();
        let lifetime = self.config.span_lifetimes.then(|| {
            self.begin_lifetime_slice(
                &span,
                user_track_name.as_deref(),
                &slice_name,
                correlation_id,
                debug_annotations,
            )
        });

        let span_state = PerfettoSpanState {
            slice_name,
            correlation_id,
            lifetime,
            creation_annotations,
            recorded_annotations: Vec::new(),
        };
        PerfettoSpanStates::insert(&mut span.extensions_mut(), self.layer_id, span_state);
    }

    fn on_record(&self, span: &span::Id, values: &span::Record<'_>, ctx: Context<'_, S>) {
        if !self.config.debug_annotations {
            return;
        }
        let Some(span) = ctx.span(span) else {
            return;
        };

        // The filter was checked in `on_new_span`, which stores state only for recorded spans.
        // Routing and naming are fixed at creation, so a late `perfetto.track_name` or
        // `perfetto.slice_name` is only an annotation.
        if let Some(states) = span.extensions_mut().get_mut::<PerfettoSpanStates>()
            && let Some(span_state) = states.get_mut(self.layer_id)
        {
            let mut debug_annotations = DebugAnnotations::default();
            values.record(&mut debug_annotations);
            span_state
                .recorded_annotations
                .append(&mut debug_annotations.annotations);
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let enabled = self.config.filter.is_none_or(|filter| {
            let mut visitor = PerfettoVisitor::new(filter);
            event.record(&mut visitor);
            visitor.perfetto
        });

        if !enabled {
            return;
        }

        let metadata = event.metadata();
        let location = metadata.file().zip(metadata.line());

        let mut debug_annotations = DebugAnnotations::default();

        if self.config.debug_annotations {
            event.record(&mut debug_annotations);
        }

        let mut track_event = create_event(
            0,
            Some(metadata.name()),
            location,
            debug_annotations,
            Some(idl::track_event::Type::Instant),
        );

        // An event belongs to its explicit parent or else the current span, and lands on the
        // track of the nearest such span that this layer records.
        let span_track_uuid = ctx.event_span(event).and_then(|event_span| {
            event_span.scope().find_map(|span| {
                span.extensions()
                    .get::<PerfettoSpanStates>()
                    .and_then(|states| states.get(self.layer_id))
                    .and_then(|span_state| span_state.lifetime.as_ref())
                    .map(|lifetime| lifetime.track_uuid)
            })
        });
        let Some(span_track_uuid) = span_track_uuid else {
            self.write_on_thread_track(vec![track_event]);
            return;
        };
        track_event.track_uuid = Some(span_track_uuid);
        self.write_packets(vec![self.event_packet(track_event)]);
    }

    fn on_enter(&self, id: &Id, ctx: Context<'_, S>) {
        if !self.config.thread_slices || self.lifecycle.is_finished() {
            return;
        }
        let Some(span) = ctx.span(id) else {
            return;
        };
        let begin = {
            let extensions = span.extensions();
            let Some(span_state) = extensions
                .get::<PerfettoSpanStates>()
                .and_then(|states| states.get(self.layer_id))
            else {
                return;
            };
            self.entered_slice_begin(span.metadata(), span_state)
        };
        if thread_slices::enter(self.layer_id, id.clone(), &begin) {
            self.write_on_thread_track(vec![begin]);
        }
    }

    fn on_exit(&self, id: &Id, _ctx: Context<'_, S>) {
        if !self.config.thread_slices {
            return;
        }
        // Only the thread's own stack decides what ends, because the exiting span may be closed
        // already or entered on other threads too.
        let events = thread_slices::exit(self.layer_id, id);
        if !events.is_empty() {
            self.write_on_thread_track(events);
        }
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(&id) else {
            return;
        };

        let Some(span_state) = span
            .extensions_mut()
            .get_mut::<PerfettoSpanStates>()
            .and_then(|states| states.remove(self.layer_id))
        else {
            return;
        };
        let Some(lifetime) = span_state.lifetime else {
            return;
        };

        // The end carries no name or location, because trace processor takes them from the begin
        // and merges the end's annotations into the slice.
        let event = create_event(
            lifetime.track_uuid,
            None,
            None,
            DebugAnnotations {
                annotations: span_state.recorded_annotations,
            },
            Some(idl::track_event::Type::SliceEnd),
        );
        self.write_packets(vec![self.event_packet(event)]);

        // Free the parent's track only after writing the end, so a sibling that nests there next
        // begins after it.
        if let Some(track_parent_id) = lifetime.track_parent
            && let Some(track_parent) = ctx.span(&track_parent_id)
            && let Some(states) = track_parent
                .extensions_mut()
                .get_mut::<PerfettoSpanStates>()
            && let Some(track_parent_state) = states
                .get_mut(self.layer_id)
                .and_then(|state| state.lifetime.as_mut())
        {
            track_parent_state.open_children_on_track =
                track_parent_state.open_children_on_track.saturating_sub(1);
        }
    }
}

macro_rules! impl_record {
    ($method:ident, $type:ty, $value_variant:ident) => {
        fn $method(&mut self, field: &Field, value: $type) {
            let mut annotation = idl::DebugAnnotation::default();
            annotation.name_field = Some(idl::debug_annotation::NameField::Name(
                field.name().to_string(),
            ));
            annotation.value = Some(idl::debug_annotation::Value::$value_variant(value.into()));
            self.annotations.push(annotation);
        }
    };
    ($method:ident, $type:ty, $value_variant:ident, $conversion:expr) => {
        fn $method(&mut self, field: &Field, value: $type) {
            let mut annotation = idl::DebugAnnotation::default();
            annotation.name_field = Some(idl::debug_annotation::NameField::Name(
                field.name().to_string(),
            ));
            annotation.value = Some(idl::debug_annotation::Value::$value_variant($conversion(
                value,
            )));
            self.annotations.push(annotation);
        }
    };
}

impl Visit for DebugAnnotations {
    impl_record!(record_bool, bool, BoolValue);
    impl_record!(record_str, &str, StringValue, String::from);
    impl_record!(record_f64, f64, DoubleValue);
    impl_record!(record_i64, i64, IntValue);
    impl_record!(record_i128, i128, StringValue, |v: i128| v.to_string());
    impl_record!(record_u128, u128, StringValue, |v: u128| v.to_string());
    impl_record!(record_u64, u64, IntValue, |v: u64| v.cast_signed());

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        let annotation = idl::DebugAnnotation {
            name_field: Some(idl::debug_annotation::NameField::Name(
                field.name().to_string(),
            )),
            value: Some(idl::debug_annotation::Value::StringValue(format!(
                "{value:?}"
            ))),
            ..Default::default()
        };
        self.annotations.push(annotation);
    }

    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        let annotation = idl::DebugAnnotation {
            name_field: Some(idl::debug_annotation::NameField::Name(
                field.name().to_string(),
            )),
            value: Some(idl::debug_annotation::Value::StringValue(format!(
                "{value}"
            ))),
            ..Default::default()
        };

        self.annotations.push(annotation);
    }
}

#[cfg(test)]
mod tests {
    use crate::PerfettoLayer;
    use crate::idl;
    use crate::idl::track_event;
    use crate::sequence::SEQUENCE_CLOCK_ID;
    use crate::test_support::TestWriter;
    use googletest::prelude::*;
    use prost::Message;
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc;
    use tracing::{field, trace_span};
    use tracing_subscriber::{fmt::MakeWriter, layer::SubscriberExt};

    // Check that we are able to write a span and confirm that it's written as protobuf data to the
    // output
    #[gtest]
    fn test_simple_span() -> Result<()> {
        let writer = TestWriter::new();
        let extra_writer = writer.make_writer();
        let perfetto_layer = PerfettoLayer::new(writer).with_debug_annotations(true);
        let subscriber = tracing_subscriber::registry().with(perfetto_layer);
        let _guard = tracing::subscriber::set_default(subscriber);
        {
            let demo_span = trace_span!("simple_span",);
            let _enter = demo_span.enter();
        }
        let buf = extra_writer.buf.lock();
        expect_that!(buf.as_slice(), not(is_empty()));
        let trace = idl::Trace::decode(buf.as_slice()).or_fail()?;

        let mut track_events_seen = 0;
        let mut saw_slice_begin = false;
        let mut saw_slice_end = false;
        // Depending on test ordering, we may or may not see a process descriptor
        for packet in trace.packet {
            let Some(idl::trace_packet::Data::TrackEvent(ref event)) = packet.data else {
                continue;
            };
            track_events_seen += 1;

            // Each slice takes its name from the begin, so the end carries none.
            match event.r#type() {
                track_event::Type::SliceBegin => {
                    saw_slice_begin = true;
                    let expected = Some(track_event::NameField::Name(String::from("simple_span")));
                    expect_eq!(event.name_field, expected);
                }
                track_event::Type::SliceEnd => {
                    saw_slice_end = true;
                    expect_eq!(event.name_field, None);
                }
                other => return fail!("unexpected track event type: {other:?}"),
            }
        }
        // The lifetime slice and the entered slice each begin and end.
        expect_eq!(track_events_seen, 4);
        expect_true!(saw_slice_begin);
        expect_true!(saw_slice_end);
        Ok(())
    }

    // Check that we are able to write arguments to a span correctly
    #[gtest]
    fn test_span_arguments() -> Result<()> {
        let writer = TestWriter::new();
        let extra_writer = writer.make_writer();
        // Entered slices carry no fields while span lifetimes are on, so only the lifetime
        // slice is recorded.
        let perfetto_layer = PerfettoLayer::new(writer)
            .with_debug_annotations(true)
            .with_thread_slices(false)
            .with_filter_by_marker(|s| s == "regular_arg");

        let subscriber = tracing_subscriber::registry().with(perfetto_layer);
        let _guard = tracing::subscriber::set_default(subscriber);
        {
            let demo_span = trace_span!(
                "span_with_args",
                regular_arg = "Arg data",
                extra_arg = field::Empty
            );
            let _enter = demo_span.enter();
            demo_span.record("extra_arg", "Some Extra Data");
        }
        let buf = extra_writer.buf.lock();
        expect_that!(buf.as_slice(), not(is_empty()));
        let trace = idl::Trace::decode(buf.as_slice()).or_fail()?;

        let mut track_events_seen = 0;
        let mut saw_slice_begin = false;
        let mut saw_slice_end = false;
        // Depending on test ordering, we may or may not see a process descriptor
        for packet in trace.packet {
            let Some(idl::trace_packet::Data::TrackEvent(ref event)) = packet.data else {
                continue;
            };
            track_events_seen += 1;

            match event.r#type() {
                track_event::Type::SliceBegin => {
                    saw_slice_begin = true;
                    let expected =
                        Some(track_event::NameField::Name(String::from("span_with_args")));
                    expect_eq!(event.name_field, expected);

                    // The begin is written when the span is created, so it carries only the
                    // fields given at creation.
                    let annotations = event.debug_annotations.as_slice();
                    let [first] = annotations else {
                        return fail!("expected 1 debug annotation, got {}", annotations.len());
                    };
                    expect_eq!(
                        first.name_field,
                        Some(idl::debug_annotation::NameField::Name(
                            "regular_arg".to_string(),
                        ))
                    );
                    expect_eq!(
                        first.value,
                        Some(idl::debug_annotation::Value::StringValue(
                            "Arg data".to_string(),
                        ))
                    );
                }
                track_event::Type::SliceEnd => {
                    saw_slice_end = true;

                    // The end carries the field recorded later, which trace processor merges
                    // into the slice's arguments.
                    let annotations = event.debug_annotations.as_slice();
                    let [recorded] = annotations else {
                        return fail!("expected 1 debug annotation, got {}", annotations.len());
                    };
                    expect_eq!(
                        recorded.name_field,
                        Some(idl::debug_annotation::NameField::Name(
                            "extra_arg".to_string(),
                        ))
                    );
                    expect_eq!(
                        recorded.value,
                        Some(idl::debug_annotation::Value::StringValue(
                            "Some Extra Data".to_string(),
                        ))
                    );
                }
                other => return fail!("unexpected track event type: {other:?}"),
            }
        }
        expect_eq!(track_events_seen, 2);
        expect_true!(saw_slice_begin);
        expect_true!(saw_slice_end);
        Ok(())
    }

    #[gtest]
    fn test_span_slice_name_override_prefers_perfetto_track_name() -> Result<()> {
        let writer = TestWriter::new();
        let extra_writer = writer.make_writer();
        let perfetto_layer = PerfettoLayer::new(writer).with_debug_annotations(true);
        let subscriber = tracing_subscriber::registry().with(perfetto_layer);
        let _guard = tracing::subscriber::set_default(subscriber);

        {
            let otel_name = String::from("otel-name");
            let demo_span = trace_span!(
                "static_span",
                {"perfetto.track_name"} = "my-task",
                {"otel.name"} = %otel_name
            );
            let _enter = demo_span.enter();
        }

        let buf = extra_writer.buf.lock();
        expect_that!(buf.as_slice(), not(is_empty()));
        let trace = idl::Trace::decode(buf.as_slice()).or_fail()?;

        // Only the begins of the lifetime slice and the entered slice name them.
        let names = crate::decode_track_event_names(&trace.encode_to_vec()).or_fail()?;
        expect_eq!(names, ["otel-name", "otel-name"]);
        Ok(())
    }

    #[gtest]
    fn test_span_slice_name_override_uses_otel_name_when_no_track_name() -> Result<()> {
        let writer = TestWriter::new();
        let extra_writer = writer.make_writer();
        let perfetto_layer = PerfettoLayer::new(writer).with_debug_annotations(true);
        let subscriber = tracing_subscriber::registry().with(perfetto_layer);
        let _guard = tracing::subscriber::set_default(subscriber);

        {
            let otel_name = String::from("my-otel-span");
            let demo_span = trace_span!("static_span", {"otel.name"} = %otel_name);
            let _enter = demo_span.enter();
        }

        let buf = extra_writer.buf.lock();
        expect_that!(buf.as_slice(), not(is_empty()));
        let trace = idl::Trace::decode(buf.as_slice()).or_fail()?;

        // Only the begins of the lifetime slice and the entered slice name them.
        let names = crate::decode_track_event_names(&trace.encode_to_vec()).or_fail()?;
        expect_eq!(names, ["my-otel-span", "my-otel-span"]);
        Ok(())
    }

    // If all our spans are filtered, we shouldn't get any trace data at all.
    // Doing a `.record` on a span should also "fail successfully".
    #[gtest]
    fn test_span_arguments_filtered() {
        let writer = TestWriter::new();
        let extra_writer = writer.make_writer();
        let perfetto_layer = PerfettoLayer::new(writer)
            .with_debug_annotations(true)
            .with_filter_by_marker(|s| s == "NO SUCH ARG");

        let subscriber = tracing_subscriber::registry().with(perfetto_layer);
        let _guard = tracing::subscriber::set_default(subscriber);
        {
            let demo_span = trace_span!(
                "span_with_args",
                regular_arg = "Arg data",
                extra_arg = field::Empty
            );
            let _enter = demo_span.enter();
            demo_span.record("extra_arg", "Some Extra Data");
        }
        let buf = extra_writer.buf.lock();
        expect_eq!(buf.len(), 0);
    }

    struct FixedProcessMemory;

    impl crate::process_memory::ProcessMemorySource for FixedProcessMemory {
        fn resident_set_size_bytes(&self) -> Option<u64> {
            Some(1024)
        }
    }

    /// Finishing through the handle ends the trace while the layer stays installed: the sampler
    /// thread is joined and later spans are not written.
    #[gtest]
    fn finish_handle_ends_the_trace_of_a_live_layer() {
        let writer = TestWriter::new();
        let extra_writer = writer.make_writer();
        let perfetto_layer = PerfettoLayer::new(writer).with_process_memory_source(
            std::time::Duration::from_millis(1),
            Box::new(FixedProcessMemory),
        );
        let finish = perfetto_layer.finish_handle();
        let layer_writer = Arc::clone(&perfetto_layer.writer);
        let subscriber = tracing_subscriber::registry().with(perfetto_layer);
        let _guard = tracing::subscriber::set_default(subscriber);

        trace_span!("before_finish").in_scope(|| {});
        finish.finish();
        let written = extra_writer.buf.lock().len();
        trace_span!("after_finish").in_scope(|| {});

        // The joined sampler released its writer, leaving this test's and the layer's references.
        expect_eq!(Arc::strong_count(&layer_writer), 2);
        // The span recorded before finishing reached the writer; the later one did not.
        expect_that!(
            written,
            gt(0),
            "expected the span before finishing to be written"
        );
        expect_eq!(extra_writer.buf.lock().len(), written);
    }

    /// A memory source that reports samples once armed and announces each sample it reports.
    ///
    /// The sampler writes a sample before it reads the next one, so the second announcement
    /// proves that the first sample reached the writer.
    struct AnnouncingProcessMemory {
        armed: Arc<AtomicBool>,
        announcements: mpsc::Sender<()>,
    }

    impl crate::process_memory::ProcessMemorySource for AnnouncingProcessMemory {
        fn resident_set_size_bytes(&self) -> Option<u64> {
            if !self.armed.load(Ordering::Acquire) {
                return None;
            }
            // The receiver is gone only after the test stopped waiting for samples.
            self.announcements.send(()).ok();
            Some(1024)
        }
    }

    /// Blocks until the sampler has written at least one sample.
    fn wait_for_written_sample(announcements: &mpsc::Receiver<()>) -> Result<()> {
        announcements.recv().or_fail()?;
        announcements.recv().or_fail()
    }

    fn is_process_descriptor(packet: &idl::TracePacket) -> bool {
        matches!(
            &packet.data,
            Some(idl::trace_packet::Data::TrackDescriptor(descriptor))
                if descriptor.process.is_some()
        )
    }

    fn is_clock_snapshot(packet: &idl::TracePacket) -> bool {
        matches!(packet.data, Some(idl::trace_packet::Data::ClockSnapshot(_)))
    }

    fn is_counter_event(packet: &idl::TracePacket) -> bool {
        matches!(
            &packet.data,
            Some(idl::trace_packet::Data::TrackEvent(event))
                if event.r#type() == track_event::Type::Counter
        )
    }

    fn sequence_id(packet: &idl::TracePacket) -> Option<u32> {
        let idl::trace_packet::OptionalTrustedPacketSequenceId::TrustedPacketSequenceId(id) =
            packet.optional_trusted_packet_sequence_id?;
        Some(id)
    }

    /// Checks that `trace` opens with its preamble and never repeats it.
    ///
    /// The preamble is the process track descriptor followed by the clock snapshot that maps the
    /// sequence clock onto the wall clock.
    fn expect_preamble_once(trace: &idl::Trace) -> Result<()> {
        let [descriptor, snapshot, ..] = trace.packet.as_slice() else {
            return fail!("expected a preamble, got {} packets", trace.packet.len());
        };
        expect_true!(is_process_descriptor(descriptor));
        let Some(idl::trace_packet::Data::ClockSnapshot(snapshot_data)) = &snapshot.data else {
            return fail!("expected a clock snapshot after the process descriptor");
        };

        // The snapshot pairs the sequence clock with the wall clock, which is the trace clock.
        let realtime_clock_id = u32::try_from(i32::from(idl::BuiltinClock::Realtime)).or_fail()?;
        let clock_ids: Vec<_> = snapshot_data
            .clocks
            .iter()
            .map(|clock| clock.clock_id)
            .collect();
        expect_eq!(
            clock_ids,
            vec![Some(SEQUENCE_CLOCK_ID), Some(realtime_clock_id)]
        );
        expect_eq!(
            snapshot_data.primary_trace_clock,
            Some(i32::from(idl::BuiltinClock::Realtime))
        );
        // The user-defined clock is scoped to the snapshot's sequence.
        expect_true!(sequence_id(snapshot).is_some());

        // Later writes reuse the preamble instead of repeating it.
        let process_descriptors = trace
            .packet
            .iter()
            .filter(|packet| is_process_descriptor(packet))
            .count();
        let clock_snapshots = trace
            .packet
            .iter()
            .filter(|packet| is_clock_snapshot(packet))
            .count();
        expect_eq!(process_descriptors, 1);
        expect_eq!(clock_snapshots, 1);
        Ok(())
    }

    /// Checks that every timestamped packet reads the sequence clock that the preamble defines.
    fn expect_sequence_clock(trace: &idl::Trace) {
        let clock_ids: Vec<_> = trace
            .packet
            .iter()
            .filter(|packet| packet.timestamp.is_some())
            .map(|packet| packet.timestamp_clock_id)
            .collect();
        expect_that!(clock_ids, not(is_empty()));
        expect_that!(clock_ids, each(eq(&Some(SEQUENCE_CLOCK_ID))));
    }

    /// Checks that every track an event or descriptor refers to is described in `trace` itself.
    fn expect_self_contained(trace: &idl::Trace) {
        let mut described = HashSet::new();
        let mut referenced = HashSet::new();
        for packet in &trace.packet {
            match &packet.data {
                Some(idl::trace_packet::Data::TrackDescriptor(descriptor)) => {
                    described.insert(descriptor.uuid());
                    referenced.extend(descriptor.parent_uuid);
                }
                Some(idl::trace_packet::Data::TrackEvent(event)) => {
                    referenced.extend(event.track_uuid);
                }
                _ => {}
            }
        }
        expect_that!(referenced, not(is_empty()));
        let undescribed: Vec<_> = referenced.difference(&described).collect();
        expect_that!(undescribed, is_empty());
    }

    /// Returns the calling thread's kernel ID from the first field of its procfs `stat` file.
    ///
    /// This source is independent of the `/proc/thread-self` link that the layer reads.
    #[cfg(target_os = "linux")]
    fn procfs_kernel_thread_id() -> Result<i32> {
        let stat = std::fs::read_to_string("/proc/thread-self/stat").or_fail()?;
        stat.split_whitespace().next().or_fail()?.parse().or_fail()
    }

    /// Runs `record` under `dispatch` and returns the calling thread's kernel ID.
    ///
    /// The ID is `None` on platforms where the test has no source for the kernel's thread ID.
    fn record_on_current_thread(
        dispatch: &tracing::Dispatch,
        record: impl FnOnce(),
    ) -> Result<Option<i32>> {
        tracing::dispatcher::with_default(dispatch, record);
        #[cfg(target_os = "linux")]
        let kernel_thread_id = Some(procfs_kernel_thread_id()?);
        // `thread_id::get` is `pthread_threadid_np` on macOS and `GetCurrentThreadId` on Windows.
        #[cfg(any(target_os = "macos", target_os = "windows"))]
        let kernel_thread_id = Some(i32::try_from(thread_id::get()).or_fail()?);
        #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
        let kernel_thread_id = None::<i32>;
        Ok(kernel_thread_id)
    }

    fn new_announcing_layer(
        writer: TestWriter,
        armed: bool,
    ) -> (
        PerfettoLayer<TestWriter>,
        Arc<AtomicBool>,
        mpsc::Receiver<()>,
    ) {
        let armed = Arc::new(AtomicBool::new(armed));
        let (announcements_sender, announcements) = mpsc::channel();
        let layer = PerfettoLayer::new(writer).with_process_memory_source(
            std::time::Duration::ZERO,
            Box::new(AnnouncingProcessMemory {
                armed: Arc::clone(&armed),
                announcements: announcements_sender,
            }),
        );
        (layer, armed, announcements)
    }

    /// Thread descriptors carry each thread's own OS thread ID.
    ///
    /// On Linux, `thread_id::get` is a `pthread_t` address, which clamped to `i32` gives every
    /// thread the same ID and merges all thread tracks of the process into one.
    #[gtest]
    fn thread_descriptors_carry_the_os_thread_id_of_each_thread() -> Result<()> {
        let writer = TestWriter::new();
        let extra_writer = writer.make_writer();
        let dispatch =
            tracing::Dispatch::new(tracing_subscriber::registry().with(PerfettoLayer::new(writer)));

        // Record one unscoped event on the test thread, then one on a worker thread.
        let main_kernel_thread_id = record_on_current_thread(&dispatch, || {
            tracing::info!("on the main thread");
        })?;
        let worker_kernel_thread_id = std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    record_on_current_thread(&dispatch, || {
                        tracing::info!("on the worker thread");
                    })
                })
                .join()
        })
        .or_fail()??;

        // Map each event, in recording order, to the thread ID of the track it is on.
        let trace = idl::Trace::decode(extra_writer.buf.lock().as_slice()).or_fail()?;
        let thread_ids: HashMap<u64, Option<i32>> = trace
            .packet
            .iter()
            .filter_map(|packet| match &packet.data {
                Some(idl::trace_packet::Data::TrackDescriptor(descriptor)) => descriptor
                    .thread
                    .as_ref()
                    .map(|thread| (descriptor.uuid(), thread.tid)),
                _ => None,
            })
            .collect();
        let event_thread_ids: Vec<Option<i32>> = trace
            .packet
            .iter()
            .filter_map(|packet| match &packet.data {
                Some(idl::trace_packet::Data::TrackEvent(event)) => {
                    Some(thread_ids.get(&event.track_uuid()).copied().flatten())
                }
                _ => None,
            })
            .collect();
        let [Some(main_thread_id), Some(worker_thread_id)] = event_thread_ids[..] else {
            return fail!("expected two events on thread tracks, got {event_thread_ids:?}");
        };

        // Each thread track names its own, valid thread.
        expect_ne!(main_thread_id, worker_thread_id);
        expect_that!(main_thread_id, gt(0));
        expect_that!(worker_thread_id, gt(0));

        // Where the kernel's ID is known, the descriptors carry exactly that ID, so the tracks
        // line up with other tools' view of the threads.
        if let (Some(main_kernel_thread_id), Some(worker_kernel_thread_id)) =
            (main_kernel_thread_id, worker_kernel_thread_id)
        {
            expect_eq!(main_thread_id, main_kernel_thread_id);
            expect_eq!(worker_thread_id, worker_kernel_thread_id);
        }
        Ok(())
    }

    /// Spans, unscoped events and memory samples of one layer all share one valid sequence ID.
    ///
    /// A random `u64` clamped to `u32` is `u32::MAX` for all but a 2^-32 share of layers, so
    /// every layer would share that one sequence and its sequence-scoped clock.
    #[gtest]
    fn packets_of_one_layer_share_one_valid_sequence_id() -> Result<()> {
        let writer = TestWriter::new();
        let extra_writer = writer.make_writer();
        let (perfetto_layer, _, announcements) = new_announcing_layer(writer, true);
        let finish = perfetto_layer.finish_handle();
        let subscriber = tracing_subscriber::registry().with(perfetto_layer);
        let _guard = tracing::subscriber::set_default(subscriber);

        trace_span!("span").in_scope(|| tracing::info!("inside the span"));
        tracing::info!("outside any span");
        wait_for_written_sample(&announcements)?;
        finish.finish();

        let trace = idl::Trace::decode(extra_writer.buf.lock().as_slice()).or_fail()?;

        // Every timestamped packet, the memory samples included, names a sequence.
        let timestamped_sequence_ids: Vec<_> = trace
            .packet
            .iter()
            .filter(|packet| packet.timestamp.is_some())
            .map(sequence_id)
            .collect();
        expect_that!(timestamped_sequence_ids, each(some(anything())));
        expect_true!(trace.packet.iter().any(is_counter_event));

        // All packets name the same sequence, which is not invalid, not the tracing service's
        // own, and not a clamped value.
        let sequence_ids: HashSet<u32> = trace.packet.iter().filter_map(sequence_id).collect();
        let [only_sequence_id] = sequence_ids.iter().copied().collect::<Vec<_>>()[..] else {
            return fail!("expected one sequence ID, got {sequence_ids:?}");
        };
        expect_that!(only_sequence_id, all!(gt(1), lt(u32::MAX)));
        Ok(())
    }

    /// Two layers on one registry each record the same spans into their own complete trace.
    ///
    /// Span extensions hold one value per type, so a layer that stored its state as its own
    /// extension type would panic when the second layer inserted the same type.
    #[gtest]
    fn two_layers_on_one_registry_each_write_a_complete_trace() -> Result<()> {
        let first_writer = TestWriter::new();
        let first_buf = first_writer.make_writer();
        let second_writer = TestWriter::new();
        let second_buf = second_writer.make_writer();
        let subscriber = tracing_subscriber::registry()
            .with(PerfettoLayer::new(first_writer).with_debug_annotations(true))
            .with(PerfettoLayer::new(second_writer));
        let _guard = tracing::subscriber::set_default(subscriber);

        // A named parent makes the child read its track from the parent's per-layer state, and
        // the recorded field updates only the recording layer's state.
        trace_span!(
            "parent",
            { "perfetto.track_name" } = "jobs",
            value = field::Empty
        )
        .in_scope(|| {
            tracing::Span::current().record("value", 1);
            trace_span!("child").in_scope(|| tracing::info!("inside the child"));
        });

        let first = idl::Trace::decode(first_buf.buf.lock().as_slice()).or_fail()?;
        let second = idl::Trace::decode(second_buf.buf.lock().as_slice()).or_fail()?;
        for trace in [&first, &second] {
            expect_preamble_once(trace)?;
            expect_self_contained(trace);

            // Both spans begin a lifetime and an entered slice, and the event inside the child is
            // recorded once.
            let names = crate::decode_track_event_names(&trace.encode_to_vec()).or_fail()?;
            let (event_names, mut span_names): (Vec<_>, Vec<_>) = names
                .into_iter()
                .partition(|name| name.starts_with("event "));
            span_names.sort();
            expect_eq!(span_names, ["child", "child", "parent", "parent"]);
            expect_eq!(event_names.len(), 1);
        }

        // Each layer writes on its own sequence.
        let first_ids: HashSet<u32> = first.packet.iter().filter_map(sequence_id).collect();
        let second_ids: HashSet<u32> = second.packet.iter().filter_map(sequence_id).collect();
        expect_that!(first_ids.intersection(&second_ids).count(), eq(0));
        Ok(())
    }

    /// Every timestamped packet reads the sequence clock that one earlier snapshot maps onto the
    /// wall clock.
    ///
    /// A packet without a clock ID is read as boot time, so elapsed nanoseconds since the layer's
    /// epoch would land decades before the snapshot's wall-clock time.
    #[gtest]
    fn timestamped_packets_read_the_clock_of_one_earlier_snapshot() -> Result<()> {
        let writer = TestWriter::new();
        let extra_writer = writer.make_writer();
        let (perfetto_layer, _, announcements) = new_announcing_layer(writer, true);
        let finish = perfetto_layer.finish_handle();
        let subscriber = tracing_subscriber::registry().with(perfetto_layer);
        let _guard = tracing::subscriber::set_default(subscriber);

        // Cover every write path: span begin and end, span event, unscoped event and memory
        // sample.
        trace_span!("span").in_scope(|| tracing::info!("inside the span"));
        tracing::info!("outside any span");
        wait_for_written_sample(&announcements)?;
        finish.finish();

        let trace = idl::Trace::decode(extra_writer.buf.lock().as_slice()).or_fail()?;

        // Exactly one snapshot defines the clock, and it precedes every packet that reads it.
        let snapshot_positions: Vec<usize> = trace
            .packet
            .iter()
            .enumerate()
            .filter(|(_, packet)| is_clock_snapshot(packet))
            .map(|(position, _)| position)
            .collect();
        let [snapshot_position] = snapshot_positions[..] else {
            return fail!("expected one clock snapshot, got {snapshot_positions:?}");
        };
        let first_timestamped_position = trace
            .packet
            .iter()
            .position(|packet| packet.timestamp.is_some())
            .or_fail()?;
        expect_that!(snapshot_position, lt(first_timestamped_position));

        // The snapshot pairs the sequence clock with the wall clock on the packets' sequence.
        let snapshot = trace.packet.get(snapshot_position).or_fail()?;
        let Some(idl::trace_packet::Data::ClockSnapshot(snapshot_data)) = &snapshot.data else {
            return fail!("expected a clock snapshot at {snapshot_position}");
        };
        let realtime_clock_id = u32::try_from(i32::from(idl::BuiltinClock::Realtime)).or_fail()?;
        let clock_ids: Vec<_> = snapshot_data
            .clocks
            .iter()
            .map(|clock| clock.clock_id)
            .collect();
        expect_eq!(
            clock_ids,
            vec![Some(SEQUENCE_CLOCK_ID), Some(realtime_clock_id)]
        );
        expect_that!(
            snapshot_data
                .clocks
                .iter()
                .map(|clock| clock.timestamp)
                .collect::<Vec<_>>(),
            each(some(gt(&0)))
        );
        expect_eq!(
            snapshot_data.primary_trace_clock,
            Some(i32::from(idl::BuiltinClock::Realtime))
        );
        let first_timestamped = trace.packet.get(first_timestamped_position).or_fail()?;
        expect_eq!(sequence_id(snapshot), sequence_id(first_timestamped));

        // Every timestamped packet, span, event and sample alike, reads that clock.
        expect_sequence_clock(&trace);
        expect_true!(trace.packet.iter().any(is_counter_event));
        Ok(())
    }

    /// Sequentially recorded spans and events are written with non-decreasing timestamps.
    ///
    /// The timestamps come from a monotonic clock, so a wall-clock adjustment cannot reorder
    /// slices.
    #[gtest]
    fn sequential_packets_have_non_decreasing_timestamps() -> Result<()> {
        let writer = TestWriter::new();
        let extra_writer = writer.make_writer();
        let subscriber = tracing_subscriber::registry().with(PerfettoLayer::new(writer));
        let _guard = tracing::subscriber::set_default(subscriber);

        // Packets are written as they happen, so nested spans are written in time order too.
        for _ in 0..3 {
            trace_span!("span").in_scope(|| {
                tracing::info!("inside the span");
                trace_span!("child").in_scope(|| tracing::info!("inside the child"));
            });
            tracing::info!("outside any span");
        }

        let trace = idl::Trace::decode(extra_writer.buf.lock().as_slice()).or_fail()?;
        expect_sequence_clock(&trace);
        let timestamps: Vec<u64> = trace
            .packet
            .iter()
            .filter_map(|packet| packet.timestamp)
            .collect();
        let mut sorted_timestamps = timestamps.clone();
        sorted_timestamps.sort_unstable();
        // Each iteration writes two lifetime and two entered slices, each with a begin and an
        // end, and two instants, then another instant.
        expect_eq!(timestamps.len(), 33);
        expect_eq!(timestamps, sorted_timestamps);
        Ok(())
    }

    /// The process descriptor and clock snapshot open the trace and appear once across writes.
    ///
    /// Repeating the process descriptor on every write would duplicate it once per span and
    /// event.
    #[gtest]
    fn preamble_is_written_once_across_many_writes() -> Result<()> {
        let writer = TestWriter::new();
        let extra_writer = writer.make_writer();
        let subscriber = tracing_subscriber::registry().with(PerfettoLayer::new(writer));
        let _guard = tracing::subscriber::set_default(subscriber);

        // Each slice begin, slice end and unscoped event is a separate write.
        for _ in 0..3 {
            trace_span!("span").in_scope(|| {});
            tracing::info!("outside any span");
        }

        let trace = idl::Trace::decode(extra_writer.buf.lock().as_slice()).or_fail()?;
        expect_preamble_once(&trace)?;
        let slice_begins = trace
            .packet
            .iter()
            .filter(|packet| {
                matches!(
                    &packet.data,
                    Some(idl::trace_packet::Data::TrackEvent(event))
                        if event.r#type() == track_event::Type::SliceBegin
                )
            })
            .count();
        // Each span begins a lifetime and an entered slice.
        expect_eq!(slice_begins, 6);
        Ok(())
    }

    /// The preamble precedes everything when the memory sampler writes before any span closes.
    ///
    /// A sampler that wrote its samples without the preamble would put them ahead of the clock
    /// snapshot that defines their clock.
    #[gtest]
    fn preamble_precedes_a_memory_sample_written_first() -> Result<()> {
        let writer = TestWriter::new();
        let extra_writer = writer.make_writer();
        let (perfetto_layer, _, announcements) = new_announcing_layer(writer, true);
        let finish = perfetto_layer.finish_handle();
        let subscriber = tracing_subscriber::registry().with(perfetto_layer);
        let _guard = tracing::subscriber::set_default(subscriber);

        // The sampler writes first, then a span and an unscoped event follow.
        wait_for_written_sample(&announcements)?;
        trace_span!("span").in_scope(|| {});
        tracing::info!("outside any span");
        finish.finish();

        let trace = idl::Trace::decode(extra_writer.buf.lock().as_slice()).or_fail()?;
        expect_preamble_once(&trace)?;
        expect_sequence_clock(&trace);
        // The sample's counter track descriptor is the first packet after the preamble.
        expect_true!(trace.packet.get(2).is_some_and(|packet| matches!(
            &packet.data,
            Some(idl::trace_packet::Data::TrackDescriptor(descriptor))
                if descriptor.counter.is_some()
        )));
        Ok(())
    }

    /// The preamble precedes everything when a span is written before the memory sampler writes.
    ///
    /// A preamble written only by the sampler would follow the span's packets.
    #[gtest]
    fn preamble_precedes_a_span_written_first() -> Result<()> {
        let writer = TestWriter::new();
        let extra_writer = writer.make_writer();
        let (perfetto_layer, armed, announcements) = new_announcing_layer(writer, false);
        let finish = perfetto_layer.finish_handle();
        let subscriber = tracing_subscriber::registry().with(perfetto_layer);
        let _guard = tracing::subscriber::set_default(subscriber);

        // The disarmed sampler skips its intervals until the span has been written.
        trace_span!("span").in_scope(|| {});
        armed.store(true, Ordering::Release);
        wait_for_written_sample(&announcements)?;
        finish.finish();

        let trace = idl::Trace::decode(extra_writer.buf.lock().as_slice()).or_fail()?;
        expect_preamble_once(&trace)?;
        expect_sequence_clock(&trace);
        expect_true!(trace.packet.iter().any(is_counter_event));
        Ok(())
    }

    /// First writes racing on several threads and the sampler all land after the preamble.
    ///
    /// Concurrent first writers wait for the one writing the preamble, so none of them can
    /// write a packet between the preamble's start and its end.
    #[gtest]
    fn concurrent_first_writes_all_follow_the_preamble() -> Result<()> {
        const THREADS: usize = 8;
        let writer = TestWriter::new();
        let extra_writer = writer.make_writer();
        let (perfetto_layer, armed, announcements) = new_announcing_layer(writer, false);
        let finish = perfetto_layer.finish_handle();
        let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(perfetto_layer));

        // Release every thread's first span and the sampler's first sample together.
        let start = std::sync::Barrier::new(THREADS + 1);
        std::thread::scope(|scope| {
            for _ in 0..THREADS {
                scope.spawn(|| {
                    tracing::dispatcher::with_default(&dispatch, || {
                        start.wait();
                        trace_span!("racing_span").in_scope(|| {});
                    });
                });
            }
            start.wait();
            armed.store(true, Ordering::Release);
        });
        wait_for_written_sample(&announcements)?;
        finish.finish();

        let trace = idl::Trace::decode(extra_writer.buf.lock().as_slice()).or_fail()?;
        expect_preamble_once(&trace)?;
        expect_sequence_clock(&trace);
        // Each span begins a lifetime and an entered slice.
        let names = crate::decode_track_event_names(&trace.encode_to_vec()).or_fail()?;
        expect_eq!(names.len(), 2 * THREADS);
        Ok(())
    }
}
