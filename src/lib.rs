//! A [`tracing_subscriber::Layer`] that writes spans and events as a [Perfetto] trace.
//!
//! [`PerfettoLayer`] records each span as a slice and each event as an instant, and writes them as
//! encoded `Trace` protobuf packets to a [`PerfettoWriter`].
//! Open the output in the [Perfetto UI].
//!
//! Span fields control where a slice appears:
//!
//! - `perfetto.track_name` moves the span and its children from the thread's track to a named
//!   track.
//! - `perfetto.slice_name` overrides the slice name, which otherwise falls back to `otel.name` and
//!   then to the span's name.
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
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tracing::Event;
use tracing::Id;
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

pub mod process_memory;

struct PerfettoSpanState {
    track_descriptor: Option<idl::TrackDescriptor>, // optional track descriptor for this span, defaults to thread if not found
    slice_name: Option<String>,
    trace: idl::Trace, // The Protobuf trace messages that we accumulate for this span.
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
#[derive(Clone, Copy, PartialEq, Eq)]
struct LayerId(u64);

impl LayerId {
    fn next() -> Self {
        static NEXT_LAYER_ID: AtomicU64 = AtomicU64::new(0);
        Self(NEXT_LAYER_ID.fetch_add(1, Ordering::Relaxed))
    }
}

/// A `Layer` that records span as perfetto's
/// `TYPE_SLICE_BEGIN`/`TYPE_SLICE_END`, and event as `TYPE_INSTANT`.
///
/// `PerfettoLayer` will output the records as encoded [protobuf messages](https://github.com/google/perfetto).
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

#[derive(Default)]
struct Config {
    debug_annotations: bool,
    filter: Option<fn(&str) -> bool>,
}

impl<W: PerfettoWriter> PerfettoLayer<W> {
    /// Creates a layer that writes encoded trace packets to `writer`.
    ///
    /// Debug annotations and the field-name filter start disabled, and no process memory is
    /// sampled.
    #[must_use]
    pub fn new(writer: W) -> Self {
        Self {
            layer_id: LayerId::next(),
            sequence: Arc::new(TraceSequence::new()),
            writer: Arc::new(writer),
            config: Config::default(),

            process_memory_rss_track_uuid: TrackUuid::new(rand::random()),
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

    fn write_log(&self, log: idl::Trace, track_descriptor: Option<idl::TrackDescriptor>) {
        if self.lifecycle.is_finished() {
            return;
        }
        write_trace(&self.sequence, self.writer.as_ref(), log, track_descriptor);
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

fn write_trace<W: PerfettoWriter>(
    sequence: &TraceSequence,
    writer: &W,
    mut trace: idl::Trace,
    track_descriptor: Option<idl::TrackDescriptor>,
) {
    if let Some(track_descriptor) = track_descriptor {
        trace.packet.insert(
            0,
            idl::TracePacket {
                data: Some(idl::trace_packet::Data::TrackDescriptor(track_descriptor)),
                ..Default::default()
            },
        );
    }

    sequence.write(writer, &trace);
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

        let mut packet = self.sequence.timestamped_packet();

        // check if parent span has a non default track descriptor
        let inherited_track_descriptor = span
            .parent()
            // If the span has a parent, try retrieving the track descriptor from the parent's state
            .and_then(|parent_span| {
                parent_span
                    .extensions()
                    .get::<PerfettoSpanStates>()
                    .and_then(|states| states.get(self.layer_id))
                    .map(|state| state.track_descriptor.clone())
            })
            .flatten();

        let mut user_track_name = None;
        let mut slice_name_override = None;
        let mut visitor = TrackNameVisitor {
            user_track_name: &mut user_track_name,
            slice_name_override: &mut slice_name_override,
        };
        attrs.record(&mut visitor);

        let slice_name = slice_name_override
            .as_deref()
            .unwrap_or_else(|| span.name());

        // Resolve the optional track descriptor for this span (either inherited from parent, user set, or None).
        // For named tracks, reuse a stable UUID per track name so the Perfetto UI gets a single lane.
        let span_track_descriptor = user_track_name
            .as_deref()
            .map(|name| idl::TrackDescriptor {
                static_or_dynamic_name: Some(idl::track_descriptor::StaticOrDynamicName::Name(
                    name.to_string(),
                )),
                ..idl::TrackDescriptor::named_child_for(name, self.sequence.process_track_uuid())
            })
            .or(inherited_track_descriptor);

        let final_uuid = span_track_descriptor
            .as_ref()
            .map_or_else(current_thread_uuid, idl::TrackDescriptor::uuid);

        let event = create_event(
            final_uuid, // span track id if exists, otherwise thread track id
            Some(slice_name),
            span.metadata().file().zip(span.metadata().line()),
            debug_annotations,
            Some(idl::track_event::Type::SliceBegin),
        );
        packet.data = Some(idl::trace_packet::Data::TrackEvent(event));

        let span_state = PerfettoSpanState {
            track_descriptor: span_track_descriptor,
            slice_name: if slice_name == span.name() {
                None
            } else {
                Some(slice_name.to_string())
            },
            trace: idl::Trace {
                packet: vec![packet],
            },
        };
        PerfettoSpanStates::insert(&mut span.extensions_mut(), self.layer_id, span_state);
    }

    fn on_record(&self, span: &span::Id, values: &span::Record<'_>, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(span) else {
            return;
        };

        // We don't check the filter here -- we've already checked it when we handled the span on
        // `on_new_span`. Iff we successfully attached a track packet to the span, then we'll also
        // update the trace packet with the debug data here.
        if let Some(states) = span.extensions_mut().get_mut::<PerfettoSpanStates>()
            && let Some(extension) = states.get_mut(self.layer_id)
            && let Some(packet) = extension.trace.packet.get_mut(0)
            && let Some(idl::trace_packet::Data::TrackEvent(event)) = &mut packet.data
        {
            let mut debug_annotations = DebugAnnotations::default();
            values.record(&mut debug_annotations);
            event
                .debug_annotations
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

        let thread_track_uuid = current_thread_uuid();
        let mut track_event = create_event(
            0,
            Some(metadata.name()),
            location,
            debug_annotations,
            Some(idl::track_event::Type::Instant),
        );

        let mut packet = self.sequence.timestamped_packet();

        if let Some(span) = ctx.event_span(event)
            && let Some(states) = span.extensions_mut().get_mut::<PerfettoSpanStates>()
            && let Some(span_state) = states.get_mut(self.layer_id)
        {
            track_event.track_uuid = span_state
                .track_descriptor
                .as_ref()
                .map(idl::TrackDescriptor::uuid)
                .or(Some(current_thread_uuid()));
            packet.data = Some(idl::trace_packet::Data::TrackEvent(track_event));
            span_state.trace.packet.push(packet);
            return;
        }

        // no span or no span state, just write the event
        track_event.track_uuid = Some(thread_track_uuid);
        packet.data = Some(idl::trace_packet::Data::TrackEvent(track_event));
        let trace = idl::Trace {
            packet: vec![packet],
        };
        self.write_log(trace, Some(idl_helpers::current_thread_track_descriptor()));
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(&id) else {
            return;
        };

        let Some(mut span_state) = span
            .extensions_mut()
            .get_mut::<PerfettoSpanStates>()
            .and_then(|states| states.remove(self.layer_id))
        else {
            return;
        };

        let debug_annotations = DebugAnnotations::default();

        let track_uuid = span_state
            .track_descriptor
            .as_ref()
            .map_or_else(current_thread_uuid, idl::TrackDescriptor::uuid);

        let mut packet = self.sequence.timestamped_packet();
        let meta = span.metadata();
        let slice_name = span_state
            .slice_name
            .as_deref()
            .unwrap_or_else(|| meta.name());

        let event = create_event(
            track_uuid,
            Some(slice_name),
            meta.file().zip(meta.line()),
            debug_annotations,
            Some(idl::track_event::Type::SliceEnd),
        );
        packet.data = Some(idl::trace_packet::Data::TrackEvent(event));

        span_state.trace.packet.push(packet);

        self.write_log(
            span_state.trace,
            Some(
                span_state
                    .track_descriptor
                    .unwrap_or_else(idl_helpers::current_thread_track_descriptor),
            ),
        );
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
    use googletest::prelude::*;
    use parking_lot::Mutex;
    use prost::Message;
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc;
    use tracing::{field, trace_span};
    use tracing_subscriber::{fmt::MakeWriter, layer::SubscriberExt};

    /// A Sink for testing that can be passed to `PerfettoLayer::new` to write trace data to. The
    /// sink just accumulates the trace data into a buffer in memory. The data will be
    /// `idl::Trace` protobufs which can be `.decode`'ed.
    struct TestWriter {
        buf: Arc<Mutex<Vec<u8>>>,
    }

    impl TestWriter {
        fn new() -> Self {
            Self {
                buf: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    impl<'a> MakeWriter<'a> for TestWriter {
        type Writer = TestWriter;
        fn make_writer(&'a self) -> Self::Writer {
            TestWriter {
                buf: self.buf.clone(),
            }
        }
    }

    impl std::io::Write for TestWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            let mut guard = self.buf.lock();
            guard.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            // There's nothing to flush, we always immediately append to the buffer.
            std::io::Result::Ok(())
        }
    }

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
            let expected = Some(track_event::NameField::Name(String::from("simple_span")));
            expect_eq!(event.name_field, expected);

            match event.r#type() {
                track_event::Type::SliceBegin => saw_slice_begin = true,
                track_event::Type::SliceEnd => saw_slice_end = true,
                other => return fail!("unexpected track event type: {other:?}"),
            }
        }
        expect_eq!(track_events_seen, 2);
        expect_true!(saw_slice_begin);
        expect_true!(saw_slice_end);
        Ok(())
    }

    // Check that we are able to write arguments to a span correctly
    #[gtest]
    fn test_span_arguments() -> Result<()> {
        let writer = TestWriter::new();
        let extra_writer = writer.make_writer();
        let perfetto_layer = PerfettoLayer::new(writer)
            .with_debug_annotations(true)
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
            let expected = Some(track_event::NameField::Name(String::from("span_with_args")));
            expect_eq!(event.name_field, expected);

            match event.r#type() {
                track_event::Type::SliceBegin => {
                    saw_slice_begin = true;

                    // The SliceBegin isn't recorded until it's dropped, so both the args are added to the
                    // SliceBegin record.
                    let annotations = event.debug_annotations.as_slice();
                    let [first, second] = annotations else {
                        return fail!("expected 2 debug annotations, got {}", annotations.len());
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
                    expect_eq!(
                        second.name_field,
                        Some(idl::debug_annotation::NameField::Name(
                            "extra_arg".to_string(),
                        ))
                    );
                    expect_eq!(
                        second.value,
                        Some(idl::debug_annotation::Value::StringValue(
                            "Some Extra Data".to_string(),
                        ))
                    );
                }
                track_event::Type::SliceEnd => {
                    saw_slice_end = true;
                    // The SliceEnd won't have any arguments
                    expect_eq!(event.debug_annotations.len(), 0);
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

        let mut track_events_seen = 0;
        for packet in trace.packet {
            let Some(idl::trace_packet::Data::TrackEvent(ref event)) = packet.data else {
                continue;
            };
            track_events_seen += 1;

            let expected = Some(track_event::NameField::Name(String::from("otel-name")));
            expect_eq!(event.name_field, expected);
        }
        expect_eq!(track_events_seen, 2);
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

        let mut track_events_seen = 0;
        for packet in trace.packet {
            let Some(idl::trace_packet::Data::TrackEvent(ref event)) = packet.data else {
                continue;
            };
            track_events_seen += 1;

            let expected = Some(track_event::NameField::Name(String::from("my-otel-span")));
            expect_eq!(event.name_field, expected);
        }
        expect_eq!(track_events_seen, 2);
        Ok(())
    }

    // If all our spans are filtered, we shouldn't get any trace data at all. Doing a `.record` on
    // a span should also "fail successfully".
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
            Some(idl::trace_packet::Data::TrackDescriptor(descriptor)) if descriptor.process.is_some()
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

        // Record one span on the test thread and one on a worker thread.
        let main_kernel_thread_id = record_on_current_thread(&dispatch, || {
            trace_span!("main_thread_span").in_scope(|| {});
        })?;
        let worker_kernel_thread_id = std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    record_on_current_thread(&dispatch, || {
                        trace_span!("worker_thread_span").in_scope(|| {});
                    })
                })
                .join()
        })
        .or_fail()??;

        // Map each span to the thread ID of the track its slice begins on.
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
        let span_thread_id = |span_name: &str| {
            trace.packet.iter().find_map(|packet| match &packet.data {
                Some(idl::trace_packet::Data::TrackEvent(event))
                    if event.name_field
                        == Some(track_event::NameField::Name(span_name.to_string())) =>
                {
                    thread_ids.get(&event.track_uuid()).copied().flatten()
                }
                _ => None,
            })
        };
        let main_thread_id = span_thread_id("main_thread_span").or_fail()?;
        let worker_thread_id = span_thread_id("worker_thread_span").or_fail()?;

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
        // the recorded field and the event each update only the recording layer's state.
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

            // Both spans begin and end, and the event inside the child is recorded once.
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

        // Cover every write path: span close, unscoped event and memory sample.
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

        // Spans are buffered until they close, so only unnested spans are written in time order.
        for _ in 0..3 {
            trace_span!("span").in_scope(|| tracing::info!("inside the span"));
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
        // Each iteration writes a slice begin, an instant and a slice end, then another instant.
        expect_eq!(timestamps.len(), 12);
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

        // Each closed span and each unscoped event is a separate write.
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
        expect_eq!(slice_begins, 3);
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

    /// The preamble precedes everything when a span closes before the memory sampler writes.
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

        // Release every thread's first span close and the sampler's first sample together.
        let start = std::sync::Barrier::new(THREADS + 1);
        std::thread::scope(|scope| {
            for _ in 0..THREADS {
                scope.spawn(|| {
                    tracing::dispatcher::with_default(&dispatch, || {
                        let span = trace_span!("racing_span");
                        start.wait();
                        drop(span);
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
        let names = crate::decode_track_event_names(&trace.encode_to_vec()).or_fail()?;
        expect_eq!(names.len(), 2 * THREADS);
        Ok(())
    }
}
