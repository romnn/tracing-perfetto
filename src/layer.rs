//! The [`PerfettoLayer`], its builder methods, and the tracing callbacks that write a span or
//! event as trace packets when the layer observes it.

use crate::fields::{self, DebugAnnotations, NamingFields};
use crate::lifecycle::{FinishHandle, TraceLifecycle};
use crate::packets::{self, create_event};
use crate::process_memory::sampler::{ProcessMemorySampler, ProcessMemorySamplerConfig};
use crate::process_memory::{self, ProcessMemorySource};
use crate::sequence::TraceSequence;
use crate::span_state::{self, LifetimeSlice, SpanPlacement, SpanState, SpanStates};
use crate::writer::PerfettoWriter;
use crate::{idl, thread_slices, thread_track};
use std::borrow::Cow;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tracing::field::Visit;
use tracing::span;
use tracing::{Event, Id, Metadata, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::{LookupSpan, SpanRef};

/// Identifies one [`PerfettoLayer`] among all layers in the process.
///
/// IDs are never reused, so state keyed by a dropped layer's ID never applies to a later layer.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct LayerId(u64);

impl LayerId {
    /// Returns an ID that no other layer in the process has.
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
///
/// Dropping the layer, or finishing it through a [`FinishHandle`], ends the trace.
pub struct PerfettoLayer<W = fn() -> std::io::Stdout> {
    layer_id: LayerId,
    sequence: Arc<TraceSequence>,
    writer: Arc<W>,
    config: Config,
    /// The counter track of the process-memory samples, which every sampler of the layer reuses.
    process_memory_rss_track_uuid: u64,
    lifecycle: Arc<TraceLifecycle>,
}

/// What a [`PerfettoLayer`] records, as its builder methods set it.
struct Config {
    debug_annotations: bool,
    span_lifetimes: bool,
    thread_slices: bool,
    /// The marker filter, which records only spans and events with a field it accepts.
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
            process_memory_rss_track_uuid: packets::unique_uuid(),
            lifecycle: Arc::default(),
        }
    }

    /// Returns a handle that ends this layer's trace without dropping the layer.
    #[must_use]
    pub fn finish_handle(&self) -> FinishHandle {
        self.lifecycle.finish_handle()
    }

    /// Configures whether spans and events carry their fields as debug annotations.
    ///
    /// Disabled by default.
    /// When enabled, an event's fields are annotations on its instant.
    /// A span's creation fields are annotations on its lifetime slice's begin, and fields
    /// recorded later are annotations on its end.
    /// Without span lifetimes, each entered slice's begin carries the fields given and recorded
    /// before the entry.
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

    /// Records only the spans and events that have a field whose name `filter` accepts.
    ///
    /// The filter sees a span's fields at its creation, so a marker recorded later does not
    /// enable the span.
    /// Events inside a recorded span still need a marker of their own.
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
    /// // This event is recorded, because it has a `perfetto` field.
    /// tracing::info!(perfetto = true, my_bool = true);
    ///
    /// // This span is recorded, because it has a `perfetto` field.
    /// #[tracing::instrument(fields(perfetto = true))]
    /// fn to_instr() {
    ///   // This event is not recorded.
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
        source: Box<dyn ProcessMemorySource>,
    ) -> Self
    where
        W: Send + Sync + 'static,
    {
        let sampler = ProcessMemorySampler::spawn(ProcessMemorySamplerConfig {
            writer: Arc::clone(&self.writer),
            sequence: Arc::clone(&self.sequence),
            counter_track_uuid: self.process_memory_rss_track_uuid,
            interval,
            source,
        });
        self.lifecycle.replace_sampler(sampler);
        self
    }

    /// Returns whether the layer records a span or event whose fields `record` visits.
    fn is_recorded(&self, record: impl FnOnce(&mut dyn Visit)) -> bool {
        self.config
            .filter
            .is_none_or(|filter| fields::has_marker(filter, record))
    }

    /// Writes `trace_packets` as one `Trace` message, unless the trace is finished.
    ///
    /// Every packet of the layer reaches the writer through here or through the sampler, both of
    /// which write through the [`TraceSequence`], so the sequence's preamble precedes them.
    fn write_packets(&self, trace_packets: Vec<idl::TracePacket>) {
        if self.lifecycle.is_finished() {
            return;
        }
        self.sequence.write(
            self.writer.as_ref(),
            &idl::Trace {
                packet: trace_packets,
            },
        );
    }

    /// Writes `events` on the calling thread's track, which is described on its first use.
    ///
    /// The descriptor goes in the same `Trace` message as the events, so no packet refers to the
    /// thread's track before the trace describes it.
    /// The events share one timestamp, so slices that a non-LIFO exit ends and begins again
    /// resume exactly where they ended.
    fn write_on_thread_track(&self, events: Vec<idl::TrackEvent>) {
        if self.lifecycle.is_finished() {
            return;
        }
        let track_uuid = thread_track::current_thread_uuid();
        let mut trace_packets = Vec::with_capacity(events.len() + 1);
        if thread_track::first_thread_track_use(self.layer_id) {
            trace_packets.push(packets::track_descriptor_packet(
                thread_track::current_thread_track_descriptor(),
            ));
        }
        let timestamp = self.sequence.now();
        trace_packets.extend(events.into_iter().map(|mut event| {
            event.track_uuid = Some(track_uuid);
            self.event_packet_at(event, timestamp)
        }));
        self.write_packets(trace_packets);
    }

    /// Wraps `event` in a packet timestamped now.
    fn event_packet(&self, event: idl::TrackEvent) -> idl::TracePacket {
        self.event_packet_at(event, self.sequence.now())
    }

    /// Wraps `event` in a packet of the layer's sequence, timestamped at `timestamp`.
    fn event_packet_at(&self, event: idl::TrackEvent, timestamp: u64) -> idl::TracePacket {
        idl::TracePacket {
            data: Some(idl::trace_packet::Data::TrackEvent(event)),
            ..self.sequence.packet_at(timestamp)
        }
    }

    /// Writes the begin of a new span's lifetime slice and returns where that slice lies.
    ///
    /// A new track's descriptor goes in the same `Trace` message as the begin.
    fn begin_lifetime_slice<S>(
        &self,
        span: &SpanRef<'_, S>,
        own_track_name: Option<&str>,
        slice_name: &str,
        correlation_id: u64,
        debug_annotations: Vec<idl::DebugAnnotation>,
    ) -> LifetimeSlice
    where
        S: for<'a> LookupSpan<'a>,
    {
        // The begin is timestamped after the placement, so it follows the end of any sibling
        // whose close freed the track.
        let SpanPlacement {
            lifetime,
            new_track,
        } = span_state::place_span(
            span,
            self.layer_id,
            self.sequence.process_track_uuid(),
            own_track_name,
        );
        let mut event = create_event(
            lifetime.track_uuid,
            Some(slice_name),
            span.metadata().file().zip(span.metadata().line()),
            debug_annotations,
            idl::track_event::Type::SliceBegin,
        );
        event.correlation_id_field = Some(idl::track_event::CorrelationIdField::CorrelationId(
            correlation_id,
        ));
        let mut trace_packets = Vec::with_capacity(2);
        trace_packets.extend(new_track.map(packets::track_descriptor_packet));
        trace_packets.push(self.event_packet(event));
        self.write_packets(trace_packets);
        lifetime
    }

    /// Builds the begin of a slice of `span_state` entered on the calling thread.
    ///
    /// A lifetime slice carries the span's fields and source location, so an entered slice
    /// repeats them only when span lifetimes are off.
    fn entered_slice_begin(
        &self,
        metadata: &'static Metadata<'static>,
        span_state: &SpanState,
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
            thread_track::current_thread_uuid(),
            Some(&span_state.slice_name),
            location,
            annotations,
            idl::track_event::Type::SliceBegin,
        );
        event.correlation_id_field = Some(idl::track_event::CorrelationIdField::CorrelationId(
            span_state.correlation_id,
        ));
        event
    }
}

#[cfg(test)]
impl<W> PerfettoLayer<W> {
    /// Returns the UUID of the process track that the layer's preamble describes.
    pub(crate) fn process_track_uuid(&self) -> u64 {
        self.sequence.process_track_uuid()
    }

    /// Returns the writer that the layer and its sampler share.
    pub(crate) fn writer(&self) -> &Arc<W> {
        &self.writer
    }
}

impl<W> Drop for PerfettoLayer<W> {
    fn drop(&mut self) {
        self.lifecycle.finish();
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
        if !self.is_recorded(|visitor| attrs.record(visitor)) {
            return;
        }

        let mut debug_annotations = DebugAnnotations::default();
        if self.config.debug_annotations {
            attrs.record(&mut debug_annotations);
        }

        let mut naming = NamingFields::default();
        attrs.record(&mut naming);
        let slice_name = naming
            .slice_name
            .map_or(Cow::Borrowed(span.name()), Cow::Owned);

        // Without a lifetime slice to carry them, the creation fields go on every entered slice.
        let creation_annotations = if self.config.span_lifetimes {
            Vec::new()
        } else {
            std::mem::take(&mut debug_annotations.annotations)
        };
        let correlation_id = packets::unique_uuid();
        let lifetime = self.config.span_lifetimes.then(|| {
            self.begin_lifetime_slice(
                &span,
                naming.track_name.as_deref(),
                &slice_name,
                correlation_id,
                debug_annotations.annotations,
            )
        });

        let span_state = SpanState {
            slice_name,
            correlation_id,
            lifetime,
            creation_annotations,
            recorded_annotations: Vec::new(),
        };
        SpanStates::insert(&mut span.extensions_mut(), self.layer_id, span_state);
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
        if let Some(states) = span.extensions_mut().get_mut::<SpanStates>()
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
        if !self.is_recorded(|visitor| event.record(visitor)) {
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
            debug_annotations.annotations,
            idl::track_event::Type::Instant,
        );

        // An event belongs to its explicit parent or else the current span, and lands on the
        // track of the nearest such span that this layer records.
        let span_track_uuid = ctx.event_span(event).and_then(|event_span| {
            event_span.scope().find_map(|span| {
                span.extensions()
                    .get::<SpanStates>()
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
                .get::<SpanStates>()
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
            .get_mut::<SpanStates>()
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
            span_state.recorded_annotations,
            idl::track_event::Type::SliceEnd,
        );
        self.write_packets(vec![self.event_packet(event)]);

        // Free the parent's track only after writing the end, so a sibling that nests there next
        // begins after it.
        lifetime.close(&ctx, self.layer_id);
    }
}
