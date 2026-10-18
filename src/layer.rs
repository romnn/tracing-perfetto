//! The [`PerfettoLayer`], its builder methods, and the tracing callbacks that write a span or
//! event as trace packets when the layer observes it.

use crate::fields::{DebugAnnotations, NamingFields};
use crate::lifecycle::{FinishHandle, TraceLifecycle};
use crate::packets::{self, SourceLocation, UnplacedEvent};
use crate::process_memory::sampler::{ProcessMemorySampler, ProcessMemorySamplerConfig};
use crate::process_memory::{self, ProcessMemorySource};
use crate::sequence::TraceSequence;
use crate::span_state::{
    self, LayerId, LifetimeSlice, SpanKind, SpanPlacement, SpanState, SpanStates,
};
use crate::thread_state::ThreadStates;
use crate::writer::PerfettoWriter;
use crate::{idl, thread_track};
use std::borrow::Cow;
use std::sync::Arc;
use std::time::Duration;
use tracing::span;
use tracing::{Event, Id, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::{LookupSpan, SpanRef};

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
pub struct PerfettoLayer<W> {
    layer_id: LayerId,
    sequence: Arc<TraceSequence>,
    writer: Arc<W>,
    config: Config,
    threads: ThreadStates,
    /// The counter track of the process-memory samples, which every sampler of the layer reuses.
    process_memory_rss_track_uuid: u64,
    lifecycle: Arc<TraceLifecycle>,
}

/// What a [`PerfettoLayer`] records, as its builder methods set it.
struct Config {
    debug_annotations: bool,
    span_lifetimes: bool,
    thread_slices: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            debug_annotations: false,
            span_lifetimes: true,
            thread_slices: true,
        }
    }
}

impl<W: PerfettoWriter> PerfettoLayer<W> {
    /// Creates a layer that writes encoded trace packets to `writer`.
    ///
    /// Spans are recorded as lifetime and entered slices, debug annotations start disabled, and
    /// no process memory is sampled.
    #[must_use]
    pub fn new(writer: W) -> Self {
        Self {
            layer_id: LayerId::next(),
            sequence: Arc::new(TraceSequence::new()),
            writer: Arc::new(writer),
            config: Config::default(),
            threads: ThreadStates::default(),
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
    fn write_on_thread_track(&self, events: Vec<UnplacedEvent>) {
        if self.lifecycle.is_finished() {
            return;
        }
        let track_uuid = thread_track::current_thread_uuid();
        let mut trace_packets = Vec::with_capacity(events.len() + 1);

        // During thread teardown the layer keeps no record of the description, so every use
        // describes the track again and no packet refers to an undescribed track.
        let first_use = self
            .threads
            .with_current(|thread| !std::mem::replace(&mut thread.track_described, true))
            .unwrap_or(true);
        if first_use {
            trace_packets.push(packets::track_descriptor_packet(
                thread_track::current_thread_track_descriptor(),
            ));
        }
        let timestamp = self.sequence.now();
        trace_packets.extend(
            events
                .into_iter()
                .map(|event| self.event_packet(event.on_track(track_uuid), timestamp)),
        );
        self.write_packets(trace_packets);
    }

    /// Writes `event` on the span track `track_uuid`, timestamped now.
    ///
    /// The descriptor of a track that `event` is the first to use, if any, goes in the same
    /// `Trace` message.
    fn write_on_span_track(
        &self,
        track_uuid: u64,
        new_track: Option<idl::TrackDescriptor>,
        event: UnplacedEvent,
    ) {
        let mut trace_packets = Vec::with_capacity(2);
        trace_packets.extend(new_track.map(packets::track_descriptor_packet));
        trace_packets.push(self.event_packet(event.on_track(track_uuid), self.sequence.now()));
        self.write_packets(trace_packets);
    }

    /// Wraps `event` in a packet of the layer's sequence, timestamped at `timestamp`.
    fn event_packet(&self, event: idl::TrackEvent, timestamp: u64) -> idl::TracePacket {
        idl::TracePacket {
            data: Some(idl::trace_packet::Data::TrackEvent(event)),
            ..self.sequence.packet_at(timestamp)
        }
    }

    /// Writes `begin`, the begin of a new span's lifetime slice, and returns where that slice
    /// lies.
    fn begin_lifetime_slice<S>(
        &self,
        span: &SpanRef<'_, S>,
        own_track_name: Option<&str>,
        begin: UnplacedEvent,
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
        self.write_on_span_track(lifetime.track_uuid, new_track, begin);
        lifetime
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

        let mut debug_annotations = DebugAnnotations::default();
        if self.config.debug_annotations {
            attrs.record(&mut debug_annotations);
        }

        let mut naming = NamingFields::default();
        attrs.record(&mut naming);
        let slice_name = naming
            .slice_name
            .map_or(Cow::Borrowed(span.name()), Cow::Owned);

        let correlation_id = packets::unique_uuid();
        let kind = if self.config.span_lifetimes {
            let begin = UnplacedEvent {
                event_type: idl::track_event::Type::SliceBegin,
                name: Some(slice_name.clone()),
                location: SourceLocation::of(span.metadata()),
                debug_annotations: debug_annotations.annotations,
                correlation_id: Some(correlation_id),
            };
            SpanKind::Lifetime(self.begin_lifetime_slice(
                &span,
                naming.track_name.as_deref(),
                begin,
            ))
        } else {
            // Without a lifetime slice to carry them, the creation fields go on every entered
            // slice.
            SpanKind::Entered {
                creation_annotations: debug_annotations.annotations,
            }
        };

        let span_state = SpanState {
            slice_name,
            correlation_id,
            kind,
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
        let metadata = event.metadata();
        let mut debug_annotations = DebugAnnotations::default();
        if self.config.debug_annotations {
            event.record(&mut debug_annotations);
        }
        let instant = UnplacedEvent {
            event_type: idl::track_event::Type::Instant,
            name: Some(Cow::Borrowed(metadata.name())),
            location: SourceLocation::of(metadata),
            debug_annotations: debug_annotations.annotations,
            correlation_id: None,
        };

        // An event belongs to its explicit parent or else the current span, and lands on the
        // track of the nearest such span that this layer records.
        let span_track_uuid = ctx.event_span(event).and_then(|event_span| {
            event_span.scope().find_map(|span| {
                span.extensions()
                    .get::<SpanStates>()
                    .and_then(|states| states.get(self.layer_id))
                    .and_then(SpanState::lifetime)
                    .map(|lifetime| lifetime.track_uuid)
            })
        });
        match span_track_uuid {
            Some(track_uuid) => self.write_on_span_track(track_uuid, None, instant),
            None => self.write_on_thread_track(vec![instant]),
        }
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
            span_state.entered_slice_begin(span.metadata())
        };
        // During thread teardown no exit could end the slice, so the layer writes no begin.
        let entered = self
            .threads
            .with_current(|thread| thread.entered.enter(id.clone(), begin.clone()))
            .is_some();
        if entered {
            self.write_on_thread_track(vec![begin]);
        }
    }

    fn on_exit(&self, id: &Id, _ctx: Context<'_, S>) {
        if !self.config.thread_slices {
            return;
        }
        // Only the thread's own stack decides what ends, because the exiting span may be closed
        // already or entered on other threads too.
        let events = self
            .threads
            .with_current(|thread| thread.entered.exit(id))
            .unwrap_or_default();
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
        let SpanKind::Lifetime(lifetime) = span_state.kind else {
            return;
        };

        self.write_on_span_track(
            lifetime.track_uuid,
            None,
            UnplacedEvent::slice_end(span_state.recorded_annotations),
        );

        // Free the parent's track only after writing the end, so a sibling that nests there next
        // begins after it.
        lifetime.close(&ctx, self.layer_id);
    }
}
