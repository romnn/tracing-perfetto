//! The [`PerfettoLayer`], its builder methods, and the tracing callbacks that write a span or
//! event as trace packets when the layer observes it.

use crate::event_counters::EventCounterTracks;
use crate::fields::{DebugAnnotations, EventFields, NamingFields};
use crate::lifecycle::{FinishHandle, TraceLifecycle};
use crate::packets::{self, SourceLocation, UnplacedEvent};
use crate::process_memory::sampler::{
    CounterTrack, ProcessMemorySampler, ProcessMemorySamplerConfig,
};
use crate::process_memory::{self, ProcessMemorySource};
use crate::sink::TraceSink;
use crate::span_state::{
    self, LayerId, LifetimeSlice, SpanKind, SpanPlacement, SpanState, SpanStates,
};
use crate::thread_state::ThreadStates;
use crate::writer::PerfettoWriter;
use crate::{idl, thread_track};
use std::borrow::Cow;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tracing::dispatcher::WeakDispatch;
use tracing::span;
use tracing::{Dispatch, Event, Id, Subscriber};
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::{LookupSpan, SpanRef};
use tracing_subscriber::{Layer, Registry};

/// A [`Layer`] that streams spans as slices and events as instants into a Perfetto trace.
///
/// The numeric `counter.*` fields of events are values on counter tracks, as the
/// [crate documentation](crate#counters) describes.
///
/// A span's lifetime slice begins when the span is created and ends when it closes, whichever
/// thread closes it, on a track chosen as the [crate documentation](crate#tracks) describes.
/// Each time a thread enters the span, an entered slice runs on that thread's track until the
/// span exits, so each poll of an instrumented future is one slice on the worker that ran it.
/// Every packet is written when the layer observes it, so a trace cut short still holds every
/// slice begun so far.
///
/// Timestamps come from a monotonic clock, which on Linux and Android counts boot time.
/// The trace maps it onto boot time there, so it merges with a system trace, and onto wall-clock
/// time elsewhere.
/// Several layers can share one registry, and each writes a trace that opens on its own.
///
/// Dropping the layer, or finishing it through a [`FinishHandle`], ends the trace and flushes the
/// writer, with the [limitations](crate#limitations) that the crate documents.
pub struct PerfettoLayer<W> {
    layer_id: LayerId,
    /// The sink that the layer and its process-memory sampler write through.
    sink: Arc<TraceSink<W>>,
    config: Config,
    threads: ThreadStates,
    /// The counter track of the process-memory samples, which every sampler of the layer reuses.
    process_memory_rss_track: Arc<CounterTrack>,
    /// The counter tracks that the `counter.*` fields of events write to.
    event_counters: EventCounterTracks,
    lifecycle: Arc<TraceLifecycle>,
    /// The dispatcher that the layer is part of, through which events find the spans that the
    /// layer's per-layer filter hides from its context.
    ///
    /// It is weak because the dispatcher owns the layer.
    dispatch: OnceLock<WeakDispatch>,
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

impl<W: PerfettoWriter + Send + Sync + 'static> PerfettoLayer<W> {
    /// Creates a layer that writes encoded trace packets to `writer`.
    ///
    /// Spans are recorded as lifetime and entered slices, debug annotations start disabled, and
    /// no process memory is sampled.
    ///
    /// The writer is shared with every [`FinishHandle`], which flushes it from whichever thread
    /// finishes the trace, so it must be [`Send`] and [`Sync`], as any layer of a
    /// [`Dispatch`](tracing::Dispatch) must be anyway.
    #[must_use]
    pub fn new(writer: W) -> Self {
        let layer_id = LayerId::next();
        let sink = Arc::new(TraceSink::new(writer));
        let weak_sink = Arc::downgrade(&sink);
        let lifecycle = Arc::new(TraceLifecycle::new(weak_sink, Arc::clone(sink.gate())));
        Self {
            layer_id,
            sink,
            config: Config::default(),
            threads: ThreadStates::new(layer_id),
            process_memory_rss_track: Arc::new(CounterTrack::new()),
            event_counters: EventCounterTracks::default(),
            lifecycle,
            dispatch: OnceLock::new(),
        }
    }

    /// Returns a handle that ends this layer's trace without dropping the layer.
    #[must_use]
    pub fn finish_handle(&self) -> FinishHandle {
        self.lifecycle.finish_handle()
    }

    /// Configures whether spans and events carry their fields as debug annotations.
    ///
    /// Disabled by default, because formatting every field costs time on every span and event,
    /// and the fields recorded on a span stay in memory until it closes.
    /// When enabled, an event's fields are annotations on its instant, except for the numeric
    /// `counter.*` fields, which are values on counter tracks either way.
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
    /// through a [`FinishHandle`], which stops it without waiting for the rest of its interval.
    /// An `interval` shorter than one millisecond samples every millisecond.
    #[must_use]
    pub fn with_process_memory_rss(self, interval: Duration) -> Self {
        let Some(source) = process_memory::default_process_memory_source() else {
            return self;
        };
        self.with_process_memory_source(interval, source)
    }

    /// Samples the resident set size from `source` every `interval` onto a `process.rss` counter
    /// track.
    ///
    /// Intervals in which `source` reports no value are skipped, and an `interval` shorter than
    /// one millisecond samples every millisecond.
    /// Calling this again stops the previous sampler and waits for the sample it is reading, if
    /// any, before the new one starts, and the new one continues the previous one's track.
    /// Once the trace is finished, no sampler starts, and `source` is dropped.
    /// It must not be called from the memory source or from the writer, since it waits for the
    /// previous sampler, which may be the very thread that calls it.
    #[must_use]
    pub fn with_process_memory_source(
        self,
        interval: Duration,
        source: Box<dyn ProcessMemorySource>,
    ) -> Self {
        let source: Arc<dyn ProcessMemorySource> = Arc::from(source);
        self.lifecycle.replace_sampler(|| {
            ProcessMemorySampler::spawn(ProcessMemorySamplerConfig {
                sink: Arc::clone(&self.sink),
                counter_track: Arc::clone(&self.process_memory_rss_track),
                interval,
                source: Arc::clone(&source),
            })
        });
        // The source's `Drop` may finish the trace, which takes the lifecycle's locks, so a source
        // that no sampler took, such as one whose thread could not be spawned under those locks,
        // is dropped only here.
        drop(source);
        self
    }
}

impl<W: PerfettoWriter> PerfettoLayer<W> {
    /// Writes `trace_packets` as one `Trace` message, unless the trace is finished, and returns
    /// whether the message was written.
    ///
    /// Every packet of the layer reaches the writer through here or through the sampler, both of
    /// which write through the [`TraceSink`], so the sequence's preamble precedes them, and none
    /// follows the end of the trace.
    fn write_packets(&self, trace_packets: Vec<idl::TracePacket>) -> bool {
        self.sink.write(&idl::Trace {
            packet: trace_packets,
        })
    }

    /// Writes `events` at `timestamp` on the calling thread's track, which is described on its
    /// first use.
    ///
    /// The descriptor goes in the same `Trace` message as the events, so no packet refers to the
    /// thread's track before the trace describes it.
    /// The events share one timestamp, so slices that a non-LIFO exit ends and begins again
    /// resume exactly where they ended.
    fn write_on_thread_track(&self, events: Vec<UnplacedEvent>, timestamp: u64) {
        let track_uuid = thread_track::current_thread_uuid();
        let mut trace_packets = Vec::with_capacity(events.len() + 1);

        // During thread teardown the layer keeps no record of the description, so every use
        // describes the track again and no packet refers to an undescribed track.
        let described = self
            .threads
            .with_current(|thread| thread.track_described)
            .unwrap_or(false);
        if !described {
            trace_packets.push(packets::track_descriptor_packet(
                thread_track::current_thread_track_descriptor(
                    self.sink.sequence().process_track_uuid(),
                ),
            ));
        }
        trace_packets.extend(
            events
                .into_iter()
                .map(|event| self.event_packet(event.on_track(track_uuid), timestamp)),
        );
        // The track counts as described only once its descriptor is written.
        if self.write_packets(trace_packets) && !described {
            self.threads
                .with_current(|thread| thread.track_described = true);
        }
    }

    /// Writes `event` at `timestamp` on the span track `track_uuid`.
    ///
    /// The descriptor of a track that `event` is the first to use, if any, goes in the same
    /// `Trace` message.
    fn write_on_span_track(
        &self,
        track_uuid: u64,
        new_track: Option<idl::TrackDescriptor>,
        event: UnplacedEvent,
        timestamp: u64,
    ) {
        let mut trace_packets = Vec::with_capacity(2);
        trace_packets.extend(new_track.map(packets::track_descriptor_packet));
        trace_packets.push(self.event_packet(event.on_track(track_uuid), timestamp));
        self.write_packets(trace_packets);
    }

    /// Wraps `event` in a packet of the layer's sequence, timestamped at `timestamp`.
    fn event_packet(&self, event: idl::TrackEvent, timestamp: u64) -> idl::TracePacket {
        idl::TracePacket {
            data: Some(idl::trace_packet::Data::TrackEvent(event)),
            ..self.sink.sequence().packet_at(timestamp)
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
            self.sink.sequence().process_track_uuid(),
            own_track_name,
        );
        self.write_on_span_track(
            lifetime.track_uuid,
            new_track,
            begin,
            self.sink.sequence().now(),
        );
        lifetime
    }

    /// Returns the lifetime track of the nearest span that the layer records, among the span
    /// that `event` belongs to and its ancestors.
    ///
    /// An event belongs to its explicit parent, or else to the current span.
    fn event_span_track<S>(&self, event: &Event<'_>, ctx: &Context<'_, S>) -> Option<u64>
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
    {
        // The subscriber's current span may be one that the layer's filter rejected, whereas
        // `Context::lookup_current` skips such a span in favor of a recorded span entered on the
        // thread, which may be unrelated to the event.
        let span_id = if event.is_contextual() {
            ctx.current_span().id()?.clone()
        } else {
            event.parent()?.clone()
        };
        if let Some(span) = ctx.span(&span_id) {
            return span_state::nearest_lifetime_track(&span, self.layer_id);
        }

        // The context hides a span that the layer's filter rejected, so the span is looked up in
        // the registry itself, through which its ancestors are reached as well.
        // Only a `Registry` hides spans from a filtered context.
        if let Some(dispatch) = self.dispatch.get().and_then(WeakDispatch::upgrade)
            && let Some(registry) = dispatch.downcast_ref::<Registry>()
        {
            return registry
                .span(&span_id)
                .and_then(|span| span_state::nearest_lifetime_track(&span, self.layer_id));
        }

        // A layer that never learned its dispatcher, as inside an `Option` or a `Vec` layer or a
        // `Box` or `Arc` subscriber, or once a reload installed it, has only the context's view of
        // the spans.
        // An event inside the rejected span then goes to the innermost recorded span entered on
        // the thread, and one that names it as its explicit parent to the thread's track.
        ctx.event_span(event)
            .and_then(|span| span_state::nearest_lifetime_track(&span, self.layer_id))
    }
}

#[cfg(test)]
impl<W> PerfettoLayer<W> {
    /// Returns the UUID of the process track that the layer's preamble describes.
    pub(crate) fn process_track_uuid(&self) -> u64 {
        self.sink.sequence().process_track_uuid()
    }

    /// Returns the sink that the layer and its sampler share.
    pub(crate) fn sink(&self) -> &Arc<TraceSink<W>> {
        &self.sink
    }

    /// Returns the counter tracks that the `counter.*` fields of events write to.
    pub(crate) fn event_counters(&self) -> &EventCounterTracks {
        &self.event_counters
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
    fn on_register_dispatch(&self, dispatch: &Dispatch) {
        // Only the creation of a dispatcher around the layer registers it, and attaching the layer
        // anew forgets the dispatcher, so the layer is registered at most once per attachment.
        // A wrapper that does not pass the call on, such as an `Option` layer or a `Box`
        // subscriber, and a reload that installs the layer later leave it without a dispatcher.
        self.dispatch.get_or_init(|| dispatch.downgrade());
    }

    fn on_layer(&mut self, _subscriber: &mut S) {
        // A layer moved out of one subscriber, such as through a reload handle, and attached to
        // another is part of the new subscriber's dispatcher only, whose creation registers it.
        self.dispatch = OnceLock::new();
    }

    fn on_new_span(&self, attrs: &span::Attributes<'_>, id: &span::Id, ctx: Context<'_, S>) {
        if !self.config.span_lifetimes && !self.config.thread_slices {
            return;
        }
        // A span that the writer creates while the layer calls it keeps no state, as if this
        // layer did not record it: its descriptor and begin could not be written, so a placed
        // span would put its events on an undescribed track, or end its parent's slice when it
        // closes.
        // Its events then land on the track of its nearest recorded ancestor.
        if self.sink.gate().is_calling_writer() {
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

        let recorded = span
            .extensions()
            .get::<SpanStates>()
            .is_some_and(|states| states.get(self.layer_id).is_some());
        if !recorded {
            return;
        }

        // The values are formatted while no extensions lock is held, because formatting runs user
        // code.
        // A `Debug` impl that records into this span would otherwise take the span's lock again
        // and deadlock, and one that panics would poison the lock for every later access.
        //
        // Routing and naming are fixed at creation, so a late `perfetto.track_name` or
        // `perfetto.slice_name` is only an annotation.
        let mut debug_annotations = DebugAnnotations::default();
        values.record(&mut debug_annotations);

        if let Some(states) = span.extensions_mut().get_mut::<SpanStates>()
            && let Some(span_state) = states.get_mut(self.layer_id)
        {
            span_state
                .recorded_annotations
                .append(&mut debug_annotations.annotations);
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        // An event that the writer records while the layer calls it is not written, so its
        // packets are not built either.
        // Building them may describe the thread's track, which looks up the thread's name
        // through `std::thread::current`, and that panics in a destructor that runs after the
        // thread's handle is gone, such as one that finishes the trace and so flushes the writer.
        if self.sink.gate().is_calling_writer() {
            return;
        }
        let metadata = event.metadata();
        let mut fields = EventFields::new(self.config.debug_annotations);
        event.record(&mut fields);
        let EventFields {
            counters,
            annotations,
        } = fields;

        // The counter values and the instant share the event's timestamp.
        let timestamp = self.sink.sequence().now();
        self.event_counters.write(&self.sink, counters, timestamp);

        let instant = UnplacedEvent {
            event_type: idl::track_event::Type::Instant,
            name: Some(Cow::Borrowed(metadata.name())),
            location: SourceLocation::of(metadata),
            debug_annotations: annotations
                .map(|annotations| annotations.annotations)
                .unwrap_or_default(),
            correlation_id: None,
        };

        match self.event_span_track(event, &ctx) {
            Some(track_uuid) => self.write_on_span_track(track_uuid, None, instant, timestamp),
            None => self.write_on_thread_track(vec![instant], timestamp),
        }
    }

    fn on_enter(&self, id: &Id, ctx: Context<'_, S>) {
        // The writer's entries are skipped for the same reason as its events, and its exits with
        // them, so that an exit inside the writer never ends a slice that an entry outside it
        // began.
        if !self.config.thread_slices || self.sink.gate().is_calling_writer() {
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
            self.write_on_thread_track(vec![begin], self.sink.sequence().now());
        }
    }

    fn on_exit(&self, id: &Id, _ctx: Context<'_, S>) {
        if !self.config.thread_slices || self.sink.gate().is_calling_writer() {
            return;
        }
        // Only the thread's own stack decides what ends, because the exiting span may be closed
        // already or entered on other threads too.
        let events = self
            .threads
            .with_current(|thread| thread.entered.exit(id))
            .unwrap_or_default();
        if !events.is_empty() {
            self.write_on_thread_track(events, self.sink.sequence().now());
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
            self.sink.sequence().now(),
        );

        // Free the parent's track only after writing the end, so a sibling that nests there next
        // begins after it.
        lifetime.close(&ctx, self.layer_id);
    }
}
