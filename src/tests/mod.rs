//! Unit tests that decode the written trace through the generated Perfetto types, and the
//! in-memory writer and decoding helpers they share.

use crate::idl;
use googletest::prelude::*;
use parking_lot::Mutex;
use prost::Message;
use std::collections::HashMap;
use std::sync::Arc;
use tracing_subscriber::fmt::MakeWriter;

mod layer;
mod thread_slices;
mod tracks;
mod wire;

/// A sink that a [`crate::PerfettoLayer`] writes to, accumulating the trace in memory.
///
/// The buffer holds the encoded `Trace` messages back to back, which decode as one `Trace`.
struct TestWriter {
    buf: Arc<Mutex<Vec<u8>>>,
    /// The offset in `buf` at which each write ends.
    write_ends: Arc<Mutex<Vec<usize>>>,
}

impl TestWriter {
    fn new() -> Self {
        Self {
            buf: Arc::new(Mutex::new(Vec::new())),
            write_ends: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl<'a> MakeWriter<'a> for TestWriter {
    type Writer = TestWriter;
    fn make_writer(&'a self) -> Self::Writer {
        TestWriter {
            buf: Arc::clone(&self.buf),
            write_ends: Arc::clone(&self.write_ends),
        }
    }
}

impl std::io::Write for TestWriter {
    /// Appends `buf` as one write.
    ///
    /// The layer hands each encoded `Trace` message to a single `write` call, so each recorded
    /// write is one message.
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut guard = self.buf.lock();
        guard.extend_from_slice(buf);
        self.write_ends.lock().push(guard.len());
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        // There's nothing to flush, we always immediately append to the buffer.
        std::io::Result::Ok(())
    }
}

/// Returns a layer that records debug annotations, and a writer that shares its buffer.
fn annotated_layer() -> (crate::PerfettoLayer<TestWriter>, TestWriter) {
    let writer = TestWriter::new();
    let output = writer.make_writer();
    (
        crate::PerfettoLayer::new(writer).with_debug_annotations(true),
        output,
    )
}

/// Decodes everything written to `writer` so far as one trace.
fn decode(writer: &TestWriter) -> Result<idl::Trace> {
    idl::Trace::decode(writer.buf.lock().as_slice()).or_fail()
}

/// Decodes each write to `writer` as its own `Trace` message.
fn decode_writes(writer: &TestWriter) -> Result<Vec<idl::Trace>> {
    let buf = writer.buf.lock();
    let write_ends = writer.write_ends.lock();
    let mut start = 0;
    let mut traces = Vec::with_capacity(write_ends.len());
    for &end in write_ends.iter() {
        traces.push(idl::Trace::decode(buf.get(start..end).or_fail()?).or_fail()?);
        start = end;
    }
    Ok(traces)
}

fn track_event(packet: &idl::TracePacket) -> Option<&idl::TrackEvent> {
    match &packet.data {
        Some(idl::trace_packet::Data::TrackEvent(event)) => Some(event),
        _ => None,
    }
}

fn track_descriptor(packet: &idl::TracePacket) -> Option<&idl::TrackDescriptor> {
    match &packet.data {
        Some(idl::trace_packet::Data::TrackDescriptor(descriptor)) => Some(descriptor),
        _ => None,
    }
}

fn events_of_type(
    trace: &idl::Trace,
    event_type: idl::track_event::Type,
) -> impl Iterator<Item = &idl::TrackEvent> {
    trace
        .packet
        .iter()
        .filter_map(track_event)
        .filter(move |event| event.r#type() == event_type)
}

fn event_name(event: &idl::TrackEvent) -> Option<&str> {
    match &event.name_field {
        Some(idl::track_event::NameField::Name(name)) => Some(name),
        _ => None,
    }
}

fn correlation_id(event: &idl::TrackEvent) -> Option<u64> {
    match event.correlation_id_field {
        Some(idl::track_event::CorrelationIdField::CorrelationId(id)) => Some(id),
        _ => None,
    }
}

fn track_name(descriptor: &idl::TrackDescriptor) -> Option<&str> {
    match &descriptor.static_or_dynamic_name {
        Some(idl::track_descriptor::StaticOrDynamicName::Name(name)) => Some(name),
        _ => None,
    }
}

/// Returns the descriptors of span tracks, which describe no process, thread or counter.
fn span_track_descriptors(trace: &idl::Trace) -> Vec<&idl::TrackDescriptor> {
    trace
        .packet
        .iter()
        .filter_map(track_descriptor)
        .filter(|descriptor| {
            descriptor.process.is_none()
                && descriptor.thread.is_none()
                && descriptor.counter.is_none()
        })
        .collect()
}

/// Returns the UUIDs of the thread tracks described in `trace`.
fn thread_track_uuids(trace: &idl::Trace) -> Vec<u64> {
    trace
        .packet
        .iter()
        .filter_map(track_descriptor)
        .filter(|descriptor| descriptor.thread.is_some())
        .map(idl::TrackDescriptor::uuid)
        .collect()
}

fn string_annotation<'a>(annotations: &'a [idl::DebugAnnotation], name: &str) -> Option<&'a str> {
    annotations.iter().find_map(
        |annotation| match (&annotation.name_field, &annotation.value) {
            (
                Some(idl::debug_annotation::NameField::Name(annotation_name)),
                Some(idl::debug_annotation::Value::StringValue(value)),
            ) if annotation_name == name => Some(value.as_str()),
            _ => None,
        },
    )
}

fn annotation_names(annotations: &[idl::DebugAnnotation]) -> Vec<&str> {
    annotations
        .iter()
        .filter_map(|annotation| match &annotation.name_field {
            Some(idl::debug_annotation::NameField::Name(name)) => Some(name.as_str()),
            _ => None,
        })
        .collect()
}

/// A timestamped instant event of a trace.
struct Instant<'a> {
    timestamp: u64,
    event: &'a idl::TrackEvent,
}

/// Returns the one instant whose `marker` field is `marker`.
fn instant_marked<'a>(trace: &'a idl::Trace, marker: &str) -> Result<Instant<'a>> {
    let instants: Vec<_> = trace
        .packet
        .iter()
        .filter_map(|packet| {
            let event = track_event(packet)?;
            (event.r#type() == idl::track_event::Type::Instant
                && string_annotation(&event.debug_annotations, "marker") == Some(marker))
            .then_some(Instant {
                timestamp: packet.timestamp?,
                event,
            })
        })
        .collect();
    let count = instants.len();
    let Ok([instant]) = <[_; 1]>::try_from(instants) else {
        return Err(format!("expected one instant marked {marker}, got {count}")).or_fail();
    };
    Ok(instant)
}

/// Records `name` into the `closes` field of `span` and drops it.
///
/// The slice's end then carries the name of the slice it ends, which [`replay_slices`] checks.
fn close_labeled(span: tracing::Span, name: &str) {
    span.record("closes", name);
    drop(span);
}

/// A slice reconstructed by replaying the begins and ends on its track.
#[derive(Debug)]
struct ReplayedSlice {
    name: String,
    track_uuid: u64,
    /// The number of slices that enclose this one on its track.
    depth: usize,
    begin_timestamp: u64,
    begin_annotations: Vec<idl::DebugAnnotation>,
    /// The correlation ID of the slice's begin.
    correlation_id: Option<u64>,
    /// Whether the slice's begin names a source location.
    has_source_location: bool,
    /// The timestamp of the slice's end, or `None` while the slice is open.
    end_timestamp: Option<u64>,
    end_annotations: Vec<idl::DebugAnnotation>,
}

/// Replays the slice begins and ends of every track and checks that they form valid stacks.
///
/// Packets are replayed in timestamp order, keeping write order for equal timestamps, as trace
/// processor imports them.
/// Each end must close an open slice on its own track no earlier than that slice began.
/// An end whose `closes` annotation names a slice, as [`close_labeled`] records it, must close
/// the slice of that name.
fn replay_slices(trace: &idl::Trace) -> Result<Vec<ReplayedSlice>> {
    let mut slice_events: Vec<(u64, &idl::TrackEvent)> = trace
        .packet
        .iter()
        .filter_map(|packet| {
            let event = track_event(packet)?;
            matches!(
                event.r#type(),
                idl::track_event::Type::SliceBegin | idl::track_event::Type::SliceEnd
            )
            .then_some((packet.timestamp?, event))
        })
        .collect();
    slice_events.sort_by_key(|&(timestamp, _)| timestamp);

    let mut slices: Vec<ReplayedSlice> = Vec::new();
    let mut open_slices: HashMap<u64, Vec<usize>> = HashMap::new();
    for (timestamp, event) in slice_events {
        let track_uuid = event.track_uuid.or_fail()?;
        let stack = open_slices.entry(track_uuid).or_default();

        // Slice begin
        if event.r#type() == idl::track_event::Type::SliceBegin {
            stack.push(slices.len());
            slices.push(ReplayedSlice {
                name: event_name(event).or_fail()?.to_string(),
                track_uuid,
                depth: stack.len() - 1,
                begin_timestamp: timestamp,
                begin_annotations: event.debug_annotations.clone(),
                correlation_id: correlation_id(event),
                has_source_location: event.source_location_field.is_some(),
                end_timestamp: None,
                end_annotations: Vec::new(),
            });
            continue;
        }

        // Slice end
        let Some(index) = stack.pop() else {
            return Err(format!(
                "the slice end at {timestamp} on track {track_uuid} closes no open slice"
            ))
            .or_fail();
        };
        let slice = slices.get_mut(index).or_fail()?;
        verify_that!(timestamp, ge(slice.begin_timestamp))?;
        if let Some(label) = string_annotation(&event.debug_annotations, "closes")
            && label != slice.name
        {
            return Err(format!(
                "the end of {label} on track {track_uuid} closes the open slice {}",
                slice.name
            ))
            .or_fail();
        }
        slice.end_timestamp = Some(timestamp);
        slice.end_annotations = event.debug_annotations.clone();
    }
    Ok(slices)
}

/// Replays the slices of every track like [`replay_slices`] and returns those on span tracks.
///
/// Entered slices on thread tracks are checked for valid stacks too, but left out.
fn replay_lifetime_slices(trace: &idl::Trace) -> Result<Vec<ReplayedSlice>> {
    let thread_tracks = thread_track_uuids(trace);
    let mut slices = replay_slices(trace)?;
    slices.retain(|slice| !thread_tracks.contains(&slice.track_uuid));
    Ok(slices)
}

/// Returns the one replayed slice named `name`.
fn slice_named<'a>(slices: &'a [ReplayedSlice], name: &str) -> Result<&'a ReplayedSlice> {
    let named: Vec<_> = slices.iter().filter(|slice| slice.name == name).collect();
    let [slice] = named[..] else {
        return Err(format!(
            "expected one slice named {name}, got {}",
            named.len()
        ))
        .or_fail();
    };
    Ok(slice)
}
