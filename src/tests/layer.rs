//! Regression tests for what one span or the finish handle writes, decoded slice by slice.

use crate::PerfettoLayer;
use crate::idl;
use crate::idl::track_event;
use crate::process_memory::sampler::MIN_SAMPLING_INTERVAL;
use crate::tests::{TestWriter, events_of_type};
use googletest::prelude::*;
use prost::Message;
use std::sync::{Arc, mpsc};
use std::time::Duration;
use tracing::{field, trace_span};
use tracing_subscriber::Layer as _;
use tracing_subscriber::filter::filter_fn;
use tracing_subscriber::{fmt::MakeWriter, layer::SubscriberExt};

/// One entered span reaches the output as protobuf data: a named begin and an unnamed end for
/// each of its lifetime and entered slices.
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
    // Count only track events, skipping the preamble and the track descriptors.
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

/// A span's arguments are written correctly: creation fields on the begin and recorded fields
/// on the end.
#[gtest]
fn test_span_arguments() -> Result<()> {
    let writer = TestWriter::new();
    let extra_writer = writer.make_writer();
    // Entered slices carry no fields while span lifetimes are on, so only the lifetime
    // slice is recorded.
    let perfetto_layer = PerfettoLayer::new(writer)
        .with_debug_annotations(true)
        .with_thread_slices(false)
        .with_filter(filter_fn(|metadata| {
            metadata.fields().field("regular_arg").is_some()
        }));

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
    // Count only track events, skipping the preamble and the track descriptors.
    for packet in trace.packet {
        let Some(idl::trace_packet::Data::TrackEvent(ref event)) = packet.data else {
            continue;
        };
        track_events_seen += 1;

        match event.r#type() {
            track_event::Type::SliceBegin => {
                saw_slice_begin = true;
                let expected = Some(track_event::NameField::Name(String::from("span_with_args")));
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

/// A memory source that always reports 1 KiB.
struct FixedProcessMemory;

impl crate::process_memory::ProcessMemorySource for FixedProcessMemory {
    fn resident_set_size_bytes(&self) -> Option<u64> {
        Some(1024)
    }
}

/// Finishing through the handle ends the trace while the layer stays installed.
///
/// The sampler thread is joined and later spans are not written.
#[gtest]
fn finish_handle_ends_the_trace_of_a_live_layer() {
    let writer = TestWriter::new();
    let extra_writer = writer.make_writer();
    let perfetto_layer = PerfettoLayer::new(writer).with_process_memory_source(
        std::time::Duration::from_millis(1),
        Box::new(FixedProcessMemory),
    );
    let finish = perfetto_layer.finish_handle();
    let layer_sink = Arc::clone(perfetto_layer.sink());
    let subscriber = tracing_subscriber::registry().with(perfetto_layer);
    let _guard = tracing::subscriber::set_default(subscriber);

    trace_span!("before_finish").in_scope(|| {});
    expect_that!(finish.finish(), ok(anything()));
    let written = extra_writer.buf.lock().len();
    trace_span!("after_finish").in_scope(|| {});

    // The joined sampler released its sink, leaving this test's and the layer's references.
    expect_eq!(Arc::strong_count(&layer_sink), 2);
    // The span recorded before finishing reached the writer; the later one did not.
    expect_that!(
        written,
        gt(0),
        "expected the span before finishing to be written"
    );
    expect_eq!(extra_writer.buf.lock().len(), written);
}

/// A clone of a finish handle finishes the same trace, and both report it as finished.
///
/// A clone with a lifecycle of its own would leave the layer writing after the clone's owner
/// finished it.
#[gtest]
fn cloned_finish_handle_finishes_the_same_trace() {
    let writer = TestWriter::new();
    let output = writer.make_writer();
    let perfetto_layer = PerfettoLayer::new(writer);
    let finish = perfetto_layer.finish_handle();
    let cloned = finish.clone();
    let _guard =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(perfetto_layer));

    expect_eq!(format!("{finish:?}"), "FinishHandle { finished: false }");
    expect_that!(cloned.finish(), ok(anything()));
    let written = output.buf.lock().len();
    trace_span!("after_finish").in_scope(|| {});

    expect_eq!(output.buf.lock().len(), written);
    expect_eq!(format!("{finish:?}"), "FinishHandle { finished: true }");
}

/// A memory source that reports 1 KiB and announces each sample it reports.
struct AnnouncingProcessMemory {
    announcements: mpsc::Sender<()>,
}

impl crate::process_memory::ProcessMemorySource for AnnouncingProcessMemory {
    fn resident_set_size_bytes(&self) -> Option<u64> {
        // The receiver is gone only after the test stopped waiting for samples.
        self.announcements.send(()).ok();
        Some(1024)
    }
}

/// Returns a layer that samples an announcing memory source every `interval`, and the receiver
/// of the source's announcements.
fn announcing_layer(
    writer: TestWriter,
    interval: Duration,
) -> (PerfettoLayer<TestWriter>, mpsc::Receiver<()>) {
    let (announcements, announced) = mpsc::channel();
    let layer = PerfettoLayer::new(writer).with_process_memory_source(
        interval,
        Box::new(AnnouncingProcessMemory { announcements }),
    );
    (layer, announced)
}

/// A writer that keeps nothing and announces each counter sample it is handed.
#[derive(Clone)]
struct SampleAnnouncingWriter {
    announcements: mpsc::Sender<()>,
}

impl MakeWriter<'_> for SampleAnnouncingWriter {
    type Writer = Self;

    fn make_writer(&self) -> Self::Writer {
        self.clone()
    }
}

impl std::io::Write for SampleAnnouncingWriter {
    /// Announces `buf` if it holds a counter sample.
    ///
    /// The layer hands each encoded `Trace` message to a single `write` call.
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let is_sample = idl::Trace::decode(buf).is_ok_and(|trace| {
            events_of_type(&trace, track_event::Type::Counter)
                .next()
                .is_some()
        });
        if is_sample {
            // The receiver is gone only after the test stopped waiting for samples.
            self.announcements.send(()).ok();
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Finishing a trace stops a sampler that waits for its next sample without waiting for the rest
/// of its interval.
///
/// A sampler that slept through its interval before noticing the stop would block the finish, and
/// a process that finishes its trace at exit, for an hour with this interval.
#[gtest]
fn finish_stops_the_sampler_without_waiting_for_its_interval() -> Result<()> {
    let (announcements, announced) = mpsc::channel();
    let layer = PerfettoLayer::new(SampleAnnouncingWriter { announcements })
        .with_process_memory_source(Duration::from_secs(3600), Box::new(FixedProcessMemory));
    let finish = layer.finish_handle();

    // The sampler writes its first sample after its last check for a stop before it waits an
    // hour for the next one, so the finish below can only stop it in that wait.
    announced.recv_timeout(Duration::from_secs(30)).or_fail()?;
    let (finished, finished_receiver) = mpsc::channel();
    std::thread::spawn(move || {
        finished.send(finish.finish()).ok();
    });

    // The bound is far above any scheduling delay and far below the interval.
    let finished = finished_receiver.recv_timeout(Duration::from_secs(30));
    if finished.is_err() {
        // Dropping the layer would finish it too, which waits for the stuck finish.
        std::mem::forget(layer);
    }
    verify_that!(finished, ok(ok(anything())))
}

/// A zero sampling interval samples at most once per millisecond.
///
/// A sampler that waited for exactly the requested interval would sample in a busy loop, taking
/// a CPU core and flooding the trace with samples.
#[gtest]
fn zero_interval_samples_at_most_once_per_millisecond() -> Result<()> {
    let writer = TestWriter::new();
    let output = writer.make_writer();
    let (layer, announced) = announcing_layer(writer, Duration::ZERO);
    let finish = layer.finish_handle();

    // The fourth announcement proves that the first three samples were written.
    for _ in 0..4 {
        announced.recv().or_fail()?;
    }
    finish.finish().or_fail()?;

    // Consecutive samples lie at least the minimum interval apart.
    let trace = idl::Trace::decode(output.buf.lock().as_slice()).or_fail()?;
    let sample_timestamps: Vec<u64> = trace
        .packet
        .iter()
        .filter(|packet| {
            matches!(
                &packet.data,
                Some(idl::trace_packet::Data::TrackEvent(event))
                    if event.r#type() == track_event::Type::Counter
            )
        })
        .filter_map(|packet| packet.timestamp)
        .collect();
    expect_that!(sample_timestamps, len(ge(3)));
    let min_interval_nanos = u64::try_from(MIN_SAMPLING_INTERVAL.as_nanos()).or_fail()?;
    for pair in sample_timestamps.windows(2) {
        let [earlier, later] = pair else {
            return fail!("expected a pair of timestamps");
        };
        expect_that!(later.saturating_sub(*earlier), ge(min_interval_nanos));
    }
    drop(layer);
    Ok(())
}

/// The sampler raises intervals below one millisecond to exactly one millisecond and keeps
/// longer ones.
///
/// The expected values are literals, so the test fails if the minimum disappears or changes,
/// which a test comparing against the production constant would not notice.
#[gtest]
fn sampling_interval_is_at_least_one_millisecond() {
    use crate::process_memory::sampler::sampling_interval;

    // Raised to the minimum
    expect_eq!(sampling_interval(Duration::ZERO), Duration::from_millis(1));
    expect_eq!(
        sampling_interval(Duration::from_nanos(1)),
        Duration::from_millis(1)
    );
    expect_eq!(
        sampling_interval(Duration::from_micros(999)),
        Duration::from_millis(1)
    );

    // Kept as requested
    expect_eq!(
        sampling_interval(Duration::from_millis(1)),
        Duration::from_millis(1)
    );
    expect_eq!(
        sampling_interval(Duration::from_micros(1_001)),
        Duration::from_micros(1_001)
    );
    expect_eq!(
        sampling_interval(Duration::from_secs(3600)),
        Duration::from_secs(3600)
    );
}
