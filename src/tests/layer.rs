//! Regression tests for what one span or the finish handle writes, decoded slice by slice.

use crate::PerfettoLayer;
use crate::idl;
use crate::idl::track_event;
use crate::tests::TestWriter;
use googletest::prelude::*;
use prost::Message;
use std::sync::Arc;
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
    let layer_writer = Arc::clone(perfetto_layer.writer());
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
