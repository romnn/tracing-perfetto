//! Span fields that name slices or mark spans for recording, read back through the public decode
//! helper.

use googletest::prelude::*;
use parking_lot::Mutex;
use std::sync::Arc;
use tracing::{field, trace_span};
use tracing_perfetto::{PerfettoLayer, decode_track_event_names};
use tracing_subscriber::{fmt::MakeWriter, layer::SubscriberExt};

/// A sink that a [`PerfettoLayer`] writes to, accumulating the trace in memory.
///
/// Every clone shares one buffer, so the test keeps a clone to read what the layer wrote.
#[derive(Clone, Default)]
struct TestWriter {
    buf: Arc<Mutex<Vec<u8>>>,
}

impl<'a> MakeWriter<'a> for TestWriter {
    type Writer = TestWriter;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

impl std::io::Write for TestWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buf.lock().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[gtest]
fn test_span_slice_name_override_prefers_perfetto_track_name() -> Result<()> {
    let writer = TestWriter::default();
    let extra_writer = writer.clone();
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

    // Only the begins of the lifetime slice and the entered slice name them.
    let names = decode_track_event_names(&buf).or_fail()?;
    expect_eq!(names, ["otel-name", "otel-name"]);
    Ok(())
}

#[gtest]
fn test_span_slice_name_override_uses_otel_name_when_no_track_name() -> Result<()> {
    let writer = TestWriter::default();
    let extra_writer = writer.clone();
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

    // Only the begins of the lifetime slice and the entered slice name them.
    let names = decode_track_event_names(&buf).or_fail()?;
    expect_eq!(names, ["my-otel-span", "my-otel-span"]);
    Ok(())
}

// If all our spans are filtered, we shouldn't get any trace data at all.
// Doing a `.record` on a span should also "fail successfully".
#[gtest]
fn test_span_arguments_filtered() {
    let writer = TestWriter::default();
    let extra_writer = writer.clone();
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
