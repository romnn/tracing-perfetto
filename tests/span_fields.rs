//! Span fields that name slices or select spans for recording, read back through the public
//! decode helper.

use googletest::prelude::*;
use parking_lot::Mutex;
use std::sync::Arc;
use tracing::{field, trace_span};
use tracing_perfetto::{PerfettoLayer, decode_track_event_names};
use tracing_subscriber::Layer as _;
use tracing_subscriber::filter::filter_fn;
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

/// A span with both `perfetto.track_name` and `otel.name` takes its slice name from `otel.name`.
///
/// The track name names only the span's track, so its slices keep a name of their own.
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

/// A span with `otel.name` and no track name takes its slice name from `otel.name`.
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

/// A layer whose per-layer filter rejects every span writes no trace data at all.
///
/// Recording a field on a filtered span succeeds without writing anything.
#[gtest]
fn test_span_arguments_filtered() {
    let writer = TestWriter::default();
    let extra_writer = writer.clone();
    let perfetto_layer = PerfettoLayer::new(writer)
        .with_debug_annotations(true)
        .with_filter(filter_fn(|metadata| {
            metadata.fields().field("NO SUCH ARG").is_some()
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
    expect_eq!(buf.len(), 0);
}

/// Spans and events selected by a field-marker filter.
#[tracing::instrument(fields(perfetto))]
fn marked_without_a_value() {
    tracing::info!(perfetto = true, "marked event");
    tracing::info!("unmarked event");
}

/// A per-layer filter on a declared field selects spans that declare it without a value.
///
/// `#[instrument(fields(perfetto))]` declares the field as empty, so a filter that looked for a
/// recorded value instead of the declared field would drop the span.
#[gtest]
fn field_filter_selects_spans_that_declare_the_field_without_a_value() -> Result<()> {
    let writer = TestWriter::default();
    let output = writer.clone();
    let perfetto_layer = PerfettoLayer::new(writer)
        .with_thread_slices(false)
        .with_filter(filter_fn(|metadata| {
            metadata.fields().field("perfetto").is_some()
        }));
    let _guard =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(perfetto_layer));

    marked_without_a_value();

    // The span's lifetime slice and the marked event are recorded, the unmarked event is not.
    let names = decode_track_event_names(&output.buf.lock()).or_fail()?;
    expect_that!(names, len(eq(2)));
    expect_eq!(
        names.first().map(String::as_str),
        Some("marked_without_a_value")
    );
    Ok(())
}
