//! An event recorded during thread teardown by a thread whose only span the layer's per-layer
//! filter rejects, through a test-owned global subscriber.

use googletest::prelude::*;
use parking_lot::Mutex;
use std::sync::Arc;
use tracing_perfetto::{PerfettoLayer, decode_track_event_names};
use tracing_subscriber::{fmt::MakeWriter, layer::SubscriberExt};

/// A sink that a [`PerfettoLayer`] writes to, accumulating the trace in memory.
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

/// A thread-local value that records an unscoped event when its thread destroys it.
struct RecordsOnDrop;

impl Drop for RecordsOnDrop {
    fn drop(&mut self) {
        tracing::info!(parent: None, during_teardown = true);
    }
}

thread_local! {
    static RECORDS_ON_DROP: RecordsOnDrop = const { RecordsOnDrop };
}

/// Runs a thread that creates [`RECORDS_ON_DROP`] and then enters and exits a span named
/// `entered`, which the layer under test leaves off the thread's track.
///
/// Entering the span makes the registry set up its own per-thread storage after the value, while
/// the layer has written nothing for the thread, so the value's event during teardown is the
/// layer's first use of the thread.
fn run_thread_that_records_while_tearing_down() -> Result<()> {
    std::thread::Builder::new()
        .name("tearing down".to_string())
        .spawn(|| {
            RECORDS_ON_DROP.with(|_| {});
            tracing::info_span!("entered").in_scope(|| {});
        })
        .or_fail()?
        .join()
        .or_fail()
}

/// A layer that records nothing, which keeps every callsite enabled so that the span the
/// filter rejects still exists in the registry.
struct KeepsCallsitesEnabled;

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for KeepsCallsitesEnabled {}

/// An event recorded during thread teardown is written without a panic when the layer's
/// per-layer filter rejected every span the thread entered.
///
/// The layer then first looks up its per-thread state from a thread-local destructor, after the
/// registry's per-thread storage may already be gone.
/// A lookup that panics there aborts the process.
#[gtest]
fn event_during_teardown_after_filtered_entries_is_written() -> Result<()> {
    use tracing_subscriber::Layer as _;

    let writer = TestWriter::default();
    let output = writer.clone();
    let layer =
        PerfettoLayer::new(writer).with_filter(tracing_subscriber::filter::filter_fn(|metadata| {
            metadata.name() != "entered"
        }));
    let subscriber = tracing_subscriber::registry()
        .with(layer)
        .with(KeepsCallsitesEnabled);
    tracing::subscriber::set_global_default(subscriber).or_fail()?;

    run_thread_that_records_while_tearing_down()?;

    // Only the teardown event is recorded.
    let names = decode_track_event_names(&output.buf.lock()).or_fail()?;
    expect_that!(names, elements_are![starts_with("event ")]);
    Ok(())
}
