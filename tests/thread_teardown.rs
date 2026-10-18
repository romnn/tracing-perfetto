//! Events recorded while a thread's thread-local values are destroyed, through a test-owned
//! global subscriber.

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

/// A thread-local value that records an event when its thread destroys it.
struct RecordsOnDrop;

impl Drop for RecordsOnDrop {
    fn drop(&mut self) {
        tracing::info!(parent: None, during_teardown = true);
    }
}

thread_local! {
    static RECORDS_ON_DROP: RecordsOnDrop = const { RecordsOnDrop };
}

/// An event recorded by a thread-local destructor, after the layer's per-thread state is gone,
/// is written without a panic.
///
/// The value is created before the thread first records, so it is destroyed after the
/// thread-local values that the layer and its per-thread storage rely on.
/// Looking up the layer's per-thread state then would panic inside a thread-local destructor,
/// which aborts the process.
/// The global subscriber reaches the destructor without any thread-local dispatcher state.
#[gtest]
fn event_recorded_during_thread_teardown_is_written() -> Result<()> {
    let writer = TestWriter::default();
    let output = writer.clone();
    let subscriber = tracing_subscriber::registry().with(PerfettoLayer::new(writer));
    tracing::subscriber::set_global_default(subscriber).or_fail()?;

    std::thread::Builder::new()
        .name("tearing down".to_string())
        .spawn(|| {
            RECORDS_ON_DROP.with(|_| {});
            tracing::info!(parent: None, before_teardown = true);
        })
        .or_fail()?
        .join()
        .or_fail()?;

    // Both events are on the thread's track, the second written during its teardown.
    let names = decode_track_event_names(&output.buf.lock()).or_fail()?;
    expect_that!(names, len(eq(2)));
    Ok(())
}
