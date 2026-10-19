//! A writer that records an event while the layer calls it, through a test-owned global
//! subscriber.
//!
//! A scoped dispatcher hands a nested record no subscriber, so only the global default reaches
//! the layer again from inside its own write, as a panic hook that records the writer's panic
//! does.

use googletest::prelude::*;
use parking_lot::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::time::Duration;
use tracing_perfetto::{PerfettoLayer, decode_track_event_names};
use tracing_subscriber::{fmt::MakeWriter, layer::SubscriberExt};

/// The bound on waiting for the record whose write records another event, far above any
/// scheduling delay on a loaded machine.
///
/// A write that deadlocks exceeds it, which fails the test instead of hanging the suite.
const RECORD_BOUND: Duration = Duration::from_secs(30);

/// A sink that accumulates the trace in memory, and records an event from inside its first write.
#[derive(Clone, Default)]
struct RecordingWriter {
    buf: Arc<Mutex<Vec<u8>>>,
    recorded: Arc<AtomicBool>,
}

impl<'a> MakeWriter<'a> for RecordingWriter {
    type Writer = RecordingWriter;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

impl std::io::Write for RecordingWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if !self.recorded.swap(true, Ordering::Relaxed) {
            tracing::info!(name: "from_the_writer", "recorded inside a write");
        }
        self.buf.lock().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// An event that the writer records while the layer calls it is dropped, and the write that
/// called the writer, and every later one, completes.
///
/// The layer calls the writer while holding the lock that every write of the trace takes, so a
/// write of the writer's own event that waited for that lock would wait for itself.
#[gtest]
fn an_event_that_the_writer_records_is_dropped_instead_of_deadlocking() -> Result<()> {
    let writer = RecordingWriter::default();
    let output = writer.clone();
    let layer = PerfettoLayer::new(writer);
    let finish = layer.finish_handle();
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(layer))
        .or_fail()?;

    // The first write, which records the writer's event, runs on a thread of its own, so that a
    // deadlock fails the test instead of hanging it.
    let (recorded, first_recorded) = mpsc::channel();
    std::thread::spawn(move || {
        tracing::info!(name: "first", "recorded by the test");
        recorded.send(()).ok();
    });
    if first_recorded.recv_timeout(RECORD_BOUND).is_err() {
        return fail!("the write of the writer's own event deadlocked");
    }
    tracing::info!(name: "after", "recorded by the test");

    verify_that!(finish.finish(), ok(anything()))?;
    let names = decode_track_event_names(&output.buf.lock()).or_fail()?;
    verify_that!(names, elements_are![eq("first"), eq("after")])
}
