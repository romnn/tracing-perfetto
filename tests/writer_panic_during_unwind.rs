//! A writer that panics while its thread unwinds from another panic, in a test binary of its
//! own, because a panic that escapes the layer there aborts the process.

use googletest::prelude::*;
use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tracing_perfetto::{PerfettoLayer, decode_track_event_names};
use tracing_subscriber::{fmt::MakeWriter, layer::SubscriberExt};

/// The payload of the application's panic, which unwinds through the layer.
const APPLICATION_PANIC: &str = "injected application panic";

/// The payload of the writer's panic.
const WRITER_PANIC: &str = "injected writer panic";

/// The bound on waiting for another thread to let go of a dispatch, far above any scheduling
/// delay on a loaded machine.
const RELEASE_BOUND: Duration = Duration::from_secs(30);

/// A sink that accumulates the trace in memory, and panics in a write once `write_panics` is set
/// or in every flush if `flush_panics` is.
#[derive(Clone, Default)]
struct PanickingWriter {
    buf: Arc<Mutex<Vec<u8>>>,
    write_panics: Arc<AtomicBool>,
    flush_panics: bool,
}

impl<'a> MakeWriter<'a> for PanickingWriter {
    type Writer = PanickingWriter;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

impl std::io::Write for PanickingWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if self.write_panics.load(Ordering::Relaxed) {
            #[expect(
                clippy::panic,
                reason = "the writer's panic is the behavior under test, and clippy allows \
                          panics only inside test functions, not in the writer they install"
            )]
            std::panic::panic_any(WRITER_PANIC);
        }
        self.buf.lock().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        if self.flush_panics {
            #[expect(
                clippy::panic,
                reason = "the writer's panic is the behavior under test, and clippy allows \
                          panics only inside test functions, not in the writer they install"
            )]
            std::panic::panic_any(WRITER_PANIC);
        }
        Ok(())
    }
}

/// Checks that `result` is the application's panic, which the writer's panic did not replace.
fn expect_application_panic<T>(result: std::thread::Result<T>) -> Result<()> {
    let Err(payload) = result else {
        return fail!("the application's panic did not reach the caller");
    };
    verify_that!(payload.downcast_ref::<&str>(), some(eq(&APPLICATION_PANIC)))
}

/// A layer that an application's panic drops while it unwinds, and whose final flush panics,
/// leaves the trace written so far and lets the application's panic reach its caller.
///
/// Dropping the layer flushes the writer, and a panic that escaped that drop while another
/// panic unwinds would abort the process.
#[gtest]
fn a_flush_that_panics_while_a_panic_drops_the_layer_does_not_abort() -> Result<()> {
    let writer = PanickingWriter {
        flush_panics: true,
        ..PanickingWriter::default()
    };
    let output = writer.clone();
    // tracing briefly holds every dispatcher from a thread that creates a dispatcher or registers
    // a callsite, such as the other test's, which may then drop the layer outside the unwind.
    // The dispatch therefore holds the subscriber through an `Arc`, whose last handle the unwind
    // drops once no other thread holds the dispatch.
    let subscriber = Arc::new(tracing_subscriber::registry().with(PerfettoLayer::new(writer)));
    let dispatch = tracing::Dispatch::new(Arc::clone(&subscriber));
    tracing::dispatcher::with_default(&dispatch, || tracing::info!("before_the_panic"));
    drop(dispatch);
    let deadline = Instant::now() + RELEASE_BOUND;
    while Arc::strong_count(&subscriber) > 1 {
        if Instant::now() >= deadline {
            return fail!("another thread kept the dispatch");
        }
        std::thread::yield_now();
    }

    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let _dropped_by_the_unwind = subscriber;
        std::panic::panic_any(APPLICATION_PANIC);
    }));

    expect_application_panic(panicked)?;
    let names = decode_track_event_names(&output.buf.lock()).or_fail()?;
    verify_that!(names, len(eq(1)))
}

/// A span end whose write panics while an application's panic unwinds through the span's guard
/// ends the trace, and lets the application's panic reach its caller.
///
/// The unwind writes the span's exit from the guard's destructor, and a panic that escaped that
/// write would abort the process.
#[gtest]
fn a_span_end_whose_write_panics_while_a_panic_unwinds_does_not_abort() -> Result<()> {
    let writer = PanickingWriter::default();
    let output = writer.clone();
    let write_panics = Arc::clone(&writer.write_panics);
    let layer = PerfettoLayer::new(writer);
    let finish = layer.finish_handle();
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(layer));

    let panicked = tracing::dispatcher::with_default(&dispatch, || {
        std::panic::catch_unwind(|| {
            let _unwound = tracing::info_span!("unwound").entered();
            write_panics.store(true, Ordering::Relaxed);
            std::panic::panic_any(APPLICATION_PANIC);
        })
    });

    expect_application_panic(panicked)?;
    verify_that!(
        finish.finish(),
        err(displays_as(eq("the trace's writer panicked")))
    )?;
    // The span's lifetime slice and its entered slice began before the writer panicked.
    let names = decode_track_event_names(&output.buf.lock()).or_fail()?;
    verify_that!(names, elements_are![eq("unwound"), eq("unwound")])?;
    drop(dispatch);
    Ok(())
}
