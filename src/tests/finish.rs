//! Regression tests for the end of a trace: a finish waits for the writes already under way,
//! nothing is written after it, and the writer is flushed, also when the layer is dropped.

use crate::process_memory::ProcessMemorySource;
use crate::process_memory::sampler::MIN_SAMPLING_INTERVAL;
use crate::tests::{ReplayedSlice, events_of_type, replay_lifetime_slices, wait_until};
use crate::{FinishHandle, PerfettoLayer, idl};
use googletest::prelude::*;
use parking_lot::Mutex;
use prost::Message;
use std::sync::{Arc, OnceLock, mpsc};
use std::time::Duration;
use tracing::{info, info_span};
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::layer::SubscriberExt;

/// The bound on waiting for a finish or a held write, far above any scheduling delay on a loaded
/// machine.
///
/// A finish that deadlocks exceeds it, which fails the test instead of hanging the suite.
const FINISH_BOUND: Duration = Duration::from_secs(30);

/// The message of the error that a failing [`BufferingWriter`] returns from a flush.
const FLUSH_ERROR: &str = "injected flush failure";

/// The number of root spans that [`record_jobs`] records, each with one child.
const JOBS: usize = 3;

/// An in-memory writer that, like a [`std::io::BufWriter`], holds what it is given until it is
/// flushed, and shows only the flushed bytes.
///
/// It is not flushed when it is dropped, as a writer that the layer shares with its owner, such
/// as an `Arc` of a `BufWriter`, outlives the layer, so only the layer's own flush releases the
/// end of the trace.
struct BufferingWriter {
    pending: Vec<u8>,
    flushed: Arc<Mutex<Vec<u8>>>,
    /// Whether a flush fails and keeps the pending bytes.
    flush_fails: bool,
}

impl std::io::Write for BufferingWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.pending.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        if self.flush_fails {
            return Err(std::io::Error::other(FLUSH_ERROR));
        }
        self.flushed.lock().append(&mut self.pending);
        Ok(())
    }
}

/// Returns a layer that writes to a [`BufferingWriter`] behind a `std::sync::Mutex`, as a
/// `Mutex<BufWriter<File>>` would be, and the bytes that the writer has flushed.
///
/// The `std::sync::Mutex` is deliberate: it is the writer type that tracing-subscriber implements
/// [`MakeWriter`] for, so the layer flushes it as it flushes a user's buffered file.
fn buffering_layer(
    flush_fails: bool,
) -> (
    PerfettoLayer<std::sync::Mutex<BufferingWriter>>,
    Arc<Mutex<Vec<u8>>>,
) {
    let flushed = Arc::new(Mutex::new(Vec::new()));
    let writer = BufferingWriter {
        pending: Vec::new(),
        flushed: Arc::clone(&flushed),
        flush_fails,
    };
    (PerfettoLayer::new(std::sync::Mutex::new(writer)), flushed)
}

/// Records [`JOBS`] root spans named `job`, each with a child named `step` and an event inside
/// it.
fn record_jobs() {
    for _ in 0..JOBS {
        info_span!("job").in_scope(|| info_span!("step").in_scope(|| info!("working")));
    }
}

/// Checks that `bytes` hold the whole trace of [`record_jobs`]: every lifetime slice begins and
/// ends.
fn expect_every_job_recorded(bytes: &[u8]) -> Result<()> {
    let trace = idl::Trace::decode(bytes).or_fail()?;
    let slices = replay_lifetime_slices(&trace)?;
    let names: Vec<&str> = slices.iter().map(|slice| slice.name.as_str()).collect();
    let jobs = names.iter().filter(|&&name| name == "job").count();
    let steps = names.iter().filter(|&&name| name == "step").count();
    verify_that!((jobs, steps), eq((JOBS, JOBS)))?;
    let open: Vec<&str> = slices
        .iter()
        .filter(|slice| slice.end_timestamp.is_none())
        .map(|slice| slice.name.as_str())
        .collect();
    verify_that!(open, is_empty())
}

/// Finishing a trace written to a buffering writer flushes it, so the output holds the whole
/// trace while the layer is still installed.
///
/// A global dispatcher never drops its layer, so a finish that did not flush would leave the end
/// of the trace in the buffer, and a short trace with no bytes in the output at all.
#[gtest]
fn finish_flushes_a_buffering_writer() -> Result<()> {
    let (layer, flushed) = buffering_layer(false);
    let finish = layer.finish_handle();
    // The dispatch keeps the layer alive, as a global dispatcher would.
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(layer));
    tracing::dispatcher::with_default(&dispatch, record_jobs);

    finish.finish().or_fail()?;

    expect_every_job_recorded(&flushed.lock())?;
    drop(dispatch);
    Ok(())
}

/// Dropping a layer that writes to a buffering writer flushes it, so the output holds the whole
/// trace.
///
/// A drop that only stopped the layer would leave the end of the trace in a writer that outlives
/// the layer.
#[gtest]
fn dropping_the_layer_flushes_a_buffering_writer() -> Result<()> {
    let (layer, flushed) = buffering_layer(false);

    // The subscriber, and with it the layer, is dropped once the closure returns.
    tracing::subscriber::with_default(tracing_subscriber::registry().with(layer), record_jobs);

    expect_every_job_recorded(&flushed.lock())
}

/// A flush that fails is reported by the finish that flushed, and only by that one.
///
/// A finish that discarded the flush error would report a trace whose end never reached the
/// output as complete.
#[gtest]
fn finish_returns_the_flush_error() {
    let (layer, _) = buffering_layer(true);
    let finish = layer.finish_handle();
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(layer));
    tracing::dispatcher::with_default(&dispatch, record_jobs);

    expect_that!(finish.finish(), err(displays_as(eq(FLUSH_ERROR))));
    expect_that!(finish.finish(), ok(anything()));
    drop(dispatch);
}

/// The message of the error that a [`FailingFlushWriter`] returns from every write.
const WRITE_ERROR: &str = "injected write failure";

/// The shared state of a [`FailingFlushWriter`]: the finish it performs from inside its flush,
/// and what that finish returned.
struct FailingFlushState {
    /// The handle of the trace that the writer writes, set once the layer is built.
    finish: OnceLock<FinishHandle>,
    /// The result of the finish from inside the flush, once the flush ran.
    nested_finish: Mutex<Option<std::io::Result<()>>>,
}

/// A writer whose writes fail with [`WRITE_ERROR`], and whose flush finishes the trace before
/// it fails with [`FLUSH_ERROR`].
struct FailingFlushWriter {
    state: Arc<FailingFlushState>,
}

impl<'a> MakeWriter<'a> for FailingFlushWriter {
    type Writer = FailingFlushWriter;

    fn make_writer(&'a self) -> Self::Writer {
        FailingFlushWriter {
            state: Arc::clone(&self.state),
        }
    }
}

impl std::io::Write for FailingFlushWriter {
    fn write(&mut self, _buf: &[u8]) -> std::io::Result<usize> {
        Err(std::io::Error::other(WRITE_ERROR))
    }

    fn flush(&mut self) -> std::io::Result<()> {
        if let Some(finish) = self.state.finish.get() {
            let nested_finish = finish.finish();
            *self.state.nested_finish.lock() = Some(nested_finish);
        }
        Err(std::io::Error::other(FLUSH_ERROR))
    }
}

/// A writer that finishes the trace from inside the flush that follows a failed write, and whose
/// flush then fails too, gets only the write's error reported, once across all finishes.
///
/// A finish from inside the flush that reported the write's error would leave no error behind,
/// so the flush's error would count as the first failure and be reported a second time.
#[gtest]
fn a_finish_inside_a_failing_flush_reports_only_the_first_failure() -> Result<()> {
    let state = Arc::new(FailingFlushState {
        finish: OnceLock::new(),
        nested_finish: Mutex::new(None),
    });
    let layer = PerfettoLayer::new(FailingFlushWriter {
        state: Arc::clone(&state),
    });
    state.finish.set(layer.finish_handle()).or_fail()?;
    let finish = layer.finish_handle();
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(layer));

    // The event's write fails, then the finish flushes, and the flush finishes the trace too.
    tracing::dispatcher::with_default(&dispatch, || info!("lost"));
    let outer_finish = finish.finish();
    let later_finish = finish.finish();
    let nested_finish = state.nested_finish.lock().take().or_fail()?;
    drop(dispatch);

    // The write's error is reported once, and the flush's error never.
    let reported: Vec<String> = [nested_finish, outer_finish, later_finish]
        .into_iter()
        .filter_map(|result| result.err().map(|err| err.to_string()))
        .collect();
    verify_that!(reported, elements_are![eq(WRITE_ERROR)])
}

/// The output that [`HoldingWriter`] shares with the test, and the channels that hold its write.
struct HoldingState {
    output: Mutex<Vec<u8>>,
    /// Announces, once, that a write holds a message.
    held: Mutex<Option<mpsc::Sender<()>>>,
    /// Releases the held write, behind a mutex because the writer must be [`Sync`].
    release: Mutex<mpsc::Receiver<()>>,
}

/// A writer that holds the first message that ends a slice inside its write until the test
/// releases it, and writes every message in full.
struct HoldingWriter {
    state: Arc<HoldingState>,
}

impl<'a> MakeWriter<'a> for HoldingWriter {
    type Writer = HoldingWriter;

    fn make_writer(&'a self) -> Self::Writer {
        HoldingWriter {
            state: Arc::clone(&self.state),
        }
    }
}

impl std::io::Write for HoldingWriter {
    /// Appends `buf`, after holding it until the test releases it if it is the first message
    /// that ends a slice.
    ///
    /// The layer hands each message to one call, so `buf` is one whole `Trace` message.
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let ends_a_slice = idl::Trace::decode(buf).is_ok_and(|trace| {
            events_of_type(&trace, idl::track_event::Type::SliceEnd)
                .next()
                .is_some()
        });
        let held = ends_a_slice
            .then(|| self.state.held.lock().take())
            .flatten();
        if let Some(held) = held {
            // The receiver is gone only after the test stopped waiting, and the bound keeps a
            // test that never releases the write from hanging.
            held.send(()).ok();
            self.state.release.lock().recv_timeout(FINISH_BOUND).ok();
        }
        self.state.output.lock().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        // Writes append to the output immediately, so there is nothing to flush.
        Ok(())
    }
}

/// What a test observed right after a finish on another thread returned.
struct FinishReturn {
    /// The length of the output.
    written: usize,
    result: std::io::Result<()>,
}

/// A finish while another thread's write is held inside the writer returns only after that write
/// landed, and nothing is written after the finish returned.
///
/// A finish that only stopped new writes would return while the held write, a slice's end, can
/// still append to the output, so a reader of the finished output would miss it, and a later
/// write would land after the finish.
#[gtest]
fn finish_waits_for_a_write_in_progress_and_nothing_follows() -> Result<()> {
    let (held, held_receiver) = mpsc::channel();
    let (release, release_receiver) = mpsc::channel();
    let state = Arc::new(HoldingState {
        output: Mutex::new(Vec::new()),
        held: Mutex::new(Some(held)),
        release: Mutex::new(release_receiver),
    });
    let layer = PerfettoLayer::new(HoldingWriter {
        state: Arc::clone(&state),
    });
    let finish = layer.finish_handle();
    let observer = layer.finish_handle();
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(layer));

    // Another thread closes a span, and the writer holds the end of its slice.
    std::thread::spawn({
        let dispatch = dispatch.clone();
        move || tracing::dispatcher::with_default(&dispatch, || drop(info_span!("held")))
    });
    held_receiver.recv_timeout(FINISH_BOUND).or_fail()?;

    // Finish on a third thread while the end is held.
    let (returned, returns) = mpsc::channel();
    std::thread::spawn({
        let state = Arc::clone(&state);
        move || {
            let result = finish.finish();
            let finish_return = FinishReturn {
                written: state.output.lock().len(),
                result,
            };
            returned.send(finish_return).ok();
        }
    });

    // The finish waits for the held write before the test releases it, and has not returned.
    if !wait_until(FINISH_BOUND, || observer.sink_close_contentions() == 1) {
        std::mem::forget(dispatch);
        return fail!("the finish never waited for the write in progress");
    }
    let early_return = returns.try_recv();
    expect_that!(
        early_return.is_ok(),
        eq(false),
        "the finish returned while a write was held"
    );
    release.send(()).or_fail()?;
    let Ok(finish_return) = early_return.or_else(|_| returns.recv_timeout(FINISH_BOUND)) else {
        // Dropping the layer would finish it too, which waits for the stuck finish.
        std::mem::forget(dispatch);
        return fail!("the finish did not return after the write was released");
    };

    // A span recorded after the finish writes nothing.
    tracing::dispatcher::with_default(&dispatch, || drop(info_span!("after_finish")));
    drop(dispatch);

    // The finish returned after the held end landed, and nothing reached the writer after it.
    expect_that!(finish_return.result, ok(anything()));
    let output = state.output.lock();
    expect_eq!(finish_return.written, output.len());
    let trace = idl::Trace::decode(output.as_slice()).or_fail()?;
    let slices = replay_lifetime_slices(&trace)?;
    verify_that!(
        slices,
        elements_are![all![
            field!(ReplayedSlice.name, eq("held")),
            field!(ReplayedSlice.end_timestamp, some(anything())),
        ]]
    )
}

/// The thread on which [`FinishingWriter`] finishes the trace.
const FINISHER_THREAD: &str = "perfetto-test-finisher";

/// The shared state of a [`FinishingWriter`]: a buffering output as in [`BufferingWriter`], and
/// the finish it performs from inside a write.
struct FinishingState {
    pending: Mutex<Vec<u8>>,
    flushed: Mutex<Vec<u8>>,
    /// The handle of the trace that the writer writes, set once the layer is built.
    finish: OnceLock<FinishHandle>,
    /// Sends the result of the writer's own finish, once.
    finished: Mutex<Option<mpsc::Sender<std::io::Result<()>>>>,
}

/// A buffering writer that finishes the trace from inside its first write on the thread named
/// [`FINISHER_THREAD`].
struct FinishingWriter {
    state: Arc<FinishingState>,
}

impl<'a> MakeWriter<'a> for FinishingWriter {
    type Writer = FinishingWriter;

    fn make_writer(&'a self) -> Self::Writer {
        FinishingWriter {
            state: Arc::clone(&self.state),
        }
    }
}

impl std::io::Write for FinishingWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let on_finisher_thread = std::thread::current().name() == Some(FINISHER_THREAD);
        if on_finisher_thread
            && let Some(finish) = self.state.finish.get()
            && let Some(finished) = self.state.finished.lock().take()
        {
            // The receiver is gone only after the test stopped waiting.
            finished.send(finish.finish()).ok();
        }
        self.state.pending.lock().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let mut pending = self.state.pending.lock();
        self.state.flushed.lock().append(&mut pending);
        Ok(())
    }
}

/// A memory source that always reports 1 KiB.
struct FixedProcessMemory;

impl ProcessMemorySource for FixedProcessMemory {
    fn resident_set_size_bytes(&self) -> Option<u64> {
        Some(1024)
    }
}

/// A writer that finishes the trace from inside a write on a thread other than the sampler's
/// gets the finish's result back, and the trace ends, flushed, once that write returns.
///
/// The write holds the lock that every write of the trace takes, and the sampler may wait for
/// it, so a finish that waited for the sampler, or for the lock to end the trace, would wait for
/// the very write that called it.
#[gtest]
fn finish_from_inside_a_write_ends_the_trace_after_that_write() -> Result<()> {
    let (finished, writer_finished) = mpsc::channel();
    let state = Arc::new(FinishingState {
        pending: Mutex::new(Vec::new()),
        flushed: Mutex::new(Vec::new()),
        finish: OnceLock::new(),
        finished: Mutex::new(Some(finished)),
    });
    let layer = PerfettoLayer::new(FinishingWriter {
        state: Arc::clone(&state),
    })
    .with_process_memory_source(MIN_SAMPLING_INTERVAL, Box::new(FixedProcessMemory));
    let layer_sink = Arc::clone(layer.sink());
    state.finish.set(layer.finish_handle()).or_fail()?;
    let finish = layer.finish_handle();
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(layer));

    // The finisher records an event, whose write finishes the trace.
    let (recorded, finisher_recorded) = mpsc::channel();
    std::thread::Builder::new()
        .name(FINISHER_THREAD.to_string())
        .spawn({
            let dispatch = dispatch.clone();
            move || {
                tracing::dispatcher::with_default(&dispatch, || info!("finishing"));
                recorded.send(()).ok();
            }
        })
        .or_fail()?;
    let Ok(writer_finish) = writer_finished.recv_timeout(FINISH_BOUND) else {
        // Dropping the layer would finish it too, which waits for the stuck finish.
        std::mem::forget(dispatch);
        return fail!("the writer's finish deadlocked");
    };
    if finisher_recorded.recv_timeout(FINISH_BOUND).is_err() {
        std::mem::forget(dispatch);
        return fail!("the write that finished the trace never returned");
    }

    // The trace ended once the write returned: it was flushed, and nothing is written after it.
    let flushed = state.flushed.lock().clone();
    tracing::dispatcher::with_default(&dispatch, || info!("after_finish"));
    expect_that!(writer_finish, ok(anything()));
    expect_that!(state.pending.lock().as_slice(), is_empty());
    expect_eq!(state.flushed.lock().as_slice(), flushed.as_slice());
    // The event whose write finished the trace is its only instant.
    let trace = idl::Trace::decode(flushed.as_slice()).or_fail()?;
    expect_eq!(
        events_of_type(&trace, idl::track_event::Type::Instant).count(),
        1
    );

    // A later finish waits for the sampler, which released the sink once it stopped.
    let (returned, finish_returned) = mpsc::channel();
    std::thread::spawn(move || {
        returned.send(finish.finish()).ok();
    });
    let Ok(later_finish) = finish_returned.recv_timeout(FINISH_BOUND) else {
        std::mem::forget(dispatch);
        return fail!("the finish after the writer's finish deadlocked");
    };
    expect_that!(later_finish, ok(anything()));
    expect_eq!(Arc::strong_count(&layer_sink), 2);
    drop(dispatch);
    Ok(())
}
