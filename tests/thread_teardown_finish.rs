//! A sampled trace finished during a thread's teardown, from a thread-local destructor and, on
//! Unix, from a pthread key destructor, which on Linux runs after the standard library has
//! destroyed the thread's handle.

use googletest::prelude::*;
use parking_lot::Mutex;
use std::sync::{Arc, mpsc};
use std::time::Duration;
use tracing_perfetto::process_memory::ProcessMemorySource;
use tracing_perfetto::{FinishHandle, PerfettoLayer, decode_track_event_names};
use tracing_subscriber::{fmt::MakeWriter, layer::SubscriberExt};

/// The bound on waiting for the sampler, far above any scheduling delay on a loaded machine.
///
/// A sampler that never samples exceeds it, which fails the test instead of hanging it.
const BOUND: Duration = Duration::from_secs(30);

/// The name of the span that the tearing-down thread records before it exits.
const SPAN_NAME: &str = "before teardown";

/// What the layer asked of its writer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WriterCall {
    Write,
    Flush,
}

/// A sink that a [`PerfettoLayer`] writes to, accumulating the trace and its calls in memory.
#[derive(Clone, Default)]
struct TestWriter {
    buf: Arc<Mutex<Vec<u8>>>,
    calls: Arc<Mutex<Vec<WriterCall>>>,
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
        self.calls.lock().push(WriterCall::Write);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.calls.lock().push(WriterCall::Flush);
        Ok(())
    }
}

/// A memory source that announces each read.
struct AnnouncingSource {
    announcements: mpsc::Sender<()>,
}

impl ProcessMemorySource for AnnouncingSource {
    fn resident_set_size_bytes(&self) -> Option<u64> {
        // The receiver is gone only after the test stopped waiting for reads.
        self.announcements.send(()).ok();
        Some(1024)
    }
}

/// A trace whose layer samples process memory, inside a dispatcher that the test keeps alive.
struct SampledTrace {
    dispatch: tracing::Dispatch,
    finish: FinishHandle,
    output: TestWriter,
}

/// Returns a trace whose sampler has written its first sample.
fn sampled_trace() -> Result<SampledTrace> {
    let writer = TestWriter::default();
    let output = writer.clone();
    let (announcements, announced) = mpsc::channel();
    let layer = PerfettoLayer::new(writer)
        .with_process_memory_source(Duration::ZERO, Box::new(AnnouncingSource { announcements }));
    let finish = layer.finish_handle();
    // The sampler writes the sample of a read before it reads again.
    for _ in 0..2 {
        announced.recv_timeout(BOUND).or_fail()?;
    }
    Ok(SampledTrace {
        dispatch: tracing::Dispatch::new(tracing_subscriber::registry().with(layer)),
        finish,
        output,
    })
}

/// Finishes a trace and keeps the outcome for the test, which reads it once the finishing thread
/// has exited.
struct Finisher {
    finish: FinishHandle,
    outcome: Arc<Mutex<Option<std::io::Result<()>>>>,
}

impl Finisher {
    fn finish(&self) {
        *self.outcome.lock() = Some(self.finish.finish());
    }
}

/// Runs a thread that records [`SPAN_NAME`] into `trace`, hands `install` a [`Finisher`] that it
/// arranges to run during the thread's teardown, and exits, and returns the finish's outcome.
fn finish_during_teardown(
    trace: &SampledTrace,
    install: impl FnOnce(Finisher) -> Result<()> + Send + 'static,
) -> Result<std::io::Result<()>> {
    let outcome = Arc::new(Mutex::new(None));
    let finisher = Finisher {
        finish: trace.finish.clone(),
        outcome: Arc::clone(&outcome),
    };
    let dispatch = trace.dispatch.clone();
    std::thread::Builder::new()
        .name("tearing down".to_string())
        .spawn(move || {
            tracing::dispatcher::with_default(&dispatch, || {
                tracing::info_span!(SPAN_NAME).in_scope(|| {});
            });
            install(finisher)
        })
        .or_fail()?
        .join()
        .or_fail()??;
    // A thread's teardown is complete once a join of it returns.
    let outcome = outcome.lock().take();
    outcome.or_fail()
}

/// Expects the output of `trace` to be complete: it holds the span recorded before the teardown,
/// and the finish flushed the writer after its last write.
fn expect_complete(trace: &SampledTrace) -> Result<()> {
    let names = decode_track_event_names(&trace.output.buf.lock()).or_fail()?;
    expect_that!(names, contains(eq(SPAN_NAME)));
    let calls = trace.output.calls.lock();
    expect_that!(calls.last(), some(eq(&WriterCall::Flush)));
    expect_that!(
        calls
            .iter()
            .filter(|&&call| call == WriterCall::Write)
            .count(),
        ge(2)
    );
    Ok(())
}

/// Holds the [`Finisher`] that the calling thread runs while it destroys its thread-local values.
struct FinishesOnDrop(Option<Finisher>);

impl Drop for FinishesOnDrop {
    fn drop(&mut self) {
        if let Some(finisher) = self.0.take() {
            finisher.finish();
        }
    }
}

thread_local! {
    static FINISHES_ON_DROP: std::cell::RefCell<FinishesOnDrop> =
        const { std::cell::RefCell::new(FinishesOnDrop(None)) };
}

/// A sampled trace that a thread-local destructor finishes is complete, and the finish succeeds.
///
/// The finish stops the sampler from a thread that destroys its thread-local values, so a lookup
/// of the calling thread through one of them panics there, which aborts the process.
/// The standard library destroys the thread's handle only after these values, so this finish
/// still runs while `std::thread::current` works.
#[gtest]
fn a_thread_local_destructor_finishes_a_sampled_trace() -> Result<()> {
    let trace = sampled_trace()?;

    let outcome = finish_during_teardown(&trace, |finisher| {
        FINISHES_ON_DROP.with(|slot| slot.borrow_mut().0 = Some(finisher));
        Ok(())
    })?;

    expect_that!(outcome, ok(eq(&())));
    expect_complete(&trace)
}

/// Finishing from a pthread key destructor, which on Linux runs after the standard library has
/// destroyed the thread's handle.
#[cfg(unix)]
mod after_thread_handle {
    use super::{Finisher, expect_complete, finish_during_teardown, sampled_trace};
    use googletest::prelude::*;
    use std::ffi::c_void;

    /// A pthread key whose destructor runs a [`Finisher`] in the second round of key
    /// destructors.
    struct FinishingKey(libc::pthread_key_t);

    impl FinishingKey {
        #[expect(
            unsafe_code,
            reason = "only a pthread key destructor runs after std's own cleanup at thread exit"
        )]
        fn create() -> Result<Self> {
            let mut key = 0;
            // SAFETY: `key` is valid for the write, and every value that the key holds is a
            // `Deferred` of `Self::defer`, which `run_deferred` takes back.
            let created = unsafe { libc::pthread_key_create(&raw mut key, Some(run_deferred)) };
            verify_eq!(created, 0)?;
            Ok(Self(key))
        }

        /// Makes the calling thread run `finisher` in the second round of its key destructors.
        fn defer(&self, finisher: Finisher) -> Result<()> {
            let deferred = Box::new(Deferred {
                key: self.0,
                rounds_left: 1,
                finisher,
            });
            if set_deferred(deferred).is_err() {
                return fail!("the thread's value of the key could not be set");
            }
            Ok(())
        }
    }

    impl Drop for FinishingKey {
        #[expect(unsafe_code, reason = "a pthread key is freed only through its C API")]
        fn drop(&mut self) {
            // SAFETY: the key was created by `Self::create`, and the threads that used it have
            // exited, so none of its destructors runs any more.
            unsafe { libc::pthread_key_delete(self.0) };
        }
    }

    /// The value that a [`FinishingKey`] holds on a thread.
    struct Deferred {
        key: libc::pthread_key_t,
        /// The destructor rounds to wait before running `finisher`.
        rounds_left: u8,
        finisher: Finisher,
    }

    /// Stores `deferred` as the calling thread's value of its key, and hands it back if that
    /// fails.
    #[expect(
        unsafe_code,
        reason = "a pthread key holds its value through its C API"
    )]
    fn set_deferred(deferred: Box<Deferred>) -> std::result::Result<(), Box<Deferred>> {
        let key = deferred.key;
        let value = Box::into_raw(deferred);
        // SAFETY: the key was created by `FinishingKey::create` and is not deleted while the
        // thread runs.
        if unsafe { libc::pthread_setspecific(key, value.cast()) } == 0 {
            return Ok(());
        }
        // SAFETY: `value` came from `Box::into_raw` above, and the key does not hold it.
        Err(unsafe { Box::from_raw(value) })
    }

    /// Runs the [`Finisher`] of a [`Deferred`] once its rounds have passed, and defers it to the
    /// next round until then.
    ///
    /// The system clears the thread's value of a key before it calls the key's destructor, and
    /// calls the destructors again, up to `PTHREAD_DESTRUCTOR_ITERATIONS` rounds, as long as one
    /// of them sets a value.
    #[expect(
        unsafe_code,
        reason = "a pthread key destructor receives its value as a raw pointer"
    )]
    extern "C" fn run_deferred(value: *mut c_void) {
        // SAFETY: the key holds only values that `set_deferred` stored, each of which it hands to
        // this destructor once, after clearing it.
        let mut deferred = unsafe { Box::from_raw(value.cast::<Deferred>()) };
        if deferred.rounds_left == 0 {
            deferred.finisher.finish();
            return;
        }
        deferred.rounds_left -= 1;
        // A value that cannot be stored drops its finisher unfinished, which the test reports as
        // a missing outcome.
        set_deferred(deferred).ok();
    }

    /// A sampled trace that a thread finishes after the standard library destroyed its handle is
    /// complete, and the finish succeeds.
    ///
    /// On Linux, the standard library destroys a thread's handle in the first round of pthread
    /// key destructors, from its own key's destructor, so the finish runs in the second round.
    /// `std::thread::current` panics from then on, so a finish that looks up the calling
    /// thread's identity through it to tell whether it is the sampler thread aborts the process.
    #[gtest]
    fn a_destructor_after_the_thread_handle_finishes_a_sampled_trace() -> Result<()> {
        let trace = sampled_trace()?;
        let key = std::sync::Arc::new(FinishingKey::create()?);

        let outcome = finish_during_teardown(&trace, {
            let key = std::sync::Arc::clone(&key);
            move |finisher| key.defer(finisher)
        })?;

        expect_that!(outcome, ok(eq(&())));
        expect_complete(&trace)
    }
}
