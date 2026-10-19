//! Regression tests for finishing a trace while its sampler is mid-sample: from the sampler
//! thread itself, and from several threads at once.

use crate::process_memory::ProcessMemorySource;
use crate::process_memory::sampler::MIN_SAMPLING_INTERVAL;
use crate::tests::{TestWriter, decode, events_of_type, wait_until};
use crate::{FinishHandle, PerfettoLayer, idl};
use googletest::prelude::*;
use parking_lot::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock, mpsc};
use std::time::Duration;
use tracing_subscriber::fmt::MakeWriter;

/// The bound on waiting for a finish, far above any scheduling delay on a loaded machine.
///
/// A finish that deadlocks exceeds it, which fails the test instead of hanging the suite.
const FINISH_BOUND: Duration = Duration::from_secs(30);

/// The value of the sample written before the sampler pauses.
const FIRST_SAMPLE: u64 = 1024;

/// The value of the sample read while the trace finishes, which is never written.
const SECOND_SAMPLE: u64 = 2048;

/// When a [`PausingProcessMemory`] finishes the trace itself, once the test released it.
enum SourceFinish {
    /// The source never finishes the trace.
    Never,
    /// The source finishes the trace before anything else does.
    First,
    /// The source finishes the trace while another thread's finish joins the sampler.
    DuringAnotherFinish,
}

/// A memory source that reports [`FIRST_SAMPLE`], then pauses in its second sample until the
/// test releases it, and finishes the trace from the sampler thread as `source_finish` says.
///
/// The sampler writes a sample before it reads the next one, so the pause proves that the first
/// sample reached the writer.
/// The sampler thread drops its source when it exits, which sets `sampler_exited`.
struct PausingProcessMemory {
    samples: AtomicUsize,
    paused: mpsc::Sender<()>,
    /// The test's release, behind a mutex because the source must be [`Sync`].
    release: Mutex<mpsc::Receiver<()>>,
    source_finish: SourceFinish,
    /// The handle of the trace that the source samples for, set once the layer is built.
    finish: Arc<OnceLock<FinishHandle>>,
    output: TestWriter,
    /// Sends what the source observed when its own finish returned.
    finished: mpsc::Sender<SourceFinishReturn>,
    sampler_exited: Arc<AtomicBool>,
}

impl Drop for PausingProcessMemory {
    fn drop(&mut self) {
        self.sampler_exited.store(true, Ordering::Release);
    }
}

/// What a [`PausingProcessMemory`] observed right after its own finish returned.
struct SourceFinishReturn {
    /// The length of the output.
    written: usize,
    result: std::io::Result<()>,
}

impl PausingProcessMemory {
    /// Finishes the trace as `source_finish` says, and reports the output written by then.
    fn finish_trace(&self) {
        let Some(finish) = self.finish.get() else {
            return;
        };
        match self.source_finish {
            SourceFinish::Never => return,
            SourceFinish::First => {}
            SourceFinish::DuringAnotherFinish => {
                // Wait until the other finish has taken the sampler thread to join it.
                // The check reads the sampler under the lock that the sampler's own finish takes,
                // so a finish that held it while joining keeps the check false, and the test's
                // bound reports the stall that the sampler's own finish would hit.
                while !finish.sampler_join_began() {
                    std::thread::yield_now();
                }
            }
        }
        let result = finish.finish();
        let source_return = SourceFinishReturn {
            written: self.output.buf.lock().len(),
            result,
        };
        // The receiver is gone only after the test stopped waiting.
        self.finished.send(source_return).ok();
    }
}

impl ProcessMemorySource for PausingProcessMemory {
    fn resident_set_size_bytes(&self) -> Option<u64> {
        match self.samples.fetch_add(1, Ordering::Relaxed) {
            0 => Some(FIRST_SAMPLE),
            1 => {
                // The receivers are gone only after the test stopped waiting.
                self.paused.send(()).ok();
                self.release.lock().recv().ok();
                self.finish_trace();
                Some(SECOND_SAMPLE)
            }
            _ => None,
        }
    }
}

/// A layer whose sampler samples a [`PausingProcessMemory`], with the handles that drive it.
struct PausingLayer {
    layer: PerfettoLayer<TestWriter>,
    output: TestWriter,
    paused: mpsc::Receiver<()>,
    release: mpsc::Sender<()>,
    /// Receives what the source observed when its own finish returned.
    source_finished: mpsc::Receiver<SourceFinishReturn>,
    /// Whether the sampler thread has exited, after which it writes nothing and has released
    /// the layer's sink.
    sampler_exited: Arc<AtomicBool>,
}

/// Returns a layer that samples a [`PausingProcessMemory`] every millisecond, which finishes the
/// trace as `source_finish` says.
fn pausing_layer(source_finish: SourceFinish) -> Result<PausingLayer> {
    let writer = TestWriter::new();
    let output = writer.make_writer();
    let (paused_sender, paused) = mpsc::channel();
    let (release, release_receiver) = mpsc::channel();
    let (finished_sender, source_finished) = mpsc::channel();
    let finish = Arc::new(OnceLock::new());
    let sampler_exited = Arc::new(AtomicBool::new(false));
    let source = PausingProcessMemory {
        samples: AtomicUsize::new(0),
        paused: paused_sender,
        release: Mutex::new(release_receiver),
        source_finish,
        finish: Arc::clone(&finish),
        output: output.make_writer(),
        finished: finished_sender,
        sampler_exited: Arc::clone(&sampler_exited),
    };
    let layer = PerfettoLayer::new(writer)
        .with_process_memory_source(MIN_SAMPLING_INTERVAL, Box::new(source));
    // The source reads the handle only after the test releases it, which is after this.
    finish.set(layer.finish_handle()).or_fail()?;
    Ok(PausingLayer {
        layer,
        output,
        paused,
        release,
        source_finished,
        sampler_exited,
    })
}

/// What a test observed right after a finish on another thread returned.
struct FinishReturn {
    /// The length of the output.
    written: usize,
    /// Whether the sampler thread had exited.
    sampler_exited: bool,
    result: std::io::Result<()>,
}

/// Finishes the trace of `pausing` on a new thread, and sends what it observed on return.
fn finish_on_thread(pausing: &PausingLayer, returned: mpsc::Sender<FinishReturn>) {
    let finish = pausing.layer.finish_handle();
    let output = pausing.output.make_writer();
    let sampler_exited = Arc::clone(&pausing.sampler_exited);
    std::thread::spawn(move || {
        let result = finish.finish();
        let finish_return = FinishReturn {
            written: output.buf.lock().len(),
            sampler_exited: sampler_exited.load(Ordering::Acquire),
            result,
        };
        returned.send(finish_return).ok();
    });
}

/// Returns the values of the counter samples in `trace`.
fn sample_values(trace: &idl::Trace) -> Vec<i64> {
    events_of_type(trace, idl::track_event::Type::Counter)
        .filter_map(|event| match event.counter_value_field {
            Some(idl::track_event::CounterValueField::CounterValue(value)) => Some(value),
            _ => None,
        })
        .collect()
}

/// A finish from the sampler thread while another thread's finish stops the sampler lets both
/// return, and no sample is written after either returned.
///
/// A finish that joined the sampler while holding the lock that the sampler's own finish takes
/// would deadlock both, as would one that waited for the sampler on the sampler thread itself.
#[gtest]
fn finish_from_the_sampler_thread_during_another_finish_returns() -> Result<()> {
    let pausing = pausing_layer(SourceFinish::DuringAnotherFinish)?;
    let layer_sink = Arc::clone(pausing.layer.sink());

    // Finish while the sampler is paused in its second sample, which then finishes too.
    pausing.paused.recv_timeout(FINISH_BOUND).or_fail()?;
    let (returned, returns) = mpsc::channel();
    finish_on_thread(&pausing, returned);
    pausing.release.send(()).or_fail()?;

    let Ok(test_finish) = returns.recv_timeout(FINISH_BOUND) else {
        // Dropping the layer would finish it too, which waits for the stuck finish.
        std::mem::forget(pausing.layer);
        return fail!("the test's finish deadlocked");
    };
    let Ok(source_return) = pausing.source_finished.recv_timeout(FINISH_BOUND) else {
        std::mem::forget(pausing.layer);
        return fail!("the sampler's own finish deadlocked or panicked");
    };
    // The sampler released its sink, and no finish holds it any more.
    expect_eq!(Arc::strong_count(&layer_sink), 2);
    drop(pausing.layer);

    // Both finishes found every write succeeded.
    expect_that!(test_finish.result, ok(anything()));
    expect_that!(source_return.result, ok(anything()));
    // The test's finish returned only after the sampler had exited.
    expect_that!(test_finish.sampler_exited, eq(true));
    // Nothing reached the writer after either finish returned.
    let written = pausing.output.buf.lock().len();
    expect_eq!(test_finish.written, written);
    expect_eq!(source_return.written, written);
    // The sample read while the trace finished is discarded.
    let trace = decode(&pausing.output)?;
    let first_sample = i64::try_from(FIRST_SAMPLE).or_fail()?;
    verify_that!(sample_values(&trace), elements_are![eq(&first_sample)])
}

/// A finish from the sampler thread returns without waiting for the sampler, the sampler writes
/// nothing afterwards, and a later finish still waits for the sampler to stop.
///
/// A finish that joined the sampler from the sampler thread would join itself, which panics on
/// Unix and blocks forever on Windows.
#[gtest]
fn finish_from_the_sampler_thread_does_not_join_itself() -> Result<()> {
    let pausing = pausing_layer(SourceFinish::First)?;
    let layer_sink = Arc::clone(pausing.layer.sink());

    // The source finishes the trace from the sampler thread, then the test finishes it again.
    pausing.paused.recv_timeout(FINISH_BOUND).or_fail()?;
    pausing.release.send(()).or_fail()?;
    let Ok(source_return) = pausing.source_finished.recv_timeout(FINISH_BOUND) else {
        // Dropping the layer would finish it too, which waits for the stuck finish.
        std::mem::forget(pausing.layer);
        return fail!("the sampler's own finish deadlocked or panicked");
    };
    let (returned, returns) = mpsc::channel();
    finish_on_thread(&pausing, returned);
    let Ok(test_finish) = returns.recv_timeout(FINISH_BOUND) else {
        std::mem::forget(pausing.layer);
        return fail!("the finish after the sampler's own finish deadlocked");
    };
    // The sampler released its sink, and no finish holds it any more.
    expect_eq!(Arc::strong_count(&layer_sink), 2);
    drop(pausing.layer);

    // Both finishes found every write succeeded.
    expect_that!(test_finish.result, ok(anything()));
    expect_that!(source_return.result, ok(anything()));
    // The later finish returned only after the sampler had exited.
    expect_that!(test_finish.sampler_exited, eq(true));
    // Nothing reached the writer after the sampler's own finish returned.
    expect_eq!(pausing.output.buf.lock().len(), source_return.written);
    // The sample read while the trace finished is discarded.
    let trace = decode(&pausing.output)?;
    let first_sample = i64::try_from(FIRST_SAMPLE).or_fail()?;
    verify_that!(sample_values(&trace), elements_are![eq(&first_sample)])
}

/// Two concurrent finishes both return only after the sampler has stopped, including the one that
/// finds the sampler already being joined by the other.
///
/// A finish that returned once another had taken the sampler to join it would return while the
/// sampler can still write a sample.
#[gtest]
fn concurrent_finishes_return_after_the_sampler_stopped() -> Result<()> {
    let pausing = pausing_layer(SourceFinish::Never)?;
    let layer_sink = Arc::clone(pausing.layer.sink());

    // Finish twice while the sampler is paused in its second sample.
    pausing.paused.recv_timeout(FINISH_BOUND).or_fail()?;
    let (returned, returns) = mpsc::channel();
    for _ in 0..2 {
        finish_on_thread(&pausing, returned.clone());
    }

    // Both finishes wait for the sampler before the test releases it, one joining it and the
    // other waiting for that join, and neither has returned.
    let observer = pausing.layer.finish_handle();
    let both_wait = || observer.sampler_join_contentions() == 1 && observer.sampler_join_began();
    if !wait_until(FINISH_BOUND, both_wait) {
        std::mem::forget(pausing.layer);
        return fail!("no finish waited for another finish's join of the sampler");
    }
    let mut finish_returns: Vec<FinishReturn> = returns.try_iter().collect();
    expect_that!(
        finish_returns.len(),
        eq(0),
        "a finish returned while the sampler was paused"
    );
    pausing.release.send(()).or_fail()?;

    while finish_returns.len() < 2 {
        let Ok(finish_return) = returns.recv_timeout(FINISH_BOUND) else {
            // Dropping the layer would finish it too, which waits for the stuck finish.
            std::mem::forget(pausing.layer);
            return fail!("a finish did not return after the sampler was released");
        };
        finish_returns.push(finish_return);
    }
    // The sampler released its sink, and no finish holds it any more.
    expect_eq!(Arc::strong_count(&layer_sink), 2);
    drop(pausing.layer);

    // Each finish returned after the sampler had exited and written its last sample.
    let written = pausing.output.buf.lock().len();
    for finish_return in finish_returns {
        expect_that!(finish_return.result, ok(anything()));
        expect_that!(finish_return.sampler_exited, eq(true));
        expect_eq!(finish_return.written, written);
    }
    Ok(())
}

/// A writer that finishes the trace when the sampler thread first writes through it, and sends
/// what that finish returned.
struct FinishingWriter {
    /// The handle of the trace that the writer writes, set once the layer is built.
    finish: Arc<OnceLock<FinishHandle>>,
    /// Sends the result of the writer's own finish, once.
    finished: Mutex<Option<mpsc::Sender<std::io::Result<()>>>>,
}

impl<'a> MakeWriter<'a> for FinishingWriter {
    type Writer = std::io::Sink;

    fn make_writer(&'a self) -> Self::Writer {
        let on_sampler_thread = std::thread::current().name() == Some("perfetto-process-memory");
        if on_sampler_thread
            && let Some(finish) = self.finish.get()
            && let Some(finished) = self.finished.lock().take()
        {
            // The receiver is gone only after the test stopped waiting.
            finished.send(finish.finish()).ok();
        }
        std::io::sink()
    }
}

/// A writer that finishes the trace from the sampler thread, inside the sampler's write, gets
/// the finish's result back.
///
/// A finish that waited for the write in progress to report its error would wait for the very
/// write that called it, and deadlock the sampler.
#[gtest]
fn finish_from_the_writer_on_the_sampler_thread_returns() -> Result<()> {
    let finish = Arc::new(OnceLock::new());
    let (finished, writer_finished) = mpsc::channel();
    let writer = FinishingWriter {
        finish: Arc::clone(&finish),
        finished: Mutex::new(Some(finished)),
    };
    let layer = PerfettoLayer::new(writer)
        .with_process_memory_source(MIN_SAMPLING_INTERVAL, Box::new(FixedProcessMemory));
    // The writer finishes only once it can read the handle, so a sample written before this
    // finishes nothing.
    finish.set(layer.finish_handle()).or_fail()?;

    let Ok(result) = writer_finished.recv_timeout(FINISH_BOUND) else {
        // Dropping the layer would finish it too, which waits for the stuck sampler.
        std::mem::forget(layer);
        return fail!("the writer's finish deadlocked");
    };
    drop(layer);
    verify_that!(result, ok(anything()))
}

/// A memory source that always reports 1 KiB.
struct FixedProcessMemory;

impl ProcessMemorySource for FixedProcessMemory {
    fn resident_set_size_bytes(&self) -> Option<u64> {
        Some(1024)
    }
}
