//! Regression tests for the life of a trace's process-memory sampler: none starts once the trace
//! is finished, a replacement starts only after its predecessor stopped, and a sampler's panic
//! cannot deadlock a finish.

use crate::process_memory::ProcessMemorySource;
use crate::process_memory::sampler::MIN_SAMPLING_INTERVAL;
use crate::tests::{TestWriter, decode, events_of_type, track_descriptor, track_name, wait_until};
use crate::{FinishHandle, PerfettoLayer, idl};
use googletest::prelude::*;
use parking_lot::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock, mpsc};
use std::time::Duration;
use tracing_subscriber::fmt::MakeWriter;

/// The bound on waiting for a sampler or a finish, far above any scheduling delay on a loaded
/// machine.
///
/// A sampler or finish that deadlocks exceeds it, which fails the test instead of hanging the
/// suite.
const BOUND: Duration = Duration::from_secs(30);

/// The value that the first sampler of a test reports.
const FIRST_SOURCE_VALUE: u64 = 1024;

/// The value that the sampler replacing the first one reports.
const SECOND_SOURCE_VALUE: u64 = 2048;

/// A memory source that reports `value` and announces each read.
struct AnnouncingSource {
    value: u64,
    announcements: mpsc::Sender<()>,
}

impl ProcessMemorySource for AnnouncingSource {
    fn resident_set_size_bytes(&self) -> Option<u64> {
        // The receiver is gone only after the test stopped waiting for reads.
        self.announcements.send(()).ok();
        Some(self.value)
    }
}

/// Returns a source that reports `value`, and the receiver of its read announcements.
fn announcing_source(value: u64) -> (Box<dyn ProcessMemorySource>, mpsc::Receiver<()>) {
    let (announcements, announced) = mpsc::channel();
    let source = AnnouncingSource {
        value,
        announcements,
    };
    (Box::new(source), announced)
}

/// Returns the counter samples in `trace`, in the order they were written.
fn counter_samples(trace: &idl::Trace) -> Vec<&idl::TrackEvent> {
    events_of_type(trace, idl::track_event::Type::Counter).collect()
}

/// Returns the value of a counter sample.
fn counter_value(event: &idl::TrackEvent) -> Option<i64> {
    match event.counter_value_field {
        Some(idl::track_event::CounterValueField::CounterValue(value)) => Some(value),
        _ => None,
    }
}

/// Returns the descriptors of the `process.rss` counter track in `trace`.
fn rss_track_descriptors(trace: &idl::Trace) -> Vec<&idl::TrackDescriptor> {
    trace
        .packet
        .iter()
        .filter_map(track_descriptor)
        .filter(|descriptor| track_name(descriptor) == Some("process.rss"))
        .collect()
}

/// A memory source that counts its reads and records when the sampler drops it.
struct ProbedSource {
    reads: Arc<AtomicUsize>,
    dropped: Arc<AtomicBool>,
}

impl ProcessMemorySource for ProbedSource {
    fn resident_set_size_bytes(&self) -> Option<u64> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        Some(FIRST_SOURCE_VALUE)
    }
}

impl Drop for ProbedSource {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::Release);
    }
}

/// A sampler installed after the trace is finished never starts: its source is dropped unread
/// before the installation returns, and the trace holds no counter.
///
/// A sampler started after the finish would read its source every interval and keep the writer
/// alive until the layer is dropped, although its trace has ended.
#[gtest]
fn a_sampler_installed_after_finish_never_starts() -> Result<()> {
    let writer = TestWriter::new();
    let output = writer.make_writer();
    let layer = PerfettoLayer::new(writer);
    layer.finish_handle().finish().or_fail()?;

    let reads = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicBool::new(false));
    let source = ProbedSource {
        reads: Arc::clone(&reads),
        dropped: Arc::clone(&dropped),
    };
    let layer = layer.with_process_memory_source(MIN_SAMPLING_INTERVAL, Box::new(source));

    // The source was dropped without a single read, so no sampler thread holds it.
    expect_that!(dropped.load(Ordering::Acquire), eq(true));
    expect_eq!(reads.load(Ordering::Relaxed), 0);
    drop(layer);
    // The finished trace holds neither the counter track nor a sample.
    let trace = decode(&output)?;
    expect_that!(rss_track_descriptors(&trace), is_empty());
    verify_that!(counter_samples(&trace), is_empty())
}

/// A replacement sampler continues the counter track of the one it replaces: the trace describes
/// the track once, every sample lies on it, and every sample of the replaced source precedes
/// those of its replacement.
///
/// Two samplers that each tracked whether they had described the track would describe it twice,
/// and two that ran at once would interleave their samples.
#[gtest]
fn a_replacement_sampler_describes_the_counter_track_once() -> Result<()> {
    let writer = TestWriter::new();
    let output = writer.make_writer();
    let (first_source, first_reads) = announcing_source(FIRST_SOURCE_VALUE);
    let layer =
        PerfettoLayer::new(writer).with_process_memory_source(MIN_SAMPLING_INTERVAL, first_source);
    // A sampler writes a sample before it reads the next one, so a second read proves that the
    // first sample, with the track's descriptor, was written.
    for _ in 0..2 {
        first_reads.recv_timeout(BOUND).or_fail()?;
    }

    let (second_source, second_reads) = announcing_source(SECOND_SOURCE_VALUE);
    let layer = layer.with_process_memory_source(MIN_SAMPLING_INTERVAL, second_source);
    for _ in 0..2 {
        second_reads.recv_timeout(BOUND).or_fail()?;
    }
    layer.finish_handle().finish().or_fail()?;
    drop(layer);

    let trace = decode(&output)?;
    let descriptors = rss_track_descriptors(&trace);
    let [descriptor] = descriptors.as_slice() else {
        return fail!(
            "expected one process.rss descriptor, got {}",
            descriptors.len()
        );
    };
    let samples = counter_samples(&trace);
    // Every sample lies on the one described track.
    for sample in &samples {
        expect_eq!(sample.track_uuid, descriptor.uuid);
    }
    // Both samplers wrote, the replaced one only before its replacement.
    let first_value = i64::try_from(FIRST_SOURCE_VALUE).or_fail()?;
    let second_value = i64::try_from(SECOND_SOURCE_VALUE).or_fail()?;
    let values: Vec<i64> = samples.iter().copied().filter_map(counter_value).collect();
    let Some(replaced_at) = values.iter().position(|&value| value == second_value) else {
        return fail!("the replacement wrote no sample: {values:?}");
    };
    let (replaced, replacing) = values.split_at(replaced_at);
    expect_that!(replaced, all![not(is_empty()), each(eq(&first_value))]);
    verify_that!(replacing, each(eq(&second_value)))
}

/// A memory source that pauses in its second read until the test releases it, and records when
/// the sampler drops it.
struct PausingSource {
    reads: AtomicUsize,
    paused: mpsc::Sender<()>,
    /// The test's release, behind a mutex because the source must be [`Sync`].
    release: Mutex<mpsc::Receiver<()>>,
    dropped: Arc<AtomicBool>,
}

impl ProcessMemorySource for PausingSource {
    fn resident_set_size_bytes(&self) -> Option<u64> {
        if self.reads.fetch_add(1, Ordering::Relaxed) == 1 {
            // The receiver and the sender are gone only after the test stopped waiting.
            self.paused.send(()).ok();
            self.release.lock().recv().ok();
        }
        Some(FIRST_SOURCE_VALUE)
    }
}

impl Drop for PausingSource {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::Release);
    }
}

/// A memory source that records, on its first read, whether the source it replaces was dropped.
struct ReplacingSource {
    replaced_dropped: Arc<AtomicBool>,
    /// Whether the replaced source was dropped when this source was first read, set once.
    replaced_dropped_at_first_read: Arc<OnceLock<bool>>,
}

impl ProcessMemorySource for ReplacingSource {
    fn resident_set_size_bytes(&self) -> Option<u64> {
        let replaced_dropped = self.replaced_dropped.load(Ordering::Acquire);
        // Only the first read sets the observation, so later reads leave it unchanged.
        self.replaced_dropped_at_first_read
            .set(replaced_dropped)
            .ok();
        Some(SECOND_SOURCE_VALUE)
    }
}

/// Replacing a sampler stops it and waits until it has dropped its source before the replacement
/// starts, even while the replaced sampler is in the middle of a read.
///
/// A replacement started first would sample while its predecessor still reads and writes, so
/// both samplers would write at once.
#[gtest]
fn a_replacement_sampler_starts_after_the_replaced_one_stopped() -> Result<()> {
    let (paused_sender, paused) = mpsc::channel();
    let (release, release_receiver) = mpsc::channel();
    let replaced_dropped = Arc::new(AtomicBool::new(false));
    let replaced = PausingSource {
        reads: AtomicUsize::new(0),
        paused: paused_sender,
        release: Mutex::new(release_receiver),
        dropped: Arc::clone(&replaced_dropped),
    };
    let layer = PerfettoLayer::new(TestWriter::new())
        .with_process_memory_source(MIN_SAMPLING_INTERVAL, Box::new(replaced));
    let observer = layer.finish_handle();

    // Replace the sampler on another thread while it is paused in its second read.
    paused.recv_timeout(BOUND).or_fail()?;
    let replaced_dropped_at_first_read = Arc::new(OnceLock::new());
    let replacing = ReplacingSource {
        replaced_dropped: Arc::clone(&replaced_dropped),
        replaced_dropped_at_first_read: Arc::clone(&replaced_dropped_at_first_read),
    };
    let (replaced_layer, replaced_layers) = mpsc::channel();
    std::thread::spawn(move || {
        let layer = layer.with_process_memory_source(MIN_SAMPLING_INTERVAL, Box::new(replacing));
        // The receiver is gone only after the test stopped waiting.
        replaced_layer.send(layer).ok();
    });

    // Release the paused sampler once the replacement waits for it to stop, or once the
    // replacement samples while it is paused.
    let replacement_waits_or_samples =
        || observer.sampler_join_began() || replaced_dropped_at_first_read.get().is_some();
    if !wait_until(BOUND, replacement_waits_or_samples) {
        return fail!("the replacement neither waited for the paused sampler nor sampled");
    }
    release.send(()).or_fail()?;
    let Ok(layer) = replaced_layers.recv_timeout(BOUND) else {
        return fail!("the replacement did not return after the paused sampler was released");
    };
    // The replaced sampler had dropped its source by the time the replacement returned.
    expect_that!(replaced_dropped.load(Ordering::Acquire), eq(true));
    if !wait_until(BOUND, || replaced_dropped_at_first_read.get().is_some()) {
        std::mem::forget(layer);
        return fail!("the replacement never sampled");
    }
    drop(layer);
    // The replacement first sampled only after the replaced sampler had stopped.
    verify_that!(replaced_dropped_at_first_read.get(), some(eq(&true)))
}

/// A panic payload that finishes the trace when it is dropped, and sends that finish's result.
struct FinishingPayload {
    finish: FinishHandle,
    finished: mpsc::Sender<std::io::Result<()>>,
}

impl Drop for FinishingPayload {
    fn drop(&mut self) {
        // The receiver is gone only after the test stopped waiting.
        self.finished.send(self.finish.finish()).ok();
    }
}

/// A memory source that, once the test releases it, panics with a [`FinishingPayload`].
struct PanickingSource {
    /// The test's release, behind a mutex because the source must be [`Sync`].
    release: Mutex<mpsc::Receiver<()>>,
    /// The handle of the trace that the source samples for, set once the layer is built.
    finish: Arc<OnceLock<FinishHandle>>,
    finished: mpsc::Sender<std::io::Result<()>>,
}

impl ProcessMemorySource for PanickingSource {
    fn resident_set_size_bytes(&self) -> Option<u64> {
        self.release.lock().recv().ok();
        if let Some(finish) = self.finish.get() {
            std::panic::panic_any(FinishingPayload {
                finish: finish.clone(),
                finished: self.finished.clone(),
            });
        }
        None
    }
}

/// Returns a layer whose sampler panics with a [`FinishingPayload`] at its first read, and the
/// receiver of the payload's finish result.
fn panicking_layer() -> Result<(
    PerfettoLayer<TestWriter>,
    mpsc::Receiver<std::io::Result<()>>,
)> {
    let (release, release_receiver) = mpsc::channel();
    let (payload_finish_sender, payload_finishes) = mpsc::channel();
    let finish = Arc::new(OnceLock::new());
    let source = PanickingSource {
        release: Mutex::new(release_receiver),
        finish: Arc::clone(&finish),
        finished: payload_finish_sender,
    };
    let layer = PerfettoLayer::new(TestWriter::new())
        .with_process_memory_source(MIN_SAMPLING_INTERVAL, Box::new(source));
    // The source panics only once it can read the handle, which the payload finishes through.
    finish.set(layer.finish_handle()).or_fail()?;
    release.send(()).or_fail()?;
    Ok((layer, payload_finishes))
}

/// A finish that waits for a sampler whose source panicked returns, also when dropping the
/// panic's payload finishes the trace again.
///
/// A finish that dropped the payload while it still held the lock under which it waited for the
/// sampler would deadlock in the payload's own finish, which takes that lock.
#[gtest]
fn a_finish_that_receives_a_sampler_panic_returns() -> Result<()> {
    let (layer, payload_finishes) = panicking_layer()?;

    let (returned, finish_returns) = mpsc::channel();
    std::thread::spawn({
        let finish = layer.finish_handle();
        move || {
            // The receiver is gone only after the test stopped waiting.
            returned.send(finish.finish()).ok();
        }
    });
    let Ok(test_finish) = finish_returns.recv_timeout(BOUND) else {
        // Dropping the layer would finish it too, which waits for the stuck finish.
        std::mem::forget(layer);
        return fail!("the finish that received the sampler's panic deadlocked");
    };
    let Ok(payload_finish) = payload_finishes.recv_timeout(BOUND) else {
        std::mem::forget(layer);
        return fail!("the panic's payload was never dropped");
    };
    drop(layer);

    // Both finishes found every write succeeded.
    expect_that!(test_finish, ok(anything()));
    verify_that!(payload_finish, ok(anything()))
}

/// A replacement of a sampler whose source panicked returns, also when dropping the panic's
/// payload finishes the trace, which then stops the replacement too.
///
/// A replacement that dropped the payload while it still held the lock under which it waited for
/// the replaced sampler would deadlock in the payload's own finish, which takes that lock.
#[gtest]
fn a_replacement_that_receives_a_sampler_panic_returns() -> Result<()> {
    let (layer, payload_finishes) = panicking_layer()?;
    let observer = layer.finish_handle();

    let (replaced_layer, replaced_layers) = mpsc::channel();
    let (replacing, _) = announcing_source(SECOND_SOURCE_VALUE);
    std::thread::spawn(move || {
        let layer = layer.with_process_memory_source(MIN_SAMPLING_INTERVAL, replacing);
        // The receiver is gone only after the test stopped waiting.
        replaced_layer.send(layer).ok();
    });
    let Ok(layer) = replaced_layers.recv_timeout(BOUND) else {
        return fail!("the replacement that received the sampler's panic deadlocked");
    };
    let Ok(payload_finish) = payload_finishes.recv_timeout(BOUND) else {
        std::mem::forget(layer);
        return fail!("the panic's payload was never dropped");
    };
    drop(layer);

    // The payload's finish ended the trace before the replacement returned.
    expect_that!(payload_finish, ok(anything()));
    verify_that!(
        format!("{observer:?}"),
        eq("FinishHandle { finished: true }")
    )
}
