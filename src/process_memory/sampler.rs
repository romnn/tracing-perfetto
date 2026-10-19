//! The thread that samples a [`ProcessMemorySource`] onto a layer's `process.rss` counter track.

use crate::process_memory::ProcessMemorySource;
use crate::sink::TraceSink;
use crate::writer::PerfettoWriter;
use crate::{idl, packets};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError, TryRecvError};
use std::thread::{JoinHandle, ThreadId};
use std::time::Duration;

/// The shortest interval between two samples, which keeps a zero interval from sampling in a
/// busy loop.
pub(crate) const MIN_SAMPLING_INTERVAL: Duration = Duration::from_millis(1);

/// The `process.rss` counter track of a layer, which every sampler of the layer writes to.
pub(crate) struct CounterTrack {
    uuid: u64,
    /// Whether the track's descriptor was written.
    ///
    /// A replacement sampler starts only after the sampler it replaces has exited, so the
    /// samplers of a layer read and write it one after another.
    described: AtomicBool,
}

impl CounterTrack {
    /// Returns a track that is not described yet, with a new UUID.
    pub(crate) fn new() -> Self {
        Self {
            uuid: packets::unique_uuid(),
            described: AtomicBool::new(false),
        }
    }
}

/// What a [`ProcessMemorySampler`] samples and where it writes the samples.
pub(crate) struct ProcessMemorySamplerConfig<W> {
    /// The layer's sink, so the samples share its sequence, clock and preamble.
    pub(crate) sink: Arc<TraceSink<W>>,
    pub(crate) counter_track: Arc<CounterTrack>,
    /// The interval between samples, raised to at least [`MIN_SAMPLING_INTERVAL`].
    pub(crate) interval: Duration,
    /// The source, which the installing layer shares until it has released the lifecycle's
    /// locks, so that its last reference never drops under them.
    pub(crate) source: Arc<dyn ProcessMemorySource>,
}

/// A running sampler thread, which writes until [`Self::request_stop`] stops it.
pub(crate) struct ProcessMemorySampler {
    /// Dropping this wakes the thread from its wait for the next sample and ends it.
    ///
    /// `None` once a stop was requested.
    stop: Option<mpsc::Sender<()>>,
    /// The sampler thread, until [`Self::stop`] hands it out to be joined, or `None` if it could
    /// not be spawned.
    join_handle: Option<JoinHandle<()>>,
    /// The ID of the sampler thread, which outlives `join_handle` so that the thread still
    /// recognizes itself while another thread joins it.
    thread_id: Option<ThreadId>,
}

impl ProcessMemorySampler {
    /// Spawns a thread that samples `config.source` every `config.interval`.
    ///
    /// The counter track's descriptor is written with the first sample of the layer's first
    /// sampler, and the track counts as described only once that write succeeds.
    /// Intervals without a value are skipped.
    /// A sample read while a stop is requested is not written, even when the source itself
    /// requested it.
    /// If the thread cannot be spawned, nothing is sampled.
    pub(crate) fn spawn<W>(config: ProcessMemorySamplerConfig<W>) -> Self
    where
        W: PerfettoWriter + Send + Sync + 'static,
    {
        let (stop, stopped) = mpsc::channel::<()>();
        let interval = sampling_interval(config.interval);

        let join_handle = spawn_thread(move || {
            loop {
                let rss_bytes = config.source.resident_set_size_bytes();
                // A stop requested while the source was read, possibly by the source
                // finishing the trace itself, discards the sample, which could otherwise
                // land after that finish returned.
                if stop_requested(&stopped) {
                    break;
                }
                if let Some(rss_bytes) = rss_bytes {
                    write_sample(&config, rss_bytes);
                }
                match stopped.recv_timeout(interval) {
                    Err(RecvTimeoutError::Timeout) => {}
                    Ok(()) | Err(RecvTimeoutError::Disconnected) => break,
                }
            }
        })
        .ok();
        let thread_id = join_handle.as_ref().map(|handle| handle.thread().id());

        Self {
            stop: Some(stop),
            join_handle,
            thread_id,
        }
    }

    /// Returns whether the calling thread is the sampler thread, which happens when its source
    /// or writer calls back into the layer.
    pub(crate) fn runs_on_current_thread(&self) -> bool {
        self.thread_id == Some(std::thread::current().id())
    }

    /// Wakes the thread so that it stops, without waiting for it.
    ///
    /// The thread wakes from its wait between samples at once.
    /// It discards a sample whose value it is still reading, while one it is already writing
    /// still lands.
    pub(crate) fn request_stop(&mut self) {
        self.stop = None;
    }

    /// Requests a stop, and hands out the thread so that the caller can wait for it without
    /// holding a lock that the thread may take.
    ///
    /// Returns `None` when the thread was already handed out or never spawned.
    /// The sampler thread must not call this, since it cannot wait for itself.
    pub(crate) fn stop(&mut self) -> Option<StoppingSampler> {
        self.request_stop();
        self.join_handle.take().map(StoppingSampler)
    }

    /// Returns whether [`Self::stop`] has handed out the thread to be joined.
    #[cfg(test)]
    pub(crate) fn is_handed_out(&self) -> bool {
        self.thread_id.is_some() && self.join_handle.is_none()
    }
}

/// A sampler thread that was asked to stop, which [`Self::join`] waits for.
pub(crate) struct StoppingSampler(JoinHandle<()>);

impl StoppingSampler {
    /// Waits for the thread to exit, after which it writes nothing and has released the writer,
    /// and returns how it exited.
    ///
    /// The thread wakes from its wait between samples at once, so this waits only for a sample
    /// that is in progress.
    /// A thread that panicked returns the panic's payload, whose `Drop` is user code that may
    /// finish the trace, so the caller drops it only after releasing the layer's locks.
    pub(crate) fn join(self) -> std::thread::Result<()> {
        self.0.join()
    }
}

/// Spawns `thread` as a sampler thread.
///
/// When the thread cannot be spawned, `thread` is dropped on the calling thread.
fn spawn_thread(thread: impl FnOnce() + Send + 'static) -> std::io::Result<JoinHandle<()>> {
    #[cfg(test)]
    if FAILING_SPAWNS.with(std::cell::Cell::get) {
        // The standard library drops the thread's closure on the calling thread, too, when the
        // system refuses a thread.
        drop(thread);
        return Err(std::io::Error::other("injected sampler spawn failure"));
    }
    std::thread::Builder::new()
        .name("perfetto-process-memory".to_string())
        .spawn(thread)
}

#[cfg(test)]
thread_local! {
    /// Whether spawning a sampler thread from the calling thread fails.
    static FAILING_SPAWNS: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Makes spawning a sampler thread from the calling thread fail, until it is dropped.
#[cfg(test)]
pub(crate) struct FailingSpawns(());

#[cfg(test)]
impl FailingSpawns {
    pub(crate) fn begin() -> Self {
        FAILING_SPAWNS.with(|failing| failing.set(true));
        Self(())
    }
}

#[cfg(test)]
impl Drop for FailingSpawns {
    fn drop(&mut self) {
        FAILING_SPAWNS.with(|failing| failing.set(false));
    }
}

/// Returns whether a stop was requested through the sender of `stopped`.
fn stop_requested(stopped: &mpsc::Receiver<()>) -> bool {
    match stopped.try_recv() {
        Err(TryRecvError::Empty) => false,
        Ok(()) | Err(TryRecvError::Disconnected) => true,
    }
}

/// Returns the interval at which the sampler samples when `requested` is asked for.
pub(crate) fn sampling_interval(requested: Duration) -> Duration {
    requested.max(MIN_SAMPLING_INTERVAL)
}

/// Writes one counter sample of `rss_bytes`, preceded by the counter track's descriptor until a
/// write of it succeeds.
fn write_sample<W: PerfettoWriter>(config: &ProcessMemorySamplerConfig<W>, rss_bytes: u64) {
    let track = &config.counter_track;
    // A replacement sampler is spawned only after the sampler it replaces was joined, and a join
    // and a spawn each order the memory accesses of the two threads, so the flag needs no
    // ordering of its own.
    let include_descriptor = !track.described.load(Ordering::Relaxed);
    let sequence = config.sink.sequence();
    let mut packet = sequence.timestamped_packet();

    let counter_value = i64::try_from(rss_bytes).unwrap_or(i64::MAX);
    let mut event = idl::TrackEvent {
        track_uuid: Some(track.uuid),
        counter_value_field: Some(idl::track_event::CounterValueField::CounterValue(
            counter_value,
        )),
        ..Default::default()
    };
    event.set_type(idl::track_event::Type::Counter);
    packet.data = Some(idl::trace_packet::Data::TrackEvent(event));

    let mut trace_packets = Vec::with_capacity(2);
    if include_descriptor {
        let mut counter = idl::CounterDescriptor::default();
        counter.set_unit(idl::counter_descriptor::Unit::SizeBytes);
        trace_packets.push(packets::track_descriptor_packet(idl::TrackDescriptor {
            uuid: Some(track.uuid),
            parent_uuid: Some(sequence.process_track_uuid()),
            static_or_dynamic_name: Some(packets::track_name("process.rss")),
            counter: Some(counter),
            ..Default::default()
        }));
    }
    trace_packets.push(packet);

    let written = config.sink.write(&idl::Trace {
        packet: trace_packets,
    });
    if written && include_descriptor {
        track.described.store(true, Ordering::Relaxed);
    }
}
