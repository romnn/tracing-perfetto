//! The thread that samples a [`ProcessMemorySource`] onto a layer's `process.rss` counter track.

use crate::process_memory::ProcessMemorySource;
use crate::sequence::TraceSequence;
use crate::writer::PerfettoWriter;
use crate::{idl, packets};
use std::sync::Arc;
use std::sync::mpsc::{self, RecvTimeoutError, TryRecvError};
use std::thread::{JoinHandle, ThreadId};
use std::time::Duration;

/// The shortest interval between two samples, which keeps a zero interval from sampling in a
/// busy loop.
pub(crate) const MIN_SAMPLING_INTERVAL: Duration = Duration::from_millis(1);

/// What a [`ProcessMemorySampler`] samples and where it writes the samples.
pub(crate) struct ProcessMemorySamplerConfig<W> {
    pub(crate) writer: Arc<W>,
    /// The layer's sequence, so the samples share its clock and preamble.
    pub(crate) sequence: Arc<TraceSequence>,
    pub(crate) counter_track_uuid: u64,
    /// The interval between samples, raised to at least [`MIN_SAMPLING_INTERVAL`].
    pub(crate) interval: Duration,
    pub(crate) source: Box<dyn ProcessMemorySource>,
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
    /// The counter track's descriptor is written with the first sample, and intervals without a
    /// value are skipped.
    /// A sample read while a stop is requested is not written, even when the source itself
    /// requested it.
    /// If the thread cannot be spawned, nothing is sampled.
    pub(crate) fn spawn<W>(config: ProcessMemorySamplerConfig<W>) -> Self
    where
        W: PerfettoWriter + Send + Sync + 'static,
    {
        let (stop, stopped) = mpsc::channel::<()>();
        let interval = sampling_interval(config.interval);

        let join_handle = std::thread::Builder::new()
            .name("perfetto-process-memory".to_string())
            .spawn(move || {
                let mut wrote_descriptor = false;
                loop {
                    let rss_bytes = config.source.resident_set_size_bytes();
                    // A stop requested while the source was read, possibly by the source
                    // finishing the trace itself, discards the sample, which could otherwise
                    // land after that finish returned.
                    if stop_requested(&stopped) {
                        break;
                    }
                    if let Some(rss_bytes) = rss_bytes {
                        write_sample(&config, rss_bytes, !wrote_descriptor);
                        wrote_descriptor = true;
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
    /// Waits for the thread to exit, after which it writes nothing and has released the writer.
    ///
    /// The thread wakes from its wait between samples at once, so this waits only for a sample
    /// that is in progress.
    pub(crate) fn join(self) {
        // A sampler that panicked has stopped writing too, so its panic is not propagated.
        let _ = self.0.join();
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

/// Writes one counter sample of `rss_bytes`, preceded by the counter track's descriptor if
/// `include_descriptor` is set.
fn write_sample<W: PerfettoWriter>(
    config: &ProcessMemorySamplerConfig<W>,
    rss_bytes: u64,
    include_descriptor: bool,
) {
    let mut packet = config.sequence.timestamped_packet();

    let counter_value = i64::try_from(rss_bytes).unwrap_or(i64::MAX);
    let mut event = idl::TrackEvent {
        track_uuid: Some(config.counter_track_uuid),
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
            uuid: Some(config.counter_track_uuid),
            parent_uuid: Some(config.sequence.process_track_uuid()),
            static_or_dynamic_name: Some(packets::track_name("process.rss")),
            counter: Some(counter),
            ..Default::default()
        }));
    }
    trace_packets.push(packet);

    config.sequence.write(
        config.writer.as_ref(),
        &idl::Trace {
            packet: trace_packets,
        },
    );
}
