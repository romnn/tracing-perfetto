//! The thread that samples a [`ProcessMemorySource`] onto a layer's `process.rss` counter track.

use crate::process_memory::ProcessMemorySource;
use crate::sequence::TraceSequence;
use crate::writer::PerfettoWriter;
use crate::{idl, packets};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

/// What a [`ProcessMemorySampler`] samples and where it writes the samples.
pub(crate) struct ProcessMemorySamplerConfig<W> {
    pub(crate) writer: Arc<W>,
    /// The layer's sequence, so the samples share its clock and preamble.
    pub(crate) sequence: Arc<TraceSequence>,
    pub(crate) counter_track_uuid: u64,
    pub(crate) interval: Duration,
    pub(crate) source: Box<dyn ProcessMemorySource>,
}

/// A running sampler thread, which writes until [`Self::stop`] joins it.
pub(crate) struct ProcessMemorySampler {
    stop: Arc<AtomicBool>,
    /// The sampler thread, or `None` if it could not be spawned.
    join_handle: parking_lot::Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl ProcessMemorySampler {
    /// Spawns a thread that samples `config.source` every `config.interval`.
    ///
    /// The counter track's descriptor is written with the first sample, and intervals without a
    /// value are skipped.
    /// If the thread cannot be spawned, nothing is sampled.
    pub(crate) fn spawn<W>(config: ProcessMemorySamplerConfig<W>) -> Self
    where
        W: PerfettoWriter + Send + Sync + 'static,
    {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_for_thread = Arc::clone(&stop);

        let join_handle = std::thread::Builder::new()
            .name("perfetto-process-memory".to_string())
            .spawn(move || {
                let mut wrote_descriptor = false;
                while !stop_for_thread.load(Ordering::Relaxed) {
                    if let Some(rss_bytes) = config.source.resident_set_size_bytes() {
                        write_sample(&config, rss_bytes, !wrote_descriptor);
                        wrote_descriptor = true;
                    }
                    std::thread::sleep(config.interval);
                }
            })
            .ok();

        Self {
            stop,
            join_handle: parking_lot::Mutex::new(join_handle),
        }
    }

    /// Stops the thread and waits for it, so it writes nothing afterwards.
    ///
    /// The thread notices the stop after its current interval, so this blocks for up to one
    /// interval.
    pub(crate) fn stop(self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.join_handle.lock().take() {
            // A sampler that panicked has stopped writing too, so its panic is not propagated.
            let _ = handle.join();
        }
    }
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
