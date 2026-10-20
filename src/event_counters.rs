//! The counter tracks that the numeric `counter.*` fields of events write to.

use crate::fields::CounterSample;
use crate::sink::TraceSink;
use crate::writer::PerfettoWriter;
use crate::{idl, packets};
use std::collections::HashMap;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};

/// The counter tracks of a layer's event counters, one per counter name.
///
/// Every track is a child of the process track, so the values of one counter from every thread
/// are one series.
#[derive(Default)]
pub(crate) struct EventCounterTracks {
    /// The tracks by counter name.
    ///
    /// The lock is held across the write of an event's counter values, so whether a track is
    /// described changes only together with the write that describes it.
    tracks: parking_lot::Mutex<HashMap<&'static str, EventCounterTrack>>,
    /// The writes that found the lock held, so tests know when a write waits for another.
    #[cfg(test)]
    contentions: AtomicUsize,
}

struct EventCounterTrack {
    uuid: u64,
    /// Whether a message with the track's descriptor was written.
    described: bool,
}

impl EventCounterTracks {
    /// Writes `samples` at `timestamp` through `sink`, as one message that describes each track
    /// that is not described yet before its first value.
    ///
    /// A track is described with the unit of the first sample of its name, and counts as
    /// described only once that message is written, so no value lands on an undescribed track.
    /// A write of the first value of a counter waits for a write on another thread that
    /// describes it, so each track is described once.
    pub(crate) fn write<W: PerfettoWriter>(
        &self,
        sink: &TraceSink<W>,
        samples: Vec<CounterSample>,
        timestamp: u64,
    ) {
        if samples.is_empty() {
            return;
        }
        #[cfg(test)]
        let mut tracks = crate::sink::lock_counting_contention(&self.tracks, &self.contentions);
        #[cfg(not(test))]
        let mut tracks = self.tracks.lock();

        let sequence = sink.sequence();
        let mut trace_packets = Vec::with_capacity(2 * samples.len());
        let mut described_now = Vec::new();
        for CounterSample { name, unit, value } in samples {
            let track = tracks.entry(name).or_insert_with(|| EventCounterTrack {
                uuid: packets::unique_uuid(),
                described: false,
            });
            if !track.described {
                // Taken for described while the message is built, so that a counter which the
                // event names twice is described once.
                track.described = true;
                described_now.push(name);
                trace_packets.push(packets::track_descriptor_packet(idl::TrackDescriptor {
                    uuid: Some(track.uuid),
                    parent_uuid: Some(sequence.process_track_uuid()),
                    static_or_dynamic_name: Some(packets::track_name(name)),
                    counter: Some(counter_descriptor(unit)),
                    ..Default::default()
                }));
            }
            trace_packets.push(idl::TracePacket {
                data: Some(idl::trace_packet::Data::TrackEvent(packets::counter_event(
                    track.uuid, value,
                ))),
                ..sequence.packet_at(timestamp)
            });
        }

        let written = sink.write(&idl::Trace {
            packet: trace_packets,
        });
        if !written {
            for name in described_now {
                if let Some(track) = tracks.get_mut(name) {
                    track.described = false;
                }
            }
        }
    }

    /// Returns how many writes had to wait for another write.
    #[cfg(test)]
    pub(crate) fn contentions(&self) -> usize {
        self.contentions.load(Ordering::Relaxed)
    }
}

/// Returns the descriptor of a counter whose unit is `unit`.
///
/// A unit that the descriptor knows is its built-in unit, and any other one its free-form unit
/// name.
fn counter_descriptor(unit: Option<&str>) -> idl::CounterDescriptor {
    let mut counter = idl::CounterDescriptor::default();
    match unit {
        Some("bytes") => counter.set_unit(idl::counter_descriptor::Unit::SizeBytes),
        Some("ns") => counter.set_unit(idl::counter_descriptor::Unit::TimeNs),
        Some("count") => counter.set_unit(idl::counter_descriptor::Unit::Count),
        Some(unit_name) => counter.unit_name = Some(unit_name.to_owned()),
        None => {}
    }
    counter
}
