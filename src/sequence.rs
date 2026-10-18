//! The trusted packet sequence a layer writes on, with its clock and preamble.

use crate::{PerfettoWriter, idl, idl_helpers};
use bytes::BytesMut;
use prost::Message;
use std::sync::Once;
use std::time::{Duration, Instant, SystemTime};

/// The clock that every timestamped packet of a layer refers to.
///
/// IDs 64 to 127 are user-defined clocks scoped to one trusted packet sequence, so each layer
/// defines its own clock without colliding with another layer's.
pub const SEQUENCE_CLOCK_ID: u32 = 64;

/// The packet sequence shared by a layer and its process-memory sampler.
///
/// Every packet that reads [`SEQUENCE_CLOCK_ID`] must follow the preamble that defines it, so
/// [`TraceSequence::write`] is the only way packets reach the writer.
pub struct TraceSequence {
    sequence_id: u32,
    process_track_uuid: u64,
    /// The zero point of [`SEQUENCE_CLOCK_ID`].
    epoch: Instant,
    preamble: Once,
}

impl TraceSequence {
    /// Starts a sequence with a random sequence ID and a random process track UUID.
    ///
    /// Sequence ID 0 is invalid and the tracing service reserves 1 for its own packets, so the
    /// random ID starts at 2.
    pub fn new() -> Self {
        Self {
            sequence_id: rand::random_range(2..=u32::MAX),
            process_track_uuid: idl_helpers::unique_uuid(),
            epoch: Instant::now(),
            preamble: Once::new(),
        }
    }

    pub fn process_track_uuid(&self) -> u64 {
        self.process_track_uuid
    }

    /// Returns a packet on this sequence, timestamped now on [`SEQUENCE_CLOCK_ID`].
    pub fn timestamped_packet(&self) -> idl::TracePacket {
        idl::TracePacket {
            timestamp: Some(duration_nanos(self.epoch.elapsed())),
            timestamp_clock_id: Some(SEQUENCE_CLOCK_ID),
            ..self.untimestamped_packet()
        }
    }

    fn untimestamped_packet(&self) -> idl::TracePacket {
        idl::TracePacket {
            trusted_pid: Some(i32::try_from(std::process::id()).unwrap_or(i32::MAX)),
            optional_trusted_packet_sequence_id: Some(
                idl::trace_packet::OptionalTrustedPacketSequenceId::TrustedPacketSequenceId(
                    self.sequence_id,
                ),
            ),
            ..Default::default()
        }
    }

    /// Writes `trace` to `writer`, preceded by this sequence's preamble on the first write.
    ///
    /// Concurrent first writes block until the preamble is written, so on a writer that applies
    /// writes in call order no packet of this sequence precedes it.
    pub fn write<W: PerfettoWriter + ?Sized>(&self, writer: &W, trace: &idl::Trace) {
        // Forcing past a poisoned guard retries the preamble after a panicking writer, instead of
        // panicking on every later write.
        self.preamble
            .call_once_force(|_| write_encoded(writer, &self.preamble_trace()));
        write_encoded(writer, trace);
    }

    /// Builds the process track descriptor and the clock snapshot that defines
    /// [`SEQUENCE_CLOCK_ID`].
    ///
    /// The snapshot pairs the sequence clock with the wall clock and makes the wall clock the trace
    /// clock, so trace processor keeps the monotonic ordering and displays wall-clock time.
    fn preamble_trace(&self) -> idl::Trace {
        // Read both clocks back to back so that the snapshot pins their offset.
        let sequence_timestamp = duration_nanos(self.epoch.elapsed());
        let realtime_timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_or(0, duration_nanos);

        let realtime_clock = idl::BuiltinClock::Realtime;
        let clock_snapshot = idl::ClockSnapshot {
            clocks: vec![
                idl::clock_snapshot::Clock {
                    clock_id: Some(SEQUENCE_CLOCK_ID),
                    timestamp: Some(sequence_timestamp),
                    ..Default::default()
                },
                idl::clock_snapshot::Clock {
                    clock_id: u32::try_from(i32::from(realtime_clock)).ok(),
                    timestamp: Some(realtime_timestamp),
                    ..Default::default()
                },
            ],
            primary_trace_clock: Some(realtime_clock.into()),
        };

        idl::Trace {
            packet: vec![
                idl_helpers::process_descriptor(self.process_track_uuid),
                idl::TracePacket {
                    data: Some(idl::trace_packet::Data::ClockSnapshot(clock_snapshot)),
                    ..self.untimestamped_packet()
                },
            ],
        }
    }
}

fn duration_nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn write_encoded<W: PerfettoWriter + ?Sized>(writer: &W, trace: &idl::Trace) {
    let mut buf = BytesMut::new();
    let Ok(()) = trace.encode(&mut buf) else {
        return;
    };
    _ = writer.write_log(buf);
}
