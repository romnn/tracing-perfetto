//! The trusted packet sequence a layer writes on, with its clock and preamble.

use crate::{idl, packets};
use std::time::{Duration, Instant, SystemTime};

/// The clock that every timestamped packet of a layer refers to.
///
/// IDs 64 to 127 are user-defined clocks scoped to one trusted packet sequence, so each layer
/// defines its own clock without colliding with another layer's.
pub(crate) const SEQUENCE_CLOCK_ID: u32 = 64;

/// The clock that trace processor converts every timestamp to.
///
/// A system trace that `traced` records on Linux or Android is timestamped in boot time.
/// Trace processor keeps the trace clock of whichever trace in a concatenated file declares it
/// first, so declaring boot time here aligns the layer's slices with such a trace exactly in
/// either order, and never through the wall clock, which NTP steps and slews.
#[cfg(any(target_os = "linux", target_os = "android"))]
const TRACE_CLOCK: idl::BuiltinClock = idl::BuiltinClock::Boottime;
/// The clock that trace processor converts every timestamp to.
///
/// No boot time reading is available here, so the wall clock is the trace clock.
#[cfg(not(any(target_os = "linux", target_os = "android")))]
const TRACE_CLOCK: idl::BuiltinClock = idl::BuiltinClock::Realtime;

/// The packet sequence shared by a layer and its process-memory sampler.
///
/// Every packet that reads [`SEQUENCE_CLOCK_ID`] must follow the preamble that defines it, so
/// packets reach the writer only through the [`TraceSink`](crate::sink::TraceSink) that writes
/// the preamble first.
pub(crate) struct TraceSequence {
    sequence_id: u32,
    process_track_uuid: u64,
    /// The zero point of [`SEQUENCE_CLOCK_ID`].
    epoch: Instant,
}

impl TraceSequence {
    /// Starts a sequence with a random sequence ID and a unique process track UUID.
    ///
    /// Sequence ID 0 is invalid and the tracing service reserves 1 for its own packets, so the
    /// random ID starts at 2.
    pub(crate) fn new() -> Self {
        Self {
            sequence_id: rand::random_range(2..=u32::MAX),
            process_track_uuid: packets::unique_uuid(),
            epoch: Instant::now(),
        }
    }

    /// Returns the UUID of the process track that the preamble describes.
    pub(crate) fn process_track_uuid(&self) -> u64 {
        self.process_track_uuid
    }

    /// Returns the current time on [`SEQUENCE_CLOCK_ID`], in nanoseconds since the epoch.
    pub(crate) fn now(&self) -> u64 {
        duration_nanos(self.epoch.elapsed())
    }

    /// Returns a packet on this sequence, timestamped now on [`SEQUENCE_CLOCK_ID`].
    pub(crate) fn timestamped_packet(&self) -> idl::TracePacket {
        self.packet_at(self.now())
    }

    /// Returns a packet on this sequence, timestamped at `timestamp` on [`SEQUENCE_CLOCK_ID`].
    pub(crate) fn packet_at(&self, timestamp: u64) -> idl::TracePacket {
        idl::TracePacket {
            timestamp: Some(timestamp),
            timestamp_clock_id: Some(SEQUENCE_CLOCK_ID),
            ..self.untimestamped_packet()
        }
    }

    /// Returns a packet on this sequence without a timestamp.
    ///
    /// The sequence ID scopes [`SEQUENCE_CLOCK_ID`] to this sequence.
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

    /// Builds the process track descriptor and the clock snapshot that defines
    /// [`SEQUENCE_CLOCK_ID`].
    ///
    /// The snapshot pairs the sequence clock with the wall clock and, on Linux and Android, with
    /// boot time and monotonic time.
    /// It makes [`TRACE_CLOCK`] the trace clock, so trace processor keeps the monotonic ordering.
    /// The Perfetto UI shows wall-clock time through the snapshot's wall-clock reading whichever
    /// clock is the trace clock.
    pub(crate) fn preamble_trace(&self) -> idl::Trace {
        // Read all clocks back to back so that the snapshot pins their offsets.
        let sequence_clock = snapshot_clock(Some(SEQUENCE_CLOCK_ID), self.now());
        #[cfg(any(target_os = "linux", target_os = "android"))]
        let kernel_clocks = [
            kernel_clock(idl::BuiltinClock::Boottime, rustix::time::ClockId::Boottime),
            kernel_clock(
                idl::BuiltinClock::Monotonic,
                rustix::time::ClockId::Monotonic,
            ),
        ];
        #[cfg(not(any(target_os = "linux", target_os = "android")))]
        let kernel_clocks = [];
        let realtime_timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_or(0, duration_nanos);
        let realtime_clock = snapshot_clock(
            builtin_clock_id(idl::BuiltinClock::Realtime),
            realtime_timestamp,
        );

        let clock_snapshot = idl::ClockSnapshot {
            clocks: [sequence_clock]
                .into_iter()
                .chain(kernel_clocks)
                .chain([realtime_clock])
                .collect(),
            primary_trace_clock: Some(TRACE_CLOCK.into()),
        };

        idl::Trace {
            packet: vec![
                packets::process_descriptor_packet(self.process_track_uuid),
                idl::TracePacket {
                    data: Some(idl::trace_packet::Data::ClockSnapshot(clock_snapshot)),
                    ..self.untimestamped_packet()
                },
            ],
        }
    }
}

/// Returns `duration` in nanoseconds, saturating at [`u64::MAX`] after about 584 years.
fn duration_nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

/// Returns a snapshot entry that reads `timestamp` on `clock_id`.
fn snapshot_clock(clock_id: Option<u32>, timestamp: u64) -> idl::clock_snapshot::Clock {
    idl::clock_snapshot::Clock {
        clock_id,
        timestamp: Some(timestamp),
        ..Default::default()
    }
}

/// Returns the snapshot ID of a builtin clock.
fn builtin_clock_id(clock: idl::BuiltinClock) -> Option<u32> {
    u32::try_from(i32::from(clock)).ok()
}

/// Returns a snapshot entry that reads the kernel clock `id` now as the builtin clock `clock`.
#[cfg(any(target_os = "linux", target_os = "android"))]
fn kernel_clock(clock: idl::BuiltinClock, id: rustix::time::ClockId) -> idl::clock_snapshot::Clock {
    let timestamp = Duration::try_from(rustix::time::clock_gettime(id)).map_or(0, duration_nanos);
    snapshot_clock(builtin_clock_id(clock), timestamp)
}
