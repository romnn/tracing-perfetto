//! Constructors for the track events, track descriptors and trace packets that a layer writes.

use crate::idl;
use std::borrow::Cow;
use std::hash::{BuildHasher as _, RandomState};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::Metadata;

/// Returns an ID for a track or a correlation, which is never 0 and never repeats within the
/// process.
///
/// Each ID is a process-wide counter passed through a bijective mix keyed by the process ID, so
/// IDs differ between processes and need no central registry.
/// The process ID is read on every call, so a child forked after the parent drew IDs draws
/// different ones, although it inherits the parent's counter and hash keys.
/// The IDs do not come from the thread-local random generator, because spans and events are
/// still recorded during thread teardown, after that generator may be gone.
pub(crate) fn unique_uuid() -> u64 {
    static HASH_KEYS: OnceLock<RandomState> = OnceLock::new();
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    // The standard library's hash keys stay readable during thread teardown.
    let hash_keys = HASH_KEYS.get_or_init(RandomState::new);
    loop {
        let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
        let id = id_for(hash_keys, std::process::id(), counter);
        if id != 0 {
            return id;
        }
    }
}

/// Returns the ID that the process `pid` draws for `counter` under `hash_keys`.
///
/// For one process the IDs of distinct counters are distinct, because the mix is a bijection.
pub(crate) fn id_for(hash_keys: &RandomState, pid: u32, counter: u64) -> u64 {
    mix(hash_keys.hash_one(pid).wrapping_add(counter))
}

/// Returns the finalizer of the [SplitMix64] generator applied to `value`, a bijection on `u64`
/// that scatters consecutive inputs across the whole range.
///
/// [SplitMix64]: https://prng.di.unimi.it/splitmix64.c
fn mix(value: u64) -> u64 {
    let value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    let value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

/// Returns `name` as the static-or-dynamic name of a track descriptor.
pub(crate) fn track_name(name: &str) -> idl::track_descriptor::StaticOrDynamicName {
    idl::track_descriptor::StaticOrDynamicName::Name(name.to_owned())
}

/// Wraps `descriptor` in a packet without a timestamp or sequence.
pub(crate) fn track_descriptor_packet(descriptor: idl::TrackDescriptor) -> idl::TracePacket {
    idl::TracePacket {
        data: Some(idl::trace_packet::Data::TrackDescriptor(descriptor)),
        ..Default::default()
    }
}

/// Returns a counter event that sets the counter track `track_uuid` to `value`.
pub(crate) fn counter_event(
    track_uuid: u64,
    value: idl::track_event::CounterValueField,
) -> idl::TrackEvent {
    let mut event = idl::TrackEvent {
        track_uuid: Some(track_uuid),
        counter_value_field: Some(value),
        ..Default::default()
    };
    event.set_type(idl::track_event::Type::Counter);
    event
}

/// Returns the packet that describes the process track `process_track_uuid` of this process.
pub(crate) fn process_descriptor_packet(process_track_uuid: u64) -> idl::TracePacket {
    let process = idl::ProcessDescriptor {
        pid: Some(i32::try_from(std::process::id()).unwrap_or(i32::MAX)),
        process_name: process_name(),
        ..Default::default()
    };
    track_descriptor_packet(idl::TrackDescriptor {
        uuid: Some(process_track_uuid),
        process: Some(process),
        ..Default::default()
    })
}

/// Returns the file name of the running executable, which names the process in the trace.
///
/// Returns `None` if the executable's path is unavailable, in which case the trace names the
/// process only by its ID.
fn process_name() -> Option<String> {
    let executable = std::env::current_exe().ok()?;
    Some(executable.file_name()?.to_string_lossy().into_owned())
}

/// A track event that is not yet on a track.
///
/// The write path that places the event adds the track, so no event is built with a track it
/// does not end up on.
#[derive(Clone, Debug)]
pub(crate) struct UnplacedEvent {
    pub(crate) event_type: idl::track_event::Type,
    pub(crate) name: Option<Cow<'static, str>>,
    pub(crate) location: Option<SourceLocation>,
    /// The fields of the span or event, which become the event's arguments.
    pub(crate) debug_annotations: Vec<idl::DebugAnnotation>,
    /// The ID that links the slices of one span across tracks.
    pub(crate) correlation_id: Option<u64>,
}

/// Where in the source a span or event was declared.
#[derive(Clone, Copy, Debug)]
pub(crate) struct SourceLocation {
    pub(crate) file: &'static str,
    pub(crate) line: u32,
}

impl SourceLocation {
    /// Returns the location that `metadata` declares, if it names both a file and a line.
    pub(crate) fn of(metadata: &'static Metadata<'static>) -> Option<Self> {
        Some(Self {
            file: metadata.file()?,
            line: metadata.line()?,
        })
    }
}

impl UnplacedEvent {
    /// Returns the end of a slice, carrying `debug_annotations`.
    ///
    /// The end carries no name or location, because trace processor takes them from the begin and
    /// merges the end's annotations into the slice.
    pub(crate) fn slice_end(debug_annotations: Vec<idl::DebugAnnotation>) -> Self {
        Self {
            event_type: idl::track_event::Type::SliceEnd,
            name: None,
            location: None,
            debug_annotations,
            correlation_id: None,
        }
    }

    /// Places the event on the track `track_uuid`.
    pub(crate) fn on_track(self, track_uuid: u64) -> idl::TrackEvent {
        let Self {
            event_type,
            name,
            location,
            debug_annotations,
            correlation_id,
        } = self;
        let mut event = idl::TrackEvent {
            track_uuid: Some(track_uuid),
            name_field: name.map(|name| idl::track_event::NameField::Name(name.into_owned())),
            debug_annotations,
            source_location_field: location.map(|SourceLocation { file, line }| {
                idl::track_event::SourceLocationField::SourceLocation(idl::SourceLocation {
                    file_name: Some(file.to_owned()),
                    line_number: Some(line),
                    ..Default::default()
                })
            }),
            correlation_id_field: correlation_id
                .map(idl::track_event::CorrelationIdField::CorrelationId),
            ..Default::default()
        };
        event.set_type(event_type);
        event
    }
}
