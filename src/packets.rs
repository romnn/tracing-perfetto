//! Constructors for the track events, track descriptors and trace packets that a layer writes.

use crate::idl;
use std::borrow::Cow;
use tracing::Metadata;

/// Returns a random ID for a track or a correlation, which is never 0.
///
/// IDs drawn from the full `u64` range do not collide in practice, so tracks and correlations
/// need no central registry.
pub(crate) fn unique_uuid() -> u64 {
    rand::random_range(1..=u64::MAX)
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
