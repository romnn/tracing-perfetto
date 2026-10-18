//! Constructors for the track events, track descriptors and trace packets that a layer writes.

use crate::idl;

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
        ..Default::default()
    };
    track_descriptor_packet(idl::TrackDescriptor {
        uuid: Some(process_track_uuid),
        process: Some(process),
        ..Default::default()
    })
}

/// Builds a track event of `event_type` on the track `track_uuid`.
///
/// `location` is the source file and line, and `debug_annotations` become the event's
/// arguments.
pub(crate) fn create_event(
    track_uuid: u64,
    name: Option<&str>,
    location: Option<(&str, u32)>,
    debug_annotations: Vec<idl::DebugAnnotation>,
    event_type: idl::track_event::Type,
) -> idl::TrackEvent {
    let mut event = idl::TrackEvent {
        track_uuid: Some(track_uuid),
        name_field: name.map(|name| idl::track_event::NameField::Name(name.to_string())),
        debug_annotations,
        ..Default::default()
    };
    event.set_type(event_type);
    if let Some((file, line)) = location {
        let source_location = idl::SourceLocation {
            file_name: Some(file.to_owned()),
            line_number: Some(line),
            ..Default::default()
        };
        let location = idl::track_event::SourceLocationField::SourceLocation(source_location);
        event.source_location_field = Some(location);
    }
    event
}
