//! Decoding of written traces, for checking what a [`PerfettoLayer`](crate::PerfettoLayer)
//! recorded without depending on the Perfetto protobuf types.

use crate::idl;
use prost::Message;

/// The error returned when a buffer is not a valid perfetto `Trace` protobuf.
///
/// The underlying protobuf decode error is available through
/// [`source`](std::error::Error::source).
#[derive(Debug)]
pub struct DecodeError(prost::DecodeError);

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("invalid perfetto trace")
    }
}

impl std::error::Error for DecodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}

/// Decodes a perfetto trace buffer and returns the names of all contained track events.
///
/// The names are in packet order.
/// Events without a name, such as slice ends and counter samples, are skipped.
///
/// # Errors
///
/// Returns [`DecodeError`] if the buffer does not contain a valid perfetto `Trace` protobuf.
pub fn decode_track_event_names(buf: &[u8]) -> Result<Vec<String>, DecodeError> {
    let trace = idl::Trace::decode(buf).map_err(DecodeError)?;
    let mut names = Vec::new();
    for packet in trace.packet {
        let Some(idl::trace_packet::Data::TrackEvent(event)) = packet.data else {
            continue;
        };

        let Some(idl::track_event::NameField::Name(name)) = event.name_field else {
            continue;
        };
        names.push(name);
    }
    Ok(names)
}
