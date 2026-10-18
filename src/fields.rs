//! Visitors that read the fields of spans and events, as debug annotations and for the naming
//! fields.

use crate::idl;
use tracing::field::{Field, Visit};

/// The span field that gives a span its own track below the process track.
const TRACK_NAME_FIELD: &str = "perfetto.track_name";

/// The span field that names a span's slices.
const SLICE_NAME_FIELD: &str = "perfetto.slice_name";

/// The OpenTelemetry span name, which names a span's slices unless [`SLICE_NAME_FIELD`] does.
const OTEL_NAME_FIELD: &str = "otel.name";

/// Collects every visited field as a debug annotation named after the field.
///
/// Booleans, floats and 64-bit integers keep their type, with a `u64` reinterpreted as the
/// `i64` of the same bits.
/// Strings, 128-bit integers, errors and [`Debug`](std::fmt::Debug) values become strings.
#[derive(Default)]
pub(crate) struct DebugAnnotations {
    pub(crate) annotations: Vec<idl::DebugAnnotation>,
}

impl DebugAnnotations {
    fn push(&mut self, field: &Field, value: idl::debug_annotation::Value) {
        self.annotations.push(idl::DebugAnnotation {
            name_field: Some(idl::debug_annotation::NameField::Name(
                field.name().to_string(),
            )),
            value: Some(value),
            ..Default::default()
        });
    }
}

impl Visit for DebugAnnotations {
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.push(field, idl::debug_annotation::Value::BoolValue(value));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.push(
            field,
            idl::debug_annotation::Value::StringValue(value.to_owned()),
        );
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        self.push(field, idl::debug_annotation::Value::DoubleValue(value));
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.push(field, idl::debug_annotation::Value::IntValue(value));
    }

    fn record_i128(&mut self, field: &Field, value: i128) {
        self.push(
            field,
            idl::debug_annotation::Value::StringValue(value.to_string()),
        );
    }

    fn record_u128(&mut self, field: &Field, value: u128) {
        self.push(
            field,
            idl::debug_annotation::Value::StringValue(value.to_string()),
        );
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.push(
            field,
            idl::debug_annotation::Value::IntValue(value.cast_signed()),
        );
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.push(
            field,
            idl::debug_annotation::Value::StringValue(format!("{value:?}")),
        );
    }

    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        self.push(
            field,
            idl::debug_annotation::Value::StringValue(format!("{value}")),
        );
    }
}

/// The fields of a new span that decide where its slices appear and what they are called.
///
/// A later [`SLICE_NAME_FIELD`] overrides an earlier [`OTEL_NAME_FIELD`], but not the reverse,
/// so `perfetto.slice_name` wins in either order.
#[derive(Default)]
pub(crate) struct NamingFields {
    /// The value of [`TRACK_NAME_FIELD`].
    pub(crate) track_name: Option<String>,
    /// The value of [`SLICE_NAME_FIELD`], or else of [`OTEL_NAME_FIELD`].
    pub(crate) slice_name: Option<String>,
}

impl NamingFields {
    /// Returns where the value of `field` is stored, or `None` if it names nothing.
    fn slot(&mut self, field: &Field) -> Option<&mut Option<String>> {
        match field.name() {
            TRACK_NAME_FIELD => Some(&mut self.track_name),
            SLICE_NAME_FIELD => Some(&mut self.slice_name),
            OTEL_NAME_FIELD if self.slice_name.is_none() => Some(&mut self.slice_name),
            _ => None,
        }
    }
}

impl Visit for NamingFields {
    /// Stores the [`Debug`](std::fmt::Debug) rendering of a naming field, without the quotes
    /// that a string's rendering adds.
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if let Some(slot) = self.slot(field) {
            let rendered = format!("{value:?}");
            let unquoted = rendered
                .strip_prefix('"')
                .and_then(|unprefixed| unprefixed.strip_suffix('"'))
                .unwrap_or(rendered.as_str());
            *slot = Some(unquoted.to_string());
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if let Some(slot) = self.slot(field) {
            *slot = Some(value.to_string());
        }
    }
}
