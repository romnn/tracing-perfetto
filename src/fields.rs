//! Visitors that read the fields of spans and events, as debug annotations, as event counters,
//! and for the naming fields.

use crate::idl;
use crate::idl::track_event::CounterValueField;
use tracing::field::{Field, Visit};

/// The span field that gives a span its own track below the process track.
const TRACK_NAME_FIELD: &str = "perfetto.track_name";

/// The span field that names a span's slices.
const SLICE_NAME_FIELD: &str = "perfetto.slice_name";

/// The OpenTelemetry span name, which names a span's slices unless [`SLICE_NAME_FIELD`] does.
const OTEL_NAME_FIELD: &str = "otel.name";

/// The prefix of the event fields whose numeric values are written on counter tracks.
const COUNTER_FIELD_PREFIX: &str = "counter.";

/// Collects every visited field as a debug annotation named after the field.
///
/// Booleans, floats and 64-bit integers keep their type.
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
        self.push(field, idl::debug_annotation::Value::UintValue(value));
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

/// A numeric value of an event field named `counter.<name>` or `counter.<name>.<unit>`.
pub(crate) struct CounterSample {
    /// The name of the counter, which is the field's name without the prefix and without the
    /// last `.`-segment, if the rest has more than one.
    pub(crate) name: &'static str,
    /// The last `.`-segment of the field's name after the prefix, if the rest has more than one.
    pub(crate) unit: Option<&'static str>,
    pub(crate) value: CounterValueField,
}

/// Collects the numeric [`COUNTER_FIELD_PREFIX`] fields of an event as counter samples, and, if
/// it collects debug annotations, the event's other fields as debug annotations.
///
/// A [`COUNTER_FIELD_PREFIX`] field of any other type, such as a string or a boolean, is a debug
/// annotation like any other field.
pub(crate) struct EventFields {
    pub(crate) counters: Vec<CounterSample>,
    /// The debug annotations, or `None` if the fields other than the counters are not read.
    pub(crate) annotations: Option<DebugAnnotations>,
}

impl EventFields {
    /// Returns a visitor that collects debug annotations only if `debug_annotations` holds.
    pub(crate) fn new(debug_annotations: bool) -> Self {
        Self {
            counters: Vec::new(),
            annotations: debug_annotations.then(DebugAnnotations::default),
        }
    }

    /// Collects `value` as a sample and returns `true` if `field` is a counter field.
    fn record_counter(&mut self, field: &Field, value: CounterValueField) -> bool {
        let Some(counter) = field.name().strip_prefix(COUNTER_FIELD_PREFIX) else {
            return false;
        };
        let (name, unit) = match counter.rsplit_once('.') {
            Some((name, unit)) => (name, Some(unit)),
            None => (counter, None),
        };
        self.counters.push(CounterSample { name, unit, value });
        true
    }
}

/// Returns `value` as an integer counter value if it fits an `i64`, and else as the nearest
/// double.
fn integer_counter_value(value: i128) -> CounterValueField {
    match i64::try_from(value) {
        Ok(value) => CounterValueField::CounterValue(value),
        #[expect(
            clippy::cast_precision_loss,
            reason = "a counter value is an i64 or a double, so a value beyond the i64 range keeps \
                      its magnitude as the nearest double"
        )]
        Err(_) => CounterValueField::DoubleCounterValue(value as f64),
    }
}

impl Visit for EventFields {
    fn record_bool(&mut self, field: &Field, value: bool) {
        if let Some(annotations) = &mut self.annotations {
            annotations.record_bool(field, value);
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if let Some(annotations) = &mut self.annotations {
            annotations.record_str(field, value);
        }
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        if !self.record_counter(field, CounterValueField::DoubleCounterValue(value))
            && let Some(annotations) = &mut self.annotations
        {
            annotations.record_f64(field, value);
        }
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        if !self.record_counter(field, CounterValueField::CounterValue(value))
            && let Some(annotations) = &mut self.annotations
        {
            annotations.record_i64(field, value);
        }
    }

    fn record_i128(&mut self, field: &Field, value: i128) {
        if !self.record_counter(field, integer_counter_value(value))
            && let Some(annotations) = &mut self.annotations
        {
            annotations.record_i128(field, value);
        }
    }

    fn record_u128(&mut self, field: &Field, value: u128) {
        #[expect(
            clippy::cast_precision_loss,
            reason = "a counter value is an i64 or a double, so a value beyond the i128 range keeps \
                      its magnitude as the nearest double"
        )]
        let counter_value = i128::try_from(value).map_or(
            CounterValueField::DoubleCounterValue(value as f64),
            integer_counter_value,
        );
        if !self.record_counter(field, counter_value)
            && let Some(annotations) = &mut self.annotations
        {
            annotations.record_u128(field, value);
        }
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        if !self.record_counter(field, integer_counter_value(i128::from(value)))
            && let Some(annotations) = &mut self.annotations
        {
            annotations.record_u64(field, value);
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if let Some(annotations) = &mut self.annotations {
            annotations.record_debug(field, value);
        }
    }

    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        if let Some(annotations) = &mut self.annotations {
            annotations.record_error(field, value);
        }
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
