//! Regression tests for the debug annotations that event fields are written as.

use crate::idl;
use crate::tests::{annotated_layer, decode, instant_marked};
use googletest::prelude::*;
use tracing_subscriber::layer::SubscriberExt;

/// Returns the value of the annotation named `name`, or `None` if `annotations` has none.
fn annotation_value<'a>(
    annotations: &'a [idl::DebugAnnotation],
    name: &str,
) -> Option<&'a idl::debug_annotation::Value> {
    annotations
        .iter()
        .find_map(|annotation| match &annotation.name_field {
            Some(idl::debug_annotation::NameField::Name(annotation_name))
                if annotation_name == name =>
            {
                annotation.value.as_ref()
            }
            _ => None,
        })
}

/// A `u64` field keeps its value as an unsigned annotation, including above `i64::MAX`.
///
/// Written as the `i64` of the same bits, a hash, an ID or a `u64::MAX` sentinel would show up
/// in the trace as a negative number.
#[gtest]
fn u64_fields_keep_their_value() -> Result<()> {
    let (layer, output) = annotated_layer();
    let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));

    let above_i64 = i64::MAX.cast_unsigned() + 1;
    tracing::info!(
        marker = "u64",
        max = u64::MAX,
        above_i64,
        small = 7_u64,
        "u64 fields"
    );

    let trace = decode(&output)?;
    let annotations = &instant_marked(&trace, "u64")?.event.debug_annotations;
    expect_eq!(
        annotation_value(annotations, "max"),
        Some(&idl::debug_annotation::Value::UintValue(u64::MAX))
    );
    expect_eq!(
        annotation_value(annotations, "above_i64"),
        Some(&idl::debug_annotation::Value::UintValue(above_i64))
    );
    expect_eq!(
        annotation_value(annotations, "small"),
        Some(&idl::debug_annotation::Value::UintValue(7))
    );
    Ok(())
}
