//! Regression tests for user formatting that runs while the layer records a span's fields.

use crate::tests::{
    annotated_layer, annotation_names, decode, replay_lifetime_slices, slice_named,
    string_annotation,
};
use googletest::prelude::*;
use std::fmt;
use std::panic::{self, AssertUnwindSafe};
use std::sync::mpsc;
use std::time::Duration;
use tracing::{field, trace_span};
use tracing_subscriber::layer::SubscriberExt;

/// A value whose `Debug` rendering records the field `b` into the span it holds.
struct RecordsWhileFormatted(tracing::Span);

impl fmt::Debug for RecordsWhileFormatted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.record("b", 1);
        formatter.write_str("formatted")
    }
}

/// A value whose `Debug` rendering panics.
struct PanicsWhenFormatted;

impl fmt::Debug for PanicsWhenFormatted {
    fn fmt(&self, _formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        panic!("formatting failed")
    }
}

/// A recorded value whose formatting records another field into the same span completes, and
/// the span's end carries both fields.
///
/// A layer that formatted the value while holding the span's extensions lock would take that
/// lock again from the nested record, and the thread would deadlock.
#[gtest]
fn recording_from_inside_a_recorded_value_completes() -> Result<()> {
    let (layer, output) = annotated_layer();

    // The scenario runs on its own thread, so a deadlock fails the test after the bound instead of
    // hanging the suite, and the stuck thread is left behind.
    let (completed, completed_receiver) = mpsc::channel();
    std::thread::spawn(move || {
        let _default = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));
        let span = trace_span!("reentrant", a = field::Empty, b = field::Empty);
        span.record("a", field::debug(RecordsWhileFormatted(span.clone())));
        drop(span);
        completed.send(()).ok();
    });

    // The bound is far above any scheduling delay, and the receiver also fails if the thread
    // panicked instead.
    verify_that!(
        completed_receiver.recv_timeout(Duration::from_secs(30)),
        ok(anything())
    )?;

    // The nested record and the outer one both reach the end of the span's lifetime slice.
    let slices = replay_lifetime_slices(&decode(&output)?)?;
    let end_annotations = &slice_named(&slices, "reentrant")?.end_annotations;
    expect_that!(
        annotation_names(end_annotations),
        unordered_elements_are![eq(&"a"), eq(&"b")]
    );
    expect_eq!(string_annotation(end_annotations, "a"), Some("formatted"));
    Ok(())
}

/// A recorded value whose formatting panics leaves the layer able to record that span and other
/// spans, once the caller has caught the panic.
///
/// A layer that formatted the value while holding the span's extensions lock would poison the
/// lock, and every later access to that span's extensions, including its close, would panic.
/// The registry reuses the poisoned slot for later spans, so they would panic too.
#[gtest]
fn panicking_recorded_value_leaves_spans_recordable() -> Result<()> {
    let (layer, output) = annotated_layer();
    let _default = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));

    let panicking = trace_span!("panicking", a = field::Empty, b = field::Empty);
    let concurrent = trace_span!("concurrent", c = field::Empty);
    let unwound = panic::catch_unwind(AssertUnwindSafe(|| {
        panicking.record("a", field::debug(PanicsWhenFormatted));
    }));
    verify_that!(unwound, err(anything()))?;

    // Record into the span whose formatting panicked, into a span that was open at the time, and
    // into a span created after the first one closed.
    panicking.record("b", "after the panic");
    drop(panicking);
    concurrent.record("c", "after the panic");
    drop(concurrent);
    let later = trace_span!("later", d = field::Empty);
    later.record("d", "after the panic");
    drop(later);

    // Every field recorded after the panic reaches the end of its span's lifetime slice, and the
    // field whose formatting panicked is left out.
    let slices = replay_lifetime_slices(&decode(&output)?)?;
    for (name, field) in [("panicking", "b"), ("concurrent", "c"), ("later", "d")] {
        let end_annotations = &slice_named(&slices, name)?.end_annotations;
        expect_that!(annotation_names(end_annotations), elements_are![eq(&field)]);
        expect_eq!(
            string_annotation(end_annotations, field),
            Some("after the panic")
        );
    }
    Ok(())
}
