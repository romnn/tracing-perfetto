//! Unit tests for the stack of entered spans that a layer keeps per thread, checked directly
//! against the slice events each exit returns.

use crate::idl::track_event;
use crate::packets::UnplacedEvent;
use crate::thread_slices::EnteredStack;
use googletest::prelude::*;
use std::borrow::Cow;
use tracing::Id;

/// What an event that an exit returns does to the thread's track.
#[derive(Debug, PartialEq, Eq)]
enum SliceEvent {
    /// Ends the innermost open slice.
    End,
    /// Begins the slice of the span with this name again.
    Resume(String),
}

/// Returns the begin of an entered slice named `name`.
fn begin(name: &'static str) -> UnplacedEvent {
    UnplacedEvent {
        event_type: track_event::Type::SliceBegin,
        name: Some(Cow::Borrowed(name)),
        location: None,
        debug_annotations: Vec::new(),
        correlation_id: None,
    }
}

/// Returns what each of `events` does to the thread's track.
fn slice_events(events: &[UnplacedEvent]) -> Vec<SliceEvent> {
    events
        .iter()
        .map(|event| match event.event_type {
            track_event::Type::SliceEnd => SliceEvent::End,
            _ => SliceEvent::Resume(event.name.as_deref().unwrap_or_default().to_string()),
        })
        .collect()
}

/// Exits in reverse order of entry each end only the exited span's slice.
///
/// Ending more than the innermost slice on a nested exit would cut short the slices that
/// enclose it.
#[gtest]
fn nested_exits_end_one_slice_each() {
    let mut stack = EnteredStack::default();
    stack.enter(Id::from_u64(1), begin("outer"));
    stack.enter(Id::from_u64(2), begin("inner"));

    expect_eq!(
        slice_events(&stack.exit(&Id::from_u64(2))),
        [SliceEvent::End]
    );
    expect_eq!(
        slice_events(&stack.exit(&Id::from_u64(1))),
        [SliceEvent::End]
    );

    // Both frames are gone, so a further exit ends nothing.
    expect_that!(stack.exit(&Id::from_u64(1)), is_empty());
}

/// A span that exits below a span entered after it ends both slices and resumes the later one.
///
/// Popping only the innermost frame would end the later span's slice at the earlier span's
/// exit, and leave the earlier span's frame to be ended by the later span's exit.
#[gtest]
fn non_lifo_exit_ends_the_slices_above_and_resumes_them() {
    let mut stack = EnteredStack::default();
    stack.enter(Id::from_u64(1), begin("first"));
    stack.enter(Id::from_u64(2), begin("second"));
    stack.enter(Id::from_u64(3), begin("third"));

    // The slices above end innermost first and resume outermost first.
    expect_eq!(
        slice_events(&stack.exit(&Id::from_u64(1))),
        [
            SliceEvent::End,
            SliceEvent::End,
            SliceEvent::End,
            SliceEvent::Resume("second".to_string()),
            SliceEvent::Resume("third".to_string()),
        ]
    );

    // The resumed frames stay in order, and the exited one is gone.
    expect_eq!(
        slice_events(&stack.exit(&Id::from_u64(3))),
        [SliceEvent::End]
    );
    expect_eq!(
        slice_events(&stack.exit(&Id::from_u64(2))),
        [SliceEvent::End]
    );
    expect_that!(stack.exit(&Id::from_u64(1)), is_empty());
}

/// An exit of a span entered twice ends the innermost entry and keeps the outer one.
///
/// Matching the outermost entry would resume the inner entry's slice in place of the outer one,
/// which then ends at the wrong exit.
#[gtest]
fn exit_of_a_reentered_span_ends_its_innermost_entry() {
    let mut stack = EnteredStack::default();
    stack.enter(Id::from_u64(1), begin("reentered"));
    stack.enter(Id::from_u64(2), begin("between"));
    stack.enter(Id::from_u64(1), begin("reentered"));

    expect_eq!(
        slice_events(&stack.exit(&Id::from_u64(1))),
        [SliceEvent::End]
    );

    // The outer entry is still below the span entered between the two entries.
    expect_eq!(
        slice_events(&stack.exit(&Id::from_u64(1))),
        [
            SliceEvent::End,
            SliceEvent::End,
            SliceEvent::Resume("between".to_string()),
        ]
    );
}

/// An exit of a span that is not on the stack ends nothing and leaves the stack unchanged.
///
/// Ending the innermost slice instead would cut short a span that is still entered.
#[gtest]
fn unmatched_exit_leaves_the_stack_unchanged() {
    let mut stack = EnteredStack::default();
    stack.enter(Id::from_u64(1), begin("entered"));

    expect_that!(stack.exit(&Id::from_u64(2)), is_empty());
    expect_eq!(
        slice_events(&stack.exit(&Id::from_u64(1))),
        [SliceEvent::End]
    );
}
