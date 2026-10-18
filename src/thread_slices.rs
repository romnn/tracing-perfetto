//! The spans that a layer has entered on one thread, whose entered slices nest on the thread's
//! track.
//!
//! A span exits in any order and may be entered several times at once, on one thread or on
//! several, so every exit is matched against the exiting thread's own stack.

use crate::{idl, packets};
use tracing::Id;

/// The spans a layer has entered on one thread, innermost last.
#[derive(Default)]
pub(crate) struct EnteredStack {
    frames: Vec<EnteredFrame>,
}

/// An entered span whose slice is open on the thread's track.
struct EnteredFrame {
    span_id: Id,
    /// The begin of the frame's slice, written again when the slice resumes after a non-LIFO
    /// exit.
    begin: idl::TrackEvent,
}

impl EnteredFrame {
    /// Builds the end of the frame's slice, which carries no name, location or annotations.
    fn end(&self) -> idl::TrackEvent {
        packets::create_event(
            self.begin.track_uuid(),
            None,
            None,
            Vec::new(),
            idl::track_event::Type::SliceEnd,
        )
    }
}

impl EnteredStack {
    /// Records that `span_id` was entered with the slice that `begin` opens.
    pub(crate) fn enter(&mut self, span_id: Id, begin: idl::TrackEvent) {
        self.frames.push(EnteredFrame { span_id, begin });
    }

    /// Removes the innermost frame of `span_id` and returns the events that end its slice.
    ///
    /// The events are to be written in order, all at one timestamp.
    /// Slices entered above the exited one end first and begin again right after it, unchanged,
    /// so the thread's track keeps a valid stack even when spans exit out of order.
    /// The events are empty when the span is not on the stack, so an unmatched exit ends
    /// nothing.
    pub(crate) fn exit(&mut self, span_id: &Id) -> Vec<idl::TrackEvent> {
        let Some(position) = self
            .frames
            .iter()
            .rposition(|frame| frame.span_id == *span_id)
        else {
            return Vec::new();
        };
        let above = self.frames.split_off(position + 1);
        let Some(exited) = self.frames.pop() else {
            return Vec::new();
        };

        // End the slices above innermost first, then the exited slice, then resume the slices
        // above outermost first.
        let mut events = Vec::with_capacity(2 * above.len() + 1);
        events.extend(above.iter().rev().map(EnteredFrame::end));
        events.push(exited.end());
        events.extend(above.iter().map(|frame| frame.begin.clone()));
        self.frames.extend(above);
        events
    }
}
