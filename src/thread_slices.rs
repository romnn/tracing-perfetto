//! The spans that a layer has entered on one thread, whose entered slices nest on the thread's
//! track.
//!
//! A span exits in any order and may be entered several times at once, on one thread or on
//! several, so every exit is matched against the exiting thread's own stack.

use crate::packets::UnplacedEvent;
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
    begin: UnplacedEvent,
}

impl EnteredStack {
    /// Records that `span_id` was entered with the slice that `begin` opens.
    pub(crate) fn enter(&mut self, span_id: Id, begin: UnplacedEvent) {
        self.frames.push(EnteredFrame { span_id, begin });
    }

    /// Removes the innermost frame of `span_id` and returns the events that end its slice.
    ///
    /// The events are to be written on the thread's track in order, all at one timestamp.
    /// Slices entered above the exited one end first and begin again right after it, unchanged,
    /// so the thread's track keeps a valid stack even when spans exit out of order.
    /// The events are empty when the span is not on the stack, so an unmatched exit ends
    /// nothing.
    pub(crate) fn exit(&mut self, span_id: &Id) -> Vec<UnplacedEvent> {
        let Some(position) = self
            .frames
            .iter()
            .rposition(|frame| frame.span_id == *span_id)
        else {
            return Vec::new();
        };
        let above = self.frames.split_off(position + 1);
        self.frames.truncate(position);

        // End the slices above and the exited slice, innermost first, then resume the slices
        // above outermost first.
        // An end carries no annotations of an entered slice, so every end is the same.
        let ends = above.len() + 1;
        let mut events = Vec::with_capacity(ends + above.len());
        events.extend(std::iter::repeat_with(|| UnplacedEvent::slice_end(Vec::new())).take(ends));
        events.extend(above.iter().map(|frame| frame.begin.clone()));
        self.frames.extend(above);
        events
    }
}
