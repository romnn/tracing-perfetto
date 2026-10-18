//! The spans that each layer has entered on the calling thread, whose entered slices nest on the
//! thread's track.
//!
//! A span exits in any order and may be entered several times at once, on one thread or on
//! several, so every exit is matched against the calling thread's own stack.

use crate::layer::LayerId;
use crate::{idl, packets};
use std::cell::RefCell;
use tracing::Id;

thread_local! {
    /// The entered spans of every layer that has one open on the calling thread.
    static ENTERED_SPANS: RefCell<Vec<LayerEnteredSpans>> = const { RefCell::new(Vec::new()) };
}

/// The spans one layer has entered on the calling thread, innermost last.
struct LayerEnteredSpans {
    layer_id: LayerId,
    frames: Vec<EnteredFrame>,
}

/// An entered span whose slice is open on the calling thread's track.
struct EnteredFrame {
    span_id: Id,
    /// The begin of the frame's slice, written again when the slice resumes after a non-LIFO
    /// exit.
    begin: idl::TrackEvent,
}

impl EnteredFrame {
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

/// Records that the layer `layer_id` entered `span_id` on the calling thread with `begin`.
///
/// Returns `false` during thread teardown, when the stack is gone, in which case the caller
/// writes no begin, since no exit could end its slice.
pub(crate) fn enter(layer_id: LayerId, span_id: Id, begin: &idl::TrackEvent) -> bool {
    ENTERED_SPANS
        .try_with(|layers| {
            let frame = EnteredFrame {
                span_id,
                begin: begin.clone(),
            };
            let mut layers = layers.borrow_mut();
            if let Some(entered) = layers
                .iter_mut()
                .find(|entered| entered.layer_id == layer_id)
            {
                entered.frames.push(frame);
            } else {
                layers.push(LayerEnteredSpans {
                    layer_id,
                    frames: vec![frame],
                });
            }
        })
        .is_ok()
}

/// Removes the innermost entry of `span_id` from the calling thread's stack of `layer_id`.
///
/// Returns the slice events to write on the thread's track, in order, all at one timestamp.
/// They are empty when the span is not on the stack, so an unmatched exit ends nothing.
pub(crate) fn exit(layer_id: LayerId, span_id: &Id) -> Vec<idl::TrackEvent> {
    ENTERED_SPANS
        .try_with(|layers| {
            let mut layers = layers.borrow_mut();
            let Some(index) = layers
                .iter()
                .position(|entered| entered.layer_id == layer_id)
            else {
                return Vec::new();
            };
            let Some(entered) = layers.get_mut(index) else {
                return Vec::new();
            };
            let events = entered.exit(span_id);

            // Drop an empty stack, so a finished layer leaves nothing behind on the thread.
            if entered.frames.is_empty() {
                layers.swap_remove(index);
            }
            events
        })
        .unwrap_or_default()
}

impl LayerEnteredSpans {
    /// Removes the innermost frame of `span_id` and returns the events that end its slice.
    ///
    /// Slices entered above it end first and begin again right after it, unchanged, so the
    /// thread's track keeps a valid stack even when spans exit out of order.
    fn exit(&mut self, span_id: &Id) -> Vec<idl::TrackEvent> {
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
