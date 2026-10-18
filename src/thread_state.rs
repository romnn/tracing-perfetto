//! What a layer keeps of each thread that records through it.

use crate::thread_slices::EnteredStack;
use crate::thread_track;
use std::cell::RefCell;
use thread_local::ThreadLocal;

/// The state that one [`PerfettoLayer`](crate::PerfettoLayer) keeps of every thread that records
/// through it.
///
/// The layer owns the states, so they are freed with the layer and need no key that tells layers
/// apart.
#[derive(Default)]
pub(crate) struct ThreadStates {
    threads: ThreadLocal<RefCell<ThreadState>>,
}

/// What a layer keeps of one thread.
pub(crate) struct ThreadState {
    /// The track of the thread that owns this state.
    ///
    /// [`ThreadLocal`] hands the slot of an exited thread to a later thread, which then starts
    /// from a fresh state instead of inheriting the exited thread's.
    owner_track_uuid: u64,
    /// Whether the layer's trace already describes the thread's track.
    pub(crate) track_described: bool,
    /// The spans the layer has entered on the thread.
    pub(crate) entered: EnteredStack,
}

impl ThreadState {
    fn new(owner_track_uuid: u64) -> Self {
        Self {
            owner_track_uuid,
            track_described: false,
            entered: EnteredStack::default(),
        }
    }
}

impl ThreadStates {
    /// Runs `update` on the calling thread's state, and returns its result.
    ///
    /// Returns `None` during thread teardown, once the thread's thread-local values are being
    /// destroyed, so that the layer then keeps no state for the thread: it writes no entered
    /// slice, which no exit could end, and describes the thread's track with every use.
    pub(crate) fn with_current<R>(&self, update: impl FnOnce(&mut ThreadState) -> R) -> Option<R> {
        let track_uuid = thread_track::live_thread_uuid()?;
        let state = self
            .threads
            .get_or(|| RefCell::new(ThreadState::new(track_uuid)));
        let mut state = state.borrow_mut();
        if state.owner_track_uuid != track_uuid {
            *state = ThreadState::new(track_uuid);
        }
        Some(update(&mut state))
    }
}
