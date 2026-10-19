//! What a layer keeps of each thread that records through it.

use crate::span_state::LayerId;
use crate::thread_slices::EnteredStack;
use std::cell::RefCell;

thread_local! {
    /// The state that each layer keeps of the calling thread.
    ///
    /// An entry stays until the thread exits, even after its layer is dropped, so a thread
    /// keeps a small entry for each layer it has recorded through.
    static THREADS: RefCell<Vec<LayerThreadState>> = const { RefCell::new(Vec::new()) };
}

/// The state that one layer keeps of the calling thread.
struct LayerThreadState {
    layer_id: LayerId,
    state: ThreadState,
}

/// The state that one [`PerfettoLayer`](crate::PerfettoLayer) keeps of every thread that records
/// through it.
pub(crate) struct ThreadStates {
    layer_id: LayerId,
}

/// What a layer keeps of one thread.
#[derive(Default)]
pub(crate) struct ThreadState {
    /// Whether the layer's trace already describes the thread's track.
    pub(crate) track_described: bool,
    /// The spans the layer has entered on the thread.
    pub(crate) entered: EnteredStack,
}

impl ThreadStates {
    /// Returns the per-thread states of the layer `layer_id`.
    pub(crate) fn new(layer_id: LayerId) -> Self {
        Self { layer_id }
    }

    /// Runs `update` on the calling thread's state, and returns its result.
    ///
    /// Returns `None` during thread teardown, once the thread's states are destroyed, so that the
    /// layer then keeps no state for the thread: it writes no entered slice, which no exit could
    /// end, and describes the thread's track with every use.
    /// A destroyed thread-local value reports its absence instead of panicking, so this is safe
    /// to call from any thread-local destructor.
    pub(crate) fn with_current<R>(&self, update: impl FnOnce(&mut ThreadState) -> R) -> Option<R> {
        THREADS
            .try_with(|threads| {
                let mut threads = threads.borrow_mut();
                if let Some(entry) = threads
                    .iter_mut()
                    .find(|entry| entry.layer_id == self.layer_id)
                {
                    return Some(update(&mut entry.state));
                }
                threads.push(LayerThreadState {
                    layer_id: self.layer_id,
                    state: ThreadState::default(),
                });
                threads.last_mut().map(|entry| update(&mut entry.state))
            })
            .ok()
            .flatten()
    }
}
