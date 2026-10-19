//! What a layer keeps of each thread that records through it.

use crate::span_state::LayerId;
use crate::thread_slices::EnteredStack;
use std::cell::RefCell;
use std::sync::{Arc, Weak};

thread_local! {
    /// The state that each layer keeps of the calling thread, oldest layer first.
    ///
    /// The entries of dropped layers stay until a layer next adds an entry on the thread, which
    /// removes them.
    static THREADS: RefCell<Vec<LayerThreadState>> = const { RefCell::new(Vec::new()) };
}

/// The state that one layer keeps of the calling thread.
struct LayerThreadState {
    layer_id: LayerId,
    /// Dangles once the layer is dropped.
    layer: Weak<()>,
    state: ThreadState,
}

/// The state that one [`PerfettoLayer`](crate::PerfettoLayer) keeps of every thread that records
/// through it.
pub(crate) struct ThreadStates {
    layer_id: LayerId,
    /// Lives as long as the layer, which tells the threads' entries whether it is dropped.
    liveness: Arc<()>,
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
        Self {
            layer_id,
            liveness: Arc::new(()),
        }
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
                // The layers recording most recently are usually the newest.
                if let Some(entry) = threads
                    .iter_mut()
                    .rev()
                    .find(|entry| entry.layer_id == self.layer_id)
                {
                    return Some(update(&mut entry.state));
                }
                threads.retain(|entry| entry.layer.strong_count() > 0);
                threads.push(LayerThreadState {
                    layer_id: self.layer_id,
                    layer: Arc::downgrade(&self.liveness),
                    state: ThreadState::default(),
                });
                threads.last_mut().map(|entry| update(&mut entry.state))
            })
            .ok()
            .flatten()
    }
}

/// Returns the number of layers that keep state of the calling thread.
#[cfg(test)]
pub(crate) fn current_thread_layer_count() -> usize {
    THREADS.with(|threads| threads.borrow().len())
}
