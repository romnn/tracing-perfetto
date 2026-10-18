//! Ending a layer's trace, whether the layer is dropped or finished through a [`FinishHandle`].

use crate::process_memory::sampler::ProcessMemorySampler;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// Whether a layer's trace has ended, and the sampler that must stop when it does.
///
/// The layer and its [`FinishHandle`]s share this state.
#[derive(Default)]
pub(crate) struct TraceLifecycle {
    finished: AtomicBool,
    process_memory_sampler: parking_lot::Mutex<Option<ProcessMemorySampler>>,
}

impl TraceLifecycle {
    /// Returns a handle that finishes this trace.
    pub(crate) fn finish_handle(self: &Arc<Self>) -> FinishHandle {
        FinishHandle {
            lifecycle: Arc::clone(self),
        }
    }

    /// Ends the trace and joins the process-memory sampler, if one runs.
    ///
    /// Spans and events stop being written before the join, which waits for the sampler's
    /// current interval.
    pub(crate) fn finish(&self) {
        self.finished.store(true, Ordering::Release);
        if let Some(sampler) = self.process_memory_sampler.lock().take() {
            sampler.stop();
        }
    }

    /// Returns whether the trace has ended, after which the layer writes nothing.
    pub(crate) fn is_finished(&self) -> bool {
        self.finished.load(Ordering::Acquire)
    }

    /// Makes `sampler` the trace's sampler, and stops and joins the one it replaces.
    pub(crate) fn replace_sampler(&self, sampler: ProcessMemorySampler) {
        let replaced = self.process_memory_sampler.lock().replace(sampler);
        if let Some(replaced) = replaced {
            replaced.stop();
        }
    }
}

/// Ends a [`PerfettoLayer`](crate::PerfettoLayer)'s trace while something else still owns the
/// layer.
///
/// A layer inside a process-global dispatcher is never dropped, so its owner finishes the trace
/// through this handle instead.
/// Clones finish the same trace.
#[derive(Clone)]
pub struct FinishHandle {
    lifecycle: Arc<TraceLifecycle>,
}

impl std::fmt::Debug for FinishHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FinishHandle")
            .field("finished", &self.lifecycle.is_finished())
            .finish()
    }
}

impl FinishHandle {
    /// Stops the layer from writing and joins its process-memory sampler.
    ///
    /// Spans and events recorded afterwards are discarded, so the trace stays complete as written.
    /// Finishing again has no further effect.
    pub fn finish(&self) {
        self.lifecycle.finish();
    }
}
