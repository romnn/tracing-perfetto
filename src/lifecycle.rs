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
    /// The sampler, which stays here after it is stopped so that its thread still recognizes
    /// itself while a finish joins it.
    process_memory_sampler: parking_lot::Mutex<Option<ProcessMemorySampler>>,
    /// Held by a finish while it joins the sampler, so that a concurrent finish returns only
    /// after the sampler has stopped.
    ///
    /// The sampler thread never takes it, and the join happens outside the
    /// `process_memory_sampler` lock, which the sampler thread takes when it finishes the trace.
    sampler_join: parking_lot::Mutex<()>,
}

impl TraceLifecycle {
    /// Returns a handle that finishes this trace.
    pub(crate) fn finish_handle(self: &Arc<Self>) -> FinishHandle {
        FinishHandle {
            lifecycle: Arc::clone(self),
        }
    }

    /// Ends the trace and waits for the process-memory sampler, if one runs, to stop.
    ///
    /// Spans and events stop being written before the wait, which lasts only for a sample in
    /// progress.
    /// On the sampler thread, which cannot wait for itself, this returns at once, and the sampler
    /// writes no further sample.
    pub(crate) fn finish(&self) {
        self.finished.store(true, Ordering::Release);

        // Sampler thread
        {
            let mut sampler = self.process_memory_sampler.lock();
            let Some(sampler) = sampler.as_mut() else {
                return;
            };
            if sampler.runs_on_current_thread() {
                sampler.request_stop();
                return;
            }
        }

        // Any other thread
        let _sampler_join = self.sampler_join.lock();
        let stopping = self
            .process_memory_sampler
            .lock()
            .as_mut()
            .and_then(ProcessMemorySampler::stop);
        if let Some(stopping) = stopping {
            stopping.join();
        }
    }

    /// Returns whether the trace has ended, after which the layer writes nothing.
    pub(crate) fn is_finished(&self) -> bool {
        self.finished.load(Ordering::Acquire)
    }

    /// Makes `sampler` the trace's sampler, and stops and joins the one it replaces.
    pub(crate) fn replace_sampler(&self, sampler: ProcessMemorySampler) {
        let replaced = self.process_memory_sampler.lock().replace(sampler);
        if let Some(stopping) = replaced.and_then(|mut replaced| replaced.stop()) {
            stopping.join();
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
    /// Spans and events recorded afterwards are discarded.
    /// Concurrent finishes each return only after the sampler has stopped.
    /// Called from the sampler thread, by a [`ProcessMemorySource`] or the writer, it returns
    /// without waiting for the sampler, which then writes no further sample.
    /// A write already under way can still land after this returns, and the writer is not
    /// flushed ([#5]).
    /// Finishing again has no further effect.
    ///
    /// [#5]: https://github.com/romnn/tracing-perfetto/issues/5
    /// [`ProcessMemorySource`]: crate::process_memory::ProcessMemorySource
    pub fn finish(&self) {
        self.lifecycle.finish();
    }

    /// Returns whether a finish has taken the sampler thread to join it.
    #[cfg(test)]
    pub(crate) fn sampler_join_began(&self) -> bool {
        self.lifecycle
            .process_memory_sampler
            .lock()
            .as_ref()
            .is_some_and(ProcessMemorySampler::is_handed_out)
    }
}
