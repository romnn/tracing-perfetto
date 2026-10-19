//! Ending a layer's trace, whether the layer is dropped or finished through a [`FinishHandle`].

use crate::process_memory::sampler::ProcessMemorySampler;
use crate::sink::{CloseSink, WriteGate};
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

/// The end of a layer's trace: the sampler that must stop, and the sink that must close.
///
/// The layer and its [`FinishHandle`]s share this state.
pub(crate) struct TraceLifecycle {
    /// The sampler, which stays here after it is stopped so that its thread still recognizes
    /// itself while a finish joins it.
    process_memory_sampler: parking_lot::Mutex<Option<ProcessMemorySampler>>,
    /// Held by a finish while it joins the sampler, so that a concurrent finish returns only
    /// after the sampler has stopped.
    ///
    /// The sampler thread never takes it, and the join happens outside the
    /// `process_memory_sampler` lock, which the sampler thread takes when it finishes the trace.
    sampler_join: parking_lot::Mutex<()>,
    /// The layer's sink, which a finish closes and flushes.
    ///
    /// It is weak so that a [`FinishHandle`] does not keep the writer open after the layer is
    /// gone.
    /// The layer closes the sink when it is dropped, so a sink that is gone is closed already.
    sink: Weak<dyn CloseSink>,
    /// The gate of the layer's sink, which outlives the sink to hold the first error for
    /// [`FinishHandle`].
    write_gate: Arc<WriteGate>,
    /// The finishes that found `sampler_join` held by another finish, so tests release the
    /// sampler only once a finish waits for that join.
    #[cfg(test)]
    sampler_join_contentions: AtomicUsize,
}

impl TraceLifecycle {
    /// Returns the lifecycle of a trace that is not finished, written through `sink`, whose gate
    /// is `write_gate`.
    pub(crate) fn new(sink: Weak<dyn CloseSink>, write_gate: Arc<WriteGate>) -> Self {
        Self {
            process_memory_sampler: parking_lot::Mutex::default(),
            sampler_join: parking_lot::Mutex::default(),
            sink,
            write_gate,
            #[cfg(test)]
            sampler_join_contentions: AtomicUsize::new(0),
        }
    }

    /// Returns a handle that finishes this trace.
    pub(crate) fn finish_handle(self: &Arc<Self>) -> FinishHandle {
        FinishHandle {
            lifecycle: Arc::clone(self),
        }
    }

    /// Ends the trace: stops the process-memory sampler, if one runs, and waits for it, then
    /// waits for the writes in progress, stops all later writes and flushes the writer.
    ///
    /// Spans and events are written until the sink closes, which happens after the sampler
    /// stopped.
    /// On the sampler thread, which cannot wait for itself, this does not wait for the sampler,
    /// which writes no further sample.
    /// Inside a call to the writer, on the thread that the layer calls it on, this neither waits
    /// for the sampler, which may wait for that call, nor for the sink, whose lock that call
    /// holds: it requests both to stop, and the sink closes and flushes when that call returns.
    ///
    /// Returns whether the trace has ended, which it has unless this was called inside a call to
    /// the writer.
    pub(crate) fn finish(&self) -> bool {
        let inside_writer = self.write_gate.request_close_from_writer();
        self.stop_sampler(inside_writer);
        if inside_writer {
            return false;
        }
        if let Some(sink) = self.sink.upgrade() {
            sink.close();
        }
        true
    }

    /// Stops the sampler, and waits for it unless the calling thread is the sampler thread or
    /// `inside_writer` says that it is inside a call to the writer.
    fn stop_sampler(&self, inside_writer: bool) {
        // Sampler thread, or inside the writer
        {
            let mut sampler = self.process_memory_sampler.lock();
            let Some(sampler) = sampler.as_mut() else {
                return;
            };
            if inside_writer || sampler.runs_on_current_thread() {
                sampler.request_stop();
                return;
            }
        }

        // Any other thread
        #[cfg(test)]
        let _sampler_join = crate::sink::lock_counting_contention(
            &self.sampler_join,
            &self.sampler_join_contentions,
        );
        #[cfg(not(test))]
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
        self.write_gate.is_closed()
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
/// A layer inside a process-global dispatcher is never dropped, so its owner finishes the trace,
/// which flushes the writer, through this handle instead.
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
    /// Ends the trace: stops the process-memory sampler, waits for the writes under way, flushes
    /// the writer, and reports a failed write or flush.
    ///
    /// When this returns, the sampler has stopped, each write of the layer has either reached the
    /// writer or been discarded, and the writer is flushed, so the output holds the whole trace.
    /// Nothing is written afterwards: spans and events recorded while this runs may land, and
    /// those recorded later are discarded.
    /// Concurrent finishes each return only once all of that holds.
    /// Finishing again has no further effect.
    ///
    /// Called from the sampler thread, by a [`ProcessMemorySource`], it ends the trace without
    /// waiting for the sampler, which then writes no further sample.
    /// Called from inside the writer, on the thread that the layer calls the writer on, it
    /// returns at once, because it cannot wait for the write that called it: it asks the sampler
    /// to stop, and the trace ends and the writer is flushed when that write returns.
    /// A later finish waits for the sampler and reports an error of that write or flush.
    /// Any other thread waits for the write in progress, so a finish on a thread that the writer
    /// waits for, such as one it spawns and joins inside a write, deadlocks.
    ///
    /// The caller must not hold anything that the writer or the process-memory source needs,
    /// such as the `Mutex` of a `Mutex<W>` writer, because this waits for the write in progress
    /// and for the sample being read, and then flushes the writer on the calling thread.
    ///
    /// Dropping the layer ends the trace and flushes the writer too, but only this reports an
    /// error.
    /// A global dispatcher never drops its layer, so finish a trace recorded there through this
    /// handle.
    ///
    /// [`ProcessMemorySource`]: crate::process_memory::ProcessMemorySource
    ///
    /// # Errors
    ///
    /// Returns the first error of the writer, after which the layer wrote nothing more, so the
    /// output is the trace up to the failed write, followed by whatever part of that write the
    /// writer took.
    /// A failed flush is reported the same way, unless a write failed before.
    /// Only the first call after the failure returns the error, and later calls return `Ok`.
    /// A call from inside the writer returns `Ok` and leaves the error to a later call.
    pub fn finish(&self) -> std::io::Result<()> {
        // Inside the writer, the error is left to a later finish, since taking it would wait for
        // the write that called this, and that write may still fail its flush.
        if !self.lifecycle.finish() {
            return Ok(());
        }
        self.lifecycle.write_gate.take_error().map_or(Ok(()), Err)
    }

    /// Returns whether a finish has taken the sampler thread to join it.
    ///
    /// Returns `false` while another thread holds the sampler's lock, so that a test polling
    /// this never waits for a finish that holds the lock.
    #[cfg(test)]
    pub(crate) fn sampler_join_began(&self) -> bool {
        self.lifecycle
            .process_memory_sampler
            .try_lock()
            .is_some_and(|sampler| {
                sampler
                    .as_ref()
                    .is_some_and(ProcessMemorySampler::is_handed_out)
            })
    }

    /// Returns how many finishes had to wait for another finish's join of the sampler.
    #[cfg(test)]
    pub(crate) fn sampler_join_contentions(&self) -> usize {
        self.lifecycle
            .sampler_join_contentions
            .load(Ordering::Relaxed)
    }

    /// Returns how many finishes had to wait for the write in progress to close the sink.
    #[cfg(test)]
    pub(crate) fn sink_close_contentions(&self) -> usize {
        self.lifecycle.write_gate.close_contentions()
    }
}
