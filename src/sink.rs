//! The sink that owns a layer's output stream: the writer, the packet sequence it carries, and
//! the gate that keeps the stream readable when a write fails and ends it when the trace ends.

use crate::idl;
use crate::sequence::TraceSequence;
use crate::writer::PerfettoWriter;
use bytes::BytesMut;
use prost::Message;
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, Ordering};

/// The message of the error that a writer's panic leaves in the gate.
pub(crate) const WRITER_PANICKED: &str = "the trace's writer panicked";

/// The only path from a layer and its process-memory sampler to the writer.
///
/// The sequence's preamble precedes every other message, because every packet that reads the
/// sequence clock must follow the clock snapshot that defines it.
/// After the first failed write the sink writes nothing more, so the output is the trace up to
/// that write, possibly followed by the part of the failed message that the writer took.
/// A writer that panics fails the same way, and is not called again, not even to flush it.
/// Once [closed](CloseSink::close), the sink writes nothing more either.
pub(crate) struct TraceSink<W> {
    writer: W,
    sequence: TraceSequence,
    gate: Arc<WriteGate>,
}

/// Whether a sink's writes reach its writer, shared with the [`FinishHandle`]s that end the
/// trace and report the first error.
///
/// Every write, and the flush that closes the gate, happens under its state lock, so they reach
/// the writer one at a time and in the order in which they took the lock.
///
/// [`FinishHandle`]: crate::FinishHandle
pub(crate) struct WriteGate {
    state: parking_lot::Mutex<GateState>,
    /// Whether the gate is closed, which only the holder of the state lock changes.
    ///
    /// Once set, no write reaches the writer, and the thread that set it flushes the writer
    /// before it releases the lock, unless it set it because the writer panicked.
    closed: AtomicBool,
    /// The call to the writer that a thread makes while it holds the state lock, while one does.
    ///
    /// It lets a finish from inside the writer see that it cannot take the lock that its own
    /// write holds, and leave closing the gate to that write.
    /// Its lock is never held across anything else.
    writer_call: parking_lot::Mutex<Option<WriterCall>>,
    /// The closes that found the state lock held, so tests release a held write only once a
    /// finish waits for it.
    #[cfg(test)]
    close_contentions: AtomicUsize,
}

/// A call to the writer, made by a thread that holds the state lock.
struct WriterCall {
    /// The [`thread_id::get`] of the calling thread.
    ///
    /// [`thread_id::get`] reads no thread-local value, so it works while a thread's thread-local
    /// values are destroyed, when the layer still writes, and [`std::thread::current`] panics.
    thread: usize,
    /// Whether a finish from inside the call asked to close the gate once the call returns.
    close_requested: bool,
}

enum GateState {
    /// Nothing has been written, so the next write begins with the preamble.
    Pending,
    /// The preamble is written, and every write so far succeeded.
    Open,
    /// A write or the flush failed, so no more writes reach the writer.
    ///
    /// It holds the error until a [`FinishHandle`] reports it, which is [`WRITER_PANICKED`] for
    /// a writer that panicked.
    /// A gate fails only once, so a later failure of the flush is neither kept nor reported,
    /// also after the first error was reported.
    ///
    /// [`FinishHandle`]: crate::FinishHandle
    Failed(Option<std::io::Error>),
}

impl WriteGate {
    /// Returns the error of the write or flush that failed first, unless it was taken already.
    ///
    /// Waits for the write in progress, so the calling thread must not be inside a call to the
    /// writer.
    pub(crate) fn take_error(&self) -> Option<std::io::Error> {
        match &mut *self.state.lock() {
            GateState::Failed(error) => error.take(),
            GateState::Pending | GateState::Open => None,
        }
    }

    /// Returns whether the gate is closed, after which no write reaches the writer.
    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Asks the call to the writer that the calling thread is inside, if it is inside one, to
    /// close the gate once that call returns.
    ///
    /// Returns whether the calling thread is inside a call to the writer, holding the state lock.
    /// A finish there cannot take the lock, nor wait for anything that waits for it, such as
    /// the sampler's next write.
    pub(crate) fn request_close_from_writer(&self) -> bool {
        let mut writer_call = self.writer_call.lock();
        match writer_call.as_mut() {
            Some(call) if call.thread == thread_id::get() => {
                call.close_requested = true;
                true
            }
            _ => false,
        }
    }

    /// Returns how many closes had to wait for the state lock.
    #[cfg(test)]
    pub(crate) fn close_contentions(&self) -> usize {
        self.close_contentions.load(Ordering::Relaxed)
    }
}

/// Locks `mutex`, and counts in `contentions` a lock that had to wait for another thread.
///
/// A test that observes the count knows that a thread waits for the lock, which it holds until
/// it releases what the test blocked, so a thread that never waits leaves the count unchanged.
#[cfg(test)]
pub(crate) fn lock_counting_contention<'mutex, T>(
    mutex: &'mutex parking_lot::Mutex<T>,
    contentions: &AtomicUsize,
) -> parking_lot::MutexGuard<'mutex, T> {
    mutex.try_lock().unwrap_or_else(|| {
        contentions.fetch_add(1, Ordering::Relaxed);
        mutex.lock()
    })
}

/// Holds the state lock for calls to the writer, and marks the calling thread as the one
/// calling the writer, until it is dropped.
///
/// Dropping it clears the mark before it releases the lock.
/// Dropped without [`Self::returned`], because the writer panicked and the panic unwinds
/// through the sink, it fails the gate with [`WRITER_PANICKED`] and closes it, without
/// flushing, so the writer is never called again.
/// A writer that panicked may be broken for good: tracing-subscriber's writer for a
/// `std::sync::Mutex` panics on every call once the panic poisoned the mutex.
/// The panic itself keeps unwinding to the code that recorded the span or event.
struct CallingWriter<'gate> {
    gate: &'gate WriteGate,
    state: parking_lot::MutexGuard<'gate, GateState>,
    /// Whether the calls to the writer returned instead of unwinding.
    ///
    /// It is set on return instead of read from [`std::thread::panicking`], which is also `true`
    /// for a write that returns while another panic unwinds, such as the end of a span whose
    /// guard that unwinding drops.
    returned: bool,
}

impl<'gate> CallingWriter<'gate> {
    /// Marks the calling thread, which holds `state`, as the one calling the writer.
    fn enter(gate: &'gate WriteGate, state: parking_lot::MutexGuard<'gate, GateState>) -> Self {
        *gate.writer_call.lock() = Some(WriterCall {
            thread: thread_id::get(),
            close_requested: false,
        });
        Self {
            gate,
            state,
            returned: false,
        }
    }

    /// Ends the calls to the writer, which returned, and releases the state lock.
    fn returned(mut self) {
        self.returned = true;
    }

    /// Returns whether a finish from inside this call asked to close the gate.
    fn close_requested(&self) -> bool {
        self.gate
            .writer_call
            .lock()
            .as_ref()
            .is_some_and(|call| call.close_requested)
    }
}

impl Drop for CallingWriter<'_> {
    fn drop(&mut self) {
        *self.gate.writer_call.lock() = None;
        if self.returned {
            return;
        }

        // The writer panicked
        self.gate.closed.store(true, Ordering::Release);
        if !matches!(*self.state, GateState::Failed(_)) {
            *self.state = GateState::Failed(Some(std::io::Error::other(WRITER_PANICKED)));
        }
    }
}

impl<W> TraceSink<W> {
    /// Returns a sink that writes to `writer` on a new sequence.
    pub(crate) fn new(writer: W) -> Self {
        Self {
            writer,
            sequence: TraceSequence::new(),
            gate: Arc::new(WriteGate {
                state: parking_lot::Mutex::new(GateState::Pending),
                closed: AtomicBool::new(false),
                writer_call: parking_lot::Mutex::default(),
                #[cfg(test)]
                close_contentions: AtomicUsize::new(0),
            }),
        }
    }

    /// Returns the sequence that every packet written through this sink belongs to.
    pub(crate) fn sequence(&self) -> &TraceSequence {
        &self.sequence
    }

    /// Returns the gate that decides whether writes reach the writer and holds the first error.
    pub(crate) fn gate(&self) -> &Arc<WriteGate> {
        &self.gate
    }
}

impl<W: PerfettoWriter> TraceSink<W> {
    /// Writes `trace` as one message, preceded by the sequence's preamble on the first write.
    ///
    /// Returns whether `trace` reached the writer, so that state which a later packet relies on,
    /// such as a described track, advances only once its message is written.
    /// A failed write, of `trace` or of the preamble, fails the gate for good: this and every
    /// later write return `false`, and the gate keeps the error.
    /// A closed gate writes nothing and returns `false`.
    /// A finish from inside the writer closes the gate when this write returns.
    /// A writer that panics fails and closes the gate, and the panic propagates to the caller.
    pub(crate) fn write(&self, trace: &idl::Trace) -> bool {
        let Some(message) = encode(trace) else {
            return false;
        };

        let mut calling_writer = CallingWriter::enter(&self.gate, self.gate.state.lock());
        let written = self.write_locked(&mut calling_writer.state, message);
        // A finish from inside the writer could not take the lock that this write holds.
        if calling_writer.close_requested() {
            self.close_locked(&mut calling_writer.state);
        }
        calling_writer.returned();
        written
    }

    /// Writes `message`, preceded by the preamble if nothing was written yet, unless the gate
    /// is closed or failed.
    fn write_locked(&self, state: &mut GateState, message: BytesMut) -> bool {
        if self.gate.is_closed() {
            return false;
        }
        match *state {
            GateState::Failed(_) => return false,
            GateState::Open => {}
            GateState::Pending => {
                let Some(preamble) = encode(&self.sequence.preamble_trace()) else {
                    return false;
                };
                if !self.write_message(state, preamble) {
                    return false;
                }
                *state = GateState::Open;
            }
        }
        self.write_message(state, message)
    }

    /// Hands `message` to the writer, and fails the gate if the write fails.
    fn write_message(&self, state: &mut GateState, message: BytesMut) -> bool {
        match self.writer.write_log(message) {
            Ok(()) => true,
            Err(err) => {
                // Only the first failure gets here, since the gate stays failed after it.
                *state = GateState::Failed(Some(err));
                false
            }
        }
    }

    /// Closes the gate and flushes the writer, unless the gate is closed already.
    ///
    /// The caller holds the state lock and has marked itself as [`CallingWriter`], so that a
    /// finish from inside the flush does not wait for that lock.
    /// The writer is flushed after a failed write too, which lets a buffering writer pass on the
    /// whole messages it took before the failure.
    /// A failed flush fails the gate, unless a write failed before.
    fn close_locked(&self, state: &mut GateState) {
        if self.gate.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        if let Err(err) = self.writer.flush()
            && !matches!(state, GateState::Failed(_))
        {
            *state = GateState::Failed(Some(err));
        }
    }
}

/// Ends a sink's stream, for the [`FinishHandle`](crate::FinishHandle)s, which do not know the
/// type of its writer.
pub(crate) trait CloseSink: Send + Sync {
    /// Closes the gate, so that no later write reaches the writer, and flushes the writer once.
    ///
    /// Waits for the write in progress, which holds the state lock, so every write that took the
    /// lock before has reached the writer when this returns.
    /// A flush error is kept like a write error, unless a write failed before.
    /// A flush that panics fails the gate too, and the panic propagates to the caller.
    /// Closing again has no further effect.
    /// The calling thread must not be inside a call to the writer, since this waits for the lock
    /// that such a call holds.
    fn close(&self);
}

impl<W: PerfettoWriter + Send + Sync> CloseSink for TraceSink<W> {
    fn close(&self) {
        #[cfg(test)]
        let state = lock_counting_contention(&self.gate.state, &self.gate.close_contentions);
        #[cfg(not(test))]
        let state = self.gate.state.lock();
        let mut calling_writer = CallingWriter::enter(&self.gate, state);
        self.close_locked(&mut calling_writer.state);
        calling_writer.returned();
    }
}

/// Encodes `trace` into one buffer.
///
/// Returns `None` if encoding fails, which a growable buffer never makes it do.
fn encode(trace: &idl::Trace) -> Option<BytesMut> {
    let mut buf = BytesMut::with_capacity(trace.encoded_len());
    trace.encode(&mut buf).ok()?;
    Some(buf)
}
