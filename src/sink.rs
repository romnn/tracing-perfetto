//! The sink that owns a layer's output stream: the writer, the packet sequence it carries, and
//! the gate that keeps the stream readable when a write fails and ends it when the trace ends.

use crate::idl;
use crate::sequence::TraceSequence;
use crate::writer::PerfettoWriter;
use bytes::BytesMut;
use prost::Message;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// The only path from a layer and its process-memory sampler to the writer.
///
/// The sequence's preamble precedes every other message, because every packet that reads the
/// sequence clock must follow the clock snapshot that defines it.
/// After the first failed write the sink writes nothing more, so the output is the trace up to
/// that write, possibly followed by the part of the failed message that the writer took.
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
    /// before it releases the lock.
    closed: AtomicBool,
    /// The [`thread_id::get`] of the thread that holds the state lock to call the writer, while
    /// one does.
    ///
    /// It lets a finish from inside the writer see that it cannot take the lock that its own
    /// write holds.
    /// [`thread_id::get`] reads no thread-local value, so it works while a thread's thread-local
    /// values are destroyed, when the layer still writes, and [`std::thread::current`] panics.
    /// Its lock is never held across anything else.
    writer_thread: parking_lot::Mutex<Option<usize>>,
    /// Set by a finish from inside the writer, which leaves closing the gate to the write that
    /// called it.
    close_requested: AtomicBool,
    /// The first error of a write or of the flush, until a [`FinishHandle`] reports it.
    ///
    /// It has a lock of its own, which is never held across a call to the writer, so a writer
    /// that finishes the trace from inside its own write does not wait for the state lock that
    /// write holds.
    ///
    /// [`FinishHandle`]: crate::FinishHandle
    error: parking_lot::Mutex<Option<std::io::Error>>,
}

enum GateState {
    /// Nothing has been written, so the next write begins with the preamble.
    Pending,
    /// The preamble is written, and every write so far succeeded.
    Open,
    /// A write failed, so no more writes reach the writer.
    Failed,
}

impl WriteGate {
    /// Returns the error of the write or flush that failed first, unless it was taken already.
    ///
    /// The error of a write still in progress is not reported yet.
    pub(crate) fn take_error(&self) -> Option<std::io::Error> {
        self.error.lock().take()
    }

    /// Returns whether the gate is closed, after which no write reaches the writer.
    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Returns whether the calling thread is inside a call to the writer, holding the state lock.
    ///
    /// A finish there cannot take the lock, nor wait for anything that waits for it, such as
    /// the sampler's next write.
    pub(crate) fn is_writing_on_current_thread(&self) -> bool {
        *self.writer_thread.lock() == Some(thread_id::get())
    }

    /// Closes the gate once the write that the calling thread is inside returns.
    ///
    /// Only the thread inside the write calls this, so that write sees the request before it
    /// releases the lock.
    pub(crate) fn request_close(&self) {
        self.close_requested.store(true, Ordering::Relaxed);
    }

    /// Keeps `err` unless an earlier failure is still unreported.
    fn latch_error(&self, err: std::io::Error) {
        let mut error = self.error.lock();
        if error.is_none() {
            *error = Some(err);
        }
    }
}

/// Marks the calling thread as the one calling the writer, until it is dropped.
///
/// It must be created while the state lock is held and dropped before the lock is released.
struct CallingWriter<'gate> {
    gate: &'gate WriteGate,
}

impl<'gate> CallingWriter<'gate> {
    fn enter(gate: &'gate WriteGate) -> Self {
        *gate.writer_thread.lock() = Some(thread_id::get());
        Self { gate }
    }
}

impl Drop for CallingWriter<'_> {
    fn drop(&mut self) {
        *self.gate.writer_thread.lock() = None;
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
                writer_thread: parking_lot::Mutex::default(),
                close_requested: AtomicBool::new(false),
                error: parking_lot::Mutex::default(),
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
    pub(crate) fn write(&self, trace: &idl::Trace) -> bool {
        let Some(message) = encode(trace) else {
            return false;
        };

        let mut state = self.gate.state.lock();
        let calling_writer = CallingWriter::enter(&self.gate);
        let written = self.write_locked(&mut state, message);
        // A finish from inside the writer could not take the lock that this write holds.
        if self.gate.close_requested.load(Ordering::Relaxed) {
            self.close_locked();
        }
        drop(calling_writer);
        written
    }

    /// Writes `message`, preceded by the preamble if nothing was written yet, unless the gate
    /// is closed or failed.
    fn write_locked(&self, state: &mut GateState, message: BytesMut) -> bool {
        if self.gate.is_closed() {
            return false;
        }
        match *state {
            GateState::Failed => return false,
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
                *state = GateState::Failed;
                self.gate.latch_error(err);
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
    fn close_locked(&self) {
        if self.gate.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        if let Err(err) = self.writer.flush() {
            self.gate.latch_error(err);
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
    /// A flush error is kept like a write error, unless an earlier error is still unreported.
    /// Closing again has no further effect.
    /// The calling thread must not be inside a call to the writer, since this waits for the lock
    /// that such a call holds.
    fn close(&self);
}

impl<W: PerfettoWriter + Send + Sync> CloseSink for TraceSink<W> {
    fn close(&self) {
        let _state = self.gate.state.lock();
        let _calling_writer = CallingWriter::enter(&self.gate);
        self.close_locked();
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
