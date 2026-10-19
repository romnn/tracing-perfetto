//! The sink that owns a layer's output stream: the writer, the packet sequence it carries, and
//! the gate that keeps the stream readable when a write fails.

use crate::idl;
use crate::sequence::TraceSequence;
use crate::writer::PerfettoWriter;
use bytes::BytesMut;
use prost::Message;
use std::sync::Arc;

/// The only path from a layer and its process-memory sampler to the writer.
///
/// The sequence's preamble precedes every other message, because every packet that reads the
/// sequence clock must follow the clock snapshot that defines it.
/// After the first failed write the sink writes nothing more, so the output is the trace up to
/// that write, possibly followed by the part of the failed message that the writer took.
pub(crate) struct TraceSink<W> {
    writer: W,
    sequence: TraceSequence,
    gate: Arc<WriteGate>,
}

/// Whether a sink's writes reach its writer, shared with the [`FinishHandle`]s that report the
/// first write error.
///
/// Every write happens under its lock, so writes reach the writer one at a time and in the order
/// in which they took the lock.
///
/// [`FinishHandle`]: crate::FinishHandle
pub(crate) struct WriteGate {
    state: parking_lot::Mutex<GateState>,
}

enum GateState {
    /// Nothing has been written, so the next write begins with the preamble.
    Pending,
    /// The preamble is written, and every write so far succeeded.
    Open,
    /// A write failed, so no more writes reach the writer.
    Failed {
        /// The error of the failed write, until a [`FinishHandle`](crate::FinishHandle)
        /// reports it.
        error: Option<std::io::Error>,
    },
}

impl WriteGate {
    /// Returns the error of the write that closed the gate, unless it was taken already.
    pub(crate) fn take_error(&self) -> Option<std::io::Error> {
        match &mut *self.state.lock() {
            GateState::Failed { error } => error.take(),
            GateState::Pending | GateState::Open => None,
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
            }),
        }
    }

    /// Returns the sequence that every packet written through this sink belongs to.
    pub(crate) fn sequence(&self) -> &TraceSequence {
        &self.sequence
    }

    /// Returns the gate that holds the first write error.
    pub(crate) fn gate(&self) -> &Arc<WriteGate> {
        &self.gate
    }
}

impl<W: PerfettoWriter> TraceSink<W> {
    /// Writes `trace` as one message, preceded by the sequence's preamble on the first write.
    ///
    /// Returns whether `trace` reached the writer, so that state which a later packet relies on,
    /// such as a described track, advances only once its message is written.
    /// A failed write, of `trace` or of the preamble, closes the gate for good: this and every
    /// later write return `false`, and the gate keeps the error.
    pub(crate) fn write(&self, trace: &idl::Trace) -> bool {
        let Some(message) = encode(trace) else {
            return false;
        };

        let mut state = self.gate.state.lock();
        match *state {
            GateState::Failed { .. } => return false,
            GateState::Open => {}
            GateState::Pending => {
                let Some(preamble) = encode(&self.sequence.preamble_trace()) else {
                    return false;
                };
                if !self.write_message(&mut state, preamble) {
                    return false;
                }
                *state = GateState::Open;
            }
        }
        self.write_message(&mut state, message)
    }

    /// Hands `message` to the writer, and closes the gate with the error if the write fails.
    fn write_message(&self, state: &mut GateState, message: BytesMut) -> bool {
        match self.writer.write_log(message) {
            Ok(()) => true,
            Err(err) => {
                *state = GateState::Failed { error: Some(err) };
                false
            }
        }
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
