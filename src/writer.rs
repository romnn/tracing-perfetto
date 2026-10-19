//! The sink that a [`PerfettoLayer`](crate::PerfettoLayer) writes its encoded trace to.

use bytes::BytesMut;
use std::io::Write;
use tracing_subscriber::fmt::MakeWriter;

/// A sink for the encoded `Trace` messages of a [`PerfettoLayer`](crate::PerfettoLayer).
///
/// Each call hands over one complete `Trace` message, and messages written back to back decode as
/// one trace, so a sink that appends every buffer in call order produces a valid trace file.
/// The layer writes from every thread that records spans or events, and from its process-memory
/// sampler thread, but hands over one message at a time.
///
/// After the first failed write the layer writes nothing more, so the output is the trace up to
/// that write, followed by whatever part of the failed message the sink took.
/// [`FinishHandle::finish`](crate::FinishHandle::finish) returns the error.
///
/// Every [`MakeWriter`] is a `PerfettoWriter` that writes each buffer with one
/// [`write_all`](std::io::Write::write_all) call on a fresh writer, such as a
/// `Mutex<File>`, and flushes through [`flush`](std::io::Write::flush) on a fresh writer.
///
/// The layer writes each message as it happens, so a trace cut short holds everything written
/// so far.
/// The end of the trace, through [`FinishHandle::finish`](crate::FinishHandle::finish) or by
/// dropping the layer, flushes the sink once, so a buffering sink such as a
/// `Mutex<BufWriter<File>>` passes on the whole trace.
/// A global dispatcher never drops its layer, so its owner finishes the trace instead.
/// `tracing_appender::non_blocking` drops messages when its queue is full unless it is built with
/// `lossy(false)`, and its flush does not wait for the queue.
/// Drop its `WorkerGuard` after [`FinishHandle::finish`](crate::FinishHandle::finish): dropping
/// the guard flushes the queue, but it waits about a second at most, so a large backlog can still
/// lose its tail.
///
/// A sink must not record spans or events: the layer calls it while holding the lock that every
/// write of the trace takes, so a span or event that the sink records deadlocks on that lock.
/// A sink may finish the trace from inside a write or flush, on any thread: that finish returns
/// at once, and the trace ends when the write returns.
/// A sink must not wait for a finish on another thread, which waits for the write in progress.
pub trait PerfettoWriter {
    /// Writes one encoded perfetto `Trace` message.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying writer fails, which ends the layer's trace.
    fn write_log(&self, buf: BytesMut) -> std::io::Result<()>;

    /// Passes on every message written so far to the destination, as [`Write::flush`] does.
    ///
    /// The layer calls it once, when the trace ends, and writes nothing afterwards.
    /// A sink that buffers messages must pass them on here, since nothing else may ever flush it:
    /// a layer in a global dispatcher is never dropped, so the end of its trace would stay in
    /// the buffer.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying writer fails, which
    /// [`FinishHandle::finish`](crate::FinishHandle::finish) reports unless a write failed
    /// before.
    fn flush(&self) -> std::io::Result<()>;
}

impl<W: for<'writer> MakeWriter<'writer> + 'static> PerfettoWriter for W {
    fn write_log(&self, buf: BytesMut) -> std::io::Result<()> {
        self.make_writer().write_all(&buf)
    }

    fn flush(&self) -> std::io::Result<()> {
        self.make_writer().flush()
    }
}
