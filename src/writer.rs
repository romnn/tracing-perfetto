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
/// `Mutex<File>`.
///
/// The layer writes each message as it happens and never flushes, so a trace cut short holds
/// everything written so far.
/// A buffering sink, such as a [`BufWriter`](std::io::BufWriter), holds the end of the trace
/// until something else flushes it ([#5]).
/// `tracing_appender::non_blocking` drops messages when its queue is full unless it is built with
/// `lossy(false)`.
/// Drop its `WorkerGuard` after [`FinishHandle::finish`](crate::FinishHandle::finish): dropping
/// the guard flushes the queue, but it waits about a second at most, so a large backlog can still
/// lose its tail.
///
/// A sink must not record spans or events: the layer calls it while holding the lock that every
/// write of the trace takes, so a span or event that the sink records deadlocks on that lock.
/// Reentering the layer from inside its own write was never safe.
/// A sink may finish the trace from the process-memory sampler thread, but a finish from any
/// other thread can deadlock with a sampler that waits to write.
///
/// [#5]: https://github.com/romnn/tracing-perfetto/issues/5
pub trait PerfettoWriter {
    /// Writes one encoded perfetto `Trace` message.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying writer fails, which ends the layer's trace.
    fn write_log(&self, buf: BytesMut) -> std::io::Result<()>;
}

impl<W: for<'writer> MakeWriter<'writer> + 'static> PerfettoWriter for W {
    fn write_log(&self, buf: BytesMut) -> std::io::Result<()> {
        self.make_writer().write_all(&buf)
    }
}
