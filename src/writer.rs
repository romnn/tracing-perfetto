//! The sink that a [`PerfettoLayer`](crate::PerfettoLayer) writes its encoded trace to.

use bytes::BytesMut;
use std::io::Write;
use tracing_subscriber::fmt::MakeWriter;

/// A sink for the encoded `Trace` messages of a [`PerfettoLayer`](crate::PerfettoLayer).
///
/// Each call hands over one complete `Trace` message, and messages written back to back decode as
/// one trace, so a sink that appends every buffer in call order produces a valid trace file.
/// The layer writes from every thread that records spans or events, and from its process-memory
/// sampler thread, so a sink shared across threads must apply each buffer as a whole.
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
/// `lossy(false)`, and its `WorkerGuard` must outlive the trace, because dropping the guard
/// flushes the queue.
///
/// A sink must not record spans or events, because it runs inside the layer's callbacks, which
/// a span or event it records would reenter.
///
/// [#5]: https://github.com/romnn/tracing-perfetto/issues/5
pub trait PerfettoWriter {
    /// Writes one encoded perfetto `Trace` message.
    ///
    /// The layer ignores the result, and a failed or partial write can leave the rest of the
    /// trace unreadable ([#4]).
    ///
    /// [#4]: https://github.com/romnn/tracing-perfetto/issues/4
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying writer fails.
    fn write_log(&self, buf: BytesMut) -> std::io::Result<()>;
}

impl<W: for<'writer> MakeWriter<'writer> + 'static> PerfettoWriter for W {
    fn write_log(&self, buf: BytesMut) -> std::io::Result<()> {
        self.make_writer().write_all(&buf)
    }
}
