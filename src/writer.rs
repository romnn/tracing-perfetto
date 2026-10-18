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
/// [`write_all`](std::io::Write::write_all) call on a fresh writer.
pub trait PerfettoWriter {
    /// Writes one encoded perfetto `Trace` message.
    ///
    /// The layer ignores the result, so a failed write loses that message and later writes
    /// continue.
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
