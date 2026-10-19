//! Process memory sources for the RSS counter track of a [`PerfettoLayer`](crate::PerfettoLayer).
//!
//! [`PerfettoLayer::with_process_memory_rss`](crate::PerfettoLayer::with_process_memory_rss)
//! samples [`default_process_memory_source`], and
//! [`PerfettoLayer::with_process_memory_source`](crate::PerfettoLayer::with_process_memory_source)
//! samples any other [`ProcessMemorySource`].

pub(crate) mod sampler;

/// A best-effort source of process memory statistics.
///
/// The sampler calls it from its own thread once per interval.
pub trait ProcessMemorySource: Send + Sync + 'static {
    /// Returns the resident set size (RSS) in bytes.
    ///
    /// Returns `None` if memory usage is unavailable, in which case the interval has no sample.
    fn resident_set_size_bytes(&self) -> Option<u64>;
}

/// Returns the default [`ProcessMemorySource`] backed by the [`memory_stats`] crate.
///
/// It supports Linux, macOS and Windows.
/// Returns `None` on a platform where `memory_stats` reports no statistics.
#[must_use]
pub fn default_process_memory_source() -> Option<Box<dyn ProcessMemorySource>> {
    // Probe once to check whether the platform is supported.
    memory_stats::memory_stats()?;
    Some(Box::new(MemoryStatsSource))
}

/// Reads the physical memory that [`memory_stats`] reports as the resident set size.
struct MemoryStatsSource;

impl ProcessMemorySource for MemoryStatsSource {
    fn resident_set_size_bytes(&self) -> Option<u64> {
        memory_stats::memory_stats().map(|stats| stats.physical_mem as u64)
    }
}
