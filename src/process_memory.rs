//! Process memory sources for the RSS counter track of a [`PerfettoLayer`](crate::PerfettoLayer).

/// A best-effort source of process memory statistics.
///
/// Implementations should return `None` if memory usage is unavailable.
pub trait ProcessMemorySource: Send + Sync + 'static {
    /// Returns the resident set size (RSS) in bytes.
    fn resident_set_size_bytes(&self) -> Option<u64>;
}

/// Returns the default [`ProcessMemorySource`] backed by the `memory-stats` crate.
///
/// This supports Linux, macOS, and Windows out of the box.
#[must_use]
pub fn default_process_memory_source() -> Option<Box<dyn ProcessMemorySource>> {
    // Probe once to check whether the platform is supported.
    memory_stats::memory_stats()?;
    Some(Box::new(MemoryStatsSource))
}

/// Default implementation using the [`memory_stats`](https://docs.rs/memory-stats) crate.
struct MemoryStatsSource;

impl ProcessMemorySource for MemoryStatsSource {
    fn resident_set_size_bytes(&self) -> Option<u64> {
        memory_stats::memory_stats().map(|s| s.physical_mem as u64)
    }
}
