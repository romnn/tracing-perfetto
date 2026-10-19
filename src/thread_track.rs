//! The calling thread's track: its UUID, and its descriptor with the kernel's thread ID if known.

use crate::{idl, packets};

thread_local! {
    /// The UUID of the calling thread's track, shared by every layer in the process.
    static THREAD_TRACK_UUID: u64 = packets::unique_uuid();
    /// The kernel's ID for the calling thread as a trace thread ID, read once per thread.
    static TRACE_THREAD_ID: Option<i32> = trace_thread_id(kernel_thread_id());
}

#[cfg(test)]
thread_local! {
    /// The source of the kernel thread ID that the calling thread reports.
    static KERNEL_THREAD_ID_SOURCE: std::cell::Cell<KernelThreadIdSource> =
        const { std::cell::Cell::new(KernelThreadIdSource::Platform) };
}

/// The source of a thread's kernel thread ID in tests.
#[cfg(test)]
#[derive(Clone, Copy)]
enum KernelThreadIdSource {
    /// The platform's ID for the thread.
    Platform,
    /// A given ID, or `None` for an unknown one, in place of the platform's.
    Override(Option<u64>),
}

/// Returns the UUID of the calling thread's track.
pub(crate) fn current_thread_uuid() -> u64 {
    THREAD_TRACK_UUID.with(|uuid| *uuid)
}

/// Returns the descriptor of the calling thread's track, named after the thread.
///
/// Where the kernel's thread ID is known, the track is the thread track of that ID.
/// Otherwise it is a track below the process track `process_track_uuid`: a thread descriptor
/// identifies its thread by the process and thread IDs alone, so a made-up ID would claim
/// another thread, and trace processor would merge the tracks of every thread sharing it.
pub(crate) fn current_thread_track_descriptor(process_track_uuid: u64) -> idl::TrackDescriptor {
    let thread_name = std::thread::current().name().map(ToString::to_string);
    let pid = i32::try_from(std::process::id()).ok();
    let thread = pid
        .zip(current_trace_thread_id())
        .map(|(pid, tid)| idl::ThreadDescriptor {
            pid: Some(pid),
            tid: Some(tid),
            thread_name: thread_name.clone(),
            ..Default::default()
        });
    idl::TrackDescriptor {
        uuid: Some(current_thread_uuid()),
        static_or_dynamic_name: thread_name.as_deref().map(packets::track_name),
        // A thread track belongs to its process through its thread descriptor.
        parent_uuid: thread.is_none().then_some(process_track_uuid),
        thread,
        ..Default::default()
    }
}

/// Makes the calling thread report `kernel_thread_id` in place of the kernel's ID for it.
#[cfg(test)]
pub(crate) fn override_kernel_thread_id(kernel_thread_id: Option<u64>) {
    KERNEL_THREAD_ID_SOURCE.with(|source| {
        source.set(KernelThreadIdSource::Override(kernel_thread_id));
    });
}

/// Returns the trace thread ID of the calling thread, if the kernel's ID for it is known.
fn current_trace_thread_id() -> Option<i32> {
    #[cfg(test)]
    if let KernelThreadIdSource::Override(kernel_thread_id) =
        KERNEL_THREAD_ID_SOURCE.with(std::cell::Cell::get)
    {
        return trace_thread_id(kernel_thread_id);
    }
    TRACE_THREAD_ID.with(|tid| *tid)
}

/// Converts the kernel's thread ID to the trace's 32-bit thread ID.
///
/// An ID that does not fit is dropped rather than clamped, since clamping gives every such
/// thread the same ID.
fn trace_thread_id(kernel_thread_id: Option<u64>) -> Option<i32> {
    kernel_thread_id.and_then(|id| i32::try_from(id).ok())
}

/// Reads the calling thread's ID from the `<pid>/task/<tid>` target of `/proc/thread-self`.
///
/// The `thread-id` crate returns the `pthread_t` address on Linux rather than the kernel
/// thread ID, and the `gettid` syscall needs unsafe code, so the ID is unknown without procfs.
/// A procfs from another PID namespace names other IDs, so a link whose PID is not this
/// process's is rejected.
#[cfg(target_os = "linux")]
fn kernel_thread_id() -> Option<u64> {
    let target = std::fs::read_link("/proc/thread-self").ok()?;
    let mut components = target.to_str()?.split('/');
    let pid = components.next()?.parse::<u32>().ok()?;
    let task = components.next()?;
    let tid = components.next()?.parse().ok()?;
    let complete = components.next().is_none();
    (pid == std::process::id() && task == "task" && complete).then_some(tid)
}

/// Returns [`thread_id::get`], which is the kernel thread ID on macOS and Windows.
#[cfg(any(target_os = "macos", target_os = "windows"))]
fn kernel_thread_id() -> Option<u64> {
    u64::try_from(thread_id::get()).ok()
}

/// Returns `None`, since the kernel thread ID has no known source on this platform.
#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
fn kernel_thread_id() -> Option<u64> {
    None
}
