//! The calling thread's track: its UUID, its descriptor with the kernel's thread ID, and whether
//! the thread is still recording.

use crate::{idl, packets};
use thread_local::ThreadLocal;

thread_local! {
    /// The UUID of the calling thread's track, shared by every layer in the process.
    static THREAD_TRACK_UUID: u64 = packets::unique_uuid();
    /// The kernel's ID for the calling thread, read once per thread.
    static OS_THREAD_ID: i32 = os_thread_id();
    /// Alive until the calling thread's thread-local values are destroyed.
    static LIVE_THREAD: LiveThread = LiveThread::new();
}

/// The [`ThreadLocal`] whose first use on a thread assigns the thread its [`ThreadLocal`] ID.
static THREAD_LOCAL_ID: ThreadLocal<()> = ThreadLocal::new();

/// A thread-local value whose destruction marks the start of its thread's teardown.
struct LiveThread;

impl LiveThread {
    fn new() -> Self {
        // `ThreadLocal` releases a thread's ID in a thread-local destructor of its own, after
        // which a lookup on that thread panics.
        // Assigning the ID first registers that destructor before this value's, and the standard
        // library runs thread-local destructors in reverse order of registration, so the ID
        // outlives this value.
        THREAD_LOCAL_ID.get_or(|| ());
        Self
    }
}

impl Drop for LiveThread {
    /// Does nothing, because only the destruction itself matters to [`live_thread_uuid`].
    fn drop(&mut self) {}
}

/// Returns the UUID of the calling thread's track.
pub(crate) fn current_thread_uuid() -> u64 {
    THREAD_TRACK_UUID.with(|uuid| *uuid)
}

/// Returns the UUID of the calling thread's track, or `None` once the thread's teardown has
/// begun.
///
/// While this returns a UUID, every [`ThreadLocal`] can be looked up on the calling thread.
pub(crate) fn live_thread_uuid() -> Option<u64> {
    LIVE_THREAD.try_with(|_| current_thread_uuid()).ok()
}

/// Returns the descriptor of the calling thread's track, named after the thread.
pub(crate) fn current_thread_track_descriptor() -> idl::TrackDescriptor {
    let thread = idl::ThreadDescriptor {
        pid: Some(i32::try_from(std::process::id()).unwrap_or(i32::MAX)),
        tid: Some(OS_THREAD_ID.with(|tid| *tid)),
        thread_name: std::thread::current().name().map(ToString::to_string),
        ..Default::default()
    };
    idl::TrackDescriptor {
        uuid: Some(current_thread_uuid()),
        static_or_dynamic_name: std::thread::current().name().map(packets::track_name),
        thread: Some(thread),
        ..Default::default()
    }
}

/// Returns the kernel's ID for the calling thread, as the trace's thread descriptors expect.
///
/// On Linux, [`thread_id::get`] returns the `pthread_t` address rather than the kernel thread
/// ID, so the ID comes from procfs and falls back to [`thread_id::get`] only when procfs is
/// unavailable.
#[cfg(target_os = "linux")]
fn os_thread_id() -> i32 {
    procfs_thread_id().unwrap_or_else(fallback_thread_id)
}

/// Returns the kernel's ID for the calling thread, as the trace's thread descriptors expect.
#[cfg(not(target_os = "linux"))]
fn os_thread_id() -> i32 {
    fallback_thread_id()
}

/// Reads the calling thread's ID from the `<pid>/task/<tid>` target of `/proc/thread-self`.
///
/// A procfs from another PID namespace names other IDs, so a link whose PID is not this
/// process's is rejected.
#[cfg(target_os = "linux")]
fn procfs_thread_id() -> Option<i32> {
    let target = std::fs::read_link("/proc/thread-self").ok()?;
    let mut components = target.to_str()?.split('/');
    let pid = components.next()?.parse::<u32>().ok()?;
    let task = components.next()?;
    let tid = components.next()?.parse().ok()?;
    let complete = components.next().is_none();
    (pid == std::process::id() && task == "task" && complete).then_some(tid)
}

/// Returns [`thread_id::get`], which is the kernel thread ID on macOS and Windows.
fn fallback_thread_id() -> i32 {
    i32::try_from(thread_id::get()).unwrap_or(i32::MAX)
}
