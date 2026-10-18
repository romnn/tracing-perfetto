//! The calling thread's track: its UUID, its descriptor with the kernel's thread ID, and which
//! layers have described it.

use crate::layer::LayerId;
use crate::{idl, packets};
use std::cell::RefCell;
use std::collections::HashSet;

thread_local! {
    /// The UUID of the calling thread's track, shared by every layer in the process.
    static THREAD_TRACK_UUID: u64 = packets::unique_uuid();
    /// The kernel's ID for the calling thread, read once per thread.
    static OS_THREAD_ID: i32 = os_thread_id();
    /// The layers whose trace already describes the calling thread's track.
    static THREAD_TRACK_DESCRIBED_BY: RefCell<HashSet<LayerId>> = RefCell::new(HashSet::new());
}

/// Returns the UUID of the calling thread's track.
pub(crate) fn current_thread_uuid() -> u64 {
    THREAD_TRACK_UUID.with(|uuid| *uuid)
}

/// Marks the calling thread's track as described in the trace of `layer_id`.
///
/// Returns whether this is the first time, in which case the caller writes the thread's track
/// descriptor with the packet that first references the track.
/// During thread teardown, when the record is gone, every call counts as the first, so a packet
/// never refers to an undescribed track.
pub(crate) fn first_thread_track_use(layer_id: LayerId) -> bool {
    THREAD_TRACK_DESCRIBED_BY
        .try_with(|layers| layers.borrow_mut().insert(layer_id))
        .unwrap_or(true)
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
