use crate::{LayerId, idl};
use std::cell::RefCell;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
thread_local! {
    static THREAD_TRACK_UUID: AtomicU64 = AtomicU64::new(unique_uuid());
    static OS_THREAD_ID: i32 = os_thread_id();
    /// The layers whose trace already describes the calling thread's track.
    static THREAD_TRACK_DESCRIBED_BY: RefCell<HashSet<LayerId>> = RefCell::new(HashSet::new());
}

#[derive(Default)]
pub struct DebugAnnotations {
    pub annotations: Vec<idl::DebugAnnotation>,
}

/// Returns a random track UUID, which is never 0.
pub fn unique_uuid() -> u64 {
    rand::random_range(1..=u64::MAX)
}

/// Marks the calling thread's track as described in the trace of `layer_id`.
///
/// Returns whether this is the first time, in which case the caller writes the thread's track
/// descriptor with the packet that first references the track.
/// During thread teardown, when the record is gone, every call counts as the first, so a packet
/// never refers to an undescribed track.
pub fn first_thread_track_use(layer_id: LayerId) -> bool {
    THREAD_TRACK_DESCRIBED_BY
        .try_with(|layers| layers.borrow_mut().insert(layer_id))
        .unwrap_or(true)
}

pub fn current_thread_uuid() -> u64 {
    THREAD_TRACK_UUID.with(|id| id.load(Ordering::Relaxed))
}

pub fn current_thread_descriptor() -> idl::ThreadDescriptor {
    let mut thread = idl::ThreadDescriptor::default();
    thread.pid = Some(i32::try_from(std::process::id()).unwrap_or(i32::MAX));
    thread.tid = Some(OS_THREAD_ID.with(|tid| *tid));
    thread.thread_name = std::thread::current().name().map(ToString::to_string);
    thread
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

pub fn current_thread_track_descriptor() -> idl::TrackDescriptor {
    let thread_track_uuid = THREAD_TRACK_UUID.with(|id| id.load(Ordering::Relaxed));
    let thread_desc = current_thread_descriptor().into();

    create_track_descriptor(
        thread_track_uuid.into(),
        None,
        std::thread::current().name(),
        None,
        thread_desc,
        None,
    )
}

impl idl::TrackDescriptor {
    pub fn for_process_descriptor(uuid: u64, process_descriptor: idl::ProcessDescriptor) -> Self {
        idl::TrackDescriptor {
            uuid: Some(uuid),
            process: Some(process_descriptor),
            ..Default::default()
        }
    }
}

pub fn create_track_descriptor(
    uuid: Option<u64>,
    parent_uuid: Option<u64>,
    name: Option<impl AsRef<str>>,
    process: Option<idl::ProcessDescriptor>,
    thread: Option<idl::ThreadDescriptor>,
    counter: Option<idl::CounterDescriptor>,
) -> idl::TrackDescriptor {
    idl::TrackDescriptor {
        uuid,
        parent_uuid,
        static_or_dynamic_name: name
            .map(|s| s.as_ref().to_string())
            .map(idl::track_descriptor::StaticOrDynamicName::Name),
        process,
        thread,
        counter,
        ..Default::default()
    }
}

pub fn current_process_descriptor() -> idl::ProcessDescriptor {
    let mut process = idl::ProcessDescriptor::default();
    process.pid = Some(i32::try_from(std::process::id()).unwrap_or(i32::MAX));
    process
}

pub fn process_descriptor(process_track_uuid: u64) -> idl::TracePacket {
    let track_desc = idl::TrackDescriptor::for_process_descriptor(
        process_track_uuid,
        current_process_descriptor(),
    );

    idl::TracePacket {
        data: Some(idl::trace_packet::Data::TrackDescriptor(track_desc)),
        ..Default::default()
    }
}

pub fn create_event(
    track_uuid: u64,
    name: Option<&str>,
    location: Option<(&str, u32)>,
    debug_annotations: DebugAnnotations,
    track_event_type: Option<idl::track_event::Type>,
) -> idl::TrackEvent {
    let mut event = idl::TrackEvent {
        track_uuid: Some(track_uuid),
        name_field: name.map(|name| idl::track_event::NameField::Name(name.to_string())),
        ..Default::default()
    };
    if let Some(t) = track_event_type {
        event.set_type(t);
    }

    if !debug_annotations.annotations.is_empty() {
        event.debug_annotations = debug_annotations.annotations;
    }
    if let Some((file, line)) = location {
        let source_location = idl::SourceLocation {
            file_name: Some(file.to_owned()),
            line_number: Some(line),
            ..Default::default()
        };
        let location = idl::track_event::SourceLocationField::SourceLocation(source_location);
        event.source_location_field = Some(location);
    }
    event
}
