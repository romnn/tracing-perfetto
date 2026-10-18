use crate::idl;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
thread_local! {
    static THREAD_TRACK_UUID: AtomicU64 = AtomicU64::new(unique_uuid());
    static THREAD_DESCRIPTOR_SENT: AtomicBool = const { AtomicBool::new(false) };
}

#[derive(Default)]
pub struct DebugAnnotations {
    pub annotations: Vec<idl::DebugAnnotation>,
}

pub fn unique_uuid() -> u64 {
    rand::random()
}

pub fn current_thread_uuid() -> u64 {
    THREAD_TRACK_UUID.with(|id| id.load(Ordering::Relaxed))
}

pub fn current_thread_descriptor() -> idl::ThreadDescriptor {
    let mut thread = idl::ThreadDescriptor::default();
    thread.pid = Some(i32::try_from(std::process::id()).unwrap_or(i32::MAX));
    thread.tid = Some(i32::try_from(thread_id::get()).unwrap_or(i32::MAX));
    thread.thread_name = std::thread::current().name().map(ToString::to_string);
    thread
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
    pub fn named_child_for(name: &str, parent_uuid: u64) -> Self {
        idl::TrackDescriptor {
            uuid: Some(unique_uuid()),
            parent_uuid: Some(parent_uuid),
            static_or_dynamic_name: Some(idl::track_descriptor::StaticOrDynamicName::Name(
                name.to_string(),
            )),
            ..Default::default()
        }
    }

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
        categories: vec![String::new()],
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
