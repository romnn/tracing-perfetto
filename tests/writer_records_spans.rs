//! A writer that creates spans while the layer calls it and keeps them, through a test-owned
//! global subscriber.
//!
//! A scoped dispatcher hands a nested record no subscriber, so only the global default reaches
//! the layer again from inside its own write.

use googletest::prelude::*;
use parking_lot::Mutex;
use prost::Message;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing_perfetto::PerfettoLayer;
use tracing_subscriber::{fmt::MakeWriter, layer::SubscriberExt};

/// The parts of the Perfetto `Trace` message that this test reads, with the field numbers of
/// `protos/perfetto_trace.proto`.
#[derive(Clone, PartialEq, Message)]
struct Trace {
    #[prost(message, repeated, tag = "1")]
    packet: Vec<TracePacket>,
}

#[derive(Clone, PartialEq, Message)]
struct TracePacket {
    #[prost(message, optional, tag = "11")]
    track_event: Option<TrackEvent>,
    #[prost(message, optional, tag = "60")]
    track_descriptor: Option<TrackDescriptor>,
}

#[derive(Clone, PartialEq, Message)]
struct TrackEvent {
    #[prost(int32, optional, tag = "9")]
    r#type: Option<i32>,
    #[prost(uint64, optional, tag = "11")]
    track_uuid: Option<u64>,
    #[prost(string, optional, tag = "23")]
    name: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
struct TrackDescriptor {
    #[prost(uint64, optional, tag = "1")]
    uuid: Option<u64>,
    #[prost(uint64, optional, tag = "5")]
    parent_uuid: Option<u64>,
}

/// The `TrackEvent.Type` of a slice begin.
const SLICE_BEGIN: i32 = 1;

/// The `TrackEvent.Type` of a slice end.
const SLICE_END: i32 = 2;

/// A sink that accumulates the trace in memory, and creates a root span and a child of the
/// armed parent from inside the first write after it is armed, which it keeps.
#[derive(Clone, Default)]
struct SpanningWriter {
    buf: Arc<Mutex<Vec<u8>>>,
    armed_parent: Arc<Mutex<Option<tracing::Span>>>,
    kept: Arc<Mutex<Vec<tracing::Span>>>,
}

impl<'a> MakeWriter<'a> for SpanningWriter {
    type Writer = SpanningWriter;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

impl std::io::Write for SpanningWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let armed_parent = self.armed_parent.lock().take();
        if let Some(parent) = armed_parent {
            let root = tracing::info_span!(parent: None, "root_from_the_writer");
            let child = tracing::info_span!(parent: &parent, "child_from_the_writer");
            self.kept.lock().extend([root, child]);
        }
        self.buf.lock().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Checks that every track a packet refers to is described by an earlier packet, and that every
/// slice end closes a slice that began on its own track, and returns the track of each named
/// event.
fn replay(trace: &Trace) -> Result<HashMap<String, u64>> {
    let mut described = HashSet::new();
    let mut open_slices: HashMap<u64, Vec<String>> = HashMap::new();
    let mut tracks = HashMap::new();
    for packet in &trace.packet {
        if let Some(descriptor) = &packet.track_descriptor {
            if let Some(parent) = descriptor.parent_uuid {
                verify_that!(described.contains(&parent), eq(true))?;
            }
            described.insert(descriptor.uuid.or_fail()?);
        }
        let Some(event) = &packet.track_event else {
            continue;
        };
        let track_uuid = event.track_uuid.or_fail()?;
        if !described.contains(&track_uuid) {
            return Err(format!(
                "{event:?} refers to the undescribed track {track_uuid}"
            ))
            .or_fail();
        }
        let stack = open_slices.entry(track_uuid).or_default();
        match event.r#type {
            Some(SLICE_BEGIN) => stack.push(event.name.clone().or_fail()?),
            Some(SLICE_END) if stack.pop().is_none() => {
                return Err(format!("a slice end on track {track_uuid} closes no slice")).or_fail();
            }
            _ => {}
        }
        if let Some(name) = &event.name {
            tracks.insert(name.clone(), track_uuid);
        }
    }
    let unended: Vec<_> = open_slices.values().flatten().collect();
    verify_that!(unended, is_empty())?;
    Ok(tracks)
}

/// Spans that the writer creates while the layer calls it, and keeps, are left out of the
/// trace, so that events under them land on the track of their nearest recorded ancestor and
/// their closes end no slice.
///
/// Their descriptors and begins cannot be written from inside the writer, so a layer that still
/// placed them would put the root's events on a track the trace never describes, and end the
/// recorded parent's slice when the child closes.
#[gtest]
fn spans_that_the_writer_creates_are_left_out_of_the_trace() -> Result<()> {
    let writer = SpanningWriter::default();
    let output = writer.clone();
    let layer = PerfettoLayer::new(writer.clone());
    let finish = layer.finish_handle();
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(layer))
        .or_fail()?;

    let parent = tracing::info_span!("parent");
    *writer.armed_parent.lock() = Some(parent.clone());
    tracing::info!(name: "trigger", "its write creates the spans");
    let kept = std::mem::take(&mut *writer.kept.lock());
    let [root, child] = <[tracing::Span; 2]>::try_from(kept)
        .map_err(|kept| format!("the writer created {} spans", kept.len()))
        .or_fail()?;
    tracing::info!(name: "under_the_root", parent: &root, "recorded by the test");
    tracing::info!(name: "under_the_child", parent: &child, "recorded by the test");
    drop(child);
    drop(root);
    drop(parent);

    verify_that!(finish.finish(), ok(anything()))?;
    let trace = Trace::decode(output.buf.lock().as_slice()).or_fail()?;
    let tracks = replay(&trace)?;
    // The root has no recorded ancestor, so its event lands on the thread's track, as the
    // trigger does, and the child's event lands on its recorded parent's track.
    expect_that!(tracks.get("under_the_root"), eq(tracks.get("trigger")));
    expect_that!(tracks.get("under_the_child"), eq(tracks.get("parent")));
    expect_that!(tracks.get("under_the_child"), some(anything()));
    expect_that!(
        tracks.keys().collect::<Vec<_>>(),
        each(not(ends_with("from_the_writer")))
    );
    Ok(())
}
