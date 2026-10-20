//! Children of a span that no layer enables, which become roots because the span is
//! `Span::none`.
//!
//! A span is `Span::none` only while no dispatcher in the process enables its callsite, because
//! tracing caches one interest per callsite for all of them.
//! These tests therefore run in a test binary of their own, in which every dispatcher disables
//! the `filtered_out` callsites, and any test added here must disable them as well.

use googletest::prelude::*;
use parking_lot::Mutex;
use prost::Message;
use std::sync::Arc;
use tracing::instrument::Instrument as _;
use tracing::trace_span;
use tracing_perfetto::PerfettoLayer;
use tracing_subscriber::Layer as _;
use tracing_subscriber::filter::filter_fn;
use tracing_subscriber::{fmt::MakeWriter, layer::SubscriberExt};

/// The parts of the Perfetto `Trace` message that these tests read, with the field numbers of
/// `protos/perfetto_trace.proto`.
#[derive(Clone, PartialEq, Message)]
struct Trace {
    #[prost(message, repeated, tag = "1")]
    packet: Vec<TracePacket>,
}

#[derive(Clone, PartialEq, Message)]
struct TracePacket {
    #[prost(message, optional, tag = "60")]
    track_descriptor: Option<TrackDescriptor>,
}

#[derive(Clone, PartialEq, Message)]
struct TrackDescriptor {
    #[prost(uint64, optional, tag = "1")]
    uuid: Option<u64>,
    #[prost(string, optional, tag = "2")]
    name: Option<String>,
    #[prost(message, optional, tag = "3")]
    process: Option<Described>,
    #[prost(message, optional, tag = "4")]
    thread: Option<Described>,
    #[prost(uint64, optional, tag = "5")]
    parent_uuid: Option<u64>,
}

/// A process or thread descriptor, of which these tests read only whether it is present.
#[derive(Clone, PartialEq, Message)]
struct Described {}

/// A sink that a [`PerfettoLayer`] writes to, accumulating the trace in memory.
///
/// Every clone shares one buffer, so the test keeps a clone to read what the layer wrote.
#[derive(Clone, Default)]
struct TestWriter {
    buf: Arc<Mutex<Vec<u8>>>,
}

impl<'a> MakeWriter<'a> for TestWriter {
    type Writer = TestWriter;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

impl std::io::Write for TestWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buf.lock().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Returns a subscriber whose only layer records every span except those named `filtered_out`,
/// and a writer that shares the layer's buffer.
fn filtering_out_one_span() -> (impl tracing::Subscriber + Send + Sync + 'static, TestWriter) {
    let writer = TestWriter::default();
    let output = writer.clone();
    let layer = PerfettoLayer::new(writer)
        .with_filter(filter_fn(|metadata| metadata.name() != "filtered_out"));
    (tracing_subscriber::registry().with(layer), output)
}

/// Checks that the span named `child` is a root: its track is its own and sits directly below
/// the process track, beside the track of the span named `root`.
fn verify_child_is_a_root(output: &TestWriter) -> Result<()> {
    let trace = Trace::decode(output.buf.lock().as_slice()).or_fail()?;
    let descriptors: Vec<&TrackDescriptor> = trace
        .packet
        .iter()
        .filter_map(|packet| packet.track_descriptor.as_ref())
        .collect();
    let process_track = descriptors
        .iter()
        .find(|descriptor| descriptor.process.is_some())
        .and_then(|descriptor| descriptor.uuid)
        .or_fail()?;

    // A span track describes no process or thread, and is named after its root span.
    let span_track_named = |name: &str| -> Result<&TrackDescriptor> {
        let named: Vec<&TrackDescriptor> = descriptors
            .iter()
            .copied()
            .filter(|descriptor| {
                descriptor.process.is_none()
                    && descriptor.thread.is_none()
                    && descriptor.name.as_deref() == Some(name)
            })
            .collect();
        let [descriptor] = named[..] else {
            return Err(format!(
                "expected one span track named {name}, got {}",
                named.len()
            ))
            .or_fail();
        };
        Ok(descriptor)
    };
    let child_track = span_track_named("child")?;
    let root_track = span_track_named("root")?;
    expect_eq!(child_track.parent_uuid, Some(process_track));
    expect_ne!(child_track.uuid, root_track.uuid);
    Ok(())
}

/// When no layer enables a filtered-out span's callsite, a span created inside a task that the
/// filtered-out span instruments, polled on another thread, is a root.
///
/// The filtered-out span is `Span::none`, so entering it on the worker enters nothing, and the
/// worker has no current span that the new span could nest under.
#[gtest]
fn child_in_a_task_instrumented_with_a_span_no_layer_enables_is_a_root() -> Result<()> {
    let (subscriber, output) = filtering_out_one_span();
    let dispatch = tracing::Dispatch::new(subscriber);

    // The filtered-out span is created inside the root, under the dispatcher that rejects it.
    let (root, task_span) = tracing::dispatcher::with_default(&dispatch, || {
        let root = trace_span!("root");
        let task_span = root.in_scope(|| trace_span!("filtered_out"));
        (root, task_span)
    });
    expect_true!(task_span.is_disabled());

    // The worker polls a task instrumented with the filtered-out span.
    let task = async { trace_span!("child").in_scope(|| {}) }.instrument(task_span);
    std::thread::scope(|scope| {
        scope
            .spawn(|| {
                tracing::dispatcher::with_default(&dispatch, || {
                    let mut task = std::pin::pin!(task);
                    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
                    task.as_mut().poll(&mut context).is_ready()
                })
            })
            .join()
    })
    .or_fail()
    .and_then(|ready| verify_true!(ready))?;
    drop(root);

    verify_child_is_a_root(&output)
}

/// When no layer enables a filtered-out span's callsite, the span does not exist, so a span
/// that names it as its explicit parent is a root with a track of its own.
///
/// The registry never creates the filtered-out span, so nothing links the child to the spans
/// the filtered-out span would have nested in.
#[gtest]
fn child_of_a_span_no_layer_enables_is_a_root() -> Result<()> {
    let (subscriber, output) = filtering_out_one_span();
    let _guard = tracing::subscriber::set_default(subscriber);

    let root = trace_span!("root");
    let filtered_out = trace_span!(parent: &root, "filtered_out");
    expect_true!(filtered_out.is_disabled());
    trace_span!(parent: &filtered_out, "child").in_scope(|| {});
    drop(root);

    verify_child_is_a_root(&output)
}
