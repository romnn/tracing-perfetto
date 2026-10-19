//! Regression tests for a writer that fails: the output stays a readable trace up to the failed
//! write, and finishing reports the error.

use crate::idl;
use crate::tests::{close_labeled, event_name, replay_slices, track_descriptor, track_event};
use crate::{FinishHandle, PerfettoLayer};
use googletest::prelude::*;
use parking_lot::Mutex;
use prost::Message;
use std::collections::HashSet;
use std::sync::{Arc, mpsc};
use std::time::Duration;
use tracing::{field, info, info_span};
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::layer::SubscriberExt;

/// The message of the error that a [`FailingWriter`] returns.
const INJECTED_ERROR: &str = "injected write failure";

/// A sink that fails the first message its predicate selects and writes every other message in
/// full.
///
/// The failed message's first `fragment_len` bytes reach the output before the error, as when
/// `write_all` stops partway through a message.
/// The writer accepts every later message again, so only the layer can keep the output from
/// continuing after the failure.
struct FailingWriter {
    state: Arc<Mutex<FailingWriterState>>,
}

struct FailingWriterState {
    buf: Vec<u8>,
    /// Selects the message to fail, decoded from the buffer that the layer hands over.
    fails: Box<dyn Fn(&idl::Trace) -> bool + Send>,
    fragment_len: usize,
    /// The offset in `buf` at which the failed message starts, once a message has failed.
    failure_offset: Option<usize>,
}

/// What a [`FailingWriter`] has written.
struct Written {
    bytes: Vec<u8>,
    failure_offset: Option<usize>,
}

impl FailingWriter {
    /// Returns a writer that fails the first message that `fails` selects, after writing
    /// `fragment_len` bytes of it.
    fn new(fragment_len: usize, fails: impl Fn(&idl::Trace) -> bool + Send + 'static) -> Self {
        Self {
            state: Arc::new(Mutex::new(FailingWriterState {
                buf: Vec::new(),
                fails: Box::new(fails),
                fragment_len,
                failure_offset: None,
            })),
        }
    }

    /// Returns a writer that shares this writer's output.
    fn output(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
        }
    }

    fn written(&self) -> Written {
        let state = self.state.lock();
        Written {
            bytes: state.buf.clone(),
            failure_offset: state.failure_offset,
        }
    }
}

impl<'a> MakeWriter<'a> for FailingWriter {
    type Writer = MessageWriter;

    fn make_writer(&'a self) -> Self::Writer {
        MessageWriter {
            state: Arc::clone(&self.state),
            failing: false,
        }
    }
}

/// The writer of one message, which `write_all` calls until the message is written or a call
/// fails.
struct MessageWriter {
    state: Arc<Mutex<FailingWriterState>>,
    /// Whether this message fails, so every further call of `write_all` returns the error.
    failing: bool,
}

impl std::io::Write for MessageWriter {
    /// Writes `buf`, or only the fragment of it that precedes the injected error.
    ///
    /// The first call for a message receives the whole message, which decides whether it fails.
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if self.failing {
            return Err(std::io::Error::other(INJECTED_ERROR));
        }
        let mut state = self.state.lock();
        let fails = state.failure_offset.is_none()
            && idl::Trace::decode(buf).is_ok_and(|trace| (state.fails)(&trace));
        if !fails {
            state.buf.extend_from_slice(buf);
            return Ok(buf.len());
        }

        // Write the fragment, which `write_all` follows with a call that returns the error
        self.failing = true;
        state.failure_offset = Some(state.buf.len());
        let fragment = buf.get(..state.fragment_len).unwrap_or_default();
        state.buf.extend_from_slice(fragment);
        if fragment.is_empty() {
            return Err(std::io::Error::other(INJECTED_ERROR));
        }
        Ok(fragment.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        // Writes append to the buffer immediately, so there is nothing to flush.
        Ok(())
    }
}

/// Selects every message.
fn any_message(_: &idl::Trace) -> bool {
    true
}

/// Selects a message that describes a thread's track.
fn describes_a_thread(trace: &idl::Trace) -> bool {
    trace
        .packet
        .iter()
        .filter_map(track_descriptor)
        .any(|descriptor| descriptor.thread.is_some())
}

/// Selects a message that describes a counter track.
fn describes_a_counter(trace: &idl::Trace) -> bool {
    trace
        .packet
        .iter()
        .filter_map(track_descriptor)
        .any(|descriptor| descriptor.counter.is_some())
}

/// Selects a message that holds an instant.
fn holds_an_instant(trace: &idl::Trace) -> bool {
    trace
        .packet
        .iter()
        .filter_map(track_event)
        .any(|event| event.r#type() == idl::track_event::Type::Instant)
}

/// Selects a message that begins a slice named `child`.
fn begins_the_child(trace: &idl::Trace) -> bool {
    trace.packet.iter().filter_map(track_event).any(|event| {
        event.r#type() == idl::track_event::Type::SliceBegin && event_name(event) == Some("child")
    })
}

/// Returns a layer that writes to `writer`, with debug annotations so that [`close_labeled`]
/// labels each slice's end.
fn failing_layer(writer: FailingWriter) -> PerfettoLayer<FailingWriter> {
    PerfettoLayer::new(writer).with_debug_annotations(true)
}

/// Records two rounds of a parent span with a child nested on its track, each entered on the
/// calling thread, with an instant inside the child and one outside any span.
///
/// The second round writes every kind of message again after a failure in the first.
fn record_rounds(layer: PerfettoLayer<FailingWriter>) {
    let subscriber = tracing_subscriber::registry().with(layer);
    tracing::subscriber::with_default(subscriber, || {
        for _ in 0..2 {
            let parent = info_span!("parent", closes = field::Empty);
            let child = info_span!(parent: &parent, "child", closes = field::Empty);
            parent.in_scope(|| child.in_scope(|| info!("inside")));
            info!("outside");
            close_labeled(child, "child");
            close_labeled(parent, "parent");
        }
    });
}

/// Checks that `trace` reads as a trace whose every packet trace processor can place.
///
/// A trace that is not empty opens with the preamble, which defines the sequence clock that
/// every timestamped packet reads.
/// Every track that a packet refers to is described by an earlier packet, and every slice end
/// closes an open slice of its own track.
fn expect_readable(trace: &idl::Trace) -> Result<()> {
    if let Some(first) = trace.packet.first() {
        let process_described =
            track_descriptor(first).is_some_and(|descriptor| descriptor.process.is_some());
        verify_that!(process_described, eq(true))?;
        let clock_defined = trace.packet.get(1).is_some_and(|packet| {
            matches!(packet.data, Some(idl::trace_packet::Data::ClockSnapshot(_)))
        });
        verify_that!(clock_defined, eq(true))?;
    }

    let mut described = HashSet::new();
    let mut undescribed = Vec::new();
    for packet in &trace.packet {
        match &packet.data {
            Some(idl::trace_packet::Data::TrackDescriptor(descriptor)) => {
                undescribed.extend(
                    descriptor
                        .parent_uuid
                        .filter(|parent| !described.contains(parent)),
                );
                described.insert(descriptor.uuid());
            }
            Some(idl::trace_packet::Data::TrackEvent(event)) => {
                undescribed.extend(event.track_uuid.filter(|track| !described.contains(track)));
            }
            _ => {}
        }
    }
    verify_that!(undescribed, is_empty())?;

    replay_slices(trace)?;
    Ok(())
}

/// Checks that the writer failed a message and that nothing follows the `fragment_len` bytes of
/// it that reached the output, and returns the whole messages written before it.
///
/// The writer accepts every message after the failed one, so any byte after the fragment is a
/// message that the layer wrote after the failure.
fn expect_nothing_after_failure(writer: &FailingWriter, fragment_len: usize) -> Result<Vec<u8>> {
    let Written {
        mut bytes,
        failure_offset,
    } = writer.written();
    let failure_offset = failure_offset.or_fail()?;
    verify_that!(bytes.len(), eq(failure_offset + fragment_len))?;
    bytes.truncate(failure_offset);
    Ok(bytes)
}

/// Checks that the output reads as a trace that ends where the writer failed a message without
/// writing any of it.
fn expect_readable_up_to_failure(writer: &FailingWriter) -> Result<()> {
    let Written { bytes, .. } = writer.written();
    expect_readable(&idl::Trace::decode(bytes.as_slice()).or_fail()?)?;
    expect_nothing_after_failure(writer, 0)?;
    Ok(())
}

/// Checks that finishing returns the writer's error, and that only the first finish does.
fn expect_finish_reports_the_failure(finish: &FinishHandle) -> Result<()> {
    verify_that!(finish.finish(), err(displays_as(eq(INJECTED_ERROR))))?;
    verify_that!(finish.finish(), ok(anything()))
}

/// A preamble whose write fails leaves an empty trace instead of one without its preamble.
///
/// Without the preamble no packet can be placed: every timestamp reads the sequence clock that
/// only the preamble's clock snapshot defines.
#[gtest]
fn a_failed_preamble_write_leaves_no_packet_without_the_preamble() -> Result<()> {
    let writer = FailingWriter::new(0, any_message);
    let output = writer.output();
    let layer = failing_layer(writer);
    let finish = layer.finish_handle();

    record_rounds(layer);

    expect_readable_up_to_failure(&output)?;
    expect_finish_reports_the_failure(&finish)
}

/// A write that fails partway through a message leaves the messages written before it, followed
/// only by the fragment of the failed one.
///
/// A message appended after the fragment would be read as the fragment's remainder, which loses
/// the framing of every later message.
#[gtest]
fn a_partial_write_leaves_whole_messages_and_nothing_after_the_fragment() -> Result<()> {
    const FRAGMENT_LEN: usize = 3;
    let writer = FailingWriter::new(FRAGMENT_LEN, holds_an_instant);
    let output = writer.output();
    let layer = failing_layer(writer);
    let finish = layer.finish_handle();

    record_rounds(layer);

    let written = expect_nothing_after_failure(&output, FRAGMENT_LEN)?;
    let trace = idl::Trace::decode(written.as_slice()).or_fail()?;
    verify_that!(trace.packet, not(is_empty()))?;
    expect_readable(&trace)?;
    expect_finish_reports_the_failure(&finish)
}

/// A thread's track descriptor whose write fails is never followed by a packet on that track.
///
/// A layer that took the track for described after the failed write would place every later
/// slice and instant of the thread on a track the trace never describes.
#[gtest]
fn a_failed_thread_descriptor_write_leaves_no_packet_on_the_thread_track() -> Result<()> {
    let writer = FailingWriter::new(0, describes_a_thread);
    let output = writer.output();
    let layer = failing_layer(writer);
    let finish = layer.finish_handle();

    record_rounds(layer);

    expect_readable_up_to_failure(&output)?;
    expect_finish_reports_the_failure(&finish)
}

/// A child whose begin fails to write never ends its parent's slice.
///
/// The child nests on its parent's track, so a child end written after its begin was lost would
/// close the parent's slice, and the parent's own end would close nothing.
#[gtest]
fn a_failed_child_begin_never_ends_the_parent_slice() -> Result<()> {
    let writer = FailingWriter::new(0, begins_the_child);
    let output = writer.output();
    let layer = failing_layer(writer);
    let finish = layer.finish_handle();

    record_rounds(layer);

    expect_readable_up_to_failure(&output)?;
    expect_finish_reports_the_failure(&finish)
}

/// A memory source that reports 1 KiB and announces each sample it reports.
struct AnnouncingProcessMemory {
    announcements: mpsc::Sender<()>,
}

impl crate::process_memory::ProcessMemorySource for AnnouncingProcessMemory {
    fn resident_set_size_bytes(&self) -> Option<u64> {
        // The receiver is gone only after the test stopped waiting for samples.
        self.announcements.send(()).ok();
        Some(1024)
    }
}

/// A counter track descriptor whose write fails is never followed by a sample on that track.
///
/// A sampler that took the counter track for described after the failed write would write every
/// later sample on a track the trace never describes.
#[gtest]
fn a_failed_counter_descriptor_write_leaves_no_sample_on_the_counter_track() -> Result<()> {
    let writer = FailingWriter::new(0, describes_a_counter);
    let output = writer.output();
    let (announcements, announced) = mpsc::channel();
    let layer = PerfettoLayer::new(writer).with_process_memory_source(
        Duration::ZERO,
        Box::new(AnnouncingProcessMemory { announcements }),
    );
    let finish = layer.finish_handle();

    // The sampler writes each sample before it reads the next, so the third announcement proves
    // that the failed first sample and a second one were written.
    for _ in 0..3 {
        announced.recv().or_fail()?;
    }
    expect_finish_reports_the_failure(&finish)?;

    expect_readable_up_to_failure(&output)?;
    drop(layer);
    Ok(())
}
