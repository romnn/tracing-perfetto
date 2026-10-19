//! Regression tests for a writer that fails or panics: the output stays a readable trace up to
//! the failed write, and finishing reports the error.

use crate::idl;
use crate::tests::{
    annotated_layer, close_labeled, decode, event_name, instant_marked, replay_slices,
    track_descriptor, track_event,
};
use crate::{FinishHandle, PerfettoLayer};
use googletest::prelude::*;
use parking_lot::Mutex;
use prost::Message;
use std::collections::HashSet;
use std::sync::{Arc, OnceLock, mpsc};
use std::time::Duration;
use tracing::{field, info, info_span};
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::layer::SubscriberExt;

/// The message of the error that a [`FailingWriter`] returns.
const INJECTED_ERROR: &str = "injected write failure";

/// The payload of the panic of a writer that panics.
const INJECTED_PANIC: &str = "injected writer panic";

/// A sink that fails the first message its predicate selects and writes every other message in
/// full.
///
/// The failed message's first `fragment_len` bytes reach the output before the error or panic,
/// as when `write_all` stops partway through a message.
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
    failure: Failure,
    /// The offset in `buf` at which the failed message starts, once a message has failed.
    failure_offset: Option<usize>,
}

/// How a [`FailingWriter`] fails the message it selects, once the fragment of it is written.
enum Failure {
    /// `write_all` returns [`INJECTED_ERROR`].
    Error,
    /// The writer panics with [`INJECTED_PANIC`].
    ///
    /// Before it panics, it finishes the trace from inside the write if `finish` holds a handle.
    Panic { finish: Arc<OnceLock<FinishHandle>> },
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
        Self::failing_with(Failure::Error, fragment_len, fails)
    }

    /// Returns a writer that fails the first message that `fails` selects as `failure` says,
    /// after writing `fragment_len` bytes of it.
    fn failing_with(
        failure: Failure,
        fragment_len: usize,
        fails: impl Fn(&idl::Trace) -> bool + Send + 'static,
    ) -> Self {
        Self {
            state: Arc::new(Mutex::new(FailingWriterState {
                buf: Vec::new(),
                fails: Box::new(fails),
                fragment_len,
                failure,
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
        if let Failure::Panic { finish } = &state.failure {
            let finish = finish.get().cloned();
            drop(state);
            if let Some(finish) = finish {
                // From inside the write, the finish returns `Ok` at once and leaves closing the
                // gate to the write that called it.
                finish.finish().ok();
            }
            std::panic::panic_any(INJECTED_PANIC);
        }
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

/// Records two rounds of [`record_round`].
///
/// The second round writes every kind of message again after a failure in the first.
fn record_rounds(layer: PerfettoLayer<FailingWriter>) {
    let subscriber = tracing_subscriber::registry().with(layer);
    tracing::subscriber::with_default(subscriber, || {
        for _ in 0..2 {
            record_round();
        }
    });
}

/// Records a parent span with a child nested on its track, each entered on the calling thread,
/// with an instant inside the child and one outside any span.
fn record_round() {
    let parent = info_span!("parent", closes = field::Empty);
    let child = info_span!(parent: &parent, "child", closes = field::Empty);
    parent.in_scope(|| child.in_scope(|| info!("inside")));
    info!("outside");
    close_labeled(child, "child");
    close_labeled(parent, "parent");
}

/// Records a [`record_round`] whose writer panics, then a second round after the panic.
///
/// The panic reaches the code that recorded the span or event whose write panicked, which
/// catches it, as an application that survives a panic would.
fn record_rounds_across_a_panic(layer: PerfettoLayer<FailingWriter>) -> Result<()> {
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(layer));
    let panicked =
        tracing::dispatcher::with_default(&dispatch, || std::panic::catch_unwind(record_round));
    expect_injected_panic(panicked)?;
    tracing::dispatcher::with_default(&dispatch, record_round);
    Ok(())
}

/// Checks that `result` is a panic with [`INJECTED_PANIC`].
fn expect_injected_panic<T>(result: std::thread::Result<T>) -> Result<()> {
    let Err(payload) = result else {
        return fail!("the injected panic did not reach the caller");
    };
    verify_that!(payload.downcast_ref::<&str>(), some(eq(&INJECTED_PANIC)))
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

/// Checks that finishing returns the error that stands for the writer's panic, and that only the
/// first finish does.
///
/// A finish that called the writer again could panic too, so a panic counts as a failure here.
fn expect_finish_reports_the_panic(finish: &FinishHandle) -> Result<()> {
    let finished = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| finish.finish()));
    verify_that!(
        finished,
        ok(err(displays_as(eq(crate::sink::WRITER_PANICKED))))
    )?;
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

/// A writer that panics partway through a message leaves the messages written before it,
/// followed only by the fragment of the panicking one, and finishing reports the panic.
///
/// A panic unwinds through the layer's write, so a layer that left the trace open would append
/// the next message to the fragment, which loses the framing of every later message.
#[gtest]
fn a_writer_panic_mid_message_leaves_whole_messages_and_nothing_after_the_fragment() -> Result<()> {
    const FRAGMENT_LEN: usize = 3;
    let writer = FailingWriter::failing_with(
        Failure::Panic {
            finish: Arc::default(),
        },
        FRAGMENT_LEN,
        holds_an_instant,
    );
    let output = writer.output();
    let layer = failing_layer(writer);
    let finish = layer.finish_handle();

    record_rounds_across_a_panic(layer)?;

    let written = expect_nothing_after_failure(&output, FRAGMENT_LEN)?;
    let trace = idl::Trace::decode(written.as_slice()).or_fail()?;
    verify_that!(trace.packet, not(is_empty()))?;
    expect_readable(&trace)?;
    expect_finish_reports_the_panic(&finish)
}

/// A writer that finishes the trace from inside a write and then panics in that write ends the
/// trace there, and a later finish reports the panic.
///
/// The finish from inside the write leaves closing the trace to the write that called it, so a
/// write that unwound without closing it would let every later span and event land after the
/// fragment.
#[gtest]
fn a_writer_panic_after_finishing_from_inside_the_write_ends_the_trace() -> Result<()> {
    const FRAGMENT_LEN: usize = 3;
    let in_writer_finish = Arc::new(OnceLock::new());
    let writer = FailingWriter::failing_with(
        Failure::Panic {
            finish: Arc::clone(&in_writer_finish),
        },
        FRAGMENT_LEN,
        holds_an_instant,
    );
    let output = writer.output();
    let layer = failing_layer(writer);
    in_writer_finish.set(layer.finish_handle()).or_fail()?;
    let finish = layer.finish_handle();

    record_rounds_across_a_panic(layer)?;

    let written = expect_nothing_after_failure(&output, FRAGMENT_LEN)?;
    expect_readable(&idl::Trace::decode(written.as_slice()).or_fail()?)?;
    expect_finish_reports_the_panic(&finish)
}

/// A writer that writes whole messages and panics when it is flushed.
struct PanickingFlushWriter {
    buf: Arc<Mutex<Vec<u8>>>,
}

impl<'a> MakeWriter<'a> for PanickingFlushWriter {
    type Writer = PanickingFlushWriter;

    fn make_writer(&'a self) -> Self::Writer {
        PanickingFlushWriter {
            buf: Arc::clone(&self.buf),
        }
    }
}

impl std::io::Write for PanickingFlushWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buf.lock().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        std::panic::panic_any(INJECTED_PANIC)
    }
}

/// A flush that panics reaches the finish that flushes, and the next finish reports the panic.
///
/// The panic unwinds through the finish, so the error that it leaves behind is the only report
/// of the failure that a caller who survives the panic gets.
#[gtest]
fn a_panicking_flush_is_reported_by_the_next_finish() -> Result<()> {
    let buf = Arc::new(Mutex::new(Vec::new()));
    let layer = PerfettoLayer::new(PanickingFlushWriter {
        buf: Arc::clone(&buf),
    });
    let finish = layer.finish_handle();
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(layer));
    tracing::dispatcher::with_default(&dispatch, || info!("before_finish"));

    expect_injected_panic(std::panic::catch_unwind(std::panic::AssertUnwindSafe(
        || finish.finish(),
    )))?;

    expect_finish_reports_the_panic(&finish)?;
    let trace = idl::Trace::decode(buf.lock().as_slice()).or_fail()?;
    expect_readable(&trace)?;
    expect_eq!(trace.packet.iter().filter_map(track_event).count(), 1);
    drop(dispatch);
    Ok(())
}

/// A writer that panics in every write.
struct PanickingWrite;

impl std::io::Write for PanickingWrite {
    fn write(&mut self, _buf: &[u8]) -> std::io::Result<usize> {
        std::panic::panic_any(INJECTED_PANIC)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// A panic in a `std::sync::Mutex` writer, which poisons the mutex, is reported by the next
/// finish, and neither that finish nor dropping the layer panics.
///
/// tracing-subscriber's writer for a `Mutex` panics when it finds the mutex poisoned, so a layer
/// that called the writer again after its panic, even only to flush it, would panic in every
/// finish, and abort the process when the layer is dropped while another panic unwinds.
#[gtest]
fn a_panic_that_poisons_a_mutex_writer_is_reported_without_calling_it_again() -> Result<()> {
    let layer = PerfettoLayer::new(std::sync::Mutex::new(PanickingWrite));
    let finish = layer.finish_handle();
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(layer));

    let panicked = tracing::dispatcher::with_default(&dispatch, || {
        std::panic::catch_unwind(|| info!("panics"))
    });
    expect_injected_panic(panicked)?;
    let recorded_after = tracing::dispatcher::with_default(&dispatch, || {
        std::panic::catch_unwind(|| info!("after_the_panic"))
    });
    verify_that!(recorded_after, ok(anything()))?;

    expect_finish_reports_the_panic(&finish)?;
    verify_that!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(dispatch))),
        ok(anything())
    )
}

/// A panic of the code that records spans, not of the writer, leaves the trace open: the spans
/// that the panic ends as it unwinds are written, and so is everything recorded after it.
///
/// Those writes return while a panic unwinds, so a layer that took a write during a panic for a
/// panic of the writer would end the trace at the first of them.
#[gtest]
fn a_panic_outside_the_writer_ends_unwound_spans_and_keeps_the_trace_open() -> Result<()> {
    let (layer, writer) = annotated_layer();
    let finish = layer.finish_handle();
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(layer));

    let panicked = tracing::dispatcher::with_default(&dispatch, || {
        std::panic::catch_unwind(|| {
            let _unwound = info_span!("unwound").entered();
            std::panic::panic_any(INJECTED_PANIC);
        })
    });
    expect_injected_panic(panicked)?;
    tracing::dispatcher::with_default(&dispatch, || info!(marker = "after_the_panic"));

    verify_that!(finish.finish(), ok(anything()))?;
    let trace = decode(&writer)?;
    // The span's slice on the thread's track and its lifetime slice both ended.
    let unwound_ends: Vec<_> = replay_slices(&trace)?
        .into_iter()
        .filter(|slice| slice.name == "unwound")
        .map(|slice| slice.end_timestamp)
        .collect();
    expect_that!(
        unwound_ends,
        elements_are![some(anything()), some(anything())]
    );
    instant_marked(&trace, "after_the_panic")?;
    drop(dispatch);
    Ok(())
}
