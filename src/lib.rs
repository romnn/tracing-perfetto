//! A [`tracing_subscriber::Layer`] that writes spans and events as a [Perfetto] trace.
//!
//! [`PerfettoLayer`] records each span as a lifetime slice from its creation to its close, each
//! entry of a span as an entered slice on the entering thread's track, and each event as an
//! instant.
//! It streams them as encoded `Trace` protobuf packets to a [`PerfettoWriter`] as they happen, so
//! a slice's begin is in the trace while the span is still open.
//! Open the output in the [Perfetto UI].
//!
//! # Tracks
//!
//! Every root span gets its own track below the process track, named after the span, which holds
//! its lifetime slice.
//! A child span nests on its parent's track, unless another child of that parent is still open
//! there.
//! A child that overlaps such a sibling gets a sub-track of its parent's track, named after the
//! child.
//! An event inside a span is an instant on that span's track, and any other event is an instant
//! on the track of the thread that records it.
//!
//! Each thread's track shows where spans actually ran: every entry of a span is a slice from
//! the enter to the exit, so each poll of an instrumented future is one slice on the worker that
//! polled it.
//! A span's entered slices and its lifetime slice share a `correlation_id` of the span's own, which
//! links them across threads.
//!
//! The Perfetto UI merges tracks of the same name into one visual track, with a lane for each
//! span that is open at the same time.
//! Fifty concurrent tasks instrumented as `handle` show as one `handle` track with up to fifty
//! lanes, each task's sequential children nested beneath its slice, and children that run
//! concurrently, as in `join!`, on sub-tracks.
//!
//! Span fields, read when the span is created, control where its slice appears:
//!
//! - `perfetto.track_name` gives the span its own track below the process track, under this
//!   name.
//! - `perfetto.slice_name` overrides the slice name, which otherwise falls back to `otel.name` and
//!   then to the span's name.
//!
//! Recording either field later leaves the slice where it is.
//!
//! [`PerfettoLayer::with_thread_slices`] turns entered slices off.
//! [`PerfettoLayer::with_span_lifetimes`] turns lifetime slices off, which leaves a synchronous
//! program a plain flame chart per thread.
//!
//! [`PerfettoLayer::with_process_memory_rss`] adds a counter track that samples the process's
//! resident set size.
//! A [`FinishHandle`] ends the trace while the layer is still installed, for example in a global
//! dispatcher that is never dropped.
//!
//! # Examples
//!
//! ```
//! use tracing_perfetto::PerfettoLayer;
//! use tracing_subscriber::layer::SubscriberExt;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let file = std::fs::File::create(std::env::temp_dir().join("example.pftrace"))?;
//! let layer = PerfettoLayer::new(std::sync::Mutex::new(file))
//!     .with_debug_annotations(true)
//!     .with_process_memory_rss(std::time::Duration::from_millis(100));
//! let finish = layer.finish_handle();
//!
//! tracing::subscriber::with_default(tracing_subscriber::registry().with(layer), || {
//!     let _span = tracing::info_span!("work", perfetto.track_name = "jobs").entered();
//!     tracing::info!("working");
//! });
//! finish.finish();
//! # Ok(())
//! # }
//! ```
//!
//! [Perfetto]: https://perfetto.dev
//! [Perfetto UI]: https://ui.perfetto.dev

#[allow(
    clippy::all,
    clippy::pedantic,
    clippy::allow_attributes_without_reason,
    unreachable_pub,
    missing_docs,
    unused,
    reason = "generated protobuf sources are not written to this workspace's lint policy"
)]
#[rustfmt::skip]
mod idl {
    include!("generated/perfetto.protos.rs");
}

mod decode;
mod fields;
mod layer;
mod lifecycle;
mod packets;
pub mod process_memory;
mod sequence;
mod span_state;
mod thread_slices;
mod thread_track;
mod writer;

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod thread_slice_tests;
#[cfg(test)]
mod track_tests;

pub use decode::{DecodeError, decode_track_event_names};
pub use layer::PerfettoLayer;
pub use lifecycle::FinishHandle;
pub use writer::PerfettoWriter;

#[cfg(test)]
mod tests {
    use crate::PerfettoLayer;
    use crate::idl;
    use crate::idl::track_event;
    use crate::sequence::SEQUENCE_CLOCK_ID;
    use crate::test_support::TestWriter;
    use googletest::prelude::*;
    use prost::Message;
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc;
    use tracing::{field, trace_span};
    use tracing_subscriber::{fmt::MakeWriter, layer::SubscriberExt};

    // Check that we are able to write a span and confirm that it's written as protobuf data to the
    // output
    #[gtest]
    fn test_simple_span() -> Result<()> {
        let writer = TestWriter::new();
        let extra_writer = writer.make_writer();
        let perfetto_layer = PerfettoLayer::new(writer).with_debug_annotations(true);
        let subscriber = tracing_subscriber::registry().with(perfetto_layer);
        let _guard = tracing::subscriber::set_default(subscriber);
        {
            let demo_span = trace_span!("simple_span",);
            let _enter = demo_span.enter();
        }
        let buf = extra_writer.buf.lock();
        expect_that!(buf.as_slice(), not(is_empty()));
        let trace = idl::Trace::decode(buf.as_slice()).or_fail()?;

        let mut track_events_seen = 0;
        let mut saw_slice_begin = false;
        let mut saw_slice_end = false;
        // Depending on test ordering, we may or may not see a process descriptor
        for packet in trace.packet {
            let Some(idl::trace_packet::Data::TrackEvent(ref event)) = packet.data else {
                continue;
            };
            track_events_seen += 1;

            // Each slice takes its name from the begin, so the end carries none.
            match event.r#type() {
                track_event::Type::SliceBegin => {
                    saw_slice_begin = true;
                    let expected = Some(track_event::NameField::Name(String::from("simple_span")));
                    expect_eq!(event.name_field, expected);
                }
                track_event::Type::SliceEnd => {
                    saw_slice_end = true;
                    expect_eq!(event.name_field, None);
                }
                other => return fail!("unexpected track event type: {other:?}"),
            }
        }
        // The lifetime slice and the entered slice each begin and end.
        expect_eq!(track_events_seen, 4);
        expect_true!(saw_slice_begin);
        expect_true!(saw_slice_end);
        Ok(())
    }

    // Check that we are able to write arguments to a span correctly
    #[gtest]
    fn test_span_arguments() -> Result<()> {
        let writer = TestWriter::new();
        let extra_writer = writer.make_writer();
        // Entered slices carry no fields while span lifetimes are on, so only the lifetime
        // slice is recorded.
        let perfetto_layer = PerfettoLayer::new(writer)
            .with_debug_annotations(true)
            .with_thread_slices(false)
            .with_filter_by_marker(|s| s == "regular_arg");

        let subscriber = tracing_subscriber::registry().with(perfetto_layer);
        let _guard = tracing::subscriber::set_default(subscriber);
        {
            let demo_span = trace_span!(
                "span_with_args",
                regular_arg = "Arg data",
                extra_arg = field::Empty
            );
            let _enter = demo_span.enter();
            demo_span.record("extra_arg", "Some Extra Data");
        }
        let buf = extra_writer.buf.lock();
        expect_that!(buf.as_slice(), not(is_empty()));
        let trace = idl::Trace::decode(buf.as_slice()).or_fail()?;

        let mut track_events_seen = 0;
        let mut saw_slice_begin = false;
        let mut saw_slice_end = false;
        // Depending on test ordering, we may or may not see a process descriptor
        for packet in trace.packet {
            let Some(idl::trace_packet::Data::TrackEvent(ref event)) = packet.data else {
                continue;
            };
            track_events_seen += 1;

            match event.r#type() {
                track_event::Type::SliceBegin => {
                    saw_slice_begin = true;
                    let expected =
                        Some(track_event::NameField::Name(String::from("span_with_args")));
                    expect_eq!(event.name_field, expected);

                    // The begin is written when the span is created, so it carries only the
                    // fields given at creation.
                    let annotations = event.debug_annotations.as_slice();
                    let [first] = annotations else {
                        return fail!("expected 1 debug annotation, got {}", annotations.len());
                    };
                    expect_eq!(
                        first.name_field,
                        Some(idl::debug_annotation::NameField::Name(
                            "regular_arg".to_string(),
                        ))
                    );
                    expect_eq!(
                        first.value,
                        Some(idl::debug_annotation::Value::StringValue(
                            "Arg data".to_string(),
                        ))
                    );
                }
                track_event::Type::SliceEnd => {
                    saw_slice_end = true;

                    // The end carries the field recorded later, which trace processor merges
                    // into the slice's arguments.
                    let annotations = event.debug_annotations.as_slice();
                    let [recorded] = annotations else {
                        return fail!("expected 1 debug annotation, got {}", annotations.len());
                    };
                    expect_eq!(
                        recorded.name_field,
                        Some(idl::debug_annotation::NameField::Name(
                            "extra_arg".to_string(),
                        ))
                    );
                    expect_eq!(
                        recorded.value,
                        Some(idl::debug_annotation::Value::StringValue(
                            "Some Extra Data".to_string(),
                        ))
                    );
                }
                other => return fail!("unexpected track event type: {other:?}"),
            }
        }
        expect_eq!(track_events_seen, 2);
        expect_true!(saw_slice_begin);
        expect_true!(saw_slice_end);
        Ok(())
    }

    #[gtest]
    fn test_span_slice_name_override_prefers_perfetto_track_name() -> Result<()> {
        let writer = TestWriter::new();
        let extra_writer = writer.make_writer();
        let perfetto_layer = PerfettoLayer::new(writer).with_debug_annotations(true);
        let subscriber = tracing_subscriber::registry().with(perfetto_layer);
        let _guard = tracing::subscriber::set_default(subscriber);

        {
            let otel_name = String::from("otel-name");
            let demo_span = trace_span!(
                "static_span",
                {"perfetto.track_name"} = "my-task",
                {"otel.name"} = %otel_name
            );
            let _enter = demo_span.enter();
        }

        let buf = extra_writer.buf.lock();
        expect_that!(buf.as_slice(), not(is_empty()));
        let trace = idl::Trace::decode(buf.as_slice()).or_fail()?;

        // Only the begins of the lifetime slice and the entered slice name them.
        let names = crate::decode_track_event_names(&trace.encode_to_vec()).or_fail()?;
        expect_eq!(names, ["otel-name", "otel-name"]);
        Ok(())
    }

    #[gtest]
    fn test_span_slice_name_override_uses_otel_name_when_no_track_name() -> Result<()> {
        let writer = TestWriter::new();
        let extra_writer = writer.make_writer();
        let perfetto_layer = PerfettoLayer::new(writer).with_debug_annotations(true);
        let subscriber = tracing_subscriber::registry().with(perfetto_layer);
        let _guard = tracing::subscriber::set_default(subscriber);

        {
            let otel_name = String::from("my-otel-span");
            let demo_span = trace_span!("static_span", {"otel.name"} = %otel_name);
            let _enter = demo_span.enter();
        }

        let buf = extra_writer.buf.lock();
        expect_that!(buf.as_slice(), not(is_empty()));
        let trace = idl::Trace::decode(buf.as_slice()).or_fail()?;

        // Only the begins of the lifetime slice and the entered slice name them.
        let names = crate::decode_track_event_names(&trace.encode_to_vec()).or_fail()?;
        expect_eq!(names, ["my-otel-span", "my-otel-span"]);
        Ok(())
    }

    // If all our spans are filtered, we shouldn't get any trace data at all.
    // Doing a `.record` on a span should also "fail successfully".
    #[gtest]
    fn test_span_arguments_filtered() {
        let writer = TestWriter::new();
        let extra_writer = writer.make_writer();
        let perfetto_layer = PerfettoLayer::new(writer)
            .with_debug_annotations(true)
            .with_filter_by_marker(|s| s == "NO SUCH ARG");

        let subscriber = tracing_subscriber::registry().with(perfetto_layer);
        let _guard = tracing::subscriber::set_default(subscriber);
        {
            let demo_span = trace_span!(
                "span_with_args",
                regular_arg = "Arg data",
                extra_arg = field::Empty
            );
            let _enter = demo_span.enter();
            demo_span.record("extra_arg", "Some Extra Data");
        }
        let buf = extra_writer.buf.lock();
        expect_eq!(buf.len(), 0);
    }

    struct FixedProcessMemory;

    impl crate::process_memory::ProcessMemorySource for FixedProcessMemory {
        fn resident_set_size_bytes(&self) -> Option<u64> {
            Some(1024)
        }
    }

    /// Finishing through the handle ends the trace while the layer stays installed: the sampler
    /// thread is joined and later spans are not written.
    #[gtest]
    fn finish_handle_ends_the_trace_of_a_live_layer() {
        let writer = TestWriter::new();
        let extra_writer = writer.make_writer();
        let perfetto_layer = PerfettoLayer::new(writer).with_process_memory_source(
            std::time::Duration::from_millis(1),
            Box::new(FixedProcessMemory),
        );
        let finish = perfetto_layer.finish_handle();
        let layer_writer = Arc::clone(perfetto_layer.writer());
        let subscriber = tracing_subscriber::registry().with(perfetto_layer);
        let _guard = tracing::subscriber::set_default(subscriber);

        trace_span!("before_finish").in_scope(|| {});
        finish.finish();
        let written = extra_writer.buf.lock().len();
        trace_span!("after_finish").in_scope(|| {});

        // The joined sampler released its writer, leaving this test's and the layer's references.
        expect_eq!(Arc::strong_count(&layer_writer), 2);
        // The span recorded before finishing reached the writer; the later one did not.
        expect_that!(
            written,
            gt(0),
            "expected the span before finishing to be written"
        );
        expect_eq!(extra_writer.buf.lock().len(), written);
    }

    /// A memory source that reports samples once armed and announces each sample it reports.
    ///
    /// The sampler writes a sample before it reads the next one, so the second announcement
    /// proves that the first sample reached the writer.
    struct AnnouncingProcessMemory {
        armed: Arc<AtomicBool>,
        announcements: mpsc::Sender<()>,
    }

    impl crate::process_memory::ProcessMemorySource for AnnouncingProcessMemory {
        fn resident_set_size_bytes(&self) -> Option<u64> {
            if !self.armed.load(Ordering::Acquire) {
                return None;
            }
            // The receiver is gone only after the test stopped waiting for samples.
            self.announcements.send(()).ok();
            Some(1024)
        }
    }

    /// Blocks until the sampler has written at least one sample.
    fn wait_for_written_sample(announcements: &mpsc::Receiver<()>) -> Result<()> {
        announcements.recv().or_fail()?;
        announcements.recv().or_fail()
    }

    fn is_process_descriptor(packet: &idl::TracePacket) -> bool {
        matches!(
            &packet.data,
            Some(idl::trace_packet::Data::TrackDescriptor(descriptor))
                if descriptor.process.is_some()
        )
    }

    fn is_clock_snapshot(packet: &idl::TracePacket) -> bool {
        matches!(packet.data, Some(idl::trace_packet::Data::ClockSnapshot(_)))
    }

    fn is_counter_event(packet: &idl::TracePacket) -> bool {
        matches!(
            &packet.data,
            Some(idl::trace_packet::Data::TrackEvent(event))
                if event.r#type() == track_event::Type::Counter
        )
    }

    fn sequence_id(packet: &idl::TracePacket) -> Option<u32> {
        let idl::trace_packet::OptionalTrustedPacketSequenceId::TrustedPacketSequenceId(id) =
            packet.optional_trusted_packet_sequence_id?;
        Some(id)
    }

    /// Checks that `trace` opens with its preamble and never repeats it.
    ///
    /// The preamble is the process track descriptor followed by the clock snapshot that maps the
    /// sequence clock onto the wall clock.
    fn expect_preamble_once(trace: &idl::Trace) -> Result<()> {
        let [descriptor, snapshot, ..] = trace.packet.as_slice() else {
            return fail!("expected a preamble, got {} packets", trace.packet.len());
        };
        expect_true!(is_process_descriptor(descriptor));
        let Some(idl::trace_packet::Data::ClockSnapshot(snapshot_data)) = &snapshot.data else {
            return fail!("expected a clock snapshot after the process descriptor");
        };

        // The snapshot pairs the sequence clock with the wall clock, which is the trace clock.
        let realtime_clock_id = u32::try_from(i32::from(idl::BuiltinClock::Realtime)).or_fail()?;
        let clock_ids: Vec<_> = snapshot_data
            .clocks
            .iter()
            .map(|clock| clock.clock_id)
            .collect();
        expect_eq!(
            clock_ids,
            vec![Some(SEQUENCE_CLOCK_ID), Some(realtime_clock_id)]
        );
        expect_eq!(
            snapshot_data.primary_trace_clock,
            Some(i32::from(idl::BuiltinClock::Realtime))
        );
        // The user-defined clock is scoped to the snapshot's sequence.
        expect_true!(sequence_id(snapshot).is_some());

        // Later writes reuse the preamble instead of repeating it.
        let process_descriptors = trace
            .packet
            .iter()
            .filter(|packet| is_process_descriptor(packet))
            .count();
        let clock_snapshots = trace
            .packet
            .iter()
            .filter(|packet| is_clock_snapshot(packet))
            .count();
        expect_eq!(process_descriptors, 1);
        expect_eq!(clock_snapshots, 1);
        Ok(())
    }

    /// Checks that every timestamped packet reads the sequence clock that the preamble defines.
    fn expect_sequence_clock(trace: &idl::Trace) {
        let clock_ids: Vec<_> = trace
            .packet
            .iter()
            .filter(|packet| packet.timestamp.is_some())
            .map(|packet| packet.timestamp_clock_id)
            .collect();
        expect_that!(clock_ids, not(is_empty()));
        expect_that!(clock_ids, each(eq(&Some(SEQUENCE_CLOCK_ID))));
    }

    /// Checks that every track an event or descriptor refers to is described in `trace` itself.
    fn expect_self_contained(trace: &idl::Trace) {
        let mut described = HashSet::new();
        let mut referenced = HashSet::new();
        for packet in &trace.packet {
            match &packet.data {
                Some(idl::trace_packet::Data::TrackDescriptor(descriptor)) => {
                    described.insert(descriptor.uuid());
                    referenced.extend(descriptor.parent_uuid);
                }
                Some(idl::trace_packet::Data::TrackEvent(event)) => {
                    referenced.extend(event.track_uuid);
                }
                _ => {}
            }
        }
        expect_that!(referenced, not(is_empty()));
        let undescribed: Vec<_> = referenced.difference(&described).collect();
        expect_that!(undescribed, is_empty());
    }

    /// Returns the calling thread's kernel ID from the first field of its procfs `stat` file.
    ///
    /// This source is independent of the `/proc/thread-self` link that the layer reads.
    #[cfg(target_os = "linux")]
    fn procfs_kernel_thread_id() -> Result<i32> {
        let stat = std::fs::read_to_string("/proc/thread-self/stat").or_fail()?;
        stat.split_whitespace().next().or_fail()?.parse().or_fail()
    }

    /// Runs `record` under `dispatch` and returns the calling thread's kernel ID.
    ///
    /// The ID is `None` on platforms where the test has no source for the kernel's thread ID.
    fn record_on_current_thread(
        dispatch: &tracing::Dispatch,
        record: impl FnOnce(),
    ) -> Result<Option<i32>> {
        tracing::dispatcher::with_default(dispatch, record);
        #[cfg(target_os = "linux")]
        let kernel_thread_id = Some(procfs_kernel_thread_id()?);
        // `thread_id::get` is `pthread_threadid_np` on macOS and `GetCurrentThreadId` on Windows.
        #[cfg(any(target_os = "macos", target_os = "windows"))]
        let kernel_thread_id = Some(i32::try_from(thread_id::get()).or_fail()?);
        #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
        let kernel_thread_id = None::<i32>;
        Ok(kernel_thread_id)
    }

    fn new_announcing_layer(
        writer: TestWriter,
        armed: bool,
    ) -> (
        PerfettoLayer<TestWriter>,
        Arc<AtomicBool>,
        mpsc::Receiver<()>,
    ) {
        let armed = Arc::new(AtomicBool::new(armed));
        let (announcements_sender, announcements) = mpsc::channel();
        let layer = PerfettoLayer::new(writer).with_process_memory_source(
            std::time::Duration::ZERO,
            Box::new(AnnouncingProcessMemory {
                armed: Arc::clone(&armed),
                announcements: announcements_sender,
            }),
        );
        (layer, armed, announcements)
    }

    /// Thread descriptors carry each thread's own OS thread ID.
    ///
    /// On Linux, `thread_id::get` is a `pthread_t` address, which clamped to `i32` gives every
    /// thread the same ID and merges all thread tracks of the process into one.
    #[gtest]
    fn thread_descriptors_carry_the_os_thread_id_of_each_thread() -> Result<()> {
        let writer = TestWriter::new();
        let extra_writer = writer.make_writer();
        let dispatch =
            tracing::Dispatch::new(tracing_subscriber::registry().with(PerfettoLayer::new(writer)));

        // Record one unscoped event on the test thread, then one on a worker thread.
        let main_kernel_thread_id = record_on_current_thread(&dispatch, || {
            tracing::info!("on the main thread");
        })?;
        let worker_kernel_thread_id = std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    record_on_current_thread(&dispatch, || {
                        tracing::info!("on the worker thread");
                    })
                })
                .join()
        })
        .or_fail()??;

        // Map each event, in recording order, to the thread ID of the track it is on.
        let trace = idl::Trace::decode(extra_writer.buf.lock().as_slice()).or_fail()?;
        let thread_ids: HashMap<u64, Option<i32>> = trace
            .packet
            .iter()
            .filter_map(|packet| match &packet.data {
                Some(idl::trace_packet::Data::TrackDescriptor(descriptor)) => descriptor
                    .thread
                    .as_ref()
                    .map(|thread| (descriptor.uuid(), thread.tid)),
                _ => None,
            })
            .collect();
        let event_thread_ids: Vec<Option<i32>> = trace
            .packet
            .iter()
            .filter_map(|packet| match &packet.data {
                Some(idl::trace_packet::Data::TrackEvent(event)) => {
                    Some(thread_ids.get(&event.track_uuid()).copied().flatten())
                }
                _ => None,
            })
            .collect();
        let [Some(main_thread_id), Some(worker_thread_id)] = event_thread_ids[..] else {
            return fail!("expected two events on thread tracks, got {event_thread_ids:?}");
        };

        // Each thread track names its own, valid thread.
        expect_ne!(main_thread_id, worker_thread_id);
        expect_that!(main_thread_id, gt(0));
        expect_that!(worker_thread_id, gt(0));

        // Where the kernel's ID is known, the descriptors carry exactly that ID, so the tracks
        // line up with other tools' view of the threads.
        if let (Some(main_kernel_thread_id), Some(worker_kernel_thread_id)) =
            (main_kernel_thread_id, worker_kernel_thread_id)
        {
            expect_eq!(main_thread_id, main_kernel_thread_id);
            expect_eq!(worker_thread_id, worker_kernel_thread_id);
        }
        Ok(())
    }

    /// Spans, unscoped events and memory samples of one layer all share one valid sequence ID.
    ///
    /// A random `u64` clamped to `u32` is `u32::MAX` for all but a 2^-32 share of layers, so
    /// every layer would share that one sequence and its sequence-scoped clock.
    #[gtest]
    fn packets_of_one_layer_share_one_valid_sequence_id() -> Result<()> {
        let writer = TestWriter::new();
        let extra_writer = writer.make_writer();
        let (perfetto_layer, _, announcements) = new_announcing_layer(writer, true);
        let finish = perfetto_layer.finish_handle();
        let subscriber = tracing_subscriber::registry().with(perfetto_layer);
        let _guard = tracing::subscriber::set_default(subscriber);

        trace_span!("span").in_scope(|| tracing::info!("inside the span"));
        tracing::info!("outside any span");
        wait_for_written_sample(&announcements)?;
        finish.finish();

        let trace = idl::Trace::decode(extra_writer.buf.lock().as_slice()).or_fail()?;

        // Every timestamped packet, the memory samples included, names a sequence.
        let timestamped_sequence_ids: Vec<_> = trace
            .packet
            .iter()
            .filter(|packet| packet.timestamp.is_some())
            .map(sequence_id)
            .collect();
        expect_that!(timestamped_sequence_ids, each(some(anything())));
        expect_true!(trace.packet.iter().any(is_counter_event));

        // All packets name the same sequence, which is not invalid, not the tracing service's
        // own, and not a clamped value.
        let sequence_ids: HashSet<u32> = trace.packet.iter().filter_map(sequence_id).collect();
        let [only_sequence_id] = sequence_ids.iter().copied().collect::<Vec<_>>()[..] else {
            return fail!("expected one sequence ID, got {sequence_ids:?}");
        };
        expect_that!(only_sequence_id, all!(gt(1), lt(u32::MAX)));
        Ok(())
    }

    /// Two layers on one registry each record the same spans into their own complete trace.
    ///
    /// Span extensions hold one value per type, so a layer that stored its state as its own
    /// extension type would panic when the second layer inserted the same type.
    #[gtest]
    fn two_layers_on_one_registry_each_write_a_complete_trace() -> Result<()> {
        let first_writer = TestWriter::new();
        let first_buf = first_writer.make_writer();
        let second_writer = TestWriter::new();
        let second_buf = second_writer.make_writer();
        let subscriber = tracing_subscriber::registry()
            .with(PerfettoLayer::new(first_writer).with_debug_annotations(true))
            .with(PerfettoLayer::new(second_writer));
        let _guard = tracing::subscriber::set_default(subscriber);

        // A named parent makes the child read its track from the parent's per-layer state, and
        // the recorded field updates only the recording layer's state.
        trace_span!(
            "parent",
            { "perfetto.track_name" } = "jobs",
            value = field::Empty
        )
        .in_scope(|| {
            tracing::Span::current().record("value", 1);
            trace_span!("child").in_scope(|| tracing::info!("inside the child"));
        });

        let first = idl::Trace::decode(first_buf.buf.lock().as_slice()).or_fail()?;
        let second = idl::Trace::decode(second_buf.buf.lock().as_slice()).or_fail()?;
        for trace in [&first, &second] {
            expect_preamble_once(trace)?;
            expect_self_contained(trace);

            // Both spans begin a lifetime and an entered slice, and the event inside the child is
            // recorded once.
            let names = crate::decode_track_event_names(&trace.encode_to_vec()).or_fail()?;
            let (event_names, mut span_names): (Vec<_>, Vec<_>) = names
                .into_iter()
                .partition(|name| name.starts_with("event "));
            span_names.sort();
            expect_eq!(span_names, ["child", "child", "parent", "parent"]);
            expect_eq!(event_names.len(), 1);
        }

        // Each layer writes on its own sequence.
        let first_ids: HashSet<u32> = first.packet.iter().filter_map(sequence_id).collect();
        let second_ids: HashSet<u32> = second.packet.iter().filter_map(sequence_id).collect();
        expect_that!(first_ids.intersection(&second_ids).count(), eq(0));
        Ok(())
    }

    /// Every timestamped packet reads the sequence clock that one earlier snapshot maps onto the
    /// wall clock.
    ///
    /// A packet without a clock ID is read as boot time, so elapsed nanoseconds since the layer's
    /// epoch would land decades before the snapshot's wall-clock time.
    #[gtest]
    fn timestamped_packets_read_the_clock_of_one_earlier_snapshot() -> Result<()> {
        let writer = TestWriter::new();
        let extra_writer = writer.make_writer();
        let (perfetto_layer, _, announcements) = new_announcing_layer(writer, true);
        let finish = perfetto_layer.finish_handle();
        let subscriber = tracing_subscriber::registry().with(perfetto_layer);
        let _guard = tracing::subscriber::set_default(subscriber);

        // Cover every write path: span begin and end, span event, unscoped event and memory
        // sample.
        trace_span!("span").in_scope(|| tracing::info!("inside the span"));
        tracing::info!("outside any span");
        wait_for_written_sample(&announcements)?;
        finish.finish();

        let trace = idl::Trace::decode(extra_writer.buf.lock().as_slice()).or_fail()?;

        // Exactly one snapshot defines the clock, and it precedes every packet that reads it.
        let snapshot_positions: Vec<usize> = trace
            .packet
            .iter()
            .enumerate()
            .filter(|(_, packet)| is_clock_snapshot(packet))
            .map(|(position, _)| position)
            .collect();
        let [snapshot_position] = snapshot_positions[..] else {
            return fail!("expected one clock snapshot, got {snapshot_positions:?}");
        };
        let first_timestamped_position = trace
            .packet
            .iter()
            .position(|packet| packet.timestamp.is_some())
            .or_fail()?;
        expect_that!(snapshot_position, lt(first_timestamped_position));

        // The snapshot pairs the sequence clock with the wall clock on the packets' sequence.
        let snapshot = trace.packet.get(snapshot_position).or_fail()?;
        let Some(idl::trace_packet::Data::ClockSnapshot(snapshot_data)) = &snapshot.data else {
            return fail!("expected a clock snapshot at {snapshot_position}");
        };
        let realtime_clock_id = u32::try_from(i32::from(idl::BuiltinClock::Realtime)).or_fail()?;
        let clock_ids: Vec<_> = snapshot_data
            .clocks
            .iter()
            .map(|clock| clock.clock_id)
            .collect();
        expect_eq!(
            clock_ids,
            vec![Some(SEQUENCE_CLOCK_ID), Some(realtime_clock_id)]
        );
        expect_that!(
            snapshot_data
                .clocks
                .iter()
                .map(|clock| clock.timestamp)
                .collect::<Vec<_>>(),
            each(some(gt(&0)))
        );
        expect_eq!(
            snapshot_data.primary_trace_clock,
            Some(i32::from(idl::BuiltinClock::Realtime))
        );
        let first_timestamped = trace.packet.get(first_timestamped_position).or_fail()?;
        expect_eq!(sequence_id(snapshot), sequence_id(first_timestamped));

        // Every timestamped packet, span, event and sample alike, reads that clock.
        expect_sequence_clock(&trace);
        expect_true!(trace.packet.iter().any(is_counter_event));
        Ok(())
    }

    /// Sequentially recorded spans and events are written with non-decreasing timestamps.
    ///
    /// The timestamps come from a monotonic clock, so a wall-clock adjustment cannot reorder
    /// slices.
    #[gtest]
    fn sequential_packets_have_non_decreasing_timestamps() -> Result<()> {
        let writer = TestWriter::new();
        let extra_writer = writer.make_writer();
        let subscriber = tracing_subscriber::registry().with(PerfettoLayer::new(writer));
        let _guard = tracing::subscriber::set_default(subscriber);

        // Packets are written as they happen, so nested spans are written in time order too.
        for _ in 0..3 {
            trace_span!("span").in_scope(|| {
                tracing::info!("inside the span");
                trace_span!("child").in_scope(|| tracing::info!("inside the child"));
            });
            tracing::info!("outside any span");
        }

        let trace = idl::Trace::decode(extra_writer.buf.lock().as_slice()).or_fail()?;
        expect_sequence_clock(&trace);
        let timestamps: Vec<u64> = trace
            .packet
            .iter()
            .filter_map(|packet| packet.timestamp)
            .collect();
        let mut sorted_timestamps = timestamps.clone();
        sorted_timestamps.sort_unstable();
        // Each iteration writes two lifetime and two entered slices, each with a begin and an
        // end, and two instants, then another instant.
        expect_eq!(timestamps.len(), 33);
        expect_eq!(timestamps, sorted_timestamps);
        Ok(())
    }

    /// The process descriptor and clock snapshot open the trace and appear once across writes.
    ///
    /// Repeating the process descriptor on every write would duplicate it once per span and
    /// event.
    #[gtest]
    fn preamble_is_written_once_across_many_writes() -> Result<()> {
        let writer = TestWriter::new();
        let extra_writer = writer.make_writer();
        let subscriber = tracing_subscriber::registry().with(PerfettoLayer::new(writer));
        let _guard = tracing::subscriber::set_default(subscriber);

        // Each slice begin, slice end and unscoped event is a separate write.
        for _ in 0..3 {
            trace_span!("span").in_scope(|| {});
            tracing::info!("outside any span");
        }

        let trace = idl::Trace::decode(extra_writer.buf.lock().as_slice()).or_fail()?;
        expect_preamble_once(&trace)?;
        let slice_begins = trace
            .packet
            .iter()
            .filter(|packet| {
                matches!(
                    &packet.data,
                    Some(idl::trace_packet::Data::TrackEvent(event))
                        if event.r#type() == track_event::Type::SliceBegin
                )
            })
            .count();
        // Each span begins a lifetime and an entered slice.
        expect_eq!(slice_begins, 6);
        Ok(())
    }

    /// The preamble precedes everything when the memory sampler writes before any span closes.
    ///
    /// A sampler that wrote its samples without the preamble would put them ahead of the clock
    /// snapshot that defines their clock.
    #[gtest]
    fn preamble_precedes_a_memory_sample_written_first() -> Result<()> {
        let writer = TestWriter::new();
        let extra_writer = writer.make_writer();
        let (perfetto_layer, _, announcements) = new_announcing_layer(writer, true);
        let finish = perfetto_layer.finish_handle();
        let subscriber = tracing_subscriber::registry().with(perfetto_layer);
        let _guard = tracing::subscriber::set_default(subscriber);

        // The sampler writes first, then a span and an unscoped event follow.
        wait_for_written_sample(&announcements)?;
        trace_span!("span").in_scope(|| {});
        tracing::info!("outside any span");
        finish.finish();

        let trace = idl::Trace::decode(extra_writer.buf.lock().as_slice()).or_fail()?;
        expect_preamble_once(&trace)?;
        expect_sequence_clock(&trace);
        // The sample's counter track descriptor is the first packet after the preamble.
        expect_true!(trace.packet.get(2).is_some_and(|packet| matches!(
            &packet.data,
            Some(idl::trace_packet::Data::TrackDescriptor(descriptor))
                if descriptor.counter.is_some()
        )));
        Ok(())
    }

    /// The preamble precedes everything when a span is written before the memory sampler writes.
    ///
    /// A preamble written only by the sampler would follow the span's packets.
    #[gtest]
    fn preamble_precedes_a_span_written_first() -> Result<()> {
        let writer = TestWriter::new();
        let extra_writer = writer.make_writer();
        let (perfetto_layer, armed, announcements) = new_announcing_layer(writer, false);
        let finish = perfetto_layer.finish_handle();
        let subscriber = tracing_subscriber::registry().with(perfetto_layer);
        let _guard = tracing::subscriber::set_default(subscriber);

        // The disarmed sampler skips its intervals until the span has been written.
        trace_span!("span").in_scope(|| {});
        armed.store(true, Ordering::Release);
        wait_for_written_sample(&announcements)?;
        finish.finish();

        let trace = idl::Trace::decode(extra_writer.buf.lock().as_slice()).or_fail()?;
        expect_preamble_once(&trace)?;
        expect_sequence_clock(&trace);
        expect_true!(trace.packet.iter().any(is_counter_event));
        Ok(())
    }

    /// First writes racing on several threads and the sampler all land after the preamble.
    ///
    /// Concurrent first writers wait for the one writing the preamble, so none of them can
    /// write a packet between the preamble's start and its end.
    #[gtest]
    fn concurrent_first_writes_all_follow_the_preamble() -> Result<()> {
        const THREADS: usize = 8;
        let writer = TestWriter::new();
        let extra_writer = writer.make_writer();
        let (perfetto_layer, armed, announcements) = new_announcing_layer(writer, false);
        let finish = perfetto_layer.finish_handle();
        let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(perfetto_layer));

        // Release every thread's first span and the sampler's first sample together.
        let start = std::sync::Barrier::new(THREADS + 1);
        std::thread::scope(|scope| {
            for _ in 0..THREADS {
                scope.spawn(|| {
                    tracing::dispatcher::with_default(&dispatch, || {
                        start.wait();
                        trace_span!("racing_span").in_scope(|| {});
                    });
                });
            }
            start.wait();
            armed.store(true, Ordering::Release);
        });
        wait_for_written_sample(&announcements)?;
        finish.finish();

        let trace = idl::Trace::decode(extra_writer.buf.lock().as_slice()).or_fail()?;
        expect_preamble_once(&trace)?;
        expect_sequence_clock(&trace);
        // Each span begins a lifetime and an entered slice.
        let names = crate::decode_track_event_names(&trace.encode_to_vec()).or_fail()?;
        expect_eq!(names.len(), 2 * THREADS);
        Ok(())
    }
}
