//! Regression tests for the wire format: sequence IDs, the sequence clock and its preamble,
//! thread IDs, and traces that open on their own.

use crate::PerfettoLayer;
use crate::idl;
use crate::idl::track_event;
use crate::sequence::SEQUENCE_CLOCK_ID;
use crate::tests::TestWriter;
use googletest::prelude::*;
use prost::Message;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use tracing::{field, trace_span};
use tracing_subscriber::{fmt::MakeWriter, layer::SubscriberExt};

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

/// Returns the trusted packet sequence that `packet` names, if any.
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

/// A layer that samples an [`AnnouncingProcessMemory`], with the handles that drive the sampler.
struct AnnouncingLayer {
    layer: PerfettoLayer<TestWriter>,
    /// Arms the memory source, which reports no samples until then.
    armed: Arc<AtomicBool>,
    /// Receives one announcement per sample the memory source reports.
    announcements: mpsc::Receiver<()>,
}

/// Returns a layer writing to `writer` whose sampler polls an announcing memory source without
/// pause, armed from the start if `armed` is set.
fn new_announcing_layer(writer: TestWriter, armed: bool) -> AnnouncingLayer {
    let armed = Arc::new(AtomicBool::new(armed));
    let (announcements_sender, announcements) = mpsc::channel();
    let layer = PerfettoLayer::new(writer).with_process_memory_source(
        std::time::Duration::ZERO,
        Box::new(AnnouncingProcessMemory {
            armed: Arc::clone(&armed),
            announcements: announcements_sender,
        }),
    );
    AnnouncingLayer {
        layer,
        armed,
        announcements,
    }
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

/// Track and correlation IDs are never 0 and never repeat, also when drawn from many threads.
///
/// Track UUID 0 is invalid, and two spans sharing an ID would share a track or link each
/// other's slices.
#[gtest]
fn unique_ids_are_nonzero_and_never_repeat() -> Result<()> {
    const THREADS: usize = 4;
    const IDS_PER_THREAD: usize = 10_000;
    let drawn = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                scope.spawn(|| {
                    (0..IDS_PER_THREAD)
                        .map(|_| crate::packets::unique_uuid())
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        handles
            .into_iter()
            .map(std::thread::ScopedJoinHandle::join)
            .collect::<std::thread::Result<Vec<_>>>()
    })
    .or_fail()?;
    let ids: Vec<u64> = drawn.into_iter().flatten().collect();
    expect_that!(ids, each(gt(&0)));
    expect_eq!(
        ids.iter().collect::<HashSet<_>>().len(),
        THREADS * IDS_PER_THREAD
    );
    Ok(())
}

/// Processes with different IDs draw different IDs from the same counter and hash keys, as a
/// forked child does from the state it copied from its parent.
///
/// A seed fixed when the parent drew its first ID would carry over into the child, which would
/// then draw the parent's next IDs, so both traces would describe and use the same track UUIDs.
#[gtest]
fn ids_depend_on_the_process_id() {
    let hash_keys = std::hash::RandomState::new();
    let parent: Vec<u64> = (0..1_000)
        .map(|counter| crate::packets::id_for(&hash_keys, 100, counter))
        .collect();
    let child: Vec<u64> = (0..1_000)
        .map(|counter| crate::packets::id_for(&hash_keys, 101, counter))
        .collect();

    // No ID of the child, drawn at any counter, repeats one of the parent.
    let parent_ids: HashSet<u64> = parent.iter().copied().collect();
    expect_that!(
        child.iter().filter(|id| parent_ids.contains(id)).count(),
        eq(0)
    );

    // The same process and counter always give the same ID, so only the process ID differs.
    expect_eq!(
        crate::packets::id_for(&hash_keys, 100, 7),
        parent.get(7).copied().unwrap_or(0)
    );
}

/// The process descriptor names the process after its executable's file name.
///
/// A descriptor with only a process ID shows the process by that ID alone, so the traces of
/// several programs cannot be told apart.
#[gtest]
fn process_descriptor_names_the_executable() -> Result<()> {
    let writer = TestWriter::new();
    let extra_writer = writer.make_writer();
    let subscriber = tracing_subscriber::registry().with(PerfettoLayer::new(writer));
    tracing::subscriber::with_default(subscriber, || tracing::info!("opens the trace"));

    let trace = idl::Trace::decode(extra_writer.buf.lock().as_slice()).or_fail()?;
    let process_names: Vec<_> = trace
        .packet
        .iter()
        .filter_map(|packet| match &packet.data {
            Some(idl::trace_packet::Data::TrackDescriptor(descriptor)) => {
                descriptor.process.as_ref()
            }
            _ => None,
        })
        .map(|process| process.process_name.clone())
        .collect();
    let executable = std::env::current_exe().or_fail()?;
    let executable_name = executable
        .file_name()
        .or_fail()?
        .to_string_lossy()
        .into_owned();
    expect_eq!(process_names, [Some(executable_name)]);
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
    let AnnouncingLayer {
        layer: perfetto_layer,
        announcements,
        ..
    } = new_announcing_layer(writer, true);
    let finish = perfetto_layer.finish_handle();
    let subscriber = tracing_subscriber::registry().with(perfetto_layer);
    let _guard = tracing::subscriber::set_default(subscriber);

    trace_span!("span").in_scope(|| tracing::info!("inside the span"));
    tracing::info!("outside any span");
    wait_for_written_sample(&announcements)?;
    finish.finish().or_fail()?;

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
    let AnnouncingLayer {
        layer: perfetto_layer,
        announcements,
        ..
    } = new_announcing_layer(writer, true);
    let finish = perfetto_layer.finish_handle();
    let subscriber = tracing_subscriber::registry().with(perfetto_layer);
    let _guard = tracing::subscriber::set_default(subscriber);

    // Cover every write path: span begin and end, span event, unscoped event and memory
    // sample.
    trace_span!("span").in_scope(|| tracing::info!("inside the span"));
    tracing::info!("outside any span");
    wait_for_written_sample(&announcements)?;
    finish.finish().or_fail()?;

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
    let AnnouncingLayer {
        layer: perfetto_layer,
        announcements,
        ..
    } = new_announcing_layer(writer, true);
    let finish = perfetto_layer.finish_handle();
    let subscriber = tracing_subscriber::registry().with(perfetto_layer);
    let _guard = tracing::subscriber::set_default(subscriber);

    // The sampler writes first, then a span and an unscoped event follow.
    wait_for_written_sample(&announcements)?;
    trace_span!("span").in_scope(|| {});
    tracing::info!("outside any span");
    finish.finish().or_fail()?;

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
    let AnnouncingLayer {
        layer: perfetto_layer,
        armed,
        announcements,
    } = new_announcing_layer(writer, false);
    let finish = perfetto_layer.finish_handle();
    let subscriber = tracing_subscriber::registry().with(perfetto_layer);
    let _guard = tracing::subscriber::set_default(subscriber);

    // The disarmed sampler skips its intervals until the span has been written.
    trace_span!("span").in_scope(|| {});
    armed.store(true, Ordering::Release);
    wait_for_written_sample(&announcements)?;
    finish.finish().or_fail()?;

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
    let AnnouncingLayer {
        layer: perfetto_layer,
        armed,
        announcements,
    } = new_announcing_layer(writer, false);
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
    finish.finish().or_fail()?;

    let trace = idl::Trace::decode(extra_writer.buf.lock().as_slice()).or_fail()?;
    expect_preamble_once(&trace)?;
    expect_sequence_clock(&trace);
    // Each span begins a lifetime and an entered slice.
    let names = crate::decode_track_event_names(&trace.encode_to_vec()).or_fail()?;
    expect_eq!(names.len(), 2 * THREADS);
    Ok(())
}
