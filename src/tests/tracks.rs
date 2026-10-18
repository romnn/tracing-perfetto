//! Regression tests for the track model: which track each slice and instant lands on, when it is
//! written, and how tracks are described.

use crate::PerfettoLayer;
use crate::idl;
use crate::idl::track_event;
use crate::tests::{
    ReplayedSlice, TestWriter, annotated_layer, annotation_names, close_labeled, decode,
    decode_writes, events_of_type, instant_marked, replay_lifetime_slices, replay_slices,
    slice_named, span_track_descriptors, thread_track_uuids, track_descriptor, track_event,
    track_name,
};
use googletest::prelude::*;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::instrument::{Instrument as _, WithSubscriber as _};
use tracing::{field, trace_span};
use tracing_subscriber::Layer as _;
use tracing_subscriber::filter::filter_fn;
use tracing_subscriber::{fmt::MakeWriter, layer::SubscriberExt};

/// Returns the UUID of the one span track named `name`.
fn span_track_named(trace: &idl::Trace, name: &str) -> Result<u64> {
    let uuids: Vec<u64> = span_track_descriptors(trace)
        .into_iter()
        .filter(|descriptor| track_name(descriptor) == Some(name))
        .map(idl::TrackDescriptor::uuid)
        .collect();
    let [uuid] = uuids[..] else {
        return Err(format!(
            "expected one span track named {name}, got {}",
            uuids.len()
        ))
        .or_fail();
    };
    Ok(uuid)
}

/// Returns the slices whose begin and end are both in the trace, by name.
fn ended_slice_names(slices: &[ReplayedSlice]) -> Vec<&str> {
    slices
        .iter()
        .filter(|slice| slice.end_timestamp.is_some())
        .map(|slice| slice.name.as_str())
        .collect()
}

/// Two overlapping spans with one track name each get their own track of that name.
///
/// A UUID shared by all tracks of one name would put both slices on one track, where the first
/// span's end closes the second span's slice and the second's end closes the first's.
#[gtest]
fn overlapping_spans_with_one_track_name_get_distinct_tracks() -> Result<()> {
    let (layer, output) = annotated_layer();
    let process_track_uuid = layer.process_track_uuid();
    let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));

    // The first begins, the second begins, the first ends and the second ends.
    let first = trace_span!(
        "first",
        { "perfetto.track_name" } = "jobs",
        closes = field::Empty
    );
    let second = trace_span!(
        "second",
        { "perfetto.track_name" } = "jobs",
        closes = field::Empty
    );
    close_labeled(first, "first");
    close_labeled(second, "second");

    let trace = decode(&output)?;
    let slices = replay_lifetime_slices(&trace)?;
    expect_that!(
        ended_slice_names(&slices),
        unordered_elements_are![eq(&"first"), eq(&"second")]
    );

    // Both tracks are named after the track name and sit below the process track.
    let descriptors = span_track_descriptors(&trace);
    expect_that!(
        descriptors
            .iter()
            .map(|descriptor| (track_name(descriptor), descriptor.parent_uuid))
            .collect::<Vec<_>>(),
        elements_are![
            eq(&(Some("jobs"), Some(process_track_uuid))),
            eq(&(Some("jobs"), Some(process_track_uuid)))
        ]
    );

    // Each slice begins and ends on its own track.
    let first_track = slice_named(&slices, "first")?.track_uuid;
    let second_track = slice_named(&slices, "second")?.track_uuid;
    expect_ne!(first_track, second_track);
    let described: HashSet<u64> = descriptors
        .iter()
        .map(|descriptor| descriptor.uuid())
        .collect();
    expect_eq!(described, HashSet::from([first_track, second_track]));
    Ok(())
}

/// Children that run one after another nest on their parent's track, as do their children.
///
/// A child that kept its parent's track occupied after closing would push every later sibling
/// onto a sub-track, splitting a sequential flame chart across tracks.
#[gtest]
fn sequential_children_nest_on_the_parent_track() -> Result<()> {
    let (layer, output) = annotated_layer();
    let process_track_uuid = layer.process_track_uuid();
    let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));

    let root = trace_span!("root", closes = field::Empty);
    root.in_scope(|| {
        close_labeled(trace_span!("first", closes = field::Empty), "first");
        let second = trace_span!("second", closes = field::Empty);
        second.in_scope(|| {
            close_labeled(
                trace_span!("grandchild", closes = field::Empty),
                "grandchild",
            );
        });
        close_labeled(second, "second");
    });
    close_labeled(root, "root");

    let trace = decode(&output)?;
    let slices = replay_lifetime_slices(&trace)?;

    // The root span's track, named after it, is the only span track.
    let root_track = span_track_named(&trace, "root")?;
    expect_that!(span_track_descriptors(&trace), len(eq(1)));
    expect_that!(
        span_track_descriptors(&trace)
            .first()
            .and_then(|descriptor| descriptor.parent_uuid),
        some(eq(process_track_uuid))
    );

    // Every slice ends on that track, nested by span depth.
    let placements: Vec<_> = slices
        .iter()
        .map(|slice| {
            (
                slice.name.as_str(),
                slice.track_uuid,
                slice.depth,
                slice.end_timestamp.is_some(),
            )
        })
        .collect();
    expect_that!(
        placements,
        unordered_elements_are![
            eq(&("root", root_track, 0, true)),
            eq(&("first", root_track, 1, true)),
            eq(&("second", root_track, 1, true)),
            eq(&("grandchild", root_track, 2, true)),
        ]
    );
    Ok(())
}

/// Under a per-layer filter, a child of a filtered-out span nests on its nearest recorded
/// ancestor's track, and events inside the filtered-out span are instants there.
///
/// The filtered-out span has no slice, so a child that looked only at its direct parent would
/// become a root on a track of its own, and an event inside that parent would land on the
/// thread's track, splitting one call chain across unrelated tracks.
#[gtest]
fn child_of_a_filtered_out_span_nests_on_its_nearest_recorded_ancestor() -> Result<()> {
    let (layer, output) = annotated_layer();
    let layer = layer.with_filter(filter_fn(|metadata| metadata.name() != "filtered_out"));
    // Another layer records every span, so the filtered-out span exists in the registry instead
    // of being disabled at its callsite.
    let every_span = tracing_subscriber::fmt::layer().with_writer(std::io::sink);
    let subscriber = tracing_subscriber::registry().with(layer).with(every_span);
    let _guard = tracing::subscriber::set_default(subscriber);

    trace_span!("root").in_scope(|| {
        trace_span!("filtered_out").in_scope(|| {
            tracing::info!(marker = "in the filtered-out span");
            trace_span!("child").in_scope(|| {});
        });
    });

    let trace = decode(&output)?;
    let slices = replay_slices(&trace)?;
    let [thread_track] = thread_track_uuids(&trace)[..] else {
        return fail!("expected one thread track");
    };

    // The root's track is the only span track, and the child nests on it below the root.
    let root_track = span_track_named(&trace, "root")?;
    expect_that!(span_track_descriptors(&trace), len(eq(1)));
    let lifetimes: Vec<_> = replay_lifetime_slices(&trace)?
        .into_iter()
        .map(|slice| (slice.name, slice.track_uuid, slice.depth))
        .collect();
    expect_that!(
        lifetimes,
        elements_are![
            eq(&("root".to_string(), root_track, 0)),
            eq(&("child".to_string(), root_track, 1))
        ]
    );

    // The event inside the filtered-out span is an instant on the root's track.
    expect_eq!(
        instant_marked(&trace, "in the filtered-out span")?
            .event
            .track_uuid,
        Some(root_track)
    );

    // The filtered-out span has no slice on any track, the thread's included.
    expect_that!(
        slices
            .iter()
            .filter(|slice| slice.track_uuid == thread_track)
            .map(|slice| (slice.name.as_str(), slice.depth))
            .collect::<Vec<_>>(),
        elements_are![eq(&("root", 0)), eq(&("child", 1))]
    );
    Ok(())
}

/// A child that overlaps an open sibling gets a sub-track of the parent's track.
///
/// Sharing the parent's track would interleave the siblings' slices, so the first sibling's end
/// would close the second sibling's slice.
#[gtest]
fn overlapping_children_get_a_sub_track_of_the_parent_track() -> Result<()> {
    let (layer, output) = annotated_layer();
    let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));

    let root = trace_span!("root", closes = field::Empty);
    root.in_scope(|| {
        // The first and second child are open together and close in creation order.
        let first = trace_span!("first", closes = field::Empty);
        let second = trace_span!("second", closes = field::Empty);
        close_labeled(first, "first");
        close_labeled(second, "second");

        // Both closed, so the parent's track is free again.
        close_labeled(trace_span!("third", closes = field::Empty), "third");
    });
    close_labeled(root, "root");

    let trace = decode(&output)?;
    let slices = replay_lifetime_slices(&trace)?;
    expect_that!(ended_slice_names(&slices), len(eq(4)));

    // The overlapping child's track is named after it and sits below the parent's track.
    let root_track = span_track_named(&trace, "root")?;
    let second_track = span_track_named(&trace, "second")?;
    let second_descriptor = span_track_descriptors(&trace)
        .into_iter()
        .find(|descriptor| descriptor.uuid() == second_track)
        .or_fail()?;
    expect_eq!(second_descriptor.parent_uuid, Some(root_track));
    expect_that!(span_track_descriptors(&trace), len(eq(2)));

    // The other children nest on the parent's track.
    expect_eq!(slice_named(&slices, "first")?.track_uuid, root_track);
    expect_eq!(slice_named(&slices, "first")?.depth, 1);
    expect_eq!(slice_named(&slices, "second")?.depth, 0);
    expect_eq!(slice_named(&slices, "third")?.track_uuid, root_track);
    expect_eq!(slice_named(&slices, "third")?.depth, 1);
    Ok(())
}

/// A span closed on another thread ends on the track its slice began on.
///
/// Ending a slice on the closing thread's track would leave the slice open forever and put a
/// stray end on a track that has no open slice.
#[gtest]
fn span_closed_on_another_thread_ends_on_its_lifetime_track() -> Result<()> {
    let (layer, output) = annotated_layer();
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(layer));

    let span = tracing::dispatcher::with_default(&dispatch, || {
        trace_span!("moved", closes = field::Empty)
    });
    std::thread::scope(|scope| scope.spawn(move || close_labeled(span, "moved")).join())
        .or_fail()?;

    let trace = decode(&output)?;
    let slices = replay_lifetime_slices(&trace)?;
    let moved = slice_named(&slices, "moved")?;
    expect_eq!(moved.track_uuid, span_track_named(&trace, "moved")?);
    expect_true!(moved.end_timestamp.is_some());

    // The single end packet is on the slice's track.
    let end_tracks: Vec<_> = events_of_type(&trace, track_event::Type::SliceEnd)
        .map(|event| event.track_uuid)
        .collect();
    expect_eq!(end_tracks, [Some(moved.track_uuid)]);
    Ok(())
}

/// A span's begin and its events are written while the span is still open.
///
/// Buffering a span's packets until it closes would hide a long-running span from a trace read
/// before its end, and keep every packet of a span in memory for its lifetime.
#[gtest]
fn begin_and_span_events_are_written_while_the_span_is_open() -> Result<()> {
    let (layer, output) = annotated_layer();
    let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));

    let span = trace_span!("open");
    let entered = span.enter();
    tracing::info!(marker = "inside");

    // Before the close, the begin and the event are written and the slice is still open.
    let open_trace = decode(&output)?;
    let open_slices = replay_lifetime_slices(&open_trace)?;
    let open = slice_named(&open_slices, "open")?;
    expect_eq!(open.end_timestamp, None);
    let inside = instant_marked(&open_trace, "inside")?;
    expect_eq!(inside.event.track_uuid, Some(open.track_uuid));

    // The exit adds only the entered slice's end, and the close only the lifetime slice's end.
    drop(entered);
    drop(span);
    let closed_trace = decode(&output)?;
    let closed = slice_named(&replay_lifetime_slices(&closed_trace)?, "open")?.end_timestamp;
    expect_true!(closed.is_some());
    expect_eq!(
        closed_trace.packet.len(),
        open_trace.packet.len() + 2,
        "expected only the two slice ends to follow the exit and close"
    );
    Ok(())
}

/// A span that never closes still has its begin in the trace.
///
/// Writing slices only on close would drop every span that is still open when the program
/// exits, such as a span around `main`.
#[gtest]
fn span_that_never_closes_keeps_its_begin() -> Result<()> {
    let (layer, output) = annotated_layer();
    let guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));

    // A forgotten span is never closed.
    std::mem::forget(trace_span!("leaked"));
    drop(guard);

    let trace = decode(&output)?;
    let slices = replay_lifetime_slices(&trace)?;
    let leaked = slice_named(&slices, "leaked")?;
    expect_eq!(leaked.end_timestamp, None);
    expect_eq!(
        events_of_type(&trace, track_event::Type::SliceEnd).count(),
        0
    );
    Ok(())
}

/// An event inside a span is an instant on the span's track, between its begin and end.
///
/// An event on the thread's track would detach it from the span, which may run on several
/// threads.
#[gtest]
fn events_in_a_span_are_instants_on_its_track() -> Result<()> {
    let (layer, output) = annotated_layer();
    let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));

    trace_span!("scope").in_scope(|| tracing::info!(marker = "inside"));

    let trace = decode(&output)?;
    let slices = replay_lifetime_slices(&trace)?;
    let scope = slice_named(&slices, "scope")?;
    let inside = instant_marked(&trace, "inside")?;
    expect_eq!(inside.event.track_uuid, Some(scope.track_uuid));
    expect_that!(inside.timestamp, ge(scope.begin_timestamp));
    expect_that!(inside.timestamp, le(scope.end_timestamp.or_fail()?));
    Ok(())
}

/// An event with an explicit parent is an instant on the parent's track.
///
/// Routing by the current span instead would put the event on an unrelated span's track.
#[gtest]
fn events_with_an_explicit_parent_go_to_the_parent_track() -> Result<()> {
    let (layer, output) = annotated_layer();
    let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));

    let target = trace_span!("target");
    trace_span!("current").in_scope(|| tracing::info!(parent: &target, marker = "explicit"));
    drop(target);

    let trace = decode(&output)?;
    let slices = replay_lifetime_slices(&trace)?;
    let explicit = instant_marked(&trace, "explicit")?;
    expect_eq!(
        explicit.event.track_uuid,
        Some(slice_named(&slices, "target")?.track_uuid)
    );
    expect_ne!(
        explicit.event.track_uuid,
        Some(slice_named(&slices, "current")?.track_uuid)
    );
    Ok(())
}

/// Events outside any span, or explicitly without a parent, are instants on the thread's track.
///
/// These events have no span track, so they belong to the thread that records them.
#[gtest]
fn unscoped_events_go_to_the_thread_track() -> Result<()> {
    let (layer, output) = annotated_layer();
    let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));

    tracing::info!(marker = "outside");
    trace_span!("span").in_scope(|| tracing::info!(parent: None, marker = "rootless"));

    let trace = decode(&output)?;
    let [thread_track] = thread_track_uuids(&trace)[..] else {
        return fail!("expected one thread track");
    };
    expect_eq!(
        instant_marked(&trace, "outside")?.event.track_uuid,
        Some(thread_track)
    );
    expect_eq!(
        instant_marked(&trace, "rootless")?.event.track_uuid,
        Some(thread_track)
    );
    Ok(())
}

/// Fields recorded after a span's creation are annotations on its slice's end.
///
/// The begin is already written when a field is recorded, so a recorded value kept anywhere but
/// the end would be lost.
#[gtest]
fn recorded_fields_are_annotations_on_the_end() -> Result<()> {
    let (layer, output) = annotated_layer();
    let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));

    let span = trace_span!("recorded", initial = 1, late = field::Empty);
    span.record("late", 2);
    drop(span);

    let trace = decode(&output)?;
    let slices = replay_lifetime_slices(&trace)?;
    let recorded = slice_named(&slices, "recorded")?;
    expect_eq!(annotation_names(&recorded.begin_annotations), ["initial"]);
    expect_eq!(annotation_names(&recorded.end_annotations), ["late"]);
    expect_that!(
        recorded
            .end_annotations
            .first()
            .and_then(|late| late.value.clone()),
        some(eq(&idl::debug_annotation::Value::IntValue(2)))
    );
    Ok(())
}

/// Without debug annotations, recorded fields add nothing to the slice's end.
///
/// The layer would otherwise keep every recorded value in memory and write it although the
/// user turned annotations off.
#[gtest]
fn recorded_fields_are_dropped_without_debug_annotations() -> Result<()> {
    let writer = TestWriter::new();
    let output = writer.make_writer();
    let _guard = tracing::subscriber::set_default(
        tracing_subscriber::registry().with(PerfettoLayer::new(writer)),
    );

    let span = trace_span!("recorded", late = field::Empty);
    span.record("late", 2);
    drop(span);

    let trace = decode(&output)?;
    let ends: Vec<_> = events_of_type(&trace, track_event::Type::SliceEnd).collect();
    expect_that!(ends, elements_are![anything()]);
    expect_that!(
        ends.iter()
            .flat_map(|end| &end.debug_annotations)
            .collect::<Vec<_>>(),
        is_empty()
    );
    Ok(())
}

/// A `perfetto.track_name` recorded after creation leaves the span and its children in place.
///
/// The begin is already written on the span's track, so moving the span later would put its
/// end, events and children on a track where its slice never began.
#[gtest]
fn late_track_name_does_not_move_the_span() -> Result<()> {
    let (layer, output) = annotated_layer();
    let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));

    let span = trace_span!("early", { "perfetto.track_name" } = field::Empty);
    span.in_scope(|| {
        tracing::Span::current().record("perfetto.track_name", "late");
        trace_span!("child").in_scope(|| {});
        tracing::info!(marker = "after the record");
    });
    drop(span);

    let trace = decode(&output)?;
    let slices = replay_lifetime_slices(&trace)?;

    // The only span track keeps the span's own name.
    let early_track = span_track_named(&trace, "early")?;
    expect_that!(span_track_descriptors(&trace), len(eq(1)));

    // The span, its child and its event all stay on that track.
    expect_eq!(slice_named(&slices, "early")?.track_uuid, early_track);
    expect_true!(slice_named(&slices, "early")?.end_timestamp.is_some());
    expect_eq!(slice_named(&slices, "child")?.track_uuid, early_track);
    expect_eq!(
        instant_marked(&trace, "after the record")?.event.track_uuid,
        Some(early_track)
    );
    Ok(())
}

/// Where a packet sits in the output: its write and its position across all writes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct PacketPosition {
    write: usize,
    position: usize,
}

/// Every track is described exactly once, in the write that first references it.
///
/// Repeating the thread descriptor on every write bloats the trace, and a descriptor in a later
/// write than the track's first use leaves a reader of a cut-short trace with an undescribed
/// track.
#[gtest]
fn each_track_is_described_once_with_its_first_use() -> Result<()> {
    let (layer, output) = annotated_layer();
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(layer));

    // Each thread writes unscoped events, a root span with overlapping children and a named
    // span.
    let record = || {
        tracing::dispatcher::with_default(&dispatch, || {
            tracing::info!("unscoped");
            tracing::info!("unscoped again");
            trace_span!("root").in_scope(|| {
                let first = trace_span!("first");
                let second = trace_span!("second");
                second.in_scope(|| tracing::info!("in the sub-track"));
                drop(first);
                drop(second);
                trace_span!("named", { "perfetto.track_name" } = "jobs").in_scope(|| {});
            });
            tracing::info!("unscoped last");
        });
    };
    record();
    std::thread::scope(|scope| {
        for _ in 0..2 {
            scope.spawn(record);
        }
    });

    // Index every descriptor and every track reference by its position in the output.
    let writes = decode_writes(&output)?;
    let mut descriptions: HashMap<u64, Vec<PacketPosition>> = HashMap::new();
    let mut first_references: HashMap<u64, PacketPosition> = HashMap::new();
    let mut process_tracks = HashSet::new();
    let packets = writes
        .iter()
        .enumerate()
        .flat_map(|(write, trace)| trace.packet.iter().map(move |packet| (write, packet)));
    for (position, (write, packet)) in packets.enumerate() {
        let at = PacketPosition { write, position };
        let references = if let Some(descriptor) = track_descriptor(packet) {
            descriptions.entry(descriptor.uuid()).or_default().push(at);
            if descriptor.process.is_some() {
                process_tracks.insert(descriptor.uuid());
            }
            descriptor.parent_uuid
        } else {
            track_event(packet).and_then(|event| event.track_uuid)
        };
        if let Some(referenced) = references {
            first_references.entry(referenced).or_insert(at);
        }
    }

    // Three thread tracks and three span tracks per thread are each described once.
    let trace = decode(&output)?;
    expect_that!(thread_track_uuids(&trace), len(eq(3)));
    expect_that!(span_track_descriptors(&trace), len(eq(9)));
    for (uuid, positions) in &descriptions {
        expect_that!(
            positions,
            len(eq(1)),
            "track {uuid} is described more than once"
        );
    }

    // Each referenced track is described before its first use, in the same write unless it is
    // the process track of the preamble.
    for (uuid, first_reference) in &first_references {
        let Some([description]) = descriptions.get(uuid).map(Vec::as_slice) else {
            return fail!("track {uuid} is referenced but not described exactly once");
        };
        expect_that!(description.position, lt(first_reference.position));
        if !process_tracks.contains(uuid) {
            expect_eq!(description.write, first_reference.write, "track {uuid}");
        }
    }
    replay_slices(&trace)?;
    Ok(())
}

/// With span lifetimes and thread slices disabled, spans write no slices and their events go to
/// the thread track.
///
/// The option exists to drop the per-span tracks, which duplicate the thread's flame chart in
/// synchronous programs.
#[gtest]
fn disabled_span_lifetimes_write_no_slices() -> Result<()> {
    let writer = TestWriter::new();
    let output = writer.make_writer();
    // Entered slices are left out too, so that any slice would come from a span lifetime.
    let layer = PerfettoLayer::new(writer)
        .with_debug_annotations(true)
        .with_span_lifetimes(false)
        .with_thread_slices(false);
    let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));

    let root = trace_span!(
        "root",
        { "perfetto.track_name" } = "jobs",
        late = field::Empty
    );
    root.in_scope(|| {
        trace_span!("child").in_scope(|| tracing::info!(marker = "inside"));
    });
    root.record("late", 1);
    drop(root);

    let trace = decode(&output)?;
    let slice_events = trace
        .packet
        .iter()
        .filter_map(track_event)
        .filter(|event| event.r#type() != track_event::Type::Instant)
        .count();
    expect_eq!(slice_events, 0);
    expect_that!(span_track_descriptors(&trace), is_empty());
    expect_eq!(
        instant_marked(&trace, "inside")?.event.track_uuid,
        thread_track_uuids(&trace).first().copied()
    );
    Ok(())
}

/// Many concurrent Tokio tasks with one track name keep valid slice stacks on every track.
///
/// The tasks overlap and migrate between worker threads, so tracks shared by name or children
/// sharing a busy parent track would interleave slices, and ends recorded on worker threads'
/// tracks would close nothing.
#[gtest]
fn concurrent_tasks_with_one_track_name_keep_every_track_nested() -> Result<()> {
    const TASKS: usize = 32;
    let (layer, output) = annotated_layer();
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(layer));
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .build()
        .or_fail()?;

    // Each task runs a sequential child, then two children that wait for each other, then
    // waits until every task is open.
    let all_open = Arc::new(tokio::sync::Barrier::new(TASKS));
    let job = |all_open: Arc<tokio::sync::Barrier>| async move {
        async {
            tracing::Span::current().record("closes", "step");
        }
        .instrument(trace_span!("step", closes = field::Empty))
        .await;

        let both_open = Arc::new(tokio::sync::Barrier::new(2));
        let side = |span: tracing::Span, name: &'static str| {
            let both_open = Arc::clone(&both_open);
            async move {
                both_open.wait().await;
                tracing::info!("inside a concurrent child");
                tracing::Span::current().record("closes", name);
            }
            .instrument(span)
        };
        // Both children are created before either runs, so the second overlaps the first.
        let left = side(trace_span!("left", closes = field::Empty), "left");
        let right = side(trace_span!("right", closes = field::Empty), "right");
        tokio::join!(left, right);

        all_open.wait().await;
        tracing::Span::current().record("closes", "job");
    };
    tracing::dispatcher::with_default(&dispatch, || {
        runtime.block_on(async {
            let tasks: Vec<_> = (0..TASKS)
                .map(|_| {
                    let span = trace_span!(
                        "job",
                        { "perfetto.track_name" } = "jobs",
                        closes = field::Empty
                    );
                    tokio::spawn(
                        job(Arc::clone(&all_open))
                            .instrument(span)
                            .with_subscriber(dispatch.clone()),
                    )
                })
                .collect();
            for task in tasks {
                task.await.or_fail()?;
            }
            Result::<()>::Ok(())
        })
    })?;
    drop(runtime);

    // Every end closes the innermost open slice of its own name on its own track.
    let trace = decode(&output)?;
    let slices = replay_lifetime_slices(&trace)?;
    expect_that!(ended_slice_names(&slices), len(eq(4 * TASKS)));

    // Every task has its own "jobs" track, and its second concurrent child a sub-track of it.
    let descriptors = span_track_descriptors(&trace);
    let job_tracks: HashSet<u64> = descriptors
        .iter()
        .filter(|descriptor| track_name(descriptor) == Some("jobs"))
        .map(|descriptor| descriptor.uuid())
        .collect();
    expect_that!(job_tracks, len(eq(TASKS)));
    let right_track_parents: Vec<_> = descriptors
        .iter()
        .filter(|descriptor| track_name(descriptor) == Some("right"))
        .map(|descriptor| descriptor.parent_uuid)
        .collect();
    expect_that!(right_track_parents, len(eq(TASKS)));
    expect_that!(
        right_track_parents,
        each(predicate(
            |parent: &Option<u64>| parent.is_some_and(|parent| job_tracks.contains(&parent))
        ))
    );
    expect_that!(descriptors, len(eq(2 * TASKS)));
    Ok(())
}
