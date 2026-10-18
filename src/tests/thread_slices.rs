//! Regression tests for entered slices: the slices that each entry of a span writes on the track
//! of the thread that entered it.

use crate::PerfettoLayer;
use crate::idl;
use crate::idl::track_event;
use crate::tests::{
    ReplayedSlice, TestWriter, annotated_layer, annotation_names, decode, instant_marked,
    replay_slices, span_track_descriptors, thread_track_uuids, track_descriptor, track_event,
    track_name,
};
use googletest::prelude::*;
use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::{Arc, Barrier};
use std::task::{Context, Poll, Waker};
use tracing::instrument::{Instrument as _, WithSubscriber as _};
use tracing::{field, trace_span};
use tracing_subscriber::{fmt::MakeWriter, layer::SubscriberExt};

/// Returns the UUID of the one thread track named after the thread `thread_name`.
fn thread_track_named(trace: &idl::Trace, thread_name: &str) -> Result<u64> {
    let uuids: Vec<u64> = trace
        .packet
        .iter()
        .filter_map(track_descriptor)
        .filter(|descriptor| {
            descriptor.thread.is_some() && track_name(descriptor) == Some(thread_name)
        })
        .map(idl::TrackDescriptor::uuid)
        .collect();
    let [uuid] = uuids[..] else {
        return Err(format!(
            "expected one thread track named {thread_name}, got {}",
            uuids.len()
        ))
        .or_fail();
    };
    Ok(uuid)
}

/// Returns the slices on the track `track_uuid`, in the order they began.
fn slices_on(slices: &[ReplayedSlice], track_uuid: u64) -> Vec<&ReplayedSlice> {
    slices
        .iter()
        .filter(|slice| slice.track_uuid == track_uuid)
        .collect()
}

/// Returns each slice's name, depth and whether it ended, which together show its nesting.
fn nesting<'a>(slices: &[&'a ReplayedSlice]) -> Vec<(&'a str, usize, bool)> {
    slices
        .iter()
        .map(|slice| {
            (
                slice.name.as_str(),
                slice.depth,
                slice.end_timestamp.is_some(),
            )
        })
        .collect()
}

/// Returns the UUIDs of the span tracks, which hold lifetime slices.
fn lifetime_track_uuids(trace: &idl::Trace) -> HashSet<u64> {
    span_track_descriptors(trace)
        .into_iter()
        .map(idl::TrackDescriptor::uuid)
        .collect()
}

/// Returns the one lifetime slice named `name`.
fn lifetime_slice_named<'a>(
    trace: &idl::Trace,
    slices: &'a [ReplayedSlice],
    name: &str,
) -> Result<&'a ReplayedSlice> {
    let lifetime_tracks = lifetime_track_uuids(trace);
    let named: Vec<&ReplayedSlice> = slices
        .iter()
        .filter(|slice| slice.name == name && lifetime_tracks.contains(&slice.track_uuid))
        .collect();
    let [lifetime] = named[..] else {
        return Err(format!(
            "expected one lifetime slice named {name}, got {}",
            named.len()
        ))
        .or_fail();
    };
    Ok(lifetime)
}

/// Polls `future` once on a new thread named `thread_name`.
fn poll_on_thread<F>(thread_name: &str, future: &mut F) -> Result<Poll<F::Output>>
where
    F: Future + Unpin + Send,
    F::Output: Send,
{
    std::thread::scope(|scope| {
        std::thread::Builder::new()
            .name(thread_name.to_string())
            .spawn_scoped(scope, || {
                let mut context = Context::from_waker(Waker::noop());
                Pin::new(future).poll(&mut context)
            })
            .or_fail()?
            .join()
            .or_fail()
    })
}

/// Runs `enter` on a new thread named `thread_name` under `dispatch`.
fn enter_on_thread(
    dispatch: &tracing::Dispatch,
    thread_name: &str,
    enter: impl FnOnce() + Send,
) -> Result<()> {
    std::thread::scope(|scope| {
        std::thread::Builder::new()
            .name(thread_name.to_string())
            .spawn_scoped(scope, || tracing::dispatcher::with_default(dispatch, enter))
            .or_fail()?
            .join()
            .or_fail()
    })
}

/// Spans entered inside each other on one thread are nested slices on that thread's track.
///
/// Without entered slices, the thread's track would not show where a synchronous call chain
/// ran, and an entered slice left open past its exit would break the nesting of later slices.
#[gtest]
fn nested_entered_spans_nest_on_the_thread_track() -> Result<()> {
    let (layer, output) = annotated_layer();
    let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));

    trace_span!("a").in_scope(|| trace_span!("b").in_scope(|| trace_span!("c").in_scope(|| {})));

    // Every track, lifetime and thread alike, holds a valid stack of slices.
    let trace = decode(&output)?;
    let slices = replay_slices(&trace)?;
    let [thread_track] = thread_track_uuids(&trace)[..] else {
        return fail!("expected one thread track");
    };

    // The thread's track holds one slice per span, nested by call depth.
    let on_thread = slices_on(&slices, thread_track);
    expect_that!(
        nesting(&on_thread),
        elements_are![
            eq(&("a", 0, true)),
            eq(&("b", 1, true)),
            eq(&("c", 2, true))
        ]
    );

    // Each slice lies within the slice that encloses it.
    let [a, b, c] = on_thread[..] else {
        return fail!("expected three slices on the thread track");
    };
    for (outer, inner) in [(a, b), (b, c)] {
        expect_that!(inner.begin_timestamp, ge(outer.begin_timestamp));
        expect_that!(
            inner.end_timestamp.or_fail()?,
            le(outer.end_timestamp.or_fail()?)
        );
    }
    Ok(())
}

/// Each span's lifetime slice and entered slices share one correlation ID that no other span
/// has, even a child that nests on its parent's track.
///
/// An ID taken from the lifetime track would be shared by a parent and the children nested on
/// its track, so selecting a child's slice would highlight the parent's slices as the same span.
#[gtest]
fn each_span_has_its_own_correlation_id() -> Result<()> {
    let (layer, output) = annotated_layer();
    let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));

    trace_span!("a").in_scope(|| trace_span!("b").in_scope(|| trace_span!("c").in_scope(|| {})));
    trace_span!("other").in_scope(|| {});

    let trace = decode(&output)?;
    let slices = replay_slices(&trace)?;
    let [thread_track] = thread_track_uuids(&trace)[..] else {
        return fail!("expected one thread track");
    };

    // The children b and c nest on the lifetime track of their parent a.
    let a = lifetime_slice_named(&trace, &slices, "a")?;
    let b = lifetime_slice_named(&trace, &slices, "b")?;
    let c = lifetime_slice_named(&trace, &slices, "c")?;
    let other = lifetime_slice_named(&trace, &slices, "other")?;
    expect_eq!(b.track_uuid, a.track_uuid);
    expect_eq!(c.track_uuid, a.track_uuid);

    // Every lifetime slice carries a nonzero ID that no other span shares.
    let lifetime_ids = [a, b, c, other].map(|lifetime| lifetime.correlation_id);
    expect_that!(lifetime_ids, each(some(gt(0))));
    expect_that!(HashSet::from(lifetime_ids), len(eq(4)));

    // Each entered slice carries the ID of its own span's lifetime slice.
    let entered: Vec<_> = slices_on(&slices, thread_track)
        .into_iter()
        .map(|slice| (slice.name.as_str(), slice.correlation_id))
        .collect();
    expect_that!(
        entered,
        elements_are![
            eq(&("a", a.correlation_id)),
            eq(&("b", b.correlation_id)),
            eq(&("c", c.correlation_id)),
            eq(&("other", other.correlation_id))
        ]
    );
    Ok(())
}

/// A future polled on two threads has an entered slice on each thread's track, both linked to
/// its lifetime slice.
///
/// Recording polls only on the lifetime track, or on the thread that created the span, would
/// hide which workers ran the future.
#[gtest]
fn future_polled_on_two_threads_has_an_entered_slice_on_each() -> Result<()> {
    let (layer, output) = annotated_layer();
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(layer));
    let span = tracing::dispatcher::with_default(&dispatch, || trace_span!("polled"));

    // The future is pending on its first poll and ready on its second, each on its own thread.
    let mut polls = 0;
    let mut future = Box::pin(
        std::future::poll_fn(move |_| {
            polls += 1;
            if polls == 1 {
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        })
        .instrument(span),
    );
    expect_true!(poll_on_thread("first poller", &mut future)?.is_pending());
    expect_true!(poll_on_thread("second poller", &mut future)?.is_ready());
    drop(future);

    let trace = decode(&output)?;
    let slices = replay_slices(&trace)?;
    let lifetime = lifetime_slice_named(&trace, &slices, "polled")?;

    // Each poller's track holds one ended slice of the span, inside its lifetime slice.
    for thread_name in ["first poller", "second poller"] {
        let on_thread = slices_on(&slices, thread_track_named(&trace, thread_name)?);
        let [poll] = on_thread[..] else {
            return fail!("expected one slice on the track of {thread_name}");
        };
        expect_eq!(poll.name, "polled", "on {thread_name}");
        expect_eq!(
            poll.correlation_id,
            lifetime.correlation_id,
            "on {thread_name}"
        );
        expect_that!(poll.begin_timestamp, ge(lifetime.begin_timestamp));
        expect_that!(
            poll.end_timestamp.or_fail()?,
            le(lifetime.end_timestamp.or_fail()?)
        );
    }
    Ok(())
}

/// A span that exits while a span entered after it is still entered ends its slice, and the
/// later span's slice resumes at the same instant under the same correlation ID.
///
/// Popping the innermost slice on every exit would end the later span's slice at the first
/// exit and the earlier span's slice at the second, so both would show the wrong duration.
#[gtest]
fn out_of_order_exit_resumes_the_slices_entered_after_it() -> Result<()> {
    let (layer, output) = annotated_layer();
    let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));

    // The outer span exits while the inner one is still entered.
    let outer = trace_span!("outer");
    let inner = trace_span!("inner");
    let outer_entered = outer.enter();
    let inner_entered = inner.enter();
    drop(outer_entered);
    tracing::info!(marker = "between the exits");
    drop(inner_entered);
    drop(inner);
    drop(outer);

    let trace = decode(&output)?;
    let slices = replay_slices(&trace)?;
    let [thread_track] = thread_track_uuids(&trace)[..] else {
        return fail!("expected one thread track");
    };

    // The inner slice ends with the outer one, then resumes on its own.
    let on_thread = slices_on(&slices, thread_track);
    expect_that!(
        nesting(&on_thread),
        elements_are![
            eq(&("outer", 0, true)),
            eq(&("inner", 1, true)),
            eq(&("inner", 0, true))
        ]
    );
    let [outer_slice, inner_before, inner_after] = on_thread[..] else {
        return fail!("expected three slices on the thread track");
    };
    expect_eq!(inner_before.end_timestamp, outer_slice.end_timestamp);
    expect_eq!(Some(inner_after.begin_timestamp), outer_slice.end_timestamp);

    // The outer slice ends at the outer exit and the resumed inner slice at the inner exit.
    let between = instant_marked(&trace, "between the exits")?.timestamp;
    expect_that!(outer_slice.end_timestamp.or_fail()?, le(between));
    expect_that!(inner_after.end_timestamp.or_fail()?, ge(between));

    // Both parts of the inner slice link to the inner span's lifetime slice.
    let inner_id = lifetime_slice_named(&trace, &slices, "inner")?.correlation_id;
    expect_that!(inner_id, some(anything()));
    expect_eq!(inner_before.correlation_id, inner_id);
    expect_eq!(inner_after.correlation_id, inner_id);
    Ok(())
}

/// A span entered again while it is already entered on the same thread nests a second slice
/// inside the first.
///
/// Matching an exit against the outermost entry of the span, or recording only its first
/// entry, would end the outer slice early or lose the inner one.
#[gtest]
fn reentered_span_nests_a_slice_inside_its_own() -> Result<()> {
    let (layer, output) = annotated_layer();
    let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));

    let span = trace_span!("reentered");
    let outer = span.enter();
    let inner = span.enter();
    drop(inner);
    tracing::info!(marker = "between the exits");
    drop(outer);
    drop(span);

    let trace = decode(&output)?;
    let slices = replay_slices(&trace)?;
    let [thread_track] = thread_track_uuids(&trace)[..] else {
        return fail!("expected one thread track");
    };
    let on_thread = slices_on(&slices, thread_track);
    expect_that!(
        nesting(&on_thread),
        elements_are![eq(&("reentered", 0, true)), eq(&("reentered", 1, true))]
    );

    // The inner slice ends at the first exit and the outer slice at the second.
    let [outer_slice, inner_slice] = on_thread[..] else {
        return fail!("expected two slices on the thread track");
    };
    let between = instant_marked(&trace, "between the exits")?.timestamp;
    expect_that!(inner_slice.end_timestamp.or_fail()?, le(between));
    expect_that!(outer_slice.end_timestamp.or_fail()?, ge(between));
    Ok(())
}

/// A span entered on two threads at once has an overlapping slice on each thread's track.
///
/// A span-wide record of the entry instead of a stack per thread would skip the second
/// thread's entry, or end one thread's slice at the other thread's exit.
#[gtest]
fn span_entered_on_two_threads_at_once_nests_on_each() -> Result<()> {
    let (layer, output) = annotated_layer();
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(layer));
    let span = tracing::dispatcher::with_default(&dispatch, || trace_span!("shared"));

    // Both threads hold the span entered until each has entered it.
    let both_entered = Barrier::new(2);
    std::thread::scope(|scope| {
        for thread_name in ["first enterer", "second enterer"] {
            std::thread::Builder::new()
                .name(thread_name.to_string())
                .spawn_scoped(scope, || {
                    let _entered = span.enter();
                    both_entered.wait();
                })
                .or_fail()?;
        }
        Result::<()>::Ok(())
    })?;
    drop(span);

    let trace = decode(&output)?;
    let slices = replay_slices(&trace)?;
    let shared_id = lifetime_slice_named(&trace, &slices, "shared")?.correlation_id;
    expect_that!(shared_id, some(anything()));
    let mut entered = Vec::new();
    for thread_name in ["first enterer", "second enterer"] {
        let on_thread = slices_on(&slices, thread_track_named(&trace, thread_name)?);
        expect_that!(
            nesting(&on_thread),
            elements_are![eq(&("shared", 0, true))],
            "on {thread_name}"
        );
        expect_that!(
            on_thread
                .iter()
                .map(|slice| slice.correlation_id)
                .collect::<Vec<_>>(),
            elements_are![eq(&shared_id)]
        );
        entered.extend(on_thread);
    }

    // Each slice begins before the other ends, because both threads entered before either
    // exited.
    let [first, second] = entered[..] else {
        return fail!("expected one slice per thread");
    };
    expect_that!(first.begin_timestamp, le(second.end_timestamp.or_fail()?));
    expect_that!(second.begin_timestamp, le(first.end_timestamp.or_fail()?));
    Ok(())
}

/// With thread slices disabled, thread tracks hold no slices while lifetime slices remain.
///
/// The option exists to keep thread tracks to events, so ignoring it would still duplicate
/// every span on the thread tracks.
#[gtest]
fn disabled_thread_slices_leave_thread_tracks_without_slices() -> Result<()> {
    let writer = TestWriter::new();
    let output = writer.make_writer();
    let layer = PerfettoLayer::new(writer).with_thread_slices(false);
    let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));

    // The unscoped event describes the thread's track.
    trace_span!("a").in_scope(|| {
        trace_span!("b").in_scope(|| tracing::info!(parent: None, "unscoped"));
    });

    let trace = decode(&output)?;
    let slices = replay_slices(&trace)?;
    let [thread_track] = thread_track_uuids(&trace)[..] else {
        return fail!("expected one thread track");
    };
    expect_that!(slices_on(&slices, thread_track), is_empty());
    let lifetime_tracks = lifetime_track_uuids(&trace);
    expect_that!(
        slices
            .iter()
            .map(|slice| (
                slice.name.as_str(),
                lifetime_tracks.contains(&slice.track_uuid)
            ))
            .collect::<Vec<_>>(),
        elements_are![eq(&("a", true)), eq(&("b", true))]
    );
    Ok(())
}

/// Without span lifetimes, all of a span's entered slices share one correlation ID of the
/// span's own, across threads and entries.
///
/// The span has no lifetime track whose UUID could link its slices, so without an ID of its
/// own its polls on different workers could not be linked to each other.
#[gtest]
fn entered_slices_without_span_lifetimes_share_a_correlation_id_per_span() -> Result<()> {
    let writer = TestWriter::new();
    let output = writer.make_writer();
    let layer = PerfettoLayer::new(writer).with_span_lifetimes(false);
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(layer));
    let (first, second) = tracing::dispatcher::with_default(&dispatch, || {
        (trace_span!("first"), trace_span!("second"))
    });

    // Each thread enters both spans, one after the other.
    for thread_name in ["first enterer", "second enterer"] {
        enter_on_thread(&dispatch, thread_name, || {
            first.in_scope(|| {});
            second.in_scope(|| {});
        })?;
    }
    drop((first, second));

    // Only the two thread tracks hold slices, and no span has a track of its own.
    let trace = decode(&output)?;
    let slices = replay_slices(&trace)?;
    expect_that!(span_track_descriptors(&trace), is_empty());
    let thread_tracks = HashSet::from([
        thread_track_named(&trace, "first enterer")?,
        thread_track_named(&trace, "second enterer")?,
    ]);
    expect_that!(slices, len(eq(4)));
    expect_that!(
        slices
            .iter()
            .map(|slice| slice.track_uuid)
            .collect::<HashSet<_>>(),
        eq(&thread_tracks)
    );

    // Each span's slices share one correlation ID, which differs between the spans.
    let mut correlation_ids: HashMap<&str, HashSet<Option<u64>>> = HashMap::new();
    for slice in &slices {
        correlation_ids
            .entry(slice.name.as_str())
            .or_default()
            .insert(slice.correlation_id);
    }
    let [Some(first_id)] = correlation_ids
        .get("first")
        .or_fail()?
        .iter()
        .copied()
        .collect::<Vec<_>>()[..]
    else {
        return fail!("expected one correlation ID for first, got {correlation_ids:?}");
    };
    let [Some(second_id)] = correlation_ids
        .get("second")
        .or_fail()?
        .iter()
        .copied()
        .collect::<Vec<_>>()[..]
    else {
        return fail!("expected one correlation ID for second, got {correlation_ids:?}");
    };
    expect_ne!(first_id, second_id);
    expect_that!(first_id, gt(0));
    expect_that!(second_id, gt(0));
    Ok(())
}

/// Without span lifetimes, each entered slice's begin carries the span's source location, its
/// creation fields and the fields recorded so far.
///
/// The entered slices are then the span's only slices, so fields kept for a lifetime slice
/// would never reach the trace.
#[gtest]
fn entered_slices_without_span_lifetimes_carry_the_span_fields() -> Result<()> {
    let writer = TestWriter::new();
    let output = writer.make_writer();
    let layer = PerfettoLayer::new(writer)
        .with_debug_annotations(true)
        .with_span_lifetimes(false);
    let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));

    // The second entry follows a recorded field.
    let span = trace_span!("detailed", initial = 1, late = field::Empty);
    span.in_scope(|| {});
    span.record("late", 2);
    span.in_scope(|| {});
    drop(span);

    let trace = decode(&output)?;
    let slices = replay_slices(&trace)?;
    let [before, after] = &slices[..] else {
        return fail!("expected two entered slices, got {}", slices.len());
    };
    expect_eq!(annotation_names(&before.begin_annotations), ["initial"]);
    expect_eq!(
        annotation_names(&after.begin_annotations),
        ["initial", "late"]
    );
    expect_true!(before.has_source_location);
    expect_true!(after.has_source_location);
    Ok(())
}

/// With span lifetimes, entered slices leave the span's fields and source location to its
/// lifetime slice.
///
/// Repeating them on every entered slice would copy every field once per poll, although the
/// correlation ID already links each poll to the lifetime slice that carries them.
#[gtest]
fn entered_slices_leave_the_span_fields_to_the_lifetime_slice() -> Result<()> {
    let (layer, output) = annotated_layer();
    let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));

    let span = trace_span!("detailed", initial = 1, late = field::Empty);
    span.in_scope(|| {});
    span.record("late", 2);
    span.in_scope(|| {});
    drop(span);

    let trace = decode(&output)?;
    let slices = replay_slices(&trace)?;
    let [thread_track] = thread_track_uuids(&trace)[..] else {
        return fail!("expected one thread track");
    };

    // The entered slices carry neither fields nor a location.
    let on_thread = slices_on(&slices, thread_track);
    expect_that!(on_thread, len(eq(2)));
    for entered in &on_thread {
        expect_that!(entered.begin_annotations, is_empty());
        expect_that!(entered.end_annotations, is_empty());
        expect_false!(entered.has_source_location);
    }

    // The lifetime slice carries them all.
    let lifetime = lifetime_slice_named(&trace, &slices, "detailed")?;
    expect_eq!(annotation_names(&lifetime.begin_annotations), ["initial"]);
    expect_eq!(annotation_names(&lifetime.end_annotations), ["late"]);
    expect_true!(lifetime.has_source_location);
    Ok(())
}

/// An exit of a span that was never entered on the thread ends no slice.
///
/// Ending the innermost slice on such an exit would cut short the slice of a span that is
/// still entered, and writing an end for the unmatched span would close nothing.
#[gtest]
fn unmatched_exit_ends_no_slice() -> Result<()> {
    let (layer, output) = annotated_layer();
    let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));

    let entered = trace_span!("entered");
    let never_entered = trace_span!("never_entered");
    let entered_guard = entered.enter();
    let never_entered_id = never_entered.id().or_fail()?;
    tracing::dispatcher::get_default(|dispatch| dispatch.exit(&never_entered_id));
    tracing::info!(parent: None, marker = "after the unmatched exit");
    drop(entered_guard);

    let trace = decode(&output)?;
    let slices = replay_slices(&trace)?;
    let [thread_track] = thread_track_uuids(&trace)[..] else {
        return fail!("expected one thread track");
    };

    // The thread's track has one end, which closes the entered slice at its own exit.
    let ends_on_thread = trace
        .packet
        .iter()
        .filter_map(track_event)
        .filter(|event| {
            event.r#type() == track_event::Type::SliceEnd && event.track_uuid == Some(thread_track)
        })
        .count();
    expect_eq!(ends_on_thread, 1);
    let on_thread = slices_on(&slices, thread_track);
    let [entered_slice] = on_thread[..] else {
        return fail!("expected one slice on the thread track");
    };
    expect_eq!(entered_slice.name, "entered");
    let marker = instant_marked(&trace, "after the unmatched exit")?.timestamp;
    expect_that!(entered_slice.end_timestamp.or_fail()?, ge(marker));
    Ok(())
}

/// An exit after the trace is finished writes nothing, leaving the entered slice unfinished.
///
/// A finished trace is complete as written, so a later end would land after the trace was
/// handed off.
#[gtest]
fn exit_after_finish_writes_nothing() -> Result<()> {
    let (layer, output) = annotated_layer();
    let finish = layer.finish_handle();
    let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));

    let span = trace_span!("finished");
    let entered = span.enter();
    finish.finish();
    let finished_length = output.buf.lock().len();
    drop(entered);
    drop(span);

    expect_eq!(output.buf.lock().len(), finished_length);
    let trace = decode(&output)?;
    let slices = replay_slices(&trace)?;
    let [thread_track] = thread_track_uuids(&trace)[..] else {
        return fail!("expected one thread track");
    };
    expect_that!(
        nesting(&slices_on(&slices, thread_track)),
        elements_are![eq(&("finished", 0, false))]
    );
    Ok(())
}

/// A span still entered when its thread exits keeps an unfinished slice on the thread's track.
///
/// The span never exited, so an end written at thread exit would invent a duration, just as
/// for a lifetime slice that never closes.
#[gtest]
fn span_entered_at_thread_exit_keeps_an_unfinished_slice() -> Result<()> {
    let (layer, output) = annotated_layer();
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(layer));
    let span = tracing::dispatcher::with_default(&dispatch, || trace_span!("left_entered"));

    // The guard is forgotten, so the thread exits with the span entered.
    enter_on_thread(&dispatch, "exiting thread", || {
        std::mem::forget(span.enter());
    })?;
    drop(span);

    let trace = decode(&output)?;
    let slices = replay_slices(&trace)?;
    let thread_track = thread_track_named(&trace, "exiting thread")?;
    expect_that!(
        nesting(&slices_on(&slices, thread_track)),
        elements_are![eq(&("left_entered", 0, false))]
    );
    Ok(())
}

/// A thread that takes over the per-thread slot of an exited thread starts from a fresh state.
///
/// Per-thread slots are reused once their thread exits, so a later thread that inherited the
/// exited thread's state would leave its own track undescribed and end the exited thread's
/// entered slices on its own track.
#[gtest]
fn later_thread_does_not_inherit_the_state_of_an_exited_thread() -> Result<()> {
    let (layer, output) = annotated_layer();
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(layer));
    let left_entered = tracing::dispatcher::with_default(&dispatch, || trace_span!("left_entered"));
    let left_entered_id = left_entered.id().or_fail()?;

    // The first thread describes its track and exits with the span still entered.
    enter_on_thread(&dispatch, "exited thread", || {
        tracing::info!(parent: None, "describes the exited thread");
        std::mem::forget(left_entered.enter());
    })?;

    // The later thread exits that span, which it never entered, while its own span is entered.
    enter_on_thread(&dispatch, "later thread", || {
        tracing::info!(parent: None, "on the later thread");
        trace_span!("own").in_scope(|| {
            tracing::dispatcher::get_default(|current| current.exit(&left_entered_id));
        });
    })?;
    drop(left_entered);

    // Every slice on every track nests, and the later thread's track is described.
    let trace = decode(&output)?;
    let slices = replay_slices(&trace)?;
    let later_track = thread_track_named(&trace, "later thread")?;
    expect_that!(
        nesting(&slices_on(&slices, later_track)),
        elements_are![eq(&("own", 0, true))]
    );
    let exited_track = thread_track_named(&trace, "exited thread")?;
    expect_that!(
        nesting(&slices_on(&slices, exited_track)),
        elements_are![eq(&("left_entered", 0, false))]
    );
    Ok(())
}

/// Two layers on one registry each keep their own stack of entered spans.
///
/// A stack shared by the layers would let one layer's exit match the other layer's entry, so a
/// layer would end and resume slices of a span it never recorded.
#[gtest]
fn layers_on_one_registry_keep_separate_entered_stacks() -> Result<()> {
    let first_writer = TestWriter::new();
    let first_output = first_writer.make_writer();
    let second_writer = TestWriter::new();
    let second_output = second_writer.make_writer();
    let subscriber = tracing_subscriber::registry()
        .with(PerfettoLayer::new(first_writer).with_filter_by_marker(|name| name == "first_layer"))
        .with(PerfettoLayer::new(second_writer));
    let _guard = tracing::subscriber::set_default(subscriber);

    // Only the second layer records `second_only`, and `both` exits while it is entered.
    let both = trace_span!("both", first_layer = true);
    let second_only = trace_span!("second_only");
    let both_entered = both.enter();
    let second_only_entered = second_only.enter();
    drop(both_entered);
    drop(second_only_entered);
    drop(second_only);
    drop(both);

    let first = decode(&first_output)?;
    let first_slices = replay_slices(&first)?;
    let [first_thread_track] = thread_track_uuids(&first)[..] else {
        return fail!("expected one thread track in the first trace");
    };
    expect_that!(
        nesting(&slices_on(&first_slices, first_thread_track)),
        elements_are![eq(&("both", 0, true))]
    );

    let second = decode(&second_output)?;
    let second_slices = replay_slices(&second)?;
    let [second_thread_track] = thread_track_uuids(&second)[..] else {
        return fail!("expected one thread track in the second trace");
    };
    expect_that!(
        nesting(&slices_on(&second_slices, second_thread_track)),
        elements_are![
            eq(&("both", 0, true)),
            eq(&("second_only", 1, true)),
            eq(&("second_only", 0, true))
        ]
    );
    Ok(())
}

/// Many concurrent Tokio tasks polled across worker threads keep valid stacks on every track,
/// and each span's polls link to its own lifetime slice.
///
/// Tasks migrate between workers and interleave their polls, so an exit matched against
/// another task's or thread's entry would end the wrong slice, and a missing correlation ID
/// would detach the polls from their task.
#[gtest]
fn polls_of_concurrent_tokio_tasks_keep_every_track_nested() -> Result<()> {
    const TASKS: usize = 32;
    let (layer, output) = annotated_layer();
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(layer));
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .build()
        .or_fail()?;

    // Each task yields between instrumented steps, then waits until every task is open.
    let all_open = Arc::new(tokio::sync::Barrier::new(TASKS));
    let job = |all_open: Arc<tokio::sync::Barrier>| async move {
        for _ in 0..3 {
            tokio::task::yield_now().await;
            tokio::task::yield_now()
                .instrument(trace_span!("step"))
                .await;
        }
        all_open.wait().await;
    };
    tracing::dispatcher::with_default(&dispatch, || {
        runtime.block_on(async {
            let tasks: Vec<_> = (0..TASKS)
                .map(|_| {
                    let span = trace_span!("job", { "perfetto.track_name" } = "jobs");
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

    // Every track, lifetime and thread alike, holds a valid stack of slices.
    let trace = decode(&output)?;
    let slices = replay_slices(&trace)?;
    let lifetime_tracks = lifetime_track_uuids(&trace);
    let thread_tracks: HashSet<u64> = thread_track_uuids(&trace).into_iter().collect();
    let (lifetime_slices, entered_slices): (Vec<_>, Vec<_>) = slices
        .iter()
        .partition(|slice| lifetime_tracks.contains(&slice.track_uuid));
    expect_that!(lifetime_slices, len(eq(4 * TASKS)));
    expect_that!(
        entered_slices,
        each(predicate(|slice: &&ReplayedSlice| {
            thread_tracks.contains(&slice.track_uuid) && slice.end_timestamp.is_some()
        }))
    );

    // Every task and every step has an ID of its own, and the polls of each link to it alone.
    let lifetime_ids: HashSet<(&str, Option<u64>)> = lifetime_slices
        .iter()
        .map(|slice| (slice.name.as_str(), slice.correlation_id))
        .collect();
    expect_that!(lifetime_ids, len(eq(4 * TASKS)));
    let entered_ids: HashSet<(&str, Option<u64>)> = entered_slices
        .iter()
        .map(|slice| (slice.name.as_str(), slice.correlation_id))
        .collect();
    expect_eq!(entered_ids, lifetime_ids);
    Ok(())
}
