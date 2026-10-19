//! Regression tests for spans that close under a dispatcher other than the one that recorded
//! them ([#9]).
//!
//! tracing-subscriber 0.3.23 releases a closing span's reference to its parent through the
//! default dispatcher of the closing thread instead of the span's own
//! ([tokio-rs/tracing#2870]), so the parent's close never reaches the layer.
//! The tests that pin this behavior fail once a tracing-subscriber release fixes it, which is
//! the signal to drop the documented limitation.
//!
//! [#9]: https://github.com/romnn/tracing-perfetto/issues/9
//! [tokio-rs/tracing#2870]: https://github.com/tokio-rs/tracing/issues/2870

use crate::tests::{
    ReplayedSlice, annotated_layer, close_labeled, decode, replay_lifetime_slices, slice_named,
    span_track_descriptors, track_name,
};
use googletest::prelude::*;
use std::panic::AssertUnwindSafe;
use tracing::{Dispatch, field, trace_span};
use tracing_subscriber::layer::SubscriberExt;

/// A root span and the child of its child, recorded by one dispatcher.
///
/// The middle span, `parent`, has no handle left, so `child` alone keeps it open.
struct Family {
    root: tracing::Span,
    child: tracing::Span,
}

/// Records `root`, `parent` and `child` under `dispatch` and drops the handle of `parent`.
///
/// The handle records `parent` into its `closes` field first, so the end of its slice, once
/// written, is checked to close it.
fn family_with_released_parent(dispatch: &Dispatch) -> Family {
    tracing::dispatcher::with_default(dispatch, || {
        let root = trace_span!("root", closes = field::Empty);
        let parent = trace_span!(parent: &root, "parent", closes = field::Empty);
        let child = trace_span!(parent: &parent, "child", closes = field::Empty);
        close_labeled(parent, "parent");
        Family { root, child }
    })
}

/// Records a `sibling` of `parent` below `root` under `dispatch`, then closes both.
fn close_root_after_sibling(dispatch: &Dispatch, root: tracing::Span) {
    tracing::dispatcher::with_default(dispatch, || {
        let sibling = trace_span!(parent: &root, "sibling", closes = field::Empty);
        close_labeled(sibling, "sibling");
        close_labeled(root, "root");
    });
}

/// Returns whether the slice's end is in the trace.
fn ended(slice: &ReplayedSlice) -> bool {
    slice.end_timestamp.is_some()
}

/// A child that closes on another thread under its own dispatcher ends its released parent's
/// lifetime slice, which frees the grandparent's track for the parent's next sibling.
///
/// This is the documented way to close spans of a scoped subscriber on another thread.
/// Without the dispatcher set where the child closes, the parent never closes, as the tests
/// below pin.
#[gtest]
fn child_closed_under_its_own_dispatcher_ends_its_released_parent() -> Result<()> {
    let (layer, output) = annotated_layer();
    let dispatch = Dispatch::new(tracing_subscriber::registry().with(layer));
    let Family { root, child } = family_with_released_parent(&dispatch);

    // The closing thread has no default of its own, so it sets the span's dispatcher.
    std::thread::scope(|scope| {
        scope
            .spawn(|| {
                tracing::dispatcher::with_default(&dispatch, || close_labeled(child, "child"));
            })
            .join()
    })
    .or_fail()?;
    close_root_after_sibling(&dispatch, root);

    let trace = decode(&output)?;
    let slices = replay_lifetime_slices(&trace)?;
    let root = slice_named(&slices, "root")?;

    // Every slice ends, so the child's close released the parent and the parent's released
    // the root.
    for name in ["root", "parent", "child", "sibling"] {
        expect_true!(ended(slice_named(&slices, name)?), "{name} ended");
    }

    // The parent freed the root's track when it closed, so its sibling nests there as well.
    let parent = slice_named(&slices, "parent")?;
    let sibling = slice_named(&slices, "sibling")?;
    expect_eq!(parent.track_uuid, root.track_uuid);
    expect_eq!(sibling.track_uuid, root.track_uuid);
    expect_eq!(sibling.depth, 1);
    Ok(())
}

/// A child that closes under no dispatcher leaves its released parent's lifetime slice, and
/// every ancestor's, without an end, and moves the parent's next sibling to a sub-track.
///
/// The registry releases the parent through the closing thread's default, here the no-op
/// dispatcher, so the parent stays open, keeps holding the root open, and keeps the root's track
/// claimed ([#9]).
/// This pins the documented limitation: once tracing-subscriber releases the parent through the
/// child's own dispatcher, this test fails and the limitation can go.
///
/// [#9]: https://github.com/romnn/tracing-perfetto/issues/9
#[gtest]
fn child_closed_under_no_dispatcher_leaves_its_released_parent_open() -> Result<()> {
    let (layer, output) = annotated_layer();
    let dispatch = Dispatch::new(tracing_subscriber::registry().with(layer));
    let Family { root, child } = family_with_released_parent(&dispatch);

    // A runtime worker without a default of its own closes the span under the no-op dispatcher
    // too, unless a global default is set.
    // Setting it explicitly keeps the test independent of any global default.
    tracing::dispatcher::with_default(&Dispatch::none(), || close_labeled(child, "child"));
    close_root_after_sibling(&dispatch, root);

    let trace = decode(&output)?;
    let slices = replay_lifetime_slices(&trace)?;
    let root = slice_named(&slices, "root")?;

    // The child's close reaches the layer through the child's own dispatcher, but its parent's
    // never does, nor the root's that the parent holds.
    expect_true!(ended(slice_named(&slices, "child")?));
    expect_false!(ended(slice_named(&slices, "parent")?));
    expect_false!(ended(root));

    // The open parent still claims the root's track, so its sibling moves to a sub-track of it.
    let sibling = slice_named(&slices, "sibling")?;
    expect_true!(ended(sibling));
    expect_ne!(sibling.track_uuid, root.track_uuid);
    let sibling_track = span_track_descriptors(&trace)
        .into_iter()
        .find(|descriptor| descriptor.uuid == Some(sibling.track_uuid))
        .or_fail()?;
    expect_eq!(track_name(sibling_track), Some("sibling"));
    expect_eq!(sibling_track.parent_uuid, Some(root.track_uuid));
    Ok(())
}

/// A child that closes while an unrelated registry is the default panics in that registry, after
/// the child's slice ended, and leaves its released parent's lifetime slice without an end.
///
/// The registry releases the parent through the closing thread's default, here a registry that
/// has no span of the parent's ID, whose `try_close` panics on the unknown ID ([#9]).
/// This pins the documented limitation: once tracing-subscriber releases the parent through the
/// child's own dispatcher, this test fails and the limitation can go.
///
/// [#9]: https://github.com/romnn/tracing-perfetto/issues/9
#[gtest]
fn child_closed_under_an_unrelated_registry_panics_there() -> Result<()> {
    let (layer, output) = annotated_layer();
    let dispatch = Dispatch::new(tracing_subscriber::registry().with(layer));
    let Family { root, child } = family_with_released_parent(&dispatch);

    // The unrelated registry has recorded no span, so no ID can match one of its spans.
    let unrelated = Dispatch::new(tracing_subscriber::registry());
    let close = std::panic::catch_unwind(AssertUnwindSafe(|| {
        tracing::dispatcher::with_default(&unrelated, || close_labeled(child, "child"));
    }));
    let panic_message = close
        .err()
        .and_then(|payload| payload.downcast::<String>().ok())
        .or_fail()?;
    expect_that!(
        *panic_message,
        contains_substring("but no such span exists")
    );
    close_labeled(root, "root");

    let trace = decode(&output)?;
    let slices = replay_lifetime_slices(&trace)?;

    // The layer wrote the child's end before the registry released the parent, which panicked
    // and so never closed the parent, nor the root that the parent holds.
    expect_true!(ended(slice_named(&slices, "child")?));
    expect_false!(ended(slice_named(&slices, "parent")?));
    expect_false!(ended(slice_named(&slices, "root")?));
    Ok(())
}
