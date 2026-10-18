//! What a layer keeps of each span it records, including the track of the span's lifetime slice.
//!
//! Lifetime slices on one track must form a stack, so a span nests on its parent's track only
//! while no other child of that parent is open there.
//! [`place_span`] claims the parent's track and [`LifetimeSlice::close`] frees it, and because a
//! child always closes before its parent, the parent's slice is then the innermost open slice on
//! that track.

use crate::layer::LayerId;
use crate::{idl, packets};
use std::borrow::Cow;
use tracing::{Id, Subscriber};
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::{ExtensionsMut, LookupSpan, SpanRef};

/// What a [`PerfettoLayer`](crate::PerfettoLayer) keeps of a span it records.
pub(crate) struct SpanState {
    /// The name of the span's lifetime slice and of its entered slices.
    pub(crate) slice_name: Cow<'static, str>,
    /// The ID that links the span's entered slices to each other and to its lifetime slice.
    ///
    /// It is random and the span's own, so it never links a span to a parent whose track it
    /// shares.
    pub(crate) correlation_id: u64,
    /// The span's lifetime slice, if span lifetimes are recorded.
    pub(crate) lifetime: Option<LifetimeSlice>,
    /// The fields given at the span's creation, written on each entered slice's begin.
    ///
    /// This stays empty while span lifetimes are recorded, because the lifetime slice's begin
    /// carries these fields instead.
    pub(crate) creation_annotations: Vec<idl::DebugAnnotation>,
    /// The fields recorded after the span's creation.
    ///
    /// They are written on the lifetime slice's end, or, without span lifetimes, on the begin of
    /// each slice entered after the record.
    pub(crate) recorded_annotations: Vec<idl::DebugAnnotation>,
}

/// The lifetime slice of a span that a [`PerfettoLayer`](crate::PerfettoLayer) has begun and not
/// yet ended.
pub(crate) struct LifetimeSlice {
    /// The track that holds the span's slice.
    pub(crate) track_uuid: u64,
    /// The number of the span's open children whose slices nest on the span's track.
    ///
    /// A new child nests on the track only while this is 0, so the span's slice is then the
    /// innermost open slice there.
    open_children_on_track: usize,
    /// The span whose track this span's slice nests on, if any.
    ///
    /// This span holds one count of that span's `open_children_on_track` until it closes.
    track_parent: Option<Id>,
}

impl LifetimeSlice {
    /// Frees the track that this slice shares with its track parent, once its end is written.
    ///
    /// Freeing the track only after the end lets a sibling that nests there next begin after
    /// this slice ended.
    pub(crate) fn close<S>(self, ctx: &Context<'_, S>, layer_id: LayerId)
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
    {
        if let Some(track_parent_id) = self.track_parent
            && let Some(track_parent) = ctx.span(&track_parent_id)
            && let Some(states) = track_parent.extensions_mut().get_mut::<SpanStates>()
            && let Some(track_parent_state) = states
                .get_mut(layer_id)
                .and_then(|state| state.lifetime.as_mut())
        {
            track_parent_state.open_children_on_track =
                track_parent_state.open_children_on_track.saturating_sub(1);
        }
    }
}

/// Where a new span's lifetime slice goes.
pub(crate) struct SpanPlacement {
    /// The slice, on the track it was placed on.
    pub(crate) lifetime: LifetimeSlice,
    /// The descriptor of a track created for this span, written with the slice's begin.
    pub(crate) new_track: Option<idl::TrackDescriptor>,
}

impl SpanPlacement {
    /// Places the slice on a new track below `parent_uuid`.
    ///
    /// The track's UUID is never reused, so concurrent spans of one name never share a track.
    fn new_track(parent_uuid: u64, name: &str) -> Self {
        let track_uuid = packets::unique_uuid();
        Self {
            lifetime: LifetimeSlice {
                track_uuid,
                open_children_on_track: 0,
                track_parent: None,
            },
            new_track: Some(idl::TrackDescriptor {
                uuid: Some(track_uuid),
                parent_uuid: Some(parent_uuid),
                static_or_dynamic_name: Some(packets::track_name(name)),
                ..Default::default()
            }),
        }
    }
}

/// Chooses the track of a new span's lifetime slice for the layer `layer_id`.
///
/// A span with `own_track_name` gets a new track of that name below the process track.
/// Otherwise the span nests on the track of its nearest recorded ancestor while no other child
/// of that ancestor is open there, and claims that track until it closes.
/// A span that would overlap such a sibling gets a new track below the ancestor's track, and a
/// root span a new track below the process track, both named after the span.
pub(crate) fn place_span<S>(
    span: &SpanRef<'_, S>,
    layer_id: LayerId,
    process_track_uuid: u64,
    own_track_name: Option<&str>,
) -> SpanPlacement
where
    S: for<'a> LookupSpan<'a>,
{
    if let Some(track_name) = own_track_name {
        return SpanPlacement::new_track(process_track_uuid, track_name);
    }

    for ancestor in span.scope().skip(1) {
        let mut extensions = ancestor.extensions_mut();
        let Some(ancestor_state) = extensions
            .get_mut::<SpanStates>()
            .and_then(|states| states.get_mut(layer_id))
            .and_then(|state| state.lifetime.as_mut())
        else {
            continue;
        };

        // Nest on the ancestor's track
        if ancestor_state.open_children_on_track == 0 {
            ancestor_state.open_children_on_track = 1;
            return SpanPlacement {
                lifetime: LifetimeSlice {
                    track_uuid: ancestor_state.track_uuid,
                    open_children_on_track: 0,
                    track_parent: Some(ancestor.id()),
                },
                new_track: None,
            };
        }

        // Overlap a sibling
        return SpanPlacement::new_track(ancestor_state.track_uuid, span.name());
    }

    // Root span
    SpanPlacement::new_track(process_track_uuid, span.name())
}

/// The span states of every [`PerfettoLayer`](crate::PerfettoLayer) on a registry, stored as one
/// span extension.
///
/// [`ExtensionsMut::insert`] panics when a span already holds a value of the same type, so the
/// layers share this container and each one touches only the entry with its own [`LayerId`].
pub(crate) struct SpanStates {
    entries: Vec<LayerSpanState>,
}

/// The state that one layer keeps of a span.
struct LayerSpanState {
    layer_id: LayerId,
    state: SpanState,
}

impl SpanStates {
    /// Adds `state` as the entry of `layer_id`, creating the container on the span's first entry.
    pub(crate) fn insert(extensions: &mut ExtensionsMut<'_>, layer_id: LayerId, state: SpanState) {
        let entry = LayerSpanState { layer_id, state };
        if let Some(states) = extensions.get_mut::<Self>() {
            states.entries.push(entry);
        } else {
            extensions.insert(Self {
                entries: vec![entry],
            });
        }
    }

    pub(crate) fn get(&self, layer_id: LayerId) -> Option<&SpanState> {
        self.entries
            .iter()
            .find(|entry| entry.layer_id == layer_id)
            .map(|entry| &entry.state)
    }

    pub(crate) fn get_mut(&mut self, layer_id: LayerId) -> Option<&mut SpanState> {
        self.entries
            .iter_mut()
            .find(|entry| entry.layer_id == layer_id)
            .map(|entry| &mut entry.state)
    }

    /// Removes and returns the entry of `layer_id`, leaving the other layers' entries in place.
    pub(crate) fn remove(&mut self, layer_id: LayerId) -> Option<SpanState> {
        let index = self
            .entries
            .iter()
            .position(|entry| entry.layer_id == layer_id)?;
        Some(self.entries.swap_remove(index).state)
    }
}
