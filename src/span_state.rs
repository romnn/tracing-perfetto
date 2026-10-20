//! What a layer keeps of each span it records, including the track of the span's lifetime slice.
//!
//! Lifetime slices on one track must form a stack, so a span nests on its parent's track only
//! while no other child of that parent is open there.
//! [`place_span`] claims the parent's track and [`LifetimeSlice::close`] frees it, and because a
//! child always closes before its parent, the parent's slice is then the innermost open slice on
//! that track.
//! That order holds only while spans close under their own dispatcher: a child that closes under
//! another default dispatcher releases its parent through that dispatcher, which can leave the
//! parent open or close an unrelated span of the same ID in another registry ([#9]).
//!
//! [#9]: https://github.com/romnn/tracing-perfetto/issues/9

use crate::packets::{SourceLocation, UnplacedEvent};
use crate::{idl, packets};
use std::borrow::Cow;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::{Id, Metadata, Subscriber};
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::{ExtensionsMut, LookupSpan, SpanRef};

/// Identifies one [`PerfettoLayer`](crate::PerfettoLayer) among all layers in the process, which
/// selects its entry in the [`SpanStates`] of a span.
///
/// IDs are never reused, so an entry of a dropped layer never applies to a later layer.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct LayerId(u64);

impl LayerId {
    /// Returns an ID that no other layer in the process has.
    pub(crate) fn next() -> Self {
        static NEXT_LAYER_ID: AtomicU64 = AtomicU64::new(0);
        Self(NEXT_LAYER_ID.fetch_add(1, Ordering::Relaxed))
    }
}

/// What a [`PerfettoLayer`](crate::PerfettoLayer) keeps of a span it records.
pub(crate) struct SpanState {
    /// The name of the span's lifetime slice and of its entered slices.
    pub(crate) slice_name: Cow<'static, str>,
    /// The ID that links the span's entered slices to each other and to its lifetime slice.
    ///
    /// It is unique to the span, so it never links a span to a parent whose track it
    /// shares.
    pub(crate) correlation_id: u64,
    /// The slices that carry the span's fields.
    pub(crate) kind: SpanKind,
    /// The fields recorded after the span's creation.
    ///
    /// They are written on the lifetime slice's end, or, without span lifetimes, on the begin of
    /// each slice entered after the record.
    pub(crate) recorded_annotations: Vec<idl::DebugAnnotation>,
}

impl SpanState {
    /// Returns the span's lifetime slice, if span lifetimes are recorded.
    pub(crate) fn lifetime(&self) -> Option<&LifetimeSlice> {
        match &self.kind {
            SpanKind::Lifetime(lifetime) => Some(lifetime),
            SpanKind::Entered { .. } => None,
        }
    }

    fn lifetime_mut(&mut self) -> Option<&mut LifetimeSlice> {
        match &mut self.kind {
            SpanKind::Lifetime(lifetime) => Some(lifetime),
            SpanKind::Entered { .. } => None,
        }
    }

    /// Builds the begin of a slice of the span entered on a thread.
    ///
    /// `metadata` is the span's, whose source location the begin carries if the span has no
    /// lifetime slice to carry it.
    pub(crate) fn entered_slice_begin(
        &self,
        metadata: &'static Metadata<'static>,
    ) -> UnplacedEvent {
        let (debug_annotations, location) = match &self.kind {
            SpanKind::Lifetime(_) => (Vec::new(), None),
            SpanKind::Entered {
                creation_annotations,
            } => (
                creation_annotations
                    .iter()
                    .chain(&self.recorded_annotations)
                    .cloned()
                    .collect(),
                SourceLocation::of(metadata),
            ),
        };
        UnplacedEvent {
            event_type: idl::track_event::Type::SliceBegin,
            name: Some(self.slice_name.clone()),
            location,
            debug_annotations,
            correlation_id: Some(self.correlation_id),
        }
    }
}

/// How a span is recorded, which decides the slices that carry its fields and source location.
pub(crate) enum SpanKind {
    /// The span has a lifetime slice from its creation to its close, which carries its fields and
    /// source location, so its entered slices carry neither.
    Lifetime(LifetimeSlice),
    /// The span has only entered slices, and the begin of each carries its fields and source
    /// location.
    Entered {
        /// The fields given at the span's creation.
        creation_annotations: Vec<idl::DebugAnnotation>,
    },
}

/// The lifetime slice of a span that a [`PerfettoLayer`](crate::PerfettoLayer) has begun and not
/// yet ended.
pub(crate) struct LifetimeSlice {
    /// The track that holds the span's slice.
    pub(crate) track_uuid: u64,
    /// Whether the slice of one of the span's open children nests on the span's track.
    ///
    /// A new child nests on the track only while it is free, so the span's slice is then the
    /// innermost open slice there.
    track_occupied: bool,
    /// The span whose track this span's slice nests on, if any.
    ///
    /// This span occupies that span's track until it closes.
    occupies: Option<Id>,
}

impl LifetimeSlice {
    /// Frees the track that this slice occupies, once its end is written.
    ///
    /// Freeing the track only after the end lets a sibling that nests there next begin after
    /// this slice ended.
    pub(crate) fn close<S>(self, ctx: &Context<'_, S>, layer_id: LayerId)
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
    {
        // The registry closes every child before its parent, and a child frees the track when it
        // closes.
        debug_assert!(
            !self.track_occupied,
            "a span closed while a child's slice nests on its track, as when spans close under \
             another dispatcher (https://github.com/romnn/tracing-perfetto/issues/9)"
        );
        if let Some(occupied_id) = self.occupies
            && let Some(occupied) = ctx.span(&occupied_id)
            && let Some(states) = occupied.extensions_mut().get_mut::<SpanStates>()
            && let Some(occupied_lifetime) =
                states.get_mut(layer_id).and_then(SpanState::lifetime_mut)
        {
            occupied_lifetime.track_occupied = false;
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
                track_occupied: false,
                occupies: None,
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
            .and_then(SpanState::lifetime_mut)
        else {
            continue;
        };

        // Nest on the ancestor's track
        if !ancestor_state.track_occupied {
            ancestor_state.track_occupied = true;
            return SpanPlacement {
                lifetime: LifetimeSlice {
                    track_uuid: ancestor_state.track_uuid,
                    track_occupied: false,
                    occupies: Some(ancestor.id()),
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

/// Returns the lifetime track of the nearest span that the layer `layer_id` records, among
/// `span` and its ancestors.
///
/// Only spans that the layer records hold its state, so `span` may come from a lookup that sees
/// the spans its filter rejected as well.
pub(crate) fn nearest_lifetime_track<R>(span: &SpanRef<'_, R>, layer_id: LayerId) -> Option<u64>
where
    R: for<'a> LookupSpan<'a>,
{
    span.scope().find_map(|span| {
        span.extensions()
            .get::<SpanStates>()
            .and_then(|states| states.get(layer_id))
            .and_then(SpanState::lifetime)
            .map(|lifetime| lifetime.track_uuid)
    })
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
