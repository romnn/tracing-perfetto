//! Regression tests for the counter tracks that the `counter.*` fields of events write.

use crate::PerfettoLayer;
use crate::idl;
use crate::idl::track_event::CounterValueField;
use crate::tests::{
    TestWriter, annotated_layer, annotation_names, annotation_value, decode, decode_writes,
    instant_marked, string_annotation, track_descriptor, track_event, track_name, wait_until,
};
use googletest::prelude::*;
use parking_lot::Mutex;
use prost::Message;
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Duration;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::layer::SubscriberExt;

/// A counter track of a trace, with the values on it in write order.
struct CounterTrack<'a> {
    descriptor: &'a idl::TrackDescriptor,
    values: Vec<CounterValueField>,
}

/// Returns the counter tracks named `name` in `trace`, one per descriptor.
fn counter_tracks<'a>(trace: &'a idl::Trace, name: &str) -> Vec<CounterTrack<'a>> {
    trace
        .packet
        .iter()
        .filter_map(track_descriptor)
        .filter(|descriptor| descriptor.counter.is_some() && track_name(descriptor) == Some(name))
        .map(|descriptor| CounterTrack {
            descriptor,
            values: counter_values(trace, descriptor.uuid()),
        })
        .collect()
}

/// Returns the one counter track named `name` in `trace`.
fn counter_track<'a>(trace: &'a idl::Trace, name: &str) -> Result<CounterTrack<'a>> {
    let named = counter_tracks(trace, name);
    let count = named.len();
    let Ok([counter]) = <[_; 1]>::try_from(named) else {
        return Err(format!(
            "expected one counter track named {name}, got {count}"
        ))
        .or_fail();
    };
    Ok(counter)
}

/// Returns the values of the counter events on the track `track_uuid`, in write order.
fn counter_values(trace: &idl::Trace, track_uuid: u64) -> Vec<CounterValueField> {
    trace
        .packet
        .iter()
        .filter_map(track_event)
        .filter(|event| {
            event.r#type() == idl::track_event::Type::Counter
                && event.track_uuid == Some(track_uuid)
        })
        .filter_map(|event| event.counter_value_field)
        .collect()
}

/// Returns whether `trace` has any counter descriptor or counter event.
fn has_counters(trace: &idl::Trace) -> bool {
    trace.packet.iter().any(|packet| {
        track_descriptor(packet).is_some_and(|descriptor| descriptor.counter.is_some())
            || track_event(packet)
                .is_some_and(|event| event.r#type() == idl::track_event::Type::Counter)
    })
}

/// Returns a layer without debug annotations, and a writer that shares its buffer.
fn plain_layer() -> (PerfettoLayer<TestWriter>, TestWriter) {
    let writer = TestWriter::new();
    let output = writer.make_writer();
    (PerfettoLayer::new(writer), output)
}

/// The unit that a counter track's descriptor declares.
#[derive(Debug, PartialEq)]
struct DeclaredUnit<'a> {
    unit: idl::counter_descriptor::Unit,
    unit_name: Option<&'a str>,
}

fn declared_unit(descriptor: &idl::TrackDescriptor) -> Option<DeclaredUnit<'_>> {
    let counter = descriptor.counter.as_ref()?;
    Some(DeclaredUnit {
        unit: counter.unit(),
        unit_name: counter.unit_name.as_deref(),
    })
}

/// Each `counter.<name>` field writes its values on one counter track below the process track,
/// which is described once, with the unit that the field's last segment names, in the message
/// that carries the track's first value.
///
/// The fields are written without debug annotations, so counters do not depend on them.
#[gtest]
fn each_counter_is_described_once_with_its_unit_and_holds_every_value() -> Result<()> {
    let (layer, output) = plain_layer();
    let process_track_uuid = layer.process_track_uuid();
    let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));

    for round in 1..=2_u32 {
        tracing::info!(
            counter.mem_usage.bytes = u64::from(round) * 1000,
            counter.latency.ns = i64::from(round) * 10,
            counter.requests.count = round,
            counter.hit_rate.percent = f64::from(round) / 4.0,
            counter.queue_depth = i64::from(round) - 3,
            "sampled"
        );
    }

    let trace = decode(&output)?;
    let mem_usage = counter_track(&trace, "mem_usage")?;
    let latency = counter_track(&trace, "latency")?;
    let requests = counter_track(&trace, "requests")?;
    let hit_rate = counter_track(&trace, "hit_rate")?;
    let queue_depth = counter_track(&trace, "queue_depth")?;

    // The known units map to the built-in unit, any other one is a free-form unit name, and a
    // counter without a unit segment declares none.
    expect_that!(
        declared_unit(mem_usage.descriptor),
        some(eq(&DeclaredUnit {
            unit: idl::counter_descriptor::Unit::SizeBytes,
            unit_name: None,
        }))
    );
    expect_that!(
        declared_unit(latency.descriptor),
        some(eq(&DeclaredUnit {
            unit: idl::counter_descriptor::Unit::TimeNs,
            unit_name: None,
        }))
    );
    expect_that!(
        declared_unit(requests.descriptor),
        some(eq(&DeclaredUnit {
            unit: idl::counter_descriptor::Unit::Count,
            unit_name: None,
        }))
    );
    expect_that!(
        declared_unit(hit_rate.descriptor),
        some(eq(&DeclaredUnit {
            unit: idl::counter_descriptor::Unit::Unspecified,
            unit_name: Some("percent"),
        }))
    );
    expect_that!(
        declared_unit(queue_depth.descriptor),
        some(eq(&DeclaredUnit {
            unit: idl::counter_descriptor::Unit::Unspecified,
            unit_name: None,
        }))
    );

    // Every value of both rounds is on its counter's track, in order.
    expect_eq!(
        mem_usage.values,
        [
            CounterValueField::CounterValue(1000),
            CounterValueField::CounterValue(2000)
        ]
    );
    expect_eq!(
        latency.values,
        [
            CounterValueField::CounterValue(10),
            CounterValueField::CounterValue(20)
        ]
    );
    expect_eq!(
        requests.values,
        [
            CounterValueField::CounterValue(1),
            CounterValueField::CounterValue(2)
        ]
    );
    expect_eq!(
        hit_rate.values,
        [
            CounterValueField::DoubleCounterValue(0.25),
            CounterValueField::DoubleCounterValue(0.5)
        ]
    );
    expect_eq!(
        queue_depth.values,
        [
            CounterValueField::CounterValue(-2),
            CounterValueField::CounterValue(-1)
        ]
    );

    // Every counter track is a child of the process track, so it is one series per process.
    for track in [&mem_usage, &latency, &requests, &hit_rate, &queue_depth] {
        expect_eq!(track.descriptor.parent_uuid, Some(process_track_uuid));
    }

    // A descriptor goes in the message that carries its track's first value, after the
    // descriptor, so no write can leave a value on an undescribed track.
    for message in decode_writes(&output)? {
        for (index, packet) in message.packet.iter().enumerate() {
            let Some(descriptor) =
                track_descriptor(packet).filter(|described| described.counter.is_some())
            else {
                continue;
            };
            let later = message.packet.get(index + 1..).unwrap_or_default();
            let carries_a_value = later.iter().filter_map(track_event).any(|event| {
                event.r#type() == idl::track_event::Type::Counter
                    && event.track_uuid == descriptor.uuid
            });
            expect_that!(
                carries_a_value,
                eq(true),
                "descriptor of {:?}",
                track_name(descriptor)
            );
        }
    }
    Ok(())
}

/// Integer counter values are written as integers where they fit an `i64`, and every other
/// numeric value as a double, at the timestamp of the event's instant, which still carries the
/// event's other fields but not its counters.
#[gtest]
fn counter_values_keep_their_magnitude_and_share_the_instant_timestamp() -> Result<()> {
    let (layer, output) = annotated_layer();
    let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));

    tracing::info!(
        marker = "types",
        counter.signed = -3_i64,
        counter.small_unsigned = 7_u64,
        counter.huge_unsigned = u64::MAX,
        counter.float = 1.5_f64,
        counter.small_wide = -5_i128,
        counter.huge_wide = u128::MAX,
    );

    let trace = decode(&output)?;
    let instant = instant_marked(&trace, "types")?;

    // A `u64` or 128-bit value beyond the `i64` range becomes the nearest double instead of a
    // wrapped or saturated integer.
    let expected = [
        ("signed", CounterValueField::CounterValue(-3)),
        ("small_unsigned", CounterValueField::CounterValue(7)),
        (
            "huge_unsigned",
            CounterValueField::DoubleCounterValue(2_f64.powi(64)),
        ),
        ("float", CounterValueField::DoubleCounterValue(1.5)),
        ("small_wide", CounterValueField::CounterValue(-5)),
        (
            "huge_wide",
            CounterValueField::DoubleCounterValue(2_f64.powi(128)),
        ),
    ];
    for (name, value) in expected {
        let track = counter_track(&trace, name)?;
        expect_eq!(track.values, [value], "values of {name}");

        // The counter value lands at the instant's timestamp.
        let timestamps: Vec<_> = trace
            .packet
            .iter()
            .filter(|packet| {
                track_event(packet).is_some_and(|event| {
                    event.r#type() == idl::track_event::Type::Counter
                        && event.track_uuid == track.descriptor.uuid
                })
            })
            .map(|packet| packet.timestamp)
            .collect();
        expect_eq!(
            timestamps,
            [Some(instant.timestamp)],
            "timestamps of {name}"
        );
    }

    // The counters are on their tracks, so the instant carries only the event's other fields.
    expect_eq!(
        annotation_names(&instant.event.debug_annotations),
        ["marker"]
    );
    Ok(())
}

/// A `counter.*` field whose value is not a number stays a debug annotation on the instant and
/// writes no counter.
#[gtest]
fn non_numeric_counter_fields_stay_annotations() -> Result<()> {
    let (layer, output) = annotated_layer();
    let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));

    tracing::info!(
        marker = "non-numeric",
        counter.state = "busy",
        counter.flag = true,
        counter.items = ?[1, 2],
    );

    let trace = decode(&output)?;
    let annotations = &instant_marked(&trace, "non-numeric")?
        .event
        .debug_annotations;
    expect_eq!(
        string_annotation(annotations, "counter.state"),
        Some("busy")
    );
    expect_eq!(
        string_annotation(annotations, "counter.items"),
        Some("[1, 2]")
    );
    expect_eq!(
        annotation_value(annotations, "counter.flag"),
        Some(&idl::debug_annotation::Value::BoolValue(true))
    );
    expect_that!(has_counters(&trace), eq(false));
    Ok(())
}

/// Values of one counter recorded on two threads are one series on one track.
#[gtest]
fn values_from_two_threads_share_one_counter_track() -> Result<()> {
    let (layer, output) = plain_layer();
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(layer));

    for value in [1_i64, 2] {
        let dispatch = dispatch.clone();
        thread::spawn(move || {
            tracing::dispatcher::with_default(&dispatch, || {
                tracing::info!(counter.depth = value, "depth");
            });
        })
        .join()
        .or_fail()?;
    }

    let trace = decode(&output)?;
    let depth = counter_track(&trace, "depth")?;
    expect_eq!(
        depth.values,
        [
            CounterValueField::CounterValue(1),
            CounterValueField::CounterValue(2)
        ]
    );
    Ok(())
}

/// How long a test waits for a step that a correct layer takes at once, which only detects a
/// step that never happens.
const STEP_BOUND: Duration = Duration::from_secs(10);

/// The release of a write that a [`HoldingWriter`] holds.
struct HoldingState {
    /// Announces, once, that a write holds a message.
    held: Mutex<Option<mpsc::Sender<()>>>,
    /// Releases the held write when the test sends on it or drops its sender, behind a mutex
    /// because the writer must be [`Sync`].
    release: Mutex<mpsc::Receiver<()>>,
}

/// A writer that holds the first message with a counter value inside its write until the test
/// releases it, and writes every message in full to `output`.
struct HoldingWriter {
    output: TestWriter,
    state: Arc<HoldingState>,
}

impl<'a> MakeWriter<'a> for HoldingWriter {
    type Writer = HoldingWriter;

    fn make_writer(&'a self) -> Self::Writer {
        HoldingWriter {
            output: self.output.make_writer(),
            state: Arc::clone(&self.state),
        }
    }
}

impl std::io::Write for HoldingWriter {
    /// Appends `buf`, after holding it until the test releases it if it is the first message
    /// with a counter value.
    ///
    /// The layer hands each message to one call, so `buf` is one whole `Trace` message.
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let has_a_counter_value = idl::Trace::decode(buf).is_ok_and(|trace| {
            trace
                .packet
                .iter()
                .filter_map(track_event)
                .any(|event| event.r#type() == idl::track_event::Type::Counter)
        });
        let held = has_a_counter_value
            .then(|| self.state.held.lock().take())
            .flatten();
        if let Some(held) = held {
            // The receiver is gone only after the test stopped waiting.
            // A test that fails drops the release's sender, which releases the write.
            held.send(()).ok();
            self.state.release.lock().recv().ok();
        }
        self.output.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.output.flush()
    }
}

/// Records `counter.depth = depth` on a new thread under `dispatch`.
fn record_depth_on_a_thread(dispatch: &tracing::Dispatch, depth: i64) -> thread::JoinHandle<()> {
    let dispatch = dispatch.clone();
    thread::spawn(move || {
        tracing::dispatcher::with_default(&dispatch, || {
            tracing::info!(counter.depth = depth, "depth");
        });
    })
}

/// Two threads that write the first value of one counter at the same time describe its track
/// once, and both values land on it.
///
/// The writer holds the first thread's message, which describes the track, until the second
/// thread waits to write its own first value.
/// A layer that decided whether to describe the track before the first message was written would
/// describe it again in the second message.
#[gtest]
fn concurrent_first_values_from_two_threads_describe_the_counter_once() -> Result<()> {
    let (held, held_receiver) = mpsc::channel();
    let (release, release_receiver) = mpsc::channel::<()>();
    let output = TestWriter::new();
    let reader = output.make_writer();
    let layer = PerfettoLayer::new(HoldingWriter {
        output,
        state: Arc::new(HoldingState {
            held: Mutex::new(Some(held)),
            release: Mutex::new(release_receiver),
        }),
    });
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(layer));
    let counter_contentions = || {
        dispatch
            .downcast_ref::<PerfettoLayer<HoldingWriter>>()
            .map_or(0, |layer| layer.event_counters().contentions())
    };

    // The writer holds the first thread's first value, and the second thread then waits to write
    // its own first value.
    let first = record_depth_on_a_thread(&dispatch, 1);
    held_receiver.recv_timeout(STEP_BOUND).or_fail()?;
    let second = record_depth_on_a_thread(&dispatch, 2);
    let second_waits = wait_until(STEP_BOUND, || counter_contentions() == 1);
    release.send(()).or_fail()?;
    first.join().or_fail()?;
    second.join().or_fail()?;
    if !second_waits {
        return fail!("the second value never waited for the first");
    }

    // The track is described once, and both values are on it.
    let trace = decode(&reader)?;
    let depth = counter_track(&trace, "depth")?;
    expect_eq!(
        depth.values,
        [
            CounterValueField::CounterValue(1),
            CounterValueField::CounterValue(2)
        ]
    );
    Ok(())
}
