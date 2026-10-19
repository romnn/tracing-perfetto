//! Traces nested spans across Tokio tasks into `tokio_example.pftrace` in the temporary directory.

use std::time::Duration;

use tracing::{Instrument as _, info, info_span};
use tracing_perfetto::PerfettoLayer;
use tracing_subscriber::fmt::format::Format;
use tracing_subscriber::{Registry, fmt, layer::SubscriberExt};

/// Pause between traced steps, long enough that each span is visible in the trace viewer.
const STEP: Duration = Duration::from_millis(250);

fn init_subscriber() -> Result<(), Box<dyn std::error::Error>> {
    let trace_path = std::env::temp_dir().join("tokio_example.pftrace");
    let trace_file = std::fs::File::create(&trace_path)?;
    let perfetto_layer =
        PerfettoLayer::new(std::sync::Mutex::new(trace_file)).with_debug_annotations(true);

    let fmt_layer = fmt::layer()
        .with_writer(std::io::stdout)
        .event_format(Format::default().with_thread_ids(true))
        .with_span_events(fmt::format::FmtSpan::FULL);

    let subscriber = Registry::default().with(fmt_layer).with(perfetto_layer);

    tracing::subscriber::set_global_default(subscriber)?;
    Ok(())
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_subscriber()?;
    // The span is attached to the future rather than entered, so it is not held entered across
    // an `.await`, where it would wrongly cover whatever else the worker thread polls meanwhile.
    run().instrument(info_span!("main")).await
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    info!("start tokio example with tracing");

    let mut join_handles = Vec::new();

    // Instrument each async task with a dedicated span.
    // The tasks share one track name, so the trace viewer shows them as lanes of one track.
    for i in 0..5 {
        let span = info_span!("task", i, perfetto.track_name = "jobs");
        join_handles.push(tokio::spawn(task(i).instrument(span)));
    }

    // Attach each async task to the span entered around its spawn.
    // This suits a task that should not get a span of its own, and works without an entered span
    // too.
    for i in 5..10 {
        let _entered =
            info_span!("task", i, perfetto.track_name = format!("task track {i}")).entered();
        join_handles.push(tokio::spawn(task(i).in_current_span()));
    }

    for join_handle in join_handles {
        join_handle.await?;
    }
    info!("end tokio example with tracing");

    Ok(())
}

/// Runs three steps, each in a span that is entered whenever its future is polled.
async fn task(i: u32) {
    info!("task {i} started");
    for j in 0..3 {
        async {
            tokio::time::sleep(STEP).await;
            info!("task {i} inner loop");
            tokio::time::sleep(STEP).await;
        }
        .instrument(info_span!("loop", i, j))
        .await;
    }
    tokio::time::sleep(STEP).await;
    info!("task {i} finished");
}
