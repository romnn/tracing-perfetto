//! Traces nested spans across Tokio tasks into `tokio_example.pftrace` in the temporary directory.

use std::time::Duration;

use tracing::{Instrument, info, span};
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
    let _main_span = span!(tracing::Level::INFO, "main").entered();
    info!("start tokio example with tracing");

    let mut join_handles = Vec::new();

    // Instrument each async task with a dedicated span.
    // The tasks share one track name, so the trace viewer shows them as lanes of one track.
    for i in 0..5 {
        let task = async move {
            tokio::time::sleep(STEP).await;
            for j in 0..3 {
                let _span = span!(tracing::Level::INFO, "loop", i, j);

                tokio::time::sleep(STEP).await;
                info!("task ${i} inner loop");
                tokio::time::sleep(STEP).await;
            }
            tokio::time::sleep(STEP).await;
            info!("task ${i} finished");
        };

        let span = span!(
            tracing::Level::INFO,
            "task",
            i,
            perfetto.track_name = "jobs"
        );
        let jh = tokio::spawn(task.instrument(span.or_current()));
        join_handles.push(jh);
    }

    // Attach each async task to the span entered around its spawn.
    // This suits a task that should not get a span of its own, and works without an entered span
    // too.
    for i in 5..10 {
        let task = async move {
            info!("task ${i} started");
            for j in 0..3 {
                let _span = span!(tracing::Level::INFO, "loop", i, j);

                tokio::time::sleep(STEP).await;
                info!("task ${i} inner loop");
                tokio::time::sleep(STEP).await;
            }
            tokio::time::sleep(STEP).await;
            info!("task ${i} finished");
        };

        let _guard = span!(
            tracing::Level::INFO,
            "task",
            i,
            perfetto.track_name = format!("task track {}", i)
        )
        .entered();
        // Do some work on the span.
        let jh = tokio::spawn(task.in_current_span());
        // Do some work on the span.
        join_handles.push(jh);
    }

    for jh in join_handles {
        jh.await?;
    }
    info!("end tokio example with tracing");

    Ok(())
}
