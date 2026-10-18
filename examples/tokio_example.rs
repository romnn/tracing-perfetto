//! Traces nested spans across Tokio tasks into `test.pftrace` in the temporary directory.

use std::time::Duration;

use tracing::{Instrument, info, span};
use tracing_perfetto::PerfettoLayer;
use tracing_subscriber::fmt::format::Format;
use tracing_subscriber::{Registry, fmt, layer::SubscriberExt};

/// Pause between traced steps, long enough that each span is visible in the trace viewer.
const STEP: Duration = Duration::from_millis(250);

fn init_subscriber() -> Result<(), Box<dyn std::error::Error>> {
    let trace_path = std::env::temp_dir().join("test.pftrace");
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

    // example 1: instrument async handler chain with a dedicated span
    for i in 0..5 {
        let task = async move {
            // info!("task ${i} started");
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
            perfetto.track_name = format!("task track {}", i)
        );
        let jh = tokio::spawn(task.instrument(span.or_current()));
        join_handles.push(jh);
    }

    // example 2: attach an async handler chain to the current entered span
    // this can be useful if we spawn a task but do not want to create a new span for it
    // but to attach it to the current active span
    // (or do not have a span to attach to)
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
        // do some work on the span
        let jh = tokio::spawn(task.in_current_span());
        // do some work on the span
        join_handles.push(jh);
    }

    for jh in join_handles {
        jh.await?;
    }
    info!("end tokio example with tracing");

    Ok(())
}
