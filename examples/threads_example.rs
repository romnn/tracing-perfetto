//! Traces nested spans on several OS threads into `threads_example.pftrace` in the temporary
//! directory.

use std::time::Duration;

use tracing::{info, span};
use tracing_perfetto::PerfettoLayer;
use tracing_subscriber::fmt::format::Format;
use tracing_subscriber::{Registry, fmt, layer::SubscriberExt};

/// Pause between traced steps, long enough that each span is visible in the trace viewer.
const STEP: Duration = Duration::from_millis(250);

fn init_subscriber() -> Result<(), Box<dyn std::error::Error>> {
    let trace_path = std::env::temp_dir().join("threads_example.pftrace");
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
fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_subscriber()?;
    let main_span = span!(tracing::Level::INFO, "main");
    let _guard = main_span.enter();
    info!("start threads example with tracing");
    std::thread::sleep(STEP);
    let mut join_handles = Vec::new();

    // Threads whose root spans get tracks named after the span
    for i in 0..3 {
        let jh = std::thread::spawn(move || {
            let _guard = span!(tracing::Level::INFO, "thread", i).entered();
            info!("thread started");
            for j in 0..3 {
                let _guard = span!(tracing::Level::INFO, "loop", i, j).entered();
                std::thread::sleep(STEP);
                info!("thread inner loop");
                std::thread::sleep(STEP);
            }
            info!("thread finished");
        });
        join_handles.push(jh);
    }

    // Threads whose root spans name their own tracks
    for i in 0..3 {
        let jh = std::thread::spawn(move || {
            let _guard = span!(
                tracing::Level::INFO,
                "thread",
                i,
                perfetto.track_name = format!("thread track {}", i)
            )
            .entered();
            info!("thread started");
            for j in 0..3 {
                let _guard = span!(tracing::Level::INFO, "loop", i, j).entered();
                std::thread::sleep(STEP);
                info!("thread inner loop");
                std::thread::sleep(STEP);
            }
            info!("thread finished");
        });

        join_handles.push(jh);
    }

    for jh in join_handles {
        jh.join()
            .map_err(|err| format!("thread panicked: {err:?}"))?;
    }
    info!("end threads example with tracing");
    std::thread::sleep(STEP);

    Ok(())
}
