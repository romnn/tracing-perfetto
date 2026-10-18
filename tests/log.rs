//! Writes a trace file through a global subscriber that also logs to stdout.

use googletest::prelude::*;
use tokio::runtime::Handle;
use tracing::{Level, field, info, span};
use tracing_perfetto::PerfettoLayer;
use tracing_subscriber::fmt::format::Format;
use tracing_subscriber::{Registry, fmt, layer::SubscriberExt};

/// Records a sample trace through a test-owned global subscriber.
///
/// No other dispatcher is installed, so no thread-scoped dispatcher shadows this subscriber.
#[gtest]
#[tokio::test]
async fn write() -> Result<()> {
    // The process ID keeps concurrent test runs from sharing one trace file.
    let file =
        std::env::temp_dir().join(format!("tracing-perfetto-{}.pftrace", std::process::id()));
    let perfetto_layer = PerfettoLayer::new(std::sync::Mutex::new(
        std::fs::File::create(&file).or_fail()?,
    ))
    .with_debug_annotations(true);

    let fmt_layer = fmt::layer()
        .with_writer(std::io::stdout)
        .event_format(Format::default().with_thread_ids(true))
        .with_span_events(fmt::format::FmtSpan::FULL);

    let subscriber = Registry::default().with(fmt_layer).with(perfetto_layer);

    tracing::subscriber::set_global_default(subscriber).or_fail()?;

    info!(?file, "start");

    let demo_span = span!(
        Level::TRACE,
        "demo_span",
        regular_arg = "Arg data",
        extra_arg = field::Empty
    );
    let _enter = demo_span.enter();

    info!("in span");
    sync_fn(1);
    let handle = Handle::current();
    let t = std::thread::spawn(move || {
        handle.spawn(async_fn());
    });
    t.join().or_fail()?;

    _ = tokio::spawn(async_fn()).await;
    demo_span.record("extra_arg", "Some Extra Data");
    demo_span.record("regular_arg", "New Arg Data");

    // The closed function spans have already been written to the trace file.
    let written = std::fs::metadata(&file).or_fail()?.len();
    _ = std::fs::remove_file(&file);
    verify_that!(written, gt(0))
}

#[tracing::instrument(fields(perfetto = true))]
fn sync_fn(i: i32) {
    info!("inside function");
    sync_inner(i + 1);
}

#[tracing::instrument(skip_all, level = "trace", fields(perfetto = true))]
fn sync_inner(x: i32) {
    info!(x, "inner");
}

#[tracing::instrument]
async fn async_fn() {
    info!(perfetto = true, "test");
    async_inner().await;
}

#[tracing::instrument]
async fn async_inner() {
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
}
