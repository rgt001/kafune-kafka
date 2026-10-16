//! Opt-in phase timings for profiling real operations without logging payloads.
use std::time::Instant;

pub(crate) struct Trace {
    started: Option<Instant>,
    operation: &'static str,
}
impl Trace {
    pub(crate) fn new(operation: &'static str) -> Self {
        Self {
            started: std::env::var_os("KAFUNEKAFKA_TRACE").map(|_| Instant::now()),
            operation,
        }
    }
    pub(crate) fn mark(&self, phase: &'static str) {
        if let Some(started) = self.started {
            eprintln!(
                "PERF_TRACE {}",
                serde_json::json!({"operation":self.operation,"phase":phase,"elapsed_ms":started.elapsed().as_secs_f64()*1000.0})
            );
        }
    }
}
