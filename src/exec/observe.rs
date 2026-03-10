use crate::graph::NodeId;

/// Purpose: decouple scheduler logic from optional telemetry collection.
pub(crate) trait ExecObserver {
    fn mark_ready(&mut self, _id: NodeId, _frontier_width: usize) {}

    fn mark_start(&mut self, _id: NodeId, _worker_id: Option<usize>) {}

    fn mark_finish(&mut self, _id: NodeId) {}
}

/// Purpose: keep the default path free of telemetry overhead.
pub(crate) struct NoopObserver;

impl NoopObserver {
    pub(crate) fn new() -> Self {
        Self
    }
}

impl ExecObserver for NoopObserver {}
