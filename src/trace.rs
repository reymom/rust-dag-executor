use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use crate::{
    exec::observe::ExecObserver,
    graph::{Dag, NodeId, NodeKind},
};

/// Purpose: distinguish immutable sources from computed tasks in execution reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TraceNodeKind {
    Source,
    Task,
}

/// Purpose: expose one node's scheduling and runtime timing in a backend-agnostic shape.
#[derive(Debug, Clone)]
pub struct NodeTrace<K> {
    pub node_id: NodeId,
    pub key: K,
    pub kind: TraceNodeKind,
    pub deps: Vec<NodeId>,
    pub ready_at: Duration,
    pub started_at: Duration,
    pub finished_at: Duration,
    pub run_duration: Duration,
    pub worker_id: Option<usize>,
}

/// Purpose: summarize one DAG execution in a form suitable for benchmarking and diagnostics.
#[must_use]
#[derive(Debug, Clone)]
pub struct ExecutionTrace<K> {
    pub nodes: Vec<NodeTrace<K>>,
    pub wall_time: Duration,
    pub critical_path_time: Duration,
    pub max_frontier_width: usize,
    pub executed_nodes: usize,
}

/// Purpose: keep traced runs ergonomic without changing the existing executor API.
#[must_use]
#[derive(Debug)]
pub struct TracedExecution<K, O> {
    pub outputs: HashMap<K, Arc<O>>,
    pub trace: ExecutionTrace<K>,
}

/// Purpose: record scheduler-observed execution timing for later reporting.
pub(crate) struct TraceObserver {
    started: Instant,
    ready_at: Vec<Option<Duration>>,
    started_at: Vec<Option<Duration>>,
    finished_at: Vec<Option<Duration>>,
    worker_id: Vec<Option<usize>>,
    execution_order: Vec<NodeId>,
    max_frontier_width: usize,
}

impl TraceObserver {
    pub(crate) fn new(node_count: usize) -> Self {
        Self {
            started: Instant::now(),
            ready_at: vec![None; node_count],
            started_at: vec![None; node_count],
            finished_at: vec![None; node_count],
            worker_id: vec![None; node_count],
            execution_order: Vec::with_capacity(node_count),
            max_frontier_width: 0,
        }
    }

    fn elapsed(&self) -> Duration {
        self.started.elapsed()
    }

    pub(crate) fn finalize<K, O, E>(self, dag: &Dag<K, O, E>, needed: &[bool]) -> ExecutionTrace<K>
    where
        K: Clone,
    {
        let wall_time = self.started.elapsed();
        let mut nodes = Vec::with_capacity(self.execution_order.len());
        let mut longest_path = vec![Duration::ZERO; dag.nodes.len()];
        let mut critical_path_time = Duration::ZERO;

        // execution_order is populated in mark_start order. Since deps must be
        // started before their dependents can be scheduled, this is implicitly
        // topological — the critical-path DP below relies on this invariant.
        for id in self.execution_order {
            if !needed[id.0] {
                continue;
            }

            let started_at = self.started_at[id.0].unwrap_or(Duration::ZERO);
            let finished_at = self.finished_at[id.0].unwrap_or(started_at);
            let run_duration = finished_at.saturating_sub(started_at);

            let parent_cp = dag.nodes[id.0]
                .deps
                .iter()
                .filter(|dep| needed[dep.0])
                .map(|dep| longest_path[dep.0])
                .max()
                .unwrap_or(Duration::ZERO);

            longest_path[id.0] = parent_cp + run_duration;
            critical_path_time = critical_path_time.max(longest_path[id.0]);

            let kind = match dag.nodes[id.0].kind {
                NodeKind::Source(_) => TraceNodeKind::Source,
                NodeKind::Task(_) => TraceNodeKind::Task,
            };

            nodes.push(NodeTrace {
                node_id: id,
                key: dag.nodes[id.0].key.clone(),
                kind,
                deps: dag.nodes[id.0].deps.clone(),
                ready_at: self.ready_at[id.0].unwrap_or(Duration::ZERO),
                started_at,
                finished_at,
                run_duration,
                worker_id: self.worker_id[id.0],
            });
        }

        ExecutionTrace {
            executed_nodes: nodes.len(),
            nodes,
            wall_time,
            critical_path_time,
            max_frontier_width: self.max_frontier_width,
        }
    }
}

impl ExecObserver for TraceObserver {
    fn mark_ready(&mut self, id: NodeId, frontier_width: usize) {
        if self.ready_at[id.0].is_none() {
            self.ready_at[id.0] = Some(self.elapsed());
        }
        self.max_frontier_width = self.max_frontier_width.max(frontier_width);
    }

    fn mark_start(&mut self, id: NodeId, worker_id: Option<usize>) {
        if self.started_at[id.0].is_none() {
            self.started_at[id.0] = Some(self.elapsed());
            self.execution_order.push(id);
        }
        if self.worker_id[id.0].is_none() {
            self.worker_id[id.0] = worker_id;
        }
    }

    fn mark_finish(&mut self, id: NodeId) {
        if self.finished_at[id.0].is_none() {
            self.finished_at[id.0] = Some(self.elapsed());
        }
    }
}
