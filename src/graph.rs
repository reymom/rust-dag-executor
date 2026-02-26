use std::{collections::HashMap, sync::Arc};

/// Dense node identifier used internally in the compiled DAG.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct NodeId(pub usize);

/// Executor configuration for the parallel scheduler.
#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    pub max_workers: usize,
    pub max_in_flight: usize,
    /// Per-worker buffered tasks (not counting the running task).
    pub worker_queue_cap: usize,
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        let max_workers = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        Self {
            max_workers,
            max_in_flight: max_workers * 2,
            worker_queue_cap: 2,
        }
    }
}

pub(crate) type TaskFn<O, E> = dyn Fn(&[Arc<O>]) -> Result<O, E> + Send + Sync + 'static;

pub(crate) enum NodeKind<O, E> {
    Source(Arc<O>),
    Task(Arc<TaskFn<O, E>>),
}

pub(crate) struct Node<K, O, E> {
    pub key: K,
    pub deps: Vec<NodeId>,
    pub kind: NodeKind<O, E>,
}

/// A compiled DAG: nodes indexed densely and accessible by key.
pub struct Dag<K, O, E> {
    pub(crate) nodes: Vec<Node<K, O, E>>,
    pub(crate) index: HashMap<K, NodeId>,
}
