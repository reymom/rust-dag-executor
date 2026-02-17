use std::{collections::HashMap, sync::Arc};

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct NodeId(pub usize);

#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    pub max_workers: usize,
    pub max_in_flight: usize,
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        let max_workers = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        Self {
            max_workers,
            max_in_flight: max_workers * 2,
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

pub struct Dag<K, O, E> {
    pub(crate) nodes: Vec<Node<K, O, E>>,
    pub(crate) index: HashMap<K, NodeId>,
}
