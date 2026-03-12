#![cfg(feature = "execution-trace")]

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use dag_exec::{Executor, ExecutorConfig, TraceNodeKind};

mod common;

#[test]
fn sequential_traced_matches_untraced_output() {
    let dag = common::build_add_chain_dag(Arc::new(AtomicUsize::new(0)));
    let exec = Executor::new(ExecutorConfig::default());

    let plain = exec.run_sequential(&dag, vec!["d".into()]).unwrap();
    let traced = exec.run_sequential_traced(&dag, vec!["d".into()]).unwrap();

    assert_eq!(*plain["d"], *traced.outputs["d"]);
    assert_eq!(traced.trace.executed_nodes, 4);
    assert_eq!(traced.trace.nodes.len(), 4);
    assert!(
        traced
            .trace
            .nodes
            .iter()
            .all(|node| node.worker_id.is_none())
    );
}

#[test]
fn sequential_trace_pruned_subgraph() {
    let counter = Arc::new(AtomicUsize::new(0));
    let dag = common::build_add_chain_dag(Arc::clone(&counter));
    let exec = Executor::new(ExecutorConfig::default());

    let result = exec.run_sequential_traced(&dag, vec!["c".into()]).unwrap();

    assert_eq!(*result.outputs["c"], 3);
    assert_eq!(counter.load(Ordering::SeqCst), 0);
    assert_eq!(result.trace.executed_nodes, 3);

    let mut keys = result
        .trace
        .nodes
        .iter()
        .map(|node| node.key.as_str())
        .collect::<Vec<_>>();
    keys.sort_unstable();

    assert_eq!(keys, vec!["a", "b", "c"]);
}

#[test]
fn trace_critical_path_le_wall_time() {
    let dag = common::build_add_chain_dag(Arc::new(AtomicUsize::new(0)));
    let exec = Executor::new(ExecutorConfig::default());
    let result = exec.run_sequential_traced(&dag, vec!["d".into()]).unwrap();
    // Critical path can never exceed wall time
    assert!(result.trace.critical_path_time <= result.trace.wall_time);
}

#[test]
fn parallel_trace_worker_ids_present() {
    let dag = common::build_add_chain_dag(Arc::new(AtomicUsize::new(0)));
    let mut cfg = ExecutorConfig::default();
    cfg.max_workers = 2;

    let exec = Executor::new(cfg);
    let result = exec.run_parallel_traced(&dag, vec!["d".into()]).unwrap();

    // Task nodes should have a worker_id; source nodes won't
    let task_nodes: Vec<_> = result
        .trace
        .nodes
        .iter()
        .filter(|n| matches!(n.kind, TraceNodeKind::Task))
        .collect();

    assert!(!task_nodes.is_empty());
    assert!(task_nodes.iter().all(|n| n.worker_id.is_some()));
}

#[test]
fn trace_ordering_is_topological() {
    let dag = common::build_add_chain_dag(Arc::new(AtomicUsize::new(0)));
    let exec = Executor::new(ExecutorConfig::default());
    let result = exec.run_sequential_traced(&dag, vec!["d".into()]).unwrap();

    // For each node, all its deps must appear earlier in the trace
    let position: std::collections::HashMap<_, _> = result
        .trace
        .nodes
        .iter()
        .enumerate()
        .map(|(i, n)| (n.node_id, i))
        .collect();

    for node in &result.trace.nodes {
        for dep in &node.deps {
            assert!(position[dep] < position[&node.node_id]);
        }
    }
}
