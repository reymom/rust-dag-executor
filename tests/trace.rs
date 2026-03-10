#![cfg(feature = "execution-trace")]

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use dag_exec::{Executor, ExecutorConfig};

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
fn sequential_traced_prunes_unused_subgraph() {
    let counter_d = Arc::new(AtomicUsize::new(0));
    let dag = common::build_add_chain_dag(Arc::clone(&counter_d));
    let exec = Executor::new(ExecutorConfig::default());

    let traced = exec.run_sequential_traced(&dag, vec!["c".into()]).unwrap();

    assert_eq!(*traced.outputs["c"], 3);
    assert_eq!(counter_d.load(Ordering::SeqCst), 0);
    assert_eq!(traced.trace.executed_nodes, 3);

    let mut keys = traced
        .trace
        .nodes
        .iter()
        .map(|node| node.key.as_str())
        .collect::<Vec<_>>();
    keys.sort_unstable();

    assert_eq!(keys, vec!["a", "b", "c"]);
}
