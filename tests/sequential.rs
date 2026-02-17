use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use dag_exec::{ExecError, Executor, ExecutorConfig};

mod common;

#[test]
fn sequential_correctness() {
    let counter_d = Arc::new(AtomicUsize::new(0));
    let dag = common::build_add_chain_dag(Arc::clone(&counter_d));

    let exec = Executor::new(ExecutorConfig::default());
    let out = exec.run_sequential(&dag, vec!["d".into()]).unwrap();

    assert_eq!(*out["d"], 5);
    assert_eq!(counter_d.load(Ordering::SeqCst), 1);
}

#[test]
fn sequential_prunes_unused_subgraph() {
    let counter_d = Arc::new(AtomicUsize::new(0));
    let dag = common::build_add_chain_dag(Arc::clone(&counter_d));

    let exec = Executor::new(ExecutorConfig::default());
    let out = exec.run_sequential(&dag, vec!["c".into()]).unwrap();

    assert_eq!(*out["c"], 3);
    // d is not needed to compute c
    assert_eq!(counter_d.load(Ordering::SeqCst), 0);
}

#[test]
fn sequential_detects_cycle() {
    let dag = common::build_cycle_dag();
    let exec = Executor::new(ExecutorConfig::default());

    let err = exec.run_sequential(&dag, vec!["a".into()]).unwrap_err();
    match err {
        ExecError::Cycle { remaining } => assert!(!remaining.is_empty()),
        _ => panic!("unexpected err: {err:?}"),
    }
}

#[test]
fn sequential_propagates_task_failure() {
    let dag = common::build_failing_dag();
    let exec = Executor::new(ExecutorConfig::default());

    let err = exec.run_sequential(&dag, vec!["c".into()]).unwrap_err();
    match err {
        ExecError::TaskFailed { task, .. } => assert_eq!(task, "c"),
        _ => panic!("unexpected err: {err:?}"),
    }
}
