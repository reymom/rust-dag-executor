use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use dag_exec::{ExecError, Executor, ExecutorConfig};

mod common;

#[test]
fn parallel_correctness() {
    let counter_d = Arc::new(AtomicUsize::new(0));
    let dag = common::build_add_chain_dag(Arc::clone(&counter_d));

    let mut cfg = ExecutorConfig::default();
    cfg.max_workers = cfg.max_workers.max(2);

    let exec = Executor::new(cfg);
    let out = exec.run_parallel(&dag, vec!["d".into()]).unwrap();

    assert_eq!(*out["d"], 5);
    assert_eq!(counter_d.load(Ordering::SeqCst), 1);
}

#[test]
fn parallel_prunes_unused_subgraph() {
    let counter_d = Arc::new(AtomicUsize::new(0));
    let dag = common::build_add_chain_dag(Arc::clone(&counter_d));

    let mut cfg = ExecutorConfig::default();
    cfg.max_workers = cfg.max_workers.max(2);

    let exec = Executor::new(cfg);
    let out = exec.run_parallel(&dag, vec!["c".into()]).unwrap();

    assert_eq!(*out["c"], 3);
    assert_eq!(counter_d.load(Ordering::SeqCst), 0);
}

#[test]
fn parallel_detects_cycle() {
    let dag = common::build_cycle_dag();

    let mut cfg = ExecutorConfig::default();
    cfg.max_workers = cfg.max_workers.max(2);

    let exec = Executor::new(cfg);
    let err = exec.run_parallel(&dag, vec!["a".into()]).unwrap_err();

    match err {
        ExecError::Cycle { remaining } => assert!(!remaining.is_empty()),
        _ => panic!("unexpected err: {err:?}"),
    }
}

#[test]
fn parallel_propagates_task_failure() {
    let dag = common::build_failing_dag();

    let mut cfg = ExecutorConfig::default();
    cfg.max_workers = cfg.max_workers.max(2);

    let exec = Executor::new(cfg);
    let err = exec.run_parallel(&dag, vec!["c".into()]).unwrap_err();

    match err {
        ExecError::TaskFailed { task, .. } => assert_eq!(task, "c"),
        _ => panic!("unexpected err: {err:?}"),
    }
}
