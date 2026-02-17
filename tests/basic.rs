use std::sync::Arc;

use dag_exec::{DagBuilder, ExecError, Executor, ExecutorConfig};

#[test]
fn computes_only_requested_outputs() {
    // a=1, b=2, c=a+b, d=c+b
    let mut b = DagBuilder::<String, i32, ()>::new();

    b.add_source("a".into(), 1).unwrap();
    b.add_source("b".into(), 2).unwrap();

    b.add_task(
        "c".into(),
        vec!["a".into(), "b".into()],
        |xs: &[Arc<i32>]| Ok(*xs[0] + *xs[1]),
    )
    .unwrap();

    b.add_task(
        "d".into(),
        vec!["c".into(), "b".into()],
        |xs: &[Arc<i32>]| Ok(*xs[0] + *xs[1]),
    )
    .unwrap();

    let dag = b.build().unwrap();
    let exec = Executor::new(ExecutorConfig::default());

    let out = exec.run_sequential(&dag, vec!["d".into()]).unwrap();
    assert_eq!(*out["d"], 5);
}

#[test]
fn detects_cycle_in_needed_subgraph() {
    let mut b = DagBuilder::<String, i32, ()>::new();

    b.add_task("a".into(), vec!["b".into()], |_xs| Ok(1))
        .unwrap();
    b.add_task("b".into(), vec!["a".into()], |_xs| Ok(1))
        .unwrap();

    let dag = b.build().unwrap();
    let exec = Executor::new(ExecutorConfig::default());

    let err = exec.run_sequential(&dag, vec!["a".into()]).unwrap_err();

    match err {
        ExecError::Cycle { remaining } => assert!(!remaining.is_empty()),
        _ => panic!("unexpected err: {err:?}"),
    }
}
