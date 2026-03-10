use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use dag_exec::{Dag, DagBuilder};

pub fn build_add_chain_dag(counter_d: Arc<AtomicUsize>) -> Dag<String, i32, &'static str> {
    // a=1, b=2, c=a+b, d=c+b (d increments counter when executed)
    let mut b = DagBuilder::<String, i32, &'static str>::new();
    b.add_source("a".into(), 1).unwrap();
    b.add_source("b".into(), 2).unwrap();

    b.add_task("c".into(), vec!["a".into(), "b".into()], |xs| {
        Ok(*xs[0] + *xs[1])
    })
    .unwrap();

    b.add_task("d".into(), vec!["c".into(), "b".into()], move |xs| {
        counter_d.fetch_add(1, Ordering::SeqCst);
        Ok(*xs[0] + *xs[1])
    })
    .unwrap();

    b.build().unwrap()
}

#[allow(dead_code)]
pub fn build_cycle_dag() -> Dag<String, i32, &'static str> {
    let mut b = DagBuilder::<String, i32, &'static str>::new();
    b.add_task("a".into(), vec!["b".into()], |_xs| Ok(1))
        .unwrap();
    b.add_task("b".into(), vec!["a".into()], |_xs| Ok(1))
        .unwrap();
    b.build().unwrap()
}

#[allow(dead_code)]
pub fn build_failing_dag() -> Dag<String, i32, &'static str> {
    let mut b = DagBuilder::<String, i32, &'static str>::new();
    b.add_source("a".into(), 1).unwrap();
    b.add_task("c".into(), vec!["a".into()], |_xs| Err("boom"))
        .unwrap();
    b.build().unwrap()
}
