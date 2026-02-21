use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use dag_exec::{DagBuilder, ExecError, Executor, ExecutorConfig};

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
fn parallel_matches_sequential_on_simple_dag() {
    let counter_d = Arc::new(AtomicUsize::new(0));
    let dag = common::build_add_chain_dag(Arc::clone(&counter_d));

    let exec_seq = Executor::new(ExecutorConfig::default());

    let mut cfg = ExecutorConfig::default();
    cfg.max_workers = cfg.max_workers.max(2);
    let exec_par = Executor::new(cfg);

    let out_seq = exec_seq
        .run_sequential(&dag, vec!["d".into(), "c".into()])
        .unwrap();
    let out_par = exec_par
        .run_parallel(&dag, vec!["d".into(), "c".into()])
        .unwrap();

    assert_eq!(*out_seq["c"], *out_par["c"]);
    assert_eq!(*out_seq["d"], *out_par["d"]);
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

#[test]
fn parallel_respects_max_in_flight() {
    let n_tasks = 32;

    // Track concurrent running tasks
    let active = Arc::new(AtomicUsize::new(0));
    let max_active = Arc::new(AtomicUsize::new(0));

    // Gate tasks to force overlap without sleep
    let started = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(AtomicBool::new(false));

    let mut b = DagBuilder::<String, usize, ()>::new();
    b.add_source("src".into(), 1usize).unwrap();

    for i in 0..n_tasks {
        let active = Arc::clone(&active);
        let max_active = Arc::clone(&max_active);
        let started = Arc::clone(&started);
        let release = Arc::clone(&release);

        b.add_task(format!("t{i}"), vec!["src".into()], move |_xs| {
            let cur = active.fetch_add(1, Ordering::SeqCst) + 1;

            // update max_active = max(max_active, cur)
            loop {
                let prev = max_active.load(Ordering::SeqCst);
                if cur <= prev {
                    break;
                }
                if max_active
                    .compare_exchange(prev, cur, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    break;
                }
            }

            started.fetch_add(1, Ordering::SeqCst);

            while !release.load(Ordering::SeqCst) {
                std::hint::spin_loop();
            }

            active.fetch_sub(1, Ordering::SeqCst);
            Ok(1usize)
        })
        .unwrap();
    }

    let dag = b.build().unwrap();

    let cfg = ExecutorConfig {
        max_workers: 8,
        worker_queue_cap: 1,
        max_in_flight: 2,
    };

    let exec = Executor::new(cfg.clone());

    // Run executor in a separate thread so we can release the gate
    let release2 = Arc::clone(&release);
    let started2 = Arc::clone(&started);

    let h = std::thread::spawn(move || {
        let outs = (0..n_tasks).map(|i| format!("t{i}")).collect::<Vec<_>>();
        exec.run_parallel(&dag, outs)
    });

    while started2.load(Ordering::SeqCst) == 0 {
        std::hint::spin_loop();
    }

    release2.store(true, Ordering::SeqCst);

    let out = h.join().unwrap().unwrap();
    assert_eq!(out.len(), n_tasks);

    assert!(max_active.load(Ordering::SeqCst) <= cfg.max_in_flight);
}

#[test]
fn parallel_no_hang_on_task_failure_repeat() {
    let dag = common::build_failing_dag();
    let mut cfg = ExecutorConfig::default();
    cfg.max_workers = cfg.max_workers.max(2);
    let exec = Executor::new(cfg);

    for _ in 0..100 {
        let err = exec.run_parallel(&dag, vec!["c".into()]).unwrap_err();
        match err {
            ExecError::TaskFailed { task, .. } => assert_eq!(task, "c"),
            _ => panic!("unexpected err: {err:?}"),
        }
    }
}
