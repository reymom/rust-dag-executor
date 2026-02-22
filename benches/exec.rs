use std::sync::Arc;

use criterion::{Criterion, criterion_group, criterion_main};

use dag_exec::{Dag, DagBuilder, Executor, ExecutorConfig};

// Purpose: benchmark scheduler overhead and scaling for common DAG shapes (chain vs fan-out).
// Note: per-node work (`mix64`) is intentionally small; increase work size to see parallel speedups.

// Small, deterministic CPU work to avoid allocations and keep benches stable.
fn mix64(mut x: u64) -> u64 {
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58476d1ce4e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}

// Simulate CPU-heavy work (e.g., hashing / proof step) without allocations.
// Tune iterations to your machine; goal is to make per-node work dominate scheduling overhead.
fn work_heavy(mut x: u64) -> u64 {
    for _ in 0..20_000 {
        x = mix64(x);
    }
    x
}

// Linear DAG (no inherent parallelism). Measures baseline overhead of the parallel executor.
fn build_chain(n: usize) -> Dag<String, u64, ()> {
    let mut b = DagBuilder::<String, u64, ()>::new();
    b.add_source("0".into(), 1u64).unwrap();

    for i in 1..=n {
        let prev = (i - 1).to_string();
        let key = i.to_string();
        b.add_task(key, vec![prev], move |xs: &[Arc<u64>]| {
            Ok(mix64(*xs[0] + i as u64))
        })
        .unwrap();
    }

    b.build().unwrap()
}

// Wide fan-out DAG (high parallelism). Measures throughput and backpressure behavior under concurrency.
fn build_fanout(n: usize) -> Dag<String, u64, ()> {
    let mut b = DagBuilder::<String, u64, ()>::new();
    b.add_source("root".into(), 42u64).unwrap();

    for i in 0..n {
        let key = format!("t{i}");
        b.add_task(key, vec!["root".into()], move |xs: &[Arc<u64>]| {
            Ok(mix64(*xs[0] ^ (i as u64)))
        })
        .unwrap();
    }

    b.build().unwrap()
}

fn build_fanout_heavy(n: usize) -> Dag<String, u64, ()> {
    let mut b = DagBuilder::<String, u64, ()>::new();
    b.add_source("root".into(), 42u64).unwrap();

    for i in 0..n {
        let key = format!("t{i}");
        b.add_task(key, vec!["root".into()], move |xs: &[Arc<u64>]| {
            Ok(work_heavy(*xs[0] ^ (i as u64)))
        })
        .unwrap();
    }

    b.build().unwrap()
}

fn bench_chain(c: &mut Criterion) {
    let dag = build_chain(256);
    let exec_seq = Executor::new(ExecutorConfig::default());

    let cfg_par = ExecutorConfig {
        max_workers: 8,
        max_in_flight: 64,
        worker_queue_cap: 2,
    };
    let exec_par = Executor::new(cfg_par);

    // Bench last node only: forces full evaluation of the chain.

    c.bench_function("chain_256_sequential_last", |b| {
        b.iter(|| {
            let out = exec_seq
                .run_sequential(&dag, std::iter::once("256".to_string()))
                .unwrap();
            std::hint::black_box(out);
        })
    });

    c.bench_function("chain_256_parallel_last", |b| {
        b.iter(|| {
            let out = exec_par
                .run_parallel(&dag, std::iter::once("256".to_string()))
                .unwrap();
            std::hint::black_box(out);
        })
    });
}

fn bench_fanout(c: &mut Criterion) {
    let dag = build_fanout(512);
    let dag_heavy = build_fanout_heavy(512);
    let exec_seq = Executor::new(ExecutorConfig::default());

    let cfg_par = ExecutorConfig {
        max_workers: 8,
        max_in_flight: 128,
        worker_queue_cap: 2,
    };
    let exec_par = Executor::new(cfg_par);

    // Benchmark all leaves vs pruning to a single leaf.
    let outs_all = (0..512).map(|i| format!("t{i}")).collect::<Vec<_>>();

    c.bench_function("fanout_512_sequential_all", |b| {
        b.iter(|| {
            let out = exec_seq
                .run_sequential(&dag, outs_all.iter().cloned())
                .unwrap();
            std::hint::black_box(out);
        })
    });

    c.bench_function("fanout_512_parallel_all", |b| {
        b.iter(|| {
            let out = exec_par
                .run_parallel(&dag, outs_all.iter().cloned())
                .unwrap();
            std::hint::black_box(out);
        })
    });

    c.bench_function("fanout_512_parallel_prune_one", |b| {
        b.iter(|| {
            let out = exec_par
                .run_parallel(&dag, std::iter::once("t0".to_string()))
                .unwrap();
            std::hint::black_box(out);
        })
    });

    c.bench_function("fanout_512_sequential_all_heavy", |b| {
        b.iter(|| {
            let out = exec_seq
                .run_sequential(&dag_heavy, outs_all.iter().cloned())
                .unwrap();
            std::hint::black_box(out);
        })
    });

    c.bench_function("fanout_512_parallel_all_heavy", |b| {
        b.iter(|| {
            let out = exec_par
                .run_parallel(&dag_heavy, outs_all.iter().cloned())
                .unwrap();
            std::hint::black_box(out);
        })
    });
}

criterion_group!(benches, bench_chain, bench_fanout);
criterion_main!(benches);
