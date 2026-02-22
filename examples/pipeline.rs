use std::{
    collections::hash_map::DefaultHasher,
    error::Error,
    hash::{Hash, Hasher},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use dag_exec::{DagBuilder, Executor, ExecutorConfig};

fn hash_u64(bytes: &[u8]) -> u64 {
    let mut h = DefaultHasher::new();
    bytes.hash(&mut h);
    h.finish()
}

fn u64_to_le_bytes(x: u64) -> Vec<u8> {
    x.to_le_bytes().to_vec()
}

fn main() -> Result<(), Box<dyn Error>> {
    // Purpose: demonstrate a CPU-ish pipeline DAG + pruning + parallel config knobs.
    let ran_normalize = Arc::new(AtomicUsize::new(0));
    let ran_fee = Arc::new(AtomicUsize::new(0));
    let ran_report = Arc::new(AtomicUsize::new(0));

    // All node outputs share the same O type in this crate, so we represent stage outputs as bytes.
    let raw_tx: Vec<u8> = b"example tx bytes: alice->bob 5".to_vec();

    let mut b = DagBuilder::<String, Vec<u8>, &'static str>::new();
    b.add_source("raw".into(), raw_tx)?;

    // Stage 1: normalize (placeholder for decode/parse)
    {
        let ran_normalize = Arc::clone(&ran_normalize);
        b.add_task("norm".into(), vec!["raw".into()], move |xs| {
            ran_normalize.fetch_add(1, Ordering::SeqCst);
            let mut v = (*xs[0]).clone();
            // Tiny deterministic "normalization"
            v.make_ascii_lowercase();
            Ok(v)
        })?;
    }

    // Stage 2a: "hash" the normalized bytes (represents hashing / commitment)
    b.add_task("hash".into(), vec!["norm".into()], |xs| {
        let h = hash_u64(&xs[0]);
        Ok(u64_to_le_bytes(h))
    })?;

    // Stage 2b: compute a toy "fee" from normalized bytes (represents fee calc / scoring)
    {
        let ran_fee = Arc::clone(&ran_fee);
        b.add_task("fee".into(), vec!["norm".into()], move |xs| {
            ran_fee.fetch_add(1, Ordering::SeqCst);
            let sum: u64 = xs[0].iter().map(|&b| b as u64).sum();
            Ok(u64_to_le_bytes(sum % 10_000))
        })?;
    }

    // Stage 3: build a report from (hash, fee)
    {
        let ran_report = Arc::clone(&ran_report);
        b.add_task(
            "report".into(),
            vec!["hash".into(), "fee".into()],
            move |xs| {
                ran_report.fetch_add(1, Ordering::SeqCst);
                let hash_le = &xs[0];
                let fee_le = &xs[1];
                let s = format!("hash_le={:02x?} fee_le={:02x?}", hash_le, fee_le);
                Ok(s.into_bytes())
            },
        )?;
    }

    let dag = b.build()?;

    let cfg = ExecutorConfig {
        max_workers: 4,
        max_in_flight: 4,
        worker_queue_cap: 2,
    };
    let exec = Executor::new(cfg);

    // Prune demo: request only "hash" (should not run fee/report)
    let out = exec.run_parallel(&dag, ["hash".to_string()])?;
    println!("hash (le bytes) = {:02x?}", out["hash"]);

    println!(
        "counts after hash-only: norm={} fee={} report={}",
        ran_normalize.load(Ordering::SeqCst),
        ran_fee.load(Ordering::SeqCst),
        ran_report.load(Ordering::SeqCst),
    );

    Ok(())
}
