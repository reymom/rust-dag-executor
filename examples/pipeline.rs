mod common;

use std::{
    error::Error,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use dag_exec::{DagBuilder, Executor, ExecutorConfig};

use common::{
    env_usize, hash_domain_with_iters, hash_leaf_with_iters, hex, read_u64_le, reset_counts,
    u64_to_le_bytes,
};

/// Purpose: branching “protocol pipeline” DAG.
///
/// Demonstrates:
/// - Output-driven pruning across branches:
///   * request `commitment`      => skips fee/risk/receipts
///   * request `receipt_basic`   => skips risk + full receipt
///   * request `receipt_full`    => runs everything
///
/// Run:
/// - cargo run --example pipeline
/// - DAG_EXEC_HASH_ITERS=20000 cargo run --release --example pipeline
///
/// Note: uses a toy hash from examples/common (deterministic, NOT cryptographically secure).
fn main() -> Result<(), Box<dyn Error>> {
    let hash_iters = env_usize("DAG_EXEC_HASH_ITERS", 0);

    let ran_decode = Arc::new(AtomicUsize::new(0));
    let ran_commit = Arc::new(AtomicUsize::new(0));
    let ran_fee = Arc::new(AtomicUsize::new(0));
    let ran_risk = Arc::new(AtomicUsize::new(0));
    let ran_receipt_basic = Arc::new(AtomicUsize::new(0));
    let ran_receipt_full = Arc::new(AtomicUsize::new(0));

    // “raw tx bytes”: pretend this is canonical encoding from the network.
    let raw_tx: Vec<u8> = b"RAW_TX: Alice->Bob amount=5 nonce=7".to_vec();

    let mut b = DagBuilder::<String, Vec<u8>, &'static str>::new();
    b.add_source("raw".into(), raw_tx)?;

    // Stage 1: decode/normalize (cheap placeholder).
    {
        let ran = Arc::clone(&ran_decode);
        b.add_task("decoded".into(), vec!["raw".into()], move |xs| {
            ran.fetch_add(1, Ordering::SeqCst);
            let mut v = xs[0].as_ref().clone();
            v.make_ascii_lowercase();
            Ok(v)
        })?;
    }

    // Branch A: commitment (hash-shaped CPU work).
    {
        let ran = Arc::clone(&ran_commit);
        b.add_task("commitment".into(), vec!["decoded".into()], move |xs| {
            ran.fetch_add(1, Ordering::SeqCst);
            Ok(hash_leaf_with_iters(hash_iters, xs[0].as_ref()))
        })?;
    }

    // Branch B: fee calculation (cheap numeric stage).
    {
        let ran = Arc::clone(&ran_fee);
        b.add_task("fee".into(), vec!["decoded".into()], move |xs| {
            ran.fetch_add(1, Ordering::SeqCst);
            let sum: u64 = xs[0].iter().map(|&b| b as u64).sum();
            Ok(u64_to_le_bytes(sum % 10_000))
        })?;
    }

    // Branch C: risk score (another hash-shaped stage; different domain).
    {
        let ran = Arc::clone(&ran_risk);
        b.add_task("risk".into(), vec!["decoded".into()], move |xs| {
            ran.fetch_add(1, Ordering::SeqCst);
            Ok(hash_domain_with_iters(hash_iters, 0x02, &[xs[0].as_ref()]))
        })?;
    }

    // Receipt (basic): depends on commitment + fee (no risk).
    {
        let ran = Arc::clone(&ran_receipt_basic);
        b.add_task(
            "receipt_basic".into(),
            vec!["commitment".into(), "fee".into()],
            move |xs| {
                ran.fetch_add(1, Ordering::SeqCst);
                let commit = xs[0].as_ref();
                let fee = read_u64_le(xs[1].as_ref())?;
                let s = format!("receipt_basic(commit=0x{} fee={})", hex(commit), fee);
                Ok(s.into_bytes())
            },
        )?;
    }

    // Receipt (full): depends on receipt_basic + risk (layered pipeline).
    {
        let ran = Arc::clone(&ran_receipt_full);
        b.add_task(
            "receipt_full".into(),
            vec!["receipt_basic".into(), "risk".into()],
            move |xs| {
                ran.fetch_add(1, Ordering::SeqCst);

                let basic = String::from_utf8_lossy(xs[0].as_ref());
                let risk = xs[1].as_ref();

                let s = format!("{basic} risk=0x{}", hex(risk));
                Ok(s.into_bytes())
            },
        )?;
    }

    let dag = b.build()?;

    let exec = Executor::new(ExecutorConfig {
        max_workers: std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4),
        max_in_flight: 64,
        worker_queue_cap: 2,
    });

    println!("=== pipeline ===");
    println!("hash_iters = {hash_iters} (set DAG_EXEC_HASH_ITERS for heavy mode)");
    println!();

    // Run 1: commitment only => prunes fee/risk/receipts.
    reset_counts(&[
        &ran_decode,
        &ran_commit,
        &ran_fee,
        &ran_risk,
        &ran_receipt_basic,
        &ran_receipt_full,
    ]);
    let out = exec.run_parallel(&dag, ["commitment".to_string()])?;
    println!("request: commitment");
    println!("commitment = 0x{}", hex(out["commitment"].as_ref()));
    println!(
        "counts: decode={} commit={} fee={} risk={} basic={} full={}\n",
        ran_decode.load(Ordering::SeqCst),
        ran_commit.load(Ordering::SeqCst),
        ran_fee.load(Ordering::SeqCst),
        ran_risk.load(Ordering::SeqCst),
        ran_receipt_basic.load(Ordering::SeqCst),
        ran_receipt_full.load(Ordering::SeqCst),
    );

    // Run 2: receipt_basic => prunes risk + receipt_full.
    reset_counts(&[
        &ran_decode,
        &ran_commit,
        &ran_fee,
        &ran_risk,
        &ran_receipt_basic,
        &ran_receipt_full,
    ]);
    let out = exec.run_parallel(&dag, ["receipt_basic".to_string()])?;
    println!("request: receipt_basic");
    println!("{}", String::from_utf8_lossy(out["receipt_basic"].as_ref()));
    println!(
        "counts: decode={} commit={} fee={} risk={} basic={} full={}\n",
        ran_decode.load(Ordering::SeqCst),
        ran_commit.load(Ordering::SeqCst),
        ran_fee.load(Ordering::SeqCst),
        ran_risk.load(Ordering::SeqCst),
        ran_receipt_basic.load(Ordering::SeqCst),
        ran_receipt_full.load(Ordering::SeqCst),
    );

    // Run 3: receipt_full => computes all branches.
    reset_counts(&[
        &ran_decode,
        &ran_commit,
        &ran_fee,
        &ran_risk,
        &ran_receipt_basic,
        &ran_receipt_full,
    ]);
    let out = exec.run_parallel(&dag, ["receipt_full".to_string()])?;
    println!("request: receipt_full");
    println!("{}", String::from_utf8_lossy(out["receipt_full"].as_ref()));
    println!(
        "counts: decode={} commit={} fee={} risk={} basic={} full={}",
        ran_decode.load(Ordering::SeqCst),
        ran_commit.load(Ordering::SeqCst),
        ran_fee.load(Ordering::SeqCst),
        ran_risk.load(Ordering::SeqCst),
        ran_receipt_basic.load(Ordering::SeqCst),
        ran_receipt_full.load(Ordering::SeqCst),
    );

    Ok(())
}
