mod common;

use std::{
    error::Error,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use dag_exec::{DagBuilder, Executor, ExecutorConfig};

use common::{ensure_32, fake_encoded_tx, hash_leaf_with_iters, hash_node_with_iters, hex};

type DagT = dag_exec::Dag<String, Vec<u8>, &'static str>;
type BuildErr = dag_exec::BuildError<String>;

/// Purpose: minimal “Merkle tree as DAG” pruning demo.
///
/// Demonstrates:
/// - Request root => computes all leaves + all internal nodes.
/// - Request subtree root => prunes unrelated leaves/branches (counts prove it).
///
/// Run:
/// - cargo run --example merkle
///
/// Note: fixed iters=0 for stable output; uses toy hash from examples/common (NOT crypto).
fn main() -> Result<(), Box<dyn Error>> {
    // ---- knobs ----
    let n_leaves: usize = 16;
    let subtree_level: usize = 2;
    let subtree_index: usize = 1;

    assert!(n_leaves.is_power_of_two() && n_leaves >= 2);
    let height = n_leaves.trailing_zeros() as usize;

    assert!(subtree_level >= 1 && subtree_level <= height);
    let width_at_level = n_leaves >> subtree_level;
    assert!(subtree_index < width_at_level);

    // ---- counters so pruning is obvious ----
    let ran_leaf_hash = Arc::new(AtomicUsize::new(0));
    let ran_internal_hash = Arc::new(AtomicUsize::new(0));

    // ---- build DAG: tx -> leaf -> internal -> root ----
    let (dag, root_key, subtree_key) = build_merkle_dag(
        n_leaves,
        height,
        subtree_level,
        subtree_index,
        Arc::clone(&ran_leaf_hash),
        Arc::clone(&ran_internal_hash),
    )?;

    let cfg = ExecutorConfig {
        max_workers: std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4),
        max_in_flight: 64,
        worker_queue_cap: 2,
    };
    let exec = Executor::new(cfg);

    // ---- 1) request root (full compute) ----
    ran_leaf_hash.store(0, Ordering::SeqCst);
    ran_internal_hash.store(0, Ordering::SeqCst);

    let out_root = exec.run_parallel(&dag, std::iter::once(root_key.clone()))?;
    let root = out_root[&root_key].as_ref();

    println!("=== request: ROOT ===");
    println!("root_key   = {root_key}");
    println!("root       = 0x{}", hex(root));
    println!(
        "ran leaf_hash={} (expected={}) | ran internal_hash={} (expected={})",
        ran_leaf_hash.load(Ordering::SeqCst),
        n_leaves,
        ran_internal_hash.load(Ordering::SeqCst),
        n_leaves - 1,
    );
    println!();

    // ---- 2) request an internal subtree root (prunes other branches) ----
    ran_leaf_hash.store(0, Ordering::SeqCst);
    ran_internal_hash.store(0, Ordering::SeqCst);

    let out_sub = exec.run_parallel(&dag, std::iter::once(subtree_key.clone()))?;
    let sub = &out_sub[&subtree_key];

    let leaves_in_subtree = 1usize << subtree_level; // level=1 => 2 leaves, level=2 => 4 leaves, ...
    let leaf_start = subtree_index * leaves_in_subtree;
    let leaf_end_exclusive = leaf_start + leaves_in_subtree;

    println!("=== request: SUBTREE ===");
    println!("subtree_key = {subtree_key}");
    println!("covers leaves [{leaf_start}..{leaf_end_exclusive})  (tx batch slice)");
    println!("subtree     = 0x{}", hex(sub));
    println!(
        "ran leaf_hash={} (expected={}) | ran internal_hash={} (expected={})",
        ran_leaf_hash.load(Ordering::SeqCst),
        leaves_in_subtree,
        ran_internal_hash.load(Ordering::SeqCst),
        leaves_in_subtree - 1,
    );

    Ok(())
}

/// Build a perfect binary Merkle tree as a DAG:
/// - sources: "tx/{i}" are tx bytes (pretend canonical encoding)
/// - tasks:   "leaf/{i}" = H_leaf(tx/{i})
/// - tasks:   "node/{level}/{idx}" = H_node(child_left, child_right)
///
/// Returns: (dag, root_key, subtree_key)
fn build_merkle_dag(
    n_leaves: usize,
    height: usize,
    subtree_level: usize,
    subtree_index: usize,
    ran_leaf_hash: Arc<AtomicUsize>,
    ran_internal_hash: Arc<AtomicUsize>,
) -> Result<(DagT, String, String), BuildErr> {
    let mut b = DagBuilder::<String, Vec<u8>, &'static str>::new();

    // Sources + leaf hash tasks
    for i in 0..n_leaves {
        let tx_key = tx_key(i);
        let leaf_key = leaf_key(i);

        b.add_source(tx_key.clone(), fake_encoded_tx(i))?;

        let ran = Arc::clone(&ran_leaf_hash);
        b.add_task(leaf_key, vec![tx_key], move |xs| {
            ran.fetch_add(1, Ordering::SeqCst);
            Ok(hash_leaf_with_iters(0, xs[0].as_ref()))
        })?;
    }

    // Internal levels: 1..=height
    // Level 1 hashes pairs of leaves, level 2 hashes pairs of level-1 nodes, etc.
    for level in 1..=height {
        let width = n_leaves >> level;
        for idx in 0..width {
            let left_child = merkle_key(level - 1, idx * 2);
            let right_child = merkle_key(level - 1, idx * 2 + 1);
            let out_key = merkle_key(level, idx);

            let ran = Arc::clone(&ran_internal_hash);
            b.add_task(out_key, vec![left_child, right_child], move |xs| {
                ran.fetch_add(1, Ordering::SeqCst);

                // For the demo we expect both children to already be 32-byte hashes.
                // If you swap in a real hash, keep these checks: they catch wiring mistakes early.
                ensure_32(xs[0].as_ref())?;
                ensure_32(xs[1].as_ref())?;

                Ok(hash_node_with_iters(0, xs[0].as_ref(), xs[1].as_ref()))
            })?;
        }
    }

    let root_key = merkle_key(height, 0);
    let subtree_key = merkle_key(subtree_level, subtree_index);

    Ok((b.build()?, root_key, subtree_key))
}

fn tx_key(i: usize) -> String {
    format!("tx/{i}")
}

fn leaf_key(i: usize) -> String {
    format!("leaf/{i}")
}

fn merkle_key(level: usize, idx: usize) -> String {
    if level == 0 {
        leaf_key(idx)
    } else {
        format!("node/{level}/{idx}")
    }
}
