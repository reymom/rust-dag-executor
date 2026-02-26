mod common;

use std::{
    error::Error,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};

use dag_exec::{DagBuilder, Executor, ExecutorConfig};

use common::{
    ensure_32, env_usize, fake_encoded_tx, hash_leaf_with_iters, hash_node_with_iters, hex,
    log2_pow2, reset_counts,
};

type DagT = dag_exec::Dag<String, Vec<u8>, &'static str>;
type BuildErr = dag_exec::BuildError<String>;

#[derive(Clone, Copy)]
struct RollupSpec {
    chunk_size: usize,
    chunk_height: usize,
    batch_height: usize,
    target_tx: usize,
    hash_iters: usize,
}

#[derive(Clone)]
struct Counters {
    ran_leaf_hash: Arc<AtomicUsize>,
    ran_internal_hash: Arc<AtomicUsize>,
    ran_proof_asm: Arc<AtomicUsize>,
}

/// Purpose: rollup-ish commitment pipeline as a DAG.
///
/// Models:
/// tx bytes -> leaf commitments -> chunk roots -> batch root
///
/// Demonstrates:
/// - Partial evaluation: request one chunk root / proof => prunes other chunks.
/// - Seq vs par: parallel overhead dominates for tiny work; wins with DAG_EXEC_HASH_ITERS.
/// - Proof encoding + verification against the computed chunk root.
///
/// Run:
/// - cargo run --release --example rollup
/// - DAG_EXEC_HASH_ITERS=20000 DAG_EXEC_HASH_ITERS=200000 cargo run --release --example rollup
///
/// Note: toy hash from examples/common is a deterministic workload shaper (NOT cryptographically secure).
fn main() -> Result<(), Box<dyn Error>> {
    // ---- parameters ----
    let batch_size: usize = 16; // txs in this batch
    let chunk_size: usize = 4; // txs per chunk (DA/proving unit)
    let target_tx: usize = 5; // we'll generate inclusion proof within its chunk

    assert!(batch_size.is_power_of_two() && batch_size >= 2);
    assert!(chunk_size.is_power_of_two() && chunk_size >= 2);
    assert!(batch_size % chunk_size == 0);

    let n_chunks = batch_size / chunk_size;
    assert!(n_chunks.is_power_of_two() && n_chunks >= 2);

    let chunk_height = log2_pow2(chunk_size);
    let batch_height = log2_pow2(n_chunks);

    let target_chunk = target_tx / chunk_size;
    let target_pos_in_chunk = target_tx % chunk_size;

    let hash_iters = env_usize("DAG_EXEC_HASH_ITERS", 0);

    println!("=== rollup batch parameters ===");
    println!("batch_size   = {batch_size} txs");
    println!("chunk_size   = {chunk_size} txs/chunk");
    println!("n_chunks     = {n_chunks}");
    println!("chunk_height = {chunk_height}");
    println!("batch_height = {batch_height}");
    println!("target_tx    = {target_tx} (chunk={target_chunk}, pos={target_pos_in_chunk})");
    println!("hash_iters   = {hash_iters} (set DAG_EXEC_HASH_ITERS for heavy mode)");
    println!();

    // ---- counters to make pruning obvious ----
    let ran_leaf_hash = Arc::new(AtomicUsize::new(0));
    let ran_internal_hash = Arc::new(AtomicUsize::new(0));
    let ran_proof_asm = Arc::new(AtomicUsize::new(0));

    // ---- "tx bytes" inputs (pretend canonical encoding) ----
    let txs: Vec<Vec<u8>> = (0..batch_size).map(fake_encoded_tx).collect();

    // -----------------------------------------------------------------------------
    // Baseline: naive sequential "always compute everything"
    // - Computes all leaf hashes + all chunk roots + full batch root, regardless of what you need.
    // - This shows the *pruning* win cleanly via counts.
    // -----------------------------------------------------------------------------
    let naive_t0 = Instant::now();
    let naive = naive_full_batch_commitment(&txs, chunk_size, hash_iters);
    let naive_dt = naive_t0.elapsed();

    println!("=== naive baseline (always full compute) ===");
    println!("batch_root = 0x{}", hex(&naive.batch_root));
    println!(
        "hash counts: leaf_hash={} internal_hash={}",
        naive.count_leaf_hash, naive.count_internal_hash
    );
    println!("elapsed = {:?}\n", naive_dt);

    let spec = RollupSpec {
        chunk_size,
        chunk_height,
        batch_height,
        target_tx,
        hash_iters,
    };

    let counters = Counters {
        ran_leaf_hash: Arc::clone(&ran_leaf_hash),
        ran_internal_hash: Arc::clone(&ran_internal_hash),
        ran_proof_asm: Arc::clone(&ran_proof_asm),
    };

    let (dag, keys) = build_rollup_commitment_dag(&txs, spec, counters)?;

    // Parallel: bounded in-flight work + per-worker bounded queues.
    let avail = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    let max_workers = env_usize("DAG_EXEC_MAX_WORKERS", avail.min(4)).clamp(1, avail);

    println!("max_workers  = {max_workers}");
    println!();

    // Parallel: bounded in-flight work + per-worker bounded queues.
    let exec_par = Executor::new(ExecutorConfig {
        max_workers,
        max_in_flight: 64,
        worker_queue_cap: 2,
    });

    let exec_seq = Executor::new(ExecutorConfig::default());

    // -----------------------------------------------------------------------------
    // 1) Request batch root (SEQ vs PAR)
    // This is the "parallel sells" case: enough independent hashing exists to scale.
    // -----------------------------------------------------------------------------
    println!("=== dag_exec: request BATCH ROOT (seq vs par) ===");
    println!("batch_root_key = {}", keys.batch_root);

    reset_counts(&[&ran_leaf_hash, &ran_internal_hash, &ran_proof_asm]);
    let t0 = Instant::now();
    let out_seq = exec_seq.run_sequential(&dag, std::iter::once(keys.batch_root.clone()))?;
    let dt_seq = t0.elapsed();
    println!("seq batch_root = 0x{}", hex(&out_seq[&keys.batch_root]));
    println!(
        "seq counts: leaf_hash={} internal_hash={} proof_asm={}",
        ran_leaf_hash.load(Ordering::SeqCst),
        ran_internal_hash.load(Ordering::SeqCst),
        ran_proof_asm.load(Ordering::SeqCst),
    );
    println!("seq elapsed = {:?}", dt_seq);

    reset_counts(&[&ran_leaf_hash, &ran_internal_hash, &ran_proof_asm]);
    let t0 = Instant::now();
    let out_par = exec_par.run_parallel(&dag, std::iter::once(keys.batch_root.clone()))?;
    let dt_par = t0.elapsed();
    println!("par batch_root = 0x{}", hex(&out_par[&keys.batch_root]));
    println!(
        "par counts: leaf_hash={} internal_hash={} proof_asm={}",
        ran_leaf_hash.load(Ordering::SeqCst),
        ran_internal_hash.load(Ordering::SeqCst),
        ran_proof_asm.load(Ordering::SeqCst),
    );
    println!("par elapsed = {:?}\n", dt_par);

    // -----------------------------------------------------------------------------
    // 2) Request one chunk root (pruned)
    // This shows partial evaluation clearly: only that chunk gets hashed.
    // -----------------------------------------------------------------------------
    reset_counts(&[&ran_leaf_hash, &ran_internal_hash, &ran_proof_asm]);
    let t0 = Instant::now();
    let out = exec_par.run_parallel(&dag, std::iter::once(keys.target_chunk_root.clone()))?;
    let dt = t0.elapsed();

    let expected_leaf = chunk_size;
    let expected_internal = chunk_size - 1;

    println!("=== dag_exec: request ONE CHUNK ROOT (pruned) ===");
    println!("chunk_root_key = {}", keys.target_chunk_root);
    println!("chunk_root     = 0x{}", hex(&out[&keys.target_chunk_root]));
    println!(
        "counts: leaf_hash={} (expected={}) | internal_hash={} (expected={}) | proof_asm={}",
        ran_leaf_hash.load(Ordering::SeqCst),
        expected_leaf,
        ran_internal_hash.load(Ordering::SeqCst),
        expected_internal,
        ran_proof_asm.load(Ordering::SeqCst),
    );
    println!("elapsed = {:?}\n", dt);

    // -----------------------------------------------------------------------------
    // 3) Request a tx inclusion proof within its chunk (pruned + verifiable)
    // We request both:
    // - chunk root
    // - proof bytes for tx position within that chunk
    // and verify proof against the chunk root.
    // -----------------------------------------------------------------------------
    reset_counts(&[&ran_leaf_hash, &ran_internal_hash, &ran_proof_asm]);
    let t0 = Instant::now();
    let out = exec_par.run_parallel(
        &dag,
        [
            keys.target_chunk_root.clone(),
            keys.target_chunk_proof.clone(),
        ],
    )?;
    let dt = t0.elapsed();

    let chunk_root = &out[&keys.target_chunk_root];
    let proof_bytes = &out[&keys.target_chunk_proof];

    let ok = verify_merkle_proof(chunk_root.as_ref(), proof_bytes.as_ref(), hash_iters)?;

    println!("=== dag_exec: request CHUNK PROOF for tx (pruned + verifiable) ===");
    println!("proof_key  = {}", keys.target_chunk_proof);
    println!("chunk_root = 0x{}", hex(chunk_root.as_ref()));
    println!("proof      = 0x{}  (binary v1)", hex(proof_bytes.as_ref()));
    println!("verify(chunk_root, proof) = {ok}");
    println!(
        "counts: leaf_hash={} (expected={}) | internal_hash={} (expected={}) | proof_asm={} (expected=1)",
        ran_leaf_hash.load(Ordering::SeqCst),
        expected_leaf,
        ran_internal_hash.load(Ordering::SeqCst),
        expected_internal,
        ran_proof_asm.load(Ordering::SeqCst),
    );
    println!("elapsed = {:?}\n", dt);

    // -----------------------------------------------------------------------------
    // “Sales pitch” summary
    // -----------------------------------------------------------------------------
    println!("=== why this sells ===");
    println!("- Naive baseline computes FULL batch even if you only need one chunk root/proof.");
    println!("- dag_exec prunes by output selection: chunk root/proof => hashes only that chunk.");
    println!(
        "- With DAG_EXEC_HASH_ITERS and --release, parallel execution becomes the wall-clock win."
    );
    println!(
        "  naive counts: leaf_hash={} internal_hash={}",
        naive.count_leaf_hash, naive.count_internal_hash
    );
    println!(
        "  pruned counts: leaf_hash={} internal_hash={}",
        expected_leaf, expected_internal
    );

    Ok(())
}

// -------------------------
// DAG build (hierarchical rollup commitment)
// -------------------------

struct Keys {
    batch_root: String,
    target_chunk_root: String,
    target_chunk_proof: String,
}

/// Builds a hierarchical commitment DAG:
/// - tx/{i}          : source bytes (canonical encoding)
/// - leaf/{i}        : H_leaf(tx/{i})
/// - chunk/{c}/node/{lvl}/{idx} : internal nodes inside chunk c
/// - chunk/{c}/root   : stable alias of chunk root
/// - batch/node/{lvl}/{idx}     : nodes in top tree over chunk roots
/// - batch/root       : stable alias of batch root
/// - chunk/{c}/proof/tx/{pos}   : inclusion proof for leaf pos inside chunk c
fn build_rollup_commitment_dag(
    txs: &[Vec<u8>],
    spec: RollupSpec,
    counters: Counters,
) -> Result<(DagT, Keys), BuildErr> {
    let batch_size = txs.len();
    let n_chunks = batch_size / spec.chunk_size;

    let mut b = DagBuilder::<String, Vec<u8>, &'static str>::new();

    // 1) tx sources + leaf commitments
    for (i, tx) in txs.iter().enumerate() {
        let tx_key = tx_key(i);
        let leaf_key = leaf_key(i);

        b.add_source(tx_key.clone(), tx.clone())?;

        let ran = Arc::clone(&counters.ran_leaf_hash);
        b.add_task(leaf_key, vec![tx_key], move |xs| {
            ran.fetch_add(1, Ordering::SeqCst);
            Ok(hash_leaf_with_iters(spec.hash_iters, xs[0].as_ref()))
        })?;
    }

    // 2) chunk trees (each chunk is a Merkle tree over its leaves)
    for c in 0..n_chunks {
        for lvl in 1..=spec.chunk_height {
            let width = spec.chunk_size >> lvl;
            for idx in 0..width {
                let left = chunk_child_key(c, lvl - 1, idx * 2, spec.chunk_size);
                let right = chunk_child_key(c, lvl - 1, idx * 2 + 1, spec.chunk_size);
                let out = chunk_node_key(c, lvl, idx);

                let ran = Arc::clone(&counters.ran_internal_hash);
                b.add_task(out, vec![left, right], move |xs| {
                    ran.fetch_add(1, Ordering::SeqCst);
                    ensure_32(xs[0].as_ref())?;
                    ensure_32(xs[1].as_ref())?;
                    Ok(hash_node_with_iters(
                        spec.hash_iters,
                        xs[0].as_ref(),
                        xs[1].as_ref(),
                    ))
                })?;
            }
        }

        // alias chunk root to a stable name: chunk/{c}/root
        // IMPORTANT: the executor slice provides Arc<Vec<u8>>, so we must clone the inner Vec<u8>.
        let root_node = chunk_node_key(c, spec.chunk_height, 0);
        let root_alias = chunk_root_key(c);
        b.add_task(root_alias, vec![root_node], |xs| Ok(xs[0].as_ref().clone()))?;
    }

    // 3) top tree over chunk roots => batch root
    for lvl in 1..=spec.batch_height {
        let width = n_chunks >> lvl;
        for idx in 0..width {
            let left = batch_child_key(lvl - 1, idx * 2);
            let right = batch_child_key(lvl - 1, idx * 2 + 1);
            let out = batch_node_key(lvl, idx);

            let ran = Arc::clone(&counters.ran_internal_hash);
            b.add_task(out, vec![left, right], move |xs| {
                ran.fetch_add(1, Ordering::SeqCst);
                ensure_32(xs[0].as_ref())?;
                ensure_32(xs[1].as_ref())?;
                Ok(hash_node_with_iters(
                    spec.hash_iters,
                    xs[0].as_ref(),
                    xs[1].as_ref(),
                ))
            })?;
        }
    }

    // alias batch root
    let batch_root_node = batch_node_key(spec.batch_height, 0);
    let batch_root_alias = batch_root_key();
    b.add_task(batch_root_alias.clone(), vec![batch_root_node], |xs| {
        Ok(xs[0].as_ref().clone())
    })?;

    // 4) proof task: inclusion proof for target tx within its chunk
    let target_chunk = spec.target_tx / spec.chunk_size;
    let pos = spec.target_tx % spec.chunk_size;
    let proof_key = chunk_proof_key(target_chunk, pos);

    // deps: leaf hash + sibling hashes for each level inside the chunk
    let mut deps = Vec::with_capacity(1 + spec.chunk_height);
    deps.push(leaf_key(spec.target_tx));
    for lvl in 0..spec.chunk_height {
        deps.push(chunk_sibling_key(target_chunk, pos, lvl, spec.chunk_size));
    }

    let ran = Arc::clone(&counters.ran_proof_asm);
    b.add_task(proof_key.clone(), deps, move |xs| {
        ran.fetch_add(1, Ordering::SeqCst);

        // xs[0] = leaf hash, xs[1..] = siblings at levels 0..chunk_height-1
        ensure_32(xs[0].as_ref())?;
        for sib in xs.iter().skip(1) {
            ensure_32(sib.as_ref())?;
        }

        let index = pos as u32;
        let height = spec.chunk_height as u8;
        let proof = encode_merkle_proof_v1(index, height, xs[0].as_ref(), &xs[1..]);
        Ok(proof)
    })?;

    let dag = b.build()?;

    Ok((
        dag,
        Keys {
            batch_root: batch_root_alias,
            target_chunk_root: chunk_root_key(target_chunk),
            target_chunk_proof: proof_key,
        },
    ))
}

// -------------------------
// Keys
// -------------------------

fn tx_key(i: usize) -> String {
    format!("tx/{i}")
}

fn leaf_key(i: usize) -> String {
    format!("leaf/{i}")
}

fn chunk_node_key(chunk: usize, lvl: usize, idx: usize) -> String {
    format!("chunk/{chunk}/node/{lvl}/{idx}")
}

fn chunk_root_key(chunk: usize) -> String {
    format!("chunk/{chunk}/root")
}

fn batch_node_key(lvl: usize, idx: usize) -> String {
    format!("batch/node/{lvl}/{idx}")
}

fn batch_root_key() -> String {
    "batch/root".to_string()
}

fn chunk_proof_key(chunk: usize, pos: usize) -> String {
    format!("chunk/{chunk}/proof/tx/{pos}")
}

/// lvl=0 children are global leaf hashes leaf/{global_i}
fn chunk_child_key(chunk: usize, lvl: usize, idx: usize, chunk_size: usize) -> String {
    if lvl == 0 {
        let global_i = chunk * chunk_size + idx;
        leaf_key(global_i)
    } else {
        chunk_node_key(chunk, lvl, idx)
    }
}

/// Sibling key at level within chunk:
/// - level 0 sibling is another leaf (leaf/{global_i})
/// - level >=1 sibling is chunk/{c}/node/{level}/{sib_idx}
fn chunk_sibling_key(chunk: usize, pos_in_chunk: usize, level: usize, chunk_size: usize) -> String {
    let idx_at_level = pos_in_chunk >> level;
    let sib_idx = idx_at_level ^ 1;

    if level == 0 {
        let global = chunk * chunk_size + sib_idx;
        leaf_key(global)
    } else {
        chunk_node_key(chunk, level, sib_idx)
    }
}

/// batch tree leaves are chunk roots: chunk/{c}/root
fn batch_child_key(lvl: usize, idx: usize) -> String {
    if lvl == 0 {
        chunk_root_key(idx)
    } else {
        batch_node_key(lvl, idx)
    }
}

// -------------------------
// Proof format (binary, verifiable)
// -------------------------

/// Proof bytes v1:
/// [0]      u8  version=1
/// [1]      u8  height
/// [2..6]   u32 index (LE)
/// [6..38]  leaf_hash (32)
/// then for each level 0..height-1:
///   [side_bit: u8] [sibling_hash: 32]
///
/// side_bit at level L is the L-th bit of index:
/// - 0 => current hash is left child, sibling is right
/// - 1 => sibling is left child, current is right
fn encode_merkle_proof_v1(
    index: u32,
    height: u8,
    leaf_hash: &[u8],
    siblings: &[Arc<Vec<u8>>],
) -> Vec<u8> {
    debug_assert_eq!(leaf_hash.len(), 32);
    debug_assert_eq!(siblings.len(), height as usize);

    let mut out = Vec::with_capacity(1 + 1 + 4 + 32 + siblings.len() * (1 + 32));
    out.push(1u8);
    out.push(height);
    out.extend_from_slice(&index.to_le_bytes());
    out.extend_from_slice(leaf_hash);

    for (lvl, sib) in siblings.iter().enumerate() {
        let bit = ((index as usize) >> lvl) & 1;
        out.push(bit as u8);
        out.extend_from_slice(sib.as_ref());
    }

    out
}

fn verify_merkle_proof(root: &[u8], proof: &[u8], hash_iters: usize) -> Result<bool, &'static str> {
    if root.len() != 32 {
        return Err("root must be 32 bytes");
    }
    if proof.len() < 1 + 1 + 4 + 32 {
        return Err("proof too short");
    }
    if proof[0] != 1 {
        return Err("unsupported proof version");
    }

    let height = proof[1] as usize;
    let idx = u32::from_le_bytes([proof[2], proof[3], proof[4], proof[5]]);

    let mut offset = 6;
    let mut cur = proof[offset..offset + 32].to_vec();
    offset += 32;

    for lvl in 0..height {
        if offset + 1 + 32 > proof.len() {
            return Err("proof truncated");
        }
        let side = proof[offset];
        offset += 1;

        let sibling = &proof[offset..offset + 32];
        offset += 32;

        let expected = ((idx as usize) >> lvl) & 1;
        if side as usize != expected {
            return Err("side bit does not match index bit");
        }

        cur = if side == 0 {
            hash_node_with_iters(hash_iters, &cur, sibling)
        } else if side == 1 {
            hash_node_with_iters(hash_iters, sibling, &cur)
        } else {
            return Err("invalid side bit");
        };
    }

    Ok(cur.as_slice() == root)
}

// -------------------------
// Naive baseline (sequential full compute)
// -------------------------

struct Naive {
    batch_root: Vec<u8>,
    count_leaf_hash: usize,
    count_internal_hash: usize,
}

/// Always computes:
/// - all leaf hashes
/// - all chunk trees
/// - top tree => batch root
fn naive_full_batch_commitment(txs: &[Vec<u8>], chunk_size: usize, hash_iters: usize) -> Naive {
    let batch_size = txs.len();
    let n_chunks = batch_size / chunk_size;

    let mut count_leaf = 0usize;
    let mut count_internal = 0usize;

    let mut leaf_hashes: Vec<Vec<u8>> = Vec::with_capacity(batch_size);
    for tx in txs {
        count_leaf += 1;
        leaf_hashes.push(hash_leaf_with_iters(hash_iters, tx));
    }

    let mut chunk_roots: Vec<Vec<u8>> = Vec::with_capacity(n_chunks);
    for c in 0..n_chunks {
        let start = c * chunk_size;
        let end = start + chunk_size;
        let (root, internal) = naive_merkle_root(&leaf_hashes[start..end], hash_iters);
        count_internal += internal;
        chunk_roots.push(root);
    }

    let (batch_root, internal) = naive_merkle_root(&chunk_roots, hash_iters);
    count_internal += internal;

    Naive {
        batch_root,
        count_leaf_hash: count_leaf,
        count_internal_hash: count_internal,
    }
}

/// Returns (root, internal_hash_count)
fn naive_merkle_root(leaves: &[Vec<u8>], hash_iters: usize) -> (Vec<u8>, usize) {
    assert!(leaves.len().is_power_of_two() && leaves.len() >= 2);
    for l in leaves {
        assert_eq!(l.len(), 32);
    }

    let mut level = leaves.to_vec();
    let mut internal = 0usize;

    while level.len() > 1 {
        let mut next = Vec::with_capacity(level.len() / 2);
        for pair in level.chunks_exact(2) {
            internal += 1;
            next.push(hash_node_with_iters(hash_iters, &pair[0], &pair[1]));
        }
        level = next;
    }

    (level[0].clone(), internal)
}
