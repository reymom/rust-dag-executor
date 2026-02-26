//! Shared helpers for examples.
//!
//! IMPORTANT: the “hash” functions here are a deterministic workload shaper,
//! NOT cryptographically secure. They exist to keep examples self-contained
//! while still being CPU-ish for parallelism demos.

#![allow(dead_code)]

use std::{
    env,
    sync::atomic::{AtomicUsize, Ordering},
};

/// Parse an env var as usize, returning default on missing/invalid.
pub fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(default)
}

/// Reset multiple counters in one call (SeqCst because this is demo/test-like).
pub fn reset_counts(counts: &[&AtomicUsize]) {
    for c in counts {
        c.store(0, Ordering::SeqCst);
    }
}

/// Hex encode bytes (lowercase, no 0x prefix).
pub fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        use std::fmt::Write as _;
        let _ = write!(s, "{:02x}", b);
    }
    s
}

/// log2 for power-of-two sizes.
pub fn log2_pow2(x: usize) -> usize {
    debug_assert!(x.is_power_of_two() && x > 0);
    x.trailing_zeros() as usize
}

pub fn ensure_32(x: &[u8]) -> Result<(), &'static str> {
    if x.len() == 32 {
        Ok(())
    } else {
        Err("expected 32-byte hash")
    }
}

pub fn u64_to_le_bytes(x: u64) -> Vec<u8> {
    x.to_le_bytes().to_vec()
}

pub fn read_u64_le(bytes: &[u8]) -> Result<u64, &'static str> {
    if bytes.len() != 8 {
        return Err("expected 8 bytes for u64");
    }
    let mut arr = [0u8; 8];
    arr.copy_from_slice(bytes);
    Ok(u64::from_le_bytes(arr))
}

/// Deterministic fake “canonical tx encoding” (RLP/SSZ/scale/etc in real systems).
pub fn fake_encoded_tx(i: usize) -> Vec<u8> {
    let from = format!("0x{:040x}", 0x1000u64 + (i as u64));
    let to = format!("0x{:040x}", 0x2000u64 + (i as u64));
    let nonce = i as u64;
    let value = 1_000_000u64 + (i as u64);
    format!("from={from}|to={to}|nonce={nonce}|value={value}").into_bytes()
}

/// Domain-separated “leaf commitment”.
pub fn hash_leaf_with_iters(iters: usize, tx_bytes: &[u8]) -> Vec<u8> {
    toy_hash256(iters, 0x00, &[tx_bytes]).to_vec()
}

/// Domain-separated “internal node hash”.
pub fn hash_node_with_iters(iters: usize, left: &[u8], right: &[u8]) -> Vec<u8> {
    toy_hash256(iters, 0x01, &[left, right]).to_vec()
}

/// Generic domain-separated hash for demos (e.g. “risk score”, “receipt”).
pub fn hash_domain_with_iters(iters: usize, domain: u8, parts: &[&[u8]]) -> Vec<u8> {
    toy_hash256(iters, domain, parts).to_vec()
}

/// NOT cryptographically secure. Deterministic + “hash-shaped” CPU work for examples/benches.
fn toy_hash256(iters: usize, domain: u8, parts: &[&[u8]]) -> [u8; 32] {
    let mut x = 0xcbf29ce484222325u64 ^ (domain as u64);

    for p in parts {
        x ^= p.len() as u64;
        for &b in *p {
            x = mix64(x ^ (b as u64));
        }
        x = mix64(x ^ 0x9e3779b97f4a7c15u64);
    }

    // Heavy-hash knob: extra mixing per hash call.
    for _ in 0..iters {
        x = mix64(x ^ 0x9e3779b97f4a7c15u64);
    }

    let mut out = [0u8; 32];
    let mut y = x;
    for i in 0..4 {
        y = mix64(y ^ (i as u64));
        out[i * 8..(i + 1) * 8].copy_from_slice(&y.to_le_bytes());
    }
    out
}

fn mix64(mut x: u64) -> u64 {
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58476d1ce4e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}
