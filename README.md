# dag_exec

Sync DAG executor for CPU-heavy pipelines: **bounded parallelism** + **partial evaluation**.

## Why

When you have a dependency graph of expensive work (hashing, decoding, proof steps, indexing stages),
you often want:

- **compute only requested outputs** (prune everything else)
- **run in parallel** without bringing an async runtime
- **control backpressure** (cap in-flight work)

`dag_exec` is a small, std-only crate focused on that niche.

## Features

- **Partial evaluation**: computes only requested outputs (prunes unused subgraph)
- **Sequential executor**: Kahn-style topo scheduling + cycle detection
- **Parallel executor (std threads)**: work dispatch to a worker pool (RAII shutdown)
- Validation: missing deps, duplicate keys, empty graph

## Status

Early WIP. API may change before `publish = true`.

## Minimal example

```rust
use dag_exec::{DagBuilder, Executor, ExecutorConfig};
use std::sync::Arc;

let mut b = DagBuilder::<String, i32, ()>::new();
b.add_source("a".into(), 1)?;
b.add_source("b".into(), 2)?;
b.add_task("c".into(), vec!["a".into(), "b".into()], |xs: &[Arc<i32>]| Ok(*xs[0] + *xs[1]))?;
let dag = b.build()?;

let exec = Executor::new(ExecutorConfig::default());
let out = exec.run_sequential(&dag, ["c".to_string()])?;
assert_eq!(*out["c"], 3);
# Ok::<(), dag_exec::BuildError<String>>(())
```

## Roadmap

- Enforce **max_in_flight** via `sync_channel` + `try_send`
- Add tests for parallel correctness + backpressure invariants
- Add example(s): Merkle-style DAG, pipeline DAG
- Add benches (criterion) once API stabilizes
