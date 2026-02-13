# dag_exec

Sync DAG executor for CPU-heavy pipelines.

**Features**

- Partial evaluation: compute only requested outputs (prunes unused subgraph)
- (Soon) bounded parallelism / backpressure (std-only)
- Cycle detection, missing deps, duplicate keys

## Status

Early WIP. API may change before `publish = true`.

## Example (coming)

Merkle-style DAG → request root only.
