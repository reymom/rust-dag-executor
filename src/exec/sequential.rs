use std::{
    collections::{HashMap, VecDeque},
    hash::Hash,
    sync::Arc,
};

use super::common::{build_kahn_metadata, mark_needed};
use crate::error::ExecError;
use crate::graph::{Dag, NodeId, NodeKind};

/// Compute only the requested outputs (and their transitive deps).
pub(crate) fn run<K, O, E>(
    dag: &Dag<K, O, E>,
    out_keys: Vec<K>,
) -> Result<HashMap<K, Arc<O>>, ExecError<K, E>>
where
    K: Eq + Hash + Clone,
    O: Send + Sync + 'static,
{
    if out_keys.is_empty() {
        return Err(ExecError::InternalInvariant("outputs must be non-empty"));
    }

    let needed = mark_needed(dag, &out_keys)?;
    let (dependents, mut indeg, needed_count) = build_kahn_metadata(dag, &needed);

    // Values indexed by NodeId
    let mut vals: Vec<Option<Arc<O>>> = vec![None; dag.nodes.len()];
    let mut q: VecDeque<NodeId> = VecDeque::new();

    // Seed: nodes with indeg=0 in the needed subgraph
    for (i, is_needed) in needed.iter().enumerate() {
        if *is_needed && indeg[i] == 0 {
            q.push_back(NodeId(i));
        }
    }

    let mut processed = 0usize;

    while let Some(id) = q.pop_front() {
        processed += 1;

        // Compute if not source already present
        match &dag.nodes[id.0].kind {
            NodeKind::Source(v) => {
                vals[id.0] = Some(Arc::clone(v));
            }
            NodeKind::Task(f) => {
                let deps = &dag.nodes[id.0].deps;
                let mut args: Vec<Arc<O>> = Vec::with_capacity(deps.len());
                for dep in deps {
                    let v = vals[dep.0]
                        .as_ref()
                        .ok_or_else(|| ExecError::InternalInvariant("dep missing during exec"))?;
                    args.push(Arc::clone(v));
                }
                let out = f(&args).map_err(|e| ExecError::TaskFailed {
                    task: dag.nodes[id.0].key.clone(),
                    error: e,
                })?;
                vals[id.0] = Some(Arc::new(out));
            }
        }

        // Release dependents
        for &dst in dependents[id.0].iter() {
            indeg[dst.0] -= 1;
            if indeg[dst.0] == 0 {
                q.push_back(dst);
            }
        }
    }

    if processed != needed_count {
        let mut remaining = Vec::new();
        for (i, is_needed) in needed.iter().enumerate() {
            if *is_needed && indeg[i] > 0 {
                remaining.push(dag.nodes[i].key.clone());
            }
        }
        return Err(ExecError::Cycle { remaining });
    }

    // Return only requested outputs
    let mut out = HashMap::with_capacity(out_keys.len());
    for k in out_keys {
        let id = *dag
            .index
            .get(&k)
            .ok_or_else(|| ExecError::OutputMissing(k.clone()))?;
        let v = vals[id.0]
            .as_ref()
            .ok_or_else(|| ExecError::OutputMissing(k.clone()))?;
        out.insert(k, Arc::clone(v));
    }
    Ok(out)
}
