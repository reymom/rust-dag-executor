use std::{collections::HashMap, hash::Hash, sync::Arc};

use crate::error::ExecError;
use crate::graph::{Dag, NodeId};

pub(super) fn mark_needed<K, O, E>(
    dag: &Dag<K, O, E>,
    outputs: &[K],
) -> Result<Vec<bool>, ExecError<K, E>>
where
    K: Eq + Hash + Clone,
{
    let mut needed = vec![false; dag.nodes.len()];
    let mut stack: Vec<NodeId> = Vec::new();

    for k in outputs {
        let id = *dag
            .index
            .get(k)
            .ok_or_else(|| ExecError::OutputMissing(k.clone()))?;
        stack.push(id);
    }

    while let Some(id) = stack.pop() {
        if needed[id.0] {
            continue;
        }
        needed[id.0] = true;
        for dep in dag.nodes[id.0].deps.iter() {
            stack.push(*dep);
        }
    }

    Ok(needed)
}

pub(super) fn build_kahn_metadata<K, O, E>(
    dag: &Dag<K, O, E>,
    needed: &[bool],
) -> (Vec<Vec<NodeId>>, Vec<usize>, usize) {
    let n = dag.nodes.len();
    let mut dependents: Vec<Vec<NodeId>> = vec![Vec::new(); n];
    let mut indeg: Vec<usize> = vec![0; n];

    let mut needed_count = 0usize;
    for i in 0..n {
        if !needed[i] {
            continue;
        }
        needed_count += 1;
        indeg[i] = dag.nodes[i].deps.len();
        for dep in dag.nodes[i].deps.iter() {
            dependents[dep.0].push(NodeId(i));
        }
    }

    (dependents, indeg, needed_count)
}

pub(super) fn collect_outputs<K, O, E>(
    dag: &Dag<K, O, E>,
    out_keys: Vec<K>,
    vals: Vec<Option<Arc<O>>>,
) -> Result<HashMap<K, Arc<O>>, ExecError<K, E>>
where
    K: Eq + Hash + Clone,
{
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
