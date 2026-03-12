use std::{
    collections::{HashMap, VecDeque},
    hash::Hash,
    sync::Arc,
};

use super::{
    common::{ExecArtifacts, build_kahn_metadata, collect_outputs, mark_needed},
    observe::{ExecObserver, NoopObserver},
};
use crate::{
    error::ExecError,
    graph::{Dag, NodeId, NodeKind},
};

#[cfg(feature = "execution-trace")]
use crate::trace::{TraceObserver, TracedExecution};

/// Purpose: execute the needed subgraph once and optionally record scheduler telemetry.
fn execute<K, O, E, R>(
    dag: &Dag<K, O, E>,
    out_keys: &[K],
    observer: &mut R,
) -> Result<ExecArtifacts<O>, ExecError<K, E>>
where
    K: Eq + Hash + Clone,
    O: Send + Sync + 'static,
    R: ExecObserver,
{
    if out_keys.is_empty() {
        return Err(ExecError::InternalInvariant("outputs must be non-empty"));
    }

    let needed = mark_needed(dag, out_keys)?;
    let (dependents, mut indeg, needed_count) = build_kahn_metadata(dag, &needed);

    // Values indexed by NodeId
    let mut vals: Vec<Option<Arc<O>>> = vec![None; dag.nodes.len()];
    let mut q: VecDeque<NodeId> = VecDeque::new();

    // Seed: nodes with indeg=0 in the needed subgraph
    for (i, is_needed) in needed.iter().enumerate() {
        if *is_needed && indeg[i] == 0 {
            let id = NodeId(i);
            q.push_back(id);
            observer.mark_ready(id, q.len());
        }
    }

    let mut processed = 0usize;

    while let Some(id) = q.pop_front() {
        processed += 1;
        observer.mark_start(id, None);

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

        observer.mark_finish(id);

        // Release dependents
        for &dst in &dependents[id.0] {
            indeg[dst.0] -= 1;
            if indeg[dst.0] == 0 {
                q.push_back(dst);
                observer.mark_ready(dst, q.len());
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

    Ok((vals, needed))
}

/// Compute only the requested outputs (and their transitive deps).
pub(crate) fn run<K, O, E>(
    dag: &Dag<K, O, E>,
    out_keys: Vec<K>,
) -> Result<HashMap<K, Arc<O>>, ExecError<K, E>>
where
    K: Eq + Hash + Clone,
    O: Send + Sync + 'static,
{
    let (vals, _) = execute(dag, &out_keys, &mut NoopObserver)?;
    collect_outputs(dag, &out_keys, vals)
}

#[cfg(feature = "execution-trace")]
pub(crate) fn run_traced<K, O, E>(
    dag: &Dag<K, O, E>,
    out_keys: Vec<K>,
) -> Result<TracedExecution<K, O>, ExecError<K, E>>
where
    K: Eq + Hash + Clone,
    O: Send + Sync + 'static,
{
    let mut observer = TraceObserver::new(dag.nodes.len());
    let (vals, needed) = execute(dag, &out_keys, &mut observer)?;
    let outputs = collect_outputs(dag, &out_keys, vals)?;
    let trace = observer.finalize(dag, &needed);

    Ok(TracedExecution { outputs, trace })
}
