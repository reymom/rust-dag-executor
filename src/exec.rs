use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::sync::Arc;

use crate::error::ExecError;
use crate::graph::{Dag, NodeId, NodeKind};

impl<K, O, E> Dag<K, O, E>
where
    K: Eq + Hash + Clone,
{
    /// Compute only the requested outputs (and their transitive deps).
    pub fn run_sequential<I>(&self, outputs: I) -> Result<HashMap<K, Arc<O>>, ExecError<K, E>>
    where
        I: IntoIterator<Item = K>,
        O: Send + Sync + 'static,
    {
        let out_keys: Vec<K> = outputs.into_iter().collect();
        if out_keys.is_empty() {
            return Err(ExecError::InternalInvariant("outputs must be non-empty"));
        }

        let needed = self.mark_needed(&out_keys)?;
        let (dependents, mut indeg, needed_count) = self.build_kahn_metadata(&needed);

        // Values indexed by NodeId
        let mut vals: Vec<Option<Arc<O>>> = vec![None; self.nodes.len()];
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
            match &self.nodes[id.0].kind {
                NodeKind::Source(v) => {
                    vals[id.0] = Some(Arc::clone(v));
                }
                NodeKind::Task(f) => {
                    let deps = &self.nodes[id.0].deps;
                    let mut args: Vec<Arc<O>> = Vec::with_capacity(deps.len());
                    for dep in deps {
                        let v = vals[dep.0].as_ref().ok_or_else(|| {
                            ExecError::InternalInvariant("dep missing during exec")
                        })?;
                        args.push(Arc::clone(v));
                    }
                    let out = f(&args).map_err(|e| ExecError::TaskFailed {
                        task: self.nodes[id.0].key.clone(),
                        error: e,
                    })?;
                    vals[id.0] = Some(Arc::new(out));
                }
            }

            // Release dependents
            for &dst in dependents[id.0].iter() {
                if !needed[dst.0] {
                    continue;
                }
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
                    remaining.push(self.nodes[i].key.clone());
                }
            }
            return Err(ExecError::Cycle { remaining });
        }

        // Return only requested outputs
        let mut out = HashMap::with_capacity(out_keys.len());
        for k in out_keys {
            let id = *self
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

    fn mark_needed(&self, outputs: &[K]) -> Result<Vec<bool>, ExecError<K, E>> {
        let mut needed = vec![false; self.nodes.len()];
        let mut stack: Vec<NodeId> = Vec::new();

        for k in outputs {
            let id = *self
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
            for dep in self.nodes[id.0].deps.iter() {
                stack.push(*dep);
            }
        }

        Ok(needed)
    }

    fn build_kahn_metadata(&self, needed: &[bool]) -> (Vec<Vec<NodeId>>, Vec<usize>, usize) {
        let n = self.nodes.len();
        let mut dependents: Vec<Vec<NodeId>> = vec![Vec::new(); n];
        let mut indeg: Vec<usize> = vec![0; n];

        let mut needed_count = 0usize;
        for i in 0..n {
            if !needed[i] {
                continue;
            }
            needed_count += 1;
            indeg[i] = self.nodes[i].deps.len();
            for dep in self.nodes[i].deps.iter() {
                dependents[dep.0].push(NodeId(i));
            }
        }

        (dependents, indeg, needed_count)
    }
}
