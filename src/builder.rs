use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

use crate::error::BuildError;
use crate::graph::{Dag, Node, NodeId, NodeKind, TaskFn};

pub struct DagBuilder<K, O, E>
where
    K: Eq + Hash + Clone,
{
    index: HashMap<K, usize>, // key -> staging idx
    staging: Vec<StagedNode<K, O, E>>,
}

struct StagedNode<K, O, E> {
    key: K,
    deps: Vec<K>,
    kind: StagedKind<O, E>,
}

enum StagedKind<O, E> {
    Source(Arc<O>),
    Task(Box<TaskFn<O, E>>),
}

impl<K, O, E> DagBuilder<K, O, E>
where
    K: Eq + Hash + Clone,
{
    pub fn new() -> Self {
        Self {
            index: HashMap::new(),
            staging: Vec::new(),
        }
    }

    pub fn add_source(&mut self, key: K, value: O) -> Result<(), BuildError<K>> {
        self.insert_node(key, vec![], StagedKind::Source(Arc::new(value)))
    }

    pub fn add_task<F>(&mut self, key: K, deps: Vec<K>, f: F) -> Result<(), BuildError<K>>
    where
        F: Fn(&[Arc<O>]) -> Result<O, E> + Send + Sync + 'static,
    {
        self.insert_node(key, deps, StagedKind::Task(Box::new(f)))
    }

    fn insert_node(
        &mut self,
        key: K,
        deps: Vec<K>,
        kind: StagedKind<O, E>,
    ) -> Result<(), BuildError<K>> {
        if self.index.contains_key(&key) {
            return Err(BuildError::DuplicateKey(key));
        }
        let idx = self.staging.len();
        self.index.insert(key.clone(), idx);
        self.staging.push(StagedNode { key, deps, kind });
        Ok(())
    }

    pub fn build(self) -> Result<Dag<K, O, E>, BuildError<K>> {
        if self.staging.is_empty() {
            return Err(BuildError::EmptyGraph);
        }

        // Assign dense NodeId
        let mut key_to_id: HashMap<K, NodeId> = HashMap::with_capacity(self.staging.len());
        for (i, n) in self.staging.iter().enumerate() {
            key_to_id.insert(n.key.clone(), NodeId(i));
        }

        // Resolve deps to NodeId with missing-dep validation
        let mut nodes: Vec<Node<K, O, E>> = Vec::with_capacity(self.staging.len());
        for n in self.staging {
            let mut dep_ids = Vec::with_capacity(n.deps.len());
            for dep_key in n.deps.iter() {
                let dep_id =
                    key_to_id
                        .get(dep_key)
                        .ok_or_else(|| BuildError::MissingDependency {
                            task: n.key.clone(),
                            missing: dep_key.clone(),
                        })?;
                dep_ids.push(*dep_id);
            }

            let kind = match n.kind {
                StagedKind::Source(v) => NodeKind::Source(v),
                StagedKind::Task(f) => NodeKind::Task(f),
            };

            nodes.push(Node {
                key: n.key,
                deps: dep_ids,
                kind,
            });
        }

        Ok(Dag {
            nodes,
            index: key_to_id,
        })
    }
}

impl<K, O, E> Default for DagBuilder<K, O, E>
where
    K: Eq + std::hash::Hash + Clone,
{
    fn default() -> Self {
        Self::new()
    }
}
