mod common;
pub(crate) mod observe;
mod parallel;
mod sequential;

use std::{collections::HashMap, hash::Hash, sync::Arc};

use crate::{
    error::ExecError,
    graph::{Dag, ExecutorConfig},
};

#[cfg(feature = "execution-trace")]
use crate::trace::TracedExecution;

/// Executes a compiled `Dag` either sequentially or with a bounded std-thread worker pool.
pub struct Executor {
    cfg: ExecutorConfig,
}

impl Executor {
    pub fn new(cfg: ExecutorConfig) -> Self {
        Self { cfg }
    }

    pub fn run_sequential<K, O, E, I>(
        &self,
        dag: &Dag<K, O, E>,
        outputs: I,
    ) -> Result<HashMap<K, Arc<O>>, ExecError<K, E>>
    where
        K: Eq + Hash + Clone,
        I: IntoIterator<Item = K>,
        O: Send + Sync + 'static,
    {
        let out_keys: Vec<K> = outputs.into_iter().collect();
        sequential::run(dag, out_keys)
    }

    #[cfg(feature = "execution-trace")]
    pub fn run_sequential_traced<K, O, E, I>(
        &self,
        dag: &Dag<K, O, E>,
        outputs: I,
    ) -> Result<TracedExecution<K, O>, ExecError<K, E>>
    where
        K: Eq + Hash + Clone,
        I: IntoIterator<Item = K>,
        O: Send + Sync + 'static,
    {
        let out_keys: Vec<K> = outputs.into_iter().collect();
        sequential::run_traced(dag, out_keys)
    }

    pub fn run_parallel<K, O, E, I>(
        &self,
        dag: &Dag<K, O, E>,
        outputs: I,
    ) -> Result<HashMap<K, Arc<O>>, ExecError<K, E>>
    where
        K: Eq + Hash + Clone,
        I: IntoIterator<Item = K>,
        O: Send + Sync + 'static,
        E: Send + 'static,
    {
        let out_keys: Vec<K> = outputs.into_iter().collect();
        parallel::run(dag, &self.cfg, out_keys)
    }

    #[cfg(feature = "execution-trace")]
    pub fn run_parallel_traced<K, O, E, I>(
        &self,
        dag: &Dag<K, O, E>,
        outputs: I,
    ) -> Result<TracedExecution<K, O>, ExecError<K, E>>
    where
        K: Eq + Hash + Clone,
        I: IntoIterator<Item = K>,
        O: Send + Sync + 'static,
        E: Send + 'static,
    {
        let out_keys: Vec<K> = outputs.into_iter().collect();
        parallel::run_traced(dag, &self.cfg, out_keys)
    }
}
