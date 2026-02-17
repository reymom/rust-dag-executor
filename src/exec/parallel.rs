use std::{collections::HashMap, hash::Hash, sync::Arc};

use crate::error::ExecError;
use crate::graph::{Dag, ExecutorConfig};

pub(crate) fn run<K, O, E>(
    _dag: &Dag<K, O, E>,
    _cfg: &ExecutorConfig,
    _out_keys: Vec<K>,
) -> Result<HashMap<K, Arc<O>>, ExecError<K, E>>
where
    K: Eq + Hash + Clone,
    O: Send + Sync + 'static,
    E: Send + 'static,
{
    todo!()
}
