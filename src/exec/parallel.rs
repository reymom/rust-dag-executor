use std::{
    collections::{HashMap, VecDeque},
    hash::Hash,
    sync::{Arc, mpsc},
    thread,
};

use crate::error::ExecError;
use crate::exec::common::{build_kahn_metadata, mark_needed};
use crate::graph::{Dag, ExecutorConfig, NodeId, NodeKind, TaskFn};

struct Task<O, E> {
    id: NodeId,
    func: Arc<TaskFn<O, E>>,
    args: Vec<Arc<O>>,
}

struct TaskResult<O, E> {
    id: NodeId,
    out: Result<O, E>,
}

struct WorkerPool<O, E> {
    task_txs: Option<Vec<mpsc::Sender<Task<O, E>>>>,
    workers: Vec<thread::JoinHandle<()>>,
}

impl<O, E> WorkerPool<O, E>
where
    O: Send + Sync + 'static,
    E: Send + 'static,
{
    /// Spawn N workers with N independent task queues (each queue is mpsc: scheduler -> worker).
    /// Returns (task_senders, join_handles).
    fn spawn(res_tx: mpsc::Sender<TaskResult<O, E>>, n_workers: usize) -> Self {
        let mut task_txs = Vec::with_capacity(n_workers);
        let mut workers = Vec::with_capacity(n_workers);

        for _ in 0..n_workers {
            let (task_tx, task_rx) = mpsc::channel::<Task<O, E>>();
            task_txs.push(task_tx);

            let res_tx = res_tx.clone();
            let j = thread::spawn(move || {
                // Consume tasks until scheduler drops the Sender.
                for task in task_rx {
                    let out = (task.func)(&task.args);

                    // If scheduler went away (receiver dropped), exit.
                    if res_tx.send(TaskResult { id: task.id, out }).is_err() {
                        break;
                    }
                }
            });

            workers.push(j);
        }

        Self {
            task_txs: Some(task_txs),
            workers,
        }
    }

    fn senders(&self) -> Option<&[mpsc::Sender<Task<O, E>>]> {
        self.task_txs.as_deref()
    }
}

impl<O, E> Drop for WorkerPool<O, E> {
    fn drop(&mut self) {
        // Close task channels so workers exit their recv loops.
        let _ = self.task_txs.take();

        // Join to avoid detaching threads on drop.
        for h in self.workers.drain(..) {
            let _ = h.join();
        }
    }
}

pub(crate) fn run<K, O, E>(
    dag: &Dag<K, O, E>,
    cfg: &ExecutorConfig,
    out_keys: Vec<K>,
) -> Result<HashMap<K, Arc<O>>, ExecError<K, E>>
where
    K: Eq + Hash + Clone,
    O: Send + Sync + 'static,
    E: Send + 'static,
{
    if out_keys.is_empty() {
        return Err(ExecError::InternalInvariant("outputs must be non-empty"));
    }
    if cfg.max_workers == 0 {
        return Err(ExecError::InternalInvariant("max_workers must be > 0"));
    }

    let needed = mark_needed(dag, &out_keys)?;
    let (dependents, mut indeg, needed_count) = build_kahn_metadata(dag, &needed);

    // Values indexed by NodeId
    let mut vals: Vec<Option<Arc<O>>> = vec![None; dag.nodes.len()];

    // Seed: nodes with indeg=0 in the needed subgraph
    let mut ready: VecDeque<NodeId> = VecDeque::new();
    for (i, is_needed) in needed.iter().enumerate() {
        if *is_needed && indeg[i] == 0 {
            ready.push_back(NodeId(i));
        }
    }

    // Worker pool wiring
    let (res_tx, res_rx) = mpsc::channel::<TaskResult<O, E>>();
    let pool = WorkerPool::spawn(res_tx, cfg.max_workers);

    let mut processed = 0usize;
    let mut in_flight = 0usize;
    let mut next_worker = 0usize;

    while processed < needed_count {
        // Dispatch all currently-ready nodes.
        while let Some(id) = ready.pop_front() {
            match &dag.nodes[id.0].kind {
                NodeKind::Source(v) => {
                    vals[id.0] = Some(Arc::clone(v));
                    processed += 1;

                    // Release dependents
                    for &dst in dependents[id.0].iter() {
                        debug_assert!(indeg[dst.0] > 0);
                        indeg[dst.0] -= 1;
                        if indeg[dst.0] == 0 {
                            ready.push_back(dst);
                        }
                    }
                }
                NodeKind::Task(f) => {
                    let deps = &dag.nodes[id.0].deps;
                    let mut args: Vec<Arc<O>> = Vec::with_capacity(deps.len());
                    for dep in deps {
                        let v = vals[dep.0].as_ref().ok_or_else(|| {
                            ExecError::InternalInvariant("dep missing during exec")
                        })?;
                        args.push(Arc::clone(v));
                    }

                    let senders = pool
                        .senders()
                        .ok_or_else(|| ExecError::InternalInvariant("worker pool is closed"))?;

                    dispatch_task(
                        senders,
                        &mut next_worker,
                        Task {
                            id,
                            func: Arc::clone(f),
                            args,
                        },
                    )
                    .map_err(|_| {
                        ExecError::InternalInvariant("worker task channel disconnected")
                    })?;
                    in_flight += 1;
                }
            }
        }

        if in_flight == 0 {
            // No ready nodes and nothing running => cycle in needed subgraph.
            let mut remaining = Vec::new();
            for (i, is_needed) in needed.iter().enumerate() {
                if *is_needed && indeg[i] > 0 {
                    remaining.push(dag.nodes[i].key.clone());
                }
            }

            return Err(ExecError::Cycle { remaining });
        }

        // Wait for one finished task
        let task_res = res_rx
            .recv()
            .map_err(|_| ExecError::InternalInvariant("result channel disconnected"))?;
        in_flight -= 1;

        // Record and propagate
        let id = task_res.id;

        let out = task_res.out.map_err(|e| ExecError::TaskFailed {
            task: dag.nodes[id.0].key.clone(),
            error: e,
        })?;

        debug_assert!(vals[id.0].is_none());
        vals[id.0] = Some(Arc::new(out));
        processed += 1;

        // Release dependents (same as Source case)
        for &dst in dependents[id.0].iter() {
            debug_assert!(indeg[dst.0] > 0);
            indeg[dst.0] -= 1;
            if indeg[dst.0] == 0 {
                ready.push_back(dst);
            }
        }
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

/// Dispatch a task to the next worker (round-robin).
fn dispatch_task<O, E>(
    task_txs: &[mpsc::Sender<Task<O, E>>],
    next_worker: &mut usize,
    task: Task<O, E>,
) -> Result<(), mpsc::SendError<Task<O, E>>> {
    let idx = *next_worker % task_txs.len();
    *next_worker = idx + 1;
    task_txs[idx].send(task)
}
