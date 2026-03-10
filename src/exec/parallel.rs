use std::{
    collections::{HashMap, VecDeque},
    hash::Hash,
    sync::{Arc, mpsc},
    thread,
};

use super::{
    common::{ExecArtifacts, build_kahn_metadata, collect_outputs, mark_needed},
    observe::{ExecObserver, NoopObserver},
};
use crate::{
    error::ExecError,
    graph::{Dag, ExecutorConfig, NodeId, NodeKind, TaskFn},
};

#[cfg(feature = "execution-trace")]
use crate::trace::{TraceObserver, TracedExecution};

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
    task_txs: Option<Vec<mpsc::SyncSender<Task<O, E>>>>,
    workers: Vec<thread::JoinHandle<()>>,
}

enum DispatchOutcome<O, E> {
    Queued { worker_id: usize },
    AllFull(Task<O, E>),
}

impl<O, E> WorkerPool<O, E>
where
    O: Send + Sync + 'static,
    E: Send + 'static,
{
    /// Spawn N workers with N independent task queues (each queue is mpsc: scheduler -> worker).
    /// Returns (task_senders, join_handles).
    fn spawn(
        res_tx: mpsc::Sender<TaskResult<O, E>>,
        n_workers: usize,
        per_worker_cap: usize,
    ) -> Self {
        let mut task_txs = Vec::with_capacity(n_workers);
        let mut workers = Vec::with_capacity(n_workers);

        for _ in 0..n_workers {
            let (task_tx, task_rx) = mpsc::sync_channel::<Task<O, E>>(per_worker_cap);
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

    fn senders(&self) -> Option<&[mpsc::SyncSender<Task<O, E>>]> {
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

/// Dispatch a task to the next worker (round-robin).
fn dispatch_task_try<O, E>(
    task_txs: &[mpsc::SyncSender<Task<O, E>>],
    next_worker: &mut usize,
    mut task: Task<O, E>,
) -> Result<DispatchOutcome<O, E>, ()> {
    let n = task_txs.len();
    debug_assert!(n > 0);

    for _ in 0..n {
        let idx = *next_worker % n;
        *next_worker = idx + 1;

        match task_txs[idx].try_send(task) {
            Ok(()) => return Ok(DispatchOutcome::Queued { worker_id: idx }),
            Err(mpsc::TrySendError::Full(t)) => {
                // keep ownership and try next worker
                task = t;
            }
            Err(_e) => return Err(()), // Disconnected
        }
    }

    // All queues full: signal caller to stop dispatching and recv a result.
    Ok(DispatchOutcome::AllFull(task))
}

fn release_dependents<R: ExecObserver>(
    dependents: &[Vec<NodeId>],
    indeg: &mut [usize],
    ready: &mut VecDeque<NodeId>,
    from: NodeId,
    observer: &mut R,
) {
    for &dst in &dependents[from.0] {
        debug_assert!(indeg[dst.0] > 0);
        indeg[dst.0] -= 1;
        if indeg[dst.0] == 0 {
            ready.push_back(dst);
            observer.mark_ready(dst, ready.len());
        }
    }
}

fn run_with_observer<K, O, E, R>(
    dag: &Dag<K, O, E>,
    cfg: &ExecutorConfig,
    out_keys: &[K],
    observer: &mut R,
) -> Result<ExecArtifacts<O>, ExecError<K, E>>
where
    K: Eq + Hash + Clone,
    O: Send + Sync + 'static,
    E: Send + 'static,
    R: ExecObserver,
{
    if out_keys.is_empty() {
        return Err(ExecError::InternalInvariant("outputs must be non-empty"));
    }
    if cfg.max_workers == 0 {
        return Err(ExecError::InternalInvariant("max_workers must be > 0"));
    }
    if cfg.max_in_flight == 0 {
        return Err(ExecError::InternalInvariant("max_in_flight must be > 0"));
    }
    if cfg.worker_queue_cap == 0 {
        return Err(ExecError::InternalInvariant("worker_queue_cap must be > 0"));
    }

    let needed = mark_needed(dag, out_keys)?;
    let (dependents, mut indeg, needed_count) = build_kahn_metadata(dag, &needed);

    // Values indexed by NodeId
    let mut vals: Vec<Option<Arc<O>>> = vec![None; dag.nodes.len()];

    // Seed: nodes with indeg=0 in the needed subgraph
    let mut ready: VecDeque<NodeId> = VecDeque::new();
    for (i, is_needed) in needed.iter().enumerate() {
        if *is_needed && indeg[i] == 0 {
            let id = NodeId(i);
            ready.push_back(id);
            observer.mark_ready(id, ready.len());
        }
    }

    // Worker pool wiring
    let (res_tx, res_rx) = mpsc::channel::<TaskResult<O, E>>();
    let pool = WorkerPool::spawn(res_tx, cfg.max_workers, cfg.worker_queue_cap);

    let senders = pool
        .senders()
        .ok_or_else(|| ExecError::InternalInvariant("worker pool is closed"))?;
    let n_workers = senders.len();

    let physical_max = n_workers.saturating_mul(cfg.worker_queue_cap.saturating_add(1));
    let effective_cap = cfg.max_in_flight.min(physical_max);

    let mut processed = 0usize;
    let mut in_flight = 0usize;
    let mut next_worker = 0usize;

    // If we hit AllFull, we keep the exact task here and retry after receiving a result.
    let mut pending: Option<Task<O, E>> = None;

    while processed < needed_count {
        // Dispatch all currently-ready nodes.
        while in_flight < effective_cap {
            // Retry pending first (keeps fairness / avoids rebuilding args).
            if let Some(task) = pending.take() {
                let task_id = task.id;

                match dispatch_task_try(senders, &mut next_worker, task)
                    .map_err(|_| ExecError::InternalInvariant("worker task channel disconnected"))?
                {
                    DispatchOutcome::Queued { worker_id } => {
                        observer.mark_start(task_id, Some(worker_id));
                        in_flight += 1;
                        continue;
                    }
                    DispatchOutcome::AllFull(task) => {
                        pending = Some(task);
                        break;
                    }
                }
            }

            let Some(id) = ready.pop_front() else {
                break;
            };

            match &dag.nodes[id.0].kind {
                NodeKind::Source(v) => {
                    observer.mark_start(id, None);
                    vals[id.0] = Some(Arc::clone(v));
                    processed += 1;
                    observer.mark_finish(id);

                    release_dependents(&dependents, &mut indeg, &mut ready, id, observer);
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

                    let task = Task {
                        id,
                        func: Arc::clone(f),
                        args,
                    };

                    match dispatch_task_try(senders, &mut next_worker, task).map_err(|_| {
                        ExecError::InternalInvariant("worker task channel disconnected")
                    })? {
                        DispatchOutcome::Queued { worker_id } => {
                            observer.mark_start(id, Some(worker_id));
                            in_flight += 1;
                        }
                        DispatchOutcome::AllFull(task) => {
                            pending = Some(task);
                            break;
                        }
                    }
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

        let id = task_res.id;
        observer.mark_finish(id);

        // Record and propagate
        let out = task_res.out.map_err(|e| ExecError::TaskFailed {
            task: dag.nodes[id.0].key.clone(),
            error: e,
        })?;

        debug_assert!(vals[id.0].is_none());
        vals[id.0] = Some(Arc::new(out));
        processed += 1;

        release_dependents(&dependents, &mut indeg, &mut ready, id, observer);
    }

    Ok((vals, needed))
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
    let mut observer = NoopObserver::new();
    let (vals, _) = run_with_observer(dag, cfg, &out_keys, &mut observer)?;
    collect_outputs(dag, &out_keys, vals)
}

#[cfg(feature = "execution-trace")]
pub(crate) fn run_traced<K, O, E>(
    dag: &Dag<K, O, E>,
    cfg: &ExecutorConfig,
    out_keys: Vec<K>,
) -> Result<TracedExecution<K, O>, ExecError<K, E>>
where
    K: Eq + Hash + Clone,
    O: Send + Sync + 'static,
    E: Send + 'static,
{
    let mut observer = TraceObserver::new(dag.nodes.len());
    let (vals, needed) = run_with_observer(dag, cfg, &out_keys, &mut observer)?;
    let outputs = collect_outputs(dag, &out_keys, vals)?;
    let trace = observer.finalize(dag, &needed);

    Ok(TracedExecution { outputs, trace })
}
