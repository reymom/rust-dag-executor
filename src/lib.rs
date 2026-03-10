#![forbid(unsafe_code)]

mod builder;
mod error;
mod exec;
mod graph;

#[cfg(feature = "execution-trace")]
mod trace;

pub use builder::DagBuilder;
pub use error::{BuildError, ExecError};
pub use exec::Executor;
pub use graph::{Dag, ExecutorConfig, NodeId};

#[cfg(feature = "execution-trace")]
pub use trace::{ExecutionTrace, NodeTrace, TraceNodeKind, TracedExecution};
