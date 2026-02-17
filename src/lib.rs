#![forbid(unsafe_code)]

mod builder;
mod error;
mod exec;
mod graph;

pub use builder::DagBuilder;
pub use error::{BuildError, ExecError};
pub use exec::Executor;
pub use graph::{Dag, ExecutorConfig, NodeId};
