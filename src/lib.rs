#![forbid(unsafe_code)]

pub mod builder;
pub mod error;
pub mod exec;
pub mod graph;

pub use builder::DagBuilder;
pub use error::{BuildError, ExecError};
pub use graph::{Dag, ExecutorConfig, NodeId};
