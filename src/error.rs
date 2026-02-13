use core::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BuildError<K> {
    DuplicateKey(K),
    MissingKey(K),
    MissingDependency { task: K, missing: K },
    EmptyGraph,
}

impl<K: fmt::Debug> fmt::Display for BuildError<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BuildError::DuplicateKey(k) => write!(f, "duplicate task key: {:?}", k),
            BuildError::MissingKey(k) => write!(f, "missing task key: {:?}", k),
            BuildError::MissingDependency { task, missing } => {
                write!(f, "task {:?} depends on missing key {:?}", task, missing)
            }
            BuildError::EmptyGraph => write!(f, "empty graph"),
        }
    }
}

impl<K: fmt::Debug> std::error::Error for BuildError<K> {}

#[derive(Debug)]
pub enum ExecError<K, E> {
    Build(BuildError<K>),
    Cycle { remaining: Vec<K> },
    TaskFailed { task: K, error: E },
    OutputMissing(K),
    InternalInvariant(&'static str),
}

impl<K, E> From<BuildError<K>> for ExecError<K, E> {
    fn from(e: BuildError<K>) -> Self {
        ExecError::Build(e)
    }
}

impl<K: fmt::Debug, E: fmt::Debug> fmt::Display for ExecError<K, E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExecError::Build(e) => write!(f, "build error: {e}"),
            ExecError::Cycle { remaining } => {
                write!(f, "cycle detected; remaining: {:?}", remaining)
            }
            ExecError::TaskFailed { task, error } => {
                write!(f, "task {:?} failed: {:?}", task, error)
            }
            ExecError::OutputMissing(k) => write!(f, "requested output missing: {:?}", k),
            ExecError::InternalInvariant(msg) => write!(f, "internal invariant violated: {msg}"),
        }
    }
}

impl<K: fmt::Debug, E: fmt::Debug> std::error::Error for ExecError<K, E> {}
