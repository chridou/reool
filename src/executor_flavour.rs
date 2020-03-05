use std::error::Error;

use futures::future::Future;
use tokio::runtime::Handle;

/// Compatibility for different executors
#[derive(Clone)]
pub enum ExecutorFlavour {
    Runtime,
    TokioTaskExecutor(Handle),
}

impl ExecutorFlavour {
    pub fn spawn<F>(&self, task: F) -> Result<(), Box<dyn Error>>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        match self {
            ExecutorFlavour::Runtime => Handle::try_current()?.spawn(task),
            ExecutorFlavour::TokioTaskExecutor(handle) => handle.spawn(task),
        };

        Ok(())
    }
}

impl From<Handle> for ExecutorFlavour {
    fn from(handle: Handle) -> Self {
        ExecutorFlavour::TokioTaskExecutor(handle)
    }
}

impl From<&Handle> for ExecutorFlavour {
    fn from(handle: &Handle) -> Self {
        ExecutorFlavour::TokioTaskExecutor(handle.clone())
    }
}

impl From<()> for ExecutorFlavour {
    fn from(_: ()) -> Self {
        ExecutorFlavour::Runtime
    }
}
