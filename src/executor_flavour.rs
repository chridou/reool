use std::error::Error;

use futures::future::Future;
use tokio::executor::{DefaultExecutor, Executor};
use tokio::runtime::TaskExecutor;

/// Compatibility for different executors
#[derive(Clone)]
pub enum ExecutorFlavour {
    Runtime,
    TokioTaskExecutor(TaskExecutor),
}

impl ExecutorFlavour {
    pub fn spawn<F>(&self, task: F) -> Result<(), Box<dyn Error>>
    where
        F: Future<Item = (), Error = ()> + Send + 'static,
    {
        match self {
            ExecutorFlavour::Runtime => DefaultExecutor::current()
                .spawn(Box::new(task))
                .map_err(|err| Box::new(err) as Box<dyn Error>),
            ExecutorFlavour::TokioTaskExecutor(executor) => {
                executor.spawn(Box::new(task));
                Ok(())
            }
        }
    }
}

impl From<TaskExecutor> for ExecutorFlavour {
    fn from(exec: TaskExecutor) -> Self {
        ExecutorFlavour::TokioTaskExecutor(exec)
    }
}

impl From<()> for ExecutorFlavour {
    fn from(_: ()) -> Self {
        ExecutorFlavour::Runtime
    }
}
