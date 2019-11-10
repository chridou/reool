use futures::prelude::*;
use futures::channel::mpsc;
use log::warn;
use tokio::executor::{DefaultExecutor, Executor};
use tokio::runtime::TaskExecutor;

use crate::error::*;

#[derive(Clone)]
pub enum ExecutorFlavour {
    Runtime,
    TokioTaskExecutor(TaskExecutor),
}

impl ExecutorFlavour {
    pub fn execute<F>(&self, task: F) -> CheckoutResult<()>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        match self {
            ExecutorFlavour::Runtime => {
                DefaultExecutor::current()
                    .spawn(Box::pin(task))
                    .map_err(|err| {
                        warn!("default executor failed to execute a task: {:?}", err);
                        CheckoutError::with_cause(CheckoutErrorKind::TaskExecution, err)
                    })
            }
            ExecutorFlavour::TokioTaskExecutor(executor) => {
                executor.spawn(Box::pin(task));
                Ok(())
            }
        }
    }

    pub fn spawn_unbounded<S>(&self, stream: S) -> mpsc::UnboundedReceiver<S::Item>
    where
        S: Stream + Send + 'static,
        S::Item: Send,
    {
        let (tx, rx) = mpsc::unbounded::<S::Item>();

        let forward_task = stream
            .map(|item| Ok(item))
            .forward(tx)
            .map(|_| ());

        let _ = self.execute(forward_task);

        rx
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
