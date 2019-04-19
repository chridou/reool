use futures::{future::Future, stream::Stream, sync::mpsc};
use log::warn;
use tokio::executor::{DefaultExecutor, Executor};
use tokio::runtime::current_thread::Handle;
use tokio::runtime::TaskExecutor;

use crate::error::*;

#[derive(Clone)]
pub enum ExecutorFlavour {
    Runtime,
    TokioTaskExecutor(TaskExecutor),
    TokioHandle(Handle),
}

impl ExecutorFlavour {
    pub fn execute<F>(&self, task: F) -> Result<()>
    where
        F: Future<Item = (), Error = ()> + Send + 'static,
    {
        match self {
            ExecutorFlavour::Runtime => {
                DefaultExecutor::current()
                    .spawn(Box::new(task))
                    .map_err(|err| {
                        warn!("default executor failed to execute a task: {:?}", err);
                        Error::with_cause(ErrorKind::TaskExecution, err)
                    })
            }
            ExecutorFlavour::TokioTaskExecutor(executor) => {
                executor.spawn(Box::new(task));
                Ok(())
            }
            ExecutorFlavour::TokioHandle(executor) => {
                executor.spawn(Box::new(task)).map_err(|err| {
                    warn!("default executor failed to execute a task: {:?}", err);
                    Error::with_cause(ErrorKind::TaskExecution, err)
                })
            }
        }
    }

    pub fn spawn_unbounded<S>(&self, stream: S) -> mpsc::SpawnHandle<S::Item, S::Error>
    where
        S: Stream + Send + 'static,
        S::Item: Send,
        S::Error: Send,
    {
        match self {
            ExecutorFlavour::Runtime => mpsc::spawn_unbounded(stream, &DefaultExecutor::current()),
            ExecutorFlavour::TokioTaskExecutor(executor) => mpsc::spawn_unbounded(stream, executor),
            ExecutorFlavour::TokioHandle(executor) => mpsc::spawn_unbounded(stream, executor),
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

impl From<Handle> for ExecutorFlavour {
    fn from(handle: Handle) -> Self {
        ExecutorFlavour::TokioHandle(handle)
    }
}
