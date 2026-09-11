//! Faults are injected around the actual cache operation, including lost replies
//! after a write committed, so admission tests exercise retry deduplication.
use super::{BoxFuture, Error};
use std::{collections::HashMap, sync::Mutex, time::Duration};

struct Failure {
    operation: &'static str,
    prefix: String,
    after: bool,
    remaining: usize,
}

#[derive(Default)]
pub(crate) struct Faults {
    failure: Mutex<Option<Failure>>,
    ttls: Mutex<HashMap<String, Option<Duration>>>,
    pause: Mutex<Option<Pause>>,
}

struct Pause {
    operation: &'static str,
    prefix: String,
    entered: std::sync::Arc<tokio::sync::Notify>,
    resume: std::sync::Arc<tokio::sync::Notify>,
}

impl Faults {
    pub(crate) fn fail_once(&self, operation: &'static str, prefix: &str, after: bool) {
        self.fail_times(operation, prefix, after, 1);
    }

    pub(crate) fn fail_times(
        &self,
        operation: &'static str,
        prefix: &str,
        after: bool,
        remaining: usize,
    ) {
        *self.failure.lock().unwrap() = Some(Failure {
            operation,
            prefix: prefix.into(),
            after,
            remaining,
        });
    }

    pub(crate) fn pause_after(
        &self,
        operation: &'static str,
        prefix: &str,
    ) -> (
        std::sync::Arc<tokio::sync::Notify>,
        std::sync::Arc<tokio::sync::Notify>,
    ) {
        let entered = std::sync::Arc::new(tokio::sync::Notify::new());
        let resume = std::sync::Arc::new(tokio::sync::Notify::new());
        *self.pause.lock().unwrap() = Some(Pause {
            operation,
            prefix: prefix.into(),
            entered: entered.clone(),
            resume: resume.clone(),
        });
        (entered, resume)
    }

    pub(crate) fn ttl(&self, key: &str) -> Option<Option<Duration>> {
        self.ttls.lock().unwrap().get(key).copied()
    }

    fn check(&self, operation: &str, key: &str, after: bool) -> Result<(), Error> {
        let mut failure = self.failure.lock().unwrap();
        if failure.as_ref().is_some_and(|failure| {
            failure.operation == operation
                && key.starts_with(&failure.prefix)
                && failure.after == after
        }) {
            let pending = failure.as_mut().unwrap();
            pending.remaining -= 1;
            if pending.remaining == 0 {
                failure.take();
            }
            return Err(gproxy_core::error::StoreError(
                "injected cache failure".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn run<'a, T: Send + 'a>(
        &'a self,
        operation: &'static str,
        key: &'a str,
        ttl: Option<Option<Duration>>,
        execute: impl FnOnce() -> BoxFuture<'a, Result<T, Error>> + Send + 'a,
    ) -> BoxFuture<'a, Result<T, Error>> {
        Box::pin(async move {
            self.check(operation, key, false)?;
            let result = execute().await?;
            if let Some(ttl) = ttl {
                self.ttls.lock().unwrap().insert(key.into(), ttl);
            }
            let pause = {
                let mut pause = self.pause.lock().unwrap();
                if pause.as_ref().is_some_and(|pause| {
                    pause.operation == operation && key.starts_with(&pause.prefix)
                }) {
                    pause.take()
                } else {
                    None
                }
            };
            if let Some(pause) = pause {
                pause.entered.notify_one();
                pause.resume.notified().await;
            }
            self.check(operation, key, true)?;
            Ok(result)
        })
    }
}
