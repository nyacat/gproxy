use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use gproxy_channel_api::BoxFuture;
use gproxy_core::Spawner;
use tokio::sync::{Notify, oneshot, watch};

pub(crate) struct TokioSpawner {
    settlements: Arc<crate::ConcurrencyLimit>,
    executions: Arc<crate::ConcurrencyLimit>,
    #[cfg(test)]
    pub(crate) settlement_waiting: Notify,
    tasks: Arc<TaskGroup>,
    maintenance: Arc<TaskGroup>,
    draining: watch::Sender<bool>,
}

// Streams reserve their slot while open, so the backlog also covers active
// requests. Each pending settlement may retain request and response buffers.
const SETTLEMENT_BACKLOG: usize = 2048;

impl TokioSpawner {
    pub(crate) fn new(max_in_flight: usize) -> Self {
        Self {
            settlements: crate::ConcurrencyLimit::new(
                max_in_flight.saturating_add(SETTLEMENT_BACKLOG),
            ),
            executions: crate::ConcurrencyLimit::new(max_in_flight),
            #[cfg(test)]
            settlement_waiting: Notify::new(),
            tasks: Arc::default(),
            maintenance: Arc::default(),
            draining: watch::channel(false).0,
        }
    }

    pub(crate) fn set_max_in_flight(&self, limit: usize) {
        self.executions.set_limit(limit);
        self.settlements
            .set_limit(limit.saturating_add(SETTLEMENT_BACKLOG));
    }

    pub(crate) async fn reserve_execution(&self) -> crate::ConcurrencyPermit {
        self.executions.acquire().await
    }

    #[cfg(test)]
    pub(crate) fn settlement_limit(&self) -> &Arc<crate::ConcurrencyLimit> {
        &self.settlements
    }

    pub(crate) fn spawn_execution<F>(
        &self,
        task: F,
        permit: crate::ConcurrencyPermit,
    ) -> oneshot::Receiver<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.tasks.spawn_guarded(task, permit)
    }

    // Dropping the receiver detaches the operation. Keep the active guard until
    // an undelivered result has been dropped: its destructor may spawn refunds.
    pub(crate) fn spawn_tracked<F>(&self, task: F) -> oneshot::Receiver<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.tasks.spawn(task)
    }

    pub(crate) fn spawn_maintenance<F>(&self, mut shutdown: watch::Receiver<bool>, task: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.maintenance.spawn_detached(async move {
            let stopping = async {
                while !*shutdown.borrow_and_update() && shutdown.changed().await.is_ok() {}
            };
            tokio::select! {
                biased;
                () = stopping => {}
                () = task => {}
            }
        });
    }

    pub(crate) async fn drain(&self) {
        // Maintenance may enqueue a final finite write as it exits. Wait for
        // producers first; finite tasks may themselves enqueue child tasks.
        self.maintenance.wait().await;
        // The host calls drain only after requests finish. Expiring live
        // continuations at the initial shutdown signal could race those requests.
        self.draining.send_replace(true);
        self.tasks.wait().await;
    }
}

impl Spawner for TokioSpawner {
    fn spawn(&self, task: std::pin::Pin<Box<dyn Future<Output = ()> + Send>>) {
        self.tasks.spawn_detached(task);
    }

    fn spawn_delayed(&self, delay: BoxFuture<'static, ()>, task: BoxFuture<'static, ()>) {
        let mut shutdown = self.draining.subscribe();
        self.tasks.spawn_detached(async move {
            let stopping = async {
                while !*shutdown.borrow_and_update() && shutdown.changed().await.is_ok() {}
            };
            tokio::select! {
                biased;
                () = stopping => {}
                () = delay => {}
            }
            task.await;
        });
    }

    fn reserve_settlement(&self) -> BoxFuture<'_, gproxy_core::SettlementPermit> {
        Box::pin(async move {
            #[cfg(test)]
            self.settlement_waiting.notify_one();
            let permit = self.settlements.acquire().await;
            Box::new(permit) as gproxy_core::SettlementPermit
        })
    }
}

#[derive(Default)]
struct TaskGroup {
    active: AtomicUsize,
    changed: Notify,
}

impl TaskGroup {
    fn spawn_detached<F>(self: &Arc<Self>, task: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.active.fetch_add(1, Ordering::AcqRel);
        let active = ActiveTask(self.clone());
        tokio::spawn(async move {
            let _active = active;
            task.await;
        });
    }

    fn spawn<F>(self: &Arc<Self>, task: F) -> oneshot::Receiver<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.spawn_guarded(task, ())
    }

    fn spawn_guarded<F, G>(self: &Arc<Self>, task: F, guard: G) -> oneshot::Receiver<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
        G: Send + 'static,
    {
        self.active.fetch_add(1, Ordering::AcqRel);
        let active = ActiveTask(self.clone());
        let (sender, receiver) = oneshot::channel();
        tokio::spawn(async move {
            let _active = active;
            let _guard = guard;
            drop(sender.send(task.await));
        });
        receiver
    }

    async fn wait(&self) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.active.load(Ordering::Acquire) == 0 {
                return;
            }
            changed.await;
        }
    }
}

struct ActiveTask(Arc<TaskGroup>);

impl Drop for ActiveTask {
    fn drop(&mut self) {
        if self.0.active.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.changed.notify_waiters();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn detached_result_cleanup_is_drained_with_its_child_task() {
        struct Refund {
            spawner: Arc<TokioSpawner>,
            finished: Option<oneshot::Receiver<()>>,
        }
        impl Drop for Refund {
            fn drop(&mut self) {
                let finished = self.finished.take().unwrap();
                drop(self.spawner.spawn_tracked(async move {
                    finished.await.unwrap();
                }));
            }
        }

        let spawner = Arc::new(TokioSpawner::new(1));
        let task_spawner = spawner.clone();
        let (admit, admitted) = oneshot::channel();
        let (refund, refunded) = oneshot::channel();
        drop(spawner.spawn_tracked(async move {
            admitted.await.unwrap();
            Refund {
                spawner: task_spawner,
                finished: Some(refunded),
            }
        }));
        let drain = spawner.drain();
        tokio::pin!(drain);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut drain)
                .await
                .is_err()
        );
        admit.send(()).unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut drain)
                .await
                .is_err()
        );
        refund.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), drain)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn shutdown_stops_maintenance_but_waits_for_finite_work() {
        let (shutdown, stopping) = watch::channel(false);
        let spawner = TokioSpawner::new(1);
        let retained = Arc::new(());
        let weak = Arc::downgrade(&retained);
        spawner.spawn_maintenance(stopping, async move {
            let _retained = retained;
            std::future::pending::<()>().await;
        });
        let (finish, finished) = oneshot::channel();
        drop(spawner.spawn_tracked(async move {
            finished.await.unwrap();
        }));
        shutdown.send_replace(true);
        let drain = spawner.drain();
        tokio::pin!(drain);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut drain)
                .await
                .is_err()
        );
        assert!(
            weak.upgrade().is_none(),
            "maintenance releases owned resources"
        );
        finish.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), drain)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn failed_task_releases_tracking() {
        let spawner = TokioSpawner::new(1);
        let result = spawner.spawn_tracked(async { panic!("task failure") });
        assert!(result.await.is_err());
        tokio::time::timeout(Duration::from_secs(1), spawner.drain())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn shutdown_runs_scheduled_cleanup_without_waiting_for_expiry() {
        let spawner = TokioSpawner::new(1);
        let (completed, mut completion) = oneshot::channel();
        spawner.spawn_delayed(
            Box::pin(std::future::pending()),
            Box::pin(async move {
                completed.send(()).unwrap();
            }),
        );
        tokio::task::yield_now().await;
        assert!(matches!(
            completion.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        tokio::time::timeout(Duration::from_secs(1), spawner.drain())
            .await
            .unwrap();
        completion.await.unwrap();
    }
}
