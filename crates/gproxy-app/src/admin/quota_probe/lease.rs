use crate::AppHandle;
use gproxy_admin::AdminError;
use gproxy_core::{CacheBackend, Host};
use std::time::Duration;

pub(super) async fn acquire(app: &AppHandle, owner: &[u8]) -> Result<Guard, AdminError> {
    for _ in 0..120 {
        for slot in 0..4 {
            if let Some(guard) = Guard::try_acquire(
                app,
                format!("quota:probe-slot:{slot}"),
                owner,
                Duration::from_secs(120),
            )
            .await?
            {
                return Ok(guard);
            }
        }
        app.inner.host.wait(Duration::from_millis(100)).await;
    }
    Err(AdminError::Conflict(
        "quota probe concurrency limit reached".into(),
    ))
}

async fn release_key(app: &AppHandle, key: &str, owner: &[u8]) -> Result<(), AdminError> {
    app.inner
        .host
        .services
        .cache
        .compare_and_swap(key, Some(owner.to_vec()), None, None)
        .await
        .map_err(super::internal)?;
    Ok(())
}

/// An acquired probe slot is an ownership lease, not a best-effort lock. If
/// the request is cancelled while the upstream future is pending, its Drop
/// path schedules an owner-token-checked release so another probe does not
/// wait for the TTL to expire. Native hosts track that cleanup through the
/// same background task group used by settlement writes.
pub(super) struct Guard {
    app: AppHandle,
    key: String,
    owner: Vec<u8>,
    released: bool,
}

impl Guard {
    pub(super) async fn try_acquire(
        app: &AppHandle,
        key: String,
        owner: &[u8],
        ttl: Duration,
    ) -> Result<Option<Self>, AdminError> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let acquiring = app.clone();
            let owner = owner.to_vec();
            app.spawn_background(
                async move { Self::acquire_owned(acquiring, key, owner, ttl).await },
            )
            .await
            .map_err(super::internal)?
        }
        #[cfg(target_arch = "wasm32")]
        Self::acquire_owned(app.clone(), key, owner.to_vec(), ttl).await
    }

    async fn acquire_owned(
        app: AppHandle,
        key: String,
        owner: Vec<u8>,
        ttl: Duration,
    ) -> Result<Option<Self>, AdminError> {
        // The native task owns this guard until the CAS returns. When its
        // caller disappears, the undelivered result is dropped only after the
        // write completes, so cleanup cannot precede a delayed acquisition.
        let mut guard = Self {
            app,
            key,
            owner,
            released: false,
        };
        if guard
            .app
            .inner
            .host
            .services
            .cache
            .compare_and_swap(&guard.key, None, Some(guard.owner.clone()), Some(ttl))
            .await
            .map_err(super::internal)?
        {
            Ok(Some(guard))
        } else {
            guard.released = true;
            Ok(None)
        }
    }

    pub(super) async fn release(mut self) -> Result<(), AdminError> {
        let result = release_key(&self.app, &self.key, &self.owner).await;
        self.released = result.is_ok();
        result
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for Guard {
    fn drop(&mut self) {
        if self.released {
            return;
        }
        let app = self.app.clone();
        let key = self.key.clone();
        let owner = self.owner.clone();
        drop(self.app.spawn_background(async move {
            match tokio::time::timeout(Duration::from_secs(5), release_key(&app, &key, &owner)).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => tracing::warn!(key, error = %error, "cancelled quota probe lease cleanup failed"),
                Err(_) => tracing::warn!(key, "cancelled quota probe lease cleanup timed out"),
            }
        }));
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cancelled_acquisition_finishes_its_cas_before_releasing_the_lease() {
        let fixture = crate::tests::setup::fixture().await;
        let app = &fixture.app;
        app.shutdown();
        app.drain_background().await;
        let cache = &app.inner.host.services.cache;
        let key = "quota:probe-slot:late-cas";
        let (before, commit) = cache.testing.pause_before("compare_swap", key);
        let acquiring = app.clone();
        let request = tokio::spawn(async move {
            Guard::try_acquire(&acquiring, key.into(), b"owner", Duration::from_secs(120)).await
        });
        tokio::time::timeout(Duration::from_secs(5), before.notified())
            .await
            .unwrap();
        request.abort();
        match request.await {
            Err(error) => assert!(error.is_cancelled()),
            Ok(_) => panic!("caller finished before the delayed CAS"),
        }
        let (after, reply) = cache.testing.pause_after("compare_swap", key);
        commit.notify_one();
        tokio::time::timeout(Duration::from_secs(5), after.notified())
            .await
            .unwrap();
        // The cancelled request's CAS really committed. Its guard must still
        // be held by the acquisition task until the backend returns the reply.
        assert_eq!(cache.get(key).await.unwrap(), Some(b"owner".to_vec()));
        let draining = app.drain_background();
        futures_util::pin_mut!(draining);
        assert!(futures_util::poll!(&mut draining).is_pending());
        reply.notify_one();
        tokio::time::timeout(Duration::from_secs(5), draining)
            .await
            .unwrap();
        assert!(cache.get(key).await.unwrap().is_none());
    }
}
