use std::time::Duration;

use gproxy_core::error::StoreError as CoreStoreError;
use gproxy_core::{BoxFuture, CacheBackend as _, CredentialId, CredentialRecord, CredentialStore};

use super::AppHost;

impl CredentialStore for AppHost {
    fn load<'a>(
        &'a self,
        id: CredentialId,
    ) -> BoxFuture<'a, Result<CredentialRecord, CoreStoreError>> {
        Box::pin(load(self, id, false))
    }

    fn load_current<'a>(
        &'a self,
        id: CredentialId,
    ) -> BoxFuture<'a, Result<CredentialRecord, CoreStoreError>> {
        Box::pin(load(self, id, true))
    }

    fn persist_rotation<'a>(
        &'a self,
        id: CredentialId,
        secret: serde_json::Value,
        version: u64,
    ) -> BoxFuture<'a, Result<(), CoreStoreError>> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let host = self.clone();
            Box::pin(async move {
                let task_host = host.clone();
                host.services
                    .spawner
                    .spawn_tracked(async move {
                        persist_rotation(&task_host, id, secret, version).await
                    })
                    .await
                    .map_err(|error| {
                        CoreStoreError(format!("credential rotation task failed: {error}"))
                    })?
            })
        }
        #[cfg(target_arch = "wasm32")]
        Box::pin(persist_rotation(self, id, secret, version))
    }

    fn lease_refresh<'a>(
        &'a self,
        id: CredentialId,
        owner: &'a [u8],
        ttl: Duration,
    ) -> BoxFuture<'a, Result<bool, CoreStoreError>> {
        Box::pin(async move {
            self.services
                .cache
                .compare_and_swap(&refresh_key(id), None, Some(owner.to_vec()), Some(ttl))
                .await
                .map_err(cache_error)
        })
    }

    fn renew_refresh<'a>(
        &'a self,
        id: CredentialId,
        owner: &'a [u8],
        ttl: Duration,
    ) -> BoxFuture<'a, Result<bool, CoreStoreError>> {
        Box::pin(async move {
            self.services
                .cache
                .compare_and_swap(
                    &refresh_key(id),
                    Some(owner.to_vec()),
                    Some(owner.to_vec()),
                    Some(ttl),
                )
                .await
                .map_err(cache_error)
        })
    }

    fn release_refresh<'a>(
        &'a self,
        id: CredentialId,
        owner: &'a [u8],
    ) -> BoxFuture<'a, Result<(), CoreStoreError>> {
        Box::pin(async move {
            self.services
                .cache
                .compare_and_swap(&refresh_key(id), Some(owner.to_vec()), None, None)
                .await
                .map(|_| ())
                .map_err(cache_error)
        })
    }
}

async fn load(
    host: &AppHost,
    id: CredentialId,
    authoritative: bool,
) -> Result<CredentialRecord, CoreStoreError> {
    for _ in 0..8 {
        let (cached, epoch) = host.services.control.credential_for_load(id.0);
        if !authoritative && let Some(record) = cached {
            return Ok(record);
        }
        let stored = host
            .services
            .store
            .credential(id.0)
            .await
            .map_err(store_error)?
            .filter(|credential| credential.enabled);
        let Some(stored) = stored else {
            // A current read can discover revocation before the background
            // invalidation poll. Future ordinary loads must not reuse the
            // credential this authoritative read has already rejected.
            host.services.control.forget_credential(id.0);
            return Err(unavailable());
        };
        #[cfg(all(test, not(target_arch = "wasm32")))]
        host.services.control.pause_credential_read().await;
        let secret = host
            .services
            .cipher
            .open(&stored.envelope)
            .map_err(|_| encryption_error())?;
        let record = CredentialRecord {
            id,
            channel: gproxy_channels::canonical_channel_id(&stored.channel).into(),
            kind: stored.kind,
            secret,
            version: stored.version,
        };
        if host
            .services
            .control
            .cache_credential_if_current(&record, epoch)
        {
            return Ok(record);
        }
        // A concurrent invalidation revoked this read. Returning it would
        // expose stale authentication even if it was kept out of the cache.
    }
    Err(CoreStoreError(
        "credential changed repeatedly while loading".into(),
    ))
}

async fn persist_rotation(
    host: &AppHost,
    id: CredentialId,
    secret: serde_json::Value,
    version: u64,
) -> Result<(), CoreStoreError> {
    host.services.control.forget_credential(id.0);
    let result = async {
        let stored = host
            .services
            .store
            .credential(id.0)
            .await
            .map_err(store_error)?
            .ok_or_else(unavailable)?;
        if stored.version != version {
            return Err(CoreStoreError("credential version conflict".into()));
        }
        let current = host
            .services
            .cipher
            .open(&stored.envelope)
            .map_err(|_| encryption_error())?;
        let mut secret = secret;
        let object = secret.as_object_mut().ok_or_else(encryption_error)?;
        object.retain(|key, _| !key.starts_with("quota_"));
        if let Some(current) = current.as_object() {
            for (key, value) in current.iter().filter(|(key, _)| key.starts_with("quota_")) {
                object.insert(key.clone(), value.clone());
            }
        }
        let envelope = host
            .services
            .cipher
            .seal(&secret)
            .map_err(|_| encryption_error())?;
        #[cfg(all(test, not(target_arch = "wasm32")))]
        host.services.control.pause_credential_rotation().await;
        host.services
            .store
            .persist_credential_rotation(id.0, &envelope, version)
            .await
            .map_err(store_error)
    }
    .await;
    // The tracked native task owns invalidation through the write, even when
    // its caller is cancelled. It also revokes reads started during rotation.
    host.services.control.forget_credential(id.0);
    let invalidated = if result.is_ok() {
        crate::invalidation::bump(&host.services.cache)
            .await
            .map(|_| ())
            .map_err(|_| CoreStoreError("credential invalidation failed".into()))
    } else {
        Ok(())
    };
    result.and(invalidated)
}

fn refresh_key(id: CredentialId) -> String {
    format!("gproxy:refresh:{}", id.0)
}

fn unavailable() -> CoreStoreError {
    CoreStoreError("credential is unavailable".into())
}

fn store_error(error: gproxy_store::StoreError) -> CoreStoreError {
    let message = match error {
        gproxy_store::StoreError::VersionConflict => "credential version conflict",
        gproxy_store::StoreError::Database(_)
        | gproxy_store::StoreError::InvalidData { .. }
        | gproxy_store::StoreError::QuotaWindowMissing(_) => "credential persistence failed",
    };
    CoreStoreError(message.into())
}

fn encryption_error() -> CoreStoreError {
    CoreStoreError("credential encryption failed".into())
}

fn cache_error(_: CoreStoreError) -> CoreStoreError {
    CoreStoreError("credential refresh cache failed".into())
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    #[tokio::test]
    async fn stale_refresh_owners_cannot_renew_or_release_a_successor() {
        let fixture = crate::tests::setup::fixture().await;
        fixture.app.shutdown();
        fixture.app.drain_background().await;
        let host = &fixture.app.inner.host;
        let id = CredentialId(fixture.credential);
        let ttl = Duration::from_secs(120);
        assert!(host.lease_refresh(id, b"first", ttl).await.unwrap());
        assert!(!host.lease_refresh(id, b"second", ttl).await.unwrap());
        // Simulate expiry followed by a successful successor acquisition.
        host.services.cache.delete(&refresh_key(id)).await.unwrap();
        assert!(host.lease_refresh(id, b"second", ttl).await.unwrap());
        assert!(!host.renew_refresh(id, b"first", ttl).await.unwrap());
        host.release_refresh(id, b"first").await.unwrap();
        assert_eq!(
            host.services.cache.get(&refresh_key(id)).await.unwrap(),
            Some(b"second".to_vec())
        );
        assert!(host.renew_refresh(id, b"second", ttl).await.unwrap());
        host.release_refresh(id, b"second").await.unwrap();
        assert_eq!(
            host.services.cache.get(&refresh_key(id)).await.unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn persistence_preserves_quota_secrets_and_does_not_release_refresh_ownership() {
        let fixture = crate::tests::setup::fixture().await;
        fixture.app.shutdown();
        fixture.app.drain_background().await;
        let host = &fixture.app.inner.host;
        let id = CredentialId(fixture.credential);
        let current = host.load_current(id).await.unwrap();
        let original = serde_json::json!({"access_token":"before", "refresh_token":"saved", "quota_api_key":"quota"});
        let envelope = host.services.cipher.seal(&original).unwrap();
        host.services
            .store
            .persist_credential_rotation(id.0, &envelope, current.version)
            .await
            .unwrap();
        let current = host.load_current(id).await.unwrap();
        assert!(
            host.lease_refresh(id, b"current-owner", Duration::from_secs(120))
                .await
                .unwrap()
        );
        let replacement = serde_json::json!({"access_token":"after", "refresh_token":"rotated", "quota_cookie":"removed-during-refresh"});
        host.persist_rotation(id, replacement.clone(), current.version)
            .await
            .unwrap();
        let updated = host.load_current(id).await.unwrap();
        assert_eq!(updated.secret["refresh_token"], "rotated");
        assert_eq!(updated.secret["quota_api_key"], "quota");
        assert!(updated.secret.get("quota_cookie").is_none());
        assert_eq!(
            host.services.cache.get(&refresh_key(id)).await.unwrap(),
            Some(b"current-owner".to_vec())
        );
        assert!(
            host.persist_rotation(id, replacement, current.version)
                .await
                .is_err()
        );
        assert_eq!(
            host.services.cache.get(&refresh_key(id)).await.unwrap(),
            Some(b"current-owner".to_vec())
        );
        host.release_refresh(id, b"current-owner").await.unwrap();
    }
}
