use std::future::Future;
use std::time::Duration;

use gproxy_core::{CoreError, Host};

use super::AppHost;

const ATTEMPTS: usize = 8;

/// A failed settlement retains its reservation and failure marker for replay.
/// Bound both contention retries and native I/O waits so a broken backend
/// cannot retain a settlement permit and the request's buffers indefinitely.
pub(super) async fn run<F, Fut>(host: &AppHost, mut operation: F) -> Result<(), CoreError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<(), CoreError>>,
{
    let retry = async {
        let mut delay = Duration::from_millis(25);
        for attempt in 1..=ATTEMPTS {
            match operation().await {
                Ok(()) => return Ok(()),
                Err(error) if attempt == ATTEMPTS || !matches!(error, CoreError::Store(_)) => {
                    return Err(error);
                }
                Err(_) => {
                    host.wait(delay).await;
                    delay *= 2;
                }
            }
        }
        unreachable!("last attempt returns")
    };
    deadline(retry).await
}

pub(super) async fn deadline<T>(
    operation: impl Future<Output = Result<T, CoreError>>,
) -> Result<T, CoreError> {
    #[cfg(not(target_arch = "wasm32"))]
    {
        tokio::time::timeout(Duration::from_secs(5), operation)
            .await
            .map_err(|_| {
                CoreError::Store(gproxy_core::error::StoreError(
                    "quota storage deadline exceeded".into(),
                ))
            })?
    }
    #[cfg(target_arch = "wasm32")]
    operation.await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn permanent_errors_stop_after_one_attempt() {
        let fixture = crate::tests::setup::fixture().await;
        let attempts = std::sync::atomic::AtomicUsize::new(0);
        let result = run(&fixture.app.inner.host, || async {
            attempts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Err(CoreError::Internal("invalid admission state".into()))
        })
        .await;
        assert!(result.is_err());
        assert_eq!(attempts.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn a_stalled_backend_cannot_retain_a_settlement_forever() {
        let fixture = crate::tests::setup::fixture().await;
        let result = tokio::time::timeout(
            Duration::from_secs(6),
            run(
                &fixture.app.inner.host,
                std::future::pending::<Result<(), CoreError>>,
            ),
        )
        .await
        .expect("settlement deadline");
        assert!(matches!(result, Err(CoreError::Store(_))));
    }
}
