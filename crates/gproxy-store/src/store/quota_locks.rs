use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};

use futures_util::lock::{Mutex as AsyncMutex, MutexGuard};

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;

type WindowIndex = Mutex<HashMap<i64, Weak<AsyncMutex<()>>>>;

/// Serializes each window within a Store and its clones. Database CAS remains
/// necessary when another Store or process updates the same durable window.
#[derive(Clone, Default)]
pub(crate) struct QuotaWindowLocks {
    windows: Arc<WindowIndex>,
}

impl QuotaWindowLocks {
    pub(super) fn window(&self, window_id: i64) -> WindowLock {
        let mut windows = self
            .windows
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let lock = windows
            .get(&window_id)
            .and_then(Weak::upgrade)
            .unwrap_or_else(|| {
                let lock = Arc::new(AsyncMutex::new(()));
                windows.insert(window_id, Arc::downgrade(&lock));
                lock
            });
        WindowLock {
            window_id,
            lock: Some(lock),
            windows: self.windows.clone(),
        }
    }
}

pub(super) struct WindowLock {
    window_id: i64,
    lock: Option<Arc<AsyncMutex<()>>>,
    windows: Arc<WindowIndex>,
}

impl WindowLock {
    pub(super) async fn lock(&self) -> MutexGuard<'_, ()> {
        self.lock.as_ref().expect("live window lease").lock().await
    }
}

impl Drop for WindowLock {
    fn drop(&mut self) {
        let mut windows = self
            .windows
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        // Release and inspect under the index lock, so simultaneous cancelled
        // waiters cannot each leave the final weak entry behind.
        drop(self.lock.take());
        if windows
            .get(&self.window_id)
            .is_some_and(|lock| lock.strong_count() == 0)
        {
            windows.remove(&self.window_id);
        }
    }
}
