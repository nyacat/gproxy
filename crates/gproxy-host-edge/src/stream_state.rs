use std::cell::Cell;

use bytes::Bytes;
use futures_util::StreamExt;
use futures_util::lock::Mutex;
use gproxy_channel_api::TransportError;
use gproxy_core::{ByteStream, StreamCancellation};

pub(crate) struct StreamState {
    stream: Mutex<Option<ByteStream>>,
    cancellation: Option<StreamCancellation>,
    cancelled: Cell<bool>,
}

impl StreamState {
    pub(crate) fn new(stream: ByteStream, cancellation: Option<StreamCancellation>) -> Self {
        Self {
            stream: Mutex::new(Some(stream)),
            cancellation,
            cancelled: Cell::new(false),
        }
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancelled.get()
    }

    pub(crate) async fn next(&self) -> Option<Result<Bytes, TransportError>> {
        let mut slot = self.stream.lock().await;
        let next = slot.as_mut()?.next().await;
        if next.is_none() {
            slot.take();
        }
        next
    }

    pub(crate) async fn cancel(&self) {
        self.cancelled.set(true);
        interrupt_and_finish(
            &self.stream,
            self.cancellation.as_ref().map(|handle| || handle.cancel()),
        )
        .await;
    }
}

async fn interrupt_and_finish(state: &Mutex<Option<ByteStream>>, interrupt: Option<impl FnOnce()>) {
    let settle_inline = interrupt.is_some();
    if let Some(interrupt) = interrupt {
        // Wake a pending pull before waiting for its lock. The core then
        // stops reading upstream and polls only its inline settlement.
        interrupt();
    }
    let mut slot = state.lock().await;
    if let Some(mut stream) = slot.take()
        && settle_inline
    {
        while stream.next().await.is_some() {}
    }
    // Keep the lock through settlement so concurrent cancellation callers
    // all wait for the same completion.
}

#[cfg(test)]
mod tests;
