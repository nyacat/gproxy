use std::pin::Pin;
use std::sync::Mutex;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures_core::Stream;
use gproxy_channel_api::TransportError;

use crate::{ByteStream, Shared, StreamCancellation};

struct State {
    stream: Option<ByteStream>,
    finishing: bool,
}

struct Body(Shared<Mutex<State>>);

/// Keeps an invoked inline stream alive if a synthesizer discards its body.
/// Only streams carrying the core's cancellation handle enter this wrapper.
pub(crate) struct InlineCompletion {
    state: Shared<Mutex<State>>,
    cancellation: StreamCancellation,
}

impl InlineCompletion {
    pub(crate) fn wrap(stream: ByteStream, cancellation: StreamCancellation) -> (ByteStream, Self) {
        let state = Shared::new(Mutex::new(State {
            stream: Some(stream),
            finishing: false,
        }));
        let body = Box::pin(Body(state.clone()));
        (
            body,
            Self {
                state,
                cancellation,
            },
        )
    }

    pub(crate) fn cancel(&self) {
        self.cancellation.cancel();
        self.state.lock().expect("inline stream lock").finishing = true;
    }

    pub(crate) fn poll_finish(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        let mut state = self.state.lock().expect("inline stream lock");
        loop {
            match poll_stream(&mut state, cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(_)) => {}
                Poll::Ready(None) => return Poll::Ready(()),
            }
        }
    }

    pub(crate) async fn finish_all(mut completions: Vec<Self>) {
        for completion in &completions {
            completion.cancel();
        }
        futures_util::future::poll_fn(move |cx| Self::poll_all(&mut completions, cx)).await;
    }

    pub(crate) fn poll_all(completions: &mut Vec<Self>, cx: &mut Context<'_>) -> Poll<()> {
        let mut index = 0;
        while index < completions.len() {
            if completions[index].poll_finish(cx).is_ready() {
                completions.swap_remove(index);
            } else {
                index += 1;
            }
        }
        // Poll every cancelled child so one slow settlement cannot keep
        // another child's upstream alive while it waits for storage.
        if completions.is_empty() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

impl Stream for Body {
    type Item = Result<Bytes, TransportError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut state = self.0.lock().expect("inline stream lock");
        if state.finishing {
            return Poll::Ready(None);
        }
        poll_stream(&mut state, cx)
    }
}

fn poll_stream(
    state: &mut State,
    cx: &mut Context<'_>,
) -> Poll<Option<Result<Bytes, TransportError>>> {
    let Some(stream) = state.stream.as_mut() else {
        return Poll::Ready(None);
    };
    let next = stream.as_mut().poll_next(cx);
    if matches!(next, Poll::Ready(None)) {
        state.stream.take();
    }
    next
}
