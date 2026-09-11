use std::future::{Future, pending};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};

use futures_util::future::{AbortHandle, Abortable};
use futures_util::task::{ArcWake, AtomicWaker};

use super::*;

#[derive(Default)]
struct Gate {
    ready: AtomicBool,
    waker: AtomicWaker,
}

impl Gate {
    fn open(&self) {
        self.ready.store(true, Ordering::SeqCst);
        self.waker.wake();
    }

    async fn wait(&self) {
        futures_util::future::poll_fn(|cx| {
            self.waker.register(cx.waker());
            if self.ready.load(Ordering::SeqCst) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
    }
}

#[derive(Default)]
struct WakeCount(AtomicUsize);

impl ArcWake for WakeCount {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn poll<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}

fn settling_stream() -> (ByteStream, AbortHandle, Arc<Gate>, Arc<AtomicUsize>) {
    let (abort, registration) = AbortHandle::new_pair();
    let gate = Arc::new(Gate::default());
    let settled = Arc::new(AtomicUsize::new(0));
    let completion_gate = gate.clone();
    let completion_count = settled.clone();
    let stream = futures_util::stream::once(async move {
        assert!(Abortable::new(pending::<()>(), registration).await.is_err());
        completion_gate.wait().await;
        completion_count.fetch_add(1, Ordering::SeqCst);
        Err(TransportError::Interrupted("cancelled".into()))
    });
    (Box::pin(stream), abort, gate, settled)
}

#[test]
fn cancellation_wakes_a_pending_pull_before_waiting_for_its_lock() {
    let (stream, abort, gate, settled) = settling_stream();
    let state = StreamState::new(stream, None);
    let wake_count = Arc::new(WakeCount::default());
    let waker = futures_util::task::waker(wake_count.clone());
    let mut cx = Context::from_waker(&waker);
    let mut next = Box::pin(state.next());
    assert!(next.as_mut().poll(&mut cx).is_pending());

    let mut cancel = Box::pin(interrupt_and_finish(&state.stream, Some(|| abort.abort())));
    assert!(poll(cancel.as_mut()).is_pending());
    assert!(wake_count.0.load(Ordering::SeqCst) > 0);
    assert!(next.as_mut().poll(&mut cx).is_pending());
    assert_eq!(settled.load(Ordering::SeqCst), 0);
    assert!(poll(cancel.as_mut()).is_pending());

    gate.open();
    assert!(matches!(
        next.as_mut().poll(&mut cx),
        Poll::Ready(Some(Err(_)))
    ));
    drop(next);
    assert!(poll(cancel.as_mut()).is_ready());
    assert_eq!(settled.load(Ordering::SeqCst), 1);
    assert!(state.stream.try_lock().unwrap().is_none());
}

#[test]
fn concurrent_cancellations_wait_for_the_same_inline_settlement() {
    let (stream, abort, gate, settled) = settling_stream();
    let state = StreamState::new(stream, None);
    let mut first = Box::pin(interrupt_and_finish(&state.stream, Some(|| abort.abort())));
    let mut second = Box::pin(interrupt_and_finish(&state.stream, Some(|| abort.abort())));
    assert!(poll(first.as_mut()).is_pending());
    assert!(poll(second.as_mut()).is_pending());
    assert_eq!(settled.load(Ordering::SeqCst), 0);

    gate.open();
    assert!(poll(second.as_mut()).is_pending());
    assert!(poll(first.as_mut()).is_ready());
    assert!(poll(second.as_mut()).is_ready());
    assert_eq!(settled.load(Ordering::SeqCst), 1);
}

#[test]
fn already_settled_stream_is_discarded_without_reading_more_upstream() {
    let polled = Arc::new(AtomicUsize::new(0));
    let stream_polled = polled.clone();
    let stream = futures_util::stream::poll_fn(move |_| {
        stream_polled.fetch_add(1, Ordering::SeqCst);
        Poll::Ready(Some(Ok(Bytes::from_static(b"unneeded frame"))))
    });
    let state = StreamState::new(Box::pin(stream), None);
    assert!(poll(Box::pin(state.cancel()).as_mut()).is_ready());
    assert!(state.is_cancelled());
    assert_eq!(polled.load(Ordering::SeqCst), 0);
    assert!(matches!(
        poll(Box::pin(state.next()).as_mut()),
        Poll::Ready(None)
    ));
}

#[test]
fn eof_releases_the_stream_and_error_cleanup_does_not_resume_it() {
    for failed in [false, true] {
        let completions = Arc::new(AtomicUsize::new(0));
        let completed = completions.clone();
        let stream = futures_util::stream::poll_fn(move |_| {
            assert_eq!(completed.fetch_add(1, Ordering::SeqCst), 0);
            Poll::Ready(failed.then(|| Err(TransportError::Interrupted("upstream error".into()))))
        });
        let state = StreamState::new(Box::pin(stream), None);
        let next = poll(Box::pin(state.next()).as_mut());
        assert!(matches!(next, Poll::Ready(Some(Err(_)))) == failed);
        assert!(poll(Box::pin(state.cancel()).as_mut()).is_ready());
        assert!(matches!(
            poll(Box::pin(state.next()).as_mut()),
            Poll::Ready(None)
        ));
        assert_eq!(completions.load(Ordering::SeqCst), 1);
    }
}
