use crate::response::RequestPermit;
use axum::extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade};
use gproxy_channel_api::{WsDuplex, WsFrame};

pub(crate) fn upgrade(
    upgrade: WebSocketUpgrade,
    upstream: Box<dyn WsDuplex>,
    permit: RequestPermit,
    app: gproxy_app::AppHandle,
) -> axum::response::Response {
    let (ready, socket) = tokio::sync::oneshot::channel();
    let task_app = app.clone();
    drop(app.spawn_background(async move {
        let _permit = permit;
        let socket = tokio::select! {
            biased;
            () = task_app.wait_shutdown() => return,
            socket = socket => match socket {
                Ok(socket) => socket,
                Err(_) => return,
            },
        };
        pump(socket, upstream, task_app).await;
    }));
    upgrade.on_upgrade(move |socket| async move {
        let _ = ready.send(socket);
    })
}

async fn pump(
    mut downstream: WebSocket,
    mut upstream: Box<dyn WsDuplex>,
    app: gproxy_app::AppHandle,
) {
    let stopping = tokio::select! {
        biased;
        () = app.wait_shutdown() => true,
        () = relay(&mut downstream, upstream.as_mut()) => false,
    };
    // Native duplex wrappers settle interruptions from Drop. Release upstream
    // before the tracked pump ends, so its settlement is included in draining.
    drop(upstream);
    if stopping {
        let _ = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            close_downstream(&mut downstream, Some(1001)),
        )
        .await;
    }
}

async fn relay(downstream: &mut WebSocket, upstream: &mut dyn WsDuplex) {
    loop {
        tokio::select! {
            message = downstream.recv() => {
                let Some(message) = message else {
                    close_upstream(upstream).await;
                    return;
                };
                let Ok(message) = message else {
                    close_upstream(upstream).await;
                    return;
                };
                match message {
                    Message::Text(text) => {
                        if upstream
                            .send(WsFrame::Text(text.as_str().to_owned()))
                            .await
                            .is_err()
                        {
                            close_downstream(downstream, None).await;
                            return;
                        }
                    }
                    Message::Binary(bytes) => {
                        if upstream.send(WsFrame::Binary(bytes)).await.is_err() {
                            close_downstream(downstream, None).await;
                            return;
                        }
                    }
                    Message::Close(frame) => {
                        let code = frame.map(|frame| frame.code);
                        let _ = upstream.send(WsFrame::Close(code)).await;
                        return;
                    }
                    Message::Ping(payload) => {
                        if downstream.send(Message::Pong(payload)).await.is_err() {
                            close_upstream(upstream).await;
                            return;
                        }
                    }
                    Message::Pong(_) => {}
                }
            }
            frame = upstream.recv() => {
                match frame {
                    Ok(Some(WsFrame::Text(text))) => {
                        if downstream.send(Message::Text(text.into())).await.is_err() {
                            close_upstream(upstream).await;
                            return;
                        }
                    }
                    Ok(Some(WsFrame::Binary(bytes))) => {
                        if downstream.send(Message::Binary(bytes)).await.is_err() {
                            close_upstream(upstream).await;
                            return;
                        }
                    }
                    Ok(Some(WsFrame::Close(code))) => {
                        close_downstream(downstream, code).await;
                        return;
                    }
                    Ok(None) | Err(_) => {
                        close_downstream(downstream, None).await;
                        return;
                    }
                }
            }
        }
    }
}

async fn close_downstream(downstream: &mut WebSocket, code: Option<u16>) {
    let frame = code.map(|code| CloseFrame {
        code,
        reason: "".into(),
    });
    let _ = downstream.send(Message::Close(frame)).await;
}

async fn close_upstream(upstream: &mut dyn WsDuplex) {
    let _ = upstream.send(WsFrame::Close(None)).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use gproxy_channel_api::{BoxFuture, TransportError};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::sync::oneshot;

    struct StalledSocket {
        app: gproxy_app::AppHandle,
        sending: Option<oneshot::Sender<()>>,
        dropped: Option<oneshot::Sender<()>>,
        release: Option<oneshot::Receiver<()>>,
        settled: Option<oneshot::Sender<()>>,
    }

    impl WsDuplex for StalledSocket {
        fn send<'a>(&'a mut self, _frame: WsFrame) -> BoxFuture<'a, Result<(), TransportError>> {
            Box::pin(async move {
                if let Some(sending) = self.sending.take() {
                    let _ = sending.send(());
                }
                std::future::pending().await
            })
        }

        fn recv<'a>(&'a mut self) -> BoxFuture<'a, Result<Option<WsFrame>, TransportError>> {
            Box::pin(std::future::pending())
        }
    }

    impl Drop for StalledSocket {
        fn drop(&mut self) {
            let dropped = self.dropped.take().unwrap();
            let release = self.release.take().unwrap();
            let settled = self.settled.take().unwrap();
            drop(self.app.spawn_background(async move {
                dropped.send(()).unwrap();
                release.await.unwrap();
                settled.send(()).unwrap();
            }));
        }
    }

    #[derive(Clone)]
    struct State {
        app: gproxy_app::AppHandle,
        socket: Arc<Mutex<Option<StalledSocket>>>,
    }

    async fn accept(
        axum::extract::State(state): axum::extract::State<State>,
        request: WebSocketUpgrade,
    ) -> axum::response::Response {
        let upstream = state.socket.lock().unwrap().take().unwrap();
        upgrade(request, Box::new(upstream), None, state.app)
    }

    #[tokio::test]
    async fn shutdown_cancels_stalled_websocket_and_drains_its_settlement() {
        let directory = tempfile::tempdir().unwrap();
        let app = gproxy_app::App::start(gproxy_app::Config::sqlite(
            "127.0.0.1:0".parse().unwrap(),
            directory.path().to_path_buf(),
            gproxy_app::MasterKeyConfig::new(Some([1; 32])),
        ))
        .await
        .unwrap();
        let (sending, send_started) = oneshot::channel();
        let (dropped, was_dropped) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let (settled, settlement) = oneshot::channel();
        let state = State {
            app: app.clone(),
            socket: Arc::new(Mutex::new(Some(StalledSocket {
                app: app.clone(),
                sending: Some(sending),
                dropped: Some(dropped),
                release: Some(released),
                settled: Some(settled),
            }))),
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let stopping = app.clone();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                axum::Router::new().fallback(accept).with_state(state),
            )
            .with_graceful_shutdown(async move { stopping.wait_shutdown().await })
            .await
        });
        let client = wreq::Client::builder().no_proxy().build().unwrap();
        let mut socket =
            wreq::ws::WebSocketRequestBuilder::new(client.get(format!("http://{address}/")))
                .send()
                .await
                .unwrap()
                .into_websocket()
                .await
                .unwrap();
        socket
            .send(wreq::ws::message::Message::text("hello"))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), send_started)
            .await
            .unwrap()
            .unwrap();

        app.shutdown();
        tokio::time::timeout(Duration::from_secs(2), was_dropped)
            .await
            .unwrap()
            .unwrap();
        let drain = app.drain_background();
        tokio::pin!(drain);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut drain)
                .await
                .is_err()
        );
        release.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), drain)
            .await
            .unwrap();
        settlement.await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}
