use std::future::IntoFuture;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::serve::ListenerExt;

pub(crate) const MAX_BODY_BYTES: usize = 100 * 1024 * 1024;

#[derive(Clone, Default)]
pub struct HostConfig {
    instance_id: u64,
    autostart: Option<Arc<crate::autostart::Manager>>,
    selfupdate: Option<Arc<crate::selfupdate::Manager>>,
}

impl HostConfig {
    pub fn from_config(config: &gproxy_app::Config) -> Self {
        let autostart = Arc::new(crate::autostart::Manager::for_current_process(
            config.data_dir().to_owned(),
        ));
        if let Err(error) = autostart.initialize_default() {
            tracing::warn!(%error, "automatic startup initialization failed");
        }
        let selfupdate =
            crate::selfupdate::Manager::new(config.data_dir().to_owned(), config.update_channel())
                .map(Arc::new)
                .map_err(|error| tracing::warn!(%error, "self-update initialization failed"))
                .ok();
        Self {
            instance_id: config.instance_id(),
            autostart: Some(autostart),
            selfupdate,
        }
    }
}

#[derive(Clone)]
pub(crate) struct HostState {
    pub app: gproxy_app::AppHandle,
    pub requests: Arc<gproxy_app::ConcurrencyLimit>,
    pub uploads: Arc<gproxy_app::ConcurrencyLimit>,
    pub announcements: crate::announce::Announcements,
    pub autostart: Option<Arc<crate::autostart::Manager>>,
    pub selfupdate: Option<Arc<crate::selfupdate::Manager>>,
    instance_id: u64,
    request_prefix: u64,
    request_counter: Arc<AtomicU64>,
}

impl HostState {
    fn new(app: gproxy_app::AppHandle, config: HostConfig) -> Result<Self, HostError> {
        let mut prefix = [0_u8; 8];
        getrandom::fill(&mut prefix).map_err(|_| HostError::Randomness)?;
        Ok(Self {
            app,
            requests: gproxy_app::ConcurrencyLimit::new(1024),
            uploads: gproxy_app::ConcurrencyLimit::new(0),
            announcements: crate::announce::Announcements::new(),
            autostart: config.autostart,
            selfupdate: config.selfupdate,
            instance_id: config.instance_id,
            request_prefix: u64::from_be_bytes(prefix),
            request_counter: Arc::new(AtomicU64::new(1)),
        })
    }

    fn sync_runtime(&self) {
        let runtime = self.app.runtime_settings();
        self.requests
            .set_limit(runtime.effective.max_in_flight as usize);
        self.uploads
            .set_limit(runtime.effective.file_upload_max_in_flight as usize);
        crate::logging::apply(&runtime);
    }

    pub(crate) fn request_id(&self) -> String {
        let sequence = self.request_counter.fetch_add(1, Ordering::Relaxed);
        format!(
            "{}-{:016x}-{sequence:016x}",
            self.instance_id, self.request_prefix
        )
    }
}

pub struct AxumServer {
    address: SocketAddr,
    app: gproxy_app::AppHandle,
    task: tokio::task::JoinHandle<Result<(), std::io::Error>>,
    selfupdate: Option<Arc<crate::selfupdate::Manager>>,
}

impl AxumServer {
    pub async fn bind(app: gproxy_app::AppHandle, address: SocketAddr) -> Result<Self, HostError> {
        Self::bind_with_config(app, address, HostConfig::default()).await
    }

    pub async fn bind_with_config(
        app: gproxy_app::AppHandle,
        address: SocketAddr,
        config: HostConfig,
    ) -> Result<Self, HostError> {
        let listener = tokio::net::TcpListener::bind(address)
            .await
            .map_err(HostError::Io)?;
        Self::from_listener(app, listener, config)
    }

    pub fn from_listener(
        app: gproxy_app::AppHandle,
        listener: tokio::net::TcpListener,
        config: HostConfig,
    ) -> Result<Self, HostError> {
        let address = listener.local_addr().map_err(HostError::Io)?;
        let shutdown = app.clone();
        let selfupdate = config.selfupdate.clone();
        let state = HostState::new(app.clone(), config)?;
        // Subscribe before loading the initial snapshot so a setting changed
        // during startup is either in that snapshot or delivered by the watch.
        let mut updates = app.subscribe_runtime_settings();
        state.sync_runtime();
        let router = Router::new()
            .fallback(crate::ingress::handle)
            .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
            .with_state(state.clone());
        let task = tokio::spawn(async move {
            // SSE flushes small frames as they arrive. Nagle buffering can hold
            // the final frame until the peer's delayed ACK timer fires.
            let listener = listener.tap_io(|stream| {
                if let Err(error) = stream.set_nodelay(true) {
                    tracing::warn!(%error, "could not disable TCP buffering for accepted connection");
                }
            });
            let serving = axum::serve(
                listener,
                router.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .with_graceful_shutdown(async move { shutdown.wait_shutdown().await })
            .into_future();
            tokio::pin!(serving);
            loop {
                tokio::select! {
                    result = &mut serving => return result,
                    changed = updates.changed() => {
                        if changed.is_err() { return serving.await; }
                        state.sync_runtime();
                    }
                }
            }
        });
        Ok(Self {
            address,
            app,
            task,
            selfupdate,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.address
    }

    pub async fn shutdown(self) -> Result<(), HostError> {
        self.app.shutdown();
        self.wait().await
    }

    pub async fn wait(self) -> Result<(), HostError> {
        let result = self
            .task
            .await
            .map_err(HostError::Join)
            .and_then(|result| result.map_err(HostError::Io));
        self.app.shutdown();
        self.app.drain_background().await;
        if let Some(manager) = self.selfupdate {
            manager.restart_if_requested();
        }
        result
    }
}

#[derive(Debug, thiserror::Error)]
pub enum HostError {
    #[error("listener: {0}")]
    Io(#[source] std::io::Error),
    #[error("listener task: {0}")]
    Join(#[source] tokio::task::JoinError),
    #[error("secure request-id randomness unavailable")]
    Randomness,
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn internal_shutdown_waits_for_background_completion() {
        let directory = tempfile::tempdir().unwrap();
        let app = gproxy_app::App::start(gproxy_app::Config::sqlite(
            "127.0.0.1:0".parse().unwrap(),
            directory.path().to_path_buf(),
            gproxy_app::MasterKeyConfig::new(Some([2; 32])),
        ))
        .await
        .unwrap();
        let server = super::AxumServer::bind(app.clone(), "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let (finish, finished) = tokio::sync::oneshot::channel();
        let (complete, completed) = tokio::sync::oneshot::channel();
        drop(app.spawn_background(async move {
            finished.await.unwrap();
            complete.send(()).unwrap();
        }));
        app.shutdown();
        let stopped = server.wait();
        tokio::pin!(stopped);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), &mut stopped)
                .await
                .is_err()
        );
        finish.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), stopped)
            .await
            .unwrap()
            .unwrap();
        completed.await.unwrap();
    }
}
