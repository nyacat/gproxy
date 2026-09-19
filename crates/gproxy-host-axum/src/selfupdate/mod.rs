mod android_apk;
mod config;
mod download;
mod extract;
mod manifest;
mod notes;
mod swap;
#[cfg(test)]
mod tests;
mod version;

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use gproxy_admin::dto::{UpdateAppliedDto, UpdateStatusDto};
use http::{Method, Response, StatusCode};

pub(crate) use config::Error;
use config::{Restart, channel, restart};

type Result<T> = std::result::Result<T, Error>;

pub(crate) struct Manager {
    client: crate::outbound::OutboundClient,
    data_dir: PathBuf,
    manifest_url: Option<String>,
    restart: Restart,
    channel: Option<String>,
    store_managed: bool,
    executable: PathBuf,
    operation: tokio::sync::Mutex<()>,
    restart_requested: AtomicBool,
}

impl Manager {
    pub(crate) fn new(data_dir: PathBuf, channel: Option<&str>) -> Result<Self> {
        let manifest_url = std::env::var("GPROXY_UPDATE_SERVE").ok();
        Ok(Self {
            client: crate::outbound::OutboundClient::new(concat!(
                "gproxy-selfupdate/",
                env!("CARGO_PKG_VERSION")
            )),
            data_dir,
            manifest_url,
            restart: restart()?,
            channel: channel.map(str::to_owned),
            store_managed: crate::installation_kind() == "microsoft-store",
            // Replacing a running executable changes current_exe() on Linux.
            executable: std::env::current_exe()?,
            operation: tokio::sync::Mutex::new(()),
            restart_requested: AtomicBool::new(false),
        })
    }

    async fn check(
        &self,
        selected_channel: Option<&str>,
        settings: &gproxy_admin::dto::RuntimeSettingsDto,
    ) -> Result<UpdateStatusDto> {
        let client = self
            .client
            .get(settings)
            .map_err(|_| Error::Configuration)?;
        let channel = channel(selected_channel, self.channel.as_deref())?;
        let manifest = download::manifest(&client, &self.manifest_url(&channel)).await?;
        if manifest.channel != channel {
            return Err(Error::Manifest);
        }
        let target = version::target();
        let _artifact = manifest.artifact(&target)?;
        let (current, available) = version::available(&channel, &manifest.version)?;
        let notes = if available && channel != "staging" && manifest.notes_url.is_some() {
            notes::fetch(&client, &manifest.version).await
        } else {
            None
        };
        Ok(UpdateStatusDto {
            current,
            latest: manifest.version,
            available,
            channel,
            target,
            notes,
            rollback_available: swap::rollback_available(&self.executable),
            restart: self.restart.as_str().into(),
        })
    }

    async fn apply(
        &self,
        selected_channel: Option<&str>,
        settings: &gproxy_admin::dto::RuntimeSettingsDto,
    ) -> Result<(UpdateAppliedDto, bool)> {
        let client = self
            .client
            .get(settings)
            .map_err(|_| Error::Configuration)?;
        let channel = channel(selected_channel, self.channel.as_deref())?;
        let manifest = download::manifest(&client, &self.manifest_url(&channel)).await?;
        if manifest.channel != channel {
            return Err(Error::Manifest);
        }
        version::compatible(
            manifest.min_compatible_data_version,
            gproxy_store::schema::SchemaVersion::LATEST.number() as u32,
        )?;
        let (_, available) = version::available(&channel, &manifest.version)?;
        if available {
            let target = version::target();
            let bytes = download::artifact(&client, manifest.artifact(&target)?).await?;
            if target.ends_with("-apk") {
                android_apk::stage(&self.data_dir, &bytes)?;
            } else {
                let staged = extract::binary(&bytes, &self.data_dir.join(".update"), &target)?;
                swap::install(&self.executable, &staged)?;
            }
        }
        Ok((
            UpdateAppliedDto {
                version: manifest.version,
                restart: self.restart.as_str().into(),
            },
            available,
        ))
    }

    pub(crate) async fn dispatch(
        &self,
        method: &Method,
        path: &str,
        selected_channel: Option<&str>,
        settings: &gproxy_admin::dto::RuntimeSettingsDto,
        app: &gproxy_app::AppHandle,
    ) -> Response<Bytes> {
        if self.store_managed {
            let error = Error::MicrosoftStore;
            return json(
                error.status(),
                serde_json::json!({"error": {"message": error.to_string()}}),
            );
        }
        let _operation = if method == Method::POST
            && matches!(
                path,
                "/admin/api/native/update/apply" | "/admin/api/native/update/rollback"
            ) {
            match self.begin_operation() {
                Ok(guard) => Some(guard),
                Err(error) => {
                    return json(
                        error.status(),
                        serde_json::json!({"error": {"message": error.to_string()}}),
                    );
                }
            }
        } else {
            None
        };
        let (result, restart_after) = match (method, path) {
            (&Method::GET | &Method::HEAD, "/admin/api/native/update") => {
                (self.check(selected_channel, settings).await.and_then(to_value), false)
            }
            (&Method::POST, "/admin/api/native/update/apply") => match self.apply(selected_channel, settings).await {
                Ok((applied, changed)) => (to_value(applied), changed),
                Err(error) => (Err(error), false),
            },
            (&Method::POST, "/admin/api/native/update/rollback") => (
                swap::rollback(&self.executable).map(|_| {
                    serde_json::json!({ "version": crate::BUILD_VERSION, "restart": self.restart.as_str() })
                }),
                true,
            ),
            _ => return json(StatusCode::METHOD_NOT_ALLOWED, serde_json::json!({})),
        };
        match result {
            Ok(value) => {
                if restart_after && !matches!(self.restart, Restart::None) {
                    self.restart_requested.store(true, Ordering::Release);
                    app.shutdown();
                }
                json(StatusCode::OK, value)
            }
            Err(error) => json(
                error.status(),
                serde_json::json!({"error": {"message": error.to_string()}}),
            ),
        }
    }

    fn begin_operation(&self) -> Result<tokio::sync::MutexGuard<'_, ()>> {
        let guard = self.operation.try_lock().map_err(|_| Error::Busy)?;
        if self.restart_requested.load(Ordering::Acquire) {
            return Err(Error::Busy);
        }
        Ok(guard)
    }

    pub(crate) fn restart_if_requested(&self) {
        if !self.restart_requested.load(Ordering::Acquire) {
            return;
        }
        match self.restart {
            Restart::None => {}
            Restart::Supervisor => std::process::exit(42),
            Restart::ReExec => reexec(&self.executable),
        }
    }

    fn manifest_url(&self, channel: &str) -> String {
        self.manifest_url.clone().unwrap_or_else(|| match channel {
            "dev" => {
                "https://github.com/LeenHawk/gproxy/releases/download/dev/manifest.json".into()
            }
            "staging" => {
                "https://github.com/LeenHawk/gproxy/releases/download/staging/manifest.json".into()
            }
            _ => "https://github.com/LeenHawk/gproxy/releases/latest/download/manifest.json".into(),
        })
    }
}

#[cfg(unix)]
fn reexec(executable: &Path) -> ! {
    use std::os::unix::process::CommandExt as _;
    let error = std::process::Command::new(executable)
        .args(std::env::args_os().skip(1))
        .exec();
    tracing::error!(%error, "updated process re-exec failed");
    std::process::exit(1)
}

#[cfg(not(unix))]
fn reexec(_executable: &Path) -> ! {
    std::process::exit(42)
}

fn to_value(value: impl serde::Serialize) -> Result<serde_json::Value> {
    serde_json::to_value(value).map_err(|_| Error::Manifest)
}

fn json(status: StatusCode, value: serde_json::Value) -> Response<Bytes> {
    let mut response = Response::new(Bytes::from(value.to_string()));
    *response.status_mut() = status;
    response.headers_mut().insert(
        http::header::CONTENT_TYPE,
        http::HeaderValue::from_static("application/json"),
    );
    response
}

pub(crate) fn unavailable() -> Response<Bytes> {
    json(
        StatusCode::SERVICE_UNAVAILABLE,
        serde_json::json!({"error": {"message": Error::Configuration.to_string()}}),
    )
}
