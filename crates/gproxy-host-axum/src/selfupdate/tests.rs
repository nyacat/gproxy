use std::sync::Arc;
#[cfg(unix)]
use std::sync::atomic::Ordering;
use std::time::Duration;

use axum::{Router, routing::get};
use gproxy_admin::dto::RuntimeSettingsDto;
use http::{Method, StatusCode};
use tokio::sync::Notify;

use super::{Manager, config::Restart};

async fn fixture(restart: Restart) -> (tempfile::TempDir, gproxy_app::AppHandle, Manager) {
    let directory = tempfile::tempdir().unwrap();
    let app = gproxy_app::App::start(gproxy_app::Config::sqlite(
        "127.0.0.1:0".parse().unwrap(),
        directory.path().join("data"),
        gproxy_app::MasterKeyConfig::new(Some([5; 32])),
    ))
    .await
    .unwrap();
    let mut manager = Manager::new(directory.path().join("updates"), Some("releases")).unwrap();
    manager.executable = directory.path().join("gproxy-test");
    manager.restart = restart;
    std::fs::write(&manager.executable, b"current").unwrap();
    std::fs::write(directory.path().join("gproxy-test.prev"), b"previous").unwrap();
    (directory, app, manager)
}

#[tokio::test]
async fn active_download_rejects_overlapping_mutations_and_releases_after_failure_or_cancel() {
    let (_directory, app, mut manager) = fixture(Restart::None).await;
    let entered = Arc::new(Notify::new());
    let finish = Arc::new(Notify::new());
    let router = Router::new().route(
        "/manifest",
        get({
            let entered = entered.clone();
            let finish = finish.clone();
            move || {
                let entered = entered.clone();
                let finish = finish.clone();
                async move {
                    entered.notify_one();
                    finish.notified().await;
                    "{}"
                }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    manager.manifest_url = Some(format!(
        "http://{}/manifest",
        listener.local_addr().unwrap()
    ));
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let manager = Arc::new(manager);
    let settings = RuntimeSettingsDto::default();

    for cancel in [false, true] {
        let task = tokio::spawn({
            let manager = manager.clone();
            let app = app.clone();
            let settings = settings.clone();
            async move {
                manager
                    .dispatch(
                        &Method::POST,
                        "/admin/api/native/update/apply",
                        None,
                        &settings,
                        &app,
                    )
                    .await
            }
        });
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        for path in ["apply", "rollback"] {
            let response = manager
                .dispatch(
                    &Method::POST,
                    &format!("/admin/api/native/update/{path}"),
                    None,
                    &settings,
                    &app,
                )
                .await;
            assert_eq!(response.status(), StatusCode::CONFLICT);
            let body: serde_json::Value = serde_json::from_slice(response.body()).unwrap();
            assert_eq!(
                body["error"]["message"],
                "an update or rollback is already in progress"
            );
        }
        assert_eq!(std::fs::read(&manager.executable).unwrap(), b"current");
        if cancel {
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
        } else {
            finish.notify_one();
            assert_eq!(task.await.unwrap().status(), StatusCode::BAD_GATEWAY);
        }
        assert!(manager.begin_operation().is_ok());
    }
    server.abort();
    app.shutdown();
    app.drain_background().await;
}

#[tokio::test]
#[cfg(unix)]
async fn successful_rollback_requests_shutdown_and_blocks_mutations_until_restart() {
    let (_directory, app, manager) = fixture(Restart::ReExec).await;
    let settings = RuntimeSettingsDto::default();
    let response = manager
        .dispatch(
            &Method::POST,
            "/admin/api/native/update/rollback",
            None,
            &settings,
            &app,
        )
        .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(std::fs::read(&manager.executable).unwrap(), b"previous");
    tokio::time::timeout(Duration::from_secs(1), app.wait_shutdown())
        .await
        .unwrap();
    assert!(manager.restart_requested.load(Ordering::Acquire));
    let response = manager
        .dispatch(
            &Method::POST,
            "/admin/api/native/update/rollback",
            None,
            &settings,
            &app,
        )
        .await;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert_eq!(std::fs::read(&manager.executable).unwrap(), b"previous");
    app.drain_background().await;
}

#[tokio::test]
#[cfg(unix)]
async fn manual_restart_mode_leaves_the_application_running_and_releases_the_operation() {
    let (_directory, app, manager) = fixture(Restart::None).await;
    let response = manager
        .dispatch(
            &Method::POST,
            "/admin/api/native/update/rollback",
            None,
            &RuntimeSettingsDto::default(),
            &app,
        )
        .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert!(!manager.restart_requested.load(Ordering::Acquire));
    assert!(manager.begin_operation().is_ok());
    assert!(
        tokio::time::timeout(Duration::from_millis(10), app.wait_shutdown())
            .await
            .is_err()
    );
    app.shutdown();
    app.drain_background().await;
}
