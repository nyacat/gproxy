use serde_json::json;

#[tokio::test]
async fn cancelled_execution_keeps_ownership_while_waiting_for_settlement_capacity() {
    cancelled_execution(false).await;
}

#[tokio::test]
async fn cancelled_streaming_execution_drops_outcome_into_tracked_settlement() {
    cancelled_execution(true).await;
}

async fn cancelled_execution(streaming: bool) {
    use gproxy_core::CacheBackend;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let fixture = super::setup::fixture().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let upstream = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut received = Vec::new();
        loop {
            let mut buffer = [0; 4096];
            let size = socket.read(&mut buffer).await.unwrap();
            assert_ne!(size, 0);
            received.extend_from_slice(&buffer[..size]);
            if let Some(end) = received.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                let length = std::str::from_utf8(&received[..end])
                    .unwrap()
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().unwrap())
                    })
                    .unwrap();
                if received.len() >= end + 4 + length {
                    break;
                }
            }
        }
        let body = if streaming {
            "data: {\"id\":\"response\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"upstream-model\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ok\"},\"finish_reason\":null}]}\n\ndata: [DONE]\n\n"
        } else {
            r#"{"id":"response","object":"chat.completion","created":1,"model":"upstream-model","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}"#
        };
        let content_type = if streaming {
            "text/event-stream"
        } else {
            "application/json"
        };
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        socket.write_all(response.as_bytes()).await.unwrap();
    });
    let app = &fixture.app;
    app.inner
        .host
        .services
        .store
        .update_provider(
            fixture.provider,
            &gproxy_store::records::ProviderInput {
                name: "provider".into(),
                label: None,
                channel: "openai".into(),
                settings: json!({"base_url": format!("http://{address}")}),
                credential_strategy: "round_robin".into(),
                proxy_url: None,
                tls_fingerprint: None,
                enabled: true,
            },
        )
        .await
        .unwrap();
    app.reload().await.unwrap();
    app.inner
        .host
        .services
        .transport
        .set_inherit_system_proxy(false);
    let spawner = &app.inner.host.services.spawner;
    spawner.set_max_in_flight(1);
    spawner.settlement_limit().set_limit(1);
    let occupied = spawner.settlement_limit().acquire().await;
    let mut request = super::setup::request("cancelled-execution", "hi", &fixture.client_key);
    request.body = bytes::Bytes::from(
        json!({
            "model": "public-model", "stream": streaming,
            "messages": [{"role": "user", "content": "hi"}]
        })
        .to_string(),
    );
    let request_id = request.request_id.clone();
    let task_app = app.clone();
    let caller = tokio::spawn(async move { task_app.execute(request).await });
    tokio::time::timeout(
        std::time::Duration::from_secs(3),
        spawner.settlement_waiting.notified(),
    )
    .await
    .unwrap();
    caller.abort();
    match caller.await {
        Err(error) => assert!(error.is_cancelled()),
        Ok(_) => panic!("caller should still be waiting for settlement capacity"),
    }
    assert!(app.admission_pending(&request_id).await.unwrap());
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(10),
            spawner.reserve_execution()
        )
        .await
        .is_err(),
        "cancelled callers cannot create extra detached executions"
    );

    drop(occupied);
    let execution = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        spawner.reserve_execution(),
    )
    .await
    .unwrap();
    drop(execution);
    app.shutdown();
    tokio::time::timeout(std::time::Duration::from_secs(3), app.drain_background())
        .await
        .unwrap();
    upstream.await.unwrap();
    let usage = app.usage_by_request(&request_id).await.unwrap().unwrap();
    if streaming {
        assert_eq!(usage.usage.ended, "interrupted");
    } else {
        assert_eq!(usage.usage.input_tokens, 1);
    }
    assert!(usage.usage.cost > rust_decimal::Decimal::ZERO);
    assert_eq!(
        app.inner.host.services.store.usage_count().await.unwrap(),
        1
    );
    assert!(!app.admission_pending(&request_id).await.unwrap());
    let windows = app.quota_windows().await.unwrap();
    assert!(!windows.is_empty());
    for window in windows {
        assert_eq!(window.cost_used, usage.usage.cost);
        let pending = app
            .inner
            .host
            .services
            .cache
            .get(&format!("gproxy:quota-pending:{}", window.id))
            .await
            .unwrap();
        assert_eq!(
            pending
                .map(|bytes| i64::from_be_bytes(bytes.try_into().unwrap()))
                .unwrap_or(0),
            0
        );
    }
    drop(
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            spawner.settlement_limit().acquire(),
        )
        .await
        .unwrap(),
    );
}

#[tokio::test]
async fn shutdown_drains_background_work_and_releases_services() {
    let directory = tempfile::tempdir().unwrap();
    let app = crate::App::start(super::test_config(
        directory.path(),
        crate::MasterKeyConfig::new(None),
    ))
    .await
    .unwrap();
    let services = std::sync::Arc::downgrade(&app.inner.host.services);
    let (release, released) = tokio::sync::oneshot::channel();
    let (completed, completion) = tokio::sync::oneshot::channel();
    drop(app.inner.host.services.spawner.spawn_tracked(async move {
        released.await.unwrap();
        completed.send(()).unwrap();
    }));
    app.shutdown();
    {
        let drain = app.drain_background();
        tokio::pin!(drain);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), &mut drain)
                .await
                .is_err()
        );
        release.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), drain)
            .await
            .unwrap();
    }
    completion.await.unwrap();
    drop(app);
    assert!(
        services.upgrade().is_none(),
        "maintenance must release the store and cache"
    );
}

#[tokio::test]
async fn fresh_instance_loads_global_prices_once() {
    let directory = tempfile::tempdir().unwrap();
    let config = || super::test_config(directory.path(), crate::MasterKeyConfig::new(None));
    let app = crate::App::start(config()).await.unwrap();
    let snapshot = app.inner.host.services.control.current();
    let expected = gproxy_admin::default_model_price_count();
    assert_eq!(snapshot.price_rules.len(), expected);
    assert!(
        snapshot
            .price_rules
            .iter()
            .all(|rule| rule.provider_id.is_none())
    );
    drop(app);

    let app = crate::App::start(config()).await.unwrap();
    assert_eq!(
        app.inner.host.services.control.current().price_rules.len(),
        expected
    );
}

#[tokio::test]
async fn shared_invalidation_refreshes_the_control_snapshot() {
    let directory = tempfile::tempdir().unwrap();
    let app = crate::App::start(super::test_config(
        directory.path(),
        crate::MasterKeyConfig::new(None),
    ))
    .await
    .unwrap();
    app.inner
        .host
        .services
        .store
        .set_setting(&gproxy_store::records::SettingInput {
            key: gproxy_store::records::INSTANCE_NAME.into(),
            value: json!("remote-instance"),
        })
        .await
        .unwrap();
    crate::invalidation::bump(&app.inner.host.services.cache)
        .await
        .unwrap();

    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while app.instance_name() != "remote-instance" {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("shared invalidation was not consumed");
}

#[tokio::test]
async fn older_runtime_reload_cannot_publish_after_a_newer_mutation() {
    let fixture = super::setup::fixture().await;
    let app = &fixture.app;
    app.shutdown();
    app.drain_background().await;
    let (read, read_done) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    *app.inner.reload_runtime_pause.lock().unwrap() = Some(crate::lifecycle::ReloadRuntimePause {
        read,
        resume: resumed,
    });
    let old_app = app.clone();
    let old_reload = tokio::spawn(async move { old_app.reload().await });
    read_done.await.unwrap();
    let new_app = app.clone();
    let mut changed = tokio::spawn(async move {
        new_app
            .mutate(crate::ControlMutation::Setting(
                gproxy_store::records::SettingInput {
                    key: "max_in_flight".into(),
                    value: json!(7),
                },
            ))
            .await
    });
    // The newer mutation can finish its database write while the earlier
    // runtime publication is suspended. If publication is not ordered, it
    // completes first and the old reload later replaces its watch value.
    let new_result =
        tokio::time::timeout(std::time::Duration::from_millis(100), &mut changed).await;
    resume.send(()).unwrap();
    old_reload.await.unwrap().unwrap();
    match new_result {
        Ok(result) => {
            result.unwrap().unwrap();
        }
        Err(_) => {
            changed.await.unwrap().unwrap();
        }
    }
    assert_eq!(app.runtime_settings().effective.max_in_flight, 7);
    assert_eq!(
        app.subscribe_runtime_settings()
            .borrow()
            .effective
            .max_in_flight,
        7
    );
    assert_eq!(
        app.inner
            .invalidation_version
            .load(std::sync::atomic::Ordering::Acquire),
        crate::invalidation::current(&app.inner.host.services.cache)
            .await
            .unwrap()
    );
}
