use gproxy_core::{ControlPlane, RoutingMode};
use tokio::sync::oneshot;

pub(super) struct Pause {
    pub read: oneshot::Sender<()>,
    pub resume: oneshot::Receiver<()>,
}

#[tokio::test]
async fn an_older_reload_cannot_overwrite_a_completed_mutation() {
    let fixture = crate::tests::setup::fixture().await;
    let app = &fixture.app;
    app.shutdown();
    app.drain_background().await;
    let control = &app.inner.host.services.control;
    let (read, read_done) = oneshot::channel();
    let (resume, resumed) = oneshot::channel();
    *control.reload_pause.lock().unwrap() = Some(Pause {
        read,
        resume: resumed,
    });
    let old_control = control.clone();
    let old_reload = tokio::spawn(async move { old_control.reload().await });
    read_done.await.unwrap();
    app.mutate(crate::ControlMutation::ExposedModel(
        gproxy_store::records::ExposedModelInput {
            name: "new-exact-route".into(),
            route_id: fixture.route,
            enabled: true,
        },
    ))
    .await
    .unwrap();
    assert!(
        control
            .resolve(Some("new-exact-route"), &RoutingMode::Aggregated, None)
            .is_ok()
    );
    resume.send(()).unwrap();
    old_reload.await.unwrap().unwrap();
    let plan = control
        .resolve(Some("new-exact-route"), &RoutingMode::Aggregated, None)
        .unwrap();
    assert_eq!(plan.targets[0].upstream_model, "upstream-model");
}
