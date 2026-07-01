//! Feeder HTTP contract smoke test for `tribler-feeder`.
//!
//! Boots the real `TriblerPlugin` inside the SDK's `serve_feeders` harness (via
//! `router` + `configure_plugins`, so we can bind an ephemeral port) and asserts
//! the static surface: `GET /manifest` advertises the `tribler` upstream, and
//! `GET /health` is `ok`. Live search/compute are not driven here — they need a
//! running Tribler core (IPv8 swarm); that path is exercised by the dev stack's
//! bats suite against a real `tribler-instance`.

use std::net::SocketAddr;

use meta_feeder_sdk::serve::{configure_plugins, router};
use tribler_feeder::tribler::TriblerPlugin;

async fn boot_feeder() -> (SocketAddr, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let plugin = TriblerPlugin::new();
    let configured =
        configure_plugins(vec![Box::new(plugin)], dir.path()).expect("configure");
    let app = router(configured, "test".to_string(), dir.path());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    (addr, dir)
}

#[tokio::test]
async fn manifest_advertises_tribler_and_health_ok() {
    let (addr, _dir) = boot_feeder().await;
    let base = format!("http://{addr}");
    let http = reqwest::Client::new();

    let manifest: serde_json::Value = http
        .get(format!("{base}/manifest"))
        .send()
        .await
        .expect("manifest req")
        .json()
        .await
        .expect("manifest json");
    let ids: Vec<&str> = manifest["plugins"]
        .as_array()
        .expect("plugins array")
        .iter()
        .filter_map(|p| p["id"].as_str())
        .collect();
    assert!(ids.contains(&"tribler"), "manifest must advertise tribler, got {ids:?}");

    let health: serde_json::Value = http
        .get(format!("{base}/health"))
        .send()
        .await
        .expect("health req")
        .json()
        .await
        .expect("health json");
    assert_eq!(health["status"], "ok");
}
