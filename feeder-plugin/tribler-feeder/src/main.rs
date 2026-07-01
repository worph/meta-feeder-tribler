//! `tribler-feeder` — single-source BitTorrent feeder sidecar for the
//! decentralized Tribler (IPv8 overlay) network.
//!
//! One plugin (`tribler`) served through [`meta_feeder_sdk::serve_feeders`]. A
//! thin client of a headless Tribler core (reached at `TRIBLER_SIDECAR_URL`); it
//! finds records over the Tribler swarm and enriches them via the copied
//! torrent-core TMDB catalog-discovery stack. Split out of `meta-feeder-torrent`
//! so its dashboard config panel resolves to the tribler schema.
//!
//! ALL tribler config — sidecar URL, api key, tmdb token/language, meta-core
//! URL — is read from the persisted `config.json` (dashboard-written) ONLY.
//! There is NO env seed for these: a field in the dashboard config schema must
//! not have an env var (config from JSON file / web UI is the single source of
//! truth).
//!
//! Env (infra only): `META_FEEDER_HTTP_LISTEN` (default `0.0.0.0:8080`),
//! `META_FEEDER_STATE_DIR` (default `/data/meta-feeder`), `META_GATEWAY_PEER_ID`,
//! the enrichment-plugin URLs (`FILENAME_PARSER_URL`, `TMDB_PLUGIN_URL`,
//! `META_FEEDER_CALLBACK_URL`), `RUST_LOG`.

use std::net::SocketAddr;

use meta_feeder_sdk::plugin::FeederPlugin;
use meta_feeder_sdk::serve_feeders;
use tracing::info;
use tracing_subscriber::EnvFilter;

use tribler_feeder::tribler::TriblerPlugin;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let listen: SocketAddr = std::env::var("META_FEEDER_HTTP_LISTEN")
        .unwrap_or_else(|_| "0.0.0.0:8080".to_string())
        .parse()?;
    let state_dir =
        std::env::var("META_FEEDER_STATE_DIR").unwrap_or_else(|_| "/data/meta-feeder".to_string());

    // No env seed: the plugin reads config.json in configure() (sidecar URL,
    // api key, tmdb, meta-core URL) and builds its enrichment/self-publish
    // driver from config.meta_core_url there.
    let tr = TriblerPlugin::new();

    let plugins: Vec<Box<dyn FeederPlugin>> = vec![Box::new(tr)];

    info!(target: "meta-feeder", loaded = plugins.len(), "tribler feeder starting");

    serve_feeders(plugins, state_dir, listen).await
}
