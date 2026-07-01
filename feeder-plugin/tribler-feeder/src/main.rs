//! `tribler-feeder` — single-source BitTorrent feeder sidecar for the
//! decentralized Tribler (IPv8 overlay) network.
//!
//! One plugin (`tribler`) served through [`meta_feeder_sdk::serve_feeders`]. A
//! thin client of a headless Tribler core (reached at `TRIBLER_SIDECAR_URL`); it
//! finds records over the Tribler swarm and enriches them via the copied
//! torrent-core TMDB catalog-discovery stack. Split out of `meta-feeder-torrent`
//! so its dashboard config panel resolves to the tribler schema.
//!
//! Env (shared): `META_FEEDER_HTTP_LISTEN` (default `0.0.0.0:8080`),
//! `META_FEEDER_STATE_DIR` (default `/data/meta-feeder`), `META_CORE_URL` (D4
//! self-publish), `RUST_LOG`.
//! Env (tribler source): `TRIBLER_SIDECAR_URL`, `TRIBLER_API_KEY`,
//! `TMDB_TOKEN`/`TMDB_LANGUAGE`, plus the enrichment-plugin URLs
//! (`FILENAME_PARSER_URL`, `TMDB_PLUGIN_URL`, `META_FEEDER_CALLBACK_URL`).

use std::net::SocketAddr;

use meta_feeder_sdk::plugin::FeederPlugin;
use meta_feeder_sdk::serve_feeders;
use tracing::info;
use tracing_subscriber::EnvFilter;

use tribler_feeder::tribler::{TriblerConfigFile, TriblerPlugin};

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

    // Env is the first-boot seed; the persisted config.json (under the
    // per-source cache dir) wins once saved through the dashboard. The tribler
    // source builds its enrichment driver in configure().
    let mut tr = TriblerPlugin::new();
    tr.set_seed_config(TriblerConfigFile::from_env());

    let plugins: Vec<Box<dyn FeederPlugin>> = vec![Box::new(tr)];

    info!(target: "meta-feeder", loaded = plugins.len(), "tribler feeder starting");

    serve_feeders(plugins, state_dir, listen).await
}
