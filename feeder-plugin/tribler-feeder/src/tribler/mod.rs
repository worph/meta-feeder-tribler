//! Tribler bridge — a *decentralized* torrent network as a gateway source.
//!
//! Unlike the other plugins (centralized HTTP APIs) and even torznab
//! (centralized indexer sites), Tribler has **no central index**: it runs
//! its own IPv8 overlay search. We don't reimplement that overlay — we run a
//! headless Tribler core as a sidecar container and query its REST API,
//! exactly the way torznab treats an external indexer. The plugin therefore
//! bridges Tribler's *content corpus* (the torrents its peers have indexed)
//! into the meta-share network.
//!
//! ## Tribler REST surface (8.x line, `ghcr.io/tribler/tribler`)
//!
//! - Auth: `X-Api-Key: <key>` header on every call (fixed via `CORE_API_KEY`).
//! - Local search (sync, instant): `GET /api/metadata/search/local?fts_text=<q>`
//!   → `{ "results": [ {infohash, name, size, category, num_seeders, …}, … ] }`.
//! - Remote search (async over IPv8): `PUT /api/search/remote?fts_text=<q>`
//!   → `{ "request_uuid": "…", "peers": [ … ] }`.
//! - Result delivery for the remote search: the long-lived SSE bus
//!   `GET /api/events`. As peers reply, the core emits events with topic
//!   `remote_query_results` carrying `{ results, uuid, peer }`. There is **no**
//!   terminal "done" frame — collection ends on consumer-cancel or when the
//!   result feed goes quiet (treated as exhaustion).
//!
//! ## Outcome model (v1, metadata-only)
//!
//! Like torznab's metadata-only path, `compute_outcomes` turns the torrent
//! infohash directly into a `btih-v1-file` CID (`HashKind::BtV1File`,
//! `bytes: None`) via [`compute_bt_v1_file_cid`]. The gateway *advertises* the
//! record (so the torrent becomes discoverable in meta-share) but does **not**
//! download or seed the bytes. Full-file fetch + seed (reusing torznab's
//! librqbit machinery) is a possible phase 2 — out of scope here.
//!
//! The exact SSE frame encoding of `/api/events` (event-name vs JSON `topic`
//! field, payload nesting) varies across Tribler tags; [`parse_remote_results`]
//! is deliberately tolerant. Confirm the live schema against `/docs` on the
//! running container if remote results don't surface.

pub mod metainfo;

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::channel::mpsc;
use futures::stream::BoxStream;
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tracing::warn;

use meta_feeder_sdk::common;
use self::metainfo::TorrentFile;
use meta_feeder_sdk::cache::MidhashCache;
use meta_feeder_sdk::enrich::{EnrichTarget, EnrichmentConfig, Enricher};
use meta_feeder_sdk::hash::{compute_bt_info_cid, compute_bt_v1_file_cid};
use meta_feeder_sdk::plugin::{
    upstream_id_field, ConfigError, FeederPlugin, GatewayQuery, HashKind, HashOutcome,
};
use meta_feeder_sdk::query::GatewaySearchEvent;
use meta_feeder_sdk::types::{DiscoveryRecord, GatewayError, Hash, PluginHealth};

/// Default sidecar base URL — the `tribler-instance` service on the gateway
/// dev compose's `metamesh-mesh` network. Overridable via the dashboard config
/// form (`sidecar_url`) or the `TRIBLER_SIDECAR_URL` env seed.
const DEFAULT_SIDECAR_URL: &str = "http://tribler-instance:8085";

/// The tribler plugin's editable configuration — the persisted shape under
/// `config.json` and the seed parsed from env. Keys mirror the fields declared
/// in [`TriblerPlugin::config_schema`] exactly.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct TriblerConfigFile {
    /// meta-core root this feeder self-publishes to (records + TMDB posters).
    /// Blank/absent → `META_CORE_URL` env seed, then enrichment+publish soft-skip.
    /// Configured in the dashboard so the modular meta-core seam needs no compose
    /// env.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub meta_core_url: Option<String>,
    /// Tribler core REST base URL. Blank/absent → [`DEFAULT_SIDECAR_URL`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sidecar_url: Option<String>,
    /// Tribler REST `X-Api-Key` (secret). Blank/absent → no auth header.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key: Option<String>,
    /// TMDB API key (v3) / v4 bearer for enrichment (secret). Dashboard-overridable;
    /// blank/absent → falls back to the `TMDB_TOKEN` env seed, then soft-skips.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tmdb_api_key: Option<String>,
    /// TMDB metadata language tag (e.g. `en-US`). Blank/absent → `TMDB_LANGUAGE`
    /// env seed, then the SDK default (`en-US`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tmdb_language: Option<String>,
}

impl TriblerConfigFile {
    // NOTE: `TriblerConfigFile::from_env()` was deliberately removed. Tribler
    // config (sidecar URL, api key, tmdb token/language, meta-core URL) is
    // dashboard config — it lives ONLY in the persisted config.json, never in
    // env. (Was: TRIBLER_SIDECAR_URL / TRIBLER_API_KEY / TMDB_TOKEN /
    // TMDB_LANGUAGE / META_CORE_URL — all now config-schema fields.)
}

/// HTTP timeout for the *synchronous* calls (local search, remote-search
/// kickoff). The SSE bus read is governed by [`SSE_IDLE_EXHAUST`], not this.
const HTTP_TIMEOUT_SECS: u64 = 30;

/// Per-request timeout for the `torrentinfo/uri` metainfo fetch — a DHT resolve
/// that's much slower than a local search, so it gets its own generous ceiling
/// (overrides the client default via a per-request `.timeout()`).
const METAINFO_TIMEOUT_SECS: u64 = 60;

/// Well-known public BitTorrent trackers appended to every synthesized magnet.
/// Tribler-discovered torrents are frequently DHT-only (no usable tracker in the
/// magnet), so meta-share's on-demand fetcher would have to find peers via BT-DHT
/// alone — slow/unreliable in a constrained network even when the swarm is
/// healthy (well-seeded SubsPlease releases that still time out at 60s). The
/// seeders announce to these, so adding them lets the fetcher discover peers
/// fast. `nyaa.tracker.wf` (HTTP, usually reachable when UDP is blocked) covers
/// the common anime (SubsPlease/Erai) releases specifically.
const PUBLIC_TRACKERS: &[&str] = &[
    "http://nyaa.tracker.wf:7777/announce",
    "udp://tracker.opentrackr.org:1337/announce",
    "udp://open.stealth.si:80/announce",
    "udp://open.demonii.com:1337/announce",
    "udp://exodus.desync.com:6969/announce",
    "udp://tracker.torrent.eu.org:451/announce",
    "udp://tracker.openbittorrent.com:6969/announce",
];

/// Cap on per-file fan-out — a pathological torrent with thousands of files
/// won't spawn thousands of enrichment calls. Excess files beyond this are
/// dropped (the pack record still represents the whole torrent).
const MAX_FANOUT_FILES: usize = 100;

const USER_AGENT: &str = concat!(
    "meta-share/",
    env!("CARGO_PKG_VERSION"),
    " (gateway:tribler)"
);

/// mpsc buffer for the streaming-search producer. Matches torznab's value.
const STREAM_BUFFER: usize = 256;

/// Remote-search exhaustion detector. The `/api/events` SSE bus never closes
/// on its own and carries unrelated traffic, so we treat **"no new matching
/// result for this long"** as the feed being exhausted and end the stream.
/// The timer resets on every fresh result, so an actively-replying search
/// stays open indefinitely — this is an idle/quiet detector, not a fixed
/// collection window. Consumer-cancel (dropping the receiver) ends it
/// immediately regardless.
const SSE_IDLE_EXHAUST: Duration = Duration::from_secs(20);

/// One search-result item from Tribler. Both the local-search `results[]` and
/// the `remote_query_results` event `results[]` are `TorrentMetadata`
/// `to_simple_dict()` shapes; we deserialize only the fields we surface and
/// let serde ignore the rest (`type`, `id`, `public_key`, `status`, …).
#[derive(Debug, Clone, Default, Deserialize)]
struct TriblerItem {
    #[serde(default)]
    infohash: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    size: Option<u64>,
    #[serde(default)]
    category: Option<String>,
    #[serde(default)]
    num_seeders: Option<i64>,
    #[serde(default)]
    num_leechers: Option<i64>,
    /// Tracker announce URLs the torrent carries. Folded into the synthesized
    /// magnet (`&tr=`) so a BitTorrent fetcher connects without a DHT-only cold
    /// start. Often empty on DHT-discovered torrents.
    #[serde(default)]
    trackers: Vec<String>,
}

#[derive(Debug, Default, Deserialize)]
struct LocalSearchResponse {
    #[serde(default)]
    results: Vec<TriblerItem>,
}

#[derive(Debug, Default, Deserialize)]
struct RemoteSearchResponse {
    #[serde(default)]
    request_uuid: Option<String>,
}

/// Tribler gateway plugin. Cheap to construct; `configure()` opens the
/// per-plugin redb cache. Cloneable so the streaming producer can own a copy
/// and keep driving after `handle_query_stream` returns (torznab pattern).
#[derive(Clone)]
pub struct TriblerPlugin {
    http: reqwest::Client,
    sidecar_url: String,
    api_key: Option<String>,
    cache: Option<MidhashCache>,
    /// Effective config snapshot, exposed (redacted) through the SDK config
    /// plane for the dashboard's per-plugin config form.
    cfg: TriblerConfigFile,
    /// Self-publish + enrichment driver. `Some` when `META_CORE_URL` is set
    /// (the "smart feeder / dumb gateway" path): the feeder writes its own base
    /// record to meta-core and drives the filename-parser/tmdb plugins. `None`
    /// → legacy path (return the record for the gateway dispatcher to store).
    enricher: Option<Enricher>,
    /// Inline TMDB enrichment for live search hits — the **shared** torrent-core
    /// engine (`crate::enrich`) the torznab source uses. Drives the catalog
    /// `popular:`/`trending:` discovery seeds and stamps poster/overview/tmdbid
    /// onto each live hit as an `EnrichPatch`, so tribler results clear
    /// meta-watch's poster+description gate exactly like torznab's do. Auto-built
    /// (with a TMDB token-budget); the token comes from `tmdb_api_key`.
    inline_tmdb: crate::enrich::Enricher,
}

impl Default for TriblerPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl TriblerPlugin {
    pub fn new() -> Self {
        Self::with_sidecar_url(DEFAULT_SIDECAR_URL.to_string())
    }

    pub fn with_sidecar_url(sidecar_url: String) -> Self {
        let http = common::build_http_client(HTTP_TIMEOUT_SECS, USER_AGENT, None);
        Self {
            http,
            sidecar_url,
            api_key: None,
            cache: None,
            cfg: TriblerConfigFile::default(),
            enricher: None,
            inline_tmdb: crate::enrich::Enricher::new(),
        }
    }

    pub fn set_sidecar_url(&mut self, url: String) {
        self.sidecar_url = url;
    }

    /// Install the self-publish + enrichment driver (built from env in `main`).
    /// When set, `compute_outcomes` writes the base record to meta-core itself
    /// and fans the resolved CID out to the enrichment plugins.
    pub fn set_enricher(&mut self, enricher: Option<Enricher>) {
        self.enricher = enricher;
    }

    pub fn set_api_key(&mut self, key: String) {
        self.api_key = Some(key);
    }

    // set_seed_config was removed: there is no env seed. config.json is the only
    // config source (dashboard-written); apply_config takes it in configure().

    /// Apply an effective config: override sidecar URL / API key when present,
    /// otherwise keep the constructor defaults. Records the effective snapshot
    /// in `self.cfg` for the dashboard config form.
    fn apply_config(&mut self, cfg: TriblerConfigFile) {
        if let Some(url) = cfg.sidecar_url.as_ref() {
            if !url.trim().is_empty() {
                self.sidecar_url = url.clone();
            }
        }
        if let Some(key) = cfg.api_key.as_ref() {
            if !key.trim().is_empty() {
                self.api_key = Some(key.clone());
            }
        }
        // Snapshot the effective values (defaults filled in) for config_values()
        // + for building the enricher in `configure()`. TMDB key/language carry
        // through unchanged (empty → None) so config.json > env-seed precedence
        // holds and the secret is redacted by the SDK config plane.
        self.cfg = TriblerConfigFile {
            meta_core_url: cfg.meta_core_url.filter(|s| !s.trim().is_empty()),
            sidecar_url: Some(self.sidecar_url.clone()),
            api_key: self.api_key.clone(),
            tmdb_api_key: cfg.tmdb_api_key.filter(|s| !s.trim().is_empty()),
            tmdb_language: cfg.tmdb_language.filter(|s| !s.trim().is_empty()),
        };
    }

    fn cache(&self) -> Result<&MidhashCache, GatewayError> {
        common::require_cache(self.cache.as_ref(), "tribler")
    }

    fn base(&self) -> &str {
        self.sidecar_url.trim_end_matches('/')
    }

    /// Attach the `X-Api-Key` header when an api key is configured.
    fn auth(&self, rb: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.api_key {
            Some(k) => rb.header("X-Api-Key", k),
            None => rb,
        }
    }

    /// Synchronous local-DB search — instant hits from this node's own
    /// metadata store (which also accumulates results from prior remote
    /// searches the core ingested).
    async fn search_local(
        &self,
        query: &str,
        max_results: usize,
    ) -> Result<Vec<DiscoveryRecord>, GatewayError> {
        let url = format!(
            "{}/api/metadata/search/local?fts_text={}&first=1&last={}",
            self.base(),
            common::urlencode(query),
            max_results
        );
        let resp =
            self.auth(self.http.get(&url)).send().await.map_err(|e| {
                GatewayError::Transient(format!("tribler local search GET {url}: {e}"))
            })?;
        common::map_status(&resp)?;
        let body: LocalSearchResponse = resp
            .json()
            .await
            .map_err(|e| GatewayError::Permanent(format!("parse tribler local search: {e}")))?;
        Ok(body
            .results
            .into_iter()
            .filter_map(into_discovery_record)
            .collect())
    }

    /// Kick off a network-wide (IPv8 overlay) search. Returns the
    /// `request_uuid` used to correlate `remote_query_results` SSE frames.
    async fn start_remote_search(&self, query: &str) -> Result<Option<String>, GatewayError> {
        let url = format!(
            "{}/api/search/remote?fts_text={}",
            self.base(),
            common::urlencode(query)
        );
        // NB: the route is PUT, not POST — see the Tribler maintainer's note
        // (github discussion #6622). Semantically odd, but it's the API.
        let resp = self.auth(self.http.put(&url)).send().await.map_err(|e| {
            GatewayError::Transient(format!("tribler remote search PUT {url}: {e}"))
        })?;
        common::map_status(&resp)?;
        let body: RemoteSearchResponse = resp
            .json()
            .await
            .map_err(|e| GatewayError::Permanent(format!("parse tribler remote search: {e}")))?;
        Ok(body.request_uuid)
    }

    /// Best-effort: store a record's fields keyed by infohash so a later
    /// `compute_outcomes` (which only receives the infohash) can rebuild the
    /// full metadata-only record. Mirrors torznab's `bibrec` use.
    fn cache_record(&self, rec: &DiscoveryRecord) {
        if let Some(cache) = self.cache.as_ref() {
            if let Err(e) = cache.put_bibrec(&rec.record_id, &rec.fields) {
                warn!(
                    target: "meta-share::gateway",
                    upstream_id = "tribler",
                    record_id = %rec.record_id,
                    error = %e,
                    "tribler bibrec cache put failed (non-fatal)"
                );
            }
        }
    }

    /// Fetch + cache the torrent's file list via Tribler
    /// `POST /api/torrentinfo/uri` (body `{"uri": magnet}` — the query param
    /// alone 500s). Cached in the SDK `filelist` table keyed by infohash so the
    /// slow DHT metainfo resolve happens once. Returns `None` on any failure —
    /// the caller then falls back to the single-file record (a fan-out failure
    /// must never fail the resolve).
    async fn fetch_filelist(
        &self,
        infohash: &str,
        magnet: &str,
        timeout_secs: u64,
    ) -> Option<Vec<TorrentFile>> {
        let cache = self.cache.as_ref()?;
        if let Ok(Some(json)) = cache.get_filelist(infohash) {
            if let Ok(files) = serde_json::from_str::<Vec<TorrentFile>>(&json) {
                return Some(files);
            }
        }
        let url = format!("{}/api/torrentinfo/uri", self.base());
        let body = serde_json::json!({ "uri": magnet });
        // Two attempts: the sidecar drops idle keep-alive sockets between the
        // search calls and this torrentinfo POST, so a pooled connection picked
        // up mid-reset surfaces as a one-off "error sending request" (the same
        // class of flake torznab fixed against Prowlarr). Retry once.
        let mut resp = None;
        let mut last_err = None;
        for attempt in 0..2u8 {
            match self
                .auth(self.http.post(&url))
                .timeout(Duration::from_secs(timeout_secs))
                .json(&body)
                .send()
                .await
            {
                Ok(r) => {
                    resp = Some(r);
                    break;
                }
                Err(e) => {
                    last_err = Some(e);
                    if attempt == 0 {
                        tokio::time::sleep(Duration::from_millis(300)).await;
                    }
                }
            }
        }
        let resp = match resp {
            Some(r) => r,
            None => {
                warn!(
                    target: "meta-share::gateway", upstream_id = "tribler",
                    record_id = %infohash, error = ?last_err,
                    "tribler torrentinfo fetch failed (after retry); falling back to single-file"
                );
                return None;
            }
        };
        if common::map_status(&resp).is_err() {
            return None;
        }
        let text = resp.text().await.ok()?;
        let files = metainfo::parse_metainfo(&text);
        if files.is_empty() {
            return None;
        }
        if let Ok(json) = serde_json::to_string(&files) {
            let _ = cache.put_filelist(infohash, &json);
        }
        Some(files)
    }

    /// Per-CID idempotency guard (PR2 step 6). Returns `true` and marks `cid`
    /// resolved if it was not already; `false` if a prior resolve already wrote
    /// it. Keyed **per-CID** (not per-infohash) so one infohash yielding N file
    /// CIDs + a pack CID doesn't get wrongly short-circuited after the first.
    fn mark_fresh(&self, cache: &MidhashCache, cid: &str) -> bool {
        match cache.get_midhash(cid) {
            Ok(Some(_)) => false,
            _ => {
                let _ = cache.put_midhash(cid, "1");
                true
            }
        }
    }

    /// Self-publish one base record to meta-core; log + swallow on failure (the
    /// CID is still returned on the wire, so a write failure degrades to "not
    /// enriched", never "resolve failed").
    async fn publish(&self, enricher: &Enricher, cid: &str, rec: &DiscoveryRecord) {
        if let Err(e) = enricher.write_base_record(cid, rec).await {
            warn!(
                target: "meta-share::gateway", upstream_id = "tribler",
                cid, error = %e, "tribler base record write failed (non-fatal)"
            );
        }
    }

    /// Legacy path (no `META_CORE_URL`): the gateway dispatcher stores returned
    /// records. One CID per infohash, so the per-infohash `get_midhash` guard is
    /// correct. Unchanged from the pre-D4 behaviour.
    fn legacy_single_outcome(
        &self,
        cache: &MidhashCache,
        record_id: &str,
        infohash_20: &[u8; 20],
    ) -> Result<Vec<HashOutcome>, GatewayError> {
        let cid = compute_bt_v1_file_cid(infohash_20, 0);
        let already = cache
            .get_midhash(record_id)
            .map_err(|e| GatewayError::Internal(anyhow::anyhow!("cache get: {e}")))?
            .is_some();
        if already {
            return Ok(vec![sparse_outcome(&cid)]);
        }
        common::store_midhash(cache, record_id, "tribler", &cid);
        let fields = cache
            .get_bibrec(record_id)
            .map_err(|e| GatewayError::Internal(anyhow::anyhow!("bibrec get: {e}")))?
            .unwrap_or_else(|| minimal_fields(record_id));
        Ok(vec![HashOutcome {
            hash: Hash(cid),
            hash_kind: HashKind::BtV1File,
            bytes: None,
            record: Some(DiscoveryRecord {
                upstream_id: "tribler".to_string(),
                record_id: record_id.to_string(),
                fields,
            }),
            file_extension: None,
        }])
    }

    /// Send one live search hit downstream: the `Base` record followed by the
    /// shared torrent-core TMDB enrichment events (`EnrichPatch`/`Drop` + poster),
    /// exactly like the torznab source. This is what stamps poster/overview/tmdbid
    /// onto a tribler hit so it clears meta-watch's gate. Returns `false` if the
    /// consumer hung up (caller should stop).
    async fn send_hit(
        &self,
        rec: DiscoveryRecord,
        tx: &mut mpsc::Sender<GatewaySearchEvent>,
    ) -> bool {
        // Inline TMDB enrichment no-ops without a `contentKind` (it keys
        // movie-vs-tv off it). Tribler live hits don't carry one —
        // filename-parser stamps it post-resolve — so infer it cheaply from the
        // title here. The enricher's movie↔tv kind-fallback corrects a wrong
        // guess, and `build_enrichment_patch` reconciles `contentKind` from the
        // matched TMDB entry, so this is just a search-kind seed.
        let mut rec = rec;
        // Parse season/episode from THIS file's name (preferred — a pack's
        // per-file name carries the real SxxExx) else the torrent title.
        let parse_src = rec
            .fields
            .get("fileName")
            .or_else(|| rec.fields.get("title"))
            .cloned()
            .unwrap_or_default();
        let se = crate::filename_meta::extract_season_episode(&parse_src);
        if !rec.fields.contains_key("contentKind") {
            let kind = if se.season.is_some() || se.episode.is_some() {
                "episode"
            } else {
                "movie"
            };
            rec.fields.insert("contentKind".to_string(), kind.to_string());
        }
        // For episode records, stamp the *parsed* season/episode so the consumer
        // renders a proper series with an ordered episode list (meta-watch keys
        // `is_series` on a season/episode field). Only genuinely-parsed values are
        // stamped — an absolute-numbered "… - 28" sets episode with no season, so
        // the enricher's season-bounds drop can't fire on it. The filename-parser
        // plugin re-derives both authoritatively at resolve time.
        if rec.fields.get("contentKind").map(|k| k == "episode") == Some(true) {
            if let Some(ep) = se.episode.clone() {
                rec.fields.entry("episode".to_string()).or_insert(ep);
            }
            if let Some(s) = se.season.clone() {
                rec.fields.entry("season".to_string()).or_insert(s);
            }
        }
        self.cache_record(&rec);
        if tx
            .send(GatewaySearchEvent::Base(rec.clone()))
            .await
            .is_err()
        {
            return false;
        }
        let events = crate::enrich::enrich_one_streaming(
            rec,
            self.inline_tmdb.tmdb_enricher(),
            self.cache.clone(),
        )
        .await;
        for ev in events {
            if tx.send(ev).await.is_err() {
                return false;
            }
        }
        true
    }

    /// "Unpack" a pack torrent at search time. If the hit is a multi-video
    /// torrent (season pack / batch), probe its file list (bounded + cached) and
    /// return one record **per video file** — each carrying its own
    /// `cid_btih_v1_file` so the consumer renders distinct episodes — instead of
    /// a single, mis-labelled pack record. A single-video torrent → one record
    /// for that file; a non-video pack → no records (dropped). Obvious single
    /// files and any probe failure short-circuit to the original record, so the
    /// metainfo probe runs only for ambiguous packs. Mirrors torznab's per-file
    /// expansion (`crate::bt::build_file_record` sets the same key).
    async fn expand_hit(&self, rec: DiscoveryRecord) -> Vec<DiscoveryRecord> {
        // Clearly-non-video torrents (the `.zip` fan-art etc.) are dropped here
        // without a probe — cheap, and the same verdict record_matches would give.
        if matches!(
            rec.fields.get("fileType").map(String::as_str),
            Some("archive") | Some("audio") | Some("document") | Some("image")
        ) {
            return Vec::new();
        }
        let title = rec
            .fields
            .get("title")
            .or_else(|| rec.fields.get("fileName"))
            .cloned()
            .unwrap_or_default();
        // Every emitted video record needs a parseable `btih-v1-file` content CID
        // (compute_bt_v1_file_cid) so meta-share's BT fetch tier can stream it — a
        // raw infohash is not a valid CID and `/direct` rejects it ("parse
        // multihash"). Build it from the decoded infohash; without a decodable one
        // we can't make the record playable, so pass it through untouched.
        let Some(ih20) = common::decode_infohash(&rec.record_id)
            .and_then(|v| <[u8; 20]>::try_from(v.as_slice()).ok())
        else {
            return vec![rec];
        };
        let with_cid = move |mut r: DiscoveryRecord, idx: u64| -> DiscoveryRecord {
            r.fields
                .entry("cid_btih_v1_file".to_string())
                .or_insert_with(|| compute_bt_v1_file_cid(&ih20, idx));
            r
        };
        if is_obvious_single_file(&title) {
            // Single-file torrent → its only file is index 0.
            return vec![with_cid(rec, 0)];
        }
        let magnet = rec
            .fields
            .get("sourceUrl")
            .cloned()
            .unwrap_or_else(|| format!("magnet:?xt=urn:btih:{}", rec.record_id));
        let Some(files) = self
            .fetch_filelist(&rec.record_id, &magnet, SEARCH_METAINFO_PROBE_SECS)
            .await
        else {
            // Probe failed/timeout → single index-0 fallback (still playable).
            return vec![with_cid(rec, 0)];
        };
        let videos: Vec<&TorrentFile> = files
            .iter()
            .filter(|f| metainfo::looks_like_video(&f.name))
            .take(MAX_FANOUT_FILES)
            .collect();
        let per_file = |f: &TorrentFile, force_episode: bool| {
            let mut r = build_file_record(&rec.fields, &rec.record_id, f);
            r.fields.insert(
                "cid_btih_v1_file".to_string(),
                compute_bt_v1_file_cid(&ih20, f.index as u64),
            );
            if force_episode {
                // A multi-video pack is a TV season — force the kind so enrichment
                // resolves the show as TV (the show title drives the TMDB search).
                r.fields
                    .insert("contentKind".to_string(), "episode".to_string());
            }
            r
        };
        match videos.len() {
            0 => Vec::new(),
            1 => vec![per_file(videos[0], false)],
            _ => videos.iter().map(|f| per_file(f, true)).collect(),
        }
    }

    /// Expand a hit (pack-unpack when the query asks for video) and stream the
    /// resulting records: structured-filter each, dedup per-stream, `send_hit`.
    /// Returns `false` if the consumer hung up.
    async fn emit_hit(
        &self,
        rec: DiscoveryRecord,
        seen: &mut BTreeSet<String>,
        total: &mut usize,
        max_results: usize,
        tx: &mut mpsc::Sender<GatewaySearchEvent>,
        query: &GatewayQuery,
    ) -> bool {
        let wants_video = query
            .filters
            .get("fileType")
            .map(|v| v.iter().any(|x| x.eq_ignore_ascii_case("video")))
            .unwrap_or(false);
        let records = if wants_video {
            self.expand_hit(rec).await
        } else {
            vec![rec]
        };
        for mut r in records {
            if *total >= max_results {
                return true;
            }
            // Stamp languages BEFORE the structured filter: meta-watch's search
            // appends the viewer's `languages/<iso3>` preference, and a record with
            // no language field is dropped. Tribler live hits carry none, so a
            // language-tagged release (e.g. "[English Dub]"/"VOSTFR") must get its
            // `languages/<iso3>` key-set member here or `record_matches` would drop
            // it. (filename-parser refines these on the resolved record.)
            let src = r
                .fields
                .get("fileName")
                .or_else(|| r.fields.get("title"))
                .cloned()
                .unwrap_or_default();
            for lang in crate::filename_meta::extract_languages(&src) {
                r.fields
                    .entry(format!("languages/{lang}"))
                    .or_insert_with(|| "true".to_string());
            }
            if !meta_feeder_sdk::query_eval::record_matches(&r.fields, query) {
                continue;
            }
            if !seen.insert(stream_key(&r)) {
                continue;
            }
            if !self.send_hit(r, tx).await {
                return false;
            }
            *total += 1;
        }
        true
    }

    /// Drain the `/api/events` SSE bus, forwarding each fresh
    /// `remote_query_results` item matching `want_uuid` as a `Base` event.
    /// Ends on consumer-cancel (send fails), result cap, transport end, or
    /// [`SSE_IDLE_EXHAUST`] of quiet (feed exhausted). See module docs for the
    /// tolerant frame parsing.
    async fn collect_remote(
        &self,
        want_uuid: &str,
        seen: &mut BTreeSet<String>,
        total: &mut usize,
        max_results: usize,
        tx: &mut mpsc::Sender<GatewaySearchEvent>,
        query: &GatewayQuery,
    ) {
        let url = format!("{}/api/events", self.base());
        let resp = match self.auth(self.http.get(&url)).send().await {
            Ok(r) => r,
            Err(e) => {
                warn!(
                    target: "meta-share::gateway",
                    upstream_id = "tribler",
                    error = %e,
                    "tribler /api/events open failed; remote results unavailable"
                );
                return;
            }
        };
        if common::map_status(&resp).is_err() {
            return;
        }
        let mut stream = resp.bytes_stream();
        let mut buf = String::new();
        let mut last_result = Instant::now();
        loop {
            let idle = last_result.elapsed();
            if idle >= SSE_IDLE_EXHAUST {
                break; // feed quiet → exhausted
            }
            let remaining = SSE_IDLE_EXHAUST - idle;
            let chunk = match tokio::time::timeout(remaining, stream.next()).await {
                Err(_) => break,                      // idle window elapsed
                Ok(None) | Ok(Some(Err(_))) => break, // stream ended / transport error
                Ok(Some(Ok(bytes))) => bytes,
            };
            buf.push_str(&String::from_utf8_lossy(&chunk));
            let mut got_fresh = false;
            // SSE events are terminated by a blank line (\n\n).
            while let Some(idx) = buf.find("\n\n") {
                let frame: String = buf.drain(..idx + 2).collect();
                for item in parse_remote_results(&frame, want_uuid) {
                    let Some(rec) = into_discovery_record(item) else {
                        continue;
                    };
                    // Any new result keeps the SSE feed alive (Tribler often
                    // streams junk before the well-seeded hits); emit_hit
                    // structured-filters (e.g. `fileType:video`), pack-unpacks,
                    // and dedups per stream, so archives/noise never reach
                    // meta-watch and a season pack fans out to its episodes.
                    got_fresh = true;
                    if !self
                        .emit_hit(rec, seen, total, max_results, tx, query)
                        .await
                    {
                        return; // consumer cancelled
                    }
                    if *total >= max_results {
                        return;
                    }
                }
            }
            if got_fresh {
                last_result = Instant::now();
            }
        }
    }
}

#[async_trait]
impl FeederPlugin for TriblerPlugin {
    fn upstream_id(&self) -> &'static str {
        "tribler"
    }

    fn configure(&mut self, cache_dir: &Path) -> Result<(), ConfigError> {
        // Config precedence: a persisted `config.json` (written by the SDK config
        // plane when the operator saves through the dashboard) wins over the env
        // seed. Both are optional — tribler has working defaults, so a fully
        // unconfigured feeder still serves (DEFAULT_SIDECAR_URL, no auth key).
        // config.json (dashboard-written) is the ONLY config source — no env
        // seed. Absent → tribler's built-in defaults (DEFAULT_SIDECAR_URL, no key).
        let effective = std::fs::read(cache_dir.join("config.json"))
            .ok()
            .and_then(|b| serde_json::from_slice::<TriblerConfigFile>(&b).ok())
            .unwrap_or_default();
        self.apply_config(effective);
        self.cache = Some(common::open_midhash_cache(cache_dir, "tribler")?);
        // Build the enrichment driver: infra (META_CORE_URL + plugin URLs + peer
        // + callback) from env, with the TMDB key + language overridden by the
        // effective dashboard config (config.json > env seed). Rebuilt on every
        // configure(), so a dashboard save + feeder restart re-reads it.
        self.enricher = build_enricher(&self.http, &self.cfg);
        // Inline TMDB enrichment (shared torrent-core engine): token from the
        // effective config (`tmdb_api_key`), cache shared. Drives catalog
        // discovery seeds + the poster/overview/tmdbid `EnrichPatch` on live
        // hits so tribler results pass meta-watch's poster+description gate.
        if let Some(t) = self.cfg.tmdb_api_key.as_ref().filter(|s| !s.trim().is_empty()) {
            self.inline_tmdb.set_tmdb_token(t.clone());
        }
        self.inline_tmdb.cache = self.cache.clone();
        Ok(())
    }

    /// Non-streaming fallback: local-DB hits only (instant, synchronous). The
    /// remote IPv8 search lives in [`handle_query_stream`], which is what the
    /// dispatcher drives.
    async fn handle_query(
        &self,
        query: &GatewayQuery,
        max_results: usize,
    ) -> Result<Vec<DiscoveryRecord>, GatewayError> {
        if !meta_feeder_sdk::query_eval::query_accepts_plugin(
            query,
            self.served_file_types(),
            self.served_content_kinds(),
        ) {
            return Ok(Vec::new());
        }
        let records = self
            .search_local(query.free_text_or_star(), max_results)
            .await?;
        for rec in &records {
            self.cache_record(rec);
        }
        Ok(records)
    }

    async fn handle_query_stream(
        &self,
        query: &GatewayQuery,
        max_results: usize,
    ) -> Result<BoxStream<'static, GatewaySearchEvent>, GatewayError> {
        // Catalog discovery (`popular:`/`trending:`/`top_rated:`) — answer from
        // TMDB via the shared torrent-core discovery branch, exactly like the
        // torznab source. This is what fills meta-watch's home rows from the
        // tribler gateway (Tribler has no "what's popular" catalog of its own).
        if crate::discovery::is_discovery_query(query) {
            let (mut tx, rx) = mpsc::channel::<GatewaySearchEvent>(STREAM_BUFFER);
            match self.inline_tmdb.tmdb.clone() {
                Some(client) => {
                    let budget = self.inline_tmdb.tmdb_budget.clone();
                    tokio::spawn(crate::discovery::discover_stream(
                        client,
                        budget,
                        query.clone(),
                        max_results,
                        tx,
                    ));
                }
                // TMDB not configured — emit an empty (Done-only) stream.
                None => {
                    tokio::spawn(async move {
                        let _ = tx.send(GatewaySearchEvent::Done).await;
                    });
                }
            }
            return Ok(rx.boxed());
        }

        // Layer A: skip entirely if the query's fileType/contentKind filters
        // can't match what tribler serves — no point opening the SSE bus.
        if !meta_feeder_sdk::query_eval::query_accepts_plugin(
            query,
            self.served_file_types(),
            self.served_content_kinds(),
        ) {
            return Ok(Box::pin(futures::stream::once(async {
                GatewaySearchEvent::Done
            })));
        }
        let (tx, rx) = mpsc::channel::<GatewaySearchEvent>(STREAM_BUFFER);
        let plugin = self.clone();
        let query = query.clone();
        tokio::spawn(produce_search_events(plugin, query, max_results, tx));
        Ok(rx.boxed())
    }

    async fn compute_outcomes(&self, record_id: &str) -> Result<Vec<HashOutcome>, GatewayError> {
        let cache = self.cache()?;

        // record_id is the BitTorrent v1 infohash.
        let infohash_20 = common::decode_infohash(record_id).ok_or_else(|| {
            GatewayError::Permanent(format!(
                "tribler record_id `{record_id}` is not a v1 BT infohash \
                 (expected 40 hex chars or 32 base32 chars)"
            ))
        })?;

        // No enricher (META_CORE_URL unset) → legacy single-outcome path: emit
        // the record for the gateway dispatcher to store (fresh) or a sparse
        // outcome (re-resolve). Per-infohash guard is correct here (one CID).
        let Some(enricher) = self.enricher.clone() else {
            return self.legacy_single_outcome(cache, record_id, &infohash_20);
        };

        // Smart-feeder path (D4) + metainfo fan-out (PR2 step 3). Pull the
        // record cached at search time for the torrent-level fields + magnet.
        let bibrec = cache.get_bibrec(record_id).ok().flatten();
        let torrent_name = bibrec
            .as_ref()
            .and_then(|f| f.get("title"))
            .cloned()
            .unwrap_or_else(|| record_id.to_string());
        let magnet = bibrec
            .as_ref()
            .and_then(|f| f.get("sourceUrl"))
            .cloned()
            .unwrap_or_else(|| format!("magnet:?xt=urn:btih:{record_id}"));
        let bibrec = bibrec.unwrap_or_else(|| minimal_fields(record_id));

        // Resolve the file list (DHT metainfo), filter to video. Empty on
        // fetch failure / no metadata / no video files → single-file fallback.
        let mut video_files: Vec<TorrentFile> = self
            .fetch_filelist(record_id, &magnet, METAINFO_TIMEOUT_SECS)
            .await
            .map(|fs| {
                fs.into_iter()
                    .filter(|f| metainfo::looks_like_video(&f.name))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        video_files.truncate(MAX_FANOUT_FILES);

        let mut outcomes: Vec<HashOutcome> = Vec::new();
        let mut video_targets: Vec<EnrichTarget> = Vec::new();
        let mut pack_target: Option<EnrichTarget> = None;

        if video_files.len() >= 2 {
            // FAN-OUT: one btih-v1-file record per video file + one pack record.
            for f in &video_files {
                let cid = compute_bt_v1_file_cid(&infohash_20, f.index as u64);
                outcomes.push(sparse_outcome(&cid));
                if self.mark_fresh(cache, &cid) {
                    let rec = build_file_record(&bibrec, record_id, f);
                    self.publish(&enricher, &cid, &rec).await;
                    video_targets.push(EnrichTarget {
                        cid,
                        file_name: f.name.clone(),
                        base: rec.fields,
                    });
                }
            }
            // Whole-torrent pack record (compute_bt_info_cid): filename meta
            // only, NO tmdb (non-playable; raw search finds it by name — D1).
            let pack_cid = compute_bt_info_cid(&infohash_20);
            outcomes.push(sparse_outcome(&pack_cid));
            if self.mark_fresh(cache, &pack_cid) {
                let rec = build_pack_record(&bibrec, record_id);
                self.publish(&enricher, &pack_cid, &rec).await;
                pack_target = Some(EnrichTarget {
                    cid: pack_cid,
                    file_name: torrent_name.clone(),
                    base: rec.fields,
                });
            }
        } else {
            // SINGLE-FILE: the lone video file (at its real index) when metainfo
            // resolved, else index 0 with the torrent-level record (legacy
            // fallback when the DHT fetch failed).
            let (file_index, file_name, rec) = match video_files.first() {
                Some(f) => (f.index, f.name.clone(), build_file_record(&bibrec, record_id, f)),
                None => (
                    0,
                    torrent_name.clone(),
                    DiscoveryRecord {
                        upstream_id: "tribler".to_string(),
                        record_id: record_id.to_string(),
                        fields: bibrec.clone(),
                    },
                ),
            };
            let cid = compute_bt_v1_file_cid(&infohash_20, file_index as u64);
            outcomes.push(sparse_outcome(&cid));
            if self.mark_fresh(cache, &cid) {
                self.publish(&enricher, &cid, &rec).await;
                if rec.fields.get("fileType").map(|t| t == "video").unwrap_or(false) {
                    video_targets.push(EnrichTarget {
                        cid,
                        file_name,
                        base: rec.fields,
                    });
                }
            }
        }

        // Drive enrichment off the resolve path. The ingester propagates the
        // merged records on its next poll (eventually consistent).
        if enricher.has_plugins() && (!video_targets.is_empty() || pack_target.is_some()) {
            let enricher = enricher.clone();
            tokio::spawn(async move {
                enricher.enrich_bundle(video_targets, pack_target).await;
            });
        }

        Ok(outcomes)
    }

    fn health(&self) -> PluginHealth {
        if self.cache.is_some() {
            PluginHealth::Ok
        } else {
            PluginHealth::Degraded {
                reason: "configure() not yet called".to_string(),
            }
        }
    }

    fn served_file_types(&self) -> &'static [&'static str] {
        // Tribler is a decentralized *free-text* overlay search with no inherent
        // content-type limitation — a query for anything can return anything (a
        // video, an album, an ebook, a rar). We advertise the `"*"` wildcard so
        // meta-share routes EVERY typed query here (it honors `"*"` as
        // match-any); per-record `fileType` is then stamped from the actual
        // torrent, and consumers (meta-watch) filter to what they can use.
        &["*"]
    }

    fn served_content_kinds(&self) -> &'static [&'static str] {
        &["*"]
    }

    fn config_schema(&self) -> meta_feeder_sdk::config::ConfigSchema {
        use meta_feeder_sdk::config::{ConfigField as F, ConfigSchema};
        ConfigSchema {
            fields: vec![
                F::text("meta_core_url", "meta-core URL").with_help(
                    "Storage backend this feeder publishes resolved records + TMDB \
                     posters to (e.g. http://metacore-app:9000). Required — blank \
                     means the feeder can't publish and soft-skips. Takes effect on \
                     the next feeder restart.",
                ),
                F::text("sidecar_url", "Tribler core URL").with_help(
                    "REST base URL of the headless Tribler core (e.g. \
                     http://tribler-instance:8085). Blank uses the built-in default. \
                     To open the Tribler web UI, use the \"\u{2197} Tribler UI\" link \
                     on this plugin's card in the gateway dashboard — it opens the \
                     gated /ui/ behind your single sign-on. Note: the `?key=` must come \
                     AFTER the `#` (the UI is a HashRouter; a key before the `#` is \
                     ignored and you get \"Failed to connect\"), and it must match the \
                     Core API key below.",
                ),
                F::secret("api_key", "Core API key").with_help(
                    "Tribler REST X-Api-Key. Matches the core's configured key \
                     (the dev sidecar uses `changeme`). Blank → no auth header.",
                ),
                F::secret("tmdb_api_key", "TMDB API key / v4 token").with_help(
                    "TMDB v3 API key or v4 bearer token, used to enrich resolved \
                     torrents with poster / overview / tmdbid. Pushed to the tmdb \
                     plugin at enrich time. Blank → falls back to the TMDB_TOKEN env \
                     seed, then TMDB enrichment soft-skips. Takes effect on the next \
                     feeder restart (no hot reload).",
                ),
                F::text("tmdb_language", "TMDB language").with_help(
                    "TMDB metadata language tag — e.g. en-US, fr-FR, es-ES, ja-JP. \
                     Blank → TMDB_LANGUAGE env seed, then the default en-US. Takes \
                     effect on the next feeder restart.",
                ),
            ],
        }
    }

    fn config_values(&self) -> serde_json::Value {
        serde_json::to_value(&self.cfg).unwrap_or_else(|_| serde_json::json!({}))
    }
}

/// Floor on the local-DB candidate window pulled before structured-filtering +
/// health-ranking, so a small `max_results` still gives the filter/sort enough
/// material to surface real video over archive/noise.
const LOCAL_CANDIDATE_FLOOR: usize = 100;

/// Seeder count for health ranking (0 when absent / unparseable).
fn seeders_of(rec: &DiscoveryRecord) -> i64 {
    rec.fields
        .get("seeders")
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(0)
}

/// Bounded metainfo probe budget for **search-time** pack expansion. Short
/// (unlike the 60s resolve-time fetch) so an unresolvable pack falls back to a
/// single record fast; the file list is cached, so the cost is paid once per
/// torrent.
const SEARCH_METAINFO_PROBE_SECS: u64 = 8;

/// True for hits that are clearly a single file and so don't warrant a (costly)
/// metainfo probe: a specific `SxxExx` episode, or a name ending in a video
/// container extension. Everything else (season packs, batches, bare-name
/// folders) is ambiguous and gets probed.
fn is_obvious_single_file(title: &str) -> bool {
    if crate::filename_meta::extract_season_episode(title)
        .episode
        .is_some()
    {
        return true;
    }
    let t = title.to_ascii_lowercase();
    [".mkv", ".mp4", ".avi", ".mov", ".m4v", ".webm", ".ts", ".flv"]
        .iter()
        .any(|e| t.ends_with(e))
}

/// Per-stream dedup key: the per-file `cid_btih_v1_file` when present (so the
/// individual episodes of an unpacked pack stay distinct), else the torrent's
/// `record_id`.
fn stream_key(rec: &DiscoveryRecord) -> String {
    rec.fields
        .get("cid_btih_v1_file")
        .cloned()
        .unwrap_or_else(|| rec.record_id.clone())
}

/// The streaming-search producer driven by [`TriblerPlugin::handle_query_stream`].
/// Emits local-DB hits as `Base` immediately, fires the remote IPv8 search,
/// then forwards remote replies as they arrive over the SSE bus — until the
/// consumer cancels, the result cap is hit, or the feed is exhausted. Always
/// ends with a single `Done`. Runs in a spawned task owning a cloned plugin.
async fn produce_search_events(
    plugin: TriblerPlugin,
    query: GatewayQuery,
    max_results: usize,
    mut tx: mpsc::Sender<GatewaySearchEvent>,
) {
    let q = query.free_text_or_star().to_string();
    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut total: usize = 0;

    // 1. Local hits first (instant). Fetch a generous candidate window so the
    // structured-filter pass and the health sort below have material to work
    // with — a cold/junk-heavy local DB otherwise lets archives crowd out the
    // real video torrents (the exact reason a first-time "Frieren" search came
    // back empty while the Tribler UI, which sorts by health, found episodes).
    let local_window = max_results.saturating_mul(5).max(LOCAL_CANDIDATE_FLOOR);
    match plugin.search_local(&q, local_window).await {
        Ok(mut records) => {
            // Rank by health (seeders desc) so the best-seeded torrents are
            // expanded + emitted first, mirroring the Tribler UI's "sort by
            // health". Structured-filtering (`fileType:video` vs a `.zip`) and
            // pack-unpacking happen per-record inside emit_hit — after the
            // metainfo probe reveals each torrent's real per-file content, so a
            // bare-named video pack isn't dropped on a name-only misclassification.
            records.sort_by_key(|r| std::cmp::Reverse(seeders_of(r)));
            for rec in records {
                if total >= max_results {
                    break;
                }
                if !plugin
                    .emit_hit(rec, &mut seen, &mut total, max_results, &mut tx, &query)
                    .await
                {
                    return; // consumer cancelled
                }
            }
        }
        Err(e) => warn!(
            target: "meta-share::gateway",
            upstream_id = "tribler",
            error = %e,
            "tribler local search failed; continuing to remote"
        ),
    }

    // 2. Remote (network-wide) search, collected over the SSE bus.
    if total < max_results {
        match plugin.start_remote_search(&q).await {
            Ok(uuid) => {
                let want = uuid.unwrap_or_default();
                plugin
                    .collect_remote(&want, &mut seen, &mut total, max_results, &mut tx, &query)
                    .await;
            }
            Err(e) => warn!(
                target: "meta-share::gateway",
                upstream_id = "tribler",
                error = %e,
                "tribler remote search kickoff failed; returning local results only"
            ),
        }
    }

    let _ = tx.send(GatewaySearchEvent::Done).await;
}

/// Parse one SSE event frame, returning the Tribler items it carries **iff**
/// it's a `remote_query_results` event matching `want_uuid`.
///
/// Tolerant by design — the `/api/events` frame shape varies across Tribler
/// tags. Handles both a flat `{topic|type|name, results, uuid}` and a nested
/// `{topic, event:{results, uuid}}`. When `want_uuid` is empty (the remote
/// search returned no uuid) the uuid filter is skipped.
fn parse_remote_results(frame: &str, want_uuid: &str) -> Vec<TriblerItem> {
    // Reassemble the SSE `data:` payload (may span multiple `data:` lines).
    let data: String = frame
        .lines()
        .filter_map(|l| l.strip_prefix("data:").map(str::trim_start))
        .collect::<Vec<_>>()
        .join("\n");
    if data.trim().is_empty() {
        return Vec::new();
    }
    let Ok(v) = serde_json::from_str::<serde_json::Value>(&data) else {
        return Vec::new();
    };

    let topic = v
        .get("topic")
        .or_else(|| v.get("type"))
        .or_else(|| v.get("name"))
        .and_then(|t| t.as_str());
    // Payload may be nested under "event" or flat on the root object.
    let payload = v.get("event").unwrap_or(&v);
    let results = payload.get("results").or_else(|| v.get("results"));
    let uuid = payload
        .get("uuid")
        .or_else(|| v.get("uuid"))
        .and_then(|u| u.as_str());

    // Accept iff this looks like a remote-results event: either the topic
    // says so, or (topic absent) there's a results array to read.
    let is_results_event = topic == Some("remote_query_results")
        || (topic.is_none() && results.is_some())
        || (topic == Some("results") && results.is_some());
    if !is_results_event {
        return Vec::new();
    }
    // Correlate to our search when both sides carry a uuid.
    if !want_uuid.is_empty() && uuid.is_some_and(|u| u != want_uuid) {
        return Vec::new();
    }
    let Some(arr) = results.and_then(|r| r.as_array()) else {
        return Vec::new();
    };
    arr.iter()
        .filter_map(|item| serde_json::from_value::<TriblerItem>(item.clone()).ok())
        .collect()
}

/// Classify a torrent into a canonical `fileType` bucket (METADATA_KEYS.md
/// `fileType` vocabulary: `video`, `audio`, `image`, `document`, `archive`,
/// `other`). Tribler's own `category` label is the strongest signal when
/// present; otherwise we sniff the release name. Best-effort — the dispatcher's
/// `record_matches` and the consumer (meta-watch) do the authoritative
/// filtering, so a misclassification at worst mis-files a metasearch hit, it
/// never fabricates a fake video.
fn classify_file_type(name: &str, category: Option<&str>) -> &'static str {
    if let Some(cat) = category {
        match cat.trim().to_ascii_lowercase().as_str() {
            // `xxx` is video content; bucket it as such (adult filtering is a
            // separate, consumer-side concern keyed off other signals).
            "video" | "videoclips" | "xxx" => return "video",
            "audio" => return "audio",
            "documents" | "document" | "ebooks" | "ebook" => return "document",
            "compressed" => return "archive",
            "picture" | "pictures" | "image" | "images" => return "image",
            // Games / CD-DVD-BD / software / other / unknown → name sniff below.
            _ => {}
        }
    }
    classify_from_name(name)
}

/// Name-based fallback classifier (extension + distinctive release keywords).
/// Deliberately keyword/extension-only: title *parsing* (incl. season/episode
/// detection) now lives in the filename-parser plugin, so this classifier no
/// longer reaches into the Rust parser — a release that is video only by virtue
/// of an `SxxExx` pattern with no other video token falls through to `other`
/// here and is reclassified correctly once the plugin parses it post-resolve.
fn classify_from_name(name: &str) -> &'static str {
    let n = name.to_ascii_lowercase();
    const VIDEO: &[&str] = &[
        ".mkv", ".mp4", ".avi", ".mov", ".wmv", ".flv", ".m4v", ".webm", "1080p", "720p",
        "2160p", "480p", "x264", "x265", "h264", "h265", "hevc", "bluray", "blu-ray", "bdrip",
        "brrip", "dvdrip", "web-dl", "webrip", "hdtv", "hdrip", "xvid",
    ];
    const AUDIO: &[&str] = &[
        ".mp3", ".flac", ".wav", ".aac", ".ogg", ".m4a", ".opus", ".alac", ".wma", "320kbps",
        "discography",
    ];
    const ARCHIVE: &[&str] = &[".rar", ".zip", ".7z", ".tar", ".gz", ".tgz", ".iso"];
    const DOCUMENT: &[&str] = &[
        ".pdf", ".epub", ".mobi", ".azw3", ".djvu", ".cbz", ".cbr", ".docx", ".txt",
    ];
    const IMAGE: &[&str] = &[".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp", ".tiff"];
    if VIDEO.iter().any(|k| n.contains(k)) {
        return "video";
    }
    if AUDIO.iter().any(|k| n.contains(k)) {
        return "audio";
    }
    if ARCHIVE.iter().any(|k| n.contains(k)) {
        return "archive";
    }
    if DOCUMENT.iter().any(|k| n.contains(k)) {
        return "document";
    }
    if IMAGE.iter().any(|k| n.contains(k)) {
        return "image";
    }
    "other"
}

/// Convert one Tribler search item into the wire-shape `DiscoveryRecord`.
/// Field naming follows the same conventions as torznab (`title`, `sourceUrl`,
/// `sizeByte`, `fileType`, `contentKind`, `videoType`, `season`/`episode`,
/// `languages/<iso3>`, `quality`) plus the canonical `triblerid` provenance
/// field — minus TMDB anchoring (out of scope for v1). `fileType` is classified
/// from the actual torrent (see [`classify_file_type`]); video-only fields are
/// stamped only when the classification is `video`.
fn into_discovery_record(item: TriblerItem) -> Option<DiscoveryRecord> {
    let infohash = item.infohash?.trim().to_ascii_lowercase();
    // Must be a usable v1 infohash; otherwise the record can never resolve.
    common::decode_infohash(&infohash)?;

    let mut fields: BTreeMap<String, String> = BTreeMap::new();
    let name = item.name.unwrap_or_default();
    if !name.is_empty() {
        fields.insert("title".to_string(), name.clone());
    }
    // Provenance: canonical `<upstream_id>id` = infohash (queryable filter).
    fields.insert(upstream_id_field("tribler"), infohash.clone());

    // Synthesized magnet — the handoff key for any BitTorrent fetcher.
    let mut magnet = format!("magnet:?xt=urn:btih:{infohash}");
    if !name.is_empty() {
        magnet.push_str("&dn=");
        magnet.push_str(&common::urlencode(&name));
    }
    // Fold the torrent's own trackers PLUS a set of well-known public trackers
    // into the magnet (deduped). Tribler discovers most torrents over its DHT, so
    // their magnets carry 0-1 trackers — meta-share's on-demand fetcher would then
    // have to find the (often abundant) seeders via BT-DHT alone, which is slow /
    // unreliable in a constrained network even for a healthy swarm. Reachable
    // public trackers (the seeders announce to them) let the fetcher discover
    // peers quickly. This is magnet *metadata* only — the client still does the
    // actual download; the gateway never fetches bytes.
    let mut seen_tr: BTreeSet<String> = BTreeSet::new();
    for tr in item
        .trackers
        .iter()
        .map(|t| t.trim())
        .filter(|t| !t.is_empty())
        .chain(PUBLIC_TRACKERS.iter().copied())
    {
        if seen_tr.insert(tr.to_string()) {
            magnet.push_str("&tr=");
            magnet.push_str(&common::urlencode(tr));
        }
    }
    fields.insert("sourceUrl".to_string(), magnet);

    if let Some(size) = item.size {
        fields.insert("sizeByte".to_string(), size.to_string());
    }
    if let Some(s) = item.num_seeders {
        fields.insert("seeders".to_string(), s.to_string());
    }
    if let Some(l) = item.num_leechers {
        fields.insert("leechers".to_string(), l.to_string());
    }
    if let Some(cat) = item.category.as_deref().map(str::trim).filter(|c| !c.is_empty()) {
        // Coarse classification as a registry `categories/{name}` key-set member
        // (METADATA_KEYS.md) rather than the old flat `triblerCategory` scalar.
        // `/` is the key-set path separator, so a single label like "CD/DVD/BD"
        // is sanitized to one leaf (`categories/CD-DVD-BD`) instead of implying a
        // three-level hierarchy.
        let safe = cat.replace('/', "-");
        fields.insert(format!("categories/{safe}"), "true".to_string());
    }

    // Tribler is a wildcard free-text source — classify the *actual* content
    // type from the torrent name + tribler category instead of assuming video.
    let file_type = classify_file_type(&name, item.category.as_deref());
    fields.insert("fileType".to_string(), file_type.to_string());

    // Title-derived metadata (contentKind / videoType / season / episode /
    // quality / codec / languages) is deliberately NOT stamped on the transient
    // live-search hit. The filename-parser plugin owns title parsing now and
    // fills these onto the *resolved* record at enrich time (post-resolve); a
    // live hit carries only what the upstream itself provided (title, size,
    // category, magnet). See docs/others/feeder-enrichment.md (Option A) — the
    // grid hides poster-less items anyway, and the poster also arrives
    // post-resolve, so the parsed fields land alongside it.

    Some(DiscoveryRecord {
        upstream_id: "tribler".to_string(),
        record_id: infohash,
        fields,
    })
}

/// Build the enrichment driver: infra (`META_CORE_URL` + plugin URLs + peer +
/// callback) from env, with the TMDB key + language overridden by the effective
/// dashboard config (`config.json` > env seed). `None` when `META_CORE_URL` is
/// unset (legacy gateway-stores path). Reuses the plugin's HTTP client.
fn build_enricher(http: &reqwest::Client, cfg: &TriblerConfigFile) -> Option<Enricher> {
    let meta_core_url = cfg
        .meta_core_url
        .as_ref()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| meta_feeder_sdk::DEFAULT_META_CORE_URL.to_string());
    let mut ecfg = EnrichmentConfig::from_meta_core(meta_core_url);
    if let Some(k) = cfg.tmdb_api_key.as_ref().filter(|s| !s.trim().is_empty()) {
        ecfg.tmdb_token = Some(k.clone());
    }
    if let Some(l) = cfg.tmdb_language.as_ref().filter(|s| !s.trim().is_empty()) {
        ecfg.tmdb_language = l.clone();
    }
    Some(Enricher::new(http.clone(), ecfg))
}

/// A sparse (CID-only) outcome — feeder self-published the record, so the
/// gateway dispatcher's `(_, None)` branch skips storage. All gateway BT
/// locators ride as `BtV1File` (the pack `compute_bt_info_cid` is a BT-family
/// CID too; the kind is cosmetic on a sparse outcome since storage is skipped).
fn sparse_outcome(cid: &str) -> HashOutcome {
    HashOutcome {
        hash: Hash(cid.to_string()),
        hash_kind: HashKind::BtV1File,
        bytes: None,
        record: None,
        file_extension: None,
    }
}

/// Build a per-file record from the torrent-level fields + one file. Per-file
/// overrides: `fileName`, `sizeByte`, and the video sub-fields re-derived from
/// the **file** name (a season pack's torrent name lacks `SxxExx`; each file
/// carries its own). `record_id` stays the infohash (→ provenance.recordId).
fn build_file_record(
    bibrec: &BTreeMap<String, String>,
    record_id: &str,
    f: &TorrentFile,
) -> DiscoveryRecord {
    let mut fields = bibrec.clone();
    fields.insert("fileName".to_string(), f.name.clone());
    fields.insert("sizeByte".to_string(), f.size.to_string());
    fields.insert("fileType".to_string(), "video".to_string());
    // Per-file title-derived fields (contentKind / videoType / season / episode
    // / quality / codec / originalTitle) are written by the filename-parser
    // plugin at enrich time, keyed on this file's `fileName` — the feeder no
    // longer parses titles in Rust. Clear any inherited torrent-level values so
    // a stale pack-level season can't leak onto a file the plugin parses as a
    // movie (merge is per-field upsert, so a key the plugin doesn't set would
    // otherwise survive). See docs/others/feeder-enrichment.md.
    for k in ["season", "episode", "quality", "codec", "contentKind", "videoType"] {
        fields.remove(k);
    }
    DiscoveryRecord {
        upstream_id: "tribler".to_string(),
        record_id: record_id.to_string(),
        fields,
    }
}

/// Build the whole-torrent pack record: torrent-level fields minus per-episode
/// specifics. The enricher runs filename-parse only on it (no tmdb), so it has
/// no poster → meta-watch hides it, while raw search still finds it by title.
fn build_pack_record(bibrec: &BTreeMap<String, String>, record_id: &str) -> DiscoveryRecord {
    let mut fields = bibrec.clone();
    fields.remove("season");
    fields.remove("episode");
    DiscoveryRecord {
        upstream_id: "tribler".to_string(),
        record_id: record_id.to_string(),
        fields,
    }
}

/// Minimal fields for a `compute_outcomes` record when search didn't cache
/// the full set (direct resolve by raw infohash). Carries just the provenance
/// + magnet handle + fileType so the metadata-only store isn't empty.
fn minimal_fields(infohash: &str) -> BTreeMap<String, String> {
    let mut fields = BTreeMap::new();
    fields.insert(upstream_id_field("tribler"), infohash.to_string());
    fields.insert(
        "sourceUrl".to_string(),
        format!("magnet:?xt=urn:btih:{infohash}"),
    );
    // No name/category available on a direct raw-infohash resolve, so we can't
    // honestly classify the content. Omit `fileType` rather than fabricate one
    // (the old code hardcoded `video`). An unclassified record simply won't
    // match `fileType:` queries or surface in meta-watch — correct for an
    // unidentified torrent.
    fields
}

#[cfg(test)]
mod tests {
    use super::*;

    const IH: &str = "0123456789abcdef0123456789abcdef01234567";

    #[test]
    fn into_discovery_record_builds_base_fields() {
        let item = TriblerItem {
            infohash: Some(IH.to_uppercase()),
            name: Some("Some.Show.S02E05.1080p.WEB.h264".to_string()),
            size: Some(1_500_000_000),
            num_seeders: Some(42),
            num_leechers: Some(3),
            category: Some("Video".to_string()),
            trackers: vec!["udp://tracker.example.org:1337".to_string()],
        };
        let rec = into_discovery_record(item).expect("record");
        assert_eq!(rec.upstream_id, "tribler");
        // infohash lowercased + used as record_id and triblerid.
        assert_eq!(rec.record_id, IH);
        assert_eq!(rec.fields.get("triblerid").map(String::as_str), Some(IH));
        assert_eq!(
            rec.fields.get("fileType").map(String::as_str),
            Some("video")
        );
        // Title-derived fields are NOT stamped on the live-search hit any more —
        // the filename-parser plugin fills them on the resolved record. The
        // transient hit carries only what the upstream provided.
        for k in [
            "contentKind",
            "videoType",
            "season",
            "episode",
            "quality",
            "codec",
        ] {
            assert!(
                !rec.fields.contains_key(k),
                "live hit must not carry plugin-owned field `{k}`"
            );
        }
        assert!(!rec.fields.keys().any(|k| k.starts_with("languages/")));
        // Category → registry key-set member, not the old flat `triblerCategory`.
        assert_eq!(
            rec.fields.get("categories/Video").map(String::as_str),
            Some("true")
        );
        assert!(!rec.fields.contains_key("triblerCategory"));
        // Tracker folded into the magnet announce list.
        assert!(rec.fields.get("sourceUrl").unwrap().contains("&tr=udp"));
        assert_eq!(
            rec.fields.get("sizeByte").map(String::as_str),
            Some("1500000000")
        );
        assert!(rec
            .fields
            .get("sourceUrl")
            .unwrap()
            .starts_with("magnet:?xt=urn:btih:0123456789abcdef"));
    }

    #[test]
    fn classify_uses_tribler_category_first() {
        assert_eq!(classify_file_type("anything.bin", Some("Audio")), "audio");
        assert_eq!(
            classify_file_type("anything.bin", Some("Compressed")),
            "archive"
        );
        assert_eq!(
            classify_file_type("anything.bin", Some("Documents")),
            "document"
        );
        // Unknown category → fall through to the name sniff.
        assert_eq!(
            classify_file_type("Movie.2020.1080p.mkv", Some("Other")),
            "video"
        );
    }

    #[test]
    fn classify_falls_back_to_name_sniff() {
        assert_eq!(classify_file_type("Album - Discography [FLAC]", None), "audio");
        assert_eq!(classify_file_type("Linux.ISO.collection.rar", None), "archive");
        assert_eq!(classify_file_type("Some Book Title.epub", None), "document");
        assert_eq!(classify_file_type("Show.S01E01.720p.mkv", None), "video");
        // Nothing distinctive → "other" (still routable as a metasearch hit).
        assert_eq!(classify_file_type("mystery-blob-2021", None), "other");
    }

    #[test]
    fn into_discovery_record_non_video_omits_video_fields() {
        let item = TriblerItem {
            infohash: Some(IH.to_string()),
            name: Some("Greatest Hits Collection [320kbps].rar".to_string()),
            category: Some("Compressed".to_string()),
            ..Default::default()
        };
        let rec = into_discovery_record(item).expect("record");
        // Category wins → archive, not the audio the name might suggest.
        assert_eq!(
            rec.fields.get("fileType").map(String::as_str),
            Some("archive")
        );
        // No fabricated video metadata.
        assert!(!rec.fields.contains_key("contentKind"));
        assert!(!rec.fields.contains_key("videoType"));
        assert!(!rec.fields.contains_key("season"));
        assert!(!rec.fields.contains_key("quality"));
        // No `und` language fallback for non-video.
        assert!(!rec.fields.contains_key("languages/und"));
    }

    #[test]
    fn into_discovery_record_rejects_bad_infohash() {
        let item = TriblerItem {
            infohash: Some("not-a-hash".to_string()),
            name: Some("x".to_string()),
            ..Default::default()
        };
        assert!(into_discovery_record(item).is_none());
    }

    #[test]
    fn into_discovery_record_requires_infohash() {
        let item = TriblerItem {
            name: Some("orphan".to_string()),
            ..Default::default()
        };
        assert!(into_discovery_record(item).is_none());
    }

    #[test]
    fn parse_remote_results_flat_shape_matches_uuid() {
        let frame = format!(
            "event: remote_query_results\ndata: {}",
            serde_json::json!({
                "type": "remote_query_results",
                "uuid": "abc-123",
                "results": [
                    { "infohash": IH, "name": "Movie 2021 720p", "size": 100 }
                ]
            })
        );
        let items = parse_remote_results(&frame, "abc-123");
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].infohash.as_deref(), Some(IH));
    }

    #[test]
    fn parse_remote_results_nested_event_shape() {
        let frame = format!(
            "data: {}",
            serde_json::json!({
                "topic": "remote_query_results",
                "event": {
                    "uuid": "u-9",
                    "results": [ { "infohash": IH, "name": "A" } ]
                }
            })
        );
        let items = parse_remote_results(&frame, "u-9");
        assert_eq!(items.len(), 1);
    }

    #[test]
    fn parse_remote_results_filters_other_uuid() {
        let frame = format!(
            "data: {}",
            serde_json::json!({
                "type": "remote_query_results",
                "uuid": "other",
                "results": [ { "infohash": IH, "name": "A" } ]
            })
        );
        assert!(parse_remote_results(&frame, "mine").is_empty());
    }

    #[test]
    fn parse_remote_results_ignores_unrelated_topic() {
        let frame = format!(
            "data: {}",
            serde_json::json!({ "type": "torrent_health", "infohash": IH })
        );
        assert!(parse_remote_results(&frame, "").is_empty());
    }

    #[test]
    fn parse_remote_results_empty_uuid_accepts_any() {
        let frame = format!(
            "data: {}",
            serde_json::json!({
                "type": "remote_query_results",
                "uuid": "whatever",
                "results": [ { "infohash": IH, "name": "A" } ]
            })
        );
        assert_eq!(parse_remote_results(&frame, "").len(), 1);
    }
}
