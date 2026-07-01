//! TMDB enrichment + poster store.
//!
//! Split out of the monolithic `torznab.rs` (pure file move; no behaviour change).

use crate::tmdb_budget::Lease;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::future::{BoxFuture, Shared};
use futures::FutureExt;
use tracing::{debug, warn};

use crate::title::clean_torrent_title;
use crate::tmdb::{
    principal_top_n, season_episode_bounds, SeasonEpisodeBounds, TmdbCall, TmdbClient,
    TmdbExternalIds, TmdbHit, TmdbKind, TmdbTvDetails,
};
use crate::tmdb_budget::TmdbBudget;
use meta_feeder_sdk::cache::MidhashCache;
use meta_feeder_sdk::query::GatewaySearchEvent;
use meta_feeder_sdk::types::DiscoveryRecord;

/// Resolve a record's transient `posterPath` into a `poster_url` (the public
/// TMDB poster CDN URL) in place, removing `posterPath`. In the feeder model the
/// **gateway core** seeds `poster_url` into a content-addressed `poster` cid
/// (same path as giphy/wikicommons previews) — the feeder never fetches or
/// stores the poster bytes, so there is no meta-core poster record and no
/// blockstore seed here. No-op when there's no `posterPath`.
pub(crate) fn set_poster_url(record: &mut DiscoveryRecord, tmdb: &TmdbClient) {
    if let Some(path) = record.fields.remove("posterPath") {
        record
            .fields
            .insert("poster_url".to_string(), tmdb.poster_cdn_url(&path));
    }
}

/// Long-lived TMDB + poster + subtitle-source state, split out of
/// [`TorznabPlugin`]. Owns the enrichment config (TMDB client/budget/inflight,
/// meta-core sink, bitswap blockstore, OpenSubtitles client + language set) and
/// a clone of the shared cache/HTTP client. Builds the per-call [`TmdbEnricher`]
/// and runs the search-time poster store. The plugin composes one of these and
/// delegates its enrich methods here.
#[derive(Clone)]
pub(crate) struct Enricher {
    pub(crate) cache: Option<MidhashCache>,
    pub(crate) tmdb: Option<Arc<TmdbClient>>,
    /// Process-global TMDB token budget. Self-initialised in [`Enricher::new`]
    /// (the feeder owns it now — the gateway's `set_tmdb_budget` injection is
    /// gone). `Option` is kept so `tmdb_enricher` / the call sites stay
    /// unchanged, but it is always `Some` in practice.
    pub(crate) tmdb_budget: Option<Arc<TmdbBudget>>,
    pub(crate) tmdb_inflight:
        Arc<Mutex<HashMap<String, Shared<BoxFuture<'static, Option<TmdbHit>>>>>>,
}

impl Enricher {
    /// Construct the enricher; everything else is filled in by the setters /
    /// `configure`. The TMDB budget self-inits here (feeder-owned).
    pub(crate) fn new() -> Self {
        Self {
            cache: None,
            tmdb: None,
            tmdb_budget: Some(TmdbBudget::new(
                crate::tmdb_budget::DEFAULT_TMDB_REFILL_PER_SEC,
                crate::tmdb_budget::DEFAULT_TMDB_BURST,
            )),
            tmdb_inflight: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Enable TMDB enrichment with the given v4 bearer token. Empty /
    /// whitespace-only tokens leave it disabled (no-op). Idempotent.
    pub(crate) fn set_tmdb_token(&mut self, token: impl Into<String>) {
        let token = token.into();
        if token.trim().is_empty() {
            self.tmdb = None;
        } else {
            self.tmdb = Some(Arc::new(TmdbClient::new(token)));
        }
    }

    /// Test-only: inject a [`TmdbClient`] with mock base URLs.
    #[cfg(test)]
    pub(crate) fn set_tmdb_client_for_test(&mut self, client: TmdbClient) {
        self.tmdb = Some(Arc::new(client));
    }

    /// Build a per-call [`TmdbEnricher`] from the long-lived state. `None` when
    /// TMDB or the cache isn't configured (enrichment disabled). Cheap — every
    /// field cloned is an `Arc`/clone-cheap handle, and the single-flight map is
    /// shared (not copied) so coalescing spans calls.
    pub(crate) fn tmdb_enricher(&self) -> Option<TmdbEnricher> {
        Some(TmdbEnricher {
            client: self.tmdb.clone()?,
            cache: self.cache.clone()?,
            budget: self.tmdb_budget.clone(),
            inflight: self.tmdb_inflight.clone(),
        })
    }

}

/// Bundles the shared state a TMDB enrichment pass needs: the client, the
/// persistent redb cache, the optional process-global budget, and the
/// single-flight map. Cheap to clone (everything inside is `Arc`/clone-cheap).
/// Built per `handle_query` call by [`Enricher::tmdb_enricher`]; the
/// single-flight map is shared, not copied, so concurrent identical lookups
/// across many calls coalesce into one upstream request.
#[derive(Clone)]
pub(crate) struct TmdbEnricher {
    pub(crate) client: Arc<TmdbClient>,
    pub(crate) cache: MidhashCache,
    pub(crate) budget: Option<Arc<TmdbBudget>>,
    pub(crate) inflight: Arc<Mutex<HashMap<String, Shared<BoxFuture<'static, Option<TmdbHit>>>>>>,
}

impl TmdbEnricher {
    /// Cache → single-flight → budget-gated TMDB title search. Persistent
    /// cache hits and single-flight followers consume **no** budget token;
    /// only the leader future acquires one.
    pub(crate) async fn search_cached(
        &self,
        kind: TmdbKind,
        title: &str,
        year: Option<u16>,
    ) -> Option<TmdbHit> {
        let key = tmdb_search_key(kind, title, year);
        // 1. Persistent cache (positive hit JSON or the `"null"` negative
        //    sentinel — both terminate without touching the budget).
        if let Ok(Some(json)) = self.cache.get_tmdb_search(&key) {
            return decode_cached_hit(&json);
        }
        // 2. Single-flight: the leader does the work; followers clone and
        //    await the same shared future.
        let shared = {
            let mut map = self.inflight.lock().expect("tmdb inflight poisoned");
            if let Some(existing) = map.get(&key) {
                existing.clone()
            } else {
                let fut = self
                    .clone()
                    .search_leader(kind, title.to_string(), year, key.clone());
                let shared = fut.boxed().shared();
                map.insert(key.clone(), shared.clone());
                shared
            }
        };
        shared.await
    }

    /// The single-flight leader: gate on the budget, call TMDB, persist the
    /// result, and de-register from the in-flight map. Owns a cloned
    /// [`TmdbEnricher`] so the future is `'static` for `Shared`.
    pub(crate) async fn search_leader(
        self,
        kind: TmdbKind,
        title: String,
        year: Option<u16>,
        key: String,
    ) -> Option<TmdbHit> {
        if let Some(budget) = &self.budget {
            if matches!(
                budget
                    .acquire(Duration::from_secs(TMDB_ENRICH_WAIT_DEADLINE_SECS))
                    .await,
                Lease::DeadlineExceeded
            ) {
                // Couldn't get a token in time — transient, so don't poison
                // the cache. De-register and surface a miss for this pass.
                self.inflight
                    .lock()
                    .expect("tmdb inflight poisoned")
                    .remove(&key);
                return None;
            }
        }
        let (result, cache_write) = match self.client.search(kind, &title, year).await {
            TmdbCall::Hit(h) => {
                let json = serde_json::to_string(&h).ok();
                (Some(h), json)
            }
            // Confirmed miss: negative-cache it so the same title doesn't
            // re-burn a budget token on the next search.
            TmdbCall::Miss => (None, Some("null".to_string())),
            // Rate-limited: pause the whole budget, but DON'T cache (the
            // miss is transient — a later search should retry).
            TmdbCall::RateLimited(retry) => {
                if let Some(budget) = &self.budget {
                    budget.note_429(retry);
                }
                (None, None)
            }
        };
        if let Some(json) = cache_write {
            let _ = self.cache.put_tmdb_search(&key, &json);
        }
        self.inflight
            .lock()
            .expect("tmdb inflight poisoned")
            .remove(&key);
        result
    }

    /// Budget-gate a single TMDB call, then resolve it: acquire a token
    /// (returning `None` on deadline), map `Hit(x)` → `Some(on_hit(x))`,
    /// `Miss` → `None`, and `RateLimited` → note the 429 and `None`. Each
    /// caller keeps its own persistent-cache fast path (the decode / self-heal
    /// rules differ per table) and does its cache write inside `on_hit`. This
    /// centralises the budget-acquire + 429 handling the by-id detail lookups
    /// all repeat. The single-flight title search ([`search_leader`]) keeps its
    /// own copy — it also negative-caches and de-registers from the in-flight
    /// map, which doesn't fit this shape.
    async fn budgeted_call<T, R>(
        &self,
        call: impl std::future::Future<Output = TmdbCall<T>>,
        on_hit: impl FnOnce(T) -> R,
    ) -> Option<R> {
        if let Some(budget) = &self.budget {
            if matches!(
                budget
                    .acquire(Duration::from_secs(TMDB_ENRICH_WAIT_DEADLINE_SECS))
                    .await,
                Lease::DeadlineExceeded
            ) {
                return None;
            }
        }
        match call.await {
            TmdbCall::Hit(x) => Some(on_hit(x)),
            TmdbCall::Miss => None,
            TmdbCall::RateLimited(retry) => {
                if let Some(budget) = &self.budget {
                    budget.note_429(retry);
                }
                None
            }
        }
    }

    /// Cache + budget-gated TMDB TV-details lookup (no single-flight — the
    /// season-bearing-episode path is lower volume and the redb cache catches
    /// most repeats). Cache hits consume no budget token.
    pub(crate) async fn tv_details_cached(&self, tmdbid: u64) -> Option<TmdbTvDetails> {
        let key = tmdbid.to_string();
        if let Ok(Some(json)) = self.cache.get_tmdb_tvdetails(&key) {
            if let Ok(details) = serde_json::from_str::<TmdbTvDetails>(&json) {
                // Self-heal: an entry written before the display fields existed
                // (or by a bounds-only fetch) lacks `name`/`overview`/poster —
                // fall through to a fresh fetch so the anchored enrich path can
                // build a hit. Structurally-complete entries return as-is.
                if details.has_display() {
                    return Some(details);
                }
            }
        }
        self.budgeted_call(self.client.tv_details(tmdbid), |details| {
            if let Ok(json) = serde_json::to_string(&details) {
                let _ = self.cache.put_tmdb_tvdetails(&key, &json);
            }
            details
        })
        .await
    }

    /// Cache + budget-gated TMDB `external_ids` lookup (`tvdb_id` / `imdb_id`
    /// for a known tmdbid). External ids are immutable, so a hit is cached
    /// forever. Cache hits consume no budget token.
    pub(crate) async fn external_ids_cached(
        &self,
        kind: TmdbKind,
        tmdbid: u64,
    ) -> Option<TmdbExternalIds> {
        let key = tmdbid.to_string();
        if let Ok(Some(json)) = self.cache.get_tmdb_extids(&key) {
            return serde_json::from_str(&json).ok();
        }
        self.budgeted_call(self.client.external_ids(kind, tmdbid), |ids| {
            if let Ok(json) = serde_json::to_string(&ids) {
                let _ = self.cache.put_tmdb_extids(&key, &json);
            }
            ids
        })
        .await
    }

    /// Cache + budget-gated TMDB `GET /3/movie/{id}` → [`TmdbHit`] (resolved by
    /// id). The anchored movie enrichment source. Cache hits consume no budget.
    pub(crate) async fn movie_hit_by_id_cached(&self, tmdbid: u64) -> Option<TmdbHit> {
        let key = tmdbid.to_string();
        if let Ok(Some(json)) = self.cache.get_tmdb_moviedetails(&key) {
            return decode_cached_hit(&json);
        }
        self.budgeted_call(self.client.movie_details(tmdbid), |details| {
            let hit = details.into_hit();
            if let Ok(json) = serde_json::to_string(&hit) {
                let _ = self.cache.put_tmdb_moviedetails(&key, &json);
            }
            hit
        })
        .await
    }

    /// Resolve a [`TmdbHit`] **by id** for an anchored record (known tmdbid),
    /// bypassing the fuzzy title search entirely. TV hits are derived from the
    /// cached TV-details payload (which doubles as the season-bounds source);
    /// movie hits from `GET /3/movie/{id}`. `None` when the lookup fails — the
    /// caller keeps the authoritative tmdbid and ships without the rest.
    pub(crate) async fn hit_by_id_cached(&self, kind: TmdbKind, tmdbid: u64) -> Option<TmdbHit> {
        match kind {
            TmdbKind::Tv => self.tv_details_cached(tmdbid).await?.as_hit(tmdbid),
            TmdbKind::Movie => self.movie_hit_by_id_cached(tmdbid).await,
        }
    }

    /// Cache + budget-gated principal `search/multi`: resolve a bare keyword to
    /// up to `n` confident `(tmdbid, kind)` anchors, most-popular first (or `[]`
    /// → caller keeps today's `q=` path). The full ranked list (up to
    /// [`CACHED_PRINCIPAL_DEPTH`]) is persisted and sliced to `n` on read, so a
    /// vague keyword doesn't re-burn a budget token every search and changing
    /// the top-N knob never needs a cache wipe. An empty list is negative-cached
    /// (the `"[]"` sentinel).
    pub(crate) async fn principal_top_n_cached(
        &self,
        query: &str,
        n: usize,
    ) -> Vec<(u64, TmdbKind)> {
        let key = query.trim().to_lowercase();
        if key.is_empty() || n == 0 {
            return Vec::new();
        }
        if let Ok(Some(json)) = self.cache.get_tmdb_principal_topn(&key) {
            let mut list = decode_principal_list(&json);
            list.truncate(n);
            return list;
        }
        if let Some(budget) = &self.budget {
            if matches!(
                budget
                    .acquire(Duration::from_secs(TMDB_ENRICH_WAIT_DEADLINE_SECS))
                    .await,
                Lease::DeadlineExceeded
            ) {
                return Vec::new(); // transient — don't poison the cache
            }
        }
        let ranked: Vec<(u64, TmdbKind)> = match self.client.search_multi(query).await {
            TmdbCall::Hit(hits) => principal_top_n(&hits, query, CACHED_PRINCIPAL_DEPTH)
                .into_iter()
                .filter_map(|h| h.kind().map(|k| (h.id, k)))
                .collect(),
            // The client folds a transient error (timeout / non-2xx) and a
            // genuine "no results" into the same `Miss`, so we can't tell them
            // apart here. Do NOT negative-cache it: a transient TMDB hiccup must
            // not disable multi-anchor for this keyword until the next cache
            // wipe (the bug that made a once-failed "black" search permanently
            // anchor-less). Retry on the next search instead.
            TmdbCall::Miss => return Vec::new(),
            TmdbCall::RateLimited(retry) => {
                if let Some(budget) = &self.budget {
                    budget.note_429(retry);
                }
                return Vec::new(); // don't cache a rate-limit
            }
        };
        // Cache the resolved list — including an empty one, which here means
        // "TMDB responded but nothing passed the relevance gate" (a genuine
        // negative, safe to remember). Transient misses returned above, uncached.
        let _ = self
            .cache
            .put_tmdb_principal_topn(&key, &encode_principal_list(&ranked));
        let mut out = ranked;
        out.truncate(n);
        out
    }
}

/// JSON shape persisted in the principal-search caches.
#[derive(serde::Serialize, serde::Deserialize)]
struct PrincipalEntry {
    tmdbid: u64,
    kind: String,
}

fn kind_str(kind: TmdbKind) -> &'static str {
    match kind {
        TmdbKind::Movie => "movie",
        TmdbKind::Tv => "tv",
    }
}

fn kind_from_str(s: &str) -> Option<TmdbKind> {
    match s {
        "movie" => Some(TmdbKind::Movie),
        "tv" => Some(TmdbKind::Tv),
        _ => None,
    }
}

/// Encode a ranked anchor list (most-popular first) as a JSON array; `"[]"`
/// for empty (the negative-cache sentinel).
fn encode_principal_list(list: &[(u64, TmdbKind)]) -> String {
    let entries: Vec<PrincipalEntry> = list
        .iter()
        .map(|(id, kind)| PrincipalEntry {
            tmdbid: *id,
            kind: kind_str(*kind).to_string(),
        })
        .collect();
    serde_json::to_string(&entries).unwrap_or_else(|_| "[]".to_string())
}

/// Decode a ranked anchor list; unknown kinds are dropped, malformed JSON → [].
fn decode_principal_list(json: &str) -> Vec<(u64, TmdbKind)> {
    serde_json::from_str::<Vec<PrincipalEntry>>(json)
        .unwrap_or_default()
        .into_iter()
        .filter_map(|e| kind_from_str(&e.kind).map(|k| (e.tmdbid, k)))
        .collect()
}

/// Stable lookup key for the TMDB-search cache + single-flight map:
/// `"<m|t>\x01<cleaned-title>\x01<year-or-empty>"`.
pub(crate) fn tmdb_search_key(kind: TmdbKind, title: &str, year: Option<u16>) -> String {
    let k = match kind {
        TmdbKind::Movie => 'm',
        TmdbKind::Tv => 't',
    };
    let y = year.map(|y| y.to_string()).unwrap_or_default();
    format!("{k}\u{1}{title}\u{1}{y}")
}

/// Decode a cached TMDB-search value: the `"null"` sentinel → confirmed miss
/// (`None`), anything else → the encoded [`TmdbHit`].
pub(crate) fn decode_cached_hit(json: &str) -> Option<TmdbHit> {
    if json == "null" {
        return None;
    }
    serde_json::from_str(json).ok()
}

/// Field-level enrichment delta, or a verdict to drop/skip the record.
/// Computed without mutating the base record so the streaming path can emit
/// it as a [`GatewaySearchEvent::EnrichPatch`] / [`GatewaySearchEvent::Drop`].
pub(crate) enum EnrichOutcome {
    /// Apply `set` (insert/overwrite) and `remove` (delete) to the base record.
    Patch {
        set: BTreeMap<String, String>,
        remove: Vec<String>,
    },
    /// A TV match whose title-parsed season/episode is positively out of
    /// TMDB's bounds — retract the record entirely (mirrors the buffered
    /// path's `retain` drop).
    Drop,
    /// Not a TV/movie record, no usable title, or no TMDB match — leave the
    /// base record untouched, emit no patch.
    Noop,
}

/// Best-effort TMDB enrichment for a single record, computed as a delta.
/// No-op if the record's contentKind isn't a known TV/movie kind, if title
/// cleaning fails to produce a non-empty query, or if TMDB returns no hits.
/// Routes every TMDB call through `enricher` (cache + single-flight + budget).
///
/// When the real enrichment is a [`EnrichOutcome::Noop`] and the test-only
/// [`crate::consts::ENV_STUB_UNMATCHED`] flag is set, this falls back to
/// [`stub_unmatched_patch`] — fabricated metadata so an unmatched record
/// (e.g. an adult release tagged `categories/XXX` that never matches TMDB)
/// still surfaces in meta-watch for filter validation. Off by default.
pub(crate) async fn compute_enrichment(
    record: &DiscoveryRecord,
    enricher: &TmdbEnricher,
) -> EnrichOutcome {
    match compute_enrichment_inner(record, enricher).await {
        EnrichOutcome::Noop => stub_unmatched_patch(record).unwrap_or(EnrichOutcome::Noop),
        other => other,
    }
}

/// Is the test-only unmatched-stub flag enabled? Read once. Exposed to
/// `xml.rs` so `into_discovery_record` can stamp a base-record `fileType`
/// for kind-less (e.g. `categories/XXX`) records — without it the
/// `fileType:video` routing/base filter drops them before this enrichment
/// stub runs (the stub only adds poster/description; see `build_stub_patch`).
pub(crate) fn stub_unmatched_enabled() -> bool {
    static FLAG: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *FLAG.get_or_init(|| {
        std::env::var(crate::consts::ENV_STUB_UNMATCHED)
            .map(|v| {
                matches!(
                    v.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
            .unwrap_or(false)
    })
}

/// **Test-only.** Fabricate the minimum metadata an unmatched record needs to
/// pass meta-watch's quality gate (`is_video && has_poster && has_description`,
/// see `meta-watch/src/gate.rs`), so it surfaces in the UI for adult-filter
/// validation. Returns `None` unless [`stub_unmatched_enabled`] and the record
/// has a title. Only fills what's missing — a `5xxx` anime miss keeps its
/// `episode` kind and just gains a poster/description; an `XXX` (`6xxx`) record
/// with no kind at all gains video kind too. The `poster` is a non-resolvable
/// marker (the tile renders broken — card visibility is what the test toggles),
/// and the canonical `categories/XXX` tag is already on the record from
/// `xml.rs`, so the consumer-side filter keys on it unchanged.
fn stub_unmatched_patch(record: &DiscoveryRecord) -> Option<EnrichOutcome> {
    if !stub_unmatched_enabled() {
        return None;
    }
    build_stub_patch(&record.fields)
}

/// Pure stub-patch builder (env-independent, for unit tests). Fills only the
/// gate fields that are missing. Returns `None` if there's no title (can't
/// render a card) or nothing needs fabricating (already passes the gate).
fn build_stub_patch(f: &BTreeMap<String, String>) -> Option<EnrichOutcome> {
    if !f
        .get("title")
        .map(|t| !t.trim().is_empty())
        .unwrap_or(false)
    {
        return None;
    }
    let is_video = f.get("fileType").map(|v| v.eq_ignore_ascii_case("video")) == Some(true)
        || matches!(
            f.get("videoType")
                .map(|v| v.to_ascii_lowercase())
                .as_deref(),
            Some("movie") | Some("tvshow") | Some("episode")
        );
    let has_poster = f
        .get("poster")
        .map(|p| !p.trim().is_empty())
        .unwrap_or(false);
    let has_desc = f
        .get("description")
        .map(|d| !d.trim().is_empty())
        .unwrap_or(false)
        || f.get("plot/eng")
            .map(|d| !d.trim().is_empty())
            .unwrap_or(false);

    let mut set: BTreeMap<String, String> = BTreeMap::new();
    if !is_video {
        set.insert("fileType".to_string(), "video".to_string());
        set.insert("videoType".to_string(), "movie".to_string());
        set.insert("contentKind".to_string(), "movie".to_string());
    }
    if !has_poster {
        // Non-resolvable marker — the image 404s (broken tile), but the card
        // renders, which is all the on/off filter test needs.
        set.insert("poster".to_string(), "stub-no-tmdb-poster".to_string());
    }
    if !has_desc {
        set.insert(
            "description".to_string(),
            "[STUB] Simulated unmatched/polluted record (no TMDB match) — for adult-filter testing.".to_string(),
        );
    }
    if set.is_empty() {
        // Already passes the gate on its own — nothing to fabricate.
        return None;
    }
    Some(EnrichOutcome::Patch {
        set,
        remove: Vec::new(),
    })
}

#[cfg(test)]
mod stub_tests {
    use super::*;

    fn fields(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn patch_set(outcome: Option<EnrichOutcome>) -> BTreeMap<String, String> {
        match outcome {
            Some(EnrichOutcome::Patch { set, .. }) => set,
            _ => panic!("expected a stub patch"),
        }
    }

    #[test]
    fn xxx_record_with_no_kind_gets_full_video_stub() {
        // A pure-XXX record (no contentKind/fileType, just a title + category)
        // gets video kind + poster + description so it passes meta-watch.
        let set = patch_set(build_stub_patch(&fields(&[
            ("title", "Some Release"),
            ("categories/XXX", "true"),
        ])));
        assert_eq!(set.get("fileType").map(String::as_str), Some("video"));
        assert_eq!(set.get("videoType").map(String::as_str), Some("movie"));
        assert!(set.contains_key("poster"));
        assert!(set.contains_key("description"));
    }

    #[test]
    fn existing_video_kind_and_desc_are_preserved() {
        // A 5xxx anime miss already carries episode kind; only the missing
        // poster is fabricated — kind and description are left untouched.
        let set = patch_set(build_stub_patch(&fields(&[
            ("title", "Anime S01E01"),
            ("fileType", "video"),
            ("videoType", "tvshow"),
            ("contentKind", "episode"),
            ("plot/eng", "real synopsis"),
        ])));
        assert!(!set.contains_key("fileType"));
        assert!(!set.contains_key("videoType"));
        assert!(!set.contains_key("description"));
        assert_eq!(
            set.get("poster").map(String::as_str),
            Some("stub-no-tmdb-poster")
        );
    }

    #[test]
    fn no_title_or_nothing_missing_returns_none() {
        // No title ⇒ can't build a card.
        assert!(build_stub_patch(&fields(&[("categories/XXX", "true")])).is_none());
        // Already passes the gate ⇒ nothing to fabricate.
        assert!(build_stub_patch(&fields(&[
            ("title", "x"),
            ("fileType", "video"),
            ("poster", "real-cid"),
            ("description", "real"),
        ]))
        .is_none());
    }
}

/// Best-effort TMDB enrichment for a single record, computed as a delta.
/// No-op if the record's contentKind isn't a known TV/movie kind, if title
/// cleaning fails to produce a non-empty query, or if TMDB returns no hits.
/// Routes every TMDB call through `enricher` (cache + single-flight + budget).
///
/// Two stages: [`resolve_enrichment_hit`] picks the canonical TMDB entry (by
/// anchored id, else fuzzy title search), then [`build_enrichment_patch`]
/// turns that hit into the field delta (and runs the TV season/episode
/// bounds-check that can drop the record).
async fn compute_enrichment_inner(
    record: &DiscoveryRecord,
    enricher: &TmdbEnricher,
) -> EnrichOutcome {
    // Only enrich TV/movie records.
    let kind = match record.fields.get("contentKind").map(String::as_str) {
        Some("movie") => TmdbKind::Movie,
        Some("episode") => TmdbKind::Tv,
        _ => return EnrichOutcome::Noop,
    };
    let raw_title = record
        .fields
        .get("title")
        .map(|t| t.trim().to_string())
        .filter(|t| !t.is_empty());

    let (hit, matched_kind) = match resolve_enrichment_hit(record, enricher, kind, &raw_title).await
    {
        Some(resolved) => resolved,
        None => return EnrichOutcome::Noop,
    };
    build_enrichment_patch(record, enricher, hit, matched_kind, raw_title).await
}

/// Pick the canonical TMDB entry for `record`. Prefers the **anchored** path —
/// an authoritative `tmdbid` stamped on the Base by the structural
/// tvsearch/movie fan-out or principal-search — resolving by id so a fuzzy
/// match can't overwrite the anchor. Falls back to a **fuzzy** title search
/// (with a movie↔tv kind fallback, since torznab categories mislabel kinds).
/// `None` ⇒ the caller should no-op.
async fn resolve_enrichment_hit(
    record: &DiscoveryRecord,
    enricher: &TmdbEnricher,
    kind: TmdbKind,
    raw_title: &Option<String>,
) -> Option<(TmdbHit, TmdbKind)> {
    let anchored_id = record
        .fields
        .get("tmdbid")
        .and_then(|s| s.trim().parse::<u64>().ok());
    if let Some(id) = anchored_id {
        // Anchored: resolve by id. On lookup failure we keep the stamped
        // tmdbid and skip the rest (no-op).
        return enricher.hit_by_id_cached(kind, id).await.map(|h| (h, kind));
    }

    let raw_title = raw_title.as_ref()?;
    let cleaned = clean_torrent_title(raw_title);
    if cleaned.title.is_empty() {
        return None;
    }
    // Kind fallback: torznab/newznab categories mislabel films as episodes
    // (e.g. "CODE: White" arrives as cat 5070) and vice-versa. If the
    // primary endpoint misses, try the other before giving up.
    let other = match kind {
        TmdbKind::Tv => TmdbKind::Movie,
        TmdbKind::Movie => TmdbKind::Tv,
    };
    if let Some(h) = enricher
        .search_cached(kind, &cleaned.title, cleaned.year)
        .await
    {
        return Some((h, kind));
    }
    enricher
        .search_cached(other, &cleaned.title, cleaned.year)
        .await
        .map(|h| (h, other))
}

/// Turn a resolved TMDB `hit` into the field delta: title/synopsis/year/tmdbid,
/// content-kind reconciliation (movie clears bogus season/episode; tv normalises
/// to `episode` and bounds-checks the parsed season/episode — an out-of-bounds
/// verdict drops the record), and the transient `posterPath` the poster store
/// consumes. `raw_title` is preserved as `originalTitle`.
async fn build_enrichment_patch(
    record: &DiscoveryRecord,
    enricher: &TmdbEnricher,
    hit: TmdbHit,
    matched_kind: TmdbKind,
    raw_title: Option<String>,
) -> EnrichOutcome {
    let mut set: BTreeMap<String, String> = BTreeMap::new();
    let mut remove: Vec<String> = Vec::new();
    set.insert("title".to_string(), hit.title.clone());
    // Preserve the raw tracker release title (quality tags / release-group
    // info) in `originalTitle`. This is the canonical home now that the
    // bespoke `releaseTitle` key is retired — reuse-before-invent
    // (METADATA_KEYS.md rule #1, §14.10 title sprawl).
    if let Some(raw_title) = raw_title {
        set.insert("originalTitle".to_string(), raw_title);
    }
    // TMDB's original-language title (e.g. "ナルト") is a genuine localized
    // title — file it under the namespaced `titles/{lang3}` (METADATA_KEYS.md
    // §3), keyed by TMDB's `original_language`, instead of clobbering
    // `originalTitle`. Skipped when the language is outside the common 639-1
    // set or the title is blank (originalTitle still carries the raw title).
    if let Some(orig) = hit.original_title.clone() {
        if !orig.trim().is_empty() {
            if let Some(lang3) = hit.original_lang3() {
                set.insert(format!("titles/{lang3}"), orig);
            }
        }
    }
    if let Some(o) = hit.overview.clone() {
        if !o.trim().is_empty() {
            // Canonical synopsis field is the language-namespaced
            // `description/{lang3}` (METADATA_KEYS.md §14.6) — the
            // cross-content-kind synopsis, and the form meta-share's free-text
            // tokenizer actually indexes (`description/*` prefix, §12). TMDB
            // search responses default to en-US (the client sends no
            // `language` param), so the overview is English prose → `eng`. The
            // flat `overview`/`description` keys are neither in the registry
            // nor indexed, so they'd be invisible to cohort search.
            set.insert("description/eng".to_string(), o);
        }
    }
    if let Some(y) = hit.year {
        set.insert("movieYear".to_string(), y.to_string());
    }
    set.insert("tmdbid".to_string(), hit.tmdbid.to_string());
    // Reconcile content kind against what TMDB actually matched. A movie
    // match clears the bogus season/episode; a TV match normalizes
    // contentKind to "episode".
    match matched_kind {
        TmdbKind::Movie => {
            set.insert("contentKind".to_string(), "movie".to_string());
            // Co-write the narrower `videoType` facet (METADATA_KEYS.md §14.11
            // — contentKind=movie ↔ videoType=movie). Overwrites any
            // category-derived videoType from `into_discovery_record` with the
            // TMDB-reconciled kind.
            set.insert("videoType".to_string(), "movie".to_string());
            set.insert("fileType".to_string(), "video".to_string());
            remove.push("season".to_string());
            remove.push("episode".to_string());
        }
        TmdbKind::Tv => {
            set.insert("contentKind".to_string(), "episode".to_string());
            // contentKind=episode ↔ videoType=tvshow (METADATA_KEYS.md §14.11).
            set.insert("videoType".to_string(), "tvshow".to_string());
            // Season/episode boundary validation. Only a parsed `season`
            // triggers a details lookup, then [`season_episode_bounds`] grades
            // the parse against TMDB's real structure:
            //   - SeasonOverflow (the `S2`-of-a-1-TMDB-season anime cour case):
            //     the TMDB match is sound, only the season number is a
            //     fansub-numbering artifact — keep the record but strip the
            //     misleading `season` so it doesn't claim a season TMDB lacks.
            //   - Contradiction (negative season, or episode past a matched
            //     season's count): the parse is wrong — drop the whole record
            //     so meta-core never ingests it.
            let season = record
                .fields
                .get("season")
                .and_then(|s| s.trim().parse::<i64>().ok());
            if season.is_some() {
                let episode = record
                    .fields
                    .get("episode")
                    .and_then(|e| e.trim().parse::<i64>().ok());
                if let Some(details) = enricher.tv_details_cached(hit.tmdbid).await {
                    match season_episode_bounds(&details, season, episode) {
                        SeasonEpisodeBounds::Ok => {}
                        SeasonEpisodeBounds::SeasonOverflow => {
                            debug!(
                                target: "meta-share::gateway",
                                upstream = "prowlarr",
                                tmdbid = hit.tmdbid,
                                season = ?season,
                                episode = ?episode,
                                number_of_seasons = details.number_of_seasons,
                                record_id = %record.record_id,
                                "title-parsed season past TMDB bounds (cour-as-season); stripping season, keeping record"
                            );
                            remove.push("season".to_string());
                        }
                        SeasonEpisodeBounds::Contradiction => {
                            // The parsed season/episode contradicts TMDB's
                            // structure — e.g. an explicit "S01E25" on a show
                            // whose season 1 has 7 episodes. That is NOT absolute
                            // numbering (absolute-numbered releases carry no
                            // season token and are left season-less upstream, so
                            // they never reach here); it's a mismatched or
                            // garbled record, so drop it rather than surfacing a
                            // phantom episode. We deliberately no longer try to
                            // "rescue" it by remapping the episode onto a TMDB
                            // season — a season is only ever asserted from a
                            // trusted explicit token, never inferred.
                            warn!(
                                target: "meta-share::gateway",
                                upstream = "prowlarr",
                                tmdbid = hit.tmdbid,
                                season = ?season,
                                episode = ?episode,
                                number_of_seasons = details.number_of_seasons,
                                record_id = %record.record_id,
                                "title-parsed season/episode contradicts TMDB; dropping record"
                            );
                            return EnrichOutcome::Drop;
                        }
                    }
                }
            }
        }
    }
    if let Some(p) = hit.poster_path.clone() {
        // Transient input only — `store_poster_inner` reads it to fetch the
        // bytes, sets `poster=<cid>`, and removes it before the record ships.
        let normalised = p.trim_start_matches('/').to_string();
        set.insert("posterPath".to_string(), normalised);
    }
    EnrichOutcome::Patch { set, remove }
}

/// Buffered-path enrichment: apply [`compute_enrichment`]'s delta to `record`
/// in place. Returns `false` when the record should be dropped (out-of-bounds
/// season/episode).
pub(crate) async fn enrich_record_with_tmdb(
    record: &mut DiscoveryRecord,
    enricher: &TmdbEnricher,
) -> bool {
    match compute_enrichment(record, enricher).await {
        EnrichOutcome::Drop => false,
        EnrichOutcome::Noop => true,
        EnrichOutcome::Patch { set, remove } => {
            for k in &remove {
                record.fields.remove(k);
            }
            for (k, v) in set {
                record.fields.insert(k, v);
            }
            true
        }
    }
}

/// Streaming-path enrichment for one base record: compute the TMDB delta,
/// store the poster (off-budget), write the enriched bibrec sidecar, and
/// return the [`GatewaySearchEvent`]s the consumer should apply on top of the
/// already-emitted `Base`. Owns its inputs so the enclosing stream is
/// `'static` (dropping the stream cancels in-flight TMDB work and frees the
/// budget). Mirrors the buffered `handle_query` enrich → poster → bibrec
/// phases, minus the cross-record `retain` (a drop is signalled per-record via
/// a `Drop` event instead).
pub(crate) async fn enrich_one_streaming(
    record: DiscoveryRecord,
    enricher: Option<TmdbEnricher>,
    cache: Option<MidhashCache>,
) -> Vec<GatewaySearchEvent> {
    let record_id = record.record_id.clone();
    let mut working = record;
    let mut events: Vec<GatewaySearchEvent> = Vec::new();

    // 1. TMDB metadata enrichment (budget-gated inside the enricher).
    if let Some(enricher) = &enricher {
        match compute_enrichment(&working, enricher).await {
            EnrichOutcome::Drop => {
                // The Base already shipped; retract it. Don't poster-store or
                // bibrec-cache the bogus record (matches the buffered retain).
                events.push(GatewaySearchEvent::Drop { record_id });
                return events;
            }
            EnrichOutcome::Noop => {}
            EnrichOutcome::Patch { set, remove } => {
                for k in &remove {
                    working.fields.remove(k);
                }
                for (k, v) in &set {
                    working.fields.insert(k.clone(), v.clone());
                }
                // `posterPath` is an internal transient — `store_poster_inner`
                // consumes it and emits `poster` instead. Never expose it.
                let mut wire_set = set;
                wire_set.remove("posterPath");
                if !wire_set.is_empty() || !remove.is_empty() {
                    events.push(GatewaySearchEvent::EnrichPatch {
                        record_id: record_id.clone(),
                        set: wire_set,
                        remove,
                    });
                }
            }
        }
    }

    // 2. Poster: resolve the TMDB CDN url and emit it as `poster_url` (off the
    //    TMDB API budget — different CDN host). The gateway core seeds it into a
    //    content-addressed `poster` cid; the feeder no longer fetches or stores
    //    the bytes. `poster_url` IS persisted into `working.fields` so it rides
    //    bibrec into `compute_outcomes`, where the core seeds it onto the stored
    //    video record too.
    if let Some(enricher) = &enricher {
        if working.fields.contains_key("posterPath") {
            set_poster_url(&mut working, &enricher.client);
            if let Some(url) = working.fields.get("poster_url").cloned() {
                let mut set = BTreeMap::new();
                set.insert("poster_url".to_string(), url);
                events.push(GatewaySearchEvent::EnrichPatch {
                    record_id: record_id.clone(),
                    set,
                    remove: vec!["posterPath".to_string()],
                });
            }
        }
    }

    // 3. bibrec sidecar (enriched fields) — same as the buffered path.
    if let Some(cache) = &cache {
        if let Err(e) = cache.put_bibrec(&working.record_id, &working.fields) {
            warn!(
                target: "meta-share::gateway",
                upstream = "prowlarr",
                record_id = working.record_id,
                error = %e,
                "bibrec cache put failed (non-fatal; compute_outcomes will NotFound)"
            );
        }
    }

    events
}

use crate::consts::*;
