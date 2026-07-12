//! Shared constants for the torznab plugin module.
//!
//! These symbols are referenced across `mod.rs`, `tmdb.rs` and `enrich.rs`
//! but were never committed when the plugin was split from a single file into
//! this directory module — the crate failed to compile (`E0425: cannot find
//! value ...`). Values below are sensible defaults; review them against the
//! intended torznab/TMDB contract (endpoints + env-var names especially).

use std::time::Duration;

/// User-Agent for outbound HTTP from the torznab plugin (TMDB + indexers).
/// Mirrors the per-plugin convention in `plugins/{arxiv,giphy,gutenberg}.rs`.
pub(crate) const USER_AGENT: &str = concat!(
    "meta-share/",
    env!("CARGO_PKG_VERSION"),
    " (gateway:torznab)"
);

/// TMDB REST API base (v3).
pub(crate) const TMDB_API_BASE: &str = "https://api.themoviedb.org/3";
/// TMDB image CDN base; callers append a size segment (e.g. `/w500/<path>`).
pub(crate) const TMDB_IMAGE_BASE: &str = "https://image.tmdb.org/t/p";
/// TMDB poster size segment inserted between [`TMDB_IMAGE_BASE`] and the
/// poster path. Mandatory — TMDB 404s a path with no size. `w500` is the
/// poster-grid sweet spot (≈122 KB vs `original`'s multi-MB).
pub(crate) const TMDB_POSTER_SIZE: &str = "w500";

/// Generic outbound HTTP timeout (seconds). Raised 100x (30 -> 3000) for
/// completeness — the gateway runs inside a long, streaming search, not a
/// sub-5s one; an outbound call that's still making progress shouldn't be cut.
pub(crate) const HTTP_TIMEOUT_SECS: u64 = 3000;
/// TMDB search request timeout (seconds). Raised 100x (10 -> 1000) — TMDB
/// cross-ref enrichment is part of result completeness and needs time.
pub(crate) const TMDB_SEARCH_TIMEOUT_SECS: u64 = 1000;
/// TMDB poster download timeout (seconds). Raised 100x (30 -> 3000).
pub(crate) const TMDB_POSTER_TIMEOUT_SECS: u64 = 3000;
/// Default Retry-After (seconds) applied when TMDB returns HTTP 429 with no
/// (or an unparseable) `Retry-After` header.
pub(crate) const TMDB_RATE_LIMIT_DEFAULT_RETRY_SECS: u64 = 5;
/// Maximum time to wait for a TMDB enrichment permit before giving up (seconds).
/// Raised 100x (30 -> 3000) — under a deep search many records contend for the
/// 4 enrichment permits; giving up early drops enrichment from the tail.
pub(crate) const TMDB_ENRICH_WAIT_DEADLINE_SECS: u64 = 3000;
/// Maximum number of concurrent TMDB enrichment requests.
pub(crate) const TMDB_ENRICH_CONCURRENCY: usize = 4;
/// Per-indexer torznab query timeout (seconds). This is the cap that actually
/// guillotines slow trackers on the primary *streaming* gateway path (the 5s
/// `GATEWAY_CALL_TIMEOUT` only bounds the one-shot fallback). Raised 100x
/// (3 -> 300) for completeness so a slow-but-alive indexer is waited for
/// rather than silently dropped from the merge. Stays <= the (also-raised)
/// 500s one-shot `GATEWAY_CALL_TIMEOUT`. NOTE: `search_probe_timeout()` reuses
/// this value, so reachability probes against a *dead* indexer now hang up to
/// 300s — acceptable for a completeness-first posture, but the dual use is why
/// this isn't split into its own probe constant yet.
pub(crate) const PER_INDEXER_QUERY_TIMEOUT_SECS: u64 = 300;

/// Max concurrent `.torrent` downloads while resolving one indexer's results.
/// Bounds the socket burst when a whole result page is torrent-only. Kept low
/// (4) because the resolve fetches hit public trackers/torrent caches that
/// reset connections under a wide burst — the same load that makes Prowlarr's
/// own indexer calls flake. Indexer *search* GETs are now serialized one at a
/// time through the shared [`INDEXER_LIMITER_RPS`] token bucket, so the peak
/// socket count is just this many (the in-flight page's resolves), not a
/// product across concurrent indexers.
pub(crate) const TORRENT_RESOLVE_CONCURRENCY: usize = 4;

/// Defensive cap on how many torrent-only items we attempt to resolve per
/// indexer response. The rest are dropped (logged) — at search latency,
/// resolving an unbounded page of `.torrent` files would blow the per-indexer
/// budget. A page rarely needs more than this many resolved results anyway.
pub(crate) const TORRENT_RESOLVE_MAX_PER_INDEXER: usize = 30;

/// Wall-clock budget for the whole torrent-resolution phase of a single
/// indexer call. Items not resolved within it are dropped; items resolved so
/// far are kept (partial). The gateway returns as soon as resolution
/// *completes* — the budget is only the cutoff for a page that can't finish.
///
/// **Sized for a long, streaming search, not a sub-5s one.** meta-share's
/// primary gateway path is a streaming substream (`stream_one_gateway_peer`)
/// with **no per-call deadline** — it forwards each hit to the client SSE as
/// it arrives until the gateway sends `Done`. (The 5 s `GATEWAY_CALL_TIMEOUT`
/// is only the one-shot fallback for gateways that don't speak the streaming
/// protocol.) So this can be generous: the binding constraint is meta-share's
/// `SEARCH_TIMEOUT_SECS` silence window (300 s, the meta-search code default),
/// not a hard call timeout. 5 min is a safety ceiling on a pathological page;
/// a normal XXX page (≤ `TORRENT_RESOLVE_MAX_PER_INDEXER` items at ~1.9 s
/// each, 8-wide) finishes in well under 10 s and the gateway returns then.
pub(crate) const TORRENT_RESOLVE_BUDGET: Duration = Duration::from_secs(300);

/// Ceiling on a downloaded `.torrent` body. Real torrent files are KBs; this
/// guards against a `<link>` that points at an HTML details page or a
/// mislabeled giant download — we skip (don't buffer) anything larger.
pub(crate) const TORRENT_FILE_MAX_BYTES: u64 = 5 * 1024 * 1024;

/// Magnet BEP-9 `list_only` file-list fallback knobs (packs only) — see
/// [`crate::bt::TorznabBt::prefetch_torrent_filelists`].
///
/// HTTP `.torrent` download is always tried first (faster, swarm-free). But
/// magnet-only indexers (e.g. Nyaa.si) expose no `.torrent` URL, so a season
/// **pack** from them never gets a file list and its per-episode records ship
/// without a streamable `cid_btih_v1_file` (see `expand_record_streaming`).
/// For packs only — a single file derives its CID from the infohash alone and
/// needs no file list — fall back to a bounded BEP-9 `list_only` swarm probe
/// off the magnet. Swarm peer/DHT discovery is far slower (and often fruitless
/// for dead torrents) than an HTTP fetch, so these caps are deliberately tight:
/// the cached result makes the *next* search instant, so a partial pass is fine.
pub(crate) const TORRENT_LISTONLY_MAGNET_MAX: usize = 6;
/// Concurrent magnet enumerations. Lower than [`TORRENT_RESOLVE_CONCURRENCY`]
/// because each holds a live librqbit torrent + peer connections, not a single
/// short HTTP GET.
pub(crate) const TORRENT_LISTONLY_MAGNET_CONCURRENCY: usize = 2;
/// Per-magnet wall-clock cap on the BEP-9 enumeration (tighter than
/// `list_files`' own internal full-fetch budget, which is sized for downloads).
pub(crate) const TORRENT_LISTONLY_MAGNET_PER_ITEM: Duration = Duration::from_secs(25);
/// Overall deadline for the whole magnet-fallback pass; whatever resolved by
/// then is kept, the rest fall through to the title-only (un-CID'd) expansion.
pub(crate) const TORRENT_LISTONLY_MAGNET_BUDGET: Duration = Duration::from_secs(60);

/// **Test-only.** When truthy (`1`/`true`/`yes`/`on`), a torznab record that
/// fails TMDB enrichment is stubbed with just enough metadata (a `poster`, a
/// `description`, and video kind if missing) to pass meta-watch's quality
/// gate — simulating "content pollution" so an operator can validate the
/// default-on adult-content filter against records that carry `categories/XXX`
/// but never match TMDB. **Off by default; never enable in production** — it
/// fabricates metadata. See [`super::enrich::stub_unmatched_patch`].
pub(crate) const ENV_STUB_UNMATCHED: &str = "META_GATEWAY_PROWLARR_STUB_UNMATCHED";

/// Timeout used when probing an indexer's search path for reachability.
pub(crate) fn search_probe_timeout() -> Duration {
    Duration::from_secs(PER_INDEXER_QUERY_TIMEOUT_SECS)
}

/// Default number of TMDB anchors resolved per query (top-N by popularity).
/// For a vague keyword ("black") TMDB returns many real shows; we anchor on the
/// top-N rather than collapsing onto the single most popular (the old
/// `principal_confident` behaviour that hijacked the query). Each anchor becomes
/// its own paged search per indexer, so this multiplies the indexer request
/// count — the serialized [`INDEXER_LIMITER_RPS`] bucket keeps that gentle.
/// Overridable via `META_GATEWAY_PROWLARR_ANCHOR_TOP_N`.
pub(crate) const DEFAULT_ANCHOR_TOP_N: usize = 10;

/// How many ranked principal-search anchors to persist per query in the redb
/// cache. Sized comfortably above any reasonable top-N so flipping the knob
/// never needs a cache wipe — reads slice the cached list to the configured N.
pub(crate) const CACHED_PRINCIPAL_DEPTH: usize = 30;

/// Indexer search-request rate limiter (a [`crate::tmdb_budget::TmdbBudget`]
/// token bucket shared across the whole torznab fan-out). Public trackers and
/// Prowlarr reset/429 under bursts, so search GETs are serialized one at a time
/// at ~`INDEXER_LIMITER_RPS` grants/sec with a small `INDEXER_LIMITER_BURST`.
/// Adaptive: a 429 (or transient reset/5xx) pauses the bucket via `note_429`.
pub(crate) const INDEXER_LIMITER_RPS: f64 = 1.0;
/// Burst capacity of the indexer rate limiter (see [`INDEXER_LIMITER_RPS`]).
pub(crate) const INDEXER_LIMITER_BURST: f64 = 3.0;
/// Backoff applied to the indexer limiter on a transient (connection-reset /
/// 5xx) indexer failure that carries no explicit `Retry-After` window.
pub(crate) const INDEXER_LIMITER_TRANSIENT_BACKOFF: Duration = Duration::from_secs(2);
/// Deadline for acquiring an indexer-limiter token before a page request.
/// Generous (completeness-first; meta-share waits its silence window) so a
/// token effectively always arrives — `DeadlineExceeded` just drops that page.
pub(crate) const INDEXER_LIMITER_WAIT_DEADLINE_SECS: u64 = 3000;

/// Minimum fraction of a (cleaned) release title's tokens that must be covered
/// by an anchor's title before a text-anchored job stamps that anchor's tmdbid
/// onto the release. Guards against text false-positives — a release that
/// merely *mentions* the anchor title (e.g. "Turning Mecard … Black Mirror",
/// 2/≈9 ≈ 0.22) stays unanchored and enriches via its own fuzzy lookup, while a
/// real "Black Mirror S05E01 …" (cleans to "Black Mirror", 2/2 = 1.0) is
/// stamped. Only applies to fuzzy text jobs; id-based (tvdbid/tmdbid) jobs are
/// trusted and bypass the guard. See [`super::anchor::apply_record_tag`].
pub(crate) const ANCHOR_TITLE_MIN_COVERAGE: f64 = 0.6;

/// Backoff before the single retry [`super::TorznabPlugin::send_get_with_retry`]
/// makes after a transient (connection-reset / 5xx) indexer failure. Short —
/// just enough to let a momentarily-overloaded indexer or a stale pooled
/// connection clear before the second attempt.
pub(crate) const HTTP_RETRY_BACKOFF: Duration = Duration::from_millis(400);

/// Defensive ceiling on the per-season fan-out loop. A show with more seasons
/// than this (none real; guards a corrupt `number_of_seasons`) only fans out
/// the first `MAX_ANCHORED_SEASONS`.
pub(crate) const MAX_ANCHORED_SEASONS: u32 = 50;

/// Per-indexer fetch page size for the depth strategy. Clamped to each
/// indexer's declared Prowlarr `capabilities.limitsMax`. Kept small/light so a
/// single Prowlarr `indexerIds=<one>` call behaves like the Prowlarr UI (one
/// tracker, a modest page) instead of one heavy `indexerIds=-2&limit=2500`
/// aggregate — that oversized limit was both futile (every public tracker caps
/// at ~100) and the source of the gateway→Prowlarr connection resets under load.
pub(crate) const TORZNAB_PAGE_SIZE: usize = 100;

/// Bounded buffer between the streaming search producer task and the SSE-bound
/// receiver stream. Backpressure: when the consumer (meta-share) is slow the
/// producer's `send().await` parks rather than piling events in memory.
pub(crate) const GATEWAY_STREAM_BUFFER: usize = 256;

/// Max results fetched **per indexer, per search** (depth cap). Bounds both the
/// single-shot request for a non-paginating indexer and the offset-paging loop
/// for one that supports pagination. Tunable; the merge across indexers + the
/// caller's display `limit` truncate still apply on top. With today's 5 public
/// trackers (all `limitsMax=100`, no pagination) the effective per-indexer
/// fetch is 100, so 5×100 ≈ 500 merged — which is where this default lands.
pub(crate) const TORZNAB_MAX_DEPTH: usize = 500;

/// Ceiling on how many records one torrent may fan out to when a pack is
/// unpacked into per-file episodes (or range-inferred). A guard against a
/// pathological 1000-file torrent flooding one search; well above any real
/// season pack. Mirrors the tribler feeder's identical cap.
pub(crate) const MAX_FANOUT_FILES: usize = 100;
