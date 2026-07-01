//! Keyword-less catalog discovery branch.
//!
//! meta-share has no catalog/browse primitive, so a Stremio-style client
//! (meta-watch) can't ask "what's popular" — it can only search by keyword.
//! This branch answers a **keyword-less** gateway query whose intent is carried
//! by structured filters, by calling TMDB's catalog endpoints. TMDB is the
//! catalog oracle; the results are clean, ranked entries, each a distinct TMDB
//! entity by construction (so the per-hit anchor-stamping that causes the
//! wrong-poster bug on the indexer path simply can't happen here).
//!
//! ## Query dialect
//!
//! - **mode** (one of) — `popular:true` → `/{movie,tv}/popular`,
//!   `trending:true` → `/trending/{movie,tv}/week`,
//!   `top_rated:true` → `/{movie,tv}/top_rated`.
//! - **kind** (required, also drives meta-share routing) — `contentKind:movie`
//!   → movies, `contentKind:episode` (or `tv`/`tvshow`) → TV.
//! - **anime** (optional) — `anime:true` switches to `/discover/{movie,tv}`
//!   filtered to the TMDB *anime* keyword, sorted to match the mode. Useful when
//!   the only reachable indexer is an anime tracker (Nyaa): anime catalog titles
//!   actually have torrents, so the rows populate.
//!
//! ## Seeds, not files
//!
//! The records emitted here are **metadata-only seeds** — `title` + `tmdbid`
//! (+ year) — *not* playable releases. There is no torznab fan-out, no poster
//! fetch, and no `compute_outcomes`: meta-watch consumes a seed only to read its
//! title/tmdbid, then issues its *own* anchored per-title search
//! (`<title> fileType:video tmdbid:NNN`) to map a real file behind the card.
//!
//! ## Why the marker stamps are load-bearing
//!
//! Both the gateway's own dispatcher-side filter and meta-share's consumer-side
//! `record_matches` re-apply the query's structured filters to every returned
//! record, and a **missing field fails the match** (drop). So each seed must
//! echo *every* discovery filter the query carried (`popular`/`trending`/
//! `top_rated`, `contentKind`, and `anime` when present) or it is dropped at one
//! of the two tiers.

use futures::channel::mpsc::Sender;
use futures::SinkExt;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use meta_feeder_sdk::query::{GatewayQuery, GatewaySearchEvent};
use crate::tmdb_budget::{Lease, TmdbBudget};
use meta_feeder_sdk::types::DiscoveryRecord;

use crate::consts::TMDB_ENRICH_WAIT_DEADLINE_SECS;
use crate::tmdb::{TmdbCall, TmdbClient, TmdbHit, TmdbKind};

/// TMDB keyword id for "anime" — the `anime:true` filter narrows `/discover` to
/// it so an anime-only tracker actually has matching torrents.
const TMDB_ANIME_KEYWORD: &str = "210024";

/// The catalog-list flavour a discovery query asks for. The marker filter that
/// selected it (`popular`/`trending`/`top_rated`) is echoed onto every seed so
/// the record survives `record_matches` on both tiers.
#[derive(Clone, Copy)]
enum DiscoveryMode {
    Popular,
    Trending,
    TopRated,
}

impl DiscoveryMode {
    /// The query-filter key that selects this mode (and is stamped on seeds).
    fn marker(self) -> &'static str {
        match self {
            DiscoveryMode::Popular => "popular",
            DiscoveryMode::Trending => "trending",
            DiscoveryMode::TopRated => "top_rated",
        }
    }

    /// `sort_by` value for the `/discover` (anime) path. `/discover` has no
    /// "trending", so trending approximates to popularity — the dedicated
    /// non-anime path still uses the real `/trending` endpoint.
    fn discover_sort(self) -> &'static str {
        match self {
            DiscoveryMode::TopRated => "vote_average.desc",
            DiscoveryMode::Popular | DiscoveryMode::Trending => "popularity.desc",
        }
    }
}

/// True when this is a keyword-less catalog-discovery query the branch should
/// answer instead of the indexer fan-out: no free text and a truthy
/// mode marker. A `contentKind` is still required to route at all (meta-share
/// keys gateway routing on it) and to pick the movie-vs-TV endpoint.
pub(crate) fn is_discovery_query(query: &GatewayQuery) -> bool {
    query.free_text.trim().is_empty() && discovery_mode(query).is_some()
}

/// First truthy mode marker on the query, in precedence order.
fn discovery_mode(query: &GatewayQuery) -> Option<DiscoveryMode> {
    for mode in [
        DiscoveryMode::Trending,
        DiscoveryMode::Popular,
        DiscoveryMode::TopRated,
    ] {
        if filter_is_true(query, mode.marker()) {
            return Some(mode);
        }
    }
    None
}

/// True iff `query.filters[key]` carries a truthy value.
fn filter_is_true(query: &GatewayQuery, key: &str) -> bool {
    query
        .filters
        .get(key)
        .is_some_and(|v| v.iter().any(|s| s.eq_ignore_ascii_case("true")))
}

/// Map the query's `contentKind` filter to a TMDB media kind.
fn discovery_kind(query: &GatewayQuery) -> Option<TmdbKind> {
    let values = query.filters.get("contentKind")?;
    for v in values {
        match v.trim().to_ascii_lowercase().as_str() {
            "movie" => return Some(TmdbKind::Movie),
            "episode" | "tvshow" | "tv" => return Some(TmdbKind::Tv),
            _ => {}
        }
    }
    None
}

/// TMDB returns this many results per catalog page; used to translate a desired
/// seed count into a page count (`discover_records` walks pages until it has
/// `max_results` hits).
const TMDB_PAGE_SIZE: usize = 20;

/// Build the `path_and_query` for [`TmdbClient::discovery_list`] from the mode,
/// kind, anime flag, and 1-based `page`.
fn build_path_and_query(mode: DiscoveryMode, kind: TmdbKind, anime: bool, page: u32) -> String {
    let seg = match kind {
        TmdbKind::Movie => "movie",
        TmdbKind::Tv => "tv",
    };
    if anime {
        // `/discover` is the only endpoint that takes a keyword filter. Approx
        // top-rated with a vote-count floor so a 1-vote 10.0 doesn't win.
        let floor = match mode {
            DiscoveryMode::TopRated => "&vote_count.gte=200",
            _ => "",
        };
        format!(
            "discover/{seg}?with_keywords={TMDB_ANIME_KEYWORD}&sort_by={}&include_adult=false{floor}&page={page}",
            mode.discover_sort(),
        )
    } else {
        match mode {
            DiscoveryMode::Popular => format!("{seg}/popular?page={page}"),
            DiscoveryMode::TopRated => format!("{seg}/top_rated?page={page}"),
            DiscoveryMode::Trending => format!("trending/{seg}/week?page={page}"),
        }
    }
}

/// Build one metadata-only seed record from a discovery hit. `record_id` is a
/// synthetic, deterministic TMDB handle (`tmdb:movie:603`). Every discovery
/// marker the query carried is echoed so the seed survives `record_matches`.
fn build_seed_record(
    hit: &TmdbHit,
    kind: TmdbKind,
    mode: DiscoveryMode,
    anime: bool,
) -> DiscoveryRecord {
    let (content_kind, seg) = match kind {
        TmdbKind::Movie => ("movie", "movie"),
        TmdbKind::Tv => ("episode", "tv"),
    };
    let mut fields = BTreeMap::new();
    // Load-bearing marker stamps (see module doc): the query's filters were the
    // mode marker, contentKind, and optionally anime.
    fields.insert(mode.marker().to_string(), "true".to_string());
    fields.insert("contentKind".to_string(), content_kind.to_string());
    fields.insert("fileType".to_string(), "video".to_string());
    if anime {
        fields.insert("anime".to_string(), "true".to_string());
    }
    // Data the consumer reads to seed its per-title search.
    fields.insert("title".to_string(), hit.title.clone());
    fields.insert("tmdbid".to_string(), hit.tmdbid.to_string());
    if let Some(y) = hit.year {
        fields.insert("movieYear".to_string(), y.to_string());
    }
    DiscoveryRecord {
        upstream_id: "prowlarr".to_string(),
        record_id: format!("tmdb:{seg}:{}", hit.tmdbid),
        fields,
    }
}

/// Resolve a discovery query to up to `max_results` catalog seed records,
/// walking TMDB pages (20/page) until the cap is reached or a page is empty.
/// Each page is a separate budget-gated TMDB call; `[]` on no mode/contentKind,
/// and an early break on the first rate-limit/miss (returning what we have).
pub(crate) async fn discover_records(
    client: &TmdbClient,
    budget: Option<&Arc<TmdbBudget>>,
    query: &GatewayQuery,
    max_results: usize,
) -> Vec<DiscoveryRecord> {
    let (Some(mode), Some(kind)) = (discovery_mode(query), discovery_kind(query)) else {
        return Vec::new();
    };
    let anime = filter_is_true(query, "anime");
    // TMDB pages at TMDB_PAGE_SIZE; walk as many pages as needed to reach
    // `max_results` seeds (meta-watch asks for ~30, i.e. 2 pages). Each page is a
    // separate, budget-gated TMDB call.
    let pages = max_results.div_ceil(TMDB_PAGE_SIZE).max(1) as u32;
    let mut out: Vec<DiscoveryRecord> = Vec::new();
    for page in 1..=pages {
        if out.len() >= max_results {
            break;
        }
        if let Some(budget) = budget {
            if matches!(
                budget
                    .acquire(Duration::from_secs(TMDB_ENRICH_WAIT_DEADLINE_SECS))
                    .await,
                Lease::DeadlineExceeded
            ) {
                break;
            }
        }
        let path_and_query = build_path_and_query(mode, kind, anime, page);
        match client.discovery_list(kind, &path_and_query).await {
            TmdbCall::Hit(hits) => {
                if hits.is_empty() {
                    break; // ran past the last catalog page
                }
                out.extend(hits.iter().map(|h| build_seed_record(h, kind, mode, anime)));
            }
            TmdbCall::RateLimited(retry) => {
                if let Some(budget) = budget {
                    budget.note_429(retry);
                }
                break;
            }
            TmdbCall::Miss => break,
        }
    }
    out.truncate(max_results);
    out
}

/// Streaming variant: resolve the catalog page and push each seed as a `Base`
/// event followed by `Done`. (A discovery list is a single HTTP call, so
/// there's no incremental benefit — this just mirrors the streaming contract.)
pub(crate) async fn discover_stream(
    client: Arc<TmdbClient>,
    budget: Option<Arc<TmdbBudget>>,
    query: GatewayQuery,
    max_results: usize,
    mut tx: Sender<GatewaySearchEvent>,
) {
    let records = discover_records(&client, budget.as_ref(), &query, max_results).await;
    for record in records {
        if tx.send(GatewaySearchEvent::Base(record)).await.is_err() {
            return; // consumer gone
        }
    }
    let _ = tx.send(GatewaySearchEvent::Done).await;
}
