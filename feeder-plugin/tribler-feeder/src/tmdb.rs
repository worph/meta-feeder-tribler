//! TMDB HTTP client + DTOs + season/episode bounds check.
//!
//! Split out of the monolithic `torznab.rs` (pure file move; no behaviour change).

use meta_feeder_sdk::common::urlencode;
use std::time::Duration;

use tracing::debug;

// -- TMDB metadata enrichment ------------------------------------------------
//
// TMDB v4 bearer-token client. Two endpoints used:
//
//   GET /3/search/{movie,tv}?query=<urlencoded>&include_adult=false[&year=<YYYY>]
//   GET <image_base>/<poster_path>
//
// The token-bearing `Authorization` header gates both. Free-tier rate
// limit is ~40 req / 10 s — we don't hit it from a dev box but it's
// worth knowing. All TMDB failures degrade silently: enrichment is a
// best-effort augmentation, never a hard dep.

/// Lightweight TMDB v4 client. Cheap to clone (the underlying
/// `reqwest::Client` is `Arc`-backed).
#[derive(Clone)]
pub struct TmdbClient {
    http: reqwest::Client,
    bearer_token: String,
    api_base: String,
    image_base: String,
}

impl TmdbClient {
    pub fn new(bearer_token: String) -> Self {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(TMDB_POSTER_TIMEOUT_SECS))
            .user_agent(USER_AGENT)
            .build()
            .expect("rustls reqwest client build infallible");
        Self {
            http,
            bearer_token,
            api_base: TMDB_API_BASE.to_string(),
            image_base: TMDB_IMAGE_BASE.to_string(),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_bases(token: String, api_base: String, image_base: String) -> Self {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(TMDB_POSTER_TIMEOUT_SECS))
            .user_agent(USER_AGENT)
            .build()
            .expect("rustls reqwest client build infallible");
        Self {
            http,
            bearer_token: token,
            api_base,
            image_base,
        }
    }

    /// Search TMDB for a movie/TV title. Returns [`TmdbCall::Hit`] with the
    /// top result, [`TmdbCall::Miss`] when there are no matches (or any
    /// transient/timeout error — best-effort degrade), or
    /// [`TmdbCall::RateLimited`] carrying the Retry-After window on a 429 so
    /// the caller can pause the shared budget globally. Wraps the whole call
    /// (including JSON decode) in [`TMDB_SEARCH_TIMEOUT_SECS`].
    pub(crate) async fn search(
        &self,
        kind: TmdbKind,
        title: &str,
        year: Option<u16>,
    ) -> TmdbCall<TmdbHit> {
        let endpoint = match kind {
            TmdbKind::Movie => "search/movie",
            TmdbKind::Tv => "search/tv",
        };
        let mut url = format!(
            "{}/{}?query={}&include_adult=false",
            self.api_base.trim_end_matches('/'),
            endpoint,
            urlencode(title),
        );
        if let Some(y) = year {
            // Movie uses `year`, TV uses `first_air_date_year`; both
            // are documented but movies accept `year` for backward-
            // compat which is what we want.
            url.push_str(&format!("&year={y}"));
        }
        let fut = self
            .http
            .get(&url)
            .bearer_auth(&self.bearer_token)
            .header("accept", "application/json")
            .send();
        let resp =
            match tokio::time::timeout(Duration::from_secs(TMDB_SEARCH_TIMEOUT_SECS), fut).await {
                Ok(Ok(r)) => r,
                _ => return TmdbCall::Miss,
            };
        if resp.status().as_u16() == 429 {
            return TmdbCall::RateLimited(parse_tmdb_retry_after(&resp));
        }
        if !resp.status().is_success() {
            debug!(
                target: "meta-share::gateway",
                upstream = "prowlarr",
                tmdb_url = %url,
                status = %resp.status(),
                "tmdb search non-2xx; degrading"
            );
            return TmdbCall::Miss;
        }
        let body: TmdbSearchResponse =
            match tokio::time::timeout(Duration::from_secs(TMDB_SEARCH_TIMEOUT_SECS), resp.json())
                .await
            {
                Ok(Ok(b)) => b,
                _ => return TmdbCall::Miss,
            };
        match body.results.into_iter().next() {
            Some(top) => TmdbCall::Hit(top.into_hit(kind)),
            None => TmdbCall::Miss,
        }
    }

    /// Build the public TMDB poster CDN URL for `poster_path` (no fetch). In the
    /// feeder model the feeder no longer fetches+stores the poster bytes; it
    /// emits this URL as a `poster_url` field and the **gateway core** seeds it
    /// into a content-addressed `poster` cid (same path as giphy/wikicommons
    /// previews). Inserts the required [`TMDB_POSTER_SIZE`] segment.
    pub(crate) fn poster_cdn_url(&self, poster_path: &str) -> String {
        format!(
            "{}/{}/{}",
            self.image_base.trim_end_matches('/'),
            TMDB_POSTER_SIZE,
            poster_path.trim_start_matches('/'),
        )
    }

    /// Fetch a poster image's raw bytes. `poster_path` is the
    /// TMDB-supplied path (typically `"/abcdef.jpg"` with leading slash).
    ///
    /// `image_base` is the bare CDN root (`https://image.tmdb.org/t/p`); TMDB
    /// requires a **size segment** (`/w500`, `/original`, …) between the root
    /// and the file or it 404s. The const comment documents this contract and
    /// this caller honors it by inserting [`TMDB_POSTER_SIZE`].
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn fetch_poster(&self, poster_path: &str) -> Option<bytes::Bytes> {
        let url = format!(
            "{}/{}/{}",
            self.image_base.trim_end_matches('/'),
            TMDB_POSTER_SIZE,
            poster_path.trim_start_matches('/'),
        );
        let resp = self.http.get(&url).send().await.ok()?;
        if !resp.status().is_success() {
            debug!(
                target: "meta-share::gateway",
                upstream = "prowlarr",
                poster_url = %url,
                status = %resp.status(),
                "tmdb poster fetch non-2xx; skipping"
            );
            return None;
        }
        resp.bytes().await.ok()
    }

    /// Fetch a TV show's authoritative structure (`number_of_seasons`
    /// plus the per-season `episode_count`s) via `GET /3/tv/{id}`. Used
    /// only to bounds-check a title-parsed season/episode — see
    /// [`season_episode_bounds`]. Wrapped in the same search timeout;
    /// any failure returns `None` so validation degrades to "accept".
    pub(crate) async fn tv_details(&self, tmdbid: u64) -> TmdbCall<TmdbTvDetails> {
        let url = format!("{}/tv/{}", self.api_base.trim_end_matches('/'), tmdbid,);
        let fut = self
            .http
            .get(&url)
            .bearer_auth(&self.bearer_token)
            .header("accept", "application/json")
            .send();
        let resp =
            match tokio::time::timeout(Duration::from_secs(TMDB_SEARCH_TIMEOUT_SECS), fut).await {
                Ok(Ok(r)) => r,
                _ => return TmdbCall::Miss,
            };
        if resp.status().as_u16() == 429 {
            return TmdbCall::RateLimited(parse_tmdb_retry_after(&resp));
        }
        if !resp.status().is_success() {
            debug!(
                target: "meta-share::gateway",
                upstream = "prowlarr",
                tmdb_url = %url,
                status = %resp.status(),
                "tmdb tv-details non-2xx; skipping season/episode validation"
            );
            return TmdbCall::Miss;
        }
        match tokio::time::timeout(Duration::from_secs(TMDB_SEARCH_TIMEOUT_SECS), resp.json()).await
        {
            Ok(Ok(d)) => TmdbCall::Hit(d),
            _ => TmdbCall::Miss,
        }
    }

    /// Fetch cross-database ids (`tvdb_id`, `imdb_id`) for a known tmdbid via
    /// `GET /3/{tv,movie}/{id}/external_ids`. The anchored torznab path needs
    /// `tvdb_id` to query indexers by `tvsearch&tvdbid=` (TMDB's id is not a
    /// standard `tvsearch` param). Same timeout/429 contract as [`tv_details`].
    pub(crate) async fn external_ids(
        &self,
        kind: TmdbKind,
        tmdbid: u64,
    ) -> TmdbCall<TmdbExternalIds> {
        let seg = match kind {
            TmdbKind::Movie => "movie",
            TmdbKind::Tv => "tv",
        };
        let url = format!(
            "{}/{}/{}/external_ids",
            self.api_base.trim_end_matches('/'),
            seg,
            tmdbid,
        );
        self.get_json(&url, "external_ids").await
    }

    /// Fetch a movie's canonical details via `GET /3/movie/{id}` (the anchored
    /// enrichment source for a known movie tmdbid — resolves title/overview/
    /// poster/year/imdb_id without a fuzzy title search). Same contract as
    /// [`tv_details`].
    pub(crate) async fn movie_details(&self, tmdbid: u64) -> TmdbCall<TmdbMovieDetails> {
        let url = format!("{}/movie/{}", self.api_base.trim_end_matches('/'), tmdbid);
        self.get_json(&url, "movie-details").await
    }

    /// Principal search via `GET /3/search/multi` — maps a bare keyword to a
    /// canonical tmdbid + media type so the gateway can then query indexers
    /// structurally (the "TMDB as the front door" identification step
    /// Sonarr/Radarr use). Returns the mixed movie/tv/person result list; the
    /// caller picks the top-N confident anchors via [`principal_top_n`].
    pub(crate) async fn search_multi(&self, query: &str) -> TmdbCall<Vec<TmdbMultiItem>> {
        let url = format!(
            "{}/search/multi?query={}&include_adult=false",
            self.api_base.trim_end_matches('/'),
            urlencode(query),
        );
        match self
            .get_json::<TmdbMultiResponse>(&url, "search/multi")
            .await
        {
            TmdbCall::Hit(r) => TmdbCall::Hit(r.results),
            TmdbCall::Miss => TmdbCall::Miss,
            TmdbCall::RateLimited(d) => TmdbCall::RateLimited(d),
        }
    }

    /// Fetch a TMDB **discovery list** (trending / popular / top-rated / a
    /// `/discover` query) and map each result to a [`TmdbHit`]. Powers the
    /// keyword-less discovery branch (see [`super::discovery`]): the caller
    /// composes `path_and_query` (e.g. `"movie/popular"`,
    /// `"trending/tv/week"`, or `"discover/tv?with_keywords=210024&sort_by=…"`)
    /// and passes the `kind` so movie-vs-TV title/date fields decode correctly.
    /// All these endpoints share the `{results: [TmdbSearchItem]}` shape, so one
    /// method covers them. Returns the page's hits, or `Miss`/`RateLimited` under
    /// the usual best-effort contract.
    pub(crate) async fn discovery_list(
        &self,
        kind: TmdbKind,
        path_and_query: &str,
    ) -> TmdbCall<Vec<TmdbHit>> {
        let url = format!(
            "{}/{}",
            self.api_base.trim_end_matches('/'),
            path_and_query.trim_start_matches('/'),
        );
        match self.get_json::<TmdbSearchResponse>(&url, "discovery").await {
            TmdbCall::Hit(r) => {
                TmdbCall::Hit(r.results.into_iter().map(|i| i.into_hit(kind)).collect())
            }
            TmdbCall::Miss => TmdbCall::Miss,
            TmdbCall::RateLimited(d) => TmdbCall::RateLimited(d),
        }
    }

    /// Shared `GET <url>` → JSON helper with the standard bearer auth, search
    /// timeout, 429→`RateLimited`, non-2xx/transient→`Miss` contract. Factored
    /// out of [`tv_details`]/[`external_ids`]/[`movie_details`] (identical
    /// modulo the decoded type and the log label).
    async fn get_json<T: serde::de::DeserializeOwned>(&self, url: &str, what: &str) -> TmdbCall<T> {
        let fut = self
            .http
            .get(url)
            .bearer_auth(&self.bearer_token)
            .header("accept", "application/json")
            .send();
        let resp =
            match tokio::time::timeout(Duration::from_secs(TMDB_SEARCH_TIMEOUT_SECS), fut).await {
                Ok(Ok(r)) => r,
                // Transport error / timeout. Logged at debug so the otherwise
                // silent best-effort degrade is diagnosable.
                Ok(Err(e)) => {
                    debug!(target: "meta-share::gateway", upstream="prowlarr", tmdb_url=%url, what, error=%e, "tmdb call send error; degrading");
                    return TmdbCall::Miss;
                }
                Err(_) => {
                    debug!(target: "meta-share::gateway", upstream="prowlarr", tmdb_url=%url, what, "tmdb call send timeout; degrading");
                    return TmdbCall::Miss;
                }
            };
        if resp.status().as_u16() == 429 {
            return TmdbCall::RateLimited(parse_tmdb_retry_after(&resp));
        }
        if !resp.status().is_success() {
            debug!(
                target: "meta-share::gateway",
                upstream = "prowlarr",
                tmdb_url = %url,
                status = %resp.status(),
                what,
                "tmdb call non-2xx; degrading"
            );
            return TmdbCall::Miss;
        }
        match tokio::time::timeout(Duration::from_secs(TMDB_SEARCH_TIMEOUT_SECS), resp.json()).await {
            Ok(Ok(d)) => TmdbCall::Hit(d),
            // A decode failure here folds into `Miss` (best-effort), but log it
            // at debug: a single malformed element fails the whole `Vec` decode,
            // which would otherwise silently disable e.g. multi-anchor.
            Ok(Err(e)) => {
                debug!(target: "meta-share::gateway", upstream="prowlarr", tmdb_url=%url, what, error=%e, "tmdb call decode error; degrading");
                TmdbCall::Miss
            }
            Err(_) => {
                debug!(target: "meta-share::gateway", upstream="prowlarr", tmdb_url=%url, what, "tmdb call decode timeout; degrading");
                TmdbCall::Miss
            }
        }
    }
}

/// Outcome of a TMDB API call. Distinguishes a 429 rate-limit (which feeds
/// `Retry-After` into the shared [`TmdbBudget`] so all enrichment pauses
/// globally) from a plain miss or transient error (best-effort degrade).
pub(crate) enum TmdbCall<T> {
    Hit(T),
    Miss,
    RateLimited(Duration),
}

/// Parse a TMDB `Retry-After` header into a pause duration, defaulting to
/// [`TMDB_RATE_LIMIT_DEFAULT_RETRY_SECS`] when absent/unparseable. Mirrors
/// the indexer-path logic in [`map_status`].
pub(crate) fn parse_tmdb_retry_after(resp: &reqwest::Response) -> Duration {
    let secs = resp
        .headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(TMDB_RATE_LIMIT_DEFAULT_RETRY_SECS);
    Duration::from_secs(secs)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TmdbKind {
    Movie,
    Tv,
}

#[derive(serde::Deserialize)]
pub(crate) struct TmdbSearchResponse {
    #[serde(default)]
    pub(crate) results: Vec<TmdbSearchItem>,
}

#[derive(serde::Deserialize)]
pub(crate) struct TmdbSearchItem {
    pub(crate) id: u64,
    #[serde(default)]
    pub(crate) title: Option<String>, // movie
    #[serde(default)]
    pub(crate) name: Option<String>, // tv
    #[serde(default)]
    pub(crate) original_title: Option<String>, // movie
    #[serde(default)]
    pub(crate) original_name: Option<String>, // tv
    #[serde(default)]
    pub(crate) overview: Option<String>,
    #[serde(default)]
    pub(crate) release_date: Option<String>, // movie, YYYY-MM-DD
    #[serde(default)]
    pub(crate) first_air_date: Option<String>, // tv,    YYYY-MM-DD
    #[serde(default)]
    pub(crate) poster_path: Option<String>,
    #[serde(default)]
    pub(crate) genre_ids: Vec<u32>,
    /// ISO 639-1 (2-letter) language the title was originally produced in.
    /// Mapped to `lang3` and used to file `original_title` under
    /// `titles/{lang3}` (METADATA_KEYS.md §3).
    #[serde(default)]
    pub(crate) original_language: Option<String>,
}

impl TmdbSearchItem {
    pub(crate) fn into_hit(self, kind: TmdbKind) -> TmdbHit {
        let title = match kind {
            TmdbKind::Movie => self.title,
            TmdbKind::Tv => self.name,
        }
        .unwrap_or_else(|| "(untitled)".to_string());
        let original_title = match kind {
            TmdbKind::Movie => self.original_title,
            TmdbKind::Tv => self.original_name,
        };
        let date = match kind {
            TmdbKind::Movie => self.release_date,
            TmdbKind::Tv => self.first_air_date,
        };
        let year = date
            .as_deref()
            .filter(|d| d.len() >= 4)
            .and_then(|d| d[..4].parse::<u16>().ok());
        TmdbHit {
            tmdbid: self.id,
            title,
            original_title,
            original_language: self.original_language,
            overview: self.overview,
            year,
            poster_path: self.poster_path,
            genre_ids: self.genre_ids,
        }
    }
}

/// Map an ISO 639-1 (2-letter) code to its ISO 639-3 (`lang3`) equivalent,
/// matching the store's convention (`eng`, `jpn`, `fra`; the 639-2/T variant
/// where B/T differ, e.g. `deu` not `ger`). Covers the languages TMDB
/// commonly returns; unknown codes return `None` so the caller skips the
/// `titles/{lang3}` write rather than persisting a non-`lang3` key.
pub(crate) fn iso639_1_to_3(code: &str) -> Option<&'static str> {
    Some(match code.to_ascii_lowercase().as_str() {
        "en" => "eng",
        "ja" => "jpn",
        "fr" => "fra",
        "de" => "deu",
        "es" => "spa",
        "it" => "ita",
        "ru" => "rus",
        "ko" => "kor",
        "zh" => "zho",
        "pt" => "por",
        "nl" => "nld",
        "sv" => "swe",
        "no" => "nor",
        "da" => "dan",
        "fi" => "fin",
        "pl" => "pol",
        "tr" => "tur",
        "ar" => "ara",
        "hi" => "hin",
        "th" => "tha",
        "vi" => "vie",
        "id" => "ind",
        "cs" => "ces",
        "el" => "ell",
        "he" => "heb",
        "hu" => "hun",
        "ro" => "ron",
        "uk" => "ukr",
        "fa" => "fas",
        "ms" => "msa",
        "tl" => "tgl",
        "ca" => "cat",
        "nb" => "nob",
        _ => return None,
    })
}

// `Clone` is required so the single-flight `Shared` future (whose `Output`
// must be `Clone`) can hand the same hit to every coalesced caller; serde so
// hits persist in the redb TMDB-search cache.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct TmdbHit {
    pub(crate) tmdbid: u64,
    pub(crate) title: String,
    #[serde(default)]
    pub(crate) original_title: Option<String>,
    #[serde(default)]
    pub(crate) original_language: Option<String>,
    #[serde(default)]
    pub(crate) overview: Option<String>,
    #[serde(default)]
    pub(crate) year: Option<u16>,
    #[serde(default)]
    pub(crate) poster_path: Option<String>,
    #[serde(default)]
    #[allow(dead_code)] // surfaced as CSV of ids; name resolution would need a second call
    pub(crate) genre_ids: Vec<u32>,
}

impl TmdbHit {
    /// TMDB `original_language` (ISO 639-1) mapped to the store's `lang3`
    /// (ISO 639-3), or `None` when the language is outside the common set or
    /// absent. Used to file `original_title` under `titles/{lang3}` (§3).
    pub(crate) fn original_lang3(&self) -> Option<&'static str> {
        iso639_1_to_3(self.original_language.as_deref()?.trim())
    }
}

/// Authoritative TV structure from `GET /3/tv/{id}`, used to bounds-check
/// a title-parsed season/episode. Only the two structural fields are
/// decoded; everything else in the (large) details payload is ignored.
// `Clone` + `Serialize` added so details persist in the redb TMDB-tvdetails
// cache and can be cheaply handed around.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub(crate) struct TmdbTvDetails {
    #[serde(default)]
    pub(crate) number_of_seasons: u32,
    /// One entry per season TMDB knows about, including season 0
    /// ("Specials"). `episode_count` is the released-episode total.
    #[serde(default)]
    pub(crate) seasons: Vec<TmdbSeasonSummary>,
    // -- Display fields ------------------------------------------------------
    // Decoded from the same `GET /3/tv/{id}` payload so an anchored TV record
    // (known tmdbid) enriches directly from its canonical entry instead of a
    // fuzzy title search. Old cache entries (written before these fields
    // existed) deserialize them to empty/None; [`TmdbTvDetails::has_display`]
    // detects that so the cached-fetch path can self-heal with one refetch.
    #[serde(default)]
    pub(crate) name: String,
    #[serde(default)]
    pub(crate) original_name: Option<String>,
    #[serde(default)]
    pub(crate) original_language: Option<String>,
    #[serde(default)]
    pub(crate) overview: Option<String>,
    #[serde(default)]
    pub(crate) first_air_date: Option<String>,
    #[serde(default)]
    pub(crate) poster_path: Option<String>,
}

impl TmdbTvDetails {
    /// True when the cached payload carries the display fields (i.e. was
    /// written by the current decoder). A `false` here on a cache hit means a
    /// pre-display entry — the caller refetches once to upgrade it.
    pub(crate) fn has_display(&self) -> bool {
        !self.name.trim().is_empty()
    }

    /// Build a [`TmdbHit`] (resolved by id) from the canonical TV entry, for
    /// the anchored enrichment path. `None` when display fields are absent.
    pub(crate) fn as_hit(&self, tmdbid: u64) -> Option<TmdbHit> {
        if !self.has_display() {
            return None;
        }
        let year = self
            .first_air_date
            .as_deref()
            .filter(|d| d.len() >= 4)
            .and_then(|d| d[..4].parse::<u16>().ok());
        Some(TmdbHit {
            tmdbid,
            title: self.name.clone(),
            original_title: self.original_name.clone(),
            original_language: self.original_language.clone(),
            overview: self.overview.clone(),
            year,
            poster_path: self.poster_path.clone(),
            genre_ids: Vec::new(),
        })
    }
}

/// TMDB cross-database ids from `GET /3/{tv,movie}/{id}/external_ids`. Only the
/// two the anchored torznab path needs are decoded.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub(crate) struct TmdbExternalIds {
    #[serde(default)]
    pub(crate) tvdb_id: Option<i64>,
    #[serde(default)]
    pub(crate) imdb_id: Option<String>,
}

/// Authoritative movie structure from `GET /3/movie/{id}`. Decodes only the
/// display fields the anchored path needs; `imdb_id` (movie details carry it
/// inline, no separate `external_ids` call) feeds the `t=movie&imdbid=` query
/// fallback.
#[derive(Debug, Clone, serde::Deserialize)]
pub(crate) struct TmdbMovieDetails {
    pub(crate) id: u64,
    #[serde(default)]
    pub(crate) title: Option<String>,
    #[serde(default)]
    pub(crate) original_title: Option<String>,
    #[serde(default)]
    pub(crate) original_language: Option<String>,
    #[serde(default)]
    pub(crate) overview: Option<String>,
    #[serde(default)]
    pub(crate) release_date: Option<String>,
    #[serde(default)]
    pub(crate) poster_path: Option<String>,
}

impl TmdbMovieDetails {
    pub(crate) fn into_hit(self) -> TmdbHit {
        let year = self
            .release_date
            .as_deref()
            .filter(|d| d.len() >= 4)
            .and_then(|d| d[..4].parse::<u16>().ok());
        TmdbHit {
            tmdbid: self.id,
            title: self.title.unwrap_or_else(|| "(untitled)".to_string()),
            original_title: self.original_title,
            original_language: self.original_language,
            overview: self.overview,
            year,
            poster_path: self.poster_path,
            genre_ids: Vec::new(),
        }
    }
}

#[derive(serde::Deserialize)]
pub(crate) struct TmdbMultiResponse {
    #[serde(default)]
    pub(crate) results: Vec<TmdbMultiItem>,
}

/// One `search/multi` result. `media_type` discriminates movie / tv / person;
/// we anchor only on movie/tv.
#[derive(Debug, Clone, serde::Deserialize)]
pub(crate) struct TmdbMultiItem {
    pub(crate) id: u64,
    #[serde(default)]
    pub(crate) media_type: Option<String>,
    #[serde(default)]
    pub(crate) title: Option<String>, // movie
    #[serde(default)]
    pub(crate) name: Option<String>, // tv
    #[serde(default)]
    pub(crate) popularity: f64,
}

impl TmdbMultiItem {
    pub(crate) fn kind(&self) -> Option<TmdbKind> {
        match self.media_type.as_deref() {
            Some("movie") => Some(TmdbKind::Movie),
            Some("tv") => Some(TmdbKind::Tv),
            _ => None,
        }
    }
    pub(crate) fn display_title(&self) -> &str {
        self.title.as_deref().or(self.name.as_deref()).unwrap_or("")
    }
}

/// Lowercase alphanumeric word tokens of `s` (separators dropped). Shared by
/// the principal-search confidence check.
pub(crate) fn norm_tokens(s: &str) -> Vec<String> {
    s.split(|c: char| !c.is_alphanumeric())
        .filter(|t| !t.is_empty())
        .map(|t| t.to_lowercase())
        .collect()
}

/// Pick up to `n` **confident** anchors from `search/multi` hits for `query`,
/// most-popular first (or `[]` → the caller falls back to today's generic
/// `t=search&q=`). For each movie/tv hit, accept it only if its title shares at
/// least half of the query's word tokens (the per-candidate relevance gate),
/// then rank the survivors by popularity and take the top `n`. This binds a
/// vague keyword like "black" to the *several* real shows TMDB returns (Black
/// Mirror, Black Butler, Black Lagoon, …) instead of hijacking the whole query
/// onto the single most-popular title, while still dropping unrelated noise
/// (e.g. "zzz qqq www" → nothing). `n == 1` recovers the old single-anchor
/// "principal_confident" behaviour (modulo the gate being applied before the
/// popularity pick rather than after).
pub(crate) fn principal_top_n<'a>(
    hits: &'a [TmdbMultiItem],
    query: &str,
    n: usize,
) -> Vec<&'a TmdbMultiItem> {
    let q_tokens = norm_tokens(query);
    if q_tokens.is_empty() || n == 0 {
        return Vec::new();
    }
    let mut cands: Vec<&TmdbMultiItem> = hits
        .iter()
        .filter(|h| h.kind().is_some())
        .filter(|h| {
            let t_tokens = norm_tokens(h.display_title());
            let overlap = q_tokens.iter().filter(|t| t_tokens.contains(t)).count();
            (overlap as f64) >= (q_tokens.len() as f64) * 0.5
        })
        .collect();
    cands.sort_by(|a, b| {
        b.popularity
            .partial_cmp(&a.popularity)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    cands.truncate(n);
    cands
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct TmdbSeasonSummary {
    #[serde(default)]
    pub(crate) season_number: i64,
    #[serde(default)]
    pub(crate) episode_count: u32,
}

/// Verdict of [`season_episode_bounds`] — how a title-parsed `(season,
/// episode)` lines up against TMDB's actual structure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SeasonEpisodeBounds {
    /// Consistent with TMDB, or unverifiable — keep the record as-is.
    Ok,
    /// The parsed season sits past TMDB's known seasons and isn't otherwise
    /// listed in `seasons[]`. The canonical case is anime where fansubbers
    /// number a later cour `S2` of a show TMDB models as a **single** season
    /// (Frieren, Jujutsu Kaisen, …). The TMDB match itself is sound — only the
    /// season number is a naming-convention artifact — so the caller keeps the
    /// record but strips the misleading `season` (the frontend buckets
    /// season-less episodes into a flat "Episodes" list).
    SeasonOverflow,
    /// A *positive* contradiction the season-overflow allowance can't explain:
    /// a negative season, or an episode beyond a **matched** season's
    /// `episode_count`. The title parse is wrong — the caller drops the record.
    Contradiction,
}

/// Pure bounds check: is a title-parsed `(season, episode)` consistent
/// with what TMDB says the show actually has? Deliberately lenient —
/// it only returns a verdict other than [`SeasonEpisodeBounds::Ok`] on a
/// *positive* divergence, so anything ambiguous or unverifiable is accepted:
///
/// - No parsed season → [`Ok`]. Episode-only titles (absolute-numbered
///   anime, trailing `- 117`) are legitimately unbounded; the season-
///   based bound doesn't apply.
/// - TMDB gave us no structure (`number_of_seasons == 0` and no
///   `seasons[]`) → [`Ok`]; we can't contradict what we don't know.
/// - Season exceeds `number_of_seasons` AND isn't otherwise listed in
///   `seasons[]` → [`SeasonOverflow`] (the `S2`-of-a-1-TMDB-season anime
///   case; keep but strip `season`).
/// - Negative season → [`Contradiction`].
/// - Episode exceeds the matched season's `episode_count` (when that
///   count is known) → [`Contradiction`]. This only fires for explicit
///   `SxxEyy` titles, the exact case where the per-season bound holds.
///
/// [`Ok`]: SeasonEpisodeBounds::Ok
/// [`SeasonOverflow`]: SeasonEpisodeBounds::SeasonOverflow
/// [`Contradiction`]: SeasonEpisodeBounds::Contradiction
pub(crate) fn season_episode_bounds(
    details: &TmdbTvDetails,
    season: Option<i64>,
    episode: Option<i64>,
) -> SeasonEpisodeBounds {
    use SeasonEpisodeBounds::*;
    let season = match season {
        Some(s) => s,
        None => return Ok,
    };
    if details.number_of_seasons == 0 && details.seasons.is_empty() {
        return Ok;
    }
    if season < 0 {
        return Contradiction;
    }
    let matched = details.seasons.iter().find(|s| s.season_number == season);
    if season > details.number_of_seasons as i64 && matched.is_none() {
        return SeasonOverflow;
    }
    if let (Some(ep), Some(sm)) = (episode, matched) {
        if sm.episode_count > 0 && ep > sm.episode_count as i64 {
            return Contradiction;
        }
    }
    Ok
}

/// True when `episode` is a valid episode of `season` per TMDB's per-season
/// `episode_count` (the season must be listed and the count known). Used to
/// decide whether a bare title number is an in-season episode before falling
/// back to absolute-number interpretation.
pub(crate) fn episode_in_season(details: &TmdbTvDetails, season: i64, episode: i64) -> bool {
    details
        .seasons
        .iter()
        .find(|s| s.season_number == season)
        .map(|s| s.episode_count > 0 && episode >= 1 && episode <= s.episode_count as i64)
        .unwrap_or(false)
}

use crate::consts::*;

#[cfg(test)]
mod season_bounds_tests {
    use super::*;

    fn details(num_seasons: u32, seasons: &[(i64, u32)]) -> TmdbTvDetails {
        TmdbTvDetails {
            number_of_seasons: num_seasons,
            seasons: seasons
                .iter()
                .map(|&(season_number, episode_count)| TmdbSeasonSummary {
                    season_number,
                    episode_count,
                })
                .collect(),
            ..Default::default()
        }
    }

    #[test]
    fn bounds_ok_within_season() {
        let d = details(2, &[(1, 26), (2, 24)]);
        assert_eq!(season_episode_bounds(&d, Some(1), Some(5)), SeasonEpisodeBounds::Ok);
        assert_eq!(season_episode_bounds(&d, Some(2), Some(24)), SeasonEpisodeBounds::Ok);
    }

    #[test]
    fn bounds_no_season_or_unknown_structure_is_ok() {
        let d = details(2, &[(1, 26), (2, 24)]);
        assert_eq!(season_episode_bounds(&d, None, Some(999)), SeasonEpisodeBounds::Ok);
        let unknown = details(0, &[]);
        assert_eq!(season_episode_bounds(&unknown, Some(5), Some(40)), SeasonEpisodeBounds::Ok);
    }

    #[test]
    fn bounds_season_past_total_is_overflow() {
        // Fansub "S2" of a show TMDB models as a single season → strip-and-keep.
        let d = details(1, &[(1, 28)]);
        assert_eq!(season_episode_bounds(&d, Some(2), Some(3)), SeasonEpisodeBounds::SeasonOverflow);
    }

    #[test]
    fn bounds_episode_past_matched_season_or_negative_is_contradiction() {
        // "S1E27" on a 4-season show → contradiction. The enrich path drops the
        // record (we no longer remap absolute numbers onto seasons; a genuine
        // absolute release carries no season token and never reaches here).
        let d = details(4, &[(1, 26), (2, 13), (3, 13), (4, 12)]);
        assert_eq!(season_episode_bounds(&d, Some(1), Some(27)), SeasonEpisodeBounds::Contradiction);
        assert_eq!(season_episode_bounds(&d, Some(-1), None), SeasonEpisodeBounds::Contradiction);
    }
}
