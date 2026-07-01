//! Pure torrent-title-cleaning logic.
//!
//! Split out of the monolithic `torznab.rs` (pure file move; no behaviour change).

use crate::filename_meta;
use crate::tmdb::TmdbKind;

// -- Title cleaner -----------------------------------------------------------
//
// Torrent titles encode a lot of release-metadata noise around the
// actual movie/show name: `[Group] Show - 117 [1080p].mkv` is typical.
// The cleaner strips that noise so the residue is something TMDB's
// search can match. Heuristic, not perfect.

/// Cleaned-up title plus extracted hints.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CleanedTitle {
    /// Best-effort name suitable for TMDB query.
    pub(crate) title: String,
    /// 4-digit year if one was found in the raw title (preserved
    /// before stripping). Helps TMDB disambiguate remakes.
    pub(crate) year: Option<u16>,
    /// `Tv` when the title carries an episode marker (`S01E02`,
    /// trailing `- 117`, …); `Movie` otherwise. The kind hint flips
    /// which TMDB endpoint we hit.
    pub(crate) kind: TmdbKind,
}

/// Junk tokens to drop from torrent titles when cleaning for TMDB
/// search. ASCII case-insensitive match against whitespace-split
/// tokens AND dot-separated tokens (`a.b.c` → ["a","b","c"]).
// Kept in parity with the core torznab plugin; unused in the current
// title-cleaning path (the active path uses `filename_meta`).
#[allow(dead_code)]
pub(crate) const JUNK_TOKENS: &[&str] = &[
    // resolutions
    "1080p",
    "720p",
    "480p",
    "2160p",
    "4k",
    "uhd",
    "hd",
    // sources
    "bluray",
    "bdrip",
    "bdremux",
    "remux",
    "web-dl",
    "webdl",
    "webrip",
    "hdtv",
    "dvdrip",
    "hdrip",
    "dsnp",
    "amzn",
    "atvp",
    "nf",
    "hulu",
    "adn",
    // codecs
    "x264",
    "x265",
    "h264",
    "h265",
    "hevc",
    "avc",
    "10bit",
    "10-bit",
    "8bit",
    "8-bit",
    // audio
    "aac",
    "ac3",
    "flac",
    "dts",
    "dd5",
    "ddp",
    "ddp5",
    "ddp7",
    "atmos",
    "mp3",
    "opus",
    "5.1",
    "7.1",
    "2.0",
    // subs / language
    "multi",
    "multisub",
    "multi-sub",
    "multi-subs",
    "vostfr",
    "dual-audio",
    "dualaudio",
    "sub",
    "subs",
    "subbed",
    // common file extensions
    "mkv",
    "mp4",
    "avi",
    "ts",
    "m4v",
    // misc release tags
    "proper",
    "repack",
    "internal",
    "limited",
    "extended",
    "uncut",
    "directors",
    "remastered",
    "anniversary",
    "ova",
    "oad",
    "ona",
    "movie",
];

/// Clean a raw torrent title into a TMDB-searchable shape. Pipeline:
///
/// 1. Strip every `[...]` and `(...)` segment (release groups, tags).
/// 2. Strip trailing file extension.
/// 3. Detect SxxEyy markers (→ TV) and trailing ` - N` episode numbers (→ TV).
/// 4. Extract a year if one is visible.
/// 5. Split into tokens (whitespace + dots + hyphens treated as
///    separators), drop junk keywords (case-insensitive), drop
///    lone-year tokens after extraction, drop empty tokens.
/// 6. Re-assemble the survivors with single-space separators.
pub(crate) fn clean_torrent_title(raw: &str) -> CleanedTitle {
    let kind = detect_kind(&strip_file_extension(&strip_bracketed(raw)));
    // Year from the RAW string — `strip_bracketed` would eat a `(2023)`.
    let year = extract_year(raw);
    // Title via the *tested* filename-tools `cleanTitle` pipeline rather than a
    // second, weaker hand-rolled cleaner. This normalizes `_`/`.` separators,
    // strips the full release/quality keyword list, drops everything after the
    // first " - " delimiter, and removes season-word / episode-range markers —
    // all the cases the old cleaner left in, which made TMDB matching fail.
    let title = filename_meta::clean_title(raw);
    CleanedTitle { title, year, kind }
}

pub(crate) fn strip_bracketed(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut depth_sq = 0i32;
    let mut depth_pa = 0i32;
    for c in s.chars() {
        match c {
            '[' => depth_sq += 1,
            ']' => {
                if depth_sq > 0 {
                    depth_sq -= 1;
                    continue;
                }
            }
            '(' => depth_pa += 1,
            ')' => {
                if depth_pa > 0 {
                    depth_pa -= 1;
                    continue;
                }
            }
            _ if depth_sq > 0 || depth_pa > 0 => continue,
            _ => {}
        }
        if c != '[' && c != '(' {
            out.push(c);
        }
    }
    out
}

pub(crate) fn strip_file_extension(s: &str) -> String {
    let trimmed = s.trim();
    for ext in ["mkv", "mp4", "avi", "ts", "m4v", "webm"] {
        let suffix = format!(".{ext}");
        if trimmed.to_ascii_lowercase().ends_with(suffix.as_str()) {
            return trimmed[..trimmed.len() - suffix.len()].to_string();
        }
    }
    trimmed.to_string()
}

pub(crate) fn detect_kind(s: &str) -> TmdbKind {
    // Walk tokens (anything not alphanumeric is a separator —
    // matches dots, spaces, brackets, hyphens). A token of shape
    // S\d{1,2}, SxxEyy, EpNN, or `Season` triggers TV.
    let lower = s.to_ascii_lowercase();
    for tok in lower.split(|c: char| !c.is_ascii_alphanumeric()) {
        if tok.is_empty() {
            continue;
        }
        if tok == "season" {
            return TmdbKind::Tv;
        }
        // SxxEyy or Sxx (season-only).
        if let Some(rest) = tok.strip_prefix('s') {
            let digits_then = rest.chars().take_while(|c| c.is_ascii_digit()).count();
            if (1..=2).contains(&digits_then) {
                let after = &rest[digits_then..];
                if after.is_empty() {
                    return TmdbKind::Tv;
                }
                if after.starts_with('e')
                    && after[1..].chars().all(|c| c.is_ascii_digit())
                    && !after[1..].is_empty()
                {
                    return TmdbKind::Tv;
                }
            }
        }
    }
    // Trailing " - N" / " - NNN" episode-number pattern
    // (common Nyaa shape: `[Group] Show - 117 [1080p].mkv`).
    if has_trailing_episode_number(s) {
        return TmdbKind::Tv;
    }
    TmdbKind::Movie
}

pub(crate) fn has_trailing_episode_number(s: &str) -> bool {
    let trimmed = s.trim();
    // Look for ` - <digits>` near the end of the string.
    if let Some(idx) = trimmed.rfind(" - ") {
        let tail = trimmed[idx + 3..].trim();
        // Tail is digits only, possibly followed by a `(...)` segment
        // (already stripped) or `v<digit>` version suffix.
        let mut digits = 0;
        for c in tail.chars() {
            if c.is_ascii_digit() {
                digits += 1;
            } else if (c == 'v' || c == 'V') && digits > 0 {
                break;
            } else {
                break;
            }
        }
        return digits > 0 && digits <= 4;
    }
    false
}

#[allow(dead_code)] // parity with core torznab; unused in the active title path
pub(crate) fn strip_episode_markers(s: &str) -> String {
    let trimmed = s.trim();
    // Drop a trailing ` - N` / ` - NN` / ` - NNN` / ` - NNNN[vN]`.
    if let Some(idx) = trimmed.rfind(" - ") {
        let tail = &trimmed[idx + 3..];
        if tail
            .chars()
            .all(|c| c.is_ascii_digit() || c == 'v' || c == 'V')
            && tail.chars().any(|c| c.is_ascii_digit())
        {
            return trimmed[..idx].trim().to_string();
        }
    }
    // Drop SxxEyy and trailing season-only markers.
    let mut out = String::with_capacity(trimmed.len());
    let mut chars = trimmed.chars().peekable();
    while let Some(c) = chars.next() {
        if (c == 'S' || c == 's') && chars.peek().is_some_and(|n| n.is_ascii_digit()) {
            // Try to consume Sxx[Eyy].
            let mut probe = String::new();
            probe.push(c);
            while let Some(&n) = chars.peek() {
                if n.is_ascii_digit() || n == 'E' || n == 'e' {
                    probe.push(n);
                    chars.next();
                } else {
                    break;
                }
            }
            // Validate shape: starts with S, has digits, optional E + digits.
            let lower = probe.to_ascii_lowercase();
            let looks_like_marker = lower.starts_with('s')
                && lower[1..].chars().take(2).all(|c| c.is_ascii_digit())
                && (lower.len() == 3 || lower.contains('e'));
            if looks_like_marker {
                continue;
            }
            // Not a marker — push back as best we can.
            out.push_str(&probe);
        } else {
            out.push(c);
        }
    }
    out.trim().to_string()
}

pub(crate) fn extract_year(s: &str) -> Option<u16> {
    // Find a 4-digit standalone token between 1900 and 2099.
    for tok in s.split(|c: char| !c.is_ascii_alphanumeric()) {
        if tok.len() == 4 && tok.chars().all(|c| c.is_ascii_digit()) {
            if let Ok(y) = tok.parse::<u16>() {
                if (1900..=2099).contains(&y) {
                    return Some(y);
                }
            }
        }
    }
    None
}

#[allow(dead_code)] // parity with core torznab; unused in the active title path
pub(crate) fn tokenize_and_filter(s: &str, extracted_year: Option<u16>) -> String {
    let extracted_year_str = extracted_year.map(|y| y.to_string());
    let mut kept: Vec<String> = Vec::new();
    for tok in s.split(|c: char| !c.is_ascii_alphanumeric() && c != '\'') {
        let trimmed = tok.trim_matches(|c: char| !c.is_ascii_alphanumeric() && c != '\'');
        if trimmed.is_empty() {
            continue;
        }
        let lower = trimmed.to_ascii_lowercase();
        if JUNK_TOKENS.iter().any(|j| *j == lower) {
            continue;
        }
        // Drop the year token if we extracted it.
        if let Some(y) = extracted_year_str.as_deref() {
            if trimmed == y {
                continue;
            }
        }
        // Drop pure-digit tokens longer than 2 chars (likely episode
        // numbers that slipped through, or stray years not matching).
        if trimmed.len() > 2 && trimmed.chars().all(|c| c.is_ascii_digit()) {
            continue;
        }
        kept.push(trimmed.to_string());
    }
    kept.join(" ").trim().to_string()
}
