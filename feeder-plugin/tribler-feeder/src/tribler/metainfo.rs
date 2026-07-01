//! Tribler metainfo (file-list) fetch + parsing for the metainfo fan-out
//! (PR2 step 3). Turns one torrent infohash into the list of files it contains,
//! so `compute_outcomes` can emit one `btih-v1-file` CID per video file plus a
//! whole-torrent pack CID.
//!
//! Source: Tribler core `POST /api/torrentinfo/uri` with a JSON body
//! `{"uri": "<magnet>"}` (the `uri` query param alone 500s — the handler reads
//! the body as JSON). Response shape (`GetMetainfoResponse`):
//!
//! ```json
//! { "files": [ {"index": 0, "name": "Show.S01E01.mkv", "size": 123}, … ],
//!   "name": "Show Complete Season 1", "description": "", … }
//! ```
//!
//! The DHT metainfo resolve is **slow and may fail** (no peers / no metadata);
//! callers treat `None` as "couldn't enumerate, fall back to the single-file
//! record" — a fan-out failure must never fail the resolve.

use serde::{Deserialize, Serialize};

/// One file inside a torrent. `index` is the zero-based position in the info
/// dict's file list — i.e. the `btih-v1-file` CID's file index. Serialized into
/// the SDK `filelist` redb cache so fan-out survives across `compute_outcomes`
/// calls without re-hitting the DHT.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TorrentFile {
    pub index: usize,
    /// File path/name inside the torrent (forward-slash joined).
    pub name: String,
    /// File length in bytes.
    pub size: u64,
}

/// Video file extensions we fan out to (lowercase, no dot). Matches the torznab
/// feeder's `VIDEO_EXTENSIONS` so both feeders agree on what "a video file" is.
const VIDEO_EXTENSIONS: &[&str] = &[
    "mkv", "mp4", "avi", "m4v", "mov", "webm", "mpg", "mpeg", "wmv", "flv", "ogv", "ts", "mts",
    "m2ts",
];

/// True if `name`'s extension is a known video container.
pub fn looks_like_video(name: &str) -> bool {
    name.rsplit('.')
        .next()
        .map(|ext| {
            let lower = ext.to_ascii_lowercase();
            VIDEO_EXTENSIONS.iter().any(|e| *e == lower)
        })
        .unwrap_or(false)
}

/// Tribler `GetMetainfoResponse.files[]` entry. Tolerant field naming: Tribler
/// uses `name`/`size`; we also accept `path`/`length` in case a tag differs.
#[derive(Debug, Default, Deserialize)]
struct MiniFileInfo {
    #[serde(default)]
    index: Option<usize>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    size: Option<u64>,
    #[serde(default)]
    length: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
struct MetainfoResponse {
    #[serde(default)]
    files: Vec<MiniFileInfo>,
}

/// Parse a Tribler `GetMetainfoResponse` body into [`TorrentFile`]s, filling a
/// fallback `index` from array position when absent.
pub fn parse_metainfo(body: &str) -> Vec<TorrentFile> {
    let Ok(resp) = serde_json::from_str::<MetainfoResponse>(body) else {
        return Vec::new();
    };
    resp.files
        .into_iter()
        .enumerate()
        .filter_map(|(pos, f)| {
            let name = f.name.or(f.path)?;
            if name.trim().is_empty() {
                return None;
            }
            Some(TorrentFile {
                index: f.index.unwrap_or(pos),
                name,
                size: f.size.or(f.length).unwrap_or(0),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn looks_like_video_matches_extensions() {
        assert!(looks_like_video("Show.S01E01.1080p.mkv"));
        assert!(looks_like_video("movie.MP4"));
        assert!(!looks_like_video("readme.txt"));
        assert!(!looks_like_video("Pressrelease.pdf"));
        assert!(!looks_like_video("noext"));
    }

    #[test]
    fn parse_metainfo_real_tribler_shape() {
        // Captured from the live Tribler `/api/torrentinfo/uri` response.
        let body = r#"{"files":[
            {"index":0,"name":"Big_Buck_Bunny_1080p.avi","size":928670754},
            {"index":1,"name":"PROMOTE.txt","size":5008},
            {"index":2,"name":"Pressrelease.pdf","size":3456234},
            {"index":3,"name":"license.txt","size":180}
        ],"name":"Big Buck Bunny","description":""}"#;
        let files = parse_metainfo(body);
        assert_eq!(files.len(), 4);
        assert_eq!(files[0].index, 0);
        assert_eq!(files[0].name, "Big_Buck_Bunny_1080p.avi");
        assert_eq!(files[0].size, 928670754);
        let videos: Vec<_> = files.iter().filter(|f| looks_like_video(&f.name)).collect();
        assert_eq!(videos.len(), 1, "only the .avi is video");
    }

    #[test]
    fn parse_metainfo_index_fallback_to_position() {
        let body = r#"{"files":[{"name":"a.mkv","size":1},{"name":"b.mkv","size":2}]}"#;
        let files = parse_metainfo(body);
        assert_eq!(files[0].index, 0);
        assert_eq!(files[1].index, 1);
    }

    #[test]
    fn parse_metainfo_empty_or_garbage() {
        assert!(parse_metainfo("not json").is_empty());
        assert!(parse_metainfo(r#"{"files":[]}"#).is_empty());
    }
}
