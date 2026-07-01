//! `tribler-feeder` library surface. The binary (`main.rs`) wraps the Tribler
//! plugin in [`meta_feeder_sdk::serve_feeders`].
//!
//! This crate was split out of `meta-feeder-torrent` (the unified torrent
//! feeder) so tribler is a **single-plugin** feeder — its dashboard config
//! panel resolves to the tribler schema instead of colliding with prowlarr on a
//! shared feeder's single `/config` surface.
//!
//! It carries a **copy** of the source-agnostic "torrent-core" TMDB
//! catalog-discovery stack (`tmdb`, `tmdb_budget`, `discovery`, `enrich`,
//! `title`, `consts`, `filename_meta`). That stack cannot live in a container
//! plugin — a container plugin enriches a known CID, it can't answer "what's
//! popular"; the browse/catalog primitive is inherently in-process and shared by
//! every torrent source. The librqbit BitTorrent full-file fetch (`bt.rs` in
//! meta-feeder-torrent) is **not** copied here: the tribler source enumerates a
//! torrent's files from the Tribler core's metainfo response (`tribler::metainfo`),
//! never via librqbit.

// The copied torrent-core modules carry the same clippy lints allowed in the
// meta-feeder-torrent origin, so this crate meets a `-D warnings` bar without
// diverging the copies from their twin. `dead_code` is allowed because the
// copied TMDB stack is source-agnostic: the tribler source drives catalog
// discovery + streaming enrichment, but not the indexer-result reconciliation
// helpers (norm_tokens / principal_top_n / episode_in_season / the TMDB
// multi-search DTOs) that only the torznab source in meta-feeder-torrent calls.
// Keeping them verbatim (rather than pruning) keeps the copies diff-clean
// against their twin.
#![allow(
    dead_code,
    clippy::type_complexity,
    clippy::unnecessary_get_then_check,
    clippy::needless_borrow,
    clippy::needless_borrows_for_generic_args,
    clippy::manual_find,
    clippy::if_same_then_else,
    clippy::items_after_test_module
)]

// Source-agnostic torrent pipeline (copied "torrent-core"): TMDB client +
// budget, keyword-less catalog discovery, release-title cleaning, and the
// in-Rust TMDB enrichment reconciliation used on the discovery/search stream.
pub mod consts;
pub mod discovery;
pub mod enrich;
pub mod filename_meta;
pub mod title;
pub mod tmdb;
pub mod tmdb_budget;

// The tribler source itself (IPv8-overlay client of a headless Tribler core).
pub mod tribler;
