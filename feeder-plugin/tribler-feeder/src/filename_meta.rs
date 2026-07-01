//! Title-tag parsing moved to the shared `meta_feeder_sdk::filename_meta`
//! (de-duplicated — was copied verbatim in tribler + torznab feeders). This
//! re-export keeps the existing `crate::filename_meta::extract_*` call sites
//! working unchanged. New code can use `meta_feeder_sdk::filename_meta` directly.
pub use meta_feeder_sdk::filename_meta::*;
