# meta-feeder-tribler

**Tribler feeder sidecar** for
[MetaMesh](https://github.com/worph/meta-gateway) — bridges the
**decentralized [Tribler](https://www.tribler.org/) network** (its IPv8
peer-to-peer overlay search) into a meta-gateway as a search source.

Unlike a Torznab/Prowlarr feeder that talks to centralized indexer sites,
Tribler has **no central index**: it runs its own anonymous overlay and each
peer contributes the torrents it has seen. This feeder does not reimplement
that overlay — it runs a **headless Tribler core as a sidecar container**
(`tribler-instance`) and queries its REST API, exactly the way a Torznab feeder
treats an external indexer. It bridges Tribler's *content corpus* into the
mesh so those torrents become discoverable and playable via meta-share.

This feeder was **split out of** [`meta-feeder-torrent`](../meta-feeder-torrent)
so that Tribler is a **single-plugin** feeder. A feeder exposes one `/config`
surface; a multi-plugin feeder (Torznab + Tribler in one binary) can only route
that surface to one plugin, so its dashboard config panel could not resolve to
the Tribler schema. Splitting gives Tribler its own clean config card.

## Metadata-only

This is the **light** feeder. It resolves a Tribler search hit's infohash
directly into a `btih-v1-file` CID and **advertises** the record — it does
**not** download or seed the bytes. There is no `librqbit` here: file lists
come from the Tribler core's own `torrentinfo` metainfo response, not from a
BitTorrent fetch. So the cold Docker build is much lighter than the torrent
feeder's, and full-file fetch/seed is left to meta-share's on-demand fetcher
(the feeder synthesizes magnets with a set of well-known public trackers so
that fetcher can find peers quickly, since Tribler torrents are often DHT-only).

It carries a **copy** of the source-agnostic "torrent-core" TMDB
catalog-discovery + enrichment stack (`tmdb`, `tmdb_budget`, `discovery`,
`enrich`, `title`, `consts`, `filename_meta`). That stack cannot live in a
container plugin — a container plugin enriches a *known* CID; it cannot answer
"what's popular" for the home-row browse — so the browse/catalog primitive is
inherently in-process and shared verbatim by every torrent source. Like every
feeder it stays **meta-core-free at the transport layer and blockstore-free**:
it finds records and enriches them; publishing resolved records + TMDB posters
goes to the meta-core configured in the dashboard.

## Role in MetaMesh

A feeder is a stateless HTTP sidecar. The gateway registers it as a remote
feeder plugin pointing at its `/` and drives the SDK contract
([`meta_feeder_sdk::serve_feeders`](crates/meta-feeder-sdk/src/serve.rs)):

| Endpoint | Purpose |
|----------|---------|
| `GET /manifest` | feeder identity + served types (`*` / `*` — Tribler is free-text, any query can return anything) |
| `GET /health` | liveness (`Degraded` until `configure()` runs) |
| `POST /query`, `POST /query_stream` | structured / streaming search over the Tribler local + remote (IPv8) overlay |
| `POST /compute` | outcome compute + TMDB enrichment |
| `GET /fetch/:upstream_id/:record_id`, `GET /blob/:upstream_id/:cid` | SDK byte routes (metadata-only here — no local bytes to serve) |
| `POST /enrich/callback` | async enrichment callback sink |
| `GET /config`, `GET /config/schema`, `GET\|PUT /config/values` | schema-driven config UI + API |

## Configuration

All Tribler config is **dashboard config only** — it lives in the persisted
`config.json` (written via the gateway dashboard) and has **no env var**. This
is the MetaMesh rule: a field in the web-UI config schema must not also be an
env seed (config from JSON / web UI is the single source of truth).

Configured on the plugin's card in the gateway dashboard:

| Field | Notes |
|-------|-------|
| `meta_core_url` | meta-core this feeder publishes resolved records + TMDB posters to (e.g. `http://metacore-app:9000`). Blank → publish soft-skips. |
| `sidecar_url` | Tribler core REST base (default `http://tribler-instance:8085`). |
| `api_key` | Tribler REST `X-Api-Key` (secret); the dev sidecar uses `changeme`. Blank → no auth header. |
| `tmdb_api_key` | TMDB v3 key / v4 bearer (secret) for poster / overview / tmdbid enrichment. Blank → TMDB enrichment soft-skips. |
| `tmdb_language` | TMDB metadata language tag (e.g. `en-US`, `fr-FR`, `ja-JP`). Blank → `en-US`. |

Config changes take effect on the next feeder restart (no hot reload). Open the
Tribler web UI via the "↗ Tribler UI" link on the plugin's card — it opens the
gated `/ui/` behind single sign-on (put `?key=` **after** the `#`; the UI is a
HashRouter).

Env vars are **infra only**:

| Env var | Default | Notes |
|---------|---------|-------|
| `META_FEEDER_HTTP_LISTEN` | `0.0.0.0:8080` | HTTP listen address |
| `META_FEEDER_STATE_DIR` | `/data/meta-feeder` | redb midhash cache + state |
| `RUST_LOG` | `info` | tracing filter |

## Image

```
ghcr.io/worph/meta-feeder-tribler
```

Exposes `8080`. Built and pushed by CI on every push to `main` (the moving
`main` tag) and on `v*` tags (semver tags). Metadata-only (no `librqbit`), so
the cold build is lighter than `meta-feeder-torrent`.

## Build locally

The build context is the **repo root** (the Cargo workspace) so the vendored
`meta-feeder-sdk` path dependency resolves:

```bash
docker build -f feeder-plugin/tribler-feeder/Dockerfile -t ghcr.io/worph/meta-feeder-tribler:dev .
```

## Repo layout

This repo is a self-contained Cargo workspace vendored out of the
`meta-gateway` monorepo:

```
Cargo.toml                      # workspace: members = crates/*, feeder-plugin/*
                                # (keeps the core2 [patch.crates-io] safety net)
crates/meta-feeder-sdk/         # vendored shared feeder SDK
feeder-plugin/tribler-feeder/   # this feeder's crate + Dockerfile
    src/tribler/                # the Tribler source (IPv8-overlay REST client)
    src/{tmdb,discovery,enrich,title,filename_meta,...}.rs
                                # copied source-agnostic "torrent-core" catalog stack
```

> The workspace keeps the `core2` `[patch.crates-io]` override (a yanked
> transitive dep) as a safety net, matching the `meta-gateway` workspace.

Upstream source of truth for the SDK and the Tribler source is
[`worph/meta-gateway`](https://github.com/worph/meta-gateway); changes there are
vendored back into this repo.
