# redfolder

[![Crates.io](https://img.shields.io/crates/v/redfolder.svg)](https://crates.io/crates/redfolder)
[![Docs.rs](https://docs.rs/redfolder/badge.svg)](https://docs.rs/redfolder)
[![CI](https://github.com/0xbarss/redfolder/actions/workflows/ci.yml/badge.svg)](https://github.com/0xbarss/redfolder/actions/workflows/ci.yml)
[![Rust](https://img.shields.io/badge/rust-1.88%2B-orange.svg)](https://www.rust-lang.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Author](https://img.shields.io/badge/author-0xbarss-purple.svg)](https://github.com/0xbarss)

A reliable, asynchronous economic calendar client and event-driven trading blackout engine for Rust algorithmic trading systems, quantitative funds, and prop firm challenges.

---

## Table of Contents

- [Why redfolder?](#why-redfolder)
- [Architecture & Design](#architecture--design)
  - [System Flow](#system-flow)
  - [Event-Driven Processing Model](#event-driven-processing-model)
  - [Dynamic Window Clustering & Merging](#dynamic-window-clustering--merging)
  - [Weekend Market Close Curfew Engine](#weekend-market-close-curfew-engine)
  - [Offline Durability & Rate Limit Resilience](#offline-durability--rate-limit-resilience)
  - [Data Sources, Compliance & Multi-Feed Fallback](#data-sources-compliance--multi-feed-fallback)
  - [Response Bounding & Memory Hardening](#response-bounding--memory-hardening)
  - [Cryptographic Disk Integrity & File Permissions](#cryptographic-disk-integrity--file-permissions)
  - [Retry Latency Ceilings & Execution SLAs](#retry-latency-ceilings--execution-slas)
  - [Fail-Safe Risk Policies (Fail-Open vs. Fail-Closed)](#fail-safe-risk-policies-fail-open-vs-fail-closed)
  - [Data Freshness & Provenance](#data-freshness--provenance)
  - [Production Deployment & Verification](#production-deployment--verification)
- [Key Features](#key-features)
- [Repository Structure](#repository-structure)
- [Installation](#installation)
- [Usage & Code Examples](#usage--code-examples)
  - [1. Quickstart: Checking Active Blackout Status](#1-quickstart-checking-active-blackout-status)
  - [2. Event-Driven Strategy Integration (Tokio Streams)](#2-event-driven-strategy-integration-tokio-streams)
  - [3. Pre-Blackout Advance Warnings (Order Cancellation Guard)](#3-pre-blackout-advance-warnings-order-cancellation-guard)
  - [4. Prop Firm Challenge Preset (FTMO, FundedNext, The5ers)](#4-prop-firm-challenge-preset-ftmo-fundednext-the5ers)
  - [5. Custom Trait-Based Event Listener Callback](#5-custom-trait-based-event-listener-callback)
  - [6. Multi-Worker Symbol Isolation](#6-multi-worker-symbol-isolation)
  - [7. Terminal Command-Line Interface (CLI)](#7-terminal-command-line-interface-cli)
- [API Reference](#api-reference)
  - [RedFolderService](#redfolderservice)
  - [BlackoutEngine](#blackoutengine)
  - [CalendarClient](#calendarclient)
  - [RedFolderConfig](#redfolderconfig)
  - [Types & Domain Models](#types--domain-models)
- [Testing & Quality Assurance](#testing--quality-assurance)
  - [Failure Modes & Edge Case Matrix](#failure-modes--edge-case-matrix)
- [Troubleshooting & FAQ](#troubleshooting--faq)
- [Author & Contributions](#author--contributions)
- [License & Disclaimer](#license--disclaimer)

---

## Why redfolder?

In quantitative finance and retail foreign exchange trading, high-impact macroeconomic releases (CPI, Non-Farm Payrolls, FOMC rate decisions, central bank press conferences) are universally referred to as **"Red Folder"** events. 

During these releases, liquidity providers widen their spreads by 10x–50x, depth of book vanishes, and execution slippage drastically increases. Furthermore, major prop trading firms (**FTMO, FundedNext, The5ers, MFF**) enforce strict compliance rules that disqualify accounts holding or executing trades within 2–5 minutes of high-impact releases.

Building economic news protection from scratch usually results in brittle HTTP pollers that fail during rate limits or fail to cluster back-to-back releases. `redfolder` solves this with a robust, in-memory interval engine:

1. **Direct Calendar Ingestion**: Automatically pulls weekly schedules from ForexFactory / FairEconomy feeds.
2. **Dynamic Window Merging**: Automatically aggregates tightly spaced releases into unified blackout intervals.
3. **Event-Driven Broadcast Bus**: Pushes typed events (`BlackoutWarning`, `BlackoutStarted`, `BlackoutEnded`) directly into your strategy loop via Tokio broadcast channels or worker-specific streams.
4. **Offline Durability**: Gracefully falls back to local disk cache during network partitions or HTTP 429 rate limits.

---

## Architecture & Design

### System Flow

```text
┌───────────────────────────────────────────────────────────┐
│               ForexFactory / FairEconomy API              │
└─────────────────────────────┬─────────────────────────────┘
                              │ HTTP / JSON (Auto-cached)
┌─────────────────────────────▼─────────────────────────────┐
│                       CalendarClient                      │
│     - Metadata & Event-Count Validation                   │
│     - Bounded Stale Cache Fallback Policy                 │
│     - Source Timezone & DST Support (chrono-tz)           │
└─────────────────────────────┬─────────────────────────────┘
                              │ RawCalendarEvents
┌─────────────────────────────▼─────────────────────────────┐
│                       BlackoutEngine                      │
│        - Explicit EventTiming (Exact / All-Day / Tentative)│
│        - Overlapping Window Merge (Gap Threshold)         │
│        - Half-Open Interval Semantics: [start, end)       │
│        - Weekend Curfew Injection (Short / Weekend)       │
│        - O(log n) Binary Search Window Evaluation         │
└─────────────────────────────┬─────────────────────────────┘
                              │ Compiled BlackoutWindows
┌─────────────────────────────▼─────────────────────────────┐
│                     RedFolderService                      │
│        - Fine-Grained RwLock Concurrency Architecture     │
│        - Concurrent EventListener Dispatch (tokio::spawn) │
│        - Daily Midnight UTC Sync Loop                     │
│        - Transition-Driven Evaluation Loop (Exact Timing) │
│        - CancellationToken & Tracked JoinHandles Lifecycle│
│        - Immediate State Notification on Worker Register  │
└───────┬───────────────────────────────┬───────────────────┘
        │                               │
        │ Broadcast Channel             │ Dedicated Worker Stream
┌───────▼──────────────────────┐ ┌──────▼───────────────────┐
│     tokio::sync::broadcast   │ │  mpsc::UnboundedReceiver  │
│  (Audit, Webhooks, Logging)  │ │   (Execution Strategies)  │
└──────────────────────────────┘ └───────────────────────────┘
```

### Event-Driven Processing Model

Rather than forcing trading strategies to poll state flags in a tight loop, `redfolder` operates as an asynchronous event producer. When state changes occur, discrete domain events are broadcast across memory:

- `RedFolderEvent::BlackoutWarning`: Emitted $N$ minutes **prior** to the start of a blackout window. Gives execution bots a clean grace period to cancel pending limit orders, tighten stop-loss thresholds, or scale down leverage before volatility explodes.
- `RedFolderEvent::BlackoutStarted`: Emitted the exact second a blackout window becomes active. Directs execution modules to reject incoming trade signals and pause active scalpers.
- `RedFolderEvent::BlackoutEnded`: Emitted when the window closes (under standard half-open interval `[start, end)` semantics), signaling strategies that market conditions and spreads have normalized.
- `RedFolderEvent::CalendarUpdated`: Emitted when new weekly schedules are synchronized and indexed into memory.
- `RedFolderEvent::CalendarSyncFailed`: Emitted when scheduled or background calendar synchronization fails, alerting risk monitors of network partitions or stale data.

### Transition-Driven Event Scheduling vs. Polling

Fixed-interval polling (e.g. every 15s) risks delaying risk alerts by up to 14 seconds during market-moving news releases. `redfolder` employs a **transition-driven scheduler**:
1. Evaluates all registered workers and determines the exact timestamp of the earliest upcoming state transition (blackout start, blackout end, or warning threshold).
2. Sleeps directly until `next_transition`, guaranteeing alert emission at the exact second.
3. Automatically awakens via `tokio::sync::Notify` whenever new workers are registered/unregistered or calendar data is refreshed.
4. Retains an internal watchdog fallback ensuring configuration changes and periodic checks execute reliably.

### Deterministic Lifecycle & Cancellation Primitives

The service state machine adheres to strict lifecycle transitions:
`Stopped` -> `Starting` -> `Running` -> `Stopping` -> `Stopped`.
- **Startup Protection**: If initial calendar synchronization fails, `start()` fails cleanly and leaves the service in `Stopped` state, allowing immediate retry without getting stuck.
- **Clean Task Termination**: Background loops are spawned with `tokio_util::sync::CancellationToken` and tracked `JoinHandle`s. Calling `stop()` cancels the token, wakes up waiters, and awaits task termination before returning, preventing duplicate concurrent loops across restarts.

### Dynamic Window Clustering & Merging

Economic events rarely occur in isolation. A single trading day may schedule:
- 13:30 UTC: US Consumer Price Index (CPI)
- 14:00 UTC: FOMC Member Speech
- 14:15 UTC: Industrial Production

If configured with a 30-minute buffer and a 30-minute merge threshold, `redfolder` automatically identifies that the tail of the first window intersects or lies within the threshold of the next. It clusters all related events into a single unified `BlackoutWindow` using half-open intervals (`[start, end)`), eliminating rapid oscillation between active and inactive states.

### Calendar Event Timing & Timezone Normalization

- **Explicit Event Timing**: Distinguishes between `EventTiming::Exact(DateTime<Utc>)`, `EventTiming::AllDay(NaiveDate)`, and `EventTiming::TentativeDate(NaiveDate)`. All-day events (e.g. Bank Holidays) and tentative releases do not create spurious midnight spikes by default, but can be configured to cover full 24-hour windows via `include_all_day(true)`.
- **Source Timezone & DST Support**: Supports `chrono_tz::Tz` (e.g. `America/New_York`) to accurately interpret naive calendar date/time strings with automatic Daylight Saving Time (DST) adjustment (EDT vs EST).

### Weekend Market Close Curfew Engine

Forex markets close on Friday evening and reopen on Sunday afternoon, exposing open positions to weekend gap risk. `redfolder` provides dedicated weekend curfew management:
- **`short` mode**: Initiates a blackout at a configurable Friday UTC time (e.g. `20:30 UTC`) and ends at market close (`21:00 UTC`), preventing late-Friday slippage.
- **`weekend` mode**: Holds trading closed through Friday evening until Monday 00:00 UTC, preventing over-the-weekend exposure.

Weekend curfew windows apply globally to the worker when enabled, intentionally bypassing currency and impact filters since market-close gap risk affects all traded instruments regardless of macroeconomic news releases.

### Offline Durability & Rate Limit Resilience

External calendar APIs enforce strict Cloudflare rate limiting (HTTP 429). `CalendarClient` includes a multi-tier fallback mechanism:
1. Fresh remote synchronization writes a pretty-printed JSON copy to disk with schema metadata (`version`, `event_count`, `fetched_at`, `sha256`).
2. When loading from disk, the client validates that `metadata.event_count == events.len()` and verifies the SHA-256 cryptographic checksum, rejecting corrupted, truncated, or tampered cache files.
3. Fallback to stale cache on network failure respects a configurable maximum allowable staleness boundary (`max_stale_cache_age`, default 36 hours), preventing ancient calendars from silently driving live trading.
4. An internal lock-split design ensures network downloads execute without holding the service mutex, guaranteeing that worker evaluations are never stalled by remote latency.

### Data Sources, Compliance & Multi-Feed Fallback

- **Unofficial Feed Transparency & Risks**: By default, `redfolder` ingests releases from FairEconomy's JSON endpoint (`nfs.faireconomy.media/ff_calendar_thisweek.json`). Because this is an unofficial endpoint without a formal enterprise SLA, downstream applications must account for potential schema shifts or provider downtime.
- **Anti-Bot Mitigation & User-Agent Disclosure**: Upstream Cloudflare mitigations reject default bot User-Agents with HTTP 403 Forbidden. To provide out-of-the-box reliability, `CalendarClient` provides a standard browser User-Agent by default. For institutional environments or strict corporate compliance policies, custom identification strings can be configured via [`CalendarClient::with_user_agent`](#calendarclient).
- **Multi-Endpoint Redundancy**: Redundant endpoints, internal cache mirrors, or backup proxies can be chained using `with_fallback_url` or `with_fallback_urls`. If the primary endpoint fails or returns a non-retryable error, `fetch_remote` automatically fails over to the next candidate mirror.

### Response Bounding & Memory Hardening

To guard against malicious, misconfigured, or corrupted upstream payloads consuming unbounded heap memory:
- **Pre-Flight Content-Length Enforcement**: Validates the HTTP `Content-Length` header before allocating buffers. If the advertised size exceeds `max_response_bytes` (default: 10 MiB, configurable via `with_max_response_bytes`), retrieval is immediately aborted.
- **Streamed Chunk Size Capping**: When receiving chunked transfer encoding (where `Content-Length` is omitted), incoming chunks are streamed incrementally with a strict byte counter. If accumulated bytes exceed the limit, streaming terminates immediately with a typed `RedFolderError::Calendar`.
- **Event Count Upper Bound**: Parsed JSON arrays are bounded by `max_event_count` (default: 50,000 events, configurable via `with_max_event_count`), preventing CPU and memory exhaustion from upstream event loops or malformed JSON matrices.

### Cryptographic Disk Integrity & File Permissions

Macroeconomic release schedules directly govern live trading execution and risk parameters. `redfolder` incorporates file-level and disk-level defenses:
- **SHA-256 Cache Checksum**: Every atomic write computes a SHA-256 digest of the serialized events array stored in `CacheMetadata.sha256`. Upon disk load, the digest is re-verified. Any unauthorized out-of-process modification or bit rot triggers immediate cache rejection.
- **Shared Host File Permissions**: On Unix platforms, cache directories are created with owner-only access (`0700`), and temporary/final cache files are locked to read/write owner-only permissions (`0600`) via `std::os::unix::fs`.
- **Pluggable Integrity Validation**: Institutional teams can attach a custom verification closure via [`CalendarClient::with_integrity_validator`](#calendarclient) to assert custom HMAC signatures, PGP validation, or domain-specific sanity rules prior to committing releases into memory.

### Retry Latency Ceilings & Execution SLAs

Gatekeeping real-time order execution requires strictly bounded network latency. `fetch_remote` provides deterministic latency boundaries:
- **Bounded Exponential Backoff**: Retries are capped at `max_retries` (default: 2, bounded by `MAX_RETRIES_LIMIT = 10`), initial backoff begins at 500ms, backoff is capped by `max_backoff` (default: 10s), and `Retry-After` header values from upstream 429 responses are hard-capped at 10s.
- **Worst-Case Wall-Clock Bounds**: Under total connection timeout conditions, total wall-clock latency per endpoint candidate is bounded by $\sum \text{request\_timeout} + \sum \text{backoff\_delay}(\text{attempt})$ ($\approx 91.5\text{s}$ per endpoint under defaults). Under instant HTTP 429 status responses with $\text{Retry-After} \ge 10\text{s}$, maximum latency is $\approx 21\text{s}$ per endpoint.
- **End-to-End Operation Timeout**: An overall wall-clock deadline (`overall_timeout`, default 60s, configurable via [`CalendarClient::with_overall_timeout`](#calendarclient) or [`without_overall_timeout`](#calendarclient)) strictly caps cumulative duration across all candidate URLs, retries, and backoffs.

### Fail-Safe Risk Policies (Fail-Open vs. Fail-Closed)

When upstream network feeds, fallback mirrors, and local disk caches fail concurrently, risk engines must enforce an unambiguous safety policy:
- **`FailSafeMode::FailOpen`** (Permissive): If calendar data is missing or exceeds `max_stale_cache_age`, the engine assumes no economic blackout is active, permitting trading (weekend curfews remain enforced if enabled).
- **`FailSafeMode::FailClosed`** (Defensive / Prop Firm Shield): If calendar data is missing, corrupted, or stale, the engine assumes an active blackout condition. A synthetic `Fail-Closed Safety Blackout` window is dynamically injected, immediately halting trades and protecting prop-firm challenge accounts from catastrophic disqualification during provider outages.
- **Preset Defaults**: [`RedFolderConfig::prop_firm_strict`](#redfolderconfig) and [`RedFolderConfig::conservative`](#redfolderconfig) default to `FailSafeMode::FailClosed`.

### Data Freshness & Provenance

In financial infrastructure, knowing *where data came from and how old it is* is as critical as the data itself. `redfolder` makes provenance a first-class citizen:

- **`CalendarSnapshot`**: Bundles event records with provenance metadata:
  - `source`: `SnapshotSource::Remote`, `SnapshotSource::TtlCache`, `SnapshotSource::FallbackCache`, or `SnapshotSource::EmptyFeedCache`.
  - `data_fetched_at`: Timestamp when the data was originally retrieved from the upstream provider, not when it was served from disk.
  - `remote_error`: Reason upstream data was not used when serving degraded cache.
  - `stats`: `IngestStats` tracking `received`, `malformed`, `kept`, and `unparseable` counts.
- **`ServiceHealth`**: Telemetry snapshot returned by `service.health().await`, exposing operational metrics for monitoring:
  - `stale`: Boolean indicating whether underlying data age exceeds `max_data_age`.
  - `degraded`: Boolean indicating whether data was served from fallback cache due to an upstream outage.
  - `background_task_restarts`: Counter tracking supervisor task restarts following unexpected panics.

> [!WARNING]
> Age-based fail-closed protection requires data-timestamp tracking (`RedFolderService`, or `BlackoutEngine::compile_snapshot`). `BlackoutEngine::compile(&Vec<RawCalendarEvent>, …)` has no notion of data age.

### Production Deployment & Verification

For mission-critical prop firm and live algorithmic deployments, adhere to these operational practices:

1. **Order-Time Verification via `gate()`**:
   Events (`RedFolderEvent`) are edge-triggered notifications and can be delayed or dropped under network or channel pressure. **Always evaluate `service.gate(worker_id)` or `engine.status(&config)` synchronously prior to dispatching new orders:**
   ```rust
   match service.gate("prop_scalper").await {
       Gate::Allowed => {
           // Clear of news blackouts; proceed with order execution
           send_order(symbol, volume, price).await?;
       }
       Gate::Blocked(window) => {
           tracing::warn!("Order rejected: active blackout until {}", window.end);
       }
       Gate::Unknown(reason) => {
           // Service not running, worker unregistered, or calendar data stale
           tracing::error!("Order rejected: safety gate indeterminate ({reason})");
       }
   }
   ```
   > *Events are notifications; they are edge-triggered and can be missed. The authoritative answer is `gate(worker)` / `status(&cfg)`, and it must be evaluated at order time.*

2. **Host Clock Synchronization (NTP / chrony)**:
   All age and window calculations rely on UTC wall-clock timestamps. Configure `chrony` or `systemd-timesyncd` with alerting on maximum clock offset (> 5s).
3. **Egress Firewall Rules**:
   Restrict outbound HTTP requests to allow-listed endpoints (`nfs.faireconomy.media`) and enable `UrlPolicy` to prevent SSRF vulnerabilities.
4. **Enforce `FailClosed` for Prop Firm Challenges**:
   Always use `RedFolderConfig::prop_firm_strict()` or set `fail_safe_mode(FailSafeMode::FailClosed)` on accounts where news trading violations lead to immediate disqualification.

---

## Key Features

- **Zero Unsound Dependencies**: Pure Rust implementation with strict compile-time invariants.
- **Fast In-Memory Matching**: Compiles events into pre-sorted UTC intervals for $O(\log n)$ binary search lookup and single-digit microsecond query latency (~6–8 µs) during order placement, verified with Criterion benchmarks.
- **High-Concurrency Service Architecture**: Granular `RwLock` synchronization separates engine intervals, registered worker states, and sync metadata, enabling hundreds of trading bots to query blackout statuses concurrently without serialization bottlenecks.
- **Concurrent Event Listeners**: Trait-based event listeners are dispatched concurrently across Tokio tasks, preventing slow logging or webhook sinks from delaying order cancellation warnings.
- **Strong Domain Typing**: Strongly typed `Currency`, `Impact`, and `WeekendMode` enums with flexible builder ergonomics accepting both typed variants and string literals.
- **Multi-Worker Granularity**: Assign separate configurations to different strategies or symbols (e.g. `EURUSD` bot monitors USD + EUR; `GBPJPY` bot monitors GBP + JPY).
- **Multiple Integration Channels**: Consume events via `tokio::sync::broadcast`, per-worker `tokio::sync::mpsc`, or asynchronous `EventListener` traits.
- **Prop Firm Preset Out of the Box**: Configurable presets approximating FTMO, FundedNext, and The5ers rules.
- **Deterministic Historical Queries**: Timestamp-aware evaluation enabling accurate backtesting and replay simulations.
- **Terminal CLI Included**: Real-time terminal watcher and JSON query interface.

---

## Repository Structure

```text
redfolder/
├── .github/workflows/ci.yml       # Automated GitHub Actions CI (Check, Test, Clippy, Fmt)
├── Cargo.toml                     # Crate definition, metadata, and optional CLI dependencies
├── LICENSE                        # MIT License
├── README.md                      # Architecture documentation and usage guide
├── benches/
│   └── blackout_benchmark.rs      # Criterion benchmarks for engine compile and query matching
├── examples/
│   ├── quickstart.rs              # Basic one-shot calendar inspection
│   ├── event_driven_bot.rs        # Async event loop responding to warnings and halts
│   ├── prop_firm_guard.rs         # FTMO / FundedNext strict news rule validator
│   └── custom_listener.rs         # Trait-based EventListener callback integration
├── src/
│   ├── lib.rs                     # Public API exports and common prelude
│   ├── types.rs                   # Core domain models (Impact, BlackoutWindow, WindowEvent)
│   ├── config.rs                  # RedFolderConfig, builder, and prop-firm presets
│   ├── error.rs                   # Typed RedFolderError via thiserror
│   ├── events.rs                  # RedFolderEvent enum and EventListener trait
│   ├── curfew.rs                  # Weekend market close calculation engine
│   ├── calendar.rs                # HTTP client, JSON parser, and local cache manager
│   ├── engine.rs                  # In-memory interval compiler and window merge engine
│   ├── service.rs                 # Background Tokio task orchestrator
│   └── bin/
│       └── main.rs                # Terminal CLI binary (status, upcoming, watch, sync)
└── tests/
    ├── integration_tests.rs   # End-to-end integration test battery
    └── property_tests.rs      # Proptest property-based fuzzing and invariant battery
```

---

## Installation

Add `redfolder` to your project's `Cargo.toml`:

```toml
[dependencies]
redfolder = "1.1"
tokio = { version = "1", features = ["full"] }
```

To install the standalone terminal command-line tool:

```bash
cargo install redfolder
```

---

## Usage & Code Examples

### 1. Quickstart: Checking Active Blackout Status

Fetch the calendar schedule and verify whether a currency is currently within a blackout period:

```rust
use redfolder::calendar::CalendarClient;
use redfolder::config::RedFolderConfig;
use redfolder::engine::BlackoutEngine;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Fetch calendar (automatically cached to ~/.cache/redfolder/)
    let client = CalendarClient::new(None);
    let events = client.fetch_or_cached().await?;

    // 2. Configure 30-minute buffers for high-impact USD releases
    let config = RedFolderConfig::builder()
        .currencies(vec!["USD"])
        .impacts(vec!["High"])
        .buffer_minutes(30, 30)
        .build();

    // 3. Compile in-memory interval index
    let engine = BlackoutEngine::compile(&events, &[&config], chrono::Utc::now());

    // 4. Query blackout state atomically (prevents TOCTOU races)
    if let Some(current) = engine.status(&config) {
        println!("Blackout active: {} (ends in {} mins)", 
            current.summary_title(), current.remaining_minutes());
    } else {
        println!("Trading permitted: No active blackout for USD.");
    }

    Ok(())
}
```

---

### 2. Event-Driven Strategy Integration (Tokio Streams)

Integrate `redfolder` directly into an asynchronous strategy event loop:

```rust
use redfolder::prelude::*;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let service = RedFolderService::new(None);

    let config = RedFolderConfig::builder()
        .currencies(vec!["USD", "EUR"])
        .impacts(vec!["High"])
        .buffer_minutes(15, 15)
        .warning_minutes(5)
        .build();

    // Register worker and receive a dedicated event receiver
    let mut events = service.register_worker_events("eurusd_scalper", config).await?;
    service.start().await?;

    while let Some(event) = events.recv().await {
        match event {
            RedFolderEvent::BlackoutWarning { window, minutes_until_start, .. } => {
                println!("WARNING: '{}' starts in {} mins. Cancelling limit orders...", 
                    window.summary_title(), minutes_until_start);
            }
            RedFolderEvent::BlackoutStarted { window, .. } => {
                println!("BLACKOUT ACTIVE: {}. Halting strategy execution.", window.summary_title());
            }
            RedFolderEvent::BlackoutEnded { .. } => {
                println!("BLACKOUT CLEARED: Resuming normal execution.");
            }
            _ => {}
        }
    }

    Ok(())
}
```

> [!TIP]
> Always register workers before invoking `service.start().await?`. If no enabled workers are registered, `start()` logs an informational warning and safely remains in `ServiceState::Stopped` without spawning unnecessary background loops.

---

### 3. Pre-Blackout Advance Warnings (Order Cancellation Guard)

Spread widening often occurs 1–3 minutes before official release times. By setting `warning_minutes(5)`, your bot receives an advance event before the official blackout starts:

```rust
let config = RedFolderConfig::builder()
    .currencies(vec!["USD"])
    .impacts(vec!["High"])
    .buffer_minutes(15, 15)
    .warning_minutes(5) // Emits BlackoutWarning 5 minutes before start
    .build();
```

---

### 4. Prop Firm Challenge Preset (FTMO, FundedNext, The5ers)

Prop firm rules strictly prohibit opening trades or holding execution within 2–5 minutes of high-impact releases. Use the built-in preset:

```rust
use redfolder::config::RedFolderConfig;

// Preconfigured with:
// - 8 major currencies (USD, EUR, GBP, JPY, CAD, AUD, NZD, CHF)
// - High-impact (Red Folder) releases only
// - 5-minute pre-event buffer & 5-minute post-event buffer
// - Weekend curfew active from Friday 20:00 UTC until Monday 00:00 UTC
let prop_config = RedFolderConfig::prop_firm_strict();
```

> [!NOTE]
> **Prop Firm Qualification & Compliance**: Built-in presets approximate standard industry guidelines (such as FTMO, FundedNext, and The5ers). Specific compliance requirements vary by account type (e.g. Swing vs. Standard), challenge stage, instrument, and effective terms. Traders should verify terms directly with their funding provider and customize buffers accordingly.

---

### 5. Custom Trait-Based Event Listener Callback

Implement the `EventListener` trait to hook into external logging, Telegram bots, or compliance dashboards:

```rust
use redfolder::events::{EventListener, RedFolderEvent};
use redfolder::prelude::*;
use std::sync::Arc;

struct SlackAlertNotifier;

#[async_trait::async_trait]
impl EventListener for SlackAlertNotifier {
    async fn on_event(&self, event: &RedFolderEvent) {
        match event {
            RedFolderEvent::BlackoutStarted { window, worker_id } => {
                println!("[Audit] Worker {:?} entered blackout: {}", worker_id, window.summary_title());
            }
            RedFolderEvent::BlackoutEnded { worker_id, .. } => {
                println!("[Audit] Worker {:?} exited blackout.", worker_id);
            }
            _ => {}
        }
    }
}

// Register with service
let service = RedFolderService::new(None);
service.add_listener(Arc::new(SlackAlertNotifier)).await;
```

---

### 6. Multi-Worker Symbol Isolation

Run multiple bots concurrently with strict currency isolation. An event on USD halts the USD worker while leaving the EUR worker uninterrupted:

```rust
let usd_config = RedFolderConfig::builder().currencies(vec!["USD"]).build();
let eur_config = RedFolderConfig::builder().currencies(vec!["EUR"]).build();

let mut usd_events = service.register_worker_events("usd_bot", usd_config).await?;
let mut eur_events = service.register_worker_events("eur_bot", eur_config).await?;
```

---

### 7. Terminal Command-Line Interface (CLI)

The `redfolder` CLI provides terminal tools for live operations:

#### Status Query
```bash
# Check if USD is currently in a blackout window
redfolder status --currency USD

# Output structured JSON for shell scripting or monitoring daemons
redfolder status --currency USD --json
```

#### Upcoming Releases
```bash
# View all blackout windows scheduled within the next 48 hours
redfolder upcoming --hours 48 --currency USD --impact High
```

#### Real-Time Watcher
```bash
# Live terminal monitor with real-time countdown
redfolder watch --currency USD --interval 5
```

#### Calendar Cache Synchronization
```bash
# Manually synchronize calendar releases to disk
redfolder sync
```

#### System Health Diagnostics
```bash
# Query calendar synchronization health, age, and background supervisor status
redfolder health

# Output structured health telemetry JSON for automated monitoring alerts
redfolder health --json
```

#### CLI Exit Codes (Shell Guard Protocol)

When shell scripts or CI orchestrators gate order placement (`redfolder status --fail-closed && execute_order`), the command emits standardized exit codes:

| Exit Code | Classification | Action / Safety State |
| :---: | :--- | :--- |
| `0` | **Trading Allowed** | Clear of active news blackouts; order entry allowed. |
| `1` | **Blackout Active** | Currently within an active blackout window; trading blocked. |
| `2` | **Data Unavailable / Stale** | Calendar feed unavailable, unparseable, or stale under `--fail-closed`. |

---

## API Reference

### RedFolderService

| Method | Signature | Description |
| :--- | :--- | :--- |
| `new` | `fn(Option<PathBuf>) -> Self` | Initializes service with optional cache directory (defaults to platform cache dir). |
| `with_client` | `fn(CalendarClient) -> Self` | Initializes service using a preconfigured `CalendarClient` instance. |
| `with_event_capacity` | `fn(mut self, usize) -> Self` | Configures global event broadcast bus capacity (default 1024). |
| `state` | `async fn(&self) -> ServiceState` | Returns the current lifecycle state (`Stopped`, `Starting`, `Running`, `Stopping`). |
| `health` | `async fn(&self) -> ServiceHealth` | Returns comprehensive diagnostics (`stale`, `degraded`, `data_source`, `background_task_restarts`, `ingest`). |
| `gate` | `async fn(&self, &str) -> Gate` | Evaluates 3-state order gate decision (`Allowed`, `Blocked`, `Unknown`) with explicit fail-open staleness protection. |
| `is_running` | `async fn(&self) -> bool` | Checks if background worker tasks are active (`state == ServiceState::Running`). |
| `subscribe` | `fn(&self) -> broadcast::Receiver<RedFolderEvent>` | Subscribes to global broadcast bus receiving all system events. |
| `subscribe_sequenced` | `fn(&self) -> broadcast::Receiver<SequencedEvent>` | Subscribes to global broadcast bus with monotonic sequence numbering and timestamps. |
| `add_listener` | `async fn(&self, Arc<dyn EventListener>)` | Attaches an asynchronous trait-based callback listener using a bounded ordered queue (1024 slots). |
| `register_worker_events` | `async fn(&self, &str, RedFolderConfig) -> Result<mpsc::UnboundedReceiver<RedFolderEvent>>` | Validates config, guards against duplicate registrations, and returns a typed stream of scoped events. |
| `register_worker` | `async fn(&self, &str, RedFolderConfig) -> Result<mpsc::UnboundedReceiver<BlackoutNotification>>` | Legacy registration; validates config and guards against duplicate worker registrations. |
| `reregister_worker_events` | `async fn(&self, &str, RedFolderConfig) -> Result<mpsc::UnboundedReceiver<RedFolderEvent>>` | Re-registers an existing worker with an updated configuration, replacing its event stream. |
| `reregister_worker` | `async fn(&self, &str, RedFolderConfig) -> Result<mpsc::UnboundedReceiver<BlackoutNotification>>` | Re-registers an existing worker with an updated configuration, replacing its notification stream. |
| `unregister_worker` | `async fn(&self, &str)` | Unregisters worker, cleans up state, and recalculates transition schedule. |
| `windows_for_worker` | `async fn(&self, &str) -> Vec<BlackoutWindow>` | Returns active and upcoming blackout windows derived specifically for the worker's buffers. |
| `start` | `async fn(&self) -> Result<()>` | Starts background daily refresh and transition-driven evaluation tasks with auto-restarting supervision. |
| `start_required` | `async fn(&self) -> Result<()>` | Starts background tasks, returning an error if no enabled workers are registered. |
| `stop` | `async fn(&self)` | Gracefully terminates all background tasks and awaits their exit. |
| `set_check_interval` | `async fn(&self, Duration) -> Result<()>` | Configures evaluation frequency; validates interval is at least 100ms to prevent busy loops. |
| `refresh` | `async fn(&self) -> Result<()>` | Synchronizes calendar and recompiles blackout windows (allowing cached fallback). |
| `force_refresh` | `async fn(&self) -> Result<()>` | Forces immediate network download from remote API and window recompilation, bypassing cache. |
| `is_blackout` | `async fn(&self, &str) -> bool` | Checks if a specific registered worker is currently in blackout. |
| `current_window` | `async fn(&self, &str) -> Option<BlackoutWindow>` | Returns active window details for a specific worker. |
| `get_upcoming_blackouts` | `async fn(&self, &str, u32) -> Vec<BlackoutWindow>` | Returns upcoming blackout windows within $N$ hours. |
| `last_sync_time` | `async fn(&self) -> Option<DateTime<Utc>>` | Returns timestamp when data was fetched by the provider. |
| `last_sync_error` | `async fn(&self) -> Option<String>` | Returns description of the most recent failed sync attempt, or None if healthy. |
| `is_calendar_stale` | `async fn(&self) -> bool` | Checks if calendar data exceeds configured `max_data_age` or is uninitialized. |

### BlackoutEngine

| Method | Signature | Description |
| :--- | :--- | :--- |
| `compile` | `fn(&[RawCalendarEvent], &[&RedFolderConfig], DateTime<Utc>) -> Self` | Compiles raw releases and curfew rules into merged blackout intervals (defaulting to UTC). |
| `compile_with_tz` | `fn(&[RawCalendarEvent], &[&RedFolderConfig], DateTime<Utc>, Option<chrono_tz::Tz>) -> Self` | Compiles events using an explicit source timezone for naive timestamps. |
| `windows_for_config` | `fn(&self, &RedFolderConfig, DateTime<Utc>) -> Vec<BlackoutWindow>` | Derives isolated worker-specific blackout intervals respecting custom buffers and policies. |
| `windows` | `fn(&self) -> &[BlackoutWindow]` | Accesses pooled precompiled windows across configurations for diagnostic reference. |
| `is_blackout` | `fn(&self, &RedFolderConfig) -> bool` | Checks if the current UTC time falls within any active window. |
| `is_blackout_at` | `fn(&self, &RedFolderConfig, DateTime<Utc>) -> bool` | Evaluates blackout status at an arbitrary timestamp using half-open `[start, end)`. |
| `current_window` | `fn(&self, &RedFolderConfig) -> Option<BlackoutWindow>` | Returns the active `BlackoutWindow` matching configuration. |
| `current_window_at` | `fn(&self, &RedFolderConfig, DateTime<Utc>) -> Option<BlackoutWindow>` | Returns the active `BlackoutWindow` matching configuration at an arbitrary timestamp. |
| `status` | `fn(&self, &RedFolderConfig) -> Option<BlackoutWindow>` | Atomically queries active blackout status and returns the current window (eliminates TOCTOU races). |
| `status_at` | `fn(&self, &RedFolderConfig, DateTime<Utc>) -> Option<BlackoutWindow>` | Atomically evaluates status and active window at an arbitrary timestamp. |
| `upcoming_blackouts` | `fn(&self, &RedFolderConfig, u32) -> Vec<BlackoutWindow>` | Lists matching windows starting within $N$ hours. |

### CalendarClient

| Method | Signature | Description |
| :--- | :--- | :--- |
| `new` | `fn(Option<PathBuf>) -> Self` | Creates client with standard browser User-Agent and default cache. |
| `without_cache` | `fn() -> Self` | Creates client strictly performing live network requests. |
| `with_user_agent` | `fn(Option<PathBuf>, &str) -> Result<Self>` | Creates client with custom User-Agent and optional cache directory. |
| `with_fallback_url` | `fn(self, impl Into<String>) -> Self` | Configures secondary mirror URL for automated failover on network/HTTP errors. |
| `with_fallback_urls` | `fn(self, impl IntoIterator<Item = S>) -> Self` | Configures multiple fallback mirror URLs in failover priority order. |
| `with_max_response_bytes` | `fn(self, usize) -> Self` | Enforces upper bound on response payload body (default 10 MiB) to prevent OOM DOS. |
| `with_max_event_count` | `fn(self, usize) -> Self` | Enforces upper limit on parsed calendar events (default 50,000). |
| `with_max_retries` | `fn(self, usize) -> Self` | Sets maximum HTTP retry attempts on transient network errors (default 3). |
| `with_backoff` | `fn(self, Duration, Duration) -> Self` | Configures initial backoff delay and max retry-after cap for exponential backoff. |
| `with_overall_timeout` | `fn(self, Duration) -> Self` | Enforces hard wall-clock latency ceiling across the entire fetch-and-retry cycle. |
| `with_url_policy` | `fn(self, UrlPolicy) -> Result<Self>` | Configures URL security policy enforcing HTTPS, host allow-lists, private IP blocking, and redirect limits. |
| `try_with_fallback_url` | `fn(self, impl Into<String>) -> Result<Self>` | Adds a fallback mirror URL validated against the active URL policy. |
| `with_user_agent_str` | `fn(self, impl Into<String>) -> Result<Self>` | Configures a custom User-Agent string and rebuilds the HTTP client preserving active policies. |
| `with_integrity_validator` | `fn(self, impl Fn(&[RawCalendarEvent]) -> Result<()>) -> Self` | Injects custom cryptographic verification or semantic sanity checks on fetched events. |
| `with_timezone` | `fn(self, chrono_tz::Tz) -> Self` | Configures default source timezone for resolving naive calendar timestamps. |
| `with_max_stale_age` | `fn(self, Option<Duration>) -> Self` | Sets maximum allowable cache age for fallback on network failure. |
| `with_ttl` | `fn(self, Duration) -> Self` | Configures cache Time-To-Live before remote re-fetch is permitted. |
| `default_cache_dir` | `fn() -> PathBuf` | Resolves cross-platform cache directory (`%LOCALAPPDATA%`, XDG, macOS, fallback temp). |
| `fetch_remote` | `async fn(&self) -> Result<Vec<RawCalendarEvent>>` | Performs HTTP GET against FairEconomy feed with 429/408/5xx retry and `Retry-After` support. |
| `force_fetch` | `async fn(&self) -> Result<Vec<RawCalendarEvent>>` | Fetches fresh schedule directly from remote API, bypassing cache while saving to disk. |
| `fetch_or_cached` | `async fn(&self) -> Result<Vec<RawCalendarEvent>>` | Fetches remote schedule, verifying cache metadata and bounded staleness on failure. |
| `save_cache` | `fn(&self, &[RawCalendarEvent]) -> Result<()>` | Atomically writes JSON-serialized events using PID + nanoseconds + atomic counter. |
| `load_cache_data` | `fn(&self) -> Option<CachedCalendarData>` | Loads structured cache validating `event_count == events.len()`. |

### RedFolderConfig

| Method / Associated Fn | Signature | Description |
| :--- | :--- | :--- |
| `builder` | `fn() -> RedFolderConfigBuilder` | Creates a fluent builder with sensible defaults (USD, High impact, 30m buffers). |
| `prop_firm_strict` | `fn() -> Self` | High-risk prop firm preset: 8 major currencies, High impact, 5m buffers, weekend curfew, and `FailSafeMode::FailClosed`. |
| `conservative` | `fn() -> Self` | Wide buffer preset: 8 major currencies, High + Medium impacts, 15m buffers, weekend curfew, and `FailSafeMode::FailClosed`. |
| `crypto_curfew` | `fn() -> Self` | Pure weekend curfew preset without macroeconomic currency filters. |
| `validate` | `fn(&self) -> Result<()>` | Performs semantic validation on buffers, curfews, and intervals (unconditionally checks weekend parameters). |
| `validate_strict` | `fn(&self) -> Result<()>` | Strict validation ensuring currency and impact lists contain no blank strings and recognized codes. |

#### RedFolderConfigBuilder

| Method | Signature | Description |
| :--- | :--- | :--- |
| `currencies` | `fn(self, impl IntoIterator<Item = impl IntoCurrency>) -> Self` | Sets target currencies (accepts `Currency` variants like `Currency::USD` or string slices `["USD", "EUR"]`). |
| `impacts` | `fn(self, impl IntoIterator<Item = impl IntoImpact>) -> Self` | Sets impact tiers to filter (accepts `Impact` variants like `Impact::High` or string slices `["High", "Red"]`). |
| `buffer_minutes` | `fn(self, u32, u32) -> Self` | Sets pre-event and post-event blackout safety buffers in minutes. |
| `warning_minutes` | `fn(self, u32) -> Self` | Configures advance warning alert threshold prior to blackout onset. |
| `merge_threshold_minutes` | `fn(self, u32) -> Self` | Maximum gap between successive events to merge into a single window. |
| `weekend_curfew` | `fn(self, bool, &str, &str, impl IntoWeekendMode) -> Self` | Enables and configures weekend curfew start, end, and mode (accepts `WeekendMode`, `&str`, or `String`). |
| `include_all_day` | `fn(self, bool) -> Self` | Controls whether all-day events (e.g. bank holidays) trigger 24h blackouts. |
| `include_tentative` | `fn(self, bool) -> Self` | Controls whether unscheduled tentative releases trigger blackouts. |
| `fail_safe_mode` | `fn(self, FailSafeMode) -> Self` | Sets safety policy (`FailOpen` or `FailClosed`) for handling stale or failed feeds. |
| `build` | `fn(self) -> RedFolderConfig` | Validates parameters and constructs config; panics if invalid. |
| `try_build` | `fn(self) -> Result<RedFolderConfig>` | Validates parameters and returns typed `Result<RedFolderConfig, RedFolderError>`. |
| `try_build_strict` | `fn(self) -> Result<RedFolderConfig>` | Strict validation rejecting non-standard currency/impact typos; returns typed `Result`. |
| `build_strict` | `fn(self) -> RedFolderConfig` | Validates strictly; panics if non-standard or invalid parameters are provided. |

> [!NOTE]
> **Permissive Defaults vs. Strict Validation**: By default, `validate()` and `try_build()` allow custom non-standard currency codes (e.g. `"XAU"`, `"BTC"`, `"TRY"`) via `Currency::Custom` and `Impact::Custom`. When loading configuration from external files (YAML, JSON, TOML) or untrusted user input, prefer `validate_strict()` or `try_build_strict()` to reject typos like `"USDD"` at startup instead of silently matching zero events.

### Types & Domain Models

- **`RedFolderEvent`**: Typed enum (`BlackoutWarning`, `BlackoutStarted`, `BlackoutEnded`, `CalendarUpdated`, `CalendarSyncFailed`).
- **`FailSafeMode`**: Policy enum (`FailOpen`, `FailClosed`). In `FailClosed` mode, missing or stale calendar feeds trigger an immediate safety blackout to protect prop firm accounts from unexpected macroeconomic volatility.
- **`CustomEventKind`**: Typed classification enum (`WeekendCurfew`, `FailClosedSafety`) identifying synthetic blackout windows. `WindowEvent` provides helper accessors `is_weekend_curfew()` and `is_fail_closed_safety()`.
- **`EventTiming`**: Precision timing enum (`Exact(DateTime<Utc>)`, `AllDay(NaiveDate)`, `TentativeDate(NaiveDate)`).
- **`ServiceState`**: Service lifecycle enum (`Stopped`, `Starting`, `Running`, `Stopping`).
- **`BlackoutWindow`**: Struct containing `start: DateTime<Utc>`, `end: DateTime<Utc>`, and `events: Vec<WindowEvent>`. Uses standard half-open interval semantics `[start, end)`. Provides helper methods `remaining_minutes()`, `duration_minutes()`, `is_active()`, and `summary_title()`.
- **`Currency`**: Strongly-typed enum with variants `USD`, `EUR`, `GBP`, `JPY`, `CAD`, `AUD`, `NZD`, `CHF`, and `Custom(String)`. Supports case-insensitive string parsing, matching, and deserialization.
- **`Impact`**: Enum with variants `High`, `Medium`, `Low`, `NonEconomic`, `Custom(String)`. Supports case-insensitive string matching (`"red"`, `"high"`).
- **`WeekendMode`**: Enum with variants `Short` (Friday evening window) and `Weekend` (Friday evening through Monday 00:00 UTC). Supports case-insensitive deserialization and default derivation.
- **`UrlPolicy`**: Security policy governing transport schemes (`https_only`), hostname allow-lists (`allowed_hosts`), private/loopback/link-local IP blocking (`block_private_ips`), and HTTP redirect limits (`max_redirects`).

---

## Testing & Quality Assurance

`redfolder` includes an automated test battery with **160 unit, integration, and property tests** (73 unit + 84 integration + 3 proptest suites) covering interval calculations, state machines, and resilience guarantees.

Run the test suite:

```bash
cargo test --all-targets --all-features
```

Run code formatting and linter checks:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
```

### Failure Modes & Edge Case Matrix

| Scenario / Injected Condition | Expected Invariant / System Behavior | Status |
| :--- | :--- | :---: |
| **HTTP 429 Rate Limit from API** | Retries transient 429 honoring `Retry-After`; falls back to local cache if exhausted. | Verified by `test_http_retry_on_429_with_recovery`, `test_http_retry_on_429_exhausted_falls_back_to_cache` |
| **Network Partition / Host Offline** | Transparently serves existing cached calendar until network recovers. | Verified by `test_offline_cache_fallback` |
| **Startup Failure (Network & Cache Fail)** | `start()` errors cleanly and resets state to `Stopped`; retries succeed without lockout. | Verified by `test_failed_startup_does_not_remain_running_and_can_retry` |
| **Rapid Restart / Task Overlap** | `stop()` cleanly joins previous tasks; restarting creates zero duplicate loops. | Verified by `test_stop_waits_for_background_tasks_and_rapid_restart`, `test_service_restart_lifecycle` |
| **Evaluation Loop Lower Bound** | `set_check_interval` rejects zero or sub-100ms durations to prevent busy loops. | Verified by `test_set_check_interval_validation` |
| **CalendarUpdated Event Dispatch** | Dispatches calendar sync updates to both broadcast bus and `EventListener` callbacks. | Verified by `test_calendar_updated_reaches_both_broadcast_and_event_listener` |
| **CalendarSyncFailed Event Dispatch** | Dispatches sync error event on network/parse failure; midnight task retries every 15m. | Verified by `test_calendar_sync_failed_event_reaches_listeners`, `test_calendar_sync_failed_event_dispatch` |
| **Duplicate Worker Registration Protection** | Rejects duplicate worker IDs with typed error; updates require `reregister_*`. | Verified by `test_duplicate_worker_registration_rejected`, `test_register_worker_duplicate_id_protection_and_reregister` |
| **Atomic Status Accessor** | `engine.status(&config)` eliminates TOCTOU races between status check and window retrieval. | Verified by `test_atomic_status_accessor_consistency`, `test_status_atomic_accessor` |
| **Cross-Platform Cache Resolution** | Resolves cache directory across Linux XDG, Windows `%LOCALAPPDATA%`/`%APPDATA%`, macOS, and temp. | Verified by `test_try_default_cache_dir_returns_valid_path`, `test_default_cache_dir` |
| **Atomic Cache Write Resilience** | Writes via PID + nanosecond + atomic counter tmp file and atomic rename; prevents write collision. | Verified by `test_atomic_cache_write_leaves_no_temporary_files`, `test_concurrent_save_cache_atomic` |
| **Concurrent Refresh Serialization** | Serializes concurrent `refresh()` / `force_refresh()` calls; eliminates race conditions. | Verified by `test_concurrent_refreshes_are_serialized` |
| **Cache Event Count Mismatch** | `load_cache_data` detects corrupted/truncated cache (`event_count != len`) and rejects it. | Verified by `test_cache_rejects_event_count_mismatch` |
| **Bounded Stale Cache Fallback** | Rejects cache older than `max_stale_cache_age` on network failure; accepts within limit. | Verified by `test_stale_cache_policy_enforcement` |
| **Legacy Cache Staleness Protection** | Rejects unversioned/unknown-age legacy cache files during stale fallback. | Verified by `test_legacy_cache_older_than_max_stale_age_rejected`, `test_legacy_cache_never_served_on_empty_feed` |
| **Impact & Currency Alias Normalization** | Matches `"Red"` vs `"High"`, `"med"` vs `"Medium"`, and currencies case-insensitively. | Verified by `test_impact_aliases_high_vs_red_normalization`, `test_wildcard_currency_and_case_insensitivity` |
| **Semantic Config & Blank Item Checks** | Rejects blank or empty strings in currency/impact lists; offers `validate_strict()`. | Verified by `test_empty_currency_and_impact_filters_are_rejected`, `test_validate_strict` |
| **All-Day & Tentative Events** | Default config ignores all-day/tentative events to avoid fake midnight spikes; 24h opt-in. | Verified by `test_all_day_and_tentative_event_policies`, `test_all_day_and_tentative_parsing` |
| **Missing Release Times** | Date-only events without time parsed as `TentativeDate`, preventing fake midnight spikes. | Verified by `test_date_only_event_without_time_is_tentative` |
| **Timezone-Aware All-Day Boundaries** | Interprets all-day events in configured calendar timezone (e.g. `America/New_York`). | Verified by `test_all_day_events_in_non_utc_timezone` |
| **Naive Timestamps & DST Transition** | Converts naive times with EDT/EST DST accuracy; gaps advance 1h to valid daylight instant. | Verified by `test_timezone_aware_naive_timestamp_and_dst`, `test_dst_spring_forward_gap_shift` |
| **Half-Open Interval Semantics** | Window is active at exact start and inactive at exact end `[start, end)`. | Verified by `test_event_window_end_is_exclusive`, `test_weekend_window_exact_end_boundary` |
| **Worker Registered in Active Blackout** | Newly registered worker immediately receives active blackout notification. | Verified by `test_immediate_notification_for_worker_registered_during_blackout` |
| **Overlapping Events within Threshold** | Merges closely spaced releases into single uninterrupted `BlackoutWindow`. | Verified by `test_overlapping_event_merging`, `test_window_merging_invariants` (proptest) |
| **Zero Pre-Event Buffer (`before_min: 0`)** | Window begins precisely at scheduled event time; does not default to 30 min. | Verified by `test_worker_specific_buffers_isolation` |
| **Weekend Curfew in `weekend` Mode** | Extends blackout window 51.5 hours through to Monday 00:00 UTC. | Verified by `test_weekend_boundaries_and_determinism`, `test_next_weekend_window_weekend_mode` |
| **Cross-Midnight Short Curfew** | 23:00 -> 01:00 curfew rolls over to Saturday without inverting intervals. | Verified by `test_cross_midnight_short_curfew` |
| **Deterministic Timestamp Replay** | Historical queries evaluate deterministically without depending on `Utc::now()`. | Verified by `test_timestamp_determinism_historical_and_future` |
| **Empty Upstream Feed Protection** | Preserves known-good cache if upstream returns 0 events or invalid array. | Verified by `test_cache_preservation_on_empty_response`, `test_empty_feed_accepts_recent_cache_but_flags_it` |
| **Service Concurrency & Restart** | Multiple `start()` calls error cleanly; `start -> stop -> start` restarts properly. | Verified by `test_service_lifecycle_guards_and_restart` |
| **OOM Response / Payload Size Exceeded** | `fetch_remote` enforces `max_response_bytes` via `Content-Length` pre-check and streaming chunk counter, rejecting oversized payloads. | Verified by `test_response_body_size_limit_rejection`, `test_cache_exceeding_max_response_bytes_is_rejected` |
| **Cache Tampering / Checksum Mismatch** | `load_cache_data` verifies SHA-256 digest against `CacheMetadata.sha256`; rejects tampered or corrupted files. | Verified by `test_cache_integrity_checksum_tamper_detection`, `test_tampered_fetched_at_is_rejected` |
| **Upstream Feed Primary Outage / Failover** | `fetch_remote` automatically fails over to configured secondary mirrors in order when primary fails. | Verified by `test_fallback_url_failover`, `test_corrupted_primary_fails_over_to_mirror` |
| **Cache File & Directory Permissions** | Enforces POSIX `0o700` directory and `0o600` file permissions on Unix systems to protect against unauthorized multi-user access. | Verified by `test_cache_file_permissions_unix`, `test_group_writable_cache_dir_is_refused` |
| **Sync Failure under Fail-Closed Policy** | Automatically injects a synthetic `Fail-Closed Safety Blackout` window for `FailClosed` workers, halting trading during feed outages. | Verified by `test_prop_firm_strict_defaults_to_fail_closed`, `test_fail_safe_closed_blackout` |
| **Retry Latency Ceiling Exceeded** | Aborts retry loop once cumulative duration exceeds `overall_timeout` (default 60s), avoiding stalled caller tasks. | Verified by `test_total_latency_bounded_across_dead_fallbacks`, `test_retry_latency_ceiling_bounded` |
| **Disabled Weekend Config Validation** | `validate()` unconditionally checks weekend curfew parameters even when `weekend_enabled` is false, preventing latent runtime bugs. | Verified by `test_weekend_validation_when_disabled`, `test_config_validation_negative_buffers_and_bad_formats` |
| **Back-to-Back News Interval Merging** | Merges overlapping releases into unified intervals verified via binary search and property tests. | Verified by `test_back_to_back_news_merge`, `test_window_merging_invariants` (proptest) |
| **Weekend & Economic Overlap** | Seamlessly connects late Friday economic releases with weekend market curfews without coverage gap. | Verified by `test_weekend_and_economic_overlap`, `test_weekend_and_news_overlap` |
| **Stale Calendar Policy: Fail-Open** | Maintains normal trading execution when calendar synchronization fails under `FailOpen` mode. | Verified by `test_cold_start_fail_open`, `test_gate_three_state_flow_and_staleness_safety` |
| **Stale Calendar Policy: Fail-Closed** | Defensively triggers continuous safety blackout during feed outages under `FailClosed` prop firm mode. | Verified by `test_cold_start_fail_closed`, `test_gate_three_state_flow_and_staleness_safety` |
| **Daylight Saving Time (DST) Transitions** | Resolves spring-forward gaps and fall-back ambiguities in US Eastern / configured timezones deterministically. | Verified by `test_dst_transition`, `test_dst_boundary_parsing` |
| **URL Security Policy & SSRF Blocking** | Rejects unencrypted HTTP endpoints, non-whitelisted hosts, private/internal IPs, and excessive HTTP redirects via configurable `UrlPolicy`. | Verified by `test_url_policy_rejects_http_in_fetch`, `test_url_policy_blocks_redirect_to_private_ip`, `test_url_policy_allows_matching_allowed_host_and_blocks_unmatched`, `test_url_policy_redirect_limit_enforced` |
| **CLI Binary Subcommands & JSON Output** | Verifies `status`, `upcoming`, and flag parsing across isolated environments via command execution. | Verified by `test_cli_binary_execution`, `test_cli_status_provenance_json_and_exit_codes`, `test_cli_health_command` |
| **Fuzzing & Invariant Testing** | Property-based fuzzing of timing strings, corrupt cache bytes, and interval merge invariants. | Verified by `test_window_merging_invariants`, `test_parse_timing_adversarial_input_never_panics`, `test_cache_loading_arbitrary_bytes_never_panics` (proptest) |
| **Service Supervision & Task Restarts** | Supervised background worker tasks auto-restart on unexpected panics and increment restart counters. | Verified by `test_spawn_supervised_restarts_panicking_task`, `test_service_health_endpoint_fresh_degraded_stale` |
| **Ordered Bounded Event Delivery** | Dispatches listener callbacks sequentially in FIFO order without unbound memory growth or channel starvation. | Verified by `test_listener_sequential_ordered_delivery`, `test_listener_bounded_queue_drops_excess_events` |

---

### Performance & Empirical Benchmarks

`redfolder` includes a Criterion benchmark battery measuring engine compilation, dynamic worker window derivation, and blackout query latency.

#### Benchmark Environment & Methodology
- **Hardware / Architecture**: Representative measurements on modern x86_64 hardware (AMD Ryzen / Intel Core class, Linux x86_64, release mode with link-time optimization). Actual latencies vary based on CPU clock, cache hierarchy, and system load.
- **Dataset**: 50 raw macroeconomic calendar events across 4 currencies (USD, EUR, GBP, JPY).
- **Configuration**: Strict prop firm challenge rules (`prop_firm_strict`: 8 currencies, 5-minute pre/post buffers, weekend curfew enabled).

#### Empirical Criterion Measurements

The following representative measurements illustrate sub-microsecond to low-microsecond throughput under typical server workloads:

| Operation | Benchmark Name | Latency (Mean) | 95% Confidence Interval | Description |
| :--- | :--- | :---: | :---: | :--- |
| **Engine Compilation** | `engine_compile_50_events` | **24.02 µs** | [23.57 µs – 24.52 µs] | Parses, sorts, filters, and merges 50 raw events into UTC intervals |
| **Runtime Blackout Check** | `is_blackout_query` | **12.26 µs** | [12.17 µs – 12.37 µs] | Evaluates active blackout state across currencies, impacts, and weekend curfew |
| **Deterministic Timestamp Query** | `is_blackout_at_query` | **10.76 µs** | [10.60 µs – 10.96 µs] | Historical or future evaluation point query for tick-level backtesting |
| **Worker Window Derivation** | `windows_for_config` | **13.10 µs** | [12.92 µs – 13.36 µs] | Derives isolated worker-specific blackout intervals from parsed events |
| **Atomic Status Accessor** | `status_query` | **13.09 µs** | [13.01 µs – 13.17 µs] | Atomically evaluates status and extracts active window in a single pass |

To reproduce these benchmarks:
```bash
cargo bench --bench blackout_benchmark
```

---

## Troubleshooting & FAQ

#### 1. "HTTP 429 Too Many Requests from nfs.faireconomy.media"
- **Cause**: FairEconomy / Cloudflare enforces strict rate limits against automated clients.
- **Fix**: `redfolder` automatically caches events locally to avoid frequent requests. Avoid calling `fetch_remote()` in tight loops; use `fetch_or_cached()` or run `RedFolderService` which syncs once daily at midnight UTC.

#### 2. "Why is a blackout active even though the event time passed?"
- **Cause**: The configuration specifies an `after_min` post-event buffer (e.g. 15 or 30 minutes). Volatility and spread expansion persist after high-impact announcements.
- **Fix**: Adjust `buffer_minutes(before, after)` or inspect `window.end` to view when spreads are projected to normalize.

#### 3. "How do I filter for multiple currencies simultaneously?"
- **Cause**: Using single-currency configs on cross-pairs (e.g. trading EUR/USD requires monitoring both EUR and USD events).
- **Fix**: Pass all relevant currency codes to the builder:
  ```rust
  let config = RedFolderConfig::builder()
      .currencies(vec!["EUR", "USD"])
      .build();
  ```

#### 4. "Can I run redfolder without local file caching?"
- **Cause**: Embedded systems or restricted environments where filesystem access is read-only.
- **Fix**: Instantiate the client with `CalendarClient::without_cache()`.

#### 5. "How do I configure backup mirrors or custom endpoints?"
- **Cause**: Institutional setups requiring internal proxies or redundant mirrors if the primary feed is unreachable.
- **Fix**: Chain `.with_fallback_url()` or `.with_fallback_urls()` on the client:
  ```rust
  let client = CalendarClient::new(None)
      .with_fallback_url("https://internal-mirror.corp/calendar.json");
  let service = RedFolderService::with_client(client);
  ```

#### 6. "How does redfolder protect prop firm accounts during feed outages?"
- **Cause**: If external feeds are blocked and the local cache expires, trading during an unannounced CPI release would breach prop firm rules.
- **Fix**: Use [`RedFolderConfig::prop_firm_strict()`](#redfolderconfig) or explicitly configure `.fail_safe_mode(FailSafeMode::FailClosed)`. If synchronization fails or the cache exceeds staleness limits, a synthetic safety blackout window is immediately enforced, pausing trades until verified calendar data is restored.

---

## Author & Contributions

Created and maintained by [**0xbarss**](https://github.com/0xbarss).

Contributions, bug reports, and suggestions are welcome! Please check out [**CONTRIBUTING.md**](CONTRIBUTING.md) for architecture guidelines, code standards, and local testing instructions before opening a pull request at [**github.com/0xbarss/redfolder**](https://github.com/0xbarss/redfolder).

---

## License & Disclaimer

This project is licensed under the **[MIT License](LICENSE)**.

> **Disclaimer**: This is an open-source tool designed for risk management and educational purposes. Trading foreign exchange, CFDs, and cryptocurrencies carries high risk. Past calendar accuracy does not guarantee future event timing. Always verify news rules with your broker or prop firm before deploying automated algorithms.
