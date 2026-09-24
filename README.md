# redfolder

[![Crates.io](https://img.shields.io/crates/v/redfolder.svg)](https://crates.io/crates/redfolder)
[![Docs.rs](https://docs.rs/redfolder/badge.svg)](https://docs.rs/redfolder)
[![CI](https://github.com/0xbarss/redfolder/actions/workflows/ci.yml/badge.svg)](https://github.com/0xbarss/redfolder/actions/workflows/ci.yml)
[![Rust](https://img.shields.io/badge/rust-1.75%2B-orange.svg)](https://www.rust-lang.org)
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
└─────────────────────────────┬─────────────────────────────┘
                              │ Compiled BlackoutWindows
┌─────────────────────────────▼─────────────────────────────┐
│                     RedFolderService                      │
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

### Offline Durability & Rate Limit Resilience

External calendar APIs enforce strict Cloudflare rate limiting (HTTP 429). `CalendarClient` includes a multi-tier fallback mechanism:
1. Fresh remote synchronization writes a pretty-printed JSON copy to disk with schema metadata (`version`, `event_count`, `fetched_at`).
2. When loading from disk, the client validates that `metadata.event_count == events.len()`, rejecting corrupted or truncated cache files.
3. Fallback to stale cache on network failure respects a configurable maximum allowable staleness boundary (`max_stale_cache_age`, default 36 hours), preventing ancient calendars from silently driving live trading.
4. An internal lock-split design ensures network downloads execute without holding the service mutex, guaranteeing that worker evaluations are never stalled by remote latency.

---

## Key Features

- **Zero Unsound Dependencies**: Pure Rust implementation with strict compile-time invariants.
- **Fast In-Memory Matching**: Compiles events into pre-sorted UTC intervals for single-digit microsecond query latency (~6–8 µs) during order placement, verified with Criterion benchmarks.
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
    └── integration_tests.rs       # End-to-end integration test battery
```

---

## Installation

Add `redfolder` to your project's `Cargo.toml`:

```toml
[dependencies]
redfolder = "0.1"
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

    // 4. Query blackout state
    if engine.is_blackout(&config) {
        let current = engine.current_window(&config).unwrap();
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
    let mut events = service.register_worker_events("eurusd_scalper", config).await;
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

let mut usd_events = service.register_worker_events("usd_bot", usd_config).await;
let mut eur_events = service.register_worker_events("eur_bot", eur_config).await;
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

---

## API Reference

### RedFolderService

| Method | Signature | Description |
| :--- | :--- | :--- |
| `new` | `fn(Option<PathBuf>) -> Self` | Initializes service with optional cache directory (defaults to `~/.cache/redfolder/`). |
| `state` | `async fn(&self) -> ServiceState` | Returns the current lifecycle state (`Stopped`, `Starting`, `Running`, `Stopping`). |
| `is_running` | `async fn(&self) -> bool` | Checks if background worker tasks are active (`state == ServiceState::Running`). |
| `subscribe` | `fn(&self) -> broadcast::Receiver<RedFolderEvent>` | Subscribes to global broadcast bus receiving all system events. |
| `add_listener` | `async fn(&self, Arc<dyn EventListener>)` | Attaches an asynchronous trait-based callback listener (guarded with execution timeout). |
| `register_worker_events` | `async fn(&self, &str, RedFolderConfig) -> mpsc::UnboundedReceiver<RedFolderEvent>` | Registers worker, immediate state check, and returns a typed stream of scoped events. |
| `register_worker` | `async fn(&self, &str, RedFolderConfig) -> mpsc::UnboundedReceiver<BlackoutNotification>` | Legacy worker registration returning state change notifications. |
| `unregister_worker` | `async fn(&self, &str)` | Unregisters worker, cleans up state, and recalculates transition schedule. |
| `windows_for_worker` | `async fn(&self, &str) -> Vec<BlackoutWindow>` | Returns active and upcoming blackout windows derived specifically for the worker's buffers. |
| `start` | `async fn(&self) -> Result<()>` | Starts background daily refresh and transition-driven evaluation tasks. Reversible on error. |
| `stop` | `async fn(&self)` | Gracefully terminates all background tasks and awaits their exit. |
| `refresh` | `async fn(&self) -> Result<()>` | Synchronizes calendar and recompiles blackout windows (allowing cached fallback). |
| `force_refresh` | `async fn(&self) -> Result<()>` | Forces immediate network download from remote API and window recompilation, bypassing cache. |
| `is_blackout` | `async fn(&self, &str) -> bool` | Checks if a specific registered worker is currently in blackout. |
| `current_window` | `async fn(&self, &str) -> Option<BlackoutWindow>` | Returns active window details for a specific worker. |
| `get_upcoming_blackouts` | `async fn(&self, &str, u32) -> Vec<BlackoutWindow>` | Returns upcoming blackout windows within $N$ hours. |

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
| `upcoming_blackouts` | `fn(&self, &RedFolderConfig, u32) -> Vec<BlackoutWindow>` | Lists matching windows starting within $N$ hours. |

### CalendarClient

| Method | Signature | Description |
| :--- | :--- | :--- |
| `new` | `fn(Option<PathBuf>) -> Self` | Creates client with standard browser User-Agent and default cache. |
| `without_cache` | `fn() -> Self` | Creates client strictly performing live network requests. |
| `with_user_agent` | `fn(Option<PathBuf>, &str) -> Result<Self>` | Creates client with custom User-Agent and optional cache directory. |
| `with_timezone` | `fn(self, chrono_tz::Tz) -> Self` | Configures default source timezone for resolving naive calendar timestamps. |
| `with_max_stale_age` | `fn(self, Option<Duration>) -> Self` | Sets maximum allowable cache age for fallback on network failure. |
| `fetch_remote` | `async fn(&self) -> Result<Vec<RawCalendarEvent>>` | Performs HTTP GET against FairEconomy weekly feed with retry backoff. |
| `force_fetch` | `async fn(&self) -> Result<Vec<RawCalendarEvent>>` | Fetches fresh schedule directly from remote API, bypassing cache while saving to disk. |
| `fetch_or_cached` | `async fn(&self) -> Result<Vec<RawCalendarEvent>>` | Fetches remote schedule, verifying cache metadata and bounded staleness on failure. |
| `save_cache` | `fn(&self, &[RawCalendarEvent]) -> Result<()>` | Atomically writes JSON-serialized events with schema metadata (`version`, `event_count`). |
| `load_cache_data` | `fn(&self) -> Option<CachedCalendarData>` | Loads structured cache validating `event_count == events.len()`. |

### Types & Domain Models

- **`RedFolderEvent`**: Typed enum (`BlackoutWarning`, `BlackoutStarted`, `BlackoutEnded`, `CalendarUpdated`).
- **`EventTiming`**: Precision timing enum (`Exact(DateTime<Utc>)`, `AllDay(NaiveDate)`, `TentativeDate(NaiveDate)`).
- **`ServiceState`**: Service lifecycle enum (`Stopped`, `Starting`, `Running`, `Stopping`).
- **`BlackoutWindow`**: Struct containing `start: DateTime<Utc>`, `end: DateTime<Utc>`, and `events: Vec<WindowEvent>`. Uses standard half-open interval semantics `[start, end)`. Provides helper methods `remaining_minutes()`, `duration_minutes()`, `is_active()`, and `summary_title()`.
- **`Impact`**: Enum with variants `High`, `Medium`, `Low`, `NonEconomic`, `Custom(String)`. Supports case-insensitive string matching (`"red"`, `"high"`).
- **`WeekendMode`**: Enum with variants `Short` (Friday evening window) and `Weekend` (Friday evening through Monday 00:00 UTC). Validated strictly on parse (rejecting typos or unknown strings with a descriptive error).

---

## Testing & Quality Assurance

`redfolder` includes an automated test battery with **60 unit and integration tests** covering interval calculations, state machines, and resilience guarantees.

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
| **HTTP 429 Rate Limit from API** | Intercepts error; falls back to local disk cache without throwing an error. | Verified |
| **Network Partition / Host Offline** | Transparently serves existing cached calendar until network recovers. | Verified |
| **Startup Failure (Network & Cache Fail)** | `start()` errors cleanly and resets state to `Stopped`; retries succeed without lockout. | Verified |
| **Rapid Restart / Task Overlap** | `stop()` cleanly joins previous tasks; restarting creates zero duplicate loops. | Verified |
| **Transition-Driven Timing** | Emits alerts at exact scheduled second rather than waiting for 15s polling cycle. | Verified |
| **CalendarUpdated Event Dispatch** | Dispatches calendar sync updates to both broadcast bus and `EventListener` callbacks. | Verified |
| **Atomic Cache Write Resilience** | Writes via temp file and atomic rename; crashes leave no truncated cache or `.tmp` files. | Verified |
| **Concurrent Refresh Serialization** | Serializes concurrent `refresh()` / `force_refresh()` calls; eliminates race conditions. | Verified |
| **Cache Event Count Mismatch** | `load_cache_data` detects corrupted/truncated cache (`event_count != len`) and rejects it. | Verified |
| **Bounded Stale Cache Fallback** | Rejects cache older than `max_stale_cache_age` on network failure; accepts within limit. | Verified |
| **Legacy Cache Staleness Protection** | Rejects unversioned/unknown-age legacy cache files during stale fallback. | Verified |
| **Impact & Currency Alias Normalization** | Matches `"Red"` vs `"High"`, `"med"` vs `"Medium"`, and currencies case-insensitively. | Verified |
| **Empty Currency / Impact Filter** | Builder rejects empty filters (`currencies: []`, `impacts: []`) with descriptive errors. | Verified |
| **All-Day & Tentative Events** | Default config ignores all-day/tentative events to avoid fake midnight spikes; 24h opt-in. | Verified |
| **Timezone-Aware All-Day Boundaries** | Interprets all-day events in configured calendar timezone (e.g. `America/New_York`). | Verified |
| **Naive Timestamps & DST Transition** | Converts naive times via configured timezone (`America/New_York`) with EDT/EST DST accuracy. | Verified |
| **Half-Open Interval Semantics** | Window is active at exact start and inactive at exact end `[start, end)`. | Verified |
| **Worker Registered in Active Blackout** | Newly registered worker immediately receives active blackout notification. | Verified |
| **Overlapping Events within Threshold** | Merges closely spaced releases into single uninterrupted `BlackoutWindow`. | Verified |
| **Zero Pre-Event Buffer (`before_min: 0`)** | Window begins precisely at scheduled event time; does not default to 30 min. | Verified |
| **Weekend Curfew in `weekend` Mode** | Extends blackout window 51.5 hours through to Monday 00:00 UTC. | Verified |
| **Cross-Midnight Short Curfew** | 23:00 -> 01:00 curfew rolls over to Saturday without inverting intervals. | Verified |
| **Deterministic Timestamp Replay** | Historical queries evaluate deterministically without depending on `Utc::now()`. | Verified |
| **Empty Upstream Feed Protection** | Preserves known-good cache if upstream returns 0 events or invalid array. | Verified |
| **Service Concurrency & Restart** | Multiple `start()` calls error cleanly; `start -> stop -> start` restarts properly. | Verified |

---

### Performance & Empirical Benchmarks

`redfolder` includes a Criterion benchmark battery measuring engine compilation, dynamic worker window derivation, and blackout query latency.

#### Benchmark Environment & Methodology
- **Hardware / OS**: Linux x86_64, release mode with link-time optimization.
- **Dataset**: 50 raw macroeconomic calendar events across 4 currencies (USD, EUR, GBP, JPY).
- **Configuration**: Strict prop firm challenge rules (`prop_firm_strict`: 8 currencies, 5-minute pre/post buffers, weekend curfew enabled).

#### Empirical Criterion Measurements

| Operation | Benchmark Name | Latency (Mean) | 95% Confidence Interval | Description |
| :--- | :--- | :---: | :---: | :--- |
| **Engine Compilation** | `engine_compile_50_events` | **17.31 µs** | [17.21 µs – 17.42 µs] | Parses, sorts, filters, and merges 50 raw events into UTC intervals |
| **Runtime Blackout Check** | `is_blackout_query` | **8.55 µs** | [8.50 µs – 8.60 µs] | Evaluates active blackout state across currencies, impacts, and weekend curfew |
| **Deterministic Timestamp Query** | `is_blackout_at_query` | **6.57 µs** | [6.54 µs – 6.60 µs] | Historical or future evaluation point query for tick-level backtesting |
| **Worker Window Derivation** | `windows_for_config` | **8.74 µs** | [8.69 µs – 8.80 µs] | Derives isolated worker-specific blackout intervals from parsed events |

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

---

## Author & Contributions

Created and maintained by [**0xbarss**](https://github.com/0xbarss).

Contributions, bug reports, and suggestions are welcome! Please check out [**CONTRIBUTING.md**](CONTRIBUTING.md) for architecture guidelines, code standards, and local testing instructions before opening a pull request at [**github.com/0xbarss/redfolder**](https://github.com/0xbarss/redfolder).

---

## License & Disclaimer

This project is licensed under the **[MIT License](LICENSE)**.

> **Disclaimer**: This is an open-source tool designed for risk management and educational purposes. Trading foreign exchange, CFDs, and cryptocurrencies carries high risk. Past calendar accuracy does not guarantee future event timing. Always verify news rules with your broker or prop firm before deploying automated algorithms.
