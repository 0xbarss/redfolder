# redfolder

[![Crates.io](https://img.shields.io/crates/v/redfolder.svg)](https://crates.io/crates/redfolder)
[![Docs.rs](https://docs.rs/redfolder/badge.svg)](https://docs.rs/redfolder)
[![CI](https://github.com/0xbarss/redfolder/actions/workflows/ci.yml/badge.svg)](https://github.com/0xbarss/redfolder/actions/workflows/ci.yml)
[![Rust](https://img.shields.io/badge/rust-1.75%2B-orange.svg)](https://www.rust-lang.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Author](https://img.shields.io/badge/author-0xbarss-purple.svg)](https://github.com/0xbarss)

A high-performance, asynchronous economic calendar client and event-driven trading blackout engine for Rust algorithmic trading systems, quantitative funds, and prop firm challenges.

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

Building economic news protection from scratch usually results in brittle HTTP pollers that fail during rate limits or fail to cluster back-to-back releases. `redfolder` solves this with an institutional-grade, zero-alloc in-memory engine:

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
│             (Local Disk Cache + Retry Fallback)           │
└─────────────────────────────┬─────────────────────────────┘
                              │ RawCalendarEvents
┌─────────────────────────────▼─────────────────────────────┐
│                       BlackoutEngine                      │
│        - Timezone Normalization (RFC-3339 & AM/PM)        │
│        - Overlapping Window Merge (Gap Threshold)         │
│        - Weekend Curfew Injection (Short / Weekend)       │
└─────────────────────────────┬─────────────────────────────┘
                              │ Compiled BlackoutWindows
┌─────────────────────────────▼─────────────────────────────┐
│                     RedFolderService                      │
│        - Daily Midnight UTC Sync Loop                     │
│        - 15-Second Non-Blocking State Monitor             │
│        - Advance Warning Generator                        │
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
- `RedFolderEvent::BlackoutStarted`: Emitted the instant a blackout window becomes active. Directs execution modules to reject incoming trade signals and pause active scalpers.
- `RedFolderEvent::BlackoutEnded`: Emitted when the window closes, signaling strategies that market conditions and spreads have normalized.
- `RedFolderEvent::CalendarUpdated`: Emitted when new weekly schedules are synchronized and indexed into memory.

### Dynamic Window Clustering & Merging

Economic events rarely occur in isolation. A single trading day may schedule:
- 13:30 UTC: US Consumer Price Index (CPI)
- 14:00 UTC: FOMC Member Speech
- 14:15 UTC: Industrial Production

If configured with a 30-minute buffer and a 30-minute merge threshold, `redfolder` automatically identifies that the tail of the first window intersects or lies within the threshold of the next. It clusters all related events into a single unified `BlackoutWindow`, eliminating rapid oscillation between active and inactive states.

### Weekend Market Close Curfew Engine

Forex markets close on Friday evening and reopen on Sunday afternoon, exposing open positions to weekend gap risk. `redfolder` provides dedicated weekend curfew management:
- **`short` mode**: Initiates a blackout at a configurable Friday UTC time (e.g. `20:30 UTC`) and ends at market close (`21:00 UTC`), preventing late-Friday slippage.
- **`weekend` mode**: Holds trading closed through Friday evening until Monday 00:00 UTC, preventing over-the-weekend exposure.

### Offline Durability & Rate Limit Resilience

External calendar APIs enforce strict Cloudflare rate limiting (HTTP 429). `CalendarClient` includes a multi-tier fallback mechanism:
1. Fresh remote synchronization writes a pretty-printed JSON copy to disk (`economic_calendar.json`).
2. If remote requests return errors, timeouts, or HTTP 429 rate limits, the client automatically loads the cached calendar from disk and emits a warning through `tracing`.
3. An internal lock-split design ensures network downloads execute without holding the service mutex, guaranteeing that 15-second worker evaluations are never stalled by remote latency.

---

## Key Features

- **Zero Unsound Dependencies**: Pure Rust implementation with strict compile-time invariants.
- **Microsecond In-Memory Matching**: Compiles events into pre-sorted UTC intervals for sub-microsecond query performance during order placement.
- **Multi-Worker Granularity**: Assign separate configurations to different strategies or symbols (e.g. `EURUSD` bot monitors USD + EUR; `GBPJPY` bot monitors GBP + JPY).
- **Multiple Integration Channels**: Consume events via `tokio::sync::broadcast`, per-worker `tokio::sync::mpsc`, or asynchronous `EventListener` traits.
- **Prop Firm Preset Out of the Box**: One-line configuration matching FTMO, FundedNext, and The5ers rules.
- **Terminal CLI Included**: Real-time terminal watcher and JSON query interface.

---

## Repository Structure

```text
redfolder/
├── .github/workflows/ci.yml       # Automated GitHub Actions CI (Check, Test, Clippy, Fmt)
├── Cargo.toml                     # Crate definition, metadata, and optional CLI dependencies
├── LICENSE                        # MIT License
├── README.md                      # Architecture documentation and usage guide
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
| `subscribe` | `fn(&self) -> broadcast::Receiver<RedFolderEvent>` | Subscribes to global broadcast bus receiving all system events. |
| `add_listener` | `async fn(&self, Arc<dyn EventListener>)` | Attaches an asynchronous trait-based callback listener. |
| `register_worker_events` | `async fn(&self, &str, RedFolderConfig) -> mpsc::UnboundedReceiver<RedFolderEvent>` | Registers worker and returns a typed stream of scoped events. |
| `register_worker` | `async fn(&self, &str, RedFolderConfig) -> mpsc::UnboundedReceiver<BlackoutNotification>` | Legacy worker registration returning state change notifications. |
| `unregister_worker` | `async fn(&self, &str)` | Unregisters worker and cleans up internal state tracking. |
| `start` | `async fn(&self) -> Result<()>` | Starts background daily refresh and 15s evaluation tasks. |
| `stop` | `async fn(&self)` | Gracefully terminates all background tasks. |
| `refresh` | `async fn(&self) -> Result<()>` | Forces immediate network download and window recompilation. |
| `is_blackout` | `async fn(&self, &str) -> bool` | Checks if a specific registered worker is currently in blackout. |
| `current_window` | `async fn(&self, &str) -> Option<BlackoutWindow>` | Returns active window details for a specific worker. |
| `get_upcoming_blackouts` | `async fn(&self, &str, u32) -> Vec<BlackoutWindow>` | Returns upcoming blackout windows within $N$ hours. |

### BlackoutEngine

| Method | Signature | Description |
| :--- | :--- | :--- |
| `compile` | `fn(&[RawCalendarEvent], &[&RedFolderConfig], DateTime<Utc>) -> Self` | Compiles raw releases and curfew rules into merged blackout intervals. |
| `is_blackout` | `fn(&self, &RedFolderConfig) -> bool` | Checks if the current UTC time falls within any active window. |
| `is_blackout_at` | `fn(&self, &RedFolderConfig, DateTime<Utc>) -> bool` | Evaluates blackout status at an arbitrary timestamp. |
| `current_window` | `fn(&self, &RedFolderConfig) -> Option<BlackoutWindow>` | Returns the active `BlackoutWindow` matching configuration. |
| `upcoming_blackouts` | `fn(&self, &RedFolderConfig, u32) -> Vec<BlackoutWindow>` | Lists matching windows starting within $N$ hours. |

### CalendarClient

| Method | Signature | Description |
| :--- | :--- | :--- |
| `new` | `fn(Option<PathBuf>) -> Self` | Creates client with standard browser User-Agent and default cache. |
| `without_cache` | `fn() -> Self` | Creates client strictly performing live network requests. |
| `with_user_agent` | `fn(Option<PathBuf>, &str) -> Result<Self>` | Creates client with custom User-Agent and optional cache directory. |
| `fetch_remote` | `async fn(&self) -> Result<Vec<RawCalendarEvent>>` | Performs HTTP GET against FairEconomy weekly feed. |
| `fetch_or_cached` | `async fn(&self) -> Result<Vec<RawCalendarEvent>>` | Fetches remote schedule, persisting cache or falling back to disk on failure. |
| `save_cache` | `fn(&self, &[RawCalendarEvent]) -> Result<()>` | Writes JSON-serialized events to local disk. |
| `load_cache` | `fn(&self) -> Option<Vec<RawCalendarEvent>>` | Reads and deserializes cached events from disk. |

### Types & Domain Models

- **`RedFolderEvent`**: Typed enum (`BlackoutWarning`, `BlackoutStarted`, `BlackoutEnded`, `CalendarUpdated`).
- **`BlackoutWindow`**: Struct containing `start: DateTime<Utc>`, `end: DateTime<Utc>`, and `events: Vec<WindowEvent>`. Provides helper methods `remaining_minutes()`, `duration_minutes()`, `is_active()`, and `summary_title()`.
- **`Impact`**: Enum with variants `High`, `Medium`, `Low`, `NonEconomic`, `Custom(String)`. Supports case-insensitive string matching (`"red"`, `"high"`).
- **`WeekendMode`**: Enum with variants `Short` (Friday evening window) and `Weekend` (Friday evening through Monday 00:00 UTC). Validated strictly on parse (rejecting typos or unknown strings with a descriptive error).

---

## Testing & Quality Assurance

`redfolder` includes an automated test battery with 33 unit and integration tests covering interval calculations, state machines, and resilience guarantees.

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
| **Overlapping Events within Threshold** | Merges closely spaced releases into single uninterrupted `BlackoutWindow`. | Verified |
| **Events Separated Beyond Threshold** | Preserves distinct blackout windows; does not merge prematurely. | Verified |
| **Zero Pre-Event Buffer (`before_min: 0`)** | Window begins precisely at scheduled event time; does not default to 30 min. | Verified |
| **Active Post-Release Window** | Events whose release time passed but post-buffer is active remain indexed. | Verified |
| **Friday Past Curfew Start** | Rollover logic safely computes window for following Friday without panic. | Verified |
| **Weekend Curfew in `weekend` Mode** | Extends blackout window 51.5 hours through to Monday 00:00 UTC. | Verified |
| **Wildcard Currency (`"ALL"` / `"Global"`)** | Event automatically matches all worker currency filters. | Verified |
| **Case-Insensitive Impact Strings** | `"red"`, `"HIGH"`, and `"High"` all correctly parse to `Impact::High`. | Verified |
| **Advance Warning Generation** | `BlackoutWarning` emits exactly once per window within specified horizon. | Verified |
| **Worker Unregistration** | Cleanly terminates state tracking; subsequent queries return inactive. | Verified |
| **Corrupted JSON Disk Cache** | Bypasses corrupted cache file without panicking and attempts clean fetch. | Verified |

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

Contributions, bug reports, and suggestions are welcome. Please ensure that all pull requests pass `cargo test`, `cargo clippy --all-targets --all-features -- -D warnings`, and `cargo fmt --all -- --check`. Feel free to open an issue or pull request at [**github.com/0xbarss/redfolder**](https://github.com/0xbarss/redfolder).

---

## License & Disclaimer

This project is licensed under the **[MIT License](LICENSE)**.

> **Disclaimer**: This is an open-source tool designed for risk management and educational purposes. Trading foreign exchange, CFDs, and cryptocurrencies carries high risk. Past calendar accuracy does not guarantee future event timing. Always verify news rules with your broker or prop firm before deploying automated algorithms.
