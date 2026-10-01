# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.0] - 2026-10-01

### Breaking Changes / Behavioral Incompatibilities

- **Empty Feed Protection**: An empty upstream feed response with no acceptable cache now returns `Err(RedFolderError::EmptyFeed)` instead of silently returning `Ok(vec![])`.
- **Cache Bypass Enforcement**: `CalendarClient::force_fetch()` and `RedFolderService::force_refresh()` strictly require a successful remote network response and will never return cached data under any circumstances.
- **Data-Timestamp Staleness Tracking**: Staleness is measured from the upstream provider fetch instant (`data_fetched_at`) rather than the local call time. Serving from cache preserves the original data age.
- **Unsuppressed Degraded Retries**: A degraded synchronization fallback (serving cached data during an upstream outage) no longer suppresses subsequent retries. The service executes an exponential retry ladder (1m → 2m → 5m → 15m) until fresh data is verified.
- **Exclusive Half-Open Window End**: Event blackout intervals strictly use half-open semantics `[start, end)` where the end instant is non-inclusive (`is_active_at(end) == false`).
- **Default Bounded Overall Latency**: Network fetch operations now enforce a default 60-second overall wall-clock timeout (`DEFAULT_OVERALL_TIMEOUT`) across all retries and fallback attempts.
- **Strict Buffer Bounds & Engine Validation**: Configurations with pre/post-buffers greater than 7 days (10,080 minutes) or negative values are rejected at worker registration and fail closed within the blackout engine.

### Added

- **Service Health Diagnostics (`RF-15`)**: Added `service.health().await` returning a `ServiceHealth` struct exposing `stale`, `degraded`, `last_sync_time`, `last_sync_error`, `registered_workers`, `total_windows`, and `background_task_restarts`.
- **Three-State Order Safety Gate (`RF-15`)**: Added `service.gate(worker_id).await` returning `Gate::Allowed`, `Gate::Blocked(BlackoutWindow)`, or `Gate::Unknown(String)` to protect prop firm execution against TOCTOU races and stale feeds.
- **Auto-Restarting Supervisor (`RF-15`)**: Background evaluation and sync loops run under an auto-restarting supervisor task (`spawn_supervised`), catching panics and reporting restart telemetry.
- **Ordered & Bounded Listener Delivery (`RF-16`)**: Added bounded FIFO channels (1024 slots) per listener to eliminate unbounded memory growth and guarantee event ordering without slow sinks blocking alerts.
- **Sequenced Monotonic Event Bus (`RF-16`)**: Added `service.subscribe_sequenced()` delivering `SequencedEvent` wrapping `RedFolderEvent` with monotonic sequence IDs and UTC timestamps.
- **Mandatory Worker Start Check**: Added `service.start_required()` returning `Err(RedFolderError::NoRegisteredWorkers)` if invoked with zero enabled workers.
- **CLI Health Command**: Added terminal subcommand `redfolder health` with human-readable diagnostic tables and structured `--json` output.
- **Fuzz & Property-Based Testing**: Added proptest suite (`tests/property_tests.rs`) covering window merging invariants, timing string parsing fuzzing, and corrupted cache recovery.
- **SSRF Protection & URL Security Policy**: Added `UrlPolicy` enforcing HTTPS schemes, hostname allow-lists, loopback/private/link-local IP blocking, and HTTP redirect ceilings.
- **Cryptographic Cache Integrity**: Added SHA-256 integrity checksums (`CacheMetadata.sha256`) and strict POSIX `0o600` / `0o700` filesystem permission validation.

### Changed

- **Code Safety**: Enforced crate-wide `#![forbid(unsafe_code)]` in `src/lib.rs`.
- **CLI Output Formatting**: Enhanced CLI `status`, `upcoming`, and `health` commands with ANSI styling and machine-readable JSON flags.
- **Documentation & Verification**: Updated `README.md` with complete failure modes and edge case test citation matrix and Criterion performance benchmarks.

### Fixed

- Fixed self-deadlock in background supervisor restart counter by properly scoping `MutexGuard` lifetimes.
- Fixed potential TOCTOU race in engine status checks via atomic `engine.status(&config)` query method.
- Fixed potential time gap vulnerability when converting naive calendar timestamps across daylight saving time (DST) transitions.

## [1.0.0] - 2026-03-15

### Added

- Initial production release of `redfolder`.
- Async macroeconomic calendar fetching and caching from FairEconomy feeds.
- In-memory blackout engine with binary search window lookup.
- Weekend curfew management (`Short` and `Weekend` modes).
- Event-driven notifications via Tokio broadcast and worker-specific mpsc channels.
- Built-in presets for prop firm compliance (`prop_firm_strict`, `conservative`, `crypto_curfew`).
- Terminal command-line interface (`redfolder`).
