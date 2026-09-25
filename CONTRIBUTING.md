# Contributing to RedFolder

Thank you for your interest in contributing to **RedFolder**! This document provides guidelines and instructions for contributing to the project.

---

## Architecture & Design Principles

RedFolder is built for automated trading environments, prop firm risk management, and algorithmic execution. Because of this domain, changes must adhere to key principles:

1. **Deterministic & Isolated**: Worker configurations and blackout windows must never leak across strategies or symbols. Worker timing buffers and impacts must remain completely isolated.
2. **Zero Unsound Invariants**: All interval calculations, timezone conversions, and currency parsing must be strictly verified with tests. Never use silent fallbacks that hide configuration errors or data truncation.
3. **Resilience by Default**: External network requests can fail or hit rate limits (Cloudflare HTTP 429). The system must handle offline cache fallback and retry semantics gracefully without stalling worker evaluation loops.
4. **Zero Warnings**: Code must compile on stable Rust with zero warnings from both `rustc` and `cargo clippy`.

---

## Development Setup

### Prerequisites

- **Rust**: Stable toolchain (Rust 1.75+ / 2021 edition).
- **Cargo components**: `rustfmt` and `clippy`.

```bash
rustup update stable
rustup component add rustfmt clippy
```

### Building the Project

```bash
# Clone the repository
git clone https://github.com/0xbarss/redfolder.git
cd redfolder

# Build library and CLI binary
cargo build --all-targets --all-features
```

---

## Local Verification & CI Gates

Every pull request must pass the automated CI pipeline. Before submitting, run the full validation suite locally:

### 1. Code Formatting

Check formatting against rustfmt:

```bash
cargo fmt --all -- --check
```

To auto-format code:

```bash
cargo fmt
```

### 2. Linting (Clippy)

Run clippy with all targets, features, and warnings denied:

```bash
cargo clippy --all-targets --all-features -- -D warnings
```

### 3. Test Suite

Run all unit tests, integration tests, and doc-tests:

```bash
cargo test --all-targets --all-features
cargo test --doc
```

---

## Codebase Organization

```text
redfolder/
├── Cargo.toml                 # Package definition and dependencies
├── src/
│   ├── lib.rs                 # Library entry point and public re-exports
│   ├── calendar.rs            # FairEconomy HTTP client, cache fallback, DST parsing
│   ├── config.rs              # RedFolderConfig and builders
│   ├── curfew.rs              # Weekend market close calculation (short/weekend modes)
│   ├── engine.rs              # In-memory interval compiler and window merge engine
│   ├── error.rs               # RedFolderError enumeration via thiserror
│   ├── events.rs              # Domain events (BlackoutWarning, BlackoutStarted, etc.)
│   ├── service.rs             # RedFolderService background scheduler and event bus
│   ├── types.rs               # Strongly-typed Currency, Impact, and BlackoutWindow
│   └── bin/
│       └── main.rs            # Terminal CLI binary (status, upcoming, watch, sync)
├── tests/
│   └── integration_tests.rs   # End-to-end multi-worker and offline integration tests
└── examples/                  # Standalone runnable example bots and integrations
```

---

## Guidelines for Contributions

### Bug Fixes
- Include a regression test in either `tests/integration_tests.rs` or the relevant module unit tests under `src/` reproducing the issue before the fix.
- Ensure no silent data truncations or infallible fallbacks on user-facing inputs.

### New Features
- Maintain backward compatibility where possible.
- Update public API documentation and doc comments (`///`).
- If adding or modifying public methods, structs, or CLI options, update `README.md` accordingly.

### Commit Messages

Use clear, descriptive commit messages following the Conventional Commits style:

- `feat(engine): add custom holiday curfew calculation`
- `fix(calendar): handle edge case in tentative event timestamp parsing`
- `docs: update README with new client builder method`
- `test(service): add test for multiple worker unregistration`

---

## Submitting a Pull Request

1. Fork the repository and create your branch from `main`:
   ```bash
   git checkout -b feat/my-new-feature
   ```
2. Commit your changes and verify that `cargo fmt`, `cargo clippy`, and `cargo test` pass cleanly.
3. Push to your fork:
   ```bash
   git push origin feat/my-new-feature
   ```
4. Open a Pull Request against `main` on GitHub with a concise summary of your changes and why they are needed.
