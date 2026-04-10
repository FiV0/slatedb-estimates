# AGENTS.md

Project guidance for coding agents working in this repository.

## Scope

This repository contains a small Rust library that builds approximate range
statistics on top of SlateDB metadata APIs.

## Tech Stack

- Rust library crate
- SlateDB from GitHub `main`, pinned in `Cargo.toml`

## Working Agreement

- Keep the public API small and explicit.
- Prefer additive changes over broad refactors.
- Prefer single-line comments in code.
- Preserve the current async API shape unless there is a strong reason to change it.
- Treat memtable-backed estimation as unsupported unless explicitly implemented.

## Git

- Only commit and push when explicitly asked to by the user.
- End Codex-created commits with `Co-authored-by: Codex <noreply@openai.com>`.

## Key Files

- `src/lib.rs`: public API, error types, and tests
- `src/range_stats.rs`: estimation logic
- `Cargo.toml`: dependency and crate configuration

## Verification

Run these before finishing substantive changes:

```bash
cargo fmt
cargo check
cargo test
```
