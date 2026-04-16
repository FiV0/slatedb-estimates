### SlateDB estimates

> 🚧 **WARNING: Alpha Software** 🚧
> This is very much WIP for the range metadata API usage described in [SlateDB's range metdata RFC](https://github.com/slatedb/slatedb/blob/main/rfcs/0020-range-metadata.md)

A small Rust library that builds approximate range statistics on top of
[SlateDB](https://github.com/slatedb/slatedb) metadata APIs.

#### API

- `RangeStats::get_approximate_size` — estimate the on-disk byte size of a key range.
- `RangeStats::estimate_key_count` — estimate the number of keys in a range.
- `RangeStats::get_approximate_size_with_prefix` — prefix version of `get_approximate_size`.
- `RangeStats::estimate_key_count_with_prefix` — prefix version of `estimate_key_count`.

These APIs are best effort.

#### Quick start

```toml
[dependencies]
slatedb_estimates = { git = "https://github.com/FiV0/slatedb-estimates" }
```

#### Development

```bash
cargo fmt
cargo check
cargo test
```
