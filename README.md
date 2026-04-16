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

#### Example

```rust
use std::sync::Arc;
use slatedb::bytes::Bytes;
use slatedb::config::{FlushOptions, FlushType};
use slatedb::object_store::memory::InMemory;
use slatedb::Db;
use slatedb_estimates::{RangeStats, SizeApproximationOptions};

#[tokio::main]
async fn main() {
    let object_store: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(InMemory::new());
    let db = Db::builder("/example", Arc::clone(&object_store))
        .build()
        .await
        .unwrap();

    // Write some data and flush to SSTs.
    ...

    // Construct a RangeStats object from a db and object_storage
    // with optional cache and block_transformer
    let range_stats = RangeStats::new(Arc::new(db), "/example", object_store, None, None);
    let opts = SizeApproximationOptions {
        error_margin: 0.1,
        ..Default::default()
    };

    // Approximate size for a key range
    let size = range_stats
        .get_approximate_size(Bytes::from_static(b"k05")..Bytes::from_static(b"k15"), &opts)
        .await
        .unwrap();

    // Approximate key count for a key range
    let count = range_stats
        .estimate_key_count(Bytes::from_static(b"k05")..Bytes::from_static(b"k15"))
        .await
        .unwrap();
}
```

#### Development

```bash
cargo fmt
cargo check
cargo test
```
