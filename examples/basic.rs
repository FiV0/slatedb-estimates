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
    for i in 0..20 {
        let key = format!("k{i:02}");
        let value = format!("value-{i:02}");
        db.put(key.as_bytes(), value.as_bytes()).await.unwrap();
    }
    db.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await
    .unwrap();

    let range_stats = RangeStats::new(Arc::new(db), "/example", object_store, None, None);
    let opts = SizeApproximationOptions {
        error_margin: 0.1,
        ..Default::default()
    };

    let size = range_stats
        .get_approximate_size(Bytes::from_static(b"k05")..Bytes::from_static(b"k15"), &opts)
        .await
        .unwrap();
    println!("approximate size: {size} bytes");

    let count = range_stats
        .estimate_key_count(Bytes::from_static(b"k05")..Bytes::from_static(b"k15"))
        .await
        .unwrap();
    println!("estimated key count: {count}");
}
