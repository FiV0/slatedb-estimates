//! Approximate range statistics built on top of SlateDB metadata APIs.

mod range_stats;

pub use crate::range_stats::RangeStats;

#[derive(Debug, Clone)]
pub struct SizeApproximationOptions {
    pub include_memtables: bool,
    pub include_files: bool,
    pub error_margin: f64,
}

impl Default for SizeApproximationOptions {
    fn default() -> Self {
        Self {
            include_memtables: false,
            include_files: true,
            error_margin: 0.10,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use slatedb::bytes::Bytes;
    use slatedb::compactor::{
        CompactionScheduler, CompactionSchedulerSupplier, CompactionSpec, CompactorBuilder,
        CompactorStateView, SourceId,
    };
    use slatedb::config::{CompactorOptions, FlushOptions, FlushType, Settings, SstBlockSize};
    use slatedb::object_store::memory::InMemory;
    use slatedb::{Db, ErrorKind, SstReader};

    use super::{RangeStats, SizeApproximationOptions};

    #[tokio::test]
    async fn approximate_size_returns_zero_for_missing_range() {
        let (range_stats, _, _) = seeded_stats("/size-zero", 16, 1).await;

        let size = range_stats
            .get_approximate_size(
                Bytes::from_static(b"z00")..Bytes::from_static(b"z99"),
                &SizeApproximationOptions::default(),
            )
            .await
            .unwrap();

        assert_eq!(size, 0);
    }

    #[tokio::test]
    async fn approximate_size_requires_a_source() {
        let (range_stats, _, _) = seeded_stats("/size-sources", 16, 1).await;
        let opts = SizeApproximationOptions {
            include_memtables: false,
            include_files: false,
            error_margin: 0.0,
        };

        let err = range_stats
            .get_approximate_size(
                Bytes::from_static(b"k00")..Bytes::from_static(b"k10"),
                &opts,
            )
            .await
            .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::Invalid);
        assert!(err.to_string().contains("at least one"));
    }

    #[tokio::test]
    async fn approximate_size_rejects_memtables_for_now() {
        let (range_stats, _, _) = seeded_stats("/size-memtables", 16, 1).await;
        let opts = SizeApproximationOptions {
            include_memtables: true,
            include_files: true,
            error_margin: 0.0,
        };

        let err = range_stats
            .get_approximate_size(
                Bytes::from_static(b"k00")..Bytes::from_static(b"k10"),
                &opts,
            )
            .await
            .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::Invalid);
        assert!(err.to_string().contains("include_memtables"));
    }

    #[tokio::test]
    async fn approximate_size_validates_error_margin() {
        let (range_stats, _, _) = seeded_stats("/size-margin", 16, 1).await;
        let opts = SizeApproximationOptions {
            include_memtables: false,
            include_files: true,
            error_margin: 1.5,
        };

        let err = range_stats
            .get_approximate_size(
                Bytes::from_static(b"k00")..Bytes::from_static(b"k10"),
                &opts,
            )
            .await
            .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::Invalid);
        assert!(err.to_string().contains("1.5"));
    }

    #[tokio::test]
    async fn approximate_size_refines_partial_ranges() {
        let (range_stats, _, _) = seeded_stats("/size-refined", 20, 2).await;
        let full = range_stats
            .get_approximate_size(
                Bytes::from_static(b"k00")..Bytes::from_static(b"k20"),
                &SizeApproximationOptions::default(),
            )
            .await
            .unwrap();
        let refined = range_stats
            .get_approximate_size(
                Bytes::from_static(b"k03")..Bytes::from_static(b"k17"),
                &SizeApproximationOptions::default(),
            )
            .await
            .unwrap();

        assert!(refined < full);
    }

    #[tokio::test]
    async fn larger_error_margin_can_skip_refinement() {
        let (range_stats, _, _) = seeded_stats("/size-skip", 20, 2).await;
        let refined = range_stats
            .get_approximate_size(
                Bytes::from_static(b"k03")..Bytes::from_static(b"k17"),
                &SizeApproximationOptions {
                    include_memtables: false,
                    include_files: true,
                    error_margin: 0.0,
                },
            )
            .await
            .unwrap();
        let coarse = range_stats
            .get_approximate_size(
                Bytes::from_static(b"k03")..Bytes::from_static(b"k17"),
                &SizeApproximationOptions {
                    include_memtables: false,
                    include_files: true,
                    error_margin: 0.75,
                },
            )
            .await
            .unwrap();

        assert!(coarse >= refined);
    }

    #[tokio::test]
    async fn estimate_key_count_matches_full_scan_for_put_only_data() {
        let (range_stats, _, expected_full) = seeded_stats("/keycount-full", 12, 1).await;

        let count = range_stats
            .estimate_key_count(Bytes::from_static(b"k00")..Bytes::from_static(b"k12"))
            .await
            .unwrap();

        assert_eq!(count, expected_full);
    }

    #[tokio::test]
    async fn estimate_key_count_for_block_aligned_range_matches_scan() {
        let path = "/keycount-aligned";
        let object_store: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = build_seeded_db(path, Arc::clone(&object_store), 18, 1).await;

        let reader = SstReader::new(path, Arc::clone(&object_store), None, None);
        let manifest = db.manifest();
        let view = manifest.l0.front().unwrap();
        let sst_file = reader.open_with_handle(view.sst.clone()).unwrap();
        let index = sst_file.index().await.unwrap();
        assert!(index.len() > 2, "test data should span multiple blocks");

        let start = index[1].1.clone();
        let end = index[2].1.clone();

        let expected = scan_count(&db, start.clone()..end.clone()).await;
        let range_stats = RangeStats::new(Arc::new(db), path, object_store, None, None);

        let count = range_stats.estimate_key_count(start..end).await.unwrap();
        assert_eq!(count, expected);
    }

    #[tokio::test]
    async fn estimate_key_count_is_monotonic_for_put_only_data() {
        let (range_stats, _, _) = seeded_stats("/keycount-monotonic", 18, 1).await;
        let full = range_stats
            .estimate_key_count(Bytes::from_static(b"k00")..Bytes::from_static(b"k18"))
            .await
            .unwrap();
        let partial = range_stats
            .estimate_key_count(Bytes::from_static(b"k03")..Bytes::from_static(b"k10"))
            .await
            .unwrap();

        assert!(partial <= full);
    }

    #[tokio::test]
    async fn estimate_key_count_is_a_storage_level_estimate_for_overlapping_l0() {
        let path = "/keycount-overlapping-l0";
        let object_store: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = Db::builder(path, Arc::clone(&object_store))
            .with_sst_block_size(SstBlockSize::Block1Kib)
            .build()
            .await
            .unwrap();

        db.put(b"k", b"value-0").await.unwrap();
        flush_memtable(&db).await;
        db.put(b"k", b"value-1").await.unwrap();
        flush_memtable(&db).await;
        db.delete(b"k").await.unwrap();
        flush_memtable(&db).await;

        let scan_count = scan_count(&db, Bytes::from_static(b"k")..Bytes::from_static(b"l")).await;
        let range_stats = RangeStats::new(Arc::new(db), path, object_store, None, None);
        let estimated = range_stats
            .estimate_key_count(Bytes::from_static(b"k")..Bytes::from_static(b"l"))
            .await
            .unwrap();

        assert_eq!(scan_count, 0);
        assert_eq!(estimated, 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn estimate_key_count_matches_scan_for_compacted_put_only_data() {
        let path = "/keycount-compacted";
        let object_store: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = Db::builder(path, Arc::clone(&object_store))
            .with_settings(compacting_settings())
            .with_compactor_builder(
                CompactorBuilder::new(path, Arc::clone(&object_store))
                    .with_options(compactor_options())
                    .with_scheduler_supplier(Arc::new(CompactL0AfterTwoSchedulerSupplier)),
            )
            .with_sst_block_size(SstBlockSize::Block1Kib)
            .build()
            .await
            .unwrap();

        for i in 0..24 {
            let key = format!("k{i:02}");
            let value = format!("value-{i:02}-{}", "x".repeat(192));
            db.put(key.as_bytes(), value.as_bytes()).await.unwrap();

            if i == 11 {
                flush_memtable(&db).await;
            }
        }
        flush_memtable(&db).await;
        wait_for_compaction(&db).await;

        let manifest = db.manifest();
        assert!(manifest.l0.is_empty());
        assert!(!manifest.compacted.is_empty());

        let range = Bytes::from_static(b"k00")..Bytes::from_static(b"k24");
        let expected = scan_count(&db, range.clone()).await;
        let range_stats = RangeStats::new(Arc::new(db), path, object_store, None, None);

        let estimated = range_stats.estimate_key_count(range).await.unwrap();
        assert_eq!(estimated, expected);
    }

    #[tokio::test]
    async fn constructors_produce_identical_results() {
        let path_one = "/constructors-one";
        let path_two = "/constructors-two";
        let store_one: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(InMemory::new());
        let store_two: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(InMemory::new());
        let db_one = build_seeded_db(path_one, Arc::clone(&store_one), 18, 2).await;
        let db_two = build_seeded_db(path_two, Arc::clone(&store_two), 18, 2).await;

        let with_new = RangeStats::new(
            Arc::new(db_one),
            path_one,
            Arc::clone(&store_one),
            None,
            None,
        );
        let reader = SstReader::new(path_two, Arc::clone(&store_two), None, None);
        let with_parts = RangeStats::from_db_parts(Arc::new(db_two), reader);

        let range = Bytes::from_static(b"k03")..Bytes::from_static(b"k17");
        let opts = SizeApproximationOptions::default();

        let size_one = with_new
            .get_approximate_size(range.clone(), &opts)
            .await
            .unwrap();
        let size_two = with_parts
            .get_approximate_size(range.clone(), &opts)
            .await
            .unwrap();
        let count_one = with_new.estimate_key_count(range.clone()).await.unwrap();
        let count_two = with_parts.estimate_key_count(range).await.unwrap();

        assert_eq!(size_one, size_two);
        assert_eq!(count_one, count_two);
    }

    #[tokio::test]
    async fn approximate_size_with_prefix_matches_range_equivalent() {
        let (range_stats, _, _) = seeded_stats("/size-prefix", 20, 2).await;
        let opts = SizeApproximationOptions::default();

        let prefix_result = range_stats
            .get_approximate_size_with_prefix(b"k0", &opts)
            .await
            .unwrap();
        let range_result = range_stats
            .get_approximate_size(Bytes::from_static(b"k0")..Bytes::from_static(b"k1"), &opts)
            .await
            .unwrap();

        assert_eq!(prefix_result, range_result);
    }

    #[tokio::test]
    async fn estimate_key_count_with_prefix_matches_range_equivalent() {
        let (range_stats, _, _) = seeded_stats("/keycount-prefix", 20, 2).await;

        let prefix_result = range_stats
            .estimate_key_count_with_prefix(b"k0")
            .await
            .unwrap();
        let range_result = range_stats
            .estimate_key_count(Bytes::from_static(b"k0")..Bytes::from_static(b"k1"))
            .await
            .unwrap();

        assert_eq!(prefix_result, range_result);
    }

    #[tokio::test]
    async fn prefix_methods_handle_empty_prefix() {
        let (range_stats, _, expected_full) = seeded_stats("/prefix-empty", 12, 1).await;

        let count = range_stats
            .estimate_key_count_with_prefix(b"")
            .await
            .unwrap();

        assert_eq!(count, expected_full);
    }

    async fn seeded_stats(
        path: &'static str,
        key_count: usize,
        flushes: usize,
    ) -> (RangeStats, Arc<dyn slatedb::object_store::ObjectStore>, u64) {
        let object_store: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = build_seeded_db(path, Arc::clone(&object_store), key_count, flushes).await;
        let expected_full =
            scan_count(&db, Bytes::from_static(b"k00")..Bytes::from_static(b"zzz")).await;
        let range_stats =
            RangeStats::new(Arc::new(db), path, Arc::clone(&object_store), None, None);
        (range_stats, object_store, expected_full)
    }

    async fn build_seeded_db(
        path: &'static str,
        object_store: Arc<dyn slatedb::object_store::ObjectStore>,
        key_count: usize,
        flushes: usize,
    ) -> Db {
        let db = Db::builder(path, object_store)
            .with_sst_block_size(SstBlockSize::Block1Kib)
            .build()
            .await
            .unwrap();

        let chunk = key_count.div_ceil(flushes.max(1));
        for i in 0..key_count {
            let key = format!("k{i:02}");
            let value = format!("value-{i:02}-{}", "x".repeat(192));
            db.put(key.as_bytes(), value.as_bytes()).await.unwrap();

            if (i + 1) % chunk == 0 {
                flush_memtable(&db).await;
            }
        }

        if chunk == 0 || key_count % chunk != 0 {
            flush_memtable(&db).await;
        }

        db
    }

    async fn flush_memtable(db: &Db) {
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await
        .unwrap();
    }

    fn compacting_settings() -> Settings {
        Settings {
            flush_interval: Some(Duration::from_millis(10)),
            compactor_options: None,
            garbage_collector_options: None,
            ..Settings::default()
        }
    }

    fn compactor_options() -> CompactorOptions {
        CompactorOptions {
            poll_interval: Duration::from_millis(10),
            max_concurrent_compactions: 1,
            ..CompactorOptions::default()
        }
    }

    async fn wait_for_compaction(db: &Db) {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let manifest = db.manifest();
                if manifest.l0.is_empty() && !manifest.compacted.is_empty() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("timed out waiting for compaction");
    }

    struct CompactL0AfterTwoSchedulerSupplier;

    impl CompactionSchedulerSupplier for CompactL0AfterTwoSchedulerSupplier {
        fn compaction_scheduler(
            &self,
            _options: &CompactorOptions,
        ) -> Box<dyn CompactionScheduler + Send + Sync> {
            Box::new(CompactL0AfterTwoScheduler)
        }
    }

    struct CompactL0AfterTwoScheduler;

    impl CompactionScheduler for CompactL0AfterTwoScheduler {
        fn propose(&self, state: &CompactorStateView) -> Vec<CompactionSpec> {
            let manifest = state.manifest();
            if manifest.l0.len() < 2 {
                return Vec::new();
            }

            let sources = manifest
                .l0
                .iter()
                .map(|view| SourceId::SstView(view.id))
                .collect();
            let destination = manifest.compacted.first().map_or(0, |run| run.id + 1);

            vec![CompactionSpec::new(sources, destination)]
        }
    }

    async fn scan_count<T>(db: &Db, range: T) -> u64
    where
        T: std::ops::RangeBounds<Bytes> + Send,
    {
        let mut iter = db.scan(range).await.unwrap();
        let mut count = 0u64;
        while iter.next().await.unwrap().is_some() {
            count += 1;
        }
        count
    }
}
