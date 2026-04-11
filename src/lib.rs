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

    use slatedb::bytes::Bytes;
    use slatedb::config::{FlushOptions, FlushType, SstBlockSize};
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

    async fn seeded_stats(
        path: &'static str,
        key_count: usize,
        flushes: usize,
    ) -> (RangeStats, Arc<dyn slatedb::object_store::ObjectStore>, u64) {
        let object_store: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = build_seeded_db(path, Arc::clone(&object_store), key_count, flushes).await;
        let expected_full = scan_count(
            &db,
            Bytes::from_static(b"k00")..Bytes::from(b"zzz".to_vec()),
        )
        .await;
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
                db.flush_with_options(FlushOptions {
                    flush_type: FlushType::MemTable,
                })
                .await
                .unwrap();
            }
        }

        if chunk == 0 || key_count % chunk != 0 {
            db.flush_with_options(FlushOptions {
                flush_type: FlushType::MemTable,
            })
            .await
            .unwrap();
        }

        db
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
