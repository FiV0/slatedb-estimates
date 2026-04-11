use std::cmp::Ordering;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use slatedb::bytes::Bytes;
use slatedb::manifest::{ManifestCore, SsTableId, SsTableView};
use slatedb::object_store::ObjectStore;
use slatedb::object_store::path::Path;
use slatedb::{BlockTransformer, SstReader};

use crate::SizeApproximationOptions;

pub struct RangeStats {
    db: slatedb::Db,
    sst_reader: SstReader,
}

impl RangeStats {
    pub fn new<P: Into<Path>>(
        db: slatedb::Db,
        path: P,
        object_store: Arc<dyn ObjectStore>,
        cache: Option<Arc<dyn slatedb::db_cache::DbCache>>,
        block_transformer: Option<Arc<dyn BlockTransformer>>,
    ) -> Self {
        let sst_reader = SstReader::new(path, object_store, cache, block_transformer);
        Self::from_db_parts(db, sst_reader)
    }

    pub fn from_db_parts(db: slatedb::Db, sst_reader: SstReader) -> Self {
        Self { db, sst_reader }
    }

    pub async fn get_approximate_size<K, T>(
        &self,
        range: T,
        opts: &SizeApproximationOptions,
    ) -> Result<u64, slatedb::Error>
    where
        K: AsRef<[u8]> + Send,
        T: RangeBounds<K> + Send,
    {
        validate_size_options(opts)?;

        let query = KeyRange::from_range(&range);
        if query.is_empty() {
            return Ok(0);
        }

        let candidates = collect_candidates(self.db.manifest(), &query);
        if candidates.is_empty() {
            return Ok(0);
        }

        let coarse_total: u64 = candidates
            .iter()
            .map(|candidate| candidate.view.estimate_size())
            .sum();
        if coarse_total == 0 {
            return Ok(0);
        }

        let mut total = 0u64;
        for candidate in candidates {
            let coarse = candidate.view.estimate_size();
            if !candidate.requires_refinement {
                total = total.saturating_add(coarse);
                continue;
            }

            let candidate_fraction = coarse as f64 / coarse_total as f64;
            if candidate_fraction < opts.error_margin {
                total = total.saturating_add(coarse);
                continue;
            }

            let refined = self
                .estimate_view_size(&candidate.view, &candidate.overlap)
                .await?;
            total = total.saturating_add(refined);
        }

        Ok(total)
    }

    pub async fn estimate_key_count<K, T>(&self, range: T) -> Result<u64, slatedb::Error>
    where
        K: AsRef<[u8]> + Send,
        T: RangeBounds<K> + Send,
    {
        let query = KeyRange::from_range(&range);
        if query.is_empty() {
            return Ok(0);
        }

        let candidates = collect_candidates(self.db.manifest(), &query);
        if candidates.is_empty() {
            return Ok(0);
        }

        let mut total = 0u64;
        for candidate in candidates {
            let sst_file = self
                .sst_reader
                .open_with_handle(candidate.view.sst.clone())?;
            let stats = sst_file
                .stats()
                .await?
                .ok_or_else(|| missing_sst_stats_error(&candidate.view))?;

            if !candidate.requires_refinement {
                total = total.saturating_add(net_count(
                    stats.num_puts,
                    stats.num_deletes,
                    stats.num_merges,
                ));
                continue;
            }

            let index = sst_file.index().await?;
            if index.is_empty() || stats.block_stats.is_empty() {
                continue;
            }

            let Some((start_idx, end_idx)) = touched_block_span(&index, &candidate.overlap) else {
                continue;
            };
            let end_idx = end_idx.min(stats.block_stats.len().saturating_sub(1));
            let start_idx = start_idx.min(end_idx);

            let mut count = 0u64;
            for block_stats in &stats.block_stats[start_idx..=end_idx] {
                count = count.saturating_add(net_count(
                    u64::from(block_stats.num_puts),
                    u64::from(block_stats.num_deletes),
                    u64::from(block_stats.num_merges),
                ));
            }
            total = total.saturating_add(count);
        }

        Ok(total)
    }

    async fn estimate_view_size(
        &self,
        view: &SsTableView,
        overlap: &KeyRange,
    ) -> Result<u64, slatedb::Error> {
        let sst_file = self.sst_reader.open_with_handle(view.sst.clone())?;
        let index = sst_file.index().await?;
        if index.is_empty() {
            return Ok(view.estimate_size());
        }

        let Some((start_idx, end_idx)) = touched_block_span(&index, overlap) else {
            return Ok(0);
        };

        let total_size = view.estimate_size();
        let start_offset = index[start_idx].0;
        let end_offset = if end_idx + 1 < index.len() {
            index[end_idx + 1].0
        } else {
            total_size
        };

        Ok(end_offset.saturating_sub(start_offset).min(total_size))
    }
}

#[derive(Clone, Debug)]
struct Candidate {
    view: SsTableView,
    overlap: KeyRange,
    requires_refinement: bool,
}

fn validate_size_options(opts: &SizeApproximationOptions) -> Result<(), slatedb::Error> {
    if !opts.include_memtables && !opts.include_files {
        return Err(slatedb::Error::invalid(
            "at least one of include_memtables or include_files must be true".to_string(),
        ));
    }
    if !opts.error_margin.is_finite() || !(0.0..=1.0).contains(&opts.error_margin) {
        return Err(slatedb::Error::invalid(format!(
            "error_margin must be in the range [0.0, 1.0], got {}",
            opts.error_margin
        )));
    }
    if opts.include_memtables {
        return Err(slatedb::Error::invalid(
            "include_memtables is not supported yet".to_string(),
        ));
    }
    Ok(())
}

fn collect_candidates(manifest: ManifestCore, query: &KeyRange) -> Vec<Candidate> {
    let mut candidates = Vec::new();

    for view in manifest.l0 {
        if let Some(candidate) = build_candidate(view, query) {
            candidates.push(candidate);
        }
    }

    for sorted_run in manifest.compacted {
        for view in sorted_run.tables_covering_range((query.start.clone(), query.end.clone())) {
            if let Some(candidate) = build_candidate(view.clone(), query) {
                candidates.push(candidate);
            }
        }
    }

    candidates
}

fn build_candidate(view: SsTableView, query: &KeyRange) -> Option<Candidate> {
    let logical = logical_view_range(&view)?;
    let overlap = logical.intersection(query)?;
    let projected = view.visible_range().is_some();
    let requires_refinement = projected || !query.contains(&logical);
    Some(Candidate {
        view,
        overlap,
        requires_refinement,
    })
}

fn logical_view_range(view: &SsTableView) -> Option<KeyRange> {
    let start = Included(view.sst.info.first_entry.clone()?);
    let end = match &view.sst.info.last_entry {
        Some(last) => Included(last.clone()),
        None => Unbounded,
    };

    let physical = KeyRange { start, end };
    if let Some(visible) = view.visible_range() {
        let visible = KeyRange {
            start: visible.start_bound().cloned(),
            end: visible.end_bound().cloned(),
        };
        physical.intersection(&visible)
    } else {
        Some(physical)
    }
}

fn touched_block_span(index: &[(u64, Bytes)], range: &KeyRange) -> Option<(usize, usize)> {
    if index.is_empty() {
        return None;
    }

    let first_keys: Vec<&Bytes> = index.iter().map(|(_, key)| key).collect();
    let start_idx = start_block_index(&first_keys, &range.start);
    let end_idx = end_block_index(&first_keys, &range.end)?;
    if start_idx > end_idx || start_idx >= index.len() {
        return None;
    }
    Some((start_idx, end_idx.min(index.len() - 1)))
}

fn start_block_index(first_keys: &[&Bytes], start: &Bound<Bytes>) -> usize {
    match start {
        Unbounded => 0,
        Included(key) | Excluded(key) => {
            let idx = first_keys.partition_point(|candidate| *candidate <= key);
            idx.saturating_sub(1)
        }
    }
}

fn end_block_index(first_keys: &[&Bytes], end: &Bound<Bytes>) -> Option<usize> {
    match end {
        Unbounded => Some(first_keys.len().saturating_sub(1)),
        Included(key) => {
            let idx = first_keys.partition_point(|candidate| *candidate <= key);
            idx.checked_sub(1)
        }
        Excluded(key) => {
            let idx = first_keys.partition_point(|candidate| *candidate < key);
            idx.checked_sub(1)
        }
    }
}

fn net_count(puts: u64, deletes: u64, merges: u64) -> u64 {
    puts.saturating_add(merges).saturating_sub(deletes)
}

fn format_sst_id(view: &SsTableView) -> String {
    match view.sst.id {
        SsTableId::Compacted(id) => id.to_string(),
        SsTableId::Wal(id) => id.to_string(),
    }
}

fn missing_sst_stats_error(view: &SsTableView) -> slatedb::Error {
    slatedb::Error::data(format!(
        "cannot estimate key count because SST {} has no stats block",
        format_sst_id(view)
    ))
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct KeyRange {
    start: Bound<Bytes>,
    end: Bound<Bytes>,
}

impl KeyRange {
    fn from_range<K, T>(range: &T) -> Self
    where
        K: AsRef<[u8]> + Send,
        T: RangeBounds<K> + Send,
    {
        Self {
            start: clone_bound(range.start_bound()),
            end: clone_bound(range.end_bound()),
        }
    }

    fn is_empty(&self) -> bool {
        match (&self.start, &self.end) {
            (Unbounded, _) | (_, Unbounded) => false,
            (Included(start), Included(end)) => start > end,
            (Included(start), Excluded(end))
            | (Excluded(start), Included(end))
            | (Excluded(start), Excluded(end)) => start >= end,
        }
    }

    fn intersection(&self, other: &Self) -> Option<Self> {
        let start = max_start_bound(&self.start, &other.start);
        let end = min_end_bound(&self.end, &other.end);
        let range = Self { start, end };
        (!range.is_empty()).then_some(range)
    }

    fn contains(&self, other: &Self) -> bool {
        self.intersection(other).as_ref() == Some(other)
    }
}

fn clone_bound<K: AsRef<[u8]>>(bound: Bound<&K>) -> Bound<Bytes> {
    match bound {
        Included(key) => Included(Bytes::copy_from_slice(key.as_ref())),
        Excluded(key) => Excluded(Bytes::copy_from_slice(key.as_ref())),
        Unbounded => Unbounded,
    }
}

fn max_start_bound(left: &Bound<Bytes>, right: &Bound<Bytes>) -> Bound<Bytes> {
    match compare_start_bounds(left, right) {
        Ordering::Less => right.clone(),
        _ => left.clone(),
    }
}

fn min_end_bound(left: &Bound<Bytes>, right: &Bound<Bytes>) -> Bound<Bytes> {
    match compare_end_bounds(left, right) {
        Ordering::Greater => right.clone(),
        _ => left.clone(),
    }
}

fn compare_start_bounds(left: &Bound<Bytes>, right: &Bound<Bytes>) -> Ordering {
    match (left, right) {
        (Unbounded, Unbounded) => Ordering::Equal,
        (Unbounded, _) => Ordering::Less,
        (_, Unbounded) => Ordering::Greater,
        (Included(left), Included(right)) | (Excluded(left), Excluded(right)) => left.cmp(right),
        (Included(left), Excluded(right)) => match left.cmp(right) {
            Ordering::Equal => Ordering::Less,
            ordering => ordering,
        },
        (Excluded(left), Included(right)) => match left.cmp(right) {
            Ordering::Equal => Ordering::Greater,
            ordering => ordering,
        },
    }
}

fn compare_end_bounds(left: &Bound<Bytes>, right: &Bound<Bytes>) -> Ordering {
    match (left, right) {
        (Unbounded, Unbounded) => Ordering::Equal,
        (Unbounded, _) => Ordering::Greater,
        (_, Unbounded) => Ordering::Less,
        (Included(left), Included(right)) | (Excluded(left), Excluded(right)) => left.cmp(right),
        (Included(left), Excluded(right)) => match left.cmp(right) {
            Ordering::Equal => Ordering::Greater,
            ordering => ordering,
        },
        (Excluded(left), Included(right)) => match left.cmp(right) {
            Ordering::Equal => Ordering::Less,
            ordering => ordering,
        },
    }
}
