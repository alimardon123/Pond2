// Journal fold laws — ACCEPTANCE.md item 2 (the C9 history law) / CRITIQUE
// C3 / builder spec cron-2026-08-28-1100-b + tribunal repair r3 (cron-2026-
// 08-28-1100-c finding 2: the multi-writer interleaving dimension the
// acceptance text promises).
//
// These laws ATTACK the D3 no-CAS journal on a REAL kernel over a fresh
// tempdir per case (the production local-FS path — UnifiedStorage::new_local
// + kernel(), exactly the journal_test.rs pattern):
//
//   Law 1 — read-after-N-appends-is-the-union (the C9 invariant): every
//   appended id appears EXACTLY once (read_rows_i64 is a CONCATENATING
//   reader with no CRDT dedup — exact-once is the law, duplicates would
//   mean a racing fold double-counted an RG), and the id set equals the
//   union of all appended rows, under shuffled id→batch partitions.
//
//   Law 2 — compact-preserves-state-for-every-reader: after k appends and
//   one `journal::compact`, the same kernel AND a FRESH reader (a second
//   UnifiedStorage over the same directory — empty caches, entries ≤ the
//   snapshot's upto dropped by resolve_view, probes resuming above the
//   watermark) see byte-identical rows, and the raw resolved view has NO
//   live entries (the fold consumed everything).
//
//   Law 3 — multi-writer interleavings (tribunal r3): 2-3 writers' entries
//   fabricated at their journal paths in a SHUFFLED PHYSICAL PUT ORDER
//   (each writer's own seqs stay contiguous 1..=n — the shape concurrent
//   processes really produce), then read = the UNION, each id exactly
//   once, and one compact folds the interleaving to the same union for a
//   fresh reader with nothing left live.
//
// DETERMINISM (ACCEPTANCE.md item 6): the seed below pins the entire case
// space; CI runs are byte-reproducible. Bump the seed's trailing constant
// to explore a new random space.

use proptest::collection::vec;
use proptest::prelude::*;
use proptest::test_runner::{Config as ProptestConfig, RngSeed};

use pond_kernel::PondKernel;
use pond_storage::{journal, read, write, UnifiedStorage};

const LAWS_SEED: u64 = 0x504F4E44_00000003; // "POND" + file 3
fn law_config(cases: u32) -> ProptestConfig {
    let mut c = ProptestConfig::with_cases(cases);
    c.rng_seed = RngSeed::Fixed(LAWS_SEED);
    c
}

// ---------------------------------------------------------------------------
// Deterministic helpers (splitmix64 + Fisher-Yates — no rand dependency)
// ---------------------------------------------------------------------------

fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E3779B97F4A7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}

fn shuffle_slice<T>(items: &mut [T], seed: u64) {
    let mut state = seed | 1;
    for i in (1..items.len()).rev() {
        let j = (splitmix64(&mut state) as usize) % (i + 1);
        items.swap(i, j);
    }
}

/// One law case: k (1..=5) sequential append batch sizes (each 1..=8 rows)
/// plus a seed that shuffles WHICH ids land in which batch.
fn append_case() -> impl Strategy<Value = (Vec<u64>, u64)> {
    (1usize..=5).prop_flat_map(|k| (vec(1u64..=8, k..=k), any::<u64>()))
}

/// Write the case's k batches sequentially through the PRODUCTION journal
/// write path, ids shuffled into batches by `seed`. Returns the expected
/// (id, val) row set (unique ids by construction).
fn write_batches(
    kernel: &PondKernel,
    sizes: &[u64],
    seed: u64,
    coll: &str,
) -> Vec<(i64, i64)> {
    let total: usize = sizes.iter().map(|&s| s as usize).sum();
    let mut ids: Vec<i64> = (0..total as i64).collect();
    shuffle_slice(&mut ids, seed);

    let mut expected: Vec<(i64, i64)> = Vec::with_capacity(total);
    let mut it = ids.into_iter();
    for (i, &size) in sizes.iter().enumerate() {
        let batch: Vec<i64> = (&mut it).take(size as usize).collect();
        let vals: Vec<i64> = batch.iter().map(|id| id * 3 - 7).collect();
        expected.extend(batch.iter().copied().zip(vals.iter().copied()));
        write::write_rows_i64(
            kernel,
            coll,
            "main",
            &[("id", &batch), ("val", &vals)],
            &format!("law batch {}", i),
        )
        .unwrap();
    }
    expected
}

fn column(cols: &[(String, Vec<i64>)], name: &str) -> Vec<i64> {
    cols.iter()
        .find(|(n, _)| n == name)
        .map(|(_, v)| v.clone())
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Multi-writer fabrication (the journal_test.rs technique, minimal form)
// ---------------------------------------------------------------------------

/// Fabricate one journal data entry at
/// `collections/<c>/_branches/main/journal/<writer>/<seq:012>`: a PNPK pack
/// (commit JSON with journal metadata + a one-RG INT64 manifest). This is
/// exactly the shape `write_rows_i64` appends — built by hand so the LAW
/// controls the physical PUT ORDER across writers (the registry serializes
/// same-process writers, so interleaving requires fabrication).
fn fabricate_entry(
    kernel: &PondKernel,
    coll: &str,
    writer: &str,
    seq: u64,
    ids: &[i64],
    vals: &[i64],
) {
    use pond_core::VT_INT64;
    use pond_storage::manifest::{CollectionManifest, ColumnStatsEntry, RowGroupEntry};
    use pond_storage::pond_pack;
    use serde_json::{json, Value};

    let blob = pond_core::pnd2_encode_i64_auto(&[("id", ids), ("val", vals)]);
    let data_hash = kernel.write(&blob).unwrap();

    let mut manifest = CollectionManifest::new(
        vec![("id".to_string(), VT_INT64), ("val".to_string(), VT_INT64)],
        "id".to_string(),
    );
    let col_stats: Vec<ColumnStatsEntry> = [("id", ids), ("val", vals)]
        .iter()
        .map(|(name, values)| {
            let (min, max) = if values.is_empty() {
                (None, None)
            } else {
                (
                    Some(values.iter().min().unwrap().to_le_bytes().to_vec()),
                    Some(values.iter().max().unwrap().to_le_bytes().to_vec()),
                )
            };
            ColumnStatsEntry {
                name: name.to_string(),
                value_type: VT_INT64,
                min,
                max,
                null_count: 0,
            }
        })
        .collect();
    manifest.add_row_group(RowGroupEntry {
        key: "rg_0000000000".to_string(),
        blob_hash: data_hash,
        n_rows: ids.len() as u32,
        columns: col_stats,
        slab_byte_offset: None,
        slab_byte_len: None,
    });

    let commit_obj = json!({
        "parent": Value::Null,
        "second_parent": Value::Null,
        "manifest": "packed",
        "message": format!("fabricated {} seq {}", writer, seq),
        "timestamp": 0.0,
        "index": (seq - 1) as usize,
        "journal": {"writer": writer, "seq": seq},
    });
    let pack = pond_pack::encode_pack(&commit_obj, &manifest.encode(), None);
    let pack_hash = kernel.write(&pack).unwrap();

    let path = journal::entry_path(coll, "main", writer, seq);
    kernel.reference(&path, &pack_hash).unwrap();
}

/// The union read as a sorted (id, val) pair list — the shape every law
/// below compares against.
fn read_sorted_pairs(kernel: &PondKernel, coll: &str) -> Vec<(i64, i64)> {
    let cols = read::read_rows_i64(kernel, coll, "main", None, None).unwrap();
    let mut pairs: Vec<(i64, i64)> = column(&cols, "id")
        .iter()
        .copied()
        .zip(column(&cols, "val").iter().copied())
        .collect();
    pairs.sort_unstable();
    pairs
}

// ---------------------------------------------------------------------------
// Laws
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(law_config(24))]

    // Law 1 (the C9 history law) — read after N appends is the UNION:
    // each id EXACTLY once, and the id set equals the generated union.
    #[test]
    fn read_after_n_appends_is_the_union(
        (sizes, seed) in append_case(),
    ) {
        let coll = format!("union_{:016x}", seed);
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();

        let expected = write_batches(kernel, &sizes, seed, &coll);

        let cols = read::read_rows_i64(kernel, &coll, "main", None, None).unwrap();
        let ids_out = column(&cols, "id");
        let vals_out = column(&cols, "val");

        // EXACT-ONCE + set equality on ids (concatenating readers have no
        // CRDT dedup — duplicates or losses are both failures).
        let mut got_ids = ids_out.clone();
        got_ids.sort_unstable();
        let mut want_ids: Vec<i64> = expected.iter().map(|(id, _)| *id).collect();
        want_ids.sort_unstable();
        prop_assert_eq!(got_ids.len(), want_ids.len(),
            "row count must equal the union size ({} appends, {} rows expected)",
            sizes.len(), want_ids.len());
        prop_assert_eq!(got_ids, want_ids,
            "read after {} appends must be the exact union of all appended ids",
            sizes.len());

        // Row alignment: every id must still carry ITS OWN val.
        let mut got_pairs: Vec<(i64, i64)> =
            ids_out.iter().copied().zip(vals_out.iter().copied()).collect();
        let mut want_pairs = expected.clone();
        got_pairs.sort_unstable();
        want_pairs.sort_unstable();
        prop_assert_eq!(got_pairs, want_pairs,
            "each row's val must travel with its id through the journal");
    }

    // Law 2 — `compact` preserves the reader-visible state for EVERY
    // reader: the same kernel, a FRESH reader over the same directory, and
    // the raw resolved view (no live entries may remain).
    #[test]
    fn compact_preserves_state_for_every_reader(
        (sizes, seed) in append_case(),
    ) {
        let coll = format!("fold_{:016x}", seed);
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();

        let expected = write_batches(kernel, &sizes, seed, &coll);
        let before = read::read_rows_i64(kernel, &coll, "main", None, None).unwrap();
        // Sanity: the pre-compact state is the union (law 1's invariant,
        // checked on the exact rows we are about to fold).
        let mut before_pairs: Vec<(i64, i64)> = column(&before, "id")
            .iter()
            .copied()
            .zip(column(&before, "val").iter().copied())
            .collect();
        let mut expected_pairs = expected.clone();
        before_pairs.sort_unstable();
        expected_pairs.sort_unstable();
        prop_assert_eq!(before_pairs, expected_pairs);

        journal::compact(kernel, &coll, "main", &[]).unwrap();

        // Same kernel, after the fold.
        let after_same = read::read_rows_i64(kernel, &coll, "main", None, None).unwrap();
        prop_assert_eq!(after_same, before.clone(),
            "compact must preserve the same-kernel reader's state exactly");

        // Fresh reader: a NEW UnifiedStorage over the SAME directory (empty
        // caches, probes resuming above the new watermark).
        let storage2 = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel2 = storage2.kernel();
        let after_fresh = read::read_rows_i64(kernel2, &coll, "main", None, None).unwrap();
        prop_assert_eq!(after_fresh, before,
            "compact must preserve a FRESH reader's state exactly");

        // The fold consumed everything: no live entries above the watermark.
        let view = journal::resolve_view(kernel2, &coll, "main", true).unwrap();
        prop_assert!(view.entries.is_empty(),
            "resolve_view after compact must have no live entries (got {:?})",
            view.entries);
    }

    // Law 3 — multi-writer interleavings (tribunal r3): 2-3 writers, each
    // with 1..=3 entries (contiguous seqs 1..=n — the shape concurrent
    // processes produce), physically PUT in a SHUFFLED interleaved order.
    // The resolved read must be the UNION (each id exactly once) regardless
    // of the interleaving, and one compact must fold the interleaving to
    // the same union for a fresh reader with nothing left live.
    #[test]
    fn multi_writer_interleavings_resolve_to_the_union(
        (writer_sizes, seed) in
            (2usize..=3).prop_flat_map(|w| (vec(1u64..=3, w..=w), any::<u64>())),
    ) {
        let coll = format!("mw_{:016x}", seed);
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();

        // Disjoint id ranges per (writer, seq) so every entry's rows are
        // distinguishable: entry e of writer w owns ids
        // [e*100 + w*10, e*100 + w*10 + size).
        let mut expected: Vec<(i64, i64)> = Vec::new();
        let mut puts: Vec<(String, u64, Vec<i64>, Vec<i64>)> = Vec::new();
        for (w, &n_entries) in writer_sizes.iter().enumerate() {
            let writer = format!("W{}", w);
            for seq in 1..=n_entries {
                let base = (seq as i64 - 1) * 100 + (w as i64) * 10;
                let ids: Vec<i64> = (base..base + 5).collect();
                let vals: Vec<i64> = ids.iter().map(|id| id * 3 - 7).collect();
                expected.extend(ids.iter().copied().zip(vals.iter().copied()));
                puts.push((writer.clone(), seq, ids, vals));
            }
        }
        expected.sort_unstable();

        // SHUFFLE the physical PUT order — the interleaving dimension. The
        // journal must resolve the same union no matter which writer's PUT
        // landed first (unique paths: no writer can overwrite another).
        shuffle_slice(&mut puts, seed);
        for (writer, seq, ids, vals) in &puts {
            fabricate_entry(kernel, &coll, writer, *seq, ids, vals);
        }

        // The union, each id EXACTLY once, vals aligned with ids.
        let got = read_sorted_pairs(kernel, &coll);
        prop_assert_eq!(got, expected.clone(),
            "multi-writer interleaved reads must resolve to the exact union \
             ({} writers, {} entries, shuffled PUT order)",
            writer_sizes.len(), puts.len());

        // One compact folds the interleaving: fresh reader, same union,
        // nothing live above the watermarks.
        journal::compact(kernel, &coll, "main", &[]).unwrap();
        let storage2 = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel2 = storage2.kernel();
        let after_fold = read_sorted_pairs(kernel2, &coll);
        prop_assert_eq!(after_fold, expected,
            "compact must preserve the multi-writer union for a fresh reader");
        let view = journal::resolve_view(kernel2, &coll, "main", true).unwrap();
        prop_assert!(view.entries.is_empty(),
            "the multi-writer fold must consume every live entry (got {:?})",
            view.entries);
    }
}
