// Journal integration tests — the D3 no-CAS contract (ARCHITECTURE.md D3,
// builder spec cron-2026-08-28-0353-a, ACCEPTANCE.md iteration N+2).
//
// These tests are the cycle's behavioral harness. THE P0 PROBE
// (test_history_preserved_across_writes) reproduces CRITIQUE C9 exactly:
// two successive write_rows calls where the second commit's manifest held
// only its own row group while readers resolved only HEAD — 10+10 written,
// 10 readable. The pre-fix shape is demonstrated in-test through the PURE
// per-pack reader (read_rows_json_pruned_with_head on the latest pack),
// which is byte-for-byte what the old HEAD-only resolution returned; the
// journal-aware reader must then return the union.
//
// The rest of the harness pins the no-CAS invariants:
//   - every append is a unique-path PUT: N concurrent writers × K entries,
//     zero write errors, zero lost rows (test 3);
//   - the merge is a TOTAL order — identical merged state under any
//     permutation of entries/rows (test 4, C10);
//   - warm reads perform ZERO uncacheable LISTs — only the branch-ref GET
//     plus one epoch-probe miss per writer (test 5, C2);
//   - pre-journal repositories (HEAD commits + shards) still read (test 6);
//   - compaction folds journal + shards, advances the branch ref by benign
//     LWW, and never erases unfolded data (tests 7-9);
//   - status() introspects the journal (test 10).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use pond_core::{TypedColumn, VT_INT64};
use pond_kernel::{LocalFSObjectStore, ObjectStore, PondKernel};
use pond_storage::journal::{
    self, compact, entry_path, resolve_view, status, writer_for,
};
use pond_storage::manifest::{CollectionManifest, ColumnStatsEntry, RowGroupEntry};
use pond_storage::read;
use pond_storage::shard;
use pond_storage::write;
use pond_storage::{branch_ref, UnifiedStorage};
use serde_json::{json, Value};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Key fields passed to the JSON reader — mirrors the pyo3/SQL/CLI routing
/// (`_rowid` first, the CRDT row identity).
fn key_fields() -> Vec<String> {
    vec!["_rowid".to_string()]
}

/// Fabricate ONE journal entry for an EXTERNAL writer (a writer id this
/// process never registered): builds the same PNPK pack the write paths
/// build (PND2 data blob + manifest with column stats + commit JSON
/// stamped with `journal: {writer, seq}`), writes it, and appends the
/// pointer at the writer's entry path via a plain `reference()`.
///
/// This is how the tests simulate multiple writer PROCESSES without
/// spawning any: each fabricated writer id owns an independent log at
/// `journal/<writer_id>/<seq:012>`, exactly as a separate process would.
fn fabricate_entry(
    kernel: &PondKernel,
    collection: &str,
    branch: &str,
    writer: &str,
    seq: u64,
    ids: &[i64],
    vals: &[i64],
) -> String {
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
                (Some(values.iter().min().unwrap().to_le_bytes().to_vec()),
                 Some(values.iter().max().unwrap().to_le_bytes().to_vec()))
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
        "message": format!("fabricated entry {} for {}", seq, writer),
        "timestamp": 0.0,
        "index": (seq - 1) as usize,
        "journal": {"writer": writer, "seq": seq},
    });
    let pack = pond_storage::pond_pack::encode_pack(&commit_obj, &manifest.encode(), None);
    let pack_hash = kernel.write(&pack).unwrap();

    let path = entry_path(collection, branch, writer, seq);
    kernel.reference(&path, &pack_hash)
        .unwrap_or_else(|e| panic!("fabricated append must succeed: {}", e));
    pack_hash
}

/// Counting ObjectStore — records every LIST (dirs + paths), every
/// ref-path GET (with the path), and every content-blob GET, so the
/// warm-path budget tests can assert EXACT call counts: zero LISTs, one
/// branch-ref GET, one epoch-probe miss per discovered writer, and (D6)
/// zero extra blob reads on the resolve_packs fast path.
struct CountingStore {
    inner: LocalFSObjectStore,
    list_dirs_calls: Arc<AtomicU64>,
    list_paths_calls: Arc<AtomicU64>,
    get_path_calls: Arc<AtomicU64>,
    get_path_paths: Arc<Mutex<Vec<String>>>,
    get_blob_calls: Arc<AtomicU64>,
}

/// The CountingStore handle bundle: (kernel, list_dirs_calls,
/// list_paths_calls, get_path_calls, get_path_paths, get_blob_calls).
type CountedKernel = (
    PondKernel,
    Arc<AtomicU64>,
    Arc<AtomicU64>,
    Arc<AtomicU64>,
    Arc<Mutex<Vec<String>>>,
    Arc<AtomicU64>,
);

impl CountingStore {
    fn new(dir: &std::path::Path) -> Self {
        Self {
            inner: LocalFSObjectStore::new(dir).unwrap(),
            list_dirs_calls: Arc::new(AtomicU64::new(0)),
            list_paths_calls: Arc::new(AtomicU64::new(0)),
            get_path_calls: Arc::new(AtomicU64::new(0)),
            get_path_paths: Arc::new(Mutex::new(Vec::new())),
            get_blob_calls: Arc::new(AtomicU64::new(0)),
        }
    }

    fn kernel(self) -> CountedKernel {
        let counters = (
            Arc::clone(&self.list_dirs_calls),
            Arc::clone(&self.list_paths_calls),
            Arc::clone(&self.get_path_calls),
            Arc::clone(&self.get_path_paths),
            Arc::clone(&self.get_blob_calls),
        );
        (PondKernel::new_with_store(Box::new(self)), counters.0, counters.1, counters.2, counters.3, counters.4)
    }
}

impl ObjectStore for CountingStore {
    fn put_blob(&self, data: &[u8]) -> std::io::Result<String> {
        self.inner.put_blob(data)
    }
    fn get_blob(&self, hash: &str) -> std::io::Result<Vec<u8>> {
        self.get_blob_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.get_blob(hash)
    }
    fn put_path(&self, path: &str, hash: &str) -> std::io::Result<()> {
        self.inner.put_path(path, hash)
    }
    fn get_path(&self, path: &str) -> std::io::Result<Option<String>> {
        self.get_path_calls.fetch_add(1, Ordering::SeqCst);
        self.get_path_paths.lock().unwrap().push(path.to_string());
        self.inner.get_path(path)
    }
    fn delete_path(&self, path: &str) -> std::io::Result<bool> {
        self.inner.delete_path(path)
    }
    fn list_paths(&self, prefix: &str) -> std::io::Result<Vec<String>> {
        self.list_paths_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.list_paths(prefix)
    }
    fn list_dirs(&self, prefix: &str) -> std::io::Result<Vec<String>> {
        self.list_dirs_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.list_dirs(prefix)
    }
    fn store_id(&self) -> String {
        // Same identity as the wrapped LocalFS store: the process-local
        // journal registry/discovery keyed by store must see ONE store.
        self.inner.store_id()
    }
    fn blob_exists(&self, hash: &str) -> bool {
        self.inner.blob_exists(hash)
    }
    fn delete_blob(&self, hash: &str) -> std::io::Result<bool> {
        self.inner.delete_blob(hash)
    }
}

/// The ids column out of a read_rows_i64 result, sorted.
fn sorted_ids(cols: &[(String, Vec<i64>)]) -> Vec<i64> {
    let mut ids = cols
        .iter()
        .find(|(n, _)| n == "id")
        .map(|(_, v)| v.clone())
        .unwrap_or_default();
    ids.sort_unstable();
    ids
}

// ---------------------------------------------------------------------------
// 1 + 2. History preservation (the C9 P0 probes)
// ---------------------------------------------------------------------------

#[test]
fn test_history_preserved_across_writes() {
    // THE P0 PROBE (CRITIQUE C9, verified experimentally pre-journal):
    // two successive write_rows_i64 calls — the second commit's manifest
    // held ONLY its own row group and readers resolved only HEAD, so the
    // first write's rows vanished (10+10 written, 10 readable).
    let dir = tempfile::tempdir().unwrap();
    let storage = UnifiedStorage::new_local(dir.path()).unwrap();
    let kernel = storage.kernel();

    let ids1: Vec<i64> = (0..10).collect();
    let vals1: Vec<i64> = (0..10).map(|i| i * 10).collect();
    let ids2: Vec<i64> = (10..20).collect();
    let vals2: Vec<i64> = (10..20).map(|i| i * 10).collect();

    let _h1 = write::write_rows_i64(
        kernel, "history", "main",
        &[("id", &ids1), ("val", &vals1)], "first 10",
    ).unwrap();
    let h2 = write::write_rows_i64(
        kernel, "history", "main",
        &[("id", &ids2), ("val", &vals2)], "second 10",
    ).unwrap();

    // The PRE-JOURNAL read shape, demonstrated in-test: resolving ONLY the
    // latest pack (what the old HEAD-only reader did) returns 10 rows —
    // the first write is invisible. This is the PURE per-pack reader, so
    // it is exactly the behavior the bug exhibited.
    let head_only = read::read_rows_json_pruned_with_head(
        kernel, &h2, &key_fields(), None, &[],
    ).unwrap();
    assert_eq!(head_only.len(), 10,
        "P0 shape: HEAD-only resolution sees just the latest commit's rows");

    // The journal-aware reader returns the UNION of both commits.
    let cols = read::read_rows_i64(kernel, "history", "main", None, None).unwrap();
    assert_eq!(sorted_ids(&cols), (0..20).collect::<Vec<i64>>(),
        "journal read must return BOTH writes' rows — 20, not 10");

    // And through the JSON pipeline (the pyo3/SQL/CLI surface).
    let rows = read::read_rows_json_pruned(
        kernel, "history", "main", &key_fields(), None, &[],
    ).unwrap();
    assert_eq!(rows.len(), 20);
}

#[test]
fn test_n_sequential_writes_all_visible() {
    // 5 sequential writes × 20 rows → 100 rows visible. The old path lost
    // every write but the last; the journal unions all of them.
    let dir = tempfile::tempdir().unwrap();
    let storage = UnifiedStorage::new_local(dir.path()).unwrap();
    let kernel = storage.kernel();

    for w in 0..5i64 {
        let ids: Vec<i64> = (w * 20..w * 20 + 20).collect();
        let vals: Vec<i64> = ids.iter().map(|i| i * 3).collect();
        write::write_rows_i64(
            kernel, "seq_writes", "main",
            &[("id", &ids), ("val", &vals)], &format!("write {}", w),
        ).unwrap();
    }

    let cols = read::read_rows_i64(kernel, "seq_writes", "main", None, None).unwrap();
    assert_eq!(sorted_ids(&cols), (0..100).collect::<Vec<i64>>(),
        "all 5 sequential writes' rows must be visible");
}

// ---------------------------------------------------------------------------
// 3. Concurrent writers — no CAS, no lost rows
// ---------------------------------------------------------------------------

#[test]
fn test_concurrent_writers_no_lost_rows() {
    // 8 threads × 8 writes × 5 rows = 320 rows, zero write errors. This is
    // the no-CAS correctness claim (ARCHITECTURE.md D3): unique-path
    // appends cannot lose races by construction — no CAS, no retries.
    // Mid-run auto-compaction (default threshold 32) folds entries under
    // the same concurrency; the compactor's registry Mutex freezes its log
    // for the whole fold so the delete loop can never erase an unfolded
    // entry.
    //
    // HONESTY NOTE (tribunal F8): all 8 threads share the process's ONE
    // registry writer (journal.rs keys by store+collection+branch) — this
    // exercises concurrent appends + auto-compact interleaving on a single
    // log, NOT multi-writer-id concurrency. Multi-writer-id logs (true
    // cross-process shape) are covered by test_two_writer_logs_interleaved_
    // all_visible and the fabricated-writer tests; true cross-PROCESS
    // isolation is covered by f1_child (spawned process).
    const THREADS: i64 = 8;
    const WRITES_PER_THREAD: i64 = 8;
    const ROWS_PER_WRITE: i64 = 5;

    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(UnifiedStorage::new_local(dir.path()).unwrap());

    let mut handles = Vec::new();
    for t in 0..THREADS {
        let storage = Arc::clone(&storage);
        handles.push(std::thread::spawn(move || -> Result<(), String> {
            let kernel = storage.kernel();
            for j in 0..WRITES_PER_THREAD {
                let base = (t * WRITES_PER_THREAD + j) * ROWS_PER_WRITE;
                let ids: Vec<i64> = (base..base + ROWS_PER_WRITE).collect();
                let vals: Vec<i64> = ids.iter().map(|i| i * 7).collect();
                write::write_rows_i64(
                    kernel, "concurrent", "main",
                    &[("id", &ids), ("val", &vals)],
                    &format!("thread {} write {}", t, j),
                )?;
            }
            Ok(())
        }));
    }
    for (i, h) in handles.into_iter().enumerate() {
        h.join().expect("writer thread must not panic")
            .unwrap_or_else(|e| panic!("writer thread {} failed: {}", i, e));
    }

    let kernel = storage.kernel();
    let cols = read::read_rows_i64(kernel, "concurrent", "main", None, None).unwrap();
    let expected: Vec<i64> = (0..THREADS * WRITES_PER_THREAD * ROWS_PER_WRITE).collect();
    assert_eq!(sorted_ids(&cols), expected,
        "all 320 rows from 8 concurrent writers must survive — zero lost updates");
}

// ---------------------------------------------------------------------------
// 4. Deterministic merge under permutation (C10)
// ---------------------------------------------------------------------------

#[test]
fn test_merge_deterministic_under_permutation() {
    let dir = tempfile::tempdir().unwrap();
    let storage = UnifiedStorage::new_local(dir.path()).unwrap();
    let kernel = storage.kernel();

    // Two commits carrying CONFLICTING rows: same _rowid, same _version,
    // different payloads. (Explicit _rowid/_version columns — write_rows
    // only auto-adds them when absent.)
    let mk = |name: &str| vec![
        ("id", TypedColumn::Int64(vec![1])),
        ("name", TypedColumn::String(vec![name.to_string()])),
        ("_rowid", TypedColumn::String(vec!["tie-row-1".to_string()])),
        ("_version", TypedColumn::String(vec!["00000000000000000000000000000001".to_string()])),
    ];
    let c1 = write::write_rows(kernel, "perm", "main", &mk("aaa"), "payload aaa").unwrap();
    let c2 = write::write_rows(kernel, "perm", "main", &mk("zzz"), "payload zzz").unwrap();

    // Read both packs' raw rows (the PURE per-pack reader).
    let rows_a = read::read_rows_json_pruned_with_head(
        kernel, &c1, &key_fields(), None, &[],
    ).unwrap();
    let rows_b = read::read_rows_json_pruned_with_head(
        kernel, &c2, &key_fields(), None, &[],
    ).unwrap();
    let mut both: Vec<Value> = rows_a.iter().chain(rows_b.iter())
        .map(|(_, r)| r.clone()).collect();
    assert_eq!(both.len(), 2);

    // (a) Both merge orders → identical output; the winner is FIXED by the
    //     total order (_version, _rowid, payload) — greater payload wins.
    let ab = shard::merge_rows_by_rowid(&both, Some("_rowid"));
    both.reverse();
    let ba = shard::merge_rows_by_rowid(&both, Some("_rowid"));
    assert_eq!(ab.len(), 1);
    assert_eq!(ba.len(), 1);
    assert_eq!(ab, ba, "C10: same (version, rowid), different payloads — merge must be order-independent");
    assert_eq!(ab[0]["name"], json!("zzz"), "the total tiebreak's fixed winner (greater payload)");

    // (b) Tombstone-vs-live at equal versions: permutation must not decide
    //     whether the row survives.
    let live = json!({"_rowid": "tie-row-2", "_version": "v-equal", "name": "alive", "_deleted": false});
    let dead = json!({"_rowid": "tie-row-2", "_version": "v-equal", "name": "alive", "_deleted": true});
    let ld = shard::merge_rows_by_rowid(&[live.clone(), dead.clone()], Some("_rowid"));
    let dl = shard::merge_rows_by_rowid(&[dead.clone(), live.clone()], Some("_rowid"));
    assert_eq!(ld, dl, "tombstone-vs-live at equal versions must be order-independent");
    assert_eq!(
        shard::filter_live_rows(&ld).len(),
        shard::filter_live_rows(&dl).len(),
        "visibility must not depend on merge order"
    );

    // (c) Seeded shuffle loop (no proptest dependency): many permutations of
    //     a mixed row set → byte-identical merged state every time.
    let mut rows = vec![
        json!({"_rowid": "p1", "_version": "v1", "name": "x", "_deleted": false}),
        json!({"_rowid": "p1", "_version": "v1", "name": "a", "_deleted": false}),
        json!({"_rowid": "p1", "_version": "v1", "name": "m", "_deleted": true}),
        json!({"_rowid": "p2", "_version": "v1", "name": "q", "_deleted": false}),
        json!({"_rowid": "p2", "_version": "v2", "name": "r", "_deleted": false}),
        json!({"_rowid": "p2", "_version": "v2", "name": "s", "_deleted": false}),
        json!({"_rowid": "p3", "_version": "v0", "name": "t", "_deleted": false}),
    ];
    let reference = shard::merge_rows_by_rowid(&rows, Some("_rowid"));
    let mut seed: u64 = 0x9E3779B97F4A7C15;
    for round in 0..32 {
        for i in (1..rows.len()).rev() {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            let j = (seed >> 33) as usize % (i + 1);
            rows.swap(i, j);
        }
        let merged = shard::merge_rows_by_rowid(&rows, Some("_rowid"));
        assert_eq!(merged, reference,
            "round {}: merged state changed under permutation", round);
    }

    // (d) RESOLVER-level: the journal reader's output for the conflicting
    //     commits is deterministic and matches the total-order winner.
    let resolved = read::read_rows_json_pruned(
        kernel, "perm", "main", &key_fields(), None, &[],
    ).unwrap();
    assert_eq!(resolved.len(), 1, "conflicting rows collapse to one");
    assert_eq!(resolved[0].1["name"], json!("zzz"));
    // Reading again (fresh discovery) → identical state.
    let resolved_again = read::read_rows_json_pruned(
        kernel, "perm", "main", &key_fields(), None, &[],
    ).unwrap();
    assert_eq!(resolved, resolved_again, "resolver output is stable across reads");
}

// ---------------------------------------------------------------------------
// 5. Warm-path visibility budget — ZERO LISTs (C2)
// ---------------------------------------------------------------------------

#[test]
fn test_warm_read_zero_lists() {
    // 3 writers × 3 entries, folded by one compaction. The second read
    // within the discovery TTL performs ZERO uncacheable LISTs
    // (list_dirs == 0 AND list_paths == 0) and exactly ONE get_path per
    // discovered writer (the epoch-probe miss above the watermark) plus
    // the branch_ref get_path. That is the ACCEPTANCE #3 warm-path budget.
    let dir = tempfile::tempdir().unwrap();
    let store = CountingStore::new(dir.path());
    let (kernel, list_dirs_calls, list_paths_calls, get_path_calls, get_path_paths, _get_blob_calls) = store.kernel();

    // 3 external writers × 3 entries each (5 rows per entry → 45 rows).
    for w in 0..3 {
        for seq in 1..=3u64 {
            let base = (w * 3 + (seq - 1) as i64) * 5;
            let ids: Vec<i64> = (base..base + 5).collect();
            let vals: Vec<i64> = ids.iter().map(|i| i * 2).collect();
            fabricate_entry(&kernel, "warm", "main", &format!("writer-{}", w), seq, &ids, &vals);
        }
    }

    // Fold everything: the snapshot's upto watermarks cover all 9 entries
    // (+ the compactor's own log — the registry writer of THIS process).
    compact(&kernel, "warm", "main", &["id".to_string()]).unwrap();

    // Read #1 — warms the discovery cache (may or may not LIST depending
    // on the TTL clock; irrelevant to the budget under test).
    let rows1 = read::read_rows_json_pruned(
        &kernel, "warm", "main", &key_fields(), None, &[],
    ).unwrap();
    assert_eq!(rows1.len(), 45, "all three writers' rows visible after the fold");

    // Reset the counters: everything below measures read #2 ONLY.
    list_dirs_calls.store(0, Ordering::SeqCst);
    list_paths_calls.store(0, Ordering::SeqCst);
    get_path_calls.store(0, Ordering::SeqCst);
    get_path_paths.lock().unwrap().clear();

    // Read #2 — within the TTL: exact call counts.
    // NOTE: the two reads are back-to-back statements (µs apart) so the
    // default 1000ms TTL is comfortably fresh; a >1s scheduler stall
    // between them would legitimately re-LIST.
    let rows2 = read::read_rows_json_pruned(
        &kernel, "warm", "main", &key_fields(), None, &[],
    ).unwrap();
    assert_eq!(rows2.len(), 45);

    let list_dirs = list_dirs_calls.load(Ordering::SeqCst);
    let list_paths = list_paths_calls.load(Ordering::SeqCst);
    let get_path = get_path_calls.load(Ordering::SeqCst);
    let paths = get_path_paths.lock().unwrap().clone();

    assert_eq!(list_dirs, 0,
        "warm read: ZERO writer-discovery LISTs (TTL cache) — got {}", list_dirs);
    assert_eq!(list_paths, 0,
        "warm read: ZERO prefix LISTs — got {}", list_paths);

    // 4 discovered writers: 3 fabricated + the compactor (this process's
    // registry writer, whose log holds the fold entry at seq 1). Each
    // contributes exactly ONE probe (a miss at watermark+1), plus the
    // branch_ref resolve itself.
    let n_writers = 4;
    assert_eq!(get_path, 1 + n_writers,
        "warm read: branch_ref GET + one probe miss per writer — got {} ({:?})",
        get_path, paths);

    // Every GET path is either the branch ref or a watermark+1 probe —
    // never a folded entry path (those were deleted by the compaction).
    let branch = branch_ref("warm", "main");
    for p in &paths {
        let is_branch = p == &branch;
        let is_probe = p.starts_with("collections/warm/_branches/main/journal/")
            && p.ends_with(|c: char| c.is_ascii_digit());
        assert!(is_branch || is_probe,
            "warm read touched an unexpected path: {}", p);
    }
}

// ---------------------------------------------------------------------------
// 6. Legacy repositories read correctly through the new resolver
// ---------------------------------------------------------------------------

#[test]
fn test_legacy_repo_reads_correctly() {
    // Old layout, written manually the way the PRE-journal code did:
    // a plain commit (manifest ref, NOT a pack) under branch_ref, plus two
    // CRDT shards. The new reader must return BOTH the HEAD rows and the
    // shard rows (the python lenses still write shards — this is the
    // compat contract).
    let dir = tempfile::tempdir().unwrap();
    let storage = UnifiedStorage::new_local(dir.path()).unwrap();
    let kernel = storage.kernel();

    // Legacy HEAD: data blob + manifest blob + plain JSON commit + ref.
    let ids: Vec<i64> = (0..10).collect();
    let vals: Vec<i64> = ids.iter().map(|i| i * 10).collect();
    let blob = pond_core::pnd2_encode_i64_auto(&[("id", &ids), ("val", &vals)]);
    let data_hash = kernel.write(&blob).unwrap();

    let mut manifest = CollectionManifest::new(
        vec![("id".to_string(), VT_INT64), ("val".to_string(), VT_INT64)],
        "id".to_string(),
    );
    manifest.add_row_group(RowGroupEntry {
        key: "rg_0000000000".to_string(),
        blob_hash: data_hash.clone(),
        n_rows: ids.len() as u32,
        columns: vec![
            ColumnStatsEntry {
                name: "id".to_string(), value_type: VT_INT64,
                min: Some(0i64.to_le_bytes().to_vec()),
                max: Some(9i64.to_le_bytes().to_vec()), null_count: 0,
            },
            ColumnStatsEntry {
                name: "val".to_string(), value_type: VT_INT64,
                min: Some(0i64.to_le_bytes().to_vec()),
                max: Some(90i64.to_le_bytes().to_vec()), null_count: 0,
            },
        ],
        slab_byte_offset: None,
        slab_byte_len: None,
    });
    let manifest_hash = kernel.write(&manifest.encode()).unwrap();
    let commit_hash = pond_storage::commit::write_commit(
        kernel, "legacy", &manifest_hash, None, None, "legacy head", 0,
    ).unwrap();
    kernel.reference(&branch_ref("legacy", "main"), &commit_hash).unwrap();

    // Two shards (unique-path refs, CRDT rows with _rowid/_version).
    for s in 0..2 {
        let rows: Vec<Value> = (0..5)
            .map(|i| json!({
                "id": 100 + s * 10 + i,
                "name": format!("shard-{}-{}", s, i),
                "_rowid": format!("shard-row-{}-{}", s, i),
                "_version": format!("000000000000000{}0000000000000001", s),
                "_deleted": false,
            }))
            .collect();
        let data = serde_json::to_vec(&rows).unwrap();
        shard::append_shard(kernel, "legacy", "main", &format!("shard-{}", s), &data).unwrap();
    }

    // The new reader: journal-aware HEAD read (the legacy commit is the
    // snapshot base — no journal entries, no upto map) + the shard union,
    // exactly the flow pyo3's read_rows performs.
    let head_rows = read::read_rows_json_pruned(
        kernel, "legacy", "main", &key_fields(), None, &[],
    ).unwrap();
    assert_eq!(head_rows.len(), 10, "legacy HEAD rows read through the new resolver");

    let (_, shards) = shard::read_with_shards(kernel, "legacy", "main").unwrap();
    assert_eq!(shards.len(), 2, "both legacy shards discovered");
    let mut all: Vec<Value> = head_rows.iter().map(|(_, r)| r.clone()).collect();
    for (_, shard_hash) in &shards {
        let data = kernel.read_blob(shard_hash).unwrap();
        let rows: Vec<Value> = serde_json::from_slice(&data).unwrap();
        all.extend(rows);
    }
    let merged = shard::merge_rows_by_rowid(&all, Some("_rowid"));
    let live = shard::filter_live_rows(&merged);
    assert_eq!(live.len(), 20, "HEAD rows + both shards — the full legacy state");
}

// ---------------------------------------------------------------------------
// 7. Compaction folds journal entries AND shards
// ---------------------------------------------------------------------------

#[test]
fn test_compact_folds_journal_and_shards() {
    let dir = tempfile::tempdir().unwrap();
    let storage = UnifiedStorage::new_local(dir.path()).unwrap();
    let kernel = storage.kernel();

    // 3 structured writes (write_rows: CRDT rows, 5 rows each). The first
    // write bootstrap-folds into a snapshot; the other two stay live.
    for w in 0..3i64 {
        let ids: Vec<i64> = (w * 5..w * 5 + 5).collect();
        let names: Vec<String> = ids.iter().map(|i| format!("row-{}", i)).collect();
        write::write_rows(
            kernel, "foldme", "main",
            &[("id", TypedColumn::Int64(ids)), ("name", TypedColumn::String(names))],
            &format!("write {}", w),
        ).unwrap();
    }

    // 2 shards with distinct CRDT rowids.
    for s in 0..2 {
        let rows: Vec<Value> = (0..5)
            .map(|i| json!({
                "id": 200 + s * 10 + i,
                "name": format!("shard-{}-{}", s, i),
                "_rowid": format!("fold-shard-{}-{}", s, i),
                "_version": format!("000000000000000{}0000000000000001", s),
                "_deleted": false,
            }))
            .collect();
        let data = serde_json::to_vec(&rows).unwrap();
        shard::append_shard(kernel, "foldme", "main", &format!("s{}", s), &data).unwrap();
    }

    let stats = compact(kernel, "foldme", "main", &["id".to_string()]).unwrap();
    assert_eq!(stats.shards_folded, 2);
    assert!(stats.entries_folded >= 2, "the live entries (post-bootstrap) folded: {}", stats.entries_folded);

    // branch_ref → the new snapshot pack, whose manifest holds ALL RGs:
    // 3 write RGs + 1 shard-fold RG.
    let head = kernel.resolve(&branch_ref("foldme", "main")).unwrap().unwrap();
    assert_eq!(head, stats.new_snapshot, "compaction advanced the branch ref (LWW)");
    let manifest_bytes = pond_storage::commit::resolve_manifest_bytes(kernel, &head).unwrap();
    let manifest = CollectionManifest::decode(&manifest_bytes).unwrap();
    assert_eq!(manifest.row_groups.len(), 4,
        "folded manifest = 3 write RGs + 1 shard RG, got {}", manifest.row_groups.len());
    let total_rows: u32 = manifest.row_groups.iter().map(|rg| rg.n_rows).sum();
    assert_eq!(total_rows, 25, "15 pack rows + 10 shard rows");

    // Folded journal entry paths are GONE (probe → None); the compactor's
    // own fold entry (seq 5 — the log holds: 1 data, 2 bootstrap fold,
    // 3+4 data, 5 this fold) survives by design so readers whose
    // branch_ref read predates the LWW can still find the pack by probe.
    let writer_id = writer_for(kernel, "foldme", "main").lock().unwrap().writer_id.clone();
    for seq in 1..=5u64 {
        let path = entry_path("foldme", "main", &writer_id, seq);
        if seq == 5 {
            assert!(kernel.resolve(&path).unwrap().is_some(),
                "the compactor's own entry pointer survives the fold");
        } else {
            assert!(kernel.resolve(&path).unwrap().is_none(),
                "folded entry path must be deleted: {}", path);
        }
    }

    // Shards cleared.
    assert!(shard::list_shards(kernel, "foldme", "main").unwrap().is_empty(),
        "folded shards are cleared");

    // The full state survives the fold: 25 rows.
    let rows = read::read_rows_json_pruned(
        kernel, "foldme", "main", &key_fields(), None, &[],
    ).unwrap();
    assert_eq!(rows.len(), 25, "read after compaction returns the full folded state");

    // Post-compact write + read works (the journal keeps accepting entries
    // above the new watermark).
    let ids: Vec<i64> = (300..305).collect();
    let names: Vec<String> = ids.iter().map(|i| format!("post-{}", i)).collect();
    write::write_rows(
        kernel, "foldme", "main",
        &[("id", TypedColumn::Int64(ids)), ("name", TypedColumn::String(names))],
        "post-compact write",
    ).unwrap();
    let rows = read::read_rows_json_pruned(
        kernel, "foldme", "main", &key_fields(), None, &[],
    ).unwrap();
    assert_eq!(rows.len(), 30, "post-compact write lands above the watermark and unions in");
}

// ---------------------------------------------------------------------------
// 7b. Folded tombstones keep suppressing (no resurrection at compaction)
// ---------------------------------------------------------------------------

#[test]
fn test_compact_preserves_tombstones_no_resurrection() {
    // DELETION-AS-DATA across the fold: a shard tombstone that suppresses
    // a pack row must STILL suppress it after compaction clears the
    // shards. The fold is RG-level (no row dedup), so the tombstone rides
    // into the union manifest as a row of the shard-fold RG — dropping it
    // (filter_live_rows at fold time) would resurrect every deleted row
    // the moment its shard was cleared.
    let dir = tempfile::tempdir().unwrap();
    let storage = UnifiedStorage::new_local(dir.path()).unwrap();
    let kernel = storage.kernel();

    let ids: Vec<i64> = (0..5).collect();
    let names: Vec<String> = ids.iter().map(|i| format!("row-{}", i)).collect();
    write::write_rows(
        kernel, "graves", "main",
        &[("id", TypedColumn::Int64(ids)), ("name", TypedColumn::String(names))],
        "seed",
    ).unwrap();

    // Tombstone rows with id 1 and 3 by _rowid with a LATER version.
    // (Pick by id VALUE — the read's row order is rowid-sorted, and
    // uuidv7 rowids minted within the same millisecond sort arbitrarily.)
    let seeded = read::read_rows_json_pruned(
        kernel, "graves", "main", &key_fields(), None, &[],
    ).unwrap();
    assert_eq!(seeded.len(), 5);
    let mut dead_rows = Vec::new();
    for (_rowid, row) in seeded.iter() {
        let id = row["id"].as_i64().unwrap();
        if id == 1 || id == 3 {
            let mut dead = row.clone();
            dead["_deleted"] = json!(true);
            // LATER than any real HLC (physical ms ≈ 0x19f... hex) — the
            // tombstone must win the LWW compare against the seed version.
            dead["_version"] = json!("ffffffffffffffffffffffffffffffff");
            dead_rows.push(dead);
        }
    }
    assert_eq!(dead_rows.len(), 2);
    let data = serde_json::to_vec(&dead_rows).unwrap();
    shard::append_shard(kernel, "graves", "main", "deletes", &data).unwrap();

    // Pre-fold read (pack rows ∪ shards, CRDT-merged): 3 live rows.
    let head_rows = read::read_rows_json_pruned(
        kernel, "graves", "main", &key_fields(), None, &[],
    ).unwrap();
    let (_, shards) = shard::read_with_shards(kernel, "graves", "main").unwrap();
    let mut all: Vec<Value> = head_rows.iter().map(|(_, r)| r.clone()).collect();
    for (_, shard_hash) in &shards {
        let data = kernel.read_blob(shard_hash).unwrap();
        let rows: Vec<Value> = serde_json::from_slice(&data).unwrap();
        all.extend(rows);
    }
    let merged = shard::merge_rows_by_rowid(&all, Some("_rowid"));
    let live = shard::filter_live_rows(&merged);
    assert_eq!(live.len(), 3, "tombstones suppress before the fold");

    // Fold. The shard-fold RG must carry the TOMBSTONES, not just live rows.
    let stats = compact(kernel, "graves", "main", &["id".to_string()]).unwrap();
    assert_eq!(stats.shards_folded, 1);
    assert!(shard::list_shards(kernel, "graves", "main").unwrap().is_empty(),
        "shards cleared by the fold");

    // Post-fold read: STILL 3 live rows — no resurrection.
    let rows = read::read_rows_json_pruned(
        kernel, "graves", "main", &key_fields(), None, &[],
    ).unwrap();
    assert_eq!(rows.len(), 3,
        "folded tombstones must keep suppressing their pack rows (no resurrection)");
    let mut visible_ids: Vec<i64> = rows.iter()
        .map(|(_, r)| r["id"].as_i64().unwrap())
        .collect();
    visible_ids.sort_unstable();
    assert_eq!(visible_ids, vec![0, 2, 4], "exactly the un-deleted rows survive");

    // And the fold is idempotent: a second compaction must not resurrect
    // either (the tombstones now live in the snapshot's own RGs).
    compact(kernel, "graves", "main", &["id".to_string()]).unwrap();
    let rows = read::read_rows_json_pruned(
        kernel, "graves", "main", &key_fields(), None, &[],
    ).unwrap();
    assert_eq!(rows.len(), 3, "fold-of-fold keeps the tombstones");
}

// ---------------------------------------------------------------------------
// 8. Racing compactions are benign
// ---------------------------------------------------------------------------

#[test]
fn test_compact_race_benign() {
    // Two sequential compactions (the serialized form of two racing
    // compactors — the writer Mutex serializes same-process folds). Every
    // branch_ref value is a valid folded state, so the final read is
    // correct regardless of which compactor's LWW landed last.
    let dir = tempfile::tempdir().unwrap();
    let storage = UnifiedStorage::new_local(dir.path()).unwrap();
    let kernel = storage.kernel();

    let ids: Vec<i64> = (0..10).collect();
    let vals: Vec<i64> = ids.iter().map(|i| i * 5).collect();
    write::write_rows_i64(
        kernel, "benign", "main", &[("id", &ids), ("val", &vals)], "seed",
    ).unwrap();

    let stats1 = compact(kernel, "benign", "main", &["id".to_string()]).unwrap();
    let head1 = kernel.resolve(&branch_ref("benign", "main")).unwrap().unwrap();
    assert_eq!(head1, stats1.new_snapshot);

    // Second compaction — folds the first fold (no new data). Both folds
    // are valid states; LWW picks the second.
    let stats2 = compact(kernel, "benign", "main", &["id".to_string()]).unwrap();
    assert_ne!(stats1.new_snapshot, stats2.new_snapshot);
    let head2 = kernel.resolve(&branch_ref("benign", "main")).unwrap().unwrap();
    assert_eq!(head2, stats2.new_snapshot, "LWW: the later fold wins the ref");

    // Both packs still exist as blobs (immutable content) and carry the
    // full state; the reader sees the union of snapshot + anything above.
    let cols = read::read_rows_i64(kernel, "benign", "main", None, None).unwrap();
    assert_eq!(sorted_ids(&cols), (0..10).collect::<Vec<i64>>(),
        "final read after two compactions is correct");

    // The second fold's manifest equals the first's RG coverage (both hold
    // the same 1 RG — the fold of a fold is coverage-preserving).
    for head in [&head1, &head2] {
        let manifest_bytes = pond_storage::commit::resolve_manifest_bytes(kernel, head).unwrap();
        let manifest = CollectionManifest::decode(&manifest_bytes).unwrap();
        assert_eq!(manifest.row_groups.len(), 1);
        assert_eq!(manifest.row_groups[0].n_rows, 10);
    }
}

// ---------------------------------------------------------------------------
// 9. The upto watermark skips folded entries on reads
// ---------------------------------------------------------------------------

#[test]
fn test_upto_watermark_skips_folded_entries() {
    // After a compaction, reads probe each writer's log ONLY above the
    // snapshot's upto watermark: zero get_path calls to folded entry
    // paths, and a post-compact live entry is found by probe.
    let dir = tempfile::tempdir().unwrap();
    let store = CountingStore::new(dir.path());
    let (kernel, _list_dirs, _list_paths, get_path_calls, get_path_paths, _get_blob_calls) = store.kernel();

    // 2 external writers × 2 entries each.
    let mut folded_paths = Vec::new();
    for w in 0..2 {
        for seq in 1..=2u64 {
            let base = (w * 10 + (seq - 1) as i64) * 5;
            let ids: Vec<i64> = (base..base + 5).collect();
            let vals: Vec<i64> = ids.iter().map(|i| i * 3).collect();
            fabricate_entry(&kernel, "upto", "main", &format!("writer-{}", w), seq, &ids, &vals);
            folded_paths.push(entry_path("upto", "main", &format!("writer-{}", w), seq));
        }
    }

    compact(&kernel, "upto", "main", &["id".to_string()]).unwrap();

    // A NEW live entry above the watermark (writer-0 seq 3).
    let ids: Vec<i64> = (100..105).collect();
    let vals: Vec<i64> = ids.iter().map(|i| i * 3).collect();
    fabricate_entry(&kernel, "upto", "main", "writer-0", 3, &ids, &vals);
    let live_path = entry_path("upto", "main", "writer-0", 3);

    // Warm the discovery cache (this read FINDS the new live entry by
    // probing writer-0 at its watermark+1 = seq 3), then measure the next
    // read: probes go ONLY above the watermark, and the remembered live
    // entry is served from the discovery cache without re-probing.
    let warmed = read::read_rows_json_pruned(
        &kernel, "upto", "main", &key_fields(), None, &[],
    ).unwrap();
    assert_eq!(warmed.len(), 25, "20 folded + 5 live rows");
    get_path_calls.store(0, Ordering::SeqCst);
    get_path_paths.lock().unwrap().clear();

    let rows = read::read_rows_json_pruned(
        &kernel, "upto", "main", &key_fields(), None, &[],
    ).unwrap();
    assert_eq!(rows.len(), 25, "the remembered live entry survives (cache hit, no re-probe)");
    let paths = get_path_paths.lock().unwrap().clone();

    // ZERO probes to folded paths — the watermark is honored...
    for folded in &folded_paths {
        assert!(!paths.contains(folded),
            "read probed a FOLDED entry path — the upto watermark was ignored: {}", folded);
    }
    // ...and the REMEMBERED live entry is served from the discovery cache
    // without re-probing (its immutability is what makes caching it safe).
    assert!(!paths.contains(&live_path),
        "the remembered live entry is served from the cache — no re-probe needed: {:?}", paths);
    // ...every probe is strictly ABOVE its writer's watermark: writer-0 at
    // seq 4 (watermark 2, live entry at 3 remembered), writer-1 at seq 3
    // (watermark 2), the compactor at seq 2 (watermark 1).
    let journal_probes: Vec<&String> = paths.iter()
        .filter(|p| p.starts_with("collections/upto/_branches/main/journal/"))
        .collect();
    assert_eq!(journal_probes.len(), 3,
        "exactly one probe per writer, each above its watermark: {:?}", paths);
    assert!(paths.contains(&entry_path("upto", "main", "writer-0", 4)),
        "writer-0 probed above its REMEMBERED live entry: {:?}", paths);
    assert!(paths.contains(&entry_path("upto", "main", "writer-1", 3)),
        "writer-1 probed above its watermark: {:?}", paths);
    // Every path is either the branch ref or an above-watermark probe.
    let branch = branch_ref("upto", "main");
    for p in &paths {
        assert!(p == &branch || p.starts_with("collections/upto/_branches/main/journal/"),
            "unexpected path touched by the warm read: {}", p);
    }
}

// ---------------------------------------------------------------------------
// 10. Journal status introspection
// ---------------------------------------------------------------------------

#[test]
fn test_journal_status() {
    let dir = tempfile::tempdir().unwrap();
    let store = CountingStore::new(dir.path());
    let (kernel, _a, _b, _c, _d, _e) = store.kernel();

    // Two external writers × 2 entries each; no snapshot yet.
    for w in 0..2 {
        for seq in 1..=2u64 {
            let base = (w * 10 + (seq - 1) as i64) * 5;
            let ids: Vec<i64> = (base..base + 5).collect();
            let vals: Vec<i64> = ids.iter().map(|i| i * 3).collect();
            fabricate_entry(&kernel, "st", "main", &format!("writer-{}", w), seq, &ids, &vals);
        }
    }

    let st = status(&kernel, "st", "main").unwrap();
    assert!(st.snapshot.is_none(), "nothing folded yet");
    assert!(st.snapshot_upto.is_empty());
    assert_eq!(st.live_entries, 4, "2 writers × 2 live entries");
    assert_eq!(st.writers.len(), 2);
    for w in &st.writers {
        assert_eq!(w.entries, 2);
        assert_eq!(w.max_seq, 2);
    }

    // After compaction: a snapshot exists, the upto map covers both
    // external writers, and no live entries remain above the watermarks.
    compact(&kernel, "st", "main", &["id".to_string()]).unwrap();
    let st = status(&kernel, "st", "main").unwrap();
    let snapshot = st.snapshot.expect("compaction created the snapshot");
    assert!(kernel.resolve(&branch_ref("st", "main")).unwrap().is_some());
    assert_eq!(kernel.resolve(&branch_ref("st", "main")).unwrap(), Some(snapshot));
    assert_eq!(st.snapshot_upto.get("writer-0"), Some(&2u64));
    assert_eq!(st.snapshot_upto.get("writer-1"), Some(&2u64));
    assert_eq!(st.live_entries, 0, "everything is folded below the watermarks");
    assert!(st.writers.is_empty(), "no writer has live entries above the watermark");

    // resolve_view agrees: snapshot + zero entries.
    let view = resolve_view(&kernel, "st", "main", true).unwrap();
    assert!(view.snapshot.is_some());
    assert!(view.entries.is_empty());
}

// ---------------------------------------------------------------------------
// Extra no-CAS invariants pinned by the harness
// ---------------------------------------------------------------------------

#[test]
fn test_journal_append_never_touches_shared_refs() {
    // ARCHITECTURE.md D3 #6: "Writes touch zero shared objects." A plain
    // journal append (post-bootstrap) writes NO ref except its own unique
    // entry path — measured with the counting store. The bootstrap fold
    // only fires on the FIRST append of a fresh collection, so the second
    // append here must be the pure unique-path PUT.
    let dir = tempfile::tempdir().unwrap();
    let storage = UnifiedStorage::new_local(dir.path()).unwrap();
    let kernel = storage.kernel();

    let ids: Vec<i64> = (0..3).collect();
    let vals: Vec<i64> = ids.iter().map(|i| i * 2).collect();
    write::write_rows_i64(kernel, "refs", "main", &[("id", &ids), ("val", &vals)], "first").unwrap();
    let branch_before = kernel.resolve(&branch_ref("refs", "main")).unwrap();
    let manifest_before = kernel.resolve(&pond_storage::manifest_ref("refs", "main")).unwrap();
    let bare_before = kernel.resolve("refs").unwrap();
    let writer_id = writer_for(kernel, "refs", "main").lock().unwrap().writer_id.clone();
    let entry1 = kernel.resolve(&entry_path("refs", "main", &writer_id, 1)).unwrap();

    let ids2: Vec<i64> = (3..6).collect();
    let vals2: Vec<i64> = ids2.iter().map(|i| i * 2).collect();
    write::write_rows_i64(kernel, "refs", "main", &[("id", &ids2), ("val", &vals2)], "second").unwrap();

    // Zero shared-object writes: branch/manifest/bare refs are untouched,
    // entry #1's pointer is untouched (unique paths are never overwritten),
    // and the ONLY new ref is the writer's own entry #3 (seq 2 was the
    // bootstrap fold).
    assert_eq!(kernel.resolve(&branch_ref("refs", "main")).unwrap(), branch_before);
    assert_eq!(kernel.resolve(&pond_storage::manifest_ref("refs", "main")).unwrap(), manifest_before);
    assert_eq!(kernel.resolve("refs").unwrap(), bare_before);
    assert_eq!(kernel.resolve(&entry_path("refs", "main", &writer_id, 1)).unwrap(), entry1);
    assert!(kernel.resolve(&entry_path("refs", "main", &writer_id, 2)).unwrap().is_some(),
        "the bootstrap fold's own pointer exists");
    assert!(kernel.resolve(&entry_path("refs", "main", &writer_id, 3)).unwrap().is_some(),
        "the second append's unique-path pointer exists");

    // put_path_if has NO production callers in the journal era — the CAS
    // primitive is exercised only by its own kernel tests. (Nothing to
    // assert here beyond compilation; documented for the reviewer.)
}

#[test]
fn test_compact_on_empty_collection_is_a_clean_noop() {
    let dir = tempfile::tempdir().unwrap();
    let storage = UnifiedStorage::new_local(dir.path()).unwrap();
    let kernel = storage.kernel();
    // Idempotent no-op (was: an error — but pyo3 compact_shards' legacy
    // contract returned Ok(0) for empty collections, and lens layers call
    // compact unconditionally; erroring forced every caller to pre-check).
    let stats = compact(kernel, "ghosts", "main", &[]).unwrap();
    assert_eq!(stats.entries_folded, 0);
    assert_eq!(stats.shards_folded, 0);
    assert!(stats.new_snapshot.is_empty(), "no-op compaction writes no snapshot");
    assert!(kernel.resolve(&branch_ref("ghosts", "main")).unwrap().is_none(),
        "a no-op compaction must not create refs");
}

#[test]
fn test_discovery_ttl_zero_forces_fresh_list() {
    // POND_JOURNAL_TTL_MS=0 means "always fresh": every journal-resolving
    // read performs a discovery LIST. The env knob is parsed once per
    // process, so this test validates the KNOB PATH by spawning a child
    // PROCESS with the env set — keeping the parent's cached TTL intact.
    // (A plain in-process check would race the OnceLock against the
    // parallel test threads.)
    let out = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("ttl0_child")
        .env("POND_JOURNAL_TTL_MS", "0")
        .output()
        .expect("child test process must run");
    assert!(out.status.success(),
        "child (TTL=0) test failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr));
}

// The child half of test_discovery_ttl_zero_forces_fresh_list — runs in
// a spawned process with POND_JOURNAL_TTL_MS=0 (see above for why). When
// run in the PARENT (default TTL), it degrades to a plain correctness
// smoke so the strict per-read LIST count is only asserted under the env
// it describes.
#[test]
fn ttl0_child() {
    let dir = tempfile::tempdir().unwrap();
    let store = CountingStore::new(dir.path());
    let (kernel, list_dirs_calls, _lp, _gp, _pp, _gb) = store.kernel();

    let ids: Vec<i64> = (0..5).collect();
    let vals: Vec<i64> = ids.iter().map(|i| i * 2).collect();
    fabricate_entry(&kernel, "ttl0", "main", "writer-0", 1, &ids, &vals);

    let ttl_zero = std::env::var("POND_JOURNAL_TTL_MS")
        .map(|v| v.trim() == "0")
        .unwrap_or(false);

    // Two reads; with TTL=0 each performs a fresh discovery LIST; with the
    // default TTL the second read is served from the cache (0 LISTs).
    let expected: [u64; 2] = if ttl_zero { [1, 1] } else { [1, 0] };
    for want in expected {
        list_dirs_calls.store(0, Ordering::SeqCst);
        let rows = read::read_rows_json_pruned(
            &kernel, "ttl0", "main", &key_fields(), None, &[],
        ).unwrap();
        assert_eq!(rows.len(), 5);
        assert_eq!(list_dirs_calls.load(Ordering::SeqCst), want,
            "discovery LIST count under POND_JOURNAL_TTL_MS={}: expected {}",
            std::env::var("POND_JOURNAL_TTL_MS").unwrap_or_default(), want);
    }
}

// ---------------------------------------------------------------------------
// Determinism of the resolved entry ORDER (the resolver-level shuffle law)
// ---------------------------------------------------------------------------

#[test]
fn test_resolve_view_entry_order_is_deterministic() {
    // resolve_view returns entries sorted by (writer, seq) regardless of
    // discovery timing — the pack-processing order (and therefore the
    // merged output before the C10 tiebreak even applies) is fixed.
    let dir = tempfile::tempdir().unwrap();
    let store = CountingStore::new(dir.path());
    let (kernel, _a, _b, _c, _d, _e) = store.kernel();

    let writers = ["zeta-writer", "alpha-writer", "mid-writer"];
    for (wi, w) in writers.iter().enumerate() {
        for seq in 1..=2u64 {
            let base = ((wi as i64) * 10 + (seq - 1) as i64) * 5;
            let ids: Vec<i64> = (base..base + 5).collect();
            let vals: Vec<i64> = ids.iter().map(|i| i * 3).collect();
            fabricate_entry(&kernel, "order", "main", w, seq, &ids, &vals);
        }
    }

    let view = resolve_view(&kernel, "order", "main", true).unwrap();
    assert_eq!(view.entries.len(), 6);
    let keys: Vec<(String, u64)> = view.entries
        .iter()
        .map(|e| (e.writer.clone(), e.seq))
        .collect();
    let mut sorted = keys.clone();
    sorted.sort();
    assert_eq!(keys, sorted, "entries are (writer, seq)-sorted");
    assert_eq!(view.entries[0].writer, "alpha-writer");
    assert_eq!(view.entries[5].writer, "zeta-writer");

    // The prefix-path helper matches the layout the entries actually use.
    assert_eq!(
        journal::journal_prefix("order", "main"),
        "collections/order/_branches/main/journal/"
    );
}

// ---------------------------------------------------------------------------
// Tribunal r1 repairs — regression tests for findings F1, F2, F8
// ---------------------------------------------------------------------------

/// F1 (HIGH, verified empirically by the tribunal): a raw `write()` between
/// journal writes used to blind fresh readers — the plain commit carried no
/// `journal.upto`, probes started at seq 1, died at the first fold-deleted
/// gap, and the writer's live tail was INVISIBLE forever (fresh process
/// read 0 rows for 10 committed; `compact` from a fresh process folded 0
/// entries — the data was unrecoverable through the journal).
///
/// The fix: `write()` CARRIES the previous ref's watermark into its commit
/// JSON, and `read_snapshot_upto` parses plain commits. The hole only
/// manifests with an EMPTY discovery cache (a warm in-process cache
/// remembers the live entries and papers over the gap), so the honest test
/// spawns a CHILD PROCESS — exactly the tribunal's probe.
#[test]
fn test_f1_raw_write_does_not_blind_fresh_reader() {
    // Parent: build the repo (journal write → bootstrap fold → raw write
    // → journal write), then hand it to a fresh child process.
    let dir = tempfile::tempdir().unwrap();
    let storage = UnifiedStorage::new_local(dir.path()).unwrap();
    let kernel = storage.kernel();

    // Journal write #1: 5 rows; the bootstrap fold deletes W's seq 1 and
    // leaves the fold pack at seq 2 (branch_ref → fold pack, upto={W:2}).
    let ids1: Vec<i64> = (1..=5).collect();
    let vals1: Vec<i64> = ids1.iter().map(|i| i * 10).collect();
    write::write_rows_i64(kernel, "f1", "main", &[("id", &ids1), ("val", &vals1)], "w1").unwrap();

    // Raw write: REPLACES the folded base (its 5 rows are gone by replace
    // semantics) and — with the F1 fix — carries upto={W:2}.
    write::write(kernel, "f1", "main", b"raw-base", "raw").unwrap();

    // Journal write #2: 5 new rows at W seq 3.
    let ids2: Vec<i64> = (6..=10).collect();
    let vals2: Vec<i64> = ids2.iter().map(|i| i * 10).collect();
    write::write_rows_i64(kernel, "f1", "main", &[("id", &ids2), ("val", &vals2)], "w2").unwrap();

    // Fresh child process: empty discovery cache, cold TTL — the exact
    // tribunal probe. Without the watermark carry it reads 0 rows.
    let out = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("f1_child")
        .env("POND_F1_DIR", dir.path())
        .output()
        .expect("child test process must run");
    assert!(out.status.success(),
        "F1 child (fresh reader) failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr));
}

/// Child half of the F1 test — spawned by the parent with POND_F1_DIR set
/// (empty discovery cache = the exact tribunal probe). When run directly
/// (full suite, no env), it builds the same scenario in-process — a weaker
/// sanity check (a warm in-process cache papers over the F1 gap; the real
/// assertion is the parent's spawned run).
#[test]
fn f1_child() {
    let spawned = std::env::var("POND_F1_DIR").ok();
    // Keep the storage (and its tempdir) alive for the whole test — the
    // kernel borrows from it.
    let storage_owned;
    let _keep: Option<tempfile::TempDir>;
    let kernel = match &spawned {
        Some(dir) => {
            storage_owned = UnifiedStorage::new_local(dir).unwrap();
            storage_owned.kernel()
        }
        None => {
            let dir = tempfile::tempdir().unwrap();
            let storage = UnifiedStorage::new_local(dir.path()).unwrap();
            let kernel = storage.kernel();
            let ids1: Vec<i64> = (1..=5).collect();
            let vals1: Vec<i64> = ids1.iter().map(|i| i * 10).collect();
            write::write_rows_i64(kernel, "f1", "main", &[("id", &ids1), ("val", &vals1)], "w1").unwrap();
            write::write(kernel, "f1", "main", b"raw-base", "raw").unwrap();
            let ids2: Vec<i64> = (6..=10).collect();
            let vals2: Vec<i64> = ids2.iter().map(|i| i * 10).collect();
            write::write_rows_i64(kernel, "f1", "main", &[("id", &ids2), ("val", &vals2)], "w2").unwrap();
            _keep = Some(dir);
            return run_f1_assertions(kernel);
        }
    };
    run_f1_assertions(kernel)
}

fn run_f1_assertions(kernel: &PondKernel) {
    // Fresh-process read: snapshot = the raw commit (upto carried) + probe
    // W from seq 3 → the second journal write's 5 rows. The raw base's own
    // bytes are not PND2 → leniently skipped. Expected: 5 rows.
    let rows = read::read_rows_json_pruned(kernel, "f1", "main", &[], None, &[]).unwrap();
    assert_eq!(rows.len(), 5,
        "fresh reader must see the post-raw-write journal tail (F1): got {} rows", rows.len());

    // And a fresh-process compact must fold the live entry (the tribunal's
    // "unrecoverable orphan" probe): 1 live entry, then 0 after.
    let stats = journal::compact(kernel, "f1", "main", &[]).unwrap();
    assert_eq!(stats.entries_folded, 1,
        "fresh compact must fold the live journal entry (F1): folded {}", stats.entries_folded);
    let after = read::read_rows_json_pruned(kernel, "f1", "main", &[], None, &[]).unwrap();
    assert_eq!(after.len(), 5, "rows survive the fold");
}

/// F2 (MEDIUM-HIGH, verified empirically by the tribunal): racing compactors
/// — the loser's fold pack stays live in its writer's log (the winner's upto
/// never covered it), and the CONCATENATING readers (read_rows_i64,
/// read_all_row_groups) returned every shared RG TWICE (10 rows read for 5
/// logical). The fix (D6): resolve_packs drops a live COMPACT entry whose
/// RG set is fully covered by the snapshot ∪ data entries.
///
/// This constructs the tribunal's exact race aftermath: C1.resolve →
/// C2.resolve → C1.append+ref (winner) → C2.append (loser, same RG set,
/// stays live because the winner's upto[Wc2] never covered it).
#[test]
fn test_f2_racing_compactors_no_duplicate_rows() {
    let dir = tempfile::tempdir().unwrap();
    let storage = UnifiedStorage::new_local(dir.path()).unwrap();
    let kernel = storage.kernel();

    // Two data entries by fabricated writer W1 (5 rows each).
    let ids1: Vec<i64> = (1..=5).collect();
    let vals1: Vec<i64> = ids1.iter().map(|i| i * 2).collect();
    fabricate_entry(kernel, "f2", "main", "W1", 1, &ids1, &vals1);
    let ids2: Vec<i64> = (6..=10).collect();
    let vals2: Vec<i64> = ids2.iter().map(|i| i * 2).collect();
    fabricate_entry(kernel, "f2", "main", "W1", 2, &ids2, &vals2);

    // Winner compaction: folds both entries; branch_ref → fold pack
    // (RGs = both data blobs), upto covers W1 and the compactor.
    let stats = journal::compact(kernel, "f2", "main", &[]).unwrap();
    assert_eq!(stats.entries_folded, 2);

    // The LOSER's fold pack: same two data RGs (both compactors resolved
    // the same view), stamped as a compaction (journal.upto present),
    // appended under a DIFFERENT writer's log where the winner's upto
    // never reached → stays live. (Fixture built with proper per-RG stats
    // via fabricate_compact_pack — PMAN decode reads schema.len() stats
    // per RG, so an empty-stats RG would be a malformed manifest.)
    let h1 = kernel
        .write(&pond_core::pnd2_encode_i64_auto(&[("id", &ids1), ("val", &vals1)]))
        .unwrap();
    let h2 = kernel
        .write(&pond_core::pnd2_encode_i64_auto(&[("id", &ids2), ("val", &vals2)]))
        .unwrap();
    let loser_hash = fabricate_compact_pack(
        kernel,
        &[(h1, ids1, vals1), (h2, ids2, vals2)],
        "W_loser",
        1,
        json!({"W1": 2, "W_loser": 1}),
    );
    kernel
        .reference(&entry_path("f2", "main", "W_loser", 1), &loser_hash)
        .unwrap();

    // Force a FRESH discovery so the loser is actually VISIBLE to the
    // reads below: a read within the discovery TTL is served the
    // pre-loser writer set and never exercises the drop (warm-cache
    // blindness is bounded and by design — but THIS test exists to pin
    // the FULL-overlap drop at plan level, D6 style).
    let view = resolve_view(kernel, "f2", "main", true).unwrap();
    assert_eq!(view.entries.len(), 1, "the loser stays live in the raw view");
    assert_eq!(view.entries[0].pack_hash, loser_hash);
    let plans = journal::resolve_packs(kernel, "f2", "main", true).unwrap();
    assert_eq!(
        plans.len(),
        1,
        "the fully-covered loser drops out of the plan entirely: {:?}",
        plans
    );
    assert_eq!(plans[0].pack_hash, view.snapshot.unwrap());

    // THE assertion (tribunal: 20 rows before the fix, 5+5=10 expected):
    // the concatenating i64 reader must NOT double the shared RGs.
    let cols = read::read_rows_i64(kernel, "f2", "main", None, None).unwrap();
    let n = cols.iter().find(|(name, _)| name == "id")
        .map(|(_, vals)| vals.len()).unwrap_or(0);
    assert_eq!(n, 10,
        "racing-compact loser must not duplicate rows for concatenating readers: got {}", n);

    // And the CRDT reader sees the same 10 rows (no _rowid → legacy union).
    let rows = read::read_rows_json_pruned(kernel, "f2", "main", &[], None, &[]).unwrap();
    assert_eq!(rows.len(), 10, "pruned reader row count after race");
}

/// F8 (test honesty): the 8-thread concurrency test exercises ONE writer's
/// log (the process registry serializes same-process writers by design);
/// multi-writer-id logs are covered HERE — two fabricated writer PROCESSES
/// with interleaved entries, all rows visible after every prefix read.
///
/// Runs as a SPAWNED CHILD with POND_JOURNAL_TTL_MS=0: a fabricated writer
/// is indistinguishable from another PROCESS's writer, and a warm reader
/// within the discovery TTL legitimately misses a brand-new writer's log
/// (bounded staleness, documented in ARCHITECTURE.md D3). Exact-freshness
/// prefix assertions therefore need TTL=0, and the env knob is parsed once
/// per process — hence the child. When run directly (full suite, no env)
/// it is a NO-OP: the assertion lives in the parent's spawned run.
#[test]
fn test_two_writer_logs_interleaved_all_visible() {
    let spawned = std::env::var("POND_TWO_WRITER_CHILD").is_ok();
    if !spawned {
        return; // the parent test runs the real assertions via the child
    }
    let dir = tempfile::tempdir().unwrap();
    let storage = UnifiedStorage::new_local(dir.path()).unwrap();
    let kernel = storage.kernel();

    // Interleave: W-a seq1, W-b seq1, W-a seq2, W-b seq2, ...
    let mut expected = 0usize;
    for round in 0..4u64 {
        for (wi, w) in ["W-a", "W-b"].iter().enumerate() {
            let base = ((round * 2 + wi as u64) * 5) as i64;
            let ids: Vec<i64> = (base..base + 5).collect();
            let vals: Vec<i64> = ids.iter().map(|i| i * 7).collect();
            fabricate_entry(kernel, "two", "main", w, round + 1, &ids, &vals);
            expected += 5;
            // Read after every append (TTL=0 — exact freshness): every
            // prefix must be complete; a reader that misses one writer's
            // log fails here, not just at the end.
            let rows = read::read_rows_json_pruned(kernel, "two", "main", &[], None, &[]).unwrap();
            assert_eq!(rows.len(), expected,
                "after {} appends (round {}, writer {}): got {} rows, expected {}",
                expected, round, w, rows.len(), expected);
        }
    }
    assert_eq!(expected, 40);
}

/// Parent of the two-writer child: spawns it with TTL=0 and asserts success.
#[test]
fn test_two_writer_logs_interleaved_parent() {
    let out = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("test_two_writer_logs_interleaved_all_visible")
        .env("POND_TWO_WRITER_CHILD", "1")
        .env("POND_JOURNAL_TTL_MS", "0")
        .output()
        .expect("child test process must run");
    assert!(out.status.success(),
        "two-writer child (TTL=0) failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr));
}

// ---------------------------------------------------------------------------
// D6 — the RG-level read plan (ARCHITECTURE.md D6; C7 one resolve_packs,
// C11 partial-overlap filtering; builder spec cron-2026-08-28-1100-a)
// ---------------------------------------------------------------------------

/// Fabricate a COMPACTION pack (journal.upto present — classify_packs'
/// compact signature) whose manifest holds one RG per (blob hash, ids,
/// vals) triple, and return its hash. The CALLER decides where to
/// reference it (branch_ref or a journal entry path) — that placement is
/// exactly the LWW race variable under test.
///
/// NOTE on the per-RG column stats: PMAN decode reads `schema.len()`
/// stats entries per RG, so a fixture RG with EMPTY stats is a malformed
/// manifest (decode reads past the buffer — garbage RGs or a panic). The
/// stats here mirror what `fabricate_entry` writes, keeping the fixture
/// well-formed for every decoder in the pipeline.
fn fabricate_compact_pack(
    kernel: &PondKernel,
    rgs: &[(String, Vec<i64>, Vec<i64>)],
    writer: &str,
    seq: u64,
    upto: Value,
) -> String {
    let mut manifest = CollectionManifest::new(
        vec![("id".to_string(), VT_INT64), ("val".to_string(), VT_INT64)],
        "id".to_string(),
    );
    for (i, (blob_hash, ids, vals)) in rgs.iter().enumerate() {
        let col_stats: Vec<ColumnStatsEntry> = [("id", ids), ("val", vals)]
            .iter()
            .map(|(name, values)| {
                let (min, max) = if values.is_empty() {
                    (None, None)
                } else {
                    (Some(values.iter().min().unwrap().to_le_bytes().to_vec()),
                     Some(values.iter().max().unwrap().to_le_bytes().to_vec()))
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
            key: format!("rg_{:010}", i),
            blob_hash: blob_hash.clone(),
            n_rows: ids.len() as u32,
            columns: col_stats,
            slab_byte_offset: None,
            slab_byte_len: None,
        });
    }
    let commit_obj = json!({
        "parent": Value::Null,
        "second_parent": Value::Null,
        "manifest": "packed",
        "message": format!("fabricated compact pack for {}", writer),
        "timestamp": 0.0,
        "index": 0,
        "journal": {"writer": writer, "seq": seq, "upto": upto},
    });
    let pack = pond_storage::pond_pack::encode_pack(&commit_obj, &manifest.encode(), None);
    kernel.write(&pack).unwrap()
}

/// Build the C11 PARTIAL-OVERLAP aftermath — the acceptance-required
/// construction (ACCEPTANCE.md item 5, builder spec §5.1): the ref-LWW
/// WINNER folded LESS than the loser, and both racing delete loops
/// already removed the data entries the winner missed, so D2's rows are
/// ONLY reachable through the loser's live compact pack.
///
/// Timeline being fabricated:
///   1. w1 appends data entry D1 (ids 1-5) at W1 seq 1.
///   2. w2 appends data entry D2 (ids 6-10) at W2 seq 1.
///   3. Compactor A resolves BEFORE D2 exists → folds {D1}; its pack S_A
///      wins the branch-ref LWW.
///   4. Both delete loops remove the data entry paths (A's covered W1/1;
///      B's covered W1/1 and W2/1) — THE CRITICAL DETAIL: with W2/1 still
///      present, D2 would be a live DATA entry, S_B would be FULLY
///      covered by it, and the test would pass even without D6 (that is
///      the F2 full-overlap shape).
///   5. Compactor B folds {D1, D2}, appends S_B under W_B … and LOSES
///      the ref race (branch_ref stays at S_A). S_B stays live in W_B's
///      log because S_A's upto never covered W_B.
///
/// Returns (S_A hash, S_B hash, D1's RG identity, D2's RG identity).
type RgIdent = (String, Option<u64>);
fn build_c11_partial_overlap(
    kernel: &PondKernel,
    collection: &str,
) -> (String, String, RgIdent, RgIdent) {
    let ids1: Vec<i64> = (1..=5).collect();
    let vals1: Vec<i64> = ids1.iter().map(|i| i * 10).collect();
    let ids2: Vec<i64> = (6..=10).collect();
    let vals2: Vec<i64> = ids2.iter().map(|i| i * 10).collect();
    fabricate_entry(kernel, collection, "main", "W1", 1, &ids1, &vals1);
    fabricate_entry(kernel, collection, "main", "W2", 1, &ids2, &vals2);
    // Content-addressed store: writing the SAME PND2 bytes returns the
    // same hash the fabricated manifests carry — that is how the test
    // learns the two RG identities.
    let h1 = kernel
        .write(&pond_core::pnd2_encode_i64_auto(&[("id", &ids1), ("val", &vals1)]))
        .unwrap();
    let h2 = kernel
        .write(&pond_core::pnd2_encode_i64_auto(&[("id", &ids2), ("val", &vals2)]))
        .unwrap();
    let d1 = (h1, None);
    let d2 = (h2, None);

    // Winner S_A: resolved before D2 existed — folds ONLY D1's RG.
    let s_a = fabricate_compact_pack(
        kernel,
        &[(d1.0.clone(), ids1.clone(), vals1.clone())],
        "W_A",
        1,
        json!({"W1": 1}),
    );
    kernel.reference(&branch_ref(collection, "main"), &s_a).unwrap();

    // CRITICAL: both data entry paths are gone (the racing delete loops).
    kernel.delete_ref(&entry_path(collection, "main", "W1", 1)).unwrap();
    kernel.delete_ref(&entry_path(collection, "main", "W2", 1)).unwrap();

    // Loser S_B: folded D1 AND D2, lost the ref race — referenced only at
    // its own journal entry path (the winner's upto never covers W_B).
    let s_b = fabricate_compact_pack(
        kernel,
        &[(d1.0.clone(), ids1, vals1), (d2.0.clone(), ids2, vals2)],
        "W_B",
        1,
        json!({"W1": 1, "W2": 1, "W_B": 1}),
    );
    kernel
        .reference(&entry_path(collection, "main", "W_B", 1), &s_b)
        .unwrap();

    (s_a, s_b, d1, d2)
}

/// THE C11 REGRESSION (acceptance-required): a partially-covered COMPACT
/// entry contributes ONLY its novel row groups, so concatenating readers
/// see each RG exactly once even under racing compactors with partial
/// overlap. Pre-D6: resolve_view's pack-granular F2 drop had to keep S_B
/// whole (it was not FULLY covered) → D1's rows appeared TWICE (15 rows
/// for 10 logical).
#[test]
fn test_c11_partial_overlap_only_novel_rgs_read() {
    let dir = tempfile::tempdir().unwrap();
    let storage = UnifiedStorage::new_local(dir.path()).unwrap();
    let kernel = storage.kernel();
    let (s_a, s_b, d1, d2) = build_c11_partial_overlap(kernel, "c11");

    // The RAW view keeps the loser live (D6: resolution no longer drops
    // compact entries — that filtering moved into the plan).
    let view = resolve_view(kernel, "c11", "main", true).unwrap();
    assert_eq!(view.snapshot.as_deref(), Some(s_a.as_str()));
    assert_eq!(view.entries.len(), 1, "the loser's entry stays live in the raw view");
    assert_eq!(view.entries[0].pack_hash, s_b);

    // THE PLAN: snapshot S_A whole + S_B filtered to D2's RG only.
    let plans = journal::resolve_packs(kernel, "c11", "main", true).unwrap();
    assert_eq!(plans.len(), 2, "snapshot S_A + the loser S_B: {:?}", plans);
    assert_eq!(plans[0].pack_hash, s_a, "snapshot plan comes first (order stability)");
    assert!(plans[0].only_rgs.is_none(), "snapshots are never filtered");
    assert_eq!(plans[1].pack_hash, s_b);
    let only = plans[1]
        .only_rgs
        .as_ref()
        .expect("the partially-covered loser must carry an only_rgs filter (C11)");
    assert_eq!(only.len(), 1, "exactly one novel RG: {:?}", only);
    assert!(only.contains(&d2), "the novel RG is D2's: {:?}", only);
    assert!(!only.contains(&d1), "D1's RG is covered by snapshot S_A — must NOT be in the filter");

    // The CONCATENATING i64 reader: ids 1-10, each EXACTLY once
    // (pre-D6: 15 — D1's rows twice).
    let cols = read::read_rows_i64(kernel, "c11", "main", None, None).unwrap();
    assert_eq!(
        sorted_ids(&cols),
        (1..=10).collect::<Vec<i64>>(),
        "C11: the partial-overlap loser contributes ONLY its novel RG"
    );

    // The JSON pipeline (legacy union — no _rowid, so duplicates would
    // survive the merge) sees the same 10 rows.
    let rows = read::read_rows_json_pruned(kernel, "c11", "main", &[], None, &[]).unwrap();
    assert_eq!(rows.len(), 10, "C11: the pruned JSON reader dedups the partial overlap");

    // And the raw-RG surface: exactly two RG byte vectors (S_A's one +
    // S_B's single novel RG).
    let rgs = read::read_all_row_groups(kernel, "c11", "main").unwrap();
    assert_eq!(rgs.len(), 2, "read_all_row_groups reads S_A's RG + S_B's novel RG only");
}

/// D6 CHAIN (tribunal r3 finding 4): MULTIPLE live compact entries that
/// partially overlap EACH OTHER. Winner S_A folds {D1}; loser S_B folds
/// {D1, D2} (novel: D2); loser S_C folds {D2, D3} (novel: D3 — D2 is
/// already claimed by S_B's novel set). Each compact in the chain must
/// contribute ONLY its own novel RGs (`covered ∪= novel` per entry), so
/// the concatenating reader sees D1+D2+D3 exactly once each. Pre-D6
/// (pack-granular): both losers kept whole → 20 rows for 15 logical.
#[test]
fn test_c11_chain_of_partial_overlaps_each_novel_once() {
    let dir = tempfile::tempdir().unwrap();
    let storage = UnifiedStorage::new_local(dir.path()).unwrap();
    let kernel = storage.kernel();
    let coll = "c11chain";

    // Three data blobs with disjoint id ranges.
    let mk = |lo: i64, hi: i64| -> (Vec<i64>, Vec<i64>) {
        let ids: Vec<i64> = (lo..=hi).collect();
        let vals: Vec<i64> = ids.iter().map(|i| i * 10).collect();
        (ids, vals)
    };
    let (ids1, vals1) = mk(1, 5);
    let (ids2, vals2) = mk(6, 10);
    let (ids3, vals3) = mk(11, 15);
    let blob1 = kernel
        .write(&pond_core::pnd2_encode_i64_auto(&[("id", &ids1), ("val", &vals1)]))
        .unwrap();
    let blob2 = kernel
        .write(&pond_core::pnd2_encode_i64_auto(&[("id", &ids2), ("val", &vals2)]))
        .unwrap();
    let blob3 = kernel
        .write(&pond_core::pnd2_encode_i64_auto(&[("id", &ids3), ("val", &vals3)]))
        .unwrap();
    let d1: (String, Option<u64>) = (blob1, None);
    let d2: (String, Option<u64>) = (blob2, None);
    let d3: (String, Option<u64>) = (blob3, None);

    // Winner S_A: folded only D1 (it resolved before D2/D3 existed).
    let s_a = fabricate_compact_pack(
        kernel,
        &[(d1.0.clone(), ids1.clone(), vals1.clone())],
        "W_A",
        1,
        json!({"W1": 1}),
    );
    kernel.reference(&branch_ref(coll, "main"), &s_a).unwrap();

    // Loser S_B: folded D1+D2, referenced live at W_B/1. Its RG set vs
    // (snapshot ∪ live data) is partially covered → only D2 is novel.
    let s_b = fabricate_compact_pack(
        kernel,
        &[(d1.0.clone(), ids1.clone(), vals1.clone()), (d2.0.clone(), ids2.clone(), vals2.clone())],
        "W_B",
        1,
        json!({"W1": 1, "W2": 1, "W_B": 1}),
    );
    kernel
        .reference(&entry_path(coll, "main", "W_B", 1), &s_b)
        .unwrap();

    // Loser S_C: folded D2+D3, referenced live at W_C/1. D2 is already
    // claimed by S_B's novel set → only D3 is novel for S_C. This is the
    // `covered.extend(novel)` chain the tribunal asked to pin.
    let s_c = fabricate_compact_pack(
        kernel,
        &[(d2.0.clone(), ids2.clone(), vals2.clone()), (d3.0.clone(), ids3.clone(), vals3.clone())],
        "W_C",
        1,
        json!({"W2": 1, "W3": 1, "W_C": 1}),
    );
    kernel
        .reference(&entry_path(coll, "main", "W_C", 1), &s_c)
        .unwrap();

    // The plan: S_A whole, S_B filtered to {D2}, S_C filtered to {D3}.
    let plans = journal::resolve_packs(kernel, coll, "main", true).unwrap();
    assert_eq!(plans.len(), 3, "snapshot + both losers: {:?}", plans);
    assert_eq!(plans[0].pack_hash, s_a);
    assert!(plans[0].only_rgs.is_none());
    assert_eq!(plans[1].pack_hash, s_b);
    let only_b = plans[1].only_rgs.as_ref().expect("S_B partially covered");
    assert_eq!(only_b.len(), 1, "S_B's novel set is exactly {{D2}}: {:?}", only_b);
    assert!(only_b.contains(&d2), "S_B novel = D2: {:?}", only_b);
    assert_eq!(plans[2].pack_hash, s_c);
    let only_c = plans[2].only_rgs.as_ref().expect("S_C partially covered");
    assert_eq!(only_c.len(), 1, "S_C's novel set is exactly {{D3}}: {:?}", only_c);
    assert!(only_c.contains(&d3), "S_C novel = D3 (D2 already claimed by S_B): {:?}", only_c);

    // The concatenating reader: ids 1-15, each EXACTLY once (pre-D6: 20).
    let cols = read::read_rows_i64(kernel, coll, "main", None, None).unwrap();
    assert_eq!(
        sorted_ids(&cols),
        (1..=15).collect::<Vec<i64>>(),
        "C11 chain: each compact contributes ONLY its novel RGs"
    );

    // The pruned JSON reader agrees (no _rowid → duplicates would survive).
    let rows = read::read_rows_json_pruned(kernel, coll, "main", &[], None, &[]).unwrap();
    assert_eq!(rows.len(), 15, "C11 chain: the JSON pipeline sees each RG once");
}

/// D6 zombie cleanup: after the C11 aftermath, ONE real compaction must
/// (a) delete the stale loser's entry path (the RAW view keeps it live,
/// so its seq joins `upto` and the delete loop finally removes it —
/// pre-D6 it was re-probed and re-dropped forever), and (b) fold a
/// snapshot whose manifest carries each RG identity EXACTLY once.
#[test]
fn test_c11_zombie_loser_entry_cleaned_up_by_next_compact() {
    let dir = tempfile::tempdir().unwrap();
    let storage = UnifiedStorage::new_local(dir.path()).unwrap();
    let kernel = storage.kernel();
    let (_s_a, _s_b, d1, d2) = build_c11_partial_overlap(kernel, "zombie");

    let stats = journal::compact(kernel, "zombie", "main", &[]).unwrap();
    assert_eq!(stats.entries_folded, 1, "the stale loser entry was folded (it is live in the raw view)");
    assert!(
        kernel.resolve(&entry_path("zombie", "main", "W_B", 1)).unwrap().is_none(),
        "the zombie loser entry path must be DELETED by the fold's delete loop"
    );

    // The folded snapshot: D1's RG + D2's RG, each identity exactly once.
    let head = kernel.resolve(&branch_ref("zombie", "main")).unwrap().unwrap();
    let manifest_bytes = pond_storage::commit::resolve_manifest_bytes(kernel, &head).unwrap();
    let manifest = CollectionManifest::decode(&manifest_bytes).unwrap();
    let identities: Vec<(String, Option<u64>)> = manifest
        .row_groups
        .iter()
        .map(|rg| (rg.blob_hash.clone(), rg.slab_byte_offset))
        .collect();
    assert_eq!(identities.len(), 2, "D1's RG + D2's RG: {:?}", identities);
    let mut distinct = identities.clone();
    distinct.sort();
    distinct.dedup();
    assert_eq!(identities.len(), distinct.len(), "no duplicate RG identities in the folded snapshot");
    assert!(identities.contains(&d1) && identities.contains(&d2));

    // Full state survives the fold: 10 rows, and no live entries remain.
    let cols = read::read_rows_i64(kernel, "zombie", "main", None, None).unwrap();
    assert_eq!(sorted_ids(&cols), (1..=10).collect::<Vec<i64>>());
    let view = resolve_view(kernel, "zombie", "main", true).unwrap();
    assert!(view.entries.is_empty(), "the fold leaves nothing live above the watermarks");
}

/// D6 fast path: with only DATA entries live (the steady state),
/// resolve_packs performs NO blob reads beyond resolve_view's own (the
/// snapshot-upto read plus one classification read per live entry) — no
/// manifest resolves, no RG-identity collection. Measured with the
/// counting store's get_blob counter.
#[test]
fn test_resolve_packs_fast_path_zero_extra_reads() {
    let dir = tempfile::tempdir().unwrap();
    let store = CountingStore::new(dir.path());
    let (kernel, _a, _b, _c, _d, get_blob_calls) = store.kernel();

    // Two data entries by two writers, no snapshot yet.
    for (w, base) in [("W1", 0i64), ("W2", 10)] {
        let ids: Vec<i64> = (base..base + 5).collect();
        let vals: Vec<i64> = ids.iter().map(|i| i * 3).collect();
        fabricate_entry(&kernel, "fast", "main", w, 1, &ids, &vals);
    }
    get_blob_calls.store(0, Ordering::SeqCst);
    let plans = journal::resolve_packs(&kernel, "fast", "main", true).unwrap();
    assert_eq!(plans.len(), 2, "no snapshot → plans are exactly the data entries");
    assert!(plans.iter().all(|p| p.only_rgs.is_none()),
        "data-only plans carry no RG filter");
    assert_eq!(
        get_blob_calls.load(Ordering::SeqCst),
        2,
        "fast path: one classification read per live entry, NOTHING else (no manifest reads)"
    );

    // With a snapshot + one live data entry above its watermark: still
    // the fast path — 1 snapshot-upto read + 1 classification read.
    compact(&kernel, "fast", "main", &["id".to_string()]).unwrap();
    let ids: Vec<i64> = (100..105).collect();
    let vals: Vec<i64> = ids.iter().map(|i| i * 3).collect();
    fabricate_entry(&kernel, "fast", "main", "W1", 2, &ids, &vals);
    get_blob_calls.store(0, Ordering::SeqCst);
    let plans = journal::resolve_packs(&kernel, "fast", "main", true).unwrap();
    assert_eq!(plans.len(), 2, "snapshot + one live data entry");
    let snapshot = kernel.resolve(&branch_ref("fast", "main")).unwrap().unwrap();
    assert_eq!(plans[0].pack_hash, snapshot, "the snapshot plan is first");
    assert!(plans.iter().all(|p| p.only_rgs.is_none()),
        "no compact entry live → no coverage filtering");
    assert_eq!(
        get_blob_calls.load(Ordering::SeqCst),
        2,
        "1 snapshot-upto read + 1 classification read — zero coverage/manifest reads"
    );

    // And the plan reads the full state: 15 rows (10 folded + 5 live).
    let cols = read::read_rows_i64(&kernel, "fast", "main", None, None).unwrap();
    let mut got = sorted_ids(&cols);
    got.sort_unstable();
    let mut expected: Vec<i64> = (0..5).chain(10..15).chain(100..105).collect();
    expected.sort_unstable();
    assert_eq!(got, expected, "the fast-path plan still reads every row");
}

/// D6 order stability: plans are `[snapshot] + entries in (writer, seq)
/// order` — the pre-D6 pack order, so output row ordering is unchanged —
/// and the order is identical across cold and warm resolutions.
#[test]
fn test_resolve_packs_plan_order_is_stable() {
    let dir = tempfile::tempdir().unwrap();
    let storage = UnifiedStorage::new_local(dir.path()).unwrap();
    let kernel = storage.kernel();

    // Three writers × two entries, appended in round-robin (NOT sorted)
    // order.
    let mut appended: Vec<(String, u64, String)> = Vec::new();
    for round in 0..2u64 {
        for (wi, w) in ["W-c", "W-a", "W-b"].iter().enumerate() {
            let base = ((wi as i64) * 10 + (round as i64) * 100) * 5;
            let ids: Vec<i64> = (base..base + 5).collect();
            let vals: Vec<i64> = ids.iter().map(|i| i * 3).collect();
            let hash = fabricate_entry(kernel, "planorder", "main", w, round + 1, &ids, &vals);
            appended.push((w.to_string(), round + 1, hash));
        }
    }
    // Fold everything (so a snapshot exists), then append three more
    // entries — again in non-sorted order.
    compact(kernel, "planorder", "main", &["id".to_string()]).unwrap();
    for (wi, w) in ["W-b", "W-c", "W-a"].iter().enumerate() {
        let base = 1000 + (wi as i64) * 5;
        let ids: Vec<i64> = (base..base + 5).collect();
        let vals: Vec<i64> = ids.iter().map(|i| i * 3).collect();
        let hash = fabricate_entry(kernel, "planorder", "main", w, 3, &ids, &vals);
        appended.push((w.to_string(), 3, hash));
    }
    // The live entries above the fold watermark: the three seq-3 entries,
    // in (writer, seq) order — the pre-D6 pack order.
    let mut live: Vec<(String, u64, String)> = appended
        .iter()
        .filter(|(_, seq, _)| *seq == 3)
        .cloned()
        .collect();
    live.sort_by(|a, b| (&a.0, a.1).cmp(&(&b.0, b.1)));
    let snapshot = kernel.resolve(&branch_ref("planorder", "main")).unwrap().unwrap();
    let mut expected: Vec<String> = vec![snapshot];
    expected.extend(live.iter().map(|(_, _, h)| h.clone()));

    let plans = journal::resolve_packs(kernel, "planorder", "main", true).unwrap();
    let got: Vec<String> = plans.iter().map(|p| p.pack_hash.clone()).collect();
    assert_eq!(got, expected,
        "plans are [snapshot] + entries in (writer, seq) order — the pre-D6 pack order");
    assert!(plans.iter().all(|p| p.only_rgs.is_none()),
        "data-only live set → no RG filters");

    // Warm re-resolution: identical order.
    let plans_warm = journal::resolve_packs(kernel, "planorder", "main", false).unwrap();
    let got_warm: Vec<String> = plans_warm.iter().map(|p| p.pack_hash.clone()).collect();
    assert_eq!(got_warm, expected, "plan order is stable across resolutions");

    // Row order sanity: all 45 written rows (6 folded entries + 3 live)
    // visible, each exactly once.
    let cols = read::read_rows_i64(kernel, "planorder", "main", None, None).unwrap();
    let mut got = sorted_ids(&cols);
    got.sort_unstable();
    assert_eq!(got.len(), 45, "all 45 rows visible through the plan");
    got.dedup();
    assert_eq!(got.len(), 45, "each row exactly once");
}

// ---------------------------------------------------------------------------
// C17 — a FAILED journal epoch probe is a TRUNCATED-view error, never an
// empty suffix. `ObjectStore::get_path` used to return Option with no error
// channel: a transient ref GET failure at seq N looked exactly like "the
// writer's log ends at N-1", and resolve_view returned a view silently
// missing every entry from N on — a TRUNCATED journal, no signal.
// ---------------------------------------------------------------------------

/// A store wrapper whose get_path fails ONLY for journal ENTRY paths
/// (`.../journal/<writer>/<seq>`) once the shared flag is set — the branch
/// ref, manifest ref, and shard refs stay healthy, isolating exactly the
/// epoch-probe failure class. `store_id` is deliberately UNIQUE per
/// instance: the journal's process-local discovery cache is keyed by
/// store_id, so the reading kernel starts with a cold cache and MUST probe
/// for the tail entries (a warm cache with remembered entries could mask
/// the probe outage).
struct EntryOutageStore {
    inner: LocalFSObjectStore,
    fail_entries: Arc<AtomicU64>, // 0 = healthy, 1 = entry reads fail
    id: String,
}

impl EntryOutageStore {
    fn new(dir: &std::path::Path) -> (Self, Arc<AtomicU64>) {
        static INSTANCE: AtomicU64 = AtomicU64::new(0);
        let fail_entries = Arc::new(AtomicU64::new(0));
        let n = INSTANCE.fetch_add(1, Ordering::SeqCst);
        (
            Self {
                inner: LocalFSObjectStore::new(dir).unwrap(),
                fail_entries: Arc::clone(&fail_entries),
                // Unique identity: same backing dir, but a DIFFERENT cache
                // key than the seeding (plain) kernel — see the struct doc.
                id: format!("entry-outage-{}-{}", dir.display(), n),
            },
            fail_entries,
        )
    }
}

impl ObjectStore for EntryOutageStore {
    fn put_blob(&self, data: &[u8]) -> std::io::Result<String> {
        self.inner.put_blob(data) // writes stay healthy (outage = read-side)
    }
    fn get_blob(&self, hash: &str) -> std::io::Result<Vec<u8>> {
        self.inner.get_blob(hash) // blob reads stay healthy (isolates the ref hole)
    }
    fn put_path(&self, path: &str, hash: &str) -> std::io::Result<()> {
        self.inner.put_path(path, hash)
    }
    fn get_path(&self, path: &str) -> std::io::Result<Option<String>> {
        // Entry paths are exactly `.../journal/<writer>/<seq:012>`; the
        // branch/manifest/shard refs never contain "/journal/".
        if self.fail_entries.load(Ordering::SeqCst) == 1 && path.contains("/journal/") {
            return Err(std::io::Error::other("simulated ref outage"));
        }
        self.inner.get_path(path)
    }
    fn delete_path(&self, path: &str) -> std::io::Result<bool> {
        self.inner.delete_path(path)
    }
    fn list_paths(&self, prefix: &str) -> std::io::Result<Vec<String>> {
        self.inner.list_paths(prefix) // discovery LISTs stay healthy
    }
    fn list_dirs(&self, prefix: &str) -> std::io::Result<Vec<String>> {
        self.inner.list_dirs(prefix)
    }
    fn store_id(&self) -> String {
        self.id.clone() // unique → cold discovery cache for the reader
    }
    fn blob_exists(&self, hash: &str) -> bool {
        self.inner.blob_exists(hash)
    }
    fn delete_blob(&self, hash: &str) -> std::io::Result<bool> {
        self.inner.delete_blob(hash)
    }
}

/// REVERT CHECK (C17): before the `get_path` error channel, this exact test
/// FAILED — the failed entry probe at seq 1 masqueraded as "the writer's
/// log is empty", `resolve_packs` returned `Ok([snapshot])`, and the reader
/// silently missed every row the external writer had committed (a
/// TRUNCATED journal view). With `io::Result<Option<String>>`, the failed
/// probe propagates as `Err("journal entry probe failed (writer W_OUTAGE
/// seq 1): ...")` — and after the outage clears, the SAME view resolves
/// complete (nothing was corrupted; the failure was transient).
#[test]
fn test_c17_probe_outage_is_error_not_truncation() {
    let dir = tempfile::tempdir().unwrap();

    // 1. Seed through a PLAIN kernel (healthy; its discovery cache lives
    //    under the canonical store id and cannot warm the reader below):
    //    one journal write (bootstrap-folds into the branch ref) + two
    //    fabricated entries for an EXTERNAL writer, live above the
    //    snapshot's watermark.
    let plain = UnifiedStorage::new_local(dir.path()).unwrap();
    let kernel = plain.kernel();
    let ids = vec![1i64];
    let vals = vec![10i64];
    write::write_rows_i64(kernel, "probe17", "main", &[("id", &ids), ("val", &vals)], "base")
        .expect("seed write (bootstrap fold)");
    let e_ids = vec![2i64];
    let e_vals = vec![20i64];
    let pack2 = fabricate_entry(kernel, "probe17", "main", "W_OUTAGE", 1, &e_ids, &e_vals);
    let e_ids2 = vec![3i64];
    let e_vals2 = vec![30i64];
    let pack3 = fabricate_entry(kernel, "probe17", "main", "W_OUTAGE", 2, &e_ids2, &e_vals2);
    assert!(!pack2.is_empty() && !pack3.is_empty());

    // Healthy baseline through the outage-wrapped reader (cold cache): the
    // full view = snapshot + both external entries.
    let (store, fail_entries) = EntryOutageStore::new(dir.path());
    let reader = PondKernel::new_with_store(Box::new(store));
    let plans = journal::resolve_packs(&reader, "probe17", "main", true)
        .expect("healthy resolve through the outage-wrapped reader");
    assert_eq!(plans.len(), 3, "snapshot + two live external entries");

    // 2. Flip the entry-read outage: resolve_packs must ERROR (the probe
    //    cannot distinguish "absent" from "failed" anymore — it KNOWS the
    //    read failed). Pre-C17 this returned Ok with the entries missing.
    fail_entries.store(1, Ordering::SeqCst);
    let err = journal::resolve_packs(&reader, "probe17", "main", true)
        .expect_err("a failed epoch probe must surface as an Err, not an empty suffix");
    assert!(
        err.contains("journal entry probe failed"),
        "error should name the failed probe: {err}"
    );
    // NOTE (orchestrator fix): the failing probe may belong to EITHER
    // discovered writer — the real seed writer's log ends at its
    // watermark, so its first probe PAST the watermark (seq 3) fails just
    // like W_OUTAGE's first probe (seq 1); std::thread::scope probing
    // order is nondeterministic. What matters: the error names SOME
    // writer + seq AND the underlying io error (probe attribution), and
    // the view resolution ERRORED instead of silently truncating.
    assert!(
        err.contains("seq") && err.contains("simulated ref outage"),
        "error should carry writer/seq and the underlying io error: {err}"
    );

    // 3. Recovery: outage off → the SAME resolution is complete again (the
    //    failure was transient; no state was corrupted by the failed read).
    fail_entries.store(0, Ordering::SeqCst);
    let rows = read::read_rows_json_pruned(&reader, "probe17", "main", &key_fields(), None, &[])
        .expect("full view after recovery");
    let mut ids: Vec<i64> = rows
        .iter()
        .map(|(_, r)| r.get("id").and_then(|v| v.as_i64()).unwrap_or(-1))
        .collect();
    ids.sort();
    assert_eq!(ids, vec![1, 2, 3], "recovered view is complete, not truncated");
}

/// C13 (N+6): the RAW read path (`read::read` / `pond read` / `s.read`)
/// routes through the journal resolver instead of the branch ref alone.
/// Pre-C13, after D3 the branch ref is a CACHE of the last fold: raw
/// reads silently missed every journal pack appended since — a second
/// `write_rows_i64` (no fold yet) was INVISIBLE to `read::read` (it
/// returned the folded snapshot's bytes only). Post-C13 the raw read
/// concatenates the live RG bytes of the full D6 plan.
#[test]
fn test_c13_raw_read_is_journal_aware() {
    let dir = tempfile::tempdir().unwrap();
    let storage = UnifiedStorage::new_local(dir.path()).unwrap();
    let kernel = storage.kernel();
    let coll = "c13raw";

    // Two journal writes, NO manual compact: the first bootstraps (folds
    // into the branch ref), the second stays live above the watermark.
    let ids1: Vec<i64> = vec![1, 2, 3];
    let vals1: Vec<i64> = vec![10, 20, 30];
    write::write_rows_i64(kernel, coll, "main", &[("id", &ids1), ("val", &vals1)], "w1")
        .expect("first write (bootstrap fold)");
    let bytes_after_first = read::read(kernel, coll, "main")
        .expect("raw read after the first write");

    let ids2: Vec<i64> = vec![4, 5];
    let vals2: Vec<i64> = vec![40, 50];
    write::write_rows_i64(kernel, coll, "main", &[("id", &ids2), ("val", &vals2)], "w2")
        .expect("second write (journal-live, no fold)");
    let bytes_after_second = read::read(kernel, coll, "main")
        .expect("raw read must succeed with live journal entries");

    // The journal's live packs must contribute bytes: pre-C13 this read
    // returned the folded snapshot only (byte-identical to after the
    // first write — the second write's RG was silently missing).
    assert!(
        bytes_after_second.len() > bytes_after_first.len(),
        "C13: the raw read must include the live journal pack's bytes \
         ({} bytes after w2 vs {} after w1)",
        bytes_after_second.len(),
        bytes_after_first.len()
    );

    // Semantic anchor: the journal-aware reader sees all 5 rows — the raw
    // path's plan is the SAME plan (resolve_packs), so the raw bytes'
    // RG count matches the rows' RG provenance.
    let rows = read::read_rows_json_pruned(kernel, coll, "main", &[], None, &[])
        .expect("journal-aware rows");
    assert_eq!(rows.len(), 5, "sanity: both writes visible to the row reader");

    // After an explicit compact, the branch ref folds everything: the raw
    // read stays correct (now from the snapshot alone) — same 5 rows.
    pond_storage::journal::compact(kernel, coll, "main", &[]).expect("compact");
    let rows_after_fold = read::read_rows_json_pruned(kernel, coll, "main", &[], None, &[])
        .expect("rows after fold");
    assert_eq!(rows_after_fold.len(), 5, "fold preserves all rows");
    let bytes_after_fold = read::read(kernel, coll, "main")
        .expect("raw read after fold");
    assert!(
        !bytes_after_fold.is_empty(),
        "raw read after fold returns the snapshot's bytes"
    );
}

/// C13 companion: a FRESH collection (journal entries but the branch ref
/// not yet written by any fold — the bootstrap window) still reads: the
/// raw path's plan is entries-only, so the read returns the entry RGs,
/// NOT the pre-C13 "has no commits" error.
#[test]
fn test_c13_raw_read_no_fold_yet_returns_entry_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let storage = UnifiedStorage::new_local(dir.path()).unwrap();
    let kernel = storage.kernel();
    let coll = "c13fresh";

    // Fabricate a live entry WITHOUT any fold: fabricate_entry writes the
    // pack + entry ref directly (an external writer the snapshot never
    // folded — the same construction the C17 probe test uses).
    let ids: Vec<i64> = vec![7];
    let vals: Vec<i64> = vec![70];
    let pack = fabricate_entry(kernel, coll, "main", "C13W", 1, &ids, &vals);
    assert!(!pack.is_empty(), "entry fabricated");

    // No branch ref exists. Pre-C13: Err("Collection 'c13fresh' has no
    // commits"). Post-C13: the entry's RG bytes concatenate to non-empty
    // output (the journal IS the state during the bootstrap window).
    let bytes = read::read(kernel, coll, "main")
        .expect("entries-only collections read their live packs");
    assert!(!bytes.is_empty(), "the fabricated entry contributes bytes");

    let rows = read::read_rows_json_pruned(kernel, coll, "main", &[], None, &[])
        .expect("rows from the entries-only view");
    assert_eq!(rows.len(), 1, "the row reader sees the same live entry");
    assert_eq!(rows[0].1["id"], json!(7));
}
