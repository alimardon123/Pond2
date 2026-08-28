// Journal module — D3 no-CAS per-writer journal (ARCHITECTURE.md D3)
//
// THE CONCURRENCY ARCHITECTURE (settled): per-writer immutable journal,
// benign snapshot cache. NO CAS anywhere in this path.
//
//   Writes append, never overwrite:
//     collections/<c>/_branches/<b>/journal/<writer_id>/<seq:012>
//   is a UNIQUE path per (writer, seq) — a plain `reference()` (put_path)
//   at a unique key always succeeds, on every backend, with zero retries.
//   `writer_id` is a fresh UUIDv7 per writer instance (process boot per
//   (store, collection, branch)); `seq` is the writer's OWN local counter.
//   No coordination, no contended object, no lost updates possible — the
//   C9 P0 (every commit after the first silently hid prior commits because
//   manifests never accumulated and readers resolved only HEAD) is closed
//   by construction: readers union the SNAPSHOT with every live entry.
//
//   The pack's commit JSON carries journal metadata:
//     data entries:     journal: {writer, seq}
//     compaction snaps: journal: {writer, seq, upto: {writer → max_seq_folded}}
//   The invariant: *a pack + probes above its `upto` = complete state*.
//
//   Reads = snapshot ∪ live entries (see read::read_rows_json_pruned):
//     branch_ref (a CACHE of the last folded snapshot, not a serialization
//     point) → per-writer epoch probes from max(snapshot.upto, seen)+1
//     (parallel GETs at computable paths; first miss ends that writer's
//     log; positive hits are immutable content-addressed packs) → the ONE
//     pruned pipeline per pack → CRDT-merge (LWW by _version, total
//     tiebreak (_version, _rowid, payload) — C10 — tombstones suppress).
//
//   Compaction folds and advances the cache: one manifest-level union pack
//   (O(metadata) — pack headers only, never data blobs), appended to the
//   compactor's own log FIRST, then the branch ref is LWW-updated. The
//   race is BENIGN: every ref value is a valid folded state; a losing
//   compactor's pack simply survives as a live journal entry that readers
//     union in, and readers of a superseded snapshot probe above its upto
//     and find the newer folded packs. Correctness comes from CRDT merge
//     + unique paths, not serialization.
//
//   Env knobs (read once, cached):
//     POND_JOURNAL_TTL_MS      — discovery cache TTL, default 1000; 0 = always fresh LIST
//     POND_JOURNAL_AUTO_COMPACT — live-entry threshold, default 32; 0 = disabled

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::{Duration, Instant};

use pond_kernel::PondKernel;
use pond_kernel::crdt::uuidv7;
use serde_json::{json, Value};

use crate::branch_ref;
use crate::commit;
use crate::pond_pack;
use crate::shard;

// ---------------------------------------------------------------------------
// Path helpers
// ---------------------------------------------------------------------------

/// Journal prefix for a branch: `collections/<c>/_branches/<b>/journal/`.
pub fn journal_prefix(collection: &str, branch: &str) -> String {
    format!("collections/{}/_branches/{}/journal/", collection, branch)
}

/// One journal entry's pointer path:
/// `collections/<c>/_branches/<b>/journal/<writer_id>/<seq:012>`.
///
/// The seq is zero-padded to 12 digits so lexicographic order == numeric
/// order (S3 LIST is lexical) and so the same path is computable from
/// (writer, seq) alone — that is what makes epoch PROBING possible:
/// readers never list entries, they GET at computed paths.
pub fn entry_path(collection: &str, branch: &str, writer_id: &str, seq: u64) -> String {
    format!("{}{}/{:012}", journal_prefix(collection, branch), writer_id, seq)
}

// ---------------------------------------------------------------------------
// Env knobs (parsed once per process)
// ---------------------------------------------------------------------------

/// Discovery-cache TTL in milliseconds (POND_JOURNAL_TTL_MS, default 1000).
/// 0 means "always fresh" — every journal-resolving read performs a LIST.
fn discovery_ttl_ms() -> u64 {
    static TTL: OnceLock<u64> = OnceLock::new();
    *TTL.get_or_init(|| {
        std::env::var("POND_JOURNAL_TTL_MS")
            .ok()
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(1000)
    })
}

/// Auto-compaction threshold (POND_JOURNAL_AUTO_COMPACT, default 32).
/// A writer folds its own log (plus everything else live) once
/// `seq - last_fold_seq >= threshold`. 0 disables auto-compaction.
fn auto_compact_threshold() -> u64 {
    static THRESHOLD: OnceLock<u64> = OnceLock::new();
    *THRESHOLD.get_or_init(|| {
        std::env::var("POND_JOURNAL_AUTO_COMPACT")
            .ok()
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(32)
    })
}

// ---------------------------------------------------------------------------
// JournalWriter + process-local registry
// ---------------------------------------------------------------------------

/// A journal writer's local state: its immutable identity and its own
/// sequential counter. One instance per (store, collection, branch) per
/// PROCESS (see [`writer_for`]).
pub struct JournalWriter {
    /// Fresh UUIDv7 per writer instance. Two processes writing the same
    /// branch get different ids — that is what makes their entry paths
    /// unique and their logs independently probeable.
    pub writer_id: String,
    /// The writer's own local counter; the next append uses this seq.
    pub next_seq: u64,
    /// The seq at (the last) compaction — auto-compaction fires when
    /// `next_seq - 1 - last_fold_seq >= threshold`.
    pub last_fold_seq: u64,
}

impl JournalWriter {
    /// A brand-new writer: fresh identity, empty log (seq starts at 1).
    /// A fresh writer_id per process start means its log starts empty at
    /// seq 1 — no need to probe the store for a resume point.
    pub fn new() -> Self {
        Self {
            writer_id: uuidv7(),
            next_seq: 1,
            last_fold_seq: 0,
        }
    }
}

impl Default for JournalWriter {
    fn default() -> Self {
        Self::new()
    }
}

type RegistryKey = (String, String, String); // (store_id, collection, branch)

fn registry() -> &'static Mutex<HashMap<RegistryKey, Arc<Mutex<JournalWriter>>>> {
    static REGISTRY: OnceLock<Mutex<HashMap<RegistryKey, Arc<Mutex<JournalWriter>>>>> =
        OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Look up (or create) the journal writer for (store, collection, branch).
///
/// The registry lives for the PROCESS lifetime: every write path on the
/// same (store, collection, branch) — including free functions like
/// `write_rows_i64` that only receive `&PondKernel` — shares ONE writer,
/// so the seq counter is monotonic and appends are serialized by the
/// writer's Mutex. Creating a fresh JournalWriter per call would restart
/// seq at 1 and collide with this process's earlier entries; the registry
/// is what keeps the invariant "entries within a writer log are strictly
/// sequential" (probes rely on it: a miss at seq N means the log ended).
///
/// Keyed by the store's stable identity (`ObjectStore::store_id`): two
/// kernels over the SAME backing store share the writer; kernels over
/// different stores stay isolated.
pub fn writer_for(
    kernel: &PondKernel,
    collection: &str,
    branch: &str,
) -> Arc<Mutex<JournalWriter>> {
    let key: RegistryKey = (kernel.store_id(), collection.to_string(), branch.to_string());
    let mut reg = registry().lock().unwrap();
    reg.entry(key).or_insert_with(|| Arc::new(Mutex::new(JournalWriter::new()))).clone()
}

// ---------------------------------------------------------------------------
// Discovery cache — TTL-bounded writer discovery (C2 fix)
// ---------------------------------------------------------------------------

/// The discovered state of one journal prefix: the writer set (one-level
/// LIST result) plus the live ENTRIES each writer's last probe observed.
///
/// Remembering the entries themselves — not just a `seen` watermark — is
/// what makes the warm path CORRECT as well as cheap: a watermark alone
/// would make the second read within the TTL probe from `seen+1`, find
/// nothing, and silently drop the entries the first read consumed (the
/// read would return only the snapshot). Entries are IMMUTABLE, so a
/// remembered (seq → pack hash) pair stays valid forever; the read path
/// drops entries at/below the current snapshot's `upto` watermark (those
/// were folded into the snapshot by compaction and deleted — re-reading
/// them would double-count rows).
///
/// Only the WRITER SET changes over time (a new writer process appearing),
/// and that is what the TTL-bounded re-LIST catches.
type WriterLog = BTreeMap<u64, String>;
type LiveEntries = BTreeMap<String, WriterLog>;
struct Discovered {
    writers: BTreeSet<String>,
    /// writer → (seq → pack hash), live at the last probe/append.
    entries: LiveEntries,
    refreshed_at: Instant,
}

fn discovery_cache(
) -> &'static RwLock<HashMap<(String, String), Discovered>> {
    static DISCOVERY: OnceLock<RwLock<HashMap<(String, String), Discovered>>> = OnceLock::new();
    DISCOVERY.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Is a cached discovery still within the TTL? (TTL 0 = EXACT FRESHNESS —
/// nothing is ever fresh, so every resolution re-LISTs; see
/// `POND_JOURNAL_TTL_MS`.)
fn discovery_is_fresh(d: &Discovered) -> bool {
    let ttl = Duration::from_millis(discovery_ttl_ms());
    !ttl.is_zero() && d.refreshed_at.elapsed() < ttl
}

/// Discover the writer set + remembered live entries for a journal prefix,
/// through the TTL cache.
///
/// `force_refresh` bypasses the TTL (compaction and status need the exact
/// current state; they are rare maintenance operations).
///
/// WARM-PATH BUDGET (ACCEPTANCE.md #3): with a fresh cache a
/// journal-resolving read performs ZERO uncacheable LISTs — only the
/// branch_ref GET plus W parallel epoch probes (1 get_path miss per
/// writer). The TTL (default 1s) bounds how long a NEW writer can stay
/// invisible; `POND_JOURNAL_TTL_MS=0` gives exact freshness.
fn discover(
    kernel: &PondKernel,
    prefix: &str,
    force_refresh: bool,
) -> Result<(BTreeSet<String>, LiveEntries), String> {
    let key = (kernel.store_id(), prefix.to_string());

    if !force_refresh {
        let cache = discovery_cache().read().unwrap();
        if let Some(d) = cache.get(&key) {
            if discovery_is_fresh(d) {
                return Ok((d.writers.clone(), d.entries.clone()));
            }
        }
    }

    // Fresh one-level LIST — O(writers), never O(entries): the backend
    // maps this to a delimiter LIST (S3/R2) or read_dir (localfs).
    let writers: BTreeSet<String> = kernel
        .list_dirs(prefix)
        .map_err(|e| format!("journal discovery LIST failed for '{}': {}", prefix, e))?
        .into_iter()
        .collect();

    // Keep the remembered entries of writers that still exist — entries
    // are immutable, so they are still valid. Writers that vanished
    // (GC'd logs after another process compacted) are dropped entirely.
    let mut entries: LiveEntries = BTreeMap::new();
    {
        let cache = discovery_cache().read().unwrap();
        if let Some(d) = cache.get(&key) {
            for (w, ents) in &d.entries {
                if writers.contains(w) {
                    entries.insert(w.clone(), ents.clone());
                }
            }
        }
    }

    {
        let mut cache = discovery_cache().write().unwrap();
        cache.insert(
            key,
            Discovered {
                writers: writers.clone(),
                entries: entries.clone(),
                refreshed_at: Instant::now(),
            },
        );
    }

    Ok((writers, entries))
}

/// Record that this process appended an entry to its own log — own writes
/// become visible to own reads INSTANTLY (no TTL wait for the LIST to
/// notice our own writer dir).
///
/// Only an EXISTING cache entry is updated: if none exists, the reader's
/// first LIST discovers our writer dir anyway; pre-creating an entry here
/// with only the own writer would DELAY the first real discovery.
fn note_own_append(
    kernel: &PondKernel,
    prefix: &str,
    writer_id: &str,
    seq: u64,
    pack_hash: &str,
) {
    let key = (kernel.store_id(), prefix.to_string());
    let mut cache = discovery_cache().write().unwrap();
    if let Some(d) = cache.get_mut(&key) {
        d.writers.insert(writer_id.to_string());
        d.entries
            .entry(writer_id.to_string())
            .or_default()
            .insert(seq, pack_hash.to_string());
    }
}

/// Replace the cached live-entry set for a prefix (after a full
/// resolution). Entries at/below the snapshot's `upto` are already dropped
/// by the caller — folded data is never re-read.
fn note_live_entries(
    kernel: &PondKernel,
    prefix: &str,
    writers: &BTreeSet<String>,
    live: &LiveEntries,
) {
    let key = (kernel.store_id(), prefix.to_string());
    let mut cache = discovery_cache().write().unwrap();
    match cache.get_mut(&key) {
        Some(d) => {
            d.entries = live.clone();
            d.writers = writers.clone();
        }
        None => {
            cache.insert(
                key,
                Discovered {
                    writers: writers.clone(),
                    entries: live.clone(),
                    refreshed_at: Instant::now(),
                },
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Journal view resolution — snapshot ∪ live entries
// ---------------------------------------------------------------------------

/// One live journal entry: a pointer to an immutable PNPK pack.
#[derive(Debug, Clone)]
pub struct JournalEntry {
    pub writer: String,
    pub seq: u64,
    pub pack_hash: String,
}

/// The resolved journal view of a branch: the folded snapshot (branch_ref)
/// plus every live journal entry above its watermarks.
#[derive(Debug, Clone, Default)]
pub struct JournalView {
    /// The branch_ref pack hash — the last FOLDED snapshot (a cache, not
    /// a serialization point). None on collections that never compacted
    /// (or were never written through `write()`).
    pub snapshot: Option<String>,
    /// The snapshot's `journal.upto` map — everything ≤ upto[w] for writer
    /// w is already folded into the snapshot manifest. Empty for legacy
    /// packs written before the journal existed.
    pub snapshot_upto: BTreeMap<String, u64>,
    /// Live entries, sorted by (writer, seq) — deterministic order.
    pub entries: Vec<JournalEntry>,
}

/// Read the `journal.upto` map out of a snapshot pack's commit JSON.
///
/// Handles BOTH pack forms (PNPK compaction packs) and PLAIN JSON commits
/// (the raw `write()` base-snapshot path and merge commits): any commit
/// that carries `journal.upto` states its watermark. Without this, a
/// plain-commit snapshot would read as upto={} — and after a fold deleted
/// a writer's early entries, probes from seq 1 would die at the first gap
/// and NEVER see the writer's live tail (tribunal F1: raw `write()`
/// permanently blinded the journal; verified empirically — a fresh process
/// read 0 rows for 10 committed).
///
/// Lenient by design: a legacy snapshot (unparseable blob, missing journal
/// field) folds NOTHING — readers then probe every discovered writer from
/// seq 1, which is always CORRECT for pre-fold repos (no entries were ever
/// deleted), merely less efficient.
pub(crate) fn read_snapshot_upto(kernel: &PondKernel, snapshot_hash: &str) -> BTreeMap<String, u64> {
    let data = match kernel.read_blob(snapshot_hash) {
        Ok(d) => d,
        Err(_) => return BTreeMap::new(),
    };
    let commit_obj: Value = if pond_pack::is_pack(&data) {
        match pond_pack::decode_pack(&data) {
            Some((commit_obj, _, _)) => commit_obj,
            None => return BTreeMap::new(),
        }
    } else {
        match serde_json::from_slice(&data) {
            Ok(v) => v,
            Err(_) => return BTreeMap::new(),
        }
    };
    let Some(upto) = commit_obj.get("journal").and_then(|j| j.get("upto")) else {
        return BTreeMap::new();
    };
    let Some(map) = upto.as_object() else {
        return BTreeMap::new();
    };
    let mut out = BTreeMap::new();
    for (writer, seq) in map {
        if let Some(seq) = seq.as_u64() {
            out.insert(writer.clone(), seq);
        }
    }
    out
}

/// Probe one writer's log forward from `start` until the first miss.
///
/// Entries within a writer log are strictly sequential (the writer's own
/// counter, appended under its registry Mutex), so the first miss is the
/// log's current end — there is never a gap to re-scan.
fn probe_writer(
    kernel: &PondKernel,
    collection: &str,
    branch: &str,
    writer: &str,
    start: u64,
) -> Vec<JournalEntry> {
    let mut entries = Vec::new();
    let mut seq = start;
    while let Some(pack_hash) = kernel.resolve(&entry_path(collection, branch, writer, seq)) {
        entries.push(JournalEntry {
            writer: writer.to_string(),
            seq,
            pack_hash,
        });
        seq += 1;
    }
    entries
}

/// Resolve the full journal view: snapshot pack + live entries.
///
/// Probes run IN PARALLEL across writers (std::thread::scope — same
/// pattern as the bloom pre-check in read.rs) and sequentially within a
/// writer (epoch probing: seq, seq+1, ... until first miss). Remembered
/// entries from the discovery cache (see [`Discovered`]) are unioned in —
/// filtered to those ABOVE the snapshot's `upto` watermark — so a warm
/// read loses nothing while probing only for NEW entries.
///
/// STALENESS IS BOUNDED AND CONSISTENT: a reader may miss an entry
/// appended between its discovery LIST and its probes (or folded by a
/// concurrent compaction mid-probe), but it always observes a PREFIX of
/// each writer's log over a valid snapshot — a consistent past state,
/// never a torn one. The next read (post-TTL) sees the rest.
pub fn resolve_view(
    kernel: &PondKernel,
    collection: &str,
    branch: &str,
    force_refresh: bool,
) -> Result<JournalView, String> {
    let prefix = journal_prefix(collection, branch);

    // 1. Snapshot base: the branch ref. None on a fresh collection →
    //    empty base (the caller treats "no snapshot, no entries" as empty).
    let snapshot = kernel.resolve(&branch_ref(collection, branch));
    let snapshot_upto = snapshot
        .as_ref()
        .map(|h| read_snapshot_upto(kernel, h))
        .unwrap_or_default();

    // 2. Writer discovery (TTL-cached one-level LIST + remembered entries).
    let (writers, remembered) = discover(kernel, &prefix, force_refresh)?;

    // 3. Probe each writer forward from max(snapshot_upto, seen) + 1.
    //    The snapshot's upto is a FLOOR for the watermark: entries ≤ upto
    //    are folded into the snapshot, so probing them would only re-read
    //    packs the snapshot already contains. `seen` is the highest
    //    remembered seq — entries ≤ seen are already in `remembered`.
    let seen_of = |w: &str| {
        remembered
            .get(w)
            .and_then(|ents| ents.keys().next_back().copied())
            .unwrap_or(0)
    };
    let mut probed: Vec<JournalEntry> = Vec::new();
    let probe_error: Option<String> = std::thread::scope(|s| {
        let handles: Vec<_> = writers
            .iter()
            .map(|w| {
                let start = snapshot_upto
                    .get(w)
                    .copied()
                    .unwrap_or(0)
                    .max(seen_of(w))
                    + 1;
                s.spawn(move || probe_writer(kernel, collection, branch, w, start))
            })
            .collect();
        for h in handles {
            match h.join() {
                Ok(entries) => probed.extend(entries),
                Err(_) => return Some("journal probe thread panicked".to_string()),
            }
        }
        None
    });
    if let Some(e) = probe_error {
        return Err(e);
    }

    // 4. Live set = remembered entries above the snapshot's upto ∪ probed
    //    entries (all probed entries are > upto by construction). Entries
    //    ≤ upto were folded into the snapshot by a compaction and deleted
    //    — re-reading them would double-count rows against the snapshot.
    let mut live: LiveEntries = BTreeMap::new();
    for (w, ents) in &remembered {
        let upto = snapshot_upto.get(w).copied().unwrap_or(0);
        let kept: WriterLog = ents
            .range((upto.saturating_add(1))..)
            .map(|(seq, hash)| (*seq, hash.clone()))
            .collect();
        if !kept.is_empty() {
            live.insert(w.clone(), kept);
        }
    }
    for e in probed {
        live
            .entry(e.writer.clone())
            .or_default()
            .insert(e.seq, e.pack_hash.clone());
    }

    // 4.5 TRIBUNAL F2 — drop fully-covered COMPACT entries.
    //
    // When two compactors race, the loser's fold pack stays live in its
    // writer's log (the winner's upto never covered it). Both packs fold
    // overlapping RG sets, and the non-CRDT readers (read_rows_i64,
    // read_all_row_groups, lakehouse/vector lenses) CONCATENATE packs —
    // every shared RG appeared twice (verified empirically by the
    // tribunal: 10 rows read for 5 logical rows).
    //
    // Fix: a live entry whose pack is a COMPACTION snapshot (journal.upto
    // present) is SKIPPED when every RG blob hash in its manifest is
    // already covered by the snapshot's RGs ∪ the DATA entries' RGs ∪
    // previously-kept compact RGs. The common race (both compactors fold
    // the same view) is fully covered → skipped → no duplication. A
    // partial-overlap compact pack (the winner missed entries the loser
    // folded) is kept whole — its shared RGs still duplicate for the
    // concatenating readers; that residual is C11 in CRITIQUE.md (needs
    // RG-level plan filtering, next cycle). CRDT readers are unaffected
    // either way (duplicate rows collapse by _rowid).
    //
    // Cost: entry packs are read here to classify them — the same packs
    // the readers fetch right after, so content caching makes this free
    // after the first resolve.
    if snapshot.is_some() && !live.is_empty() {
        let all_hashes: Vec<String> = live.values()
            .flat_map(|log| log.values().cloned())
            .collect();
        let kinds = classify_packs(kernel, &all_hashes);
        let any_compact = kinds.values().any(|k| *k);
        if any_compact {
            // Covered set: snapshot RGs + DATA entry RGs.
            let mut covered: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
            if let Some(snap) = &snapshot {
                collect_rg_hashes(kernel, snap, &mut covered);
            }
            for (hash, is_compact) in &kinds {
                if !is_compact {
                    collect_rg_hashes(kernel, hash, &mut covered);
                }
            }
            // Drop compact entries whose RGs are fully covered; keep their
            // novel hashes covered for the next compact in line.
            let mut filtered: LiveEntries = BTreeMap::new();
            for (w, log) in &live {
                let mut kept_log: WriterLog = BTreeMap::new();
                for (seq, hash) in log {
                    let is_compact = kinds.get(hash).copied().unwrap_or(false);
                    if is_compact {
                        let mut rgs = std::collections::BTreeSet::new();
                        collect_rg_hashes(kernel, hash, &mut rgs);
                        if rgs.iter().all(|h| covered.contains(h)) {
                            continue; // fully covered — skip
                        }
                        covered.extend(rgs);
                    }
                    kept_log.insert(*seq, hash.clone());
                }
                if !kept_log.is_empty() {
                    filtered.insert(w.clone(), kept_log);
                }
            }
            live = filtered;
        }
    }

    // 5. Persist the new live set (entries ≤ upto are dropped here too —
    //    folded data is never re-read or re-probed).
    note_live_entries(kernel, &prefix, &writers, &live);

    // 6. Deterministic entry order: (writer, seq) — BTreeMap nesting
    //    already yields it. The CRDT merge is order-independent after the
    //    C10 total tiebreak, but a fixed order keeps output row ordering
    //    stable across reads.
    let entries: Vec<JournalEntry> = live
        .into_iter()
        .flat_map(|(writer, ents)| {
            ents.into_iter().map(move |(seq, pack_hash)| JournalEntry {
                writer: writer.clone(),
                seq,
                pack_hash,
            })
        })
        .collect();

    Ok(JournalView {
        snapshot,
        snapshot_upto,
        entries,
    })
}

// ---------------------------------------------------------------------------
// Appends — unique-path PUTs, zero retries by construction
// ---------------------------------------------------------------------------

/// Append one pack to a SPECIFIC writer's log (the caller holds the
/// writer's Mutex — see [`append_pack`] for the registry-based entry point).
///
/// SAFETY ARGUMENT (why no CAS): the entry path is
/// `journal/<writer_id>/<seq:012>` — unique per (writer, seq). A plain
/// `reference()` (PUT) at a unique key cannot lose a race, cannot
/// overwrite another writer's data, and cannot be overwritten itself
/// (the writer's registry Mutex serializes this process's appends, and
/// other processes have different writer_ids). It always succeeds, on
/// localfs and S3/R2 identically. `put_path_if` has ZERO callers here.
///
/// The append is JOURNAL-METADATA-BEARING: the commit JSON gains
/// `journal: {writer, seq}` (and `upto` for compaction snapshots), so a
/// pack discovered later can always say who wrote it and where it sits in
/// the log — and, for snapshots, what the pack already folds.
fn append_pack_with_writer(
    kernel: &PondKernel,
    collection: &str,
    branch: &str,
    writer: &mut JournalWriter,
    commit_obj: &mut Value,
    manifest_bytes: &[u8],
    upto: Option<&BTreeMap<String, u64>>,
) -> Result<(String, u64), String> {
    let seq = writer.next_seq;
    writer.next_seq += 1;

    // Stamp journal metadata into the commit JSON (the pack is built
    // BELOW, so the stamped fields are always in the stored bytes).
    //
    // A compaction snapshot's `upto` ALWAYS includes the snapshot's OWN
    // entry seq: the pack IS the complete folded state through that seq,
    // so probes above its `upto` must start PAST it (the D3 invariant
    // "a pack + probes above its upto = complete state" would be
    // violated otherwise — readers would re-read the snapshot pack as a
    // live entry and double its rows in every non-CRDT-merging path).
    let mut journal_meta = json!({"writer": writer.writer_id, "seq": seq});
    if let Some(upto) = upto {
        let mut upto = upto.clone();
        upto.insert(writer.writer_id.clone(), upto
            .get(&writer.writer_id)
            .copied()
            .unwrap_or(0)
            .max(seq));
        journal_meta["upto"] = json!(upto);
    }
    commit_obj["journal"] = journal_meta;

    let pack_bytes = pond_pack::encode_pack(commit_obj, manifest_bytes, None);
    let pack_hash = kernel
        .write(&pack_bytes)
        .map_err(|e| format!("journal append: failed to write pack blob: {}", e))?;

    // THE append: one plain PUT at a unique path. Always succeeds.
    let path = entry_path(collection, branch, &writer.writer_id, seq);
    kernel
        .reference(&path, &pack_hash)
        .map_err(|e| format!("journal append: failed to write entry {}: {}", path, e))?;

    // Own writes visible to own reads instantly (TTL cache update).
    note_own_append(
        kernel,
        &journal_prefix(collection, branch),
        &writer.writer_id,
        seq,
        &pack_hash,
    );

    Ok((pack_hash, seq))
}

/// Append one pack through the process's registry writer for
/// (store, collection, branch), with auto-compaction.
///
/// `key_fields` is only used by auto-compaction (it becomes the CRDT
/// merge key when shards get folded); pass `&[]` when there is no key.
///
/// Auto-compaction (POND_JOURNAL_AUTO_COMPACT, default 32, 0 = off): once
/// the writer's own log has grown `threshold` entries past the last fold,
/// `compact` runs synchronously — bounded live-entry counts keep warm
/// reads at O(1) probes per writer instead of O(entries). The check runs
/// OUTSIDE the writer Mutex (compact appends through the same registry).
pub fn append_pack(
    kernel: &PondKernel,
    collection: &str,
    branch: &str,
    commit_obj: &mut Value,
    manifest_bytes: &[u8],
    key_fields: &[String],
) -> Result<(String, u64), String> {
    let writer_arc = writer_for(kernel, collection, branch);
    let (pack_hash, seq) = {
        let mut writer = writer_arc.lock().unwrap();
        append_pack_with_writer(
            kernel, collection, branch, &mut writer,
            commit_obj, manifest_bytes, None,
        )?
    };

    let threshold = auto_compact_threshold();
    if threshold > 0 {
        // Bootstrap-fold: a collection with NO snapshot cache yet (branch
        // ref unresolved — fresh collection, or written only by a process
        // that had auto-compaction disabled) gets folded IMMEDIATELY, so
        // the snapshot cache exists from commit #1. Rationale: a large
        // share of existing surfaces resolve branch_ref directly as their
        // "does this collection exist" gate (CLI read-rows, branch(),
        // merge(), the lens/index HEAD readers) — bootstrapping keeps them
        // all functional under the journal without touching them, while
        // journal-aware readers are complete either way (they union the
        // live entries above the snapshot's upto). The fold is just
        // `compact` — the sanctioned branch-ref writer — triggered by the
        // first write; concurrent first writers race benignly (every
        // branch_ref value is a valid folded state; see `compact`).
        // Respected by POND_JOURNAL_AUTO_COMPACT=0 like every auto-fold.
        //
        // DEVIATION NOTE (vs builder-spec §6, recorded in the worklog): the
        // spec's auto-compact rule is threshold-only; the bootstrap term is
        // added because plain journal writes never touch branch_ref, and
        // every branch_ref-gated surface (CLI read-rows gate, branch(),
        // merge()) would otherwise see journal-only collections as empty
        // until entry #32. The branch ref is still written ONLY by
        // `compact` (the sanctioned LWW writer) — the write paths
        // themselves touch zero shared objects.
        let bootstrap = kernel.resolve(&branch_ref(collection, branch)).is_none();
        let due = {
            let writer = writer_arc.lock().unwrap();
            // next_seq - 1 == the seq just appended; fold once the log has
            // grown `threshold` entries past the last fold (or on bootstrap).
            bootstrap || writer.next_seq > writer.last_fold_seq.saturating_add(threshold)
        };
        if due {
            compact(kernel, collection, branch, key_fields)?;
        }
    }

    Ok((pack_hash, seq))
}

// ---------------------------------------------------------------------------
// Compaction — fold the journal + shards into one snapshot
// ---------------------------------------------------------------------------

/// What one `compact` run folded.
#[derive(Debug, Clone)]
pub struct CompactStats {
    /// Live journal entries folded into the new snapshot.
    pub entries_folded: usize,
    /// Shards folded (and then cleared) into the new snapshot.
    pub shards_folded: usize,
    /// The new snapshot's pack hash (now the branch ref value).
    pub new_snapshot: String,
}

/// Fold the entire journal view (snapshot + live entries + shards) into
/// ONE new snapshot pack, advance the branch ref, and delete the folded
/// entry paths + shards.
///
/// MANIFEST-LEVEL FOLD: the union manifest concatenates the RG entries of
/// the snapshot pack and every live entry pack — O(metadata): pack
/// headers (commit + manifest bytes) are read, data blobs never are. No
/// row-level dedup is needed because the read path CRDT-merges anyway
/// (duplicate rows across packs collapse by _rowid). A row-level rewrite
/// compaction is a future cycle.
///
/// LOCK DISCIPLINE (the subtle part): the compactor holds its registry
/// writer's Mutex from BEFORE the view resolution until AFTER the folded
/// entry deletes. Reason: the new pack's `upto[own]` is bumped to the
/// pack's OWN seq (the pack is the folded state through that seq), and the
/// delete loop then removes entries `1..=upto[own]`. If another thread
/// could append to the SAME writer's log between the view resolution and
/// the pack append, that entry would land BELOW the pack's seq while never
/// having been folded — and the delete loop would erase live data (the
/// exact C9-class loss the journal exists to prevent). Holding the Mutex
/// freezes the compactor's log for the whole fold: every entry below the
/// pack's seq existed at resolve time, hence is in the fold. Appends on
/// OTHER writers' logs may interleave freely — their entries are either
/// below their folded watermark (deleted, folded) or above it (kept,
/// live), never in the gap.
///
/// BENIGN-RACE ARGUMENT (why plain `reference()` on the branch ref is
/// safe): every value ever written to branch_ref is a VALID FOLDED STATE
/// of some prefix of the journal. Two racing compactors C1/C2:
///   - C2 saw C1's pack as a live entry → C2's fold ⊇ C1's fold. Safe.
///   - C2 resolved before C1 appended → C2 folds the same older set;
///     C1's pack is content-identical in coverage (same base + same
///     entries), so nothing is lost when C2's LWW wins and C1's pack
///     stays orphaned-but-unread.
///   - LWW inversion (C2 appended later, C1's ref write lands last):
///     readers see C1's snapshot, probe above C1's upto, FIND C2's pack
///     as a live entry, and union it in. Complete state either way.
///
/// A reader racing a compaction similarly observes a consistent PREFIX
/// state (see resolve_view).
pub fn compact(
    kernel: &PondKernel,
    collection: &str,
    branch: &str,
    key_fields: &[String],
) -> Result<CompactStats, String> {
    // 0. Acquire the compactor's writer Mutex FIRST and hold it for the
    //    whole fold (see the LOCK DISCIPLINE note above).
    let writer_arc = writer_for(kernel, collection, branch);
    let mut writer = writer_arc.lock().unwrap();

    // 1. Exact current view (bypass the discovery TTL — compaction must
    //    fold everything that exists, not everything cached). Resolved
    //    while the log is frozen, so the fold covers every entry that the
    //    upcoming `upto[own] = own_seq` will claim.
    let view = resolve_view(kernel, collection, branch, true)?;
    if view.snapshot.is_none()
        && view.entries.is_empty()
        && shard::list_shards(kernel, collection, branch).is_empty()
    {
        // Nothing to fold — NOT an error: idempotent no-op compaction
        // (matches the old compact_shards(Ok(0)) convention so lens-layer
        // callers that compact unconditionally stay quiet). The CLI
        // surfaces this as "nothing to compact" via the empty
        // new_snapshot hash.
        return Ok(CompactStats {
            entries_folded: 0,
            shards_folded: 0,
            new_snapshot: String::new(),
        });
    }

    // 2. Union manifest: snapshot pack RGs + every live entry pack's RGs.
    let mut packs: Vec<String> = Vec::with_capacity(view.entries.len() + 1);
    if let Some(snap) = &view.snapshot {
        packs.push(snap.clone());
    }
    packs.extend(view.entries.iter().map(|e| e.pack_hash.clone()));

    let mut union_rgs: Vec<crate::manifest::RowGroupEntry> = Vec::new();
    let mut union_schema: Vec<(String, u8)> = Vec::new();
    let mut key_col = String::new();
    for pack_hash in &packs {
        let manifest_bytes = commit::resolve_manifest_bytes(kernel, pack_hash)
            .map_err(|e| format!("compaction: manifest resolve failed for {}: {}", pack_hash, e))?;
        let manifest = crate::read::resolve_manifest(kernel, &manifest_bytes, None)?;
        if union_schema.is_empty() {
            union_schema = manifest.columns.clone();
            key_col = manifest.key_col.clone();
        } else {
            // Union columns from later packs (first declaration wins the
            // type tag; RG blobs carry their own columns regardless).
            for (name, vtype) in manifest.columns {
                if !union_schema.iter().any(|(n, _)| *n == name) {
                    union_schema.push((name, vtype));
                }
            }
        }
        union_rgs.extend(manifest.row_groups);
    }
    if union_schema.is_empty() {
        if let Some(kf) = key_fields.first() {
            key_col = kf.clone();
        }
    }

    // 3. Fold shards (legacy CRDT layer — python lenses still write them).
    //    Shards are raw JSON row arrays, NOT packs with manifests, so they
    //    cannot join a manifest union as-is: their merged live rows are
    //    re-encoded as a PND2 row group (the write_rows encode machinery)
    //    and that RG joins the union manifest.
    let shards = shard::list_shards(kernel, collection, branch);
    let shards_folded = shards.len();
    let mut shard_rows: Vec<Value> = Vec::new();
    for (_name, hash) in &shards {
        let data = kernel
            .read_blob(hash)
            .map_err(|e| format!("compaction: shard read failed {}: {}", hash, e))?;
        let arr: Vec<Value> = serde_json::from_slice(&data)
            .map_err(|e| format!("compaction: shard parse failed {}: {}", hash, e))?;
        shard_rows.extend(arr);
    }
    if !shard_rows.is_empty() {
        // Tombstones are KEPT in the fold RG: the union manifest is
        // RG-level (no row dedup), so a folded tombstone must keep
        // suppressing its target row in the snapshot/entry RGs at read
        // time — dropping it here (filter_live_rows) would RESURRECT every
        // row a shard had deleted the moment the shards were cleared.
        // Deletion-as-data: the read path's merge + filter_live_rows does
        // the suppression, exactly as it does for live journal entries.
        let merged = shard::merge_rows_by_rowid(
            &shard_rows,
            key_fields.first().map(|s| s.as_str()).or(if key_col.is_empty() { None } else { Some(key_col.as_str()) }),
        );
        if !merged.is_empty() {
            let rg = build_rg_from_json_rows(kernel, &merged)?;
            union_rgs.push(rg);
        }
    }

    // Re-key RGs sequentially (their original keys only made sense inside
    // their source manifests).
    for (i, rg) in union_rgs.iter_mut().enumerate() {
        rg.key = format!("rg_{:010}", i);
    }

    // Absorb RG-LEVEL column names into the union schema: shard-fold RGs
    // (build_rg_from_json_rows) carry their own per-RG column lists — names
    // that may appear in NO pack's manifest schema (e.g. a raw write()
    // snapshot with an empty schema). The normalization below aligns every
    // RG's stats to the union schema, so the schema must cover them first.
    for rg in &union_rgs {
        for c in &rg.columns {
            if !union_schema.iter().any(|(n, _)| n == &c.name) {
                union_schema.push((c.name.clone(), c.value_type));
            }
        }
    }

    // NORMALIZE per-RG stats to the union schema (PMAN v2 CORRECTNESS —
    // see manifest::normalize_rgs_to_schema for the full argument). This
    // compact was the first writer to assemble a manifest from RGs of
    // multiple origins; branch::merge got the same guard.
    crate::manifest::normalize_rgs_to_schema(&mut union_rgs, &union_schema);

    let mut union_manifest = crate::manifest::CollectionManifest::new(union_schema, key_col);
    for rg in union_rgs {
        union_manifest.add_row_group(rg);
    }
    let manifest_bytes = union_manifest.encode();

    // 4. The new upto map: everything ≤ upto[w] is folded. Includes
    //    writers whose entries were ALL below the snapshot's upto (max
    //    with the snapshot's map) and the compactor's own pack seq (its
    //    pack IS the folded state, so entries ≤ its seq need no probe).
    //
    //    PRUNED (tribunal F6b): writers with NO live entries above the
    //    fold are dropped from the map — this compaction deletes their
    //    remaining entry paths, the (F6a) empty-dir cleanup removes their
    //    writer dir, and discovery will never see them again. Without the
    //    prune, the upto map grew unboundedly with every writer process
    //    that ever lived (observed: 3 CLI invocations → 3 permanent upto
    //    entries). The compactor itself is always kept (its pack's own
    //    seq needs the watermark).
    let mut upto: BTreeMap<String, u64> = view.snapshot_upto.clone();
    let mut live_max: BTreeMap<&str, u64> = BTreeMap::new();
    for e in &view.entries {
        let entry = live_max.entry(e.writer.as_str()).or_insert(0);
        if e.seq > *entry {
            *entry = e.seq;
        }
    }
    for (w, m) in &live_max {
        let entry = upto.entry(w.to_string()).or_insert(0);
        if *m > *entry {
            *entry = *m;
        }
    }
    upto.retain(|w, _| {
        w == &writer.writer_id || live_max.contains_key(w.as_str())
    });

    // 5. Append the folded pack to the compactor's OWN log FIRST — it is
    //    a data-bearing entry (its manifest IS the folded manifest), so
    //    it must be discoverable by probes past the old watermark.
    //    (The writer Mutex is already held — see step 0.)
    //
    //    `folds` records the entry packs this fold absorbed: their PATH
    //    pointers are deleted below, but their PACK BLOBS survive
    //    (content-addressed) and `history` walks this list so the folded
    //    writes' messages stay visible — compaction must not erase
    //    commit-history granularity.
    let parent = view.snapshot.clone();
    let parent_index = parent
        .as_ref()
        .and_then(|p| commit::read_commit(kernel, p))
        .map(|c| c.index + 1)
        .unwrap_or(0);
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0);
    let mut commit_obj = json!({
        "parent": parent,
        "second_parent": null,
        "manifest": "packed",
        "message": "journal compaction",
        "timestamp": timestamp,
        "index": parent_index,
        "folds": view.entries.iter().map(|e| e.pack_hash.clone()).collect::<Vec<_>>(),
    });
    let (snapshot_hash, snapshot_seq) = append_pack_with_writer(
        kernel, collection, branch, &mut writer,
        &mut commit_obj, &manifest_bytes, Some(&upto),
    )?;
    // Auto-compaction bookkeeping: the writer's log is folded through
    // snapshot_seq (its own pack).
    writer.last_fold_seq = snapshot_seq;

    // 6. LWW-advance the branch ref (benign — see the function docs).
    kernel
        .reference(&branch_ref(collection, branch), &snapshot_hash)
        .map_err(|e| format!("compaction: branch ref update failed: {}", e))?;

    // 7. Delete folded entry paths — DELTA ONLY (tribunal F3): the
    //    previous snapshot's upto already had its entries deleted by the
    //    fold that stamped it; re-deleting them made every compaction
    //    O(writer's total seq count) in delete_ref calls (HTTP DELETEs on
    //    S3/R2 — quadratic cumulative cost under auto-compact). The range
    //    is `prev_upto ..= upto` (INCLUSIVE of prev): seqs below prev were
    //    deleted by earlier folds (no-ops), and prev itself is either
    //    already gone (a data entry) or the PREVIOUS fold's own pointer
    //    — which is probe-unreachable now (every reader probes from
    //    prev+1) and safe to drop. EXCEPT the compactor's just-written
    //    entry (the new snapshot's own pointer — a reader whose
    //    branch_ref read predates our LWW still finds the pack by probing
    //    the log).
    for (writer_id, max_seq) in &upto {
        let is_compactor = &writer.writer_id == writer_id;
        let prev = view.snapshot_upto.get(writer_id).copied().unwrap_or(0);
        for seq in prev..=*max_seq {
            if is_compactor && seq == snapshot_seq {
                continue;
            }
            let path = entry_path(collection, branch, writer_id, seq);
            let _ = kernel.delete_ref(&path);
        }
    }

    // 8. Clear the folded shards (refs + blobs). The writer Mutex can be
    //    released here — shard clearing does not touch the compactor's
    //    log, and the fold's deletes (the Mutex-protected part) are done.
    drop(writer);
    shard::clear_shards(kernel, collection, branch)?;

    Ok(CompactStats {
        entries_folded: view.entries.len(),
        shards_folded,
        new_snapshot: snapshot_hash,
    })
}

/// Encode merged shard rows as one PND2 row group (the write_rows encode
/// machinery: pnd2_encode_multi_typed + zstd + column stats) and return
/// its manifest entry.
///
/// Type inference mirrors the CLI/SQL writers: the first non-null value
/// fixes a column's type (bool/int/float/string; nested JSON becomes a
/// VARIANT column); nulls decode to the type's default; heterogeneous
/// values are stringified. Lens-written shard rows are uniform, so the
/// lossy cases are edge-only.
fn build_rg_from_json_rows(
    kernel: &PondKernel,
    rows: &[Value],
) -> Result<crate::manifest::RowGroupEntry, String> {
    // Column order: first-seen across rows (serde_json objects preserve
    // insertion order).
    let mut names: Vec<String> = Vec::new();
    for row in rows {
        if let Some(obj) = row.as_object() {
            for k in obj.keys() {
                if !names.iter().any(|n| n == k) {
                    names.push(k.clone());
                }
            }
        }
    }

    let mut columns: Vec<(&str, pond_core::TypedColumn)> = Vec::new();
    for name in &names {
        let col = json_column_to_typed(rows, name);
        columns.push((name.as_str(), col));
    }

    let blob = crate::write::maybe_compress_pnd2(&pond_core::pnd2_encode_multi_typed(&columns));
    let data_hash = kernel
        .write(&blob)
        .map_err(|e| format!("compaction: shard pack write failed: {}", e))?;

    let n_rows = rows.len() as u32;
    let col_stats: Vec<crate::manifest::ColumnStatsEntry> = columns
        .iter()
        .map(|(name, col)| {
            let (min, max) = col
                .min_max_bytes()
                .map(|(mn, mx)| (Some(mn), Some(mx)))
                .unwrap_or((None, None));
            crate::manifest::ColumnStatsEntry {
                name: name.to_string(),
                value_type: col.vtype(),
                min,
                max,
                null_count: 0,
            }
        })
        .collect();

    Ok(crate::manifest::RowGroupEntry {
        key: "rg_0000000000".to_string(), // re-keyed by the caller
        blob_hash: data_hash,
        n_rows,
        columns: col_stats,
        slab_byte_offset: None,
        slab_byte_len: None,
    })
}

/// Infer + convert one JSON column to a TypedColumn (see
/// `build_rg_from_json_rows` for the inference rules).
fn json_column_to_typed(rows: &[Value], name: &str) -> pond_core::TypedColumn {
    use pond_core::TypedColumn;
    use serde_json::Value as JsonValue;

    // Infer the type from the first non-null value.
    let mut inferred: Option<u8> = None;
    for row in rows {
        match row.get(name) {
            Some(JsonValue::Bool(_)) => {
                inferred = Some(pond_core::VT_BOOLEAN);
                break;
            }
            Some(JsonValue::Number(n)) => {
                inferred = Some(if n.is_i64() || n.is_u64() {
                    pond_core::VT_INT64
                } else {
                    pond_core::VT_FLOAT64
                });
                break;
            }
            Some(JsonValue::String(_)) => {
                inferred = Some(pond_core::VT_STRING);
                break;
            }
            Some(JsonValue::Array(_)) | Some(JsonValue::Object(_)) => {
                inferred = Some(pond_core::VT_VARIANT);
                break;
            }
            _ => {}
        }
    }
    match inferred {
        Some(pond_core::VT_BOOLEAN) => TypedColumn::Boolean(
            rows.iter()
                .map(|r| r.get(name).and_then(|v| v.as_bool()).unwrap_or(false))
                .collect(),
        ),
        Some(pond_core::VT_INT64) => TypedColumn::Int64(
            rows.iter()
                .map(|r| r.get(name).and_then(|v| v.as_i64()).unwrap_or(0))
                .collect(),
        ),
        Some(pond_core::VT_FLOAT64) => TypedColumn::Float64(
            rows.iter()
                .map(|r| r.get(name).and_then(|v| v.as_f64()).unwrap_or(0.0))
                .collect(),
        ),
        Some(pond_core::VT_VARIANT) => TypedColumn::Variant(
            rows.iter()
                .map(|r| match r.get(name) {
                    Some(v @ (JsonValue::Array(_) | JsonValue::Object(_))) => {
                        v.to_string()
                    }
                    Some(JsonValue::String(s)) => s.clone(),
                    Some(JsonValue::Null) | None => JsonValue::Null.to_string(),
                    Some(other) => other.to_string(),
                })
                .collect(),
        ),
        // Default (all-null or string-typed): STRING with stringified
        // non-string values — the CLI's json-to-column fallback behavior.
        _ => TypedColumn::String(
            rows.iter()
                .map(|r| match r.get(name) {
                    Some(JsonValue::String(s)) => s.clone(),
                    Some(JsonValue::Null) | None => String::new(),
                    Some(other) => other.to_string(),
                })
                .collect(),
        ),
    }
}

/// Classify packs by kind: `true` = compaction snapshot (journal.upto
/// present in the pack's commit JSON), `false` = data entry. Lenient on
/// read/parse failures (treated as data — always read, never skipped).
fn classify_packs(kernel: &PondKernel, pack_hashes: &[String]) -> HashMap<String, bool> {
    let mut out = HashMap::with_capacity(pack_hashes.len());
    for hash in pack_hashes {
        let is_compact = kernel.read_blob(hash).ok()
            .and_then(|data| {
                if pond_pack::is_pack(&data) {
                    pond_pack::decode_pack(&data).map(|(c, _, _)| c)
                } else {
                    serde_json::from_slice::<Value>(&data).ok()
                }
            })
            .and_then(|c| c.get("journal").and_then(|j| j.get("upto")).map(|_| true))
            .unwrap_or(false);
        out.insert(hash.clone(), is_compact);
    }
    out
}

/// Collect a pack's manifest RG blob hashes into `out`. Lenient: an
/// unreadable pack contributes nothing (fail-safe: the caller's coverage
/// check then keeps entries rather than dropping them).
fn collect_rg_hashes(
    kernel: &PondKernel,
    pack_hash: &str,
    out: &mut std::collections::BTreeSet<String>,
) {
    if let Ok(manifest_bytes) = commit::resolve_manifest_bytes(kernel, pack_hash) {
        if let Ok(manifest) = crate::read::resolve_manifest(kernel, &manifest_bytes, None) {
            for rg in &manifest.row_groups {
                out.insert(rg.blob_hash.clone());
            }
        }
    }
}

/// Is there anything live to fold — live journal entries or shards?
///
/// The guard `branch()`/`merge()` use before compacting: folding a quiet
/// branch is a pointless rewrite (and would force-parse every legacy
/// snapshot's manifest — including hand-built fixtures whose "manifest"
/// bytes predate PMAN, which correctly fail decode). When nothing is live,
/// the branch ref already IS the full folded state.
pub fn has_live_state(kernel: &PondKernel, collection: &str, branch: &str) -> bool {
    let has_entries = resolve_view(kernel, collection, branch, true)
        .map(|v| !v.entries.is_empty())
        .unwrap_or(false);
    has_entries || !shard::list_shards(kernel, collection, branch).is_empty()
}

// ---------------------------------------------------------------------------
// History — journal-aware commit log (git-log-like)
// ---------------------------------------------------------------------------

/// Read the `folds` list out of a compaction pack's commit JSON (the entry
/// packs this fold absorbed). Empty for data entries and legacy packs.
fn read_folds(commit_obj: &Value) -> Vec<String> {
    commit_obj
        .get("folds")
        .and_then(|f| f.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default()
}

/// Journal-aware commit history, newest first — what `pond history` shows.
///
/// Sources, all deduped by hash and sorted by `(timestamp desc, index desc)`:
///   1. every LIVE journal entry's commit (the writes since the last fold),
///   2. the SNAPSHOT chain walked via `commit::history` (fold snapshots),
///   3. one level of `folds` per snapshot — the entry packs each fold
///      absorbed. Their journal PATH pointers are deleted at fold time, but
///      the packs themselves are content-addressed blobs that survive, and
///      the fold commit records their hashes precisely so the folded
///      writes' messages remain visible here. Without this, every
///      compaction would erase the history granularity of the writes it
///      folded (bootstrap-folded first writes would vanish entirely).
///
/// Ordering note: timestamps are wall-clock stamped at write time; ties and
/// small skews are broken by `index` then hash — display order, not
/// correctness order (the CRDT merge is order-independent by construction).
pub fn history(
    kernel: &PondKernel,
    collection: &str,
    branch: &str,
    limit: usize,
) -> Result<Vec<(String, commit::Commit)>, String> {
    let view = resolve_view(kernel, collection, branch, false)?;

    let mut seen: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    let mut commits: Vec<(String, commit::Commit)> = Vec::new();

    // Insert a commit (deduped by hash) if its blob is readable.
    let push = |hash: &str,
                commits: &mut Vec<(String, commit::Commit)>,
                seen: &mut std::collections::BTreeSet<String>| {
        if seen.insert(hash.to_string()) {
            if let Some(c) = commit::read_commit(kernel, hash) {
                commits.push((hash.to_string(), c));
            }
        }
    };

    // 1. Live entries (newest data first: later seq first within a writer).
    for e in view.entries.iter().rev() {
        push(&e.pack_hash, &mut commits, &mut seen);
    }

    // 2. Snapshot chain + 3. each snapshot's folds (one level). The folds
    //    live in the pack's RAW commit JSON (the typed Commit does not
    //    carry them), so they are read straight from the blob.
    if let Some(snapshot) = &view.snapshot {
        for (hash, _c) in commit::history(kernel, snapshot, limit.max(64)) {
            for folded in read_folds_from_blob(kernel, &hash) {
                push(&folded, &mut commits, &mut seen);
            }
            if seen.insert(hash.clone()) {
                if let Some(c) = commit::read_commit(kernel, &hash) {
                    commits.push((hash, c));
                }
            }
        }
    }

    // Newest first: timestamp desc, index desc, hash as final tiebreak.
    commits.sort_by(|a, b| {
        b.1.timestamp
            .partial_cmp(&a.1.timestamp)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(b.1.index.cmp(&a.1.index))
            .then(b.0.cmp(&a.0))
    });
    commits.truncate(limit);
    Ok(commits)
}

/// Read the `folds` list straight from a pack BLOB's commit JSON (the typed
/// [`commit::Commit`] does not carry non-structural fields).
fn read_folds_from_blob(kernel: &PondKernel, pack_hash: &str) -> Vec<String> {
    let data = match kernel.read_blob(pack_hash) {
        Ok(d) => d,
        Err(_) => return Vec::new(),
    };
    if !pond_pack::is_pack(&data) {
        return Vec::new();
    }
    match pond_pack::decode_pack(&data) {
        Some((commit_obj, _, _)) => read_folds(&commit_obj),
        None => Vec::new(),
    }
}

// ---------------------------------------------------------------------------
// Status — introspection for `pond journal-status` (D2)
// ---------------------------------------------------------------------------

/// Per-writer journal state.
#[derive(Debug, Clone)]
pub struct WriterStatus {
    pub writer: String,
    /// Live entries currently discoverable above the snapshot watermarks.
    pub entries: usize,
    /// Highest live seq (0 when everything is folded).
    pub max_seq: u64,
}

/// The full journal state of a branch.
#[derive(Debug, Clone)]
pub struct JournalStatus {
    pub snapshot: Option<String>,
    pub snapshot_upto: BTreeMap<String, u64>,
    pub writers: Vec<WriterStatus>,
    /// Total live entries across writers.
    pub live_entries: usize,
}

/// Report the journal state (forces a fresh discovery LIST — this is a
/// diagnostic/maintenance entry point, not the warm read path).
pub fn status(
    kernel: &PondKernel,
    collection: &str,
    branch: &str,
) -> Result<JournalStatus, String> {
    let view = resolve_view(kernel, collection, branch, true)?;
    let mut per_writer: BTreeMap<String, (usize, u64)> = BTreeMap::new();
    for e in &view.entries {
        let entry = per_writer.entry(e.writer.clone()).or_insert((0, 0));
        entry.0 += 1;
        if e.seq > entry.1 {
            entry.1 = e.seq;
        }
    }
    let writers = per_writer
        .into_iter()
        .map(|(writer, (entries, max_seq))| WriterStatus {
            writer,
            entries,
            max_seq,
        })
        .collect();
    Ok(JournalStatus {
        snapshot: view.snapshot,
        snapshot_upto: view.snapshot_upto,
        live_entries: view.entries.len(),
        writers,
    })
}

// ---------------------------------------------------------------------------
// Tests — unit level (integration/behavioral tests live in
// core/storage/tests/journal_test.rs)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::UnifiedStorage;

    #[test]
    fn test_entry_path_is_zero_padded_and_unique() {
        let a = entry_path("c", "main", "w1", 1);
        let b = entry_path("c", "main", "w1", 2);
        let c = entry_path("c", "main", "w2", 1);
        assert_eq!(a, "collections/c/_branches/main/journal/w1/000000000001");
        assert_eq!(b, "collections/c/_branches/main/journal/w1/000000000002");
        assert_eq!(c, "collections/c/_branches/main/journal/w2/000000000001");
        assert_ne!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn test_writer_for_same_key_returns_same_writer() {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();
        let w1 = writer_for(kernel, "t", "main");
        let w2 = writer_for(kernel, "t", "main");
        // Read both ids BEFORE comparing: holding one guard while locking
        // the SAME mutex again would deadlock (std Mutex is not reentrant —
        // assert_eq! keeps the first temporary alive for the whole call).
        let id1 = w1.lock().unwrap().writer_id.clone();
        let id2 = w2.lock().unwrap().writer_id.clone();
        assert_eq!(id1, id2, "same (store, collection, branch) ⇒ same registry writer");
        let w3 = writer_for(kernel, "t", "other");
        let id3 = w3.lock().unwrap().writer_id.clone();
        assert_ne!(id1, id3, "different branch ⇒ different writer");
    }

    #[test]
    fn test_writer_for_different_stores_are_isolated() {
        let dir1 = tempfile::tempdir().unwrap();
        let dir2 = tempfile::tempdir().unwrap();
        let s1 = UnifiedStorage::new_local(dir1.path()).unwrap();
        let s2 = UnifiedStorage::new_local(dir2.path()).unwrap();
        let w1 = writer_for(s1.kernel(), "t", "main");
        let w2 = writer_for(s2.kernel(), "t", "main");
        assert_ne!(w1.lock().unwrap().writer_id, w2.lock().unwrap().writer_id);
    }

    #[test]
    fn test_resolve_view_empty_collection() {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();
        let view = resolve_view(kernel, "ghosts", "main", true).unwrap();
        assert!(view.snapshot.is_none());
        assert!(view.entries.is_empty());
    }

    #[test]
    fn test_append_and_resolve_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        let kernel = storage.kernel();

        // Two appends through the registry writer. The FIRST append on a
        // fresh collection triggers the bootstrap-fold (see append_pack):
        // entry #1 is folded into a snapshot immediately and its pointer
        // deleted — so the visible journal state after both appends is
        // ONE snapshot (folding entry #1) plus ONE live entry (#3: seq 2
        // is the bootstrap fold pack, kept as the compactor's own entry).
        let mut hashes = Vec::new();
        for i in 0..2 {
            let mut commit_obj = json!({"message": format!("entry {}", i), "index": i});
            let manifest = crate::manifest::CollectionManifest::new(
                vec![("id".to_string(), pond_core::VT_INT64)],
                "id".to_string(),
            );
            let (hash, seq) = append_pack(
                kernel, "c", "main", &mut commit_obj,
                &manifest.encode(), &["id".to_string()],
            )
            .unwrap();
            assert_eq!(seq, i as u64 * 2 + 1, "data entries are seq 1 then seq 3 (seq 2 = bootstrap fold)");
            assert!(pond_pack::is_pack(&kernel.read_blob(&hash).unwrap()));
            hashes.push(hash);
        }

        // resolve_view: snapshot (the bootstrap fold) + the second data
        // entry live above its watermark.
        let view = resolve_view(kernel, "c", "main", true).unwrap();
        let snapshot = view.snapshot.expect("bootstrap fold created a snapshot");
        assert_ne!(snapshot, hashes[0], "the snapshot is the FOLD pack, not entry #1");
        assert_ne!(snapshot, hashes[1]);
        assert_eq!(view.entries.len(), 1, "only the second data entry is live");
        assert_eq!(view.entries[0].seq, 3);
        assert_eq!(
            view.snapshot_upto.get(&view.entries[0].writer),
            Some(&2u64),
            "the fold's upto watermark covers entries 1..=2"
        );

        // The live entry's commit JSON carries journal metadata.
        let (commit_obj, _, _) =
            pond_pack::decode_pack(&kernel.read_blob(&view.entries[0].pack_hash).unwrap())
                .unwrap();
        assert_eq!(commit_obj["journal"]["seq"], json!(3));
        assert!(commit_obj["journal"]["writer"].is_string());
    }
}
