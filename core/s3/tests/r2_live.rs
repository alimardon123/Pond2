// R2 LIVE harness (Rust) — the store-primitive layer of the N+6 live
// validation (ACCEPTANCE.md item 5). Env-gated: NEVER runs in CI, needs
// real credentials, costs real money (pennies), writes to a unique
// per-run prefix and cleans up after itself.
//
// Gate (ALL four must be set, else silent skip):
//   POND_R2_ENDPOINT        https://<account>.r2.cloudflarestorage.com
//   POND_R2_BUCKET          bucket name
//   POND_R2_ACCESS_KEY_ID   R2 access key id
//   POND_R2_SECRET_ACCESS_KEY  R2 secret
// Run (the driver script sources .env and maps R2_*/AWS_* → POND_R2_*):
//   scripts/test_r2_live_rust.sh
// or directly:
//   POND_R2_ENDPOINT=... POND_R2_BUCKET=... cargo test -p pond_s3 \
//     --test r2_live -- --nocapture
//
// WHAT IT PROVES (things moto emulation cannot):
//   1. put/get blob round trip + content addressing (sha256 == key hash)
//      against real R2 SigV4-accepted writes.
//   2. get_blob_range — the REAL HTTP Range: semantics (slab range reads
//      depend on R2 honoring partial GETs; a wrong implementation returns
//      200-full-body and the range contract silently degrades to full
//      fetches — assert the byte slice is EXACTLY the requested window).
//   3. Refs: put_path/get_path JSON binding; get_path on an absent key
//      must be Ok(None) — the C17 not-found discrimination on REAL R2
//      (R2's 404 XML must flow through is_s3_not_found, not look like an
//      outage).
//   4. list_paths prefix listing.
//   5. list_dirs — THE journal writer-discovery primitive (D3): the
//      delimiter-LIST (CommonPrefixes) behavior on real R2. The journal's
//      per-writer probes depend on this returning directory names ONLY,
//      one level, no per-entry keys.
//   6. delete_path/delete_blob round trip.
//   7. CachingObjectStore warm-read timing: cold (full R2 GET round trip)
//      vs warm (local disk cache hit) — the staledb-budget evidence line.
//      Numbers print with --nocapture and land in the cycle worklog.

use std::time::Instant;

use pond_kernel::ObjectStore;
use pond_s3::{S3Credentials, S3ObjectStore};

fn r2_config() -> Option<(String, String, String, String)> {
    let endpoint = std::env::var("POND_R2_ENDPOINT").ok()?;
    let bucket = std::env::var("POND_R2_BUCKET").ok()?;
    let key = std::env::var("POND_R2_ACCESS_KEY_ID").ok()?;
    let secret = std::env::var("POND_R2_SECRET_ACCESS_KEY").ok()?;
    Some((endpoint, bucket, key, secret))
}

fn cleanup_prefix(store: &S3ObjectStore, prefix: &str) -> usize {
    // list_raw walks EVERY key under the run prefix (pagination included);
    // delete each. Best-effort: failures print but don't fail the test
    // (the run prefix is unique per run, so leftovers are inert garbage).
    let mut deleted = 0usize;
    match store.list_raw(prefix) {
        Ok(keys) => {
            for k in keys {
                if store.delete_raw(&k).unwrap_or(false) {
                    deleted += 1;
                }
            }
        }
        Err(e) => eprintln!("[cleanup] list_raw({prefix}) failed: {e}"),
    }
    deleted
}

#[test]
fn r2_live_store_primitives() {
    let Some((endpoint, bucket, key, secret)) = r2_config() else {
        eprintln!("SKIP: POND_R2_* not set (see the header of this file)");
        return;
    };

    let run_prefix = format!("r2-live-rust-{}", std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).unwrap().as_millis());
    let creds = S3Credentials { access_key: key, secret_key: secret, session_token: None };
    let agent = ureq::AgentBuilder::new()
        .timeout_connect(std::time::Duration::from_secs(10))
        .timeout_read(std::time::Duration::from_secs(60))
        .build();
    let store = S3ObjectStore::new(
        bucket.clone(), run_prefix.clone(), "auto", endpoint.clone(), creds, agent);
    println!("[r2-live] bucket={bucket} prefix={run_prefix}");

    // ── 1. Blob round trip + content addressing ─────────────────────────
    let payload: Vec<u8> = (0..4096u32).map(|i| (i % 251) as u8).collect();
    let hash = store.put_blob(&payload).expect("put_blob against real R2");
    // Content addressing: the returned hash IS sha256(payload) and the blob
    // key is blobs/<h[:2]>/<h>.
    use sha2::{Digest, Sha256};
    let expect_hash = hex::encode(Sha256::digest(&payload));
    assert_eq!(hash, expect_hash, "put_blob returns sha256(payload)");
    let got = store.get_blob(&hash).expect("get_blob round trip");
    assert_eq!(got, payload, "blob bytes round-trip byte-exactly");

    // ── 2. Range reads: REAL HTTP Range semantics ───────────────────────
    let window = store.get_blob_range(&hash, 100, 200).expect("get_blob_range");
    assert_eq!(window.len(), 100, "range read returns EXACTLY the window size");
    assert_eq!(window, payload[100..200].to_vec(),
        "range bytes are the requested window (R2 honors Range: — not a full-body 200)");

    // ── 3. Refs: binding + absent-key discrimination (C17 on real R2) ──
    store.put_path("collections/live/HEAD", &hash).expect("put_path");
    let resolved = store.get_path("collections/live/HEAD")
        .expect("get_path on a bound ref must not error");
    assert_eq!(resolved, Some(hash.clone()), "ref resolves to the bound hash");
    let absent = store.get_path("collections/never-bound/HEAD")
        .expect("C17 on real R2: a 404 must be Ok(None), not an outage-looking Err");
    assert_eq!(absent, None, "absent ref is Ok(None)");

    // ── 4. list_paths ────────────────────────────────────────────────────
    // Contract (both backends agree): results are relative to the STORE
    // root, filtered by the query prefix — callers (shard::list_shards,
    // kernel::list_names_prefix) strip the query prefix themselves.
    store.put_path("collections/live/_branches/main/commit", &hash).unwrap();
    let paths = store.list_paths("collections/").expect("list_paths");
    assert!(paths.contains(&"collections/live/HEAD".to_string()),
        "list_paths finds refs (store-root-relative): {paths:?}");
    assert!(paths.contains(&"collections/live/_branches/main/commit".to_string()));

    // ── 5. list_dirs — the journal writer-discovery primitive ───────────
    // Two "writers" with entry refs under their dirs, mirroring the journal
    // layout: .../journal/<writer>/<seq>.
    let wprefix = "collections/live/_branches/main/journal/";
    for (w, seqs) in [("writer_aaa", 1..=3u64), ("writer_bbb", 1..=2u64)] {
        for seq in seqs {
            store.put_path(&format!("{wprefix}{w}/{seq}"), &hash).unwrap();
        }
    }
    let dirs = store.list_dirs(wprefix).expect("list_dirs delimiter-LIST on real R2");
    let mut dirs = dirs;
    dirs.sort();
    assert_eq!(dirs, vec!["writer_aaa".to_string(), "writer_bbb".to_string()],
        "list_dirs returns ONE-LEVEL directory names only (CommonPrefixes), \
         never per-entry keys — the D3 discovery contract");
    // And a deeper prefix (inside one writer) lists NOTHING at dir level:
    let deep = store.list_dirs(&format!("{wprefix}writer_aaa/")).expect("list_dirs deep");
    assert_eq!(deep, Vec::<String>::new(),
        "seq entries are KEYS not dirs — list_dirs one level only");

    // ── 6. Deletes ───────────────────────────────────────────────────────
    let dp = store.delete_path("collections/live/HEAD").expect("delete_path");
    assert!(dp, "delete_path true when it existed");
    // R2 SEMANTICS NOTE (found live, N+6): S3's DELETE is IDEMPOTENT —
    // deleting an absent key returns 204 (not 404), so `delete_path` on
    // R2 returns Ok(true) again; the trait's "true if it existed" can
    // only be honored exactly on backends with a real existence check
    // (LocalFS). Distinguishing on R2 would cost a HEAD before every
    // DELETE — an extra round trip per delete for a return value no
    // production caller reads. Documented trade; accept either answer.
    let dp2 = store.delete_path("collections/live/HEAD")
        .expect("idempotent delete must not error");
    // Either answer is acceptable (see the note above) — the point is it
    // returns Ok, and the ref is really gone (asserted next).
    let _ = dp2;
    let gone = store.get_path("collections/live/HEAD").expect("get after delete");
    assert_eq!(gone, None, "the ref is really gone regardless of dp2");

    // ── 7. Warm-read timing through the 3-tier cache ────────────────────
    // Fresh cache dir so the first read is a genuine cold R2 GET.
    let cache_dir = std::env::temp_dir().join(format!("pond-r2-cache-{run_prefix}"));
    let mut inner_prefix = run_prefix.clone();
    inner_prefix.push_str("/timing");
    let tstore = S3ObjectStore::new(
        bucket, inner_prefix, "auto",
        endpoint,
        S3Credentials {
            access_key: std::env::var("POND_R2_ACCESS_KEY_ID").unwrap(),
            secret_key: std::env::var("POND_R2_SECRET_ACCESS_KEY").unwrap(),
            session_token: None,
        },
        ureq::AgentBuilder::new()
            .timeout_connect(std::time::Duration::from_secs(10))
            .timeout_read(std::time::Duration::from_secs(60))
            .build());
    let _ = tstore.put_blob(&payload).unwrap();
    let cached = pond_cache::CachingObjectStore::new(
        Box::new(tstore), &cache_dir).expect("cache wrap");

    let t0 = Instant::now();
    let _ = cached.get_blob(&expect_hash).expect("cold read");
    let cold = t0.elapsed();
    let t1 = Instant::now();
    let _ = cached.get_blob(&expect_hash).expect("warm read");
    let warm = t1.elapsed();
    let t2 = Instant::now();
    let _ = cached.get_blob(&expect_hash).expect("warm read 2");
    let warm2 = t2.elapsed();
    println!("[r2-live] 4 KiB blob read latency — cold (R2 GET RTT): {cold:?}, warm (local disk): {warm:?}, warm2: {warm2:?}");
    assert!(warm < cold,
        "warm read ({warm:?}) must beat the cold R2 round trip ({cold:?})");
    let _ = std::fs::remove_dir_all(&cache_dir);

    // ── cleanup ─────────────────────────────────────────────────────────
    // list_raw's arg is relative to the STORE ROOT (the store already
    // carries the run prefix) — "" sweeps the whole run's key space.
    let deleted = cleanup_prefix(&store, "");
    println!("[r2-live] cleanup: deleted {deleted} objects under {run_prefix}/");
    println!("[r2-live] ALL R2 STORE-PRIMITIVE TESTS PASSED");
}
