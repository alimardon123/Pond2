// PMAN format laws — ACCEPTANCE.md item 3 / CRITIQUE C3 / builder spec
// cron-2026-08-28-1100-b.
//
// These laws ATTACK the PMAN v2 collection manifest and the PMAN v3 root
// manifest with random schemas and adversarial (subset/superset/permutation,
// foreign-named, mismatched-typed) per-RG stats lists. The normalize law is
// the property whose violation CORRUPTED PMAN v2 in the wild (journal
// compaction + branch merge assembled manifests whose RG stats count ≠
// schema count — the decoder then read stats bytes as slab offsets).
//
// Format sentinel notes (shapes the byte-stability law deliberately does
// NOT generate, because the format cannot represent them — production
// writers never produce them either):
//   - stats min/max are encoded only as a PAIR (`has_stats` is one byte):
//     a (Some, None) half-stat encodes as "no stats". The normalize law
//     (arbitrary Option shapes) still covers pass-through of such shapes.
//   - slab (Some(0), Some(0)) is the "no slab" sentinel and decodes to
//     (None, None); (Some(o≠0), None) decodes to (Some(o), Some(0)).
//   - RootManifest key_min/key_max of length 0 are the None sentinel.
//   - schema_version None encodes as 0 and decodes as Some(0).
//
// DETERMINISM (ACCEPTANCE.md item 6): the seed below pins the entire case
// space; CI runs are byte-reproducible. Bump the seed's trailing constant
// to explore a new random space.

use proptest::collection::vec;
use proptest::prelude::*;
use proptest::test_runner::{Config as ProptestConfig, RngSeed};

use pond_core::{VT_BOOLEAN, VT_FLOAT64, VT_INT64, VT_STRING};
use pond_storage::manifest::{
    normalize_rgs_to_schema, CollectionManifest, ColumnStatsEntry, LeafEntry, RootManifest,
    RowGroupEntry, MAX_LEAF_RGS,
};

const LAWS_SEED: u64 = 0x504F4E44_00000002; // "POND" + file 2
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

// ---------------------------------------------------------------------------
// Shared strategies
// ---------------------------------------------------------------------------

/// One stat entry's payload BEFORE normalization: an arbitrary type tag
/// (may mismatch the schema — normalize must overwrite it) and arbitrary
/// min/max byte blobs.
type StatPayload = (u8, Option<Vec<u8>>, Option<Vec<u8>>, u32);

fn vtype_strategy() -> impl Strategy<Value = u8> {
    prop_oneof![
        1 => Just(VT_INT64),
        1 => Just(VT_FLOAT64),
        1 => Just(VT_STRING),
        1 => Just(VT_BOOLEAN),
    ]
}

fn bytes_strategy() -> impl Strategy<Value = Vec<u8>> {
    vec(any::<u8>(), 0..=8)
}

/// Arbitrary payload — min/max presence INDEPENDENT (attacks normalize's
/// pass-through with (Some, None) half-stats).
fn stat_payload_any() -> BoxedStrategy<StatPayload> {
    (
        vtype_strategy(),
        prop::option::weighted(0.75, bytes_strategy()),
        prop::option::weighted(0.75, bytes_strategy()),
        0u32..=1000,
    )
        .boxed()
}

/// Encodable payload — min/max BOTH present or BOTH absent (the shapes
/// PMAN v2 can represent; the byte-stability law generates only these).
fn stat_payload_encodable() -> BoxedStrategy<StatPayload> {
    (
        vtype_strategy(),
        any::<bool>(),
        bytes_strategy(),
        bytes_strategy(),
        0u32..=1000,
    )
        .prop_map(|(vtype, has_stats, min, max, null_count)| {
            if has_stats {
                (vtype, Some(min), Some(max), null_count)
            } else {
                (vtype, None, None, null_count)
            }
        })
        .boxed()
}

// Schema: 1..=6 columns, unique names from an 8-name pool, random VT tags.
prop_compose! {
    fn schema()(n in 1usize..=6, seed in any::<u64>(), vtypes in vec(vtype_strategy(), 6..=6))
        -> Vec<(String, u8)> {
        let mut pool = ["c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7"];
        shuffle_slice(&mut pool, seed);
        pool.iter()
            .take(n)
            .cloned()
            .zip(vtypes)
            .map(|(name, vtype)| (name.to_string(), vtype))
            .collect()
    }
}

/// One messy per-RG stats list: a random SUBSET of the schema names (each
/// with its own possibly-mismatched type tag), 0..=2 FOREIGN names, in a
/// random ORDER.
fn messy_stats(
    schema: Vec<(String, u8)>,
    payload: impl Strategy<Value = StatPayload> + Clone,
) -> impl Strategy<Value = Vec<(String, StatPayload)>> {
    let n = schema.len();
    (
        vec(any::<bool>(), n..=n),
        vec(payload.clone(), n..=n),
        vec(
            (
                prop::sample::select(vec!["fx", "fy", "fz"]),
                payload,
            ),
            0..=2,
        ),
        any::<u64>(),
    )
        .prop_map(move |(include, payloads, foreign, seed)| {
            let mut stats: Vec<(String, StatPayload)> = Vec::new();
            for (i, (name, _)) in schema.iter().enumerate() {
                if include[i] {
                    stats.push((name.clone(), payloads[i].clone()));
                }
            }
            for (fname, payload) in foreign {
                stats.push((fname.to_string(), payload));
            }
            shuffle_slice(&mut stats, seed);
            stats
        })
}

/// A full row-group spec: identity, size, messy stats, and a slab placement
/// from the roundtrip-faithful shapes (see the header's sentinel notes).
#[derive(Clone, Debug)]
struct RgSpec {
    key_num: u64,
    hash: (u64, u64, u64, u64),
    n_rows: u32,
    stats: Vec<(String, StatPayload)>,
    slab_kind: u8, // 0 = (None, None), 1 = (Some(off≥1), Some(len)), 2 = (Some(0), Some(len≥1))
    slab_off: u64,
    slab_len: u32,
}

fn rg_spec_strategy(
    schema: Vec<(String, u8)>,
    payload: impl Strategy<Value = StatPayload> + Clone,
) -> impl Strategy<Value = RgSpec> {
    (
        any::<u64>(),
        (any::<u64>(), any::<u64>(), any::<u64>(), any::<u64>()),
        0u32..=1000,
        messy_stats(schema, payload),
        0u8..3,
        0u64..=4096,
        0u32..=4096,
    )
        .prop_map(|(key_num, hash, n_rows, stats, slab_kind, slab_off, slab_len)| RgSpec {
            key_num,
            hash,
            n_rows,
            stats,
            slab_kind,
            slab_off,
            slab_len,
        })
}

fn rg_entry(spec: &RgSpec) -> RowGroupEntry {
    let blob_hash = format!(
        "{:016x}{:016x}{:016x}{:016x}",
        spec.hash.0, spec.hash.1, spec.hash.2, spec.hash.3
    );
    let (slab_byte_offset, slab_byte_len) = match spec.slab_kind {
        0 => (None, None),
        1 => (Some(spec.slab_off.max(1)), Some(spec.slab_len)),
        _ => (Some(0), Some(spec.slab_len.max(1))),
    };
    RowGroupEntry {
        key: format!("rg_{:010}", spec.key_num),
        blob_hash,
        n_rows: spec.n_rows,
        columns: spec
            .stats
            .iter()
            .map(|(name, (vtype, min, max, null_count))| ColumnStatsEntry {
                name: name.clone(),
                value_type: *vtype,
                min: min.clone(),
                max: max.clone(),
                null_count: *null_count,
            })
            .collect(),
        slab_byte_offset,
        slab_byte_len,
    }
}

/// The expected stat normalize produces for `name` (first stat carrying it).
fn expected_stat(spec: &RgSpec, name: &str) -> (Option<Vec<u8>>, Option<Vec<u8>>, u32) {
    match spec.stats.iter().find(|(n, _)| n == name) {
        Some((_, (_, min, max, null_count))) => (min.clone(), max.clone(), *null_count),
        None => (None, None, 0),
    }
}

// ---------------------------------------------------------------------------
// Laws
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(law_config(128))]

    // Law 1 — `normalize_rgs_to_schema` aligns EVERY RG's stats to the
    // schema: exactly `schema.len()` stats, names in schema ORDER, type
    // tags from the schema (mismatched tags overwritten), payloads attached
    // BY NAME, absent names becoming no-stats placeholders.
    #[test]
    fn normalize_aligns_stats_to_schema(
        case in schema().prop_flat_map(|schema| {
            let rg = rg_spec_strategy(schema.clone(), stat_payload_any());
            (Just(schema), vec(rg, 1..=5))
        }),
    ) {
        let (schema, specs) = case;
        let mut rgs: Vec<RowGroupEntry> = specs.iter().map(rg_entry).collect();

        normalize_rgs_to_schema(&mut rgs, &schema);

        for (rg, spec) in rgs.iter().zip(&specs) {
            prop_assert_eq!(rg.columns.len(), schema.len(),
                "every RG must carry exactly schema.len() stats (the PMAN v2 invariant)");
            for (i, (name, vtype)) in schema.iter().enumerate() {
                let col = &rg.columns[i];
                prop_assert_eq!(&col.name, name, "stats names must equal the schema IN ORDER");
                prop_assert_eq!(col.value_type, *vtype,
                    "type tags must come from the schema, not the RG's (possibly foreign) stats");
                // Payload attached BY NAME (semantic alignment).
                let (min, max, null_count) = expected_stat(spec, name);
                prop_assert_eq!(&col.min, &min);
                prop_assert_eq!(&col.max, &max);
                prop_assert_eq!(col.null_count, null_count);
            }
        }
    }

    // Law 2 — a NORMALIZED manifest encodes → decodes BYTE-STABLY
    // (encode(decode(e)) == e) and field-by-field faithfully (row groups
    // in order, columns, key_col, partition_spec).
    #[test]
    fn manifest_encode_decode_roundtrips_byte_stably(
        case in schema().prop_flat_map(|schema| {
            let rg = rg_spec_strategy(schema.clone(), stat_payload_encodable());
            (
                Just(schema.clone()),
                vec(rg, 1..=5),
                prop_oneof![Just("id".to_string()), Just(schema[0].0.clone())],
                prop_oneof![
                    Just(None),
                    (any::<u64>()).prop_map(|s| Some(format!("part-{:x}", s))),
                ],
                prop_oneof![Just(None), any::<u32>().prop_map(Some)],
            )
        }),
    ) {
        let (schema, specs, key_col, partition_spec, schema_version) = case;

        // Normalize first — PMAN v2 encodes stats per the manifest schema
        // count; that alignment is the format's precondition.
        let mut rgs: Vec<RowGroupEntry> = specs.iter().map(rg_entry).collect();
        normalize_rgs_to_schema(&mut rgs, &schema);

        let mut manifest = CollectionManifest::new(schema.clone(), key_col.clone());
        for rg in rgs {
            manifest.add_row_group(rg);
        }
        manifest.partition_spec = partition_spec.clone();
        manifest.schema_version = schema_version;

        let e1 = manifest.encode();
        let m2 = CollectionManifest::decode(&e1);
        prop_assert!(m2.is_some(), "decode must succeed on encode output");
        let m2 = m2.unwrap();

        let e2 = m2.encode();
        prop_assert_eq!(e2, e1, "encode(decode(e)) == e — byte-stable roundtrip");

        prop_assert_eq!(&m2.columns, &manifest.columns);
        prop_assert_eq!(&m2.key_col, &manifest.key_col);
        prop_assert_eq!(&m2.partition_spec, &manifest.partition_spec);
        // None encodes as the 0 sentinel and decodes as Some(0) — assert the
        // faithful half only (see header notes).
        if let Some(v) = manifest.schema_version {
            prop_assert_eq!(m2.schema_version, Some(v));
        }

        prop_assert_eq!(m2.row_groups.len(), manifest.row_groups.len());
        for (a, b) in manifest.row_groups.iter().zip(&m2.row_groups) {
            prop_assert_eq!(&b.key, &a.key);
            prop_assert_eq!(&b.blob_hash, &a.blob_hash);
            prop_assert_eq!(b.n_rows, a.n_rows);
            prop_assert_eq!(b.slab_byte_offset, a.slab_byte_offset);
            prop_assert_eq!(b.slab_byte_len, a.slab_byte_len);
            prop_assert_eq!(b.columns.len(), a.columns.len());
            for (ca, cb) in a.columns.iter().zip(&b.columns) {
                prop_assert_eq!(&cb.name, &ca.name);
                prop_assert_eq!(cb.value_type, ca.value_type);
                prop_assert_eq!(&cb.min, &ca.min);
                prop_assert_eq!(&cb.max, &ca.max);
                prop_assert_eq!(cb.null_count, ca.null_count);
            }
        }
    }

    // Law 3 — a PMAN v3 root manifest roundtrips: leaves preserved in
    // order (hash, n_row_groups, total_data_bytes, key bounds),
    // `total_row_groups()` sums the leaves, and `prune_leaves(&[])`
    // resolves ALL leaves (no predicates → no pruning).
    #[test]
    fn root_manifest_roundtrips_and_resolves_all_leaves(
        case in schema().prop_flat_map(|schema| {
            (
                Just(schema.clone()),
                vec(
                    (
                        (any::<u64>(), any::<u64>(), any::<u64>(), any::<u64>()),
                        vec(0u32..=1000, 1..=4),
                        prop::option::weighted(0.6, vec(any::<u8>(), 1..=16)),
                        prop::option::weighted(0.6, vec(any::<u8>(), 1..=16)),
                    ),
                    1..=5,
                ),
                prop_oneof![Just("id".to_string()), Just(schema[0].0.clone())],
            )
        }),
    ) {
        let (schema, leaves, key_col) = case;

        let mut root = RootManifest::new(schema.clone(), key_col.clone());
        let expected_total: u64 = leaves
            .iter()
            .map(|(_, rg_rows, _, _)| rg_rows.len() as u64)
            .sum();
        for (hash, rg_rows, key_min, key_max) in &leaves {
            root.leaves.push(LeafEntry {
                leaf_hash: format!(
                    "{:016x}{:016x}{:016x}{:016x}",
                    hash.0, hash.1, hash.2, hash.3
                ),
                n_row_groups: rg_rows.len() as u32,
                total_data_bytes: rg_rows.iter().map(|&n| n as u64).sum::<u64>() * 16,
                key_min: key_min.clone(),
                key_max: key_max.clone(),
            });
        }

        let encoded = root.encode();
        let decoded = RootManifest::decode(&encoded);
        prop_assert!(decoded.is_some(), "root decode must succeed on encode output");
        let decoded = decoded.unwrap();

        prop_assert_eq!(&decoded.columns, &root.columns);
        prop_assert_eq!(&decoded.key_col, &root.key_col);
        prop_assert_eq!(decoded.leaves.len(), root.leaves.len(),
            "leaves preserved in order");
        for (a, b) in root.leaves.iter().zip(&decoded.leaves) {
            prop_assert_eq!(&b.leaf_hash, &a.leaf_hash);
            prop_assert_eq!(b.n_row_groups, a.n_row_groups);
            prop_assert_eq!(b.total_data_bytes, a.total_data_bytes);
            prop_assert_eq!(&b.key_min, &a.key_min);
            prop_assert_eq!(&b.key_max, &a.key_max);
        }

        prop_assert_eq!(decoded.total_row_groups(), expected_total,
            "total_row_groups() == sum of leaf n_row_groups");
        let all_indices: Vec<usize> = (0..root.leaves.len()).collect();
        prop_assert_eq!(decoded.prune_leaves(&[]), all_indices,
            "no predicates → every leaf resolved (prune-all is resolve-all)");
    }
}

// ---------------------------------------------------------------------------
// Boundary companion (deterministic, not a proptest case)
// ---------------------------------------------------------------------------

/// MAX_LEAF_RGS boundary: a leaf at exactly the maximum legal size
/// roundtrips and still resolves under an empty predicate set.
#[test]
fn root_manifest_boundary_leaf_roundtrips() {
    let mut root = RootManifest::new(
        vec![("id".to_string(), VT_INT64)],
        "id".to_string(),
    );
    root.leaves.push(LeafEntry {
        leaf_hash: "ab".repeat(32),
        n_row_groups: MAX_LEAF_RGS as u32,
        total_data_bytes: 1024 * 1024,
        key_min: Some(0i64.to_le_bytes().to_vec()),
        key_max: Some(i64::MAX.to_le_bytes().to_vec()),
    });

    let encoded = root.encode();
    let decoded = RootManifest::decode(&encoded).expect("boundary leaf must decode");
    assert_eq!(decoded.leaves[0].n_row_groups as usize, MAX_LEAF_RGS);
    assert_eq!(decoded.total_row_groups(), MAX_LEAF_RGS as u64);
    assert_eq!(decoded.prune_leaves(&[]), vec![0usize]);
}
