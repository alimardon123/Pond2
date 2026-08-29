// PNPK + PSLB codec laws — ACCEPTANCE.md item 4 (crucible N+6) / CRITIQUE C3
// (the C3 zero-coverage residual: two load-bearing binary codecs with ZERO
// property coverage) / builder spec cron-2026-08-30-b.
//
// These laws ATTACK the two binary codecs every read depends on with random
// shapes and adversarial bytes. Their failure modes are SILENT CORRUPTION,
// not error returns: a pack that decodes a different manifest than was
// written, a slab whose planned byte ranges fetch the wrong rows, or a
// zone-map that "prunes" a row group that actually matches — none of those
// ever produce an error message.
//
// PNPK (pond_pack.rs — every commit/manifest blob pair on the hot path):
//   Law 1  roundtrip_is_lossless — commit VALUE-equal, manifest BYTE-equal,
//          inline shape-equal. Pins the documented encode-side
//          normalization Some(&[]) → has_inline=false → decodes as None
//          (the empty-inline sentinel).
//   Law 2  is_pack_discriminates — is_pack is a MAGIC-ONLY discriminator:
//          true for every encode_pack output, false for anything that
//          doesn't start with "PNPK" (incl. len < 4 and sibling magics).
//          Detection ≠ decodability: is_pack(b"PNPK") is true while
//          decode_pack needs 10 bytes (pinned in the companions).
//   Law 3  truncation_rejected — every strict prefix of a valid pack is
//          rejected. Encode emits no trailing padding, so any strict
//          prefix loses ≥ 1 required byte of some length-checked section.
//   Law 4  decode_never_panics_on_arbitrary_bytes — fuzz-style: arbitrary
//          bytes, magic-prefixed garbage, and crafted absurd length
//          fields (u32::MAX after the magic) must produce Some or None,
//          never a panic. The targeted adversarial shapes (version 3,
//          absurd lens, commit-JSON edge cases, u16 count mismatch) live
//          in the companion #[test]s below.
//   Law 5  encode_is_deterministic — byte-identical output for repeated
//          encodes (pack hashes are content-addressed; hash stability
//          needs encode stability) and the flag byte is 0 for None AND
//          Some(&[]), 1 for non-empty inline data.
//
// PSLB (slab.rs — every slab; the range-read contract the S3 reader is
// built on):
//   Law 1  roundtrip_is_lossless — payloads BYTE-equal in order, footer
//          entries match (rg_index order, offsets/lens/rows), stats equal
//          under the half-stat normalization sentinel, and footer.bloom
//          is None (encode_slab never writes a bloom section — pinned;
//          bloom slabs come only from encode_slab_compressed_with_bloom).
//   Law 2  tail_invariant — the 12-byte tail yields the footer offset;
//          the footer byte-range decodes to the SAME entries as the full
//          decode AND to the offsets recomputed independently from the
//          documented layout formula (10 + Σ(prev rg_len + 4) + 4);
//          plan_ranges(None) is exactly the entries' ranges — sorted
//          ascending, disjoint, inside [PSLB_HEADER_LEN, footer start).
//          Also pins the last-12-bytes rule for over-long tail inputs.
//   Law 3  range_fetch_reconstructs — each planned range sliced out of
//          the slab is byte-equal to the ORIGINAL row-group payload at
//          that index (the get_blob_range contract the reader relies on).
//   Law 4  compressed_roundtrip — decode_slab returns DECOMPRESSED
//          payloads (pinned); the reader-level invariant is range-fetch
//          + decompress_rg == original bytes; footer byte_lens are the
//          compressed lens and equal the payload length prefixes; zstd
//          encode is deterministic (content-addressed slab hashes).
//   Law 5  truncation_and_tail_discrimination — strict prefixes rejected
//          (payloads drawn from a magic-letter-free alphabet so the
//          tail-magic argument is DETERMINISTIC — see the law comment),
//          the wrong-tail-magic contract, is_slab discrimination, and a
//          decode-never-panics fuzz arm.
//   Law 6  plan_ranges_pruning_is_conservative — for i64 zone maps, a
//          probe inside every range keeps ALL RGs (no false drops — the
//          read-side correctness invariant), a probe outside every range
//          prunes to exactly those RGs, stats-less RGs (no column /
//          foreign column / half-stats) are NEVER pruned, and in general
//          the plan equals exactly the RGs whose [min,max] can't prove
//          exclusion. bloom is None here — pure zone map (pinned).
//
// Format sentinel notes (documented intentional behaviors the roundtrip
// laws deliberately handle rather than "fix"):
//   - PNPK inline data Some(&[]) encodes has_inline=false and decodes as
//     None (encode-side normalization); a hand-crafted flag=1 + n_blobs=0
//     pack decodes as Some(vec![]) — decoder leniency, pinned in the
//     companion test pnpk_adversarial_inline_count_mismatch.
//   - PSLB reuses the PMAN stats sentinel: a (Some, None) half-stat
//     encodes has_stats=0 and decodes as (None, None).
//   - PSLB decode_slab returns DECOMPRESSED row_groups for compressed
//     slabs while the footer's byte_len stays the COMPRESSED length.
//   - decode_slab_tail NEVER returns None for a ≥12-byte input, even
//     with a wrong magic — the documented contract is (footer_offset,
//     valid=false) and the CALLER rejects (the builder spec's "wrong
//     magic → None" phrasing was corrected against the function's own
//     doc comment; see the law + companion test).
//   - Both decoders tolerate trailing bytes after the last section (no
//     end-of-blob check) — leniency, pinned in companion tests.
//   - 32-bit caveat (documented, not law-testable on this 64-bit host):
//     decode_pack's `pos + len` and decode_slab's `byte_offset as usize`
//     would wrap/truncate on 32-bit targets for crafted length fields.
//
// KNOWN BUGS / FINDINGS (kept as #[ignore]d laws + #[ignored] tests, NOT
// fixed — read-only tree per the builder contract):
//
//   FINDING #1 (law `law_pnpk_roundtrip_is_lossless`, kept verbatim as
//   #[ignore] — the C3-laws precedent from laws_crdt.rs): the pack round
//   trip is NOT lossless for arbitrary finite f64 commits — but the loss
//   is NOT in pond_pack.rs's framing (its bytes are exact), it is in
//   serde_json's DEFAULT float parsing ("best-effort precision"; the
//   exact parser is behind serde_json's opt-in `float_roundtrip` cargo
//   feature, which pond does not enable). Measured on this toolchain:
//   ~30% of uniformly-random finite f64s and ~6.5% of everyday-magnitude
//   f64s re-parse 1 ULP off after a to_vec/from_slice text round trip —
//   e.g. 3.237893019965854e-46 (bits 0x367d937f5d7d6688) comes back as
//   0x367d937f5d7d6689. encode_pack(serde_json::to_vec) is ryu-exact;
//   decode_pack(serde_json::from_slice) is the lossy half. Production
//   commits (commit.rs) carry hashes/messages/timestamps — integers and
//   strings — so the practical blast radius is commits built from
//   arbitrary Values (journal/pyo3 surfaces). Fix shape (owner): enable
//   serde_json's `float_roundtrip` feature in core/storage. The passing
//   companion `law_pnpk_roundtrip_is_lossless_json_representable` pins
//   the pack's own framing fidelity over the domain serde_json can
//   represent losslessly (floats filtered through the dependency's own
//   round-trip oracle — the same move as laws_pman's
//   stat_payload_encodable: generate the shapes the format can carry).
//
//   FINDING #2 (#[test] pslb_decode_must_not_abort_on_huge_footer_n_
//   entries): decode_slab_footer runs Vec::with_capacity on the
//   UNVALIDATED footer entry count — a footer starting with FF FF FF FF
//   attempts a ~192 GiB allocation and ABORTS the process (allocator
//   failure does not unwind) instead of returning None, violating the
//   format's own C1/C2 contract ("malformed input never crashes").
//
// DETERMINISM (ACCEPTANCE.md item 6): the seed below pins the entire case
// space; CI runs are byte-reproducible. Bump the seed's trailing constant
// to explore a new random space.

use proptest::collection::vec;
use proptest::prelude::*;
use proptest::test_runner::{Config as ProptestConfig, RngSeed, TestCaseError};

use serde_json::Value;

use pond_core::{VT_BOOLEAN, VT_FLOAT64, VT_INT64, VT_STRING};
use pond_storage::manifest::ColumnStatsEntry;
use pond_storage::pond_pack::{decode_pack, encode_pack, is_pack, PNPK_MAGIC};
use pond_storage::slab::{
    decode_slab, decode_slab_footer, decode_slab_tail, decompress_rg, encode_slab,
    encode_slab_compressed, is_slab, is_slab_compressed, is_slab_has_bloom, PSLB_HEADER_LEN,
    PSLB_TAIL_LEN, PSLB_MAGIC, SlabEntry,
};

const LAWS_SEED: u64 = 0x504F4E44_00000004; // "POND" + file 4
fn law_config(cases: u32) -> ProptestConfig {
    let mut c = ProptestConfig::with_cases(cases);
    c.rng_seed = RngSeed::Fixed(LAWS_SEED);
    c
}

type LawResult = Result<(), TestCaseError>;

// ---------------------------------------------------------------------------
// Deterministic helpers (splitmix64 — no rand dependency)
// ---------------------------------------------------------------------------

fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E3779B97F4A7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}

/// Uniform i64 in [lo, hi] from the splitmix64 stream (lo <= hi).
fn sm_range(sm: &mut u64, lo: i64, hi: i64) -> i64 {
    lo + (splitmix64(sm) % ((hi - lo + 1) as u64)) as i64
}

/// Strict-prefix lengths to test: every length for small blobs, else the
/// structural edges (header + first section bytes, and len-1) plus
/// `budget` deterministic samples from the middle. All lengths are in
/// 0..len (strict prefixes — the full blob is never included).
fn sampled_prefix_lens(len: usize, budget: usize, seed: u64) -> Vec<usize> {
    if len == 0 {
        return Vec::new();
    }
    let mut lens: Vec<usize> = if len <= 48 {
        (0..len).collect()
    } else {
        let mut v: Vec<usize> = (0..16).collect();
        v.push(len - 1);
        let mut state = seed | 1;
        for _ in 0..budget {
            v.push((splitmix64(&mut state) as usize) % len);
        }
        v
    };
    lens.sort_unstable();
    lens.dedup();
    lens
}

/// Structural equality for footer entries (SlabEntry has no PartialEq).
fn slab_entries_equal(a: &[SlabEntry], b: &[SlabEntry]) -> bool {
    a.len() == b.len()
        && a.iter().zip(b).all(|(x, y)| {
            x.rg_index == y.rg_index
                && x.byte_offset == y.byte_offset
                && x.byte_len == y.byte_len
                && x.n_rows == y.n_rows
                && x.columns.len() == y.columns.len()
                && x.columns.iter().zip(&y.columns).all(|(p, q)| {
                    p.name == q.name
                        && p.value_type == q.value_type
                        && p.min == q.min
                        && p.max == q.max
                        && p.null_count == q.null_count
                })
        })
}

/// Decoded stats must equal the input stats under the format's half-stat
/// normalization: (min, max) both-present survive verbatim; anything else
/// decodes as (None, None) — the PMAN sentinel reused by slab footers.
/// name/vtype/null_count always pass through.
fn assert_stats_roundtrip(
    got: &[ColumnStatsEntry],
    want: &[ColumnStatsEntry],
) -> LawResult {
    prop_assert_eq!(got.len(), want.len(), "column count must roundtrip");
    for (g, w) in got.iter().zip(want) {
        prop_assert_eq!(&g.name, &w.name);
        prop_assert_eq!(g.value_type, w.value_type);
        prop_assert_eq!(g.null_count, w.null_count);
        match (&w.min, &w.max) {
            (Some(min), Some(max)) => {
                prop_assert_eq!(g.min.as_deref(), Some(min.as_slice()),
                    "full stats must survive byte-exactly");
                prop_assert_eq!(g.max.as_deref(), Some(max.as_slice()));
            }
            _ => {
                prop_assert!(
                    g.min.is_none() && g.max.is_none(),
                    "half-stats (min XOR max) must normalize to no-stats — the PMAN sentinel"
                );
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Shared strategies
// ---------------------------------------------------------------------------

/// A byte that cannot participate in spelling "PNPK" or "PSLB" — used by
/// the PSLB truncation law, whose correctness argument requires the magic
/// to be UNREPRESENTABLE in payload/footer bytes (see that law's comment).
fn non_magic_byte() -> impl Strategy<Value = u8> {
    any::<u8>().prop_filter("excludes P/N/K/S/L/B", |&b| {
        !matches!(b, b'P' | b'N' | b'K' | b'S' | b'L' | b'B')
    })
}

// --- PNPK: structured JSON commits -----------------------------------------

fn json_string() -> BoxedStrategy<String> {
    prop_oneof![
        1 => Just(String::new()),
        3 => prop::sample::select(vec![
            "a".to_string(),
            "message".to_string(),
            "ключ".to_string(),
            "列".to_string(),
            "🦀".to_string(),
            "with \"quotes\" and \\ backslash".to_string(),
            "line\nbreak\ttab\u{0}nul".to_string(),
        ]),
        2 => vec(any::<char>(), 0..=8).prop_map(|cs| cs.into_iter().collect()),
    ]
    .boxed()
}

/// Arbitrary finite f64 (bit patterns, incl. subnormals and -0.0).
fn finite_f64() -> BoxedStrategy<f64> {
    any::<u64>()
        .prop_map(f64::from_bits)
        .prop_filter("finite", |f| f.is_finite())
        .boxed()
}

/// serde_json's own text round-trip oracle (see FINDING #1): true when the
/// dependency parses the value back bit-exactly. Used to scope the
/// passing roundtrip law to the precision domain the JSON-text format as
/// configured can actually carry.
fn json_roundtrip_exact(f: f64) -> bool {
    match serde_json::to_string(&f) {
        Ok(s) => serde_json::from_str::<f64>(&s)
            .map(|g| g.to_bits() == f.to_bits())
            .unwrap_or(false),
        Err(_) => false,
    }
}

/// Finite f64s that survive a serde_json text round trip bit-exactly
/// (≈70% of uniform-random finite bit patterns — the rest re-parse 1 ULP
/// off under the default best-effort float parser; FINDING #1).
fn json_exact_f64() -> BoxedStrategy<f64> {
    finite_f64()
        .prop_filter("serde_json-exact", |f| json_roundtrip_exact(*f))
        .boxed()
}

fn json_leaf(floats: BoxedStrategy<f64>) -> BoxedStrategy<Value> {
    prop_oneof![
        2 => any::<bool>().prop_map(Value::Bool),
        3 => any::<i64>().prop_map(Value::from),
        1 => any::<u64>().prop_map(Value::from),
        2 => floats.prop_map(Value::from),
        3 => json_string().prop_map(Value::String),
        1 => Just(Value::Null),
    ]
    .boxed()
}

/// Structured JSON: objects with string keys (incl. the empty key and
/// unicode), arrays, nested to `depth`, numbers (i64/u64/f64), strings
/// (incl. empty + unicode), bools, null.
fn json_value(depth: u8, floats: BoxedStrategy<f64>) -> BoxedStrategy<Value> {
    if depth == 0 {
        return json_leaf(floats);
    }
    let inner = json_value(depth - 1, floats.clone());
    let leaf = json_leaf(floats);
    prop_oneof![
        3 => leaf,
        2 => vec((json_string(), inner.clone()), 0..=3)
            .prop_map(|kvs| Value::Object(kvs.into_iter().collect())),
        2 => vec(inner, 0..=3).prop_map(Value::Array),
    ]
    .boxed()
}

type InlineData = Option<Vec<Vec<u8>>>;

/// Inline-data shapes: None, Some(&[]) (the encode-side normalization
/// sentinel), and Some with 0..=8 blobs of 0..=64 bytes (empty blobs
/// inside a non-empty list are legal and roundtrip verbatim).
fn inline_data() -> BoxedStrategy<InlineData> {
    prop_oneof![
        3 => Just(None),
        1 => Just(Some(Vec::new())),
        4 => vec(vec(any::<u8>(), 0..=64), 0..=8).prop_map(Some),
    ]
    .boxed()
}

// --- PSLB: row-group specs ---------------------------------------------------

/// One stat entry's payload: type tag (known VT_* tags AND arbitrary bytes
/// — unknown types must roundtrip and never prune), min/max byte blobs,
/// null_count.
type StatPayload = (u8, Option<Vec<u8>>, Option<Vec<u8>>, u32);

fn vtype_strategy() -> impl Strategy<Value = u8> {
    prop_oneof![
        1 => Just(VT_INT64),
        1 => Just(VT_FLOAT64),
        1 => Just(VT_STRING),
        1 => Just(VT_BOOLEAN),
        1 => any::<u8>(),
    ]
}

/// Arbitrary stat payload — min/max presence INDEPENDENT (covers the
/// half-stat normalization sentinel on every roundtrip law).
fn stat_any() -> BoxedStrategy<StatPayload> {
    (
        vtype_strategy(),
        prop::option::weighted(0.75, vec(any::<u8>(), 0..=8)),
        prop::option::weighted(0.75, vec(any::<u8>(), 0..=8)),
        0u32..=1000,
    )
        .boxed()
}

/// Column names: empty (name_len=0 is representable), short ASCII pool,
/// unicode (byte length ≠ char count — pins name_len counting BYTES), and
/// arbitrary short char soup. Duplicate names across columns are legal.
fn col_name() -> impl Strategy<Value = String> {
    prop_oneof![
        1 => Just(String::new()),
        4 => prop::sample::select(vec![
            "c0".to_string(),
            "c1".to_string(),
            "id".to_string(),
            "ключ".to_string(),
            "列".to_string(),
        ]),
        1 => vec(any::<char>(), 1..=6).prop_map(|cs| cs.into_iter().collect()),
    ]
}

/// One row-group spec: (payload bytes, per-column stats, row count).
/// Payloads include empty RGs; row counts are mostly small with occasional
/// full-range u32 samples.
type RgSpec = (Vec<u8>, Vec<ColumnStatsEntry>, u32);

fn rg_spec_any() -> BoxedStrategy<RgSpec> {
    (
        vec(any::<u8>(), 0..=128),
        vec((col_name(), stat_any()).prop_map(
            |(name, (vt, min, max, nc))| ColumnStatsEntry {
                name,
                value_type: vt,
                min,
                max,
                null_count: nc,
            },
        ), 0..=4),
        prop_oneof![3 => 0u32..=1000, 1 => any::<u32>()],
    )
        .boxed()
}

/// Magic-letter-free RG spec for the truncation law: every byte the
/// encoder copies from the spec (payload, stats min/max, names, vtypes,
/// null_count, row count) is drawn from an alphabet in which "PSLB"
/// cannot appear.
fn rg_spec_magic_free() -> BoxedStrategy<RgSpec> {
    let safe_name = prop::sample::select(vec!["c0".to_string(), "c1".to_string(), "id".to_string()]);
    let safe_vtype = prop::sample::select(vec![VT_INT64, VT_FLOAT64, VT_STRING, VT_BOOLEAN]);
    (
        vec(non_magic_byte(), 0..=64),
        vec(
            (safe_name, safe_vtype).prop_flat_map(|(name, vt)| {
                (
                    Just(name),
                    Just(vt),
                    prop::option::weighted(0.75, vec(non_magic_byte(), 0..=8)),
                    prop::option::weighted(0.75, vec(non_magic_byte(), 0..=8)),
                    0u32..=1000,
                )
            })
            .prop_map(|(name, vt, min, max, nc)| ColumnStatsEntry {
                name,
                value_type: vt,
                min,
                max,
                null_count: nc,
            }),
            0..=3,
        ),
        0u32..=1000,
    )
        .boxed()
}

// ---------------------------------------------------------------------------
// PNPK laws
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(law_config(128))]

    // Law 1 — encode → decode is lossless for every JSON-representable
    // shape: the commit comes back VALUE-equal (numbers keep their exact
    // i64/u64/f64 identity — floats scoped to the precision domain
    // serde_json's default parser round-trips bit-exactly, see FINDING
    // #1; strings keep unicode/control chars; objects and arrays keep
    // their structure), the manifest comes back BYTE-equal (it is opaque
    // to the pack layer), and inline data comes back shape-equal — with
    // Some(&[]) normalized to None on the encode side (has_inline=false
    // when the list is empty: pinned, documented).
    #[test]
    fn law_pnpk_roundtrip_is_lossless_json_representable(
        commit in json_value(3, json_exact_f64()),
        manifest in vec(any::<u8>(), 0..=512),
        inline in inline_data(),
    ) {
        let blob = encode_pack(&commit, &manifest, inline.as_deref());
        let decoded = decode_pack(&blob);
        prop_assert!(decoded.is_some(), "decode must succeed on encode output");
        let (dec_commit, dec_manifest, dec_inline) = decoded.unwrap();
        prop_assert_eq!(dec_commit, commit, "commit must be VALUE-equal");
        prop_assert_eq!(dec_manifest, manifest, "manifest must be BYTE-equal (opaque bytes)");
        let expected: InlineData = match &inline {
            None => None,
            Some(v) if v.is_empty() => None, // Some(&[]) encodes has_inline=false — pinned
            Some(v) => Some(v.clone()),
        };
        prop_assert_eq!(dec_inline, expected, "inline shape-equal under the empty-list normalization");
    }

    // Law 1 (STRICT) — the same roundtrip with UNFILTERED finite f64s.
    // FIXED N+6 (FINDING #1): serde_json's `float_roundtrip` feature is
    // now enabled in core/storage's Cargo.toml — the exact (Grisu/Ryu
    // round-trip) float parser is active, and arbitrary finite f64
    // commits round-trip bit-exactly. The law is UN-ignored and green;
    // the finding record stays in the header + CHANGELOG.
    #[test]
    fn law_pnpk_roundtrip_is_lossless(
        commit in json_value(3, finite_f64()),
        manifest in vec(any::<u8>(), 0..=512),
        inline in inline_data(),
    ) {
        let blob = encode_pack(&commit, &manifest, inline.as_deref());
        let decoded = decode_pack(&blob);
        prop_assert!(decoded.is_some(), "decode must succeed on encode output");
        let (dec_commit, dec_manifest, dec_inline) = decoded.unwrap();
        prop_assert_eq!(dec_commit, commit, "commit must be VALUE-equal for EVERY finite f64");
        prop_assert_eq!(dec_manifest, manifest, "manifest must be BYTE-equal (opaque bytes)");
        let expected: InlineData = match &inline {
            None => None,
            Some(v) if v.is_empty() => None,
            Some(v) => Some(v.clone()),
        };
        prop_assert_eq!(dec_inline, expected);
    }

    // Law 2 — is_pack is a magic-only discriminator: true for EVERY
    // encode_pack output (all shapes), false for arbitrary bytes UNLESS
    // they begin with "PNPK" — lengths < 4 can never match, 4-byte
    // prefixes drawn from a {P,N} alphabet can never spell "PNPK" (no K),
    // and the sibling magics (PMAN/PSLB/PND2) plus a scrambled PNKP are
    // all rejected.
    #[test]
    fn law_pnpk_is_pack_discriminates(
        case in prop_oneof![
            3 => (json_value(2, finite_f64()), vec(any::<u8>(), 0..=32), inline_data())
                .prop_map(|(c, m, i)| (encode_pack(&c, &m, i.as_deref()), true)),
            2 => vec(any::<u8>(), 0..=3).prop_map(|b| (b, false)), // len < 4
            3 => (vec(prop::sample::select(vec![b'P', b'N']), 4), vec(any::<u8>(), 0..=16))
                .prop_map(|(mut p, t)| { p.extend(t); (p, false) }),
            1 => prop::sample::select(vec![
                b"PMAN\x02\x00\x00\x00\x00".to_vec(),
                b"PSLB\x01\x03\x00\x00\x00\x00".to_vec(),
                b"PND2\x01".to_vec(),
                b"PNKP\x02".to_vec(),
            ]).prop_map(|b| (b, false)),
        ],
    ) {
        let (blob, expected) = case;
        prop_assert_eq!(is_pack(&blob), expected, "is_pack must be exactly the magic check");
    }

    // Law 5 — encode is a pure function: repeated encodes are
    // byte-identical (pack hashes are content-addressed — SHA-256 of the
    // bytes — so encode instability would silently break dedup/refs).
    // The version byte is always 2 and the flag byte is 0 for None AND
    // for Some(&[]) (the normalization), 1 for non-empty inline data.
    #[test]
    fn law_pnpk_encode_is_deterministic(
        commit in json_value(3, finite_f64()),
        manifest in vec(any::<u8>(), 0..=128),
        inline in inline_data(),
    ) {
        let e1 = encode_pack(&commit, &manifest, inline.as_deref());
        let e2 = encode_pack(&commit, &manifest, inline.as_deref());
        prop_assert_eq!(&e1, &e2, "encode must be byte-stable (content-addressed hashes)");
        prop_assert_eq!(e1[4], 2, "version byte is 2");
        let expected_flag = match &inline {
            Some(v) if !v.is_empty() => 1u8,
            _ => 0u8,
        };
        prop_assert_eq!(e1[5], expected_flag, "has_inline flag: 0 for None AND Some(&[])");
    }
}

proptest! {
    #![proptest_config(law_config(64))]

    // Law 3 — every STRICT prefix of a valid pack is rejected. Encode
    // emits no trailing padding: the blob ends exactly at the end of the
    // last section, so any strict prefix cuts into a length-checked
    // region (commit JSON bytes, a length field, or inline blob bytes)
    // and the corresponding pos + len > blob.len() guard fires. A prefix
    // that decodes would mean a truncated upload can masquerade as a
    // valid pack. Prefix lengths are sampled (all of them for small
    // blobs; structural edges + deterministic samples for big ones).
    #[test]
    fn law_pnpk_truncation_rejected(
        commit in json_value(2, finite_f64()),
        manifest in vec(any::<u8>(), 0..=64),
        inline in vec(vec(any::<u8>(), 0..=16), 0..=2),
        seed in any::<u64>(),
    ) {
        let blob = encode_pack(&commit, &manifest, Some(&inline));
        for l in sampled_prefix_lens(blob.len(), 16, seed) {
            prop_assert!(
                decode_pack(&blob[..l]).is_none(),
                "strict prefix of len {} (of {}) must be rejected — encode has no trailing padding",
                l, blob.len()
            );
        }
    }

    // Law 4 — fuzz: decode_pack never panics on arbitrary input. Arms:
    // fully arbitrary bytes, magic-prefixed garbage (valid header, random
    // body — random length fields frequently overflow the blob), and
    // crafted absurd length fields (u32::MAX after the header). The
    // decoder must answer Some (a well-formed triple) or None — anything
    // else is a panic and fails the case. On the Some path the commit
    // must be re-serializable JSON (it came from serde_json).
    #[test]
    fn law_pnpk_decode_never_panics_on_arbitrary_bytes(
        blob in prop_oneof![
            4 => vec(any::<u8>(), 0..=48),
            2 => vec(any::<u8>(), 0..=40).prop_map(|tail| {
                let mut b = PNPK_MAGIC.to_vec();
                b.push(2u8);
                b.extend(tail);
                b
            }),
            1 => (
                any::<u8>(),
                prop::sample::select(vec![0u32, 1, 0x7FFF_FFFF, 0xFFFF_FFF0, u32::MAX]),
                vec(any::<u8>(), 0..=16),
            ).prop_map(|(flags, len, tail)| {
                let mut b = PNPK_MAGIC.to_vec();
                b.push(2u8);
                b.push(flags);
                b.extend_from_slice(&len.to_le_bytes());
                b.extend(tail);
                b
            }),
        ],
    ) {
        if let Some((commit, _manifest, _inline)) = decode_pack(&blob) {
            prop_assert!(
                serde_json::to_vec(&commit).is_ok(),
                "a decoded commit must be valid re-serializable JSON"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// PSLB laws
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(law_config(128))]

    // Law 1 — the uncompressed slab roundtrip is lossless: payloads come
    // back BYTE-equal in the same order; footer entries carry rg_index
    // 0..n in order; byte_len equals the payload length AND equals the
    // 4-byte length prefix sitting immediately before byte_offset (the
    // documented payload layout); n_rows survives; stats survive under
    // the half-stat sentinel; and footer.bloom is None — encode_slab
    // never builds a bloom section (pinned; only
    // encode_slab_compressed_with_bloom sets that flag).
    #[test]
    fn law_pslb_roundtrip_is_lossless(specs in vec(rg_spec_any(), 0..=8)) {
        let blob = encode_slab(&specs);
        prop_assert!(is_slab(&blob), "encode output must be detected as a slab");
        prop_assert!(!is_slab_compressed(&blob), "encode_slab is the UNCOMPRESSED encoder");
        // Header shape: magic, version 1, flags = HAS_FOOTER only (0x01).
        prop_assert_eq!(&blob[0..4], &PSLB_MAGIC[..]);
        prop_assert_eq!(blob[4], 1, "version byte is 1");
        prop_assert_eq!(blob[5], 0x01, "flags = HAS_FOOTER only — no compression, no bloom");
        prop_assert_eq!(
            u32::from_le_bytes([blob[6], blob[7], blob[8], blob[9]]) as usize,
            specs.len(),
            "header n_row_groups equals the spec count"
        );

        let slab_dec = decode_slab(&blob);
        prop_assert!(slab_dec.is_some(), "decode must succeed on encode output");
        let slab = slab_dec.unwrap();
        prop_assert_eq!(slab.row_groups.len(), specs.len());
        prop_assert!(slab.footer.bloom.is_none(),
            "encode_slab never writes a bloom section — decode must yield None (pinned)");
        for (i, (payload, stats, n_rows)) in specs.iter().enumerate() {
            prop_assert_eq!(&slab.row_groups[i], payload, "RG payloads byte-equal, same order");
            let e = &slab.footer.entries[i];
            prop_assert_eq!(e.rg_index, i as u32, "entries in rg_index order");
            prop_assert_eq!(e.n_rows, *n_rows, "row counts survive");
            prop_assert_eq!(e.byte_len as usize, payload.len(),
                "footer byte_len is the payload length (uncompressed)");
            let off = e.byte_offset as usize;
            prop_assert_eq!(
                u32::from_le_bytes(blob[off - 4..off].try_into().unwrap()) as usize,
                payload.len(),
                "the payload length prefix at byte_offset-4 matches footer byte_len"
            );
            assert_stats_roundtrip(&e.columns, stats)?;
        }
    }

    // Law 4 — the compressed (PSLB "v2", version byte still 1 + flag
    // bit1) roundtrip. READ of the decoder first: decode_slab returns
    // DECOMPRESSED payloads, and the footer's byte_len stays the
    // COMPRESSED length — so the reader-level invariant is: range-fetch
    // blob[off..off+len] + decompress_rg == the ORIGINAL payload bytes,
    // byte-exactly. Also pins: zstd encode is deterministic
    // (content-addressed slab hashes), the compressed flag is set, the
    // bloom flag is not, and the payload length prefix equals byte_len.
    #[test]
    fn law_pslb_compressed_roundtrip(specs in vec(rg_spec_any(), 0..=8)) {
        let c1 = encode_slab_compressed(&specs);
        let c2 = encode_slab_compressed(&specs);
        prop_assert_eq!(&c1, &c2, "zstd encode must be deterministic (content-addressed hashes)");
        prop_assert!(is_slab(&c1));
        prop_assert!(is_slab_compressed(&c1), "the compressed encoder sets flag bit1");
        prop_assert!(!is_slab_has_bloom(&c1), "encode_slab_compressed sets no bloom flag");

        let slab_dec = decode_slab(&c1);
        prop_assert!(slab_dec.is_some(), "compressed decode must succeed");
        let slab = slab_dec.unwrap();
        prop_assert_eq!(slab.row_groups.len(), specs.len());
        for (i, (payload, stats, n_rows)) in specs.iter().enumerate() {
            prop_assert_eq!(&slab.row_groups[i], payload,
                "decode_slab returns DECOMPRESSED payloads — pinned (byte-exact)");
            let e = &slab.footer.entries[i];
            prop_assert_eq!(e.rg_index, i as u32);
            prop_assert_eq!(e.n_rows, *n_rows);
            // The range-read contract: fetch [off, off+len) and decompress.
            let (off, len) = (e.byte_offset as usize, e.byte_len as usize);
            let fetched = &c1[off..off + len];
            match decompress_rg(fetched) {
                Ok(d) => prop_assert_eq!(d.as_slice(), payload.as_slice(),
                    "range-fetch + decompress_rg must reproduce the ORIGINAL bytes"),
                Err(err) => prop_assert!(false,
                    "decompress_rg failed on encoder output for RG {}: {}", i, err),
            }
            prop_assert_eq!(
                u32::from_le_bytes(c1[off - 4..off].try_into().unwrap()) as usize,
                len,
                "payload length prefix == footer byte_len (the compressed length)"
            );
            assert_stats_roundtrip(&e.columns, stats)?;
        }
    }
}

proptest! {
    #![proptest_config(law_config(64))]

    // Law 2 — the tail/footer invariant the S3 reader's 3-step algorithm
    // depends on: (a) the last 12 bytes decode to (footer_offset, valid);
    // (b) over-long tail input uses the LAST 12 bytes (documented); (c)
    // the footer byte-range [footer_offset, len-12) decodes to the SAME
    // entries as the full decode AND to the offsets recomputed
    // independently from the documented formula byte_offset(i) =
    // 10 + Σ_{j<i}(4 + len_j) + 4; (d) plan_ranges(None) is exactly the
    // entries' ranges — sorted ascending, pairwise disjoint, all inside
    // [PSLB_HEADER_LEN, footer_offset).
    #[test]
    fn law_pslb_tail_invariant(specs in vec(rg_spec_any(), 0..=8)) {
        let blob = encode_slab(&specs);
        let tail_start = blob.len() - PSLB_TAIL_LEN;

        let tail = decode_slab_tail(&blob[tail_start..]);
        prop_assert!(tail.is_some(), "the 12-byte tail must decode");
        let (footer_offset, valid) = tail.unwrap();
        prop_assert!(valid, "the tail magic is the PSLB echo");
        prop_assert!((footer_offset as usize) <= tail_start,
            "the footer offset points inside the blob, before the tail");
        prop_assert_eq!(
            decode_slab_tail(&blob),
            Some((footer_offset, valid)),
            "over-long input must use the LAST 12 bytes (documented contract)"
        );

        let has_bloom = is_slab_has_bloom(&blob);
        let footer_dec = decode_slab_footer(&blob[footer_offset as usize..tail_start], has_bloom);
        prop_assert!(footer_dec.is_some(), "the tail-derived footer byte-range must decode");
        let footer = footer_dec.unwrap();
        let slab_dec = decode_slab(&blob);
        prop_assert!(slab_dec.is_some());
        let slab = slab_dec.unwrap();
        prop_assert!(
            slab_entries_equal(&footer.entries, &slab.footer.entries),
            "range-read path (tail + footer) and full decode agree on every entry"
        );

        // Independent recomputation of the documented offset layout.
        let mut expected_off = PSLB_HEADER_LEN as u64;
        prop_assert_eq!(footer.entries.len(), specs.len());
        for (i, (payload, stats, n_rows)) in specs.iter().enumerate() {
            let e = &footer.entries[i];
            expected_off += 4; // this RG's length prefix
            prop_assert_eq!(e.rg_index, i as u32, "entries in rg_index order");
            prop_assert_eq!(e.byte_offset, expected_off,
                "byte_offset(i) = 10 + Σ(prev rg_len + 4) + 4 — the documented layout");
            prop_assert_eq!(e.byte_len as usize, payload.len());
            prop_assert_eq!(e.n_rows, *n_rows);
            assert_stats_roundtrip(&e.columns, stats)?;
            expected_off += payload.len() as u64;
        }
        prop_assert_eq!(expected_off, footer_offset,
            "the recomputed cursor lands exactly on the footer offset");

        let ranges = footer.plan_ranges(None);
        prop_assert_eq!(ranges.len(), footer.entries.len(),
            "no predicates → every entry planned (full scan)");
        for (i, (off, len)) in ranges.iter().enumerate() {
            prop_assert_eq!(*off, footer.entries[i].byte_offset);
            prop_assert_eq!(*len, footer.entries[i].byte_len);
            prop_assert!(*off >= PSLB_HEADER_LEN as u64, "ranges start at/after the header");
            prop_assert!(
                off + *len as u64 <= footer_offset,
                "ranges end at/before the footer — never reach the tail"
            );
        }
        for w in ranges.windows(2) {
            prop_assert!(
                w[1].0 >= w[0].0 + w[0].1 as u64,
                "planned ranges are sorted ascending and pairwise disjoint"
            );
        }
    }

    // Law 3 — the range-fetch contract: for every planned range of the
    // UNCOMPRESSED slab, blob[off..off+len) is byte-equal to the input RG
    // payload at that index. This is exactly what the S3 reader does with
    // get_blob_range(slab_hash, off, off+len) — any mismatch would feed
    // the PND2 decoder another row group's bytes (silent corruption).
    #[test]
    fn law_pslb_range_fetch_reconstructs(specs in vec(rg_spec_any(), 0..=8)) {
        let blob = encode_slab(&specs);
        let slab_dec = decode_slab(&blob);
        prop_assert!(slab_dec.is_some());
        let slab = slab_dec.unwrap();
        let ranges = slab.footer.plan_ranges(None);
        prop_assert_eq!(ranges.len(), specs.len());
        for (i, (off, len)) in ranges.iter().enumerate() {
            let start = *off as usize;
            let end = start + *len as usize;
            prop_assert_eq!(
                &blob[start..end],
                &specs[i].0[..],
                "range GET [off, off+len) must return RG {}'s ORIGINAL bytes — the S3 read contract",
                i
            );
        }
    }

    // Law 5 — truncation + discrimination. Strict prefixes of a valid
    // slab are rejected: decode_slab reads the tail from the prefix's
    // LAST 12 bytes, which for any strict prefix land inside the payload
    // or footer — so the argument is DETERMINISTIC only when those bytes
    // cannot spell "PSLB". This law therefore draws every spec byte the
    // encoder copies (payload, stats, names, vtypes, counts) from a
    // magic-letter-free alphabet; arbitrary bytes are covered by the
    // fuzz arm below. Also pins: the wrong-tail-magic CONTRACT —
    // decode_slab_tail reports (offset, valid=false) rather than None and
    // the CALLER (decode_slab) rejects; is_slab stays false for
    // non-magic bytes (incl. sub-header lengths and sibling magics); and
    // decode_slab never panics on arbitrary input.
    #[test]
    fn law_pslb_truncation_and_tail_discrimination(
        case in (
            vec(rg_spec_magic_free(), 0..=6),
            prop_oneof![
                2 => vec(any::<u8>(), 0..=9), // shorter than the 10-byte header
                3 => (vec(prop::sample::select(vec![b'P', b'S']), 4), vec(any::<u8>(), 0..=16))
                    .prop_map(|(mut a, b)| { a.extend(b); a }), // {P,S}^4 can't spell PSLB (no L, no B)
                1 => prop::sample::select(vec![
                    b"PNPK\x02\x00\x00\x00\x00".to_vec(),
                    b"PMAN\x02".to_vec(),
                    b"PND2\x01".to_vec(),
                ]),
            ],
            vec(any::<u8>(), 0..=64), // arbitrary fuzz input
            any::<u64>(),
        ),
    ) {
        let (specs, non_slab, garbage, seed) = case;
        let blob = encode_slab(&specs);
        prop_assert!(is_slab(&blob));

        // (a) every strict prefix is rejected.
        for l in sampled_prefix_lens(blob.len(), 16, seed) {
            prop_assert!(
                decode_slab(&blob[..l]).is_none(),
                "strict prefix of len {} (of {}) must be rejected",
                l, blob.len()
            );
        }

        // (b) wrong tail magic: the documented contract is Some((offset,
        // valid=false)) — NOT None — and decode_slab (the caller) rejects.
        let n = blob.len();
        let mut corrupted = blob.clone();
        corrupted[n - PSLB_TAIL_LEN..n - PSLB_TAIL_LEN + 4].copy_from_slice(b"XXXX");
        let tail_value = u64::from_le_bytes(corrupted[n - 8..].try_into().unwrap());
        prop_assert_eq!(
            decode_slab_tail(&corrupted[n - PSLB_TAIL_LEN..]),
            Some((tail_value, false)),
            "wrong magic → (offset, valid=false); the CALLER rejects — decode_slab_tail itself does not"
        );
        prop_assert!(decode_slab(&corrupted).is_none(),
            "decode_slab must reject a slab whose tail magic is wrong");

        // (c) is_slab discrimination on non-magic bytes.
        prop_assert!(!is_slab(&non_slab), "non-magic bytes are not slabs");

        // (d) fuzz: decode_slab answers Some or None on arbitrary input —
        // never a panic (a panic fails the case).
        prop_assert!(
            matches!(decode_slab(&garbage), Some(_) | None),
            "decode_slab must not panic on arbitrary bytes"
        );
    }

    // Law 6 — plan_ranges pruning is CONSERVATIVE: for a single i64
    // column with honest zone maps, the plan keeps EXACTLY the RGs whose
    // [min,max] can't prove exclusion of the probe (op "=": keep iff
    // min ≤ v ≤ max). Pins BOTH spec-mandated cases explicitly: a probe
    // inside every range keeps ALL RGs (no false drops — the read-side
    // correctness invariant), and a probe outside every range prunes to
    // exactly the RGs whose ranges don't cover it. RGs with no usable
    // stats (empty columns / a foreign column / a half-stat) are NEVER
    // pruned. bloom is None for encode_slab output — pure zone map.
    #[test]
    fn law_pslb_plan_ranges_pruning_is_conservative(
        scenario in pruning_scenario(),
    ) {
        let (rgs, probe) = scenario;
        let specs: Vec<RgSpec> = rgs.iter().map(prune_rg_to_spec).collect();
        let blob = encode_slab(&specs);
        let slab_dec = decode_slab(&blob);
        prop_assert!(slab_dec.is_some());
        let slab = slab_dec.unwrap();
        prop_assert!(slab.footer.bloom.is_none(),
            "pure zone-map precondition — encode_slab writes no bloom");

        let preds = vec![("id".to_string(), "=".to_string(), probe.to_le_bytes().to_vec())];
        let planned = slab.footer.plan_ranges(Some(&preds));

        let mut expected: Vec<(u64, u32)> = Vec::new();
        for (i, rg) in rgs.iter().enumerate() {
            let keep = match rg.range {
                None => true, // no usable stats → must be kept (conservatism)
                Some((lo, hi)) => lo <= probe && probe <= hi, // "=" exclusion rule
            };
            if keep {
                expected.push((slab.footer.entries[i].byte_offset, slab.footer.entries[i].byte_len));
            }
        }
        prop_assert_eq!(&planned, &expected,
            "the plan must keep EXACTLY the RGs whose zone maps can't prove exclusion");

        // Explicit pin 1: probe inside every usable range (and all other
        // RGs stats-less) → keep ALL — a single false drop here is silent
        // data loss on the read path.
        let all_cover = rgs.iter().all(|rg| match rg.range {
            None => true,
            Some((lo, hi)) => lo <= probe && probe <= hi,
        });
        if all_cover {
            prop_assert_eq!(planned.len(), rgs.len(),
                "probe inside EVERY range → keep ALL RGs (no false drops)");
        }

        // Explicit pin 2: probe outside every usable range and every RG
        // has stats → prune to exactly the RGs whose ranges don't cover
        // it — i.e. all of them; the plan is empty.
        let all_exclude = rgs
            .iter()
            .all(|rg| matches!(rg.range, Some((lo, hi)) if probe < lo || probe > hi));
        if all_exclude {
            prop_assert!(planned.is_empty(),
                "probe outside every range → every RG provably excluded → empty plan");
        }
    }
}

// ---------------------------------------------------------------------------
// Law 6 scenario generation (deterministic from one seed via splitmix64)
// ---------------------------------------------------------------------------

/// One RG in the pruning scenario: an honest i64 zone-map range for column
/// "id", or None for a stats-less RG (which must NEVER be pruned).
#[derive(Debug, Clone)]
struct PruneRg {
    range: Option<(i64, i64)>,
    statsless_kind: u8, // 0 = no columns, 1 = foreign column, 2 = half-stat
    payload: Vec<u8>,
    n_rows: u32,
}

prop_compose! {
    /// A pruning scenario: 1..=6 RGs and a probe value in one of three
    /// modes — 0: probe inside every range (keep-all), 1: probe strictly
    /// below every range (exclude-all), 2: mixed arbitrary ranges around
    /// the probe. ~20% of RGs lose their stats (conservatism arm).
    fn pruning_scenario()(n in 1usize..=6, mode in 0u8..3, seed in any::<u64>())
        -> (Vec<PruneRg>, i64) {
        let mut sm = seed | 1;
        // The probe is drawn FIRST — mode 0 builds every range around it.
        let probe = match mode {
            0 => sm_range(&mut sm, -1000, 1000),
            1 => sm_range(&mut sm, -1000, 999),
            _ => sm_range(&mut sm, -2500, 2500),
        };
        let mut rgs = Vec::with_capacity(n);
        for _ in 0..n {
            let range = match mode {
                0 => {
                    let a = sm_range(&mut sm, 0, 100);
                    let b = sm_range(&mut sm, 0, 100);
                    Some((probe - a, probe + b))
                }
                1 => {
                    let lo = sm_range(&mut sm, 1000, 1500);
                    let hi = sm_range(&mut sm, lo, 2000);
                    Some((lo, hi))
                }
                _ => {
                    let lo = sm_range(&mut sm, -2000, 1999);
                    let hi = sm_range(&mut sm, lo, 2000);
                    Some((lo, hi))
                }
            };
            let statsless = splitmix64(&mut sm).is_multiple_of(5);
            let kind = (splitmix64(&mut sm) % 3) as u8;
            let payload_len = (splitmix64(&mut sm) % 17) as usize;
            let payload: Vec<u8> = (0..payload_len)
                .map(|_| splitmix64(&mut sm) as u8)
                .collect();
            let n_rows = (splitmix64(&mut sm) % 1001) as u32;
            rgs.push(PruneRg {
                range: if statsless { None } else { range },
                statsless_kind: kind,
                payload,
                n_rows,
            });
        }
        (rgs, probe)
    }
}

// ---------------------------------------------------------------------------
// Adversarial companions (deterministic #[test]s — the shapes the fuzz
// laws can't reliably reach)
// ---------------------------------------------------------------------------

/// Hand-craft a PNPK blob so the decoder arms the encoder can never
/// produce are reachable (version 3, absurd lens, count mismatches).
fn craft_pack(
    version: u8,
    flags: u8,
    commit_json: &[u8],
    manifest: &[u8],
    inline: Option<&[Vec<u8>]>,
) -> Vec<u8> {
    let mut b = PNPK_MAGIC.to_vec();
    b.push(version);
    b.push(flags);
    b.extend_from_slice(&(commit_json.len() as u32).to_le_bytes());
    b.extend_from_slice(commit_json);
    b.extend_from_slice(&(manifest.len() as u32).to_le_bytes());
    b.extend_from_slice(manifest);
    if let Some(blobs) = inline {
        b.extend_from_slice(&(blobs.len() as u16).to_le_bytes());
        for blob in blobs {
            b.extend_from_slice(&(blob.len() as u32).to_le_bytes());
            b.extend_from_slice(blob);
        }
    }
    b
}

/// Version discrimination: versions ∉ {1, 2} are rejected; version 1 is
/// ACCEPTED (backward compat) and — with the inline flag set — its inline
/// section is SKIPPED (`has_inline && version >= 2`), decoding to None.
/// is_pack stays true for ALL of them: it never looks at the version.
#[test]
fn pnpk_adversarial_version_rejected() {
    for v in [0u8, 1, 3, 255] {
        let blob = craft_pack(v, 0, br#"{"m":"x"}"#, b"PMAN", None);
        if v == 1 {
            assert!(decode_pack(&blob).is_some(), "version 1 is backward-compatible");
        } else {
            assert!(decode_pack(&blob).is_none(), "version {} must be rejected", v);
        }
        assert!(is_pack(&blob), "is_pack is magic-only — version is not part of the discriminator");
    }

    // v1 + inline flag: the inline section is skipped, decode yields None.
    let blob = craft_pack(1, 0x01, br#"{"m":"x"}"#, b"PMAN", Some(&[b"data".to_vec()]));
    let (commit, _manifest, inline) = decode_pack(&blob).expect("v1 decodes");
    assert_eq!(commit["m"], "x");
    assert!(inline.is_none(), "v1 packs carry no inline data (version-gated)");
}

/// Absurd section lengths (u32::MAX) are cleanly rejected on 64-bit:
/// `pos + len` cannot overflow usize, so the `pos + len > blob.len()`
/// guard fires. 32-BIT HAZARD (documented for the owner, untestable on
/// this 64-bit host): on 32-bit targets `pos + u32::MAX` WRAPS usize —
/// debug builds panic on the overflow, release builds wrap past the
/// guard and the slice op panics. Same class for decode_slab's
/// `byte_offset as usize` (u64 → usize truncation).
#[test]
fn pnpk_adversarial_absurd_section_len_rejected() {
    // commit_len = u32::MAX directly after the header.
    let mut b = PNPK_MAGIC.to_vec();
    b.push(2);
    b.push(0);
    b.extend_from_slice(&u32::MAX.to_le_bytes());
    b.extend_from_slice(b"tail");
    assert!(decode_pack(&b).is_none(), "commit_len = u32::MAX must be rejected");

    // Valid commit section, then manifest_len = u32::MAX.
    let commit = br#"{"a":1}"#;
    let mut b = PNPK_MAGIC.to_vec();
    b.push(2);
    b.push(0);
    b.extend_from_slice(&(commit.len() as u32).to_le_bytes());
    b.extend_from_slice(commit);
    b.extend_from_slice(&u32::MAX.to_le_bytes());
    assert!(decode_pack(&b).is_none(), "manifest_len = u32::MAX must be rejected");
}

/// Commit-JSON edge cases: empty and unparseable JSON reject; valid JSON
/// that is NOT an object is ACCEPTED verbatim — the decoder's type is
/// serde_json::Value and the format contract only requires parseable
/// JSON (production writers always emit commit objects). This leniency
/// is the actual documented-by-implementation contract, pinned here.
#[test]
fn pnpk_adversarial_commit_json_shapes() {
    assert!(
        decode_pack(&craft_pack(2, 0, b"", b"PMAN", None)).is_none(),
        "empty commit JSON is not valid JSON — must reject"
    );
    assert!(decode_pack(&craft_pack(2, 0, b"{", b"PMAN", None)).is_none());
    assert!(decode_pack(&craft_pack(2, 0, b"\x00\x01garbage", b"PMAN", None)).is_none());

    for json in [br#"42"#.as_slice(), br#"[1,2]"#, br#""s""#, b"null", b"true", b"-0.5"] {
        let blob = craft_pack(2, 0, json, b"PMAN", None);
        let (commit, manifest, inline) = decode_pack(&blob)
            .unwrap_or_else(|| panic!("valid non-object JSON {:?} must decode (lenient contract)", json));
        assert_eq!(manifest, b"PMAN");
        assert!(inline.is_none());
        assert_eq!(commit, serde_json::from_slice::<Value>(json).unwrap());
    }
}

/// u16 inline count mismatch: a count claiming MORE blobs than present is
/// a section overrun → None. A count of ZERO decodes to Some(vec![]) — a
/// hand-crafted shape the encoder never produces (it normalizes Some(&[])
/// to has_inline=false) — and demonstrates the decoder's documented
/// trailing-bytes tolerance (blob0's orphaned bytes are ignored).
#[test]
fn pnpk_adversarial_inline_count_mismatch() {
    let commit = br#"{"i":0}"#;
    let inline = vec![b"blob0".to_vec()];
    let blob = encode_pack(
        &serde_json::from_slice::<Value>(commit).unwrap(),
        b"PMAN",
        Some(&inline),
    );
    // n_blobs sits right after the manifest section.
    let n_pos = 6 + 4 + commit.len() + 4 + b"PMAN".len();
    assert_eq!(u16::from_le_bytes([blob[n_pos], blob[n_pos + 1]]), 1);

    let mut mism = blob.clone();
    mism[n_pos..n_pos + 2].copy_from_slice(&3u16.to_le_bytes());
    assert!(
        decode_pack(&mism).is_none(),
        "u16 inline count > present blobs must be rejected (section overrun)"
    );

    let mut zeroed = blob.clone();
    zeroed[n_pos..n_pos + 2].copy_from_slice(&0u16.to_le_bytes());
    let (_, _, dec_inline) = decode_pack(&zeroed).unwrap();
    assert_eq!(dec_inline, Some(Vec::new()));
}

/// is_pack does NOT mean decodable: 4 magic bytes alone pass the check
/// while decode_pack needs ≥ 10. (Contrast is_slab, which requires the
/// full 10-byte header LENGTH — pinned in pslb_is_slab_discriminates.)
#[test]
fn pnpk_is_pack_is_magic_only() {
    assert!(is_pack(b"PNPK"));
    assert!(decode_pack(b"PNPK").is_none());
    assert!(!is_pack(b"PNP"));
    assert!(!is_pack(b"pnkp"));
}

/// The tail-magic contract, end to end: decode_slab_tail reports
/// (footer_offset, valid=false) for a wrong magic — it does NOT return
/// None (None is reserved for inputs shorter than 12 bytes) — and the
/// CALLER (decode_slab) is the one who rejects. Also pins the
/// last-12-bytes rule for over-long inputs.
#[test]
fn pslb_tail_magic_contract() {
    let blob = encode_slab(&[(b"rg0".to_vec(), vec![], 1)]);
    let n = blob.len();
    let (footer_offset, valid) = decode_slab_tail(&blob[n - PSLB_TAIL_LEN..]).unwrap();
    assert!(valid);
    assert!(footer_offset > 0);

    let mut corrupted = blob.clone();
    corrupted[n - PSLB_TAIL_LEN..n - PSLB_TAIL_LEN + 4].copy_from_slice(b"XXXX");
    assert_eq!(
        decode_slab_tail(&corrupted[n - PSLB_TAIL_LEN..]),
        Some((footer_offset, false)),
        "wrong magic → (offset, false); the caller rejects, not the tail decoder"
    );
    assert!(decode_slab(&corrupted).is_none(), "decode_slab must reject bad tail magic");

    assert!(decode_slab_tail(&blob[n - 11..]).is_none(), "< 12 bytes → None");
    assert!(decode_slab_tail(b"").is_none());
    assert_eq!(decode_slab_tail(&blob), Some((footer_offset, true)),
        "whole-blob input uses the LAST 12 bytes (documented)");
}

/// is_slab requires the full 10-byte header LENGTH, not just the magic
/// (contrast is_pack, which accepts a bare 4-byte magic) — and stays
/// false for sibling magics.
#[test]
fn pslb_is_slab_discriminates() {
    assert!(!is_slab(b"PSLB"), "4 bytes — too short");
    assert!(!is_slab(b"PSLB\x01\x01\x00\x00\x00"), "9 bytes — still short of the 10-byte header");
    assert!(is_slab(&encode_slab(&[])));
    for magic in [&b"PNPK"[..], b"PMAN", b"PND2"] {
        assert!(!is_slab(magic), "{:?} is not a slab", magic);
    }
}

/// The header/footer count cross-check: header n_row_groups must equal
/// footer n_entries or the slab is rejected.
#[test]
fn pslb_header_footer_count_mismatch_rejected() {
    let blob = encode_slab(&[(b"rg".to_vec(), vec![], 5), (b"rg".to_vec(), vec![], 5)]);
    let mut mism = blob.clone();
    mism[6..10].copy_from_slice(&7u32.to_le_bytes());
    assert!(
        decode_slab(&mism).is_none(),
        "header n_row_groups must equal footer n_entries"
    );
}

/// The PMAN stats sentinel, pinned at the slab layer: a (Some, None)
/// half-stat encodes has_stats=0 and decodes as (None, None); name,
/// vtype and null_count survive the normalization.
#[test]
fn pslb_half_stats_normalize_to_no_stats() {
    let stats = vec![ColumnStatsEntry {
        name: "id".to_string(),
        value_type: VT_INT64,
        min: Some(1i64.to_le_bytes().to_vec()),
        max: None,
        null_count: 3,
    }];
    let blob = encode_slab(&[(b"rg".to_vec(), stats, 1)]);
    let slab = decode_slab(&blob).unwrap();
    let col = &slab.footer.entries[0].columns[0];
    assert_eq!(col.min, None);
    assert_eq!(col.max, None);
    assert_eq!(col.null_count, 3, "null_count survives the half-stat normalization");
    assert_eq!(col.name, "id");
}

/// Build the RgSpec for one pruning-scenario RG: an honest i64 zone map
/// for column "id", or one of three stats-less shapes (empty columns /
/// a foreign column with full stats / a half-stat on "id") — all of
/// which must be conservative-kept.
fn prune_rg_to_spec(rg: &PruneRg) -> RgSpec {
    let stats = match rg.range {
        Some((lo, hi)) => vec![ColumnStatsEntry {
            name: "id".to_string(),
            value_type: VT_INT64,
            min: Some(lo.to_le_bytes().to_vec()),
            max: Some(hi.to_le_bytes().to_vec()),
            null_count: 0,
        }],
        None => match rg.statsless_kind % 3 {
            0 => vec![], // no columns at all
            1 => vec![ColumnStatsEntry {
                // a foreign column — "id" is absent from this RG
                name: "other".to_string(),
                value_type: VT_INT64,
                min: Some(i64::MIN.to_le_bytes().to_vec()),
                max: Some(i64::MAX.to_le_bytes().to_vec()),
                null_count: 0,
            }],
            _ => vec![ColumnStatsEntry {
                // half-stat on "id": min XOR max → can_prune must bail
                name: "id".to_string(),
                value_type: VT_INT64,
                min: Some(0i64.to_le_bytes().to_vec()),
                max: None,
                null_count: 7,
            }],
        },
    };
    (rg.payload.clone(), stats, rg.n_rows)
}

/// REAL BUG (documented, NOT fixed — read-only tree per the builder
/// contract): `decode_slab_footer` runs `Vec::with_capacity(n_entries)`
/// on the UNVALIDATED u32 entry count read from the footer bytes
/// (core/storage/src/slab.rs, decode_slab_footer, the line directly
/// after the n_entries read). A corrupted or malicious slab whose footer
/// starts with FF FF FF FF therefore attempts a ~192 GiB allocation
/// (u32::MAX × sizeof(SlabEntry) ≈ 48 B) BEFORE the per-entry bounds
/// checks (which would reject immediately) can run — the allocator
/// refuses, and Rust's handle_alloc_error ABORTS the process (allocation
/// failure does not unwind), violating the format's own C1/C2 contract
/// that malformed input returns None and never crashes. Reachable from
/// any reader that decodes a footer (decode_slab → decode_slab_footer)
/// and directly via the public decode_slab_footer.
///
/// Minimal reproducers (each aborts on default-overcommit Linux):
///   // via decode_slab:
///   let mut blob = pond_storage::slab::encode_slab(&[(b"x".to_vec(), vec![], 1)]);
///   let n = blob.len();
///   let (off, _) = pond_storage::slab::decode_slab_tail(&blob[n - 12..]).unwrap();
///   blob[off as usize..off as usize + 4].copy_from_slice(&u32::MAX.to_le_bytes());
///   pond_storage::slab::decode_slab(&blob);              // ← process ABORTS
///   // via the footer API directly:
///   pond_storage::slab::decode_slab_footer(&[0xFF; 4], false); // ← same abort
///
/// FIXED N+6 (FINDING #2): decode_slab_footer now validates
/// `n_entries <= footer_len / 21` BEFORE the with_capacity — oversized
/// counts are rejected with None. The law is UN-ignored and green.
#[test]
fn pslb_decode_must_not_abort_on_huge_footer_n_entries() {
    let mut blob = encode_slab(&[(b"x".to_vec(), vec![], 1)]);
    let n = blob.len();
    let (footer_offset, _) = decode_slab_tail(&blob[n - PSLB_TAIL_LEN..]).unwrap();
    blob[footer_offset as usize..footer_offset as usize + 4]
        .copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(
        decode_slab(&blob).is_none(),
        "a footer claiming u32::MAX entries must be rejected, not trigger a giant allocation"
    );
    assert!(
        decode_slab_footer(&[0xFF; 4], false).is_none(),
        "decode_slab_footer must not allocate from an unvalidated entry count"
    );
}
