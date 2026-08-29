# Builder Spec — C12: PNPK + PSLB Codec Proptest Laws (Task cron-2026-08-30-b)

You are the CODEC BUILDER for Pond2 at /home/z/Pond-review.
Your contract: ACCEPTANCE.md §"This-cycle acceptance (crucible iteration
N+6)" item 4 ONLY (a new proptest file + whatever minimal plumbing it
needs). The orchestrator owns everything else.

Before starting, read /home/z/my-project/worklog.md (last two cycles) for
context. READ-ONLY for all files except: the NEW test file you create and
(only if strictly needed) the proptest regressions file it generates.
DO NOT touch KNOWLEDGE_GRAPH.md, ACCEPTANCE.md, CRITIQUE.md, or any src/
file — another builder is doing a wide refactor in this same tree
(core/kernel, core/s3, core/cache, core/storage/src/*, pyo3); stay out of
their way. If you find a codec BUG while writing laws, do NOT fix it —
document it precisely in your report (location + minimal reproducer) and
write the law in a way that documents current behavior honestly (see
"bugs" below).

## Context

The C3 laws cycle (commit 199e598) landed property suites for CRDT,
journal, and PMAN (core/storage/tests/laws_pman.rs — 4 proptest laws +
boundary tests). Two binary codecs remain at ZERO property coverage —
they're load-bearing for every read (PNPK: every commit/manifest blob
pair; PSLB: every slab) and their failure modes are silent corruption:

- **PNPK** — core/storage/src/pond_pack.rs: `encode_pack(commit: &Value,
  manifest_bytes: &[u8], inline_data: Option<&[Vec<u8>]>) -> Vec<u8>`;
  `decode_pack(blob) -> Option<(Value, Vec<u8>, Option<Vec<Vec<u8>>>)>`;
  `is_pack(blob) -> bool`. Format: magic "PNPK" + version 2 + flags +
  len-prefixed commit JSON + len-prefixed manifest + optional
  len-prefixed inline blobs. decode rejects: short blobs, bad magic,
  versions not in {1,2}, section overruns, invalid commit JSON.
- **PSLB** — core/storage/src/slab.rs: `encode_slab(row_groups:
  &[(Vec<u8>, Vec<ColumnStatsEntry>, u32)]) -> Vec<u8>` (uncompressed),
  `encode_slab_compressed(...)` (zstd per-RG), `decode_slab(blob) ->
  Option<Slab>` (Slab { row_groups: Vec<Vec<u8>>, footer: SlabFooter }),
  `decode_slab_tail(tail) -> Option<(u64, bool)>` (last 12 bytes),
  `decode_slab_footer(footer_bytes, has_bloom_flag) ->
  Option<SlabFooter>`, `is_slab`, `is_slab_compressed`, `decompress_rg`,
  `SlabFooter::plan_ranges(predicates) -> Vec<(u64, u32)>`,
  `build_bloom(columns)`. Read the WHOLE file first — footer layout,
  tail magic, bloom section, and the compressed path have precise
  contracts documented inline.

## Deliverable — core/storage/tests/laws_pnps.rs

Follow the laws_pman.rs patterns EXACTLY (read it first):
- `fn law_config(cases: u32) -> ProptestConfig` with deterministic
  settings + the same fork/retry posture laws_pman uses.
- Boxed strategies for expensive types; sized-down collections (keep
  cases fast — the whole file should run in seconds, not minutes).
- A comment header explaining what each law pins and why it matters
  (one paragraph per law, in the voice of the existing laws files).

### PNPK laws (prefix `law_pnpk_`)

1. **roundtrip_is_lossless**: for arbitrary serde_json Values (generate
   structured JSON — objects with string keys, nested values, arrays,
   numbers, strings incl. empty + unicode), arbitrary manifest bytes
   (0..=512, include empty), and inline data shapes (None, Some(&[]),
   Some with 0..=8 blobs of 0..=64 bytes): decode_pack(encode_pack(..))
   → Some, commit value-equal, manifest BYTE-equal, inline shape-equal
   (Some(&[]) encodes to has_inline=false — decode yields None: pin that
   explicitly, it's a documented encode-side normalization).
2. **is_pack_discriminates**: is_pack(encode_pack(..)) for all shapes;
   is_pack is FALSE for arbitrary bytes UNLESS they begin with the magic
   (generate non-magic first-4-bytes strategies: shuffle a non-PNPK
   alphabet, also test len<4).
3. **truncation_rejected**: every STRICT prefix of a valid pack (test
   prefix lens from 0..blob.len()-1 — sample them, don't enumerate huge
   blobs) → decode_pack returns None. Rationale: encode has no trailing
   padding, so any strict prefix loses ≥1 required byte.
4. **decode_never_panics_on_arbitrary_bytes**: fuzz-style law —
   decode_pack on arbitrary bytes (incl. magic-prefixed garbage, absurd
   length fields via u32::MAX crafted after the magic) never panics:
   returns None or a valid triple. (Craft a few targeted adversarial
   cases as regular #[test]s alongside: magic + version 3, section lens
   that overflow usize on 32-bit, empty commit JSON, commit JSON that's
   valid JSON but not an object, u16 inline count mismatch.)
5. **encode_is_deterministic**: encode twice → byte-identical; and
   encode(commit, manifest, None) has_inline flag byte == 0.

### PSLB laws (prefix `law_pslb_`)

Generate row-group specs: 0..=8 RGs, each 0..=128 bytes of arbitrary
payload (include empty RGs), row counts 0..=u32::MAX-sampled-small,
per-column stats built from ColumnStatsEntry's own constructors — read
manifest.rs for the legal shapes (name, kind, min/max encodings) and
reuse laws_pman's stat_payload strategies where they fit.

1. **roundtrip_is_lossless**: encode_slab → is_slab true, is_slab_
   compressed false; decode_slab → Some: row_groups BYTE-equal (same
   order), footer entries match (rg_index order, byte_offset/byte_len/
   n_rows, column stats equal), footer.bloom None for the uncompressed
   encoder unless it builds one (READ the encoder: if encode_slab never
   sets bloom, assert None and pin it).
2. **tail_invariant**: for a valid slab, decode_slab_tail(&blob[len-
   PSLB_TAIL_LEN..]) → Some((footer_len, has_bloom)) where the footer
   byte-range [len - PSLB_TAIL_LEN - footer_len, len - PSLB_TAIL_LEN)
   decodes via decode_slab_footer to the SAME entries. And: the planned
   ranges (plan_ranges(None)) are exactly the encoded entries' ranges —
   sorted ascending, disjoint, all within [PSLB_HEADER_LEN, footer
   start).
3. **range_fetch_reconstructs**: for each planned range, blob[off..
   off+len] (from the UNCOMPRESSED slab) is byte-equal to the input RG
   payload at that index (the range-read contract the S3 reader relies
   on).
4. **compressed_roundtrip**: encode_slab_compressed → is_slab true,
   is_slab_compressed true; the decoded footer entries' byte_lens match;
   decompress_rg on each decoded RG payload (READ decode_slab +
   decompress_rg first — determine whether decode_slab returns
   compressed or decompressed payloads, and write the law at the level
   the READERS actually use: the invariant is that the round trip
   through encode+decode+decompress_rg reproduces the ORIGINAL payload
   bytes byte-exactly).
5. **truncation_and_tail_discrimination**: strict prefixes of a valid
   slab → decode_slab None (sample prefix lens); a tail whose magic is
   wrong → decode_slab_tail None; is_slab false for arbitrary non-magic
   bytes; decode never panics on arbitrary input (fuzz-style).
6. **plan_ranges_pruning_is_conservative**: construct stats with known
   min/max for one integer column (read ColumnStatsEntry for the
   encoding): plan_ranges with a predicate INSIDE the min/max range must
   keep ALL RGs whose stats can't prove exclusion (no false drops — the
   read-side correctness invariant), and a predicate OUTSIDE every RG's
   [min,max] prunes to exactly the RGs whose ranges don't cover it. If
   bloom is None this is pure zone-map — pin BOTH the keep-all case and
   a provable-exclusion case.

### Bugs

If a law FAILS against current code: first re-read the encoder/decoder —
if the "law" contradicts a documented intentional behavior, fix the LAW
and document the actual contract in its comment. If it's a REAL bug
(silent corruption, panic, false drop in plan_ranges), do not fix and do
not #[ignore]: write the law to pass against a BUG-FREE implementation,
mark it #[ignore] with a comment pointing at the bug, and report the
minimal reproducer prominently.

## Validation

```
cd /home/z/Pond-review
cargo test -p pond_storage --test laws_pnps            # your file, green
cargo test -p pond_storage --test laws_pman            # untouched, green
cargo clippy -p pond_storage --all-targets -- -D warnings
```
NOTE: another builder is running cargo in this same tree — the target
lock may block you for minutes. Use long timeouts (10 min) and retry
once if a command dies on lock acquisition. Do NOT run the full
workspace suite — the other builder's refactor may be mid-flight; the
orchestrator runs the full validation at integration.

## Report back (your single final message)

1. The laws you landed (names + one-line intent each).
2. Any discrepancy between documented format contracts and actual
   encode/decode behavior you discovered while reading.
3. Any REAL bugs found (reproducers) or explicit "no bugs found".
4. Validation output (test counts, clippy).
5. The proptest regressions file path if any cases shrank to interesting
   seeds (and what the seed exercises).
