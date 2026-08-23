// search.rs — Hybrid (BM25 + vector) search.
//
// This module provides:
//   - A pure-Rust BM25 scorer (k1=1.2, b=0.75, Lucene-style IDF tweak)
//   - A simple tokenizer (lowercase + split on whitespace/punctuation +
//     English stop-word removal)
//   - Reciprocal Rank Fusion (RRF, k=60) for combining ranked lists
//   - `hybrid_search` — the high-level entry point that:
//       1. scores each row by vector distance (L2 / cosine / dot) on a
//          named vector column
//       2. scores each row by BM25 across one or more text columns
//       3. ranks the two lists independently
//       4. fuses them with RRF using per-modality weights
//       5. returns the top-k hits as `SearchHit { row, score,
//          vector_distance, text_score }`
//
// The module is intentionally self-contained: it depends only on
// `serde_json` (for representing rows as JSON values) and on the
// SIMD-accelerated vector distance functions already in `vector.rs`.
// It does NOT depend on pond_kernel / pond_storage — callers feed in
// already-decoded rows. This keeps the search logic portable across
// storage backends and testable in isolation.

use crate::vector::{cosine_distance, dot_product, l2_distance};
use serde_json::Value as JsonValue;
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// A single hybrid-search result.
///
/// `score` is the fused RRF score (higher = better). `vector_distance` and
/// `text_score` are the per-modality contributions — `None` when the
/// corresponding modality was not used for this query.
#[derive(Debug, Clone)]
pub struct SearchHit {
    /// The original row, verbatim.
    pub row: JsonValue,
    /// Fused RRF score (1/(k+rank_vec) weighted + 1/(k+rank_text) weighted).
    pub score: f64,
    /// Vector distance to the query (lower = closer). None when no vector
    /// modality was requested.
    pub vector_distance: Option<f64>,
    /// BM25 score (higher = more relevant). None when no text modality was
    /// requested.
    pub text_score: Option<f64>,
}

/// Weights for the vector and text modalities. Both default to 1.0
/// (equal contribution). Weights are NOT required to sum to 1 — they're
/// applied as multipliers on the RRF contributions.
#[derive(Debug, Clone, Copy)]
pub struct SearchWeights {
    pub vector: f64,
    pub text: f64,
}

impl Default for SearchWeights {
    fn default() -> Self {
        Self { vector: 1.0, text: 1.0 }
    }
}

// ---------------------------------------------------------------------------
// Tokenizer
// ---------------------------------------------------------------------------

/// A small set of English stop words. Removing them avoids over-indexing
/// on common words ("the", "a", "is") that carry little signal for BM25.
const STOP_WORDS: &[&str] = &[
    "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "from", "has", "have",
    "he", "in", "is", "it", "its", "of", "on", "or", "that", "the", "this", "to", "was",
    "were", "will", "with", "i", "you", "we", "they", "them", "his", "her", "she", "him",
    "not", "no", "do", "does", "did", "had", "been", "being", "am", "if", "then", "else",
    "when", "where", "which", "who", "whom", "what", "why", "how", "all", "any", "each",
    "few", "more", "most", "other", "some", "such", "only", "own", "same", "so", "than",
    "too", "very", "can", "just", "should", "now",
];

/// Tokenize a piece of text for BM25 indexing / querying.
///
/// Rules (matching Lucene's "Standard" analyzer's intent, simplified):
///   1. Lowercase (case-insensitive matching)
///   2. Split on whitespace and punctuation (anything that's not a letter,
///      digit, or underscore starts a new token)
///   3. Drop empty tokens
///   4. Drop stop words
///   5. Drop single-character tokens (length < 2)
///
/// Returns the list of remaining tokens, in order of appearance.
pub fn tokenize(text: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    for c in text.chars() {
        if c.is_alphanumeric() || c == '_' {
            for lc in c.to_lowercase() {
                current.push(lc);
            }
        } else {
            if !current.is_empty() {
                push_token(&mut tokens, &current);
                current.clear();
            }
        }
    }
    if !current.is_empty() {
        push_token(&mut tokens, &current);
    }
    tokens
}

fn push_token(out: &mut Vec<String>, tok: &str) {
    if tok.len() < 2 {
        return;
    }
    if STOP_WORDS.contains(&tok) {
        return;
    }
    out.push(tok.to_string());
}

// ---------------------------------------------------------------------------
// BM25
// ---------------------------------------------------------------------------

/// BM25 scoring state.
///
/// BM25 is the standard ranking function for text search. For a document
/// `d` and query `q`:
///
/// ```text
/// score(d, q) = Σ_{t ∈ q} IDF(t) * (f(t,d) * (k1 + 1))
///                              / (f(t,d) + k1 * (1 - b + b * |d| / avgdl))
/// ```
///
/// where:
///   - `f(t, d)` is the term frequency of `t` in `d`
///   - `|d|` is the document length (token count)
///   - `avgdl` is the average document length across the corpus
///   - `IDF(t) = ln(1 + (N - n(t) + 0.5) / (n(t) + 0.5))`  (Lucene tweak)
///   - `k1 = 1.2`, `b = 0.75` (defaults)
///
/// The Lucene IDF tweak (the `1 +` inside `ln`) guarantees non-negative
/// IDF even for terms that appear in every document, which prevents
/// extremely common terms from getting a negative contribution.
pub struct Bm25 {
    /// Number of documents in the corpus.
    n_docs: usize,
    /// Average document length (in tokens).
    avgdl: f64,
    /// For each term: number of documents that contain it.
    doc_freq: HashMap<String, usize>,
    /// Per-document token frequencies: doc_idx -> (term -> tf).
    doc_tfs: Vec<HashMap<String, u32>>,
    /// Per-document length (token count).
    doc_lens: Vec<usize>,
    /// BM25 k1 parameter (term-frequency saturation).
    k1: f64,
    /// BM25 b parameter (length normalization).
    b: f64,
}

impl Bm25 {
    /// Create a new BM25 index with default parameters (k1=1.2, b=0.75).
    pub fn new() -> Self {
        Self::with_params(1.2, 0.75)
    }

    /// Create a new BM25 index with custom k1 and b parameters.
    pub fn with_params(k1: f64, b: f64) -> Self {
        Self {
            n_docs: 0,
            avgdl: 0.0,
            doc_freq: HashMap::new(),
            doc_tfs: Vec::new(),
            doc_lens: Vec::new(),
            k1,
            b,
        }
    }

    /// Add a document (already tokenized) to the index.
    ///
    /// Returns the internal document index (0-based) for later scoring.
    pub fn add_document(&mut self, tokens: &[String]) -> usize {
        let doc_idx = self.n_docs;
        self.n_docs += 1;

        let mut tf: HashMap<String, u32> = HashMap::new();
        for tok in tokens {
            *tf.entry(tok.clone()).or_insert(0) += 1;
        }
        let len = tokens.len();
        self.doc_lens.push(len);

        // Update document frequencies (unique terms only).
        for term in tf.keys() {
            *self.doc_freq.entry(term.clone()).or_insert(0) += 1;
        }

        self.doc_tfs.push(tf);

        // Recompute avgdl incrementally.
        // avgdl = sum_lens / n_docs
        // sum_lens = avgdl_old * (n_docs - 1) + len
        let prev_sum = self.avgdl * (self.n_docs - 1) as f64;
        self.avgdl = (prev_sum + len as f64) / self.n_docs as f64;

        doc_idx
    }

    /// Add a document from a raw text string (will be tokenized).
    pub fn add_text(&mut self, text: &str) -> usize {
        let tokens = tokenize(text);
        self.add_document(&tokens)
    }

    /// Score a single document against a query (already tokenized).
    pub fn score(&self, doc_idx: usize, query_terms: &[String]) -> f64 {
        if self.n_docs == 0 || doc_idx >= self.n_docs {
            return 0.0;
        }
        let tf = &self.doc_tfs[doc_idx];
        let doc_len = self.doc_lens[doc_idx] as f64;
        let avgdl = if self.avgdl > 0.0 { self.avgdl } else { 1.0 };

        let mut total = 0.0;
        for term in query_terms {
            let df = match self.doc_freq.get(term) {
                Some(&n) => n,
                None => continue, // term not in corpus → no contribution
            };
            let tf_val = tf.get(term).copied().unwrap_or(0) as f64;
            if tf_val == 0.0 {
                continue;
            }
            let idf = idf_lucene(self.n_docs, df);
            let denom = tf_val + self.k1 * (1.0 - self.b + self.b * doc_len / avgdl);
            let term_score = idf * (tf_val * (self.k1 + 1.0)) / denom;
            total += term_score;
        }
        total
    }

    /// Score all documents against a query, returning (doc_idx, score)
    /// pairs sorted by score descending.
    pub fn score_all(&self, query_terms: &[String]) -> Vec<(usize, f64)> {
        let mut results: Vec<(usize, f64)> = (0..self.n_docs)
            .map(|i| (i, self.score(i, query_terms)))
            .collect();
        // Stable sort by score descending.
        results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        results
    }

    /// Number of indexed documents.
    pub fn len(&self) -> usize {
        self.n_docs
    }

    /// Is the index empty?
    pub fn is_empty(&self) -> bool {
        self.n_docs == 0
    }

    /// Average document length (in tokens).
    pub fn avgdl(&self) -> f64 {
        self.avgdl
    }
}

impl Default for Bm25 {
    fn default() -> Self {
        Self::new()
    }
}

/// Lucene-style IDF: `ln(1 + (N - n + 0.5) / (n + 0.5))`.
///
/// This is the "Lucene tweak" over the classic Robertson-Spärck-Jones IDF
/// `ln((N - n + 0.5) / (n + 0.5))`. The `1 +` inside the log guarantees
/// the result is non-negative even when a term appears in every document
/// (n == N → classic IDF → ln(0.5/N) which is very negative). With the
/// tweak, n == N → ln(1 + 0.5/N) ≈ 0, so common terms simply contribute
/// ~0 instead of dragging the score down.
fn idf_lucene(n_docs: usize, doc_freq: usize) -> f64 {
    let n = n_docs as f64;
    let df = doc_freq as f64;
    // Guard against N=0 (shouldn't happen — caller checks — but be safe).
    if n == 0.0 {
        return 0.0;
    }
    let ratio = (n - df + 0.5) / (df + 0.5);
    (1.0 + ratio.max(0.0)).ln()
}

// ---------------------------------------------------------------------------
// Reciprocal Rank Fusion (RRF)
// ---------------------------------------------------------------------------

/// Fuse two ranked lists using Reciprocal Rank Fusion (RRF).
///
/// RRF is a simple, robust way to combine ranked lists from multiple
/// retrieval systems without needing score calibration:
///
/// ```text
/// RRF(d) = Σ_i  1 / (k + rank_i(d))
/// ```
///
/// where `rank_i(d)` is the 1-based rank of document `d` in list `i`
/// (rank 1 = best), and `k` is a smoothing constant (default 60).
///
/// Documents not present in a list contribute 0 from that list.
///
/// This implementation accepts an arbitrary number of (ranked_indices,
/// weight) pairs and returns the fused (idx, score) list sorted by score
/// descending.
pub fn reciprocal_rank_fusion(
    ranked_lists: &[(&[usize], f64)],
    k: usize,
) -> Vec<(usize, f64)> {
    let mut scores: HashMap<usize, f64> = HashMap::new();
    for (ranking, weight) in ranked_lists {
        for (zero_based, &idx) in ranking.iter().enumerate() {
            let rank = (zero_based + 1) as f64; // 1-based
            let contribution = weight / (k as f64 + rank);
            *scores.entry(idx).or_insert(0.0) += contribution;
        }
    }
    let mut out: Vec<(usize, f64)> = scores.into_iter().collect();
    out.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    out
}

// ---------------------------------------------------------------------------
// Hybrid search (top-level entry point)
// ---------------------------------------------------------------------------

/// Compute the distance from a query vector to a row's vector column.
///
/// Returns `None` if:
///   - The row doesn't have the named column
///   - The column value isn't a JSON array of numbers
///   - The dimensions don't match the query
fn row_vector_distance(
    row: &JsonValue,
    column: &str,
    query: &[f32],
    metric: &str,
) -> Option<f64> {
    let arr = row.get(column)?.as_array()?;
    let v: Vec<f32> = arr.iter()
        .filter_map(|x| x.as_f64().map(|f| f as f32))
        .collect();
    if v.len() != query.len() {
        return None;
    }
    Some(match metric {
        "l2" | "euclidean" => l2_distance(query, &v),
        "cosine" => cosine_distance(query, &v),
        "dot" => -dot_product(query, &v), // higher dot = closer, so negate for "distance"
        _ => l2_distance(query, &v),
    })
}

/// Extract a string representation of the named column from a row for
/// BM25 indexing. Concatenates the value's JSON representation so any
/// column type (number, string, bool, array) is searchable.
fn row_column_text(row: &JsonValue, column: &str) -> String {
    match row.get(column) {
        Some(JsonValue::String(s)) => s.clone(),
        Some(v) => v.to_string(),
        None => String::new(),
    }
}

/// Run a hybrid (vector + BM25 text) search over a list of rows.
///
/// # Arguments
///   - `rows`: the candidate rows to search.
///   - `vector_column`: name of the column holding the query vector. May
///     be empty if no vector search is desired.
///   - `query_vector`: the query vector. Must be non-empty if
///     `vector_column` is non-empty.
///   - `text_columns`: columns to tokenize for BM25. May be empty for
///     vector-only search.
///   - `query_text`: the query string (will be tokenized).
///   - `where_expr`: optional filter — a closure that returns `true` for
///     rows that should be included. Pass `None` for no filtering.
///   - `weights`: per-modality weights (default = equal weighting).
///   - `k`: number of top results to return.
///   - `metric`: vector distance metric — "l2", "cosine", or "dot".
///
/// # Returns
///   Up to `k` `SearchHit`s, sorted by fused RRF score descending.
///
/// If BOTH `vector_column` and `text_columns` are empty/None, returns an
/// empty list (no modality to search).
#[allow(clippy::too_many_arguments)]
pub fn hybrid_search(
    rows: &[JsonValue],
    vector_column: &str,
    query_vector: &[f32],
    text_columns: &[&str],
    query_text: &str,
    where_expr: Option<&dyn Fn(&JsonValue) -> bool>,
    weights: SearchWeights,
    k: usize,
    metric: &str,
) -> Vec<SearchHit> {

    if k == 0 || rows.is_empty() {
        return Vec::new();
    }

    let use_vector = !vector_column.is_empty() && !query_vector.is_empty();
    let use_text = !text_columns.is_empty() && !query_text.is_empty();
    if !use_vector && !use_text {
        return Vec::new();
    }

    // Apply the optional WHERE filter up-front.
    let filtered: Vec<(usize, &JsonValue)> = match where_expr {
        Some(f) => rows.iter().enumerate().filter(|(_, r)| f(r)).collect(),
        None => rows.iter().enumerate().collect(),
    };
    if filtered.is_empty() {
        return Vec::new();
    }

    // --- Vector modality ---
    // Compute distance for each row, keep only rows with a valid vector,
    // then sort ascending (closer = better).
    let mut vector_ranking: Vec<(usize, f64)> = Vec::new();
    if use_vector {
        for (idx, row) in &filtered {
            if let Some(dist) = row_vector_distance(row, vector_column, query_vector, metric) {
                vector_ranking.push((*idx, dist));
            }
        }
        vector_ranking.sort_by(|a, b| {
            a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal)
        });
    }

    // --- Text modality (BM25) ---
    let mut text_ranking: Vec<(usize, f64)> = Vec::new();
    if use_text {
        let mut bm25 = Bm25::new();
        let query_terms = tokenize(query_text);
        if !query_terms.is_empty() {
            // Add each filtered row's concatenated text as a document.
            // Keep a parallel map from BM25 doc_idx → original row idx.
            let mut doc_to_row: Vec<usize> = Vec::with_capacity(filtered.len());
            for (idx, row) in &filtered {
                let combined: String = text_columns.iter()
                    .map(|c| row_column_text(row, c))
                    .collect::<Vec<_>>()
                    .join(" ");
                let tokens = tokenize(&combined);
                let _ = bm25.add_document(&tokens);
                doc_to_row.push(*idx);
            }
            let scores = bm25.score_all(&query_terms);
            for (doc_idx, score) in scores {
                if score > 0.0 {
                    text_ranking.push((doc_to_row[doc_idx], score));
                }
            }
            // text_ranking is already sorted by score descending (from score_all).
        }
    }

    // --- RRF fusion ---
    // Build the lists of indices (in rank order) and fuse them.
    let vector_idx: Vec<usize> = vector_ranking.iter().map(|(i, _)| *i).collect();
    let text_idx: Vec<usize> = text_ranking.iter().map(|(i, _)| *i).collect();

    let mut lists: Vec<(&[usize], f64)> = Vec::new();
    if use_vector {
        lists.push((&vector_idx, weights.vector));
    }
    if use_text {
        lists.push((&text_idx, weights.text));
    }

    let fused = reciprocal_rank_fusion(&lists, 60);

    // Build the result hits. Look up the original distance / BM25 score
    // for each fused entry so the caller can inspect per-modality
    // contributions.
    let mut vector_lookup: HashMap<usize, f64> = vector_ranking.into_iter().collect();
    let mut text_lookup: HashMap<usize, f64> = text_ranking.into_iter().collect();

    let mut hits: Vec<SearchHit> = fused.into_iter()
        .take(k)
        .map(|(idx, score)| {
            let row = rows[idx].clone();
            let vector_distance = use_vector.then(|| vector_lookup.remove(&idx)).flatten();
            let text_score = use_text.then(|| text_lookup.remove(&idx)).flatten();
            SearchHit {
                row,
                score,
                vector_distance,
                text_score,
            }
        })
        .collect();
    // `fused` is already sorted by score descending, but `take(k)` preserves
    // that order — no need to re-sort.
    let _ = &mut hits; // silence unused_mut if hits is never mutated again
    hits
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ---- Tokenizer ----

    #[test]
    fn test_tokenize_basic() {
        let toks = tokenize("Hello, world! Hello again.");
        assert_eq!(toks, vec!["hello", "world", "hello", "again"]);
    }

    #[test]
    fn test_tokenize_lowercase() {
        let toks = tokenize("PondStorage POND pond");
        assert_eq!(toks, vec!["pondstorage", "pond", "pond"]);
    }

    #[test]
    fn test_tokenize_removes_stop_words() {
        let toks = tokenize("the quick brown fox jumps past the lazy dog");
        // "the" is a stop word; the rest stay.
        assert_eq!(toks, vec!["quick", "brown", "fox", "jumps", "past", "lazy", "dog"]);
    }

    #[test]
    fn test_tokenize_drops_single_chars() {
        let toks = tokenize("a 1 x apple");
        // "a", "1", "x" are single chars → dropped. "apple" stays.
        assert_eq!(toks, vec!["apple"]);
    }

    #[test]
    fn test_tokenize_handles_punctuation() {
        let toks = tokenize("user_id=42; status:'active'");
        // Splits on =, ;, :, ', and the parens.
        assert_eq!(toks, vec!["user_id", "42", "status", "active"]);
    }

    #[test]
    fn test_tokenize_empty_string() {
        assert!(tokenize("").is_empty());
    }

    #[test]
    fn test_tokenize_only_punctuation() {
        assert!(tokenize("...,,,;;;").is_empty());
    }

    #[test]
    fn test_tokenize_unicode_letters() {
        // Should treat Unicode letters as part of tokens (lowercased).
        let toks = tokenize("Café résumé");
        assert_eq!(toks, vec!["café", "résumé"]);
    }

    // ---- BM25 ----

    #[test]
    fn test_bm25_empty_index() {
        let bm = Bm25::new();
        assert!(bm.is_empty());
        let scores = bm.score_all(&["hello".to_string()]);
        assert!(scores.is_empty());
    }

    #[test]
    fn test_bm25_single_doc() {
        let mut bm = Bm25::new();
        let _ = bm.add_text("the quick brown fox");
        let scores = bm.score_all(&["quick".to_string()]);
        assert_eq!(scores.len(), 1);
        assert!(scores[0].1 > 0.0, "single matching doc should have positive score");
    }

    #[test]
    fn test_bm25_term_not_in_corpus() {
        let mut bm = Bm25::new();
        bm.add_text("hello world");
        let scores = bm.score_all(&["missing".to_string()]);
        assert_eq!(scores.len(), 1);
        // The doc has no matching terms → score is 0.0.
        assert_eq!(scores[0].1, 0.0);
    }

    #[test]
    fn test_bm25_rare_term_beats_common_term() {
        let mut bm = Bm25::new();
        // "common" appears in all 3 docs; "rare" appears in only 1.
        bm.add_text("common cat");
        bm.add_text("common dog");
        bm.add_text("common rare bird");
        let scores = bm.score_all(&["common".to_string(), "rare".to_string()]);
        // Doc 2 (the only one with "rare") should rank highest.
        assert_eq!(scores[0].0, 2, "rare-term doc should rank first");
    }

    #[test]
    fn test_bm25_term_frequency_matters() {
        let mut bm = Bm25::new();
        bm.add_text("cat cat cat"); // high tf
        bm.add_text("cat");          // low tf
        let scores = bm.score_all(&["cat".to_string()]);
        // Higher tf should rank first (BM25 saturates with k1 but still
        // rewards higher tf for short queries).
        assert_eq!(scores[0].0, 0, "doc with higher tf should rank first");
    }

    #[test]
    fn test_bm25_short_docs_score_higher() {
        // With identical tf, a shorter doc should score higher (b>0
        // normalizes by length).
        let mut bm = Bm25::new();
        bm.add_text("cat dog fish bird turtle lizard snake");  // long
        bm.add_text("cat");                                       // short
        let scores = bm.score_all(&["cat".to_string()]);
        assert_eq!(scores[0].0, 1, "shorter doc should rank higher for same tf");
    }

    #[test]
    fn test_bm25_idf_is_non_negative() {
        // Term appears in every document — Lucene tweak keeps IDF >= 0.
        let mut bm = Bm25::new();
        bm.add_text("cat dog");
        bm.add_text("cat fish");
        bm.add_text("cat bird");
        let scores = bm.score_all(&["cat".to_string()]);
        // With the Lucene tweak, IDF is ln(1 + 0.5/3.5) > 0, so all
        // scores should be >= 0.
        for (_, s) in &scores {
            assert!(*s >= 0.0, "BM25 score should be non-negative with Lucene IDF");
        }
    }

    #[test]
    fn test_bm25_avgdl_updates() {
        let mut bm = Bm25::new();
        bm.add_text("one two three"); // 3 tokens
        assert!((bm.avgdl() - 3.0).abs() < 1e-9);
        bm.add_text("four five");      // 2 tokens
        assert!((bm.avgdl() - 2.5).abs() < 1e-9);
        bm.add_text("six");            // 1 token
        assert!((bm.avgdl() - 2.0).abs() < 1e-9);
    }

    #[test]
    fn test_bm25_multi_term_query() {
        let mut bm = Bm25::new();
        bm.add_text("the red fox jumps");
        bm.add_text("the brown dog sleeps");
        bm.add_text("the red dog barks");
        // Query: "red dog" — doc 2 has both, doc 0 has one, doc 1 has one.
        let scores = bm.score_all(&["red".to_string(), "dog".to_string()]);
        assert_eq!(scores[0].0, 2, "doc with both terms should rank first");
    }

    #[test]
    fn test_bm25_custom_params() {
        // b=0 disables length normalization; k1 controls tf saturation.
        // With b=0 and equal tf, length should have NO effect → scores equal.
        let mut bm = Bm25::with_params(1.2, 0.0);
        bm.add_text("cat dog bird turtle");  // 4 tokens, tf(cat)=1
        bm.add_text("cat");                   // 1 token,  tf(cat)=1
        let scores = bm.score_all(&["cat".to_string()]);
        // Same tf, same IDF, b=0 → identical scores.
        let diff = (scores[0].1 - scores[1].1).abs();
        assert!(diff < 1e-9, "b=0 with equal tf should produce equal scores, got diff={}", diff);

        // Now confirm b>0 length-normalizes: the shorter doc should win.
        let mut bm2 = Bm25::with_params(1.2, 0.75);
        bm2.add_text("cat dog bird turtle");
        bm2.add_text("cat");
        let scores2 = bm2.score_all(&["cat".to_string()]);
        assert_eq!(scores2[0].0, 1, "with b>0, shorter doc should win for same tf");
    }

    // ---- RRF ----

    #[test]
    fn test_rrf_single_list() {
        let list = vec![3usize, 1, 2];
        let fused = reciprocal_rank_fusion(&[(&list, 1.0)], 60);
        assert_eq!(fused.len(), 3);
        // Rank 1 (idx 3) should have the highest score.
        assert_eq!(fused[0].0, 3);
        assert!(fused[0].1 > fused[1].1);
    }

    #[test]
    fn test_rrf_two_lists_overlap() {
        // Both lists agree on rank 1 → that doc should win.
        let a = vec![1usize, 2, 3];
        let b = vec![1, 3, 2];
        let fused = reciprocal_rank_fusion(&[(&a, 1.0), (&b, 1.0)], 60);
        assert_eq!(fused[0].0, 1, "rank-1 in both lists should fuse to top");
    }

    #[test]
    fn test_rrf_two_lists_disagree() {
        // A ranks 1 first; B ranks 2 first. With equal weights, the
        // doc that appears in BOTH lists should still beat one that's
        // only in one list.
        let a = vec![1usize, 2, 3];
        let b = vec![2, 3, 4]; // 4 only in B, at rank 3
        let fused = reciprocal_rank_fusion(&[(&a, 1.0), (&b, 1.0)], 60);
        // Top should be either 1 or 2 (both have a rank-1 appearance
        // across the two lists). Doc 4 should be last (only one list, rank 3).
        assert!(fused[0].0 == 1 || fused[0].0 == 2);
        assert_eq!(fused.last().unwrap().0, 4);
    }

    #[test]
    fn test_rrf_weights() {
        let a = vec![1usize, 2];
        let b = vec![2, 1];
        // Heavy weight on A → A's rank-1 (doc 1) should win.
        let fused = reciprocal_rank_fusion(&[(&a, 10.0), (&b, 1.0)], 60);
        assert_eq!(fused[0].0, 1);
    }

    #[test]
    fn test_rrf_empty_lists() {
        let fused = reciprocal_rank_fusion(&[], 60);
        assert!(fused.is_empty());
    }

    #[test]
    fn test_rrf_k_smoothing() {
        // Larger k → scores closer together (more smoothing).
        let a = vec![1usize, 2];
        let fused_small_k = reciprocal_rank_fusion(&[(&a, 1.0)], 1);
        let fused_large_k = reciprocal_rank_fusion(&[(&a, 1.0)], 1000);
        let diff_small = fused_small_k[0].1 - fused_small_k[1].1;
        let diff_large = fused_large_k[0].1 - fused_large_k[1].1;
        assert!(diff_small > diff_large, "smaller k should produce bigger score gaps");
    }

    #[test]
    fn test_rrf_disjoint_lists() {
        let a = vec![1usize, 2, 3];
        let b = vec![4, 5, 6];
        let fused = reciprocal_rank_fusion(&[(&a, 1.0), (&b, 1.0)], 60);
        assert_eq!(fused.len(), 6);
        // Doc 1 (rank 1 in A) and doc 4 (rank 1 in B) should tie for top.
        assert!((fused[0].1 - fused[1].1).abs() < 1e-9);
        assert!(fused[0].0 == 1 || fused[0].0 == 4);
    }

    // ---- Hybrid search ----

    #[test]
    fn test_hybrid_search_vector_only() {
        let rows = vec![
            json!({"id": 1, "vec": [1.0, 0.0]}),
            json!({"id": 2, "vec": [0.0, 1.0]}),
            json!({"id": 3, "vec": [1.0, 1.0]}),
        ];
        let hits = hybrid_search(
            &rows,
            "vec",
            &[1.0, 0.0],
            &[],
            "",
            None,
            SearchWeights::default(),
            2,
            "l2",
        );
        assert_eq!(hits.len(), 2);
        // Closest to [1.0, 0.0] is row 0 (distance 0), then row 2 (distance 1).
        assert_eq!(hits[0].row["id"], json!(1));
        assert_eq!(hits[1].row["id"], json!(3));
        assert!(hits[0].vector_distance.unwrap() < hits[1].vector_distance.unwrap());
        assert!(hits[0].text_score.is_none());
    }

    #[test]
    fn test_hybrid_search_text_only() {
        let rows = vec![
            json!({"id": 1, "text": "the quick brown fox"}),
            json!({"id": 2, "text": "the lazy dog"}),
            json!({"id": 3, "text": "fox fox fox"}),
        ];
        let hits = hybrid_search(
            &rows,
            "",
            &[],
            &["text"],
            "fox",
            None,
            SearchWeights::default(),
            3,
            "l2",
        );
        // Only docs 1 and 3 contain "fox" — doc 2 has no match and is
        // filtered out (BM25 score 0 doesn't make the text ranking).
        assert_eq!(hits.len(), 2);
        // Doc 3 has "fox" 3 times → highest BM25 score.
        assert_eq!(hits[0].row["id"], json!(3));
        assert!(hits[0].text_score.unwrap() > 0.0);
        assert!(hits[0].vector_distance.is_none());
    }

    #[test]
    fn test_hybrid_search_both_modalities() {
        let rows = vec![
            json!({"id": 1, "vec": [1.0, 0.0], "text": "cat cat cat"}),
            json!({"id": 2, "vec": [0.0, 1.0], "text": "dog dog dog"}),
            json!({"id": 3, "vec": [1.0, 1.0], "text": "cat dog"}),
        ];
        // Query: vector=[1.0, 0.0], text="cat"
        // Vector ranks: 1 (dist 0), 3 (dist 1), 2 (dist sqrt(2))
        // Text ranks: 1 (cat x3, high score), 3 (cat x1, lower score)
        // RRF should put doc 1 first (rank 1 in both lists).
        let hits = hybrid_search(
            &rows,
            "vec",
            &[1.0, 0.0],
            &["text"],
            "cat",
            None,
            SearchWeights::default(),
            3,
            "l2",
        );
        assert_eq!(hits.len(), 3);
        assert_eq!(hits[0].row["id"], json!(1), "doc 1 should win both modalities");
        assert!(hits[0].vector_distance.is_some());
        assert!(hits[0].text_score.is_some());
    }

    #[test]
    fn test_hybrid_search_with_where_filter() {
        let rows = vec![
            json!({"id": 1, "vec": [1.0, 0.0], "active": true}),
            json!({"id": 2, "vec": [0.5, 0.5], "active": false}),
            json!({"id": 3, "vec": [0.9, 0.1], "active": true}),
        ];
        // Filter: only active=true.
        let hits = hybrid_search(
            &rows,
            "vec",
            &[1.0, 0.0],
            &[],
            "",
            Some(&|row| row.get("active").and_then(|v| v.as_bool()).unwrap_or(false)),
            SearchWeights::default(),
            5,
            "l2",
        );
        assert_eq!(hits.len(), 2, "should only return active rows");
        for hit in &hits {
            assert_eq!(hit.row["active"], json!(true));
        }
    }

    #[test]
    fn test_hybrid_search_no_modality_returns_empty() {
        let rows = vec![json!({"id": 1})];
        let hits = hybrid_search(
            &rows,
            "",
            &[],
            &[],
            "",
            None,
            SearchWeights::default(),
            5,
            "l2",
        );
        assert!(hits.is_empty());
    }

    #[test]
    fn test_hybrid_search_empty_rows() {
        let hits = hybrid_search(
            &[],
            "vec",
            &[1.0, 0.0],
            &["text"],
            "query",
            None,
            SearchWeights::default(),
            5,
            "l2",
        );
        assert!(hits.is_empty());
    }

    #[test]
    fn test_hybrid_search_k_zero() {
        let rows = vec![json!({"id": 1, "vec": [1.0, 0.0]})];
        let hits = hybrid_search(
            &rows,
            "vec",
            &[1.0, 0.0],
            &[],
            "",
            None,
            SearchWeights::default(),
            0,
            "l2",
        );
        assert!(hits.is_empty());
    }

    #[test]
    fn test_hybrid_search_weights_affect_ranking() {
        let rows = vec![
            // Doc 0: bad vector match, perfect text match
            json!({"id": 1, "vec": [0.0, 1.0], "text": "cat cat cat"}),
            // Doc 1: perfect vector match, no text match
            json!({"id": 2, "vec": [1.0, 0.0], "text": "dog dog dog"}),
        ];
        // Vector ranking: [1, 0] (doc 1 distance 0, doc 0 distance 1)
        // Text ranking:   [0]    (only doc 0 matches "cat")
        //
        // The rank-1-vs-rank-2 gap in RRF is small (1/61 vs 1/62), so to
        // make the vector modality dominate we need a large vector weight.
        // w_v * (1/61 - 1/62) > w_t * (1/61)  ⇒  w_v > 62 * w_t.
        //
        // With w_v=100, w_t=1: doc 1 wins (perfect vector match).
        let hits_vec = hybrid_search(
            &rows,
            "vec",
            &[1.0, 0.0],
            &["text"],
            "cat",
            None,
            SearchWeights { vector: 100.0, text: 1.0 },
            2,
            "l2",
        );
        assert_eq!(hits_vec[0].row["id"], json!(2));

        // With w_v=1, w_t=100: doc 0 wins (text match dominates).
        let hits_text = hybrid_search(
            &rows,
            "vec",
            &[1.0, 0.0],
            &["text"],
            "cat",
            None,
            SearchWeights { vector: 1.0, text: 100.0 },
            2,
            "l2",
        );
        assert_eq!(hits_text[0].row["id"], json!(1));
    }

    #[test]
    fn test_hybrid_search_cosine_metric() {
        let rows = vec![
            json!({"id": 1, "vec": [1.0, 0.0]}),
            json!({"id": 2, "vec": [2.0, 0.0]}),  // same direction, larger magnitude
            json!({"id": 3, "vec": [0.0, 1.0]}),
        ];
        let hits = hybrid_search(
            &rows,
            "vec",
            &[1.0, 0.0],
            &[],
            "",
            None,
            SearchWeights::default(),
            3,
            "cosine",
        );
        // Cosine distance is 0 for same direction → docs 1 and 2 tie at distance 0.
        assert!((hits[0].vector_distance.unwrap() - 0.0).abs() < 1e-6);
        assert!((hits[1].vector_distance.unwrap() - 0.0).abs() < 1e-6);
        // Orthogonal doc 3 has cosine distance 1.0.
        assert!((hits[2].vector_distance.unwrap() - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_hybrid_search_dot_metric() {
        let rows = vec![
            json!({"id": 1, "vec": [1.0, 1.0]}),
            json!({"id": 2, "vec": [3.0, 3.0]}),   // higher dot = "closer"
            json!({"id": 3, "vec": [0.0, 0.0]}),
        ];
        let hits = hybrid_search(
            &rows,
            "vec",
            &[1.0, 1.0],
            &[],
            "",
            None,
            SearchWeights::default(),
            3,
            "dot",
        );
        // Doc 2 (dot=6) should be first; doc 1 (dot=2) second; doc 3 (dot=0) last.
        assert_eq!(hits[0].row["id"], json!(2));
        assert_eq!(hits[1].row["id"], json!(1));
        assert_eq!(hits[2].row["id"], json!(3));
    }

    #[test]
    fn test_hybrid_search_missing_vector_column_skipped() {
        let rows = vec![
            json!({"id": 1, "vec": [1.0, 0.0]}),
            json!({"id": 2, "text": "no vector here"}),
            json!({"id": 3, "vec": [0.9, 0.1]}),
        ];
        let hits = hybrid_search(
            &rows,
            "vec",
            &[1.0, 0.0],
            &[],
            "",
            None,
            SearchWeights::default(),
            5,
            "l2",
        );
        // Doc 2 has no "vec" column → skipped. Only docs 1 and 3 returned.
        assert_eq!(hits.len(), 2);
        for hit in &hits {
            assert!(hit.row.get("vec").is_some());
        }
    }

    #[test]
    fn test_hybrid_search_dimension_mismatch_skipped() {
        let rows = vec![
            json!({"id": 1, "vec": [1.0, 0.0]}),
            json!({"id": 2, "vec": [1.0, 0.0, 0.0]}),  // wrong dim
        ];
        let hits = hybrid_search(
            &rows,
            "vec",
            &[1.0, 0.0],
            &[],
            "",
            None,
            SearchWeights::default(),
            5,
            "l2",
        );
        // Only the dimension-matching doc is returned.
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].row["id"], json!(1));
    }

    #[test]
    fn test_hybrid_search_multi_text_column() {
        let rows = vec![
            json!({"id": 1, "title": "cat", "body": "dog"}),
            json!({"id": 2, "title": "dog", "body": "cat cat"}),
            json!({"id": 3, "title": "bird", "body": "fish"}),
        ];
        // Search across both columns for "cat".
        let hits = hybrid_search(
            &rows,
            "",
            &[],
            &["title", "body"],
            "cat",
            None,
            SearchWeights::default(),
            3,
            "l2",
        );
        // Doc 2 has "cat" twice (in body) → highest score.
        assert_eq!(hits[0].row["id"], json!(2));
    }

    #[test]
    fn test_hybrid_search_returns_at_most_k() {
        let rows: Vec<JsonValue> = (0..10)
            .map(|i| json!({"id": i, "vec": [i as f32, 0.0]}))
            .collect();
        let hits = hybrid_search(
            &rows,
            "vec",
            &[0.0, 0.0],
            &[],
            "",
            None,
            SearchWeights::default(),
            3,
            "l2",
        );
        assert_eq!(hits.len(), 3);
    }

    #[test]
    fn test_search_weights_default_equal() {
        let w = SearchWeights::default();
        assert_eq!(w.vector, w.text);
        assert_eq!(w.vector, 1.0);
    }

    #[test]
    fn test_search_hit_clone() {
        let hit = SearchHit {
            row: json!({"id": 1}),
            score: 0.5,
            vector_distance: Some(0.1),
            text_score: Some(1.2),
        };
        let cloned = hit.clone();
        assert_eq!(cloned.row, hit.row);
        assert_eq!(cloned.score, hit.score);
        assert_eq!(cloned.vector_distance, hit.vector_distance);
        assert_eq!(cloned.text_score, hit.text_score);
    }
}
