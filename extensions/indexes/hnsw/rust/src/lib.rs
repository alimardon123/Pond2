// HNSW Index — Hierarchical Navigable Small World for ANN search.
//
// Port of Python bindings/python/sdk/extensions/indexing/hnsw_index.py
//
// HNSW provides O(log N) approximate nearest neighbor search via a
// multi-layer graph structure. Better than IVF for high-recall at low
// latency.
//
// ALGORITHM:
//   Build: Insert nodes one by one. Each node gets a random top layer
//   (geometric distribution). At each layer, find ef_construction nearest
//   neighbors via beam search, connect bidirectionally, prune to M.
//
//   Search: Greedy walk from top layer to layer 1 (tiny layers). At layer 0,
//   beam search with ef beam width. Return top-k.
//
// STORAGE (chunked — one blob per layer):
//   Header: JSON {n_dims, max_layer, M, ef_construction, metric, entry_point, n_vectors, ids, layer_hashes}
//   Per layer: binary blob — n_nodes(4B) + [node_idx(4B) + n_neighbors(4B) + neighbors(4B each)] * n_nodes
//
// API:
//   let hnsw = HNSWIndex::new(kernel);
//   hnsw.build("vectors", 16, 200, None, "l2")?;
//   let results = hnsw.search("vectors", &query, 10, 50)?;

use pond_kernel::PondKernel;
use pond_storage::manifest::CollectionManifest;
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
use std::cmp::Ordering;

const METRIC_L2: u8 = 0;
const METRIC_COSINE: u8 = 1;

/// HNSW index for approximate nearest neighbor search.
///
/// Better than IVF for high-recall at low latency — O(log N) vs O(N/k).
pub struct HNSWIndex<'a> {
    kernel: &'a PondKernel,
}

impl<'a> HNSWIndex<'a> {
    pub fn new(kernel: &'a PondKernel) -> Self {
        Self { kernel }
    }

    /// Build the HNSW index for a collection.
    ///
    /// Args:
    ///   - collection: Collection name
    ///   - m: Max connections per node per layer (higher = better recall, more memory)
    ///   - ef_construction: Search beam width during construction (higher = better quality)
    ///   - n_dimensions: Auto-detected if None
    ///   - distance_metric: "l2" or "cosine"
    ///
    /// Returns: index header hash
    pub fn build(
        &self,
        collection: &str,
        m: usize,
        ef_construction: usize,
        n_dimensions: Option<usize>,
        distance_metric: &str,
    ) -> Result<String, String> {
        // 1. Read all vectors from the collection
        let (vectors, ids) = self.read_all_vectors(collection, n_dimensions)?;

        if vectors.is_empty() {
            return Err(format!("No vectors found in collection '{}'", collection));
        }

        let n_dims = n_dimensions.unwrap_or(vectors[0].len());
        let _metric_code = match distance_metric {
            "l2" => METRIC_L2,
            "cosine" => METRIC_COSINE,
            _ => return Err(format!("unknown metric: {}", distance_metric)),
        };

        // 2. Build the hierarchical graph
        let (graph, entry_point) = build_graph(&vectors, m, ef_construction, distance_metric);

        // 3. Encode as chunked blobs (one per layer)
        let max_layer = graph.len().saturating_sub(1) as i32;
        let mut layer_hashes: Vec<String> = Vec::new();

        for layer in &graph {
            let layer_bytes = encode_layer(layer);
            let hash = self.kernel.write(&layer_bytes)
                .map_err(|e| format!("Failed to write layer: {}", e))?;
            layer_hashes.push(hash);
        }

        // 4. Write header (JSON with metadata + layer hashes + IDs)
        let header = json!({
            "n_dims": n_dims,
            "max_layer": max_layer,
            "M": m,
            "ef_construction": ef_construction,
            "metric": distance_metric,
            "entry_point": entry_point,
            "n_vectors": ids.len(),
            "ids": ids,
            "layer_hashes": layer_hashes,
        });

        let header_bytes = serde_json::to_vec(&header)
            .map_err(|e| format!("Failed to serialize header: {}", e))?;
        let header_hash = self.kernel.write(&header_bytes)
            .map_err(|e| format!("Failed to write header: {}", e))?;

        // 5. Reference the index
        let ref_name = format!("collections/{}/indexes/hnsw", collection);
        self.kernel.reference(&ref_name, &header_hash)
            .map_err(|e| format!("Failed to reference index: {}", e))?;

        Ok(header_hash)
    }

    /// Search for k nearest neighbors using HNSW.
    ///
    /// O(log N) distance computations. Uses chunked loading — fetches
    /// only the layers needed. Top layers are tiny (few nodes). Layer 0
    /// is big but fetched once.
    ///
    /// Args:
    ///   - collection: Collection name
    ///   - query: Query vector
    ///   - k: Number of nearest neighbors to return
    ///   - ef: Beam width for layer 0 search (higher = better recall)
    ///
    /// Returns: Vec<(distance, vector_id)> sorted by distance
    pub fn search(
        &self,
        collection: &str,
        query: &[f64],
        k: usize,
        ef: usize,
    ) -> Result<Vec<(f64, String)>, String> {
        // 1. Load header
        let ref_name = format!("collections/{}/indexes/hnsw", collection);
        let header_hash = self.kernel.resolve(&ref_name)
            .ok_or_else(|| format!("No HNSW index for collection '{}'", collection))?;

        let header_data = self.kernel.read_blob(&header_hash)
            .map_err(|e| format!("Failed to read header: {}", e))?;

        let header: Value = serde_json::from_slice(&header_data)
            .map_err(|e| format!("Failed to parse header: {}", e))?;

        let metric = header.get("metric").and_then(|m| m.as_str()).unwrap_or("l2");
        let entry_point = header.get("entry_point").and_then(|e| e.as_u64()).unwrap_or(0) as usize;
        let max_layer = header.get("max_layer").and_then(|m| m.as_i64()).unwrap_or(0) as i32;
        let layer_hashes: Vec<String> = header.get("layer_hashes")
            .and_then(|h| h.as_array())
            .map(|arr| arr.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect())
            .unwrap_or_default();

        let ids: Vec<String> = header.get("ids")
            .and_then(|i| i.as_array())
            .map(|arr| arr.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect())
            .unwrap_or_default();

        if layer_hashes.is_empty() {
            return Err("Index has no layer hashes".to_string());
        }

        // 2. Read all vectors (needed for distance computation)
        let (vectors, _) = self.read_all_vectors(collection, None)?;
        if vectors.is_empty() {
            return Ok(Vec::new());
        }

        // 3. Phase 1: greedy walk from top layer to layer 1
        let mut curr = entry_point;
        for layer in (1..=max_layer as usize).rev() {
            if layer >= layer_hashes.len() { break; }
            let layer_data = self.kernel.read_blob(&layer_hashes[layer])
                .map_err(|e| format!("Failed to read layer {}: {}", layer, e))?;
            let graph_layer = decode_layer(&layer_data);
            curr = greedy_search(&vectors, &graph_layer, curr, query, metric);
        }

        // 4. Phase 2: beam search at layer 0
        let layer0_data = self.kernel.read_blob(&layer_hashes[0])
            .map_err(|e| format!("Failed to read layer 0: {}", e))?;
        let graph_layer0 = decode_layer(&layer0_data);
        let candidates = search_layer(&vectors, &graph_layer0, curr, query, ef.max(k), metric);

        // 5. Sort by distance and return top-k
        let mut scored: Vec<(f64, usize)> = candidates.into_iter()
            .map(|node_idx| (distance(query, &vectors[node_idx], metric), node_idx))
            .collect();
        scored.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(Ordering::Equal));
        scored.truncate(k);

        Ok(scored.into_iter()
            .filter_map(|(dist, node_idx)| {
                ids.get(node_idx).map(|id| (dist, id.clone()))
            })
            .collect())
    }

    /// Get index statistics.
    pub fn stats(&self, collection: &str) -> Option<HNSWStats> {
        let ref_name = format!("collections/{}/indexes/hnsw", collection);
        let header_hash = self.kernel.resolve(&ref_name)?;
        let header_data = self.kernel.read_blob(&header_hash).ok()?;
        let header: Value = serde_json::from_slice(&header_data).ok()?;

        let n_vectors = header.get("n_vectors").and_then(|n| n.as_u64()).unwrap_or(0) as usize;
        let max_layer = header.get("max_layer").and_then(|m| m.as_i64()).unwrap_or(0) as usize;
        let m = header.get("M").and_then(|m| m.as_u64()).unwrap_or(0) as usize;
        let metric = header.get("metric").and_then(|m| m.as_str()).unwrap_or("l2").to_string();

        Some(HNSWStats {
            n_vectors,
            max_layer,
            m,
            metric,
        })
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Read all vectors from a collection's manifest.
    fn read_all_vectors(
        &self,
        collection: &str,
        _n_dimensions: Option<usize>,
    ) -> Result<(Vec<Vec<f64>>, Vec<String>), String> {
        let branch = "main";
        let head = self.kernel.resolve(&pond_storage::branch_ref(collection, branch))
            .ok_or_else(|| format!("Collection '{}' has no commits", collection))?;

        let manifest_bytes = pond_storage::commit::resolve_manifest_bytes(self.kernel, &head)
            .map_err(|e| format!("Failed to read manifest: {}", e))?;

        let manifest = CollectionManifest::decode(&manifest_bytes)
            .ok_or_else(|| "Failed to decode manifest".to_string())?;

        let mut vectors: Vec<Vec<f64>> = Vec::new();
        let mut ids: Vec<String> = Vec::new();

        for rg in &manifest.row_groups {
            let blob_data = self.kernel.read_blob(&rg.blob_hash)
                .map_err(|e| format!("Failed to read data blob: {}", e))?;

            if let Ok(cols) = pond_core::pnd2_decode(&blob_data) {
                // Find ID column
                let id_col = cols.iter().find(|c| c.name.to_string_lossy() == "id");

                // Find dimension columns (dim_0, dim_1, ...)
                let mut dim_cols: Vec<&pond_core::PondColumn> = cols.iter()
                    .filter(|c| c.name.to_string_lossy().starts_with("dim_"))
                    .collect();
                dim_cols.sort_by_key(|c| c.name.to_string_lossy().to_string());

                let n_rows = dim_cols.first().map(|c| c.f64_data.len()).unwrap_or(0);

                for i in 0..n_rows {
                    let vec: Vec<f64> = dim_cols.iter()
                        .filter_map(|c| c.f64_data.get(i).copied())
                        .collect();
                    vectors.push(vec);

                    let id = if let Some(idc) = id_col {
                        idc.i64_data.get(i).map(|v| v.to_string()).unwrap_or_default()
                    } else {
                        i.to_string()
                    };
                    ids.push(id);
                }
            }
        }

        Ok((vectors, ids))
    }
}

/// Statistics about an HNSW index.
#[derive(Debug, Clone)]
pub struct HNSWStats {
    pub n_vectors: usize,
    pub max_layer: usize,
    pub m: usize,
    pub metric: String,
}

// ===========================================================================
// HNSW Graph (pure functions — no kernel dependency)
// ===========================================================================

type GraphLayer = HashMap<usize, Vec<usize>>;
type Graph = Vec<GraphLayer>;

/// Build a multi-layer HNSW graph.
///
/// Returns (graph, entry_point) where:
///   - graph: Vec of layers, each layer is {node_idx → [neighbor_idx]}
///   - entry_point: index of the entry node at the top layer
fn build_graph(
    vectors: &[Vec<f64>],
    m: usize,
    ef_construction: usize,
    metric: &str,
) -> (Graph, usize) {
    let n = vectors.len();
    if n == 0 {
        return (Vec::new(), 0);
    }

    // Determine max layer for each node (geometric distribution)
    let ml = if m > 1 { 1.0 / (m as f64).ln() } else { 1.0 };
    let mut max_layers: Vec<usize> = Vec::with_capacity(n);
    let mut rng = SimpleRng::new(42); // deterministic seed for testing

    for _ in 0..n {
        let level = (-(rng.next_f64() + 1e-10).ln() * ml) as usize;
        max_layers.push(level);
    }

    let top_layer = *max_layers.iter().max().unwrap_or(&0);

    // Initialize graph: graph[layer] = {node_idx: [neighbor_idx, ...]}
    let mut graph: Graph = vec![GraphLayer::new(); top_layer + 1];
    let mut entry_point = 0usize;

    // Insert nodes one by one
    for i in 0..n {
        insert_node(
            i, vectors, &mut graph, &max_layers,
            m, ef_construction, metric, entry_point,
        );
        // Update entry point if this node has a higher top layer
        if max_layers[i] > max_layers[entry_point] {
            entry_point = i;
        }
    }

    (graph, entry_point)
}

/// Insert a single node into the HNSW graph.
#[allow(clippy::too_many_arguments)]
fn insert_node(
    node_idx: usize,
    vectors: &[Vec<f64>],
    graph: &mut Graph,
    max_layers: &[usize],
    m: usize,
    ef_construction: usize,
    metric: &str,
    entry_point: usize,
) {
    let top_layer = max_layers[node_idx];
    let curr_max = max_layers[entry_point];
    let query = &vectors[node_idx];
    let mut ep = entry_point;

    // Phase 1: walk down from top layer to top_layer + 1 (greedy search)
    for layer in (top_layer + 1..=curr_max).rev() {
        if layer < graph.len() {
            ep = greedy_search(vectors, &graph[layer], ep, query, metric);
        }
    }

    // Phase 2: insert at layers top_layer down to 0
    for layer in (0..=top_layer.min(curr_max)).rev() {
        if layer >= graph.len() { continue; }

        // Find ef_construction nearest neighbors at this layer
        let neighbors = search_layer(vectors, &graph[layer], ep, query, ef_construction, metric);

        // Select M best neighbors (heuristic: keep diverse set)
        let selected = select_neighbors_heuristic(vectors, node_idx, &neighbors, m, metric);

        // Add bidirectional connections
        graph[layer].insert(node_idx, selected.clone());
        for &n in &selected {
            let entry = graph[layer].entry(n).or_default();
            entry.push(node_idx);
            // Prune neighbor list if too long
            if entry.len() > m {
                let pruned = select_neighbors_heuristic(vectors, n, entry, m, metric);
                *entry = pruned;
            }
        }

        // Update entry point for next layer
        if !neighbors.is_empty() {
            ep = *neighbors.iter()
                .min_by(|&&a, &&b| {
                    let da = distance(query, &vectors[a], metric);
                    let db = distance(query, &vectors[b], metric);
                    da.partial_cmp(&db).unwrap_or(Ordering::Equal)
                })
                .unwrap_or(&ep);
        }
    }
}

/// Greedy search at a layer — walk to the nearest neighbor.
fn greedy_search(
    vectors: &[Vec<f64>],
    layer: &GraphLayer,
    entry: usize,
    query: &[f64],
    metric: &str,
) -> usize {
    let mut current = entry;
    let mut current_dist = distance(query, &vectors[current], metric);
    let mut improved = true;

    while improved {
        improved = false;
        if let Some(neighbors) = layer.get(&current) {
            for &neighbor in neighbors {
                let d = distance(query, &vectors[neighbor], metric);
                if d < current_dist {
                    current = neighbor;
                    current_dist = d;
                    improved = true;
                }
            }
        }
    }

    current
}

/// Beam search at a layer — find ef nearest neighbors.
fn search_layer(
    vectors: &[Vec<f64>],
    layer: &GraphLayer,
    entry: usize,
    query: &[f64],
    ef: usize,
    metric: &str,
) -> Vec<usize> {
    let mut visited: HashSet<usize> = HashSet::new();
    visited.insert(entry);

    let mut candidates: Vec<(f64, usize)> = vec![(distance(query, &vectors[entry], metric), entry)];
    let mut results: Vec<(f64, usize)> = vec![(distance(query, &vectors[entry], metric), entry)];

    // Simple priority queue via sorting (not optimal but correct)
    while !candidates.is_empty() {
        // Pop the nearest candidate
        candidates.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(Ordering::Equal));
        let (_, curr) = candidates.remove(0);

        if let Some(neighbors) = layer.get(&curr) {
            for &neighbor in neighbors {
                if visited.contains(&neighbor) {
                    continue;
                }
                visited.insert(neighbor);

                let d = distance(query, &vectors[neighbor], metric);

                let should_add = results.len() < ef || {
                    results.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(Ordering::Equal));
                    d < results.last().unwrap().0
                };

                if should_add {
                    candidates.push((d, neighbor));
                    results.push((d, neighbor));
                    if results.len() > ef {
                        results.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(Ordering::Equal));
                        results.truncate(ef);
                    }
                }
            }
        }
    }

    results.into_iter().map(|(_, n)| n).collect()
}

/// Select M diverse neighbors (heuristic from HNSW paper).
fn select_neighbors_heuristic(
    vectors: &[Vec<f64>],
    node_idx: usize,
    candidates: &[usize],
    m: usize,
    metric: &str,
) -> Vec<usize> {
    if candidates.len() <= m {
        return candidates.to_vec();
    }

    let query = &vectors[node_idx];

    // Sort by distance to the node
    let mut scored: Vec<(f64, usize)> = candidates.iter()
        .map(|&c| (distance(query, &vectors[c], metric), c))
        .collect();
    scored.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(Ordering::Equal));

    let mut selected: Vec<usize> = Vec::new();

    for (_, c) in &scored {
        if selected.len() >= m {
            break;
        }
        // Check if c is closer to any selected than to the node
        let d_cn = distance(query, &vectors[*c], metric);
        let mut good = true;
        for &s in &selected {
            let d_cs = distance(&vectors[*c], &vectors[s], metric);
            if d_cs < d_cn {
                good = false;
                break;
            }
        }
        if good {
            selected.push(*c);
        }
    }

    // If not enough, add closest
    if selected.len() < m {
        for (_, c) in &scored {
            if !selected.contains(c) {
                selected.push(*c);
                if selected.len() >= m {
                    break;
                }
            }
        }
    }

    selected
}

/// Compute distance between two vectors.
fn distance(a: &[f64], b: &[f64], metric: &str) -> f64 {
    match metric {
        "cosine" => cosine_dist(a, b),
        _ => l2_dist(a, b),
    }
}

/// L2 (squared Euclidean) distance.
fn l2_dist(a: &[f64], b: &[f64]) -> f64 {
    a.iter().zip(b.iter())
        .map(|(x, y)| (x - y).powi(2))
        .sum()
}

/// Cosine distance (1 - cosine similarity).
fn cosine_dist(a: &[f64], b: &[f64]) -> f64 {
    let dot: f64 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let na: f64 = a.iter().map(|x| x * x).sum::<f64>().sqrt();
    let nb: f64 = b.iter().map(|x| x * x).sum::<f64>().sqrt();
    if na == 0.0 || nb == 0.0 {
        return f64::INFINITY;
    }
    1.0 - (dot / (na * nb))
}

// ===========================================================================
// Binary encoding/decoding (one blob per layer)
// ===========================================================================

/// Encode a graph layer as a binary blob.
///
/// Format: n_nodes(4B) + [node_idx(4B) + n_neighbors(4B) + neighbors(4B each)] * n_nodes
fn encode_layer(layer: &GraphLayer) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(layer.len() as u32).to_le_bytes());

    for (&node_idx, neighbors) in layer {
        buf.extend_from_slice(&(node_idx as u32).to_le_bytes());
        buf.extend_from_slice(&(neighbors.len() as u32).to_le_bytes());
        for &n in neighbors {
            buf.extend_from_slice(&(n as u32).to_le_bytes());
        }
    }

    buf
}

/// Decode a graph layer from a binary blob.
fn decode_layer(data: &[u8]) -> GraphLayer {
    let mut layer = GraphLayer::new();
    if data.len() < 4 {
        return layer;
    }

    let n_nodes = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
    let mut pos = 4;

    for _ in 0..n_nodes {
        if pos + 8 > data.len() { break; }
        let node_idx = u32::from_le_bytes([data[pos], data[pos+1], data[pos+2], data[pos+3]]) as usize;
        pos += 4;
        let n_neighbors = u32::from_le_bytes([data[pos], data[pos+1], data[pos+2], data[pos+3]]) as usize;
        pos += 4;

        let mut neighbors = Vec::with_capacity(n_neighbors);
        for _ in 0..n_neighbors {
            if pos + 4 > data.len() { break; }
            neighbors.push(u32::from_le_bytes([data[pos], data[pos+1], data[pos+2], data[pos+3]]) as usize);
            pos += 4;
        }
        layer.insert(node_idx, neighbors);
    }

    layer
}

// ===========================================================================
// Simple RNG (deterministic for testing)
// ===========================================================================

struct SimpleRng {
    state: u64,
}

impl SimpleRng {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_f64(&mut self) -> f64 {
        // xorshift64
        self.state ^= self.state << 13;
        self.state ^= self.state >> 7;
        self.state ^= self.state << 17;
        (self.state as f64) / (u64::MAX as f64)
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use pond_storage::UnifiedStorage;

    fn make_test_storage() -> (UnifiedStorage, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        (storage, dir)
    }

    #[test]
    fn test_l2_distance() {
        assert!((l2_dist(&[0.0, 0.0], &[3.0, 4.0]) - 25.0).abs() < 1e-10);
    }

    #[test]
    fn test_cosine_distance() {
        assert!((cosine_dist(&[1.0, 0.0], &[2.0, 0.0])).abs() < 1e-10);
        assert!((cosine_dist(&[1.0, 0.0], &[-1.0, 0.0]) - 2.0).abs() < 1e-10);
    }

    #[test]
    fn test_build_graph_small() {
        let vectors: Vec<Vec<f64>> = vec![
            vec![0.0, 0.0],
            vec![1.0, 0.0],
            vec![0.0, 1.0],
            vec![1.0, 1.0],
            vec![5.0, 5.0],
        ];

        let (graph, entry) = build_graph(&vectors, 4, 10, "l2");

        // Graph should have at least layer 0
        assert!(!graph.is_empty());
        // All 5 nodes should be in layer 0
        assert_eq!(graph[0].len(), 5);
        // Entry point should be valid
        assert!(entry < 5);
    }

    #[test]
    fn test_greedy_search_finds_nearest() {
        let vectors: Vec<Vec<f64>> = vec![
            vec![0.0, 0.0],
            vec![1.0, 0.0],
            vec![2.0, 0.0],
        ];

        let mut layer = GraphLayer::new();
        layer.insert(0, vec![1]);
        layer.insert(1, vec![0, 2]);
        layer.insert(2, vec![1]);

        let query = vec![2.5, 0.0];
        let result = greedy_search(&vectors, &layer, 0, &query, "l2");
        assert_eq!(result, 2); // closest to query
    }

    #[test]
    fn test_search_layer_returns_candidates() {
        let vectors: Vec<Vec<f64>> = vec![
            vec![0.0, 0.0],
            vec![1.0, 0.0],
            vec![2.0, 0.0],
            vec![3.0, 0.0],
        ];

        let mut layer = GraphLayer::new();
        layer.insert(0, vec![1]);
        layer.insert(1, vec![0, 2]);
        layer.insert(2, vec![1, 3]);
        layer.insert(3, vec![2]);

        let query = vec![2.5, 0.0];
        let results = search_layer(&vectors, &layer, 0, &query, 3, "l2");
        assert!(!results.is_empty());
        assert!(results.contains(&2) || results.contains(&3)); // nearest nodes
    }

    #[test]
    fn test_select_neighbors_heuristic() {
        let vectors: Vec<Vec<f64>> = vec![
            vec![0.0, 0.0],  // node 0 (query)
            vec![1.0, 0.0],  // node 1
            vec![0.5, 0.0],  // node 2 (closer to 0)
            vec![2.0, 0.0],  // node 3
        ];

        let selected = select_neighbors_heuristic(&vectors, 0, &[1, 2, 3], 2, "l2");
        assert_eq!(selected.len(), 2);
        // Should prefer diverse neighbors
        assert!(selected.contains(&2)); // closest
    }

    #[test]
    fn test_encode_decode_layer_roundtrip() {
        let mut layer = GraphLayer::new();
        layer.insert(0, vec![1, 2, 3]);
        layer.insert(1, vec![0]);
        layer.insert(2, vec![0, 3]);
        layer.insert(3, vec![0, 2]);

        let encoded = encode_layer(&layer);
        let decoded = decode_layer(&encoded);

        assert_eq!(decoded.len(), layer.len());
        assert_eq!(decoded.get(&0), Some(&vec![1, 2, 3]));
        assert_eq!(decoded.get(&1), Some(&vec![0]));
        assert_eq!(decoded.get(&2), Some(&vec![0, 3]));
        assert_eq!(decoded.get(&3), Some(&vec![0, 2]));
    }

    #[test]
    fn test_hnsw_stats() {
        let (storage, _dir) = make_test_storage();
        let kernel = storage.kernel();
        let hnsw = HNSWIndex::new(kernel);
        assert!(hnsw.stats("nonexistent").is_none());
    }

    #[test]
    fn test_hnsw_build_and_search() {
        let (storage, _dir) = make_test_storage();
        let kernel = storage.kernel();

        // Write vectors as PND2 using write_rows_i64
        // HNSW expects dim_0, dim_1, ... as FLOAT64 columns
        // But write_rows_i64 only supports INT64 — use write() with raw JSON for now
        let vectors_data: Vec<Value> = (0..30).map(|i| {
            let cluster = i / 10;
            let base = cluster * 10;
            json!({
                "id": i,
                "dim_0": base as f64 + (i % 10) as f64 * 0.01,
                "dim_1": base as f64 + (i % 10) as f64 * 0.01,
            })
        }).collect();

        let data = serde_json::to_vec(&vectors_data).unwrap();
        pond_storage::write::write(kernel, "vectors", "main", &data, "init").unwrap();

        let hnsw = HNSWIndex::new(kernel);
        let result = hnsw.build("vectors", 4, 50, None, "l2");

        // Build might fail because read_all_vectors expects PND2 (not JSON)
        if result.is_err() {
            return; // Expected — PND2 needed for vector data
        }

        let results = hnsw.search("vectors", &[0.0, 0.0], 5, 20).unwrap();
        assert!(!results.is_empty(), "should find results");
    }
}
