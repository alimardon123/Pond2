// Pond MCP Server — JSON-RPC 2.0 over stdio for AI agent integration.
//
// This binary exposes the Pond storage layer to AI agents via the Model
// Context Protocol (MCP). It speaks JSON-RPC 2.0 over stdio (one JSON
// object per line — NDJSON format), which is the transport MCP uses for
// local-process servers.
//
// # Supported methods
//
//   - `initialize`     → server info + capabilities (handshake)
//   - `tools/list`     → lists the 9 Pond tools
//   - `tools/call`     → executes a tool by name with params
//
// # Tools
//
//   1. pond_write_rows       — write rows to a collection
//   2. pond_read_rows        — read rows (with WHERE / projection / limit)
//   3. pond_sql              — execute a SQL statement
//   4. pond_list_collections — list all collections
//   5. pond_branch           — create a branch
//   6. pond_merge            — merge a source branch into a target
//   7. pond_vacuum           — garbage-collect unreachable blobs
//   8. pond_get_schema       — return a collection's column schema
//   9. pond_search_vectors   — vector similarity search (L2/cosine/dot)
//
// # Storage discovery (priority order)
//
//   1. `--root <path>` CLI flag
//   2. `POND_ROOT` environment variable
//   3. `.pond/` marker directory, walking up from CWD (like git)
//   4. `.` (current directory) — last-resort fallback
//
// # Wire format
//
// Each line on stdin is one JSON-RPC request. Each line on stdout is one
// JSON-RPC response. Notifications (no `id`) get no response (per spec).
// Errors are returned as JSON-RPC error objects with `code`, `message`,
// and optional `data`.

use pond_core::{pnd2_decode, search::hybrid_search, TypedColumn, VT_FLOAT64, VT_INT64, VT_STRING};
use pond_storage::manifest::CollectionManifest;
use pond_storage::{branch, commit, maintenance::GarbageCollector, read, write, UnifiedStorage};
use pond_sql::execute as sql_execute;
use serde_json::{json, Value as JsonValue};
use std::io::{self, BufRead, Write};
use std::path::PathBuf;

// ---------------------------------------------------------------------------
// Server constants
// ---------------------------------------------------------------------------

const SERVER_NAME: &str = "pond-mcp-server";
const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");
const PROTOCOL_VERSION: &str = "2024-11-05"; // MCP protocol version

const ERROR_CODE_PARSE: i32 = -32700;
const ERROR_CODE_INVALID_REQUEST: i32 = -32600;
const ERROR_CODE_METHOD_NOT_FOUND: i32 = -32601;
const ERROR_CODE_INVALID_PARAMS: i32 = -32602;
const ERROR_CODE_INTERNAL: i32 = -32603;

// ---------------------------------------------------------------------------
// Tool descriptors
// ---------------------------------------------------------------------------

/// Return the list of 9 Pond MCP tools, each with name, description, and
/// JSON Schema for its parameters.
fn tool_list() -> Vec<JsonValue> {
    vec![
        json!({
            "name": "pond_write_rows",
            "description": "Write rows to a collection on the active branch. Creates a new commit. Rows are encoded as a PND2 blob with auto-selected per-column encoding.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "collection": { "type": "string", "description": "Collection name" },
                    "rows": {
                        "type": "array",
                        "description": "Array of row objects (JSON dicts). All rows should have the same keys.",
                        "items": { "type": "object" }
                    },
                    "message": { "type": "string", "description": "Commit message (default: 'mcp write')" }
                },
                "required": ["collection", "rows"]
            }
        }),
        json!({
            "name": "pond_read_rows",
            "description": "Read rows from a collection's HEAD (active branch). Optional WHERE filter, column projection, and row limit.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "collection": { "type": "string" },
                    "where": { "type": "string", "description": "SQL WHERE clause, e.g. \"age >= 18 AND city = 'NYC'\"" },
                    "columns": { "type": "array", "items": { "type": "string" }, "description": "Columns to project (default: all)" },
                    "limit": { "type": "integer", "description": "Max rows to return (default: 100)" }
                },
                "required": ["collection"]
            }
        }),
        json!({
            "name": "pond_sql",
            "description": "Execute a SQL statement (SELECT / INSERT / UPDATE / DELETE / MERGE). Returns columnar results for SELECT, or status dicts for mutations.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": { "type": "string", "description": "The SQL statement to execute" }
                },
                "required": ["query"]
            }
        }),
        json!({
            "name": "pond_list_collections",
            "description": "List all collections in the storage. Returns a sorted array of collection names.",
            "inputSchema": { "type": "object", "properties": {} }
        }),
        json!({
            "name": "pond_branch",
            "description": "Create a new branch from the active branch (like `git branch`). Does NOT switch the active branch.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "collection": { "type": "string" },
                    "branch": { "type": "string", "description": "Name of the new branch" }
                },
                "required": ["collection", "branch"]
            }
        }),
        json!({
            "name": "pond_merge",
            "description": "Merge a source branch into a target branch (like `git merge`). Writes a merge commit with two parents.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "collection": { "type": "string" },
                    "source": { "type": "string", "description": "Source branch name" },
                    "target": { "type": "string", "description": "Target branch name (default: active branch)" },
                    "message": { "type": "string", "description": "Merge commit message" }
                },
                "required": ["collection", "source"]
            }
        }),
        json!({
            "name": "pond_vacuum",
            "description": "Garbage-collect unreachable blobs. Optionally preserve commits younger than N days. Supports dry-run mode.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "preserve_days": { "type": "integer", "description": "Keep commits younger than N days (default: 0)" },
                    "dry_run": { "type": "boolean", "description": "If true, report what would be deleted without deleting (default: false)" }
                }
            }
        }),
        json!({
            "name": "pond_get_schema",
            "description": "Get the column schema for a collection. Returns the column names and value types from the HEAD manifest.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "collection": { "type": "string" }
                },
                "required": ["collection"]
            }
        }),
        json!({
            "name": "pond_search_vectors",
            "description": "Vector similarity search over a collection's HEAD rows. Reads the named vector column, computes distance to the query vector, and returns the top-k closest rows.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "collection": { "type": "string" },
                    "vector_column": { "type": "string", "description": "Name of the column holding the vectors" },
                    "query": { "type": "array", "items": { "type": "number" }, "description": "Query vector" },
                    "metric": { "type": "string", "enum": ["l2", "cosine", "dot"], "description": "Distance metric (default: l2)" },
                    "k": { "type": "integer", "description": "Number of results to return (default: 10)" }
                },
                "required": ["collection", "vector_column", "query"]
            }
        }),
    ]
}

// ---------------------------------------------------------------------------
// JSON-RPC dispatch
// ---------------------------------------------------------------------------

/// A JSON-RPC response — either a success (`result` set) or an error
/// (`error` set). The `id` mirrors the request's `id`.
#[derive(Debug)]
struct RpcResponse {
    id: JsonValue,
    result: Option<JsonValue>,
    error: Option<RpcError>,
}

#[derive(Debug)]
struct RpcError {
    code: i32,
    message: String,
    data: Option<JsonValue>,
}

impl RpcResponse {
    fn success(id: JsonValue, result: JsonValue) -> Self {
        Self { id, result: Some(result), error: None }
    }

    fn error(id: JsonValue, code: i32, message: impl Into<String>) -> Self {
        Self {
            id,
            result: None,
            error: Some(RpcError { code, message: message.into(), data: None }),
        }
    }

    fn error_with_data(
        id: JsonValue,
        code: i32,
        message: impl Into<String>,
        data: JsonValue,
    ) -> Self {
        Self {
            id,
            result: None,
            error: Some(RpcError { code, message: message.into(), data: Some(data) }),
        }
    }

    /// Serialize to a JSON-RPC 2.0 response object.
    fn to_json(&self) -> JsonValue {
        let mut obj = serde_json::Map::new();
        obj.insert("jsonrpc".to_string(), JsonValue::String("2.0".to_string()));
        obj.insert("id".to_string(), self.id.clone());
        if let Some(result) = &self.result {
            obj.insert("result".to_string(), result.clone());
        }
        if let Some(error) = &self.error {
            let mut err = serde_json::Map::new();
            err.insert("code".to_string(), JsonValue::Number(error.code.into()));
            err.insert("message".to_string(), JsonValue::String(error.message.clone()));
            if let Some(data) = &error.data {
                err.insert("data".to_string(), data.clone());
            }
            obj.insert("error".to_string(), JsonValue::Object(err));
        }
        JsonValue::Object(obj)
    }
}

/// Dispatch a single JSON-RPC request against the storage.
///
/// This is the main entry point — it's split out from the I/O loop so it
/// can be unit-tested directly (without going through stdin/stdout).
fn dispatch(storage: &UnifiedStorage, request: &JsonValue) -> Option<RpcResponse> {
    // Validate it's a JSON-RPC 2.0 request.
    let obj = match request.as_object() {
        Some(o) => o,
        None => {
            let id = request.get("id").cloned().unwrap_or(JsonValue::Null);
            return Some(RpcResponse::error(
                id,
                ERROR_CODE_INVALID_REQUEST,
                "Request must be a JSON object",
            ));
        }
    };

    let jsonrpc = obj.get("jsonrpc").and_then(|v| v.as_str());
    if jsonrpc != Some("2.0") {
        let id = obj.get("id").cloned().unwrap_or(JsonValue::Null);
        return Some(RpcResponse::error(
            id,
            ERROR_CODE_INVALID_REQUEST,
            "Missing or invalid 'jsonrpc' field (must be \"2.0\")",
        ));
    }

    let method = match obj.get("method").and_then(|v| v.as_str()) {
        Some(m) => m,
        None => {
            let id = obj.get("id").cloned().unwrap_or(JsonValue::Null);
            return Some(RpcResponse::error(
                id,
                ERROR_CODE_INVALID_REQUEST,
                "Missing 'method' field",
            ));
        }
    };

    let id = obj.get("id").cloned();
    let params = obj.get("params").cloned().unwrap_or(JsonValue::Null);

    // Notifications (no `id`) get no response, per spec.
    let id = id?;

    match method {
        "initialize" => Some(handle_initialize(id, &params)),
        "tools/list" => Some(handle_tools_list(id, &params)),
        "tools/call" => Some(handle_tools_call(storage, id, &params)),
        _ => Some(RpcResponse::error(
            id,
            ERROR_CODE_METHOD_NOT_FOUND,
            format!("Unknown method: '{}'", method),
        )),
    }
}

// ---------------------------------------------------------------------------
// Method handlers
// ---------------------------------------------------------------------------

fn handle_initialize(id: JsonValue, _params: &JsonValue) -> RpcResponse {
    let result = json!({
        "protocolVersion": PROTOCOL_VERSION,
        "serverInfo": {
            "name": SERVER_NAME,
            "version": SERVER_VERSION,
        },
        "capabilities": {
            "tools": {
                "listChanged": false
            }
        }
    });
    RpcResponse::success(id, result)
}

fn handle_tools_list(id: JsonValue, _params: &JsonValue) -> RpcResponse {
    let result = json!({ "tools": tool_list() });
    RpcResponse::success(id, result)
}

fn handle_tools_call(storage: &UnifiedStorage, id: JsonValue, params: &JsonValue) -> RpcResponse {
    let name = match params.get("name").and_then(|v| v.as_str()) {
        Some(n) => n,
        None => return RpcResponse::error(
            id,
            ERROR_CODE_INVALID_PARAMS,
            "Missing 'name' in tools/call params",
        ),
    };
    let args = params.get("arguments").cloned().unwrap_or(JsonValue::Object(Default::default()));

    let result = match name {
        "pond_write_rows" => tool_write_rows(storage, &args),
        "pond_read_rows" => tool_read_rows(storage, &args),
        "pond_sql" => tool_sql(storage, &args),
        "pond_list_collections" => tool_list_collections(storage, &args),
        "pond_branch" => tool_branch(storage, &args),
        "pond_merge" => tool_merge(storage, &args),
        "pond_vacuum" => tool_vacuum(storage, &args),
        "pond_get_schema" => tool_get_schema(storage, &args),
        "pond_search_vectors" => tool_search_vectors(storage, &args),
        _ => return RpcResponse::error(
            id,
            ERROR_CODE_METHOD_NOT_FOUND,
            format!("Unknown tool: '{}'", name),
        ),
    };

    match result {
        Ok(value) => RpcResponse::success(id, value),
        Err(msg) => RpcResponse::error_with_data(
            id,
            ERROR_CODE_INTERNAL,
            format!("Tool '{}' failed: {}", name, msg),
            json!({ "tool": name, "error": msg }),
        ),
    }
}

// ---------------------------------------------------------------------------
// Tool implementations
// ---------------------------------------------------------------------------

/// Convert a JSON array of row objects into typed columns suitable for
/// `write_rows`. Infers each column's type from its values.
fn json_rows_to_typed_columns(rows: &[JsonValue]) -> Result<Vec<(String, TypedColumn)>, String> {
    if rows.is_empty() {
        return Err("rows is empty".to_string());
    }

    // Collect column names from the first row (assume all rows have the same keys).
    let first = rows[0].as_object()
        .ok_or_else(|| "each row must be a JSON object".to_string())?;
    let col_names: Vec<String> = first.keys().cloned().collect();

    let mut columns: Vec<(String, TypedColumn)> = Vec::with_capacity(col_names.len());
    for name in &col_names {
        // Classify each value's type.
        let mut has_i64 = false;
        let mut has_f64 = false;
        let mut has_string = false;
        let mut has_bool = false;
        let mut has_other = false;
        for row in rows {
            match row.get(name) {
                Some(JsonValue::Number(n)) if n.is_i64() => has_i64 = true,
                Some(JsonValue::Number(n)) if n.is_f64() => has_f64 = true,
                Some(JsonValue::Number(_)) => has_f64 = true, // u64 or other → f64
                Some(JsonValue::String(_)) => has_string = true,
                Some(JsonValue::Bool(_)) => has_bool = true,
                Some(JsonValue::Null) => {} // skip nulls
                _ => has_other = true,
            }
        }

        // Type inference: prefer string > float > int > bool > variant.
        let typed = if has_string || has_other {
            let vals: Vec<String> = rows.iter().map(|r| {
                match r.get(name) {
                    Some(JsonValue::String(s)) => s.clone(),
                    Some(v) => v.to_string(),
                    None => String::new(),
                }
            }).collect();
            TypedColumn::String(vals)
        } else if has_f64 {
            let vals: Vec<f64> = rows.iter().map(|r| {
                r.get(name).and_then(|v| v.as_f64()).unwrap_or(0.0)
            }).collect();
            TypedColumn::Float64(vals)
        } else if has_i64 {
            let vals: Vec<i64> = rows.iter().map(|r| {
                r.get(name).and_then(|v| v.as_i64()).unwrap_or(0)
            }).collect();
            TypedColumn::Int64(vals)
        } else if has_bool {
            let vals: Vec<String> = rows.iter().map(|r| {
                match r.get(name).and_then(|v| v.as_bool()) {
                    Some(true) => "true".to_string(),
                    Some(false) => "false".to_string(),
                    None => String::new(),
                }
            }).collect();
            TypedColumn::String(vals)
        } else {
            // All nulls — default to Int64 with zeros.
            TypedColumn::Int64(vec![0; rows.len()])
        };
        columns.push((name.clone(), typed));
    }
    Ok(columns)
}

fn tool_write_rows(storage: &UnifiedStorage, args: &JsonValue) -> Result<JsonValue, String> {
    let collection = args.get("collection")
        .and_then(|v| v.as_str())
        .ok_or("missing 'collection' (string)")?;
    let rows = args.get("rows")
        .and_then(|v| v.as_array())
        .ok_or("missing 'rows' (array of objects)")?;
    if rows.is_empty() {
        return Err("'rows' must be a non-empty array".to_string());
    }
    let message = args.get("message")
        .and_then(|v| v.as_str())
        .unwrap_or("mcp write");

    let typed_cols = json_rows_to_typed_columns(rows)?;
    let col_refs: Vec<(&str, TypedColumn)> = typed_cols.iter()
        .map(|(n, c)| (n.as_str(), c.clone()))
        .collect();
    let active = storage.get_active_branch(collection);
    let commit_hash = write::write_rows(storage.kernel(), collection, &active, &col_refs, message)?;

    Ok(json!({
        "commit": commit_hash,
        "collection": collection,
        "branch": active,
        "rows_written": rows.len(),
    }))
}

fn tool_read_rows(storage: &UnifiedStorage, args: &JsonValue) -> Result<JsonValue, String> {
    let collection = args.get("collection")
        .and_then(|v| v.as_str())
        .ok_or("missing 'collection' (string)")?;
    let limit = args.get("limit")
        .and_then(|v| v.as_u64())
        .unwrap_or(100) as usize;
    let columns: Option<Vec<String>> = args.get("columns")
        .and_then(|v| v.as_array())
        .map(|a| a.iter().filter_map(|x| x.as_str().map(|s| s.to_string())).collect());

    // Use the SQL executor to support WHERE / projection / limit uniformly.
    // Build a SQL query string.
    let col_clause = match &columns {
        Some(cs) if !cs.is_empty() => cs.join(", "),
        _ => "*".to_string(),
    };
    let limit_clause = format!("LIMIT {}", limit);
    let where_clause = args.get("where")
        .and_then(|v| v.as_str())
        .map(|w| format!("WHERE {}", w))
        .unwrap_or_default();

    let query = format!(
        "SELECT {} FROM {} {} {}",
        col_clause, collection, where_clause, limit_clause
    );
    let result = sql_execute(storage, &query)?;
    Ok(json!({
        "columns": result.columns,
        "rows": result.rows,
        "n_rows": result.rows.len(),
    }))
}

fn tool_sql(storage: &UnifiedStorage, args: &JsonValue) -> Result<JsonValue, String> {
    let query = args.get("query")
        .and_then(|v| v.as_str())
        .ok_or("missing 'query' (string)")?;
    let result = sql_execute(storage, query)?;
    Ok(json!({
        "columns": result.columns,
        "rows": result.rows,
        "n_rows": result.rows.len(),
    }))
}

fn tool_list_collections(storage: &UnifiedStorage, _args: &JsonValue) -> Result<JsonValue, String> {
    let kernel = storage.kernel();
    let names = kernel.list_names_prefix("collections/");
    let mut collections: Vec<String> = names.iter()
        .filter_map(|n| {
            let parts: Vec<&str> = n.split('/').collect();
            if parts.len() >= 2 && parts[0] == "collections" {
                Some(parts[1].to_string())
            } else {
                None
            }
        })
        .collect();
    collections.sort();
    collections.dedup();
    Ok(json!({ "collections": collections }))
}

fn tool_branch(storage: &UnifiedStorage, args: &JsonValue) -> Result<JsonValue, String> {
    let collection = args.get("collection")
        .and_then(|v| v.as_str())
        .ok_or("missing 'collection'")?;
    let branch_name = args.get("branch")
        .and_then(|v| v.as_str())
        .ok_or("missing 'branch' (new branch name)")?;
    let active = storage.get_active_branch(collection);
    let head = branch::branch(storage.kernel(), collection, branch_name, &active)?;
    Ok(json!({
        "collection": collection,
        "branch": branch_name,
        "head_commit": head,
    }))
}

fn tool_merge(storage: &UnifiedStorage, args: &JsonValue) -> Result<JsonValue, String> {
    let collection = args.get("collection")
        .and_then(|v| v.as_str())
        .ok_or("missing 'collection'")?;
    let source = args.get("source")
        .and_then(|v| v.as_str())
        .ok_or("missing 'source' (source branch name)")?;
    let target = args.get("target")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| storage.get_active_branch(collection));
    let message = args.get("message")
        .and_then(|v| v.as_str())
        .unwrap_or("merge");
    let merge_hash = branch::merge(storage.kernel(), collection, source, &target, message)?;
    Ok(json!({
        "collection": collection,
        "source": source,
        "target": target,
        "merge_commit": merge_hash,
    }))
}

fn tool_vacuum(storage: &UnifiedStorage, args: &JsonValue) -> Result<JsonValue, String> {
    let preserve_days = args.get("preserve_days")
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as u32;
    let dry_run = args.get("dry_run")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let gc = GarbageCollector::new(storage.kernel());
    // C17: vacuum aborts on ref-read failures (deleting blobs whose
    // reachability could not be established would destroy live data).
    let result = gc.vacuum(None, preserve_days, dry_run)?;
    Ok(json!({
        "deleted": result.deleted,
        "preserved": result.preserved,
        "freed_bytes": result.freed_bytes,
        "dry_run": result.dry_run,
    }))
}

fn tool_get_schema(storage: &UnifiedStorage, args: &JsonValue) -> Result<JsonValue, String> {
    let collection = args.get("collection")
        .and_then(|v| v.as_str())
        .ok_or("missing 'collection'")?;
    let kernel = storage.kernel();
    let active = storage.get_active_branch(collection);

    // Resolve HEAD, decode manifest, extract schema.
    // C17: a FAILED branch-ref read is an Err — an outage is not a fresh
    // collection (distinct from the "has no commits" arm).
    let head = kernel.resolve(&pond_storage::branch_ref(collection, &active))
        .map_err(|e| format!(
            "Failed to read branch ref for collection '{}': {}", collection, e))?
        .ok_or_else(|| format!("Collection '{}' has no commits", collection))?;

    let manifest_bytes = commit::resolve_manifest_bytes(kernel, &head)
        .map_err(|e| format!("Failed to read manifest: {}", e))?;

    let manifest = CollectionManifest::decode(&manifest_bytes)
        .ok_or_else(|| "Failed to decode manifest".to_string())?;

    // Build a JSON description of the schema.
    let columns: Vec<JsonValue> = manifest.columns.iter()
        .map(|(name, vtype)| {
            let type_name = match *vtype {
                VT_INT64 => "INT64",
                VT_FLOAT64 => "FLOAT64",
                VT_STRING => "STRING",
                _ => "OTHER",
            };
            json!({ "name": name, "type": type_name, "code": vtype })
        })
        .collect();

    Ok(json!({
        "collection": collection,
        "branch": active,
        "key_column": manifest.key_col,
        "columns": columns,
        "n_row_groups": manifest.row_groups.len(),
        "n_rows": manifest.row_groups.iter().map(|rg| rg.n_rows as u64).sum::<u64>(),
    }))
}

fn tool_search_vectors(storage: &UnifiedStorage, args: &JsonValue) -> Result<JsonValue, String> {
    let collection = args.get("collection")
        .and_then(|v| v.as_str())
        .ok_or("missing 'collection'")?;
    let vector_column = args.get("vector_column")
        .and_then(|v| v.as_str())
        .ok_or("missing 'vector_column'")?;
    let query: Vec<f32> = args.get("query")
        .and_then(|v| v.as_array())
        .ok_or("missing 'query' (array of numbers)")?
        .iter()
        .filter_map(|x| x.as_f64().map(|f| f as f32))
        .collect();
    if query.is_empty() {
        return Err("'query' must be a non-empty array of numbers".to_string());
    }
    let metric = args.get("metric")
        .and_then(|v| v.as_str())
        .unwrap_or("l2");
    let k = args.get("k")
        .and_then(|v| v.as_u64())
        .unwrap_or(10) as usize;

    // Read HEAD rows as JSON values.
    let rows = read_head_rows_as_json(storage, collection)?;
    if rows.is_empty() {
        return Ok(json!({ "hits": [], "n_rows": 0 }));
    }

    let weights = pond_core::search::SearchWeights::default();
    let hits = hybrid_search(
        &rows,
        vector_column,
        &query,
        &[],         // no text columns
        "",
        None,
        weights,
        k,
        metric,
    );

    let hits_json: Vec<JsonValue> = hits.iter().map(|h| {
        json!({
            "row": h.row,
            "score": h.score,
            "vector_distance": h.vector_distance,
        })
    }).collect();

    Ok(json!({
        "hits": hits_json,
        "n_hits": hits_json.len(),
        "n_rows_searched": rows.len(),
        "metric": metric,
    }))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Read the HEAD data blob of a collection and decode it to JSON rows
/// (transposing PND2 columns → row-oriented JSON). For collections written
/// via `write_rows`, this returns one JSON object per row.
///
/// Note: this is a simplified read path — it doesn't merge CRDT shards.
/// For full CRDT semantics, use `pond_read_rows` (which goes through the
/// SQL executor and does shard merging).
fn read_head_rows_as_json(
    storage: &UnifiedStorage,
    collection: &str,
) -> Result<Vec<JsonValue>, String> {
    let kernel = storage.kernel();
    let active = storage.get_active_branch(collection);
    let data = read::read(kernel, collection, &active)?;

    // Try PND2 first.
    if data.len() >= 4 && &data[0..4] == b"PND2" {
        let cols = pnd2_decode(&data)
            .map_err(|e| format!("Failed to decode PND2: {}", e))?;
        return Ok(pnd2_columns_to_json_rows(&cols));
    }

    // Fall back to JSON array of objects.
    if data.first() == Some(&b'[') {
        let rows: Vec<JsonValue> = serde_json::from_slice(&data)
            .map_err(|e| format!("Failed to parse JSON rows: {}", e))?;
        return Ok(rows);
    }

    // Unknown format — return a single row with the raw bytes as a hex string.
    let hex_str: String = data.iter().map(|b| format!("{:02x}", b)).collect();
    Ok(vec![json!({ "data_hex": hex_str, "n_bytes": data.len() })])
}

/// Transpose PND2 columns → row-oriented JSON. Each column contributes
/// its value at row `i` to the row object.
///
/// STRING columns whose value parses as a JSON array of numbers are
/// converted to actual JSON arrays — this lets vector columns written
/// via `write_rows` (which stores them as JSON-serialized strings) be
/// searched via `pond_search_vectors` without a separate decode path.
fn pnd2_columns_to_json_rows(cols: &[pond_core::PondColumn]) -> Vec<JsonValue> {
    let n_rows = cols.first().map(|c| c.n_values).unwrap_or(0);
    let mut rows = Vec::with_capacity(n_rows);
    for i in 0..n_rows {
        let mut row = serde_json::Map::new();
        for col in cols {
            let name = col.name.to_str().unwrap_or("").to_string();
            if name.is_empty() { continue; }
            let val = match col.vtype {
                VT_INT64 => col.i64_data.get(i).map(|x| json!(x)),
                VT_FLOAT64 => col.f64_data.get(i).and_then(|x| {
                    serde_json::Number::from_f64(*x).map(JsonValue::Number)
                }),
                VT_STRING => col.str_data.get(i).map(|s| {
                    let s_str = s.to_str().unwrap_or("");
                    // If the string parses as a JSON array, use the array form
                    // (so vector columns stored as JSON-serialized strings are
                    // searchable via pond_search_vectors).
                    if s_str.starts_with('[') && s_str.ends_with(']') {
                        if let Ok(arr) = serde_json::from_str::<JsonValue>(s_str) {
                            if arr.is_array() {
                                return arr;
                            }
                        }
                    }
                    json!(s_str)
                }),
                _ => None,
            };
            if let Some(v) = val {
                row.insert(name, v);
            }
        }
        rows.push(JsonValue::Object(row));
    }
    rows
}

/// Discover the storage root using the priority order:
///   1. explicit `--root` flag
///   2. `POND_ROOT` env var
///   3. `.pond/` marker walking up from CWD
///   4. `.` (current directory)
fn discover_root(explicit: Option<&str>) -> PathBuf {
    if let Some(p) = explicit {
        return PathBuf::from(p);
    }
    if let Ok(p) = std::env::var("POND_ROOT") {
        return PathBuf::from(p);
    }
    // Walk up from CWD looking for a .pond/ marker.
    let mut cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    loop {
        let marker = cwd.join(".pond");
        if marker.is_dir() {
            return cwd;
        }
        if !cwd.pop() {
            break;
        }
    }
    PathBuf::from(".")
}

// ---------------------------------------------------------------------------
// Main loop — stdio NDJSON server
// ---------------------------------------------------------------------------

fn main() -> io::Result<()> {
    // Parse args: --root <path>
    let mut root_arg: Option<String> = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--root" {
            root_arg = args.next();
        } else if let Some(rest) = arg.strip_prefix("--root=") {
            root_arg = Some(rest.to_string());
        } else if arg == "--help" || arg == "-h" {
            println!("Pond MCP Server — JSON-RPC 2.0 over stdio");
            println!();
            println!("Usage: pond-mcp-server [--root <path>]");
            println!();
            println!("Storage discovery (priority order):");
            println!("  1. --root <path>");
            println!("  2. POND_ROOT env var");
            println!("  3. .pond/ marker walking up from CWD");
            println!("  4. current directory");
            return Ok(());
        }
    }

    let root = discover_root(root_arg.as_deref());

    let storage = UnifiedStorage::new_local(&root)
        .map_err(|e| io::Error::other(format!("Failed to open storage at {}: {}", root.display(), e)))?;

    let stdin = io::stdin();
    let stdout = io::stdout();
    let mut stdout_handle = stdout.lock();

    let mut line = String::new();
    loop {
        line.clear();
        let n = stdin.lock().read_line(&mut line)?;
        if n == 0 {
            // EOF — client closed stdin.
            break;
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        // Parse the JSON-RPC request.
        let request: JsonValue = match serde_json::from_str(trimmed) {
            Ok(v) => v,
            Err(e) => {
                // Parse error — send back an error response with id=null.
                let resp = RpcResponse::error(
                    JsonValue::Null,
                    ERROR_CODE_PARSE,
                    format!("Parse error: {}", e),
                );
                writeln!(stdout_handle, "{}", resp.to_json())?;
                stdout_handle.flush()?;
                continue;
            }
        };

        // Dispatch.
        match dispatch(&storage, &request) {
            Some(response) => {
                writeln!(stdout_handle, "{}", response.to_json())?;
                stdout_handle.flush()?;
            }
            None => {
                // Notification — no response per spec.
            }
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::tempdir;

    /// Helper: create a fresh UnifiedStorage in a temp dir.
    fn make_storage() -> (tempfile::TempDir, UnifiedStorage) {
        let dir = tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        (dir, storage)
    }

    /// Helper: build a JSON-RPC request object.
    fn rpc_request(id: i64, method: &str, params: Option<JsonValue>) -> JsonValue {
        let mut obj = serde_json::Map::new();
        obj.insert("jsonrpc".to_string(), JsonValue::String("2.0".to_string()));
        obj.insert("id".to_string(), JsonValue::Number(id.into()));
        obj.insert("method".to_string(), JsonValue::String(method.to_string()));
        if let Some(p) = params {
            obj.insert("params".to_string(), p);
        }
        JsonValue::Object(obj)
    }

    // ---- initialize handshake ----

    #[test]
    fn test_initialize_returns_server_info() {
        let (_dir, storage) = make_storage();
        let req = rpc_request(1, "initialize", Some(json!({})));
        let resp = dispatch(&storage, &req).expect("should return a response");
        assert_eq!(resp.id, json!(1));
        let result = resp.result.expect("should have a result");
        assert_eq!(result["protocolVersion"], json!(PROTOCOL_VERSION));
        assert_eq!(result["serverInfo"]["name"], json!(SERVER_NAME));
        assert!(result["serverInfo"]["version"].is_string());
        assert!(result["capabilities"]["tools"]["listChanged"].is_boolean());
    }

    #[test]
    fn test_initialize_ignores_params() {
        let (_dir, storage) = make_storage();
        let req = rpc_request(7, "initialize", Some(json!({"clientInfo": {"name": "test"}})));
        let resp = dispatch(&storage, &req).expect("should return a response");
        assert_eq!(resp.id, json!(7));
        assert!(resp.result.is_some());
    }

    // ---- tools/list ----

    #[test]
    fn test_tools_list_returns_nine_tools() {
        let (_dir, storage) = make_storage();
        let req = rpc_request(2, "tools/list", Some(json!({})));
        let resp = dispatch(&storage, &req).expect("should return a response");
        assert_eq!(resp.id, json!(2));
        let result = resp.result.expect("should have a result");
        let tools = result["tools"].as_array().expect("tools should be an array");
        assert_eq!(tools.len(), 9, "should return exactly 9 tools");

        let names: Vec<&str> = tools.iter()
            .map(|t| t["name"].as_str().unwrap())
            .collect();
        assert!(names.contains(&"pond_write_rows"));
        assert!(names.contains(&"pond_read_rows"));
        assert!(names.contains(&"pond_sql"));
        assert!(names.contains(&"pond_list_collections"));
        assert!(names.contains(&"pond_branch"));
        assert!(names.contains(&"pond_merge"));
        assert!(names.contains(&"pond_vacuum"));
        assert!(names.contains(&"pond_get_schema"));
        assert!(names.contains(&"pond_search_vectors"));
    }

    #[test]
    fn test_tools_list_each_tool_has_schema() {
        let (_dir, storage) = make_storage();
        let req = rpc_request(3, "tools/list", None);
        let resp = dispatch(&storage, &req).expect("response");
        let result = resp.result.unwrap();
        let tools = result["tools"].as_array().unwrap();
        for tool in tools {
            assert!(tool["name"].is_string(), "tool name missing");
            assert!(tool["description"].is_string(), "tool description missing");
            assert!(tool["inputSchema"]["type"] == "object", "tool inputSchema missing");
        }
    }

    // ---- write + read round-trip ----

    #[test]
    fn test_write_rows_then_read_back() {
        let (_dir, storage) = make_storage();
        // Write 3 rows.
        let write_args = json!({
            "collection": "users",
            "rows": [
                {"id": 1, "name": "alice", "age": 30i64},
                {"id": 2, "name": "bob", "age": 25i64},
                {"id": 3, "name": "carol", "age": 35i64},
            ],
            "message": "seed users"
        });
        let req = rpc_request(10, "tools/call", Some(json!({
            "name": "pond_write_rows",
            "arguments": write_args,
        })));
        let resp = dispatch(&storage, &req).expect("response");
        assert!(resp.error.is_none(), "write should succeed: {:?}", resp.error);
        let result = resp.result.unwrap();
        assert!(result["commit"].is_string(), "should return commit hash");
        assert_eq!(result["rows_written"], json!(3));

        // Read them back via SQL.
        let read_args = json!({"collection": "users"});
        let req = rpc_request(11, "tools/call", Some(json!({
            "name": "pond_read_rows",
            "arguments": read_args,
        })));
        let resp = dispatch(&storage, &req).expect("response");
        assert!(resp.error.is_none(), "read should succeed: {:?}", resp.error);
        let result = resp.result.unwrap();
        let n_rows = result["n_rows"].as_u64().unwrap();
        assert_eq!(n_rows, 3, "should read 3 rows back, got: {}", result);
    }

    #[test]
    fn test_write_rows_with_limit() {
        let (_dir, storage) = make_storage();
        // Write 5 rows.
        let rows: Vec<JsonValue> = (1..=5)
            .map(|i| json!({"id": i, "label": format!("row{}", i)}))
            .collect();
        let write_args = json!({
            "collection": "items",
            "rows": rows,
        });
        let req = rpc_request(20, "tools/call", Some(json!({
            "name": "pond_write_rows", "arguments": write_args,
        })));
        let _ = dispatch(&storage, &req).expect("write response").result.unwrap();

        // Read with limit=2.
        let read_args = json!({"collection": "items", "limit": 2});
        let req = rpc_request(21, "tools/call", Some(json!({
            "name": "pond_read_rows", "arguments": read_args,
        })));
        let resp = dispatch(&storage, &req).expect("read response");
        let result = resp.result.unwrap();
        assert_eq!(result["n_rows"], json!(2));
    }

    // ---- list_collections ----

    #[test]
    fn test_list_collections_empty() {
        let (_dir, storage) = make_storage();
        let req = rpc_request(30, "tools/call", Some(json!({
            "name": "pond_list_collections",
            "arguments": {},
        })));
        let resp = dispatch(&storage, &req).expect("response");
        let result = resp.result.unwrap();
        assert_eq!(result["collections"], json!([]));
    }

    #[test]
    fn test_list_collections_after_write() {
        let (_dir, storage) = make_storage();
        // Write to two collections.
        for coll in ["users", "orders"] {
            let args = json!({
                "collection": coll,
                "rows": [{"id": 1i64}],
            });
            let req = rpc_request(31, "tools/call", Some(json!({
                "name": "pond_write_rows", "arguments": args,
            })));
            let _ = dispatch(&storage, &req).expect("write").result.unwrap();
        }
        let req = rpc_request(32, "tools/call", Some(json!({
            "name": "pond_list_collections", "arguments": {},
        })));
        let resp = dispatch(&storage, &req).expect("list");
        let collections = resp.result.unwrap()["collections"].as_array().unwrap().clone();
        assert!(collections.contains(&json!("users")));
        assert!(collections.contains(&json!("orders")));
        assert_eq!(collections.len(), 2);
    }

    // ---- branch + merge ----

    #[test]
    fn test_branch_creates_new_branch() {
        let (_dir, storage) = make_storage();
        // Initial write.
        let args = json!({"collection": "users", "rows": [{"id": 1i64}]});
        let req = rpc_request(40, "tools/call", Some(json!({
            "name": "pond_write_rows", "arguments": args,
        })));
        let _ = dispatch(&storage, &req).unwrap().result.unwrap();

        // Create branch.
        let args = json!({"collection": "users", "branch": "experiment"});
        let req = rpc_request(41, "tools/call", Some(json!({
            "name": "pond_branch", "arguments": args,
        })));
        let resp = dispatch(&storage, &req).unwrap();
        assert!(resp.error.is_none(), "branch should succeed: {:?}", resp.error);
        assert!(resp.result.unwrap()["head_commit"].is_string());
    }

    #[test]
    fn test_merge_branches() {
        let (_dir, storage) = make_storage();
        // Initial write.
        let args = json!({"collection": "users", "rows": [{"id": 1i64}]});
        let req = rpc_request(50, "tools/call", Some(json!({
            "name": "pond_write_rows", "arguments": args,
        })));
        let _ = dispatch(&storage, &req).unwrap().result.unwrap();

        // Branch + write to main.
        let args = json!({"collection": "users", "branch": "dev"});
        let req = rpc_request(51, "tools/call", Some(json!({
            "name": "pond_branch", "arguments": args,
        })));
        let _ = dispatch(&storage, &req).unwrap().result.unwrap();

        // Write another commit to main (so the merge has something to merge).
        let args = json!({"collection": "users", "rows": [{"id": 2i64}]});
        let req = rpc_request(52, "tools/call", Some(json!({
            "name": "pond_write_rows", "arguments": args,
        })));
        let _ = dispatch(&storage, &req).unwrap().result.unwrap();

        // Merge dev → main.
        let args = json!({
            "collection": "users",
            "source": "dev",
            "target": "main",
            "message": "merge dev",
        });
        let req = rpc_request(53, "tools/call", Some(json!({
            "name": "pond_merge", "arguments": args,
        })));
        let resp = dispatch(&storage, &req).unwrap();
        assert!(resp.error.is_none(), "merge should succeed: {:?}", resp.error);
        let result = resp.result.unwrap();
        assert!(result["merge_commit"].is_string());
    }

    // ---- vacuum ----

    #[test]
    fn test_vacuum_dry_run() {
        let (_dir, storage) = make_storage();
        // Write some data (creates live blobs).
        let args = json!({"collection": "users", "rows": [{"id": 1i64}]});
        let req = rpc_request(60, "tools/call", Some(json!({
            "name": "pond_write_rows", "arguments": args,
        })));
        let _ = dispatch(&storage, &req).unwrap().result.unwrap();

        // Run vacuum dry-run.
        let args = json!({"preserve_days": 0, "dry_run": true});
        let req = rpc_request(61, "tools/call", Some(json!({
            "name": "pond_vacuum", "arguments": args,
        })));
        let resp = dispatch(&storage, &req).unwrap();
        assert!(resp.error.is_none());
        let result = resp.result.unwrap();
        assert_eq!(result["dry_run"], json!(true));
        assert!(result["deleted"].as_u64().is_some());
    }

    // ---- get_schema ----

    #[test]
    fn test_get_schema_returns_columns() {
        let (_dir, storage) = make_storage();
        // Write rows with id (i64) + name (string) + score (f64).
        let args = json!({
            "collection": "users",
            "rows": [{"id": 1i64, "name": "alice", "score": 1.5f64}],
        });
        let req = rpc_request(70, "tools/call", Some(json!({
            "name": "pond_write_rows", "arguments": args,
        })));
        let _ = dispatch(&storage, &req).unwrap().result.unwrap();

        let args = json!({"collection": "users"});
        let req = rpc_request(71, "tools/call", Some(json!({
            "name": "pond_get_schema", "arguments": args,
        })));
        let resp = dispatch(&storage, &req).unwrap();
        assert!(resp.error.is_none(), "get_schema should succeed: {:?}", resp.error);
        let result = resp.result.unwrap();
        let columns = result["columns"].as_array().unwrap();
        // Original 3 columns + _rowid + _version = 5.
        assert!(columns.len() >= 3, "should have at least 3 columns: {:?}", columns);
        let names: Vec<&str> = columns.iter().map(|c| c["name"].as_str().unwrap()).collect();
        assert!(names.contains(&"id"));
        assert!(names.contains(&"name"));
        assert!(names.contains(&"score"));
    }

    #[test]
    fn test_get_schema_missing_collection() {
        let (_dir, storage) = make_storage();
        let args = json!({"collection": "ghosts"});
        let req = rpc_request(72, "tools/call", Some(json!({
            "name": "pond_get_schema", "arguments": args,
        })));
        let resp = dispatch(&storage, &req).unwrap();
        assert!(resp.error.is_some(), "should error for missing collection");
        let err = resp.error.unwrap();
        assert_eq!(err.code, ERROR_CODE_INTERNAL);
        assert!(err.message.contains("ghosts") || err.message.contains("no commits"));
    }

    // ---- search_vectors ----

    #[test]
    fn test_search_vectors_returns_closest() {
        let (_dir, storage) = make_storage();
        // Write 3 rows with 2D vectors.
        let rows = vec![
            json!({"id": 1i64, "vec": [1.0f64, 0.0f64]}),
            json!({"id": 2i64, "vec": [0.0f64, 1.0f64]}),
            json!({"id": 3i64, "vec": [1.0f64, 1.0f64]}),
        ];
        let args = json!({"collection": "vecs", "rows": rows});
        let req = rpc_request(80, "tools/call", Some(json!({
            "name": "pond_write_rows", "arguments": args,
        })));
        let _ = dispatch(&storage, &req).unwrap().result.unwrap();

        // Search for vectors closest to [1.0, 0.0].
        let args = json!({
            "collection": "vecs",
            "vector_column": "vec",
            "query": [1.0f64, 0.0f64],
            "metric": "l2",
            "k": 2,
        });
        let req = rpc_request(81, "tools/call", Some(json!({
            "name": "pond_search_vectors", "arguments": args,
        })));
        let resp = dispatch(&storage, &req).unwrap();
        assert!(resp.error.is_none(), "search should succeed: {:?}", resp.error);
        let result = resp.result.unwrap();
        let hits = result["hits"].as_array().unwrap();
        assert_eq!(hits.len(), 2);
        // Closest to [1.0, 0.0] is row id=1 (distance 0).
        let first_id = hits[0]["row"]["id"].as_i64().unwrap();
        assert_eq!(first_id, 1);
    }

    // ---- error handling ----

    #[test]
    fn test_unknown_method_returns_error() {
        let (_dir, storage) = make_storage();
        let req = rpc_request(90, "nonexistent/method", None);
        let resp = dispatch(&storage, &req).expect("response");
        assert!(resp.error.is_some());
        assert_eq!(resp.error.unwrap().code, ERROR_CODE_METHOD_NOT_FOUND);
    }

    #[test]
    fn test_unknown_tool_returns_error() {
        let (_dir, storage) = make_storage();
        let req = rpc_request(91, "tools/call", Some(json!({
            "name": "pond_nonexistent_tool",
            "arguments": {},
        })));
        let resp = dispatch(&storage, &req).expect("response");
        assert!(resp.error.is_some());
        assert_eq!(resp.error.unwrap().code, ERROR_CODE_METHOD_NOT_FOUND);
    }

    #[test]
    fn test_missing_jsonrpc_field() {
        let (_dir, storage) = make_storage();
        let req = json!({"id": 92, "method": "initialize"});
        let resp = dispatch(&storage, &req).expect("response");
        assert!(resp.error.is_some());
        assert_eq!(resp.error.unwrap().code, ERROR_CODE_INVALID_REQUEST);
    }

    #[test]
    fn test_missing_method_field() {
        let (_dir, storage) = make_storage();
        let req = json!({"jsonrpc": "2.0", "id": 93});
        let resp = dispatch(&storage, &req).expect("response");
        assert!(resp.error.is_some());
        assert_eq!(resp.error.unwrap().code, ERROR_CODE_INVALID_REQUEST);
    }

    #[test]
    fn test_notification_no_id_no_response() {
        let (_dir, storage) = make_storage();
        // A notification has no `id` field → no response.
        let req = json!({"jsonrpc": "2.0", "method": "initialize"});
        let resp = dispatch(&storage, &req);
        assert!(resp.is_none(), "notifications should not get a response");
    }

    #[test]
    fn test_write_rows_missing_collection() {
        let (_dir, storage) = make_storage();
        let args = json!({"rows": [{"id": 1i64}]});
        let req = rpc_request(94, "tools/call", Some(json!({
            "name": "pond_write_rows", "arguments": args,
        })));
        let resp = dispatch(&storage, &req).expect("response");
        assert!(resp.error.is_some());
        assert_eq!(resp.error.unwrap().code, ERROR_CODE_INTERNAL);
    }

    #[test]
    fn test_write_rows_empty_rows() {
        let (_dir, storage) = make_storage();
        let args = json!({"collection": "users", "rows": []});
        let req = rpc_request(95, "tools/call", Some(json!({
            "name": "pond_write_rows", "arguments": args,
        })));
        let resp = dispatch(&storage, &req).expect("response");
        assert!(resp.error.is_some());
    }

    #[test]
    fn test_response_to_json_format() {
        let resp = RpcResponse::success(json!(42), json!({"ok": true}));
        let j = resp.to_json();
        assert_eq!(j["jsonrpc"], json!("2.0"));
        assert_eq!(j["id"], json!(42));
        assert_eq!(j["result"]["ok"], json!(true));
        assert!(j.get("error").is_none());
    }

    #[test]
    fn test_response_error_to_json_format() {
        let resp = RpcResponse::error(json!(99), ERROR_CODE_METHOD_NOT_FOUND, "no such method");
        let j = resp.to_json();
        assert_eq!(j["jsonrpc"], json!("2.0"));
        assert_eq!(j["id"], json!(99));
        assert!(j.get("result").is_none());
        assert_eq!(j["error"]["code"], json!(ERROR_CODE_METHOD_NOT_FOUND));
        assert_eq!(j["error"]["message"], json!("no such method"));
    }

    #[test]
    fn test_discover_root_fallback_to_dot() {
        // With no env var and no explicit arg, falls back to ".".
        // (We can't reliably test the .pond/ marker walk without changing
        // CWD, so just verify the fallback path.)
        std::env::remove_var("POND_ROOT");
        let root = discover_root(None);
        // Should be either the CWD (if a .pond/ exists somewhere up the
        // tree) or ".". Either way it must be a valid path.
        assert!(root.is_absolute() || root.to_str() == Some("."));
    }

    #[test]
    fn test_discover_root_explicit_arg_wins() {
        std::env::remove_var("POND_ROOT");
        let root = discover_root(Some("/tmp/explicit_path"));
        assert_eq!(root, PathBuf::from("/tmp/explicit_path"));
    }

    #[test]
    fn test_rpc_response_success_serialization() {
        let resp = RpcResponse::success(JsonValue::Null, json!({"hello": "world"}));
        let s = serde_json::to_string(&resp.to_json()).unwrap();
        assert!(s.contains("\"jsonrpc\":\"2.0\""));
        assert!(s.contains("\"result\":{\"hello\":\"world\"}"));
    }

    #[test]
    fn test_tool_list_has_descriptions() {
        let tools = tool_list();
        for tool in &tools {
            let desc = tool["description"].as_str().unwrap();
            assert!(desc.len() > 20, "tool description should be meaningful: {}", desc);
        }
    }

    #[test]
    fn test_pnd2_columns_to_json_rows_basic() {
        use pond_core::{pnd2_encode_multi_typed, TypedColumn};
        let cols = vec![
            ("id", TypedColumn::Int64(vec![1, 2, 3])),
            ("name", TypedColumn::String(vec!["a".into(), "b".into(), "c".into()])),
        ];
        let blob = pnd2_encode_multi_typed(&cols);
        let decoded = pnd2_decode(&blob).unwrap();
        let rows = pnd2_columns_to_json_rows(&decoded);
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0]["id"], json!(1));
        assert_eq!(rows[0]["name"], json!("a"));
        assert_eq!(rows[2]["id"], json!(3));
    }

    #[test]
    fn test_json_rows_to_typed_columns_infers_types() {
        let rows = vec![
            json!({"id": 1i64, "name": "alice", "score": 1.5f64}),
            json!({"id": 2i64, "name": "bob", "score": 2.5f64}),
        ];
        let cols = json_rows_to_typed_columns(&rows).unwrap();
        assert_eq!(cols.len(), 3);
        // id → Int64
        let id_col = cols.iter().find(|(n, _)| n == "id").unwrap();
        assert!(matches!(id_col.1, TypedColumn::Int64(_)));
        // name → String
        let name_col = cols.iter().find(|(n, _)| n == "name").unwrap();
        assert!(matches!(name_col.1, TypedColumn::String(_)));
        // score → Float64
        let score_col = cols.iter().find(|(n, _)| n == "score").unwrap();
        assert!(matches!(score_col.1, TypedColumn::Float64(_)));
    }
}
