// Pond Python Bindings — PyO3 wrapper around bindings/python/core
//
// This crate compiles to a Python extension module named `pond`.
// It exposes the full PND2 decode/encode pipeline to Python.
//
// All decode/encode LOGIC lives in `bindings/python/core`. This file is the thin
// PyO3 glue layer that:
//   1. Accepts Python args (bytes, lists, tuples)
//   2. Calls into pond-core's pure-Rust functions
//   3. Converts the Rust result types into Python objects
//
// This is the correct architecture: the decoder is implemented ONCE in
// pure Rust, and both the C ABI (in bindings/python/core) and Python (here) use it.
#![allow(dead_code, clippy::useless_conversion, clippy::too_many_arguments, clippy::type_complexity, clippy::needless_range_loop, unused_variables, unused_imports, clippy::wildcard_in_or_patterns, unreachable_patterns)]

mod simd;
use pond_sql::{parse_where, WhereExpr, json_values_equal};
use pond_sql::{parse_sql, SqlStatement, MergeAction, TableRef, JoinType};

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyTuple};
use pyo3::Bound;
use rayon::prelude::*;

/// Parse a `where` parameter — must be a SQL string.
///
///   where="age >= 18 AND city = 'NYC'"
///   where="dept = 'eng' AND (salary > 90000 OR age < 30)"
///   where="name LIKE 'A%' AND status IN ('active', 'pending')"
fn parse_where_param(where_val: &PyObject) -> Result<WhereExpr, String> {
    Python::with_gil(|py| {
        if let Ok(s) = where_val.extract::<String>(py) {
            return parse_where(&s);
        }
        Err("where= must be a SQL string, e.g. where=\"age >= 18\"".to_string())
    })
}

// ---------------------------------------------------------------------------
// Merge helpers — multi-action + multi-key + column mapping support
// ---------------------------------------------------------------------------

/// A parsed SET clause — controls which columns to update.
///
/// Three modes:
///   None (no SET clause)     → copy ALL source columns
///   Some(copy_all=false)     → ONLY update listed columns, keep rest from target
///   Some(copy_all=true)      → copy ALL source columns, THEN override listed ones
pub struct SetClause {
    /// If true, copy ALL source columns first, then apply overrides.
    /// If false, only update the explicitly listed columns.
    pub copy_all: bool,
    /// Column overrides: (target_col, value_spec) pairs
    pub columns: Vec<(String, ValueSpec)>,
}

pub enum ValueSpec {
    /// Copy from source column: "s.col_name"
    SourceCol(String),
    /// Keep target's existing value: "t.col_name"
    TargetCol(String),
    /// Set to a static JSON value
    Static(JsonValue),
}

/// A single action in a merge plan (with optional condition + column mapping).
struct MergePlanAction {
    action: MergeActionType,
    condition: Option<WhereExpr>,
    set: Option<SetClause>,  // None = copy all source cols (default)
}

#[derive(Clone, Copy)]
enum MergeActionType {
    Update,
    Delete,
    Insert,
    Skip,
}

/// Counts for merge result reporting.
#[derive(Default)]
struct MergeCounts {
    matched: usize,
    updated: usize,
    deleted: usize,
    inserted: usize,
    skipped: usize,
}

impl Storage {
    fn empty_merge_result(&self) -> PyResult<PyObject> {
        Python::with_gil(|py| {
            let dict = PyDict::new_bound(py);
            dict.set_item("matched", 0)?;
            dict.set_item("updated", 0)?;
            dict.set_item("deleted", 0)?;
            dict.set_item("inserted", 0)?;
            dict.set_item("skipped", 0)?;
            Ok(dict.into())
        })
    }

    fn merge_result(&self, counts: MergeCounts) -> PyResult<PyObject> {
        Python::with_gil(|py| {
            let dict = PyDict::new_bound(py);
            dict.set_item("matched", counts.matched)?;
            dict.set_item("updated", counts.updated)?;
            dict.set_item("deleted", counts.deleted)?;
            dict.set_item("inserted", counts.inserted)?;
            dict.set_item("skipped", counts.skipped)?;
            Ok(dict.into())
        })
    }
}

/// Parse the `on` parameter into a list of (target_col, source_col) pairs.
///
/// Unified format — accepts all of these:
///
///   on='id'                        → [("id", "id")]
///   on=['id', 'email']             → [("id", "id"), ("email", "email")]
///   on=[('user_id', 'id')]         → [("user_id", "id")]
///   on='user_id = id'              → [("user_id", "id")]  (SQL-like)
///   on='user_id = id AND code = c' → [("user_id", "id"), ("code", "c")]
///
/// The SQL-like string format is the most expressive and consistent with
/// the WHERE clause syntax.
fn parse_match_keys(on: Option<PyObject>, key_col: Option<&str>) -> Result<Vec<(String, String)>, String> {
    let on = match on {
        Some(o) => o,
        None => {
            let key = key_col.unwrap_or("_rowid");
            return Ok(vec![(key.to_string(), key.to_string())]);
        }
    };

    Python::with_gil(|py| {
        // Try string first — could be "id" or SQL-like "user_id = id AND ..."
        if let Ok(s) = on.extract::<String>(py) {
            return parse_on_string(&s);
        }

        // Try list — could be list of strings or list of tuples
        if let Ok(list) = on.extract::<Vec<PyObject>>(py) {
            let mut keys = Vec::new();
            for item in list {
                // Try (str, str) tuple first
                if let Ok((t, s)) = item.extract::<(String, String)>(py) {
                    keys.push((t, s));
                } else if let Ok(s) = item.extract::<String>(py) {
                    // Bare string → same name both sides
                    keys.push((s.clone(), s));
                } else {
                    return Err("on= list must contain strings or (target, source) tuples".to_string());
                }
            }
            if keys.is_empty() {
                return Err("on= list is empty".to_string());
            }
            return Ok(keys);
        }

        Err("on= must be a string, list of strings, or list of (target, source) tuples".to_string())
    })
}

/// Parse a SQL-like ON string: "user_id = id AND code = code"
/// Parse a SQL-like ON string into (target_col, source_col) pairs.
///
/// Supports t./s. prefixes for clarity (t = target, s = source):
///
///   on='t.user_id = s.id'                     → [("user_id", "id")]
///   on='t.user_id = s.id AND t.code = s.code' → [("user_id", "id"), ("code", "code")]
///   on='t.id = s.id'                          → [("id", "id")]
///
/// Also supports bare column names (same name both sides):
///   on='id'                                   → [("id", "id")]
///   on='user_id = id AND code = code'         → [("user_id", "id"), ("code", "code")]
///
/// The t./s. prefix format is recommended — it's unambiguous and SQL-consistent.
fn parse_on_string(s: &str) -> Result<Vec<(String, String)>, String> {
    let s = s.trim();

    // If no '=' sign, treat as a bare column name (same name both sides)
    if !s.contains('=') {
        return Ok(vec![(s.to_string(), s.to_string())]);
    }

    // Split by AND and parse each "target = source" pair
    let mut keys = Vec::new();
    for part in s.split_whitespace().collect::<String>().split("AND") {
        let part = part.trim();
        if part.is_empty() { continue; }

        let eq_pos = part.find('=')
            .ok_or_else(|| format!("Expected '=' in ON clause: '{}'", part))?;

        let mut target = part[..eq_pos].trim().to_string();
        let mut source = part[eq_pos + 1..].trim().to_string();

        if target.is_empty() || source.is_empty() {
            return Err(format!("Invalid ON clause: '{}'", part));
        }

        // Strip t./s. prefixes (target/source disambiguation)
        target = target.strip_prefix("t.").unwrap_or(&target).to_string();
        source = source.strip_prefix("s.").unwrap_or(&source).to_string();

        keys.push((target, source));
    }

    if keys.is_empty() {
        return Err("ON clause must have at least one key pair".to_string());
    }
    Ok(keys)
}

/// Parse the on_match / on_miss parameter into a list of actions.
///
/// Unified format — accepts all of these:
///
///   on_match='update'                                    → [Update]
///   on_match=['update', 'delete']                        → [Update, Delete]
///   on_match=[('update', 'age >= 18'), ('delete', 'age < 18')]  → conditional
///   on_match={'update': 'age >= 18', 'delete': 'age < 18'}      → conditional (dict)
///
/// SQL-style string (recommended — most expressive):
///   on_match='UPDATE'                                    → [Update]
///   on_match='UPDATE WHERE age >= 18'                    → [Update if age>=18]
///   on_match='UPDATE WHERE age >= 18; DELETE WHERE age < 18'  → multi-action
///   on_miss='INSERT WHERE age >= 18'                     → [Insert if age>=18]
///
/// The SQL-style string is the recommended format — it's consistent with
/// the MERGE statement syntax and the WHERE clause format.
fn parse_merge_action(param: Option<PyObject>, is_match: bool) -> Result<Vec<MergePlanAction>, String> {
    let param = match param {
        Some(p) => p,
        None => {
            return Ok(vec![MergePlanAction {
                action: if is_match { MergeActionType::Update } else { MergeActionType::Insert },
                condition: None,
                set: None,
            }]);
        }
    };

    Python::with_gil(|py| {
        // Try string first — could be 'update', 'UPDATE WHERE age >= 18', or multi-action
        if let Ok(s) = param.extract::<String>(py) {
            return parse_merge_action_string(&s, is_match);
        }

        // Try list — could be list of strings or list of (action, where) tuples
        if let Ok(list) = param.extract::<Vec<PyObject>>(py) {
            let mut actions = Vec::new();
            for item in list {
                // Try (str, str) tuple → (action, where)
                if let Ok((action_str, where_str)) = item.extract::<(String, String)>(py) {
                    let action = parse_action_str(&action_str, is_match)?;
                    let condition = if where_str.trim().is_empty() {
                        None
                    } else {
                        Some(parse_where(&where_str)?)
                    };
                    actions.push(MergePlanAction { action, condition, set: None });
                } else if let Ok(s) = item.extract::<String>(py) {
                    // Bare string → could be 'update' or 'UPDATE WHERE ...'
                    let sub_actions = parse_merge_action_string(&s, is_match)?;
                    actions.extend(sub_actions);
                } else {
                    return Err("on_match/on_miss list must contain strings or (action, where) tuples".to_string());
                }
            }
            if actions.is_empty() {
                return Err("on_match/on_miss list is empty".to_string());
            }
            return Ok(actions);
        }

        // Try dict: {"update": "age > 18", "delete": "age < 18"}
        if let Ok(dict) = param.extract::<std::collections::HashMap<String, String>>(py) {
            let mut actions = Vec::new();
            for (action_str, where_str) in dict {
                let action = parse_action_str(&action_str, is_match)?;
                let condition = if where_str.trim().is_empty() {
                    None
                } else {
                    Some(parse_where(&where_str)?)
                };
                actions.push(MergePlanAction { action, condition, set: None });
            }
            if actions.is_empty() {
                return Err("on_match/on_miss dict is empty".to_string());
            }
            return Ok(actions);
        }

        Err("on_match/on_miss must be a string, list, or dict".to_string())
    })
}

/// Parse a SQL-style merge action string.
///
/// Formats:
///   'update'                              → [Update]
///   'UPDATE WHERE age >= 18'              → [Update if age>=18]
///   'UPDATE WHERE t.age >= 18 AND s.amount > 100'  → filter on both target+source
///   'UPDATE SET t.name = s.full_name, t.status = "active"'  → column mapping
///   'UPDATE WHERE age >= 18; DELETE WHERE age < 18'  → multi-action
///   'WHEN MATCHED THEN UPDATE WHERE age >= 18'       → SQL MERGE syntax
///
/// SET clause supports:
///   t.name = s.full_name    → copy from source column (different name)
///   t.status = 'active'     → set to static value
///   t.count = t.count       → keep target value (no-op, explicit)
fn parse_merge_action_string(s: &str, is_match: bool) -> Result<Vec<MergePlanAction>, String> {
    let s = s.trim();

    // Support 'WHEN MATCHED THEN ...' or 'WHEN NOT MATCHED THEN ...' prefix
    let s = if s.to_uppercase().starts_with("WHEN MATCHED THEN ") {
        s[18..].trim()
    } else if s.to_uppercase().starts_with("WHEN NOT MATCHED THEN ") {
        s[22..].trim()
    } else {
        s
    };

    // Split by ';' for multi-action
    let mut actions = Vec::new();
    for clause in s.split(';') {
        let clause = clause.trim();
        if clause.is_empty() { continue; }

        let upper = clause.to_uppercase();

        // Find WHERE keyword
        let where_pos = upper.find("WHERE");

        // Find SET keyword
        let set_pos = upper.find("SET");

        // Parse the three parts: ACTION [WHERE ...] [SET ...]
        // The action is always first (UPDATE, DELETE, INSERT, SKIP)
        let (action_end, where_str, set_str) = match (where_pos, set_pos) {
            (Some(wp), Some(sp)) => {
                let end = wp.min(sp);
                let w = if wp > sp { clause[wp + 5..].trim() } else { "" };
                let st = if sp > wp { clause[sp + 3..].trim() } else { "" };
                (end, w, st)
            }
            (Some(wp), None) => (wp, clause[wp + 5..].trim(), ""),
            (None, Some(sp)) => (sp, "", clause[sp + 3..].trim()),
            (None, None) => (clause.len(), "", ""),
        };

        let action_str = clause[..action_end].trim();
        let action = parse_action_str(action_str, is_match)?;

        let condition = if where_str.is_empty() {
            None
        } else {
            Some(parse_where(where_str)?)
        };

        // Parse SET clause
        let set = if set_str.is_empty() {
            None
        } else {
            Some(parse_set_clause_for_merge(set_str)?)
        };

        actions.push(MergePlanAction { action, condition, set });
    }

    if actions.is_empty() {
        return Err(format!("No valid actions found in: '{}'", s));
    }
    Ok(actions)
}

/// Parse a SET clause for merge: "t.name = s.full_name, t.status = 'active'"
/// Parse a SET clause for merge.
///
/// Supports three modes:
///
///   SET t.name = s.full_name, t.status = 'active'
///     → ONLY update listed columns, keep rest from target
///
///   SET *, t.name = s.full_name, t.status = 'active'
///     → Copy ALL source columns, THEN override specific ones
///
///   SET *  (or no SET clause)
///     → Copy ALL source columns
///
/// The `*` token enables "copy all + override" mode. Without `*`,
/// only the explicitly listed columns are updated.
fn parse_set_clause_for_merge(s: &str) -> Result<SetClause, String> {
    let s = s.trim();
    let mut copy_all = false;
    let mut columns: Vec<(String, ValueSpec)> = Vec::new();

    for part in s.split(',') {
        let part = part.trim();
        if part.is_empty() { continue; }

        // Check for * token
        if part == "*" {
            copy_all = true;
            continue;
        }

        let eq_pos = part.find('=')
            .ok_or_else(|| format!("Expected '=' in SET clause: '{}'", part))?;

        let target = part[..eq_pos].trim();
        let value_str = part[eq_pos + 1..].trim();

        // Strip t. prefix from target
        let target = target.strip_prefix("t.").unwrap_or(target).to_string();

        // Parse the value spec
        let spec = if value_str.starts_with("s.") {
            ValueSpec::SourceCol(value_str.strip_prefix("s.").unwrap().to_string())
        } else if value_str.starts_with("t.") {
            ValueSpec::TargetCol(value_str.strip_prefix("t.").unwrap().to_string())
        } else if value_str.starts_with('\'') && value_str.ends_with('\'') {
            ValueSpec::Static(JsonValue::String(value_str[1..value_str.len()-1].to_string()))
        } else if value_str.eq_ignore_ascii_case("true") {
            ValueSpec::Static(JsonValue::Bool(true))
        } else if value_str.eq_ignore_ascii_case("false") {
            ValueSpec::Static(JsonValue::Bool(false))
        } else if value_str.eq_ignore_ascii_case("null") {
            ValueSpec::Static(JsonValue::Null)
        } else if let Ok(i) = value_str.parse::<i64>() {
            ValueSpec::Static(JsonValue::Number(serde_json::Number::from(i)))
        } else if let Ok(f) = value_str.parse::<f64>() {
            ValueSpec::Static(serde_json::Number::from_f64(f)
                .map(JsonValue::Number)
                .unwrap_or(JsonValue::Null))
        } else {
            ValueSpec::Static(JsonValue::String(value_str.to_string()))
        };

        columns.push((target, spec));
    }

    if !copy_all && columns.is_empty() {
        return Err("SET clause is empty".to_string());
    }

    Ok(SetClause { copy_all, columns })
}

/// Build a combined context for evaluating WHERE clauses with t./s. prefixes.
///
/// Returns a JSON object with keys like "t.col" and "s.col" that the
/// WhereExpr evaluator can look up.
fn build_merge_context(target_row: Option<&JsonValue>, source_row: &JsonValue) -> JsonValue {
    let mut ctx = serde_json::Map::new();

    // Add target columns with t. prefix
    if let Some(target) = target_row {
        if let Some(obj) = target.as_object() {
            for (k, v) in obj {
                ctx.insert(format!("t.{}", k), v.clone());
                // Also add without prefix for backward compat
                ctx.insert(k.clone(), v.clone());
            }
        }
    }

    // Add source columns with s. prefix
    if let Some(obj) = source_row.as_object() {
        for (k, v) in obj {
            ctx.insert(format!("s.{}", k), v.clone());
            // Only add without prefix if not already present from target
            ctx.entry(k.clone()).or_insert_with(|| v.clone());
        }
    }

    JsonValue::Object(ctx)
}

/// Apply a SET clause to produce the final row for upsert.
///
/// Three modes:
///   None                          → copy ALL source columns
///   Some(copy_all=false)          → ONLY update listed columns, keep rest from target
///   Some(copy_all=true)           → copy ALL source columns, THEN override listed ones
fn apply_set_to_row(
    target_row: Option<&JsonValue>,
    source_row: &JsonValue,
    set: &Option<SetClause>,
    existing_rowid: Option<&str>,
) -> JsonValue {
    match set {
        None => {
            // Default: copy all source columns + set _rowid
            let mut result = source_row.clone();
            if let Some(obj) = result.as_object_mut() {
                if let Some(rid) = existing_rowid {
                    obj.insert("_rowid".to_string(), json!(rid));
                }
            }
            result
        }
        Some(clause) => {
            let mut result = serde_json::Map::new();

            if clause.copy_all {
                // Mode: SET *, t.col = ... → copy ALL source columns first
                if let Some(src_obj) = source_row.as_object() {
                    for (k, v) in src_obj {
                        // Skip CRDT metadata — it'll be set explicitly
                        if k != "_rowid" && k != "_version" && k != "_deleted" {
                            result.insert(k.clone(), v.clone());
                        }
                    }
                }
            } else {
                // Mode: SET t.col = ... → start with target row as base
                if let Some(target) = target_row {
                    if let Some(obj) = target.as_object() {
                        for (k, v) in obj {
                            result.insert(k.clone(), v.clone());
                        }
                    }
                }
            }

            // Apply column overrides
            for (target_col, spec) in &clause.columns {
                let val = match spec {
                    ValueSpec::SourceCol(src_col) => {
                        source_row.get(src_col).cloned().unwrap_or(JsonValue::Null)
                    }
                    ValueSpec::TargetCol(t_col) => {
                        target_row.and_then(|t| t.get(t_col)).cloned().unwrap_or(JsonValue::Null)
                    }
                    ValueSpec::Static(v) => v.clone(),
                };
                result.insert(target_col.clone(), val);
            }

            // Ensure _rowid is set
            if let Some(rid) = existing_rowid {
                result.insert("_rowid".to_string(), json!(rid));
            }

            JsonValue::Object(result)
        }
    }
}

fn parse_action_str(s: &str, is_match: bool) -> Result<MergeActionType, String> {
    match s.to_lowercase().as_str() {
        "update" => Ok(MergeActionType::Update),
        "delete" => Ok(MergeActionType::Delete),
        "skip" => Ok(MergeActionType::Skip),
        "insert" if !is_match => Ok(MergeActionType::Insert),
        "insert" if is_match => Err("'insert' is not valid for on_match".to_string()),
        other => Err(format!("Unknown merge action: '{}' (use update/delete/skip/insert)", other)),
    }
}

/// Convert a WhereExpr back to a SQL string (for passing to update_rows/delete_rows).
fn where_expr_to_sql(expr: &WhereExpr) -> String {
    match expr {
        WhereExpr::True => "".to_string(),
        WhereExpr::And(a, b) => format!("({} AND {})", where_expr_to_sql(a), where_expr_to_sql(b)),
        WhereExpr::Or(a, b) => format!("({} OR {})", where_expr_to_sql(a), where_expr_to_sql(b)),
        WhereExpr::Not(e) => format!("NOT ({})", where_expr_to_sql(e)),
        WhereExpr::Compare { col, op, value } => {
            format!("{} {} {}", col, op, json_to_sql_literal(value))
        }
        WhereExpr::In { col, values, negate } => {
            let vals: Vec<String> = values.iter().map(json_to_sql_literal).collect();
            let op = if *negate { "NOT IN" } else { "IN" };
            format!("{} {} ({})", col, op, vals.join(", "))
        }
        WhereExpr::Like { col, pattern, negate } => {
            let op = if *negate { "NOT LIKE" } else { "LIKE" };
            format!("{} {} '{}'", col, op, pattern)
        }
        WhereExpr::IsNull { col, negate } => {
            if *negate { format!("{} IS NOT NULL", col) } else { format!("{} IS NULL", col) }
        }
        WhereExpr::Subquery { col, query, negate } => {
            // Re-emit the subquery as `col IN (SELECT ...)` — pond_sql will
            // re-parse and evaluate it against storage at execution time.
            let op = if *negate { "NOT IN" } else { "IN" };
            format!("{} {} ({})", col, op, query)
        }
    }
}

fn json_to_sql_literal(v: &JsonValue) -> String {
    match v {
        JsonValue::String(s) => format!("'{}'", s),
        JsonValue::Number(n) => n.to_string(),
        JsonValue::Bool(b) => b.to_string(),
        JsonValue::Null => "NULL".to_string(),
        _ => v.to_string(),
    }
}

/// Convert a `pond_sql::SqlResult` (row-oriented) to a Python dict of
/// column_name → list of values (the columnar format Python callers expect).
///
/// Used by the `Storage.sql()` method when delegating SELECT execution to
/// the pure-Rust `pond_sql` crate.
fn sql_result_to_pydict(py: Python, result: pond_sql::SqlResult) -> PyObject {
    use std::collections::HashMap;
    let mut cols: HashMap<String, Vec<PyObject>> = HashMap::new();
    let mut col_order: Vec<String> = Vec::new();
    for name in &result.columns {
        col_order.push(name.clone());
        cols.insert(name.clone(), Vec::new());
    }
    for row in &result.rows {
        if let Some(obj) = row.as_object() {
            for (name, value) in obj {
                if !cols.contains_key(name) {
                    col_order.push(name.clone());
                    cols.insert(name.clone(), Vec::new());
                }
                cols.get_mut(name).unwrap().push(json_value_to_py(py, value));
            }
        }
    }
    let dict = PyDict::new_bound(py);
    for name in col_order {
        if let Some(values) = cols.remove(&name) {
            let list = PyList::new_bound(py, values.iter());
            let _ = dict.set_item(&name, list);
        }
    }
    dict.into()
}

/// Convert a JsonValue to a PyObject.
fn json_to_pyobject(py: Python, v: &JsonValue) -> PyObject {
    match v {
        JsonValue::Null => py.None(),
        JsonValue::Bool(b) => b.to_object(py),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                i.to_object(py)
            } else if let Some(f) = n.as_f64() {
                f.to_object(py)
            } else {
                py.None()
            }
        }
        JsonValue::String(s) => s.to_object(py),
        JsonValue::Array(arr) => {
            let list = PyList::new_bound(py, arr.iter().map(|v| json_to_pyobject(py, v)));
            list.into()
        }
        JsonValue::Object(obj) => {
            let dict = PyDict::new_bound(py);
            for (k, v) in obj {
                dict.set_item(k, json_to_pyobject(py, v)).unwrap();
            }
            dict.into()
        }
    }
}

/// Read rows from a table reference — either a Pond collection or an external file.
///
/// For collections: reads HEAD + shards, applies CRDT merge.
/// For files: reads CSV, JSON, or NDJSON files and converts to JSON rows.
fn read_table_rows(
    storage: &pond_storage::UnifiedStorage,
    table: &TableRef,
) -> Result<Vec<JsonValue>, String> {
    match table {
        TableRef::Collection(name) => {
            let kc = vec!["_rowid".to_string()];
            let all_rows = read_collection_as_json_rows(storage, name, &kc)?;
            Ok(crdt_merge_rows(all_rows))
        }
        TableRef::File(path) => {
            read_file_rows(path)
        }
    }
}

/// Read rows from a file (CSV, JSON, NDJSON).
fn read_file_rows(path: &str) -> Result<Vec<JsonValue>, String> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| format!("Failed to read file '{}': {}", path, e))?;

    if path.ends_with(".json") || path.ends_with(".ndjson") {
        // NDJSON: one JSON object per line
        if path.ends_with(".ndjson") {
            let mut rows = Vec::new();
            for line in content.lines() {
                if line.trim().is_empty() { continue; }
                let row: JsonValue = serde_json::from_str(line)
                    .map_err(|e| format!("Failed to parse NDJSON line: {}", e))?;
                rows.push(row);
            }
            Ok(rows)
        } else {
            // JSON: could be a single array or a single object
            let parsed: JsonValue = serde_json::from_str(&content)
                .map_err(|e| format!("Failed to parse JSON: {}", e))?;
            match parsed {
                JsonValue::Array(arr) => Ok(arr),
                JsonValue::Object(obj) => Ok(vec![JsonValue::Object(obj)]),
                _ => Err("JSON file must contain an array or object".to_string()),
            }
        }
    } else if path.ends_with(".csv") || path.ends_with(".tsv") {
        // CSV/TSV parsing
        let delimiter = if path.ends_with(".tsv") { '\t' } else { ',' };
        let mut rows = Vec::new();
        let mut lines = content.lines();
        let header_line = lines.next()
            .ok_or_else(|| "CSV file is empty".to_string())?;
        let headers: Vec<String> = header_line.split(delimiter)
            .map(|s| s.trim().trim_matches('"').to_string())
            .collect();

        for line in lines {
            if line.trim().is_empty() { continue; }
            let values: Vec<&str> = line.split(delimiter).collect();
            let mut obj = serde_json::Map::new();
            for (i, header) in headers.iter().enumerate() {
                let val_str = values.get(i).unwrap_or(&"").trim().trim_matches('"');
                // Try to parse as number, then bool, then string
                let val = if let Ok(n) = val_str.parse::<i64>() {
                    JsonValue::Number(serde_json::Number::from(n))
                } else if let Ok(f) = val_str.parse::<f64>() {
                    serde_json::Number::from_f64(f)
                        .map(JsonValue::Number)
                        .unwrap_or(JsonValue::String(val_str.to_string()))
                } else if val_str.eq_ignore_ascii_case("true") {
                    JsonValue::Bool(true)
                } else if val_str.eq_ignore_ascii_case("false") {
                    JsonValue::Bool(false)
                } else if val_str.is_empty() {
                    JsonValue::Null
                } else {
                    JsonValue::String(val_str.to_string())
                };
                obj.insert(header.clone(), val);
            }
            rows.push(JsonValue::Object(obj));
        }
        Ok(rows)
    } else if path.ends_with(".parquet") {
        Err("Parquet file reading not yet supported. Use CSV or JSON.".to_string())
    } else {
        Err(format!("Unsupported file format: '{}'", path))
    }
}

/// Execute a JOIN between two sets of rows.
///
/// ON conditions are pairs of qualified column names like ("u.id", "o.user_id").
/// The function finds matching rows and merges them into combined row objects.
fn execute_join(
    left_rows: Vec<JsonValue>,
    right_rows: Vec<JsonValue>,
    on: &[(String, String)],
    join_type: &JoinType,
) -> Vec<JsonValue> {
    // Build an index on right_rows: composite key → list of right rows
    let mut right_index: std::collections::HashMap<String, Vec<&JsonValue>> = std::collections::HashMap::new();
    for right_row in &right_rows {
        let mut composite_key = String::new();
        for (_, right_col) in on {
            let val = right_row.get(right_col).map(|v| v.to_string()).unwrap_or_default();
            if !composite_key.is_empty() { composite_key.push('\x1f'); }
            composite_key.push_str(&val);
        }
        right_index.entry(composite_key).or_default().push(right_row);
    }

    let mut result: Vec<JsonValue> = Vec::new();

    for left_row in &left_rows {
        // Build composite key from left row
        let mut composite_key = String::new();
        for (left_col, _) in on {
            let val = left_row.get(left_col).map(|v| v.to_string()).unwrap_or_default();
            if !composite_key.is_empty() { composite_key.push('\x1f'); }
            composite_key.push_str(&val);
        }

        match right_index.get(&composite_key) {
            Some(matches) => {
                // Inner join — merge left + each matching right
                for right_row in matches {
                    let mut merged = left_row.clone();
                    if let (Some(merged_obj), Some(right_obj)) = (merged.as_object_mut(), right_row.as_object()) {
                        for (k, v) in right_obj {
                            merged_obj.insert(k.clone(), v.clone());
                        }
                    }
                    result.push(merged);
                }
            }
            None => {
                // Left join — include left row with nulls for right columns
                if *join_type == JoinType::Left {
                    let mut merged = left_row.clone();
                    // Add null values for right columns
                    if let Some(right_row) = right_rows.first() {
                        if let Some(right_obj) = right_row.as_object() {
                            if let Some(merged_obj) = merged.as_object_mut() {
                                for k in right_obj.keys() {
                                    if !merged_obj.contains_key(k) {
                                        merged_obj.insert(k.clone(), JsonValue::Null);
                                    }
                                }
                            }
                        }
                    }
                    result.push(merged);
                }
            }
        }
    }

    result
}

// Re-use the shared constants and parser from pond-core.
use pond_core::{
    PND2_MAGIC, PND2_VERSION, FLAG_HAS_STATS,
    COMPRESSION_NONE,
    VT_INT64, VT_FLOAT64, VT_STRING, VT_BINARY,
    ENC_RAW, ENC_RLE, ENC_DICT, ENC_BITPACK, PondColumn, TypedColumn,
};

// ---------------------------------------------------------------------------
// Python-facing decode function
// ---------------------------------------------------------------------------

/// Decode a PND2 blob into a Python dict of column_name -> list of values.
///
/// Handles all value types (INT64, FLOAT64, STRING, BINARY, NULL) and all
/// encodings (RAW, RLE, DICT, BITPACK). Optionally projects columns and
/// applies row-level predicate pushdown.
#[pyfunction]
#[pyo3(signature = (blob_bytes, columns=None, predicates=None))]
#[allow(clippy::too_many_arguments)]
fn decode(
    py: Python,
    blob_bytes: &[u8],
    columns: Option<Vec<String>>,
    predicates: Option<Vec<(String, String, PyObject)>>,
) -> PyResult<PyObject> {
    // Use pond_core's decoder directly — it handles zstd decompression
    // (when the "zstd" feature is enabled) and all encodings/vtypes.
    let pond_columns = match pond_core::pnd2_decode(blob_bytes) {
        Ok(cols) => cols,
        Err(_) => return Ok(py.None()),
    };

    let n_columns = pond_columns.len();
    let n_rows = pond_columns.first().map(|c| c.n_values).unwrap_or(0);

    // Apply column projection if requested
    let projection: Option<std::collections::HashSet<String>> = columns.map(|cols| {
        cols.into_iter().collect()
    });

    // Apply predicate pushdown: determine which rows pass ALL predicates
    let mask: Vec<bool> = if let Some(ref preds) = predicates {
        compute_predicate_mask(py, &pond_columns, preds)?
    } else {
        vec![true; n_rows]
    };

    // Build the result dict: column_name -> list of values (filtered by mask)
    let result = PyDict::new_bound(py);
    for col in &pond_columns {
        let name = col.name.to_string_lossy().to_string();
        // Skip if projection requested and this column is not in it
        if let Some(ref proj) = projection {
            if !proj.contains(&name) {
                continue;
            }
        }
        let py_values = column_to_pylist_filtered(py, col, &mask)?;
        result.set_item(&name, py_values)?;
    }

    // Add metadata
    let filtered_rows = mask.iter().filter(|&&m| m).count();
    result.set_item("_n_rows", filtered_rows.to_object(py))?;
    result.set_item("_n_columns", n_columns.to_object(py))?;

    Ok(result.into())
}

/// Compute a row mask (which rows pass ALL predicates).
fn compute_predicate_mask(
    py: Python,
    columns: &[pond_core::PondColumn],
    predicates: &[(String, String, PyObject)],
) -> PyResult<Vec<bool>> {
    use pond_core::{VT_INT64, VT_FLOAT64};
    let n_rows = columns.first().map(|c| c.n_values).unwrap_or(0);
    let mut mask = vec![true; n_rows];

    for (col_name, op, value) in predicates {
        // Find the column
        let col = match columns.iter().find(|c| c.name.to_string_lossy() == col_name.as_str()) {
            Some(c) => c,
            None => continue, // Column not found — skip this predicate
        };

        for (i, m) in mask.iter_mut().enumerate() {
            if !*m { continue; }
            let passes = match col.vtype {
                VT_INT64 => {
                    let cell_val = col.i64_data.get(i).copied().unwrap_or(0);
                    let target: i64 = value.extract(py).unwrap_or(0);
                    apply_op_i64(cell_val, op, target)
                }
                VT_FLOAT64 => {
                    let cell_val = col.f64_data.get(i).copied().unwrap_or(0.0);
                    let target: f64 = value.extract(py).unwrap_or(0.0);
                    apply_op_f64(cell_val, op, target)
                }
                _ => true, // Unsupported vtype — don't filter
            };
            *m = passes;
        }
    }
    Ok(mask)
}

fn apply_op_i64(cell: i64, op: &str, target: i64) -> bool {
    match op {
        "=" | "==" => cell == target,
        "!=" | "<>" => cell != target,
        "<" => cell < target,
        "<=" => cell <= target,
        ">" => cell > target,
        ">=" => cell >= target,
        _ => true,
    }
}

fn apply_op_f64(cell: f64, op: &str, target: f64) -> bool {
    match op {
        "=" | "==" => cell == target,
        "!=" | "<>" => cell != target,
        "<" => cell < target,
        "<=" => cell <= target,
        ">" => cell > target,
        ">=" => cell >= target,
        _ => true,
    }
}

/// Convert a PondColumn to a Python list, filtered by the mask.
fn column_to_pylist_filtered(py: Python, col: &pond_core::PondColumn, mask: &[bool]) -> PyResult<PyObject> {
    use pond_core::{VT_INT64, VT_FLOAT64, VT_STRING, VT_BINARY, VT_NULL};
    let list = PyList::empty_bound(py);
    match col.vtype {
        VT_INT64 => {
            for (i, v) in col.i64_data.iter().enumerate() {
                if mask.get(i).copied().unwrap_or(false) {
                    list.append(*v)?;
                }
            }
        }
        VT_FLOAT64 => {
            for (i, v) in col.f64_data.iter().enumerate() {
                if mask.get(i).copied().unwrap_or(false) {
                    list.append(*v)?;
                }
            }
        }
        VT_STRING => {
            for (i, s) in col.str_data.iter().enumerate() {
                if mask.get(i).copied().unwrap_or(false) {
                    list.append(s.to_string_lossy().to_string())?;
                }
            }
        }
        VT_BINARY => {
            for (i, b) in col.bin_data.iter().enumerate() {
                if mask.get(i).copied().unwrap_or(false) {
                    list.append(PyBytes::new_bound(py, b))?;
                }
            }
        }
        VT_NULL | _ => {
            for i in 0..col.n_values {
                if mask.get(i).copied().unwrap_or(false) {
                    list.append(py.None())?;
                }
            }
        }
    }
    Ok(list.into())
}

/// Convert a `pond_core::PondColumn` into a Python list of values.
///
/// Handles all value types: INT64, FLOAT64, STRING, BINARY.
/// NULL values (which bindings/python/core represents as empty strings/vecs for
/// bitmap-encoded rows) become Python None.
fn column_to_pylist(py: Python, col: &PondColumn) -> PyResult<PyObject> {
    let list = PyList::empty_bound(py);
    match col.vtype {
        VT_INT64 => {
            for v in &col.i64_data { list.append(*v)?; }
        }
        VT_FLOAT64 => {
            for v in &col.f64_data { list.append(*v)?; }
        }
        VT_STRING => {
            // CString → &str via to_str (safe — we know the bytes are valid UTF-8
            // because bindings/python/core built them via bytes_to_cstring which preserves
            // the input bytes; if the input had invalid UTF-8, the original
            // decode path used String::from_utf8_lossy so the bytes are already
            // valid UTF-8 replacement sequences).
            for v in &col.str_data {
                let s = v.to_str().unwrap_or("").to_string();
                list.append(s)?;
            }
        }
        VT_BINARY => {
            for v in &col.bin_data {
                list.append(PyBytes::new_bound(py, v))?;
            }
        }
        _ => {
            // Unknown vtype — emit None for each row.
            for _ in 0..col.n_values { list.append(py.None())?; }
        }
    }
    Ok(list.into())
}

/// Apply row-level predicates to the decoded result, returning only matching rows.
fn apply_predicates(
    py: Python,
    result: &Bound<'_, PyDict>,
    preds: &[(String, String, PyObject)],
) -> PyResult<PyObject> {
    if preds.is_empty() {
        return Ok(result.clone().into());
    }

    // Find the number of rows from the first list-valued column
    let mut n_rows: Option<usize> = None;
    for (k, v) in result.iter() {
        let _ = k; // unused key
        if let Ok(list) = v.downcast::<PyList>() {
            n_rows = Some(list.len());
            break;
        }
    }
    let n_rows = match n_rows {
        Some(n) => n,
        None => return Ok(result.clone().into()),
    };

    // For each row, evaluate all predicates. Keep the row only if ALL match.
    let mut keep_mask: Vec<bool> = vec![true; n_rows];
    for (col_name, op, target) in preds {
        let col_val = match result.get_item(col_name)? {
            Some(v) => v,
            None => continue,
        };
        let col_list: &Bound<'_, PyList> = match col_val.downcast() {
            Ok(l) => l,
            Err(_) => continue,
        };
        for i in 0..n_rows {
            if !keep_mask[i] { continue; }
            let row_val = col_list.get_item(i)?;
            let matches = match op.as_str() {
                "=" | "==" => row_val.compare(target)?.is_eq(),
                "!=" => !row_val.compare(target)?.is_eq(),
                "<" => row_val.compare(target)?.is_lt(),
                "<=" => row_val.compare(target)?.is_le(),
                ">" => row_val.compare(target)?.is_gt(),
                ">=" => row_val.compare(target)?.is_ge(),
                _ => true, // unknown op: don't filter
            };
            if !matches { keep_mask[i] = false; }
        }
    }

    // Build filtered result
    let filtered = PyDict::new_bound(py);
    for (k, v) in result.iter() {
        if let Ok(list) = v.downcast::<PyList>() {
            let new_list = PyList::empty_bound(py);
            for i in 0..n_rows {
                if keep_mask[i] {
                    new_list.append(list.get_item(i)?)?;
                }
            }
            filtered.set_item(k, new_list)?;
        } else {
            filtered.set_item(k, v)?;
        }
    }
    Ok(filtered.into())
}

// ---------------------------------------------------------------------------
// zstd decompression (uses Python's `zstandard` library — no Rust dep)
// ---------------------------------------------------------------------------

fn zstd_decompress(data: &[u8]) -> Result<Vec<u8>, String> {
    Python::with_gil(|py| {
        let zstd_mod = py.import_bound("zstandard")?;
        let decompress = zstd_mod.getattr("decompress")?;
        let py_bytes = PyBytes::new_bound(py, data);
        let result = decompress.call1((py_bytes,))?;
        let result_bytes: &[u8] = result.extract::<&[u8]>()?;
        Ok(result_bytes.to_vec())
    }).map_err(|e: pyo3::PyErr| e.to_string())
}

// ---------------------------------------------------------------------------
// Python-facing encode function
// ---------------------------------------------------------------------------

/// Encode a list of column values into a PND2 blob (RAW encoding only).
///
/// Returns a dict with:
///   - "blob": bytes — the PND2 blob
///   - "stats": list of (name, vtype, min, max, null_count) tuples
///
/// Returns None for columns that need DICT/RLE/BITPACK (Python handles those
/// via pond_sdk.extensions.physical_structures.encoding).
#[pyfunction]
#[pyo3(signature = (columns, n_rows))]
fn encode(py: Python, columns: Vec<(String, PyObject)>, n_rows: usize) -> PyResult<PyObject> {
    if columns.is_empty() || n_rows == 0 {
        return Ok(py.None());
    }

    let mut inner = Vec::new();
    let mut col_payloads: Vec<Vec<u8>> = Vec::new();
    let mut stats_list: Vec<(String, u8, PyObject, PyObject, u32)> = Vec::new();

    // Schema section
    for (name, values_obj) in &columns {
        let name_bytes = name.as_bytes();
        if name_bytes.len() > 255 {
            return Ok(py.None());
        }

        // Try INT64
        if let Ok(vals) = values_obj.extract::<Vec<i64>>(py) {
            if vals.len() != n_rows { return Ok(py.None()); }
            let mut payload = Vec::with_capacity(1 + n_rows * 8);
            payload.push(VT_INT64);
            for v in &vals { payload.extend_from_slice(&v.to_le_bytes()); }
            let min_val = vals.iter().min().copied().unwrap_or(0);
            let max_val = vals.iter().max().copied().unwrap_or(0);
            inner.extend_from_slice(&[name_bytes.len() as u8]);
            inner.extend_from_slice(name_bytes);
            inner.extend_from_slice(&[VT_INT64, ENC_RAW]);
            col_payloads.push(payload);
            stats_list.push((name.clone(), VT_INT64,
                min_val.to_object(py),
                max_val.to_object(py),
                0u32));
            continue;
        }

        // Try FLOAT64
        if let Ok(vals) = values_obj.extract::<Vec<f64>>(py) {
            if vals.len() != n_rows { return Ok(py.None()); }
            let mut payload = Vec::with_capacity(1 + n_rows * 8);
            payload.push(VT_FLOAT64);
            for v in &vals { payload.extend_from_slice(&v.to_le_bytes()); }
            let min_val = vals.iter().cloned().fold(f64::INFINITY, f64::min);
            let max_val = vals.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
            inner.extend_from_slice(&[name_bytes.len() as u8]);
            inner.extend_from_slice(name_bytes);
            inner.extend_from_slice(&[VT_FLOAT64, ENC_RAW]);
            col_payloads.push(payload);
            stats_list.push((name.clone(), VT_FLOAT64,
                min_val.to_object(py),
                max_val.to_object(py),
                0u32));
            continue;
        }

        // Try STRING
        if let Ok(vals) = values_obj.extract::<Vec<String>>(py) {
            if vals.len() != n_rows { return Ok(py.None()); }
            let mut payload = Vec::new();
            payload.push(VT_STRING);
            for v in &vals {
                let vb = v.as_bytes();
                payload.extend_from_slice(&(vb.len() as u32).to_le_bytes());
                payload.extend_from_slice(vb);
            }
            inner.extend_from_slice(&[name_bytes.len() as u8]);
            inner.extend_from_slice(name_bytes);
            inner.extend_from_slice(&[VT_STRING, ENC_RAW]);
            col_payloads.push(payload);
            stats_list.push((name.clone(), VT_STRING,
                py.None(), py.None(), 0u32));
            continue;
        }

        // Can't handle — let Python do it
        return Ok(py.None());
    }

    // Stats section
    for (_, _, min_obj, max_obj, null_count) in &stats_list {
        if min_obj.is_none(py) {
            inner.push(0);
        } else {
            inner.push(1);
            if let Ok(v) = min_obj.extract::<i64>(py) {
                inner.extend_from_slice(&v.to_le_bytes());
            } else if let Ok(v) = min_obj.extract::<f64>(py) {
                inner.extend_from_slice(&v.to_le_bytes());
            } else {
                inner.extend_from_slice(&[0u8; 8]);
            }
            if let Ok(v) = max_obj.extract::<i64>(py) {
                inner.extend_from_slice(&v.to_le_bytes());
            } else if let Ok(v) = max_obj.extract::<f64>(py) {
                inner.extend_from_slice(&v.to_le_bytes());
            } else {
                inner.extend_from_slice(&[0u8; 8]);
            }
        }
        inner.extend_from_slice(&null_count.to_le_bytes());
    }

    // Per-column payloads
    for payload in &col_payloads {
        inner.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        inner.extend_from_slice(payload);
    }

    // Build final PND2 blob (uncompressed)
    let mut blob = Vec::new();
    blob.extend_from_slice(PND2_MAGIC);
    blob.push(PND2_VERSION);
    blob.push(FLAG_HAS_STATS);
    blob.extend_from_slice(&(n_rows as u32).to_le_bytes());
    blob.extend_from_slice(&(col_payloads.len() as u16).to_le_bytes());
    blob.push(COMPRESSION_NONE);
    blob.extend_from_slice(&inner);

    // Return dict: {"blob": bytes, "stats": [(name, vtype, min, max, nc), ...]}
    let result = PyDict::new_bound(py);
    result.set_item("blob", PyBytes::new_bound(py, &blob))?;
    let stats_py = PyList::new_bound(py, stats_list.iter().map(|(name, vtype, min, max, nc)| {
        let t = PyTuple::new_bound(py, [
            name.to_object(py),
            vtype.to_object(py),
            min.clone_ref(py),
            max.clone_ref(py),
            nc.to_object(py),
        ]);
        t.into_any()
    }));
    result.set_item("stats", stats_py)?;
    Ok(result.into())
}

// Suppress unused-import warning for the encoding constants — they're
// kept in scope to make it easy to add future encode paths (RLE/DICT/
// BITPACK) without re-importing.
#[allow(unused_imports)]
use {ENC_RAW as _, ENC_RLE as _, ENC_DICT as _, ENC_BITPACK as _};

// ===========================================================================
// Storage — PyO3 wrapper around UnifiedStorage (Rust core)
// ===========================================================================
//
// This lets Python call the Rust storage layer directly, without going
// through the Python reference kernel. This is the migration path: Python
// code can use `pond.Storage` instead of `PondStorage(PondMinimal(...))`.
//
// Supported operations:
//   - Storage(path)          — open local FS storage
//   - Storage.from_s3(url)   — open S3-compatible storage
//   - write(collection, data, message) → commit_hash (str)
//   - read(collection) → bytes
//   - branch(collection, branch_name) → commit_hash
//   - checkout(collection, branch_name)
//   - checkout_new(collection, branch_name)  — -b equivalent
//   - merge(collection, source, target, message) → commit_hash
//   - history(collection, limit) → list of (hash, message, index)
//   - branches(collection) → list of (name, commit_hash)
//   - ls() → list of collection names
//   - undo(collection, steps) → commit_hash
//   - revert(collection, commit_hash)

use pond_kernel::PondKernel;
use pond_storage::UnifiedStorage;
use pond_storage::{write as storage_write, read as storage_read, branch as storage_branch,
                    commit as storage_commit};
use pond_ivf_index::IVFIndex as RustIVFIndex;
use pond_hnsw_index::HNSWIndex as RustHNSWIndex;
use pond_simple_index::SimpleIndex as RustSimpleIndex;
use pond_semantic::SemanticDefinitions;
use serde_json::{json, Value as JsonValue};
use std::sync::Mutex;

/// Build a full S3 URL from a base URL and optional parameters.
///
/// If the base URL already has query params (e.g., `?region=us-east-1`),
/// append the new params. Otherwise, add them.
///
/// This lets users pass either:
///   - A full URL: `s3://bucket/prefix?region=us-east-1&endpoint=https://...`
///   - A base URL + kwargs: `Storage('s3://bucket/prefix', region='us-east-1', endpoint='...')`
fn build_s3_url(
    base: &str,
    _access_key: Option<&str>,
    _secret_key: Option<&str>,
    region: Option<&str>,
    endpoint: Option<&str>,
) -> String {
    let mut url = base.to_string();
    let mut params: Vec<String> = Vec::new();

    // Check if URL already has query params
    let has_query = url.contains('?');

    // Add region if provided and not already in URL
    if let Some(r) = region {
        if !url.contains("region=") {
            params.push(format!("region={}", r));
        }
    }
    // Add endpoint if provided and not already in URL
    if let Some(e) = endpoint {
        if !url.contains("endpoint=") {
            params.push(format!("endpoint={}", e));
        }
    }

    if !params.is_empty() {
        if has_query {
            url.push('&');
        } else {
            url.push('?');
        }
        url.push_str(&params.join("&"));
    }

    url
}

/// Build the kernel for an S3 URL, wrapped in the 3-tier smart cache
/// (memory → local disk → S3) when the `cache` feature is enabled.
///
/// Cache directory resolution (see `pond_cache::resolve_cache_dir`):
///   1. explicit `cache_dir` kwarg (Python-side control),
///   2. `POND_CACHE_DIR` environment variable,
///   3. default `$HOME/.pond_cache` (or temp dir).
///
/// Disable with `cache_dir='off'` / `'none'` / `''` or `POND_CACHE_DIR=off`.
///
/// This is what makes warm reads single-digit-ms: refs and hot blobs are
/// served from memory/disk instead of paying 50-300ms S3 RTTs.
/// If the cache directory cannot be created, degrade gracefully to the raw
/// store (`S3ObjectStore::from_url` is a pure URL parser — rebuilding the
/// store after the cache constructor consumed the first one is free).
#[cfg(all(feature = "s3", feature = "cache"))]
fn s3_kernel_cached(url: &str, cache_dir: Option<&str>) -> PyResult<PondKernel> {
    let py_err = |e: std::io::Error| pyo3::exceptions::PyIOError::new_err(e.to_string());
    if let Some(dir) = pond_cache::resolve_cache_dir(cache_dir) {
        let store = pond_s3::S3ObjectStore::from_url(url).map_err(py_err)?;
        return match pond_cache::CachingObjectStore::new(Box::new(store), &dir) {
            Ok(cached) => Ok(PondKernel::new_with_store(Box::new(cached))),
            Err(e) => {
                eprintln!("pond: local cache disabled ({e}); using direct storage");
                let raw = pond_s3::S3ObjectStore::from_url(url).map_err(py_err)?;
                Ok(PondKernel::new_with_store(Box::new(raw)))
            }
        };
    }
    let raw = pond_s3::S3ObjectStore::from_url(url).map_err(py_err)?;
    Ok(PondKernel::new_with_store(Box::new(raw)))
}

/// A Pond storage handle backed by the Rust UnifiedStorage.
///
/// This is the Python-facing wrapper around `pond_storage::UnifiedStorage`.
/// It provides the same operations as the Python `PondStorage` class, but
/// all logic runs in Rust (no Python reference kernel needed).
///
/// # Example (Python)
/// ```python
/// from pond import Storage
///
/// # Local FS
/// s = Storage("/var/lib/pond")
///
/// # S3
/// # s = Storage.from_s3("s3://bucket/prefix?region=us-east-1&endpoint=...")
///
/// s.write("users", b'[{"id":1,"name":"alice"}]', "init")
/// data = s.read("users")
/// s.branch("users", "dev")
/// s.checkout_new("users", "dev")
/// s.write("users", b'[{"id":2,"name":"bob"}]', "add bob")
/// s.checkout("users", "main")
/// s.merge("users", "dev", "main", "merge dev")
/// ```
#[pyclass]
struct Storage {
    storage: Arc<Mutex<UnifiedStorage>>,
    /// User-defined functions for SQL WHERE pushdown.
    /// Maps UDF name → Python callable.
    udfs: Mutex<std::collections::HashMap<String, PyObject>>,
    /// Row-Level Security policies for multi-tenant isolation.
    /// Maps collection name → tenant_id.
    rls_policies: Mutex<std::collections::HashMap<String, String>>,
}

#[pymethods]
impl Storage {
    /// Create a new Storage backed by a local path or S3 URL.
    ///
    /// Auto-detects the storage type:
    ///   - `Storage('/var/lib/pond')` → local filesystem
    ///   - `Storage('s3://bucket/prefix?region=us-east-1&endpoint=...')` → S3
    ///   - `Storage('.')` → local filesystem (current directory)
    ///
    /// For S3, credentials are read from the environment:
    ///   AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN (optional)
    ///
    /// For S3, you can also pass credentials as optional kwargs:
    ///   `Storage('s3://...', access_key='...', secret_key='...')`
    #[new]
    #[pyo3(signature = (location, access_key=None, secret_key=None, region=None, endpoint=None, cache_dir=None))]
    fn new(
        location: &str,
        access_key: Option<&str>,
        secret_key: Option<&str>,
        region: Option<&str>,
        endpoint: Option<&str>,
        cache_dir: Option<&str>,
    ) -> PyResult<Self> {
        if location.starts_with("s3://") {
            // S3-compatible storage
            #[cfg(feature = "s3")]
            {
                let url = build_s3_url(location, access_key, secret_key, region, endpoint);
                // If credentials are provided via kwargs, set them as env vars
                // (S3ObjectStore::from_url reads from env)
                if let (Some(ak), Some(sk)) = (access_key, secret_key) {
                    std::env::set_var("AWS_ACCESS_KEY_ID", ak);
                    std::env::set_var("AWS_SECRET_ACCESS_KEY", sk);
                }
                // Wrap with the 3-tier smart cache (memory → disk → S3)
                // when the `cache` feature is on. Warm reads become
                // single-digit-ms instead of 50-300ms S3 RTTs.
                #[cfg(feature = "cache")]
                let kernel = s3_kernel_cached(&url, cache_dir)?;
                #[cfg(not(feature = "cache"))]
                let kernel = {
                    let store = pond_s3::S3ObjectStore::from_url(&url)
                        .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
                    PondKernel::new_with_store(Box::new(store))
                };
                let storage = UnifiedStorage::new(kernel);
                Ok(Self {
                    storage: Arc::new(Mutex::new(storage)),
                    udfs: Mutex::new(std::collections::HashMap::new()),
                    rls_policies: Mutex::new(std::collections::HashMap::new()),
                })
            }
            #[cfg(not(feature = "s3"))]
            {
                Err(pyo3::exceptions::PyIOError::new_err(
                    "S3 support not compiled in. Rebuild with default features."
                ))
            }
        } else {
            // Local filesystem
            // (Silence unused-param when built without the s3 feature.)
            let _ = cache_dir;
            let path = location.strip_prefix("file://").unwrap_or(location);
            let storage = UnifiedStorage::new_local(path)
                .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
            Ok(Self {
                storage: Arc::new(Mutex::new(storage)),
                udfs: Mutex::new(std::collections::HashMap::new()),
                rls_policies: Mutex::new(std::collections::HashMap::new()),
            })
        }
    }

    /// Create a new Storage backed by S3-compatible storage.
    ///
    /// This is a convenience method — equivalent to `Storage('s3://...')`.
    /// Kept for explicit clarity, but `Storage()` auto-detects S3 URLs.
    /// Uses the 3-tier smart cache (see `POND_CACHE_DIR` / `s3_kernel_cached`).
    #[cfg(feature = "s3")]
    #[staticmethod]
    #[pyo3(signature = (url, cache_dir=None))]
    fn from_s3(url: &str, cache_dir: Option<&str>) -> PyResult<Self> {
        #[cfg(feature = "cache")]
        let kernel = s3_kernel_cached(url, cache_dir)?;
        #[cfg(not(feature = "cache"))]
        let kernel = {
            let store = pond_s3::S3ObjectStore::from_url(url)
                .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
            PondKernel::new_with_store(Box::new(store))
        };
        let storage = UnifiedStorage::new(kernel);
        Ok(Self {
            storage: Arc::new(Mutex::new(storage)),
            udfs: Mutex::new(std::collections::HashMap::new()),
            rls_policies: Mutex::new(std::collections::HashMap::new()),
        })
    }

    /// Write data to a collection on the active branch.
    ///
    /// Args:
    ///   collection: The collection name
    ///   data: The data to write (bytes)
    ///   message: The commit message
    ///
    /// Returns:
    ///   The commit hash (hex string)
    fn write(&self, collection: &str, data: &[u8], message: &str) -> PyResult<String> {
        let storage = self.storage.lock().unwrap();
        let active = storage.get_active_branch(collection);
        storage_write::write(storage.kernel(), collection, &active, data, message)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))
    }

    /// Read data from a collection's active branch.
    ///
    /// Args:
    ///   collection: The collection name
    ///
    /// Returns:
    ///   The data as bytes
    fn read<'py>(&self, py: Python<'py>, collection: &str) -> PyResult<Bound<'py, PyBytes>> {
        let storage = self.storage.lock().unwrap();
        let active = storage.get_active_branch(collection);
        let data = storage_read::read(storage.kernel(), collection, &active)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        Ok(PyBytes::new_bound(py, &data))
    }

    /// Write structured columns as a PND2 blob with column stats.
    ///
    /// Supports INT64, FLOAT64, and STRING column types (auto-detected from
    /// Python values). Each column's encoding is chosen automatically:
    ///   - INT64: RLE/DICT/BITPACK/RAW (based on data characteristics)
    ///   - FLOAT64: RAW
    ///   - STRING: RAW
    ///
    /// **CRDT by default**: auto-adds `_rowid` (UUIDv7) and `_version` (HLC)
    /// columns if not already present. This makes all data written via
    /// write_rows compatible with upsert_shard / delete_shard (which match
    /// by _rowid). Set `crdt=False` to opt out (raw bulk load, no CRDT).
    ///
    /// Args:
    ///   - collection: Collection name
    ///   - columns: List of (name, list_of_values) tuples
    ///   - message: Commit message
    ///   - crdt: If True (default), auto-add _rowid + _version columns
    ///
    /// Returns:
    ///   The commit hash
    ///
    /// Example:
    ///   s.write_rows('users', [
    ///       ('id', [1, 2, 3]),
    ///       ('score', [1.5, 2.5, 3.5]),
    ///       ('name', ['alice', 'bob', 'carol']),
    ///   ], 'init')
    ///   # → automatically adds _rowid + _version columns
    ///   # → data is now compatible with upsert_shard / delete_shard
    ///
    /// With `where=` filter (only write rows matching the condition):
    ///   s.write_rows('users', [('id', [1,2,3]), ('age', [20,30,40])], 'init',
    ///                where={'age': ('>', 25)})
    ///   # → only writes rows where age > 25
    #[pyo3(signature = (collection, columns, message, crdt=true, r#where=None))]
    fn write_rows(&self, collection: &str, columns: Vec<(String, Vec<PyObject>)>, message: &str, crdt: bool, r#where: Option<PyObject>) -> PyResult<String> {
        let storage = self.storage.lock().unwrap();
        let active = storage.get_active_branch(collection);

        // Convert Python (name, values) to Rust (name, TypedColumn)
        let typed_cols: Vec<(String, TypedColumn)> = columns.into_iter()
            .map(|(name, values)| {
                let typed = python_values_to_typed_column(&values);
                (name, typed)
            })
            .collect();

        // If where= is provided, filter rows before writing
        let mut final_cols: Vec<(String, TypedColumn)> = if let Some(ref w) = r#where {
            let where_expr = parse_where_param(w)
                .map_err(pyo3::exceptions::PyValueError::new_err)?;

            // Determine number of rows
            let n_rows = typed_cols.first().map(|(_, c)| c.len()).unwrap_or(0);

            // Build a row-index → keep bool mask
            let mut keep_mask: Vec<bool> = Vec::with_capacity(n_rows);
            for row_idx in 0..n_rows {
                // Build a JSON object for this row
                let mut row_obj = serde_json::Map::new();
                for (name, col) in &typed_cols {
                    let val = extract_cell(col, row_idx);
                    row_obj.insert(name.clone(), val);
                }
                let row = JsonValue::Object(row_obj);
                keep_mask.push(where_expr.eval(&row));
            }

            // Filter each column to only kept rows
            typed_cols.into_iter()
                .map(|(name, col)| (name, filter_column(col, &keep_mask)))
                .collect()
        } else {
            typed_cols
        };

        // === RLS: auto-add _tenant column ===
        // If an RLS policy is active for this collection, inject a _tenant
        // column with the tenant_id value for every row (unless the user
        // already provided one).
        let rls_tenant: Option<String> = {
            let policies = self.rls_policies.lock().unwrap();
            policies.get(collection).cloned()
        };
        if let Some(ref tenant) = rls_tenant {
            let n_rows = final_cols.first().map(|(_, c)| c.len()).unwrap_or(0);
            let has_tenant = final_cols.iter().any(|(name, _)| name == "_tenant");
            if !has_tenant && n_rows > 0 {
                let tenant_vals: Vec<String> = (0..n_rows).map(|_| tenant.clone()).collect();
                final_cols.push(("_tenant".to_string(), TypedColumn::String(tenant_vals)));
            }
        }

        let col_refs: Vec<(&str, TypedColumn)> = final_cols.iter()
            .map(|(name, col)| (name.as_str(), col.clone()))
            .collect();

        if crdt {
            storage_write::write_rows(storage.kernel(), collection, &active, &col_refs, message)
                .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))
        } else {
            storage_write::write_rows_no_crdt(storage.kernel(), collection, &active, &col_refs, message)
                .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))
        }
    }

    // ===================================================================
    // HIGH-LEVEL ROW OPERATIONS — update_rows, delete_rows, merge_rows
    //
    // These are built on top of the shard primitives (upsert_shard,
    // delete_shard). They auto-generate shard names, support predicate
    // filtering (like SQL WHERE), and have an optional `crdt=True` flag.
    //
    // When crdt=True (default): use shard-based operations (append-only,
    //   no HEAD rewrite, concurrent-writer safe)
    // When crdt=False: rewrite HEAD (snapshot semantics, not concurrent-safe)
    //
    // The _rowid + _version + _deleted columns are ALWAYS used internally.
    // ===================================================================

    /// Update rows matching a filter — like SQL `UPDATE ... WHERE`.
    ///
    /// Reads existing rows, applies the updates to rows that match the
    /// `where` filter, and writes them back as a CRDT upsert shard.
    ///
    /// Args:
    ///   - collection: Collection name
    ///   - updates: dict of {column: new_value} to apply to matching rows
    ///   - where: optional filter dict {column: value} for equality matching.
    ///       If None, updates ALL rows (use with caution).
    ///   - key_col: the key column for matching (default: '_rowid')
    ///   - crdt: if True (default), write as a CRDT shard (concurrent-safe,
    ///       no HEAD rewrite). If False, rewrite HEAD.
    ///
    /// Returns: number of rows updated
    ///
    /// Example:
    ///   s.update_rows('users', {'status': 'active'}, where={'city': 'NYC'})
    ///   # → UPDATE users SET status='active' WHERE city='NYC'
    #[pyo3(signature = (collection, updates, r#where=None, key_col=None, crdt=true))]
    fn update_rows(&self, collection: &str, updates: PyObject, r#where: Option<PyObject>, key_col: Option<&str>, crdt: bool) -> PyResult<usize> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();
        let active = storage.get_active_branch(collection);

        // Parse updates dict
        let updates_dict = python_to_json(&updates);
        let updates_obj = updates_dict.as_object()
            .ok_or_else(|| pyo3::exceptions::PyTypeError::new_err("updates must be a dict"))?;

        // Parse where filter (SQL string or dict)
        let where_expr: WhereExpr = if let Some(ref w) = r#where {
            parse_where_param(w)
                .map_err(pyo3::exceptions::PyValueError::new_err)?
        } else {
            WhereExpr::True
        };

        // Read all current rows (HEAD + shards) as (rowid, row JSON)
        let kc: Vec<String> = key_col.map(|k| vec![k.to_string()])
            .unwrap_or_else(|| vec!["_rowid".to_string()]);
        let all_rows = read_collection_as_json_rows(&storage, collection, &kc)
            .map_err(pyo3::exceptions::PyIOError::new_err)?;

        // Filter rows that match the where clause
        let mut matched: Vec<JsonValue> = Vec::new();
        for (_rowid, row) in &all_rows {
            if where_expr.eval(row) {
                // Apply updates to a copy of the row
                let mut updated = row.clone();
                if let Some(obj) = updated.as_object_mut() {
                    for (col, val) in updates_obj {
                        obj.insert(col.clone(), val.clone());
                    }
                    // Bump _version (HLC)
                    use pond_kernel::crdt::HLC;
                    let mut hlc = HLC::new();
                    obj.insert("_version".to_string(), json!(hlc.tick()));
                }
                matched.push(updated);
            }
        }

        let count = matched.len();
        if count == 0 {
            return Ok(0);
        }

        if crdt {
            // Write as a CRDT upsert shard — observe existing versions first
            // so the updated rows' _version is guaranteed newer than HEAD's.
            use pond_kernel::crdt::HLC;
            let mut hlc = HLC::new();
            for (_, row) in &all_rows {
                if let Some(v) = row.get("_version").and_then(|v| v.as_str()) {
                    hlc.observe(v);
                }
            }
            let shard_name = format!("update_{}", chrono_like_id());
            pond_storage::shard::upsert_shard(
                kernel, collection, &active, &shard_name,
                &matched, key_col, &mut hlc,
            ).map_err(pyo3::exceptions::PyIOError::new_err)?;
        } else {
            // Rewrite HEAD: merge updated rows with non-matching rows
            let mut final_rows: Vec<JsonValue> = Vec::new();
            let matched_rowids: std::collections::HashSet<String> = matched.iter()
                .filter_map(|r| r.get("_rowid").and_then(|v| v.as_str()).map(|s| s.to_string()))
                .collect();
            for (_rowid, row) in &all_rows {
                let rid = row.get("_rowid").and_then(|v| v.as_str()).map(|s| s.to_string());
                if let Some(rid) = &rid {
                    if matched_rowids.contains(rid) {
                        // Use the updated version
                        if let Some(updated) = matched.iter().find(|r|
                            r.get("_rowid").and_then(|v| v.as_str()) == Some(rid.as_str())
                        ) {
                            final_rows.push(updated.clone());
                            continue;
                        }
                    }
                }
                final_rows.push(row.clone());
            }
            // Write as a new HEAD snapshot
            write_rows_from_json(kernel, collection, &active, &final_rows, "update_rows")?;
        }

        Ok(count)
    }

    /// Delete rows matching a filter — like SQL `DELETE FROM ... WHERE`.
    ///
    /// Writes tombstones for rows that match the `where` filter. On merge,
    /// tombstoned rows are suppressed.
    ///
    /// Args:
    ///   - collection: Collection name
    ///   - where: optional filter dict {column: value} for equality matching.
    ///       If None, deletes ALL rows (use with caution).
    ///   - key_col: the key column for matching (default: '_rowid')
    ///   - crdt: if True (default), write as a tombstone shard. If False,
    ///       rewrite HEAD without the deleted rows.
    ///
    /// Returns: number of rows deleted
    ///
    /// Example:
    ///   s.delete_rows('users', where={'status': 'inactive'})
    ///   # → DELETE FROM users WHERE status='inactive'
    #[pyo3(signature = (collection, r#where=None, key_col=None, crdt=true))]
    fn delete_rows(&self, collection: &str, r#where: Option<PyObject>, key_col: Option<&str>, crdt: bool) -> PyResult<usize> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();
        let active = storage.get_active_branch(collection);

        // Parse where filter (SQL string or dict)
        let where_expr: WhereExpr = if let Some(ref w) = r#where {
            parse_where_param(w)
                .map_err(pyo3::exceptions::PyValueError::new_err)?
        } else {
            WhereExpr::True
        };

        // Read all current rows
        let kc: Vec<String> = key_col.map(|k| vec![k.to_string()])
            .unwrap_or_else(|| vec!["_rowid".to_string()]);
        let all_rows = read_collection_as_json_rows(&storage, collection, &kc)
            .map_err(pyo3::exceptions::PyIOError::new_err)?;

        // Filter rows that match the where clause
        let mut matched_rowids: Vec<String> = Vec::new();
        let mut surviving: Vec<JsonValue> = Vec::new();
        for (rowid, row) in &all_rows {
            if where_expr.eval(row) {
                matched_rowids.push(rowid.clone());
            } else {
                surviving.push(row.clone());
            }
        }

        let count = matched_rowids.len();
        if count == 0 {
            return Ok(0);
        }

        if crdt {
            // Write a tombstone shard — observe existing versions first so the
            // tombstone's _version is guaranteed newer than HEAD's.
            use pond_kernel::crdt::HLC;
            let mut hlc = HLC::new();
            // Observe all existing _version values to advance the HLC past them
            for (_, row) in &all_rows {
                if let Some(v) = row.get("_version").and_then(|v| v.as_str()) {
                    hlc.observe(v);
                }
            }
            let shard_name = format!("delete_{}", chrono_like_id());
            pond_storage::shard::delete_shard(
                kernel, collection, &active, &shard_name,
                &matched_rowids, key_col, &mut hlc,
            ).map_err(pyo3::exceptions::PyIOError::new_err)?;
        } else {
            // Rewrite HEAD without the deleted rows
            write_rows_from_json(kernel, collection, &active, &surviving, "delete_rows")?;
        }

        Ok(count)
    }

    /// Merge rows into a collection — upsert all (insert-or-update).
    ///
    /// Like SQL `MERGE` / `INSERT ... ON CONFLICT UPDATE`. Rows matching
    /// by `key_col` are updated; rows that don't match are inserted.
    ///
    /// Args:
    ///   - collection: Collection name
    ///   - rows: list of row dicts to merge
    ///   - key_col: the key column for matching (default: '_rowid')
    ///   - crdt: if True (default), write as a CRDT upsert shard. If False,
    /// Merge rows into a collection — SQL MERGE with multi-action + multi-key.
    ///
    /// Incoming rows are matched against existing rows by one or more key
    /// columns. You can perform ALL of update + delete + insert in a single
    /// call by passing a merge plan.
    ///
    /// Args:
    ///   - collection: target collection name
    ///   - rows: list of source row dicts to merge
    ///   - on: key specification for matching:
    ///       'id'                          → single key, same name both sides
    ///       ['id', 'email']               → multi-key, same names
    ///       [('user_id', 'id')]           → single key, different names (target, source)
    ///       [('user_id', 'id'), ('code', 'code')]  → multi-key, mixed names
    ///   - key_col: shorthand for on='col' (single key, same name). Deprecated — use `on`.
    ///   - where: SQL WHERE filter on INCOMING (source) rows
    ///   - on_match: what to do when a source row matches an existing target row:
    ///       'update'                      → update the target row (default)
    ///       'delete'                      → delete/tombstone the target row
    ///       'skip'                        → do nothing
    ///       ['update', 'delete']          → multi-action: process BOTH
    ///       {'update': "age > 18", 'delete': "age < 18"}  → conditional multi-action
    ///   - on_miss: what to do when a source row has NO match in target:
    ///       'insert'                      → insert as new row (default)
    ///       'skip'                        → do nothing
    ///       {'insert': "age >= 18"}       → conditional insert
    ///   - crdt: if True (default), use CRDT shards. If False, rewrite HEAD.
    ///
    /// Returns: dict with counts {'matched': N, 'updated': N, 'deleted': N, 'inserted': N, 'skipped': N}
    ///
    /// Examples:
    ///   # Standard upsert
    ///   s.merge_rows('users', rows, on='id')
    ///
    ///   # Multi-key with different names: match users.user_id = source.id
    ///   s.merge_rows('users', rows, on=[('user_id', 'id'), ('code', 'code')])
    ///
    ///   # Multi-action: update adults, delete minors
    ///   s.merge_rows('users', rows, on='id',
    ///       on_match={'update': "age >= 18", 'delete': "age < 18"})
    ///
    ///   # Insert-only (skip if exists)
    ///   s.merge_rows('users', rows, on='id', on_match='skip')
    ///
    ///   # Anti-join (delete matched)
    ///   s.merge_rows('users', rows, on='id', on_match='delete')
    #[pyo3(signature = (collection, rows, on=None, key_col=None, crdt=true, r#where=None, on_match=None, on_miss=None, on_miss_target=None))]
    fn merge_rows(&self, collection: &str, rows: Vec<PyObject>, on: Option<PyObject>, key_col: Option<&str>, crdt: bool, r#where: Option<PyObject>, on_match: Option<PyObject>, on_miss: Option<PyObject>, on_miss_target: Option<PyObject>) -> PyResult<PyObject> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();
        let active = storage.get_active_branch(collection);

        let json_rows: Vec<JsonValue> = rows.iter().map(python_to_json).collect();

        // Apply where= filter to incoming rows
        let filtered_rows: Vec<JsonValue> = if let Some(ref w) = r#where {
            let where_expr = parse_where_param(w)
                .map_err(pyo3::exceptions::PyValueError::new_err)?;
            json_rows.into_iter()
                .filter(|row| where_expr.eval(row))
                .collect()
        } else {
            json_rows
        };

        // Parse the `on` parameter into a list of (target_col, source_col) pairs
        let match_keys: Vec<(String, String)> = parse_match_keys(on, key_col)
            .map_err(pyo3::exceptions::PyValueError::new_err)?;

        // Read existing rows for matching
        let kc: Vec<String> = match_keys.iter().map(|(t, _)| t.clone()).collect();
        let existing = read_collection_as_json_rows(&storage, collection, &kc)
            .map_err(pyo3::exceptions::PyIOError::new_err)?;

        // Build a lookup: composite key → (existing _rowid, existing row)
        let mut key_to_target: std::collections::HashMap<String, (String, JsonValue)> = std::collections::HashMap::new();
        for (_, row) in &existing {
            let mut composite_key = String::new();
            for (target_col, _) in &match_keys {
                let val = row.get(target_col).map(|v| v.to_string()).unwrap_or_default();
                if !composite_key.is_empty() { composite_key.push('\x1f'); }
                composite_key.push_str(&val);
            }
            if let Some(rowid) = row.get("_rowid").and_then(|v| v.as_str()).map(|s| s.to_string()) {
                key_to_target.insert(composite_key, (rowid, row.clone()));
            }
        }

        // Track which target rows were matched (for on_miss_target)
        let mut matched_target_keys: std::collections::HashSet<String> = std::collections::HashSet::new();

        // Parse action plans
        let match_plan = parse_merge_action(on_match, true)
            .map_err(pyo3::exceptions::PyValueError::new_err)?;
        let miss_plan = parse_merge_action(on_miss, false)
            .map_err(pyo3::exceptions::PyValueError::new_err)?;

        // on_miss_target defaults to Skip (do nothing with unmatched targets)
        // — NOT Update, which would overwrite target rows with empty source data
        let miss_target_plan = if on_miss_target.is_some() {
            parse_merge_action(on_miss_target, true)
                .map_err(pyo3::exceptions::PyValueError::new_err)?
        } else {
            vec![MergePlanAction { action: MergeActionType::Skip, condition: None, set: None }]
        };

        // Classify incoming rows into actions
        let mut to_upsert: Vec<JsonValue> = Vec::new();
        let mut to_delete: Vec<String> = Vec::new();
        let mut counts = MergeCounts::default();

        for row in &filtered_rows {
            // Build composite key from source row using source_col names
            let mut composite_key = String::new();
            for (_, source_col) in &match_keys {
                let val = row.get(source_col).map(|v| v.to_string()).unwrap_or_default();
                if !composite_key.is_empty() { composite_key.push('\x1f'); }
                composite_key.push_str(&val);
            }

            let matched = key_to_target.get(&composite_key);

            if let Some((existing_rowid, target_row)) = matched {
                // WHEN MATCHED (source matches target)
                matched_target_keys.insert(composite_key);
                counts.matched += 1;
                let ctx = build_merge_context(Some(target_row), row);

                for action in &match_plan {
                    if let Some(ref cond) = action.condition {
                        if !cond.eval(&ctx) {
                            counts.skipped += 1;
                            continue;
                        }
                    }
                    match action.action {
                        MergeActionType::Update => {
                            let updated = apply_set_to_row(
                                Some(target_row), row, &action.set, Some(existing_rowid),
                            );
                            to_upsert.push(updated);
                            counts.updated += 1;
                        }
                        MergeActionType::Delete => {
                            to_delete.push(existing_rowid.clone());
                            counts.deleted += 1;
                        }
                        MergeActionType::Skip => {
                            counts.skipped += 1;
                        }
                        MergeActionType::Insert => {}
                    }
                }
            } else {
                // WHEN NOT MATCHED BY TARGET (source has no matching target)
                let ctx = build_merge_context(None, row);

                for action in &miss_plan {
                    if let Some(ref cond) = action.condition {
                        if !cond.eval(&ctx) {
                            counts.skipped += 1;
                            continue;
                        }
                    }
                    match action.action {
                        MergeActionType::Insert => {
                            let inserted = apply_set_to_row(None, row, &action.set, None);
                            to_upsert.push(inserted);
                            counts.inserted += 1;
                        }
                        MergeActionType::Skip => {
                            counts.skipped += 1;
                        }
                        _ => {}
                    }
                }
            }
        }

        // WHEN NOT MATCHED BY SOURCE (target rows with no matching source)
        // Process target rows that were NOT matched by any source row
        if !miss_target_plan.is_empty() {
            // Build a set of matched target rowids
            let matched_rowids: std::collections::HashSet<String> = matched_target_keys.iter()
                .filter_map(|k| key_to_target.get(k).map(|(r, _)| r.clone()))
                .collect();

            for (_, target_row) in &existing {
                let target_rowid = target_row.get("_rowid")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .unwrap_or_default();

                if matched_rowids.contains(&target_rowid) {
                    continue; // This target was matched — skip
                }

                // This target has no matching source → apply on_miss_target plan
                let ctx = build_merge_context(Some(target_row), &JsonValue::Object(serde_json::Map::new()));

                for action in &miss_target_plan {
                    if let Some(ref cond) = action.condition {
                        if !cond.eval(&ctx) {
                            counts.skipped += 1;
                            continue;
                        }
                    }
                    match action.action {
                        MergeActionType::Delete => {
                            to_delete.push(target_rowid.clone());
                            counts.deleted += 1;
                        }
                        MergeActionType::Update => {
                            // Update with SET (no source row → only static/t.col values work)
                            let updated = apply_set_to_row(
                                Some(target_row),
                                &JsonValue::Object(serde_json::Map::new()),
                                &action.set,
                                Some(&target_rowid),
                            );
                            to_upsert.push(updated);
                            counts.updated += 1;
                        }
                        MergeActionType::Skip => {
                            counts.skipped += 1;
                        }
                        MergeActionType::Insert => {} // not valid for on_miss_target
                    }
                }
            }
        }

        // Execute the writes
        if crdt {
            use pond_kernel::crdt::HLC;
            let mut hlc = HLC::new();
            for (_, row) in &existing {
                if let Some(v) = row.get("_version").and_then(|v| v.as_str()) {
                    hlc.observe(v);
                }
            }

            if !to_upsert.is_empty() {
                let shard_name = format!("merge_{}", chrono_like_id());
                let kc_ref: Option<&str> = match_keys.first().map(|(t, _)| t.as_str());
                pond_storage::shard::upsert_shard(
                    kernel, collection, &active, &shard_name,
                    &to_upsert, kc_ref, &mut hlc,
                ).map_err(pyo3::exceptions::PyIOError::new_err)?;
            }

            if !to_delete.is_empty() {
                let shard_name = format!("merge_del_{}", chrono_like_id());
                let kc_ref: Option<&str> = match_keys.first().map(|(t, _)| t.as_str());
                pond_storage::shard::delete_shard(
                    kernel, collection, &active, &shard_name,
                    &to_delete, kc_ref, &mut hlc,
                ).map_err(pyo3::exceptions::PyIOError::new_err)?;
            }
        } else {
            let mut merged: std::collections::HashMap<String, JsonValue> = std::collections::HashMap::new();
            let delete_rowids: std::collections::HashSet<String> = to_delete.into_iter().collect();

            for (_, row) in &existing {
                let rowid = row.get("_rowid").and_then(|v| v.as_str()).map(|s| s.to_string())
                    .unwrap_or_default();
                if !delete_rowids.contains(&rowid) {
                    merged.insert(rowid, row.clone());
                }
            }

            for row in &to_upsert {
                let key = match_keys.first()
                    .and_then(|(t, s)| row.get(s).or_else(|| row.get(t)))
                    .or_else(|| row.get("_rowid"))
                    .map(|v| v.to_string())
                    .unwrap_or_default();
                merged.insert(key, row.clone());
            }

            let final_rows: Vec<JsonValue> = merged.into_values().collect();
            write_rows_from_json(kernel, collection, &active, &final_rows, "merge_rows")?;
        }

        // Return result dict
        let py_result = self.merge_result(counts)?;
        Ok(py_result)
    }

    /// Execute a SQL statement — full SELECT/UPDATE/DELETE/INSERT/MERGE support.
    ///
    /// This is the unified SQL interface (like pyspark.sql() or duckdb.sql()).
    /// All execution happens in Rust — zero Python overhead.
    ///
    /// Args:
    ///   - sql: a SQL statement string
    ///
    /// Returns:
    ///   - For SELECT: a dict of {column: [values]} (same as read_rows)
    ///   - For UPDATE/DELETE/MERGE: a dict with counts
    ///   - For INSERT: the new commit hash
    ///
    /// Examples:
    ///   # SELECT
    ///   result = s.sql("SELECT * FROM users WHERE age >= 18 AND city = 'NYC'")
    ///   result = s.sql("SELECT name, salary FROM employees WHERE dept = 'eng'")
    ///
    ///   # UPDATE
    ///   s.sql("UPDATE users SET status = 'active' WHERE age >= 18")
    ///
    ///   # DELETE
    ///   s.sql("DELETE FROM users WHERE status = 'inactive'")
    ///
    ///   # INSERT
    ///   s.sql("INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob')")
    ///
    ///   # MERGE
    ///   s.sql("MERGE INTO users USING [{\"id\":1,\"name\":\"alice\"}] ON id = id WHEN MATCHED THEN UPDATE WHEN NOT MATCHED THEN INSERT")
    fn sql(&self, py: Python<'_>, sql: &str) -> PyResult<PyObject> {
        // === UDF pushdown: extract function calls from WHERE ===
        // If the SQL is a SELECT and there are registered UDFs, scan the SQL
        // for `func_name(col)` patterns and replace them with marker comparisons
        // that the WHERE parser can handle. The UDFs are evaluated separately
        // during row filtering.
        let udf_names: Vec<String> = {
            let udfs = self.udfs.lock().unwrap();
            udfs.keys().cloned().collect()
        };
        let is_select = sql.trim_start()
            .to_lowercase()
            .starts_with("select");
        let (cleaned_sql, udf_calls) = if is_select && !udf_names.is_empty() {
            extract_udf_calls_from_sql(sql, &udf_names)
        } else {
            (sql.to_string(), Vec::new())
        };

        let stmt = parse_sql(&cleaned_sql).map_err(pyo3::exceptions::PyValueError::new_err)?;

        match stmt {
            SqlStatement::Select { table, alias, columns, joins, r#where, .. } => {
                // Execute SELECT — read rows from table (collection or file),
                // apply JOINs, apply WHERE, project columns
                let storage = self.storage.lock().unwrap();

                // Fast path: when no UDFs are active, delegate to the
                // pure-Rust `pond_sql::execute` (supports GROUP BY, ORDER BY,
                // LIMIT, HAVING, aggregates, subqueries, etc.) and convert
                // the row-oriented result to the columnar dict that Python
                // callers expect.
                if udf_calls.is_empty() {
                    let result = pond_sql::execute(&storage, &cleaned_sql)
                        .map_err(pyo3::exceptions::PyIOError::new_err)?;
                    return Ok(sql_result_to_pydict(py, result));
                }

                // Read the base table
                let mut result_rows = read_table_rows(&storage, &table)
                    .map_err(pyo3::exceptions::PyIOError::new_err)?;

                // If there's an alias, prefix all columns with the alias
                if let Some(ref al) = alias {
                    for row in &mut result_rows {
                        if let Some(obj) = row.as_object_mut() {
                            let prefixed: Vec<(String, JsonValue)> = obj.iter()
                                .map(|(k, v)| (format!("{}.{}", al, k), v.clone()))
                                .collect();
                            obj.clear();
                            for (k, v) in prefixed {
                                obj.insert(k, v);
                            }
                        }
                    }
                }

                // Execute JOINs
                for join in &joins {
                    let right_rows = read_table_rows(&storage, &join.table)
                        .map_err(pyo3::exceptions::PyIOError::new_err)?;

                    // Prefix right rows with alias if present
                    let mut right_rows_prefixed: Vec<JsonValue> = right_rows;
                    if let Some(ref al) = join.alias {
                        for row in &mut right_rows_prefixed {
                            if let Some(obj) = row.as_object_mut() {
                                let prefixed: Vec<(String, JsonValue)> = obj.iter()
                                    .map(|(k, v)| (format!("{}.{}", al, k), v.clone()))
                                    .collect();
                                obj.clear();
                                for (k, v) in prefixed {
                                    obj.insert(k, v);
                                }
                            }
                        }
                    }

                    // Execute the join
                    result_rows = execute_join(result_rows, right_rows_prefixed, &join.on, &join.join_type);
                }

                // Pre-fetch UDF functions (clone_ref requires py token)
                let udf_funcs: Vec<(String, Vec<String>, Option<PyObject>)> = {
                    let udfs = self.udfs.lock().unwrap();
                    udf_calls.iter()
                        .map(|(name, args)| {
                            let func = udfs.get(name).map(|f| f.clone_ref(py));
                            (name.clone(), args.clone(), func)
                        })
                        .collect()
                };

                // Apply WHERE filter (with UDF marker injection if UDFs are active)
                let filtered: Vec<&JsonValue> = result_rows.iter()
                    .filter(|row| {
                        if udf_funcs.is_empty() {
                            return r#where.eval(row);
                        }

                        // Clone the row and inject UDF markers
                        let mut row_with_markers = (*row).clone();
                        if let Some(obj) = row_with_markers.as_object_mut() {
                            for (idx, (_, args, func)) in udf_funcs.iter().enumerate() {
                                let passes = if let Some(f) = func {
                                    evaluate_udf(py, f, row, args)
                                } else {
                                    false
                                };
                                let marker_val = if passes {
                                    JsonValue::Number(serde_json::Number::from(1i64))
                                } else {
                                    JsonValue::Number(serde_json::Number::from(0i64))
                                };
                                obj.insert(format!("_udf_marker_{}", idx), marker_val);
                            }
                        }
                        r#where.eval(&row_with_markers)
                    })
                    .collect();

                // Build columnar result — handle qualified column names
                let mut result_cols: std::collections::HashMap<String, Vec<PyObject>> = std::collections::HashMap::new();
                for row in &filtered {
                    if let Some(obj) = row.as_object() {
                        for (name, value) in obj {
                            // Apply projection (support both "col" and "alias.col")
                            if !columns.is_empty() {
                                let matches = columns.iter().any(|c| {
                                    c == name || c.split('.').next_back() == Some(name.as_str())
                                        || name.ends_with(&format!(".{}", c))
                                });
                                if !matches { continue; }
                            }
                            // Skip CRDT metadata + RLS _tenant + UDF marker columns
                            let base_name = name.rsplit('.').next().unwrap_or(name);
                            if base_name == "_rowid" || base_name == "_version" || base_name == "_deleted"
                                || base_name == "_tenant" || base_name.starts_with("_udf_marker_") {
                                continue;
                            }
                            let entry = result_cols.entry(name.clone()).or_default();
                            entry.push(json_value_to_py(py, value));
                        }
                    }
                }

                let dict = PyDict::new_bound(py);
                for (name, values) in result_cols {
                    let list = PyList::new_bound(py, values.iter());
                    dict.set_item(&name, list)?;
                }
                Ok(dict.into())
            }

            SqlStatement::Update { collection, sets, r#where } => {
                // Execute UPDATE via update_rows
                let updates_dict = JsonValue::Object(sets.into_iter().collect());
                let updates_py = json_to_pyobject(py, &updates_dict);

                let where_str = where_expr_to_sql(&r#where);
                let count = self.update_rows(
                    &collection,
                    updates_py,
                    Some(where_str.to_object(py)),
                    None,
                    true,
                )?;

                let dict = PyDict::new_bound(py);
                dict.set_item("updated", count)?;
                Ok(dict.into())
            }

            SqlStatement::Delete { collection, r#where } => {
                let where_str = where_expr_to_sql(&r#where);
                let count = self.delete_rows(
                    &collection,
                    Some(where_str.to_object(py)),
                    None,
                    true,
                )?;

                let dict = PyDict::new_bound(py);
                dict.set_item("deleted", count)?;
                Ok(dict.into())
            }

            SqlStatement::Insert { collection, columns, rows } => {
                // Build columnar input from rows
                let mut col_data: std::collections::HashMap<String, Vec<PyObject>> = std::collections::HashMap::new();
                for col_name in &columns {
                    col_data.insert(col_name.clone(), Vec::new());
                }
                for row_vals in &rows {
                    for (i, col_name) in columns.iter().enumerate() {
                        if let Some(val) = row_vals.get(i) {
                            col_data.get_mut(col_name).unwrap().push(json_value_to_py(py, val));
                        }
                    }
                }
                let cols_vec: Vec<(String, Vec<PyObject>)> = columns.iter()
                    .map(|c| (c.clone(), col_data.remove(c).unwrap_or_default()))
                    .collect();

                let commit = self.write_rows(&collection, cols_vec, "INSERT", true, None)?;
                let dict = PyDict::new_bound(py);
                dict.set_item("commit", commit)?;
                Ok(dict.into())
            }

            SqlStatement::Merge { target, source_rows, match_keys, when_matched, when_not_matched } => {
                // Convert source_rows to PyObject list
                let rows_py: Vec<PyObject> = source_rows.iter()
                    .map(|r| json_to_pyobject(py, r))
                    .collect();

                // Build `on` parameter from match_keys
                let _on_str: String = if match_keys.len() == 1 && match_keys[0].0 == match_keys[0].1 {
                    format!("'{}'", match_keys[0].0)
                } else {
                    let pairs: Vec<String> = match_keys.iter()
                        .map(|(t, s)| format!("('{}', '{}')", t, s))
                        .collect();
                    format!("[{}]", pairs.join(", "))
                };

                // For now, call merge_rows with the first key as key_col
                // (full multi-key merge_rows handles this internally)
                let key_col = match_keys.first().map(|(t, _)| t.as_str());
                let on_match_str = match when_matched {
                    MergeAction::Update => "update",
                    MergeAction::Delete => "delete",
                    MergeAction::Skip => "skip",
                    _ => "update",
                };
                let on_miss_str = match when_not_matched {
                    MergeAction::Insert => "insert",
                    MergeAction::Skip => "skip",
                    _ => "insert",
                };

                // Use the Python-level merge_rows with string params
                let on_match_py = on_match_str.to_object(py);
                let on_miss_py = on_miss_str.to_object(py);

                let result = self.merge_rows(
                    &target, rows_py, None, key_col, true,
                    None, Some(on_match_py), Some(on_miss_py), None,
                )?;
                Ok(result)
            }
        }
    }

    /// Read structured columns from a collection with optional pruning.
    ///
    /// Decodes PND2 blobs with predicate pruning (skip row groups whose
    /// stats don't match) and column projection (only decode requested columns).
    ///
    /// Returns typed Python values: int for INT64, float for FLOAT64, str for STRING.
    ///
    /// Args:
    ///   - collection: Collection name
    ///   - columns: Optional list of column names to project (None = all)
    ///   - predicates: Optional list of (column, op, value) for row-group pruning
    ///
    /// Returns:
    ///   Dict of {column_name: list_of_values}
    ///
    /// Example:
    ///   data = s.read_rows('users')
    ///   # → {'id': [1, 2, 3], 'score': [1.5, 2.5, 3.5], 'name': ['a', 'b', 'c']}
    #[pyo3(signature = (collection, columns=None, predicates=None))]
    fn read_rows(
        &self,
        py: Python<'_>,
        collection: &str,
        columns: Option<Vec<String>>,
        predicates: Option<Vec<(String, String, PyObject)>>,
    ) -> PyResult<PyObject> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();

        // Convert predicates to JSON values for flexible comparison
        let predicates_json: Vec<(String, String, JsonValue)> = predicates
            .unwrap_or_default()
            .into_iter()
            .map(|(col, op, val)| (col, op, python_to_json(&val)))
            .collect();

        // === AUTO-INDEX ACCELERATION (multi-key + composite) ===
        // For EACH equality predicate, check if a simple index covers that column.
        // - Single-column index: O(1) exact lookup
        // - Composite index: prefix scan (check if any key contains the value)
        // If an index exists AND the lookup key is not found → return empty (early exit).
        if !predicates_json.is_empty() {
            let indexer = RustSimpleIndex::new(kernel);
            for (col, op, val) in &predicates_json {
                if op == "=" || op == "==" {
                    if let Some(index_name) = indexer.find_index_by_column(collection, col) {
                        let lookup_key = val.to_string();
                        let key_fields = indexer.get_index_key_fields(collection, &index_name);
                        let is_composite = key_fields.as_ref()
                            .map(|f| f.len() > 1)
                            .unwrap_or(false);

                        if is_composite {
                            let ref_name = format!("collections/{}/indexes/{}", collection, index_name);
                            if let Some(hash) = kernel.resolve(&ref_name) {
                                if let Ok(data) = kernel.read_blob(&hash) {
                                    if let Ok(index) = serde_json::from_slice::<std::collections::HashMap<String, String>>(&data) {
                                        let found = index.keys().any(|k| {
                                            k.split('\x1f').any(|comp| comp == lookup_key)
                                        });
                                        if !found {
                                            let dict = PyDict::new_bound(py);
                                            return Ok(dict.into());
                                        }
                                    }
                                }
                            }
                        } else {
                            if indexer.lookup(collection, &index_name, &lookup_key).is_none() {
                                let dict = PyDict::new_bound(py);
                                return Ok(dict.into());
                            }
                        }
                    }
                }
            }
        }

        // === READ HEAD + ALL SHARDS, MERGE BY _rowid (CRDT) ===
        // COLUMNAR PREDICATE EVALUATION: pass predicates to the read function
        // so HEAD data is filtered at the PND2 column level BEFORE JSON conversion.
        // Shards are already JSON — they're filtered after CRDT merge.
        let kc: Vec<String> = vec!["_rowid".to_string()];
        let all_rows = read_collection_as_json_rows_filtered(&storage, collection, &kc, &predicates_json)
            .map_err(pyo3::exceptions::PyIOError::new_err)?;

        // CRDT merge: dedup by _rowid, latest _version wins, tombstones suppress
        let merged = crdt_merge_rows(all_rows);

        // === RLS: filter rows by _tenant ===
        // If an RLS policy is active for this collection, only keep rows
        // where the _tenant column matches the policy's tenant_id.
        let rls_tenant: Option<String> = {
            let policies = self.rls_policies.lock().unwrap();
            policies.get(collection).cloned()
        };
        let merged: Vec<JsonValue> = if let Some(ref tenant) = rls_tenant {
            merged.into_iter().filter(|row| {
                row.get("_tenant").and_then(|v| v.as_str()) == Some(tenant.as_str())
            }).collect()
        } else {
            merged
        };

        // Build projection set
        let projection: Option<std::collections::HashSet<String>> = columns.map(|cols| {
            cols.into_iter().collect()
        });

        // Apply row-level predicates (filter after merge, since shards may have updates)
        // Uses SIMD-accelerated INT64 filter when possible, falls back to JSON comparison
        let filtered: Vec<&JsonValue> = if !predicates_json.is_empty() {
            // Try SIMD-accelerated path: extract INT64 column data and run AVX2 filter
            // For each predicate, if the column is INT64 and the value is numeric,
            // use the SIMD filter. Otherwise fall back to JSON comparison.
            let mut keep_mask: Vec<bool> = vec![true; merged.len()];

            for (col, op, value) in &predicates_json {
                // Try to extract INT64 values for SIMD acceleration
                let i64_values: Option<Vec<i64>> = if let Some(_target_i) = value.as_i64() {
                    // Check if all rows have this column as INT64
                    let vals: Vec<Option<i64>> = merged.iter()
                        .map(|row| row.get(col).and_then(|v| v.as_i64()))
                        .collect();
                    if vals.iter().all(|v| v.is_some()) {
                        Some(vals.into_iter().map(|v| v.unwrap()).collect())
                    } else {
                        None
                    }
                } else {
                    None
                };

                if let Some(ref i64_vals) = i64_values {
                    // SIMD-accelerated path for INT64 columns
                    let target = value.as_i64().unwrap();
                    let col_mask = match op.as_str() {
                        "=" | "==" => simd::filter_eq_i64(i64_vals, target),
                        "!=" | "<>" | ">" | ">=" | "<" | "<=" => simd::filter_cmp_i64(i64_vals, op, target),
                        _ => vec![true; merged.len()],
                    };
                    // AND-combine with existing mask
                    for (i, m) in col_mask.iter().enumerate() {
                        keep_mask[i] = keep_mask[i] && *m;
                    }
                } else {
                    // Scalar JSON comparison fallback for non-INT64 columns
                    for (i, row) in merged.iter().enumerate() {
                        if !keep_mask[i] { continue; }
                        let cell = row.get(col);
                        let matches = match op.as_str() {
                            "=" | "==" => json_values_equal(cell, value),
                            "!=" | "<>" => !json_values_equal(cell, value),
                            ">" => cmp_values(cell, value) == std::cmp::Ordering::Greater,
                            ">=" => matches!(cmp_values(cell, value), std::cmp::Ordering::Greater | std::cmp::Ordering::Equal),
                            "<" => cmp_values(cell, value) == std::cmp::Ordering::Less,
                            "<=" => matches!(cmp_values(cell, value), std::cmp::Ordering::Less | std::cmp::Ordering::Equal),
                            _ => true,
                        };
                        keep_mask[i] = keep_mask[i] && matches;
                    }
                }
            }

            merged.iter().enumerate()
                .filter(|(i, _)| keep_mask[*i])
                .map(|(_, row)| row)
                .collect()
        } else {
            merged.iter().collect()
        };

        // Convert to columnar format — pad missing values with None so all
        // columns have the same length (rows from HEAD + shards may have
        // different key sets after CRDT merge)
        let mut result_cols: std::collections::HashMap<String, Vec<PyObject>> = std::collections::HashMap::new();

        for row in &filtered {
            if let Some(obj) = row.as_object() {
                // Track which keys this row has
                let mut seen_keys: std::collections::HashSet<String> = std::collections::HashSet::new();

                // Get current max column length (for padding)
                let current_max = result_cols.values().map(|v| v.len()).max().unwrap_or(0);

                for (name, value) in obj {
                    // Apply projection
                    if let Some(ref proj) = projection {
                        if !proj.contains(name) { continue; }
                    }
                    seen_keys.insert(name.clone());
                    let entry = result_cols.entry(name.clone()).or_insert_with(|| Vec::with_capacity(filtered.len()));
                    // Pad with None if this column was missing in previous rows
                    while entry.len() < current_max {
                        entry.push(py.None());
                    }
                    let py_val = json_value_to_py(py, value);
                    entry.push(py_val);
                }

                // Pad missing columns with None for this row
                let new_max = result_cols.values().map(|v| v.len()).max().unwrap_or(0);
                for (name, values) in result_cols.iter_mut() {
                    if !seen_keys.contains(name.as_str()) {
                        while values.len() < new_max {
                            values.push(py.None());
                        }
                    }
                }
            }
        }

        // Convert to Python dict — filter out CRDT metadata columns
        // (_rowid, _version, _deleted, _tenant) unless the user explicitly
        // requested them via the columns= parameter.
        let crdt_cols: std::collections::HashSet<&str> = ["_rowid", "_version", "_deleted", "_tenant"]
            .iter().cloned().collect();
        let explicit_columns = projection.is_some();

        let dict = PyDict::new_bound(py);
        for (name, values) in result_cols {
            if !explicit_columns && crdt_cols.contains(name.as_str()) {
                continue;
            }
            let list = PyList::new_bound(py, values.iter());
            dict.set_item(&name, list)?;
        }
        Ok(dict.into())
    }

    /// Create a new branch from the active branch.
    ///
    /// Args:
    ///   collection: The collection name
    ///   branch_name: The new branch name
    ///
    /// Returns:
    ///   The commit hash the branch was created at
    fn branch(&self, collection: &str, branch_name: &str) -> PyResult<String> {
        let storage = self.storage.lock().unwrap();
        let active = storage.get_active_branch(collection);
        storage_branch::branch(storage.kernel(), collection, branch_name, &active)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))
    }

    /// Switch the active branch.
    ///
    /// Args:
    ///   collection: The collection name
    ///   branch_name: The branch to switch to (must exist)
    fn checkout(&self, collection: &str, branch_name: &str) -> PyResult<()> {
        let storage = self.storage.lock().unwrap();
        storage.set_active_branch(collection, branch_name);
        Ok(())
    }

    /// Create a new branch and switch to it (like `git checkout -b`).
    ///
    /// Args:
    ///   collection: The collection name
    ///   branch_name: The new branch name
    fn checkout_new(&self, collection: &str, branch_name: &str) -> PyResult<()> {
        let storage = self.storage.lock().unwrap();
        let active = storage.get_active_branch(collection);
        storage_branch::branch(storage.kernel(), collection, branch_name, &active)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        storage.set_active_branch(collection, branch_name);
        Ok(())
    }

    /// Merge a source branch into a target branch.
    ///
    /// Args:
    ///   collection: The collection name
    ///   source: The source branch name
    ///   target: The target branch name (None = active branch)
    ///   message: The merge commit message
    ///
    /// Returns:
    ///   The merge commit hash
    #[pyo3(signature = (collection, source, target=None, message="merge"))]
    fn merge(&self, collection: &str, source: &str, target: Option<&str>, message: &str) -> PyResult<String> {
        let storage = self.storage.lock().unwrap();
        let target = target.map(|t| t.to_string())
            .unwrap_or_else(|| storage.get_active_branch(collection));
        storage_branch::merge(storage.kernel(), collection, source, &target, message)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))
    }

    /// Show commit history for a collection.
    ///
    /// Args:
    ///   collection: The collection name
    ///   limit: Max number of commits to show (default 20)
    ///
    /// Returns:
    ///   List of (commit_hash, message, index) tuples, newest first
    #[pyo3(signature = (collection, limit=20))]
    fn history(&self, py: Python<'_>, collection: &str, limit: usize) -> PyResult<PyObject> {
        let storage = self.storage.lock().unwrap();
        let active = storage.get_active_branch(collection);

        // Get the current commit hash
        let commit_ref = format!("collections/{}/_branches/{}/commit", collection, active);
        let commit_hash = storage.kernel().resolve(&commit_ref)
            .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err(format!(
                "no commit found for collection '{}' on branch '{}'", collection, active
            )))?;

        // Walk the commit history
        let history = storage_commit::history(storage.kernel(), &commit_hash, limit);

        let list = PyList::new_bound(py, history.iter().map(|(hash, commit)| {
            PyTuple::new_bound(py, [
                hash.to_object(py),
                commit.message.to_object(py),
                commit.index.to_object(py),
            ]).into_any()
        }));
        Ok(list.into())
    }

    /// List all collections.
    ///
    /// Returns:
    ///   List of collection names (strings)
    fn ls(&self, py: Python<'_>) -> PyResult<PyObject> {
        let storage = self.storage.lock().unwrap();
        // List all refs, then extract collection names from "collections/{name}/..."
        let names = storage.kernel().list_names_prefix("collections/");
        let mut collections: Vec<String> = names.iter()
            .filter_map(|n| {
                // n looks like "collections/users/_branches/main/commit"
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
        let list = PyList::new_bound(py, collections.iter().map(|n| n.to_object(py)));
        Ok(list.into())
    }

    /// Undo the last N commits.
    ///
    /// Args:
    ///   collection: The collection name
    ///   steps: Number of commits to undo (default 1)
    ///
    /// Returns:
    ///   The new HEAD commit hash
    #[pyo3(signature = (collection, steps=1))]
    fn undo(&self, collection: &str, steps: usize) -> PyResult<String> {
        let storage = self.storage.lock().unwrap();
        let active = storage.get_active_branch(collection);
        storage_branch::undo(storage.kernel(), collection, &active, steps)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))
    }

    /// Revert to a specific commit.
    ///
    /// Args:
    ///   collection: The collection name
    ///   commit_hash: The commit hash to revert to
    fn revert(&self, collection: &str, commit_hash: &str) -> PyResult<()> {
        let storage = self.storage.lock().unwrap();
        let active = storage.get_active_branch(collection);
        storage_branch::revert(storage.kernel(), collection, &active, commit_hash)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))
    }

    /// Get the active branch name for a collection.
    ///
    /// Returns "main" if no active branch has been set.
    fn get_active_branch(&self, collection: &str) -> String {
        let storage = self.storage.lock().unwrap();
        storage.get_active_branch(collection)
    }

    /// Set the active branch for a collection.
    fn set_active_branch(&self, collection: &str, branch_name: &str) {
        let storage = self.storage.lock().unwrap();
        storage.set_active_branch(collection, branch_name);
    }

    // ===================================================================
    // Index operations — UNIFIED API for ALL index types
    // ===================================================================

    /// Build an index on a collection. Works for ALL index types.
    ///
    /// Args:
    ///   - collection: Collection name
    ///   - index_name: Name for this index
    ///   - index_type: Type of index ("simple", "ivf", "hnsw")
    ///   - config: Dict of index-specific config:
    ///       "simple": {"key_field": "name"}
    ///       "ivf":    {"n_clusters": 10, "metric": "euclidean"}
    ///       "hnsw":   {"m": 16, "ef_construction": 200, "metric": "l2"}
    ///   - rows: For "simple" index — list of (rowid, row_dict) tuples.
    ///           For "ivf"/"hnsw" — not needed (reads from collection).
    ///
    /// Returns:
    ///   The index blob hash.
    ///
    /// Examples:
    ///   # Simple secondary index
    ///   s.build_index('users', 'by_name', 'simple',
    ///       config={'key_field': 'name'},
    ///       rows=[('user:1', {'name': 'alice'})])
    ///
    ///   # IVF vector index
    ///   s.build_index('vectors', 'ann', 'ivf',
    ///       config={'n_clusters': 10, 'metric': 'euclidean'})
    ///
    ///   # HNSW vector index
    ///   s.build_index('vectors', 'ann', 'hnsw',
    ///       config={'m': 16, 'metric': 'l2'})
    #[pyo3(signature = (collection, index_name, index_type, config=None))]
    fn build_index(
        &self,
        collection: &str,
        index_name: &str,
        index_type: &str,
        config: Option<PyObject>,
    ) -> PyResult<String> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();

        match index_type {
            "simple" => {
                let indexer = RustSimpleIndex::new(kernel);
                let cfg = config.as_ref().map(python_to_json);
                // Support both key_field (string) and key_fields (list) in config
                let key_fields: Vec<String> = if let Some(ref c) = cfg {
                    if let Some(arr) = c.get("key_fields").and_then(|v| v.as_array()) {
                        arr.iter()
                            .filter_map(|v| v.as_str().map(|s| s.to_string()))
                            .collect()
                    } else if let Some(s) = c.get("key_field").and_then(|v| v.as_str()) {
                        vec![s.to_string()]
                    } else {
                        vec!["id".to_string()]
                    }
                } else {
                    vec!["id".to_string()]
                };

                // AUTO-READ from the collection — no `rows` parameter needed.
                // Read all rows from HEAD + shards, convert to (rowid, JSON row) pairs.
                let rust_rows = read_collection_as_json_rows(&storage, collection, &key_fields)
                    .map_err(pyo3::exceptions::PyIOError::new_err)?;

                let kf_refs: Vec<&str> = key_fields.iter().map(|s| s.as_str()).collect();
                indexer.build_index(collection, index_name, &rust_rows, |row| {
                    // Build composite key from all key_fields
                    let mut parts: Vec<String> = Vec::new();
                    for kf in &key_fields {
                        match row.get(kf) {
                            Some(JsonValue::String(s)) => parts.push(s.clone()),
                            Some(JsonValue::Number(n)) => parts.push(n.to_string()),
                            Some(JsonValue::Array(arr)) => {
                                for v in arr {
                                    match v {
                                        JsonValue::String(s) => parts.push(s.clone()),
                                        JsonValue::Number(n) => parts.push(n.to_string()),
                                        _ => {}
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                    // For composite keys: join with separator
                    if parts.len() > 1 {
                        vec![parts.join("\x1f")]  // ASCII unit separator
                    } else {
                        parts
                    }
                }, &kf_refs).map_err(pyo3::exceptions::PyIOError::new_err)
            }
            "ivf" => {
                let cfg = config.as_ref().map(python_to_json);
                let n_clusters = cfg.as_ref()
                    .and_then(|c| c.get("n_clusters"))
                    .and_then(|v| v.as_u64())
                    .unwrap_or(10) as usize;
                let metric = cfg.as_ref()
                    .and_then(|c| c.get("metric"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("euclidean");

                // IVF reads from the collection directly (internal)
                let ivf = RustIVFIndex::new(kernel);
                ivf.build(collection, n_clusters, metric)
                    .map_err(pyo3::exceptions::PyIOError::new_err)
            }
            "hnsw" => {
                let cfg = config.as_ref().map(python_to_json);
                let m = cfg.as_ref()
                    .and_then(|c| c.get("m"))
                    .and_then(|v| v.as_u64())
                    .unwrap_or(16) as usize;
                let ef_construction = cfg.as_ref()
                    .and_then(|c| c.get("ef_construction"))
                    .and_then(|v| v.as_u64())
                    .unwrap_or(200) as usize;
                let metric = cfg.as_ref()
                    .and_then(|c| c.get("metric"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("l2");

                // HNSW reads from the collection directly (internal)
                let hnsw = RustHNSWIndex::new(kernel);
                hnsw.build(collection, m, ef_construction, None, metric)
                    .map_err(pyo3::exceptions::PyIOError::new_err)
            }
            _ => Err(pyo3::exceptions::PyValueError::new_err(
                format!("Unknown index type: '{}'. Supported: simple, ivf, hnsw", index_type)
            )),
        }
    }

    /// Look up a rowid by index key (exact lookup — for simple indexes).
    ///
    /// Returns None if the key is not found.
    fn lookup_index(&self, collection: &str, index_name: &str, index_key: &str) -> Option<String> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();
        let indexer = RustSimpleIndex::new(kernel);
        indexer.lookup(collection, index_name, index_key)
    }

    /// Search an index (approximate search — for vector indexes: IVF, HNSW).
    ///
    /// Args:
    ///   - collection: Collection name
    ///   - index_type: "ivf" or "hnsw"
    ///   - query: Query vector (list of floats)
    ///   - k: Number of nearest neighbors to return
    ///   - n_probe: IVF clusters to search (default 10)
    ///   - ef: HNSW beam width (default 50)
    ///
    /// Returns:
    ///   List of (distance, vector_id) tuples, sorted by distance.
    #[pyo3(signature = (collection, index_type, query, k=10, n_probe=10, ef=50))]
    fn search_index(&self, py: Python<'_>, collection: &str, index_type: &str, query: Vec<f64>, k: usize, n_probe: usize, ef: usize) -> PyResult<PyObject> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();

        let results = match index_type {
            "ivf" => {
                let ivf = RustIVFIndex::new(kernel);
                ivf.search(collection, &query, k, n_probe)
                    .map_err(pyo3::exceptions::PyIOError::new_err)?
            }
            "hnsw" => {
                let hnsw = RustHNSWIndex::new(kernel);
                hnsw.search(collection, &query, k, ef)
                    .map_err(pyo3::exceptions::PyIOError::new_err)?
            }
            _ => return Err(pyo3::exceptions::PyValueError::new_err(
                format!("Unknown index type: '{}'. Supported: ivf, hnsw", index_type)
            )),
        };

        let list = PyList::new_bound(py, results.iter().map(|(dist, id)| {
            PyTuple::new_bound(py, [dist.to_object(py), id.to_object(py)]).into_any()
        }));
        Ok(list.into())
    }

    /// Drop an index. Works for ALL index types.
    ///
    /// Returns True if the index existed and was dropped.
    fn drop_index(&self, collection: &str, index_name: &str) -> bool {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();
        let indexer = RustSimpleIndex::new(kernel);
        indexer.drop_index(collection, index_name)
    }

    /// List all active indexes for a collection.
    fn list_indexes(&self, collection: &str) -> Vec<String> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();
        let indexer = RustSimpleIndex::new(kernel);
        indexer.list_indexes(collection)
    }

    // ===================================================================
    // GC / Vacuum — maintenance operations
    // ===================================================================

    /// Analyze reachability and return GC stats (read-only).
    ///
    /// Returns a dict with: live, dead, dead_size_bytes (-1 if compute_size=False)
    #[pyo3(signature = (compute_size=false))]
    fn gc_stats(&self, py: Python<'_>, compute_size: bool) -> PyResult<PyObject> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();
        let gc = pond_storage::maintenance::GarbageCollector::new(kernel);
        let stats = gc.collect(None, compute_size);

        let dict = PyDict::new_bound(py);
        dict.set_item("live", stats.live)?;
        dict.set_item("dead", stats.dead)?;
        dict.set_item("dead_size_bytes", stats.dead_size_bytes)?;
        Ok(dict.into())
    }

    /// Vacuum — delete unreachable blobs with time-travel safety.
    ///
    /// Args:
    ///   - preserve_days: Keep commits younger than N days (default 0)
    ///   - dry_run: If True, report what would be deleted without deleting
    ///
    /// Returns a dict with: deleted, preserved, dry_run
    #[pyo3(signature = (preserve_days=0, dry_run=false))]
    fn vacuum(&self, py: Python<'_>, preserve_days: u32, dry_run: bool) -> PyResult<PyObject> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();
        let gc = pond_storage::maintenance::GarbageCollector::new(kernel);
        let result = gc.vacuum(None, preserve_days, dry_run);

        let dict = PyDict::new_bound(py);
        dict.set_item("deleted", result.deleted)?;
        dict.set_item("preserved", result.preserved)?;
        dict.set_item("dry_run", result.dry_run)?;
        Ok(dict.into())
    }

    // ===================================================================
    // CRDT Shards — concurrent multi-writer without coordination
    //
    // Shards allow multiple writers to write concurrently without CAS:
    //   - Each writer writes its own shard to a unique path
    //   - Readers union HEAD + all live shards via read_with_shards
    //   - compact_shards merges shards into HEAD (clears the shard list)
    //
    // Row-level CRDT operations (upsert_shard, delete_shard) add _rowid
    // + _version to each row, enabling deterministic merge on conflict
    // (latest _version wins, tombstones suppress).
    // ===================================================================

    /// Append a CRDT shard to the active branch.
    ///
    /// The shard is written to a unique path. Readers will discover and
    /// merge it via read_with_shards. No CAS, no coordination — works
    /// on any object store (local FS, S3, R2, MinIO, ...).
    ///
    /// Args:
    ///   - collection: Collection name
    ///   - shard_name: Unique name for this shard (e.g., 'writer1_001')
    ///   - data: The shard data (raw bytes — JSON, PND2, anything)
    ///
    /// Returns: shard blob hash
    fn append_shard(&self, collection: &str, shard_name: &str, data: &[u8]) -> PyResult<String> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();
        let active = storage.get_active_branch(collection);
        pond_storage::shard::append_shard(kernel, collection, &active, shard_name, data)
            .map_err(pyo3::exceptions::PyIOError::new_err)
    }

    /// Upsert rows as a CRDT shard with _rowid + _version.
    ///
    /// Each row gets:
    ///   - _rowid: UUIDv7 (stable across updates, generated if not present)
    ///   - _version: HLC (new per write, used for CRDT merge — latest wins)
    ///   - _deleted: false (tombstone marker)
    ///
    /// On merge (read_with_shards), rows with the same _rowid are
    /// deduplicated — the one with the latest _version wins.
    ///
    /// Args:
    ///   - collection: Collection name
    ///   - shard_name: Unique name for this shard
    ///   - rows: List of row dicts to upsert
    ///   - key_col: Optional key column name (for legacy non-CRDT rows)
    ///
    /// Returns: shard blob hash
    #[pyo3(signature = (collection, shard_name, rows, key_col=None))]
    fn upsert_shard(&self, collection: &str, shard_name: &str, rows: Vec<PyObject>, key_col: Option<&str>) -> PyResult<String> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();
        let active = storage.get_active_branch(collection);

        // Convert Python rows to JSON values
        let json_rows: Vec<JsonValue> = rows.iter().map(python_to_json).collect();

        // Use a thread-local HLC (clock-skew-safe)
        use pond_kernel::crdt::HLC;
        let mut hlc = HLC::new();

        pond_storage::shard::upsert_shard(kernel, collection, &active, shard_name, &json_rows, key_col, &mut hlc)
            .map_err(pyo3::exceptions::PyIOError::new_err)
    }

    /// Delete rows by writing a tombstone shard.
    ///
    /// Each deleted _rowid gets a tombstone with _deleted=true and a new
    /// _version. On merge, if the tombstone's _version is later than any
    /// live row's _version, the row is suppressed.
    ///
    /// Args:
    ///   - collection: Collection name
    ///   - shard_name: Unique name for this tombstone shard
    ///   - rowids: List of _rowid values to tombstone
    ///   - key_col: Optional key column name
    ///
    /// Returns: shard blob hash
    #[pyo3(signature = (collection, shard_name, rowids, key_col=None))]
    fn delete_shard(&self, collection: &str, shard_name: &str, rowids: Vec<String>, key_col: Option<&str>) -> PyResult<String> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();
        let active = storage.get_active_branch(collection);

        use pond_kernel::crdt::HLC;
        let mut hlc = HLC::new();

        pond_storage::shard::delete_shard(kernel, collection, &active, shard_name, &rowids, key_col, &mut hlc)
            .map_err(pyo3::exceptions::PyIOError::new_err)
    }

    /// Read HEAD + all live shards (CRDT read path).
    ///
    /// Returns a list of (shard_name, data_bytes) tuples. The first
    /// element is HEAD (name='__head__'), followed by all shards.
    /// The caller is responsible for merging rows by _rowid (latest
    /// _version wins, tombstones suppress).
    ///
    /// For simple raw-byte reads, use `read()` instead. For structured
    /// reads with auto-merge, use `read_rows()`.
    fn read_with_shards<'py>(&self, py: Python<'py>, collection: &str) -> PyResult<Bound<'py, PyList>> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();
        let active = storage.get_active_branch(collection);

        let (head_manifest, shards) = pond_storage::shard::read_with_shards(kernel, collection, &active);

        let result = PyList::empty_bound(py);

        // Read HEAD data
        if let Some(manifest_hash) = head_manifest {
            if let Ok(manifest_bytes) = kernel.read_blob(&manifest_hash) {
                if let Some(manifest) = pond_storage::manifest::CollectionManifest::decode(&manifest_bytes) {
                    for rg in &manifest.row_groups {
                        if let Ok(data) = kernel.read_blob(&rg.blob_hash) {
                            let tuple = PyTuple::new_bound(py, [
                                "__head__".to_object(py),
                                PyBytes::new_bound(py, &data).into(),
                            ]);
                            result.append(tuple)?;
                        }
                    }
                }
            }
        }

        // Read shard data
        for (name, hash) in &shards {
            if let Ok(data) = kernel.read_blob(hash) {
                let tuple = PyTuple::new_bound(py, [
                    name.to_object(py),
                    PyBytes::new_bound(py, &data).into(),
                ]);
                result.append(tuple)?;
            }
        }

        Ok(result)
    }

    /// Count the number of live shards for a collection's active branch.
    fn shard_count(&self, collection: &str) -> usize {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();
        let active = storage.get_active_branch(collection);
        pond_storage::shard::shard_count(kernel, collection, &active)
    }

    /// Compact shards — merge all shards into HEAD and clear the shard list.
    ///
    /// After compaction, all shard data is absorbed into HEAD (a new commit),
    /// and the shard refs are deleted. This reclaims storage space and
    /// simplifies future reads (no shard merge needed).
    ///
    /// Returns: number of shards compacted
    fn compact_shards(&self, collection: &str) -> PyResult<usize> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();
        let active = storage.get_active_branch(collection);
        pond_storage::shard::clear_shards(kernel, collection, &active)
            .map_err(pyo3::exceptions::PyIOError::new_err)
    }

    // ===================================================================
    // Atomic Publication (Transactions)
    //
    // This is NOT full ACID. It provides ATOMIC VISIBILITY:
    //   - begin_tx() generates a transaction ID
    //   - Writers attach tx_id to their shards (shards are tentative)
    //   - commit_tx() writes a commit marker → all tentative shards
    //     become visible atomically
    //   - abort_tx() is a no-op (tentative shards are orphaned until GC)
    //
    // There is NO isolation, NO rollback, NO conflict detection.
    // See docs/HONEST_COMPETITOR_COMPARISON.md §3.
    // ===================================================================

    /// Begin a transaction. Returns a transaction ID.
    ///
    /// The tx_id is used to tag tentative writes. Until commit_tx() is
    /// called, the writes are invisible to readers. Once committed, all
    /// tagged writes become visible atomically.
    fn begin_tx(&self) -> String {
        pond_storage::transaction::begin_tx()
    }

    /// Commit a transaction. Writes a commit marker at transactions/{tx_id}.
    ///
    /// Once the marker exists, all tentative shards (tagged with tx_id)
    /// become visible to readers. This is ATOMIC PUBLICATION —
    /// all-or-nothing visibility.
    ///
    /// NOT full ACID: no isolation, no rollback, no conflict detection.
    fn commit_tx(&self, tx_id: &str, message: &str) -> PyResult<String> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();
        pond_storage::transaction::commit_tx(kernel, tx_id, message)
            .map_err(pyo3::exceptions::PyIOError::new_err)
    }

    /// Abort a transaction. Currently a NO-OP.
    ///
    /// Tentative shards are orphaned until GC cleans them up. There is
    /// no real rollback — the shards remain on storage but are invisible
    /// to readers (because the commit marker doesn't exist).
    fn abort_tx(&self, tx_id: &str) -> PyResult<String> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();
        pond_storage::transaction::abort_tx(kernel, tx_id).map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
    }

    /// Check if a transaction has been committed.
    fn is_tx_committed(&self, tx_id: &str) -> bool {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();
        pond_storage::transaction::is_tx_committed(kernel, tx_id)
    }

    /// Check if a transaction has been aborted.
    fn is_tx_aborted(&self, tx_id: &str) -> bool {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();
        pond_storage::transaction::is_tx_aborted(kernel, tx_id)
    }

    /// Get transaction status: "committed", "aborted", or "pending".
    fn tx_status(&self, tx_id: &str) -> String {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();
        pond_storage::transaction::tx_status(kernel, tx_id).to_string()
    }

    /// Read data at a specific commit (snapshot isolation).
    ///
    /// Reads ONLY the manifest at the given commit, ignoring any shards
    /// written after that commit. Provides a consistent snapshot for
    /// long-running analytical queries.
    fn read_at_snapshot(&self, commit_hash: &str) -> PyResult<Vec<u8>> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();
        pond_storage::read::read_at_snapshot(kernel, commit_hash)
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
    }

    // ===================================================================
    // Optimize — compact shards + flatten delta manifests
    //
    // Delta/Iceberg-style optimize: merges small files into larger ones
    // for better read performance. Does TWO things:
    //   1. compact_shards: merge all shards into HEAD (clears shard list)
    //   2. (future) compact_manifest: flatten delta-manifest chains
    //
    // Currently only shard compaction is implemented in the Rust core.
    // Manifest flattening is a Python SDK feature pending port.
    // ===================================================================

    /// Optimize storage — compact shards + flatten delta manifests.
    ///
    /// Args:
    ///   - collection: if None, optimize ALL collections. If specified,
    ///     optimize only that collection.
    ///
    /// Returns: dict with collections_optimized, shards_compacted
    #[pyo3(signature = (collection=None))]
    fn optimize(&self, py: Python<'_>, collection: Option<&str>) -> PyResult<PyObject> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();

        // Determine which collections to optimize
        let collections: Vec<String> = if let Some(c) = collection {
            vec![c.to_string()]
        } else {
            // List all collections by scanning refs
            kernel.list_names_prefix("collections/")
                .into_iter()
                .filter_map(|n| {
                    // Extract collection name from "collections/{name}/_branches/..."
                    n.strip_prefix("collections/")?
                        .split('/').next()
                        .map(|s| s.to_string())
                })
                .collect::<std::collections::HashSet<_>>()
                .into_iter()
                .collect()
        };

        let mut shards_compacted = 0usize;
        let mut optimized = 0usize;

        for coll in &collections {
            let active = storage.get_active_branch(coll);
            let shard_n = pond_storage::shard::shard_count(kernel, coll, &active);
            if shard_n > 0 {
                match pond_storage::shard::clear_shards(kernel, coll, &active) {
                    Ok(n) => shards_compacted += n,
                    Err(_) => continue,
                }
            }
            optimized += 1;
        }

        let dict = PyDict::new_bound(py);
        dict.set_item("collections_optimized", optimized)?;
        dict.set_item("shards_compacted", shards_compacted)?;
        dict.set_item("manifests_flattened", 0)?; // pending port from Python
        Ok(dict.into())
    }

    // ===================================================================
    // Media upload/download — ergonomic unstructured data in structured tables
    //
    // upload() combines write() (store bytes) + write_rows() (store metadata)
    // into one call. download() lazy-loads bytes by querying the table.
    //
    // Inspired by Pixeltable's media reference pattern, but simpler:
    //   s.upload('media', 'video.mp4', video_bytes, duration=120.5)
    //   data = s.download('media', where="name = 'video.mp4'")
    // ===================================================================

    /// Upload a file into a structured table with metadata.
    ///
    /// Combines write() (store bytes as content-addressed blob) + upsert_shard()
    /// (store metadata row with blob_hash reference) in one call.
    /// Auto-extracts: name, size, blob_hash. Extra metadata via kwargs.
    ///
    /// Args:
    ///   - collection: target collection name
    ///   - name: file name (e.g., 'video.mp4')
    ///   - data: file content as bytes
    ///   - mime_type: optional MIME type (auto-detected from extension if omitted)
    ///   - **metadata: any additional columns to store (id, duration, tags, etc.)
    ///
    /// Returns: the row that was inserted (as a dict)
    ///
    /// Example:
    ///   s.upload('media', 'video.mp4', video_bytes, mime_type='video/mp4', duration=120.5)
    ///   s.upload('media', 'photo.jpg', photo_bytes, album='vacation')
    #[pyo3(signature = (collection, name, data, mime_type=None, **kwargs))]
    fn upload(&self, py: Python<'_>, collection: &str, name: &str, data: &[u8], mime_type: Option<&str>, kwargs: Option<PyObject>) -> PyResult<PyObject> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();
        let active = storage.get_active_branch(collection);

        // Step 1: Store the binary blob as a content-addressed blob (NOT a commit)
        // This stores it in the kernel's object store without creating a HEAD commit
        let blob_hash = kernel.write(data)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;

        // Also store as a named blob for easy retrieval
        let blob_ref = format!("files/{}", name);
        kernel.reference(&blob_ref, &blob_hash)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;

        // Step 2: Auto-detect MIME type from extension
        let mime = mime_type.unwrap_or_else(|| guess_mime(name));

        // Step 3: Build the metadata row
        let mut row = serde_json::Map::new();
        row.insert("name".to_string(), json!(name));
        row.insert("mime_type".to_string(), json!(mime));
        row.insert("blob_hash".to_string(), json!(blob_hash));
        row.insert("size".to_string(), json!(data.len()));
        row.insert("_blob_ref".to_string(), json!(blob_ref));

        // Add any extra metadata from kwargs
        if let Some(ref kw) = kwargs {
            let kw_json = python_to_json(kw);
            if let Some(obj) = kw_json.as_object() {
                for (k, v) in obj {
                    row.insert(k.clone(), v.clone());
                }
            }
        }

        let row_json = JsonValue::Object(row);

        // Step 4: Upsert the row (CRDT shard — concurrent-safe)
        use pond_kernel::crdt::HLC;
        let mut hlc = HLC::new();

        // Observe existing versions
        let kc = vec!["name".to_string()];
        let existing = read_collection_as_json_rows(&storage, collection, &kc)
            .map_err(pyo3::exceptions::PyIOError::new_err)?;
        for (_, r) in &existing {
            if let Some(v) = r.get("_version").and_then(|v| v.as_str()) {
                hlc.observe(v);
            }
        }

        let shard_name = format!("upload_{}", chrono_like_id());
        pond_storage::shard::upsert_shard(
            kernel, collection, &active, &shard_name,
            std::slice::from_ref(&row_json), Some("name"), &mut hlc,
        ).map_err(pyo3::exceptions::PyIOError::new_err)?;

        // Return the row as a Python dict
        Ok(json_to_pyobject(py, &row_json))
    }

    /// Download a file's bytes from a structured table.
    ///
    /// Queries the table by name (or WHERE clause), finds the blob_hash,
    /// and lazy-loads the actual bytes.
    ///
    /// Args:
    ///   - collection: collection name
    ///   - name: file name (default lookup), OR use where= for custom query
    ///   - where: SQL WHERE to find the row (e.g., "id = 1")
    ///
    /// Returns: file content as bytes, or None if not found
    ///
    /// Example:
    ///   data = s.download('media', 'video.mp4')
    ///   data = s.download('media', where="id = 1")
    #[pyo3(signature = (collection, name=None, r#where=None))]
    fn download(&self, py: Python<'_>, collection: &str, name: Option<&str>, r#where: Option<PyObject>) -> PyResult<PyObject> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();

        // Build the WHERE clause
        let where_str = if let Some(ref w) = r#where {
            // Use the provided WHERE clause
            match w.extract::<String>(py) {
                Ok(s) => s,
                Err(_) => return Ok(py.None()),
            }
        } else if let Some(n) = name {
            // Default: look up by name
            format!("name = '{}'", n.replace('\'', "\\'"))
        } else {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "Must provide either name= or where= to download()"
            ));
        };

        let where_expr = parse_where(&where_str)
            .map_err(pyo3::exceptions::PyValueError::new_err)?;

        // Read all rows and find the matching one
        let kc = vec!["_rowid".to_string()];
        let all_rows = read_collection_as_json_rows(&storage, collection, &kc)
            .map_err(pyo3::exceptions::PyIOError::new_err)?;
        let merged = crdt_merge_rows(all_rows);

        // Find the matching row
        for row in &merged {
            if where_expr.eval(row) {
                // Get the blob reference
                let blob_ref = row.get("_blob_ref").and_then(|v| v.as_str())
                    .or_else(|| row.get("blob_hash").and_then(|v| v.as_str()));

                if let Some(ref_str) = blob_ref {
                    // Try to resolve as a name first, then as a hash
                    let hash = kernel.resolve(ref_str)
                        .unwrap_or_else(|| ref_str.to_string());
                    match kernel.read_blob(&hash) {
                        Ok(data) => return Ok(PyBytes::new_bound(py, &data).into()),
                        Err(_) => {
                            // Try reading directly as hash
                            match kernel.read_blob(ref_str) {
                                Ok(data) => return Ok(PyBytes::new_bound(py, &data).into()),
                                Err(e) => return Err(pyo3::exceptions::PyIOError::new_err(
                                    format!("Failed to read blob: {}", e))),
                            }
                        }
                    }
                }
            }
        }

        // Not found
        Ok(py.None())
    }

    // ===================================================================
    // Semantic layers — cross-collection, handle-based API
    //
    // WHY "layer" (not "model"): the word "model" collides with ML models,
    // which Pond may host in the future. "Semantic Layer" is the industry-
    // standard term (dbt Semantic Layer, Cube Semantic Layer, Looker LookML).
    // ===================================================================

    /// Get a semantic layer handle (creates the layer if it doesn't exist).
    ///
    /// Returns a SemanticLayer object that groups all semantic operations:
    ///   m = s.layer('sales')
    ///   m.add_datasets(['orders', 'users'])
    ///   m.add_metrics({'revenue': 'SUM(orders.amount)'})
    ///   m.info()
    ///   m.export()
    ///
    /// Multiple adapters: pass a list to `adapters`. A layer can be exposed
    /// via Ossie + Cube + dbt simultaneously. Adapters can also be added /
    /// removed later via `m.add_adapter(name)` / `m.remove_adapter(name)`.
    #[pyo3(signature = (name, adapters=None, enable_reflection=false))]
    fn layer(&self, name: &str, adapters: Option<Vec<String>>, enable_reflection: bool) -> PyResult<SemanticLayer> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();

        // Create layer metadata if it doesn't exist
        let layer_ref = format!("semantic_layers/{}/_meta", name);
        if kernel.resolve(&layer_ref).is_none() {
            // Default to ['ossie'] if no adapters specified
            let adapter_list = adapters.unwrap_or_else(|| vec!["ossie".to_string()]);
            let layer_meta = serde_json::json!({
                "name": name,
                "adapters": adapter_list,
                "enable_reflection": enable_reflection,
            });
            let meta_bytes = serde_json::to_vec(&layer_meta)
                .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
            let hash = kernel.write(&meta_bytes)
                .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
            kernel.reference(&layer_ref, &hash)
                .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        }

        Ok(SemanticLayer {
            storage: self.storage.clone(),
            name: name.to_string(),
        })
    }

    /// List all semantic layers.
    fn layers(&self) -> Vec<String> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();
        let prefix = "semantic_layers/";
        let mut seen = std::collections::HashSet::new();
        kernel.list_names_prefix(prefix).into_iter()
            .filter_map(|n| {
                let rest = n.strip_prefix("semantic_layers/")?;
                let layer_name = rest.split('/').next()?;
                if layer_name == "_meta" { return None; }
                if seen.contains(layer_name) { return None; }
                seen.insert(layer_name.to_string());
                Some(layer_name.to_string())
            })
            .collect()
    }

    // ===================================================================
    // UDF Pushdown — register Python functions for SQL WHERE filtering
    //
    // UDFs allow users to push arbitrary Python predicates into the SQL
    // WHERE clause:
    //
    //   storage.register_udf("is_adult", lambda age: age >= 18)
    //   storage.sql("SELECT * FROM users WHERE is_adult(age)")
    //
    // The UDF is called once per row with the column value(s) as args.
    // Rows where the UDF returns a truthy value are included in the result.
    // ===================================================================

    /// Register a user-defined function (UDF) for SQL WHERE pushdown.
    ///
    /// The UDF is a Python callable that takes one or more column values
    /// as arguments and returns a boolean (or truthy value).
    ///
    /// Args:
    ///   - name: The function name (used in SQL WHERE, e.g. `name(col)`)
    ///   - func: The Python callable
    ///
    /// Example:
    ///   s.register_udf("is_adult", lambda age: age >= 18)
    ///   s.sql("SELECT * FROM users WHERE is_adult(age)")
    fn register_udf(&self, name: &str, func: PyObject) -> PyResult<()> {
        let mut udfs = self.udfs.lock().unwrap();
        udfs.insert(name.to_string(), func);
        Ok(())
    }

    /// Unregister a previously-registered UDF by name.
    ///
    /// Returns True if the UDF was found and removed, False otherwise.
    fn unregister_udf(&self, name: &str) -> bool {
        let mut udfs = self.udfs.lock().unwrap();
        udfs.remove(name).is_some()
    }

    /// List all registered UDF names.
    fn list_udfs(&self) -> Vec<String> {
        let udfs = self.udfs.lock().unwrap();
        let mut names: Vec<String> = udfs.keys().cloned().collect();
        names.sort();
        names
    }

    // ===================================================================
    // Row-Level Security (RLS) — multi-tenant isolation
    //
    // RLS policies ensure that each tenant only sees their own data.
    // When a policy is set on a collection:
    //   - write_rows auto-adds a `_tenant` column with the tenant_id
    //   - read_rows filters out rows where `_tenant` doesn't match
    //
    // This is transparent to the user — they write and read normally,
    // and RLS is enforced automatically.
    // ===================================================================

    /// Set a Row-Level Security policy for a collection.
    ///
    /// After calling this:
    ///   - write_rows() auto-adds `_tenant=<tenant_id>` to every row
    ///   - read_rows() only returns rows where `_tenant` matches
    ///
    /// Args:
    ///   - collection: The collection name
    ///   - tenant_id: The tenant identifier (e.g. "tenant_123")
    ///
    /// Example:
    ///   s.set_rls_policy("users", "tenant_123")
    ///   s.write_rows("users", [("name", ["alice", "bob"])], "init")
    ///   # → rows are stored with _tenant="tenant_123"
    ///   s.read_rows("users")
    ///   # → only returns rows where _tenant="tenant_123"
    fn set_rls_policy(&self, collection: &str, tenant_id: &str) -> PyResult<()> {
        let mut policies = self.rls_policies.lock().unwrap();
        policies.insert(collection.to_string(), tenant_id.to_string());
        Ok(())
    }

    /// Get the RLS tenant_id for a collection, if a policy is set.
    ///
    /// Returns None if no RLS policy is active for this collection.
    fn get_rls_policy(&self, collection: &str) -> Option<String> {
        let policies = self.rls_policies.lock().unwrap();
        policies.get(collection).cloned()
    }

    /// Clear the RLS policy for a collection.
    ///
    /// After clearing, read_rows() returns all rows (regardless of
    /// _tenant) and write_rows() no longer auto-adds _tenant.
    ///
    /// Returns True if a policy was cleared, False if none was set.
    fn clear_rls_policy(&self, collection: &str) -> bool {
        let mut policies = self.rls_policies.lock().unwrap();
        policies.remove(collection).is_some()
    }

    // ===================================================================
    // VECTOR SEARCH — SIMD-accelerated k-NN over stored vectors
    //
    // Reads all rows, extracts the vector column (JSON array of floats),
    // and calls pond_core::vector::search_vectors() which uses AVX2/NEON
    // for the distance computation. Supports L2, cosine, and dot metrics.
    // ===================================================================

    /// Search stored vectors for the k closest to the query vector.
    ///
    /// Reads all rows from the collection (HEAD + shards, CRDT-merged),
    /// extracts the vector from `vector_column` (expected to be a JSON
    /// array of numbers), and uses SIMD-accelerated distance functions
    /// to find the k nearest neighbors.
    ///
    /// Args:
    ///   - collection: Collection name
    ///   - vector_column: Column containing the vector (JSON array of floats)
    ///   - query: Query vector (list of floats)
    ///   - metric: "l2" (Euclidean), "cosine", or "dot"
    ///   - k: Number of nearest neighbors to return
    ///   - where_clause: Optional SQL WHERE filter (e.g., "category = 'sports'")
    ///
    /// Returns:
    ///   List of (row_dict, distance) tuples sorted by distance ascending.
    ///
    /// Example:
    ///   results = s.search_vectors('embeddings', 'vec', [0.1, 0.2, ...],
    ///                              metric='cosine', k=10)
    ///   for row, dist in results:
    ///       print(dist, row['name'])
    #[pyo3(signature = (collection, vector_column, query, metric, k, where_clause=None))]
    fn search_vectors(
        &self,
        py: Python<'_>,
        collection: &str,
        vector_column: &str,
        query: Vec<f32>,
        metric: &str,
        k: usize,
        where_clause: Option<&str>,
    ) -> PyResult<PyObject> {
        let storage = self.storage.lock().unwrap();
        let kc: Vec<String> = vec!["_rowid".to_string()];
        let all_rows = read_collection_as_json_rows(&storage, collection, &kc)
            .map_err(pyo3::exceptions::PyIOError::new_err)?;
        let merged = crdt_merge_rows(all_rows);

        // Optional WHERE filter (SQL string)
        let where_expr: WhereExpr = match where_clause {
            Some(s) if !s.trim().is_empty() => parse_where(s)
                .map_err(pyo3::exceptions::PyValueError::new_err)?,
            _ => WhereExpr::True,
        };

        // Build (row, vector) pairs, filtering by WHERE and extracting vectors.
        // Vectors may be stored as a JSON array (Variant column) or as a
        // JSON-encoded string (String column from a VARIANT round-trip).
        let mut rows_with_vectors: Vec<(JsonValue, Vec<f32>)> = Vec::new();
        for row in merged {
            if !where_expr.eval(&row) { continue; }
            let vec_val = match row.get(vector_column) {
                Some(v) => v,
                None => continue,
            };
            let vector: Vec<f32> = match vec_val {
                JsonValue::Array(arr) => arr.iter().filter_map(|x| {
                    x.as_f64().map(|f| f as f32)
                        .or_else(|| x.as_i64().map(|i| i as f32))
                }).collect(),
                JsonValue::String(s) => {
                    // Variant columns store vectors as JSON-encoded strings
                    serde_json::from_str::<Vec<JsonValue>>(s).ok()
                        .map(|arr| arr.iter().filter_map(|x| {
                            x.as_f64().map(|f| f as f32)
                                .or_else(|| x.as_i64().map(|i| i as f32))
                        }).collect())
                        .unwrap_or_default()
                }
                _ => continue,
            };
            if vector.is_empty() { continue; }
            rows_with_vectors.push((row, vector));
        }

        // Run the SIMD-accelerated search
        let stored: Vec<Vec<f32>> = rows_with_vectors.iter()
            .map(|(_, v)| v.clone())
            .collect();
        let results = pond_core::vector::search_vectors(&query, &stored, metric, k);

        // Build Python result: [(row_dict, distance), ...] sorted by distance
        let list = PyList::new_bound(py, results.iter().map(|(idx, dist)| {
            let row_dict = json_value_to_py(py, &rows_with_vectors[*idx].0);
            let dist_obj = (*dist).to_object(py);
            PyTuple::new_bound(py, [row_dict, dist_obj]).into_any()
        }));
        Ok(list.into())
    }

    /// Hybrid search combining vector similarity, BM25 text scoring, and
    /// metadata filtering via weighted Reciprocal Rank Fusion (RRF).
    ///
    /// Combines three search signals:
    ///   1. Vector: SIMD-accelerated L2/cosine/dot distance
    ///   2. Text: BM25 scoring over specified text columns
    ///   3. Filter: metadata WHERE clause (exact match boost)
    ///
    /// Results are fused using weighted RRF (k=60) and returned sorted by
    /// fused score descending.
    ///
    /// # Arguments
    ///   - collection: the collection to search
    ///   - vector_column: name of the VECTOR column (None = skip vector search)
    ///   - query_vector: the query embedding
    ///   - text_columns: columns to search with BM25 (None = skip text search)
    ///   - query_text: the text query
    ///   - where_clause: SQL WHERE filter (None = skip filter boost)
    ///   - metric: "l2", "cosine", or "dot"
    ///   - k: number of results to return
    ///   - vector_weight: weight for vector signal (default 1.0)
    ///   - text_weight: weight for text signal (default 1.0)
    #[pyo3(signature = (
        collection,
        vector_column=None,
        query_vector=None,
        text_columns=None,
        query_text=None,
        where_clause=None,
        metric="l2",
        k=10,
        vector_weight=1.0,
        text_weight=1.0,
    ))]
    fn hybrid_search(
        &self,
        py: Python<'_>,
        collection: &str,
        vector_column: Option<&str>,
        query_vector: Option<Vec<f32>>,
        text_columns: Option<Vec<String>>,
        query_text: Option<&str>,
        where_clause: Option<&str>,
        metric: &str,
        k: usize,
        vector_weight: f64,
        text_weight: f64,
    ) -> PyResult<PyObject> {
        use pond_core::search::{self, SearchWeights};

        // Read all rows
        let rows_py = self.read_rows(py, collection, None, None)?;
        let rows_json = python_to_json(&rows_py);
        let rows: Vec<JsonValue> = rows_json.as_array()
            .cloned()
            .unwrap_or_default();

        if rows.is_empty() {
            return Ok(PyList::empty_bound(py).into());
        }

        // Extract parameters
        let vc = vector_column.unwrap_or("");
        let qv: &[f32] = query_vector.as_deref().unwrap_or(&[]);
        let tc_owned: Vec<String> = text_columns.unwrap_or_default();
        let tc: Vec<&str> = tc_owned.iter().map(|s| s.as_str()).collect();
        let qt = query_text.unwrap_or("");

        // Parse WHERE clause if present
        let where_closure: Option<Box<dyn Fn(&JsonValue) -> bool>> = if let Some(w) = where_clause.filter(|s| !s.is_empty()) {
            match parse_where(w) {
                Ok(expr) => Some(Box::new(move |row: &JsonValue| expr.eval(row))),
                Err(_) => None,
            }
        } else {
            None
        };

        // Execute hybrid search
        let hits = search::hybrid_search(
            &rows,
            vc,
            qv,
            &tc,
            qt,
            where_closure.as_ref().map(|f| f as &dyn Fn(&JsonValue) -> bool),
            SearchWeights { vector: vector_weight, text: text_weight },
            k,
            metric,
        );

        // Build result list of (row_dict, score, vector_distance, text_score) tuples
        let result_list = PyList::new_bound(py, hits.iter().map(|hit| {
            let row_dict = json_to_pyobject(py, &hit.row);
            let score = hit.score.to_object(py);
            let vdist = hit.vector_distance.map(|d| d.to_object(py)).unwrap_or_else(|| py.None());
            let tscore = hit.text_score.map(|s| s.to_object(py)).unwrap_or_else(|| py.None());
            PyTuple::new_bound(py, [row_dict, score, vdist, tscore]).into_any()
        }));

        Ok(result_list.into())
    }

    // ===================================================================
    // STREAMING READS — batched iterator over rows
    //
    // Returns a RowBatchStream that yields List[Dict[str, Any]] batches.
    // Useful for large collections where materializing all rows at once
    // would blow memory — the consumer can process batch-by-batch.
    // ===================================================================

    /// Stream rows from a collection in batches.
    ///
    /// Reads HEAD + all shards (CRDT merge), then yields rows in batches
    /// of `batch_size`. Each batch is a `List[Dict[str, Any]]`.
    ///
    /// Args:
    ///   - collection: Collection name
    ///   - batch_size: Number of rows per batch (last batch may be smaller)
    ///
    /// Returns:
    ///   A `RowBatchStream` iterator. Use `for batch in s.read_rows_stream(...)`
    ///   to iterate.
    ///
    /// Example:
    ///   for batch in s.read_rows_stream('events', 1000):
    ///       for row in batch:
    ///           process(row)
    fn read_rows_stream(&self, collection: &str, batch_size: usize) -> PyResult<RowBatchStream> {
        if batch_size == 0 {
            return Err(pyo3::exceptions::PyValueError::new_err("batch_size must be > 0"));
        }
        let storage = self.storage.lock().unwrap();
        let kc: Vec<String> = vec!["_rowid".to_string()];
        let all_rows = read_collection_as_json_rows(&storage, collection, &kc)
            .map_err(pyo3::exceptions::PyIOError::new_err)?;
        let merged = crdt_merge_rows(all_rows);

        // Strip CRDT metadata columns (matches read_rows behavior — these
        // are internal unless explicitly requested).
        let crdt_cols: std::collections::HashSet<&str> = ["_rowid", "_version", "_deleted", "_tenant"]
            .iter().cloned().collect();

        let mut batches: Vec<Vec<JsonValue>> = Vec::new();
        let mut chunk: Vec<JsonValue> = Vec::with_capacity(batch_size);
        for row in merged {
            let filtered = if let Some(obj) = row.as_object() {
                let mut new_obj = serde_json::Map::new();
                for (k, v) in obj {
                    if !crdt_cols.contains(k.as_str()) {
                        new_obj.insert(k.clone(), v.clone());
                    }
                }
                JsonValue::Object(new_obj)
            } else {
                row
            };
            chunk.push(filtered);
            if chunk.len() >= batch_size {
                batches.push(std::mem::replace(&mut chunk, Vec::with_capacity(batch_size)));
            }
        }
        if !chunk.is_empty() {
            batches.push(chunk);
        }

        Ok(RowBatchStream {
            batches: Mutex::new(batches.into_iter().collect()),
        })
    }
}

// ===========================================================================
// RowBatchStream — Python iterator over row batches
//
// Created by Storage::read_rows_stream(). Each __next__ call returns a
// List[Dict[str, Any]] batch. Raises StopIteration when exhausted.
// ===========================================================================

/// A Python iterator over row batches from a collection.
///
/// Created by `Storage.read_rows_stream(collection, batch_size)`. Each
/// `__next__` call returns a `List[Dict[str, Any]]` of up to `batch_size`
/// rows. Raises `StopIteration` when exhausted.
///
/// Example:
///   for batch in s.read_rows_stream('users', 100):
///       for row in batch:
///           print(row['name'])
#[pyclass]
struct RowBatchStream {
    /// Pre-materialized batches of JSON rows. Held under a Mutex so the
    /// Python iterator protocol (which can be called from any thread under
    /// the GIL) stays sound.
    batches: Mutex<std::collections::VecDeque<Vec<JsonValue>>>,
}

#[pymethods]
impl RowBatchStream {
    /// Return self — iterators are their own iterables.
    fn __iter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    /// Return the next batch of rows, or raise StopIteration.
    fn __next__(&self, py: Python<'_>) -> PyResult<PyObject> {
        let next_batch = self.batches.lock().unwrap().pop_front();
        match next_batch {
            None => Err(pyo3::exceptions::PyStopIteration::new_err(())),
            Some(rows) => {
                let list = PyList::new_bound(py, rows.iter().map(|row| json_value_to_py(py, row)));
                Ok(list.into())
            }
        }
    }
}

// ===========================================================================
// SemanticLayer — handle for cross-collection semantic layer operations
//
// WHY "layer" (not "model"): avoids confusion with ML models. Industry
// standard (dbt Semantic Layer, Cube Semantic Layer, Looker LookML).
// ===========================================================================

use std::sync::Arc;

/// A handle to a semantic layer. All semantic operations go through this handle.
///
/// Get one via: `m = s.layer('sales')`
#[pyclass]
struct SemanticLayer {
    storage: Arc<Mutex<UnifiedStorage>>,
    name: String,
}

#[pymethods]
impl SemanticLayer {
    /// Add multiple datasets (collections) to the layer in one call.
    ///
    /// Args:
    ///   - datasets: List of collection names to add
    fn add_datasets(&self, datasets: Vec<String>) -> PyResult<()> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();
        for ds in &datasets {
            let ds_json = serde_json::json!({"name": ds, "source": ds});
            let ds_bytes = serde_json::to_vec(&ds_json)
                .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
            let hash = kernel.write(&ds_bytes)
                .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
            let ref_name = format!("semantic_layers/{}/datasets/{}", self.name, ds);
            kernel.reference(&ref_name, &hash)
                .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        }
        Ok(())
    }

    /// Add multiple metrics to the model in one call.
    ///
    /// Args:
    ///   - metrics: Dict of {metric_name: expression}
    ///
    /// Example:
    ///   m.add_metrics({'revenue': 'SUM(orders.amount)', 'count': 'COUNT(orders.id)'})
    fn add_metrics(&self, metrics: std::collections::HashMap<String, String>) -> PyResult<()> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();
        for (name, expr) in &metrics {
            let metric = serde_json::json!({
                "name": name,
                "expression": expr,
                "description": "",
                "format": "number",
            });
            let bytes = serde_json::to_vec(&metric)
                .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
            let hash = kernel.write(&bytes)
                .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
            let ref_name = format!("semantic_layers/{}/metrics/{}", self.name, name);
            kernel.reference(&ref_name, &hash)
                .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        }
        Ok(())
    }

    /// Add multiple dimensions to the model in one call.
    ///
    /// Args:
    ///   - dimensions: Dict of {dim_name: (collection, field, data_type)}
    ///
    /// Example:
    ///   m.add_dimensions({
    ///       'country': ('users', 'country', 'string'),
    ///       'order_date': ('orders', 'created_at', 'time'),
    ///   })
    fn add_dimensions(&self, dimensions: std::collections::HashMap<String, (String, String, String)>) -> PyResult<()> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();
        for (name, (dataset, field, data_type)) in &dimensions {
            let dim = serde_json::json!({
                "name": name,
                "dataset": dataset,
                "field": field,
                "data_type": data_type,
            });
            let bytes = serde_json::to_vec(&dim)
                .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
            let hash = kernel.write(&bytes)
                .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
            let ref_name = format!("semantic_layers/{}/dimensions/{}", self.name, name);
            kernel.reference(&ref_name, &hash)
                .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        }
        Ok(())
    }

    /// Add multiple relationships to the model in one call.
    ///
    /// Args:
    ///   - relationships: Dict of {rel_name: (from, to, condition)}
    ///
    /// Example:
    ///   m.add_relationships({
    ///       'user_orders': ('users', 'orders', 'users.id = orders.user_id'),
    ///   })
    fn add_relationships(&self, relationships: std::collections::HashMap<String, (String, String, String)>) -> PyResult<()> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();
        for (name, (from, to, condition)) in &relationships {
            let rel = serde_json::json!({
                "name": name,
                "from": from,
                "to": to,
                "condition": condition,
            });
            let bytes = serde_json::to_vec(&rel)
                .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
            let hash = kernel.write(&bytes)
                .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
            let ref_name = format!("semantic_layers/{}/relationships/{}", self.name, name);
            kernel.reference(&ref_name, &hash)
                .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        }
        Ok(())
    }

    /// Get a full overview of the layer.
    ///
    /// Returns a dict with: name, adapters, datasets, metrics, dimensions,
    /// relationships (each with count + names), reflection_enabled.
    fn info(&self, py: Python<'_>) -> PyResult<PyObject> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();

        // Read layer metadata
        let layer_ref = format!("semantic_layers/{}/_meta", self.name);
        let layer_hash = kernel.resolve(&layer_ref)
            .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err(
                format!("Semantic layer '{}' not found", self.name)))?;
        let layer_data = kernel.read_blob(&layer_hash)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        let layer_meta: serde_json::Value = serde_json::from_slice(&layer_data)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;

        let datasets = self.list_datasets_impl(kernel);
        let metrics = self.list_metrics_impl(kernel);
        let dimensions = self.list_dimensions_impl(kernel);
        let relationships = self.list_relationships_impl(kernel);

        // adapters: a JSON list (with backward compat for the legacy
        // single-string "adapter" field).
        let adapters_val: Vec<String> = if let Some(arr) = layer_meta.get("adapters").and_then(|v| v.as_array()) {
            arr.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect()
        } else if let Some(single) = layer_meta.get("adapter").and_then(|v| v.as_str()) {
            vec![single.to_string()]
        } else {
            vec!["ossie".to_string()]
        };

        let dict = PyDict::new_bound(py);
        dict.set_item("name", layer_meta.get("name").and_then(|v| v.as_str()).unwrap_or(&self.name))?;
        dict.set_item("adapters", adapters_val)?;
        dict.set_item("reflection_enabled", layer_meta.get("enable_reflection").and_then(|v| v.as_bool()).unwrap_or(false))?;
        dict.set_item("datasets", datasets)?;
        dict.set_item("metrics", metrics)?;
        dict.set_item("dimensions", dimensions)?;
        dict.set_item("relationships", relationships)?;
        Ok(dict.into())
    }

    /// List datasets in this layer.
    fn datasets(&self) -> Vec<String> {
        let storage = self.storage.lock().unwrap();
        self.list_datasets_impl(storage.kernel())
    }

    /// List metrics in this layer.
    fn metrics(&self) -> Vec<String> {
        let storage = self.storage.lock().unwrap();
        self.list_metrics_impl(storage.kernel())
    }

    /// List dimensions in this layer.
    fn dimensions(&self) -> Vec<String> {
        let storage = self.storage.lock().unwrap();
        self.list_dimensions_impl(storage.kernel())
    }

    /// List relationships in this layer.
    fn relationships(&self) -> Vec<String> {
        let storage = self.storage.lock().unwrap();
        self.list_relationships_impl(storage.kernel())
    }

    /// List the adapters currently enabled on this layer.
    ///
    /// A layer can be exposed via multiple adapters simultaneously
    /// (e.g., Ossie + Cube + dbt). Use `add_adapter` / `remove_adapter`
    /// to manage them independently of the spec.
    fn adapters(&self) -> PyResult<Vec<String>> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();
        let layer_ref = format!("semantic_layers/{}/_meta", self.name);
        let hash = kernel.resolve(&layer_ref)
            .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err("Layer not found"))?;
        let data = kernel.read_blob(&hash)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        let meta: serde_json::Value = serde_json::from_slice(&data)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        if let Some(arr) = meta.get("adapters").and_then(|v| v.as_array()) {
            Ok(arr.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect())
        } else if let Some(single) = meta.get("adapter").and_then(|v| v.as_str()) {
            Ok(vec![single.to_string()])
        } else {
            Ok(vec!["ossie".to_string()])
        }
    }

    /// Add an adapter to this layer. Idempotent.
    ///
    /// The layer becomes queryable via this adapter's protocol immediately
    /// (auto-exposure — no explicit export step needed).
    fn add_adapter(&self, adapter: String) -> PyResult<()> {
        let mut current = self.adapters()?;
        if !current.contains(&adapter) {
            current.push(adapter);
            self.set_adapters_field(current)?;
        }
        Ok(())
    }

    /// Remove an adapter from this layer. Returns True if it was present.
    fn remove_adapter(&self, adapter: String) -> PyResult<bool> {
        let mut current = self.adapters()?;
        let before = current.len();
        current.retain(|a| a != &adapter);
        let removed = current.len() < before;
        if removed {
            self.set_adapters_field(current)?;
        }
        Ok(removed)
    }

    /// Export the layer in a specific adapter format.
    ///
    /// If `adapter` is None, uses the first adapter in the layer's
    /// `adapters` list (the "default" for the layer).
    ///
    /// Auto-exposure: this method is OPTIONAL. Adapters can read the
    /// layer's spec directly from storage at query time. This method
    /// is for one-shot snapshots (file export, debugging, migration).
    #[pyo3(signature = (adapter=None))]
    fn export(&self, py: Python<'_>, adapter: Option<&str>) -> PyResult<PyObject> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();

        // Determine adapter: explicit > first in adapters list > "ossie"
        let layer_ref = format!("semantic_layers/{}/_meta", self.name);
        let adapter_name = if let Some(a) = adapter {
            a.to_string()
        } else {
            let hash = kernel.resolve(&layer_ref)
                .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err("Layer not found"))?;
            let data = kernel.read_blob(&hash)
                .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
            let meta: serde_json::Value = serde_json::from_slice(&data)
                .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
            if let Some(arr) = meta.get("adapters").and_then(|v| v.as_array()) {
                arr.first().and_then(|v| v.as_str()).unwrap_or("ossie").to_string()
            } else {
                meta.get("adapter").and_then(|v| v.as_str()).unwrap_or("ossie").to_string()
            }
        };

        // Read all definitions
        let mut defs = SemanticDefinitions::new();

        let metric_prefix = format!("semantic_layers/{}/metrics/", self.name);
        for ref_name in kernel.list_names_prefix(&metric_prefix) {
            if let Some(hash) = kernel.resolve(&ref_name) {
                if let Ok(data) = kernel.read_blob(&hash) {
                    if let Ok(m) = serde_json::from_slice::<serde_json::Value>(&data) {
                        defs.metrics.push(pond_semantic::Metric {
                            name: m.get("name").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                            expression: m.get("expression").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                            description: m.get("description").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                            format: m.get("format").and_then(|v| v.as_str()).unwrap_or("number").to_string(),
                        });
                    }
                }
            }
        }

        let dim_prefix = format!("semantic_layers/{}/dimensions/", self.name);
        for ref_name in kernel.list_names_prefix(&dim_prefix) {
            if let Some(hash) = kernel.resolve(&ref_name) {
                if let Ok(data) = kernel.read_blob(&hash) {
                    if let Ok(d) = serde_json::from_slice::<serde_json::Value>(&data) {
                        defs.dimensions.push(pond_semantic::Dimension {
                            name: d.get("name").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                            data_type: d.get("data_type").and_then(|v| v.as_str()).unwrap_or("string").to_string(),
                            description: d.get("field").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                        });
                    }
                }
            }
        }

        let rel_prefix = format!("semantic_layers/{}/relationships/", self.name);
        for ref_name in kernel.list_names_prefix(&rel_prefix) {
            if let Some(hash) = kernel.resolve(&ref_name) {
                if let Ok(data) = kernel.read_blob(&hash) {
                    if let Ok(r) = serde_json::from_slice::<serde_json::Value>(&data) {
                        defs.relationships.push(pond_semantic::Relationship {
                            name: r.get("name").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                            from_collection: r.get("from").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                            to_collection: r.get("to").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                            join_type: "inner".to_string(),
                            join_condition: r.get("condition").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                        });
                    }
                }
            }
        }

        // Export using the adapter
        let layer = match adapter_name.as_str() {
            "ossie" => {
                use pond_semantic::SemanticModelAdapter;
                pond_ossie_adapter::OssieAdapter::new().export_model(&defs)
            }
            _ => return Err(pyo3::exceptions::PyValueError::new_err(
                format!("Unknown adapter: '{}'. Supported: ossie", adapter_name)
            )),
        };

        let layer_str = serde_json::to_string(&layer)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        let json_module = py.import_bound("json")?;
        let result = json_module.call_method("loads", (layer_str,), None)?;
        Ok(result.into())
    }

    /// Enable reflection on this layer.
    fn enable_reflection(&self) -> PyResult<()> {
        self.set_reflection_flag(true)
    }

    /// Disable reflection on this layer.
    fn disable_reflection(&self) -> PyResult<()> {
        self.set_reflection_flag(false)
    }
}

impl SemanticLayer {
    fn list_datasets_impl(&self, kernel: &PondKernel) -> Vec<String> {
        let prefix = format!("semantic_layers/{}/datasets/", self.name);
        kernel.list_names_prefix(&prefix).into_iter()
            .filter_map(|n| n.strip_prefix(&prefix).map(|s| s.to_string()))
            .collect()
    }

    fn list_metrics_impl(&self, kernel: &PondKernel) -> Vec<String> {
        let prefix = format!("semantic_layers/{}/metrics/", self.name);
        kernel.list_names_prefix(&prefix).into_iter()
            .filter_map(|n| n.strip_prefix(&prefix).map(|s| s.to_string()))
            .collect()
    }

    fn list_dimensions_impl(&self, kernel: &PondKernel) -> Vec<String> {
        let prefix = format!("semantic_layers/{}/dimensions/", self.name);
        kernel.list_names_prefix(&prefix).into_iter()
            .filter_map(|n| n.strip_prefix(&prefix).map(|s| s.to_string()))
            .collect()
    }

    fn list_relationships_impl(&self, kernel: &PondKernel) -> Vec<String> {
        let prefix = format!("semantic_layers/{}/relationships/", self.name);
        kernel.list_names_prefix(&prefix).into_iter()
            .filter_map(|n| n.strip_prefix(&prefix).map(|s| s.to_string()))
            .collect()
    }

    fn set_reflection_flag(&self, enabled: bool) -> PyResult<()> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();

        // Read existing meta
        let layer_ref = format!("semantic_layers/{}/_meta", self.name);
        let hash = kernel.resolve(&layer_ref)
            .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err("Layer not found"))?;
        let data = kernel.read_blob(&hash)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        let mut meta: serde_json::Value = serde_json::from_slice(&data)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;

        // Update flag
        if let Some(obj) = meta.as_object_mut() {
            obj.insert("enable_reflection".to_string(), serde_json::json!(enabled));
        }

        // Write back
        let new_bytes = serde_json::to_vec(&meta)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        let new_hash = kernel.write(&new_bytes)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        kernel.reference(&layer_ref, &new_hash)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        Ok(())
    }

    /// Update the `adapters` list in the layer's _meta. Migrates the
    /// legacy single-string `adapter` field to the new `adapters` list
    /// if present.
    fn set_adapters_field(&self, adapters: Vec<String>) -> PyResult<()> {
        let storage = self.storage.lock().unwrap();
        let kernel = storage.kernel();

        let layer_ref = format!("semantic_layers/{}/_meta", self.name);
        let hash = kernel.resolve(&layer_ref)
            .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err("Layer not found"))?;
        let data = kernel.read_blob(&hash)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        let mut meta: serde_json::Value = serde_json::from_slice(&data)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;

        if let Some(obj) = meta.as_object_mut() {
            // Migrate: remove legacy single-string `adapter`, set `adapters` list
            obj.remove("adapter");
            obj.insert("adapters".to_string(), serde_json::json!(adapters));
        }

        let new_bytes = serde_json::to_vec(&meta)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        let new_hash = kernel.write(&new_bytes)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        kernel.reference(&layer_ref, &new_hash)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        Ok(())
    }
}

/// Check if a row group can be pruned based on column stats + predicate.
/// Returns true if the row group CANNOT match the predicate (should be skipped).
fn can_prune_row_group_py(
    stats: &pond_storage::manifest::ColumnStatsEntry,
    op: &str,
    value: i64,
) -> bool {
    let (min, max) = match (&stats.min, &stats.max) {
        (Some(m), Some(x)) if m.len() >= 8 && x.len() >= 8 => {
            let min_val = i64::from_le_bytes([
                m[0], m[1], m[2], m[3], m[4], m[5], m[6], m[7]
            ]);
            let max_val = i64::from_le_bytes([
                x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7]
            ]);
            (min_val, max_val)
        }
        _ => return false,
    };

    match op {
        "=" | "==" => value < min || value > max,
        "<" => min >= value,
        "<=" => min > value,
        ">" => max <= value,
        ">=" => max < value,
        "!=" | "<>" => false,
        _ => false,
    }
}

/// Convert a Python object to a serde_json::Value.
fn python_to_json(obj: &PyObject) -> JsonValue {
    Python::with_gil(|py| {
        // Try bytes FIRST (before String — bytes don't extract as String)
        if let Ok(b) = obj.extract::<&[u8]>(py) {
            // Binary data encoded as base64 string with prefix
            return JsonValue::String(format!("__bin_b64__:{}", base64_encode(b)));
        }
        if let Ok(s) = obj.extract::<String>(py) {
            JsonValue::String(s)
        } else if let Ok(i) = obj.extract::<i64>(py) {
            JsonValue::Number(serde_json::Number::from(i))
        } else if let Ok(f) = obj.extract::<f64>(py) {
            serde_json::Number::from_f64(f).map(JsonValue::Number).unwrap_or(JsonValue::Null)
        } else if let Ok(b) = obj.extract::<bool>(py) {
            JsonValue::Bool(b)
        } else if let Ok(dict) = obj.extract::<std::collections::HashMap<String, PyObject>>(py) {
            let map: serde_json::Map<String, JsonValue> = dict.into_iter()
                .map(|(k, v)| (k, python_to_json(&v)))
                .collect();
            JsonValue::Object(map)
        } else if let Ok(list) = obj.extract::<Vec<PyObject>>(py) {
            JsonValue::Array(list.into_iter().map(|item| python_to_json(&item)).collect())
        } else {
            JsonValue::Null
        }
    })
}

/// Convert a Vec<PyObject> (Python list of values) to a TypedColumn.
///
/// Auto-detects the type from the values:
///   - All ints → TypedColumn::Int64
///   - All floats → TypedColumn::Float64
///   - All strings → TypedColumn::String
///   - All bytes → TypedColumn::Binary
///   - Mixed types (int + float + string + bool + None + dict + list) → TypedColumn::Variant
///   - Empty → TypedColumn::Int64 (default)
fn python_values_to_typed_column(values: &[PyObject]) -> TypedColumn {
    Python::with_gil(|py| {
        if values.is_empty() {
            return TypedColumn::Int64(Vec::new());
        }

        // Classify each value's type
        #[derive(PartialEq, Clone, Copy)]
        enum PyType { Int, Float, Str, Bytes, Bool, None, Other }

        let types: Vec<PyType> = values.iter().map(|v| {
            if v.is_none(py) { PyType::None }
            else if let Ok(true) = v.extract::<bool>(py) { PyType::Bool }
            else if v.extract::<i64>(py).is_ok() { PyType::Int }
            else if v.extract::<f64>(py).is_ok() { PyType::Float }
            else if v.extract::<&[u8]>(py).is_ok() { PyType::Bytes }
            else if v.extract::<String>(py).is_ok() { PyType::Str }
            else { PyType::Other }
        }).collect();

        // Check if all values are the same type (excluding None)
        let non_none_types: Vec<PyType> = types.iter()
            .filter(|t| **t != PyType::None)
            .cloned()
            .collect();

        if non_none_types.is_empty() {
            // All None → Int64 with zeros (will be treated as null)
            return TypedColumn::Int64(vec![0; values.len()]);
        }

        let first_type = non_none_types[0];
        let all_same = non_none_types.iter().all(|t| *t == first_type);

        if all_same {
            // Homogeneous column — use the specific type
            match first_type {
                PyType::Int => {
                    let vals: Vec<i64> = values.iter()
                        .map(|v| v.extract::<i64>(py).unwrap_or(0))
                        .collect();
                    return TypedColumn::Int64(vals);
                }
                PyType::Float => {
                    let vals: Vec<f64> = values.iter()
                        .map(|v| v.extract::<f64>(py).unwrap_or(0.0))
                        .collect();
                    return TypedColumn::Float64(vals);
                }
                PyType::Str => {
                    let vals: Vec<String> = values.iter()
                        .map(|v| v.extract::<String>(py).unwrap_or_default())
                        .collect();
                    return TypedColumn::String(vals);
                }
                PyType::Bytes => {
                    let vals: Vec<Vec<u8>> = values.iter()
                        .map(|v| v.extract::<&[u8]>(py).unwrap_or(&[]).to_vec())
                        .collect();
                    return TypedColumn::Binary(vals);
                }
                _ => {} // Fall through to Variant
            }
        }

        // Mixed types or contains dicts/lists/bools → Variant column
        // Each value is stored as a JSON-encoded string
        let vals: Vec<String> = values.iter()
            .map(|v| {
                let jv = python_to_json(v);
                jv.to_string()
            })
            .collect();
        TypedColumn::Variant(vals)
    })
}

/// Extract a single cell from a TypedColumn at a given row index as a JSON value.
fn extract_cell(col: &TypedColumn, idx: usize) -> JsonValue {
    match col {
        TypedColumn::Int64(v) => v.get(idx).map(|i| JsonValue::Number(serde_json::Number::from(*i))).unwrap_or(JsonValue::Null),
        TypedColumn::Float64(v) => v.get(idx).and_then(|f| serde_json::Number::from_f64(*f)).map(JsonValue::Number).unwrap_or(JsonValue::Null),
        TypedColumn::String(v) => v.get(idx).map(|s| JsonValue::String(s.clone())).unwrap_or(JsonValue::Null),
        TypedColumn::Binary(v) => v.get(idx).map(|b| JsonValue::String(format!("<{} bytes>", b.len()))).unwrap_or(JsonValue::Null),
        TypedColumn::Variant(v) => v.get(idx).and_then(|s| serde_json::from_str(s).ok()).unwrap_or(JsonValue::Null),
        TypedColumn::Boolean(v) => v.get(idx).map(|&b| JsonValue::Bool(b)).unwrap_or(JsonValue::Null),
        TypedColumn::Date(v) | TypedColumn::Timestamp(v) => v.get(idx).map(|i| JsonValue::Number(serde_json::Number::from(*i))).unwrap_or(JsonValue::Null),
        TypedColumn::Vector(v) => v.get(idx).map(|vec| { JsonValue::Array(vec.iter().map(|&f| serde_json::Number::from_f64(f as f64).map(JsonValue::Number).unwrap_or(JsonValue::Null)).collect()) }).unwrap_or(JsonValue::Null),
    }
}

/// Filter a TypedColumn to only rows where keep_mask[idx] is true.
fn filter_column(col: TypedColumn, keep_mask: &[bool]) -> TypedColumn {
    let m = |i: &usize| keep_mask.get(*i).copied().unwrap_or(false);
    match col {
        TypedColumn::Int64(v) => TypedColumn::Int64(v.into_iter().enumerate().filter(|(i,_)| m(i)).map(|(_,v)| v).collect()),
        TypedColumn::Float64(v) => TypedColumn::Float64(v.into_iter().enumerate().filter(|(i,_)| m(i)).map(|(_,v)| v).collect()),
        TypedColumn::String(v) => TypedColumn::String(v.into_iter().enumerate().filter(|(i,_)| m(i)).map(|(_,v)| v).collect()),
        TypedColumn::Binary(v) => TypedColumn::Binary(v.into_iter().enumerate().filter(|(i,_)| m(i)).map(|(_,v)| v).collect()),
        TypedColumn::Variant(v) => TypedColumn::Variant(v.into_iter().enumerate().filter(|(i,_)| m(i)).map(|(_,v)| v).collect()),
        TypedColumn::Boolean(v) => TypedColumn::Boolean(v.into_iter().enumerate().filter(|(i,_)| m(i)).map(|(_,v)| v).collect()),
        TypedColumn::Date(v) => TypedColumn::Date(v.into_iter().enumerate().filter(|(i,_)| m(i)).map(|(_,v)| v).collect()),
        TypedColumn::Timestamp(v) => TypedColumn::Timestamp(v.into_iter().enumerate().filter(|(i,_)| m(i)).map(|(_,v)| v).collect()),
        TypedColumn::Vector(v) => TypedColumn::Vector(v.into_iter().enumerate().filter(|(i,_)| m(i)).map(|(_,v)| v).collect()),
    }
}

/// Evaluate whether a row matches a `where` filter.
///
/// Supports rich predicates (inspired by SQL / polars / pyspark):
///
///   where={'city': 'NYC'}                    → city = 'NYC'  (equality)
///   where={'age': ('>', 25)}                 → age > 25
///   where={'age': ('>=', 18)}                → age >= 18
///   where={'age': ('<', 65)}                 → age < 65
///   where={'age': ('<=', 30)}                → age <= 30
///   where={'age': ('!=', 30)}                → age != 30
///   where={'status': ('in', ['active', 'pending'])}  → status IN (...)
///   where={'city': 'NYC', 'age': ('>', 25)}  → city='NYC' AND age>25  (AND)
///   where={'age': [('>', 18), ('<', 65)]}    → age>18 AND age<65  (range)
///
/// All conditions are AND-combined. For OR, use multiple calls or a list
/// of where dicts (future enhancement).
fn row_matches_where(row: &JsonValue, where_dict: &serde_json::Map<String, JsonValue>) -> bool {
    let row_obj = match row.as_object() {
        Some(obj) => obj,
        None => return false,
    };

    for (col, condition) in where_dict {
        let cell = row_obj.get(col);
        if !eval_condition(cell, condition) {
            return false;
        }
    }
    true
}

/// Evaluate a single condition against a cell value.
///
/// condition can be:
///   - A bare value (equality): `25`, `"NYC"`, `true`
///   - A tuple/list [op, value]: `(">", 25)`, `[">=", 18]`
///   - A list of [op, value] tuples (AND): `[(">", 18), ("<", 65)]`
fn eval_condition(cell: Option<&JsonValue>, condition: &JsonValue) -> bool {
    match condition {
        // Bare value → equality
        JsonValue::String(_) | JsonValue::Number(_) | JsonValue::Bool(_) => {
            cell == Some(condition)
        }

        // Array → could be [op, value] or list of [op, value] tuples
        JsonValue::Array(arr) => {
            if arr.is_empty() {
                return true;
            }

            // Check if first element is a string (operator) → [op, value]
            if let Some(first) = arr.first() {
                if first.is_string() {
                    // [op, value] — single condition
                    return eval_op_condition(cell, arr);
                }
            }

            // List of conditions — all must match (AND)
            for sub in arr {
                if !eval_condition(cell, sub) {
                    return false;
                }
            }
            true
        }

        // null → match null cells
        JsonValue::Null => cell.is_none() || cell == Some(&JsonValue::Null),

        // Object → could be {"op": ">=", "value": 18} format
        JsonValue::Object(obj) => {
            if let (Some(op), Some(val)) = (
                obj.get("op").and_then(|v| v.as_str()),
                obj.get("value")
            ) {
                let fake_arr = vec![JsonValue::String(op.to_string()), val.clone()];
                eval_op_condition(cell, &fake_arr)
            } else {
                false
            }
        }
    }
}

/// Evaluate a single [op, value] condition.
fn eval_op_condition(cell: Option<&JsonValue>, op_val: &[JsonValue]) -> bool {
    if op_val.len() < 2 {
        return false;
    }
    let op = match op_val[0].as_str() {
        Some(s) => s,
        None => return false,
    };
    let target = &op_val[1];

    match op {
        "=" | "==" => cell == Some(target),
        "!=" | "<>" => cell != Some(target),
        ">" => cmp_values(cell, target) == std::cmp::Ordering::Greater,
        ">=" => matches!(cmp_values(cell, target), std::cmp::Ordering::Greater | std::cmp::Ordering::Equal),
        "<" => cmp_values(cell, target) == std::cmp::Ordering::Less,
        "<=" => matches!(cmp_values(cell, target), std::cmp::Ordering::Less | std::cmp::Ordering::Equal),
        "in" => {
            // target is a list of values
            if let Some(arr) = target.as_array() {
                arr.iter().any(|v| cell == Some(v))
            } else {
                false
            }
        }
        "not in" => {
            if let Some(arr) = target.as_array() {
                !arr.iter().any(|v| cell == Some(v))
            } else {
                true
            }
        }
        "is null" | "isnull" => cell.is_none() || cell == Some(&JsonValue::Null),
        "is not null" | "notnull" => cell.is_some() && cell != Some(&JsonValue::Null),
        "like" => {
            // Simple SQL LIKE: % = any chars, _ = single char
            if let (Some(cell_str), Some(pattern)) = (cell.and_then(|c| c.as_str()), target.as_str()) {
                like_match(cell_str, pattern)
            } else {
                false
            }
        }
        _ => false,
    }
}

/// Compare two JSON values (numbers compared numerically, strings lexicographically).
fn cmp_values(a: Option<&JsonValue>, b: &JsonValue) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    let a = match a {
        Some(v) => v,
        None => return Ordering::Less,
    };

    // Try numeric comparison first
    if let (Some(an), Some(bn)) = (a.as_f64(), b.as_f64()) {
        return an.partial_cmp(&bn).unwrap_or(Ordering::Equal);
    }

    // Try string comparison
    if let (Some(as_), Some(bs)) = (a.as_str(), b.as_str()) {
        return as_.cmp(bs);
    }

    // Fallback: string representation
    a.to_string().cmp(&b.to_string())
}

/// Simple SQL LIKE pattern matching: % = any chars, _ = single char.
fn like_match(text: &str, pattern: &str) -> bool {
    let text_chars: Vec<char> = text.chars().collect();
    let pattern_chars: Vec<char> = pattern.chars().collect();
    like_match_helper(&text_chars, &pattern_chars)
}

fn like_match_helper(text: &[char], pattern: &[char]) -> bool {
    if pattern.is_empty() {
        return text.is_empty();
    }
    match pattern[0] {
        '%' => {
            // Match zero or more characters
            if like_match_helper(text, &pattern[1..]) {
                return true;
            }
            if !text.is_empty() && like_match_helper(&text[1..], pattern) {
                return true;
            }
            false
        }
        '_' => {
            // Match exactly one character
            !text.is_empty() && like_match_helper(&text[1..], &pattern[1..])
        }
        c => {
            // Match literal character
            !text.is_empty() && text[0] == c && like_match_helper(&text[1..], &pattern[1..])
        }
    }
}

/// Simple base64 encoding (no external dependency).
fn base64_encode(data: &[u8]) -> String {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut result = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let triple = (b0 << 16) | (b1 << 8) | b2;
        result.push(CHARS[((triple >> 18) & 0x3F) as usize] as char);
        result.push(CHARS[((triple >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            result.push(CHARS[((triple >> 6) & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
        if chunk.len() > 2 {
            result.push(CHARS[(triple & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
    }
    result
}

/// Simple base64 decoding.
fn base64_decode(s: &str) -> Vec<u8> {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut result = Vec::new();
    let mut buf = 0u32;
    let mut bits = 0;
    for c in s.bytes() {
        if c == b'=' { break; }
        let val = CHARS.iter().position(|&x| x == c).map(|i| i as u32).unwrap_or(0);
        buf = (buf << 6) | val;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            result.push((buf >> bits) as u8);
            buf &= (1 << bits) - 1;
        }
    }
    result
}

/// Guess MIME type from file extension.
fn guess_mime(name: &str) -> &'static str {
    let ext = name.rsplit('.').next().unwrap_or("").to_lowercase();
    match ext.as_str() {
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "svg" => "image/svg+xml",
        "mp4" => "video/mp4",
        "avi" => "video/x-msvideo",
        "mov" => "video/quicktime",
        "webm" => "video/webm",
        "mp3" => "audio/mpeg",
        "wav" => "audio/wav",
        "ogg" => "audio/ogg",
        "flac" => "audio/flac",
        "pdf" => "application/pdf",
        "json" => "application/json",
        "csv" => "text/csv",
        "txt" => "text/plain",
        "html" => "text/html",
        "xml" => "application/xml",
        "zip" => "application/zip",
        "gz" | "gzip" => "application/gzip",
        "tar" => "application/x-tar",
        "parquet" => "application/vnd.apache.parquet",
        "pt" | "pth" => "application/octet-stream",  // PyTorch model
        "onnx" => "application/octet-stream",
        _ => "application/octet-stream",
    }
}

/// Generate a short unique ID for shard names (timestamp-based).
fn chrono_like_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{:016x}", ts)
}

/// CRDT merge: dedup by _rowid, latest _version wins, tombstones suppress.
///
/// Input: Vec<(rowid, row)> from HEAD + all shards.
/// Output: Vec<row> with duplicates removed and tombstones filtered out.
///
/// Rules:
///   1. Group rows by _rowid
///   2. Within each group, the row with the latest _version wins
///   3. If the winning row has _deleted=true, it's suppressed (not in output)
///   4. Rows without _rowid are passed through as-is (legacy non-CRDT data)
///   5. Insertion order is preserved (first occurrence of each _rowid)
///
/// For large row sets (>10K), uses rayon for parallel chunked merge.
/// Each chunk is merged independently, then chunk results are merged
/// sequentially (the final merge is O(unique_keys) which is fast).
fn crdt_merge_rows(rows: Vec<(String, JsonValue)>) -> Vec<JsonValue> {
    use std::collections::HashMap;

    const PARALLEL_THRESHOLD: usize = 10_000;

    if rows.len() > PARALLEL_THRESHOLD {
        return crdt_merge_rows_parallel(rows);
    }

    crdt_merge_rows_sequential(rows)
}

/// Sequential CRDT merge (for small row sets).
fn crdt_merge_rows_sequential(rows: Vec<(String, JsonValue)>) -> Vec<JsonValue> {
    use std::collections::HashMap;

    let mut order: Vec<String> = Vec::new();
    let mut latest: HashMap<String, (String, JsonValue)> = HashMap::new();
    let mut no_rowid: Vec<JsonValue> = Vec::new();

    for (rowid, row) in rows {
        let effective_rowid = row.get("_rowid")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or(rowid);

        let is_deleted = row.get("_deleted")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let version = row.get("_version")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();

        if effective_rowid.is_empty() {
            if !is_deleted {
                no_rowid.push(row);
            }
            continue;
        }

        match latest.get(&effective_rowid) {
            Some((existing_ver, _)) => {
                if version > *existing_ver {
                    latest.insert(effective_rowid.clone(), (version, row));
                }
            }
            None => {
                order.push(effective_rowid.clone());
                latest.insert(effective_rowid, (version, row));
            }
        }
    }

    let mut result: Vec<JsonValue> = no_rowid;
    for rowid in &order {
        if let Some((_, row)) = latest.get(rowid) {
            let is_deleted = row.get("_deleted")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            if !is_deleted {
                result.push(row.clone());
            }
        }
    }

    result
}

/// Parallel CRDT merge using rayon (for large row sets >10K).
///
/// Splits rows into chunks, merges each chunk in parallel (producing a
/// HashMap per chunk), then merges the chunk HashMaps sequentially.
/// The parallel phase is O(N/num_threads) per thread; the sequential
/// merge phase is O(unique_keys) which is typically much smaller than N.
fn crdt_merge_rows_parallel(rows: Vec<(String, JsonValue)>) -> Vec<JsonValue> {
    use std::collections::HashMap;

    // Split into chunks and merge each chunk in parallel
    let chunk_size = (rows.len() / rayon::current_num_threads().max(1)).max(1000);

    let chunk_results: Vec<(Vec<JsonValue>, Vec<(String, String, JsonValue)>)> = rows
        .par_chunks(chunk_size)
        .map(|chunk| {
            let mut no_rowid: Vec<JsonValue> = Vec::new();
            let mut latest: HashMap<String, (String, JsonValue)> = HashMap::new();

            for (rowid, row) in chunk {
                let row = row.clone();
                let effective_rowid = row.get("_rowid")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| rowid.clone());

                let is_deleted = row.get("_deleted")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);

                let version = row.get("_version")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .unwrap_or_default();

                if effective_rowid.is_empty() {
                    if !is_deleted {
                        no_rowid.push(row);
                    }
                    continue;
                }

                match latest.get(&effective_rowid) {
                    Some((existing_ver, _)) => {
                        if version > *existing_ver {
                            latest.insert(effective_rowid, (version, row));
                        }
                    }
                    None => {
                        latest.insert(effective_rowid, (version, row));
                    }
                }
            }

            // Convert HashMap to Vec for the sequential merge phase
            let merged: Vec<(String, String, JsonValue)> = latest
                .into_iter()
                .map(|(rowid, (version, row))| (rowid, version, row))
                .collect();

            (no_rowid, merged)
        })
        .collect();

    // Sequential merge phase: combine all chunk results
    let mut result: Vec<JsonValue> = Vec::new();
    let mut final_latest: HashMap<String, (String, JsonValue)> = HashMap::new();
    let mut order: Vec<String> = Vec::new();

    for (no_rowid, merged) in chunk_results {
        // Add non-CRDT rows directly
        result.extend(no_rowid);

        // Merge CRDT rows — latest version wins across chunks
        for (rowid, version, row) in merged {
            match final_latest.get(&rowid) {
                Some((existing_ver, _)) => {
                    if version > *existing_ver {
                        final_latest.insert(rowid.clone(), (version, row));
                    }
                }
                None => {
                    order.push(rowid.clone());
                    final_latest.insert(rowid, (version, row));
                }
            }
        }
    }

    // Collect in insertion order, skipping tombstones
    for rowid in &order {
        if let Some((_, row)) = final_latest.get(rowid) {
            let is_deleted = row.get("_deleted")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            if !is_deleted {
                result.push(row.clone());
            }
        }
    }

    result
}

/// Convert a serde_json::Value to a Python object.
/// Binary data encoded as "__bin_b64__:" prefix is decoded back to bytes.
/// JSON arrays and objects (from VARIANT columns) are converted to Python lists/dicts.
fn json_value_to_py(py: Python, value: &JsonValue) -> PyObject {
    match value {
        JsonValue::Null => py.None(),
        JsonValue::Bool(b) => b.to_object(py),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                i.to_object(py)
            } else if let Some(f) = n.as_f64() {
                f.to_object(py)
            } else {
                py.None()
            }
        }
        JsonValue::String(s) => {
            // Check for binary data encoded as base64
            if let Some(b64) = s.strip_prefix("__bin_b64__:") {
                let bytes = base64_decode(b64);
                PyBytes::new_bound(py, &bytes).into()
            } else {
                s.to_object(py)
            }
        }
        JsonValue::Array(arr) => {
            let list = PyList::new_bound(py, arr.iter().map(|v| json_value_to_py(py, v)));
            list.into()
        }
        JsonValue::Object(obj) => {
            let dict = PyDict::new_bound(py);
            for (k, v) in obj {
                dict.set_item(k, json_value_to_py(py, v)).unwrap();
            }
            dict.into()
        }
    }
}

/// Write a list of JSON rows as a new HEAD snapshot (PND2 + manifest + commit).
///
/// Converts the JSON rows to typed columns, then delegates to
/// storage_write::write_rows (which auto-adds _rowid + _version if missing).
fn write_rows_from_json(
    kernel: &PondKernel,
    collection: &str,
    active_branch: &str,
    rows: &[JsonValue],
    message: &str,
) -> PyResult<String> {
    if rows.is_empty() {
        // Write an empty snapshot
        return storage_write::write(kernel, collection, active_branch, b"[]", message)
            .map_err(pyo3::exceptions::PyIOError::new_err);
    }

    // Collect all column names from the first row (assume homogeneous)
    let first_row = rows[0].as_object()
        .ok_or_else(|| pyo3::exceptions::PyTypeError::new_err("row is not an object"))?;
    let col_names: Vec<String> = first_row.keys().cloned().collect();

    // Build typed columns
    let mut typed_cols: Vec<(String, TypedColumn)> = Vec::new();
    for col_name in &col_names {
        let mut i64_vals: Vec<i64> = Vec::new();
        let mut f64_vals: Vec<f64> = Vec::new();
        let mut str_vals: Vec<String> = Vec::new();
        let mut col_type: u8 = 0; // 0=unknown, 1=i64, 2=f64, 3=str

        for row in rows {
            let obj = row.as_object()
                .ok_or_else(|| pyo3::exceptions::PyTypeError::new_err("row is not an object"))?;
            match obj.get(col_name) {
                Some(JsonValue::Number(n)) => {
                    if let Some(i) = n.as_i64() {
                        i64_vals.push(i);
                        if col_type == 0 || col_type == 1 { col_type = 1; }
                    } else if let Some(f) = n.as_f64() {
                        f64_vals.push(f);
                        if col_type == 0 || col_type == 2 { col_type = 2; }
                    }
                }
                Some(JsonValue::String(s)) => {
                    str_vals.push(s.clone());
                    if col_type == 0 || col_type == 3 { col_type = 3; }
                }
                _ => {
                    // null or other → push default for the detected type
                    match col_type {
                        1 => i64_vals.push(0),
                        2 => f64_vals.push(0.0),
                        3 => str_vals.push(String::new()),
                        _ => { col_type = 3; str_vals.push(String::new()); }
                    }
                }
            }
        }

        let typed = match col_type {
            1 => TypedColumn::Int64(i64_vals),
            2 => TypedColumn::Float64(f64_vals),
            _ => TypedColumn::String(str_vals),
        };
        typed_cols.push((col_name.clone(), typed));
    }

    let col_refs: Vec<(&str, TypedColumn)> = typed_cols.iter()
        .map(|(name, col)| (name.as_str(), col.clone()))
        .collect();

    storage_write::write_rows(kernel, collection, active_branch, &col_refs, message)
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))
}

/// Read all rows from a collection as (rowid, JSON row) pairs.
///
/// This is the auto-read helper used by `build_index` for simple indexes.
/// It reads HEAD + all shards, decodes PND2 blobs, and converts each row
/// to a JSON object. The rowid is taken from the first available key column
/// (tries _rowid, then the first key_field, then _key, then id).
///
/// For shard rows (CRDT), the _rowid field is used if present.
fn read_collection_as_json_rows(
    storage: &pond_storage::UnifiedStorage,
    collection: &str,
    key_fields: &[String],
) -> Result<Vec<(String, JsonValue)>, String> {
    read_collection_as_json_rows_filtered(storage, collection, key_fields, &[])
}

/// Read rows with optional columnar predicate filtering.
///
/// When predicates are provided, the PND2 columns are filtered at the column
/// level BEFORE converting to JSON rows. This skips the JSON conversion for
/// filtered-out rows — major speedup for selective queries on large datasets.
///
/// Uses SIMD (AVX2) for INT64 and FLOAT64 columns, scalar for STRING.
fn read_collection_as_json_rows_filtered(
    storage: &pond_storage::UnifiedStorage,
    collection: &str,
    key_fields: &[String],
    predicates: &[(String, String, JsonValue)],
) -> Result<Vec<(String, JsonValue)>, String> {
    use pond_storage::shard;
    use pond_storage::manifest::CollectionManifest;
    use pond_storage::commit;
    use pond_storage::branch_ref;

    let kernel = storage.kernel();
    let mut rows: Vec<(String, JsonValue)> = Vec::new();

    let active = storage.get_active_branch(collection);

    // --- Read HEAD data ---
    let head = kernel.resolve(&branch_ref(collection, &active));
    if let Some(ref head_hash) = head {
        let manifest_bytes = pond_storage::commit::resolve_manifest_bytes(kernel, head_hash)
            .map_err(|e| format!("Failed to read manifest: {}", e))?;

        let manifest = CollectionManifest::decode(&manifest_bytes)
            .ok_or_else(|| "Failed to decode manifest".to_string())?;

        // COLUMNAR PREDICATE EVALUATION:
        // Decode PND2 → apply SIMD columnar filter → only convert surviving rows to JSON
        if manifest.row_groups.len() > 2 {
            // Parallel decode with columnar filter
            let row_groups = &manifest.row_groups;
            let key_fields_ref = key_fields;
            let preds_ref = predicates;
            let results: Vec<Result<Vec<(String, JsonValue)>, String>> = std::thread::scope(|s| {
                let handles: Vec<_> = row_groups.iter().map(|rg| {
                    s.spawn(move || {
                        let blob_data = kernel.read_blob(&rg.blob_hash)
                            .map_err(|e| format!("Failed to read data blob: {}", e))?;
                        let cols = pond_core::pnd2_decode(&blob_data)
                            .map_err(|e| format!("Failed to decode PND2: {}", e))?;

                        // Apply columnar predicate filter BEFORE JSON conversion
                        if preds_ref.is_empty() {
                            Ok(decode_cols_to_rows(&cols, key_fields_ref))
                        } else {
                            let mask = simd::columnar_filter(&cols, preds_ref);
                            Ok(decode_cols_to_rows_filtered(&cols, key_fields_ref, Some(&mask)))
                        }
                    })
                }).collect();

                handles.into_iter().map(|h| h.join().unwrap_or(Err("Thread panicked".to_string()))).collect()
            });

            for result in results {
                rows.extend(result?);
            }
        } else {
            // Sequential decode with columnar filter
            for rg in &manifest.row_groups {
                let blob_data = kernel.read_blob(&rg.blob_hash)
                    .map_err(|e| format!("Failed to read data blob: {}", e))?;

                let cols = pond_core::pnd2_decode(&blob_data)
                    .map_err(|e| format!("Failed to decode PND2: {}", e))?;

                if predicates.is_empty() {
                    rows.extend(decode_cols_to_rows(&cols, key_fields));
                } else {
                    let mask = simd::columnar_filter(&cols, predicates);
                    rows.extend(decode_cols_to_rows_filtered(&cols, key_fields, Some(&mask)));
                }
            }
        }
    }

    // --- Read shard data (CRDT) ---
    // Shards are JSON — no columnar filter possible (they're already JSON)
    let (_, shards) = shard::read_with_shards(kernel, collection, &active);
    for (_, shard_hash) in shards {
        if let Ok(data) = kernel.read_blob(&shard_hash) {
            if let Ok(arr) = serde_json::from_slice::<Vec<JsonValue>>(&data) {
                for row in arr {
                    let rowid = determine_rowid(&row, key_fields);
                    rows.push((rowid, row));
                }
            }
        }
    }

    Ok(rows)
}

/// Decode PND2 columns into (rowid, JSON row) pairs.
///
/// This is the shared helper used by both the parallel and sequential
/// decode paths. Extracted to avoid code duplication.
/// Decode PND2 columns into (rowid, JSON row) pairs.
///
/// If a `keep_mask` is provided, only rows where keep_mask[row_idx] is true
/// are converted to JSON — skipping the JSON conversion for filtered-out rows.
/// This is the columnar predicate evaluation optimization.
fn decode_cols_to_rows(cols: &[pond_core::PondColumn], key_fields: &[String]) -> Vec<(String, JsonValue)> {
    decode_cols_to_rows_filtered(cols, key_fields, None)
}

fn decode_cols_to_rows_filtered(
    cols: &[pond_core::PondColumn],
    key_fields: &[String],
    keep_mask: Option<&[bool]>,
) -> Vec<(String, JsonValue)> {
    use pond_core::{VT_INT64, VT_FLOAT64, VT_STRING, VT_BINARY, VT_NULL};
    let mut rows = Vec::new();
    let n_rows = cols.first().map(|c| c.n_values).unwrap_or(0);

    for row_idx in 0..n_rows {
        // Skip filtered-out rows — this is the key optimization:
        // we never convert these rows to JSON
        if let Some(mask) = keep_mask {
            if !mask[row_idx] { continue; }
        }

        let mut row_obj = serde_json::Map::new();
        for col in cols {
            let name = col.name.to_string_lossy().to_string();
            let val = match col.vtype {
                VT_INT64 => {
                    col.i64_data.get(row_idx)
                        .map(|v| JsonValue::Number(serde_json::Number::from(*v)))
                        .unwrap_or(JsonValue::Null)
                }
                VT_FLOAT64 => {
                    col.f64_data.get(row_idx)
                        .and_then(|v| serde_json::Number::from_f64(*v))
                        .map(JsonValue::Number)
                        .unwrap_or(JsonValue::Null)
                }
                VT_STRING => {
                    col.str_data.get(row_idx)
                        .map(|v| JsonValue::String(v.to_string_lossy().to_string()))
                        .unwrap_or(JsonValue::Null)
                }
                VT_BINARY => {
                    // Binary data stored as base64 string — decoded back to bytes on read
                    col.bin_data.get(row_idx)
                        .map(|b| JsonValue::String(format!("__bin_b64__:{}", base64_encode(b))))
                        .unwrap_or(JsonValue::Null)
                }
                _vt_variant => {
                    // Variant: JSON-encoded string — parse back to JSON value
                    col.str_data.get(row_idx)
                        .and_then(|s| {
                            let s_str = s.to_string_lossy();
                            serde_json::from_str::<JsonValue>(&s_str).ok()
                        })
                        .unwrap_or(JsonValue::Null)
                }
                VT_NULL | _ => JsonValue::Null,
            };
            row_obj.insert(name, val);
        }
        let rowid = determine_rowid(&JsonValue::Object(row_obj.clone()), key_fields);
        rows.push((rowid, JsonValue::Object(row_obj)));
    }
    rows
}

/// Determine the rowid for a row.
///
/// Tries (in order): _rowid, first key_field, _key, id, then a hash of the row.
fn determine_rowid(row: &JsonValue, key_fields: &[String]) -> String {
    // Try _rowid first (CRDT rows have this)
    if let Some(r) = row.get("_rowid").and_then(|v| v.as_str()) {
        return r.to_string();
    }
    if let Some(n) = row.get("_rowid").and_then(|v| v.as_i64()) {
        return n.to_string();
    }
    // Try the first key_field
    if let Some(kf) = key_fields.first() {
        if let Some(s) = row.get(kf).and_then(|v| v.as_str()) {
            return s.to_string();
        }
        if let Some(n) = row.get(kf).and_then(|v| v.as_i64()) {
            return n.to_string();
        }
    }
    // Try _key, id
    for fallback in &["_key", "id", "key"] {
        if let Some(s) = row.get(fallback).and_then(|v| v.as_str()) {
            return s.to_string();
        }
        if let Some(n) = row.get(fallback).and_then(|v| v.as_i64()) {
            return n.to_string();
        }
    }
    // Last resort: hash the row
    let s = serde_json::to_string(row).unwrap_or_default();
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    s.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

// ---------------------------------------------------------------------------
// UDF pushdown helpers
// ---------------------------------------------------------------------------

/// Extract UDF function calls from a SQL string and replace them with
/// marker comparisons that the WHERE parser can handle.
///
/// For each `func_name(col1, col2, ...)` found in the SQL where `func_name`
/// is a registered UDF, this function:
///   1. Records the call as (udf_name, Vec<column_args>)
///   2. Replaces the call text with `_udf_marker_<idx> = 1`
///
/// At evaluation time, each row gets `_udf_marker_<idx>` set to 1 (if the
/// UDF returns truthy) or 0 (if falsy), so the marker comparison evaluates
/// correctly within AND/OR/NOT expressions.
///
/// Returns (cleaned_sql, list_of_udf_calls).
fn extract_udf_calls_from_sql(
    sql: &str,
    udf_names: &[String],
) -> (String, Vec<(String, Vec<String>)>) {
    if udf_names.is_empty() {
        return (sql.to_string(), Vec::new());
    }

    let mut result = sql.to_string();
    let mut calls: Vec<(String, Vec<String>)> = Vec::new();

    for udf_name in udf_names {
        let pattern = format!("{}(", udf_name);
        loop {
            // Find the next occurrence of "udf_name(" with a word boundary
            // before it (so "my_is_adult(" doesn't match UDF "is_adult").
            let mut search_from = 0;
            let pos = loop {
                match result[search_from..].find(&pattern) {
                    Some(p) => {
                        let abs_pos = search_from + p;
                        let before_ok = abs_pos == 0 || {
                            let prev = result.as_bytes()[abs_pos - 1] as char;
                            !prev.is_alphanumeric() && prev != '_'
                        };
                        if before_ok {
                            break abs_pos;
                        }
                        search_from = abs_pos + pattern.len();
                    }
                    None => break usize::MAX,
                }
            };

            if pos == usize::MAX {
                break;
            }

            // Find the matching close paren
            let start = pos + pattern.len();
            let mut depth: i32 = 1;
            let mut end = start;
            let bytes = result.as_bytes();
            while end < bytes.len() && depth > 0 {
                match bytes[end] as char {
                    '(' => depth += 1,
                    ')' => depth -= 1,
                    _ => {}
                }
                if depth > 0 {
                    end += 1;
                }
            }
            if depth != 0 {
                break; // unbalanced parens — skip
            }

            // Extract and parse args (comma-separated column names)
            let args_str = &result[start..end];
            let args: Vec<String> = args_str
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();

            // Record the call and replace with a marker comparison
            let call_idx = calls.len();
            calls.push((udf_name.clone(), args));
            let marker = format!("_udf_marker_{} = 1", call_idx);
            result.replace_range(pos..=end, &marker);
        }
    }

    (result, calls)
}

/// Evaluate a UDF against a single row.
///
/// Calls the Python function with the values of the specified columns
/// from the row. Returns `true` if the function returns a truthy value,
/// `false` otherwise (including on error).
fn evaluate_udf(
    py: Python,
    func: &PyObject,
    row: &JsonValue,
    args: &[String],
) -> bool {
    let arg_values: Vec<PyObject> = args
        .iter()
        .map(|col| {
            let val = row.get(col).unwrap_or(&JsonValue::Null);
            json_to_pyobject(py, val)
        })
        .collect();

    // Build a Python tuple from the arg values (Vec<PyObject> converts to
    // a Python list, not a tuple, so we must wrap explicitly).
    let args_tuple = PyTuple::new_bound(py, arg_values);
    match func.call1(py, args_tuple) {
        Ok(result) => result.is_truthy(py).unwrap_or(false),
        Err(_) => false,
    }
}

// (fetch_udf_functions removed — UDF funcs are fetched inline in sql()
// where the `py` token is available for clone_ref.)

// ---------------------------------------------------------------------------
// Python module definition
// ---------------------------------------------------------------------------

#[pymodule]
fn pond(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(decode, m)?)?;
    m.add_function(wrap_pyfunction!(encode, m)?)?;
    m.add_class::<Storage>()?;
    m.add_class::<RowBatchStream>()?;
    m.add_class::<SemanticLayer>()?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests — UDF pushdown + RLS
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Create a unique temp directory for test storage.
    fn make_temp_dir() -> String {
        let dir = format!("/tmp/pond_test_{}", chrono_like_id());
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// Remove a temp directory (best-effort).
    fn cleanup_dir(dir: &str) {
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Extract a Vec<String> from a Python dict result by key.
    fn extract_string_vec(py: Python, obj: &PyObject, key: &str) -> Vec<String> {
        let dict = obj.bind(py).downcast::<PyDict>().unwrap();
        let val = dict.get_item(key).unwrap().unwrap();
        val.extract::<Vec<String>>().unwrap()
    }

    /// Extract a Vec<i64> from a Python dict result by key.
    fn extract_i64_vec(py: Python, obj: &PyObject, key: &str) -> Vec<i64> {
        let dict = obj.bind(py).downcast::<PyDict>().unwrap();
        let val = dict.get_item(key).unwrap().unwrap();
        val.extract::<Vec<i64>>().unwrap()
    }

    // ===================================================================
    // Pure Rust tests — extract_udf_calls_from_sql (no Python needed)
    // ===================================================================

    #[test]
    fn test_extract_udf_calls_simple() {
        let udfs = vec!["is_adult".to_string()];
        let (cleaned, calls) =
            extract_udf_calls_from_sql("SELECT * FROM users WHERE is_adult(age)", &udfs);
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, "is_adult");
        assert_eq!(calls[0].1, vec!["age".to_string()]);
        assert!(cleaned.contains("_udf_marker_0 = 1"));
        assert!(!cleaned.contains("is_adult(age)"));
    }

    #[test]
    fn test_extract_udf_calls_multiple() {
        let udfs = vec!["is_adult".to_string(), "starts_with_a".to_string()];
        let (cleaned, calls) = extract_udf_calls_from_sql(
            "SELECT * FROM users WHERE is_adult(age) AND starts_with_a(name)",
            &udfs,
        );
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].0, "is_adult");
        assert_eq!(calls[0].1, vec!["age".to_string()]);
        assert_eq!(calls[1].0, "starts_with_a");
        assert_eq!(calls[1].1, vec!["name".to_string()]);
        assert!(cleaned.contains("_udf_marker_0 = 1"));
        assert!(cleaned.contains("_udf_marker_1 = 1"));
    }

    #[test]
    fn test_extract_udf_calls_multi_col() {
        let udfs = vec!["validate".to_string()];
        let (cleaned, calls) =
            extract_udf_calls_from_sql("SELECT * FROM t WHERE validate(a, b, c)", &udfs);
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, "validate");
        assert_eq!(calls[0].1, vec!["a".to_string(), "b".to_string(), "c".to_string()]);
        assert!(cleaned.contains("_udf_marker_0 = 1"));
    }

    #[test]
    fn test_extract_udf_calls_word_boundary() {
        // "my_is_adult(age)" should NOT match UDF "is_adult"
        let udfs = vec!["is_adult".to_string()];
        let (cleaned, calls) = extract_udf_calls_from_sql(
            "SELECT * FROM users WHERE my_is_adult(age)",
            &udfs,
        );
        assert_eq!(calls.len(), 0);
        assert!(cleaned.contains("my_is_adult(age)"));
    }

    #[test]
    fn test_extract_udf_calls_no_udfs_registered() {
        let (cleaned, calls) =
            extract_udf_calls_from_sql("SELECT * FROM users WHERE age >= 18", &[]);
        assert_eq!(calls.len(), 0);
        assert_eq!(cleaned, "SELECT * FROM users WHERE age >= 18");
    }

    #[test]
    fn test_extract_udf_calls_with_or() {
        let udfs = vec!["is_adult".to_string()];
        let (cleaned, calls) = extract_udf_calls_from_sql(
            "SELECT * FROM users WHERE is_adult(age) OR city = 'NYC'",
            &udfs,
        );
        assert_eq!(calls.len(), 1);
        assert!(cleaned.contains("_udf_marker_0 = 1 OR city = 'NYC'"));
    }

    #[test]
    fn test_extract_udf_calls_preserves_non_udf_sql() {
        let udfs = vec!["is_adult".to_string()];
        let (cleaned, calls) = extract_udf_calls_from_sql(
            "SELECT name, age FROM users WHERE age >= 18 AND city = 'NYC'",
            &udfs,
        );
        // No UDF calls in this SQL
        assert_eq!(calls.len(), 0);
        assert_eq!(cleaned, "SELECT name, age FROM users WHERE age >= 18 AND city = 'NYC'");
    }

    // ===================================================================
    // UDF pushdown tests — require Python (register_udf + sql)
    // ===================================================================

    #[test]
    fn test_udf_is_adult() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let dir = make_temp_dir();
            let storage = Storage::new(&dir, None, None, None, None, None).unwrap();

            // Write test data: alice(25), bob(15), carol(30)
            storage
                .write_rows(
                    "users",
                    vec![
                        (
                            "name".to_string(),
                            vec![
                                "alice".to_object(py),
                                "bob".to_object(py),
                                "carol".to_object(py),
                            ],
                        ),
                        (
                            "age".to_string(),
                            vec![25.to_object(py), 15.to_object(py), 30.to_object(py)],
                        ),
                    ],
                    "init",
                    true,
                    None,
                )
                .unwrap();

            // Register UDF: is_adult(age) → age >= 18
            let func = py
                .eval_bound("lambda age: age >= 18", None, None)
                .unwrap()
                .unbind();
            storage.register_udf("is_adult", func).unwrap();
            assert_eq!(storage.list_udfs(), vec!["is_adult".to_string()]);

            // Query with UDF in WHERE
            let result = storage
                .sql(py, "SELECT * FROM users WHERE is_adult(age)")
                .unwrap();

            // Should return alice(25) and carol(30), but NOT bob(15)
            let names = extract_string_vec(py, &result, "name");
            assert_eq!(names.len(), 2, "Expected 2 adult rows, got {:?}", names);
            assert!(names.contains(&"alice".to_string()));
            assert!(names.contains(&"carol".to_string()));
            assert!(!names.contains(&"bob".to_string()));

            // Unregister and verify list is empty
            assert!(storage.unregister_udf("is_adult"));
            assert!(storage.list_udfs().is_empty());

            cleanup_dir(&dir);
        });
    }

    #[test]
    fn test_udf_starts_with_a() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let dir = make_temp_dir();
            let storage = Storage::new(&dir, None, None, None, None, None).unwrap();

            // Write test data with various names
            storage
                .write_rows(
                    "users",
                    vec![
                        (
                            "name".to_string(),
                            vec![
                                "alice".to_object(py),
                                "bob".to_object(py),
                                "aaron".to_object(py),
                                "charlie".to_object(py),
                            ],
                        ),
                    ],
                    "init",
                    true,
                    None,
                )
                .unwrap();

            // Register UDF: starts_with_a(name) → name.startswith('a')
            let func = py
                .eval_bound("lambda name: name.startswith('a')", None, None)
                .unwrap()
                .unbind();
            storage.register_udf("starts_with_a", func).unwrap();

            // Query with UDF in WHERE
            let result = storage
                .sql(py, "SELECT * FROM users WHERE starts_with_a(name)")
                .unwrap();

            // Should return alice and aaron
            let names = extract_string_vec(py, &result, "name");
            assert_eq!(names.len(), 2, "Expected 2 names starting with 'a', got {:?}", names);
            assert!(names.contains(&"alice".to_string()));
            assert!(names.contains(&"aaron".to_string()));

            cleanup_dir(&dir);
        });
    }

    #[test]
    fn test_udf_multiple_columns() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let dir = make_temp_dir();
            let storage = Storage::new(&dir, None, None, None, None, None).unwrap();

            // Write test data
            storage
                .write_rows(
                    "users",
                    vec![
                        (
                            "name".to_string(),
                            vec![
                                "alice".to_object(py),
                                "bob".to_object(py),
                                "carol".to_object(py),
                            ],
                        ),
                        (
                            "age".to_string(),
                            vec![25.to_object(py), 30.to_object(py), 20.to_object(py)],
                        ),
                        (
                            "score".to_string(),
                            vec![85.to_object(py), 90.to_object(py), 95.to_object(py)],
                        ),
                    ],
                    "init",
                    true,
                    None,
                )
                .unwrap();

            // Register UDF: is_eligible(age, score) → age >= 21 AND score >= 90
            let func = py
                .eval_bound("lambda age, score: age >= 21 and score >= 90", None, None)
                .unwrap()
                .unbind();
            storage.register_udf("is_eligible", func).unwrap();

            // Query with UDF in WHERE (multiple columns)
            let result = storage
                .sql(py, "SELECT * FROM users WHERE is_eligible(age, score)")
                .unwrap();

            // Should return bob(30, 90) and carol... wait carol is 20, not >= 21
            // Only bob(30, 90) qualifies: age >= 21 AND score >= 90
            let names = extract_string_vec(py, &result, "name");
            assert_eq!(names.len(), 1, "Expected 1 eligible row, got {:?}", names);
            assert!(names.contains(&"bob".to_string()));

            cleanup_dir(&dir);
        });
    }

    #[test]
    fn test_udf_combined_with_sql_condition() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let dir = make_temp_dir();
            let storage = Storage::new(&dir, None, None, None, None, None).unwrap();

            storage
                .write_rows(
                    "users",
                    vec![
                        (
                            "name".to_string(),
                            vec![
                                "alice".to_object(py),
                                "bob".to_object(py),
                                "carol".to_object(py),
                            ],
                        ),
                        (
                            "age".to_string(),
                            vec![25.to_object(py), 30.to_object(py), 20.to_object(py)],
                        ),
                        (
                            "city".to_string(),
                            vec!["NYC".to_object(py), "LA".to_object(py), "NYC".to_object(py)],
                        ),
                    ],
                    "init",
                    true,
                    None,
                )
                .unwrap();

            // Register UDF
            let func = py
                .eval_bound("lambda age: age >= 21", None, None)
                .unwrap()
                .unbind();
            storage.register_udf("is_adult", func).unwrap();

            // Query: is_adult(age) AND city = 'NYC'
            // alice(25, NYC) ✓, bob(30, LA) ✗ (wrong city), carol(20, NYC) ✗ (not adult)
            let result = storage
                .sql(py, "SELECT * FROM users WHERE is_adult(age) AND city = 'NYC'")
                .unwrap();

            let names = extract_string_vec(py, &result, "name");
            assert_eq!(names.len(), 1, "Expected 1 row, got {:?}", names);
            assert!(names.contains(&"alice".to_string()));

            cleanup_dir(&dir);
        });
    }

    // ===================================================================
    // RLS tests — set_rls_policy, read_rows filtering, write_rows auto-add
    // ===================================================================

    #[test]
    fn test_rls_policy_management() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let dir = make_temp_dir();
            let storage = Storage::new(&dir, None, None, None, None, None).unwrap();

            // No policy initially
            assert_eq!(storage.get_rls_policy("users"), None);

            // Set policy
            storage.set_rls_policy("users", "tenant_123").unwrap();
            assert_eq!(storage.get_rls_policy("users"), Some("tenant_123".to_string()));

            // Clear policy
            assert!(storage.clear_rls_policy("users"));
            assert_eq!(storage.get_rls_policy("users"), None);

            // Clear again → false (not found)
            assert!(!storage.clear_rls_policy("users"));

            cleanup_dir(&dir);
        });
    }

    #[test]
    fn test_rls_write_auto_adds_tenant() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let dir = make_temp_dir();
            let storage = Storage::new(&dir, None, None, None, None, None).unwrap();

            // Set RLS policy
            storage.set_rls_policy("users", "tenant_A").unwrap();

            // Write rows — _tenant should be auto-added
            storage
                .write_rows(
                    "users",
                    vec![
                        (
                            "name".to_string(),
                            vec!["alice".to_object(py), "bob".to_object(py)],
                        ),
                        (
                            "age".to_string(),
                            vec![25.to_object(py), 30.to_object(py)],
                        ),
                    ],
                    "init",
                    true,
                    None,
                )
                .unwrap();

            // Read with explicit _tenant column to verify it was auto-added
            let result = storage
                .read_rows(py, "users", Some(vec!["_tenant".to_string()]), None)
                .unwrap();

            let tenants = extract_string_vec(py, &result, "_tenant");
            assert_eq!(tenants.len(), 2);
            assert!(tenants.iter().all(|t| t == "tenant_A"));

            cleanup_dir(&dir);
        });
    }

    #[test]
    fn test_rls_read_filters_by_tenant() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let dir = make_temp_dir();
            let storage = Storage::new(&dir, None, None, None, None, None).unwrap();

            // Write rows with explicit _tenant values (no policy → no auto-add,
            // we provide _tenant manually)
            storage
                .write_rows(
                    "users",
                    vec![
                        (
                            "name".to_string(),
                            vec![
                                "alice".to_object(py),
                                "bob".to_object(py),
                                "carol".to_object(py),
                            ],
                        ),
                        (
                            "_tenant".to_string(),
                            vec![
                                "tenant_A".to_object(py),
                                "tenant_A".to_object(py),
                                "tenant_B".to_object(py),
                            ],
                        ),
                    ],
                    "init",
                    true,
                    None,
                )
                .unwrap();

            // Set RLS policy for tenant_A → should only see alice, bob
            storage.set_rls_policy("users", "tenant_A").unwrap();
            let result = storage.read_rows(py, "users", None, None).unwrap();
            let names = extract_string_vec(py, &result, "name");
            assert_eq!(names.len(), 2, "Expected 2 rows for tenant_A, got {:?}", names);
            assert!(names.contains(&"alice".to_string()));
            assert!(names.contains(&"bob".to_string()));

            // Switch to tenant_B → should only see carol
            storage.set_rls_policy("users", "tenant_B").unwrap();
            let result = storage.read_rows(py, "users", None, None).unwrap();
            let names = extract_string_vec(py, &result, "name");
            assert_eq!(names.len(), 1, "Expected 1 row for tenant_B, got {:?}", names);
            assert!(names.contains(&"carol".to_string()));

            // Clear RLS policy → should see all rows
            storage.clear_rls_policy("users");
            let result = storage.read_rows(py, "users", None, None).unwrap();
            let names = extract_string_vec(py, &result, "name");
            assert_eq!(names.len(), 3, "Expected 3 rows after clearing RLS, got {:?}", names);

            cleanup_dir(&dir);
        });
    }

    #[test]
    fn test_rls_multiple_tenants_isolation() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let dir = make_temp_dir();
            let storage = Storage::new(&dir, None, None, None, None, None).unwrap();

            // Write all rows at once with explicit _tenant values
            storage
                .write_rows(
                    "orders",
                    vec![
                        (
                            "id".to_string(),
                            vec![
                                1.to_object(py),
                                2.to_object(py),
                                3.to_object(py),
                                4.to_object(py),
                            ],
                        ),
                        (
                            "product".to_string(),
                            vec![
                                "widget".to_object(py),
                                "gadget".to_object(py),
                                "widget".to_object(py),
                                "gizmo".to_object(py),
                            ],
                        ),
                        (
                            "_tenant".to_string(),
                            vec![
                                "t1".to_object(py),
                                "t1".to_object(py),
                                "t2".to_object(py),
                                "t2".to_object(py),
                            ],
                        ),
                    ],
                    "init",
                    true,
                    None,
                )
                .unwrap();

            // Tenant t1 sees only orders 1, 2
            storage.set_rls_policy("orders", "t1").unwrap();
            let result = storage.read_rows(py, "orders", None, None).unwrap();
            let ids = extract_i64_vec(py, &result, "id");
            assert_eq!(ids.len(), 2);
            assert!(ids.contains(&1));
            assert!(ids.contains(&2));

            // Tenant t2 sees only orders 3, 4
            storage.set_rls_policy("orders", "t2").unwrap();
            let result = storage.read_rows(py, "orders", None, None).unwrap();
            let ids = extract_i64_vec(py, &result, "id");
            assert_eq!(ids.len(), 2);
            assert!(ids.contains(&3));
            assert!(ids.contains(&4));

            // Clear policy → all 4 orders visible
            storage.clear_rls_policy("orders");
            let result = storage.read_rows(py, "orders", None, None).unwrap();
            let ids = extract_i64_vec(py, &result, "id");
            assert_eq!(ids.len(), 4);

            cleanup_dir(&dir);
        });
    }

    #[test]
    fn test_rls_tenant_column_hidden_by_default() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let dir = make_temp_dir();
            let storage = Storage::new(&dir, None, None, None, None, None).unwrap();

            // Set policy and write
            storage.set_rls_policy("users", "tenant_X").unwrap();
            storage
                .write_rows(
                    "users",
                    vec![(
                        "name".to_string(),
                        vec!["alice".to_object(py), "bob".to_object(py)],
                    )],
                    "init",
                    true,
                    None,
                )
                .unwrap();

            // Read without specifying columns → _tenant should be hidden
            let result = storage.read_rows(py, "users", None, None).unwrap();
            let dict = result.bind(py).downcast::<PyDict>().unwrap();

            // _tenant should NOT be in the result (hidden like _rowid, _version)
            assert!(
                dict.get_item("_tenant").unwrap().is_none(),
                "_tenant should be hidden by default in read_rows"
            );
            // name should be present
            assert!(dict.get_item("name").unwrap().is_some());

            cleanup_dir(&dir);
        });
    }
}
