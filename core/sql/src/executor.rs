// SQL executor — runs parsed SQL statements against a `UnifiedStorage`.
//
// This is the pure-Rust entry point used by the Go SDK, CLI, and MCP server
// to execute SQL without going through PyO3 / Python.
//
// Public API:
//   - `execute(storage, query) -> Result<SqlResult, String>`
//   - `SqlResult { columns, rows }`
//
// Supported statements: SELECT (with JOIN, WHERE, GROUP BY, HAVING, ORDER BY,
// LIMIT/OFFSET, aggregates), INSERT, UPDATE, DELETE, MERGE.

use crate::parser::{
    AggregateExpr, AggregateFunc, JoinClause, JoinType, MergeAction, OrderByItem, SelectItem,
    SqlStatement, TableRef,
};
use crate::where_clause::WhereExpr;
use pond_core::{pnd2_decode_projected, PondColumn, TypedColumn, VT_BINARY, VT_FLOAT64, VT_INT64,
                VT_STRING, VT_VARIANT};
use pond_kernel::crdt::{uuidv7, HLC};
use pond_storage::shard;
use pond_storage::{write as storage_write, UnifiedStorage};
use serde_json::{json, Value as JsonValue};
use std::collections::{HashSet, HashMap};

/// Result of executing a SQL statement.
///
/// For SELECT: `columns` is the projected column list and `rows` is a
/// vector of JSON objects (one per result row).
///
/// For INSERT/UPDATE/DELETE/MERGE: `columns` is `["status"]` (or similar)
/// and `rows` contains a single status object with the affected-row count.
#[derive(Debug, Clone)]
pub struct SqlResult {
    pub columns: Vec<String>,
    pub rows: Vec<JsonValue>,
}

impl SqlResult {
    fn status(action: &str, count: usize) -> Self {
        Self {
            columns: vec![action.to_string()],
            rows: vec![json!({ action: count })],
        }
    }

    fn commit(commit_hash: &str) -> Self {
        Self {
            columns: vec!["commit".to_string()],
            rows: vec![json!({ "commit": commit_hash })],
        }
    }
}

/// Execute a SQL statement against `storage`.
///
/// `storage` is borrowed for the duration of the call — the executor does
/// not retain a reference.
pub fn execute(storage: &UnifiedStorage, query: &str) -> Result<SqlResult, String> {
    let stmt = parse_sql_internal(query)?;
    execute_stmt(storage, &stmt)
}

/// Re-export of `crate::parser::parse_sql` so callers can parse without
/// pulling in another `use`.
pub fn parse_sql_internal(query: &str) -> Result<SqlStatement, String> {
    crate::parser::parse_sql(query)
}

fn execute_stmt(storage: &UnifiedStorage, stmt: &SqlStatement) -> Result<SqlResult, String> {
    match stmt {
        SqlStatement::Select {
            table,
            alias,
            columns,
            select_items,
            joins,
            r#where,
            groups,
            having,
            orders,
            limit,
            offset,
        } => execute_select(
            storage, table, alias, columns, select_items, joins, r#where,
            groups, having, orders, *limit, *offset,
        ),
        SqlStatement::Update { collection, sets, r#where } => {
            execute_update(storage, collection, sets, r#where)
        }
        SqlStatement::Delete { collection, r#where } => {
            execute_delete(storage, collection, r#where)
        }
        SqlStatement::Insert { collection, columns, rows } => {
            execute_insert(storage, collection, columns, rows)
        }
        SqlStatement::Merge {
            target,
            source_rows,
            match_keys,
            when_matched,
            when_not_matched,
        } => execute_merge(storage, target, source_rows, match_keys, when_matched, when_not_matched),
    }
}

// ---------------------------------------------------------------------------
// SELECT
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
fn execute_select(
    storage: &UnifiedStorage,
    table: &TableRef,
    alias: &Option<String>,
    columns: &[String],
    select_items: &[SelectItem],
    joins: &[JoinClause],
    where_expr: &WhereExpr,
    groups: &[String],
    having: &WhereExpr,
    orders: &[OrderByItem],
    limit: Option<usize>,
    offset: Option<usize>,
) -> Result<SqlResult, String> {
    let join_on_left: Vec<String> = joins.iter()
        .flat_map(|j| j.on.iter().map(|(l, _)| l.clone()))
        .collect();
    let projection = extract_required_columns(
        select_items, where_expr, groups, orders, &join_on_left, having,
    );
    let mut result_rows = read_table_rows(storage, table, projection.as_ref())?;

    // If there's an alias, prefix all columns with the alias.
    if let Some(al) = alias {
        prefix_rows_with_alias(&mut result_rows, al);
    }

    // Execute JOINs.
    for join in joins {
        let mut right_rows = read_table_rows(storage, &join.table, None)?;
        if let Some(al) = &join.alias {
            prefix_rows_with_alias(&mut right_rows, al);
        }
        result_rows = execute_join(result_rows, right_rows, &join.on, &join.join_type);
    }

    // Resolve subqueries in WHERE.
    let where_resolved = where_expr.resolve_subqueries(|q| {
        let sub = crate::parser::parse_sql(q)?;
        if !matches!(sub, SqlStatement::Select { .. }) {
            return Err("Subquery must be a SELECT".to_string());
        }
        let sub_result = execute_stmt(storage, &sub)?;
        // Collect distinct values of the first column.
        let first_col = sub_result.columns.first().cloned();
        let first_col = match first_col {
            Some(c) => c,
            None => return Ok(Vec::new()),
        };
        let mut seen: std::collections::HashSet<JsonValue> = std::collections::HashSet::new();
        for row in &sub_result.rows {
            if let Some(v) = row.get(&first_col) {
                seen.insert(v.clone());
            }
        }
        Ok(seen.into_iter().collect())
    });

    // Apply WHERE filter.
    let mut filtered: Vec<JsonValue> = result_rows
        .into_iter()
        .filter(|r| where_resolved.eval(r))
        .collect();

    // Aggregates / GROUP BY.
    let has_aggregates = select_items.iter().any(|it| matches!(it, SelectItem::Aggregate(_)));
    if has_aggregates || !groups.is_empty() {
        filtered = apply_group_by(filtered, groups, select_items, having)?;
    }

    // ORDER BY.
    if !orders.is_empty() {
        apply_order_by(&mut filtered, orders);
    }

    // OFFSET + LIMIT.
    if let Some(off) = offset {
        let off = off.min(filtered.len());
        filtered.drain(..off);
    }
    if let Some(lim) = limit {
        filtered.truncate(lim);
    }

    // Projection.
    let result_columns = compute_result_columns(select_items, &filtered);
    let projected = project_rows(&filtered, select_items, columns);

    Ok(SqlResult {
        columns: result_columns,
        rows: projected,
    })
}

/// Prefix every column in every row with `alias.`. Used when a SELECT
/// statement assigns an alias to a table (`FROM users u`).
fn prefix_rows_with_alias(rows: &mut [JsonValue], alias: &str) {
    for row in rows.iter_mut() {
        if let Some(obj) = row.as_object_mut() {
            let prefixed: Vec<(String, JsonValue)> = obj
                .iter()
                .map(|(k, v)| (format!("{}.{}", alias, k), v.clone()))
                .collect();
            obj.clear();
            for (k, v) in prefixed {
                obj.insert(k, v);
            }
        }
    }
}

/// Compute the output column names for a SELECT result.
fn compute_result_columns(items: &[SelectItem], rows: &[JsonValue]) -> Vec<String> {
    let mut cols: Vec<String> = Vec::new();
    for item in items {
        match item {
            SelectItem::Star => {
                // Collect from the first row, skipping CRDT metadata.
                if let Some(row) = rows.first() {
                    if let Some(obj) = row.as_object() {
                        for k in obj.keys() {
                            let base = k.rsplit('.').next().unwrap_or(k);
                            if base == "_rowid" || base == "_version" || base == "_deleted" {
                                continue;
                            }
                            cols.push(k.clone());
                        }
                    }
                }
            }
            SelectItem::Column(c) => cols.push(c.clone()),
            SelectItem::Aggregate(a) => cols.push(aggregate_output_name(a)),
        }
    }
    cols
}

/// Determine the output column name for an aggregate.
fn aggregate_output_name(a: &AggregateExpr) -> String {
    if let Some(al) = &a.alias {
        return al.clone();
    }
    match a.func {
        AggregateFunc::Count if a.arg.is_none() => "COUNT(*)".to_string(),
        _ => format!("{}({})", a.func.as_str(), a.arg.clone().unwrap_or_default()),
    }
}

/// Project rows according to SELECT items.
fn project_rows(rows: &[JsonValue], items: &[SelectItem], legacy_cols: &[String]) -> Vec<JsonValue> {
    // Special case: SELECT * (single Star item, no aggregates, no GROUP BY).
    // Return rows as-is but strip CRDT metadata.
    let only_star = items.len() == 1 && matches!(items[0], SelectItem::Star);
    if only_star {
        return rows
            .iter()
            .map(strip_crdt_meta)
            .collect();
    }

    // Mixed: build a new row for each, keeping only the projected columns
    // (in declared order). For aggregates that were computed by
    // `apply_group_by`, the output column name is already in the row.
    rows.iter()
        .map(|r| {
            let mut out = serde_json::Map::new();
            for item in items {
                match item {
                    SelectItem::Star => {
                        if let Some(obj) = r.as_object() {
                            for (k, v) in obj {
                                let base = k.rsplit('.').next().unwrap_or(k);
                                if base == "_rowid" || base == "_version" || base == "_deleted" {
                                    continue;
                                }
                                out.insert(k.clone(), v.clone());
                            }
                        }
                    }
                    SelectItem::Column(c) => {
                        let v = lookup_col(r, c).unwrap_or(JsonValue::Null);
                        out.insert(c.clone(), v);
                    }
                    SelectItem::Aggregate(a) => {
                        let name = aggregate_output_name(a);
                        let v = r.get(&name).cloned().unwrap_or(JsonValue::Null);
                        out.insert(name, v);
                    }
                }
            }
            // Also include legacy columns if not already present — this keeps
            // backward compat with the PyO3 binding's old `columns` API.
            for c in legacy_cols {
                if !out.contains_key(c) {
                    let v = lookup_col(r, c).unwrap_or(JsonValue::Null);
                    out.insert(c.clone(), v);
                }
            }
            JsonValue::Object(out)
        })
        .collect()
}

/// Look up a column in a row, supporting qualified (`alias.col`) and
/// unqualified names.
fn lookup_col(row: &JsonValue, col: &str) -> Option<JsonValue> {
    if let Some(obj) = row.as_object() {
        if let Some(v) = obj.get(col) {
            return Some(v.clone());
        }
        // Try matching by suffix (`alias.col` → `col`).
        for (k, v) in obj {
            if k.ends_with(&format!(".{}", col)) || k.rsplit('.').next() == Some(col) {
                return Some(v.clone());
            }
        }
    }
    None
}

/// Strip CRDT metadata (`_rowid`, `_version`, `_deleted`) from a row.
fn strip_crdt_meta(row: &JsonValue) -> JsonValue {
    if let Some(obj) = row.as_object() {
        let mut out = serde_json::Map::new();
        for (k, v) in obj {
            let base = k.rsplit('.').next().unwrap_or(k);
            if base == "_rowid" || base == "_version" || base == "_deleted" {
                continue;
            }
            out.insert(k.clone(), v.clone());
        }
        JsonValue::Object(out)
    } else {
        row.clone()
    }
}

// ---------------------------------------------------------------------------
// GROUP BY + aggregates
// ---------------------------------------------------------------------------

fn apply_group_by(
    rows: Vec<JsonValue>,
    groups: &[String],
    select_items: &[SelectItem],
    having: &WhereExpr,
) -> Result<Vec<JsonValue>, String> {

    // If no GROUP BY but aggregates present, treat the whole input as one group.
    let aggregates: Vec<&AggregateExpr> = select_items
        .iter()
        .filter_map(|it| match it {
            SelectItem::Aggregate(a) => Some(a),
            _ => None,
        })
        .collect();

    if groups.is_empty() {
        // Single aggregate row over all rows.
        let mut out = serde_json::Map::new();
        for a in &aggregates {
            out.insert(aggregate_output_name(a), compute_aggregate(a, &rows));
        }
        // Resolve any bare aggregate references in HAVING (e.g.
        // `HAVING COUNT(*) > 5` or `HAVING AVG(salary) > 50000`) that
        // weren't already computed as SELECT items. The aggregate is
        // computed from the group's rows (here, all input rows).
        resolve_having_aggregates(having, &rows, &mut out);
        let row = JsonValue::Object(out);
        if having.eval(&row) {
            Ok(vec![row])
        } else {
            Ok(vec![])
        }
    } else {
        // Group by the listed columns.
        let mut buckets: HashMap<Vec<String>, Vec<JsonValue>> = HashMap::new();
        let mut order: Vec<Vec<String>> = Vec::new();
        for row in &rows {
            let key: Vec<String> = groups
                .iter()
                .map(|g| lookup_col(row, g).map(|v| v.to_string()).unwrap_or_default())
                .collect();
            if !buckets.contains_key(&key) {
                order.push(key.clone());
            }
            buckets.entry(key).or_default().push(row.clone());
        }

        let mut result = Vec::new();
        for key in &order {
            let group_rows = buckets.get(key).unwrap();
            let mut out = serde_json::Map::new();
            // Include the GROUP BY columns.
            for g in groups.iter() {
                if let Some(v) = group_rows.first().and_then(|r| lookup_col(r, g)) {
                    out.insert(g.clone(), v);
                }
            }
            // Compute each aggregate.
            for a in &aggregates {
                out.insert(aggregate_output_name(a), compute_aggregate(a, group_rows));
            }
            // Resolve any bare aggregate references in HAVING that weren't
            // already computed as SELECT items.
            resolve_having_aggregates(having, group_rows, &mut out);
            let row = JsonValue::Object(out);
            if having.eval(&row) {
                result.push(row);
            }
        }
        Ok(result)
    }
}

/// Walk the HAVING expression tree, find every column reference that looks
/// like an aggregate function call (e.g. `COUNT(*)`, `SUM(salary)`,
/// `AVG(salary)`), and — if it isn't already present in `out` — compute
/// the aggregate from `group_rows` and insert it.
///
/// This is what makes `HAVING COUNT(*) > 5` work even when the SELECT list
/// doesn't include `COUNT(*)` (or aliases it to something else).
fn resolve_having_aggregates(
    having: &WhereExpr,
    group_rows: &[JsonValue],
    out: &mut serde_json::Map<String, JsonValue>,
) {
    for name in collect_aggregate_refs(having) {
        if out.contains_key(&name) {
            continue;
        }
        if let Some(agg) = parse_aggregate_from_name(&name) {
            out.insert(name, compute_aggregate(&agg, group_rows));
        }
    }
}

/// Collect every column reference in a WhereExpr tree that looks like an
/// aggregate function call (matches `^(COUNT|SUM|AVG|MIN|MAX)\(...\)$`).
fn collect_aggregate_refs(expr: &WhereExpr) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    collect_aggregate_refs_inner(expr, &mut out);
    out
}

fn collect_aggregate_refs_inner(expr: &WhereExpr, out: &mut Vec<String>) {
    match expr {
        WhereExpr::True | WhereExpr::Subquery { .. } => {}
        WhereExpr::And(a, b) | WhereExpr::Or(a, b) => {
            collect_aggregate_refs_inner(a, out);
            collect_aggregate_refs_inner(b, out);
        }
        WhereExpr::Not(e) => collect_aggregate_refs_inner(e, out),
        WhereExpr::Compare { col, .. }
        | WhereExpr::In { col, .. }
        | WhereExpr::Like { col, .. }
        | WhereExpr::IsNull { col, .. } => {
            if is_aggregate_name(col) && !out.iter().any(|s| s == col) {
                out.push(col.clone());
            }
        }
    }
}

/// Check whether `s` looks like an aggregate function call name, e.g.
/// `COUNT(*)`, `SUM(salary)`, `AVG(amount)`. Case-insensitive on the
/// function name.
fn is_aggregate_name(s: &str) -> bool {
    let upper = s.to_uppercase();
    let prefixes = ["COUNT(", "SUM(", "AVG(", "MIN(", "MAX("];
    upper.ends_with(')') && prefixes.iter().any(|p| upper.starts_with(p))
}

/// Parse a canonical aggregate name like `COUNT(*)` or `SUM(salary)` back
/// into an `AggregateExpr` so it can be passed to `compute_aggregate`.
fn parse_aggregate_from_name(name: &str) -> Option<AggregateExpr> {
    let paren_pos = name.find('(')?;
    let close_pos = name.rfind(')')?;
    if close_pos <= paren_pos {
        return None;
    }
    let func_str = name[..paren_pos].trim().to_uppercase();
    let func = match func_str.as_str() {
        "COUNT" => AggregateFunc::Count,
        "SUM" => AggregateFunc::Sum,
        "AVG" => AggregateFunc::Avg,
        "MIN" => AggregateFunc::Min,
        "MAX" => AggregateFunc::Max,
        _ => return None,
    };
    let arg_str = name[paren_pos + 1..close_pos].trim();
    let arg = if arg_str == "*" || arg_str.is_empty() {
        None
    } else {
        Some(arg_str.to_string())
    };
    Some(AggregateExpr { func, arg, alias: None })
}

fn compute_aggregate(a: &AggregateExpr, rows: &[JsonValue]) -> JsonValue {
    match a.func {
        AggregateFunc::Count => {
            if a.arg.is_none() {
                return JsonValue::Number(serde_json::Number::from(rows.len() as i64));
            }
            let arg = a.arg.as_ref().unwrap();
            let n = rows
                .iter()
                .filter(|r| {
                    let v = lookup_col(r, arg);
                    v.is_some() && v != Some(JsonValue::Null)
                })
                .count();
            JsonValue::Number(serde_json::Number::from(n as i64))
        }
        AggregateFunc::Sum => {
            let arg = a.arg.as_ref().unwrap();
            let mut sum = 0.0f64;
            let mut is_int = true;
            for r in rows {
                if let Some(v) = lookup_col(r, arg) {
                    if let Some(i) = v.as_i64() {
                        sum += i as f64;
                    } else if let Some(f) = v.as_f64() {
                        sum += f;
                        is_int = false;
                    }
                }
            }
            if is_int {
                JsonValue::Number(serde_json::Number::from(sum as i64))
            } else {
                serde_json::Number::from_f64(sum)
                    .map(JsonValue::Number)
                    .unwrap_or(JsonValue::Null)
            }
        }
        AggregateFunc::Avg => {
            let arg = a.arg.as_ref().unwrap();
            let mut sum = 0.0f64;
            let mut n = 0u64;
            for r in rows {
                if let Some(v) = lookup_col(r, arg) {
                    if let Some(i) = v.as_i64() {
                        sum += i as f64;
                        n += 1;
                    } else if let Some(f) = v.as_f64() {
                        sum += f;
                        n += 1;
                    }
                }
            }
            if n == 0 {
                JsonValue::Null
            } else {
                serde_json::Number::from_f64(sum / n as f64)
                    .map(JsonValue::Number)
                    .unwrap_or(JsonValue::Null)
            }
        }
        AggregateFunc::Min => {
            let arg = a.arg.as_ref().unwrap();
            let mut best: Option<f64> = None;
            for r in rows {
                if let Some(v) = lookup_col(r, arg) {
                    if let Some(f) = v.as_f64() {
                        best = Some(best.map(|b| b.min(f)).unwrap_or(f));
                    }
                }
            }
            best.map(|f| serde_json::Number::from_f64(f).map(JsonValue::Number).unwrap_or(JsonValue::Null))
                .unwrap_or(JsonValue::Null)
        }
        AggregateFunc::Max => {
            let arg = a.arg.as_ref().unwrap();
            let mut best: Option<f64> = None;
            for r in rows {
                if let Some(v) = lookup_col(r, arg) {
                    if let Some(f) = v.as_f64() {
                        best = Some(best.map(|b| b.max(f)).unwrap_or(f));
                    }
                }
            }
            best.map(|f| serde_json::Number::from_f64(f).map(JsonValue::Number).unwrap_or(JsonValue::Null))
                .unwrap_or(JsonValue::Null)
        }
    }
}

// ---------------------------------------------------------------------------
// ORDER BY
// ---------------------------------------------------------------------------

fn apply_order_by(rows: &mut [JsonValue], orders: &[OrderByItem]) {
    rows.sort_by(|a, b| {
        for ord in orders {
            let av = lookup_col(a, &ord.col);
            let bv = lookup_col(b, &ord.col);
            let ord_val = cmp_json_values(av.as_ref(), bv.as_ref());
            if ord_val != std::cmp::Ordering::Equal {
                return if ord.desc { ord_val.reverse() } else { ord_val };
            }
        }
        std::cmp::Ordering::Equal
    });
}

fn cmp_json_values(a: Option<&JsonValue>, b: Option<&JsonValue>) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a, b) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(av), Some(bv)) => {
            if let (Some(an), Some(bn)) = (av.as_f64(), bv.as_f64()) {
                return an.partial_cmp(&bn).unwrap_or(Ordering::Equal);
            }
            if let (Some(as_), Some(bs)) = (av.as_str(), bv.as_str()) {
                return as_.cmp(bs);
            }
            av.to_string().cmp(&bv.to_string())
        }
    }
}

// ---------------------------------------------------------------------------
// JOIN execution
// ---------------------------------------------------------------------------

fn execute_join(
    left_rows: Vec<JsonValue>,
    right_rows: Vec<JsonValue>,
    on: &[(String, String)],
    join_type: &JoinType,
) -> Vec<JsonValue> {
    // Build an index on right_rows: composite key → list of right rows.
    let mut right_index: HashMap<String, Vec<&JsonValue>> = HashMap::new();
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
        let mut composite_key = String::new();
        for (left_col, _) in on {
            let val = left_row.get(left_col).map(|v| v.to_string()).unwrap_or_default();
            if !composite_key.is_empty() { composite_key.push('\x1f'); }
            composite_key.push_str(&val);
        }

        match right_index.get(&composite_key) {
            Some(matches) => {
                for right_row in matches {
                    let mut merged = left_row.clone();
                    if let (Some(merged_obj), Some(right_obj)) =
                        (merged.as_object_mut(), right_row.as_object())
                    {
                        for (k, v) in right_obj {
                            merged_obj.insert(k.clone(), v.clone());
                        }
                    }
                    result.push(merged);
                }
            }
            None => {
                if *join_type == JoinType::Left || *join_type == JoinType::FullOuter {
                    let mut merged = left_row.clone();
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

    // For RIGHT/FULL OUTER: include unmatched right rows with null left cols.
    if *join_type == JoinType::Right || *join_type == JoinType::FullOuter {
        let left_keys: std::collections::HashSet<String> = left_rows
            .iter()
            .map(|l| {
                let mut k = String::new();
                for (left_col, _) in on {
                    let v = l.get(left_col).map(|v| v.to_string()).unwrap_or_default();
                    if !k.is_empty() { k.push('\x1f'); }
                    k.push_str(&v);
                }
                k
            })
            .collect();
        for right_row in &right_rows {
            let mut composite_key = String::new();
            for (_, right_col) in on {
                let val = right_row.get(right_col).map(|v| v.to_string()).unwrap_or_default();
                if !composite_key.is_empty() { composite_key.push('\x1f'); }
                composite_key.push_str(&val);
            }
            if !left_keys.contains(&composite_key) {
                let mut merged = right_row.clone();
                if let Some(left_row) = left_rows.first() {
                    if let Some(left_obj) = left_row.as_object() {
                        if let Some(merged_obj) = merged.as_object_mut() {
                            for k in left_obj.keys() {
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

    // CROSS JOIN: emit the cartesian product (on is empty).
    if *join_type == JoinType::Cross && on.is_empty() {
        let mut crossed: Vec<JsonValue> = Vec::new();
        for l in &left_rows {
            for r in &right_rows {
                let mut merged = l.clone();
                if let (Some(mo), Some(ro)) = (merged.as_object_mut(), r.as_object()) {
                    for (k, v) in ro {
                        mo.insert(k.clone(), v.clone());
                    }
                }
                crossed.push(merged);
            }
        }
        return crossed;
    }

    result
}

// ---------------------------------------------------------------------------
// Table reading
// ---------------------------------------------------------------------------

fn read_table_rows(
    storage: &UnifiedStorage,
    table: &TableRef,
    projection: Option<&HashSet<String>>,
) -> Result<Vec<JsonValue>, String> {
    match table {
        TableRef::Collection(name) => {
            let kc = vec!["_rowid".to_string()];
            // Convert HashSet<String> → HashSet<&str> for codec layer
            let proj_ref: Option<HashSet<&str>> = projection.map(|s| {
                s.iter().map(|c| c.as_str()).collect()
            });
            let all_rows = read_collection_as_json_rows(
                storage, name, &kc, proj_ref.as_ref(),
            )?;
            Ok(crdt_merge_rows(all_rows))
        }
        TableRef::File(path) => read_file_rows(path),
    }
}

fn read_file_rows(path: &str) -> Result<Vec<JsonValue>, String> {
    // Parquet is a binary format — handle it before the read_to_string
    // path used for the text-based formats (NDJSON / JSON / CSV / TSV),
    // which would otherwise fail on non-UTF-8 bytes.
    if path.ends_with(".parquet") {
        return read_parquet_file(path);
    }

    let content = std::fs::read_to_string(path)
        .map_err(|e| format!("Failed to read file '{}': {}", path, e))?;

    if path.ends_with(".ndjson") {
        let mut rows = Vec::new();
        for line in content.lines() {
            if line.trim().is_empty() { continue; }
            let row: JsonValue = serde_json::from_str(line)
                .map_err(|e| format!("Failed to parse NDJSON line: {}", e))?;
            rows.push(row);
        }
        Ok(rows)
    } else if path.ends_with(".json") {
        let parsed: JsonValue = serde_json::from_str(&content)
            .map_err(|e| format!("Failed to parse JSON: {}", e))?;
        match parsed {
            JsonValue::Array(arr) => Ok(arr),
            JsonValue::Object(obj) => Ok(vec![JsonValue::Object(obj)]),
            _ => Err("JSON file must contain an array or object".to_string()),
        }
    } else if path.ends_with(".csv") || path.ends_with(".tsv") {
        let delimiter = if path.ends_with(".tsv") { '\t' } else { ',' };
        let mut rows = Vec::new();

        // Use a proper CSV parser that handles quoted fields, embedded delimiters,
        // and embedded newlines (RFC 4180 compliant).
        let records = parse_csv(&content, delimiter);
        if records.is_empty() {
            return Err("CSV file is empty".to_string());
        }

        let headers: Vec<String> = records[0].to_vec();

        for record in records.iter().skip(1) {
            if record.is_empty() { continue; }
            let mut obj = serde_json::Map::new();
            for (i, header) in headers.iter().enumerate() {
                let val_str = record.get(i).map_or("", |v| v.as_str());
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
    } else {
        Err(format!("Unsupported file format: '{}'", path))
    }
}

/// Read a Parquet file into a Vec of JSON row objects.
///
/// Opens the file with `ParquetRecordBatchReaderBuilder`, iterates over the
/// record batches, and converts each row to a `serde_json::Value` object
/// keyed by column names.
///
/// Supported Arrow data types:
///   - Boolean
///   - Int8 / Int16 / Int32 / Int64
///   - UInt8 / UInt16 / UInt32 / UInt64
///   - Float32 / Float64
///   - Utf8 / LargeUtf8
///   - Date32 / Date64            (rendered as ISO 8601 date strings)
///   - Timestamp(Second | Millisecond | Microsecond | Nanosecond, _)
///     (rendered as ISO 8601 datetime strings)
///   - Null
///
/// Null cells (regardless of the underlying type) are returned as
/// `JsonValue::Null`.
fn read_parquet_file(path: &str) -> Result<Vec<JsonValue>, String> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::fs::File;

    let file = File::open(path)
        .map_err(|e| format!("Failed to open parquet file '{}': {}", path, e))?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| format!("Failed to open parquet reader for '{}': {}", path, e))?;
    let reader = builder
        .build()
        .map_err(|e| format!("Failed to build parquet reader for '{}': {}", path, e))?;

    let mut rows: Vec<JsonValue> = Vec::new();
    for batch in reader {
        let batch = batch
            .map_err(|e| format!("Failed to read parquet record batch: {}", e))?;
        let schema = batch.schema();
        let n_rows = batch.num_rows();
        let columns = batch.columns();

        for row_idx in 0..n_rows {
            let mut obj = serde_json::Map::new();
            for (col_idx, array) in columns.iter().enumerate() {
                let field = schema.field(col_idx);
                let name = field.name();
                let val = arrow_cell_to_json(
                    array.as_ref(),
                    row_idx,
                    field.data_type(),
                );
                obj.insert(name.clone(), val);
            }
            rows.push(JsonValue::Object(obj));
        }
    }

    Ok(rows)
}

/// Convert a single cell of an Arrow array to a JSON value.
///
/// `array` is the column array, `idx` is the row index, `dt` is the
/// arrow DataType (passed in to avoid re-fetching from the array).
fn arrow_cell_to_json(
    array: &dyn arrow::array::Array,
    idx: usize,
    dt: &arrow::datatypes::DataType,
) -> JsonValue {
    use arrow::array::{
        BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array,
        Int8Array, Int16Array, Int32Array, Int64Array, LargeStringArray, StringArray,
        TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
        TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
    };
    use arrow::datatypes::TimeUnit;

    if array.is_null(idx) {
        return JsonValue::Null;
    }
    match dt {
        arrow::datatypes::DataType::Boolean => {
            let a = array.as_any().downcast_ref::<BooleanArray>().expect("BooleanArray");
            JsonValue::Bool(a.value(idx))
        }
        arrow::datatypes::DataType::Int8 => {
            let a = array.as_any().downcast_ref::<Int8Array>().expect("Int8Array");
            JsonValue::Number(serde_json::Number::from(a.value(idx) as i64))
        }
        arrow::datatypes::DataType::Int16 => {
            let a = array.as_any().downcast_ref::<Int16Array>().expect("Int16Array");
            JsonValue::Number(serde_json::Number::from(a.value(idx) as i64))
        }
        arrow::datatypes::DataType::Int32 => {
            let a = array.as_any().downcast_ref::<Int32Array>().expect("Int32Array");
            JsonValue::Number(serde_json::Number::from(a.value(idx) as i64))
        }
        arrow::datatypes::DataType::Int64 => {
            let a = array.as_any().downcast_ref::<Int64Array>().expect("Int64Array");
            JsonValue::Number(serde_json::Number::from(a.value(idx)))
        }
        arrow::datatypes::DataType::UInt8 => {
            let a = array.as_any().downcast_ref::<UInt8Array>().expect("UInt8Array");
            JsonValue::Number(serde_json::Number::from(a.value(idx) as u64))
        }
        arrow::datatypes::DataType::UInt16 => {
            let a = array.as_any().downcast_ref::<UInt16Array>().expect("UInt16Array");
            JsonValue::Number(serde_json::Number::from(a.value(idx) as u64))
        }
        arrow::datatypes::DataType::UInt32 => {
            let a = array.as_any().downcast_ref::<UInt32Array>().expect("UInt32Array");
            JsonValue::Number(serde_json::Number::from(a.value(idx) as u64))
        }
        arrow::datatypes::DataType::UInt64 => {
            let a = array.as_any().downcast_ref::<UInt64Array>().expect("UInt64Array");
            JsonValue::Number(serde_json::Number::from(a.value(idx)))
        }
        arrow::datatypes::DataType::Float32 => {
            let a = array.as_any().downcast_ref::<Float32Array>().expect("Float32Array");
            serde_json::Number::from_f64(a.value(idx) as f64)
                .map(JsonValue::Number)
                .unwrap_or(JsonValue::Null)
        }
        arrow::datatypes::DataType::Float64 => {
            let a = array.as_any().downcast_ref::<Float64Array>().expect("Float64Array");
            serde_json::Number::from_f64(a.value(idx))
                .map(JsonValue::Number)
                .unwrap_or(JsonValue::Null)
        }
        arrow::datatypes::DataType::Utf8 => {
            let a = array.as_any().downcast_ref::<StringArray>().expect("StringArray");
            JsonValue::String(a.value(idx).to_string())
        }
        arrow::datatypes::DataType::LargeUtf8 => {
            let a = array.as_any().downcast_ref::<LargeStringArray>().expect("LargeStringArray");
            JsonValue::String(a.value(idx).to_string())
        }
        arrow::datatypes::DataType::Date32 => {
            let a = array.as_any().downcast_ref::<Date32Array>().expect("Date32Array");
            let days = a.value(idx);
            match chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
                .and_then(|epoch| epoch.checked_add_signed(chrono::Duration::days(days as i64)))
            {
                Some(date) => JsonValue::String(date.format("%Y-%m-%d").to_string()),
                None => JsonValue::Null,
            }
        }
        arrow::datatypes::DataType::Date64 => {
            let a = array.as_any().downcast_ref::<Date64Array>().expect("Date64Array");
            let ms = a.value(idx);
            match chrono::DateTime::<chrono::Utc>::from_timestamp_millis(ms) {
                Some(dt) => JsonValue::String(dt.format("%Y-%m-%d").to_string()),
                None => JsonValue::Null,
            }
        }
        arrow::datatypes::DataType::Timestamp(unit, _tz) => {
            match unit {
                TimeUnit::Second => {
                    let a = array.as_any().downcast_ref::<TimestampSecondArray>()
                        .expect("TimestampSecondArray");
                    let s = a.value(idx);
                    match chrono::DateTime::<chrono::Utc>::from_timestamp(s, 0) {
                        Some(dt) => JsonValue::String(dt.format("%Y-%m-%dT%H:%M:%S").to_string()),
                        None => JsonValue::Null,
                    }
                }
                TimeUnit::Millisecond => {
                    let a = array.as_any().downcast_ref::<TimestampMillisecondArray>()
                        .expect("TimestampMillisecondArray");
                    let ms = a.value(idx);
                    match chrono::DateTime::<chrono::Utc>::from_timestamp_millis(ms) {
                        Some(dt) => JsonValue::String(dt.format("%Y-%m-%dT%H:%M:%S").to_string()),
                        None => JsonValue::Null,
                    }
                }
                TimeUnit::Microsecond => {
                    let a = array.as_any().downcast_ref::<TimestampMicrosecondArray>()
                        .expect("TimestampMicrosecondArray");
                    let us = a.value(idx);
                    let secs = us / 1_000_000;
                    let nsecs = ((us % 1_000_000) * 1_000) as u32;
                    match chrono::DateTime::<chrono::Utc>::from_timestamp(secs, nsecs) {
                        Some(dt) => JsonValue::String(dt.format("%Y-%m-%dT%H:%M:%S").to_string()),
                        None => JsonValue::Null,
                    }
                }
                TimeUnit::Nanosecond => {
                    let a = array.as_any().downcast_ref::<TimestampNanosecondArray>()
                        .expect("TimestampNanosecondArray");
                    let ns = a.value(idx);
                    let secs = ns / 1_000_000_000;
                    let nsecs = (ns % 1_000_000_000) as u32;
                    match chrono::DateTime::<chrono::Utc>::from_timestamp(secs, nsecs) {
                        Some(dt) => JsonValue::String(dt.format("%Y-%m-%dT%H:%M:%S").to_string()),
                        None => JsonValue::Null,
                    }
                }
            }
        }
        arrow::datatypes::DataType::Null => JsonValue::Null,
        // Unsupported types fall back to Null rather than failing the whole
        // read. This keeps a query usable even when the parquet file has a
        // column type the engine doesn't fully understand.
        _ => JsonValue::Null,
    }
}

/// Strip a leading "alias." prefix from a column reference.
/// E.g., "u.name" → "name", "id" → "id".
fn strip_alias_prefix(col: &str) -> &str {
    col.rsplit('.').next().unwrap_or(col)
}

/// Extract aggregate function argument from a string like "SUM(amount)" or
/// "AVG(salary)". Returns None for non-aggregate strings.
fn extract_aggregate_arg(col: &str) -> Option<&str> {
    let col = col.trim();
    let open = col.find('(')?;
    let close = col.rfind(')')?;
    if close <= open + 1 { return None; }
    let inner = &col[open + 1..close];
    let inner = inner.trim();
    if inner == "*" { return None; }
    // Strip alias prefix from the arg
    Some(strip_alias_prefix(inner))
}

/// Extract the set of column names that must be present in decoded rows
/// for a SELECT to execute correctly. Returns `None` if all columns are
/// needed (SELECT * or mixed Star + named).
///
/// Always includes CRDT metadata columns (`_rowid`, `_version`, `_deleted`)
/// because `crdt_merge_rows` requires them for dedup + tombstone filtering.
fn extract_required_columns(
    select_items: &[SelectItem],
    where_expr: &WhereExpr,
    groups: &[String],
    orders: &[OrderByItem],
    join_on_cols: &[String],
    having: &WhereExpr,
) -> Option<HashSet<String>> {
    // SELECT * → read everything
    if select_items.len() == 1 && matches!(select_items[0], SelectItem::Star) {
        return None;
    }

    let mut needed: HashSet<String> = HashSet::new();

    // CRDT metadata (always required for merge/dedup)
    needed.insert("_rowid".into());
    needed.insert("_version".into());
    needed.insert("_deleted".into());

    for item in select_items {
        match item {
            SelectItem::Star => return None, // mixed * → bail, read all
            SelectItem::Column(c) => {
                needed.insert(strip_alias_prefix(c).to_string());
            }
            SelectItem::Aggregate(a) => {
                if let Some(arg) = &a.arg {
                    needed.insert(strip_alias_prefix(arg).to_string());
                }
            }
        }
    }

    // WHERE columns
    where_expr.collect_columns(&mut needed);

    // GROUP BY columns
    for g in groups {
        needed.insert(strip_alias_prefix(g).to_string());
    }

    // ORDER BY columns
    for o in orders {
        needed.insert(strip_alias_prefix(&o.col).to_string());
    }

    // JOIN ON columns (left side)
    for c in join_on_cols {
        needed.insert(strip_alias_prefix(c).to_string());
    }

    // HAVING: extract aggregate arguments (e.g., "SUM(amount)" → "amount")
    // HAVING uses WhereExpr::Compare { col: "SUM(amount)", ... } for
    // aggregate predicates. We need the raw column for the aggregate to work.
    extract_having_columns(having, &mut needed);

    Some(needed)
}

/// Recursively extract column names from a HAVING expression.
/// For aggregate comparisons like "SUM(amount) < 1000", parses the
/// aggregate function argument ("amount") and adds it to the set.
fn extract_having_columns(expr: &WhereExpr, needed: &mut HashSet<String>) {
    match expr {
        WhereExpr::True => {}
        WhereExpr::Compare { col, .. }
        | WhereExpr::In { col, .. }
        | WhereExpr::Like { col, .. }
        | WhereExpr::IsNull { col, .. }
        | WhereExpr::Subquery { col, .. } => {
            // Try to extract aggregate arg (e.g., "SUM(amount)" → "amount")
            if let Some(arg) = extract_aggregate_arg(col) {
                needed.insert(arg.to_string());
            } else {
                // Plain column reference
                needed.insert(strip_alias_prefix(col).to_string());
            }
        }
        WhereExpr::And(a, b) | WhereExpr::Or(a, b) => {
            extract_having_columns(a, needed);
            extract_having_columns(b, needed);
        }
        WhereExpr::Not(e) => extract_having_columns(e, needed),
    }
}

/// Read all rows from a collection as (rowid, JSON row) pairs.
///
/// Reads HEAD + shards, decodes PND2 blobs, converts each row to a JSON
/// object. Sequential CRDT merge is then applied by the caller.
///
/// Uses the optimized read path: slab-aware range reads, range coalescing,
/// and parallel blob reads (see `read::read_all_row_groups`). This avoids
/// full blob GETs when row groups are stored in slabs, reducing S3
/// bandwidth by up to 100x for selective queries.
fn read_collection_as_json_rows(
    storage: &UnifiedStorage,
    collection: &str,
    key_fields: &[String],
    projection: Option<&std::collections::HashSet<&str>>,
) -> Result<Vec<(String, JsonValue)>, String> {
    let kernel = storage.kernel();
    let mut rows: Vec<(String, JsonValue)> = Vec::new();

    let active = storage.get_active_branch(collection);

    // --- Read HEAD data (optimized: slab-aware range reads) ---
    // Uses read_all_row_groups which handles:
    //   - PondPack transparently
    //   - Slab-backed RGs via range reads (not full blob GETs)
    //   - Range coalescing (adjacent RGs → single GET)
    //   - Parallel reads (bounded thread pool)
    //   - PSLB v2 zstd decompression
    // Previous impl did kernel.read_blob(&rg.blob_hash) per RG, which
    // fetched the entire slab (up to 128 MB) for each RG — 100x waste.
    match pond_storage::read::read_all_row_groups(kernel, collection, &active) {
        Ok(rg_blobs) => {
            for blob_data in rg_blobs {
                let cols = pnd2_decode_projected(&blob_data, projection)
                    .map_err(|e| format!("Failed to decode PND2: {}", e))?;
                rows.extend(decode_cols_to_rows(&cols, key_fields));
            }
        }
        Err(_) => { /* no HEAD data — proceed to shards */ }
    }

    // --- Read shard data (CRDT) ---
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

fn decode_cols_to_rows(cols: &[PondColumn], key_fields: &[String]) -> Vec<(String, JsonValue)> {
    let mut rows = Vec::new();
    let n_rows = cols.first().map(|c| c.n_values).unwrap_or(0);

    for row_idx in 0..n_rows {
        let mut row_obj = serde_json::Map::new();
        for col in cols {
            let name = col.name.to_string_lossy().to_string();
            let val = match col.vtype {
                VT_INT64 => col
                    .i64_data
                    .get(row_idx)
                    .map(|v| JsonValue::Number(serde_json::Number::from(*v)))
                    .unwrap_or(JsonValue::Null),
                VT_FLOAT64 => col
                    .f64_data
                    .get(row_idx)
                    .and_then(|v| serde_json::Number::from_f64(*v))
                    .map(JsonValue::Number)
                    .unwrap_or(JsonValue::Null),
                VT_STRING => col
                    .str_data
                    .get(row_idx)
                    .map(|v| JsonValue::String(v.to_string_lossy().to_string()))
                    .unwrap_or(JsonValue::Null),
                VT_BINARY => col
                    .bin_data
                    .get(row_idx)
                    .map(|b| {
                        JsonValue::String(format!("__bin_b64__:{}", simple_base64_encode(b)))
                    })
                    .unwrap_or(JsonValue::Null),
                VT_VARIANT => col
                    .str_data
                    .get(row_idx)
                    .and_then(|s| {
                        let s_str = s.to_string_lossy();
                        serde_json::from_str::<JsonValue>(&s_str).ok()
                    })
                    .unwrap_or(JsonValue::Null),
                _ => JsonValue::Null,
            };
            row_obj.insert(name, val);
        }
        let row = JsonValue::Object(row_obj);
        let rowid = determine_rowid(&row, key_fields);
        rows.push((rowid, row));
    }
    rows
}

fn determine_rowid(row: &JsonValue, key_fields: &[String]) -> String {
    if let Some(r) = row.get("_rowid").and_then(|v| v.as_str()) {
        return r.to_string();
    }
    for kf in key_fields {
        if let Some(r) = row.get(kf).and_then(|v| v.as_str()) {
            return r.to_string();
        }
        if let Some(r) = row.get(kf).and_then(|v| v.as_i64()) {
            return r.to_string();
        }
    }
    if let Some(r) = row.get("_key").and_then(|v| v.as_str()) {
        return r.to_string();
    }
    if let Some(r) = row.get("id").and_then(|v| v.as_str()) {
        return r.to_string();
    }
    if let Some(r) = row.get("id").and_then(|v| v.as_i64()) {
        return r.to_string();
    }
    // Fallback: hash of the row's JSON.
    let s = row.to_string();
    let mut h: u64 = 0xcbf29ce484222325;
    for b in s.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    format!("auto_{:x}", h)
}

/// Sequential CRDT merge — dedup by _rowid, latest _version wins.
fn crdt_merge_rows(rows: Vec<(String, JsonValue)>) -> Vec<JsonValue> {
    let mut order: Vec<String> = Vec::new();
    let mut latest: HashMap<String, (String, JsonValue)> = HashMap::new();
    let mut no_rowid: Vec<JsonValue> = Vec::new();

    for (rowid, row) in rows {
        let effective_rowid = row
            .get("_rowid")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or(rowid);

        let is_deleted = row
            .get("_deleted")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let version = row
            .get("_version")
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
            let is_deleted = row
                .get("_deleted")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            if !is_deleted {
                result.push(row.clone());
            }
        }
    }
    result
}

// ---------------------------------------------------------------------------
// Writes
// ---------------------------------------------------------------------------

fn execute_insert(
    storage: &UnifiedStorage,
    collection: &str,
    columns: &[String],
    rows: &[Vec<JsonValue>],
) -> Result<SqlResult, String> {
    let kernel = storage.kernel();
    let active = storage.get_active_branch(collection);

    // Build TypedColumns from the rows.
    let typed_cols = build_typed_columns(columns, rows);

    let col_refs: Vec<(&str, TypedColumn)> = typed_cols
        .iter()
        .map(|(n, c)| (n.as_str(), c.clone()))
        .collect();

    let commit_hash = storage_write::write_rows(kernel, collection, &active, &col_refs, "INSERT")?;
    Ok(SqlResult::commit(&commit_hash))
}

fn execute_update(
    storage: &UnifiedStorage,
    collection: &str,
    sets: &[(String, JsonValue)],
    where_expr: &WhereExpr,
) -> Result<SqlResult, String> {
    let kernel = storage.kernel();
    let active = storage.get_active_branch(collection);

    let kc = vec!["_rowid".to_string()];
    let all_rows = read_collection_as_json_rows(storage, collection, &kc, None)?;

    let mut matched: Vec<JsonValue> = Vec::new();
    for (_rowid, row) in &all_rows {
        if where_expr.eval(row) {
            let mut updated = row.clone();
            if let Some(obj) = updated.as_object_mut() {
                for (col, val) in sets {
                    obj.insert(col.clone(), val.clone());
                }
                // Bump _version.
                let mut hlc = HLC::new();
                obj.insert("_version".to_string(), json!(hlc.tick()));
            }
            matched.push(updated);
        }
    }

    let count = matched.len();
    if count == 0 {
        return Ok(SqlResult::status("updated", 0));
    }

    // Observe existing versions, then write as a CRDT upsert shard.
    let mut hlc = HLC::new();
    for (_, row) in &all_rows {
        if let Some(v) = row.get("_version").and_then(|v| v.as_str()) {
            hlc.observe(v);
        }
    }
    let shard_name = format!("update_{}", chrono_like_id());
    shard::upsert_shard(
        kernel, collection, &active, &shard_name,
        &matched, Some("_rowid"), &mut hlc,
    )?;

    Ok(SqlResult::status("updated", count))
}

fn execute_delete(
    storage: &UnifiedStorage,
    collection: &str,
    where_expr: &WhereExpr,
) -> Result<SqlResult, String> {
    let kernel = storage.kernel();
    let active = storage.get_active_branch(collection);

    let kc = vec!["_rowid".to_string()];
    let all_rows = read_collection_as_json_rows(storage, collection, &kc, None)?;

    let mut tombstones: Vec<String> = Vec::new();
    for (rowid, row) in &all_rows {
        if where_expr.eval(row) {
            if let Some(r) = row.get("_rowid").and_then(|v| v.as_str()) {
                tombstones.push(r.to_string());
            } else if !rowid.is_empty() && !rowid.starts_with("auto_") {
                tombstones.push(rowid.clone());
            }
        }
    }

    let count = tombstones.len();
    if count == 0 {
        return Ok(SqlResult::status("deleted", 0));
    }

    let mut hlc = HLC::new();
    for (_, row) in &all_rows {
        if let Some(v) = row.get("_version").and_then(|v| v.as_str()) {
            hlc.observe(v);
        }
    }
    let shard_name = format!("delete_{}", chrono_like_id());
    shard::delete_shard(
        kernel, collection, &active, &shard_name,
        &tombstones, Some("_rowid"), &mut hlc,
    )?;

    Ok(SqlResult::status("deleted", count))
}

fn execute_merge(
    storage: &UnifiedStorage,
    target: &str,
    source_rows: &[JsonValue],
    match_keys: &[(String, String)],
    when_matched: &MergeAction,
    when_not_matched: &MergeAction,
) -> Result<SqlResult, String> {
    let kernel = storage.kernel();
    let active = storage.get_active_branch(target);

    let kc = vec!["_rowid".to_string()];
    let target_rows = read_collection_as_json_rows(storage, target, &kc, None)?;

    // Build an index of target rows by the composite key built from the
    // FIRST match_key (target side). Multi-key match isn't fully supported
    // here — kept simple for the v1 port.
    let first_key = match_keys.first()
        .ok_or_else(|| "MERGE requires at least one match key".to_string())?;
    let target_key_col = &first_key.0;
    let source_key_col = &first_key.1;

    let mut target_index: HashMap<String, (String, JsonValue)> = HashMap::new();
    for (rowid, row) in &target_rows {
        let key = lookup_col(row, target_key_col)
            .map(|v| v.to_string())
            .unwrap_or_default();
        target_index.insert(key, (rowid.clone(), row.clone()));
    }

    let mut to_upsert: Vec<JsonValue> = Vec::new();
    let mut to_delete: Vec<String> = Vec::new();
    let mut inserted = 0usize;
    let mut updated = 0usize;
    let mut deleted = 0usize;
    let mut skipped = 0usize;
    let mut matched_total = 0usize;

    let mut hlc = HLC::new();
    for (_, row) in &target_rows {
        if let Some(v) = row.get("_version").and_then(|v| v.as_str()) {
            hlc.observe(v);
        }
    }

    for source in source_rows {
        let key = lookup_col(source, source_key_col)
            .map(|v| v.to_string())
            .unwrap_or_default();
        if let Some((rowid, target_row)) = target_index.get(&key) {
            matched_total += 1;
            match when_matched {
                MergeAction::Update => {
                    let mut merged = target_row.clone();
                    if let Some(obj) = merged.as_object_mut() {
                        if let Some(src_obj) = source.as_object() {
                            for (k, v) in src_obj {
                                obj.insert(k.clone(), v.clone());
                            }
                        }
                        obj.insert("_version".to_string(), json!(hlc.tick()));
                    }
                    to_upsert.push(merged);
                    updated += 1;
                }
                MergeAction::Delete => {
                    if let Some(r) = target_row.get("_rowid").and_then(|v| v.as_str()) {
                        to_delete.push(r.to_string());
                    } else if !rowid.is_empty() && !rowid.starts_with("auto_") {
                        to_delete.push(rowid.clone());
                    }
                    deleted += 1;
                }
                MergeAction::Skip => {
                    skipped += 1;
                }
                MergeAction::Insert => {
                    // INSERT on match is not valid — treat as skip.
                    skipped += 1;
                }
            }
        } else {
            match when_not_matched {
                MergeAction::Insert => {
                    let mut new_row = source.clone();
                    if let Some(obj) = new_row.as_object_mut() {
                        if obj.get("_rowid").is_none() {
                            obj.insert("_rowid".to_string(), json!(uuidv7()));
                        }
                        obj.insert("_version".to_string(), json!(hlc.tick()));
                        obj.insert("_deleted".to_string(), json!(false));
                    }
                    to_upsert.push(new_row);
                    inserted += 1;
                }
                MergeAction::Skip | MergeAction::Update | MergeAction::Delete => {
                    skipped += 1;
                }
            }
        }
    }

    if !to_upsert.is_empty() {
        let shard_name = format!("merge_upsert_{}", chrono_like_id());
        shard::upsert_shard(
            kernel, target, &active, &shard_name,
            &to_upsert, Some("_rowid"), &mut hlc,
        )?;
    }
    if !to_delete.is_empty() {
        let shard_name = format!("merge_delete_{}", chrono_like_id());
        shard::delete_shard(
            kernel, target, &active, &shard_name,
            &to_delete, Some("_rowid"), &mut hlc,
        )?;
    }

    let result = SqlResult {
        columns: vec![
            "matched".to_string(),
            "updated".to_string(),
            "deleted".to_string(),
            "inserted".to_string(),
            "skipped".to_string(),
        ],
        rows: vec![json!({
            "matched": matched_total,
            "updated": updated,
            "deleted": deleted,
            "inserted": inserted,
            "skipped": skipped,
        })],
    };
    Ok(result)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a `Vec<(String, TypedColumn)>` from columnar input.
fn build_typed_columns(
    columns: &[String],
    rows: &[Vec<JsonValue>],
) -> Vec<(String, TypedColumn)> {
    let mut result = Vec::new();
    for (col_idx, col_name) in columns.iter().enumerate() {
        let mut i64_vals: Vec<i64> = Vec::with_capacity(rows.len());
        let mut f64_vals: Vec<f64> = Vec::with_capacity(rows.len());
        let mut str_vals: Vec<String> = Vec::with_capacity(rows.len());
        let mut col_type: u8 = 0; // 0=unknown, 1=i64, 2=f64, 3=str

        for row in rows {
            match row.get(col_idx) {
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
                _ => match col_type {
                    1 => i64_vals.push(0),
                    2 => f64_vals.push(0.0),
                    3 => str_vals.push(String::new()),
                    _ => { col_type = 3; str_vals.push(String::new()); }
                },
            }
        }

        let typed = match col_type {
            1 => TypedColumn::Int64(i64_vals),
            2 => TypedColumn::Float64(f64_vals),
            _ => TypedColumn::String(str_vals),
        };
        result.push((col_name.clone(), typed));
    }
    result
}

/// Tiny base64 encoder (avoids pulling in a base64 crate dependency).
fn simple_base64_encode(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] =
        b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0];
        let b1 = chunk.get(1).copied().unwrap_or(0);
        let b2 = chunk.get(2).copied().unwrap_or(0);
        let triple = ((b0 as u32) << 16) | ((b1 as u32) << 8) | (b2 as u32);
        out.push(TABLE[((triple >> 18) & 0x3F) as usize] as char);
        out.push(TABLE[((triple >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            out.push(TABLE[((triple >> 6) & 0x3F) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(TABLE[(triple & 0x3F) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

/// Generate a sortable, time-based identifier for shard names.
fn chrono_like_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{:x}", nanos)
}

/// RFC 4180 compliant CSV parser.
///
/// Handles:
///   - Quoted fields: "hello, world" → hello, world
///   - Embedded quotes: "He said ""hi""" → He said "hi"
///   - Embedded newlines: "line1\nline2" → line1\nline2
///   - Empty fields: ,, → empty string
///   - Trailing newlines: ignores empty last line
///
/// Returns a Vec of records, each record is a Vec of field strings.
fn parse_csv(content: &str, delimiter: char) -> Vec<Vec<String>> {
    let mut records: Vec<Vec<String>> = Vec::new();
    let mut current_record: Vec<String> = Vec::new();
    let mut current_field = String::new();
    let mut in_quotes = false;
    let mut chars = content.chars().peekable();

    while let Some(c) = chars.next() {
        if in_quotes {
            if c == '"' {
                // Check for escaped quote ""
                if chars.peek() == Some(&'"') {
                    chars.next(); // consume the second "
                    current_field.push('"');
                } else {
                    // End of quoted field
                    in_quotes = false;
                }
            } else {
                current_field.push(c);
            }
        } else {
            if c == '"' {
                in_quotes = true;
            } else if c == delimiter {
                current_record.push(std::mem::take(&mut current_field));
            } else if c == '\n' {
                current_record.push(std::mem::take(&mut current_field));
                // Only add non-empty records (skip trailing newline)
                if !current_record.is_empty() && !(current_record.len() == 1 && current_record[0].is_empty()) {
                    records.push(std::mem::take(&mut current_record));
                } else {
                    current_record.clear();
                }
            } else if c == '\r' {
                // Handle \r\n line endings — skip \r, let \n handle the record end
                continue;
            } else {
                current_field.push(c);
            }
        }
    }

    // Don't forget the last field/record if there's no trailing newline
    if !current_field.is_empty() || !current_record.is_empty() {
        current_record.push(current_field);
        records.push(current_record);
    }

    records
}

#[cfg(test)]
mod csv_tests {
    use super::*;

    #[test]
    fn test_parse_csv_basic() {
        let csv = "name,age,city\nAlice,30,NYC\nBob,25,SF";
        let records = parse_csv(csv, ',');
        assert_eq!(records.len(), 3);
        assert_eq!(records[0], vec!["name", "age", "city"]);
        assert_eq!(records[1], vec!["Alice", "30", "NYC"]);
        assert_eq!(records[2], vec!["Bob", "25", "SF"]);
    }

    #[test]
    fn test_parse_csv_quoted_fields() {
        let csv = "name,description\n\"Alice\",\"Hello, World\"\n\"Bob\",\"He said \"\"hi\"\"\"";
        let records = parse_csv(csv, ',');
        assert_eq!(records.len(), 3);
        assert_eq!(records[1], vec!["Alice", "Hello, World"]);
        assert_eq!(records[2], vec!["Bob", "He said \"hi\""]);
    }

    #[test]
    fn test_parse_csv_embedded_newline() {
        let csv = "name,text\n\"Alice\",\"Line 1\nLine 2\"";
        let records = parse_csv(csv, ',');
        assert_eq!(records.len(), 2);
        assert_eq!(records[1][1], "Line 1\nLine 2");
    }

    #[test]
    fn test_parse_csv_empty_fields() {
        let csv = "a,b,c\n1,,3";
        let records = parse_csv(csv, ',');
        assert_eq!(records[1], vec!["1", "", "3"]);
    }

    #[test]
    fn test_parse_csv_tsv() {
        let tsv = "name\tage\nAlice\t30";
        let records = parse_csv(tsv, '\t');
        assert_eq!(records.len(), 2);
        assert_eq!(records[1], vec!["Alice", "30"]);
    }

    #[test]
    fn test_parse_csv_trailing_newline() {
        let csv = "a,b\n1,2\n";
        let records = parse_csv(csv, ',');
        assert_eq!(records.len(), 2); // trailing newline doesn't create empty record
    }

    #[test]
    fn test_parse_csv_crlf() {
        let csv = "a,b\r\n1,2\r\n3,4";
        let records = parse_csv(csv, ',');
        assert_eq!(records.len(), 3);
        assert_eq!(records[1], vec!["1", "2"]);
    }
}
