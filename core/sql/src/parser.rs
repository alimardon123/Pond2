// SQL parser — pure Rust, no PyO3.
//
// Ported from `bindings/python/pyo3/src/sql_engine.rs` and extended with
// support for SELECT items (Star / Column / Aggregate), GROUP BY, ORDER BY,
// LIMIT/OFFSET, HAVING, RIGHT/FULL OUTER/CROSS joins, and subqueries in
// WHERE (handled by `where_clause::WhereExpr::Subquery`).
//
// Supported statements:
//
//   SELECT [items] FROM collection [alias]
//          [JOIN ... ON ...]
//          [WHERE ...]
//          [GROUP BY col1, col2 [HAVING ...]]
//          [ORDER BY col1 [ASC|DESC], ...]
//          [LIMIT n [OFFSET m]]
//   SELECT * FROM 'data.csv' WHERE ...           (file reading)
//   UPDATE collection SET col1 = val1 [WHERE ...]
//   DELETE FROM collection [WHERE ...]
//   INSERT INTO collection (col1, col2) VALUES (v1, v2), (v3, v4)
//   MERGE INTO target USING source_rows ON key = key
//     WHEN MATCHED THEN UPDATE | DELETE | SKIP
//     WHEN NOT MATCHED THEN INSERT | SKIP
//
// All statements support full WHERE clauses (see `where_clause.rs`).

use crate::where_clause::{parse_where, WhereExpr};
use serde_json::Value as JsonValue;

/// A table reference — either a Pond collection or an external file.
#[derive(Debug, Clone)]
pub enum TableRef {
    /// A Pond collection name
    Collection(String),
    /// A file path (CSV, JSON, Parquet) — detected by extension
    File(String),
}

impl TableRef {
    /// Parse a table reference from a string. If it looks like a file path
    /// (contains a '.' extension or starts with a quote), treat it as a file.
    pub fn parse(s: &str) -> Self {
        let s = s.trim().trim_matches('\'').trim_matches('"');
        if s.ends_with(".csv") || s.ends_with(".json") || s.ends_with(".parquet")
            || s.ends_with(".tsv") || s.ends_with(".ndjson") {
            TableRef::File(s.to_string())
        } else {
            TableRef::Collection(s.to_string())
        }
    }

    pub fn collection_name(&self) -> Option<&str> {
        match self {
            TableRef::Collection(name) => Some(name),
            _ => None,
        }
    }
}

/// A JOIN type.
#[derive(Debug, Clone, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    FullOuter,
    Cross,
}

/// A JOIN clause in a SELECT statement.
#[derive(Debug, Clone)]
pub struct JoinClause {
    pub table: TableRef,
    pub alias: Option<String>,
    pub join_type: JoinType,
    /// (left_col, right_col) — qualified names like "u.id".
    /// Empty for CROSS JOIN.
    pub on: Vec<(String, String)>,
}

/// A parsed SELECT item.
///
/// `Star`           — `SELECT *`
/// `Column(name)`   — `SELECT col` or `SELECT alias.col`
/// `Aggregate(agg)` — `SELECT COUNT(*)`, `SUM(col)`, `AVG(col)`, ...
#[derive(Debug, Clone)]
pub enum SelectItem {
    Star,
    Column(String),
    Aggregate(AggregateExpr),
}

/// A parsed aggregate function call.
#[derive(Debug, Clone)]
pub struct AggregateExpr {
    pub func: AggregateFunc,
    /// Argument column name, or None for COUNT(*).
    pub arg: Option<String>,
    /// Output alias (e.g. `COUNT(*) AS cnt` → Some("cnt")).
    pub alias: Option<String>,
}

/// Supported aggregate functions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

impl AggregateFunc {
    pub fn as_str(&self) -> &'static str {
        match self {
            AggregateFunc::Count => "COUNT",
            AggregateFunc::Sum => "SUM",
            AggregateFunc::Avg => "AVG",
            AggregateFunc::Min => "MIN",
            AggregateFunc::Max => "MAX",
        }
    }
}

/// An ORDER BY column with direction.
#[derive(Debug, Clone)]
pub struct OrderByItem {
    pub col: String,
    pub desc: bool,
}

/// A parsed SQL statement.
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum SqlStatement {
    Select {
        table: TableRef,
        alias: Option<String>,
        /// Legacy projection list (empty = SELECT *). Computed from
        /// `select_items` for backward compatibility with the PyO3 binding.
        columns: Vec<String>,
        /// Full select-item list (includes aggregates + aliases).
        select_items: Vec<SelectItem>,
        joins: Vec<JoinClause>,
        r#where: WhereExpr,
        groups: Vec<String>,
        having: WhereExpr,
        orders: Vec<OrderByItem>,
        limit: Option<usize>,
        offset: Option<usize>,
    },
    Update {
        collection: String,
        sets: Vec<(String, JsonValue)>,
        r#where: WhereExpr,
    },
    Delete {
        collection: String,
        r#where: WhereExpr,
    },
    Insert {
        collection: String,
        columns: Vec<String>,
        rows: Vec<Vec<JsonValue>>,
    },
    Merge {
        target: String,
        source_rows: Vec<JsonValue>,
        match_keys: Vec<(String, String)>,
        when_matched: MergeAction,
        when_not_matched: MergeAction,
    },
}

#[derive(Debug, Clone, Default)]
pub enum MergeAction {
    Update,
    Delete,
    #[default]
    Skip,
    Insert,
}


/// Parse a SQL statement string.
pub fn parse_sql(sql: &str) -> Result<SqlStatement, String> {
    let sql = sql.trim();
    let upper = sql.to_uppercase();

    if upper.starts_with("SELECT") {
        parse_select(sql)
    } else if upper.starts_with("UPDATE") {
        parse_update(sql)
    } else if upper.starts_with("DELETE") {
        parse_delete(sql)
    } else if upper.starts_with("INSERT") {
        parse_insert(sql)
    } else if upper.starts_with("MERGE") {
        parse_merge(sql)
    } else {
        Err(format!(
            "Unsupported SQL statement. Expected SELECT, UPDATE, DELETE, INSERT, or MERGE. Got: {}",
            sql.split_whitespace().next().unwrap_or("")
        ))
    }
}

// ---------------------------------------------------------------------------
// SELECT parser
// ---------------------------------------------------------------------------

fn parse_select(sql: &str) -> Result<SqlStatement, String> {
    // SELECT [items] FROM collection [alias] [JOIN ... ON ...]
    //        [WHERE ...] [GROUP BY ...] [HAVING ...]
    //        [ORDER BY ...] [LIMIT n [OFFSET m]]

    let after_select = strip_prefix_ci(sql, "SELECT")
        .ok_or_else(|| "Expected SELECT".to_string())?
        .trim();

    // Find FROM
    let from_pos = find_keyword(after_select, "FROM")
        .ok_or_else(|| "Expected FROM in SELECT".to_string())?;

    let cols_str = after_select[..from_pos].trim();
    let after_from = after_select[from_pos + 4..].trim();

    // Parse SELECT items.
    let select_items = parse_select_items(cols_str)?;
    // Build legacy `columns` projection list from select_items.
    let columns: Vec<String> = select_items.iter().filter_map(|it| match it {
        SelectItem::Column(c) => Some(c.clone()),
        _ => None,
    }).collect();

    // The remaining string may contain: table [alias] [JOINs] [WHERE]
    // [GROUP BY] [HAVING] [ORDER BY] [LIMIT] [OFFSET].
    //
    // To keep parsing simple and robust, we split the remaining string at
    // the FIRST occurrence of any top-level clause keyword, in priority
    // order: WHERE, GROUP BY, ORDER BY, LIMIT. JOINs come before all of
    // those, so they are parsed from the head.
    let splits = split_select_tail(after_from);

    let before_join_clause = splits.head; // table [alias] [JOINs]
    let where_str = splits.r#where;
    let group_str = splits.group;
    let having_str = splits.having;
    let order_str = splits.order;
    let limit_str = splits.limit;
    let offset_str = splits.offset;

    // Parse table name + optional alias + JOINs from before_join_clause.
    let (table, alias, joins) = parse_table_alias_joins(before_join_clause)?;

    let where_expr = if where_str.is_empty() {
        WhereExpr::True
    } else {
        parse_where(where_str)?
    };

    let groups: Vec<String> = if group_str.is_empty() {
        Vec::new()
    } else {
        group_str.split(',').map(|s| s.trim().to_string()).collect()
    };

    let having = if having_str.is_empty() {
        WhereExpr::True
    } else {
        parse_where(having_str)?
    };

    let orders = if order_str.is_empty() {
        Vec::new()
    } else {
        parse_order_by(order_str)?
    };

    let limit: Option<usize> = if limit_str.is_empty() {
        None
    } else {
        Some(limit_str.trim().parse::<usize>()
            .map_err(|e| format!("Invalid LIMIT value: {}", e))?)
    };

    let offset: Option<usize> = if offset_str.is_empty() {
        None
    } else {
        Some(offset_str.trim().parse::<usize>()
            .map_err(|e| format!("Invalid OFFSET value: {}", e))?)
    };

    Ok(SqlStatement::Select {
        table,
        alias,
        columns,
        select_items,
        joins,
        r#where: where_expr,
        groups,
        having,
        orders,
        limit,
        offset,
    })
}

struct SelectTailSplits<'a> {
    head: &'a str,    // table [alias] [JOINs ...]
    r#where: &'a str,
    group: &'a str,
    having: &'a str,
    order: &'a str,
    limit: &'a str,
    offset: &'a str,
}

/// Split the post-FROM clause into its component parts.
///
/// Order of clauses (per SQL): FROM ... JOINs ... WHERE ... GROUP BY ...
/// HAVING ... ORDER BY ... LIMIT ... OFFSET.
fn split_select_tail(s: &str) -> SelectTailSplits<'_> {
    let empty = "";

    // Find the FIRST occurrence of WHERE / GROUP BY / HAVING / ORDER BY /
    // LIMIT / OFFSET — anything before that is table+alias+JOINs.
    //
    // HAVING is included here (even though standard SQL only allows it
    // after GROUP BY) so that `SELECT COUNT(*) FROM t HAVING ...` doesn't
    // get its HAVING clause mis-parsed as the table alias.
    let mut head_end = s.len();
    for kw in ["WHERE", "GROUP BY", "HAVING", "ORDER BY", "LIMIT", "OFFSET"] {
        if let Some(p) = find_keyword(s, kw) {
            if p < head_end {
                head_end = p;
            }
        }
    }
    let head = s[..head_end].trim();

    let rest = &s[head_end..];

    // Extract each clause by slicing between keyword positions.
    let where_pos = find_keyword(rest, "WHERE");
    let group_pos = find_keyword(rest, "GROUP BY");
    let having_pos = find_keyword(rest, "HAVING");
    let order_pos = find_keyword(rest, "ORDER BY");
    let limit_pos = find_keyword(rest, "LIMIT");
    let offset_pos = find_keyword(rest, "OFFSET");

    // Build a sorted list of (pos, label) — then each clause's body is the
    // text from its keyword end to the next keyword's start.
    let mut marks: Vec<(usize, &str, usize)> = Vec::new();
    if let Some(p) = where_pos { marks.push((p, "WHERE", 5)); }
    if let Some(p) = group_pos { marks.push((p, "GROUP BY", 8)); }
    if let Some(p) = having_pos { marks.push((p, "HAVING", 6)); }
    if let Some(p) = order_pos { marks.push((p, "ORDER BY", 8)); }
    if let Some(p) = limit_pos { marks.push((p, "LIMIT", 5)); }
    if let Some(p) = offset_pos { marks.push((p, "OFFSET", 6)); }
    marks.sort_by_key(|(p, _, _)| *p);

    let mut where_str = empty;
    let mut group_str = empty;
    let mut having_str = empty;
    let mut order_str = empty;
    let mut limit_str = empty;
    let mut offset_str = empty;

    for (idx, (p, label, kw_len)) in marks.iter().enumerate() {
        let body_start = p + kw_len;
        let body_end = marks.get(idx + 1).map(|(np, _, _)| *np).unwrap_or(rest.len());
        let body = rest[body_start..body_end].trim();
        match *label {
            "WHERE" => where_str = body,
            "GROUP BY" => group_str = body,
            "HAVING" => having_str = body,
            "ORDER BY" => order_str = body,
            "LIMIT" => limit_str = body,
            "OFFSET" => offset_str = body,
            _ => {}
        }
    }

    SelectTailSplits {
        head,
        r#where: where_str,
        group: group_str,
        having: having_str,
        order: order_str,
        limit: limit_str,
        offset: offset_str,
    }
}

/// Parse table + optional alias + JOINs from the head of the post-FROM string.
fn parse_table_alias_joins(s: &str) -> Result<(TableRef, Option<String>, Vec<JoinClause>), String> {
    let mut parts = s.split_whitespace().peekable();
    let table_str = parts.next()
        .ok_or_else(|| "Expected table name after FROM".to_string())?;

    // Check if the table is a quoted string (file path)
    let table_str = if table_str.starts_with('\'') {
        // Find the closing quote — might span multiple words
        let mut full = String::new();
        full.push_str(table_str);
        while !full.ends_with('\'') {
            if let Some(next) = parts.next() {
                full.push(' ');
                full.push_str(next);
            } else {
                break;
            }
        }
        full.trim_matches('\'').to_string()
    } else {
        table_str.to_string()
    };

    let table = TableRef::parse(&table_str);

    // Next token could be an alias or JOIN or nothing
    let alias = if let Some(next) = parts.peek() {
        let upper = next.to_uppercase();
        if upper != "JOIN" && upper != "INNER" && upper != "LEFT" && upper != "RIGHT"
            && upper != "FULL" && upper != "CROSS" && upper != "WHERE" && upper != "ON" {
            Some(parts.next().unwrap().to_string())
        } else {
            None
        }
    } else {
        None
    };

    // Parse JOINs
    let mut joins: Vec<JoinClause> = Vec::new();
    let mut remaining = parts.collect::<Vec<&str>>().join(" ");

    while !remaining.is_empty() {
        let upper = remaining.to_uppercase();

        // Check for JOIN variants
        let (join_type, skip_len) = if upper.starts_with("INNER JOIN") {
            (JoinType::Inner, 10)
        } else if upper.starts_with("LEFT OUTER JOIN") {
            (JoinType::Left, 15)
        } else if upper.starts_with("LEFT JOIN") {
            (JoinType::Left, 9)
        } else if upper.starts_with("RIGHT OUTER JOIN") {
            (JoinType::Right, 16)
        } else if upper.starts_with("RIGHT JOIN") {
            (JoinType::Right, 10)
        } else if upper.starts_with("FULL OUTER JOIN") {
            (JoinType::FullOuter, 15)
        } else if upper.starts_with("FULL JOIN") {
            (JoinType::FullOuter, 9)
        } else if upper.starts_with("CROSS JOIN") {
            (JoinType::Cross, 10)
        } else if upper.starts_with("JOIN") {
            (JoinType::Inner, 4)
        } else {
            break;
        };

        remaining = remaining[skip_len..].trim().to_string();

        // Parse the joined table name + alias
        let mut j_parts = remaining.split_whitespace().peekable();
        let j_table_str = j_parts.next()
            .ok_or_else(|| "Expected table name after JOIN".to_string())?;

        // Check for quoted file path
        let j_table_str = if j_table_str.starts_with('\'') {
            let mut full = String::new();
            full.push_str(j_table_str);
            while !full.ends_with('\'') {
                if let Some(next) = j_parts.next() {
                    full.push(' ');
                    full.push_str(next);
                } else {
                    break;
                }
            }
            full.trim_matches('\'').to_string()
        } else {
            j_table_str.to_string()
        };

        let j_table = TableRef::parse(&j_table_str);

        // Parse optional alias
        let j_alias = if let Some(next) = j_parts.peek() {
            let upper = next.to_uppercase();
            if upper != "ON" && upper != "WHERE" && upper != "JOIN" && upper != "INNER"
                && upper != "LEFT" && upper != "RIGHT" && upper != "FULL" && upper != "CROSS" {
                Some(j_parts.next().unwrap().to_string())
            } else {
                None
            }
        } else {
            None
        };

        // CROSS JOINs have no ON clause
        if join_type == JoinType::Cross {
            joins.push(JoinClause {
                table: j_table,
                alias: j_alias,
                join_type,
                on: vec![],
            });
            remaining = j_parts.collect::<Vec<&str>>().join(" ");
            continue;
        }

        // Expect ON
        remaining = j_parts.collect::<Vec<&str>>().join(" ");
        let on_pos = find_keyword(&remaining, "ON")
            .ok_or_else(|| "Expected ON after JOIN table".to_string())?;

        let _on_str_before = remaining[..on_pos].trim();
        remaining = remaining[on_pos + 2..].trim().to_string();

        // Find where the ON clause ends (at next JOIN, WHERE, or end)
        let on_end = find_keyword(&remaining, "JOIN")
            .or_else(|| find_keyword(&remaining, "WHERE"))
            .or_else(|| find_keyword(&remaining, "INNER"))
            .or_else(|| find_keyword(&remaining, "LEFT"))
            .or_else(|| find_keyword(&remaining, "RIGHT"))
            .or_else(|| find_keyword(&remaining, "FULL"))
            .or_else(|| find_keyword(&remaining, "CROSS"))
            .unwrap_or(remaining.len());

        let on_condition = remaining[..on_end].trim().to_string();
        remaining = remaining[on_end..].trim().to_string();

        // Parse ON clause: "u.id = o.user_id [AND u.code = o.code]"
        let on_pairs = parse_on_clause_for_join(&on_condition)?;

        joins.push(JoinClause {
            table: j_table,
            alias: j_alias,
            join_type,
            on: on_pairs,
        });
    }

    Ok((table, alias, joins))
}

/// Parse SELECT items: `*`, `col1, col2`, `alias.col`, `COUNT(*) [AS cnt]`,
/// `SUM(col) [AS total]`, etc.
fn parse_select_items(s: &str) -> Result<Vec<SelectItem>, String> {
    let s = s.trim();
    if s == "*" {
        return Ok(vec![SelectItem::Star]);
    }
    let mut items = Vec::new();
    // Naive comma split — sufficient for our supported subset. Does not
    // handle commas inside aggregate arg lists (we don't support multi-arg
    // aggregates like CORR(a,b) yet).
    for raw in s.split(',') {
        let item_str = raw.trim();
        if item_str.is_empty() {
            continue;
        }
        if item_str == "*" {
            items.push(SelectItem::Star);
            continue;
        }
        // Check for aggregate function call: NAME(arg) [AS alias]
        if let Some(item) = try_parse_aggregate(item_str)? {
            items.push(item);
            continue;
        }
        // Plain column — strip optional "AS alias" / "alias"
        let col = strip_alias(item_str);
        items.push(SelectItem::Column(col));
    }
    if items.is_empty() {
        return Err("SELECT list is empty".to_string());
    }
    Ok(items)
}

fn try_parse_aggregate(s: &str) -> Result<Option<SelectItem>, String> {
    // Match:  FUNC(arg) [AS alias]   |   FUNC( * ) [AS alias]
    let s = s.trim();
    let paren_pos = match s.find('(') {
        Some(p) => p,
        None => return Ok(None),
    };
    let func_str = s[..paren_pos].trim().to_uppercase();
    let func = match func_str.as_str() {
        "COUNT" => AggregateFunc::Count,
        "SUM" => AggregateFunc::Sum,
        "AVG" => AggregateFunc::Avg,
        "MIN" => AggregateFunc::Min,
        "MAX" => AggregateFunc::Max,
        _ => return Ok(None),
    };
    let close_pos = s.find(')').ok_or_else(|| format!("Unclosed '(' in aggregate: {}", s))?;
    let arg_str = s[paren_pos + 1..close_pos].trim();
    let arg = if arg_str == "*" || arg_str.is_empty() {
        None
    } else {
        Some(arg_str.to_string())
    };
    let after = s[close_pos + 1..].trim();
    let alias = if let Some(rest) = after.strip_prefix("AS ").or_else(|| after.strip_prefix("as ")) {
        Some(rest.trim().to_string())
    } else if !after.is_empty() {
        Some(after.to_string())
    } else {
        None
    };
    Ok(Some(SelectItem::Aggregate(AggregateExpr { func, arg, alias })))
}

/// Strip "col AS alias" or "col alias" — return just `col`.
fn strip_alias(s: &str) -> String {
    let s = s.trim();
    let upper = s.to_uppercase();
    if let Some(p) = upper.find(" AS ") {
        return s[..p].trim().to_string();
    }
    s.to_string()
}

/// Parse ORDER BY items: `col1 [ASC|DESC], col2 [ASC|DESC], ...`
fn parse_order_by(s: &str) -> Result<Vec<OrderByItem>, String> {
    let mut items = Vec::new();
    for raw in s.split(',') {
        let part = raw.trim();
        if part.is_empty() {
            continue;
        }
        let upper = part.to_uppercase();
        let (col, desc) = if let Some(p) = upper.rfind(" DESC") {
            (part[..p].trim().to_string(), true)
        } else if let Some(p) = upper.rfind(" ASC") {
            (part[..p].trim().to_string(), false)
        } else {
            (part.to_string(), false)
        };
        items.push(OrderByItem { col, desc });
    }
    if items.is_empty() {
        return Err("ORDER BY list is empty".to_string());
    }
    Ok(items)
}

/// Parse a JOIN ON clause: "u.id = o.user_id [AND u.code = o.code]"
/// Returns a list of (left_qualified_col, right_qualified_col) pairs.
fn parse_on_clause_for_join(s: &str) -> Result<Vec<(String, String)>, String> {
    let parts: Vec<&str> = s.split_whitespace().collect();
    let mut pairs = Vec::new();
    let mut i = 0;
    while i < parts.len() {
        let left = parts[i].trim_end_matches(',');
        i += 1;
        if i >= parts.len() || parts[i] != "=" {
            return Err(format!("Expected = in ON clause after '{}'", left));
        }
        i += 1;
        if i >= parts.len() {
            return Err("Expected column after = in ON clause".to_string());
        }
        let right = parts[i].trim_end_matches(',');
        i += 1;
        pairs.push((left.to_string(), right.to_string()));
        // Skip AND
        if i < parts.len() && parts[i].to_uppercase() == "AND" {
            i += 1;
        }
    }
    if pairs.is_empty() {
        return Err("ON clause must have at least one key pair".to_string());
    }
    Ok(pairs)
}

// ---------------------------------------------------------------------------
// UPDATE parser
// ---------------------------------------------------------------------------

fn parse_update(sql: &str) -> Result<SqlStatement, String> {
    // UPDATE collection SET col1 = val1, col2 = val2 [WHERE ...]

    let after_update = strip_prefix_ci(sql, "UPDATE")
        .ok_or_else(|| "Expected UPDATE".to_string())?
        .trim();

    let set_pos = find_keyword(after_update, "SET")
        .ok_or_else(|| "Expected SET in UPDATE".to_string())?;

    let collection = after_update[..set_pos].trim().to_string();
    let after_set = after_update[set_pos + 3..].trim();

    // Find optional WHERE
    let (sets_str, where_str) = if let Some(where_pos) = find_keyword(after_set, "WHERE") {
        (after_set[..where_pos].trim(), after_set[where_pos + 5..].trim())
    } else {
        (after_set, "")
    };

    // Parse SET col = val, ...
    let sets = parse_set_clause(sets_str)?;
    let where_expr = if where_str.is_empty() {
        WhereExpr::True
    } else {
        parse_where(where_str)?
    };

    Ok(SqlStatement::Update {
        collection,
        sets,
        r#where: where_expr,
    })
}

// ---------------------------------------------------------------------------
// DELETE parser
// ---------------------------------------------------------------------------

fn parse_delete(sql: &str) -> Result<SqlStatement, String> {
    // DELETE FROM collection [WHERE ...]

    let after_delete = strip_prefix_ci(sql, "DELETE")
        .ok_or_else(|| "Expected DELETE".to_string())?
        .trim();

    let from_pos = find_keyword(after_delete, "FROM")
        .ok_or_else(|| "Expected FROM in DELETE".to_string())?;

    let after_from = after_delete[from_pos + 4..].trim();

    let (collection, where_expr) = if let Some(where_pos) = find_keyword(after_from, "WHERE") {
        let coll = after_from[..where_pos].trim().to_string();
        let where_str = after_from[where_pos + 5..].trim();
        (coll, parse_where(where_str)?)
    } else {
        (after_from.trim().to_string(), WhereExpr::True)
    };

    Ok(SqlStatement::Delete {
        collection,
        r#where: where_expr,
    })
}

// ---------------------------------------------------------------------------
// INSERT parser
// ---------------------------------------------------------------------------

fn parse_insert(sql: &str) -> Result<SqlStatement, String> {
    // INSERT INTO collection (col1, col2) VALUES (v1, v2), (v3, v4)

    let after_insert = strip_prefix_ci(sql, "INSERT")
        .ok_or_else(|| "Expected INSERT".to_string())?
        .trim();

    let into_pos = find_keyword(after_insert, "INTO")
        .ok_or_else(|| "Expected INTO in INSERT".to_string())?;

    let after_into = after_insert[into_pos + 4..].trim();

    // Find the opening paren for columns
    let paren_pos = after_into.find('(')
        .ok_or_else(|| "Expected ( after collection name in INSERT".to_string())?;

    let collection = after_into[..paren_pos].trim().to_string();
    let after_paren = after_into[paren_pos..].trim();

    // Parse column list
    let close_paren = after_paren.find(')')
        .ok_or_else(|| "Expected ) after column list".to_string())?;

    let cols_str = &after_paren[1..close_paren];
    let columns: Vec<String> = cols_str.split(',')
        .map(|s| s.trim().to_string())
        .collect();

    let after_cols = after_paren[close_paren + 1..].trim();

    // Expect VALUES
    let values_pos = find_keyword(after_cols, "VALUES")
        .ok_or_else(|| "Expected VALUES in INSERT".to_string())?;

    let values_str = after_cols[values_pos + 6..].trim();

    // Parse value tuples: (v1, v2), (v3, v4)
    let rows = parse_value_tuples(values_str)?;

    Ok(SqlStatement::Insert {
        collection,
        columns,
        rows,
    })
}

// ---------------------------------------------------------------------------
// MERGE parser
// ---------------------------------------------------------------------------

fn parse_merge(sql: &str) -> Result<SqlStatement, String> {
    // MERGE INTO target USING source_rows ON key1 = key2 [AND key3 = key4]
    //   WHEN MATCHED THEN UPDATE | DELETE | SKIP
    //   WHEN NOT MATCHED THEN INSERT | SKIP

    let after_merge = strip_prefix_ci(sql, "MERGE")
        .ok_or_else(|| "Expected MERGE".to_string())?
        .trim();

    let into_pos = find_keyword(after_merge, "INTO")
        .ok_or_else(|| "Expected INTO in MERGE".to_string())?;

    let after_into = after_merge[into_pos + 4..].trim();

    let using_pos = find_keyword(after_into, "USING")
        .ok_or_else(|| "Expected USING in MERGE".to_string())?;

    let target = after_into[..using_pos].trim().to_string();
    let after_using = after_into[using_pos + 6..].trim();

    let on_pos = find_keyword(after_using, "ON")
        .ok_or_else(|| "Expected ON in MERGE".to_string())?;

    // Source is between USING and ON — for now, source must be a JSON array
    let source_str = after_using[..on_pos].trim();
    let source_rows: Vec<JsonValue> = if source_str.starts_with('[') {
        serde_json::from_str(source_str)
            .map_err(|e| format!("Invalid source JSON: {}", e))?
    } else {
        return Err("MERGE source must be a JSON array of row objects".to_string());
    };

    let after_on = after_using[on_pos + 2..].trim();

    // Parse match keys: key1 = key2 [AND key3 = key4]
    // Find WHEN keyword to delimit the ON clause
    let when_pos = find_keyword(after_on, "WHEN")
        .ok_or_else(|| "Expected WHEN MATCHED in MERGE".to_string())?;

    let on_str = after_on[..when_pos].trim();
    let match_keys = parse_on_clause(on_str)?;

    let after_when = after_on[when_pos..].trim();

    // Parse WHEN MATCHED and WHEN NOT MATCHED clauses
    let (when_matched, when_not_matched) = parse_when_clauses(after_when)?;

    Ok(SqlStatement::Merge {
        target,
        source_rows,
        match_keys,
        when_matched,
        when_not_matched,
    })
}

fn parse_on_clause(on_str: &str) -> Result<Vec<(String, String)>, String> {
    // Parse: target_col = source_col [AND target_col2 = source_col2]
    let parts: Vec<&str> = on_str.split_whitespace().collect();
    let mut keys = Vec::new();
    let mut i = 0;
    while i < parts.len() {
        let left = parts[i].trim_end_matches(',');
        i += 1;
        if i >= parts.len() || parts[i] != "=" {
            return Err(format!("Expected = in ON clause after '{}'", left));
        }
        i += 1;
        if i >= parts.len() {
            return Err("Expected source column after = in ON clause".to_string());
        }
        let right = parts[i].trim_end_matches(',');
        i += 1;
        keys.push((left.to_string(), right.to_string()));
        // Skip AND
        if i < parts.len() && parts[i].to_uppercase() == "AND" {
            i += 1;
        }
    }
    if keys.is_empty() {
        return Err("ON clause must have at least one key pair".to_string());
    }
    Ok(keys)
}

fn parse_when_clauses(s: &str) -> Result<(MergeAction, MergeAction), String> {
    let mut when_matched = MergeAction::Update;
    let mut when_not_matched = MergeAction::Insert;

    // Split by WHEN
    let clauses: Vec<&str> = s.split_whitespace().collect();
    let mut i = 0;
    while i < clauses.len() {
        if clauses[i].to_uppercase() == "WHEN" {
            i += 1;
            if i >= clauses.len() {
                return Err("Expected MATCHED or NOT after WHEN".to_string());
            }
            let is_not = clauses[i].to_uppercase() == "NOT";
            if is_not {
                i += 1;
            }
            if i >= clauses.len() || clauses[i].to_uppercase() != "MATCHED" {
                return Err("Expected MATCHED after WHEN".to_string());
            }
            i += 1;
            if i >= clauses.len() || clauses[i].to_uppercase() != "THEN" {
                return Err("Expected THEN after WHEN MATCHED".to_string());
            }
            i += 1;
            if i >= clauses.len() {
                return Err("Expected action after THEN".to_string());
            }
            let action = match clauses[i].to_uppercase().as_str() {
                "UPDATE" => MergeAction::Update,
                "DELETE" => MergeAction::Delete,
                "SKIP" => MergeAction::Skip,
                "INSERT" => MergeAction::Insert,
                other => return Err(format!("Unknown merge action: {}", other)),
            };
            if is_not {
                when_not_matched = action;
            } else {
                when_matched = action;
            }
            i += 1;
            // Skip any SET clause or other tokens until next WHEN
            while i < clauses.len() && clauses[i].to_uppercase() != "WHEN" {
                i += 1;
            }
        } else {
            i += 1;
        }
    }

    Ok((when_matched, when_not_matched))
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

fn strip_prefix_ci<'a>(s: &'a str, prefix: &str) -> Option<&'a str> {
    if s.to_uppercase().starts_with(prefix) {
        Some(&s[prefix.len()..])
    } else {
        None
    }
}

fn find_keyword(s: &str, keyword: &str) -> Option<usize> {
    let upper = s.to_uppercase();
    let kw_upper = keyword.to_uppercase();
    // Find the keyword as a whole word
    let mut search_start = 0;
    while let Some(pos) = upper[search_start..].find(&kw_upper) {
        let abs_pos = search_start + pos;
        // Check it's a whole word
        let before_ok = abs_pos == 0 || !s[..abs_pos].chars().last().unwrap().is_alphanumeric();
        let after_pos = abs_pos + kw_upper.len();
        let after_ok = after_pos >= s.len() || !s[after_pos..].chars().next().unwrap().is_alphanumeric();
        if before_ok && after_ok {
            return Some(abs_pos);
        }
        search_start = abs_pos + 1;
    }
    None
}

fn parse_set_clause(s: &str) -> Result<Vec<(String, JsonValue)>, String> {
    // Parse: col1 = val1, col2 = val2
    let mut sets = Vec::new();
    for part in s.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let eq_pos = part.find('=')
            .ok_or_else(|| format!("Expected = in SET clause: '{}'", part))?;
        let col = part[..eq_pos].trim().to_string();
        let val_str = part[eq_pos + 1..].trim();
        let val = parse_sql_value(val_str)?;
        sets.push((col, val));
    }
    Ok(sets)
}

fn parse_sql_value(s: &str) -> Result<JsonValue, String> {
    let s = s.trim();
    if s.starts_with('\'') && s.ends_with('\'') {
        return Ok(JsonValue::String(s[1..s.len()-1].to_string()));
    }
    if s.eq_ignore_ascii_case("true") {
        return Ok(JsonValue::Bool(true));
    }
    if s.eq_ignore_ascii_case("false") {
        return Ok(JsonValue::Bool(false));
    }
    if s.eq_ignore_ascii_case("null") {
        return Ok(JsonValue::Null);
    }
    if let Ok(i) = s.parse::<i64>() {
        return Ok(JsonValue::Number(serde_json::Number::from(i)));
    }
    if let Ok(f) = s.parse::<f64>() {
        if let Some(n) = serde_json::Number::from_f64(f) {
            return Ok(JsonValue::Number(n));
        }
    }
    // Unquoted string — treat as string literal
    Ok(JsonValue::String(s.to_string()))
}

fn parse_value_tuples(s: &str) -> Result<Vec<Vec<JsonValue>>, String> {
    // Parse: (v1, v2), (v3, v4)
    let mut rows = Vec::new();
    let mut depth = 0;
    let mut current = String::new();
    let mut in_tuple = false;

    for c in s.chars() {
        if c == '(' {
            depth += 1;
            in_tuple = true;
            current.clear();
        } else if c == ')' {
            depth -= 1;
            if depth == 0 && in_tuple {
                let values: Vec<JsonValue> = current.split(',')
                    .map(|v| parse_sql_value(v.trim()).unwrap_or(JsonValue::Null))
                    .collect();
                rows.push(values);
                in_tuple = false;
            }
        } else if in_tuple {
            current.push(c);
        }
    }

    if rows.is_empty() {
        return Err("No value tuples found".to_string());
    }
    Ok(rows)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_select_star() {
        let stmt = parse_sql("SELECT * FROM users").unwrap();
        match stmt {
            SqlStatement::Select { table, columns, .. } => {
                assert_eq!(table.collection_name(), Some("users"));
                assert!(columns.is_empty()); // SELECT *
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_select_cols_where() {
        let stmt = parse_sql("SELECT name, age FROM users WHERE age >= 18").unwrap();
        match stmt {
            SqlStatement::Select { table, columns, r#where, .. } => {
                assert_eq!(table.collection_name(), Some("users"));
                assert_eq!(columns, vec!["name", "age"]);
                assert!(matches!(r#where, WhereExpr::Compare { .. }));
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_select_join() {
        let stmt = parse_sql(
            "SELECT * FROM users u JOIN orders o ON u.id = o.user_id WHERE u.age > 18"
        ).unwrap();
        match stmt {
            SqlStatement::Select { table, alias, joins, .. } => {
                assert_eq!(table.collection_name(), Some("users"));
                assert_eq!(alias, Some("u".to_string()));
                assert_eq!(joins.len(), 1);
                assert_eq!(joins[0].table.collection_name(), Some("orders"));
                assert_eq!(joins[0].alias, Some("o".to_string()));
                assert_eq!(joins[0].on, vec![("u.id".to_string(), "o.user_id".to_string())]);
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_select_left_join() {
        let stmt = parse_sql(
            "SELECT * FROM users u LEFT JOIN orders o ON u.id = o.user_id"
        ).unwrap();
        match stmt {
            SqlStatement::Select { joins, .. } => {
                assert_eq!(joins.len(), 1);
                assert_eq!(joins[0].join_type, JoinType::Left);
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_select_file() {
        let stmt = parse_sql("SELECT * FROM 'data.csv' WHERE age > 18").unwrap();
        match stmt {
            SqlStatement::Select { table, .. } => {
                match table {
                    TableRef::File(path) => assert_eq!(path, "data.csv"),
                    _ => panic!("Expected File table ref"),
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_select_group_by() {
        let stmt = parse_sql(
            "SELECT dept, COUNT(*) FROM employees GROUP BY dept"
        ).unwrap();
        match stmt {
            SqlStatement::Select { groups, select_items, .. } => {
                assert_eq!(groups, vec!["dept"]);
                assert_eq!(select_items.len(), 2);
                assert!(matches!(select_items[0], SelectItem::Column(_)));
                assert!(matches!(select_items[1], SelectItem::Aggregate(_)));
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_select_order_by_limit() {
        let stmt = parse_sql(
            "SELECT name, age FROM users ORDER BY age DESC LIMIT 10 OFFSET 5"
        ).unwrap();
        match stmt {
            SqlStatement::Select { orders, limit, offset, .. } => {
                assert_eq!(orders.len(), 1);
                assert_eq!(orders[0].col, "age");
                assert!(orders[0].desc);
                assert_eq!(limit, Some(10));
                assert_eq!(offset, Some(5));
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_select_having() {
        let stmt = parse_sql(
            "SELECT dept, COUNT(*) AS cnt FROM employees GROUP BY dept HAVING cnt > 5"
        ).unwrap();
        match stmt {
            SqlStatement::Select { having, .. } => {
                assert!(!matches!(having, WhereExpr::True));
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_update() {
        let stmt = parse_sql("UPDATE users SET status = 'active' WHERE age >= 18").unwrap();
        match stmt {
            SqlStatement::Update { collection, sets, .. } => {
                assert_eq!(collection, "users");
                assert_eq!(sets.len(), 1);
                assert_eq!(sets[0].0, "status");
                assert_eq!(sets[0].1, JsonValue::String("active".to_string()));
            }
            _ => panic!("Expected Update"),
        }
    }

    #[test]
    fn test_parse_delete() {
        let stmt = parse_sql("DELETE FROM users WHERE status = 'inactive'").unwrap();
        match stmt {
            SqlStatement::Delete { collection, .. } => {
                assert_eq!(collection, "users");
            }
            _ => panic!("Expected Delete"),
        }
    }

    #[test]
    fn test_parse_insert() {
        let stmt = parse_sql(
            "INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob')"
        ).unwrap();
        match stmt {
            SqlStatement::Insert { collection, columns, rows } => {
                assert_eq!(collection, "users");
                assert_eq!(columns, vec!["id", "name"]);
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0][0], JsonValue::Number(serde_json::Number::from(1)));
                assert_eq!(rows[0][1], JsonValue::String("alice".to_string()));
            }
            _ => panic!("Expected Insert"),
        }
    }

    #[test]
    fn test_parse_merge() {
        let sql = "MERGE INTO users USING [{\"id\":1,\"name\":\"alice\"}] ON id = id \
                   WHEN MATCHED THEN UPDATE \
                   WHEN NOT MATCHED THEN INSERT";
        let stmt = parse_sql(sql).unwrap();
        match stmt {
            SqlStatement::Merge { target, match_keys, when_matched, when_not_matched, .. } => {
                assert_eq!(target, "users");
                assert_eq!(match_keys, vec![("id".to_string(), "id".to_string())]);
                assert!(matches!(when_matched, MergeAction::Update));
                assert!(matches!(when_not_matched, MergeAction::Insert));
            }
            _ => panic!("Expected Merge"),
        }
    }
}
