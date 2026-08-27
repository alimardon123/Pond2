// SQL WHERE clause parser + evaluator.
//
// Supports a practical subset of SQL WHERE syntax:
//
//   col = value
//   col != value / col <> value
//   col > value, col >= value, col < value, col <= value
//   col IN (v1, v2, v3)
//   col NOT IN (v1, v2, v3)
//   col LIKE 'pattern%'    (% = any chars, _ = single char)
//   col IS NULL
//   col IS NOT NULL
//   expr AND expr
//   expr OR expr
//   NOT expr
//   (expr)
//
// Value types:
//   'string literal'   (single-quoted)
//   42                 (integer)
//   3.14               (float)
//   true / false       (boolean)
//   NULL               (null)
//
// Example:
//   "age >= 18 AND city = 'NYC'"
//   "dept = 'eng' AND (salary > 90000 OR age < 30)"
//   "name LIKE 'A%' AND status IN ('active', 'pending')"

use serde_json::Value as JsonValue;

/// A parsed WHERE expression — an AST that can be evaluated against rows.
#[derive(Debug, Clone)]
pub enum WhereExpr {
    /// col op value  (e.g., age >= 18)
    Compare { col: String, op: String, value: JsonValue },

    /// col IN (v1, v2, ...)
    In { col: String, values: Vec<JsonValue>, negate: bool },

    /// col LIKE 'pattern'
    Like { col: String, pattern: String, negate: bool },

    /// col IS NULL / col IS NOT NULL
    IsNull { col: String, negate: bool },

    /// expr AND expr
    And(Box<WhereExpr>, Box<WhereExpr>),

    /// expr OR expr
    Or(Box<WhereExpr>, Box<WhereExpr>),

    /// NOT expr
    Not(Box<WhereExpr>),

    /// col IN (SELECT subquery) — evaluated by the executor.
    /// Stores the raw subquery SQL string; the executor resolves it
    /// against storage and replaces it with a literal `In` node
    /// before evaluation.
    Subquery { col: String, query: String, negate: bool },

    /// Always true (empty WHERE)
    True,
}

impl WhereExpr {
    /// Evaluate this expression against a JSON row object.
    ///
    /// Subquery nodes always evaluate to `false` here — they MUST be
    /// resolved into `In` nodes by `resolve_subqueries` before evaluation.
    pub fn eval(&self, row: &JsonValue) -> bool {
        match self {
            WhereExpr::True => true,

            WhereExpr::And(a, b) => a.eval(row) && b.eval(row),
            WhereExpr::Or(a, b) => a.eval(row) || b.eval(row),
            WhereExpr::Not(e) => !e.eval(row),

            WhereExpr::Compare { col, op, value } => {
                let cell = row.get(col);
                eval_compare(cell, op, value)
            }

            WhereExpr::In { col, values, negate } => {
                let cell = row.get(col);
                let found = values.iter().any(|v| cell == Some(v));
                if *negate { !found } else { found }
            }

            WhereExpr::Like { col, pattern, negate } => {
                let cell = row.get(col);
                let matches = cell
                    .and_then(|c| c.as_str())
                    .map(|s| like_match(s, pattern))
                    .unwrap_or(false);
                if *negate { !matches } else { matches }
            }

            WhereExpr::IsNull { col, negate } => {
                let cell = row.get(col);
                let is_null = cell.is_none() || cell == Some(&JsonValue::Null);
                if *negate { !is_null } else { is_null }
            }

            // Unresolved subqueries never match.
            WhereExpr::Subquery { .. } => false,
        }
    }

    /// Walk the AST and replace every `Subquery` node with a resolved `In`
    /// node, by evaluating the subquery against `storage` and collecting
    /// the distinct values of its first column.
    ///
    /// This is the only way a Subquery node can become evaluable.
    pub fn resolve_subqueries<F>(&self, mut resolver: F) -> WhereExpr
    where
        F: FnMut(&str) -> Result<Vec<JsonValue>, String>,
    {
        self.resolve_subqueries_inner(&mut resolver)
    }

    fn resolve_subqueries_inner<F>(&self, resolver: &mut F) -> WhereExpr
    where
        F: FnMut(&str) -> Result<Vec<JsonValue>, String>,
    {
        match self {
            WhereExpr::True => WhereExpr::True,
            WhereExpr::And(a, b) => WhereExpr::And(
                Box::new(a.resolve_subqueries_inner(resolver)),
                Box::new(b.resolve_subqueries_inner(resolver)),
            ),
            WhereExpr::Or(a, b) => WhereExpr::Or(
                Box::new(a.resolve_subqueries_inner(resolver)),
                Box::new(b.resolve_subqueries_inner(resolver)),
            ),
            WhereExpr::Not(e) => {
                WhereExpr::Not(Box::new(e.resolve_subqueries_inner(resolver)))
            }
            WhereExpr::Compare { col, op, value } => WhereExpr::Compare {
                col: col.clone(),
                op: op.clone(),
                value: value.clone(),
            },
            WhereExpr::In { col, values, negate } => WhereExpr::In {
                col: col.clone(),
                values: values.clone(),
                negate: *negate,
            },
            WhereExpr::Like { col, pattern, negate } => WhereExpr::Like {
                col: col.clone(),
                pattern: pattern.clone(),
                negate: *negate,
            },
            WhereExpr::IsNull { col, negate } => WhereExpr::IsNull {
                col: col.clone(),
                negate: *negate,
            },
            WhereExpr::Subquery { col, query, negate } => {
                match resolver(query) {
                    Ok(values) => WhereExpr::In {
                        col: col.clone(),
                        values,
                        negate: *negate,
                    },
                    Err(_) => WhereExpr::Subquery {
                        col: col.clone(),
                        query: query.clone(),
                        negate: *negate,
                    },
                }
            }
        }
    }
}

impl WhereExpr {
    /// Collect all column names referenced in this WHERE expression.
    /// Used for projection pushdown to ensure filter columns are decoded.
    pub fn collect_columns(&self, out: &mut std::collections::HashSet<String>) {
        match self {
            WhereExpr::True => {}
            WhereExpr::Compare { col, .. }
            | WhereExpr::In { col, .. }
            | WhereExpr::Like { col, .. }
            | WhereExpr::IsNull { col, .. }
            | WhereExpr::Subquery { col, .. } => {
                let base = col.rsplit('.').next().unwrap_or(col);
                out.insert(base.to_string());
            }
            WhereExpr::And(a, b) | WhereExpr::Or(a, b) => {
                a.collect_columns(out);
                b.collect_columns(out);
            }
            WhereExpr::Not(e) => e.collect_columns(out),
        }
    }
}

/// Evaluate a comparison: cell op target.
///
/// Handles type coercion: bool true/false is equivalent to int 1/0,
/// so `flag = true` matches a cell stored as `1`.
fn eval_compare(cell: Option<&JsonValue>, op: &str, target: &JsonValue) -> bool {
    match op {
        "=" | "==" => json_values_equal(cell, target),
        "!=" | "<>" => !json_values_equal(cell, target),
        ">" => cmp_json(cell, target) == std::cmp::Ordering::Greater,
        ">=" => matches!(cmp_json(cell, target), std::cmp::Ordering::Greater | std::cmp::Ordering::Equal),
        "<" => cmp_json(cell, target) == std::cmp::Ordering::Less,
        "<=" => matches!(cmp_json(cell, target), std::cmp::Ordering::Less | std::cmp::Ordering::Equal),
        _ => false,
    }
}

/// Check if two JSON values are equal, with type coercion:
///   Bool(true) == Number(1), Bool(false) == Number(0)
pub fn json_values_equal(cell: Option<&JsonValue>, target: &JsonValue) -> bool {
    match (cell, target) {
        (Some(JsonValue::Bool(b)), JsonValue::Number(n)) => {
            n.as_i64() == Some(if *b { 1 } else { 0 })
        }
        (Some(JsonValue::Number(n)), JsonValue::Bool(b)) => {
            n.as_i64() == Some(if *b { 1 } else { 0 })
        }
        _ => cell == Some(target),
    }
}

/// Compare two JSON values (numbers numerically, strings lexicographically).
fn cmp_json(a: Option<&JsonValue>, b: &JsonValue) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    let a = match a {
        Some(v) => v,
        None => return Ordering::Less,
    };
    if let (Some(an), Some(bn)) = (a.as_f64(), b.as_f64()) {
        return an.partial_cmp(&bn).unwrap_or(Ordering::Equal);
    }
    if let (Some(as_), Some(bs)) = (a.as_str(), b.as_str()) {
        return as_.cmp(bs);
    }
    a.to_string().cmp(&b.to_string())
}

/// SQL LIKE pattern matching: % = any chars, _ = single char.
fn like_match(text: &str, pattern: &str) -> bool {
    let text_chars: Vec<char> = text.chars().collect();
    let pattern_chars: Vec<char> = pattern.chars().collect();
    like_helper(&text_chars, &pattern_chars)
}

fn like_helper(text: &[char], pattern: &[char]) -> bool {
    if pattern.is_empty() {
        return text.is_empty();
    }
    match pattern[0] {
        '%' => {
            if like_helper(text, &pattern[1..]) { return true; }
            if !text.is_empty() && like_helper(&text[1..], pattern) { return true; }
            false
        }
        '_' => !text.is_empty() && like_helper(&text[1..], &pattern[1..]),
        c => !text.is_empty() && text[0] == c && like_helper(&text[1..], &pattern[1..]),
    }
}

// ---------------------------------------------------------------------------
// Tokenizer
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    Ident(String),
    String(String),
    Number(f64),
    Bool(bool),
    Null,
    Op(String),       // =, ==, !=, <>, >, >=, <, <=
    LParen,
    RParen,
    Comma,
    And,
    Or,
    Not,
    In,
    Like,
    Is,
    EOF,
}

fn tokenize(input: &str) -> Result<Vec<Token>, String> {
    let chars: Vec<char> = input.chars().collect();
    let mut tokens = Vec::new();
    let mut i = 0;

    while i < chars.len() {
        let c = chars[i];

        // Skip whitespace
        if c.is_whitespace() {
            i += 1;
            continue;
        }

        // String literal
        if c == '\'' {
            i += 1;
            let mut s = String::new();
            while i < chars.len() && chars[i] != '\'' {
                s.push(chars[i]);
                i += 1;
            }
            if i >= chars.len() {
                return Err("Unterminated string literal".to_string());
            }
            i += 1; // skip closing quote
            tokens.push(Token::String(s));
            continue;
        }

        // Number
        if c.is_ascii_digit() || (c == '-' && i + 1 < chars.len() && chars[i + 1].is_ascii_digit()) {
            let mut s = String::new();
            s.push(c);
            i += 1;
            while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '.') {
                s.push(chars[i]);
                i += 1;
            }
            let n: f64 = s.parse().map_err(|_| format!("Invalid number: {}", s))?;
            tokens.push(Token::Number(n));
            continue;
        }

        // Identifier or keyword (supports qualified names like u.age, o.amount)
        if c.is_alphabetic() || c == '_' {
            let mut s = String::new();
            while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_' || chars[i] == '.') {
                s.push(chars[i]);
                i += 1;
            }
            let upper = s.to_uppercase();

            // Aggregate function call as a single token.
            //
            // When the identifier is one of COUNT/SUM/AVG/MIN/MAX (the
            // aggregate functions supported by this engine) AND it is
            // immediately followed by `(`, scan forward to the matching `)`
            // and emit the entire function call as a single Ident token in
            // its canonical form, e.g. `COUNT(*)` or `SUM(salary)`.
            //
            // This lets HAVING (and WHERE) treat `COUNT(*) > 5` as a regular
            // `Compare { col: "COUNT(*)", op: ">", value: 5 }` expression.
            // The executor resolves the aggregate during HAVING evaluation
            // by computing it from the group's rows.
            if matches!(upper.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX") {
                // Peek past whitespace for '('
                let mut j = i;
                while j < chars.len() && chars[j].is_whitespace() {
                    j += 1;
                }
                if j < chars.len() && chars[j] == '(' {
                    // Scan to the matching ')'.
                    let mut depth: i32 = 1;
                    let mut k = j + 1;
                    let mut inner = String::new();
                    while k < chars.len() && depth > 0 {
                        match chars[k] {
                            '(' => { depth += 1; inner.push(chars[k]); }
                            ')' => {
                                depth -= 1;
                                if depth > 0 {
                                    inner.push(chars[k]);
                                }
                            }
                            other => inner.push(other),
                        }
                        k += 1;
                    }
                    if depth == 0 {
                        // Canonical form: FUNC(inner) — uppercase the function
                        // name, preserve the argument verbatim (case-sensitive
                        // column names matter).
                        let inner_trimmed = inner.trim();
                        let canonical = format!("{}({})", upper, inner_trimmed);
                        i = k;
                        tokens.push(Token::Ident(canonical));
                        continue;
                    }
                    // Unbalanced parens — fall through to normal keyword
                    // handling so the parser surfaces a clear error.
                }
            }

            match upper.as_str() {
                "AND" => tokens.push(Token::And),
                "OR" => tokens.push(Token::Or),
                "NOT" => tokens.push(Token::Not),
                "IN" => tokens.push(Token::In),
                "LIKE" => tokens.push(Token::Like),
                "IS" => tokens.push(Token::Is),
                "NULL" => tokens.push(Token::Null),
                "TRUE" => tokens.push(Token::Bool(true)),
                "FALSE" => tokens.push(Token::Bool(false)),
                _ => tokens.push(Token::Ident(s)),
            }
            continue;
        }

        // Two-char operators
        if i + 1 < chars.len() {
            let two = format!("{}{}", c, chars[i + 1]);
            match two.as_str() {
                ">=" | "<=" | "!=" | "<>" | "==" => {
                    tokens.push(Token::Op(two));
                    i += 2;
                    continue;
                }
                _ => {}
            }
        }

        // Single-char tokens
        match c {
            '=' | '>' | '<' => {
                tokens.push(Token::Op(c.to_string()));
                i += 1;
                continue;
            }
            '(' => { tokens.push(Token::LParen); i += 1; continue; }
            ')' => { tokens.push(Token::RParen); i += 1; continue; }
            ',' => { tokens.push(Token::Comma); i += 1; continue; }
            _ => return Err(format!("Unexpected character: '{}'", c)),
        }
    }

    tokens.push(Token::EOF);
    Ok(tokens)
}

// ---------------------------------------------------------------------------
// Parser (recursive descent)
// ---------------------------------------------------------------------------

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, pos: 0 }
    }

    fn peek(&self) -> &Token {
        &self.tokens[self.pos]
    }

    fn advance(&mut self) -> Token {
        let t = self.tokens[self.pos].clone();
        self.pos += 1;
        t
    }

    fn expect(&mut self, expected: &Token) -> Result<(), String> {
        if self.peek() == expected {
            self.advance();
            Ok(())
        } else {
            Err(format!("Expected {:?}, got {:?}", expected, self.peek()))
        }
    }

    /// entry: parse a full WHERE expression
    fn parse(&mut self) -> Result<WhereExpr, String> {
        let expr = self.parse_or()?;
        if self.peek() != &Token::EOF {
            return Err(format!("Unexpected token after expression: {:?}", self.peek()));
        }
        Ok(expr)
    }

    /// or_expr := and_expr (OR and_expr)*
    fn parse_or(&mut self) -> Result<WhereExpr, String> {
        let mut left = self.parse_and()?;
        while self.peek() == &Token::Or {
            self.advance();
            let right = self.parse_and()?;
            left = WhereExpr::Or(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    /// and_expr := not_expr (AND not_expr)*
    fn parse_and(&mut self) -> Result<WhereExpr, String> {
        let mut left = self.parse_not()?;
        while self.peek() == &Token::And {
            self.advance();
            let right = self.parse_not()?;
            left = WhereExpr::And(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    /// not_expr := NOT not_expr | primary
    fn parse_not(&mut self) -> Result<WhereExpr, String> {
        if self.peek() == &Token::Not {
            self.advance();
            let inner = self.parse_not()?;
            return Ok(WhereExpr::Not(Box::new(inner)));
        }
        self.parse_primary()
    }

    /// primary := '(' expr ')' | condition
    fn parse_primary(&mut self) -> Result<WhereExpr, String> {
        if self.peek() == &Token::LParen {
            self.advance();
            let expr = self.parse_or()?;
            self.expect(&Token::RParen)?;
            return Ok(expr);
        }
        self.parse_condition()
    }

    /// condition := col op value
    ///            | col IN ( value_list | SELECT ... )
    ///            | col NOT IN ( value_list | SELECT ... )
    ///            | col LIKE string
    ///            | col NOT LIKE string
    ///            | col IS NULL
    ///            | col IS NOT NULL
    fn parse_condition(&mut self) -> Result<WhereExpr, String> {
        // Expect column name
        let col = match self.advance() {
            Token::Ident(s) => s,
            other => return Err(format!("Expected column name, got {:?}", other)),
        };

        // Check what follows
        match self.peek().clone() {
            Token::Op(op) => {
                self.advance();
                let value = self.parse_value()?;
                Ok(WhereExpr::Compare { col, op, value })
            }

            Token::In => {
                self.advance();
                self.expect(&Token::LParen)?;
                // Subquery? peek for SELECT
                if matches!(self.peek(), Token::Ident(s) if s.eq_ignore_ascii_case("SELECT")) {
                    let query = self.collect_subquery()?;
                    Ok(WhereExpr::Subquery { col, query, negate: false })
                } else {
                    let values = self.parse_value_list()?;
                    self.expect(&Token::RParen)?;
                    Ok(WhereExpr::In { col, values, negate: false })
                }
            }

            Token::Not => {
                self.advance();
                match self.peek().clone() {
                    Token::In => {
                        self.advance();
                        self.expect(&Token::LParen)?;
                        if matches!(self.peek(), Token::Ident(s) if s.eq_ignore_ascii_case("SELECT")) {
                            let query = self.collect_subquery()?;
                            Ok(WhereExpr::Subquery { col, query, negate: true })
                        } else {
                            let values = self.parse_value_list()?;
                            self.expect(&Token::RParen)?;
                            Ok(WhereExpr::In { col, values, negate: true })
                        }
                    }
                    Token::Like => {
                        self.advance();
                        let pattern = match self.advance() {
                            Token::String(s) => s,
                            other => return Err(format!("Expected string after LIKE, got {:?}", other)),
                        };
                        Ok(WhereExpr::Like { col, pattern, negate: true })
                    }
                    other => Err(format!("Expected IN or LIKE after NOT, got {:?}", other)),
                }
            }

            Token::Like => {
                self.advance();
                let pattern = match self.advance() {
                    Token::String(s) => s,
                    other => return Err(format!("Expected string after LIKE, got {:?}", other)),
                };
                Ok(WhereExpr::Like { col, pattern, negate: false })
            }

            Token::Is => {
                self.advance();
                if self.peek() == &Token::Not {
                    self.advance();
                    self.expect(&Token::Null)?;
                    Ok(WhereExpr::IsNull { col, negate: true })
                } else {
                    self.expect(&Token::Null)?;
                    Ok(WhereExpr::IsNull { col, negate: false })
                }
            }

            other => Err(format!(
                "Expected operator after column '{}', got {:?}",
                col, other
            )),
        }
    }

    /// Collect a subquery body inside parens, until the matching RParen.
    /// Returns the raw subquery string (including SELECT).
    fn collect_subquery(&mut self) -> Result<String, String> {
        let mut depth = 1usize;
        let mut parts: Vec<String> = Vec::new();
        while depth > 0 {
            match self.peek().clone() {
                Token::EOF => return Err("Unterminated subquery".to_string()),
                Token::LParen => { depth += 1; parts.push("(".to_string()); self.advance(); }
                Token::RParen => {
                    depth -= 1;
                    if depth == 0 {
                        self.advance(); // consume the closing RParen
                        break;
                    } else {
                        parts.push(")".to_string());
                        self.advance();
                    }
                }
                Token::Ident(s) => { parts.push(s); self.advance(); }
                Token::String(s) => { parts.push(format!("'{}'", s)); self.advance(); }
                Token::Number(n) => { parts.push(n.to_string()); self.advance(); }
                Token::Bool(b) => { parts.push(b.to_string()); self.advance(); }
                Token::Null => { parts.push("NULL".to_string()); self.advance(); }
                Token::Op(s) => { parts.push(s); self.advance(); }
                Token::Comma => { parts.push(",".to_string()); self.advance(); }
                Token::And => { parts.push("AND".to_string()); self.advance(); }
                Token::Or => { parts.push("OR".to_string()); self.advance(); }
                Token::Not => { parts.push("NOT".to_string()); self.advance(); }
                Token::In => { parts.push("IN".to_string()); self.advance(); }
                Token::Like => { parts.push("LIKE".to_string()); self.advance(); }
                Token::Is => { parts.push("IS".to_string()); self.advance(); }
            }
        }
        Ok(parts.join(" "))
    }

    /// value := string | number | bool | NULL
    fn parse_value(&mut self) -> Result<JsonValue, String> {
        match self.advance() {
            Token::String(s) => Ok(JsonValue::String(s)),
            Token::Number(n) => {
                if n.fract() == 0.0 && n.abs() < (i64::MAX as f64) {
                    Ok(JsonValue::Number(serde_json::Number::from(n as i64)))
                } else {
                    serde_json::Number::from_f64(n)
                        .map(JsonValue::Number)
                        .ok_or_else(|| format!("Invalid number: {}", n))
                }
            }
            Token::Bool(b) => Ok(JsonValue::Bool(b)),
            Token::Null => Ok(JsonValue::Null),
            other => Err(format!("Expected value, got {:?}", other)),
        }
    }

    /// value_list := value (',' value)*
    fn parse_value_list(&mut self) -> Result<Vec<JsonValue>, String> {
        let mut values = vec![self.parse_value()?];
        while self.peek() == &Token::Comma {
            self.advance();
            values.push(self.parse_value()?);
        }
        Ok(values)
    }
}

/// Parse a SQL WHERE string into a WhereExpr AST.
///
/// Examples:
///   "age >= 18"
///   "city = 'NYC' AND age > 25"
///   "dept = 'eng' AND (salary > 90000 OR age < 30)"
///   "name LIKE 'A%' AND status IN ('active', 'pending')"
///   "email IS NOT NULL"
///   "user_id IN (SELECT id FROM users WHERE age > 18)"
pub fn parse_where(expr: &str) -> Result<WhereExpr, String> {
    let trimmed = expr.trim();
    if trimmed.is_empty() {
        return Ok(WhereExpr::True);
    }
    let tokens = tokenize(trimmed)?;
    let mut parser = Parser::new(tokens);
    parser.parse()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_simple_equality() {
        let expr = parse_where("name = 'alice'").unwrap();
        let row = json!({"name": "alice"});
        assert!(expr.eval(&row));
        let row = json!({"name": "bob"});
        assert!(!expr.eval(&row));
    }

    #[test]
    fn test_comparison() {
        let expr = parse_where("age >= 18").unwrap();
        assert!(expr.eval(&json!({"age": 25})));
        assert!(expr.eval(&json!({"age": 18})));
        assert!(!expr.eval(&json!({"age": 15})));
    }

    #[test]
    fn test_and_or() {
        let expr = parse_where("age > 18 AND city = 'NYC'").unwrap();
        assert!(expr.eval(&json!({"age": 25, "city": "NYC"})));
        assert!(!expr.eval(&json!({"age": 25, "city": "LA"})));
        assert!(!expr.eval(&json!({"age": 15, "city": "NYC"})));

        let expr = parse_where("age < 20 OR age > 60").unwrap();
        assert!(expr.eval(&json!({"age": 15})));
        assert!(expr.eval(&json!({"age": 65})));
        assert!(!expr.eval(&json!({"age": 35})));
    }

    #[test]
    fn test_parens() {
        let expr = parse_where("dept = 'eng' AND (salary > 90000 OR age < 30)").unwrap();
        assert!(expr.eval(&json!({"dept": "eng", "salary": 95000, "age": 35})));
        assert!(expr.eval(&json!({"dept": "eng", "salary": 50000, "age": 25})));
        assert!(!expr.eval(&json!({"dept": "eng", "salary": 50000, "age": 35})));
    }

    #[test]
    fn test_in() {
        let expr = parse_where("city IN ('NYC', 'LA', 'SF')").unwrap();
        assert!(expr.eval(&json!({"city": "NYC"})));
        assert!(expr.eval(&json!({"city": "SF"})));
        assert!(!expr.eval(&json!({"city": "Boston"})));
    }

    #[test]
    fn test_not_in() {
        let expr = parse_where("city NOT IN ('NYC', 'LA')").unwrap();
        assert!(!expr.eval(&json!({"city": "NYC"})));
        assert!(expr.eval(&json!({"city": "Boston"})));
    }

    #[test]
    fn test_like() {
        let expr = parse_where("name LIKE 'A%'").unwrap();
        assert!(expr.eval(&json!({"name": "Alice"})));
        assert!(expr.eval(&json!({"name": "Aaron"})));
        assert!(!expr.eval(&json!({"name": "Bob"})));
    }

    #[test]
    fn test_is_null() {
        let expr = parse_where("email IS NULL").unwrap();
        assert!(expr.eval(&json!({"name": "alice"})));
        assert!(expr.eval(&json!({"name": "alice", "email": null})));
        assert!(!expr.eval(&json!({"name": "alice", "email": "a@b.com"})));

        let expr = parse_where("email IS NOT NULL").unwrap();
        assert!(expr.eval(&json!({"name": "alice", "email": "a@b.com"})));
        assert!(!expr.eval(&json!({"name": "alice"})));
    }

    #[test]
    fn test_not() {
        let expr = parse_where("NOT age > 30").unwrap();
        assert!(expr.eval(&json!({"age": 25})));
        assert!(!expr.eval(&json!({"age": 35})));
    }

    #[test]
    fn test_complex() {
        let expr = parse_where(
            "dept = 'eng' AND (age < 30 OR salary > 100000) AND city IN ('NYC', 'SF')"
        ).unwrap();
        assert!(expr.eval(&json!({
            "dept": "eng", "age": 25, "salary": 50000, "city": "NYC"
        })));
        assert!(expr.eval(&json!({
            "dept": "eng", "age": 35, "salary": 120000, "city": "SF"
        })));
        assert!(!expr.eval(&json!({
            "dept": "sales", "age": 25, "salary": 50000, "city": "NYC"
        })));
    }

    #[test]
    fn test_subquery_parses() {
        // Subqueries should parse but eval to false until resolved.
        let expr = parse_where("user_id IN (SELECT id FROM users WHERE age > 18)").unwrap();
        let row = json!({"user_id": 42});
        assert!(!expr.eval(&row)); // unresolved → false
        assert!(matches!(expr, WhereExpr::Subquery { .. }));
    }
}
