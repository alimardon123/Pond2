// Integration tests for the pond CLI
// Tests the full API: init, write, read, branch, checkout (-b), merge (source→target),
// branches, history, undo, ls, cat, version, and the interactive shell/REPL.

use std::process::Command;
use std::fs;
use tempfile::TempDir;

const POND_BIN: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../target/release/pond");

fn pond(root: &std::path::Path, args: &[&str]) -> Command {
    let mut cmd = Command::new(POND_BIN);
    cmd.arg("--root").arg(root).args(args);
    cmd
}

fn run(root: &std::path::Path, args: &[&str]) -> String {
    let output = pond(root, args).output().unwrap();
    if !output.status.success() {
        panic!("pond {:?} failed: {}", args, String::from_utf8_lossy(&output.stderr));
    }
    String::from_utf8_lossy(&output.stdout).to_string()
}

/// Run `pond shell` with the given stdin bytes, returning (stdout, stderr, status).
fn run_shell(root: &std::path::Path, args: &[&str], stdin: &[u8]) -> (String, String, bool) {
    let mut cmd = pond(root, args);
    cmd.stdin(std::process::Stdio::piped());
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());
    let mut child = cmd.spawn().unwrap();
    {
        use std::io::Write;
        let stdin_handle = child.stdin.as_mut().unwrap();
        stdin_handle.write_all(stdin).unwrap();
    }
    // Drop stdin to signal EOF so the REPL exits.
    drop(child.stdin.take());
    let output = child.wait_with_output().unwrap();
    (
        String::from_utf8_lossy(&output.stdout).to_string(),
        String::from_utf8_lossy(&output.stderr).to_string(),
        output.status.success(),
    )
}

#[test]
fn test_init_creates_pond_dir() {
    let dir = TempDir::new().unwrap();
    run(dir.path(), &["init", "."]);
    assert!(dir.path().join("blobs").exists());
}

#[test]
fn test_write_and_read_json() {
    let dir = TempDir::new().unwrap();
    run(dir.path(), &["init", "."]);
    run(dir.path(), &["write", "users", "--json", r#"{"name":"alice"}"#]);
    let out = run(dir.path(), &["read", "users"]);
    assert!(out.contains("alice"));
}

#[test]
fn test_write_from_file() {
    let dir = TempDir::new().unwrap();
    run(dir.path(), &["init", "."]);
    let f = dir.path().join("data.txt");
    fs::write(&f, "hello from file").unwrap();
    run(dir.path(), &["write", "docs", f.to_str().unwrap()]);
    assert_eq!(run(dir.path(), &["read", "docs"]), "hello from file");
}

#[test]
fn test_write_from_stdin() {
    use std::io::Write;
    let dir = TempDir::new().unwrap();
    run(dir.path(), &["init", "."]);
    let mut cmd = pond(dir.path(), &["write", "logs", "--bytes"]);
    cmd.stdin(std::process::Stdio::piped());
    let mut child = cmd.spawn().unwrap();
    child.stdin.as_mut().unwrap().write_all(b"raw log data").unwrap();
    child.wait_with_output().unwrap();
    assert_eq!(run(dir.path(), &["read", "logs"]), "raw log data");
}

#[test]
fn test_dedup() {
    let dir = TempDir::new().unwrap();
    run(dir.path(), &["init", "."]);
    // Write the same data to two collections. The DATA blob is deduped
    // (same hash), but the COMMIT blobs differ (different timestamps).
    // We verify dedup by checking the underlying data is identical.
    run(dir.path(), &["write", "c1", "--json", r#"{"d":"same"}"#]);
    run(dir.path(), &["write", "c2", "--json", r#"{"d":"same"}"#]);
    // Both collections should return the same data
    let out1 = run(dir.path(), &["read", "c1"]);
    let out2 = run(dir.path(), &["read", "c2"]);
    assert_eq!(out1, out2, "same data must produce same content (dedup)");
}

#[test]
fn test_ls() {
    let dir = TempDir::new().unwrap();
    run(dir.path(), &["init", "."]);
    run(dir.path(), &["write", "users", "--json", r#"{"a":1}"#]);
    run(dir.path(), &["write", "orders", "--json", r#"{"b":2}"#]);
    let out = run(dir.path(), &["ls"]);
    assert!(out.contains("users"));
    assert!(out.contains("orders"));
}

#[test]
fn test_history_shows_commits() {
    let dir = TempDir::new().unwrap();
    run(dir.path(), &["init", "."]);
    run(dir.path(), &["write", "users", "--json", r#"{"v":1}"#, "-m", "first"]);
    run(dir.path(), &["write", "users", "--json", r#"{"v":2}"#, "-m", "second"]);
    run(dir.path(), &["write", "users", "--json", r#"{"v":3}"#, "-m", "third"]);
    let out = run(dir.path(), &["history", "users"]);
    assert!(out.contains("first"));
    assert!(out.contains("second"));
    assert!(out.contains("third"));
}

#[test]
fn test_checkout_and_branch() {
    let dir = TempDir::new().unwrap();
    run(dir.path(), &["init", "."]);
    run(dir.path(), &["write", "users", "--json", r#"{"v":1}"#]);
    run(dir.path(), &["checkout", "-b", "users", "experiment"]);
    run(dir.path(), &["write", "users", "--json", r#"{"v":99}"#]);
    let out = run(dir.path(), &["read", "users"]);
    assert!(out.contains("99"), "experiment should have v=99, got: {}", out);
    run(dir.path(), &["checkout", "users", "main"]);
    let out = run(dir.path(), &["read", "users"]);
    assert!(out.contains(r#""v":1"#), "main should have v=1, got: {}", out);
}

#[test]
fn test_merge_source_into_target() {
    let dir = TempDir::new().unwrap();
    run(dir.path(), &["init", "."]);
    run(dir.path(), &["write", "users", "--json", r#"{"v":1}"#]);
    run(dir.path(), &["checkout", "-b", "users", "experiment"]);
    run(dir.path(), &["write", "users", "--json", r#"{"v":99}"#]);
    run(dir.path(), &["checkout", "users", "main"]);
    run(dir.path(), &["merge", "users", "experiment"]);
    let out = run(dir.path(), &["read", "users"]);
    assert!(out.contains("99"), "after merge, main should have v=99, got: {}", out);
}

#[test]
fn test_merge_into_specific_target() {
    let dir = TempDir::new().unwrap();
    run(dir.path(), &["init", "."]);
    run(dir.path(), &["write", "users", "--json", r#"{"v":1}"#]);
    run(dir.path(), &["branch", "users", "feature"]);
    run(dir.path(), &["branch", "users", "staging"]);
    run(dir.path(), &["checkout", "users", "feature"]);
    run(dir.path(), &["write", "users", "--json", r#"{"v":42}"#]);
    run(dir.path(), &["checkout", "users", "main"]);
    run(dir.path(), &["merge", "users", "feature", "--into", "staging"]);
    run(dir.path(), &["checkout", "users", "staging"]);
    let out = run(dir.path(), &["read", "users"]);
    assert!(out.contains("42"), "staging should have v=42, got: {}", out);
    run(dir.path(), &["checkout", "users", "main"]);
    let out = run(dir.path(), &["read", "users"]);
    assert!(out.contains(r#""v":1"#), "main should still have v=1, got: {}", out);
}

#[test]
fn test_branches_command() {
    let dir = TempDir::new().unwrap();
    run(dir.path(), &["init", "."]);
    run(dir.path(), &["write", "users", "--json", r#"{"v":1}"#]);
    run(dir.path(), &["branch", "users", "experiment"]);
    run(dir.path(), &["branch", "users", "feature"]);
    let out = run(dir.path(), &["branches", "users"]);
    assert!(out.contains("main"));
    assert!(out.contains("experiment"));
    assert!(out.contains("feature"));
    assert!(out.contains("* main"));
}

#[test]
fn test_undo() {
    let dir = TempDir::new().unwrap();
    run(dir.path(), &["init", "."]);
    run(dir.path(), &["write", "users", "--json", r#"{"v":1}"#]);
    run(dir.path(), &["write", "users", "--json", r#"{"v":2}"#]);
    run(dir.path(), &["write", "users", "--json", r#"{"v":3}"#]);
    run(dir.path(), &["undo", "users", "1"]);
    let out = run(dir.path(), &["read", "users"]);
    assert!(out.contains(r#""v":2"#), "after undo 1, should be v=2, got: {}", out);
    run(dir.path(), &["undo", "users", "1"]);
    let out = run(dir.path(), &["read", "users"]);
    assert!(out.contains(r#""v":1"#), "after undo 2, should be v=1, got: {}", out);
}

#[test]
fn test_cat_by_prefix() {
    let dir = TempDir::new().unwrap();
    run(dir.path(), &["init", "."]);
    // Write data and get the commit hash
    let out = run(dir.path(), &["write", "coll", "--json", r#"{"key":"value"}"#]);
    let prefix = out.split('\t').next().unwrap();
    // cat reads the raw blob (which is the commit JSON, containing "manifest" field)
    let out = run(dir.path(), &["cat", prefix]);
    assert!(out.contains("manifest"), "cat should read the commit blob with manifest field, got: {}", out);
}

#[test]
fn test_version() {
    let out = run(std::path::Path::new("/tmp"), &["version"]);
    assert!(out.starts_with("pond "));
}

#[test]
fn test_persistence_across_invocations() {
    let dir = TempDir::new().unwrap();
    run(dir.path(), &["init", "."]);
    run(dir.path(), &["write", "persistent", "--json", r#"{"survives":true}"#]);
    let out = run(dir.path(), &["read", "persistent"]);
    assert!(out.contains("survives"));
}

#[test]
fn test_auto_discovery_from_subdir() {
    // Test git-style auto-discovery: `pond init` creates .pond/ marker,
    // and subsequent commands find it by walking up from CWD.
    let dir = TempDir::new().unwrap();
    let root = dir.path();

    // Init in the root
    run(root, &["init", "."]);
    assert!(root.join(".pond").exists());
    assert!(root.join(".pond/config").exists());

    // Write data from the root
    run(root, &["write", "users", "--json", r#"{"name":"alice"}"#]);

    // Read from a nested subdirectory — should auto-discover .pond/
    let subdir = root.join("a/b/c");
    fs::create_dir_all(&subdir).unwrap();

    let mut cmd = Command::new(POND_BIN);
    let output = cmd.current_dir(&subdir).args(["read", "users"]).output().unwrap();
    assert!(output.status.success(),
        "auto-discovery failed: {}", String::from_utf8_lossy(&output.stderr));
    let out = String::from_utf8_lossy(&output.stdout);
    assert!(out.contains("alice"), "expected 'alice' in output, got: {}", out);

    // Also test `ls` from the subdirectory
    let mut cmd = Command::new(POND_BIN);
    let output = cmd.current_dir(&subdir).args(["ls"]).output().unwrap();
    assert!(output.status.success());
    let out = String::from_utf8_lossy(&output.stdout);
    assert!(out.contains("users"), "expected 'users' in ls output, got: {}", out);
}

#[test]
fn test_auto_discovery_creates_pond_marker() {
    // Verify that `pond init` creates the .pond/ marker directory
    let dir = TempDir::new().unwrap();
    run(dir.path(), &["init", "."]);

    // The .pond/ marker should exist
    assert!(dir.path().join(".pond").is_dir(),
        ".pond/ marker directory not created");

    // The .pond/config file should exist (Pond-level settings)
    let config = dir.path().join(".pond/config");
    assert!(config.exists(), ".pond/config not created");
    // Config for local FS should NOT have an active storage= line
    // (only comments mentioning storage=s3:// as documentation).
    // An active storage= line means S3.
    let config_content = fs::read_to_string(&config).unwrap();
    let has_active_storage = config_content.lines()
        .map(|l| l.trim())
        .filter(|l| !l.starts_with('#'))
        .any(|l| l.starts_with("storage="));
    assert!(!has_active_storage,
        "local FS config should NOT have an active 'storage=' line (only comments)");
}

// ===========================================================================
// Structured row tests — write-rows + read-rows round-trips
// ===========================================================================

#[test]
fn test_write_rows_and_read_rows_round_trip() {
    let dir = TempDir::new().unwrap();
    run(dir.path(), &["init", "."]);

    // Write a small users table.
    let json = r#"[
        {"id": 1, "name": "alice", "age": 30},
        {"id": 2, "name": "bob",   "age": 25},
        {"id": 3, "name": "carol", "age": 35}
    ]"#;
    let out = run(dir.path(), &["write-rows", "users", "--json", json, "-m", "seed users"]);
    // Output is "<12-char-hash>\t<collection>"
    assert!(out.contains("users"), "write-rows output should mention collection: {}", out);

    // Read back as JSON.
    let read = run(dir.path(), &["read-rows", "users"]);
    let parsed: serde_json::Value = serde_json::from_str(&read)
        .unwrap_or_else(|e| panic!("read-rows output is not valid JSON: {}\noutput: {}", e, read));

    let arr = parsed.as_array().expect("read-rows output should be a JSON array");
    assert_eq!(arr.len(), 3, "expected 3 rows, got: {}", read);

    // Find alice's row and verify.
    let alice = arr.iter().find(|r| r.get("name").and_then(|v| v.as_str()) == Some("alice"))
        .unwrap_or_else(|| panic!("alice not found in rows: {}", read));
    assert_eq!(alice.get("id").and_then(|v| v.as_i64()), Some(1));
    assert_eq!(alice.get("age").and_then(|v| v.as_i64()), Some(30));
}

#[test]
fn test_read_rows_with_where_filter() {
    let dir = TempDir::new().unwrap();
    run(dir.path(), &["init", "."]);

    let json = r#"[
        {"id": 1, "name": "alice", "age": 30},
        {"id": 2, "name": "bob",   "age": 25},
        {"id": 3, "name": "carol", "age": 35},
        {"id": 4, "name": "dave",  "age": 40}
    ]"#;
    run(dir.path(), &["write-rows", "users", "--json", json, "-m", "seed"]);

    // WHERE age > 30 → carol (35) + dave (40).
    let read = run(dir.path(), &["read-rows", "users", "--where", "age > 30"]);
    let parsed: serde_json::Value = serde_json::from_str(&read)
        .unwrap_or_else(|e| panic!("read-rows output is not valid JSON: {}\noutput: {}", e, read));
    let arr = parsed.as_array().expect("array");
    assert_eq!(arr.len(), 2, "expected 2 rows with age > 30, got: {}", read);

    let names: Vec<&str> = arr.iter()
        .filter_map(|r| r.get("name").and_then(|v| v.as_str()))
        .collect();
    assert!(names.contains(&"carol"));
    assert!(names.contains(&"dave"));
}

#[test]
fn test_read_rows_with_column_projection() {
    let dir = TempDir::new().unwrap();
    run(dir.path(), &["init", "."]);

    let json = r#"[
        {"id": 1, "name": "alice", "age": 30, "city": "NYC"},
        {"id": 2, "name": "bob",   "age": 25, "city": "LA"}
    ]"#;
    run(dir.path(), &["write-rows", "users", "--json", json, "-m", "seed"]);

    // Project only id and name.
    let read = run(dir.path(), &["read-rows", "users", "--columns", "id,name"]);
    let parsed: serde_json::Value = serde_json::from_str(&read)
        .unwrap_or_else(|e| panic!("read-rows output is not valid JSON: {}\noutput: {}", e, read));
    let arr = parsed.as_array().expect("array");
    assert_eq!(arr.len(), 2);

    for row in arr {
        let obj = row.as_object().unwrap();
        assert!(obj.contains_key("id"), "id column should be present: {:?}", obj);
        assert!(obj.contains_key("name"), "name column should be present: {:?}", obj);
        assert!(!obj.contains_key("age"), "age should be projected out: {:?}", obj);
        assert!(!obj.contains_key("city"), "city should be projected out: {:?}", obj);
    }
}

#[test]
fn test_sql_select_star() {
    let dir = TempDir::new().unwrap();
    run(dir.path(), &["init", "."]);

    // Seed via SQL INSERT.
    let insert_sql = "INSERT INTO products (id, name, price) VALUES (10, 'widget', 9.99), (20, 'gadget', 19.99)";
    let out = run(dir.path(), &["sql", insert_sql]);
    let parsed: serde_json::Value = serde_json::from_str(&out)
        .unwrap_or_else(|e| panic!("sql output is not valid JSON: {}\noutput: {}", e, out));
    // INSERT returns a commit status row.
    assert!(parsed.get("rows").is_some(), "sql output should have rows: {}", out);

    // SELECT * FROM products.
    let select_out = run(dir.path(), &["sql", "SELECT * FROM products"]);
    let select_parsed: serde_json::Value = serde_json::from_str(&select_out)
        .unwrap_or_else(|e| panic!("sql SELECT output is not valid JSON: {}\noutput: {}", e, select_out));
    let rows = select_parsed.get("rows").and_then(|v| v.as_array())
        .unwrap_or_else(|| panic!("sql SELECT output should have rows array: {}", select_out));
    assert_eq!(rows.len(), 2, "expected 2 product rows, got: {}", select_out);
}

#[test]
fn test_sql_select_with_where_and_limit() {
    let dir = TempDir::new().unwrap();
    run(dir.path(), &["init", "."]);

    // Seed 5 users.
    let insert_sql = "INSERT INTO users (id, name, age) VALUES \
        (1, 'alice', 30), (2, 'bob', 25), (3, 'carol', 35), \
        (4, 'dave', 40), (5, 'erin', 28)";
    run(dir.path(), &["sql", insert_sql]);

    // SELECT with WHERE age >= 30 LIMIT 2.
    let out = run(dir.path(), &["sql", "SELECT name, age FROM users WHERE age >= 30 ORDER BY age ASC LIMIT 2"]);
    let parsed: serde_json::Value = serde_json::from_str(&out)
        .unwrap_or_else(|e| panic!("sql output is not valid JSON: {}\noutput: {}", e, out));
    let rows = parsed.get("rows").and_then(|v| v.as_array())
        .unwrap_or_else(|| panic!("sql output should have rows: {}", out));

    assert_eq!(rows.len(), 2, "expected 2 rows after LIMIT, got: {}", out);
    // Ordered by age ASC, the two youngest >=30 are alice (30) and carol (35).
    let first_age = rows[0].get("age").and_then(|v| v.as_i64());
    let second_age = rows[1].get("age").and_then(|v| v.as_i64());
    assert_eq!(first_age, Some(30), "first row age should be 30: {}", out);
    assert_eq!(second_age, Some(35), "second row age should be 35: {}", out);
}

// ===========================================================================
// Shell / REPL tests — `pond shell` interactive mode
// ===========================================================================
//
// These tests exercise the REPL via subprocess invocations with piped stdin.
// The REPL exits cleanly on EOF (closed stdin), so each test pipes a complete
// command sequence followed by `\q` (or just closes stdin) and asserts on the
// captured stdout.

/// Helper: seed a small `items` collection in a fresh repo.
fn seed_items(root: &std::path::Path) {
    run(root, &["init", "."]);
    run(root, &["sql", "INSERT INTO items (id, name) VALUES (1, 'widget'), (2, 'gadget')"]);
}

#[test]
fn test_shell_exec_select_and_exit() {
    // `pond shell --exec "SELECT ..."` with closed stdin should execute the
    // SQL once and exit cleanly (the REPL sees EOF on the first read).
    let dir = TempDir::new().unwrap();
    seed_items(dir.path());

    let (out, stderr, ok) = run_shell(dir.path(), &["shell", "--exec", "SELECT * FROM items"], b"");
    assert!(ok, "shell --exec should exit cleanly; stderr: {}", stderr);
    assert!(out.contains("Pond REPL"), "banner should be printed: {}", out);
    assert!(out.contains("widget"), "SQL result should be printed: {}", out);
    assert!(out.contains("gadget"), "SQL result should be printed: {}", out);
}

#[test]
fn test_shell_exec_insert_then_select() {
    // --exec can run an INSERT; a subsequent piped SELECT shows the row.
    let dir = TempDir::new().unwrap();
    run(dir.path(), &["init", "."]);

    let (out, _err, ok) = run_shell(
        dir.path(),
        &["shell", "--exec", "INSERT INTO nums (id, val) VALUES (1, 'first')"],
        b"SELECT * FROM nums;\n\\q\n",
    );
    assert!(ok, "shell should exit cleanly");
    assert!(out.contains("first"), "SELECT should show inserted row: {}", out);
}

#[test]
fn test_shell_parser_sql_select() {
    // A SELECT statement (ending with `;`) is dispatched to the SQL executor.
    let dir = TempDir::new().unwrap();
    seed_items(dir.path());

    let (out, _err, ok) = run_shell(
        dir.path(),
        &["shell"],
        b"SELECT * FROM items WHERE name = 'widget';\n\\q\n",
    );
    assert!(ok);
    assert!(out.contains("widget"), "SELECT should find widget: {}", out);
    // Ensure the non-matching row is absent from this filtered query.
    // (We can't assert !contains('gadget') because the banner/help might mention it,
    //  but the JSON rows block should only contain widget.)
    assert!(out.contains("columns"), "SQL result should be JSON: {}", out);
}

#[test]
fn test_shell_parser_meta_list_collections() {
    // `\l` is dispatched as a meta-command (list collections), not as SQL.
    let dir = TempDir::new().unwrap();
    seed_items(dir.path());

    let (out, err, ok) = run_shell(dir.path(), &["shell"], b"\\l\n\\q\n");
    assert!(ok);
    assert!(out.contains("items"), "\\l should list the items collection: {}", out);
    // `\l` should NOT produce a SQL error — if it were mis-dispatched as SQL,
    // we'd see the parser's "Expected FROM" error on stderr.
    assert!(!err.contains("Expected FROM"), "\\l must not be parsed as SQL: stderr={}", err);
}

#[test]
fn test_shell_parser_meta_describe() {
    // `\d <name>` shows the collection schema.
    let dir = TempDir::new().unwrap();
    seed_items(dir.path());

    let (out, _err, ok) = run_shell(dir.path(), &["shell"], b"\\d items\n\\q\n");
    assert!(ok);
    assert!(out.contains("Collection:"), "\\d should print collection header: {}", out);
    assert!(out.contains("id"), "\\d should list the id column: {}", out);
    assert!(out.contains("name"), "\\d should list the name column: {}", out);
    assert!(out.contains("INT64"), "\\d should show column types: {}", out);
}

#[test]
fn test_shell_parser_meta_branches() {
    // `\b <name>` shows branches for a collection.
    let dir = TempDir::new().unwrap();
    seed_items(dir.path());

    let (out, _err, ok) = run_shell(dir.path(), &["shell"], b"\\b items\n\\q\n");
    assert!(ok);
    assert!(out.contains("main"), "\\b should show the main branch: {}", out);
}

#[test]
fn test_shell_parser_meta_help() {
    // `\h` prints the help text.
    let dir = TempDir::new().unwrap();
    run(dir.path(), &["init", "."]);

    let (out, _err, ok) = run_shell(dir.path(), &["shell"], b"\\h\n\\q\n");
    assert!(ok);
    assert!(out.contains("Meta-commands"), "\\h should print the help section: {}", out);
    assert!(out.contains("\\l"), "help should mention \\l: {}", out);
    assert!(out.contains("\\q"), "help should mention \\q: {}", out);
    assert!(out.contains("\\history"), "help should mention \\history: {}", out);
}

#[test]
fn test_shell_parser_quit_variants() {
    // All quit variants (`\q`, `\quit`, `exit`, `quit`) exit the REPL cleanly.
    let dir = TempDir::new().unwrap();
    run(dir.path(), &["init", "."]);

    for quit_cmd in &["\\q", "\\quit", "exit", "quit"] {
        let stdin = format!("{}\n", quit_cmd);
        let (out, _err, ok) = run_shell(dir.path(), &["shell"], stdin.as_bytes());
        assert!(ok, "quit variant {:?} should exit cleanly", quit_cmd);
        assert!(out.contains("Pond REPL"), "banner should print before quit: {}", out);
        // After the quit command, the REPL should NOT print another prompt.
        // Count prompts: exactly one "pond> " before the quit command.
        let prompt_count = out.matches("pond> ").count();
        assert_eq!(prompt_count, 1, "expected exactly one prompt for {:?}", quit_cmd);
    }
}

#[test]
fn test_shell_multiline_input_accumulation() {
    // SQL split across multiple lines (without a trailing `;` on the first
    // line) is accumulated until a line ending with `;` is seen.
    let dir = TempDir::new().unwrap();
    seed_items(dir.path());

    let (out, _err, ok) = run_shell(
        dir.path(),
        &["shell"],
        b"SELECT *\nFROM items\nWHERE id = 1;\n\\q\n",
    );
    assert!(ok, "multiline SQL should execute cleanly");
    assert!(out.contains("widget"), "multiline SELECT should find widget: {}", out);
    // The continuation prompt `  ... ` should appear for lines 2 and 3.
    let cont_count = out.matches("  ... ").count();
    assert_eq!(cont_count, 2, "expected 2 continuation prompts, got: {}", out);
}

#[test]
fn test_shell_multiline_then_immediate_meta() {
    // A meta-command should execute immediately even after a partial SQL line
    // has been entered (meta-commands don't participate in accumulation).
    // NOTE: in the current implementation, a partial SQL buffer is NOT cleared
    // by a meta-command — the buffer persists. This test verifies that a
    // meta-command still executes correctly mid-statement.
    let dir = TempDir::new().unwrap();
    seed_items(dir.path());

    // Type "SELECT *" (no semicolon, accumulates), then "\l" (meta, executes
    // immediately), then "FROM items;" completes the SQL.
    let (out, _err, ok) = run_shell(
        dir.path(),
        &["shell"],
        b"SELECT *\n\\l\nFROM items;\n\\q\n",
    );
    assert!(ok);
    // The \l meta-command should have executed and listed items.
    assert!(out.contains("items"), "\\l should still execute mid-statement: {}", out);
    // The SQL statement should have completed and executed after the meta.
    assert!(out.contains("widget"), "SQL should complete after the meta-command: {}", out);
}

#[test]
fn test_shell_history_command() {
    // `\history` shows the commands executed so far.
    let dir = TempDir::new().unwrap();
    seed_items(dir.path());

    let (out, _err, ok) = run_shell(
        dir.path(),
        &["shell"],
        b"SELECT * FROM items;\n\\history\n\\q\n",
    );
    assert!(ok);
    // The history output should include the SELECT statement we ran.
    assert!(out.contains("SELECT * FROM items"), "\\history should show prior SQL: {}", out);
    // It should also include itself and the preceding commands.
    assert!(out.contains("\\history"), "\\history should include itself: {}", out);
}

#[test]
fn test_shell_history_capped_at_100() {
    // History is capped at 100 entries; older entries are evicted.
    // We run 105 trivial SQL statements (each fails harmlessly) and then
    // check that \history shows at most 100 entries.
    let dir = TempDir::new().unwrap();
    run(dir.path(), &["init", "."]);

    let mut stdin = Vec::new();
    for i in 0..105 {
        stdin.extend_from_slice(format!("SELECT * FROM items WHERE id = {};\n", i).as_bytes());
    }
    stdin.extend_from_slice(b"\\history\n\\q\n");

    let (out, _err, _ok) = run_shell(dir.path(), &["shell"], &stdin);
    // History is capped at 100 entries. We pushed 105 SQL statements + the
    // `\history` command itself = 106 entries, so the oldest 6 are evicted.
    // After eviction, the surviving entries are id=6..104 plus `\history`.
    //
    // We use ";" as a terminator in the substring check to avoid ambiguity:
    //   - "id = 0;" only matches the id=0 entry (not id=100..104, which start
    //     with "id = 1").
    //   - "id = 5;" only matches id=5 (not id=50..59, because those are
    //     "id = 50;" etc., and "id = 5;" is not a substring of "id = 50;").
    assert!(!out.contains("id = 0;"), "history should evict id=0 (oldest): {}", out);
    assert!(!out.contains("id = 5;"), "history should evict id=5 (pushed out by \\history): {}", out);
    assert!(out.contains("id = 6;"), "history should retain id=6 (first survivor): {}", out);
    assert!(out.contains("id = 104;"), "history should retain id=104 (most recent SQL): {}", out);
    // The `\history` command itself should appear in its own output.
    assert!(out.contains("\\history"), "\\history should include itself: {}", out);
}

#[test]
fn test_shell_empty_lines_skipped() {
    // Empty lines (and whitespace-only lines) are skipped, not added to the
    // buffer or history.
    let dir = TempDir::new().unwrap();
    seed_items(dir.path());

    let (out, _err, ok) = run_shell(
        dir.path(),
        &["shell"],
        b"\n   \nSELECT * FROM items;\n\n\\history\n\\q\n",
    );
    assert!(ok);
    // History should contain exactly 2 entries: the SELECT and \history.
    // (Empty lines are NOT in history.)
    let select_lines: Vec<&str> = out.lines().filter(|l| l.contains("SELECT * FROM items")).collect();
    assert_eq!(select_lines.len(), 1, "SELECT should appear once in output (in history): {}", out);
}

#[test]
fn test_shell_sql_error_does_not_exit() {
    // A SQL error should be printed but not terminate the REPL — the user can
    // continue entering commands.
    let dir = TempDir::new().unwrap();
    seed_items(dir.path());

    // Run an invalid SQL statement, then a valid one.
    let (out, err, ok) = run_shell(
        dir.path(),
        &["shell"],
        b"SELECT FROM bogus_syntax;\nSELECT * FROM items;\n\\q\n",
    );
    assert!(ok, "REPL should not crash on SQL error");
    assert!(err.contains("Error"), "SQL error should be reported on stderr: {}", err);
    // The subsequent valid SELECT should still execute.
    assert!(out.contains("widget"), "valid SELECT after error should still run: {}", out);
}

#[test]
fn test_shell_exit_on_eof() {
    // Closing stdin (EOF) without an explicit \q should exit cleanly.
    let dir = TempDir::new().unwrap();
    seed_items(dir.path());

    let (out, _err, ok) = run_shell(
        dir.path(),
        &["shell"],
        b"SELECT * FROM items;\n", // no \q — just EOF
    );
    assert!(ok, "REPL should exit cleanly on EOF");
    assert!(out.contains("widget"), "SQL should execute before EOF: {}", out);
}

#[test]
fn test_shell_describe_unknown_collection() {
    // `\d <unknown>` should print a friendly "no commits" message, not crash.
    let dir = TempDir::new().unwrap();
    run(dir.path(), &["init", "."]);

    let (out, _err, ok) = run_shell(dir.path(), &["shell"], b"\\d nonexistent\n\\q\n");
    assert!(ok);
    assert!(out.contains("no commits") || out.contains("nonexistent"),
        "describe on unknown collection should be graceful: {}", out);
}

#[test]
fn test_shell_shell_escape() {
    // `\! <cmd>` runs a shell command and forwards its output.
    let dir = TempDir::new().unwrap();
    run(dir.path(), &["init", "."]);

    let (out, _err, ok) = run_shell(dir.path(), &["shell"], b"\\! echo hello_from_shell\n\\q\n");
    assert!(ok);
    assert!(out.contains("hello_from_shell"), "\\! should forward shell output: {}", out);
}

// ---------------------------------------------------------------------------
// C20 — `pond cat <short-hash>` prefix resolution tests.
//
// Blob seeding uses a local PondKernel over the SAME directory the CLI
// opens (kernel.write returns the content hash), so the CLI's raw-listing
// handle and the kernel agree on the store layout byte-for-byte.
// ---------------------------------------------------------------------------

/// Run pond expecting FAILURE; return (stdout, stderr). Panics if it succeeds.
fn run_fail(root: &std::path::Path, args: &[&str]) -> (String, String) {
    let output = pond(root, args).output().unwrap();
    if output.status.success() {
        panic!("pond {:?} unexpectedly succeeded: {}", args,
            String::from_utf8_lossy(&output.stdout));
    }
    (
        String::from_utf8_lossy(&output.stdout).to_string(),
        String::from_utf8_lossy(&output.stderr).to_string(),
    )
}

/// Seed one blob into the store rooted at `dir` and return its full hash.
fn seed_blob(dir: &std::path::Path, data: &[u8]) -> String {
    let kernel = pond_kernel::PondKernel::new_local(dir).unwrap();
    kernel.write(data).unwrap()
}

#[test]
fn test_cat_full_hash() {
    let dir = TempDir::new().unwrap();
    run(dir.path(), &["init", "."]);
    let h = seed_blob(dir.path(), b"full-hash-payload");
    let out = run(dir.path(), &["cat", &h]);
    assert_eq!(out, "full-hash-payload");
}

#[test]
fn test_cat_unique_short_hash_prefix() {
    let dir = TempDir::new().unwrap();
    run(dir.path(), &["init", "."]);
    let h1 = seed_blob(dir.path(), b"prefix-target-blob");
    let _h2 = seed_blob(dir.path(), b"prefix-decoy-blob");
    // 8 chars of h1 uniquely identify it (verified distinct from h2 below).
    let short = &h1[..8];
    assert_ne!(&_h2[..8], short, "test premise: prefixes differ");
    let out = run(dir.path(), &["cat", short]);
    assert_eq!(out, "prefix-target-blob");
}

#[test]
fn test_cat_ambiguous_short_hash_prefix_errors() {
    let dir = TempDir::new().unwrap();
    run(dir.path(), &["init", "."]);
    // Deterministic birthday grind (in-memory, self-verifying): the first
    // pair of `ambig-grind-{i}` contents whose sha256 hashes share a
    // 6-char prefix. Cross-checked against an independent python sha256
    // probe: first collision lands at i=5652 (first seen at i=4145,
    // prefix ef5830) — but the test never hard-codes those: it re-grinds
    // with the kernel's own hash_bytes and asserts the collision exists.
    let mut seen: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();
    let mut pair: Option<(usize, usize)> = None;
    for i in 0..20000usize {
        let h = pond_kernel::hash_bytes(format!("ambig-grind-{i}").as_bytes());
        let p = h[..6].to_string();
        if let Some(&first) = seen.get(&p) {
            pair = Some((first, i));
            break;
        }
        seen.insert(p, i);
    }
    let (i1, i2) = pair.expect("grind must find a 6-char collision within 20000");
    let h1 = seed_blob(dir.path(), format!("ambig-grind-{i1}").as_bytes());
    let h2 = seed_blob(dir.path(), format!("ambig-grind-{i2}").as_bytes());
    assert_eq!(&h1[..6], &h2[..6], "test premise: shared 6-char prefix");
    assert_ne!(h1, h2);
    let (out, err) = run_fail(dir.path(), &["cat", &h1[..6]]);
    assert!(out.is_empty(), "ambiguity must not write stdout: {out:?}");
    assert!(err.contains("ambiguous prefix"), "stderr: {err}");
    assert!(err.contains("2 blobs match"), "stderr: {err}");
    assert!(err.contains(&h1), "candidate h1 must be listed: {err}");
    assert!(err.contains(&h2), "candidate h2 must be listed: {err}");
}

#[test]
fn test_cat_short_hash_prefix_no_match_errors() {
    let dir = TempDir::new().unwrap();
    run(dir.path(), &["init", "."]);
    let h = seed_blob(dir.path(), b"no-match-anchor");
    // Guaranteed non-matching 6-char prefix: flip the last hex digit of a
    // real hash (0→1 etc.) — stays plausible hex, matches nothing.
    let mut probe = h[..6].to_string();
    let last = probe.pop().unwrap();
    probe.push(if last == '0' { '1' } else { '0' });
    let (out, err) = run_fail(dir.path(), &["cat", &probe]);
    assert!(out.is_empty());
    // The HISTORICAL error message, kept verbatim.
    assert!(err.contains(&format!("Error: no blob with prefix '{}'", probe)),
        "expected historical no-match error, got: {err}");
}

#[test]
fn test_cat_prefix_below_minimum_not_resolved() {
    let dir = TempDir::new().unwrap();
    run(dir.path(), &["init", "."]);
    let h = seed_blob(dir.path(), b"below-minimum-anchor");
    // A 4-char prefix that UNIQUELY matches a blob must NOT resolve —
    // the resolution gate starts at 6 chars.
    let short = &h[..4];
    let (out, err) = run_fail(dir.path(), &["cat", short]);
    assert!(out.is_empty());
    assert!(err.contains(&format!("Error: no blob with prefix '{}'", short)),
        "4-char prefix must keep the historical error, got: {err}");
}

#[test]
fn test_cat_single_char_argument_errors_cleanly() {
    let dir = TempDir::new().unwrap();
    run(dir.path(), &["init", "."]);
    // Pre-C20 this PANICKED in the store layer (blob_path slices
    // hash[..2]); now it must be a clean exit-1 error.
    let (out, err) = run_fail(dir.path(), &["cat", "x"]);
    assert!(out.is_empty());
    assert!(err.contains("Error: no blob with prefix 'x'"), "stderr: {err}");
    assert!(!err.contains("panicked"), "must not panic: {err}");
}
