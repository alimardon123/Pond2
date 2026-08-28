#![allow(clippy::approx_constant)]
//
// Each test exercises one of the SQL features listed in the task spec:
//   - SELECT * with WHERE
//   - SELECT with JOIN
//   - INSERT + SELECT round-trip
//   - UPDATE with WHERE
//   - DELETE with WHERE
//   - GROUP BY with COUNT/SUM/AVG
//   - ORDER BY ASC/DESC
//   - LIMIT/OFFSET
//   - Subqueries in WHERE
//   - Parquet file reading (basic types, nulls, multiple row groups,
//     end-to-end SQL SELECT)
//   - HAVING with bare aggregates (COUNT(*), AVG, SUM)

use pond_sql::execute;
use pond_storage::UnifiedStorage;
use serde_json::Value as JsonValue;

fn setup() -> tempfile::TempDir {
    tempfile::tempdir().expect("tempdir")
}

fn open_storage(dir: &tempfile::TempDir) -> UnifiedStorage {
    UnifiedStorage::new_local(dir.path()).expect("open storage")
}

/// Helper: insert some rows into a fresh `users` collection.
fn seed_users(storage: &UnifiedStorage) {
    let sql = "INSERT INTO users (id, name, age, city) VALUES \
               (1, 'alice', 30, 'NYC'), \
               (2, 'bob', 25, 'LA'), \
               (3, 'carol', 35, 'NYC'), \
               (4, 'dave', 40, 'SF'), \
               (5, 'erin', 28, 'LA')";
    execute(storage, sql).expect("seed users");
}

fn row_col(row: &JsonValue, col: &str) -> JsonValue {
    if let Some(obj) = row.as_object() {
        if let Some(v) = obj.get(col) {
            return v.clone();
        }
        // Try matching by suffix (`alias.col` → `col`).
        for (k, v) in obj {
            if k.ends_with(&format!(".{}", col)) || k.rsplit('.').next() == Some(col) {
                return v.clone();
            }
        }
    }
    JsonValue::Null
}

#[test]
fn test_select_star_with_where() {
    let dir = setup();
    let storage = open_storage(&dir);
    seed_users(&storage);

    let result = execute(&storage, "SELECT * FROM users WHERE age >= 30")
        .expect("select * where");
    // alice (30), carol (35), dave (40) → 3 rows
    assert_eq!(result.rows.len(), 3);
    let names: Vec<String> = result.rows.iter()
        .map(|r| row_col(r, "name").as_str().unwrap().to_string())
        .collect();
    assert!(names.contains(&"alice".to_string()));
    assert!(names.contains(&"carol".to_string()));
    assert!(names.contains(&"dave".to_string()));
}

#[test]
fn test_insert_select_roundtrip() {
    let dir = setup();
    let storage = open_storage(&dir);

    let insert = execute(
        &storage,
        "INSERT INTO products (id, name, price) VALUES (10, 'widget', 9.99), (20, 'gadget', 19.99)",
    )
    .expect("insert");
    // Insert returns a commit hash.
    assert!(insert.rows[0].get("commit").is_some());

    let select = execute(&storage, "SELECT id, name, price FROM products")
        .expect("select");
    assert_eq!(select.rows.len(), 2);
    // Both rows should be present.
    let ids: Vec<i64> = select.rows.iter()
        .filter_map(|r| row_col(r, "id").as_i64())
        .collect();
    assert!(ids.contains(&10));
    assert!(ids.contains(&20));
}

#[test]
fn test_select_with_join() {
    let dir = setup();
    let storage = open_storage(&dir);

    execute(&storage, "INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob')")
        .expect("seed users");
    execute(
        &storage,
        "INSERT INTO orders (id, user_id, amount) VALUES \
         (100, 1, 50), (101, 1, 75), (102, 2, 30)",
    )
    .expect("seed orders");

    let result = execute(
        &storage,
        "SELECT * FROM users u JOIN orders o ON u.id = o.user_id WHERE u.id = 1",
    )
    .expect("join");

    // alice has 2 orders → 2 joined rows.
    assert_eq!(result.rows.len(), 2);
    for row in &result.rows {
        assert_eq!(row_col(row, "name").as_str(), Some("alice"));
        assert!(row_col(row, "amount").as_i64().is_some());
    }
}

#[test]
fn test_select_left_join() {
    let dir = setup();
    let storage = open_storage(&dir);

    execute(&storage, "INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')")
        .expect("seed users");
    execute(
        &storage,
        "INSERT INTO orders (id, user_id, amount) VALUES (100, 1, 50)",
    )
    .expect("seed orders");

    let result = execute(
        &storage,
        "SELECT * FROM users u LEFT JOIN orders o ON u.id = o.user_id",
    )
    .expect("left join");

    // All 3 users should appear; bob + carol have NULL for order columns.
    assert_eq!(result.rows.len(), 3);
}

#[test]
fn test_update_with_where() {
    let dir = setup();
    let storage = open_storage(&dir);
    seed_users(&storage);

    let result = execute(
        &storage,
        "UPDATE users SET city = 'Boston' WHERE age > 30",
    )
    .expect("update");
    // carol (35) + dave (40) → 2 updated.
    let count = result.rows[0].get("updated").and_then(|v| v.as_u64()).unwrap_or(0);
    assert_eq!(count, 2);

    // Verify via SELECT.
    let select = execute(&storage, "SELECT name, city FROM users WHERE city = 'Boston'")
        .expect("select after update");
    assert_eq!(select.rows.len(), 2);
}

#[test]
fn test_delete_with_where() {
    let dir = setup();
    let storage = open_storage(&dir);
    seed_users(&storage);

    let result = execute(&storage, "DELETE FROM users WHERE age < 30")
        .expect("delete");
    // bob (25) + erin (28) → 2 deleted.
    let count = result.rows[0].get("deleted").and_then(|v| v.as_u64()).unwrap_or(0);
    assert_eq!(count, 2);

    let select = execute(&storage, "SELECT name FROM users").expect("select after delete");
    assert_eq!(select.rows.len(), 3); // 5 - 2
}

#[test]
fn test_group_by_with_count() {
    let dir = setup();
    let storage = open_storage(&dir);
    seed_users(&storage);

    let result = execute(
        &storage,
        "SELECT city, COUNT(*) FROM users GROUP BY city",
    )
    .expect("group by count");

    // NYC: alice + carol = 2, LA: bob + erin = 2, SF: dave = 1 → 3 groups.
    assert_eq!(result.rows.len(), 3);

    let by_city: std::collections::HashMap<String, u64> = result.rows.iter()
        .map(|r| {
            let city = row_col(r, "city").as_str().unwrap().to_string();
            let count = row_col(r, "COUNT(*)").as_u64().unwrap_or(0);
            (city, count)
        })
        .collect();
    assert_eq!(by_city.get("NYC"), Some(&2));
    assert_eq!(by_city.get("LA"), Some(&2));
    assert_eq!(by_city.get("SF"), Some(&1));
}

#[test]
fn test_group_by_with_sum_and_avg() {
    let dir = setup();
    let storage = open_storage(&dir);
    seed_users(&storage);

    let result = execute(
        &storage,
        "SELECT city, SUM(age), AVG(age) FROM users GROUP BY city",
    )
    .expect("group by sum/avg");

    // Find the NYC group: alice (30) + carol (35) → sum=65, avg=32.5.
    let nyc = result.rows.iter()
        .find(|r| row_col(r, "city").as_str() == Some("NYC"))
        .expect("NYC group");
    let sum = nyc.get("SUM(age)").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let avg = nyc.get("AVG(age)").and_then(|v| v.as_f64()).unwrap_or(0.0);
    assert_eq!(sum, 65.0);
    assert!((avg - 32.5).abs() < 1e-6);
}

#[test]
fn test_order_by_asc_desc() {
    let dir = setup();
    let storage = open_storage(&dir);
    seed_users(&storage);

    let asc = execute(&storage, "SELECT name, age FROM users ORDER BY age ASC")
        .expect("order asc");
    let ages_ascending: Vec<i64> = asc.rows.iter()
        .filter_map(|r| row_col(r, "age").as_i64())
        .collect();
    let mut expected = ages_ascending.clone();
    expected.sort();
    assert_eq!(ages_ascending, expected);

    let desc = execute(&storage, "SELECT name, age FROM users ORDER BY age DESC")
        .expect("order desc");
    let ages_descending: Vec<i64> = desc.rows.iter()
        .filter_map(|r| row_col(r, "age").as_i64())
        .collect();
    let mut expected_desc = ages_descending.clone();
    expected_desc.sort();
    expected_desc.reverse();
    assert_eq!(ages_descending, expected_desc);
}

#[test]
fn test_limit_and_offset() {
    let dir = setup();
    let storage = open_storage(&dir);
    seed_users(&storage);

    let limit_only = execute(&storage, "SELECT name FROM users ORDER BY name ASC LIMIT 2")
        .expect("limit only");
    assert_eq!(limit_only.rows.len(), 2);

    let limit_offset = execute(
        &storage,
        "SELECT name FROM users ORDER BY name ASC LIMIT 2 OFFSET 1",
    )
    .expect("limit+offset");
    assert_eq!(limit_offset.rows.len(), 2);
    // Make sure offset actually skipped the first row.
    let first_name = row_col(&limit_offset.rows[0], "name").as_str().unwrap().to_string();
    let first_in_full = execute(&storage, "SELECT name FROM users ORDER BY name ASC")
        .expect("full select");
    let full_first_name = row_col(&first_in_full.rows[0], "name").as_str().unwrap().to_string();
    assert_ne!(first_name, full_first_name);
}

#[test]
fn test_subquery_in_where() {
    let dir = setup();
    let storage = open_storage(&dir);
    seed_users(&storage);
    // Add some orders referencing user ids.
    execute(
        &storage,
        "INSERT INTO orders (id, user_id, amount) VALUES \
         (1, 1, 100), (2, 3, 200), (3, 5, 50)",
    )
    .expect("seed orders");

    // Find users whose id appears in orders.user_id.
    let result = execute(
        &storage,
        "SELECT name FROM users WHERE id IN (SELECT user_id FROM orders)",
    )
    .expect("subquery");

    // user_ids in orders: 1, 3, 5 → alice, carol, erin.
    let names: Vec<String> = result.rows.iter()
        .filter_map(|r| row_col(r, "name").as_str().map(|s| s.to_string()))
        .collect();
    assert_eq!(names.len(), 3);
    assert!(names.contains(&"alice".to_string()));
    assert!(names.contains(&"carol".to_string()));
    assert!(names.contains(&"erin".to_string()));
}

#[test]
fn test_select_file_csv() {
    let dir = setup();
    let csv_path = dir.path().join("data.csv");
    std::fs::write(
        &csv_path,
        "id,name,age\n1,alice,30\n2,bob,25\n3,carol,35\n",
    )
    .expect("write csv");

    let storage = open_storage(&dir);
    let csv_str = csv_path.to_str().unwrap();
    let result = execute(
        &storage,
        &format!("SELECT name FROM '{}' WHERE age > 26", csv_str),
    )
    .expect("select from csv");

    // alice (30), carol (35) → 2 rows.
    assert_eq!(result.rows.len(), 2);
    let names: Vec<String> = result.rows.iter()
        .filter_map(|r| row_col(r, "name").as_str().map(|s| s.to_string()))
        .collect();
    assert!(names.contains(&"alice".to_string()));
    assert!(names.contains(&"carol".to_string()));
}

#[test]
fn test_select_file_json() {
    let dir = setup();
    let json_path = dir.path().join("data.json");
    std::fs::write(
        &json_path,
        r#"[
            {"id": 1, "name": "alice", "active": true},
            {"id": 2, "name": "bob", "active": false},
            {"id": 3, "name": "carol", "active": true}
        ]"#,
    )
    .expect("write json");

    let storage = open_storage(&dir);
    let json_str = json_path.to_str().unwrap();
    let result = execute(
        &storage,
        &format!("SELECT name FROM '{}' WHERE active = true", json_str),
    )
    .expect("select from json");

    // alice + carol → 2 rows.
    assert_eq!(result.rows.len(), 2);
}

#[test]
fn test_merge_statement() {
    let dir = setup();
    let storage = open_storage(&dir);

    execute(
        &storage,
        "INSERT INTO users (id, name, age) VALUES (1, 'alice', 30), (2, 'bob', 25)",
    )
    .expect("seed users");

    // Merge: update existing id=1, insert new id=3.
    let result = execute(
        &storage,
        "MERGE INTO users USING [{\"id\":1,\"age\":31},{\"id\":3,\"name\":\"carol\",\"age\":28}] \
         ON id = id \
         WHEN MATCHED THEN UPDATE \
         WHEN NOT MATCHED THEN INSERT",
    )
    .expect("merge");

    // matched: 1 (id=1), inserted: 1 (id=3).
    let matched = result.rows[0].get("matched").and_then(|v| v.as_u64()).unwrap_or(99);
    let inserted = result.rows[0].get("inserted").and_then(|v| v.as_u64()).unwrap_or(99);
    assert_eq!(matched, 1);
    assert_eq!(inserted, 1);

    // Verify via SELECT.
    let select = execute(&storage, "SELECT id, name, age FROM users ORDER BY id ASC")
        .expect("select after merge");
    // Should be 3 rows total: id=1 (updated), id=2 (unchanged), id=3 (inserted).
    assert_eq!(select.rows.len(), 3);
}

// ---------------------------------------------------------------------------
// Parquet file reading
// ---------------------------------------------------------------------------

/// Build a `RecordBatch` from column arrays + a schema.
///
/// Tiny helper used by the parquet tests below.
fn make_batch(
    schema: arrow::datatypes::Schema,
    arrays: Vec<std::sync::Arc<dyn arrow::array::Array>>,
) -> arrow::record_batch::RecordBatch {
    use std::sync::Arc;
    arrow::record_batch::RecordBatch::try_new(Arc::new(schema), arrays)
        .expect("build record batch")
}

/// Write a parquet file containing `batches`. The writer is configured to
/// use UNCOMPRESSED pages (since pond_sql's parquet dep doesn't enable any
/// compression codecs) and a small row group size to facilitate multi-row
/// group tests.
fn write_parquet(
    path: &std::path::Path,
    batches: &[arrow::record_batch::RecordBatch],
    max_row_group_size: usize,
) {
    use parquet::arrow::arrow_writer::ArrowWriter;
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;
    use std::fs::File;

    let schema = batches[0].schema();
    let file = File::create(path).expect("create parquet file");
    let props = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_max_row_group_size(max_row_group_size)
        .build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props)).expect("open writer");
    for batch in batches {
        writer.write(batch).expect("write batch");
    }
    writer.close().expect("close writer");
}

#[test]
fn test_select_file_parquet_basic_types() {
    use arrow::array::{
        BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
        Int8Array, LargeStringArray, StringArray, UInt16Array, UInt32Array, UInt64Array,
        UInt8Array,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    let dir = setup();
    let path = dir.path().join("types.parquet");

    let schema = Schema::new(vec![
        Field::new("b", DataType::Boolean, false),
        Field::new("i8", DataType::Int8, false),
        Field::new("i16", DataType::Int16, false),
        Field::new("i32", DataType::Int32, false),
        Field::new("i64", DataType::Int64, false),
        Field::new("u8", DataType::UInt8, false),
        Field::new("u16", DataType::UInt16, false),
        Field::new("u32", DataType::UInt32, false),
        Field::new("u64", DataType::UInt64, false),
        Field::new("f32", DataType::Float32, false),
        Field::new("f64", DataType::Float64, false),
        Field::new("s", DataType::Utf8, false),
        Field::new("ls", DataType::LargeUtf8, false),
    ]);

    let batch = make_batch(
        schema,
        vec![
            Arc::new(BooleanArray::from(vec![true, false])),
            Arc::new(Int8Array::from(vec![1i8, -2i8])),
            Arc::new(Int16Array::from(vec![1000i16, -2000i16])),
            Arc::new(Int32Array::from(vec![100_000i32, -100_001i32])),
            Arc::new(Int64Array::from(vec![1_000_000_000i64, -1_000_000_001i64])),
            Arc::new(UInt8Array::from(vec![200u8, 255u8])),
            Arc::new(UInt16Array::from(vec![60_000u16, 65535u16])),
            Arc::new(UInt32Array::from(vec![4_000_000_000u32, 3_999_999_999u32])),
            Arc::new(UInt64Array::from(vec![18_000_000_000_000_000_000u64, 1u64])),
            Arc::new(Float32Array::from(vec![1.5f32, 2.5f32])),
            Arc::new(Float64Array::from(vec![3.14f64, 6.28f64])),
            Arc::new(StringArray::from(vec!["alice", "bob"])),
            Arc::new(LargeStringArray::from(vec!["alpha", "beta"])),
        ],
    );

    write_parquet(&path, &[batch], 1024);

    let storage = open_storage(&dir);
    let pq_str = path.to_str().unwrap();
    let result = execute(&storage, &format!("SELECT * FROM '{}'", pq_str))
        .expect("select from parquet");

    assert_eq!(result.rows.len(), 2);

    let r0 = &result.rows[0];
    assert_eq!(row_col(r0, "b"), JsonValue::Bool(true));
    assert_eq!(row_col(r0, "i8"), JsonValue::Number(serde_json::Number::from(1)));
    assert_eq!(row_col(r0, "i16"), JsonValue::Number(serde_json::Number::from(1000)));
    assert_eq!(row_col(r0, "i32"), JsonValue::Number(serde_json::Number::from(100_000)));
    assert_eq!(row_col(r0, "i64"), JsonValue::Number(serde_json::Number::from(1_000_000_000)));
    assert_eq!(row_col(r0, "u8"), JsonValue::Number(serde_json::Number::from(200u64)));
    assert_eq!(row_col(r0, "u16"), JsonValue::Number(serde_json::Number::from(60_000u64)));
    assert_eq!(row_col(r0, "u32"), JsonValue::Number(serde_json::Number::from(4_000_000_000u64)));
    assert_eq!(row_col(r0, "u64"), JsonValue::Number(serde_json::Number::from(18_000_000_000_000_000_000u64)));
    assert_eq!(row_col(r0, "f32").as_f64(), Some(1.5));
    assert_eq!(row_col(r0, "f64").as_f64(), Some(3.14));
    assert_eq!(row_col(r0, "s"), JsonValue::String("alice".to_string()));
    assert_eq!(row_col(r0, "ls"), JsonValue::String("alpha".to_string()));

    let r1 = &result.rows[1];
    assert_eq!(row_col(r1, "b"), JsonValue::Bool(false));
    assert_eq!(row_col(r1, "i8"), JsonValue::Number(serde_json::Number::from(-2)));
    assert_eq!(row_col(r1, "i32"), JsonValue::Number(serde_json::Number::from(-100_001)));
    assert_eq!(row_col(r1, "u64"), JsonValue::Number(serde_json::Number::from(1u64)));
    assert_eq!(row_col(r1, "f64").as_f64(), Some(6.28));
    assert_eq!(row_col(r1, "s"), JsonValue::String("bob".to_string()));
    assert_eq!(row_col(r1, "ls"), JsonValue::String("beta".to_string()));
}

#[test]
fn test_select_file_parquet_nulls() {
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    let dir = setup();
    let path = dir.path().join("nulls.parquet");

    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("age", DataType::Int32, true),
    ]);

    // id: [1, 2, 3]
    // name: ["alice", NULL, "carol"]
    // age: [NULL, 25, 35]
    let batch = make_batch(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![
                Some("alice"),
                None,
                Some("carol"),
            ])),
            Arc::new(Int32Array::from(vec![None, Some(25), Some(35)])),
        ],
    );

    write_parquet(&path, &[batch], 1024);

    let storage = open_storage(&dir);
    let pq_str = path.to_str().unwrap();
    let result = execute(&storage, &format!("SELECT * FROM '{}'", pq_str))
        .expect("select from parquet with nulls");

    assert_eq!(result.rows.len(), 3);

    let r0 = &result.rows[0];
    assert_eq!(row_col(r0, "id"), JsonValue::Number(serde_json::Number::from(1)));
    assert_eq!(row_col(r0, "name"), JsonValue::String("alice".to_string()));
    assert_eq!(row_col(r0, "age"), JsonValue::Null);

    let r1 = &result.rows[1];
    assert_eq!(row_col(r1, "id"), JsonValue::Number(serde_json::Number::from(2)));
    assert_eq!(row_col(r1, "name"), JsonValue::Null);
    assert_eq!(row_col(r1, "age"), JsonValue::Number(serde_json::Number::from(25)));

    let r2 = &result.rows[2];
    assert_eq!(row_col(r2, "name"), JsonValue::String("carol".to_string()));
    assert_eq!(row_col(r2, "age"), JsonValue::Number(serde_json::Number::from(35)));
}

#[test]
fn test_select_file_parquet_multiple_row_groups() {
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::fs::File;
    use std::sync::Arc;

    let dir = setup();
    let path = dir.path().join("multi_rg.parquet");

    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]);

    // 6 rows; with max_row_group_size=2 this produces 3 row groups.
    let batch = make_batch(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6])),
            Arc::new(StringArray::from(vec![
                "a", "b", "c", "d", "e", "f",
            ])),
        ],
    );

    write_parquet(&path, &[batch], 2);

    // Sanity-check that the parquet file really has 3 row groups — otherwise
    // this test wouldn't actually exercise the "iterate over multiple
    // record batches" code path in read_parquet_file.
    let file = File::open(&path).expect("open parquet for metadata check");
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).expect("builder");
    assert_eq!(builder.metadata().num_row_groups(), 3,
        "parquet file should have 3 row groups (6 rows / max_row_group_size=2)");

    let storage = open_storage(&dir);
    let pq_str = path.to_str().unwrap();
    let result = execute(&storage, &format!("SELECT * FROM '{}' ORDER BY id ASC", pq_str))
        .expect("select from multi-row-group parquet");

    // All 6 rows should be present regardless of how many row groups they
    // span — the reader iterates over every record batch the parquet
    // reader produces.
    assert_eq!(result.rows.len(), 6);
    let ids: Vec<i64> = result.rows.iter()
        .filter_map(|r| row_col(r, "id").as_i64())
        .collect();
    assert_eq!(ids, vec![1, 2, 3, 4, 5, 6]);
    let names: Vec<String> = result.rows.iter()
        .filter_map(|r| row_col(r, "name").as_str().map(|s| s.to_string()))
        .collect();
    assert_eq!(names, vec!["a", "b", "c", "d", "e", "f"]);
}

#[test]
fn test_select_file_parquet_dates_and_timestamps() {
    use arrow::array::{
        Date32Array, Date64Array, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray,
    };
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use std::sync::Arc;

    let dir = setup();
    let path = dir.path().join("dates.parquet");

    let schema = Schema::new(vec![
        Field::new("d32", DataType::Date32, false),
        Field::new("d64", DataType::Date64, false),
        Field::new("ts_s", DataType::Timestamp(TimeUnit::Second, None), false),
        Field::new("ts_ms", DataType::Timestamp(TimeUnit::Millisecond, None), false),
        Field::new("ts_us", DataType::Timestamp(TimeUnit::Microsecond, None), false),
        Field::new("ts_ns", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
    ]);

    // Epoch (1970-01-01 00:00:00 UTC) for every column.
    let batch = make_batch(
        schema,
        vec![
            Arc::new(Date32Array::from(vec![0])),
            Arc::new(Date64Array::from(vec![0])),
            Arc::new(TimestampSecondArray::from(vec![0])),
            Arc::new(TimestampMillisecondArray::from(vec![0])),
            Arc::new(TimestampMicrosecondArray::from(vec![0])),
            Arc::new(TimestampNanosecondArray::from(vec![0])),
        ],
    );

    write_parquet(&path, &[batch], 1024);

    let storage = open_storage(&dir);
    let pq_str = path.to_str().unwrap();
    let result = execute(&storage, &format!("SELECT * FROM '{}'", pq_str))
        .expect("select from parquet with dates");

    assert_eq!(result.rows.len(), 1);
    let r0 = &result.rows[0];
    assert_eq!(row_col(r0, "d32"), JsonValue::String("1970-01-01".to_string()));
    assert_eq!(row_col(r0, "d64"), JsonValue::String("1970-01-01".to_string()));
    assert_eq!(
        row_col(r0, "ts_s"),
        JsonValue::String("1970-01-01T00:00:00".to_string())
    );
    assert_eq!(
        row_col(r0, "ts_ms"),
        JsonValue::String("1970-01-01T00:00:00".to_string())
    );
    assert_eq!(
        row_col(r0, "ts_us"),
        JsonValue::String("1970-01-01T00:00:00".to_string())
    );
    assert_eq!(
        row_col(r0, "ts_ns"),
        JsonValue::String("1970-01-01T00:00:00".to_string())
    );
}

#[test]
fn test_select_file_parquet_end_to_end_sql() {
    use arrow::array::{Float64Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    let dir = setup();
    let path = dir.path().join("people.parquet");

    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, false),
        Field::new("city", DataType::Utf8, false),
        Field::new("salary", DataType::Float64, false),
    ]);

    let batch = make_batch(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec![
                "alice", "bob", "carol", "dave", "erin",
            ])),
            Arc::new(Int32Array::from(vec![30, 25, 35, 40, 28])),
            Arc::new(StringArray::from(vec![
                "NYC", "LA", "NYC", "SF", "LA",
            ])),
            Arc::new(Float64Array::from(vec![
                100_000.0, 80_000.0, 120_000.0, 90_000.0, 75_000.0,
            ])),
        ],
    );

    write_parquet(&path, &[batch], 1024);

    let storage = open_storage(&dir);
    let pq_str = path.to_str().unwrap();

    // WHERE on a parquet file.
    let result = execute(
        &storage,
        &format!("SELECT name, age FROM '{}' WHERE age > 28 ORDER BY age DESC", pq_str),
    )
    .expect("select with where on parquet");
    assert_eq!(result.rows.len(), 3); // alice (30), carol (35), dave (40).
    let ages: Vec<i64> = result.rows.iter()
        .filter_map(|r| row_col(r, "age").as_i64())
        .collect();
    assert_eq!(ages, vec![40, 35, 30]);

    // GROUP BY on a parquet file.
    let grouped = execute(
        &storage,
        &format!(
            "SELECT city, COUNT(*), AVG(salary) FROM '{}' GROUP BY city",
            pq_str
        ),
    )
    .expect("group by on parquet");
    assert_eq!(grouped.rows.len(), 3); // NYC, LA, SF.

    // HAVING with bare AVG on a parquet file — NYC has alice + carol:
    // avg = (100_000 + 120_000) / 2 = 110_000, which is > 100_000.
    let having = execute(
        &storage,
        &format!(
            "SELECT city FROM '{}' GROUP BY city HAVING AVG(salary) > 100000",
            pq_str
        ),
    )
    .expect("having avg on parquet");
    let cities: Vec<String> = having.rows.iter()
        .filter_map(|r| row_col(r, "city").as_str().map(|s| s.to_string()))
        .collect();
    assert_eq!(cities, vec!["NYC".to_string()]);
}

// ---------------------------------------------------------------------------
// HAVING with bare aggregates
// ---------------------------------------------------------------------------

/// Seed an `employees` collection: (id, dept, salary, amount).
///
///   eng:  alice 100, bob 90, carol 110   (3 rows, avg=100, sum=300)
///   sales: dave 60, erin 70              (2 rows, avg=65, sum=130)
///   hr:    frank 50                      (1 row,  avg=50, sum=50)
fn seed_employees(storage: &UnifiedStorage) {
    let sql = "INSERT INTO employees (id, dept, salary, amount) VALUES \
               (1, 'eng', 100000, 500), \
               (2, 'eng', 90000, 300), \
               (3, 'eng', 110000, 200), \
               (4, 'sales', 60000, 100), \
               (5, 'sales', 70000, 50), \
               (6, 'hr', 50000, 25)";
    execute(storage, sql).expect("seed employees");
}

#[test]
fn test_having_count_star() {
    let dir = setup();
    let storage = open_storage(&dir);
    seed_employees(&storage);

    // HAVING COUNT(*) > 1 → eng (3) and sales (2) qualify; hr (1) does not.
    let result = execute(
        &storage,
        "SELECT dept FROM employees GROUP BY dept HAVING COUNT(*) > 1",
    )
    .expect("having count");

    assert_eq!(result.rows.len(), 2);
    let mut depts: Vec<String> = result.rows.iter()
        .filter_map(|r| row_col(r, "dept").as_str().map(|s| s.to_string()))
        .collect();
    depts.sort();
    assert_eq!(depts, vec!["eng".to_string(), "sales".to_string()]);
}

#[test]
fn test_having_count_star_eq_threshold() {
    let dir = setup();
    let storage = open_storage(&dir);
    seed_employees(&storage);

    // HAVING COUNT(*) >= 3 → only eng qualifies.
    let result = execute(
        &storage,
        "SELECT dept FROM employees GROUP BY dept HAVING COUNT(*) >= 3",
    )
    .expect("having count ge");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(row_col(&result.rows[0], "dept").as_str(), Some("eng"));
}

#[test]
fn test_having_avg() {
    let dir = setup();
    let storage = open_storage(&dir);
    seed_employees(&storage);

    // eng avg = 100_000; sales avg = 65_000; hr avg = 50_000.
    // HAVING AVG(salary) > 80000 → only eng.
    let result = execute(
        &storage,
        "SELECT dept FROM employees GROUP BY dept HAVING AVG(salary) > 80000",
    )
    .expect("having avg");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(row_col(&result.rows[0], "dept").as_str(), Some("eng"));

    // HAVING AVG(salary) > 60000 → eng + sales.
    let result = execute(
        &storage,
        "SELECT dept FROM employees GROUP BY dept HAVING AVG(salary) > 60000",
    )
    .expect("having avg 2");
    assert_eq!(result.rows.len(), 2);
}

#[test]
fn test_having_sum() {
    let dir = setup();
    let storage = open_storage(&dir);
    seed_employees(&storage);

    // amount sums: eng=1000, sales=150, hr=25.
    // HAVING SUM(amount) < 1000 → sales + hr.
    let result = execute(
        &storage,
        "SELECT dept FROM employees GROUP BY dept HAVING SUM(amount) < 1000",
    )
    .expect("having sum");
    assert_eq!(result.rows.len(), 2);
    let mut depts: Vec<String> = result.rows.iter()
        .filter_map(|r| row_col(r, "dept").as_str().map(|s| s.to_string()))
        .collect();
    depts.sort();
    assert_eq!(depts, vec!["hr".to_string(), "sales".to_string()]);
}

#[test]
fn test_having_with_aggregate_in_select() {
    let dir = setup();
    let storage = open_storage(&dir);
    seed_employees(&storage);

    // The SELECT list computes COUNT(*) explicitly; HAVING references the
    // same aggregate. Both should work — the executor resolves the
    // HAVING aggregate from the group's rows even when it's already in
    // the SELECT list.
    let result = execute(
        &storage,
        "SELECT dept, COUNT(*) AS n FROM employees GROUP BY dept HAVING COUNT(*) > 1 ORDER BY n DESC",
    )
    .expect("having + select aggregate");

    assert_eq!(result.rows.len(), 2);
    // eng (3) before sales (2).
    assert_eq!(row_col(&result.rows[0], "dept").as_str(), Some("eng"));
    assert_eq!(row_col(&result.rows[0], "n").as_i64(), Some(3));
    assert_eq!(row_col(&result.rows[1], "dept").as_str(), Some("sales"));
    assert_eq!(row_col(&result.rows[1], "n").as_i64(), Some(2));
}

#[test]
fn test_having_no_group_by() {
    let dir = setup();
    let storage = open_storage(&dir);
    seed_employees(&storage);

    // HAVING without GROUP BY: the entire input is one group. COUNT(*) = 6.
    let result = execute(
        &storage,
        "SELECT COUNT(*) FROM employees HAVING COUNT(*) > 5",
    )
    .expect("having no group by");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(row_col(&result.rows[0], "COUNT(*)").as_i64(), Some(6));

    // HAVING that filters out the single group.
    let result = execute(
        &storage,
        "SELECT COUNT(*) FROM employees HAVING COUNT(*) > 100",
    )
    .expect("having no group by filtered");
    assert_eq!(result.rows.len(), 0);
}

// ---------------------------------------------------------------------------
// WHERE pushdown into the pruned reader (read_rows_json_pruned routing)
// ---------------------------------------------------------------------------

/// Shard-updated rows must still appear when the UPDATE moved them INTO the
/// WHERE range — the reader's pre-filter is conservative and the executor's
/// post-merge WHERE eval is authoritative.
#[test]
fn test_where_pushdown_shard_updated_row_appears() {
    let dir = setup();
    let storage = open_storage(&dir);
    seed_users(&storage);

    // UPDATE writes a CRDT shard: bob (25) and erin (28) move to age >= 30.
    // Their HEAD versions still carry age < 30 — the pre-filter would drop
    // them if it were (incorrectly) authoritative.
    execute(&storage, "UPDATE users SET age = 31 WHERE age < 30")
        .expect("update");

    let result = execute(&storage, "SELECT name, age FROM users WHERE age >= 30")
        .expect("select after update");
    // alice (30), carol (35), dave (40), bob (31), erin (31) → 5 rows.
    assert_eq!(result.rows.len(), 5);
    let names: Vec<String> = result.rows.iter()
        .map(|r| row_col(r, "name").as_str().unwrap().to_string())
        .collect();
    for want in ["alice", "carol", "dave", "bob", "erin"] {
        assert!(names.contains(&want.to_string()),
            "shard-updated row {} must appear in the WHERE >= 30 result", want);
    }
}

/// The mirror case: an UPDATE that moves a row OUT of the WHERE range must
/// remove it from the result (the post-merge WHERE filter drops it).
#[test]
fn test_where_pushdown_shard_updated_row_disappears() {
    let dir = setup();
    let storage = open_storage(&dir);
    seed_users(&storage);

    // alice (30) → 22: HEAD still says 30, the shard says 22.
    execute(&storage, "UPDATE users SET age = 22 WHERE name = 'alice'")
        .expect("update");

    let result = execute(&storage, "SELECT name FROM users WHERE age >= 30")
        .expect("select after update");
    // carol (35), dave (40) → 2 rows; alice must be gone.
    assert_eq!(result.rows.len(), 2);
    let names: Vec<String> = result.rows.iter()
        .map(|r| row_col(r, "name").as_str().unwrap().to_string())
        .collect();
    assert!(!names.contains(&"alice".to_string()),
        "shard-updated row moved out of range must not appear");
}

/// Non-conjunctive / non-comparison WHERE shapes take the no-pushdown path
/// and must remain correct.
#[test]
fn test_where_or_like_in_not_pushed() {
    let dir = setup();
    let storage = open_storage(&dir);
    seed_users(&storage);

    // OR — not a conjunction, no pushdown.
    let result = execute(
        &storage,
        "SELECT name FROM users WHERE age < 26 OR age > 38",
    )
    .expect("or");
    // bob (25), erin (28)? no — 28 < 26 false; dave (40) → bob + dave.
    assert_eq!(result.rows.len(), 2);

    // LIKE.
    let result = execute(
        &storage,
        "SELECT name FROM users WHERE name LIKE 'a%'",
    )
    .expect("like");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(row_col(&result.rows[0], "name").as_str(), Some("alice"));

    // IN.
    let result = execute(
        &storage,
        "SELECT name FROM users WHERE city IN ('NYC', 'SF')",
    )
    .expect("in");
    // alice + carol (NYC), dave (SF) → 3.
    assert_eq!(result.rows.len(), 3);

    // NOT — wraps a comparison; the subtree contributes no pushdown.
    let result = execute(
        &storage,
        "SELECT name FROM users WHERE NOT (age >= 30)",
    )
    .expect("not");
    // bob (25) + erin (28) → 2.
    assert_eq!(result.rows.len(), 2);

    // IS NULL.
    let result = execute(
        &storage,
        "SELECT name FROM users WHERE age IS NULL",
    )
    .expect("is null");
    assert_eq!(result.rows.len(), 0);
}

/// A conjunction of comparisons pushes BOTH predicates; the result must
/// match the intersection exactly.
#[test]
fn test_where_pushdown_conjunction() {
    let dir = setup();
    let storage = open_storage(&dir);
    seed_users(&storage);

    let result = execute(
        &storage,
        "SELECT name FROM users WHERE age >= 25 AND age <= 35 AND city = 'LA'",
    )
    .expect("conjunction");
    // bob (25, LA) + erin (28, LA) → 2. alice is NYC, carol is NYC.
    assert_eq!(result.rows.len(), 2);
    let names: Vec<String> = result.rows.iter()
        .map(|r| row_col(r, "name").as_str().unwrap().to_string())
        .collect();
    assert!(names.contains(&"bob".to_string()));
    assert!(names.contains(&"erin".to_string()));
}

/// DELETE + SELECT still observes shard-tombstoned rows correctly with the
/// pruned reader (delete writes tombstone shards; the merge suppresses).
#[test]
fn test_where_pushdown_after_delete() {
    let dir = setup();
    let storage = open_storage(&dir);
    seed_users(&storage);

    execute(&storage, "DELETE FROM users WHERE age >= 30").expect("delete");

    let result = execute(&storage, "SELECT name FROM users WHERE age >= 30")
        .expect("select after delete");
    // alice, carol, dave tombstoned — none remain in range.
    assert_eq!(result.rows.len(), 0);

    let result = execute(&storage, "SELECT name FROM users").expect("all");
    assert_eq!(result.rows.len(), 2);
}
