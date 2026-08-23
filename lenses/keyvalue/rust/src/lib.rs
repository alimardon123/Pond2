// KeyValueLens — key-value lens over Pond's UnifiedStorage (Rust port)
//
// This is the first lens ported from Python to Rust. It demonstrates the
// pattern for future lens ports:
//   1. The lens calls pond_kernel and pond_storage directly (no Python dep)
//   2. The lens is a thin layer over UnifiedStorage — it adds key-value
//      semantics (per-row key→value storage) on top of the collection model
//   3. The lens exposes a C ABI for cross-language access (future)
//
// API parity with Python KeyValueLens (lenses/keyvalue/python/keyvalue_lens.py):
//   - put(collection, key, value)      → stage a key→value mapping
//   - get(collection, key)             → read a single value by key
//   - delete(collection, key)          → stage a deletion (tombstone)
//   - commit(collection, message)      → flush staged changes to storage
//   - get_all(collection)              → read all key→value pairs
//   - keys(collection)                 → list all keys
//   - exists(collection, key)          → check if a key exists
//   - count(collection)                → count rows
//
// STORAGE: Uses UnifiedStorage's write/read API. Each row is stored as a
// JSON object with the user-supplied key as the primary key column.
//
// This is a focused port — it covers the core KV operations. The Python
// lens has additional features (LensQuery, zone maps, attach_indexer)
// that are not yet ported. See lenses/keyvalue/python/keyvalue_lens.py
// for the full API.

use std::collections::HashMap;
use std::sync::Mutex;

use pond_storage::UnifiedStorage;
use serde_json::{Value, json};

/// KeyValueLens — key-value storage over Pond's UnifiedStorage.
///
/// STATELESS read/write engine: does NOT bind to a single collection.
/// Pass the collection name to each operation.
///
/// # Example
/// ```ignore
/// use pond_keyvalue_lens::KeyValueLens;
/// use pond_storage::UnifiedStorage;
///
/// let storage = UnifiedStorage::new_local("/var/lib/pond").unwrap();
/// let lens = KeyValueLens::new(storage);
///
/// lens.put("users", "user:1", &json!({"name": "alice"}));
/// lens.commit("users", "insert alice");
/// let val = lens.get("users", "user:1"); // → Some({"name": "alice"})
/// ```
pub struct KeyValueLens {
    storage: UnifiedStorage,
    // Staged changes: collection → (key → Optional<value>)
    // None means "delete this key"
    staged: Mutex<HashMap<String, HashMap<String, Option<Value>>>>,
}

impl KeyValueLens {
    /// Create a new KeyValueLens over a UnifiedStorage.
    pub fn new(storage: UnifiedStorage) -> Self {
        Self {
            storage,
            staged: Mutex::new(HashMap::new()),
        }
    }

    /// Stage a key→value mapping. Does NOT write to storage until commit().
    ///
    /// The value is a serde_json::Value (any JSON-serializable data).
    pub fn put(&self, collection: &str, key: &str, value: &Value) {
        let mut staged = self.staged.lock().unwrap();
        staged
            .entry(collection.to_string())
            .or_default()
            .insert(key.to_string(), Some(value.clone()));
    }

    /// Stage a deletion (tombstone). Does NOT write to storage until commit().
    pub fn delete(&self, collection: &str, key: &str) {
        let mut staged = self.staged.lock().unwrap();
        staged
            .entry(collection.to_string())
            .or_default()
            .insert(key.to_string(), None);
    }

    /// Commit all staged changes for a collection.
    ///
    /// Reads the current collection state, applies the staged puts/deletes,
    /// and writes the merged result as a new commit.
    pub fn commit(&self, collection: &str, message: &str) -> Result<String, String> {
        let staged = {
            let mut staged = self.staged.lock().unwrap();
            staged.remove(collection).unwrap_or_default()
        };

        if staged.is_empty() {
            return Err(format!("no staged changes for collection '{}'", collection));
        }

        // Read current state (HEAD + shards)
        let current = self.get_all_internal(collection)?;

        // Apply staged changes
        let mut merged: HashMap<String, Value> = current.into_iter().collect();
        for (key, value_opt) in &staged {
            match value_opt {
                Some(v) => { merged.insert(key.clone(), v.clone()); }
                None => { merged.remove(key); }
            }
        }

        // Serialize to JSON array
        let rows: Vec<Value> = merged.iter()
            .map(|(k, v)| {
                // Each row is an object with the key injected as "_key"
                if let Some(obj) = v.as_object() {
                    let mut obj = obj.clone();
                    obj.insert("_key".to_string(), json!(k));
                    Value::Object(obj)
                } else {
                    json!({"_key": k, "value": v})
                }
            })
            .collect();

        let data = serde_json::to_vec(&rows).map_err(|e| e.to_string())?;

        // Write via UnifiedStorage
        let active_branch = self.storage.get_active_branch(collection);
        let commit_hash = pond_storage::write::write(
            self.storage.kernel(),
            collection,
            &active_branch,
            &data,
            message,
        ).map_err(|e| e.to_string())?;

        Ok(commit_hash)
    }

    /// Get a single value by key. Returns None if not found.
    pub fn get(&self, collection: &str, key: &str) -> Option<Value> {
        let all = self.get_all_internal(collection).ok()?;
        all.into_iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v)
    }

    /// Get all key→value pairs in the collection.
    pub fn get_all(&self, collection: &str) -> Result<HashMap<String, Value>, String> {
        self.get_all_internal(collection)
    }

    /// List all keys in the collection.
    pub fn keys(&self, collection: &str) -> Result<Vec<String>, String> {
        let all = self.get_all_internal(collection)?;
        Ok(all.into_keys().collect())
    }

    /// Check if a key exists in the collection.
    pub fn exists(&self, collection: &str, key: &str) -> bool {
        self.get(collection, key).is_some()
    }

    /// Count the number of rows in the collection.
    pub fn count(&self, collection: &str) -> Result<usize, String> {
        let all = self.get_all_internal(collection)?;
        Ok(all.len())
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Read all key→value pairs from storage (HEAD + shards, merged).
    fn get_all_internal(&self, collection: &str) -> Result<HashMap<String, Value>, String> {
        let active_branch = self.storage.get_active_branch(collection);
        let data = pond_storage::read::read_full(
            self.storage.kernel(),
            collection,
            &active_branch,
        );

        let mut result = HashMap::new();
        for blob in data {
            // Each blob is a JSON array of row objects
            let rows: Vec<Value> = serde_json::from_slice(&blob)
                .map_err(|e| format!("failed to parse collection data: {}", e))?;
            for row in rows {
                if let Some(obj) = row.as_object() {
                    if let Some(key) = obj.get("_key").and_then(|k| k.as_str()) {
                        // Strip the _key field from the value
                        let mut value_obj = obj.clone();
                        value_obj.remove("_key");
                        result.insert(key.to_string(), Value::Object(value_obj));
                    }
                }
            }
        }
        Ok(result)
    }

    /// Get a reference to the underlying UnifiedStorage (for branch/merge ops).
    pub fn storage(&self) -> &UnifiedStorage {
        &self.storage
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_lens() -> KeyValueLens {
        let dir = tempfile::tempdir().unwrap();
        let storage = UnifiedStorage::new_local(dir.path()).unwrap();
        // Leak the tempdir so it persists for the test
        std::mem::forget(dir);
        KeyValueLens::new(storage)
    }

    #[test]
    fn test_put_and_get() {
        let lens = make_test_lens();
        lens.put("users", "user:1", &json!({"name": "alice", "age": 30}));
        lens.commit("users", "insert alice").unwrap();

        let val = lens.get("users", "user:1").unwrap();
        assert_eq!(val["name"], "alice");
        assert_eq!(val["age"], 30);
    }

    #[test]
    fn test_get_nonexistent() {
        let lens = make_test_lens();
        assert!(lens.get("users", "user:999").is_none());
    }

    #[test]
    fn test_delete() {
        let lens = make_test_lens();
        lens.put("users", "user:1", &json!({"name": "alice"}));
        lens.commit("users", "insert").unwrap();
        assert!(lens.exists("users", "user:1"));

        lens.delete("users", "user:1");
        lens.commit("users", "delete").unwrap();
        assert!(!lens.exists("users", "user:1"));
    }

    #[test]
    fn test_keys_and_count() {
        let lens = make_test_lens();
        lens.put("users", "user:1", &json!({"name": "alice"}));
        lens.put("users", "user:2", &json!({"name": "bob"}));
        lens.put("users", "user:3", &json!({"name": "carol"}));
        lens.commit("users", "insert 3 users").unwrap();

        let mut keys = lens.keys("users").unwrap();
        keys.sort();
        assert_eq!(keys, vec!["user:1", "user:2", "user:3"]);
        assert_eq!(lens.count("users").unwrap(), 3);
    }

    #[test]
    fn test_get_all() {
        let lens = make_test_lens();
        lens.put("users", "user:1", &json!({"name": "alice"}));
        lens.put("users", "user:2", &json!({"name": "bob"}));
        lens.commit("users", "insert").unwrap();

        let all = lens.get_all("users").unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all.get("user:1").unwrap()["name"], "alice");
        assert_eq!(all.get("user:2").unwrap()["name"], "bob");
    }

    #[test]
    fn test_overwrite_key() {
        let lens = make_test_lens();
        lens.put("users", "user:1", &json!({"name": "alice"}));
        lens.commit("users", "v1").unwrap();

        lens.put("users", "user:1", &json!({"name": "alice v2"}));
        lens.commit("users", "v2").unwrap();

        let val = lens.get("users", "user:1").unwrap();
        assert_eq!(val["name"], "alice v2");
    }

    #[test]
    fn test_commit_with_no_changes_fails() {
        let lens = make_test_lens();
        let result = lens.commit("users", "empty");
        assert!(result.is_err());
    }

    #[test]
    fn test_multiple_collections() {
        let lens = make_test_lens();
        lens.put("users", "user:1", &json!({"name": "alice"}));
        lens.put("orders", "order:1", &json!({"amount": 50.0}));
        lens.commit("users", "insert user").unwrap();
        lens.commit("orders", "insert order").unwrap();

        assert_eq!(lens.count("users").unwrap(), 1);
        assert_eq!(lens.count("orders").unwrap(), 1);
        assert_eq!(lens.get("users", "user:1").unwrap()["name"], "alice");
        assert_eq!(lens.get("orders", "order:1").unwrap()["amount"], 50.0);
    }
}
