// Pond CLI — the `pond` command
//
// Uses pond_storage (the Rust UnifiedStorage port) for all storage operations.
// This ensures the CLI uses the SAME code path as the Python UnifiedStorage —
// no duplicate logic, no drift.
//
// Design principles:
//   - DuckDB philosophy: one binary, no server, embedded
//   - Git-style auto-discovery: `pond init` creates a `.pond/` marker;
//     subsequent commands find it by walking up from CWD
//   - Universal storage: accepts any data format (JSON, CSV, raw bytes)
//   - Simple: delegates to pond_storage for all logic
//   - Beautiful: CLI is a thin UI layer over the storage library
//
// STORAGE DISCOVERY (in priority order):
//   1. --root <url>           (explicit override)
//   2. POND_ROOT env var      (explicit override)
//   3. .pond/config file      (auto-discovery — walks up from CWD)
//   4. . (current directory)  (fallback)
//
// The .pond/ marker directory contains a `config` file:
//   - Lives at the storage root (local path or S3 prefix)
//   - For local FS: location is implicit (.pond/'s parent directory)
//   - For S3: .pond/config is a key in the S3 prefix
//   - Config holds Pond-level settings (NOT storage URL — that's passed via --root)

use clap::{Parser, Subcommand};
use pond_core::{pnd2_decode, TypedColumn, VT_BOOLEAN, VT_FLOAT64, VT_INT64, VT_STRING};
use pond_kernel::PondKernel;
use pond_storage::manifest::CollectionManifest;
use pond_storage::{branch, commit, read, write, UnifiedStorage};
use serde_json::{json, Value as JsonValue};
use std::io::{self, Read as IoRead, Write as IoWrite};

#[derive(Parser)]
#[command(name = "pond")]
#[command(version = env!("CARGO_PKG_VERSION"))]
#[command(about = "Content-addressed storage with branching and time-travel")]
struct Cli {
    /// Storage root URL. Overrides .pond/ auto-discovery.
    /// Can be a local path or an S3 URL:
    ///   /var/lib/pond                                   (local filesystem)
    ///   s3://bucket/prefix?region=us-east-1&endpoint=... (S3-compatible)
    ///
    /// If not provided, the CLI auto-discovers a .pond/ marker by walking
    /// up from the current directory (like git finds .git/).
    #[arg(long, env = "POND_ROOT", global = true)]
    root: Option<String>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize a new Pond repository.
    /// Creates a .pond/ marker directory with a config file.
    /// For local FS: `pond init` or `pond init /path`
    /// For S3: `pond init "s3://bucket/prefix?region=..."`
    Init {
        /// Path (local FS) or S3 URL. Defaults to current directory.
        #[arg(default_value = ".")]
        location: String,
    },
    Write { collection: String, #[arg(group = "input")] file: Option<String>,
            #[arg(long, group = "input")] json: Option<String>,
            #[arg(long, group = "input")] bytes: bool,
            #[arg(short, long)] message: Option<String> },
    Read { name_or_hash: String, #[arg(short, long)] output: Option<String> },
    Branch { collection: String, branch_name: String },
    Checkout { collection: String, branch_name: String,
               #[arg(short = 'b', long = "new")] new: bool },
    Merge { collection: String, source_branch: String,
            #[arg(short, long)] into: Option<String>,
            #[arg(short, long)] message: Option<String> },
    Branches { collection: String },
    History { collection: String, #[arg(short, long, default_value = "20")] limit: usize },
    Undo { collection: String, #[arg(default_value = "1")] steps: usize },
    Revert { collection: String, commit_hash: String },
    Ls,
    Cat { hash: String },
    /// Garbage collect unreachable blobs.
    /// Analyzes reachability and optionally deletes dead blobs.
    Gc {
        /// Compute dead blob sizes (slower — reads each dead blob)
        #[arg(long)]
        compute_size: bool,
        /// Dry run — report what would be deleted without deleting
        #[arg(long)]
        dry_run: bool,
    },
    /// Vacuum — delete unreachable blobs with time-travel safety.
    /// Preserves commits younger than preserve_days.
    Vacuum {
        /// Keep commits younger than N days (default 0 = only current HEAD)
        #[arg(short, long, default_value = "0")]
        preserve_days: u32,
        /// Dry run — report what would be deleted without deleting
        #[arg(long)]
        dry_run: bool,
    },
    /// Write structured rows (JSON array of objects) as a PND2 blob.
    /// Column types are inferred automatically (INT64, FLOAT64, STRING, BOOLEAN).
    ///
    /// Example:
    ///   pond write-rows users --json '[{"id":1,"name":"alice"},{"id":2,"name":"bob"}]' -m "seed"
    WriteRows {
        collection: String,
        /// Inline JSON array of row objects.
        #[arg(long, group = "input")]
        json: Option<String>,
        /// Read JSON array from a file (use "-" for stdin).
        #[arg(long, group = "input")]
        file: Option<String>,
        #[arg(short, long)]
        message: Option<String>,
    },
    /// Read structured rows from a collection's HEAD as JSON or aligned table.
    ///
    /// Example:
    ///   pond read-rows users
    ///   pond read-rows users --where "age > 30" --columns id,name --limit 10
    ///   pond read-rows users --format table
    ReadRows {
        collection: String,
        /// Simple WHERE filter: "col op val [AND col op val ...]"
        /// e.g. "age > 30", "city = 'NYC' AND age < 40"
        #[arg(long)]
        r#where: Option<String>,
        /// Comma-separated list of columns to project.
        #[arg(long)]
        columns: Option<String>,
        /// Maximum number of rows to return.
        #[arg(long)]
        limit: Option<usize>,
        /// Output format: "json" (default) or "table".
        #[arg(long, default_value = "json")]
        format: String,
    },
    /// Execute a SQL statement against the storage.
    ///
    /// Example:
    ///   pond sql "SELECT * FROM users WHERE age > 30"
    ///   pond sql "INSERT INTO users (id, name) VALUES (1, 'alice')"
    Sql {
        /// SQL query string.
        query: String,
    },
    /// Start interactive REPL (read-eval-print loop) mode.
    ///
    /// Enters an interactive shell where you can run SQL statements and
    /// meta-commands. SQL statements accumulate until a line ending with `;`
    /// is entered. Meta-commands (starting with `\`) execute immediately.
    ///
    /// Example:
    ///   pond shell
    ///   pond shell --exec "SELECT * FROM users"
    Shell {
        /// Execute a SQL query on startup, then enter the REPL.
        #[arg(long)]
        exec: Option<String>,
    },
    Version,
}

fn main() {
    let cli = Cli::parse();
    match cli.command {
        Commands::Init { location } => {
            cmd_init(&location, cli.root.as_deref());
        }
        Commands::Version => {
            println!("pond {}", env!("CARGO_PKG_VERSION"));
        }
        cmd => {
            // Resolve the storage location using the discovery chain.
            let storage_url = resolve_storage_url(cli.root.as_deref());
            let storage = match open_storage(&storage_url) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("Error: failed to open storage: {}", e);
                    eprintln!("Hint: run 'pond init' first, or use --root / POND_ROOT");
                    std::process::exit(1);
                }
            };
            // CLI-specific: load persisted active branches from kernel refs.
            // The Python UnifiedStorage keeps these in-memory (long-running process).
            // The CLI is a new process each invocation, so we persist via kernel refs
            // at _active_branch/{collection} → blob containing branch name.
            load_persisted_active_branches(&storage);
            match cmd {
                Commands::Write { collection, file, json, bytes, message } => {
                    cmd_write(&storage, &collection, file, json, bytes, message);
                }
                Commands::Read { name_or_hash, output } => {
                    cmd_read(&storage, &name_or_hash, output);
                }
                Commands::Branch { collection, branch_name } => {
                    cmd_branch(&storage, &collection, &branch_name);
                }
                Commands::Checkout { collection, branch_name, new } => {
                    cmd_checkout(&storage, &collection, &branch_name, new);
                }
                Commands::Merge { collection, source_branch, into, message } => {
                    cmd_merge(&storage, &collection, &source_branch, into, message);
                }
                Commands::Branches { collection } => {
                    cmd_branches(&storage, &collection);
                }
                Commands::History { collection, limit } => {
                    cmd_history(&storage, &collection, limit);
                }
                Commands::Undo { collection, steps } => {
                    cmd_undo(&storage, &collection, steps);
                }
                Commands::Revert { collection, commit_hash } => {
                    cmd_revert(&storage, &collection, &commit_hash);
                }
                Commands::Ls => { cmd_ls(&storage); }
                Commands::Cat { hash } => { cmd_cat(&storage, &hash); }
                Commands::Gc { compute_size, dry_run } => {
                    cmd_gc(&storage, compute_size, dry_run);
                }
                Commands::Vacuum { preserve_days, dry_run } => {
                    cmd_vacuum(&storage, preserve_days, dry_run);
                }
                Commands::WriteRows { collection, json, file, message } => {
                    cmd_write_rows(&storage, &collection, json, file, message);
                }
                Commands::ReadRows { collection, r#where, columns, limit, format } => {
                    cmd_read_rows(&storage, &collection, r#where, columns, limit, &format);
                }
                Commands::Sql { query } => {
                    cmd_sql(&storage, &query);
                }
                Commands::Shell { exec } => {
                    // Shell takes ownership of storage — it's the only command
                    // in this dispatch arm that runs a long-lived REPL loop.
                    cmd_shell(storage, exec);
                }
                _ => unreachable!(),
            }
        }
    }
}

/// Load persisted active branches from kernel refs.
/// The CLI persists active branch at _active_branch/{collection} → blob containing branch name.
/// This is a CLI-specific persistence layer (the Python UnifiedStorage keeps it in-memory).
fn load_persisted_active_branches(storage: &UnifiedStorage) {
    let kernel = storage.kernel();
    // Find all _active_branch/ refs
    let names = kernel.list_names_prefix("_active_branch/");
    for name in names {
        if let Some(hash) = kernel.resolve(&name) {
            if let Ok(data) = kernel.read_blob(&hash) {
                let branch = String::from_utf8_lossy(&data).to_string();
                if let Some(collection) = name.strip_prefix("_active_branch/") {
                    storage.set_active_branch(collection, &branch);
                }
            }
        }
    }
}

/// Persist the active branch for a collection to a kernel ref.
fn persist_active_branch(storage: &UnifiedStorage, collection: &str, branch: &str) {
    let kernel = storage.kernel();
    let ref_name = format!("_active_branch/{}", collection);
    if let Ok(h) = kernel.write(branch.as_bytes()) {
        let _ = kernel.reference(&ref_name, &h);
    }
}

// ---------------------------------------------------------------------------
// Storage discovery (git-style .pond/ marker)
// ---------------------------------------------------------------------------

/// Resolve the storage URL using the discovery chain:
///   1. --root <url>           (explicit override — required for S3)
///   2. POND_ROOT env var      (explicit override — required for S3)
///   3. .pond/ marker in CWD   (auto-discovery — local FS only, walks up from CWD)
///   4. . (current directory)  (fallback)
///
/// Note: S3 storage requires --root or POND_ROOT (no auto-discovery for remote).
/// This is intentional — you can't "walk up" to find an S3 URL.
fn resolve_storage_url(explicit_root: Option<&str>) -> String {
    // 1. Explicit --root or POND_ROOT env var
    if let Some(root) = explicit_root {
        return root.to_string();
    }

    // 3. Auto-discover .pond/ by walking up from CWD (local FS only)
    if let Some(pond_dir) = find_pond_marker() {
        let repo_root = pond_dir.parent().unwrap_or(std::path::Path::new("."));
        // For local FS, the .pond/ marker's parent IS the storage root.
        // (No need to read .pond/config — location is implicit.)
        return repo_root.to_string_lossy().to_string();
    }

    // 4. Fallback: current directory
    ".".to_string()
}

/// Walk up from CWD looking for a `.pond/` directory (like git finds `.git/`).
fn find_pond_marker() -> Option<std::path::PathBuf> {
    let cwd = std::env::current_dir().ok()?;
    let mut current: &std::path::Path = &cwd;
    loop {
        let pond_dir = current.join(".pond");
        if pond_dir.is_dir() {
            return Some(pond_dir);
        }
        current = current.parent()?;
    }
}

/// Get a human-readable description of the storage connection for display.
fn describe_storage(url: &str) -> String {
    if let Some(bucket) = url.strip_prefix("s3://") {
        let bucket_end = bucket.find('/').unwrap_or(bucket.len());
        let bucket_name = &bucket[..bucket_end];
        let prefix = &bucket[bucket_end..];
        // Check if it's R2, MinIO, etc. from the endpoint param
        let endpoint = url.split("endpoint=").nth(1).unwrap_or("");
        let provider = if endpoint.contains("r2.cloudflarestorage.com") {
            "Cloudflare R2"
        } else if endpoint.contains("localhost") || endpoint.contains("127.0.0.1") {
            "MinIO/Local"
        } else if endpoint.is_empty() {
            "AWS S3"
        } else {
            "S3-compatible"
        };
        format!("{} ({}: s3://{}/{})", provider, bucket_name, bucket_name, prefix)
    } else {
        let path = std::path::Path::new(url);
        let abs = if path.is_absolute() {
            path.to_path_buf()
        } else {
            std::env::current_dir().unwrap_or_default().join(path)
        };
        format!("local FS: {}", abs.display())
    }
}

// ---------------------------------------------------------------------------
// Command implementations — thin wrappers over pond_storage
// ---------------------------------------------------------------------------

/// Open a storage backend from a root URL/path.
///
/// Auto-detects:
///   - `s3://bucket/prefix?...` → S3-compatible storage
///   - `/path/to/dir` or `.` → local filesystem
fn open_storage(root: &str) -> Result<UnifiedStorage, Box<dyn std::error::Error>> {
    if root.starts_with("s3://") {
        #[cfg(feature = "s3")]
        {
            let store = pond_s3::S3ObjectStore::from_url(root)?;
            let kernel = PondKernel::new_with_store(Box::new(store));
            Ok(UnifiedStorage::new(kernel))
        }
        #[cfg(not(feature = "s3"))]
        {
            Err(format!(
                "S3 support not compiled in (built with --no-default-features). \
                 Rebuild with `cargo build` (default features include s3)."
            ).into())
        }
    } else if let Some(path) = root.strip_prefix("file://") {
        UnifiedStorage::new_local(path).map_err(|e| e.into())
    } else {
        UnifiedStorage::new_local(root).map_err(|e| e.into())
    }
}

/// Initialize or connect to a Pond repository.
///
/// Behavior:
///   - `pond init` (no path) → create `.pond/` in CWD (local FS)
///   - `pond init /path` → create `.pond/` at /path (local FS); just connect if exists
///   - `pond init "s3://..."` → create `.pond/config` IN S3; just connect if exists
///
/// The `.pond/config` ALWAYS lives at the storage root (not in CWD for remote).
/// For local FS: `.pond/` is a directory in the storage path.
/// For S3: `.pond/config` is a key in the S3 prefix.
///
/// If `--root` is provided, it overrides the location argument.
fn cmd_init(location: &str, explicit_root: Option<&str>) {
    let location = explicit_root.unwrap_or(location);

    if location.starts_with("s3://") {
        cmd_init_s3(location);
    } else {
        cmd_init_local(location);
    }
}

/// Initialize/connect to a local FS storage.
/// `.pond/` lives in the storage path (not CWD).
fn cmd_init_local(location: &str) {
    let path_stripped = location.strip_prefix("file://").unwrap_or(location);
    let base_path = std::path::Path::new(path_stripped);
    let pond_dir = base_path.join(".pond");
    let blobs_dir = base_path.join("blobs");

    let already_initialized = pond_dir.is_dir();

    if !already_initialized {
        // Create blobs/ directory
        if let Err(e) = std::fs::create_dir_all(&blobs_dir) {
            eprintln!("Error: failed to create blobs directory: {}", e);
            std::process::exit(1);
        }
        // Create .pond/ marker with Pond-level config
        if let Err(e) = std::fs::create_dir_all(&pond_dir) {
            eprintln!("Error: failed to create .pond/ marker: {}", e);
            std::process::exit(1);
        }
        // Config holds Pond-level settings (NOT storage= — location is implicit)
        let config = "# Pond repository configuration\n\
            # Storage location: this directory (local filesystem)\n";
        if let Err(e) = std::fs::write(pond_dir.join("config"), config) {
            eprintln!("Error: failed to write .pond/config: {}", e);
            std::process::exit(1);
        }
    }

    // Display connection info
    let abs_path = if base_path.is_absolute() {
        base_path.to_path_buf()
    } else {
        std::env::current_dir().unwrap_or_default().join(base_path)
    };
    println!("Connected to: local FS: {}", abs_path.display());
    if already_initialized {
        println!("(already initialized)");
    } else {
        println!("Initialized empty Pond repository.");
    }
    println!("\nNow you can run:");
    println!("  pond write users --json '[{{\"id\":1}}]' -m \"first commit\"");
}

/// Initialize/connect to S3-compatible storage.
/// `.pond/config` lives IN S3 (not in local CWD).
fn cmd_init_s3(url: &str) {
    #[cfg(feature = "s3")]
    {
        match pond_s3::S3ObjectStore::from_url(url) {
            Ok(store) => {
                use pond_kernel::ObjectStore;
                // Check if .pond/config already exists in S3
                let existing_config = store.get_path(".pond/config");
                let already_initialized = existing_config.is_some();

                // Write .pond/config to S3 (create or update)
                let config = format!(
                    "# Pond repository configuration\n\
                     # Storage: S3-compatible ({})\n",
                    url
                );
                // Store the config as a blob, then reference it at .pond/config
                if let Err(e) = store.put_path(".pond/config", &pond_kernel::hash_bytes(config.as_bytes())) {
                    eprintln!("Error: failed to write .pond/config to S3: {}", e);
                    std::process::exit(1);
                }
                // Also need to write the actual config content as a blob
                let config_hash = pond_kernel::hash_bytes(config.as_bytes());
                let _ = store.put_blob(config.as_bytes());
                let _ = store.put_path(".pond/config", &config_hash);

                // Verify connectivity
                match store.list_paths("") {
                    Ok(_) => {
                        // Display connection info
                        println!("Connected to: {}", describe_storage(url));
                        if already_initialized {
                            println!("(already initialized — .pond/config exists in S3)");
                        } else {
                            println!("Created .pond/config in S3.");
                        }
                        println!("\nTo use this storage in future commands:");
                        println!("  pond --root \"{}\" write users --json '[{{\"id\":1}}]' -m \"first\"", url);
                        println!("\nOr set POND_ROOT:");
                        println!("  export POND_ROOT=\"{}\"", url);
                    }
                    Err(e) => {
                        eprintln!("Error: cannot access S3 storage: {}", e);
                        eprintln!("Hint: check your credentials and endpoint URL.");
                        std::process::exit(1);
                    }
                }
            }
            Err(e) => {
                eprintln!("Error: invalid S3 URL: {}", e);
                std::process::exit(1);
            }
        }
    }
    #[cfg(not(feature = "s3"))]
    {
        eprintln!("Error: S3 support not compiled in.");
        eprintln!("Hint: rebuild with `cargo build` (default features include s3).");
        std::process::exit(1);
    }
}

fn cmd_write(storage: &UnifiedStorage, collection: &str, file: Option<String>,
             json: Option<String>, bytes: bool, message: Option<String>) {
    let data: Vec<u8> = if let Some(j) = json {
        match serde_json::from_str::<serde_json::Value>(&j) {
            Ok(_) => j.into_bytes(),
            Err(e) => { eprintln!("Error: invalid JSON: {}", e); std::process::exit(1); }
        }
    } else if bytes || file.as_deref() == Some("-") {
        let mut buf = Vec::new();
        io::stdin().read_to_end(&mut buf).unwrap();
        buf
    } else if let Some(path) = file {
        std::fs::read(&path).unwrap_or_else(|e| {
            eprintln!("Error: failed to read {}: {}", path, e); std::process::exit(1);
        })
    } else {
        eprintln!("Error: no input provided. Use <file>, --json, or --bytes");
        std::process::exit(1);
    };

    let active = storage.get_active_branch(collection);
    match write::write(storage.kernel(), collection, &active, &data,
                       &message.unwrap_or_default()) {
        Ok(hash) => println!("{}\t{}", &hash[..12], collection),
        Err(e) => { eprintln!("Error: {}", e); std::process::exit(1); }
    }
}

fn cmd_read(storage: &UnifiedStorage, name_or_hash: &str, output: Option<String>) {
    let kernel = storage.kernel();
    // Try as collection name first (active branch), then as hash
    let active = storage.get_active_branch(name_or_hash);
    let data = match read::read(kernel, name_or_hash, &active) {
        Ok(data) => data,
        Err(_) => {
            // Try as hash or flat ref
            match kernel.read(name_or_hash) {
                Ok(data) => data,
                Err(e) => {
                    eprintln!("Error: '{}': {}", name_or_hash, e);
                    std::process::exit(1);
                }
            }
        }
    };

    if let Some(path) = output {
        std::fs::write(&path, &data).unwrap_or_else(|e| {
            eprintln!("Error: {}", e); std::process::exit(1);
        });
    } else {
        io::stdout().write_all(&data).unwrap_or_else(|e| {
            eprintln!("Error: {}", e); std::process::exit(1);
        });
    }
}

fn cmd_branch(storage: &UnifiedStorage, collection: &str, branch_name: &str) {
    let active = storage.get_active_branch(collection);
    match branch::branch(storage.kernel(), collection, branch_name, &active) {
        Ok(hash) => println!("Created branch '{}' at {}", branch_name, &hash[..12]),
        Err(e) => { eprintln!("Error: {}", e); std::process::exit(1); }
    }
}

fn cmd_checkout(storage: &UnifiedStorage, collection: &str, branch_name: &str, new: bool) {
    if new {
        let active = storage.get_active_branch(collection);
        match branch::checkout_new(storage.kernel(), collection, branch_name, &active) {
            Ok(_) => {}
            Err(e) => { eprintln!("Error: {}", e); std::process::exit(1); }
        }
    } else {
        match branch::checkout(storage.kernel(), collection, branch_name) {
            Ok(_) => {}
            Err(e) => { eprintln!("Error: {}", e); std::process::exit(1); }
        }
    }
    storage.set_active_branch(collection, branch_name);
    persist_active_branch(storage, collection, branch_name);
    println!("Switched to branch '{}'", branch_name);
}

fn cmd_merge(storage: &UnifiedStorage, collection: &str, source_branch: &str,
             into: Option<String>, message: Option<String>) {
    let target = into.unwrap_or_else(|| storage.get_active_branch(collection));
    match branch::merge(storage.kernel(), collection, source_branch, &target,
                        &message.unwrap_or_default()) {
        Ok(hash) => println!("Merge commit {} ('{}' → '{}')", &hash[..12], source_branch, target),
        Err(e) => { eprintln!("Error: {}", e); std::process::exit(1); }
    }
}

fn cmd_branches(storage: &UnifiedStorage, collection: &str) {
    let kernel = storage.kernel();
    let branches = branch::list_branches(kernel, collection);
    let active = storage.get_active_branch(collection);
    if branches.is_empty() {
        if kernel.resolve(collection).is_some() {
            println!("* main");
        } else {
            println!("(no branches)");
        }
        return;
    }
    for b in branches {
        let marker = if b == active { "*" } else { " " };
        let hash = kernel.resolve(&pond_storage::branch_ref(collection, &b))
            .unwrap_or_default();
        let prefix = if hash.len() >= 12 { &hash[..12] } else { &hash };
        println!("{} {}\t{}", marker, b, prefix);
    }
}

fn cmd_history(storage: &UnifiedStorage, collection: &str, limit: usize) {
    let kernel = storage.kernel();
    let active = storage.get_active_branch(collection);
    let head = kernel.resolve(&pond_storage::branch_ref(collection, &active))
        .or_else(|| kernel.resolve(collection));

    match head {
        Some(h) => {
            let hist = commit::history(kernel, &h, limit);
            if hist.is_empty() {
                println!("(no commits)");
            } else {
                for (hash, commit) in hist {
                    let merge_marker = if commit.is_merge() { " (merge)" } else { "" };
                    println!("{}\t{}{}", &hash[..12], commit.message, merge_marker);
                }
            }
        }
        None => println!("(no commits)"),
    }
}

fn cmd_undo(storage: &UnifiedStorage, collection: &str, steps: usize) {
    let active = storage.get_active_branch(collection);
    match branch::undo(storage.kernel(), collection, &active, steps) {
        Ok(hash) => println!("Undo {} → now at {}", steps, &hash[..12]),
        Err(e) => { eprintln!("Error: {}", e); std::process::exit(1); }
    }
}

fn cmd_revert(storage: &UnifiedStorage, collection: &str, commit_hash: &str) {
    let active = storage.get_active_branch(collection);
    match branch::revert(storage.kernel(), collection, &active, commit_hash) {
        Ok(()) => println!("Reverted to {}", &commit_hash[..12]),
        Err(e) => { eprintln!("Error: {}", e); std::process::exit(1); }
    }
}

fn cmd_ls(storage: &UnifiedStorage) {
    let kernel = storage.kernel();
    let names = kernel.list_names();
    if names.is_empty() { println!("(no collections)"); return; }
    let mut collections: Vec<String> = names.iter()
        .filter(|n| n.starts_with("collections/"))
        .filter_map(|n| n.strip_prefix("collections/").and_then(|s| s.split('/').next()))
        .map(|s| s.to_string())
        .collect();
    for n in &names {
        if !n.starts_with("collections/") && !n.contains('/') {
            collections.push(n.clone());
        }
    }
    collections.sort(); collections.dedup();
    for name in collections {
        let active = storage.get_active_branch(&name);
        let hash = kernel.resolve(&pond_storage::branch_ref(&name, &active))
            .or_else(|| kernel.resolve(&name))
            .unwrap_or_default();
        let prefix = if hash.len() >= 12 { &hash[..12] } else { &hash };
        println!("{}\t{}", prefix, name);
    }
}

fn cmd_cat(storage: &UnifiedStorage, hash: &str) {
    let kernel = storage.kernel();
    match kernel.read_blob(hash) {
        Ok(data) => { io::stdout().write_all(&data).unwrap(); }
        Err(_) if hash.len() < 64 => {
            let matches = kernel.list_blobs_prefix(hash);
            if matches.len() == 1 {
                kernel.read_blob(&matches[0]).map(|d| { io::stdout().write_all(&d).unwrap(); }).ok();
                return;
            } else if matches.is_empty() {
                eprintln!("Error: no blob with prefix '{}'", hash);
            } else {
                eprintln!("Error: ambiguous prefix '{}'", hash);
            }
            std::process::exit(1);
        }
        Err(e) => { eprintln!("Error: '{}': {}", hash, e); std::process::exit(1); }
    }
}


/// Garbage collect — analyze reachability and optionally delete dead blobs.
fn cmd_gc(storage: &UnifiedStorage, compute_size: bool, dry_run: bool) {
    let kernel = storage.kernel();
    let gc = pond_storage::maintenance::GarbageCollector::new(kernel);

    let stats = gc.collect(None, compute_size);

    println!("GC Analysis:");
    println!("  Live blobs:     {}", stats.live);
    println!("  Dead blobs:     {}", stats.dead);

    if stats.dead_size_bytes >= 0 {
        println!("  Dead size:      {} bytes", stats.dead_size_bytes);
    } else {
        println!("  Dead size:      (use --compute-size to calculate)");
    }

    if dry_run && stats.dead > 0 {
        println!("\n  (dry run — no blobs deleted)");
        println!("  Would delete {} blobs:", stats.dead);
        for hash in stats.dead_hashes.iter().take(20) {
            println!("    {}", hash);
        }
        if stats.dead > 20 {
            println!("    ... and {} more", stats.dead - 20);
        }
    } else if stats.dead > 0 {
        let result = gc.vacuum(None, 0, false);
        println!("\nVacuumed: deleted {} blobs, preserved {}", result.deleted, result.preserved);
    } else {
        println!("\nNo dead blobs to clean up.");
    }
}

/// Vacuum — delete dead blobs with time-travel safety.
fn cmd_vacuum(storage: &UnifiedStorage, preserve_days: u32, dry_run: bool) {
    let kernel = storage.kernel();
    let gc = pond_storage::maintenance::GarbageCollector::new(kernel);

    // First analyze
    let stats = gc.collect(None, false);
    println!("Before vacuum:");
    println!("  Live blobs: {}", stats.live);
    println!("  Dead blobs: {}", stats.dead);

    if dry_run {
        println!("\n  (dry run — no blobs deleted)");
        println!("  Would delete {} blobs (preserving last {} days)", stats.dead, preserve_days);
    } else if stats.dead > 0 {
        let result = gc.vacuum(None, preserve_days, false);
        println!("\nVacuum complete:");
        println!("  Deleted:   {} blobs", result.deleted);
        println!("  Preserved: {} blobs", result.preserved);
    } else {
        println!("\nNo dead blobs to vacuum.");
    }
}

// ===========================================================================
// Structured row commands — write-rows, read-rows, sql
// ===========================================================================

/// write-rows: parse a JSON array of objects, infer column types, encode as
/// PND2 via `pond_storage::write::write_rows`, and print the commit hash.
fn cmd_write_rows(
    storage: &UnifiedStorage,
    collection: &str,
    json_arg: Option<String>,
    file_arg: Option<String>,
    message: Option<String>,
) {
    let raw: Vec<u8> = if let Some(j) = json_arg {
        match serde_json::from_str::<JsonValue>(&j) {
            Ok(_) => j.into_bytes(),
            Err(e) => {
                eprintln!("Error: invalid JSON: {}", e);
                std::process::exit(1);
            }
        }
    } else if let Some(path) = file_arg {
        if path == "-" {
            let mut buf = Vec::new();
            io::stdin().read_to_end(&mut buf).unwrap();
            buf
        } else {
            std::fs::read(&path).unwrap_or_else(|e| {
                eprintln!("Error: failed to read {}: {}", path, e);
                std::process::exit(1);
            })
        }
    } else {
        eprintln!("Error: no input provided. Use --json '<json array>' or --file <path>");
        std::process::exit(1);
    };

    let parsed: Vec<JsonValue> = match serde_json::from_slice(&raw) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("Error: input must be a JSON array of objects: {}", e);
            std::process::exit(1);
        }
    };

    if parsed.is_empty() {
        eprintln!("Error: empty JSON array — nothing to write");
        std::process::exit(1);
    }

    // Infer column types and build TypedColumn values.
    let typed = json_to_typed_columns(&parsed);

    if typed.is_empty() {
        eprintln!("Error: no columns inferred from input rows");
        std::process::exit(1);
    }

    // Verify all columns have the same length.
    let n_rows = parsed.len();
    for (name, col) in &typed {
        if col.len() != n_rows {
            eprintln!(
                "Error: column '{}' has {} values, expected {} (all rows must have the same keys)",
                name,
                col.len(),
                n_rows
            );
            std::process::exit(1);
        }
    }

    // Build the slice-of-tuples that write_rows expects.
    let columns_ref: Vec<(&str, TypedColumn)> = typed
        .iter()
        .map(|(name, col)| (name.as_str(), col.clone()))
        .collect();

    let active = storage.get_active_branch(collection);
    match write::write_rows(
        storage.kernel(),
        collection,
        &active,
        &columns_ref,
        &message.unwrap_or_default(),
    ) {
        Ok(hash) => println!("{}\t{}", &hash[..12], collection),
        Err(e) => {
            eprintln!("Error: write_rows failed: {}", e);
            std::process::exit(1);
        }
    }
}

/// read-rows: load HEAD manifest, decode PND2 row groups, apply WHERE/columns/
/// LIMIT, print as JSON array or aligned table.
fn cmd_read_rows(
    storage: &UnifiedStorage,
    collection: &str,
    where_filter: Option<String>,
    columns: Option<String>,
    limit: Option<usize>,
    format: &str,
) {
    let rows = match read_rows_as_json(storage, collection, where_filter.as_deref(), columns.as_deref(), limit) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
    };
    print_rows(&rows, format);
}

/// sql: delegate to `pond_sql::execute`, print results as JSON.
fn cmd_sql(storage: &UnifiedStorage, query: &str) {
    let result = match pond_sql::execute(storage, query) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Error: SQL execution failed: {}", e);
            std::process::exit(1);
        }
    };

    // Output: a JSON object with `columns` and `rows`.
    let out = json!({
        "columns": result.columns,
        "rows": result.rows,
    });
    println!("{}", serde_json::to_string_pretty(&out).unwrap_or_else(|_| "{}".to_string()));
}

// ---------------------------------------------------------------------------
// Helpers for write-rows / read-rows / sql
// ---------------------------------------------------------------------------

/// Infer column types from a JSON array of row objects.
///
/// Type inference rules (per column, looking at all non-null values):
///   - All integers        → INT64
///   - All numbers (mix)    → FLOAT64
///   - All strings          → STRING
///   - All booleans         → BOOLEAN
///   - Mixed or all-null    → STRING (safe default; nulls become "")
///
/// Returns a Vec<(column_name, TypedColumn)> in the order columns first
/// appear in the input rows.
fn json_to_typed_columns(rows: &[JsonValue]) -> Vec<(String, TypedColumn)> {
    use std::collections::BTreeMap;

    let mut cols: BTreeMap<String, ColInfo> = BTreeMap::new();
    let mut order_counter = 0usize;

    for row in rows {
        let obj = match row.as_object() {
            Some(o) => o,
            None => continue,
        };
        for (k, v) in obj {
            let info = cols.entry(k.clone()).or_insert_with(|| {
                let i = ColInfo {
                    order: order_counter,
                    ..Default::default()
                };
                order_counter += 1;
                i
            });
            info.values.push(v.clone());
            match v {
                JsonValue::Null => info.null_count += 1,
                JsonValue::Bool(_) => info.has_bool = true,
                JsonValue::Number(n) => {
                    if n.is_i64() {
                        info.has_int = true;
                    } else {
                        info.has_float = true;
                    }
                }
                JsonValue::String(_) => info.has_str = true,
                _ => info.has_other = true,
            }
        }
    }

    // For columns that didn't appear in every row, pad with nulls at the
    // missing positions. We re-walk the rows to build aligned value vectors.
    let mut padded: BTreeMap<String, Vec<JsonValue>> = BTreeMap::new();
    for name in cols.keys() {
        padded.insert(name.clone(), Vec::with_capacity(rows.len()));
    }
    for row in rows {
        let obj = row.as_object();
        for name in cols.keys() {
            let v = obj
                .and_then(|o| o.get(name))
                .cloned()
                .unwrap_or(JsonValue::Null);
            padded.get_mut(name).unwrap().push(v);
        }
    }
    for (name, info) in cols.iter_mut() {
        info.values = padded.remove(name).unwrap();
    }

    // Order columns by their first-appearance index, then build TypedColumns.
    let mut ordered: Vec<(String, ColInfo)> = cols.into_iter().collect();
    ordered.sort_by_key(|(_, info)| info.order);

    ordered
        .into_iter()
        .map(|(name, info)| {
            let col = build_typed_column(&info);
            (name, col)
        })
        .collect()
}

/// Per-column type inference state.
#[derive(Default)]
struct ColInfo {
    order: usize,
    has_int: bool,
    has_float: bool,
    has_str: bool,
    has_bool: bool,
    has_other: bool,
    null_count: usize,
    values: Vec<JsonValue>,
}

/// Build a TypedColumn from inferred ColInfo, substituting default values
/// for any nulls.
fn build_typed_column(info: &ColInfo) -> TypedColumn {
    let has_int = info.has_int;
    let has_float = info.has_float;
    let has_str = info.has_str;
    let has_bool = info.has_bool;
    let has_other = info.has_other;
    let values = &info.values;

    // Decide the column type.
    if has_other || (has_int as u8 + has_float as u8 + has_bool as u8 > 1 && !has_str) {
        // Mixed non-string types → fall back to STRING (JSON-encoded).
        let strs: Vec<String> = values
            .iter()
            .map(|v| match v {
                JsonValue::String(s) => s.clone(),
                JsonValue::Null => String::new(),
                other => other.to_string(),
            })
            .collect();
        return TypedColumn::String(strs);
    }
    if has_str {
        let strs: Vec<String> = values
            .iter()
            .map(|v| match v {
                JsonValue::String(s) => s.clone(),
                JsonValue::Null => String::new(),
                other => other.to_string(),
            })
            .collect();
        return TypedColumn::String(strs);
    }
    if has_float {
        let nums: Vec<f64> = values
            .iter()
            .map(|v| match v {
                JsonValue::Number(n) => n.as_f64().unwrap_or(0.0),
                JsonValue::Null => 0.0,
                _ => 0.0,
            })
            .collect();
        return TypedColumn::Float64(nums);
    }
    if has_int {
        let nums: Vec<i64> = values
            .iter()
            .map(|v| match v {
                JsonValue::Number(n) => n.as_i64().unwrap_or(0),
                JsonValue::Null => 0,
                _ => 0,
            })
            .collect();
        return TypedColumn::Int64(nums);
    }
    if has_bool {
        let bools: Vec<bool> = values
            .iter()
            .map(|v| match v {
                JsonValue::Bool(b) => *b,
                JsonValue::Null => false,
                _ => false,
            })
            .collect();
        return TypedColumn::Boolean(bools);
    }
    // All-null column → default to STRING with empty strings.
    let strs: Vec<String> = values.iter().map(|_| String::new()).collect();
    TypedColumn::String(strs)
}

/// Read all rows from a collection's HEAD as JSON objects, applying optional
/// WHERE filter, column projection, and LIMIT.
///
/// WHERE syntax: "col op val [AND col op val ...]"
///   op: =, !=, <, <=, >, >=
///   val: integer (42), float (3.14), 'string', "string", true, false
fn read_rows_as_json(
    storage: &UnifiedStorage,
    collection: &str,
    where_filter: Option<&str>,
    columns: Option<&str>,
    limit: Option<usize>,
) -> Result<Vec<JsonValue>, String> {
    let kernel = storage.kernel();
    let active = storage.get_active_branch(collection);

    // Resolve HEAD commit.
    let head = kernel
        .resolve(&pond_storage::branch_ref(collection, &active))
        .or_else(|| kernel.resolve(collection))
        .ok_or_else(|| format!("Collection '{}' has no commits", collection))?;

    let manifest_bytes = commit::resolve_manifest_bytes(kernel, &head)
        .map_err(|e| format!("Failed to read manifest: {}", e))?;

    let manifest = CollectionManifest::decode(&manifest_bytes)
        .ok_or_else(|| "Failed to decode manifest".to_string())?;

    // Decode each row group's PND2 blob into JSON rows.
    let mut all_rows: Vec<JsonValue> = Vec::new();
    for rg in &manifest.row_groups {
        let blob = kernel
            .read_blob(&rg.blob_hash)
            .map_err(|e| format!("Failed to read data blob: {}", e))?;
        let cols = pnd2_decode(&blob).map_err(|e| format!("Failed to decode PND2: {}", e))?;
        let n_rows = cols.first().map(|c| c.n_values).unwrap_or(0);
        for row_idx in 0..n_rows {
            let mut row_obj = serde_json::Map::new();
            for col in &cols {
                let name = col.name.to_string_lossy().to_string();
                // Skip CRDT metadata columns — they're internal.
                if name == "_rowid" || name == "_version" || name == "_deleted" {
                    continue;
                }
                let val = match col.vtype {
                    VT_INT64 => col
                        .i64_data
                        .get(row_idx)
                        .map(|v| json!(*v))
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
                        .map(|v| json!(v.to_string_lossy().to_string()))
                        .unwrap_or(JsonValue::Null),
                    VT_BOOLEAN => col
                        .i64_data
                        .get(row_idx)
                        .map(|v| json!(*v != 0))
                        .unwrap_or(JsonValue::Null),
                    _ => JsonValue::Null,
                };
                row_obj.insert(name, val);
            }
            all_rows.push(JsonValue::Object(row_obj));
        }
    }

    // Apply WHERE filter.
    if let Some(w) = where_filter {
        if !w.trim().is_empty() {
            let predicates = parse_where_clause(w)?;
            all_rows.retain(|r| predicates.iter().all(|(c, op, v)| eval_predicate(r, c, op, v)));
        }
    }

    // Apply column projection.
    if let Some(cols_str) = columns {
        if !cols_str.trim().is_empty() {
            let col_list: Vec<String> = cols_str
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            all_rows = all_rows
                .into_iter()
                .map(|r| {
                    if let Some(obj) = r.as_object() {
                        let mut new_obj = serde_json::Map::new();
                        for c in &col_list {
                            if let Some(v) = obj.get(c) {
                                new_obj.insert(c.clone(), v.clone());
                            } else {
                                new_obj.insert(c.clone(), JsonValue::Null);
                            }
                        }
                        JsonValue::Object(new_obj)
                    } else {
                        r
                    }
                })
                .collect();
        }
    }

    // Apply LIMIT.
    if let Some(lim) = limit {
        all_rows.truncate(lim);
    }

    Ok(all_rows)
}

/// Parse a simple WHERE clause into a list of (column, op, value) predicates
/// joined by implicit AND.
///
/// Grammar: predicate (AND predicate)*
///   predicate: col op value
///   op: =, ==, !=, <>, <, <=, >, >=
///   value: integer | float | 'string' | "string" | true | false | null
///
/// Returns an error if the clause can't be parsed.
fn parse_where_clause(s: &str) -> Result<Vec<(String, String, JsonValue)>, String> {
    // Split on " AND " (case-insensitive). We do a simple scan rather than
    // a regex to keep dependencies minimal.
    let mut parts: Vec<&str> = Vec::new();
    let bytes = s.as_bytes();
    let mut start = 0usize;
    let mut i = 0usize;
    while i + 4 < bytes.len() {
        if bytes[i..i + 5].eq_ignore_ascii_case(b" and ") {
            // Only treat as a separator if we're not inside a quoted string.
            // (We can be inside a quoted string if the user has " AND " inside
            // a value, but for simplicity we check the preceding quote count.)
            let prefix = &s[start..i];
            let single_q = prefix.matches('\'').count();
            let double_q = prefix.matches('"').count();
            if single_q.is_multiple_of(2) && double_q.is_multiple_of(2) {
                parts.push(&s[start..i]);
                start = i + 5;
                i = start;
                continue;
            }
        }
        i += 1;
    }
    parts.push(&s[start..]);

    let mut out = Vec::new();
    for part in parts {
        let p = part.trim();
        if p.is_empty() {
            continue;
        }
        let (col, op, val) = parse_single_predicate(p)?;
        out.push((col, op, val));
    }
    Ok(out)
}

/// Parse a single predicate: "col op value".
fn parse_single_predicate(s: &str) -> Result<(String, String, JsonValue), String> {
    // Find the operator. We check 2-char ops first, then 1-char.
    let ops_2 = ["==", "!=", "<=", ">=", "<>"];
    let ops_1 = ["=", "<", ">"];

    let mut found: Option<(usize, &str)> = None;
    for op in ops_2.iter() {
        if let Some(idx) = s.find(op) {
            // Make sure we're not matching inside a quoted string.
            if !is_inside_quotes(s, idx) {
                found = Some((idx, op));
                break;
            }
        }
    }
    if found.is_none() {
        for op in ops_1.iter() {
            if let Some(idx) = s.find(op) {
                if !is_inside_quotes(s, idx) {
                    found = Some((idx, op));
                    break;
                }
            }
        }
    }

    let (op_idx, op) = found.ok_or_else(|| {
        format!("invalid predicate '{}': expected operator (=, !=, <, <=, >, >=)", s)
    })?;

    let col = s[..op_idx].trim().to_string();
    let val_str = s[op_idx + op.len()..].trim();

    if col.is_empty() {
        return Err(format!("invalid predicate '{}': missing column name", s));
    }

    let val = parse_value(val_str)?;

    // Normalize <> to !=.
    let op_norm = if op == "<>" { "!=".to_string() } else { op.to_string() };

    Ok((col, op_norm, val))
}

/// Check if the byte at position `idx` is inside a quoted string.
fn is_inside_quotes(s: &str, idx: usize) -> bool {
    let bytes = s.as_bytes();
    let mut in_single = false;
    let mut in_double = false;
    for &b in &bytes[..idx] {
        match b {
            b'\'' if !in_double => in_single = !in_single,
            b'"' if !in_single => in_double = !in_double,
            _ => {}
        }
    }
    in_single || in_double
}

/// Parse a predicate value: integer, float, 'string', "string", true, false, null.
fn parse_value(s: &str) -> Result<JsonValue, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("empty value".to_string());
    }

    // Quoted string.
    if (s.starts_with('\'') && s.ends_with('\'') && s.len() >= 2)
        || (s.starts_with('"') && s.ends_with('"') && s.len() >= 2)
    {
        return Ok(JsonValue::String(s[1..s.len() - 1].to_string()));
    }

    // Booleans / null.
    match s.to_ascii_lowercase().as_str() {
        "true" => return Ok(json!(true)),
        "false" => return Ok(json!(false)),
        "null" => return Ok(JsonValue::Null),
        _ => {}
    }

    // Integer.
    if let Ok(i) = s.parse::<i64>() {
        return Ok(json!(i));
    }
    // Float.
    if let Ok(f) = s.parse::<f64>() {
        if let Some(n) = serde_json::Number::from_f64(f) {
            return Ok(JsonValue::Number(n));
        }
    }

    // Bare string (no quotes).
    Ok(JsonValue::String(s.to_string()))
}

/// Evaluate a single (col, op, value) predicate against a JSON row.
fn eval_predicate(row: &JsonValue, col: &str, op: &str, value: &JsonValue) -> bool {
    let cell = match row.get(col) {
        Some(v) => v,
        None => return false,
    };

    // Try numeric comparison first.
    if let (Some(a), Some(b)) = (cell.as_f64(), value.as_f64()) {
        return match op {
            "=" | "==" => a == b,
            "!=" => a != b,
            "<" => a < b,
            "<=" => a <= b,
            ">" => a > b,
            ">=" => a >= b,
            _ => false,
        };
    }

    // String comparison.
    if let (Some(a), Some(b)) = (cell.as_str(), value.as_str()) {
        return match op {
            "=" | "==" => a == b,
            "!=" => a != b,
            "<" => a < b,
            "<=" => a <= b,
            ">" => a > b,
            ">=" => a >= b,
            _ => false,
        };
    }

    // Boolean comparison.
    if let (Some(a), Some(b)) = (cell.as_bool(), value.as_bool()) {
        return match op {
            "=" | "==" => a == b,
            "!=" => a != b,
            _ => false,
        };
    }

    // Equality fallback (JSON structural equality).
    match op {
        "=" | "==" => cell == value,
        "!=" => cell != value,
        _ => false,
    }
}

/// Print rows as a JSON array (default) or an aligned text table.
fn print_rows(rows: &[JsonValue], format: &str) {
    match format.to_ascii_lowercase().as_str() {
        "json" | "" => {
            let arr = JsonValue::Array(rows.to_vec());
            println!("{}", serde_json::to_string_pretty(&arr).unwrap_or_else(|_| "[]".to_string()));
        }
        "table" => {
            if rows.is_empty() {
                println!("(no rows)");
                return;
            }
            // Collect column names from the first row, preserving order.
            let mut headers: Vec<String> = Vec::new();
            if let Some(obj) = rows[0].as_object() {
                for k in obj.keys() {
                    headers.push(k.clone());
                }
            }
            // Compute column widths.
            let mut widths: Vec<usize> = headers.iter().map(|h| h.len()).collect();
            let cells: Vec<Vec<String>> = rows
                .iter()
                .map(|r| {
                    let obj = r.as_object();
                    headers
                        .iter()
                        .enumerate()
                        .map(|(i, h)| {
                            let v = obj.and_then(|o| o.get(h)).cloned().unwrap_or(JsonValue::Null);
                            let s = match v {
                                JsonValue::String(s) => s,
                                other => other.to_string(),
                            };
                            if s.len() > widths[i] {
                                widths[i] = s.len();
                            }
                            s
                        })
                        .collect()
                })
                .collect();

            // Header line.
            let header_line: String = headers
                .iter()
                .enumerate()
                .map(|(i, h)| format!("{:width$}", h, width = widths[i]))
                .collect::<Vec<_>>()
                .join(" | ");
            println!("{}", header_line);
            println!("{}", "-".repeat(header_line.len()));

            // Data rows.
            for row_cells in &cells {
                let line: String = row_cells
                    .iter()
                    .enumerate()
                    .map(|(i, c)| format!("{:width$}", c, width = widths[i]))
                    .collect::<Vec<_>>()
                    .join(" | ");
                println!("{}", line);
            }
        }
        other => {
            eprintln!("Error: unknown format '{}' (use 'json' or 'table')", other);
            std::process::exit(1);
        }
    }
}

// ===========================================================================
// Shell / REPL mode — interactive read-eval-print loop
// ===========================================================================

/// Maximum number of commands retained in REPL history (in-memory only).
const REPL_HISTORY_LIMIT: usize = 100;

/// Classification of a single REPL input line.
///
/// Used by `parse_repl_command` to decide how to dispatch a line. SQL lines
/// are not parsed here — they're passed verbatim to `pond_sql::execute`.
enum ReplCommand {
    /// A SQL statement (SELECT/INSERT/UPDATE/DELETE/MERGE, or any
    /// unrecognized input that we'll attempt as SQL).
    Sql(String),
    /// `\l` or `\list` — list collections.
    ListCollections,
    /// `\d <name>` or `\describe <name>` — show collection schema.
    Describe(String),
    /// `\b <name>` — show branches for a collection.
    Branches(String),
    /// `\h`, `\help`, or `\?` — show help.
    Help,
    /// `\history` — show command history.
    History,
    /// `\! <cmd>` — execute a shell command.
    Shell(String),
    /// `\q`, `\quit`, or `exit` — quit the REPL.
    Quit,
    /// Empty/whitespace-only line — no-op.
    Empty,
}

/// Parse a single REPL input line into a [`ReplCommand`].
///
/// Classification rules:
///   - Empty/whitespace-only → [`ReplCommand::Empty`]
///   - Starts with `\` → meta-command (split on first whitespace)
///   - Exactly `exit` or `quit` (case-insensitive) → [`ReplCommand::Quit`]
///   - Otherwise → [`ReplCommand::Sql`] (attempted as SQL)
///
/// Unknown meta-commands fall through to `Sql` so the user sees a SQL error
/// rather than a silent no-op.
fn parse_repl_command(line: &str) -> ReplCommand {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return ReplCommand::Empty;
    }

    // Meta-commands start with a backslash.
    if trimmed.starts_with('\\') {
        // Split into command + rest on the first whitespace run.
        let mut parts = trimmed.splitn(2, char::is_whitespace);
        let cmd = parts.next().unwrap_or("");
        let arg = parts.next().map(|s| s.trim().to_string()).unwrap_or_default();
        return match cmd {
            "\\l" | "\\list" => ReplCommand::ListCollections,
            "\\d" | "\\describe" => ReplCommand::Describe(arg),
            "\\b" => ReplCommand::Branches(arg),
            "\\h" | "\\help" | "\\?" => ReplCommand::Help,
            "\\history" => ReplCommand::History,
            "\\!" => ReplCommand::Shell(arg),
            "\\q" | "\\quit" => ReplCommand::Quit,
            // Unknown meta-command — pass through to SQL so the user gets a
            // visible error rather than a silent drop.
            _ => ReplCommand::Sql(trimmed.to_string()),
        };
    }

    // Plain `exit` or `quit` (case-insensitive, single word).
    let lower = trimmed.to_ascii_lowercase();
    if lower == "exit" || lower == "quit" {
        return ReplCommand::Quit;
    }

    // Everything else is treated as SQL.
    ReplCommand::Sql(trimmed.to_string())
}

/// Start the interactive REPL.
///
/// Behavior:
///   1. Print a welcome banner with available commands.
///   2. If `--exec` is provided, execute that SQL query first.
///   3. Enter a read-eval-print loop:
///      - Read a line from stdin.
///      - Meta-commands (start with `\`) execute immediately, no `;` needed.
///      - `exit`/`quit`/`\q` exit the REPL.
///      - SQL lines accumulate until a line ending with `;` is seen.
///      - Empty lines are skipped.
///   4. On EOF (Ctrl+D) or read error, exit cleanly.
///
/// Ctrl+C relies on the default SIGINT disposition (terminate the process).
/// This is intentional — no external signal-handling crates are pulled in,
/// keeping the CLI dependency-light. The process exits with the conventional
/// SIGINT exit code (130).
fn cmd_shell(storage: UnifiedStorage, exec: Option<String>) {
    println!("Pond REPL v{}", env!("CARGO_PKG_VERSION"));
    println!("Type \\h for help, \\q to quit.");

    let mut history: Vec<String> = Vec::with_capacity(REPL_HISTORY_LIMIT + 1);

    // Execute --exec SQL first, before entering the read loop.
    if let Some(sql) = exec {
        execute_repl_line(&storage, &sql, &mut history);
    }

    let stdin = io::stdin();
    let mut buffer = String::new();

    loop {
        // Print the prompt: `pond> ` for a fresh statement, `  ... ` for a
        // multi-line continuation.
        if buffer.is_empty() {
            print!("pond> ");
        } else {
            print!("  ... ");
        }
        io::stdout().flush().ok();

        // Read a line from stdin.
        let mut line = String::new();
        match stdin.read_line(&mut line) {
            Ok(0) => break, // EOF (Ctrl+D)
            Ok(_) => {}
            Err(_) => break, // Read error — bail out.
        }

        // Strip trailing CR/LF.
        let line = line.trim_end_matches(['\n', '\r']);
        let trimmed = line.trim();

        // Skip empty lines (don't add to buffer or history).
        if trimmed.is_empty() {
            continue;
        }

        // Meta-commands execute immediately (no semicolon needed).
        if trimmed.starts_with('\\') {
            let was_quit = matches!(parse_repl_command(trimmed), ReplCommand::Quit);
            execute_repl_line(&storage, trimmed, &mut history);
            if was_quit {
                break;
            }
            continue;
        }

        // Plain `exit`/`quit` (case-insensitive, single word) — exit now.
        let lower = trimmed.to_ascii_lowercase();
        if lower == "exit" || lower == "quit" {
            break;
        }

        // SQL accumulation: append to buffer with a separating space.
        if !buffer.is_empty() {
            buffer.push(' ');
        }
        buffer.push_str(trimmed);

        // Execute when the buffer ends with `;`.
        if buffer.ends_with(';') {
            let cmd = buffer.clone();
            execute_repl_line(&storage, &cmd, &mut history);
            buffer.clear();
        }
    }
}

/// Execute a single REPL line (already classified by [`parse_repl_command`]).
///
/// Records the line in `history` (capped at [`REPL_HISTORY_LIMIT`] entries),
/// then dispatches to the appropriate handler. Errors are printed to stderr
/// and do not terminate the REPL.
fn execute_repl_line(storage: &UnifiedStorage, line: &str, history: &mut Vec<String>) {
    // Record in history (skip pure-empty lines).
    let entry = line.trim().to_string();
    if !entry.is_empty() {
        history.push(entry.clone());
        if history.len() > REPL_HISTORY_LIMIT {
            history.remove(0);
        }
    }

    match parse_repl_command(line) {
        ReplCommand::Empty => {}
        ReplCommand::Quit => {
            // Handled by the caller (`cmd_shell` breaks out of the loop).
        }
        ReplCommand::Help => print_repl_help(),
        ReplCommand::History => print_history(history),
        ReplCommand::ListCollections => {
            cmd_ls(storage);
        }
        ReplCommand::Describe(name) => {
            if name.is_empty() {
                eprintln!("Usage: \\d <collection>");
            } else {
                describe_collection(storage, &name);
            }
        }
        ReplCommand::Branches(name) => {
            if name.is_empty() {
                eprintln!("Usage: \\b <collection>");
            } else {
                cmd_branches(storage, &name);
            }
        }
        ReplCommand::Shell(cmd) => {
            execute_shell_escape(&cmd);
        }
        ReplCommand::Sql(query) => {
            // Strip trailing semicolons — the SQL executor expects a single
            // statement without the terminator.
            let q = query.trim().trim_end_matches(';').trim();
            if q.is_empty() {
                return;
            }
            match pond_sql::execute(storage, q) {
                Ok(result) => print_sql_result(&result),
                Err(e) => eprintln!("Error: {}", e),
            }
        }
    }
}

/// Print the REPL help text.
fn print_repl_help() {
    println!("Pond REPL commands:");
    println!("  SQL statements execute when a line ends with ';'.");
    println!("  Multi-line SQL is accumulated until ';' is seen.");
    println!();
    println!("Meta-commands (no ';' needed):");
    println!("  \\l, \\list              List collections");
    println!("  \\d <name>,              Show schema for a collection");
    println!("  \\describe <name>");
    println!("  \\b <name>               Show branches for a collection");
    println!("  \\history                Show command history (last {})", REPL_HISTORY_LIMIT);
    println!("  \\! <cmd>                Execute a shell command");
    println!("  \\h, \\help, \\?           Show this help");
    println!("  \\q, \\quit, exit         Quit the REPL");
    println!();
    println!("SQL keywords: SELECT, INSERT, UPDATE, DELETE, MERGE");
}

/// Print the command history (oldest first, most recent last).
fn print_history(history: &[String]) {
    if history.is_empty() {
        println!("(no history yet)");
        return;
    }
    for (i, cmd) in history.iter().enumerate() {
        println!("{:>4}  {}", i + 1, cmd);
    }
}

/// Execute a shell escape: `\! <cmd>`.
///
/// Runs the rest of the line via `sh -c`. Stdout/stderr of the child process
/// are forwarded to the REPL's stdout/stderr. On Unix only — `sh` must be on
/// PATH.
fn execute_shell_escape(cmd: &str) {
    if cmd.is_empty() {
        eprintln!("Usage: \\! <command>");
        return;
    }
    match std::process::Command::new("sh").arg("-c").arg(cmd).output() {
        Ok(output) => {
            io::stdout().write_all(&output.stdout).ok();
            io::stderr().write_all(&output.stderr).ok();
        }
        Err(e) => eprintln!("Error: failed to execute '{}': {}", cmd, e),
    }
}

/// Print a SQL result as JSON (same format as `pond sql`).
fn print_sql_result(result: &pond_sql::SqlResult) {
    let out = json!({
        "columns": result.columns,
        "rows": result.rows,
    });
    println!(
        "{}",
        serde_json::to_string_pretty(&out).unwrap_or_else(|_| "{}".to_string())
    );
}

/// Describe a collection's schema (columns, types, row groups, branches).
///
/// Loads the active branch's HEAD, decodes the manifest, and prints:
///   - Collection name and active branch
///   - Key column
///   - Row-group count and total rows
///   - Column schema (name + type)
fn describe_collection(storage: &UnifiedStorage, collection: &str) {
    let kernel = storage.kernel();
    let active = storage.get_active_branch(collection);
    let head = kernel
        .resolve(&pond_storage::branch_ref(collection, &active))
        .or_else(|| kernel.resolve(collection));

    let head = match head {
        Some(h) => h,
        None => {
            println!("Collection '{}' has no commits.", collection);
            return;
        }
    };

    let manifest_bytes = match commit::resolve_manifest_bytes(kernel, &head) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("Error reading manifest: {}", e);
            return;
        }
    };

    let manifest = match CollectionManifest::decode(&manifest_bytes) {
        Some(m) => m,
        None => {
            eprintln!("Error: failed to decode manifest");
            return;
        }
    };

    println!("Collection:    {}", collection);
    println!("Active branch: {}", active);
    println!("Key column:    {}", manifest.key_col);
    println!("Row groups:    {}", manifest.row_groups.len());
    let total_rows: u32 = manifest.row_groups.iter().map(|rg| rg.n_rows).sum();
    println!("Total rows:    {}", total_rows);
    println!();
    println!("Schema:");
    println!("  {:<24} {:<10}", "Column", "Type");
    println!("  {:<24} {:<10}", "------", "----");
    for (name, vtype) in &manifest.columns {
        let type_str = match *vtype {
            VT_INT64 => "INT64",
            VT_FLOAT64 => "FLOAT64",
            VT_STRING => "STRING",
            VT_BOOLEAN => "BOOLEAN",
            _ => "OTHER",
        };
        println!("  {:<24} {:<10}", name, type_str);
    }
}
