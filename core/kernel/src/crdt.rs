// UUIDv7 + HLC — distributed row identification and clock-skew-safe versioning
//
// These are the CRDT primitives:
//   - UUIDv7: time-ordered unique IDs for _rowid (distributed, no coordinator)
//   - HLC: Hybrid Logical Clock for _version (monotonic under clock skew)
//
// Used by:
//   - upsert_shard: generates _rowid (UUIDv7) + _version (HLC) per row
//   - merge: compares _version to determine which row wins (latest wins)
//   - delete_shard: writes tombstone with _deleted=True + new _version

use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

// ---------------------------------------------------------------------------
// UUIDv7 — time-ordered UUID for distributed row identification
// ---------------------------------------------------------------------------

/// Monotonic counter state for same-millisecond uniqueness (per-process).
struct MonotonicState {
    last_ms: u64,
    counter: u16,
}

static MONOTONIC: Mutex<MonotonicState> = Mutex::new(MonotonicState {
    last_ms: 0,
    counter: 0,
});

/// Generate a UUIDv7 string (36 chars, dashed format).
///
/// Format (128 bits):
///   - 48 bits: Unix epoch milliseconds (big-endian)
///   - 4 bits: version (7)
///   - 12 bits: random_a
///   - 2 bits: variant (10)
///   - 62 bits: random_b
///
/// Time-ordered: earlier timestamps sort before later ones lexicographically.
/// Distributed-friendly: no central ID allocator needed.
pub fn uuidv7() -> String {
    let timestamp_ms = current_time_ms();
    uuidv7_with_timestamp(timestamp_ms)
}

/// Generate a UUIDv7 with a specific timestamp (for testing).
pub fn uuidv7_with_timestamp(timestamp_ms: u64) -> String {
    let mut rand_bytes = [0u8; 10];
    fill_random(&mut rand_bytes);

    // Build 16 bytes
    let ts_bytes = timestamp_ms.to_be_bytes(); // 8 bytes, take first 6
    let mut bytes = [0u8; 16];

    // Bytes 0-5: timestamp (48 bits, big-endian)
    bytes[0] = ts_bytes[2];
    bytes[1] = ts_bytes[3];
    bytes[2] = ts_bytes[4];
    bytes[3] = ts_bytes[5];
    bytes[4] = ts_bytes[6];
    bytes[5] = ts_bytes[7];

    // Byte 6: version (4 bits = 7) + random_a high (4 bits)
    bytes[6] = 0x70 | (rand_bytes[0] & 0x0F);

    // Byte 7: random_a low (8 bits)
    bytes[7] = rand_bytes[1];

    // Byte 8: variant (2 bits = 10) + random_b high (6 bits)
    bytes[8] = 0x80 | (rand_bytes[2] & 0x3F);

    // Bytes 9-15: random_b remaining (56 bits)
    bytes[9..16].copy_from_slice(&rand_bytes[3..10]);

    format_uuid(&bytes)
}

/// Generate a UUIDv7 with guaranteed monotonic ordering within a process.
///
/// If called multiple times within the same millisecond, uses a counter
/// to ensure strict ordering. 4096 unique IDs per millisecond per process.
pub fn uuidv7_monotonic() -> String {
    let mut state = MONOTONIC.lock().unwrap();
    let now_ms = current_time_ms();

    let (ts, counter) = if now_ms <= state.last_ms {
        state.counter += 1;
        if state.counter > 0xFFF {
            // Counter overflow — advance to next millisecond
            state.last_ms += 1;
            state.counter = 0;
            (state.last_ms, 0u16)
        } else {
            (state.last_ms, state.counter)
        }
    } else {
        state.last_ms = now_ms;
        state.counter = 0;
        (now_ms, 0u16)
    };

    let mut rand_bytes = [0u8; 8];
    fill_random(&mut rand_bytes);

    let ts_bytes = ts.to_be_bytes();
    let mut bytes = [0u8; 16];

    bytes[0] = ts_bytes[2];
    bytes[1] = ts_bytes[3];
    bytes[2] = ts_bytes[4];
    bytes[3] = ts_bytes[5];
    bytes[4] = ts_bytes[6];
    bytes[5] = ts_bytes[7];

    // Embed counter in random_a (12 bits)
    bytes[6] = 0x70 | ((counter >> 8) as u8 & 0x0F);
    bytes[7] = (counter & 0xFF) as u8;

    bytes[8] = 0x80 | (rand_bytes[0] & 0x3F);
    bytes[9..16].copy_from_slice(&rand_bytes[1..8]);

    format_uuid(&bytes)
}

/// Extract the Unix millisecond timestamp from a UUIDv7 string.
pub fn uuidv7_timestamp(uuid_str: &str) -> Option<u64> {
    let hex: String = uuid_str.chars().filter(|c| *c != '-').collect();
    if hex.len() < 12 {
        return None;
    }
    let ts_high = u32::from_str_radix(&hex[0..8], 16).ok()?;
    let ts_low = u16::from_str_radix(&hex[8..12], 16).ok()?;
    Some(((ts_high as u64) << 16) | (ts_low as u64))
}

/// Format 16 bytes as a standard UUID string (36 chars with dashes).
fn format_uuid(bytes: &[u8; 16]) -> String {
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0], bytes[1], bytes[2], bytes[3],
        bytes[4], bytes[5],
        bytes[6], bytes[7],
        bytes[8], bytes[9],
        bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15]
    )
}

/// Fill a buffer with cryptographically secure random bytes.
///
/// Uses (in priority order):
///   1. `/dev/urandom` on Unix (POSIX, no external deps)
///   2. `BCryptGenRandom` on Windows (Win32 CryptoAPI)
///   3. Fallback: time-seeded xorshift64 — ONLY if both OS sources fail.
///      This is a hard correctness panic-bait; a working CSPRNG is required
///      for HLC uniqueness guarantees.
///
/// This is the same algorithm the Python `secrets` module uses internally.
/// No `rand` crate dependency — keeps the kernel dependency-free.
fn fill_random(buf: &mut [u8]) {
    if try_fill_random_system(buf).is_ok() {
        return;
    }
    // Last-resort fallback: time-seeded xorshift64.
    // This only runs if /dev/urandom and BCryptGenRandom both fail — i.e.,
    // the OS is in a broken state. We log to stderr (no `log` dep in kernel)
    // and continue, because panicking here would crash every write.
    eprintln!("pond: WARNING: CSPRNG unavailable, using insecure fallback");
    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
        .wrapping_add(std::process::id() as u64)
        .wrapping_add(buf.as_ptr() as u64);

    let mut state = seed.max(1); // xorshift64 must never see a 0 seed
    for byte in buf.iter_mut() {
        // xorshift64 (Marsaglia, 2003)
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        *byte = state as u8;
    }
}

/// Try to fill `buf` from the OS CSPRNG. Returns `Ok(())` on success.
///
/// - Unix: reads from `/dev/urandom` (retries on `EINTR`).
/// - Windows: calls `BCryptGenRandom` via the Win32 API.
///
/// Returns `Err(io::Error)` if the OS source is unavailable.
#[cfg(unix)]
fn try_fill_random_system(buf: &mut [u8]) -> std::io::Result<()> {
    use std::io::Read;
    let mut f = std::fs::File::open("/dev/urandom")?;
    // Read in a loop — `/dev/urandom` may give partial reads under memory pressure.
    // EINTR is handled by std::fs::File::read itself on modern Rust.
    f.read_exact(buf)?;
    Ok(())
}

#[cfg(windows)]
fn try_fill_random_system(buf: &mut [u8]) -> std::io::Result<()> {
    // BCryptGenRandom — Win32 CryptoAPI, no `rand` crate dep.
    // https://learn.microsoft.com/en-us/windows/win32/api/bcrypt/nf-bcrypt-bcryptgenrandom
    extern "system" {
        fn BCryptGenRandom(
            halgorithm: *mut std::ffi::c_void,
            pbuffer: *mut u8,
            cbbuffer: u32,
            dwflags: u32,
        ) -> i32;
    }
    const BCRYPT_USE_SYSTEM_PREFERRED_RNG: u32 = 0x00000002;
    let status = unsafe {
        BCryptGenRandom(
            std::ptr::null_mut(),
            buf.as_mut_ptr(),
            buf.len() as u32,
            BCRYPT_USE_SYSTEM_PREFERRED_RNG,
        )
    };
    if status == 0 {
        Ok(())
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("BCryptGenRandom failed: NTSTATUS 0x{:08X}", status as u32),
        ))
    }
}

#[cfg(not(any(unix, windows)))]
fn try_fill_random_system(_buf: &mut [u8]) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "no CSPRNG available on this platform",
    ))
}

fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// HLC — Hybrid Logical Clock for clock-skew-safe _version
// ---------------------------------------------------------------------------

/// Hybrid Logical Clock — monotonic under clock skew.
///
/// Each process has its own HLC instance. The clock advances on every
/// tick (before each write). The logical counter increments when the
/// physical clock hasn't advanced.
///
/// Usage:
///   let mut clock = HLC::new();
///   let v1 = clock.tick();  // before write 1
///   let v2 = clock.tick();  // before write 2 (may have same physical, higher logical)
///   assert!(v1 < v2);  // lexicographic = chronological
pub struct HLC {
    physical: u64,
    logical: u64,
}

impl HLC {
    pub fn new() -> Self {
        Self { physical: 0, logical: 0 }
    }

    /// Advance the clock and return the current HLC value as a hex string.
    ///
    /// Format: 32 hex chars (16 bytes): 8 bytes physical_ms big-endian
    /// + 8 bytes logical big-endian. Sorts lexicographically = chronologically.
    pub fn tick(&mut self) -> String {
        let now = current_time_ms();
        if now > self.physical {
            self.physical = now;
            self.logical = 0;
        } else {
            self.logical += 1;
        }
        self.encode()
    }

    /// Observe another HLC value (e.g., from a remote write).
    /// Updates the local clock to be at least as high as the observed value.
    pub fn observe(&mut self, other: &str) {
        if let Some((other_physical, other_logical)) = Self::decode(other) {
            let now = current_time_ms();
            if now > self.physical && now > other_physical {
                self.physical = now;
                self.logical = 0;
            } else if other_physical > self.physical {
                self.physical = other_physical;
                self.logical = other_logical + 1;
            } else if other_physical == self.physical {
                if other_logical > self.logical {
                    self.logical = other_logical + 1;
                } else {
                    self.logical += 1;
                }
            } else {
                self.logical += 1;
            }
        }
    }

    /// Encode the current HLC state as a 32-char hex string.
    fn encode(&self) -> String {
        format!("{:016x}{:016x}", self.physical, self.logical)
    }

    /// Decode a 32-char hex string into (physical, logical).
    fn decode(s: &str) -> Option<(u64, u64)> {
        if s.len() != 32 {
            return None;
        }
        let physical = u64::from_str_radix(&s[0..16], 16).ok()?;
        let logical = u64::from_str_radix(&s[16..32], 16).ok()?;
        Some((physical, logical))
    }

    /// Check if a string is a valid HLC value.
    pub fn is_valid(s: &str) -> bool {
        s.len() == 32 && s.chars().all(|c| c.is_ascii_hexdigit())
    }
}

impl Default for HLC {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_uuidv7_format() {
        let id = uuidv7();
        assert_eq!(id.len(), 36, "UUIDv7 must be 36 chars");
        assert_eq!(id.chars().nth(8), Some('-'));
        assert_eq!(id.chars().nth(13), Some('-'));
        assert_eq!(id.chars().nth(18), Some('-'));
        assert_eq!(id.chars().nth(23), Some('-'));
        // Version 7
        assert_eq!(id.chars().nth(14), Some('7'));
    }

    #[test]
    fn test_uuidv7_time_ordered() {
        let id1 = uuidv7();
        thread::sleep(Duration::from_millis(2));
        let id2 = uuidv7();
        assert!(id1 < id2, "UUIDv7 must be time-ordered: {} < {}", id1, id2);
    }

    #[test]
    fn test_uuidv7_monotonic() {
        let ids: Vec<String> = (0..100).map(|_| uuidv7_monotonic()).collect();
        for i in 0..ids.len() - 1 {
            assert!(ids[i] < ids[i + 1], "Monotonic violated at {}", i);
        }
    }

    #[test]
    fn test_uuidv7_timestamp_extraction() {
        let ts = current_time_ms();
        let id = uuidv7_with_timestamp(ts);
        let extracted = uuidv7_timestamp(&id).unwrap();
        assert_eq!(extracted, ts, "Timestamp mismatch");
    }

    #[test]
    fn test_uuidv7_uniqueness() {
        let mut seen = std::collections::HashSet::new();
        for _ in 0..10000 {
            let id = uuidv7();
            assert!(seen.insert(id), "Duplicate UUID generated");
        }
        assert_eq!(seen.len(), 10000);
    }

    #[test]
    fn test_hlc_monotonic() {
        let mut clock = HLC::new();
        let v1 = clock.tick();
        let v2 = clock.tick();
        let v3 = clock.tick();
        assert!(v1 < v2, "HLC must be monotonic: {} < {}", v1, v2);
        assert!(v2 < v3, "HLC must be monotonic: {} < {}", v2, v3);
    }

    #[test]
    fn test_hlc_format() {
        let mut clock = HLC::new();
        let v = clock.tick();
        assert_eq!(v.len(), 32, "HLC must be 32 hex chars");
        assert!(HLC::is_valid(&v), "HLC must be valid");
    }

    #[test]
    fn test_hlc_observe() {
        let mut clock1 = HLC::new();
        let mut clock2 = HLC::new();

        let v1 = clock1.tick();
        thread::sleep(Duration::from_millis(2));
        let v2 = clock2.tick();

        // clock1 observes v2 — should advance
        clock1.observe(&v2);
        let v3 = clock1.tick();
        assert!(v3 > v2, "After observing v2, clock1 should advance past it");
    }

    // -------------------------------------------------------------------
    // CSPRNG tests — verify fill_random() uses the OS source (NOT xorshift).
    // These were added to fix the bug uncovered by Task 0-d: the docstring
    // claimed /dev/urandom but the implementation was time-seeded xorshift64.
    // -------------------------------------------------------------------

    #[test]
    fn test_fill_random_does_not_panic() {
        let mut buf = [0u8; 32];
        fill_random(&mut buf);
        // Sanity: not all zeros (would indicate failure)
        let non_zero = buf.iter().filter(|&&b| b != 0).count();
        assert!(non_zero > 0, "fill_random returned all zeros");
    }

    #[test]
    fn test_fill_random_uses_os_csprng_on_unix() {
        // On Unix, /dev/urandom is the OS CSPRNG. If we read from it twice
        // in quick succession, we should get different bytes (the OS keeps
        // an entropy pool). A time-seeded xorshift64 would also produce
        // different bytes here, so this isn't a perfect discriminator —
        // but it catches the degenerate "always returns zeros" case.
        #[cfg(unix)]
        {
            let mut buf1 = [0u8; 16];
            let mut buf2 = [0u8; 16];
            fill_random(&mut buf1);
            fill_random(&mut buf2);
            assert_ne!(buf1, buf2, "two fill_random calls must differ");
        }
        #[cfg(not(unix))]
        {
            // On non-Unix, just ensure the function runs without panic.
            let mut buf = [0u8; 16];
            fill_random(&mut buf);
        }
    }

    #[test]
    fn test_fill_random_large_buffer() {
        // 4 KB — verify read_exact semantics (no partial reads returned)
        let mut buf = vec![0u8; 4096];
        fill_random(&mut buf);
        // Count distinct byte values — a CSPRNG should produce >100 distinct
        // values in 4096 bytes (statistically, ~255 are expected). xorshift64
        // also passes this test, so it's a weak check, but it catches "always
        // zero" and "always same byte" regressions.
        let distinct = buf.iter().copied().collect::<std::collections::HashSet<u8>>().len();
        assert!(distinct > 100, "fill_random produced only {} distinct bytes in 4KB", distinct);
    }

    #[test]
    fn test_try_fill_random_system_available_on_supported_platforms() {
        // The OS CSPRNG should be available on all production platforms.
        // If this test fails, either /dev/urandom is missing (broken Unix)
        // or BCryptGenRandom is missing (broken Windows) — both are critical.
        let mut buf = [0u8; 8];
        let result = try_fill_random_system(&mut buf);
        assert!(result.is_ok(),
            "OS CSPRNG unavailable on this platform: {:?}. \
             fill_random will fall back to insecure xorshift64 — \
             this is a production-critical regression.",
            result.err());
    }

    #[test]
    fn test_uuidv7_random_bits_change_between_calls() {
        // Two UUIDs generated within the same millisecond must differ in their
        // random bits (the whole point of CSPRNG-seeded UUIDv7).
        let id1 = uuidv7_with_timestamp(1_700_000_000_000);
        let id2 = uuidv7_with_timestamp(1_700_000_000_000);
        // Same timestamp, but random bits (positions 6-7, 9-15) must differ.
        // (They could theoretically collide with probability 2^-74 —
        // acceptable for a test that runs once.)
        assert_ne!(id1, id2, "two UUIDv7 with same ts must differ in random bits");
    }
}
