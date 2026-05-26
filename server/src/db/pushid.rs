use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

/// Characters used in push IDs, sorted for correct ASCII string ordering.
const PUSH_CHARS: &[u8; 64] = b"-0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz";

/// Number of timestamp characters in push ID.
const TIMESTAMP_LEN: usize = 8;

/// Number of random characters in push ID.
const RANDOM_LEN: usize = 12;

/// State for push ID generation.
struct PushIdState {
    last_push_time: i64,
    last_rand_chars: [u8; RANDOM_LEN],
}

static PUSH_STATE: Mutex<PushIdState> = Mutex::new(PushIdState {
    last_push_time: 0,
    last_rand_chars: [0; RANDOM_LEN],
});

/// Generate a unique push ID.
///
/// Push IDs are:
/// - 20 characters total
/// - First 8 characters encode timestamp (milliseconds since epoch)
/// - Last 12 characters are random
/// - Chronologically sortable (later IDs sort after earlier ones)
pub fn generate_push_id() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    generate_push_id_at(now)
}

/// Generate a push ID for the given timestamp (milliseconds since epoch).
/// Useful for testing.
pub fn generate_push_id_at(now: i64) -> String {
    let mut state = PUSH_STATE.lock().unwrap();

    let duplicate_time = now == state.last_push_time;
    state.last_push_time = now;

    let mut id = [0u8; TIMESTAMP_LEN + RANDOM_LEN];

    // Encode timestamp in first 8 characters (base64-like encoding)
    let mut ts = now;
    for i in (0..TIMESTAMP_LEN).rev() {
        id[i] = PUSH_CHARS[(ts % 64) as usize];
        ts /= 64;
    }

    if duplicate_time {
        // Same millisecond: increment the random portion
        for i in (0..RANDOM_LEN).rev() {
            if state.last_rand_chars[i] < 63 {
                state.last_rand_chars[i] += 1;
                break;
            }
            state.last_rand_chars[i] = 0;
        }
    } else {
        // New millisecond: generate fresh random portion
        // Use simple random bytes
        for i in 0..RANDOM_LEN {
            state.last_rand_chars[i] = rand_byte() % 64;
        }
    }

    // Encode random portion
    for i in 0..RANDOM_LEN {
        id[TIMESTAMP_LEN + i] = PUSH_CHARS[state.last_rand_chars[i] as usize];
    }

    // Safety: all characters are ASCII
    String::from_utf8(id.to_vec()).unwrap()
}

/// Simple random byte generator using system randomness.
fn rand_byte() -> u8 {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};

    // Use the random state from HashMap as entropy source
    let state = RandomState::new();
    let mut hasher = state.build_hasher();
    hasher.write_u64(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64,
    );
    hasher.finish() as u8
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_push_id_length() {
        let id = generate_push_id();
        assert_eq!(id.len(), 20);
    }

    #[test]
    fn test_push_id_unique() {
        let id1 = generate_push_id();
        let id2 = generate_push_id();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_push_id_chronological() {
        // IDs generated at later times should sort after earlier ones
        let t1 = 1704067200000i64; // 2024-01-01 00:00:00 UTC
        let t2 = 1704067201000i64; // 1 second later

        let id1 = generate_push_id_at(t1);
        // Reset state to ensure we get new random portion
        {
            let mut state = PUSH_STATE.lock().unwrap();
            state.last_push_time = 0;
        }
        let id2 = generate_push_id_at(t2);

        assert!(id2 > id1, "expected {} > {}", id2, id1);
    }

    #[test]
    fn test_push_id_valid_chars() {
        let id = generate_push_id();
        for c in id.chars() {
            assert!(
                PUSH_CHARS.contains(&(c as u8)),
                "invalid character '{}' in push ID",
                c
            );
        }
    }

    #[test]
    fn test_push_id_same_millisecond_increments() {
        let ts = 1704067200000i64;

        // Reset state
        {
            let mut state = PUSH_STATE.lock().unwrap();
            state.last_push_time = 0;
        }

        let id1 = generate_push_id_at(ts);
        let id2 = generate_push_id_at(ts); // Same timestamp

        // IDs should be different
        assert_ne!(id1, id2);

        // ID2 should sort after ID1 (incrementing random portion)
        assert!(id2 > id1, "expected {} > {} for same timestamp", id2, id1);
    }
}
