use smallvec::SmallVec;
use std::fmt;
use std::sync::Arc;

/// Maximum bytes allowed for a single key segment.
pub const MAX_KEY_BYTES: usize = 768;

/// A path in the database tree.
///
/// Paths are sequences of string segments like "/users/abc/name".
/// Uses SmallVec to avoid heap allocation for typical paths (< 8 segments).
#[derive(Clone, PartialEq, Eq, Hash, Default)]
pub struct Path {
    segments: SmallVec<[Arc<str>; 8]>,
}

impl Path {
    /// Create a new empty path (root).
    pub fn root() -> Self {
        Self {
            segments: SmallVec::new(),
        }
    }

    /// Parse a path string into a Path.
    ///
    /// "/players/abc/score" -> Path with ["players", "abc", "score"]
    /// "" or "/" -> empty Path (root)
    pub fn parse(path: &str) -> Self {
        let trimmed = path.trim_matches('/');
        if trimmed.is_empty() {
            return Self::root();
        }

        let segments: SmallVec<[Arc<str>; 8]> = trimmed.split('/').map(Arc::from).collect();

        Self { segments }
    }

    /// Returns true if this is the root path (no segments).
    pub fn is_root(&self) -> bool {
        self.segments.is_empty()
    }

    /// Returns the number of segments in the path.
    pub fn len(&self) -> usize {
        self.segments.len()
    }

    /// Returns true if the path has no segments (i.e., it is the root).
    pub fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }

    /// Returns the segments as a slice.
    pub fn segments(&self) -> &[Arc<str>] {
        &self.segments
    }

    /// Get a specific segment by index.
    pub fn get(&self, index: usize) -> Option<&str> {
        self.segments.get(index).map(|s| s.as_ref())
    }

    /// Returns the last segment (key) of the path.
    /// Returns None for root path.
    pub fn last_segment(&self) -> Option<&str> {
        self.segments.last().map(|s| s.as_ref())
    }

    /// Returns the parent path.
    /// Returns None if this is the root path.
    pub fn parent(&self) -> Option<Self> {
        if self.segments.is_empty() {
            return None;
        }
        let mut parent_segments = self.segments.clone();
        parent_segments.pop();
        Some(Self {
            segments: parent_segments,
        })
    }

    /// Join this path with a child segment or sub-path.
    ///
    /// "/users".join("abc") -> "/users/abc"
    /// "/users".join("abc/name") -> "/users/abc/name"
    pub fn join(&self, child: &str) -> Self {
        let child_path = Self::parse(child);
        let mut new_segments = self.segments.clone();
        new_segments.extend(child_path.segments);
        Self {
            segments: new_segments,
        }
    }

    /// Join this path with another Path.
    pub fn join_path(&self, other: &Path) -> Self {
        let mut new_segments = self.segments.clone();
        new_segments.extend(other.segments.iter().cloned());
        Self {
            segments: new_segments,
        }
    }

    /// Returns true if this path is an ancestor of the given path.
    ///
    /// "/users" is an ancestor of "/users/abc"
    /// "/" (root) is an ancestor of everything except itself
    /// A path is NOT an ancestor of itself.
    pub fn is_ancestor_of(&self, other: &Path) -> bool {
        if self.segments.len() >= other.segments.len() {
            return false;
        }
        // Check that all our segments match the prefix of other
        self.segments
            .iter()
            .zip(other.segments.iter())
            .all(|(a, b)| a == b)
    }

    /// Returns the relative path from ancestor to this path.
    /// Returns None if ancestor is not actually an ancestor.
    ///
    /// "/users/abc/name".relative_to("/users") -> Some("/abc/name")
    pub fn relative_to(&self, ancestor: &Path) -> Option<Self> {
        if !ancestor.is_ancestor_of(self) && ancestor != self {
            return None;
        }
        let relative_segments: SmallVec<[Arc<str>; 8]> = self
            .segments
            .iter()
            .skip(ancestor.segments.len())
            .cloned()
            .collect();
        Some(Self {
            segments: relative_segments,
        })
    }

    /// Convert to a path string with leading slash.
    // `Display` delegates to this method, so it can't simply be removed.
    #[allow(clippy::inherent_to_string_shadow_display)]
    pub fn to_string(&self) -> String {
        if self.segments.is_empty() {
            return "/".to_string();
        }
        let mut result = String::with_capacity(self.segments.iter().map(|s| s.len() + 1).sum());
        for segment in &self.segments {
            result.push('/');
            result.push_str(segment);
        }
        result
    }

    /// Iterate over path segments.
    pub fn iter(&self) -> impl Iterator<Item = &str> {
        self.segments.iter().map(|s| s.as_ref())
    }
}

impl fmt::Display for Path {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

impl fmt::Debug for Path {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Path({})", self.to_string())
    }
}

impl From<&str> for Path {
    fn from(s: &str) -> Self {
        Self::parse(s)
    }
}

impl From<String> for Path {
    fn from(s: String) -> Self {
        Self::parse(&s)
    }
}

/// Normalize a path string: collapse any leading or trailing slashes to a
/// single leading slash and no trailing slash.
///
/// ```text
/// ""              -> "/"
/// "/"             -> "/"
/// "players"       -> "/players"
/// "/players/"     -> "/players"
/// "//players"     -> "/players"
/// "//players//"   -> "/players"
/// ```
///
/// **Internal empty segments are preserved** (e.g. `"/a//b"` stays `"/a//b"`):
/// `validate_path` rejects those at the write boundary, so a request that
/// reaches here has already been validated. Silently collapsing them would
/// hide a future validate_path regression that lets one slip through.
pub fn normalize_path(path: &str) -> String {
    if path.is_empty() || path == "/" {
        return "/".to_string();
    }

    // Trim leading and trailing slashes — any number of either.
    let trimmed = path.trim_matches('/');
    if trimmed.is_empty() {
        return "/".to_string();
    }

    format!("/{}", trimmed)
}

/// Error returned when a key is invalid.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyError {
    pub key: String,
    pub reason: String,
}

impl fmt::Display for KeyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid key '{}': {}", self.key, self.reason)
    }
}

impl std::error::Error for KeyError {}

/// Validate a single key segment.
///
/// Key restrictions:
/// - Cannot be empty
/// - Cannot exceed 768 bytes
/// - Cannot contain: $ # [ ] / or ASCII control characters 0-31 or 127
/// - Cannot contain . except as the first character (for .priority, .value, .sv)
pub fn validate_key(key: &str) -> Result<(), KeyError> {
    if key.is_empty() {
        return Err(KeyError {
            key: key.to_string(),
            reason: "key cannot be empty".to_string(),
        });
    }

    if key.len() > MAX_KEY_BYTES {
        return Err(KeyError {
            key: key.to_string(),
            reason: "key exceeds 768 byte limit".to_string(),
        });
    }

    for (i, c) in key.chars().enumerate() {
        // ASCII control characters (0-31 and 127)
        if c <= '\x1F' || c == '\x7F' {
            return Err(KeyError {
                key: key.to_string(),
                reason: "key contains control character".to_string(),
            });
        }

        // Invalid characters: $ # [ ] /
        match c {
            '$' | '#' | '[' | ']' | '/' => {
                return Err(KeyError {
                    key: key.to_string(),
                    reason: format!("key contains invalid character: {}", c),
                });
            }
            '.'
                // . is only allowed as first character (for .priority, .value, .sv)
                if i > 0 => {
                    return Err(KeyError {
                        key: key.to_string(),
                        reason: "key contains invalid character: .".to_string(),
                    });
                }
            _ => {}
        }
    }

    Ok(())
}

/// Validate all segments of a path.
pub fn validate_path(path: &str) -> Result<(), KeyError> {
    let parsed = Path::parse(path);
    for segment in parsed.segments() {
        validate_key(segment)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // Path parsing tests (ported from Go TestParsePath)
    // =========================================================================

    #[test]
    fn test_parse_root() {
        assert_eq!(Path::parse("/").segments().len(), 0);
        assert!(Path::parse("/").is_root());
    }

    #[test]
    fn test_parse_empty() {
        assert_eq!(Path::parse("").segments().len(), 0);
        assert!(Path::parse("").is_root());
    }

    #[test]
    fn test_parse_simple() {
        let path = Path::parse("/players");
        assert_eq!(path.segments().len(), 1);
        assert_eq!(path.get(0), Some("players"));
    }

    #[test]
    fn test_parse_nested() {
        let path = Path::parse("/players/abc");
        assert_eq!(path.segments().len(), 2);
        assert_eq!(path.get(0), Some("players"));
        assert_eq!(path.get(1), Some("abc"));
    }

    #[test]
    fn test_parse_deeply_nested() {
        let path = Path::parse("/players/abc/score");
        assert_eq!(path.segments().len(), 3);
        assert_eq!(path.get(0), Some("players"));
        assert_eq!(path.get(1), Some("abc"));
        assert_eq!(path.get(2), Some("score"));
    }

    #[test]
    fn test_parse_without_leading_slash() {
        let path = Path::parse("players/abc");
        assert_eq!(path.segments().len(), 2);
        assert_eq!(path.get(0), Some("players"));
        assert_eq!(path.get(1), Some("abc"));
    }

    // =========================================================================
    // NormalizePath tests (ported from Go TestNormalizePath)
    // =========================================================================

    #[test]
    fn test_normalize_root() {
        assert_eq!(normalize_path("/"), "/");
    }

    #[test]
    fn test_normalize_empty() {
        assert_eq!(normalize_path(""), "/");
    }

    #[test]
    fn test_normalize_already_normalized() {
        assert_eq!(normalize_path("/players"), "/players");
    }

    #[test]
    fn test_normalize_without_leading_slash() {
        assert_eq!(normalize_path("players"), "/players");
    }

    #[test]
    fn test_normalize_with_trailing_slash() {
        assert_eq!(normalize_path("/players/"), "/players");
    }

    #[test]
    fn test_normalize_nested_without_leading_slash() {
        assert_eq!(normalize_path("players/abc"), "/players/abc");
    }

    #[test]
    fn test_normalize_nested_with_trailing_slash() {
        assert_eq!(normalize_path("/players/abc/"), "/players/abc");
    }

    #[test]
    fn test_normalize_collapses_doubled_leading_slashes() {
        // Regression: an earlier "fast path" returned `"//foo"` unchanged
        // because it satisfied `starts_with('/') && !ends_with('/')`. The
        // doubled-slash form then leaked into `MutationEvent.path` where
        // `find_affected_shared_views` does raw string-prefix matching,
        // silently missing every subscription on `/foo`.
        assert_eq!(normalize_path("//players"), "/players");
        assert_eq!(normalize_path("///players"), "/players");
        assert_eq!(normalize_path("//players//"), "/players");
        assert_eq!(normalize_path("//posts/post1"), "/posts/post1");
        assert_eq!(
            normalize_path("//user-posts/user1/post1"),
            "/user-posts/user1/post1"
        );
    }

    #[test]
    fn test_normalize_preserves_internal_empty_segments() {
        // Validate-path rejects these at the write boundary; not silently
        // collapsing them here means a future regression that lets one slip
        // through stays visible (and routable to the same wrong place) rather
        // than getting masked by canonicalization.
        assert_eq!(normalize_path("/a//b"), "/a//b");
        assert_eq!(normalize_path("/a//b/"), "/a//b");
    }

    // =========================================================================
    // ValidateKey tests (ported from Go TestValidateKey)
    // =========================================================================

    #[test]
    fn test_validate_key_simple() {
        assert!(validate_key("hello").is_ok());
    }

    #[test]
    fn test_validate_key_numbers() {
        assert!(validate_key("12345").is_ok());
    }

    #[test]
    fn test_validate_key_mixed() {
        assert!(validate_key("user123").is_ok());
    }

    #[test]
    fn test_validate_key_underscores() {
        assert!(validate_key("user_name").is_ok());
    }

    #[test]
    fn test_validate_key_dashes() {
        assert!(validate_key("user-name").is_ok());
    }

    #[test]
    fn test_validate_key_unicode() {
        assert!(validate_key("héllo").is_ok());
    }

    #[test]
    fn test_validate_key_emoji() {
        assert!(validate_key("👍").is_ok());
    }

    #[test]
    fn test_validate_key_dot_prefix_priority() {
        assert!(validate_key(".priority").is_ok());
    }

    #[test]
    fn test_validate_key_dot_prefix_value() {
        assert!(validate_key(".value").is_ok());
    }

    #[test]
    fn test_validate_key_dot_prefix_sv() {
        assert!(validate_key(".sv").is_ok());
    }

    #[test]
    fn test_validate_key_empty() {
        assert!(validate_key("").is_err());
    }

    #[test]
    fn test_validate_key_null_char() {
        assert!(validate_key("hel\x00lo").is_err());
    }

    #[test]
    fn test_validate_key_tab() {
        assert!(validate_key("hel\tlo").is_err());
    }

    #[test]
    fn test_validate_key_newline() {
        assert!(validate_key("hel\nlo").is_err());
    }

    #[test]
    fn test_validate_key_carriage_return() {
        assert!(validate_key("hel\rlo").is_err());
    }

    #[test]
    fn test_validate_key_delete_char() {
        assert!(validate_key("hel\x7Flo").is_err());
    }

    #[test]
    fn test_validate_key_dot_in_middle() {
        assert!(validate_key("hel.lo").is_err());
    }

    #[test]
    fn test_validate_key_dollar_sign() {
        assert!(validate_key("hel$lo").is_err());
    }

    #[test]
    fn test_validate_key_hash() {
        assert!(validate_key("hel#lo").is_err());
    }

    #[test]
    fn test_validate_key_open_bracket() {
        assert!(validate_key("hel[lo").is_err());
    }

    #[test]
    fn test_validate_key_close_bracket() {
        assert!(validate_key("hel]lo").is_err());
    }

    #[test]
    fn test_validate_key_slash() {
        assert!(validate_key("hel/lo").is_err());
    }

    #[test]
    fn test_validate_key_too_long() {
        let long_key = "a".repeat(769);
        assert!(validate_key(&long_key).is_err());
    }

    #[test]
    fn test_validate_key_max_length_ok() {
        let max_key = "a".repeat(768);
        assert!(validate_key(&max_key).is_ok());
    }

    // =========================================================================
    // ValidatePath tests (ported from Go TestValidatePath)
    // =========================================================================

    #[test]
    fn test_validate_path_root() {
        assert!(validate_path("/").is_ok());
    }

    #[test]
    fn test_validate_path_simple() {
        assert!(validate_path("/users").is_ok());
    }

    #[test]
    fn test_validate_path_nested() {
        assert!(validate_path("/users/abc/name").is_ok());
    }

    #[test]
    fn test_validate_path_numbers_in_path() {
        assert!(validate_path("/users/123").is_ok());
    }

    #[test]
    fn test_validate_path_priority_child() {
        assert!(validate_path("/users/abc/.priority").is_ok());
    }

    #[test]
    fn test_validate_path_invalid_char_in_segment() {
        assert!(validate_path("/users/ab#c").is_err());
    }

    #[test]
    fn test_validate_path_dot_in_middle_of_segment() {
        assert!(validate_path("/users/ab.c").is_err());
    }

    #[test]
    fn test_validate_path_control_char() {
        assert!(validate_path("/users/ab\x00c").is_err());
    }

    #[test]
    fn test_validate_path_rejects_internal_empty_segment() {
        // Security: `users//abc` tokenizes to a real ""-keyed segment in storage
        // (`Path::parse` keeps empties) but collapses to `users/abc` in the rules
        // matcher (`find_rules_on_path` skips empties) — a confused-deputy shape.
        // Enforcing validate_path at the write boundary rejects it before that
        // divergence can matter.
        assert!(validate_path("/users//abc").is_err());
        assert!(validate_path("users//abc").is_err());
        assert!(validate_path("a//b//c").is_err());
        // Leading/trailing slashes are trimmed (not internal empties) → fine.
        assert!(validate_path("//users/abc//").is_ok());
    }

    // =========================================================================
    // Path operations tests (additional tests for join, parent, etc.)
    // =========================================================================

    #[test]
    fn test_path_to_string_root() {
        assert_eq!(Path::root().to_string(), "/");
    }

    #[test]
    fn test_path_to_string_simple() {
        assert_eq!(Path::parse("/users").to_string(), "/users");
    }

    #[test]
    fn test_path_to_string_nested() {
        assert_eq!(
            Path::parse("/users/abc/name").to_string(),
            "/users/abc/name"
        );
    }

    #[test]
    fn test_path_join() {
        let path = Path::parse("/users");
        let joined = path.join("abc");
        assert_eq!(joined.to_string(), "/users/abc");
    }

    #[test]
    fn test_path_join_nested() {
        let path = Path::parse("/users");
        let joined = path.join("abc/name");
        assert_eq!(joined.to_string(), "/users/abc/name");
    }

    #[test]
    fn test_path_join_from_root() {
        let path = Path::root();
        let joined = path.join("users");
        assert_eq!(joined.to_string(), "/users");
    }

    #[test]
    fn test_path_parent() {
        let path = Path::parse("/users/abc/name");
        let parent = path.parent().unwrap();
        assert_eq!(parent.to_string(), "/users/abc");
    }

    #[test]
    fn test_path_parent_to_root() {
        let path = Path::parse("/users");
        let parent = path.parent().unwrap();
        assert_eq!(parent.to_string(), "/");
    }

    #[test]
    fn test_path_parent_of_root() {
        let path = Path::root();
        assert!(path.parent().is_none());
    }

    #[test]
    fn test_path_last_segment() {
        let path = Path::parse("/users/abc/name");
        assert_eq!(path.last_segment(), Some("name"));
    }

    #[test]
    fn test_path_last_segment_single() {
        let path = Path::parse("/users");
        assert_eq!(path.last_segment(), Some("users"));
    }

    #[test]
    fn test_path_last_segment_root() {
        let path = Path::root();
        assert_eq!(path.last_segment(), None);
    }

    #[test]
    fn test_is_ancestor_basic() {
        let ancestor = Path::parse("/users");
        let descendant = Path::parse("/users/abc");
        assert!(ancestor.is_ancestor_of(&descendant));
    }

    #[test]
    fn test_is_ancestor_deeper() {
        let ancestor = Path::parse("/users");
        let descendant = Path::parse("/users/abc/name");
        assert!(ancestor.is_ancestor_of(&descendant));
    }

    #[test]
    fn test_is_ancestor_root() {
        let ancestor = Path::root();
        let descendant = Path::parse("/users/abc");
        assert!(ancestor.is_ancestor_of(&descendant));
    }

    #[test]
    fn test_is_ancestor_not_same_path() {
        let path = Path::parse("/users");
        assert!(!path.is_ancestor_of(&path));
    }

    #[test]
    fn test_is_ancestor_not_sibling() {
        let path1 = Path::parse("/users");
        let path2 = Path::parse("/rooms");
        assert!(!path1.is_ancestor_of(&path2));
    }

    #[test]
    fn test_is_ancestor_not_descendant_of_self() {
        let ancestor = Path::parse("/users/abc");
        let shorter = Path::parse("/users");
        assert!(!ancestor.is_ancestor_of(&shorter));
    }

    #[test]
    fn test_is_ancestor_similar_prefix() {
        // "/user" should NOT be ancestor of "/users" - they're siblings
        let path1 = Path::parse("/user");
        let path2 = Path::parse("/users");
        assert!(!path1.is_ancestor_of(&path2));
    }

    #[test]
    fn test_relative_to() {
        let ancestor = Path::parse("/users");
        let descendant = Path::parse("/users/abc/name");
        let relative = descendant.relative_to(&ancestor).unwrap();
        assert_eq!(relative.to_string(), "/abc/name");
    }

    #[test]
    fn test_relative_to_root() {
        let ancestor = Path::root();
        let descendant = Path::parse("/users/abc");
        let relative = descendant.relative_to(&ancestor).unwrap();
        assert_eq!(relative.to_string(), "/users/abc");
    }

    #[test]
    fn test_relative_to_self() {
        let path = Path::parse("/users/abc");
        let relative = path.relative_to(&path).unwrap();
        assert_eq!(relative.to_string(), "/");
    }

    #[test]
    fn test_relative_to_not_ancestor() {
        let path1 = Path::parse("/rooms");
        let path2 = Path::parse("/users/abc");
        assert!(path2.relative_to(&path1).is_none());
    }

    #[test]
    fn test_path_equality() {
        let path1 = Path::parse("/users/abc");
        let path2 = Path::parse("/users/abc");
        assert_eq!(path1, path2);
    }

    #[test]
    fn test_path_inequality() {
        let path1 = Path::parse("/users/abc");
        let path2 = Path::parse("/users/xyz");
        assert_ne!(path1, path2);
    }

    #[test]
    fn test_path_from_str() {
        let path: Path = "/users/abc".into();
        assert_eq!(path.to_string(), "/users/abc");
    }
}
