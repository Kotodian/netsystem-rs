//! Shared helpers used across config submodules and re-exported for the
//! runtime crates. Keep this small — it's the only place where utilities
//! that don't naturally belong to a single section live.
//!
//! The first inhabitant is `normalize_domain`. Both `[[route.rules]]` and
//! `[[dns.rules]]` accept user-typed domain strings, and the runtime
//! sniffers / reverse-DNS path also write into `metadata.domain`. Anywhere a
//! domain is produced or consumed we want the same canonical form so
//! matchers are pure byte equality at runtime.

/// Canonicalise a user / wire-typed domain string:
///
/// - trim ASCII whitespace,
/// - strip a single trailing dot (DNS canonical names are commonly written
///   either way; hickory hands names back with the trailing dot, sniffers
///   don't),
/// - ASCII-lowercase (DNS labels are case-insensitive on the wire).
///
/// Idempotent. Cheap enough to call on every config-load value and on every
/// sniffed name; allocates one new `String`.
///
/// `#[inline]`: this is called from hammer-runtime sniffers
/// (`sniff_http` / `sniff_tls_sni`) on every TCP connection's first
/// payload; cross-crate inlining via workspace `lto = "fat"` avoids a
/// function call on the hot path.
#[inline]
pub fn normalize_domain(input: &str) -> String {
    input.trim().trim_end_matches('.').to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lowercases_mixed_case() {
        assert_eq!(normalize_domain("Example.COM"), "example.com");
    }

    #[test]
    fn strips_single_trailing_dot() {
        assert_eq!(normalize_domain("ifconfig.so."), "ifconfig.so");
    }

    #[test]
    fn strips_runs_of_trailing_dots() {
        // Greedy strip — matches `Name::to_ascii().trim_end_matches('.')`
        // already used elsewhere; keeps a typo and a canonical FQDN
        // ("foo.bar." vs "foo.bar..") from collapsing into different keys.
        assert_eq!(normalize_domain("foo.bar.."), "foo.bar");
    }

    #[test]
    fn trims_surrounding_whitespace() {
        assert_eq!(normalize_domain("  foo.bar  "), "foo.bar");
    }

    #[test]
    fn empty_input_remains_empty() {
        assert_eq!(normalize_domain(""), "");
        assert_eq!(normalize_domain("   "), "");
        assert_eq!(normalize_domain("."), "");
    }

    #[test]
    fn idempotent() {
        let once = normalize_domain("Example.COM.");
        let twice = normalize_domain(&once);
        assert_eq!(once, twice);
    }
}
