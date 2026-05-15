use hammer_core::config::UtlsFingerprint;
#[cfg(feature = "tls-utls")]
use hammer_core::config::UtlsOptions;
#[cfg(feature = "tls-utls")]
use hammer_core::error::HammerError;

pub(super) fn fingerprint_name(fingerprint: UtlsFingerprint) -> &'static str {
    match fingerprint {
        UtlsFingerprint::Chrome => "chrome",
        UtlsFingerprint::Firefox => "firefox",
        UtlsFingerprint::Edge => "edge",
        UtlsFingerprint::Safari => "safari",
        UtlsFingerprint::ThreeSixty => "360",
        UtlsFingerprint::Qq => "qq",
        UtlsFingerprint::Ios => "ios",
        UtlsFingerprint::Android => "android",
        UtlsFingerprint::Random => "random",
        UtlsFingerprint::Randomized => "randomized",
    }
}

#[cfg(feature = "tls-utls")]
pub(super) fn unsupported_for_rustls(options: &UtlsOptions) -> HammerError {
    HammerError::config_validation(format!(
        "tls.utls fingerprint {} requires a uTLS-capable backend; rustls/aws-lc-rs cannot shape ClientHello fingerprints",
        fingerprint_name(options.fingerprint),
    ))
}
