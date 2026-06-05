use std::time::Duration;

use hammer_core::config;
use indoc::indoc;

const MINIMAL_CONFIG: &str = r#"
[tun]
address = ["172.19.0.1/30"]

[[outbounds]]
type = "direct"
id = "direct"

[dns]
server = "https://1.1.1.1/dns-query"

[route]
final = "direct"
"#;

#[test]
fn parse_config_defaults_runtime_disabled() {
    let options = config::parse_config(MINIMAL_CONFIG).expect("parse");

    assert!(!options.runtime.enabled);
    assert_eq!(options.runtime.interval, Duration::from_secs(30));
}

#[test]
fn parse_config_parses_runtime_enabled_5s() {
    let cfg = format!(
        "{MINIMAL_CONFIG}\n{}",
        indoc! {r#"
            [runtime]
            enabled = true
            interval = "5s"
        "#}
    );

    let options = config::parse_config(&cfg).expect("parse runtime");

    assert!(options.runtime.enabled);
    assert_eq!(options.runtime.interval, Duration::from_secs(5));
}

#[test]
fn parse_config_rejects_runtime_zero_interval() {
    let cfg = format!(
        "{MINIMAL_CONFIG}\n{}",
        indoc! {r#"
            [runtime]
            interval = "0s"
        "#}
    );

    let err = config::parse_config(&cfg).expect_err("zero runtime interval must be rejected");
    let msg = err.to_string();

    assert!(
        msg.contains("runtime.interval must be non-zero"),
        "error = {msg:?}"
    );
}

#[test]
fn parse_config_rejects_unknown_runtime_key() {
    let cfg = format!(
        "{MINIMAL_CONFIG}\n{}",
        indoc! {r#"
            [runtime]
            enabled = true
            extra = true
        "#}
    );

    let err = config::parse_config(&cfg).expect_err("unknown runtime key must be rejected");
    let msg = err.to_string();

    assert!(
        msg.contains("unsupported config key: extra"),
        "error = {msg:?}"
    );
}
