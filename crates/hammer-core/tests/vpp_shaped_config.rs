//! Functional config tests for VPP-shaped Lab startup surface (#62).
//! Assert typed fields after parse — do not match error/message strings.

use std::net::SocketAddr;
use std::time::Duration;

use hammer_core::config::{SessionBackend, parse_config};

#[test]
fn vpp_worker_aliases_drive_typed_worker_fields() {
    let cfg = parse_config(
        r#"
[worker]
workers = 1
poll_sleep = "50ms"

[worker.buffer]
data_size = 2048
buffers_per_numa = 2048
"#,
    )
    .expect("parse VPP-shaped worker aliases");

    assert_eq!(cfg.worker.count, 1);
    assert_eq!(cfg.worker.idle_slice, Duration::from_millis(50));
    assert_eq!(cfg.worker.buffer.slot_bytes, 2048);
    assert_eq!(cfg.worker.buffer.slots_per_numa, 2048);
}

#[test]
fn vpp_session_aliases_drive_typed_session_fields() {
    let cfg = parse_config(
        r#"
[network.session]
backend = "svm"
attach_socket_path = "/var/run/hammer/attach.sock"
preallocated_sessions = 64
event_queue_length = 256
"#,
    )
    .expect("parse VPP-shaped session aliases");

    let session = cfg.network.session.as_ref().expect("configured session");
    assert_eq!(session.backend, SessionBackend::Svm);
    assert_eq!(
        session.attach_socket_path.as_deref(),
        Some("/var/run/hammer/attach.sock")
    );
    assert_eq!(session.pool_capacity, 64);
    assert_eq!(session.ready_queue_capacity, 256);
}

#[test]
fn tcp_listen_entries_bind_typed_socket_addrs() {
    let cfg = parse_config(
        r#"
[[network.tcp.listen]]
address = "10.66.77.1:7300"

[[network.tcp.listen]]
address = "10.66.77.1:7301"
md5_password = "lab-secret"
"#,
    )
    .expect("parse tcp listen entries");

    let listen = &cfg.network.tcp.listen;
    assert_eq!(listen.len(), 2);
    assert_eq!(
        listen[0].address,
        "10.66.77.1:7300".parse::<SocketAddr>().expect("addr")
    );
    assert!(listen[0].md5_password.is_none());
    assert!(listen[0].ao_keys.is_empty());
    assert_eq!(
        listen[1].address,
        "10.66.77.1:7301".parse::<SocketAddr>().expect("addr")
    );
    assert_eq!(listen[1].md5_password.as_deref(), Some("lab-secret"));
}

#[test]
fn tcp_nagle_and_pmtu_parse_as_typed_policy() {
    let cfg = parse_config(
        r#"
[network.tcp]
nagle = false

[network.tcp.pmtu]
enabled = false
"#,
    )
    .expect("parse nagle/pmtu");

    assert!(!cfg.network.tcp.nagle);
    assert!(!cfg.network.tcp.pmtu.enabled);
}

#[test]
fn tcp_nagle_and_pmtu_default_enabled() {
    let cfg = parse_config("").expect("empty config");
    assert!(cfg.network.tcp.nagle);
    assert!(cfg.network.tcp.pmtu.enabled);
    assert!(cfg.network.tcp.listen.is_empty());
}

#[test]
fn tcp_ao_keys_parse_on_listen_entry() {
    let cfg = parse_config(
        r#"
[[network.tcp.listen]]
address = "10.66.77.1:7300"

[[network.tcp.listen.ao_keys]]
key_id = 1
rnext_key_id = 2
key = "ao-material"
"#,
    )
    .expect("parse ao keys");

    assert_eq!(cfg.network.tcp.listen.len(), 1);
    let keys = &cfg.network.tcp.listen[0].ao_keys;
    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0].key_id, 1);
    assert_eq!(keys[0].rnext_key_id, 2);
    assert_eq!(keys[0].key, "ao-material");
}

#[test]
fn lab_toml_example_parses_to_locked_topology() {
    let content = include_str!("../../../examples/tun-tcp-echo.toml");
    let cfg = parse_config(content).expect("parse Lab TOML");

    assert_eq!(cfg.worker.count, 1);
    assert_eq!(cfg.worker.idle_slice, Duration::from_millis(50));
    assert_eq!(cfg.worker.buffer.slot_bytes, 2048);
    assert_eq!(cfg.worker.buffer.slots_per_numa, 2048);
    let session = cfg.network.session.as_ref().expect("configured session");
    assert_eq!(session.backend, SessionBackend::Svm);
    assert_eq!(
        session.attach_socket_path.as_deref(),
        Some("/var/run/hammer/attach.sock")
    );
    assert_eq!(session.pool_capacity, 64);
    assert_eq!(session.ready_queue_capacity, 256);
    assert!(cfg.network.tcp.nagle);
    assert!(cfg.network.tcp.pmtu.enabled);
    assert_eq!(cfg.network.tcp.time_wait, Duration::from_secs(2));
    assert_eq!(cfg.network.tcp.keepalive.idle, Duration::from_secs(3));
    assert_eq!(
        cfg.network.tcp.keepalive.probe_interval,
        Duration::from_secs(1)
    );
    assert_eq!(cfg.network.tcp.keepalive.probe_limit, 3);
    assert_eq!(cfg.network.tcp.listen.len(), 1);
    assert_eq!(
        cfg.network.tcp.listen[0].address,
        "10.66.77.1:7300".parse::<SocketAddr>().expect("listen")
    );
    assert_eq!(cfg.network.interface.len(), 1);
    assert_eq!(cfg.network.interface[0].name, "utun");
    assert_eq!(
        cfg.network.interface[0].address[0].to_string(),
        "10.66.77.1/30"
    );
    assert_eq!(cfg.network.route.len(), 1);
    assert_eq!(cfg.network.route[0].prefix.to_string(), "10.66.77.0/30");
}
