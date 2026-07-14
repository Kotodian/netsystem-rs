//! Functional config tests for VPP-shaped Lab startup surface (#62).
//! Assert typed fields after parse — do not match error/message strings.

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
fn legacy_network_tcp_schema_is_rejected() {
    let error = parse_config(
        r#"
[network.tcp]
mss = 1200
"#,
    )
    .expect_err("TCP schema belongs to plugin.tcp");

    assert!(error.to_string().contains("tcp"));
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
    let tcp = cfg.plugin_toml_text("tcp").expect("TCP plugin TOML");
    assert!(tcp.contains("time_wait = \"2s\""));
    assert!(tcp.contains("probe_limit = 3"));
    assert_eq!(cfg.network.interface.len(), 1);
    assert_eq!(cfg.network.interface[0].name, "utun");
    assert_eq!(
        cfg.network.interface[0].address[0].to_string(),
        "10.66.77.1/30"
    );
    assert_eq!(cfg.network.route.len(), 1);
    assert_eq!(cfg.network.route[0].prefix.to_string(), "10.66.77.0/30");
}
