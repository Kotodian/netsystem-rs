use hammer_core::config;
use indoc::indoc;

#[test]
fn parse_config_defaults_trace_disabled() {
    let options = config::parse_config("").expect("parse");
    assert!(!options.trace.enabled);
    assert_eq!(options.trace.record_capacity, 1024);
    assert_eq!(options.trace.packet_capacity, 256);
    assert!(options.trace.inputs.is_empty());
}

#[test]
fn parse_config_builds_enabled_trace_options() {
    let cfg = indoc! {r#"
        [trace]
        enabled = true
        record_capacity = 32
        packet_capacity = 8

        [[trace.inputs]]
        node = "tun-input"
        count = 4

        [[trace.inputs]]
        node = "ip-input"
        count = 2
    "#};
    let options = config::parse_config(cfg).expect("parse trace config");
    assert!(options.trace.enabled);
    assert_eq!(options.trace.record_capacity, 32);
    assert_eq!(options.trace.packet_capacity, 8);
    assert_eq!(options.trace.inputs.len(), 2);
    assert_eq!(options.trace.inputs[0].node, "tun-input");
    assert_eq!(options.trace.inputs[0].count, 4);
    assert_eq!(options.trace.inputs[1].node, "ip-input");
    assert_eq!(options.trace.inputs[1].count, 2);
}

#[test]
fn parse_config_keeps_trace_disabled_with_inputs_as_no_marking_policy() {
    let cfg = indoc! {r#"
        [trace]
        enabled = false
        record_capacity = 16
        packet_capacity = 4

        [[trace.inputs]]
        node = "tun-input"
        count = 10
    "#};
    let options = config::parse_config(cfg).expect("parse disabled trace config");
    assert!(!options.trace.enabled);
    assert_eq!(options.trace.record_capacity, 16);
    assert_eq!(options.trace.packet_capacity, 4);
    assert_eq!(options.trace.inputs.len(), 1);
}

#[test]
fn parse_config_allows_enabled_trace_with_empty_inputs() {
    let cfg = indoc! {r#"
        [trace]
        enabled = true
        record_capacity = 16
        packet_capacity = 4
    "#};
    let options = config::parse_config(cfg).expect("parse empty trace inputs");
    assert!(options.trace.enabled);
    assert!(options.trace.inputs.is_empty());
}
