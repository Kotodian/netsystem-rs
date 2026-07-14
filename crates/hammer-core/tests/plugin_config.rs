use hammer_core::config::parse_config;

#[derive(Debug, PartialEq, Eq, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct TunConfig {
    interface: Vec<String>,
}

#[test]
fn loaded_plugin_decodes_its_owned_config_section() {
    let config = parse_config(
        r#"
plugins = ["tun"]

[tun]
interface = ["utun"]
"#,
    )
    .expect("parse startup config");

    let tun = config
        .plugin::<TunConfig>("tun")
        .expect("decode loaded TUN plugin config");

    assert_eq!(
        tun,
        TunConfig {
            interface: vec!["utun".to_owned()],
        }
    );
}
