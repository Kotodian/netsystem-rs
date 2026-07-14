use hammer_core::config::parse_config;
use hammer_infra::vec::Vec;

#[derive(Debug, PartialEq, Eq, serde::Deserialize)]
#[serde(deny_unknown_fields, default)]
struct TunConfig {
    #[serde(default, alias = "interface")]
    interfaces: Vec<String>,
}

impl Default for TunConfig {
    fn default() -> Self {
        Self {
            interfaces: Vec::new(),
        }
    }
}

#[test]
fn loaded_plugin_decodes_its_owned_config_section() {
    let config = parse_config(
        r#"
plugins = ["tun"]

[plugin.tun]
interfaces = ["utun"]
"#,
    )
    .expect("parse startup config");

    let tun = config
        .plugin_config::<TunConfig>("tun")
        .expect("decode loaded TUN plugin config");

    assert_eq!(
        tun,
        TunConfig {
            interfaces: hammer_infra::vec!["utun".to_owned()],
        }
    );
}

#[test]
fn empty_plugin_section_decodes_to_zero_instances() {
    let config = parse_config(
        r#"
plugins = ["tun"]
"#,
    )
    .expect("parse startup config");

    let tun = config
        .plugin_config::<TunConfig>("tun")
        .expect("decode empty TUN plugin config");
    assert!(tun.interfaces.is_empty());
}

#[test]
fn plugin_toml_text_hands_raw_section_to_plugin() {
    let config = parse_config(
        r#"
plugins = ["tun"]

[plugin.tun]
interfaces = ["utun"]
"#,
    )
    .expect("parse startup config");

    let text = config
        .plugin_toml_text("tun")
        .expect("raw tun plugin toml");
    assert!(text.contains("interfaces"));
    assert!(text.contains("utun"));
}

