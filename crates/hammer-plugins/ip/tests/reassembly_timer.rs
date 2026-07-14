use hammer_runtime::PluginMain;
use hammer_runtime::plugin_loader::built_plugin_path;

#[test]
fn reassembly_expiry_is_a_main_process_node() {
    let main = PluginMain::load(
        env!("CARGO_PKG_VERSION"),
        built_plugin_path(),
        &["ip".into()],
    )
    .expect("load IP plugin");
    let process = main
        .process_nodes()
        .into_iter()
        .find(|process| process.name == "ip-reassembly-expire-walk")
        .expect("IP reassembly expiry process");

    assert_eq!(process.plugin, Some("ip"));
}
