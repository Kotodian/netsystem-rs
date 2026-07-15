use hammer_ipc::{PluginCommandError, PluginCommandReply};

#[test]
fn plugin_reply_borrows_names_and_carries_typed_errors() {
    let loaded = PluginCommandReply::Loaded(hammer_infra::vec!["ip", "tcp"]);
    let encoded = bincode::serialize(&loaded).expect("encode loaded plugins");
    let decoded: PluginCommandReply<'_> =
        bincode::deserialize(&encoded).expect("decode loaded plugins");
    assert_eq!(
        decoded,
        PluginCommandReply::Loaded(hammer_infra::vec!["ip", "tcp"])
    );

    let failure = PluginCommandReply::Error(PluginCommandError::WorkerGraphUpdate);
    let encoded = bincode::serialize(&failure).expect("encode plugin failure");
    let decoded: PluginCommandReply<'_> =
        bincode::deserialize(&encoded).expect("decode plugin failure");
    assert_eq!(decoded, failure);
}
