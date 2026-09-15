//! Core control messages. Business operations live here, not on ApiMain.
use super::{Api, CONTROL_PING_REPLY};
use hammer_component_macros::api_reply;

#[derive(Clone, Copy, Debug, Api)]
#[api(name = "control_ping", returns = ControlPingReply)]
pub struct ControlPing {
    pub id: u16,
    pub client_index: u32,
    pub context: u32,
}

#[derive(Clone, Copy, Debug, Api)]
#[api(name = "control_ping_reply")]
pub struct ControlPingReply {
    pub id: u16,
    pub context: u32,
    pub retval: i32,
    pub client_index: u32,
    pub vpe_pid: u32,
}

fn control_ping_handler(request: ControlPing) {
    api_reply!(request, id CONTROL_PING_REPLY, ControlPingReply {
        retval: 0,
        client_index: request.client_index,
        vpe_pid: std::process::id(),
    });
}

hammer_component_macros::api_message_table! {
    pub fn setup_message_id_table;
    ids super;
    ControlPing => control_ping_handler { is_mp_safe: true, traced: true, replay: false };
    ControlPingReply { is_mp_safe: true, traced: true, replay: false };
}
