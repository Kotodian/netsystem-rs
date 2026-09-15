//! Core control messages. Business operations live here, not on ApiMain.
use super::memclnt::is_committed;
use super::{Api, ApiMain};
use hammer_infra::svm::queue::{SvmQueueConditionalWait, SvmQueueOperation};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Api)]
#[api(name = "control_ping", returns = ControlPingReply, handler = control_ping_handler)]
pub struct ControlPing {
    pub id: u16,
    pub client_index: u32,
    pub context: u32,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Api)]
#[api(name = "control_ping_reply")]
pub struct ControlPingReply {
    pub id: u16,
    pub context: u32,
    pub retval: i32,
    pub client_index: u32,
    pub vpe_pid: u32,
}

fn control_ping_handler(request: ControlPing) {
    let api = ApiMain::current();
    let Some(registration) = api.registration(request.client_index) else {
        return;
    };
    let queue = unsafe { registration.as_ref().input_queue.as_ref() };
    let reply = ControlPingReply {
        id: 24,
        context: request.context,
        retval: 0,
        client_index: request.client_index,
        vpe_pid: std::process::id(),
    };
    let mut message = unsafe { api.alloc(18) };
    let written = unsafe { message.encode(&reply) }
        .expect("owned protocol message encodes at its declared length");
    assert_eq!(written, 18, "message length agrees with API declaration");
    let address = usize::from(&message).to_ne_bytes();
    let sent = queue.add(&address, SvmQueueConditionalWait::Nowait);
    if sent
        .as_ref()
        .err()
        .is_some_and(|source| !is_committed(source, SvmQueueOperation::Add))
    {
        unsafe { api.free(message) };
    }
    if let Err(source) = sent {
        tracing::error!(
            ?source,
            client_index = request.client_index,
            "control ping reply queue"
        );
    }
}

hammer_component_macros::api_message_table! {
    pub fn setup_message_id_table;
    ControlPing = 23 { is_mp_safe: true, traced: true, replay: false };
    ControlPingReply = 24 { is_mp_safe: true, traced: true, replay: false };
}
