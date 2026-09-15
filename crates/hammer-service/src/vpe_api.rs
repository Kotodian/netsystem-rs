//! VPE-owned binary API messages.
use std::sync::OnceLock;

use hammer_component_macros::{Api, api_message_table, init_function};
use hammer_ipc::binary_api::ApiMain;
use hammer_infra::svm::queue::{SvmQueueConditionalWait, SvmQueueOperation};
use hammer_runtime::{DataPlaneMain, RuntimeError, RuntimeResult};
use serde::{Deserialize, Serialize};

use crate::net::NetMain;

pub struct VpeApiMain {
    pub net_main: &'static NetMain,
}

macro_rules! fixed_string {
    ($name:ident, $size:expr) => {
        #[derive(Clone, Debug, Serialize, Deserialize, hammer_ipc::binary_api::Typedef)]
        #[api(alias)]
        pub struct $name(pub [u8; $size]);
    };
}

fixed_string!(String32, 32);

#[derive(Clone, Debug, hammer_ipc::binary_api::Typedef)]
#[api(alias)]
pub struct String256(pub [u8; 256]);

impl Serialize for String256 {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(&self.0)
    }
}

impl<'de> Deserialize<'de> for String256 {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let bytes = Vec::<u8>::deserialize(deserializer)?;
        if bytes.len() != 256 {
            return Err(serde::de::Error::invalid_length(bytes.len(), &"256 bytes"));
        }
        let mut value = [0; 256];
        value.copy_from_slice(&bytes);
        Ok(Self(value))
    }
}

fn string32(value: &[u8]) -> String32 {
    let mut bytes = [0; 32];
    let length = value.len().min(31);
    bytes[..length].copy_from_slice(&value[..length]);
    String32(bytes)
}

fn string256(value: &[u8]) -> String256 {
    let mut bytes = [0; 256];
    let length = value.len().min(255);
    bytes[..length].copy_from_slice(&value[..length]);
    String256(bytes)
}

static VPE_API_MAIN: OnceLock<VpeApiMain> = OnceLock::new();

impl VpeApiMain {
    pub fn global() -> RuntimeResult<&'static Self> {
        VPE_API_MAIN.get().ok_or(RuntimeError::RuntimeCapabilityMissing {
            type_name: "hammer_service::binary_api::VpeApiMain",
        })
    }
}

#[init_function(name = "vpe_api_init", runs_after = ["net_main_init"])]
fn init_vpe_api(_: &mut DataPlaneMain) -> RuntimeResult<()> {
    let net_main = NetMain::global()?;
    VPE_API_MAIN
        .set(VpeApiMain { net_main })
        .map_err(|_| RuntimeError::RuntimeCapabilityMissing {
            type_name: "hammer_service::binary_api::VpeApiMain already initialized",
        })
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Api)]
#[api(name = "show_version", returns = ShowVersionReply, handler = show_version_handler)]
pub struct ShowVersion {
    pub id: u16,
    pub client_index: u32,
    pub context: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize, Api)]
#[api(name = "show_version_reply")]
pub struct ShowVersionReply {
    pub id: u16,
    pub context: u32,
    pub retval: i32,
    pub program: String32,
    pub version: String32,
    pub build_date: String32,
    pub build_directory: String256,
}

fn show_version_handler(request: ShowVersion) {
    let api = ApiMain::current();
    let Some(queue) = api.registration_queue(request.client_index) else {
        return;
    };
    if VpeApiMain::global().is_err() {
        return;
    }
    let mut reply = ShowVersionReply {
        id: 30,
        context: request.context,
        retval: 0,
        program: string32(b"vpe"),
        version: string32(env!("CARGO_PKG_VERSION").as_bytes()),
        build_date: string32(option_env!("HAMMER_BUILD_DATE").unwrap_or("unknown").as_bytes()),
        build_directory: string256(option_env!("HAMMER_BUILD_DIRECTORY").unwrap_or("").as_bytes()),
    };
    let message_len = 10 + 32 + 32 + 32 + 256;
    let mut message = unsafe { api.alloc(message_len) };
    let written = unsafe { message.encode(&reply) }.expect("show version reply encodes");
    assert_eq!(written, message_len);
    let address = usize::from(&message).to_ne_bytes();
    let sent = queue.add(&address, SvmQueueConditionalWait::Nowait);
    if sent.as_ref().err().is_some_and(|source| {
        !hammer_ipc::binary_api::memclnt::is_committed(source, SvmQueueOperation::Add)
    }) {
        unsafe { api.free(message) };
    }
    let _ = sent;
}

api_message_table! {
    pub fn setup_message_id_table;
    ShowVersion = 29 { is_mp_safe: true, traced: true, replay: false };
    ShowVersionReply = 30 { is_mp_safe: true, traced: true, replay: false };
}

#[init_function(name = "vpe_api_hookup", runs_after = ["vpe_api_init"])]
fn hookup_vpe_api(_: &mut DataPlaneMain) -> RuntimeResult<()> {
    setup_message_id_table(ApiMain::current());
    Ok(())
}
