//! VPE-owned binary API messages.
use std::sync::OnceLock;

use hammer_component_macros::{Api, api_message_range, api_reply, init_function};
use hammer_ipc::binary_api::ApiMain;
use hammer_runtime::{DataPlaneMain, RuntimeError, RuntimeResult};

use crate::net::NetMain;

pub struct VpeApiMain {
    pub net_main: &'static NetMain,
}

#[derive(Clone, Copy, Debug, Api)]
#[api(name = "show_version", returns = ShowVersionReply)]
pub struct ShowVersion {
    pub id: u16,
    pub client_index: u32,
    pub context: u32,
}

#[derive(Clone, Debug, Api)]
#[api(name = "show_version_reply")]
pub struct ShowVersionReply {
    pub id: u16,
    pub context: u32,
    pub retval: i32,
    #[api(string = 32)]
    pub program: String,
    #[api(string = 32)]
    pub version: String,
    #[api(string = 32)]
    pub build_date: String,
    #[api(string = 256)]
    pub build_directory: String,
}

static VPE_API_MAIN: OnceLock<VpeApiMain> = OnceLock::new();
static MSG_ID_BASE: OnceLock<u16> = OnceLock::new();

impl VpeApiMain {
    pub fn global() -> RuntimeResult<&'static Self> {
        VPE_API_MAIN
            .get()
            .ok_or(RuntimeError::RuntimeCapabilityMissing {
                type_name: "hammer_service::binary_api::VpeApiMain",
            })
    }
}

#[init_function(name = "vpe_api_init", runs_after = ["net_main_init"])]
fn vpe_api_init(_: &mut DataPlaneMain) -> RuntimeResult<()> {
    let net_main = NetMain::global()?;
    assert!(
        VPE_API_MAIN.set(VpeApiMain { net_main }).is_ok(),
        "VPE API init executes once"
    );
    Ok(())
}

fn show_version_handler(request: ShowVersion) {
    api_reply!(
        request,
        base MSG_ID_BASE,
        ShowVersionReply {
            retval: 0,
            program: "vpe".to_owned(),
            version: env!("CARGO_PKG_VERSION").to_owned(),
            build_date: option_env!("HAMMER_BUILD_DATE")
                .unwrap_or("unknown")
                .to_owned(),
            build_directory: env!("CARGO_MANIFEST_DIR").to_owned(),
        }
    );
}

api_message_range! {
    pub fn setup_message_id_table;
    range "vpe";
    ShowVersion => show_version_handler { is_mp_safe: true, traced: true, replay: false };
    ShowVersionReply { is_mp_safe: true, traced: true, replay: false };
}

#[init_function(name = "vpe_api_hookup")]
fn vpe_api_hookup(_: &mut DataPlaneMain) -> RuntimeResult<()> {
    let base =
        setup_message_id_table(ApiMain::current()).expect("VPE API message range installs once");
    assert!(
        MSG_ID_BASE.set(base).is_ok(),
        "VPE API hookup executes once"
    );
    Ok(())
}
