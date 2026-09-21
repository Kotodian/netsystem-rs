use std::sync::OnceLock;

use hammer_component_macros::{Api, Typedef, api_message_range, api_reply, init_function};
use hammer_ipc::binary_api::ApiMain;
use hammer_runtime::{DataPlaneMain, RuntimeResult};
use serde::{Deserialize, Serialize};

use crate::namespace::namespaces;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Typedef)]
#[api(name = "interface_index", alias)]
pub struct InterfaceIndex(pub u32);

#[derive(Clone, Debug, Api)]
#[api(
    name = "app_namespace_add_del_v4",
    returns = AppNamespaceAddDelReply,
    option(deprecated)
)]
pub struct AppNamespaceAddDel {
    pub id: u16,
    pub client_index: u32,
    pub context: u32,
    pub secret: u64,
    pub is_add: bool,
    pub sw_if_index: InterfaceIndex,
    pub ip4_fib_id: u32,
    pub ip6_fib_id: u32,
    #[api(string = 64)]
    pub namespace_id: String,
    #[api(string)]
    pub sock_name: String,
}

#[derive(Clone, Copy, Debug, Api)]
#[api(name = "app_namespace_add_del_v4_reply")]
pub struct AppNamespaceAddDelReply {
    pub id: u16,
    pub context: u32,
    pub retval: i32,
    pub appns_index: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppNamespaceAddDelRetval {
    Invalid,
    NotSupported,
}

impl From<AppNamespaceAddDelRetval> for i32 {
    #[inline(always)]
    fn from(retval: AppNamespaceAddDelRetval) -> Self {
        match retval {
            AppNamespaceAddDelRetval::Invalid => -19,
            AppNamespaceAddDelRetval::NotSupported => -10,
        }
    }
}

static MSG_ID_BASE: OnceLock<u16> = OnceLock::new();

fn app_namespace_add_del_handler(request: AppNamespaceAddDel) {
    let result = if !request.sock_name.is_empty() {
        Err(AppNamespaceAddDelRetval::NotSupported)
    } else if request.is_add {
        namespaces().add_or_rebind(
            request.namespace_id.clone(),
            request.secret,
            request.sw_if_index.0,
            request.ip4_fib_id,
            request.ip6_fib_id,
        )
    } else {
        namespaces().delete(&request.namespace_id).map(|()| 0)
    };
    let (retval, appns_index) = match result {
        Ok(appns_index) => (0, appns_index),
        Err(retval) => (i32::from(retval), 0),
    };
    api_reply!(
        request,
        base MSG_ID_BASE,
        AppNamespaceAddDelReply {
            retval,
            appns_index,
        }
    );
}

api_message_range! {
    pub fn setup_message_id_table;
    range "session";
    AppNamespaceAddDel => app_namespace_add_del_handler {
        is_mp_safe: false,
        traced: true,
        replay: false
    };
    AppNamespaceAddDelReply {
        is_mp_safe: false,
        traced: true,
        replay: false
    };
}

#[init_function(name = "session_api_hookup")]
fn session_api_hookup(_: &mut DataPlaneMain) -> RuntimeResult<()> {
    let base = setup_message_id_table(ApiMain::current())
        .expect("Session API message range installs once");
    assert!(
        MSG_ID_BASE.set(base).is_ok(),
        "Session API hookup executes once"
    );
    Ok(())
}
