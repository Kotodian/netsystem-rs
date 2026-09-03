use std::sync::{Arc, OnceLock};

use hammer_runtime::{RuntimeError, RuntimeResult};

use crate::interface::InterfaceMain;

pub mod dpo;
pub mod fib;

pub use dpo::{
    AdjacencyDpo, DpoError, DpoId, DpoMain, DpoProto, DpoType, LoadBalanceDpo, LoadBalanceFlags,
    LookupDpo, ReceiveDpo,
};
pub use fib::{
    FibEntry, FibEntryFlags, FibEntrySrc, FibEntrySrcFlags, FibPath, FibPathExt, FibPathExtList,
    FibPathList, FibPathListFlags, FibSource, FibSourceBehavior, FibTable, FibTableBackend,
};

#[derive(Clone)]
pub struct NetMain {
    interface_main: Arc<InterfaceMain>,
    dpo_main: DpoMain,
    local_interface_hw_index: u32,
    local_interface_sw_index: u32,
}

impl NetMain {
    pub fn global() -> RuntimeResult<&'static NetMain> {
        NET_MAIN
            .get()
            .ok_or(RuntimeError::RuntimeCapabilityMissing {
                type_name: "hammer_service::net::NetMain",
            })
    }

    pub fn init(interface_main: Arc<InterfaceMain>) -> RuntimeResult<Arc<NetMain>> {
        let local_hw = interface_main
            .register_hardware_interface(0, 0, 0, 0)
            .map_err(RuntimeError::from)?;
        interface_main
            .set_interface_name(local_hw, "local0")
            .map_err(RuntimeError::from)?;
        let local_sw = interface_main
            .hardware_interface(local_hw)
            .map(|interface| interface.sw_if_index)
            .ok_or(RuntimeError::RuntimeCapabilityMissing {
                type_name: "local0",
            })?;
        let main = NetMain {
            interface_main,
            dpo_main: DpoMain::new(),
            local_interface_hw_index: local_hw,
            local_interface_sw_index: local_sw,
        };
        let shared = Arc::new(main.clone());
        NET_MAIN
            .set(main)
            .map_err(|_| RuntimeError::PluginStateNotInitialized { plugin: "net" })?;
        Ok(shared)
    }

    pub fn interface_main(&self) -> &InterfaceMain {
        &self.interface_main
    }
    pub fn dpo_main(&self) -> &DpoMain {
        &self.dpo_main
    }
    pub fn interface_main_arc(&self) -> Arc<InterfaceMain> {
        Arc::clone(&self.interface_main)
    }
    pub fn local_interface_hw_index(&self) -> u32 {
        self.local_interface_hw_index
    }
    pub fn local_interface_sw_index(&self) -> u32 {
        self.local_interface_sw_index
    }
}

pub static NET_MAIN: OnceLock<NetMain> = OnceLock::new();

#[hammer_component_macros::init_function(name = "net_main_init", runs_after = ["interface_main_init"])]
fn init_net_main(interface_main: Arc<InterfaceMain>) -> RuntimeResult<Arc<NetMain>> {
    NetMain::init(interface_main)
}
