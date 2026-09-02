use std::sync::{Arc, OnceLock};

use hammer_runtime::{GlobalMain, RuntimeError, RuntimeResult};

use crate::interface::InterfaceMain;

pub mod pmtu;

pub struct NetMain {
    interface_main: Arc<InterfaceMain>,
    local_interface_hw_index: u32,
    local_interface_sw_index: u32,
}

impl NetMain {
    pub fn new(interface_main: Arc<InterfaceMain>) -> Self {
        Self {
            interface_main,
            local_interface_hw_index: u32::MAX,
            local_interface_sw_index: u32::MAX,
        }
    }

    pub fn global() -> RuntimeResult<&'static NetMain> {
        NET_MAIN
            .get()
            .map(Arc::as_ref)
            .ok_or(RuntimeError::RuntimeCapabilityMissing {
                type_name: "hammer_service::net::NetMain",
            })
    }

    pub fn init(
        _: &mut GlobalMain,
        interface_main: Arc<InterfaceMain>,
    ) -> RuntimeResult<Arc<NetMain>> {
        Self::init_with_interface(interface_main)
    }

    fn init_with_interface(interface_main: Arc<InterfaceMain>) -> RuntimeResult<Arc<NetMain>> {
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
        let mut value = NetMain::new(interface_main);
        value.local_interface_hw_index = local_hw;
        value.local_interface_sw_index = local_sw;
        let main = Arc::new(value);
        let _ = NET_MAIN.set(Arc::clone(&main));
        Ok(main)
    }

    pub fn interface_main(&self) -> &InterfaceMain {
        &self.interface_main
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

pub static NET_MAIN: OnceLock<Arc<NetMain>> = OnceLock::new();

#[hammer_component_macros::init_function(name = "net_main_init", runs_after = ["interface_main_init"])]
fn init_net_main(
    _: &mut GlobalMain,
    interface_main: Arc<InterfaceMain>,
) -> RuntimeResult<Arc<NetMain>> {
    NetMain::init_with_interface(interface_main)
}
