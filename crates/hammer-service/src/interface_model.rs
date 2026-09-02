use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use hammer_core::data_plane::NodeId;
use hammer_infra::bitmap::Bitmap;
use hammer_infra::pool::Pool;
use hammer_runtime::{DataWorkerId, GlobalMain, RuntimeResult};
use ipnet::IpNet;

use crate::interface::{InterfaceError, InterfaceMtu, InterfaceMtuKind, InterfaceResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DriverScheduleMode {
    Poll,
    Interrupt,
    Adaptive,
}

pub type InterfaceCallback = fn(&InterfaceMain, u32, bool) -> InterfaceResult<()>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InterfaceCallbackRegistration {
    pub callback: InterfaceCallback,
    pub priority: u8,
}

pub type HwInterfaceCallback = InterfaceCallbackRegistration;
pub type SwInterfaceCallback = InterfaceCallbackRegistration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeviceClass {
    pub name: &'static str,
    pub index: Option<u32>,
    pub interface_add_del_function: Option<InterfaceCallback>,
    pub admin_up_down_function: Option<InterfaceCallback>,
    pub tx_function: Option<fn()>,
    pub format_device_name: Option<fn(&mut fmt::Formatter<'_>) -> fmt::Result>,
    pub unformat_device_name: Option<fn(&str) -> Result<(), InterfaceError>>,
    pub subif_add_del_function: Option<fn()>,
    pub rx_mode_change_function: Option<fn()>,
    pub set_l2_mode_function: Option<fn()>,
    pub redistribute: Option<fn()>,
    pub tx_fn_registrations: Option<&'static [fn()]>,
    pub tx_function_error_strings: Option<&'static [&'static str]>,
    pub tx_function_error_counters: Option<&'static [u64]>,
    pub tx_function_n_errors: Option<u32>,
    pub name_renumber: Option<fn()>,
    pub flow_ops_function: Option<fn()>,
    pub format_device: Option<fn()>,
    pub format_tx_trace: Option<fn()>,
    pub format_flow: Option<fn()>,
    pub ip_tun_desc: Option<fn()>,
    pub clear_counters: Option<fn()>,
    pub is_valid_class_for_interface: Option<fn() -> bool>,
    pub hw_class_change: Option<fn()>,
    pub rx_redirect_to_node: Option<fn()>,
    pub mac_addr_change_function: Option<fn()>,
    pub mac_addr_add_del_function: Option<fn()>,
    pub set_rss_queues_function: Option<fn()>,
    pub eeprom_read_function: Option<fn()>,
    pub set_link_speed_function: Option<fn()>,
    pub traffic_manager_impl: Option<fn()>,
}

impl DeviceClass {
    pub const fn new(name: &'static str) -> Self {
        Self {
            name,
            index: None,
            interface_add_del_function: None,
            admin_up_down_function: None,
            tx_function: None,
            format_device_name: None,
            unformat_device_name: None,
            subif_add_del_function: None,
            rx_mode_change_function: None,
            set_l2_mode_function: None,
            redistribute: None,
            tx_fn_registrations: None,
            tx_function_error_strings: None,
            tx_function_error_counters: None,
            tx_function_n_errors: None,
            name_renumber: None,
            flow_ops_function: None,
            format_device: None,
            format_tx_trace: None,
            format_flow: None,
            ip_tun_desc: None,
            clear_counters: None,
            is_valid_class_for_interface: None,
            hw_class_change: None,
            rx_redirect_to_node: None,
            mac_addr_change_function: None,
            mac_addr_add_del_function: None,
            set_rss_queues_function: None,
            eeprom_read_function: None,
            set_link_speed_function: None,
            traffic_manager_impl: None,
        }
    }

    pub const fn index(&self) -> Option<u32> {
        self.index
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HwClass {
    pub name: &'static str,
    pub index: Option<u32>,
    pub flags: u32,
    pub tx_hash_fn_type: u8,
    pub interface_add_del_function: Option<InterfaceCallback>,
    pub admin_up_down_function: Option<InterfaceCallback>,
    pub link_up_down_function: Option<fn()>,
    pub mac_addr_change_function: Option<fn()>,
    pub mac_addr_add_del_function: Option<fn()>,
    pub set_max_frame_size: Option<fn()>,
    pub format_interface_name: Option<fn()>,
    pub format_address: Option<fn()>,
    pub format_header: Option<fn()>,
    pub format_device: Option<fn()>,
    pub unformat_hw_address: Option<fn(&str) -> Result<(), InterfaceError>>,
    pub unformat_header: Option<fn()>,
    pub build_rewrite: Option<fn()>,
    pub update_adjacency: Option<fn()>,
    pub is_valid_class_for_interface: Option<fn() -> bool>,
    pub hw_class_change: Option<fn()>,
}

impl HwClass {
    pub const fn new(name: &'static str) -> Self {
        Self {
            name,
            index: None,
            flags: 0,
            tx_hash_fn_type: 0,
            interface_add_del_function: None,
            admin_up_down_function: None,
            link_up_down_function: None,
            mac_addr_change_function: None,
            mac_addr_add_del_function: None,
            set_max_frame_size: None,
            format_interface_name: None,
            format_address: None,
            format_header: None,
            format_device: None,
            unformat_hw_address: None,
            unformat_header: None,
            build_rewrite: None,
            update_adjacency: None,
            is_valid_class_for_interface: None,
            hw_class_change: None,
        }
    }

    pub const fn index(&self) -> Option<u32> {
        self.index
    }
}

#[derive(Debug, Clone, Copy)]
pub struct InterfaceRegistrationImage {
    pub device_class_registrations: &'static [DeviceClass],
    pub hw_interface_class_registrations: &'static [HwClass],
    pub hw_interface_callbacks: &'static [InterfaceCallbackRegistration],
    pub sw_interface_callbacks: &'static [InterfaceCallbackRegistration],
}

impl InterfaceRegistrationImage {
    pub const fn new(
        device_class_registrations: &'static [DeviceClass],
        hw_interface_class_registrations: &'static [HwClass],
        hw_interface_callbacks: &'static [InterfaceCallbackRegistration],
        sw_interface_callbacks: &'static [InterfaceCallbackRegistration],
    ) -> Self {
        Self {
            device_class_registrations,
            hw_interface_class_registrations,
            hw_interface_callbacks,
            sw_interface_callbacks,
        }
    }
}

#[derive(Debug, Clone)]
pub struct HwInterface {
    pub flags: u32,
    pub caps: u32,
    pub hw_address: Vec<u8>,
    pub output_node_index: NodeId,
    pub tx_node_index: NodeId,
    pub dev_class_index: u32,
    pub dev_instance: u32,
    pub hw_class_index: u32,
    pub hw_instance: u32,
    pub hw_if_index: u32,
    pub sw_if_index: u32,
    pub name: String,
    pub link_speed: u64,
    pub supported_link_speeds: Vec<u64>,
    pub input_node_index: NodeId,
    pub default_rx_mode: DriverScheduleMode,
    pub rx_queue_indices: Vec<u32>,
    pub tx_queue_indices: Vec<u32>,
    pub numa_node: u32,
}

impl HwInterface {
    pub fn hw_if_index(&self) -> u32 {
        self.hw_if_index
    }
    pub fn sw_if_index(&self) -> u32 {
        self.sw_if_index
    }
    pub fn device_instance(&self) -> u32 {
        self.dev_instance
    }
    pub fn queue_indices(&self) -> (&[u32], &[u32]) {
        (&self.rx_queue_indices, &self.tx_queue_indices)
    }
}

#[derive(Debug, Clone)]
pub struct SwInterface {
    pub interface_type: u8,
    pub flags: u32,
    pub sw_if_index: u32,
    pub sup_sw_if_index: u32,
    pub unnumbered_sw_if_index: Option<u32>,
    pub hw_if_index: Option<u32>,
    pub mtu: InterfaceMtu,
    pub addresses: Vec<u32>,
}

impl SwInterface {
    pub fn sw_if_index(&self) -> u32 {
        self.sw_if_index
    }
    pub fn hw_if_index(&self) -> Option<u32> {
        self.hw_if_index
    }
    pub fn mtu(&self, slot: usize) -> u32 {
        match slot {
            0 => self.mtu.l3(),
            1 => self.mtu.ip4(),
            2 => self.mtu.ip6(),
            3 => self.mtu.mpls(),
            _ => 0,
        }
    }
    pub fn is_admin_up(&self) -> bool {
        self.flags & 1 != 0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RxQueue {
    pub hw_if_index: u32,
    pub device_instance: u32,
    pub worker: DataWorkerId,
    pub file_index: u32,
    pub queue_id: u32,
    pub mode: DriverScheduleMode,
}

impl RxQueue {
    pub fn is_polling(&self) -> bool {
        matches!(
            self.mode,
            DriverScheduleMode::Poll | DriverScheduleMode::Adaptive
        )
    }
    pub fn is_interrupt(&self) -> bool {
        matches!(self.mode, DriverScheduleMode::Interrupt)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxQueue {
    pub shared_queue: bool,
    pub hw_if_index: u32,
    pub device_instance: u32,
    pub queue_id: u32,
    pub(crate) output_slot: u16,
    pub(crate) drop_slot: u16,
    assigned_workers: Bitmap<DataWorkerId>,
}

impl TxQueue {
    pub fn is_shared(&self) -> bool {
        self.shared_queue
    }
    pub fn is_assigned_to(&self, worker: DataWorkerId) -> bool {
        self.assigned_workers.is_set(worker)
    }
}

#[derive(Default)]
struct InterfaceState {
    hardware_interfaces: Pool<HwInterface>,
    software_interfaces: Pool<SwInterface>,
    rx_queues: Pool<RxQueue>,
    tx_queues: Pool<TxQueue>,
    names: HashMap<String, u32>,
    addresses: Vec<(u32, IpNet)>,
    device_classes: Vec<DeviceClass>,
    hw_classes: Vec<HwClass>,
    hw_callbacks: Vec<InterfaceCallbackRegistration>,
    sw_callbacks: Vec<InterfaceCallbackRegistration>,
}

pub struct InterfaceMain {
    state: UnsafeCell<InterfaceState>,
}

unsafe impl Send for InterfaceMain {}
unsafe impl Sync for InterfaceMain {}

impl Default for InterfaceMain {
    fn default() -> Self {
        Self::new()
    }
}

impl InterfaceMain {
    pub fn new() -> Self {
        Self {
            state: UnsafeCell::new(InterfaceState::default()),
        }
    }

    fn state(&self) -> &InterfaceState {
        // SAFETY: control-plane mutation is serialized by the worker barrier;
        // readers only borrow the published state.
        unsafe { &*self.state.get() }
    }
    #[allow(clippy::mut_from_ref)]
    fn state_mut(&self) -> &mut InterfaceState {
        // SAFETY: callers perform mutations during single-threaded setup or a
        // worker-barrier interval, so no concurrent mutable borrow exists.
        unsafe { &mut *self.state.get() }
    }

    pub fn consume_registration_image(
        &self,
        image: &InterfaceRegistrationImage,
    ) -> RuntimeResult<()> {
        let state = self.state_mut();
        for class in image.device_class_registrations.iter().rev() {
            let mut class = *class;
            class.index = Some(state.device_classes.len() as u32);
            state.device_classes.push(class);
        }
        for class in image.hw_interface_class_registrations.iter().rev() {
            let mut class = *class;
            class.index = Some(state.hw_classes.len() as u32);
            state.hw_classes.push(class);
        }
        state
            .hw_callbacks
            .extend_from_slice(image.hw_interface_callbacks);
        state
            .sw_callbacks
            .extend_from_slice(image.sw_interface_callbacks);
        state
            .hw_callbacks
            .sort_by_key(|registration| registration.priority);
        state
            .sw_callbacks
            .sort_by_key(|registration| registration.priority);
        Ok(())
    }

    pub fn register_hardware_interface(
        &self,
        device_class_index: u32,
        device_instance: u32,
        hw_class_index: u32,
        hw_instance: u32,
    ) -> InterfaceResult<u32> {
        let state = self.state_mut();
        let hw_if_index = state.hardware_interfaces.insert(HwInterface {
            flags: 0,
            caps: 0,
            hw_address: Vec::new(),
            output_node_index: NodeId::new(0),
            tx_node_index: NodeId::new(0),
            dev_class_index: device_class_index,
            dev_instance: device_instance,
            hw_class_index,
            hw_instance,
            hw_if_index: 0,
            sw_if_index: 0,
            name: format!("if{device_instance}"),
            link_speed: 0,
            supported_link_speeds: Vec::new(),
            input_node_index: NodeId::new(0),
            default_rx_mode: DriverScheduleMode::Interrupt,
            rx_queue_indices: Vec::new(),
            tx_queue_indices: Vec::new(),
            numa_node: 0,
        });
        let sw_if_index = state.software_interfaces.insert(SwInterface {
            interface_type: 0,
            flags: 0,
            sw_if_index: 0,
            sup_sw_if_index: 0,
            unnumbered_sw_if_index: None,
            hw_if_index: Some(hw_if_index),
            mtu: InterfaceMtu::default(),
            addresses: Vec::new(),
        });
        state
            .hardware_interfaces
            .get_mut(hw_if_index)
            .expect("inserted hardware interface")
            .hw_if_index = hw_if_index;
        state
            .hardware_interfaces
            .get_mut(hw_if_index)
            .expect("inserted hardware interface")
            .sw_if_index = sw_if_index;
        state
            .software_interfaces
            .get_mut(sw_if_index)
            .expect("inserted software interface")
            .sw_if_index = sw_if_index;
        Ok(hw_if_index)
    }

    pub fn delete_hardware_interface(&self, hw_if_index: u32) -> InterfaceResult<()> {
        self.delete_hardware_interface_with_file_cleanup(hw_if_index, |_| {})
    }

    pub fn delete_hardware_interface_with_file_cleanup(
        &self,
        hw_if_index: u32,
        mut remove_file_interest: impl FnMut(u32),
    ) -> InterfaceResult<()> {
        let state = self.state_mut();
        let hw = state
            .hardware_interfaces
            .get(hw_if_index)
            .ok_or(InterfaceError::NotRegistered {
                interface_index: hw_if_index,
            })?
            .clone();
        for index in &hw.rx_queue_indices {
            if let Some(queue) = state.rx_queues.get(*index) {
                remove_file_interest(queue.file_index);
            }
        }
        for index in hw.rx_queue_indices {
            state.rx_queues.remove(index);
        }
        for index in hw.tx_queue_indices {
            state.tx_queues.remove(index);
        }
        state
            .addresses
            .retain(|(interface, _)| *interface != hw.sw_if_index);
        state.software_interfaces.remove(hw.sw_if_index);
        state.names.retain(|_, index| *index != hw_if_index);
        state.hardware_interfaces.remove(hw_if_index);
        Ok(())
    }

    pub fn hardware_interface(&self, index: u32) -> Option<&HwInterface> {
        self.state().hardware_interfaces.get(index)
    }
    pub fn software_interface(&self, index: u32) -> Option<&SwInterface> {
        self.state().software_interfaces.get(index)
    }

    pub fn interface_index(&self, name: &str) -> Option<u32> {
        self.state().names.get(name).copied()
    }
    pub fn set_interface_name(
        &self,
        hw_if_index: u32,
        name: impl Into<String>,
    ) -> InterfaceResult<()> {
        let state = self.state_mut();
        let name = name.into();
        let hw = state.hardware_interfaces.get_mut(hw_if_index).ok_or(
            InterfaceError::NotRegistered {
                interface_index: hw_if_index,
            },
        )?;
        if name.is_empty() {
            return Err(InterfaceError::NameEmpty);
        }
        state.names.retain(|_, index| *index != hw_if_index);
        hw.name = name.clone();
        drop(hw);
        state.names.insert(name.clone(), hw_if_index);
        Ok(())
    }
    pub fn interface_name(&self, index: u32) -> Option<String> {
        self.hardware_interface(index).map(|hw| hw.name.clone())
    }
    pub fn interface_addresses(&self, index: u32) -> Vec<IpNet> {
        self.state()
            .addresses
            .iter()
            .filter(|(item, _)| *item == index)
            .map(|(_, address)| *address)
            .collect()
    }
    pub fn interface_mtu(&self, index: u32) -> Option<InterfaceMtu> {
        self.hardware_interface(index)
            .and_then(|hw| self.software_interface(hw.sw_if_index))
            .map(|sw| sw.mtu)
    }
    pub fn interface_address_index(&self, index: u32, address: IpNet) -> Option<u32> {
        self.state()
            .addresses
            .iter()
            .position(|(item, value)| *item == index && *value == address)
            .and_then(|value| u32::try_from(value).ok())
    }

    pub fn register_rx_queue(
        &self,
        hw_if_index: u32,
        queue_id: u32,
        worker: DataWorkerId,
        file_index: u32,
        mode: DriverScheduleMode,
    ) -> InterfaceResult<u32> {
        let state = self.state_mut();
        let device_instance = state
            .hardware_interfaces
            .get(hw_if_index)
            .ok_or(InterfaceError::NotRegistered {
                interface_index: hw_if_index,
            })?
            .dev_instance;
        let queue_index = state.rx_queues.insert(RxQueue {
            hw_if_index,
            device_instance,
            worker,
            file_index,
            queue_id,
            mode,
        });
        state
            .hardware_interfaces
            .get_mut(hw_if_index)
            .expect("hardware interface exists")
            .rx_queue_indices
            .push(queue_index);
        Ok(queue_index)
    }

    pub fn register_tx_queue(
        &self,
        hw_if_index: u32,
        queue_id: u32,
        shared: bool,
    ) -> InterfaceResult<u32> {
        let state = self.state_mut();
        let device_instance = state
            .hardware_interfaces
            .get(hw_if_index)
            .ok_or(InterfaceError::NotRegistered {
                interface_index: hw_if_index,
            })?
            .dev_instance;
        let queue_index = state.tx_queues.insert(TxQueue {
            shared_queue: shared,
            hw_if_index,
            device_instance,
            queue_id,
            output_slot: 0,
            drop_slot: 0,
            assigned_workers: Bitmap::new(),
        });
        state
            .hardware_interfaces
            .get_mut(hw_if_index)
            .expect("hardware interface exists")
            .tx_queue_indices
            .push(queue_index);
        Ok(queue_index)
    }

    pub fn assign_tx_queue_to_worker(
        &self,
        tx_queue_index: u32,
        worker: DataWorkerId,
    ) -> InterfaceResult<()> {
        self.state_mut()
            .tx_queues
            .get_mut(tx_queue_index)
            .ok_or(InterfaceError::NotRegistered {
                interface_index: tx_queue_index,
            })?
            .assigned_workers
            .set(worker);
        Ok(())
    }

    pub fn rx_queues_for_worker(&self, worker: DataWorkerId) -> Vec<RxQueue> {
        self.state()
            .rx_queues
            .iter()
            .filter(|(_, queue)| queue.worker == worker)
            .map(|(_, queue)| *queue)
            .collect()
    }
    pub fn tx_queues_for_worker(&self, worker: DataWorkerId) -> Vec<TxQueue> {
        self.state()
            .tx_queues
            .iter()
            .filter(|(_, queue)| queue.is_assigned_to(worker))
            .map(|(_, queue)| queue.clone())
            .collect()
    }
    pub fn tx_queues(&self) -> Vec<TxQueue> {
        self.state()
            .tx_queues
            .iter()
            .map(|(_, queue)| queue.clone())
            .collect()
    }

    pub fn tx_slot_for_worker(&self, worker: DataWorkerId, sw_if_index: u32) -> Option<u16> {
        let hw_if_index = self.software_interface(sw_if_index)?.hw_if_index?;
        self.state()
            .tx_queues
            .iter()
            .find(|(_, queue)| queue.hw_if_index == hw_if_index && queue.is_assigned_to(worker))
            .map(|(_, queue)| queue.output_slot)
    }

    pub fn set_mtu(&self, sw_if_index: u32, mtu: InterfaceMtu) -> InterfaceResult<()> {
        self.state_mut()
            .software_interfaces
            .get_mut(sw_if_index)
            .ok_or(InterfaceError::NotRegistered {
                interface_index: sw_if_index,
            })?
            .mtu = mtu;
        Ok(())
    }
    pub fn set_protocol_mtu(
        &self,
        sw_if_index: u32,
        kind: InterfaceMtuKind,
        value: u32,
    ) -> InterfaceResult<()> {
        let state = self.state_mut();
        let sw = state.software_interfaces.get_mut(sw_if_index).ok_or(
            InterfaceError::NotRegistered {
                interface_index: sw_if_index,
            },
        )?;
        sw.mtu.set(kind, value);
        Ok(())
    }
    pub fn add_address(&self, sw_if_index: u32, address: IpNet) -> InterfaceResult<u32> {
        let state = self.state_mut();
        if let Some(index) = state
            .addresses
            .iter()
            .position(|(item, value)| *item == sw_if_index && *value == address)
        {
            return Ok(index as u32);
        }
        let index = state.addresses.len() as u32;
        state.addresses.push((sw_if_index, address));
        state
            .software_interfaces
            .get_mut(sw_if_index)
            .ok_or(InterfaceError::NotRegistered {
                interface_index: sw_if_index,
            })?
            .addresses
            .push(index);
        Ok(index)
    }
    pub fn remove_address(&self, sw_if_index: u32, address: IpNet) -> InterfaceResult<bool> {
        let state = self.state_mut();
        let Some(index) = state
            .addresses
            .iter()
            .position(|(item, value)| *item == sw_if_index && *value == address)
        else {
            return Ok(false);
        };
        state.addresses.remove(index);
        Ok(true)
    }

    pub fn call_hw_interface_add_del(
        &self,
        hw_if_index: u32,
        is_create: bool,
    ) -> InterfaceResult<()> {
        let state = self.state();
        let hw =
            state
                .hardware_interfaces
                .get(hw_if_index)
                .ok_or(InterfaceError::NotRegistered {
                    interface_index: hw_if_index,
                })?;
        let hw_class = state
            .hw_classes
            .get(hw.hw_class_index as usize)
            .and_then(|class| class.interface_add_del_function);
        let device_class = state
            .device_classes
            .get(hw.dev_class_index as usize)
            .and_then(|class| class.interface_add_del_function);
        drop(state);
        if let Some(callback) = hw_class {
            callback(self, hw_if_index, is_create)?;
        }
        if let Some(callback) = device_class {
            callback(self, hw_if_index, is_create)?;
        }
        for registration in self.state().hw_callbacks.clone() {
            (registration.callback)(self, hw_if_index, is_create)?;
        }
        Ok(())
    }

    pub fn call_sw_interface_add_del(
        &self,
        sw_if_index: u32,
        is_create: bool,
    ) -> InterfaceResult<()> {
        self.call_callbacks(sw_if_index, is_create, self.state().sw_callbacks.clone())
    }
    pub fn call_sw_interface_admin_up_down(
        &self,
        sw_if_index: u32,
        is_up: bool,
    ) -> InterfaceResult<()> {
        self.call_sw_interface_add_del(sw_if_index, is_up)
    }
    pub fn call_sw_interface_mtu_change(
        &self,
        sw_if_index: u32,
        is_create: bool,
    ) -> InterfaceResult<()> {
        self.call_sw_interface_add_del(sw_if_index, is_create)
    }

    fn call_callbacks(
        &self,
        sw_if_index: u32,
        state: bool,
        callbacks: Vec<InterfaceCallbackRegistration>,
    ) -> InterfaceResult<()> {
        if self.software_interface(sw_if_index).is_none() {
            return Err(InterfaceError::NotRegistered {
                interface_index: sw_if_index,
            });
        }
        for registration in callbacks {
            (registration.callback)(self, sw_if_index, state)?;
        }
        Ok(())
    }
}

#[hammer_component_macros::init_function(name = "interface_main_init")]
pub fn interface_main_init(_: &mut GlobalMain) -> RuntimeResult<Arc<InterfaceMain>> {
    Ok(Arc::new(InterfaceMain::new()))
}
