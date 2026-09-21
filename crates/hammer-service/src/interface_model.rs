use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, OnceLock};

use hammer_core::data_plane::{NodeId, NodeKind, NodeRegistration};
use hammer_infra::bitmap::Bitmap;
use hammer_infra::pool::Pool;
use hammer_runtime::{
    DataPlaneMain, DataWorkerId, NodeDescriptor, NodeProcessFn, NodeRuntime, RuntimeResult,
};

use crate::ethernet::{EthernetInterface, EthernetInterfaceRegistration, EthernetMain};
use crate::interface::{InterfaceError, InterfaceMtu, InterfaceMtuKind, InterfaceResult};
use crate::net::NetMain;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DriverScheduleMode {
    Poll,
    Interrupt,
    Adaptive,
}

pub type InterfaceCallback =
    fn(&mut DataPlaneMain, &InterfaceMain, u32, bool) -> InterfaceResult<()>;
pub type FormatDeviceNameFn = for<'a, 'b> fn(u32, &'a mut fmt::Formatter<'b>) -> fmt::Result;
pub type BuildRewrite = fn(&InterfaceMain, u32, Option<u16>, Option<&[u8]>, &mut [u8]) -> usize;
pub type UpdateAdjacency = fn(&mut DataPlaneMain, u32, u32);

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct HwClassFlags: u32 {
        const P2P = 1 << 0;
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct HwInterfaceFlags: u32 {
        const LINK_UP = 1 << 0;
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct SwInterfaceFlags: u32 {
        const ADMIN_UP = 1 << 0;
    }
}

#[derive(Debug, Clone, Copy)]
pub struct InterfaceCallbackRegistration {
    pub callback: InterfaceCallback,
    pub priority: u8,
}

pub type HwInterfaceCallback = InterfaceCallbackRegistration;
pub type SwInterfaceCallback = InterfaceCallbackRegistration;

#[derive(Debug, Clone, Copy)]
pub struct DeviceClass {
    pub name: &'static str,
    pub index: u32,
    pub interface_add_del_function: Option<InterfaceCallback>,
    pub admin_up_down_function: Option<InterfaceCallback>,
    pub tx_function: Option<NodeProcessFn>,
    pub format_device_name: Option<FormatDeviceNameFn>,
    pub unformat_device_name: Option<fn(&str) -> Result<(), InterfaceError>>,
    pub subif_add_del_function: Option<fn()>,
    pub rx_mode_change_function: Option<fn()>,
    pub set_l2_mode_function: Option<fn()>,
    pub redistribute: Option<fn()>,
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
            index: 0,
            interface_add_del_function: None,
            admin_up_down_function: None,
            tx_function: None,
            format_device_name: None,
            unformat_device_name: None,
            subif_add_del_function: None,
            rx_mode_change_function: None,
            set_l2_mode_function: None,
            redistribute: None,
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

    pub const fn with_format_device_name(mut self, format: FormatDeviceNameFn) -> Self {
        self.format_device_name = Some(format);
        self
    }

    pub const fn with_tx_function(mut self, function: NodeProcessFn) -> Self {
        self.tx_function = Some(function);
        self
    }

    pub const fn index(&self) -> u32 {
        self.index
    }
}

#[derive(Debug, Clone, Copy)]
pub struct HwClass {
    pub name: &'static str,
    pub index: u32,
    pub flags: HwClassFlags,
    pub tx_hash_fn_type: u8,
    pub interface_add_del_function: Option<InterfaceCallback>,
    pub admin_up_down_function: Option<InterfaceCallback>,
    pub link_up_down_function: Option<InterfaceCallback>,
    pub mac_addr_change_function: Option<fn()>,
    pub mac_addr_add_del_function: Option<fn()>,
    pub set_max_frame_size: Option<fn()>,
    pub format_interface_name: Option<fn()>,
    pub format_address: Option<fn()>,
    pub format_header: Option<fn()>,
    pub format_device: Option<fn()>,
    pub unformat_hw_address: Option<fn(&str) -> Result<(), InterfaceError>>,
    pub unformat_header: Option<fn()>,
    pub build_rewrite: Option<BuildRewrite>,
    pub update_adjacency: Option<UpdateAdjacency>,
    pub is_valid_class_for_interface: Option<fn() -> bool>,
    pub hw_class_change: Option<fn()>,
}

impl HwClass {
    pub const fn new(name: &'static str) -> Self {
        Self {
            name,
            index: 0,
            flags: HwClassFlags::empty(),
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

    pub const fn with_flags(mut self, flags: HwClassFlags) -> Self {
        self.flags = flags;
        self
    }

    pub const fn index(&self) -> u32 {
        self.index
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct InterfaceRegistrationImage {
    pub device_class_registrations: &'static linkme::DistributedSlice<[DeviceClass]>,
    pub hw_interface_class_registrations: &'static linkme::DistributedSlice<[HwClass]>,
    pub hw_interface_callbacks: &'static [InterfaceCallbackRegistration],
    pub sw_interface_callbacks: &'static [InterfaceCallbackRegistration],
    pub admin_up_down_callbacks: &'static [InterfaceCallbackRegistration],
}

impl InterfaceRegistrationImage {
    pub const fn new(
        device_class_registrations: &'static linkme::DistributedSlice<[DeviceClass]>,
        hw_interface_class_registrations: &'static linkme::DistributedSlice<[HwClass]>,
        hw_interface_callbacks: &'static [InterfaceCallbackRegistration],
        sw_interface_callbacks: &'static [InterfaceCallbackRegistration],
        admin_up_down_callbacks: &'static [InterfaceCallbackRegistration],
    ) -> Self {
        Self {
            device_class_registrations,
            hw_interface_class_registrations,
            hw_interface_callbacks,
            sw_interface_callbacks,
            admin_up_down_callbacks,
        }
    }
}

#[derive(Debug, Clone)]
pub struct HwInterface {
    pub flags: HwInterfaceFlags,
    pub caps: u32,
    pub hw_address: Vec<u8>,
    pub output_node_index: Option<NodeId>,
    pub tx_node_index: Option<NodeId>,
    pub output_node_next_index: Option<u16>,
    pub if_out_arc_end_node_next_index: Option<u16>,
    pub dev_class_index: u32,
    pub dev_instance: u32,
    pub hw_class_index: u32,
    pub hw_instance: u32,
    pub hw_if_index: u32,
    pub sw_if_index: u32,
    pub name: String,
    pub link_speed: u64,
    pub supported_link_speeds: Vec<u64>,
    pub min_frame_size: u16,
    pub frame_overhead: u16,
    pub max_frame_size: u16,
    pub input_node_index: NodeId,
    pub default_rx_mode: DriverScheduleMode,
    pub rx_queue_indices: Vec<u32>,
    pub tx_queue_indices: Vec<u32>,
    pub numa_node: u32,
}

pub const INVALID_HW_IF_INDEX: u32 = u32::MAX;
pub const INVALID_NODE_NEXT_INDEX: u16 = u16::MAX;

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct InterfaceLookupEntry {
    pub(crate) hw_if_index: u32,
    pub(crate) if_out_arc_end_next_index: u16,
}

impl InterfaceLookupEntry {
    pub const ABSENT: Self = Self {
        hw_if_index: INVALID_HW_IF_INDEX,
        if_out_arc_end_next_index: INVALID_NODE_NEXT_INDEX,
    };

    pub const fn has_hardware(self) -> bool {
        self.hw_if_index != INVALID_HW_IF_INDEX
    }

    pub const fn has_arc_end(self) -> bool {
        self.if_out_arc_end_next_index != INVALID_NODE_NEXT_INDEX
    }
}

pub(crate) struct InterfaceLookupTable {
    entries: Vec<InterfaceLookupEntry>,
}

impl InterfaceLookupTable {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    pub fn publish(&mut self, sw_if_index: u32, hw_if_index: u32, if_out_arc_end_next_index: u16) {
        let position = sw_if_index as usize;
        if position >= self.entries.len() {
            self.entries
                .resize(position + 1, InterfaceLookupEntry::ABSENT);
        }
        self.entries[position] = InterfaceLookupEntry {
            hw_if_index,
            if_out_arc_end_next_index,
        };
    }

    pub fn clear(&mut self, sw_if_index: u32) {
        let position = sw_if_index as usize;
        if position >= self.entries.len() {
            self.entries
                .resize(position + 1, InterfaceLookupEntry::ABSENT);
        }
        self.entries[position] = InterfaceLookupEntry::ABSENT;
    }

    pub fn entry(&self, sw_if_index: u32) -> InterfaceLookupEntry {
        self.entries
            .get(sw_if_index as usize)
            .copied()
            .unwrap_or(InterfaceLookupEntry::ABSENT)
    }
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
    pub flags: SwInterfaceFlags,
    pub sw_if_index: u32,
    pub sup_sw_if_index: u32,
    pub unnumbered_sw_if_index: Option<u32>,
    pub hw_if_index: Option<u32>,
    pub mtu: InterfaceMtu,
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
        self.flags.contains(SwInterfaceFlags::ADMIN_UP)
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
    device_class_by_name: HashMap<&'static str, u32>,
    hw_class_by_name: HashMap<&'static str, u32>,
    device_classes: Vec<DeviceClass>,
    hw_classes: Vec<HwClass>,
    hw_callbacks: Vec<InterfaceCallbackRegistration>,
    sw_callbacks: Vec<InterfaceCallbackRegistration>,
    admin_up_down_callbacks: Vec<InterfaceCallbackRegistration>,
    mtu_callbacks: Vec<fn(&mut DataPlaneMain, &InterfaceMain, u32)>,
    lookup_table: InterfaceLookupTable,
    output_feature_arc_index: u8,
    deleted_node_pairs: Vec<(u32, NodeId, NodeId)>,
}

impl Default for InterfaceLookupTable {
    fn default() -> Self {
        Self::new()
    }
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
        let mut state = InterfaceState::default();
        state.output_feature_arc_index = u8::MAX;
        let interfaces = Self {
            state: UnsafeCell::new(state),
        };
        interfaces
            .consume_class_registrations(
                SERVICE_INTERFACE_REGISTRATION_IMAGE.device_class_registrations,
                SERVICE_INTERFACE_REGISTRATION_IMAGE.hw_interface_class_registrations,
            )
            .expect("built-in interface classes fit the class index space");
        interfaces
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
        self.consume_class_registrations(
            image.device_class_registrations,
            image.hw_interface_class_registrations,
        )?;
        self.consume_callback_registrations(
            image.hw_interface_callbacks,
            image.sw_interface_callbacks,
            image.admin_up_down_callbacks,
        );
        Ok(())
    }

    fn consume_class_registrations(
        &self,
        device_classes: &'static linkme::DistributedSlice<[DeviceClass]>,
        hw_classes: &'static linkme::DistributedSlice<[HwClass]>,
    ) -> RuntimeResult<()> {
        let state = self.state_mut();
        for class in device_classes.iter().rev() {
            let mut class = *class;
            class.index = u32::try_from(state.device_classes.len()).map_err(|_| {
                InterfaceError::IndexSpaceExhausted {
                    interface_count: state.device_classes.len(),
                }
            })?;
            state.device_class_by_name.insert(class.name, class.index);
            state.device_classes.push(class);
        }
        for class in hw_classes.iter().rev() {
            let mut class = *class;
            class.index = u32::try_from(state.hw_classes.len()).map_err(|_| {
                InterfaceError::IndexSpaceExhausted {
                    interface_count: state.hw_classes.len(),
                }
            })?;
            state.hw_class_by_name.insert(class.name, class.index);
            state.hw_classes.push(class);
        }
        Ok(())
    }

    fn consume_callback_registrations(
        &self,
        hw_callbacks: &'static [InterfaceCallbackRegistration],
        sw_callbacks: &'static [InterfaceCallbackRegistration],
        admin_up_down_callbacks: &'static [InterfaceCallbackRegistration],
    ) {
        let state = self.state_mut();
        state.hw_callbacks.extend_from_slice(hw_callbacks);
        state.sw_callbacks.extend_from_slice(sw_callbacks);
        state
            .admin_up_down_callbacks
            .extend_from_slice(admin_up_down_callbacks);
        state
            .hw_callbacks
            .sort_by_key(|registration| registration.priority);
        state
            .sw_callbacks
            .sort_by_key(|registration| registration.priority);
        state
            .admin_up_down_callbacks
            .sort_by_key(|registration| registration.priority);
    }

    pub fn device_class_index(&self, name: &str) -> u32 {
        *self
            .state()
            .device_class_by_name
            .get(name)
            .unwrap_or_else(|| panic!("device class `{name}` is not installed"))
    }

    pub fn hw_class_index(&self, name: &str) -> u32 {
        *self
            .state()
            .hw_class_by_name
            .get(name)
            .unwrap_or_else(|| panic!("hardware interface class `{name}` is not installed"))
    }

    pub fn register_interface(
        &self,
        main: &mut DataPlaneMain,
        dev_class_index: u32,
        dev_instance: u32,
        hw_class_index: u32,
        hw_instance: u32,
    ) -> u32 {
        hammer_runtime::ensure_main_thread_with_barrier()
            .expect("interface registration requires publication ownership");
        let state = self.state_mut();
        let device_class = *state
            .device_classes
            .get(dev_class_index as usize)
            .expect("device class index names an installed class");
        let hw_class = *state
            .hw_classes
            .get(hw_class_index as usize)
            .expect("hardware interface class index names an installed class");
        let name = match device_class.format_device_name {
            Some(format) => {
                struct DeviceName {
                    format: FormatDeviceNameFn,
                    instance: u32,
                }

                impl fmt::Display for DeviceName {
                    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                        (self.format)(self.instance, formatter)
                    }
                }

                DeviceName {
                    format,
                    instance: dev_instance,
                }
                .to_string()
            }
            None => format!("{}{dev_instance:x}", hw_class.name),
        };
        let hw_if_index = state.hardware_interfaces.insert(HwInterface {
            flags: HwInterfaceFlags::empty(),
            caps: 0,
            hw_address: Vec::new(),
            output_node_index: None,
            tx_node_index: None,
            output_node_next_index: None,
            if_out_arc_end_node_next_index: None,
            dev_class_index,
            dev_instance,
            hw_class_index,
            hw_instance,
            hw_if_index: 0,
            sw_if_index: 0,
            name: name.clone(),
            link_speed: 0,
            supported_link_speeds: Vec::new(),
            min_frame_size: 0,
            frame_overhead: 0,
            max_frame_size: 0,
            input_node_index: NodeId::new(0),
            default_rx_mode: DriverScheduleMode::Interrupt,
            rx_queue_indices: Vec::new(),
            tx_queue_indices: Vec::new(),
            numa_node: 0,
        });
        let sw_if_index = state.software_interfaces.insert(SwInterface {
            interface_type: 0,
            flags: SwInterfaceFlags::empty(),
            sw_if_index: 0,
            sup_sw_if_index: 0,
            unnumbered_sw_if_index: None,
            hw_if_index: Some(hw_if_index),
            mtu: InterfaceMtu::default(),
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
        state
            .software_interfaces
            .get_mut(sw_if_index)
            .expect("inserted software interface")
            .sup_sw_if_index = sw_if_index;
        state.names.insert(name, hw_if_index);
        self.if_update_lookup_tables(sw_if_index);

        if let Some(tx_function) = device_class.tx_function {
            let output_name = Box::leak(
                format!("{0}-output", self.hardware_interface(hw_if_index).name).into_boxed_str(),
            );
            let tx_name = Box::leak(
                format!("{0}-tx", self.hardware_interface(hw_if_index).name).into_boxed_str(),
            );
            let runtime_data = NodeRuntime::from_words([
                u64::from(hw_if_index),
                u64::from(sw_if_index),
                u64::from(dev_instance),
                0,
            ]);
            let recycled = self
                .state_mut()
                .deleted_node_pairs
                .iter()
                .position(|(class, _, _)| *class == dev_class_index)
                .map(|position| self.state_mut().deleted_node_pairs.swap_remove(position));
            let (output_node, tx_node) = if let Some((_, output_node, tx_node)) = recycled {
                main.nodes()
                    .recycle_node_descriptor(
                        tx_node,
                        NodeDescriptor::new(
                            tx_function,
                            runtime_data,
                            Some(NodeRegistration::next(tx_name, 1)),
                            &[],
                            None,
                        ),
                    )
                    .expect("interface TX node recycle must succeed");
                main.nodes()
                    .recycle_node_descriptor(
                        output_node,
                        NodeDescriptor::new(
                            crate::interface::interface_output_template,
                            runtime_data,
                            Some(NodeRegistration::next(output_name, 2)),
                            &[],
                            None,
                        ),
                    )
                    .expect("interface output node recycle must succeed");
                (output_node, tx_node)
            } else {
                let tx_node = main
                    .nodes()
                    .try_register_descriptor(
                        NodeKind::Internal,
                        NodeDescriptor::new(
                            tx_function,
                            runtime_data,
                            Some(NodeRegistration::next(tx_name, 1)),
                            &[],
                            None,
                        ),
                    )
                    .expect("interface TX node registration must succeed");
                let output_node = main
                    .nodes()
                    .try_register_descriptor(
                        NodeKind::Internal,
                        NodeDescriptor::new(
                            crate::interface::interface_output_template,
                            runtime_data,
                            Some(NodeRegistration::next(output_name, 2)),
                            &[],
                            None,
                        ),
                    )
                    .expect("interface output node registration must succeed");
                main.register_node_errors(
                    output_node,
                    &crate::interface::INTERFACE_OUTPUT_ERROR_DESCRIPTORS,
                )
                .expect("interface output errors must register");
                (output_node, tx_node)
            };
            let hardware = self
                .state_mut()
                .hardware_interfaces
                .get_mut(hw_if_index)
                .expect("registered hardware interface remains live");
            hardware.output_node_index = Some(output_node);
            hardware.tx_node_index = Some(tx_node);

            if self.state().output_feature_arc_index != u8::MAX {
                let public_output = main
                    .nodes()
                    .node_by_name("interface-output")
                    .expect("public interface output is materialized");
                let arc_end = main
                    .nodes()
                    .node_by_name("interface-output-arc-end")
                    .expect("interface output arc end is materialized");
                let drop_node = main
                    .nodes()
                    .node_by_name("drop")
                    .expect("drop is materialized");
                self.complete_output_graph(
                    main,
                    self.state().output_feature_arc_index,
                    public_output,
                    arc_end,
                    drop_node,
                );
            }
        }

        drop(self.call_sw_interface_add_del(main, sw_if_index, true));
        drop(self.call_hw_interface_add_del(main, hw_if_index, true));
        hw_if_index
    }

    pub fn set_hardware_flags(
        &self,
        main: &mut DataPlaneMain,
        hw_if_index: u32,
        flags: HwInterfaceFlags,
    ) -> InterfaceResult<()> {
        hammer_runtime::ensure_main_thread_with_barrier()?;
        let state = self.state();
        let hardware =
            state
                .hardware_interfaces
                .get(hw_if_index)
                .ok_or(InterfaceError::NotRegistered {
                    interface_index: hw_if_index,
                })?;
        let old_flags = hardware.flags;
        if old_flags == flags {
            return Ok(());
        }
        let callback = state
            .hw_classes
            .get(hardware.hw_class_index as usize)
            .expect("hardware interface names an installed class")
            .link_up_down_function;
        if let Some(callback) = callback {
            callback(
                main,
                self,
                hw_if_index,
                flags.contains(HwInterfaceFlags::LINK_UP),
            )?;
        }
        self.state_mut()
            .hardware_interfaces
            .get_mut(hw_if_index)
            .expect("validated hardware interface remains occupied")
            .flags = flags;
        Ok(())
    }

    pub fn set_software_flags(
        &self,
        main: &mut DataPlaneMain,
        sw_if_index: u32,
        flags: SwInterfaceFlags,
    ) -> InterfaceResult<()> {
        hammer_runtime::ensure_main_thread_with_barrier()?;
        let state = self.state();
        let software =
            state
                .software_interfaces
                .get(sw_if_index)
                .ok_or(InterfaceError::NotRegistered {
                    interface_index: sw_if_index,
                })?;
        let old_flags = software.flags;
        if old_flags == flags {
            return Ok(());
        }
        let primary = state
            .software_interfaces
            .get(software.sup_sw_if_index)
            .expect("software interface names an occupied super-interface");
        let hw_if_index = software
            .hw_if_index
            .or(primary.hw_if_index)
            .expect("hardware software-interface has a hardware owner");
        let hardware = state
            .hardware_interfaces
            .get(hw_if_index)
            .expect("software interface names an occupied hardware owner");
        let device_callback = state
            .device_classes
            .get(hardware.dev_class_index as usize)
            .expect("hardware interface names an installed device class")
            .admin_up_down_function;
        let hw_callback = state
            .hw_classes
            .get(hardware.hw_class_index as usize)
            .expect("hardware interface names an installed hardware class")
            .admin_up_down_function;
        let admin_callbacks = state.admin_up_down_callbacks.clone();
        let is_up = flags.contains(SwInterfaceFlags::ADMIN_UP);
        self.state_mut()
            .software_interfaces
            .get_mut(sw_if_index)
            .expect("validated software interface remains occupied")
            .flags = flags;
        if (flags | old_flags).contains(SwInterfaceFlags::ADMIN_UP) {
            for registration in admin_callbacks {
                if let Err(error) = (registration.callback)(main, self, sw_if_index, is_up) {
                    self.state_mut()
                        .software_interfaces
                        .get_mut(sw_if_index)
                        .expect("rejected software flag change retains its slot")
                        .flags = old_flags;
                    return Err(error);
                }
            }
        }
        if let Some(callback) = device_callback {
            if let Err(error) = callback(main, self, hw_if_index, is_up) {
                self.state_mut()
                    .software_interfaces
                    .get_mut(sw_if_index)
                    .expect("rejected software flag change retains its slot")
                    .flags = old_flags;
                return Err(error);
            }
        }
        if let Some(callback) = hw_callback {
            if let Err(error) = callback(main, self, hw_if_index, is_up) {
                self.state_mut()
                    .software_interfaces
                    .get_mut(sw_if_index)
                    .expect("rejected software flag change retains its slot")
                    .flags = old_flags;
                return Err(error);
            }
        }
        Ok(())
    }

    pub fn delete_hardware_interface(&self, main: &mut DataPlaneMain, hw_if_index: u32) {
        hammer_runtime::ensure_main_thread_with_barrier()
            .expect("hardware interface deletion requires the main thread barrier");
        let hw = self
            .state()
            .hardware_interfaces
            .get(hw_if_index)
            .expect("hardware interface deletion requires a live interface")
            .clone();
        drop(self.set_hardware_flags(main, hw_if_index, HwInterfaceFlags::empty()));
        drop(self.call_hw_interface_add_del(main, hw_if_index, false));
        self.remove_interface_queues(hw_if_index);
        drop(self.set_software_flags(main, hw.sw_if_index, SwInterfaceFlags::empty()));
        drop(self.call_sw_interface_add_del(main, hw.sw_if_index, false));
        self.remove_interface_slots(main, hw_if_index);
    }

    fn remove_interface_queues(&self, hw_if_index: u32) {
        let state = self.state_mut();
        let (rx_queue_indices, tx_queue_indices) = {
            let hw = state
                .hardware_interfaces
                .get_mut(hw_if_index)
                .expect("interface queue removal retains its hardware slot");
            (
                std::mem::take(&mut hw.rx_queue_indices),
                std::mem::take(&mut hw.tx_queue_indices),
            )
        };
        for index in rx_queue_indices {
            state.rx_queues.remove(index);
        }
        for index in tx_queue_indices {
            state.tx_queues.remove(index);
        }
    }

    fn remove_interface_slots(&self, main: &mut DataPlaneMain, hw_if_index: u32) {
        let state = self.state_mut();
        let hw = state
            .hardware_interfaces
            .get(hw_if_index)
            .expect("interface removal retains its hardware slot")
            .clone();
        if let (Some(output), Some(tx)) = (hw.output_node_index, hw.tx_node_index) {
            let tx_function = state.device_classes[hw.dev_class_index as usize]
                .tx_function
                .expect("interface with a TX node retains its DeviceClass process");
            let runtime_data = NodeRuntime::from_words([
                u64::from(hw.hw_if_index),
                u64::from(hw.sw_if_index),
                u64::from(hw.dev_instance),
                1,
            ]);
            let output_name =
                Box::leak(format!("interface-{}-output-deleted", hw.hw_if_index).into_boxed_str());
            let tx_name =
                Box::leak(format!("interface-{}-tx-deleted", hw.hw_if_index).into_boxed_str());
            main.nodes()
                .recycle_node_descriptor(
                    output,
                    NodeDescriptor::new(
                        crate::interface::interface_output_template,
                        runtime_data,
                        Some(NodeRegistration::next(output_name, 2)),
                        &[],
                        None,
                    ),
                )
                .expect("deleted interface output runtime must publish");
            main.nodes()
                .recycle_node_descriptor(
                    tx,
                    NodeDescriptor::new(
                        tx_function,
                        runtime_data,
                        Some(NodeRegistration::next(tx_name, 1)),
                        &[],
                        None,
                    ),
                )
                .expect("deleted interface TX runtime must publish");
        }
        for index in hw.rx_queue_indices {
            state.rx_queues.remove(index);
        }
        for index in hw.tx_queue_indices {
            state.tx_queues.remove(index);
        }
        state.lookup_table.clear(hw.sw_if_index);
        if let (Some(output), Some(tx)) = (hw.output_node_index, hw.tx_node_index) {
            state
                .deleted_node_pairs
                .push((hw.dev_class_index, output, tx));
        }
        state.software_interfaces.remove(hw.sw_if_index);
        state.names.retain(|_, index| *index != hw_if_index);
        state
            .hardware_interfaces
            .remove(hw_if_index)
            .expect("interface removal releases its hardware slot");
    }

    pub fn hardware_interface(&self, index: u32) -> &HwInterface {
        self.state()
            .hardware_interfaces
            .get(index)
            .expect("hardware interface index names an occupied slot")
    }

    pub fn software_interface(&self, index: u32) -> Option<&SwInterface> {
        self.state().software_interfaces.get(index)
    }

    pub fn tx_node_index_for_sw_interface(&self, sw_if_index: u32) -> NodeId {
        let software = self
            .software_interface(sw_if_index)
            .expect("TX interface must be live");
        let super_interface = self
            .software_interface(software.sup_sw_if_index)
            .expect("TX interface must name a live super-interface");
        let hw_if_index = software
            .hw_if_index
            .or(super_interface.hw_if_index)
            .expect("TX interface must resolve to hardware");
        self.hardware_interface(hw_if_index)
            .output_node_index
            .expect("TX interface must have a published output node")
    }

    fn if_update_lookup_tables(&self, sw_if_index: u32) {
        let state = self.state_mut();
        let software = state
            .software_interfaces
            .get(sw_if_index)
            .expect("lookup publication requires a live software interface");
        let super_interface = state
            .software_interfaces
            .get(software.sup_sw_if_index)
            .expect("lookup publication requires a live super-interface");
        let hw_if_index = software
            .hw_if_index
            .or(super_interface.hw_if_index)
            .expect("lookup publication requires a hardware interface");
        let arc_end = state
            .hardware_interfaces
            .get(hw_if_index)
            .expect("lookup publication requires live hardware")
            .if_out_arc_end_node_next_index
            .unwrap_or(INVALID_NODE_NEXT_INDEX);
        state
            .lookup_table
            .publish(sw_if_index, hw_if_index, arc_end);
    }

    pub(crate) fn interface_lookup_entry(&self, sw_if_index: u32) -> InterfaceLookupEntry {
        self.state().lookup_table.entry(sw_if_index)
    }

    pub(crate) fn output_feature_arc_index(&self) -> u8 {
        let index = self.state().output_feature_arc_index;
        assert_ne!(index, u8::MAX, "interface-output Feature Arc is published");
        index
    }

    pub(crate) fn output_node_next_index_for_sw_interface(&self, sw_if_index: u32) -> u16 {
        let software = self
            .software_interface(sw_if_index)
            .expect("output packet requires a live software interface");
        let super_interface = self
            .software_interface(software.sup_sw_if_index)
            .expect("output packet requires a live super-interface");
        let hw_if_index = software
            .hw_if_index
            .or(super_interface.hw_if_index)
            .expect("output packet requires a hardware interface");
        self.hardware_interface(hw_if_index)
            .output_node_next_index
            .expect("output packet requires a published interface-output edge")
    }

    pub(crate) fn complete_output_graph(
        &self,
        main: &mut DataPlaneMain,
        arc_index: u8,
        public_output: NodeId,
        arc_end: NodeId,
        drop_node: NodeId,
    ) {
        hammer_runtime::ensure_main_thread_with_barrier()
            .expect("interface output graph publication requires main-thread ownership");
        self.state_mut().output_feature_arc_index = arc_index;
        let interfaces = self
            .state()
            .hardware_interfaces
            .iter()
            .filter_map(|(hw_if_index, hardware)| {
                Some((
                    hw_if_index,
                    hardware.sw_if_index,
                    hardware.output_node_index?,
                    hardware.tx_node_index?,
                ))
            })
            .collect::<Vec<_>>();
        let features = crate::feature::FeatureMain::global()
            .expect("FeatureMain exists before interface output publication");
        let drop_slot = main
            .nodes()
            .add_node_next_slot(arc_end, drop_node)
            .expect("interface arc-end drop edge must publish");
        assert_eq!(drop_slot, 0, "interface arc-end drop slot is zero");
        for (hw_if_index, sw_if_index, output, tx) in interfaces {
            main.nodes()
                .set_node_next_slot(tx, 0, drop_node)
                .expect("interface TX drop edge must publish");
            main.nodes()
                .set_node_next_slot(output, 0, drop_node)
                .expect("interface output drop edge must publish");
            main.nodes()
                .set_node_next_slot(output, 1, tx)
                .expect("interface output TX edge must publish");
            features
                .add_feature_arc_start(main.nodes(), arc_index, output)
                .expect("interface output start must publish");
            let arc_end_next = main
                .nodes()
                .add_node_next_slot(arc_end, tx)
                .expect("interface arc-end TX edge must publish");
            let output_next = main
                .nodes()
                .add_node_next_slot(public_output, output)
                .expect("public interface-output edge must publish");
            let hardware = self
                .state_mut()
                .hardware_interfaces
                .get_mut(hw_if_index)
                .expect("interface remains live during graph publication");
            hardware.output_node_next_index = Some(output_next);
            hardware.if_out_arc_end_node_next_index = Some(arc_end_next);
            self.if_update_lookup_tables(sw_if_index);
        }
    }

    fn hw_class_for_software(&self, sw_if_index: u32) -> HwClass {
        let state = self.state();
        let software = state
            .software_interfaces
            .get(sw_if_index)
            .expect("hardware class query requires a live interface");
        let primary = state
            .software_interfaces
            .get(software.sup_sw_if_index)
            .expect("software interface names a live super-interface");
        let hardware = state
            .hardware_interfaces
            .get(
                software
                    .hw_if_index
                    .or(primary.hw_if_index)
                    .expect("hardware interface required"),
            )
            .expect("software interface names a live hardware interface");
        state.hw_classes[hardware.hw_class_index as usize]
    }

    pub fn is_p2p(&self, sw_if_index: u32) -> bool {
        self.hw_class_for_software(sw_if_index)
            .flags
            .contains(HwClassFlags::P2P)
    }

    pub fn build_rewrite(
        &self,
        sw_if_index: u32,
        ethernet_type: Option<u16>,
        destination: Option<&[u8]>,
        output: &mut [u8],
    ) -> usize {
        let class = self.hw_class_for_software(sw_if_index);
        let written = class.build_rewrite.map_or(0, |build| {
            build(self, sw_if_index, ethernet_type, destination, output)
        });
        assert!(
            written <= output.len(),
            "interface class exceeded rewrite capacity"
        );
        written
    }

    pub fn software_interface_indices(&self) -> Vec<u32> {
        hammer_runtime::ensure_main_thread_with_barrier()
            .expect("software interface enumeration requires the publication scope");
        self.state()
            .software_interfaces
            .iter()
            .map(|(index, _)| index)
            .collect()
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
        if name.is_empty() {
            return Err(InterfaceError::NameEmpty);
        }
        state.names.retain(|_, index| *index != hw_if_index);
        state
            .hardware_interfaces
            .get_mut(hw_if_index)
            .ok_or(InterfaceError::NotRegistered {
                interface_index: hw_if_index,
            })?
            .name = name.clone();
        state.names.insert(name, hw_if_index);
        Ok(())
    }
    pub fn interface_name(&self, index: u32) -> Option<String> {
        self.state()
            .hardware_interfaces
            .get(index)
            .map(|hw| hw.name.clone())
    }
    pub fn interface_mtu(&self, index: u32) -> Option<InterfaceMtu> {
        self.state()
            .hardware_interfaces
            .get(index)
            .and_then(|hw| self.software_interface(hw.sw_if_index))
            .map(|sw| sw.mtu)
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

    pub(crate) fn has_tx_queue_for_worker(&self, worker: DataWorkerId, hw_if_index: u32) -> bool {
        self.state()
            .tx_queues
            .iter()
            .any(|(_, queue)| queue.hw_if_index == hw_if_index && queue.is_assigned_to(worker))
    }

    pub fn register_mtu_callback(&self, callback: fn(&mut DataPlaneMain, &InterfaceMain, u32)) {
        hammer_runtime::ensure_main_thread()
            .expect("interface MTU callback registration is main-thread-only");
        assert!(
            !hammer_runtime::barrier::global().is_some_and(|barrier| barrier.worker_count() != 0),
            "interface MTU callbacks register before Data Worker startup"
        );
        self.state_mut().mtu_callbacks.push(callback);
    }

    pub fn set_mtu(
        &self,
        main: &mut DataPlaneMain,
        sw_if_index: u32,
        mtu: InterfaceMtu,
    ) -> InterfaceResult<()> {
        hammer_runtime::ensure_main_thread_with_barrier()?;
        let interface = self
            .state_mut()
            .software_interfaces
            .get_mut(sw_if_index)
            .ok_or(InterfaceError::NotRegistered {
                interface_index: sw_if_index,
            })?;
        if interface.mtu != mtu {
            interface.mtu = mtu;
            for callback in self.state().mtu_callbacks.clone() {
                callback(main, self, sw_if_index);
            }
        }
        Ok(())
    }
    pub fn set_protocol_mtu(
        &self,
        main: &mut DataPlaneMain,
        sw_if_index: u32,
        kind: InterfaceMtuKind,
        value: u32,
    ) -> InterfaceResult<()> {
        hammer_runtime::ensure_main_thread_with_barrier()?;
        let state = self.state_mut();
        let sw = state.software_interfaces.get_mut(sw_if_index).ok_or(
            InterfaceError::NotRegistered {
                interface_index: sw_if_index,
            },
        )?;
        if sw.mtu.get(kind) != value {
            sw.mtu.set(kind, value);
            for callback in self.state().mtu_callbacks.clone() {
                callback(main, self, sw_if_index);
            }
        }
        Ok(())
    }
    pub fn call_hw_interface_add_del(
        &self,
        main: &mut DataPlaneMain,
        hw_if_index: u32,
        is_create: bool,
    ) -> InterfaceResult<()> {
        hammer_runtime::ensure_main_thread_with_barrier()?;
        let (hw_class, device_class, callbacks) = {
            let state = self.state();
            let hw = state.hardware_interfaces.get(hw_if_index).ok_or(
                InterfaceError::NotRegistered {
                    interface_index: hw_if_index,
                },
            )?;
            (
                state
                    .hw_classes
                    .get(hw.hw_class_index as usize)
                    .and_then(|class| class.interface_add_del_function),
                state
                    .device_classes
                    .get(hw.dev_class_index as usize)
                    .and_then(|class| class.interface_add_del_function),
                state.hw_callbacks.clone(),
            )
        };
        if let Some(callback) = hw_class {
            callback(main, self, hw_if_index, is_create)?;
        }
        if let Some(callback) = device_class {
            callback(main, self, hw_if_index, is_create)?;
        }
        for registration in callbacks {
            (registration.callback)(main, self, hw_if_index, is_create)?;
        }
        Ok(())
    }

    pub fn call_sw_interface_add_del(
        &self,
        main: &mut DataPlaneMain,
        sw_if_index: u32,
        is_create: bool,
    ) -> InterfaceResult<()> {
        hammer_runtime::ensure_main_thread_with_barrier()?;
        self.call_callbacks(
            main,
            sw_if_index,
            is_create,
            self.state().sw_callbacks.clone(),
        )
    }
    fn call_callbacks(
        &self,
        main: &mut DataPlaneMain,
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
            (registration.callback)(main, self, sw_if_index, state)?;
        }
        Ok(())
    }
}

impl EthernetMain {
    pub fn eth_register_interface(
        &self,
        main: &mut DataPlaneMain,
        registration: EthernetInterfaceRegistration,
    ) -> u32 {
        hammer_runtime::ensure_main_thread_with_barrier()
            .expect("Ethernet interface registration requires publication ownership");
        // SAFETY: Ethernet registration is serialized by the startup/main-thread
        // publication scope and no pool borrow escapes this operation.
        let ethernet_index = unsafe {
            (&mut *self.interfaces.get()).insert(EthernetInterface {
                flag_change: registration.flag_change,
                set_max_frame_size: registration.set_max_frame_size,
                flags: 0,
                address: registration.address,
            })
        };
        let interfaces = NetMain::global()
            .expect("Ethernet registration requires the network Main")
            .interface_main();
        let hw_if_index = interfaces.register_interface(
            main,
            registration.dev_class_index,
            registration.dev_instance,
            interfaces.hw_class_index("ethernet"),
            ethernet_index,
        );
        let overhead = if registration.frame_overhead == 0 {
            22
        } else {
            registration.frame_overhead
        };
        let max_frame_size = if registration.max_frame_size == 0 {
            9_000u16
                .checked_add(overhead)
                .expect("default Ethernet frame size fits u16")
        } else {
            registration.max_frame_size
        };
        let state = interfaces.state_mut();
        let hardware = state
            .hardware_interfaces
            .get_mut(hw_if_index)
            .expect("new Ethernet interface remains live during registration");
        hardware.min_frame_size = 64;
        hardware.frame_overhead = overhead;
        hardware.max_frame_size = max_frame_size;
        hardware.hw_address.clear();
        hardware.hw_address.extend_from_slice(&registration.address);
        let sw_if_index = hardware.sw_if_index;
        interfaces
            .set_protocol_mtu(main, sw_if_index, InterfaceMtuKind::L3, 9_000)
            .expect("new Ethernet interface remains live during MTU publication");
        hw_if_index
    }
}

pub static INTERFACE_MAIN: OnceLock<Arc<InterfaceMain>> = OnceLock::new();

#[derive(hammer_component_macros::DeviceClass)]
#[device_class(name = "local")]
pub struct LocalDeviceClass;

#[derive(hammer_component_macros::HwClass)]
#[hw_class(name = "local")]
pub struct LocalHwClass;

pub(crate) static SERVICE_INTERFACE_REGISTRATION_IMAGE: InterfaceRegistrationImage =
    InterfaceRegistrationImage::new(
        &crate::__HAMMER_DEVICE_CLASS_REGISTRATIONS,
        &crate::__HAMMER_HW_CLASS_REGISTRATIONS,
        &[],
        &crate::feature::FEATURE_SW_INTERFACE_CALLBACKS,
        &[],
    );

#[hammer_component_macros::init_function(name = "interface_main_init")]
pub fn interface_main_init() -> RuntimeResult<()> {
    let interfaces = Arc::new(InterfaceMain::new());
    interfaces.consume_callback_registrations(
        SERVICE_INTERFACE_REGISTRATION_IMAGE.hw_interface_callbacks,
        SERVICE_INTERFACE_REGISTRATION_IMAGE.sw_interface_callbacks,
        SERVICE_INTERFACE_REGISTRATION_IMAGE.admin_up_down_callbacks,
    );
    match hammer_runtime::PluginMain::global() {
        Ok(plugins) => {
            for plugin in plugins.loaded_plugins() {
                let image = match plugins.get_plugin_symbol::<InterfaceRegistrationImage>(
                    &plugin,
                    "HAMMER_INTERFACE_REGISTRATION_IMAGE",
                ) {
                    Ok(image) => image,
                    Err(hammer_runtime::PluginError::SymbolLookup { .. }) => continue,
                    Err(error) => return Err(error.into()),
                };
                // SAFETY: the service-owned export has this concrete type and
                // PluginMain keeps its defining DSO mapped for process life.
                interfaces.consume_registration_image(unsafe { &*image })?;
            }
        }
        Err(hammer_runtime::PluginError::MainUnavailable) => {}
        Err(error) => return Err(error.into()),
    }
    assert!(
        INTERFACE_MAIN.set(interfaces).is_ok(),
        "interface initialization callback executes once"
    );
    Ok(())
}
