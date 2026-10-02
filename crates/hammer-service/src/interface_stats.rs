//! Interface-name projection into the stats segment directory.

use std::cell::UnsafeCell;
use std::sync::OnceLock;

use hammer_runtime::{
    DataPlaneMain, DirectoryIndex, DirectoryType, NameVector, RuntimeResult, StatsMain,
};

use crate::interface::InterfaceResult;
use crate::interface_model::{InterfaceCallbackRegistration, InterfaceMain};

struct InterfaceStats {
    if_names: NameVector,
    if_counters: [(DirectoryIndex, &'static str); 13],
    dir_entry_indices: UnsafeCell<Vec<Vec<DirectoryIndex>>>,
}

static INTERFACE_STATS: OnceLock<InterfaceStats> = OnceLock::new();

// SAFETY: only the main thread with worker publication ownership mutates the
// per-interface lists. Data Workers do not borrow this callback state.
unsafe impl Sync for InterfaceStats {}

pub(crate) const INTERFACE_STATS_CALLBACK: InterfaceCallbackRegistration =
    InterfaceCallbackRegistration {
        callback: statseg_sw_interface_add_del,
        priority: 0,
    };

pub(crate) fn init() -> RuntimeResult<()> {
    let segment = &StatsMain::global()?.segment;
    let if_counters = [
        (
            segment.find("/if/drops", DirectoryType::CounterVectorSimple)?,
            "drops",
        ),
        (
            segment.find("/if/rx-no-buf", DirectoryType::CounterVectorSimple)?,
            "rx-no-buf",
        ),
        (
            segment.find("/if/rx-miss", DirectoryType::CounterVectorSimple)?,
            "rx-miss",
        ),
        (
            segment.find("/if/rx-error", DirectoryType::CounterVectorSimple)?,
            "rx-error",
        ),
        (
            segment.find("/if/tx-error", DirectoryType::CounterVectorSimple)?,
            "tx-error",
        ),
        (
            segment.find("/if/rx", DirectoryType::CounterVectorCombined)?,
            "rx",
        ),
        (
            segment.find("/if/rx-unicast", DirectoryType::CounterVectorCombined)?,
            "rx-unicast",
        ),
        (
            segment.find("/if/rx-multicast", DirectoryType::CounterVectorCombined)?,
            "rx-multicast",
        ),
        (
            segment.find("/if/rx-broadcast", DirectoryType::CounterVectorCombined)?,
            "rx-broadcast",
        ),
        (
            segment.find("/if/tx", DirectoryType::CounterVectorCombined)?,
            "tx",
        ),
        (
            segment.find("/if/tx-unicast", DirectoryType::CounterVectorCombined)?,
            "tx-unicast",
        ),
        (
            segment.find("/if/tx-multicast", DirectoryType::CounterVectorCombined)?,
            "tx-multicast",
        ),
        (
            segment.find("/if/tx-broadcast", DirectoryType::CounterVectorCombined)?,
            "tx-broadcast",
        ),
    ];
    let if_names = segment.add_name_vector("/if/names", 0)?;
    assert!(
        INTERFACE_STATS
            .set(InterfaceStats {
                if_names,
                if_counters,
                dir_entry_indices: UnsafeCell::new(Vec::new()),
            })
            .is_ok(),
        "interface stats callback initializes once"
    );
    Ok(())
}

// VPP: vnet/interface/stats.c:26-84. The main-thread callback publishes
// one name and all family links in one stats directory epoch.
fn statseg_sw_interface_add_del(
    _: &mut DataPlaneMain,
    interfaces: &InterfaceMain,
    sw_if_index: u32,
    is_add: bool,
) -> InterfaceResult<()> {
    hammer_runtime::ensure_main_thread_with_barrier()
        .expect("interface stats callback requires main-thread publication ownership");
    let stats = INTERFACE_STATS
        .get()
        .expect("interface stats initialized before callbacks");
    let segment = &StatsMain::global()
        .expect("stats Main exists before interface callbacks")
        .segment;
    let sw = interfaces
        .software_interface(sw_if_index)
        .expect("stats callback receives a live software interface");
    let sup = interfaces
        .software_interface(sw.sup_sw_if_index)
        .expect("software interface has a superior interface");
    let hw_if_index = sup
        .hw_if_index
        .expect("superior software interface owns hardware");
    assert_eq!(
        sw_if_index, sw.sup_sw_if_index,
        "subinterface ids are not represented yet"
    );
    let name = &interfaces.hardware_interface(hw_if_index).name;
    let slot = sw_if_index as usize;
    // SAFETY: only this main-thread callback mutates the list while workers
    // are stopped by the interface publication barrier.
    let indices = unsafe { &mut *stats.dir_entry_indices.get() };
    if indices.len() <= slot {
        indices.resize_with(slot + 1, Vec::new);
    }
    let current = &mut indices[slot];
    if is_add {
        assert!(current.is_empty(), "interface stats links already exist");
        let path_name = name.replace('/', "_");
        let paths: [(String, DirectoryIndex); 13] = std::array::from_fn(|index| {
            let (target, suffix) = stats.if_counters[index];
            (format!("/interfaces/{path_name}/{suffix}"), target)
        });
        let links: [(&str, DirectoryIndex); 13] =
            std::array::from_fn(|index| (paths[index].0.as_str(), paths[index].1));
        *current = segment
            .add_name_symlinks(stats.if_names.index, sw_if_index, name, &links)
            .expect("registered interface stats symlinks must publish");
    } else {
        segment
            .remove_name_symlinks(stats.if_names.index, sw_if_index, current)
            .expect("registered interface stats symlinks must retire");
        current.clear();
    }
    Ok(())
}
