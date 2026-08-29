//! NUMA node probing and memory binding (Linux only).
//!
//! Uses `libc` syscalls (`getcpu`, `mbind`) without linking libnuma/hwloc.
//! macOS and other platforms expose stub no-ops.

#![cfg_attr(not(target_os = "linux"), allow(dead_code))]

#[cfg(target_os = "linux")]
use crate::error::RuntimeError;
use crate::error::RuntimeResult;

#[cfg(target_os = "linux")]
use std::path::Path;

/// Return the NUMA node index for the current CPU, when available.
#[cfg(target_os = "linux")]
#[inline]
pub fn current_numa_node() -> Option<u32> {
    current_numa_node_impl()
}

#[cfg(not(target_os = "linux"))]
#[inline]
pub fn current_numa_node() -> Option<u32> {
    None
}

/// Bind the current thread's future heap allocations to `node` when supported.
#[cfg(target_os = "linux")]
#[inline]
pub fn bind_current_thread_memory_to_numa(node: u32) -> RuntimeResult<()> {
    bind_current_thread_memory_to_numa_impl(node)
}

#[cfg(target_os = "linux")]
pub(crate) fn node_for_cpu(cpu: usize) -> RuntimeResult<u32> {
    node_for_cpu_in(Path::new("/sys/devices/system/cpu"), cpu)
}

#[cfg(not(target_os = "linux"))]
#[inline]
pub fn bind_current_thread_memory_to_numa(_: u32) -> RuntimeResult<()> {
    Ok(())
}

#[cfg(target_os = "linux")]
fn current_numa_node_impl() -> Option<u32> {
    let mut cpu: libc::c_uint = 0;
    let mut node: libc::c_uint = 0;
    let rc = unsafe {
        libc::syscall(
            libc::SYS_getcpu,
            &mut cpu,
            &mut node,
            std::ptr::null_mut::<libc::c_void>(),
        )
    };
    if rc == 0 { Some(node) } else { None }
}

#[cfg(target_os = "linux")]
fn bind_current_thread_memory_to_numa_impl(node: u32) -> RuntimeResult<()> {
    const MAX_NUMA_NODES: usize = 1024;

    let node = node as usize;
    if node >= MAX_NUMA_NODES {
        return Err(RuntimeError::lifecycle(
            "bind worker NUMA memory",
            format!("NUMA node {node} exceeds the Linux nodemask limit"),
        ));
    }

    let mut mask = [0 as libc::c_ulong; MAX_NUMA_NODES / libc::c_ulong::BITS as usize];
    mask[node / libc::c_ulong::BITS as usize] =
        (1 as libc::c_ulong) << (node % libc::c_ulong::BITS as usize);
    // SAFETY: `mask` remains live for the syscall and contains exactly the
    // requested NUMA node.
    if unsafe {
        libc::syscall(
            libc::SYS_set_mempolicy,
            libc::MPOL_BIND,
            mask.as_ptr(),
            MAX_NUMA_NODES as libc::c_ulong,
        )
    } != 0
    {
        return Err(RuntimeError::lifecycle(
            "bind worker NUMA memory",
            std::io::Error::last_os_error().to_string(),
        ));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn node_for_cpu_in(root: &Path, cpu: usize) -> RuntimeResult<u32> {
    let directory = root.join(format!("cpu{cpu}"));
    let entries = std::fs::read_dir(&directory).map_err(|error| {
        RuntimeError::lifecycle(
            "resolve worker NUMA node",
            format!("read {}: {error}", directory.display()),
        )
    })?;
    for entry in entries {
        let entry = entry.map_err(|error| {
            RuntimeError::lifecycle(
                "resolve worker NUMA node",
                format!("read entry in {}: {error}", directory.display()),
            )
        })?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let Some(node) = name.strip_prefix("node") else {
            continue;
        };
        if let Ok(node) = node.parse::<u32>() {
            return Ok(node);
        }
    }
    Ok(0)
}

#[cfg(not(target_os = "linux"))]
fn current_numa_node_impl() -> Option<u32> {
    None
}

#[cfg(not(target_os = "linux"))]
fn bind_current_thread_memory_to_numa_impl(_: u32) -> RuntimeResult<()> {
    Ok(())
}
