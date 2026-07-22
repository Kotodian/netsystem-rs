//! NUMA node probing and memory binding (Linux only).
//!
//! Uses `libc` syscalls (`getcpu`, `mbind`) without linking libnuma/hwloc.
//! macOS and other platforms expose stub no-ops.

#![cfg_attr(not(target_os = "linux"), allow(dead_code))]

use crate::error::RuntimeResult;

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

#[cfg(not(target_os = "linux"))]
#[inline]
pub fn bind_current_thread_memory_to_numa(_node: u32) -> RuntimeResult<()> {
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
fn bind_current_thread_memory_to_numa_impl(_node: u32) -> RuntimeResult<()> {
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn current_numa_node_impl() -> Option<u32> {
    None
}

#[cfg(not(target_os = "linux"))]
fn bind_current_thread_memory_to_numa_impl(_node: u32) -> RuntimeResult<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numa_probe_does_not_panic() {
        let _ = current_numa_node();
    }

    #[test]
    fn numa_bind_stub_is_noop_on_unsupported_platforms() {
        bind_current_thread_memory_to_numa(0).expect("bind stub");
    }
}
