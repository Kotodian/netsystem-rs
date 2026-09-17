//! CPU timestamp counter reads.
//!
//! The single operation here is VPP's `clib_cpu_time_now` (`vppinfra/time.h`):
//! one hardware counter read with no fence, no calibration and no conversion.
//! Callers use the returned value only for differences (a dispatch's `clocks`,
//! an interval measurement); turning it into seconds requires the calibration
//! VPP performs in `clib_time_init`, which Hammer has no consumer for yet.

/// Reads the CPU timestamp counter once.
///
/// Returns the raw counter (`rdtsc` on x86_64, `cntvct_el0` on aarch64), not
/// nanoseconds: the value is resynchronized per core and carries no ordering
/// guarantee relative to surrounding instructions, exactly like the VPP
/// implementation. Use differences between two reads on the same thread.
#[cfg(target_arch = "x86_64")]
#[inline(always)]
pub fn cpu_time_now() -> u64 {
    // SAFETY: `_rdtsc` only reads the timestamp counter and has no side effect;
    // it makes no claim about ordering with surrounding instructions.
    unsafe { core::arch::x86_64::_rdtsc() }
}

/// Reads the CPU timestamp counter once.
///
/// Returns the raw counter (`rdtsc` on x86_64, `cntvct_el0` on aarch64), not
/// nanoseconds: the value is resynchronized per core and carries no ordering
/// guarantee relative to surrounding instructions, exactly like the VPP
/// implementation. Use differences between two reads on the same thread.
#[cfg(target_arch = "aarch64")]
#[inline(always)]
pub fn cpu_time_now() -> u64 {
    let counter: u64;
    // SAFETY: `cntvct_el0` is readable at EL0, touches no memory and changes
    // neither the stack nor the flags.
    unsafe {
        core::arch::asm!(
            "mrs {}, cntvct_el0",
            out(reg) counter,
            options(nomem, nostack, preserves_flags)
        );
    }
    counter
}

#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
compile_error!("cpu_time_now has no counter read for this target");
