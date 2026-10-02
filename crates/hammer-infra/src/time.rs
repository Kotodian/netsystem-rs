//! CPU timestamp counter reads.
//!
//! The single operation here is VPP's `clib_cpu_time_now` (`vppinfra/time.h`):
//! one hardware counter read with no fence, no calibration and no conversion.
//! Callers use the returned value only for differences (a dispatch's `clocks`,
//! an interval measurement). `cpu_clock_frequency` calibrates the conversion
//! used by packet-trace display at startup.

/// Returns the hardware counter frequency in ticks per second.
///
/// VPP `vppinfra/time.c:110-202`: use an architectural frequency when
/// available, then a short wall-clock estimate, then the 2 GHz fallback.
/// The estimate uses monotonic `Instant` rather than CPU MHz from sysfs/proc.
pub fn cpu_clock_frequency() -> f64 {
    #[cfg(target_arch = "aarch64")]
    {
        let frequency: u64;
        // SAFETY: cntfrq_el0 is a read-only architectural register.
        unsafe {
            core::arch::asm!(
                "mrs {}, cntfrq_el0",
                out(reg) frequency,
                options(nomem, nostack, preserves_flags)
            );
        }
        if frequency != 0 {
            return frequency as f64;
        }
    }

    #[cfg(target_arch = "x86_64")]
    {
        // SAFETY: CPUID only queries supported processor information.
        let max_leaf = unsafe { core::arch::x86_64::__cpuid(0) }.eax;
        if max_leaf >= 0x15 {
            let ratio = unsafe { core::arch::x86_64::__cpuid(0x15) };
            if ratio.eax != 0 && ratio.ebx != 0 && ratio.ecx != 0 {
                return ratio.ecx as f64 * ratio.ebx as f64 / ratio.eax as f64;
            }
        }
        if max_leaf >= 0x16 {
            let base = unsafe { core::arch::x86_64::__cpuid(0x16) }.eax & 0xffff;
            if base != 0 {
                return base as f64 * 1_000_000.0;
            }
        }
    }

    let start = std::time::Instant::now();
    let start_ticks = cpu_time_now();
    while start.elapsed() < std::time::Duration::from_millis(1) {
        std::hint::spin_loop();
    }
    let elapsed = start.elapsed().as_secs_f64();
    let ticks = cpu_time_now().wrapping_sub(start_ticks);
    if elapsed > 0.0 && ticks != 0 {
        ticks as f64 / elapsed
    } else {
        2_000_000_000.0
    }
}

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
