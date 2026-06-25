#[cfg(target_arch = "x86")]
#[inline(always)]
pub fn prefetch_read_l1<T>(ptr: *const T) {
    unsafe {
        core::arch::x86::_mm_prefetch(ptr.cast::<i8>(), core::arch::x86::_MM_HINT_T0);
    }
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
pub fn prefetch_read_l1<T>(ptr: *const T) {
    unsafe {
        core::arch::x86_64::_mm_prefetch(ptr.cast::<i8>(), core::arch::x86_64::_MM_HINT_T0);
    }
}

#[cfg(target_arch = "aarch64")]
#[inline(always)]
pub fn prefetch_read_l1<T>(ptr: *const T) {
    unsafe {
        core::arch::asm!(
            "prfm pldl1keep, [{0}]",
            in(reg) ptr,
            options(nostack, preserves_flags)
        );
    }
}

#[cfg(not(any(target_arch = "x86", target_arch = "x86_64", target_arch = "aarch64")))]
#[inline(always)]
pub fn prefetch_read_l1<T>(_ptr: *const T) {}

#[cfg(target_arch = "x86")]
#[inline(always)]
pub fn prefetch_write_l1<T>(ptr: *const T) {
    unsafe {
        core::arch::asm!(
            "prefetchw [{0}]",
            in(reg) ptr,
            options(nostack, preserves_flags)
        );
    }
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
pub fn prefetch_write_l1<T>(ptr: *const T) {
    unsafe {
        core::arch::asm!(
            "prefetchw [{0}]",
            in(reg) ptr,
            options(nostack, preserves_flags)
        );
    }
}

#[cfg(target_arch = "aarch64")]
#[inline(always)]
pub fn prefetch_write_l1<T>(ptr: *const T) {
    unsafe {
        core::arch::asm!(
            "prfm pstl1keep, [{0}]",
            in(reg) ptr,
            options(nostack, preserves_flags)
        );
    }
}

#[cfg(not(any(target_arch = "x86", target_arch = "x86_64", target_arch = "aarch64")))]
#[inline(always)]
pub fn prefetch_write_l1<T>(_ptr: *const T) {}

// L2 prefetch hints. Used by batched buffer alloc/free and node loops that
// walk several buffers ahead of the one being touched: a single buffer header
// spans two 64B cachelines and consecutive slots are ~192B apart, so an L1
// hint issued one step ahead is usually too late to hide the miss. L2 hints
// keep the next-but-one slot warm while the current slot is being processed.

#[cfg(target_arch = "x86")]
#[inline(always)]
pub fn prefetch_read_l2<T>(ptr: *const T) {
    unsafe {
        core::arch::x86::_mm_prefetch(ptr.cast::<i8>(), core::arch::x86::_MM_HINT_T1);
    }
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
pub fn prefetch_read_l2<T>(ptr: *const T) {
    unsafe {
        core::arch::x86_64::_mm_prefetch(ptr.cast::<i8>(), core::arch::x86_64::_MM_HINT_T1);
    }
}

#[cfg(target_arch = "aarch64")]
#[inline(always)]
pub fn prefetch_read_l2<T>(ptr: *const T) {
    unsafe {
        core::arch::asm!(
            "prfm pldl2keep, [{0}]",
            in(reg) ptr,
            options(nostack, preserves_flags)
        );
    }
}

#[cfg(not(any(target_arch = "x86", target_arch = "x86_64", target_arch = "aarch64")))]
#[inline(always)]
pub fn prefetch_read_l2<T>(_ptr: *const T) {}

#[cfg(target_arch = "x86")]
#[inline(always)]
pub fn prefetch_write_l2<T>(ptr: *const T) {
    unsafe {
        core::arch::x86::_mm_prefetch(ptr.cast::<i8>(), core::arch::x86::_MM_HINT_T2);
    }
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
pub fn prefetch_write_l2<T>(ptr: *const T) {
    unsafe {
        core::arch::x86_64::_mm_prefetch(ptr.cast::<i8>(), core::arch::x86_64::_MM_HINT_T2);
    }
}

#[cfg(target_arch = "aarch64")]
#[inline(always)]
pub fn prefetch_write_l2<T>(ptr: *const T) {
    unsafe {
        core::arch::asm!(
            "prfm pstl2keep, [{0}]",
            in(reg) ptr,
            options(nostack, preserves_flags)
        );
    }
}

#[cfg(not(any(target_arch = "x86", target_arch = "x86_64", target_arch = "aarch64")))]
#[inline(always)]
pub fn prefetch_write_l2<T>(_ptr: *const T) {}
