#[cfg(target_arch = "x86")]
#[inline]
pub fn prefetch_read_l1<T>(ptr: *const T) {
    unsafe {
        core::arch::x86::_mm_prefetch(ptr.cast::<i8>(), core::arch::x86::_MM_HINT_T0);
    }
}

#[cfg(target_arch = "x86_64")]
#[inline]
pub fn prefetch_read_l1<T>(ptr: *const T) {
    unsafe {
        core::arch::x86_64::_mm_prefetch(ptr.cast::<i8>(), core::arch::x86_64::_MM_HINT_T0);
    }
}

#[cfg(target_arch = "aarch64")]
#[inline]
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
#[inline]
pub fn prefetch_read_l1<T>(_ptr: *const T) {}

#[cfg(target_arch = "x86")]
#[inline]
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
#[inline]
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
#[inline]
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
#[inline]
pub fn prefetch_write_l1<T>(_ptr: *const T) {}
