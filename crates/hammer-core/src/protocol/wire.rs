use std::mem::{size_of, transmute};

use crate::error::{CoreError, CoreResult};

#[inline(always)]
pub fn header_ptr<T>(packet: &[u8], offset: usize) -> CoreResult<*const T> {
    let end = offset
        .checked_add(size_of::<T>())
        .ok_or_else(|| CoreError::internal("wire header offset overflow"))?;
    if packet.get(offset..end).is_none() {
        return Err(CoreError::internal("wire header is truncated"));
    }
    // SAFETY: The range check above proves that `offset` points to
    // `size_of::<T>()` initialized bytes inside `packet`. The returned raw
    // pointer may be unaligned; callers must use unaligned access or packed
    // field-safe methods.
    Ok(unsafe { transmute::<_, *const T>(packet.as_ptr().add(offset)) })
}

#[inline(always)]
pub fn header_mut_ptr<T>(packet: &mut [u8], offset: usize) -> CoreResult<*mut T> {
    let end = offset
        .checked_add(size_of::<T>())
        .ok_or_else(|| CoreError::internal("wire header offset overflow"))?;
    if packet.get_mut(offset..end).is_none() {
        return Err(CoreError::internal("wire header is truncated"));
    }
    // SAFETY: The range check above proves that `offset` points to
    // `size_of::<T>()` initialized bytes inside `packet`. The returned raw
    // pointer may be unaligned; callers must use unaligned access or packed
    // field-safe methods.
    Ok(unsafe { transmute::<_, *mut T>(packet.as_mut_ptr().add(offset)) })
}

#[inline(always)]
pub fn read_header<T>(packet: &[u8], offset: usize) -> CoreResult<T>
where
    T: Copy,
{
    let ptr = header_ptr::<T>(packet, offset)?;
    // SAFETY: `header_ptr` checked that the full header range is present.
    // Unaligned access is intentional because network headers can start at
    // arbitrary current-data offsets and all callers use wire-layout types.
    Ok(unsafe { ptr.read_unaligned() })
}

#[inline(always)]
pub fn write_header<T>(packet: &mut [u8], offset: usize, header: T) -> CoreResult<()>
where
    T: Copy,
{
    let ptr = header_mut_ptr::<T>(packet, offset)?;
    // SAFETY: `header_mut_ptr` checked that the full header range is present.
    // Unaligned access is intentional for packed wire headers.
    unsafe { ptr.write_unaligned(header) };
    Ok(())
}
