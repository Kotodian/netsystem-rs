use super::*;

pub(crate) fn prefetch_buffer_header(buffer: &Buffer) {
    prefetch_read_l1(ptr::from_ref(&buffer.cacheline0).cast::<u8>());
}

#[inline(always)]
pub(crate) fn prefetch_buffer_header_write(buffer: &Buffer) {
    prefetch_write_l1(ptr::from_ref(&buffer.cacheline0).cast::<u8>());
}

#[inline(always)]
pub(crate) fn prefetch_buffer_cacheline1(buffer: &Buffer) {
    prefetch_read_l1(ptr::from_ref(&buffer.cacheline1).cast::<u8>());
}

#[inline(always)]
pub(crate) fn prefetch_buffer_cacheline1_write(buffer: &Buffer) {
    prefetch_write_l1(ptr::from_ref(&buffer.cacheline1).cast::<u8>());
}

#[inline(always)]
pub(crate) fn prefetch_buffer_data(buffer: &Buffer) {
    if !buffer.current().is_empty() {
        prefetch_read_l1(buffer.current().as_ptr());
    }
}

#[inline(always)]
pub(crate) fn prefetch_buffer_data_write(buffer: &Buffer) {
    if !buffer.current().is_empty() {
        prefetch_write_l1(buffer.current().as_ptr());
    }
}
