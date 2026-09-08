use crate::DataPlaneMain;

#[inline(always)]
pub fn default_prefetch_indices(runtime: &DataPlaneMain, indices: &[u32]) {
    let mut read = 0usize;
    let len = indices.len();
    while read < len {
        runtime.prefetch_header(indices[read]);
        read += 1;
    }
}
