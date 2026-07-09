use hammer_core::data_plane::BufferIndex;

use crate::DataPlaneRuntime;

#[inline(always)]
pub fn default_prefetch_indices(runtime: &DataPlaneRuntime, indices: &[BufferIndex]) {
    let mut read = 0usize;
    let len = indices.len();
    while read < len {
        runtime.prefetch_header(indices[read]);
        read += 1;
    }
}
