use hammer_core::data_plane::Index;

use crate::DataPlaneRuntime;

#[inline(always)]
pub fn default_prefetch_indices(runtime: &DataPlaneRuntime, indices: &[Index]) {
    let mut read = 0usize;
    let len = indices.len();
    while read < len {
        runtime.prefetch_header(indices[read]);
        read += 1;
    }
}
