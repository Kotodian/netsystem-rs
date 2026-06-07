#[inline(always)]
pub fn unlikely(value: bool) -> bool {
    if value {
        core::hint::cold_path();
    }
    value
}
