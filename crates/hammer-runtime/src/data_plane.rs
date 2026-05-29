use hammer_adapter::{DataPlaneRuntime, NoopNode};

pub(crate) type RuntimeDataPlaneRuntime = DataPlaneRuntime<NoopNode>;

pub(crate) fn new_worker_runtime(slot_capacity: usize, slots: usize) -> RuntimeDataPlaneRuntime {
    RuntimeDataPlaneRuntime::with_buffer_capacity(slot_capacity, slots)
}
