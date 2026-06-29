use hammer_adapter::{DataPlaneInstructionSet, DataPlaneRuntime};
use hammer_core::config::Config;

pub(crate) type RuntimeDataPlaneRuntime = DataPlaneRuntime;

pub fn new_worker_runtime(config: &Config) -> RuntimeDataPlaneRuntime {
    let buffer = &config.worker.buffer;
    let instr_set = parse_instruction_set(&config.worker.instruction_set);
    RuntimeDataPlaneRuntime::with_capacities_and_instruction_set(
        buffer.slot_bytes,
        buffer.slots_per_numa,
        buffer.frame_capacity,
        buffer.frame_pool_size,
        instr_set,
    )
}

fn parse_instruction_set(s: &str) -> DataPlaneInstructionSet {
    match s.to_lowercase().as_str() {
        "native" => DataPlaneInstructionSet::native(),
        "scalar" => DataPlaneInstructionSet::Scalar,
        "sse2" => DataPlaneInstructionSet::Sse2,
        "avx2" => DataPlaneInstructionSet::Avx2,
        "avx512" => DataPlaneInstructionSet::Avx512,
        "neon" => DataPlaneInstructionSet::Neon,
        _ => {
            tracing::warn!("unknown instruction_set '{s}', falling back to native");
            DataPlaneInstructionSet::native()
        }
    }
}
