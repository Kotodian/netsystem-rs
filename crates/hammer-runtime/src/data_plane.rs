use hammer_adapter::{DataPlaneInstructionSet, DataPlaneRuntime, DataPlaneRuntimeConfig};
use hammer_core::config::Config;
use hammer_core::data_plane::DataPlaneBufferConfig;

pub(crate) type RuntimeDataPlaneRuntime = DataPlaneRuntime;

pub fn new_worker_runtime(config: &Config) -> RuntimeDataPlaneRuntime {
    let buffer = &config.worker.buffer;
    let buffers = DataPlaneBufferConfig {
        buffer_slot_capacity: buffer.slot_bytes,
        buffer_slots: buffer.slots_per_numa,
        frame_capacity: buffer.frame_capacity,
        frame_slots: buffer.frame_pool_size,
        ..DataPlaneBufferConfig::default()
    };
    RuntimeDataPlaneRuntime::new_with_instruction_set(
        DataPlaneRuntimeConfig { buffers },
        parse_instruction_set(&config.worker.instruction_set),
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
