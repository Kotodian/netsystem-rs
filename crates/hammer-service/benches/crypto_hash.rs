use std::hint::black_box;

use criterion::Criterion;
use hammer_infra::crypto::InstructionSet;
use hammer_service::crypto::{Engine, Hash, HashOperation, Input, SelectionPolicy};

fn measure_implementation(criterion: &mut Criterion, implementation: &str) {
    let mut engine =
        Engine::with_builtins(InstructionSet::detect()).expect("built-in crypto registry is valid");
    engine.set_selection_policy(SelectionPolicy::only([implementation]));
    let algorithm = engine
        .algorithm::<Hash>("sha-256")
        .expect("standard SHA-256 algorithm");
    let mut context = engine
        .context(algorithm)
        .expect("selected SHA-256 implementation is available");
    let input = [0x5a; 1024];

    criterion.bench_function(&format!("sha-256/{implementation}/singleton"), |bencher| {
        bencher.iter(|| {
            let mut output = [0u8; 32];
            let mut operations = [HashOperation::new(
                Input::Contiguous(black_box(&input)),
                &mut output,
            )];
            context
                .execute(&mut operations)
                .expect("selected implementation remains available");
            black_box(output)
        });
    });

    criterion.bench_function(&format!("sha-256/{implementation}/multi-64"), |bencher| {
        bencher.iter(|| {
            let mut outputs = [[0u8; 32]; 64];
            let mut operations = outputs
                .each_mut()
                .map(|output| HashOperation::new(Input::Contiguous(black_box(&input)), output));
            context
                .execute(&mut operations)
                .expect("selected implementation remains available");
            black_box(outputs)
        });
    });
}

fn main() {
    let instructions = InstructionSet::detect();
    let mut criterion = Criterion::default().configure_from_args();
    measure_implementation(&mut criterion, "hammer:hash-portable");

    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    if instructions.contains(InstructionSet::SHA2) {
        measure_implementation(&mut criterion, "hammer:sha-256-sha-ni");
    }
    #[cfg(target_arch = "aarch64")]
    if instructions.contains(InstructionSet::SHA2) {
        measure_implementation(&mut criterion, "hammer:sha-256-armv8");
    }

    criterion.final_summary();
}
