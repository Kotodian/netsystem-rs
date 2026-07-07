fn forbidden_constructor_tokens() -> [&'static str; 3] {
    [
        concat!("with_", "buffer_", "capacity"),
        concat!("with_", "capacities"),
        concat!("with_", "capacities_", "and_", "instruction_", "set"),
    ]
}

#[test]
fn runtime_sources_use_config_or_static_memory_runtime_constructors() {
    let sources = [
        (
            "crates/hammer-runtime/src/data_plane.rs",
            include_str!("../src/data_plane.rs"),
        ),
        (
            "crates/hammer-runtime/src/engine.rs",
            include_str!("../src/engine.rs"),
        ),
        (
            "crates/hammer-runtime/src/main_loop.rs",
            include_str!("../src/main_loop.rs"),
        ),
        (
            "crates/hammer-runtime/src/spawn.rs",
            include_str!("../src/spawn.rs"),
        ),
        (
            "crates/hammer-runtime/tests/engine_numa_runtime.rs",
            include_str!("engine_numa_runtime.rs"),
        ),
        (
            "crates/hammer-runtime/tests/worker_spawn.rs",
            include_str!("worker_spawn.rs"),
        ),
    ];

    for (path, source) in sources {
        for token in forbidden_constructor_tokens() {
            assert!(
                !source.contains(token),
                "{path} must use the config/static-memory runtime construction path, not {token}"
            );
        }
    }
}
