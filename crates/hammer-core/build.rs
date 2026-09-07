use std::{env, fs, path::PathBuf};

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=HAMMER_BUFFER_PRE_DATA_SIZE");
    println!("cargo:rerun-if-env-changed=HAMMER_BUFFER_TRACE_TRAJECTORY");
    println!("cargo:rustc-check-cfg=cfg(hammer_buffer_trace_trajectory)");

    let pre_data = env::var_os("HAMMER_BUFFER_PRE_DATA_SIZE").unwrap_or_else(|| "128".into());
    let pre_data = pre_data
        .to_str()
        .expect("HAMMER_BUFFER_PRE_DATA_SIZE must be decimal UTF-8");
    assert!(
        !pre_data.is_empty() && pre_data.bytes().all(|byte| byte.is_ascii_digit()),
        "HAMMER_BUFFER_PRE_DATA_SIZE must contain only decimal digits"
    );
    let pre_data: usize = pre_data
        .parse()
        .expect("HAMMER_BUFFER_PRE_DATA_SIZE must fit usize");
    assert!(
        pre_data.is_multiple_of(64) && pre_data <= 32_768,
        "HAMMER_BUFFER_PRE_DATA_SIZE must be a multiple of 64 in 0..=32768 so -pre_data fits i16"
    );

    let trajectory = env::var_os("HAMMER_BUFFER_TRACE_TRAJECTORY").unwrap_or_else(|| "0".into());
    let trajectory = match trajectory.to_str() {
        Some("0") => false,
        Some("1") => true,
        _ => panic!("HAMMER_BUFFER_TRACE_TRAJECTORY must be exactly 0 or 1"),
    };
    if trajectory {
        println!("cargo:rustc-cfg=hammer_buffer_trace_trajectory");
    }
    let trajectory_size = usize::from(trajectory) * 64;
    let header_size = 128 + trajectory_size;
    let output = PathBuf::from(env::var_os("OUT_DIR").expect("Cargo provides OUT_DIR"));
    fs::write(
        output.join("buffer_config.rs"),
        format!(
            "/// Compile-time inline pre-data capacity, in bytes.\n\
             pub const BUFFER_PRE_DATA_SIZE: usize = {pre_data};\n\
             /// Whether this artifact includes the Buffer trajectory area.\n\
             pub const BUFFER_TRACE_TRAJECTORY: bool = {trajectory};\n\
             /// Bytes reserved for the optional Buffer trajectory area.\n\
             pub const BUFFER_TRACE_TRAJECTORY_SIZE: usize = {trajectory_size};\n\
             /// Buffer header bytes, excluding inline pre-data and trailing packet data.\n\
             pub const BUFFER_HEADER_SIZE: usize = {header_size};\n"
        ),
    )
    .expect("write generated Buffer configuration");
}
