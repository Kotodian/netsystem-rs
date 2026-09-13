use std::env;
use std::ffi::OsStr;
use std::path::PathBuf;

fn main() {
    let manifest_dir =
        PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("Cargo sets CARGO_MANIFEST_DIR"));
    let source_dir = manifest_dir.join("../../third_party/dlmalloc");

    let mut build = cc::Build::new();
    build
        .file(source_dir.join("dlmalloc.c"))
        .include(&source_dir)
        .define("ONLY_MSPACES", "1")
        .define("MSPACES", "1")
        .define("USE_LOCKS", "1")
        .flag_if_supported("-fvisibility=hidden");

    if env::var_os("CARGO_CFG_TARGET_OS").as_deref() == Some(OsStr::new("macos")) {
        build.define("DARWIN", "1");
    }

    build.compile("dlmalloc");

    println!(
        "cargo:rerun-if-changed={}",
        source_dir.join("dlmalloc.c").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        source_dir.join("dlmalloc.h").display()
    );
}
