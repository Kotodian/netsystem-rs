fn main() {
    if std::env::var_os("CARGO_CFG_TARGET_OS").is_some_and(|value| value == "linux") {
        println!("cargo:rustc-link-arg=-Wl,-z,interpose");
    }
}
