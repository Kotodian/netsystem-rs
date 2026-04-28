fn main() {
    uniffi::generate_scaffolding("src/hammer.udl").expect("uniffi scaffolding failed");
}
