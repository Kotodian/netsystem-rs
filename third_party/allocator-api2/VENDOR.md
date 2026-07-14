# allocator-api2 vendor notes

## Upstream

- Crate: `allocator-api2`
- Version: `0.4.0`
- Source: https://github.com/zakarumych/allocator-api2
- License: MIT OR Apache-2.0 (see `LICENSE-MIT`, `LICENSE-APACHE`)

## Hammer packaging

- Path: `third_party/allocator-api2`
- Package / lib names unchanged: `allocator-api2` / `allocator_api2`
- `publish = false`
- Consumed as a path dependency from `hammer-infra`
- Do **not** add `[patch.crates-io]` for this crate; leave crates.io resolution for `talc` alone unless Cargo requires an explicit unification decision later

## Rust 1.96 deltas

Port into this tree as needed for Hammer's public Vec facade:

- `Vec::extract_if`
- `Vec::push_mut`
- `Vec::insert_mut`
- `Vec::pop_if`
- Matching `ExtractIf` / `Splice` iterator surfaces

Do not copy rustc-private `liballoc` sources directly.
