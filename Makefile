.PHONY: build build-release run ctl clean test clippy fmt fmt-check

build:
	cargo build --workspace

build-release:
	cargo build --workspace --release

run:
	cargo run -p hammer -- -c startup.toml

ctl:
	cargo run -p hammerctl --

clean:
	cargo clean

test:
	cargo test --workspace

clippy:
	cargo clippy --workspace --all-targets

fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all -- --check