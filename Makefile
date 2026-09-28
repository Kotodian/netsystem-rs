.PHONY: build build-release run clean clippy fmt fmt-check verify-dataplane-performance

build:
	cargo build --workspace

build-release:
	cargo build --workspace --release

run:
	cargo run -p hammer -- -c startup.toml

clean:
	cargo clean

clippy:
	cargo clippy --workspace --all-targets

fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all -- --check

verify-dataplane-performance:
	cargo bench --profile release-perf -p hammer-runtime --bench buffer_alloc_free -- --noplot --sample-size 10 --warm-up-time 0.1 --measurement-time 0.2
	cargo bench --profile release-perf -p hammer-runtime --bench graph_fanout -- --noplot --sample-size 10 --warm-up-time 0.1 --measurement-time 0.2
