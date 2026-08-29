.PHONY: build build-release run ctl clean clippy fmt fmt-check verify-allocation-contract verify-dataplane-performance

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

clippy:
	cargo clippy --workspace --all-targets

fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all -- --check

verify-allocation-contract:
	cargo build -p hammer-plugin-tun -p hammer-plugin-ip -p hammer-plugin-tcp -p hammer-plugin-udp
	HAMMER_PLUGIN_DIR="$${CARGO_TARGET_DIR:-target}/debug" cargo run -p hammer --example plugin_additive_load

verify-dataplane-performance:
	cargo bench --profile release-perf -p hammer-runtime --bench buffer_alloc_free -- --noplot --sample-size 10 --warm-up-time 0.1 --measurement-time 0.2
	cargo bench --profile release-perf -p hammer-runtime --bench graph_fanout -- --noplot --sample-size 10 --warm-up-time 0.1 --measurement-time 0.2
