.PHONY: build build-release run ctl clean test clippy fmt fmt-check verify-allocation-contract verify-dataplane-performance

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
	cargo build -p hammer-plugin-tun -p hammer-plugin-ip -p hammer-plugin-tcp -p hammer-plugin-udp
	cargo test --workspace

clippy:
	cargo clippy --workspace --all-targets

fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all -- --check

verify-allocation-contract:
	cargo build -p hammer-plugin-tun -p hammer-plugin-ip -p hammer-plugin-tcp -p hammer-plugin-udp
	cargo test -p hammer-infra --test main_heap_interpose -- --nocapture
	cargo test -p hammer-infra --test main_heap_exhaustion -- --nocapture
	cargo test -p hammer-core --test buffer_and_svm_allocation_domains -- --nocapture
	cargo test -p hammer-runtime --test buffer_arena_reuse -- --nocapture
	cargo test -p hammer-runtime --test plugin_loader
	cargo test -p hammer-core --test plugin_config
	cargo run -p hammer --example plugin_additive_load

verify-dataplane-performance:
	cargo test --profile release-perf -p hammer-plugin-ip --test net_lookup_perf -- --ignored --nocapture --test-threads=1
	cargo bench --profile release-perf -p hammer-runtime --bench buffer_alloc_free -- --noplot --sample-size 10 --warm-up-time 0.1 --measurement-time 0.2
	cargo bench --profile release-perf -p hammer-runtime --bench graph_fanout -- --noplot --sample-size 10 --warm-up-time 0.1 --measurement-time 0.2
