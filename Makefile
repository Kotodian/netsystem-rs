.PHONY: build build-release run ctl clean test clippy fmt fmt-check verify-allocation-contract verify-dataplane-performance verify-hugepages

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
	HAMMER_PLUGIN_DIR="$${CARGO_TARGET_DIR:-target}/debug" cargo run -p hammer --example plugin_additive_load

verify-dataplane-performance:
	cargo test --profile release-perf -p hammer-plugin-ip --test net_lookup_perf -- --ignored --nocapture --test-threads=1
	cargo bench --profile release-perf -p hammer-runtime --bench buffer_alloc_free -- --noplot --sample-size 10 --warm-up-time 0.1 --measurement-time 0.2
	cargo bench --profile release-perf -p hammer-runtime --bench graph_fanout -- --noplot --sample-size 10 --warm-up-time 0.1 --measurement-time 0.2

verify-hugepages:
	cargo test -p hammer-infra --test hugepages automatic_pool_growth_and_buffer_backing_use_2_mib_hugetlb -- --ignored --exact --nocapture
	cargo test -p hammer-infra --test hugepages buffer_backing_uses_1_gib_hugetlb_when_advertised -- --ignored --exact --nocapture
	cargo test -p hammer-infra --test hugepages main_heap_uses_2_mib_hugetlb_and_never_falls_back -- --ignored --exact --nocapture
	cargo test -p hammer-infra --test hugepages main_heap_uses_1_gib_hugetlb_when_advertised -- --ignored --exact --nocapture
	cargo test -p hammer-infra --test hugepages unsupported_explicit_hugepage_requests_fail_without_fallback -- --ignored --exact --nocapture
	cargo test -p hammer-infra --test hugepages transparent_hugepage_hint_is_not_reported_as_hugetlb -- --ignored --exact --nocapture
