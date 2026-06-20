Status: DONE
Commits: none
Test summary: `cargo test -p hammer-service --test tcp_state_machine` passed (6/6); active production-path grep shows no `take_connection(`/`replace_session_state(` remains in the scoped node files, only `crates/hammer-service/src/transport/tcp/syn_sent.rs` unit tests still use `take_connection()`.
Concerns: no commit was created yet; `take_connection()` and `replace_session_state()` are now dead-code in `crates/hammer-service/src/transport/tcp/session.rs` / `crates/hammer-service/src/session/runtime.rs`, but I left removal for a follow-up because the current file still contains unit-test-only uses in `session.rs` and `syn_sent.rs`.
