//! Focused tests for the HTTP plugin descriptor and the builtin HTTP Session
//! App registration seam (VPP `http_app_cb_vft` attach, http.c:1004-1063).

use hammer_runtime::{
    DataPlaneRuntime, DataPlaneRuntimeConfig, DataWorkerId, Engine, RuntimeError, RuntimeRegistry,
};
use hammer_service::session::protocol::SessionAppCallbacks;

use super::http_app::{CALLBACKS, HTTP_SESSION_APP, NAME, destroy, install};

#[test]
fn plugin_descriptor_declares_name_http_loaded_after_quic() {
    let manifest = crate::__HAMMER_PLUGIN_MANIFEST_TOML;
    assert!(manifest.contains("name = \"http\""), "manifest: {manifest}");
    assert!(
        manifest.contains("load_after = [\"quic\",]"),
        "manifest: {manifest}"
    );
}

#[test]
fn registers_exactly_the_builtin_http_session_app() {
    assert_eq!(HTTP_SESSION_APP.name(), NAME);
}

#[test]
fn callback_table_defers_all_stateful_vpp_callbacks() {
    // Every VPP `http_app_cb_vft` entry (http.c:1004-1017) needs HTTP worker
    // connection state, so the table is exactly `all_none()`: no speculative
    // no-ops until the owning slices land.
    let callbacks: SessionAppCallbacks = CALLBACKS;
    assert!(callbacks.add_segment.is_none());
    assert!(callbacks.del_segment.is_none());
    assert!(callbacks.accept.is_none());
    assert!(callbacks.connected.is_none());
    assert!(callbacks.disconnect.is_none());
    assert!(callbacks.reset.is_none());
    assert!(callbacks.transport_closed.is_none());
    assert!(callbacks.cleanup.is_none());
    assert!(callbacks.half_open_cleanup.is_none());
    assert!(callbacks.migrate.is_none());
    assert!(callbacks.listened.is_none());
    assert!(callbacks.unlistened.is_none());
    assert!(callbacks.builtin_rx.is_none());
    assert!(callbacks.builtin_tx.is_none());
    assert!(callbacks.fifo_tuning.is_none());
    assert!(callbacks.proxy_alloc_fifos.is_none());
    assert!(callbacks.proxy_write_early_data.is_none());
    assert!(callbacks.app_evt.is_none());
    assert!(callbacks.crypto_async.is_none());
}

#[test]
fn install_returns_typed_error_without_session_main() {
    let mut engine = Engine::new(
        DataPlaneRuntime::new(DataPlaneRuntimeConfig::default()),
        RuntimeRegistry::new(),
    );
    let error = install(&mut engine).expect_err("install without SessionMain must fail");
    assert!(
        matches!(error, RuntimeError::RuntimeCapabilityMissing { .. }),
        "expected RuntimeCapabilityMissing, got {error:?}"
    );
}

#[test]
fn destroy_without_worker_contexts_is_noop() {
    // destroy cannot report a Result; with no HTTP worker context ever
    // created in this slice, the hook must be callable and do nothing.
    destroy(DataWorkerId::new(0), 0);
}
