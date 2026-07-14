use hammer_component_macros::ipc_handler;
use hammer_infra::vec::Vec;
use hammer_runtime::engine::Engine;

#[ipc_handler(name = "ping")]
fn handle_ping(_engine: &mut Engine, _request: &[u8]) -> Vec<u8> {
    Vec::from(b"pong".as_slice())
}

#[ipc_handler(name = "status")]
fn handle_status(_engine: &mut Engine, _request: &[u8]) -> Vec<u8> {
    Vec::from(b"ok".as_slice())
}

#[ipc_handler(name = "pause")]
fn handle_pause(_engine: &mut Engine, _request: &[u8]) -> Vec<u8> {
    Vec::new()
}

#[ipc_handler(name = "wake")]
fn handle_wake(_engine: &mut Engine, _request: &[u8]) -> Vec<u8> {
    Vec::new()
}

#[ipc_handler(name = "shutdown")]
fn handle_shutdown(engine: &mut Engine, _request: &[u8]) -> Vec<u8> {
    engine
        .main_loop_exit_now
        .store(true, std::sync::atomic::Ordering::Relaxed);
    Vec::new()
}

#[ipc_handler(name = "reset_network")]
fn handle_reset_network(_engine: &mut Engine, _request: &[u8]) -> Vec<u8> {
    Vec::new()
}

#[ipc_handler(name = "metrics")]
fn handle_metrics(_engine: &mut Engine, _request: &[u8]) -> Vec<u8> {
    Vec::new()
}

#[ipc_handler(name = "list_listeners")]
fn handle_list_listeners(_engine: &mut Engine, _request: &[u8]) -> Vec<u8> {
    Vec::new()
}

#[ipc_handler(name = "config_reload")]
fn handle_config_reload(_engine: &mut Engine, _request: &[u8]) -> Vec<u8> {
    Vec::new()
}
