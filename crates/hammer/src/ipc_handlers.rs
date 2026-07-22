use hammer_component_macros::ipc_handler;
use hammer_ipc::{PluginCommandError, PluginCommandReply};
use hammer_runtime::RuntimeError;
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

#[ipc_handler(name = "plugin_list")]
fn handle_plugin_list(engine: &mut Engine, request: &[u8]) -> Vec<u8> {
    if !request.is_empty() {
        return encode_plugin_reply(PluginCommandReply::Error(
            PluginCommandError::InvalidRequest,
        ));
    }
    let names = engine.loaded_plugins();
    let names = names.iter().map(String::as_str).collect();
    encode_plugin_reply(PluginCommandReply::Loaded(names))
}

#[ipc_handler(name = "plugin_load")]
fn handle_plugin_load(engine: &mut Engine, request: &[u8]) -> Vec<u8> {
    let roots: Vec<String> = match bincode::deserialize(request) {
        Ok(roots) => roots,
        Err(_) => {
            return encode_plugin_reply(PluginCommandReply::Error(
                PluginCommandError::InvalidRequest,
            ));
        }
    };
    let result =
        super::load_current_config().and_then(|config| engine.load_plugins(&roots, &config));
    match result {
        Ok(()) => {
            let names = engine.loaded_plugins();
            let names = names.iter().map(String::as_str).collect();
            encode_plugin_reply(PluginCommandReply::Loaded(names))
        }
        Err(error) => encode_plugin_reply(PluginCommandReply::Error(plugin_command_error(error))),
    }
}

fn encode_plugin_reply(reply: PluginCommandReply<'_>) -> Vec<u8> {
    match bincode::serialize(&reply) {
        Ok(encoded) => Vec::from(encoded),
        Err(_) => Vec::new(),
    }
}

fn plugin_command_error(error: RuntimeError) -> PluginCommandError {
    match error {
        RuntimeError::MemoryNotInitialized => PluginCommandError::MemoryNotInitialized,
        RuntimeError::Plugin(_) => PluginCommandError::Lifecycle,
        RuntimeError::WorkerCountOverflow { .. } => PluginCommandError::WorkerCountOverflow,
        RuntimeError::WorkerGraphUpdateAlreadyPending => {
            PluginCommandError::WorkerGraphUpdatePending
        }
        RuntimeError::WorkerGraphUpdateMissing
        | RuntimeError::WorkerGraphUpdateStatePoisoned
        | RuntimeError::WorkerGraphUpdateNotAdditive => PluginCommandError::WorkerGraphUpdate,
        RuntimeError::ConfigParse { .. } | RuntimeError::ConfigValidation { .. } => {
            PluginCommandError::Configuration
        }
        RuntimeError::PacketGraph(_) => PluginCommandError::GraphMaterialization,
        RuntimeError::Attach(_)
        | RuntimeError::MainHeap(_)
        | RuntimeError::Lifecycle { .. }
        | RuntimeError::ServiceClosed
        | RuntimeError::Invariant { .. } => PluginCommandError::Lifecycle,
    }
}
