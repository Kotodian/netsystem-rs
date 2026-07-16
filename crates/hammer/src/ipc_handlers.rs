use hammer_component_macros::ipc_handler;
use hammer_core::error::CoreError;
use hammer_ipc::{PluginCommandError, PluginCommandReply};
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
    encode_plugin_reply(PluginCommandReply::Loaded(engine.loaded_plugins()))
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
    let result = hammer_runtime::plugin_loader::configured_plugin_path()
        .map_err(CoreError::from)
        .and_then(|path| engine.load_plugins(&path, &roots));
    match result {
        Ok(()) => encode_plugin_reply(PluginCommandReply::Loaded(engine.loaded_plugins())),
        Err(error) => encode_plugin_reply(PluginCommandReply::Error(plugin_command_error(error))),
    }
}

fn encode_plugin_reply(reply: PluginCommandReply<'_>) -> Vec<u8> {
    match bincode::serialize(&reply) {
        Ok(encoded) => Vec::from(encoded),
        Err(_) => Vec::new(),
    }
}

fn plugin_command_error(error: CoreError) -> PluginCommandError {
    match error {
        CoreError::MemoryNotInitialized => PluginCommandError::MemoryNotInitialized,
        CoreError::PluginDuplicateRoot { .. } => PluginCommandError::DuplicateRoot,
        CoreError::PluginDependencyCycle { .. } => PluginCommandError::DependencyCycle,
        CoreError::PluginHostVersionInvalid => PluginCommandError::HostVersionInvalid,
        CoreError::PluginRequiredVersionInvalid => PluginCommandError::RequiredVersionInvalid,
        CoreError::PluginSemVerMismatch => PluginCommandError::VersionMismatch,
        CoreError::PluginLibraryOpen { .. } => PluginCommandError::LibraryOpen,
        CoreError::PluginRegistrationSymbol { .. } => PluginCommandError::RegistrationSymbol,
        CoreError::PluginRegistrationNull { .. } => PluginCommandError::RegistrationNull,
        CoreError::PluginNameMismatch { .. } => PluginCommandError::RegistrationNameMismatch,
        CoreError::PluginExecutablePath { .. } => PluginCommandError::ExecutablePath,
        CoreError::PluginExecutableParentMissing { .. } => {
            PluginCommandError::ExecutableParentMissing
        }
        CoreError::WorkerCountOverflow { .. } => PluginCommandError::WorkerCountOverflow,
        CoreError::WorkerGraphUpdateAlreadyPending => PluginCommandError::WorkerGraphUpdatePending,
        CoreError::WorkerGraphUpdateMissing
        | CoreError::WorkerGraphUpdateStatePoisoned
        | CoreError::WorkerGraphUpdateNotAdditive => PluginCommandError::WorkerGraphUpdate,
        CoreError::ConfigParse { .. } | CoreError::ConfigValidation { .. } => {
            PluginCommandError::Configuration
        }
        CoreError::DataPlane(_) => PluginCommandError::GraphMaterialization,
        CoreError::Attach(_)
        | CoreError::MainHeap(_)
        | CoreError::Lifecycle { .. }
        | CoreError::ServiceClosed
        | CoreError::Tcp(_)
        | CoreError::Internal { .. } => PluginCommandError::Lifecycle,
    }
}
