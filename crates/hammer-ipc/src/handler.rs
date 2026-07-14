use linkme::distributed_slice;
use serde::{Deserialize, Serialize};

use hammer_infra::vec::Vec;
use hammer_runtime::engine::Engine;

/// Handler function type: synchronous fn running on the reactor thread.
pub type IpcHandlerFn = fn(&mut Engine, &[u8]) -> Vec<u8>;

/// A registered IPC handler with a name for dispatch.
pub struct IpcHandler {
    pub name: &'static str,
    pub handler: IpcHandlerFn,
}

/// Linkme distributed slice — registered by `#[ipc_handler]` macro.
#[distributed_slice]
pub static IPC_HANDLERS: [IpcHandler] = [..];

/// Dispatch an IPC request by handler name.
pub fn dispatch_handler(engine: &mut Engine, name: &str, request: &[u8]) -> Option<Vec<u8>> {
    IPC_HANDLERS
        .iter()
        .find(|h| h.name == name)
        .map(|h| (h.handler)(engine, request))
}

/// IPC request frame: handler name + handler-specific payload.
#[derive(Debug, Serialize, Deserialize)]
pub struct IpcRequest {
    pub name: String,
    pub payload: Vec<u8>,
}

/// IPC response frame: handler-specific payload.
#[derive(Debug, Serialize, Deserialize)]
pub struct IpcResponse {
    pub payload: Vec<u8>,
}
