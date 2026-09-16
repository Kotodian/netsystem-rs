//! Server-owned Binary API protocol, shared-memory transport definitions, and
//! the socket envelope. Handler registration, Main Thread dispatch, and
//! lifecycle ownership remain with the daemon-side server.

pub mod api;
pub mod table;
pub mod vpe;
pub use api::{ApiMain, ApiMsgConfig, ApiMsgData, ApiMsgRange, ApiVersion};
pub mod codec;
pub mod control;
pub use codec::{Array, Deserializer, Serializer, deserialize, serialize, serialize_uninit};
pub mod definition;
pub mod memclnt;
pub mod memory_shared;
pub use definition::{Api, Block, Field, Service, Typedef};

// vlibmemory/memclnt.api declaration order. The macro assigns the stable
// built-in IDs; registration sites select implemented messages from this one
// order without repeating numeric IDs.
hammer_component_macros::api_message_table! {
    pub ids MEMCLNT_LAST;
    MemclntCreate;
    MemclntCreateReply;
    MemclntDelete;
    MemclntDeleteReply;
    RxThreadExit;
    MemclntRxThreadSuspend;
    MemclntReadTimeout;
    RpcCall;
    RpcCallReply;
    GetFirstMsgId;
    GetFirstMsgIdReply;
    ApiVersions;
    ApiVersionsReply;
    TracePluginMsgIds;
    SockclntCreate;
    SockclntCreateReply;
    SockclntDelete;
    SockclntDeleteReply;
    SockInitShm;
    SockInitShmReply;
    MemclntKeepalive;
    MemclntKeepaliveReply;
    ControlPing;
    ControlPingReply;
    MemclntCreateV2;
    MemclntCreateV2Reply;
    GetApiJson;
    GetApiJsonReply;
}

use prost::Message;

/// Default maximum frame size accepted by the server socket envelope.
pub const DEFAULT_MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;

/// One protobuf request frame: context correlates a reply to its request,
/// `method` names the registered Binary API method, and `payload` carries
/// the method's typed protobuf request.
#[derive(Clone, PartialEq, Message)]
pub struct BinaryApiRequest {
    #[prost(uint64, tag = "1")]
    pub context: u64,
    #[prost(string, tag = "2")]
    pub method: String,
    #[prost(bytes = "vec", tag = "3")]
    pub payload: Vec<u8>,
}

/// One protobuf reply frame carrying the transport-level status and the
/// method's typed protobuf reply payload.
#[derive(Clone, PartialEq, Message)]
pub struct BinaryApiReply {
    #[prost(uint64, tag = "1")]
    pub context: u64,
    #[prost(enumeration = "BinaryApiStatus", tag = "2")]
    pub status: i32,
    #[prost(bytes = "vec", tag = "3")]
    pub payload: Vec<u8>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
pub enum BinaryApiStatus {
    Ok = 0,
    InvalidRequest = 1,
    MethodMissing = 2,
    MethodDuplicate = 3,
    MethodPanicked = 4,
    MainThreadUnavailable = 5,
    Internal = 6,
}
