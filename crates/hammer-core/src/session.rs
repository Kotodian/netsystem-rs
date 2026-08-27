//! Cross-crate Session ABI values.

/// Identity of one Session in its owning worker pool.
///
/// VPP's `session_handle_t` carries these two `u32` facts. Keeping them as
/// fields makes the owner and Pool index explicit. The C representation is
/// not part of this Rust domain value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SessionHandle {
    pub session_index: u32,
    pub thread_index: u32,
}

impl SessionHandle {
    #[inline(always)]
    pub const fn new(session_index: u32, thread_index: u32) -> Self {
        Self {
            session_index,
            thread_index,
        }
    }
}

impl From<SessionHandle> for u64 {
    #[inline]
    fn from(handle: SessionHandle) -> Self {
        let session_index: u64 = handle.session_index.into();
        let thread_index: u64 = handle.thread_index.into();
        session_index | (thread_index << 32)
    }
}

impl From<u64> for SessionHandle {
    #[inline]
    fn from(value: u64) -> Self {
        Self::new(value as u32, (value >> 32) as u32)
    }
}

/// One Session event placed on an IO or control message queue.
///
/// VPP `session_event_t` uses a union: IO events name a Session pool index,
/// while control events name a Session handle. Hammer retains both facts as
/// explicit fields so the shared-memory codec never packs an identity into an
/// unrelated integer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionEvt {
    pub evt_type: SessionEvtType,
    pub postponed: bool,
    pub session_index: u32,
    pub thread_index: u32,
}

impl SessionEvt {
    #[inline]
    pub const fn io(session_index: u32, evt_type: SessionEvtType) -> Self {
        Self {
            evt_type,
            postponed: false,
            session_index,
            thread_index: 0,
        }
    }

    #[inline]
    pub const fn ctrl(handle: SessionHandle, evt_type: SessionEvtType) -> Self {
        Self {
            evt_type,
            postponed: false,
            session_index: handle.session_index,
            thread_index: handle.thread_index,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionEvtType {
    RxEnq = 0,
    TxDeq = 1,
    Connect = 2,
    Close = 3,
    RxDeq = 4,
    TxEnq = 5,
    ProtocolOutput = 6,
    HalfClose = 7,
    Reset = 8,
    Disconnected = 9,
    TransportClosed = 10,
    Bound = 11,
    UnlistenReply = 12,
    Accepted = 13,
    AcceptedReply = 14,
    Connected = 15,
    Listen = 16,
    Unlisten = 17,
    ConnectStream = 18,
}

impl TryFrom<u8> for SessionEvtType {
    type Error = u8;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::RxEnq),
            1 => Ok(Self::TxDeq),
            2 => Ok(Self::Connect),
            3 => Ok(Self::Close),
            4 => Ok(Self::RxDeq),
            5 => Ok(Self::TxEnq),
            6 => Ok(Self::ProtocolOutput),
            7 => Ok(Self::HalfClose),
            8 => Ok(Self::Reset),
            9 => Ok(Self::Disconnected),
            10 => Ok(Self::TransportClosed),
            11 => Ok(Self::Bound),
            12 => Ok(Self::UnlistenReply),
            13 => Ok(Self::Accepted),
            14 => Ok(Self::AcceptedReply),
            15 => Ok(Self::Connected),
            16 => Ok(Self::Listen),
            17 => Ok(Self::Unlisten),
            18 => Ok(Self::ConnectStream),
            value => Err(value),
        }
    }
}
