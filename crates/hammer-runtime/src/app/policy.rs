use thiserror::Error;

pub const APP_SESSION_POLICY_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct ApplicationId(u64);

impl ApplicationId {
    #[inline]
    pub const fn new(slot: u32, generation: u32) -> Self {
        Self((slot as u64) | ((generation as u64) << 32))
    }

    #[inline]
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    #[inline]
    pub const fn slot(self) -> u32 {
        self.0 as u32
    }

    #[inline]
    pub const fn generation(self) -> u32 {
        (self.0 >> 32) as u32
    }

    #[inline]
    pub const fn raw(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct AppSessionProtocolSelection {
    protocol: String,
    #[serde(default)]
    id: Option<u64>,
}

impl AppSessionProtocolSelection {
    pub fn new(protocol: impl Into<String>) -> Self {
        Self {
            protocol: protocol.into(),
            id: None,
        }
    }

    pub fn with_id(protocol: impl Into<String>, id: u64) -> Self {
        Self {
            protocol: protocol.into(),
            id: Some(id),
        }
    }

    #[inline]
    pub fn protocol(&self) -> &str {
        &self.protocol
    }

    #[inline]
    pub const fn id(&self) -> Option<u64> {
        self.id
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct AppSessionPolicy {
    version: u32,
    transport: String,
    protocols: Box<[AppSessionProtocolSelection]>,
}

impl AppSessionPolicy {
    pub fn new(
        version: u32,
        transport: impl Into<String>,
        protocols: impl IntoIterator<Item = AppSessionProtocolSelection>,
    ) -> Result<Self, AppSessionPolicyError> {
        if version != APP_SESSION_POLICY_VERSION {
            return Err(AppSessionPolicyError::UnsupportedVersion { actual: version });
        }
        let transport = transport.into();
        if transport.trim().is_empty() {
            return Err(AppSessionPolicyError::TransportNameEmpty);
        }
        let protocols = protocols.into_iter().collect::<Vec<_>>().into_boxed_slice();
        if protocols.is_empty() {
            return Err(AppSessionPolicyError::ProtocolSequenceEmpty);
        }
        for (index, selection) in protocols.iter().enumerate() {
            if selection.protocol.trim().is_empty() {
                return Err(AppSessionPolicyError::ProtocolNameEmpty { index });
            }
        }
        Ok(Self {
            version,
            transport,
            protocols,
        })
    }

    #[inline]
    pub const fn version(&self) -> u32 {
        self.version
    }

    #[inline]
    pub fn transport(&self) -> &str {
        &self.transport
    }

    #[inline]
    pub fn protocols(&self) -> &[AppSessionProtocolSelection] {
        &self.protocols
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum AppSessionPolicyError {
    #[error("App Session Policy version {actual} is unsupported")]
    UnsupportedVersion { actual: u32 },
    #[error("App Session Policy Transport name is empty")]
    TransportNameEmpty,
    #[error("App Session Policy protocol sequence is empty")]
    ProtocolSequenceEmpty,
    #[error("App Session Policy protocol at index {index} has an empty name")]
    ProtocolNameEmpty { index: usize },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct ApplicationListenerId(u64);

impl ApplicationListenerId {
    #[inline]
    pub const fn new(slot: u32, generation: u32) -> Self {
        Self((slot as u64) | ((generation as u64) << 32))
    }

    #[inline]
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    #[inline]
    pub const fn slot(self) -> u32 {
        self.0 as u32
    }

    #[inline]
    pub const fn generation(self) -> u32 {
        (self.0 >> 32) as u32
    }

    #[inline]
    pub const fn raw(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct ApplicationConnectionId(u64);

impl ApplicationConnectionId {
    #[inline]
    pub const fn new(slot: u32, generation: u32) -> Self {
        Self((slot as u64) | ((generation as u64) << 32))
    }

    #[inline]
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    #[inline]
    pub const fn slot(self) -> u32 {
        self.0 as u32
    }

    #[inline]
    pub const fn generation(self) -> u32 {
        (self.0 >> 32) as u32
    }

    #[inline]
    pub const fn raw(self) -> u64 {
        self.0
    }
}
