use crate::error::HammerError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartStage {
    Initialize,
    Start,
    PostStart,
    Started,
}

impl StartStage {
    pub fn name(self) -> &'static str {
        match self {
            StartStage::Initialize => "initialize",
            StartStage::Start => "start",
            StartStage::PostStart => "post-start",
            StartStage::Started => "started",
        }
    }
}

pub const ALL_STAGES: [StartStage; 4] = [
    StartStage::Initialize,
    StartStage::Start,
    StartStage::PostStart,
    StartStage::Started,
];

/// Order MUST match `hammer/service.go` `lifecycles` slice in the Go reference.
/// Reordering will silently break dependencies (e.g. network manager must
/// initialize before dns-transport opens its first socket).
pub const LIFECYCLE_ORDER: [&str; 11] = [
    "certificate-store",
    "certificate-provider",
    "endpoint",
    "network",
    "dns-transport",
    "outbound",
    "dns-router",
    "router",
    "inbound",
    "service",
    "connection",
];

pub trait Lifecycle: Send + Sync {
    fn name(&self) -> &str;
    fn start(&self, stage: StartStage) -> Result<(), HammerError>;
    fn close(&self) -> Result<(), HammerError>;
}

/// `adapter.LifecycleService` — 1:1 with Lifecycle in Go but historically used
/// to mark managers that show up in the ServiceManager / CertificateStore
/// slots. Kept as a marker alias so trait bounds across crates can express
/// "this is a service that participates in the start/close orchestration"
/// without coupling to a specific manager kind.
pub trait LifecycleService: Lifecycle {}

impl<T: Lifecycle + ?Sized> LifecycleService for T {}
