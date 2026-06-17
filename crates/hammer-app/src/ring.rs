use hammer_core::error::HammerResult;
use hammer_runtime::app as runtime_app;

pub type AppCompletionEntry = runtime_app::AppCompletionEntry;
pub type AppCqe = runtime_app::AppCqe;
pub type AppCqeData = runtime_app::AppCqeData;
pub type AppCqeDescriptor = runtime_app::AppCqeDescriptor;
pub type AppCqeFlags = runtime_app::AppCqeFlags;
pub type AppCqeKind = runtime_app::AppCqeKind;
pub type AppDataAddr = runtime_app::AppDataAddr;
pub type AppDataArea = runtime_app::AppDataArea;
pub type AppDataAreaConfig = runtime_app::AppDataAreaConfig;
pub type AppObjectRef = runtime_app::AppObjectRef;
pub type AppOpId = runtime_app::AppOpId;
pub type AppOpcode = runtime_app::AppOpcode;
pub type AppRecv = runtime_app::AppRecv;
pub type AppRingExport = runtime_app::AppRingExport;
pub type AppRingHandle = runtime_app::AppRingHandle;
pub type AppRingIpcReservation = runtime_app::AppRingIpcReservation;
pub type AppRingLayout = runtime_app::AppRingLayout;
pub type AppRingMemoryKind = runtime_app::AppRingMemoryKind;
pub type AppSend = runtime_app::AppSend;
pub type AppSqe = runtime_app::AppSqe;
pub type AppSqeData = runtime_app::AppSqeData;
pub type AppSqeDescriptor = runtime_app::AppSqeDescriptor;
pub type AppSubmissionEntry = runtime_app::AppSubmissionEntry;
pub type AppUserData = runtime_app::AppUserData;

#[derive(Clone)]
pub struct AppRing {
    inner: runtime_app::AppRuntime,
}

impl AppRing {
    #[inline]
    pub fn new(inner: runtime_app::AppRuntime) -> Self {
        Self { inner }
    }

    #[inline]
    pub fn runtime(&self) -> &runtime_app::AppRuntime {
        &self.inner
    }

    #[inline]
    pub fn recv(&self) -> runtime_app::AppRecvFuture {
        self.inner.recv()
    }

    #[inline]
    pub async fn send(&self, send: AppSend) -> HammerResult<()> {
        self.inner.send(send).await
    }

    #[inline]
    pub fn send_from_bytes(&self, bytes: &[u8]) -> HammerResult<AppSend> {
        self.inner.send_from_bytes(bytes)
    }

    #[inline]
    pub fn read_data(&self, data: AppDataAddr) -> HammerResult<std::vec::Vec<u8>> {
        Ok(self.inner.read_data(data)?.as_slice().to_vec())
    }

    #[inline]
    pub fn try_push_submission_descriptor(&self, descriptor: AppSqeDescriptor) -> HammerResult<()> {
        self.inner.try_push_submission_descriptor(descriptor)
    }

    #[inline]
    pub fn try_push_submission(&self, sqe: AppSqe) -> HammerResult<()> {
        self.inner.try_push_submission(sqe)
    }

    #[inline]
    pub fn try_push_submission_entry(&self, entry: AppSubmissionEntry) -> HammerResult<()> {
        self.inner.try_push_submission_entry(entry)
    }

    #[inline]
    pub async fn next_submission_descriptor(&self) -> Option<AppSqeDescriptor> {
        self.inner.next_submission_descriptor().await
    }

    #[inline]
    pub fn try_pop_submission_descriptor(&self) -> Option<AppSqeDescriptor> {
        self.inner.try_pop_submission_descriptor()
    }

    #[inline]
    pub async fn next_submission_entry(&self) -> Option<AppSubmissionEntry> {
        self.inner.next_submission_entry().await
    }

    #[inline]
    pub fn try_pop_submission_entry(&self) -> Option<AppSubmissionEntry> {
        self.inner.try_pop_submission_entry()
    }

    #[inline]
    pub async fn next_send(&self) -> Option<AppSend> {
        self.inner.next_send().await
    }

    #[inline]
    pub fn try_push_completion_descriptor(&self, descriptor: AppCqeDescriptor) -> HammerResult<()> {
        self.inner.try_push_completion_descriptor(descriptor)
    }

    #[inline]
    pub fn try_push_completion_entry(&self, entry: AppCompletionEntry) -> HammerResult<()> {
        self.inner.try_push_completion_entry(entry)
    }

    #[inline]
    pub async fn next_completion_descriptor(&self) -> Option<AppCqeDescriptor> {
        self.inner.next_completion_descriptor().await
    }

    #[inline]
    pub async fn next_completion_entry(&self) -> Option<AppCompletionEntry> {
        self.inner.next_completion_entry().await
    }

    #[inline]
    pub async fn next_completion(&self) -> Option<AppCqe> {
        self.inner.next_completion().await
    }
}
