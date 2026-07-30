use std::sync::Arc;

use hammer_runtime::RuntimeResult;
use hammer_runtime::app::{AppSession, AppSessionProtocol};

/// Session Runtime's statically dispatched protocol-chain I/O contract.
///
/// Ingress always runs lower protocols before upper protocols. Egress always
/// runs upper protocols before lower protocols. This trait is `Sized`, so a
/// chain cannot be erased behind a protocol trait object.
pub trait ProtocolChainIo: Sized {
    fn ingress(&mut self) -> RuntimeResult<()>;

    fn egress(&mut self) -> RuntimeResult<()>;

    fn app_session(&self) -> &Arc<AppSession>;
}

/// One App Session protocol chain fixed when its connection is created.
///
/// The chain owns the App-facing session, one concrete protocol state, and the
/// complete lower chain. Session Runtime schedules the chain through
/// [`ProtocolChainIo`] without inspecting or erasing its protocol state.
#[derive(Debug)]
pub struct ProtocolChain<P, L = ()> {
    app_session: Arc<AppSession>,
    protocol: P,
    lower: L,
}

impl<P, L> ProtocolChain<P, L> {
    #[inline]
    pub const fn new(app_session: Arc<AppSession>, protocol: P, lower: L) -> Self {
        Self {
            app_session,
            protocol,
            lower,
        }
    }

    #[inline]
    pub fn app_session(&self) -> &Arc<AppSession> {
        &self.app_session
    }
}

impl ProtocolChain<()> {
    /// Starts a chain at the App Session adjacent to its Transport.
    #[inline]
    pub const fn transport(app_session: Arc<AppSession>) -> Self {
        Self {
            app_session,
            protocol: (),
            lower: (),
        }
    }
}

impl ProtocolChainIo for ProtocolChain<()> {
    #[inline]
    fn ingress(&mut self) -> RuntimeResult<()> {
        Ok(())
    }

    #[inline]
    fn egress(&mut self) -> RuntimeResult<()> {
        Ok(())
    }

    #[inline]
    fn app_session(&self) -> &Arc<AppSession> {
        &self.app_session
    }
}

impl<P, L> ProtocolChainIo for ProtocolChain<P, L>
where
    P: AppSessionProtocol,
    L: ProtocolChainIo,
{
    #[inline]
    fn ingress(&mut self) -> RuntimeResult<()> {
        loop {
            self.lower.ingress()?;
            let lower_session = self.lower.app_session();
            let (source_consumed, destination_produced) = self
                .protocol
                .ingress(lower_session.rx_fifo(), self.app_session.rx_fifo())?;
            lower_session.publish_rx_dequeue(source_consumed);
            self.app_session.publish_rx_enqueue(destination_produced)?;
            if source_consumed == 0 && destination_produced == 0 {
                return Ok(());
            }
        }
    }

    #[inline]
    fn egress(&mut self) -> RuntimeResult<()> {
        loop {
            let lower_session = self.lower.app_session();
            let lower_pending_before = lower_session.tx_fifo().max_dequeue();
            let (source_consumed, destination_produced) = self
                .protocol
                .egress(self.app_session.tx_fifo(), lower_session.tx_fifo())?;
            self.app_session.publish_tx_dequeue(source_consumed)?;
            lower_session.publish_tx_enqueue(destination_produced)?;
            self.lower.egress()?;
            let lower_pending_after = self.lower.app_session().tx_fifo().max_dequeue();
            if source_consumed == 0
                && destination_produced == 0
                && lower_pending_after >= lower_pending_before
            {
                return Ok(());
            }
        }
    }

    #[inline]
    fn app_session(&self) -> &Arc<AppSession> {
        &self.app_session
    }
}
