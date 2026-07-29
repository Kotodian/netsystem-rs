use std::sync::Arc;

use hammer_infra::fifo::Fifo;
use hammer_runtime::RuntimeResult;
use hammer_runtime::app::AppSession;

/// The FIFO positions advanced by one successful protocol operation.
///
/// A protocol reports these exact values after it commits its destination FIFO
/// and consumes its source FIFO. The chain uses them to publish the matching
/// App Session events without giving the protocol access to either session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub struct ProtocolFifoAdvance {
    pub source_consumed: usize,
    pub destination_produced: usize,
}

impl ProtocolFifoAdvance {
    #[inline]
    pub const fn new(source_consumed: usize, destination_produced: usize) -> Self {
        Self {
            source_consumed,
            destination_produced,
        }
    }
}

/// One concrete App Session protocol in a statically composed chain.
///
/// The protocol owns only its connection-local protocol state. It receives
/// borrows of the two adjacent FIFOs for each operation and cannot access an
/// [`AppSession`], another protocol, or a concrete Transport through this
/// interface.
///
/// Implementations borrow source segments with [`Fifo::peek_segments`] and
/// transform them directly into a [`Fifo::reserve_write`] reservation. They
/// must commit the complete destination output before consuming the source.
/// Returning an error must leave both visible FIFO positions unchanged.
pub trait AppSessionProtocol: Sized {
    /// Transforms lower-session ingress into upper-session ingress.
    fn ingress(
        &mut self,
        lower_rx_fifo: &Fifo,
        upper_rx_fifo: &Fifo,
    ) -> RuntimeResult<ProtocolFifoAdvance>;

    /// Transforms upper-session egress into lower-session egress.
    fn egress(
        &mut self,
        upper_tx_fifo: &Fifo,
        lower_tx_fifo: &Fifo,
    ) -> RuntimeResult<ProtocolFifoAdvance>;
}

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
    /// Starts a plaintext chain with the existing App Session itself.
    ///
    /// No additional session, forwarding protocol, or protocol state is
    /// created.
    #[inline]
    pub const fn plaintext(app_session: Arc<AppSession>) -> Self {
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
        self.lower.ingress()?;
        let lower_session = self.lower.app_session();
        let advance = self
            .protocol
            .ingress(lower_session.rx_fifo(), self.app_session.rx_fifo())?;
        lower_session.publish_rx_dequeue(advance.source_consumed);
        self.app_session
            .publish_rx_enqueue(advance.destination_produced)?;
        Ok(())
    }

    #[inline]
    fn egress(&mut self) -> RuntimeResult<()> {
        let lower_session = self.lower.app_session();
        let advance = self
            .protocol
            .egress(self.app_session.tx_fifo(), lower_session.tx_fifo())?;
        self.app_session
            .publish_tx_dequeue(advance.source_consumed)?;
        lower_session.publish_tx_enqueue(advance.destination_produced)?;
        self.lower.egress()
    }

    #[inline]
    fn app_session(&self) -> &Arc<AppSession> {
        &self.app_session
    }
}
