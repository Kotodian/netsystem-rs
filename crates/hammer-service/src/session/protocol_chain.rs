use std::io::{BufRead, Write};
use std::sync::Arc;

use hammer_infra::fifo::Fifo;
use hammer_runtime::RuntimeResult;
use hammer_runtime::app::AppSession;

/// One concrete App Session protocol in a statically composed chain.
///
/// The protocol owns only its connection-local protocol state. It receives
/// borrows of the two adjacent FIFOs for each operation and cannot access an
/// [`AppSession`], another protocol, or a concrete Transport through this
/// interface.
///
/// A successful operation returns the exact number of source bytes it consumed
/// and destination bytes it produced. One call performs at most one ownership
/// transfer. The chain publishes that transfer before calling the protocol
/// again, so buffered protocols may accept source bytes before they can produce
/// bytes for the next layer.
pub trait AppSessionProtocol: Sized {
    /// Transforms lower-session ingress into upper-session ingress.
    fn ingress(
        &mut self,
        lower_rx_fifo: &Fifo,
        upper_rx_fifo: &Fifo,
    ) -> RuntimeResult<(usize, usize)>;

    /// Transforms upper-session egress into lower-session egress.
    fn egress(
        &mut self,
        upper_tx_fifo: &Fifo,
        lower_tx_fifo: &Fifo,
    ) -> RuntimeResult<(usize, usize)>;
}

/// The App Session protocol that transfers bytes without changing them.
#[derive(Debug, Clone, Copy, Default)]
pub struct Plaintext;

impl AppSessionProtocol for Plaintext {
    fn ingress(
        &mut self,
        lower_rx_fifo: &Fifo,
        upper_rx_fifo: &Fifo,
    ) -> RuntimeResult<(usize, usize)> {
        transfer_plaintext(lower_rx_fifo, upper_rx_fifo)
    }

    fn egress(
        &mut self,
        upper_tx_fifo: &Fifo,
        lower_tx_fifo: &Fifo,
    ) -> RuntimeResult<(usize, usize)> {
        transfer_plaintext(upper_tx_fifo, lower_tx_fifo)
    }
}

fn transfer_plaintext(source: &Fifo, destination: &Fifo) -> RuntimeResult<(usize, usize)> {
    if source.max_dequeue() == 0 {
        return Ok((0, 0));
    }
    if destination.max_enqueue() == 0 {
        destination.want_deq_notification();
        if destination.max_enqueue() == 0 {
            return Ok((0, 0));
        }
        destination.clear_deq_notification();
    }
    let mut source = source;
    let bytes = source
        .fill_buf()
        .map_err(|source| hammer_runtime::RuntimeError::subsystem("plaintext", source))?;
    let mut destination = destination;
    let transferred = destination
        .write(bytes)
        .map_err(|source| hammer_runtime::RuntimeError::subsystem("plaintext", source))?;
    source.consume(transferred);
    Ok((transferred, transferred))
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
