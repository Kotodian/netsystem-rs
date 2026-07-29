//! Statically typed, synchronous cryptographic exchanges.

use super::Engine;

/// One protocol-owned cryptographic negotiation.
pub trait Protocol<C>: Sized {
    /// Caller-supplied parameters used to begin one exchange.
    type Parameters;
    /// Protocol-private state carried between transitions.
    type State;
    /// Protocol-specific result produced after authentication and establishment.
    type Established;
    /// Protocol-specific rejection or transition failure.
    type Error;

    /// Starts an exchange and writes its first protocol message to caller-owned memory.
    fn start(
        &mut self,
        engine: &Engine,
        parameters: Self::Parameters,
        crypto: &mut C,
        output: &mut [u8],
    ) -> Result<(Self::State, usize), Self::Error>;

    /// Consumes one peer message and advances the protocol synchronously.
    fn advance(
        &mut self,
        engine: &Engine,
        state: Self::State,
        crypto: &mut C,
        peer_input: &[u8],
        output: &mut [u8],
    ) -> Result<Transition<Self::State, Self::Established>, Self::Error>;
}

/// The result of one synchronous Exchange transition.
#[derive(Debug, Eq, PartialEq)]
pub enum Transition<S, E> {
    /// The protocol produced another message and retained typed state.
    Continue {
        /// State consumed by the next transition.
        state: S,
        /// Bytes initialized in caller-owned output.
        written: usize,
    },
    /// The protocol completed and produced its typed established result.
    Established {
        /// Protocol-specific established result.
        result: E,
        /// Bytes initialized in caller-owned output.
        written: usize,
    },
}

/// One protocol-typed cryptographic exchange.
///
/// A protocol that owns thread-bound cryptographic state carries that state in
/// `P`, `P::State`, or `C`. The Main Thread owner, rather than a marker field,
/// keeps handshake exchanges on the Main Thread.
pub struct Exchange<P, C>
where
    P: Protocol<C>,
{
    protocol: P,
    state: P::State,
    crypto: C,
}

impl Engine {
    /// Starts one protocol-typed exchange over caller-owned output memory.
    pub fn start_exchange<P, C>(
        &self,
        mut protocol: P,
        parameters: P::Parameters,
        mut crypto: C,
        output: &mut [u8],
    ) -> Result<(Exchange<P, C>, usize), P::Error>
    where
        P: Protocol<C>,
    {
        let (state, written) = protocol.start(self, parameters, &mut crypto, output)?;
        Ok((
            Exchange {
                protocol,
                state,
                crypto,
            },
            written,
        ))
    }

    /// Advances one exchange and returns either its next typed state or established result.
    pub fn advance_exchange<P, C>(
        &self,
        mut exchange: Exchange<P, C>,
        peer_input: &[u8],
        output: &mut [u8],
    ) -> Result<Transition<Exchange<P, C>, P::Established>, P::Error>
    where
        P: Protocol<C>,
    {
        match exchange.protocol.advance(
            self,
            exchange.state,
            &mut exchange.crypto,
            peer_input,
            output,
        )? {
            Transition::Continue { state, written } => {
                exchange.state = state;
                Ok(Transition::Continue {
                    state: exchange,
                    written,
                })
            }
            Transition::Established { result, written } => {
                Ok(Transition::Established { result, written })
            }
        }
    }
}
