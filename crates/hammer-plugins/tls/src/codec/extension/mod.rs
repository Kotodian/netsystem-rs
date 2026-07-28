//! TLS extension bodies remain independent of the handshake messages that
//! carry them. Protocol state requests concrete extension types and never
//! dispatches through a central extension enum.

pub(crate) mod key_share;
pub(crate) mod signature_algorithms;
pub(crate) mod supported_versions;

mod framing;

pub(crate) use framing::ExtensionError;
pub(crate) use framing::{find, validate};

pub(crate) trait Extension<'a>: Sized {
    type Error;

    const TYPE: u16;

    fn decode_body(bytes: &'a [u8]) -> Result<Self, Self::Error>;

    fn body_len(&self) -> usize;

    fn encode_body(&self, output: &mut [u8]);
}
