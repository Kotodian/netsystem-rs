use std::collections::TryReserveError;

use hammer_service::crypto::{
    AlgorithmId, Context, ContextError, Engine, Hash, HashOperation, Input,
};
use thiserror::Error;

const MAX_TRANSCRIPT_LEN: usize = 32 * 1024 * 1024;
const MAX_TRANSCRIPT_HASH_LEN: usize = 48;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TranscriptHash {
    pub(crate) bytes: [u8; MAX_TRANSCRIPT_HASH_LEN],
    pub(crate) len: usize,
}

impl TranscriptHash {
    pub(crate) fn as_slice(&self) -> &[u8] {
        &self.bytes[..self.len]
    }
}

pub(crate) struct Transcript {
    bytes: Vec<u8>,
    hash: Context<Hash>,
}

impl Transcript {
    pub(super) fn new(
        engine: &Engine,
        algorithm: AlgorithmId<Hash>,
    ) -> Result<Self, TranscriptError> {
        let hash = engine
            .context(algorithm)
            .map_err(|source| TranscriptError::Context { source })?;
        Ok(Self {
            bytes: Vec::new(),
            hash,
        })
    }

    pub(super) fn append(&mut self, message: &[u8]) -> Result<(), TranscriptError> {
        let required =
            self.bytes
                .len()
                .checked_add(message.len())
                .ok_or(TranscriptError::Capacity {
                    required: usize::MAX,
                    limit: MAX_TRANSCRIPT_LEN,
                })?;
        if required > MAX_TRANSCRIPT_LEN {
            return Err(TranscriptError::Capacity {
                required,
                limit: MAX_TRANSCRIPT_LEN,
            });
        }
        self.bytes
            .try_reserve(message.len())
            .map_err(|source| TranscriptError::Allocation {
                requested: message.len(),
                source,
            })?;
        self.bytes.extend_from_slice(message);
        Ok(())
    }

    pub(super) fn hash(&mut self) -> Result<TranscriptHash, TranscriptError> {
        let mut bytes = [0u8; MAX_TRANSCRIPT_HASH_LEN];
        let mut operations = [HashOperation::new(
            Input::Contiguous(&self.bytes),
            &mut bytes,
        )];
        self.hash
            .execute(&mut operations)
            .map_err(|source| TranscriptError::Context { source })?;
        let len = match operations[0].status() {
            Some(Ok(len @ (32 | 48))) => len,
            Some(Ok(len)) => return Err(TranscriptError::DigestLength { len }),
            Some(Err(source)) => return Err(TranscriptError::Hash { source }),
            None => panic!("synchronous Crypto Hash Context must complete every operation"),
        };
        Ok(TranscriptHash { bytes, len })
    }
}

#[derive(Debug, Error)]
pub(crate) enum TranscriptError {
    #[error("TLS transcript requires {required} bytes, exceeding the {limit}-byte limit")]
    Capacity { required: usize, limit: usize },
    #[error("TLS transcript could not reserve {requested} bytes")]
    Allocation {
        requested: usize,
        #[source]
        source: TryReserveError,
    },
    #[error("TLS transcript Crypto Hash Context failed")]
    Context {
        #[source]
        source: ContextError,
    },
    #[error("TLS transcript hash failed")]
    Hash {
        #[source]
        source: hammer_infra::crypto::hash::Error,
    },
    #[error("TLS transcript hash produced unsupported {len}-byte output")]
    DigestLength { len: usize },
}

#[cfg(test)]
mod tests {
    use hammer_infra::crypto::InstructionSet;

    use super::*;

    #[test]
    fn sha256_hashes_ordered_handshake_messages_through_crypto_engine() {
        let engine =
            Engine::with_builtins(InstructionSet::empty()).expect("built-in Crypto Engine");
        let algorithm = engine
            .algorithm::<Hash>("sha-256")
            .expect("SHA-256 algorithm");
        let mut transcript = Transcript::new(&engine, algorithm).expect("transcript");
        transcript.append(b"first").expect("first message");
        transcript.append(b"second").expect("second message");

        let hash = transcript.hash().expect("transcript hash");

        assert_eq!(hash.len, 32);
        assert_eq!(
            &hash.bytes[..hash.len],
            &[
                0xda, 0x83, 0xf6, 0x3e, 0x1a, 0x47, 0x30, 0x03, 0x71, 0x2c, 0x18, 0xf5, 0xaf, 0xc5,
                0xa7, 0x90, 0x44, 0x22, 0x19, 0x43, 0xd1, 0x08, 0x3c, 0x7c, 0x5a, 0x7a, 0xc7, 0x23,
                0x6d, 0x85, 0xe8, 0xd2,
            ]
        );
    }
}
