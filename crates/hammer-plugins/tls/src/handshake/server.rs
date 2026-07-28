use core::mem::size_of;

use hammer_service::crypto::exchange::{Protocol, Transition};
use hammer_service::crypto::{
    AlgorithmId, Context, ContextError, Engine, Hash, Input, Kdf, KeyHandle, Kx, KxOperation,
    KxStatus, Mac, MacOperation, Sign, SignOperation,
};
use thiserror::Error;

use crate::codec::extension;
use crate::codec::extension::key_share::{
    KeyShareError, OfferedKeyShares, SelectedKeyShare, X25519,
};
use crate::codec::extension::signature_algorithms::{
    ED25519, SignatureAlgorithms, SignatureAlgorithmsError,
};
use crate::codec::extension::supported_versions::{
    SupportedVersions, SupportedVersionsError, TLS_1_3,
};
use crate::codec::{
    Certificate, CertificateError, CertificateVerify, CertificateVerifyError, ClientHello,
    EncryptedExtensions, EncryptedExtensionsError, HandshakeError, HandshakeHeader,
    HandshakeMessage, HandshakeType, HelloError, ServerHello,
};

use super::transcript::{Transcript, TranscriptError, TranscriptHash};
use super::{
    CERTIFICATE_VERIFY_PADDING, CERTIFICATE_VERIFY_SEPARATOR, MAX_FINISHED_LEN,
    SERVER_CERTIFICATE_VERIFY_CONTEXT, authenticators_equal,
};

pub(crate) struct ServerHandshake<'a> {
    hello: ServerHello<'a>,
    encrypted_extensions: EncryptedExtensions<'a>,
    certificate: Certificate<'a>,
    hash: AlgorithmId<Hash>,
}

impl<'a> ServerHandshake<'a> {
    pub(crate) const fn new(
        hello: ServerHello<'a>,
        encrypted_extensions: EncryptedExtensions<'a>,
        certificate: Certificate<'a>,
        hash: AlgorithmId<Hash>,
    ) -> Self {
        Self {
            hello,
            encrypted_extensions,
            certificate,
            hash,
        }
    }
}

pub(crate) enum ServerState {
    AwaitClientHello {
        transcript: Transcript,
    },
    AwaitClientFinished {
        transcript: Transcript,
        client_random: [u8; 32],
        client_key_exchange: [u8; 32],
    },
}

pub(crate) struct ServerCrypto {
    key_exchange: Context<Kx>,
    private_key: KeyHandle,
    shared_secret_target: AlgorithmId<Kdf>,
    sign: Context<Sign>,
    server_finished: Context<Mac>,
    client_finished: Context<Mac>,
}

impl ServerCrypto {
    pub(crate) fn new(
        key_exchange: Context<Kx>,
        private_key: KeyHandle,
        shared_secret_target: AlgorithmId<Kdf>,
        sign: Context<Sign>,
        server_finished: Context<Mac>,
        client_finished: Context<Mac>,
    ) -> Self {
        Self {
            key_exchange,
            private_key,
            shared_secret_target,
            sign,
            server_finished,
            client_finished,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ServerEstablished {
    pub(crate) shared_secret: KeyHandle,
    pub(crate) client_random: [u8; 32],
    pub(crate) cipher_suite: [u8; 2],
    pub(crate) transcript_hash: TranscriptHash,
}

impl Protocol<ServerCrypto> for ServerHandshake<'_> {
    type Parameters = ();
    type State = ServerState;
    type Established = ServerEstablished;
    type Error = ServerError;

    fn start(
        &mut self,
        engine: &Engine,
        _: (),
        _: &mut ServerCrypto,
        _: &mut [u8],
    ) -> Result<(Self::State, usize), Self::Error> {
        let transcript = Transcript::new(engine, self.hash)
            .map_err(|source| ServerError::Transcript { source })?;
        Ok((ServerState::AwaitClientHello { transcript }, 0))
    }

    fn advance(
        &mut self,
        _: &Engine,
        state: Self::State,
        crypto: &mut ServerCrypto,
        peer_input: &[u8],
        output: &mut [u8],
    ) -> Result<Transition<Self::State, Self::Established>, Self::Error> {
        let mut transcript = match state {
            ServerState::AwaitClientHello { transcript } => transcript,
            ServerState::AwaitClientFinished {
                mut transcript,
                client_random,
                client_key_exchange,
            } => {
                let finished_hash = transcript
                    .hash()
                    .map_err(|source| ServerError::Transcript { source })?;
                let (message, consumed) = HandshakeMessage::decode(peer_input)
                    .map_err(|source| ServerError::Message { source })?;
                if consumed != peer_input.len() {
                    return Err(ServerError::TrailingData {
                        trailing: peer_input.len() - consumed,
                    });
                }
                if message.handshake_type() != HandshakeType::Finished {
                    return Err(ServerError::UnexpectedMessage {
                        expected: HandshakeType::Finished,
                        actual: message.handshake_type(),
                    });
                }
                let mut expected_finished = [0u8; MAX_FINISHED_LEN];
                let expected_len = {
                    let mut authentication = [MacOperation::authenticate(
                        Input::Contiguous(finished_hash.as_slice()),
                        &mut expected_finished,
                    )];
                    crypto
                        .client_finished
                        .execute(&mut authentication)
                        .map_err(|source| ServerError::CryptoContext { source })?;
                    match authentication[0].status() {
                        Some(Ok(len)) => len,
                        Some(Err(source)) => return Err(ServerError::FinishedMac { source }),
                        None => panic!("synchronous TLS Finished MAC operation must complete"),
                    }
                };
                if !authenticators_equal(&expected_finished[..expected_len], message.body()) {
                    return Err(ServerError::ClientFinishedAuthentication);
                }
                transcript
                    .append(peer_input)
                    .map_err(|source| ServerError::Transcript { source })?;
                let transcript_hash = transcript
                    .hash()
                    .map_err(|source| ServerError::Transcript { source })?;
                let shared_secret = {
                    let mut agreement = [KxOperation::agree(
                        crypto.private_key,
                        &client_key_exchange,
                        crypto.shared_secret_target,
                    )];
                    crypto
                        .key_exchange
                        .execute(&mut agreement)
                        .map_err(|source| ServerError::CryptoContext { source })?;
                    match agreement[0].status() {
                        KxStatus::SharedSecret { key } => key,
                        status => return Err(ServerError::KeyAgreement { status }),
                    }
                };
                return Ok(Transition::Established {
                    result: ServerEstablished {
                        shared_secret,
                        client_random,
                        cipher_suite: self.hello.cipher_suite,
                        transcript_hash,
                    },
                    written: 0,
                });
            }
        };
        let (message, consumed) = HandshakeMessage::decode(peer_input)
            .map_err(|source| ServerError::Message { source })?;
        if consumed != peer_input.len() {
            return Err(ServerError::TrailingData {
                trailing: peer_input.len() - consumed,
            });
        }
        if message.handshake_type() != HandshakeType::ClientHello {
            return Err(ServerError::UnexpectedMessage {
                expected: HandshakeType::ClientHello,
                actual: message.handshake_type(),
            });
        }
        let client_hello =
            ClientHello::decode(message.body()).map_err(|source| ServerError::Hello { source })?;
        let supported_versions = extension::find::<SupportedVersions>(client_hello.extensions)
            .map_err(|source| ServerError::SupportedVersions { source })?
            .ok_or(ServerError::SupportedVersionsMissing)?;
        if !supported_versions.contains(TLS_1_3) {
            return Err(ServerError::Tls13NotOffered);
        }
        if client_hello.session_id != self.hello.session_id {
            return Err(ServerError::SessionIdMismatch);
        }
        if !client_hello
            .cipher_suites
            .chunks_exact(2)
            .any(|cipher_suite| cipher_suite == self.hello.cipher_suite)
        {
            return Err(ServerError::CipherSuiteNotOffered {
                cipher_suite: self.hello.cipher_suite,
            });
        }
        let key_shares = extension::find::<OfferedKeyShares>(client_hello.extensions)
            .map_err(|source| ServerError::KeyShare { source })?
            .ok_or(ServerError::KeyShareMissing)?;
        let client_key_exchange = key_shares
            .key_exchange(X25519)
            .map_err(|source| ServerError::KeyShare { source })?
            .ok_or(ServerError::X25519Missing)?;
        let client_key_exchange =
            <[u8; 32]>::try_from(client_key_exchange).map_err(|_| ServerError::X25519Length {
                length: client_key_exchange.len(),
            })?;
        let signature_algorithms = extension::find::<SignatureAlgorithms>(client_hello.extensions)
            .map_err(|source| ServerError::SignatureAlgorithms { source })?
            .ok_or(ServerError::SignatureAlgorithmsMissing)?;
        if !signature_algorithms.contains(ED25519) {
            return Err(ServerError::Ed25519NotOffered);
        }
        let selected_key_share = extension::find::<SelectedKeyShare>(self.hello.extensions)
            .map_err(|source| ServerError::KeyShare { source })?
            .ok_or(ServerError::ServerKeyShareMissing)?;
        if selected_key_share.group() != X25519 {
            return Err(ServerError::ServerKeyShareGroup {
                group: selected_key_share.group(),
            });
        }
        transcript
            .append(peer_input)
            .map_err(|source| ServerError::Transcript { source })?;

        let header_len = size_of::<HandshakeHeader>();
        let mut offset = 0;
        let available = output.len();
        let body_len = self
            .hello
            .encode(
                output
                    .get_mut(header_len..)
                    .ok_or(ServerError::OutputTooSmall {
                        required: header_len,
                        available,
                    })?,
            )
            .map_err(|source| ServerError::Hello { source })?;
        HandshakeHeader::new(HandshakeType::ServerHello, body_len)
            .and_then(|header| header.encode(output))
            .map_err(|source| ServerError::Message { source })?;
        offset += header_len + body_len;
        transcript
            .append(&output[..offset])
            .map_err(|source| ServerError::Transcript { source })?;

        let message_start = offset;
        let body_start = message_start + header_len;
        let available = output.len();
        let body_len = self
            .encrypted_extensions
            .encode(
                output
                    .get_mut(body_start..)
                    .ok_or(ServerError::OutputTooSmall {
                        required: body_start,
                        available,
                    })?,
            )
            .map_err(|source| ServerError::EncryptedExtensions { source })?;
        HandshakeHeader::new(HandshakeType::EncryptedExtensions, body_len)
            .and_then(|header| header.encode(&mut output[message_start..]))
            .map_err(|source| ServerError::Message { source })?;
        offset = body_start + body_len;
        transcript
            .append(&output[message_start..offset])
            .map_err(|source| ServerError::Transcript { source })?;

        let message_start = offset;
        let body_start = message_start + header_len;
        let available = output.len();
        let body_len = self
            .certificate
            .encode(
                output
                    .get_mut(body_start..)
                    .ok_or(ServerError::OutputTooSmall {
                        required: body_start,
                        available,
                    })?,
            )
            .map_err(|source| ServerError::Certificate { source })?;
        HandshakeHeader::new(HandshakeType::Certificate, body_len)
            .and_then(|header| header.encode(&mut output[message_start..]))
            .map_err(|source| ServerError::Message { source })?;
        offset = body_start + body_len;
        transcript
            .append(&output[message_start..offset])
            .map_err(|source| ServerError::Transcript { source })?;

        let certificate_verify_hash = transcript
            .hash()
            .map_err(|source| ServerError::Transcript { source })?;
        let signed: &[&[u8]] = &[
            &CERTIFICATE_VERIFY_PADDING,
            SERVER_CERTIFICATE_VERIFY_CONTEXT,
            &CERTIFICATE_VERIFY_SEPARATOR,
            certificate_verify_hash.as_slice(),
        ];
        let mut signature = [0u8; 64];
        let signature_len = {
            let mut signing = [SignOperation::sign(Input::Scatter(signed), &mut signature)];
            crypto
                .sign
                .execute(&mut signing)
                .map_err(|source| ServerError::CryptoContext { source })?;
            match signing[0].status() {
                Some(Ok(len)) => len,
                Some(Err(source)) => return Err(ServerError::CertificateSigning { source }),
                None => panic!("synchronous TLS CertificateVerify signing must complete"),
            }
        };
        let certificate_verify = CertificateVerify::new(ED25519, &signature[..signature_len]);
        let message_start = offset;
        let body_start = message_start + header_len;
        let available = output.len();
        let body_len = certificate_verify
            .encode(
                output
                    .get_mut(body_start..)
                    .ok_or(ServerError::OutputTooSmall {
                        required: body_start,
                        available,
                    })?,
            )
            .map_err(|source| ServerError::CertificateVerify { source })?;
        HandshakeHeader::new(HandshakeType::CertificateVerify, body_len)
            .and_then(|header| header.encode(&mut output[message_start..]))
            .map_err(|source| ServerError::Message { source })?;
        offset = body_start + body_len;
        transcript
            .append(&output[message_start..offset])
            .map_err(|source| ServerError::Transcript { source })?;

        let finished_hash = transcript
            .hash()
            .map_err(|source| ServerError::Transcript { source })?;
        let mut finished = [0u8; MAX_FINISHED_LEN];
        let finished_len = {
            let mut authentication = [MacOperation::authenticate(
                Input::Contiguous(finished_hash.as_slice()),
                &mut finished,
            )];
            crypto
                .server_finished
                .execute(&mut authentication)
                .map_err(|source| ServerError::CryptoContext { source })?;
            match authentication[0].status() {
                Some(Ok(len)) => len,
                Some(Err(source)) => return Err(ServerError::FinishedMac { source }),
                None => panic!("synchronous TLS Finished MAC operation must complete"),
            }
        };
        let message_start = offset;
        let body_start = message_start + header_len;
        let written = body_start + finished_len;
        if output.len() < written {
            return Err(ServerError::OutputTooSmall {
                required: written,
                available: output.len(),
            });
        }
        HandshakeHeader::new(HandshakeType::Finished, finished_len)
            .and_then(|header| header.encode(&mut output[message_start..]))
            .map_err(|source| ServerError::Message { source })?;
        output[body_start..written].copy_from_slice(&finished[..finished_len]);
        transcript
            .append(&output[message_start..written])
            .map_err(|source| ServerError::Transcript { source })?;

        Ok(Transition::Continue {
            state: ServerState::AwaitClientFinished {
                transcript,
                client_random: client_hello.random,
                client_key_exchange,
            },
            written,
        })
    }
}

#[derive(Debug, Error)]
pub(crate) enum ServerError {
    #[error("TLS Server handshake message failed")]
    Message {
        #[source]
        source: HandshakeError,
    },
    #[error("TLS Server hello failed")]
    Hello {
        #[source]
        source: HelloError,
    },
    #[error("TLS ClientHello supported_versions extension failed")]
    SupportedVersions {
        #[source]
        source: SupportedVersionsError,
    },
    #[error("TLS ClientHello key_share extension failed")]
    KeyShare {
        #[source]
        source: KeyShareError,
    },
    #[error("TLS ClientHello signature_algorithms extension failed")]
    SignatureAlgorithms {
        #[source]
        source: SignatureAlgorithmsError,
    },
    #[error("TLS EncryptedExtensions failed")]
    EncryptedExtensions {
        #[source]
        source: EncryptedExtensionsError,
    },
    #[error("TLS Certificate failed")]
    Certificate {
        #[source]
        source: CertificateError,
    },
    #[error("TLS CertificateVerify failed")]
    CertificateVerify {
        #[source]
        source: CertificateVerifyError,
    },
    #[error("TLS Server transcript failed")]
    Transcript {
        #[source]
        source: TranscriptError,
    },
    #[error("TLS Server Crypto Context failed")]
    CryptoContext {
        #[source]
        source: ContextError,
    },
    #[error("TLS CertificateVerify signing failed")]
    CertificateSigning {
        #[source]
        source: hammer_infra::crypto::signature::SignError,
    },
    #[error("TLS Finished MAC failed")]
    FinishedMac {
        #[source]
        source: hammer_infra::crypto::mac::Error,
    },
    #[error("TLS client Finished authentication failed")]
    ClientFinishedAuthentication,
    #[error("TLS key agreement failed with status {status:?}")]
    KeyAgreement { status: KxStatus },
    #[error("TLS Server output requires at least {required} bytes, received {available}")]
    OutputTooSmall { required: usize, available: usize },
    #[error("TLS Server expected {expected:?}, received {actual:?}")]
    UnexpectedMessage {
        expected: HandshakeType,
        actual: HandshakeType,
    },
    #[error("TLS Server handshake input has {trailing} trailing bytes")]
    TrailingData { trailing: usize },
    #[error("TLS ServerHello legacy session id does not echo ClientHello")]
    SessionIdMismatch,
    #[error("TLS Server selected unoffered cipher suite {cipher_suite:02x?}")]
    CipherSuiteNotOffered { cipher_suite: [u8; 2] },
    #[error("TLS ClientHello is missing supported_versions")]
    SupportedVersionsMissing,
    #[error("TLS ClientHello is missing key_share")]
    KeyShareMissing,
    #[error("TLS ClientHello does not offer X25519")]
    X25519Missing,
    #[error("TLS ClientHello X25519 key share must be 32 bytes, received {length}")]
    X25519Length { length: usize },
    #[error("TLS ClientHello is missing signature_algorithms")]
    SignatureAlgorithmsMissing,
    #[error("TLS ClientHello does not offer Ed25519")]
    Ed25519NotOffered,
    #[error("TLS ServerHello is missing key_share")]
    ServerKeyShareMissing,
    #[error("TLS ServerHello selected unsupported key share group {group:02x?}")]
    ServerKeyShareGroup { group: [u8; 2] },
    #[error("TLS ClientHello did not offer TLS 1.3")]
    Tls13NotOffered,
}
