use core::mem::size_of;

use hammer_service::crypto::exchange::{Protocol, Transition};
use hammer_service::crypto::{
    AlgorithmId, Context, ContextError, Engine, Hash, Input, Kdf, KeyHandle, Kx, KxOperation,
    KxStatus, Mac, MacOperation, Verify, VerifyOperation,
};
use thiserror::Error;

use crate::codec::extension;
use crate::codec::extension::key_share::{KeyShareError, SelectedKeyShare, X25519};
use crate::codec::extension::signature_algorithms::ED25519;
use crate::codec::extension::supported_versions::{
    SelectedVersion, SupportedVersionsError, TLS_1_3,
};
use crate::codec::{
    Certificate, CertificateError, CertificateVerify, CertificateVerifyError, EncryptedExtensions,
    EncryptedExtensionsError, HandshakeError, HandshakeHeader, HandshakeMessage, HandshakeType,
    HelloError, ServerHello,
};

use super::transcript::{Transcript, TranscriptError, TranscriptHash};
use super::{
    CERTIFICATE_VERIFY_PADDING, CERTIFICATE_VERIFY_SEPARATOR, MAX_FINISHED_LEN,
    SERVER_CERTIFICATE_VERIFY_CONTEXT, authenticators_equal,
};
use crate::codec::ClientHello;

pub(crate) struct ClientHandshake<'a> {
    hello: ClientHello<'a>,
    hash: AlgorithmId<Hash>,
}

impl<'a> ClientHandshake<'a> {
    pub(crate) const fn new(hello: ClientHello<'a>, hash: AlgorithmId<Hash>) -> Self {
        Self { hello, hash }
    }
}

pub(crate) enum ClientState {
    AwaitServerFlight { transcript: Transcript },
}

pub(crate) struct ClientCrypto {
    key_exchange: Context<Kx>,
    private_key: KeyHandle,
    shared_secret_target: AlgorithmId<Kdf>,
    verify: Context<Verify>,
    server_finished: Context<Mac>,
    client_finished: Context<Mac>,
}

impl ClientCrypto {
    pub(crate) fn new(
        key_exchange: Context<Kx>,
        private_key: KeyHandle,
        shared_secret_target: AlgorithmId<Kdf>,
        verify: Context<Verify>,
        server_finished: Context<Mac>,
        client_finished: Context<Mac>,
    ) -> Self {
        Self {
            key_exchange,
            private_key,
            shared_secret_target,
            verify,
            server_finished,
            client_finished,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ClientEstablished {
    pub(crate) shared_secret: KeyHandle,
    pub(crate) server_random: [u8; 32],
    pub(crate) cipher_suite: [u8; 2],
    pub(crate) transcript_hash: TranscriptHash,
}

impl Protocol<ClientCrypto> for ClientHandshake<'_> {
    type Parameters = ();
    type State = ClientState;
    type Established = ClientEstablished;
    type Error = ClientError;

    fn start(
        &mut self,
        engine: &Engine,
        _: (),
        _: &mut ClientCrypto,
        output: &mut [u8],
    ) -> Result<(Self::State, usize), Self::Error> {
        let mut transcript = Transcript::new(engine, self.hash)
            .map_err(|source| ClientError::Transcript { source })?;
        let header_len = size_of::<HandshakeHeader>();
        let available = output.len();
        let body = output
            .get_mut(header_len..)
            .ok_or(ClientError::OutputTooSmall {
                required: header_len,
                available,
            })?;
        let body_len = self
            .hello
            .encode(body)
            .map_err(|source| ClientError::Hello { source })?;
        HandshakeHeader::new(HandshakeType::ClientHello, body_len)
            .and_then(|header| header.encode(output))
            .map_err(|source| ClientError::Message { source })?;
        let written = header_len + body_len;
        transcript
            .append(&output[..written])
            .map_err(|source| ClientError::Transcript { source })?;
        Ok((ClientState::AwaitServerFlight { transcript }, written))
    }

    fn advance(
        &mut self,
        _: &Engine,
        state: Self::State,
        crypto: &mut ClientCrypto,
        peer_input: &[u8],
        output: &mut [u8],
    ) -> Result<Transition<Self::State, Self::Established>, Self::Error> {
        let ClientState::AwaitServerFlight { mut transcript } = state;
        let mut offset = 0;

        let (message, consumed) = HandshakeMessage::decode(&peer_input[offset..])
            .map_err(|source| ClientError::Message { source })?;
        if message.handshake_type() != HandshakeType::ServerHello {
            return Err(ClientError::UnexpectedMessage {
                expected: HandshakeType::ServerHello,
                actual: message.handshake_type(),
            });
        }
        let hello =
            ServerHello::decode(message.body()).map_err(|source| ClientError::Hello { source })?;
        let selected_version = extension::find::<SelectedVersion>(hello.extensions)
            .map_err(|source| ClientError::SupportedVersions { source })?
            .ok_or(ClientError::SupportedVersionsMissing)?;
        if !selected_version.is(TLS_1_3) {
            return Err(ClientError::Tls13NotSelected);
        }
        if hello.session_id != self.hello.session_id {
            return Err(ClientError::SessionIdMismatch);
        }
        if !self
            .hello
            .cipher_suites
            .chunks_exact(2)
            .any(|cipher_suite| cipher_suite == hello.cipher_suite)
        {
            return Err(ClientError::CipherSuiteNotOffered {
                cipher_suite: hello.cipher_suite,
            });
        }
        let selected_key_share = extension::find::<SelectedKeyShare>(hello.extensions)
            .map_err(|source| ClientError::KeyShare { source })?
            .ok_or(ClientError::KeyShareMissing)?;
        if selected_key_share.group() != X25519 {
            return Err(ClientError::KeyShareGroup {
                group: selected_key_share.group(),
            });
        }
        transcript
            .append(&peer_input[offset..offset + consumed])
            .map_err(|source| ClientError::Transcript { source })?;
        offset += consumed;

        let (message, consumed) = HandshakeMessage::decode(&peer_input[offset..])
            .map_err(|source| ClientError::Message { source })?;
        if message.handshake_type() != HandshakeType::EncryptedExtensions {
            return Err(ClientError::UnexpectedMessage {
                expected: HandshakeType::EncryptedExtensions,
                actual: message.handshake_type(),
            });
        }
        EncryptedExtensions::decode(message.body())
            .map_err(|source| ClientError::EncryptedExtensions { source })?;
        transcript
            .append(&peer_input[offset..offset + consumed])
            .map_err(|source| ClientError::Transcript { source })?;
        offset += consumed;

        let (message, consumed) = HandshakeMessage::decode(&peer_input[offset..])
            .map_err(|source| ClientError::Message { source })?;
        if message.handshake_type() != HandshakeType::Certificate {
            return Err(ClientError::UnexpectedMessage {
                expected: HandshakeType::Certificate,
                actual: message.handshake_type(),
            });
        }
        let certificate = Certificate::decode(message.body())
            .map_err(|source| ClientError::Certificate { source })?;
        transcript
            .append(&peer_input[offset..offset + consumed])
            .map_err(|source| ClientError::Transcript { source })?;
        offset += consumed;

        let certificate_verify_hash = transcript
            .hash()
            .map_err(|source| ClientError::Transcript { source })?;
        let (message, consumed) = HandshakeMessage::decode(&peer_input[offset..])
            .map_err(|source| ClientError::Message { source })?;
        if message.handshake_type() != HandshakeType::CertificateVerify {
            return Err(ClientError::UnexpectedMessage {
                expected: HandshakeType::CertificateVerify,
                actual: message.handshake_type(),
            });
        }
        let certificate_verify = CertificateVerify::decode(message.body())
            .map_err(|source| ClientError::CertificateVerify { source })?;
        if certificate_verify.signature_scheme() != ED25519 {
            return Err(ClientError::CertificateSignatureScheme {
                scheme: certificate_verify.signature_scheme(),
            });
        }
        let signed: &[&[u8]] = &[
            &CERTIFICATE_VERIFY_PADDING,
            SERVER_CERTIFICATE_VERIFY_CONTEXT,
            &CERTIFICATE_VERIFY_SEPARATOR,
            certificate_verify_hash.as_slice(),
        ];
        let mut verify = [VerifyOperation::verify(
            certificate.leaf(),
            Input::Scatter(signed),
            certificate_verify.signature(),
        )];
        crypto
            .verify
            .execute(&mut verify)
            .map_err(|source| ClientError::CryptoContext { source })?;
        match verify[0].status() {
            Some(Ok(())) => {}
            Some(Err(source)) => return Err(ClientError::CertificateAuthentication { source }),
            None => panic!("synchronous TLS CertificateVerify operation must complete"),
        }
        transcript
            .append(&peer_input[offset..offset + consumed])
            .map_err(|source| ClientError::Transcript { source })?;
        offset += consumed;

        let finished_hash = transcript
            .hash()
            .map_err(|source| ClientError::Transcript { source })?;
        let (message, consumed) = HandshakeMessage::decode(&peer_input[offset..])
            .map_err(|source| ClientError::Message { source })?;
        if message.handshake_type() != HandshakeType::Finished {
            return Err(ClientError::UnexpectedMessage {
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
                .server_finished
                .execute(&mut authentication)
                .map_err(|source| ClientError::CryptoContext { source })?;
            match authentication[0].status() {
                Some(Ok(len)) => len,
                Some(Err(source)) => return Err(ClientError::FinishedMac { source }),
                None => panic!("synchronous TLS Finished MAC operation must complete"),
            }
        };
        if !authenticators_equal(&expected_finished[..expected_len], message.body()) {
            return Err(ClientError::ServerFinishedAuthentication);
        }
        transcript
            .append(&peer_input[offset..offset + consumed])
            .map_err(|source| ClientError::Transcript { source })?;
        offset += consumed;
        if offset != peer_input.len() {
            return Err(ClientError::TrailingData {
                trailing: peer_input.len() - offset,
            });
        }

        let client_finished_hash = transcript
            .hash()
            .map_err(|source| ClientError::Transcript { source })?;
        let mut client_finished = [0u8; MAX_FINISHED_LEN];
        let client_finished_len = {
            let mut authentication = [MacOperation::authenticate(
                Input::Contiguous(client_finished_hash.as_slice()),
                &mut client_finished,
            )];
            crypto
                .client_finished
                .execute(&mut authentication)
                .map_err(|source| ClientError::CryptoContext { source })?;
            match authentication[0].status() {
                Some(Ok(len)) => len,
                Some(Err(source)) => return Err(ClientError::FinishedMac { source }),
                None => panic!("synchronous TLS Finished MAC operation must complete"),
            }
        };
        let written = size_of::<HandshakeHeader>() + client_finished_len;
        if output.len() < written {
            return Err(ClientError::OutputTooSmall {
                required: written,
                available: output.len(),
            });
        }
        HandshakeHeader::new(HandshakeType::Finished, client_finished_len)
            .and_then(|header| header.encode(output))
            .map_err(|source| ClientError::Message { source })?;
        output[size_of::<HandshakeHeader>()..written]
            .copy_from_slice(&client_finished[..client_finished_len]);
        transcript
            .append(&output[..written])
            .map_err(|source| ClientError::Transcript { source })?;
        let transcript_hash = transcript
            .hash()
            .map_err(|source| ClientError::Transcript { source })?;

        let shared_secret = {
            let mut agreement = [KxOperation::agree(
                crypto.private_key,
                selected_key_share.key_exchange(),
                crypto.shared_secret_target,
            )];
            crypto
                .key_exchange
                .execute(&mut agreement)
                .map_err(|source| ClientError::CryptoContext { source })?;
            match agreement[0].status() {
                KxStatus::SharedSecret { key } => key,
                status => return Err(ClientError::KeyAgreement { status }),
            }
        };

        Ok(Transition::Established {
            result: ClientEstablished {
                shared_secret,
                server_random: hello.random,
                cipher_suite: hello.cipher_suite,
                transcript_hash,
            },
            written,
        })
    }
}

#[derive(Debug, Error)]
pub(crate) enum ClientError {
    #[error("TLS Client handshake message failed")]
    Message {
        #[source]
        source: HandshakeError,
    },
    #[error("TLS Client hello failed")]
    Hello {
        #[source]
        source: HelloError,
    },
    #[error("TLS ServerHello supported_versions extension failed")]
    SupportedVersions {
        #[source]
        source: SupportedVersionsError,
    },
    #[error("TLS ServerHello key_share extension failed")]
    KeyShare {
        #[source]
        source: KeyShareError,
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
    #[error("TLS CertificateVerify selected unsupported signature scheme {scheme:02x?}")]
    CertificateSignatureScheme { scheme: [u8; 2] },
    #[error("TLS Client transcript failed")]
    Transcript {
        #[source]
        source: TranscriptError,
    },
    #[error("TLS Client Crypto Context failed")]
    CryptoContext {
        #[source]
        source: ContextError,
    },
    #[error("TLS server CertificateVerify authentication failed")]
    CertificateAuthentication {
        #[source]
        source: hammer_infra::crypto::signature::VerifyError,
    },
    #[error("TLS Finished MAC failed")]
    FinishedMac {
        #[source]
        source: hammer_infra::crypto::mac::Error,
    },
    #[error("TLS server Finished authentication failed")]
    ServerFinishedAuthentication,
    #[error("TLS key agreement failed with status {status:?}")]
    KeyAgreement { status: KxStatus },
    #[error("TLS Client output requires at least {required} bytes, received {available}")]
    OutputTooSmall { required: usize, available: usize },
    #[error("TLS Client expected {expected:?}, received {actual:?}")]
    UnexpectedMessage {
        expected: HandshakeType,
        actual: HandshakeType,
    },
    #[error("TLS Client handshake input has {trailing} trailing bytes")]
    TrailingData { trailing: usize },
    #[error("TLS ServerHello legacy session id does not echo ClientHello")]
    SessionIdMismatch,
    #[error("TLS ServerHello selected unoffered cipher suite {cipher_suite:02x?}")]
    CipherSuiteNotOffered { cipher_suite: [u8; 2] },
    #[error("TLS ServerHello is missing supported_versions")]
    SupportedVersionsMissing,
    #[error("TLS ServerHello is missing key_share")]
    KeyShareMissing,
    #[error("TLS ServerHello selected unsupported key share group {group:02x?}")]
    KeyShareGroup { group: [u8; 2] },
    #[error("TLS ServerHello did not select TLS 1.3")]
    Tls13NotSelected,
}
