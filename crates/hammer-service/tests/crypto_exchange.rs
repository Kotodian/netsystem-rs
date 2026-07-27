use hammer_infra::crypto::InstructionSet;
use hammer_service::crypto::exchange::{Protocol, Transition};
use hammer_service::crypto::{
    Aead, AlgorithmId, Context, ContextError, Engine, Hash, HashOperation, Input, Kdf,
    KdfOperation, KdfStatus, KeyError, KeyHandle, KeyOperations, KeyPolicy, Kx, KxOperation,
    KxStatus, Sign, SignOperation, Verify, VerifyOperation,
};

#[derive(Debug, Eq, PartialEq)]
struct Established {
    parameter: u8,
    peer_len: usize,
}

#[derive(Debug)]
struct State {
    parameter: u8,
}

#[derive(Default)]
struct Crypto {
    transitions: usize,
}

struct Probe;

impl Protocol<Crypto> for Probe {
    type Parameters = u8;
    type State = State;
    type Established = Established;
    type Error = &'static str;

    fn start(
        &mut self,
        _: &Engine,
        parameter: Self::Parameters,
        crypto: &mut Crypto,
        output: &mut [u8],
    ) -> Result<(Self::State, usize), Self::Error> {
        let byte = output.first_mut().ok_or("output too small")?;
        *byte = parameter;
        crypto.transitions += 1;
        Ok((State { parameter }, 1))
    }

    fn advance(
        &mut self,
        _: &Engine,
        state: Self::State,
        crypto: &mut Crypto,
        peer_input: &[u8],
        output: &mut [u8],
    ) -> Result<Transition<Self::State, Self::Established>, Self::Error> {
        let byte = output.first_mut().ok_or("output too small")?;
        *byte = u8::try_from(peer_input.len()).map_err(|_| "peer input too large")?;
        crypto.transitions += 1;
        Ok(Transition::Established {
            result: Established {
                parameter: state.parameter,
                peer_len: peer_input.len(),
            },
            written: 1,
        })
    }
}

#[test]
fn engine_drives_a_typed_protocol_over_caller_owned_memory() {
    let engine = Engine::new(InstructionSet::empty());
    let mut output = [0u8; 1];
    let (exchange, written) = engine
        .start_exchange(Probe, 7, Crypto::default(), &mut output)
        .expect("probe starts");
    assert_eq!(written, 1);
    assert_eq!(output, [7]);

    let transition = engine
        .advance_exchange(exchange, b"peer", &mut output)
        .expect("probe advances");
    assert_eq!(output, [4]);
    let Transition::Established { result, written } = transition else {
        panic!("probe did not establish")
    };
    assert_eq!(
        result,
        Established {
            parameter: 7,
            peer_len: 4,
        }
    );
    assert_eq!(written, 1);
}

#[derive(Debug)]
enum AuthState {
    AwaitPeer {
        private_key: KeyHandle,
        public_key: [u8; 32],
    },
    AwaitFinish {
        shared_key: KeyHandle,
        traffic_key: KeyHandle,
        transcript: [u8; 32],
    },
}

#[derive(Debug, Eq, PartialEq)]
struct Authenticated {
    shared_key: KeyHandle,
    traffic_key: KeyHandle,
    transcript: [u8; 32],
}

#[derive(Debug)]
enum AuthError {
    OutputTooSmall,
    PeerMessageMalformed,
    Context(ContextError),
    KeyEstablishment(KxStatus),
    Derivation(KdfStatus),
    Hash(hammer_infra::crypto::hash::Error),
    Authentication(hammer_infra::crypto::signature::VerifyError),
}

impl std::fmt::Display for AuthError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OutputTooSmall => formatter.write_str("caller output is too small"),
            Self::PeerMessageMalformed => formatter.write_str("peer message is malformed"),
            Self::Context(error) => write!(formatter, "crypto Context failed: {error}"),
            Self::KeyEstablishment(status) => {
                write!(formatter, "key establishment failed: {status:?}")
            }
            Self::Derivation(status) => write!(formatter, "derivation failed: {status:?}"),
            Self::Hash(error) => write!(formatter, "transcript hash failed: {error}"),
            Self::Authentication(error) => write!(formatter, "authentication failed: {error}"),
        }
    }
}

impl std::error::Error for AuthError {}

struct AuthCrypto {
    key_establishment: Context<Kx>,
    derivation: Context<Kdf>,
    hash: Context<Hash>,
    verify: Context<Verify>,
    derivation_algorithm: AlgorithmId<Kdf>,
    traffic_algorithm: AlgorithmId<Aead>,
    server_public_key: [u8; 32],
}

impl AuthCrypto {
    fn new(engine: &Engine, server_public_key: [u8; 32]) -> Self {
        let key_establishment_algorithm = engine
            .algorithm::<Kx>("x25519")
            .expect("X25519 is built in");
        let derivation_algorithm = engine
            .algorithm::<Kdf>("hkdf-sha-256")
            .expect("HKDF-SHA-256 is built in");
        let traffic_algorithm = engine
            .algorithm::<Aead>("aes-128-gcm")
            .expect("AES-128-GCM is built in");
        let hash_algorithm = engine
            .algorithm::<Hash>("sha-256")
            .expect("SHA-256 is built in");
        let verify_algorithm = engine
            .algorithm::<Verify>("ed25519")
            .expect("Ed25519 verification is built in");
        let derivation_key = engine
            .create_key(
                &[0x42; 32],
                KeyPolicy::new(derivation_algorithm, KeyOperations::DERIVE, false).with_derivation(
                    traffic_algorithm,
                    KeyOperations::AEAD_SEAL | KeyOperations::AEAD_OPEN,
                    false,
                ),
            )
            .expect("synthetic derivation key installs");

        Self {
            key_establishment: engine
                .context(key_establishment_algorithm)
                .expect("X25519 Context"),
            derivation: engine
                .context_with_key(derivation_algorithm, derivation_key)
                .expect("HKDF Context"),
            hash: engine.context(hash_algorithm).expect("SHA-256 Context"),
            verify: engine.context(verify_algorithm).expect("Ed25519 Context"),
            derivation_algorithm,
            traffic_algorithm,
            server_public_key,
        }
    }
}

struct AuthProtocol;

impl AuthProtocol {
    fn accept_peer(
        &mut self,
        private_key: KeyHandle,
        public_key: [u8; 32],
        crypto: &mut AuthCrypto,
        peer_input: &[u8],
        output: &mut [u8],
    ) -> Result<Transition<AuthState, Authenticated>, AuthError> {
        if peer_input.len() != 96 || output.len() < 32 {
            return Err(if peer_input.len() != 96 {
                AuthError::PeerMessageMalformed
            } else {
                AuthError::OutputTooSmall
            });
        }
        let (peer_public_key, signature) = peer_input.split_at(32);
        let peer_public_key =
            <&[u8; 32]>::try_from(peer_public_key).map_err(|_| AuthError::PeerMessageMalformed)?;
        let transcript_parts: &[&[u8]] = &[&public_key, peer_public_key];
        let mut verify = [VerifyOperation::verify(
            &crypto.server_public_key,
            Input::Scatter(transcript_parts),
            signature,
        )];
        crypto
            .verify
            .execute(&mut verify)
            .map_err(AuthError::Context)?;
        match verify[0].status() {
            Some(Ok(())) => {}
            Some(Err(error)) => return Err(AuthError::Authentication(error)),
            None => unreachable!("synchronous verification sets one operation status"),
        }

        let shared_key = {
            let mut agreement = [KxOperation::agree(
                private_key,
                peer_public_key,
                crypto.derivation_algorithm,
            )];
            crypto
                .key_establishment
                .execute(&mut agreement)
                .map_err(AuthError::Context)?;
            match agreement[0].status() {
                KxStatus::SharedSecret { key } => key,
                status => return Err(AuthError::KeyEstablishment(status)),
            }
        };

        let traffic_key = {
            let mut derivation = [KdfOperation::derive(
                None,
                Input::Scatter(transcript_parts),
                16,
                crypto.traffic_algorithm,
            )];
            crypto
                .derivation
                .execute(&mut derivation)
                .map_err(AuthError::Context)?;
            match derivation[0].status() {
                KdfStatus::Complete { key } => key,
                status => return Err(AuthError::Derivation(status)),
            }
        };

        let mut hash = [HashOperation::new(
            Input::Scatter(transcript_parts),
            &mut output[..32],
        )];
        crypto.hash.execute(&mut hash).map_err(AuthError::Context)?;
        match hash[0].status() {
            Some(Ok(32)) => {}
            Some(Ok(_)) => unreachable!("SHA-256 always writes 32 bytes"),
            Some(Err(error)) => return Err(AuthError::Hash(error)),
            None => unreachable!("synchronous hashing sets one operation status"),
        }
        let mut transcript = [0u8; 32];
        transcript.copy_from_slice(&output[..32]);

        Ok(Transition::Continue {
            state: AuthState::AwaitFinish {
                shared_key,
                traffic_key,
                transcript,
            },
            written: 32,
        })
    }

    fn finish(
        &mut self,
        shared_key: KeyHandle,
        traffic_key: KeyHandle,
        transcript: [u8; 32],
        crypto: &mut AuthCrypto,
        peer_input: &[u8],
        output: &mut [u8],
    ) -> Result<Transition<AuthState, Authenticated>, AuthError> {
        if peer_input.len() != 64 {
            return Err(AuthError::PeerMessageMalformed);
        }
        let confirmation = b"established";
        if output.len() < confirmation.len() {
            return Err(AuthError::OutputTooSmall);
        }
        let mut verify = [VerifyOperation::verify(
            &crypto.server_public_key,
            Input::Contiguous(&transcript),
            peer_input,
        )];
        crypto
            .verify
            .execute(&mut verify)
            .map_err(AuthError::Context)?;
        match verify[0].status() {
            Some(Ok(())) => {}
            Some(Err(error)) => return Err(AuthError::Authentication(error)),
            None => unreachable!("synchronous verification sets one operation status"),
        }
        output[..confirmation.len()].copy_from_slice(confirmation);
        Ok(Transition::Established {
            result: Authenticated {
                shared_key,
                traffic_key,
                transcript,
            },
            written: confirmation.len(),
        })
    }
}

impl Protocol<AuthCrypto> for AuthProtocol {
    type Parameters = KeyPolicy;
    type State = AuthState;
    type Established = Authenticated;
    type Error = AuthError;

    fn start(
        &mut self,
        _: &Engine,
        private_key_policy: Self::Parameters,
        crypto: &mut AuthCrypto,
        output: &mut [u8],
    ) -> Result<(Self::State, usize), Self::Error> {
        if output.len() < 32 {
            return Err(AuthError::OutputTooSmall);
        }
        let mut generation = [KxOperation::generate_keypair(
            private_key_policy,
            &mut output[..32],
        )];
        crypto
            .key_establishment
            .execute(&mut generation)
            .map_err(AuthError::Context)?;
        let private_key = match generation[0].status() {
            KxStatus::Generated { key, .. } => key,
            status => return Err(AuthError::KeyEstablishment(status)),
        };
        let mut public_key = [0u8; 32];
        public_key.copy_from_slice(&output[..32]);
        Ok((
            AuthState::AwaitPeer {
                private_key,
                public_key,
            },
            32,
        ))
    }

    fn advance(
        &mut self,
        _: &Engine,
        state: Self::State,
        crypto: &mut AuthCrypto,
        peer_input: &[u8],
        output: &mut [u8],
    ) -> Result<Transition<Self::State, Self::Established>, Self::Error> {
        match state {
            AuthState::AwaitPeer {
                private_key,
                public_key,
            } => self.accept_peer(private_key, public_key, crypto, peer_input, output),
            AuthState::AwaitFinish {
                shared_key,
                traffic_key,
                transcript,
            } => self.finish(
                shared_key,
                traffic_key,
                transcript,
                crypto,
                peer_input,
                output,
            ),
        }
    }
}

fn server_identity(engine: &Engine) -> ([u8; 32], Context<Sign>) {
    let algorithm = engine
        .algorithm::<Sign>("ed25519")
        .expect("Ed25519 signing is built in");
    let private_key = [
        0x9d, 0x61, 0xb1, 0x9d, 0xef, 0xfd, 0x5a, 0x60, 0xba, 0x84, 0x4a, 0xf4, 0x92, 0xec, 0x2c,
        0xc4, 0x44, 0x49, 0xc5, 0x69, 0x7b, 0x32, 0x69, 0x19, 0x70, 0x3b, 0xac, 0x03, 0x1c, 0xae,
        0x7f, 0x60,
    ];
    let key = engine
        .create_key(
            &private_key,
            KeyPolicy::new(algorithm, KeyOperations::SIGN, false),
        )
        .expect("server signing key installs");
    let mut context = engine
        .context_with_key(algorithm, key)
        .expect("server signing Context");
    let mut public_key = [0u8; 32];
    let mut operation = [SignOperation::public_key(&mut public_key)];
    context
        .execute(&mut operation)
        .expect("server signing implementation remains available");
    assert_eq!(operation[0].status(), Some(Ok(32)));
    (public_key, context)
}

fn peer_key(engine: &Engine) -> [u8; 32] {
    let algorithm = engine
        .algorithm::<Kx>("x25519")
        .expect("X25519 is built in");
    let derivation = engine
        .algorithm::<Kdf>("hkdf-sha-256")
        .expect("HKDF-SHA-256 is built in");
    let mut context = engine.context(algorithm).expect("peer X25519 Context");
    let mut public_key = [0u8; 32];
    let mut operation = [KxOperation::generate_keypair(
        KeyPolicy::new(algorithm, KeyOperations::KX_AGREE, false).with_derivation(
            derivation,
            KeyOperations::DERIVE,
            false,
        ),
        &mut public_key,
    )];
    context
        .execute(&mut operation)
        .expect("peer X25519 implementation remains available");
    assert!(matches!(operation[0].status(), KxStatus::Generated { .. }));
    public_key
}

fn sign_message(context: &mut Context<Sign>, input: Input<'_>) -> [u8; 64] {
    let mut signature = [0u8; 64];
    let mut operation = [SignOperation::sign(input, &mut signature)];
    context
        .execute(&mut operation)
        .expect("server signing implementation remains available");
    assert_eq!(operation[0].status(), Some(Ok(64)));
    signature
}

#[test]
fn synthetic_exchange_establishes_after_key_authentication_and_derivation() {
    let engine =
        Engine::with_builtins(InstructionSet::detect()).expect("built-in crypto registry is valid");
    let (server_public_key, mut server_signer) = server_identity(&engine);
    let peer_public_key = peer_key(&engine);
    let key_establishment_algorithm = engine
        .algorithm::<Kx>("x25519")
        .expect("X25519 is built in");
    let derivation_algorithm = engine
        .algorithm::<Kdf>("hkdf-sha-256")
        .expect("HKDF-SHA-256 is built in");
    let private_key_policy =
        KeyPolicy::new(key_establishment_algorithm, KeyOperations::KX_AGREE, false)
            .with_derivation(derivation_algorithm, KeyOperations::DERIVE, false);
    let mut output = [0u8; 128];
    let (exchange, written) = engine
        .start_exchange(
            AuthProtocol,
            private_key_policy,
            AuthCrypto::new(&engine, server_public_key),
            &mut output,
        )
        .expect("synthetic exchange starts");
    assert_eq!(written, 32);
    let mut client_public_key = [0u8; 32];
    client_public_key.copy_from_slice(&output[..32]);

    let transcript_parts: &[&[u8]] = &[&client_public_key, &peer_public_key];
    let peer_signature = sign_message(&mut server_signer, Input::Scatter(transcript_parts));
    let mut peer_message = [0u8; 96];
    peer_message[..32].copy_from_slice(&peer_public_key);
    peer_message[32..].copy_from_slice(&peer_signature);
    let transition = engine
        .advance_exchange(exchange, &peer_message, &mut output)
        .expect("authenticated peer message advances");
    let Transition::Continue {
        state: exchange,
        written,
    } = transition
    else {
        panic!("first authenticated transition established too early")
    };
    assert_eq!(written, 32);
    let mut transcript = [0u8; 32];
    transcript.copy_from_slice(&output[..32]);

    let finish_signature = sign_message(&mut server_signer, Input::Contiguous(&transcript));
    let transition = engine
        .advance_exchange(exchange, &finish_signature, &mut output)
        .expect("authenticated finish establishes");
    let Transition::Established { result, written } = transition else {
        panic!("finish did not establish")
    };
    assert_eq!(written, b"established".len());
    assert_eq!(&output[..written], b"established");
    assert_eq!(result.transcript, transcript);
    assert_eq!(
        engine.export_secret(result.shared_key, &mut [0u8; 32]),
        Err(KeyError::SecretExportDenied {
            key: result.shared_key
        })
    );
    assert_eq!(
        engine.export_secret(result.traffic_key, &mut [0u8; 16]),
        Err(KeyError::SecretExportDenied {
            key: result.traffic_key
        })
    );
}

#[test]
fn synthetic_exchange_rejects_an_invalid_peer_signature() {
    let engine =
        Engine::with_builtins(InstructionSet::detect()).expect("built-in crypto registry is valid");
    let (server_public_key, _) = server_identity(&engine);
    let key_establishment_algorithm = engine
        .algorithm::<Kx>("x25519")
        .expect("X25519 is built in");
    let derivation_algorithm = engine
        .algorithm::<Kdf>("hkdf-sha-256")
        .expect("HKDF-SHA-256 is built in");
    let mut output = [0u8; 128];
    let (exchange, _) = engine
        .start_exchange(
            AuthProtocol,
            KeyPolicy::new(key_establishment_algorithm, KeyOperations::KX_AGREE, false)
                .with_derivation(derivation_algorithm, KeyOperations::DERIVE, false),
            AuthCrypto::new(&engine, server_public_key),
            &mut output,
        )
        .expect("synthetic exchange starts");
    let mut peer_message = [0u8; 96];
    peer_message[..32].copy_from_slice(&peer_key(&engine));

    let error = match engine.advance_exchange(exchange, &peer_message, &mut output) {
        Ok(_) => panic!("zero signature must be rejected"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        AuthError::Authentication(hammer_infra::crypto::signature::VerifyError::SignatureMismatch)
    ));
}
