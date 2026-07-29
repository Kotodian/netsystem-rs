//! TLS 1.3 role state and transcript ownership.

mod client;
mod server;
mod transcript;

pub(crate) use client::{ClientCrypto, ClientError, ClientEstablished, ClientHandshake};
pub(crate) use server::{ServerCrypto, ServerError, ServerEstablished, ServerHandshake};
pub(crate) use transcript::{TranscriptError, TranscriptHash};

const CERTIFICATE_VERIFY_PADDING: [u8; 64] = [0x20; 64];
const SERVER_CERTIFICATE_VERIFY_CONTEXT: &[u8] = b"TLS 1.3, server CertificateVerify";
const CERTIFICATE_VERIFY_SEPARATOR: [u8; 1] = [0];
const MAX_FINISHED_LEN: usize = 48;

fn authenticators_equal(expected: &[u8], received: &[u8]) -> bool {
    let mut difference = expected.len() ^ received.len();
    for index in 0..expected.len().max(received.len()) {
        difference |= usize::from(
            expected.get(index).copied().unwrap_or(0) ^ received.get(index).copied().unwrap_or(0),
        );
    }
    difference == 0
}

#[cfg(test)]
mod tests {
    use hammer_infra::crypto::InstructionSet;
    use hammer_service::crypto::exchange::Transition;
    use hammer_service::crypto::{
        Context, Engine, Hash, Kdf, KeyOperations, KeyPolicy, Kx, Mac, Sign, SignOperation, Verify,
    };

    use super::*;
    use crate::codec::{Certificate, ClientHello, EncryptedExtensions, ServerHello};
    use crate::test_fixtures::{
        RFC8448_CLIENT_HELLO, RFC8448_CLIENT_X25519_PRIVATE, RFC8448_SERVER_HELLO,
        RFC8448_SERVER_X25519_PRIVATE,
    };

    fn finished_context(engine: &Engine, key: hammer_service::crypto::KeyHandle) -> Context<Mac> {
        let algorithm = engine
            .algorithm::<Mac>("hmac-sha-256")
            .expect("HMAC-SHA-256 algorithm");
        engine
            .context_with_key(algorithm, key)
            .expect("HMAC-SHA-256 Context")
    }

    #[test]
    fn client_and_server_establish_only_after_certificate_verify_and_finished() {
        let engine =
            Engine::with_builtins(InstructionSet::empty()).expect("built-in Crypto Engine");
        let hash = engine
            .algorithm::<Hash>("sha-256")
            .expect("SHA-256 algorithm");
        let key_exchange = engine.algorithm::<Kx>("x25519").expect("X25519 algorithm");
        let shared_secret_target = engine
            .algorithm::<Kdf>("hkdf-sha-256")
            .expect("HKDF-SHA-256 algorithm");
        let key_exchange_policy = KeyPolicy::new(key_exchange, KeyOperations::KX_AGREE, false)
            .with_derivation(shared_secret_target, KeyOperations::DERIVE, true);
        let client_private_key = engine
            .create_key(&RFC8448_CLIENT_X25519_PRIVATE, key_exchange_policy.clone())
            .expect("client X25519 key");
        let server_private_key = engine
            .create_key(&RFC8448_SERVER_X25519_PRIVATE, key_exchange_policy)
            .expect("server X25519 key");

        let signing_algorithm = engine
            .algorithm::<Sign>("ed25519")
            .expect("Ed25519 signing algorithm");
        let signing_key = engine
            .create_key(
                &[
                    0x9d, 0x61, 0xb1, 0x9d, 0xef, 0xfd, 0x5a, 0x60, 0xba, 0x84, 0x4a, 0xf4, 0x92,
                    0xec, 0x2c, 0xc4, 0x44, 0x49, 0xc5, 0x69, 0x7b, 0x32, 0x69, 0x19, 0x70, 0x3b,
                    0xac, 0x03, 0x1c, 0xae, 0x7f, 0x60,
                ],
                KeyPolicy::new(signing_algorithm, KeyOperations::SIGN, false),
            )
            .expect("server Ed25519 key");
        let mut signing = engine
            .context_with_key(signing_algorithm, signing_key)
            .expect("server Ed25519 signing Context");
        let mut server_identity = [0u8; 32];
        {
            let mut operation = [SignOperation::public_key(&mut server_identity)];
            signing
                .execute(&mut operation)
                .expect("derive server Ed25519 public key");
            assert_eq!(operation[0].status(), Some(Ok(32)));
        }

        let mac = engine
            .algorithm::<Mac>("hmac-sha-256")
            .expect("HMAC-SHA-256 algorithm");
        let server_finished_key = engine
            .create_key(
                &[0x53; 32],
                KeyPolicy::new(mac, KeyOperations::MAC_AUTHENTICATE, false),
            )
            .expect("server Finished key");
        let client_finished_key = engine
            .create_key(
                &[0x43; 32],
                KeyPolicy::new(mac, KeyOperations::MAC_AUTHENTICATE, false),
            )
            .expect("client Finished key");

        let client_crypto = ClientCrypto::new(
            engine.context(key_exchange).expect("client X25519 Context"),
            client_private_key,
            shared_secret_target,
            engine
                .context(
                    engine
                        .algorithm::<Verify>("ed25519")
                        .expect("Ed25519 verification algorithm"),
                )
                .expect("client Ed25519 verification Context"),
            finished_context(&engine, server_finished_key),
            finished_context(&engine, client_finished_key),
        );
        let server_crypto = ServerCrypto::new(
            engine.context(key_exchange).expect("server X25519 Context"),
            server_private_key,
            shared_secret_target,
            signing,
            finished_context(&engine, server_finished_key),
            finished_context(&engine, client_finished_key),
        );

        let mut client_hello_body = [0u8; RFC8448_CLIENT_HELLO.len()];
        client_hello_body.copy_from_slice(RFC8448_CLIENT_HELLO);
        client_hello_body[150..152].copy_from_slice(&[0x08, 0x07]);
        let client_hello = ClientHello::decode(&client_hello_body)
            .expect("RFC 8448 ClientHello with Ed25519 offer");
        let server_hello = ServerHello::decode(RFC8448_SERVER_HELLO).expect("RFC 8448 ServerHello");
        let encrypted_extensions =
            EncryptedExtensions::decode(&[0, 0]).expect("empty EncryptedExtensions");
        let mut certificate_body = [0u8; 41];
        certificate_body[..7].copy_from_slice(&[0, 0, 0, 37, 0, 0, 32]);
        certificate_body[7..39].copy_from_slice(&server_identity);
        let certificate = Certificate::decode(&certificate_body).expect("server Certificate");

        let mut client_hello_message = [0u8; 196];
        let (client, client_hello_written) = engine
            .start_exchange(
                ClientHandshake::new(client_hello, hash),
                (),
                client_crypto,
                &mut client_hello_message,
            )
            .expect("client handshake starts");
        assert_eq!(client_hello_written, client_hello_message.len());

        let (server, server_start_written) = engine
            .start_exchange(
                ServerHandshake::new(server_hello, encrypted_extensions, certificate, hash),
                (),
                server_crypto,
                &mut [],
            )
            .expect("server handshake starts");
        assert_eq!(server_start_written, 0);

        let mut server_flight = [0u8; 512];
        let transition = engine
            .advance_exchange(
                server,
                &client_hello_message[..client_hello_written],
                &mut server_flight,
            )
            .expect("server accepts ClientHello");
        let Transition::Continue {
            state: server,
            written: server_flight_written,
        } = transition
        else {
            panic!("server must wait for authenticated Client Finished")
        };

        let mut client_finished = [0u8; 52];
        let transition = engine
            .advance_exchange(
                client,
                &server_flight[..server_flight_written],
                &mut client_finished,
            )
            .expect("client authenticates server flight");
        let Transition::Established {
            result: client,
            written: client_finished_written,
        } = transition
        else {
            panic!("client must establish after authenticating server Finished")
        };
        assert_eq!(&client_finished[..4], &[20, 0, 0, 32]);

        let transition = engine
            .advance_exchange(server, &client_finished[..client_finished_written], &mut [])
            .expect("server authenticates Client Finished");
        let Transition::Established {
            result: server,
            written,
        } = transition
        else {
            panic!("server must establish after authenticating Client Finished")
        };
        assert_eq!(written, 0);
        assert_eq!(client.transcript_hash, server.transcript_hash);

        let mut client_shared_secret = [0u8; 32];
        let mut server_shared_secret = [0u8; 32];
        assert_eq!(
            engine
                .export_secret(client.shared_secret, &mut client_shared_secret)
                .expect("test policy exports client shared secret"),
            32
        );
        assert_eq!(
            engine
                .export_secret(server.shared_secret, &mut server_shared_secret)
                .expect("test policy exports server shared secret"),
            32
        );
        assert_eq!(client_shared_secret, server_shared_secret);
    }
}
