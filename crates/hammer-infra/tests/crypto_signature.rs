use hammer_infra::crypto::signature::{
    Algorithm, EcdsaP256Sha256, EcdsaP384Sha384, Ed25519, Output, RsaPssSha256, RsaPssSha384,
    RsaPssSha512, SignError, VerifyError,
};
use rsa::pkcs1::DecodeRsaPrivateKey;
use rsa::pkcs8::{DecodePrivateKey, EncodePrivateKey};

const RSA_2048_PRIVATE_KEY: &str = r#"-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC2xCxRXxCmqvKC
xj7b4kJDoXDz+iYzvUgzY39Hyk9vNuA6XSnvwxkayA85DYdLOeMPQU/Owfyg7YHl
R+3CzTgsdvYckBiXPbn6U3lyp8cB9rd+CYLfwV/AGSfuXnzZS09Zn/BwE6fIKBvf
Ity8mtfKu3xDEcmC9Y7bchOtRVizMiZtdDrtgZLRiEytuLFHOaja2mbclwgG2ces
RQyxPQ18V1+xmFNPxhvEG8DwV04OATDHu7+9/cn2puLj4q/xy+rIm6V4hFKNVc+w
gyeh6MifTgA88oiOkzJB2daVvLus3JC0Tj4JX6NwWOolsT9eKVy+rG3oOKuMUK9h
4piXW4cvAgMBAAECggEAfsyDYsDtsHQRZCFeIvdKudkboGkAcAz2NpDlEU2O5r3P
uy4/lhRpKmd6CD8Wil5S5ZaOZAe52XxuDkBk+C2gt1ihTxe5t9QfX0jijWVRcE9W
5p56qfpjD8dkKMBtJeRV3PxVt6wrT3ZkP97T/hX/eKuyfmWsxKrQvfbbJ+9gppEM
XEoIXtQydasZwdmXoyxu/8598tGTX25gHu3hYaErXMJ8oh+B0smcPR6gjpDjBTqw
m++nJN7w0MOjwel0DA2fdhJqFJ7Aqn2AeCBUhCVNlR2wfEz5H7ZFTAlliP1ZJNur
6zWcogJSaNAE+dZus9b3rcETm61A8W3eY54RZHN2wQKBgQDcwGEkLU6Sr67nKsUT
ymW593A2+b1+Dm5hRhp+92VCJewVPH5cMaYVem5aE/9uF46HWMHLM9nWu+MXnvGJ
mOQi7Ny+149Oz9vl9PzYrsLJ0NyGRzypvRbZ0jjSH7Xd776xQ8ph0L1qqNkfM6CX
eQ6WQNvJEIXcXyY0O6MTj2stZwKBgQDT8xR1fkDpVINvkr4kI2ry8NoEo0ZTwYCv
Z+lgCG2T/eZcsj79nQk3R2L1mB42GEmvaM3XU5T/ak4G62myCeQijbLfpw5A9/l1
ClKBdmR7eI0OV3eiy4si480mf/cLTzsC06r7DhjFkKVksDGIsKpfxIFWsHYiIUJD
vRIn76fy+QKBgQDOaLesGw0QDWNuVUiHU8XAmEP9s5DicF33aJRXyb2Nl2XjCXhh
fi78gEj0wyQgbbhgh7ZU6Xuz1GTn7j+M2D/hBDb33xjpqWPE5kkR1n7eNAQvLibj
06GtNGra1rm39ncIywlOYt7p/01dZmmvmIryJV0c6O0xfGp9hpHaNU0S2wKBgCX2
5ZRCIChrTfu/QjXA7lhD0hmAkYlRINbKeyALgm0+znOOLgBJj6wKKmypacfww8oa
sLxAKXEyvnU4177fTLDvxrmO99ulT1aqmaq85TTEnCeUfUZ4xRxjx4x84WhyMbTI
61h65u8EgMuvT8AXPP1Yen5nr1FfubnedREYOXIpAoGAMZlUBtQGIHyt6uo1s40E
DF+Kmhrggn6e0GsVPYO2ghk1tLNqgr6dVseRtYwnJxpXk9U6HWV8CJl5YLFDPlFx
mH9FLxRKfHIwbWPh0//Atxt1qwjy5FpILpiEUcvkeOEusijQdFbJJLZvbO0EjYU/
Uz4xpoYU8cPObY7JmDznKvc=
-----END PRIVATE KEY-----"#;

const RSA_512_PRIVATE_KEY: &str = r#"-----BEGIN RSA PRIVATE KEY-----
MIIBOgIBAAJBALKZD0nEffqM1ACuak0bijtqE2QrI/KLADv7l3kK3ppMyCuLKoF0
fd7Ai2KW5ToIwzFofvJcS/STa6HA5gQenRUCAwEAAQJBAIq9amn00aS0h/CrjXqu
/ThglAXJmZhOMPVn4eiu7/ROixi9sex436MaVeMqSNf7Ex9a8fRNfWss7Sqd9eWu
RTUCIQDasvGASLqmjeffBNLTXV2A5g4t+kLVCpsEIZAycV5GswIhANEPLmax0ME/
EO+ZJ79TJKN5yiGBRsv5yvx5UiHxajEXAiAhAol5N4EUyq6I9w1rYdhPMGpLfk7A
IU2snfRJ6Nq2CQIgFrPsWRCkV+gOYcajD17rEqmuLrdIRexpg8N1DOSXoJ8CIGlS
tAboUGBxTDq3ZroNism3DaMIbKPyYrAqhKov1h5V
-----END RSA PRIVATE KEY-----"#;

#[test]
fn ed25519_matches_rfc_8032_test_vector_one() {
    let private_key = hex::decode(
        "9d61b19deffd5a60ba844af492ec2cc4\
         4449c5697b326919703bac031cae7f60",
    )
    .expect("valid private-key vector");
    let expected_public_key = hex::decode(
        "d75a980182b10ab7d54bfed3c964073a\
         0ee172f3daa62325af021a68f707511a",
    )
    .expect("valid public-key vector");
    let expected_signature = hex::decode(
        "e5564300c360ac729086e2cc806e828a\
         84877f1eb8e5d974d873e06522490155\
         5fb8821590a33bacc61e39701cf9b46b\
         d25bf5f0595bbe24655141438e7a100b",
    )
    .expect("valid signature vector");
    let mut public_key = [0_u8; 32];
    let mut signature = [0_u8; 64];
    let algorithm = Ed25519;

    assert_eq!(
        algorithm
            .public_key(&private_key, &mut public_key)
            .expect("public-key derivation succeeds"),
        public_key.len()
    );
    assert_eq!(public_key.as_slice(), expected_public_key);
    assert_eq!(
        algorithm
            .sign(&private_key, &[b""], &mut signature)
            .expect("signing succeeds"),
        signature.len()
    );
    assert_eq!(signature.as_slice(), expected_signature);
    algorithm
        .verify(&public_key, &[b""], &signature)
        .expect("signature verifies");
}

#[test]
fn ecdsa_p256_sha256_round_trips_scatter_input() {
    let algorithm = EcdsaP256Sha256;
    let private_key = [1_u8; 32];
    let message: [&[u8]; 3] = [b"p-256 ", b"scatter ", b"message"];
    let mut public_key = [0_u8; 65];
    let mut signature = [0_u8; 64];

    assert_eq!(
        algorithm
            .public_key(&private_key, &mut public_key)
            .expect("public-key derivation succeeds"),
        public_key.len()
    );
    assert_eq!(
        algorithm
            .sign(&private_key, &message, &mut signature)
            .expect("signing succeeds"),
        signature.len()
    );
    algorithm
        .verify(&public_key, &message, &signature)
        .expect("signature verifies");

    signature[0] ^= 1;
    assert_eq!(
        algorithm.verify(&public_key, &message, &signature),
        Err(VerifyError::SignatureMismatch)
    );
}

#[test]
fn ecdsa_p384_sha384_round_trips_scatter_input() {
    let algorithm = EcdsaP384Sha384;
    let private_key = [1_u8; 48];
    let message: [&[u8]; 2] = [b"p-384 ", b"message"];
    let mut public_key = [0_u8; 97];
    let mut signature = [0_u8; 96];

    assert_eq!(
        algorithm
            .public_key(&private_key, &mut public_key)
            .expect("public-key derivation succeeds"),
        public_key.len()
    );
    assert_eq!(
        algorithm
            .sign(&private_key, &message, &mut signature)
            .expect("signing succeeds"),
        signature.len()
    );
    algorithm
        .verify(&public_key, &message, &signature)
        .expect("signature verifies");
}

#[test]
fn rsa_pss_sha2_parameter_sets_round_trip() {
    let private_key = rsa::RsaPrivateKey::from_pkcs8_pem(RSA_2048_PRIVATE_KEY)
        .expect("valid synthetic RSA test key")
        .to_pkcs8_der()
        .expect("RSA test key encodes as PKCS#8");
    let message: [&[u8]; 2] = [b"rsa-pss ", b"message"];
    let mut public_key = [0_u8; 512];
    let mut signature = [0_u8; 256];

    let algorithm = RsaPssSha256::default();
    let public_key_len = algorithm
        .public_key(private_key.as_bytes(), &mut public_key)
        .expect("RSA public-key derivation succeeds");
    let signature_len = algorithm
        .sign(private_key.as_bytes(), &message, &mut signature)
        .expect("RSA-PSS SHA-256 signing succeeds");
    algorithm
        .verify(
            &public_key[..public_key_len],
            &message,
            &signature[..signature_len],
        )
        .expect("RSA-PSS SHA-256 signature verifies");

    let algorithm = RsaPssSha384::default();
    let signature_len = algorithm
        .sign(private_key.as_bytes(), &message, &mut signature)
        .expect("RSA-PSS SHA-384 signing succeeds");
    algorithm
        .verify(
            &public_key[..public_key_len],
            &message,
            &signature[..signature_len],
        )
        .expect("RSA-PSS SHA-384 signature verifies");

    let algorithm = RsaPssSha512::default();
    let signature_len = algorithm
        .sign(private_key.as_bytes(), &message, &mut signature)
        .expect("RSA-PSS SHA-512 signing succeeds");
    algorithm
        .verify(
            &public_key[..public_key_len],
            &message,
            &signature[..signature_len],
        )
        .expect("RSA-PSS SHA-512 signature verifies");
}

#[test]
fn signature_errors_preserve_operation_specific_categories() {
    let algorithm = Ed25519;
    let private_key = [7_u8; 32];
    let mut short_output = [0xa5; 63];

    assert_eq!(
        algorithm.sign(&private_key, &[b"message"], &mut short_output),
        Err(SignError::OutputTooSmall {
            output: Output::Signature,
            required: 64,
            provided: 63,
        })
    );
    assert_eq!(short_output, [0xa5; 63]);
    assert_eq!(
        algorithm.verify(&[0; 31], &[b"message"], &[0; 64]),
        Err(VerifyError::InvalidPublicKeyLength {
            required: 32,
            provided: 31,
        })
    );

    let private_key = rsa::RsaPrivateKey::from_pkcs1_pem(RSA_512_PRIVATE_KEY)
        .expect("valid undersized synthetic RSA test key")
        .to_pkcs8_der()
        .expect("RSA test key encodes as PKCS#8");
    let mut output = [0_u8; 128];
    assert_eq!(
        RsaPssSha512::default().public_key(private_key.as_bytes(), &mut output),
        Err(SignError::KeyTooSmall {
            required: 130,
            provided: 64,
        })
    );
}
