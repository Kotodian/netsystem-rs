use hammer_infra::crypto::{
    AeadAlgorithm, AeadCipher, HashAlgorithm, Hkdf, Hmac, KdfError, MacError, Sha2Algorithm, hash,
};

fn bytes(value: &str) -> Vec<u8> {
    hex::decode(value).expect("test vector is valid hexadecimal")
}

#[test]
fn digest_catalog_matches_empty_message_vectors() {
    let vectors = [
        (
            HashAlgorithm::Sha256,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        ),
        (
            HashAlgorithm::Sha384,
            "38b060a751ac96384cd9327eb1b1e36a21fdb71114be07434c0cc7bf63f6e1da274edebfe76f65fbd51ad2f14898b95b",
        ),
        (
            HashAlgorithm::Sha512,
            "cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e",
        ),
        (
            HashAlgorithm::Blake2s,
            "69217a3079908094e11121d042354a7c1f55b6482ca1a51e1b250dfd1ed0eef9",
        ),
        (
            HashAlgorithm::Blake2b,
            "786a02f742015903c6c6fd852552d272912f4740e15847618a86e217f71f5419d25e1031afee585313896444934eb04b903a685b1448b755d56f701afe9be2ce",
        ),
    ];

    for (algorithm, expected) in vectors {
        let expected = bytes(expected);
        let mut output = vec![0; expected.len()];
        let written =
            hash(algorithm, &[b"", b""], &mut output).expect("digest output has exact capacity");
        assert_eq!(written, expected.len());
        assert_eq!(output, expected);
    }
}

#[test]
fn aes_256_gcm_matches_nist_zero_vector() {
    let cipher =
        AeadCipher::new(AeadAlgorithm::Aes256Gcm, &[0; 32]).expect("AES-256 key has 32 bytes");
    let mut output = [0; 16];
    let mut tag = [0; 16];

    cipher
        .seal(&[&[0; 16]], &[0; 12], &[], &mut output, &mut tag)
        .expect("NIST vector is valid");

    assert_eq!(output.as_slice(), bytes("cea7403d4d606b6e074ec5d3baf39d18"));
    assert_eq!(tag.as_slice(), bytes("d0d1c8a799996bf0265b98b5d48ab919"));
}

#[test]
fn chacha20_poly1305_matches_rfc_8439_vector() {
    let key = bytes("808182838485868788898a8b8c8d8e8f909192939495969798999a9b9c9d9e9f");
    let cipher =
        AeadCipher::new(AeadAlgorithm::ChaCha20Poly1305, &key).expect("ChaCha20 key has 32 bytes");
    let nonce = bytes("070000004041424344454647");
    let associated_data = bytes("50515253c0c1c2c3c4c5c6c7");
    let plaintext = b"Ladies and Gentlemen of the class of '99: If I could offer you only one tip for the future, sunscreen would be it.";
    let mut output = vec![0; plaintext.len()];
    let mut tag = [0; 16];

    cipher
        .seal(
            &[&plaintext[..37], &plaintext[37..]],
            &nonce,
            &associated_data,
            &mut output,
            &mut tag,
        )
        .expect("RFC vector is valid");

    assert_eq!(
        output,
        bytes(
            "d31a8d34648e60db7b86afbc53ef7ec2a4aded51296e08fea9e2b5a736ee62d63dbea45e8ca9671282fafb69da92728b1a71de0a9e060b2905d6a5b67ecd3b3692ddbd7f2d778b8c9803aee328091b58fab324e4fad675945585808b4831d7bc3ff4def08e4b7a9de576d26586cec64b6116"
        )
    );
    assert_eq!(tag.as_slice(), bytes("1ae10b594f09e26a7e902ecbd0600691"));
}

#[test]
fn hmac_sha2_catalog_matches_rfc_4231_case_one() {
    let vectors = [
        (
            Sha2Algorithm::Sha256,
            "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7",
        ),
        (
            Sha2Algorithm::Sha384,
            "afd03944d84895626b0825f4ab46907f15f9dadbe4101ec682aa034c7cebc59cfaea9ea9076ede7f4af152e8b2fa9cb6",
        ),
        (
            Sha2Algorithm::Sha512,
            "87aa7cdea5ef619d4ff0b4241a1d6cb02379f4e2ce4ec2787ad0b30545e17cdedaa833b7d6b8a702038b274eaea3f4e4be9d914eeb61f1702e696c203a126854",
        ),
    ];

    for (algorithm, expected) in vectors {
        let expected = bytes(expected);
        let hmac = Hmac::new(algorithm, &[0x0b; 20]);
        let mut output = vec![0; expected.len()];
        let written = hmac
            .authenticate(&[b"Hi ", b"There"], &mut output)
            .expect("HMAC output has exact capacity");
        assert_eq!(written, expected.len());
        assert_eq!(output, expected);
    }
}

#[test]
fn hkdf_sha256_matches_rfc_5869_case_one() {
    let input_key_material = [0x0b; 22];
    let salt = bytes("000102030405060708090a0b0c");
    let hkdf = Hkdf::new(Sha2Algorithm::Sha256, Some(&salt), &input_key_material);
    let mut output = [0; 42];

    let written = hkdf
        .expand(&[&bytes("f0f1f2f3f4f5f6f7f8f9")], 42, &mut output)
        .expect("RFC output length is valid");

    assert_eq!(written, 42);
    assert_eq!(
        output.as_slice(),
        bytes(
            "3cb25f25faacd57a90434f64d0362f2a2d2d0a90cf1a5a4c5db02d56ecc4c5bf34007208d5b887185865"
        )
    );
}

#[test]
fn keyed_interfaces_reject_capacity_before_modifying_output() {
    let hmac = Hmac::new(Sha2Algorithm::Sha256, b"key");
    let mut mac_output = [0xa5; 31];
    assert_eq!(
        hmac.authenticate(&[b"message"], &mut mac_output),
        Err(MacError::OutputTooSmall {
            required: 32,
            provided: 31,
        })
    );
    assert_eq!(mac_output, [0xa5; 31]);

    let hkdf = Hkdf::new(Sha2Algorithm::Sha256, None, b"key");
    let mut kdf_output = [0xa5; 15];
    assert_eq!(
        hkdf.expand(&[b"info"], 16, &mut kdf_output),
        Err(KdfError::OutputTooSmall {
            required: 16,
            provided: 15,
        })
    );
    assert_eq!(kdf_output, [0xa5; 15]);
}
