use hammer_infra::crypto::{HashError, sha256};

const SHA256_ABC: [u8; 32] = [
    0xba, 0x78, 0x16, 0xbf, 0x8f, 0x01, 0xcf, 0xea, 0x41, 0x41, 0x40, 0xde, 0x5d, 0xae, 0x22, 0x23,
    0xb0, 0x03, 0x61, 0xa3, 0x96, 0x17, 0x7a, 0x9c, 0xb4, 0x10, 0xff, 0x61, 0xf2, 0x00, 0x15, 0xad,
];

#[test]
fn sha256_writes_the_known_digest_to_caller_memory() {
    let mut output = [0; 32];

    sha256(&[b"abc"], &mut output).expect("SHA-256 output has the required capacity");

    assert_eq!(output, SHA256_ABC);
}

#[test]
fn sha256_hashes_scatter_gather_input_without_joining_it() {
    let mut output = [0; 32];

    sha256(&[b"a", b"b", b"c"], &mut output).expect("SHA-256 output has the required capacity");

    assert_eq!(output, SHA256_ABC);
}

#[test]
fn sha256_rejects_insufficient_caller_output() {
    let mut output = [0; 31];

    let error = sha256(&[b"abc"], &mut output).expect_err("output is one byte too short");

    assert_eq!(
        error,
        HashError::OutputTooSmall {
            required: 32,
            provided: 31,
        }
    );
}
