use hammer_service::crypto::{Batch, Engine, Hash, HashInput, HashOperation, HashStatus};

const SHA256_ABC: [u8; 32] = [
    0xba, 0x78, 0x16, 0xbf, 0x8f, 0x01, 0xcf, 0xea, 0x41, 0x41, 0x40, 0xde, 0x5d, 0xae, 0x22, 0x23,
    0xb0, 0x03, 0x61, 0xa3, 0x96, 0x17, 0x7a, 0x9c, 0xb4, 0x10, 0xff, 0x61, 0xf2, 0x00, 0x15, 0xad,
];

#[test]
fn hash_context_executes_a_singleton_batch_synchronously() {
    let engine = Engine::with_builtins().expect("built-in crypto registry is valid");
    let algorithm = engine
        .algorithm::<Hash>("sha-256")
        .expect("SHA-256 is built in");
    let mut context = engine
        .context(algorithm)
        .expect("portable SHA-256 is available");
    let mut output = [0; 32];
    let mut operations = [HashOperation::new(
        HashInput::Contiguous(b"abc"),
        &mut output,
    )];
    let mut batch = Batch::new(&mut operations);

    context.execute(&mut batch);

    assert_eq!(operations[0].status(), HashStatus::Complete { written: 32 });
    assert_eq!(output, SHA256_ABC);
}

#[test]
fn hash_batch_records_success_and_failure_per_operation() {
    let engine = Engine::with_builtins().expect("built-in crypto registry is valid");
    let algorithm = engine
        .algorithm::<Hash>("sha-256")
        .expect("SHA-256 is built in");
    let mut context = engine
        .context(algorithm)
        .expect("portable SHA-256 is available");
    let chunks: [&[u8]; 3] = [b"a", b"b", b"c"];
    let mut valid_output = [0; 32];
    let mut short_output = [0; 31];
    let mut operations = [
        HashOperation::new(HashInput::Scatter(&chunks), &mut valid_output),
        HashOperation::new(HashInput::Contiguous(b"abc"), &mut short_output),
    ];
    let mut batch = Batch::new(&mut operations);

    context.execute(&mut batch);

    assert_eq!(operations[0].status(), HashStatus::Complete { written: 32 });
    assert_eq!(
        operations[1].status(),
        HashStatus::OutputTooSmall {
            required: 32,
            provided: 31,
        }
    );
    assert_eq!(valid_output, SHA256_ABC);
}
