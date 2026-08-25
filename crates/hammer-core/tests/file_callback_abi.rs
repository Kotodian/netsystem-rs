use std::os::fd::OwnedFd;
use std::os::unix::net::UnixStream;

use hammer_core::file::{File, FileFunctions};

struct TestContext;

#[derive(Debug, PartialEq, Eq)]
struct TestError;

fn read_callback(
    _context: &TestContext,
    file: &mut File<TestContext, TestError>,
) -> Result<(), TestError> {
    file.set_private_data(file.private_data() + 1);
    Ok(())
}

#[test]
fn file_callback_abi_is_available_from_hammer_core() {
    let (listener, _peer) = UnixStream::pair().expect("create callback ABI socket");
    let mut file = File::new(
        OwnedFd::from(listener),
        "callback ABI".to_owned(),
        0,
        FileFunctions {
            read: Some(read_callback),
            write: None,
            error: None,
        },
    );

    let context = TestContext;
    (file.functions().read.expect("read callback"))(&context, &mut file)
        .expect("dispatch callback");
    assert_eq!(file.private_data(), 1);
}
