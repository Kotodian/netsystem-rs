use std::cell::RefCell;
use std::os::fd::{OwnedFd, RawFd};
use std::os::unix::net::UnixStream;

use hammer_component_macros::file;
use hammer_core::file::File;

#[derive(Debug, PartialEq, Eq)]
enum FixtureError {
    Callback,
}

#[derive(Debug, PartialEq, Eq)]
enum TypedError {
    Callback(FixtureError),
}

impl From<FixtureError> for TypedError {
    fn from(error: FixtureError) -> Self {
        Self::Callback(error)
    }
}

trait CallbackContext {
    fn record(&self, kind: char, fd: RawFd);
}

#[derive(Default)]
struct TestContext {
    events: RefCell<Vec<(char, RawFd)>>,
}

impl CallbackContext for TestContext {
    fn record(&self, kind: char, fd: RawFd) {
        self.events.borrow_mut().push((kind, fd));
    }
}

#[file]
mod fixture {
    use super::*;

    fn read<Context, Error>(context: &Context, file: &mut File<Context, Error>) -> Result<(), Error>
    where
        Context: CallbackContext,
        Error: From<FixtureError>,
    {
        context.record('r', file.fd());
        Ok(())
    }

    fn error<Context, Error>(
        context: &Context,
        file: &mut File<Context, Error>,
    ) -> Result<(), Error>
    where
        Context: CallbackContext,
        Error: From<FixtureError>,
    {
        context.record('e', file.fd());
        Err(FixtureError::Callback.into())
    }
}

#[test]
fn file_macro_constructor_dispatches_distinct_callbacks_through_core_abi() {
    let functions = fixture::file_functions::<TestContext, TypedError>();
    let (stream, _peer) = UnixStream::pair().expect("create callback fixture socket");
    let mut file = File::new(
        OwnedFd::from(stream),
        "file macro fixture".to_owned(),
        0,
        functions,
    );
    let expected_fd = file.fd();
    let registered = file.functions();
    let context = TestContext::default();

    (registered.read.expect("read callback"))(&context, &mut file).expect("read callback");
    let error = (registered.error.expect("error callback"))(&context, &mut file)
        .expect_err("error callback must convert its typed error");

    assert_eq!(error, TypedError::Callback(FixtureError::Callback));
    assert_eq!(
        context.events.borrow().as_slice(),
        &[('r', expected_fd), ('e', expected_fd)]
    );
    assert!(registered.write.is_none());
}
