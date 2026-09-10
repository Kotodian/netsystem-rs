use std::future::Future;

use tracing::instrument::WithSubscriber;

pub fn spawn<F>(future: F) -> tokio::task::JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    tokio::spawn(future.with_current_subscriber())
}
