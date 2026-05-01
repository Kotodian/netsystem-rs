use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::Notify;

/// `runtime/service/pause` PauseManager port. Two independent pause sources
/// (device-level pause, e.g. iOS app sleeping; and network-level pause,
/// e.g. radio off) drive a single "active" condition that callers can `await`
/// on via `wait_active`.
pub struct PauseManager {
    device_paused: AtomicBool,
    network_paused: AtomicBool,
    /// Wakers for `wait_active` consumers; notified whenever either pause
    /// source toggles back to active.
    notify: Arc<Notify>,
}

impl PauseManager {
    pub fn new() -> Self {
        Self {
            device_paused: AtomicBool::new(false),
            network_paused: AtomicBool::new(false),
            notify: Arc::new(Notify::new()),
        }
    }

    pub fn device_pause(&self) {
        self.device_paused.store(true, Ordering::SeqCst);
    }

    pub fn device_wake(&self) {
        if self.device_paused.swap(false, Ordering::SeqCst) {
            self.notify.notify_waiters();
        }
    }

    pub fn network_pause(&self) {
        self.network_paused.store(true, Ordering::SeqCst);
    }

    pub fn network_wake(&self) {
        if self.network_paused.swap(false, Ordering::SeqCst) {
            self.notify.notify_waiters();
        }
    }

    pub fn is_device_paused(&self) -> bool {
        self.device_paused.load(Ordering::SeqCst)
    }

    pub fn is_network_paused(&self) -> bool {
        self.network_paused.load(Ordering::SeqCst)
    }

    pub fn is_paused(&self) -> bool {
        self.is_device_paused() || self.is_network_paused()
    }

    /// Resolves immediately when neither pause source is active; otherwise
    /// suspends the caller until `device_wake` / `network_wake` clears the
    /// last source.
    pub async fn wait_active(&self) {
        loop {
            if !self.is_paused() {
                return;
            }
            // `notified()` does not enroll the waiter until the future is
            // first polled, so a `notify_waiters()` racing between the
            // double-check and `await` would be missed. `enable()` registers
            // the wakeup eagerly — the idiom tokio's own `Notify` docs
            // recommend for this exact double-check pattern.
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if !self.is_paused() {
                return;
            }
            notified.await;
        }
    }

    /// Convenience aliases used by the M1 Service API surface (`Service.pause`
    /// / `Service.wake`). Map onto the device-level pause source.
    pub fn pause(&self) {
        self.device_pause();
    }

    pub fn wake(&self) {
        self.device_wake();
    }
}

impl Default for PauseManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::time::{Duration, timeout};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wait_active_returns_immediately_when_not_paused() {
        let pm = PauseManager::new();
        timeout(Duration::from_millis(50), pm.wait_active())
            .await
            .expect("wait_active should not block when not paused");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wait_active_blocks_until_device_wake() {
        let pm = Arc::new(PauseManager::new());
        pm.device_pause();
        let pm2 = Arc::clone(&pm);
        let waker = crate::spawn::spawn(async move {
            tokio::time::sleep(Duration::from_millis(40)).await;
            pm2.device_wake();
        });
        timeout(Duration::from_millis(500), pm.wait_active())
            .await
            .expect("wait_active should resolve after device_wake");
        waker.await.unwrap();
        assert!(!pm.is_paused());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wait_active_holds_until_both_sources_clear() {
        let pm = Arc::new(PauseManager::new());
        pm.device_pause();
        pm.network_pause();
        let pm2 = Arc::clone(&pm);
        crate::spawn::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            pm2.device_wake();
            tokio::time::sleep(Duration::from_millis(20)).await;
            pm2.network_wake();
        });
        timeout(Duration::from_millis(500), pm.wait_active())
            .await
            .expect("wait_active should resolve once both sources are awake");
        assert!(!pm.is_paused());
    }

    #[test]
    fn pause_and_wake_aliases_track_device_source() {
        let pm = PauseManager::new();
        assert!(!pm.is_paused());
        pm.pause();
        assert!(pm.is_device_paused());
        assert!(!pm.is_network_paused());
        pm.wake();
        assert!(!pm.is_paused());
    }
}
