use std::collections::HashMap;
use std::sync::Arc;

use hammer_core::error::HammerResult;
use hammer_core::log::Logger;

#[cfg(any(feature = "endpoint-wireguard", feature = "outbound-hysteria2"))]
use crate::component_registry::register_components;
use crate::{ControlEventSubscriptionHandle, ControlThreadHandle};

pub type EventSubscriberBuilder =
    fn(Logger, Arc<ControlThreadHandle>) -> HammerResult<Vec<ControlEventSubscriptionHandle>>;

#[derive(Clone)]
struct EventSubscriberFactorySet {
    builders: Arc<HashMap<&'static str, EventSubscriberBuilder>>,
}

impl EventSubscriberFactorySet {
    fn standard() -> Self {
        let mut builders = HashMap::new();
        register_standard_event_subscriber_builders(&mut builders);
        Self {
            builders: Arc::new(builders),
        }
    }

    fn build_all(
        &self,
        logger: Logger,
        control_handle: Arc<ControlThreadHandle>,
    ) -> HammerResult<Vec<ControlEventSubscriptionHandle>> {
        let mut subscriptions = Vec::new();
        for builder in self.builders.values() {
            subscriptions.extend(builder(logger.clone(), Arc::clone(&control_handle))?);
        }
        Ok(subscriptions)
    }
}

fn register_standard_event_subscriber_builders(
    _builders: &mut HashMap<&'static str, EventSubscriberBuilder>,
) {
    #[cfg(feature = "outbound-hysteria2")]
    register_components!(
        event,
        _builders,
        [crate::protocol::hysteria2::Hysteria2AuthLogSubscriber]
    );
    #[cfg(feature = "endpoint-wireguard")]
    register_components!(
        event,
        _builders,
        [
            crate::protocol::endpoint::wireguard::WireguardStartHandshakeSubscriber,
            crate::protocol::endpoint::wireguard::WireguardInboundControlSubscriber
        ]
    );
}

pub(crate) fn build_standard_event_subscribers(
    logger: Logger,
    control_handle: Arc<ControlThreadHandle>,
) -> HammerResult<Vec<ControlEventSubscriptionHandle>> {
    EventSubscriberFactorySet::standard().build_all(logger, control_handle)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use hammer_core::log::{Level, LogWriter};
    use hammer_core::metrics::MetricsRegistry;

    use crate::ControlEventFilter;
    use crate::ControlThread;
    use crate::component_registry::register_components;
    use crate::control_thread::SyntheticEventArgs;
    #[cfg(feature = "outbound-hysteria2")]
    use crate::{Hysteria2AuthFailureArgs, Hysteria2AuthSuccessArgs};

    #[derive(Default)]
    struct CaptureWriter {
        lines: Mutex<Vec<String>>,
    }

    impl LogWriter for CaptureWriter {
        fn write_message(&self, _level: Level, message: String) {
            self.lines.lock().unwrap().push(message);
        }
    }

    fn run_control_thread(thread: ControlThread) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("control test runtime");
            runtime.block_on(thread.run());
        })
    }

    fn wait_for_captured_line(writer: &CaptureWriter, needle: &str) {
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            let lines = writer.lines.lock().unwrap();
            if lines.iter().any(|line| line.contains(needle)) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "missing log line containing {needle:?}: {lines:?}"
            );
            drop(lines);
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    static EVENT_COMPONENT_RUNS: AtomicUsize = AtomicUsize::new(0);

    #[hammer_component_macros::hammer_component(
        event,
        name = "test-event",
        builder = build_test_event_subscriber
    )]
    struct TestEventSubscriber;

    fn build_test_event_subscriber(
        _logger: hammer_core::log::Logger,
        control_handle: Arc<ControlThreadHandle>,
    ) -> HammerResult<Vec<ControlEventSubscriptionHandle>> {
        let subscription = control_handle.subscribe_event(
            ControlEventFilter::event::<SyntheticEventArgs>(),
            |_| async move {
                EVENT_COMPONENT_RUNS.fetch_add(1, Ordering::SeqCst);
            },
        )?;
        Ok(vec![subscription])
    }

    fn test_logger(id: &str) -> hammer_core::log::Logger {
        hammer_core::log::Factory::new(Instant::now(), Arc::new(hammer_core::log::DiscardWriter))
            .new_logger(id)
    }

    #[test]
    fn event_component_macro_registers_subscriber_builder() {
        EVENT_COMPONENT_RUNS.store(0, Ordering::SeqCst);
        let mut builders: HashMap<&'static str, EventSubscriberBuilder> = HashMap::new();
        register_components!(event, &mut builders, [TestEventSubscriber]);
        let builder = *builders
            .get("test-event")
            .expect("event subscriber builder should be registered");

        let inner = Arc::new(CaptureWriter::default());
        let metrics = MetricsRegistry::new();
        let (control_handle, thread) = ControlThread::new(
            Instant::now(),
            inner.clone(),
            metrics,
            Duration::from_secs(60),
            Level::Info,
        );
        let handle = run_control_thread(thread);
        let _subscriptions = builder(test_logger("test-event"), Arc::clone(&control_handle))
            .expect("build event subscriber");

        control_handle
            .publish_event(SyntheticEventArgs {
                id: Arc::<str>::from("macro"),
                value: 9,
            })
            .expect("publish event for macro subscriber");
        for _ in 0..20 {
            if EVENT_COMPONENT_RUNS.load(Ordering::SeqCst) == 1 {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(EVENT_COMPONENT_RUNS.load(Ordering::SeqCst), 1);

        assert!(control_handle.shutdown_timeout(Duration::from_secs(1)));
        handle.join().expect("control thread join");
    }

    #[cfg(feature = "outbound-hysteria2")]
    #[test]
    fn standard_event_subscribers_log_hysteria2_auth_events() {
        let inner = Arc::new(CaptureWriter::default());
        let metrics = MetricsRegistry::new();
        let (control_handle, thread) = ControlThread::new(
            Instant::now(),
            inner.clone(),
            metrics,
            Duration::from_secs(60),
            Level::Info,
        );
        let handle = run_control_thread(thread);
        let writer: Arc<dyn LogWriter> = control_handle.clone();
        let log_factory = hammer_core::log::Factory::new(Instant::now(), writer);
        let _subscriptions = build_standard_event_subscribers(
            log_factory.new_logger("control-event"),
            Arc::clone(&control_handle),
        )
        .expect("build standard event subscribers");

        control_handle
            .publish_event(Hysteria2AuthSuccessArgs {
                outbound_id: "hysteria2".to_owned(),
                server: "127.0.0.1:443".to_owned(),
                udp_enabled: true,
                rx_auto: false,
                server_rx_bps: 1024,
                send_bps: 2048,
                congestion: "brutal".to_owned(),
            })
            .expect("publish hysteria2 auth success");
        wait_for_captured_line(
            &inner,
            "hysteria2 outbound hysteria2 auth success server=127.0.0.1:443",
        );

        control_handle
            .publish_event(Hysteria2AuthFailureArgs {
                outbound_id: "hysteria2".to_owned(),
                server: "127.0.0.1:443".to_owned(),
                error: "authentication failed, status code: 401 Unauthorized".to_owned(),
            })
            .expect("publish hysteria2 auth failure");
        wait_for_captured_line(
            &inner,
            "hysteria2 outbound hysteria2 auth failed server=127.0.0.1:443",
        );

        assert!(control_handle.shutdown_timeout(Duration::from_secs(1)));
        handle.join().expect("control thread join");
    }
}
