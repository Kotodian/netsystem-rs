use std::collections::HashMap;

pub trait EventSubscriberComponentDeclaration {
    const TYPE_NAME: &'static str;

    fn build(
        logger: hammer_core::log::Logger,
        control_handle: std::sync::Arc<crate::ControlThreadHandle>,
    ) -> hammer_core::error::HammerResult<Vec<crate::ControlEventSubscriptionHandle>>;
}

pub fn register_event_subscriber_component<C>(
    builders: &mut HashMap<&'static str, crate::EventSubscriberBuilder>,
) where
    C: EventSubscriberComponentDeclaration,
{
    builders.insert(C::TYPE_NAME, C::build);
}

#[cfg(any(test))]
macro_rules! register_components {
    (event, $builders:expr, [$($component:path),* $(,)?]) => {
        $(crate::component_registry::register_event_subscriber_component::<$component>($builders);)*
    };
}

#[cfg(test)]
pub(crate) use register_components;

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use hammer_core::error::HammerResult;
    use hammer_core::log::{DiscardWriter, Factory, LogWriter, Logger};
    use hammer_core::metrics::MetricsRegistry;

    use crate::control_thread::SyntheticEventArgs;
    use crate::{
        ControlEventFilter, ControlEventSubscriptionHandle, ControlThread, ControlThreadHandle,
    };

    static EVENT_COMPONENT_RUNS: AtomicUsize = AtomicUsize::new(0);

    #[hammer_component_macros::hammer_component(
        event,
        name = "test-event",
        builder = build_test_event_subscriber
    )]
    struct TestEventSubscriber;

    fn build_test_event_subscriber(
        _logger: Logger,
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

    fn run_control_thread(thread: ControlThread) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("control test runtime");
            runtime.block_on(thread.run());
        })
    }

    fn test_logger(id: &str) -> Logger {
        Factory::new(Instant::now(), Arc::new(DiscardWriter)).new_logger(id)
    }

    #[test]
    fn event_component_macro_registers_subscriber_builder() {
        EVENT_COMPONENT_RUNS.store(0, Ordering::SeqCst);
        let mut builders: HashMap<&'static str, crate::EventSubscriberBuilder> = HashMap::new();
        register_components!(event, &mut builders, [TestEventSubscriber]);
        let builder = *builders
            .get("test-event")
            .expect("event subscriber builder should be registered");

        let writer: Arc<dyn LogWriter> = Arc::new(DiscardWriter);
        let (control_handle, thread) = ControlThread::new(
            Instant::now(),
            writer,
            MetricsRegistry::new(),
            Duration::from_secs(60),
            hammer_core::log::Level::Info,
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
}
