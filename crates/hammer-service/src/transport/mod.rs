use std::sync::Arc;

use arc_swap::ArcSwapOption;
use hammer_core::config::SessionBackend;
use hammer_core::config::network::CongestionController;
use hammer_core::error::HammerResult;
use hammer_core::registry::RuntimeRegistry;

pub mod congestion;
pub mod tcp;
pub mod udp;

use tcp::TcpPolicy;

pub struct TransportMain {
    congestion: CongestionController,
    session_backend: SessionBackend,
    tcp_policy: TcpPolicy,
}

impl TransportMain {
    pub fn new(
        congestion: CongestionController,
        session_backend: SessionBackend,
        tcp_policy: TcpPolicy,
    ) -> Self {
        Self {
            congestion,
            session_backend,
            tcp_policy,
        }
    }

    pub fn congestion(&self) -> CongestionController {
        self.congestion
    }

    pub fn session_backend(&self) -> SessionBackend {
        self.session_backend
    }

    pub fn tcp_policy(&self) -> TcpPolicy {
        self.tcp_policy
    }
}

// VPP alignment: `transport_main_t transport_main;` is a file-scope global in
// VPP's `transport.c`; nodes read it via `&transport_main` (lock-free direct
// deref). `transport_init` fills it once and `vlib_test_cleanup` resets it
// between tests. The Rust mirror is a `pub static ArcSwapOption<TransportMain>`:
// `.load()` is lock-free on the hot path, and `store(None)` makes it resettable
// for test isolation — neither of which `OnceLock` provides.
pub static TRANSPORT_MAIN: ArcSwapOption<TransportMain> = ArcSwapOption::const_empty();

/// Convenience accessor for the config-level session backend enum.
pub fn session_backend() -> Option<SessionBackend> {
    TRANSPORT_MAIN
        .load()
        .as_deref()
        .map(|m| m.session_backend())
}

/// Published `[network.tcp]` policy, if `transport_init` has run.
pub fn tcp_policy() -> Option<TcpPolicy> {
    TRANSPORT_MAIN.load().as_deref().map(|m| m.tcp_policy())
}

/// Active TCP policy: published config, or production defaults when unset.
pub fn active_tcp_policy() -> TcpPolicy {
    tcp_policy().unwrap_or_else(TcpPolicy::production_defaults)
}

#[cfg(test)]
pub(crate) fn reset_for_test() {
    TRANSPORT_MAIN.store(None);
}

pub fn init(reg: &RuntimeRegistry) -> HammerResult<()> {
    init_transport(reg.require::<hammer_core::config::Config>()?)
}

#[hammer_component_macros::init_function(name = "transport_init")]
fn init_transport(config: Arc<hammer_core::config::Config>) -> HammerResult<()> {
    let session_backend = config
        .network
        .session
        .as_ref()
        .map(|session| session.backend)
        .unwrap_or_default();
    TRANSPORT_MAIN.store(Some(Arc::new(TransportMain::new(
        config.network.tcp.congestion,
        session_backend,
        TcpPolicy::from_config(&config.network.tcp),
    ))));
    Ok(())
}

/// Config → CC controller type. Single transport-layer dispatch point.
#[macro_export]
macro_rules! with_congestion {
    (|$cc:ident| $body:expr) => {{
        match crate::transport::TRANSPORT_MAIN
            .load()
            .as_deref()
            .ok_or_else(|| {
                ::hammer_core::error::CoreError::internal("transport main not initialized")
            })?
            .congestion()
        {
            ::hammer_core::config::network::CongestionController::Bbr => {
                type $cc = $crate::transport::congestion::BbrController;
                $body
            }
        }
    }};
}

/// Config → segment type dispatch point.
#[macro_export]
macro_rules! with_segment {
    (|$seg:ident| $body:expr) => {{
        match crate::transport::TRANSPORT_MAIN
            .load()
            .as_deref()
            .ok_or_else(|| {
                ::hammer_core::error::CoreError::internal("transport main not initialized")
            })?
            .session_backend()
        {
            ::hammer_core::config::SessionBackend::Local => {
                type $seg = ::hammer_infra::segment::Local;
                $body
            }
            ::hammer_core::config::SessionBackend::Svm => {
                type $seg = ::hammer_infra::segment::Svm;
                $body
            }
        }
    }};
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use hammer_core::config::Config;
    use hammer_core::config::parse_config;
    use hammer_core::registry::RuntimeRegistry;
    use hammer_runtime::DataWorkerId;

    use super::congestion::{BbrController, CongestionController};
    use super::tcp::{TcpConnection, TcpRetransmitTimeoutState};
    use super::{active_tcp_policy, init, reset_for_test, tcp_policy};

    static TRANSPORT_POLICY_TEST_LOCK: Mutex<()> = Mutex::new(());

    fn with_transport_policy_lock<R>(f: impl FnOnce() -> R) -> R {
        let _guard = TRANSPORT_POLICY_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        reset_for_test();
        let result = f();
        reset_for_test();
        result
    }

    #[test]
    fn transport_init_publishes_typed_tcp_policy_from_config() {
        with_transport_policy_lock(|| {
            let cfg = parse_config(
                r#"
[network.tcp]
mss = 1200
receive_window = 32000
time_wait = "2s"
paws_idle = "1h"
nagle = false

[network.tcp.retransmit]
initial = "100ms"
min = "50ms"
max = "3s"

[network.tcp.keepalive]
idle = "3s"
probe_interval = "1s"
probe_limit = 3
"#,
            )
            .expect("parse");
            let registry = Arc::new(RuntimeRegistry::new());
            registry.set::<Config>(Arc::new(cfg));
            init(&registry).expect("init");

            let policy = tcp_policy().expect("policy");
            assert_eq!(policy.mss, 1200);
            assert_eq!(policy.receive_window, 32000);
            assert_eq!(policy.time_wait, Duration::from_secs(2));
            assert_eq!(policy.paws_idle, Duration::from_secs(3600));
            assert!(!policy.nagle);
            assert_eq!(policy.retransmit_initial, Duration::from_millis(100));
            assert_eq!(policy.retransmit_min, Duration::from_millis(50));
            assert_eq!(policy.retransmit_max, Duration::from_secs(3));
            assert_eq!(policy.keepalive_idle, Duration::from_secs(3));
            assert_eq!(policy.keepalive_probe_interval, Duration::from_secs(1));
            assert_eq!(policy.keepalive_probe_limit, 3);
        });
    }

    #[test]
    fn active_tcp_policy_falls_back_to_production_defaults() {
        with_transport_policy_lock(|| {
            let policy = active_tcp_policy();
            assert_eq!(policy.mss, 1_440);
            assert_eq!(policy.receive_window, u16::MAX as u32);
            assert!(policy.nagle);
            assert_eq!(policy.time_wait, Duration::from_secs(60));
            assert_eq!(policy.keepalive_probe_limit, 8);
        });
    }

    #[test]
    fn tcp_connection_inherits_published_policy_knobs() {
        with_transport_policy_lock(|| {
            let cfg = parse_config(
                r#"
[network.tcp]
mss = 1200
receive_window = 32000
time_wait = "2s"
paws_idle = "1h"
nagle = false

[network.tcp.retransmit]
initial = "100ms"
min = "50ms"
max = "3s"

[network.tcp.keepalive]
idle = "3s"
probe_interval = "1s"
probe_limit = 3
"#,
            )
            .expect("parse");
            let registry = Arc::new(RuntimeRegistry::new());
            registry.set::<Config>(Arc::new(cfg));
            init(&registry).expect("init");

            let remote: SocketAddr = "10.66.77.2:7300".parse().expect("remote");
            let connection = TcpConnection::<BbrController>::new(
                None,
                DataWorkerId::new(0),
                7300,
                Some("10.66.77.1:7300".parse().expect("local")),
                remote,
            );

            assert_eq!(connection.rcv_wnd(), 32_000);
            assert_eq!(connection.snd_wnd(), 32_000);
            assert_eq!(connection.congestion().max_datagram_size(), 1_200);
            assert!(!connection.nagle());
            assert_eq!(connection.time_wait(), Duration::from_secs(2));
            assert_eq!(connection.paws_idle(), Duration::from_secs(3_600));
            assert_eq!(
                connection.retransmit_timeout().retransmit_timeout(),
                Duration::from_millis(100)
            );
            assert_eq!(connection.keepalive_idle(), Duration::from_secs(3));
            assert_eq!(
                connection.keepalive_probe_interval(),
                Duration::from_secs(1)
            );
            assert_eq!(connection.keepalive_probe_limit(), 3);
        });
    }

    #[test]
    fn retransmit_timeout_state_clamps_to_published_bounds() {
        with_transport_policy_lock(|| {
            let cfg = parse_config(
                r#"
[network.tcp.retransmit]
initial = "100ms"
min = "50ms"
max = "3s"
"#,
            )
            .expect("parse");
            let registry = Arc::new(RuntimeRegistry::new());
            registry.set::<Config>(Arc::new(cfg));
            init(&registry).expect("init");

            let mut rto = TcpRetransmitTimeoutState::new();
            assert_eq!(rto.retransmit_timeout(), Duration::from_millis(100));

            rto.observe_ack_sample(Duration::from_nanos(1));
            assert_eq!(rto.retransmit_timeout(), Duration::from_millis(50));

            for _ in 0..16 {
                rto.on_retransmission_timeout();
            }
            assert_eq!(rto.retransmit_timeout(), Duration::from_secs(3));
        });
    }

    fn established_with_mss(mss: u16) -> TcpConnection<BbrController> {
        use hammer_core::protocol::tcp::TcpCapabilities;

        let local: SocketAddr = "10.66.77.1:7300".parse().expect("local");
        let remote: SocketAddr = "10.66.77.2:50001".parse().expect("remote");
        let caps = TcpCapabilities {
            max_segment_size: Some(mss),
            window_scale: None,
            sack: true,
            timestamps: false,
            ecn: false,
            accurate_ecn: false,
            fast_open: false,
        };
        TcpConnection::established_with_capabilities_for_test(
            None,
            DataWorkerId::new(0),
            local.port(),
            Some(local),
            remote,
            caps,
            caps,
        )
    }

    #[test]
    fn path_mtu_clamps_tcp_effective_mss_when_pmtu_enabled() {
        with_transport_policy_lock(|| {
            let cfg = parse_config(
                r#"
[network.tcp]
mss = 1440

[network.tcp.pmtu]
enabled = true
"#,
            )
            .expect("parse");
            let registry = Arc::new(RuntimeRegistry::new());
            registry.set::<Config>(Arc::new(cfg));
            init(&registry).expect("init");

            let mut connection = established_with_mss(1_440);
            assert_eq!(connection.output_payload_len(), 1_440);

            connection.apply_path_mtu(576);

            // IPv4(20) + TCP(20) = 40; effective MSS = 576 - 40 = 536.
            assert_eq!(connection.output_payload_len(), 536);
            assert_eq!(connection.congestion().max_datagram_size(), 536);
            assert_eq!(
                connection.negotiated_options().send_max_segment_size,
                Some(536)
            );
        });
    }

    #[test]
    fn path_mtu_does_not_clamp_when_pmtu_disabled() {
        with_transport_policy_lock(|| {
            let cfg = parse_config(
                r#"
[network.tcp]
mss = 1440

[network.tcp.pmtu]
enabled = false
"#,
            )
            .expect("parse");
            let registry = Arc::new(RuntimeRegistry::new());
            registry.set::<Config>(Arc::new(cfg));
            init(&registry).expect("init");

            let mut connection = established_with_mss(1_440);
            connection.apply_path_mtu(576);

            assert_eq!(connection.output_payload_len(), 1_440);
            assert_eq!(connection.congestion().max_datagram_size(), 1_440);
        });
    }

    #[test]
    fn path_mtu_shrink_with_unacked_data_requests_retransmit() {
        with_transport_policy_lock(|| {
            let cfg = parse_config(
                r#"
[network.tcp]
mss = 1440

[network.tcp.pmtu]
enabled = true
"#,
            )
            .expect("parse");
            let registry = Arc::new(RuntimeRegistry::new());
            registry.set::<Config>(Arc::new(cfg));
            init(&registry).expect("init");

            let mut connection = established_with_mss(1_440);
            connection.mark_unacked_for_path_mtu_test(1_000);
            assert!(connection.apply_path_mtu(576));
            assert_eq!(connection.output_payload_len(), 536);
            assert!(connection.take_path_mtu_retransmit());
            assert!(!connection.take_path_mtu_retransmit());
        });
    }

    #[test]
    fn path_mtu_shrink_arms_tx_intent_under_new_mss() {
        with_transport_policy_lock(|| {
            use hammer_core::protocol::tcp::{TcpCapabilities, TcpSeq};
            use std::time::Instant;

            let cfg = parse_config(
                r#"
[network.tcp]
mss = 1440

[network.tcp.pmtu]
enabled = true
"#,
            )
            .expect("parse");
            let registry = Arc::new(RuntimeRegistry::new());
            registry.set::<Config>(Arc::new(cfg));
            init(&registry).expect("init");

            let mut connection = established_with_mss(1_440);
            let snd_una = TcpSeq::from(connection.snd_una());
            connection.mark_unacked_for_path_mtu_test(1_000);
            assert!(connection.apply_path_mtu(576));

            // Oversized in-flight is retried from snd_una under the new MSS.
            assert_eq!(connection.tx_payload_sequence(), snd_una);
            assert_eq!(
                connection.tx_payload_budget(1_000, Instant::now(), TcpCapabilities::default()),
                536
            );
            assert!(connection.pacing_ready());
            assert!(connection.take_path_mtu_retransmit());
        });
    }

    #[test]
    fn refresh_path_mtu_from_cache_clamps_mss_for_remote() {
        with_transport_policy_lock(|| {
            let cfg = parse_config(
                r#"
[network.tcp]
mss = 1440

[network.tcp.pmtu]
enabled = true
"#,
            )
            .expect("parse");
            let registry = Arc::new(RuntimeRegistry::new());
            registry.set::<Config>(Arc::new(cfg));
            init(&registry).expect("init");

            use crate::net::ip::{
                PathMtuCache, publish_path_mtu_cache, reset_path_mtu_cache_for_test,
            };
            use std::net::Ipv4Addr;

            reset_path_mtu_cache_for_test();
            let cache = PathMtuCache::new();
            cache.apply_ipv4_fragmentation_needed(Ipv4Addr::new(10, 66, 77, 2), 576);
            publish_path_mtu_cache(cache);

            let mut connection = established_with_mss(1_440);
            connection.mark_unacked_for_path_mtu_test(1_000);
            assert!(connection.refresh_path_mtu_from_cache());
            assert_eq!(connection.output_payload_len(), 536);
            assert!(connection.take_path_mtu_retransmit());
            reset_path_mtu_cache_for_test();
        });
    }
}
