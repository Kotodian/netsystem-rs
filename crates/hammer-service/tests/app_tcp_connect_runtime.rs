use std::net::{Shutdown, SocketAddr};
use std::sync::Arc;

use hammer_core::error::HammerResult;
use hammer_core::log::DiscardWriter;
use hammer_runtime::adapter::PlatformInterface;
use hammer_runtime::app::AppTcpShutdown;
use hammer_service::RuntimeService;

struct NoopPlatform;

impl PlatformInterface for NoopPlatform {
    fn open_tun(&self, _options: hammer_runtime::adapter::TunOptions) -> HammerResult<i32> {
        Ok(42)
    }

    fn use_platform_auto_detect_interface_control(&self) -> bool {
        false
    }

    fn auto_detect_interface_control(&self, _fd: i32) -> HammerResult<()> {
        Ok(())
    }

    fn start_default_interface_monitor(
        &self,
        _listener: Arc<dyn hammer_runtime::adapter::DefaultInterfaceUpdateListener>,
    ) -> HammerResult<()> {
        Ok(())
    }

    fn close_default_interface_monitor(
        &self,
        _listener: Arc<dyn hammer_runtime::adapter::DefaultInterfaceUpdateListener>,
    ) -> HammerResult<()> {
        Ok(())
    }

    fn get_interfaces(&self) -> HammerResult<Vec<hammer_runtime::adapter::NetworkInterface>> {
        Ok(Vec::new())
    }

    fn read_wifi_state(&self) -> Option<hammer_runtime::adapter::WifiState> {
        None
    }
}

fn minimal_config() -> &'static str {
    r#"
[log]
level = "debug"

[tun]
interface_name = "utun"
address = ["172.19.0.1/30"]
route_address = ["0.0.0.0/0"]
auto_route = false
strict_route = true
mtu = 1400
stack = "disabled"

[dns]
server = "udp://1.1.1.1"

[[outbounds]]
type = "direct"
id = "direct"

[route]
final = "direct"
"#
}

fn new_test_service() -> Arc<RuntimeService> {
    RuntimeService::new(
        minimal_config(),
        Arc::new(NoopPlatform),
        Arc::new(DiscardWriter),
    )
    .expect("test service should build")
}

fn enqueue_shutdown(
    app: &hammer_runtime::app::AppContext,
    flow: hammer_runtime::app::AppFlowId,
) -> AppTcpShutdown {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            app.spawn_on_flow(flow, move |worker| async move {
                worker
                    .runtime()
                    .shutdown(Shutdown::Write)
                    .await
                    .expect("enqueue shutdown");
                worker
                    .backend()
                    .next_tcp_shutdown()
                    .await
                    .expect("tcp shutdown request")
            })
            .await
            .expect("spawn flow task")
        })
}

#[test]
fn service_tcp_connect_registers_requested_owner_worker() {
    let service = new_test_service();
    let app = service.app_context();
    let peer: SocketAddr = "198.51.100.42:443".parse().expect("tcp peer");

    let flow = app.connect_tcp_stream(peer, 1).expect("connect tcp stream");

    assert_eq!(app.owner_worker_for_flow(flow).expect("flow owner"), 1);

    let shutdown = enqueue_shutdown(&app, flow);
    assert_eq!(shutdown.flow(), flow);
    assert_eq!(shutdown.how(), Shutdown::Write);

    service.close().expect("close service");
}

#[test]
fn service_tcp_connect_allocates_distinct_pending_flows() {
    let service = new_test_service();
    let app = service.app_context();
    let peer: SocketAddr = "198.51.100.42:443".parse().expect("tcp peer");

    let first = app.connect_tcp_stream(peer, 0).expect("first connect");
    let second = app.connect_tcp_stream(peer, 1).expect("second connect");

    assert_ne!(first, second);
    assert_eq!(app.owner_worker_for_flow(first).expect("first owner"), 0);
    assert_eq!(app.owner_worker_for_flow(second).expect("second owner"), 1);

    let first_shutdown = enqueue_shutdown(&app, first);
    let second_shutdown = enqueue_shutdown(&app, second);
    assert_eq!(first_shutdown.flow(), first);
    assert_eq!(second_shutdown.flow(), second);

    service.close().expect("close service");
}
