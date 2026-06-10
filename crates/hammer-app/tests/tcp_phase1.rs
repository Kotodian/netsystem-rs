use std::future::Future;
use std::net::{Shutdown, SocketAddr};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll, Waker};
use std::thread;
use std::time::Duration;

use hammer_adapter::DataPlaneRuntime;
use hammer_app::tcp::TcpStream;
use hammer_app::{
    App, AppBufferLease, AppControl, AppControlBackend, AppFlowId, AppObjectRef, AppOpcode,
    AppSqeData,
};
use hammer_core::error::HammerResult;
use hammer_runtime::spawn::{DataRuntime, with_data_plane_buffers};

#[derive(Default)]
struct MockControlBackend {
    next_flow: AtomicU64,
    connects: Mutex<Vec<(SocketAddr, usize, u64)>>,
    closed_flows: Mutex<Vec<u64>>,
}

impl MockControlBackend {
    fn new(first_flow: u64) -> Self {
        Self {
            next_flow: AtomicU64::new(first_flow),
            ..Self::default()
        }
    }
}

impl AppControlBackend for MockControlBackend {
    fn bind_tcp_listener(
        &self,
        _app: &hammer_runtime::app::AppContext,
        _bind: SocketAddr,
        _owner_worker: usize,
    ) -> HammerResult<hammer_runtime::app::AppSocketId> {
        unreachable!("tcp phase1 connect test does not bind listeners")
    }

    fn connect_tcp_stream(
        &self,
        _app: &hammer_runtime::app::AppContext,
        peer: SocketAddr,
        owner_worker: usize,
    ) -> HammerResult<hammer_runtime::app::AppFlowId> {
        let flow = self.next_flow.fetch_add(1, Ordering::Relaxed);
        self.connects
            .lock()
            .expect("connects poisoned")
            .push((peer, owner_worker, flow));
        Ok(hammer_runtime::app::AppFlowId::new(flow))
    }

    fn bind_udp_socket(
        &self,
        _app: &hammer_runtime::app::AppContext,
        _bind: SocketAddr,
        _owner_worker: usize,
    ) -> HammerResult<hammer_runtime::app::AppSocketId> {
        unreachable!("tcp phase1 connect test does not bind udp sockets")
    }

    fn close_socket(
        &self,
        _app: &hammer_runtime::app::AppContext,
        _socket: hammer_runtime::app::AppSocketId,
    ) -> HammerResult<()> {
        Ok(())
    }

    fn close_tcp_flow(
        &self,
        _app: &hammer_runtime::app::AppContext,
        flow: hammer_runtime::app::AppFlowId,
    ) -> HammerResult<()> {
        self.closed_flows
            .lock()
            .expect("closed flows poisoned")
            .push(flow.value());
        Ok(())
    }
}

#[test]
fn tcp_stream_connect_registers_flow_without_reporting_transport_completion() {
    let data_runtime =
        DataRuntime::new(1, "hammer-app-tcp-connect-phase1", 512 * 1024, 2).expect("data runtime");
    let app = App::with_ring_capacity(data_runtime.context(), 4);
    let control_backend = Arc::new(MockControlBackend::new(0x7000));
    let control: Arc<dyn AppControlBackend> = control_backend.clone();
    app.install_control(AppControl::new(control))
        .expect("install control");

    let peer: SocketAddr = "203.0.113.10:443".parse().expect("tcp peer");
    let stream = TcpStream::connect(&app, peer, 0).expect("phase1 connect");
    let flow = stream.flow();

    assert_eq!(flow, AppFlowId::new(0x7000));
    assert_eq!(app.context().owner_worker_for_flow(flow).expect("owner"), 0);
    assert_eq!(
        control_backend
            .connects
            .lock()
            .expect("connects poisoned")
            .as_slice(),
        &[(peer, 0, flow.value())]
    );

    let no_connect_completion = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            tokio::time::timeout(
                Duration::from_millis(20),
                app.spawn(flow, move |flow| async move {
                    flow.backend().next_cqe_descriptor().await
                }),
            )
            .await
            .is_err()
        });

    assert!(
        no_connect_completion,
        "phase1 connect must not synthesize a transport completion"
    );

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn tcp_stream_context_send_buffer_is_backend_visible_without_copying() {
    let data_runtime =
        DataRuntime::new(1, "hammer-app-tcp-send-phase1", 512 * 1024, 2).expect("data runtime");
    let app = App::with_ring_capacity(data_runtime.context(), 4);
    let control_backend = Arc::new(MockControlBackend::new(0x7008));
    let control: Arc<dyn AppControlBackend> = control_backend.clone();
    app.install_control(AppControl::new(control))
        .expect("install control");

    let peer: SocketAddr = "203.0.113.12:443".parse().expect("tcp peer");
    let stream = TcpStream::connect(&app, peer, 0).expect("phase1 connect");
    let flow = stream.flow();

    let result = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async move {
            app.spawn(flow, move |flow_ctx| async move {
                let backend = flow_ctx.backend();
                let stream = TcpStream::new(flow_ctx.ring(), flow_ctx.id());
                let buffers = with_data_plane_buffers(Clone::clone);
                let index = buffers
                    .alloc_index_with_bytes(Default::default(), b"tcp-send-phase1")
                    .expect("alloc tcp send buffer");
                let expected_ptr = buffers.current_ptr(index).expect("send ptr") as usize;

                stream
                    .send_buffer(AppBufferLease::from_buffer(buffers, index))
                    .await
                    .expect("enqueue tcp send");

                let entry = backend
                    .next_submission_entry()
                    .await
                    .expect("send submission entry");
                let descriptor = entry.descriptor();
                let attachment = entry.attachment().expect("send attachment");
                let send_ptr = attachment.lease().current_ptr().expect("attachment ptr") as usize;
                let send_payload = attachment
                    .lease()
                    .copy_current()
                    .expect("attachment payload");

                (descriptor, expected_ptr, send_ptr, send_payload)
            })
            .await
            .expect("spawn send flow")
        });

    assert_eq!(result.0.opcode(), AppOpcode::Send);
    assert_eq!(result.0.object(), AppObjectRef::Flow(flow));
    match result.0.payload() {
        AppSqeData::Send { .. } => {}
        other => panic!("unexpected tcp send sqe payload: {other:?}"),
    }
    assert_eq!(result.1, result.2);
    assert_eq!(result.3, b"tcp-send-phase1");

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn tcp_stream_shutdown_helpers_preserve_read_write_and_both_directions() {
    let data_runtime =
        DataRuntime::new(1, "hammer-app-tcp-shutdown-phase1", 512 * 1024, 2).expect("data runtime");
    let app = App::with_ring_capacity(data_runtime.context(), 4);
    let control_backend = Arc::new(MockControlBackend::new(0x7010));
    let control: Arc<dyn AppControlBackend> = control_backend.clone();
    app.install_control(AppControl::new(control))
        .expect("install control");
    let flow = AppFlowId::new(0x7010);

    let shutdowns = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            app.spawn(flow, move |flow| async move {
                let backend = flow.backend();
                let stream = TcpStream::new(flow.ring(), flow.id());

                stream.shutdown_read().await.expect("shutdown read");
                stream.shutdown_write().await.expect("shutdown write");
                stream.shutdown_both().await.expect("shutdown both");

                [
                    backend
                        .next_tcp_shutdown()
                        .await
                        .expect("read shutdown request"),
                    backend
                        .next_tcp_shutdown()
                        .await
                        .expect("write shutdown request"),
                    backend
                        .next_tcp_shutdown()
                        .await
                        .expect("both shutdown request"),
                ]
            })
            .await
            .expect("spawn shutdown flow")
        });

    assert_eq!(
        shutdowns.map(|shutdown| (shutdown.flow().value(), shutdown.how())),
        [
            (flow.value(), Shutdown::Read),
            (flow.value(), Shutdown::Write),
            (flow.value(), Shutdown::Both),
        ]
    );

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn tcp_stream_close_uses_control_plane_for_context_streams_without_enqueuing_close_sqe() {
    let data_runtime =
        DataRuntime::new(1, "hammer-app-tcp-close-phase1", 512 * 1024, 2).expect("data runtime");
    let app = App::with_ring_capacity(data_runtime.context(), 4);
    let control_backend = Arc::new(MockControlBackend::new(0x7100));
    let control: Arc<dyn AppControlBackend> = control_backend.clone();
    app.install_control(AppControl::new(control))
        .expect("install control");

    let peer: SocketAddr = "203.0.113.11:443".parse().expect("tcp peer");
    let stream = TcpStream::connect(&app, peer, 0).expect("phase1 connect");
    let flow = stream.flow();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (go_tx, go_rx) = tokio::sync::oneshot::channel();
    let (result_tx, result_rx) = std::sync::mpsc::channel();
    let observer = {
        let app = app.clone();
        thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("driver runtime")
                .block_on(async move {
                    let _ = app
                        .spawn(flow, move |flow| async move {
                            let backend = flow.backend();
                            ready_tx.send(()).expect("observer ready");
                            let _ = go_rx.await;
                            let mut future = std::pin::pin!(backend.next_sqe_descriptor());
                            let waker = Waker::noop();
                            let mut cx = Context::from_waker(waker);
                            let observed = match future.as_mut().poll(&mut cx) {
                                Poll::Ready(descriptor) => {
                                    descriptor.map(|descriptor| descriptor.opcode())
                                }
                                Poll::Pending => None,
                            };
                            result_tx.send(observed).expect("send observed close sqe");
                        })
                        .await
                        .expect("observe close sqe");
                })
        })
    };
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async { ready_rx.await.expect("wait for close observer") });

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async { stream.close().await.expect("close stream") });

    assert_eq!(
        control_backend
            .closed_flows
            .lock()
            .expect("closed flows poisoned")
            .as_slice(),
        &[flow.value()]
    );
    assert!(
        app.context().owner_worker_for_flow(flow).is_err(),
        "closed flow must be unregistered from app ownership"
    );

    go_tx.send(()).expect("release close observer");
    let observed = result_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("receive observed close sqe");
    observer.join().expect("close observer join");
    assert_eq!(
        observed, None,
        "context close must route via control plane instead of enqueuing close sqe"
    );

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn tcp_stream_context_send_buffer_hops_to_owner_runtime_queue() {
    let data_runtime =
        DataRuntime::new(1, "hammer-app-tcp-send-phase1", 512 * 1024, 2).expect("data runtime");
    let app = App::with_ring_capacity(data_runtime.context(), 4);
    let control_backend = Arc::new(MockControlBackend::new(0x7150));
    let control: Arc<dyn AppControlBackend> = control_backend.clone();
    app.install_control(AppControl::new(control))
        .expect("install control");

    let peer: SocketAddr = "203.0.113.12:443".parse().expect("tcp peer");
    let stream = TcpStream::connect(&app, peer, 0).expect("phase1 connect");
    let flow = stream.flow();
    let (tx, rx) = std::sync::mpsc::channel();

    let observer = {
        let app = app.clone();
        thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("driver runtime")
                .block_on(async move {
                    let _ = app
                        .spawn(flow, move |flow| async move {
                            let entry = flow
                                .backend()
                                .next_submission_entry()
                                .await
                                .expect("send submission entry");
                            let payload = entry
                                .attachment()
                                .expect("send attachment")
                                .lease()
                                .copy_current()
                                .expect("send payload");
                            tx.send((entry.descriptor().opcode(), payload))
                                .expect("send observed submission");
                        })
                        .await
                        .expect("observe send submission");
                })
        })
    };

    let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
    let index = runtime
        .alloc_index_with_bytes(Default::default(), b"context-send")
        .expect("alloc send buffer");
    let lease =
        hammer_app::AppBufferLease::from_buffer(std::ops::Deref::deref(&runtime).clone(), index);

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async { stream.send_buffer(lease).await.expect("send buffer") });

    let observed = rx
        .recv_timeout(Duration::from_secs(1))
        .expect("receive observed submission");
    observer.join().expect("send observer join");
    assert_eq!(observed.0, hammer_app::AppOpcode::Send);
    assert_eq!(observed.1, b"context-send");

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn tcp_stream_context_backend_observes_closed_cqe_after_control_plane_close() {
    let data_runtime = DataRuntime::new(1, "hammer-app-tcp-closed-cqe-phase1", 512 * 1024, 2)
        .expect("data runtime");
    let app = App::with_ring_capacity(data_runtime.context(), 4);
    let flow = AppFlowId::new(0x7200);
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (go_tx, go_rx) = tokio::sync::oneshot::channel();
    let (result_tx, result_rx) = std::sync::mpsc::channel();

    let observer = {
        let app = app.clone();
        thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("driver runtime")
                .block_on(async move {
                    let _ = app
                        .spawn(flow, move |flow| async move {
                            let backend = flow.backend();
                            ready_tx.send(()).expect("closed observer ready");
                            let _ = go_rx.await;
                            let cqe = backend
                                .next_cqe_descriptor()
                                .await
                                .expect("closed cqe descriptor");
                            result_tx.send(cqe.payload()).expect("send closed payload");
                        })
                        .await
                        .expect("observe closed cqe");
                })
        })
    };

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async { ready_rx.await.expect("wait for closed observer") });

    let app_for_enqueue = app.clone();
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async move {
            app_for_enqueue
                .spawn(flow, move |flow| async move {
                    flow.backend()
                        .try_push_cqe_descriptor(hammer_app::AppCqeDescriptor::new(
                            hammer_app::AppUserData::new(0),
                            0,
                            hammer_app::AppCqeFlags::NONE,
                            hammer_app::AppObjectRef::Flow(flow.id()),
                            hammer_app::AppCqeData::Closed {
                                flow: Some(flow.id()),
                                socket: None,
                            },
                        ))
                        .expect("enqueue closed cqe");
                })
                .await
                .expect("spawn closed cqe enqueue");
        });

    go_tx.send(()).expect("release closed observer");
    let observed = result_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("receive closed payload");
    observer.join().expect("closed observer join");

    assert_eq!(
        observed,
        hammer_app::AppCqeData::Closed {
            flow: Some(flow),
            socket: None,
        }
    );

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn tcp_stream_recv_buffer_reports_stream_closed_when_closed_cqe_arrives() {
    let data_runtime = DataRuntime::new(1, "hammer-app-tcp-closed-recv-phase1", 512 * 1024, 2)
        .expect("data runtime");
    let app = App::with_ring_capacity(data_runtime.context(), 4);
    let flow = AppFlowId::new(0x7201);

    let result = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            app.spawn(flow, move |flow| async move {
                let backend = flow.backend();
                let stream = TcpStream::new(flow.ring(), flow.id());
                let recv = flow.spawn_local({
                    let stream = stream.clone();
                    move || async move {
                        match stream.recv_buffer().await {
                            Ok(lease) => {
                                let len = lease.current_len().expect("recv lease len");
                                lease.release();
                                format!("unexpected recv:{len}")
                            }
                            Err(err) => err.to_string(),
                        }
                    }
                });

                let recv_sqe = backend
                    .next_sqe_descriptor()
                    .await
                    .expect("recv sqe descriptor");
                assert_eq!(recv_sqe.opcode(), hammer_app::AppOpcode::Recv);
                assert_eq!(recv_sqe.object(), hammer_app::AppObjectRef::Flow(flow.id()));

                backend
                    .try_push_cqe_descriptor(hammer_app::AppCqeDescriptor::new(
                        hammer_app::AppUserData::new(0),
                        0,
                        hammer_app::AppCqeFlags::NONE,
                        hammer_app::AppObjectRef::Flow(flow.id()),
                        hammer_app::AppCqeData::Closed {
                            flow: Some(flow.id()),
                            socket: None,
                        },
                    ))
                    .expect("push closed cqe");

                recv.await.expect("join recv future")
            })
            .await
            .expect("spawn closed recv flow")
        });

    assert!(
        result.contains("tcp stream closed"),
        "unexpected recv result: {result}"
    );

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}
