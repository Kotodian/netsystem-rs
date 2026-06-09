use std::future::Future;
use std::net::Shutdown;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use hammer_adapter::BufferIndex;
use hammer_adapter::RouteMetadata;
use hammer_core::SocksAddr;
use hammer_runtime::app::{
    AppBufferLease, AppCompletionEntry, AppContext, AppControl, AppControlBackend, AppCqe,
    AppCqeData, AppCqeDescriptor, AppCqeFlags, AppCqeKind, AppFlowId, AppObjectRef, AppOpcode,
    AppRegisteredBuffer, AppRingHandle, AppSocketId, AppSqe, AppSqeData, AppSqeDescriptor,
    AppSubmissionEntry, AppUserData, TransportKind,
};
use hammer_runtime::spawn::{DataRuntime, with_data_plane_buffers};

#[derive(Default)]
struct MockControlBackend {
    next_socket: AtomicU64,
    next_flow: AtomicU64,
    tcp_connects: Mutex<Vec<(SocketAddr, usize, u64)>>,
}

impl MockControlBackend {
    fn alloc_socket(&self) -> AppSocketId {
        AppSocketId::new(self.next_socket.fetch_add(1, Ordering::Relaxed) + 1)
    }

    fn alloc_flow(&self) -> AppFlowId {
        AppFlowId::new(self.next_flow.fetch_add(1, Ordering::Relaxed) + 1)
    }
}

impl AppControlBackend for MockControlBackend {
    fn bind_tcp_listener(
        &self,
        _app: &AppContext,
        _bind: SocketAddr,
        _owner_worker: usize,
    ) -> hammer_core::error::HammerResult<AppSocketId> {
        Ok(self.alloc_socket())
    }

    fn bind_udp_socket(
        &self,
        _app: &AppContext,
        _bind: SocketAddr,
        _owner_worker: usize,
    ) -> hammer_core::error::HammerResult<AppSocketId> {
        Ok(self.alloc_socket())
    }

    fn connect_tcp_stream(
        &self,
        _app: &AppContext,
        peer: SocketAddr,
        owner_worker: usize,
    ) -> hammer_core::error::HammerResult<AppFlowId> {
        let flow = self.alloc_flow();
        self.tcp_connects
            .lock()
            .expect("tcp connects poisoned")
            .push((peer, owner_worker, flow.value()));
        Ok(flow)
    }

    fn close_socket(
        &self,
        _app: &AppContext,
        _socket: AppSocketId,
    ) -> hammer_core::error::HammerResult<()> {
        Ok(())
    }

    fn close_tcp_flow(
        &self,
        _app: &AppContext,
        _flow: AppFlowId,
    ) -> hammer_core::error::HammerResult<()> {
        Ok(())
    }
}

#[test]
fn app_ring_surface_covers_tcp_and_udp_shapes() {
    let tcp = AppSqe::recv(AppUserData::new(7), AppFlowId::new(11), 2048);
    assert_eq!(tcp.user_data(), AppUserData::new(7));
    assert_eq!(tcp.transport(), Some(TransportKind::Tcp));
    assert_eq!(tcp.opcode(), AppOpcode::Recv);
    assert_eq!(tcp.flow(), Some(AppFlowId::new(11)));
    assert_eq!(tcp.socket(), None);

    let udp = AppSqe::recv_from(AppUserData::new(8), AppSocketId::new(13), 2048);
    assert_eq!(udp.user_data(), AppUserData::new(8));
    assert_eq!(udp.transport(), Some(TransportKind::Udp));
    assert_eq!(udp.opcode(), AppOpcode::RecvFrom);
    assert_eq!(udp.socket(), Some(AppSocketId::new(13)));
    assert_eq!(udp.flow(), None);

    let cqe = AppCqe::new(
        AppUserData::new(9),
        AppCqeKind::RecvFrom {
            socket: AppSocketId::new(13),
            source: "127.0.0.1:5353".parse().expect("socket addr"),
            recv: None,
            truncated: false,
        },
    );
    assert_eq!(cqe.user_data(), AppUserData::new(9));
    assert_eq!(cqe.transport(), Some(TransportKind::Udp));
    assert_eq!(cqe.opcode(), AppOpcode::RecvFrom);
    assert!(matches!(cqe.kind(), AppCqeKind::RecvFrom { .. }));
}

#[test]
fn app_ring_handle_batches_submissions_and_completions() {
    let ring = AppRingHandle::for_tests(4, 4);
    ring.push_test_submission(AppSqe::nop(AppUserData::new(1)))
        .expect("first sqe");
    ring.push_test_submission(AppSqe::close_flow(AppUserData::new(2), AppFlowId::new(77)))
        .expect("second sqe");

    let sqes = ring.take_test_submissions(8);
    assert_eq!(sqes.len(), 2);
    assert_eq!(sqes[0].opcode(), AppOpcode::Nop);
    assert_eq!(sqes[1].opcode(), AppOpcode::Close);
    assert_eq!(sqes[1].flow(), Some(AppFlowId::new(77)));

    ring.push_test_completion(AppCqe::closed(
        AppUserData::new(2),
        Some(AppFlowId::new(77)),
    ))
    .expect("closed cqe");

    let cqes = ring.take_test_completions(8);
    assert_eq!(cqes.len(), 1);
    assert_eq!(cqes[0].user_data(), AppUserData::new(2));
    assert_eq!(cqes[0].opcode(), AppOpcode::Close);
}

#[test]
fn app_context_connect_tcp_stream_registers_owner_via_control_backend() {
    let data_runtime =
        DataRuntime::new(2, "app-connect-control-test", 512 * 1024, 2).expect("data runtime");
    let app = AppContext::with_ring_capacity(data_runtime.context(), 2);
    let control_backend = Arc::new(MockControlBackend::default());
    let control: Arc<dyn AppControlBackend> = control_backend.clone();
    app.install_control(AppControl::new(control))
        .expect("install control");

    let peer: SocketAddr = "198.51.100.42:443".parse().expect("tcp peer");
    let flow = app.connect_tcp_stream(peer, 1).expect("connect tcp stream");

    assert_eq!(app.owner_worker_for_flow(flow).expect("flow owner"), 1);
    let connects = control_backend
        .tcp_connects
        .lock()
        .expect("tcp connects poisoned");
    assert_eq!(connects.as_slice(), &[(peer, 1, flow.value())]);

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn app_runtime_shutdown_enqueues_flow_owned_tcp_shutdown_request() {
    let data_runtime =
        DataRuntime::new(1, "app-runtime-shutdown-test", 512 * 1024, 2).expect("data runtime");
    let app = AppContext::with_ring_capacity(data_runtime.context(), 2);
    let flow = AppFlowId::new(0x51);

    let shutdown = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            app.spawn_on_flow(flow, move |worker| async move {
                let backend = worker.backend();
                let app_runtime = worker.runtime();

                app_runtime
                    .shutdown(Shutdown::Write)
                    .await
                    .expect("enqueue shutdown");

                backend
                    .next_tcp_shutdown()
                    .await
                    .expect("tcp shutdown request")
            })
            .await
            .expect("spawn flow task")
        });

    assert_eq!(shutdown.flow(), flow);
    assert_eq!(shutdown.how(), Shutdown::Write);

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn app_runtime_shutdown_preserves_read_write_and_both_directions() {
    let data_runtime = DataRuntime::new(1, "app-runtime-shutdown-directions-test", 512 * 1024, 2)
        .expect("data runtime");
    let app = AppContext::with_ring_capacity(data_runtime.context(), 4);
    let flow = AppFlowId::new(0x52);

    let shutdowns = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            app.spawn_on_flow(flow, move |worker| async move {
                let backend = worker.backend();
                let app_runtime = worker.runtime();

                app_runtime
                    .shutdown(Shutdown::Read)
                    .await
                    .expect("enqueue read shutdown");
                app_runtime
                    .shutdown(Shutdown::Write)
                    .await
                    .expect("enqueue write shutdown");
                app_runtime
                    .shutdown(Shutdown::Both)
                    .await
                    .expect("enqueue both shutdown");

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
            .expect("spawn flow task")
        });

    assert_eq!(
        shutdowns.map(|shutdown| shutdown.flow()),
        [flow, flow, flow]
    );
    assert_eq!(
        shutdowns.map(|shutdown| shutdown.how()),
        [Shutdown::Read, Shutdown::Write, Shutdown::Both]
    );

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn app_send_descriptor_uses_buffer_handle_not_payload_object() {
    let data_runtime =
        DataRuntime::new(1, "app-ring-descriptor-test", 512 * 1024, 2).expect("data runtime");
    let app = AppContext::with_ring_capacity(data_runtime.context(), 2);

    let descriptor = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            app.spawn_on_flow(AppFlowId::new(31), move |_worker| async move {
                let runtime = with_data_plane_buffers(Clone::clone);
                let index = runtime
                    .alloc_index_with_bytes(Default::default(), b"descriptor-buffer")
                    .expect("alloc app send buffer");
                let lease = AppBufferLease::from_buffer(runtime, index);
                let send = hammer_runtime::app::AppSend::new(lease);
                send.descriptor(AppUserData::new(55), AppFlowId::new(31))
                    .expect("send descriptor")
            })
            .await
            .expect("spawn flow task")
        });

    assert_eq!(descriptor.opcode(), AppOpcode::Send);
    assert_eq!(descriptor.user_data(), AppUserData::new(55));
    assert_eq!(descriptor.object(), AppObjectRef::Flow(AppFlowId::new(31)));
    match descriptor.payload() {
        AppSqeData::Send { buffer } => assert_ne!(buffer, buffer_index(0, 0, 0)),
        other => panic!("unexpected sqe data: {other:?}"),
    }

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn app_runtime_send_enqueues_flow_owned_send_sqe_descriptor() {
    let data_runtime =
        DataRuntime::new(1, "app-runtime-send-sqe-test", 512 * 1024, 2).expect("data runtime");
    let app = AppContext::with_ring_capacity(data_runtime.context(), 2);

    let descriptor = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            app.spawn_on_flow(AppFlowId::new(61), move |worker| async move {
                let backend = worker.backend();
                let app_runtime = worker.runtime();
                let runtime = with_data_plane_buffers(Clone::clone);
                let recv_future = app_runtime.recv();
                let recv_sqe = backend
                    .next_sqe_descriptor()
                    .await
                    .expect("recv sqe descriptor");
                let index = runtime
                    .alloc_index_with_bytes(Default::default(), b"runtime-send-sqe")
                    .expect("alloc app send buffer");

                backend
                    .complete_recv(AppBufferLease::from_buffer(runtime.clone(), index))
                    .await
                    .expect("complete recv cqe");

                let recv = recv_future.await.expect("recv cqe");
                app_runtime.send(recv.into_send()).await.expect("send sqe");

                (
                    recv_sqe,
                    backend
                        .next_sqe()
                        .await
                        .expect("send sqe")
                        .descriptor()
                        .expect("sqe descriptor"),
                )
            })
            .await
            .expect("spawn flow task")
        });

    assert_eq!(descriptor.0.opcode(), AppOpcode::Recv);
    assert_eq!(
        descriptor.0.object(),
        AppObjectRef::Flow(AppFlowId::new(61))
    );
    assert_eq!(descriptor.1.opcode(), AppOpcode::Send);
    assert_eq!(
        descriptor.1.object(),
        AppObjectRef::Flow(AppFlowId::new(61))
    );
    match descriptor.1.payload() {
        AppSqeData::Send { buffer } => assert_ne!(buffer, buffer_index(0, 0, 0)),
        other => panic!("unexpected sqe data: {other:?}"),
    }

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn app_context_try_complete_recv_buffer_enqueues_flow_owned_cqe() {
    let data_runtime =
        DataRuntime::new(1, "app-complete-recv-cqe-test", 512 * 1024, 2).expect("data runtime");
    let app = AppContext::with_ring_capacity(data_runtime.context(), 2);
    let flow = AppFlowId::new(71);

    let result = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            let app_for_worker = app.clone();
            app.spawn_on_flow(flow, move |worker| async move {
                let backend = worker.backend();
                let app_runtime = worker.runtime();
                let recv_future = app_runtime.recv();
                let recv_sqe = backend
                    .next_sqe_descriptor()
                    .await
                    .expect("recv sqe descriptor");
                let runtime = with_data_plane_buffers(Clone::clone);
                let before_in_use = with_data_plane_buffers(|runtime| runtime.in_use_buffers());
                let index = runtime
                    .alloc_index_with_bytes(Default::default(), b"complete-recv")
                    .expect("alloc app recv buffer");
                let expected_ptr = runtime.current_ptr(index).expect("buffer pointer") as usize;

                app_for_worker
                    .try_complete_recv_buffer(flow, runtime.clone(), index, false)
                    .expect("enqueue recv cqe");

                assert_eq!(recv_sqe.user_data(), AppUserData::new(0));
                let recv = recv_future.await.expect("recv cqe");
                let descriptor = AppCqeDescriptor::new(
                    recv_sqe.user_data(),
                    recv.lease().current_len().expect("recv len") as i32,
                    AppCqeFlags::BUFFER,
                    AppObjectRef::Flow(flow),
                    AppCqeData::Recv {
                        flow,
                        buffer: recv.lease().index(),
                    },
                );
                let recv_ptr = recv.lease().current_ptr().expect("recv pointer") as usize;
                let recv_payload = recv.lease().copy_current().expect("recv payload");
                recv.release();
                let after_in_use = with_data_plane_buffers(|runtime| runtime.in_use_buffers());

                (
                    descriptor,
                    expected_ptr,
                    recv_ptr,
                    recv_payload,
                    before_in_use,
                    after_in_use,
                )
            })
            .await
            .expect("spawn flow task")
        });

    assert_eq!(result.0.user_data(), AppUserData::new(0));
    assert_eq!(result.0.result(), "complete-recv".len() as i32);
    assert_eq!(result.0.object(), AppObjectRef::Flow(flow));
    assert!(result.0.flags().contains(AppCqeFlags::BUFFER));
    match result.0.payload() {
        AppCqeData::Recv {
            flow: recv_flow,
            buffer,
        } => {
            assert_eq!(recv_flow, flow);
            assert_ne!(buffer, buffer_index(0, 0, 0));
        }
        other => panic!("unexpected cqe data: {other:?}"),
    }
    assert_eq!(result.1, result.2);
    assert_eq!(result.3, b"complete-recv");
    assert_eq!(result.4, 0);
    assert_eq!(result.5, 0);

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn app_context_try_complete_recv_buffer_requires_pending_recv_submission() {
    let data_runtime = DataRuntime::new(1, "app-complete-recv-requires-sqe-test", 512 * 1024, 2)
        .expect("data runtime");
    let app = AppContext::with_ring_capacity(data_runtime.context(), 2);
    let flow = AppFlowId::new(72);

    let result = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            let app_for_worker = app.clone();
            app.spawn_on_flow(flow, move |_worker| async move {
                let runtime = with_data_plane_buffers(Clone::clone);
                let index = runtime
                    .alloc_index_with_bytes(Default::default(), b"missing-recv-sqe")
                    .expect("alloc app recv buffer");

                let err = app_for_worker
                    .try_complete_recv_buffer(flow, runtime.clone(), index, false)
                    .expect_err("recv completion should require a pending recv sqe");

                (err.to_string(), runtime.in_use_buffers())
            })
            .await
            .expect("spawn flow task")
        });

    assert!(
        result.0.contains("pending recv"),
        "unexpected error: {}",
        result.0
    );
    assert_eq!(result.1, 1);

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn app_context_try_complete_recv_from_buffer_enqueues_socket_owned_cqe() {
    let data_runtime = DataRuntime::new(1, "app-complete-recv-from-cqe-test", 512 * 1024, 2)
        .expect("data runtime");
    let app = AppContext::with_ring_capacity(data_runtime.context(), 2);
    app.install_control(AppControl::new(Arc::new(MockControlBackend::default())))
        .expect("install control");
    let socket = app
        .bind_udp_socket("127.0.0.1:5353".parse().expect("udp bind"), 0)
        .expect("bind udp socket");
    let source = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 7)), 40007);

    let (tx, rx) = std::sync::mpsc::channel();
    data_runtime
        .context()
        .spawn_local_on_worker(0, {
            let app = app.clone();
            move || async move {
                let backend = app
                    .local_backend_for_socket(socket)
                    .expect("socket backend");
                backend
                    .try_push_sqe_descriptor(AppSqeDescriptor::new(
                        AppOpcode::RecvFrom,
                        AppUserData::new(17),
                        AppObjectRef::Socket(socket),
                        AppSqeData::RecvFrom { max_len: u32::MAX },
                    ))
                    .expect("push recv_from sqe");

                let runtime = with_data_plane_buffers(Clone::clone);
                let before_in_use = runtime.in_use_buffers();
                let index = runtime
                    .alloc_index_with_bytes(
                        RouteMetadata {
                            source: Some(SocksAddr::ip(source.ip(), source.port())),
                            ..Default::default()
                        },
                        b"complete-recv-from",
                    )
                    .expect("alloc recv_from buffer");
                let expected_ptr = runtime.current_ptr(index).expect("buffer pointer") as usize;

                app.try_complete_recv_from_buffer(socket, source, runtime.clone(), index, false)
                    .expect("enqueue recv_from cqe");

                let descriptor = backend
                    .next_cqe_descriptor()
                    .await
                    .expect("recv_from cqe descriptor");
                let recv = backend
                    .take_completion_buffer(match descriptor.payload() {
                        AppCqeData::RecvFrom { buffer, .. } => buffer,
                        other => panic!("unexpected cqe payload: {other:?}"),
                    })
                    .expect("take recv_from buffer");
                let recv_ptr = recv.lease().current_ptr().expect("recv pointer") as usize;
                let recv_payload = recv.lease().copy_current().expect("recv payload");
                recv.release();
                let after_in_use = with_data_plane_buffers(|runtime| runtime.in_use_buffers());

                tx.send((
                    descriptor,
                    expected_ptr,
                    recv_ptr,
                    recv_payload,
                    before_in_use,
                    after_in_use,
                ))
                .expect("send recv_from result");
            }
        })
        .expect("spawn recv_from worker");

    let result = rx
        .recv_timeout(Duration::from_secs(1))
        .expect("receive recv_from result");

    assert_eq!(result.0.user_data(), AppUserData::new(17));
    assert_eq!(result.0.object(), AppObjectRef::Socket(socket));
    assert!(result.0.flags().contains(AppCqeFlags::BUFFER));
    match result.0.payload() {
        AppCqeData::RecvFrom {
            socket: recv_socket,
            source: recv_source,
            buffer,
        } => {
            assert_eq!(recv_socket, socket);
            assert_eq!(recv_source, source);
            assert_ne!(buffer, buffer_index(0, 0, 0));
        }
        other => panic!("unexpected cqe payload: {other:?}"),
    }
    assert_eq!(result.1, result.2);
    assert_eq!(result.3, b"complete-recv-from");
    assert_eq!(result.4, 0);
    assert_eq!(result.5, 0);

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn app_context_try_complete_accept_enqueues_listener_owned_cqe() {
    let data_runtime =
        DataRuntime::new(1, "app-complete-accept-cqe-test", 512 * 1024, 2).expect("data runtime");
    let app = AppContext::with_ring_capacity(data_runtime.context(), 2);
    app.install_control(AppControl::new(Arc::new(MockControlBackend::default())))
        .expect("install control");
    let listener = app
        .bind_tcp_listener("127.0.0.1:7000".parse().expect("tcp bind"), 0)
        .expect("bind tcp listener");
    let accepted_flow = AppFlowId::new(0x7001);

    let (tx, rx) = std::sync::mpsc::channel();
    data_runtime
        .context()
        .spawn_local_on_worker(0, {
            let app = app.clone();
            move || async move {
                let backend = app
                    .local_backend_for_socket(listener)
                    .expect("listener backend");
                backend
                    .try_push_sqe_descriptor(AppSqeDescriptor::new(
                        AppOpcode::Accept,
                        AppUserData::new(23),
                        AppObjectRef::Socket(listener),
                        AppSqeData::Accept,
                    ))
                    .expect("push accept sqe");

                app.try_complete_accept(listener, accepted_flow)
                    .expect("enqueue accept cqe");

                tx.send((
                    backend
                        .next_cqe_descriptor()
                        .await
                        .expect("accept cqe descriptor"),
                    app.owner_worker_for_flow(accepted_flow)
                        .expect("accepted flow owner"),
                ))
                .expect("send accept result");
            }
        })
        .expect("spawn accept worker");

    let result = rx
        .recv_timeout(Duration::from_secs(1))
        .expect("receive accept result");

    assert_eq!(result.0.user_data(), AppUserData::new(23));
    assert_eq!(result.0.object(), AppObjectRef::Socket(listener));
    match result.0.payload() {
        AppCqeData::Accepted {
            listener: cqe_listener,
            flow,
        } => {
            assert_eq!(cqe_listener, listener);
            assert_eq!(flow, accepted_flow);
        }
        other => panic!("unexpected accept payload: {other:?}"),
    }
    assert_eq!(result.1, 0);

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn app_context_try_complete_accept_enqueues_listener_owned_cqe_from_non_worker_thread() {
    let data_runtime = DataRuntime::new(1, "app-complete-accept-cross-thread-test", 512 * 1024, 2)
        .expect("data runtime");
    let app = AppContext::with_ring_capacity(data_runtime.context(), 2);
    app.install_control(AppControl::new(Arc::new(MockControlBackend::default())))
        .expect("install control");
    let listener = app
        .bind_tcp_listener("127.0.0.1:7001".parse().expect("tcp bind"), 0)
        .expect("bind tcp listener");
    let accepted_flow = AppFlowId::new(0x7002);

    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let (result_tx, result_rx) = std::sync::mpsc::channel();
    data_runtime
        .context()
        .spawn_local_on_worker(0, {
            let app = app.clone();
            move || async move {
                let backend = app
                    .local_backend_for_socket(listener)
                    .expect("listener backend");
                backend
                    .try_push_sqe_descriptor(AppSqeDescriptor::new(
                        AppOpcode::Accept,
                        AppUserData::new(24),
                        AppObjectRef::Socket(listener),
                        AppSqeData::Accept,
                    ))
                    .expect("push accept sqe");
                ready_tx.send(()).expect("signal ready");
                result_tx
                    .send(
                        backend
                            .next_cqe_descriptor()
                            .await
                            .expect("accept cqe descriptor"),
                    )
                    .expect("send accept cqe");
            }
        })
        .expect("spawn accept worker");

    ready_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("wait pending accept");
    app.try_complete_accept(listener, accepted_flow)
        .expect("enqueue accept cqe from non-worker thread");

    let result = result_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("receive accept result");

    assert_eq!(result.user_data(), AppUserData::new(24));
    assert_eq!(result.object(), AppObjectRef::Socket(listener));
    match result.payload() {
        AppCqeData::Accepted {
            listener: cqe_listener,
            flow,
        } => {
            assert_eq!(cqe_listener, listener);
            assert_eq!(flow, accepted_flow);
        }
        other => panic!("unexpected accept payload: {other:?}"),
    }
    assert_eq!(
        app.owner_worker_for_flow(accepted_flow)
            .expect("accepted flow owner"),
        0
    );

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn app_backend_and_runtime_share_one_ring_handle_for_sq_and_cq() {
    let data_runtime =
        DataRuntime::new(1, "app-shared-ring-handle-test", 512 * 1024, 2).expect("data runtime");
    let app = AppContext::with_ring_capacity(data_runtime.context(), 2);
    let flow = AppFlowId::new(81);

    let result = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            let app_for_worker = app.clone();
            app.spawn_on_flow(flow, move |worker| async move {
                let backend = worker.backend();
                let ring = backend.ring_handle();
                let app_runtime = worker.runtime();
                let recv_future = app_runtime.recv();
                let recv_sqe = ring
                    .next_submission_descriptor()
                    .await
                    .expect("recv sqe descriptor");
                let runtime = with_data_plane_buffers(Clone::clone);
                let index = runtime
                    .alloc_index_with_bytes(Default::default(), b"shared-ring")
                    .expect("alloc shared ring buffer");

                app_for_worker
                    .try_complete_recv_buffer(flow, runtime.clone(), index, false)
                    .expect("enqueue recv cqe");

                assert_eq!(recv_sqe.opcode(), AppOpcode::Recv);
                let recv = recv_future.await.expect("recv cqe");
                let recv_payload = recv.lease().copy_current().expect("recv payload");
                app_runtime
                    .send(recv.into_send())
                    .await
                    .expect("enqueue send sqe");

                let sqe = ring.next_submission().await.expect("ring submission");
                let descriptor = sqe.descriptor().expect("sqe descriptor");

                (recv_payload, descriptor)
            })
            .await
            .expect("spawn flow task")
        });

    assert_eq!(result.0, b"shared-ring");
    assert_eq!(result.1.opcode(), AppOpcode::Send);
    assert_eq!(result.1.object(), AppObjectRef::Flow(flow));
    match result.1.payload() {
        AppSqeData::Send { buffer } => assert_ne!(buffer, buffer_index(0, 0, 0)),
        other => panic!("unexpected sqe data: {other:?}"),
    }

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn app_ring_handle_round_trips_pure_descriptors_without_high_level_objects() {
    let ring = AppRingHandle::new(4, 4);
    let send_descriptor = AppSqeDescriptor::new(
        AppOpcode::Send,
        AppUserData::new(41),
        AppObjectRef::Flow(AppFlowId::new(91)),
        AppSqeData::Send {
            buffer: buffer_index(7, 9, 11),
        },
    );
    let recv_descriptor = AppCqeDescriptor::new(
        AppUserData::new(42),
        13,
        AppCqeFlags::BUFFER,
        AppObjectRef::Flow(AppFlowId::new(91)),
        AppCqeData::Recv {
            flow: AppFlowId::new(91),
            buffer: buffer_index(7, 9, 11),
        },
    );

    ring.try_push_submission_descriptor(send_descriptor)
        .expect("push sqe descriptor");
    ring.try_push_completion_descriptor(recv_descriptor)
        .expect("push cqe descriptor");

    let round_trip = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            (
                ring.next_submission_descriptor()
                    .await
                    .expect("next sqe descriptor"),
                ring.next_completion_descriptor()
                    .await
                    .expect("next cqe descriptor"),
            )
        });

    assert_eq!(round_trip.0, send_descriptor);
    assert_eq!(round_trip.1, recv_descriptor);
}

#[test]
fn app_ring_descriptor_and_object_apis_share_one_underlying_queue() {
    let ring = AppRingHandle::new(4, 4);
    let send_descriptor = AppSqeDescriptor::new(
        AppOpcode::Close,
        AppUserData::new(51),
        AppObjectRef::Flow(AppFlowId::new(151)),
        AppSqeData::Close,
    );
    let recv_descriptor = AppCqeDescriptor::new(
        AppUserData::new(52),
        0,
        AppCqeFlags::NONE,
        AppObjectRef::Flow(AppFlowId::new(151)),
        AppCqeData::Closed {
            flow: Some(AppFlowId::new(151)),
            socket: None,
        },
    );

    ring.try_push_submission_descriptor(send_descriptor)
        .expect("push submission descriptor");
    ring.try_push_completion_descriptor(recv_descriptor)
        .expect("push completion descriptor");

    let round_trip = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            (
                ring.next_submission()
                    .await
                    .expect("next submission object"),
                ring.next_completion()
                    .await
                    .expect("next completion object"),
            )
        });

    let submission_descriptor = round_trip
        .0
        .descriptor()
        .expect("submission descriptor from object");
    let completion_descriptor = round_trip
        .1
        .descriptor()
        .expect("completion descriptor option from object")
        .expect("completion descriptor from object");

    assert_eq!(submission_descriptor, send_descriptor);
    assert_eq!(completion_descriptor, recv_descriptor);
}

#[test]
fn app_ring_entry_registers_recv_buffer_for_object_view() {
    let data_runtime =
        DataRuntime::new(1, "app-ring-entry-recv-test", 512 * 1024, 2).expect("data runtime");
    let ring = AppRingHandle::new(4, 4);
    let flow = AppFlowId::new(161);

    let result = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            let runtime = with_data_plane_buffers(Clone::clone);
            let index = runtime
                .alloc_index_with_bytes(Default::default(), b"entry-registered-recv")
                .expect("alloc recv buffer");
            let lease = AppBufferLease::from_buffer(runtime, index);
            let registered = AppRegisteredBuffer::from_lease(lease).expect("registered buffer");
            let descriptor = AppCqeDescriptor::new(
                AppUserData::new(64),
                "entry-registered-recv".len() as i32,
                AppCqeFlags::BUFFER,
                AppObjectRef::Flow(flow),
                AppCqeData::Recv {
                    flow,
                    buffer: registered.index(),
                },
            );

            ring.try_push_completion_entry(AppCompletionEntry::with_attachment(
                descriptor, registered,
            ))
            .expect("push registered completion entry");

            let cqe = ring.next_completion().await.expect("next completion");
            let descriptor_round_trip = cqe
                .descriptor()
                .expect("cqe descriptor")
                .expect("recv descriptor");
            let recv = cqe.into_recv().expect("recv cqe");
            let payload = recv.lease().copy_current().expect("recv payload");
            recv.release();

            (descriptor_round_trip, payload)
        });

    assert_eq!(result.0.user_data(), AppUserData::new(64));
    assert_eq!(result.0.object(), AppObjectRef::Flow(flow));
    assert_eq!(result.1, b"entry-registered-recv");

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn app_ring_entry_registers_send_buffer_for_object_view() {
    let data_runtime =
        DataRuntime::new(1, "app-ring-entry-send-test", 512 * 1024, 2).expect("data runtime");
    let ring = AppRingHandle::new(4, 4);
    let flow = AppFlowId::new(171);

    let result = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            let runtime = with_data_plane_buffers(Clone::clone);
            let index = runtime
                .alloc_index_with_bytes(Default::default(), b"entry-registered-send")
                .expect("alloc send buffer");
            let lease = AppBufferLease::from_buffer(runtime, index);
            let registered = AppRegisteredBuffer::from_lease(lease).expect("registered buffer");
            let descriptor = AppSqeDescriptor::new(
                AppOpcode::Send,
                AppUserData::new(65),
                AppObjectRef::Flow(flow),
                AppSqeData::Send {
                    buffer: registered.index(),
                },
            );

            ring.try_push_submission_entry(AppSubmissionEntry::with_attachment(
                descriptor, registered,
            ))
            .expect("push registered submission entry");

            let sqe = ring.next_submission().await.expect("next submission");
            let descriptor_round_trip = sqe.descriptor().expect("sqe descriptor");
            let send = sqe.into_send().expect("send sqe");
            let payload = send.lease().copy_current().expect("send payload");
            send.release();

            (descriptor_round_trip, payload)
        });

    assert_eq!(result.0.user_data(), AppUserData::new(65));
    assert_eq!(result.0.object(), AppObjectRef::Flow(flow));
    assert_eq!(result.1, b"entry-registered-send");

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn app_recv_cqe_descriptor_uses_result_flags_and_buffer_handle() {
    let data_runtime =
        DataRuntime::new(1, "app-cqe-descriptor-test", 512 * 1024, 2).expect("data runtime");
    let app = AppContext::with_ring_capacity(data_runtime.context(), 2);

    let descriptor = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            app.spawn_on_flow(AppFlowId::new(41), move |_worker| async move {
                let runtime = with_data_plane_buffers(Clone::clone);
                let index = runtime
                    .alloc_index_with_bytes(Default::default(), b"descriptor-cqe")
                    .expect("alloc app recv buffer");
                let recv =
                    hammer_runtime::app::AppRecv::new(AppBufferLease::from_buffer(runtime, index));
                AppCqe::recv(AppUserData::new(77), AppFlowId::new(41), recv, true)
                    .descriptor()
                    .expect("descriptor conversion")
                    .expect("recv descriptor")
            })
            .await
            .expect("spawn flow task")
        });

    assert_eq!(descriptor.user_data(), AppUserData::new(77));
    assert_eq!(descriptor.result(), "descriptor-cqe".len() as i32);
    assert_eq!(descriptor.object(), AppObjectRef::Flow(AppFlowId::new(41)));
    assert!(descriptor.flags().contains(AppCqeFlags::BUFFER));
    assert!(descriptor.flags().contains(AppCqeFlags::FIN));
    match descriptor.payload() {
        AppCqeData::Recv { flow, buffer } => {
            assert_eq!(flow, AppFlowId::new(41));
            assert_ne!(buffer, buffer_index(0, 0, 0));
        }
        other => panic!("unexpected cqe data: {other:?}"),
    }

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn app_ring_zero_copy_recv_and_stable_flow_owner() {
    let data_runtime = DataRuntime::new(2, "app-ring-test", 512 * 1024, 2).expect("data runtime");
    let app = AppContext::with_ring_capacity(data_runtime.context(), 4);
    let flow = AppFlowId::new(7);

    let first = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            app.spawn_on_flow(flow, move |worker| async move {
                let owner = worker.owner_worker();
                let thread = std::thread::current()
                    .name()
                    .map(ToOwned::to_owned)
                    .unwrap_or_default();
                let backend = worker.backend();
                let app_runtime = worker.runtime();
                let recv_future = app_runtime.recv();
                let recv_sqe = backend
                    .next_sqe_descriptor()
                    .await
                    .expect("recv sqe descriptor");
                let runtime = with_data_plane_buffers(Clone::clone);
                let before_in_use = with_data_plane_buffers(|runtime| runtime.in_use_buffers());
                let index = runtime
                    .alloc_index_with_bytes(Default::default(), b"ring-zero-copy")
                    .expect("alloc app recv buffer");
                let expected_ptr = runtime.current_ptr(index).expect("buffer pointer");

                backend
                    .complete_recv(AppBufferLease::from_buffer(runtime.clone(), index))
                    .await
                    .expect("complete recv cqe");

                assert_eq!(recv_sqe.opcode(), AppOpcode::Recv);
                let recv = recv_future.await.expect("recv cqe");
                let recv_ptr = recv.lease().current_ptr().expect("recv lease pointer");
                let recv_payload = recv.lease().copy_current().expect("recv payload");

                app_runtime.send(recv.into_send()).await.expect("send sqe");

                let send = backend.next_send().await.expect("send sqe");
                let send_ptr = send.lease().current_ptr().expect("send lease pointer");
                let send_payload = send.lease().copy_current().expect("send payload");
                drop(send);
                let after_in_use = with_data_plane_buffers(|runtime| runtime.in_use_buffers());

                (
                    owner,
                    thread,
                    expected_ptr as usize,
                    recv_ptr as usize,
                    send_ptr as usize,
                    recv_payload,
                    send_payload,
                    before_in_use,
                    after_in_use,
                )
            })
            .await
            .expect("spawn flow task")
        });

    let second = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            app.spawn_on_flow(flow, move |worker| async move {
                (
                    worker.owner_worker(),
                    std::thread::current()
                        .name()
                        .map(ToOwned::to_owned)
                        .unwrap_or_default(),
                )
            })
            .await
            .expect("spawn flow task")
        });

    assert_eq!(first.0, second.0);
    assert_eq!(first.1, second.1);
    assert_eq!(first.2, first.3);
    assert_eq!(first.3, first.4);
    assert_eq!(first.5, b"ring-zero-copy");
    assert_eq!(first.6, b"ring-zero-copy");
    assert_eq!(first.7, 0);
    assert_eq!(first.8, 0);

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn app_recv_drop_releases_buffer_lease() {
    let data_runtime =
        DataRuntime::new(1, "app-ring-drop-test", 512 * 1024, 2).expect("data runtime");
    let app = AppContext::with_ring_capacity(data_runtime.context(), 2);

    let result = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            app.spawn_on_flow(AppFlowId::new(19), move |worker| async move {
                let backend = worker.backend();
                let app_runtime = worker.runtime();
                let recv_future = app_runtime.recv();
                let recv_sqe = backend
                    .next_sqe_descriptor()
                    .await
                    .expect("recv sqe descriptor");
                let runtime = with_data_plane_buffers(Clone::clone);
                let before_in_use = with_data_plane_buffers(|runtime| runtime.in_use_buffers());
                let index = runtime
                    .alloc_index_with_bytes(Default::default(), b"drop-recv")
                    .expect("alloc app recv buffer");

                backend
                    .complete_recv(AppBufferLease::from_buffer(runtime.clone(), index))
                    .await
                    .expect("complete recv cqe");

                assert_eq!(recv_sqe.opcode(), AppOpcode::Recv);
                let recv = recv_future.await.expect("recv cqe");
                let payload = recv.lease().copy_current().expect("recv payload");
                drop(recv);
                let after_in_use = with_data_plane_buffers(|runtime| runtime.in_use_buffers());

                (payload, before_in_use, after_in_use)
            })
            .await
            .expect("spawn flow task")
        });

    assert_eq!(result.0, b"drop-recv");
    assert_eq!(result.1, 0);
    assert_eq!(result.2, 0);

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn same_flow_reuses_backend_across_spawn_calls() {
    let data_runtime =
        DataRuntime::new(1, "app-ring-backend-test", 512 * 1024, 2).expect("data runtime");
    let app = AppContext::with_ring_capacity(data_runtime.context(), 2);
    let flow = AppFlowId::new(29);

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            app.spawn_on_flow(flow, move |worker| async move {
                let backend = worker.backend();
                let app_runtime = worker.runtime();
                let recv_future = app_runtime.recv();
                let recv_sqe = backend
                    .next_sqe_descriptor()
                    .await
                    .expect("recv sqe descriptor");
                let runtime = with_data_plane_buffers(Clone::clone);
                let index = runtime
                    .alloc_index_with_bytes(Default::default(), b"persisted-backend")
                    .expect("alloc app recv buffer");

                backend
                    .complete_recv(AppBufferLease::from_buffer(runtime.clone(), index))
                    .await
                    .expect("complete recv cqe");
                assert_eq!(recv_sqe.opcode(), AppOpcode::Recv);
                recv_future.await.expect("recv cqe").release();
            })
            .await
            .expect("prime flow backend");

            app.spawn_on_flow(flow, move |worker| async move {
                let app_runtime = worker.runtime();
                let recv_future = app_runtime.recv();
                let recv_sqe = worker
                    .backend()
                    .next_sqe_descriptor()
                    .await
                    .expect("recv sqe descriptor");
                assert_eq!(recv_sqe.opcode(), AppOpcode::Recv);
                let mut recv_future = std::pin::pin!(recv_future);
                let still_pending =
                    std::future::poll_fn(|cx| match recv_future.as_mut().poll(cx) {
                        std::task::Poll::Pending => std::task::Poll::Ready(true),
                        std::task::Poll::Ready(_) => std::task::Poll::Ready(false),
                    })
                    .await;
                assert!(still_pending, "recv should wait for a future completion");
            })
            .await
            .expect("reuse flow backend")
        });

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn app_backend_round_trips_send_descriptor_without_app_send_wrapper() {
    let data_runtime = DataRuntime::new(1, "app-backend-send-descriptor-test", 512 * 1024, 2)
        .expect("data runtime");
    let app = AppContext::with_ring_capacity(data_runtime.context(), 2);
    let flow = AppFlowId::new(93);
    let descriptor = AppSqeDescriptor::new(
        AppOpcode::Send,
        AppUserData::new(51),
        AppObjectRef::Flow(flow),
        AppSqeData::Send {
            buffer: buffer_index(17, 19, 23),
        },
    );

    let round_trip = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            app.spawn_on_flow(flow, move |worker| async move {
                let backend = worker.backend();
                backend
                    .try_push_sqe_descriptor(descriptor)
                    .expect("push sqe descriptor");
                backend
                    .next_sqe_descriptor()
                    .await
                    .expect("next sqe descriptor")
            })
            .await
            .expect("spawn flow task")
        });

    assert_eq!(round_trip, descriptor);

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn app_backend_round_trips_recv_cqe_descriptor_without_app_recv_wrapper() {
    let data_runtime = DataRuntime::new(1, "app-backend-cqe-descriptor-test", 512 * 1024, 2)
        .expect("data runtime");
    let app = AppContext::with_ring_capacity(data_runtime.context(), 2);
    let flow = AppFlowId::new(95);
    let descriptor = AppCqeDescriptor::new(
        AppUserData::new(52),
        29,
        AppCqeFlags::BUFFER,
        AppObjectRef::Flow(flow),
        AppCqeData::Recv {
            flow,
            buffer: buffer_index(31, 37, 41),
        },
    );

    let round_trip = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            app.spawn_on_flow(flow, move |worker| async move {
                let backend = worker.backend();
                backend
                    .try_push_cqe_descriptor(descriptor)
                    .expect("push cqe descriptor");
                backend
                    .next_cqe_descriptor()
                    .await
                    .expect("next cqe descriptor")
            })
            .await
            .expect("spawn flow task")
        });

    assert_eq!(round_trip, descriptor);

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn app_runtime_exposes_descriptor_first_submission_and_completion_paths() {
    let data_runtime = DataRuntime::new(1, "app-runtime-descriptor-path-test", 512 * 1024, 2)
        .expect("data runtime");
    let app = AppContext::with_ring_capacity(data_runtime.context(), 2);
    let flow = AppFlowId::new(97);
    let pushed_submission = AppSqeDescriptor::new(
        AppOpcode::Close,
        AppUserData::new(61),
        AppObjectRef::Flow(flow),
        AppSqeData::Close,
    );
    let queued_submission = AppSqeDescriptor::new(
        AppOpcode::Nop,
        AppUserData::new(62),
        AppObjectRef::None,
        AppSqeData::Nop,
    );
    let queued_completion = AppCqeDescriptor::new(
        AppUserData::new(63),
        0,
        AppCqeFlags::NONE,
        AppObjectRef::Flow(flow),
        AppCqeData::Closed {
            flow: Some(flow),
            socket: None,
        },
    );

    let round_trip = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            app.spawn_on_flow(flow, move |worker| async move {
                let backend = worker.backend();
                let runtime = worker.runtime();

                runtime
                    .try_push_submission_descriptor(pushed_submission)
                    .expect("push runtime submission descriptor");
                let pushed_round_trip = backend
                    .next_sqe_descriptor()
                    .await
                    .expect("backend next sqe descriptor");

                backend
                    .try_push_sqe_descriptor(queued_submission)
                    .expect("queue backend submission descriptor");
                let queued_submission_round_trip = runtime
                    .next_submission_descriptor()
                    .await
                    .expect("runtime next submission descriptor");

                backend
                    .try_push_cqe_descriptor(queued_completion)
                    .expect("queue backend completion descriptor");
                let queued_completion_round_trip = runtime
                    .next_completion_descriptor()
                    .await
                    .expect("runtime next completion descriptor");

                (
                    pushed_round_trip,
                    queued_submission_round_trip,
                    queued_completion_round_trip,
                )
            })
            .await
            .expect("spawn flow task")
        });

    assert_eq!(round_trip.0, pushed_submission);
    assert_eq!(round_trip.1, queued_submission);
    assert_eq!(round_trip.2, queued_completion);

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn app_runtime_pushes_submission_entry_without_app_send_wrapper() {
    let data_runtime =
        DataRuntime::new(1, "app-runtime-entry-path-test", 512 * 1024, 2).expect("data runtime");
    let app = AppContext::with_ring_capacity(data_runtime.context(), 2);
    let flow = AppFlowId::new(99);

    let round_trip = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            app.spawn_on_flow(flow, move |worker| async move {
                let backend = worker.backend();
                let runtime = worker.runtime();
                let buffers = with_data_plane_buffers(Clone::clone);
                let index = buffers
                    .alloc_index_with_bytes(Default::default(), b"runtime-entry-send")
                    .expect("alloc runtime entry buffer");
                let registered =
                    AppRegisteredBuffer::from_lease(AppBufferLease::from_buffer(buffers, index))
                        .expect("registered buffer");
                let descriptor = AppSqeDescriptor::new(
                    AppOpcode::Send,
                    AppUserData::new(64),
                    AppObjectRef::Flow(flow),
                    AppSqeData::Send {
                        buffer: registered.index(),
                    },
                );

                runtime
                    .try_push_submission_entry(AppSubmissionEntry::with_attachment(
                        descriptor, registered,
                    ))
                    .expect("push runtime submission entry");

                let sqe = backend
                    .next_submission_entry()
                    .await
                    .expect("backend next submission entry");
                let (descriptor_round_trip, registered) = sqe.into_parts();
                let (_handle, lease) = registered.expect("submission attachment").into_parts();
                let payload = lease.copy_current().expect("send payload");
                lease.release();

                (descriptor_round_trip, payload)
            })
            .await
            .expect("spawn flow task")
        });

    assert_eq!(round_trip.0.user_data(), AppUserData::new(64));
    assert_eq!(round_trip.0.object(), AppObjectRef::Flow(flow));
    match round_trip.0.payload() {
        AppSqeData::Send { buffer } => assert_ne!(buffer, buffer_index(0, 0, 0)),
        other => panic!("unexpected sqe data: {other:?}"),
    }
    assert_eq!(round_trip.1, b"runtime-entry-send");

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn non_owner_worker_cannot_access_local_backend_for_foreign_flow() {
    let data_runtime =
        DataRuntime::new(2, "app-ring-owner-test", 512 * 1024, 2).expect("data runtime");
    let app = AppContext::with_ring_capacity(data_runtime.context(), 2);
    let flow = AppFlowId::new(1);
    let owner = app.owner_worker_for_flow(flow).expect("owner worker");
    let non_owner = (owner + 1) % app.worker_count();

    let result = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async move {
            let probe = app.clone();
            app.spawn_on_flow(AppFlowId::new(non_owner as u64), move |_| async move {
                (
                    probe.current_worker_owns_flow(flow),
                    probe.local_backend_for_flow(flow).map(|_| ()),
                )
            })
            .await
            .expect("non-owner worker task")
        });

    assert!(!result.0);
    let err = result
        .1
        .expect_err("non-owner worker must reject backend lookup");
    assert!(err.to_string().contains(&format!(
        "app flow {} is owned by worker {owner}",
        flow.value()
    )));

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[inline]
fn buffer_index(pool_id: u64, slot: u32, generation: u32) -> BufferIndex {
    unsafe { std::mem::transmute::<(u64, u32, u32), BufferIndex>((pool_id, slot, generation)) }
}
