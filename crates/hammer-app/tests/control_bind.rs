use std::future::{Future, poll_fn};
use std::net::SocketAddr;
use std::pin::pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::Duration;

use hammer_app::tcp::TcpListener;
use hammer_app::udp::UdpSocket;
use hammer_app::{
    App, AppBufferLease, AppCompletionEntry, AppControl, AppControlBackend, AppCqeData,
    AppCqeDescriptor, AppCqeFlags, AppFlowId, AppObjectRef, AppOpcode, AppRegisteredBuffer,
    AppSqeData, AppUserData,
};
use hammer_core::error::HammerResult;
use hammer_runtime::spawn::{DataRuntime, with_data_plane_buffers};

#[derive(Default)]
struct MockControlBackend {
    next_socket: AtomicU64,
    tcp_binds: Mutex<Vec<(SocketAddr, usize, u64)>>,
    udp_binds: Mutex<Vec<(SocketAddr, usize, u64)>>,
    closed: Mutex<Vec<u64>>,
}

impl MockControlBackend {
    fn new() -> Self {
        Self {
            next_socket: AtomicU64::new(0x9000),
            ..Self::default()
        }
    }

    fn alloc_socket(&self) -> u64 {
        self.next_socket.fetch_add(1, Ordering::Relaxed)
    }
}

impl AppControlBackend for MockControlBackend {
    fn bind_tcp_listener(
        &self,
        _app: &hammer_runtime::app::AppContext,
        bind: SocketAddr,
        owner_worker: usize,
    ) -> HammerResult<hammer_runtime::app::AppSocketId> {
        let socket = self.alloc_socket();
        self.tcp_binds
            .lock()
            .expect("tcp binds poisoned")
            .push((bind, owner_worker, socket));
        Ok(hammer_runtime::app::AppSocketId::new(socket))
    }

    fn bind_udp_socket(
        &self,
        _app: &hammer_runtime::app::AppContext,
        bind: SocketAddr,
        owner_worker: usize,
    ) -> HammerResult<hammer_runtime::app::AppSocketId> {
        let socket = self.alloc_socket();
        self.udp_binds
            .lock()
            .expect("udp binds poisoned")
            .push((bind, owner_worker, socket));
        Ok(hammer_runtime::app::AppSocketId::new(socket))
    }

    fn close_socket(
        &self,
        _app: &hammer_runtime::app::AppContext,
        socket: hammer_runtime::app::AppSocketId,
    ) -> HammerResult<()> {
        self.closed
            .lock()
            .expect("closed sockets poisoned")
            .push(socket.value());
        Ok(())
    }

    fn close_tcp_flow(
        &self,
        _app: &hammer_runtime::app::AppContext,
        _flow: hammer_runtime::app::AppFlowId,
    ) -> HammerResult<()> {
        Ok(())
    }
}

#[test]
fn tcp_listener_bind_accept_and_stream_io_stay_on_owner_worker() {
    let data_runtime =
        DataRuntime::new(1, "hammer-app-tcp-control", 512 * 1024, 2).expect("data runtime");
    let app = App::with_ring_capacity(data_runtime.context(), 4);
    let control_backend = Arc::new(MockControlBackend::new());
    let control: Arc<dyn AppControlBackend> = control_backend.clone();
    app.install_control(AppControl::new(control))
        .expect("install control");

    let bind: SocketAddr = "127.0.0.1:7000".parse().expect("tcp bind");
    let listener = TcpListener::bind(&app, bind, 0).expect("bind listener");
    assert_eq!(
        app.context()
            .owner_worker_for_socket(listener.listener())
            .expect("listener owner"),
        0
    );

    let (tx, rx) = std::sync::mpsc::channel();
    data_runtime
        .context()
        .spawn_local_on_worker(0, {
            let app = app.clone();
            let listener = listener.clone();
            move || async move {
                let mut accept_future = pin!(listener.accept());
                poll_fn(|cx| match accept_future.as_mut().poll(cx) {
                    Poll::Pending => Poll::Ready(()),
                    Poll::Ready(_) => panic!("accept completed before accepted cqe"),
                })
                .await;

                let listener_backend = app
                    .context()
                    .local_backend_for_socket(listener.listener())
                    .expect("listener backend");
                let accept_sqe = listener_backend
                    .next_sqe_descriptor()
                    .await
                    .expect("accept sqe descriptor");
                assert_eq!(accept_sqe.opcode(), AppOpcode::Accept);
                assert_eq!(
                    accept_sqe.object(),
                    AppObjectRef::Socket(listener.listener())
                );
                assert_eq!(accept_sqe.payload(), AppSqeData::Accept);

                let accepted_flow = AppFlowId::new(0x1234);
                listener_backend
                    .try_push_cqe_descriptor(AppCqeDescriptor::new(
                        AppUserData::new(0),
                        0,
                        AppCqeFlags::NONE,
                        AppObjectRef::Socket(listener.listener()),
                        AppCqeData::Accepted {
                            listener: listener.listener(),
                            flow: accepted_flow,
                        },
                    ))
                    .expect("push accept cqe");
                let stream = accept_future.await.expect("accept stream");

                let mut recv_future = pin!(stream.recv_buffer());
                poll_fn(|cx| match recv_future.as_mut().poll(cx) {
                    Poll::Pending => Poll::Ready(()),
                    Poll::Ready(_) => panic!("recv completed before recv cqe"),
                })
                .await;

                let flow_backend = app
                    .context()
                    .local_backend_for_flow(accepted_flow)
                    .expect("accepted flow backend");
                let recv_sqe = flow_backend
                    .next_sqe_descriptor()
                    .await
                    .expect("recv sqe descriptor");
                assert_eq!(recv_sqe.opcode(), AppOpcode::Recv);
                assert_eq!(recv_sqe.object(), AppObjectRef::Flow(accepted_flow));

                let runtime = with_data_plane_buffers(Clone::clone);
                let index = runtime
                    .alloc_index_with_bytes(Default::default(), b"tcp-control")
                    .expect("alloc tcp buffer");
                let expected_ptr = runtime.current_ptr(index).expect("tcp ptr") as usize;
                let registered =
                    AppRegisteredBuffer::from_lease(AppBufferLease::from_buffer(runtime, index))
                        .expect("register tcp buffer");
                flow_backend
                    .try_push_completion_entry(AppCompletionEntry::with_attachment(
                        AppCqeDescriptor::new(
                            AppUserData::new(0),
                            b"tcp-control".len() as i32,
                            AppCqeFlags::BUFFER,
                            AppObjectRef::Flow(accepted_flow),
                            AppCqeData::Recv {
                                flow: accepted_flow,
                                buffer: registered.index(),
                            },
                        ),
                        registered,
                    ))
                    .expect("push recv cqe");

                let lease = recv_future.await.expect("recv lease");
                let recv_ptr = lease.current_ptr().expect("recv ptr") as usize;
                let recv_payload = lease.copy_current().expect("recv payload");
                stream.send_buffer(lease).await.expect("send buffer");

                let send_entry = flow_backend
                    .next_submission_entry()
                    .await
                    .expect("send entry");
                let send_attachment = send_entry.attachment().expect("send attachment");
                let send_ptr = send_attachment.lease().current_ptr().expect("send ptr") as usize;
                let send_payload = send_attachment
                    .lease()
                    .copy_current()
                    .expect("send payload");

                tx.send((
                    expected_ptr,
                    recv_ptr,
                    recv_payload,
                    send_entry.descriptor(),
                    send_ptr,
                    send_payload,
                ))
                .expect("send tcp result");
            }
        })
        .expect("spawn tcp control task");

    let result = rx
        .recv_timeout(Duration::from_secs(1))
        .expect("receive tcp result");
    assert_eq!(result.0, result.1);
    assert_eq!(result.2, b"tcp-control");
    assert_eq!(result.3.opcode(), AppOpcode::Send);
    assert_eq!(result.4, result.1);
    assert_eq!(result.5, b"tcp-control");

    let tcp_binds = control_backend
        .tcp_binds
        .lock()
        .expect("tcp binds poisoned");
    assert_eq!(
        tcp_binds.as_slice(),
        &[(bind, 0, listener.listener().value())]
    );

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn udp_socket_bind_recv_from_send_to_and_close_use_control_owned_socket_backend() {
    let data_runtime =
        DataRuntime::new(1, "hammer-app-udp-control", 512 * 1024, 2).expect("data runtime");
    let app = App::with_ring_capacity(data_runtime.context(), 4);
    let control_backend = Arc::new(MockControlBackend::new());
    let control: Arc<dyn AppControlBackend> = control_backend.clone();
    app.install_control(AppControl::new(control))
        .expect("install control");

    let bind: SocketAddr = "127.0.0.1:9000".parse().expect("udp bind");
    let peer: SocketAddr = "127.0.0.1:5353".parse().expect("udp peer");

    let (tx, rx) = std::sync::mpsc::channel();
    data_runtime
        .context()
        .spawn_local_on_worker(0, {
            let app = app.clone();
            move || async move {
                let socket = UdpSocket::bind(&app, bind, 0).expect("bind udp socket");
                let backend = app
                    .context()
                    .local_backend_for_socket(socket.socket())
                    .expect("udp backend");
                let (
                    owner_worker,
                    expected_ptr,
                    recv_ptr,
                    recv_peer,
                    recv_payload,
                    send_descriptor,
                    send_ptr,
                    send_payload,
                ) = {
                    let mut recv_future = pin!(socket.recv_from_buffer());
                    poll_fn(|cx| match recv_future.as_mut().poll(cx) {
                        Poll::Pending => Poll::Ready(()),
                        Poll::Ready(_) => panic!("recv_from completed before recv cqe"),
                    })
                    .await;

                    let recv_sqe = backend
                        .next_sqe_descriptor()
                        .await
                        .expect("udp recv_from sqe");
                    assert_eq!(recv_sqe.opcode(), AppOpcode::RecvFrom);
                    assert_eq!(recv_sqe.object(), AppObjectRef::Socket(socket.socket()));
                    assert_eq!(
                        recv_sqe.payload(),
                        AppSqeData::RecvFrom { max_len: u32::MAX }
                    );

                    let runtime = with_data_plane_buffers(Clone::clone);
                    let index = runtime
                        .alloc_index_with_bytes(Default::default(), b"udp-control")
                        .expect("alloc udp buffer");
                    let expected_ptr = runtime.current_ptr(index).expect("udp ptr") as usize;
                    let registered = AppRegisteredBuffer::from_lease(AppBufferLease::from_buffer(
                        runtime, index,
                    ))
                    .expect("register udp buffer");
                    backend
                        .try_push_completion_entry(AppCompletionEntry::with_attachment(
                            AppCqeDescriptor::new(
                                AppUserData::new(0),
                                b"udp-control".len() as i32,
                                AppCqeFlags::BUFFER,
                                AppObjectRef::Socket(socket.socket()),
                                AppCqeData::RecvFrom {
                                    socket: socket.socket(),
                                    source: peer,
                                    buffer: registered.index(),
                                },
                            ),
                            registered,
                        ))
                        .expect("push udp recv_from cqe");

                    let (lease, recv_peer) = recv_future.as_mut().await.expect("recv_from lease");
                    let recv_ptr = lease.current_ptr().expect("recv ptr") as usize;
                    let recv_payload = lease.copy_current().expect("recv payload");
                    socket
                        .send_buffer_to(lease, recv_peer)
                        .await
                        .expect("send udp buffer");

                    let send_entry = backend
                        .next_submission_entry()
                        .await
                        .expect("udp send_to entry");
                    let send_attachment = send_entry.attachment().expect("udp send attachment");
                    let send_ptr =
                        send_attachment.lease().current_ptr().expect("send ptr") as usize;
                    let send_payload = send_attachment
                        .lease()
                        .copy_current()
                        .expect("send payload");
                    let owner_worker = app
                        .context()
                        .owner_worker_for_socket(socket.socket())
                        .expect("udp owner before close");

                    (
                        owner_worker,
                        expected_ptr,
                        recv_ptr,
                        recv_peer,
                        recv_payload,
                        send_entry.descriptor(),
                        send_ptr,
                        send_payload,
                    )
                };

                tx.send((
                    socket.socket(),
                    owner_worker,
                    expected_ptr,
                    recv_ptr,
                    recv_peer,
                    recv_payload,
                    send_descriptor,
                    send_ptr,
                    send_payload,
                ))
                .expect("send udp result");

                socket.close().await.expect("close udp socket");
            }
        })
        .expect("spawn udp control task");

    let result = rx
        .recv_timeout(Duration::from_secs(1))
        .expect("receive udp result");
    assert_eq!(result.1, 0);
    assert!(
        app.context().owner_worker_for_socket(result.0).is_err(),
        "closed udp socket handle must be invalid"
    );
    assert_eq!(result.2, result.3);
    assert_eq!(result.4, peer);
    assert_eq!(result.5, b"udp-control");
    assert_eq!(result.6.opcode(), AppOpcode::SendTo);
    assert_eq!(result.6.object(), AppObjectRef::Socket(result.0));
    assert_eq!(result.7, result.3);
    assert_eq!(result.8, b"udp-control");

    match result.6.payload() {
        AppSqeData::SendTo { target, .. } => assert_eq!(target, peer),
        other => panic!("expected send_to payload, got {other:?}"),
    }

    let udp_binds = control_backend
        .udp_binds
        .lock()
        .expect("udp binds poisoned");
    assert_eq!(udp_binds.as_slice(), &[(bind, 0, result.0.value())]);
    let closed = control_backend
        .closed
        .lock()
        .expect("closed sockets poisoned");
    assert_eq!(closed.as_slice(), &[result.0.value()]);

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}
