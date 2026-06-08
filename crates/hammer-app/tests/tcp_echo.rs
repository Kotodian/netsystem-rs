use std::time::Duration;

use hammer_app::echo::{echo_once, run_tcp_echo};
use hammer_app::tcp::TcpStream;
use hammer_app::{App, AppBufferLease, AppFlowId, AppOpcode};
use hammer_runtime::spawn::{DataRuntime, with_data_plane_buffers};

#[test]
fn tcp_echo_helpers_round_trip_without_copying() {
    let data_runtime =
        DataRuntime::new(2, "hammer-app-tcp-echo", 512 * 1024, 2).expect("data runtime");
    let app = App::with_ring_capacity(data_runtime.context(), 4);
    let flow = AppFlowId::new(11);

    let result = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            app.spawn(flow, move |flow| async move {
                let backend = flow.backend();
                let stream = TcpStream::new(flow.ring());
                let echo = flow.spawn_local({
                    let stream = stream.clone();
                    move || async move {
                        echo_once(&stream).await.expect("echo once");
                    }
                });
                let runtime = with_data_plane_buffers(Clone::clone);
                let recv_sqe = backend.next_sqe_descriptor().await.expect("next recv sqe");
                assert_eq!(recv_sqe.opcode(), AppOpcode::Recv);
                let index = runtime
                    .alloc_index_with_bytes(Default::default(), b"tcp-echo")
                    .expect("alloc tcp buffer");
                let expected_ptr = runtime.current_ptr(index).expect("buffer pointer");

                backend
                    .complete_recv(AppBufferLease::from_buffer(runtime.clone(), index))
                    .await
                    .expect("complete recv");
                echo.await.expect("join echo");

                let send = backend.next_send().await.expect("next send");
                let send_ptr = send.lease().current_ptr().expect("send pointer");
                let send_payload = send.lease().copy_current().expect("send payload");

                (expected_ptr as usize, send_ptr as usize, send_payload)
            })
            .await
            .expect("spawn flow")
        });

    assert_eq!(result.0, result.1);
    assert_eq!(result.2, b"tcp-echo");

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn tcp_echo_loop_uses_local_executor() {
    let data_runtime =
        DataRuntime::new(2, "hammer-app-echo-loop", 512 * 1024, 2).expect("data runtime");
    let app = App::with_ring_capacity(data_runtime.context(), 4);
    let flow = AppFlowId::new(17);

    let result = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            app.spawn(flow, move |flow| async move {
                let owner_thread = std::thread::current()
                    .name()
                    .map(ToOwned::to_owned)
                    .unwrap_or_default();
                let backend = flow.backend();
                let stream = TcpStream::new(flow.ring());
                let echo = flow.spawn_local({
                    let stream = stream.clone();
                    move || async move {
                        run_tcp_echo(&stream, 1).await.expect("run tcp echo");
                        std::thread::current()
                            .name()
                            .map(ToOwned::to_owned)
                            .unwrap_or_default()
                    }
                });
                let runtime = with_data_plane_buffers(Clone::clone);
                let recv_sqe = backend.next_sqe_descriptor().await.expect("next recv sqe");
                assert_eq!(recv_sqe.opcode(), AppOpcode::Recv);
                let first = runtime
                    .alloc_index_with_bytes(Default::default(), b"loop-echo")
                    .expect("alloc loop buffer");

                backend
                    .complete_recv(AppBufferLease::from_buffer(runtime, first))
                    .await
                    .expect("complete loop recv");

                let loop_send = backend.next_send().await.expect("loop send");
                let loop_payload = loop_send.lease().copy_current().expect("copy loop payload");
                let echo_thread = echo.await.expect("join local echo");

                (owner_thread, echo_thread, loop_payload)
            })
            .await
            .expect("spawn flow")
        });

    assert_eq!(result.0, result.1);
    assert_eq!(result.2, b"loop-echo");

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}
