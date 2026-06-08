use std::time::Duration;

use hammer_app::echo::run_udp_echo;
use hammer_app::udp::UdpSocket;
use hammer_app::{App, AppBufferLease, AppFlowId, AppOpcode};
use hammer_runtime::spawn::{DataRuntime, with_data_plane_buffers};

#[test]
fn udp_echo_helper_reuses_same_ring() {
    let data_runtime =
        DataRuntime::new(2, "hammer-app-udp-echo", 512 * 1024, 2).expect("data runtime");
    let app = App::with_ring_capacity(data_runtime.context(), 4);
    let flow = AppFlowId::new(19);

    let result = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            app.spawn(flow, move |flow| async move {
                let backend = flow.backend();
                let ring = flow.ring();
                let udp = UdpSocket::new(ring.clone(), "127.0.0.1:5353".parse().expect("peer"));
                let udp_echo = flow.spawn_local({
                    let udp = udp.clone();
                    move || async move { run_udp_echo(&udp).await.expect("udp echo once") }
                });
                let runtime = with_data_plane_buffers(Clone::clone);
                let first_recv_sqe = backend.next_sqe_descriptor().await.expect("first recv sqe");
                assert_eq!(first_recv_sqe.opcode(), AppOpcode::Recv);
                let echo_index = runtime
                    .alloc_index_with_bytes(Default::default(), b"udp-echo")
                    .expect("alloc udp buffer");
                let replay_index = runtime
                    .alloc_index_with_bytes(Default::default(), b"ring-echo")
                    .expect("alloc ring buffer");

                backend
                    .complete_recv(AppBufferLease::from_buffer(runtime.clone(), echo_index))
                    .await
                    .expect("complete udp recv");
                let peer = udp_echo.await.expect("join udp echo");
                let udp_send = backend.next_send().await.expect("udp send");
                let udp_payload = udp_send.lease().copy_current().expect("copy udp payload");

                let ring_recv_future = ring.recv();
                let second_recv_sqe = backend
                    .next_sqe_descriptor()
                    .await
                    .expect("second recv sqe");
                assert_eq!(second_recv_sqe.opcode(), AppOpcode::Recv);
                backend
                    .complete_recv(AppBufferLease::from_buffer(runtime.clone(), replay_index))
                    .await
                    .expect("complete ring recv");

                let ring_recv = ring_recv_future.await.expect("ring recv after udp");
                let udp_ptr = ring_recv.lease().current_ptr().expect("udp recv ptr") as usize;
                let expected_udp_ptr =
                    runtime.current_ptr(replay_index).expect("udp expected ptr") as usize;
                ring.send(ring_recv.into_send())
                    .await
                    .expect("ring send back");
                let replay = backend.next_send().await.expect("replay send");
                let replay_payload = replay.lease().copy_current().expect("replay payload");

                (peer, udp_payload, udp_ptr, expected_udp_ptr, replay_payload)
            })
            .await
            .expect("spawn flow")
        });

    assert_eq!(result.0, "127.0.0.1:5353".parse().expect("peer"));
    assert_eq!(result.1, b"udp-echo");
    assert_eq!(result.2, result.3);
    assert_eq!(result.4, b"ring-echo");

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}
