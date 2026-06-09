use std::time::Duration;

use hammer_app::echo::run_udp_echo;
use hammer_app::udp::UdpSocket;
use hammer_app::{
    App, AppBufferLease, AppCompletionEntry, AppCqeData, AppCqeDescriptor, AppCqeFlags,
    AppObjectRef, AppOpcode, AppRegisteredBuffer, AppSocketId, AppSqeData, AppUserData,
};
use hammer_runtime::spawn::{DataRuntime, with_data_plane_buffers};

#[test]
fn udp_echo_helper_reuses_same_zero_copy_buffer_via_recv_from_send_to() {
    let data_runtime =
        DataRuntime::new(2, "hammer-app-udp-echo", 512 * 1024, 2).expect("data runtime");
    let app = App::with_ring_capacity(data_runtime.context(), 4);
    let flow = hammer_app::AppFlowId::new(19);
    let socket = AppSocketId::new(53);
    let peer = "127.0.0.1:5353".parse().expect("peer");

    let result = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            app.spawn(flow, move |flow| async move {
                let backend = flow.backend();
                let udp = UdpSocket::new(flow.ring(), socket);
                let udp_echo = flow.spawn_local({
                    let udp = udp.clone();
                    move || async move { run_udp_echo(&udp).await.expect("udp echo once") }
                });
                let runtime = with_data_plane_buffers(Clone::clone);
                let recv_sqe = backend
                    .next_sqe_descriptor()
                    .await
                    .expect("udp recv_from sqe");
                assert_eq!(recv_sqe.opcode(), AppOpcode::RecvFrom);
                assert_eq!(recv_sqe.object(), AppObjectRef::Socket(socket));
                assert_eq!(
                    recv_sqe.payload(),
                    AppSqeData::RecvFrom { max_len: u32::MAX }
                );

                let echo_index = runtime
                    .alloc_index_with_bytes(Default::default(), b"udp-echo")
                    .expect("alloc udp buffer");
                let expected_ptr = runtime.current_ptr(echo_index).expect("udp ptr");
                let registered = AppRegisteredBuffer::from_lease(AppBufferLease::from_buffer(
                    runtime, echo_index,
                ))
                .expect("register udp buffer");
                backend
                    .try_push_completion_entry(AppCompletionEntry::with_attachment(
                        AppCqeDescriptor::new(
                            AppUserData::new(0),
                            b"udp-echo".len() as i32,
                            AppCqeFlags::BUFFER,
                            AppObjectRef::Socket(socket),
                            AppCqeData::RecvFrom {
                                socket,
                                source: peer,
                                buffer: registered.index(),
                            },
                        ),
                        registered,
                    ))
                    .expect("push udp recv_from cqe");

                let recv_peer = udp_echo.await.expect("join udp echo");
                let send_entry = backend
                    .next_submission_entry()
                    .await
                    .expect("udp send_to entry");
                let send_descriptor = send_entry.descriptor();
                let send_attachment = send_entry.attachment().expect("udp send attachment");
                let send_ptr = send_attachment.lease().current_ptr().expect("send ptr");
                let send_payload = send_attachment
                    .lease()
                    .copy_current()
                    .expect("copy udp payload");

                (
                    recv_peer,
                    expected_ptr as usize,
                    send_ptr as usize,
                    send_descriptor,
                    send_payload,
                )
            })
            .await
            .expect("spawn flow")
        });

    assert_eq!(result.0, peer);
    assert_eq!(result.1, result.2);
    assert_eq!(result.3.opcode(), AppOpcode::SendTo);
    assert_eq!(result.3.object(), AppObjectRef::Socket(socket));
    match result.3.payload() {
        AppSqeData::SendTo { target, .. } => assert_eq!(target, peer),
        other => panic!("expected send_to payload, got {other:?}"),
    }
    assert_eq!(result.4, b"udp-echo");

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}
