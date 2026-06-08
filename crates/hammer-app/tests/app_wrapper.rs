use std::time::Duration;

use hammer_app::{
    App, AppBufferLease, AppCqeData, AppCqeFlags, AppFlow, AppFlowId, AppObjectRef, AppOpcode,
    AppRegisteredBuffer, AppSqeData, AppSqeDescriptor, AppSubmissionEntry, AppUserData,
};
use hammer_runtime::spawn::{DataRuntime, with_data_plane_buffers};

#[test]
fn app_public_types_are_owned_by_hammer_app() {
    assert!(std::any::type_name::<hammer_app::AppFlowId>().starts_with("hammer_app::"));
    assert!(std::any::type_name::<hammer_app::AppBufferLease>().starts_with("hammer_app::"));
    assert!(std::any::type_name::<hammer_app::AppRecv>().starts_with("hammer_app::"));
    assert!(std::any::type_name::<hammer_app::AppSend>().starts_with("hammer_app::"));
    assert!(std::any::type_name::<hammer_app::AppBackend>().starts_with("hammer_app::"));
    assert!(std::any::type_name::<hammer_app::AppRuntime>().starts_with("hammer_app::"));
}

#[test]
fn app_wrapper_keeps_flow_pinned_and_zero_copy() {
    let data_runtime =
        DataRuntime::new(2, "hammer-app-wrapper-test", 512 * 1024, 2).expect("data runtime");
    let app = App::with_ring_capacity(data_runtime.context(), 4);
    let flow = AppFlowId::new(5);

    let first = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            app.spawn(flow, move |flow| async move {
                let owner = flow.owner_worker();
                let thread = std::thread::current()
                    .name()
                    .map(ToOwned::to_owned)
                    .unwrap_or_default();
                let backend = flow.backend();
                let recv_future = flow.recv();
                let recv_sqe = backend
                    .next_sqe_descriptor()
                    .await
                    .expect("recv sqe descriptor");
                let runtime = with_data_plane_buffers(Clone::clone);
                let index = runtime
                    .alloc_index_with_bytes(Default::default(), b"app-wrapper")
                    .expect("alloc app buffer");
                let expected_ptr = runtime.current_ptr(index).expect("buffer pointer");

                backend
                    .push_recv(AppBufferLease::from_buffer(runtime.clone(), index))
                    .await
                    .expect("push recv");

                assert_eq!(recv_sqe.opcode(), AppOpcode::Recv);
                let recv = recv_future.await.expect("recv");
                let recv_ptr = recv.lease().current_ptr().expect("recv pointer");
                let recv_payload = recv.lease().copy_current().expect("recv payload");

                flow.send(recv.into_send()).await.expect("send");
                let send_sqe = backend
                    .next_sqe_descriptor()
                    .await
                    .expect("send sqe descriptor");
                assert_eq!(send_sqe.opcode(), AppOpcode::Send);
                let send_ptr = recv_ptr;
                let send_payload = recv_payload.clone();

                (
                    owner,
                    thread,
                    expected_ptr as usize,
                    recv_ptr as usize,
                    send_ptr as usize,
                    recv_payload,
                    send_payload,
                )
            })
            .await
            .expect("spawn flow")
        });

    let second = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            app.spawn(flow, move |flow| async move {
                (
                    flow.owner_worker(),
                    std::thread::current()
                        .name()
                        .map(ToOwned::to_owned)
                        .unwrap_or_default(),
                )
            })
            .await
            .expect("spawn flow")
        });

    assert_eq!(first.0, second.0);
    assert_eq!(first.1, second.1);
    assert_eq!(first.2, first.3);
    assert_eq!(first.3, first.4);
    assert_eq!(first.5, b"app-wrapper");
    assert_eq!(first.6, b"app-wrapper");

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn app_flow_from_context_exposes_same_runtime_surface() {
    let data_runtime =
        DataRuntime::new(2, "hammer-app-flow-test", 512 * 1024, 2).expect("data runtime");
    let app = App::new(data_runtime.context());
    let flow = AppFlow::new(app.clone(), AppFlowId::new(9));

    let owner = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async { flow.owner().await.expect("resolve owner") });

    let spawned_owner = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            flow.run(|flow| async move { flow.owner_worker() })
                .await
                .expect("run flow")
        });

    assert_eq!(owner, spawned_owner);

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn flow_runtime_handles_use_hammer_app_wrappers() {
    let data_runtime =
        DataRuntime::new(2, "hammer-app-flow-types", 512 * 1024, 2).expect("data runtime");
    let app = App::new(data_runtime.context());
    let flow = AppFlowId::new(21);

    let result = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            app.spawn(flow, move |flow| async move {
                let backend_type = std::any::type_name_of_val(&flow.backend()).to_owned();
                let runtime_type = std::any::type_name_of_val(&flow.runtime()).to_owned();
                let ring = flow.ring();
                let ring_type = std::any::type_name_of_val(ring.runtime()).to_owned();

                let local = flow.spawn_local(|| async {});
                let local_type = std::any::type_name_of_val(&local).to_owned();
                local.await.expect("join local");

                (backend_type, runtime_type, ring_type, local_type)
            })
            .await
            .expect("spawn flow")
        });

    assert!(result.0.starts_with("hammer_app::"));
    assert!(result.1.starts_with("hammer_app::"));
    assert!(result.2.starts_with("hammer_app::"));
    assert!(result.3.starts_with("hammer_app::"));

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn hammer_app_ring_exposes_descriptor_entry_round_trip_without_send_recv_wrappers() {
    let data_runtime =
        DataRuntime::new(2, "hammer-app-low-level-ring", 512 * 1024, 2).expect("data runtime");
    let app = App::with_ring_capacity(data_runtime.context(), 4);
    let flow = AppFlowId::new(31);

    let result = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            app.spawn(flow, move |flow| async move {
                let runtime = with_data_plane_buffers(Clone::clone);
                let index = runtime
                    .alloc_index_with_bytes(Default::default(), b"app-low-level-ring")
                    .expect("alloc app buffer");
                let lease = AppBufferLease::from_buffer(runtime, index);
                let registered = AppRegisteredBuffer::from_lease(lease).expect("registered buffer");
                let descriptor = AppSqeDescriptor::new(
                    AppOpcode::Send,
                    AppUserData::new(73),
                    AppObjectRef::Flow(flow.id()),
                    AppSqeData::Send {
                        buffer: registered.index(),
                    },
                );

                flow.backend()
                    .try_push_sqe_entry(AppSubmissionEntry::with_attachment(descriptor, registered))
                    .expect("push sqe entry");

                let round_trip = flow
                    .backend()
                    .next_submission_entry()
                    .await
                    .expect("next submission entry");
                let (descriptor, attachment) = round_trip.into_parts();
                let attachment_index = attachment.expect("attachment").index();
                (descriptor, attachment_index)
            })
            .await
            .expect("spawn flow")
        });

    assert_eq!(result.0.user_data(), AppUserData::new(73));
    assert_eq!(result.0.object(), AppObjectRef::Flow(flow));
    match result.0.payload() {
        AppSqeData::Send { buffer } => {
            assert_eq!(buffer, result.1);
        }
        other => panic!("expected send payload, got {other:?}"),
    }

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn hammer_app_ring_exposes_completion_entry_without_recv_wrapper() {
    let data_runtime =
        DataRuntime::new(2, "hammer-app-low-level-cq", 512 * 1024, 2).expect("data runtime");
    let app = App::with_ring_capacity(data_runtime.context(), 4);
    let flow = AppFlowId::new(37);

    let result = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            app.spawn(flow, move |flow| async move {
                let backend = flow.backend();
                let recv_future = flow.recv();
                let recv_sqe = backend
                    .next_sqe_descriptor()
                    .await
                    .expect("recv sqe descriptor");
                let runtime = with_data_plane_buffers(Clone::clone);
                let index = runtime
                    .alloc_index_with_bytes(Default::default(), b"app-low-level-cq")
                    .expect("alloc app buffer");

                backend
                    .push_recv(AppBufferLease::from_buffer(runtime, index))
                    .await
                    .expect("push recv");

                assert_eq!(recv_sqe.opcode(), AppOpcode::Recv);
                drop(recv_future);
                let entry = flow.ring().recv_entry().await.expect("recv entry");
                let (descriptor, registered) = entry.into_parts();
                let registered = registered.expect("registered buffer");
                let (index, lease) = registered.into_parts();
                let payload = lease.copy_current().expect("recv payload");
                let payload_len = lease.current_len().expect("recv len");
                lease.release();

                (descriptor, index, payload_len, payload)
            })
            .await
            .expect("spawn flow")
        });

    assert_eq!(result.0.object(), AppObjectRef::Flow(flow));
    match result.0.payload() {
        AppCqeData::Recv {
            flow: recv_flow,
            buffer,
        } => {
            assert_eq!(recv_flow, flow);
            assert_eq!(buffer, result.1);
        }
        other => panic!("expected recv completion payload, got {other:?}"),
    }
    assert_eq!(result.2, "app-low-level-cq".len());
    assert_eq!(result.3, b"app-low-level-cq");

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn hammer_app_ring_exposes_symmetric_entry_surfaces_without_send_recv_wrappers() {
    let data_runtime =
        DataRuntime::new(2, "hammer-app-entry-surfaces", 512 * 1024, 2).expect("data runtime");
    let app = App::with_ring_capacity(data_runtime.context(), 4);
    let flow = AppFlowId::new(41);

    let result = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            app.spawn(flow, move |flow| async move {
                let ring = flow.ring();
                let backend = flow.backend();
                let runtime = with_data_plane_buffers(Clone::clone);

                let send_index = runtime
                    .alloc_index_with_bytes(Default::default(), b"entry-send")
                    .expect("alloc send buffer");
                let send_registered = AppRegisteredBuffer::from_lease(AppBufferLease::from_buffer(
                    runtime.clone(),
                    send_index,
                ))
                .expect("registered send buffer");
                let send_entry = AppSubmissionEntry::with_attachment(
                    AppSqeDescriptor::new(
                        AppOpcode::Send,
                        AppUserData::new(91),
                        AppObjectRef::Flow(flow.id()),
                        AppSqeData::Send {
                            buffer: send_registered.index(),
                        },
                    ),
                    send_registered,
                );
                let send_attachment_index =
                    send_entry.attachment().expect("send attachment").index();
                let send_attachment_len = send_entry
                    .attachment()
                    .expect("send attachment")
                    .lease()
                    .current_len()
                    .expect("send attachment len");

                ring.try_push_submission_entry(send_entry)
                    .expect("push submission entry");
                let send_round_trip = ring
                    .next_submission_entry()
                    .await
                    .expect("next submission entry");
                let (send_descriptor, send_registered) = send_round_trip.into_parts();
                let send_round_trip_attachment = send_registered
                    .expect("send round-trip attachment")
                    .into_parts();
                let send_round_trip_attachment_index = send_round_trip_attachment.0;
                let send_round_trip_attachment_len = send_round_trip_attachment
                    .1
                    .current_len()
                    .expect("send round-trip attachment len");

                let recv_index = runtime
                    .alloc_index_with_bytes(Default::default(), b"entry-recv")
                    .expect("alloc recv buffer");
                let recv_future = flow.recv();
                let recv_sqe = backend
                    .next_sqe_descriptor()
                    .await
                    .expect("recv sqe descriptor");
                backend
                    .push_recv(AppBufferLease::from_buffer(runtime, recv_index))
                    .await
                    .expect("push recv");

                assert_eq!(recv_sqe.opcode(), AppOpcode::Recv);
                drop(recv_future);
                let completion_entry = ring
                    .next_completion_entry()
                    .await
                    .expect("next completion entry");
                let (completion_descriptor, completion_registered) = completion_entry.into_parts();
                let completion_attachment = completion_registered
                    .expect("completion attachment")
                    .into_parts();
                let completion_attachment_index = completion_attachment.0;
                let completion_attachment_len = completion_attachment
                    .1
                    .current_len()
                    .expect("completion attachment len");

                (
                    send_descriptor,
                    send_attachment_index,
                    send_attachment_len,
                    send_round_trip_attachment_index,
                    send_round_trip_attachment_len,
                    completion_descriptor,
                    completion_attachment_index,
                    completion_attachment_len,
                )
            })
            .await
            .expect("spawn flow")
        });

    assert_eq!(result.0.opcode(), AppOpcode::Send);
    assert_eq!(result.0.user_data(), AppUserData::new(91));
    assert_eq!(result.0.object(), AppObjectRef::Flow(flow));
    match result.0.payload() {
        AppSqeData::Send { buffer } => {
            assert_eq!(buffer, result.1);
        }
        other => panic!("expected send submission payload, got {other:?}"),
    }
    assert_eq!(result.2, b"entry-send".len());
    assert_eq!(result.3, result.1);
    assert_eq!(result.4, result.2);

    let completion_descriptor = result.5;
    assert_eq!(completion_descriptor.user_data(), AppUserData::new(0));
    assert_eq!(completion_descriptor.result(), b"entry-recv".len() as i32);
    assert!(completion_descriptor.flags().contains(AppCqeFlags::BUFFER));
    assert_eq!(completion_descriptor.object(), AppObjectRef::Flow(flow));
    match completion_descriptor.payload() {
        AppCqeData::Recv {
            flow: recv_flow,
            buffer,
        } => {
            assert_eq!(recv_flow, flow);
            assert_eq!(buffer, result.6);
        }
        other => panic!("expected recv completion payload, got {other:?}"),
    }
    assert_eq!(result.7, b"entry-recv".len());

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}
