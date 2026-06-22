use std::time::Duration;

use hammer_app::echo::echo_once;
use hammer_app::{
    App, AppObjectRef, AppOpId, AppOpcode, AppSqeData, AppSubmissionEntry, AppUserData,
};
use hammer_runtime::spawn::{DataRuntime, with_data_plane_buffers};

#[test]
fn hammer_app_exposes_op_owned_runtime_surface() {
    assert!(std::any::type_name::<hammer_app::AppOpId>().contains("Descriptor"));
    assert!(std::any::type_name::<hammer_app::AppRecv>().contains("hammer_runtime::app"));
    assert!(std::any::type_name::<hammer_app::AppSend>().contains("hammer_runtime::app"));
    assert!(std::any::type_name::<hammer_app::AppRuntime>().starts_with("hammer_app::"));
}

#[test]
fn app_op_echo_copies_recv_into_app_data_and_enqueues_send_descriptor() {
    let data_runtime =
        DataRuntime::new(1, "hammer-app-op-echo", 512 * 1024, 2).expect("data runtime");
    let app = App::with_ring_capacity(data_runtime.context(), 4);
    let op = AppOpId::new(19);

    let result = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            app.spawn_on_op(op, 0, move |op_ctx| async move {
                let runtime = op_ctx.runtime();
                let echo = op_ctx.spawn_local({
                    let op_ctx = op_ctx.clone();
                    move || async move {
                        echo_once(&op_ctx).await.expect("echo once");
                    }
                });

                let recv_descriptor = runtime
                    .next_submission_descriptor()
                    .await
                    .expect("recv descriptor");
                assert_eq!(recv_descriptor.opcode(), AppOpcode::Recv);
                assert_eq!(recv_descriptor.object(), AppObjectRef::Operation(op));

                let buffers = with_data_plane_buffers(Clone::clone);
                let before = buffers.in_use_buffers();
                let index = buffers
                    .alloc_index_with_bytes(b"hammer-app-echo")
                    .expect("alloc recv buffer");

                runtime
                    .complete_recv_buffer(buffers.clone(), index)
                    .await
                    .expect("complete recv");
                echo.await.expect("join echo");
                let after = buffers.in_use_buffers();

                let entry = runtime
                    .next_submission_entry()
                    .await
                    .expect("send submission");
                let descriptor = *entry.descriptor();
                assert!(entry.attachment().is_none());
                let payload = match descriptor.payload() {
                    AppSqeData::Send { data } => runtime.read_data(data).expect("read app data"),
                    other => panic!("unexpected payload: {other:?}"),
                };

                (before, after, descriptor, payload)
            })
            .await
            .expect("spawn app op")
        });

    assert_eq!(result.0, 0);
    assert_eq!(result.1, 0);
    assert_eq!(result.2.opcode(), AppOpcode::Send);
    assert_eq!(result.2.object(), AppObjectRef::Operation(op));
    assert_eq!(result.3, b"hammer-app-echo");

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn app_op_ring_round_trips_descriptor_entries_without_attachments() {
    let data_runtime =
        DataRuntime::new(1, "hammer-app-op-entry", 512 * 1024, 2).expect("data runtime");
    let app = App::with_ring_capacity(data_runtime.context(), 4);
    let op = AppOpId::new(23);

    let descriptor = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            app.spawn_on_op(op, 0, move |op_ctx| async move {
                let runtime = op_ctx.runtime();
                let send = runtime
                    .send_from_bytes(b"entry-payload")
                    .expect("send data");
                let data = send.into_data_addr().expect("send addr");
                let descriptor = hammer_app::AppSqeDescriptor::new(
                    AppOpcode::Send,
                    Some(AppUserData::new(77)),
                    AppObjectRef::Operation(op),
                    AppSqeData::Send { data },
                );
                runtime
                    .try_push_submission_entry(AppSubmissionEntry::new(descriptor))
                    .expect("push entry");
                let entry = runtime.next_submission_entry().await.expect("next entry");
                assert!(entry.attachment().is_none());
                *entry.descriptor()
            })
            .await
            .expect("spawn app op")
        });

    assert_eq!(descriptor.user_data(), Some(AppUserData::new(77)));
    assert_eq!(descriptor.object(), AppObjectRef::Operation(op));

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}
