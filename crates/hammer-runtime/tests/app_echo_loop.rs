use std::time::Duration;

use hammer_runtime::app::{
    AppBufferLease, AppContext, AppFlowId, AppObjectRef, AppOpcode, AppSend, AppSqeData,
};
use hammer_runtime::spawn::{DataRuntime, with_data_plane_buffers};

#[test]
fn app_echo_loop_runs_on_owner_worker_with_local_executor() {
    let data_runtime = DataRuntime::new(1, "app-echo-test", 512 * 1024, 2).expect("data runtime");
    let app = AppContext::with_ring_capacity(data_runtime.context(), 4);
    let flow = AppFlowId::new(11);

    let result = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            app.spawn_on_flow(flow, move |worker| async move {
                let app_runtime = worker.runtime();
                let backend = worker.backend();
                let owner_thread = std::thread::current()
                    .name()
                    .map(ToOwned::to_owned)
                    .unwrap_or_default();
                let before = std::thread::current()
                    .name()
                    .map(ToOwned::to_owned)
                    .unwrap_or_default();
                let recv_future = app_runtime.recv();

                let runtime = with_data_plane_buffers(Clone::clone);
                let recv_descriptor = backend
                    .next_sqe_descriptor()
                    .await
                    .expect("collect recv descriptor");
                let index = runtime
                    .alloc_index_with_bytes(Default::default(), b"echo-from-app")
                    .expect("alloc app echo buffer");
                backend
                    .complete_recv(AppBufferLease::from_buffer(runtime.clone(), index))
                    .await
                    .expect("complete recv");
                let recv = recv_future.await.expect("recv echo buffer");
                let recv_ptr = recv.lease().current_ptr().expect("recv pointer") as usize;
                let copied = recv.lease().copy_current().expect("copy echo payload");
                let send = recv.into_send();
                let send_ptr = send.lease().current_ptr().expect("send pointer") as usize;
                app_runtime.send(send).await.expect("echo send");
                let after = std::thread::current()
                    .name()
                    .map(ToOwned::to_owned)
                    .unwrap_or_default();
                let send_descriptor = backend
                    .next_sqe_descriptor()
                    .await
                    .expect("collect echo send descriptor");

                (
                    owner_thread,
                    before,
                    after,
                    copied,
                    recv_ptr,
                    send_ptr,
                    recv_descriptor,
                    send_descriptor,
                )
            })
            .await
            .expect("spawn flow task")
        });

    assert_eq!(result.0, result.1);
    assert_eq!(result.1, result.2);
    assert_eq!(result.3, b"echo-from-app");
    assert_eq!(result.4, result.5);
    assert_eq!(result.6.opcode(), hammer_runtime::app::AppOpcode::Recv);
    assert_eq!(result.7.opcode(), hammer_runtime::app::AppOpcode::Send);

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn app_context_send_on_flow_forwards_registered_buffer_across_workers() {
    let data_runtime =
        DataRuntime::new(2, "app-send-on-flow", 512 * 1024, 2).expect("data runtime");
    let app = AppContext::with_ring_capacity(data_runtime.context(), 4);
    let source_flow = AppFlowId::new(10);
    let target_flow = AppFlowId::new(11);

    let result = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            let app_for_source = app.clone();
            let source_ptr = app
                .spawn_on_flow(source_flow, move |_worker| async move {
                    let runtime = with_data_plane_buffers(Clone::clone);
                    let index = runtime
                        .alloc_index_with_bytes(Default::default(), b"cross-worker-send")
                        .expect("alloc cross-worker send buffer");
                    let send = AppSend::new(AppBufferLease::from_buffer(runtime, index));
                    let source_ptr = send.lease().current_ptr().expect("source pointer") as usize;
                    app_for_source
                        .send_on_flow(target_flow, send)
                        .await
                        .expect("send on target flow");
                    source_ptr
                })
                .await
                .expect("spawn source flow");

            let (descriptor, payload, target_ptr) = app
                .spawn_on_flow(target_flow, move |worker| async move {
                    let entry = worker
                        .backend()
                        .next_submission_entry()
                        .await
                        .expect("target send entry");
                    let descriptor = *entry.descriptor();
                    let attachment = entry.attachment().expect("target send attachment");
                    let payload = attachment
                        .lease()
                        .copy_current()
                        .expect("copy target payload");
                    let ptr = attachment.lease().current_ptr().expect("target pointer") as usize;
                    (descriptor, payload, ptr)
                })
                .await
                .expect("spawn target flow");

            (source_ptr, descriptor, payload, target_ptr)
        });

    assert_eq!(result.1.opcode(), AppOpcode::Send);
    assert_eq!(result.1.object(), AppObjectRef::Flow(target_flow));
    match result.1.payload() {
        AppSqeData::Send { .. } => {}
        other => panic!("unexpected sqe data: {other:?}"),
    }
    assert_eq!(result.2, b"cross-worker-send");
    assert_eq!(
        result.0, result.3,
        "cross-worker send must preserve the registered buffer lease"
    );

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}
