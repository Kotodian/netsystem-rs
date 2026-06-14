use std::time::Duration;

use hammer_runtime::app::{AppContext, AppObjectRef, AppOpId, AppOpcode, AppSqeData};
use hammer_runtime::spawn::{DataRuntime, with_data_plane_buffers};

#[test]
fn app_echo_loop_runs_on_owner_worker_with_local_executor() {
    let data_runtime = DataRuntime::new(1, "app-echo-test", 512 * 1024, 2).expect("data runtime");
    let app = AppContext::with_ring_capacity(data_runtime.context(), 4);
    let op = AppOpId::new(11);

    let result = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            app.spawn_on_op(op, 0, move |worker| async move {
                let app_runtime = worker.runtime();
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
                let recv_descriptor = app_runtime
                    .next_submission_descriptor()
                    .await
                    .expect("collect recv descriptor");
                let index = runtime
                    .alloc_index_with_bytes(Default::default(), b"echo-from-app")
                    .expect("alloc app echo buffer");
                app_runtime
                    .complete_recv_buffer(runtime.clone(), index)
                    .await
                    .expect("complete recv");
                let recv = recv_future.await.expect("recv echo buffer");
                let copied = recv.copy_current().expect("copy echo payload");
                let send = recv.into_send();
                app_runtime.send(send).await.expect("echo send");
                let after = std::thread::current()
                    .name()
                    .map(ToOwned::to_owned)
                    .unwrap_or_default();
                let send_descriptor = app_runtime
                    .next_submission_descriptor()
                    .await
                    .expect("collect echo send descriptor");

                (
                    owner_thread,
                    before,
                    after,
                    copied,
                    recv_descriptor,
                    send_descriptor,
                )
            })
            .await
            .expect("spawn app op task")
        });

    assert_eq!(result.0, result.1);
    assert_eq!(result.1, result.2);
    assert_eq!(result.3, b"echo-from-app");
    assert_eq!(result.4.opcode(), AppOpcode::Recv);
    assert_eq!(result.5.opcode(), AppOpcode::Send);

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn app_context_send_on_op_forwards_app_data_across_workers() {
    let data_runtime = DataRuntime::new(2, "app-send-on-op", 512 * 1024, 2).expect("data runtime");
    let app = AppContext::with_ring_capacity(data_runtime.context(), 4);
    let source_op = AppOpId::new(10);
    let target_op = AppOpId::new(11);

    let result = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            let app_for_source = app.clone();
            app.spawn_on_op(source_op, 0, move |worker| async move {
                let send = worker
                    .runtime()
                    .send_from_bytes(b"cross-worker-send")
                    .expect("app send data");
                app_for_source
                    .send_on_op(target_op, send)
                    .await
                    .expect("send on target op");
            })
            .await
            .expect("spawn source op");

            let (descriptor, payload) = app
                .spawn_on_op(target_op, 1, move |worker| async move {
                    let runtime = worker.runtime();
                    let entry = runtime
                        .next_submission_entry()
                        .await
                        .expect("target send entry");
                    let descriptor = *entry.descriptor();
                    assert!(entry.attachment().is_none());
                    let payload = match descriptor.payload() {
                        AppSqeData::Send { data } => {
                            runtime.read_data(data).expect("copy target payload")
                        }
                        other => panic!("unexpected sqe data: {other:?}"),
                    };
                    (descriptor, payload)
                })
                .await
                .expect("spawn target op");

            (descriptor, payload)
        });

    assert_eq!(result.0.opcode(), AppOpcode::Send);
    assert_eq!(result.0.object(), AppObjectRef::Operation(target_op));
    match result.0.payload() {
        AppSqeData::Send { .. } => {}
        other => panic!("unexpected sqe data: {other:?}"),
    }
    assert_eq!(result.1, b"cross-worker-send");

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}
