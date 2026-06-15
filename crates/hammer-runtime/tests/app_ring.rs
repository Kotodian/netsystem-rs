use std::future::Future;
use std::time::Duration;

use hammer_infra::align::CACHE_LINE;
use hammer_runtime::app::{
    AppCompletionEntry, AppContext, AppCqe, AppCqeData, AppCqeDescriptor, AppCqeFlags, AppCqeKind,
    AppDataAddr, AppDataArea, AppDataAreaConfig, AppObjectRef, AppOpId, AppOpcode, AppRingHandle,
    AppRingMemoryKind, AppSqe, AppSqeData, AppSqeDescriptor, AppSubmissionEntry, AppUserData,
};
use hammer_runtime::spawn::{DataRuntime, with_data_plane_buffers};

#[test]
fn app_ring_uses_opaque_operation_identity() {
    let op = AppOpId::new(7);
    let object = AppObjectRef::Operation(op);

    assert_eq!(object, AppObjectRef::Operation(op));
    assert!(matches!(
        AppSqeData::Recv { max_len: 64 },
        AppSqeData::Recv { max_len: 64 }
    ));
    assert_eq!(AppCqeData::None, AppCqeData::None);
}

#[test]
fn app_data_area_allocates_copies_and_rejects_stale_addresses() {
    let area = AppDataArea::new(AppDataAreaConfig {
        chunk_size: 64,
        chunk_count: 2,
    })
    .expect("data area");

    let first = area.alloc().expect("first chunk");
    assert_eq!(first.offset(), 0);
    assert_eq!(first.offset() % CACHE_LINE, 0);
    assert_eq!(first.len(), 0);
    assert_eq!(first.capacity(), 64);

    let first = area.write(first, b"hello").expect("write first");
    assert_eq!(area.read(first).expect("read first"), b"hello");

    let second = area.alloc().expect("second chunk");
    assert_eq!(second.offset(), 64);
    assert_eq!(second.offset() % CACHE_LINE, 0);
    assert!(area.alloc().is_none());

    area.release(first).expect("release first");
    assert!(
        area.read(first).is_err(),
        "released address generation must be stale"
    );

    let reused = area.alloc().expect("reused chunk");
    assert_eq!(reused.offset(), 0);
    assert_ne!(reused.generation(), first.generation());

    area.release(second).expect("release second");
    area.release(reused).expect("release reused");
}

#[test]
fn app_data_area_rejects_non_cacheline_chunk_size() {
    assert!(
        AppDataArea::new(AppDataAreaConfig {
            chunk_size: CACHE_LINE - 1,
            chunk_count: 1,
        })
        .is_err()
    );
}

#[test]
fn app_data_area_rejects_forged_chunk_offset() {
    let area = AppDataArea::new(AppDataAreaConfig {
        chunk_size: 64,
        chunk_count: 2,
    })
    .expect("data area");
    let addr = area.alloc().expect("chunk");
    let forged = AppDataAddr::new(
        addr.chunk(),
        addr.generation(),
        64,
        addr.len() as u32,
        addr.capacity() as u32,
    );

    assert!(area.write(forged, b"bad").is_err());
    area.release(addr).expect("release original");
}

#[test]
fn app_ring_export_layout_has_no_process_local_state() {
    let ring = AppRingHandle::with_data_area(8, 16, 2048, 64).expect("ring");
    let export = ring.export_layout();

    assert_eq!(export.memory_kind(), AppRingMemoryKind::ProcessLocal);
    assert_eq!(export.cacheline_size(), CACHE_LINE);
    assert_eq!(export.submission_capacity(), 8);
    assert_eq!(export.completion_capacity(), 16);
    assert_eq!(export.data_chunk_count(), 64);
    assert_eq!(export.data_chunk_size(), 2048);
    assert_eq!(export.submission_ring_offset(), 0);
    assert!(export.submission_ring_bytes() > 0);
    assert!(export.completion_ring_bytes() > 0);
    assert!(export.fill_ring_bytes() > 0);
    assert!(export.submission_ring_offset() < export.completion_ring_offset());
    assert!(export.completion_ring_offset() < export.fill_ring_offset());
    assert!(export.fill_ring_offset() < export.data_area_offset());
    assert_eq!(export.submission_ring_offset() % CACHE_LINE, 0);
    assert_eq!(export.completion_ring_offset() % CACHE_LINE, 0);
    assert_eq!(export.fill_ring_offset() % CACHE_LINE, 0);
    assert_eq!(export.data_area_offset() % CACHE_LINE, 0);
}

#[test]
fn app_descriptors_use_data_area_addresses_not_dataplane_buffer_indexes() {
    let addr = AppDataAddr::new(3, 9, 4096, 12, 2048);

    assert_eq!(
        AppSqeData::Send { data: addr },
        AppSqeData::Send { data: addr }
    );
    assert_eq!(
        AppCqeData::Recv { data: addr },
        AppCqeData::Recv { data: addr }
    );
}

#[test]
fn app_ring_handle_batches_submissions_and_completions() {
    let ring = AppRingHandle::for_tests(4, 4);
    let op = AppOpId::new(77);

    ring.push_test_submission(AppSqe::nop(Some(AppUserData::new(1))))
        .expect("first sqe");
    ring.push_test_submission(AppSqe::close(Some(AppUserData::new(2)), op))
        .expect("second sqe");

    let sqes = ring.take_test_submissions(8);
    assert_eq!(sqes.len(), 2);
    assert_eq!(sqes[0].opcode(), AppOpcode::Nop);
    assert_eq!(sqes[1].opcode(), AppOpcode::Close);
    assert_eq!(sqes[1].op(), Some(op));

    ring.push_test_completion(AppCqe::closed(Some(AppUserData::new(2)), Some(op)))
        .expect("closed cqe");

    let cqes = ring.take_test_completions(8);
    assert_eq!(cqes.len(), 1);
    assert_eq!(cqes[0].user_data(), Some(AppUserData::new(2)));
    assert_eq!(cqes[0].opcode(), AppOpcode::Close);
}

#[test]
fn app_runtime_try_pop_submission_entry_without_awaiting() {
    let data_runtime = DataRuntime::new(1, "app-runtime-sync-entry-pop-test", 512 * 1024, 2)
        .expect("data runtime");
    let app = AppContext::with_ring_capacity(data_runtime.context(), 2);
    let op = AppOpId::new(101);

    let round_trip = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            app.spawn_on_op(op, 0, move |worker| async move {
                let runtime = worker.runtime();
                let send = runtime
                    .send_from_bytes(b"sync-entry-send")
                    .expect("send data");
                let data = send.into_data_addr().expect("send data address");
                let descriptor = AppSqeDescriptor::new(
                    AppOpcode::Send,
                    Some(AppUserData::new(101)),
                    AppObjectRef::Operation(op),
                    AppSqeData::Send { data },
                );

                runtime
                    .try_push_submission_entry(AppSubmissionEntry::new(descriptor))
                    .expect("push submission entry");

                let entry = runtime
                    .try_pop_submission_entry()
                    .expect("pop submission entry");
                assert!(runtime.try_pop_submission_entry().is_none());
                let (descriptor_round_trip, registered) = entry.into_parts();
                assert!(registered.is_none());
                let payload = match descriptor_round_trip.payload() {
                    AppSqeData::Send { data } => runtime
                        .read_data(data)
                        .expect("copy send payload from app data"),
                    other => panic!("unexpected payload: {other:?}"),
                };
                (descriptor_round_trip, payload)
            })
            .await
            .expect("spawn app op task")
        });

    assert_eq!(round_trip.0.user_data(), Some(AppUserData::new(101)));
    assert_eq!(round_trip.0.object(), AppObjectRef::Operation(op));
    assert_eq!(round_trip.1, b"sync-entry-send");

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn app_send_descriptor_uses_data_area_address_not_payload_object() {
    let data_runtime =
        DataRuntime::new(1, "app-ring-descriptor-test", 512 * 1024, 2).expect("data runtime");
    let app = AppContext::with_ring_capacity(data_runtime.context(), 2);
    let op = AppOpId::new(31);

    let descriptor = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            app.spawn_on_op(op, 0, move |worker| async move {
                let app_runtime = worker.runtime();
                let send = app_runtime
                    .send_from_bytes(b"descriptor-buffer")
                    .expect("send data");
                app_runtime
                    .try_push_submission(AppSqe::send(Some(AppUserData::new(55)), op, send))
                    .expect("push send");
                app_runtime
                    .try_pop_submission_descriptor()
                    .expect("send descriptor")
            })
            .await
            .expect("spawn app op task")
        });

    assert_eq!(descriptor.opcode(), AppOpcode::Send);
    assert_eq!(descriptor.user_data(), Some(AppUserData::new(55)));
    assert_eq!(descriptor.object(), AppObjectRef::Operation(op));
    match descriptor.payload() {
        AppSqeData::Send { data } => {
            assert_eq!(data.len(), "descriptor-buffer".len());
            assert_eq!(data.offset() % CACHE_LINE, 0);
        }
        other => panic!("unexpected sqe data: {other:?}"),
    }

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn app_runtime_send_enqueues_op_owned_send_sqe_descriptor() {
    let data_runtime =
        DataRuntime::new(1, "app-runtime-send-sqe-test", 512 * 1024, 2).expect("data runtime");
    let app = AppContext::with_ring_capacity(data_runtime.context(), 2);
    let op = AppOpId::new(61);

    let (recv_descriptor, send_descriptor) = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            app.spawn_on_op(op, 0, move |worker| async move {
                let app_runtime = worker.runtime();
                let runtime = with_data_plane_buffers(Clone::clone);
                let recv_future = app_runtime.recv();
                let recv_sqe = app_runtime
                    .next_submission_descriptor()
                    .await
                    .expect("recv sqe descriptor");
                let index = runtime
                    .alloc_index_with_bytes(Default::default(), b"runtime-send-sqe")
                    .expect("alloc app send buffer");

                app_runtime
                    .complete_recv_buffer(runtime.clone(), index)
                    .await
                    .expect("complete recv cqe");

                let recv = recv_future.await.expect("recv cqe");
                app_runtime.send(recv.into_send()).await.expect("send sqe");

                (
                    recv_sqe,
                    app_runtime
                        .next_send()
                        .await
                        .expect("send sqe")
                        .descriptor(None, op)
                        .expect("sqe descriptor"),
                )
            })
            .await
            .expect("spawn app op task")
        });

    assert_eq!(recv_descriptor.opcode(), AppOpcode::Recv);
    assert_eq!(recv_descriptor.object(), AppObjectRef::Operation(op));
    assert_eq!(send_descriptor.opcode(), AppOpcode::Send);
    assert_eq!(send_descriptor.object(), AppObjectRef::Operation(op));
    match send_descriptor.payload() {
        AppSqeData::Send { data } => {
            assert_eq!(data.len(), "runtime-send-sqe".len());
            assert_eq!(data.offset() % CACHE_LINE, 0);
        }
        other => panic!("unexpected sqe data: {other:?}"),
    }

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn app_context_try_complete_recv_buffer_enqueues_op_owned_cqe() {
    let data_runtime =
        DataRuntime::new(1, "app-complete-recv-cqe-test", 512 * 1024, 2).expect("data runtime");
    let app = AppContext::with_ring_capacity(data_runtime.context(), 2);
    let op = AppOpId::new(71);

    let result = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            let app_for_worker = app.clone();
            app.spawn_on_op(op, 0, move |worker| async move {
                let app_runtime = worker.runtime();
                let recv_future = app_runtime.recv();
                let recv_sqe = app_runtime
                    .next_submission_descriptor()
                    .await
                    .expect("recv sqe descriptor");
                let runtime = with_data_plane_buffers(Clone::clone);
                let before_in_use = with_data_plane_buffers(|runtime| runtime.in_use_buffers());
                let index = runtime
                    .alloc_index_with_bytes(Default::default(), b"complete-recv")
                    .expect("alloc app recv buffer");

                app_for_worker
                    .try_complete_recv_buffer(op, runtime.clone(), index, false)
                    .expect("enqueue recv cqe");

                assert_eq!(recv_sqe.user_data(), None);
                let recv = recv_future.await.expect("recv cqe");
                let descriptor = AppCqeDescriptor::new(
                    recv_sqe.user_data(),
                    recv.copy_current().expect("recv payload").len() as i32,
                    AppCqeFlags::BUFFER,
                    AppObjectRef::Operation(op),
                    AppCqeData::Recv { data: recv.data() },
                );
                let recv_payload = recv.copy_current().expect("recv payload");
                recv.release();
                let after_in_use = with_data_plane_buffers(|runtime| runtime.in_use_buffers());

                (descriptor, recv_payload, before_in_use, after_in_use)
            })
            .await
            .expect("spawn app op task")
        });

    assert_eq!(result.0.user_data(), None);
    assert_eq!(result.0.result(), "complete-recv".len() as i32);
    assert_eq!(result.0.object(), AppObjectRef::Operation(op));
    assert!(result.0.flags().contains(AppCqeFlags::BUFFER));
    match result.0.payload() {
        AppCqeData::Recv { data } => {
            assert_eq!(data.len(), "complete-recv".len());
            assert_eq!(data.offset() % CACHE_LINE, 0);
        }
        other => panic!("unexpected cqe data: {other:?}"),
    }
    assert_eq!(result.1, b"complete-recv");
    assert_eq!(result.2, 0);
    assert_eq!(result.3, 0);

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn app_context_try_complete_recv_buffer_requires_pending_recv_submission() {
    let data_runtime = DataRuntime::new(1, "app-complete-recv-requires-sqe-test", 512 * 1024, 2)
        .expect("data runtime");
    let app = AppContext::with_ring_capacity(data_runtime.context(), 2);
    let op = AppOpId::new(72);

    let result = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            let app_for_worker = app.clone();
            app.spawn_on_op(op, 0, move |_worker| async move {
                let runtime = with_data_plane_buffers(Clone::clone);
                let index = runtime
                    .alloc_index_with_bytes(Default::default(), b"missing-recv-sqe")
                    .expect("alloc app recv buffer");

                let err = app_for_worker
                    .try_complete_recv_buffer(op, runtime.clone(), index, false)
                    .expect_err("recv completion should require a pending recv sqe");

                (err.to_string(), runtime.in_use_buffers())
            })
            .await
            .expect("spawn app op task")
        });

    assert!(
        result.0.contains("pending recv"),
        "unexpected error: {}",
        result.0
    );
    assert_eq!(result.1, 0);

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn app_context_failed_recv_completion_keeps_pending_recv_submission() {
    let data_runtime = DataRuntime::new(1, "app-complete-recv-keeps-pending-test", 512 * 1024, 2)
        .expect("data runtime");
    let app = AppContext::with_ring_capacity(data_runtime.context(), 2);
    let op = AppOpId::new(73);

    let payload = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            let app_for_worker = app.clone();
            app.spawn_on_op(op, 0, move |worker| async move {
                let app_runtime = worker.runtime();
                let recv_future = app_runtime.recv();
                let _recv_sqe = app_runtime
                    .next_submission_descriptor()
                    .await
                    .expect("recv sqe descriptor");
                let runtime = with_data_plane_buffers(Clone::clone);
                let first = runtime
                    .alloc_index_with_bytes(Default::default(), b"first")
                    .expect("alloc first recv buffer");

                app_for_worker
                    .try_complete_recv_buffer(op, runtime.clone(), first, false)
                    .expect("first recv completion");

                let second = runtime
                    .alloc_index_with_bytes(Default::default(), b"second")
                    .expect("alloc second recv buffer");
                app_for_worker
                    .try_complete_recv_buffer(op, runtime.clone(), second, false)
                    .expect_err("pending recv was already consumed");
                assert_eq!(runtime.in_use_buffers(), 0);

                let recv = recv_future.await.expect("first recv cqe");
                let payload = recv.copy_current().expect("first payload");
                recv.release();
                payload
            })
            .await
            .expect("spawn app op task")
        });

    assert_eq!(payload, b"first");

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn app_runtime_reuses_one_ring_handle_for_sq_and_cq() {
    let data_runtime =
        DataRuntime::new(1, "app-shared-ring-handle-test", 512 * 1024, 2).expect("data runtime");
    let app = AppContext::with_ring_capacity(data_runtime.context(), 2);
    let op = AppOpId::new(81);

    let result = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            let app_for_worker = app.clone();
            app.spawn_on_op(op, 0, move |worker| async move {
                let app_runtime = worker.runtime();
                let recv_future = app_runtime.recv();
                let recv_sqe = app_runtime
                    .next_submission_descriptor()
                    .await
                    .expect("recv sqe descriptor");
                let runtime = with_data_plane_buffers(Clone::clone);
                let index = runtime
                    .alloc_index_with_bytes(Default::default(), b"shared-ring")
                    .expect("alloc shared ring buffer");

                app_for_worker
                    .try_complete_recv_buffer(op, runtime.clone(), index, false)
                    .expect("enqueue recv cqe");

                assert_eq!(recv_sqe.opcode(), AppOpcode::Recv);
                let recv = recv_future.await.expect("recv cqe");
                let recv_payload = recv.copy_current().expect("recv payload");
                app_runtime
                    .send(recv.into_send())
                    .await
                    .expect("enqueue send sqe");

                let send = app_runtime.next_send().await.expect("ring submission");
                let descriptor = send.descriptor(None, op).expect("sqe descriptor");

                (recv_payload, descriptor)
            })
            .await
            .expect("spawn app op task")
        });

    assert_eq!(result.0, b"shared-ring");
    assert_eq!(result.1.opcode(), AppOpcode::Send);
    assert_eq!(result.1.object(), AppObjectRef::Operation(op));
    match result.1.payload() {
        AppSqeData::Send { data } => {
            assert_eq!(data.len(), "shared-ring".len());
            assert_eq!(data.offset() % CACHE_LINE, 0);
        }
        other => panic!("unexpected sqe data: {other:?}"),
    }

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn app_ring_handle_round_trips_pure_descriptors_without_high_level_objects() {
    let ring = AppRingHandle::new(4, 4);
    let op = AppOpId::new(91);
    let send_descriptor = AppSqeDescriptor::new(
        AppOpcode::Send,
        Some(AppUserData::new(41)),
        AppObjectRef::Operation(op),
        AppSqeData::Send {
            data: AppDataAddr::new(7, 9, 64, 11, 64),
        },
    );
    let recv_descriptor = AppCqeDescriptor::new(
        Some(AppUserData::new(42)),
        13,
        AppCqeFlags::BUFFER,
        AppObjectRef::Operation(op),
        AppCqeData::Recv {
            data: AppDataAddr::new(7, 9, 64, 11, 64),
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
    let op = AppOpId::new(151);
    let send_descriptor = AppSqeDescriptor::new(
        AppOpcode::Close,
        Some(AppUserData::new(51)),
        AppObjectRef::Operation(op),
        AppSqeData::Close,
    );
    let recv_descriptor = AppCqeDescriptor::new(
        Some(AppUserData::new(52)),
        0,
        AppCqeFlags::NONE,
        AppObjectRef::Operation(op),
        AppCqeData::Closed,
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
fn app_ring_connected_completion_round_trips_descriptor() {
    let ring = AppRingHandle::new(4, 4);
    let op = AppOpId::new(9_001);

    ring.push_test_completion(AppCqe::connected(Some(AppUserData::new(44)), op))
        .expect("push connected completion");

    let completion = ring.pop_completion().expect("connected completion");
    assert_eq!(completion.user_data(), Some(AppUserData::new(44)));
    assert_eq!(completion.opcode(), AppOpcode::Nop);
    match completion.kind() {
        AppCqeKind::Connected { op: completed_op } => assert_eq!(*completed_op, op),
        other => panic!("expected connected completion, got {other:?}"),
    }

    let descriptor = AppCqe::connected(None, op)
        .descriptor()
        .expect("connected descriptor")
        .expect("connected descriptor present");
    assert_eq!(descriptor.result(), 0);
    assert_eq!(descriptor.flags(), AppCqeFlags::NONE);
    assert_eq!(descriptor.object(), AppObjectRef::Operation(op));
    assert_eq!(descriptor.payload(), AppCqeData::Connected);

    let round_trip = AppCqe::from((descriptor, ring.clone()));
    match round_trip.kind() {
        AppCqeKind::Connected { op: completed_op } => assert_eq!(*completed_op, op),
        other => panic!("expected connected round trip, got {other:?}"),
    }
}

#[test]
fn app_ring_entry_round_trips_recv_data_address_for_object_view() {
    let data_runtime =
        DataRuntime::new(1, "app-ring-entry-recv-test", 512 * 1024, 2).expect("data runtime");
    let ring = AppRingHandle::new(4, 4);
    let op = AppOpId::new(161);

    let result = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            let data = ring
                .alloc_data_for_bytes(b"entry-data-recv")
                .expect("recv data");
            let descriptor = AppCqeDescriptor::new(
                Some(AppUserData::new(64)),
                "entry-data-recv".len() as i32,
                AppCqeFlags::BUFFER,
                AppObjectRef::Operation(op),
                AppCqeData::Recv { data },
            );

            ring.try_push_completion_entry(AppCompletionEntry::new(descriptor))
                .expect("push completion entry");

            let descriptor_round_trip = ring
                .next_completion()
                .await
                .expect("next completion")
                .descriptor()
                .expect("cqe descriptor")
                .expect("recv descriptor");
            let payload = match descriptor_round_trip.payload() {
                AppCqeData::Recv { data } => ring.read_data(data).expect("recv payload"),
                other => panic!("unexpected cqe data: {other:?}"),
            };

            (descriptor_round_trip, payload)
        });

    assert_eq!(result.0.user_data(), Some(AppUserData::new(64)));
    assert_eq!(result.0.object(), AppObjectRef::Operation(op));
    assert_eq!(result.1, b"entry-data-recv");

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn app_ring_entry_round_trips_send_data_address_for_object_view() {
    let data_runtime =
        DataRuntime::new(1, "app-ring-entry-send-test", 512 * 1024, 2).expect("data runtime");
    let ring = AppRingHandle::new(4, 4);
    let op = AppOpId::new(171);

    let result = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            let data = ring
                .alloc_data_for_bytes(b"entry-data-send")
                .expect("send data");
            let descriptor = AppSqeDescriptor::new(
                AppOpcode::Send,
                Some(AppUserData::new(65)),
                AppObjectRef::Operation(op),
                AppSqeData::Send { data },
            );

            ring.try_push_submission_entry(AppSubmissionEntry::new(descriptor))
                .expect("push submission entry");

            let sqe = ring.next_submission().await.expect("next submission");
            let descriptor_round_trip = sqe.descriptor().expect("sqe descriptor");
            let send = sqe.into_send().expect("send sqe");
            let payload = send.copy_current().expect("send payload");
            send.release();

            (descriptor_round_trip, payload)
        });

    assert_eq!(result.0.user_data(), Some(AppUserData::new(65)));
    assert_eq!(result.0.object(), AppObjectRef::Operation(op));
    assert_eq!(result.1, b"entry-data-send");

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn app_recv_cqe_descriptor_uses_result_flags_and_data_address() {
    let data_runtime =
        DataRuntime::new(1, "app-cqe-descriptor-test", 512 * 1024, 2).expect("data runtime");
    let app = AppContext::with_ring_capacity(data_runtime.context(), 2);
    let op = AppOpId::new(41);

    let (descriptor, payload) = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            app.spawn_on_op(op, 0, move |worker| async move {
                let app_runtime = worker.runtime();
                let recv_future = app_runtime.recv();
                let _recv_sqe = app_runtime
                    .next_submission_descriptor()
                    .await
                    .expect("recv sqe descriptor");
                let runtime = with_data_plane_buffers(Clone::clone);
                let index = runtime
                    .alloc_index_with_bytes(Default::default(), b"descriptor-cqe")
                    .expect("alloc app recv buffer");
                app_runtime
                    .complete_recv_buffer(runtime, index)
                    .await
                    .expect("complete recv");
                let recv = recv_future.await.expect("recv");
                let descriptor = AppCqe::recv(Some(AppUserData::new(77)), op, recv, true)
                    .descriptor()
                    .expect("descriptor conversion")
                    .expect("recv descriptor");
                let payload = match descriptor.payload() {
                    AppCqeData::Recv { data } => app_runtime
                        .read_data(data)
                        .expect("descriptor data remains owned by cqe"),
                    other => panic!("unexpected cqe data: {other:?}"),
                };
                (descriptor, payload)
            })
            .await
            .expect("spawn app op task")
        });

    assert_eq!(descriptor.user_data(), Some(AppUserData::new(77)));
    assert_eq!(descriptor.result(), "descriptor-cqe".len() as i32);
    assert_eq!(descriptor.object(), AppObjectRef::Operation(op));
    assert!(descriptor.flags().contains(AppCqeFlags::BUFFER));
    assert!(descriptor.flags().contains(AppCqeFlags::FIN));
    match descriptor.payload() {
        AppCqeData::Recv { data } => {
            assert_eq!(data.len(), "descriptor-cqe".len());
            assert_eq!(data.offset() % CACHE_LINE, 0);
        }
        other => panic!("unexpected cqe data: {other:?}"),
    }
    assert_eq!(payload, b"descriptor-cqe");

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn app_ring_copies_recv_into_app_data_and_keeps_stable_op_owner() {
    let data_runtime = DataRuntime::new(2, "app-ring-test", 512 * 1024, 2).expect("data runtime");
    let app = AppContext::with_ring_capacity(data_runtime.context(), 4);
    let op = AppOpId::new(7);

    let first = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            app.spawn_on_op(op, 1, move |worker| async move {
                let owner = worker.owner_worker();
                let thread = std::thread::current()
                    .name()
                    .map(ToOwned::to_owned)
                    .unwrap_or_default();
                let app_runtime = worker.runtime();
                let recv_future = app_runtime.recv();
                let recv_sqe = app_runtime
                    .next_submission_descriptor()
                    .await
                    .expect("recv sqe descriptor");
                let runtime = with_data_plane_buffers(Clone::clone);
                let before_in_use = with_data_plane_buffers(|runtime| runtime.in_use_buffers());
                let index = runtime
                    .alloc_index_with_bytes(Default::default(), b"ring-app-data-copy")
                    .expect("alloc app recv buffer");

                app_runtime
                    .complete_recv_buffer(runtime.clone(), index)
                    .await
                    .expect("complete recv cqe");

                assert_eq!(recv_sqe.opcode(), AppOpcode::Recv);
                let recv = recv_future.await.expect("recv cqe");
                let recv_payload = recv.copy_current().expect("recv payload");

                app_runtime.send(recv.into_send()).await.expect("send sqe");

                let send = app_runtime.next_send().await.expect("send sqe");
                let send_payload = send.copy_current().expect("send payload");
                drop(send);
                let after_in_use = with_data_plane_buffers(|runtime| runtime.in_use_buffers());

                (
                    owner,
                    thread,
                    recv_payload,
                    send_payload,
                    before_in_use,
                    after_in_use,
                )
            })
            .await
            .expect("spawn app op task")
        });

    let second = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            app.spawn_on_op(op, 1, move |worker| async move {
                (
                    worker.owner_worker(),
                    std::thread::current()
                        .name()
                        .map(ToOwned::to_owned)
                        .unwrap_or_default(),
                )
            })
            .await
            .expect("spawn app op task")
        });

    assert_eq!(first.0, second.0);
    assert_eq!(first.1, second.1);
    assert_eq!(first.2, b"ring-app-data-copy");
    assert_eq!(first.3, b"ring-app-data-copy");
    assert_eq!(first.4, 0);
    assert_eq!(first.5, 0);

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn app_recv_drop_releases_app_data_chunk() {
    let data_runtime =
        DataRuntime::new(1, "app-ring-drop-test", 512 * 1024, 2).expect("data runtime");
    let app = AppContext::with_ring_capacity(data_runtime.context(), 2);
    let op = AppOpId::new(19);

    let result = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            app.spawn_on_op(op, 0, move |worker| async move {
                let app_runtime = worker.runtime();
                let recv_future = app_runtime.recv();
                let recv_sqe = app_runtime
                    .next_submission_descriptor()
                    .await
                    .expect("recv sqe descriptor");
                let runtime = with_data_plane_buffers(Clone::clone);
                let before_in_use = with_data_plane_buffers(|runtime| runtime.in_use_buffers());
                let index = runtime
                    .alloc_index_with_bytes(Default::default(), b"drop-recv")
                    .expect("alloc app recv buffer");

                app_runtime
                    .complete_recv_buffer(runtime.clone(), index)
                    .await
                    .expect("complete recv cqe");

                assert_eq!(recv_sqe.opcode(), AppOpcode::Recv);
                let recv = recv_future.await.expect("recv cqe");
                let payload = recv.copy_current().expect("recv payload");
                drop(recv);
                let after_in_use = with_data_plane_buffers(|runtime| runtime.in_use_buffers());

                (payload, before_in_use, after_in_use)
            })
            .await
            .expect("spawn app op task")
        });

    assert_eq!(result.0, b"drop-recv");
    assert_eq!(result.1, 0);
    assert_eq!(result.2, 0);

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn same_op_reuses_runtime_ring_across_spawn_calls() {
    let data_runtime =
        DataRuntime::new(1, "app-ring-runtime-test", 512 * 1024, 2).expect("data runtime");
    let app = AppContext::with_ring_capacity(data_runtime.context(), 2);
    let op = AppOpId::new(29);

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            app.spawn_on_op(op, 0, move |worker| async move {
                let app_runtime = worker.runtime();
                let recv_future = app_runtime.recv();
                let recv_sqe = app_runtime
                    .next_submission_descriptor()
                    .await
                    .expect("recv sqe descriptor");
                let runtime = with_data_plane_buffers(Clone::clone);
                let index = runtime
                    .alloc_index_with_bytes(Default::default(), b"persisted-runtime")
                    .expect("alloc app recv buffer");

                app_runtime
                    .complete_recv_buffer(runtime.clone(), index)
                    .await
                    .expect("complete recv cqe");
                assert_eq!(recv_sqe.opcode(), AppOpcode::Recv);
                recv_future.await.expect("recv cqe").release();
            })
            .await
            .expect("prime app op runtime ring");

            app.spawn_on_op(op, 0, move |worker| async move {
                let app_runtime = worker.runtime();
                let recv_future = app_runtime.recv();
                let recv_sqe = app_runtime
                    .next_submission_descriptor()
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
            .expect("reuse app op runtime ring")
        });

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn app_runtime_exposes_descriptor_first_submission_and_completion_paths() {
    let data_runtime = DataRuntime::new(1, "app-runtime-descriptor-path-test", 512 * 1024, 2)
        .expect("data runtime");
    let app = AppContext::with_ring_capacity(data_runtime.context(), 2);
    let op = AppOpId::new(97);
    let pushed_submission = AppSqeDescriptor::new(
        AppOpcode::Close,
        Some(AppUserData::new(61)),
        AppObjectRef::Operation(op),
        AppSqeData::Close,
    );
    let queued_submission = AppSqeDescriptor::new(
        AppOpcode::Nop,
        Some(AppUserData::new(62)),
        AppObjectRef::None,
        AppSqeData::Nop,
    );
    let queued_completion = AppCqeDescriptor::new(
        Some(AppUserData::new(63)),
        0,
        AppCqeFlags::NONE,
        AppObjectRef::Operation(op),
        AppCqeData::Closed,
    );

    let round_trip = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            app.spawn_on_op(op, 0, move |worker| async move {
                let runtime = worker.runtime();

                runtime
                    .try_push_submission_descriptor(pushed_submission)
                    .expect("push runtime submission descriptor");
                let pushed_round_trip = runtime
                    .next_submission_descriptor()
                    .await
                    .expect("runtime next sqe descriptor");

                runtime
                    .try_push_submission_descriptor(queued_submission)
                    .expect("queue runtime submission descriptor");
                let queued_submission_round_trip = runtime
                    .next_submission_descriptor()
                    .await
                    .expect("runtime next submission descriptor");

                runtime
                    .try_push_completion_descriptor(queued_completion)
                    .expect("queue runtime completion descriptor");
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
            .expect("spawn app op task")
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
    let op = AppOpId::new(99);

    let round_trip = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async {
            app.spawn_on_op(op, 0, move |worker| async move {
                let runtime = worker.runtime();
                let send = runtime
                    .send_from_bytes(b"runtime-entry-send")
                    .expect("send data");
                let data = send.into_data_addr().expect("send data address");
                let descriptor = AppSqeDescriptor::new(
                    AppOpcode::Send,
                    Some(AppUserData::new(64)),
                    AppObjectRef::Operation(op),
                    AppSqeData::Send { data },
                );

                runtime
                    .try_push_submission_entry(AppSubmissionEntry::new(descriptor))
                    .expect("push runtime submission entry");

                let sqe = runtime
                    .next_submission_entry()
                    .await
                    .expect("runtime next submission entry");
                let (descriptor_round_trip, registered) = sqe.into_parts();
                assert!(registered.is_none());
                let payload = match descriptor_round_trip.payload() {
                    AppSqeData::Send { data } => {
                        runtime.read_data(data).expect("send payload from app data")
                    }
                    other => panic!("unexpected payload: {other:?}"),
                };

                (descriptor_round_trip, payload)
            })
            .await
            .expect("spawn app op task")
        });

    assert_eq!(round_trip.0.user_data(), Some(AppUserData::new(64)));
    assert_eq!(round_trip.0.object(), AppObjectRef::Operation(op));
    match round_trip.0.payload() {
        AppSqeData::Send { data } => {
            assert_eq!(data.len(), "runtime-entry-send".len());
            assert_eq!(data.offset() % CACHE_LINE, 0);
        }
        other => panic!("unexpected sqe data: {other:?}"),
    }
    assert_eq!(round_trip.1, b"runtime-entry-send");

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn non_owner_worker_does_not_own_foreign_op_runtime() {
    let data_runtime =
        DataRuntime::new(2, "app-ring-owner-test", 512 * 1024, 2).expect("data runtime");
    let app = AppContext::with_ring_capacity(data_runtime.context(), 2);
    let op = AppOpId::new(1);
    let owner = app.owner_worker_for_op(op).expect("owner worker");
    let non_owner = (owner + 1) % app.worker_count();

    let result = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime")
        .block_on(async move {
            let probe = app.clone();
            app.spawn_on_op(AppOpId::new(100), non_owner, move |_| async move {
                probe.current_worker_owns_op(op)
            })
            .await
            .expect("non-owner worker task")
        });

    assert!(
        !result,
        "worker {non_owner} must not own worker {owner}'s op"
    );

    data_runtime.shutdown_timeout(Duration::from_secs(1));
}
