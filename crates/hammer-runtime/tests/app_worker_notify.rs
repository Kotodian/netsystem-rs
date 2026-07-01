use hammer_infra::segment::Local;
use hammer_runtime::app::{AppSessionConfig, SessionHandle, with_current_app_worker};
use tokio::time::{Duration, timeout};

#[tokio::test(flavor = "current_thread")]
async fn local_app_worker_recv_wakes_after_rx_notify() {
    let handle = SessionHandle::new(7, 0);
    let session = with_current_app_worker(0, |worker| {
        worker
            .attach_session_local(handle, AppSessionConfig::new(64, 4))
            .expect("attach")
    });
    let worker = with_current_app_worker(0, |worker| worker.clone());
    let recv = async {
        let mut out = [0u8; 8];
        let read = worker.recv(handle, &mut out).await;
        (read, out)
    };
    let producer = async {
        tokio::task::yield_now().await;
        session.enqueue_rx(b"hi").expect("enqueue rx");
        with_current_app_worker(0, |worker| worker.wake_rx(handle));
    };

    let ((read, out), _) = timeout(Duration::from_millis(200), async {
        tokio::join!(recv, producer)
    })
    .await
    .expect("recv should wake");

    assert_eq!(read, 2);
    assert_eq!(&out[..2], b"hi");
}

#[tokio::test(flavor = "current_thread")]
async fn local_app_worker_next_event_wakes_after_event_notify() {
    let handle = SessionHandle::new(8, 0);
    let session = with_current_app_worker(0, |worker| {
        worker
            .attach_session_local(handle, AppSessionConfig::new(64, 4))
            .expect("attach")
    });
    let worker = with_current_app_worker(0, |worker| worker.clone());
    let next = async { worker.next_event(handle).await.expect("event") };
    let producer = async {
        tokio::task::yield_now().await;
        session
            .push_event(hammer_infra::msg_queue::SessionEvtType::Connect)
            .expect("push event");
        with_current_app_worker(0, |worker| worker.wake_evt(handle));
    };

    let (event, _) = timeout(Duration::from_millis(200), async {
        tokio::join!(next, producer)
    })
    .await
    .expect("next_event should wake");

    assert_eq!(event.session_index, handle.session_index());
}

#[tokio::test(flavor = "current_thread")]
async fn recv_does_not_lose_wake_under_race() {
    let handle = SessionHandle::new(9, 0);
    let session = with_current_app_worker(0, |worker| {
        worker
            .attach_session_local(handle, AppSessionConfig::new(64, 4))
            .expect("attach")
    });
    let worker = with_current_app_worker(0, |worker| worker.clone());
    let recv = async {
        let mut out = [0u8; 8];
        let read = worker.recv(handle, &mut out).await;
        (read, out)
    };
    let producer = async {
        tokio::task::yield_now().await;
        session.enqueue_rx(b"hi").expect("enqueue rx");
        with_current_app_worker(0, |worker| worker.wake_rx(handle));
        tokio::task::yield_now().await;
        session.enqueue_rx(b"!").expect("enqueue rx");
        with_current_app_worker(0, |worker| worker.wake_rx(handle));
    };

    let ((read, out), _) = timeout(Duration::from_millis(200), async {
        tokio::join!(recv, producer)
    })
    .await
    .expect("recv should wake even with early notify");

    assert!(read > 0, "must receive data despite racy notify");
}
