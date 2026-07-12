//! Seam 2: shared Local Session Message Queue concurrent producers + single drain.
//! Behavioral only — no source greps.

use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

use hammer_runtime::app::{SessionEventQueue, SessionEvt, SessionEvtType, SessionMsgQueue};

#[test]
fn shared_tx_evt_q_concurrent_enqueue_io_preserves_all_events() {
    let q: Arc<SessionMsgQueue> =
        Arc::new(SessionMsgQueue::with_cfg(512, 256).expect("queue"));
    let producers = 8usize;
    let per = 50usize;
    let total = producers * per;
    let started = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();
    for p in 0..producers {
        let q = Arc::clone(&q);
        let started = Arc::clone(&started);
        handles.push(thread::spawn(move || {
            started.fetch_add(1, Ordering::SeqCst);
            while started.load(Ordering::SeqCst) < producers {
                thread::yield_now();
            }
            for i in 0..per {
                let evt = SessionEvt::io(((p as u32) << 16) | i as u32, SessionEvtType::TxDeq);
                loop {
                    match q.enqueue_io(evt) {
                        Ok(()) => break,
                        Err(_) => thread::yield_now(),
                    }
                }
            }
        }));
    }

    let mut seen = HashSet::with_capacity(total);
    while seen.len() < total {
        if let Some(evt) = q.dequeue() {
            assert_eq!(evt.evt_type, SessionEvtType::TxDeq);
            seen.insert(evt.session_index());
        } else {
            thread::yield_now();
        }
    }
    for h in handles {
        h.join().expect("join");
    }
    assert_eq!(seen.len(), total);
    assert!(q.dequeue().is_none());
}
