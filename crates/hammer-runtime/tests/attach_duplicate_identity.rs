//! Attach-seam behavior: when `attach_application` hands back an identity
//! already held by a live client, `AppServer::serve` must reject the
//! duplicate client over the wire and keep the first client attached --
//! mirroring VPP's `SESSION_E_APP_ATTACHED` rejection
//! (`vnet/session/application.c:1138-1139`, `session_api.c:775-781`) --
//! instead of panicking.

use std::convert::Infallible;
use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use hammer_infra::segment::Segment;
use hammer_runtime::app::{ApplicationId, SessionMsgQueue};
use hammer_runtime::attach::{
    ATTACH_PROTOCOL_VERSION, ATTACH_REPLY_BYTES, ATTACH_REPLY_WORDS, ATTACH_STATUS_ACCEPTED,
    ATTACH_STATUS_REJECTED, AppServer, ApplicationMqPublication,
};

const FIRST_APPLICATION: ApplicationId = ApplicationId::new(3);
const SECOND_APPLICATION: ApplicationId = ApplicationId::new(4);

const MQ_SEGMENT_BYTES: usize = 1024 * 1024;
const MQ_QUEUE_ITEMS: u32 = 32;
const MQ_RING_ITEMS: u32 = 16;

static NAME_COUNTER: AtomicU64 = AtomicU64::new(0);

fn socket_path() -> PathBuf {
    let counter = NAME_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "hamr-attach-{:x}-{counter:x}.sock",
        std::process::id()
    ))
}

/// One per-Application Rx MQ publication, built like the daemon's
/// `application_mq_publication`: one shared segment holding one worker queue.
/// The publication owns the segment and queue by reference, so they stay
/// alive with it.
fn build_application_mqs() -> ApplicationMqPublication {
    let counter = NAME_COUNTER.fetch_add(1, Ordering::Relaxed);
    let segment = Segment::shared(
        &format!("hamr-mq-{}-{counter}", std::process::id()),
        MQ_SEGMENT_BYTES,
    )
    .expect("Application MQ segment");
    assert_eq!(segment.size(), MQ_SEGMENT_BYTES, "MQ segment size");
    let queue_bytes =
        SessionMsgQueue::layout_bytes(MQ_QUEUE_ITEMS, MQ_RING_ITEMS).expect("queue layout");
    let offset = segment.alloc(queue_bytes, 64).expect("queue offset");
    // SAFETY: the offset was allocated with the matching layout size and
    // the queue is exclusively owned by this segment until publication.
    let queue = unsafe {
        SessionMsgQueue::init_at_with_signal(segment.clone(), offset, MQ_QUEUE_ITEMS, MQ_RING_ITEMS)
    }
    .expect("Application MQ");
    let queues = vec![Arc::new(queue)].into_boxed_slice();
    let offsets = vec![offset].into_boxed_slice();
    ApplicationMqPublication::new(
        segment.clone(),
        queues,
        offsets,
        0, // no ext-config store, like a daemon without one
    )
    .expect("Application MQ publication")
}

/// Connects an attach client and bounds the reply read so a serve-side hang
/// fails the test instead of blocking it forever.
fn connect_client(path: &PathBuf, name: &str) -> UnixStream {
    let stream =
        UnixStream::connect(path).unwrap_or_else(|error| panic!("{name} connect: {error}"));
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap_or_else(|error| panic!("{name} read timeout: {error}"));
    stream
}

/// Sends the attach request and reads the three-word reply; asserts the
/// status word equals `expected_status` and returns the application word.
fn attach_client(stream: &mut UnixStream, expected_status: u64) -> u64 {
    stream
        .write_all(&ATTACH_PROTOCOL_VERSION.to_le_bytes())
        .expect("write attach request");
    let mut reply = [0_u8; ATTACH_REPLY_BYTES];
    stream.read_exact(&mut reply).expect("read attach reply");
    let mut words = [0_u64; ATTACH_REPLY_WORDS];
    for (chunk, word) in reply.chunks_exact(size_of::<u64>()).zip(&mut words) {
        *word = u64::from_le_bytes(chunk.try_into().expect("reply word"));
    }
    assert_eq!(words[0], ATTACH_PROTOCOL_VERSION, "reply protocol version");
    assert_eq!(words[1], expected_status, "attach reply status");
    words[2]
}

/// Runs `AppServer::serve` in its own thread. The identity callback returns
/// FIRST_APPLICATION twice (so the second client collides with the first),
/// then SECOND_APPLICATION. Reports how serve ended, and counts `detached`
/// calls, so the test can prove the rejection was non-destructive.
fn spawn_serve(
    server: Arc<AppServer>,
    application_mqs: ApplicationMqPublication,
) -> (
    Arc<AtomicUsize>,
    std::sync::mpsc::Receiver<Result<(), String>>,
) {
    let detached = Arc::new(AtomicUsize::new(0));
    let detached_reports = Arc::clone(&detached);
    let identity = Arc::new(AtomicU64::new(0));
    let (outcome_tx, outcome_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("serve runtime");
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            runtime.block_on(server.serve(
                move || {
                    let step = identity.fetch_add(1, Ordering::Relaxed);
                    let application = match step {
                        0 => FIRST_APPLICATION,
                        1 => FIRST_APPLICATION, // duplicate: rejected at the attach seam
                        _ => SECOND_APPLICATION,
                    };
                    Ok::<ApplicationId, Infallible>(application)
                },
                move |_| Ok::<ApplicationMqPublication, Infallible>(application_mqs.clone()),
                |_, _, _| Ok(()),
                move |_| {
                    detached_reports.fetch_add(1, Ordering::Relaxed);
                },
            ))
        }));
        let _ = outcome_tx.send(match outcome {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(format!("serve returned: {error}")),
            Err(_) => Err("serve panicked".into()),
        });
    });
    (detached, outcome_rx)
}

#[test]
fn duplicate_attach_identity_is_rejected_without_panicking() {
    let path = socket_path();
    let server = Arc::new(
        AppServer::bind(path.to_str().expect("socket path"), 2).expect("bind attach server"),
    );
    let (detached, outcome_rx) = spawn_serve(server, build_application_mqs());

    let mut first = connect_client(&path, "first client");
    assert_eq!(
        attach_client(&mut first, ATTACH_STATUS_ACCEPTED),
        FIRST_APPLICATION.raw(),
        "first client attaches the identity"
    );

    // The duplicate client is handed the same identity: the serve loop must
    // reject it over the wire and keep the first client attached.
    let mut duplicate = connect_client(&path, "duplicate client");
    assert_eq!(
        attach_client(&mut duplicate, ATTACH_STATUS_REJECTED),
        0,
        "duplicate identity attach must be rejected without an application"
    );

    // The serve loop survived the rejection: a fresh identity still attaches.
    let mut next = connect_client(&path, "next client");
    assert_eq!(
        attach_client(&mut next, ATTACH_STATUS_ACCEPTED),
        SECOND_APPLICATION.raw(),
        "fresh identity still attaches after the rejection"
    );

    // The first live client was never torn down, and serve neither panicked
    // nor returned: the rejection was non-destructive.
    assert_eq!(
        detached.load(Ordering::Relaxed),
        0,
        "first live client must not be detached by a duplicate attach"
    );
    match outcome_rx.try_recv() {
        Ok(Ok(())) => panic!("serve returned unexpectedly"),
        Ok(Err(reason)) => panic!("serve ended unexpectedly: {reason}"),
        Err(_) => {}
    }
}
