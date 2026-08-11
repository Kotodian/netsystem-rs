//! MQ protocol tests: hammer-vcl against a real Session control queue pair.
//!
//! No closure replaces the dispatcher and no CONNECTED / ACCEPTED message is
//! fabricated through an injected handler: the daemon side owns the real
//! reply `SessionProducer` over a real shared control segment, consumes the
//! client's real requests, and delivers established Sessions in the
//! production attach descriptor format. Blocking flows wait on the real
//! queue signal pair; nonblocking flows drain the real reply queue through
//! `session_poll`.

mod common;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use common::{PublishedSession, TestControlPair, control_pair, publish_session};
use hammer_infra::fifo::Fifo;
use hammer_runtime::app::{
    AppSessionConfig, SessionAcceptedMsg, SessionAcceptedReplyMsg, SessionBoundMsg,
    SessionConnectError, SessionConnectMsg, SessionConnectedMsg, SessionFlags, SessionHandle,
    SessionListenMsg, TransportProtocol,
};
use hammer_runtime::{DataWorkerId, SessionListenEndpoint};
use hammer_vcl::{
    VclDirection, VclError, VclEvent, VclInitiator, VclSessionHandle, VclSessionState, VclWorker,
};

const LOCAL: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 4433);
const REMOTE: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 4433);

/// Full MQ listen + accept: the daemon consumes the real SessionListenMsg,
/// replies BOUND on the real reply queue, publishes the accepted Session's
/// descriptors, and sends ACCEPTED; the worker allocates the peer child,
/// attaches the descriptors, and replies ACCEPTED_REPLY on the real request
/// queue.
fn listen_and_accept(
    worker: &mut VclWorker,
    pair: &mut TestControlPair,
) -> (VclSessionHandle, VclSessionHandle, SessionHandle) {
    let listener_wire = SessionHandle::new(101, 0);
    pair.enqueue(&SessionBoundMsg {
        context: 1,
        result: Ok(listener_wire),
        local: Some(LOCAL),
        opaque: None,
    });
    let listener = worker
        .session_listen(
            TransportProtocol::Quic,
            SessionListenEndpoint::new(LOCAL, DataWorkerId::new(0)),
            None,
        )
        .expect("listen");
    let request = pair.dequeue::<SessionListenMsg>();
    assert_eq!(request.context, 1);
    assert_eq!(request.transport, TransportProtocol::Quic);
    assert_eq!(request.application, pair.application);

    let peer_wire = SessionHandle::new(5, 0);
    publish_session(&pair.stream, peer_wire, AppSessionConfig::new(128, 16));
    pair.enqueue(&SessionAcceptedMsg::new(
        2,
        listener_wire,
        peer_wire,
        SessionFlags::STREAM | SessionFlags::UNIDIRECTIONAL,
    ));
    let events = worker.session_poll().expect("poll accepted");
    let VclEvent::Accepted { session, parent } = events[0] else {
        panic!("expected one Accepted event, got {events:?}");
    };
    assert_eq!(parent, listener);
    assert_ne!(session, listener);
    let reply = pair.dequeue::<SessionAcceptedReplyMsg>();
    assert_eq!(reply.session, peer_wire);
    assert!(reply.result.is_ok());
    (listener, session, peer_wire)
}

#[test]
fn accept_consumes_real_accepted_and_replies() {
    let (client, mut pair) = control_pair();
    let mut worker = VclWorker::with_client(client, 8).expect("worker");
    let (listener, peer, _peer_wire) = listen_and_accept(&mut worker, &mut pair);

    assert_eq!(
        worker.session_state(listener).expect("listener state"),
        VclSessionState::Listen
    );
    assert_eq!(
        worker.session_state(peer).expect("peer state"),
        VclSessionState::Ready
    );
    let attributes = worker.session_attributes(peer).expect("peer attributes");
    assert!(attributes.stream);
    assert!(attributes.unidirectional);
    assert_eq!(attributes.initiator, VclInitiator::Peer);
    // Peer-initiated unidirectional: readable, not writable.
    assert!(attributes.readable());
    assert!(!attributes.writable());

    // A stale ACCEPTED for an unknown listener drops without allocation,
    // callback, or state mutation.
    pair.enqueue(&SessionAcceptedMsg::new(
        3,
        SessionHandle::new(999, 0),
        SessionHandle::new(6, 0),
        SessionFlags::STREAM,
    ));
    assert!(worker.session_poll().expect("poll stale").is_empty());
    assert_eq!(
        worker.session_state(peer).expect("peer state unchanged"),
        VclSessionState::Ready
    );
}

/// Full nonblocking stream connect over the real queue pair: the child
/// leaves `session_stream_connect` in Connecting, the daemon consumes the
/// real CONNECT_STREAM request (context, parent wire handle, flags), and
/// the real CONNECTED reply with the published descriptors transitions it
/// to Ready preserving the established-session flags.
#[test]
fn nonblocking_stream_connect_consumes_real_connected() {
    let (client, mut pair) = control_pair();
    let mut worker = VclWorker::with_client(client, 8).expect("worker");
    let (_listener, parent, parent_wire) = listen_and_accept(&mut worker, &mut pair);

    let child = worker
        .session_create(TransportProtocol::Quic, true)
        .expect("create");
    worker
        .session_stream_connect(child, parent, REMOTE, None, SessionFlags::empty())
        .expect("nonblocking connect returns immediately");
    assert_eq!(
        worker.session_state(child).expect("child state"),
        VclSessionState::Connecting
    );

    let request = pair.dequeue::<SessionConnectMsg>();
    assert_eq!(request.context, child.raw());
    assert_eq!(request.parent_handle, Some(parent_wire));
    assert!(request.flags.contains(SessionFlags::STREAM));
    assert_eq!(request.transport, TransportProtocol::Quic);
    assert_eq!(request.remote, REMOTE);

    let child_wire = SessionHandle::new(9, 0);
    let published = publish_session(&pair.stream, child_wire, AppSessionConfig::new(128, 16));
    pair.enqueue(&SessionConnectedMsg {
        context: child.raw(),
        result: Ok(child_wire),
        local: Some(LOCAL),
        remote: Some(REMOTE),
        flags: SessionFlags::empty(),
        opaque: None,
    });
    let events = worker.session_poll().expect("poll connected");
    assert_eq!(events, vec![VclEvent::Connected { session: child }]);
    assert_eq!(
        worker.session_state(child).expect("child state"),
        VclSessionState::Ready
    );
    let attributes = worker.session_attributes(child).expect("child attributes");
    assert!(attributes.stream);
    assert!(!attributes.unidirectional);
    assert_eq!(attributes.initiator, VclInitiator::Local);
    assert!(attributes.readable());
    assert!(attributes.writable());

    // The descriptor-delivered AppSession is functional: the daemon writes
    // into the published rx FIFO, the client reads it, the client sends into
    // the tx FIFO, the daemon reads it back.
    round_trip(&mut worker, child, &published);
}

/// Blocking stream connect waits on the real queue signal pair and returns
/// only once the real CONNECTED message resolved the child.
#[test]
fn blocking_stream_connect_waits_on_real_signal() {
    let (client, mut pair) = control_pair();
    let mut worker = VclWorker::with_client(client, 8).expect("worker");
    let (_listener, parent, _parent_wire) = listen_and_accept(&mut worker, &mut pair);

    let child = worker
        .session_create(TransportProtocol::Quic, false)
        .expect("create");
    publish_session(
        &pair.stream,
        SessionHandle::new(9, 0),
        AppSessionConfig::new(128, 16),
    );
    pair.enqueue(&SessionConnectedMsg {
        context: child.raw(),
        result: Ok(SessionHandle::new(9, 0)),
        local: None,
        remote: None,
        flags: SessionFlags::empty(),
        opaque: None,
    });
    worker
        .session_stream_connect(child, parent, REMOTE, None, SessionFlags::empty())
        .expect("blocking connect completes");
    assert_eq!(
        worker.session_state(child).expect("child state"),
        VclSessionState::Ready
    );
}

/// A failed CONNECTED reply detaches the child retaining the typed
/// `SessionConnectError`; no descriptors are delivered and no Session is
/// attached.
#[test]
fn blocking_connect_failure_detaches_with_typed_error() {
    let (client, mut pair) = control_pair();
    let mut worker = VclWorker::with_client(client, 8).expect("worker");
    let (_listener, parent, _parent_wire) = listen_and_accept(&mut worker, &mut pair);

    let child = worker
        .session_create(TransportProtocol::Quic, false)
        .expect("create");
    pair.enqueue(&SessionConnectedMsg::new(
        child.raw(),
        Err(SessionConnectError::TimedOut),
    ));
    let error = worker
        .session_stream_connect(child, parent, REMOTE, None, SessionFlags::empty())
        .expect_err("blocking connect must fail");
    assert!(
        matches!(error, VclError::ConnectFailed { session, error: SessionConnectError::TimedOut } if session == child),
        "expected ConnectFailed(TimedOut), got {error:?}"
    );
    assert_eq!(
        worker.session_state(child).expect("child state"),
        VclSessionState::Detached
    );
    assert!(worker.session_attributes(child).expect("attributes").stream);
}

/// A CONNECTED message whose context resolves to no local Session (or to a
/// Session that is not Connecting) drops without allocation, callback, or
/// state mutation.
#[test]
fn stale_connected_drops_without_state_mutation() {
    let (client, mut pair) = control_pair();
    let mut worker = VclWorker::with_client(client, 8).expect("worker");
    let (_listener, parent, _parent_wire) = listen_and_accept(&mut worker, &mut pair);

    // Context that never resolved to an allocated local Session.
    pair.enqueue(&SessionConnectedMsg::new(
        VclSessionHandle::new(200, 200).raw(),
        Ok(SessionHandle::new(9, 0)),
    ));
    // Context that resolves to the READY peer, which is not Connecting.
    pair.enqueue(&SessionConnectedMsg::new(
        parent.raw(),
        Ok(SessionHandle::new(9, 0)),
    ));
    assert!(worker.session_poll().expect("poll stale").is_empty());
    assert_eq!(
        worker.session_state(parent).expect("parent unchanged"),
        VclSessionState::Ready
    );
    assert_eq!(
        worker
            .session_attributes(parent)
            .expect("parent attributes")
            .initiator,
        VclInitiator::Peer
    );
}

/// A locally initiated unidirectional Session is writable and not readable;
/// a read is a typed direction error and mutates nothing.
#[test]
fn local_uni_child_is_write_only() {
    let (client, mut pair) = control_pair();
    let mut worker = VclWorker::with_client(client, 8).expect("worker");
    let (_listener, parent, _parent_wire) = listen_and_accept(&mut worker, &mut pair);

    let child = worker
        .session_create(TransportProtocol::Quic, true)
        .expect("create");
    worker
        .session_stream_connect(child, parent, REMOTE, None, SessionFlags::UNIDIRECTIONAL)
        .expect("nonblocking connect returns immediately");
    let request = pair.dequeue::<SessionConnectMsg>();
    assert!(request.flags.contains(SessionFlags::UNIDIRECTIONAL));

    publish_session(
        &pair.stream,
        SessionHandle::new(9, 0),
        AppSessionConfig::new(128, 16),
    );
    pair.enqueue(&SessionConnectedMsg {
        context: child.raw(),
        result: Ok(SessionHandle::new(9, 0)),
        local: None,
        remote: None,
        flags: SessionFlags::STREAM | SessionFlags::UNIDIRECTIONAL,
        opaque: None,
    });
    assert_eq!(
        worker.session_poll().expect("poll connected"),
        vec![VclEvent::Connected { session: child }]
    );
    let attributes = worker.session_attributes(child).expect("child attributes");
    assert!(attributes.stream);
    assert!(attributes.unidirectional);
    assert_eq!(attributes.initiator, VclInitiator::Local);
    assert!(!attributes.readable());
    assert!(attributes.writable());
    let mut out = [0_u8; 16];
    assert!(
        matches!(
            worker.session_recv(child, &mut out),
            Err(VclError::DirectionInvalid {
                session,
                direction: VclDirection::Read,
            }) if session == child
        ),
        "read on a local-uni Session must be a typed direction error"
    );
}

/// Daemon-side write into the published rx FIFO and read of the published tx
/// FIFO, exercising the descriptor-delivered AppSession data path.
fn round_trip(worker: &mut VclWorker, child: VclSessionHandle, published: &PublishedSession) {
    let rx = unsafe { Fifo::from_shared(published.segment.clone(), published.offsets.rx_fifo_off) };
    assert_eq!(rx.enqueue(b"hello"), 5);
    let mut out = [0_u8; 16];
    assert_eq!(worker.session_recv(child, &mut out).expect("recv"), 5);
    assert_eq!(&out[..5], b"hello");

    assert_eq!(worker.session_send(child, b"world").expect("send"), 5);
    let tx = unsafe { Fifo::from_shared(published.segment.clone(), published.offsets.tx_fifo_off) };
    let mut tx_out = [0_u8; 16];
    assert_eq!(tx.peek(0, tx_out.len(), &mut tx_out), 5);
    assert_eq!(&tx_out[..5], b"world");
}
