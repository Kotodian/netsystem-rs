//! Behavioral tests for the Application Session control seam: fixed
//! VPP-shaped control slots over the SessionMsgQueue CTRL ring.
//!
//! Observable enqueue/dequeue only — no source greps.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use hammer_infra::segment::Segment;

use hammer_runtime::app::{
    ApplicationId, SessionAcceptedMsg, SessionAcceptedReplyMsg, SessionBoundMsg,
    SessionConnectError, SessionConnectMsg, SessionConnectedMsg, SessionControlError,
    SessionControlItem, SessionEvtType, SessionFlags, SessionHandle, SessionListenMsg,
    SessionMsgQueue, SessionProducer, SessionUnlistenMsg, SessionUnlistenReplyMsg, SingleProducer,
    TransportProtocol,
};
use hammer_runtime::attach::{EXT_CONFIG_CHUNK_BYTES, EXT_CONFIG_CHUNK_COUNT, ExtConfigStore};
use hammer_runtime::{
    AttachError, DataWorkerId, RuntimeResult, SessionConnectEndpoint, SessionListenEndpoint,
    SessionTransportRegistration,
};

fn control_queue() -> (SessionMsgQueue<SingleProducer>, SessionProducer) {
    let queue = SessionMsgQueue::<SingleProducer>::with_control_defaults().expect("control queue");
    let producer = queue.claim_producer().expect("claim producer");
    (queue, producer)
}

#[test]
fn transport_protocol_stable_values_and_names() {
    assert_eq!(TransportProtocol::Tcp as u8, 0);
    assert_eq!(TransportProtocol::Udp as u8, 1);
    assert_eq!(TransportProtocol::Ct as u8, 2);
    assert_eq!(TransportProtocol::Tls as u8, 3);
    assert_eq!(TransportProtocol::Quic as u8, 4);
    assert_eq!(TransportProtocol::Dtls as u8, 5);
    assert_eq!(TransportProtocol::Srtp as u8, 6);
    assert_eq!(TransportProtocol::Http as u8, 7);

    assert_eq!(TransportProtocol::Tcp.name(), "tcp");
    assert_eq!(TransportProtocol::Udp.name(), "udp");
    assert_eq!(TransportProtocol::Quic.name(), "quic");
    assert_eq!(TransportProtocol::Tls.name(), "tls");
    assert_eq!(TransportProtocol::Ct.name(), "ct");
    assert_eq!(TransportProtocol::Dtls.name(), "dtls");
    assert_eq!(TransportProtocol::Srtp.name(), "srtp");
    assert_eq!(TransportProtocol::Http.name(), "http");

    assert_eq!(
        TransportProtocol::try_from("tcp"),
        Ok(TransportProtocol::Tcp)
    );
    assert_eq!(
        TransportProtocol::try_from("quic"),
        Ok(TransportProtocol::Quic)
    );
    assert_eq!(
        TransportProtocol::try_from("http"),
        Ok(TransportProtocol::Http)
    );
    assert!(TransportProtocol::try_from("sctp").is_err());
    assert_eq!(
        TransportProtocol::try_from(4_u8),
        Ok(TransportProtocol::Quic)
    );
    assert_eq!(
        TransportProtocol::try_from(7_u8),
        Ok(TransportProtocol::Http)
    );
    assert!(TransportProtocol::try_from(8_u8).is_err());
}

#[test]
fn session_flags_bitflags_preserved() {
    assert_eq!(SessionFlags::STREAM.bits(), 0x0001);
    assert_eq!(SessionFlags::UNIDIRECTIONAL.bits(), 0x0002);
    let flags = SessionFlags::STREAM | SessionFlags::UNIDIRECTIONAL;
    assert!(flags.contains(SessionFlags::STREAM));
    assert!(flags.contains(SessionFlags::UNIDIRECTIONAL));
    assert!(SessionFlags::empty().is_empty());
}

#[test]
fn session_connect_error_is_shared_from_runtime_app() {
    let error = SessionConnectError::QuicTransportError { code: 0x101 };

    assert_eq!(error.to_string(), "QUIC transport error 257");
}

#[test]
fn transport_registration_exposes_stream_connect_vft() {
    fn connect_stream(_endpoint: SessionConnectEndpoint) -> RuntimeResult<()> {
        Ok(())
    }

    let registration = SessionTransportRegistration::with_connect_stream(
        "test-session",
        None,
        None,
        None,
        Some(connect_stream),
    );

    assert!(registration.connect_stream().is_some());
}

#[test]
fn connect_round_trips_through_control_queue() {
    let (mut queue, mut producer) = control_queue();
    let message = SessionConnectMsg::connect(
        41,
        TransportProtocol::Quic,
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 4433),
        Some(SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 0)),
        ApplicationId::new(9, 2),
        Some(77),
    );
    producer.enqueue_control(&message).expect("enqueue");

    let item = queue.dequeue_control().expect("dequeue").expect("item");
    assert_eq!(item.event_type(), SessionEvtType::Connect);
    assert_eq!(item.decode::<SessionConnectMsg>(), Some(Ok(message)));
    // The borrowed slot must be released before the next dequeue (one
    // outstanding borrowed slot per queue).
    drop(item);
    assert!(queue.dequeue_control().expect("dequeue").is_none());
}

#[test]
fn connect_stream_selects_connect_stream_event_and_pins_parent() {
    let (mut queue, mut producer) = control_queue();
    let parent = SessionHandle::new(17, 3);
    let message = SessionConnectMsg::connect_stream(
        41,
        TransportProtocol::Quic,
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 4433),
        None,
        ApplicationId::new(9, 2),
        parent,
        SessionFlags::UNIDIRECTIONAL,
        Some(77),
    );

    assert_eq!(message.parent_handle, Some(parent));
    assert!(message.flags.contains(SessionFlags::STREAM));
    assert!(message.flags.contains(SessionFlags::UNIDIRECTIONAL));

    producer.enqueue_control(&message).expect("enqueue");
    let item = queue.dequeue_control().expect("dequeue").expect("item");
    assert_eq!(item.event_type(), SessionEvtType::ConnectStream);
    assert_eq!(item.decode::<SessionConnectMsg>(), Some(Ok(message)));
}

#[test]
fn ordinary_connect_carries_no_parent_handle() {
    let message = SessionConnectMsg::connect(
        41,
        TransportProtocol::Tcp,
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
        None,
        ApplicationId::new(9, 2),
        None,
    );

    assert_eq!(message.parent_handle, None);
    assert!(message.flags.is_empty());
}

#[test]
fn connected_round_trip_preserves_typed_failure() {
    let (mut queue, mut producer) = control_queue();
    let error = SessionConnectError::TlsAlert { alert: 42 };
    let message = SessionConnectedMsg::new(41, Err(error));
    producer.enqueue_control(&message).expect("enqueue");

    let item = queue.dequeue_control().expect("dequeue").expect("item");
    assert_eq!(item.event_type(), SessionEvtType::Connected);
    assert_eq!(item.decode::<SessionConnectedMsg>(), Some(Ok(message)));
}

#[test]
fn connected_round_trip_preserves_full_width_error_payload() {
    let (mut queue, mut producer) = control_queue();
    // Codes wider than 16 bits must round-trip exactly: the wire carries the
    // full payload, the retval keeps only a stable tag.
    let quic = SessionConnectedMsg::new(
        41,
        Err(SessionConnectError::QuicTransportError { code: 0x12345 }),
    );
    producer.enqueue_control(&quic).expect("enqueue");
    let item = queue.dequeue_control().expect("dequeue").expect("item");
    assert_eq!(item.decode::<SessionConnectedMsg>(), Some(Ok(quic)));
    drop(item);

    let peer_closed = SessionConnectedMsg::new(
        41,
        Err(SessionConnectError::PeerClosed { code: 0x1234567890 }),
    );
    producer.enqueue_control(&peer_closed).expect("enqueue");
    let item = queue.dequeue_control().expect("dequeue").expect("item");
    assert_eq!(item.decode::<SessionConnectedMsg>(), Some(Ok(peer_closed)));
}

#[test]
fn connected_round_trip_preserves_connect_control_errors() {
    // Specific CONNECT/CONNECT_STREAM failures must survive the control
    // boundary as their own wire variants, never as the opaque
    // TransportFailed collapse (issue #222).
    let (mut queue, mut producer) = control_queue();
    for error in [
        SessionControlError::ConnectStreamParentMissing,
        SessionControlError::ConnectStreamWrongWorker,
        SessionControlError::NoDataWorkers,
        SessionControlError::ConnectionMissing,
        SessionControlError::ConnectionNotOwned,
    ] {
        let message = SessionConnectedMsg::new(41, Err(SessionConnectError::Control { error }));
        producer.enqueue_control(&message).expect("enqueue");
        let item = queue.dequeue_control().expect("dequeue").expect("item");
        assert_eq!(item.decode::<SessionConnectedMsg>(), Some(Ok(message)));
        drop(item);
    }
}

#[test]
fn connected_round_trip_preserves_handle_and_endpoints() {
    let (mut queue, mut producer) = control_queue();
    let handle = SessionHandle::new(17, 3);
    let message = SessionConnectedMsg {
        context: 41,
        result: Ok(handle),
        local: Some(SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 9000)),
        remote: Some(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
            4433,
        )),
        flags: SessionFlags::UNIDIRECTIONAL,
        opaque: Some(77),
    };
    producer.enqueue_control(&message).expect("enqueue");

    let item = queue.dequeue_control().expect("dequeue").expect("item");
    assert_eq!(item.decode::<SessionConnectedMsg>(), Some(Ok(message)));
}

#[test]
fn accepted_round_trip_preserves_listener_child_and_flags() {
    let (mut queue, mut producer) = control_queue();
    let listener = SessionHandle::new(17, 3);
    let child = SessionHandle::new(18, 4);
    let message = SessionAcceptedMsg {
        context: 42,
        listener,
        session: child,
        flags: SessionFlags::STREAM | SessionFlags::UNIDIRECTIONAL,
        local: Some(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9000)),
        remote: Some(SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 51234)),
        opaque: Some(9),
    };
    producer.enqueue_control(&message).expect("enqueue");

    let item = queue.dequeue_control().expect("dequeue").expect("item");
    assert_eq!(item.event_type(), SessionEvtType::Accepted);
    assert_eq!(item.decode::<SessionAcceptedMsg>(), Some(Ok(message)));
}

#[test]
fn listen_round_trip_preserves_endpoint_transport_and_flags() {
    let (mut queue, mut producer) = control_queue();
    let endpoint = SessionListenEndpoint::new(
        SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 4433),
        DataWorkerId::new(3),
    );
    let message = SessionListenMsg {
        context: 51,
        transport: TransportProtocol::Quic,
        endpoint,
        application: ApplicationId::new(9, 2),
        app: None,
        flags: SessionFlags::UNIDIRECTIONAL,
        opaque: Some(9),
    };
    producer.enqueue_control(&message).expect("enqueue");

    let item = queue.dequeue_control().expect("dequeue").expect("item");
    assert_eq!(item.event_type(), SessionEvtType::Listen);
    assert_eq!(item.decode::<SessionListenMsg>(), Some(Ok(message)));
}

#[test]
fn unlisten_round_trip() {
    let (mut queue, mut producer) = control_queue();
    let message = SessionUnlistenMsg {
        context: 52,
        listener: SessionHandle::new(17, 3),
    };
    producer.enqueue_control(&message).expect("enqueue");

    let item = queue.dequeue_control().expect("dequeue").expect("item");
    assert_eq!(item.event_type(), SessionEvtType::Unlisten);
    assert_eq!(item.decode::<SessionUnlistenMsg>(), Some(Ok(message)));
}

#[test]
fn bound_and_replies_preserve_typed_control_errors() {
    let (mut queue, mut producer) = control_queue();

    let bound = SessionBoundMsg {
        context: 61,
        result: Err(SessionControlError::TransportMissing),
        local: Some(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9000)),
        opaque: Some(1),
    };
    producer.enqueue_control(&bound).expect("enqueue");
    let item = queue.dequeue_control().expect("dequeue").expect("item");
    assert_eq!(item.event_type(), SessionEvtType::Bound);
    assert_eq!(item.decode::<SessionBoundMsg>(), Some(Ok(bound)));
    drop(item);

    let unlisten_reply = SessionUnlistenReplyMsg {
        context: 62,
        listener: SessionHandle::new(17, 3),
        result: Err(SessionControlError::ListenerNotOwned),
    };
    producer.enqueue_control(&unlisten_reply).expect("enqueue");
    let item = queue.dequeue_control().expect("dequeue").expect("item");
    assert_eq!(item.event_type(), SessionEvtType::UnlistenReply);
    assert_eq!(
        item.decode::<SessionUnlistenReplyMsg>(),
        Some(Ok(unlisten_reply))
    );
    drop(item);

    let accepted_reply = SessionAcceptedReplyMsg::new(63, SessionHandle::new(18, 4), Ok(()));
    producer.enqueue_control(&accepted_reply).expect("enqueue");
    let item = queue.dequeue_control().expect("dequeue").expect("item");
    assert_eq!(item.event_type(), SessionEvtType::AcceptedReply);
    assert_eq!(
        item.decode::<SessionAcceptedReplyMsg>(),
        Some(Ok(accepted_reply))
    );
}

#[test]
fn dequeue_control_empty_queue_returns_none() {
    let (mut queue, _) = control_queue();
    assert!(queue.dequeue_control().expect("dequeue").is_none());
}

#[test]
fn decode_with_mismatched_event_type_returns_none() {
    let (mut queue, mut producer) = control_queue();
    let message = SessionConnectedMsg::new(41, Ok(SessionHandle::new(17, 3)));
    producer.enqueue_control(&message).expect("enqueue");

    let item: SessionControlItem<'_> = queue.dequeue_control().expect("dequeue").expect("item");
    assert_eq!(item.event_type(), SessionEvtType::Connected);
    assert_eq!(item.decode::<SessionConnectMsg>(), None);
    assert!(item.decode::<SessionConnectedMsg>().is_some());
}

#[test]
fn ext_config_store_bounded_ownership_round_trips() {
    let seg = Segment::shared_default();
    let region = seg
        .alloc(ExtConfigStore::layout_bytes(), 64)
        .expect("region");
    let store = unsafe { ExtConfigStore::init_at(seg, region as usize) };

    let config_offset = store.alloc(b"example.test").expect("alloc");
    assert_eq!(
        store.read(config_offset).expect("read"),
        &b"example.test"[..]
    );
    store.free(config_offset).expect("free");

    // A freed chunk is immediately reusable: bounded fixed ownership, no growth.
    let again = store.alloc(b"example.org").expect("reuse");
    assert_eq!(store.read(again).expect("read"), &b"example.org"[..]);
    store.free(again).expect("free");

    let empty = store.alloc(&[]).expect("empty config");
    assert_eq!(store.read(empty).expect("read"), &[][..] as &[u8]);
    store.free(empty).expect("free");

    let oversized = [0_u8; EXT_CONFIG_CHUNK_BYTES + 1];
    assert!(store.alloc(&oversized).is_err());

    assert!(store.read(u64::MAX).is_err());
    assert!(store.free(u64::MAX).is_err());

    // All chunks exhausted -> bounded failure.
    let mut offsets = Vec::new();
    for _ in 0..EXT_CONFIG_CHUNK_COUNT {
        offsets.push(store.alloc(b"x").expect("fill"));
    }
    assert!(store.alloc(b"x").is_err());
    for offset in offsets {
        store.free(offset).expect("free");
    }
}

#[test]
fn ext_config_store_rejects_double_free_and_read_after_free() {
    let seg = Segment::shared_default();
    let region = seg
        .alloc(ExtConfigStore::layout_bytes(), 64)
        .expect("region");
    let store = unsafe { ExtConfigStore::init_at(seg, region as usize) };

    let offset = store.alloc(b"owned").expect("alloc");
    assert_eq!(store.read(offset).expect("read"), &b"owned"[..]);

    store.free(offset).expect("free");

    // Read after free must fail instead of returning payload bytes.
    assert!(matches!(
        store.read(offset),
        Err(AttachError::ExtConfigNotAllocated)
    ));

    // Double free must be detected instead of linking the chunk twice.
    assert!(matches!(
        store.free(offset),
        Err(AttachError::ExtConfigNotAllocated)
    ));
}

#[test]
fn ext_config_store_double_free_never_aliases_an_allocation() {
    let seg = Segment::shared_default();
    let region = seg
        .alloc(ExtConfigStore::layout_bytes(), 64)
        .expect("region");
    let store = unsafe { ExtConfigStore::init_at(seg, region as usize) };

    // A double free was rejected; the chunk must appear in the free list at
    // most once, so filling the store yields no duplicate offsets.
    let offset = store.alloc(b"a").expect("alloc");
    store.free(offset).expect("free");
    assert!(store.free(offset).is_err());

    let mut offsets = Vec::new();
    for _ in 0..EXT_CONFIG_CHUNK_COUNT {
        offsets.push(store.alloc(b"x").expect("fill"));
    }
    assert!(store.alloc(b"x").is_err());
    offsets.sort_unstable();
    offsets.dedup();
    assert_eq!(offsets.len(), EXT_CONFIG_CHUNK_COUNT);
}
