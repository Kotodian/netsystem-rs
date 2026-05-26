use async_trait::async_trait;
use hammer_adapter::{
    BufferPool, Network, Outbound, ProxyDatagram, ProxyPacketConn, ProxyStream, RouteMetadata,
    SocksAddr,
};
use hammer_core::error::{CoreError, CoreResult};

#[test]
fn buffer_handle_drop_releases_slot_for_reuse_with_new_generation() {
    let pool = BufferPool::with_capacity(128, 1);
    let first_index = {
        let buffer = pool
            .alloc_with_bytes(RouteMetadata::default(), b"hello")
            .expect("alloc first buffer");
        let index = buffer.index();
        assert_eq!(pool.in_use(), 1);
        assert_eq!(buffer.current(), b"hello");
        index
    };

    assert_eq!(pool.in_use(), 0);

    let second = pool
        .alloc_with_bytes(RouteMetadata::default(), b"world")
        .expect("alloc second buffer");
    let second_index = second.index();

    assert_eq!(second_index.slot(), first_index.slot());
    assert_ne!(second_index.generation(), first_index.generation());
    assert!(pool.get(first_index).is_err());
    assert_eq!(second.current(), b"world");
}

#[test]
fn buffer_cursor_headroom_and_append_manage_current_bytes() {
    let pool = BufferPool::with_capacity(16, 2);
    let mut buffer = pool
        .alloc_with_bytes(RouteMetadata::default(), b"payload")
        .expect("alloc buffer");

    buffer.advance(3).expect("advance current");
    assert_eq!(buffer.current(), b"load");

    buffer.prepend(b"pre").expect("prepend into headroom");
    assert_eq!(buffer.current(), b"preload");

    buffer.append(b"-tail").expect("append current");
    assert_eq!(buffer.copy_current_chain(), b"preload-tail");

    buffer.truncate_current(7).expect("truncate current");
    assert_eq!(buffer.current(), b"preload");
}

#[test]
fn append_beyond_one_slot_creates_and_frees_chain() {
    let pool = BufferPool::with_capacity(8, 4);
    let mut buffer = pool
        .alloc_with_bytes(RouteMetadata::default(), b"123456")
        .expect("alloc buffer");

    buffer.append(b"7890abcdef").expect("append chain");

    assert!(buffer.next_buffer().is_some());
    assert_eq!(buffer.copy_current_chain(), b"1234567890abcdef");
    assert!(pool.in_use() > 1);

    drop(buffer);
    assert_eq!(pool.in_use(), 0);
}

#[test]
fn alloc_with_bytes_beyond_one_slot_creates_chain() {
    let pool = BufferPool::with_capacity(4, 4);
    let buffer = pool
        .alloc_with_bytes(RouteMetadata::default(), b"abcdefghijkl")
        .expect("alloc chained buffer");

    assert!(buffer.next_buffer().is_some());
    assert_eq!(buffer.copy_current_chain(), b"abcdefghijkl");
    assert_eq!(pool.in_use(), 3);
}

#[test]
fn handoff_exports_from_source_pool_and_imports_into_target_pool() {
    let source = BufferPool::with_capacity(8, 4);
    let target = BufferPool::with_capacity(8, 4);
    let mut metadata = RouteMetadata::default();
    metadata.protocol = "tls".to_owned();

    let mut buffer = source
        .alloc_with_bytes(metadata, b"client")
        .expect("alloc buffer");
    buffer.append(b"-hello").expect("append chain");

    let handoff = buffer.into_handoff();
    assert_eq!(source.in_use(), 0);

    let imported = target.import(handoff).expect("import handoff");
    assert_eq!(target.in_use(), 2);
    assert_eq!(imported.metadata().protocol, "tls");
    assert_eq!(imported.copy_current_chain(), b"client-hello");
}

#[test]
fn handoff_exposes_current_bytes_for_async_boundaries() {
    let pool = BufferPool::with_capacity(8, 4);
    let mut buffer = pool
        .alloc_with_bytes(RouteMetadata::default(), b"01234567")
        .expect("alloc buffer");

    buffer.advance(2).expect("advance current");
    buffer.append(b"89").expect("append second segment");

    let handoff = buffer.into_handoff();

    assert_eq!(handoff.current_bytes(), b"23456789");
}

struct CapturePacketConn {
    last: Option<Vec<u8>>,
}

#[derive(Default)]
struct CaptureOutbound {
    last: std::sync::Mutex<Option<Vec<u8>>>,
}

#[async_trait]
impl Outbound for CaptureOutbound {
    async fn dial(
        &self,
        _network: Network,
        _destination: SocksAddr,
        initial_payload: &[u8],
    ) -> CoreResult<Box<dyn ProxyStream>> {
        *self.last.lock().expect("last payload poisoned") = Some(initial_payload.to_vec());
        let (client, _server) = tokio::io::duplex(64);
        Ok(Box::new(client))
    }

    async fn listen_packet(&self) -> CoreResult<Box<dyn ProxyPacketConn>> {
        Err(CoreError::internal("listen_packet not used"))
    }
}

#[async_trait]
impl ProxyPacketConn for CapturePacketConn {
    async fn send_to(&mut self, _destination: SocksAddr, payload: bytes::Bytes) -> CoreResult<()> {
        self.last = Some(payload.to_vec());
        Ok(())
    }

    async fn recv_from(&mut self) -> CoreResult<ProxyDatagram> {
        Err(CoreError::internal("recv_from not used"))
    }
}

#[tokio::test]
async fn packet_conn_default_send_buffer_to_uses_handoff_current_bytes() {
    let pool = BufferPool::with_capacity(8, 4);
    let buffer = pool
        .alloc_with_bytes(RouteMetadata::default(), b"datagram")
        .expect("alloc buffer");
    let mut conn = CapturePacketConn { last: None };

    conn.send_buffer_to(
        SocksAddr::ip("127.0.0.1".parse().unwrap(), 53),
        buffer.into_handoff(),
    )
    .await
    .expect("send buffer");

    assert_eq!(conn.last.as_deref(), Some(&b"datagram"[..]));
}

#[tokio::test]
async fn outbound_default_dial_buffer_uses_handoff_current_bytes() {
    let pool = BufferPool::with_capacity(8, 4);
    let mut buffer = pool
        .alloc_with_bytes(RouteMetadata::default(), b"xxhello")
        .expect("alloc buffer");
    buffer.advance(2).expect("advance current");
    let outbound = CaptureOutbound::default();

    let _stream = outbound
        .dial_buffer(
            Network::Tcp,
            SocksAddr::ip("127.0.0.1".parse().unwrap(), 80),
            buffer.into_handoff(),
        )
        .await
        .expect("dial buffer");

    assert_eq!(
        outbound
            .last
            .lock()
            .expect("last payload poisoned")
            .as_deref(),
        Some(&b"hello"[..])
    );
}
