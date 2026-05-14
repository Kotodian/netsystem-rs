use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine;
use bytes::Bytes;
use hammer_adapter::{
    Inbound, Network, OutboundManager as OutboundManagerTrait, ProxyDatagram, ProxyPacketConn,
    ProxyStream, RouteDecision, RouteMetadata, RouteTarget, SocksAddr,
};
use hammer_core::config::{
    HttpInboundOptions, InboundKind, InboundUser, ListenInboundOptions, MixedInboundOptions,
    SocksInboundOptions,
};
use hammer_core::error::{HammerError, HammerResult};
use hammer_core::lifecycle::{Lifecycle, StartStage};
use hammer_core::log::Logger;
use hammer_core::metrics::MetricsRegistry;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use crate::{DnsRouter, OutboundManager, Router};

const DEFAULT_UDP_TIMEOUT: Duration = Duration::from_secs(300);

#[derive(Clone, Copy)]
enum ProxyMode {
    Socks,
    Http,
    Mixed,
}

#[hammer_component_macros::hammer_component(
    inbound,
    name = "socks",
    builder = build_socks_inbound,
    metrics = ("inbound", "socks")
)]
pub struct SocksInbound {
    id: String,
    options: ProxyInboundOptions,
    router: Arc<Router>,
    outbound: Arc<OutboundManager>,
    closed: Arc<AtomicBool>,
    task: Arc<Mutex<Option<JoinHandle<()>>>>,
}

#[hammer_component_macros::hammer_component(
    inbound,
    name = "http",
    builder = build_http_inbound,
    metrics = ("inbound", "http")
)]
pub struct HttpInbound {
    id: String,
    options: ProxyInboundOptions,
    router: Arc<Router>,
    outbound: Arc<OutboundManager>,
    closed: Arc<AtomicBool>,
    task: Arc<Mutex<Option<JoinHandle<()>>>>,
}

#[hammer_component_macros::hammer_component(
    inbound,
    name = "mixed",
    builder = build_mixed_inbound,
    metrics = ("inbound", "mixed")
)]
pub struct MixedInbound {
    id: String,
    options: ProxyInboundOptions,
    router: Arc<Router>,
    outbound: Arc<OutboundManager>,
    closed: Arc<AtomicBool>,
    task: Arc<Mutex<Option<JoinHandle<()>>>>,
}

#[derive(Clone)]
struct ProxyInboundOptions {
    listen: ListenInboundOptions,
    users: Vec<InboundUser>,
    mode: ProxyMode,
}

impl SocksInbound {
    fn new(
        id: String,
        options: SocksInboundOptions,
        router: Arc<Router>,
        outbound: Arc<OutboundManager>,
    ) -> Self {
        Self {
            id,
            options: ProxyInboundOptions {
                listen: options.listen,
                users: options.users,
                mode: ProxyMode::Socks,
            },
            router,
            outbound,
            closed: Arc::new(AtomicBool::new(false)),
            task: Arc::new(Mutex::new(None)),
        }
    }
}

impl HttpInbound {
    fn new(
        id: String,
        options: HttpInboundOptions,
        router: Arc<Router>,
        outbound: Arc<OutboundManager>,
    ) -> Self {
        Self {
            id,
            options: ProxyInboundOptions {
                listen: options.listen,
                users: options.users,
                mode: ProxyMode::Http,
            },
            router,
            outbound,
            closed: Arc::new(AtomicBool::new(false)),
            task: Arc::new(Mutex::new(None)),
        }
    }
}

impl MixedInbound {
    fn new(
        id: String,
        options: MixedInboundOptions,
        router: Arc<Router>,
        outbound: Arc<OutboundManager>,
    ) -> Self {
        Self {
            id,
            options: ProxyInboundOptions {
                listen: options.listen,
                users: options.users,
                mode: ProxyMode::Mixed,
            },
            router,
            outbound,
            closed: Arc::new(AtomicBool::new(false)),
            task: Arc::new(Mutex::new(None)),
        }
    }
}

macro_rules! impl_proxy_lifecycle {
    ($ty:ty) => {
        impl Lifecycle for $ty {
            fn name(&self) -> &str {
                "inbound"
            }

            fn start(&self, stage: StartStage) -> HammerResult<()> {
                if !matches!(stage, StartStage::Start) {
                    return Ok(());
                }
                self.closed.store(false, Ordering::SeqCst);
                let mut task = self
                    .task
                    .lock()
                    .expect("proxy inbound task handle poisoned");
                if task.as_ref().is_some_and(|handle| !handle.is_finished()) {
                    return Ok(());
                }
                let handle = start_listener(
                    self.id.clone(),
                    self.options.clone(),
                    Arc::clone(&self.router),
                    Arc::clone(&self.outbound),
                    Arc::clone(&self.closed),
                )?;
                *task = Some(handle);
                Ok(())
            }

            fn close(&self) -> HammerResult<()> {
                self.closed.store(true, Ordering::SeqCst);
                if let Some(task) = self
                    .task
                    .lock()
                    .expect("proxy inbound task handle poisoned")
                    .take()
                {
                    task.abort();
                }
                Ok(())
            }
        }

        impl Inbound for $ty {}
    };
}

impl_proxy_lifecycle!(SocksInbound);
impl_proxy_lifecycle!(HttpInbound);
impl_proxy_lifecycle!(MixedInbound);

#[allow(clippy::too_many_arguments)]
pub(crate) fn build_socks_inbound(
    id: String,
    _logger: Logger,
    kind: &InboundKind,
    router: Arc<Router>,
    _dns_router: Option<Arc<DnsRouter>>,
    outbound: Option<Arc<OutboundManager>>,
    _platform: Option<Arc<dyn hammer_adapter::PlatformInterface>>,
    _metrics: Arc<MetricsRegistry>,
) -> HammerResult<Arc<SocksInbound>> {
    let InboundKind::Socks(options) = kind else {
        return Err(HammerError::internal(
            "socks factory received wrong options",
        ));
    };
    let outbound =
        outbound.ok_or_else(|| HammerError::internal("socks inbound requires outbound manager"))?;
    Ok(Arc::new(SocksInbound::new(
        id,
        options.clone(),
        router,
        outbound,
    )))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn build_http_inbound(
    id: String,
    _logger: Logger,
    kind: &InboundKind,
    router: Arc<Router>,
    _dns_router: Option<Arc<DnsRouter>>,
    outbound: Option<Arc<OutboundManager>>,
    _platform: Option<Arc<dyn hammer_adapter::PlatformInterface>>,
    _metrics: Arc<MetricsRegistry>,
) -> HammerResult<Arc<HttpInbound>> {
    let InboundKind::Http(options) = kind else {
        return Err(HammerError::internal("http factory received wrong options"));
    };
    let outbound =
        outbound.ok_or_else(|| HammerError::internal("http inbound requires outbound manager"))?;
    Ok(Arc::new(HttpInbound::new(
        id,
        options.clone(),
        router,
        outbound,
    )))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn build_mixed_inbound(
    id: String,
    _logger: Logger,
    kind: &InboundKind,
    router: Arc<Router>,
    _dns_router: Option<Arc<DnsRouter>>,
    outbound: Option<Arc<OutboundManager>>,
    _platform: Option<Arc<dyn hammer_adapter::PlatformInterface>>,
    _metrics: Arc<MetricsRegistry>,
) -> HammerResult<Arc<MixedInbound>> {
    let InboundKind::Mixed(options) = kind else {
        return Err(HammerError::internal(
            "mixed factory received wrong options",
        ));
    };
    let outbound =
        outbound.ok_or_else(|| HammerError::internal("mixed inbound requires outbound manager"))?;
    Ok(Arc::new(MixedInbound::new(
        id,
        options.clone(),
        router,
        outbound,
    )))
}

fn start_listener(
    inbound_id: String,
    options: ProxyInboundOptions,
    router: Arc<Router>,
    outbound: Arc<OutboundManager>,
    closed: Arc<AtomicBool>,
) -> HammerResult<JoinHandle<()>> {
    let bind = SocketAddr::new(options.listen.listen, options.listen.listen_port);
    let std_listener = std::net::TcpListener::bind(bind)
        .map_err(|err| HammerError::internal(format!("bind proxy inbound {bind}: {err}")))?;
    std_listener
        .set_nonblocking(true)
        .map_err(|err| HammerError::internal(format!("set proxy inbound nonblocking: {err}")))?;
    let listener = TcpListener::from_std(std_listener)
        .map_err(|err| HammerError::internal(format!("create proxy listener: {err}")))?;
    let task = crate::spawn::spawn(async move {
        loop {
            if closed.load(Ordering::SeqCst) {
                break;
            }
            match listener.accept().await {
                Ok((stream, source)) => {
                    let options = options.clone();
                    let router = Arc::clone(&router);
                    let outbound = Arc::clone(&outbound);
                    let inbound_id = inbound_id.clone();
                    crate::spawn::spawn(async move {
                        if let Err(err) = handle_proxy_stream(
                            inbound_id, options, router, outbound, stream, source,
                        )
                        .await
                        {
                            debug!("proxy inbound connection failed: {err}");
                        }
                    });
                }
                Err(err) => {
                    if closed.load(Ordering::SeqCst) {
                        break;
                    }
                    warn!("proxy inbound accept failed: {err}");
                    break;
                }
            }
        }
    });
    Ok(task)
}

async fn handle_proxy_stream(
    inbound_id: String,
    options: ProxyInboundOptions,
    router: Arc<Router>,
    outbound: Arc<OutboundManager>,
    stream: TcpStream,
    source: SocketAddr,
) -> HammerResult<()> {
    match options.mode {
        ProxyMode::Socks => {
            handle_socks_stream(inbound_id, options, router, outbound, stream, source).await
        }
        ProxyMode::Http => {
            handle_http_stream(inbound_id, options, router, outbound, stream, source).await
        }
        ProxyMode::Mixed => {
            let first = stream.peek(&mut [0; 1]).await.map_err(|err| {
                HammerError::internal(format!("peek mixed inbound protocol: {err}"))
            })?;
            if first == 0 {
                return Ok(());
            }
            let mut buf = [0_u8; 1];
            stream.peek(&mut buf).await.map_err(|err| {
                HammerError::internal(format!("peek mixed inbound protocol byte: {err}"))
            })?;
            if matches!(buf[0], 0x04 | 0x05) {
                handle_socks_stream(inbound_id, options, router, outbound, stream, source).await
            } else {
                handle_http_stream(inbound_id, options, router, outbound, stream, source).await
            }
        }
    }
}

async fn handle_http_stream(
    inbound_id: String,
    options: ProxyInboundOptions,
    router: Arc<Router>,
    outbound: Arc<OutboundManager>,
    mut stream: TcpStream,
    source: SocketAddr,
) -> HammerResult<()> {
    let mut header = Vec::new();
    read_http_header(&mut stream, &mut header).await?;
    let request = String::from_utf8_lossy(&header);
    let mut lines = request.split("\r\n");
    let Some(request_line) = lines.next() else {
        return Ok(());
    };
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default();
    let target = parts.next().unwrap_or_default();
    let version = parts.next().unwrap_or("HTTP/1.1");
    let headers: Vec<&str> = lines.collect();
    let username = match authenticate_http(&options.users, &headers) {
        Ok(username) => username,
        Err(err) => {
            if !options.users.is_empty() {
                let _ = stream
                    .write_all(
                        b"HTTP/1.1 407 Proxy Authentication Required\r\nProxy-Authenticate: Basic realm=\"hammer\"\r\nContent-Length: 0\r\n\r\n",
                    )
                    .await;
            }
            return Err(err);
        }
    };
    if method.eq_ignore_ascii_case("CONNECT") {
        let destination = parse_host_port(target, 443)?;
        let remote = route_tcp(
            inbound_id,
            router,
            outbound,
            source,
            destination,
            username,
            &[],
        )
        .await?;
        stream
            .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            .await
            .map_err(|err| HammerError::internal(format!("write CONNECT response: {err}")))?;
        relay_tcp(stream, remote).await
    } else {
        let (destination, path) = parse_absolute_http_target(target)?;
        let initial = rewrite_http_request(method, &path, version, &headers);
        let remote = route_tcp(
            inbound_id,
            router,
            outbound,
            source,
            destination,
            username,
            &initial,
        )
        .await?;
        relay_tcp(stream, remote).await
    }
}

async fn read_http_header(stream: &mut TcpStream, out: &mut Vec<u8>) -> HammerResult<()> {
    let mut byte = [0_u8; 1];
    while out.len() < 64 * 1024 {
        let n = stream
            .read(&mut byte)
            .await
            .map_err(|err| HammerError::internal(format!("read HTTP proxy request: {err}")))?;
        if n == 0 {
            return Err(HammerError::internal("HTTP proxy request closed early"));
        }
        out.push(byte[0]);
        if out.ends_with(b"\r\n\r\n") {
            return Ok(());
        }
    }
    Err(HammerError::internal("HTTP proxy request header too large"))
}

fn authenticate_http(users: &[InboundUser], headers: &[&str]) -> HammerResult<Option<String>> {
    if users.is_empty() {
        return Ok(None);
    }
    let Some(header) = headers.iter().find_map(|line| {
        line.split_once(':').and_then(|(name, value)| {
            if name.eq_ignore_ascii_case("proxy-authorization") {
                Some(value.trim())
            } else {
                None
            }
        })
    }) else {
        return Err(HammerError::internal("HTTP proxy authentication required"));
    };
    let Some((scheme, encoded)) = header.split_once(' ') else {
        return Err(HammerError::internal(
            "unsupported HTTP proxy authentication",
        ));
    };
    if !scheme.eq_ignore_ascii_case("basic") {
        return Err(HammerError::internal(
            "unsupported HTTP proxy authentication",
        ));
    }
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(|err| HammerError::internal(format!("decode proxy authorization: {err}")))?;
    let decoded = String::from_utf8(decoded)
        .map_err(|err| HammerError::internal(format!("decode proxy authorization utf8: {err}")))?;
    let Some((username, password)) = decoded.split_once(':') else {
        return Err(HammerError::internal("invalid proxy authorization"));
    };
    if users
        .iter()
        .any(|user| user.username == username && user.password == password)
    {
        return Ok(Some(username.to_owned()));
    }
    Err(HammerError::internal("invalid proxy credentials"))
}

fn rewrite_http_request(method: &str, path: &str, version: &str, headers: &[&str]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(format!("{method} {path} {version}\r\n").as_bytes());
    for header in headers {
        if header.is_empty() {
            continue;
        }
        if header
            .split_once(':')
            .is_some_and(|(name, _)| name.eq_ignore_ascii_case("proxy-authorization"))
        {
            continue;
        }
        out.extend_from_slice(header.as_bytes());
        out.extend_from_slice(b"\r\n");
    }
    out.extend_from_slice(b"\r\n");
    out
}

async fn handle_socks_stream(
    inbound_id: String,
    options: ProxyInboundOptions,
    router: Arc<Router>,
    outbound: Arc<OutboundManager>,
    mut stream: TcpStream,
    source: SocketAddr,
) -> HammerResult<()> {
    let version = stream
        .read_u8()
        .await
        .map_err(|err| HammerError::internal(format!("read SOCKS version: {err}")))?;
    match version {
        0x05 => handle_socks5_stream(inbound_id, options, router, outbound, stream, source).await,
        0x04 => handle_socks4_stream(inbound_id, options, router, outbound, stream, source).await,
        _ => Err(HammerError::internal("unsupported SOCKS version")),
    }
}

async fn handle_socks5_stream(
    inbound_id: String,
    options: ProxyInboundOptions,
    router: Arc<Router>,
    outbound: Arc<OutboundManager>,
    mut stream: TcpStream,
    source: SocketAddr,
) -> HammerResult<()> {
    let method_count = stream
        .read_u8()
        .await
        .map_err(|err| HammerError::internal(format!("read SOCKS5 method count: {err}")))?;
    let mut methods = vec![0; method_count as usize];
    stream
        .read_exact(&mut methods)
        .await
        .map_err(|err| HammerError::internal(format!("read SOCKS5 methods: {err}")))?;
    let require_auth = !options.users.is_empty();
    let selected = if require_auth {
        if methods.contains(&0x02) { 0x02 } else { 0xff }
    } else if methods.contains(&0x00) {
        0x00
    } else {
        0xff
    };
    stream
        .write_all(&[0x05, selected])
        .await
        .map_err(|err| HammerError::internal(format!("write SOCKS5 method: {err}")))?;
    if selected == 0xff {
        return Err(HammerError::internal("no supported SOCKS5 auth method"));
    }
    let username = if selected == 0x02 {
        Some(authenticate_socks5(&mut stream, &options.users).await?)
    } else {
        None
    };
    let version = stream
        .read_u8()
        .await
        .map_err(|err| HammerError::internal(format!("read SOCKS5 request version: {err}")))?;
    if version != 0x05 {
        return Err(HammerError::internal("invalid SOCKS5 request version"));
    }
    let command = stream
        .read_u8()
        .await
        .map_err(|err| HammerError::internal(format!("read SOCKS5 command: {err}")))?;
    let _reserved = stream
        .read_u8()
        .await
        .map_err(|err| HammerError::internal(format!("read SOCKS5 reserved: {err}")))?;
    let destination = read_socks5_addr(&mut stream).await?;
    match command {
        0x01 => {
            let remote = route_tcp(
                inbound_id,
                router,
                outbound,
                source,
                destination,
                username,
                &[],
            )
            .await?;
            write_socks5_success(
                &mut stream,
                SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
            )
            .await?;
            relay_tcp(stream, remote).await
        }
        0x03 => {
            handle_socks5_udp_associate(
                inbound_id, options, router, outbound, stream, source, username,
            )
            .await
        }
        _ => {
            write_socks5_reply(
                &mut stream,
                0x07,
                SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
            )
            .await?;
            Err(HammerError::internal("unsupported SOCKS5 command"))
        }
    }
}

async fn authenticate_socks5(
    stream: &mut TcpStream,
    users: &[InboundUser],
) -> HammerResult<String> {
    let version = stream
        .read_u8()
        .await
        .map_err(|err| HammerError::internal(format!("read SOCKS5 auth version: {err}")))?;
    if version != 0x01 {
        return Err(HammerError::internal("unsupported SOCKS5 auth version"));
    }
    let username_len = stream
        .read_u8()
        .await
        .map_err(|err| HammerError::internal(format!("read SOCKS5 username len: {err}")))?;
    let mut username = vec![0; username_len as usize];
    stream
        .read_exact(&mut username)
        .await
        .map_err(|err| HammerError::internal(format!("read SOCKS5 username: {err}")))?;
    let password_len = stream
        .read_u8()
        .await
        .map_err(|err| HammerError::internal(format!("read SOCKS5 password len: {err}")))?;
    let mut password = vec![0; password_len as usize];
    stream
        .read_exact(&mut password)
        .await
        .map_err(|err| HammerError::internal(format!("read SOCKS5 password: {err}")))?;
    let username = String::from_utf8(username)
        .map_err(|err| HammerError::internal(format!("SOCKS5 username utf8: {err}")))?;
    let password = String::from_utf8(password)
        .map_err(|err| HammerError::internal(format!("SOCKS5 password utf8: {err}")))?;
    let ok = users
        .iter()
        .any(|user| user.username == username && user.password == password);
    stream
        .write_all(&[0x01, if ok { 0x00 } else { 0x01 }])
        .await
        .map_err(|err| HammerError::internal(format!("write SOCKS5 auth result: {err}")))?;
    if ok {
        Ok(username)
    } else {
        Err(HammerError::internal("invalid SOCKS5 credentials"))
    }
}

async fn handle_socks4_stream(
    inbound_id: String,
    options: ProxyInboundOptions,
    router: Arc<Router>,
    outbound: Arc<OutboundManager>,
    mut stream: TcpStream,
    source: SocketAddr,
) -> HammerResult<()> {
    let command = stream
        .read_u8()
        .await
        .map_err(|err| HammerError::internal(format!("read SOCKS4 command: {err}")))?;
    if command != 0x01 {
        return Err(HammerError::internal("unsupported SOCKS4 command"));
    }
    let port = stream
        .read_u16()
        .await
        .map_err(|err| HammerError::internal(format!("read SOCKS4 port: {err}")))?;
    let mut ip = [0_u8; 4];
    stream
        .read_exact(&mut ip)
        .await
        .map_err(|err| HammerError::internal(format!("read SOCKS4 address: {err}")))?;
    let user_id = read_nul_string(&mut stream).await?;
    let destination = if ip[0..3] == [0, 0, 0] && ip[3] != 0 {
        let domain = read_nul_string(&mut stream).await?;
        SocksAddr::domain(domain, IpAddr::V4(Ipv4Addr::UNSPECIFIED), port)
    } else {
        SocksAddr::ip(IpAddr::V4(Ipv4Addr::from(ip)), port)
    };
    if !options.users.is_empty() && !options.users.iter().any(|user| user.username == user_id) {
        stream.write_all(&[0x00, 0x5d, 0, 0, 0, 0, 0, 0]).await.ok();
        return Err(HammerError::internal("invalid SOCKS4 credentials"));
    }
    let remote = route_tcp(
        inbound_id,
        router,
        outbound,
        source,
        destination,
        Some(user_id),
        &[],
    )
    .await?;
    stream
        .write_all(&[0x00, 0x5a, 0, 0, 0, 0, 0, 0])
        .await
        .map_err(|err| HammerError::internal(format!("write SOCKS4 success response: {err}")))?;
    relay_tcp(stream, remote).await
}

async fn read_nul_string(stream: &mut TcpStream) -> HammerResult<String> {
    let mut bytes = Vec::new();
    loop {
        let byte = stream
            .read_u8()
            .await
            .map_err(|err| HammerError::internal(format!("read nul string: {err}")))?;
        if byte == 0 {
            break;
        }
        bytes.push(byte);
        if bytes.len() > 255 {
            return Err(HammerError::internal("nul string too long"));
        }
    }
    String::from_utf8(bytes).map_err(|err| HammerError::internal(format!("nul string utf8: {err}")))
}

async fn read_socks5_addr(stream: &mut TcpStream) -> HammerResult<SocksAddr> {
    let atyp = stream
        .read_u8()
        .await
        .map_err(|err| HammerError::internal(format!("read SOCKS5 atyp: {err}")))?;
    match atyp {
        0x01 => {
            let mut ip = [0_u8; 4];
            stream
                .read_exact(&mut ip)
                .await
                .map_err(|err| HammerError::internal(format!("read SOCKS5 IPv4: {err}")))?;
            let port = stream
                .read_u16()
                .await
                .map_err(|err| HammerError::internal(format!("read SOCKS5 port: {err}")))?;
            Ok(SocksAddr::ip(IpAddr::V4(Ipv4Addr::from(ip)), port))
        }
        0x03 => {
            let len = stream
                .read_u8()
                .await
                .map_err(|err| HammerError::internal(format!("read SOCKS5 domain len: {err}")))?;
            let mut domain = vec![0; len as usize];
            stream
                .read_exact(&mut domain)
                .await
                .map_err(|err| HammerError::internal(format!("read SOCKS5 domain: {err}")))?;
            let port = stream
                .read_u16()
                .await
                .map_err(|err| HammerError::internal(format!("read SOCKS5 port: {err}")))?;
            let domain = String::from_utf8(domain)
                .map_err(|err| HammerError::internal(format!("SOCKS5 domain utf8: {err}")))?;
            Ok(SocksAddr::domain(
                domain,
                IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                port,
            ))
        }
        0x04 => {
            let mut ip = [0_u8; 16];
            stream
                .read_exact(&mut ip)
                .await
                .map_err(|err| HammerError::internal(format!("read SOCKS5 IPv6: {err}")))?;
            let port = stream
                .read_u16()
                .await
                .map_err(|err| HammerError::internal(format!("read SOCKS5 port: {err}")))?;
            Ok(SocksAddr::ip(IpAddr::V6(ip.into()), port))
        }
        _ => Err(HammerError::internal("unsupported SOCKS5 address type")),
    }
}

async fn write_socks5_success(stream: &mut TcpStream, bind: SocketAddr) -> HammerResult<()> {
    write_socks5_reply(stream, 0x00, bind).await
}

async fn write_socks5_reply(
    stream: &mut TcpStream,
    code: u8,
    bind: SocketAddr,
) -> HammerResult<()> {
    let mut response = Vec::new();
    response.extend_from_slice(&[0x05, code, 0x00]);
    match bind.ip() {
        IpAddr::V4(addr) => {
            response.push(0x01);
            response.extend_from_slice(&addr.octets());
        }
        IpAddr::V6(addr) => {
            response.push(0x04);
            response.extend_from_slice(&addr.octets());
        }
    }
    response.extend_from_slice(&bind.port().to_be_bytes());
    stream
        .write_all(&response)
        .await
        .map_err(|err| HammerError::internal(format!("write SOCKS5 reply: {err}")))
}

async fn handle_socks5_udp_associate(
    inbound_id: String,
    options: ProxyInboundOptions,
    router: Arc<Router>,
    outbound: Arc<OutboundManager>,
    mut stream: TcpStream,
    _source: SocketAddr,
    username: Option<String>,
) -> HammerResult<()> {
    let bind_ip = match options.listen.listen {
        IpAddr::V4(_) => IpAddr::V4(Ipv4Addr::LOCALHOST),
        IpAddr::V6(_) => IpAddr::V6(std::net::Ipv6Addr::LOCALHOST),
    };
    let socket = tokio::net::UdpSocket::bind(SocketAddr::new(bind_ip, 0))
        .await
        .map_err(|err| HammerError::internal(format!("bind SOCKS5 UDP relay: {err}")))?;
    let bind = socket
        .local_addr()
        .map_err(|err| HammerError::internal(format!("SOCKS5 UDP relay addr: {err}")))?;
    write_socks5_success(&mut stream, bind).await?;
    let (response_tx, mut response_rx) = mpsc::channel(128);
    let mut relays = HashMap::<String, mpsc::Sender<UdpRelaySend>>::new();
    let timeout = options.listen.udp_timeout.unwrap_or(DEFAULT_UDP_TIMEOUT);
    let mut buf = vec![0_u8; 64 * 1024];
    let mut client_addr = None;
    loop {
        tokio::select! {
            result = socket.recv_from(&mut buf) => {
                let (n, client) = result.map_err(|err| HammerError::internal(format!("recv SOCKS5 UDP packet: {err}")))?;
                if let Some(known_client) = client_addr {
                    if known_client != client {
                        continue;
                    }
                } else {
                    client_addr = Some(client);
                }
                let (destination, payload) = decode_socks5_udp(&buf[..n])?;
                let outbound_id =
                    route_udp(&inbound_id, &router, client, destination.clone(), username.clone())?;
                let relay = udp_relay_sender(
                    &mut relays,
                    &outbound,
                    &outbound_id,
                    response_tx.clone(),
                )
                .await?;
                relay
                    .send(UdpRelaySend {
                        destination,
                        payload: Bytes::copy_from_slice(payload),
                    })
                    .await
                    .map_err(|_| HammerError::internal("SOCKS5 UDP relay closed"))?;
            }
            result = response_rx.recv() => {
                let Some(packet) = result else {
                    break;
                };
                if let Some(client) = client_addr {
                    let response = encode_socks5_udp(&packet.destination, &packet.payload)?;
                    socket
                        .send_to(&response, client)
                        .await
                        .map_err(|err| HammerError::internal(format!("send SOCKS5 UDP response: {err}")))?;
                }
            }
            _ = tokio::time::sleep(timeout) => break,
            result = stream.read_u8() => {
                if result.is_err() {
                    break;
                }
            }
        }
    }
    debug!("SOCKS5 UDP association closed for {inbound_id}");
    Ok(())
}

struct UdpRelaySend {
    destination: SocksAddr,
    payload: Bytes,
}

async fn udp_relay_sender(
    relays: &mut HashMap<String, mpsc::Sender<UdpRelaySend>>,
    outbound: &OutboundManager,
    outbound_id: &str,
    response_tx: mpsc::Sender<ProxyDatagram>,
) -> HammerResult<mpsc::Sender<UdpRelaySend>> {
    if let Some(sender) = relays.get(outbound_id) {
        return Ok(sender.clone());
    }
    let outbound = outbound
        .get(outbound_id)
        .ok_or_else(|| HammerError::internal(format!("outbound not found: {outbound_id}")))?;
    let packet_conn = outbound.runtime().listen_packet().await?;
    let (send_tx, send_rx) = mpsc::channel(128);
    spawn_udp_relay(outbound_id.to_owned(), packet_conn, send_rx, response_tx);
    relays.insert(outbound_id.to_owned(), send_tx.clone());
    Ok(send_tx)
}

fn spawn_udp_relay(
    outbound_id: String,
    mut packet_conn: Box<dyn ProxyPacketConn>,
    mut send_rx: mpsc::Receiver<UdpRelaySend>,
    response_tx: mpsc::Sender<ProxyDatagram>,
) {
    crate::spawn::spawn(async move {
        let mut can_recv = false;
        loop {
            if !can_recv {
                let Some(packet) = send_rx.recv().await else {
                    break;
                };
                if let Err(err) = packet_conn
                    .send_to(packet.destination, packet.payload)
                    .await
                {
                    debug!("SOCKS5 UDP send failed via outbound={outbound_id}: {err}");
                    break;
                }
                can_recv = true;
                continue;
            }
            tokio::select! {
                packet = send_rx.recv() => {
                    let Some(packet) = packet else {
                        break;
                    };
                    if let Err(err) = packet_conn.send_to(packet.destination, packet.payload).await {
                        debug!("SOCKS5 UDP send failed via outbound={outbound_id}: {err}");
                        break;
                    }
                }
                result = packet_conn.recv_from() => {
                    match result {
                        Ok(packet) => {
                            if response_tx.try_send(packet).is_err() {
                                debug!("SOCKS5 UDP response dropped via outbound={outbound_id}: relay channel full or closed");
                            }
                        }
                        Err(err) => {
                            debug!("SOCKS5 UDP recv failed via outbound={outbound_id}: {err}");
                            break;
                        }
                    }
                }
            }
        }
    });
}

fn decode_socks5_udp(packet: &[u8]) -> HammerResult<(SocksAddr, &[u8])> {
    if packet.len() < 4 || packet[0] != 0 || packet[1] != 0 || packet[2] != 0 {
        return Err(HammerError::internal("invalid SOCKS5 UDP header"));
    }
    let atyp = packet[3];
    let mut offset = 4;
    let destination = match atyp {
        0x01 => {
            if packet.len() < offset + 4 + 2 {
                return Err(HammerError::internal("truncated SOCKS5 UDP IPv4"));
            }
            let ip = IpAddr::V4(Ipv4Addr::new(
                packet[offset],
                packet[offset + 1],
                packet[offset + 2],
                packet[offset + 3],
            ));
            offset += 4;
            let port = u16::from_be_bytes([packet[offset], packet[offset + 1]]);
            offset += 2;
            SocksAddr::ip(ip, port)
        }
        0x03 => {
            if packet.len() < offset + 1 {
                return Err(HammerError::internal("truncated SOCKS5 UDP domain len"));
            }
            let len = packet[offset] as usize;
            offset += 1;
            if packet.len() < offset + len + 2 {
                return Err(HammerError::internal("truncated SOCKS5 UDP domain"));
            }
            let domain = String::from_utf8(packet[offset..offset + len].to_vec())
                .map_err(|err| HammerError::internal(format!("SOCKS5 UDP domain utf8: {err}")))?;
            offset += len;
            let port = u16::from_be_bytes([packet[offset], packet[offset + 1]]);
            offset += 2;
            SocksAddr::domain(domain, IpAddr::V4(Ipv4Addr::UNSPECIFIED), port)
        }
        0x04 => {
            if packet.len() < offset + 16 + 2 {
                return Err(HammerError::internal("truncated SOCKS5 UDP IPv6"));
            }
            let mut ip = [0_u8; 16];
            ip.copy_from_slice(&packet[offset..offset + 16]);
            offset += 16;
            let port = u16::from_be_bytes([packet[offset], packet[offset + 1]]);
            offset += 2;
            SocksAddr::ip(IpAddr::V6(Ipv6Addr::from(ip)), port)
        }
        _ => return Err(HammerError::internal("unsupported SOCKS5 UDP address type")),
    };
    Ok((destination, &packet[offset..]))
}

fn encode_socks5_udp(destination: &SocksAddr, payload: &[u8]) -> HammerResult<Vec<u8>> {
    let mut packet = Vec::with_capacity(4 + 1 + 255 + 2 + payload.len());
    packet.extend_from_slice(&[0, 0, 0]);
    if let Some(domain) = &destination.domain {
        let len = u8::try_from(domain.len())
            .map_err(|_| HammerError::internal("SOCKS5 UDP domain too long"))?;
        packet.push(0x03);
        packet.push(len);
        packet.extend_from_slice(domain.as_bytes());
    } else {
        match destination.host {
            IpAddr::V4(addr) => {
                packet.push(0x01);
                packet.extend_from_slice(&addr.octets());
            }
            IpAddr::V6(addr) => {
                packet.push(0x04);
                packet.extend_from_slice(&addr.octets());
            }
        }
    }
    packet.extend_from_slice(&destination.port.to_be_bytes());
    packet.extend_from_slice(payload);
    Ok(packet)
}

async fn route_tcp(
    inbound_id: String,
    router: Arc<Router>,
    outbound: Arc<OutboundManager>,
    source: SocketAddr,
    destination: SocksAddr,
    username: Option<String>,
    initial_payload: &[u8],
) -> HammerResult<Box<dyn ProxyStream>> {
    let mut metadata = RouteMetadata {
        inbound: inbound_id,
        network: Network::Tcp,
        protocol: String::new(),
        source: Some(SocksAddr::ip(source.ip(), source.port())),
        destination: Some(destination.clone()),
        domain: destination.domain.clone(),
        client: username,
        domain_strategy: None,
        udp_disable_domain_unmapping: false,
        override_destination: false,
    };
    router.prepare_route_metadata(&mut metadata)?;
    match router.match_route(&mut metadata)? {
        RouteDecision::Route {
            target: RouteTarget::Outbound(id),
        } => {
            let outbound = outbound
                .get(&id)
                .ok_or_else(|| HammerError::internal(format!("outbound not found: {id}")))?;
            outbound
                .runtime()
                .dial(Network::Tcp, destination, initial_payload)
                .await
        }
        RouteDecision::Route {
            target: RouteTarget::Endpoint(id),
        } => Err(HammerError::internal(format!(
            "proxy inbound cannot route to L3 endpoint: {id}"
        ))),
        RouteDecision::Reject { method } => Err(HammerError::internal(format!(
            "proxy inbound rejected by route: {method}"
        ))),
        RouteDecision::HijackDns => {
            Err(HammerError::internal("proxy inbound cannot hijack TCP DNS"))
        }
    }
}

fn route_udp(
    inbound_id: &str,
    router: &Router,
    source: SocketAddr,
    destination: SocksAddr,
    username: Option<String>,
) -> HammerResult<String> {
    let mut metadata = RouteMetadata {
        inbound: inbound_id.to_owned(),
        network: Network::Udp,
        protocol: String::new(),
        source: Some(SocksAddr::ip(source.ip(), source.port())),
        destination: Some(destination.clone()),
        domain: destination.domain.clone(),
        client: username,
        domain_strategy: None,
        udp_disable_domain_unmapping: false,
        override_destination: false,
    };
    router.prepare_route_metadata(&mut metadata)?;
    match router.match_route(&mut metadata)? {
        RouteDecision::Route {
            target: RouteTarget::Outbound(id),
        } => Ok(id),
        RouteDecision::Route {
            target: RouteTarget::Endpoint(id),
        } => Err(HammerError::internal(format!(
            "proxy inbound cannot route UDP to L3 endpoint: {id}"
        ))),
        RouteDecision::Reject { method } => Err(HammerError::internal(format!(
            "proxy inbound UDP rejected by route: {method}"
        ))),
        RouteDecision::HijackDns => {
            Err(HammerError::internal("proxy inbound cannot hijack UDP DNS"))
        }
    }
}

async fn relay_tcp(mut client: TcpStream, mut remote: Box<dyn ProxyStream>) -> HammerResult<()> {
    tokio::io::copy_bidirectional(&mut client, &mut remote)
        .await
        .map_err(|err| HammerError::internal(format!("proxy relay tcp: {err}")))?;
    Ok(())
}

fn parse_host_port(value: &str, default_port: u16) -> HammerResult<SocksAddr> {
    if let Ok(addr) = value.parse::<SocketAddr>() {
        return Ok(SocksAddr::ip(addr.ip(), addr.port()));
    }
    let (host, port) = match value.rsplit_once(':') {
        Some((host, port)) => (
            host.trim_matches(['[', ']']),
            port.parse::<u16>()
                .map_err(|err| HammerError::internal(format!("parse destination port: {err}")))?,
        ),
        None => (value, default_port),
    };
    if let Ok(ip) = host.parse::<IpAddr>() {
        Ok(SocksAddr::ip(ip, port))
    } else {
        Ok(SocksAddr::domain(
            host,
            IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            port,
        ))
    }
}

fn parse_absolute_http_target(value: &str) -> HammerResult<(SocksAddr, String)> {
    let Some(rest) = value.strip_prefix("http://") else {
        return Err(HammerError::internal(
            "HTTP proxy request must use absolute http URL",
        ));
    };
    let (authority, path) = match rest.find('/') {
        Some(index) => (&rest[..index], &rest[index..]),
        None => (rest, "/"),
    };
    Ok((parse_host_port(authority, 80)?, path.to_owned()))
}
