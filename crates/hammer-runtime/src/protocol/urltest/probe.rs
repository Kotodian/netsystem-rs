//! HTTP HEAD probe that runs over a proxied [`ProxyStream`].
//!
//! Models sing-box's `common/urltest.URLTest`: open a TCP stream through the
//! candidate outbound, optionally upgrade to TLS, fire a single HTTP/1.1 HEAD
//! request and time the round-trip until the response status line is parsed.
//!
//! Hand-rolled HTTP/1.1 keeps us off `hyper` for a single one-shot HEAD —
//! the wire format is dirt-simple and the only piece needing real parsing is
//! the response head, which `httparse` handles in a few hundred bytes.

use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use hammer_adapter::{Network, OutboundComponent, PlatformInterface, ProxyStream, SocksAddr};
use hammer_core::error::{HammerError, HammerResult};
use rustls::ClientConfig;
use rustls::pki_types::ServerName;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_rustls::TlsConnector;
use url::Url;

use crate::RuntimePlatform;
use crate::tls_support::client_verifier_builder;

/// Maximum bytes of response head we will buffer before bailing — gstatic and
/// other typical probe URLs answer in well under 1 KiB; cap to bound memory.
const MAX_HEAD_BYTES: usize = 4 * 1024;
/// Single read chunk size — 1 KiB is plenty for HEAD responses and avoids
/// growing the buffer past `MAX_HEAD_BYTES` on the common case.
const READ_CHUNK: usize = 1024;

/// One urltest probe configuration. Cheap to clone via `Arc`; safe to share
/// across concurrent probe tasks because all dial state lives on the
/// per-call stream rather than on `self`.
pub struct HttpUrltestProbe {
    url: Url,
    host: String,
    port: u16,
    use_tls: bool,
    tls: Option<Arc<ClientConfig>>,
    server_name: Option<ServerName<'static>>,
}

impl HttpUrltestProbe {
    /// Build a probe targeting `url`. `https://` URLs build a `ClientConfig`
    /// up-front so every call reuses the same root store; `http://` URLs
    /// skip the TLS plumbing entirely.
    pub fn new(url: Url) -> HammerResult<Self> {
        Self::build(url, None)
    }

    pub fn new_with_platform(url: Url, platform: impl Into<RuntimePlatform>) -> HammerResult<Self> {
        Self::build(url, Some(platform.into().into_inner()))
    }

    fn build(url: Url, platform: Option<Arc<dyn PlatformInterface>>) -> HammerResult<Self> {
        let scheme = url.scheme();
        let use_tls = match scheme {
            "https" => true,
            "http" => false,
            other => {
                return Err(HammerError::config_validation(format!(
                    "urltest url scheme must be http or https, got: {other}"
                )));
            }
        };
        let host = url
            .host_str()
            .ok_or_else(|| HammerError::config_validation("urltest url is missing a host"))?
            .to_owned();
        let port = url
            .port_or_known_default()
            .ok_or_else(|| HammerError::config_validation("urltest url is missing a port"))?;

        let (tls, server_name) = if use_tls {
            let provider = Arc::new(rustls::crypto::ring::default_provider());
            let builder = ClientConfig::builder_with_provider(provider)
                .with_safe_default_protocol_versions()
                .map_err(|err| HammerError::internal(format!("urltest tls versions: {err}")))?;
            let mut config = client_verifier_builder(builder, platform)?.with_no_client_auth();
            config.alpn_protocols = vec![b"http/1.1".to_vec()];
            let server_name: ServerName<'static> = ServerName::try_from(host.as_str())
                .map_err(|err| {
                    HammerError::config_validation(format!("urltest tls server name: {err}"))
                })?
                .to_owned();
            (Some(Arc::new(config)), Some(server_name))
        } else {
            (None, None)
        };

        Ok(Self {
            url,
            host,
            port,
            use_tls,
            tls,
            server_name,
        })
    }

    /// The URL this probe targets.
    pub fn url(&self) -> &Url {
        &self.url
    }

    /// Single round-trip latency probe through `outbound`. Times the wall
    /// clock from before `dial` until the response head's status line is
    /// parsed; on any I/O error returns `Err` carrying the underlying cause.
    pub async fn measure(
        &self,
        outbound: &OutboundComponent,
        timeout: Duration,
    ) -> HammerResult<Duration> {
        let work = async {
            let start = Instant::now();
            // The fallback IP is a sentinel; outbounds that honor `domain`
            // ignore it (hysteria2 forwards the name to its server, direct
            // resolves via the OS resolver). 0.0.0.0 is never routable.
            let dest =
                SocksAddr::domain(self.host.clone(), Ipv4Addr::UNSPECIFIED.into(), self.port);
            let stream = outbound.runtime().dial(Network::Tcp, dest, &[]).await?;
            self.run_http_head(stream).await?;
            Ok::<Duration, HammerError>(start.elapsed())
        };
        match tokio::time::timeout(timeout, work).await {
            Ok(result) => result,
            Err(_) => Err(HammerError::internal(format!(
                "urltest probe timed out for outbound: {}",
                outbound.meta().id()
            ))),
        }
    }

    async fn run_http_head(&self, stream: Box<dyn ProxyStream>) -> HammerResult<()> {
        if self.use_tls {
            let connector =
                TlsConnector::from(self.tls.clone().expect("tls config present on https probe"));
            let server_name = self
                .server_name
                .clone()
                .expect("server name present on https probe");
            let tls_stream = connector
                .connect(server_name, stream)
                .await
                .map_err(|err| HammerError::internal(format!("urltest tls handshake: {err}")))?;
            self.exchange_head(tls_stream).await
        } else {
            self.exchange_head(stream).await
        }
    }

    async fn exchange_head<S>(&self, mut stream: S) -> HammerResult<()>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let request = self.format_request();
        stream
            .write_all(request.as_bytes())
            .await
            .map_err(|err| HammerError::internal(format!("urltest write: {err}")))?;
        stream
            .flush()
            .await
            .map_err(|err| HammerError::internal(format!("urltest flush: {err}")))?;

        let mut buffer = Vec::with_capacity(READ_CHUNK);
        let mut chunk = [0u8; READ_CHUNK];
        loop {
            let n = stream
                .read(&mut chunk)
                .await
                .map_err(|err| HammerError::internal(format!("urltest read: {err}")))?;
            if n == 0 {
                return Err(HammerError::internal(
                    "urltest connection closed before response head",
                ));
            }
            buffer.extend_from_slice(&chunk[..n]);
            if buffer.len() > MAX_HEAD_BYTES {
                return Err(HammerError::internal(
                    "urltest response head exceeds buffer cap",
                ));
            }
            let mut headers = [httparse::EMPTY_HEADER; 32];
            let mut response = httparse::Response::new(&mut headers);
            match response.parse(&buffer) {
                Ok(httparse::Status::Complete(_)) => {
                    let status = response
                        .code
                        .ok_or_else(|| HammerError::internal("urltest response missing status"))?;
                    if !(100..=399).contains(&status) {
                        return Err(HammerError::internal(format!(
                            "urltest unexpected status: {status}"
                        )));
                    }
                    return Ok(());
                }
                Ok(httparse::Status::Partial) => continue,
                Err(err) => {
                    return Err(HammerError::internal(format!(
                        "urltest parse response: {err}"
                    )));
                }
            }
        }
    }

    fn format_request(&self) -> String {
        let path = if self.url.path().is_empty() {
            "/"
        } else {
            self.url.path()
        };
        let query = self
            .url
            .query()
            .map(|q| format!("?{q}"))
            .unwrap_or_default();
        let host_header = if self.is_default_port() {
            self.host.clone()
        } else {
            format!("{}:{}", self.host, self.port)
        };
        format!(
            "HEAD {path}{query} HTTP/1.1\r\n\
             Host: {host_header}\r\n\
             User-Agent: hammer-urltest/1\r\n\
             Accept: */*\r\n\
             Connection: close\r\n\
             \r\n"
        )
    }

    fn is_default_port(&self) -> bool {
        matches!((self.use_tls, self.port), (true, 443) | (false, 80))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use hammer_adapter::{
        ComponentMeta, Network, Outbound, ProxyPacketConn, ProxyStream, RuntimeComponent, SocksAddr,
    };
    use hammer_core::error::{HammerError, HammerResult};
    use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};

    use super::*;

    /// Mock outbound that hands out one half of a `tokio::io::duplex`
    /// pair the test pre-stuffs with a fake HEAD response.
    struct ScriptedOutbound {
        client_half: tokio::sync::Mutex<Option<Box<dyn ProxyStream>>>,
    }

    impl ScriptedOutbound {
        fn new(stream: Box<dyn ProxyStream>) -> Self {
            Self {
                client_half: tokio::sync::Mutex::new(Some(stream)),
            }
        }
    }

    #[async_trait]
    impl Outbound for ScriptedOutbound {
        async fn dial(
            &self,
            _network: Network,
            _destination: SocksAddr,
            _initial_payload: &[u8],
        ) -> HammerResult<Box<dyn ProxyStream>> {
            self.client_half
                .lock()
                .await
                .take()
                .ok_or_else(|| HammerError::internal("scripted outbound dialed twice"))
        }

        async fn listen_packet(&self) -> HammerResult<Box<dyn ProxyPacketConn>> {
            Err(HammerError::internal("scripted outbound: udp unsupported"))
        }
    }

    fn scripted_component(outbound: Arc<dyn Outbound>) -> OutboundComponent {
        RuntimeComponent::new(
            ComponentMeta::new(
                "outbound",
                "scripted",
                "scripted",
                vec![Network::Tcp],
                Vec::new(),
                None,
            ),
            outbound,
        )
    }

    #[tokio::test]
    async fn measure_parses_status_line_and_returns_elapsed() {
        let (client_side, mut test_side) = duplex(4096);
        let outbound = scripted_component(Arc::new(ScriptedOutbound::new(Box::new(client_side))));

        let probe =
            HttpUrltestProbe::new(Url::parse("http://example.test/probe").expect("valid url"))
                .expect("plain http probe builds");

        let measure = tokio::spawn(async move {
            probe
                .measure(&outbound, Duration::from_secs(2))
                .await
                .expect("probe completes")
        });

        // Drain the request the probe writes to the proxy stream.
        let mut request = vec![0u8; 256];
        let mut total = 0;
        loop {
            let n = test_side.read(&mut request[total..]).await.expect("read");
            total += n;
            if request[..total].windows(4).any(|w| w == b"\r\n\r\n") {
                break;
            }
        }
        assert!(request[..total].starts_with(b"HEAD /probe HTTP/1.1\r\n"));

        // Reply with a 204 — gstatic-like response.
        test_side
            .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
            .await
            .expect("respond");
        test_side.shutdown().await.ok();

        let elapsed = measure.await.expect("join");
        assert!(elapsed >= Duration::from_nanos(1));
    }

    #[tokio::test]
    async fn measure_rejects_5xx_status() {
        let (client_side, mut test_side) = duplex(4096);
        let outbound = scripted_component(Arc::new(ScriptedOutbound::new(Box::new(client_side))));

        let probe = HttpUrltestProbe::new(Url::parse("http://example.test/").expect("valid url"))
            .expect("plain http probe builds");

        let measure =
            tokio::spawn(async move { probe.measure(&outbound, Duration::from_secs(2)).await });

        let mut buffer = vec![0u8; 256];
        let mut total = 0;
        loop {
            let n = test_side.read(&mut buffer[total..]).await.expect("read");
            total += n;
            if buffer[..total].windows(4).any(|w| w == b"\r\n\r\n") {
                break;
            }
        }
        test_side
            .write_all(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n")
            .await
            .expect("respond");
        test_side.shutdown().await.ok();

        let err = measure.await.expect("join").expect_err("502 should fail");
        assert!(err.to_string().contains("status: 502"), "{err}");
    }
}
