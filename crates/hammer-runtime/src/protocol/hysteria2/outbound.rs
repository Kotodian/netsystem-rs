use super::*;

#[hammer_component_macros::hammer_component(
    outbound,
    name = "hysteria2",
    builder = build_outbound,
    metrics = ("outbound", "outbound")
)]
pub struct Hysteria2Outbound {
    id: String,
    options: Hysteria2OutboundOptions,
    networks: Vec<Network>,
    dependencies: Vec<String>,
    client_state: StdMutex<ClientState>,
    connect_backoff: StdMutex<ConnectBackoff>,
    client_init: Mutex<()>,
    protector: SocketProtector,
    control_handle: Option<Arc<ControlThreadHandle>>,
}

struct ClientState {
    epoch: u64,
    client: Option<Arc<Hysteria2Client>>,
}

#[derive(Debug, Default)]
pub(super) struct ConnectBackoff {
    current_delay: Duration,
    next_allowed: Option<StdInstant>,
}

impl ConnectBackoff {
    pub(super) fn remaining(&self, now: StdInstant) -> Option<Duration> {
        let next_allowed = self.next_allowed?;
        if now >= next_allowed {
            None
        } else {
            Some(next_allowed.duration_since(now))
        }
    }

    pub(super) fn record_failure(&mut self, now: StdInstant) {
        self.current_delay = if self.current_delay.is_zero() {
            HYSTERIA2_CONNECT_BACKOFF_INITIAL
        } else {
            (self.current_delay * 2).min(HYSTERIA2_CONNECT_BACKOFF_MAX)
        };
        self.next_allowed = Some(now + self.current_delay);
    }

    pub(super) fn reset(&mut self) {
        self.current_delay = Duration::ZERO;
        self.next_allowed = None;
    }

    #[cfg(test)]
    pub(super) fn current_delay(&self) -> Duration {
        self.current_delay
    }
}

impl Hysteria2Outbound {
    pub fn new(logger: Logger, id: String, options: Hysteria2OutboundOptions) -> Self {
        Self::new_with_protector(logger, id, options, SocketProtector::default(), None)
    }

    pub(crate) fn new_with_protector(
        _logger: Logger,
        id: String,
        options: Hysteria2OutboundOptions,
        protector: SocketProtector,
        control_handle: Option<Arc<ControlThreadHandle>>,
    ) -> Self {
        let networks = adapter_networks(&options.network);
        Self {
            id,
            options,
            networks,
            dependencies: Vec::new(),
            client_state: StdMutex::new(ClientState {
                epoch: 0,
                client: None,
            }),
            connect_backoff: StdMutex::new(ConnectBackoff::default()),
            client_init: Mutex::new(()),
            protector,
            control_handle,
        }
    }

    async fn client(&self) -> HammerResult<Arc<Hysteria2Client>> {
        self.client_with_timeout(HYSTERIA2_CONNECT_TIMEOUT).await
    }

    pub(super) async fn client_with_timeout(
        &self,
        connect_timeout: Duration,
    ) -> HammerResult<Arc<Hysteria2Client>> {
        if let Some(client) = self.cached_client() {
            return Ok(client);
        }
        loop {
            let _guard = self.client_init.lock().await;
            if let Some(client) = self.cached_client() {
                return Ok(client);
            }
            if let Some(remaining) = self.connect_backoff_remaining() {
                return Err(HammerError::internal(format!(
                    "hysteria2 outbound {} connect backing off for {} after recent failure",
                    self.id,
                    duration_label(remaining)
                )));
            }
            let epoch = self.client_epoch();
            let options = self.client_options()?;
            let auth_event_context =
                self.control_handle
                    .as_ref()
                    .map(|control_handle| Hysteria2AuthEventContext {
                        outbound_id: self.id.clone(),
                        control_handle: Arc::clone(control_handle),
                    });
            debug!("hysteria2 outbound {} initializing client", self.id);
            let client = match connect_with_timeout_and_events(
                options,
                connect_timeout,
                auth_event_context,
            )
            .await
            {
                Ok(client) => client,
                Err(err) => {
                    if self.client_epoch() != epoch {
                        debug!(
                            "hysteria2 outbound {} stale connect failed after reset: {err}",
                            self.id
                        );
                        continue;
                    }
                    self.record_connect_failure();
                    error!("hysteria2 outbound {} connect failed: {err}", self.id);
                    return Err(err);
                }
            };
            let mut state = self
                .client_state
                .lock()
                .expect("Hysteria2Outbound client poisoned");
            if state.epoch != epoch {
                drop(state);
                client.close(b"network reset during connect");
                continue;
            }
            state.client = Some(Arc::clone(&client));
            drop(state);
            self.clear_connect_backoff();
            debug!("hysteria2 outbound {} client ready", self.id);
            return Ok(client);
        }
    }

    pub(super) fn connect_backoff_remaining(&self) -> Option<Duration> {
        self.connect_backoff
            .lock()
            .expect("Hysteria2Outbound connect backoff poisoned")
            .remaining(StdInstant::now())
    }

    fn record_connect_failure(&self) {
        self.connect_backoff
            .lock()
            .expect("Hysteria2Outbound connect backoff poisoned")
            .record_failure(StdInstant::now());
    }

    fn clear_connect_backoff(&self) {
        self.connect_backoff
            .lock()
            .expect("Hysteria2Outbound connect backoff poisoned")
            .reset();
    }

    pub(super) fn cached_client(&self) -> Option<Arc<Hysteria2Client>> {
        let mut state = self
            .client_state
            .lock()
            .expect("Hysteria2Outbound client poisoned");
        if let Some(client) = state.client.as_ref()
            && client.is_closed()
        {
            debug!(
                "hysteria2 outbound {} dropping closed cached client",
                self.id
            );
            state.epoch = state.epoch.wrapping_add(1);
            state.client = None;
        }
        state.client.clone()
    }

    fn client_epoch(&self) -> u64 {
        self.client_state
            .lock()
            .expect("Hysteria2Outbound client poisoned")
            .epoch
    }

    fn client_options(&self) -> HammerResult<ClientOptions> {
        validate_hysteria2_tls_options(&self.options.tls)?;
        Ok(ClientOptions {
            server: self.options.server.clone(),
            server_port: self.options.server_port,
            password: self.options.password.clone(),
            server_name: self.options.tls.server_name.clone(),
            insecure: self.options.tls.insecure,
            udp_enabled: self.networks.contains(&Network::Udp),
            bbr_profile: self.options.bbr_profile,
            disable_path_mtu_discovery: self.options.disable_path_mtu_discovery,
            initial_packet_size: self.options.initial_packet_size,
            idle_timeout: self.options.idle_timeout,
            keep_alive_period: self.options.keep_alive_period,
            send_bps: mbps_to_bps(self.options.up_mbps)?,
            receive_bps: mbps_to_bps(self.options.down_mbps)?,
            brutal_debug: self.options.brutal_debug,
            tls: self.options.tls.clone(),
            obfs: self.options.obfs.clone(),
            platform: self.protector.platform(),
        })
    }

    pub(super) async fn resolve_probe_server(&self) -> HammerResult<SocketAddr> {
        resolve_server(&self.options.server, self.options.server_port).await
    }
}

pub(crate) fn build_outbound(
    logger: Logger,
    id: String,
    kind: &OutboundKind,
    protector: SocketProtector,
    control_handle: Option<Arc<ControlThreadHandle>>,
) -> HammerResult<Arc<Hysteria2Outbound>> {
    match kind {
        OutboundKind::Hysteria2(options) => Ok(Arc::new(Hysteria2Outbound::new_with_protector(
            logger,
            id,
            options.clone(),
            protector,
            control_handle,
        ))),
        _ => Err(HammerError::internal(
            "hysteria2 factory received wrong options",
        )),
    }
}

#[async_trait]
impl Outbound for Hysteria2Outbound {
    fn reset(&self) {
        let client = {
            let mut state = self
                .client_state
                .lock()
                .expect("Hysteria2Outbound client poisoned");
            state.epoch = state.epoch.wrapping_add(1);
            state.client.take()
        };
        if let Some(client) = client {
            client.close(b"network reset");
        }
        self.clear_connect_backoff();
    }

    async fn ensure_connected(&self) -> HammerResult<()> {
        self.client().await.map(|_| ())
    }

    #[cfg(feature = "probe")]
    async fn probe_latency(
        &self,
        protocol: &str,
        timeout_duration: Duration,
    ) -> HammerResult<Duration> {
        if protocol != "icmp" {
            return Err(HammerError::internal(format!(
                "{protocol} probe not supported by outbound: {}",
                self.id
            )));
        }

        let protector = self.protector.clone();
        let probe = async {
            let server = self.resolve_probe_server().await?;
            icmp::probe_echo(server.ip(), timeout_duration, protector).await
        };

        match tokio::time::timeout(timeout_duration, probe).await {
            Ok(result) => result,
            Err(_) => Err(HammerError::internal(format!(
                "icmp probe timed out after {}",
                duration_label(timeout_duration)
            ))),
        }
    }

    async fn dial(
        &self,
        network: Network,
        destination: SocksAddr,
        initial_payload: &[u8],
    ) -> HammerResult<Box<dyn ProxyStream>> {
        if !self.networks.contains(&network) {
            return Err(HammerError::internal(format!(
                "{network} is not supported by outbound: {}",
                self.id
            )));
        }
        match network {
            Network::Tcp => Ok(Box::new(
                self.client()
                    .await?
                    .dial_tcp(destination, initial_payload)
                    .await?,
            )),
            Network::Udp => Err(HammerError::internal("use listen_packet for hysteria2 UDP")),
            // Unreachable in practice: `self.networks` only ever
            // contains Tcp/Udp (see `adapter_networks`), so the guard
            // above already filters Icmp out. Kept for exhaustiveness.
            Network::Icmp => Err(HammerError::internal(
                "icmp not supported by hysteria2 outbound",
            )),
        }
    }

    async fn listen_packet(&self) -> HammerResult<Box<dyn ProxyPacketConn>> {
        if !self.networks.contains(&Network::Udp) {
            return Err(HammerError::internal(format!(
                "udp is not supported by outbound: {}",
                self.id
            )));
        }
        Ok(Box::new(self.client().await?.listen_udp().await?))
    }
}
