use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwapOption;
use hammer_adapter::DataWorkerId;
use hammer_core::error::CoreResult;
use hammer_core::protocol::tcp::{TcpCapabilities, TcpConnectionId, TcpNegotiatedOptions, TcpSeq};
use hammer_infra::map::FlatHashTable;

use crate::app::AppIngressTarget;

use super::congestion::TcpCongestionState;
use super::output::{
    DEFAULT_TCP_OUTPUT_PAYLOAD_LEN, TcpOutputRetransmitQueue, TcpOutputSendView,
    tcp_effective_output_payload_len,
};
use super::{TcpLookupId, TcpState};

const DEFAULT_TCP_WINDOW: u32 = u16::MAX as u32;
const DEFAULT_TCP_MAX_SEGMENT_SIZE: u32 = DEFAULT_TCP_OUTPUT_PAYLOAD_LEN as u32;
const TCP_MAX_WINDOW_SCALE: u8 = 14;
pub const TCP_INITIAL_RETRANSMIT_TIMEOUT: Duration = Duration::from_millis(50);
pub const TCP_MIN_RETRANSMIT_TIMEOUT: Duration = Duration::from_millis(50);
pub const TCP_MAX_RETRANSMIT_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpRetransmitTimeoutState {
    srtt: Option<Duration>,
    rttvar: Option<Duration>,
    rto: Duration,
    skip_next_sample: bool,
}

impl TcpRetransmitTimeoutState {
    #[inline]
    pub fn new() -> Self {
        Self {
            srtt: None,
            rttvar: None,
            rto: TCP_INITIAL_RETRANSMIT_TIMEOUT,
            skip_next_sample: false,
        }
    }

    #[inline]
    pub fn smoothed_rtt(&self) -> Option<Duration> {
        self.srtt
    }

    #[inline]
    pub fn rtt_variance(&self) -> Option<Duration> {
        self.rttvar
    }

    #[inline]
    pub fn retransmit_timeout(&self) -> Duration {
        self.rto
    }

    pub fn observe_ack_sample(&mut self, rtt: Duration) -> Duration {
        if self.skip_next_sample {
            self.skip_next_sample = false;
            return self.rto;
        }
        match (self.srtt, self.rttvar) {
            (Some(srtt), Some(rttvar)) => {
                let rtt_delta = duration_abs_diff(srtt, rtt);
                let next_rttvar = duration_weighted_average(rttvar, 3, rtt_delta, 1, 4);
                let next_srtt = duration_weighted_average(srtt, 7, rtt, 1, 8);
                self.srtt = Some(next_srtt);
                self.rttvar = Some(next_rttvar);
            }
            _ => {
                self.srtt = Some(rtt);
                self.rttvar = Some(duration_div(rtt, 2));
            }
        }
        self.rto = retransmit_timeout_from_estimate(
            self.srtt
                .expect("smoothed RTT should be initialized by ACK sample"),
            self.rttvar
                .expect("RTT variance should be initialized by ACK sample"),
        );
        self.rto
    }

    #[inline]
    pub fn on_retransmission_timeout(&mut self) -> Duration {
        self.rto = clamp_retransmit_timeout(duration_mul(self.rto, 2));
        self.skip_next_sample = true;
        self.rto
    }
}

impl Default for TcpRetransmitTimeoutState {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpConnectionOptionState {
    local_capabilities: TcpCapabilities,
    remote_capabilities: Option<TcpCapabilities>,
    negotiated: TcpNegotiatedOptions,
}

impl TcpConnectionOptionState {
    #[inline]
    pub fn new(local_capabilities: TcpCapabilities) -> Self {
        Self {
            local_capabilities: normalize_tcp_capabilities(local_capabilities),
            remote_capabilities: None,
            negotiated: TcpNegotiatedOptions::default(),
        }
    }

    #[inline]
    pub fn local_capabilities(&self) -> TcpCapabilities {
        self.local_capabilities
    }

    #[inline]
    pub fn remote_capabilities(&self) -> Option<TcpCapabilities> {
        self.remote_capabilities
    }

    #[inline]
    pub fn negotiated_options(&self) -> TcpNegotiatedOptions {
        self.negotiated
    }

    #[inline]
    pub fn set_local_capabilities(
        &mut self,
        capabilities: TcpCapabilities,
    ) -> TcpNegotiatedOptions {
        self.local_capabilities = normalize_tcp_capabilities(capabilities);
        self.recalculate_negotiated_options();
        self.negotiated
    }

    #[inline]
    pub fn apply_peer_handshake_capabilities(
        &mut self,
        capabilities: TcpCapabilities,
    ) -> TcpNegotiatedOptions {
        self.remote_capabilities = Some(normalize_tcp_capabilities(capabilities));
        self.recalculate_negotiated_options();
        self.negotiated
    }

    #[inline]
    pub fn effective_send_window_scale(&self) -> u8 {
        tcp_window_scale(self.negotiated.send_window_scale)
    }

    #[inline]
    pub fn effective_receive_window_scale(&self) -> u8 {
        tcp_window_scale(self.negotiated.receive_window_scale)
    }

    #[inline]
    pub fn effective_send_window(&self, advertised_window: u32) -> u32 {
        scaled_window_from_advertised(advertised_window, self.effective_send_window_scale())
    }

    #[inline]
    pub fn advertised_receive_window(&self, receive_window: u32) -> u16 {
        advertised_window_from_receive(receive_window, self.effective_receive_window_scale())
    }

    #[inline]
    fn recalculate_negotiated_options(&mut self) {
        self.negotiated = self
            .remote_capabilities
            .map(|remote| tcp_negotiate_options(self.local_capabilities, remote))
            .unwrap_or_default();
    }
}

impl Default for TcpConnectionOptionState {
    #[inline]
    fn default() -> Self {
        Self::new(TcpCapabilities::default())
    }
}

#[derive(Debug, Clone)]
pub struct TcpDataPlaneConnection {
    lookup_id: TcpLookupId,
    connection_id: Option<TcpConnectionId>,
    owner_worker: DataWorkerId,
    state: TcpState,
    local_port: u16,
    local: Option<SocketAddr>,
    remote: SocketAddr,
    iss: u32,
    irs: u32,
    snd_una: u32,
    snd_nxt: u32,
    snd_wnd: u32,
    rcv_nxt: u32,
    rcv_wnd: u32,
    options: TcpConnectionOptionState,
    output_payload_len: usize,
    retransmit_queue: TcpOutputRetransmitQueue,
    retransmit_timeout: TcpRetransmitTimeoutState,
    congestion: TcpCongestionState,
    next_output_at: Option<std::time::Instant>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpConnectionView {
    pub lookup_id: TcpLookupId,
    pub connection_id: Option<TcpConnectionId>,
    pub owner_worker: DataWorkerId,
    pub state: TcpState,
    pub local_port: u16,
    pub local: Option<SocketAddr>,
    pub remote: SocketAddr,
    pub iss: u32,
    pub irs: u32,
    pub snd_una: u32,
    pub snd_nxt: u32,
    pub snd_wnd: u32,
    pub rcv_nxt: u32,
    pub rcv_wnd: u32,
}

impl TcpDataPlaneConnection {
    #[inline]
    pub fn new(
        lookup_id: TcpLookupId,
        connection_id: Option<TcpConnectionId>,
        owner_worker: DataWorkerId,
        state: TcpState,
        local_port: u16,
        local: Option<SocketAddr>,
        remote: SocketAddr,
    ) -> Self {
        Self {
            lookup_id,
            connection_id,
            owner_worker,
            state,
            local_port,
            local,
            remote,
            iss: 0,
            irs: 0,
            snd_una: 0,
            snd_nxt: 0,
            snd_wnd: DEFAULT_TCP_WINDOW,
            rcv_nxt: 0,
            rcv_wnd: DEFAULT_TCP_WINDOW,
            options: TcpConnectionOptionState::default(),
            output_payload_len: tcp_effective_output_payload_len(None),
            retransmit_queue: TcpOutputRetransmitQueue::new(),
            retransmit_timeout: TcpRetransmitTimeoutState::new(),
            congestion: TcpCongestionState::new(DEFAULT_TCP_MAX_SEGMENT_SIZE),
            next_output_at: None,
        }
    }

    #[inline]
    pub fn lookup_id(&self) -> TcpLookupId {
        self.lookup_id
    }

    #[inline]
    pub fn connection_id(&self) -> Option<TcpConnectionId> {
        self.connection_id
    }

    #[inline]
    pub fn owner_worker(&self) -> DataWorkerId {
        self.owner_worker
    }

    #[inline]
    pub fn state(&self) -> TcpState {
        self.state
    }

    #[inline]
    pub fn local_port(&self) -> u16 {
        self.local_port
    }

    #[inline]
    pub fn local(&self) -> Option<SocketAddr> {
        self.local
    }

    #[inline]
    pub fn remote(&self) -> SocketAddr {
        self.remote
    }

    #[inline]
    pub fn iss(&self) -> u32 {
        self.iss
    }

    #[inline]
    pub fn irs(&self) -> u32 {
        self.irs
    }

    #[inline]
    pub fn snd_una(&self) -> u32 {
        self.snd_una
    }

    #[inline]
    pub fn snd_nxt(&self) -> u32 {
        self.snd_nxt
    }

    #[inline]
    pub fn snd_wnd(&self) -> u32 {
        self.snd_wnd
    }

    #[inline]
    pub fn rcv_nxt(&self) -> u32 {
        self.rcv_nxt
    }

    #[inline]
    pub fn rcv_wnd(&self) -> u32 {
        self.rcv_wnd
    }

    #[inline]
    pub fn output_payload_len(&self) -> usize {
        self.output_payload_len
    }

    #[inline]
    pub fn apply_peer_max_segment_size(&mut self, max_segment_size: Option<u16>) {
        if let Some(max_segment_size) =
            max_segment_size.filter(|max_segment_size| *max_segment_size != 0)
        {
            self.output_payload_len = tcp_effective_output_payload_len(Some(max_segment_size));
            self.congestion = TcpCongestionState::new(u32::from(max_segment_size));
        }
    }

    #[inline]
    pub fn option_state(&self) -> &TcpConnectionOptionState {
        &self.options
    }

    #[inline]
    pub fn option_state_mut(&mut self) -> &mut TcpConnectionOptionState {
        &mut self.options
    }

    #[inline]
    pub fn local_capabilities(&self) -> TcpCapabilities {
        self.options.local_capabilities()
    }

    #[inline]
    pub fn remote_capabilities(&self) -> Option<TcpCapabilities> {
        self.options.remote_capabilities()
    }

    #[inline]
    pub fn negotiated_options(&self) -> TcpNegotiatedOptions {
        self.options.negotiated_options()
    }

    #[inline]
    pub fn set_local_capabilities(
        &mut self,
        capabilities: TcpCapabilities,
    ) -> TcpNegotiatedOptions {
        self.options.set_local_capabilities(capabilities)
    }

    #[inline]
    pub fn apply_peer_handshake_capabilities(
        &mut self,
        capabilities: TcpCapabilities,
    ) -> TcpNegotiatedOptions {
        let negotiated = self.options.apply_peer_handshake_capabilities(capabilities);
        self.apply_peer_max_segment_size(negotiated.send_max_segment_size);
        negotiated
    }

    #[inline]
    pub fn effective_send_window_scale(&self) -> u8 {
        self.options.effective_send_window_scale()
    }

    #[inline]
    pub fn effective_receive_window_scale(&self) -> u8 {
        self.options.effective_receive_window_scale()
    }

    #[inline]
    pub fn effective_send_window(&self, advertised_window: u32) -> u32 {
        self.options.effective_send_window(advertised_window)
    }

    #[inline]
    pub fn advertised_receive_window(&self, receive_window: u32) -> u16 {
        self.options.advertised_receive_window(receive_window)
    }

    #[inline]
    pub fn congestion(&self) -> &TcpCongestionState {
        &self.congestion
    }

    #[inline]
    pub fn congestion_mut(&mut self) -> &mut TcpCongestionState {
        &mut self.congestion
    }

    #[inline]
    pub fn retransmit_queue(&self) -> &TcpOutputRetransmitQueue {
        &self.retransmit_queue
    }

    #[inline]
    pub fn retransmit_queue_mut(&mut self) -> &mut TcpOutputRetransmitQueue {
        &mut self.retransmit_queue
    }

    #[inline]
    pub fn retransmit_timeout(&self) -> &TcpRetransmitTimeoutState {
        &self.retransmit_timeout
    }

    #[inline]
    pub fn retransmit_timeout_mut(&mut self) -> &mut TcpRetransmitTimeoutState {
        &mut self.retransmit_timeout
    }

    #[inline]
    pub fn output_send_view(&self) -> TcpOutputSendView {
        TcpOutputSendView {
            snd_una: self.snd_una,
            snd_nxt: self.snd_nxt,
            snd_wnd: self.snd_wnd,
            congestion_window: self.congestion.congestion_window(),
        }
    }

    #[inline]
    pub fn view(&self) -> TcpConnectionView {
        TcpConnectionView {
            lookup_id: self.lookup_id,
            connection_id: self.connection_id,
            owner_worker: self.owner_worker,
            state: self.state,
            local_port: self.local_port,
            local: self.local,
            remote: self.remote,
            iss: self.iss,
            irs: self.irs,
            snd_una: self.snd_una,
            snd_nxt: self.snd_nxt,
            snd_wnd: self.snd_wnd,
            rcv_nxt: self.rcv_nxt,
            rcv_wnd: self.rcv_wnd,
        }
    }

    #[inline]
    pub fn set_send_state(&mut self, snd_una: u32, snd_nxt: u32, snd_wnd: u32) {
        self.snd_una = snd_una;
        self.snd_nxt = snd_nxt;
        self.snd_wnd = self.effective_send_window(snd_wnd);
    }

    #[inline]
    pub fn set_receive_state(&mut self, rcv_nxt: u32, rcv_wnd: u32) {
        self.rcv_nxt = rcv_nxt;
        self.rcv_wnd = rcv_wnd;
    }

    #[inline]
    pub fn set_sequence_state(
        &mut self,
        iss: u32,
        irs: u32,
        snd_una: u32,
        snd_nxt: u32,
        snd_wnd: u32,
        rcv_nxt: u32,
        rcv_wnd: u32,
    ) {
        self.iss = iss;
        self.irs = irs;
        self.snd_una = snd_una;
        self.snd_nxt = snd_nxt;
        self.snd_wnd = snd_wnd;
        self.rcv_nxt = rcv_nxt;
        self.rcv_wnd = rcv_wnd;
    }

    #[inline]
    pub fn set_state(&mut self, state: TcpState) {
        self.state = state;
    }

    #[inline]
    pub(crate) fn apply_receive_progress(&mut self, progress: TcpReceiveProgress) {
        if self.irs == 0 && self.rcv_nxt == 0 {
            self.irs = progress.sequence.wrapping_sub(1);
            self.rcv_nxt = progress.sequence;
        }
        if let Some(acknowledgment) = progress.acknowledgment {
            if self.iss == 0 && self.snd_una == 0 && self.snd_nxt == 0 {
                self.iss = acknowledgment.wrapping_sub(1);
            }
            self.snd_una = tcp_seq_max(self.snd_una, acknowledgment);
            self.snd_nxt = tcp_seq_max(self.snd_nxt, self.snd_una);
            self.snd_wnd = self.effective_send_window(progress.advertised_window);
        }
        if let Some(next_receive_sequence) = progress.next_receive_sequence
            && self.rcv_nxt == progress.sequence
        {
            self.rcv_nxt = next_receive_sequence;
        }
        if let Some(state) = progress.state {
            self.state = state;
        }
    }

    #[inline]
    pub fn next_output_at(&self) -> Option<std::time::Instant> {
        self.next_output_at
    }

    #[inline]
    pub fn set_next_output_at(&mut self, deadline: Option<std::time::Instant>) {
        self.next_output_at = deadline;
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct TcpReceiveProgress {
    pub(crate) state: Option<TcpState>,
    pub(crate) sequence: u32,
    pub(crate) acknowledgment: Option<u32>,
    pub(crate) advertised_window: u32,
    pub(crate) next_receive_sequence: Option<u32>,
}

pub trait TcpSessionAccess: Send + Sync {
    #[inline]
    fn connection_view(&self, _lookup_id: TcpLookupId) -> CoreResult<Option<TcpConnectionView>> {
        Ok(None)
    }

    #[inline]
    fn apply_receive_progress(
        &self,
        _lookup_id: TcpLookupId,
        _progress: TcpReceiveProgress,
    ) -> CoreResult<()> {
        Ok(())
    }

    #[inline]
    fn target_for_lookup(&self, _lookup_id: TcpLookupId) -> CoreResult<Option<AppIngressTarget>> {
        Ok(None)
    }
}

struct TcpSessionAccessHandle {
    raw: *const (),
    clone_raw: fn(*const ()) -> *const (),
    drop_raw: fn(*const ()),
    connection_view: fn(*const (), TcpLookupId) -> CoreResult<Option<TcpConnectionView>>,
    apply_receive_progress: fn(*const (), TcpLookupId, TcpReceiveProgress) -> CoreResult<()>,
    target_for_lookup: fn(*const (), TcpLookupId) -> CoreResult<Option<AppIngressTarget>>,
}

unsafe impl Send for TcpSessionAccessHandle {}
unsafe impl Sync for TcpSessionAccessHandle {}

impl Default for TcpSessionAccessHandle {
    #[inline]
    fn default() -> Self {
        Self {
            raw: std::ptr::null(),
            clone_raw: clone_noop_session_access,
            drop_raw: drop_noop_session_access,
            connection_view: noop_session_connection_view,
            apply_receive_progress: noop_session_apply_receive_progress,
            target_for_lookup: noop_session_target_for_lookup,
        }
    }
}

impl Clone for TcpSessionAccessHandle {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            raw: (self.clone_raw)(self.raw),
            clone_raw: self.clone_raw,
            drop_raw: self.drop_raw,
            connection_view: self.connection_view,
            apply_receive_progress: self.apply_receive_progress,
            target_for_lookup: self.target_for_lookup,
        }
    }
}

impl Drop for TcpSessionAccessHandle {
    #[inline]
    fn drop(&mut self) {
        (self.drop_raw)(self.raw);
    }
}

impl TcpSessionAccessHandle {
    #[inline]
    fn new<O>(access: Arc<O>) -> Self
    where
        O: TcpSessionAccess + 'static,
    {
        Self {
            raw: Arc::into_raw(access) as *const (),
            clone_raw: clone_session_access_arc::<O>,
            drop_raw: drop_session_access_arc::<O>,
            connection_view: session_connection_view_with::<O>,
            apply_receive_progress: session_apply_receive_progress_with::<O>,
            target_for_lookup: session_target_for_lookup_with::<O>,
        }
    }
}

#[inline]
fn clone_noop_session_access(_raw: *const ()) -> *const () {
    std::ptr::null()
}

#[inline]
fn drop_noop_session_access(_raw: *const ()) {}

#[inline]
fn noop_session_connection_view(
    _raw: *const (),
    _lookup_id: TcpLookupId,
) -> CoreResult<Option<TcpConnectionView>> {
    Ok(None)
}

#[inline]
fn noop_session_apply_receive_progress(
    _raw: *const (),
    _lookup_id: TcpLookupId,
    _progress: TcpReceiveProgress,
) -> CoreResult<()> {
    Ok(())
}

#[inline]
fn noop_session_target_for_lookup(
    _raw: *const (),
    _lookup_id: TcpLookupId,
) -> CoreResult<Option<AppIngressTarget>> {
    Ok(None)
}

#[inline]
fn clone_session_access_arc<O>(raw: *const ()) -> *const ()
where
    O: TcpSessionAccess + 'static,
{
    let raw = raw.cast::<O>();
    if !raw.is_null() {
        unsafe {
            Arc::increment_strong_count(raw);
        }
    }
    raw.cast()
}

#[inline]
fn drop_session_access_arc<O>(raw: *const ())
where
    O: TcpSessionAccess + 'static,
{
    let raw = raw.cast::<O>();
    if !raw.is_null() {
        unsafe {
            drop(Arc::from_raw(raw));
        }
    }
}

#[inline]
fn session_connection_view_with<O>(
    raw: *const (),
    lookup_id: TcpLookupId,
) -> CoreResult<Option<TcpConnectionView>>
where
    O: TcpSessionAccess + 'static,
{
    let raw = raw.cast::<O>();
    if raw.is_null() {
        return Ok(None);
    }
    unsafe { (&*raw).connection_view(lookup_id) }
}

#[inline]
fn session_apply_receive_progress_with<O>(
    raw: *const (),
    lookup_id: TcpLookupId,
    progress: TcpReceiveProgress,
) -> CoreResult<()>
where
    O: TcpSessionAccess + 'static,
{
    let raw = raw.cast::<O>();
    if raw.is_null() {
        return Ok(());
    }
    unsafe { (&*raw).apply_receive_progress(lookup_id, progress) }
}

#[inline]
fn session_target_for_lookup_with<O>(
    raw: *const (),
    lookup_id: TcpLookupId,
) -> CoreResult<Option<AppIngressTarget>>
where
    O: TcpSessionAccess + 'static,
{
    let raw = raw.cast::<O>();
    if raw.is_null() {
        return Ok(None);
    }
    unsafe { (&*raw).target_for_lookup(lookup_id) }
}

#[derive(Clone, Default)]
pub struct TcpSessionAccessSlot {
    inner: Arc<ArcSwapOption<TcpSessionAccessHandle>>,
}

impl TcpSessionAccessSlot {
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn install<O>(&self, access: Arc<O>)
    where
        O: TcpSessionAccess + 'static,
    {
        self.inner
            .store(Some(Arc::new(TcpSessionAccessHandle::new(access))));
    }

    #[inline]
    pub fn connection_view(&self, lookup_id: TcpLookupId) -> CoreResult<Option<TcpConnectionView>> {
        let access = self.load();
        (access.connection_view)(access.raw, lookup_id)
    }

    #[inline]
    pub(crate) fn apply_receive_progress(
        &self,
        lookup_id: TcpLookupId,
        progress: TcpReceiveProgress,
    ) -> CoreResult<()> {
        let access = self.load();
        (access.apply_receive_progress)(access.raw, lookup_id, progress)
    }

    #[inline]
    pub fn target_for_lookup(
        &self,
        lookup_id: TcpLookupId,
    ) -> CoreResult<Option<AppIngressTarget>> {
        let access = self.load();
        (access.target_for_lookup)(access.raw, lookup_id)
    }

    #[inline]
    fn load(&self) -> TcpSessionAccessHandle {
        self.inner
            .load_full()
            .as_deref()
            .cloned()
            .unwrap_or_default()
    }
}

#[derive(Debug, Clone)]
pub struct TcpConnectionTable {
    connections: hammer_infra::vec::Vec<TcpDataPlaneConnection>,
    lookup_slots: FlatHashTable<TcpLookupId, usize>,
    connection_slots: FlatHashTable<u64, usize>,
}

impl TcpConnectionTable {
    #[inline]
    pub fn empty() -> Self {
        Self {
            connections: hammer_infra::vec::Vec::new(),
            lookup_slots: FlatHashTable::new(),
            connection_slots: FlatHashTable::new(),
        }
    }

    #[inline]
    pub fn insert(&mut self, connection: TcpDataPlaneConnection) {
        let slot = self.connections.len();
        self.lookup_slots.insert(connection.lookup_id(), slot);
        if let Some(connection_id) = connection.connection_id() {
            self.connection_slots.insert(connection_id.get(), slot);
        }
        self.connections.push(connection);
    }

    #[inline]
    pub fn upsert(&mut self, connection: TcpDataPlaneConnection) {
        if let Some(slot) = self.lookup_slots.lookup(&connection.lookup_id()) {
            self.connections[slot] = connection.clone();
            if let Some(connection_id) = connection.connection_id() {
                self.connection_slots.insert(connection_id.get(), slot);
            }
            return;
        }
        self.insert(connection);
    }

    #[inline]
    pub fn lookup_by_lookup_id(&self, lookup_id: TcpLookupId) -> Option<&TcpDataPlaneConnection> {
        self.lookup_slots
            .lookup(&lookup_id)
            .and_then(|slot| self.connections.get(slot))
    }

    #[inline]
    pub fn lookup_by_lookup_id_mut(
        &mut self,
        lookup_id: TcpLookupId,
    ) -> Option<&mut TcpDataPlaneConnection> {
        let slot = self.lookup_slots.lookup(&lookup_id)?;
        self.connections.get_mut(slot)
    }

    #[inline]
    pub fn lookup_by_connection_id(
        &self,
        connection_id: TcpConnectionId,
    ) -> Option<&TcpDataPlaneConnection> {
        self.connection_slots
            .lookup(&connection_id.get())
            .and_then(|slot| self.connections.get(slot))
    }

    #[inline]
    pub fn lookup_by_connection_id_mut(
        &mut self,
        connection_id: TcpConnectionId,
    ) -> Option<&mut TcpDataPlaneConnection> {
        let slot = self.connection_slots.lookup(&connection_id.get())?;
        self.connections.get_mut(slot)
    }
}

impl Default for TcpConnectionTable {
    #[inline]
    fn default() -> Self {
        Self::empty()
    }
}

#[inline]
fn tcp_seq_max(current: u32, candidate: u32) -> u32 {
    if current == 0 || TcpSeq::new(current).before(TcpSeq::new(candidate)) {
        candidate
    } else {
        current
    }
}

#[inline]
fn tcp_negotiate_options(local: TcpCapabilities, remote: TcpCapabilities) -> TcpNegotiatedOptions {
    let (send_window_scale, receive_window_scale) = match (local.window_scale, remote.window_scale)
    {
        (Some(local_scale), Some(remote_scale)) => (Some(remote_scale), Some(local_scale)),
        _ => (None, None),
    };
    TcpNegotiatedOptions {
        send_max_segment_size: remote.max_segment_size,
        receive_max_segment_size: local.max_segment_size,
        send_window_scale,
        receive_window_scale,
        sack: local.sack && remote.sack,
        timestamps: local.timestamps && remote.timestamps,
        ecn: local.ecn && remote.ecn,
    }
}

#[inline]
fn normalize_tcp_capabilities(capabilities: TcpCapabilities) -> TcpCapabilities {
    TcpCapabilities {
        max_segment_size: capabilities
            .max_segment_size
            .filter(|max_segment_size| *max_segment_size != 0),
        window_scale: capabilities
            .window_scale
            .map(|scale| scale.min(TCP_MAX_WINDOW_SCALE)),
        sack: capabilities.sack,
        timestamps: capabilities.timestamps,
        ecn: capabilities.ecn,
    }
}

#[inline]
fn tcp_window_scale(window_scale: Option<u8>) -> u8 {
    window_scale.unwrap_or_default().min(TCP_MAX_WINDOW_SCALE)
}

#[inline]
fn scaled_window_from_advertised(advertised_window: u32, window_scale: u8) -> u32 {
    advertised_window.saturating_mul(1_u32 << tcp_window_scale(Some(window_scale)))
}

#[inline]
fn advertised_window_from_receive(receive_window: u32, window_scale: u8) -> u16 {
    (receive_window >> tcp_window_scale(Some(window_scale))).min(u32::from(u16::MAX)) as u16
}

#[inline]
fn retransmit_timeout_from_estimate(srtt: Duration, rttvar: Duration) -> Duration {
    clamp_retransmit_timeout(duration_add(srtt, duration_mul(rttvar, 4)))
}

#[inline]
fn clamp_retransmit_timeout(timeout: Duration) -> Duration {
    if timeout < TCP_MIN_RETRANSMIT_TIMEOUT {
        TCP_MIN_RETRANSMIT_TIMEOUT
    } else if timeout > TCP_MAX_RETRANSMIT_TIMEOUT {
        TCP_MAX_RETRANSMIT_TIMEOUT
    } else {
        timeout
    }
}

#[inline]
fn duration_weighted_average(
    left: Duration,
    left_weight: u32,
    right: Duration,
    right_weight: u32,
    denominator: u32,
) -> Duration {
    duration_from_nanos_saturating(
        left.as_nanos()
            .saturating_mul(u128::from(left_weight))
            .saturating_add(right.as_nanos().saturating_mul(u128::from(right_weight)))
            / u128::from(denominator),
    )
}

#[inline]
fn duration_add(left: Duration, right: Duration) -> Duration {
    duration_from_nanos_saturating(left.as_nanos().saturating_add(right.as_nanos()))
}

#[inline]
fn duration_mul(duration: Duration, factor: u32) -> Duration {
    duration_from_nanos_saturating(duration.as_nanos().saturating_mul(u128::from(factor)))
}

#[inline]
fn duration_div(duration: Duration, divisor: u32) -> Duration {
    duration_from_nanos_saturating(duration.as_nanos() / u128::from(divisor))
}

#[inline]
fn duration_abs_diff(left: Duration, right: Duration) -> Duration {
    if left >= right {
        left - right
    } else {
        right - left
    }
}

#[inline]
fn duration_from_nanos_saturating(nanos: u128) -> Duration {
    const NANOS_PER_SECOND: u128 = 1_000_000_000;
    let seconds = nanos / NANOS_PER_SECOND;
    let subsecond_nanos = (nanos % NANOS_PER_SECOND) as u32;
    if seconds > u128::from(u64::MAX) {
        Duration::new(u64::MAX, 999_999_999)
    } else {
        Duration::new(seconds as u64, subsecond_nanos)
    }
}
