use std::collections::VecDeque;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use hammer_infra::vec::Vec;

#[cfg(feature = "amneziawg")]
use boringtun::noise::AmneziaConfig;
use boringtun::noise::errors::WireGuardError;
use boringtun::noise::{
    DataSession, Packet, SESSION_RING_SIZE, Tunn, TunnControlResult, TunnResult, TunnTimerResult,
};
use boringtun::x25519;
use ipnet::IpNet;

use crate::config::WireguardPeerOptions;

/// WireGuard peer metadata used by Hammer outside the mutable tunnel state.
pub struct Peer {
    public_key: [u8; 32],
    allowed_ips: Vec<IpNet>,
    endpoint: SocketAddr,
    reserved: [u8; 3],
}

/// Mutable boringtun state for one peer.
///
/// This type is intentionally separate from [`Peer`]. Runtime code should keep
/// it owned by one transport actor instead of sharing it across threads.
pub struct PeerTunnel {
    tunn: Tunn,
}

pub struct PeerDataSessionUpdate {
    pub generation: u64,
    pub session: DataSession,
}

struct PeerDataSession {
    generation: u64,
    session: DataSession,
}

pub struct PeerDataTunnel {
    sessions: [Option<PeerDataSession>; SESSION_RING_SIZE],
    current: Option<usize>,
    packet_queue: VecDeque<Vec<u8>>,
}

#[derive(Debug)]
pub enum PeerDataResult<'a> {
    Done,
    NeedHandshake,
    Err(WireGuardError),
    WriteToNetwork(&'a mut [u8]),
    WriteToTunnelV4(&'a mut [u8], Ipv4Addr),
    WriteToTunnelV6(&'a mut [u8], Ipv6Addr),
}

impl Peer {
    pub fn from_options(opts: &WireguardPeerOptions) -> Self {
        Self {
            public_key: opts.public_key,
            allowed_ips: opts.allowed_ips.clone(),
            endpoint: opts.endpoint,
            reserved: opts.reserved,
        }
    }

    pub fn public_key(&self) -> [u8; 32] {
        self.public_key
    }

    pub fn endpoint(&self) -> SocketAddr {
        self.endpoint
    }

    pub fn reserved(&self) -> [u8; 3] {
        self.reserved
    }

    pub fn allowed_ips(&self) -> &[IpNet] {
        &self.allowed_ips
    }

    /// Longest-prefix match against this peer's `allowed_ips`. Returns the
    /// matching prefix length so the caller can pick the most specific peer
    /// when multiple have overlapping ranges.
    pub fn match_prefix(&self, dst: IpAddr) -> Option<u8> {
        self.allowed_ips
            .iter()
            .filter(|net| net.contains(&dst))
            .map(|net| net.prefix_len())
            .max()
    }
}

impl PeerTunnel {
    pub fn new(
        opts: &WireguardPeerOptions,
        local_private: &x25519::StaticSecret,
        index: u32,
        #[cfg(feature = "amneziawg")] amnezia: Option<AmneziaConfig>,
    ) -> Self {
        let public_key = x25519::PublicKey::from(opts.public_key);
        // boringtun stores the keepalive interval as `u16` seconds; clamp the
        // configured Duration into that window. None disables keepalive.
        let keepalive = opts
            .persistent_keepalive
            .map(|d| d.as_secs().min(u16::MAX as u64) as u16);
        let tunn = Tunn::new(
            local_private.clone(),
            public_key,
            opts.pre_shared_key,
            keepalive,
            index,
            None, // rate_limiter: None lets boringtun build a default per-peer one
            #[cfg(feature = "amneziawg")]
            amnezia,
        );
        Self { tunn }
    }

    pub fn encapsulate<'a>(&mut self, src: &[u8], dst: &'a mut [u8]) -> TunnResult<'a> {
        self.tunn.encapsulate(src, dst)
    }

    pub fn start_handshake<'a>(&mut self, dst: &'a mut [u8], force: bool) -> TunnResult<'a> {
        self.tunn.format_handshake_initiation(dst, force)
    }

    pub fn decapsulate<'a>(
        &mut self,
        src_addr: Option<IpAddr>,
        datagram: &[u8],
        dst: &'a mut [u8],
    ) -> TunnResult<'a> {
        self.tunn.decapsulate(src_addr, datagram, dst)
    }

    pub fn update_timers<'a>(&mut self, dst: &'a mut [u8]) -> TunnResult<'a> {
        self.tunn.update_timers(dst)
    }

    pub fn decapsulate_control<'a>(
        &mut self,
        src_addr: Option<IpAddr>,
        datagram: &[u8],
        dst: &'a mut [u8],
    ) -> TunnControlResult<'a> {
        self.tunn.decapsulate_control(src_addr, datagram, dst)
    }

    pub fn update_timers_control<'a>(&mut self, dst: &'a mut [u8]) -> TunnTimerResult<'a> {
        self.tunn.update_timers_control(dst)
    }

    pub fn record_data_plane_packet_sent(&mut self, plaintext_len: usize) {
        self.tunn.record_data_plane_packet_sent(plaintext_len);
    }

    pub fn record_data_plane_packet_received(&mut self, plaintext_len: usize) {
        self.tunn.record_data_plane_packet_received(plaintext_len);
    }
}

impl PeerDataTunnel {
    pub fn new() -> Self {
        Self {
            sessions: std::array::from_fn(|_| None),
            current: None,
            packet_queue: VecDeque::new(),
        }
    }

    pub fn install_session(&mut self, update: PeerDataSessionUpdate) {
        let local_index = update.session.local_index();
        let slot = local_index % SESSION_RING_SIZE;
        if self.sessions[slot]
            .as_ref()
            .is_some_and(|session| session.generation > update.generation)
        {
            return;
        }

        self.sessions[slot] = Some(PeerDataSession {
            generation: update.generation,
            session: update.session,
        });

        if self
            .current_generation()
            .is_none_or(|generation| update.generation >= generation)
        {
            self.current = Some(local_index);
        }
    }

    pub fn expire_session(&mut self, local_index: usize, generation: u64) {
        let slot = local_index % SESSION_RING_SIZE;
        if self.sessions[slot].as_ref().is_some_and(|session| {
            session.session.local_index() == local_index && session.generation == generation
        }) {
            self.sessions[slot] = None;
            if self.current == Some(local_index) {
                self.current = self.latest_session_index();
            }
        }
    }

    pub fn encapsulate<'a>(&mut self, src: &[u8], dst: &'a mut [u8]) -> PeerDataResult<'a> {
        let Some(session) = self.current_session() else {
            self.queue_packet(src);
            return PeerDataResult::NeedHandshake;
        };
        PeerDataResult::WriteToNetwork(session.format_packet_data(src, dst))
    }

    pub fn encapsulate_keepalive<'a>(&mut self, dst: &'a mut [u8]) -> PeerDataResult<'a> {
        let Some(session) = self.current_session() else {
            return PeerDataResult::NeedHandshake;
        };
        PeerDataResult::WriteToNetwork(session.format_packet_data(&[], dst))
    }

    pub fn encapsulate_next_queued<'a>(&mut self, dst: &'a mut [u8]) -> PeerDataResult<'a> {
        self.encapsulate_next_queued_with_len(dst).0
    }

    pub fn encapsulate_next_queued_with_len<'a>(
        &mut self,
        dst: &'a mut [u8],
    ) -> (PeerDataResult<'a>, usize) {
        let Some(packet) = self.packet_queue.pop_front() else {
            return (PeerDataResult::Done, 0);
        };
        let plaintext_len = packet.len();
        match self.encapsulate(&packet, dst) {
            PeerDataResult::NeedHandshake | PeerDataResult::Err(_) => {
                self.requeue_packet(packet);
                (PeerDataResult::NeedHandshake, plaintext_len)
            }
            result => (result, plaintext_len),
        }
    }

    pub fn decapsulate<'a>(&mut self, datagram: &[u8], dst: &'a mut [u8]) -> PeerDataResult<'a> {
        let packet = match Tunn::parse_incoming_packet(datagram) {
            Ok(Packet::PacketData(packet)) => packet,
            Ok(_) => return PeerDataResult::Done,
            Err(err) => return PeerDataResult::Err(err),
        };
        let remote_index = packet.receiver_idx as usize;
        let slot = remote_index % SESSION_RING_SIZE;
        let Some(session) = self.sessions[slot].as_ref() else {
            return PeerDataResult::Err(WireGuardError::NoCurrentSession);
        };
        if session.session.local_index() != remote_index {
            return PeerDataResult::Err(WireGuardError::WrongIndex);
        }

        let decrypted = match session.session.receive_packet_data(packet, dst) {
            Ok(decrypted) => decrypted,
            Err(err) => return PeerDataResult::Err(err),
        };
        if self
            .current_generation()
            .is_none_or(|generation| session.generation >= generation)
        {
            self.current = Some(remote_index);
        }
        Self::from_tunn_result(Tunn::validate_decapsulated_packet(decrypted))
    }

    fn current_session(&self) -> Option<&DataSession> {
        let index = self.current?;
        let session = self.sessions[index % SESSION_RING_SIZE].as_ref()?;
        if session.session.local_index() == index {
            Some(&session.session)
        } else {
            None
        }
    }

    fn current_generation(&self) -> Option<u64> {
        let index = self.current?;
        let session = self.sessions[index % SESSION_RING_SIZE].as_ref()?;
        if session.session.local_index() == index {
            Some(session.generation)
        } else {
            None
        }
    }

    fn latest_session_index(&self) -> Option<usize> {
        self.sessions
            .iter()
            .filter_map(|session| {
                session
                    .as_ref()
                    .map(|session| (session.session.local_index(), session.generation))
            })
            .max_by_key(|(_, generation)| *generation)
            .map(|(index, _)| index)
    }

    fn queue_packet(&mut self, packet: &[u8]) {
        if self.packet_queue.len() < 256 {
            self.packet_queue.push_back(Vec::from(packet));
        }
    }

    fn requeue_packet(&mut self, packet: Vec<u8>) {
        if self.packet_queue.len() < 256 {
            self.packet_queue.push_front(packet);
        }
    }

    fn from_tunn_result<'a>(result: TunnResult<'a>) -> PeerDataResult<'a> {
        match result {
            TunnResult::Done => PeerDataResult::Done,
            TunnResult::Err(err) => PeerDataResult::Err(err),
            TunnResult::WriteToNetwork(out) => PeerDataResult::WriteToNetwork(out),
            TunnResult::WriteToTunnelV4(out, addr) => PeerDataResult::WriteToTunnelV4(out, addr),
            TunnResult::WriteToTunnelV6(out, addr) => PeerDataResult::WriteToTunnelV6(out, addr),
        }
    }
}

impl Default for PeerDataTunnel {
    fn default() -> Self {
        Self::new()
    }
}

/// Pick the peer that has the longest matching `allowed_ips` prefix for `dst`.
/// Returns the peer's index in the input slice — `None` when nothing matches,
/// which the caller should surface as "no route" (drop the packet).
pub fn route_outbound(peers: &[Peer], dst: IpAddr) -> Option<usize> {
    peers
        .iter()
        .enumerate()
        .filter_map(|(idx, peer)| peer.match_prefix(dst).map(|len| (idx, len)))
        .max_by_key(|(_, len)| *len)
        .map(|(idx, _)| idx)
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use boringtun::noise::{Packet, TunnControlResult, TunnResult};
    use hammer_infra::vec::Vec;

    use super::*;

    fn public_key(secret: [u8; 32]) -> [u8; 32] {
        x25519::PublicKey::from(&x25519::StaticSecret::from(secret)).to_bytes()
    }

    fn peer_options(peer_pub: [u8; 32], endpoint: SocketAddr) -> WireguardPeerOptions {
        WireguardPeerOptions {
            public_key: peer_pub,
            pre_shared_key: None,
            endpoint,
            allowed_ips: hammer_infra::vec!["10.0.0.0/8".parse().unwrap()],
            persistent_keepalive: None,
            reserved: [0; 3],
        }
    }

    fn dummy_ipv4_packet(last_octet: u8) -> Vec<u8> {
        let mut pkt = hammer_infra::vec![0u8; 60];
        pkt[0] = 0x45;
        pkt[2..4].copy_from_slice(&60u16.to_be_bytes());
        pkt[8] = 64;
        pkt[9] = 17;
        pkt[12..16].copy_from_slice(&[10, 0, 0, 1]);
        pkt[16..20].copy_from_slice(&[10, 0, 0, last_octet]);
        pkt
    }

    fn handshake(
        initiator: &mut PeerTunnel,
        responder: &mut PeerTunnel,
        generation: u64,
        initiator_data: &mut PeerDataTunnel,
        responder_data: &mut PeerDataTunnel,
    ) {
        let mut init_buf = hammer_infra::vec![0u8; 2048];
        let init = match initiator.start_handshake(&mut init_buf, true) {
            TunnResult::WriteToNetwork(out) => out.to_vec(),
            other => panic!("expected handshake init, got {:?}", other),
        };

        let mut response_buf = hammer_infra::vec![0u8; 2048];
        let response = match responder.decapsulate_control(None, &init, &mut response_buf) {
            TunnControlResult::WriteToNetworkAndInstallSession { packet, session } => {
                responder_data.install_session(PeerDataSessionUpdate {
                    generation,
                    session,
                });
                packet.to_vec()
            }
            other => panic!("expected responder session, got {:?}", other),
        };

        let mut final_buf = hammer_infra::vec![0u8; 2048];
        match initiator.decapsulate_control(None, &response, &mut final_buf) {
            TunnControlResult::InstallSession { session, .. } => {
                initiator_data.install_session(PeerDataSessionUpdate {
                    generation,
                    session,
                });
            }
            other => panic!("expected initiator session, got {:?}", other),
        }
    }

    #[test]
    fn data_tunnel_keeps_old_rx_session_while_new_generation_is_tx_current() {
        let a_priv = [1u8; 32];
        let b_priv = [2u8; 32];
        let a_opts = peer_options(
            public_key(b_priv),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 51820),
        );
        let b_opts = peer_options(
            public_key(a_priv),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 51821),
        );
        let mut a_control = PeerTunnel::new(&a_opts, &x25519::StaticSecret::from(a_priv), 0);
        let mut b_control = PeerTunnel::new(&b_opts, &x25519::StaticSecret::from(b_priv), 1);
        let mut a_data = PeerDataTunnel::new();
        let mut b_data = PeerDataTunnel::new();

        handshake(&mut a_control, &mut b_control, 1, &mut a_data, &mut b_data);

        let old_packet_1 = dummy_ipv4_packet(5);
        let old_packet_2 = dummy_ipv4_packet(6);
        let mut old_buf_1 = hammer_infra::vec![0u8; 2048];
        let old_encrypted_1 = match a_data.encapsulate(&old_packet_1, &mut old_buf_1) {
            PeerDataResult::WriteToNetwork(out) => out.to_vec(),
            other => panic!("old packet 1 should encrypt, got {:?}", other),
        };
        let mut old_buf_2 = hammer_infra::vec![0u8; 2048];
        let old_encrypted_2 = match a_data.encapsulate(&old_packet_2, &mut old_buf_2) {
            PeerDataResult::WriteToNetwork(out) => out.to_vec(),
            other => panic!("old packet 2 should encrypt, got {:?}", other),
        };

        let old_receiver_index = match Tunn::parse_incoming_packet(&old_encrypted_1)
            .expect("parse old encrypted packet")
        {
            Packet::PacketData(packet) => packet.receiver_idx as usize,
            other => panic!("expected old data packet, got {:?}", other),
        };

        handshake(&mut a_control, &mut b_control, 2, &mut a_data, &mut b_data);

        let new_packet = dummy_ipv4_packet(7);
        let mut new_buf = hammer_infra::vec![0u8; 2048];
        let new_encrypted = match a_data.encapsulate(&new_packet, &mut new_buf) {
            PeerDataResult::WriteToNetwork(out) => out.to_vec(),
            other => panic!("new packet should encrypt, got {:?}", other),
        };
        let new_receiver_index = match Tunn::parse_incoming_packet(&new_encrypted)
            .expect("parse new encrypted packet")
        {
            Packet::PacketData(packet) => packet.receiver_idx as usize,
            other => panic!("expected new data packet, got {:?}", other),
        };
        assert_ne!(
            old_receiver_index, new_receiver_index,
            "rekey must switch tx to the newly installed session"
        );

        let mut old_plain = hammer_infra::vec![0u8; 2048];
        match b_data.decapsulate(&old_encrypted_1, &mut old_plain) {
            PeerDataResult::WriteToTunnelV4(out, _) => assert_eq!(out, old_packet_1),
            other => panic!("old rx session should still decrypt, got {:?}", other),
        }

        let mut new_plain = hammer_infra::vec![0u8; 2048];
        match b_data.decapsulate(&new_encrypted, &mut new_plain) {
            PeerDataResult::WriteToTunnelV4(out, _) => assert_eq!(out, new_packet),
            other => panic!("new rx session should decrypt, got {:?}", other),
        }

        b_data.expire_session(old_receiver_index, 1);
        let mut expired_plain = hammer_infra::vec![0u8; 2048];
        match b_data.decapsulate(&old_encrypted_2, &mut expired_plain) {
            PeerDataResult::Err(_) => {}
            other => panic!(
                "expired old session must reject delayed packet, got {:?}",
                other
            ),
        }
    }
}
