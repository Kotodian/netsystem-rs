use boringtun::noise::errors::WireGuardError;
#[cfg(feature = "endpoint-amneziawg")]
use boringtun::noise::{
    AmneziaConfig as BoringtunAmneziaConfig, AmneziaMessageTypeRange as BoringtunMessageTypeRange,
};
use hammer_core::protocol::wireguard::amnezia2::Amnezia2Options;
use rand::{Rng, thread_rng};

#[cfg(feature = "endpoint-amneziawg")]
pub(super) fn to_boringtun_config(options: &Amnezia2Options) -> BoringtunAmneziaConfig {
    BoringtunAmneziaConfig {
        h1: to_boringtun_range(options.h1),
        h2: to_boringtun_range(options.h2),
        h3: to_boringtun_range(options.h3),
        h4: to_boringtun_range(options.h4),
        s1: options.s1,
        s2: options.s2,
        s3: options.s3,
        s4: options.s4,
    }
}

#[cfg(feature = "endpoint-amneziawg")]
fn to_boringtun_range(
    range: hammer_core::protocol::wireguard::amnezia2::MessageTypeRange,
) -> BoringtunMessageTypeRange {
    BoringtunMessageTypeRange {
        min: range.min,
        max: range.max,
    }
}

pub(super) fn encode_outbound_packet(options: &Amnezia2Options, packet: &mut Vec<u8>) {
    let Some(kind) = options.classify_wireguard_packet(packet) else {
        return;
    };
    let prefix_len = options.prefix_len(kind);
    if prefix_len == 0 {
        return;
    }
    let mut padded = vec![0u8; prefix_len + packet.len()];
    thread_rng().fill(&mut padded[..prefix_len]);
    padded[prefix_len..].copy_from_slice(packet);
    *packet = padded;
}

pub(super) fn decode_inbound_packet(
    options: &Amnezia2Options,
    packet: &mut Vec<u8>,
) -> Result<bool, WireGuardError> {
    let Some(kind) = options.classify_wireguard_packet(packet) else {
        if is_junk_candidate(options, packet) {
            return Ok(false);
        }
        return Err(WireGuardError::InvalidPacket);
    };
    let prefix_len = options.prefix_len(kind);
    if prefix_len == 0 {
        return Ok(true);
    }
    packet.drain(..prefix_len);
    Ok(true)
}

pub(super) fn make_handshake_junk_packets(options: &Amnezia2Options) -> Vec<Vec<u8>> {
    if options.jc == 0 {
        return Vec::new();
    }
    let mut rng = thread_rng();
    let mut packets = Vec::with_capacity(usize::from(options.jc));
    for _ in 0..options.jc {
        let len = if options.jmin == options.jmax {
            usize::from(options.jmin)
        } else {
            usize::from(rng.gen_range(options.jmin..=options.jmax))
        };
        let mut packet = vec![0u8; len];
        rng.fill(packet.as_mut_slice());
        packets.push(packet);
    }
    packets
}

fn is_junk_candidate(options: &Amnezia2Options, packet: &[u8]) -> bool {
    usize::from(options.jmin) <= packet.len() && packet.len() <= usize::from(options.jmax)
}
