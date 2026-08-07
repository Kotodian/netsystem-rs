use crate::connection::spaces::{ApplicationSpace, HandshakeSpace};
use crate::crypto::{HeaderKey, KeyPair, PacketKey};
use crate::packet::{Packet, PartialDecode, SpaceId};
use crate::token::ResetToken;
use crate::Instant;
use crate::{TransportError, RESET_TOKEN_SIZE};

/// Removes header protection of a packet, or returns `None` if the packet was dropped.
pub(super) fn unprotect_header<'a>(
    partial_decode: PartialDecode,
    scratch: &'a mut [u8],
    initial: Option<&HandshakeSpace>,
    handshake: Option<&HandshakeSpace>,
    application: Option<&ApplicationSpace>,
    stateless_reset_token: Option<ResetToken>,
) -> Option<UnprotectHeaderResult<'a>> {
    let header_crypto = if partial_decode.is_0rtt() {
        application
            .and_then(|space| space.zero_rtt_crypto.as_ref())
            .map(|crypto| &*crypto.header)
    } else if let Some(space) = partial_decode.space() {
        match space {
            SpaceId::Initial => initial.map(|space| space.crypto.header.remote.as_ref()),
            SpaceId::Handshake => handshake.map(|space| space.crypto.header.remote.as_ref()),
            SpaceId::Data => application
                .and_then(|space| space.crypto.as_ref())
                .map(|crypto| crypto.header.remote.as_ref()),
        }
    } else {
        None
    };
    if partial_decode.space().is_some() && header_crypto.is_none() {
        return None;
    }

    let packet = partial_decode.data(scratch);
    let stateless_reset = packet.len() >= RESET_TOKEN_SIZE + 5
        && stateless_reset_token.as_deref() == Some(&packet[packet.len() - RESET_TOKEN_SIZE..]);

    match partial_decode.finish(scratch, header_crypto) {
        Ok(packet) => Some(UnprotectHeaderResult {
            packet: Some(packet),
            stateless_reset,
        }),
        Err(_) if stateless_reset => Some(UnprotectHeaderResult {
            packet: None,
            stateless_reset: true,
        }),
        Err(_) => None,
    }
}

pub(super) struct UnprotectHeaderResult<'a> {
    pub(super) packet: Option<Packet<'a>>,
    pub(super) stateless_reset: bool,
}

/// Decrypts a packet's body in-place.
pub(super) fn decrypt_packet_body(
    packet: &mut Packet<'_>,
    initial: Option<&HandshakeSpace>,
    handshake: Option<&HandshakeSpace>,
    application: Option<&ApplicationSpace>,
) -> Result<Option<DecryptPacketResult>, Option<TransportError>> {
    if !packet.header.is_protected() {
        return Ok(None);
    }

    let space = packet.header.space();
    let rx_packet = match space {
        SpaceId::Initial => initial.map(|space| space.packets.rx_packet),
        SpaceId::Handshake => handshake.map(|space| space.packets.rx_packet),
        SpaceId::Data => application.map(|space| space.packets.rx_packet),
    }
    .ok_or(None)?;
    let number = packet.header.number().ok_or(None)?.expand(rx_packet + 1);
    let packet_key_phase = packet.header.key_phase();

    let mut crypto_update = false;
    let crypto = if packet.header.is_0rtt() {
        &application
            .and_then(|space| space.zero_rtt_crypto.as_ref())
            .ok_or(None)?
            .packet
    } else if space != SpaceId::Data {
        let crypto = match space {
            SpaceId::Initial => initial.map(|space| &space.crypto),
            SpaceId::Handshake => handshake.map(|space| &space.crypto),
            SpaceId::Data => None,
        }
        .ok_or(None)?;
        &crypto.packet.remote
    } else {
        let space = application.ok_or(None)?;
        if packet_key_phase == space.key_phase {
            &space.crypto.as_ref().ok_or(None)?.packet.remote
        } else if let Some(prev) = space.prev_crypto.as_ref().and_then(|crypto| {
            if crypto.end_packet.map_or(true, |(pn, _)| number < pn) {
                Some(crypto)
            } else {
                None
            }
        }) {
            &prev.crypto.remote
        } else {
            crypto_update = true;
            &space.next_crypto.as_ref().ok_or(None)?.remote
        }
    };

    let payload_len = crypto
        .decrypt(number, &packet.header_data, packet.payload.as_mut())
        .map_err(|_| None)?;
    packet.payload_len = payload_len;

    if !packet.reserved_bits_valid() {
        return Err(Some(TransportError::PROTOCOL_VIOLATION(
            "reserved bits set",
        )));
    }

    let mut outgoing_key_update_acked = false;
    if let Some(space) = application {
        if let Some(prev) = &space.prev_crypto {
            if prev.end_packet.is_none() && packet_key_phase == space.key_phase {
                outgoing_key_update_acked = true;
            }
        }
    }

    if crypto_update {
        if number <= rx_packet
            || application
                .and_then(|space| space.prev_crypto.as_ref())
                .is_some_and(|crypto| crypto.update_unacked)
        {
            return Err(Some(TransportError::KEY_UPDATE_ERROR("")));
        }
    }

    Ok(Some(DecryptPacketResult {
        number,
        outgoing_key_update_acked,
        incoming_key_update: crypto_update,
    }))
}

pub(super) struct DecryptPacketResult {
    pub(super) number: u64,
    pub(super) outgoing_key_update_acked: bool,
    pub(super) incoming_key_update: bool,
}

pub(super) struct PrevCrypto {
    pub(super) crypto: KeyPair<Box<dyn PacketKey>>,
    pub(super) end_packet: Option<(u64, Instant)>,
    pub(super) update_unacked: bool,
}

pub(super) struct ZeroRttCrypto {
    pub(super) header: Box<dyn HeaderKey>,
    pub(super) packet: Box<dyn PacketKey>,
}
