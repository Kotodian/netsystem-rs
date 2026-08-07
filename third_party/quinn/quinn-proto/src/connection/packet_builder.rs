use std::mem;

use bytes::{Bytes, BytesMut};
use rand::Rng;
use tracing::trace_span;

use super::{spaces::SentPacket, Connection, SentFrames};
use crate::{
    connection::ConnectionSide,
    frame::{self, Close},
    packet::{Header, InitialHeader, LongType, PacketNumber, PartialEncode, SpaceId, FIXED_BIT},
    ConnectionId, Instant, TransportError, TransportErrorCode,
};

pub(super) struct PacketBuilder {
    pub(super) datagram_start: usize,
    pub(super) space: SpaceId,
    pub(super) partial_encode: PartialEncode,
    pub(super) ack_eliciting: bool,
    pub(super) exact_number: u64,
    pub(super) short_header: bool,
    pub(super) min_size: usize,
    pub(super) max_size: usize,
    pub(super) tag_len: usize,
    pub(super) _span: tracing::span::EnteredSpan,
}

impl PacketBuilder {
    pub(super) fn new(
        now: Instant,
        space_id: SpaceId,
        dst_cid: ConnectionId,
        buffer: &mut BytesMut,
        buffer_capacity: usize,
        datagram_start: usize,
        ack_eliciting: bool,
        conn: &mut Connection,
    ) -> Option<Self> {
        let version = conn.version;

        match space_id {
            SpaceId::Data => {
                let space = conn
                    .application
                    .as_ref()
                    .expect("application packet space present");
                if space.sent_with_keys >= space.key_phase_size {
                    conn.force_key_update();
                }
            }
            SpaceId::Initial | SpaceId::Handshake => {
                let (keys, sent_with_keys) = match space_id {
                    SpaceId::Initial => {
                        let space = conn.initial.as_ref().expect("initial packet space present");
                        (&space.crypto, space.sent_with_keys)
                    }
                    SpaceId::Handshake => {
                        let space = conn
                            .handshake
                            .as_ref()
                            .expect("handshake packet space present");
                        (&space.crypto, space.sent_with_keys)
                    }
                    SpaceId::Data => unreachable!(),
                };
                let confidentiality_limit = keys.packet.local.confidentiality_limit();
                if sent_with_keys.saturating_add(1) == confidentiality_limit {
                    conn.close_inner(
                        now,
                        Close::Connection(frame::ConnectionClose {
                            error_code: TransportErrorCode::AEAD_LIMIT_REACHED,
                            frame_type: None,
                            reason: Bytes::from_static(b"confidentiality limit reached"),
                        }),
                    );
                } else if sent_with_keys > confidentiality_limit {
                    conn.kill(
                        TransportError::AEAD_LIMIT_REACHED("confidentiality limit reached").into(),
                    );
                    return None;
                }
            }
        }

        let exact_number = match space_id {
            SpaceId::Initial => conn
                .initial
                .as_mut()
                .expect("initial packet space present")
                .get_tx_number(),
            SpaceId::Handshake => conn
                .handshake
                .as_mut()
                .expect("handshake packet space present")
                .get_tx_number(),
            SpaceId::Data => {
                let space = conn
                    .application
                    .as_mut()
                    .expect("application packet space present");
                space.packet_number_filter.allocate(
                    &mut conn.rng,
                    &mut space.packets,
                    &mut space.sent_with_keys,
                )
            }
        };

        let span = trace_span!("send", space = ?space_id, pn = exact_number).entered();
        let (number, largest_acked) = match space_id {
            SpaceId::Initial => {
                let space = conn.initial.as_ref().expect("initial packet space present");
                (
                    PacketNumber::new(
                        exact_number,
                        space.packets.loss.largest_acked_packet.unwrap_or(0),
                    ),
                    space.packets.loss.largest_acked_packet.unwrap_or(0),
                )
            }
            SpaceId::Handshake => {
                let space = conn
                    .handshake
                    .as_ref()
                    .expect("handshake packet space present");
                (
                    PacketNumber::new(
                        exact_number,
                        space.packets.loss.largest_acked_packet.unwrap_or(0),
                    ),
                    space.packets.loss.largest_acked_packet.unwrap_or(0),
                )
            }
            SpaceId::Data => {
                let space = conn
                    .application
                    .as_ref()
                    .expect("application packet space present");
                (
                    PacketNumber::new(
                        exact_number,
                        space.packets.loss.largest_acked_packet.unwrap_or(0),
                    ),
                    space.packets.loss.largest_acked_packet.unwrap_or(0),
                )
            }
        };
        let _ = largest_acked;

        let header = match space_id {
            SpaceId::Data => {
                let space = conn
                    .application
                    .as_ref()
                    .expect("application packet space present");
                if space.crypto.is_some() {
                    Header::Short {
                        dst_cid,
                        number,
                        spin: if conn.spin_enabled {
                            conn.spin
                        } else {
                            conn.rng.random()
                        },
                        key_phase: space.key_phase,
                    }
                } else {
                    Header::Long {
                        ty: LongType::ZeroRtt,
                        src_cid: conn.handshake_cid,
                        dst_cid,
                        number,
                        version,
                    }
                }
            }
            SpaceId::Handshake => Header::Long {
                ty: LongType::Handshake,
                src_cid: conn.handshake_cid,
                dst_cid,
                number,
                version,
            },
            SpaceId::Initial => Header::Initial(InitialHeader {
                src_cid: conn.handshake_cid,
                dst_cid,
                token: match &conn.side {
                    ConnectionSide::Client { token, .. } => token.clone(),
                    ConnectionSide::Server { .. } => Bytes::new(),
                },
                number,
                version,
            }),
        };
        let partial_encode = header.encode(buffer);
        if conn.peer_params.grease_quic_bit && conn.rng.random() {
            buffer[partial_encode.start] ^= FIXED_BIT;
        }

        let (sample_size, tag_len) = match space_id {
            SpaceId::Initial => {
                let space = conn.initial.as_ref().expect("initial packet space present");
                (
                    space.crypto.header.local.sample_size(),
                    space.crypto.packet.local.tag_len(),
                )
            }
            SpaceId::Handshake => {
                let space = conn
                    .handshake
                    .as_ref()
                    .expect("handshake packet space present");
                (
                    space.crypto.header.local.sample_size(),
                    space.crypto.packet.local.tag_len(),
                )
            }
            SpaceId::Data => {
                let space = conn
                    .application
                    .as_ref()
                    .expect("application packet space present");
                if let Some(ref crypto) = space.crypto {
                    (
                        crypto.header.local.sample_size(),
                        crypto.packet.local.tag_len(),
                    )
                } else {
                    let zero_rtt = space
                        .zero_rtt_crypto
                        .as_ref()
                        .expect("0-RTT packet requires 0-RTT keys");
                    (zero_rtt.header.sample_size(), zero_rtt.packet.tag_len())
                }
            }
        };

        let min_size = Ord::max(
            buffer.len() + (sample_size + 4).saturating_sub(number.len() + tag_len),
            partial_encode.start + dst_cid.len() + 6,
        );
        let max_size = buffer_capacity - tag_len;
        debug_assert!(max_size >= min_size);

        Some(Self {
            datagram_start,
            space: space_id,
            partial_encode,
            exact_number,
            short_header: header.is_short(),
            min_size,
            max_size,
            tag_len,
            ack_eliciting,
            _span: span,
        })
    }

    pub(super) fn pad_to(&mut self, min_size: u16) {
        self.min_size = Ord::max(
            self.min_size,
            self.datagram_start + (min_size as usize) - self.tag_len,
        );
    }

    pub(super) fn finish_and_track(
        self,
        now: Instant,
        conn: &mut Connection,
        sent: Option<SentFrames>,
        buffer: &mut BytesMut,
    ) {
        let ack_eliciting = self.ack_eliciting;
        let exact_number = self.exact_number;
        let space_id = self.space;
        let (size, padded) = self.finish(conn, now, buffer);
        let sent = match sent {
            Some(sent) => sent,
            None => return,
        };

        let size = match padded || ack_eliciting {
            true => size as u16,
            false => 0,
        };

        match space_id {
            SpaceId::Data => {
                let packet = SentPacket {
                    path_generation: conn.path.generation(),
                    largest_acked: sent.largest_acked,
                    time_sent: now,
                    size,
                    ack_eliciting,
                    frames: sent.retransmits,
                };
                conn.path.sent(
                    exact_number,
                    packet,
                    &mut conn
                        .application
                        .as_mut()
                        .expect("application packet space present")
                        .packets,
                );
            }
            SpaceId::Initial | SpaceId::Handshake => {
                let frames = sent
                    .retransmits
                    .retransmits
                    .map(|mut retransmits| Box::new(mem::take(&mut retransmits.crypto)));
                let packet = SentPacket {
                    path_generation: conn.path.generation(),
                    largest_acked: sent.largest_acked,
                    time_sent: now,
                    size,
                    ack_eliciting,
                    frames,
                };
                match space_id {
                    SpaceId::Initial => conn.path.sent(
                        exact_number,
                        packet,
                        &mut conn
                            .initial
                            .as_mut()
                            .expect("initial packet space present")
                            .packets,
                    ),
                    SpaceId::Handshake => conn.path.sent(
                        exact_number,
                        packet,
                        &mut conn
                            .handshake
                            .as_mut()
                            .expect("handshake packet space present")
                            .packets,
                    ),
                    SpaceId::Data => unreachable!(),
                }
            }
        }

        conn.stats.path.sent_packets += 1;
        conn.reset_keep_alive(now);
        if size != 0 && ack_eliciting {
            match space_id {
                SpaceId::Initial => {
                    conn.initial
                        .as_mut()
                        .expect("initial packet space present")
                        .packets
                        .loss
                        .time_of_last_ack_eliciting_packet = Some(now)
                }
                SpaceId::Handshake => {
                    conn.handshake
                        .as_mut()
                        .expect("handshake packet space present")
                        .packets
                        .loss
                        .time_of_last_ack_eliciting_packet = Some(now)
                }
                SpaceId::Data => {
                    conn.application
                        .as_mut()
                        .expect("application packet space present")
                        .packets
                        .loss
                        .time_of_last_ack_eliciting_packet = Some(now)
                }
            }
            if conn.permit_idle_reset {
                conn.reset_idle_timeout(now, space_id);
            }
            conn.permit_idle_reset = false;
        }
        if size != 0 {
            conn.set_loss_detection_timer(now);
            conn.path.pacing.on_transmit(size);
        }
    }

    pub(super) fn finish(
        self,
        conn: &mut Connection,
        now: Instant,
        buffer: &mut BytesMut,
    ) -> (usize, bool) {
        let pad = buffer.len() < self.min_size;
        if pad {
            buffer.resize(self.min_size, 0);
        }

        let (header_crypto, packet_crypto) = match self.space {
            SpaceId::Initial => {
                let space = conn.initial.as_ref().expect("initial packet space present");
                (&*space.crypto.header.local, &*space.crypto.packet.local)
            }
            SpaceId::Handshake => {
                let space = conn
                    .handshake
                    .as_ref()
                    .expect("handshake packet space present");
                (&*space.crypto.header.local, &*space.crypto.packet.local)
            }
            SpaceId::Data => {
                let space = conn
                    .application
                    .as_ref()
                    .expect("application packet space present");
                if let Some(ref crypto) = space.crypto {
                    (&*crypto.header.local, &*crypto.packet.local)
                } else {
                    let zero_rtt = space
                        .zero_rtt_crypto
                        .as_ref()
                        .expect("0-RTT packet requires 0-RTT keys");
                    (&*zero_rtt.header, &*zero_rtt.packet)
                }
            }
        };

        debug_assert_eq!(
            packet_crypto.tag_len(),
            self.tag_len,
            "Mismatching crypto tag len"
        );

        buffer.resize(buffer.len() + packet_crypto.tag_len(), 0);
        let encode_start = self.partial_encode.start;
        let packet_buf = &mut buffer[encode_start..];
        self.partial_encode.finish(
            packet_buf,
            header_crypto,
            Some((self.exact_number, packet_crypto)),
        );

        let len = buffer.len() - encode_start;
        let is_0rtt = self.space == SpaceId::Data
            && conn
                .application
                .as_ref()
                .is_some_and(|space| space.crypto.is_none());
        conn.config.qlog_sink.emit_packet_sent(
            self.exact_number,
            len,
            self.space,
            is_0rtt,
            now,
            conn.orig_rem_cid,
        );

        (len, pad)
    }
}
