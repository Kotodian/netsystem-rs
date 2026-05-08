//! `smoltcp::phy::Device` backed by a pair of mpsc channels. Carrier-agnostic:
//! the WireGuard outbound feeds it via boringtun today, and the TUN inbound's
//! smoltcp mode will share it once it lands.
//!
//! sing-box hands wireguard-go's tun.Device interface to gVisor's network
//! stack via a `channel.Endpoint`. Hammer's equivalent: a tiny in-memory
//! VecDeque-backed device that the stack actor pushes inbound IP packets into
//! and that drains its egress straight into a `tokio::sync::mpsc::Sender` —
//! whatever sits on the other side of that channel (boringtun, a kernel TUN
//! fd, or a test harness) is free to interpret the frames.

use std::collections::VecDeque;

use bytes::{Bytes, BytesMut};
use smoltcp::phy::{
    Checksum, ChecksumCapabilities, Device, DeviceCapabilities, Medium, RxToken, TxToken,
};
use smoltcp::time::Instant;
use tokio::sync::mpsc::UnboundedSender;

/// Soft cap on queued inbound packets before we start dropping. 256 ≈ 360 KiB
/// at MTU=1408 — a brief stall on the smoltcp poll loop won't snowball.
const INBOUND_BACKLOG: usize = 256;

pub(crate) struct ChannelDevice {
    inbound: VecDeque<Bytes>,
    egress: UnboundedSender<Bytes>,
    mtu: usize,
}

impl ChannelDevice {
    pub(crate) fn new(mtu: u32, egress: UnboundedSender<Bytes>) -> Self {
        Self {
            inbound: VecDeque::new(),
            egress,
            mtu: mtu as usize,
        }
    }

    /// Push an IP packet (just received from the carrier) into the inbound
    /// queue. Drops the oldest packet when the backlog overflows so a stalled
    /// poll loop can't keep pulling memory.
    #[inline]
    pub(crate) fn deliver(&mut self, packet: Bytes) {
        if self.inbound.len() >= INBOUND_BACKLOG {
            let _ = self.inbound.pop_front();
        }
        self.inbound.push_back(packet);
    }

    /// Push a burst of IP packets into the inbound queue without forcing the
    /// carrier adapter through one mpsc wakeup per packet. Ordering is
    /// preserved, and the same oldest-drop policy applies across the batch.
    #[inline]
    pub(crate) fn deliver_batch(&mut self, packets: Vec<Bytes>) {
        // If the batch alone would overflow the backlog, skip the prefix that
        // `deliver`'s oldest-drop would pop right back out — saves a round of
        // push_back/pop_front (and the matching `Bytes` clone bookkeeping) per
        // dropped packet.
        let skip = packets.len().saturating_sub(INBOUND_BACKLOG);
        for packet in packets.into_iter().skip(skip) {
            self.deliver(packet);
        }
    }

    /// `true` if the inbound queue is non-empty — caller can use this to skip
    /// poll() when there's nothing waiting.
    #[inline]
    pub(crate) fn has_inbound(&self) -> bool {
        !self.inbound.is_empty()
    }
}

impl Device for ChannelDevice {
    type RxToken<'a>
        = ChannelRxToken
    where
        Self: 'a;
    type TxToken<'a>
        = ChannelTxToken
    where
        Self: 'a;

    fn receive(&mut self, _timestamp: Instant) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        let packet = self.inbound.pop_front()?;
        let rx = ChannelRxToken { packet };
        let tx = ChannelTxToken {
            egress: self.egress.clone(),
        };
        Some((rx, tx))
    }

    fn transmit(&mut self, _timestamp: Instant) -> Option<Self::TxToken<'_>> {
        // Egress is an unbounded channel — never refuse a transmit slot. If the
        // receiver has gone away that's the actor shutting down, and the next
        // send will simply error out.
        Some(ChannelTxToken {
            egress: self.egress.clone(),
        })
    }

    fn capabilities(&self) -> DeviceCapabilities {
        let mut caps = DeviceCapabilities::default();
        caps.medium = Medium::Ip;
        caps.max_transmission_unit = self.mtu;
        // Tx-only checksumming: smoltcp still computes IP/TCP/UDP/ICMP
        // checksums for outbound packets so peers get well-formed frames,
        // but skips the redundant verification on inbound. Inbound IP
        // packets arrive already authenticated by boringtun's Poly1305 (and
        // by the kernel for the TUN-smoltcp path), so any tampering would
        // already have been caught upstream — folding the inner checksums
        // again is pure CPU. Mirrors sing-box's `CapabilityRXChecksumOffload`
        // on its gVisor stack.
        let mut chk = ChecksumCapabilities::default();
        chk.ipv4 = Checksum::Tx;
        chk.tcp = Checksum::Tx;
        chk.udp = Checksum::Tx;
        chk.icmpv4 = Checksum::Tx;
        caps.checksum = chk;
        caps
    }
}

pub(crate) struct ChannelRxToken {
    packet: Bytes,
}

impl RxToken for ChannelRxToken {
    #[inline]
    fn consume<R, F>(self, f: F) -> R
    where
        F: FnOnce(&[u8]) -> R,
    {
        f(&self.packet)
    }
}

pub(crate) struct ChannelTxToken {
    egress: UnboundedSender<Bytes>,
}

impl TxToken for ChannelTxToken {
    #[inline]
    fn consume<R, F>(self, len: usize, f: F) -> R
    where
        F: FnOnce(&mut [u8]) -> R,
    {
        // smoltcp wants to write `len` bytes into a mutable slice; we hand it
        // a freshly-allocated `BytesMut`, freeze it, and ship the resulting
        // `Bytes` straight into the egress mpsc — zero memcpy across the
        // ownership transfer.
        let mut buf = BytesMut::zeroed(len);
        let result = f(&mut buf);
        // If the receiver has been dropped (actor shutting down) the packet is
        // discarded. smoltcp has no notion of "tx failed" past consume(), so
        // this is the cleanest place to swallow the error.
        let _ = self.egress.send(buf.freeze());
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    #[test]
    fn receive_returns_oldest_inbound_packet_first() {
        let (egress_tx, _egress_rx) = mpsc::unbounded_channel();
        let mut dev = ChannelDevice::new(1408, egress_tx);
        dev.deliver(Bytes::from_static(&[1, 2, 3]));
        dev.deliver(Bytes::from_static(&[4, 5, 6]));
        let (rx, _tx) = dev.receive(Instant::from_millis(0)).expect("rx token");
        rx.consume(|buf| assert_eq!(buf, &[1, 2, 3]));
        let (rx, _tx) = dev.receive(Instant::from_millis(0)).expect("rx token");
        rx.consume(|buf| assert_eq!(buf, &[4, 5, 6]));
        assert!(dev.receive(Instant::from_millis(0)).is_none());
    }

    #[test]
    fn transmit_pushes_packet_to_egress_channel() {
        let (egress_tx, mut egress_rx) = mpsc::unbounded_channel();
        let mut dev = ChannelDevice::new(1408, egress_tx);
        let tx = dev.transmit(Instant::from_millis(0)).expect("tx token");
        tx.consume(20, |buf| {
            buf.fill(0xAB);
        });
        let pushed = egress_rx.try_recv().expect("egress sees the packet");
        assert_eq!(pushed.len(), 20);
        assert!(pushed.iter().all(|b| *b == 0xAB));
    }

    #[test]
    fn deliver_drops_oldest_when_backlog_overflows() {
        let (egress_tx, _egress_rx) = mpsc::unbounded_channel();
        let mut dev = ChannelDevice::new(1408, egress_tx);
        for i in 0..(INBOUND_BACKLOG + 5) {
            dev.deliver(Bytes::from(vec![i as u8]));
        }
        // Oldest 5 must have been dropped — the next receive returns the 6th.
        let (rx, _tx) = dev.receive(Instant::from_millis(0)).expect("rx token");
        rx.consume(|buf| assert_eq!(buf, &[5]));
    }

    #[test]
    fn deliver_batch_preserves_packet_order() {
        let (egress_tx, _egress_rx) = mpsc::unbounded_channel();
        let mut dev = ChannelDevice::new(1408, egress_tx);
        dev.deliver_batch(vec![
            Bytes::from_static(&[1]),
            Bytes::from_static(&[2]),
            Bytes::from_static(&[3]),
        ]);

        for expected in 1..=3 {
            let (rx, _tx) = dev.receive(Instant::from_millis(0)).expect("rx token");
            rx.consume(|buf| assert_eq!(buf, &[expected]));
        }
        assert!(dev.receive(Instant::from_millis(0)).is_none());
    }

    #[test]
    fn deliver_batch_drops_oldest_when_backlog_overflows() {
        let (egress_tx, _egress_rx) = mpsc::unbounded_channel();
        let mut dev = ChannelDevice::new(1408, egress_tx);
        let packets = (0..(INBOUND_BACKLOG + 5))
            .map(|i| Bytes::from(vec![i as u8]))
            .collect();
        dev.deliver_batch(packets);

        let (rx, _tx) = dev.receive(Instant::from_millis(0)).expect("rx token");
        rx.consume(|buf| assert_eq!(buf, &[5]));
    }

    #[test]
    fn capabilities_match_configured_mtu_and_ip_medium() {
        let (egress_tx, _) = mpsc::unbounded_channel();
        let dev = ChannelDevice::new(1408, egress_tx);
        let caps = dev.capabilities();
        assert_eq!(caps.medium, Medium::Ip);
        assert_eq!(caps.max_transmission_unit, 1408);
    }
}
