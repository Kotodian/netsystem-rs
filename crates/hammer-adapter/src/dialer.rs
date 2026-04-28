use std::fmt;

/// Mirror of Go's `network` enum used by Outbound.Network() / Listen* paths.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Network {
    #[default]
    Tcp,
    Udp,
}

impl fmt::Display for Network {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Network::Tcp => "tcp",
            Network::Udp => "udp",
        })
    }
}

/// `runtime/common/network/dialer.go` Dialer interface — concrete async
/// methods (DialContext / ListenPacket) come online with M6's quinn-based
/// Hysteria2 outbound. The trait exists today so M2 can wire up dialer slots
/// in the OutboundManager without a placeholder type.
pub trait Dialer: Send + Sync + 'static {}
