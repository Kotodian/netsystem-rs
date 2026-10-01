use std::io;
use std::net::IpAddr;

use ipnet::IpNet;
use netlink_packet_core::{
    NLM_F_ACK, NLM_F_CREATE, NLM_F_EXCL, NLM_F_REQUEST, NetlinkMessage, NetlinkPayload,
};
use netlink_packet_route::{
    AddressFamily, RouteNetlinkMessage,
    address::{AddressAttribute, AddressMessage, AddressScope},
    link::{LinkAttribute, LinkFlags, LinkMessage},
};
use netlink_sys::{Socket, SocketAddr, protocols::NETLINK_ROUTE};

pub(crate) struct HostLink {
    socket: Socket,
    sequence: u32,
}

impl HostLink {
    pub(crate) fn new() -> io::Result<Self> {
        let mut socket = Socket::new(NETLINK_ROUTE)?;
        socket.bind_auto()?;
        socket.connect(&SocketAddr::new(0, 0))?;
        Ok(Self {
            socket,
            sequence: 0,
        })
    }

    pub(crate) fn set_mtu(&mut self, ifindex: u32, mtu: u32) -> io::Result<()> {
        let mut link = LinkMessage::default();
        link.header.index = ifindex;
        link.attributes.push(LinkAttribute::Mtu(mtu));
        self.request(RouteNetlinkMessage::NewLink(link), 0)
    }

    pub(crate) fn set_up(&mut self, ifindex: u32) -> io::Result<()> {
        let mut link = LinkMessage::default();
        link.header.index = ifindex;
        link.header.flags = LinkFlags::Up;
        link.header.change_mask = LinkFlags::Up;
        self.request(RouteNetlinkMessage::NewLink(link), 0)
    }

    pub(crate) fn add_address(&mut self, ifindex: u32, network: IpNet) -> io::Result<()> {
        let mut address = AddressMessage::default();
        address.header.index = ifindex;
        address.header.prefix_len = network.prefix_len();
        address.header.scope = AddressScope::Universe;
        address.header.family = match network {
            IpNet::V4(_) => AddressFamily::Inet,
            IpNet::V6(_) => AddressFamily::Inet6,
        };
        let ip = match network {
            IpNet::V4(network) => IpAddr::V4(network.addr()),
            IpNet::V6(network) => IpAddr::V6(network.addr()),
        };
        address.attributes.push(AddressAttribute::Address(ip));
        if ip.is_ipv4() {
            address.attributes.push(AddressAttribute::Local(ip));
        }
        self.request(
            RouteNetlinkMessage::NewAddress(address),
            NLM_F_CREATE | NLM_F_EXCL,
        )
    }

    fn request(&mut self, payload: RouteNetlinkMessage, flags: u16) -> io::Result<()> {
        self.sequence = self.sequence.wrapping_add(1);
        let mut message = NetlinkMessage::from(payload);
        message.header.flags = NLM_F_REQUEST | NLM_F_ACK | flags;
        message.header.sequence_number = self.sequence;
        message.finalize();
        let mut bytes = vec![0; message.buffer_len()];
        message.serialize(&mut bytes);
        if self.socket.send(&bytes, 0)? != bytes.len() {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "short netlink request",
            ));
        }

        let mut response = [0u8; 8192];
        loop {
            let length = self.socket.recv(&mut &mut response[..], 0)?;
            let mut offset = 0;
            while offset + 16 <= length {
                let header_length = u32::from_ne_bytes(
                    response[offset..offset + 4]
                        .try_into()
                        .expect("netlink header bounds"),
                ) as usize;
                if header_length < 16 || offset + header_length > length {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "invalid netlink response length",
                    ));
                }
                let reply = NetlinkMessage::<RouteNetlinkMessage>::deserialize(
                    &response[offset..offset + header_length],
                )
                .map_err(|source| io::Error::new(io::ErrorKind::InvalidData, source))?;
                if reply.header.sequence_number == self.sequence {
                    match reply.payload {
                        NetlinkPayload::Error(error) => {
                            return match error.code {
                                None => Ok(()),
                                Some(_) => Err(error.to_io()),
                            };
                        }
                        NetlinkPayload::Overrun(_) => {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "netlink response overrun",
                            ));
                        }
                        _ => {}
                    }
                }
                offset += (header_length + 3) & !3;
            }
        }
    }
}
