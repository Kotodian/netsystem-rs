use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use crate::protocol::ip::{IpProtocol, IpVersion, ParsedIpPacket};
use hammer_infra::prefetch::prefetch_read_l1;

use super::dpo::{Adjacency, AdjacencyIndex, DpoId, DpoProto, DpoType};
use super::ip4_mtrie::Ip4Mtrie;
use super::ip6_fib::Ip6Fib;
use super::load_balance::{LoadBalance, LoadBalanceIndex};

#[derive(Debug, Clone)]
pub struct FibTable {
    lookup: FibLookupTables,
    adjacencies: Box<[Adjacency]>,
    drop_next: u16,
    ip4_drop: DpoId,
    ip6_drop: DpoId,
}

#[derive(Debug, Clone)]
#[repr(C)]
struct FibLookupTables {
    ip4: Ip4Mtrie<u32>,
    ip6: Ip6Fib<u32>,
    route_dpos: Box<[DpoId]>,
    load_balances: Box<[LoadBalance]>,
}

impl FibTable {
    #[inline]
    pub fn new(drop_next: u16) -> Self {
        Self {
            lookup: FibLookupTables {
                ip4: Ip4Mtrie::empty(),
                ip6: Ip6Fib::empty(),
                route_dpos: Box::new([]),
                load_balances: Box::new([]),
            },
            adjacencies: Box::new([]),
            drop_next,
            ip4_drop: DpoId::drop(DpoProto::IP4, drop_next),
            ip6_drop: DpoId::drop(DpoProto::IP6, drop_next),
        }
    }

    #[inline(always)]
    pub fn lookup_packet(&self, packet: &ParsedIpPacket) -> Option<FibLookupResult> {
        let hash = flow_hash(packet);
        match packet.destination {
            IpAddr::V4(destination) => self.lookup_ip4(destination, hash),
            IpAddr::V6(destination) => self.lookup_ip6(destination, hash),
        }
    }

    #[inline(always)]
    pub fn prefetch_packet(&self, packet: &ParsedIpPacket) {
        match packet.destination {
            IpAddr::V4(destination) => self.prefetch_ip4(destination),
            IpAddr::V6(destination) => self.prefetch_ip6(destination),
        }
    }

    #[inline(always)]
    pub fn lookup_ip4(&self, destination: Ipv4Addr, hash: usize) -> Option<FibLookupResult> {
        let route_dpo = self.lookup.ip4.lookup(destination)?;
        self.select_route_dpo(route_dpo, hash)
    }

    #[inline(always)]
    pub fn lookup_ip6(&self, destination: Ipv6Addr, hash: usize) -> Option<FibLookupResult> {
        let route_dpo = self.lookup.ip6.lookup(destination)?;
        self.select_route_dpo(route_dpo, hash)
    }

    #[inline(always)]
    pub fn prefetch_ip4(&self, destination: Ipv4Addr) {
        self.lookup.ip4.prefetch(destination);
    }

    #[inline(always)]
    pub fn prefetch_ip6(&self, destination: Ipv6Addr) {
        self.lookup.ip6.prefetch_destination(destination);
    }

    #[inline(always)]
    pub fn load_balance(&self, index: LoadBalanceIndex) -> Option<&LoadBalance> {
        self.lookup.load_balances.get(index.slot())
    }

    #[inline(always)]
    pub fn adjacency(&self, index: AdjacencyIndex) -> Option<Adjacency> {
        self.adjacencies.get(index.slot()).copied()
    }

    #[inline(always)]
    pub fn drop_next(&self) -> u16 {
        self.drop_next
    }

    #[inline(always)]
    pub fn drop_dpo(&self, version: IpVersion) -> DpoId {
        match version {
            IpVersion::V4 => self.ip4_drop,
            IpVersion::V6 => self.ip6_drop,
        }
    }

    #[inline(always)]
    fn select_route_dpo(&self, route_dpo_index: u32, hash: usize) -> Option<FibLookupResult> {
        let route_dpo = *self.lookup.route_dpos.get(route_dpo_index as usize)?;
        let Some(load_balance) = (route_dpo.class() == DpoType::LOAD_BALANCE)
            .then(|| LoadBalanceIndex::new(route_dpo.index()))
        else {
            return Some(FibLookupResult::terminal(route_dpo));
        };
        let load_balance_ref = self.load_balance(load_balance)?;
        prefetch_read_l1(load_balance_ref);
        load_balance_ref.prefetch_bucket(hash);
        let (bucket_index, dpo) = load_balance_ref.select_hash(hash);
        let Some(nested_load_balance) =
            (dpo.class() == DpoType::LOAD_BALANCE).then(|| LoadBalanceIndex::new(dpo.index()))
        else {
            return Some(FibLookupResult::from_load_balance(
                route_dpo,
                load_balance,
                bucket_index,
                dpo,
            ));
        };
        self.select_nested_load_balance_dpo(route_dpo, nested_load_balance, hash)
    }

    #[inline(always)]
    fn select_nested_load_balance_dpo(
        &self,
        route_dpo: DpoId,
        mut load_balance: LoadBalanceIndex,
        hash: usize,
    ) -> Option<FibLookupResult> {
        loop {
            let load_balance_ref = self.load_balance(load_balance)?;
            prefetch_read_l1(load_balance_ref);
            load_balance_ref.prefetch_bucket(hash);
            let (bucket_index, dpo) = load_balance_ref.select_hash(hash);
            let Some(nested_load_balance) =
                (dpo.class() == DpoType::LOAD_BALANCE).then(|| LoadBalanceIndex::new(dpo.index()))
            else {
                return Some(FibLookupResult::from_load_balance(
                    route_dpo,
                    load_balance,
                    bucket_index,
                    dpo,
                ));
            };
            load_balance = nested_load_balance;
        }
    }
}

const FIB_LOOKUP_NO_LOAD_BALANCE: u32 = u32::MAX;
const FIB_LOOKUP_NO_BUCKET: u16 = u16::MAX;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FibLookupResult {
    pub route_dpo: DpoId,
    load_balance: u32,
    bucket_index: u16,
    pub dpo: DpoId,
}

impl FibLookupResult {
    #[inline(always)]
    pub fn terminal(route_dpo: DpoId) -> Self {
        Self {
            route_dpo,
            load_balance: FIB_LOOKUP_NO_LOAD_BALANCE,
            bucket_index: FIB_LOOKUP_NO_BUCKET,
            dpo: route_dpo,
        }
    }

    #[inline(always)]
    pub fn from_load_balance(
        route_dpo: DpoId,
        load_balance: LoadBalanceIndex,
        bucket_index: u16,
        dpo: DpoId,
    ) -> Self {
        Self {
            route_dpo,
            load_balance: load_balance.get(),
            bucket_index,
            dpo,
        }
    }

    #[inline(always)]
    pub fn load_balance(&self) -> Option<LoadBalanceIndex> {
        (self.load_balance != FIB_LOOKUP_NO_LOAD_BALANCE)
            .then_some(LoadBalanceIndex::new(self.load_balance))
    }

    #[inline(always)]
    pub fn bucket_index(&self) -> Option<u16> {
        (self.bucket_index != FIB_LOOKUP_NO_BUCKET).then_some(self.bucket_index)
    }

    #[inline(always)]
    pub fn forwarding_load_balance_index(&self) -> u32 {
        self.load_balance
    }

    #[inline(always)]
    pub fn forwarding_bucket_index(&self) -> u16 {
        self.bucket_index
    }
}

#[inline(always)]
pub fn flow_hash(packet: &ParsedIpPacket) -> usize {
    let mut value = 0x9e37_79b9_7f4a_7c15u64 ^ u64::from(ip_protocol_number(packet.protocol));
    value = mix_ip(value, packet.source);
    value = mix_ip(value, packet.destination);
    value as usize
}

#[inline(always)]
fn mix_ip(mut value: u64, addr: IpAddr) -> u64 {
    match addr {
        IpAddr::V4(addr) => {
            value ^= u32::from(addr) as u64;
            value = value.rotate_left(27).wrapping_mul(0x94d0_49bb_1331_11eb);
        }
        IpAddr::V6(addr) => {
            let raw = u128::from(addr);
            value ^= raw as u64;
            value = value.rotate_left(27).wrapping_mul(0x94d0_49bb_1331_11eb);
            value ^= (raw >> 64) as u64;
            value = value.rotate_left(31).wrapping_mul(0x2545_f491_4f6c_dd1d);
        }
    }
    value
}

#[inline(always)]
fn ip_protocol_number(protocol: IpProtocol) -> u8 {
    match protocol {
        IpProtocol::Icmpv4 => 1,
        IpProtocol::Tcp => 6,
        IpProtocol::Udp => 17,
        IpProtocol::Icmpv6 => 58,
        IpProtocol::Other(value) => value,
    }
}
