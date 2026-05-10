use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use hickory_proto::op::{Message, MessageType, OpCode, Query, ResponseCode};
use hickory_proto::rr::{DNSClass, Name, RData, Record, RecordType};
use hickory_proto::serialize::binary::{BinDecodable, BinDecoder, BinEncodable, BinEncoder};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::config::normalize_domain;
use crate::error::{HammerError, HammerResult, WithContext};

#[derive(Clone, Copy)]
pub enum FixedResponseCode {
    NoError,
    NXDomain,
    FormatError,
    Refused,
}

impl FixedResponseCode {
    fn response_code(self) -> ResponseCode {
        match self {
            Self::NoError => ResponseCode::NoError,
            Self::NXDomain => ResponseCode::NXDomain,
            Self::FormatError => ResponseCode::FormErr,
            Self::Refused => ResponseCode::Refused,
        }
    }
}

pub trait MessageExt {
    fn from_bytes(bytes: &[u8]) -> HammerResult<Message>;
    fn to_bytes(&self) -> HammerResult<Vec<u8>>;
    fn fixed_response(&self, code: FixedResponseCode) -> Message;
    fn addresses(&self) -> Vec<IpAddr>;
}

impl MessageExt for Message {
    fn from_bytes(bytes: &[u8]) -> HammerResult<Message> {
        let mut decoder = BinDecoder::new(bytes);
        Message::read(&mut decoder)
            .map_err(|e| HammerError::internal(format!("decode DNS message: {e}")))
    }

    fn to_bytes(&self) -> HammerResult<Vec<u8>> {
        let mut bytes = Vec::with_capacity(512);
        let mut encoder = BinEncoder::new(&mut bytes);
        self.emit(&mut encoder)
            .map_err(|e| HammerError::internal(format!("encode DNS message: {e}")))?;
        Ok(bytes)
    }

    fn fixed_response(&self, code: FixedResponseCode) -> Message {
        let mut response = Message::new(self.metadata.id, MessageType::Response, OpCode::Query);
        response.metadata.authoritative = true;
        response.metadata.recursion_desired = true;
        response.metadata.recursion_available = true;
        response.metadata.response_code = code.response_code();
        for query in &self.queries {
            response.add_query(query.clone());
        }
        response
    }

    fn addresses(&self) -> Vec<IpAddr> {
        self.answers.iter().filter_map(record_addr).collect()
    }
}

pub fn record_addr(record: &Record) -> Option<IpAddr> {
    match &record.data {
        RData::A(addr) => Some(IpAddr::V4(Ipv4Addr::from(*addr))),
        RData::AAAA(addr) => Some(IpAddr::V6(Ipv6Addr::from(*addr))),
        _ => None,
    }
}

pub fn fqdn(domain: &str) -> HammerResult<Name> {
    let name = if domain.ends_with('.') {
        domain.to_owned()
    } else {
        format!("{domain}.")
    };
    Name::from_ascii(&name).map_err(|e| HammerError::internal(format!("invalid domain: {e}")))
}

#[inline]
pub fn domain_from_name(name: &Name) -> String {
    let mut s = name.to_ascii();
    let new_len = s.trim_end_matches('.').len();
    s.truncate(new_len);
    s.make_ascii_lowercase();
    s
}

pub fn query_message(domain: &str, record_type: RecordType) -> HammerResult<Message> {
    let mut message = Message::new(0, MessageType::Query, OpCode::Query);
    message.add_query({
        let mut query = Query::query(fqdn(domain)?, record_type);
        query.set_query_class(DNSClass::IN);
        query
    });
    message.metadata.recursion_desired = true;
    Ok(message)
}

pub fn fixed_address_response(
    request: &Message,
    query: &Query,
    addresses: Vec<IpAddr>,
    ttl: u32,
) -> Message {
    let mut response = request.fixed_response(FixedResponseCode::NoError);
    for address in addresses {
        match (query.query_type(), address) {
            (RecordType::A, IpAddr::V4(ip)) => {
                response.add_answer(Record::from_rdata(
                    query.name().clone(),
                    ttl,
                    RData::A(ip.into()),
                ));
            }
            (RecordType::AAAA, IpAddr::V6(ip)) => {
                response.add_answer(Record::from_rdata(
                    query.name().clone(),
                    ttl,
                    RData::AAAA(ip.into()),
                ));
            }
            _ => {}
        }
    }
    response
}

pub fn parse_hosts(content: &str) -> Vec<(String, IpAddr)> {
    let mut entries = Vec::new();
    for line in content.lines() {
        let line = line.split('#').next().unwrap_or("").trim();
        if line.is_empty() {
            continue;
        }
        let mut fields = line.split_whitespace();
        let Some(addr) = fields.next().and_then(|v| v.parse::<IpAddr>().ok()) else {
            continue;
        };
        for domain in fields {
            entries.push((normalize_domain(domain), addr));
        }
    }
    entries
}

pub fn encode_tcp_dns_query(message: &Message) -> HammerResult<Vec<u8>> {
    let bytes = MessageExt::to_bytes(message)?;
    let len = u16::try_from(bytes.len())
        .map_err(|_| HammerError::internal("DNS message exceeds TCP frame limit"))?;
    let mut frame = Vec::with_capacity(bytes.len() + 2);
    frame.extend_from_slice(&len.to_be_bytes());
    frame.extend_from_slice(&bytes);
    Ok(frame)
}

pub async fn read_tcp_dns_response<S: AsyncRead + Unpin>(stream: &mut S) -> HammerResult<Message> {
    let mut len_buf = [0_u8; 2];
    stream
        .read_exact(&mut len_buf)
        .await
        .map_err(|e| HammerError::internal(format!("read TCP DNS length: {e}")))?;
    let len = usize::from(u16::from_be_bytes(len_buf));
    let mut payload = vec![0_u8; len];
    stream
        .read_exact(&mut payload)
        .await
        .map_err(|e| HammerError::internal(format!("read TCP DNS response: {e}")))?;
    <Message as MessageExt>::from_bytes(&payload)
}

pub async fn tcp_exchange_over_stream<S>(stream: &mut S, message: Message) -> HammerResult<Message>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let bytes = MessageExt::to_bytes(&message)?;
    let len = u16::try_from(bytes.len())
        .map_err(|_| HammerError::internal("DNS message exceeds TCP frame limit"))?;
    stream
        .write_all(&len.to_be_bytes())
        .await
        .with_context(|| "write TCP DNS length")?;
    stream
        .write_all(&bytes)
        .await
        .with_context(|| "write TCP DNS request")?;
    read_tcp_dns_response(stream).await
}
