//! Focused ABI tests for the VPP HTTP FIFO codec.
//!
//! `layout_matches_vpp` pins sizes/offsets against values verified with a C
//! compiler probe of the structs in
//! `third_party/vpp/src/plugins/http/http.h`. `golden_request` builds the
//! exact expected on-FIFO bytes independently of the codec, so the encode
//! test is a true golden-byte test.

use core::mem::offset_of;
use std::io::Read;

use hammer_infra::fifo::Fifo;
use hammer_infra::segment::Segment;

use super::*;

/// Headers of the golden request, static so tests can borrow them.
static GOLDEN_HEADERS: [AppHeader<'static>; 2] = [
    AppHeader::Known {
        flags: FieldLineFlags::empty(),
        name: header_name::ACCEPT,
        value: b"text/html",
    },
    AppHeader::Custom {
        flags: FieldLineFlags::empty(),
        name: b"X-Test",
        value: b"1",
    },
];

/// Golden request, byte-for-byte independent of the codec:
/// GET /index.html, headers Accept: text/html and X-Test: 1, body "abc".
/// Total = 88 (msg) + 11 (path) + 32 (header list) + 3 (body) = 134 bytes.
fn golden_request() -> Vec<u8> {
    let mut b = Vec::with_capacity(134);
    // http_msg_t: type=REQUEST(0), method_or_code=GET(0), data.type=INLINE(0).
    b.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
    b.extend_from_slice(&[0, 0, 0, 0]); // `http_msg_data_t` padding: type @0, len @8
    // data.len = 11 + 32 + 3 = 46.
    b.extend_from_slice(&46u64.to_le_bytes());
    b.push(0); // scheme: HTTP
    b.extend_from_slice(&[0, 0, 0]); // struct padding
    // target_authority_offset/len @28, target_path_offset=0 @36, path_len=11
    // @40, query off/len @44.
    b.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0]);
    b.extend_from_slice(&0u32.to_le_bytes());
    b.extend_from_slice(&11u32.to_le_bytes());
    b.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0]);
    // headers_offset=11, headers_len=32, body_offset=43, body_len=3.
    b.extend_from_slice(&11u32.to_le_bytes());
    b.extend_from_slice(&32u32.to_le_bytes());
    b.extend_from_slice(&43u32.to_le_bytes());
    b.extend_from_slice(&3u64.to_le_bytes());
    b.extend_from_slice(&0u64.to_le_bytes()); // headers_ctx
    b.push(0); // upgrade_proto: NA
    b.extend_from_slice(&[0; 7]); // struct tail padding
    assert_eq!(b.len(), 88);
    b.extend_from_slice(b"/index.html");
    // Entry 1: known header ACCEPT (4), flags 0, value_len 9:
    // [flags:u16][name:u16][value_len:u32][value] = 8 + 9 = 17 bytes.
    b.extend_from_slice(&[0, 0]);
    b.extend_from_slice(&4u16.to_le_bytes());
    b.extend_from_slice(&9u32.to_le_bytes());
    b.extend_from_slice(b"text/html");
    // Entry 2: custom name "X-Test", flags CUSTOM_NAME(4), name_len 6,
    // value_len 1: [flags:u16][name_len:u16][name][value_len:u32][value]
    // = 8 + 6 + 1 = 15 bytes. No padding between name and value_len.
    b.extend_from_slice(&[4, 0]);
    b.extend_from_slice(&6u16.to_le_bytes());
    b.extend_from_slice(b"X-Test");
    b.extend_from_slice(&1u32.to_le_bytes());
    b.extend_from_slice(b"1");
    b.extend_from_slice(b"abc");
    assert_eq!(b.len(), 134);
    b
}

fn golden_request_value() -> Request<'static> {
    Request {
        method: ReqMethod::Get,
        scheme: UrlScheme::Http,
        target_path: b"/index.html",
        headers: &GOLDEN_HEADERS,
        body: b"abc",
    }
}

#[test]
fn layout_matches_vpp() {
    // Sizes/offsets verified with a C compiler probe of the VPP structs
    // (http.h:87-91, 395-401, 412-418, 426-455).
    assert_eq!(size_of::<HttpMsg>(), 88);
    assert_eq!(size_of::<MsgData>(), 80);
    assert_eq!(offset_of!(HttpMsg, msg_type), 0);
    assert_eq!(offset_of!(HttpMsg, method_or_code), 4);
    assert_eq!(offset_of!(HttpMsg, data), 8);
    assert_eq!(offset_of!(MsgData, data_type), 0);
    assert_eq!(offset_of!(MsgData, len), 8);
    assert_eq!(offset_of!(MsgData, scheme), 16);
    assert_eq!(offset_of!(MsgData, target_authority_offset), 20);
    assert_eq!(offset_of!(MsgData, target_path_offset), 28);
    assert_eq!(offset_of!(MsgData, target_path_len), 32);
    assert_eq!(offset_of!(MsgData, headers_offset), 44);
    assert_eq!(offset_of!(MsgData, headers_len), 48);
    assert_eq!(offset_of!(MsgData, body_offset), 52);
    assert_eq!(offset_of!(MsgData, body_len), 56);
    assert_eq!(offset_of!(MsgData, headers_ctx), 64);
    assert_eq!(offset_of!(MsgData, upgrade_proto), 72);
    // Enum wire widths: plain C int enums are 4 bytes, `: u8`/`: u16` the rest.
    assert_eq!(size_of::<MsgType>(), 4);
    assert_eq!(size_of::<ReqMethod>(), 1);
    assert_eq!(size_of::<MsgDataType>(), 4);
    assert_eq!(size_of::<UrlScheme>(), 1);
    assert_eq!(size_of::<UpgradeProto>(), 1);
    assert_eq!(size_of::<FieldLineFlags>(), 2);
    // One header entry prefix: flags(2) + name/name_len(2) + value_len(4) = 8.
    let req = golden_request_value();
    assert_eq!(req.encoded_len().unwrap(), 134);
}

#[test]
fn encode_matches_golden_bytes() {
    let req = golden_request_value();
    let mut buf = vec![0u8; req.encoded_len().unwrap()];
    let n = req.encode(&mut buf).unwrap();
    assert_eq!(n, 134);
    assert_eq!(buf, golden_request());
}

#[test]
fn decode_matches_golden_bytes() {
    let golden = golden_request();
    let decoded = decode(&golden).unwrap();
    assert_eq!(decoded.method, ReqMethod::Get);
    assert_eq!(decoded.scheme, UrlScheme::Http);
    assert_eq!(decoded.upgrade_proto, UpgradeProto::Na);
    assert_eq!(decoded.target_authority, b"");
    assert_eq!(decoded.target_path, b"/index.html");
    assert_eq!(decoded.target_query, b"");
    assert_eq!(decoded.body, b"abc");
    let headers: Vec<DecodedHeader<'_>> = decoded.headers().collect();
    assert_eq!(headers.len(), 2);
    assert_eq!(headers[0].flags, FieldLineFlags::empty());
    assert_eq!(
        headers[0].name,
        DecodedHeaderName::Known(header_name::ACCEPT)
    );
    assert_eq!(headers[0].value, b"text/html");
    assert_eq!(
        headers[1].flags,
        FieldLineFlags(FieldLineFlags::CUSTOM_NAME)
    );
    assert_eq!(headers[1].name, DecodedHeaderName::Custom(b"X-Test"));
    assert_eq!(headers[1].value, b"1");
}

#[test]
fn encode_then_decode_round_trip() {
    let req = golden_request_value();
    let mut buf = vec![0u8; req.encoded_len().unwrap()];
    let n = req.encode(&mut buf).unwrap();
    let decoded = decode(&buf[..n]).unwrap();
    assert_eq!(decoded.method, req.method);
    assert_eq!(decoded.scheme, req.scheme);
    assert_eq!(decoded.target_path, req.target_path);
    assert_eq!(decoded.body, req.body);
    let decoded_headers: Vec<_> = decoded.headers().collect();
    assert_eq!(decoded_headers.len(), req.headers.len());
    assert_eq!(decoded_headers[0].value, b"text/html");
    assert_eq!(
        decoded_headers[1].name,
        DecodedHeaderName::Custom(b"X-Test")
    );
    assert_eq!(decoded_headers[1].value, b"1");
}

#[test]
fn decode_minimal_request() {
    let req = Request {
        method: ReqMethod::Post,
        scheme: UrlScheme::Https,
        target_path: b"/",
        headers: &[],
        body: b"",
    };
    let mut buf = vec![0u8; req.encoded_len().unwrap()];
    let n = req.encode(&mut buf).unwrap();
    let decoded = decode(&buf[..n]).unwrap();
    assert_eq!(decoded.method, ReqMethod::Post);
    assert_eq!(decoded.scheme, UrlScheme::Https);
    assert_eq!(decoded.target_path, b"/");
    assert_eq!(decoded.body, b"");
    assert_eq!(decoded.headers().count(), 0);
    assert_eq!(n, 89); // 88 + 1 path byte + 0 headers + 0 body
}

#[test]
fn decode_rejects_truncated() {
    let golden = golden_request();
    for cut in 0..golden.len() {
        assert_eq!(
            decode(&golden[..cut]).unwrap_err(),
            DecodeError::Truncated,
            "cut at {cut}"
        );
    }
}

#[test]
fn decode_rejects_trailing_data() {
    let mut golden = golden_request();
    golden.push(0);
    assert_eq!(decode(&golden).unwrap_err(), DecodeError::TrailingData);
}

#[test]
fn decode_rejects_bad_discriminants() {
    let mut b = golden_request();
    // Unknown msg type.
    b[0] = 7;
    assert_eq!(
        decode(&b).unwrap_err(),
        DecodeError::InvalidMsgType { value: 7 }
    );
    b[0] = 1; // HTTP_MSG_REPLY: not publishable in this seam.
    assert_eq!(
        decode(&b).unwrap_err(),
        DecodeError::InvalidMsgType { value: 1 }
    );
    let mut b = golden_request();
    // Unknown method byte (offset 4).
    b[4] = 6;
    assert_eq!(
        decode(&b).unwrap_err(),
        DecodeError::InvalidMethod { value: 6 }
    );
    let mut b = golden_request();
    // PTR data type (offset 8): out of seam scope.
    b[8] = 1;
    assert_eq!(
        decode(&b).unwrap_err(),
        DecodeError::InvalidDataType { value: 1 }
    );
    let mut b = golden_request();
    // Unknown scheme (offset 24).
    b[24] = 9;
    assert_eq!(
        decode(&b).unwrap_err(),
        DecodeError::InvalidScheme { value: 9 }
    );
    let mut b = golden_request();
    // Unknown upgrade proto (offset 80).
    b[80] = 4;
    assert_eq!(
        decode(&b).unwrap_err(),
        DecodeError::InvalidUpgradeProto { value: 4 }
    );
}

#[test]
fn decode_rejects_bad_offsets() {
    let mut b = golden_request();
    // headers_offset (52) disagrees with target_path_len (40 = 11).
    b[52] = 12;
    assert_eq!(decode(&b).unwrap_err(), DecodeError::LayoutMismatch);
    let mut b = golden_request();
    // body_offset (60) must equal headers_offset + headers_len = 51.
    b[60] = 50;
    assert_eq!(decode(&b).unwrap_err(), DecodeError::LayoutMismatch);
    let mut b = golden_request();
    // data.len (16) must equal 54.
    b[16] = 53;
    assert_eq!(decode(&b).unwrap_err(), DecodeError::LayoutMismatch);
    let mut b = golden_request();
    // headers_len = u32::MAX: headers_offset + headers_len overflows.
    b[56..60].copy_from_slice(&u32::MAX.to_le_bytes());
    assert_eq!(decode(&b).unwrap_err(), DecodeError::LengthOverflow);
    let mut b = golden_request();
    // data.len = u64::MAX: fails the data_len == body_end equality.
    b[16..24].copy_from_slice(&u64::MAX.to_le_bytes());
    assert_eq!(decode(&b).unwrap_err(), DecodeError::LayoutMismatch);
    let mut b = golden_request();
    // Authority span (offset 28, len 32) outside the 54-byte data area.
    b[28..32].copy_from_slice(&60u32.to_le_bytes());
    b[32..36].copy_from_slice(&1u32.to_le_bytes());
    assert_eq!(decode(&b).unwrap_err(), DecodeError::InvalidDataSpan);
}

/// Build a wire frame with offsets kept consistent with the given path,
/// header-region and body bytes, independent of the codec.
fn build_frame(path: &[u8], region: &[u8], body: &[u8]) -> Vec<u8> {
    let mut b = Vec::new();
    b.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]); // type, method, data.type
    b.extend_from_slice(&[0, 0, 0, 0]); // struct padding: type @0, len @8
    b.extend_from_slice(&((path.len() + region.len() + body.len()) as u64).to_le_bytes());
    b.push(0); // scheme
    b.extend_from_slice(&[0, 0, 0]); // struct padding
    b.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0]); // authority off/len
    b.extend_from_slice(&0u32.to_le_bytes()); // path offset
    b.extend_from_slice(&(path.len() as u32).to_le_bytes());
    b.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0]); // query off/len
    b.extend_from_slice(&(path.len() as u32).to_le_bytes()); // headers offset
    b.extend_from_slice(&(region.len() as u32).to_le_bytes()); // headers len
    b.extend_from_slice(&((path.len() + region.len()) as u32).to_le_bytes()); // body offset
    b.extend_from_slice(&(body.len() as u64).to_le_bytes());
    b.extend_from_slice(&0u64.to_le_bytes()); // headers_ctx
    b.push(0); // upgrade proto
    b.extend_from_slice(&[0; 7]); // tail padding
    b.extend_from_slice(path);
    b.extend_from_slice(region);
    b.extend_from_slice(body);
    b
}

#[test]
fn decode_rejects_bad_header_lists() {
    // Region too short for even one entry (needs >= 8 bytes).
    let frame = build_frame(b"/", &[0, 0, 0, 0], b"");
    assert_eq!(decode(&frame).unwrap_err(), DecodeError::HeaderListOverrun);
    // Known entry declaring value_len 5 with only 8 of 13 bytes present.
    let mut region = vec![0, 0, 0, 0, 5, 0, 0, 0];
    let frame = build_frame(b"/", &region, b"");
    assert_eq!(decode(&frame).unwrap_err(), DecodeError::HeaderListOverrun);
    // Custom entry whose name_len claims bytes past the region end.
    region = vec![4, 0, 100, 0];
    let frame = build_frame(b"/", &region, b"");
    assert_eq!(decode(&frame).unwrap_err(), DecodeError::HeaderListOverrun);
    // Valid entry followed by a 2-byte stub: leftover shorter than the
    // 8-byte entry prefix.
    let mut region = vec![0, 0, 0, 0, 1, 0, 0, 0, b'a'];
    region.extend_from_slice(&[0, 0]);
    let frame = build_frame(b"/", &region, b"");
    assert_eq!(decode(&frame).unwrap_err(), DecodeError::HeaderListOverrun);
    // Positive control: one exact-fit known entry decodes.
    let region = vec![0, 0, 0, 0, 1, 0, 0, 0, b'a'];
    let frame = build_frame(b"/", &region, b"");
    let decoded = decode(&frame).unwrap();
    let headers: Vec<_> = decoded.headers().collect();
    assert_eq!(headers.len(), 1);
    assert_eq!(headers[0].name, DecodedHeaderName::Known(0));
    assert_eq!(headers[0].value, b"a");
}

#[test]
fn encode_rejects_bad_input() {
    let req = golden_request_value();
    let mut small = [0u8; 10];
    assert_eq!(
        req.encode(&mut small),
        Err(EncodeError::BufferTooSmall {
            needed: 134,
            available: 10
        })
    );
    // Custom name longer than u16::MAX.
    let long = Request {
        method: ReqMethod::Get,
        scheme: UrlScheme::Http,
        target_path: b"/",
        headers: &[AppHeader::Custom {
            flags: FieldLineFlags::empty(),
            name: &[0u8; u16::MAX as usize + 1],
            value: b"",
        }],
        body: b"",
    };
    assert_eq!(long.encoded_len(), Err(EncodeError::LengthOverflow));
    // CUSTOM_NAME bit set by the caller.
    let reserved = Request {
        method: ReqMethod::Get,
        scheme: UrlScheme::Http,
        target_path: b"/",
        headers: &[AppHeader::Known {
            flags: FieldLineFlags(FieldLineFlags::CUSTOM_NAME),
            name: header_name::ACCEPT,
            value: b"v",
        }],
        body: b"",
    };
    assert_eq!(reserved.encoded_len(), Err(EncodeError::ReservedFlag));
}

#[test]
fn encode_custom_never_index_flags() {
    // Custom header carrying NEVER_INDEX must encode flags = 4 | 2 = 6.
    let req = Request {
        method: ReqMethod::Put,
        scheme: UrlScheme::Http,
        target_path: b"/",
        headers: &[AppHeader::Custom {
            flags: FieldLineFlags(FieldLineFlags::NEVER_INDEX),
            name: b"X-A",
            value: b"b",
        }],
        body: b"",
    };
    let mut buf = vec![0u8; req.encoded_len().unwrap()];
    let n = req.encode(&mut buf).unwrap();
    assert_eq!(
        decode(&buf[..n])
            .unwrap()
            .headers()
            .next()
            .unwrap()
            .flags
            .bits(),
        6
    );
}

// --- FIFO publish -----------------------------------------------------------

/// A local FIFO of `capacity` data bytes backed by a private 1 MiB segment.
fn local_fifo(capacity: usize) -> Fifo {
    Fifo::new(Segment::local(1 << 20), capacity).expect("local FIFO")
}

/// Read `len` published bytes out of `fifo`.
fn read_published(fifo: &Fifo, len: usize) -> Vec<u8> {
    let mut out = vec![0u8; len];
    let mut reader = fifo;
    reader.read_exact(&mut out).unwrap();
    out
}

#[test]
fn publish_request_writes_vpp_order_and_round_trips() {
    // The published FIFO bytes must equal the literal VPP layout byte for
    // byte: 88-byte msg header, then the target path, then the header list,
    // then the body (`hc_request`, http_client.c:229-250). `golden_request`
    // writes the path before the headers independently of the codec.
    let fifo = local_fifo(8192);
    let req = golden_request_value();
    publish_request(&fifo, &req).unwrap();
    // Successful publishes arm no deq notification.
    assert!(!fifo.needs_deq_notification(1));
    let observed = read_published(&fifo, 134);
    assert_eq!(observed, golden_request());
    // The published bytes decode back to the same request.
    let decoded = decode(&observed).unwrap();
    assert_eq!(decoded.method, req.method);
    assert_eq!(decoded.scheme, req.scheme);
    assert_eq!(decoded.target_path, req.target_path);
    assert_eq!(decoded.body, req.body);
    let headers: Vec<_> = decoded.headers().collect();
    assert_eq!(headers.len(), 2);
    assert_eq!(headers[0].value, b"text/html");
    assert_eq!(headers[1].name, DecodedHeaderName::Custom(b"X-Test"));
    assert_eq!(headers[1].value, b"1");
}

#[test]
fn publish_request_capacity_preflight_leaves_fifo_unchanged() {
    // 40-byte path + 1-byte body: 88 + 41 = 129 bytes, one byte over a
    // 128-byte FIFO.
    let path = [b'a'; 40];
    let req = Request {
        method: ReqMethod::Get,
        scheme: UrlScheme::Http,
        target_path: &path,
        headers: &[],
        body: b"x",
    };
    assert_eq!(req.encoded_len().unwrap(), 129);
    let fifo = local_fifo(128);
    assert_eq!(
        publish_request(&fifo, &req),
        Err(PublishError::Capacity {
            requested: 129,
            available: 128
        })
    );
    // The preflight armed the want-deq notification flag and the FIFO holds
    // zero bytes: nothing was reserved or published.
    assert!(fifo.needs_deq_notification(1));
    assert_eq!(fifo.max_dequeue(), 0);
}

#[test]
fn publish_request_encode_error_leaves_fifo_usable() {
    let fifo = local_fifo(8192);
    // A caller-set CUSTOM_NAME flag is rejected before any reservation, and
    // arms no notification.
    let bad = Request {
        method: ReqMethod::Get,
        scheme: UrlScheme::Http,
        target_path: b"/",
        headers: &[AppHeader::Known {
            flags: FieldLineFlags(FieldLineFlags::CUSTOM_NAME),
            name: header_name::ACCEPT,
            value: b"v",
        }],
        body: b"",
    };
    assert_eq!(
        publish_request(&fifo, &bad),
        Err(PublishError::Encode(EncodeError::ReservedFlag))
    );
    assert_eq!(fifo.max_dequeue(), 0);
    assert!(!fifo.needs_deq_notification(1));
    // The same FIFO still publishes a valid request afterwards.
    let req = golden_request_value();
    publish_request(&fifo, &req).unwrap();
    assert_eq!(read_published(&fifo, 134), golden_request());
}

#[test]
fn publish_request_multi_chunk_body() {
    // An 8192-byte FIFO carves 4096-byte data chunks; a 5000-byte body
    // pushes the request across the chunk boundary, so the reservation and
    // scatter copy span two chunks.
    let body: Vec<u8> = (0..5000).map(|i| (i % 251) as u8).collect();
    let req = Request {
        method: ReqMethod::Put,
        scheme: UrlScheme::Http,
        target_path: b"/upload.bin",
        headers: &GOLDEN_HEADERS,
        body: &body,
    };
    let total = req.encoded_len().unwrap();
    assert!(total > 4096);
    let fifo = local_fifo(8192);
    publish_request(&fifo, &req).unwrap();
    assert_eq!(fifo.max_dequeue(), total);
    let observed = read_published(&fifo, total);
    // The contiguous encode of the same request is the reference layout.
    let mut reference = vec![0u8; total];
    req.encode(&mut reference).unwrap();
    assert_eq!(observed, reference);
    let decoded = decode(&observed).unwrap();
    assert_eq!(decoded.target_path, b"/upload.bin");
    assert_eq!(decoded.body, body.as_slice());
}

// --- Inbound FIFO publish ----------------------------------------------------

/// Little-endian `u32` at `off` in a published frame (tests only; the codec
/// keeps its own checked readers).
fn le32(buf: &[u8], off: usize) -> u32 {
    u32::from_le_bytes(buf[off..off + 4].try_into().unwrap())
}

fn le64(buf: &[u8], off: usize) -> u64 {
    u64::from_le_bytes(buf[off..off + 8].try_into().unwrap())
}

/// Inbound request exercising every request-target pseudo field: authority,
/// path, query, the two regular headers and a body.
fn inbound_request_value() -> InboundRequest<'static> {
    InboundRequest {
        method: ReqMethod::Post,
        scheme: UrlScheme::Http,
        target_authority: b"example.com",
        target_path: b"/index.html",
        target_query: b"a=1",
        headers: &GOLDEN_HEADERS,
        body: b"abc",
    }
}

/// Expected on-FIFO bytes for `inbound_request_value`, built independently of
/// the codec. The data area tiles [authority][path][query][header list][body]:
/// 11 + 11 + 3 + 32 + 3 = 60 bytes after the 88-byte msg header, 148 total.
fn golden_inbound_request() -> Vec<u8> {
    let mut b = Vec::with_capacity(148);
    // http_msg_t: type=REQUEST(0), method_or_code=POST(1), data.type=INLINE(0).
    b.extend_from_slice(&[0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0]);
    b.extend_from_slice(&[0, 0, 0, 0]); // `http_msg_data_t` padding: type @0, len @8
    // data.len = 11 + 11 + 3 + 32 + 3 = 60.
    b.extend_from_slice(&60u64.to_le_bytes());
    b.push(0); // scheme: HTTP
    b.extend_from_slice(&[0, 0, 0]); // struct padding
    // authority @28 off=0 len=11; path @36 off=11 len=11; query @44 off=22
    // len=3.
    b.extend_from_slice(&0u32.to_le_bytes());
    b.extend_from_slice(&11u32.to_le_bytes());
    b.extend_from_slice(&11u32.to_le_bytes());
    b.extend_from_slice(&11u32.to_le_bytes());
    b.extend_from_slice(&22u32.to_le_bytes());
    b.extend_from_slice(&3u32.to_le_bytes());
    // headers @52 off=25 len=32; body @60 off=57 len=3; headers_ctx @72 = 0.
    b.extend_from_slice(&25u32.to_le_bytes());
    b.extend_from_slice(&32u32.to_le_bytes());
    b.extend_from_slice(&57u32.to_le_bytes());
    b.extend_from_slice(&3u64.to_le_bytes());
    b.extend_from_slice(&0u64.to_le_bytes()); // headers_ctx
    b.push(0); // upgrade_proto: NA
    b.extend_from_slice(&[0; 7]); // struct tail padding
    assert_eq!(b.len(), 88);
    b.extend_from_slice(b"example.com");
    b.extend_from_slice(b"/index.html");
    b.extend_from_slice(b"a=1");
    // Entry 1: known header ACCEPT (4), flags 0, value_len 9:
    // [flags:u16][name:u16][value_len:u32][value] = 8 + 9 = 17 bytes.
    b.extend_from_slice(&[0, 0]);
    b.extend_from_slice(&4u16.to_le_bytes());
    b.extend_from_slice(&9u32.to_le_bytes());
    b.extend_from_slice(b"text/html");
    // Entry 2: custom name "X-Test", flags CUSTOM_NAME(4), name_len 6,
    // value_len 1: 8 + 6 + 1 = 15 bytes.
    b.extend_from_slice(&[4, 0]);
    b.extend_from_slice(&6u16.to_le_bytes());
    b.extend_from_slice(b"X-Test");
    b.extend_from_slice(&1u32.to_le_bytes());
    b.extend_from_slice(b"1");
    b.extend_from_slice(b"abc");
    assert_eq!(b.len(), 148);
    b
}

#[test]
fn publish_inbound_request_writes_vpp_order_and_spans() {
    let fifo = local_fifo(8192);
    let req = inbound_request_value();
    publish_inbound_request(&fifo, &req).unwrap();
    // Successful publishes arm no deq notification.
    assert!(!fifo.needs_deq_notification(1));
    let observed = read_published(&fifo, 148);
    assert_eq!(observed, golden_inbound_request());
    // Fixed-header spans: authority off/len @28/32, path @36/40, query @44/48,
    // headers @52/56, body @60/64, headers_ctx @72 = 0, data.len @16 = 60.
    assert_eq!(le64(&observed, 16), 60);
    assert_eq!(le32(&observed, 28), 0);
    assert_eq!(le32(&observed, 32), 11);
    assert_eq!(le32(&observed, 36), 11);
    assert_eq!(le32(&observed, 40), 11);
    assert_eq!(le32(&observed, 44), 22);
    assert_eq!(le32(&observed, 48), 3);
    assert_eq!(le32(&observed, 52), 25);
    assert_eq!(le32(&observed, 56), 32);
    assert_eq!(le32(&observed, 60), 57);
    assert_eq!(le64(&observed, 64), 3);
    assert_eq!(le64(&observed, 72), 0);
    // Pseudo-field spans tile the data area in order; the authority bytes are
    // NOT part of the header region. The regular header list begins after the
    // query, at 88 + 25 = 113, with the first entry's prefix
    // [flags:u16][name:u16][value_len:u32].
    assert_eq!(&observed[88..99], b"example.com");
    assert_eq!(&observed[99..110], b"/index.html");
    assert_eq!(&observed[110..113], b"a=1");
    assert_eq!(&observed[113..121], &[0, 0, 4, 0, 9, 0, 0, 0]);
    assert_eq!(&observed[145..148], b"abc");
}

#[test]
fn publish_inbound_request_round_trips_through_decode() {
    // With empty authority and query the data area collapses to the app-writer
    // layout (path at 0, headers after, body last, data.len the sum including
    // the body), so the frame round-trips through `decode` unchanged.
    let fifo = local_fifo(8192);
    let req = InboundRequest {
        method: ReqMethod::Post,
        scheme: UrlScheme::Https,
        target_authority: b"",
        target_path: b"/x",
        target_query: b"",
        headers: &GOLDEN_HEADERS,
        body: b"abc",
    };
    publish_inbound_request(&fifo, &req).unwrap();
    let observed = read_published(&fifo, 125);
    let decoded = decode(&observed).unwrap();
    assert_eq!(decoded.method, req.method);
    assert_eq!(decoded.scheme, req.scheme);
    assert_eq!(decoded.target_authority, b"");
    assert_eq!(decoded.target_path, b"/x");
    assert_eq!(decoded.target_query, b"");
    assert_eq!(decoded.body, b"abc");
    // The header iterator begins at the regular header list, right after the
    // path span.
    let headers: Vec<_> = decoded.headers().collect();
    assert_eq!(headers.len(), 2);
    assert_eq!(headers[0].name, DecodedHeaderName::Known(header_name::ACCEPT));
    assert_eq!(headers[0].value, b"text/html");
    assert_eq!(headers[1].name, DecodedHeaderName::Custom(b"X-Test"));
    assert_eq!(headers[1].value, b"1");
}

#[test]
fn publish_inbound_request_empty_query_and_body() {
    // Authority "h", path "/", empty query, no headers, empty body: data area
    // = [h][/], 88 + 2 = 90 bytes total. Offsets stay consistent with the
    // empty spans: query @2 len 0, headers @2 len 0, body @2 len 0.
    let fifo = local_fifo(8192);
    let req = InboundRequest {
        method: ReqMethod::Get,
        scheme: UrlScheme::Http,
        target_authority: b"h",
        target_path: b"/",
        target_query: b"",
        headers: &[],
        body: b"",
    };
    publish_inbound_request(&fifo, &req).unwrap();
    let observed = read_published(&fifo, 90);
    assert_eq!(&observed[88..89], b"h");
    assert_eq!(&observed[89..90], b"/");
    assert_eq!(le64(&observed, 16), 2);
    assert_eq!(le32(&observed, 28), 0);
    assert_eq!(le32(&observed, 32), 1);
    assert_eq!(le32(&observed, 36), 1);
    assert_eq!(le32(&observed, 40), 1);
    assert_eq!(le32(&observed, 44), 2);
    assert_eq!(le32(&observed, 52), 2);
    assert_eq!(le32(&observed, 60), 2);
}

#[test]
fn decode_server_layout_golden() {
    // The server layout puts the authority first and tiles the data area as
    // [authority][path][query][header list][body]; `decode` accepts it via
    // the checked tiling (no path-at-0 assumption).
    let fifo = local_fifo(8192);
    publish_inbound_request(&fifo, &inbound_request_value()).unwrap();
    let observed = read_published(&fifo, 148);
    let decoded = decode(&observed).unwrap();
    assert_eq!(decoded.method, ReqMethod::Post);
    assert_eq!(decoded.scheme, UrlScheme::Http);
    assert_eq!(decoded.upgrade_proto, UpgradeProto::Na);
    assert_eq!(decoded.target_authority, b"example.com");
    assert_eq!(decoded.target_path, b"/index.html");
    assert_eq!(decoded.target_query, b"a=1");
    assert_eq!(decoded.body, b"abc");
    // The header iterator begins at the regular header list, right after the
    // query span, and never sees the authority/path/query pseudo fields.
    let headers: Vec<_> = decoded.headers().collect();
    assert_eq!(headers.len(), 2);
    assert_eq!(headers[0].name, DecodedHeaderName::Known(header_name::ACCEPT));
    assert_eq!(headers[0].value, b"text/html");
    assert_eq!(headers[1].name, DecodedHeaderName::Custom(b"X-Test"));
    assert_eq!(headers[1].value, b"1");
}

#[test]
fn decode_rejects_bad_server_layout_offsets() {
    // Negative offset mutations of the server-layout golden frame: each
    // tiling equality is violated independently.
    let mut b = golden_inbound_request();
    // path_offset (36) must equal authority_end = 0 + 11.
    b[36..40].copy_from_slice(&10u32.to_le_bytes());
    assert_eq!(decode(&b).unwrap_err(), DecodeError::LayoutMismatch);
    let mut b = golden_inbound_request();
    // Non-empty query_offset (44) must equal path_end = 11 + 11 = 22.
    b[44..48].copy_from_slice(&21u32.to_le_bytes());
    assert_eq!(decode(&b).unwrap_err(), DecodeError::LayoutMismatch);
    let mut b = golden_inbound_request();
    // headers_offset (52) must equal max(path_end, query_end) = 25.
    b[52..56].copy_from_slice(&24u32.to_le_bytes());
    assert_eq!(decode(&b).unwrap_err(), DecodeError::LayoutMismatch);
    let mut b = golden_inbound_request();
    // body_offset (60) must equal headers_end = 25 + 32 = 57.
    b[60..64].copy_from_slice(&56u32.to_le_bytes());
    assert_eq!(decode(&b).unwrap_err(), DecodeError::LayoutMismatch);
    let mut b = golden_inbound_request();
    // data.len (16) must equal body_end = 57 + 3 = 60.
    b[16..24].copy_from_slice(&59u64.to_le_bytes());
    assert_eq!(decode(&b).unwrap_err(), DecodeError::LayoutMismatch);
    // A pseudo-field span leaving the data area is still InvalidDataSpan.
    let mut b = golden_inbound_request();
    b[28..32].copy_from_slice(&60u32.to_le_bytes());
    b[32..36].copy_from_slice(&1u32.to_le_bytes());
    assert_eq!(decode(&b).unwrap_err(), DecodeError::InvalidDataSpan);
}

#[test]
fn publish_inbound_request_capacity_preflight_leaves_fifo_unchanged() {
    // 40-byte path + 1-byte body: 88 + 41 = 129 bytes, one byte over a
    // 128-byte FIFO.
    let path = [b'a'; 40];
    let req = InboundRequest {
        method: ReqMethod::Get,
        scheme: UrlScheme::Http,
        target_authority: b"",
        target_path: &path,
        target_query: b"",
        headers: &[],
        body: b"x",
    };
    assert_eq!(req.encoded_len().unwrap(), 129);
    let fifo = local_fifo(128);
    assert_eq!(
        publish_inbound_request(&fifo, &req),
        Err(PublishError::Capacity {
            requested: 129,
            available: 128
        })
    );
    // The preflight armed the want-deq notification flag and the FIFO holds
    // zero bytes: nothing was reserved or published.
    assert!(fifo.needs_deq_notification(1));
    assert_eq!(fifo.max_dequeue(), 0);
}

#[test]
fn publish_inbound_request_encode_error_leaves_fifo_usable() {
    let fifo = local_fifo(8192);
    // A caller-set CUSTOM_NAME flag is rejected before any reservation, and
    // arms no notification.
    let bad = InboundRequest {
        method: ReqMethod::Get,
        scheme: UrlScheme::Http,
        target_authority: b"",
        target_path: b"/",
        target_query: b"",
        headers: &[AppHeader::Known {
            flags: FieldLineFlags(FieldLineFlags::CUSTOM_NAME),
            name: header_name::ACCEPT,
            value: b"v",
        }],
        body: b"",
    };
    assert_eq!(
        publish_inbound_request(&fifo, &bad),
        Err(PublishError::Encode(EncodeError::ReservedFlag))
    );
    assert_eq!(fifo.max_dequeue(), 0);
    assert!(!fifo.needs_deq_notification(1));
    // The same FIFO still publishes a valid inbound request afterwards.
    publish_inbound_request(&fifo, &inbound_request_value()).unwrap();
    assert_eq!(read_published(&fifo, 148), golden_inbound_request());
}
