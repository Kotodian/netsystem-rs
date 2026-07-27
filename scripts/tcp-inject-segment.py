"""Inject one IPv4 TCP data segment for receiver-side protocol labs."""

import argparse
import os
import socket
import struct


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inject one raw IPv4 TCP segment")
    parser.add_argument("--source", required=True)
    parser.add_argument("--destination", required=True)
    parser.add_argument("--source-port", required=True, type=int)
    parser.add_argument("--destination-port", required=True, type=int)
    parser.add_argument("--sequence", required=True, type=int)
    parser.add_argument("--acknowledgment", required=True, type=int)
    parser.add_argument("--window", type=int, default=508)
    parser.add_argument("--payload-offset", required=True, type=int)
    parser.add_argument("--payload-bytes", required=True, type=int)
    return parser.parse_args()


def payload_slice(offset: int, length: int) -> bytes:
    return bytes(
        (index * 197 + (index >> 8) * 83 + (index >> 16) * 29 + 13) & 0xFF
        for index in range(offset, offset + length)
    )


def internet_checksum(data: bytes) -> int:
    if len(data) % 2 != 0:
        data += b"\0"
    total = sum(struct.unpack(f"!{len(data) // 2}H", data))
    total = (total & 0xFFFF) + (total >> 16)
    total = (total & 0xFFFF) + (total >> 16)
    return (~total) & 0xFFFF


def tcp_segment(args: argparse.Namespace, source: bytes, destination: bytes) -> bytes:
    payload = payload_slice(args.payload_offset, args.payload_bytes)
    offset_and_flags = (5 << 12) | 0x18  # 20-byte header, PSH + ACK
    header = struct.pack(
        "!HHIIHHHH",
        args.source_port,
        args.destination_port,
        args.sequence,
        args.acknowledgment,
        offset_and_flags,
        args.window,
        0,
        0,
    )
    pseudo_header = source + destination + struct.pack(
        "!BBH", 0, socket.IPPROTO_TCP, len(header) + len(payload)
    )
    checksum = internet_checksum(pseudo_header + header + payload)
    header = struct.pack(
        "!HHIIHHHH",
        args.source_port,
        args.destination_port,
        args.sequence,
        args.acknowledgment,
        offset_and_flags,
        args.window,
        checksum,
        0,
    )
    return header + payload


def ipv4_packet(args: argparse.Namespace) -> bytes:
    source = socket.inet_aton(args.source)
    destination = socket.inet_aton(args.destination)
    segment = tcp_segment(args, source, destination)
    total_length = 20 + len(segment)
    header = struct.pack(
        "!BBHHHBBH4s4s",
        0x45,
        0,
        total_length,
        os.getpid() & 0xFFFF,
        0x4000,
        64,
        socket.IPPROTO_TCP,
        0,
        source,
        destination,
    )
    checksum = internet_checksum(header)
    header = struct.pack(
        "!BBHHHBBH4s4s",
        0x45,
        0,
        total_length,
        os.getpid() & 0xFFFF,
        0x4000,
        64,
        socket.IPPROTO_TCP,
        checksum,
        source,
        destination,
    )
    return header + segment


def main() -> None:
    args = parse_args()
    if not (1 <= args.source_port <= 65535 and 1 <= args.destination_port <= 65535):
        raise SystemExit("source and destination ports must be in 1..=65535")
    if not (0 <= args.sequence <= 0xFFFFFFFF and 0 <= args.acknowledgment <= 0xFFFFFFFF):
        raise SystemExit("sequence and acknowledgment must be 32-bit unsigned values")
    if not (0 <= args.window <= 65535):
        raise SystemExit("window must be a 16-bit unsigned value")
    if args.payload_offset < 0 or not (1 <= args.payload_bytes <= 65535 - 40):
        raise SystemExit("payload offset must be >= 0 and payload bytes must fit IPv4")

    packet = ipv4_packet(args)
    with socket.socket(socket.AF_INET, socket.SOCK_RAW, socket.IPPROTO_RAW) as raw:
        raw.setsockopt(socket.IPPROTO_IP, socket.IP_HDRINCL, 1)
        written = raw.sendto(packet, (args.destination, 0))
    if written != len(packet):
        raise SystemExit(f"raw TCP injection wrote {written}/{len(packet)} bytes")
    end_sequence = (args.sequence + args.payload_bytes) % (2**32)
    print(
        f"injected_sequence={args.sequence} injected_end={end_sequence} "
        f"payload_bytes={args.payload_bytes}"
    )


if __name__ == "__main__":
    main()
