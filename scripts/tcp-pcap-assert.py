"""Assert TCP lab packet behavior from a pcap file or `tcpdump -nn -S` text.

The input format is auto-detected from the pcap magic bytes. Binary pcap is
authoritative: tcpdump's one-line text omits `seq` on zero-length pure ACKs,
which makes keepalive probes (zero-length segments at SND.NXT - 1)
unobservable in text form.

Parses absolute sequence numbers, direction, flags, lengths, and SACK blocks
for the probe<->Hammer flow, always prints a machine-readable handshake
summary (ISNs and first data sequences, used by the workflow to install
absolute-sequence fault rules), and applies only the assertions requested on
the command line:

- handshake completeness (SYN / SYN-ACK / ACK with matching acknowledgments)
- keepalive probes: zero-length server segments at SEQ = SND.NXT - 1
- receiver SACK: server ACK carrying a valid SACK block above its ACK point
- server retransmission of an exact absolute sequence (RACK/TLP labs), with
  an optional upper bound on the delay between first and second transmission
- minimum server data transfer (unique span and data packet count)

Internal RACK/TLP attribution is asserted from hammerctl status counters by
the workflow; this script only proves on-the-wire packet shape.
"""

import argparse
import re
import struct
from pathlib import Path

SEQ_MOD = 1 << 32

# Magic -> (struct byte order, timestamp ticks per second).
PCAP_FORMATS = {
    0xA1B2C3D4: (">", 1_000_000),
    0xD4C3B2A1: ("<", 1_000_000),
    0xA1B23C4D: (">", 1_000_000_000),
    0x4D3CB2A1: ("<", 1_000_000_000),
}
LINKTYPE_NULL = 0  # 4-byte address-family header (macOS utun captures)
LINKTYPE_ETHERNET = 1
LINKTYPE_RAW = 101  # bare IP (Linux TUN captures)

LINE_RE = re.compile(
    r"(?P<time>\d{2}:\d{2}:\d{2}\.\d+)\s+IP\s+"
    r"(?P<src>\d+\.\d+\.\d+\.\d+)\.(?P<sport>\d+)\s+>\s+"
    r"(?P<dst>\d+\.\d+\.\d+\.\d+)\.(?P<dport>\d+):\s+"
    r"Flags\s+\[(?P<flags>[^\]]*)\](?P<rest>.*)"
)
SEQ_RE = re.compile(r"\bseq (\d+)(?::(\d+))?")
ACK_RE = re.compile(r"\back (\d+)")
LENGTH_RE = re.compile(r"\blength (\d+)")
OPTIONS_RE = re.compile(r"options \[([^\]]*)\]")
SACK_BLOCK_RE = re.compile(r"\{(\d+):(\d+)\}")


class Packet:
    def __init__(self, time_seconds, from_server, flags, seq, seq_end, ack, length, sack_blocks):
        self.time_seconds = time_seconds
        self.from_server = from_server
        self.flags = flags
        self.seq = seq
        self.seq_end = seq_end
        self.ack = ack
        self.length = length
        self.sack_blocks = sack_blocks


def seq_offset(base: int, seq: int) -> int:
    return (seq - base) % SEQ_MOD


def parse_time(text: str) -> float:
    hours, minutes, seconds = text.split(":")
    return int(hours) * 3600 + int(minutes) * 60 + float(seconds)


def tcp_packet_from_frame(frame, linktype, time_seconds, server, port) -> Packet | None:
    if linktype == LINKTYPE_RAW:
        ip = frame
    elif linktype == LINKTYPE_NULL:
        ip = frame[4:]
    elif linktype == LINKTYPE_ETHERNET:
        if len(frame) < 14 or frame[12:14] != b"\x08\x00":
            return None
        ip = frame[14:]
    else:
        raise SystemExit(f"unsupported pcap linktype {linktype}")

    if len(ip) < 20 or ip[0] >> 4 != 4:
        return None
    header_len = (ip[0] & 0x0F) * 4
    total_length = int.from_bytes(ip[2:4], "big")
    if ip[9] != 6 or len(ip) < header_len + 20 or total_length < header_len + 20:
        return None
    src = ".".join(str(byte) for byte in ip[12:16])
    dst = ".".join(str(byte) for byte in ip[16:20])

    tcp = ip[header_len:]
    sport, dport, seq, ack = struct.unpack_from(">HHII", tcp, 0)
    src_is_server = src == server and sport == port
    dst_is_server = dst == server and dport == port
    if not src_is_server and not dst_is_server:
        return None
    data_offset = (tcp[12] >> 4) * 4
    flag_bits = tcp[13]
    flags = "".join(
        letter
        for bit, letter in ((0x02, "S"), (0x01, "F"), (0x04, "R"), (0x08, "P"), (0x20, "U"))
        if flag_bits & bit
    )
    if flag_bits & 0x10:
        flags += "."
    length = max(total_length - header_len - data_offset, 0)

    sack_blocks = []
    options = tcp[20:data_offset]
    cursor = 0
    while cursor < len(options):
        kind = options[cursor]
        if kind == 0:  # end of option list
            break
        if kind == 1:  # no-op pad
            cursor += 1
            continue
        if cursor + 2 > len(options):
            break
        option_len = options[cursor + 1]
        if option_len < 2 or cursor + option_len > len(options):
            break
        if kind == 5:  # SACK: pairs of absolute 32-bit block edges
            block = options[cursor + 2 : cursor + option_len]
            for start in range(0, len(block) - 7, 8):
                sack_blocks.append(struct.unpack_from(">II", block, start))
        cursor += option_len

    return Packet(
        time_seconds=time_seconds,
        from_server=src_is_server,
        flags=flags,
        seq=seq,
        seq_end=(seq + length) % SEQ_MOD,
        ack=ack if flag_bits & 0x10 else None,
        length=length,
        sack_blocks=sack_blocks,
    )


def parse_pcap_packets(data: bytes, server: str, port: int) -> list[Packet]:
    if len(data) < 24:
        raise SystemExit("pcap file is truncated before the end of its header")
    byte_order, ticks_per_second = PCAP_FORMATS[int.from_bytes(data[:4], "big")]
    linktype = struct.unpack_from(byte_order + "I", data, 20)[0]
    packets = []
    offset = 24
    while offset + 16 <= len(data):
        seconds, fraction, incl_len, _ = struct.unpack_from(byte_order + "IIII", data, offset)
        offset += 16
        if offset + incl_len > len(data):
            break  # live capture cut mid-record
        frame = data[offset : offset + incl_len]
        offset += incl_len
        packet = tcp_packet_from_frame(
            frame, linktype, seconds + fraction / ticks_per_second, server, port
        )
        if packet is not None:
            packets.append(packet)
    return packets


def parse_packets(path: Path, server: str, port: int) -> list[Packet]:
    data = path.read_bytes()
    if int.from_bytes(data[:4], "big") in PCAP_FORMATS:
        return parse_pcap_packets(data, server, port)
    packets = []
    for line in data.decode(encoding="utf-8", errors="replace").splitlines():
        match = LINE_RE.search(line)
        if match is None:
            continue
        src_is_server = match["src"] == server and int(match["sport"]) == port
        dst_is_server = match["dst"] == server and int(match["dport"]) == port
        if not src_is_server and not dst_is_server:
            continue
        rest = match["rest"]
        seq_match = SEQ_RE.search(rest)
        ack_match = ACK_RE.search(rest)
        length_match = LENGTH_RE.search(rest)
        options_match = OPTIONS_RE.search(rest)
        sack_blocks = []
        if options_match and "sack" in options_match[1]:
            sack_blocks = [
                (int(left), int(right))
                for left, right in SACK_BLOCK_RE.findall(options_match[1])
            ]
        seq = int(seq_match[1]) if seq_match else None
        seq_end = int(seq_match[2]) if seq_match and seq_match[2] else seq
        packets.append(
            Packet(
                time_seconds=parse_time(match["time"]),
                from_server=src_is_server,
                flags=match["flags"],
                seq=seq,
                seq_end=seq_end,
                ack=int(ack_match[1]) if ack_match else None,
                length=int(length_match[1]) if length_match else 0,
                sack_blocks=sack_blocks,
            )
        )
    return packets


def find_isns(packets: list[Packet]) -> tuple[int | None, int | None]:
    client_isn = None
    server_isn = None
    for packet in packets:
        if "S" not in packet.flags or packet.seq is None:
            continue
        if packet.from_server and server_isn is None:
            server_isn = packet.seq
        elif not packet.from_server and client_isn is None:
            client_isn = packet.seq
    return client_isn, server_isn


def data_packets(packets: list[Packet], from_server: bool) -> list[Packet]:
    return [
        packet
        for packet in packets
        if packet.from_server == from_server and packet.length > 0 and packet.seq is not None
    ]


def max_data_end(packets: list[Packet], from_server: bool, isn: int) -> int | None:
    ends = [packet.seq_end for packet in data_packets(packets, from_server)]
    if not ends:
        return None
    return max(ends, key=lambda end: seq_offset(isn, end))


def assert_handshake(packets, client_isn, server_isn) -> None:
    if client_isn is None or server_isn is None:
        raise SystemExit("handshake incomplete: SYN or SYN-ACK missing from capture")
    syn_ack_ok = any(
        packet.from_server
        and "S" in packet.flags
        and packet.ack == (client_isn + 1) % SEQ_MOD
        for packet in packets
    )
    if not syn_ack_ok:
        raise SystemExit("handshake incomplete: server SYN-ACK does not acknowledge client ISN+1")
    ack_ok = any(
        not packet.from_server
        and "S" not in packet.flags
        and packet.ack == (server_isn + 1) % SEQ_MOD
        for packet in packets
    )
    if not ack_ok:
        raise SystemExit("handshake incomplete: client never acknowledged server ISN+1")
    print("handshake=ok")


def count_keepalive_probes(packets, server_isn) -> int:
    snd_nxt = max_data_end(packets, from_server=True, isn=server_isn)
    if snd_nxt is None:
        snd_nxt = (server_isn + 1) % SEQ_MOD
    probe_seq = (snd_nxt - 1) % SEQ_MOD
    return sum(
        packet.from_server
        and packet.length == 0
        and packet.seq == probe_seq
        and not any(flag in packet.flags for flag in "SFRP")
        for packet in packets
    )


def count_sack_acks(packets) -> int:
    count = 0
    for packet in packets:
        if not packet.from_server or packet.ack is None:
            continue
        for left, right in packet.sack_blocks:
            above_ack = 0 < seq_offset(packet.ack, left) < SEQ_MOD // 2
            valid_span = 0 < seq_offset(left, right) < SEQ_MOD // 2
            if above_ack and valid_span:
                count += 1
                break
    return count


def assert_server_retransmit(packets, target_seq: int, within_seconds: float | None) -> None:
    transmissions = [
        packet.time_seconds
        for packet in data_packets(packets, from_server=True)
        if packet.seq == target_seq
    ]
    if len(transmissions) < 2:
        raise SystemExit(
            f"expected server retransmission of seq {target_seq}, "
            f"observed {len(transmissions)} transmission(s)"
        )
    delay = transmissions[1] - transmissions[0]
    print(f"server_retransmit_seq={target_seq} delay={delay:.3f}s")
    if within_seconds is not None and delay > within_seconds:
        raise SystemExit(
            f"retransmission of seq {target_seq} took {delay:.3f}s, "
            f"expected under {within_seconds:.3f}s"
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Assert TCP lab packets from a pcap file or tcpdump -nn -S text"
    )
    parser.add_argument("--input", required=True)
    parser.add_argument("--server", required=True, help="Hammer listener IPv4 address")
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("--assert-handshake", action="store_true")
    parser.add_argument("--min-keepalive-probes", type=int, default=0)
    parser.add_argument("--min-sack-acks", type=int, default=0)
    parser.add_argument("--require-server-retransmit", type=int, default=None)
    parser.add_argument("--retransmit-within", type=float, default=None)
    parser.add_argument("--min-server-data-bytes", type=int, default=0)
    parser.add_argument("--min-server-data-packets", type=int, default=0)
    args = parser.parse_args()

    packets = parse_packets(Path(args.input), args.server, args.port)
    client_isn, server_isn = find_isns(packets)
    print(f"packets={len(packets)}")
    if client_isn is not None:
        print(f"client_isn={client_isn}")
        print(f"client_first_data_seq={(client_isn + 1) % SEQ_MOD}")
    if server_isn is not None:
        print(f"server_isn={server_isn}")
        print(f"server_first_data_seq={(server_isn + 1) % SEQ_MOD}")

    if args.assert_handshake:
        assert_handshake(packets, client_isn, server_isn)

    if args.min_keepalive_probes > 0:
        if server_isn is None:
            raise SystemExit("cannot locate keepalive probes without the server ISN")
        probes = count_keepalive_probes(packets, server_isn)
        print(f"server_keepalive_probes={probes}")
        if probes < args.min_keepalive_probes:
            raise SystemExit(
                f"expected at least {args.min_keepalive_probes} keepalive probes at "
                f"SND.NXT-1, observed {probes}"
            )

    if args.min_sack_acks > 0:
        sack_acks = count_sack_acks(packets)
        print(f"server_sack_acks={sack_acks}")
        if sack_acks < args.min_sack_acks:
            raise SystemExit(
                f"expected at least {args.min_sack_acks} server ACKs with a valid "
                f"SACK block, observed {sack_acks}"
            )

    if args.require_server_retransmit is not None:
        assert_server_retransmit(packets, args.require_server_retransmit, args.retransmit_within)

    if args.min_server_data_bytes > 0 or args.min_server_data_packets > 0:
        if server_isn is None:
            raise SystemExit("cannot measure server data transfer without the server ISN")
        packet_count = len(data_packets(packets, from_server=True))
        max_end = max_data_end(packets, from_server=True, isn=server_isn)
        span = 0 if max_end is None else seq_offset((server_isn + 1) % SEQ_MOD, max_end)
        print(f"server_data_packets={packet_count}")
        print(f"server_data_bytes={span}")
        if span < args.min_server_data_bytes:
            raise SystemExit(
                f"expected at least {args.min_server_data_bytes} unique server data "
                f"bytes, observed {span}"
            )
        if packet_count < args.min_server_data_packets:
            raise SystemExit(
                f"expected at least {args.min_server_data_packets} server data "
                f"packets, observed {packet_count}"
            )


if __name__ == "__main__":
    main()
