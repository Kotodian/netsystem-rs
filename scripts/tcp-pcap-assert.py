"""Assert TCP lab packet behavior from `tcpdump -nn -S` text output.

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
from pathlib import Path

SEQ_MOD = 1 << 32

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


def parse_packets(path: Path, server: str, port: int) -> list[Packet]:
    packets = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
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
    parser = argparse.ArgumentParser(description="Assert TCP lab packets from tcpdump -nn -S text")
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
