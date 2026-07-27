"""TCP lab probe: connect, exact-echo verification, and idle liveness.

The probe opens an explicit IPv4 TCP connection (setting TCP_MAXSEG before
connect and TCP_NODELAY after), optionally waits for the workflow to install
fault rules, then sends a position-dependent deterministic payload and reads
the exact same bytes back from the echo peer with bounded send/read
interleaving so the payload can exceed the peer FIFO capacity without
deadlocking either direction.

Failure modes are explicit: echoed-byte mismatch, early EOF, missing echo
bytes at the deadline, unexpected trailing data, and close during idle.
"""

import argparse
import select
import socket
import time
from pathlib import Path

RECV_CHUNK_BYTES = 65536


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Probe TCP echo and liveness")
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("--attempts", type=int, default=1)
    parser.add_argument("--connect-timeout", type=float, default=2.0)
    parser.add_argument("--retry-delay", type=float, default=1.0)
    parser.add_argument(
        "--mss", type=int, default=0, help="TCP_MAXSEG set before connect (0 = kernel default)"
    )
    parser.add_argument(
        "--echo-bytes", type=int, default=0, help="exact echo payload size (0 = no data phase)"
    )
    parser.add_argument("--chunk-bytes", type=int, default=4096)
    parser.add_argument(
        "--window-bytes",
        type=int,
        default=262144,
        help="maximum unacknowledged echo bytes in flight (sent - received)",
    )
    parser.add_argument("--echo-timeout", type=float, default=60.0)
    parser.add_argument("--idle-seconds", type=float, default=0.0)
    parser.add_argument("--ready-file")
    parser.add_argument("--continue-file")
    parser.add_argument("--continue-timeout", type=float, default=30.0)
    return parser.parse_args()


def payload_slice(offset: int, length: int) -> bytes:
    """Deterministic bytes whose value depends on absolute stream position."""
    return bytes(
        (index * 197 + (index >> 8) * 83 + (index >> 16) * 29 + 13) & 0xFF
        for index in range(offset, offset + length)
    )


def wait_for_file(path: Path, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            return
        time.sleep(0.05)
    raise SystemExit(f"timed out waiting for {path}")


def connect(args: argparse.Namespace) -> socket.socket:
    last_error: OSError | None = None
    for attempt in range(1, args.attempts + 1):
        connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            if args.mss > 0:
                connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_MAXSEG, args.mss)
            connection.settimeout(args.connect_timeout)
            connection.connect((args.host, args.port))
            connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            print(f"TCP connection succeeded on attempt {attempt}", flush=True)
            return connection
        except (OSError, TimeoutError) as error:
            connection.close()
            last_error = error
            if attempt < args.attempts:
                time.sleep(args.retry_delay)
    raise SystemExit(f"TCP connection failed after {args.attempts} attempts: {last_error}")


def run_echo(connection: socket.socket, args: argparse.Namespace) -> None:
    total = args.echo_bytes
    sent = 0
    received = 0
    deadline = time.monotonic() + args.echo_timeout
    connection.setblocking(False)
    while received < total:
        if time.monotonic() > deadline:
            raise SystemExit(
                f"echo incomplete at deadline: sent {sent}/{total}, "
                f"received {received}/{total} bytes"
            )
        want_write = sent < total and (sent - received) < args.window_bytes
        readable, writable, _ = select.select(
            [connection], [connection] if want_write else [], [], 0.5
        )
        if readable:
            data = connection.recv(RECV_CHUNK_BYTES)
            if data == b"":
                raise SystemExit(
                    f"peer closed before echo completed: sent {sent}, received {received}"
                )
            if received + len(data) > total:
                raise SystemExit(
                    f"received {received + len(data)} echo bytes, expected exactly {total}"
                )
            expected = payload_slice(received, len(data))
            if data != expected:
                mismatch = next(
                    index for index, byte in enumerate(data) if byte != expected[index]
                )
                raise SystemExit(
                    f"echo mismatch at byte {received + mismatch}: "
                    f"expected {expected[mismatch]:#04x}, got {data[mismatch]:#04x}"
                )
            received += len(data)
        if writable:
            budget = min(args.chunk_bytes, total - sent, args.window_bytes - (sent - received))
            if budget > 0:
                sent += connection.send(payload_slice(sent, budget))
    print(f"echo verified: {received} bytes matched exactly", flush=True)


def run_idle(connection: socket.socket, idle_seconds: float) -> None:
    deadline = time.monotonic() + idle_seconds
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        readable, _, _ = select.select([connection], [], [], min(remaining, 0.5))
        if not readable:
            continue
        data = connection.recv(RECV_CHUNK_BYTES)
        if data == b"":
            raise SystemExit("peer closed the connection during the idle window")
        raise SystemExit(f"unexpected trailing data during idle: {len(data)} bytes")


def main() -> None:
    args = parse_args()
    if (
        args.echo_bytes < 0
        or args.chunk_bytes <= 0
        or args.window_bytes <= 0
    ):
        raise SystemExit(
            "--echo-bytes must be >= 0; --chunk-bytes/--window-bytes must be > 0"
        )
    connection = connect(args)
    with connection:
        if args.ready_file:
            Path(args.ready_file).write_text(f"{connection.getsockname()[1]}\n")
        if args.continue_file:
            wait_for_file(Path(args.continue_file), args.continue_timeout)
        if args.echo_bytes > 0:
            run_echo(connection, args)
        if args.idle_seconds > 0:
            run_idle(connection, args.idle_seconds)
    print("probe completed", flush=True)


if __name__ == "__main__":
    main()
