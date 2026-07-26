import argparse
import socket
import time
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Probe TCP reachability and liveness")
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("--attempts", type=int, default=1)
    parser.add_argument("--connect-timeout", type=float, default=2.0)
    parser.add_argument("--retry-delay", type=float, default=1.0)
    parser.add_argument("--idle-seconds", type=float, default=0.0)
    parser.add_argument("--send-bytes", type=int, default=0)
    parser.add_argument("--chunk-bytes", type=int, default=1024)
    parser.add_argument("--ready-file")
    parser.add_argument("--continue-file")
    parser.add_argument("--continue-timeout", type=float, default=10.0)
    return parser.parse_args()


def wait_for_file(path: Path, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            return
        time.sleep(0.05)
    raise TimeoutError(f"timed out waiting for {path}")


def assert_connection_open(connection: socket.socket) -> None:
    connection.settimeout(0.1)
    try:
        received = connection.recv(1)
    except socket.timeout:
        return
    if received == b"":
        raise ConnectionError("peer closed the TCP connection")
    raise ConnectionError("probe received unexpected application payload")


def run_probe(connection: socket.socket, args: argparse.Namespace) -> None:
    connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    if args.ready_file:
        Path(args.ready_file).touch()
    if args.continue_file:
        wait_for_file(Path(args.continue_file), args.continue_timeout)

    remaining = args.send_bytes
    pattern = bytes(index % 251 for index in range(args.chunk_bytes))
    while remaining > 0:
        chunk = pattern[: min(remaining, len(pattern))]
        connection.sendall(chunk)
        remaining -= len(chunk)

    if args.idle_seconds > 0:
        time.sleep(args.idle_seconds)
    assert_connection_open(connection)


def main() -> None:
    args = parse_args()
    if args.send_bytes < 0:
        raise SystemExit("--send-bytes must be non-negative")
    if args.chunk_bytes <= 0:
        raise SystemExit("--chunk-bytes must be positive")

    last_error: OSError | None = None
    for attempt in range(1, args.attempts + 1):
        try:
            with socket.create_connection(
                (args.host, args.port), timeout=args.connect_timeout
            ) as connection:
                run_probe(connection, args)
                print(f"TCP connection succeeded on attempt {attempt}")
                return
        except (OSError, TimeoutError) as error:
            last_error = error
            if attempt < args.attempts:
                time.sleep(args.retry_delay)

    raise SystemExit(
        f"TCP connection failed after {args.attempts} attempts: {last_error}"
    )


if __name__ == "__main__":
    main()
