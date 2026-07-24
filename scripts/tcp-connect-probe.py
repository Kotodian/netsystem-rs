import argparse
import socket
import time


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Probe TCP reachability")
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("--attempts", type=int, default=1)
    parser.add_argument("--connect-timeout", type=float, default=2.0)
    parser.add_argument("--retry-delay", type=float, default=1.0)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    last_error: OSError | None = None
    for attempt in range(1, args.attempts + 1):
        try:
            with socket.create_connection(
                (args.host, args.port), timeout=args.connect_timeout
            ):
                print(f"TCP connection succeeded on attempt {attempt}")
                return
        except OSError as error:
            last_error = error
            if attempt < args.attempts:
                time.sleep(args.retry_delay)

    raise SystemExit(
        f"TCP connection failed after {args.attempts} attempts: {last_error}"
    )


if __name__ == "__main__":
    main()
