#!/usr/bin/env python3
"""Banking CLI. Supply the server VM's real IP using --host."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal, InvalidOperation
import json
import socket
import sys


def exchange(host, port, payload, timeout=40):
    data = payload if isinstance(payload, bytes) else (json.dumps(payload) + "\n").encode("utf-8")
    with socket.create_connection((host, port), timeout=timeout) as connection:
        connection.settimeout(timeout)
        connection.sendall(data)
        with connection.makefile("rb") as stream:
            line = stream.readline(65_537)
        if not line or len(line) > 65_536:
            raise ConnectionError("server disconnected or sent an oversized response")
        return json.loads(line.decode("utf-8"))


def numeric_amount(text):
    try:
        value = Decimal(text)
        if not value.is_finite() or value <= 0 or value * 100 != (value * 100).to_integral_value():
            raise InvalidOperation
    except InvalidOperation:
        raise argparse.ArgumentTypeError("use a positive amount with at most two decimal places") from None
    # Keep the exact decimal spelling in the JSON wire format; never convert to binary float.
    return format(value, "f")


def make_payload(action, account, amount=None, demo_fail=False, demo_pause_ms=0):
    payload = json.dumps({"action": action, "account": account})[:-1]
    if amount is not None:
        payload += ', "amount": ' + amount
    if demo_fail:
        payload += ', "demo_fail": true'
    if demo_pause_ms:
        payload += ', "demo_pause_ms": ' + str(demo_pause_ms)
    return (payload + "}\n").encode("utf-8")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", type=int, default=5050)
    commands = parser.add_subparsers(dest="command", required=True)
    for action in ("balance", "deposit", "withdraw"):
        sub = commands.add_parser(action)
        sub.add_argument("account")
        if action != "balance":
            sub.add_argument("amount", type=numeric_amount)
            sub.add_argument("--demo-fail", action="store_true")
            sub.add_argument("--demo-pause-ms", type=int, default=0)
    raw = commands.add_parser("raw", help="send a raw JSON string, including malformed JSON for testing")
    raw.add_argument("payload")
    load = commands.add_parser("load", help="send concurrent requests without automatic retries")
    load.add_argument("action", choices=("withdraw", "deposit"))
    load.add_argument("account")
    load.add_argument("amount", type=numeric_amount)
    load.add_argument("--requests", type=int, default=100)
    load.add_argument("--workers", type=int, default=16)
    args = parser.parse_args()
    try:
        if args.command == "raw":
            result = exchange(args.host, args.port, (args.payload + "\n").encode("utf-8"))
        elif args.command == "load":
            if args.requests < 1 or args.workers < 1:
                parser.error("requests and workers must be positive")
            payload = make_payload(args.action, args.account, args.amount)
            def send(index):
                try:
                    response = exchange(args.host, args.port, payload)
                except (OSError, ValueError) as exc:
                    response = {"status": "unknown", "message": str(exc)}
                print(json.dumps({"index": index, **response}), flush=True)
                return response
            with ThreadPoolExecutor(max_workers=args.workers) as pool:
                responses = list(pool.map(send, range(1, args.requests + 1)))
            result = {"requests": args.requests,
                      "success": sum(r.get("status") == "success" for r in responses),
                      "errors": sum(r.get("status") == "error" for r in responses),
                      "unknown": sum(r.get("status") == "unknown" for r in responses),
                      "final_balance": exchange(args.host, args.port, {"action": "balance", "account": args.account})}
        else:
            result = exchange(args.host, args.port, make_payload(args.command, args.account,
                              getattr(args, "amount", None), getattr(args, "demo_fail", False),
                              getattr(args, "demo_pause_ms", 0)))
        print(json.dumps(result, indent=2, ensure_ascii=False))
        if result.get("status") == "error" or result.get("errors", 0) or result.get("unknown", 0):
            return 1
        return 0
    except (OSError, ValueError) as exc:
        print(f"Connection failed: {exc}. A write may have committed if its reply was lost; "
              "no automatic retry was performed.", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
