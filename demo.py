#!/usr/bin/env python3
"""Run the live demonstration on the CLIENT VM against the SERVER VM."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
import ipaddress
import json
import socket
import sys
from client import exchange


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", required=True, help="real IP of the server VM")
    parser.add_argument("--port", type=int, default=5050)
    args = parser.parse_args()
    addresses = socket.getaddrinfo(args.host, args.port, type=socket.SOCK_STREAM)
    if any(ipaddress.ip_address(address[4][0]).is_loopback for address in addresses):
        parser.error("this demonstration requires a separate server VM; use its real network IP")
    def call(payload, expected="success", code=None):
        response = exchange(args.host, args.port, payload)
        print(json.dumps({"request": payload.decode(errors="replace").strip() if isinstance(payload, bytes) else payload,
                          "response": response}), flush=True)
        if response.get("status") != expected or (code and response.get("code") != code):
            raise RuntimeError(f"unexpected response: {response}")
        return response
    def balance():
        return Decimal(call({"action": "balance", "account": "A1001"})["balance"])
    baseline = balance()
    call({"action": "deposit", "account": "A1001", "amount": 200})
    call({"action": "withdraw", "account": "A1001", "amount": 100})
    invalid = [({"action": "balance", "account": "Z9999"}, "unknown_account"),
               ({"action": "withdraw", "account": "A1001", "amount": 90000000000000}, "insufficient_funds"),
               ({"action": "withdraw", "account": "A1001", "amount": -50}, "invalid_amount"),
               ({"action": "withdraw", "account": "A1001", "amount": 0}, "invalid_amount"),
               ({"action": "withdraw", "account": "A1001", "amount": "abc"}, "invalid_amount"),
               ({"action": "transfer", "account": "A1001"}, "unsupported_action"),
               ({"action": "withdraw", "account": "A1001"}, "missing_field"),
               (b'{broken JSON}\n', "malformed_request")]
    for payload, code in invalid:
        call(payload, "error", code)
    before = balance()
    call({"action": "withdraw", "account": "A1001", "amount": 1, "demo_fail": True}, "error", "injected_failure")
    if balance() != before:
        raise RuntimeError("rollback verification failed")
    print("CONCURRENCY: 100 withdrawals of 1.00, 16 concurrent clients", flush=True)
    with ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(lambda _: call({"action": "withdraw", "account": "A1001", "amount": 1}), range(100)))
    final = balance()
    if final != baseline:
        raise RuntimeError(f"balance mismatch: expected {baseline}, received {final}")
    print("PASS: normal operations, all required errors, rollback, 100 concurrent withdrawals. "
          "Final balance equals the starting balance. Crash/restart must also be demonstrated.", flush=True)


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, RuntimeError) as exc:
        print(f"DEMO FAILED: {exc}", file=sys.stderr)
        sys.exit(1)
