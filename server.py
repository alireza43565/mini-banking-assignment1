#!/usr/bin/env python3
"""Simulated banking server. Protocol: one UTF-8 JSON line per TCP connection."""
import argparse
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import json
import logging
from pathlib import Path
import socket
import socketserver
import sqlite3
import threading
import time
import uuid

MAX_REQUEST = 16_384
MAX_CENTS = 9_000_000_000_000_000
INITIAL_ACCOUNTS = {"A1001": 100_000, "A1002": 200_000, "A1003": 50_000}


class BankError(Exception):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code


def money(cents):
    return f"{cents // 100}.{cents % 100:02d}"


def amount_in_cents(value):
    # bool is a subclass of int, but is not a valid monetary amount.
    if isinstance(value, bool) or not isinstance(value, (int, Decimal)):
        raise BankError("invalid_amount", "amount must be a positive JSON number")
    try:
        number = Decimal(value)
        if not number.is_finite() or number <= 0 or number > Decimal(MAX_CENTS) / 100:
            raise ValueError
        # Inspect the decimal digits directly: the default Decimal precision can
        # otherwise round a long fractional input into an apparently valid cent.
        _, digits, exponent = number.as_tuple()
        shift = exponent + 2
        if shift < 0:
            places = -shift
            if places >= len(digits) or any(digits[-places:]):
                raise ValueError
            digits, shift = digits[:-places], 0
        return int("".join(map(str, digits))) * (10 ** shift)
    except (InvalidOperation, ValueError, OverflowError):
        raise BankError("invalid_amount", "amount must be positive with at most two decimal places") from None


class Bank:
    def __init__(self, path, demo_faults=False):
        self.path = str(path)
        self.demo_faults = demo_faults
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        connection = self.connect()
        try:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute(f"""CREATE TABLE IF NOT EXISTS accounts (
                account TEXT PRIMARY KEY,
                balance_cents INTEGER NOT NULL CHECK (
                    typeof(balance_cents)='integer' AND balance_cents BETWEEN 0 AND {MAX_CENTS})
            )""")
            connection.execute("""CREATE TABLE IF NOT EXISTS transactions (
                request_id TEXT PRIMARY KEY, timestamp TEXT NOT NULL,
                account TEXT NOT NULL REFERENCES accounts(account), action TEXT NOT NULL,
                amount_cents INTEGER NOT NULL, before_cents INTEGER NOT NULL,
                after_cents INTEGER NOT NULL
            )""")
            connection.execute("BEGIN IMMEDIATE")
            connection.executemany("INSERT OR IGNORE INTO accounts VALUES (?, ?)", INITIAL_ACCOUNTS.items())
            connection.commit()
        finally:
            connection.close()

    def connect(self):
        connection = sqlite3.connect(self.path, timeout=35, isolation_level=None)
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("PRAGMA foreign_keys=ON")
        return connection

    def execute(self, request, request_id):
        if not isinstance(request, dict):
            raise BankError("invalid_request", "request must be a JSON object")
        for field in ("action", "account"):
            if field not in request:
                raise BankError("missing_field", f"missing field: {field}")
        action, account = request["action"], request["account"]
        if not isinstance(action, str) or action not in ("balance", "deposit", "withdraw"):
            raise BankError("unsupported_action", "supported actions: balance, deposit, withdraw")
        if not isinstance(account, str) or not account or len(account) > 64:
            raise BankError("invalid_account", "account must be a nonempty string of at most 64 characters")
        cents = None
        if action != "balance":
            if "amount" not in request:
                raise BankError("missing_field", "missing field: amount")
            cents = amount_in_cents(request["amount"])
        fail = request.get("demo_fail", False)
        pause = request.get("demo_pause_ms", 0)
        if type(fail) is not bool or type(pause) is not int or not 0 <= pause <= 30_000:
            raise BankError("invalid_demo_option", "demo_fail must be boolean; demo_pause_ms must be 0..30000")
        if (fail or pause) and (not self.demo_faults or action == "balance"):
            raise BankError("demo_disabled", "fault injection requires a write and --demo-faults on the server")
        connection = self.connect()
        try:
            if action != "balance":
                # Acquire the SQLite writer lock BEFORE reading the balance.
                connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT balance_cents FROM accounts WHERE account=?", (account,)).fetchone()
            if row is None:
                raise BankError("unknown_account", "account does not exist")
            before = after = row[0]
            if action == "withdraw":
                if before < cents:
                    raise BankError("insufficient_funds", "account has insufficient funds")
                after = before - cents
            elif action == "deposit":
                after = before + cents
                if after > MAX_CENTS:
                    raise BankError("balance_limit", "deposit exceeds the maximum account balance")
            if action != "balance":
                connection.execute("UPDATE accounts SET balance_cents=? WHERE account=?", (after, account))
                if pause:
                    print(f"DEMO: request={request_id} update uncommitted; pausing {pause}ms", flush=True)
                    time.sleep(pause / 1000)
                if fail:
                    raise BankError("injected_failure", "demo exception after UPDATE; transaction rolled back")
                connection.execute("INSERT INTO transactions VALUES (?, ?, ?, ?, ?, ?, ?)", (
                    request_id, datetime.now(timezone.utc).isoformat(), account, action, cents, before, after))
                # FULL synchronous commit completes before a success response can be sent.
                connection.commit()
            return {"status": "success", "account": account, "balance": money(after)}
        finally:
            if connection.in_transaction:
                connection.rollback()
            connection.close()


def error_response(code, message):
    return {"status": "error", "code": code, "message": message}


def reject_constant(_):
    raise ValueError("non-finite JSON number")


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


class Handler(socketserver.StreamRequestHandler):
    def handle(self):
        started = time.perf_counter()
        request_id = uuid.uuid4().hex
        raw = b""
        request = None
        delivery = "sent"
        self.connection.settimeout(self.server.client_timeout)
        try:
            raw = self.rfile.readline(MAX_REQUEST + 1)
            if len(raw) > MAX_REQUEST:
                raise BankError("request_too_large", f"maximum request size is {MAX_REQUEST} bytes")
            if not raw.endswith(b"\n"):
                raise BankError("malformed_request", "send one UTF-8 JSON object followed by a newline")
            try:
                request = json.loads(raw.decode("utf-8"), parse_float=Decimal,
                                     parse_constant=reject_constant, object_pairs_hook=unique_object)
            except (ValueError, UnicodeError, RecursionError):
                raise BankError("malformed_request", "invalid UTF-8 JSON") from None
            response = self.server.bank.execute(request, request_id)
        except BankError as exc:
            response = error_response(exc.code, str(exc))
        except (TimeoutError, socket.timeout):
            response = error_response("request_timeout", "client did not finish the request in time")
        except sqlite3.OperationalError as exc:
            code = "database_busy" if "locked" in str(exc).lower() else "storage_error"
            response = error_response(code, "transaction could not complete; no success was confirmed")
        except Exception:
            logging.getLogger("bank.internal").exception("request failed")
            response = error_response("internal_error", "request failed")
        response["request_id"] = request_id
        try:
            self.wfile.write((json.dumps(response) + "\n").encode("utf-8"))
            self.wfile.flush()
        except OSError:
            delivery = "client_disconnected"
        self.server.audit(request_id, self.client_address[0], request, response, started, raw, delivery)


class BankingServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True
    request_queue_size = 128

    def __init__(self, address, bank, max_clients, log_path, client_timeout=5):
        self.bank = bank
        self.slots = threading.BoundedSemaphore(max_clients)
        self.client_timeout = client_timeout
        self.logger = logging.getLogger(f"bank.audit.{id(self)}")
        self.logger.setLevel(logging.INFO)
        self.logger.propagate = False
        Path(log_path).parent.mkdir(parents=True, exist_ok=True)
        handler = logging.FileHandler(log_path, encoding="utf-8")
        handler.setFormatter(logging.Formatter("%(message)s"))
        self.logger.addHandler(handler)
        console = logging.StreamHandler()
        console.setFormatter(logging.Formatter("%(message)s"))
        self.logger.addHandler(console)
        super().__init__(address, Handler)

    def audit(self, request_id, client_ip, request, response, started, raw=b"", delivery="sent"):
        fields = request if isinstance(request, dict) else {}
        record = {"timestamp": datetime.now(timezone.utc).isoformat(), "request_id": request_id,
                  "client_ip": client_ip, "action": fields.get("action"), "account": fields.get("account"),
                  "result": response["status"], "latency_ms": round((time.perf_counter() - started) * 1000, 3),
                  "request": request, "response": response, "delivery": delivery}
        if request is None:
            record["raw_request"] = raw.decode("utf-8", errors="replace")
        self.logger.info(json.dumps(record, default=str, ensure_ascii=True))

    def process_request(self, request, client_address):
        if not self.slots.acquire(blocking=False):
            started = time.perf_counter()
            request_id = uuid.uuid4().hex
            response = error_response("server_busy", "maximum simultaneous clients reached")
            response["request_id"] = request_id
            delivery = "sent"
            raw = b""
            rejected_request = None
            try:
                request.settimeout(0.25)
                # Read the bounded request before closing: unread incoming bytes
                # can turn a TCP close into a reset that discards our busy reply.
                with request.makefile("rb") as stream:
                    raw = stream.readline(MAX_REQUEST + 1)
                if len(raw) <= MAX_REQUEST and raw.endswith(b"\n"):
                    try:
                        rejected_request = json.loads(raw.decode("utf-8"), parse_float=Decimal,
                                                      parse_constant=reject_constant, object_pairs_hook=unique_object)
                    except (ValueError, UnicodeError, RecursionError):
                        pass
            except OSError:
                pass
            try:
                request.sendall((json.dumps(response) + "\n").encode("utf-8"))
            except OSError:
                delivery = "client_disconnected"
            finally:
                self.audit(request_id, client_address[0], rejected_request, response, started, raw, delivery)
                self.shutdown_request(request)
            return
        try:
            super().process_request(request, client_address)
        except BaseException:
            self.slots.release()
            raise

    def process_request_thread(self, request, client_address):
        try:
            super().process_request_thread(request, client_address)
        finally:
            self.slots.release()

    def server_close(self):
        super().server_close()
        for handler in self.logger.handlers[:]:
            handler.close()
            self.logger.removeHandler(handler)


def main():
    import os
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default=os.getenv("BANK_HOST", "0.0.0.0"))
    parser.add_argument("--port", type=int, default=int(os.getenv("BANK_PORT", "5050")))
    parser.add_argument("--max-clients", type=int, default=int(os.getenv("BANK_MAX_CLIENTS", "32")))
    parser.add_argument("--db", default="data/bank.sqlite3")
    parser.add_argument("--log", default="logs/requests.jsonl")
    parser.add_argument("--client-timeout", type=float, default=5)
    parser.add_argument("--demo-faults", action="store_true", help="enable explicit rollback/crash demonstrations")
    args = parser.parse_args()
    if args.max_clients < 1 or args.client_timeout <= 0 or not 0 <= args.port <= 65535:
        parser.error("max-clients and timeout must be positive; port must be 0..65535")
    bank = Bank(args.db, demo_faults=args.demo_faults)
    with BankingServer((args.host, args.port), bank, args.max_clients, args.log, args.client_timeout) as server:
        print(f"READY host={args.host} port={server.server_address[1]} max_clients={args.max_clients} "
              f"db={Path(args.db).resolve()} demo_faults={args.demo_faults}", flush=True)
        try:
            server.serve_forever(poll_interval=0.2)
        except KeyboardInterrupt:
            print("Server stopped.", flush=True)


if __name__ == "__main__":
    main()
