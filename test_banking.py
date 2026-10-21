"""Integration tests using real TCP sockets and abrupt process termination."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import json
from pathlib import Path
import re
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest

from client import exchange

HERE = Path(__file__).resolve().parent


class ServerProcess:
    def __init__(self, directory, max_clients=32):
        self.directory = Path(directory)
        self.max_clients = max_clients
        self.process = None
        self.start()

    def start(self):
        self.output_path = self.directory / "stdout.txt"
        self.output = self.output_path.open("w", encoding="utf-8")
        self.process = subprocess.Popen([
            sys.executable, "-u", str(HERE / "server.py"), "--host", "127.0.0.1", "--port", "0",
            "--max-clients", str(self.max_clients), "--demo-faults", "--client-timeout", "1",
            "--db", str(self.directory / "bank.sqlite3"), "--log", str(self.directory / "requests.jsonl"),
        ], stdout=self.output, stderr=subprocess.STDOUT)
        self.wait_output("READY")
        self.port = int(re.search(r"port=(\d+)", self.output_path.read_text(encoding="utf-8")).group(1))

    def wait_output(self, text, timeout=15):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            output = self.output_path.read_text(encoding="utf-8")
            if text in output:
                return
            if self.process.poll() is not None:
                raise RuntimeError(output)
            time.sleep(0.02)
        raise TimeoutError(f"server did not output {text!r}")

    def stop(self):
        if self.process and self.process.poll() is None:
            self.process.kill()
            self.process.wait(timeout=10)
        self.output.close()

    def call(self, action="balance", account="A1001", **kwargs):
        return exchange("127.0.0.1", self.port, {"action": action, "account": account, **kwargs})

    def sql(self, statement):
        with closing(sqlite3.connect(self.directory / "bank.sqlite3")) as connection:
            return connection.execute(statement).fetchall()


class BankingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="bank-test-")
        self.server = ServerProcess(self.temp.name)

    def tearDown(self):
        self.server.stop()
        self.temp.cleanup()

    def test_normal_operations_and_exact_cents(self):
        self.assertEqual(self.server.call()["balance"], "1000.00")
        response = exchange("127.0.0.1", self.server.port,
                            b'{"action":"deposit","account":"A1001","amount":0.10}\n')
        self.assertEqual(response["balance"], "1000.10")
        self.assertEqual(self.server.call("withdraw", amount=200)["balance"], "800.10")
        self.assertEqual(len(self.server.sql("SELECT * FROM transactions")), 2)

    def test_all_required_errors(self):
        cases = [({"action": "balance", "account": "Z9999"}, "unknown_account"),
                 ({"action": "withdraw", "account": "A1001", "amount": 5000}, "insufficient_funds"),
                 ({"action": "deposit", "account": "A1001", "amount": -50}, "invalid_amount"),
                 ({"action": "deposit", "account": "A1001", "amount": 0}, "invalid_amount"),
                 ({"action": "deposit", "account": "A1001", "amount": "abc"}, "invalid_amount"),
                 ({"action": "transfer", "account": "A1001"}, "unsupported_action"),
                 ({"action": "withdraw", "account": "A1001"}, "missing_field")]
        for payload, expected in cases:
            with self.subTest(expected=expected, payload=payload):
                self.assertEqual(exchange("127.0.0.1", self.server.port, payload)["code"], expected)
        self.assertEqual(self.server.call()["balance"], "1000.00")

    def test_malformed_inputs_do_not_stop_server(self):
        for payload in (b'{broken}\n', b'\xff\xfe\n', b'[]\n', b'null\n',
                        b'{"action":"balance","action":"withdraw"}\n',
                        b'{"action":"withdraw","account":"A1001","amount":NaN}\n'):
            with self.subTest(payload=payload):
                self.assertEqual(exchange("127.0.0.1", self.server.port, payload)["status"], "error")
        self.assertEqual(self.server.call()["status"], "success")

    def test_amount_validation_including_long_precision(self):
        for token in ("true", "0.001", "1e1000000", "1e-1000000", "0.01000000000000000000000000000000000000000000001"):
            payload = ('{"action":"deposit","account":"A1001","amount":' + token + '}\n').encode()
            with self.subTest(token=token):
                self.assertEqual(exchange("127.0.0.1", self.server.port, payload)["code"], "invalid_amount")
        self.assertEqual(self.server.call()["balance"], "1000.00")

    def test_concurrent_withdrawals_no_lost_updates(self):
        with ThreadPoolExecutor(max_workers=16) as pool:
            responses = list(pool.map(lambda _: self.server.call("withdraw", amount=1), range(200)))
        self.assertTrue(all(r["status"] == "success" for r in responses))
        self.assertEqual(self.server.call()["balance"], "800.00")
        self.assertEqual(len(self.server.sql("SELECT * FROM transactions")), 200)

    def test_concurrent_overdraft_prevention(self):
        with ThreadPoolExecutor(max_workers=16) as pool:
            responses = list(pool.map(lambda _: self.server.call("withdraw", amount=100), range(25)))
        self.assertEqual(sum(r["status"] == "success" for r in responses), 10)
        self.assertEqual(sum(r.get("code") == "insufficient_funds" for r in responses), 15)
        self.assertEqual(self.server.call()["balance"], "0.00")

    def test_exception_after_update_rolls_back(self):
        response = self.server.call("withdraw", amount=200, demo_fail=True)
        self.assertEqual(response["code"], "injected_failure")
        self.assertEqual(self.server.call()["balance"], "1000.00")
        self.assertEqual(self.server.sql("SELECT * FROM transactions"), [])

    def test_read_does_not_see_uncommitted_update(self):
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(self.server.call, "withdraw", amount=200, demo_pause_ms=500, demo_fail=True)
            self.server.wait_output("update uncommitted")
            self.assertEqual(self.server.call()["balance"], "1000.00")
            self.assertEqual(future.result()["code"], "injected_failure")

    def test_confirmed_commit_survives_kill_and_restart(self):
        self.assertEqual(self.server.call("deposit", amount=123)["balance"], "1123.00")
        self.server.stop()
        self.server.start()
        self.assertEqual(self.server.call()["balance"], "1123.00")
        self.assertEqual(len(self.server.sql("SELECT * FROM transactions")), 1)
        self.assertEqual(self.server.sql("PRAGMA integrity_check"), [("ok",)])

    def test_kill_during_uncommitted_write_rolls_back(self):
        connection = socket.create_connection(("127.0.0.1", self.server.port), timeout=5)
        try:
            connection.sendall(b'{"action":"withdraw","account":"A1001","amount":200,"demo_pause_ms":30000}\n')
            self.server.wait_output("update uncommitted")
            self.server.stop()
        finally:
            connection.close()
        self.server.start()
        self.assertEqual(self.server.call()["balance"], "1000.00")
        self.assertEqual(self.server.sql("SELECT * FROM transactions"), [])
        self.assertEqual(self.server.sql("PRAGMA integrity_check"), [("ok",)])

    def test_logging_required_fields(self):
        response = self.server.call("withdraw", amount=100)
        path = Path(self.temp.name) / "requests.jsonl"
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            lines = path.read_text(encoding="utf-8").splitlines()
            if lines:
                break
            time.sleep(0.01)
        record = json.loads(lines[0])
        for field in ("request_id", "client_ip", "action", "account", "timestamp", "result", "latency_ms", "request", "response"):
            self.assertIn(field, record)
        self.assertEqual(record["request_id"], response["request_id"])
        self.assertEqual(record["response"], response)
        self.assertEqual(record["request"]["amount"], 100)

    def test_maximum_clients_is_enforced(self):
        self.server.stop()
        self.server.max_clients = 2
        self.server.start()
        held = []
        try:
            for _ in range(2):
                connection = socket.create_connection(("127.0.0.1", self.server.port), timeout=5)
                connection.sendall(b'{')
                held.append(connection)
            time.sleep(0.1)
            self.assertEqual(self.server.call()["code"], "server_busy")
        finally:
            for connection in held:
                connection.close()
        time.sleep(0.1)
        self.assertEqual(self.server.call()["status"], "success")

    def test_oversized_and_unfinished_requests(self):
        self.assertEqual(exchange("127.0.0.1", self.server.port, b'x' * 16_385 + b'\n')["code"], "request_too_large")
        self.assertEqual(exchange("127.0.0.1", self.server.port, b'{')["code"], "request_timeout")
        self.assertEqual(self.server.call()["status"], "success")


if __name__ == "__main__":
    unittest.main(verbosity=2)
