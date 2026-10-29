import json
import logging
import os
from pathlib import Path
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading

from archive import ArchiveStore, MAX_BODY_BYTES, s3_client, verify_signature

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def handler_type(store, secret):
    class Handler(BaseHTTPRequestHandler):
        def setup(self):
            super().setup()
            self.connection.settimeout(30)

        def reply(self, status, body):
            raw = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def do_GET(self):
            if self.path != "/ready":
                self.reply(404, {"error": "not found"})
                return
            try:
                store.ready()
                self.reply(200, {"status": "ready"})
            except Exception:
                self.reply(503, {"status": "storage unavailable"})

        def do_POST(self):
            if self.path != "/events":
                self.reply(404, {"error": "not found"})
                return
            try:
                size = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                size = 0
            if not 0 < size <= MAX_BODY_BYTES or self.headers.get("Transfer-Encoding"):
                self.reply(413, {"error": "invalid body size or transfer encoding"})
                return
            body = self.rfile.read(size)
            if len(body) != size or not verify_signature(body, self.headers, secret):
                self.reply(401, {"error": "invalid webhook signature"})
                return
            try:
                result = store.archive(body)
            except (ValueError, TypeError, KeyError):
                self.reply(400, {"error": "invalid CDC batch"})
                return
            except Exception as exc:
                # Log the exception type only; data, SQL credentials and S3 keys stay out of logs.
                logging.error("Archive not acknowledged: %s", type(exc).__name__)
                self.reply(503, {"error": "archive not verified; retry this batch"})
                return
            logging.info("Archived batch=%s records=%s", result["batch_id"], result["records"])
            self.reply(200, result)

        def log_message(self, *args):
            pass
    return Handler


class LimitedServer(ThreadingHTTPServer):
    daemon_threads = True
    slots = threading.BoundedSemaphore(4)

    def process_request(self, request, address):
        if not self.slots.acquire(blocking=False):
            self.shutdown_request(request)
            return
        try:
            super().process_request(request, address)
        except Exception:
            self.slots.release()
            raise

    def process_request_thread(self, request, address):
        try:
            super().process_request_thread(request, address)
        finally:
            self.slots.release()


if __name__ == "__main__":
    store = ArchiveStore(s3_client(), os.environ.get("S3_BUCKET", "cdc-raw"))
    secret = Path(os.environ["WEBHOOK_SECRET_FILE"]).read_text().strip()
    logging.info("Starting CDC archive receiver on port 8080")
    LimitedServer(("0.0.0.0", 8080), handler_type(store, secret)).serve_forever()
