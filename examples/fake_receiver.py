"""Tiny local webhook receiver for manual verification (not part of tests)."""
import json
from http.server import BaseHTTPRequestHandler, HTTPServer


class Handler(BaseHTTPRequestHandler):
    count = 0

    def do_POST(self):  # noqa: N802
        length = int(self.headers.get("Content-Length", "0"))
        body = json.loads(self.rfile.read(length).decode())
        Handler.count += 1
        # Fail the first two deliveries to let us observe retries.
        code = 500 if Handler.count <= 2 else 200
        print(f"[{Handler.count}] {self.path} -> {code}: {body}", flush=True)
        self.send_response(code)
        self.end_headers()
        self.wfile.write(b"ok" if code < 400 else b"retry")

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    HTTPServer(("127.0.0.1", 8787), Handler).serve_forever()
