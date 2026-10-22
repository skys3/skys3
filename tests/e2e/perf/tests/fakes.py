"""A fake node for the harness's tests: an HTTP server answering S3 PUTs
and GETs, /metrics, and the admin API, in a thread."""

from __future__ import annotations

import http.server
import json
import threading


class FakeNode:
    """Answers PUT with 200 and counts it, GET with `body`, /metrics with
    `metrics` (a callable giving the text), and /v1/* with `admin[path]`."""

    def __init__(self) -> None:
        self.puts = 0
        self.put_bytes = 0
        self.body = b"hello"
        self.status = 200
        self.metrics = lambda: "# EOF\n"
        self.admin: dict[str, object] = {}
        self.lock = threading.Lock()
        node = self

        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *_args) -> None:
                pass

            def _answer(self, status: int, body: bytes, kind: str = "text/plain") -> None:
                self.send_response(status)
                self.send_header("content-type", kind)
                self.send_header("content-length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_PUT(self) -> None:
                length = int(self.headers.get("content-length", 0))
                data = self.rfile.read(length)
                with node.lock:
                    node.puts += 1
                    node.put_bytes += len(data)
                self._answer(node.status, b"")

            def do_GET(self) -> None:
                if self.path == "/metrics":
                    self._answer(200, node.metrics().encode())
                elif self.path.startswith("/v1/"):
                    answer = node.admin.get(self.path)
                    if answer is None:
                        self._answer(404, b"{}")
                    else:
                        self._answer(200, json.dumps(answer).encode(), "application/json")
                else:
                    self._answer(node.status, node.body)

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.address = f"127.0.0.1:{self.server.server_address[1]}"
        self.url = f"http://{self.address}"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def __enter__(self) -> FakeNode:
        self.thread.start()
        return self

    def __exit__(self, *_exc) -> None:
        self.server.shutdown()
        self.server.server_close()
