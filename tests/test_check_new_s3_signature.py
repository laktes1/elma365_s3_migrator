"""The signature check sends and signs ELMA365's Host, not the endpoint address."""
from __future__ import annotations

import importlib.util
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

spec = importlib.util.spec_from_file_location("chk", Path(__file__).parent.parent / "scripts" / "check_new_s3_signature.py")
chk = importlib.util.module_from_spec(spec)
spec.loader.exec_module(chk)  # type: ignore[union-attr]


def test_host_and_region_are_elma365s(monkeypatch) -> None:
    seen: list = []
    store: dict = {}

    class H(BaseHTTPRequestHandler):
        def _reply(self, code: int, body: bytes = b"") -> None:
            self.send_response(code)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("ETag", '"e"')
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(body)

        def _seen(self) -> None:
            seen.append((self.command, self.headers["Host"], self.headers.get("Authorization", ""), self.path))

        def do_PUT(self) -> None:  # noqa: N802
            self._seen()
            store[self.path] = self.rfile.read(int(self.headers["Content-Length"]))
            self._reply(200)

        def do_HEAD(self) -> None:  # noqa: N802
            self._seen(); self._reply(200)

        def do_GET(self) -> None:  # noqa: N802
            self._seen(); self._reply(200, store[self.path.split("?")[0]])

        def do_DELETE(self) -> None:  # noqa: N802
            self._seen(); self._reply(204)

        def log_message(self, *a) -> None:  # noqa: ANN002
            pass

    srv = HTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    monkeypatch.setenv("ELMA_ACCESS_KEY", "AK")
    monkeypatch.setenv("ELMA_SECRET_KEY", "SK")
    monkeypatch.setenv("NO_PROXY", "127.0.0.1")
    monkeypatch.setenv("no_proxy", "127.0.0.1")
    try:
        rc = chk.main(["--endpoint", f"http://127.0.0.1:{srv.server_port}", "--host", "elma.example.com"])
    finally:
        srv.shutdown()
    assert rc == 0
    assert [m for m, *_ in seen] == ["PUT", "HEAD", "GET", "GET", "DELETE"]
    assert all(h == "elma.example.com" for _, h, _, _ in seen)
    signed = [a for _, _, a, _ in seen if a]
    assert signed and all("/ru-central-1/s3/aws4_request" in a and "host" in a for a in signed)
    presigned = seen[3][3]
    assert "X-Amz-SignedHeaders=host" in presigned and "ru-central-1" in presigned
