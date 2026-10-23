"""S3 gateway: durable intent before every key-changing request (docs/gateway-design.md, section 3).

The gateway is a transparent proxy: the request line, headers (including Host
and Authorization) and body go to MinIO unchanged, so MinIO verifies the
client's SigV4 signature itself. Only hop-by-hop headers are dropped.
"""
from __future__ import annotations

import argparse
import asyncio
import concurrent.futures as cf
import logging
import os
import sqlite3
import time
import uuid
from xml.sax.saxutils import escape
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional, Sequence, Tuple

import aiohttp
from aiohttp import web
from multidict import CIMultiDict
from yarl import URL

from . import journal, s3parse

log = logging.getLogger("s3gw.gateway")

HOP_BY_HOP = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "expect",  # answered by this server; upstream gets the body directly
}
# Added to every request the gateway forwards, so MinIO's trace shows which changes came through it
# (s3gw/tracecheck.py). Client IPs cannot tell: MinIO reports X-Forwarded-For set by ingress.
REQUEST_ID_HEADER = "X-S3gw-Request-Id"
JOURNAL_BUSY_MS = 5000
MAX_SMALL_BODY = 4 * 1024 * 1024  # DeleteObjects / Complete request bodies, XML responses
_BUFFERED_RESPONSE_OPS = {s3parse.COPY, s3parse.MPU_COMPLETE, s3parse.DELETE_BATCH, s3parse.MPU_CREATE}


@dataclass
class GatewayConfig:
    journal_path: Path
    upstream: str  # e.g. http://127.0.0.1:9000
    bucket: str
    settle_seconds: float = 900.0
    journal_timeout: float = 2.0
    upstream_timeout: float = 3600.0
    allow_bucket_ops: Sequence[str] = field(default_factory=tuple)  # e.g. ("PUT:", "GET:location")
    # while this file exists every change gets 503 and reads go on: write freeze for the cutover window
    readonly_file: Optional[Path] = None
    # other buckets and bucket-less requests (Loki, Tempo, backups, admin API) always go here,
    # unjournaled and never frozen: they stay on MinIO when `upstream` moves to the new S3
    other_upstream: Optional[str] = None


_FAULT_POINT = os.environ.get("S3GW_FAULT_POINT", "")


def _fault(point: str) -> None:
    """Test hook: die like kill -9 at a named point (stand failure tests only)."""
    if _FAULT_POINT == point:
        os._exit(137)


def s3_error_xml(code: str, message: str) -> bytes:
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        f"<Error><Code>{escape(code)}</Code><Message>{escape(message)}</Message></Error>"
    ).encode()


async def _read_limited(request: web.Request, limit: int) -> bytes:
    """Up to `limit` bytes of the body. content.read(n) returns only what is already buffered."""
    buf = bytearray()
    while len(buf) < limit:
        chunk = await request.content.read(limit - len(buf))
        if not chunk:
            break
        buf += chunk
    return bytes(buf)


class Journal:
    """All SQLite access on one thread: one connection, writes are serialized."""

    def __init__(self, path: Path, settle_seconds: float) -> None:
        self._pool = cf.ThreadPoolExecutor(max_workers=1, thread_name_prefix="journal")
        self._conn: Optional[sqlite3.Connection] = None
        self.epoch = self._pool.submit(self._open, path, settle_seconds).result()

    def _open(self, path: Path, settle_seconds: float) -> int:
        self._conn = journal.connect(path, synchronous="FULL", timeout=JOURNAL_BUSY_MS / 1000)
        return journal.start_epoch(self._conn, settle_seconds)

    async def call(self, fn, *args, timeout: Optional[float] = None):  # type: ignore[no-untyped-def]
        loop = asyncio.get_running_loop()
        fut = loop.run_in_executor(self._pool, lambda: fn(self._conn, *args))
        return await asyncio.wait_for(fut, timeout) if timeout else await fut

    async def write_intents(self, request_id: str, op: str, keys: Sequence[str], timeout: float) -> List[int]:
        """Durable intents or an exception within `timeout`.

        A write that starts after its deadline is skipped, and SQLite waits on a
        lock only for the time left. If the commit still lands after the caller
        gave up, the intents are closed as 'error' (the request was never
        forwarded), so the keys do not stay in flight until a restart.
        """
        loop = asyncio.get_running_loop()
        deadline = time.monotonic() + timeout

        def _write(conn: sqlite3.Connection) -> List[int]:
            left = deadline - time.monotonic()
            if left <= 0:
                raise TimeoutError("intent deadline passed while queued")
            conn.execute(f"PRAGMA busy_timeout = {max(int(left * 1000), 1)}")
            try:
                return journal.write_intents(conn, self.epoch, request_id, op, keys)
            finally:
                conn.execute(f"PRAGMA busy_timeout = {JOURNAL_BUSY_MS}")

        fut = loop.run_in_executor(self._pool, lambda: _write(self._conn))
        try:
            return await asyncio.wait_for(asyncio.shield(fut), timeout)
        except asyncio.TimeoutError:
            def _late(f: "asyncio.Future[List[int]]") -> None:
                if f.cancelled() or f.exception() is not None:
                    return
                rows = [(i, k, journal.ERROR, None, "NotForwarded") for i, k in zip(f.result(), keys)]
                self._pool.submit(lambda: journal.write_results(self._conn, rows))

            fut.add_done_callback(_late)
            raise

    def close(self) -> None:
        def _close() -> None:
            if self._conn is not None:
                self._conn.close()

        self._pool.submit(_close).result()
        self._pool.shutdown()


class Gateway:
    def __init__(self, cfg: GatewayConfig) -> None:
        self.cfg = cfg
        self.journal = Journal(cfg.journal_path, cfg.settle_seconds)
        self.session: Optional[aiohttp.ClientSession] = None

    async def start(self, app: web.Application) -> None:
        self.session = aiohttp.ClientSession(
            # reads go through the gateway too: no pool limit (aiohttp default 100 queues everything else)
            connector=aiohttp.TCPConnector(limit=0),
            auto_decompress=False,
            timeout=aiohttp.ClientTimeout(total=None, sock_connect=10, sock_read=self.cfg.upstream_timeout),
            skip_auto_headers=("User-Agent", "Accept-Encoding", "Accept", "Content-Type"),
        )

    async def stop(self, app: web.Application) -> None:
        if self.session:
            await self.session.close()
        self.journal.close()

    # -- request handling ---------------------------------------------------

    async def handle(self, request: web.Request) -> web.StreamResponse:
        try:
            s3req = s3parse.classify(request.method, request.raw_path, request.headers)
        except s3parse.BadRequest as exc:
            return web.Response(status=exc.status, body=s3_error_xml(exc.code, str(exc)), content_type="application/xml")

        request_id = uuid.uuid4().hex
        if s3req.bucket != self.cfg.bucket:
            return await self._forward(request, s3req, request_id, intents=[], upstream=self.cfg.other_upstream)
        if s3req.op == s3parse.READ:
            return await self._forward(request, s3req, request_id, intents=[])

        if self.cfg.readonly_file is not None and self.cfg.readonly_file.exists():
            return web.Response(
                status=503,
                body=s3_error_xml("SlowDown", "storage is read-only for maintenance"),
                content_type="application/xml",
            )

        if s3req.op == s3parse.BUCKET_OP:
            sig = f"{request.method.upper()}:{','.join(sorted(s3req.params))}"
            if sig not in self.cfg.allow_bucket_ops:
                log.warning("rejected bucket-level operation %s", sig)
                await self._safe(journal.record_error, "gateway", "bucket_op_rejected", None, sig)
                return web.Response(
                    status=501,
                    body=s3_error_xml("NotImplemented", "bucket-level operation is disabled during migration"),
                    content_type="application/xml",
                )
            return await self._forward(request, s3req, request_id, intents=[])

        body: Optional[bytes] = None
        keys: List[str] = []
        if s3req.op == s3parse.DELETE_BATCH:
            body = await _read_limited(request, MAX_SMALL_BODY + 1)
            if len(body) > MAX_SMALL_BODY:
                return web.Response(status=400, body=s3_error_xml("MalformedXML", "body too large"), content_type="application/xml")
            try:
                keys = s3parse.parse_delete_objects_request(body)
            except s3parse.BadRequest as exc:
                return web.Response(status=exc.status, body=s3_error_xml(exc.code, str(exc)), content_type="application/xml")
        elif s3req.op in s3parse.KEY_CHANGING:
            keys = [s3req.key]

        intents: List[Tuple[int, str]] = []
        if keys:
            try:
                ids = await self.journal.write_intents(request_id, s3req.op, keys, self.cfg.journal_timeout)
            except Exception as exc:  # journal unavailable: do not forward (section 3)
                log.error("intent write failed, rejecting request: %s", exc)
                return web.Response(
                    status=503,
                    body=s3_error_xml("SlowDown", "change journal unavailable"),
                    content_type="application/xml",
                )
            intents = list(zip(ids, keys))
            _fault("after_intent")
        return await self._forward(request, s3req, request_id, intents=intents, body=body)

    async def _safe(self, fn, *args) -> None:  # type: ignore[no-untyped-def]
        try:
            await self.journal.call(fn, *args)
        except Exception as exc:
            log.error("journal write failed: %s", exc)

    def _upstream_headers(self, request: web.Request, request_id: str) -> "CIMultiDict[str]":
        headers: "CIMultiDict[str]" = CIMultiDict()  # keeps repeated headers
        for name, value in request.headers.items():
            if name.lower() in HOP_BY_HOP or name.lower() == REQUEST_ID_HEADER.lower():
                continue  # a client cannot pretend its request came through the gateway
            headers.add(name, value)
        headers[REQUEST_ID_HEADER] = request_id
        return headers

    async def _forward(
        self,
        request: web.Request,
        s3req: s3parse.S3Request,
        request_id: str,
        intents: List[Tuple[int, str]],
        body: Optional[bytes] = None,
        upstream: Optional[str] = None,
    ) -> web.StreamResponse:
        assert self.session is not None
        url = URL((upstream or self.cfg.upstream).rstrip("/") + request.raw_path, encoded=True)
        headers = self._upstream_headers(request, request_id)
        data = body
        if data is None and request.body_exists:
            data = request.content.iter_chunked(256 * 1024)

        buffered = s3req.op in _BUFFERED_RESPONSE_OPS
        try:
            upstream = await self.session.request(
                request.method, url, headers=headers, data=data, allow_redirects=False,
            )
        except (aiohttp.ClientConnectorError, ConnectionRefusedError) as exc:
            # Nothing reached MinIO: the operation did not happen.
            await self._close_intents(intents, journal.ERROR, None, "ConnectError")
            log.error("upstream connect failed: %s", exc)
            return web.Response(status=502, body=s3_error_xml("BadGateway", "storage unavailable"), content_type="application/xml")
        except Exception as exc:
            await self._close_intents(intents, journal.UNCERTAIN, None, type(exc).__name__)
            log.error("upstream request failed: %s", exc)
            return web.Response(status=504, body=s3_error_xml("GatewayTimeout", "storage did not answer"), content_type="application/xml")

        async with upstream:
            if intents:
                _fault("after_upstream")
            resp_headers = [
                (k.decode("latin-1"), v.decode("latin-1"))
                for k, v in upstream.raw_headers
                if k.decode("latin-1").lower() not in HOP_BY_HOP
            ]
            if buffered:
                try:
                    payload = await upstream.read()
                except Exception as exc:
                    await self._close_intents(intents, journal.UNCERTAIN, upstream.status, type(exc).__name__)
                    return web.Response(status=504, body=s3_error_xml("GatewayTimeout", "storage answer cut off"), content_type="application/xml")
                await self._record_buffered(s3req, intents, upstream.status, payload)
                resp = web.Response(status=upstream.status, body=payload)
                self._copy_headers(resp, resp_headers, drop_length=True)
                return resp

            await self._close_intents(
                intents, journal.SUCCESS if 200 <= upstream.status < 300 else journal.ERROR, upstream.status, None
            )
            if s3req.op == s3parse.MPU_ABORT and 200 <= upstream.status < 300 and s3req.upload_id:
                await self._safe(journal.mpu_close, s3req.upload_id, "aborted")
            resp = web.StreamResponse(status=upstream.status)
            self._copy_headers(resp, resp_headers, drop_length=False)
            if request.method.upper() == "HEAD":
                await resp.prepare(request)
                await resp.write_eof()
                return resp
            await resp.prepare(request)
            async for chunk in upstream.content.iter_chunked(256 * 1024):
                await resp.write(chunk)
            await resp.write_eof()
            return resp

    @staticmethod
    def _copy_headers(resp: web.StreamResponse, headers: List[Tuple[str, str]], drop_length: bool) -> None:
        for k, v in headers:
            if drop_length and k.lower() == "content-length":
                continue
            resp.headers.add(k, v)  # add, not set: MinIO sends some headers twice (Vary)

    async def _record_buffered(
        self, s3req: s3parse.S3Request, intents: List[Tuple[int, str]], status: int, payload: bytes
    ) -> None:
        if s3req.op == s3parse.MPU_CREATE:
            upload_id = s3parse.parse_upload_id(payload) if 200 <= status < 300 else None
            if upload_id:
                await self._safe(journal.mpu_open, self.journal.epoch, upload_id, s3req.key)
            return
        if s3req.op == s3parse.DELETE_BATCH:
            parsed = s3parse.parse_delete_objects_response(payload) if 200 <= status < 300 else None
            results = []
            for intent_id, key in intents:
                if not (200 <= status < 300):
                    results.append((intent_id, key, journal.ERROR, status, None))
                elif parsed is None:
                    results.append((intent_id, key, journal.UNCERTAIN, status, "UnparsedResponse"))
                elif key in parsed.errors:
                    results.append((intent_id, key, journal.ERROR, status, parsed.errors[key]))
                elif key in parsed.deleted:
                    results.append((intent_id, key, journal.SUCCESS, status, None))
                else:
                    # Quiet mode lists only errors: absent from errors means deleted.
                    results.append((intent_id, key, journal.SUCCESS, status, None))
            await self._safe(journal.write_results, results)
            return
        # CopyObject / CompleteMultipartUpload may return 200 with an <Error> body.
        code = s3parse.response_error_code(payload)
        ok = 200 <= status < 300 and code is None
        await self._close_intents(intents, journal.SUCCESS if ok else journal.ERROR, status, code)
        if s3req.op == s3parse.MPU_COMPLETE and s3req.upload_id and ok:
            await self._safe(journal.mpu_close, s3req.upload_id, "completed")

    async def _close_intents(
        self, intents: List[Tuple[int, str]], result: str, status: Optional[int], code: Optional[str]
    ) -> None:
        if intents:
            await self._safe(journal.write_results, [(i, k, result, status, code) for i, k in intents])


def build_app(cfg: GatewayConfig) -> web.Application:
    gw = Gateway(cfg)
    app = web.Application(client_max_size=0)
    app.on_startup.append(gw.start)
    app.on_cleanup.append(gw.stop)
    app.router.add_route("*", "/{tail:.*}", gw.handle)
    return app


def main(argv: Optional[list] = None) -> int:
    p = argparse.ArgumentParser(description="S3 gateway with a durable intent journal")
    p.add_argument("--listen", default=os.environ.get("GW_LISTEN", "127.0.0.1:9100"))
    p.add_argument("--upstream", default=os.environ.get("GW_UPSTREAM", "http://127.0.0.1:9000"))
    # not --upstream by default: an env file without this line must not send other buckets to the new S3
    p.add_argument("--other-upstream", default=os.environ.get("GW_OTHER_UPSTREAM", "http://127.0.0.1:9000"),
                   help="where requests to other buckets go (MinIO)")
    p.add_argument("--bucket", default=os.environ.get("GW_BUCKET", "s3elma365"))
    p.add_argument("--journal", default=os.environ.get("JOURNAL_DB", "journal.db"))
    p.add_argument("--settle-seconds", type=float, default=float(os.environ.get("GW_SETTLE_SECONDS", "900")))
    p.add_argument("--allow-bucket-ops", default=os.environ.get("GW_ALLOW_BUCKET_OPS", ""))
    p.add_argument("--readonly-file", default=os.environ.get("GW_READONLY_FILE", ""),
                   help="while this file exists, changes get 503 (cutover window)")
    p.add_argument("--journal-timeout", type=float, default=float(os.environ.get("GW_JOURNAL_TIMEOUT", "2")),
                   help="seconds to wait for a durable intent before answering 503")
    p.add_argument("--upstream-timeout", type=float, default=float(os.environ.get("GW_UPSTREAM_TIMEOUT", "3600")),
                   help="socket read timeout towards MinIO, seconds")
    a = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    host, _, port = a.listen.rpartition(":")
    if not host or not port.isdigit():
        p.error(f"--listen must be HOST:PORT, got {a.listen!r}")
    cfg = GatewayConfig(
        journal_path=Path(a.journal),
        upstream=a.upstream,
        bucket=a.bucket,
        settle_seconds=a.settle_seconds,
        journal_timeout=a.journal_timeout,
        upstream_timeout=a.upstream_timeout,
        allow_bucket_ops=tuple(x.strip() for x in a.allow_bucket_ops.split(";") if x.strip()),
        readonly_file=Path(a.readonly_file) if a.readonly_file else None,
        other_upstream=a.other_upstream or None,
    )
    web.run_app(build_app(cfg), host=host, port=int(port), access_log=None)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
