"""Gateway proxying without MinIO: a fake upstream records what arrives."""
from __future__ import annotations

import asyncio
from pathlib import Path

import aiohttp
from aiohttp import web

from s3gw import gateway


async def _serve(app: web.Application) -> tuple:
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]  # type: ignore[union-attr]
    return runner, f"http://127.0.0.1:{port}"


def test_reads_are_not_limited_by_pool_and_carry_marker(tmp_path: Path) -> None:
    # 150 slow reads at once: with aiohttp's default pool (100) the rest would queue behind them.
    n = 150
    seen: list = []
    release = asyncio.Event()

    async def upstream(request: web.Request) -> web.Response:
        seen.append(request.headers.get(gateway.REQUEST_ID_HEADER))
        if len(seen) == n:
            release.set()
        await asyncio.wait_for(release.wait(), 10)
        return web.Response(body=b"x")

    async def run() -> None:
        up = web.Application()
        up.router.add_route("*", "/{tail:.*}", upstream)
        up_runner, up_url = await _serve(up)
        cfg = gateway.GatewayConfig(journal_path=tmp_path / "journal.db", upstream=up_url, bucket="s3elma365")
        gw_runner, gw_url = await _serve(gateway.build_app(cfg))
        try:
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=0)) as s:
                async def get(i: int) -> int:
                    headers = {gateway.REQUEST_ID_HEADER: "spoofed"} if i == 0 else {}
                    async with s.get(f"{gw_url}/s3elma365/k{i}", headers=headers) as r:
                        await r.read()
                        return r.status
                statuses = await asyncio.wait_for(asyncio.gather(*(get(i) for i in range(n))), 20)
        finally:
            await gw_runner.cleanup()
            await up_runner.cleanup()
        assert statuses == [200] * n
        assert "spoofed" not in seen and None not in seen  # client's own value is replaced
        assert len(set(seen)) == n  # one id per request

    asyncio.run(run())


def test_readonly_file_freezes_changes_but_not_reads(tmp_path: Path) -> None:
    calls: list = []

    async def upstream(request: web.Request) -> web.Response:
        calls.append(request.method)
        return web.Response(body=b"")

    async def run() -> None:
        up = web.Application()
        up.router.add_route("*", "/{tail:.*}", upstream)
        up_runner, up_url = await _serve(up)
        flag = tmp_path / "read-only"
        cfg = gateway.GatewayConfig(journal_path=tmp_path / "journal.db", upstream=up_url, bucket="s3elma365",
                                    readonly_file=flag)
        gw_runner, gw_url = await _serve(gateway.build_app(cfg))
        try:
            async with aiohttp.ClientSession() as s:
                async def status(method: str) -> int:
                    async with s.request(method, f"{gw_url}/s3elma365/k", data=b"x" if method == "PUT" else None) as r:
                        await r.read()
                        return r.status
                flag.touch()
                frozen = [await status("PUT"), await status("DELETE"), await status("GET")]
                flag.unlink()
                thawed = await status("PUT")
        finally:
            await gw_runner.cleanup()
            await up_runner.cleanup()
        assert frozen == [503, 503, 200] and thawed == 200
        assert calls == ["GET", "PUT"]

    asyncio.run(run())


def test_other_buckets_go_to_minio_unjournaled_and_unfrozen(tmp_path: Path) -> None:
    # After the cutover `upstream` is the new S3; Loki, Tempo and backups must stay on MinIO.
    calls: list = []

    def make(name: str):
        async def upstream(request: web.Request) -> web.Response:
            calls.append((name, request.method, request.path))
            return web.Response(body=b"")
        return upstream

    async def run() -> None:
        runners, urls = [], []
        for name in ("new", "minio"):
            up = web.Application()
            up.router.add_route("*", "/{tail:.*}", make(name))
            r, u = await _serve(up)
            runners.append(r)
            urls.append(u)
        flag = tmp_path / "read-only"
        flag.touch()
        cfg = gateway.GatewayConfig(journal_path=tmp_path / "journal.db", upstream=urls[0], bucket="s3elma365",
                                    readonly_file=flag, other_upstream=urls[1])
        gw_runner, gw_url = await _serve(gateway.build_app(cfg))
        try:
            async with aiohttp.ClientSession() as s:
                for method, path in (("PUT", "/elma365-loki/chunk"), ("DELETE", "/elma365-tempo/x"),
                                     ("GET", "/"), ("GET", "/minio/health/live"), ("GET", "/s3elma365/k")):
                    async with s.request(method, gw_url + path, data=b"x" if method == "PUT" else None) as r:
                        await r.read()
                        assert r.status == 200, (method, path)
        finally:
            await gw_runner.cleanup()
            for r in runners:
                await r.cleanup()
        assert calls == [("minio", "PUT", "/elma365-loki/chunk"), ("minio", "DELETE", "/elma365-tempo/x"),
                         ("minio", "GET", "/"), ("minio", "GET", "/minio/health/live"), ("new", "GET", "/s3elma365/k")]
        import sqlite3
        assert sqlite3.connect(tmp_path / "journal.db").execute("SELECT count(*) FROM intents").fetchone()[0] == 0

    asyncio.run(run())
