"""Gateway against an in-process fake upstream (no MinIO needed)."""
from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path
from typing import Any, Dict, List

import pytest

aiohttp = pytest.importorskip("aiohttp")
from aiohttp import web  # noqa: E402

from s3gw import gateway  # noqa: E402


async def _run(tmp_path: Path, scenario) -> Dict[str, Any]:  # type: ignore[no-untyped-def]
    seen: Dict[str, Any] = {}

    async def upstream_handler(request: web.Request) -> web.Response:
        seen.setdefault("bodies", []).append(await request.read())
        seen.setdefault("marks", []).append(request.headers.getall(gateway.REQUEST_ID_HEADER, []))
        resp = web.Response(
            status=200,
            body=b'<?xml version="1.0"?><DeleteResult><Deleted><Key>k0</Key></Deleted></DeleteResult>',
            content_type="application/xml",
        )
        resp.headers.add("Vary", "Origin")
        resp.headers.add("Vary", "Accept-Encoding")
        return resp

    up_app = web.Application()
    up_app.router.add_route("*", "/{tail:.*}", upstream_handler)
    up_runner = web.AppRunner(up_app)
    await up_runner.setup()
    up_site = web.TCPSite(up_runner, "127.0.0.1", 0)
    await up_site.start()
    up_port = up_runner.addresses[0][1]

    cfg = gateway.GatewayConfig(journal_path=tmp_path / "journal.db", upstream=f"http://127.0.0.1:{up_port}", bucket="b")
    gw_runner = web.AppRunner(gateway.build_app(cfg))
    await gw_runner.setup()
    gw_site = web.TCPSite(gw_runner, "127.0.0.1", 0)
    await gw_site.start()
    gw_port = gw_runner.addresses[0][1]
    try:
        async with aiohttp.ClientSession() as session:
            seen.update(await scenario(session, f"http://127.0.0.1:{gw_port}"))
    finally:
        await gw_runner.cleanup()
        await up_runner.cleanup()
    return seen


def _delete_body(n: int) -> bytes:
    objs = "".join(f"<Object><Key>k{i}</Key></Object>" for i in range(n))
    return f'<?xml version="1.0" encoding="UTF-8"?><Delete><Quiet>true</Quiet>{objs}</Delete>'.encode()


def test_delete_objects_body_arriving_in_chunks_is_read_whole(tmp_path: Path) -> None:
    body = _delete_body(1000)

    async def scenario(session: Any, base: str) -> Dict[str, Any]:
        async def chunks():  # type: ignore[no-untyped-def]
            for i in range(0, len(body), 4096):
                yield body[i : i + 4096]
                await asyncio.sleep(0.005)

        async with session.post(f"{base}/b?delete", data=chunks()) as r:
            out = {"status": r.status, "vary": r.headers.getall("Vary", []), "ctype": r.headers.getall("Content-Type")}
        async with session.put(f"{base}/b/k1", data=b"x" * 10) as r:
            out["put"] = (r.status, r.headers.getall("Content-Length"), r.headers.getall("Vary", []))
        return out

    seen = asyncio.run(_run(tmp_path, scenario))
    assert seen["status"] == 200
    assert seen["bodies"][0] == body
    assert seen["vary"] == ["Origin", "Accept-Encoding"]
    assert seen["ctype"] == ["application/xml"]
    status, length, vary = seen["put"]
    assert status == 200 and len(length) == 1 and vary == ["Origin", "Accept-Encoding"]
    conn = sqlite3.connect(str(tmp_path / "journal.db"))
    assert conn.execute("SELECT count(*) FROM intents WHERE result = 'success'").fetchone()[0] == 1001


def test_error_xml_message_is_escaped() -> None:
    xml: List[str] = [gateway.s3_error_xml("X", "a<b & c").decode()]
    assert "a&lt;b &amp; c" in xml[0]


@pytest.mark.parametrize("listen", ["9100", ":9100", "host:port"])
def test_listen_must_be_host_port(listen: str) -> None:
    with pytest.raises(SystemExit):
        gateway.main(["--listen", listen])


def test_forwarded_requests_carry_request_id_of_their_intents(tmp_path: Path) -> None:
    async def scenario(session: Any, base: str) -> Dict[str, Any]:
        headers = {gateway.REQUEST_ID_HEADER: "forged"}  # a client must not be able to fake the mark
        async with session.put(f"{base}/b/k1", data=b"x", headers=headers) as r:
            assert r.status == 200
        async with session.get(f"{base}/b/k1") as r:
            assert r.status == 200
        return {}

    seen = asyncio.run(_run(tmp_path, scenario))
    (put_mark,), (get_mark,) = seen["marks"]
    assert put_mark != "forged" and len(put_mark) == 32 and get_mark != put_mark
    conn = sqlite3.connect(str(tmp_path / "journal.db"))
    assert conn.execute("SELECT key FROM intents WHERE request_id = ?", (put_mark,)).fetchall() == [("k1",)]
