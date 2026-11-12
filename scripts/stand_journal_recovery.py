#!/usr/bin/env python3
"""Interruptions of the change journal (gateway + handler) on the Docker replica of minio2.

    python scripts/stand_journal_recovery.py [--objects 1500] [--keep]     # needs Docker; ~12 minutes

Same containers, versions and helpers as scripts/stand_pipeline.py, with the new S3 there from the start
(PIPELINE.md: "Если новое S3 появится раньше") and /var/lib/s3gw on a 96 MB tmpfs (block F fills it).
ELMA365-like traffic runs throughout; after each block MinIO and the new S3 are compared object by object.
  K  every s3gw process killed at once (minio2 power cut) + MinIO restart, restarted in the wrong order
  G  kill -9 of the gateway five times under heavy traffic (streamed PUTs, multipart across restarts)
  T  SIGTERM of the gateway (systemctl restart) during uploads: they are cut after GW_SHUTDOWN_TIMEOUT
  H  kill -9 of the handler mid multipart copy, the B4 cleanup of its upload, a crash loop of the handler
  F  journal disk full: 503, recovery without a restart, results that could not be written
  R  journal lost in stage B: restore from the A5 copy by PIPELINE.md, then C1 reconcile; tag-only changes stay lost
Report: .runtime/stand-journal-recovery/report.json; logs of all s3gw processes next to it.
"""
from __future__ import annotations

import argparse
import hashlib
import http.client
import io
import json
import random
import sys
import threading
import time
import traceback
import urllib.parse
import uuid
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List

import stand_pipeline as sp
from stand_pipeline import (BUCKET, DST, ELMA, J, RO, ROOT, alive, body, dx, dx_rc, exit_code, in_minio, mc, ngx, one,
                            s3, s3gw, signal_proc, sql, start, try_op, utc_now, wait, wait_exit)

REPO = Path(__file__).resolve().parents[1]
OUT = REPO / ".runtime" / "stand-journal-recovery"
sp.OUT = OUT
ALL = "'^/opt/venv/bin/python -m s3gw[.]'"
CHUNK = 64 * 1024
SHUTDOWN = 5  # GW_SHUTDOWN_TIMEOUT, the default
NEW_EP, MINIO_EP = "http://newstore:8333", "http://127.0.0.1:9000"

_run = sp.run


def _run_tmpfs(*args: str, **kw: Any) -> Any:
    """The journal on its own small file system, so that it can be filled (block F)."""
    if args[:5] == ("docker", "run", "-d", "--name", sp.HOST):
        args = (*args[:5], "--tmpfs", f"{J}:rw,size=96m,mode=700", *args[5:])
    return _run(*args, **kw)


sp.run = _run_tmpfs


def slow_put(presigner: Any, key: str, size: int, rate: float, seed: Any) -> str:
    """PUT through nginx at `rate` bytes/s by a presigned URL, as a browser uploads: the request stays in flight."""
    url = presigner.generate_presigned_url("put_object", Params={"Bucket": BUCKET, "Key": key}, ExpiresIn=3600)
    u = urllib.parse.urlsplit(url)
    data = body(size, seed)
    conn = http.client.HTTPConnection(u.hostname, u.port, timeout=180)
    try:
        conn.putrequest("PUT", f"{u.path}?{u.query}", skip_accept_encoding=True)
        conn.putheader("Content-Length", str(size))
        conn.putheader("X-Forwarded-For", "10.233.1.8")
        conn.endheaders()
        t0 = time.monotonic()
        for off in range(0, size, CHUNK):
            conn.send(data[off:off + CHUNK])
            ahead = (off + CHUNK) / rate - (time.monotonic() - t0)
            if ahead > 0:
                time.sleep(ahead)
        return str(conn.getresponse().status)
    except Exception as exc:  # noqa: BLE001
        return type(exc).__name__
    finally:
        conn.close()


class Heavy:
    """Streamed PUTs of 2-20 MB at 4 MB/s and multipart uploads with pauses between parts: always something in flight."""

    def __init__(self, uploaders: int = 3, mpus: int = 2) -> None:
        self.stop_ev = threading.Event()
        self.stats: Counter = Counter()
        self.threads = [threading.Thread(target=self.uploader, args=(i,), daemon=True) for i in range(uploaders)]
        self.threads += [threading.Thread(target=self.mpu, args=(i,), daemon=True) for i in range(mpus)]

    def uploader(self, seed: int) -> None:
        rng, cl = random.Random(seed), s3(f"http://127.0.0.1:{sp.PORT}", ELMA)
        while not self.stop_ev.is_set():
            st = slow_put(cl, f"heavy/{uuid.uuid4()}", rng.randint(2, 20) * 2**20, 4 * 2**20, rng.random())
            self.stats[f"slow_put:{st}"] += 1

    def mpu(self, seed: int) -> None:
        cl = s3(f"http://127.0.0.1:{sp.PORT}", ELMA, xff=True)
        while not self.stop_ev.is_set():
            k = f"heavy-mpu/{uuid.uuid4()}"

            def go() -> None:
                u = cl.create_multipart_upload(Bucket=BUCKET, Key=k)["UploadId"]
                parts = []
                for n in (1, 2, 3):
                    self.stop_ev.wait(1.5)
                    p = cl.upload_part(Bucket=BUCKET, Key=k, UploadId=u, PartNumber=n,
                                       Body=body(5 * 2**20 if n < 3 else 1000, f"{k}{n}"))
                    parts.append({"PartNumber": n, "ETag": p["ETag"]})
                cl.complete_multipart_upload(Bucket=BUCKET, Key=k, UploadId=u, MultipartUpload={"Parts": parts})

            self.stats[f"mpu:{try_op(go)}"] += 1

    def start(self) -> None:
        for t in self.threads:
            t.start()

    def stop(self) -> Dict[str, int]:
        self.stop_ev.set()
        for t in self.threads:
            t.join(120)
        return dict(sorted(self.stats.items()))


# Runs on minio2: kills the handler once its multipart copy of `key` in the new S3 has two parts.
POLL_MPU = """
import boto3, os, sys, time
from botocore.config import Config
c = boto3.client('s3', endpoint_url='http://newstore:8333', aws_access_key_id=sys.argv[1], aws_secret_access_key=sys.argv[2],
                 region_name='ru-central-1', config=Config(s3={'addressing_style': 'path'}))
key, end = sys.argv[3], time.time() + 180
while time.time() < end:
    for u in c.list_multipart_uploads(Bucket='s3elma365', Prefix=key).get('Uploads', []):
        n = len(c.list_parts(Bucket='s3elma365', Key=key, UploadId=u['UploadId']).get('Parts', []))
        if n >= 2:
            os.system("pkill -9 -f '^/opt/venv/bin/python -m s3gw[.]handler( |$)'")
            print(n)
            sys.exit(0)
    time.sleep(0.05)
print(0)
"""


class Recovery(sp.Stand):
    ENV = {"GW_LEASE_SECONDS": "70", "LIST_PAGE_SIZE": "100", "LIST_WORKERS": "2", "BULK_MAX_ATTEMPTS": "8",
           "HANDLER_MAX_ATTEMPTS": "8", "DST_ENDPOINT": NEW_EP, "DST_ACCESS_KEY": DST[0], "DST_SECRET_KEY": DST[1]}

    # -- helpers -----------------------------------------------------------------
    def counter(self, name: str) -> int:
        return int(one(f"SELECT value FROM counters WHERE name = '{name}'") or 0)

    def epoch(self) -> int:
        return int(one("SELECT max(epoch) FROM gateway_epochs") or 0)

    def open_old(self) -> int:
        """Intents of dead gateway processes still open: must be 0 after every restart."""
        return int(one("SELECT count(*) FROM intents WHERE result IS NULL AND epoch < (SELECT max(epoch) FROM gateway_epochs)") or 0)

    def intent(self, key: str) -> Any:
        return one(f"SELECT result FROM intents WHERE key = '{key}' ORDER BY id DESC LIMIT 1")

    def kstate(self, key: str) -> Any:
        return one(f"SELECT state FROM key_state WHERE key = '{key}'")

    def settle(self, timeout: float = 400) -> None:
        wait(lambda: self.backlog() == 0, timeout, "handler backlog 0", every=1)

    def identical(self, block: str) -> bool:
        rep = self.compare(MINIO_EP, ROOT, NEW_EP, DST)
        self.results[f"compare_{block}"] = rep
        return self.check(f"{block}: MinIO and the new S3 identical (keys, content, metadata, tags)",
                          rep["missing_in_b"] == rep["extra_in_b"] == rep["different"] == 0,
                          {k: rep[k] for k in ("objects_a", "objects_b", "missing_in_b", "extra_in_b", "different")}
                          | ({"samples": rep["samples"]} if rep["missing_in_b"] or rep["extra_in_b"] or rep["different"] else {}))

    def no_errors(self, block: str) -> bool:
        errs = sql("SELECT key, attempts, last_error FROM key_state WHERE state = 'error'")
        return self.check(f"{block}: no key in error, nothing in flight",
                          not errs and one("SELECT count(*) FROM key_state WHERE in_flight > 0") == 0, errs[:3])

    # -- setup: A1, A2, A4 with the new S3 already there ----------------------------
    def setup(self) -> List[str]:
        keys = self.stage0()
        self.elma = s3(f"http://127.0.0.1:{sp.PORT}", ELMA, xff=True)
        self.new = s3(f"http://127.0.0.1:{sp.NEW_PORT}", DST)
        (OUT / "ro.json").write_text(self.pipeline_block("cat > /tmp/s3gw-src-ro.json <<'EOF'\n", "EOF"), encoding="utf-8")
        mc("admin", "policy", "create", "local", "s3gw-src-ro", "/w/ro.json")
        mc("admin", "user", "add", "local", RO[0], RO[1])
        mc("admin", "policy", "attach", "local", "s3gw-src-ro", "--user", RO[0])
        self.start_newstore()
        self.write_env(**self.ENV)
        self.start_gateway()
        conf = (REPO / "deploy/nginx/s3gw.conf").read_text(encoding="utf-8").replace("10.10.0.12", self.addr)
        conf = conf.replace(f"    # listen {self.addr}:9000;", f"    listen {self.addr}:9000;")
        (OUT / "s3gw.conf").write_text(conf, encoding="utf-8", newline="\n")
        sp.run("docker", "cp", str(OUT / "s3gw.conf"), f"{sp.NGINX}:/etc/nginx/conf.d/s3gw.conf")
        self.start_minio("127.0.0.1:9000")
        ngx("nginx -t && nginx -s reload")
        wait(lambda: sp.http_ok(f"http://127.0.0.1:{sp.PORT}/minio/health/live"), 30, "nginx on minio2:9000")
        ss = dx("ss -Hltn 'sport = :9000'")
        self.check("setup: nginx on <addr>:9000, MinIO on 127.0.0.1:9000, journal on tmpfs",
                   f"{self.addr}:9000" in ss and "*:9000" not in ss and "tmpfs" in dx(f"df {J}"), dx(f"df -h {J} | tail -1").split())
        start("handler", "s3gw.handler")
        start("list", "s3gw.bulk list")
        self.check("setup: listing finished", wait_exit("list", 900) == 0, self.bulk_status()["ranges"])
        return keys

    # -- K: everything killed at once --------------------------------------------
    def block_k(self, traffic: sp.Traffic) -> None:
        s3gw(f"s3gw.bulk set mbps_max 3 --bulk-db {J}/bulk.db")
        start("bulk1", "s3gw.bulk run")
        wait(lambda: self.done() > 150, 300, "copy under way", every=1)
        time.sleep(3)
        e0, u0, d0 = self.epoch(), self.counter("intents_recovered_uncertain"), self.done()
        open_at_kill = one("SELECT count(*) FROM intents WHERE result IS NULL")
        dx_rc(f"pkill -9 -f {ALL}")
        sp.run("docker", "restart", "-t", "0", sp.MINIO)
        codes = {n: wait_exit(n, 30) for n in ("bulk1", "handler", "gateway")}
        self.check("K power cut: gateway, handler and bulk killed at once (137), MinIO restarted", set(codes.values()) == {137}, codes)
        wait(lambda: dx_rc("curl -sf http://127.0.0.1:9000/minio/health/live").returncode == 0, 60, "MinIO up")
        held = sorted(r["key"] for r in sql("SELECT key FROM key_state WHERE state IN ('bulk', 'syncing')"))
        claimed = self.claimed()
        # wrong order on purpose: bulk and handler before the gateway (PIPELINE.md A4: order does not matter)
        start("bulk2", "s3gw.bulk run")
        start("handler", "s3gw.handler")
        time.sleep(5)
        st = try_op(lambda: self.elma.put_object(Bucket=BUCKET, Key="k-before-gateway", Body=b"x"))
        early = {"handler": alive("s3gw.handler"), "bulk": alive("s3gw.bulk"), "write": st}
        self.start_gateway()
        self.check("K handler and bulk come up before the gateway; writes get 502 until it is up",
                   early == {"handler": True, "bulk": True, "write": "502"} and not in_minio("k-before-gateway"), early)
        rec = self.counter("intents_recovered_uncertain") - u0
        self.check("K gateway restart: new epoch, every open intent of the killed one closed as uncertain",
                   self.epoch() == e0 + 1 and self.open_old() == 0, {"open_at_kill": open_at_kill, "recovered_uncertain": rec})
        self.check("K bulk resumes from checkpoints after the power cut", self.moving() and self.done() >= d0,
                   {"done_before": d0, "claimed_at_kill": claimed, "journal_leases_of_dead_processes": len(held)})
        s3gw(f"s3gw.bulk set mbps_max 100000 --bulk-db {J}/bulk.db")
        code = wait_exit("bulk2", 900)
        self.check("K bulk finishes: exit 0, no job left", code == 0 and self.left() == 0, self.bulk_status()["jobs"])
        traffic.pause()
        self.settle()
        states = Counter(r["state"] for r in sql("SELECT state FROM key_state WHERE key IN (%s)" % ",".join(
            "'" + k.replace("'", "''") + "'" for k in held))) if held else Counter()
        self.check("K keys leased by the killed processes synced by the handler after GW_LEASE_SECONDS",
                   set(states) <= {"synced", "deleted"}, dict(states))
        self.no_errors("K")
        self.identical("K")
        traffic.resume()

    # -- G: gateway crash loop under heavy traffic --------------------------------
    def block_g(self, traffic: sp.Traffic) -> None:
        heavy = Heavy()
        heavy.start()
        time.sleep(4)
        e0, u0 = self.epoch(), self.counter("intents_recovered_uncertain")
        kills = []
        for _ in range(5):
            time.sleep(random.uniform(4, 7))
            n_open = one("SELECT count(*) FROM intents WHERE result IS NULL")
            r0 = self.counter("intents_recovered_uncertain")
            signal_proc("s3gw.gateway", "9")
            code = wait_exit("gateway", 10)
            time.sleep(1)  # RestartSec=1
            self.start_gateway()
            kills.append({"exit": code, "open_before_kill": n_open, "recovered": self.counter("intents_recovered_uncertain") - r0})
        time.sleep(5)
        stats = heavy.stop()
        traffic.pause()
        rec = self.counter("intents_recovered_uncertain") - u0
        self.results["g_kills"], self.results["g_heavy"] = kills, stats
        self.check("G five kill -9 of the gateway under traffic: five new epochs, intents left open closed as uncertain",
                   self.epoch() == e0 + 5 and self.open_old() == 0 and rec > 0 and all(k["exit"] == 137 for k in kills),
                   {"kills": kills, "heavy": stats})
        self.settle()
        self.no_errors("G")
        self.identical("G")
        traffic.resume()

    # -- T: SIGTERM (systemctl restart) during uploads ------------------------------
    def block_t(self, traffic: sp.Traffic) -> None:
        """On SIGTERM aiohttp stops reading request bodies: uploads in flight cannot finish and are cut after
        GW_SHUTDOWN_TIMEOUT. Meanwhile writes get 502; reads (straight to MinIO before C2) go on."""
        traffic.pause()
        pre = s3(f"http://127.0.0.1:{sp.PORT}", ELMA)
        ups = {f"sigterm/short-{uuid.uuid4()}": 8 * 2**20, f"sigterm/long-{uuid.uuid4()}": 40 * 2**20}  # 8 s and 40 s
        res: Dict[str, str] = {}
        threads = [threading.Thread(target=lambda k=k, n=n: res.__setitem__(k, slow_put(pre, k, n, 2**20, k)), daemon=True)
                   for k, n in ups.items()]
        for t in threads:
            t.start()
        names = ",".join(f"'{k}'" for k in ups)
        wait(lambda: one(f"SELECT count(*) FROM intents WHERE key IN ({names}) AND result IS NULL") == 2, 20, "both journaled")
        t0 = time.time()
        signal_proc("s3gw.gateway", "TERM")
        writes: Counter = Counter()
        reads: Counter = Counter()
        while alive("s3gw.gateway") and time.time() - t0 < 120:
            writes[try_op(lambda: self.elma.put_object(Bucket=BUCKET, Key=f"sigterm-probe/{uuid.uuid4()}", Body=b"p"))] += 1
            reads[try_op(lambda: self.elma.get_object(Bucket=BUCKET, Key=sp.STABLE)["Body"].read())] += 1
            time.sleep(0.5)
        stop_s = round(time.time() - t0, 1)
        code = exit_code("gateway")
        self.start_gateway()
        for t in threads:
            t.join(120)
        self.results["t"] = {"stop_seconds": stop_s, "exit": code, "writes": dict(writes), "reads": dict(reads), "uploads": res}
        self.check(f"T SIGTERM: new writes 502 at once, reads 200, exit 0 within GW_SHUTDOWN_TIMEOUT ({SHUTDOWN} s)",
                   code == 0 and set(writes) == {"502"} and set(reads) == {"200"} and stop_s <= SHUTDOWN + 6, self.results["t"])
        self.settle()
        cut = {k: (res.get(k), in_minio(k), self.intent(k), self.kstate(k), try_op(lambda: self.new.head_object(Bucket=BUCKET, Key=k)))
               for k in ups}
        self.check("T uploads in flight are cut: client error, not in MinIO, intent closed, absent in the new S3",
                   all(c[0] != "200" and c[1:] in ((False, "uncertain", "deleted", "404"), (False, "error", "deleted", "404"))
                       for c in cut.values()), list(cut.values()))
        traffic.resume()

    # -- H: handler killed mid multipart copy, then a crash loop -------------------
    def block_h(self, traffic: sp.Traffic) -> None:
        from boto3.s3.transfer import TransferConfig

        traffic.pause()
        self.settle()
        key = f"handler-mpu/{uuid.uuid4()}"
        data = body(300 * 2**20, 77)
        dx("cat > /tmp/poll_mpu.py", user="s3gw", input=POLL_MPU)
        out: Dict[str, str] = {}
        poller = threading.Thread(target=lambda: out.__setitem__("parts", dx(
            f"/opt/venv/bin/python /tmp/poll_mpu.py {DST[0]} {DST[1]} '{key}'", user="s3gw", check=False)), daemon=True)
        poller.start()
        time.sleep(2)
        self.elma.upload_fileobj(io.BytesIO(data), BUCKET, key,
                                 Config=TransferConfig(multipart_threshold=64 * 2**20, multipart_chunksize=64 * 2**20))
        poller.join(200)
        parts = int((out.get("parts") or "0").strip() or 0)
        if not parts:  # copy done before the poller saw it: kill anyway, the check below fails on parts
            signal_proc("s3gw.handler", "9")
        code = wait_exit("handler", 30)
        at_kill = self.kstate(key)
        start("handler", "s3gw.handler")
        t0 = time.time()
        wait(lambda: self.kstate(key) == "synced", 300, "big key synced after the lease", every=1)
        took = round(time.time() - t0)
        got = hashlib.md5(self.new.get_object(Bucket=BUCKET, Key=key)["Body"].read()).hexdigest()
        orphans = len(self.new.list_multipart_uploads(Bucket=BUCKET, Prefix=key).get("Uploads", []))
        self.results["h_mpu"] = {"parts_at_kill": parts, "state_at_kill": at_kill, "synced_after_s": took,
                                 "incomplete_uploads_left_in_new_s3": orphans}
        self.check("H kill -9 of the handler mid multipart copy: the key waits for the lease (GW_LEASE_SECONDS=70), "
                   "then is copied whole; the killed copy's upload stays incomplete in the new S3",
                   parts >= 2 and code == 137 and at_kill == "syncing" and took >= 40 and orphans >= 1
                   and got == hashlib.md5(data).hexdigest(), self.results["h_mpu"])
        # PIPELINE.md B4, literally, with the age cut from 1d to 1s (the stand's upload is minutes old)
        cmd = self.pipeline_block("Убрать те, что старше суток (свежие могут быть копированием работающего обработчика):\n\n```bash\n",
                                  "```").replace("--older-than 1d", "--older-than 1s")
        r = dx_rc(cmd)
        left = len(self.new.list_multipart_uploads(Bucket=BUCKET, Prefix=key).get("Uploads", []))
        self.check("H B4 cleanup removes the incomplete upload, the object stays",
                   r.returncode == 0 and left == 0 and try_op(lambda: self.new.head_object(Bucket=BUCKET, Key=key)) == "200",
                   {"listed_before": r.stdout.strip(), "left": left, "stderr": r.stderr.strip()[-200:]})
        traffic.resume()
        time.sleep(3)
        left_syncing = []
        for _ in range(6):
            time.sleep(3)
            signal_proc("s3gw.handler", "9")
            wait_exit("handler", 10)
            left_syncing.append(int(one("SELECT count(*) FROM key_state WHERE state = 'syncing'") or 0))
            time.sleep(1)  # RestartSec=5 in the unit; 1 here
            start("handler", "s3gw.handler")
        time.sleep(5)
        traffic.pause()
        self.settle()
        self.results["h_crash_loop_syncing_left"] = left_syncing
        self.check("H six kill -9 of the handler under traffic: keys of the dead processes retaken after the lease, backlog 0",
                   self.backlog() == 0 and alive("s3gw.handler"), {"syncing_left_by_each_kill": left_syncing})
        self.no_errors("H")
        self.identical("H")
        traffic.resume()

    # -- F: journal disk full --------------------------------------------------------
    def block_f(self, traffic: sp.Traffic) -> None:
        traffic.pause()
        self.settle()
        pre = s3(f"http://127.0.0.1:{sp.PORT}", ELMA)
        k_up = f"diskfull/in-flight-{uuid.uuid4()}"
        res: Dict[str, str] = {}
        t = threading.Thread(target=lambda: res.__setitem__("up", slow_put(pre, k_up, 6 * 2**20, 128 * 1024, 5)), daemon=True)  # 48 s
        t.start()
        wait(lambda: one(f"SELECT count(*) FROM intents WHERE key = '{k_up}' AND result IS NULL") == 1, 20, "upload journaled")
        df = dx(f"dd if=/dev/zero of={J}/fill bs=1M 2>/dev/null; sync; df -k {J} | tail -1", check=False).split()
        # SQLite writes into the WAL it already has: the journal fails only once that space is used up
        statuses: Counter = Counter()
        k503 = None
        for i in range(2000):
            k = f"diskfull/new-{i}-{uuid.uuid4()}"
            st = try_op(lambda: self.elma.put_object(Bucket=BUCKET, Key=k, Body=b"x"))
            statuses[st] += 1
            if st == "503":
                k503 = k
                break
        st_get = try_op(lambda: self.elma.get_object(Bucket=BUCKET, Key=sp.STABLE)["Body"].read())
        t.join(90)
        time.sleep(3)
        alive_now = {"gateway": alive("s3gw.gateway"), "handler": alive("s3gw.handler")}
        self.results["f_full"] = {"df": df, "writes_until_503": dict(statuses), "in_flight_upload": res.get("up")}
        self.check("F journal disk full: writes end in 503 and are not forwarded, reads 200, gateway and handler stay up",
                   k503 is not None and not in_minio(k503) and st_get == "200" and all(alive_now.values()),
                   {"writes_until_503": dict(statuses), "read": st_get, **alive_now})
        dx(f"rm -f {J}/fill")
        time.sleep(2)
        k_after = f"diskfull/after-{uuid.uuid4()}"
        st_after = try_op(lambda: self.elma.put_object(Bucket=BUCKET, Key=k_after, Body=b"after"))
        self.check("F space freed: writes accepted and journaled again without a restart",
                   st_after == "200" and self.intent(k_after) == "success", st_after)
        time.sleep(20)  # handler idle loop, settle 3 s: anything it can take, it has taken
        stuck = sql("SELECT i.key, s.in_flight, s.state FROM intents i JOIN key_state s ON s.key = i.key WHERE i.result IS NULL")
        hlog = dx("grep -c 'journal error' /var/log/s3gw/handler.log || true").strip()
        glog = dx("grep -c 'journal write failed' /var/log/s3gw/gateway.log || true").strip()
        self.results["f_after_free"] = {"open_intents": stuck, "handler_journal_errors": hlog, "gateway_result_write_failures": glog}
        # PIPELINE.md "Намерение осталось без результата": open intents older than a minute -> restart the gateway
        self.stop_gateway()
        self.start_gateway()
        self.settle()
        fin = {r["key"]: (self.intent(r["key"]), self.kstate(r["key"]), in_minio(r["key"]),
                          try_op(lambda k=r["key"]: self.new.head_object(Bucket=BUCKET, Key=k)) == "200") for r in stuck}
        self.check("F results lost to the full disk leave intents open (keys wait); a gateway restart closes them as "
                   "uncertain and the handler syncs those keys",
                   bool(stuck) and self.open_old() == 0
                   and all(v[0] == "uncertain" and v[1] in ("synced", "deleted") and v[2] == v[3] for v in fin.values()),
                   {"open_before_restart": len(stuck), "after": list(fin.values())[:3], "gateway_result_write_failures": glog})
        self.no_errors("F")
        self.identical("F")
        traffic.resume()

    # -- R: journal lost in stage B, restored from the A5 copy -----------------------
    def block_r(self, traffic: sp.Traffic) -> None:
        traffic.resume()
        r = dx_rc("date -u +%Y-%m-%dT%H:%M:%S.%6NZ && "  # created_at format of the journal, minio2's clock
                  f"JOURNAL_DB={J}/journal.db BACKUP_DIR=/backup/s3gw KEEP=2 sh /opt/elma365_s3_migrator/deploy/systemd/"
                  "s3gw-journal-backup.sh && ls -1t /backup/s3gw/journal-2*.db | head -1", user="s3gw")
        t_backup, backup = r.stdout.strip().splitlines()[0], r.stdout.strip().splitlines()[-1]
        time.sleep(15)  # changes after the copy that the handler syncs
        signal_proc("s3gw.handler", "TERM")
        wait_exit("handler", 60)
        time.sleep(15)  # changes after the copy that the handler never saw: lost with the journal
        # A5 "Восстановление": routing rollback with a trace, stop handler and bulk, then the gateway; the copy in place
        trace = self.trace_start("restore")
        time.sleep(2)
        ngx("sed -i 's|default s3gw_gateway;|default s3gw_minio;|' /etc/nginx/conf.d/s3gw.conf && nginx -t && nginx -s reload")
        time.sleep(2)
        self.stop_gateway()
        dx(f"rm -rf /var/tmp/lost && mkdir -p /var/tmp/lost && cp {J}/journal.db* /var/tmp/lost/", user="s3gw")  # this report only
        dx(f"rm -f {J}/journal.db-wal {J}/journal.db-shm && cp {backup} {J}/journal.db", user="s3gw")
        e_backup = self.epoch()
        open_in_copy = one("SELECT count(*) FROM intents WHERE result IS NULL")
        self.start_gateway()
        ngx("sed -i 's|default s3gw_minio;|default s3gw_gateway;|' /etc/nginx/conf.d/s3gw.conf && nginx -t && nginx -s reload")
        time.sleep(5)
        tc = self.trace_check(trace, "restore", mark_dirty=True)
        start("handler", "s3gw.handler")
        rids = [m["rid"] for m in tc["missed"]]
        in_lost = int(dx("sqlite3 /var/tmp/lost/journal.db \"SELECT count(DISTINCT request_id) FROM intents WHERE request_id IN (%s)\""
                         % ",".join(f"'{x}'" for x in rids or ["-"]), user="s3gw").strip() or 0)
        self.check("R gateway on the restored copy: new epoch, open intents of the copy closed; tracecheck --mark-dirty "
                   "hands over the bypass of the restore window and the missed changes, which were journaled only in the lost journal",
                   self.epoch() == e_backup + 1 and self.open_old() == 0 and tc["missed_total"] > 0 and in_lost == len(set(rids))
                   and tc.get("marked_dirty", 0) > 0,
                   {"backup": backup, "open_in_copy": open_in_copy, "bypass_total": tc["bypass_total"],
                    "missed_total": tc["missed_total"], "missed_sample_in_lost_journal": f"{in_lost}/{len(set(rids))}",
                    "marked_dirty": tc.get("marked_dirty")})
        traffic.pause()
        self.settle()
        lost_rows = json.loads(dx("sqlite3 -json /var/tmp/lost/journal.db \"SELECT i.key, group_concat(DISTINCT i.op) AS ops, "
                                  "s.synced_seq = s.change_seq AS synced FROM intents i JOIN key_state s ON s.key = i.key "
                                  f"WHERE i.created_at > '{t_backup}' GROUP BY i.key\"", user="s3gw") or "[]")
        unsynced = [r for r in lost_rows if not r["synced"]]
        # a lost tag change may follow a PUT the handler did sync: such a key's lost ops are 'put,tagging'
        lost_tagging = {r["key"]: r["ops"] for r in unsynced if "tagging" in r["ops"].split(",")}
        dx("cat > /var/tmp/lost-keys.txt && chmod 644 /var/tmp/lost-keys.txt", input="\n".join(r["key"] for r in unsynced) + "\n")
        before = json.loads(s3gw("s3gw.verifykeys --keys-file /var/tmp/lost-keys.txt", check=False).stdout)
        self.results["r_lost"] = {"changed_after_copy": len(lost_rows), "unsynced_when_lost": len(unsynced),
                                  "unsynced_with_tagging": len(lost_tagging), "verifykeys_on_them": before["mismatched"]}
        # stage B: full reconcile (C1), its discrepancies re-checked and handed to the handler
        dx(f"mc alias set minio_old {MINIO_EP} {RO[0]} {RO[1]} >/dev/null && mc alias set minio_new {NEW_EP} {sp.NEWRO[0]} {sp.NEWRO[1]} >/dev/null")
        v = self.st5_verify(utc_now())
        dx("cd /opt/elma365_s3_migrator && sqlite3 .runtime/st5-reconcile/state.db "
           "\"SELECT key FROM reconcile_queue UNION SELECT key FROM dst_only\" > /var/tmp/reconcile-keys.txt && chmod 644 /var/tmp/reconcile-keys.txt")
        found = json.loads(s3gw("s3gw.verifykeys --keys-file /var/tmp/reconcile-keys.txt --mark-dirty", check=False).stdout)
        self.settle()
        again = json.loads(s3gw("s3gw.verifykeys --keys-file /var/tmp/reconcile-keys.txt", check=False).stdout)
        self.results["r_reconcile"] = {k: v.get(k) for k in ("missing_count", "size_mismatch_count", "dst_only_count", "partition_errors", "exit")} \
            | {"marked_dirty": found.get("marked_dirty"), "recheck_mismatched": again["mismatched"]}
        self.check("R C1 reconcile finds the lost changes that verifykeys sees, --mark-dirty repairs them",
                   v["partition_errors"] == 0 and before["mismatched"] > 0 and found["mismatched"] >= before["mismatched"]
                   and again["mismatched"] == 0, {**self.results["r_lost"], **self.results["r_reconcile"]})
        self.no_errors("R")
        rep = self.compare(MINIO_EP, ROOT, NEW_EP, DST, samples=100000)
        diff = rep["samples"]["different"]
        self.results["compare_R"] = {k: rep[k] for k in ("objects_a", "objects_b", "missing_in_b", "extra_in_b", "different")}
        self.results["r_different"] = [{**d, "lost_ops": lost_tagging.get(d["key"])} for d in diff]
        self.check("R after the repair only tags differ, and only on keys whose tag change was lost with the journal "
                   "(C1 does not compare tags, PIPELINE.md A5)",
                   rep["missing_in_b"] == rep["extra_in_b"] == 0 and all(d["problem"] == "tags" for d in diff)
                   and {d["key"] for d in diff} <= set(lost_tagging),
                   {**self.results["compare_R"], "unsynced_with_tagging": len(lost_tagging),
                    "outside": [d for d in self.results["r_different"] if d["problem"] != "tags" or not d["lost_ops"]][:5]})

    def run(self) -> int:
        traffic = None
        ok = True
        try:
            keys = self.setup()
            self.note(f"setup: {len(keys)} objects, minio2 at {self.addr}")
            traffic = sp.Traffic(list(keys), n=4)
            traffic.start()
            for block in (self.block_k, self.block_g, self.block_t, self.block_h, self.block_f, self.block_r):
                self.note(f"--- {block.__name__}")
                block(traffic)
        except Exception as exc:  # noqa: BLE001
            ok = False
            self.check(f"stand aborted: {type(exc).__name__}", False, str(exc)[:500])
            traceback.print_exc()
        finally:
            if traffic:
                traffic.stop()
                self.results["traffic"] = traffic.stats()
            try:
                self.results["counters"] = {r["name"]: r["value"] for r in sql("SELECT name, value FROM counters")}
            except Exception:  # noqa: BLE001
                pass
            self.collect_logs()
            self.results["checks"] = self.checks
            self.results["seconds"] = round(time.time() - self.t0)
            (OUT / "report.json").write_text(json.dumps(self.results, indent=1, ensure_ascii=False, default=str), encoding="utf-8")
        failed = [c["check"] for c in self.checks if not c["ok"]]
        print(f"\n{len(self.checks) - len(failed)}/{len(self.checks)} checks passed in {self.results['seconds']} s; report {OUT / 'report.json'}")
        if failed:
            print("FAILED:\n  " + "\n  ".join(failed))
        return 0 if ok and not failed else 1


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--objects", type=int, default=1500)
    p.add_argument("--keep", action="store_true", help="leave the containers running for inspection")
    a = p.parse_args()
    stand = Recovery(a.objects)
    try:
        return stand.run()
    finally:
        if not a.keep:
            stand.cleanup()


if __name__ == "__main__":
    sys.exit(main())
