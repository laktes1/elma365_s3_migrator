#!/usr/bin/env python3
"""Whole PIPELINE.md (stages A, B, C) on a Docker replica of minio2, with failures injected.

    python scripts/stand_pipeline.py [--objects 4000] [--keep]     # needs Docker; images are pulled/built

minio2 is one network namespace, as on the server:
  pipe-minio2    Python 3.11 + the working tree (git archive), runs gateway/handler/bulk as user s3gw
  pipe-minio     MinIO RELEASE.2025-09-07 in minio2's netns: *:9000 first, 127.0.0.1:9000 after A4
  pipe-nginx     nginx 1.28 in minio2's netns with deploy/nginx/s3gw.conf (10.10.0.12 -> minio2's address)
  pipe-newstore  SeaweedFS 4.48, the new S3, a separate host on the same network
The host plays ELMA365 (X-Forwarded-For, through minio2:9000 published as 127.0.0.1:29000) and the
operator: every step runs the PIPELINE.md command or its exact equivalent, then checks the result.
Failures: kill -9 / SIGTERM of the lister, the copier, the handler and the gateway (also at the
fault points after_intent / after_upstream), a locked journal, the new S3 down, a source object
deleted after listing, a second bulk process, a routing rollback, the cutover and its rollback.
ELMA365 calls checked one by one through nginx: special keys, metadata with "_", tags set and removed, multipart,
DeleteObjects on 400 keys, presigned PUT/GET, a bucket-level change (501), MinIO unreachable (intent error).
Data is compared object by object (content, metadata, tags) between MinIO and the new S3.
Report: .runtime/stand-pipeline/report.json; logs of all s3gw processes next to it.
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import hashlib
import io
import json
import os
import random
import re
import shlex
import subprocess
import sys
import textwrap
import threading
import time
import traceback
import urllib.parse
import urllib.request
import uuid
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

REPO = Path(__file__).resolve().parents[1]
OUT = REPO / ".runtime" / "stand-pipeline"
NET = "s3gw-pipe"
HOST, MINIO, NGINX, NEW, TRACE = "pipe-minio2", "pipe-minio", "pipe-nginx", "pipe-newstore", "pipe-trace"
HOST_IMG = "s3gw-stand-host:py311"
MINIO_IMG = "minio/minio:RELEASE.2025-09-07T16-13-09Z"
MC_IMG = "minio/mc:RELEASE.2025-08-13T08-35-41Z"
NGINX_IMG = "nginx:1.28"
WEED_IMG = "chrislusf/seaweedfs:4.48"
PORT, NEW_PORT = 29000, 28333
ROOT = ("pipeadmin", "pipeadminsecret")
ELMA = ("elmauser", "elmasecret123")       # ELMA365's key: in MinIO and, from C0, in the new S3
RO = ("s3gw-src", "s3gwsrcsecret123")      # A1
REV = ("s3gw-dst-minio", "s3gwdstminio1")  # C0.2
DST = ("newdst", "newdstsecret123")        # new S3: write key of bulk/handler
NEWRO = ("newro", "newrosecret123")        # new S3: read key of the reverse handler
BUCKET = "s3elma365"
STABLE = "stable/policy.pdf"  # read checks; the traffic never touches it
SPECIAL = ["a/b+c.pdf", "a/b c.pdf", "a/b+ +c.pdf", "x/100%.txt", "x/%2B.txt", "папка/Договор №1 (копия).docx",
           "k/semi;colon&amp=eq?#~", "AB12-upper", "zz/последний"]
OTHER = ("elma365-loki", "elma365-backups")
J = "/var/lib/s3gw"
PYM = "/opt/venv/bin/python -m"

# ---------------------------------------------------------------------------
# shell helpers
# ---------------------------------------------------------------------------


def run(*args: str, check: bool = True, input: Optional[str] = None, timeout: float = 900) -> subprocess.CompletedProcess:
    env = {**os.environ, "MSYS_NO_PATHCONV": "1"}
    # bytes, not text mode: on Windows text mode turns \n of the input into \r\n
    raw = subprocess.run(list(args), capture_output=True, input=input.encode("utf-8") if input is not None else None,
                         timeout=timeout, env=env)
    r = subprocess.CompletedProcess(raw.args, raw.returncode, raw.stdout.decode("utf-8", "replace"),
                                    raw.stderr.decode("utf-8", "replace"))
    if check and r.returncode != 0:
        raise RuntimeError(f"{' '.join(args)[:300]} -> {r.returncode}\n{r.stdout[-2000:]}\n{r.stderr[-2000:]}")
    return r


def dx(cmd: str, user: str = "root", check: bool = True, input: Optional[str] = None, timeout: float = 900) -> str:
    """Command on minio2."""
    return run("docker", "exec", "-i", "-u", user, HOST, "sh", "-c", cmd, check=check, input=input, timeout=timeout).stdout


def dx_rc(cmd: str, user: str = "root") -> subprocess.CompletedProcess:
    return run("docker", "exec", "-u", user, HOST, "sh", "-c", cmd, check=False)


def ngx(cmd: str) -> subprocess.CompletedProcess:
    return run("docker", "exec", NGINX, "sh", "-c", cmd, check=False)


def mc(*args: str, check: bool = True, input_text: Optional[str] = None) -> subprocess.CompletedProcess:
    """mc on minio2 with an admin alias to 127.0.0.1:9000, as in PIPELINE.md."""
    return run("docker", "run", "--rm", "-i", "--network", f"container:{HOST}",
               "-e", f"MC_HOST_local=http://{ROOT[0]}:{ROOT[1]}@127.0.0.1:9000",
               "-v", f"{OUT.as_posix()}:/w", MC_IMG, *args, check=check, input=input_text or "")


def in_minio(key: str) -> bool:
    return mc("stat", f"local/{BUCKET}/{key}", check=False).returncode == 0


def sql(q: str, db: str = "journal.db") -> List[Dict[str, Any]]:
    out = dx(f"sqlite3 -readonly -json -cmd '.timeout 10000' {J}/{db} {shlex.quote(q)}", user="s3gw")
    return json.loads(out) if out.strip() else []


def one(q: str, db: str = "journal.db") -> Any:
    rows = sql(q, db)
    return next(iter(rows[0].values())) if rows else None


ENV_PREFIX = "set -a; . /etc/s3gw/s3gw.env; set +a; cd /opt/elma365_s3_migrator; export PYTHONDONTWRITEBYTECODE=1;"


def s3gw(args: str, extra_env: str = "", check: bool = True, timeout: float = 900) -> subprocess.CompletedProcess:
    """A s3gw command as user s3gw with /etc/s3gw/s3gw.env (EnvironmentFile of the units)."""
    return run("docker", "exec", "-u", "s3gw", HOST, "sh", "-c", f"{ENV_PREFIX} {extra_env} {PYM} {args}",
               check=check, timeout=timeout)


def start(name: str, args: str, extra_env: str = "") -> None:
    """Background s3gw process (a unit); its exit code lands in /var/log/s3gw/<name>.exit."""
    dx(f"rm -f /var/log/s3gw/{name}.exit", user="s3gw")
    run("docker", "exec", "-d", "-u", "s3gw", HOST, "sh", "-c",
        f"{ENV_PREFIX} {extra_env} /opt/venv/bin/python -m {args} >> /var/log/s3gw/{name}.log 2>&1; "
        f"echo $? > /var/log/s3gw/{name}.exit")


def pattern(module: str) -> str:
    return shlex.quote(f"^/opt/venv/bin/python -m {re.escape(module)}( |$)")


def alive(module: str) -> bool:
    return dx_rc(f"pgrep -f {pattern(module)}").returncode == 0


def signal_proc(module: str, sig: str) -> None:
    dx_rc(f"pkill -{sig} -f {pattern(module)}")


def exit_code(name: str) -> Optional[int]:
    out = dx_rc(f"cat /var/log/s3gw/{name}.exit 2>/dev/null").stdout.strip()
    return int(out) if out else None


def wait(fn: Callable[[], Any], timeout: float, what: str, every: float = 0.5) -> Any:
    end = time.time() + timeout
    last: Any = None
    while time.time() < end:
        try:
            last = fn()
            if last:
                return last
        except Exception as exc:  # noqa: BLE001 - polled conditions may fail while a service starts
            last = exc
        time.sleep(every)
    raise TimeoutError(f"{what}: not reached in {timeout:.0f} s (last: {last!r})")


def wait_exit(name: str, timeout: float) -> int:
    return int(wait(lambda: (exit_code(name),) if exit_code(name) is not None else None, timeout, f"{name} exit")[0])


def http_ok(url: str) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=3) as r:
            return r.status == 200
    except Exception:  # noqa: BLE001
        return False


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def utc_minus(ts: str, minutes: float) -> str:
    t = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc) - timedelta(minutes=minutes)
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# S3 clients
# ---------------------------------------------------------------------------


def s3(endpoint: str, creds: tuple, xff: bool = False) -> Any:
    import boto3
    from botocore.config import Config

    c = boto3.client(
        "s3", endpoint_url=endpoint, aws_access_key_id=creds[0], aws_secret_access_key=creds[1], region_name="ru-central-1",
        config=Config(s3={"addressing_style": "path"}, signature_version="s3v4", retries={"max_attempts": 1},
                      connect_timeout=5, read_timeout=60, max_pool_connections=32,
                      request_checksum_calculation="when_required", response_checksum_validation="when_required"),
    )
    if xff:  # ingress-nginx adds these; not signed
        def add(request, **_kw):  # type: ignore[no-untyped-def]
            request.headers["X-Forwarded-For"] = "10.233.1.7"
            request.headers["X-Real-IP"] = "10.233.1.7"
        c.meta.events.register("before-send.s3", add)
    return c


def status_of(exc: Exception) -> str:
    resp = getattr(exc, "response", None) or {}
    code = resp.get("ResponseMetadata", {}).get("HTTPStatusCode")
    return str(code or type(exc).__name__)


def try_op(fn: Callable[[], Any]) -> str:
    try:
        fn()
        return "200"
    except Exception as exc:  # noqa: BLE001
        return status_of(exc)


def body(size: int, seed: int) -> bytes:
    block = hashlib.sha256(str(seed).encode()).digest() * 64  # 2 KiB, distinct per object
    return (block * (size // len(block) + 1))[:size]


# ---------------------------------------------------------------------------
# ELMA365-like traffic through nginx
# ---------------------------------------------------------------------------


class Elma(threading.Thread):
    """Puts, overwrites, deletes, copies, batch deletes, tags, multipart, reads; never stops on errors."""

    def __init__(self, keys: List[str], lock: threading.Lock, seed: int) -> None:
        super().__init__(daemon=True, name=f"elma-{seed}")
        self.cl = s3(f"http://127.0.0.1:{PORT}", ELMA, xff=True)
        self.keys, self.lock, self.rng = keys, lock, random.Random(seed)
        self.stop_ev, self.go = threading.Event(), threading.Event()
        self.go.set()
        self.stats: Counter = Counter()
        self.ops = 0

    def pick(self) -> Optional[str]:
        with self.lock:
            return self.rng.choice(self.keys) if self.keys else None

    def step(self) -> None:
        r, cl, b = self.rng.random(), self.cl, BUCKET
        if r < 0.30:
            k = str(uuid.uuid4())
            st = try_op(lambda: cl.put_object(Bucket=b, Key=k, Body=body(self.rng.randint(100, 20000), self.ops),
                                              ContentType="application/pdf", Metadata={"author": "elma"}))
            if st == "200":
                with self.lock:
                    self.keys.append(k)
            op = "put"
        elif r < 0.45 and (k := self.pick()):
            st, op = try_op(lambda: cl.put_object(Bucket=b, Key=k, Body=body(self.rng.randint(100, 9000), -self.ops))), "overwrite"
        elif r < 0.53 and (k := self.pick()):
            st, op = try_op(lambda: cl.delete_object(Bucket=b, Key=k)), "delete"
        elif r < 0.58 and (k := self.pick()):
            dst = "copy/" + str(uuid.uuid4())
            st, op = try_op(lambda: cl.copy_object(Bucket=b, Key=dst, CopySource={"Bucket": b, "Key": k})), "copy"
            if st == "200":
                with self.lock:
                    self.keys.append(dst)
        elif r < 0.60:
            ks = [x for x in (self.pick() for _ in range(5)) if x]
            st, op = try_op(lambda: cl.delete_objects(Bucket=b, Delete={"Objects": [{"Key": x} for x in ks], "Quiet": True})), "batch_delete"
        elif r < 0.65 and (k := self.pick()):
            st, op = try_op(lambda: cl.put_object_tagging(Bucket=b, Key=k, Tagging={"TagSet": [
                {"Key": "status", "Value": self.rng.choice(["draft", "signed"])}]})), "tag"
        elif r < 0.655:
            k = "mpu/" + str(uuid.uuid4())

            def mpu() -> None:
                u = cl.create_multipart_upload(Bucket=b, Key=k)["UploadId"]
                p1 = cl.upload_part(Bucket=b, Key=k, UploadId=u, PartNumber=1, Body=body(5 * 1024 * 1024, self.ops))
                p2 = cl.upload_part(Bucket=b, Key=k, UploadId=u, PartNumber=2, Body=b"tail")
                cl.complete_multipart_upload(Bucket=b, Key=k, UploadId=u, MultipartUpload={"Parts": [
                    {"PartNumber": 1, "ETag": p1["ETag"]}, {"PartNumber": 2, "ETag": p2["ETag"]}]})
            st, op = try_op(mpu), "multipart"
        elif r < 0.68:
            st, op = try_op(lambda: cl.put_object(Bucket=OTHER[0], Key=f"chunk-{self.ops}", Body=b"log")), "other_bucket"
        else:
            k = self.pick()
            st, op = try_op(lambda: cl.get_object(Bucket=b, Key=k or "none")["Body"].read()), "get"
        self.stats[f"{op}:{st}"] += 1

    def run(self) -> None:
        while not self.stop_ev.is_set():
            if not self.go.wait(0.2):
                continue
            self.ops += 1
            self.step()
            time.sleep(0.05)


class Traffic:
    def __init__(self, keys: List[str], n: int = 2) -> None:
        self.lock = threading.Lock()
        self.keys = keys
        self.workers = [Elma(keys, self.lock, seed=i + 1) for i in range(n)]

    def forget(self, key: str) -> None:
        with self.lock:
            if key in self.keys:
                self.keys.remove(key)

    def start(self) -> None:
        for w in self.workers:
            w.start()

    def pause(self) -> None:
        for w in self.workers:
            w.go.clear()
        time.sleep(1.5)  # let requests in flight finish

    def resume(self) -> None:
        for w in self.workers:
            w.go.set()

    def stop(self) -> None:
        for w in self.workers:
            w.stop_ev.set()
            w.go.set()
        for w in self.workers:
            if w.ident is not None:
                w.join(10)

    def stats(self) -> Dict[str, int]:
        total: Counter = Counter()
        for w in self.workers:
            total.update(w.stats)
        return dict(sorted(total.items()))


# ---------------------------------------------------------------------------
# compare mode: runs on minio2 (it reaches MinIO at 127.0.0.1:9000 and the new S3)
# ---------------------------------------------------------------------------


def compare_buckets(a: Any, b: Any, bucket: str, workers: int = 16) -> Dict[str, Any]:
    def listing(cl: Any) -> Dict[str, int]:
        out: Dict[str, int] = {}
        for page in cl.get_paginator("list_objects_v2").paginate(Bucket=bucket):
            for o in page.get("Contents", []):
                out[o["Key"]] = int(o["Size"])
        return out

    la, lb = listing(a), listing(b)
    missing, extra = sorted(set(la) - set(lb)), sorted(set(lb) - set(la))
    common = sorted(set(la) & set(lb))
    problems: List[Dict[str, Any]] = []

    def one_key(k: str) -> Optional[Dict[str, Any]]:
        ra, rb = a.get_object(Bucket=bucket, Key=k), b.get_object(Bucket=bucket, Key=k)
        ha, hb = hashlib.md5(ra["Body"].read()).hexdigest(), hashlib.md5(rb["Body"].read()).hexdigest()
        if ha != hb:
            return {"key": k, "problem": "content"}
        for f in ("Metadata", "ContentType", "ContentDisposition"):
            if ra.get(f) != rb.get(f):
                return {"key": k, "problem": f, "a": ra.get(f), "b": rb.get(f)}
        ta = sorted((t["Key"], t["Value"]) for t in a.get_object_tagging(Bucket=bucket, Key=k)["TagSet"])
        tb = sorted((t["Key"], t["Value"]) for t in b.get_object_tagging(Bucket=bucket, Key=k)["TagSet"])
        if ta != tb:
            return {"key": k, "problem": "tags", "a": ta, "b": tb}
        return None

    with cf.ThreadPoolExecutor(workers) as ex:
        for res in ex.map(one_key, common):
            if res:
                problems.append(res)
    return {"objects_a": len(la), "objects_b": len(lb), "missing_in_b": len(missing), "extra_in_b": len(extra),
            "different": len(problems), "samples": {"missing": missing[:10], "extra": extra[:10], "different": problems[:10]}}


def compare_main(argv: List[str]) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--a", required=True)
    p.add_argument("--a-creds", required=True)
    p.add_argument("--b", required=True)
    p.add_argument("--b-creds", required=True)
    p.add_argument("--bucket", default=BUCKET)
    a = p.parse_args(argv)
    rep = compare_buckets(s3(a.a, tuple(a.a_creds.split(":"))), s3(a.b, tuple(a.b_creds.split(":"))), a.bucket)
    print(json.dumps(rep, ensure_ascii=False))
    return 0


# ---------------------------------------------------------------------------
# the stand
# ---------------------------------------------------------------------------


class Stand:
    def __init__(self, objects: int) -> None:
        self.objects = objects
        self.results: Dict[str, Any] = {}
        self.checks: List[Dict[str, Any]] = []
        self.addr = ""
        self.t0 = time.time()

    # -- bookkeeping -------------------------------------------------------
    def check(self, name: str, ok: bool, detail: Any = None) -> bool:
        self.checks.append({"check": name, "ok": bool(ok), "detail": detail})
        print(f"[{time.time() - self.t0:6.0f}s] {'OK  ' if ok else 'FAIL'} {name}" + (f"  {detail}" if detail is not None else ""),
              flush=True)
        return bool(ok)

    def note(self, msg: str) -> None:
        print(f"[{time.time() - self.t0:6.0f}s] .... {msg}", flush=True)

    # -- infrastructure ------------------------------------------------------
    def cleanup(self) -> None:
        for c in (TRACE, NGINX, MINIO, NEW, HOST):
            run("docker", "rm", "-f", "-v", c, check=False)  # -v: the anonymous /data volume of the SeaweedFS image
        run("docker", "volume", "rm", "-f", "pipe-minio-data", check=False)
        run("docker", "network", "rm", NET, check=False)

    def ensure_images(self) -> None:
        dockerfile = textwrap.dedent(f"""\
            FROM python:3.11-slim
            RUN apt-get update && apt-get install -y --no-install-recommends sqlite3 procps curl iproute2 \\
                && rm -rf /var/lib/apt/lists/*
            COPY --from={MC_IMG} /usr/bin/mc /usr/local/bin/mc
            COPY requirements.txt /tmp/requirements.txt
            RUN python -m venv /opt/venv && /opt/venv/bin/pip install -q -r /tmp/requirements.txt
            CMD ["sleep", "infinity"]
            """)
        reqs = (REPO / "requirements.txt").read_bytes()
        self.host_img = f"{HOST_IMG}-{hashlib.sha1(dockerfile.encode() + reqs).hexdigest()[:10]}"  # rebuilt when either changes
        if run("docker", "image", "inspect", self.host_img, check=False).returncode != 0:
            build = OUT / "build"
            build.mkdir(parents=True, exist_ok=True)
            (build / "requirements.txt").write_bytes(reqs)
            (build / "Dockerfile").write_text(dockerfile, encoding="utf-8")
            run("docker", "build", "-q", "-t", self.host_img, str(build))
        for img in (MINIO_IMG, MC_IMG, NGINX_IMG, WEED_IMG):
            if run("docker", "image", "inspect", img, check=False).returncode != 0:
                run("docker", "pull", img)

    def deploy_code(self) -> None:
        """git clone on the server = git archive of the working tree here (tracked files, LF as in git)."""
        rev = run("git", "-C", str(REPO), "stash", "create").stdout.strip() or "HEAD"
        tar = subprocess.run(["git", "-C", str(REPO), "archive", "--format=tar", rev], capture_output=True, check=True).stdout
        subprocess.run(["docker", "exec", "-i", HOST, "tar", "-x", "-C", "/opt/elma365_s3_migrator"], input=tar, check=True)
        run("docker", "cp", str(Path(__file__).resolve()), f"{HOST}:/opt/stand_pipeline.py")
        self.results["code_rev"] = rev

    def start_minio(self, address: str) -> None:
        run("docker", "rm", "-f", MINIO, check=False)
        run("docker", "run", "-d", "--name", MINIO, "--network", f"container:{HOST}", "-v", "pipe-minio-data:/data",
            "-e", f"MINIO_ROOT_USER={ROOT[0]}", "-e", f"MINIO_ROOT_PASSWORD={ROOT[1]}", MINIO_IMG,
            "server", "/data", "--address", address, "--console-address", "127.0.0.1:9001")
        wait(lambda: dx_rc("curl -sf http://127.0.0.1:9000/minio/health/live").returncode == 0, 60, "MinIO up")

    def write_env(self, **over: str) -> None:
        env = {
            "JOURNAL_DB": f"{J}/journal.db", "BULK_DB": f"{J}/bulk.db", "PROGRESS_FILE": f"{J}/bulk-progress.jsonl",
            "GW_LISTEN": "127.0.0.1:9100", "GW_UPSTREAM": "http://127.0.0.1:9000",
            "GW_OTHER_UPSTREAM": "http://127.0.0.1:9000", "GW_BUCKET": BUCKET, "GW_SETTLE_SECONDS": "3",
            "GW_ALLOW_BUCKET_OPS": "", "GW_READONLY_FILE": f"{J}/read-only", "GW_JOURNAL_TIMEOUT": "2",
            "GW_UPSTREAM_TIMEOUT": "3600",
            "SRC_ENDPOINT": "http://127.0.0.1:9000", "SRC_ACCESS_KEY": RO[0], "SRC_SECRET_KEY": RO[1], "SRC_BUCKET": BUCKET,
            "DST_BUCKET": BUCKET, "S3_REGION": "ru-central-1",
            "HANDLER_WORKERS": "4", "GW_LEASE_SECONDS": "90",
            "BULK_WORKERS": "4", "LIST_WORKERS": "1", "LIST_PAGE_SIZE": "2", "SCHEDULE_TZ": "Europe/Moscow",
            "JOURNAL_BACKLOG_MAX": "0", "COPY_TAGS": "1",
            "BULK_MAX_ATTEMPTS": "5", "HANDLER_MAX_ATTEMPTS": "4",  # errors within a minute; outages shorter than that survive
        }
        env.update(over)
        # values with spaces (SCHEDULE) in double quotes: valid for systemd's EnvironmentFile and for sh
        text = "".join(f'{k}="{v}"\n' if re.search(r"[\s;]", v) else f"{k}={v}\n" for k, v in env.items() if v is not None)
        dx("cat > /etc/s3gw/s3gw.env && chmod 600 /etc/s3gw/s3gw.env", user="s3gw", input=text)

    def save_env(self, name: str) -> None:
        dx(f"cp -p /etc/s3gw/s3gw.env /etc/s3gw/{name}", user="s3gw")

    def gateway_up(self) -> bool:
        return dx_rc("curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:9100/minio/health/live").stdout == "200"

    def start_gateway(self, extra_env: str = "") -> None:
        start("gateway", "s3gw.gateway", extra_env)
        wait(self.gateway_up, 30, "gateway health")

    def stop_gateway(self) -> None:
        signal_proc("s3gw.gateway", "TERM")
        wait(lambda: not alive("s3gw.gateway"), 20, "gateway stopped")

    def backlog(self, db: str = "journal.db") -> int:
        return int(one("SELECT count(*) FROM key_state WHERE state IN ('dirty','uncertain','syncing','bulk') "
                       "AND NOT (synced_seq = change_seq AND in_flight = 0)", db) or 0)

    def bulk_status(self) -> Dict[str, Any]:
        return json.loads(s3gw(f"s3gw.bulk status --bulk-db {J}/bulk.db").stdout)

    def compare(self, a: str, a_creds: tuple, b: str, b_creds: tuple) -> Dict[str, Any]:
        out = dx(f"/opt/venv/bin/python /opt/stand_pipeline.py --compare --a {a} --a-creds {':'.join(a_creds)} "
                 f"--b {b} --b-creds {':'.join(b_creds)}", timeout=1800)
        return json.loads(out.strip().splitlines()[-1])

    def pipeline_block(self, start: str, end: str) -> str:
        """Heredoc body from PIPELINE.md: the lines after `start` up to the line `end` (indentation removed)."""
        lines = (REPO / "PIPELINE.md").read_text(encoding="utf-8").split(start, 1)[1].splitlines()
        return textwrap.dedent("\n".join(lines[:[l.strip() for l in lines].index(end)])) + "\n"

    def collect_logs(self) -> None:
        logs = OUT / "logs"
        logs.mkdir(exist_ok=True)
        run("docker", "cp", f"{HOST}:/var/log/s3gw/.", str(logs), check=False)
        for c in (MINIO, NGINX, NEW):
            r = run("docker", "logs", c, check=False)
            (logs / f"{c}.log").write_text(r.stdout + r.stderr, encoding="utf-8")
        run("docker", "cp", f"{NGINX}:/var/log/nginx/.", str(logs / "nginx"), check=False)

    # -- stage 0: today's minio2 -------------------------------------------
    def stage0(self) -> List[str]:
        self.cleanup()
        OUT.mkdir(parents=True, exist_ok=True)
        self.ensure_images()
        run("docker", "network", "create", NET)
        run("docker", "run", "-d", "--name", HOST, "--hostname", "minio2", "--network", NET,
            "-p", f"127.0.0.1:{PORT}:9000", self.host_img)
        self.addr = run("docker", "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", HOST).stdout.strip()
        self.results["minio2_address"] = self.addr
        dx("useradd --system --home /var/lib/s3gw --shell /usr/sbin/nologin s3gw && "
           "install -d -o s3gw -m 700 /var/lib/s3gw /etc/s3gw /backup/s3gw && install -d -o s3gw /var/log/s3gw && "
           "mkdir -p /opt/elma365_s3_migrator")
        self.deploy_code()
        self.start_minio(":9000")  # as now: MinIO on all addresses
        run("docker", "run", "-d", "--name", NGINX, "--network", f"container:{HOST}", NGINX_IMG)
        wait(lambda: http_ok(f"http://127.0.0.1:{PORT}/minio/health/live"), 60, "MinIO on the published port")

        root = s3(f"http://127.0.0.1:{PORT}", ROOT)
        for b in (BUCKET, *OTHER):
            root.create_bucket(Bucket=b)
        keys = [str(uuid.uuid4()) for _ in range(self.objects)]
        big = [f"big/{i}-{uuid.uuid4()}" for i in range(4)]  # > 16 MiB: multipart copy in copykey

        def put(i_k: tuple) -> None:
            i, k = i_k
            size = 17 * 1024 * 1024 + i if k.startswith("big/") else 50 + (i * 7919) % 12000
            extra: Dict[str, Any] = {"ContentType": "application/pdf"} if i % 3 == 0 else {}
            if i % 7 == 0:
                extra.update(Metadata={"orig_name": f"doc{i}.pdf"}, ContentDisposition='attachment; filename="d.pdf"')
            if i % 10 == 0:
                extra["Tagging"] = "status=archived&dept=hr"
            root.put_object(Bucket=BUCKET, Key=k, Body=body(size, i), **extra)

        all_keys = keys + SPECIAL + big
        with cf.ThreadPoolExecutor(16) as ex:
            list(ex.map(put, enumerate(all_keys)))
        root.put_object(Bucket=BUCKET, Key=STABLE, Body=b"stable")
        for b in OTHER:
            for i in range(20):
                root.put_object(Bucket=b, Key=f"old-{i}", Body=b"x" * 100)
        self.results["seeded_objects"] = len(all_keys) + 1
        # ELMA365's own key (readwrite) and the read-only key of A1
        mc("admin", "user", "add", "local", ELMA[0], ELMA[1])
        mc("admin", "policy", "attach", "local", "readwrite", "--user", ELMA[0])
        return all_keys

    # -- stage A ---------------------------------------------------------------
    def stage_a(self, traffic: Traffic) -> None:
        # A0
        ss = dx("ss -Hltn 'sport = :9000'")
        self.check("A0 MinIO listens on all addresses before the switch", "*:9000" in ss or "0.0.0.0:9000" in ss or "[::]:9000" in ss, ss.split())

        # A1: read-only user with exactly the policy from PIPELINE.md
        (OUT / "ro.json").write_text(self.pipeline_block("cat > /tmp/s3gw-src-ro.json <<'EOF'\n", "EOF"), encoding="utf-8")
        mc("admin", "policy", "create", "local", "s3gw-src-ro", "/w/ro.json")
        mc("admin", "user", "add", "local", RO[0], RO[1])
        mc("admin", "policy", "attach", "local", "s3gw-src-ro", "--user", RO[0])
        ro = s3(f"http://127.0.0.1:{PORT}", RO)
        self.check("A1 read-only key lists s3elma365", ro.list_objects_v2(Bucket=BUCKET, MaxKeys=3)["KeyCount"] == 3)
        self.check("A1 read-only key cannot write", try_op(lambda: ro.put_object(Bucket=BUCKET, Key="_ro_check", Body=b"x")) == "403")

        # A2: gateway only
        self.write_env(DST_ENDPOINT=None)
        self.start_gateway()
        ep = sql("SELECT epoch FROM gateway_epochs")
        self.check("A2 gateway up, one epoch, journal owned by s3gw",
                   len(ep) == 1 and "root" not in dx(f"ls -l {J}"), ep)
        rc = s3gw("s3gw.handler --once", check=False)
        self.check("A2 handler refuses to start without DST_ENDPOINT", rc.returncode != 0 and "DST_ENDPOINT" in rc.stderr,
                   rc.stderr.strip().splitlines()[-1:] or rc.returncode)

        # A3: nginx on the check port only; bench through nginx -> gateway -> MinIO
        conf = (REPO / "deploy/nginx/s3gw.conf").read_text(encoding="utf-8").replace("10.10.0.12", self.addr)
        (OUT / "s3gw.conf").write_text(conf, encoding="utf-8", newline="\n")
        run("docker", "cp", str(OUT / "s3gw.conf"), f"{NGINX}:/etc/nginx/conf.d/s3gw.conf")
        r = ngx("nginx -t && nginx -s reload")
        self.check("A3 nginx -t and reload with the repo config", r.returncode == 0, r.stderr.strip().splitlines()[-1:])
        time.sleep(1)
        self.check("A3 check port 127.0.0.1:9080 answers",
                   dx_rc("curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:9080/minio/health/live").stdout == "200")
        bench = dx(f"cd /opt/elma365_s3_migrator && S3_REGION=ru-central-1 SRC_ACCESS_KEY={ROOT[0]} SRC_SECRET_KEY={ROOT[1]} "
                   "/opt/venv/bin/python scripts/bench_gateway_latency.py --direct http://127.0.0.1:9000 "
                   "--gateway http://127.0.0.1:9080 --count 30", check=False)
        self.results["bench"] = bench.strip().splitlines()[-4:]
        bi = {r["k"]: r["n"] for r in sql("SELECT op || ':' || result AS k, count(*) AS n FROM intents "
                                          "WHERE key LIKE 'bench-gw/%/gateway/%' GROUP BY 1")}
        self.check("A3 bench writes journaled, direct ones not", bi == {"put:success": 30, "delete:success": 30}
                   and one("SELECT count(*) FROM intents WHERE key LIKE 'bench-gw/%/direct/%'") == 0, bi)
        self.check("A3 bench left nothing in the bucket", mc("ls", "-r", f"local/{BUCKET}/bench-gw/", check=False).stdout.strip() == "")

        # A4: the switch. MinIO -> 127.0.0.1:9000 (MINIO_OPTS), listen <addr>:9000 in nginx, restart + reload
        enable = f"sed -i 's|^    # listen {self.addr}:9000;|    listen {self.addr}:9000;|' /etc/nginx/conf.d/s3gw.conf"
        ngx(enable)
        self.check("A4 nginx -t with the 9000 listener (port still taken by MinIO)", ngx("nginx -t").returncode == 0)
        t_switch = time.time()
        self.start_minio("127.0.0.1:9000")
        r = ngx("nginx -s reload")
        wait(lambda: http_ok(f"http://127.0.0.1:{PORT}/minio/health/live"), 30, "nginx on minio2:9000")
        self.results["switch_seconds"] = round(time.time() - t_switch, 1)
        ss = dx("ss -Hltn 'sport = :9000'")
        self.check("A4 nginx on <addr>:9000, MinIO only on 127.0.0.1:9000",
                   f"{self.addr}:9000" in ss and "127.0.0.1:9000" in ss and "*:9000" not in ss, ss.split())
        traffic.start()
        time.sleep(8)
        elma = s3(f"http://127.0.0.1:{PORT}", ELMA, xff=True)
        k = "a4-check/" + str(uuid.uuid4())
        data = body(300000, 1)
        elma.put_object(Bucket=BUCKET, Key=k, Body=data)
        got = elma.get_object(Bucket=BUCKET, Key=k)["Body"].read()
        elma.delete_object(Bucket=BUCKET, Key=k)
        ops = {r["op"] + ":" + str(r["result"]): r["n"] for r in sql(
            f"SELECT op, result, count(*) AS n FROM intents WHERE key = '{k}' GROUP BY 1, 2")}
        self.check("A4 ELMA365 put/get/delete through nginx, journaled", got == data and ops == {"put:success": 1, "delete:success": 1}, ops)
        self.check("A4 other bucket goes straight to MinIO (no intents)",
                   one("SELECT count(*) FROM intents WHERE key LIKE 'chunk-%' OR key LIKE 'old-%'") == 0)
        self.check("A4 no 5xx in nginx log under traffic",
                   ngx("wc -l < /var/log/nginx/s3gw-5xx.log 2>/dev/null || echo 0").stdout.strip() in ("0", ""),
                   ngx("tail -3 /var/log/nginx/s3gw-5xx.log 2>/dev/null").stdout.strip()[-300:])

        # ELMA365 calls the random traffic makes rarely or never; parity with the new S3 is checked in C1
        from boto3.s3.transfer import TransferConfig

        sk = ["a4-elma/" + k for k in SPECIAL]
        for k in sk:
            elma.put_object(Bucket=BUCKET, Key=k, Body=k.encode(), ContentType="application/pdf", Tagging="t=v",
                            ContentDisposition='attachment; filename="d.pdf"', Metadata={"orig_name": "x"})
        self.tagged = sk[0]  # its tag is removed in stage B
        got = all(elma.get_object(Bucket=BUCKET, Key=k)["Body"].read() == k.encode() for k in sk)
        meta = elma.head_object(Bucket=BUCKET, Key=sk[0])["Metadata"]
        elma.copy_object(Bucket=BUCKET, Key="a4-elma/copy", CopySource={"Bucket": BUCKET, "Key": sk[5]})
        elma.delete_object(Bucket=BUCKET, Key=sk[1])
        elma.upload_fileobj(io.BytesIO(body(12 * 1024 * 1024 + 7, 2)), BUCKET, "a4-elma/big.bin",
                            Config=TransferConfig(multipart_threshold=5 * 1024 * 1024, multipart_chunksize=5 * 1024 * 1024))
        batch = [f"a4-elma/batch-{i:04d}" for i in range(400)]
        for k in batch[:20]:
            elma.put_object(Bucket=BUCKET, Key=k, Body=b"b")
        errors = elma.delete_objects(Bucket=BUCKET, Delete={"Objects": [{"Key": k} for k in batch], "Quiet": True}).get("Errors")
        left = elma.list_objects_v2(Bucket=BUCKET, Prefix="a4-elma/batch-")["KeyCount"]
        # presigned PUT and GET as a browser sends them: SignedHeaders=host, a long response-content-disposition
        url = elma.generate_presigned_url("put_object", Params={"Bucket": BUCKET, "Key": "a4-elma/p/файл.txt"}, ExpiresIn=600)
        pput = urllib.request.urlopen(urllib.request.Request(url, data=b"presigned", method="PUT",
                                                             headers={"X-Forwarded-For": "10.233.9.9"})).status
        url = elma.generate_presigned_url("get_object", ExpiresIn=600, Params={
            "Bucket": BUCKET, "Key": "a4-elma/p/файл.txt",
            "ResponseContentDisposition": "attachment; filename*=UTF-8''" + urllib.parse.quote("Договор №1 (копия) " * 20)})
        pget = urllib.request.urlopen(url).read()
        ops = {r["k"]: r["n"] for r in sql("SELECT op || ':' || result AS k, count(*) AS n FROM intents "
                                           "WHERE key LIKE 'a4-elma/%' GROUP BY 1")}
        self.check("A4 ELMA365 calls through nginx: special keys, metadata with '_', copy, delete, multipart, "
                   "DeleteObjects on 400 keys, presigned PUT/GET; every change journaled",
                   got and meta == {"orig_name": "x"} and not errors and left == 0 and pput == 200 and pget == b"presigned"
                   and ops == {"put:success": len(sk) + 21, "copy:success": 1, "delete:success": 1, "complete_mpu:success": 1,
                               "delete_batch:success": 400},
                   {"journal": ops, "metadata": meta, "batch_errors": errors, "presigned_get_url_len": len(url)})
        lc = {"Rules": [{"ID": "x", "Status": "Enabled", "Filter": {"Prefix": ""}, "Expiration": {"Days": 1}}]}
        st_put = try_op(lambda: elma.put_bucket_lifecycle_configuration(Bucket=BUCKET, LifecycleConfiguration=lc))
        st_get = try_op(lambda: elma.get_bucket_lifecycle_configuration(Bucket=BUCKET))
        self.check("A4 bucket-level change (lifecycle) refused with 501, MinIO has none", st_put == "501" and st_get == "404",
                   {"put": st_put, "get": st_get})

        # gateway down: writes 502, reads keep working (before C2 reads bypass the gateway)
        traffic.pause()
        self.stop_gateway()
        st_put = try_op(lambda: elma.put_object(Bucket=BUCKET, Key="gw-down", Body=b"x"))
        st_get = try_op(lambda: elma.get_object(Bucket=BUCKET, Key=STABLE)["Body"].read())
        self.start_gateway()
        self.check("A4 gateway down: write 502 and not in MinIO, read 200",
                   st_put == "502" and st_get == "200" and not in_minio("gw-down"),
                   {"put": st_put, "get": st_get})
        # MinIO unreachable from the gateway: write 502, its intent closed as error, not left open
        self.stop_gateway()
        self.start_gateway("GW_UPSTREAM=http://127.0.0.1:9")
        st_put = try_op(lambda: elma.put_object(Bucket=BUCKET, Key="minio-down", Body=b"x"))
        self.stop_gateway()
        self.start_gateway()
        rows = sql("SELECT result, s3_code FROM intents WHERE key = 'minio-down'")
        self.check("A4 MinIO unreachable: write 502, its intents (one per client attempt) closed as error",
                   st_put == "502" and {r["result"] for r in rows} == {"error"} and not in_minio("minio-down"),
                   {"put": st_put, "intents": rows})
        traffic.resume()

        # A5: journal backup by the unit's script, as s3gw
        r = dx_rc(f"JOURNAL_DB={J}/journal.db BACKUP_DIR=/backup/s3gw KEEP=2 sh /opt/elma365_s3_migrator/deploy/systemd/s3gw-journal-backup.sh "
                  f"&& sqlite3 $(ls -1t /backup/s3gw/journal-2*.db | head -1) 'PRAGMA integrity_check'", user="s3gw")
        self.check("A5 journal backup made and intact", r.returncode == 0 and r.stdout.strip().endswith("ok"), r.stderr.strip()[-200:])

        # A6: coverage by trace, with one deliberate bypass from minio2 itself
        trace = self.trace_start("a6")
        time.sleep(12)
        mc("pipe", f"local/{BUCKET}/bypass-a6", input_text="z")
        time.sleep(4)
        tc = self.trace_check(trace, "a6", mark_dirty=True)
        self.check("A6 tracecheck: changes seen, every one via the gateway except the deliberate bypass, none missed",
                   tc["exit"] == 1 and tc["changes"] > 20 and tc["without_headers"] == 0 and tc["bypass_keys"] == ["bypass-a6"]
                   and tc["missed_total"] == 0, {k: tc[k] for k in ("changes", "bypass_total", "missed_total", "exit")})
        self.check("A6 --mark-dirty hands the bypass key to the handler",
                   one("SELECT state FROM key_state WHERE key = 'bypass-a6'") == "dirty")

        # A4 rollback with trace: changes past the gateway go back to the journal
        trace = self.trace_start("rollback")
        time.sleep(2)
        ngx("sed -i 's|default s3gw_gateway;|default s3gw_minio;|' /etc/nginx/conf.d/s3gw.conf && nginx -t && nginx -s reload")
        time.sleep(2)  # old nginx workers finish their requests to the gateway
        n0 = one("SELECT count(*) FROM intents")
        time.sleep(8)
        n1 = one("SELECT count(*) FROM intents")
        ngx("sed -i 's|default s3gw_minio;|default s3gw_gateway;|' /etc/nginx/conf.d/s3gw.conf && nginx -t && nginx -s reload")
        time.sleep(3)
        tc = self.trace_check(trace, "rollback", mark_dirty=True)
        n2 = one("SELECT count(*) FROM intents")
        self.check("A4 rollback: no intents while rolled back, journal resumes after", n1 == n0 and n2 > n1, (n0, n1, n2))
        self.check("A4 rollback: tracecheck --mark-dirty returns the bypassed keys to the journal",
                   tc["bypass_total"] > 0 and tc["missed_total"] == 0 and tc.get("marked_dirty", 0) > 0,
                   {k: tc.get(k) for k in ("bypass_total", "missed_total", "marked_dirty")})
        self.results["rollback_by_call"] = tc["by_call"]

        # A7: listing; kill -9, SIGTERM, a second process, then to the end
        start("list1", "s3gw.bulk list --list-workers 1")
        wait(lambda: self.bulk_status()["ranges"].get("listed", 0) >= 40, 300, "40 ranges listed")
        signal_proc("s3gw.bulk", "9")
        code = wait_exit("list1", 30)
        st1 = self.bulk_status()
        self.check("A7 kill -9 of the lister: exit 137, progress kept", code == 137 and st1["listed_objects"] > 0, st1["ranges"])
        # paused before the start: the lister waits, holding the lock, so a second process meets it
        s3gw(f"s3gw.bulk pause --bulk-db {J}/bulk.db")
        start("list2", "s3gw.bulk list --list-workers 1")
        time.sleep(3)
        second = s3gw("s3gw.bulk list --list-workers 1", check=False)
        self.check("A7 second bulk process on the same bulk.db is refused",
                   alive("s3gw.bulk") and second.returncode != 0 and "another bulk process" in second.stderr,
                   second.stderr.strip()[-120:])
        s3gw(f"s3gw.bulk resume --bulk-db {J}/bulk.db")
        time.sleep(6)  # limits refresh every 5 s
        signal_proc("s3gw.bulk", "TERM")
        code = wait_exit("list2", 60)
        st3 = self.bulk_status()
        self.check("A7 SIGTERM: exit 0, no range left 'listing', checkpoints kept",
                   code == 0 and not st3["ranges"].get("listing") and st3["listed_objects"] >= st1["listed_objects"]
                   and st3["ranges"].get("listed", 0) > st1["ranges"].get("listed", 0), (code, st3["ranges"]))
        # ranges the source refuses to list (one first-hex prefix denied to the read-only key): error, then retry-errors
        prefix = sql("SELECT lo FROM ranges WHERE status = 'pending' AND hi IS NOT NULL ORDER BY id DESC LIMIT 1", "bulk.db")[0]["lo"][0]
        self.deny("s3gw-src-nolist", [{"Effect": "Deny", "Action": ["s3:ListBucket"], "Resource": [f"arn:aws:s3:::{BUCKET}"],
                                       "Condition": {"StringEquals": {"s3:prefix": [prefix]}}}])
        start("list3", "s3gw.bulk list --list-workers 2", "BULK_MAX_ATTEMPTS=2")
        code = wait_exit("list3", 900)
        st4 = self.bulk_status()
        bad = sql("SELECT lo, attempts, last_error FROM ranges WHERE status = 'error'", "bulk.db")
        self.check("A7 ranges the source refuses to list end in error after BULK_MAX_ATTEMPTS, the rest is listed, exit 2",
                   code == 2 and bool(bad) and all(r["lo"].startswith(prefix) and r["attempts"] == 1 for r in bad)
                   and st4["ranges"].get("listed", 0) + len(bad) == 256,
                   {"exit": code, "ranges": st4["ranges"], "error": bad[0]["last_error"][:90] if bad else None})
        exported = s3gw(f"s3gw.bulk export-errors --bulk-db {J}/bulk.db --out /tmp/list-errors.jsonl").stdout.strip()
        self.undeny("s3gw-src-nolist")
        s3gw(f"s3gw.bulk retry-errors --bulk-db {J}/bulk.db")
        start("list4", "s3gw.bulk list --list-workers 2")
        code = wait_exit("list4", 900)
        st4 = self.bulk_status()
        self.results["listing"] = st4
        self.check("A7 export-errors lists the ranges; after retry-errors the listing finishes: exit 0, 256 ranges listed",
                   int(exported) == len(bad) and code == 0 and st4["ranges"] == {"listed": 256}, (exported, st4["ranges"]))
        # completeness: every key in the bucket now is either listed or known to the journal
        traffic.pause()
        now_keys = set(self.list_keys_on_minio2())
        jobs = {r["key"] for r in sql("SELECT key FROM jobs", "bulk.db")}
        journal_keys = {r["key"] for r in sql("SELECT key FROM key_state")}
        lost = sorted(now_keys - jobs - journal_keys)
        dup_pages = one("SELECT sum(listed_count) FROM ranges", "bulk.db") - one("SELECT count(*) FROM jobs", "bulk.db")
        self.check("A7 every object is listed or journaled (nothing lost across kill/restart)", not lost, lost[:5])
        self.check("A7 no object counted twice across restarts", dup_pages == 0, dup_pages)
        traffic.resume()

    def list_keys_on_minio2(self, limit: int = 0) -> List[str]:
        code = ("import boto3,json,sys;from botocore.config import Config;"
                f"c=boto3.client('s3',endpoint_url='http://127.0.0.1:9000',aws_access_key_id='{ROOT[0]}',"
                f"aws_secret_access_key='{ROOT[1]}',region_name='ru-central-1',config=Config(s3={{'addressing_style':'path'}}));"
                f"ks=[o['Key'] for p in c.get_paginator('list_objects_v2').paginate(Bucket='{BUCKET}') for o in p.get('Contents',[])];"
                "print(json.dumps(ks))")
        keys = json.loads(dx(f"/opt/venv/bin/python -c {shlex.quote(code)}"))
        return keys[:limit] if limit else keys

    def trace_start(self, name: str) -> subprocess.Popen:
        run("docker", "rm", "-f", TRACE, check=False)
        fh = open(OUT / f"trace-{name}.raw.jsonl", "w", encoding="utf-8")
        p = subprocess.Popen(["docker", "run", "--rm", "--name", TRACE, "--network", f"container:{HOST}",
                              "-e", f"MC_HOST_local=http://{ROOT[0]}:{ROOT[1]}@127.0.0.1:9000", MC_IMG,
                              "admin", "trace", "--json", "--verbose", "--call", "s3", "local"],
                             stdout=fh, stderr=subprocess.DEVNULL)
        time.sleep(3)
        return p

    def trace_check(self, proc: subprocess.Popen, name: str, mark_dirty: bool) -> Dict[str, Any]:
        run("docker", "rm", "-f", TRACE, check=False)
        proc.wait(20)
        rx = re.compile(r'"method": ?"(GET|HEAD|OPTIONS)"', re.I)  # PIPELINE.md A6 grep
        raw = (OUT / f"trace-{name}.raw.jsonl").read_text(encoding="utf-8", errors="replace").splitlines()
        kept = "\n".join(l for l in raw if not rx.search(l)) + "\n"
        dx(f"cat > /tmp/trace-{name}.jsonl", input=kept)
        r = s3gw(f"s3gw.tracecheck /tmp/trace-{name}.jsonl --journal {J}/journal.db" + (" --mark-dirty" if mark_dirty else ""),
                 check=False)
        out = json.loads(r.stdout)
        out["exit"] = r.returncode
        out["bypass_keys"] = sorted({e["key"] for e in out.get("bypass", [])})
        return out

    # -- stage B ---------------------------------------------------------------
    def start_newstore(self) -> None:
        cfg = {"identities": [
            {"name": "newdst", "credentials": [{"accessKey": DST[0], "secretKey": DST[1]}], "actions": ["Admin", "Read", "Write", "List", "Tagging"]},
            {"name": "elma", "credentials": [{"accessKey": ELMA[0], "secretKey": ELMA[1]}], "actions": ["Read", "Write", "List", "Tagging"]},
            {"name": "newro", "credentials": [{"accessKey": NEWRO[0], "secretKey": NEWRO[1]}], "actions": ["Read", "List", "Tagging"]},
        ]}
        (OUT / "weed-s3.json").write_text(json.dumps(cfg), encoding="utf-8")
        run("docker", "run", "-d", "--name", NEW, "--hostname", "newstore", "--network", NET, "-p", f"127.0.0.1:{NEW_PORT}:8333",
            "-v", f"{(OUT / 'weed-s3.json').as_posix()}:/etc/weed-s3.json:ro", "--entrypoint", "/usr/bin/weed", WEED_IMG,
            "server", "-dir=/data", "-ip=newstore", "-ip.bind=0.0.0.0", "-master.volumeSizeLimitMB=256", "-volume.max=100",
            "-s3", "-s3.port=8333", "-s3.config=/etc/weed-s3.json")
        self.wait_newstore()
        s3(f"http://127.0.0.1:{NEW_PORT}", DST).create_bucket(Bucket=BUCKET)

    def wait_newstore(self) -> None:
        wait(lambda: s3(f"http://127.0.0.1:{NEW_PORT}", DST).list_buckets() is not None, 120, "SeaweedFS S3 up", every=1)

    def done(self) -> int:
        return int(one("SELECT count(*) FROM jobs WHERE state NOT IN ('pending', 'retry', 'claimed')", "bulk.db") or 0)

    def claimed(self) -> int:
        return int(one("SELECT count(*) FROM jobs WHERE state = 'claimed'", "bulk.db") or 0)

    def left(self) -> int:
        return int(one("SELECT count(*) FROM jobs WHERE state IN ('pending', 'retry', 'claimed')", "bulk.db") or 0)

    def idle(self) -> Dict[str, Any]:
        """Copiers stopped: every claim handed back (a copier finishes the job it is in first), nothing finishes."""
        try:
            wait(lambda: self.claimed() == 0, 240, "copiers handed their claims back", every=1)
        except TimeoutError:
            return {"idle": False, "claimed": self.claimed()}
        d0 = self.done()
        time.sleep(6)
        return {"idle": self.done() == d0 and self.claimed() == 0, "finished_in_6s": self.done() - d0}

    def moving(self) -> bool:
        d0 = self.done()
        try:
            wait(lambda: self.done() > d0 or self.left() == 0, 240, "copiers work again", every=1)
            return True
        except TimeoutError:
            return False

    def kill_holding(self, module: str, state: str, timeout: int = 120) -> bool:
        """kill -9 `module` the moment some key is in `state` in the journal (a sync/copy in progress).

        Polls on minio2 itself: a round trip through docker exec is slower than one small copy."""
        q = f"SELECT key FROM key_state WHERE state = '{state}' LIMIT 1"
        r = dx_rc(f'end=$(( $(date +%s) + {timeout} )); while [ $(date +%s) -lt $end ]; do '
                  f'if [ -n "$(sqlite3 -readonly -cmd ".timeout 2000" {J}/journal.db "{q}")" ]; then pkill -9 -f {pattern(module)}; exit 0; fi; done; '
                  f'pkill -9 -f {pattern(module)}; exit 1', user="s3gw")
        return r.returncode == 0

    def deny(self, name: str, statements: List[Dict[str, Any]]) -> None:
        """An extra Deny policy on the read-only key of A1: unreadable keys or prefixes, as a broken permission would."""
        (OUT / f"{name}.json").write_text(json.dumps({"Version": "2012-10-17", "Statement": statements}), encoding="utf-8")
        mc("admin", "policy", "create", "local", name, f"/w/{name}.json")
        mc("admin", "policy", "attach", "local", name, "--user", RO[0])

    def undeny(self, name: str) -> None:
        mc("admin", "policy", "detach", "local", name, "--user", RO[0])

    def untouched_jobs(self, state: str, n: int) -> List[str]:
        """Job keys the journal never saw (bulk owns them), plain UUIDs, from the last ranges."""
        cands = [r["key"] for r in sql(f"SELECT key FROM jobs WHERE state = '{state}' AND key NOT LIKE '%/%' "
                                       "ORDER BY range_id DESC LIMIT 200", "bulk.db")]
        touched = {r["key"] for r in sql("SELECT key FROM key_state WHERE key IN (%s)" % ",".join(f"'{k}'" for k in cands))}
        return [k for k in cands if k not in touched][:n]

    def copied_unchanged(self, n: int) -> List[str]:
        """Keys copied by bulk that ELMA365 has not changed since (change_seq 0 in the journal)."""
        cands = [r["key"] for r in sql("SELECT key FROM jobs WHERE state = 'done' AND key NOT LIKE '%/%' LIMIT 200", "bulk.db")]
        changed = {r["key"] for r in sql("SELECT key FROM key_state WHERE change_seq > 0 AND key IN (%s)"
                                         % ",".join(f"'{k}'" for k in cands))}
        return [k for k in cands if k not in changed][:n]

    def done_bytes(self) -> int:
        return int(one("SELECT coalesce(sum(size), 0) FROM jobs WHERE state = 'done'", "bulk.db") or 0)

    def schedule_check(self) -> None:
        """SCHEDULE from the env file: a window ending in 1-2 minutes (1 copier, 2 Mbit/s), then another (3, 6)."""
        now = dx("/opt/venv/bin/python -c \"from datetime import datetime;from zoneinfo import ZoneInfo;"
                 "n=datetime.now(ZoneInfo('Europe/Moscow'));print(n.hour*60+n.minute, n.second)\"").split()
        minute, second = int(now[0]), int(now[1])

        def hm(m: int) -> str:
            return f"{(m // 60) % 24:02d}:{m % 60:02d}"

        boundary = minute + 2
        sched = f"{hm(minute - 1)}-{hm(boundary)} workers=1 mbps=2; {hm(boundary)}-{hm(minute - 1)} workers=3 mbps=6"
        self.write_env(**self.env_b, SCHEDULE=sched)
        s3gw(f"s3gw.bulk set mbps_max 100000 --bulk-db {J}/bulk.db")  # manual cap off: the schedule alone decides
        t_boundary = time.time() + (120 - second)
        lines0 = int(dx(f"wc -l < {J}/bulk-progress.jsonl", user="s3gw").strip() or 0)
        start("bulk-sched", "s3gw.bulk run")

        def window(until: float) -> Dict[str, Any]:
            owners: set = set()
            t0, d0 = time.time(), self.done_bytes()
            while time.time() < until:
                owners |= {r["owner"] for r in sql("SELECT DISTINCT owner FROM jobs WHERE state = 'claimed'", "bulk.db")}
                time.sleep(0.5)
            secs = time.time() - t0
            return {"copiers": len(owners), "kbytes_per_s": round((self.done_bytes() - d0) / 1024 / secs), "seconds": round(secs)}

        time.sleep(8)  # first refresh of the limits
        w1 = window(t_boundary - 3)
        time.sleep(10)  # the boundary and a refresh (every 5 s)
        w2 = window(time.time() + 40)
        signal_proc("s3gw.bulk", "TERM")
        code = wait_exit("bulk-sched", 60)
        reports = [json.loads(line) for line in dx(f"tail -n +{lines0 + 1} {J}/bulk-progress.jsonl", user="s3gw").splitlines()
                   if line.strip()]
        limits = sorted({(r.get("limit_workers"), r.get("limit_mbps"), r.get("limit_reason")) for r in reports}, key=str)
        self.write_env(**self.env_b)
        self.results["schedule"] = {"SCHEDULE": sched, "window1": w1, "window2": w2, "reported_limits": limits}
        self.check("B SCHEDULE: one copier at 2 Mbit/s in the first window, then 3 copiers at 6 Mbit/s without a restart",
                   w1["copiers"] == 1 and w2["copiers"] >= 2 and (1, 2.0, "schedule") in limits and (3, 6.0, "schedule") in limits
                   and w1["kbytes_per_s"] <= 3 * 256 and code == 0,
                   {"w1": w1, "w2": w2, "limits": limits})

    def stage_b(self, traffic: Traffic) -> None:
        self.start_newstore()
        # B1: handler with the destination, drains what stage A journaled
        self.env_b = {"DST_ENDPOINT": "http://newstore:8333", "DST_ACCESS_KEY": DST[0], "DST_SECRET_KEY": DST[1],
                      "JOURNAL_BACKLOG_MAX": "60"}
        self.write_env(**self.env_b)
        b0 = self.backlog()
        start("handler", "s3gw.handler")
        wait(lambda: self.backlog() < 20, 900, "handler drains stage A backlog", every=2)
        self.check("B1 handler drains the journal of stage A", b0 > 100, {"backlog_before": b0, "now": self.backlog()})

        # tag removal reaches the new S3 (the traffic only sets tags)
        new = s3(f"http://127.0.0.1:{NEW_PORT}", DST)

        def tags() -> List[Dict[str, str]]:
            return new.get_object_tagging(Bucket=BUCKET, Key=self.tagged)["TagSet"]

        wait(tags, 300, "tagged A4 key in the new S3", every=1)
        s3(f"http://127.0.0.1:{PORT}", ELMA, xff=True).delete_object_tagging(Bucket=BUCKET, Key=self.tagged)
        try:
            wait(lambda: not tags(), 120, "tag removal in the new S3", every=1)
        except TimeoutError:
            pass
        left = tags()
        self.check("B tag removal through nginx reaches the new S3", not left, left)

        # Unreadable keys (a permission broken on purpose): two listed objects and one ELMA365 changes now.
        poison = self.untouched_jobs("pending", 2)
        for k in poison:
            traffic.forget(k)
        poison_h = "poison-h/" + str(uuid.uuid4())
        self.deny("s3gw-src-poison", [{"Effect": "Deny", "Action": ["s3:GetObject"], "Resource":
                   [f"arn:aws:s3:::{BUCKET}/{k}" for k in poison] + [f"arn:aws:s3:::{BUCKET}/poison-h/*"]}])
        s3(f"http://127.0.0.1:{PORT}", ELMA, xff=True).put_object(Bucket=BUCKET, Key=poison_h, Body=b"handler cannot read me")
        # a listed object deleted before its copy, past the gateway (the journal does not know): source_missing later
        gone = self.untouched_jobs("pending", 3)[2]
        traffic.forget(gone)
        mc("rm", f"local/{BUCKET}/{gone}")

        # B3: copy by the ready listing, slowed so the failures land mid-copy
        s3gw(f"s3gw.bulk set mbps_max 4 --bulk-db {J}/bulk.db")
        start("bulk1", "s3gw.bulk run")
        wait(lambda: self.done() > 300, 300, "copy started", every=1)
        self.check("B3 run goes straight to copy (listing ready), copiers make progress", self.done() > 300, self.done())
        st = wait(lambda: (lambda s: s if s.get("copiers_last_5m") and s["rate"]["last_5m"]["copied_per_min"] else None)(
            json.loads(dx(f"cat {J}/bulk-status.json", user="s3gw"))), 60, "bulk-status.json with rates", every=2)
        self.results["bulk_status_while_copying"] = st
        self.check("B3 bulk-status.json while copying: progress, rates, ETA, where the copiers' time goes, a bottleneck hint",
                   st["progress"]["jobs_total"] > 0 and st["eta"]["hours"] is not None
                   and st["bottleneck"] in ("mbps_limit", "source", "destination", "balanced", "idle")
                   and st["limits"] == {"workers": 4, "mbps": 4.0, "reason": "schedule"},
                   {"pct": st["progress"]["pct"], "rate_5m": st["rate"]["last_5m"], "copiers": st["copiers_last_5m"],
                    "per_object": st["per_copied_object_last_5m"], "bottleneck": st["bottleneck"], "eta_h": st["eta"]["hours"]})

        # the new S3 down: copy jobs and handler syncs fail, back off; they finish once it is back
        fails = "SELECT coalesce(sum(value), 0) FROM counters WHERE name LIKE 'sync_fail_%'"
        h0 = one(fails)
        traffic.resume()  # the handler needs fresh changes to fail on
        a0 = one("SELECT count(*) FROM jobs WHERE last_error LIKE 'Endpoint%'", "bulk.db")
        run("docker", "stop", "-t", "2", NEW)
        try:
            wait(lambda: (one("SELECT count(*) FROM jobs WHERE last_error LIKE 'Endpoint%'", "bulk.db") or 0) > a0
                 and one(fails) > h0, 180, "failed attempts while the new S3 is down", every=1)
        except TimeoutError:
            pass
        retry_jobs = one("SELECT count(*) FROM jobs WHERE last_error LIKE 'Endpoint%'", "bulk.db") - a0
        errs = [r["last_error"][:120] for r in sql("SELECT last_error FROM jobs WHERE last_error LIKE 'Endpoint%' LIMIT 1", "bulk.db")]
        handler_fails = one(fails) - h0
        down_for = time.time()
        run("docker", "start", NEW)
        self.wait_newstore()
        self.check("B new S3 down: copy jobs and handler syncs fail into retry with backoff", retry_jobs > 0 and handler_fails > 0,
                   {"jobs_failed": retry_jobs, "handler_sync_fail": handler_fails, "error": errs})
        self.results["new_s3_restart_seconds"] = round(time.time() - down_for, 1)

        # B4: pause / resume
        s3gw(f"s3gw.bulk pause --bulk-db {J}/bulk.db")
        paused = self.idle()
        s3gw(f"s3gw.bulk resume --bulk-db {J}/bulk.db")
        self.check("B4 pause stops copying (claims handed back), resume continues", paused["idle"] and self.moving(), paused)

        # journal lag: handler stopped -> backlog > JOURNAL_BACKLOG_MAX -> copy waits
        signal_proc("s3gw.handler", "TERM")
        wait_exit("handler", 60)
        wait(lambda: self.backlog() > 60, 120, "backlog above JOURNAL_BACKLOG_MAX")
        lagged = self.idle()
        lag_backlog = self.backlog()
        start("handler", "s3gw.handler")
        wait(lambda: self.backlog() < 30, 120, "handler catches up")
        self.check("B2 copy waits on journal lag and resumes after the handler catches up",
                   lagged["idle"] and self.moving(), {**lagged, "backlog": lag_backlog})

        # SIGTERM, then the schedule from the env file, across a window boundary
        signal_proc("s3gw.bulk", "TERM")
        code = wait_exit("bulk1", 60)
        self.check("B SIGTERM of bulk: exit 0, no claims left", code == 0 and self.claimed() == 0, (code, self.claimed()))
        self.schedule_check()
        s3gw(f"s3gw.bulk set mbps_max 4 --bulk-db {J}/bulk.db")

        # kill -9 of the copier while it holds a key's lease in the journal (mid-copy)
        start("bulk2", "s3gw.bulk run")
        wait(lambda: self.claimed() > 0, 60, "copiers claim again")
        caught = self.kill_holding("s3gw.bulk", "bulk")
        code = wait_exit("bulk2", 30)
        before = self.done()
        claimed = self.claimed()
        held = sorted(r["key"] for r in sql("SELECT key FROM key_state WHERE state = 'bulk'"))
        self.check("B kill -9 of bulk mid-copy: exit 137, done jobs kept", code == 137 and before > 300,
                   {"done": before, "claimed_at_kill": claimed, "journal_leases_held": len(held), "caught_mid_copy": caught})
        start("bulk3", "s3gw.bulk run")
        self.check("B restart after kill -9: stale claims back to work, copy continues from the checkpoint",
                   self.moving() and self.done() >= before)
        self.results["bulk_kill9_held_keys"] = held
        second = s3gw("s3gw.bulk copy", check=False)
        self.check("B second bulk process refused while run works", second.returncode != 0 and "another bulk process" in second.stderr)

        try:
            wait(lambda: one(f"SELECT state FROM jobs WHERE key = '{gone}'", "bulk.db") not in ("pending", "retry", "claimed"),
                 300, "deleted source object handled")
        except TimeoutError:
            pass
        st_gone = one(f"SELECT state FROM jobs WHERE key = '{gone}'", "bulk.db")
        self.check("B object deleted after listing ends as source_missing, not error", st_gone == "source_missing", (gone, st_gone))

        # gateway failures under the handler (traffic paused: the fault point kills on the first change)
        traffic.pause()
        elma = s3(f"http://127.0.0.1:{PORT}", ELMA, xff=True)
        run("docker", "exec", "-d", "-u", "s3gw", HOST, "/opt/venv/bin/python", "-c",
            f"import sqlite3,time;c=sqlite3.connect('{J}/journal.db',isolation_level=None);"
            "c.execute('BEGIN EXCLUSIVE');time.sleep(6);c.execute('ROLLBACK')")
        time.sleep(1)
        st_put = try_op(lambda: elma.put_object(Bucket=BUCKET, Key="locked-put", Body=b"x"))
        st_get = try_op(lambda: elma.get_object(Bucket=BUCKET, Key=STABLE)["Body"].read())
        time.sleep(6)
        stored = in_minio("locked-put")
        self.check("B journal locked: write 503 and not forwarded, read 200, handler survives",
                   st_put == "503" and st_get == "200" and not stored and alive("s3gw.handler"), {"put": st_put, "get": st_get})
        late = one("SELECT count(*) FROM intents WHERE key = 'locked-put' AND (result IS NULL OR result != 'error')")
        self.check("B late intent of the rejected write is closed as error, not left in flight", late == 0, late)

        for point, key, expect_in_minio in (("after_upstream", "fault-after-upstream", True), ("after_intent", "fault-after-intent", False)):
            self.stop_gateway()
            self.start_gateway(f"S3GW_FAULT_POINT={point}")
            st = try_op(lambda: elma.put_object(Bucket=BUCKET, Key=key, Body=b"payload-" + key.encode()))
            wait(lambda: not alive("s3gw.gateway"), 10, "gateway died at the fault point")
            self.start_gateway()
            wait(lambda: one(f"SELECT state FROM key_state WHERE key = '{key}'") in ("synced", "deleted"), 120,
                 f"{point}: handler settles the uncertain key")
            src_has = in_minio(key)
            dst = s3(f"http://127.0.0.1:{NEW_PORT}", DST)
            dst_has = try_op(lambda: dst.head_object(Bucket=BUCKET, Key=key)) == "200"
            res = one(f"SELECT result FROM intents WHERE key = '{key}' ORDER BY id DESC LIMIT 1")
            self.check(f"B gateway killed {point}: client got an error, intent uncertain, both sides agree",
                       st != "200" and res == "uncertain" and src_has == expect_in_minio and dst_has == src_has,
                       {"client": st, "in_minio": src_has, "in_new": dst_has, "intent": res})

        # handler kill -9 mid-sync: its leases expire (GW_LEASE_SECONDS) and the restarted handler retakes the keys
        traffic.resume()
        caught = self.kill_holding("s3gw.handler", "syncing")
        wait_exit("handler", 30)
        stuck = sorted(r["key"] for r in sql("SELECT key FROM key_state WHERE state = 'syncing'"))
        start("handler", "s3gw.handler")
        self.check("B kill -9 of the handler mid-sync: restarted", caught and alive("s3gw.handler"), {"syncing_at_kill": len(stuck)})
        held = sorted(set(held) | set(stuck))
        bulk_poison = [k for k in poison if k not in held]  # one the killed copier held went to the handler with its lease

        # to the end: copy finishes with the unreadable keys in error (exit 2), the handler drains the rest
        s3gw(f"s3gw.bulk set mbps_max 100000 --bulk-db {J}/bulk.db")
        code = wait_exit("bulk3", 1200)
        stb = self.bulk_status()
        bad_jobs = {r["key"]: r for r in sql("SELECT key, attempts, last_error FROM jobs WHERE state = 'error'", "bulk.db")}
        self.check("B5 unreadable objects: jobs in error after BULK_MAX_ATTEMPTS, the rest finished, exit 2",
                   code == 2 and set(bulk_poison) <= set(bad_jobs)
                   and all(bad_jobs[k]["attempts"] == 5 and "AccessDenied" in bad_jobs[k]["last_error"] for k in bulk_poison)
                   and not any(stb["jobs"].get(s) for s in ("pending", "retry", "claimed")),
                   {"exit": code, "jobs": stb["jobs"], "error_keys": len(bad_jobs), "poison_held": len(poison) - len(bulk_poison),
                    "sample": [bad_jobs[k]["last_error"][:100] for k in bulk_poison if k in bad_jobs][:1]})
        traffic.pause()
        wait(lambda: self.backlog() == 0, 300, "handler backlog 0", every=1)
        herr = sql("SELECT key, attempts, last_error FROM key_state WHERE state = 'error'")
        hrec = one(f"SELECT count(*) FROM errors WHERE component = 'handler' AND kind = 'max_attempts' AND key = '{poison_h}'")
        self.check("B handler: unreadable key in error after HANDLER_MAX_ATTEMPTS, recorded in errors",
                   sorted(r["key"] for r in herr) == sorted([poison_h] + [k for k in poison if k in held])
                   and all(r["attempts"] == 4 for r in herr) and hrec >= 1, herr[:3])

        # B5 repair as in PIPELINE.md: export, fix the cause, retry-errors, start again
        dx(f"cd /opt/elma365_s3_migrator && {PYM} s3gw.bulk export-errors --bulk-db {J}/bulk.db --out {J}/bulk-errors.jsonl", user="s3gw")
        exported = [json.loads(line) for line in dx(f"cat {J}/bulk-errors.jsonl", user="s3gw").splitlines() if line.strip()]
        self.undeny("s3gw-src-poison")
        retried = s3gw(f"s3gw.bulk retry-errors --bulk-db {J}/bulk.db").stdout.strip()
        start("bulk4", "s3gw.bulk run")
        code = wait_exit("bulk4", 600)
        stb = self.bulk_status()
        self.results["bulk_final"] = stb
        self.check("B5 export-errors lists them; after retry-errors the next run copies them: exit 0, no job left",
                   {e["key"] for e in exported if e["kind"] == "job"} == set(bad_jobs) and int(retried) == len(bad_jobs) and code == 0
                   and not any(stb["jobs"].get(s) for s in ("pending", "retry", "claimed", "error"))
                   and all(one(f"SELECT state FROM jobs WHERE key = '{k}'", "bulk.db") == "done" for k in bulk_poison),
                   {"exported": len(exported), "retried": retried, "exit": code, "jobs": stb["jobs"]})
        # keys in error, PIPELINE.md "Сбои и восстановление": the batched UPDATE, literally
        r = dx_rc("J=\"sqlite3 -readonly /var/lib/s3gw/journal.db\"; JW=\"sqlite3 /var/lib/s3gw/journal.db\"; "
                  "MAX=$($J \"SELECT max(rowid) FROM key_state\"); for s in $(seq 0 50000 $MAX); do "
                  "$JW \"UPDATE key_state SET state = 'dirty', attempts = 0, not_before = 0, lease_owner = NULL "
                  "WHERE state = 'error' AND rowid BETWEEN $s AND $((s + 49999))\"; done", user="s3gw")
        wait(lambda: one(f"SELECT state FROM key_state WHERE key = '{poison_h}'") == "synced", 120, "handler retakes the reset key")
        dst = s3(f"http://127.0.0.1:{NEW_PORT}", DST)
        self.check("B handler key in error: reset by the PIPELINE.md command, then synced",
                   r.returncode == 0 and dst.get_object(Bucket=BUCKET, Key=poison_h)["Body"].read() == b"handler cannot read me")

        wait(lambda: self.backlog() == 0, 300, "handler backlog 0", every=1)
        held_states = Counter(r["state"] for r in sql("SELECT state FROM key_state WHERE key IN (%s)" % ",".join(
            "'" + k.replace("'", "''") + "'" for k in held))) if held else Counter()
        self.check("B keys held by killed bulk/handler processes were synced by the handler after the lease",
                   not held or set(held_states) <= {"synced", "deleted"}, dict(held_states))
        errs = sql("SELECT key, last_error FROM key_state WHERE state = 'error'")
        self.check("B no key in error", not errs, errs[:3])

    # -- stage C ---------------------------------------------------------------
    def st5_verify(self, watermark: str) -> Dict[str, Any]:
        """PIPELINE.md C1: the reconcile's verify phase as root with the mc aliases, as on the server."""
        r = dx_rc(f"cd /opt/elma365_s3_migrator && PATH=/opt/venv/bin:$PATH WATERMARK={watermark} "
                  "bash scripts/st5_reconcile_buckets.sh --phase verify > /var/tmp/reconcile.log 2>&1")
        rep = json.loads(dx("cat /opt/elma365_s3_migrator/.runtime/st5-reconcile/report.json"))
        rep["exit"] = r.returncode
        return rep

    def stage_c(self, traffic: Traffic) -> None:
        new_ep, minio_ep = "http://newstore:8333", "http://127.0.0.1:9000"
        # C1 as in PIPELINE.md: st5 verify while ELMA365 works, then its discrepancies re-checked one by one.
        # Three real ones are planted in the new S3 past the journal: a missing object, another size, an extra object.
        traffic.resume()
        k_missing, k_size = self.copied_unchanged(2)
        for k in (k_missing, k_size):
            traffic.forget(k)
        k_extra = "extra-only-in-new/" + str(uuid.uuid4())
        new = s3(f"http://127.0.0.1:{NEW_PORT}", DST)
        new.delete_object(Bucket=BUCKET, Key=k_missing)
        new.put_object(Bucket=BUCKET, Key=k_size, Body=b"truncated")
        new.put_object(Bucket=BUCKET, Key=k_extra, Body=b"nobody wrote me through the gateway")
        planted = {(k_missing, "missing_in_dst"), (k_size, "size"), (k_extra, "extra_in_dst")}
        dx(f"mc alias set minio_old {minio_ep} {RO[0]} {RO[1]} >/dev/null && mc alias set minio_new {new_ep} {NEWRO[0]} {NEWRO[1]} >/dev/null")
        t_reconcile = utc_now()
        v1 = self.st5_verify(t_reconcile)
        traffic.pause()
        dx("cd /opt/elma365_s3_migrator && sqlite3 .runtime/st5-reconcile/state.db "
           "\"SELECT key FROM reconcile_queue UNION SELECT key FROM dst_only\" > /var/tmp/reconcile-keys.txt"
           " && chmod 644 /var/tmp/reconcile-keys.txt")
        nkeys = int(dx("wc -l < /var/tmp/reconcile-keys.txt").strip())
        wait(lambda: self.backlog() == 0, 300, "handler backlog 0", every=1)
        r = s3gw("s3gw.verifykeys --keys-file /var/tmp/reconcile-keys.txt", check=False)
        found = {(m["key"], m["problem"]) for m in json.loads(r.stdout)["mismatches"]}
        self.results["c1_st5_live"] = {**v1, "keys_to_recheck": nkeys}
        self.check("C1 st5 verify on live traffic: discrepancies saved with their keys; the re-check leaves exactly the planted ones",
                   v1["partition_errors"] == 0 and nkeys >= 3 and found == planted,
                   {k: v1.get(k) for k in ("missing_count", "size_mismatch_count", "dst_only_count", "exit")} | {"recheck": sorted(found)})
        r = s3gw("s3gw.verifykeys --keys-file /var/tmp/reconcile-keys.txt --mark-dirty", check=False)
        marked = json.loads(r.stdout).get("marked_dirty")
        wait(lambda: self.backlog() == 0, 300, "handler repairs the marked keys", every=1)
        r = s3gw("s3gw.verifykeys --keys-file /var/tmp/reconcile-keys.txt", check=False)
        self.check("C1 --mark-dirty hands them to the handler: re-check mismatched 0",
                   marked == 3 and r.returncode == 0 and json.loads(r.stdout)["mismatched"] == 0, {"marked_dirty": marked})
        v2 = self.st5_verify(utc_now())
        self.results["c1_st5_quiet"] = v2
        self.check("C1 st5 verify with ELMA365 quiet: missing, size_mismatch, dst_only all 0, exit 0",
                   v2["exit"] == 0 and v2["missing_count"] == v2["size_mismatch_count"] == v2["dst_only_count"] == 0
                   and v2["partition_errors"] == 0, {k: v2.get(k) for k in ("missing_count", "size_mismatch_count", "dst_only_count")})
        rep = self.compare(minio_ep, ROOT, new_ep, DST)
        self.results["c1_compare"] = rep
        self.check("C1 MinIO and the new S3 identical (keys, content, metadata, tags)",
                   rep["missing_in_b"] == rep["extra_in_b"] == rep["different"] == 0, {k: rep[k] for k in list(rep)[:5]})
        traffic.resume()
        time.sleep(5)

        # C0: signature as ELMA365 signs; reverse MinIO user from PIPELINE.md; reverse env
        sig = dx(f"cd /opt/elma365_s3_migrator && ELMA_ACCESS_KEY={ELMA[0]} ELMA_SECRET_KEY={ELMA[1]} "
                 f"/opt/venv/bin/python scripts/check_new_s3_signature.py --endpoint {new_ep}", check=False)
        self.check("C0 new S3 accepts ELMA365's signatures", "all passed" in sig, sig.strip().splitlines()[-3:])
        (OUT / "rev.json").write_text(self.pipeline_block("cat > /tmp/s3gw-dst-minio.json <<'EOF2'\n", "EOF2"), encoding="utf-8")
        mc("admin", "policy", "create", "local", "s3gw-dst-minio", "/w/rev.json")
        mc("admin", "user", "add", "local", REV[0], REV[1])
        mc("admin", "policy", "attach", "local", "s3gw-dst-minio", "--user", REV[0])
        self.save_env("s3gw-forward-base.env")

        # C2.1 write freeze
        dx(f"touch {J}/read-only")
        elma = s3(f"http://127.0.0.1:{PORT}", ELMA, xff=True)
        time.sleep(2)
        st_put = try_op(lambda: elma.put_object(Bucket=BUCKET, Key="frozen", Body=b"x"))
        st_get = try_op(lambda: elma.get_object(Bucket=BUCKET, Key=STABLE)["Body"].read())
        self.check("C2.1 read-only: writes 503, reads 200", st_put == "503" and st_get == "200", (st_put, st_get))
        traffic.pause()
        # C2.2 queue to zero
        wait(lambda: self.backlog() == 0 and one("SELECT count(*) FROM intents WHERE result IS NULL") == 0, 300, "queue zero")
        self.check("C2.2 queue zero, no open intents, no errors", not sql("SELECT key FROM key_state WHERE state = 'error'"))
        # C2.3 verifykeys since the reconcile
        vk = s3gw(f"s3gw.verifykeys --journal {J}/journal.db --since {utc_minus(t_reconcile, 1)}", check=False)
        vko = json.loads(vk.stdout)
        self.check("C2.3 verifykeys after the reconcile: mismatched 0", vk.returncode == 0 and vko["mismatched"] == 0 and vko["checked"] > 0,
                   {"checked": vko["checked"], "mismatched": vko["mismatched"]})
        # C2.4 switch the gateway to the new S3, reverse handler, reads through the gateway
        signal_proc("s3gw.handler", "TERM")
        wait_exit("handler", 60)
        self.save_env("s3gw-forward.env")
        self.write_env(GW_UPSTREAM=new_ep, JOURNAL_DB=f"{J}/journal-reverse.db", SRC_ENDPOINT=new_ep, SRC_ACCESS_KEY=NEWRO[0],
                       SRC_SECRET_KEY=NEWRO[1], DST_ENDPOINT=minio_ep, DST_ACCESS_KEY=REV[0], DST_SECRET_KEY=REV[1])
        self.save_env("s3gw-reverse.env")
        self.stop_gateway()
        self.start_gateway()
        start("handler", "s3gw.handler")
        r = ngx("sed -i -E 's/^(    (GET|HEAD|OPTIONS) +)s3gw_minio;/\\1s3gw_gateway;/' /etc/nginx/conf.d/s3gw.conf && "
                "grep -cE '^ +(GET|HEAD|OPTIONS) +s3gw_minio;' /etc/nginx/conf.d/s3gw.conf; nginx -t && nginx -s reload")
        self.check("C2.4 nginx: reads of s3elma365 now go to the gateway", r.stdout.strip().startswith("0") and r.returncode == 0,
                   r.stdout.strip())
        time.sleep(1)
        probe = "c2-probe-only-in-new"
        s3(f"http://127.0.0.1:{NEW_PORT}", DST).put_object(Bucket=BUCKET, Key=probe, Body=b"from-new")
        got = try_op(lambda: elma.get_object(Bucket=BUCKET, Key=probe)["Body"].read())
        other = try_op(lambda: elma.get_object(Bucket=OTHER[1], Key="old-1")["Body"].read())
        self.check("C2.4 reads come from the new S3, other buckets still from MinIO", got == "200" and other == "200", (got, other))
        s3(f"http://127.0.0.1:{NEW_PORT}", DST).delete_object(Bucket=BUCKET, Key=probe)
        # C2.5 open writes
        dx(f"rm {J}/read-only")
        t_cutover = utc_now()
        after = "after-cutover/" + str(uuid.uuid4())
        elma.put_object(Bucket=BUCKET, Key=after, Body=b"written-after-cutover")
        traffic.resume()
        time.sleep(15)
        ops = {r["k"]: r["n"] for r in sql("SELECT op || ':' || result AS k, count(*) AS n FROM intents GROUP BY 1", "journal-reverse.db")}
        self.check("C2.5 after the cutover changes are journaled in journal-reverse.db, reads are not",
                   ops.get("put:success", 0) > 10 and not any(k.startswith("read:") for k in ops), ops)
        traffic.pause()
        wait(lambda: self.backlog("journal-reverse.db") == 0, 300, "reverse backlog 0")
        rep = self.compare(new_ep, DST, minio_ep, ROOT)
        self.results["c3_compare"] = rep
        self.check("C3 reverse handler keeps MinIO identical to the new S3", rep["missing_in_b"] == rep["extra_in_b"] == rep["different"] == 0,
                   {k: rep[k] for k in list(rep)[:5]})

        # C4 rollback
        traffic.resume()
        time.sleep(5)
        dx(f"touch {J}/read-only")
        time.sleep(2)
        traffic.pause()
        wait(lambda: self.backlog("journal-reverse.db") == 0 and one("SELECT count(*) FROM intents WHERE result IS NULL", "journal-reverse.db") == 0,
             300, "reverse queue zero")
        vk = s3gw(f"s3gw.verifykeys --journal {J}/journal-reverse.db --since {utc_minus(t_cutover, 1)}", check=False)
        vko = json.loads(vk.stdout)
        self.check("C4.3 verifykeys of the reverse journal: mismatched 0", vk.returncode == 0 and vko["mismatched"] == 0 and vko["checked"] > 0,
                   {"checked": vko["checked"], "mismatched": vko["mismatched"]})
        signal_proc("s3gw.handler", "TERM")
        wait_exit("handler", 60)
        dx("cp -p /etc/s3gw/s3gw-forward.env /etc/s3gw/s3gw.env && "
           f"sed -i 's#^JOURNAL_DB=.*#JOURNAL_DB={J}/journal-after-rollback.db#' /etc/s3gw/s3gw.env", user="s3gw")
        self.stop_gateway()
        self.start_gateway()
        ngx("sed -i -E 's/^(    (GET|HEAD|OPTIONS) +)s3gw_gateway;/\\1s3gw_minio;/' /etc/nginx/conf.d/s3gw.conf && nginx -t && nginx -s reload")
        dx(f"rm {J}/read-only")
        time.sleep(1)
        st_after = try_op(lambda: elma.get_object(Bucket=BUCKET, Key=after)["Body"].read())
        k2 = "after-rollback/" + str(uuid.uuid4())
        st_put = try_op(lambda: elma.put_object(Bucket=BUCKET, Key=k2, Body=b"r"))
        journaled = one(f"SELECT result FROM intents WHERE key = '{k2}'", "journal-after-rollback.db")
        stored = in_minio(k2)
        self.check("C4.4 after rollback: file from after the cutover readable from MinIO, new writes journaled into a fresh journal",
                   st_after == "200" and st_put == "200" and journaled == "success" and stored, (st_after, st_put, journaled, stored))

    # -- run -------------------------------------------------------------------
    def run(self) -> int:
        traffic: Optional[Traffic] = None
        ok = True
        try:
            keys = self.stage0()
            self.note(f"stage 0: {len(keys)} objects in MinIO, minio2 at {self.addr}")
            traffic = Traffic(list(keys))
            self.stage_a(traffic)
            self.stage_b(traffic)
            self.stage_c(traffic)
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
    if "--compare" in sys.argv:
        argv = [a for a in sys.argv[1:] if a != "--compare"]
        return compare_main(argv)
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--objects", type=int, default=4000)
    p.add_argument("--keep", action="store_true", help="leave the containers running for inspection")
    a = p.parse_args()
    stand = Stand(a.objects)
    try:
        return stand.run()
    finally:
        if not a.keep:
            stand.cleanup()


if __name__ == "__main__":
    sys.exit(main())
