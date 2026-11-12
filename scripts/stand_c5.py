#!/usr/bin/env python3
"""C5: the proxy on the new server, deploy/nginx/s3-c5.conf, in Docker (PIPELINE.md, C5).

    python scripts/stand_c5.py [--keep]     # needs Docker; ~1 minute

  c5-old    MinIO RELEASE.2025-09-07, the old MinIO on its new address; ELMA365's key is its root
  c5-new    SeaweedFS 4.48, the new S3 with ELMA365's key; elma365-loki created there beforehand
  c5-nginx  nginx 1.28 with the config as it is in the repo (10.10.0.12 and the placeholders -> the stand's addresses)
The host plays ELMA365 through 10.10.0.12:9000 (127.0.0.1:19000). Keys only in the old MinIO (special characters too),
in both (the new S3 must answer) and only in the new S3; reads by GET, HEAD, Range and a presigned URL; writes, deletes,
multipart, listing and another bucket must never reach the old MinIO; every read answered by the old MinIO is in
s3-c5-old-minio.log with its key and no signature; the old MinIO down affects only keys missing in the new S3.
Report: .runtime/stand-c5/report.json
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import traceback
import urllib.request
import uuid
from pathlib import Path
from typing import Any, Dict, List

import stand_pipeline as sp
from stand_pipeline import BUCKET, DST, ELMA, s3, try_op, wait

REPO = Path(__file__).resolve().parents[1]
OUT = REPO / ".runtime" / "stand-c5"
NET, SUBNET = "s3gw-c5", "172.30.5.0/24"
OLD, NEW, NGINX = "c5-old", "c5-new", "c5-nginx"
OLD_ADDR, NEW_ADDR, ADDR = "172.30.5.10", "172.30.5.20", "172.30.5.12"
PORT, OLD_PORT, NEW_PORT = 19000, 19010, 19020
LOG = "/var/log/nginx/s3-c5-old-minio.log"
ONLY_OLD = ["c5/old-1.pdf", "c5/a b+c.pdf", "c5/папка/Договор №1 (копия).docx", "c5/100%.txt", "c5/semi;colon&amp=eq"]
BOTH = ["c5/both-1.pdf", "c5/both a+b.pdf"]
ONLY_NEW = ["c5/new-1.pdf"]


class Stand:
    def __init__(self) -> None:
        self.checks: List[Dict[str, Any]] = []
        self.results: Dict[str, Any] = {}
        self.t0 = time.time()

    def check(self, name: str, ok: bool, detail: Any = None) -> bool:
        self.checks.append({"check": name, "ok": bool(ok), "detail": detail})
        print(f"[{time.time() - self.t0:5.0f}s] {'OK  ' if ok else 'FAIL'} {name}" + (f"  {detail}" if detail is not None else ""), flush=True)
        return bool(ok)

    def cleanup(self) -> None:
        for c in (NGINX, NEW, OLD):
            sp.run("docker", "rm", "-f", "-v", c, check=False)
        sp.run("docker", "network", "rm", NET, check=False)

    def log_lines(self) -> List[Dict[str, Any]]:
        out = sp.run("docker", "exec", NGINX, "sh", "-c", f"cat {LOG} 2>/dev/null", check=False).stdout
        return [json.loads(line) for line in out.splitlines() if line.strip()]

    def up(self) -> None:
        self.cleanup()
        OUT.mkdir(parents=True, exist_ok=True)
        for img in (sp.MINIO_IMG, sp.NGINX_IMG, sp.WEED_IMG):
            if sp.run("docker", "image", "inspect", img, check=False).returncode != 0:
                sp.run("docker", "pull", img)
        sp.run("docker", "network", "create", "--subnet", SUBNET, NET)
        sp.run("docker", "run", "-d", "--name", OLD, "--network", NET, "--ip", OLD_ADDR, "-p", f"127.0.0.1:{OLD_PORT}:9000",
               "-e", f"MINIO_ROOT_USER={ELMA[0]}", "-e", f"MINIO_ROOT_PASSWORD={ELMA[1]}", sp.MINIO_IMG, "server", "/data")
        weed = OUT / "weed-s3.json"
        weed.write_text(json.dumps({"identities": [
            {"name": "admin", "credentials": [{"accessKey": DST[0], "secretKey": DST[1]}], "actions": ["Admin", "Read", "Write", "List", "Tagging"]},
            {"name": "elma", "credentials": [{"accessKey": ELMA[0], "secretKey": ELMA[1]}], "actions": ["Read", "Write", "List", "Tagging"]},
        ]}), encoding="utf-8")
        sp.run("docker", "run", "-d", "--name", NEW, "--network", NET, "--ip", NEW_ADDR, "-p", f"127.0.0.1:{NEW_PORT}:8333",
               "-v", f"{weed.as_posix()}:/etc/weed-s3.json:ro", "--entrypoint", "/usr/bin/weed", sp.WEED_IMG, "server", "-dir=/data",
               f"-ip={NEW_ADDR}", "-ip.bind=0.0.0.0", "-master.volumeSizeLimitMB=64", "-volume.max=20", "-s3", "-s3.port=8333",
               "-s3.config=/etc/weed-s3.json")
        conf = (REPO / "deploy" / "nginx" / "s3-c5.conf").read_text(encoding="utf-8")
        for old, new in (("10.10.0.12", ADDR), ("NEW_S3_HOST:PORT", f"{NEW_ADDR}:8333"), ("OLD_MINIO_HOST", OLD_ADDR)):
            if old not in conf:
                raise RuntimeError(f"s3-c5.conf: no {old!r} (the config changed?)")
            conf = conf.replace(old, new)
        (OUT / "s3-c5.conf").write_text(conf, encoding="utf-8", newline="\n")
        sp.run("docker", "run", "-d", "--name", NGINX, "--network", NET, "--ip", ADDR, "-p", f"127.0.0.1:{PORT}:9000",
               "-v", f"{(OUT / 's3-c5.conf').as_posix()}:/etc/nginx/conf.d/s3-c5.conf:ro", sp.NGINX_IMG)
        self.old = s3(f"http://127.0.0.1:{OLD_PORT}", ELMA)
        self.new = s3(f"http://127.0.0.1:{NEW_PORT}", ELMA)
        self.admin = s3(f"http://127.0.0.1:{NEW_PORT}", DST)
        wait(lambda: try_op(self.old.list_buckets) == "200", 60, "old MinIO up", every=1)
        wait(lambda: try_op(self.admin.list_buckets) == "200", 120, "SeaweedFS up", every=1)
        self.old.create_bucket(Bucket=BUCKET)
        self.admin.create_bucket(Bucket=BUCKET)
        self.admin.create_bucket(Bucket="elma365-loki")  # PIPELINE.md, C5: the other buckets created in the new S3 beforehand
        for k in ONLY_OLD + BOTH:
            self.old.put_object(Bucket=BUCKET, Key=k, Body=f"old:{k}".encode(), ContentType="application/pdf")
        for k in BOTH + ONLY_NEW:
            self.new.put_object(Bucket=BUCKET, Key=k, Body=f"new:{k}".encode())
        out = sp.run("docker", "exec", NGINX, "nginx", "-t", check=False)
        self.check("nginx -t passes with the config of the repo", out.returncode == 0, (out.stdout + out.stderr).strip().splitlines()[-1:])
        self.elma = s3(f"http://127.0.0.1:{PORT}", ELMA, xff=True)

    def get(self, key: str, **kw: Any) -> str:
        try:
            return self.elma.get_object(Bucket=BUCKET, Key=key, **kw)["Body"].read().decode()
        except Exception as exc:  # noqa: BLE001
            return sp.status_of(exc) + ":" + str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))

    def reads(self) -> None:
        got = {k: self.get(k) for k in ONLY_OLD + BOTH + ONLY_NEW}
        self.check("GET: keys missing in the new S3 come from the old MinIO (special characters too: the signature holds), "
                   "keys in the new S3 from it",
                   all(got[k] == f"old:{k}" for k in ONLY_OLD) and all(got[k] == f"new:{k}" for k in BOTH + ONLY_NEW),
                   {k: v for k, v in got.items() if not v.startswith(("old:" if k in ONLY_OLD else "new:"))} or None)
        k = ONLY_OLD[1]
        head = self.elma.head_object(Bucket=BUCKET, Key=k)
        rng = self.elma.get_object(Bucket=BUCKET, Key=k, Range="bytes=0-3")
        url = self.elma.generate_presigned_url("get_object", Params={"Bucket": BUCKET, "Key": k}, ExpiresIn=600)
        with urllib.request.urlopen(url, timeout=10) as r:
            presigned = r.read().decode()
        self.check("HEAD, Range and a presigned GET of a key only in the old MinIO", head["ContentLength"] == len(f"old:{k}")
                   and head["ContentType"] == "application/pdf" and rng["Body"].read() == b"old:" and rng["ResponseMetadata"]["HTTPStatusCode"] == 206
                   and presigned == f"old:{k}", {"head": head["ContentLength"], "range": rng.get("ContentRange")})
        missing = f"c5/nowhere-{uuid.uuid4()}"
        self.check("a key in neither: GET 404 NoSuchKey (the old MinIO's answer), HEAD 404",
                   self.get(missing) == "404:NoSuchKey" and try_op(lambda: self.elma.head_object(Bucket=BUCKET, Key=missing)) == "404",
                   self.get(missing))
        self.missing = missing

    def writes(self) -> None:
        k = f"c5/put-{uuid.uuid4()}"
        put = try_op(lambda: self.elma.put_object(Bucket=BUCKET, Key=k, Body=b"via proxy"))
        gone = ONLY_OLD[0]
        dele = try_op(lambda: self.elma.delete_object(Bucket=BUCKET, Key=gone))
        mk = f"c5/mpu-{uuid.uuid4()}"
        u = self.elma.create_multipart_upload(Bucket=BUCKET, Key=mk)["UploadId"]
        p1 = self.elma.upload_part(Bucket=BUCKET, Key=mk, UploadId=u, PartNumber=1, Body=b"m" * (5 * 2**20))
        self.elma.complete_multipart_upload(Bucket=BUCKET, Key=mk, UploadId=u, MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": p1["ETag"]}]})
        bad = try_op(lambda: self.elma.upload_part(Bucket=BUCKET, Key=mk, UploadId="no-such-upload", PartNumber=1, Body=b"x"))
        other = (try_op(lambda: self.elma.put_object(Bucket="elma365-loki", Key="chunk-1", Body=b"log")),
                 try_op(lambda: self.new.head_object(Bucket="elma365-loki", Key="chunk-1")))
        old_has = {x: try_op(lambda x=x: self.old.head_object(Bucket=BUCKET, Key=x)) for x in (k, mk, gone)}
        new_has = {x: try_op(lambda x=x: self.new.head_object(Bucket=BUCKET, Key=x)) for x in (k, mk)}
        listed = {o["Key"] for o in self.elma.list_objects_v2(Bucket=BUCKET).get("Contents", [])}
        self.check("PUT, multipart, DELETE and another bucket go to the new S3 only; the old MinIO untouched; a 404 of a write "
                   "(NoSuchUpload) passes as it is; the listing is the new S3's",
                   put == "200" and dele == "200" and bad == "404" and other == ("200", "200")
                   and old_has == {k: "404", mk: "404", gone: "200"} and new_has == {k: "200", mk: "200"}
                   and not listed & set(ONLY_OLD) and set(BOTH + ONLY_NEW) <= listed,
                   {"put": put, "delete": dele, "bad_part": bad, "other": other, "old": old_has, "new": new_has})
        self.check("a file deleted in ELMA365 after the reverse handler stopped is served again from the old MinIO "
                   "(PIPELINE.md, C5) and shows in the log", self.get(gone) == f"old:{gone}")
        self.gone = gone

    def logged(self) -> None:
        lines = self.log_lines()
        keys = [x["key"].removeprefix(f"/{BUCKET}/") for x in lines]
        raw = sp.run("docker", "exec", NGINX, "cat", LOG).stdout
        expected = set(ONLY_OLD) | {self.missing, self.gone}
        self.results["log_sample"] = lines[:3]
        self.check("every read answered by the old MinIO is a line of s3-c5-old-minio.log with method, key, both upstream answers "
                   "and the client's status; nothing else is; no signature in it",
                   set(keys) == expected and all(x["method"] in ("GET", "HEAD") for x in lines)
                   and all(x["upstream"].startswith("404 : ") for x in lines) and "Signature" not in raw
                   and {x["status"] for x in lines if x["key"].endswith(self.missing)} == {404},
                   {"lines": len(lines), "unexpected": sorted(set(keys) - expected), "not_logged": sorted(expected - set(keys))})

    def old_down(self) -> None:
        sp.run("docker", "stop", OLD)
        got = (self.get(BOTH[0]), self.get(ONLY_OLD[2]))
        last = self.log_lines()[-1]
        # 502 when the old MinIO refuses the connection, 504 when its address does not answer (a stopped container)
        self.check("the old MinIO down: keys in the new S3 still read, a key only in the old MinIO gets 502 or 504, logged",
                   got[0] == f"new:{BOTH[0]}" and got[1][:3] in ("502", "504") and last["status"] == int(got[1][:3]),
                   {"elma": got, "logged": last})

    def run(self) -> int:
        ok = True
        try:
            self.up()
            self.reads()
            self.writes()
            self.logged()
            self.old_down()
        except Exception as exc:  # noqa: BLE001
            ok = False
            self.check(f"stand aborted: {type(exc).__name__}", False, str(exc)[:500])
            traceback.print_exc()
        finally:
            self.results["checks"] = self.checks
            self.results["seconds"] = round(time.time() - self.t0)
            (OUT / "report.json").write_text(json.dumps(self.results, indent=1, ensure_ascii=False), encoding="utf-8")
        failed = [c["check"] for c in self.checks if not c["ok"]]
        print(f"\n{len(self.checks) - len(failed)}/{len(self.checks)} checks passed in {self.results['seconds']} s; report {OUT / 'report.json'}")
        return 0 if ok and not failed else 1


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--keep", action="store_true", help="leave the containers running")
    a = p.parse_args()
    sys.stdout.reconfigure(encoding="utf-8")
    stand = Stand()
    try:
        return stand.run()
    finally:
        if not a.keep:
            stand.cleanup()


if __name__ == "__main__":
    sys.exit(main())
