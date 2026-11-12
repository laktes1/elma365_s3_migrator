#!/usr/bin/env python3
"""PIPELINE.md word for word, A0 through C4, on a systemd replica of minio2 in Docker.

    python scripts/stand_runbook.py [--objects 1500] [--soak HOURS] [--keep]     # needs Docker (privileged); ~6 minutes

scripts/stand_pipeline.py checks the logic under failures but emulates systemd and nginx. Here the runbook's own bash
blocks run as root, in one operator session (variables and the working directory carry over), on a minio2 that has
what the server has:
  rb-minio2    Debian 12 with systemd: MinIO RELEASE.2025-09-07 as minio.service with /etc/default/minio, nginx 1.28,
               mc, jq, sqlite3, sysstat, python3 with the wheels of requirements.txt (A2's pip install works offline)
  rb-newstore  SeaweedFS 4.48, the new S3
Network 172.30.0.0/24 with minio2 at 172.30.0.12: it replaces 10.10.0.12 in the blocks and in the deployed nginx config.
A block is found by a line of its own text, so a changed runbook fails here instead of drifting. Every other change to a
block (placeholders, `vi`, waits shortened for the stand) is in the report with its reason; steps the runbook gives as
prose (edit s3gw.env, check in ELMA365) are done by the stand and reported as such. The host plays ELMA365 through
minio2:9000, published as 127.0.0.1:39000. Every block must run without shell errors and meet its norm.
Interruptions, the way they come in the field: the A7 listing stopped and restarted; the C1 reconcile killed mid-way and
continued with RESUME=1; cutover.sh switch, undo and rollback killed after each of their steps in turn (CUTOVER_FAULT),
every rerun going on from where the last one died; undo after open refused. minio2 rebooted three times: mid-listing
(A7), at a power cut mid-copy with MinIO up 20 s after the s3gw units (B1), and in the C2 window after switch; what came up
by itself and what the runbook starts again. The journal lost in stage A and restored from the hourly copy by A5.
--soak HOURS: before stage C, ELMA365's traffic for that long against the gateway and the handler, with readers on the
journal and the real hourly backup timer: memory of the gateway and the handler, the WAL, the rotation of the copies.
Report: .runtime/stand-runbook/report.json; each block with its output: blocks.log next to it.
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import hashlib
import json
import re
import subprocess
import sys
import textwrap
import time
import traceback
import uuid
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import stand_pipeline as sp
from stand_pipeline import BUCKET, DST, ELMA, NEWRO, OTHER, ROOT, body, run, s3, try_op, wait

REPO = Path(__file__).resolve().parents[1]
OUT = REPO / ".runtime" / "stand-runbook"
NET, SUBNET = "s3gw-runbook", "172.30.0.0/24"
HOST, NEW = "rb-minio2", "rb-newstore"
ADDR, NEW_ADDR, PROD_ADDR = "172.30.0.12", "172.30.0.20", "10.10.0.12"
PORT, NEW_PORT = 39000, 38333
NEW_EP = f"http://{NEW_ADDR}:8333"
SESSION = "/root/.runbook-session"
BIG = "big/report-2026.pdf"
# cutover.sh steps after which CUTOVER_FAULT kills it: each is interrupted once and resumed by running the command again
SWITCH_STEPS = ("forward-saved", "env-swapped", "gateway-restarted", "handler-started", "reads-switched")
UNITS = ("minio", "nginx", "s3gw-gateway", "s3gw-journal-backup.timer", "s3gw-handler", "s3gw-bulk", "s3gw-list")
UNDO_STEPS = ("reads-back", "env-restored", "gateway-restarted", "handler-started", "archived", "forward-removed")
ROLLBACK_STEPS = ("handler-off", "reads-back", "env-restored", "gateway-restarted", "archived", "forward-removed")
# --soak: one line of numbers a minute; readers on the journal the way monitoring and long reads come in the field
SOAK_SAMPLE = r"""
gw=$(systemctl show -p MainPID --value s3gw-gateway); hd=$(systemctl show -p MainPID --value s3gw-handler)
rss() { awk '/^VmRSS/ {print $2}' "/proc/$1/status" 2>/dev/null || echo 0; }
size() { stat -c %s "$1" 2>/dev/null || echo 0; }
echo "$(date +%s) $gw $(rss "$gw") $hd $(rss "$hd") $(size /var/lib/s3gw/journal.db) $(size /var/lib/s3gw/journal.db-wal)" \
     "$(cat /var/log/nginx/s3gw-5xx.log 2>/dev/null | wc -l) $(ls /backup/s3gw | grep -c '^journal-2.*[.]db$')" \
     "$(ls /backup/s3gw | grep '^journal-2.*[.]db$' | sort | tail -1)"
"""
SOAK_READERS = r"""
J=/var/lib/s3gw/journal.db
cd /opt/elma365_s3_migrator
while :; do sudo -u s3gw env JOURNAL_DB=$J .venv/bin/python -m s3gw.state >/dev/null 2>&1; sleep 5; done &
while :; do sudo -u s3gw sqlite3 -readonly $J "SELECT state, count(*) FROM key_state GROUP BY state" >/dev/null 2>&1; sleep 2; done &
while :; do sleep 600; sudo -u s3gw sqlite3 -readonly $J "BEGIN" "SELECT count(*) FROM intents" ".shell sleep 120" "COMMIT" >/dev/null 2>&1; done &
wait
"""
SHELL_ERRORS = re.compile(r"command not found|No such file or directory|syntax error|unbound variable|Permission denied"
                          r"|invalid option|unrecognized option|Traceback \(most recent call last\)|jq: error|mc: <ERROR>")

MINIO_UNIT = """\
[Unit]
Description=MinIO
Wants=network-online.target
After=network-online.target
AssertFileIsExecutable=/usr/local/bin/minio

[Service]
User=minio-user
Group=minio-user
EnvironmentFile=-/etc/default/minio
ExecStart=/usr/local/bin/minio server $MINIO_OPTS $MINIO_VOLUMES
Restart=always
LimitNOFILE=65536
TimeoutStopSec=infinity
SendSIGKILL=no

[Install]
WantedBy=multi-user.target
"""
MINIO_DEFAULT = f"""\
MINIO_VOLUMES="/mnt/data"
MINIO_OPTS="--address :9000 --console-address :9001"
MINIO_ROOT_USER={ROOT[0]}
MINIO_ROOT_PASSWORD={ROOT[1]}
"""
DOCKERFILE = f"""\
FROM {sp.NGINX_IMG}
RUN apt-get update && apt-get install -y --no-install-recommends systemd systemd-sysv dbus sudo python3 python3-venv \\
      python3-pip sqlite3 jq sysstat curl procps iproute2 openssl ca-certificates \\
    && rm -rf /var/lib/apt/lists/* \\
    && rm -f /var/log/nginx/access.log /var/log/nginx/error.log \\
    && systemctl enable nginx && systemctl mask systemd-modules-load.service
COPY --from={sp.MINIO_IMG} /usr/bin/minio /usr/local/bin/minio
COPY --from={sp.MC_IMG} /usr/bin/mc /usr/local/bin/mc
COPY requirements.txt /opt/requirements.txt
RUN python3 -m pip download -q --no-cache-dir -d /opt/wheels -r /opt/requirements.txt
COPY minio.service /etc/systemd/system/minio.service
COPY minio.default /etc/default/minio
RUN useradd --system --no-create-home --shell /usr/sbin/nologin minio-user && install -d -o minio-user /mnt/data \\
    && systemctl enable minio
ENV PIP_NO_INDEX=1 PIP_FIND_LINKS=/opt/wheels
STOPSIGNAL SIGRTMIN+3
ENTRYPOINT []
CMD ["/lib/systemd/systemd"]
"""


def json_in(out: str) -> Dict[str, Any]:
    """The JSON object a command printed among other lines (jq, tracecheck, verifykeys)."""
    start, end = out.find("{"), out.rfind("}")
    return json.loads(out[start:end + 1]) if 0 <= start < end else {}


class Stand:
    def __init__(self, objects: int, soak_hours: float = 0) -> None:
        self.objects = objects
        self.soak_hours = soak_hours
        self.checks: List[Dict[str, Any]] = []
        self.results: Dict[str, Any] = {"blocks": [], "prose": []}
        self.t0 = time.time()
        text = (REPO / "PIPELINE.md").read_text(encoding="utf-8")
        self.blocks = [textwrap.dedent(b) for b in re.findall(r"```bash\n(.*?)```", text, re.S)]
        self.monitoring = [c.replace("\\|", "|") for c in re.findall(
            r"(?m)^\|[^|]+\| `(.+?)` \|", text.split("# Наблюдение за журналом", 1)[1].split("# Сбои и восстановление", 1)[0])]
        self.log: Any = None

    # -- bookkeeping -------------------------------------------------------
    def check(self, name: str, ok: bool, detail: Any = None) -> bool:
        self.checks.append({"check": name, "ok": bool(ok), "detail": detail})
        print(f"[{time.time() - self.t0:6.0f}s] {'OK  ' if ok else 'FAIL'} {name}" + (f"  {detail}" if detail is not None else ""),
              flush=True)
        return bool(ok)

    def note(self, msg: str) -> None:
        print(f"[{time.time() - self.t0:6.0f}s] .... {msg}", flush=True)

    # -- the operator's shell on minio2 ------------------------------------------
    def find(self, anchor: str, exact: bool = False) -> str:
        hits = [b for b in self.blocks if (b.strip() == anchor if exact else anchor in b)]
        if len(hits) != 1:
            raise RuntimeError(f"PIPELINE.md: {len(hits)} bash blocks with {anchor!r} (the runbook changed?)")
        return hits[0]

    def shell(self, label: str, text: str, timeout: float = 900, errexit: bool = False,
              errors_ok: Sequence[str] = ()) -> Tuple[int, str]:
        """Root bash on minio2 in the operator session; no output line may be a shell error."""
        script = (f"[ -f {SESSION} ] && . {SESSION}\n"
                  f"trap '__rc=$?; set +a; {{ export -p; printf \"cd %q\\n\" \"$PWD\"; }} > {SESSION}; exit $__rc' EXIT\n"
                  "set -a\n" + ("set -e\n" if errexit else "") + text + "\n")
        run("docker", "exec", "-i", HOST, "sh", "-c", "cat > /root/.runbook-block.sh", input=script)
        r = run("docker", "exec", HOST, "bash", "-c", "bash /root/.runbook-block.sh </dev/null 2>&1", check=False, timeout=timeout)
        bad = [line for line in r.stdout.splitlines() if SHELL_ERRORS.search(line) and not any(e in line for e in errors_ok)]
        self.log.write(f"===== {label} (exit {r.returncode})\n{text}\n----- output\n{r.stdout}\n")
        self.log.flush()
        if bad:
            self.check(f"{label}: no shell errors", False, bad[:3])
        return r.returncode, r.stdout

    def block(self, name: str, anchor: str, subs: Sequence[Tuple[str, str, str]] = (), exact: bool = False,
              **kw: Any) -> Tuple[int, str]:
        """The PIPELINE.md block with `anchor`, 10.10.0.12 -> the replica; subs: (old, new, why), each must apply."""
        text = self.find(anchor, exact).replace(PROD_ADDR, ADDR)
        for old, new, why in subs:
            if old not in text:
                raise RuntimeError(f"{name}: {old!r} is not in the block (the runbook changed?)")
            text = text.replace(old, new)
        t = time.time()
        rc, out = self.shell(name, text, **kw)
        self.results["blocks"].append({"block": name, "exit": rc, "seconds": round(time.time() - t, 1),
                                       "changes": [{"from": o, "to": n, "why": w} for o, n, w in subs]})
        return rc, out

    def prose(self, name: str, text: str, **kw: Any) -> Tuple[int, str]:
        """A step PIPELINE.md gives in words; the stand does it the way the words say."""
        self.results["prose"].append({"step": name, "commands": text})
        return self.shell(f"prose: {name}", text, **kw)

    def interrupted(self, label: str, command: str, steps: Sequence[str]) -> None:
        """`command` killed (-9) after each of `steps` in turn: every rerun must go on from where the last one died."""
        codes = [self.shell(f"{label} killed after {s}", f"CUTOVER_FAULT={s} {command}")[0] for s in steps]
        self.check(f"{label}: killed after each step in turn, every rerun went on from there",
                   codes == [137] * len(steps), dict(zip(steps, codes)))

    def var(self, name: str) -> str:
        return run("docker", "exec", HOST, "bash", "-c", f". {SESSION}; printf %s \"${name}\"").stdout

    def sh(self, cmd: str, check: bool = True, timeout: float = 300) -> str:
        """The stand's own look at minio2, outside the session."""
        return run("docker", "exec", HOST, "bash", "-c", cmd, check=check, timeout=timeout).stdout

    def state(self) -> Dict[str, Any]:
        return json.loads(self.sh(
            "systemd-run --wait --pipe -q -p User=s3gw -p EnvironmentFile=/etc/s3gw/s3gw.env "
            "-p WorkingDirectory=/opt/elma365_s3_migrator /opt/elma365_s3_migrator/.venv/bin/python -m s3gw.state"))

    def compare(self) -> Dict[str, Any]:
        out = self.sh(f"/opt/elma365_s3_migrator/.venv/bin/python /root/stand_pipeline.py --compare --a http://127.0.0.1:9000 "
                      f"--a-creds {':'.join(ROOT)} --b {NEW_EP} --b-creds {':'.join(DST)}", timeout=1800 + 3600 * self.soak_hours)
        return json.loads(out.strip().splitlines()[-1])

    # -- infrastructure ------------------------------------------------------------
    def cleanup(self) -> None:
        for c in (HOST, NEW):
            run("docker", "rm", "-f", "-v", c, check=False)
        run("docker", "network", "rm", NET, check=False)

    def image(self) -> str:
        reqs = (REPO / "requirements.txt").read_bytes()
        tag = "s3gw-replica-minio2:" + hashlib.sha1(DOCKERFILE.encode() + MINIO_UNIT.encode() + MINIO_DEFAULT.encode()
                                                     + reqs).hexdigest()[:10]
        if run("docker", "image", "inspect", tag, check=False).returncode != 0:
            build = OUT / "build"
            build.mkdir(parents=True, exist_ok=True)
            (build / "requirements.txt").write_bytes(reqs)
            for name, text in (("Dockerfile", DOCKERFILE), ("minio.service", MINIO_UNIT), ("minio.default", MINIO_DEFAULT)):
                (build / name).write_text(text, encoding="utf-8", newline="\n")
            self.note("building the minio2 replica image (once)")
            run("docker", "build", "-q", "-t", tag, str(build), timeout=1800)
        return tag

    def up(self) -> List[str]:
        self.cleanup()
        OUT.mkdir(parents=True, exist_ok=True)
        tag = self.image()
        if run("docker", "image", "inspect", sp.WEED_IMG, check=False).returncode != 0:
            run("docker", "pull", sp.WEED_IMG)
        run("docker", "network", "create", "--subnet", SUBNET, NET)
        run("docker", "run", "-d", "--name", HOST, "--hostname", "minio2", "--privileged", "--cgroupns=host",
            "-v", "/sys/fs/cgroup:/sys/fs/cgroup:rw", "--tmpfs", "/run", "--tmpfs", "/run/lock",
            "--network", NET, "--ip", ADDR, "-p", f"127.0.0.1:{PORT}:9000", tag)
        booted = wait(lambda: self.sh("systemctl is-system-running", check=False).strip() in ("running", "degraded"), 120, "systemd up")
        failed = self.sh("systemctl --failed --no-legend", check=False).strip()
        wait(lambda: run("docker", "exec", HOST, "curl", "-sf", "http://127.0.0.1:9000/minio/health/live",
                         check=False).returncode == 0, 60, "minio.service up")
        self.check("replica: systemd PID 1, minio.service and nginx.service active, no failed units",
                   booted and not failed and self.sh("systemctl is-active minio nginx").split() == ["active", "active"],
                   failed or self.sh("nginx -v 2>&1; minio --version | head -1").strip().splitlines())

        weed = OUT / "weed-s3.json"
        weed.write_text(json.dumps({"identities": [
            {"name": "newdst", "credentials": [{"accessKey": DST[0], "secretKey": DST[1]}], "actions": ["Admin", "Read", "Write", "List", "Tagging"]},
            {"name": "elma", "credentials": [{"accessKey": ELMA[0], "secretKey": ELMA[1]}], "actions": ["Read", "Write", "List", "Tagging"]},
            {"name": "newro", "credentials": [{"accessKey": NEWRO[0], "secretKey": NEWRO[1]}], "actions": ["Read", "List", "Tagging"]},
        ]}), encoding="utf-8")
        run("docker", "run", "-d", "--name", NEW, "--hostname", "newstore", "--network", NET, "--ip", NEW_ADDR,
            "-p", f"127.0.0.1:{NEW_PORT}:8333", "-v", f"{weed.as_posix()}:/etc/weed-s3.json:ro", "--entrypoint", "/usr/bin/weed",
            sp.WEED_IMG, "server", "-dir=/data", f"-ip={NEW_ADDR}", "-ip.bind=0.0.0.0", "-master.volumeSizeLimitMB=256",
            "-volume.max=100", "-s3", "-s3.port=8333", "-s3.config=/etc/weed-s3.json")
        new = s3(f"http://127.0.0.1:{NEW_PORT}", DST)
        wait(lambda: new.list_buckets() is not None, 120, "SeaweedFS up", every=1)
        new.create_bucket(Bucket=BUCKET)  # task 03: the new S3 is deployed with the bucket

        # Today's minio2: ELMA365's bucket, the other services' buckets, an admin mc alias (A0), ELMA365's own key
        root = s3(f"http://127.0.0.1:{PORT}", ROOT)
        for b in (BUCKET, *OTHER):
            root.create_bucket(Bucket=b)
        keys = [str(uuid.uuid4()) for _ in range(self.objects)]

        def put(i_k: Tuple[int, str]) -> None:
            i, k = i_k
            extra: Dict[str, Any] = {"ContentType": "application/pdf"} if i % 3 == 0 else {}
            if i % 10 == 0:
                extra["Tagging"] = "status=archived"
            root.put_object(Bucket=BUCKET, Key=k, Body=body(50 + (i * 7919) % 12000, i), **extra)

        with cf.ThreadPoolExecutor(16) as ex:
            list(ex.map(put, enumerate(keys)))
        root.put_object(Bucket=BUCKET, Key=BIG, Body=body(20 * 2**20, -1))
        for b in OTHER:
            root.put_object(Bucket=b, Key="old-1", Body=b"x" * 100)
        self.sh(f"mc alias set local http://127.0.0.1:9000 {ROOT[0]} {ROOT[1]} >/dev/null && "
                f"mc admin user add local {ELMA[0]} {ELMA[1]} >/dev/null && mc admin policy attach local readwrite --user {ELMA[0]} >/dev/null")

        # what `git clone` of A2 fetches: the working tree, tracked files (staged new ones too)
        rev = run("git", "-C", str(REPO), "stash", "create").stdout.strip() or "HEAD"
        tar = OUT / "s3gw-src.tar"
        tar.write_bytes(subprocess.run(["git", "-C", str(REPO), "archive", "--format=tar", rev], capture_output=True, check=True).stdout)
        run("docker", "cp", str(tar), f"{HOST}:/root/s3gw-src.tar")
        run("docker", "cp", str(Path(sp.__file__).resolve()), f"{HOST}:/root/stand_pipeline.py")
        self.rev = run("git", "-C", str(REPO), "rev-parse", "--short", rev).stdout.strip()
        self.results["code_rev"] = self.rev
        self.elma = s3(f"http://127.0.0.1:{PORT}", ELMA, xff=True)
        self.new = new
        return keys + [BIG]

    # -- stage A ---------------------------------------------------------------
    def stage_a(self, traffic: sp.Traffic) -> None:
        rc, _ = self.block("definitions J, RUN, B, CUT", 'CUT="bash /opt/elma365_s3_migrator/scripts/cutover.sh"')
        rc, out = self.block("A0 nginx", "grep -E 'worker_(connections|rlimit_nofile)' /etc/nginx/nginx.conf")
        self.check("A0 nginx 1.28, worker_connections shown", "nginx/1.28" in out and "worker_connections" in out, out.strip().splitlines())
        self.prose("A0 worker_connections below 4096: raise it, worker_rlimit_nofile 16384, reload",
                   "sed -i -E 's/worker_connections +[0-9]+;/worker_connections 4096;/' /etc/nginx/nginx.conf\n"
                   "sed -i '1i worker_rlimit_nofile 16384;' /etc/nginx/nginx.conf\nnginx -t && systemctl reload nginx")
        rc, out = self.block("A0 how MinIO runs", "systemctl cat minio | grep -E 'EnvironmentFile|ExecStart'")
        self.check("A0 MinIO from /etc/default/minio, on *:9000", "EnvironmentFile=-/etc/default/minio" in out and "MINIO_OPTS" in out
                   and "*:9000" in out, out.strip().splitlines()[:6])
        rc, out = self.prose("A0 one MinIO node", "mc admin info local --json | jq '.info.servers | length'")
        self.check("A0 mc admin info: one server", out.strip() == "1", out.strip())
        rc, out = self.block("A0 console, disks, fsync", "oflag=dsync")
        fs = re.findall(r"copied, ([0-9.,]+) s", out)
        self.check("A0 console address shown, the file systems of /var/lib, the fsync speed",
                   "console-address" in out and "Filesystem" in out and bool(fs),
                   {"ms_per_fsync": round(float(fs[0].replace(",", ".")) * 1000 / 500, 2) if fs else None})

        rc, out = self.block("A1 read-only user", "mc admin user add $ALIAS s3gw-src")
        self.check("A1 policy and user created, SECRET kept for later steps", rc == 0 and len(self.var("SECRET")) == 40, rc)
        denied = ("Insufficient permissions", "Access Denied")  # mc 2025-08, older mc
        rc, out = self.block("A1 check", 'echo x | mc pipe s3gwro/s3elma365/_ro_check', errors_ok=denied)
        self.check("A1 the key lists the bucket and cannot write", any(d in out for d in denied) and len(
            [line for line in out.splitlines() if line.startswith("[")]) == 3, out.strip().splitlines()[-2:])

        rc, out = self.block("A2 install", "useradd --system --home /var/lib/s3gw", subs=[
            ("git clone https://github.com/laktes1/elma365_s3_migrator /opt/elma365_s3_migrator",
             "mkdir -p /opt/elma365_s3_migrator && tar -x -C /opt/elma365_s3_migrator -f /root/s3gw-src.tar && "
             f"sed -i 's/{PROD_ADDR}/{ADDR}/g' /opt/elma365_s3_migrator/deploy/nginx/s3gw.conf",
             "the working tree under test instead of GitHub; the replica's address in the nginx config"),
            ("git -C /opt/elma365_s3_migrator log -1 --format='%h %s'", f"echo {self.rev}", "the copy has no .git")])
        self.check("A2 user, venv (pip from the wheels), env file, units installed", rc == 0 and "s3gw-gateway.service" in self.sh(
            "ls /etc/systemd/system") and "aiohttp" in self.sh("/opt/elma365_s3_migrator/.venv/bin/pip list 2>/dev/null"), rc)
        self.prose("A2 fill s3gw.env: SRC_* of A1, DST_* commented out; for the stand GW_SETTLE_SECONDS 5, LIST_PAGE_SIZE 20 and "
                   "GW_LEASE_SECONDS 70 (keys leased by the processes of the B1 power cut come back within the stand)",
                   "sed -i -e 's/^SRC_ACCESS_KEY=.*/SRC_ACCESS_KEY=s3gw-src/' -e \"s/^SRC_SECRET_KEY=.*/SRC_SECRET_KEY=$SECRET/\" "
                   "-e 's/^DST_/# DST_/' -e 's/^GW_SETTLE_SECONDS=.*/GW_SETTLE_SECONDS=5/' -e 's/^LIST_PAGE_SIZE=.*/LIST_PAGE_SIZE=20/' "
                   "-e 's/^GW_LEASE_SECONDS=.*/GW_LEASE_SECONDS=70/' /etc/s3gw/s3gw.env")
        self.block("A2 start", "systemctl enable --now s3gw-gateway s3gw-journal-backup.timer")
        rc, out = self.block("A2 check", "systemctl is-active s3gw-gateway s3gw-journal-backup.timer")
        lines = out.splitlines()
        self.check("A2 gateway and backup timer active, handler disabled, 200 through the gateway, epoch 1, first copy",
                   lines[:4] == ["active", "active", "disabled", "200"] and '"epoch": 1' in out and "journal-2" in out
                   and "root" not in self.sh("ls -l /var/lib/s3gw"), lines[:4])

        self.block("A3 nginx on the check port", "cp /opt/elma365_s3_migrator/deploy/nginx/s3gw.conf /etc/nginx/conf.d/s3gw.conf")
        self.check("A3 127.0.0.1:9080 answers through nginx", self.sh(
            "curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:9080/minio/health/live") == "200")
        rc, out = self.block("A3 latency bench", "scripts/bench_gateway_latency.py", subs=[
            ("SRC_ACCESS_KEY=<ключ с записью>", f"SRC_ACCESS_KEY={ROOT[0]}", "MinIO's root key can write"),
            ("SRC_SECRET_KEY=<секрет>", f"SRC_SECRET_KEY={ROOT[1]}", "its secret")])
        self.check("A3 bench through nginx -> gateway -> MinIO", rc == 0, out.strip().splitlines()[-3:])
        rc, out = self.block("A3 bench check", "WHERE key LIKE 'bench-gw/%/gateway/%'")
        self.check("A3 put and delete 100 each journaled, no component errors, nothing left",
                   "put|success|100" in out and "delete|success|100" in out and "[]" in out and out.strip().endswith("0"),
                   out.strip().splitlines())
        rc, out = self.block("A3 large read through nginx", "mc alias set nginxtest http://127.0.0.1:9080",
                             subs=[("K=s3elma365/<крупный объект>", f"K=s3elma365/{BIG}", "a 20 MiB object of the stand")])
        sums = re.findall(r"^([0-9a-f]{32})\s", out, re.M)
        self.check("A3 the same md5 directly and through nginx", len(sums) == 2 and sums[0] == sums[1], sums)

        rc, out = self.block("A4 prepare", "cp /etc/default/minio /etc/default/minio.before-s3gw", subs=[
            ("vi /etc/default/minio", "sed -i 's|^MINIO_OPTS=.*|MINIO_OPTS=\"--address 127.0.0.1:9000 --console-address :9001\"|' "
             "/etc/default/minio", "the edit the comment above it describes")])
        self.check("A4 both listeners in the config, nginx -t passes", f"listen {ADDR}:9000;" in out and "test is successful" in out,
                   [line for line in out.splitlines() if "listen" in line])
        self.block("A4 switch", "systemctl restart minio && systemctl reload nginx")
        # MinIO itself first: a probe through nginx while MinIO restarts would put the stand's own 502 into check 5
        wait(lambda: self.sh("curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:9000/minio/health/live", check=False) == "200",
             60, "MinIO on 127.0.0.1:9000")
        wait(lambda: self.sh(f"curl -s -o /dev/null -w '%{{http_code}}' http://{ADDR}:9000/minio/health/live", check=False) == "200",
             60, "minio2:9000 through nginx")
        ss = self.sh("ss -ltnp | grep ':9000\\b'")
        self.check("A4 check 1: nginx on <addr>:9000, minio on 127.0.0.1:9000", f'{ADDR}:9000' in ss and "nginx" in ss
                   and "127.0.0.1:9000" in ss and "*:9000" not in ss, ss.split())
        k = f"a4-check/{uuid.uuid4()}"
        part = self.elma.get_object(Bucket=BUCKET, Key=BIG, Range="bytes=1048576-1049599")
        st = (try_op(lambda: self.elma.get_object(Bucket=BUCKET, Key=BIG)["Body"].read()),
              try_op(lambda: self.elma.put_object(Bucket=BUCKET, Key=k, Body=b"test")),
              try_op(lambda: self.elma.delete_object(Bucket=BUCKET, Key=k)),
              f"{part['ResponseMetadata']['HTTPStatusCode']}:{len(part['Body'].read())}")
        self.check("A4 checks 2-3 (ELMA365): an existing file opens (whole and a Range of it), a test file uploads and deletes",
                   st == ("200", "200", "200", "206:1024"), st)
        rc, out = self.block("A4 check 4", "SELECT id, key, op, result, http_status, created_at FROM intents ORDER BY id DESC LIMIT 10")
        st4 = json_in(out)
        self.check("A4 check 4: the test file journaled as success, no stuck intents",
                   f"{k}|put|success" in out and f"{k}|delete|success" in out and st4.get("stuck_intents") == 0, st4)
        traffic.start()  # after check 4: under traffic its last 10 intents would be the traffic's
        time.sleep(8)
        rc, out = self.prose("A4 check 5: no errors", "cat /var/log/nginx/s3gw-5xx.log 2>/dev/null | wc -l\n"
                             "cat /var/log/nginx/s3gw-error.log 2>/dev/null | wc -c\n"
                             "journalctl -u s3gw-gateway --since '-10 min' | grep -c -i -E 'error|failed'")
        self.check("A4 check 5: no 5xx, empty nginx error log, no gateway errors", out.split() == ["0", "0", "0"], out.split())
        rc, out = self.prose("A4 check 6: other services write as before", "mc ls $ALIAS/elma365-loki | tail -2")
        self.check("A4 check 6: fresh objects in elma365-loki", "chunk-" in out, out.strip().splitlines())

        # instant rollback with a trace and back, as written; writes of the window come back to the journal
        rc, out = self.block("A4 rollback", "nohup sh -c \"mc admin trace --json --verbose --call s3 $ALIAS")
        self.check("A4 rollback: trace written, writes straight to MinIO", "трасса пишется" in out, out.strip().splitlines()[-1:])
        time.sleep(8)
        rc, out = self.block("A4 return", "sed -i 's|default s3gw_minio;|default s3gw_gateway;|'", subs=[
            ("sleep 60;", "sleep 5;", "requests in flight on old nginx workers end within seconds on the stand")])
        tc = json_in(out)
        self.check("A4 return: tracecheck --mark-dirty hands the bypassed changes to the journal",
                   tc.get("bypass_total", 0) > 0 and tc.get("missed_total") == 0 and tc.get("marked_dirty", 0) > 0 and "exit=1" in out,
                   {k: tc.get(k) for k in ("changes", "bypass_total", "missed_total", "marked_dirty")})
        self.results["not_run"] = {"A4 full return": "it undoes A4"}

        rc, out = self.block("A6 trace", "timeout 3600 mc admin trace --json --verbose --call s3 $ALIAS", subs=[
            ("timeout 3600", "timeout 40", "40 s of the stand's traffic instead of an hour")], timeout=300)
        rc, out = self.block("A6 tracecheck", "s3gw.tracecheck $T --journal /var/lib/s3gw/journal.db; echo")
        tc = json_in(out)
        self.check("A6 coverage: changes seen, all through the gateway, none missed, exit 0",
                   tc.get("changes", 0) > 20 and tc.get("without_headers") == 0 and tc.get("bypass_total") == 0
                   and tc.get("missed_total") == 0 and "exit=0" in out, {k: tc.get(k) for k in ("changes", "bypass_total", "missed_total")})
        self.results["a6_by_call"] = tc.get("by_call")

        # A7: start, look, stop mid-way, restart with a day/night schedule, start over, to the end
        self.block("A7 start", "python -m s3gw.bulk list --list-workers 1")
        wait(lambda: self.sh("test -s /var/lib/s3gw/bulk-status.json && echo y", check=False).strip() == "y", 60, "status file")
        rc, out = self.block("A7 first check", "jq .listing /var/lib/s3gw/bulk-status.json")
        self.check("A7 status file with the listing", '"ranges"' in out, out.strip().splitlines()[:2])
        self.block("A7 stop", "systemctl stop s3gw-list      # текущая страница")
        self.check("A7 stopped: no range left 'listing'", "listing" not in self.sh(
            "sudo -u s3gw sqlite3 -readonly /var/lib/s3gw/bulk.db 'SELECT DISTINCT status FROM ranges'"))
        self.block("A7 restart with a schedule", "--list-workers 4 --schedule '08:00-20:00 workers=1; 20:00-08:00 workers=4'")
        wait(lambda: self.sh("systemctl is-active s3gw-list", check=False).strip() != "active", 600, "listing done", every=2)
        self.a5_restore()
        self.block("A7 start over", "mkdir -p /var/lib/s3gw/old && mv /var/lib/s3gw/bulk.db* /var/lib/s3gw/old/")
        self.block("A7 start again", "python -m s3gw.bulk list --list-workers 1")
        wait(lambda: len(self.listed()) >= 20, 300, "20 ranges listed", every=0.3)
        self.reboot("A7 reboot mid-listing")
        before = self.listed()
        self.block("A7 after the reboot: the A7 start command again", "python -m s3gw.bulk list --list-workers 1")
        wait(lambda: self.sh("systemctl is-active s3gw-list", check=False).strip() != "active", 600, "listing done", every=2)
        after = self.listed()
        self.check("A7 after the reboot the listing went on from its checkpoints: ranges listed before it not listed again",
                   20 <= len(before) < 256 and all(after.get(r) == f for r, f in before.items()),
                   {"listed_before_reboot": len(before), "listed_again": [r for r, f in before.items() if after.get(r) != f][:5]})
        rc, out = self.prose("A7 done: $B status", "$B status")
        st = json_in(out)
        listed = st.get("jobs", {}).get("pending", 0)
        self.check("A7 all 256 ranges listed, every object a job, after stop, restart, starting over and a reboot",
                   st.get("ranges") == {"listed": 256} and listed >= self.objects, {"ranges": st.get("ranges"), "pending": listed})
        rc, out = self.block("A7 the largest objects", "ORDER BY size DESC LIMIT 5")
        sizes = [int(line.split("|", 1)[0]) for line in out.splitlines() if "|" in line]
        self.check("A7 the five largest objects from bulk.db", len(sizes) == 5 and sizes == sorted(sizes, reverse=True), out.splitlines()[:2])

    def a5_restore(self) -> None:
        """A5 word for word: the journal lost in stage A after the listing, restored from the hourly copy; then A7 over."""
        self.prose("A5 the hourly copy (the timer's run at :00)", "systemctl start s3gw-journal-backup")
        copy_epoch = self.state()["epoch"]["epoch"]
        time.sleep(8)  # ELMA365's writes after the copy: their intents are only in the journal that is lost
        rc, out = self.block("A5.1 instant rollback with a trace (A4)", "nohup sh -c \"mc admin trace --json --verbose --call s3 $ALIAS")
        self.check("A5.1 trace written, writes straight to MinIO", "трасса пишется" in out, out.strip().splitlines()[-1:])
        time.sleep(5)
        rc, out = self.block("A5.2 restore from the copy", "BROKEN=/var/lib/s3gw/broken-")
        st = json_in(out)
        broken = self.sh("ls /var/lib/s3gw/broken-*/").split()
        # -wal and -shm only if the gateway did not close cleanly: its last connection checkpoints and removes them
        self.check("A5.2 the lost journal moved aside, the gateway on the copy in a new epoch, the timer running",
                   rc == 0 and st.get("epoch") == copy_epoch + 1 and "journal.db" in broken
                   and self.sh("systemctl is-active s3gw-gateway s3gw-journal-backup.timer", check=False).split() == ["active"] * 2,
                   {"epoch": [copy_epoch, st.get("epoch")], "moved": broken})
        rc, out = self.block("A5.3 writes back to the gateway (A4 return, --mark-dirty)", "sed -i 's|default s3gw_minio;|default s3gw_gateway;|'",
                             subs=[("sleep 60;", "sleep 5;", "requests in flight on old nginx workers end within seconds on the stand")])
        tc = json_in(out)
        self.check("A5.3 tracecheck: the rollback's writes bypassed the gateway, the writes before it are missed (journaled only in "
                   "the lost journal); --mark-dirty hands both to the journal",
                   tc.get("bypass_total", 0) > 0 and tc.get("missed_total", 0) > 0 and tc.get("marked_dirty", 0) > 0 and "exit=1" in out,
                   {k: tc.get(k) for k in ("changes", "bypass_total", "missed_total", "marked_dirty")})
        # what A5.4 starts the inventory over for: keys written between the copy and the trace are in no list
        root = s3(f"http://127.0.0.1:{PORT}", ROOT)
        in_minio = {o["Key"] for page in root.get_paginator("list_objects_v2").paginate(Bucket=BUCKET) for o in page.get("Contents", [])}
        known = set(self.sh("sudo -u s3gw sqlite3 -readonly /var/lib/s3gw/bulk.db 'SELECT key FROM jobs'").splitlines()) | set(
            self.sh("sudo -u s3gw sqlite3 -readonly /var/lib/s3gw/journal.db 'SELECT key FROM key_state'").splitlines())
        gap = sorted(in_minio - known)
        self.results["a5_gap"] = {"keys": len(gap), "sample": gap[:5]}
        self.check("A5.4 keys in MinIO that neither the listing nor the restored journal has: why the inventory starts over",
                   len(gap) > 0, self.results["a5_gap"])

    def listed(self) -> Dict[str, str]:
        """range id -> finished_at of the ranges listed so far."""
        out = self.sh("sudo -u s3gw sqlite3 -readonly /var/lib/s3gw/bulk.db \"SELECT id, finished_at FROM ranges WHERE status = 'listed'\" "
                      "2>/dev/null", check=False)
        return dict(line.split("|", 1) for line in out.split() if "|" in line)

    def copied(self) -> Tuple[float, int]:
        """(share, number) of jobs no longer waiting to be copied."""
        out = self.sh("sudo -u s3gw sqlite3 -readonly /var/lib/s3gw/bulk.db 'SELECT state, n FROM job_counts'", check=False)
        n = {k: int(v) for k, v in (line.split("|") for line in out.split() if "|" in line)}
        total = sum(n.values())
        done = total - sum(n.get(s, 0) for s in ("pending", "retry", "claimed"))
        return (done / total if total else 0.0), done

    def reboot(self, label: str, hard: bool = False, minio_late: int = 0) -> float:
        """minio2 restarted as in the field, then the runbook's look after a boot; returns the seconds it was down.

        hard: a power cut, every process killed and no unit stopped; else a reboot, systemd stops the units first.
        minio_late: MinIO starts that many seconds after everything else (a drop-in for this boot).
        """
        epoch = self.state()["epoch"]["epoch"]
        if minio_late:
            self.sh(f"mkdir -p /etc/systemd/system/minio.service.d && printf '[Service]\\nExecStartPre=/bin/sleep {minio_late}\\n' "
                    "> /etc/systemd/system/minio.service.d/late.conf && systemctl daemon-reload")
        t = time.time()
        if hard:
            run("docker", "kill", "-s", "KILL", HOST)
            run("docker", "start", HOST)
        else:
            run("docker", "restart", "-t", "120", HOST)
        wait(lambda: self.sh("systemctl is-system-running", check=False).strip() in ("running", "degraded"), 300, "minio2 booted", every=1)
        seconds = round(time.time() - t, 1)
        active = dict(zip(UNITS, self.sh("systemctl is-active " + " ".join(UNITS), check=False).split()))
        failed = self.sh("systemctl --failed --no-legend", check=False).strip()
        up_at = dict(zip(UNITS[:5], (int(x or 0) for x in self.sh(
            "systemctl show -p ActiveEnterTimestampMonotonic --value " + " ".join(UNITS[:5]), check=False).split())))
        if minio_late:
            self.sh("rm -r /etc/systemd/system/minio.service.d && systemctl daemon-reload")
        self.results.setdefault("reboots", []).append({"reboot": label, "hard": hard, "minio_late": minio_late, "seconds": seconds,
                                                       "active": active, "failed": failed, "active_at_s": {
                                                           u: round(v / 1e6, 1) for u, v in up_at.items()}})
        self.note(f"{label}: back in {seconds} s, {active}")
        self.block(f"{label}: definitions again (a new session)", 'CUT="bash /opt/elma365_s3_migrator/scripts/cutover.sh"')
        rc, out = self.block(f"{label}: check after the boot", "systemctl is-active minio nginx s3gw-gateway s3gw-journal-backup.timer")
        st = json_in(out)
        self.check(f"{label}: MinIO, nginx, the gateway and the backup timer up by themselves; nginx on <addr>:9000, MinIO on "
                   "127.0.0.1:9000; the gateway in a new epoch",
                   out.splitlines()[:4] == ["active"] * 4 and not failed and f"{ADDR}:9000" in out and "127.0.0.1:9000" in out
                   and (st.get("epoch") or 0) > epoch,
                   {"active": active, "failed": failed or None, "epoch": [epoch, st.get("epoch")], "down_s": seconds})
        return seconds

    # -- stage B ---------------------------------------------------------------
    def stage_b(self) -> None:
        self.prose("B1 uncomment DST_* with the new S3's write key", "sed -i "
                   f"-e 's|^# DST_ENDPOINT=.*|DST_ENDPOINT={NEW_EP}|' -e 's|^# DST_ACCESS_KEY=.*|DST_ACCESS_KEY={DST[0]}|' "
                   f"-e 's|^# DST_SECRET_KEY=.*|DST_SECRET_KEY={DST[1]}|' -e 's|^# DST_BUCKET=|DST_BUCKET=|' /etc/s3gw/s3gw.env")
        self.prose("B1 the stand slows the copy to 1 MB/s (B2's control, kept in bulk.db) so that the power cut lands mid-way",
                   "$B set mbps_max 8")
        self.block("B1 start", "systemctl enable --now s3gw-handler && systemctl start s3gw-bulk")
        rc, out = self.block("B1 first check", "$RUN s3gw.state | jq '{backlog, backlog_oldest, error_keys}'")
        self.check("B1 state and limits readable; copy limited by schedule or journal_lag",
                   '"error_keys": 0' in out and ('"journal_lag"' in out or '"schedule"' in out), out.split())
        self.block("B1 watching", "cat /var/lib/s3gw/bulk-status.json", subs=[
            ("journalctl -u s3gw-bulk -f", "journalctl -u s3gw-bulk -n 20 --no-pager", "-f follows forever")])
        wait(lambda: self.copied()[0] >= 0.1, 300, "a tenth copied", every=0.5)
        share, done = self.copied()
        self.reboot("B1 power cut mid-copy, MinIO up 20 s after the rest", hard=True, minio_late=20)
        rb = self.results["reboots"][-1]
        self.check("B1 after the power cut: the handler up by itself (enabled in B1), before MinIO; the copy is not",
                   rb["active"].get("s3gw-handler") == "active" and rb["active"].get("s3gw-bulk") == "inactive"
                   and 0 < rb["active_at_s"]["s3gw-handler"] < rb["active_at_s"]["minio"], rb["active_at_s"])
        self.prose("B1 after the reboot: the copy started again (Сбои и восстановление, «Перезагрузка minio2»)", "systemctl start s3gw-bulk")
        time.sleep(12)  # the status file is rewritten every 10 s
        lim = json.loads(self.sh("jq -c .limits /var/lib/s3gw/bulk-status.json"))
        share2, done2 = self.copied()
        self.check("B1 the copy went on from bulk.db (nothing copied before the cut is pending again), the manual limit kept",
                   0 < share < 1 and done2 >= done and lim.get("mbps") == 8, {"copied_at_cut": f"{share:.0%}", "done": [done, done2], "limits": lim})
        self.block("B2 control", "$B set workers_max 4")
        wait(lambda: self.sh("systemctl is-active s3gw-bulk", check=False).strip() != "active", 900, "copy done", every=2)
        self.check("B1 copy finished: unit exited 0", self.sh("systemctl show -p ExecMainStatus --value s3gw-bulk").strip() == "0")
        rc, out = self.block("B3 errors", "$B export-errors --out /var/lib/s3gw/bulk-errors.jsonl")
        wait(lambda: self.sh("systemctl is-active s3gw-bulk", check=False).strip() != "active", 120, "copy rerun done", every=1)
        self.check("B3 export-errors 0, retry-errors 0, the rerun exits 0", out.split()[:2] == ["0", "0"]
                   and self.sh("systemctl show -p ExecMainStatus --value s3gw-bulk").strip() == "0", out.split()[:2])
        rc, out = self.prose("B4 done: $B status", "$B status")
        jobs = json_in(out).get("jobs", {})
        self.check("B4 no job pending, claimed, retry or error", not any(jobs.get(s) for s in ("pending", "claimed", "retry", "error")), jobs)
        rc, out = self.block("B4 incomplete uploads", "mc ls --incomplete --recursive s3gwdst/s3elma365 | wc -l")
        self.check("B4 cleanup of incomplete uploads runs with the env file sourced in bash", rc == 0, out.strip().splitlines())

    # -- soak ------------------------------------------------------------------
    def soak(self) -> None:
        """ELMA365's traffic for --soak hours against the gateway and the handler, with readers on the journal and the hourly
        backup timer; one sample a minute. The traffic is ~100 times ELMA365's real write rate."""
        for name, text in (("soak-sample.sh", SOAK_SAMPLE), ("soak-readers.sh", SOAK_READERS)):
            run("docker", "exec", "-i", HOST, "sh", "-c", f"cat > /root/{name}", input=text)
        run("docker", "exec", "-d", HOST, "bash", "/root/soak-readers.sh")
        self.note(f"soak: {self.soak_hours} h of traffic, readers on the journal, the backup timer as it is")
        cols = ("t", "gw_pid", "gw_rss_kb", "hd_pid", "hd_rss_kb", "db", "wal", "n5xx", "copies", "newest")
        samples: List[Dict[str, Any]] = []
        backlog: List[Tuple[int, int]] = []
        end = time.time() + self.soak_hours * 3600
        while True:
            vals = self.sh("bash /root/soak-sample.sh", check=False).split()
            if len(vals) >= 9:
                samples.append(dict(zip(cols, [int(v) for v in vals[:9]] + vals[9:10])))
            if len(samples) % 5 == 1:
                backlog.append((int(time.time()), self.state()["backlog"]))
            if time.time() >= end:
                break
            time.sleep(60)
        self.sh("pkill -f soak-readers.sh", check=False)
        hour = 60  # samples
        first = [x for x in samples[10:10 + hour]]  # after 10 min of warm-up
        last = samples[-hour:]
        newest = sorted({x["newest"] for x in samples if x.get("newest")})
        mb = lambda kb: round(kb / 1024, 1)  # noqa: E731
        res = {
            "hours": self.soak_hours, "samples": len(samples),
            "gateway_rss_mb": {"first_hour_max": mb(max(x["gw_rss_kb"] for x in first)), "last_hour_max": mb(max(x["gw_rss_kb"] for x in last))},
            "handler_rss_mb": {"first_hour_max": mb(max(x["hd_rss_kb"] for x in first)), "last_hour_max": mb(max(x["hd_rss_kb"] for x in last))},
            "wal_mb": {"first_hour_max": round(max(x["wal"] for x in first) / 2**20, 1), "last_hour_max": round(max(x["wal"] for x in last) / 2**20, 1)},
            "journal_mb": [round(samples[0]["db"] / 2**20, 1), round(samples[-1]["db"] / 2**20, 1)],
            "copies_seen": newest, "copies_at_end": samples[-1]["copies"],
            "nginx_5xx": samples[-1]["n5xx"] - samples[0]["n5xx"],
            "pids": sorted({(x["gw_pid"], x["hd_pid"]) for x in samples}),
            "backlog_max": max(b for _t, b in backlog), "backlog_end": backlog[-1][1],
            "every_10_min": [{"min": round((x["t"] - samples[0]["t"]) / 60), "gw_rss_mb": mb(x["gw_rss_kb"]), "hd_rss_mb": mb(x["hd_rss_kb"]),
                              "wal_mb": round(x["wal"] / 2**20, 1)} for x in samples[::10]],
        }
        self.results["soak"] = res

        def flat(r: Dict[str, float]) -> bool:  # no leak: the last hour within 20 % (and 20 MB) of the first
            return r["last_hour_max"] <= max(r["first_hour_max"] * 1.2, r["first_hour_max"] + 20)

        self.check("soak: the gateway and the handler ran the whole time without a restart, their memory flat",
                   len(res["pids"]) == 1 and flat(res["gateway_rss_mb"]) and flat(res["handler_rss_mb"]),
                   {k: res[k] for k in ("gateway_rss_mb", "handler_rss_mb", "pids")})
        self.check("soak: the WAL stays bounded with readers on the journal (a 2-minute read every 10 minutes)",
                   res["wal_mb"]["last_hour_max"] <= max(res["wal_mb"]["first_hour_max"] * 1.5, 64), res["wal_mb"])
        self.check("soak: the hourly timer made a copy every hour, KEEP=2 kept two",
                   len(newest) >= int(self.soak_hours) and res["copies_at_end"] == 2, {"copies_seen": newest, "at_end": res["copies_at_end"]})
        self.check("soak: no 5xx through nginx, the handler kept up", res["nginx_5xx"] == 0 and res["backlog_end"] < 500,
                   {k: res[k] for k in ("nginx_5xx", "backlog_max", "backlog_end")})

    # -- stage C ---------------------------------------------------------------
    def stage_c(self, traffic: sp.Traffic) -> None:
        rc, out = self.block("C0.1 signature", "scripts/check_new_s3_signature.py --endpoint", subs=[
            ("ELMA_ACCESS_KEY=<ключ ELMA365>", f"ELMA_ACCESS_KEY={ELMA[0]}", "ELMA365's key, also in the new S3"),
            ("ELMA_SECRET_KEY=<секрет>", f"ELMA_SECRET_KEY={ELMA[1]}", "its secret"),
            ("--endpoint https://<новое S3>:<порт>", f"--endpoint {NEW_EP}", "the new S3 of the stand")])
        self.check("C0.1 the new S3 accepts ELMA365's signatures", "all passed" in out, out.strip().splitlines()[-1:])
        rc, out = self.block("C0.2 MinIO user for the reverse direction", "mc admin policy create local s3gw-dst-minio /tmp/s3gw-dst-minio.json")
        self.check("C0.2 user created, SECRET2 kept", rc == 0 and len(self.var("SECRET2")) == 40, rc)
        self.block("C0.3 overrides", "deploy/systemd/s3gw-reverse.overrides.example s3gw-reverse.overrides", subs=[
            ("vi s3gw-reverse.overrides",
             f"sed -i -e 's|https://NEW_S3_HOST:PORT|{NEW_EP}|' -e 's|^SRC_ACCESS_KEY=.*|SRC_ACCESS_KEY={NEWRO[0]}|' "
             f"-e 's|^SRC_SECRET_KEY=.*|SRC_SECRET_KEY={NEWRO[1]}|' -e \"s|^DST_SECRET_KEY=.*|DST_SECRET_KEY=$SECRET2|\" "
             "s3gw-reverse.overrides", "the edit the comment describes")])
        rc, out = self.block("C0.3 build", "paste -sd'|'")
        changed = sorted({m for m in re.findall(r"^[<>] ([A-Z_]+)=", out, re.M)})
        self.check("C0.3 s3gw-reverse.env differs from s3gw.env in exactly the override lines",
                   changed == ["DST_ACCESS_KEY", "DST_ENDPOINT", "DST_SECRET_KEY", "GW_UPSTREAM", "JOURNAL_DB", "SRC_ACCESS_KEY",
                               "SRC_ENDPOINT", "SRC_SECRET_KEY"], changed)
        rc, out = self.block("C0.3 GW_READONLY_FILE", "grep -c '^GW_READONLY_FILE=/var/lib/s3gw/read-only$'")
        self.check("C0.3 GW_READONLY_FILE in both configs", re.findall(r":(\d)$", out, re.M) == ["1", "1"], out.split())

        rc, out = self.block("C1 reconcile", "T_RECONCILE=$(date -u +%FT%TZ)", subs=[
            ("mc alias set minio_new https://<новое S3>:<порт> <ключ чтения нового S3> <секрет>",
             f"mc alias set minio_new {NEW_EP} {NEWRO[0]} {NEWRO[1]}", "the new S3 and its read key")])
        # killed mid-way (as by a reboot), then continued by the runbook's RESUME=1 block
        st5 = "/opt/elma365_s3_migrator/.runtime/st5-reconcile"
        done_sql = f"sqlite3 -readonly {st5}/state.db \"SELECT count(*) FROM partition_progress WHERE status = 'done'\" 2>/dev/null"
        wait(lambda: int(self.sh(done_sql, check=False).strip() or 0) >= 30, 600, "30 partitions reconciled", every=0.5)
        # [r]: the pattern must not match this pkill's own shell, whose command line holds it
        self.sh("pkill -9 -f '[r]econcile_buckets.py'; pkill -9 -f '[s]t5_reconcile_buckets.sh'", check=False)
        wait(lambda: run("docker", "exec", HOST, "pgrep", "-f", "reconcile_buckets", check=False).returncode != 0, 30, "reconcile dead")
        at_kill = int(self.sh(done_sql).strip())
        self.block("C1 reconcile resumed", "RESUME=1 \\")
        time.sleep(2)
        wait(lambda: run("docker", "exec", HOST, "pgrep", "-f", "st5_reconcile_buckets.sh", check=False).returncode != 0, 1200,
             "reconcile done", every=3)
        resumed = re.findall(r"resume: (\d+) partitions done, (\d+) to list", self.sh("cat /var/tmp/reconcile.log"))
        events = Counter(json.loads(line)["partition"] for line in self.sh(f"cat {st5}/progress.jsonl").splitlines()
                         if '"partition_done"' in line)
        parts = int(self.sh(f"sqlite3 -readonly {st5}/state.db 'SELECT count(*) FROM partition_progress'").strip())
        self.check("C1 reconcile killed mid-way and resumed: finished partitions not listed again, every partition done once",
                   len(resumed) == 1 and int(resumed[0][0]) >= at_kill >= 30 and sum(map(int, resumed[0])) == parts
                   and int(self.sh(done_sql).strip()) == parts and max(events.values()) == 1,
                   {"done_at_kill": at_kill, "resume": resumed, "partitions": parts, "listed_twice": [p for p, n in events.items() if n > 1]})
        rep = json.loads(self.sh("cat /opt/elma365_s3_migrator/.runtime/st5-reconcile/report.json"))
        self.check("C1 reconcile on live traffic: no partition errors", rep.get("partition_errors") == 0,
                   {k: rep.get(k) for k in ("missing_count", "size_mismatch_count", "dst_only_count", "partition_errors")})
        traffic.pause()  # the last verifykeys is a snapshot: ELMA365 quiet for it, as in the window
        rc, out = self.block("C1 re-check", "SELECT key FROM reconcile_queue UNION SELECT key FROM dst_only", timeout=1500)
        mism = [int(n) for n in re.findall(r'"mismatched": (\d+)', out)]
        self.check("C1 re-check: drained twice, the last verifykeys mismatched 0",
                   re.findall(r"^(true|false)$", out, re.M) == ["true", "true"] and mism[-1:] == [0], {"mismatched": mism})

        # C2 with the script. First attempt: switch and undo each killed after every step in turn, resumed by rerunning;
        # then the window again to the end
        self.block("C2 JR", 'JR="sudo -u s3gw sqlite3 -readonly /var/lib/s3gw/journal-reverse.db"')
        self.window_to_switch("C2 (first attempt)", faults=SWITCH_STEPS)
        self.interrupted("C2 undo", "$CUT undo", UNDO_STEPS)
        rc, out = self.block("C2 undo", "$CUT undo", exact=True)
        k = f"after-undo/{uuid.uuid4()}"
        st = try_op(lambda: self.elma.put_object(Bucket=BUCKET, Key=k, Body=b"u"))
        self.new.put_object(Bucket=BUCKET, Key="only-in-new", Body=b"n")
        got = try_op(lambda: self.elma.get_object(Bucket=BUCKET, Key="only-in-new")["Body"].read())
        self.new.delete_object(Bucket=BUCKET, Key="only-in-new")
        archived = self.sh("ls /var/lib/s3gw | grep -c 'journal-reverse.db.undone-' || true").strip()
        self.check("C2 undo: writes journaled in journal.db again, reads from MinIO, unused reverse journal archived",
                   rc == 0 and st == "200" and got == "404" and self.state()["journal"] == "/var/lib/s3gw/journal.db"
                   and archived != "0", {"exit": rc, "write": st, "read_only_in_new": got, "archived_files": archived})
        t_window = time.time()
        self.window_to_switch("C2")
        t_reboot = time.time()
        self.reboot("C2 reboot in the window after switch")
        self.block("C2 JR again (a new session)", 'JR="sudo -u s3gw sqlite3 -readonly /var/lib/s3gw/journal-reverse.db"')
        rb = self.results["reboots"][-1]
        reads = (try_op(lambda: self.elma.get_object(Bucket=BUCKET, Key=BIG)["Body"].read()),
                 try_op(lambda: self.elma.put_object(Bucket=BUCKET, Key="frozen", Body=b"x")))
        other = self.sh(f"curl -sI http://{ADDR}:9000/elma365-loki/ | grep -ci '^x-minio-' || true").strip()
        self.check("C2 after the reboot the window holds: the reverse journal, writes still 503, reads from the new S3, other buckets "
                   "from MinIO, the reverse handler up by itself, the copy not",
                   self.state()["journal"] == "/var/lib/s3gw/journal-reverse.db" and reads == ("200", "503") and other != "0"
                   and rb["active"].get("s3gw-handler") == "active" and rb["active"].get("s3gw-bulk") == "inactive",
                   {"elma": reads, "x_minio_headers": other, "active": rb["active"]})
        rebooted = time.time() - t_reboot
        rc, out = self.block("C2.5 open", "$CUT open")
        # C0.4: the window, freeze to open, without the reboot the stand put into it
        self.results["c2_window_seconds"] = round(time.time() - t_window - rebooted, 1)
        t_cut = re.findall(r"^(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ)$", out, re.M)
        self.prose("C2.5 write T_CUTOVER down", f"T_CUTOVER={t_cut[-1] if t_cut else ''}")
        k = f"after-cutover/{uuid.uuid4()}"
        st = (try_op(lambda: self.elma.put_object(Bucket=BUCKET, Key=k, Body=b"after")),
              try_op(lambda: self.elma.put_object(Bucket=BUCKET, Key=k + "-del", Body=b"d")),
              try_op(lambda: self.elma.delete_object(Bucket=BUCKET, Key=k + "-del")))
        traffic.resume()
        time.sleep(10)
        traffic.pause()
        rc, out = self.prose("C2.5 check", "$JR \"SELECT key, op, result FROM intents ORDER BY id DESC LIMIT 5\"\n"
                             "$RUN s3gw.state --wait-drained 300 | jq .backlog")
        self.check("C2.5 open: ELMA365 writes go through the reverse journal, the reverse backlog drains to 0",
                   rc == 0 and bool(t_cut) and st == ("200", "200", "200") and out.strip().endswith("0"),
                   {"T_CUTOVER": t_cut[-1:], "elma": st, "backlog": out.strip().splitlines()[-1:]})
        cmp = self.compare()
        self.results["c3_compare"] = cmp
        self.check("C3 MinIO and the new S3 identical after the cutover (reverse handler)",
                   cmp["missing_in_b"] == cmp["extra_in_b"] == cmp["different"] == 0, {k2: cmp[k2] for k2 in list(cmp)[:5]})
        rc, out = self.shell("C2 undo after open (operator's mistake)", "$CUT freeze >/dev/null\n$CUT undo")
        self.check("C2 undo after open is refused (writes went through the reverse journal): only C4",
                   rc == 1 and "только rollback" in out and self.state()["journal"] == "/var/lib/s3gw/journal-reverse.db",
                   out.strip().splitlines()[-1:])

        # C4: the runbook's block, its rollback killed after the first step; then killed after every other step in turn;
        # the last rerun finishes
        traffic.resume()
        time.sleep(5)
        rc, out = self.block("C4 rollback", "$CUT rollback", errexit=True, timeout=1500, subs=[
            ("<T_CUTOVER минус 1 минута>", '"$(date -u -d "$T_CUTOVER - 1 minute" +%FT%TZ)"', "the operator's arithmetic"),
            ("$CUT rollback", f"CUTOVER_FAULT={ROLLBACK_STEPS[0]} $CUT rollback", "the stand interrupts the rollback")])
        traffic.pause()
        self.check("C4 freeze, drain and verify of the block pass; its rollback killed after the first step",
                   rc == 137 and out.count("ok:") >= 3, {"exit": rc, "last": out.strip().splitlines()[-1:]})
        self.interrupted("C4 rollback", "$CUT rollback", ROLLBACK_STEPS[1:])
        rc, out = self.shell("C4 rollback, rerun to the end", "$CUT rollback")
        got = try_op(lambda: self.elma.get_object(Bucket=BUCKET, Key=k)["Body"].read())
        k2 = f"after-rollback/{uuid.uuid4()}"
        put = try_op(lambda: self.elma.put_object(Bucket=BUCKET, Key=k2, Body=b"r"))
        st = self.state()
        journaled = self.sh(f"sudo -u s3gw sqlite3 -readonly {st['journal']} \"SELECT result FROM intents WHERE key = '{k2}'\"").strip()
        archived = self.sh("ls /var/lib/s3gw | grep -c 'journal-reverse.db.rolled-back-' || true").strip()
        self.check("C4 rollback resumed to the end: the file of after the cutover read from MinIO, new writes journaled "
                   "in a fresh journal-after-rollback-<time>.db, reverse journal archived, handler off",
                   rc == 0 and got == "200" and put == "200" and journaled == "success" and archived != "0"
                   and re.fullmatch(r"/var/lib/s3gw/journal-after-rollback-\d{8}-\d{6}\.db", st["journal"]) is not None
                   and self.sh("systemctl is-active s3gw-handler", check=False).strip() == "inactive",
                   {"exit": rc, "read": got, "write": put, "journal": st["journal"], "journaled": journaled, "archived_files": archived})

        rc, out = self.block("Failures: keys in error, the batched reset", 'JW="sudo -u s3gw sqlite3 /var/lib/s3gw/journal.db"')
        for cmd in self.monitoring:
            self.prose(f"monitoring: {cmd[:40]}", cmd, errors_ok=())
        self.check("monitoring table: every command runs", True, len(self.monitoring))

    def window_to_switch(self, label: str, faults: Sequence[str] = ()) -> None:
        rc, out = self.block(f"{label} freeze", "$CUT freeze      # печатает время начала окна")
        st = try_op(lambda: self.elma.put_object(Bucket=BUCKET, Key="frozen", Body=b"x"))
        self.check(f"{label}.1 freeze: the script's own check passed, ELMA365 upload gets 503", rc == 0 and st == "503", (rc, st))
        rc, out = self.block(f"{label} drain", "$CUT drain", exact=True)
        self.check(f"{label}.2 drain: queue empty, no keys in error", rc == 0 and '"drained": true' in out, out.strip().splitlines()[-1:])
        rc, out = self.block(f"{label} verify", "$CUT verify <T_RECONCILE минус 1 минута>", subs=[
            ("<T_RECONCILE минус 1 минута>", '"$(date -u -d "$T_RECONCILE - 1 minute" +%FT%TZ)"', "the operator's arithmetic")])
        self.check(f"{label}.3 verify since the reconcile: mismatched 0", rc == 0 and '"mismatched": 0' in out, out.strip().splitlines()[-1:])
        if faults:
            self.interrupted(f"{label} switch", "$CUT switch", faults)
        rc, out = self.block(f"{label} switch", "$CUT switch", exact=True)
        reads = (try_op(lambda: self.elma.get_object(Bucket=BUCKET, Key=BIG)["Body"].read()),
                 try_op(lambda: self.elma.put_object(Bucket=BUCKET, Key="frozen", Body=b"x")))
        self.check(f"{label}.4 switch: the script's checks passed; ELMA365 opens an old file (now from the new S3), upload still 503",
                   rc == 0 and reads == ("200", "503"), {"exit": rc, "elma": reads, "last": out.strip().splitlines()[-2:]})

    # -- run -------------------------------------------------------------------
    def run(self) -> int:
        traffic: Optional[sp.Traffic] = None
        ok = True
        OUT.mkdir(parents=True, exist_ok=True)
        self.log = open(OUT / "blocks.log", "w", encoding="utf-8")
        try:
            keys = self.up()
            self.note(f"replica up: {len(keys)} objects, minio2 at {ADDR}, new S3 at {NEW_ADDR}")
            sp.PORT = PORT  # ELMA365's traffic goes to the replica's published minio2:9000
            traffic = sp.Traffic([k for k in keys if k != BIG])  # the checks read BIG: the traffic must not delete it
            self.stage_a(traffic)
            traffic.pause()
            self.stage_b()
            traffic.resume()
            if self.soak_hours:
                self.soak()
            self.stage_c(traffic)
        except Exception as exc:  # noqa: BLE001
            ok = False
            self.check(f"stand aborted: {type(exc).__name__}", False, str(exc)[:500])
            traceback.print_exc()
        finally:
            if traffic:
                traffic.stop()
                self.results["traffic"] = traffic.stats()
            run("docker", "exec", HOST, "bash", "-c", "journalctl --no-pager -u 's3gw-*' -u minio -u nginx > /root/journal.log",
                check=False)
            run("docker", "cp", f"{HOST}:/root/journal.log", str(OUT / "journal.log"), check=False)
            self.log.close()
            self.results["checks"] = self.checks
            self.results["seconds"] = round(time.time() - self.t0)
            (OUT / "report.json").write_text(json.dumps(self.results, indent=1, ensure_ascii=False, default=str), encoding="utf-8")
        failed = [c["check"] for c in self.checks if not c["ok"]]
        print(f"\n{len(self.checks) - len(failed)}/{len(self.checks)} checks passed in {self.results['seconds']} s; "
              f"{len(self.results['blocks'])} runbook blocks run; report {OUT / 'report.json'}")
        if failed:
            print("FAILED:\n  " + "\n  ".join(failed))
        return 0 if ok and not failed else 1


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--objects", type=int, default=1500)
    p.add_argument("--soak", type=float, default=0, metavar="HOURS",
                   help="before stage C, that many hours of traffic: memory, the WAL, the backup timer (3 or more)")
    p.add_argument("--keep", action="store_true", help="leave the replica running: docker exec -it rb-minio2 bash")
    a = p.parse_args()
    if 0 < a.soak < 2:
        p.error("--soak: at least 2 hours (the first and the last hour are compared)")
    sys.stdout.reconfigure(encoding="utf-8")  # the runbook's commands answer in Russian
    stand = Stand(a.objects, a.soak)
    try:
        return stand.run()
    finally:
        if not a.keep:
            stand.cleanup()


if __name__ == "__main__":
    sys.exit(main())
