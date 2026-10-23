"""Helpers of migrator/reconcile_buckets.py: mc alias credentials, S3 clients.

The tool runs both as a script (python3 migrator/reconcile_buckets.py) and as a module
(tests), so it imports this file with:

    try:
        from migrator import common
    except ImportError:  # python3 migrator/reconcile_buckets.py
        import common  # type: ignore[no-redef]
"""
from __future__ import annotations

import json
import subprocess
import sys
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

_REPO = str(Path(__file__).resolve().parents[1])
if _REPO not in sys.path:  # run as a script: s3gw/ is not importable otherwise
    sys.path.insert(0, _REPO)

Credentials = Tuple[str, str, str]  # (endpoint_url, access_key, secret_key)

_thread_local = threading.local()


def run_cmd(args: List[str], timeout: int = 60) -> subprocess.CompletedProcess:
    return subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)


def load_mc_alias_credentials(mc_bin: str, alias: str) -> Optional[Credentials]:
    """(endpoint_url, access_key, secret_key) of an mc alias, or None.

    Tries several mc CLI variants for compatibility. mc is called only here:
    all S3 I/O goes through boto3.
    """
    candidates = [
        [mc_bin, "alias", "list", "--json", alias],
        [mc_bin, "alias", "ls", "--json", alias],
        [mc_bin, "config", "host", "ls", "--json", alias],
    ]
    for cmd in candidates:
        try:
            cp = run_cmd(cmd, timeout=15)
        except Exception:
            continue
        if cp.returncode != 0:
            continue
        for line in cp.stdout.decode("utf-8", errors="replace").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
            except json.JSONDecodeError:
                continue
            url = data.get("URL") or data.get("url") or data.get("hostname") or ""
            access_key = data.get("accessKey") or data.get("access_key") or ""
            secret_key = data.get("secretKey") or data.get("secret_key") or ""
            if url and access_key and secret_key:
                if not url.startswith(("http://", "https://")):
                    url = "http://" + url
                return url.rstrip("/"), access_key, secret_key
    return None


def make_s3_client(creds: Credentials, read_timeout: int = 300, max_pool_connections: int = 25) -> Any:
    import boto3
    from botocore.config import Config

    endpoint_url, access_key, secret_key = creds
    # A session per client: creating clients from the default session in several threads is not safe.
    return boto3.session.Session().client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(
            signature_version="s3v4",
            max_pool_connections=max_pool_connections,
            connect_timeout=10,
            read_timeout=read_timeout,
            retries={"max_attempts": 3, "mode": "adaptive"},
        ),
    )


def get_thread_s3_client(
    name: str, creds: Credentials, read_timeout: int = 300, max_pool_connections: int = 25
) -> Any:
    """One client per (thread, name, endpoint, timeout, pool size)."""
    clients: Dict[str, Any] = getattr(_thread_local, "clients", None)  # type: ignore[assignment]
    if clients is None:
        clients = _thread_local.clients = {}
    key = f"{name}:{creds[0]}:{read_timeout}:{max_pool_connections}"
    if key not in clients:
        clients[key] = make_s3_client(creds, read_timeout, max_pool_connections)
    return clients[key]

