#!/usr/bin/env python3
"""Latency the gateway adds to PUT/DELETE: same requests direct to MinIO and through the gateway.

Usage:
  SRC_ACCESS_KEY=... SRC_SECRET_KEY=... python3 scripts/bench_gateway_latency.py \
      --direct http://127.0.0.1:9000 --gateway http://127.0.0.1:9100 --bucket s3elma365 --count 500
Writes keys under the prefix bench-gw/<run id>/ and deletes them afterwards.
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import os
import statistics
import time
import uuid

import boto3
from botocore.config import Config


def client(endpoint: str) -> object:
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.environ["SRC_ACCESS_KEY"],
        aws_secret_access_key=os.environ["SRC_SECRET_KEY"],
        region_name=os.environ.get("S3_REGION", "us-east-1"),
        config=Config(s3={"addressing_style": "path"}, signature_version="s3v4", retries={"max_attempts": 1},
                      max_pool_connections=64, request_checksum_calculation="when_required"),
    )


def pct(values: list, p: float) -> float:
    s = sorted(values)
    return s[min(len(s) - 1, int(round(p / 100 * (len(s) - 1))))] * 1000


def run(cl: object, bucket: str, prefix: str, count: int, size: int, workers: int) -> dict:
    body = os.urandom(size)

    def put(i: int) -> float:
        t = time.perf_counter()
        cl.put_object(Bucket=bucket, Key=f"{prefix}{i}", Body=body)
        return time.perf_counter() - t

    def delete(i: int) -> float:
        t = time.perf_counter()
        cl.delete_object(Bucket=bucket, Key=f"{prefix}{i}")
        return time.perf_counter() - t

    out = {}
    try:
        for name, fn in (("put", put), ("delete", delete)):
            with cf.ThreadPoolExecutor(workers) as pool:
                lat = list(pool.map(fn, range(count)))
            out[name] = {"p50": pct(lat, 50), "p95": pct(lat, 95), "p99": pct(lat, 99), "mean": statistics.mean(lat) * 1000}
    finally:
        if "delete" not in out:  # a failed run must not leave bench keys in the production bucket
            for i in range(count):
                try:
                    cl.delete_object(Bucket=bucket, Key=f"{prefix}{i}")
                except Exception:
                    pass
    return out


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--direct", required=True)
    p.add_argument("--gateway", required=True)
    p.add_argument("--bucket", default="s3elma365")
    p.add_argument("--count", type=int, default=500)
    p.add_argument("--size", type=int, default=64 * 1024)
    p.add_argument("--workers", type=int, default=4)
    a = p.parse_args()
    run_id = uuid.uuid4().hex[:8]
    for name, ep in (("direct", a.direct), ("gateway", a.gateway)):
        r = run(client(ep), a.bucket, f"bench-gw/{run_id}/{name}/", a.count, a.size, a.workers)
        for op, v in r.items():
            print(f"{name:8} {op:7} p50={v['p50']:.1f}ms p95={v['p95']:.1f}ms p99={v['p99']:.1f}ms mean={v['mean']:.1f}ms")


if __name__ == "__main__":
    main()
