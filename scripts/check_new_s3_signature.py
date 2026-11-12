#!/usr/bin/env python3
"""Does the new S3 accept requests exactly as ELMA365 signs them? (PIPELINE.md, stage C)

After the cutover the gateway on minio2 forwards ELMA365 requests to the new S3 unchanged:
Host elma.example.com, path-style /s3elma365/<key>, region ru-central-1, ELMA365's own keys.
This script signs requests the same way, sends them straight to the new S3 and checks
PUT, HEAD, GET, a presigned GET (X-Amz-SignedHeaders=host) and DELETE. Last, a PUT whose body
does not match its Content-MD5 must be refused: the copy (s3gw/copykey.py) sends Content-MD5 with
every write and relies on the new S3 to check it (SeaweedFS does not check the signed payload hash).

Usage (on minio2, ELMA365's access keys):
  ELMA_ACCESS_KEY=... ELMA_SECRET_KEY=... python3 scripts/check_new_s3_signature.py \
      --endpoint https://NEW_S3_HOST:PORT --host elma.example.com --region ru-central-1
Writes one object under _cutover-check/ and deletes it. Exit code 0 when every step passed.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import os
import sys
import urllib.request
import uuid

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


def client(endpoint: str, host: str, region: str) -> object:
    cl = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.environ["ELMA_ACCESS_KEY"],
        aws_secret_access_key=os.environ["ELMA_SECRET_KEY"],
        region_name=region,
        config=Config(s3={"addressing_style": "path"}, signature_version="s3v4", retries={"max_attempts": 1},
                      request_checksum_calculation="when_required", response_checksum_validation="when_required"),
    )

    def set_host(request, **_):  # type: ignore[no-untyped-def]
        request.headers["Host"] = host  # signed and sent as ELMA365's requests arrive through the gateway

    cl.meta.events.register("before-sign.s3", set_host)
    return cl


def main(argv: list | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--endpoint", required=True, help="new S3, as GW_UPSTREAM will be after the cutover")
    p.add_argument("--host", default="elma.example.com")
    p.add_argument("--region", default="ru-central-1")
    p.add_argument("--bucket", default="s3elma365")
    a = p.parse_args(argv)
    cl = client(a.endpoint, a.host, a.region)
    key = f"_cutover-check/{uuid.uuid4().hex}"
    body = os.urandom(1024)
    ok = True

    def step(name: str, fn) -> object:  # type: ignore[no-untyped-def]
        nonlocal ok
        try:
            res = fn()
            print(f"ok    {name}")
            return res
        except Exception as exc:  # report every step, do not stop at the first failure
            ok = False
            print(f"FAIL  {name}: {exc}")
            return None

    step("PUT", lambda: cl.put_object(Bucket=a.bucket, Key=key, Body=body))
    step("HEAD", lambda: cl.head_object(Bucket=a.bucket, Key=key))

    def get() -> None:
        got = cl.get_object(Bucket=a.bucket, Key=key)["Body"].read()
        assert got == body, "content differs"
    step("GET", get)

    def presigned_get() -> None:
        url = cl.generate_presigned_url("get_object", Params={"Bucket": a.bucket, "Key": key}, ExpiresIn=300)
        req = urllib.request.Request(url, headers={"Host": a.host})
        with urllib.request.urlopen(req, timeout=30) as r:
            assert r.read() == body, "content differs"
    step("presigned GET", presigned_get)

    step("DELETE", lambda: cl.delete_object(Bucket=a.bucket, Key=key))

    def wrong_md5() -> None:
        other = base64.b64encode(hashlib.md5(b"other").digest()).decode()
        try:
            cl.put_object(Bucket=a.bucket, Key=key, Body=body, ContentMD5=other)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code")
            assert code == "BadDigest", f"refused with {code}, not BadDigest"
            return
        cl.delete_object(Bucket=a.bucket, Key=key)
        raise AssertionError("a body that does not match Content-MD5 was stored: the copy's content check does not hold")
    step("PUT with a wrong Content-MD5 refused (BadDigest)", wrong_md5)
    print("all passed" if ok else "some steps failed (FAIL above): the new S3 is not ready for the cutover")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
