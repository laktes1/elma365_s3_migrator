"""Bring one key in the destination to the current source state.

Shared by the change handler and the bulk transfer (docs/gateway-design.md,
section 5, step 2-3; docs/bulk-transfer-design.md, section 5).
"""
from __future__ import annotations

import base64
import hashlib
import re
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

try:
    from botocore.exceptions import ClientError
except ImportError:  # pragma: no cover - tests use fakes
    class ClientError(Exception):  # type: ignore[no-redef]
        response: Dict[str, Any] = {}

COPIED = "copied"
DELETED = "deleted"
SOURCE_MISSING = "source_missing"

_MD5_ETAG = re.compile(r"^[0-9a-f]{32}$")
_COPY_FIELDS = (
    "ContentType",
    "ContentDisposition",
    "ContentEncoding",
    "ContentLanguage",
    "CacheControl",
    "Expires",
)


# Slowest destination write rate we plan for: a write of n bytes starts only if the
# lease has at least n / MIN_WRITE_BPS seconds left (16 MiB part -> 64 s), so a
# stale owner cannot finish a write over the copy of the worker that took the key over.
MIN_WRITE_BPS = 256 * 1024


def write_guard(base_seconds: float, nbytes: int) -> float:
    return max(base_seconds, nbytes / MIN_WRITE_BPS)


def min_lease_seconds(base_seconds: float, cfg: "CopyConfig") -> float:
    """A shorter lease never lets the largest write (one part) start: the key would cycle
    through LeaseExpired forever without an error."""
    return write_guard(base_seconds, max(cfg.single_put_max, cfg.part_size))


class LeaseExpired(Exception):
    pass


class VerifyFailed(Exception):
    pass


@dataclass
class CopyConfig:
    src_bucket: str
    dst_bucket: str
    single_put_max: int = 16 * 1024 * 1024
    part_size: int = 16 * 1024 * 1024
    copy_tags: bool = True


@dataclass
class CopyOutcome:
    status: str  # copied | deleted | source_missing
    size: Optional[int] = None
    etag: Optional[str] = None
    verify: Optional[str] = None  # size | md5


def s3_error_code(exc: BaseException) -> str:
    if isinstance(exc, ClientError):
        return str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))
    return ""


# The source refuses these names outright, so such an object cannot exist in
# it; the gateway still journals the attempt (MinIO answered 400).
INVALID_NAME_CODES = {"XMinioInvalidObjectName", "XMinioInvalidResourceName", "InvalidObjectName", "KeyTooLongError"}


def _is_no_such_key(exc: BaseException) -> bool:
    # Only a NoSuchKey code means "object is absent". A bare 404 without a code
    # (HEAD) or NoSuchBucket must not be read as absence.
    return s3_error_code(exc) == "NoSuchKey"


def metadata_kwargs(resp: Dict[str, Any]) -> Dict[str, Any]:
    """Headers and x-amz-meta-* of a GetObject/HeadObject response, as PutObject arguments."""
    kwargs: Dict[str, Any] = {}
    for f in _COPY_FIELDS:
        v = resp.get(f)
        if v not in (None, ""):
            kwargs[f] = v
    meta = resp.get("Metadata")
    if meta:
        kwargs["Metadata"] = dict(meta)
    return kwargs


def _put_kwargs(resp: Dict[str, Any], cfg: CopyConfig, key: str) -> Dict[str, Any]:
    return {"Bucket": cfg.dst_bucket, "Key": key, **metadata_kwargs(resp)}


def _read_exact(body: Any, n: int) -> bytes:
    chunks: List[bytes] = []
    remaining = n
    while remaining > 0:
        data = body.read(remaining)
        if not data:
            break
        chunks.append(data)
        remaining -= len(data)
    return b"".join(chunks)


def sync_key(
    src: Any,
    dst: Any,
    key: str,
    cfg: CopyConfig,
    check_lease: Callable[[int], None] = lambda _nbytes: None,
    delete_missing: bool = True,
) -> CopyOutcome:
    """GET the key from the source and write it to the destination.

    If the source reports NoSuchKey, the key is deleted in the destination
    (when delete_missing) and the outcome is DELETED, else SOURCE_MISSING.
    check_lease(nbytes) is called right before every destination write of
    nbytes bytes and must raise LeaseExpired when the caller cannot be sure
    to own the key until that write ends (see write_guard).
    """
    try:
        resp = src.get_object(Bucket=cfg.src_bucket, Key=key)
    except Exception as exc:
        if s3_error_code(exc) in INVALID_NAME_CODES:
            return CopyOutcome(DELETED if delete_missing else SOURCE_MISSING)
        if _is_no_such_key(exc):
            if not delete_missing:
                return CopyOutcome(SOURCE_MISSING)
            check_lease(0)
            try:
                dst.delete_object(Bucket=cfg.dst_bucket, Key=key)
            except Exception as dexc:
                if not _is_no_such_key(dexc):
                    raise
            return CopyOutcome(DELETED)
        raise

    body = resp["Body"]
    size = int(resp["ContentLength"])
    src_etag = str(resp.get("ETag", "")).strip('"')
    md5_comparable = bool(_MD5_ETAG.match(src_etag))
    kwargs = _put_kwargs(resp, cfg, key)
    try:
        if size <= cfg.single_put_max:
            data = _read_exact(body, size)
            if len(data) != size:
                raise VerifyFailed(f"short read from source: expected={size} got={len(data)}")
            digest = hashlib.md5(data).digest()
            if md5_comparable and digest.hex() != src_etag:
                raise VerifyFailed("source body md5 does not match source etag")
            check_lease(size)
            dst.put_object(Body=data, ContentMD5=base64.b64encode(digest).decode(), **kwargs)
            verify = "md5" if md5_comparable else "size"
        else:
            _multipart(dst, body, size, kwargs, cfg, check_lease)
            verify = "size"
    finally:
        close = getattr(body, "close", None)
        if close:
            close()

    head = dst.head_object(Bucket=cfg.dst_bucket, Key=key)
    if int(head["ContentLength"]) != size:
        raise VerifyFailed(f"size mismatch after copy: src={size} dst={head['ContentLength']}")

    # GET reports x-amz-tagging-count (TagCount) only for tagged objects (MinIO, SeaweedFS): no extra request for the rest.
    if cfg.copy_tags and resp.get("TagCount"):
        tags = src.get_object_tagging(Bucket=cfg.src_bucket, Key=key).get("TagSet", [])
        # The object was just written, so it has no tags: only non-empty sets need a request.
        if tags:
            check_lease(0)
            dst.put_object_tagging(Bucket=cfg.dst_bucket, Key=key, Tagging={"TagSet": tags})

    return CopyOutcome(COPIED, size=size, etag=src_etag, verify=verify)


def _multipart(
    dst: Any,
    body: Any,
    size: int,
    kwargs: Dict[str, Any],
    cfg: CopyConfig,
    check_lease: Callable[[int], None],
) -> None:
    check_lease(0)
    upload_id = dst.create_multipart_upload(**kwargs)["UploadId"]
    parts: List[Dict[str, Any]] = []
    try:
        sent = 0
        number = 1
        while sent < size:
            chunk = _read_exact(body, min(cfg.part_size, size - sent))
            if not chunk:
                raise VerifyFailed(f"short read from source: expected={size} got={sent}")
            check_lease(len(chunk))
            part = dst.upload_part(
                Bucket=cfg.dst_bucket,
                Key=kwargs["Key"],
                UploadId=upload_id,
                PartNumber=number,
                Body=chunk,
                ContentMD5=base64.b64encode(hashlib.md5(chunk).digest()).decode(),
            )
            parts.append({"PartNumber": number, "ETag": part["ETag"]})
            sent += len(chunk)
            number += 1
        check_lease(0)
        dst.complete_multipart_upload(
            Bucket=cfg.dst_bucket, Key=kwargs["Key"], UploadId=upload_id, MultipartUpload={"Parts": parts}
        )
    except BaseException:
        try:
            dst.abort_multipart_upload(Bucket=cfg.dst_bucket, Key=kwargs["Key"], UploadId=upload_id)
        except Exception:
            pass
        raise
