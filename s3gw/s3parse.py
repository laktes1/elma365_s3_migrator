"""Classification of S3 requests and parsing of bodies (docs/gateway-design.md, section 2)."""
from __future__ import annotations

import urllib.parse
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from typing import Dict, List, Mapping, Optional, Tuple

# Operations
PUT = "put"
COPY = "copy"
MPU_CREATE = "mpu_create"
MPU_PART = "mpu_part"
MPU_COMPLETE = "complete_mpu"
MPU_ABORT = "mpu_abort"
DELETE = "delete"
DELETE_BATCH = "delete_batch"
TAGGING = "tagging"
OBJECT_ATTR = "object_attr"  # acl, retention, legal-hold
UNKNOWN_KEY = "unknown"
BUCKET_OP = "bucket_op"
READ = "read"

# Operations that change a key and need an intent before forwarding.
KEY_CHANGING = {PUT, COPY, MPU_COMPLETE, DELETE, DELETE_BATCH, TAGGING, OBJECT_ATTR, UNKNOWN_KEY}

_OBJECT_ATTR_PARAMS = ("acl", "retention", "legal-hold")


class BadRequest(Exception):
    def __init__(self, code: str, message: str, status: int = 400) -> None:
        super().__init__(message)
        self.code = code
        self.status = status


@dataclass
class S3Request:
    op: str
    bucket: str
    key: str  # decoded object key, "" for bucket-level requests
    upload_id: Optional[str] = None
    params: Dict[str, List[str]] = field(default_factory=dict)


def split_raw_target(raw_target: str) -> Tuple[str, str]:
    """'/b/k?x=1' -> ('/b/k', 'x=1') without decoding."""
    path, _, query = raw_target.partition("?")
    return path, query


def decode_path_segment(raw: str) -> str:
    """Percent-decode a path as S3 does: '+' stays '+', invalid UTF-8 is rejected."""
    try:
        return urllib.parse.unquote(raw, encoding="utf-8", errors="strict")
    except UnicodeDecodeError as exc:
        raise BadRequest("InvalidObjectName", "object name is not valid UTF-8") from exc


def parse_bucket_key(raw_path: str) -> Tuple[str, str]:
    """Return (bucket, key) of a path-style request (ELMA365 uses path-style; nginx routes only those here).

    The key is taken from the raw path with no slash merging or dot-segment
    removal: S3 keys may legitimately contain '//' and '/./'.
    """
    if not raw_path.startswith("/"):
        raise BadRequest("InvalidURI", "path must start with /")
    rest = raw_path[1:]
    bucket_raw, sep, key_raw = rest.partition("/")
    bucket = decode_path_segment(bucket_raw)
    return bucket, decode_path_segment(key_raw) if sep else ""


def classify(method: str, raw_target: str, headers: Mapping[str, str]) -> S3Request:
    """Map an HTTP request to an S3 operation (table 2.2)."""
    raw_path, raw_query = split_raw_target(raw_target)
    bucket, key = parse_bucket_key(raw_path)
    params = urllib.parse.parse_qs(raw_query, keep_blank_values=True)
    m = method.upper()
    upload_id = params.get("uploadId", [None])[0]
    has_copy_source = any(h.lower() == "x-amz-copy-source" for h in headers.keys())

    def req(op: str) -> S3Request:
        return S3Request(op=op, bucket=bucket, key=key, upload_id=upload_id, params=params)

    if m in ("GET", "HEAD", "OPTIONS"):  # OPTIONS: CORS preflight, changes nothing
        return req(READ)
    if not bucket:
        return req(BUCKET_OP)
    if not key:
        if m == "POST" and "delete" in params:
            return req(DELETE_BATCH)
        return req(BUCKET_OP)
    if m == "PUT":
        if any(h.lower() == "x-amz-meta-snowball-auto-extract" for h in headers.keys()):
            return req(BUCKET_OP)  # MinIO unpacks the archive into other keys
        if "partNumber" in params and upload_id:
            return req(MPU_PART)
        if "tagging" in params:
            return req(TAGGING)
        if any(p in params for p in _OBJECT_ATTR_PARAMS):
            return req(OBJECT_ATTR)
        if params:
            known = {"versionId", "x-id"}
            if not set(params) <= known and not _is_presigned(params):
                return req(UNKNOWN_KEY)
        return req(COPY if has_copy_source else PUT)
    if m == "POST":
        # MinIO's route order: uploadId wins over uploads, and ?delete on any path under the bucket
        # deletes the keys in the body. A form upload takes its key from the body too: refused.
        if upload_id:
            return req(MPU_COMPLETE)
        if "uploads" in params:
            return req(MPU_CREATE)
        if "delete" in params:
            return req(DELETE_BATCH)
        return req(BUCKET_OP)
    if m == "DELETE":
        if upload_id:
            return req(MPU_ABORT)
        if "tagging" in params:
            return req(TAGGING)
        return req(DELETE)
    return req(UNKNOWN_KEY)


def _is_presigned(params: Mapping[str, List[str]]) -> bool:
    """Presigned URLs carry X-Amz-* (SigV4) or AWSAccessKeyId/Signature (SigV2) in the query."""
    names = set(params)
    rest = {n for n in names if not (n.startswith("X-Amz-") or n in ("AWSAccessKeyId", "Signature", "Expires"))}
    return rest <= {"versionId", "x-id"} and names != rest


def _strip_ns(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def parse_delete_objects_request(body: bytes) -> List[str]:
    """Keys from a DeleteObjects body (<Delete><Object><Key>..</Key></Object>..</Delete>)."""
    try:
        root = ET.fromstring(body)
    except ET.ParseError as exc:
        raise BadRequest("MalformedXML", "DeleteObjects body is not valid XML") from exc
    if _strip_ns(root.tag) != "Delete":
        raise BadRequest("MalformedXML", "DeleteObjects root element must be Delete")
    keys: List[str] = []
    for obj in root:
        if _strip_ns(obj.tag) != "Object":
            continue
        for child in obj:
            if _strip_ns(child.tag) == "Key":
                keys.append(child.text or "")
    if not keys:
        raise BadRequest("MalformedXML", "DeleteObjects body has no keys")
    return keys


@dataclass
class DeleteResult:
    deleted: List[str]
    errors: Dict[str, str]  # key -> S3 error code


def parse_delete_objects_response(body: bytes) -> Optional[DeleteResult]:
    """Per-key results; None if the body cannot be parsed (all keys become uncertain)."""
    try:
        root = ET.fromstring(body)
    except ET.ParseError:
        return None
    if _strip_ns(root.tag) != "DeleteResult":
        return None
    encoding_url = False
    deleted: List[str] = []
    errors: Dict[str, str] = {}
    for el in root:
        name = _strip_ns(el.tag)
        if name == "EncodingType" and (el.text or "").lower() == "url":
            encoding_url = True
    for el in root:
        name = _strip_ns(el.tag)
        fields = {_strip_ns(c.tag): (c.text or "") for c in el}
        key = fields.get("Key", "")
        if encoding_url:
            key = urllib.parse.unquote_plus(key)
        if name == "Deleted":
            deleted.append(key)
        elif name == "Error":
            errors[key] = fields.get("Code", "")
    return DeleteResult(deleted=deleted, errors=errors)


def response_error_code(body: bytes) -> Optional[str]:
    """S3 error code if the body is an <Error> document (CopyObject/Complete may return 200 + Error)."""
    stripped = body.lstrip()
    if not stripped.startswith(b"<"):
        return None
    try:
        root = ET.fromstring(stripped)
    except ET.ParseError:
        return None
    if _strip_ns(root.tag) != "Error":
        return None
    for c in root:
        if _strip_ns(c.tag) == "Code":
            return c.text or "Unknown"
    return "Unknown"


def parse_upload_id(body: bytes) -> Optional[str]:
    try:
        root = ET.fromstring(body)
    except ET.ParseError:
        return None
    for c in root:
        if _strip_ns(c.tag) == "UploadId":
            return c.text
    return None
