from __future__ import annotations

import pytest

from s3gw import s3parse as p

H = {"Host": "elma.example.com"}


@pytest.mark.parametrize(
    "method,target,headers,op,key",
    [
        ("GET", "/s3elma365/k", H, p.READ, "k"),
        ("HEAD", "/s3elma365/k", H, p.READ, "k"),
        ("PUT", "/s3elma365/a%2Bb%20c", H, p.PUT, "a+b c"),
        ("PUT", "/s3elma365/a+b", H, p.PUT, "a+b"),
        ("PUT", "/s3elma365/%D0%BF%D0%B0%D0%BF%D0%BA%D0%B0/f", H, p.PUT, "папка/f"),
        ("PUT", "/s3elma365/k", {**H, "x-amz-copy-source": "/s3elma365/o"}, p.COPY, "k"),
        ("PUT", "/s3elma365/k?partNumber=1&uploadId=u", H, p.MPU_PART, "k"),
        ("PUT", "/s3elma365/k?partNumber=1&uploadId=u", {**H, "x-amz-copy-source": "/s3elma365/o"}, p.MPU_PART, "k"),
        ("POST", "/s3elma365/k?uploads", H, p.MPU_CREATE, "k"),
        ("POST", "/s3elma365/k?uploadId=u", H, p.MPU_COMPLETE, "k"),
        ("DELETE", "/s3elma365/k?uploadId=u", H, p.MPU_ABORT, "k"),
        ("DELETE", "/s3elma365/k", H, p.DELETE, "k"),
        ("POST", "/s3elma365?delete", H, p.DELETE_BATCH, ""),
        ("POST", "/s3elma365/?delete", H, p.DELETE_BATCH, ""),
        ("PUT", "/s3elma365/k?tagging", H, p.TAGGING, "k"),
        ("DELETE", "/s3elma365/k?tagging", H, p.TAGGING, "k"),
        ("PUT", "/s3elma365/k?acl", H, p.OBJECT_ATTR, "k"),
        ("PUT", "/s3elma365/k?retention", H, p.OBJECT_ATTR, "k"),
        ("PUT", "/s3elma365/k?something-new", H, p.UNKNOWN_KEY, "k"),
        ("POST", "/s3elma365/k?select&select-type=2", H, p.BUCKET_OP, "k"),
        # MinIO changes keys other than the path's: journal the body's keys or refuse
        ("POST", "/s3elma365/x?delete", H, p.DELETE_BATCH, "x"),
        ("POST", "/s3elma365/k?uploads&uploadId=u", H, p.MPU_COMPLETE, "k"),
        ("POST", "/s3elma365/x", {**H, "Content-Type": "multipart/form-data; boundary=zz"}, p.BUCKET_OP, "x"),
        ("PUT", "/s3elma365/a.tar", {**H, "X-Amz-Meta-Snowball-Auto-Extract": "true"}, p.BUCKET_OP, "a.tar"),
        ("PATCH", "/s3elma365/k", H, p.UNKNOWN_KEY, "k"),
        ("PUT", "/s3elma365", H, p.BUCKET_OP, ""),
        ("PUT", "/s3elma365?lifecycle", H, p.BUCKET_OP, ""),
        ("DELETE", "/s3elma365", H, p.BUCKET_OP, ""),
        (
            "PUT",
            "/s3elma365/k?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=x&X-Amz-Date=d&X-Amz-Expires=60"
            "&X-Amz-SignedHeaders=host&X-Amz-Signature=s",
            H,
            p.PUT,
            "k",
        ),
    ],
)
def test_classify(method: str, target: str, headers: dict, op: str, key: str) -> None:
    r = p.classify(method, target, headers)
    assert (r.op, r.key, r.bucket) == (op, key, "s3elma365")


def test_invalid_utf8_key_is_rejected() -> None:
    with pytest.raises(p.BadRequest) as e:
        p.classify("PUT", "/s3elma365/%FF", H)
    assert e.value.code == "InvalidObjectName"


def test_delete_objects_request_keys_with_entities() -> None:
    body = (
        b'<?xml version="1.0" encoding="UTF-8"?>'
        b'<Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Quiet>false</Quiet>'
        b"<Object><Key>a&amp;b</Key></Object><Object><Key>\xd0\xbf+ x</Key><VersionId>v</VersionId></Object></Delete>"
    )
    assert p.parse_delete_objects_request(body) == ["a&b", "п+ x"]


@pytest.mark.parametrize("body", [b"not xml", b"<Other/>", b"<Delete></Delete>"])
def test_delete_objects_request_malformed(body: bytes) -> None:
    with pytest.raises(p.BadRequest):
        p.parse_delete_objects_request(body)


def test_delete_objects_response_per_key() -> None:
    body = (
        b'<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
        b"<Deleted><Key>a</Key></Deleted>"
        b"<Error><Key>b</Key><Code>AccessDenied</Code><Message>x</Message></Error>"
        b"</DeleteResult>"
    )
    r = p.parse_delete_objects_response(body)
    assert r is not None and r.deleted == ["a"] and r.errors == {"b": "AccessDenied"}
    assert p.parse_delete_objects_response(b"garbage") is None


def test_response_error_code() -> None:
    assert p.response_error_code(b"<Error><Code>InternalError</Code></Error>") == "InternalError"
    assert p.response_error_code(b"<CompleteMultipartUploadResult/>") is None
    assert p.response_error_code(b"") is None


def test_parse_upload_id() -> None:
    body = b'<InitiateMultipartUploadResult xmlns="x"><Bucket>b</Bucket><Key>k</Key><UploadId>u-1</UploadId></InitiateMultipartUploadResult>'
    assert p.parse_upload_id(body) == "u-1"
