from __future__ import annotations

from s3gw.verifykeys import compare


def test_compare() -> None:
    a = {"size": 3, "etag": "abc"}
    assert compare("k", None, None) is None
    assert compare("k", a, dict(a)) is None
    assert compare("k", a, None)["problem"] == "missing_in_dst"
    assert compare("k", None, a)["problem"] == "extra_in_dst"
    assert compare("k", a, {"size": 4, "etag": "abc"})["problem"] == "size"
    assert compare("k", a, {"size": 3, "etag": "abd"})["problem"] == "etag"
    assert compare("k", {"size": 3, "etag": "x-2"}, a) is None  # multipart ETag is not comparable


class _S3:
    def __init__(self, objects: dict) -> None:
        self.objects = objects

    def head_object(self, Bucket: str, Key: str) -> dict:
        from botocore.exceptions import ClientError

        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {"ContentLength": len(self.objects[Key]), "ETag": '"' + str(hash(self.objects[Key])) + '"'}


def test_keys_file_rechecks_listed_keys_and_marks_mismatches_dirty(tmp_path, monkeypatch, capsys) -> None:
    import json

    from s3gw import journal, verifykeys

    db = tmp_path / "journal.db"
    journal.connect(db).close()
    src = _S3({"same": b"1", "gone-in-dst": b"2", "changed": b"333"})
    dst = _S3({"same": b"1", "changed": b"3", "extra": b"4"})
    monkeypatch.setattr(verifykeys, "clients_from_env", lambda: (lambda: (src, dst)))
    keys = tmp_path / "keys.txt"
    keys.write_text("same\ngone-in-dst\r\nchanged\n\nextra\nsame\n", encoding="utf-8")  # CRLF, blank, duplicate
    assert verifykeys.main(["--journal", str(db), "--keys-file", str(keys), "--mark-dirty"]) == 1
    out = json.loads(capsys.readouterr().out)
    assert out["checked"] == 4 and out["marked_dirty"] == 3
    assert sorted((m["key"], m["problem"]) for m in out["mismatches"]) == [
        ("changed", "size"), ("extra", "extra_in_dst"), ("gone-in-dst", "missing_in_dst")]
    conn = journal.connect(db)
    assert sorted(r["key"] for r in conn.execute("SELECT key FROM key_state WHERE state = 'dirty'")) == [
        "changed", "extra", "gone-in-dst"]
