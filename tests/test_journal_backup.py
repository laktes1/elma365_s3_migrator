"""deploy/systemd/s3gw-journal-backup.sh while bulk writes to the journal (PIPELINE.md, A5).

Needs the sqlite3 CLI and sh, as on minio2; skipped where they are missing (Windows).
"""
from __future__ import annotations

import os
import shutil
import sqlite3
import subprocess
import sys
import uuid
from pathlib import Path

import pytest

from s3gw import journal

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "deploy" / "systemd" / "s3gw-journal-backup.sh"
# Another process doing what bulk does per copied object, ~100 objects/s, with a checkpoint after each.
WRITER = """
import sys, time, uuid
from pathlib import Path
from s3gw import journal
w = journal.connect(Path(sys.argv[1]), synchronous="NORMAL")
while True:
    journal.bulk_finish(w, journal.bulk_claim(w, uuid.uuid4().hex, "bulk-1", 900), source_missing=False)
    w.execute("PRAGMA wal_checkpoint(PASSIVE)")
    time.sleep(0.01)
"""


@pytest.mark.skipif(not (shutil.which("sqlite3") and shutil.which("sh")), reason="needs the sqlite3 CLI and sh")
def test_backup_finishes_while_bulk_writes_to_the_journal(tmp_path: Path) -> None:
    # `.backup` copies 100 pages a step and starts over when another connection changed the database:
    # here it timed out in some runs, on a 2-3 GB journal under bulk it did not finish at all.
    db = tmp_path / "journal.db"
    conn = journal.connect(db, synchronous="NORMAL")
    conn.execute("BEGIN")
    conn.executemany("INSERT INTO key_state (key, state, updated_at) VALUES (?, 'synced', 'x')",
                     ((uuid.uuid4().hex,) for _ in range(1_500_000)))  # ~160 MB; the real journal is 2-3 GB
    conn.execute("COMMIT")
    conn.close()
    writer = subprocess.Popen([sys.executable, "-c", WRITER, str(db)], env={**os.environ, "PYTHONPATH": str(ROOT)})
    try:
        (tmp_path / "backup").mkdir()
        env = {**os.environ, "JOURNAL_DB": str(db), "BACKUP_DIR": str(tmp_path / "backup")}
        subprocess.run(["sh", str(SCRIPT)], env=env, check=True, timeout=30)
    finally:
        writer.kill()
        writer.wait()
    (copy,) = (tmp_path / "backup").glob("journal-2*.db")
    c = sqlite3.connect(str(copy))
    assert c.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    assert c.execute("SELECT count(*) FROM key_state").fetchone()[0] >= 1_500_000
