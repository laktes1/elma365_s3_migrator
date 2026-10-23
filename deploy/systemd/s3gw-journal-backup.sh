#!/bin/sh
# Копия журнала на другой диск, хранятся последние $KEEP копий (PIPELINE.md, раздел A5).
# Запускается от s3gw (s3gw-journal-backup.service): root не должен открывать журнал,
# иначе файлы -wal/-shm могут остаться с владельцем root, и шлюз не откроет базу.
set -eu
DB=${JOURNAL_DB:-/var/lib/s3gw/journal.db}    # после переключения это journal-reverse.db (этап C)
DIR=${BACKUP_DIR:-/backup/s3gw}
KEEP=${KEEP:-2}
name=$(basename "$DB" .db)
f="$DIR/$name-$(date +%Y%m%d-%H%M).db"
rm -f "$DIR/$name"-*.tmp
# Не .backup: он копирует по 100 страниц и начинает заново после каждой записи другого соединения,
# а bulk пишет в журнал на каждый объект, и копия не кончается. VACUUM INTO читает одной транзакцией.
sqlite3 "$DB" "VACUUM INTO '$f.tmp'"
mv "$f.tmp" "$f"
ls -1t "$DIR/$name"-2*.db | tail -n +$((KEEP + 1)) | xargs -r rm -f
