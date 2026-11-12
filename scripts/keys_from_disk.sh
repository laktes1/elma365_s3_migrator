#!/bin/bash
# Ключи бакета MinIO с его дисков, без ListObjectsV2 (PIPELINE.md, B1, «Ключи с диска»).
#
# MinIO хранит объект key как каталог <диск>/<бакет>/<key>/ с файлом xl.meta. Если все ключи лежат в корне бакета,
# ListObjectsV2 на каждую страницу обходит весь этот каталог (на minio2: 12 млн записей, ~45 минут на страницу),
# а имена можно прочитать с диска один раз. Читаются два диска набора: объект, записанный, пока один диск был
# недоступен и ещё не вылечен, есть хотя бы на другом. Только чтение; MinIO не останавливается.
#
#   scripts/keys_from_disk.sh OUT DIR1 [DIR2 ...]
#   DIR — каталог бакета на диске MinIO, например /var/lib/minio/pool1/data1/minio/s3elma365
#
# Запись в корне, у которой нет xl.meta, — префикс (ключи с '/'): ключи под ним ищутся через find.
# Снимать после включения шлюза (A4): ключ, созданный позже, есть в журнале; удалённый позже станет source_missing.
set -euo pipefail
export LC_ALL=C   # побайтовый порядок, как у S3, и быстрая сортировка
[ $# -ge 2 ] || { echo "usage: $0 OUT DIR1 [DIR2 ...]" >&2; exit 64; }
OUT=$1; shift
TMP=$(mktemp -d "${OUT}.tmp.XXXXXX")
trap 'rm -rf "$TMP"' EXIT
UUID='^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'

i=0
for d in "$@"; do
  [ -d "$d" ] || { echo "no directory $d" >&2; exit 66; }
  i=$((i + 1))
  # ls -f: без сортировки и stat, имена идут по мере чтения каталога
  ( ls -f "$d" | grep -v -x -e '.' -e '..' > "$TMP/top.$i"; echo "$(date -u +%T) $d: $(wc -l < "$TMP/top.$i") записей" >&2 ) &
done
wait

sort -u -S 1G -T "$TMP" "$TMP"/top.* > "$TMP/top"
# UUID в корне — объекты; остальное проверить по одному (их мало): объект или префикс
grep -E "$UUID" "$TMP/top" > "$TMP/keys" || true
grep -v -E "$UUID" "$TMP/top" > "$TMP/other" || true
echo "$(date -u +%T) uuid: $(wc -l < "$TMP/keys"), прочие записи: $(wc -l < "$TMP/other")" >&2
while IFS= read -r e; do
  for d in "$@"; do
    [ -e "$d/$e" ] || continue
    if [ -f "$d/$e/xl.meta" ]; then
      printf '%s\n' "$e"
    else
      (cd "$d" && find "./$e" -name xl.meta -type f -printf '%h\n' | sed 's,^\./,,')   # ./: имя с '-' не флаг
    fi
  done
done < "$TMP/other" >> "$TMP/keys"

sort -u -S 1G -T "$TMP" "$TMP/keys" > "$OUT"
echo "$(date -u +%T) ключей: $(wc -l < "$OUT") -> $OUT" >&2
