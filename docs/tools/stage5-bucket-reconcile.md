# Stage 5: сверка бакетов (фаза verify)

Независимая сверка `источник -> новое S3` по листингу, шаг C1 в
[PIPELINE.md](../../PIPELINE.md). Она не опирается ни на `bulk.db`, ни на
журнал. Скрипт ничего не копирует и не удаляет: расхождения пишет в `state.db`,
а перепроверяет их `s3gw.verifykeys --keys-file`.

## Что делает

1. Листит оба бакета через `list_objects_v2` по разделам `00..ff` и
   `__catch_all__` (см. ниже), без `HEAD` на каждый объект.
2. Сравнивает ключи источника с новым S3 и пишет расхождения в
   `reconcile_queue`, вид расхождения в столбце `action`:
   - `missing`: ключа нет в новом S3;
   - `size_mismatch`: другой размер;
   - `etag_mismatch`: размер тот же, ETag другой, и с обеих сторон ETag без `-`.
     ETag без `-` — это MD5 одиночного PUT (так у MinIO и SeaweedFS). ETag
     multipart (`md5-N`) зависит от размера частей, поэтому его не сравнивают.
     Правило то же, что в `s3gw/verifykeys.py`. ETag приходит в том же
     листинге, лишних запросов нет.
3. Ключи, которые есть только в новом S3, пишет в `dst_only`. Это только отчёт.

## Запуск

```bash
cd /opt/elma365_s3_migrator
WATERMARK=$(date -u +%FT%TZ) bash scripts/st5_reconcile_buckets.sh
```

`--phase verify` можно указать явно, других фаз нет. Ключи S3 берутся из
алиасов `mc` (`SRC_ALIAS=minio_old`, `DST_ALIAS=minio_new`), `python3` с
`boto3` должен быть в `PATH`. Каталог проекта по умолчанию — родитель
`scripts/`. Полная команда для миграции и разбор расхождений: PIPELINE.md, C1.

## Отчёт

`.runtime/st5-reconcile/report.json`:

- `missing_count`, `size_mismatch_count`, `dst_only_count` и те же `*_bytes`.
  `size_mismatch_count` считает и расхождения ETag (`etag_mismatch`);
- `partition_errors`: разделы, листинг которых упал. Должно быть 0;
- `overall_ok`: `missing_count`, `size_mismatch_count` и `partition_errors`
  равны 0.

Коды возврата: `0` — `overall_ok`, `2` — расхождения или ошибки разделов,
`1` — ошибка конфигурации или доступа.

Ключи для перепроверки:

```bash
sqlite3 .runtime/st5-reconcile/state.db \
  "SELECT key FROM reconcile_queue UNION SELECT key FROM dst_only"
```

В `state.db` также `partition_progress` (счётчики и статус каждого раздела).
Каждый запуск очищает все три таблицы, поэтому отчёт покрывает только разделы
этого запуска. Листинги раздела пишутся во временные таблицы
`src_partition_<p>`/`dst_partition_<p>` и удаляются сразу после сравнения: на
диске лежат листинги только разделов в работе (до `LIST_WORKERS`). Прогресс по
разделам пишется в `progress.jsonl`, ошибки в `errors.log`.

## Watermark

Источник берётся только до `WATERMARK` (`LastModified <= WATERMARK`; по
умолчанию сейчас минус `WATERMARK_DELAY_MINUTES`), новое S3 листится целиком.
Поэтому ключи, записанные после watermark, попадают в `dst_only`, а изменённые
во время листинга — в `reconcile_queue`. Их и перепроверяет `verifykeys`.

## Разделы и catch-all

При `PREFIX_LEN=2` раздел `ab` листится с `Prefix=ab`. Раздел `__catch_all__` —
ключи, которые не начинаются с двух символов `0-9a-f` в нижнем регистре: `AB…`,
`zz…`, `a/…`, ключи короче `PREFIX_LEN`, ключи не на ASCII.

Catch-all не листит бакет целиком, а только промежутки между hex-разделами.
Ключи раздела `p` — это `[p, p с последним символом + 1)` в порядке байтов
UTF-8 (так сортирует S3), поэтому промежутки такие:

- `['', '00')`;
- для каждого первого символа `c`: `[c':', c'a')` и `[c'g', следующий_c'0')`;
- последний `['fg', конец бакета)`.

Всего 33 промежутка. При `PREFIX_LEN=1`: `['', '0')`, `[':', 'a')`,
`['g', конец)`. Промежуток `[lo, hi)` листится с
`StartAfter = s3gw.bulk.pred(lo)` и `Prefix`, равным общему префиксу `lo` и
`hi`. Листинг промежутка останавливается на первом ключе `>= hi`. Для бакета
из UUID-ключей это один запрос на промежуток, около 33 запросов на сторону
вместо второго полного листинга. Если hex-ключ всё же пришёл, он отбрасывается
на клиенте.

Фильтры разделов:

- `START_PREFIX` и `STOP_PREFIX` (включительно), `ONLY_PREFIXES=00,01,__catch_all__`;
- `STOP_PREFIX` исключает catch-all (кроме `STOP_PREFIX=__catch_all__`, который
  отсекает и разделы на `a-f`), `ONLY_PREFIXES` — если `__catch_all__` нет в
  списке, `START_PREFIX` без `STOP_PREFIX` его включает.

Полная сверка — один запуск без фильтров.

## Параметры

Переменные окружения `scripts/st5_reconcile_buckets.sh`:

| Переменная | Умолчание | Назначение |
|------------|-----------|------------|
| `SRC_ALIAS`, `SRC_BUCKET` | `minio_old`, `s3elma365` | Источник: алиас `mc` и бакет |
| `DST_ALIAS`, `DST_BUCKET` | `minio_new`, `s3elma365` | Новое S3 |
| `MC_BIN` | `mc` | Откуда читать алиасы |
| `PROJECT_DIR` | родитель `scripts/` | Корень репозитория |
| `ST5_RUNTIME_DIR` | `$PROJECT_DIR/.runtime/st5-reconcile` | Каталог `state.db`, `report.json`, `progress.jsonl`, `errors.log` (`ST5_STATE_DB`, `ST5_REPORT_FILE`, `ST5_PROGRESS_FILE`, `ST5_ERROR_LOG`) |
| `PREFIX_LEN` | `2` | Разделы `00..ff` и catch-all (`1` — `0..f` и catch-all) |
| `LIST_WORKERS` | `8` | Разделов в работе одновременно |
| `SRC_LIST_WORKERS` | `1` | Одновременных листингов источника (старый MinIO не держит параллельный листинг) |
| `DST_LIST_WORKERS` | `LIST_WORKERS` | Одновременных листингов нового S3 |
| `LIST_PAGE_SIZE` | `1000` | `PageSize` для `list_objects_v2` |
| `SRC_LIST_PAGE_SIZE` | `100` | `PageSize` источника |
| `DST_LIST_PAGE_SIZE` | `LIST_PAGE_SIZE` | `PageSize` нового S3 |
| `LIST_LOG_EVERY_PAGES` | `100` | Строка прогресса листинга каждые N страниц |
| `S3_READ_TIMEOUT` | `300` | Read timeout S3 |
| `SRC_S3_READ_TIMEOUT` | `900` | Read timeout листинга источника |
| `DST_S3_READ_TIMEOUT` | `S3_READ_TIMEOUT` | Read timeout листинга нового S3 |
| `START_PREFIX`, `STOP_PREFIX`, `ONLY_PREFIXES` | пусто | Фильтр разделов (см. выше) |
| `WATERMARK` | пусто | Фиксированный watermark (UTC ISO); пусто — сейчас минус `WATERMARK_DELAY_MINUTES` |
| `WATERMARK_DELAY_MINUTES` | `10` | Отступ watermark от текущего времени |

Если листинг падает с `Read timeout`, снижайте `LIST_WORKERS` и
`LIST_PAGE_SIZE` и для нового S3 (`DST_*`).
