# Инструменты сверки

Сверка бакетов перед переключением (PIPELINE.md, C1). Код в `migrator/reconcile_buckets.py` (общие функции в `migrator/common.py`), запуск через `scripts/st5_reconcile_buckets.sh` из любого каталога: корень репозитория скрипт находит сам.

Остальной код прошлой миграции (tar-конвейер Stage 1/3, поиск дельты Stage 2, Kafka-синк `event_sync`, манифесты, мониторинг) в эту версию репозитория не входит. Проверки Stage 4 по ELMA365 API, `check_object.py`, `copy_recent_dst_to_src.py` и фазы `benchmark`/`scan-copy` сверки тоже удалены: их заменяют C1 и `s3gw.verifykeys`.

| Что | Запуск | Документация |
|-----|--------|-------------|
| Сверка бакетов по листингу (key, size, ETag), без копирования | `bash scripts/st5_reconcile_buckets.sh` | [stage5-bucket-reconcile.md](stage5-bucket-reconcile.md) |

## Конфиг

Файла конфига нет: параметры задаются переменными окружения при запуске, умолчания в самом скрипте, например `WATERMARK=2026-11-20T18:00:00Z bash scripts/st5_reconcile_buckets.sh`. Ключи S3 берутся из алиасов `mc`. Состояние и отчёт пишутся в `.runtime/st5-reconcile/`.
