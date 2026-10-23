# elma365_s3_migrator

Миграция объектного хранилища ELMA365 с MinIO на новое S3 на физическом сервере. Текущая схема: шлюз `s3gw` записывает намерение в журнал перед каждым изменяющим запросом и дублирует изменения в новое хранилище, а утилита массового переноса копирует накопленные объекты.

Постановка: [docs/problem-and-tasks.md](docs/problem-and-tasks.md). План работ по задачам: [tasks/README.md](tasks/README.md).

## Статус задач

На 2026-10-22. Подробности по каждой задаче смотрите в [tasks/](tasks/README.md), в разделе «Решения» каждого файла.

| № | Задача | Статус |
|---|--------|--------|
| 01 | [Паспорт источника и операции ELMA365](tasks/01-source-passport-and-s3-operations.md) | Сделано: [source-passport.md](docs/source-passport.md). Замеры листинга решено не делать, скорость покажет отчёт переноса |
| 02 | [Выбор замены MinIO](tasks/02-choose-minio-replacement.md) | Частично: SeaweedFS 4.48 проходит все стендовые сценарии. Осталось: нагрузка, позиция поддержки ELMA, итоговая записка |
| 03 | [Развёртывание нового хранилища](tasks/03-deploy-new-storage.md) | Ждёт новых серверов |
| 04 | [Шлюз и журнал намерений](tasks/04-gateway-and-intent-journal-design.md) | Сделано: [gateway-design.md](docs/gateway-design.md) |
| 05 | [Утилита массового переноса](tasks/05-bulk-transfer-tool-design.md) | Сделано: [bulk-transfer-design.md](docs/bulk-transfer-design.md) |
| 06 | [Реализация и испытания](tasks/06-implement-and-test-components.md) | Сделано на стенде: `s3gw/`, стендовые тесты операций, отказов, конкурентного переноса, замер задержки. Осталось: нагрузка на ≥ 1 млн объектов и память на реальной ВМ |
| 07 | [Маршрутизация](tasks/07-nginx-routing.md) | Сделано: nginx на `minio2`, Kubernetes не меняется ([nginx-routing.md](docs/nginx-routing.md), [PIPELINE.md](PIPELINE.md), раздел A4). Конфиг проверен на локальном стенде с MinIO и `mc` версий `minio2` ([scripts/stand_pipeline.py](scripts/stand_pipeline.py)) |
| 08 | [Включение журнала](tasks/08-enable-change-journal.md) | Можно включать до серверов: [PIPELINE.md](PIPELINE.md), этап A (журнал без приёмника, сверка с `mc admin trace`, листинг заранее; при таком порядке скрипт для окна переключения не нужен) |
| 09 | [Перенос накопленных объектов](tasks/09-bulk-transfer-run.md) | Ждёт 08. Готово: [PIPELINE.md](PIPELINE.md), этап B |
| 10 | [Проверка полноты и целостности](tasks/10-completeness-and-integrity-check.md) | Ждёт 09. Сверка C1: [PIPELINE.md](PIPELINE.md), [docs/tools](docs/tools/README.md) |
| 11 | [Переключение ELMA365](tasks/11-cutover-and-finish.md) | Ждёт 10 и 12. Шаги на `minio2` без доступа к кластеру: [PIPELINE.md](PIPELINE.md), этап C. После срока отката адрес `minio2` переносится на новый сервер (C5) |
| 12 | [Процедура отката](tasks/12-rollback-procedure.md) | Описано под `minio2`: [rollback.md](docs/rollback.md), команды в [PIPELINE.md](PIPELINE.md), C4. C2 и C4 проходит стенд `scripts/stand_pipeline.py` |

## Runbook миграции

Два параллельных процесса (журнал изменений и массовый перенос), как они стыкуются, порядок запуска и все шаги на серверах описаны в [PIPELINE.md](PIPELINE.md):

1. **Этап A, сейчас, без нового S3:** шлюз в рабочем пути, подтверждение охвата журнала, инвентаризация бакета.
2. **Этап B, когда новое S3 развёрнуто ([задача 03](tasks/03-deploy-new-storage.md)):** обработчик разбирает журнал, перенос копирует по готовому листингу.
3. **Этап C:** проверка полноты, переключение шлюза на новое S3 на `minio2`, откат при необходимости.

## Структура

```
s3gw/          шлюз, журнал намерений и массовый перенос (текущая миграция)
migrator/      сверка бакетов для C1 (reconcile_buckets.py, фаза verify)
scripts/       сверка st5, стенд конвейера в Docker, замер задержки шлюза, проверка подписи нового S3
deploy/        nginx для включения шлюза на minio2; systemd-юниты s3gw (шлюз, бэкап журнала, обработчик, перенос)
docs/          актуальные техописания (шлюз, перенос, маршрутизация, откат, паспорт источника)
docs/tools/    документация к сверке C1
tasks/         задачи 01–12 текущей миграции
tests/         pytest
```

Состояние запусков (SQLite, логи, спулы) скрипты пишут в `.runtime/`, он тоже не в git.

Код прошлой миграции, который новой схеме не нужен (Stage 1–3, Kafka-синк, манифесты, проверки по ELMA365 Stage 4, докопирование), в эту версию репозитория не входит.

## Документация

| Документ | О чём |
|----------|-------|
| [gateway-design.md](docs/gateway-design.md) | Шлюз и журнал намерений |
| [bulk-transfer-design.md](docs/bulk-transfer-design.md) | Утилита массового переноса |
| [PIPELINE.md](PIPELINE.md) | Конвейер миграции: журнал и перенос, порядок запуска, эксплуатация |
| [nginx-routing.md](docs/nginx-routing.md) | Маршрутизация через nginx на `minio2` |
| [rollback.md](docs/rollback.md) | Процедура отката |
| [source-passport.md](docs/source-passport.md) | Паспорт исходной системы |
| [docs/tools/README.md](docs/tools/README.md) | Сверка бакетов (C1) |

## Запуск

Все команды на серверах — в [PIPELINE.md](PIPELINE.md); код ставится в `/opt/elma365_s3_migrator` (раздел A2).

## Тесты

```bash
pip install -r requirements-dev.txt
python3 -m pytest
```

Стенд всего [PIPELINE.md](PIPELINE.md) от A до C в Docker (MinIO и `mc` версий `minio2`, nginx 1.28, SeaweedFS 4.48 как новое S3): `python scripts/stand_pipeline.py`, ~17 минут. Прогонять перед выкладкой изменений.
