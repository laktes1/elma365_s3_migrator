# systemd-юниты s3gw

| Файл | Что делает |
|---|---|
| `s3gw-gateway.service` | шлюз с журналом намерений |
| `s3gw-journal-backup.service`, `.timer`, `.sh` | копия журнала раз в час с ротацией (A5) |
| `s3gw-handler.service` | обработчик журнала (этап B) |
| `s3gw-bulk.service` | массовый перенос (этап B) |
| `s3gw.env.example` | общий конфиг, ставится в `/etc/s3gw/s3gw.env`, права 600 |
| `s3gw-reverse.overrides.example` | строки обратного направления; из них и `s3gw.env` собирается `/etc/s3gw/s3gw-reverse.env` (C0.3) |

Установка и порядок запуска: [PIPELINE.md](../../PIPELINE.md), раздел A2 и дальше.

Шлюз слушает только `127.0.0.1:9110`: к нему ходит nginx на этой же машине ([deploy/nginx/s3gw.conf](../nginx/s3gw.conf), схема в [docs/nginx-routing.md](../../docs/nginx-routing.md)), открывать порт наружу не нужно.
