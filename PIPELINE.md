# Конвейер миграции: журнал и перенос

Миграция идёт двумя процессами, которые работают параллельно на `minio2` (10.10.0.12) рядом с MinIO и делят одну базу состояния ключей.

```
процесс 1: журнал изменений
ELMA365 ─► ingress ─► haproxy ─► nginx 10.10.0.12:9000
                                   ├─ чтения s3elma365, другие бакеты ──────────► MinIO 127.0.0.1:9000
                                   └─ изменения s3elma365 ─► шлюз 127.0.0.1:9100 ─► MinIO
                                                               │ намерение            │ чтение
                                                               ▼                      ▼
                                                    journal.db (key_state) ◄──── обработчик ─► новое S3
                                                               ▲
процесс 2: перенос                                             │ аренда ключа
bulk list: листинг MinIO по 256 диапазонам ─► bulk.db ─► bulk copy ─► новое S3
```

Шлюз включается nginx на самом `minio2`, Kubernetes, ingress и haproxy не меняются: схема и конфиг в [docs/nginx-routing.md](docs/nginx-routing.md).

| | Процесс 1: журнал | Процесс 2: перенос |
|---|---|---|
| Что делает | Ловит каждое изменение ELMA365 и приводит ключ в новом S3 к текущему состоянию MinIO | Перечисляет все объекты бакета и копирует те, которых журнал не касался |
| Компоненты | `s3gw-gateway` (шлюз), `s3gw-handler` (обработчик) | `s3gw.bulk list` (инвентаризация), `s3gw.bulk copy` (копирование), вместе `run` (`s3gw-bulk`) |
| База | `/var/lib/s3gw/journal.db` | `/var/lib/s3gw/bulk.db` и аренды в `journal.db` |
| Работает | Постоянно, с момента включения шлюза (A4) до конца миграции | Один раз, от инвентаризации до опустевшей очереди |
| Подробно | [docs/gateway-design.md](docs/gateway-design.md) | [docs/bulk-transfer-design.md](docs/bulk-transfer-design.md) |

## Как процессы стыкуются

- **Ключ принадлежит тому, кто увидел его первым.** Перенос копирует ключ, только если его нет в `key_state` журнала, и на время копирования берёт аренду. Ключ, который хоть раз прошёл через шлюз, принадлежит обработчику: перенос его пропускает (`skipped_journal`). Если ELMA365 изменила ключ, пока перенос его копирует, перенос отдаёт ключ обработчику (`dirty`), и тот перечитывает текущую версию.
- **Шлюз никого не ждёт.** Намерение пишется всегда, аренды на запись ELMA365 не влияют.
- **Копирование уступает журналу.** Если обработчик отстаёт больше чем на `JOURNAL_BACKLOG_MAX` ключей, копирование встаёт на паузу (`journal_lag`). Листинг от журнала не зависит.
- **Инвентаризация начинается после переключения.** Листинг запускают после первой чистой проверки охвата трассировкой (A6), то есть не раньше чем через час после включения шлюза. Тогда всё, что в окне переключения было записано мимо шлюза, уже лежит в MinIO и попадёт в листинг. Отдельный учёт этого окна не нужен.
- **Отдельного прохода по изменённым ключам нет.** Всё, что менялось во время переноса, есть в журнале и закрывается обработчиком.
- **Без нового S3 работают только шлюз и листинг.** Обработчик и копирование без `DST_*` не стартуют, а журнал копит грязные ключи до появления приёмника.

## Порядок запуска

| Этап | Когда | Процесс 1: журнал | Процесс 2: перенос |
|---|---|---|---|
| A | Сейчас, нового S3 ещё нет | Шлюз в рабочем пути, охват подтверждён трассировкой; обработчик выключен | Инвентаризация (`list`) в `bulk.db` |
| B | Новое S3 развёрнуто ([задача 03](tasks/03-deploy-new-storage.md)) | Включается обработчик и разбирает накопленный журнал | Копирование по готовому листингу (`run` сразу переходит к `copy`) |
| C | Очередь переноса пуста | Работает до переключения ELMA365 | Проверка полноты, переключение, откат |

Если новое S3 появится раньше, чем пройден этап A, шаги этапа A выполняются так же, только обработчик включается сразу (раздел B1), а вместо `list` запускается `run`.

Все команды выполняются на `minio2` от root, если не сказано иное. Каждый шаг заканчивается проверкой. Если проверка не прошла, дальше не идти.

```bash
J="sudo -u s3gw sqlite3 -readonly /var/lib/s3gw/journal.db"   # только чтение и только от s3gw: root оставил бы -wal/-shm, которые шлюз не откроет
RUN="systemd-run --wait --pipe -q -p User=s3gw -p EnvironmentFile=/etc/s3gw/s3gw.env -p WorkingDirectory=/opt/elma365_s3_migrator /opt/elma365_s3_migrator/.venv/bin/python -m"   # утилита s3gw с конфигом юнитов, из любого каталога
B="$RUN s3gw.bulk --bulk-db /var/lib/s3gw/bulk.db"
```

# Этап A. Журнал и инвентаризация до нового S3

Цель: включить шлюз в боевой путь записи, убедиться на живом трафике, что журнал ловит каждое изменение, и заранее перечислить объекты `s3elma365` в `bulk.db`. Копирования на этом этапе нет. К приезду серверов охват уже подтверждён, а перенос стартует сразу с копирования, без многодневного листинга.

## A0. Что нужно заранее

- `python3` **3.10 или новее** (`python3 --version`: зависимости из `requirements.txt` старше не ставятся), `python3-venv`, `sqlite3`, `sysstat` (для `iostat`), `jq` (читать `bulk-status.json`): `apt install python3-venv sqlite3 sysstat jq`.
- `mc` на `minio2` с алиасом, у которого права admin (ниже `ALIAS=local`, подставьте свой).
- nginx на `minio2` уже стоит (старый конфиг `minio-migration.conf` на `:8083` в пути запросов не участвует и не трогается). Через nginx пойдут все запросы к MinIO, каждый занимает два соединения, поэтому проверить запас:

  ```bash
  nginx -v
  grep -E 'worker_(connections|rlimit_nofile)' /etc/nginx/nginx.conf
  ```

  Если `worker_connections` меньше 4096, поднять: в блоке `events` выставить `worker_connections 4096;`, на верхнем уровне добавить `worker_rlimit_nofile 16384;`, затем `nginx -t && systemctl reload nginx`.
- Как запущен MinIO и кто к нему ходит:

  ```bash
  systemctl cat minio | grep -E 'EnvironmentFile|ExecStart'   # где задаются опции (обычно /etc/default/minio, MINIO_OPTS)
  grep -E 'MINIO_OPTS|MINIO_VOLUMES|address' /etc/default/minio
  ss -ltnp | grep -E ':(9000|9001|9080|9100)\b'               # сейчас :9000 у minio, 9080 и 9100 свободны
  ss -tn state established '( sport = :9000 )' | awk 'NR>1 {print $3}' | sort | uniq -c   # на какой адрес minio2 приходят клиенты
  ```

  Все клиенты должны приходить на `10.10.0.12:9000` или `127.0.0.1:9000`. Если есть подключения на другой адрес `minio2`, его нужно добавить в nginx отдельным `listen` (A4), иначе эти клиенты после включения потеряют MinIO. Если MinIO запущен не через systemd (например, в Docker), сказать нам: смену адреса MinIO в A4 нужно будет делать иначе.
- MinIO на `minio2` один узел: `mc admin info $ALIAS` показывает один сервер. Если узлов несколько, остановиться и сказать нам: после A4 MinIO слушает только `127.0.0.1`, и узлы друг друга не увидят.
- Другие бакеты (`elma365-backups`, `-loki`, `-tempo`) в миграцию не входят. nginx отправляет их запросы прямо в MinIO, шлюз в них не участвует ни на одном этапе, и после переключения тоже. Посмотреть, кто к ним ходит и сколько: `timeout 300 mc admin trace --json --call s3 $ALIAS | grep -o '"path": *"/[^/"]*' | sort | uniq -c`.
- Доступ к кластеру Kubernetes не нужен: всё делается на `minio2`, haproxy не меняется.

## A1. Пользователь MinIO только на чтение

Листингу нужен ключ, который может только читать `s3elma365`.

```bash
ALIAS=local
cat > /tmp/s3gw-src-ro.json <<'EOF'
{"Version": "2012-10-17", "Statement": [
  {"Effect": "Allow", "Action": ["s3:ListBucket", "s3:GetBucketLocation"], "Resource": ["arn:aws:s3:::s3elma365"]},
  {"Effect": "Allow", "Action": ["s3:GetObject", "s3:GetObjectTagging"], "Resource": ["arn:aws:s3:::s3elma365/*"]}
]}
EOF
SECRET=$(openssl rand -hex 20); echo "$SECRET"   # сохранить, пойдёт в SRC_SECRET_KEY
mc admin policy create $ALIAS s3gw-src-ro /tmp/s3gw-src-ro.json      # старый mc: mc admin policy add
mc admin user add $ALIAS s3gw-src "$SECRET"
mc admin policy attach $ALIAS s3gw-src-ro --user s3gw-src            # старый mc: mc admin policy set ... user=s3gw-src
```

Проверка: чтение есть, записи нет.

```bash
mc alias set s3gwro http://127.0.0.1:9000 s3gw-src "$SECRET"
mc ls s3gwro/s3elma365 | head -3        # видны объекты
echo x | mc pipe s3gwro/s3elma365/_ro_check   # должно быть Access Denied
```

## A2. Установить шлюз

Журнал лежит на системном SSD (`/var/lib/s3gw`, по оценке до 6 ГБ на 12 млн ключей вместе с `bulk.db`).

```bash
useradd --system --home /var/lib/s3gw --shell /usr/sbin/nologin s3gw
install -d -o s3gw -m 700 /var/lib/s3gw /etc/s3gw
git clone https://github.com/laktes1/elma365_s3_migrator /opt/elma365_s3_migrator
python3 -m venv /opt/elma365_s3_migrator/.venv && /opt/elma365_s3_migrator/.venv/bin/pip install -r /opt/elma365_s3_migrator/requirements.txt
install -o s3gw -m 600 /opt/elma365_s3_migrator/deploy/systemd/s3gw.env.example /etc/s3gw/s3gw.env
cp /opt/elma365_s3_migrator/deploy/systemd/s3gw-*.service /opt/elma365_s3_migrator/deploy/systemd/s3gw-*.timer /etc/systemd/system/ && systemctl daemon-reload
git -C /opt/elma365_s3_migrator log -1 --format='%h %s'   # записать версию в «Решения» задачи 08
```

В `/etc/s3gw/s3gw.env` заполнить `SRC_ACCESS_KEY=s3gw-src`, `SRC_SECRET_KEY=<SECRET>`, а строки `DST_*` закомментировать, пока нового S3 нет. `GW_LISTEN=127.0.0.1:9100` оставить как в примере: к шлюзу ходит только nginx на этой же машине, снаружи он недоступен. `GW_OTHER_UPSTREAM=http://127.0.0.1:9000` тоже оставить (это же значение берётся, если строки нет): nginx другие бакеты в шлюз не шлёт, это страховка. Затем запустить только шлюз:

```bash
systemctl enable --now s3gw-gateway        # s3gw-handler не включать до этапа B
```

Проверка:

```bash
systemctl is-active s3gw-gateway                                                  # active
systemctl is-enabled s3gw-handler                                                 # disabled
curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:9100/minio/health/live  # 200 (запрос проходит шлюз и доходит до MinIO)
$J "SELECT epoch, started_at FROM gateway_epochs ORDER BY epoch DESC LIMIT 1"     # одна строка
ls -l /var/lib/s3gw/                                                              # journal.db, -wal, -shm принадлежат s3gw
```

## A3. nginx на служебном порту и проверка без трафика ELMA365

Конфиг [deploy/nginx/s3gw.conf](deploy/nginx/s3gw.conf) ставится пока только со служебным портом `127.0.0.1:9080`; боевой трафик по-прежнему идёт прямо в MinIO.

```bash
cp /opt/elma365_s3_migrator/deploy/nginx/s3gw.conf /etc/nginx/conf.d/s3gw.conf
nginx -t && systemctl reload nginx
curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:9080/minio/health/live    # 200
```

Замер задержки заодно проверяет запись через связку nginx → шлюз → MinIO. Нужен ключ **с правом записи** в `s3elma365` (например, root MinIO); скрипт пишет только под префикс `bench-gw/` и сразу удаляет записанное, в том числе если упадёт на середине.

```bash
cd /opt/elma365_s3_migrator
S3_REGION=ru-central-1 SRC_ACCESS_KEY=<ключ с записью> SRC_SECRET_KEY=<секрет> .venv/bin/python scripts/bench_gateway_latency.py \
  --direct http://127.0.0.1:9000 --gateway http://127.0.0.1:9080 --count 100
```

Проверка:

```bash
$J "SELECT op, result, count(*) FROM intents WHERE key LIKE 'bench-gw/%/gateway/%' GROUP BY op, result"
# put success 100, delete success 100; ключей из bench-gw/%/direct/ в журнале нет
$J "SELECT count(*) FROM errors"                                                  # 0
mc ls $ALIAS/s3elma365/bench-gw/ | wc -l                                          # 0: ничего не осталось
```

Прибавка p95 на шлюзе должна быть единицы миллисекунд (см. [docs/gateway-design.md](docs/gateway-design.md), раздел 12).

Чтение через nginx (после A4 через него пойдут все чтения). Взять любой крупный объект и сравнить время и контрольную сумму напрямую и через nginx:

```bash
mc alias set nginxtest http://127.0.0.1:9080 s3gw-src "$SECRET"
K=s3elma365/<крупный объект>
time mc cat s3gwro/$K | md5sum
time mc cat nginxtest/$K | md5sum     # та же сумма, время того же порядка
```

## A4. Включение шлюза в рабочий путь (на minio2)

Доступа к кластеру Kubernetes нет, поэтому haproxy не трогаем: он по-прежнему ходит на `10.10.0.12:9000`. Этот адрес теперь слушает nginx, а MinIO переходит на `127.0.0.1:9000`:

- изменения `s3elma365` nginx отправляет в шлюз, чтения `s3elma365` и все запросы к другим бакетам прямо в MinIO;
- снаружи в MinIO мимо nginx не попасть: MinIO слушает только loopback;
- запасного пути мимо шлюза нет: если шлюз недоступен, запись в `s3elma365` получает `502` (видно в `/var/log/nginx/s3gw-5xx.log`), а не уходит в MinIO без журнала. Чтения и другие бакеты при этом работают. systemd поднимает шлюз за секунду, предела перезапусков у него нет (`StartLimitIntervalSec=0`).

При перезагрузке `minio2` порядок запуска не важен: MinIO снаружи недоступен, а пока шлюз не поднялся, запись получает `502`, и ELMA365 её повторит.

MinIO перезапускается, поэтому на несколько секунд недоступны все бакеты; выбрать спокойное время. Подготовить, ничего пока не применяя:

```bash
cp /etc/default/minio /etc/default/minio.before-s3gw
# В MINIO_OPTS добавить (или заменить) адрес API, остальные опции не трогать:
#   MINIO_OPTS="--address 127.0.0.1:9000 <прежние опции, например --console-address :9001>"
vi /etc/default/minio
sed -i 's|^    # listen 10.10.0.12:9000;|    listen 10.10.0.12:9000;|' /etc/nginx/conf.d/s3gw.conf
grep -n 'listen' /etc/nginx/conf.d/s3gw.conf       # listen 127.0.0.1:9080 и listen 10.10.0.12:9000
nginx -t                                            # синтаксис; порт nginx -t не занимает
```

Включить и **записать время** (от него считается окно наблюдения):

```bash
date -Is; systemctl restart minio && systemctl reload nginx; date -Is
```

Активные загрузки в момент включения обрываются, ELMA365 повторяет запрос уже через шлюз.

Проверка сразу после включения, по порядку:

1. Адреса: `ss -ltnp | grep ':9000\b'` показывает `nginx` на `10.10.0.12:9000` и `minio` на `127.0.0.1:9000`; `curl -s -o /dev/null -w '%{http_code}\n' http://10.10.0.12:9000/minio/health/live` даёт `200`.
2. ELMA365 открывает существующий файл (чтение идёт через nginx прямо в MinIO).
3. Загрузить в ELMA365 тестовый файл, затем удалить его.
4. В журнале появились намерения с результатом `success`:

   ```bash
   $J "SELECT id, key, op, result, http_status, created_at FROM intents ORDER BY id DESC LIMIT 10"
   $J "SELECT count(*) FROM intents WHERE result IS NULL"     # 0 или единицы (текущие запросы)
   ```

5. Ошибок нет: `wc -l /var/log/nginx/s3gw-5xx.log` даёт `0` (или файла ещё нет), `/var/log/nginx/s3gw-error.log` пуст, `journalctl -u s3gw-gateway --since '-10 min' | grep -c -i -E 'error|failed'` даёт `0`.
6. Другие сервисы пишут как раньше: `mc ls $ALIAS/elma365-loki | tail -2` показывает свежие объекты.

### Откат (мгновенный, без перезапуска MinIO)

Любая из проверок не прошла, ELMA365 получает ошибки при загрузке, шлюз недоступен. Сначала запустить трассировку (по ней изменения, сделанные мимо шлюза, потом вернутся в журнал), затем отправить запись прямо в MinIO; nginx остаётся в пути:

```bash
ALIAS=local   # алиас с правами admin, как в A1
T=/var/tmp/trace-rollback-$(date +%F-%H%M).jsonl   # чтения отфильтрованы: трасса лежит на SSD журнала
nohup sh -c "mc admin trace --json --verbose --call s3 $ALIAS | grep --line-buffered -v -i -E '\"method\": ?\"(GET|HEAD|OPTIONS)\"' > $T" 2>/var/tmp/trace-rollback.err &
sleep 5; pgrep -f 'mc admin trace --json' >/dev/null && echo 'трасса пишется' || cat /var/tmp/trace-rollback.err
sed -i 's|default s3gw_gateway;|default s3gw_minio;|' /etc/nginx/conf.d/s3gw.conf && nginx -t && nginx -s reload
```

Данные не теряются: MinIO остаётся единственным хранилищем, журнал просто перестаёт пополняться. Шлюз можно не останавливать.

Когда причина устранена, включить обратно и сдать журналу всё, что прошло мимо:

```bash
sed -i 's|default s3gw_minio;|default s3gw_gateway;|' /etc/nginx/conf.d/s3gw.conf && nginx -t && nginx -s reload
sleep 60; pgrep -f 'mc admin trace --json' >/dev/null || echo 'трасса умерла раньше: см. ниже'; pkill -f 'mc admin trace --json'
chown s3gw $T
cd /opt/elma365_s3_migrator && sudo -u s3gw .venv/bin/python -m s3gw.tracecheck $T --journal /var/lib/s3gw/journal.db --mark-dirty; echo "exit=$?"
```

`--mark-dirty` помечает ключи, изменённые мимо шлюза, как грязные: их возьмёт обработчик, а перенос их больше не трогает. Это закрывает и уже пройденные листингом диапазоны, и уже перенесённые ключи. Два случая остаются:

- в `by_call` есть `s3.DeleteMultipleObjects`: ключи удалений пачкой в трассировке не видны. На этапе A это не страшно (копирование не найдёт удалённый объект), на этапе B после этого обязательна полная сверка (этап C, пункт 1);
- трассировка не писалась всё время отката (`mc` умер раньше), пуста или снята без `--verbose` (`tracecheck` вернул код 2): на этапе A начать инвентаризацию заново (раздел A7, «Начать заново»), на этапе B обязательна полная сверка.

**Полный возврат** (в окно, если nginx на `:9000` нужно убрать совсем): вернуть MinIO на прежний адрес.

```bash
cp /etc/default/minio.before-s3gw /etc/default/minio
sed -i 's|^    listen 10.10.0.12:9000;|    # listen 10.10.0.12:9000;|' /etc/nginx/conf.d/s3gw.conf
systemctl reload nginx && systemctl restart minio
```

Мгновенный откат и полный возврат годятся только до переключения ELMA365 на новое S3 (C2). После него откат идёт по C4.

## A5. Резервная копия журнала

Включить сразу после A4: с этого момента журнал единственная запись изменений. Копия снимается раз в час от пользователя `s3gw` (`VACUUM INTO`: одна читающая транзакция, запись шлюза и bulk не мешает), хранятся последние 2 (`KEEP` и `BACKUP_DIR` в `s3gw-journal-backup.service`):

```bash
install -d -o s3gw -m 700 /backup/s3gw
df -h /var/lib/s3gw /backup/s3gw     # если файловая система одна: свободно не меньше (KEEP + 2) × размер журнала
systemctl enable --now s3gw-journal-backup.timer
systemctl start s3gw-journal-backup; ls -l /backup/s3gw     # первая копия
```

К концу переноса журнал вырастает до 2–3 ГБ. Другого диска, кроме дисков MinIO, на `minio2` нет, поэтому `/backup` обычно лежит на том же системном SSD (свободно 35 ГБ), что и журнал: копия спасает от порчи файла, но не от отказа диска. Свежесть копии смотреть в «Наблюдении за журналом».

Восстановление, если `journal.db` потерян или повреждён: сделать мгновенный откат в nginx (раздел A4, «Откат», с трассировкой), остановить `s3gw-handler` и `s3gw-bulk`, затем шлюз, подложить последнюю копию (`cp /backup/s3gw/journal-<последняя>.db /var/lib/s3gw/journal.db`, убрать старые `journal.db-wal` и `journal.db-shm`, владелец `s3gw`), запустить шлюз и вернуть запись в шлюз (там же). Изменения с момента копии потеряны. На этапе A после этого начать инвентаризацию заново (раздел A7), на этапе B обязательна полная сверка.

## A6. Подтверждение охвата по трассировке MinIO

`mc admin trace` показывает каждый запрос, который дошёл до MinIO. Шлюз добавляет к каждому пересланному запросу заголовок `X-S3gw-Request-Id` и под этим же id пишет намерения; заголовок, присланный клиентом, срезается. `s3gw.tracecheck` проверяет две вещи:

- все изменения в `s3elma365` пришли через шлюз (`bypass_total = 0`);
- у каждого из них есть намерение этого запроса для этого ключа (`missed_total = 0`). Так ловится и пропущенная перезапись ключа, который уже был в журнале.

Адрес клиента для проверки не годится: MinIO показывает в поле `client` значение `X-Forwarded-For` от ingress, и оно одинаково для запросов через шлюз и мимо него (проверено на стенде).

Записать час рабочего трафика. Флаг `--verbose` обязателен: без него в трассировке нет заголовков. Чтения отфильтровываются, чтобы файл был небольшим.

```bash
ALIAS=local; T=/var/tmp/trace-$(date +%F-%H%M).jsonl
timeout 3600 mc admin trace --json --verbose --call s3 $ALIAS 2>/dev/null | grep --line-buffered -v -i -E '"method": ?"(GET|HEAD|OPTIONS)"' > $T
chown s3gw $T; chmod 600 $T     # в трассировке заголовки запросов (подписи, не секреты); читает её tracecheck от s3gw
```

Через минуту после начала проверить формат: `grep -m1 '"s3.PutObject"' $T | head -c 1500`. В строке должны быть `"api"`, `"path"`, `"request"` с `"method"` и `"headers"`, а среди заголовков `"X-S3gw-Request-Id"` (так пишет `mc` 2025-08; старый формат с `funcName` и `http.request` скрипт тоже понимает). Если формат другой, прислать одну строку: скрипт подстроим.

После окончания записи:

```bash
cd /opt/elma365_s3_migrator && sudo -u s3gw .venv/bin/python -m s3gw.tracecheck $T --journal /var/lib/s3gw/journal.db; echo "exit=$?"
```

Норма: `changes` больше нуля (ELMA365 что-то писала), `without_headers: 0`, `bypass_total: 0`, `missed_total: 0`, `exit=0`.

- `exit=2`: проверять нечего. Либо `changes` 0 (`mc` умер, алиас не тот или ELMA365 не писала), либо `without_headers` больше 0 (снято без `--verbose`). Повторить запись.
- `bypass` не пуст: запись дошла до MinIO мимо шлюза. Снаружи это невозможно (MinIO слушает только `127.0.0.1`), значит пишет процесс на самом `minio2`: `mc`, скрипт, замер задержки (A3 во время записи трассировки не запускать). Найти и остановить, затем сдать эти ключи журналу: тот же запуск с `--mark-dirty`.
- `missed` не пуст: запрос прошёл через шлюз, а намерения для ключа нет. Это ошибка шлюза; прислать вывод и строки `$J "SELECT * FROM errors ORDER BY id DESC LIMIT 20"`. Затем начать инвентаризацию заново (раздел A7), если она уже идёт.
- Для пакетного удаления (`s3.DeleteMultipleObjects`) ключи лежат в теле запроса, которого в трассировке нет, поэтому скрипт проверяет только, что у запроса есть намерения. Поключевой охват пакетного удаления проверен тестами шлюза и на стенде (400 ключей).
- Теги переносятся (`COPY_TAGS=1`, политика из A1 это разрешает), но запрос тегов идёт только к объектам, у которых они есть: GET из MinIO сообщает их число (`x-amz-tagging-count`). Для остальных лишнего запроса к дискам нет.

Первая чистая запись разрешает старт инвентаризации (раздел A7). Повторить запись в разное время несколько дней (день, вечер, время резервного копирования ELMA365) и после каждой перезагрузки `minio2`. Результаты и время переключения записать в «Решения» задачи 08. Файлы трассировки после проверки удалить.

Ежедневно смотреть журнал по разделу «Наблюдение за журналом».

## A7. Инвентаризация бакета (листинг без копирования)

Запускать после первой чистой проверки охвата (раздел A6). Тогда отдельный учёт ключей из окна переключения не нужен: всё, что было записано мимо шлюза в этом окне, уже лежит в MinIO и попадёт в листинг, а копирование потом читает текущую версию объекта.

Листинг идёт по 256 диапазонам по первым двум hex-символам ключа (ключи — UUID) и сохраняет контрольную точку после каждой страницы. Он ходит в MinIO напрямую (`SRC_ENDPOINT=http://127.0.0.1:9000`), мимо nginx. Число параллельных листеров берётся из расписания `workers=` (не больше `LIST_WORKERS`), пауза и расписание проверяются между диапазонами. Журнальное отставание (`JOURNAL_BACKLOG_MAX`) в режиме `list` не учитывается. Второй процесс `bulk` на том же `bulk.db` не запустится, пока работает первый.

Начать с одного листера в любое время суток, чтобы померить нагрузку на 16 HDD:

```bash
systemd-run --unit s3gw-list -p User=s3gw -p EnvironmentFile=/etc/s3gw/s3gw.env -p WorkingDirectory=/opt/elma365_s3_migrator \
  /opt/elma365_s3_migrator/.venv/bin/python -m s3gw.bulk list --list-workers 1
```

Проверка в первые полчаса:

```bash
systemctl is-active s3gw-list                         # active
jq .listing /var/lib/s3gw/bulk-status.json            # ranges, listed_objects, objects_per_min_last_5m, page_ms_last_5m
iostat -x 5 3                                         # %util и await дисков MinIO
```

`page_ms_last_5m` — сколько MinIO отвечает на страницу листинга. Его рост вместе с `await` дисков значит, что листинг упирается в диски MinIO.

Плюс задержки ELMA365: открыть несколько файлов, спросить пользователей. Если `await` дисков заметно вырос или ELMA365 тормозит, сразу остановить:

```bash
systemctl stop s3gw-list      # текущая страница сохраняется, диапазон вернётся в очередь
```

Если нагрузка заметна только днём, перезапустить с дневным и ночным режимом (продолжит с контрольных точек):

```bash
systemctl reset-failed s3gw-list 2>/dev/null
systemd-run --unit s3gw-list -p User=s3gw -p EnvironmentFile=/etc/s3gw/s3gw.env -p WorkingDirectory=/opt/elma365_s3_migrator \
  /opt/elma365_s3_migrator/.venv/bin/python -m s3gw.bulk list --list-workers 4 --schedule '08:00-20:00 workers=1; 20:00-08:00 workers=4'
```

Днём без перезапуска можно приостановить: `$B pause` и `$B resume`. Пауза срабатывает после текущего диапазона (около 1/256 бакета); для немедленной остановки использовать `systemctl stop s3gw-list`.

Готово, когда `systemctl is-active s3gw-list` даёт `inactive`, а `$B status` показывает все 256 диапазонов в `listed` и `jobs.pending` около 12 млн. Если юнит завершился с ошибкой (`systemctl status s3gw-list`, строка `Main PID ... status=`):

- `status=2`: есть диапазоны в `error`. Причины: `$B export-errors --out /var/lib/s3gw/list-errors.jsonl`; вернуть в очередь: `$B retry-errors`.
- `status=3`: работа осталась, хотя остановки не было (упал поток), причина в `journalctl -u s3gw-list`.

В обоих случаях после разбора: `systemctl reset-failed s3gw-list` и снова команда запуска выше; листинг продолжит с контрольных точек. Время окончания и скорость записать в «Решения» задачи 09: это первая реальная оценка скорости источника.

**Начать заново** (после `missed`, потери журнала или отката без трассировки): листинг не знает, что ключи в уже пройденных диапазонах могли появиться мимо журнала.

```bash
systemctl stop s3gw-list s3gw-bulk 2>/dev/null
mkdir -p /var/lib/s3gw/old && mv /var/lib/s3gw/bulk.db* /var/lib/s3gw/old/
```

Затем запуск, как выше.

# Этап B. Новое S3: обработчик и копирование

## B1. Включить обработчик

Раскомментировать `DST_*` в `/etc/s3gw/s3gw.env` (ключ с записью только в один бакет нового S3), затем:

```bash
systemctl enable --now s3gw-handler
```

Обработчик начинает разбирать ключи, накопленные журналом на этапе A. Проверка: отставание (запрос в разделе «Наблюдение за журналом») уменьшается, ключи переходят в `synced` и `deleted`, `errors` не растёт. Перед копированием дождаться, пока отставание сойдёт до единиц ключей.

## B2. Перед копированием

- Согласовать порог деградации ELMA365 (например p95 задержки GET) и дневное и ночное окно, затем записать их в `SCHEDULE` в `/etc/s3gw/s3gw.env`. Формат: `08:00-20:00 workers=2 mbps=200; 20:00-08:00 workers=8 mbps=800`, часовой пояс задаётся в `SCHEDULE_TZ`.
- `JOURNAL_BACKLOG_MAX` ставит копирование на паузу, если обработчик отстаёт больше чем на это число ключей.
- Начинать с консервативных лимитов.

## B3. Запуск копирования

Листинг из A7 должен быть закончен или остановлен (`systemctl is-active s3gw-list` даёт `inactive`): второй процесс `bulk` на том же `bulk.db` не стартует.

```bash
systemctl start s3gw-bulk          # run: если листинг готов, сразу copy; продолжает с контрольных точек
journalctl -u s3gw-bulk -f
```

Наблюдение:

```bash
cat /var/lib/s3gw/bulk-status.json          # текущее состояние, перезаписывается каждые 10 с
iostat -x 5 3                               # нагрузка на диски MinIO
```

`bulk-status.json` (путь задаёт `BULK_STATUS_FILE`, по умолчанию рядом с `bulk.db`):

| Поле | Что показывает |
|---|---|
| `progress` | заданий всего, обработано, осталось, процент; `listing_complete: false` — листинг ещё идёт, «осталось» растёт |
| `jobs`, `bytes` | задания по состояниям (`done` скопировано, `skipped_journal` отдано журналу, `source_missing`, `error`), байты скопированные и оставшиеся |
| `rate` | объектов в минуту, скопировано в минуту и МБ/с за 5 минут, за час и с запуска |
| `eta` | когда закончится по скорости последнего часа (по объектам и по байтам, берётся медленнее) |
| `limits` | текущие потоки, Мбит/с и причина: `schedule`, `manual`, `paused`, `journal_lag` |
| `copiers_last_5m` | доля времени копировщиков: чтение из MinIO (`source_read`), запись в новое S3 (`destination_write`), ожидание лимита скорости (`throttle_wait`), простой (`idle`) |
| `per_copied_object_last_5m` | средний размер объекта, мс на объект и на МБ отдельно для MinIO и нового S3 |
| `bottleneck` | подсказка, что сейчас ограничивает скорость (ниже) |
| `journal` | отставание обработчика и порог `JOURNAL_BACKLOG_MAX` |
| `errors` | повторов за 5 минут, заданий и диапазонов в `error`, последняя ошибка с ключом |

Как подбирать нагрузку по `bottleneck` (смотреть вместе с `iostat` и задержками ELMA365):

- `mbps_limit` — копировщики в основном ждут лимита скорости. Если `await` дисков MinIO низкий и ELMA365 не тормозит, лимит можно поднять (`$B set mbps_max ...` или `mbps=` в `SCHEDULE`).
- `source` — время уходит на чтение из MinIO. Больше потоков почти не ускорит, а нагрузку на диски поднимет; при росте `await` или задержек ELMA365 потоки снижать.
- `destination` — упираемся в запись в новое S3; на MinIO это не влияет, потоки можно добавлять, пока растёт `rate`.
- `balanced` — обе стороны поровну; добавлять потоки по одному, следя за `rate` и `await`.
- `idle` — копировщики простаивают: работы почти не осталось или задания в `retry` ждут паузы.
- `paused`, `journal_lag`, `no_workers` — копирование остановлено паузой, отставанием обработчика или окном расписания с `workers=0`.

История — `bulk-progress.jsonl`: строка раз в 30 с (около 1 МБ в сутки, растёт со временем, а не с числом объектов) с теми же счётчиками, скоростью за 5 минут, ETA и `bottleneck`. По ней можно разобрать скорость задним числом: `jq -c '[.time, .objects_per_min_5m, .mb_per_s_5m, .bottleneck] ' /var/lib/s3gw/bulk-progress.jsonl | tail`.

Параллельно следить за задержками ELMA365. Раз в день записывать в «Решения» задачи 09 прогресс, ETA, изменения лимитов и повторяющиеся ошибки.

## B4. Управление без перезапуска

```bash
$B pause                    # workers = 0, причина paused
$B resume
$B set workers_max 4        # потолок потоков поверх расписания
$B set mbps_max 300         # потолок полосы
```

Причина текущего лимита видна в `bulk-status.json` (`limits.reason`): `schedule`, `manual`, `paused`, `journal_lag`.

## B5. Ошибки переноса

Задание или диапазон листинга, у которых `BULK_MAX_ATTEMPTS` (10) попыток подряд не удались, переходит в `error`; паузы между попытками 2, 4, 8 ... с, всего около 17 минут. Перенос доделывает остальное и завершается с кодом 2, systemd его не перезапускает.

```bash
$B export-errors --out /var/lib/s3gw/bulk-errors.jsonl   # или .csv: ключ, попытки, last_error
$B retry-errors                                          # после устранения причины: error -> retry, попытки с нуля
systemctl start s3gw-bulk                                # доделывает только вернувшиеся задания и диапазоны
```

`source_missing` означает, что ключ удалили из MinIO после листинга. Это не ошибка, такие ключи перечисляются в отчёте.

## B6. Завершение

Перенос закончен, когда `$B status` показывает все диапазоны в `listed`, а заданий в `pending`, `claimed` и `retry` нет. Оставшиеся `error` нужно разобрать или выгрузить в отчёт. Отдельный проход по изменённым ключам не нужен, их закрывает обработчик. Обработчик продолжает работать до переключения ELMA365.

# Этап C. Проверка, переключение, откат

Всё делается на `minio2`, haproxy и кластер не меняются. После переключения путь остаётся тем же (`haproxy → nginx minio2:9000 → шлюз`), но через шлюз идут и чтения `s3elma365`, а шлюз отправляет запросы вместо MinIO в новое S3. Другие бакеты nginx по-прежнему шлёт прямо в MinIO. Журнал и обработчик работают в обратную сторону, из нового S3 в MinIO, поэтому откат возможен без потери изменений. Почему так и критерии решения об откате: [docs/rollback.md](docs/rollback.md).

## C0. Подготовка (за несколько дней до окна)

1. **Новое S3 принимает запросы ELMA365 как есть.** В новом S3 бакет `s3elma365` и пользователь с теми же ключами доступа, что в конфиге ELMA365. Порт нового S3 открыт только для `minio2`: тогда писать в него можно только через шлюз. Проверка подписи (Host `elma.example.com`, регион `ru-central-1`, подписанная ссылка):

   ```bash
   cd /opt/elma365_s3_migrator && ELMA_ACCESS_KEY=<ключ ELMA365> ELMA_SECRET_KEY=<секрет> \
     .venv/bin/python scripts/check_new_s3_signature.py --endpoint http://<новое S3>:<порт>
   ```

   Норма: `all passed`. Если хоть один шаг `FAIL`, переключаться нельзя: ELMA365 получит отказ на каждый запрос.

2. **Пользователь MinIO для обратной записи.** Обратный обработчик пишет в MinIO изменения, сделанные после переключения:

   ```bash
   cat > /tmp/s3gw-dst-minio.json <<'EOF2'
   {"Version": "2012-10-17", "Statement": [
     {"Effect": "Allow", "Action": ["s3:ListBucket", "s3:GetBucketLocation", "s3:ListBucketMultipartUploads"], "Resource": ["arn:aws:s3:::s3elma365"]},
     {"Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:GetObjectTagging", "s3:PutObjectTagging",
       "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"], "Resource": ["arn:aws:s3:::s3elma365/*"]}
   ]}
   EOF2
   SECRET2=$(openssl rand -hex 20); echo "$SECRET2"
   mc admin policy create local s3gw-dst-minio /tmp/s3gw-dst-minio.json
   mc admin user add local s3gw-dst-minio "$SECRET2"
   mc admin policy attach local s3gw-dst-minio --user s3gw-dst-minio
   ```

3. **Обратный конфиг** `/etc/s3gw/s3gw-reverse.env` (владелец `s3gw`, права 600): копия `s3gw.env`, в которой

   ```
   GW_UPSTREAM=http://<новое S3>:<порт>
   JOURNAL_DB=/var/lib/s3gw/journal-reverse.db
   # источник теперь новое S3, ключ на чтение
   SRC_ENDPOINT=http://<новое S3>:<порт>
   SRC_ACCESS_KEY=<ключ чтения нового S3>
   SRC_SECRET_KEY=<секрет>
   # приёмник теперь MinIO
   DST_ENDPOINT=http://127.0.0.1:9000
   DST_ACCESS_KEY=s3gw-dst-minio
   DST_SECRET_KEY=<SECRET2>
   ```

   `GW_OTHER_UPSTREAM` не меняется (без строки шлюз берёт `http://127.0.0.1:9000`): если запрос к другому бакету всё же попадёт в шлюз, он уйдёт в MinIO. `GW_READONLY_FILE` должен быть и в прямом, и в обратном конфиге: без него запрет записи в C2 и C4 не действует, и записи окна уходят мимо обратного журнала. Если в действующем `s3gw.env` его ещё нет, добавить и перезапустить шлюз в тихое время (`systemctl restart s3gw-gateway`, обрыв на секунду). Проверка:

   ```bash
   grep -c '^GW_READONLY_FILE=/var/lib/s3gw/read-only$' /etc/s3gw/s3gw.env /etc/s3gw/s3gw-reverse.env   # по 1 в каждом
   ```

4. **Репетиция на стенде** по шагам C2 и C4 с замером времени ([задача 11](tasks/11-cutover-and-finish.md)). Согласовать окно с владельцами ELMA365 и баз данных и назначить, кто принимает решение об откате.

## C1. Проверка полноты и целостности

Сверка независимая (решение владельца, 2026-10-22): она не опирается ни на `bulk.db`, ни на журнал, а заново листит оба бакета и сравнивает `key + size`, а ещё ETag, когда он с обеих сторон без `-` (у одиночного PUT это MD5 и в MinIO, и в SeaweedFS), то есть ловит и другое содержимое того же размера ([docs/tools/stage5-bucket-reconcile.md](docs/tools/stage5-bucket-reconcile.md)). Источник листится один раз: 256 hex-префиксов и 33 промежутка между ними, одним потоком страницами по 100 (умолчания `st5_reconcile_buckets.sh` для старого MinIO). Это ~122 тыс. запросов, порядка 7–17 ч; если листинг A7 со страницей 1000 прошёл без тайм-аутов, можно задать `SRC_LIST_PAGE_SIZE=1000`. Запускать после конца переноса (B6), лучше на ночь, следить за `iostat` и задержками ELMA365, как в A7. Критерии в [задаче 10](tasks/10-completeness-and-integrity-check.md).

Запуск на `minio2` от root (скрипт читает ключи из алиасов `mc`). `verify` в начале очищает результаты прошлого прогона: перед повтором (сбой, прерывание) скопировать `.runtime/st5-reconcile/state.db`, ключи из него тоже нужно перепроверить.

```bash
mc alias set minio_old http://127.0.0.1:9000 s3gw-src "$SECRET"                 # ключ только на чтение из A1
mc alias set minio_new http://<новое S3>:<порт> <ключ чтения нового S3> <секрет>
cd /opt/elma365_s3_migrator
T_RECONCILE=$(date -u +%FT%TZ); echo "$T_RECONCILE"                                # записать
PATH=/opt/elma365_s3_migrator/.venv/bin:$PATH WATERMARK=$T_RECONCILE \
  nohup bash scripts/st5_reconcile_buckets.sh --phase verify > /var/tmp/reconcile.log 2>&1 &
```

`WATERMARK` отсекает в источнике объекты с `LastModified` позже `T_RECONCILE`, а новое S3 листится целиком. Поэтому ключи, которые ELMA365 меняла во время сверки, попадают в отчёт, хотя ошибки нет: созданные или перезаписанные после `T_RECONCILE` как `dst_only` (в источнике отсечены, в новом S3 уже есть), а изменённые, пока шёл листинг, как `missing`, `size_mismatch` или `etag_mismatch` (он входит в `size_mismatch_count`). Поэтому счётчики отчёта (`.runtime/st5-reconcile/report.json`, `missing_count`, `size_mismatch_count`, `dst_only_count`) сами по себе не норма, а `partition_errors` должно быть 0. Все такие ключи сохраняются в `state.db` (`reconcile_queue` и `dst_only`), и их нужно перепроверить по одному, когда отставание обработчика сошло до единиц («Наблюдение за журналом»):

```bash
cd /opt/elma365_s3_migrator && sqlite3 .runtime/st5-reconcile/state.db \
  "SELECT key FROM reconcile_queue UNION SELECT key FROM dst_only" > /var/tmp/reconcile-keys.txt
chmod 644 /var/tmp/reconcile-keys.txt; wc -l < /var/tmp/reconcile-keys.txt
$RUN s3gw.verifykeys --keys-file /var/tmp/reconcile-keys.txt
```

Норма: `mismatched: 0` (временные расхождения сошлись: обе стороны сейчас одинаковы). Ключи из `mismatches` разошлись мимо журнала, это ошибка: выяснить как (запись в MinIO в обход шлюза, откат A4 без трассировки) и записать в «Решения» задачи 10. Затем отдать их обработчику, он приведёт новое S3 к MinIO (докопирует, перезапишет или удалит лишнее):

```bash
$RUN s3gw.verifykeys --keys-file /var/tmp/reconcile-keys.txt --mark-dirty
sleep 60; $RUN s3gw.verifykeys --keys-file /var/tmp/reconcile-keys.txt      # mismatched: 0
```

## C2. Окно переключения

Обозначение для обратного направления:

```bash
JR="sudo -u s3gw sqlite3 -readonly /var/lib/s3gw/journal-reverse.db"
```

1. **Остановить запись.** ELMA365 по возможности в режим обслуживания, и на шлюзе:

   ```bash
   touch /var/lib/s3gw/read-only; date -u +%FT%TZ
   ```

   С этого момента шлюз отвечает `503` на любые изменения, чтения идут. Проверка: загрузка файла в ELMA365 даёт ошибку, новых строк в `intents` нет.

2. **Довести очередь до нуля.**

   ```bash
   systemctl stop s3gw-bulk
   $J "SELECT count(*) FROM intents WHERE result IS NULL"                                                     # 0
   $J "SELECT count(*) FROM key_state WHERE state IN ('dirty','uncertain','syncing','bulk') AND NOT (synced_seq = change_seq AND in_flight = 0)"   # 0
   $J "SELECT key, last_error FROM key_state WHERE state = 'error'"                                           # пусто или каждый разобран
   $J "SELECT count(*) FROM mpu WHERE closed_at IS NULL"
   ```

   Незавершённые multipart-загрузки, начатые до запрета, закончить уже нельзя: их части получают `503`, ELMA365 повторит загрузку после окна.

   Если `result IS NULL` не уходит в 0 за минуту, у намерения не записался результат (сбой записи в журнал, строка в `errors`), и его ключ не отдаётся обработчику. Перезапустить шлюз (`systemctl restart s3gw-gateway`): такие намерения станут `uncertain`, а обработчик сверит их ключи с MinIO через `GW_SETTLE_SECONDS` (15 минут). Заложить это время в окно.

3. **Проверить изменения после сверки:**

   ```bash
   $RUN s3gw.verifykeys --journal /var/lib/s3gw/journal.db --since <T_RECONCILE минус 1 минута>
   ```

   Норма: `checked` больше 0 (примерно число изменений ELMA365 с `T_RECONCILE`; 0 значит ошибку в `--since`), `mismatched: 0`, код возврата `0`. Если есть расхождения: тот же запуск с `--mark-dirty`, дождаться нулевого отставания по запросу шага 2 (обработчик ещё работает) и повторить без `--mark-dirty`.

   Окно короче, если до него прогнать эту проверку с `--since <T_RECONCILE − 1 мин>`, а в окне — только с `--since <начало того прогона − 1 мин>`.

4. **Переключить шлюз на новое S3:**

   ```bash
   systemctl stop s3gw-handler
   cp -p /etc/s3gw/s3gw.env /etc/s3gw/s3gw-forward.env
   cp -p /etc/s3gw/s3gw-reverse.env /etc/s3gw/s3gw.env
   systemctl restart s3gw-gateway
   systemctl enable --now s3gw-handler
   # чтения s3elma365 тоже в шлюз, а он ходит в новое S3
   sed -i -E 's/^(    (GET|HEAD|OPTIONS) +)s3gw_minio;/\1s3gw_gateway;/' /etc/nginx/conf.d/s3gw.conf
   grep -cE '^ +(GET|HEAD|OPTIONS) +s3gw_minio;' /etc/nginx/conf.d/s3gw.conf     # 0
   nginx -t && nginx -s reload
   ```

   Проверка: `$JR "SELECT epoch, started_at FROM gateway_epochs"` даёт одну строку; загрузка файла в ELMA365 по-прежнему даёт ошибку (запрет записи действует и в обратном конфиге); ELMA365 открывает старый файл (теперь он читается из нового S3 через шлюз); другие бакеты по-прежнему отвечает MinIO: `curl -sI http://10.10.0.12:9000/elma365-loki/ | grep -i '^server'` даёт `Server: MinIO`. С этого момента мгновенный откат из A4 запрещён: он отправил бы запись в MinIO, а чтения остались бы в новом S3.

5. **Снять запрет записи и проверить:**

   ```bash
   rm /var/lib/s3gw/read-only; date -u +%FT%TZ          # T_CUTOVER, записать
   ```

   Загрузить в ELMA365 тестовый файл, затем удалить. В `$JR "SELECT key, op, result FROM intents ORDER BY id DESC LIMIT 5"` намерения с `success`. Обратный обработчик перенёс их в MinIO: отставание по `$JR` равно 0. Проверить предпросмотр, большие файлы, подписанные ссылки ([задача 11](tasks/11-cutover-and-finish.md)).

**Если что-то не так до шага 5,** вернуться без потерь: в новое S3 ещё ничего не записано. Если шаг 4 ещё не начинали, достаточно `rm /var/lib/s3gw/read-only`; команды ниже — после шага 4 (файл `s3gw-forward.env` создаётся в нём):

```bash
systemctl stop s3gw-handler
sed -i -E 's/^(    (GET|HEAD|OPTIONS) +)s3gw_gateway;/\1s3gw_minio;/' /etc/nginx/conf.d/s3gw.conf && nginx -t && nginx -s reload
cp -p /etc/s3gw/s3gw-forward.env /etc/s3gw/s3gw.env
systemctl restart s3gw-gateway; systemctl start s3gw-handler
rm /var/lib/s3gw/read-only
```

## C3. После переключения

- Обратное отставание и ошибки смотреть так же, как в разделе «Наблюдение за журналом», но с `$JR` вместо `$J`. Бэкап журнала (A5) сам переходит на `journal-reverse.db`, потому что берёт `JOURNAL_DB` из `s3gw.env`.
- Трассировка MinIO (A6) больше не нужна: в `s3elma365` MinIO пишет только обратный обработчик через `127.0.0.1`, а снаружи MinIO доступен только через nginx, который шлёт `s3elma365` в шлюз.
- Через `minio2` и шлюз теперь идёт весь трафик `s3elma365` к новому хранилищу, включая чтения: следить за сетью и CPU `minio2` и за `s3gw-5xx.log`.

## C4. Откат

Решение принимается по критериям [docs/rollback.md](docs/rollback.md), раздел 3.

1. **Остановить запись:** `touch /var/lib/s3gw/read-only`. Проверка: загрузка файла в ELMA365 даёт ошибку, новых строк в `$JR "SELECT max(id) FROM intents"` нет.
2. **Довести обратную очередь до нуля:** те же три запроса, что в C2 шаг 2, с `$JR`.
3. **Проверить изменения после переключения:**

   ```bash
   $RUN s3gw.verifykeys --journal /var/lib/s3gw/journal-reverse.db --since <T_CUTOVER минус 1 минута>
   ```

   Норма: `checked` больше 0, `mismatched: 0`. Здесь источник — новое S3, приёмник — MinIO. При расхождениях: тот же запуск с `--mark-dirty`, дождаться нулевого обратного отставания и повторить.

4. **Вернуть шлюз на MinIO со свежим журналом:**

   ```bash
   systemctl disable --now s3gw-handler
   cp -p /etc/s3gw/s3gw-forward.env /etc/s3gw/s3gw.env
   sed -i 's#^JOURNAL_DB=.*#JOURNAL_DB=/var/lib/s3gw/journal-after-rollback.db#' /etc/s3gw/s3gw.env
   systemctl restart s3gw-gateway
   # чтения s3elma365 снова прямо в MinIO
   sed -i -E 's/^(    (GET|HEAD|OPTIONS) +)s3gw_gateway;/\1s3gw_minio;/' /etc/nginx/conf.d/s3gw.conf && nginx -t && nginx -s reload
   systemctl is-active s3gw-handler   # inactive: обратный обработчик не перезапишет MinIO
   rm /var/lib/s3gw/read-only
   rm /etc/s3gw/s3gw-forward.env      # устарел (JOURNAL_DB=journal.db); при повторе C2 создаётся заново
   ```

   Проверка: ELMA365 открывает старый файл и файл, созданный после переключения; загрузка и удаление тестового файла проходят, в новом журнале есть намерения.

После отката система в состоянии этапа A: MinIO основной, журнал пишется, обработчик выключен. Повторная попытка переключения начинается со сверки (C1), потому что новое S3 и MinIO с этого момента расходятся.

## C5. Завершение (после срока поддержки отката)

Решено 2026-10-22 (владелец): haproxy не меняется. Когда основной перенос закончен и срок отката прошёл, адрес `minio2` (`10.10.0.12`) вручную переносится на новый физический сервер. На нём на время настраивается прокси на старый MinIO только для чтения: `GET` и `HEAD` объекта `s3elma365`, которого нет в новом S3 (`404`), повторяются в старом MinIO; запись и удаление идут только в новое S3. Шлюз, журнал и обратный обработчик при этом выводятся. Это делает владелец после завершения переноса, отдельно от этого конвейера.

Что учесть перед переносом адреса:

- файл, удалённый в ELMA365 после выключения обратного обработчика, остаётся в старом MinIO, и прокси снова отдаст его по прямой ссылке. Поэтому каждое обращение прокси к старому MinIO писать в лог (ключ, время) и держать прокси недолго: после чистой сверки (C1) в новом S3 есть всё, и попадание в старый MinIO означает либо такой удалённый файл, либо пропуск, который нужно разобрать;
- другие бакеты (`elma365-backups`, `-loki`, `-tempo`) не переносятся и не проксируются: после переноса адреса бэкапы и логи пишутся уже в новое хранилище (их бакеты создать там заранее), старые остаются в старом MinIO, пока он работает;
- обратная очередь пуста (`$JR`, как в C4 шаг 2), иначе изменения последних минут не попадут в MinIO, а он остаётся запасным источником;
- после выключения обратного обработчика откат возможен только через полную сверку и перенос в обратную сторону;
- старый MinIO переводится на другой адрес и остаётся доступным только для прокси нового сервера; в `s3elma365` старого MinIO больше никто не пишет;
- на старом сервере убрать `/etc/nginx/conf.d/s3gw.conf` (`nginx -t && systemctl reload nginx`) и вернуть MinIO на адрес, доступный прокси нового сервера: `127.0.0.1:9000` снаружи не виден.

Старый MinIO работает ещё около месяца после переноса адреса (для прокси и старых бэкапов и логов), в `s3elma365` в нём никто не пишет; затем выводится. Остальное (срок поддержки отката) описано в [docs/rollback.md](docs/rollback.md), раздел 4, и [задаче 11](tasks/11-cutover-and-finish.md).

# Наблюдение за журналом

На всех этапах.

| Что | Запрос | Норма |
|-----|--------|-------|
| Отставание обработчика | `$J "SELECT count(*), min(updated_at) FROM key_state WHERE state IN ('dirty','uncertain','syncing','bulk') AND NOT (synced_seq = change_seq AND in_flight = 0)"` | на этапе A растёт (обработчика нет), на этапе B единицы ключей, возраст до минут |
| Ключи по состояниям | `$J "SELECT state, count(*) FROM key_state GROUP BY state"` | |
| Ключи в ошибке | `$J "SELECT key, attempts, last_error FROM key_state WHERE state = 'error'"` | пусто |
| Незакрытые намерения | `$J "SELECT count(*) FROM intents WHERE result IS NULL"` | только текущие запросы |
| Ошибки компонентов | `$J "SELECT at, component, kind, key, detail FROM errors ORDER BY id DESC LIMIT 20"` | редкие временные |
| Счётчики | `$J "SELECT name, value FROM counters"` | |
| Место | `df -h /var/lib/s3gw; du -sh /var/lib/s3gw` | |
| Копия журнала (A5) | `ls -lt /backup/s3gw \| head -3; systemctl is-active s3gw-journal-backup` | свежая не старше 2 ч; юнит не висит в `activating` |
| Ошибки на входе | `wc -l /var/log/nginx/s3gw-5xx.log; tail /var/log/nginx/s3gw-5xx.log` | не растёт; `502` — шлюз недоступен, `503` — журнал недоступен |

Логи: `journalctl -u s3gw-gateway -u s3gw-handler -u s3gw-bulk --since '1 hour ago'`.

# Сбои и восстановление

- **Шлюз упал или перезапущен.** systemd поднимает его за секунду. Новая эпоха переводит незакрытые намерения старой в `uncertain`, обработчик берёт их через `GW_SETTLE_SECONDS` (900 с) и сверяет с MinIO. Вручную ничего делать не нужно.
- **Обработчик остановлен.** Запись в ELMA365 продолжает работать, растёт только отставание. После запуска очередь дорабатывается из журнала. Копирование при этом встанет на паузу по `journal_lag`.
- **Перенос остановлен.** Ни на что не влияет. После запуска продолжает с контрольных точек.
- **Поток обработчика или переноса упал.** Временную ошибку журнала (`database is locked` дольше тайм-аута) поток обработчика пишет в лог и повторяет после паузы. На любой другой ошибке процесс останавливает остальные потоки и завершается с ошибкой, трассировка в `journalctl -u s3gw-handler` или `-u s3gw-bulk`. Задания упавшего потока переноса возвращаются в `retry`. systemd перезапускает обработчик через 5 с, перенос через 30 с (код 3); перенос продолжает с контрольных точек. Код 2 у переноса означает, что он закончил с ошибками заданий (раздел B5), его systemd не перезапускает.
- **ELMA365 получает 502 или 503 на запись** (`/var/log/nginx/s3gw-5xx.log`). `502` — шлюз недоступен, `503` — шлюз не может записать намерение (диск, блокировка журнала). До переключения чтения при этом работают. Проверить `systemctl status s3gw-gateway`, свободное место на `/var/lib/s3gw`, последние строки `errors`. Если быстро не чинится, сделать мгновенный откат в nginx с трассировкой (раздел A4, «Откат») и после возврата сдать изменения журналу (`--mark-dirty`). После переключения (C2) мгновенного отката нет, только C4.
- **nginx не отвечает.** ELMA365 и другие сервисы теряют MinIO целиком (чтения тоже). `systemctl status nginx`, `nginx -t`, `systemctl restart nginx`. Мимо журнала при этом ничего не пишется: MinIO снаружи недоступен.
- **Журнал потерян или повреждён.** Восстановить из копии (раздел A5).
- **Ключи в `error`.** Это ключи, у которых исчерпаны попытки. Разобрать причину по `last_error`, устранить её, затем вернуть ключи в работу. Править пачками по `rowid`, чтобы не держать блокировку журнала дольше `journal_timeout` шлюза (2 с), иначе ELMA365 получит `503`:

  ```bash
  JW="sudo -u s3gw sqlite3 /var/lib/s3gw/journal.db"
  MAX=$($J "SELECT max(rowid) FROM key_state")
  for s in $(seq 0 50000 $MAX); do
    $JW "UPDATE key_state SET state = 'dirty', attempts = 0, not_before = 0, lease_owner = NULL WHERE state = 'error' AND rowid BETWEEN $s AND $((s + 49999))"
  done
  ```
