# Конвейер миграции: журнал и перенос

Миграция идёт двумя процессами, которые работают параллельно на `minio2` (10.10.0.12) рядом с MinIO и делят одну базу состояния ключей.

```
процесс 1: журнал изменений
ELMA365 ─► ingress ─► haproxy ─► nginx 10.10.0.12:9000
                                   ├─ чтения s3elma365, другие бакеты ──────────► MinIO 127.0.0.1:9000
                                   └─ изменения s3elma365 ─► шлюз 127.0.0.1:9110 ─► MinIO
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

Если новое S3 появится раньше, чем пройден этап A, шаги этапа A выполняются так же, только вместо листинга A7 сразу выполняется B1: обработчик и `run`, который сам пролистает бакет и скопирует.

Все команды выполняются на `minio2` от root, если не сказано иное. Каждый шаг заканчивается проверкой. Если проверка не прошла, дальше не идти.

```bash
J="sudo -u s3gw sqlite3 -readonly /var/lib/s3gw/journal.db"   # только чтение и только от s3gw: root оставил бы -wal/-shm, которые шлюз не откроет
RUN="systemd-run --wait --pipe -q -p User=s3gw -p EnvironmentFile=/etc/s3gw/s3gw.env -p WorkingDirectory=/opt/elma365_s3_migrator /opt/elma365_s3_migrator/.venv/bin/python -m"   # утилита s3gw с конфигом юнитов, из любого каталога
B="$RUN s3gw.bulk --bulk-db /var/lib/s3gw/bulk.db"
CUT="bash /opt/elma365_s3_migrator/scripts/cutover.sh"   # окно переключения и откат (этап C)
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
  ss -ltnp | grep -E ':(9000|9001|9080|9110)\b'               # сейчас :9000 у minio, 9080 и 9110 свободны (9100 часто занят node_exporter, поэтому шлюз на 9110)
  ss -tn state established '( sport = :9000 )' | awk 'NR>1 {print $3}' | sort | uniq -c   # на какой адрес minio2 приходят клиенты
  ```

  Все клиенты должны приходить на `10.10.0.12:9000` или `127.0.0.1:9000`. Если есть подключения на другой адрес `minio2`, его нужно добавить в nginx отдельным `listen` (A4), иначе эти клиенты после включения потеряют MinIO. Если MinIO запущен не через systemd (например, в Docker), сказать нам: смену адреса MinIO в A4 нужно будет делать иначе.
- MinIO на `minio2` один узел: `mc admin info $ALIAS` показывает один сервер. Если узлов несколько, остановиться и сказать нам: после A4 MinIO слушает только `127.0.0.1`, и узлы друг друга не увидят.
- Другие бакеты (`elma365-backups`, `-loki`, `-tempo`) в миграцию не входят. nginx отправляет их запросы прямо в MinIO, шлюз в них не участвует ни на одном этапе, и после переключения тоже. Посмотреть, кто к ним ходит и сколько: `timeout 300 mc admin trace --json --call s3 $ALIAS | grep -o '"path": *"/[^/"]*' | sort | uniq -c`.
- Доступ к кластеру Kubernetes не нужен: всё делается на `minio2`, haproxy не меняется.
- Записать в «Решения» задачи 08 (открытые вопросы из [задачи 13](tasks/13-remaining-verification.md)):

  ```bash
  grep -E 'MINIO_SERVER_URL|MINIO_BROWSER|console-address' /etc/default/minio; ss -ltn | grep -c ':9001\b'   # консоль MinIO: включена ли, куда ходит
  df -h /var/lib $(test -d /backup && echo /backup)          # одна ли файловая система у журнала и его копий; свободно ли 15 ГБ (A2)
  dd if=/dev/zero of=/var/lib/s3gw-fsync.test bs=4k count=500 oflag=dsync 2>&1 | tail -1; rm -f /var/lib/s3gw-fsync.test
  ```

  Последняя строка — скорость fsync системного SSD: 500 записей с fsync, время делить на 500. Шлюз на каждое изменение делает два fsync журнала. Норма — единицы миллисекунд на fsync; на стенде было 4 мс.

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
# Любой листинг 12 млн ключей в корне бакета идёт долго (mc ls, mc stat отсутствующего ключа, даже max-keys=1).
# Права MinIO проверяет до листинга, поэтому отказ пришёл бы сразу.
A=(-s -m 10 --aws-sigv4 "aws:amz:ru-central-1:s3" --user "s3gw-src:$SECRET")
curl "${A[@]}" -I -o /dev/null -w 'HEAD %{http_code}\n' http://127.0.0.1:9000/s3elma365/_ro_check    # HEAD 404: чтение есть (403 — нет прав, 400 — другой регион)
curl "${A[@]}" -o /dev/null -w 'LIST %{http_code}\n' "http://127.0.0.1:9000/s3elma365?list-type=2&max-keys=1"   # LIST 200 или 000 (тайм-аут: MinIO листает); 403 — нет прав
echo x | mc pipe s3gwro/s3elma365/_ro_check   # отказ: mc 2025 пишет Insufficient permissions, старые — Access Denied
```

## A2. Установить шлюз

Журнал лежит на системном SSD (`/var/lib/s3gw`, по замеру 5,2 ГБ на 12 млн ключей вместе с `bulk.db`), его копии в `/backup/s3gw` (раздел A5).

```bash
useradd --system --home /var/lib/s3gw --shell /usr/sbin/nologin s3gw
install -d -o s3gw -m 700 /var/lib/s3gw /etc/s3gw /backup/s3gw
git clone https://github.com/laktes1/elma365_s3_migrator /opt/elma365_s3_migrator
python3 -m venv /opt/elma365_s3_migrator/.venv && /opt/elma365_s3_migrator/.venv/bin/pip install -r /opt/elma365_s3_migrator/requirements.txt
install -o s3gw -m 600 /opt/elma365_s3_migrator/deploy/systemd/s3gw.env.example /etc/s3gw/s3gw.env
cp /opt/elma365_s3_migrator/deploy/systemd/s3gw-*.service /opt/elma365_s3_migrator/deploy/systemd/s3gw-*.timer /etc/systemd/system/ && systemctl daemon-reload
git -C /opt/elma365_s3_migrator log -1 --format='%h %s'   # записать версию в «Решения» задачи 08
```

В `/etc/s3gw/s3gw.env` заполнить `SRC_ACCESS_KEY=s3gw-src`, `SRC_SECRET_KEY=<SECRET>`, а строки `DST_*` закомментировать, пока нового S3 нет. `GW_LISTEN=127.0.0.1:9110` оставить как в примере: к шлюзу ходит только nginx на этой же машине, снаружи он недоступен. `GW_OTHER_UPSTREAM=http://127.0.0.1:9000` тоже оставить (это же значение берётся, если строки нет): nginx другие бакеты в шлюз не шлёт, это страховка. Затем запустить шлюз и часовую копию журнала:

```bash
systemctl enable --now s3gw-gateway s3gw-journal-backup.timer   # s3gw-handler не включать до этапа B
```

Проверка:

```bash
systemctl is-active s3gw-gateway s3gw-journal-backup.timer                        # active, active
systemctl is-enabled s3gw-handler                                                 # disabled
curl -s -o /dev/null -w '%{http_code}\n' --retry 10 --retry-connrefused --retry-delay 1 http://127.0.0.1:9110/minio/health/live   # 200: шлюз поднялся (ждёт до 10 с), запрос дошёл до MinIO
$RUN s3gw.state | jq .epoch                                                       # epoch 1, started_at сейчас
systemctl start s3gw-journal-backup; ls -l /var/lib/s3gw/ /backup/s3gw/           # journal.db, -wal, -shm принадлежат s3gw; первая копия
df -h /var/lib/s3gw /backup/s3gw     # если файловая система одна: свободно не меньше 15 ГБ (к концу переноса журнал 2,4 ГБ и bulk.db 2,8 ГБ, KEEP=2 копии журнала и временная)
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
$RUN s3gw.state | jq .recent_errors                                              # []
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

При перезагрузке `minio2` порядок запуска не важен: MinIO снаружи недоступен, а пока шлюз не поднялся, запись получает `502`. Что поднимается само, а что запускать снова: «Сбои и восстановление», «Перезагрузка `minio2`».

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
   $RUN s3gw.state | jq '{open_intents, stuck_intents}'     # 0 или единицы (текущие запросы); stuck_intents 0
   ```

5. Ошибок нет: в `/var/log/nginx/s3gw-5xx.log` и `/var/log/nginx/s3gw-error.log` пусто (или файлов ещё нет). Допустимы только строки из секунд между двумя `date -Is` включения: MinIO уже перезапускался, а nginx уже слушал. Также `journalctl -u s3gw-gateway --since '-10 min' | grep -c -i -E 'error|failed'` даёт `0`.
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

Таймер включается в A2 вместе со шлюзом; с A4 журнал единственная запись изменений. Копия снимается раз в час от пользователя `s3gw` (`VACUUM INTO`: одна читающая транзакция, запись шлюза и bulk не мешает), хранятся последние 2 (`KEEP` и `BACKUP_DIR` в `s3gw-journal-backup.service`). К концу переноса журнал вырастает до 2–3 ГБ, копия снимается секунды (замер на 12 млн ключей: 2,4 ГБ за 4 с на NVMe). Другого диска, кроме дисков MinIO, на `minio2` нет, поэтому `/backup` обычно лежит на том же системном SSD (свободно 35 ГБ), что и журнал: копия спасает от порчи файла, но не от отказа диска. Свежесть копии смотреть в «Наблюдении за журналом».

Восстановление, если `journal.db` потерян или повреждён (до переключения C2):

1. Сделать мгновенный откат в nginx с трассировкой (раздел A4, «Откат»).
2. Остановить всё, что пишет в журнал, подложить последнюю копию и запустить шлюз. Испорченный журнал уходит в сторону вместе с `-wal` и `-shm`: чужой WAL рядом с копией SQLite применит к ней и испортит её.

   ```bash
   systemctl stop s3gw-handler s3gw-bulk s3gw-journal-backup.timer && systemctl stop s3gw-gateway
   BROKEN=/var/lib/s3gw/broken-$(date +%F-%H%M); mkdir "$BROKEN" && mv /var/lib/s3gw/journal.db* "$BROKEN"/
   C=$(ls -1t /backup/s3gw/journal-2*.db | head -1); echo "$C"
   install -o s3gw -m 600 "$C" /var/lib/s3gw/journal.db
   systemctl start s3gw-gateway s3gw-journal-backup.timer
   curl -s -o /dev/null -w '%{http_code}\n' --retry 10 --retry-connrefused --retry-delay 1 http://127.0.0.1:9110/minio/health/live   # 200: шлюз открыл копию
   $RUN s3gw.state | jq '{epoch: .epoch.epoch, journal}'      # эпоха копии плюс 1
   ```

3. Вернуть запись в шлюз (раздел A4, «Откат», вторая часть, с `--mark-dirty`).
4. Этап A: начать инвентаризацию заново (раздел A7, «Начать заново»). Этап B: `systemctl start s3gw-handler s3gw-bulk`; после переноса обязательна полная сверка (C1).

`tracecheck` в этом случае покажет `missed`: это изменения, прошедшие через шлюз после снятия копии, их намерения были только в потерянном журнале. `--mark-dirty` отдаёт обработчику и эти ключи. Изменения с момента копии до начала трассировки журнал потерял. Поэтому на этапе A начать инвентаризацию заново (раздел A7), а на этапе B обязательна полная сверка (C1). Сверка не сравнивает теги, а MinIO при их смене не меняет `LastModified`. Изменение только тегов, которое обработчик не перенёс до сбоя, останется в новом S3 старым. Это важно, только если ELMA365 пользуется тегами (раздел A6, `by_call`).

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
- `missed` не пуст: запрос прошёл через шлюз, а намерения для ключа нет. Это ошибка шлюза (кроме проверки после восстановления журнала из копии, раздел A5); прислать вывод и строки `$J "SELECT * FROM errors ORDER BY id DESC LIMIT 20"`. Затем отдать ключи обработчику тем же запуском с `--mark-dirty` и начать инвентаризацию заново (раздел A7), если она уже идёт.
- Для пакетного удаления (`s3.DeleteMultipleObjects`) ключи лежат в теле запроса, которого в трассировке нет, поэтому скрипт проверяет только, что у запроса есть намерения. Поключевой охват пакетного удаления проверен тестами шлюза и на стенде (400 ключей).
- Теги переносятся (`COPY_TAGS=1`, политика из A1 это разрешает), но запрос тегов идёт только к объектам, у которых они есть: GET из MinIO сообщает их число (`x-amz-tagging-count`). Для остальных лишнего запроса к дискам нет.
- Есть ли в `by_call` вызовы `s3.PutObjectTagging` и `s3.DeleteObjectTagging`, записать в «Решения» задачи 08. Если есть, ELMA365 пользуется тегами, а сверка C1 их не сравнивает (раздел A5, восстановление журнала).

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

В обоих случаях после разбора: `systemctl reset-failed s3gw-list` и снова команда запуска выше; листинг продолжит с контрольных точек. Время окончания и скорость записать в «Решения» задачи 09: это первая реальная оценка скорости источника. Туда же — самые крупные объекты. Загрузку nginx и шлюз ждут до 3600 с, этого хватает на 3,6 ГБ при 1 МБ/с от браузера:

```bash
sudo -u s3gw sqlite3 -readonly /var/lib/s3gw/bulk.db "SELECT size, key FROM jobs ORDER BY size DESC LIMIT 5"
```

По итогам листинга, ещё до нового S3, согласовать нагрузку копирования на этапе B. Нужны порог деградации ELMA365 (например, p95 задержки GET) и дневное и ночное окно. Записать их в `/etc/s3gw/s3gw.env` в кавычках: `SCHEDULE="08:00-20:00 workers=2 mbps=200; 20:00-08:00 workers=8 mbps=800"`, часовой пояс задаётся в `SCHEDULE_TZ`. Без кавычек systemd строку поймёт, а `. s3gw.env` в bash (B4) — нет. Начинать с консервативных лимитов: `await` дисков и `page_ms_last_5m` листинга уже показали, сколько выдерживают HDD MinIO.

**Начать заново** (после `missed`, потери журнала или отката без трассировки): листинг не знает, что ключи в уже пройденных диапазонах могли появиться мимо журнала.

```bash
systemctl stop s3gw-list s3gw-bulk 2>/dev/null
mkdir -p /var/lib/s3gw/old && mv /var/lib/s3gw/bulk.db* /var/lib/s3gw/old/
```

Затем запуск, как выше.

# Этап B. Новое S3: обработчик и копирование

## B1. Запуск обработчика и копирования

Листинг из A7 должен быть закончен или остановлен (`systemctl is-active s3gw-list` даёт `inactive`): второй процесс `bulk` на том же `bulk.db` не стартует. Раскомментировать `DST_*` в `/etc/s3gw/s3gw.env` (ключ с записью только в один бакет нового S3); `SCHEDULE` согласован в конце A7. Затем:

```bash
systemctl enable --now s3gw-handler && systemctl start s3gw-bulk   # run: листинг готов, сразу copy; продолжает с контрольных точек
```

Обработчик начинает разбирать ключи, накопленные журналом на этапе A. Ждать его вручную не нужно, копирование подождёт само. Пока отставание больше `JOURNAL_BACKLOG_MAX`, у него 0 потоков и `limits.reason` = `journal_lag`. Ключи, которые есть в журнале, копирование и так не трогает (`skipped_journal`).

Проверка в первые минуты:

```bash
$RUN s3gw.state | jq '{backlog, backlog_oldest, error_keys}'   # backlog уменьшается, error_keys 0
jq .limits /var/lib/s3gw/bulk-status.json                      # reason journal_lag, затем schedule
```

Наблюдение:

```bash
cat /var/lib/s3gw/bulk-status.json          # текущее состояние, перезаписывается каждые 10 с
iostat -x 5 3                               # нагрузка на диски MinIO
journalctl -u s3gw-bulk -f
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

## B2. Управление без перезапуска

```bash
$B pause                    # workers = 0, причина paused
$B resume
$B set workers_max 4        # потолок потоков поверх расписания
$B set mbps_max 300         # потолок полосы
```

Причина текущего лимита видна в `bulk-status.json` (`limits.reason`): `schedule`, `manual`, `paused`, `journal_lag`.

## B3. Ошибки переноса

Задание или диапазон листинга, у которых `BULK_MAX_ATTEMPTS` (10) попыток подряд не удались, переходит в `error`; паузы между попытками 2, 4, 8 ... с, всего около 17 минут. Перенос доделывает остальное и завершается с кодом 2, systemd его не перезапускает.

```bash
$B export-errors --out /var/lib/s3gw/bulk-errors.jsonl   # или .csv: ключ, попытки, last_error
$B retry-errors                                          # после устранения причины: error -> retry, попытки с нуля
systemctl start s3gw-bulk                                # доделывает только вернувшиеся задания и диапазоны
```

`source_missing` означает, что ключ удалили из MinIO после листинга. Это не ошибка, такие ключи перечисляются в отчёте.

## B4. Завершение

Перенос закончен, когда `$B status` показывает все диапазоны в `listed`, а заданий в `pending`, `claimed` и `retry` нет. Оставшиеся `error` нужно разобрать или выгрузить в отчёт. Отдельный проход по изменённым ключам не нужен, их закрывает обработчик. Обработчик продолжает работать до переключения ELMA365.

Обработчик или перенос, убитые посреди копирования крупного объекта, оставляют в новом S3 незавершённую multipart-загрузку: данных в ней нет, только занятое место. Убрать те, что старше суток (свежие могут быть копированием работающего обработчика):

```bash
( set -a; . /etc/s3gw/s3gw.env; mc alias set s3gwdst "$DST_ENDPOINT" "$DST_ACCESS_KEY" "$DST_SECRET_KEY" >/dev/null )
mc ls --incomplete --recursive s3gwdst/s3elma365 | wc -l
mc rm --incomplete --recursive --force --older-than 1d s3gwdst/s3elma365
```

# Этап C. Проверка, переключение, откат

Всё делается на `minio2`, haproxy и кластер не меняются. После переключения путь остаётся тем же (`haproxy → nginx minio2:9000 → шлюз`), но через шлюз идут и чтения `s3elma365`, а шлюз отправляет запросы вместо MinIO в новое S3. Другие бакеты nginx по-прежнему шлёт прямо в MinIO. Журнал и обработчик работают в обратную сторону, из нового S3 в MinIO, поэтому откат возможен без потери изменений. Почему так и критерии решения об откате: [docs/rollback.md](docs/rollback.md).

## C0. Подготовка (за несколько дней до окна)

1. **Новое S3 принимает запросы ELMA365 как есть.** В новом S3 бакет `s3elma365` и пользователь с теми же ключами доступа, что в конфиге ELMA365. Порт нового S3 открыт только для `minio2`: тогда писать в него можно только через шлюз. Установка, TLS, проброс через внешний адрес площадки и доверие `minio2` к сертификату (без него boto3 отвечает `CERTIFICATE_VERIFY_FAILED`): [docs/new-s3-install.md](docs/new-s3-install.md). Проверка подписи (Host `elma.example.com`, регион `ru-central-1`, подписанная ссылка):

   ```bash
   cd /opt/elma365_s3_migrator && ELMA_ACCESS_KEY=<ключ ELMA365> ELMA_SECRET_KEY=<секрет> \
     .venv/bin/python scripts/check_new_s3_signature.py --endpoint https://<новое S3>:<порт>
   ```

   Норма: `all passed`. Если хоть один шаг `FAIL`, переключаться нельзя: ELMA365 получит отказ на каждый запрос. Последний шаг проверяет, что новое S3 отвергает запись с чужим `Content-MD5` (`BadDigest`). На этом держится проверка содержимого при переносе крупных объектов: их ETag сверка C1 не сравнивает. Если шаг `FAIL`, сказать нам.

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

3. **Обратный конфиг** `/etc/s3gw/s3gw-reverse.env`: тот же `s3gw.env`, в котором заменены строки из [s3gw-reverse.overrides.example](deploy/systemd/s3gw-reverse.overrides.example). Шлюз и источник смотрят на новое S3, журнал свой (`journal-reverse.db`), приёмник — MinIO с ключом из шага 2. Остальное, в том числе `GW_OTHER_UPSTREAM` и `GW_READONLY_FILE`, берётся из `s3gw.env` как есть.

   ```bash
   cd /etc/s3gw
   install -o s3gw -m 600 /opt/elma365_s3_migrator/deploy/systemd/s3gw-reverse.overrides.example s3gw-reverse.overrides
   vi s3gw-reverse.overrides     # адрес нового S3, ключ чтения нового S3, SECRET2
   ```

   Собрать; повторять после каждой правки `s3gw.env` или `s3gw-reverse.overrides`:

   ```bash
   cd /etc/s3gw && K=$(grep -oE '^[A-Z_]+=' s3gw-reverse.overrides | paste -sd'|')
   (umask 077; { grep -vE "^($K)" s3gw.env; cat s3gw-reverse.overrides; } > s3gw-reverse.env) && chown s3gw s3gw-reverse.env
   diff s3gw.env s3gw-reverse.env     # отличаются только строки из s3gw-reverse.overrides
   ```

   `GW_READONLY_FILE` должен быть в `s3gw.env`: без него запрет записи в C2 и C4 не действует, и записи окна уходят мимо обратного журнала. Если его ещё нет, добавить, перезапустить шлюз в тихое время и пересобрать обратный конфиг. При перезапуске (`systemctl restart s3gw-gateway`) загрузки в полёте обрываются, до `GW_SHUTDOWN_TIMEOUT` (5 с) запись получает `502`, ELMA365 повторит. Проверка:

   ```bash
   grep -c '^GW_READONLY_FILE=/var/lib/s3gw/read-only$' /etc/s3gw/s3gw.env /etc/s3gw/s3gw-reverse.env   # по 1 в каждом
   ```

4. **Репетиция на стенде** по шагам C2 и C4 с замером времени ([задача 11](tasks/11-cutover-and-finish.md)). `python scripts/stand_runbook.py` дословно выполняет этот runbook на systemd-реплике `minio2`, в том числе C2 и C4 через `scripts/cutover.sh`. Время каждого шага и всего окна стенд пишет в `.runtime/stand-runbook/report.json` (`blocks[].seconds`, `c2_window_seconds`). С `--keep` реплика остаётся работать, и шаги можно пройти руками: `docker exec -it rb-minio2 bash`, затем `. /root/.runbook-session` (переменные сессии: `$CUT`, `$RUN`, `$J`, `T_RECONCILE` и другие). Согласовать окно с владельцами ELMA365 и баз данных и назначить, кто принимает решение об откате.

## C1. Проверка полноты и целостности

Сверка независимая (решение владельца, 2026-10-22): она не опирается ни на `bulk.db`, ни на журнал, а заново листит оба бакета и сравнивает `key + size`, а ещё ETag, когда он с обеих сторон без `-` (у одиночного PUT это MD5 и в MinIO, и в SeaweedFS), то есть ловит и другое содержимое того же размера ([docs/tools/stage5-bucket-reconcile.md](docs/tools/stage5-bucket-reconcile.md)). Теги и метаданные она не сравнивает. Источник листится один раз: 256 hex-префиксов и 33 промежутка между ними, одним потоком страницами по 100 (умолчания `st5_reconcile_buckets.sh` для старого MinIO). Это ~122 тыс. запросов, порядка 7–17 ч; если листинг A7 со страницей 1000 прошёл без тайм-аутов, можно задать `SRC_LIST_PAGE_SIZE=1000`. Запускать после конца переноса (B4), лучше на ночь, следить за `iostat` и задержками ELMA365, как в A7. Критерии в [задаче 10](tasks/10-completeness-and-integrity-check.md).

Запуск на `minio2` от root (скрипт читает ключи из алиасов `mc`). Новый запуск начинает с нуля и стирает результаты прошлого.

```bash
mc alias set minio_old http://127.0.0.1:9000 s3gw-src "$SECRET"                 # ключ только на чтение из A1
mc alias set minio_new https://<новое S3>:<порт> <ключ чтения нового S3> <секрет>
cd /opt/elma365_s3_migrator
T_RECONCILE=$(date -u +%FT%TZ); echo "$T_RECONCILE"                                # записать
PATH=/opt/elma365_s3_migrator/.venv/bin:$PATH WATERMARK=$T_RECONCILE \
  nohup bash scripts/st5_reconcile_buckets.sh --phase verify > /var/tmp/reconcile.log 2>&1 &
```

Если сверка прервалась (сбой, перезагрузка, `kill`), её можно продолжить: готовые разделы не листятся заново. Момент `T_RECONCILE` берётся из `state.db` прерванного прогона. Пока прерванная сверка ещё жива (например, после обрыва SSH её держит `nohup`; проверить: `pgrep -f reconcile_buckets.py`), вторая на той же `state.db` не стартует и пишет `another reconcile is working`.

```bash
cd /opt/elma365_s3_migrator && PATH=/opt/elma365_s3_migrator/.venv/bin:$PATH RESUME=1 \
  nohup bash scripts/st5_reconcile_buckets.sh --phase verify >> /var/tmp/reconcile.log 2>&1 &
```

`WATERMARK` отсекает в источнике объекты с `LastModified` позже `T_RECONCILE`, а новое S3 листится целиком. Поэтому ключи, которые ELMA365 меняла во время сверки, попадают в отчёт, хотя ошибки нет: созданные или перезаписанные после `T_RECONCILE` как `dst_only` (в источнике отсечены, в новом S3 уже есть), а изменённые, пока шёл листинг, как `missing`, `size_mismatch` или `etag_mismatch` (он входит в `size_mismatch_count`). Поэтому счётчики отчёта (`.runtime/st5-reconcile/report.json`, `missing_count`, `size_mismatch_count`, `dst_only_count`) сами по себе не норма, а `partition_errors` должно быть 0. Все такие ключи сохраняются в `state.db` (`reconcile_queue` и `dst_only`), их перепроверяют по одному. Расхождения сразу отдаются обработчику: он приведёт новое S3 к MinIO (докопирует, перезапишет или удалит лишнее), а для временных расхождений это пустая работа.

```bash
cd /opt/elma365_s3_migrator && sqlite3 .runtime/st5-reconcile/state.db \
  "SELECT key FROM reconcile_queue UNION SELECT key FROM dst_only" > /var/tmp/reconcile-keys.txt
chmod 644 /var/tmp/reconcile-keys.txt; wc -l < /var/tmp/reconcile-keys.txt
$RUN s3gw.state --wait-drained 600 | jq .drained                                # true: обработчик догнал, временные расхождения сошлись
$RUN s3gw.verifykeys --keys-file /var/tmp/reconcile-keys.txt --mark-dirty
$RUN s3gw.state --wait-drained 600 | jq .drained                                # true
$RUN s3gw.verifykeys --keys-file /var/tmp/reconcile-keys.txt                    # mismatched: 0
```

Первый `verifykeys` в норме даёт `mismatched: 0`. Ключи из его `mismatches` разошлись мимо журнала, и это ошибка: нужно выяснить как (запись в MinIO в обход шлюза, откат A4 без трассировки) и записать в «Решения» задачи 10. Второй `verifykeys` показывает, что обработчик их исправил.

## C2. Окно переключения

Шаги выполняет [scripts/cutover.sh](scripts/cutover.sh) (`$CUT`). Каждая команда проверяет, что предыдущий шаг сделан, делает свой и проверяет результат, а на первой ошибке останавливается с объяснением и кодом 1. Что именно меняется, видно в самом скрипте. Проверки в ELMA365 остаются за оператором.

**Если команда прервалась** (обрыв SSH, Ctrl-C, перезагрузка `minio2`), её достаточно запустить снова. Она видит, что уже сделано, и продолжает с этого места. Ничего не теряется: признак незаконченного шага убирается последним, а `s3gw.env` подменяется целиком. Окно лучше вести в `tmux`, тогда обрыв SSH не прервёт команду.

Обратный журнал смотреть так:

```bash
JR="sudo -u s3gw sqlite3 -readonly /var/lib/s3gw/journal-reverse.db"
```

1. **Остановить запись.** ELMA365 по возможности в режим обслуживания, и на шлюзе:

   ```bash
   $CUT freeze      # печатает время начала окна
   ```

   С этого момента шлюз отвечает `503` на любые изменения, чтения идут; скрипт проверяет это сам. В ELMA365 загрузка файла даёт ошибку.

2. **Довести очередь до нуля:**

   ```bash
   $CUT drain
   ```

   Скрипт останавливает перенос и ждёт до 10 минут, пока опустеет очередь журнала (`s3gw.state --wait-drained`). Ключ в `error` останавливает его: разобрать (раздел «Ключи в error») и повторить. `open_mpu` — незавершённые multipart-загрузки, начатые до запрета: закончить их уже нельзя, их части получают `503`, ELMA365 повторит загрузку после окна.

   Если очередь не опустела и `stuck_intents` больше 0, у намерений не записался результат (сбой записи в журнал, строка в `recent_errors`), и их ключи не отдаются обработчику. Перезапустить шлюз (`systemctl restart s3gw-gateway`): такие намерения станут `uncertain`, а обработчик сверит их ключи с MinIO через `GW_SETTLE_SECONDS` (15 минут). Затем `$CUT drain 1200`. Заложить это время в окно.

3. **Проверить изменения после сверки:**

   ```bash
   $CUT verify <T_RECONCILE минус 1 минута>
   ```

   Норма: `checked` больше 0 (примерно число изменений ELMA365 с `T_RECONCILE`; 0 значит ошибку во времени), `mismatched: 0`. При расхождениях: `$RUN s3gw.verifykeys --since <то же время> --mark-dirty`, затем `$CUT drain` (обработчик ещё работает) и `$CUT verify` ещё раз.

   Окно короче, если до него прогнать `$RUN s3gw.verifykeys --since <T_RECONCILE − 1 мин>`, а в окне — только `$CUT verify <начало того прогона − 1 мин>`.

4. **Переключить шлюз на новое S3:**

   ```bash
   $CUT switch
   ```

   Сначала скрипт сверяет предусловия:
   - запись запрещена, очередь пуста;
   - `s3gw-reverse.env` из C0.3 есть, в нём другой `JOURNAL_DB` и тот же `GW_READONLY_FILE`;
   - обратного журнала ещё нет.

   Затем останавливает обработчик, сохраняет `s3gw.env` как `s3gw-forward.env` и ставит на его место обратный конфиг. Перезапускает шлюз, включает обратный обработчик и переводит в nginx `GET`, `HEAD` и `OPTIONS` бакета `s3elma365` на шлюз. В конце проверяет:
   - шлюз пишет в свежий `journal-reverse.db` (эпоха 1);
   - изменения по-прежнему получают `503`;
   - другие бакеты отвечает MinIO.

   Оператору: ELMA365 открывает старый файл (теперь он читается из нового S3 через шлюз), загрузка по-прежнему даёт ошибку. С этого момента мгновенный откат из A4 запрещён: он отправил бы запись в MinIO, а чтения остались бы в новом S3.

5. **Снять запрет записи:**

   ```bash
   $CUT open        # печатает T_CUTOVER, записать
   ```

   Загрузить в ELMA365 тестовый файл, затем удалить. В `$JR "SELECT key, op, result FROM intents ORDER BY id DESC LIMIT 5"` должны быть намерения с `success`. Обратный обработчик перенёс их в MinIO: `$RUN s3gw.state | jq .backlog` даёт 0. Проверить предпросмотр, большие файлы, подписанные ссылки ([задача 11](tasks/11-cutover-and-finish.md)).

**Если что-то не так до шага 5,** вернуться без потерь: в новое S3 ещё ничего не записано.

```bash
$CUT undo
```

До шага 4 команда только снимает запрет записи. После шага 4 она возвращает чтения на MinIO, прямой конфиг, шлюз и обработчик. Неиспользованный обратный журнал она убирает в архив (`journal-reverse.db.undone-<время>`), чтобы следующая попытка начала с пустого. Если через обратный журнал уже шла запись (было `open`), `undo` откажется: тогда только C4.

## C3. После переключения

- Обратное отставание и ошибки смотреть так же, как в разделе «Наблюдение за журналом»: `$RUN s3gw.state` и бэкап журнала (A5) сами переходят на `journal-reverse.db`, потому что берут `JOURNAL_DB` из `s3gw.env`; запросы `$J` выполнять с `$JR`.
- Трассировка MinIO (A6) больше не нужна: в `s3elma365` MinIO пишет только обратный обработчик через `127.0.0.1`, а снаружи MinIO доступен только через nginx, который шлёт `s3elma365` в шлюз.
- Через `minio2` и шлюз теперь идёт весь трафик `s3elma365` к новому хранилищу, включая чтения: следить за сетью и CPU `minio2` и за `s3gw-5xx.log`.

## C4. Откат

Решение принимается по критериям [docs/rollback.md](docs/rollback.md), раздел 3.

```bash
$CUT freeze                                # 1. запись остановлена
$CUT drain                                 # 2. обратная очередь пуста: после C2 скрипт и $RUN читают обратный журнал
$CUT verify <T_CUTOVER минус 1 минута>     # 3. изменения после переключения совпадают (источник — новое S3, приёмник — MinIO)
$CUT rollback                              # 4. шлюз на MinIO со свежим журналом
```

Норма шагов 1–3 та же, что в C2; при расхождениях на шаге 3 действовать так же: `--mark-dirty`, `drain`, повтор `verify`.

Что делает `rollback`:
1. Выключает обратный обработчик, чтобы он не перезаписал MinIO.
2. Возвращает чтения `s3elma365` в MinIO, ещё до перезапуска шлюза. MinIO к этому моменту догнан шагами 2–3.
3. Ставит прямой конфиг с новым журналом `journal-after-rollback-<время>.db` и перезапускает шлюз. Время в имени нужно, чтобы повторный откат после новой попытки не взял журнал прошлого.
4. Снимает запрет записи и удаляет устаревший `s3gw-forward.env`.
5. Убирает обратный журнал в архив (`journal-reverse.db.rolled-back-<время>`).

Проверка: ELMA365 открывает старый файл и файл, созданный после переключения; загрузка и удаление тестового файла проходят, в новом журнале есть намерения.

После отката система в состоянии этапа A: MinIO основной, журнал пишется, обработчик выключен. Повторная попытка переключения начинается со сверки (C1), потому что новое S3 и MinIO с этого момента расходятся.

## C5. Завершение (после срока поддержки отката)

Решено 2026-10-22 (владелец): haproxy не меняется. Когда основной перенос закончен и срок отката прошёл, адрес `minio2` (`10.10.0.12`) вручную переносится на новый физический сервер. На нём на время настраивается прокси на старый MinIO только для чтения: `GET` и `HEAD` объекта `s3elma365`, которого нет в новом S3 (`404`), повторяются в старом MinIO; запись и удаление идут только в новое S3. Шлюз, журнал и обратный обработчик при этом выводятся. Это делает владелец после завершения переноса, отдельно от этого конвейера.

Прокси — nginx с конфигом [deploy/nginx/s3-c5.conf](deploy/nginx/s3-c5.conf), его проверяет стенд `python scripts/stand_c5.py`. Запись, удаление, листинг и другие бакеты конфиг отправляет только в новое S3. Чтение объекта (`GET`, `HEAD`, `Range`, подписанная ссылка) при `404` нового S3 он повторяет в старом MinIO без изменений, подпись ELMA365 проходит. Каждое такое обращение пишется строкой JSON в `/var/log/nginx/s3-c5-old-minio.log`: время, метод, ключ, ответы обоих хранилищ, без подписи ссылки. Какие ключи читались из старого MinIO:

```bash
jq -r 'select(.status < 300) | .key' /var/log/nginx/s3-c5-old-minio.log | sort | uniq -c | sort -rn | head
```

Что учесть перед переносом адреса:

- файл, удалённый в ELMA365 после выключения обратного обработчика, остаётся в старом MinIO, и прокси снова отдаст его по прямой ссылке (стенд это показывает). Поэтому прокси держат недолго: после чистой сверки (C1) в новом S3 есть всё, и строка в логе означает либо такой удалённый файл, либо пропуск, который нужно разобрать;
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
| Состояние журнала | `$RUN s3gw.state` | `backlog`: на этапе A растёт (обработчика нет), на этапе B единицы ключей, `backlog_oldest` до минут; `stuck_intents: 0` (иначе «Намерение осталось без результата» в «Сбоях»); `error_keys: 0`; `recent_errors` редкие временные |
| Ключи по состояниям | `$J "SELECT state, count(*) FROM key_state GROUP BY state"` | |
| Место | `df -h /var/lib/s3gw; du -sh /var/lib/s3gw` | |
| Копия журнала (A5) | `ls -lt /backup/s3gw \| head -3; systemctl is-active s3gw-journal-backup` | свежая не старше 2 ч; юнит не висит в `activating` |
| Ошибки на входе | `wc -l /var/log/nginx/s3gw-5xx.log; tail /var/log/nginx/s3gw-5xx.log` | не растёт; `502` — шлюз недоступен, `503` — журнал недоступен |

Логи: `journalctl -u s3gw-gateway -u s3gw-handler -u s3gw-bulk --since '1 hour ago'`.

# Сбои и восстановление

- **Шлюз упал или перезапущен.** После падения systemd поднимает его за секунду. `systemctl restart` и `stop` дают запросам в полёте `GW_SHUTDOWN_TIMEOUT` (5 с) и обрывают их: после SIGTERM загрузка дойти не может (aiohttp перестаёт читать тело), ELMA365 её повторит. Всё это время запись получает `502`, а после C2 и чтения. Новая эпоха переводит незакрытые намерения старой в `uncertain`, обработчик берёт их через `GW_SETTLE_SECONDS` (900 с) и сверяет с MinIO. Вручную ничего делать не нужно.
- **Обработчик остановлен.** Запись в ELMA365 продолжает работать, растёт только отставание. После запуска очередь дорабатывается из журнала. Копирование при этом встанет на паузу по `journal_lag`.
- **Перенос остановлен.** Ни на что не влияет. После запуска продолжает с контрольных точек.
- **Поток обработчика или переноса упал.** Временную ошибку журнала (`database is locked` дольше тайм-аута) поток обработчика пишет в лог и повторяет после паузы. На любой другой ошибке процесс останавливает остальные потоки и завершается с ошибкой, трассировка в `journalctl -u s3gw-handler` или `-u s3gw-bulk`. Задания упавшего потока переноса возвращаются в `retry`. systemd перезапускает обработчик через 5 с, перенос через 30 с (код 3); перенос продолжает с контрольных точек. Код 2 у переноса означает, что он закончил с ошибками заданий (раздел B3), его systemd не перезапускает.
- **ELMA365 получает 502 или 503 на запись** (`/var/log/nginx/s3gw-5xx.log`). `502` — шлюз недоступен, `503` — шлюз не может записать намерение (диск, блокировка журнала). До переключения чтения при этом работают. Проверить `systemctl status s3gw-gateway`, свободное место на `/var/lib/s3gw`, `$RUN s3gw.state | jq .recent_errors`. Если быстро не чинится, сделать мгновенный откат в nginx с трассировкой (раздел A4, «Откат») и после возврата сдать изменения журналу (`--mark-dirty`). После переключения (C2) мгновенного отката нет, только C4.
- **Намерение осталось без результата.** Если результат не записался в журнал (диск журнала заполнен, в логе шлюза `journal write failed`), клиент ответ получает, а намерение остаётся открытым. Ключ числится в полёте, и обработчик его не возьмёт, пока шлюз не перезапустят. После того как место освобождено, запись принимается и без перезапуска. Затем проверить `$RUN s3gw.state | jq .stuck_intents` (намерения без результата дольше минуты): если больше 0, выполнить `systemctl restart s3gw-gateway` в тихое время. Они станут `uncertain`, и обработчик сверит их ключи через `GW_SETTLE_SECONDS`.
- **nginx не отвечает.** ELMA365 и другие сервисы теряют MinIO целиком (чтения тоже). `systemctl status nginx`, `nginx -t`, `systemctl restart nginx`. Мимо журнала при этом ничего не пишется: MinIO снаружи недоступен.
- **Журнал потерян или повреждён.** Восстановить из копии (раздел A5).
- **Перезагрузка `minio2`.** Сами поднимаются MinIO, nginx, шлюз, таймер копий журнала и, с этапа B, обработчик, в любом порядке. Запрет записи окна (`/var/lib/s3gw/read-only`) и маршруты nginx перезагрузку переживают. Не поднимаются сами процессы, запущенные вручную; их запускают снова, и они продолжают с контрольных точек:
  - листинг A7 — той же командой запуска из A7;
  - копирование B1 — `systemctl start s3gw-bulk`;
  - сверка C1 — с `RESUME=1`;
  - команда `$CUT`, которую прервала перезагрузка, — та же команда;
  - трассировка A6 — записать заново. Трассировка отката A4 оборвалась: это случай «трассировка не писалась всё время отката» (раздел A4).

  Сессия оператора тоже новая: переменные `J`, `RUN`, `B`, `CUT` задать заново (начало раздела «Порядок запуска»). Проверка после загрузки:

  ```bash
  systemctl is-active minio nginx s3gw-gateway s3gw-journal-backup.timer     # active, active, active, active
  systemctl is-active s3gw-handler                                            # этап A: inactive; с этапа B: active
  ss -ltnp | grep ':9000\b'                                                   # nginx на 10.10.0.12:9000, minio на 127.0.0.1:9000
  $RUN s3gw.state | jq '{epoch: .epoch.epoch, backlog, stuck_intents, error_keys}'   # эпоха выросла на 1
  ```

  Если nginx не поднялся с `bind() to 10.10.0.12:9000 failed (99: Cannot assign requested address)`, он стартовал раньше сети. Тогда `systemctl restart nginx`, а чтобы не повторилось, `echo net.ipv4.ip_nonlocal_bind=1 > /etc/sysctl.d/90-s3gw.conf && sysctl --system`.
- **Ключи в `error`.** Это ключи, у которых исчерпаны попытки. Разобрать причину по `last_error`, устранить её, затем вернуть ключи в работу. Править пачками по `rowid`, чтобы не держать блокировку журнала дольше `journal_timeout` шлюза (2 с), иначе ELMA365 получит `503`:

  ```bash
  JW="sudo -u s3gw sqlite3 /var/lib/s3gw/journal.db"
  MAX=$($J "SELECT max(rowid) FROM key_state")
  for s in $(seq 0 50000 $MAX); do
    $JW "UPDATE key_state SET state = 'dirty', attempts = 0, not_before = 0, lease_owner = NULL WHERE state = 'error' AND rowid BETWEEN $s AND $((s + 49999))"
  done
  ```
