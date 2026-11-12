# Новое S3: установка на ВМ и доступ через внешний адрес

Новое S3 (SeaweedFS 4.48, задачи [02](../tasks/02-choose-minio-replacement.md) и [03](../tasks/03-deploy-new-storage.md)) стоит на ВМ гипервизора в локальной сети площадки, за роутером с внешним адресом. `minio2` работает в облаке и ходит в новое S3 через интернет.

В новое S3 ходит только `minio2`:

- обработчик и перенос пишут в него на этапе B;
- после переключения (C2) шлюз пересылает туда запросы ELMA365;
- сверка C1 листит его бакет.

ELMA365 и браузеры напрямую в новое S3 не ходят: подписанные ссылки ведут на `elma.example.com`, оттуда через ingress и haproxy на `minio2`. Поэтому наружу открыт один порт, только для внешнего адреса `minio2`, и за ним nginx с TLS.

```
облако                                     площадка
minio2 ─► внешний адрес MINIO2_PUBLIC_IP
            │ интернет, TLS
            ▼
          WHITE_IP:9443 роутер ── проброс ──► S3_VM_LAN_IP:9443 nginx на ВМ (TLS, allow MINIO2_PUBLIC_IP)
                                                 └─► SeaweedFS S3 127.0.0.1:8333 (master, volume, filer: тоже 127.0.0.1)
```

Конфиги из этой инструкции (`deploy/seaweedfs/`, [deploy/nginx/new-s3.conf](../deploy/nginx/new-s3.conf)) проверены 2026-10-29 в Docker на SeaweedFS 4.48 и nginx 1.28. Клиентом была реплика `minio2` с CA в системном хранилище. Проверка C0.1 прошла напрямую и через шлюз с `Host: elma.example.com`, права ключей совпали с ожидаемыми, данные пережили перезапуск, чужой адрес получил отказ.

Обозначения ниже заменить своими значениями:

| Обозначение | Что это | Пример |
|---|---|---|
| `WHITE_IP` | внешний адрес роутера площадки | `203.0.113.10` |
| `S3_NAME` | DNS-имя нового S3, если оно есть | `s3-new.example.net` |
| `S3_VM_LAN_IP` | адрес ВМ в локальной сети | `10.20.0.20` |
| `LAN` | локальная сеть площадки | `10.20.0.0/24` |
| `MINIO2_PUBLIC_IP` | адрес, с которого `minio2` выходит в интернет | `198.51.100.7` |
| порт | внешний порт нового S3, он же порт nginx на ВМ | `9443` |

Ниже `ENDPOINT` — адрес, по которому `minio2` ходит в новое S3: `https://S3_NAME:9443`, а без DNS-имени `https://WHITE_IP:9443`.

## 1. Что узнать заранее

- **Внешний адрес `minio2`** и постоянный ли он. На `minio2`: `curl -s https://ifconfig.me; echo`. У провайдера облака уточнить, что адрес закреплён за ВМ (или за NAT-шлюзом сети) и не сменится. Если `minio2` выходит в интернет через несколько адресов, в списки ниже вписать все.
- **Внешний адрес площадки** статический. Если он динамический, нужно DNS-имя с обновлением адреса, а в сертификате и в `ENDPOINT` только имя.
- **Порт 9443 на роутере свободен**: за роутером несколько серверов, у них могут быть свои пробросы. Занят — взять другой и заменить 9443 везде ниже.
- **Скорость канала площадки** в обе стороны (тариф) и **исходящий трафик облака**: сколько стоит и есть ли лимит. На этапе B через него уйдёт весь бакет, 9,3 ТиБ ([source-passport.md](source-passport.md)).

## 2. ВМ на гипервизоре

- 8 vCPU, 16 ГБ RAM (бюджет постановки).
- Системный диск на SSD: ОС, метаданные master и filer, журналы. Размер метаданных filer на 12 млн объектов уточняет нагрузочный прогон (задача 02).
- Диск данных — логический том RAID6 целиком, блочным устройством, без файла образа на хранилище гипервизора. Так между SeaweedFS и массивом меньше слоёв, а `fio` из ВМ (задача 03) мерит сам массив. В Proxmox, например: `qm set <VMID> --scsihw virtio-scsi-single --scsi1 /dev/disk/by-id/<том RAID>,cache=none,iothread=1`.
- Сеть: сетевая карта ВМ в мосту в локальную сеть, без NAT внутри гипервизора: в Proxmox это `vmbr0`, в ESXi port group на vSwitch с выходом в LAN, в Hyper-V внешний коммутатор. Иначе проброс придётся делать дважды.
- Адрес `S3_VM_LAN_IP` статический в настройках ОС (или резервирование DHCP на роутере). nginx слушает именно его и без него не стартует.

## 3. ОС и диски

Ubuntu 24.04 LTS или Debian 12, команды от root.

```bash
apt update && apt install -y nginx ufw xfsprogs git jq curl openssl
timedatectl | grep -E 'synchronized|NTP'     # System clock synchronized: yes
lsblk -o NAME,SIZE,ROTA,MODEL                 # диск RAID6, ниже /dev/sdb
```

Часы обязаны быть синхронизированы: подпись SigV4 с расхождением больше 15 минут отвергается (`RequestTimeTooSkewed`).

Файловая система и размещение метаданных — решения задачи 03. По умолчанию здесь XFS на томе данных, а метаданные master и filer лежат на системном SSD:

```bash
mkfs.xfs -L s3data /dev/sdb                   # если stripe size массива известен: -d su=<stripe>,sw=6 (RAID6 из 8 дисков)
install -d /srv/seaweedfs
echo 'LABEL=s3data /srv/seaweedfs xfs defaults,noatime 0 0' >> /etc/fstab && mount /srv/seaweedfs
useradd --system --home-dir /var/lib/seaweedfs --shell /usr/sbin/nologin seaweedfs
install -d -o seaweedfs -g seaweedfs /srv/seaweedfs/volumes /var/lib/seaweedfs /var/lib/seaweedfs/master
df -h /srv/seaweedfs /var/lib/seaweedfs
```

## 4. SeaweedFS

Та же сборка, что в образе стендов (`chrislusf/seaweedfs:4.48`): обычная, не `large_disk`, тома до 30 ГБ.

```bash
cd /tmp && V=4.48
curl -fLO https://github.com/seaweedfs/seaweedfs/releases/download/$V/linux_amd64.tar.gz
curl -fLO https://github.com/seaweedfs/seaweedfs/releases/download/$V/linux_amd64.tar.gz.md5
echo "$(cat linux_amd64.tar.gz.md5)  linux_amd64.tar.gz" | md5sum -c     # linux_amd64.tar.gz: OK
tar -xzf linux_amd64.tar.gz weed && install -m 755 weed /usr/local/bin/weed && rm weed
weed version                                  # version 30GB 4.48 530be3e37 linux amd64

git clone https://github.com/laktes1/elma365_s3_migrator /opt/elma365_s3_migrator
D=/opt/elma365_s3_migrator/deploy
install -d /etc/seaweedfs
install -m 644 $D/seaweedfs/filer.toml /etc/seaweedfs/filer.toml
install -m 644 $D/seaweedfs/seaweedfs.service /etc/systemd/system/seaweedfs.service
install -m 600 -o seaweedfs $D/seaweedfs/s3.json.example /etc/seaweedfs/s3.json
```

Ключи доступа в `/etc/seaweedfs/s3.json`. У каждого ключа права только на `s3elma365`, кроме `admin`:

| Имя | Кто им пользуется | Права |
|---|---|---|
| `admin` | администратор, только на самой ВМ | всё |
| `elma365` | ELMA365 после переключения (через шлюз, C2) | чтение, запись, листинг, теги |
| `s3gw-dst` | обработчик и перенос на `minio2` (`DST_*` в `s3gw.env`) | чтение, запись, листинг, теги |
| `s3gw-read` | обратный обработчик (C0.3) и сверка C1 | чтение, листинг, теги |

У `elma365` ключ и секрет те же, что в настройках S3 ELMA365, то есть те, с которыми ELMA365 сейчас ходит в MinIO: после C2 шлюз пересылает её подписанные запросы как есть. Остальные секреты сгенерировать:

```bash
cd /etc/seaweedfs
for v in ADMIN_SECRET S3GW_DST_SECRET S3GW_READ_SECRET; do sed -i "s/$v/$(openssl rand -hex 20)/" s3.json; done
sed -i "s/\"ADMIN_KEY\"/\"admin-$(openssl rand -hex 4)\"/" s3.json
vi s3.json                                    # ELMA365_KEY и ELMA365_SECRET: ключ и секрет из настроек S3 ELMA365
jq -r '.identities[] | "\(.name) \(.credentials[0].accessKey)"' s3.json     # файл разбирается, ключи на месте
```

Секреты `s3gw-dst` и `s3gw-read` передать на `minio2` (раздел 9) защищённым каналом, не почтой и не в чат.

Запуск:

```bash
systemctl daemon-reload && systemctl enable --now seaweedfs
journalctl -u seaweedfs -n 5 --no-pager       # Start Seaweed S3 API Server 30GB 4.48 ... at http port 8333
ss -ltnp | grep weed
```

Норма: все порты `weed` только на `127.0.0.1`: 8333 (S3), 8888 (filer), 8080 (volume), 9333 (master) и их gRPC-порты (18333, 18888, 18080, 19333). Портов 8111, 8181 и 9101 нет. Метаданные filer лежат в `/var/lib/seaweedfs/filerldb2`, тома — в `/srv/seaweedfs/volumes`.

Бакет:

```bash
printf 's3.bucket.create -name s3elma365\ns3.bucket.list\n' | weed shell -master=127.0.0.1:9333
```

`weed shell` на `127.0.0.1` работает без пароля, и любой пользователь ВМ может удалить им всё. Поэтому на ВМ работают только администраторы хранилища.

После `systemctl restart seaweedfs` S3 отвечает не сразу: тома загружаются от нескольких секунд до минуты, и всё это время nginx отдаёт `502`. Обработчик и перенос повторят запрос. После C2 этот `502` получает ELMA365, поэтому перезапускать S3 только в тихое время. Версию SeaweedFS во время миграции не обновлять: стенды проверяли 4.48.

## 5. TLS-сертификат

Канал идёт через интернет, поэтому без TLS нельзя. Сертификат выпускает свой CA, ему доверяет только `minio2`. Так не нужны ни DNS-имя, ни открытый всем порт 80, а сертификат на 3 года не истечёт посреди миграции. Ключ CA удаляется сразу после выпуска: если понадобится новый сертификат, проще сделать новый CA, чем хранить ключ, которому доверяет `minio2`.

На ВМ:

```bash
install -d -m 700 /root/new-s3-ca && cd /root/new-s3-ca && umask 077
NAME=s3-new.example.net; PUB=203.0.113.10; LAN=10.20.0.20     # S3_NAME, WHITE_IP, S3_VM_LAN_IP
openssl req -x509 -newkey rsa:3072 -sha256 -days 3650 -nodes -keyout ca.key -out ca.crt -subj "/CN=new-s3 CA" \
  -addext "keyUsage=critical,keyCertSign,cRLSign"
openssl req -newkey rsa:2048 -nodes -keyout new-s3.key -out new-s3.csr -subj "/CN=$NAME"
cat > new-s3.ext <<EOF
basicConstraints = critical, CA:FALSE
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = DNS:$NAME, IP:$PUB, IP:$LAN
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid
EOF
openssl x509 -req -in new-s3.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 1095 -sha256 -extfile new-s3.ext -out new-s3.crt
openssl verify -x509_strict -CAfile ca.crt new-s3.crt     # new-s3.crt: OK
install -d /etc/nginx/tls && install -m 644 new-s3.crt /etc/nginx/tls/ && install -m 600 new-s3.key /etc/nginx/tls/
rm ca.key ca.srl new-s3.csr new-s3.key new-s3.ext         # остаются ca.crt (для minio2) и new-s3.crt
```

Без DNS-имени убрать `DNS:$NAME, ` из `subjectAltName`. `-x509_strict` проверяет то же, что шлюз (aiohttp) на Python 3.13 и новее: без этих расширений он отверг бы сертификат. Дату окончания (`openssl x509 -in new-s3.crt -noout -enddate`) записать в «Решения» задачи 03.

Можно взять и публичный сертификат, например Let's Encrypt, если у площадки есть домен и DNS с API (проверка DNS-01). Тогда на `minio2` ничего настраивать не нужно, раздел 8.1 пропускается. Проверка HTTP-01 не подходит: ей нужен порт 80, открытый всем.

## 6. nginx на ВМ

```bash
rm -f /etc/nginx/sites-enabled/default        # стандартный сайт на :80 не нужен
install -m 644 /opt/elma365_s3_migrator/deploy/nginx/new-s3.conf /etc/nginx/conf.d/new-s3.conf
sed -i 's/S3_VM_LAN_IP/10.20.0.20/; s/MINIO2_PUBLIC_IP/198.51.100.7/' /etc/nginx/conf.d/new-s3.conf
nginx -t && systemctl reload nginx
ss -ltnp | grep nginx                         # только 10.20.0.20:9443
curl -s --cacert /root/new-s3-ca/ca.crt https://10.20.0.20:9443/ | grep -o '403 Forbidden'
```

Последняя команда идёт с адреса самой ВМ, его нет в `allow`, поэтому норма — `403 Forbidden` от nginx: TLS работает, чужой адрес отвергнут. Если у `minio2` несколько внешних адресов, на каждый своя строка `allow`.

Конфиг не меняет `Host` и не добавляет `X-Forwarded-*`: подпись SigV4 проверяется по `Host`, а после C2 это `elma.example.com`. По той же причине сервер `default_server`. В лог `/var/log/nginx/new-s3.log` пишется путь без строки запроса, так что подписанные ссылки туда не попадают. Поле `addr` в этом логе — адрес клиента, каким его видит ВМ.

## 7. Файрвол ВМ и роутер

На ВМ:

```bash
ufw default deny incoming && ufw default allow outgoing
ufw allow from 10.20.0.0/24 to any port 22 proto tcp      # ssh только из локальной сети
ufw allow from 198.51.100.7 to any port 9443 proto tcp    # minio2
ufw enable && ufw status verbose
```

Если вы зашли по ssh не из `LAN`, сначала разрешите свой адрес, иначе `ufw enable` оборвёт сессию.

На роутере одно правило проброса (DNAT):

| Поле | Значение |
|---|---|
| протокол | TCP |
| внешний интерфейс и порт | WAN (`WHITE_IP`), 9443 |
| куда | `S3_VM_LAN_IP`, 9443 |
| источник | `MINIO2_PUBLIC_IP`, если роутер умеет ограничивать источник в правиле проброса; если нет, ограничивает `ufw` на ВМ |

Больше на ВМ ничего не пробрасывать: ни 22, ни 8333, 8888, 9333, 80, ни DMZ. Интерфейс управления гипервизором тоже не пробрасывать.

Пример для MikroTik RouterOS (в стандартной конфигурации цепочка `forward` пропускает соединения с WAN, прошедшие `dst-nat`):

```
/ip firewall nat add chain=dstnat in-interface=<WAN> protocol=tcp dst-port=9443 src-address=198.51.100.7 \
    action=dst-nat to-addresses=10.20.0.20 to-ports=9443 comment="new S3 for minio2"
```

Роутер не должен подменять адрес источника у входящих соединений (masquerade или SNAT на входе). Иначе ВМ видит адрес роутера, и `ufw` с `allow` в nginx не отличат `minio2` от остальных. Это проверяется в разделе 8.2.

Из локальной сети по `WHITE_IP` новое S3 может быть недоступно (нужен hairpin NAT). Это нормально: из `LAN` ходят на `S3_VM_LAN_IP`, а снаружи проверяют с `minio2`.

## 8. Проверка с minio2

### 8.1. Доверие к CA

`ca.crt` из `/root/new-s3-ca/` на ВМ скопировать на `minio2` (например, `scp`). Затем на `minio2`:

```bash
install -m 644 ca.crt /usr/local/share/ca-certificates/new-s3-ca.crt && update-ca-certificates   # 1 added
grep -q '^AWS_CA_BUNDLE=' /etc/s3gw/s3gw.env || echo 'AWS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt' >> /etc/s3gw/s3gw.env
test -e /root/.aws/config || { install -d -m 700 /root/.aws && printf '[default]\nca_bundle = /etc/ssl/certs/ca-certificates.crt\n' > /root/.aws/config; }
grep ca_bundle /root/.aws/config              # если файл уже был и строки нет, дописать её в [default] вручную
```

Системному хранилищу доверяют шлюз (aiohttp), `curl` и `mc`. У boto3 свой список CA: юнитам s3gw путь к системному даёт `AWS_CA_BUNDLE` из `s3gw.env`, а командам root (C0.1, сверка C1) — `/root/.aws/config`. Без них boto3 отвечает `SSL: CERTIFICATE_VERIFY_FAILED` на каждый запрос, на стенде так и было.

Процессы читают CA при старте. Если обработчик и перенос уже работают, перезапустить их; если раздел делается до B1, перезапускать нечего. Шлюз в новое S3 до C2 не ходит, а в C2 его перезапускает `cutover.sh`.

### 8.2. Доступ и TLS

```bash
E=https://s3-new.example.net:9443             # ENDPOINT
curl -sS $E/ | head -c 200; echo
curl -sS -o /dev/null -w 'connect %{time_connect}s, tls %{time_appconnect}s\n' $E/
```

Норма: XML с `AccessDenied` от SeaweedFS (запрос без подписи), без `-k` и без ошибок сертификата. `connect` — это примерно RTT до площадки. На ВМ в `tail -1 /var/log/nginx/new-s3.log | jq -r .addr` должен стоять `MINIO2_PUBLIC_IP`.

С адреса не из списка, например с ноутбука через мобильный интернет: `curl -m 10 -k https://WHITE_IP:9443/`. Норма: тайм-аут (отрезал роутер или `ufw`).

### 8.3. Подпись ELMA365

Тот же запуск, что C0.1 в [PIPELINE.md](../PIPELINE.md), только раньше. Venv на `minio2` уже есть с A2:

```bash
cd /opt/elma365_s3_migrator && ELMA_ACCESS_KEY=<ключ ELMA365> ELMA_SECRET_KEY=<секрет> \
  .venv/bin/python scripts/check_new_s3_signature.py --endpoint $E
```

Норма: `all passed`, в том числе последний шаг `BadDigest`.

### 8.4. Скорость канала

Через само S3, то есть по настоящему пути: TLS, nginx, SeaweedFS. Ключ `s3gw-dst`. Нужен 1 ГБ свободного места на системном диске `minio2`.

```bash
mc alias set newdst $E s3gw-dst <секрет s3gw-dst> >/dev/null
head -c 1G /dev/urandom > /var/tmp/net-check.bin
time mc cp -q /var/tmp/net-check.bin newdst/s3elma365/_net-check/1g.bin    # в площадку
time mc cat newdst/s3elma365/_net-check/1g.bin > /dev/null                 # из площадки
mc rm newdst/s3elma365/_net-check/1g.bin; rm /var/tmp/net-check.bin
mc ls newdst/s3elma365/_net-check/ | wc -l    # 0: в бакете не осталось лишнего, иначе его найдёт сверка C1
```

1 ГБ делить на время — МБ/с; умножить на 8 — Мбит/с. RTT и скорость в обе стороны записать в «Решения» задачи 03 (канал до нового S3 — открытый пункт [задачи 13](../tasks/13-remaining-verification.md)).

Как читать результат:

- **Этап B** ограничен входящим каналом площадки. Если считать только полосу, 9,3 ТиБ проходят за:

  | Канал в площадку | Время |
  |---|---|
  | 100 Мбит/с | ≈ 9,5 сут |
  | 300 Мбит/с | ≈ 3,2 сут |
  | 1 Гбит/с | ≈ 23 ч |

  Для мелких объектов важнее задержка: на объект уходит два запроса в новое S3 (PUT и проверочный HEAD), поэтому сеть даёт не больше `BULK_WORKERS / (2 × RTT)` объектов/с. При 8 потоках и RTT 30 мс это около 130 объектов/с. Прошлая миграция шла около 12 объектов/с, так что упор скорее будет в HDD источника. Если `bottleneck` покажет `destination`, потоки можно добавить (B1, B2). Если канал общий с другими серверами за роутером, ограничить перенос `mbps=` в `SCHEDULE` ниже скорости канала.
- **После C2** все чтения файлов ELMA365 идут из нового S3 через исходящий канал площадки. Скорость скачивания у всех пользователей вместе не больше его.

### 8.5. Если не работает

| Что видно | Причина | Что делать |
|---|---|---|
| `curl` висит до тайм-аута | нет проброса, `ufw`, провайдер режет порт; или проверка идёт из `LAN` | правило роутера, `ufw status`, проверить с `minio2` |
| HTML `403 Forbidden` от nginx | nginx видит не адрес `minio2` | `addr` в логе ВМ: адрес роутера — роутер делает SNAT на входе; другой внешний адрес — дописать его в `allow` и `ufw` |
| `certificate verify failed` у `curl` или `mc` | CA не в хранилище или адреса нет в сертификате | 8.1; `openssl s_client -connect s3-new.example.net:9443 -servername s3-new.example.net </dev/null 2>/dev/null \| openssl x509 -noout -ext subjectAltName` |
| `CERTIFICATE_VERIFY_FAILED` только у boto3 | нет `AWS_CA_BUNDLE` или `ca_bundle` | 8.1 |
| `SignatureDoesNotMatch` | кто-то по пути меняет `Host` | в nginx только `proxy_set_header Host $http_host`, без `X-Forwarded-Host`; у SeaweedFS не задавать `-s3.externalUrl` |
| `RequestTimeTooSkewed` | часы ВМ или `minio2` | `timedatectl` |
| `502` от nginx | `weed` не запущен или ещё загружает тома | `systemctl status seaweedfs`, `journalctl -u seaweedfs` |
| мелкие запросы проходят, большая загрузка виснет | MTU на пути (PPPoE), не доходят ICMP | ограничение MSS на роутере (в MikroTik — `change-mss` в `mangle`) |

## 9. Что вписать на minio2

`/etc/s3gw/s3gw.env`: `AWS_CA_BUNDLE` из 8.1 и строки `DST_*`, которые раскомментируют в B1:

```
DST_ENDPOINT=https://s3-new.example.net:9443
DST_ACCESS_KEY=s3gw-dst
DST_SECRET_KEY=<секрет s3gw-dst>
AWS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt
```

`/etc/s3gw/s3gw-reverse.overrides` (C0.3): `GW_UPSTREAM` и `SRC_ENDPOINT` — это `ENDPOINT`, `SRC_ACCESS_KEY=s3gw-read` и его секрет.

Сверка C1: `mc alias set minio_new <ENDPOINT> s3gw-read <секрет s3gw-read>`.

## Что не входит

Это задача 03, отдельно от этой инструкции:

- проверка RAID (уровень, stripe size, кеш, BBU) и замер `fio` из ВМ;
- мониторинг ёмкости, SMART, массива, доступности S3 и задержек;
- копия метаданных filer и проверка восстановления на чистую ВМ;
- что происходит при отказе SSD, ВМ и сервера.

К C5 в новом S3 нужны ещё бакеты `elma365-backups`, `-loki`, `-tempo` и ключи их сервисов ([PIPELINE.md](../PIPELINE.md), C5).
