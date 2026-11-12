#!/usr/bin/env bash
# Окно переключения ELMA365 на новое S3 (PIPELINE.md, C2) и откат (C4). На minio2, от root.
#
#   cutover.sh freeze          C2.1, C4.1   запрет записи: шлюз отвечает 503 на любые изменения
#   cutover.sh drain [СЕК]     C2.2, C4.2   перенос остановлен, очередь журнала пуста, ключей в error нет (ждёт до СЕК, 600)
#   cutover.sh verify С        C2.3, C4.3   ключи, изменённые с момента С (UTC), одинаковы в источнике и приёмнике
#   cutover.sh switch          C2.4         шлюз и чтения s3elma365 на новое S3, обратный обработчик
#   cutover.sh open            C2.5         запрет записи снят
#   cutover.sh undo            C2 до open   всё обратно на MinIO, неиспользованный обратный журнал в архив
#   cutover.sh rollback        C4.4         шлюз на MinIO со свежим журналом, обратный журнал в архив
#
# Каждая команда проверяет, что предыдущий шаг сделан, делает свой и проверяет результат;
# на первой ошибке останавливается с объяснением и кодом 1. Прерванную команду (обрыв SSH,
# Ctrl-C, перезагрузка) достаточно запустить снова: она видит, что уже сделано, и продолжает.
# Признак незаконченных switch, undo и rollback — файл s3gw-forward.env, он убирается последним;
# s3gw.env подменяется целиком (mv). Проверки в ELMA365 остаются за оператором.
# CUTOVER_FAULT=<шаг> — только для стенда: команда убивает себя после этого шага.
set -euo pipefail

APP=${APP:-/opt/elma365_s3_migrator}
ETC=${ETC:-/etc/s3gw}
NGINX_CONF=${NGINX_CONF:-/etc/nginx/conf.d/s3gw.conf}
GW=http://127.0.0.1:9110

die() { echo "cutover: $*" >&2; exit 1; }
ok() { echo "ok: $*"; }
step() { if [ "${CUTOVER_FAULT:-}" = "$1" ]; then kill -9 $$; fi; }
env_of() { sed -n "s/^$1=//p" "$2" | tail -1; }   # значение $1 в env-файле $2
# $RUN из PIPELINE.md: от s3gw, с конфигом юнитов
run() { systemd-run --wait --pipe -q -p User=s3gw -p EnvironmentFile="$ETC/s3gw.env" -p WorkingDirectory="$APP" "$APP/.venv/bin/python" -m "$@"; }
# s3gw.env целиком из файла $1 (с правкой JOURNAL_DB=$2, если задан): прерывание не оставит наполовину записанный конфиг
put_env() {
  cp -p "$1" "$ETC/s3gw.env.new"
  if [ -n "${2:-}" ]; then sed -i "s#^JOURNAL_DB=.*#JOURNAL_DB=$2#" "$ETC/s3gw.env.new"; fi
  mv "$ETC/s3gw.env.new" "$ETC/s3gw.env"
}
# Изменение без подписи: при запрете шлюз отвечает 503 до журнала и до MinIO
probe_write() { curl -s -o /dev/null -w '%{http_code}' -X PUT "$GW/s3elma365/.cutover-probe"; }
gateway_up() {
  for _ in $(seq 60); do
    [ "$(curl -s -o /dev/null -w '%{http_code}' "$GW/minio/health/live")" = 200 ] && return 0
    sleep 1
  done
  die "шлюз не ответил за 60 с: systemctl status s3gw-gateway"
}
# GET/HEAD/OPTIONS бакета s3elma365 в nginx на $1 (s3gw_minio или s3gw_gateway)
reads_to() {
  local from=s3gw_minio
  [ "$1" = s3gw_minio ] && from=s3gw_gateway
  sed -i -E "s/^(    (GET|HEAD|OPTIONS) +)$from;/\1$1;/" "$NGINX_CONF"
  [ "$(grep -cE "^ +(GET|HEAD|OPTIONS) +$1;" "$NGINX_CONF")" = 3 ] || die "в $NGINX_CONF не все GET/HEAD/OPTIONS указывают на $1"
  nginx -t -q && nginx -s reload
}
intents_in() {  # сколько намерений записано в журнал $1 (0, если его нет)
  if [ -e "$1" ]; then run s3gw.state --journal "$1" | jq '.counters.intents // 0'; else echo 0; fi
}
# Журнал, через который больше никто не пишет, уходит в архив: следующее переключение начнёт с пустого
archive() {
  local f stamp
  [ -e "$1" ] || return 0
  stamp=$(date -u +%Y%m%d-%H%M%S)
  for f in "$1" "$1-wal" "$1-shm"; do
    if [ -e "$f" ]; then mv "$f" "$f.$2-$stamp"; fi
  done
  ok "$1 -> $1.$2-$stamp"
}
# Работающий шлюз открыл журнал $1, и в нём ещё нет намерений
journal_fresh() {
  local pid
  pid=$(systemctl show -p MainPID --value s3gw-gateway)
  for _ in $(seq 10); do
    find "/proc/$pid/fd" -lname "$1" 2>/dev/null | grep -q . && break
    sleep 1
  done
  find "/proc/$pid/fd" -lname "$1" 2>/dev/null | grep -q . || die "шлюз (pid $pid) не открыл $1"
  [ "$(intents_in "$1")" = 0 ] || die "в $1 уже есть намерения: журнал не свежий"
}

cmd=${1:-}
case "$cmd" in
freeze | drain | verify | switch | open | undo | rollback) ;;
*) sed -n '2,10p' "$0" | sed 's/^# \{0,1\}//'; exit 2 ;;
esac
[ "$(id -u)" = 0 ] || die "запускать от root"
RO=$(env_of GW_READONLY_FILE "$ETC/s3gw.env")
[ -n "$RO" ] || die "в $ETC/s3gw.env нет GW_READONLY_FILE: без него запрет записи не действует (PIPELINE.md, C0.3)"

case "$cmd" in
freeze)
  touch "$RO"
  date -u +%FT%TZ
  [ "$(probe_write)" = 503 ] || die "шлюз не отвечает 503 на изменение: работает ли он с GW_READONLY_FILE=$RO?"
  ok "запись остановлена; проверить в ELMA365: загрузка файла даёт ошибку"
  ;;
drain)
  [ -e "$RO" ] || die "запись не остановлена: сначала freeze"
  systemctl stop s3gw-bulk
  if ! out=$(run s3gw.state --wait-drained "${2:-600}"); then
    echo "$out" | jq '{drained, backlog, open_intents, stuck_intents}'
    die "очередь не опустела за ${2:-600} с. stuck_intents больше 0: systemctl restart s3gw-gateway, затем drain 1200"
  fi
  echo "$out" | jq '{journal, drained, error_keys, error_sample, open_mpu}'
  [ "$(echo "$out" | jq .error_keys)" = 0 ] || die "есть ключи в error (error_sample): разобрать (PIPELINE.md, «Ключи в error») и повторить drain"
  ok "очередь пуста; open_mpu — загрузки, начатые до запрета, ELMA365 повторит их после окна"
  ;;
verify)
  [ -n "${2:-}" ] || die "verify С: С — UTC на минуту раньше нужного момента, например 2026-11-20T18:00:00Z"
  rc=0
  out=$(run s3gw.verifykeys --since "$2") || rc=$?
  echo "$out" | jq '{checked, mismatched, mismatches: .mismatches[:5]}'
  [ "$(echo "$out" | jq .checked)" -gt 0 ] || die "checked 0: с $2 изменений нет, проверьте время"
  [ "$rc" = 0 ] || die "есть расхождения: $0 verify $2 повторить после run s3gw.verifykeys --since $2 --mark-dirty и drain"
  ok "изменения с $2 совпадают"
  ;;
switch)
  [ -e "$RO" ] || die "запись не остановлена: сначала freeze"
  [ -f "$ETC/s3gw-reverse.env" ] || die "нет $ETC/s3gw-reverse.env (PIPELINE.md, C0.3)"
  rev_j=$(env_of JOURNAL_DB "$ETC/s3gw-reverse.env")
  addr=$(sed -nE 's/^ +listen ([0-9.]+):9000;.*/\1/p' "$NGINX_CONF" | head -1)
  [ -n "$addr" ] || die "в $NGINX_CONF нет listen <адрес>:9000 (PIPELINE.md, A4)"
  if [ -e "$ETC/s3gw-forward.env" ]; then
    fwd_j=$(env_of JOURNAL_DB "$ETC/s3gw-forward.env")
    echo "продолжение прерванного switch"
  else
    fwd_j=$(env_of JOURNAL_DB "$ETC/s3gw.env")
    [ -n "$rev_j" ] && [ "$rev_j" != "$fwd_j" ] || die "JOURNAL_DB в s3gw-reverse.env пуст или тот же, что в s3gw.env"
    [ ! -e "$rev_j" ] || die "$rev_j уже есть: обратный журнал должен начаться пустым (убрать в архив)"
    [ "$(env_of GW_READONLY_FILE "$ETC/s3gw-reverse.env")" = "$RO" ] || die "в s3gw-reverse.env другой GW_READONLY_FILE: пересобрать (C0.3)"
    run s3gw.state | jq -e .drained >/dev/null || die "очередь не пуста: сначала drain"
    cp -p "$ETC/s3gw.env" "$ETC/s3gw-forward.env"
    step forward-saved
  fi
  [ "$rev_j" != "$fwd_j" ] || die "JOURNAL_DB прямого и обратного конфига совпадают"
  systemctl stop s3gw-handler
  [ "$(env_of JOURNAL_DB "$ETC/s3gw.env")" = "$rev_j" ] || put_env "$ETC/s3gw-reverse.env"
  step env-swapped
  systemctl restart s3gw-gateway
  gateway_up
  step gateway-restarted
  systemctl enable --now s3gw-handler
  step handler-started
  reads_to s3gw_gateway
  step reads-switched
  journal_fresh "$rev_j"
  [ "$(probe_write)" = 503 ] || die "в обратном конфиге запрет записи не действует"
  # nginx ставит свой Server, но заголовки X-Minio-* пропускает; новое S3 их не шлёт
  curl -sI "http://$addr:9000/elma365-loki/" | grep -qi '^x-minio-' || die "другие бакеты больше не отвечает MinIO"
  ok "шлюз и чтения s3elma365 на новом S3, журнал $rev_j; проверить в ELMA365: старый файл открывается, загрузка даёт ошибку"
  ok "до open вернуться без потерь: undo. Мгновенный откат из A4 с этого момента запрещён"
  ;;
open)
  [ -e "$ETC/s3gw-forward.env" ] || die "переключения не было: сначала switch (или просто rm $RO)"
  if [ -e "$RO" ]; then
    rm "$RO"
    date -u +%FT%TZ
  else
    echo "запись уже открыта"
  fi
  ok "запись открыта, это T_CUTOVER; проверить в ELMA365: загрузка и удаление тестового файла, предпросмотр, большие файлы, подписанные ссылки"
  ;;
undo)
  [ -e "$RO" ] || die "запись уже открыта: после open только rollback (C4)"
  if [ -e "$ETC/s3gw-forward.env" ]; then
    fwd_j=$(env_of JOURNAL_DB "$ETC/s3gw-forward.env")
    rev_j=$(env_of JOURNAL_DB "$ETC/s3gw-reverse.env")
    [ -n "$rev_j" ] && [ "$rev_j" != "$fwd_j" ] || die "JOURNAL_DB прямого и обратного конфига совпадают"
    [ "$(intents_in "$rev_j")" = 0 ] || die "через $rev_j уже шла запись (был open): только rollback (C4)"
    systemctl stop s3gw-handler
    reads_to s3gw_minio
    step reads-back
    put_env "$ETC/s3gw-forward.env"
    step env-restored
    systemctl restart s3gw-gateway
    gateway_up
    step gateway-restarted
    systemctl start s3gw-handler
    step handler-started
    archive "$rev_j" undone   # запись была запрещена: через него ничего не прошло
    step archived
    [ "$(run s3gw.state | jq -r .journal)" = "$fwd_j" ] || die "шлюз не вернулся на $fwd_j"
    rm "$ETC/s3gw-forward.env"
    step forward-removed
  fi
  rm "$RO"
  ok "всё на MinIO, запись открыта"
  ;;
rollback)
  [ -e "$RO" ] || die "запись не остановлена: сначала freeze (C4.1)"
  cur=$(env_of JOURNAL_DB "$ETC/s3gw.env")
  if [ ! -e "$ETC/s3gw-forward.env" ]; then
    case "$cur" in
    */journal-after-rollback-*.db) rm "$RO"; ok "откат уже сделан, запись открыта"; exit 0 ;;
    *) die "нет $ETC/s3gw-forward.env: переключения не было" ;;
    esac
  fi
  rev_j=$(env_of JOURNAL_DB "$ETC/s3gw-reverse.env")
  case "$cur" in
  */journal-after-rollback-*.db)
    after_j=$cur
    echo "продолжение прерванного rollback"
    ;;
  *)
    [ "$cur" = "$rev_j" ] || die "в s3gw.env журнал $cur, а не обратный $rev_j: переключения не было?"
    run s3gw.state | jq -e .drained >/dev/null || die "обратная очередь не пуста: сначала drain (C4.2)"
    after_j=$(dirname "$rev_j")/journal-after-rollback-$(date -u +%Y%m%d-%H%M%S).db
    ;;
  esac
  systemctl disable --now s3gw-handler
  step handler-off
  reads_to s3gw_minio    # MinIO догнан (C4.2, C4.3); до перезапуска шлюза, который оборвёт запросы в полёте
  step reads-back
  [ "$(env_of JOURNAL_DB "$ETC/s3gw.env")" = "$after_j" ] || put_env "$ETC/s3gw-forward.env" "$after_j"
  step env-restored
  systemctl restart s3gw-gateway
  gateway_up
  step gateway-restarted
  ! systemctl is-active --quiet s3gw-handler || die "обратный обработчик всё ещё работает и перезапишет MinIO"
  archive "$rev_j" rolled-back
  step archived
  journal_fresh "$after_j"
  rm "$ETC/s3gw-forward.env"
  step forward-removed
  rm "$RO"
  ok "шлюз на MinIO, журнал $after_j, обработчик выключен; проверить в ELMA365: старый файл и файл после переключения открываются, загрузка проходит"
  ;;
esac
