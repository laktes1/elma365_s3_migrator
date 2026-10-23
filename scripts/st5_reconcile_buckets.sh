#!/usr/bin/env bash
# Stage 5: compare the source and destination buckets by listing (key, size, ETag), PIPELINE.md C1.
# Copies nothing: discrepancies go to reconcile_queue and dst_only in ST5_STATE_DB for a re-check.
#
# Parameters are environment variables with the defaults below, e.g.
#   WATERMARK=2026-11-20T18:00:00Z bash scripts/st5_reconcile_buckets.sh
set -euo pipefail
trap 'echo "[st5_reconcile] ERROR at line $LINENO (exit $?)" >&2' ERR

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

usage() {
  cat <<'EOF'
Usage:
  bash scripts/st5_reconcile_buckets.sh [--phase verify] [--help]

Examples:
  bash scripts/st5_reconcile_buckets.sh
  WATERMARK=2026-11-20T18:00:00Z bash scripts/st5_reconcile_buckets.sh --phase verify
  ONLY_PREFIXES=00,01,__catch_all__ bash scripts/st5_reconcile_buckets.sh
EOF
}

PHASE="${PHASE:-verify}"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --phase) PHASE="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

PROJECT_DIR="${PROJECT_DIR:-$(cd "$SCRIPT_DIR/.." && pwd)}"
SRC_ALIAS="${SRC_ALIAS:-minio_old}"
SRC_BUCKET="${SRC_BUCKET:-s3elma365}"
DST_ALIAS="${DST_ALIAS:-minio_new}"
DST_BUCKET="${DST_BUCKET:-s3elma365}"

ST5_RUNTIME_DIR="${ST5_RUNTIME_DIR:-$PROJECT_DIR/.runtime/st5-reconcile}"
ST5_STATE_DB="${ST5_STATE_DB:-$ST5_RUNTIME_DIR/state.db}"
ST5_REPORT_FILE="${ST5_REPORT_FILE:-$ST5_RUNTIME_DIR/report.json}"
ST5_PROGRESS_FILE="${ST5_PROGRESS_FILE:-$ST5_RUNTIME_DIR/progress.jsonl}"
ST5_ERROR_LOG="${ST5_ERROR_LOG:-$ST5_RUNTIME_DIR/errors.log}"

PREFIX_LEN="${PREFIX_LEN:-2}"
LIST_WORKERS="${LIST_WORKERS:-8}"
LIST_PAGE_SIZE="${LIST_PAGE_SIZE:-1000}"
LIST_LOG_EVERY_PAGES="${LIST_LOG_EVERY_PAGES:-100}"
S3_READ_TIMEOUT="${S3_READ_TIMEOUT:-300}"
SRC_LIST_WORKERS="${SRC_LIST_WORKERS:-1}"  # old MinIO times out on parallel listing
DST_LIST_WORKERS="${DST_LIST_WORKERS:-$LIST_WORKERS}"
SRC_LIST_PAGE_SIZE="${SRC_LIST_PAGE_SIZE:-100}"
DST_LIST_PAGE_SIZE="${DST_LIST_PAGE_SIZE:-$LIST_PAGE_SIZE}"
SRC_S3_READ_TIMEOUT="${SRC_S3_READ_TIMEOUT:-900}"
DST_S3_READ_TIMEOUT="${DST_S3_READ_TIMEOUT:-$S3_READ_TIMEOUT}"
START_PREFIX="${START_PREFIX:-}"
STOP_PREFIX="${STOP_PREFIX:-}"
ONLY_PREFIXES="${ONLY_PREFIXES:-}"
WATERMARK="${WATERMARK:-}"
WATERMARK_DELAY_MINUTES="${WATERMARK_DELAY_MINUTES:-10}"
MC_BIN="${MC_BIN:-mc}"

if [[ ! -d "$PROJECT_DIR" ]]; then
  echo "CONFIG ERROR: PROJECT_DIR does not exist: $PROJECT_DIR"
  exit 1
fi

mkdir -p "$ST5_RUNTIME_DIR" "$(dirname "$ST5_STATE_DB")" "$(dirname "$ST5_REPORT_FILE")"
touch "$ST5_ERROR_LOG"
cd "$PROJECT_DIR"

echo "[st5] Bucket reconcile"
echo "  phase:             $PHASE"
echo "  src:               $SRC_ALIAS/$SRC_BUCKET"
echo "  dst:               $DST_ALIAS/$DST_BUCKET"
echo "  state_db:          $ST5_STATE_DB"
echo "  report:            $ST5_REPORT_FILE"
echo "  progress:          $ST5_PROGRESS_FILE"
echo "  prefix_len:        $PREFIX_LEN"
echo "  list_workers:      $LIST_WORKERS"
echo "  src_list_workers:  $SRC_LIST_WORKERS"
echo "  dst_list_workers:  $DST_LIST_WORKERS"
echo "  list_page_size:    $LIST_PAGE_SIZE"
echo "  src_page_size:     $SRC_LIST_PAGE_SIZE"
echo "  dst_page_size:     $DST_LIST_PAGE_SIZE"
echo "  list_log_pages:    $LIST_LOG_EVERY_PAGES"
echo "  s3_read_timeout:   $S3_READ_TIMEOUT"
echo "  src_read_timeout:  $SRC_S3_READ_TIMEOUT"
echo "  dst_read_timeout:  $DST_S3_READ_TIMEOUT"
echo "  start_prefix:      ${START_PREFIX:-<none>}"
echo "  stop_prefix:       ${STOP_PREFIX:-<none>}"
echo "  only_prefixes:     ${ONLY_PREFIXES:-<none>}"
echo "  watermark:         ${WATERMARK:-now-${WATERMARK_DELAY_MINUTES}m}"
echo ""

CMD=(
  python3 -u "$PROJECT_DIR/migrator/reconcile_buckets.py"
  --phase "$PHASE"
  --src-alias "$SRC_ALIAS"
  --src-bucket "$SRC_BUCKET"
  --dst-alias "$DST_ALIAS"
  --dst-bucket "$DST_BUCKET"
  --state-db "$ST5_STATE_DB"
  --report-file "$ST5_REPORT_FILE"
  --progress-file "$ST5_PROGRESS_FILE"
  --error-log-file "$ST5_ERROR_LOG"
  --mc-bin "$MC_BIN"
  --prefix-len "$PREFIX_LEN"
  --list-workers "$LIST_WORKERS"
  --src-list-workers "$SRC_LIST_WORKERS"
  --dst-list-workers "$DST_LIST_WORKERS"
  --list-page-size "$LIST_PAGE_SIZE"
  --src-list-page-size "$SRC_LIST_PAGE_SIZE"
  --dst-list-page-size "$DST_LIST_PAGE_SIZE"
  --list-log-every-pages "$LIST_LOG_EVERY_PAGES"
  --s3-read-timeout "$S3_READ_TIMEOUT"
  --src-s3-read-timeout "$SRC_S3_READ_TIMEOUT"
  --dst-s3-read-timeout "$DST_S3_READ_TIMEOUT"
  --watermark-delay-minutes "$WATERMARK_DELAY_MINUTES"
  --start-prefix "$START_PREFIX"
  --stop-prefix "$STOP_PREFIX"
  --only-prefixes "$ONLY_PREFIXES"
)
[[ -n "$WATERMARK" ]] && CMD+=(--watermark "$WATERMARK")

"${CMD[@]}"
