#!/bin/bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: collect_perf_metrics.sh [options]

Options:
  --duration <seconds>   Total sampling duration (default: 300)
  --interval <seconds>   Sampling interval (default: 5)
  --mysql-host <host>    MySQL host (default: 192.168.10.8)
  --mysql-port <port>    MySQL port (default: 3306)
  --mysql-user <user>    MySQL user (default: root)
  --mysql-pass <pass>    MySQL password (default: iam59!z$)
  --mysql-db   <db>      Database for context (default: iam)
  --output-dir <path>    Directory for metric outputs (default: log/perf/<timestamp>)
  --tag <name>           Optional tag appended to output filenames
  -h, --help             Show this help

Collects MySQL thread metrics together with pidstat and iostat samples.
EOF
}

DURATION=300
INTERVAL=5
MYSQL_HOST="192.168.10.8"
MYSQL_PORT=3306
MYSQL_USER="root"
MYSQL_PASS='iam59!z$'
MYSQL_DB="iam"
OUTPUT_DIR=""
TAG=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --duration)
      DURATION="$2"; shift 2 ;;
    --interval)
      INTERVAL="$2"; shift 2 ;;
    --mysql-host)
      MYSQL_HOST="$2"; shift 2 ;;
    --mysql-port)
      MYSQL_PORT="$2"; shift 2 ;;
    --mysql-user)
      MYSQL_USER="$2"; shift 2 ;;
    --mysql-pass)
      MYSQL_PASS="$2"; shift 2 ;;
    --mysql-db)
      MYSQL_DB="$2"; shift 2 ;;
    --output-dir)
      OUTPUT_DIR="$2"; shift 2 ;;
    --tag)
      TAG="$2"; shift 2 ;;
    -h|--help)
      usage; exit 0 ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 1 ;;
  esac
done

if ! [[ "$DURATION" =~ ^[0-9]+$ ]] || [[ "$DURATION" -le 0 ]]; then
  echo "Invalid duration: $DURATION" >&2
  exit 1
fi
if ! [[ "$INTERVAL" =~ ^[0-9]+$ ]] || [[ "$INTERVAL" -le 0 ]]; then
  echo "Invalid interval: $INTERVAL" >&2
  exit 1
fi

if [[ -z "$OUTPUT_DIR" ]]; then
  timestamp=$(date +%Y%m%d-%H%M%S)
  OUTPUT_DIR="log/perf/${timestamp}"
fi
mkdir -p "$OUTPUT_DIR"

THREADS_FILE="$OUTPUT_DIR/mysql_threads${TAG:+_$TAG}.log"
PIDSTAT_FILE="$OUTPUT_DIR/pidstat${TAG:+_$TAG}.log"
IOSTAT_FILE="$OUTPUT_DIR/iostat${TAG:+_$TAG}.log"
SUMMARY_FILE="$OUTPUT_DIR/summary${TAG:+_$TAG}.txt"

SAMPLES=$(( (DURATION + INTERVAL - 1) / INTERVAL ))

if ! command -v mysql >/dev/null 2>&1; then
  echo "mysql client not found" >&2
  exit 1
fi
if ! command -v pidstat >/dev/null 2>&1; then
  echo "pidstat command not found (install sysstat)" >&2
  exit 1
fi
if ! command -v iostat >/dev/null 2>&1; then
  echo "iostat command not found (install sysstat)" >&2
  exit 1
fi

export MYSQL_PWD="$MYSQL_PASS"
MYSQL_CMD=(mysql --batch --silent --skip-column-names -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u "$MYSQL_USER" "$MYSQL_DB")

samples_collected=0
{
  echo "# timestamp threads_connected threads_running"
  for ((i=0; i< SAMPLES; i++)); do
    now=$(date +%Y-%m-%dT%H:%M:%S%z)
    threads_connected=$("${MYSQL_CMD[@]}" -e "SHOW GLOBAL STATUS LIKE 'Threads_connected';" | awk 'NR==1 {print $2}')
    threads_running=$("${MYSQL_CMD[@]}" -e "SHOW GLOBAL STATUS LIKE 'Threads_running';" | awk 'NR==1 {print $2}')
    if [[ -z "$threads_connected" ]]; then
      threads_connected="NA"
    fi
    if [[ -z "$threads_running" ]]; then
      threads_running="NA"
    fi
    echo "$now $threads_connected $threads_running"
    samples_collected=$((samples_collected + 1))
    if (( i < SAMPLES - 1 )); then
      sleep "$INTERVAL"
    fi
  done
} > "$THREADS_FILE" &
THREADS_PID=$!

PID_SAMPLES=$(( (DURATION + INTERVAL - 1) / INTERVAL ))
PIDSTAT_ITER=$(( (DURATION + INTERVAL - 1) / INTERVAL ))
if (( PIDSTAT_ITER < 1 )); then
  PIDSTAT_ITER=1
fi

pidstat -urd "$INTERVAL" "$PIDSTAT_ITER" > "$PIDSTAT_FILE" &
PIDSTAT_PID=$!

iostat -dx "$INTERVAL" "$PIDSTAT_ITER" > "$IOSTAT_FILE" &
IOSTAT_PID=$!

wait "$THREADS_PID"
wait "$PIDSTAT_PID" || true
wait "$IOSTAT_PID" || true

unset MYSQL_PWD

{
  echo "MySQL threads log   : $THREADS_FILE"
  echo "pidstat output      : $PIDSTAT_FILE"
  echo "iostat output       : $IOSTAT_FILE"
  echo "samples_collected   : $samples_collected"
  echo "duration_seconds    : $DURATION"
  echo "interval_seconds    : $INTERVAL"
  echo "mysql_host          : $MYSQL_HOST"
  echo "mysql_port          : $MYSQL_PORT"
  echo "mysql_user          : $MYSQL_USER"
  echo "tag                 : ${TAG:-<none>}"
} > "$SUMMARY_FILE"

cat "$SUMMARY_FILE"
