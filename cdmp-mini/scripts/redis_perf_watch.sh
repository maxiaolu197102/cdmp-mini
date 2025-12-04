#!/usr/bin/env bash
# 轮询采集 Redis 慢日志、延迟诊断与集群指标，便于压测期间定位热点命令和资源瓶颈。
#
# 使用方法：
#   ./scripts/redis_perf_watch.sh                              # 使用默认节点与间隔
#   NODES="host1:6379 host2:6380" ./scripts/redis_perf_watch.sh
#   INTERVAL=3 SLOWLOG_DEPTH=128 ./scripts/redis_perf_watch.sh 10.0.0.1:6379
#
# 可配置环境变量：
#   NODES                      空格分隔的 host:port 列表，优先级高于参数。
#   AUTO_DISCOVER              是否自动从集群拓扑补全节点列表(默认开启，设为0关闭)。
#   OUTPUT_DIR                 输出目录，默认仓库 log/ 目录。
#   INTERVAL                   采样间隔秒，默认 5。
#   SLOWLOG_DEPTH              每次拉取的慢日志条数，默认 64。
#   LATENCY_DOCTOR_INTERVAL    每多少次循环执行一次 LATENCY DOCTOR，默认 6。
#   COMMANDSTATS_INTERVAL      执行 INFO commandstats 的循环间隔，默认 1。
#   CLUSTER_INFO_INTERVAL      执行 CLUSTER INFO 的循环间隔，默认 1。
#   INFO_STATS_INTERVAL        执行 INFO stats 的循环间隔，默认 1。

set -euo pipefail

DEFAULT_NODES=(
  "192.168.10.14:6379"
  "192.168.10.14:6380"
  "192.168.10.14:6381"
)

if [[ -n "${NODES:-}" ]]; then
  read -r -a TARGET_NODES <<<"${NODES}"
elif [[ $# -gt 0 ]]; then
  TARGET_NODES=("$@")
else
  TARGET_NODES=("${DEFAULT_NODES[@]}")
fi

if [[ ${#TARGET_NODES[@]} -eq 0 ]]; then
  echo "未指定 Redis 节点" >&2
  exit 1
fi

if ! command -v redis-cli >/dev/null 2>&1; then
  echo "redis-cli 未安装" >&2
  exit 1
fi

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 未安装" >&2
  exit 1
fi

OUTPUT_DIR="${OUTPUT_DIR:-/home/mxl/cdmp-mini/cdmp-mini/log}"
mkdir -p "${OUTPUT_DIR}"
RUN_ID="$(date '+%Y%m%d-%H%M%S')"
LOG_FILE="${OUTPUT_DIR}/redis_perf_watch_${RUN_ID}.log"

INTERVAL="${INTERVAL:-5}"
SLOWLOG_DEPTH="${SLOWLOG_DEPTH:-64}"
LATENCY_DOCTOR_INTERVAL="${LATENCY_DOCTOR_INTERVAL:-6}"
COMMANDSTATS_INTERVAL="${COMMANDSTATS_INTERVAL:-1}"
CLUSTER_INFO_INTERVAL="${CLUSTER_INFO_INTERVAL:-1}"
INFO_STATS_INTERVAL="${INFO_STATS_INTERVAL:-1}"

declare -A LAST_SLOWLOG_ID

node_exists() {
  local needle="$1"
  shift
  local n
  for n in "$@"; do
    if [[ "$n" == "$needle" ]]; then
      return 0
    fi
  done
  return 1
}

discover_cluster_nodes() {
  local seeds=() node host port line hostport
  seeds=("${TARGET_NODES[@]}")
  for node in "${seeds[@]}"; do
    host="${node%%:*}"
    port="${node##*:}"
    while IFS= read -r line; do
      [[ -z "$line" ]] && continue
      hostport="${line%%@*}"
      [[ "$hostport" != *:* ]] && continue
      if ! node_exists "$hostport" "${TARGET_NODES[@]}"; then
        TARGET_NODES+=("$hostport")
      fi
    done < <(redis-cli -c -h "$host" -p "$port" --raw CLUSTER NODES 2>/dev/null | awk '{print $2}')
  done
}

if [[ "${AUTO_DISCOVER:-1}" != "0" ]]; then
  discover_cluster_nodes
fi

log_start_ts="$(date '+%Y-%m-%dT%H:%M:%S%z')"
printf '[%s] monitor start\n' "${log_start_ts}" >>"${LOG_FILE}"
printf 'targets: %s\n' "${TARGET_NODES[*]}" >>"${LOG_FILE}"
printf 'interval=%s slowlog_depth=%s latency_doctor_interval=%s\n' \
  "${INTERVAL}" "${SLOWLOG_DEPTH}" "${LATENCY_DOCTOR_INTERVAL}" >>"${LOG_FILE}"
printf 'commandstats_interval=%s cluster_info_interval=%s info_stats_interval=%s\n' \
  "${COMMANDSTATS_INTERVAL}" "${CLUSTER_INFO_INTERVAL}" "${INFO_STATS_INTERVAL}" >>"${LOG_FILE}"
printf 'log_file=%s\n' "${LOG_FILE}" >>"${LOG_FILE}"

cleanup() {
  local code=$?
  local ts
  ts="$(date '+%Y-%m-%dT%H:%M:%S%z')"
  printf '[%s] monitor stop exit=%d\n' "${ts}" "${code}" >>"${LOG_FILE}"
}
trap cleanup EXIT

run_redis_cmd() {
  local node="$1" stamp="$2" label="$3"
  shift 3
  local host="${node%%:*}"
  local port="${node##*:}"
  {
    printf '\n[%s] [%s] --- %s ---\n' "${stamp}" "${node}" "${label}"
    if ! redis-cli -c -h "${host}" -p "${port}" "$@"; then
      local status=$?
      printf '命令执行失败 exit=%d\n' "${status}"
    fi
  } >>"${LOG_FILE}" 2>&1
}

collect_slowlog() {
  local node="$1" stamp="$2"
  local host="${node%%:*}"
  local port="${node##*:}"
  local last_id="${LAST_SLOWLOG_ID[$node]:-0}"
  local len_output
  len_output=$(redis-cli -c -h "${host}" -p "${port}" --raw SLOWLOG LEN 2>/dev/null || echo "ERR")
  {
    printf '\n[%s] [%s] --- slowlog window ---\n' "${stamp}" "${node}"
    printf 'length=%s depth=%s last_seen_id=%s\n' "${len_output}" "${SLOWLOG_DEPTH}" "${last_id}"
  } >>"${LOG_FILE}"

  local slowlog_output
  if ! slowlog_output=$(redis-cli --csv -c -h "${host}" -p "${port}" SLOWLOG GET "${SLOWLOG_DEPTH}" 2>/dev/null); then
    local status=$?
    printf '[%s] [%s] SLOWLOG GET 失败 exit=%d\n' "${stamp}" "${node}" "${status}" >>"${LOG_FILE}"
    return
  fi

  local tmp_file new_last
  tmp_file=$(mktemp)
  NODE="${node}" LAST_ID="${last_id}" STAMP="${stamp}" python3 <<'PY' 1>>"${LOG_FILE}" 2>"${tmp_file}" <<<"${slowlog_output}"
import csv
import io
import os
import sys
import time
import shlex

node = os.environ.get("NODE", "?")
last = int(os.environ.get("LAST_ID", "0") or 0)
stamp = os.environ.get("STAMP", "?")

data = sys.stdin.read()
if not data.strip():
    print(last, file=sys.stderr)
    sys.exit(0)

reader = csv.reader(io.StringIO(data))
rows = [row for row in reader if row]
try:
    rows.sort(key=lambda r: int(r[0]))
except Exception:
    rows = []

new_last = last
for row in rows:
    if len(row) < 3:
        continue
    try:
        entry_id = int(row[0])
    except ValueError:
        continue
    if entry_id > new_last:
        new_last = entry_id
    if entry_id <= last:
        continue
    try:
        ts_val = int(row[1])
    except ValueError:
        ts_val = 0
    try:
        duration_us = int(row[2])
    except ValueError:
        duration_us = 0
    if len(row) >= 6:
        cmd_parts = row[3:-2]
        client = row[-2] or "-"
        client_name = row[-1] or "-"
    else:
        cmd_parts = row[3:]
        client = "-"
        client_name = "-"
    if not cmd_parts:
        cmd_parts = ["(nil)"]
    command = " ".join(shlex.quote(part) for part in cmd_parts)
    slow_ts = time.strftime('%Y-%m-%dT%H:%M:%S', time.localtime(ts_val)) if ts_val else "unknown"
    duration_ms = duration_us / 1000.0
    print(f"[{stamp}] [{node}] slowlog id={entry_id} at={slow_ts} duration={duration_ms:.3f}ms cmd={command} client={client} name={client_name}")

if new_last < last:
    new_last = last
print(new_last, file=sys.stderr)
PY
  new_last=$(<"${tmp_file}")
  rm -f "${tmp_file}"

  if [[ "${new_last}" =~ ^[0-9]+$ ]]; then
    LAST_SLOWLOG_ID[$node]="${new_last}"
  fi

  if [[ ! "${new_last}" =~ ^[0-9]+$ || "${new_last}" == "${last_id}" ]]; then
    printf '[%s] [%s] slowlog 无新增记录\n' "${stamp}" "${node}" >>"${LOG_FILE}"
  fi
}

iteration=0
while true; do
  iteration=$((iteration + 1))
  timestamp="$(date '+%Y-%m-%dT%H:%M:%S%z')"
  printf '\n[%s] ===== iteration %d =====\n' "${timestamp}" "${iteration}" >>"${LOG_FILE}"

  for node in "${TARGET_NODES[@]}"; do
    collect_slowlog "${node}" "${timestamp}"
    run_redis_cmd "${node}" "${timestamp}" "LATENCY LATEST" LATENCY LATEST
    if (( LATENCY_DOCTOR_INTERVAL > 0 )) && (( iteration % LATENCY_DOCTOR_INTERVAL == 0 )); then
      run_redis_cmd "${node}" "${timestamp}" "LATENCY DOCTOR" LATENCY DOCTOR
    fi
    if (( COMMANDSTATS_INTERVAL > 0 )) && (( iteration % COMMANDSTATS_INTERVAL == 0 )); then
      run_redis_cmd "${node}" "${timestamp}" "INFO commandstats" INFO commandstats
    fi
    if (( CLUSTER_INFO_INTERVAL > 0 )) && (( iteration % CLUSTER_INFO_INTERVAL == 0 )); then
      run_redis_cmd "${node}" "${timestamp}" "CLUSTER INFO" CLUSTER INFO
    fi
    if (( INFO_STATS_INTERVAL > 0 )) && (( iteration % INFO_STATS_INTERVAL == 0 )); then
      run_redis_cmd "${node}" "${timestamp}" "INFO stats" INFO stats
    fi
  done

  sleep "${INTERVAL}"
done
