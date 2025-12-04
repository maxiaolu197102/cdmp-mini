#!/usr/bin/env bash
# 对 Redis 集群做一次即时体检，采集慢日志、延迟、命令统计等关键信息。
#
# 用法：
#   ./redis_diag_snapshot.sh                     # 使用脚本内默认节点
#   NODES="host1:6379 host2:6379" ./redis_diag_snapshot.sh
#   ./redis_diag_snapshot.sh host1:6379 host2:6379
#
# 支持的环境变量：
#   NODES           空格分隔的 host:port 列表，优先级高于命令行参数。
#   OUTPUT_DIR      输出目录，默认为仓库 log/ 目录。
#   KEY_PREFIX      队列前缀（含 hash tag），用于对关键队列做额外检测，默认 genericapiserver:user:{user:queue}
#   QUEUE_TTL_WARN  任务 TTL 阈值，秒。超出后会提示。默认 600。

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

OUTPUT_DIR="${OUTPUT_DIR:-/home/mxl/cdmp-mini/cdmp-mini/log}"
mkdir -p "${OUTPUT_DIR}"
TIMESTAMP="$(date '+%Y%m%d-%H%M%S')"
OUTPUT_FILE="${OUTPUT_DIR}/redis_diag_${TIMESTAMP}.log"

KEY_PREFIX="${KEY_PREFIX:-genericapiserver:user:{user:queue}}"
QUEUE_READY_KEY="${KEY_PREFIX}:ready"
QUEUE_SCHED_KEY="${KEY_PREFIX}:scheduled"
QUEUE_INFLIGHT_KEY="${KEY_PREFIX}:inflight"
QUEUE_TTL_WARN=${QUEUE_TTL_WARN:-600}

print_header() {
  printf '\n===== %s =====\n' "$1" >>"${OUTPUT_FILE}"
}

run_cmd() {
  local node host port label
  node="$1"; shift
  label="$1"; shift
  host="${node%%:*}"
  port="${node##*:}"
  {
    printf '\n[%s] --- %s ---\n' "${node}" "${label}"
    if ! redis-cli -c -h "${host}" -p "${port}" "$@"; then
      printf '命令执行失败: redis-cli %s\n' "$*"
    fi
  } >>"${OUTPUT_FILE}"
}

queue_stats() {
  local node host port now ready scheduled inflight oldest result ttl
  node="$1"
  host="${node%%:*}"
  port="${node##*:}"
  {
    printf '\n[%s] --- queue snapshot (%s) ---\n' "${node}" "${KEY_PREFIX}"
    now="$(date '+%Y-%m-%dT%H:%M:%S%z')"
    printf '采集时间: %s\n' "${now}"
    ready="$(redis-cli -c -h "${host}" -p "${port}" LLEN "${QUEUE_READY_KEY}" 2>/dev/null || echo 'ERR')"
    scheduled="$(redis-cli -c -h "${host}" -p "${port}" ZCARD "${QUEUE_SCHED_KEY}" 2>/dev/null || echo 'ERR')"
    inflight="$(redis-cli -c -h "${host}" -p "${port}" SCARD "${QUEUE_INFLIGHT_KEY}" 2>/dev/null || echo 'ERR')"
    printf 'ready=%s scheduled=%s inflight=%s\n' "${ready}" "${scheduled}" "${inflight}"
    result="$(redis-cli -c -h "${host}" -p "${port}" --raw ZRANGE "${QUEUE_SCHED_KEY}" 0 0 WITHSCORES 2>/dev/null || true)"
    if [[ -n "${result}" ]]; then
      oldest="$(echo "${result}" | tail -n1)"
      if [[ -n "${oldest}" && "${oldest}" =~ ^[0-9]+$ ]]; then
        ttl=$(( oldest/1000 - $(date +%s) ))
        printf '最早调度项毫秒时间戳=%s (距当前 %s 秒)\n' "${oldest}" "${ttl}"
        if (( ttl > QUEUE_TTL_WARN )); then
          printf 'WARN: 队列内存在等待时间超过 %s 秒的任务\n' "${QUEUE_TTL_WARN}"
        fi
      fi
    fi
  } >>"${OUTPUT_FILE}"
}

collect_node_snapshot() {
  local node host port
  node="$1"
  host="${node%%:*}"
  port="${node##*:}"

  print_header "节点 ${node}"

  run_cmd "${node}" "PING" PING
  run_cmd "${node}" "CLUSTER INFO" CLUSTER INFO
  run_cmd "${node}" "CLUSTER NODES" CLUSTER NODES
  run_cmd "${node}" "延迟诊断" LATENCY DOCTOR
  run_cmd "${node}" "慢日志" SLOWLOG GET 64
  run_cmd "${node}" "命令统计" INFO commandstats
  run_cmd "${node}" "CPU 与 IO" INFO cpu
  run_cmd "${node}" "内存" INFO memory
  run_cmd "${node}" "客户端" CLIENT LIST
  run_cmd "${node}" "阻塞客户端" CLIENT LIST TYPE master
  run_cmd "${node}" "主从延迟" INFO replication
  run_cmd "${node}" "Keyspace" INFO keyspace
  run_cmd "${node}" "统计" INFO stats
  run_cmd "${node}" "当前事务" INFO persistence

  queue_stats "${node}"

  {
    printf '\n[%s] --- 示例 Eval 延迟测试 ---\n' "${node}"
    /usr/bin/time -f 'elapsed=%E user=%U sys=%S' redis-cli -c -h "${host}" -p "${port}" EVAL "return redis.call('LLEN', KEYS[1])" 1 "${QUEUE_READY_KEY}" >/dev/null 2>&1
  } >>"${OUTPUT_FILE}" 2>&1
}

print_header "Redis 诊断快照"
printf '采集时间: %s\n' "$(date '+%Y-%m-%dT%H:%M:%S%z')" >"${OUTPUT_FILE}"
printf '输出文件: %s\n' "${OUTPUT_FILE}" >>"${OUTPUT_FILE}"
printf '目标节点: %s\n' "${TARGET_NODES[*]}" >>"${OUTPUT_FILE}"
printf '队列前缀: %s\n' "${KEY_PREFIX}" >>"${OUTPUT_FILE}"
printf '阈值: QUEUE_TTL_WARN=%s 秒\n' "${QUEUE_TTL_WARN}" >>"${OUTPUT_FILE}"

for node in "${TARGET_NODES[@]}"; do
  collect_node_snapshot "${node}"
done

print_header "完成"
printf '诊断结果已写入 %s\n' "${OUTPUT_FILE}"
