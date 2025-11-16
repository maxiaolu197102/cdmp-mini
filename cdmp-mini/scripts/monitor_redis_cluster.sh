#!/usr/bin/env bash
# 持续执行 redis-cli PING / CLUSTER INFO / CLUSTER NODES，定位失联节点

set -u

# 默认为配置中的 Redis 集群节点，可通过命令行参数覆盖
DEFAULT_NODES=(
  "192.168.10.14:6379"
  "192.168.10.14:6380"
  "192.168.10.14:6381"
)

if [[ $# -gt 0 ]]; then
  NODES=("$@")
else
  NODES=()
  for node in "${DEFAULT_NODES[@]}"; do
    NODES+=("$node")
  done
fi

INTERVAL="${INTERVAL:-30}"      # 两次采样时间间隔（秒）
LOG_DIR="${LOG_DIR:-/home/mxl/cdmp-mini/cdmp-mini/log}"  # 输出目录，可通过环境变量覆盖
LOG_FILE="${LOG_FILE:-$LOG_DIR/redis_cluster_watch.log}"

mkdir -p "$LOG_DIR"

if ! command -v redis-cli >/dev/null 2>&1; then
  echo "redis-cli 未安装，无法执行检查" >&2
  exit 1
fi

if [[ ${#NODES[@]} -eq 0 ]]; then
  echo "未指定任何 Redis 节点" >&2
  exit 1
fi

log_run() {
  local node="$1"
  shift
  local ts shell_cmd
  ts="$(date '+%Y-%m-%dT%H:%M:%S%z')"
  shell_cmd="$*"
  {
    printf '[%s] [%s] >>> %s\n' "$ts" "$node" "$shell_cmd"
    "$@"
    local status=$?
    printf '[%s] [%s] <<< exit=%d\n' "$ts" "$node" "$status"
  } >>"$LOG_FILE" 2>&1
}

echo "$(date '+%Y-%m-%dT%H:%M:%S%z') 监控启动，节点: ${NODES[*]}" >>"$LOG_FILE"

while true; do
  for node in "${NODES[@]}"; do
    host="${node%%:*}"
    port="${node##*:}"

    log_run "$node" redis-cli -c -h "$host" -p "$port" PING
    log_run "$node" redis-cli -c -h "$host" -p "$port" CLUSTER INFO
    log_run "$node" redis-cli -c -h "$host" -p "$port" CLUSTER NODES
  done
  sleep "$INTERVAL"
done
