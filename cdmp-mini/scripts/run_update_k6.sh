#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'EOF'
用法: run_update_k6.sh -u BASE_URL -d DATASET.json [-t TOKEN | -a USER -p PASS] [-- k6参数]

必需参数:
  -u, --base-url        目标 IAM API 根地址 (例如 https://iam.example.com)
  -d, --dataset         update.js 使用的 JSON 数据集文件路径

管理员凭据 (二选一):
  -t, --admin-token     直接传入管理员 access token
  -a, --admin-username  管理员用户名 (需与 -p 一起使用)
  -p, --admin-password  管理员密码 (需与 -a 一起使用)

可选参数:
      --summary FILE    将 k6 summary 输出到指定文件 (默认写入 output/history 下)
  -h, --help            显示本帮助并退出

其余参数会原样传递给 k6 run (在参数列表中使用 "--" 分隔)。
EOF
}

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PROJECT_ROOT=$(cd "${SCRIPT_DIR}/.." && pwd)
SCENARIO_FILE="${PROJECT_ROOT}/test/iam-apiserver/user/update/k6/update.js"
DEFAULT_OUTPUT_DIR="${PROJECT_ROOT}/test/iam-apiserver/user/update/output/history"

BASE_URL=""
ADMIN_TOKEN_VALUE=""
ADMIN_USERNAME_VALUE=""
ADMIN_PASSWORD_VALUE=""
DATASET_FILE=""
SUMMARY_FILE=""
K6_ARGS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        -u|--base-url)
            BASE_URL="${2:-}"
            shift 2
            ;;
        -d|--dataset)
            DATASET_FILE="${2:-}"
            shift 2
            ;;
        -t|--admin-token)
            ADMIN_TOKEN_VALUE="${2:-}"
            shift 2
            ;;
        -a|--admin-username)
            ADMIN_USERNAME_VALUE="${2:-}"
            shift 2
            ;;
        -p|--admin-password)
            ADMIN_PASSWORD_VALUE="${2:-}"
            shift 2
            ;;
        --summary)
            SUMMARY_FILE="${2:-}"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        --)
            shift
            K6_ARGS+=("$@")
            break
            ;;
        *)
            echo "未知参数: $1" >&2
            usage
            exit 1
            ;;
    esac
done

if [[ -z "${BASE_URL}" ]]; then
    echo "缺少 --base-url" >&2
    usage
    exit 1
fi

if [[ -z "${DATASET_FILE}" ]]; then
    echo "缺少 --dataset" >&2
    usage
    exit 1
fi

if [[ -z "${ADMIN_TOKEN_VALUE}" ]]; then
    if [[ -z "${ADMIN_USERNAME_VALUE}" || -z "${ADMIN_PASSWORD_VALUE}" ]]; then
        echo "请提供 --admin-token 或 --admin-username/--admin-password" >&2
        usage
        exit 1
    fi
else
    if [[ -n "${ADMIN_USERNAME_VALUE}" || -n "${ADMIN_PASSWORD_VALUE}" ]]; then
        echo "--admin-token 与 用户名/密码 不能同时使用" >&2
        exit 1
    fi
fi

if [[ ! -f "${SCENARIO_FILE}" ]]; then
    echo "未找到 update.js: ${SCENARIO_FILE}" >&2
    exit 1
fi

if [[ ! -f "${DATASET_FILE}" ]]; then
    echo "未找到数据集文件: ${DATASET_FILE}" >&2
    exit 1
fi

if ! command -v k6 >/dev/null 2>&1; then
    echo "未检测到 k6，请先安装: https://k6.io/docs/getting-started/installation/" >&2
    exit 1
fi

# 允许通过 K6_DURATION 环境变量统一缩短/拉长所有场景时长。
# 该变量是 k6 的内置配置，如果直接传给 k6 会覆盖脚本自定义的 scenarios。
# 因此在启动前把它取出转发到自定义变量，并从环境中删除，避免触发 k6 内置 fallback。
K6_DURATION_OVERRIDE_VALUE="${K6_DURATION:-}"
if [[ -n "${K6_DURATION_OVERRIDE_VALUE}" ]]; then
    export K6_DURATION_OVERRIDE="${K6_DURATION_OVERRIDE_VALUE}"
    unset K6_DURATION
fi

BASE_URL=$(printf '%s' "${BASE_URL}" | sed 's#[[:space:]]##g')
BASE_URL=${BASE_URL%%/}
if [[ ! "${BASE_URL}" =~ ^https?:// ]]; then
    echo "BASE_URL 必须以 http:// 或 https:// 开头" >&2
    exit 1
fi

if [[ -z "${SUMMARY_FILE}" ]]; then
    mkdir -p "${DEFAULT_OUTPUT_DIR}"
    SUMMARY_FILE="${DEFAULT_OUTPUT_DIR}/update_k6_$(date +%Y%m%d-%H%M%S).json"
else
    mkdir -p "$(dirname "${SUMMARY_FILE}")"
fi

if ! DATASET_JSON=$(DATASET_FILE_PATH="${DATASET_FILE}" python3 <<'PY'
import json
import os
import sys
from pathlib import Path

path = Path(os.environ['DATASET_FILE_PATH'])
try:
    text = path.read_text(encoding='utf-8')
except FileNotFoundError:
    print(f"dataset file not found: {path}", file=sys.stderr)
    raise

data = json.loads(text)
print(json.dumps(data, separators=(',', ':')))
PY
); then
    echo "加载数据集失败" >&2
    exit 1
fi


export BASE_URL="${BASE_URL}"
export UPDATE_DATASET="${DATASET_JSON}"
unset ADMIN_TOKEN ADMIN_USERNAME ADMIN_PASSWORD

if [[ -n "${ADMIN_TOKEN_VALUE}" ]]; then
    export ADMIN_TOKEN="${ADMIN_TOKEN_VALUE}"
else
    export ADMIN_USERNAME="${ADMIN_USERNAME_VALUE}"
    export ADMIN_PASSWORD="${ADMIN_PASSWORD_VALUE}"
fi

CMD=(k6 run --summary-export "${SUMMARY_FILE}")
if [[ ${#K6_ARGS[@]} -gt 0 ]]; then
    CMD+=("${K6_ARGS[@]}")
fi
CMD+=("${SCENARIO_FILE}")

echo "运行 k6: ${CMD[*]}"
"${CMD[@]}"

echo "运行完成，摘要输出: ${SUMMARY_FILE}"
