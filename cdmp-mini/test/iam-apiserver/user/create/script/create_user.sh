#!/bin/bash

usage() {
  echo "用法: $0 --token <认证令牌>"
  echo "示例: $0 --token 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...'"
  exit 1
}

API_URL="http://192.168.10.8:8088/v1/users"
USER_METADATA_NAME="fixed_user"
USER_PASSWORD="Fixed@2024"
USER_EMAIL="fixed_user@example.com"
USER_NICKNAME="固定用户"
USER_PHONE="13800138000"
USER_IS_ADMIN=0
USER_STATUS=1

TOKEN=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --token)
      TOKEN="$2"
      shift 2
      ;;
    *)
      echo "错误：未知参数 $1"
      usage
      ;;
  esac
done

if [[ -z "$TOKEN" ]]; then
  echo "错误：请通过 --token 指定认证令牌！"
  usage
fi
if [[ -z "$USER_METADATA_NAME" ]]; then
  echo "错误：脚本中 USER_METADATA_NAME 未赋值！"
  exit 1
fi
if [[ -z "$USER_PASSWORD" || -z "$USER_EMAIL" ]]; then
  echo "错误：密码或邮箱未赋值！"
  exit 1
fi

echo "调试信息："
echo "metadata.name: $USER_METADATA_NAME"
echo "请求URL: $API_URL"
echo "------------------------"

# 核心修正：移除 JSON 中的所有注释
curl -X POST "${API_URL}" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${TOKEN}" \
  -d '{
    "metadata": {
      "name": "'"${USER_METADATA_NAME}"'"
    },
    "password": "'"${USER_PASSWORD}"'",
    "email": "'"${USER_EMAIL}"'",
    "nickname": "'"${USER_NICKNAME}"'",
    "phone": "'"${USER_PHONE}"'",
    "isAdmin": '"${USER_IS_ADMIN}"',
    "status": '"${USER_STATUS}"'
  }'