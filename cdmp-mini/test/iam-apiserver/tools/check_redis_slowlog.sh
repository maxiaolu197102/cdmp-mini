#!/bin/bash
# check_redis_slowlog.sh

REDIS_HOST="192.168.10.14"
PORTS=("6379" "6380" "6381")

echo "🔍 Redis集群慢查询分析 - $(date)"
echo "======================================"

for port in "${PORTS[@]}"; do
    echo ""
    echo "=== 节点 $port 慢查询分析 ==="
    
    # 检查慢查询数量
    slow_count=$(redis-cli -h $REDIS_HOST -p $port SLOWLOG LEN)
    echo "慢查询数量: $slow_count"
    
    # 获取慢查询详情
    if [ "$slow_count" -gt 0 ]; then
        echo "--- 最近 ${slow_count} 条慢查询 ---"
        redis-cli -h $REDIS_HOST -p $port SLOWLOG GET $slow_count | \
        while read line; do
            if echo "$line" | grep -q "user:pending"; then
                echo "🚨 发现目标慢查询: $line"
            fi
        done
    fi
    
    # 检查节点角色和槽位分布
    echo "--- 节点信息 ---"
    redis-cli -h $REDIS_HOST -p $port CLUSTER NODES | grep "$REDIS_HOST:$port"
done