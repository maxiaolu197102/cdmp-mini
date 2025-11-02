#!/bin/bash

# Kafka集群配置
KAFKA_BROKERS=("192.168.10.8:9092" "192.168.10.8:9093" "192.168.10.8:9094")
BROKERS_STRING=$(IFS=','; echo "${KAFKA_BROKERS[*]}")

# 颜色定义
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 检查kafka-topics.sh是否可用
if ! command -v kafka-topics.sh &> /dev/null; then
    echo -e "${RED}错误: kafka-topics.sh 命令未找到。请确保Kafka已安装并添加到PATH中。${NC}"
    exit 1
fi

# 检查kafka-consumer-groups.sh是否可用
if ! command -v kafka-consumer-groups.sh &> /dev/null; then
    echo -e "${RED}错误: kafka-consumer-groups.sh 命令未找到。请确保Kafka已安装并添加到PATH中。${NC}"
    exit 1
fi

# 列出所有主题
list_topics() {
    echo -e "${YELLOW}正在获取所有主题...${NC}"
    TOPICS=($(kafka-topics.sh --bootstrap-server $BROKERS_STRING --list 2>/dev/null))
    
    if [ $? -ne 0 ]; then
        echo -e "${RED}无法连接到Kafka集群。请检查集群地址和网络连接。${NC}"
        exit 1
    fi
    
    if [ ${#TOPICS[@]} -eq 0 ]; then
        echo -e "${YELLOW}没有找到任何主题。${NC}"
        return 1
    fi
    
    echo -e "${GREEN}找到 ${#TOPICS[@]} 个主题:${NC}"
    for i in "${!TOPICS[@]}"; do
        echo "[$i] ${TOPICS[$i]}"
    done
    echo ""
    return 0
}

# 获取主题的分区信息
get_topic_partitions() {
    local topic=$1
    local partitions=($(kafka-topics.sh --bootstrap-server $BROKERS_STRING --describe --topic $topic 2>/dev/null | grep Partition | awk '{print $2}'))
    echo "${partitions[@]}"
}

# 删除并重建主题（彻底清除数据）
clean_topic() {
    local topic=$1
    local partitions=($(get_topic_partitions $topic))
    
    if [ ${#partitions[@]} -eq 0 ]; then
        echo -e "${YELLOW}无法获取主题 $topic 的分区信息。${NC}"
        return 1
    fi
    
    # 获取主题配置
    local topic_config=$(kafka-topics.sh --bootstrap-server $BROKERS_STRING --describe --topic "$topic" 2>/dev/null)
    local partition_count=$(echo "$topic_config" | grep Partition | wc -l)
    local replication_factor=$(echo "$topic_config" | grep ReplicationFactor | head -1 | awk '{print $3}')
    
    echo -e "${YELLOW}正在处理主题 $topic (分区数: $partition_count, 复制因子: $replication_factor)...${NC}"
    
    # 检查是否为内部主题
    if [[ "$topic" == "__consumer_offsets" ]]; then
        echo -e "${RED}错误: 不能删除内部主题 $topic${NC}"
        return 1
    fi
    
    # 第一步：删除主题
    echo -e "${YELLOW}  - 删除主题 $topic...${NC}"
    kafka-topics.sh --bootstrap-server $BROKERS_STRING \
                   --delete \
                   --topic "$topic" >/dev/null 2>&1
    
    if [ $? -ne 0 ]; then
        echo -e "    ${RED}✗ 删除主题失败${NC}"
        echo -e "${YELLOW}  尝试使用强制删除...${NC}"
        
        # 尝试强制删除
        kafka-topics.sh --bootstrap-server $BROKERS_STRING \
                       --delete \
                       --topic "$topic" \
                       --if-exists >/dev/null 2>&1
        
        if [ $? -ne 0 ]; then
            echo -e "    ${RED}✗ 强制删除也失败${NC}"
            return 1
        fi
    fi
    
    echo -e "    ${GREEN}✓ 主题已删除${NC}"
    
    # 等待主题删除完成
    echo -e "${YELLOW}  - 等待主题删除完成...${NC}"
    sleep 3
    
    # 第二步：重建主题
    echo -e "${YELLOW}  - 重建主题 $topic...${NC}"
    kafka-topics.sh --bootstrap-server $BROKERS_STRING \
                   --create \
                   --topic "$topic" \
                   --partitions "$partition_count" \
                   --replication-factor "$replication_factor" >/dev/null 2>&1
    
    if [ $? -eq 0 ]; then
        echo -e "    ${GREEN}✓ 主题已重建${NC}"
        echo -e "${GREEN}主题 $topic 已成功清空（通过删除并重建）${NC}"
        return 0
    else
        echo -e "    ${RED}✗ 重建主题失败${NC}"
        echo -e "${YELLOW}注意: 主题已删除但重建失败，请手动检查并重建${NC}"
        return 1
    fi
}

# 清空所有主题
clean_all_topics() {
    echo -e "${YELLOW}准备清空所有 ${#TOPICS[@]} 个主题...${NC}"
    
    local success_count=0
    local fail_count=0
    
    for i in "${!TOPICS[@]}"; do
        local topic=${TOPICS[$i]}
        echo -e "\n${YELLOW}[${i}/${#TOPICS[@]}] 处理主题: $topic${NC}"
        
        clean_topic "$topic"
        if [ $? -eq 0 ]; then
            success_count=$((success_count + 1))
        else
            fail_count=$((fail_count + 1))
        fi
        
        # 显示进度
        local progress=$(( (i + 1) * 100 / ${#TOPICS[@]} ))
        echo -ne "${YELLOW}进度: ${progress}%${NC}\r"
    done
    
    echo -e "\n${GREEN}清空完成!${NC}"
    echo -e "${GREEN}成功: ${success_count} 个主题${NC}"
    if [ $fail_count -gt 0 ]; then
        echo -e "${RED}失败: ${fail_count} 个主题${NC}"
    fi
}

# 交互式选择主题
select_topics() {
    echo -e "${YELLOW}请选择要清空的主题:${NC}"
    echo "[a] 清空所有主题"
    echo "[n] 输入主题名称"
    echo "[q] 退出"
    
    read -p "请选择 [a/n/q]: " choice
    
    case $choice in
        a|A)
            echo -e "${RED}警告: 这将清空所有 ${#TOPICS[@]} 个主题的所有数据!${NC}"
            read -p "确定要继续吗? [y/N]: " confirm
            if [[ $confirm == [yY] ]]; then
                clean_all_topics
            else
                echo -e "${YELLOW}操作已取消。${NC}"
            fi
            ;;
        n|N)
            read -p "请输入主题名称: " topic_name
            if [[ " ${TOPICS[*]} " == *" $topic_name "* ]]; then
                echo -e "${RED}警告: 这将清空主题 '$topic_name' 的所有数据!${NC}"
                read -p "确定要继续吗? [y/N]: " confirm
                if [[ $confirm == [yY] ]]; then
                    clean_topic "$topic_name"
                else
                    echo -e "${YELLOW}操作已取消。${NC}"
                fi
            else
                echo -e "${RED}错误: 主题 '$topic_name' 不存在。${NC}"
            fi
            ;;
        q|Q)
            echo -e "${YELLOW}程序已退出。${NC}"
            exit 0
            ;;
        *)
            echo -e "${RED}错误: 无效的选择。${NC}"
            select_topics
            ;;
    esac
}

# 主函数
main() {
    echo -e "${GREEN}====================================${NC}"
    echo -e "${GREEN}        Kafka 分区清空工具          ${NC}"
    echo -e "${GREEN}====================================${NC}"
    echo -e "${YELLOW}连接到集群: ${BROKERS_STRING}${NC}"
    echo ""
    
    # 列出所有主题
    list_topics
    if [ $? -eq 0 ]; then
        # 交互式选择
        select_topics
    fi
    
    echo -e "\n${GREEN}程序执行完毕。${NC}"
}

# 执行主函数
main