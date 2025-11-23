#!/bin/bash

# Kafka 集群管理脚本
# 作者: 基于当前集群配置
# 日期: $(date +%Y-%m-%d)

KAFKA_HOME="/opt/kafka"
CONFIG_DIR="$KAFKA_HOME/config"
LOG_DIR="$KAFKA_HOME/logs"
USER="kafka"

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 日志函数
log() {
    echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1"
}

warn() {
    echo -e "${YELLOW}[$(date '+%Y-%m-%d %H:%M:%S')] WARN${NC} $1"
}

error() {
    echo -e "${RED}[$(date '+%Y-%m-%d %H:%M:%S')] ERROR${NC} $1"
}

info() {
    echo -e "${BLUE}[$(date '+%Y-%m-%d %H:%M:%S')] INFO${NC} $1"
}

# 检查用户权限
check_user() {
    if [ "$(whoami)" != "$USER" ]; then
        warn "建议使用 $USER 用户执行此脚本"
        read -p "是否继续? (y/n): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            exit 1
        fi
    fi
}

# 获取进程PID
get_pid() {
    local config_file=$1
    ps -ef | grep "$config_file" | grep -v grep | awk '{print $2}'
}

# 检查进程状态
check_process() {
    local config_file=$1
    local pid=$(get_pid "$config_file")
    if [ -n "$pid" ]; then
        echo "$pid"
        return 0
    else
        return 1
    fi
}

# 启动单个服务
start_service() {
    local service_type=$1
    local config_file=$2
    local service_name=$3
    
    log "启动 $service_name..."
    
    if check_process "$config_file" > /dev/null; then
        warn "$service_name 已经在运行"
        return 0
    fi
    
    case $service_type in
        "zookeeper")
            nohup $KAFKA_HOME/bin/zookeeper-server-start.sh -daemon $CONFIG_DIR/$config_file > $LOG_DIR/${config_file%.*}.log 2>&1 &
            ;;
        "kafka")
            nohup $KAFKA_HOME/bin/kafka-server-start.sh -daemon $CONFIG_DIR/$config_file > $LOG_DIR/${config_file%.*}.log 2>&1 &
            ;;
    esac
    
    # 等待启动
    sleep 5
    
    if check_process "$config_file" > /dev/null; then
        log "✅ $service_name 启动成功"
        return 0
    else
        error "❌ $service_name 启动失败"
        return 1
    fi
}

# 停止单个服务
stop_service() {
    local config_file=$1
    local service_name=$2
    
    log "停止 $service_name..."
    
    local pid=$(check_process "$config_file")
    if [ -z "$pid" ]; then
        warn "$service_name 未在运行"
        return 0
    fi
    
    # 优雅停止
    kill -TERM $pid
    sleep 10
    
    # 检查是否停止
    if check_process "$config_file" > /dev/null; then
        warn "$service_name 未正常停止，强制停止..."
        kill -9 $pid
        sleep 5
    fi
    
    if check_process "$config_file" > /dev/null; then
        error "❌ $service_name 停止失败"
        return 1
    else
        log "✅ $service_name 停止成功"
        return 0
    fi
}

# 启动 ZooKeeper 集群
start_zookeeper() {
    log "启动 ZooKeeper 集群..."
    
    local zk_services=("zookeeper1.properties" "zookeeper2.properties" "zookeeper3.properties")
    local zk_names=("ZooKeeper-1" "ZooKeeper-2" "ZooKeeper-3")
    
    for i in "${!zk_services[@]}"; do
        start_service "zookeeper" "${zk_services[$i]}" "${zk_names[$i]}"
        sleep 2
    done
}

# 停止 ZooKeeper 集群
stop_zookeeper() {
    log "停止 ZooKeeper 集群..."
    
    local zk_services=("zookeeper3.properties" "zookeeper2.properties" "zookeeper1.properties")
    local zk_names=("ZooKeeper-3" "ZooKeeper-2" "ZooKeeper-1")
    
    for i in "${!zk_services[@]}"; do
        stop_service "${zk_services[$i]}" "${zk_names[$i]}"
        sleep 2
    done
}

# 启动 Kafka 集群
start_kafka() {
    log "启动 Kafka Broker 集群..."
    
    local kafka_services=("server1.properties" "server2.properties" "server3.properties")
    local kafka_names=("Kafka-Broker-1" "Kafka-Broker-2" "Kafka-Broker-3")
    
    for i in "${!kafka_services[@]}"; do
        start_service "kafka" "${kafka_services[$i]}" "${zk_names[$i]}"
        sleep 5
    done
}

# 停止 Kafka 集群
stop_kafka() {
    log "停止 Kafka Broker 集群..."
    
    local kafka_services=("server3.properties" "server2.properties" "server1.properties")
    local kafka_names=("Kafka-Broker-3" "Kafka-Broker-2" "Kafka-Broker-1")
    
    for i in "${!kafka_services[@]}"; do
        stop_service "${kafka_services[$i]}" "${kafka_names[$i]}"
        sleep 5
    done
}

# 检查集群状态
check_status() {
    log "检查集群状态..."
    
    echo
    info "=== ZooKeeper 状态 ==="
    local zk_services=("zookeeper1.properties" "zookeeper2.properties" "zookeeper3.properties")
    local zk_ports=("2181" "2182" "2183")
    local zk_names=("ZooKeeper-1" "ZooKeeper-2" "ZooKeeper-3")
    
    for i in "${!zk_services[@]}"; do
        local pid=$(check_process "${zk_services[$i]}")
        if [ -n "$pid" ]; then
            echo -e "  ${GREEN}✅${NC} ${zk_names[$i]} (PID: $pid) - 运行中"
        else
            echo -e "  ${RED}❌${NC} ${zk_names[$i]} - 未运行"
        fi
    done
    
    echo
    info "=== Kafka Broker 状态 ==="
    local kafka_services=("server1.properties" "server2.properties" "server3.properties")
    local kafka_ports=("9092" "9093" "9094")
    local kafka_names=("Kafka-Broker-1" "Kafka-Broker-2" "Kafka-Broker-3")
    
    for i in "${!kafka_services[@]}"; do
        local pid=$(check_process "${kafka_services[$i]}")
        if [ -n "$pid" ]; then
            # 测试 Broker 是否可访问
            if $KAFKA_HOME/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:${kafka_ports[$i]} --command-config /dev/null > /dev/null 2>&1; then
                echo -e "  ${GREEN}✅${NC} ${kafka_names[$i]} (PID: $pid, Port: ${kafka_ports[$i]}) - 运行中且可访问"
            else
                echo -e "  ${YELLOW}⚠️${NC} ${kafka_names[$i]} (PID: $pid) - 运行中但不可访问"
            fi
        else
            echo -e "  ${RED}❌${NC} ${kafka_names[$i]} - 未运行"
        fi
    done
    
    echo
    info "=== 集群信息 ==="
    local brokers="localhost:9092,localhost:9093,localhost:9094"
    if $KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server $brokers --list > /dev/null 2>&1; then
        echo -e "  ${GREEN}✅${NC} 集群可访问"
        echo -e "  📊 主题数量: $($KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server $brokers --list | wc -l)"
        echo -e "  👥 消费者组数量: $($KAFKA_HOME/bin/kafka-consumer-groups.sh --bootstrap-server $brokers --list | wc -l)"
    else
        echo -e "  ${RED}❌${NC} 集群不可访问"
    fi
}

# 重启集群
restart_cluster() {
    log "重启集群..."
    stop_cluster
    sleep 10
    start_cluster
}

# 启动整个集群
start_cluster() {
    log "开始启动 Kafka 集群..."
    check_user
    
    start_zookeeper
    sleep 10
    start_kafka
    sleep 15
    
    log "集群启动完成"
    check_status
}

# 停止整个集群
stop_cluster() {
    log "开始停止 Kafka 集群..."
    check_user
    
    stop_kafka
    sleep 10
    stop_zookeeper
    
    log "集群停止完成"
}

# 查看日志
show_logs() {
    local service=$1
    case $service in
        "zk1") sudo tail -f $LOG_DIR/zookeeper1.log ;;
        "zk2") sudo tail -f $LOG_DIR/zookeeper2.log ;;
        "zk3") sudo tail -f $LOG_DIR/zookeeper3.log ;;
        "kafka1") sudo tail -f $LOG_DIR/server1.log ;;
        "kafka2") sudo tail -f $LOG_DIR/server2.log ;;
        "kafka3") sudo tail -f $LOG_DIR/server3.log ;;
        *) error "未知的服务: $service. 可用选项: zk1, zk2, zk3, kafka1, kafka2, kafka3" ;;
    esac
}

# 显示使用说明
usage() {
    echo "Kafka 集群管理脚本"
    echo
    echo "用法: $0 {start|stop|restart|status|logs|help} [service]"
    echo
    echo "命令:"
    echo "  start    启动整个集群"
    echo "  stop     停止整个集群"
    echo "  restart  重启整个集群"
    echo "  status   检查集群状态"
    echo "  logs     查看日志 (需要指定服务: zk1, zk2, zk3, kafka1, kafka2, kafka3)"
    echo "  help     显示此帮助信息"
    echo
    echo "示例:"
    echo "  $0 start           # 启动整个集群"
    echo "  $0 stop            # 停止整个集群"
    echo "  $0 status          # 检查集群状态"
    echo "  $0 logs kafka1     # 查看 Kafka Broker 1 的日志"
    echo "  $0 logs zk2        # 查看 ZooKeeper 2 的日志"
}

# 主函数
main() {
    case $1 in
        "start")
            start_cluster
            ;;
        "stop")
            stop_cluster
            ;;
        "restart")
            restart_cluster
            ;;
        "status")
            check_status
            ;;
        "logs")
            show_logs $2
            ;;
        "help"|"-h"|"--help")
            usage
            ;;
        *)
            error "未知命令: $1"
            echo
            usage
            exit 1
            ;;
    esac
}

# 脚本入口
if [ $# -eq 0 ]; then
    usage
    exit 1
fi

main "$@"
