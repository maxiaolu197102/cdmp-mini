#!/bin/bash

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
WHITE='\033[1;37m'
NC='\033[0m' # No Color

# 样式定义
BOLD='\033[1m'
UNDERLINE='\033[4m'
NC_STYLE='\033[0m'

# 默认参数
KEY_PATTERN=""
SHOW_VALUE=false
CLEAR_DATA=false
SHOW_ALL=false

# 解析命令行参数
while [[ $# -gt 0 ]]; do
    case $1 in
        -k|--key)
            KEY_PATTERN="$2"
            shift 2
            ;;
        -v|--value)
            SHOW_VALUE=true
            shift
            ;;
        -c|--clear)
            CLEAR_DATA=true
            shift
            ;;
        -a|--all)
            SHOW_ALL=true
            shift
            ;;
        -h|--help)
            echo -e "${CYAN}${BOLD}用法: $0 [选项]${NC}"
            echo -e "${CYAN}选项:${NC}"
            echo -e "  ${GREEN}-k, --key PATTERN${NC}    指定要查找的key模式"
            echo -e "  ${GREEN}-v, --value${NC}          显示匹配key的值"
            echo -e "  ${GREEN}-c, --clear${NC}          在执行查询前清空所有Redis数据"
            echo -e "  ${GREEN}-a, --all${NC}            显示所有key和value"
            echo -e "  ${GREEN}-h, --help${NC}           显示帮助信息"
            exit 0
            ;;
        *)
            echo -e "${RED}未知参数: $1${NC}"
            exit 1
            ;;
    esac
done

# 函数：打印带颜色的标题
print_header() {
    echo -e "\n${PURPLE}${BOLD}=== $1 ===${NC}"
}

# 函数：打印带颜色的子标题
print_subheader() {
    echo -e "${CYAN}${BOLD}--- $1 ---${NC}"
}

# 函数：打印成功信息
print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

# 函数：打印警告信息
print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

# 函数：打印错误信息
print_error() {
    echo -e "${RED}✗ $1${NC}"
}

# 函数：打印信息
print_info() {
    echo -e "${BLUE}ℹ $1${NC}"
}

# 函数：安全获取Redis值
safe_get_redis_value() {
    local port=$1
    local key=$2
    
    # 首先检查key是否存在
    local exists=$(redis-cli -h 192.168.10.14 -p $port EXISTS "$key" 2>/dev/null)
    if [ "$exists" -eq 0 ]; then
        echo "(Key不存在)"
        return 0
    fi
    
    # 尝试获取key的类型
    local type=$(redis-cli -h 192.168.10.14 -p $port TYPE "$key" 2>/dev/null)
    if [ $? -ne 0 ]; then
        echo "(无法获取类型)"
        return 1
    fi
    
    case $type in
        "string")
            # 对于字符串类型，直接获取值
            local value=$(redis-cli -h 192.168.10.14 -p $port GET "$key" 2>/dev/null)
            if [ $? -eq 0 ]; then
                echo "$value"
            else
                echo "(获取字符串值失败)"
            fi
            ;;
        "hash")
            local count=$(redis-cli -h 192.168.10.14 -p $port HLEN "$key" 2>/dev/null)
            echo -e "${YELLOW}(Hash结构 - $count 个字段)${NC}"
            redis-cli -h 192.168.10.14 -p $port HGETALL "$key" 2>/dev/null | head -20 | sed 's/^/    /'
            ;;
        "list")
            local count=$(redis-cli -h 192.168.10.14 -p $port LLEN "$key" 2>/dev/null)
            echo -e "${YELLOW}(List结构 - $count 个元素)${NC}"
            redis-cli -h 192.168.10.14 -p $port LRANGE "$key" 0 4 2>/dev/null | sed 's/^/    /'
            ;;
        "set")
            local count=$(redis-cli -h 192.168.10.14 -p $port SCARD "$key" 2>/dev/null)
            echo -e "${YELLOW}(Set结构 - $count 个成员)${NC}"
            redis-cli -h 192.168.10.14 -p $port SMEMBERS "$key" 2>/dev/null | head -10 | sed 's/^/    /'
            ;;
        "zset")
            local count=$(redis-cli -h 192.168.10.14 -p $port ZCARD "$key" 2>/dev/null)
            echo -e "${YELLOW}(Sorted Set结构 - $count 个成员)${NC}"
            redis-cli -h 192.168.10.14 -p $port ZRANGE "$key" 0 4 WITHSCORES 2>/dev/null | sed 's/^/    /'
            ;;
        "none")
            echo "(Key不存在)"
            ;;
        *)
            # 对于未知类型或错误类型，尝试多种方式获取
            echo -e "${RED}(类型: $type - 可能损坏)${NC}"
            
            # 尝试作为字符串读取
            local str_value=$(redis-cli -h 192.168.10.14 -p $port GET "$key" 2>&1)
            if [[ ! "$str_value" =~ "WRONGTYPE" ]]; then
                echo "$str_value"
                return 0
            fi
            
            # 尝试作为Hash读取
            echo "  尝试作为Hash读取:"
            redis-cli -h 192.168.10.14 -p $port HGETALL "$key" 2>&1 | grep -v "WRONGTYPE" | head -5 | sed 's/^/      /'
            
            # 尝试作为Set读取  
            echo "  尝试作为Set读取:"
            redis-cli -h 192.168.10.14 -p $port SMEMBERS "$key" 2>&1 | grep -v "WRONGTYPE" | head -5 | sed 's/^/      /'
            
            # 尝试作为List读取
            echo "  尝试作为List读取:"
            redis-cli -h 192.168.10.14 -p $port LRANGE "$key" 0 4 2>&1 | grep -v "WRONGTYPE" | sed 's/^/      /'
            ;;
    esac
}

# 函数：修复损坏的key
fix_corrupted_key() {
    local port=$1
    local key=$2
    
    print_warning "尝试修复损坏的key: $key"
    
    # 1. 首先检查key是否存在
    local exists=$(redis-cli -h 192.168.10.14 -p $port EXISTS "$key" 2>/dev/null)
    if [ "$exists" -eq 0 ]; then
        print_info "Key不存在，无需修复"
        return 0
    fi
    
    # 2. 获取key的类型（可能会失败）
    local type=$(redis-cli -h 192.168.10.14 -p $port TYPE "$key" 2>/dev/null)
    
    # 3. 如果无法正常操作，删除并重新创建
    print_info "Key类型: $type"
    read -p "$(echo -e '  ${YELLOW}是否删除并重新创建此key? (y/N): ${NC}')" confirm
    if [[ $confirm =~ ^[Yy]$ ]]; then
        # 备份key的名称（如果需要）
        local backup_key="${key}_backup_$(date +%s)"
        
        # 重命名key作为备份
        redis-cli -h 192.168.10.14 -p $port RENAME "$key" "$backup_key" 2>/dev/null
        
        if [ $? -eq 0 ]; then
            print_success "已备份到: $backup_key"
            print_success "原key已删除，可以在需要时重新创建"
        else
            print_warning "备份失败，尝试直接删除..."
            redis-cli -h 192.168.10.14 -p $port DEL "$key" 2>/dev/null
            if [ $? -eq 0 ]; then
                print_success "Key已删除"
            else
                print_error "删除失败，可能需要手动处理"
            fi
        fi
    else
        print_info "跳过修复"
    fi
}

# 清空Redis数据（如果指定了-c参数）
if [ "$CLEAR_DATA" = true ]; then
    print_header "清空 Redis 数据"
    print_warning "此操作将永久删除所有Redis数据！"
    read -p "$(echo -e '${YELLOW}确认要清空所有Redis数据吗？(y/N): ${NC}')" confirm
    if [[ $confirm =~ ^[Yy]$ ]]; then
        for port in 6379 6380 6381; do
            echo -n "清空端口 $port 的数据..."
            redis-cli -h 192.168.10.14 -p $port FLUSHALL >/dev/null 2>&1
            if [ $? -eq 0 ]; then
                print_success " 成功"
            else
                print_error " 失败"
            fi
        done
        echo ""
    else
        print_info "已取消清空操作"
        echo ""
    fi
fi

# 显示所有key和value（如果指定了-a参数）
if [ "$SHOW_ALL" = true ]; then
    print_header "所有 Key 和 Value"
    for port in 6379 6380 6381; do
        print_subheader "端口 $port 的所有数据"
        keys=$(redis-cli -h 192.168.10.14 -p $port --scan --pattern "*" 2>/dev/null)
        if [ -z "$keys" ]; then
            print_info "没有找到任何key"
        else
            while read key; do
                if [ -n "$key" ]; then
                    echo -e "${WHITE}  $key${NC} ="
                    value=$(safe_get_redis_value $port "$key")
                    # 对于多行输出，已经在前面的函数中处理了缩进
                    if [ $(echo "$value" | wc -l) -le 1 ]; then
                        echo "    $value"
                    else
                        echo "$value"
                    fi
                    echo ""
                fi
            done <<< "$keys"
        fi
        echo ""
    done
else
    # 只有在不使用 -a 参数时才显示以下统计信息
    
    # 统计 genericapiserver:test_ 相关的key
    print_header "genericapiserver:test_ Key 统计"
    TOTAL_USER_KEYS=0
    for port in 6379 6380 6381; do
        USER_KEY_COUNT=$(redis-cli -h 192.168.10.14 -p $port --scan --pattern "*genericapiserver:test_*" 2>/dev/null | wc -l)
        if [ $USER_KEY_COUNT -gt 0 ]; then
            echo -e "端口 ${BLUE}$port${NC}: ${GREEN}$USER_KEY_COUNT${NC} 个 genericapiserver:test_ key"
        else
            echo -e "端口 ${BLUE}$port${NC}: ${YELLOW}$USER_KEY_COUNT${NC} 个 genericapiserver:test_ key"
        fi
        TOTAL_USER_KEYS=$((TOTAL_USER_KEYS + USER_KEY_COUNT))
    done

    if [ $TOTAL_USER_KEYS -gt 0 ]; then
        echo -e "总计: ${GREEN}${BOLD}$TOTAL_USER_KEYS${NC} 个 genericapiserver:test_ key"
    else
        echo -e "总计: ${YELLOW}$TOTAL_USER_KEYS${NC} 个 genericapiserver:test_ key"
    fi
    echo ""

    # 特别检查 user_sessions 相关的key
    print_header "user_sessions Key 状态检查"
    for port in 6379 6380 6381; do
        print_subheader "端口 $port"
        sessions_keys=$(redis-cli -h 192.168.10.14 -p $port --scan --pattern "*user_sessions*" 2>/dev/null)
        if [ -z "$sessions_keys" ]; then
            print_info "没有找到 user_sessions 相关的key"
        else
            while read key; do
                if [ -n "$key" ]; then
                    echo -e "${WHITE}  $key${NC}"
                    type=$(redis-cli -h 192.168.10.14 -p $port TYPE "$key" 2>/dev/null)
                    if [ $? -eq 0 ]; then
                        echo -e "    类型: ${CYAN}$type${NC}"
                        # 如果是损坏的key，提供修复选项
                        if [ "$type" = "none" ] || [[ "$type" =~ "WRONGTYPE" ]] || [[ "$type" =~ "error" ]]; then
                            print_warning "此key可能已损坏"
                            read -p "$(echo -e '    ${YELLOW}是否尝试修复? (y/N): ${NC}')" fix_confirm
                            if [[ $fix_confirm =~ ^[Yy]$ ]]; then
                                fix_corrupted_key $port "$key"
                            fi
                        fi
                    else
                        print_error "无法获取类型"
                    fi
                    echo ""
                fi
            done <<< "$sessions_keys"
        fi
    done

    # 统计 value 为 RATE_LIMIT_PREVENTION 的key数量
    print_header "RATE_LIMIT_PREVENTION Value 统计"
    TOTAL_RATE_LIMIT_KEYS=0
    for port in 6379 6380 6381; do
        RATE_LIMIT_COUNT=0
        # 扫描所有key并检查value
        keys=$(redis-cli -h 192.168.10.14 -p $port --scan --pattern "*" 2>/dev/null)
        if [ -n "$keys" ]; then
            while read key; do
                [ -z "$key" ] && continue
                # 只检查字符串类型的key
                type=$(redis-cli -h 192.168.10.14 -p $port TYPE "$key" 2>/dev/null)
                if [ "$type" = "string" ]; then
                    value=$(redis-cli -h 192.168.10.14 -p $port GET "$key" 2>/dev/null)
                    if [ "$value" = "RATE_LIMIT_PREVENTION" ]; then
                        RATE_LIMIT_COUNT=$((RATE_LIMIT_COUNT + 1))
                    fi
                fi
            done <<< "$keys"
        fi
        
        if [ $RATE_LIMIT_COUNT -gt 0 ]; then
            echo -e "端口 ${BLUE}$port${NC}: ${GREEN}$RATE_LIMIT_COUNT${NC} 个 value=RATE_LIMIT_PREVENTION 的key"
        else
            echo -e "端口 ${BLUE}$port${NC}: ${YELLOW}$RATE_LIMIT_COUNT${NC} 个 value=RATE_LIMIT_PREVENTION 的key"
        fi
        TOTAL_RATE_LIMIT_KEYS=$((TOTAL_RATE_LIMIT_KEYS + RATE_LIMIT_COUNT))
    done

    if [ $TOTAL_RATE_LIMIT_KEYS -gt 0 ]; then
        echo -e "总计: ${GREEN}${BOLD}$TOTAL_RATE_LIMIT_KEYS${NC} 个 value=RATE_LIMIT_PREVENTION 的key"
    else
        echo -e "总计: ${YELLOW}$TOTAL_RATE_LIMIT_KEYS${NC} 个 value=RATE_LIMIT_PREVENTION 的key"
    fi
    echo ""
fi

# 如果指定了key模式，搜索并显示
if [ -n "$KEY_PATTERN" ]; then
    print_header "搜索 Key 模式: $KEY_PATTERN"
    for port in 6379 6380 6381; do
        print_subheader "端口 $port 的匹配结果"
        keys=$(redis-cli -h 192.168.10.14 -p $port --scan --pattern "*$KEY_PATTERN*" 2>/dev/null)
        if [ -z "$keys" ]; then
            print_info "没有找到匹配的key"
        else
            while read key; do
                if [ -n "$key" ]; then
                    if [ "$SHOW_VALUE" = true ] || [ "$SHOW_ALL" = true ]; then
                        echo -e "${WHITE}  $key${NC} ="
                        value=$(safe_get_redis_value $port "$key")
                        if [ $(echo "$value" | wc -l) -le 1 ]; then
                            echo "    $value"
                        else
                            echo "$value"
                        fi
                    else
                        echo -e "${WHITE}  $key${NC}"
                    fi
                    echo ""
                fi
            done <<< "$keys"
        fi
    done
fi


