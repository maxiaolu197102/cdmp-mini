#!/usr/bin/env bash

set -euo pipefail

# Default configuration
HOSTS="127.0.0.1:6379"
PATTERN="genericapiserver:user:pending:*"
SAMPLE_LIMIT=50
INTERVAL=30
ITERATIONS=0
REDIS_CLI="redis-cli"

print_usage() {
    cat <<'EOF'
Usage: sample_pending_backlog.sh [options]

Options:
  --hosts HOSTS           Comma-separated Redis host:port entries (default: 127.0.0.1:6379)
  --pattern PATTERN       Key pattern to scan (default: genericapiserver:user:pending:*)
  --sample-limit N        Max keys used for TTL sampling per host (0 disables TTL sampling, default: 50)
  --iterations N          Number of iterations to run (0 for infinite, default: 0)
  --interval SECONDS      Delay between iterations (default: 30)
  --redis-cli PATH        Alternate redis-cli binary path (default: redis-cli)
  -h, --help              Show this help and exit

Examples:
  ./sample_pending_backlog.sh --hosts 192.168.10.14:6379,192.168.10.14:6380,192.168.10.14:6381 --interval 15
  ./sample_pending_backlog.sh --sample-limit 0 --pattern 'user:pending:delete-force*'
EOF
}

log() {
    printf '%s %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --hosts)
                HOSTS="${2:?missing value for --hosts}"
                shift 2
                ;;
            --pattern)
                PATTERN="${2:?missing value for --pattern}"
                shift 2
                ;;
            --sample-limit)
                SAMPLE_LIMIT="${2:?missing value for --sample-limit}"
                shift 2
                ;;
            --interval)
                INTERVAL="${2:?missing value for --interval}"
                shift 2
                ;;
            --iterations)
                ITERATIONS="${2:?missing value for --iterations}"
                shift 2
                ;;
            --redis-cli)
                REDIS_CLI="${2:?missing value for --redis-cli}"
                shift 2
                ;;
            -h|--help)
                print_usage
                exit 0
                ;;
            *)
                printf 'Unknown option: %s\n' "$1" >&2
                print_usage >&2
                exit 1
                ;;
        esac
    done
}

require_binary() {
    if ! command -v "$REDIS_CLI" >/dev/null 2>&1; then
        printf 'Error: redis-cli binary not found: %s\n' "$REDIS_CLI" >&2
        exit 1
    fi
}

scan_total_keys() {
    local host="$1" port="$2" pattern="$3" total
    # Use --scan to iterate over the cluster node and count matches.
    # shellcheck disable=SC2034
    total=$($REDIS_CLI -c -h "$host" -p "$port" --scan --pattern "$pattern" | wc -l)
    printf '%s' "$total"
}

collect_sample_ttls() {
    local host="$1" port="$2" pattern="$3" sample_limit="$4"
    local -a ttls=()
    local count=0 key ttl

    if [[ "$sample_limit" -le 0 ]]; then
        printf ''
        return 0
    fi

    while IFS= read -r key; do
        ttl=$($REDIS_CLI -c -h "$host" -p "$port" pttl "$key" 2>/dev/null || true)
        if [[ "$ttl" =~ ^-?[0-9]+$ ]]; then
            ttls+=("$ttl")
        fi
        count=$((count + 1))
        if [[ "$count" -ge "$sample_limit" ]]; then
            break
        fi
    done < <($REDIS_CLI -c -h "$host" -p "$port" --scan --pattern "$pattern")

    printf '%s\n' "${ttls[*]}"
}

summarize_ttls() {
    local ttls_str="$1"
    if [[ -z "$ttls_str" ]]; then
        printf 'sample_min_ttl=NA sample_max_ttl=NA sample_avg_ttl=NA sample_size=0'
        return
    fi

    local -a ttls
    # Convert to array; ttl values are space-separated.
    read -r -a ttls <<<"$ttls_str"

    local min=9223372036854775807
    local max=-9223372036854775808
    local sum=0
    local count=0
    local ttl

    for ttl in "${ttls[@]}"; do
        if (( ttl < min )); then
            min=$ttl
        fi
        if (( ttl > max )); then
            max=$ttl
        fi
        sum=$((sum + ttl))
        count=$((count + 1))
    done

    local avg="NA"
    if (( count > 0 )); then
        avg=$((sum / count))
    fi

    printf 'sample_min_ttl=%s sample_max_ttl=%s sample_avg_ttl=%s sample_size=%s' "$min" "$max" "$avg" "$count"
}

run_iteration() {
    local iteration="$1"
    local timestamp
    timestamp="$(date '+%Y-%m-%d %H:%M:%S')"

    IFS=',' read -r -a host_entries <<<"$HOSTS"

    for entry in "${host_entries[@]}"; do
        local host port total ttls summary
        host="${entry%%:*}"
        port="${entry##*:}"
        if [[ -z "$host" || -z "$port" ]]; then
            log "skip invalid host entry: $entry"
            continue
        fi

        total=$(scan_total_keys "$host" "$port" "$PATTERN")
        ttls=$(collect_sample_ttls "$host" "$port" "$PATTERN" "$SAMPLE_LIMIT")
        summary=$(summarize_ttls "$ttls")

        printf '%s iteration=%s host=%s port=%s pattern="%s" total_keys=%s %s\n' \
            "$timestamp" "$iteration" "$host" "$port" "$PATTERN" "$total" "$summary"
    done
}

main() {
    parse_args "$@"
    require_binary

    local iteration=1
    while :; do
        run_iteration "$iteration"
        if [[ "$ITERATIONS" -ne 0 && "$iteration" -ge "$ITERATIONS" ]]; then
            break
        fi
        iteration=$((iteration + 1))
        sleep "$INTERVAL"
    done
}

main "$@"
