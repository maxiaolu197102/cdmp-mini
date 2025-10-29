import argparse
import json
import re
from typing import Any, Dict, List, Optional


STATUS_TEXT = {
    200: "OK",
    201: "Created",
    202: "Accepted",
    204: "No Content",
    400: "Bad Request",
    401: "Unauthorized",
    403: "Forbidden",
    404: "Not Found",
    409: "Conflict",
    422: "Unprocessable Entity",
    429: "Too Many Requests",
    500: "Internal Server Error",
    502: "Bad Gateway",
    503: "Service Unavailable",
}


def extract_log_info(line: str) -> Optional[Dict[str, Any]]:
    if "trace/trace.go" not in line:
        return None

    brace_index = line.find("{")
    if brace_index == -1:
        return None

    payload = line[brace_index:]
    try:
        data = json.loads(payload)
    except json.JSONDecodeError:
        return None

    trace_id = data.get("trace_id")
    if not trace_id:
        return None

    req_ctx = data.get("request_context", {})
    extra = req_ctx.get("extra", {})
    username = (
        extra.get("target_user")
        or extra.get("username")
        or req_ctx.get("user_id")
        or "unknown"
    )

    status = req_ctx.get("http_status")
    try:
        status = int(status)
    except (TypeError, ValueError):
        status = 0

    call_chain = data.get("call_chain", {})
    spans = call_chain.get("spans", [])

    metrics = data.get("business_metrics", {})
    perf = metrics.get("performance_summary", {})

    timestamp = data.get("timestamp")
    if not timestamp:
        ts_match = re.search(r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?", line)
        timestamp = ts_match.group(0) if ts_match else ""

    return {
        "trace_id": trace_id,
        "username": username,
        "http_method": req_ctx.get("http_method", "UNKNOWN"),
        "http_path": req_ctx.get("http_path", "UNKNOWN"),
        "http_status": status,
        "operator": req_ctx.get("operator") or "unknown",
        "level": data.get("level", "INFO"),
        "timestamp": timestamp,
        "spans": spans,
        "root_start": call_chain.get("start_time", 0),
        "api_dur": perf.get("api_processing_ms") or 0.0,
        "total_dur": metrics.get("total_duration_ms") or 0.0,
    }


def build_span_tree(spans: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    span_map = {span["span_id"]: span for span in spans if span.get("span_id")}
    trees: List[Dict[str, Any]] = []
    for span_id, span in span_map.items():
        if not span.get("parent_id"):
            trees.append(_build_subtree(span_id, span_map))
    trees.sort(key=lambda node: node["span"].get("start_time", 0))
    return trees


def _build_subtree(span_id: str, span_map: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    node = span_map[span_id]
    children: List[Dict[str, Any]] = []
    for child_id, child in span_map.items():
        if child.get("parent_id") == span_id:
            children.append(_build_subtree(child_id, span_map))
    children.sort(key=lambda node: node["span"].get("start_time", 0))
    return {"span": node, "children": children}


def format_span_tree(nodes: List[Dict[str, Any]], root_start: int, indent: str = "") -> List[str]:
    lines: List[str] = []
    for index, node in enumerate(nodes):
        span = node["span"]
        is_last = index == len(nodes) - 1
        branch = "└─ " if is_last else "├─ "

        component = span.get("component", "unknown")
        operation = span.get("operation", "unknown")
        duration = span.get("duration_ms", 0.0)
        status = "✅" if span.get("status") == "success" else "❌"
        business_code = span.get("business_code")

        line = f"{indent}{branch}{component}.{operation} ({duration:.2f}ms) {status}"
        if business_code:
            line += f" [{business_code}]"

        start_time = span.get("start_time") or 0
        if root_start and start_time and start_time > root_start:
            offset = int(start_time - root_start)
            if offset > 0:
                line += f" @+{offset}ms"

        lines.append(line)

        if node["children"]:
            child_indent = indent + ("   " if is_last else "│  ")
            lines.extend(format_span_tree(node["children"], root_start, child_indent))
    return lines


def status_text(code: int) -> str:
    return STATUS_TEXT.get(code, str(code))


def main() -> None:
    parser = argparse.ArgumentParser(description="分布式调用链日志追踪工具")
    parser.add_argument("log_file", help="日志文件路径")
    parser.add_argument("-u", "--username", help="按用户名过滤")
    parser.add_argument("-t", "--trace", help="按 trace_id 过滤")
    parser.add_argument("-n", type=int, default=0, help="输出记录数量，0 表示全部")
    parser.add_argument(
        "--top",
        type=int,
        default=0,
        help="按总耗时倒序输出前 N 条记录（隐式启用排序）",
    )
    args = parser.parse_args()

    if not args.username and not args.trace and args.top <= 0:
        parser.error("必须指定 --username 或 --trace 中的至少一个过滤条件")

    matches: List[Dict[str, Any]] = []
    with open(args.log_file, "r", errors="ignore") as handle:
        for line in handle:
            info = extract_log_info(line)
            if not info:
                continue
            if args.username and args.username not in info["username"]:
                continue
            if args.trace and info["trace_id"] != args.trace:
                continue
            matches.append(info)

    if not matches:
        target = args.username or args.trace
        print(f"未找到符合条件的记录（{target}）")
        return

    if args.top > 0:
        matches.sort(key=lambda item: float(item["total_dur"] or 0.0), reverse=True)
        limit = args.top if args.n == 0 else min(args.top, args.n)
        if limit > 0:
            matches = matches[:limit]
    else:
        matches.sort(key=lambda item: item["timestamp"], reverse=True)
        if args.n > 0:
            matches = matches[: args.n]

    for entry in matches:
        method = entry["http_method"]
        path = entry["http_path"]
        code = entry["http_status"]
        text = status_text(code)
        print(f"{method} {path} → {code} {text}")
        print(f"   时间戳: {entry['timestamp']}")
        print(f"   TraceID: {entry['trace_id']}")
        print(f"   用户名: {entry['username']}")
        print(f"   操作者: {entry['operator']}")
        print(f"   级别: {entry['level']}")

        tree = build_span_tree(entry["spans"])
        for line in format_span_tree(tree, entry["root_start"]):
            print(line)

        print(
            f"   总耗时: {float(entry['api_dur']):.2f}ms (API), 完整: {float(entry['total_dur']):.2f}ms"
        )
        print()


if __name__ == "__main__":
    main()