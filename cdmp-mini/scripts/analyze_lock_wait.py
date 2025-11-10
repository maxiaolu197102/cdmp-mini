#!/usr/bin/env python3
"""Aggregate iam-apiserver traces by minute and align with k6 throughput."""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


LOCAL_TZ = timezone(timedelta(hours=8))


def _extract_json_payload(line: str) -> Optional[Dict[str, Any]]:
    try:
        idx = line.index("{")
    except ValueError:
        return None
    try:
        return json.loads(line[idx:])
    except json.JSONDecodeError:
        return None


def _parse_iso_ts(value: str) -> Optional[datetime]:
    if not value:
        return None
    cleaned = value
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"
    if "." in cleaned:
        main, frac_and_offset = cleaned.split(".", 1)
        offset_sep = "+" if "+" in frac_and_offset else "-" if "-" in frac_and_offset[1:] else None
        if offset_sep:
            frac, offset = frac_and_offset.split(offset_sep, 1)
            frac = (frac + "000000")[:6]
            cleaned = f"{main}.{frac}{offset_sep}{offset}"
        else:
            frac = (frac_and_offset + "000000")[:6]
            cleaned = f"{main}.{frac}"
    try:
        return datetime.fromisoformat(cleaned)
    except ValueError:
        return None


@dataclass
class MinuteStats:
    requests: int = 0
    delete_force: int = 0
    get_user: int = 0
    post_user: int = 0
    total_duration_sum: float = 0.0
    total_duration_max: float = 0.0
    total_duration_ge6s: int = 0
    api_processing_sum: float = 0.0
    api_processing_count: int = 0
    pending_active: int = 0
    pending_new: int = 0
    pending_ttl_sum: float = 0.0
    pending_ttl_count: int = 0
    lock_wait_flag: int = 0
    check_user_lookup_sum: float = 0.0
    check_user_lookup_count: int = 0
    check_user_lookup_max: float = 0.0
    check_user_exist_sum: float = 0.0
    check_user_exist_count: int = 0
    check_user_exist_max: float = 0.0
    span_duration_max: float = 0.0
    insufficient_vus_events: int = 0

    def update_from_trace(self, record: Dict[str, Any]) -> None:
        self.requests += 1

        ctx = record.get("request_context", {})
        method = ctx.get("http_method")
        path = ctx.get("http_path")
        if method == "DELETE" and path == "/v1/users/:name/force":
            self.delete_force += 1
        elif method == "GET" and path == "/v1/users/:name":
            self.get_user += 1
        elif method == "POST" and path == "/v1/users":
            self.post_user += 1

        metrics = record.get("business_metrics", {})
        total_duration = metrics.get("total_duration_ms")
        if isinstance(total_duration, (int, float)):
            self.total_duration_sum += float(total_duration)
            self.total_duration_max = max(self.total_duration_max, float(total_duration))
            if total_duration >= 6000:
                self.total_duration_ge6s += 1

        perf = metrics.get("performance_summary", {})
        api_processing = perf.get("api_processing_ms")
        if isinstance(api_processing, (int, float)):
            self.api_processing_sum += float(api_processing)
            self.api_processing_count += 1

        extra = ctx.get("extra", {})
        if extra.get("pending_marker_active"):
            self.pending_active += 1
        if extra.get("pending_marker_new"):
            self.pending_new += 1
        ttl = extra.get("pending_marker_ttl_ms")
        if isinstance(ttl, (int, float)):
            self.pending_ttl_sum += float(ttl)
            self.pending_ttl_count += 1
        if extra.get("check_user_exist_db_lock_wait"):
            self.lock_wait_flag += 1

        spans = record.get("call_chain", {}).get("spans", [])
        for span in spans:
            duration = span.get("duration_ms")
            if not isinstance(duration, (int, float)):
                continue
            duration = float(duration)
            self.span_duration_max = max(self.span_duration_max, duration)
            op = span.get("operation")
            if op == "check_user_primary_lookup":
                self.check_user_lookup_sum += duration
                self.check_user_lookup_count += 1
                self.check_user_lookup_max = max(self.check_user_lookup_max, duration)
            elif op == "check_user_exist":
                self.check_user_exist_sum += duration
                self.check_user_exist_count += 1
                self.check_user_exist_max = max(self.check_user_exist_max, duration)

    def as_row(self, minute: str, expected_total: Optional[float], expected_delete: Optional[float]) -> List[Any]:
        avg_total = self.total_duration_sum / self.requests if self.requests else 0.0
        avg_api_proc = self.api_processing_sum / self.api_processing_count if self.api_processing_count else 0.0
        avg_lookup = self.check_user_lookup_sum / self.check_user_lookup_count if self.check_user_lookup_count else 0.0
        avg_exist = self.check_user_exist_sum / self.check_user_exist_count if self.check_user_exist_count else 0.0
        pending_ttl_avg = self.pending_ttl_sum / self.pending_ttl_count if self.pending_ttl_count else 0.0

        deficit_total = None
        if expected_total is not None:
            deficit_total = expected_total - self.requests

        deficit_delete = None
        if expected_delete is not None:
            deficit_delete = expected_delete - self.delete_force

        return [
            minute,
            self.requests,
            self.delete_force,
            self.get_user,
            self.post_user,
            round(avg_total, 3),
            round(self.total_duration_max, 3),
            self.total_duration_ge6s,
            round(avg_api_proc, 3),
            self.pending_active,
            self.pending_new,
            round(pending_ttl_avg, 3),
            self.lock_wait_flag,
            round(avg_lookup, 3),
            round(self.check_user_lookup_max, 3),
            self.check_user_lookup_count,
            round(avg_exist, 3),
            round(self.check_user_exist_max, 3),
            self.check_user_exist_count,
            round(self.span_duration_max, 3),
            self.insufficient_vus_events,
            round(deficit_total, 3) if deficit_total is not None else "",
            round(deficit_delete, 3) if deficit_delete is not None else "",
        ]


def load_k6_summary(path: Optional[Path]) -> Dict[str, Any]:
    if not path or not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def derive_expected_per_minute(summary: Dict[str, Any]) -> Dict[str, float]:
    rates: List[float] = []
    delete_force_rate: Optional[float] = None

    def visit(obj: Any) -> None:
        nonlocal delete_force_rate
        if isinstance(obj, dict):
            executor = obj.get("executor")
            rate = obj.get("rate")
            name = obj.get("name") or obj.get("path") or ""
            if executor == "constant-arrival-rate" and isinstance(rate, (int, float)):
                rates.append(float(rate))
                if not delete_force_rate and "single_parallel" in str(name):
                    delete_force_rate = float(rate)
            for value in obj.values():
                visit(value)
        elif isinstance(obj, list):
            for item in obj:
                visit(item)

    visit(summary)

    total_rate = sum(rates)
    expected_total = total_rate * 60 if total_rate else None
    expected_delete = delete_force_rate * 60 if delete_force_rate else None

    out: Dict[str, float] = {}
    if expected_total is not None:
        out["total_per_minute"] = expected_total
    if expected_delete is not None:
        out["delete_force_per_minute"] = expected_delete
    return out


def parse_k6_cli(path: Optional[Path]) -> Dict[str, int]:
    events: Dict[str, int] = defaultdict(int)
    if not path or not path.exists():
        return events

    time_re = re.compile(r'time="([0-9T:+-]+)"')
    with path.open() as fp:
        for line in fp:
            if "Insufficient VUs" not in line:
                continue
            match = time_re.search(line)
            if not match:
                continue
            ts = _parse_iso_ts(match.group(1))
            if not ts:
                continue
            minute = ts.astimezone(LOCAL_TZ).strftime("%Y-%m-%d %H:%M")
            events[minute] += 1
    return events


def analyse(log_path: Path, k6_cli_path: Optional[Path], summary_path: Optional[Path]) -> tuple[Dict[str, MinuteStats], Optional[float], Optional[float]]:
    stats: Dict[str, MinuteStats] = defaultdict(MinuteStats)

    with log_path.open() as fp:
        for line in fp:
            payload = _extract_json_payload(line)
            if not payload:
                continue
            ts = _parse_iso_ts(payload.get("timestamp"))
            if not ts:
                continue
            minute = ts.astimezone(LOCAL_TZ).strftime("%Y-%m-%d %H:%M")
            stats[minute].update_from_trace(payload)

    k6_events = parse_k6_cli(k6_cli_path)
    for minute, count in k6_events.items():
        stats[minute].insufficient_vus_events += count

    summary = load_k6_summary(summary_path)
    expected = derive_expected_per_minute(summary)
    expected_total = expected.get("total_per_minute")
    expected_delete = expected.get("delete_force_per_minute")

    return stats, expected_total, expected_delete


def write_csv(stats: Dict[str, MinuteStats], output_path: Path, expected_total: Optional[float], expected_delete: Optional[float]) -> None:

    fieldnames = [
        "minute",
        "requests",
        "delete_force",
        "get_user",
        "post_user",
        "avg_total_duration_ms",
        "max_total_duration_ms",
        "count_total_duration_ge6000",
        "avg_api_processing_ms",
        "pending_marker_active",
        "pending_marker_new",
        "avg_pending_marker_ttl_ms",
        "lock_wait_flag",
        "avg_check_user_lookup_ms",
        "max_check_user_lookup_ms",
        "check_user_lookup_count",
        "avg_check_user_exist_ms",
        "max_check_user_exist_ms",
        "check_user_exist_count",
        "max_span_duration_ms",
        "insufficient_vus_events",
        "requests_deficit_vs_expected",
        "delete_force_deficit_vs_expected",
    ]

    rows = []
    for minute in sorted(stats.keys()):
        rows.append(stats[minute].as_row(minute, expected_total, expected_delete))

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="") as fp:
        writer = csv.writer(fp)
        writer.writerow(fieldnames)
        writer.writerows(rows)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Analyse iam-apiserver traces against k6 output.")
    parser.add_argument("--log", type=Path, required=True, help="Path to iam-apiserver.log")
    parser.add_argument("--k6-cli", type=Path, default=None, help="Path to k6_cli_output.txt")
    parser.add_argument("--k6-summary", type=Path, default=None, help="Path to k6 summary export JSON")
    parser.add_argument("--output", type=Path, required=True, help="Destination CSV path")
    return parser


def main(argv: Optional[Iterable[str]] = None) -> None:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    stats, expected_total, expected_delete = analyse(args.log, args.k6_cli, args.k6_summary)
    write_csv(stats, args.output, expected_total, expected_delete)


if __name__ == "__main__":
    main()
