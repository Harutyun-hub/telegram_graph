#!/usr/bin/env python3
from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import date, timedelta
import json
import sys
import time
from typing import Any

import httpx


VOLATILE_META_KEYS = {
    "snapshotBuiltAt",
    "cacheStatus",
    "cacheSource",
    "isStale",
    "buildElapsedSeconds",
    "buildMode",
    "refreshFailureCount",
    "responseBytes",
    "responseSerializeMs",
    "persistedReadStatus",
    "persistedReadMs",
    "defaultResolutionPath",
}
VOLATILE_FRESHNESS_KEYS = {"generatedAt", "source"}


def _bearer_headers(token: str | None, *, header_name: str = "Authorization") -> dict[str, str]:
    clean = str(token or "").strip()
    if not clean:
        return {}
    return {header_name: f"Bearer {clean}"}


def _request(
    client: httpx.Client,
    method: str,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    json_body: dict[str, Any] | None = None,
) -> tuple[httpx.Response, float]:
    started_at = time.perf_counter()
    response = client.request(method, url, headers=headers, json=json_body)
    elapsed_ms = round((time.perf_counter() - started_at) * 1000, 2)
    return response, elapsed_ms


def _clear_cache(client: httpx.Client, base_url: str, admin_token: str | None) -> dict[str, Any]:
    response, elapsed_ms = _request(
        client,
        "POST",
        f"{base_url.rstrip('/')}/api/cache/clear",
        headers=_bearer_headers(admin_token, header_name="X-Admin-Authorization"),
    )
    response.raise_for_status()
    payload = response.json()
    payload["elapsedMs"] = elapsed_ms
    return payload


def _warm_dashboard(
    client: httpx.Client,
    base_url: str,
    admin_token: str | None,
    *,
    from_date: str | None = None,
    to_date: str | None = None,
    force_refresh: bool = False,
    include_profile: bool = True,
) -> dict[str, Any]:
    response, elapsed_ms = _request(
        client,
        "POST",
        f"{base_url.rstrip('/')}/api/admin/dashboard/warm",
        headers=_bearer_headers(admin_token, header_name="X-Admin-Authorization"),
        json_body={
            "from_date": from_date,
            "to_date": to_date,
            "wait": True,
            "force_refresh": force_refresh,
            "profile": include_profile,
        },
    )
    response.raise_for_status()
    payload = response.json()
    payload["elapsedMs"] = elapsed_ms
    return payload


def _fetch_dashboard(
    client: httpx.Client,
    base_url: str,
    analytics_token: str | None,
    *,
    from_date: str | None = None,
    to_date: str | None = None,
) -> dict[str, Any]:
    params: dict[str, str] = {}
    if from_date and to_date:
        params["from"] = from_date
        params["to"] = to_date
    started_at = time.perf_counter()
    response = client.get(
        f"{base_url.rstrip('/')}/api/dashboard",
        params=params,
        headers=_bearer_headers(analytics_token),
    )
    elapsed_ms = round((time.perf_counter() - started_at) * 1000, 2)
    response.raise_for_status()
    payload = response.json()
    return {
        "statusCode": response.status_code,
        "elapsedMs": elapsed_ms,
        "responseBytes": len(response.content),
        "serverTiming": response.headers.get("Server-Timing"),
        "meta": payload.get("meta") or {},
        "data": payload.get("data") or {},
    }


def _window_dates(trusted_end: str, days: int) -> tuple[str, str]:
    end = date.fromisoformat(trusted_end)
    start = end - timedelta(days=max(0, days - 1))
    return start.isoformat(), end.isoformat()


def _normalize_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = deepcopy(payload)
    meta = normalized.get("meta")
    if isinstance(meta, dict):
        for key in VOLATILE_META_KEYS:
            meta.pop(key, None)
        freshness = meta.get("freshness")
        if isinstance(freshness, dict):
            for key in VOLATILE_FRESHNESS_KEYS:
                freshness.pop(key, None)
    return normalized


def _diff_values(left: Any, right: Any, *, path: str = "root", limit: int = 20) -> list[str]:
    if limit <= 0:
        return []
    if type(left) is not type(right):
        return [f"{path}: type {type(left).__name__} != {type(right).__name__}"]
    if isinstance(left, dict):
        diffs: list[str] = []
        all_keys = sorted(set(left.keys()) | set(right.keys()))
        for key in all_keys:
            if key not in left:
                diffs.append(f"{path}.{key}: missing on left")
            elif key not in right:
                diffs.append(f"{path}.{key}: missing on right")
            else:
                diffs.extend(_diff_values(left[key], right[key], path=f"{path}.{key}", limit=limit - len(diffs)))
            if len(diffs) >= limit:
                break
        return diffs[:limit]
    if isinstance(left, list):
        diffs = []
        if len(left) != len(right):
            diffs.append(f"{path}: len {len(left)} != {len(right)}")
        for index, (left_item, right_item) in enumerate(zip(left, right)):
            diffs.extend(_diff_values(left_item, right_item, path=f"{path}[{index}]", limit=limit - len(diffs)))
            if len(diffs) >= limit:
                break
        return diffs[:limit]
    if left != right:
        return [f"{path}: {left!r} != {right!r}"]
    return []


def _baseline_case(
    client: httpx.Client,
    base_url: str,
    analytics_token: str | None,
    admin_token: str | None,
    *,
    label: str,
    from_date: str | None,
    to_date: str | None,
    warm_repeats: int,
) -> dict[str, Any]:
    clear_result = _clear_cache(client, base_url, admin_token)
    cold_build = _warm_dashboard(
        client,
        base_url,
        admin_token,
        from_date=from_date,
        to_date=to_date,
        include_profile=True,
    )
    warm_requests = [
        _fetch_dashboard(
            client,
            base_url,
            analytics_token,
            from_date=from_date,
            to_date=to_date,
        )
        for _ in range(max(1, warm_repeats))
    ]
    return {
        "label": label,
        "clearCache": clear_result,
        "coldBuild": cold_build,
        "warmRequests": warm_requests,
    }


def command_warm(args: argparse.Namespace) -> int:
    with httpx.Client(timeout=args.timeout) as client:
        if args.clear_cache:
            _clear_cache(client, args.base_url, args.admin_token)
        payload = _warm_dashboard(
            client,
            args.base_url,
            args.admin_token,
            from_date=args.from_date,
            to_date=args.to_date,
            force_refresh=args.force_refresh,
            include_profile=not args.no_profile,
        )
    print(json.dumps(payload, indent=2, ensure_ascii=True))
    return 0


def command_baseline(args: argparse.Namespace) -> int:
    with httpx.Client(timeout=args.timeout) as client:
        default_case = _baseline_case(
            client,
            args.base_url,
            args.analytics_token,
            args.admin_token,
            label="default",
            from_date=None,
            to_date=None,
            warm_repeats=args.repeats,
        )
        trusted_end = str((default_case.get("coldBuild") or {}).get("meta", {}).get("trustedEndDate") or "").strip()
        if not trusted_end:
            raise RuntimeError("Unable to determine trustedEndDate from default warm response.")
        week_from, week_to = _window_dates(trusted_end, 7)
        month_from, month_to = _window_dates(trusted_end, 30)
        seven_day_case = _baseline_case(
            client,
            args.base_url,
            args.analytics_token,
            args.admin_token,
            label="explicit_7d",
            from_date=week_from,
            to_date=week_to,
            warm_repeats=args.repeats,
        )
        thirty_day_case = _baseline_case(
            client,
            args.base_url,
            args.analytics_token,
            args.admin_token,
            label="explicit_30d",
            from_date=month_from,
            to_date=month_to,
            warm_repeats=args.repeats,
        )

    payload = {
        "baseUrl": args.base_url,
        "trustedEndDate": trusted_end,
        "cases": [default_case, seven_day_case, thirty_day_case],
    }
    print(json.dumps(payload, indent=2, ensure_ascii=True))
    return 0


def command_compare(args: argparse.Namespace) -> int:
    with httpx.Client(timeout=args.timeout) as client:
        left_payload = _fetch_dashboard(
            client,
            args.left_base_url,
            args.left_analytics_token,
            from_date=args.from_date,
            to_date=args.to_date,
        )
        right_payload = _fetch_dashboard(
            client,
            args.right_base_url,
            args.right_analytics_token,
            from_date=args.from_date,
            to_date=args.to_date,
        )

    normalized_left = _normalize_payload({"meta": left_payload["meta"], "data": left_payload["data"]})
    normalized_right = _normalize_payload({"meta": right_payload["meta"], "data": right_payload["data"]})
    diffs = _diff_values(normalized_left, normalized_right, limit=args.diff_limit)
    result = {
        "leftBaseUrl": args.left_base_url,
        "rightBaseUrl": args.right_base_url,
        "fromDate": args.from_date,
        "toDate": args.to_date,
        "matches": len(diffs) == 0,
        "diffs": diffs,
    }
    print(json.dumps(result, indent=2, ensure_ascii=True))
    return 0 if not diffs else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Original dashboard mirror-lab warmup, baseline, and parity tooling.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    warm = subparsers.add_parser("warm", help="Warm a dashboard range through the mirror-lab admin endpoint.")
    warm.add_argument("--base-url", required=True)
    warm.add_argument("--admin-token", default="")
    warm.add_argument("--from-date")
    warm.add_argument("--to-date")
    warm.add_argument("--force-refresh", action="store_true")
    warm.add_argument("--clear-cache", action="store_true")
    warm.add_argument("--no-profile", action="store_true")
    warm.add_argument("--timeout", type=float, default=120.0)
    warm.set_defaults(func=command_warm)

    baseline = subparsers.add_parser("baseline", help="Record cold-build and warm-request timings for default, 7d, and 30d ranges.")
    baseline.add_argument("--base-url", required=True)
    baseline.add_argument("--analytics-token", default="")
    baseline.add_argument("--admin-token", default="")
    baseline.add_argument("--repeats", type=int, default=3)
    baseline.add_argument("--timeout", type=float, default=120.0)
    baseline.set_defaults(func=command_baseline)

    compare = subparsers.add_parser("compare", help="Compare normalized dashboard payloads between two environments.")
    compare.add_argument("--left-base-url", required=True)
    compare.add_argument("--right-base-url", required=True)
    compare.add_argument("--left-analytics-token", default="")
    compare.add_argument("--right-analytics-token", default="")
    compare.add_argument("--from-date")
    compare.add_argument("--to-date")
    compare.add_argument("--diff-limit", type=int, default=20)
    compare.add_argument("--timeout", type=float, default=120.0)
    compare.set_defaults(func=command_compare)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        return int(args.func(args))
    except httpx.HTTPStatusError as exc:
        print(f"HTTP error: {exc.response.status_code} {exc.response.text}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
