from __future__ import annotations

import argparse
import os
import sys
import time
import urllib.error
import urllib.request


def _env(name: str) -> str:
    return str(os.getenv(name, "") or "").strip()


def _request(url: str, headers: dict[str, str] | None = None, timeout: int = 20) -> int:
    request = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        response.read()
        return int(response.status)


def wait_for_ready(base_url: str, timeout_seconds: int) -> None:
    ready_url = f"{base_url}/readyz"
    deadline = time.time() + max(1, timeout_seconds)
    last_error = "unknown"

    while time.time() < deadline:
        try:
            status = _request(ready_url, timeout=15)
            if status == 200:
                print(f"[smoke] ready: {ready_url}")
                return
            last_error = f"unexpected status {status}"
        except Exception as exc:  # pragma: no cover - runtime/transport specific
            last_error = str(exc)
        time.sleep(10)

    raise SystemExit(f"Timed out waiting for readiness at {ready_url}: {last_error}")


def run_smoke_checks(base_url: str, analytics_token: str, admin_token: str) -> None:
    checks = [
        ("readyz", f"{base_url}/readyz", {}),
        (
            "dashboard",
            f"{base_url}/api/dashboard",
            {"Authorization": f"Bearer {analytics_token}"},
        ),
        (
            "topics",
            f"{base_url}/api/topics?page=0&size=100",
            {"Authorization": f"Bearer {analytics_token}"},
        ),
        (
            "freshness",
            f"{base_url}/api/freshness?force=true",
            {"Authorization": f"Bearer {analytics_token}"},
        ),
        (
            "operator_scheduler",
            f"{base_url}/api/scraper/scheduler",
            {"Authorization": f"Bearer {admin_token}"},
        ),
        (
            "social_overview",
            f"{base_url}/api/social/overview",
            {"Authorization": f"Bearer {admin_token}"},
        ),
        (
            "social_activities",
            f"{base_url}/api/social/activities?limit=5",
            {"Authorization": f"Bearer {admin_token}"},
        ),
        (
            "social_runtime_status",
            f"{base_url}/api/social/runtime/status",
            {"Authorization": f"Bearer {admin_token}"},
        ),
    ]

    for label, url, headers in checks:
        try:
            status = _request(url, headers=headers, timeout=20)
        except urllib.error.HTTPError as exc:
            raise SystemExit(f"Smoke check failed for {label}: HTTP {exc.code} at {url}") from exc
        except Exception as exc:  # pragma: no cover - runtime/transport specific
            raise SystemExit(f"Smoke check failed for {label}: {exc}") from exc
        print(f"[smoke] {label}: {status} {url}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run deployment smoke checks.")
    parser.add_argument("--base-url", default=_env("DEPLOY_BASE_URL") or _env("PRODUCTION_BASE_URL"))
    parser.add_argument("--analytics-token", default=_env("ANALYTICS_API_KEY_FRONTEND"))
    parser.add_argument("--admin-token", default=_env("ADMIN_API_KEY"))
    parser.add_argument("--wait-ready", action="store_true")
    parser.add_argument("--ready-timeout-seconds", type=int, default=300)
    parser.add_argument("--label", default="deployment")
    args = parser.parse_args()

    base_url = str(args.base_url or "").rstrip("/")
    if not base_url:
        raise SystemExit("Missing base URL. Set DEPLOY_BASE_URL or pass --base-url.")
    if not args.analytics_token:
        raise SystemExit("Missing analytics token. Set ANALYTICS_API_KEY_FRONTEND or pass --analytics-token.")
    if not args.admin_token:
        raise SystemExit("Missing admin token. Set ADMIN_API_KEY or pass --admin-token.")

    print(f"[smoke] starting {args.label} checks against {base_url}")
    if args.wait_ready:
        wait_for_ready(base_url, timeout_seconds=args.ready_timeout_seconds)
    run_smoke_checks(base_url, analytics_token=args.analytics_token, admin_token=args.admin_token)
    print(f"[smoke] all {args.label} checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
