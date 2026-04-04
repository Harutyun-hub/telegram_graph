from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from utils.source_bulk_import import (
    DEFAULT_COMMENT_BACKLOG_THRESHOLD,
    DEFAULT_POST_BACKLOG_THRESHOLD,
    DEFAULT_SCRAPE_COMMENTS,
    DEFAULT_SCRAPE_DEPTH_DAYS,
    DEFAULT_WAVE_SIZE,
    SourceImportApiClient,
    execute_bulk_import,
    utc_now,
    write_results_csv,
    write_summary_json,
)


def _env(name: str) -> str:
    return str(os.getenv(name, "") or "").strip()


def default_output_dir(csv_path: Path) -> Path:
    stamp = utc_now().strftime("%Y%m%dT%H%M%SZ")
    return PROJECT_ROOT / "tmp" / f"source-import-{stamp}-{csv_path.stem}"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Bulk import Telegram sources from CSV via the Sources API.")
    parser.add_argument("--file", required=True, help="Path to the CSV file to import.")
    parser.add_argument("--api-base", default=_env("SOURCE_IMPORT_API_BASE"), help="Base API URL, with or without /api.")
    parser.add_argument(
        "--auth-token",
        default=_env("SOURCE_IMPORT_AUTH_TOKEN"),
        help="Supabase access token for the admin user. Accepts raw or Bearer token. Optional for local bypass testing.",
    )
    parser.add_argument("--wave-size", type=int, default=DEFAULT_WAVE_SIZE)
    parser.add_argument("--scrape-depth-days", type=int, default=DEFAULT_SCRAPE_DEPTH_DAYS)
    parser.add_argument("--scrape-comments", dest="scrape_comments", action="store_true", default=DEFAULT_SCRAPE_COMMENTS)
    parser.add_argument("--no-scrape-comments", dest="scrape_comments", action="store_false")
    parser.add_argument("--dry-run", action="store_true", help="Validate and compare against existing sources without writing.")
    parser.add_argument("--output-dir", default="", help="Directory for summary.json and results.csv.")
    parser.add_argument("--timeout-seconds", type=float, default=30.0)
    parser.add_argument("--max-attempts", type=int, default=3)
    parser.add_argument("--retry-backoff-seconds", type=float, default=1.0)
    parser.add_argument("--poll-interval-seconds", type=float, default=15.0)
    parser.add_argument("--cycle-timeout-seconds", type=float, default=1800.0)
    parser.add_argument("--post-threshold", type=int, default=DEFAULT_POST_BACKLOG_THRESHOLD)
    parser.add_argument("--comment-threshold", type=int, default=DEFAULT_COMMENT_BACKLOG_THRESHOLD)
    return parser


def print_summary(summary: dict) -> None:
    run_counts = summary.get("run_counts") or {}
    print(
        "[import] summary:"
        f" total_rows={summary.get('total_rows', 0)}"
        f" valid_rows={summary.get('valid_rows', 0)}"
        f" invalid_rows={summary.get('invalid_rows', 0)}"
        f" imported_rows={summary.get('imported_rows', 0)}"
        f" created={run_counts.get('created', 0)}"
        f" reactivated={run_counts.get('reactivated', 0)}"
        f" exists={run_counts.get('exists', 0)}"
        f" api_failed={run_counts.get('api_failed', 0)}"
        f" resolved={run_counts.get('resolved', 0)}"
        f" pending_resolution={run_counts.get('pending_resolution', 0)}"
    )
    print(
        "[import] waves:"
        f" total={summary.get('total_waves', 0)}"
        f" highest_activated={summary.get('highest_activated_wave', 0)}"
        f" completed_all={summary.get('completed_all_waves', False)}"
    )

    verification = summary.get("verification") or {}
    duplicates = len(verification.get("duplicate_handles") or [])
    missing = len(verification.get("missing_handles") or [])
    unexpected_active = len(verification.get("unexpected_active_handles") or [])
    unexpected_inactive = len(verification.get("unexpected_inactive_handles") or [])
    print(
        "[import] verification:"
        f" duplicate_handles={duplicates}"
        f" missing_handles={missing}"
        f" unexpected_active={unexpected_active}"
        f" unexpected_inactive={unexpected_inactive}"
    )


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    csv_path = Path(args.file).expanduser().resolve()
    if not csv_path.exists():
        raise SystemExit(f"CSV file not found: {csv_path}")

    if not args.api_base:
        raise SystemExit("Missing API base URL. Pass --api-base or set SOURCE_IMPORT_API_BASE.")
    if int(args.wave_size) < 1:
        raise SystemExit("--wave-size must be at least 1.")

    output_dir = Path(args.output_dir).expanduser().resolve() if args.output_dir else default_output_dir(csv_path)
    output_dir.mkdir(parents=True, exist_ok=True)

    client = SourceImportApiClient(
        api_base=args.api_base,
        auth_token=args.auth_token,
        timeout_seconds=args.timeout_seconds,
    )

    print(
        f"[import] starting {'dry-run' if args.dry_run else 'write run'}"
        f" file={csv_path}"
        f" api_base={client.api_base}"
        f" wave_size={args.wave_size}"
        f" output_dir={output_dir}"
    )

    rows, summary = execute_bulk_import(
        csv_path=csv_path,
        client=client,
        wave_size=int(args.wave_size),
        scrape_depth_days=int(args.scrape_depth_days),
        scrape_comments=bool(args.scrape_comments),
        dry_run=bool(args.dry_run),
        max_attempts=int(args.max_attempts),
        retry_backoff_seconds=float(args.retry_backoff_seconds),
        poll_interval_seconds=float(args.poll_interval_seconds),
        cycle_timeout_seconds=float(args.cycle_timeout_seconds),
        post_backlog_threshold=int(args.post_threshold),
        comment_backlog_threshold=int(args.comment_threshold),
    )

    results_path = output_dir / "results.csv"
    summary_path = output_dir / "summary.json"
    write_results_csv(rows, results_path)
    write_summary_json(summary, summary_path)

    print_summary(summary)
    print(f"[import] results_csv={results_path}")
    print(f"[import] summary_json={summary_path}")

    verification = summary.get("verification") or {}
    has_blocking_issues = bool(
        (summary.get("run_counts") or {}).get("api_failed")
        or verification.get("duplicate_handles")
        or verification.get("missing_handles")
        or verification.get("unexpected_active_handles")
        or verification.get("unexpected_inactive_handles")
    )
    return 1 if has_blocking_issues else 0


if __name__ == "__main__":
    raise SystemExit(main())
