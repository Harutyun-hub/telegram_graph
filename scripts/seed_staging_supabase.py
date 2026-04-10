from __future__ import annotations

import argparse
import os
from datetime import datetime, timedelta, timezone
from typing import Any

from supabase import Client, create_client


def _env(name: str) -> str:
    return str(os.getenv(name, "") or "").strip()


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _cutoff_iso(days: int) -> str:
    return (_utc_now() - timedelta(days=max(1, int(days)))).isoformat()


def _make_client(url: str, service_role_key: str) -> Client:
    if not url or not service_role_key:
        raise ValueError("Both Supabase URL and service-role key are required.")
    return create_client(url, service_role_key)


def _fetch_windowed_rows(
    client: Client,
    *,
    table: str,
    time_column: str,
    cutoff_iso: str,
    batch_size: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    start = 0
    size = max(1, int(batch_size))
    while True:
        res = client.table(table) \
            .select("*") \
            .gte(time_column, cutoff_iso) \
            .order(time_column, desc=False) \
            .range(start, start + size - 1) \
            .execute()
        batch = list(res.data or [])
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < size:
            break
        start += len(batch)
    return rows


def _chunked(values: list[Any], size: int) -> list[list[Any]]:
    step = max(1, int(size))
    return [values[index:index + step] for index in range(0, len(values), step)]


def _fetch_rows_by_field(
    client: Client,
    *,
    table: str,
    field: str,
    values: list[Any],
    chunk_size: int,
) -> list[dict[str, Any]]:
    normalized = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        normalized.append(value)

    rows: list[dict[str, Any]] = []
    for chunk in _chunked(normalized, chunk_size):
        res = client.table(table) \
            .select("*") \
            .in_(field, chunk) \
            .execute()
        rows.extend(list(res.data or []))
    return rows


def _upsert_rows(
    client: Client,
    *,
    table: str,
    rows: list[dict[str, Any]],
    on_conflict: str,
    batch_size: int,
) -> int:
    if not rows:
        return 0
    total = 0
    for chunk in _chunked(rows, batch_size):
        client.table(table).upsert(chunk, on_conflict=on_conflict).execute()
        total += len(chunk)
    return total


def _merge_rows_by_id(*row_sets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for row_set in row_sets:
        for row in row_set:
            row_id = str(row.get("id") or "").strip()
            if row_id:
                merged[row_id] = row
    return list(merged.values())


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Copy a recent Telegram analytics sample from production Supabase to staging Supabase."
    )
    parser.add_argument("--days", type=int, default=7, help="How many trailing days of data to copy.")
    parser.add_argument("--read-batch-size", type=int, default=500, help="Batch size for source reads.")
    parser.add_argument("--write-batch-size", type=int, default=200, help="Batch size for staging upserts.")
    parser.add_argument("--in-chunk-size", type=int, default=200, help="Chunk size for IN() lookups.")
    parser.add_argument("--dry-run", action="store_true", help="Report counts without writing to staging.")
    parser.add_argument("--prod-url", default=_env("PROD_SUPABASE_URL"))
    parser.add_argument("--prod-service-role-key", default=_env("PROD_SUPABASE_SERVICE_ROLE_KEY"))
    parser.add_argument("--staging-url", default=_env("STAGING_SUPABASE_URL"))
    parser.add_argument("--staging-service-role-key", default=_env("STAGING_SUPABASE_SERVICE_ROLE_KEY"))
    return parser


def main() -> int:
    args = build_parser().parse_args()
    cutoff_iso = _cutoff_iso(args.days)

    prod = _make_client(args.prod_url, args.prod_service_role_key)
    staging = _make_client(args.staging_url, args.staging_service_role_key)

    posts = _fetch_windowed_rows(
        prod,
        table="telegram_posts",
        time_column="posted_at",
        cutoff_iso=cutoff_iso,
        batch_size=args.read_batch_size,
    )
    comments = _fetch_windowed_rows(
        prod,
        table="telegram_comments",
        time_column="posted_at",
        cutoff_iso=cutoff_iso,
        batch_size=args.read_batch_size,
    )
    analyses = _fetch_windowed_rows(
        prod,
        table="ai_analysis",
        time_column="created_at",
        cutoff_iso=cutoff_iso,
        batch_size=args.read_batch_size,
    )

    channel_ids = {
        str(value)
        for value in [
            *[row.get("channel_id") for row in posts],
            *[row.get("channel_id") for row in comments],
            *[row.get("channel_id") for row in analyses],
        ]
        if value
    }
    channels = _fetch_rows_by_field(
        prod,
        table="telegram_channels",
        field="id",
        values=sorted(channel_ids),
        chunk_size=args.in_chunk_size,
    )

    user_uuid_ids = {
        str(value)
        for value in [row.get("user_id") for row in comments]
        if value
    }
    telegram_user_ids = {
        int(value)
        for value in [
            *[row.get("telegram_user_id") for row in comments],
            *[row.get("telegram_user_id") for row in analyses],
        ]
        if value is not None
    }
    users_by_uuid = _fetch_rows_by_field(
        prod,
        table="telegram_users",
        field="id",
        values=sorted(user_uuid_ids),
        chunk_size=args.in_chunk_size,
    )
    users_by_telegram_id = _fetch_rows_by_field(
        prod,
        table="telegram_users",
        field="telegram_user_id",
        values=sorted(telegram_user_ids),
        chunk_size=args.in_chunk_size,
    )
    users = _merge_rows_by_id(users_by_uuid, users_by_telegram_id)

    summary = {
        "cutoff_iso": cutoff_iso,
        "telegram_channels": len(channels),
        "telegram_users": len(users),
        "telegram_posts": len(posts),
        "telegram_comments": len(comments),
        "ai_analysis": len(analyses),
    }

    print("Seed sample summary:")
    for table, count in summary.items():
        print(f"  {table}: {count}")

    if args.dry_run:
        print("Dry run only; no staging writes performed.")
        return 0

    written = {
        "telegram_channels": _upsert_rows(
            staging,
            table="telegram_channels",
            rows=channels,
            on_conflict="id",
            batch_size=args.write_batch_size,
        ),
        "telegram_users": _upsert_rows(
            staging,
            table="telegram_users",
            rows=users,
            on_conflict="id",
            batch_size=args.write_batch_size,
        ),
        "telegram_posts": _upsert_rows(
            staging,
            table="telegram_posts",
            rows=posts,
            on_conflict="id",
            batch_size=args.write_batch_size,
        ),
        "telegram_comments": _upsert_rows(
            staging,
            table="telegram_comments",
            rows=comments,
            on_conflict="id",
            batch_size=args.write_batch_size,
        ),
        "ai_analysis": _upsert_rows(
            staging,
            table="ai_analysis",
            rows=analyses,
            on_conflict="id",
            batch_size=args.write_batch_size,
        ),
    }

    print("Staging upsert counts:")
    for table, count in written.items():
        print(f"  {table}: {count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
