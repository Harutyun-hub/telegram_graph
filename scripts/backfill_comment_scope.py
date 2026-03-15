from __future__ import annotations

import argparse
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from loguru import logger

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from buffer.supabase_writer import SupabaseWriter
from processor.intent_extractor import extract_intents


def _utc_iso(hours_ago: int) -> str:
    dt = datetime.now(timezone.utc) - timedelta(hours=max(1, int(hours_ago)))
    return dt.isoformat()


def _scoped_row_exists(writer: SupabaseWriter, *, channel_id: str, telegram_user_id: int, post_id: str) -> bool:
    res = writer.client.table("ai_analysis") \
        .select("id", count="exact") \
        .eq("content_type", "batch") \
        .eq("channel_id", channel_id) \
        .eq("telegram_user_id", telegram_user_id) \
        .eq("content_id", post_id) \
        .limit(1) \
        .execute()
    return bool(getattr(res, "count", 0) or 0)


def _load_candidate_comments(writer: SupabaseWriter, *, scan_comments: int, lookback_hours: int) -> list[dict]:
    threshold = _utc_iso(lookback_hours)
    res = writer.client.table("telegram_comments") \
        .select("*") \
        .not_.is_("post_id", "null") \
        .not_.is_("telegram_user_id", "null") \
        .gte("posted_at", threshold) \
        .order("posted_at", desc=True) \
        .limit(max(1, int(scan_comments))) \
        .execute()
    return res.data or []


def _pick_groups_needing_scope(writer: SupabaseWriter, comments: list[dict], *, group_limit: int) -> list[dict]:
    grouped: dict[tuple[int, str, str], list[dict]] = defaultdict(list)
    for comment in comments:
        user_id = comment.get("telegram_user_id")
        channel_id = comment.get("channel_id")
        post_id = comment.get("post_id")
        if user_id is None or not channel_id or not post_id:
            continue
        key = (int(user_id), str(channel_id), str(post_id))
        grouped[key].append(comment)

    selected_comments: list[dict] = []
    selected_groups = 0
    for (user_id, channel_id, post_id), rows in grouped.items():
        if _scoped_row_exists(
            writer,
            channel_id=channel_id,
            telegram_user_id=user_id,
            post_id=post_id,
        ):
            continue

        selected_comments.extend(rows)
        selected_groups += 1
        if selected_groups >= max(1, int(group_limit)):
            break

    return selected_comments


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill per-user-per-post scoped comment analyses")
    parser.add_argument("--scan-comments", type=int, default=500, help="How many recent comments to scan")
    parser.add_argument("--group-limit", type=int, default=20, help="Maximum missing groups to process")
    parser.add_argument("--lookback-hours", type=int, default=24 * 21, help="Only scan comments newer than this")
    parser.add_argument("--deadline-seconds", type=int, default=900, help="Deadline for AI processing")
    parser.add_argument("--dry-run", action="store_true", help="Show target groups without calling AI")
    args = parser.parse_args()

    writer = SupabaseWriter()
    comments = _load_candidate_comments(
        writer,
        scan_comments=args.scan_comments,
        lookback_hours=args.lookback_hours,
    )
    logger.info(f"Loaded {len(comments)} comments for scope-backfill scan")

    to_process = _pick_groups_needing_scope(
        writer,
        comments,
        group_limit=args.group_limit,
    )

    unique_groups = {
        (c.get("telegram_user_id"), c.get("channel_id"), c.get("post_id"))
        for c in to_process
    }
    logger.info(
        f"Scope-backfill candidates: {len(unique_groups)} groups / {len(to_process)} comments"
    )

    if args.dry_run:
        for user_id, channel_id, post_id in sorted(unique_groups):
            logger.info(f"DRY-RUN group user={user_id} channel={channel_id} post={post_id}")
        return 0

    if not to_process:
        logger.info("Nothing to backfill — scoped rows already present for scanned window")
        return 0

    deadline = time.monotonic() + max(60, int(args.deadline_seconds))
    saved = extract_intents(to_process, writer, deadline_epoch=deadline)
    logger.success(
        f"Scope-backfill done: saved={saved}, candidate_groups={len(unique_groups)}, candidate_comments={len(to_process)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
