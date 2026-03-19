from __future__ import annotations

import argparse
import sys
from pathlib import Path

from loguru import logger

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import config
from buffer.supabase_writer import SupabaseWriter
from ingester.neo4j_writer import Neo4jWriter
from processor.intent_extractor import extract_intents, extract_post_intents


def _sync_recent_posts(
    supabase_writer: SupabaseWriter,
    writer: Neo4jWriter,
    *,
    since_iso: str,
    sync_limit: int,
) -> dict:
    synced = 0
    errors = 0

    while True:
        posts = supabase_writer.get_unsynced_posts_since(since_iso, limit=sync_limit)
        if not posts:
            break

        for post in posts:
            try:
                bundle = supabase_writer.get_post_bundle(post)
                writer.sync_bundle(bundle)
                supabase_writer.mark_post_neo4j_synced(post["id"])
                analysis_records = bundle.get("analysis_records") or list(bundle.get("analyses", {}).values())
                for analysis in analysis_records:
                    analysis_id = analysis.get("id")
                    if analysis_id:
                        supabase_writer.mark_analysis_synced(str(analysis_id))
                synced += 1
            except Exception as exc:
                errors += 1
                logger.error(f"Neo4j sync failed for post {post.get('id')}: {exc}")

    return {
        "posts_synced": synced,
        "sync_errors": errors,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Reset topic analytics to the recent retention window and rebuild Neo4j from clean recent analyses.",
    )
    parser.add_argument("--days", type=int, default=config.GRAPH_ANALYTICS_RETENTION_DAYS)
    parser.add_argument("--comment-limit", type=int, default=config.AI_CATCHUP_COMMENT_LIMIT)
    parser.add_argument("--post-limit", type=int, default=config.AI_CATCHUP_POST_LIMIT)
    parser.add_argument("--sync-limit", type=int, default=config.AI_CATCHUP_SYNC_LIMIT)
    parser.add_argument("--max-passes", type=int, default=12)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only report what would be reset/rebuilt; do not mutate Supabase or Neo4j.",
    )
    args = parser.parse_args()

    days = max(1, int(args.days))
    writer = SupabaseWriter()
    recent_before = writer.get_recent_pipeline_snapshot(days)

    logger.info(
        "Recent analytics window snapshot | "
        f"days={days} recent_posts={recent_before.get('recent_posts')} "
        f"recent_comments={recent_before.get('recent_comments')} "
        f"recent_unsynced_posts={recent_before.get('recent_unsynced_posts')}"
    )

    if args.dry_run:
        return 0

    reset_result = writer.reset_recent_graph_window(days)
    since_iso = str(reset_result.get("window_start_at") or writer.get_recent_pipeline_snapshot(days).get("window_start_at"))

    neo = Neo4jWriter()
    try:
        neo.clear_graph()

        totals = {
            "comment_groups_saved": 0,
            "post_analyses_saved": 0,
            "posts_synced": 0,
            "sync_errors": 0,
        }

        for pass_index in range(max(1, int(args.max_passes))):
            comments = writer.get_unprocessed_comments_since(since_iso, limit=max(1, int(args.comment_limit)))
            posts = writer.get_unprocessed_posts_since(since_iso, limit=max(1, int(args.post_limit)))

            if comments:
                comment_stats = extract_intents(comments, writer, include_stats=True)
                totals["comment_groups_saved"] += int((comment_stats or {}).get("saved", 0))
            if posts:
                post_stats = extract_post_intents(posts, writer, include_stats=True)
                totals["post_analyses_saved"] += int((post_stats or {}).get("saved", 0))

            sync_stats = _sync_recent_posts(
                writer,
                neo,
                since_iso=since_iso,
                sync_limit=max(1, int(args.sync_limit)),
            )
            totals["posts_synced"] += int(sync_stats.get("posts_synced", 0))
            totals["sync_errors"] += int(sync_stats.get("sync_errors", 0))

            remaining_comments = writer.get_unprocessed_comments_since(since_iso, limit=1)
            remaining_posts = writer.get_unprocessed_posts_since(since_iso, limit=1)
            remaining_unsynced = writer.get_unsynced_posts_since(since_iso, limit=1)
            if not remaining_comments and not remaining_posts and not remaining_unsynced:
                break

        recent_after = writer.get_recent_pipeline_snapshot(days)
        logger.info(
            "Topic analytics reset complete | "
            f"days={days} comment_groups_saved={totals['comment_groups_saved']} "
            f"post_analyses_saved={totals['post_analyses_saved']} "
            f"posts_synced={totals['posts_synced']} "
            f"sync_errors={totals['sync_errors']} "
            f"recent_unsynced_posts={recent_after.get('recent_unsynced_posts')}"
        )
    finally:
        neo.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
