"""
scrape_orchestrator.py — Shared scraper cycle used by scheduler surfaces.
"""
from __future__ import annotations

import asyncio
from functools import partial
import threading
import time

from loguru import logger
from telethon import TelegramClient

import config
from api.runtime_executors import run_background
from scraper.channel_scraper import prepare_source_for_scrape, scrape_channel
from scraper.comment_scraper import scrape_comments_for_post
from processor.intent_extractor import extract_intents, extract_post_intents
from ingester.neo4j_writer import Neo4jWriter

_writer_lock = threading.Lock()
_shared_background_writer: Neo4jWriter | None = None


def _get_background_writer() -> Neo4jWriter:
    global _shared_background_writer
    with _writer_lock:
        if _shared_background_writer is None:
            _shared_background_writer = Neo4jWriter()
        return _shared_background_writer


async def run_scrape_cycle(client: TelegramClient, supabase_writer) -> dict:
    """Run one complete scrape cycle for all active channel sources."""
    channels = supabase_writer.get_active_channels()
    if not channels:
        logger.warning("No active channels found in telegram_channels table")
        return {
            "channels_total": 0,
            "channels_processed": 0,
            "posts_found": 0,
            "comments_found": 0,
        }

    channels_processed = 0
    total_posts = 0
    total_comments = 0

    for channel in channels:
        username = channel["channel_username"]
        try:
            prepared_channel, entity = await prepare_source_for_scrape(client, channel, supabase_writer)
            if not prepared_channel or entity is None:
                await asyncio.sleep(5)
                continue

            scrape_result = await scrape_channel(
                client,
                prepared_channel,
                supabase_writer,
                entity=entity,
            )
            total_posts += int(scrape_result.get("posts_found", 0) or 0)
            total_comments += int(scrape_result.get("comments_found", 0) or 0)

            if (
                str(prepared_channel.get("source_type") or "").strip().lower() == "channel"
                and prepared_channel.get("scrape_comments", True)
            ):
                posts_with_comments = supabase_writer.get_posts_with_comments_pending_for_channel(
                    prepared_channel["id"],
                    limit=20,
                )
                if posts_with_comments:
                    for post in posts_with_comments:
                        comment_count = await scrape_comments_for_post(client, entity, post, supabase_writer)
                        total_comments += int(comment_count)
                        await asyncio.sleep(1)

            channels_processed += 1
        except Exception as e:
            logger.error(f"[{username}] Scrape cycle failed: {e}")

        await asyncio.sleep(5)

    return {
        "channels_total": len(channels),
        "channels_processed": channels_processed,
        "posts_found": total_posts,
        "comments_found": total_comments,
    }


def _run_ai_process_and_sync_blocking(
    supabase_writer,
    *,
    comment_limit: int,
    post_limit: int,
    sync_limit: int,
) -> dict:
    """Blocking AI + Neo4j sync stage (intended for background thread)."""
    stage_started_at = time.monotonic()
    result: dict = {
        "ai_analysis_saved": 0,
        "posts_processed": 0,
        "posts_pending_sync": 0,
        "posts_synced": 0,
        "sync_errors": 0,
        "recovery_unlocked_posts": 0,
        "recovery_unlocked_comment_groups": 0,
        "recovery_promoted_permanent": 0,
    }
    started_at = time.monotonic()
    process_budget = max(60, int(config.AI_PROCESS_STAGE_MAX_SECONDS))
    sync_budget = max(60, int(config.AI_SYNC_STAGE_MAX_SECONDS))
    process_deadline = started_at + process_budget

    # AI processing stage
    try:
        process_started = time.monotonic()
        if hasattr(supabase_writer, "auto_recover_transient_failures"):
            recovery = supabase_writer.auto_recover_transient_failures() or {}
            result["recovery_unlocked_posts"] = int(recovery.get("post_retried", 0) or 0)
            result["recovery_unlocked_comment_groups"] = int(recovery.get("comment_group_retried", 0) or 0)
            result["recovery_promoted_permanent"] = int(recovery.get("promoted_permanent", 0) or 0)
        comment_metrics: dict[str, int | float] = {
            "saved": 0,
            "failed_groups": 0,
            "blocked_groups": 0,
            "deferred_groups": 0,
            "attempted_groups": 0,
        }
        comments = supabase_writer.get_unprocessed_comments(limit=comment_limit)
        if comments:
            comment_metrics_res = extract_intents(
                comments,
                supabase_writer,
                deadline_epoch=process_deadline,
                include_stats=True,
            )
            if isinstance(comment_metrics_res, dict):
                comment_metrics = dict(comment_metrics_res)
            else:
                comment_metrics["saved"] = int(comment_metrics_res or 0)
            result["ai_analysis_saved"] = int(comment_metrics.get("saved", 0) or 0)
        result["comment_metrics"] = comment_metrics

        posts = supabase_writer.get_unprocessed_posts(limit=post_limit)
        post_metrics_res = extract_post_intents(
            posts,
            supabase_writer,
            deadline_epoch=process_deadline,
            include_stats=True,
        )
        post_metrics: dict[str, int | float] = {
            "saved": 0,
            "failed_posts": 0,
            "blocked_posts": 0,
            "deferred_posts": 0,
            "attempted_posts": 0,
        }
        if isinstance(post_metrics_res, dict):
            post_metrics = dict(post_metrics_res)
        else:
            post_metrics["saved"] = int(post_metrics_res or 0)
        result["post_metrics"] = post_metrics
        result["posts_processed"] = int(post_metrics.get("saved", 0) or 0)

        result["ai_attempted_items"] = int(comment_metrics.get("attempted_groups", 0) or 0) + int(
            post_metrics.get("attempted_posts", 0) or 0
        )
        result["ai_failed_items"] = int(comment_metrics.get("failed_groups", 0) or 0) + int(
            post_metrics.get("failed_posts", 0) or 0
        )
        result["ai_blocked_items"] = int(comment_metrics.get("blocked_groups", 0) or 0) + int(
            post_metrics.get("blocked_posts", 0) or 0
        )
        result["ai_deferred_items"] = int(comment_metrics.get("deferred_groups", 0) or 0) + int(
            post_metrics.get("deferred_posts", 0) or 0
        )
        result["process_duration_seconds"] = round(max(0.0, time.monotonic() - process_started), 2)
    except Exception as e:
        logger.error(f"AI process stage failed: {e}")
        result["process_error"] = str(e)

    # Neo4j sync stage
    sync_deadline = time.monotonic() + sync_budget
    posts_to_sync = supabase_writer.get_unsynced_posts(limit=sync_limit)
    result["posts_pending_sync"] = len(posts_to_sync)
    if posts_to_sync:
        try:
            writer = _get_background_writer()
            batch_size = max(1, int(getattr(config, "NEO4J_SYNC_BATCH_CHUNK_SIZE", 20)))
            for start in range(0, len(posts_to_sync), batch_size):
                if time.monotonic() >= sync_deadline:
                    result["sync_timeout"] = True
                    logger.warning("Neo4j sync stage budget reached; deferring remaining posts to next cycle")
                    break
                post_chunk = posts_to_sync[start:start + batch_size]
                try:
                    bundles = supabase_writer.get_post_bundles_batch(post_chunk)
                    if not bundles:
                        continue
                    writer.sync_post_batch(bundles)

                    synced_post_ids = [
                        str(post_payload["id"])
                        for bundle in bundles
                        for post_payload in [bundle.get("post") or {}]
                        if post_payload.get("id")
                    ]
                    analysis_ids = sorted({
                        str(analysis.get("id"))
                        for bundle in bundles
                        for analysis in (bundle.get("analysis_records") or list((bundle.get("analyses") or {}).values()))
                        if analysis.get("id")
                    })
                    supabase_writer.mark_posts_neo4j_synced(synced_post_ids)
                    if analysis_ids:
                        supabase_writer.mark_analyses_synced(analysis_ids)
                    result["posts_synced"] += len(synced_post_ids)
                except Exception as e:
                    error_text = str(e).lower()
                    if "serviceunavailable" in error_text or "connection" in error_text:
                        result["sync_errors"] += 1
                        result["sync_error"] = str(e)
                        logger.error(f"Neo4j batch sync failed for posts {[post.get('id') for post in post_chunk]}: {e}")
                        break

                    logger.warning(
                        "Neo4j batch sync failed for posts {}. Falling back to single-post sync: {}",
                        [post.get("id") for post in post_chunk],
                        e,
                    )
                    for post in post_chunk:
                        if time.monotonic() >= sync_deadline:
                            result["sync_timeout"] = True
                            logger.warning("Neo4j sync stage budget reached during fallback; deferring remaining posts")
                            break
                        try:
                            bundle = supabase_writer.get_post_bundle(post)
                            writer.sync_bundle(bundle)
                            supabase_writer.mark_post_neo4j_synced(post["id"])
                            analysis_records = bundle.get("analysis_records") or list(bundle["analyses"].values())
                            for analysis in analysis_records:
                                analysis_id = analysis.get("id")
                                if analysis_id:
                                    supabase_writer.mark_analysis_synced(analysis_id)
                            result["posts_synced"] += 1
                        except Exception as inner:
                            result["sync_errors"] += 1
                            logger.error(f"Neo4j sync failed for post {post.get('id')}: {inner}")
                            if "serviceunavailable" in str(inner).lower() or "connection" in str(inner).lower():
                                result["sync_error"] = str(inner)
                                break
                    if result.get("sync_error"):
                        break
        except Exception as e:
            logger.error(f"Neo4j writer init failed: {e}")
            result["sync_error"] = str(e)

    # One-pass reconciliation for historic post-analysis sync mismatches.
    try:
        reconciled = supabase_writer.reconcile_post_analysis_sync(limit=max(50, int(sync_limit) * 2))
        result["post_analysis_reconciled"] = reconciled
    except Exception as e:
        logger.warning(f"Post-analysis reconciliation failed: {e}")
        result["post_analysis_reconciled"] = 0

    result["sync_duration_seconds"] = round(max(0.0, time.monotonic() - stage_started_at), 2)
    return result


async def run_ai_process_and_sync(
    supabase_writer,
    *,
    comment_limit: int = config.AI_NORMAL_COMMENT_LIMIT,
    post_limit: int = config.AI_NORMAL_POST_LIMIT,
    sync_limit: int = config.AI_NORMAL_SYNC_LIMIT,
) -> dict:
    """Run AI processing + Neo4j sync in worker thread to keep API responsive."""
    task = partial(
        _run_ai_process_and_sync_blocking,
        supabase_writer,
        comment_limit=max(1, int(comment_limit)),
        post_limit=max(1, int(post_limit)),
        sync_limit=max(1, int(sync_limit)),
    )

    return await run_background(task)


async def run_full_cycle(client: TelegramClient, supabase_writer) -> dict:
    """Run scrape + AI process + Neo4j sync as one runtime cycle."""
    backlog = supabase_writer.get_backlog_counts()
    runnable_posts = int(backlog.get("runnable_posts") or 0)
    runnable_comment_groups = int(backlog.get("runnable_comment_groups") or 0)

    should_skip_scrape = bool(
        config.SCRAPE_SKIP_WHEN_BACKLOG
        and (
            runnable_posts >= max(1, int(config.SCRAPE_BACKPRESSURE_UNPROCESSED_POSTS))
            or runnable_comment_groups >= max(1, int(config.SCRAPE_BACKPRESSURE_UNPROCESSED_COMMENTS))
        )
    )

    if should_skip_scrape:
        scrape_result = {
            "channels_total": 0,
            "channels_processed": 0,
            "posts_found": 0,
            "comments_found": 0,
            "scrape_skipped": True,
            "scrape_skipped_reason": "backpressure",
            "backlog_before": backlog,
        }
        logger.warning(
            "Skipping scrape stage due to backlog pressure "
            f"(runnable_posts={runnable_posts}, runnable_comment_groups={runnable_comment_groups})"
        )
    else:
        scrape_result = await run_scrape_cycle(client, supabase_writer)

    process_sync_result = await run_ai_process_and_sync(
        supabase_writer,
        comment_limit=config.AI_NORMAL_COMMENT_LIMIT,
        post_limit=config.AI_NORMAL_POST_LIMIT,
        sync_limit=config.AI_NORMAL_SYNC_LIMIT,
    )
    merged = {
        **scrape_result,
        **process_sync_result,
        "mode": "normal",
    }
    return merged


async def run_catchup_cycle(client: TelegramClient, supabase_writer) -> dict:
    """Run AI+sync-heavy catch-up cycle without new scraping."""
    process_sync_result = await run_ai_process_and_sync(
        supabase_writer,
        comment_limit=config.AI_CATCHUP_COMMENT_LIMIT,
        post_limit=config.AI_CATCHUP_POST_LIMIT,
        sync_limit=config.AI_CATCHUP_SYNC_LIMIT,
    )
    return {
        "channels_total": 0,
        "channels_processed": 0,
        "posts_found": 0,
        "comments_found": 0,
        **process_sync_result,
        "mode": "catchup",
    }
