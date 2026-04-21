"""
main.py — Pipeline orchestration.

Three async jobs run on a schedule:
  1. scrape_job()     — fetches new posts + comments from Telegram
  2. process_job()    — sends unprocessed content to the configured OpenAI extraction model
  3. neo4j_sync_job() — pushes AI results into Neo4j graph

Run with:  python main.py
"""
import asyncio
from loguru import logger
from apscheduler.schedulers.asyncio import AsyncIOScheduler

import config
config.validate()

# Railway's staging web service starts with `uvicorn main:app`.
# Re-export the FastAPI app here so the legacy pipeline entrypoint and
# the web-serving entrypoint can coexist on this branch.
from api.server import app

from scraper.session_manager  import get_client
from scraper.channel_scraper  import prepare_source_for_scrape, scrape_channel
from scraper.comment_scraper  import scrape_comments_for_post
from buffer.supabase_writer   import SupabaseWriter
from processor.intent_extractor import extract_intents, extract_post_intents
from ingester.neo4j_writer import Neo4jWriter, _collect_topics

# Shared instances
db      = None
neo4j   = None
_client = None   # Telethon client — initialized once in start()


def _ensure_pipeline_state():
    global db, neo4j

    if db is None:
        db = SupabaseWriter()

    if neo4j is None:
        try:
            neo4j = Neo4jWriter()
        except Exception as e:
            logger.warning(f"Neo4j unavailable at startup: {e} — sync job will be skipped until it's reachable")
            neo4j = None


# ── Job 1: Telegram Scraper ───────────────────────────────────────────────────

async def scrape_job():
    """Scrape new posts and comments from all active channels."""
    global _client
    _ensure_pipeline_state()
    logger.info("═══ SCRAPE JOB STARTED ═══")

    channels = db.get_active_channels()
    if not channels:
        logger.warning("No active channels found in telegram_channels table")
        return

    for channel in channels:
        username = channel["channel_username"]
        try:
            prepared_channel, entity = await prepare_source_for_scrape(_client, channel, db)
            if not prepared_channel or entity is None:
                await asyncio.sleep(5)
                continue

            # 1. Scrape new posts
            scrape_result = await scrape_channel(_client, prepared_channel, db, entity=entity)
            logger.info(
                f"[{username}] posts={int(scrape_result.get('posts_found', 0) or 0)} "
                f"comments={int(scrape_result.get('comments_found', 0) or 0)}"
            )

            # 2. Scrape comments for posts that have them
            if (
                str(prepared_channel.get("source_type") or "").strip().lower() == "channel"
                and prepared_channel.get("scrape_comments", True)
            ):
                posts_with_comments = db.get_posts_with_comments_pending_for_channel(
                    prepared_channel["id"],
                    limit=20,
                )
                for post in posts_with_comments:
                    comment_count = await scrape_comments_for_post(_client, entity, post, db)
                    logger.info(f"  └─ Post {post['telegram_message_id']}: {comment_count} comments")
                    await asyncio.sleep(1)   # polite pause between posts

        except Exception as e:
            logger.error(f"[{username}] Scrape job failed: {e}")

        # Pause between channels to avoid rate limits
        await asyncio.sleep(5)

    logger.success("═══ SCRAPE JOB COMPLETE ═══")


# ── Job 2: AI Processor ───────────────────────────────────────────────────────

async def process_job():
    """Send unprocessed comments and posts to the configured extraction model."""
    _ensure_pipeline_state()
    logger.info("═══ AI PROCESS JOB STARTED ═══")

    # Process comments (main behavioral analysis)
    comments = db.get_unprocessed_comments(limit=200)
    if comments:
        logger.info(f"Processing {len(comments)} unprocessed comments...")
        count = extract_intents(comments, db)
        logger.success(f"Created {count} AI analysis records from comments")
    else:
        logger.info("No unprocessed comments found")

    # Process standalone posts
    posts = db.get_unprocessed_posts(limit=50)
    if posts:
        logger.info(f"Processing {len(posts)} unprocessed posts...")
        processed_posts = extract_post_intents(posts, db)
        logger.success(f"Processed {processed_posts}/{len(posts)} posts")
    else:
        logger.info("No unprocessed posts found")

    logger.success("═══ AI PROCESS JOB COMPLETE ═══")


# ── Job 3: Neo4j Sync ─────────────────────────────────────────────────────────

async def neo4j_sync_job():
    """
    Enterprise Translator: assembles post bundles and pushes full graph to Neo4j.

    Per post, the translator builds:
      Channel → Post → Comment → User
      User → Intent, Topic, Sentiment, GeopoliticalStance,
             LifeStage, BusinessOpportunity, CollectiveMemory
      Topic → CO_OCCURS_WITH → Topic
      Channel → DISCUSSES → Topic
    """
    logger.info("═══ NEO4J SYNC JOB STARTED ═══")

    global neo4j
    _ensure_pipeline_state()

    # Try to (re)connect if neo4j is not available
    if neo4j is None:
        try:
            neo4j = Neo4jWriter()
            logger.info("Neo4j connected ✅")
        except Exception as e:
            logger.warning(f"Neo4j unavailable — skipping sync: {e}")
            logger.warning("Resume your AuraDB instance at console.neo4j.io if paused.")
            return

    # Fetch posts not yet reflected in Neo4j
    posts = db.get_unsynced_posts()
    if not posts:
        logger.info("No posts pending Neo4j sync")
        logger.success("═══ NEO4J SYNC JOB COMPLETE ═══")
        return

    logger.info(f"Syncing {len(posts)} posts to Neo4j graph...")
    synced = 0

    for post in posts:
        try:
            # Assemble full bundle: post + channel + comments + AI analyses
            bundle = db.get_post_bundle(post)

            comment_count = len(bundle["comments"])
            analysis_count = len(bundle["analyses"])

            # Push to Neo4j — one transaction per post
            neo4j.sync_bundle(bundle)
            db.mark_post_neo4j_synced(post["id"])

            # Mark each ai_analysis row as synced too
            analysis_records = bundle.get("analysis_records") or list(bundle["analyses"].values())
            for analysis in analysis_records:
                if analysis.get("id"):
                    db.mark_analysis_synced(analysis["id"])

            synced += 1

            logger.debug(
                f"  Post {post['telegram_message_id']} → "
                f"{comment_count} comments | "
                f"{analysis_count} user analyses | "
                f"topics={len(_collect_topics(bundle['analyses']))}"
            )

        except Exception as e:
            logger.error(f"Neo4j bundle sync failed for post {post.get('id')}: {e}")
            # Mark neo4j as unavailable if it's a connection error
            if "ServiceUnavailable" in str(e) or "connection" in str(e).lower():
                neo4j = None
                logger.warning("Neo4j connection lost — will retry next cycle")
                break

    logger.success(f"═══ NEO4J SYNC JOB COMPLETE — {synced}/{len(posts)} posts ═══")

    # Reconcile historic post-level analyses that were left unsynced.
    try:
        reconciled = db.reconcile_post_analysis_sync(limit=300)
        if reconciled:
            logger.info(f"Post-analysis reconciliation marked {reconciled} rows as synced")
    except Exception as e:
        logger.warning(f"Post-analysis reconciliation skipped: {e}")



# ── Main Entry Point ──────────────────────────────────────────────────────────

async def start():
    global _client

    _ensure_pipeline_state()
    logger.info("🚀 Starting Telegram Intelligence Pipeline...")

    # Initialize Telethon client (will prompt for SMS code on first run)
    _client = await get_client()

    # Run all jobs once immediately on startup
    await scrape_job()
    await process_job()
    await neo4j_sync_job()

    # Schedule recurring jobs
    scheduler = AsyncIOScheduler()
    scheduler.add_job(scrape_job,     "interval", minutes=config.SCRAPER_INTERVAL_MINUTES,   id="scraper")
    scheduler.add_job(process_job,    "interval", minutes=config.PROCESSOR_INTERVAL_MINUTES,  id="processor")
    scheduler.add_job(neo4j_sync_job, "interval", minutes=config.NEO4J_SYNC_INTERVAL_MINUTES, id="neo4j_sync")
    scheduler.start()

    logger.success(
        f"✅ Pipeline running!\n"
        f"   • Scraper:    every {config.SCRAPER_INTERVAL_MINUTES} minutes\n"
        f"   • AI Process: every {config.PROCESSOR_INTERVAL_MINUTES} minutes\n"
        f"   • Neo4j Sync: every {config.NEO4J_SYNC_INTERVAL_MINUTES} minutes\n"
        f"   Press Ctrl+C to stop."
    )

    # Keep the event loop alive
    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        logger.info("Shutting down pipeline...")
        scheduler.shutdown()
        await _client.disconnect()
        if neo4j:
            neo4j.close()


if __name__ == "__main__":
    asyncio.run(start())
