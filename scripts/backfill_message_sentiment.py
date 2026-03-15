from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

from loguru import logger

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from api.db import get_driver
from buffer.supabase_writer import SupabaseWriter
import config


SOCIAL_SENTIMENT_TAGS = {
    "Anxious",
    "Frustrated",
    "Angry",
    "Confused",
    "Hopeful",
    "Trusting",
    "Distrustful",
    "Solidarity",
    "Exhausted",
    "Grief",
}

SENTIMENT_CANON = {
    "positive": "Positive",
    "negative": "Negative",
    "neutral": "Neutral",
    "mixed": "Mixed",
    "urgent": "Urgent",
    "sarcastic": "Sarcastic",
}


def _normalize_sentiment(raw: Any) -> str | None:
    if raw is None:
        return None
    key = str(raw).strip().lower()
    if not key or key in {"null", "none", "unknown", "n/a"}:
        return None
    return SENTIMENT_CANON.get(key)


def _normalize_tags(raw_llm: dict, sentiment: str | None) -> list[str]:
    tags: list[str] = []
    seen: set[str] = set()

    def _add(tag: str | None) -> None:
        if not tag:
            return
        norm = str(tag).strip().title()
        if norm not in SOCIAL_SENTIMENT_TAGS or norm in seen:
            return
        seen.add(norm)
        tags.append(norm)

    raw_tags = raw_llm.get("social_sentiment_tags")
    if isinstance(raw_tags, list):
        for item in raw_tags:
            if isinstance(item, str):
                _add(item)

    tone = str(raw_llm.get("emotional_tone") or "").lower()
    if "anx" in tone or "worr" in tone or "fear" in tone:
        _add("Anxious")
    if "frustr" in tone:
        _add("Frustrated")
    if "ang" in tone or "indignan" in tone:
        _add("Angry")
    if "confus" in tone or "uncertain" in tone:
        _add("Confused")
    if "hope" in tone or "optim" in tone:
        _add("Hopeful")
    if "trust" in tone:
        _add("Trusting")
    if "distrust" in tone or "skeptic" in tone:
        _add("Distrustful")
    if "solidar" in tone:
        _add("Solidarity")
    if "exhaust" in tone or "fatigue" in tone:
        _add("Exhausted")
    if "grief" in tone or "mour" in tone:
        _add("Grief")

    if sentiment == "Urgent":
        _add("Anxious")
    elif sentiment == "Negative":
        _add("Frustrated")
    elif sentiment == "Sarcastic":
        _add("Distrustful")
    elif sentiment == "Positive":
        _add("Hopeful")

    return tags


def _fetch_analysis_rows(writer: SupabaseWriter, *, content_type: str, limit: int) -> list[dict]:
    page_size = 500
    rows: list[dict] = []
    offset = 0
    while len(rows) < limit:
        upper = offset + page_size - 1
        try:
            query = (
                writer.client.table("ai_analysis")
                .select("id,content_id,channel_id,telegram_user_id,raw_llm_response,created_at")
                .eq("content_type", content_type)
                .not_.is_("content_id", "null")
                .order("created_at", desc=True)
                .range(offset, upper)
            )
            if content_type == "batch":
                query = query.not_.is_("telegram_user_id", "null").not_.is_("channel_id", "null")
            resp = query.execute()
        except Exception as exc:
            logger.warning(f"Failed to fetch {content_type} analysis rows at offset={offset}: {exc}")
            break
        chunk = resp.data or []
        if not chunk:
            break
        rows.extend(chunk)
        if len(chunk) < page_size:
            break
        offset += page_size
    return rows[:limit]


def _build_post_rows(analysis_rows: list[dict]) -> list[dict]:
    latest_by_post: dict[str, dict] = {}
    for row in analysis_rows:
        post_uuid = str(row.get("content_id") or "").strip()
        if not post_uuid:
            continue
        if post_uuid not in latest_by_post:
            latest_by_post[post_uuid] = row

    out: list[dict] = []
    for post_uuid, row in latest_by_post.items():
        raw_llm = row.get("raw_llm_response") if isinstance(row.get("raw_llm_response"), dict) else {}
        sentiment = _normalize_sentiment(raw_llm.get("sentiment"))
        tags = _normalize_tags(raw_llm, sentiment)
        if not sentiment and not tags:
            continue
        out.append({"post_uuid": post_uuid, "sentiment": sentiment, "tags": tags})
    return out


def _build_comment_rows(writer: SupabaseWriter, analysis_rows: list[dict], max_rows: int) -> list[dict]:
    out: list[dict] = []
    seen: set[str] = set()
    for row in analysis_rows:
        if len(out) >= max_rows:
            break
        post_id = str(row.get("content_id") or "").strip()
        channel_id = str(row.get("channel_id") or "").strip()
        user_id = row.get("telegram_user_id")
        if not post_id or not channel_id or user_id is None:
            continue
        try:
            user_id_int = int(user_id)
        except Exception:
            continue

        raw_llm = row.get("raw_llm_response") if isinstance(row.get("raw_llm_response"), dict) else {}
        sentiment = _normalize_sentiment(raw_llm.get("sentiment"))
        tags = _normalize_tags(raw_llm, sentiment)
        if not sentiment and not tags:
            continue

        try:
            c_resp = (
                writer.client.table("telegram_comments")
                .select("id")
                .eq("post_id", post_id)
                .eq("channel_id", channel_id)
                .eq("telegram_user_id", user_id_int)
                .limit(1000)
                .execute()
            )
        except Exception as exc:
            logger.warning(
                "Skipping comment-group due to Supabase error "
                f"post_id={post_id} channel_id={channel_id} user_id={user_id_int}: {exc}"
            )
            continue
        for c in (c_resp.data or []):
            comment_uuid = str(c.get("id") or "").strip()
            if not comment_uuid or comment_uuid in seen:
                continue
            seen.add(comment_uuid)
            out.append({"comment_uuid": comment_uuid, "sentiment": sentiment, "tags": tags})
            if len(out) >= max_rows:
                break
    return out


def _write_post_sentiment(rows: list[dict], dry_run: bool) -> int:
    if not rows:
        return 0
    if dry_run:
        return len(rows)
    query = """
    UNWIND $rows AS row
    MATCH (p:Post {uuid: row.post_uuid})
    FOREACH (_ IN CASE WHEN row.sentiment IS NULL THEN [] ELSE [1] END |
      MERGE (s:Sentiment {label: row.sentiment})
      MERGE (p)-[r:HAS_SENTIMENT]->(s)
      ON CREATE SET r.count = 1
      ON MATCH  SET r.count = coalesce(r.count, 0) + 1
    )
    FOREACH (tag IN coalesce(row.tags, []) |
      MERGE (st:SentimentTag {name: tag})
      MERGE (p)-[rt:HAS_SENTIMENT_TAG]->(st)
      ON CREATE SET rt.count = 1
      ON MATCH  SET rt.count = coalesce(rt.count, 0) + 1
    )
    RETURN count(p) AS n
    """
    driver = get_driver()
    with driver.session(database=config.NEO4J_DATABASE) as session:
        session.run(query, {"rows": rows}).consume()
    return len(rows)


def _write_comment_sentiment(rows: list[dict], dry_run: bool) -> int:
    if not rows:
        return 0
    if dry_run:
        return len(rows)
    query = """
    UNWIND $rows AS row
    MATCH (c:Comment {uuid: row.comment_uuid})
    FOREACH (_ IN CASE WHEN row.sentiment IS NULL THEN [] ELSE [1] END |
      MERGE (s:Sentiment {label: row.sentiment})
      MERGE (c)-[r:HAS_SENTIMENT]->(s)
      ON CREATE SET r.count = 1
      ON MATCH  SET r.count = coalesce(r.count, 0) + 1
    )
    FOREACH (tag IN coalesce(row.tags, []) |
      MERGE (st:SentimentTag {name: tag})
      MERGE (c)-[rt:HAS_SENTIMENT_TAG]->(st)
      ON CREATE SET rt.count = 1
      ON MATCH  SET rt.count = coalesce(rt.count, 0) + 1
    )
    RETURN count(c) AS n
    """
    driver = get_driver()
    with driver.session(database=config.NEO4J_DATABASE) as session:
        session.run(query, {"rows": rows}).consume()
    return len(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill message-level sentiment edges in Neo4j")
    parser.add_argument("--post-analysis-limit", type=int, default=3000)
    parser.add_argument("--batch-analysis-limit", type=int, default=3000)
    parser.add_argument("--comment-row-limit", type=int, default=20000)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    writer = SupabaseWriter()
    post_analysis = _fetch_analysis_rows(writer, content_type="post", limit=max(1, int(args.post_analysis_limit)))
    batch_analysis = _fetch_analysis_rows(writer, content_type="batch", limit=max(1, int(args.batch_analysis_limit)))

    post_rows = _build_post_rows(post_analysis)
    comment_rows = _build_comment_rows(writer, batch_analysis, max_rows=max(1, int(args.comment_row_limit)))

    touched_posts = _write_post_sentiment(post_rows, dry_run=bool(args.dry_run))
    touched_comments = _write_comment_sentiment(comment_rows, dry_run=bool(args.dry_run))

    logger.success(
        "Message sentiment backfill done | "
        f"post_rows={len(post_rows)} applied={touched_posts} | "
        f"comment_rows={len(comment_rows)} applied={touched_comments} | "
        f"dry_run={bool(args.dry_run)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
