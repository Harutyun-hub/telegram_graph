from __future__ import annotations
"""
neo4j_writer.py — Enterprise Graph Translator (Expert-Optimized)

Graph Expert Rules Applied:
  1. Canonical identity keys on every node type
  2. Topics normalized via topic_normalizer BEFORE any MERGE
  3. TopicCategory hierarchy prevents super-nodes
  4. Relationship timestamps on all dynamic relationships
  5. Composite + full-text indexes for query performance
  6. Channel anchoring — every node traceable to Channel
  7. MERGE-only on relationships (no duplicate creation)

Node Types (15):
  Channel, Post, Comment, User, Topic, TopicCategory,
  TopicDomain, Entity, Intent, Sentiment, SentimentTag,
  GeopoliticalStance, LifeStage, BusinessOpportunity, CollectiveMemory

Relationship Types (20):
  IN_CHANNEL, REPLIES_TO, WROTE, TAGGED, EXHIBITS,
  HAS_SENTIMENT, ALIGNED_WITH, REMEMBERS, IN_LIFE_STAGE,
  SIGNALS_OPPORTUNITY, INTERESTED_IN, DISCUSSES,
  CO_OCCURS_WITH, BELONGS_TO_CATEGORY, IN_DOMAIN,
  MENTIONS, MENTIONS_ENTITY, HAS_SENTIMENT_TAG
"""
from neo4j import GraphDatabase
from loguru import logger
from typing import cast
from utils.topic_normalizer import (
    normalize_model_topics,
    normalize_topics,
    get_topic_category,
    get_topic_domain,
    normalize_topic_category,
    normalize_topic_domain,
)
import config


class Neo4jWriter:

    def __init__(self):
        # neo4j+ssc:// = routing protocol + SSL + skip certificate verification
        # Resolves macOS SSL cert errors with AuraDB
        uri = config.NEO4J_URI
        if uri.startswith("neo4j+s://"):
            uri = uri.replace("neo4j+s://", "neo4j+ssc://")
        self.driver = GraphDatabase.driver(
            uri,
            auth=(config.NEO4J_USERNAME, config.NEO4J_PASSWORD),
        )
        self._setup_indexes()

    def close(self):
        self.driver.close()

    def _setup_indexes(self):
        """
        Create uniqueness constraints and performance indexes.
        Expert Rule: indexes first, data second — never the other way around.
        """
        constraints = [
            # Uniqueness constraints (also create implicit B-tree indexes)
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Channel)             REQUIRE n.uuid IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Post)                REQUIRE n.uuid IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Comment)             REQUIRE n.uuid IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:User)                REQUIRE n.telegram_user_id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Topic)               REQUIRE n.name IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:TopicCategory)       REQUIRE n.name IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:TopicDomain)         REQUIRE n.name IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Entity)              REQUIRE n.name IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Intent)              REQUIRE n.name IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Sentiment)           REQUIRE n.label IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:SentimentTag)        REQUIRE n.name IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:GeopoliticalStance)  REQUIRE n.alignment IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:LifeStage)           REQUIRE n.name IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:BusinessOpportunity) REQUIRE n.type IS UNIQUE",
            # Composite indexes for common traversal patterns
            "CREATE INDEX post_time_idx        IF NOT EXISTS FOR (p:Post)    ON (p.channel_uuid, p.posted_at)",
            "CREATE INDEX user_lang_idx        IF NOT EXISTS FOR (u:User)    ON (u.language, u.inferred_gender)",
            "CREATE INDEX comment_time_idx     IF NOT EXISTS FOR (c:Comment) ON (c.telegram_user_id, c.posted_at)",
            # New behavioral intelligence indexes
            "CREATE INDEX user_nostalgia_idx   IF NOT EXISTS FOR (u:User)    ON (u.soviet_nostalgia)",
            "CREATE INDEX user_distress_idx    IF NOT EXISTS FOR (u:User)    ON (u.financial_distress_level)",
            "CREATE INDEX user_locus_idx       IF NOT EXISTS FOR (u:User)    ON (u.locus_of_control)",
            "CREATE INDEX comment_tod_idx      IF NOT EXISTS FOR (c:Comment) ON (c.time_of_day, c.posting_hour)",
        ]
        full_text = [
            "CREATE FULLTEXT INDEX post_text_ft    IF NOT EXISTS FOR (p:Post)    ON EACH [p.text]",
            "CREATE FULLTEXT INDEX comment_text_ft IF NOT EXISTS FOR (c:Comment) ON EACH [c.text]",
        ]
        with self.driver.session(database=config.NEO4J_DATABASE) as session:
            for stmt in constraints:
                try:
                    session.run(stmt)
                except Exception as e:
                    logger.debug(f"Index/constraint skip: {e}")
            for stmt in full_text:
                try:
                    session.run(stmt)
                except Exception as e:
                    logger.debug(f"Full-text index skip: {e}")
        logger.info("Neo4j indexes and constraints ready")

    # ── Enterprise Bundle Sync ────────────────────────────────────────────────

    def sync_bundle(self, bundle: dict):
        """
        Main translator entry point. Takes an assembled post bundle and
        writes the fully normalized, connected graph in one session.

        Expert rules enforced:
          - Topics normalized before any MERGE
          - Relationship timestamps included
          - TopicCategory hierarchy created
          - All nodes traceable back to Channel
        """
        post           = bundle["post"]
        channel        = bundle["channel"]
        comments       = bundle["comments"]
        analyses       = bundle["analyses"]
        post_analysis  = bundle.get("post_analysis")
        reply_user_map = bundle.get("reply_user_map", {})

        with self.driver.session(database=config.NEO4J_DATABASE) as session:
            # 1. Channel + Post (foundation — must exist before anything else)
            session.run(_CYPHER_CHANNEL_POST, _channel_post_params(channel, post))

            # 2. Comments + Users (with full AI analysis per user)
            for comment in comments:
                uid = str(comment.get("telegram_user_id") or "anonymous")
                analysis = analyses.get(uid) or {}
                # Resolve who this comment was replying to (for User→User network)
                reply_to_msg_id  = comment.get("reply_to_message_id")
                reply_to_user_id = reply_user_map.get(int(reply_to_msg_id)) if reply_to_msg_id else None
                params = _comment_params(comment, post, analysis, reply_to_user_id)
                session.run(_CYPHER_COMMENT, params)

            # 3. Topics — normalized union across all users in this post
            topic_items = _collect_topic_items(analyses, post_analysis=post_analysis)
            post_sentiment, post_social_sentiment_tags = _extract_sentiment_payload_from_analysis(post_analysis or {})
            if topic_items or post_sentiment or post_social_sentiment_tags:
                session.run(_CYPHER_POST_TOPICS, {
                    "post_uuid":    post["id"],
                    "channel_uuid": channel["id"],
                    "posted_at":    str(post.get("posted_at")),
                    "topics":       topic_items,
                    "post_sentiment": post_sentiment,
                    "post_social_sentiment_tags": post_social_sentiment_tags,
                })

    # ── Orphan Check (Expert Rule 6) ─────────────────────────────────────────

    def check_orphans(self) -> dict:
        """
        Verify graph integrity: no orphan posts or comments.
        Run periodically to detect data issues.
        """
        with self.driver.session(database=config.NEO4J_DATABASE) as session:
            orphan_posts = session.run(
                "MATCH (p:Post) WHERE NOT (p)-[:IN_CHANNEL]->(:Channel) RETURN count(p) AS n"
            ).single()["n"]
            orphan_comments = session.run(
                "MATCH (c:Comment) WHERE NOT (c)-[:REPLIES_TO]->(:Post) RETURN count(c) AS n"
            ).single()["n"]

        result = {"orphan_posts": orphan_posts, "orphan_comments": orphan_comments}
        if orphan_posts > 0 or orphan_comments > 0:
            logger.warning(f"Graph integrity issue: {result}")
        return result

    def list_topic_promotion_candidates(
        self,
        *,
        min_discuss_count: int = 12,
        min_channel_count: int = 2,
        limit: int = 50,
    ) -> list[dict]:
        """
        Return high-signal proposed topics that are good candidates
        for taxonomy promotion during weekly review.
        """
        query = """
        MATCH (ch:Channel)-[d:DISCUSSES]->(t:Topic {proposed: true})
        OPTIONAL MATCH (t)-[:BELONGS_TO_CATEGORY]->(cat:TopicCategory)
        OPTIONAL MATCH (cat)-[:IN_DOMAIN]->(dom:TopicDomain)
        WITH t, cat, dom,
             sum(coalesce(d.count, 1)) AS discuss_count,
             count(DISTINCT ch) AS channel_count
        WHERE discuss_count >= $min_discuss_count
          AND channel_count >= $min_channel_count
        RETURN t.name AS topic,
               coalesce(cat.name, 'General') AS category,
               coalesce(dom.name, 'General') AS domain,
               discuss_count,
               channel_count
        ORDER BY discuss_count DESC, channel_count DESC, topic ASC
        LIMIT $limit
        """

        with self.driver.session(database=config.NEO4J_DATABASE) as session:
            rows = session.run(
                query,
                {
                    "min_discuss_count": max(1, int(min_discuss_count)),
                    "min_channel_count": max(1, int(min_channel_count)),
                    "limit": max(1, int(limit)),
                },
            )
            return [dict(row) for row in rows]


# ── Cypher Templates ──────────────────────────────────────────────────────────

_CYPHER_CHANNEL_POST = """
// ── Channel Node (anchor) ──
MERGE (ch:Channel {uuid: $channel_uuid})
SET   ch.username           = $channel_username,
      ch.title              = $channel_title,
      ch.member_count       = $member_count,
      ch.description        = $channel_description,
      ch.telegram_channel_id = $telegram_channel_id

// ── Post Node ──
MERGE (p:Post {uuid: $post_uuid})
SET   p.telegram_message_id = $telegram_message_id,
      p.channel_uuid         = $channel_uuid,
      p.text                 = $text,
      p.posted_at            = datetime($posted_at),
      p.views                = $views,
      p.forwards             = $forwards,
      p.reactions            = $reactions,
      p.comment_count        = $comment_count,
      p.media_type           = $media_type

// ── Post → Channel (anchor relationship) ──
MERGE (p)-[:IN_CHANNEL]->(ch)
"""

_CYPHER_COMMENT = """
// ── Comment Node (with time-of-day label) ──
MERGE (c:Comment {uuid: $comment_uuid})
SET   c.telegram_message_id = $telegram_message_id,
      c.telegram_user_id    = $telegram_user_id,
      c.text                = $text,
      c.posted_at           = datetime($posted_at),
      c.time_of_day         = $time_of_day,
      c.posting_hour        = $posting_hour

// ── Comment → Post (idempotent — no properties on MERGE) ──
MERGE (p:Post {uuid: $post_uuid})
MERGE (c)-[rcp:REPLIES_TO]->(p)
ON CREATE SET rcp.posted_at = datetime($posted_at)

// ── Comment → Primary Sentiment (message-scoped) ──
FOREACH (_ IN CASE WHEN $sentiment IS NOT NULL THEN [1] ELSE [] END |
  MERGE (smsg:Sentiment {label: $sentiment})
  MERGE (c)-[rcs:HAS_SENTIMENT]->(smsg)
  ON CREATE SET rcs.count = 1,
                rcs.first_seen = datetime($posted_at),
                rcs.last_seen = datetime($posted_at)
  ON MATCH  SET rcs.count = coalesce(rcs.count, 0) + 1,
                rcs.last_seen = datetime($posted_at)
)

// ── Comment → Social sentiment tags (message-scoped) ──
FOREACH (tag IN $social_sentiment_tags |
  MERGE (st:SentimentTag {name: tag})
  MERGE (c)-[rct:HAS_SENTIMENT_TAG]->(st)
  ON CREATE SET rct.count = 1,
                rct.first_seen = datetime($posted_at),
                rct.last_seen = datetime($posted_at)
  ON MATCH  SET rct.count = coalesce(rct.count, 0) + 1,
                rct.last_seen = datetime($posted_at)
)

// ── User node + all AI intelligence (only if real user) ──
FOREACH (_ IN CASE WHEN $telegram_user_id IS NOT NULL THEN [1] ELSE [] END |

  MERGE (u:User {telegram_user_id: $telegram_user_id})
  SET   u.inferred_gender         = $inferred_gender,
        u.inferred_age_bracket    = $inferred_age_bracket,
        u.language                = $language,
        u.community_role          = $community_role,
        u.communication_style     = $communication_style,
        u.migration_intent        = $migration_intent,
        u.diaspora_signals        = $diaspora_signals,
        u.authority_attitude      = $authority_attitude,
        u.last_seen               = datetime($posted_at),
        // ── Psychographic Profile ──
        u.soviet_nostalgia        = $soviet_nostalgia,
        u.locus_of_control        = $locus_of_control,
        u.coping_style            = $coping_style,
        u.security_vs_freedom     = $security_vs_freedom,
        // ── Trust Landscape ──
        u.trust_government        = $trust_government,
        u.trust_media             = $trust_media,
        u.trust_peers             = $trust_peers,
        u.trust_foreign           = $trust_foreign,
        // ── Linguistic Intelligence ──
        u.code_switching          = $code_switching,
        u.certainty_level         = $certainty_level,
        u.rhetorical_strategy     = $rhetorical_strategy,
        u.pronoun_pattern         = $pronoun_pattern,
        // ── Financial Signals ──
        u.financial_distress_level = $financial_distress_level,
        u.price_sensitivity       = $price_sensitivity

  // User → Comment (with timestamp on relationship)
  MERGE (u)-[rw:WROTE]->(c)
  ON CREATE SET rw.posted_at = datetime($posted_at)

  // User → Intent (with frequency + sentiment tracking)
  FOREACH (_ IN CASE WHEN $primary_intent IS NOT NULL THEN [1] ELSE [] END |
    MERGE (i:Intent {name: $primary_intent})
    MERGE (u)-[ri:EXHIBITS]->(i)
    ON CREATE SET ri.count        = 1,
                  ri.avg_sentiment = $sentiment_score,
                  ri.first_seen   = datetime($posted_at),
                  ri.last_seen    = datetime($posted_at)
    ON MATCH  SET ri.count        = ri.count + 1,
                  ri.avg_sentiment = (ri.avg_sentiment * ri.count + $sentiment_score) / (ri.count + 1),
                  ri.last_seen    = datetime($posted_at)
  )

  // User → Sentiment (with timestamps)
  FOREACH (_ IN CASE WHEN $sentiment IS NOT NULL THEN [1] ELSE [] END |
    MERGE (s:Sentiment {label: $sentiment})
    MERGE (u)-[rs:HAS_SENTIMENT]->(s)
    ON CREATE SET rs.count      = 1,
                  rs.first_seen = datetime($posted_at),
                  rs.last_seen  = datetime($posted_at)
    ON MATCH  SET rs.count      = rs.count + 1,
                  rs.last_seen  = datetime($posted_at)
  )

  // User → Social sentiment tags
  FOREACH (tag IN $social_sentiment_tags |
    MERGE (st:SentimentTag {name: tag})
    MERGE (u)-[rst:HAS_SENTIMENT_TAG]->(st)
    ON CREATE SET rst.count      = 1,
                  rst.first_seen = datetime($posted_at),
                  rst.last_seen  = datetime($posted_at)
    ON MATCH  SET rst.count      = coalesce(rst.count, 0) + 1,
                  rst.last_seen  = datetime($posted_at)
  )

  // User → GeopoliticalStance
  FOREACH (_ IN CASE WHEN $geopolitical_alignment IS NOT NULL
                      AND $geopolitical_alignment <> 'Neutral'
                      AND $geopolitical_alignment <> 'Ambiguous'
                      AND $geopolitical_alignment <> 'null'
                      AND $geopolitical_alignment <> 'None'
               THEN [1] ELSE [] END |
    MERGE (g:GeopoliticalStance {alignment: $geopolitical_alignment})
    MERGE (u)-[rg:ALIGNED_WITH]->(g)
    ON CREATE SET rg.first_seen = datetime($posted_at), rg.confidence = 0.7
  )

  // User → CollectiveMemory
  FOREACH (_ IN CASE WHEN $collective_memory IS NOT NULL
                      AND $collective_memory <> 'null'
               THEN [1] ELSE [] END |
    MERGE (m:CollectiveMemory {event: $collective_memory})
    MERGE (u)-[rm:REMEMBERS]->(m)
    ON CREATE SET rm.first_seen = datetime($posted_at)
    ON MATCH  SET rm.last_seen  = datetime($posted_at)
  )

  // User → LifeStage
  FOREACH (_ IN CASE WHEN $life_stage IS NOT NULL AND $life_stage <> 'null'
               THEN [1] ELSE [] END |
    MERGE (l:LifeStage {name: $life_stage})
    MERGE (u)-[:IN_LIFE_STAGE]->(l)
  )

  // User → BusinessOpportunity
  FOREACH (_ IN CASE WHEN $business_opportunity_type IS NOT NULL
                      AND $business_opportunity_type <> 'none'
                      AND $business_opportunity_type <> 'null'
               THEN [1] ELSE [] END |
    MERGE (b:BusinessOpportunity {type: $business_opportunity_type})
    SET b.description = $business_opportunity_desc
    MERGE (u)-[:SIGNALS_OPPORTUNITY]->(b)
  )

  // User → Topics (normalized, with timestamps)
  FOREACH (topic IN $user_topics |
    MERGE (t:Topic {name: topic})
    MERGE (u)-[rt:INTERESTED_IN]->(t)
    ON CREATE SET rt.count      = 1,
                  rt.first_seen = datetime($posted_at),
                  rt.last_seen  = datetime($posted_at)
    ON MATCH  SET rt.count      = rt.count + 1,
                  rt.last_seen  = datetime($posted_at)
    // Tag the comment itself
    MERGE (c)-[:TAGGED]->(t)
  )

  // User → Entity mentions (with sentiment and frequency)
  FOREACH (entity IN $entities |
    MERGE (e:Entity {name: entity.name})
    ON CREATE SET e.type = entity.type
    ON MATCH  SET e.type = coalesce(e.type, entity.type)
    MERGE (u)-[rm:MENTIONS]->(e)
    ON CREATE SET rm.count      = 1,
                  rm.first_seen = datetime($posted_at),
                  rm.last_seen  = datetime($posted_at),
                  rm.sentiment  = entity.sentiment_toward
    ON MATCH  SET rm.count      = rm.count + 1,
                  rm.last_seen  = datetime($posted_at),
                  rm.sentiment  = coalesce(entity.sentiment_toward, rm.sentiment)
  )
)

// Comment → Entity mentions (available even for anonymous users)
FOREACH (entity IN $entities |
  MERGE (e:Entity {name: entity.name})
  ON CREATE SET e.type = entity.type
  ON MATCH  SET e.type = coalesce(e.type, entity.type)
  MERGE (c)-[:MENTIONS_ENTITY]->(e)
)

// ── User → User Social Network (who replied to whom) ──
// Only fires when this comment is a reply to another user's comment in the same post
FOREACH (_ IN CASE WHEN $telegram_user_id IS NOT NULL
                    AND $reply_to_telegram_user_id IS NOT NULL
                    AND $telegram_user_id <> $reply_to_telegram_user_id
               THEN [1] ELSE [] END |
  MERGE (commenter:User {telegram_user_id: $telegram_user_id})
  MERGE (replied_to:User {telegram_user_id: $reply_to_telegram_user_id})
  MERGE (commenter)-[rru:REPLIED_TO_USER]->(replied_to)
  ON CREATE SET rru.count      = 1,
                rru.first_seen = datetime($posted_at),
                rru.last_seen  = datetime($posted_at)
  ON MATCH  SET rru.count      = rru.count + 1,
                rru.last_seen  = datetime($posted_at)
)
"""

_CYPHER_POST_TOPICS = """
// ── Post + Channel Topic Tagging + Category Hierarchy ──
MERGE (p:Post {uuid: $post_uuid})
MERGE (ch:Channel {uuid: $channel_uuid})

FOREACH (item IN $topics |
  // Topic node (canonical name, pre-normalized)
  MERGE (t:Topic {name: item.name})
  ON CREATE SET t.proposed  = coalesce(item.proposed, false),
                t.created_at = datetime($posted_at)
  ON MATCH  SET t.proposed  = coalesce(t.proposed, false) OR coalesce(item.proposed, false)

  // TopicCategory hierarchy (prevents super-nodes)
  MERGE (cat:TopicCategory {name: item.category})
  MERGE (dom:TopicDomain {name: item.domain})
  MERGE (cat)-[:IN_DOMAIN]->(dom)
  MERGE (t)-[:BELONGS_TO_CATEGORY]->(cat)

  // Post tagged with topic
  MERGE (p)-[:TAGGED]->(t)

  // Channel discusses this topic (with frequency)
  MERGE (ch)-[rd:DISCUSSES]->(t)
  ON CREATE SET rd.count     = 1,
                rd.last_seen = datetime($posted_at)
  ON MATCH  SET rd.count     = rd.count + 1,
                rd.last_seen = datetime($posted_at)
)

// ── Topic Co-occurrence Matrix (undirected, t1 < t2 prevents duplicates) ──
FOREACH (t1 IN $topics |
  FOREACH (t2 IN $topics |
    FOREACH (_ IN CASE WHEN t1.name < t2.name THEN [1] ELSE [] END |
      MERGE (n1:Topic {name: t1.name})
      MERGE (n2:Topic {name: t2.name})
      MERGE (n1)-[r:CO_OCCURS_WITH]-(n2)
      ON CREATE SET r.count     = 1,
                    r.last_seen = datetime($posted_at)
      ON MATCH  SET r.count     = r.count + 1,
                    r.last_seen = datetime($posted_at)
    )
  )
)

// ── Post → Primary Sentiment (message-scoped) ──
FOREACH (_ IN CASE WHEN $post_sentiment IS NOT NULL THEN [1] ELSE [] END |
  MERGE (smsg:Sentiment {label: $post_sentiment})
  MERGE (p)-[rps:HAS_SENTIMENT]->(smsg)
  ON CREATE SET rps.count = 1,
                rps.first_seen = datetime($posted_at),
                rps.last_seen = datetime($posted_at)
  ON MATCH  SET rps.count = coalesce(rps.count, 0) + 1,
                rps.last_seen = datetime($posted_at)
)

// ── Post → Social sentiment tags (message-scoped) ──
FOREACH (tag IN $post_social_sentiment_tags |
  MERGE (st:SentimentTag {name: tag})
  MERGE (p)-[rpt:HAS_SENTIMENT_TAG]->(st)
  ON CREATE SET rpt.count = 1,
                rpt.first_seen = datetime($posted_at),
                rpt.last_seen = datetime($posted_at)
  ON MATCH  SET rpt.count = coalesce(rpt.count, 0) + 1,
                rpt.last_seen = datetime($posted_at)
)
"""


# ── Helpers ───────────────────────────────────────────────────────────────────

def _normalize_enum(value: str | None) -> str | None:
    """
    Normalize AI-generated enum values to consistent Title_Case.

    Prevents graph pollution from case variants:
      "neutral" / "Neutral" / "NEUTRAL" → "Neutral"
      "pro_armenia" / "Pro_Armenia" / "pro-armenia" → "Pro_Armenia"
    """
    if not value or value.lower() in ("null", "none", "n/a", "unknown"):
        return None
    # Normalize separators → underscore, then Title_Case each segment
    normalized = value.strip().replace("-", "_").replace(" ", "_")
    parts = normalized.split("_")
    return "_".join(p.capitalize() for p in parts if p)


_SOCIAL_SENTIMENT_TAGS = {
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

_SENTIMENT_CANON = {
    "positive": "Positive",
    "negative": "Negative",
    "neutral": "Neutral",
    "mixed": "Mixed",
    "urgent": "Urgent",
    "sarcastic": "Sarcastic",
}

_EMOTIONAL_TONE_HINTS = [
    ("anx", "Anxious"),
    ("worr", "Anxious"),
    ("fear", "Anxious"),
    ("frustr", "Frustrated"),
    ("ang", "Angry"),
    ("indignan", "Angry"),
    ("confus", "Confused"),
    ("uncertain", "Confused"),
    ("hope", "Hopeful"),
    ("optim", "Hopeful"),
    ("trust", "Trusting"),
    ("distrust", "Distrustful"),
    ("skeptic", "Distrustful"),
    ("solidar", "Solidarity"),
    ("exhaust", "Exhausted"),
    ("fatigue", "Exhausted"),
    ("grief", "Grief"),
    ("mour", "Grief"),
]

_DEFAULT_TAGS_BY_PRIMARY = {
    "Urgent": ["Anxious"],
    "Negative": ["Frustrated"],
    "Sarcastic": ["Distrustful"],
    "Positive": ["Hopeful"],
}


def _normalize_sentiment_label(value: str | None) -> str | None:
    if not value:
        return None
    key = str(value).strip().lower()
    if not key or key in {"null", "none", "n/a", "unknown"}:
        return None
    return _SENTIMENT_CANON.get(key)


def _normalize_social_sentiment_tags(
    raw_tags: list | None,
    *,
    primary_sentiment: str | None = None,
    emotional_tone: str | None = None,
) -> list[str]:
    tags: list[str] = []
    seen: set[str] = set()

    def _add(tag: str | None) -> None:
        if not tag:
            return
        normalized = str(tag).strip().title()
        if normalized not in _SOCIAL_SENTIMENT_TAGS or normalized in seen:
            return
        seen.add(normalized)
        tags.append(normalized)

    for item in raw_tags or []:
        if isinstance(item, str):
            _add(item)

    tone = str(emotional_tone or "").strip().lower()
    if tone:
        for needle, mapped in _EMOTIONAL_TONE_HINTS:
            if needle in tone:
                _add(mapped)

    for fallback in _DEFAULT_TAGS_BY_PRIMARY.get(primary_sentiment or "", []):
        _add(fallback)

    return tags


def _extract_sentiment_payload_from_analysis(analysis: dict) -> tuple[str | None, list[str]]:
    raw = analysis.get("raw_llm_response") or {}
    primary = _normalize_sentiment_label(raw.get("sentiment"))
    emotional_tone = raw.get("emotional_tone") if isinstance(raw.get("emotional_tone"), str) else None
    raw_tags = raw.get("social_sentiment_tags") if isinstance(raw.get("social_sentiment_tags"), list) else []
    tags = _normalize_social_sentiment_tags(
        raw_tags,
        primary_sentiment=primary,
        emotional_tone=emotional_tone,
    )
    return primary, tags


# ── Parameter Builders ────────────────────────────────────────────────────────

def _channel_post_params(channel: dict, post: dict) -> dict:
    import json
    reactions = post.get("reactions")
    if isinstance(reactions, dict):
        reactions = json.dumps(reactions)
    return {
        "channel_uuid":           channel.get("id"),
        "channel_username":       channel.get("channel_username"),
        "channel_title":          channel.get("channel_title"),
        "channel_description":    channel.get("description"),
        "telegram_channel_id":    channel.get("telegram_channel_id"),
        "member_count":           channel.get("member_count"),
        "post_uuid":              post["id"],
        "telegram_message_id":    post.get("telegram_message_id"),
        "text":                   (post.get("text") or "")[:1000],
        "posted_at":              str(post.get("posted_at")),
        "views":                  post.get("views", 0),
        "forwards":               post.get("forwards", 0),
        "reactions":              reactions,
        "comment_count":          post.get("comment_count", 0),
        "media_type":             post.get("media_type"),
    }


def _comment_params(comment: dict, post: dict, analysis: dict,
                    reply_to_telegram_user_id: int | None = None) -> dict:
    raw      = analysis.get("raw_llm_response") or {}
    social   = raw.get("social_signals") or {}
    demo     = raw.get("demographics") or {}
    behavior = raw.get("behavioral_pattern") or {}
    daily    = raw.get("daily_life") or {}
    biz      = raw.get("business_opportunity") or {}
    psycho   = raw.get("psychographic") or {}
    trust    = raw.get("trust_landscape") or {}
    ling     = raw.get("linguistic_intelligence") or {}
    fin      = raw.get("financial_signals") or {}

    uid = comment.get("telegram_user_id")

    # ── Time-of-day derivation (from posted_at ISO timestamp) ──────────────────
    posting_hour = None
    time_of_day  = "unknown"
    try:
        from datetime import datetime, timezone
        raw_ts = comment.get("posted_at")
        if raw_ts:
            dt = datetime.fromisoformat(str(raw_ts).replace("Z", "+00:00"))
            posting_hour = dt.hour
            if   0  <= posting_hour < 6:  time_of_day = "late_night"
            elif 6  <= posting_hour < 12: time_of_day = "morning"
            elif 12 <= posting_hour < 17: time_of_day = "afternoon"
            elif 17 <= posting_hour < 21: time_of_day = "evening"
            else:                          time_of_day = "night"
    except Exception:
        pass

    # Normalize topics for this specific user
    candidate_topics = raw.get("topics")
    raw_topics = cast(list, candidate_topics) if isinstance(candidate_topics, list) else []
    model_topics = normalize_model_topics(raw_topics)
    if model_topics:
        canon_user_topics = [str(item["name"]) for item in model_topics if item.get("name")]
    else:
        fallback_topics = [str(item) for item in (analysis.get("topics") or []) if item]
        canon_user_topics = normalize_topics(fallback_topics)

    entities = _extract_entities(raw)
    primary_sentiment, social_sentiment_tags = _extract_sentiment_payload_from_analysis(analysis)

    return {
        "comment_uuid":                 comment["id"],
        "telegram_message_id":          comment.get("telegram_message_id"),
        "text":                         (comment.get("text") or "")[:500],
        "posted_at":                    str(comment.get("posted_at")),
        "time_of_day":                  time_of_day,
        "posting_hour":                 posting_hour,
        "telegram_user_id":             uid,
        "post_uuid":                    post["id"],
        "reply_to_telegram_user_id":    reply_to_telegram_user_id,
        # ── Demographics (from profile + AI) ──
        "inferred_gender":              demo.get("inferred_gender", "unknown"),
        "inferred_age_bracket":         demo.get("inferred_age_bracket", "unknown"),
        "language":                     demo.get("language") or analysis.get("language"),
        # ── Behavioral ──
        "community_role":               behavior.get("community_role"),
        "communication_style":          behavior.get("communication_style"),
        # ── Social signals ──
        "geopolitical_alignment":       _normalize_enum(social.get("geopolitical_alignment")),
        "collective_memory":            social.get("collective_memory"),
        "migration_intent":             social.get("migration_intent"),
        "diaspora_signals":             social.get("diaspora_signals"),
        "authority_attitude":           social.get("authority_attitude"),
        # ── Psychographic Profile ──
        "soviet_nostalgia":             psycho.get("soviet_nostalgia"),
        "locus_of_control":             psycho.get("locus_of_control"),
        "coping_style":                 psycho.get("coping_style"),
        "security_vs_freedom":          psycho.get("security_vs_freedom"),
        # ── Trust Landscape ──
        "trust_government":             trust.get("trust_government"),
        "trust_media":                  trust.get("trust_media"),
        "trust_peers":                  trust.get("trust_peers"),
        "trust_foreign":                trust.get("trust_foreign"),
        # ── Linguistic Intelligence ──
        "code_switching":               ling.get("code_switching"),
        "certainty_level":              ling.get("certainty_level"),
        "rhetorical_strategy":          ling.get("rhetorical_strategy"),
        "pronoun_pattern":              ling.get("pronoun_pattern"),
        # ── Financial Signals ──
        "financial_distress_level":     fin.get("financial_distress_level"),
        "price_sensitivity":            fin.get("price_sensitivity"),
        # ── Life & business ──
        "life_stage":                   daily.get("life_stage_signal"),
        "business_opportunity_type":    biz.get("opportunity_type"),
        "business_opportunity_desc":    (biz.get("description") or "")[:200],
        # ── Intent & sentiment ──
        "primary_intent":               analysis.get("primary_intent"),
        "sentiment":                    primary_sentiment,
        "social_sentiment_tags":        social_sentiment_tags,
        "sentiment_score":              float(analysis.get("sentiment_score") or 0.0),
        # ── Normalized topics for this user ──
        "user_topics":                  canon_user_topics,
        # ── Entity mentions for user/comment linking ──
        "entities":                     entities,
    }


def _extract_entities(raw_response: dict) -> list[dict]:
    entities: list[dict] = []
    seen: set[str] = set()

    for item in (raw_response.get("entities") or []):
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip()
        if not name:
            continue
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)
        entities.append(
            {
                "name": name,
                "type": str(item.get("type") or "concept"),
                "sentiment_toward": item.get("sentiment_toward"),
            }
        )
    return entities


def _collect_topic_items(analyses: dict, post_analysis: dict | None = None) -> list[dict]:
    """Collect canonical topic items with category, domain, and proposed flag."""
    items_by_name: dict[str, dict] = {}

    analysis_rows = list(analyses.values())
    if isinstance(post_analysis, dict):
        analysis_rows.append(post_analysis)

    for analysis in analysis_rows:
        raw = analysis.get("raw_llm_response") or {}
        candidate_topics = raw.get("topics")
        raw_topics = cast(list, candidate_topics) if isinstance(candidate_topics, list) else []
        normalized_topics = normalize_model_topics(raw_topics)
        if not normalized_topics:
            normalized_topics = normalize_model_topics(
                [str(topic) for topic in (analysis.get("topics") or []) if topic]
            )

        for topic_item in normalized_topics:
            name = str(topic_item.get("name") or "").strip()
            if not name:
                continue

            proposed = bool(topic_item.get("proposed", False))
            category = normalize_topic_category(topic_item.get("closest_category") or get_topic_category(name))
            domain = normalize_topic_domain(topic_item.get("domain") or get_topic_domain(name))

            existing = items_by_name.get(name)
            if not existing:
                items_by_name[name] = {
                    "name": name,
                    "category": category,
                    "domain": domain,
                    "proposed": proposed,
                }
            else:
                existing["proposed"] = bool(existing.get("proposed") or proposed)

    return list(items_by_name.values())


def _collect_raw_topics(analyses: dict) -> list[str]:
    """Collect topic names across all analyses for compatibility callers."""
    return [item["name"] for item in _collect_topic_items(analyses)]


# ── Re-export for main.py ─────────────────────────────────────────────────────

def _collect_topics(analyses: dict) -> list[str]:
    """Public alias used in main.py for logging topic count."""
    return normalize_topics(_collect_raw_topics(analyses))
