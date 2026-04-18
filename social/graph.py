from __future__ import annotations

from typing import Any

from loguru import logger
from neo4j import GraphDatabase

import config


def _slug(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return "unknown"
    out = []
    dash = False
    for char in text:
        if char.isalnum():
            out.append(char)
            dash = False
        elif not dash:
            out.append("-")
            dash = True
    return "".join(out).strip("-") or "unknown"


class SocialGraphWriter:
    def __init__(self) -> None:
        uri = config.SOCIAL_NEO4J_URI
        if uri.startswith("neo4j+s://"):
            uri = uri.replace("neo4j+s://", "neo4j+ssc://")
        self.driver = GraphDatabase.driver(
            uri,
            auth=(config.SOCIAL_NEO4J_USERNAME, config.SOCIAL_NEO4J_PASSWORD),
        )
        self._setup_constraints()

    def close(self) -> None:
        self.driver.close()

    def _setup_constraints(self) -> None:
        statements = [
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:TrackedEntity) REQUIRE n.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:SocialActivity) REQUIRE n.uid IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Platform) REQUIRE n.name IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:ContentFormat) REQUIRE n.name IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:CTA) REQUIRE n.type IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Region) REQUIRE n.name IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:TimePeriod) REQUIRE n.key IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Topic) REQUIRE n.name IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Sentiment) REQUIRE n.name IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:MarketingTactic) REQUIRE n.name IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:SocialProduct) REQUIRE n.key IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:SocialAudience) REQUIRE n.key IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:SocialPainPoint) REQUIRE n.key IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:SocialValueProposition) REQUIRE n.key IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:SocialCustomerIntent) REQUIRE n.key IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:SocialCompetitor) REQUIRE n.key IS UNIQUE",
        ]
        with self.driver.session(database=config.SOCIAL_NEO4J_DATABASE) as session:
            for statement in statements:
                try:
                    session.run(statement).consume()
                except Exception as exc:  # pragma: no cover - environment-specific
                    logger.debug("Social Neo4j constraint skipped: {}", exc)

    def sync_activity(self, item: dict[str, Any]) -> None:
        activity = dict(item)
        entity = dict(activity.get("entity") or {})
        analysis = dict((activity.get("analysis") or {}).get("analysis_payload") or {})

        params = {
            "entity_id": entity.get("id"),
            "entity_name": entity.get("name"),
            "industry": entity.get("industry"),
            "activity_uid": activity.get("activity_uid"),
            "platform": activity.get("platform"),
            "source_kind": activity.get("source_kind"),
            "source_url": activity.get("source_url"),
            "text_content": activity.get("text_content"),
            "published_at": activity.get("published_at"),
            "author_handle": activity.get("author_handle"),
            "parent_activity_uid": activity.get("parent_activity_uid"),
            "cta_type": activity.get("cta_type"),
            "content_format": activity.get("content_format"),
            "region_name": activity.get("region_name"),
            "sentiment": analysis.get("sentiment"),
            "sentiment_score": analysis.get("sentiment_score"),
            "marketing_tactic": analysis.get("marketing_tactic"),
            "summary": analysis.get("summary"),
            "products": self._entity_scoped_nodes(entity.get("id"), analysis.get("products"), "name"),
            "audiences": self._entity_scoped_nodes(entity.get("id"), analysis.get("audience_segments"), "name"),
            "pain_points": self._entity_scoped_nodes(entity.get("id"), analysis.get("pain_points"), "name"),
            "value_props": self._entity_scoped_nodes(entity.get("id"), analysis.get("value_propositions"), "claim"),
            "topics": self._topics(analysis.get("topics")),
            "competitors": self._competitors(analysis.get("competitive_signals")),
            "customer_intent": self._customer_intent(entity.get("id"), analysis.get("customer_intent")),
            "time_key": self._time_key(activity.get("published_at")),
        }

        query = """
        MERGE (entity:TrackedEntity {id: $entity_id})
        SET entity.name = $entity_name,
            entity.industry = $industry

        MERGE (activity:SocialActivity {uid: $activity_uid})
        SET activity.platform = $platform,
            activity.source_kind = $source_kind,
            activity.source_url = $source_url,
            activity.text = $text_content,
            activity.author_handle = $author_handle,
            activity.summary = $summary,
            activity.sentiment_score = $sentiment_score,
            activity.published_at = CASE
              WHEN $published_at IS NULL OR $published_at = '' THEN NULL
              ELSE datetime($published_at)
            END

        MERGE (entity)-[:HAS_ACTIVITY]->(activity)

        FOREACH (_ IN CASE WHEN $parent_activity_uid IS NULL OR $parent_activity_uid = '' THEN [] ELSE [1] END |
          MERGE (parent:SocialActivity {uid: $parent_activity_uid})
          MERGE (activity)-[:COMMENTS_ON]->(parent)
        )

        MERGE (platform:Platform {name: $platform})
        MERGE (entity)-[:USES_PLATFORM]->(platform)
        MERGE (platform)-[:HOSTS]->(activity)

        FOREACH (_ IN CASE WHEN $content_format IS NULL OR $content_format = '' THEN [] ELSE [1] END |
          MERGE (format:ContentFormat {name: $content_format})
          MERGE (activity)-[:HAS_FORMAT]->(format)
        )

        FOREACH (_ IN CASE WHEN $cta_type IS NULL OR $cta_type = '' THEN [] ELSE [1] END |
          MERGE (cta:CTA {type: $cta_type})
          MERGE (activity)-[:HAS_CTA]->(cta)
        )

        FOREACH (_ IN CASE WHEN $region_name IS NULL OR $region_name = '' THEN [] ELSE [1] END |
          MERGE (region:Region {name: $region_name})
          MERGE (activity)-[:TARGETS_REGION]->(region)
        )

        FOREACH (_ IN CASE WHEN $time_key IS NULL OR $time_key = '' THEN [] ELSE [1] END |
          MERGE (period:TimePeriod {key: $time_key})
          SET period.label = $time_key
          MERGE (activity)-[:PUBLISHED_IN]->(period)
        )

        FOREACH (_ IN CASE WHEN $sentiment IS NULL OR $sentiment = '' THEN [] ELSE [1] END |
          MERGE (sentiment:Sentiment {name: $sentiment})
          MERGE (activity)-[:HAS_SENTIMENT]->(sentiment)
        )

        FOREACH (_ IN CASE WHEN $marketing_tactic IS NULL OR $marketing_tactic = '' THEN [] ELSE [1] END |
          MERGE (tactic:MarketingTactic {name: $marketing_tactic})
          MERGE (activity)-[:USES_TACTIC]->(tactic)
        )

        FOREACH (prod IN $products |
          MERGE (p:SocialProduct {key: prod.key})
          SET p.name = prod.name,
              p.entity_id = $entity_id
          MERGE (entity)-[:OFFERS]->(p)
          MERGE (activity)-[:PROMOTES]->(p)
        )

        FOREACH (aud IN $audiences |
          MERGE (a:SocialAudience {key: aud.key})
          SET a.name = aud.name,
              a.entity_id = $entity_id
          MERGE (activity)-[:TARGETS_AUDIENCE]->(a)
        )

        FOREACH (pain IN $pain_points |
          MERGE (p:SocialPainPoint {key: pain.key})
          SET p.name = pain.name,
              p.entity_id = $entity_id
          MERGE (activity)-[:ADDRESSES]->(p)
        )

        FOREACH (vp IN $value_props |
          MERGE (v:SocialValueProposition {key: vp.key})
          SET v.claim = vp.name,
              v.entity_id = $entity_id
          MERGE (activity)-[:OFFERS_VALUE]->(v)
        )

        FOREACH (topic IN $topics |
          MERGE (t:Topic {name: topic.name})
          MERGE (activity)-[:COVERS]->(t)
        )

        FOREACH (comp IN $competitors |
          MERGE (c:SocialCompetitor {key: comp.key})
          SET c.name = comp.name,
              c.domain = comp.domain
          MERGE (activity)-[:MENTIONS_COMPETITOR]->(c)
        )

        FOREACH (_ IN CASE WHEN $customer_intent.key IS NULL THEN [] ELSE [1] END |
          MERGE (ci:SocialCustomerIntent {key: $customer_intent.key})
          SET ci.name = $customer_intent.name,
              ci.entity_id = $entity_id
          MERGE (activity)-[:HELPS_WITH]->(ci)
        )
        """

        with self.driver.session(database=config.SOCIAL_NEO4J_DATABASE) as session:
            session.run(query, params).consume()

    @staticmethod
    def _entity_scoped_nodes(entity_id: str | None, rows: Any, key_field: str) -> list[dict[str, str]]:
        out: list[dict[str, str]] = []
        for row in rows or []:
            if isinstance(row, str):
                name = row.strip()
            elif isinstance(row, dict):
                name = str(row.get(key_field) or row.get("name") or "").strip()
            else:
                name = ""
            if not name:
                continue
            out.append(
                {
                    "key": f"{entity_id}:{_slug(name)}",
                    "name": name[:160],
                }
            )
        return out

    @staticmethod
    def _topics(rows: Any) -> list[dict[str, str]]:
        out: list[dict[str, str]] = []
        for row in rows or []:
            if isinstance(row, str):
                name = row.strip()
            elif isinstance(row, dict):
                name = str(row.get("name") or "").strip()
            else:
                name = ""
            if name:
                out.append({"name": name[:120]})
        return out

    @staticmethod
    def _competitors(rows: Any) -> list[dict[str, str | None]]:
        out: list[dict[str, str | None]] = []
        for row in rows or []:
            if isinstance(row, str):
                name = row.strip()
                domain = None
            elif isinstance(row, dict):
                name = str(row.get("competitor_name") or row.get("name") or "").strip()
                domain = str(row.get("domain") or "").strip() or None
            else:
                name = ""
                domain = None
            if not name:
                continue
            domain_key = domain or "no-domain"
            out.append({"key": f"{_slug(name)}:{_slug(domain_key)}", "name": name[:160], "domain": domain})
        return out

    @staticmethod
    def _customer_intent(entity_id: str | None, value: Any) -> dict[str, str | None]:
        name = str(value or "").strip()
        if not name:
            return {"key": None, "name": ""}
        return {"key": f"{entity_id}:{_slug(name)}", "name": name[:160]}

    @staticmethod
    def _time_key(value: Any) -> str | None:
        text = str(value or "").strip()
        if not text:
            return None
        try:
            return text.replace("Z", "+00:00")[:7]
        except Exception:
            return None
