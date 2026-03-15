"""
graph_dashboard.py — Graph API query layer for the integrated /graph page.

This module maps the existing community-intelligence Neo4j schema
(Channel/Post/User/Topic/Intent/Sentiment/...) to graph-dashboard responses.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from api.db import run_query, run_single


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _to_iso(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    if not text:
        return None
    return text


def _safe_name(value: Any, fallback: str) -> str:
    text = (str(value) if value is not None else "").strip()
    return text or fallback


def _parse_timeframe_to_since(timeframe: str | None) -> datetime:
    now = datetime.now(timezone.utc)
    tf = (timeframe or "").strip().lower()

    mapping = {
        "last 24h": timedelta(hours=24),
        "24h": timedelta(hours=24),
        "last 7 days": timedelta(days=7),
        "7d": timedelta(days=7),
        "last month": timedelta(days=30),
        "30d": timedelta(days=30),
        "last 3 months": timedelta(days=90),
        "90d": timedelta(days=90),
    }

    delta = mapping.get(tf, timedelta(days=7))
    return now - delta


def _normalize_channels(values: list[str] | None) -> list[str]:
    if not values:
        return []
    out: list[str] = []
    seen = set()
    for raw in values:
        key = (raw or "").strip().lower().lstrip("@")
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out


def _normalize_sentiments(values: list[str] | None) -> list[str]:
    if not values:
        return []
    out: list[str] = []
    seen = set()
    for raw in values:
        label = (raw or "").strip().title()
        if not label or label in seen:
            continue
        seen.add(label)
        out.append(label)
    return out


def _normalize_topics(values: list[str] | None) -> list[str]:
    if not values:
        return []
    out: list[str] = []
    seen = set()
    for raw in values:
        text = (raw or "").strip()
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(text)
    return out


def _normalize_layers(values: list[str] | None) -> set[str]:
    layers: set[str] = {"topic"}
    if not values:
        return layers

    for raw in values:
        layer = (raw or "").strip().lower()
        if not layer:
            continue
        layers.add(layer)
    return layers


def _add_node(nodes: dict[str, dict], node: dict) -> None:
    node_id = node["id"]
    existing = nodes.get(node_id)
    if not existing:
        nodes[node_id] = node
        return

    existing["val"] = max(_to_float(existing.get("val"), 0.0), _to_float(node.get("val"), 0.0))
    existing["size"] = max(_to_float(existing.get("size"), 0.0), _to_float(node.get("size"), 0.0))
    existing["signal"] = max(_to_float(existing.get("signal"), 0.0), _to_float(node.get("signal"), 0.0))
    if not existing.get("category") and node.get("category"):
        existing["category"] = node.get("category")


def _build_channel_topic_graph(filters: dict) -> dict:
    since = _parse_timeframe_to_since(filters.get("timeframe"))
    channel_filters = _normalize_channels(filters.get("channels") or filters.get("brandSource"))
    topic_filters = _normalize_topics(filters.get("topics"))
    sentiment_filters = _normalize_sentiments(filters.get("sentiment") or filters.get("sentiments"))

    insight_mode = (filters.get("insightMode") or "marketMap").strip().lower()
    source_profile = (filters.get("sourceProfile") or "balanced").strip().lower()
    connection_strength = max(1, min(5, _to_int(filters.get("connectionStrength"), 3)))
    confidence_threshold = max(1, min(100, _to_int(filters.get("confidenceThreshold"), 35)))
    layers = _normalize_layers(filters.get("layers"))

    edge_limit_default = {1: 420, 2: 320, 3: 240, 4: 180, 5: 130}[connection_strength]
    topic_limit_default = {1: 48, 2: 36, 3: 28, 4: 20, 5: 14}[connection_strength]
    min_weight_default = {1: 1, 2: 1, 3: 1, 4: 2, 5: 3}[connection_strength]

    edge_limit = max(20, min(_to_int(filters.get("max_edges"), edge_limit_default), 1200))
    topic_limit = max(6, min(_to_int(filters.get("max_nodes"), topic_limit_default), 120))
    min_weight = max(min_weight_default, _to_int(filters.get("min_weight"), min_weight_default))

    include_product_layer = "product" in layers
    include_audience_layer = "audience" in layers
    include_intent_layer = "intent" in layers
    include_competitor_layer = "competitor" in layers
    include_painpoint_layer = "painpoint" in layers
    include_valueprop_layer = "valueprop" in layers
    include_sentiment_layer = "sentiment" in layers

    requires_competitor_stats = include_competitor_layer or insight_mode in {"competitormoves", "opportunities"}
    requires_sentiment_stats = (
        include_sentiment_layer
        or include_painpoint_layer
        or include_valueprop_layer
        or bool(sentiment_filters)
        or insight_mode in {"messagefit", "competitormoves", "opportunities"}
    )

    rows = run_query(
        """
        MATCH (ch:Channel)<-[:IN_CHANNEL]-(p:Post)
        WHERE p.posted_at >= datetime($since)
          AND (
              $channel_count = 0
              OR toLower(coalesce(ch.username, '')) IN $channels
              OR toLower(coalesce(ch.title, '')) IN $channels
              OR ch.uuid IN $channels
              OR ('channel:' + ch.uuid) IN $channels
          )
        MATCH (p)-[:TAGGED]->(t:Topic)
        OPTIONAL MATCH (t)-[:BELONGS_TO_CATEGORY]->(cat:TopicCategory)
        OPTIONAL MATCH (p)<-[:REPLIES_TO]-(c:Comment)
        WITH ch, t,
             coalesce(cat.name, 'General') AS category,
             count(DISTINCT p) AS postMentions,
             count(c) AS commentMentions,
             avg(coalesce(p.views, 0)) AS avgViews,
             count(DISTINCT date(p.posted_at)) AS activeDays,
             max(p.posted_at) AS lastSeen
        WHERE postMentions >= $min_weight
          AND ($topic_count = 0 OR t.name IN $topics)
        RETURN ch.uuid AS channel_uuid,
               ch.username AS channel_username,
               ch.title AS channel_title,
               ch.member_count AS channel_members,
               t.name AS topic,
               category,
               postMentions,
               commentMentions,
               avgViews,
               activeDays,
               toString(lastSeen) AS lastSeen
        ORDER BY postMentions DESC
        LIMIT $edge_limit
        """,
        {
            "since": since.isoformat(),
            "channels": channel_filters,
            "channel_count": len(channel_filters),
            "topics": topic_filters,
            "topic_count": len(topic_filters),
            "edge_limit": edge_limit,
            "min_weight": min_weight,
        },
    )

    if not rows:
        return {
            "nodes": [],
            "links": [],
            "meta": {
                "timeframe": filters.get("timeframe") or "Last 7 Days",
                "since": since.isoformat(),
                "insightMode": insight_mode,
                "sourceProfile": source_profile,
                "confidenceThreshold": confidence_threshold,
                "connectionStrength": connection_strength,
                "layers": sorted(layers),
                "selectedChannels": channel_filters,
                "topicCountConsidered": 0,
                "topicCountReturned": 0,
                "thresholdRelaxed": False,
                "generatedAt": datetime.now(timezone.utc).isoformat(),
            },
        }

    topic_stats: dict[str, dict] = {}
    for row in rows:
        topic_name = row["topic"]
        topic_entry = topic_stats.setdefault(
            topic_name,
            {
                "topic": topic_name,
                "category": row.get("category") or "General",
                "total_posts": 0,
                "total_comments": 0,
                "weighted_views": 0.0,
                "active_days": 0,
                "channels": {},
                "last_seen": "",
                "competitor_posts": 0,
                "competitor_channels": 0,
                "competitor_names": [],
                "sentiments": {},
            },
        )

        post_mentions = _to_int(row.get("postMentions"), 0)
        comment_mentions = _to_int(row.get("commentMentions"), 0)
        avg_views = _to_float(row.get("avgViews"), 0.0)
        active_days = _to_int(row.get("activeDays"), 0)
        channel_uuid = row.get("channel_uuid")

        topic_entry["total_posts"] += post_mentions
        topic_entry["total_comments"] += comment_mentions
        topic_entry["weighted_views"] += avg_views * max(1, post_mentions)
        topic_entry["active_days"] = max(topic_entry["active_days"], active_days)
        if channel_uuid:
            topic_entry["channels"][channel_uuid] = topic_entry["channels"].get(channel_uuid, 0) + post_mentions

        last_seen = _safe_name(row.get("lastSeen"), "")
        if last_seen and last_seen > topic_entry["last_seen"]:
            topic_entry["last_seen"] = last_seen

    if requires_competitor_stats and channel_filters and topic_stats:
        competitor_topic_rows = run_query(
            """
            MATCH (ch:Channel)<-[:IN_CHANNEL]-(p:Post)-[:TAGGED]->(t:Topic)
            WHERE p.posted_at >= datetime($since)
              AND t.name IN $topics
              AND NOT (
                  toLower(coalesce(ch.username, '')) IN $channels
                  OR toLower(coalesce(ch.title, '')) IN $channels
                  OR ch.uuid IN $channels
                  OR ('channel:' + ch.uuid) IN $channels
              )
            RETURN t.name AS topic,
                   count(DISTINCT ch) AS competitorChannels,
                   count(DISTINCT p) AS competitorPosts,
                   collect(DISTINCT coalesce(ch.title, ch.username))[..8] AS competitorNames
            """,
            {
                "since": since.isoformat(),
                "topics": list(topic_stats.keys()),
                "channels": channel_filters,
            },
        )

        for row in competitor_topic_rows:
            topic_name = row.get("topic")
            if topic_name not in topic_stats:
                continue
            topic_stats[topic_name]["competitor_posts"] = _to_int(row.get("competitorPosts"), 0)
            topic_stats[topic_name]["competitor_channels"] = _to_int(row.get("competitorChannels"), 0)
            topic_stats[topic_name]["competitor_names"] = row.get("competitorNames") or []

    sentiment_rows: list[dict] = []
    if requires_sentiment_stats and topic_stats:
        sentiment_rows = run_query(
            """
            CALL () {
                MATCH (p:Post)-[:TAGGED]->(t:Topic)
                MATCH (p)-[:HAS_SENTIMENT]->(s:Sentiment)
                WHERE p.posted_at >= datetime($since)
                  AND t.name IN $topics
                RETURN t.name AS topic,
                       s.label AS sentiment,
                       count(*) AS weight
                UNION ALL
                MATCH (c:Comment)-[:TAGGED]->(t:Topic)
                MATCH (c)-[:HAS_SENTIMENT]->(s:Sentiment)
                WHERE c.posted_at >= datetime($since)
                  AND t.name IN $topics
                RETURN t.name AS topic,
                       s.label AS sentiment,
                       count(*) AS weight
            }
            RETURN topic,
                   sentiment,
                   sum(weight) AS weight
            ORDER BY weight DESC
            LIMIT 1200
            """,
            {"since": since.isoformat(), "topics": list(topic_stats.keys())},
        )

        for row in sentiment_rows:
            topic_name = row.get("topic")
            if topic_name not in topic_stats:
                continue
            label = _safe_name(row.get("sentiment"), "Unknown")
            weight = max(0, _to_int(row.get("weight"), 0))
            topic_stats[topic_name]["sentiments"][label] = topic_stats[topic_name]["sentiments"].get(label, 0) + weight

    selected_sentiment_topics = set(topic_stats.keys())
    if sentiment_filters:
        selected_sentiment_topics = set()
        for topic_name, stats in topic_stats.items():
            matched_weight = sum(stats["sentiments"].get(label, 0) for label in sentiment_filters)
            if matched_weight > 0:
                selected_sentiment_topics.add(topic_name)

    negative_sentiments = {"Negative", "Urgent", "Sarcastic"}
    positive_sentiments = {"Positive"}

    topic_ranked: list[dict] = []
    for topic_name, stats in topic_stats.items():
        if topic_name not in selected_sentiment_topics:
            continue

        total_posts = max(0, _to_int(stats.get("total_posts"), 0))
        if total_posts <= 0:
            continue
        total_comments = max(0, _to_int(stats.get("total_comments"), 0))
        channel_count = max(1, len(stats.get("channels") or {}))
        max_channel_posts = max((stats.get("channels") or {"_": 0}).values())
        avg_views = _to_float(stats.get("weighted_views"), 0.0) / max(1, total_posts)
        active_days = max(1, _to_int(stats.get("active_days"), 0))

        ownership_rate = min(100.0, (max_channel_posts / max(1, total_posts)) * 100.0)
        need_rate = min(100.0, (total_comments / max(1, total_posts)) * 18.0)
        competitor_posts = max(0, _to_int(stats.get("competitor_posts"), 0))
        competitor_rate = min(100.0, (competitor_posts / max(1, total_posts + competitor_posts)) * 100.0)
        momentum = min(100.0, (total_posts / max(1, active_days)) * 12.0)
        specificity = max(0.0, min(100.0, 100.0 - (channel_count * 11.0) + min(28.0, total_posts * 2.2)))

        evidence_score = min(100.0, total_posts * 9.0)
        breadth_score = min(100.0, channel_count * 22.0)
        recency_score = min(100.0, active_days * 16.0)
        confidence = int(round(max(1.0, min(99.0, (0.52 * evidence_score) + (0.30 * breadth_score) + (0.18 * recency_score)))))

        performance_score = (total_posts * 4.0) + (total_comments * 1.35) + (avg_views * 0.03)
        strategy_score = (channel_count * 18.0) + ((100.0 - abs(ownership_rate - 50.0)) * 0.8) + (active_days * 6.0)

        if source_profile == "performance":
            source_score = performance_score
        elif source_profile in {"brandstrategy", "strategy"}:
            source_score = strategy_score
        else:
            source_score = (0.55 * performance_score) + (0.45 * strategy_score)

        if insight_mode == "ownership":
            insight_score = (ownership_rate * 2.0) + (channel_count * 12.0) + (source_score * 0.20)
        elif insight_mode == "messagefit":
            insight_score = (need_rate * 2.0) + (total_comments * 1.2) + (source_score * 0.10)
        elif insight_mode == "competitormoves":
            insight_score = (competitor_rate * 2.2) + (momentum * 1.1) + (source_score * 0.08)
        elif insight_mode == "opportunities":
            insight_score = (need_rate * 1.9) + ((100.0 - competitor_rate) * 1.5) + (specificity * 0.9) + (momentum * 0.6) + (source_score * 0.06)
        else:
            insight_score = source_score

        opportunity_eligible = bool(
            total_posts >= 3
            and active_days >= 2
            and need_rate >= 25.0
            and competitor_rate <= 65.0
            and confidence >= confidence_threshold
        )

        topic_ranked.append(
            {
                "topic": topic_name,
                "category": stats.get("category") or "General",
                "total_posts": total_posts,
                "total_comments": total_comments,
                "channel_count": channel_count,
                "avg_views": round(avg_views, 2),
                "active_days": active_days,
                "ownership_rate": round(ownership_rate, 2),
                "need_rate": round(need_rate, 2),
                "competitor_rate": round(competitor_rate, 2),
                "competitor_posts": competitor_posts,
                "specificity": round(specificity, 2),
                "momentum": round(momentum, 2),
                "confidence": confidence,
                "insight_score": round(insight_score, 2),
                "opportunity_eligible": opportunity_eligible,
                "sentiments": stats.get("sentiments") or {},
                "last_seen": stats.get("last_seen"),
            }
        )

    threshold_relaxed = False
    topic_candidates = [topic for topic in topic_ranked if topic["confidence"] >= confidence_threshold]
    if not topic_candidates and topic_ranked:
        threshold_relaxed = True
        topic_candidates = sorted(topic_ranked, key=lambda item: (item["confidence"], item["insight_score"]), reverse=True)[: min(5, len(topic_ranked))]

    topic_candidates = sorted(topic_candidates, key=lambda item: (item["insight_score"], item["total_posts"]), reverse=True)[:topic_limit]
    topic_metrics = {item["topic"]: item for item in topic_candidates}
    selected_topics = set(topic_metrics.keys())

    nodes: dict[str, dict] = {}
    links: list[dict] = []

    channel_post_totals: dict[str, int] = {}
    for row in rows:
        if row["topic"] not in selected_topics:
            continue
        channel_id = f"channel:{row['channel_uuid']}"
        channel_post_totals[channel_id] = channel_post_totals.get(channel_id, 0) + _to_int(row.get("postMentions"), 0)

    for row in rows:
        topic_name = row["topic"]
        if topic_name not in selected_topics:
            continue

        topic_meta = topic_metrics[topic_name]
        channel_id = f"channel:{row['channel_uuid']}"
        topic_id = f"topic:{topic_name}"

        post_mentions = _to_int(row.get("postMentions"), 0)
        comment_mentions = _to_int(row.get("commentMentions"), 0)
        avg_views = _to_float(row.get("avgViews"), 0.0)
        members = _to_int(row.get("channel_members"), 0)
        channel_signal = channel_post_totals.get(channel_id, post_mentions)

        _add_node(
            nodes,
            {
                "id": channel_id,
                "name": _safe_name(row.get("channel_title"), _safe_name(row.get("channel_username"), "Channel")),
                "type": "channel",
                "val": max(14, min(34, 11 + channel_signal / 2.2)),
                "size": max(14, min(34, 11 + channel_signal / 2.2)),
                "signal": channel_signal,
                "details": row.get("channel_username"),
                "segmentType": "source_channel",
                "category": "Channel",
                "connections": channel_signal,
                "memberCount": members,
                "confidence": max(1, min(99, int(round(min(99.0, 18.0 + (channel_signal * 8.0)))))),
            },
        )

        topic_node = {
            "id": topic_id,
            "name": topic_name,
            "type": "topic",
            "val": max(9, min(28, 8 + topic_meta["total_posts"] / 2.5)),
            "size": max(9, min(28, 8 + topic_meta["total_posts"] / 2.5)),
            "signal": topic_meta["total_posts"],
            "category": topic_meta["category"],
            "connections": topic_meta["total_posts"],
            "commentMentions": topic_meta["total_comments"],
            "confidence": topic_meta["confidence"],
            "insightScore": topic_meta["insight_score"],
            "topicChannelCoverage": topic_meta["channel_count"],
            "topicBrandCoverage": topic_meta["channel_count"],
            "opportunityScore": topic_meta["insight_score"],
            "opportunityEvidenceCount": topic_meta["total_posts"],
            "opportunityActiveDays": topic_meta["active_days"],
            "opportunityNeedRate": topic_meta["need_rate"],
            "opportunityCompetitorRate": topic_meta["competitor_rate"],
            "opportunityOwnershipRate": topic_meta["ownership_rate"],
            "opportunityMomentum": topic_meta["momentum"],
            "opportunitySpecificity": topic_meta["specificity"],
            "opportunityEligible": topic_meta["opportunity_eligible"],
        }

        if insight_mode == "opportunities" and topic_meta["opportunity_eligible"]:
            topic_node["semanticRole"] = "opportunity"
            topic_node["opportunityTier"] = "gold" if topic_meta["insight_score"] >= 80 else "silver"

        _add_node(nodes, topic_node)

        links.append(
            {
                "source": channel_id,
                "target": topic_id,
                "value": post_mentions,
                "type": "DISCUSSES",
                "adVolume": post_mentions,
                "avgSentiment": 0,
                "sentimentLabel": "Mixed",
                "lastSeen": row.get("lastSeen"),
                "commentMentions": comment_mentions,
                "avgViews": round(avg_views, 2),
                "confidence": topic_meta["confidence"],
            }
        )

    selected_topic_names = list(selected_topics)

    if include_product_layer and selected_topic_names:
        for topic_name in selected_topic_names:
            topic_meta = topic_metrics[topic_name]
            product_label = _safe_name(topic_meta.get("category"), "General")
            product_id = f"product:{product_label}"
            weight = max(1, _to_int(topic_meta.get("total_posts"), 1))

            _add_node(
                nodes,
                {
                    "id": product_id,
                    "name": product_label,
                    "type": "product",
                    "val": max(7, min(18, 6 + weight / 6)),
                    "size": max(7, min(18, 6 + weight / 6)),
                    "signal": weight,
                    "category": "Product Context",
                },
            )

            links.append(
                {
                    "source": f"topic:{topic_name}",
                    "target": product_id,
                    "value": weight,
                    "type": "HAS_PRODUCT_CONTEXT",
                    "adVolume": weight,
                    "avgSentiment": 0,
                    "sentimentLabel": "Mixed",
                }
            )

    if include_audience_layer and selected_topic_names:
        audience_limit = {1: 240, 2: 180, 3: 140, 4: 90, 5: 60}[connection_strength]
        audience_rows = run_query(
            """
            MATCH (u:User)-[it:INTERESTED_IN]->(t:Topic)
            WHERE it.last_seen >= datetime($since)
              AND t.name IN $topics
            WITH t, coalesce(u.community_role, u.language, 'Community Members') AS audience, sum(coalesce(it.count, 1)) AS weight
            RETURN t.name AS topic, audience, weight
            ORDER BY weight DESC
            LIMIT $limit
            """,
            {
                "since": since.isoformat(),
                "topics": selected_topic_names,
                "limit": audience_limit,
            },
        )

        for row in audience_rows:
            topic_name = row.get("topic")
            if topic_name not in selected_topics:
                continue
            audience_name = _safe_name(row.get("audience"), "Community Members")
            audience_id = f"audience:{audience_name}"
            weight = max(1, _to_int(row.get("weight"), 1))

            _add_node(
                nodes,
                {
                    "id": audience_id,
                    "name": audience_name,
                    "type": "audience",
                    "val": max(7, min(18, 6 + weight / 9)),
                    "size": max(7, min(18, 6 + weight / 9)),
                    "signal": weight,
                    "category": "Audience",
                },
            )

            links.append(
                {
                    "source": f"topic:{topic_name}",
                    "target": audience_id,
                    "value": weight,
                    "type": "RESONATES_WITH",
                    "adVolume": weight,
                    "avgSentiment": 0,
                    "sentimentLabel": "Mixed",
                }
            )

    if include_intent_layer and selected_topic_names:
        intent_limit = {1: 220, 2: 180, 3: 130, 4: 90, 5: 70}[connection_strength]
        intent_rows = run_query(
            """
            MATCH (u:User)-[it:INTERESTED_IN]->(t:Topic)
            WHERE it.last_seen >= datetime($since)
              AND t.name IN $topics
            MATCH (u)-[ex:EXHIBITS]->(i:Intent)
            WHERE ex.last_seen >= datetime($since)
            WITH t, i, sum(coalesce(it.count, 1) * coalesce(ex.count, 1)) AS weight
            RETURN t.name AS topic,
                   i.name AS intent,
                   weight
            ORDER BY weight DESC
            LIMIT $limit
            """,
            {
                "since": since.isoformat(),
                "topics": selected_topic_names,
                "limit": intent_limit,
            },
        )

        for row in intent_rows:
            topic_name = row.get("topic")
            if topic_name not in selected_topics:
                continue
            intent_name = _safe_name(row.get("intent"), "Intent")
            intent_id = f"intent:{intent_name}"
            weight = max(1, _to_int(row.get("weight"), 1))

            _add_node(
                nodes,
                {
                    "id": intent_id,
                    "name": intent_name,
                    "type": "intent",
                    "val": max(7, min(18, 6 + weight / 10)),
                    "size": max(7, min(18, 6 + weight / 10)),
                    "signal": weight,
                    "category": "Intent",
                },
            )

            links.append(
                {
                    "source": f"topic:{topic_name}",
                    "target": intent_id,
                    "value": weight,
                    "type": "HAS_INTENT",
                    "adVolume": weight,
                    "avgSentiment": 0,
                    "sentimentLabel": "Mixed",
                }
            )

    if include_competitor_layer and selected_topic_names and channel_filters:
        competitor_limit = {1: 180, 2: 130, 3: 90, 4: 60, 5: 45}[connection_strength]
        competitor_rows = run_query(
            """
            MATCH (ch:Channel)<-[:IN_CHANNEL]-(p:Post)-[:TAGGED]->(t:Topic)
            WHERE p.posted_at >= datetime($since)
              AND t.name IN $topics
              AND NOT (
                  toLower(coalesce(ch.username, '')) IN $channels
                  OR toLower(coalesce(ch.title, '')) IN $channels
                  OR ch.uuid IN $channels
                  OR ('channel:' + ch.uuid) IN $channels
              )
            RETURN t.name AS topic,
                   coalesce(ch.title, ch.username) AS competitor,
                   count(DISTINCT p) AS posts
            ORDER BY posts DESC
            LIMIT $limit
            """,
            {
                "since": since.isoformat(),
                "topics": selected_topic_names,
                "channels": channel_filters,
                "limit": competitor_limit,
            },
        )

        for row in competitor_rows:
            topic_name = row.get("topic")
            if topic_name not in selected_topics:
                continue
            competitor_name = _safe_name(row.get("competitor"), "Competitor")
            competitor_id = f"competitor:{competitor_name}"
            weight = max(1, _to_int(row.get("posts"), 1))

            _add_node(
                nodes,
                {
                    "id": competitor_id,
                    "name": competitor_name,
                    "type": "competitor",
                    "val": max(7, min(20, 6 + weight / 2.5)),
                    "size": max(7, min(20, 6 + weight / 2.5)),
                    "signal": weight,
                    "category": "Competitor",
                },
            )

            links.append(
                {
                    "source": f"topic:{topic_name}",
                    "target": competitor_id,
                    "value": weight,
                    "type": "COMPETES_ON",
                    "adVolume": weight,
                    "avgSentiment": 0,
                    "sentimentLabel": "Mixed",
                }
            )

    if (include_painpoint_layer or include_valueprop_layer) and selected_topic_names:
        diagnostics_limit = {1: 34, 2: 28, 3: 22, 4: 16, 5: 12}[connection_strength]
        painpoint_candidates: list[tuple[str, int]] = []
        valueprop_candidates: list[tuple[str, int]] = []

        for topic_name in selected_topic_names:
            topic_meta = topic_metrics[topic_name]
            sentiments = topic_meta.get("sentiments") or {}
            negative_weight = sum(weight for label, weight in sentiments.items() if label in negative_sentiments)
            positive_weight = sum(weight for label, weight in sentiments.items() if label in positive_sentiments)

            if include_painpoint_layer and negative_weight > 0:
                painpoint_candidates.append((topic_name, int(negative_weight)))
            if include_valueprop_layer and positive_weight > 0:
                valueprop_candidates.append((topic_name, int(positive_weight)))

        for topic_name, weight in sorted(painpoint_candidates, key=lambda item: item[1], reverse=True)[:diagnostics_limit]:
            painpoint_id = f"painpoint:{topic_name}"
            _add_node(
                nodes,
                {
                    "id": painpoint_id,
                    "name": f"{topic_name} Friction",
                    "type": "painpoint",
                    "val": max(7, min(18, 6 + weight / 12)),
                    "size": max(7, min(18, 6 + weight / 12)),
                    "signal": weight,
                    "category": "Pain Point",
                },
            )
            links.append(
                {
                    "source": f"topic:{topic_name}",
                    "target": painpoint_id,
                    "value": max(1, weight),
                    "type": "HAS_PAINPOINT",
                    "adVolume": max(1, weight),
                    "avgSentiment": 0,
                    "sentimentLabel": "Negative",
                }
            )

        for topic_name, weight in sorted(valueprop_candidates, key=lambda item: item[1], reverse=True)[:diagnostics_limit]:
            valueprop_id = f"valueprop:{topic_name}"
            _add_node(
                nodes,
                {
                    "id": valueprop_id,
                    "name": f"{topic_name} Value",
                    "type": "valueprop",
                    "val": max(7, min(18, 6 + weight / 12)),
                    "size": max(7, min(18, 6 + weight / 12)),
                    "signal": weight,
                    "category": "Value Proposition",
                },
            )
            links.append(
                {
                    "source": f"topic:{topic_name}",
                    "target": valueprop_id,
                    "value": max(1, weight),
                    "type": "HAS_VALUE_PROP",
                    "adVolume": max(1, weight),
                    "avgSentiment": 0,
                    "sentimentLabel": "Positive",
                }
            )

    if include_sentiment_layer and sentiment_rows:
        sentiment_limit = {1: 160, 2: 120, 3: 90, 4: 60, 5: 45}[connection_strength]
        added = 0
        for row in sentiment_rows:
            topic_name = row.get("topic")
            if topic_name not in selected_topics:
                continue
            sentiment_label = _safe_name(row.get("sentiment"), "Unknown")
            if sentiment_filters and sentiment_label not in sentiment_filters:
                continue

            weight = max(1, _to_int(row.get("weight"), 1))
            sentiment_id = f"sentiment:{sentiment_label}"

            _add_node(
                nodes,
                {
                    "id": sentiment_id,
                    "name": sentiment_label,
                    "type": "sentiment",
                    "val": max(7, min(18, 6 + weight / 12)),
                    "size": max(7, min(18, 6 + weight / 12)),
                    "signal": weight,
                    "category": "Sentiment",
                    "connections": weight,
                },
            )

            links.append(
                {
                    "source": f"topic:{topic_name}",
                    "target": sentiment_id,
                    "value": weight,
                    "type": "HAS_SENTIMENT",
                    "adVolume": weight,
                    "avgSentiment": 0,
                    "sentimentLabel": sentiment_label,
                }
            )

            added += 1
            if added >= sentiment_limit:
                break

    return {
        "nodes": list(nodes.values()),
        "links": links,
        "meta": {
            "timeframe": filters.get("timeframe") or "Last 7 Days",
            "since": since.isoformat(),
            "insightMode": insight_mode,
            "sourceProfile": source_profile,
            "confidenceThreshold": confidence_threshold,
            "connectionStrength": connection_strength,
            "layers": sorted(layers),
            "selectedChannels": channel_filters,
            "topicCountConsidered": len(topic_ranked),
            "topicCountReturned": len(selected_topics),
            "thresholdRelaxed": threshold_relaxed,
            "generatedAt": datetime.now(timezone.utc).isoformat(),
        },
    }


def _build_topic_mesh_graph(filters: dict) -> dict:
    since = _parse_timeframe_to_since(filters.get("timeframe"))
    channel_filters = _normalize_channels(filters.get("channels") or filters.get("brandSource"))
    topic_filters = _normalize_topics(filters.get("topics"))

    edge_limit = max(20, min(_to_int(filters.get("max_edges"), 320), 1000))
    min_weight = max(1, _to_int(filters.get("min_weight"), 1))

    rows = run_query(
        """
        MATCH (t1:Topic)-[r:CO_OCCURS_WITH]-(t2:Topic)
        WHERE t1.name < t2.name
          AND r.last_seen >= datetime($since)
          AND r.count >= $min_weight
          AND ($topic_count = 0 OR t1.name IN $topics OR t2.name IN $topics)
          AND (
               $channel_count = 0 OR
               (
                 EXISTS {
                   MATCH (ch:Channel)-[:DISCUSSES]->(t1)
                    WHERE toLower(coalesce(ch.username, '')) IN $channels
                       OR toLower(coalesce(ch.title, '')) IN $channels
                       OR ch.uuid IN $channels
                       OR ('channel:' + ch.uuid) IN $channels
                  }
                  AND EXISTS {
                    MATCH (ch2:Channel)-[:DISCUSSES]->(t2)
                    WHERE toLower(coalesce(ch2.username, '')) IN $channels
                       OR toLower(coalesce(ch2.title, '')) IN $channels
                       OR ch2.uuid IN $channels
                       OR ('channel:' + ch2.uuid) IN $channels
                  }
                )
           )
        OPTIONAL MATCH (t1)-[:BELONGS_TO_CATEGORY]->(c1:TopicCategory)
        OPTIONAL MATCH (t2)-[:BELONGS_TO_CATEGORY]->(c2:TopicCategory)
        RETURN t1.name AS topic1,
               t2.name AS topic2,
               coalesce(c1.name, 'General') AS cat1,
               coalesce(c2.name, 'General') AS cat2,
               r.count AS weight,
               toString(r.last_seen) AS lastSeen
        ORDER BY weight DESC
        LIMIT $edge_limit
        """,
        {
            "since": since.isoformat(),
            "min_weight": min_weight,
            "edge_limit": edge_limit,
            "channels": channel_filters,
            "channel_count": len(channel_filters),
            "topics": topic_filters,
            "topic_count": len(topic_filters),
        },
    )

    nodes: dict[str, dict] = {}
    links: list[dict] = []

    for row in rows:
        t1_id = f"topic:{row['topic1']}"
        t2_id = f"topic:{row['topic2']}"
        weight = max(1, _to_int(row.get("weight"), 1))

        _add_node(
            nodes,
            {
                "id": t1_id,
                "name": row["topic1"],
                "type": "topic",
                "val": max(8, min(24, 8 + weight / 3)),
                "size": max(8, min(24, 8 + weight / 3)),
                "signal": weight,
                "category": row.get("cat1") or "General",
            },
        )

        _add_node(
            nodes,
            {
                "id": t2_id,
                "name": row["topic2"],
                "type": "topic",
                "val": max(8, min(24, 8 + weight / 3)),
                "size": max(8, min(24, 8 + weight / 3)),
                "signal": weight,
                "category": row.get("cat2") or "General",
            },
        )

        links.append(
            {
                "source": t1_id,
                "target": t2_id,
                "value": weight,
                "type": "CO_OCCURS_WITH",
                "adVolume": weight,
                "avgSentiment": 0,
                "sentimentLabel": "Mixed",
                "lastSeen": row.get("lastSeen"),
            }
        )

    return {"nodes": list(nodes.values()), "links": links}


def _build_voice_intel_graph(filters: dict) -> dict:
    since = _parse_timeframe_to_since(filters.get("timeframe"))
    topic_filters = _normalize_topics(filters.get("topics"))

    user_topic_limit = max(20, min(_to_int(filters.get("max_edges"), 260), 700))

    user_topic_rows = run_query(
        """
        MATCH (u:User)-[it:INTERESTED_IN]->(t:Topic)
        WHERE it.last_seen >= datetime($since)
          AND ($topic_count = 0 OR t.name IN $topics)
        RETURN u.telegram_user_id AS user_id,
               t.name AS topic,
               it.count AS weight,
               toString(it.last_seen) AS lastSeen,
               u.community_role AS role,
               u.communication_style AS style,
               u.language AS language
        ORDER BY weight DESC
        LIMIT $user_topic_limit
        """,
        {
            "since": since.isoformat(),
            "topics": topic_filters,
            "topic_count": len(topic_filters),
            "user_topic_limit": user_topic_limit,
        },
    )

    users = sorted({str(r["user_id"]) for r in user_topic_rows if r.get("user_id") is not None})
    intent_rows: list[dict] = []
    sentiment_rows: list[dict] = []

    if users:
        intent_rows = run_query(
            """
            MATCH (u:User)-[e:EXHIBITS]->(i:Intent)
            WHERE toString(u.telegram_user_id) IN $users
            RETURN toString(u.telegram_user_id) AS user_id,
                   i.name AS intent,
                   e.count AS weight,
                   toString(e.last_seen) AS lastSeen
            ORDER BY weight DESC
            LIMIT 500
            """,
            {"users": users},
        )

        sentiment_rows = run_query(
            """
            MATCH (u:User)-[hs:HAS_SENTIMENT]->(s:Sentiment)
            WHERE toString(u.telegram_user_id) IN $users
            RETURN toString(u.telegram_user_id) AS user_id,
                   s.label AS sentiment,
                   hs.count AS weight,
                   toString(hs.last_seen) AS lastSeen
            ORDER BY weight DESC
            LIMIT 500
            """,
            {"users": users},
        )

    nodes: dict[str, dict] = {}
    links: list[dict] = []

    for row in user_topic_rows:
        uid = str(row["user_id"])
        user_id = f"user:{uid}"
        topic_id = f"topic:{row['topic']}"
        weight = max(1, _to_int(row.get("weight"), 1))

        _add_node(
            nodes,
            {
                "id": user_id,
                "name": f"User {uid}",
                "type": "audience",
                "val": max(8, min(18, 7 + weight / 5)),
                "size": max(8, min(18, 7 + weight / 5)),
                "signal": weight,
                "category": "Community Voice",
                "details": row.get("role") or row.get("style") or row.get("language"),
            },
        )

        _add_node(
            nodes,
            {
                "id": topic_id,
                "name": row["topic"],
                "type": "topic",
                "val": max(8, min(22, 7 + weight / 4)),
                "size": max(8, min(22, 7 + weight / 4)),
                "signal": weight,
                "category": "Topic",
            },
        )

        links.append(
            {
                "source": user_id,
                "target": topic_id,
                "value": weight,
                "type": "INTERESTED_IN",
                "adVolume": weight,
                "avgSentiment": 0,
                "sentimentLabel": "Mixed",
                "lastSeen": row.get("lastSeen"),
            }
        )

    for row in intent_rows:
        user_id = f"user:{row['user_id']}"
        if user_id not in nodes:
            continue
        intent_id = f"intent:{row['intent']}"
        weight = max(1, _to_int(row.get("weight"), 1))

        _add_node(
            nodes,
            {
                "id": intent_id,
                "name": row["intent"],
                "type": "intent",
                "val": max(7, min(18, 6 + weight / 5)),
                "size": max(7, min(18, 6 + weight / 5)),
                "signal": weight,
                "category": "Intent",
            },
        )

        links.append(
            {
                "source": user_id,
                "target": intent_id,
                "value": weight,
                "type": "EXHIBITS",
                "adVolume": weight,
                "avgSentiment": 0,
                "sentimentLabel": "Mixed",
                "lastSeen": row.get("lastSeen"),
            }
        )

    for row in sentiment_rows:
        user_id = f"user:{row['user_id']}"
        if user_id not in nodes:
            continue
        sentiment_id = f"sentiment:{row['sentiment']}"
        weight = max(1, _to_int(row.get("weight"), 1))

        _add_node(
            nodes,
            {
                "id": sentiment_id,
                "name": row["sentiment"],
                "type": "sentiment",
                "val": max(7, min(16, 6 + weight / 6)),
                "size": max(7, min(16, 6 + weight / 6)),
                "signal": weight,
                "category": "Sentiment",
            },
        )

        links.append(
            {
                "source": user_id,
                "target": sentiment_id,
                "value": weight,
                "type": "HAS_SENTIMENT",
                "adVolume": weight,
                "avgSentiment": 0,
                "sentimentLabel": row.get("sentiment"),
                "lastSeen": row.get("lastSeen"),
            }
        )

    return {"nodes": list(nodes.values()), "links": links}


def get_graph_data(filters: dict | None = None) -> dict:
    payload = filters or {}
    mode = (payload.get("mode") or "").strip().lower()

    if mode == "topic_mesh":
        return _build_topic_mesh_graph(payload)
    if mode == "voice_intel":
        return _build_voice_intel_graph(payload)
    return _build_channel_topic_graph(payload)


def get_node_details(
    node_id: str,
    node_type: str,
    timeframe: str | None = None,
    channels: list[str] | None = None,
) -> dict | None:
    node_type_l = (node_type or "").strip().lower()
    since = _parse_timeframe_to_since(timeframe)
    channel_filters = _normalize_channels(channels)

    if node_type_l in {"brand", "channel"}:
        uuid = node_id.split(":", 1)[1] if node_id.startswith("channel:") else node_id
        row = run_single(
            """
            MATCH (ch:Channel {uuid: $uuid})
            OPTIONAL MATCH (ch)<-[:IN_CHANNEL]-(p:Post)
            WHERE p.posted_at >= datetime($since)
            OPTIONAL MATCH (p)-[:TAGGED]->(t:Topic)
            WITH ch, count(DISTINCT p) AS totalPosts,
                 sum(coalesce(p.comment_count,0)) AS totalComments,
                 round(avg(coalesce(p.views,0))) AS avgViews,
                 collect(DISTINCT t.name)[..8] AS topTopics
            RETURN ch.uuid AS id,
                   coalesce(ch.title, ch.username) AS name,
                   ch.username AS username,
                   ch.description AS description,
                   ch.member_count AS memberCount,
                   totalPosts,
                   totalComments,
                   avgViews,
                   topTopics
            """,
            {"uuid": uuid, "since": since.isoformat()},
        )
        if not row:
            return None

        top_topics = [topic for topic in (row.get("topTopics") or []) if topic]

        return {
            "id": f"channel:{row['id']}",
            "name": row["name"],
            "type": "channel",
            "insight": f"{row['name']} produced {row['totalPosts']} posts and {row['totalComments']} comments in the selected timeframe.",
            "recommendations": "Track this channel against top connected topics and watch changes in sentiment-linked topic edges.",
            "totalAds": _to_int(row.get("totalPosts"), 0),
            "totalMentions": _to_int(row.get("totalComments"), 0),
            "degree": len(top_topics),
            "channelCount": 1,
            "channels": [],
            "brands": [],
            "topics": [{"topic": topic, "adCount": 0} for topic in top_topics],
            "relatedTopics": [{"name": topic, "score": 0} for topic in top_topics],
            "description": row.get("description"),
            "memberCount": row.get("memberCount"),
            "username": row.get("username"),
        }

    if node_type_l == "topic":
        topic = node_id.split(":", 1)[1] if node_id.startswith("topic:") else node_id
        row = run_single(
            """
            MATCH (t:Topic {name: $topic})
            OPTIONAL MATCH (t)<-[:TAGGED]-(p:Post)-[:IN_CHANNEL]->(ch:Channel)
            WHERE p.posted_at >= datetime($since)
              AND (
                $channel_count = 0
                OR toLower(coalesce(ch.username, '')) IN $channels
                OR toLower(coalesce(ch.title, '')) IN $channels
                OR ch.uuid IN $channels
                OR ('channel:' + ch.uuid) IN $channels
              )
            OPTIONAL MATCH (t)-[:BELONGS_TO_CATEGORY]->(cat:TopicCategory)
            WITH t, cat,
                 count(DISTINCT p) AS totalPosts,
                 count(DISTINCT ch) AS channelCount,
                 [name IN collect(DISTINCT coalesce(ch.title, ch.username))[..8] WHERE name IS NOT NULL] AS channelNames
            OPTIONAL MATCH (t)-[r:CO_OCCURS_WITH]-(rt:Topic)
            WHERE r.last_seen >= datetime($since)
            WITH t, cat, totalPosts, channelCount, channelNames,
                 collect(DISTINCT {name: rt.name, score: coalesce(r.count,0)})[..8] AS relatedTopics
            RETURN t.name AS name,
                   coalesce(cat.name, 'General') AS category,
                   totalPosts,
                   channelCount,
                   channelNames,
                   relatedTopics
            """,
            {
                "topic": topic,
                "since": since.isoformat(),
                "channels": channel_filters,
                "channel_count": len(channel_filters),
            },
        )
        if not row:
            return None

        row_channels = [{"name": name, "adCount": 0} for name in (row.get("channelNames") or []) if name]

        return {
            "id": f"topic:{row['name']}",
            "name": row["name"],
            "type": "topic",
            "insight": f"{row['name']} appears in {row['totalPosts']} posts across {row['channelCount']} channels.",
            "recommendations": "Compare this topic against co-occurring topics and sentiment-linked audience signals.",
            "totalAds": _to_int(row.get("totalPosts"), 0),
            "totalMentions": _to_int(row.get("totalPosts"), 0),
            "degree": len(row.get("relatedTopics") or []),
            "channelCount": _to_int(row.get("channelCount"), 0),
            "channels": row_channels,
            "relatedChannels": row_channels,
            "brandCount": _to_int(row.get("channelCount"), 0),
            "brands": row_channels,
            "relatedBrands": row_channels,
            "relatedTopics": row.get("relatedTopics") or [],
            "category": row.get("category"),
        }

    if node_type_l in {"intent", "sentiment", "audience"}:
        return {
            "id": node_id,
            "name": node_id.split(":", 1)[1] if ":" in node_id else node_id,
            "type": node_type_l,
            "insight": "This analytical node is aggregated from user-behavior edges in the selected timeframe.",
            "recommendations": "Use this node as an overlay to explain why topic/channel connections are strengthening.",
        }

    return None


def search_graph(query: str, limit: int = 20) -> list[dict]:
    q = (query or "").strip().lower()
    if not q:
        return []

    lim = max(1, min(limit, 100))

    rows = run_query(
        """
        CALL {
          MATCH (ch:Channel)
          WHERE toLower(coalesce(ch.title, '')) CONTAINS $q
             OR toLower(coalesce(ch.username, '')) CONTAINS $q
          RETURN 'channel' AS type,
                 'channel:' + ch.uuid AS id,
                 coalesce(ch.title, ch.username) AS name,
                 ch.username AS text,
                 3 AS rank
          UNION ALL
          MATCH (t:Topic)
          WHERE toLower(t.name) CONTAINS $q
          RETURN 'topic' AS type,
                 'topic:' + t.name AS id,
                 t.name AS name,
                 'Topic' AS text,
                 2 AS rank
          UNION ALL
          MATCH (i:Intent)
          WHERE toLower(i.name) CONTAINS $q
          RETURN 'intent' AS type,
                 'intent:' + i.name AS id,
                 i.name AS name,
                 'Intent' AS text,
                 1 AS rank
        }
        RETURN type, id, name, text
        ORDER BY rank DESC, name ASC
        LIMIT $limit
        """,
        {"q": q, "limit": lim},
    )

    return rows


def get_trending_topics(limit: int = 10, timeframe: str | None = None) -> list[dict]:
    since = _parse_timeframe_to_since(timeframe)
    lim = max(1, min(limit, 100))
    rows = run_query(
        """
        MATCH (p:Post)-[:TAGGED]->(t:Topic)
        WHERE p.posted_at >= datetime($since)
        RETURN t.name AS name,
               'topic:' + t.name AS id,
               count(p) AS adCount
        ORDER BY adCount DESC
        LIMIT $limit
        """,
        {"since": since.isoformat(), "limit": lim},
    )
    return rows


def get_top_channels(limit: int = 10, timeframe: str | None = None) -> list[dict]:
    since = _parse_timeframe_to_since(timeframe)
    lim = max(1, min(limit, 100))
    rows = run_query(
        """
        MATCH (ch:Channel)<-[:IN_CHANNEL]-(p:Post)
        WHERE p.posted_at >= datetime($since)
        RETURN 'channel:' + ch.uuid AS id,
               coalesce(ch.title, ch.username) AS name,
               count(p) AS adCount
        ORDER BY adCount DESC
        LIMIT $limit
        """,
        {"since": since.isoformat(), "limit": lim},
    )
    return rows


def get_all_channels() -> list[dict]:
    rows = run_query(
        """
        MATCH (ch:Channel)
        OPTIONAL MATCH (ch)<-[:IN_CHANNEL]-(p:Post)
        WITH ch, count(p) AS postCount
        RETURN 'channel:' + ch.uuid AS id,
               coalesce(ch.title, ch.username) AS name,
               postCount AS adCount
        ORDER BY adCount DESC, name ASC
        """
    )
    return rows


def get_sentiment_distribution(timeframe: str | None = None) -> list[dict]:
    since = _parse_timeframe_to_since(timeframe)
    rows = run_query(
        """
        CALL () {
            MATCH (p:Post)-[:HAS_SENTIMENT]->(s:Sentiment)
            WHERE p.posted_at >= datetime($since)
            RETURN s.label AS label, count(*) AS count
            UNION ALL
            MATCH (c:Comment)-[:HAS_SENTIMENT]->(s:Sentiment)
            WHERE c.posted_at >= datetime($since)
            RETURN s.label AS label, count(*) AS count
        }
        RETURN label,
               sum(count) AS count
        ORDER BY count DESC
        """,
        {"since": since.isoformat()},
    )
    return rows


def get_graph_insights(timeframe: str | None = None) -> dict:
    since = _parse_timeframe_to_since(timeframe)

    stats = run_single(
        """
        MATCH (p:Post)
        WHERE p.posted_at >= datetime($since)
        OPTIONAL MATCH (p)<-[:REPLIES_TO]-(c:Comment)
        OPTIONAL MATCH (p)-[:TAGGED]->(t:Topic)
        OPTIONAL MATCH (p)-[:IN_CHANNEL]->(ch:Channel)
        RETURN count(DISTINCT p) AS posts,
               count(c) AS comments,
               count(DISTINCT t) AS activeTopics,
               count(DISTINCT ch) AS activeChannels
        """,
        {"since": since.isoformat()},
    ) or {}

    top_topic = run_single(
        """
        MATCH (p:Post)-[:TAGGED]->(t:Topic)
        WHERE p.posted_at >= datetime($since)
        RETURN t.name AS topic, count(*) AS n
        ORDER BY n DESC
        LIMIT 1
        """,
        {"since": since.isoformat()},
    ) or {}

    top_channel = run_single(
        """
        MATCH (ch:Channel)<-[:IN_CHANNEL]-(p:Post)
        WHERE p.posted_at >= datetime($since)
        RETURN coalesce(ch.title, ch.username) AS channel, count(*) AS n
        ORDER BY n DESC
        LIMIT 1
        """,
        {"since": since.isoformat()},
    ) or {}

    insight = (
        f"In the selected window, {stats.get('activeChannels', 0)} channels generated "
        f"{stats.get('posts', 0)} posts and {stats.get('comments', 0)} comments. "
        f"Top active topic: {top_topic.get('topic', 'N/A')}. "
        f"Top publishing channel: {top_channel.get('channel', 'N/A')}."
    )

    return {
        "insight": insight,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "stats": stats,
        "topTopic": top_topic,
        "topChannel": top_channel,
    }
