"""OpenClaw V3 bounded analyst probes.

This module intentionally exposes recipes, not raw SQL/Cypher execution.  The
OpenClaw bridge can ask for a deep analysis, but every database operation here
is backend-owned, parameterized, and bounded.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import math
import re
from typing import Any

from api.db import run_query
from api.queries import graph_dashboard


WINDOW_DAYS = {"24h": 1, "7d": 7, "30d": 30, "90d": 90}
TOKEN_RE = re.compile(r"[a-zA-Z0-9_]+")
QUOTED_PHRASE_RE = re.compile(r"['\"]([^'\"]{2,100})['\"]")
STOPWORDS = {
    "a", "an", "and", "are", "about", "at", "be", "by", "for", "from", "how", "in",
    "is", "it", "main", "of", "on", "or", "the", "to", "what", "which", "who", "why",
    "with", "driving", "drive", "issue", "issues", "problem", "problems", "trend", "trends",
    "now", "right", "week", "month", "today", "current", "currently", "happening",
}
QUESTION_ALIASES = {
    "permit": ["Residency permits", "Visa And Residency"],
    "permits": ["Residency permits", "Visa And Residency"],
    "residence permit": ["Residency permits", "Visa And Residency"],
    "residence permits": ["Residency permits", "Visa And Residency"],
    "residency": ["Residency permits", "Visa And Residency"],
    "visa": ["Visa And Residency", "Visa appointments"],
    "visas": ["Visa And Residency", "Visa appointments"],
    "appointment": ["Visa appointments", "Visa And Residency"],
    "appointments": ["Visa appointments", "Visa And Residency"],
    "paperwork": ["Residency permits", "Documents"],
    "document": ["Documents", "Residency permits"],
    "documents": ["Documents", "Residency permits"],
    "politics": ["Political Protest", "Political protests", "Politics"],
    "political": ["Political Protest", "Political protests", "Politics"],
    "rent": ["Rental costs", "Housing"],
    "rental": ["Rental costs", "Housing"],
    "housing": ["Rental costs", "Housing"],
}
NEGATIVE_SENTIMENTS = {"negative", "urgent", "sarcastic"}
POSITIVE_SENTIMENTS = {"positive"}
QUESTION_MARKERS = (
    "?",
    "how ",
    "why ",
    "where ",
    "when ",
    "what ",
    "can ",
    "как ",
    "почему ",
    "где ",
    "когда ",
    "что ",
    "можно ",
    "кто ",
)
ANSWER_MARKERS = (
    "спасибо",
    "ответ",
    "можно обратиться",
    "попробуйте",
    "нужно",
    "надо",
    "try ",
    "you can",
    "answer",
)


def _clean_text(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def _normalize_text(value: Any) -> str:
    return _clean_text(value).lower()


def _trim(value: Any, limit: int = 220) -> str:
    text = _clean_text(value)
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)].rstrip() + "..."


def _tokens(value: Any) -> list[str]:
    return [token for token in TOKEN_RE.findall(_normalize_text(value)) if token not in STOPWORDS]


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        text = _clean_text(value)
        key = _normalize_text(text)
        if not text or key in seen:
            continue
        seen.add(key)
        output.append(text)
    return output


def _analysis_window(window: str) -> dict[str, Any]:
    normalized = _normalize_text(window) or "7d"
    if normalized not in WINDOW_DAYS:
        normalized = "7d"
    days = WINDOW_DAYS[normalized]
    end_at = datetime.now(timezone.utc).replace(microsecond=0)
    start_at = end_at - timedelta(days=days)
    previous_start_at = start_at - timedelta(days=days)
    return {
        "window": normalized,
        "days": days,
        "start": start_at.isoformat(),
        "end": end_at.isoformat(),
        "previous_start": previous_start_at.isoformat(),
        "previous_end": start_at.isoformat(),
    }


def _candidate_terms(question: str) -> list[str]:
    terms: list[str] = []
    terms.extend(match.group(1) for match in QUOTED_PHRASE_RE.finditer(question or ""))
    normalized = _normalize_text(question)
    for alias, expansions in QUESTION_ALIASES.items():
        if re.search(rf"\b{re.escape(alias)}\b", normalized):
            terms.extend(expansions)
    toks = [token for token in _tokens(question) if len(token) >= 4]
    terms.extend(" ".join(toks[idx: idx + 2]) for idx in list(range(max(0, len(toks) - 1)))[:3])
    terms.extend(toks[:5])
    return _dedupe(terms)[:8]


def _resolve_topic(question: str, ctx: dict[str, Any]) -> dict[str, Any]:
    tried_terms = _candidate_terms(question)
    for term in tried_terms:
        try:
            matches = graph_dashboard.search_graph(term, limit=5)
        except Exception:
            matches = []
        for item in matches or []:
            if _normalize_text(item.get("type")) == "topic" and _clean_text(item.get("name")):
                return {
                    "scope": "topic",
                    "topic": _clean_text(item.get("name")),
                    "topic_id": _clean_text(item.get("id")),
                    "resolution": "search_topic",
                    "tried_terms": tried_terms,
                }

    broad = _top_surprising_topic(ctx)
    if broad.get("topic"):
        return {
            "scope": "topic",
            "topic": broad["topic"],
            "topic_id": f"topic:{broad['topic']}",
            "resolution": "broad_surprise_fallback",
            "tried_terms": tried_terms,
        }

    return {
        "scope": "unknown",
        "topic": None,
        "topic_id": None,
        "resolution": "unresolved",
        "tried_terms": tried_terms,
    }


def _range_params(ctx: dict[str, Any], *, current: bool = True) -> dict[str, Any]:
    if current:
        return {"start": ctx["start"], "end": ctx["end"]}
    return {"start": ctx["previous_start"], "end": ctx["previous_end"]}


def _topic_mentions(topic: str, ctx: dict[str, Any], *, current: bool) -> int:
    rows = run_query(
        """
        CALL {
            MATCH (p:Post)-[:TAGGED]->(t:Topic {name: $topic})
            WHERE p.posted_at >= datetime($start)
              AND p.posted_at < datetime($end)
              AND coalesce(p.entry_kind, 'broadcast_post') = 'broadcast_post'
            RETURN count(DISTINCT p) AS mentionCount

            UNION ALL

            MATCH (c:Comment)-[:TAGGED]->(t:Topic {name: $topic})
            WHERE c.posted_at >= datetime($start)
              AND c.posted_at < datetime($end)
            RETURN count(DISTINCT c) AS mentionCount
        }
        RETURN coalesce(sum(mentionCount), 0) AS mentionCount
        """,
        {"topic": topic, **_range_params(ctx, current=current)},
        op_name="agent.deep.topic_mentions",
    )
    return int((rows[0] if rows else {}).get("mentionCount") or 0)


def _surprise_score(topic: str, ctx: dict[str, Any]) -> dict[str, Any]:
    current = _topic_mentions(topic, ctx, current=True)
    previous = _topic_mentions(topic, ctx, current=False)
    expected = max(1, previous)
    lift = round(current / expected, 2)
    delta = current - previous
    score = round(math.log2((current + 1) / (expected + 1)), 2)
    if current < 3:
        label = "thin"
    elif lift >= 2.0 and delta >= 5:
        label = "high"
    elif lift >= 1.35 and delta >= 3:
        label = "medium"
    else:
        label = "low"
    return {
        "recipe": "surprise_score",
        "topic": topic,
        "current_mentions": current,
        "baseline_mentions": previous,
        "delta_mentions": delta,
        "lift": lift,
        "score": score,
        "label": label,
        "supported": label in {"high", "medium"},
    }


def _sentiment_distribution(topic: str, ctx: dict[str, Any], *, current: bool) -> dict[str, int]:
    rows = run_query(
        """
        CALL {
            MATCH (p:Post)-[:TAGGED]->(t:Topic {name: $topic})
            MATCH (p)-[:HAS_SENTIMENT]->(s:Sentiment)
            WHERE p.posted_at >= datetime($start)
              AND p.posted_at < datetime($end)
            RETURN toLower(coalesce(s.label, '')) AS label, count(DISTINCT p) AS count

            UNION ALL

            MATCH (c:Comment)-[:TAGGED]->(t:Topic {name: $topic})
            MATCH (c)-[:HAS_SENTIMENT]->(s:Sentiment)
            WHERE c.posted_at >= datetime($start)
              AND c.posted_at < datetime($end)
            RETURN toLower(coalesce(s.label, '')) AS label, count(DISTINCT c) AS count
        }
        RETURN label, coalesce(sum(count), 0) AS count
        ORDER BY count DESC
        """,
        {"topic": topic, **_range_params(ctx, current=current)},
        op_name="agent.deep.sentiment_distribution",
    )
    dist = {"positive": 0, "neutral": 0, "negative": 0}
    for row in rows:
        label = _normalize_text(row.get("label"))
        count = int(row.get("count") or 0)
        if label in POSITIVE_SENTIMENTS:
            dist["positive"] += count
        elif label in NEGATIVE_SENTIMENTS:
            dist["negative"] += count
        else:
            dist["neutral"] += count
    return dist


def _pct(part: int, total: int) -> float:
    return round((part / total) * 100.0, 1) if total else 0.0


def _sentiment_flip(topic: str, ctx: dict[str, Any]) -> dict[str, Any]:
    current = _sentiment_distribution(topic, ctx, current=True)
    previous = _sentiment_distribution(topic, ctx, current=False)
    current_total = sum(current.values())
    previous_total = sum(previous.values())
    current_negative = _pct(current["negative"], current_total)
    previous_negative = _pct(previous["negative"], previous_total)
    negative_delta = round(current_negative - previous_negative, 1)
    supported = current_total >= 5 and previous_total >= 5 and abs(negative_delta) >= 10.0
    direction = "more_negative" if negative_delta > 0 else "less_negative" if negative_delta < 0 else "stable"
    return {
        "recipe": "sentiment_flip",
        "current": current,
        "baseline": previous,
        "current_negative_pct": current_negative,
        "baseline_negative_pct": previous_negative,
        "negative_delta_pp": negative_delta,
        "direction": direction,
        "supported": supported,
    }


def _channel_concentration(topic: str, ctx: dict[str, Any]) -> dict[str, Any]:
    rows = run_query(
        """
        CALL {
            MATCH (p:Post)-[:TAGGED]->(t:Topic {name: $topic})
            MATCH (p)-[:IN_CHANNEL]->(ch:Channel)
            WHERE p.posted_at >= datetime($start)
              AND p.posted_at < datetime($end)
              AND coalesce(p.entry_kind, 'broadcast_post') = 'broadcast_post'
            RETURN coalesce(ch.title, ch.username, ch.uuid, 'unknown') AS channel,
                   count(DISTINCT p) AS mentions

            UNION ALL

            MATCH (c:Comment)-[:TAGGED]->(t:Topic {name: $topic})
            MATCH (c)-[:REPLIES_TO]->(:Post)-[:IN_CHANNEL]->(ch:Channel)
            WHERE c.posted_at >= datetime($start)
              AND c.posted_at < datetime($end)
            RETURN coalesce(ch.title, ch.username, ch.uuid, 'unknown') AS channel,
                   count(DISTINCT c) AS mentions
        }
        WITH channel, sum(mentions) AS mentions
        WHERE mentions > 0
        RETURN channel, mentions
        ORDER BY mentions DESC, channel ASC
        LIMIT 8
        """,
        {"topic": topic, **_range_params(ctx, current=True)},
        op_name="agent.deep.channel_concentration",
    )
    channels = [
        {"channel": _clean_text(row.get("channel")), "mentions": int(row.get("mentions") or 0)}
        for row in rows
    ]
    total = sum(row["mentions"] for row in channels)
    top = channels[0] if channels else {"channel": None, "mentions": 0}
    top_share = round(top["mentions"] / total, 3) if total else 0.0
    high_risk = total >= 5 and (top_share >= 0.70 or (len(channels) <= 2 and total >= 8))
    return {
        "recipe": "concentration_risk",
        "total_mentions": total,
        "distinct_channels": len(channels),
        "top_channel": top,
        "top_channel_share": top_share,
        "risk": "high" if high_risk else "medium" if top_share >= 0.5 and total >= 5 else "low",
        "supported": high_risk,
        "channels": channels[:5],
    }


def _counterfactual_channel_lift(
    topic: str,
    surprise: dict[str, Any],
    concentration: dict[str, Any],
) -> dict[str, Any]:
    total = int(concentration.get("total_mentions") or surprise.get("current_mentions") or 0)
    top = concentration.get("top_channel") if isinstance(concentration.get("top_channel"), dict) else {}
    top_mentions = int(top.get("mentions") or 0)
    without_top = max(0, total - top_mentions)
    baseline = max(1, int(surprise.get("baseline_mentions") or 0))
    lift_without_top = round(without_top / baseline, 2)
    survives = without_top >= 5 and lift_without_top >= 1.35 and without_top >= baseline + 3
    return {
        "recipe": "counterfactual_channel_lift",
        "topic": topic,
        "top_channel": top,
        "current_mentions": total,
        "mentions_without_top_channel": without_top,
        "baseline_mentions": int(surprise.get("baseline_mentions") or 0),
        "lift_without_top_channel": lift_without_top,
        "survives_top_channel_removal": survives,
        "supported": survives,
    }


def _demand_supply_mismatch(topic: str, ctx: dict[str, Any]) -> dict[str, Any]:
    question_markers = list(QUESTION_MARKERS)
    answer_markers = list(ANSWER_MARKERS)
    rows = run_query(
        """
        CALL {
            MATCH (c:Comment)-[:TAGGED]->(t:Topic {name: $topic})
            WHERE c.posted_at >= datetime($start)
              AND c.posted_at < datetime($end)
            WITH c,
                 toLower(coalesce(c.text, '')) AS text,
                 $question_markers AS questionMarkers,
                 $answer_markers AS answerMarkers
            RETURN count(c) AS totalComments,
                   sum(CASE WHEN any(marker IN questionMarkers WHERE text CONTAINS marker) THEN 1 ELSE 0 END) AS questionCount,
                   sum(CASE WHEN any(marker IN answerMarkers WHERE text CONTAINS marker) THEN 1 ELSE 0 END) AS answerProxyCount
        }
        CALL {
            MATCH (c:Comment)-[:TAGGED]->(t:Topic {name: $topic})
            WHERE c.posted_at >= datetime($start)
              AND c.posted_at < datetime($end)
            WITH coalesce(c.telegram_user_id, '') AS userId,
                 toLower(coalesce(c.text, '')) AS text,
                 $question_markers AS questionMarkers
            WITH userId,
                 sum(CASE WHEN any(marker IN questionMarkers WHERE text CONTAINS marker) THEN 1 ELSE 0 END) AS userQuestionCount
            WHERE userId <> '' AND userQuestionCount >= 2
            RETURN count(userId) AS repeatQuestionUsers
        }
        RETURN totalComments,
               questionCount,
               answerProxyCount,
               repeatQuestionUsers
        """,
        {
            "topic": topic,
            **_range_params(ctx, current=True),
            "question_markers": question_markers,
            "answer_markers": answer_markers,
        },
        op_name="agent.deep.demand_supply_mismatch",
    )
    row = rows[0] if rows else {}
    total = int(row.get("totalComments") or 0)
    questions = int(row.get("questionCount") or 0)
    answer_proxy = int(row.get("answerProxyCount") or 0)
    repeat_users = int(row.get("repeatQuestionUsers") or 0)
    question_share = round(questions / total, 3) if total else 0.0
    answer_gap = questions - answer_proxy
    supported = questions >= 5 and question_share >= 0.35 and answer_gap >= 3
    return {
        "recipe": "demand_supply_mismatch",
        "total_comments": total,
        "question_count": questions,
        "question_share": question_share,
        "answer_proxy_count": answer_proxy,
        "answer_gap": answer_gap,
        "repeat_question_users": repeat_users,
        "supported": supported,
        "caveat": "Answer coverage is inferred from lightweight text markers, not a full answer classifier.",
    }


def _evidence_trace(topic: str, ctx: dict[str, Any], *, limit: int) -> list[dict[str, Any]]:
    rows = run_query(
        """
        CALL {
            MATCH (p:Post)-[:TAGGED]->(t:Topic {name: $topic})
            MATCH (p)-[:IN_CHANNEL]->(ch:Channel)
            WHERE p.posted_at >= datetime($start)
              AND p.posted_at < datetime($end)
            RETURN coalesce(p.uuid, 'post:' + elementId(p)) AS id,
                   'post' AS type,
                   coalesce(ch.title, ch.username, 'unknown') AS channel,
                   coalesce(p.text, '') AS text,
                   p.posted_at AS timestamp,
                   coalesce(p.views, 0) AS reactions,
                   coalesce(p.comment_count, 0) AS replies

            UNION ALL

            MATCH (c:Comment)-[:TAGGED]->(t:Topic {name: $topic})
            MATCH (c)-[:REPLIES_TO]->(:Post)-[:IN_CHANNEL]->(ch:Channel)
            WHERE c.posted_at >= datetime($start)
              AND c.posted_at < datetime($end)
            RETURN coalesce(c.uuid, 'comment:' + elementId(c)) AS id,
                   'comment' AS type,
                   coalesce(ch.title, ch.username, 'unknown') AS channel,
                   coalesce(c.text, '') AS text,
                   c.posted_at AS timestamp,
                   0 AS reactions,
                   0 AS replies
        }
        RETURN id, type, channel, left(text, 220) AS text, toString(timestamp) AS timestamp, reactions, replies
        ORDER BY timestamp DESC
        LIMIT $limit
        """,
        {"topic": topic, **_range_params(ctx, current=True), "limit": max(1, min(int(limit), 8))},
        op_name="agent.deep.evidence_trace",
    )
    return [
        {
            "id": row.get("id"),
            "type": row.get("type"),
            "channel": row.get("channel"),
            "text": _trim(row.get("text"), 180),
            "timestamp": row.get("timestamp"),
            "reactions": int(row.get("reactions") or 0),
            "replies": int(row.get("replies") or 0),
        }
        for row in rows
    ]


def _top_surprising_topic(ctx: dict[str, Any]) -> dict[str, Any]:
    rows = run_query(
        """
        CALL {
            MATCH (p:Post)-[:TAGGED]->(t:Topic)-[:BELONGS_TO_CATEGORY]->(cat:TopicCategory)
            WHERE p.posted_at >= datetime($start)
              AND p.posted_at < datetime($end)
              AND coalesce(t.proposed, false) = false
              AND coalesce(cat.name, '') <> 'General'
            RETURN t.name AS topic, count(DISTINCT p) AS currentMentions

            UNION ALL

            MATCH (c:Comment)-[:TAGGED]->(t:Topic)-[:BELONGS_TO_CATEGORY]->(cat:TopicCategory)
            WHERE c.posted_at >= datetime($start)
              AND c.posted_at < datetime($end)
              AND coalesce(t.proposed, false) = false
              AND coalesce(cat.name, '') <> 'General'
            RETURN t.name AS topic, count(DISTINCT c) AS currentMentions
        }
        WITH topic, sum(currentMentions) AS currentMentions
        CALL {
            WITH topic
            MATCH (p:Post)-[:TAGGED]->(t:Topic {name: topic})
            WHERE p.posted_at >= datetime($previous_start)
              AND p.posted_at < datetime($previous_end)
            RETURN count(DISTINCT p) AS previousMentions

            UNION ALL

            WITH topic
            MATCH (c:Comment)-[:TAGGED]->(t:Topic {name: topic})
            WHERE c.posted_at >= datetime($previous_start)
              AND c.posted_at < datetime($previous_end)
            RETURN count(DISTINCT c) AS previousMentions
        }
        WITH topic, currentMentions, sum(previousMentions) AS previousMentions
        WHERE currentMentions >= 5
        RETURN topic,
               currentMentions,
               previousMentions,
               currentMentions - previousMentions AS deltaMentions
        ORDER BY deltaMentions DESC, currentMentions DESC, topic ASC
        LIMIT 1
        """,
        ctx,
        op_name="agent.deep.top_surprising_topic",
    )
    return rows[0] if rows else {}


def _finding_from_probe(probe: dict[str, Any]) -> str | None:
    recipe = probe.get("recipe")
    if recipe == "surprise_score":
        label = probe.get("label")
        if label in {"high", "medium"}:
            return (
                f"{probe.get('topic')} is above its baseline: {probe.get('current_mentions')} mentions "
                f"vs {probe.get('baseline_mentions')} previously ({probe.get('lift')}x lift)."
            )
        return (
            f"{probe.get('topic')} is not strongly above baseline: {probe.get('current_mentions')} mentions "
            f"vs {probe.get('baseline_mentions')} previously."
        )
    if recipe == "sentiment_flip":
        delta = float(probe.get("negative_delta_pp") or 0.0)
        if probe.get("supported"):
            direction = "more negative" if delta > 0 else "less negative"
            return f"Sentiment shifted {direction}: negative share moved {delta:+.1f} percentage points."
        return "No strong sentiment flip was detected against the baseline."
    if recipe == "concentration_risk":
        top = probe.get("top_channel") if isinstance(probe.get("top_channel"), dict) else {}
        if probe.get("risk") == "high":
            return (
                f"Concentration risk is high: {top.get('channel') or 'one channel'} carries "
                f"{round(float(probe.get('top_channel_share') or 0) * 100)}% of observed mentions."
            )
        return "The signal does not appear to collapse into a single-channel artifact."
    if recipe == "counterfactual_channel_lift":
        without_top = int(probe.get("mentions_without_top_channel") or 0)
        lift = probe.get("lift_without_top_channel")
        top = probe.get("top_channel") if isinstance(probe.get("top_channel"), dict) else {}
        if probe.get("supported"):
            return (
                f"The trend survives without {top.get('channel') or 'the top channel'}: "
                f"{without_top} mentions remain ({lift}x baseline)."
            )
        return (
            f"Counterfactual check weakens the trend: without {top.get('channel') or 'the top channel'}, "
            f"only {without_top} mentions remain ({lift}x baseline)."
        )
    if recipe == "demand_supply_mismatch":
        if probe.get("supported"):
            return (
                f"Demand pressure is visible: {probe.get('question_count')} question-like comments "
                f"against {probe.get('answer_proxy_count')} answer-like replies."
            )
        return "Question pressure is not strong enough to call an unmet-demand signal."
    return None


def _tested_explanations(
    surprise: dict[str, Any],
    sentiment: dict[str, Any],
    concentration: dict[str, Any],
    counterfactual: dict[str, Any],
    demand: dict[str, Any],
) -> list[dict[str, Any]]:
    distributed_status = "rejected" if concentration.get("risk") == "high" else "supported"
    return [
        {
            "explanation": "The topic is genuinely above its recent baseline.",
            "status": "supported" if surprise.get("supported") else "rejected",
            "probe": "surprise_score",
        },
        {
            "explanation": "The story is mainly a sentiment shift.",
            "status": "supported" if sentiment.get("supported") else "rejected",
            "probe": "sentiment_flip",
        },
        {
            "explanation": "The trend is a single-channel amplification artifact.",
            "status": "supported" if concentration.get("supported") else "rejected",
            "probe": "concentration_risk",
        },
        {
            "explanation": "The signal is broadly distributed across independent channels.",
            "status": distributed_status,
            "probe": "concentration_risk",
        },
        {
            "explanation": "The trend survives after removing the top channel.",
            "status": "supported" if counterfactual.get("supported") else "rejected",
            "probe": "counterfactual_channel_lift",
        },
        {
            "explanation": "The discussion reflects unmet question demand.",
            "status": "supported" if demand.get("supported") else "rejected",
            "probe": "demand_supply_mismatch",
        },
    ]


def _confidence(
    supported_count: int,
    *,
    mentions: int,
    concentration: dict[str, Any],
    evidence_count: int,
    resolution: str,
) -> str:
    if mentions < 5 or evidence_count == 0 or resolution == "unresolved":
        return "low_confidence"
    if concentration.get("risk") == "high":
        return "medium" if supported_count >= 2 else "low_confidence"
    if supported_count >= 2:
        return "high"
    if supported_count == 1:
        return "medium"
    return "low_confidence"


def _summary(topic: str | None, findings: list[str], confidence: str) -> str:
    if not topic:
        return "The question did not resolve to a stable analysis target, so no deep claim was made."
    lead = findings[0] if findings else f"{topic} has limited current evidence."
    prefix = "The important signal" if confidence != "low_confidence" else "The cautious read"
    return f"{prefix}: {lead}"


def _telegram_text(summary: str, findings: list[str], caveats: list[str], confidence: str) -> str:
    lines = [_trim(summary, 220), f"Confidence: {confidence}."]
    for finding in findings[:3]:
        clean = _trim(finding, 170)
        if clean and clean != lines[0]:
            lines.append(f"- {clean}")
    if caveats:
        lines.append(f"Caveat: {_trim(caveats[0], 170)}")
    return _trim("\n".join(lines), 900)


def deep_analyze(question: str, *, window: str = "7d", mode: str = "quick") -> dict[str, Any]:
    clean_question = _clean_text(question)
    normalized_mode = _normalize_text(mode)
    if normalized_mode not in {"quick", "deep"}:
        normalized_mode = "quick"
    ctx = _analysis_window(window)
    resolution = _resolve_topic(clean_question, ctx)
    topic = resolution.get("topic")
    evidence_limit = 5 if normalized_mode == "deep" else 3

    if not topic:
        caveats = ["No exact topic, category, or channel resolved from the question."]
        return {
            "summary": "The question did not resolve to a stable analysis target, so no deep claim was made.",
            "surprise": {},
            "key_findings": [],
            "tested_explanations": [],
            "considered_and_rejected": [],
            "evidence": [],
            "confidence": "low_confidence",
            "caveats": caveats,
            "analysis_trace": {
                "mode": normalized_mode,
                "window": ctx["window"],
                "resolution": resolution,
                "recipes": [],
                "probe_count": 0,
            },
            "telegram_text": _telegram_text(
                "The question did not resolve to a stable analysis target, so no deep claim was made.",
                [],
                caveats,
                "low_confidence",
            ),
        }

    surprise = _surprise_score(topic, ctx)
    sentiment = _sentiment_flip(topic, ctx)
    concentration = _channel_concentration(topic, ctx)
    counterfactual = _counterfactual_channel_lift(topic, surprise, concentration)
    demand = _demand_supply_mismatch(topic, ctx)
    evidence = _evidence_trace(topic, ctx, limit=evidence_limit)
    probes = [surprise, sentiment, concentration, counterfactual, demand]
    findings = [finding for finding in (_finding_from_probe(probe) for probe in probes) if finding]
    supported_count = sum(1 for probe in [surprise, sentiment, concentration, demand] if probe.get("supported"))
    confidence = _confidence(
        supported_count,
        mentions=int(surprise.get("current_mentions") or 0),
        concentration=concentration,
        evidence_count=len(evidence),
        resolution=str(resolution.get("resolution") or ""),
    )
    tested = _tested_explanations(surprise, sentiment, concentration, counterfactual, demand)
    rejected = [item for item in tested if item.get("status") == "rejected"]
    caveats: list[str] = []
    if int(surprise.get("current_mentions") or 0) < 5:
        caveats.append("Small sample size: fewer than 5 mentions in the current window.")
    if concentration.get("risk") == "high":
        caveats.append("The signal is concentrated; it may not represent broad community movement.")
    if demand.get("caveat"):
        caveats.append(str(demand["caveat"]))
    if resolution.get("resolution") == "broad_surprise_fallback":
        caveats.append("The topic was selected from broad surprise scanning because the question was not specific.")

    summary = _summary(topic, findings, confidence)
    return {
        "summary": summary,
        "surprise": surprise,
        "key_findings": findings,
        "tested_explanations": tested,
        "considered_and_rejected": rejected,
        "evidence": evidence,
        "confidence": confidence,
        "caveats": caveats,
        "analysis_trace": {
            "mode": normalized_mode,
            "window": ctx["window"],
            "resolution": resolution,
            "recipes": [probe["recipe"] for probe in probes] + ["evidence_trace"],
            "probe_count": len(probes) + 1,
            "independent_supported_probes": supported_count,
        },
        "telegram_text": _telegram_text(summary, findings, caveats, confidence),
    }
