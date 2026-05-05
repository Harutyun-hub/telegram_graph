"""Global analysis lens catalog and prompt helpers.

The lens catalog is code-versioned static data. Runtime admin config stores
only selected lens IDs.
"""

from __future__ import annotations

import hashlib
import json
from statistics import mean
from typing import Any

from loguru import logger


DEFAULT_ANALYSIS_LENS_IDS: tuple[str, ...] = ("finance_markets",)

SOCIAL_SYSTEM_PROMPT = """
You are an AI analyst extracting high-value, evidence-grounded signals from social media content.

Analyze each activity through the selected analysis lenses provided in the system message under ACTIVE_ANALYSIS_LENSES.

The selected lenses define:
- what the user cares about
- what counts as relevant
- what a high-quality topic should look like
- what should be ignored as noise

Use the lens rules as the relevance filter:
- Prefer insights that match one or more selected lenses.
- Do not force relevance if the evidence does not support it.
- If an activity is weakly relevant, keep a short factual summary but return empty topics.
- Topic names must follow the topic_quality_rules from the matched lens.
- Avoid generic labels unless the selected lens explicitly allows them.

Return structured JSON only. Never include markdown or prose outside JSON.

For each activity, extract:
- batch_index
- activity_uid
- summary
- lens_relevance: high, medium, low
- matched_lenses
- lens_signals
- topics: objects with name, evidence, confidence
- sentiment
- sentiment_score
- marketing_intent
- products
- audience_segments
- pain_points
- value_propositions
- competitive_signals
- customer_intent
- urgency_indicators
- marketing_tactic

Quality rules:
- Ground every insight in the provided evidence.
- Prefer specific, decision-useful signals over vague abstractions.
- Use the selected lens definitions to decide what is specific and useful.
- Every topic must include confidence as a float from 0.0 to 1.0.
- If a field is not relevant to the selected lens, return an empty array or null-like string.
- Do not invent facts, motivations, audiences, products, or causes.
""".strip()

LENS_DIRECTIVE = """
LENS DIRECTIVE

ACTIVE_ANALYSIS_LENSES are provided below as JSON. Use them only for relevance filtering, topic quality, evidence prioritization, summaries, and the optional fields lens_relevance, matched_lenses, and lens_signals.

Do not use the lenses to change, rename, remove, reinterpret, or extend any fixed schema fields or allowed enum values in this prompt.

Lens rules affect topics as follows:
- Emit topics only when the message or post is relevant to at least one selected lens.
- Prefer concrete, decision-useful topics that match the selected lens topic_quality_rules.
- Every topic object must include confidence as a float from 0.0 to 1.0.
- If evidence is weakly relevant, keep the existing fixed schema populated as usual but return empty topics.
- If no selected lens is relevant, return empty topics and set lens_relevance to low.
""".strip()

_LENS_CATALOG: tuple[dict[str, Any], ...] = (
    {
        "id": "finance_markets",
        "version": 1,
        "name": "Finance & Markets",
        "analyst_role": (
            "Market intelligence analyst for financial services, brokers, "
            "trading platforms, and investment research teams."
        ),
        "objective": (
            "Identify content that reveals market narratives, trading themes, "
            "investor education, product positioning, risk messaging, customer "
            "acquisition tactics, or competitor movement in financial markets."
        ),
        "relevance_definition": (
            "Relevant content helps explain how a tracked company discusses "
            "markets, educates traders, promotes financial products, reacts to "
            "macro events, or positions itself against competitors."
        ),
        "priority_signals": [
            "asset class or instrument focus",
            "market driver explanation",
            "volatility or risk narrative",
            "macro event interpretation",
            "central bank or rate commentary",
            "commodity, index, FX, crypto, or equity theme",
            "trading education or webinar funnel",
            "platform feature or tool promotion",
            "broker offer or account acquisition message",
            "regulatory, trust, or risk disclosure angle",
            "competitor positioning in financial products",
            "customer segment being targeted",
        ],
        "topic_quality_rules": {
            "prefer": [
                "specific market narrative",
                "specific instrument plus driver",
                "specific education or acquisition theme",
                "specific competitor or product positioning",
            ],
            "avoid_generic": [
                "finance",
                "investing",
                "trading",
                "stocks",
                "geopolitics",
                "market analysis",
                "technical analysis",
            ],
            "good_examples": [
                "beginner trader education funnel",
                "oil volatility from Middle East risk",
                "DAX pressure from tariff concerns",
                "broker webinar acquisition",
                "platform tools for market research",
            ],
        },
        "confidence_threshold": 0.70,
        "few_shot_examples": [
            {
                "input_excerpt": "Analysts expect oil to swing as Middle East tensions raise supply risk.",
                "bad_output_example": "geopolitics",
                "good_output_example": "oil volatility from Middle East risk",
                "reason": "Names the instrument and the market driver instead of a broad world-news category.",
            },
            {
                "input_excerpt": "DAX futures slipped as tariff headlines pressured European exporters.",
                "bad_output_example": "stocks",
                "good_output_example": "DAX pressure from tariff concerns",
                "reason": "Connects the specific index to the stated pressure point.",
            },
            {
                "input_excerpt": "Join our free webinar on risk management for new CFD traders this Thursday.",
                "bad_output_example": "trading",
                "good_output_example": "broker webinar acquisition",
                "reason": "Captures the education funnel and acquisition tactic.",
            },
            {
                "input_excerpt": "The platform now includes analyst calendars, market screeners, and watchlist tools.",
                "bad_output_example": "market analysis",
                "good_output_example": "platform tools for market research",
                "reason": "Focuses on the product capability and user job-to-be-done.",
            },
        ],
    },
    {
        "id": "competitor_analysis",
        "version": 1,
        "name": "Competitor Analysis",
        "analyst_role": (
            "Competitive intelligence analyst tracking how companies position, "
            "promote, differentiate, and compete."
        ),
        "objective": (
            "Identify signals that reveal competitor strategy, messaging, offers, "
            "product claims, audience targeting, channel usage, campaign mechanics, "
            "or positioning changes."
        ),
        "relevance_definition": (
            "Relevant content helps explain what a tracked company is trying to sell, "
            "who it is targeting, how it differentiates itself, and what strategic "
            "moves it is making."
        ),
        "priority_signals": [
            "new product or feature promotion",
            "pricing, discount, or offer message",
            "brand positioning claim",
            "audience segment targeted",
            "CTA or conversion tactic",
            "campaign theme",
            "partnership or sponsorship",
            "trust, credibility, or proof point",
            "comparison against alternatives",
            "regional market focus",
            "content format strategy",
            "repeated messaging pattern",
        ],
        "topic_quality_rules": {
            "prefer": [
                "specific positioning move",
                "specific campaign or offer",
                "specific audience strategy",
                "specific product claim",
            ],
            "avoid_generic": [
                "marketing",
                "business",
                "campaign",
                "brand",
                "competition",
                "promotion",
            ],
            "good_examples": [
                "zero-commission acquisition offer",
                "premium platform positioning",
                "beginner audience targeting",
                "trust-led broker messaging",
                "regional expansion campaign",
            ],
        },
        "confidence_threshold": 0.70,
        "few_shot_examples": [
            {
                "input_excerpt": "Open an account this week and trade US shares with zero commission.",
                "bad_output_example": "promotion",
                "good_output_example": "zero-commission acquisition offer",
                "reason": "Identifies the offer and conversion purpose.",
            },
            {
                "input_excerpt": "Our professional suite gives active traders institutional-grade charts.",
                "bad_output_example": "brand",
                "good_output_example": "premium platform positioning",
                "reason": "Captures the differentiation claim rather than a generic brand label.",
            },
            {
                "input_excerpt": "New to investing? Start with simple lessons and a demo account.",
                "bad_output_example": "marketing",
                "good_output_example": "beginner audience targeting",
                "reason": "Names the audience strategy directly supported by the copy.",
            },
            {
                "input_excerpt": "Regulated in multiple jurisdictions and trusted by over one million clients.",
                "bad_output_example": "competition",
                "good_output_example": "trust-led broker messaging",
                "reason": "Extracts the proof-point positioning signal.",
            },
        ],
    },
    {
        "id": "business_analysis",
        "version": 1,
        "name": "Business Analysis",
        "analyst_role": (
            "Business analyst focused on customer needs, value propositions, growth "
            "signals, operational themes, and market-facing strategy."
        ),
        "objective": (
            "Identify signals that explain customer problems, business opportunities, "
            "value propositions, demand patterns, product-market fit, growth levers, "
            "or operational risks."
        ),
        "relevance_definition": (
            "Relevant content helps a decision-maker understand what customers care "
            "about, what the company is emphasizing, what business opportunity exists, "
            "or what risk may affect performance."
        ),
        "priority_signals": [
            "customer pain point",
            "customer motivation",
            "value proposition",
            "purchase or adoption trigger",
            "retention or loyalty message",
            "trust or credibility signal",
            "service quality signal",
            "business opportunity",
            "market demand pattern",
            "operational risk",
            "education or enablement need",
            "customer objection or friction",
        ],
        "topic_quality_rules": {
            "prefer": [
                "specific customer need",
                "specific business opportunity",
                "specific value proposition",
                "specific operational or trust signal",
            ],
            "avoid_generic": [
                "business",
                "customers",
                "growth",
                "strategy",
                "opportunity",
                "service",
            ],
            "good_examples": [
                "trust barrier for new traders",
                "education need before conversion",
                "mobile-first customer acquisition",
                "low-fee value proposition",
                "customer confidence building",
            ],
        },
        "confidence_threshold": 0.70,
        "few_shot_examples": [
            {
                "input_excerpt": "People keep asking whether the broker is safe before they deposit.",
                "bad_output_example": "customers",
                "good_output_example": "trust barrier for new traders",
                "reason": "Names the customer friction and affected segment.",
            },
            {
                "input_excerpt": "Users want basic lessons before deciding to open a live account.",
                "bad_output_example": "growth",
                "good_output_example": "education need before conversion",
                "reason": "Explains the enablement gap before adoption.",
            },
            {
                "input_excerpt": "Most signups now begin from the app after a one-tap KYC flow.",
                "bad_output_example": "strategy",
                "good_output_example": "mobile-first customer acquisition",
                "reason": "Turns the operational detail into a concrete growth signal.",
            },
            {
                "input_excerpt": "The campaign emphasizes lower spreads and fewer account fees.",
                "bad_output_example": "business",
                "good_output_example": "low-fee value proposition",
                "reason": "Captures the customer-facing value proposition.",
            },
        ],
    },
)

_CATALOG_BY_ID: dict[str, dict[str, Any]] = {lens["id"]: lens for lens in _LENS_CATALOG}


def get_analysis_lens_catalog() -> list[dict[str, Any]]:
    return [dict(lens) for lens in _LENS_CATALOG]


def normalize_analysis_lens_ids(value: Any, *, default: tuple[str, ...] = DEFAULT_ANALYSIS_LENS_IDS) -> list[str]:
    raw_ids = list(value) if isinstance(value, (list, tuple)) else list(default)
    selected: list[str] = []
    for item in raw_ids:
        lens_id = str(item or "").strip()
        if not lens_id:
            continue
        if lens_id not in _CATALOG_BY_ID:
            raise ValueError(f"Unknown analysis lens id: {lens_id}")
        if lens_id not in selected:
            selected.append(lens_id)
    if not selected:
        selected = list(default)
    if len(selected) > 3:
        raise ValueError("Select at most 3 analysis lenses")
    return selected


def resolve_analysis_lenses(selected_ids: Any | None = None) -> list[dict[str, Any]]:
    lens_ids = normalize_analysis_lens_ids(selected_ids)
    return [dict(_CATALOG_BY_ID[lens_id]) for lens_id in lens_ids]


def get_active_analysis_lens_ids() -> list[str]:
    from api.admin_runtime import get_admin_runtime_value

    value = get_admin_runtime_value("analysisLensIds", list(DEFAULT_ANALYSIS_LENS_IDS))
    try:
        return normalize_analysis_lens_ids(value)
    except ValueError as exc:
        logger.warning("Invalid analysisLensIds runtime config; using default lenses: {}", exc)
        return list(DEFAULT_ANALYSIS_LENS_IDS)


def get_active_analysis_lenses() -> list[dict[str, Any]]:
    return resolve_analysis_lenses(get_active_analysis_lens_ids())


def analysis_lens_signature(lenses_or_ids: Any | None = None) -> str:
    if lenses_or_ids is None:
        lenses = get_active_analysis_lenses()
    elif isinstance(lenses_or_ids, list) and all(isinstance(item, dict) for item in lenses_or_ids):
        lenses = [dict(item) for item in lenses_or_ids]
    else:
        lenses = resolve_analysis_lenses(lenses_or_ids)
    pairs = sorted((str(lens["id"]), int(lens.get("version") or 1)) for lens in lenses)
    digest = hashlib.sha256(json.dumps(pairs, separators=(",", ":"), ensure_ascii=True).encode("utf-8")).hexdigest()
    return digest[:16]


def render_active_lenses_block(lenses: list[dict[str, Any]] | None = None) -> str:
    active_lenses = lenses if lenses is not None else get_active_analysis_lenses()
    payload = {
        "analysis_lenses": active_lenses,
        "analysis_lens_ids": [str(lens["id"]) for lens in active_lenses],
        "analysis_lens_signature": analysis_lens_signature(active_lenses),
    }
    return "ACTIVE_ANALYSIS_LENSES\n```json\n" + json.dumps(payload, ensure_ascii=False, indent=2) + "\n```"


def build_lens_system_prompt(base_prompt: str, *, include_directive: bool = True, suffix: str | None = None) -> str:
    parts = [str(base_prompt or "").strip()]
    if include_directive:
        parts.append(LENS_DIRECTIVE)
    parts.append(render_active_lenses_block())
    if suffix:
        parts.append(str(suffix).strip())
    return "\n\n".join(part for part in parts if part)


def build_lens_prompt_template(base_prompt: str, *, include_directive: bool = True, suffix: str | None = None) -> str:
    """Render the stable prompt template shown in Admin without active lens JSON."""
    parts = [str(base_prompt or "").strip()]
    if include_directive:
        parts.append(LENS_DIRECTIVE)
    if suffix:
        parts.append(str(suffix).strip())
    return "\n\n".join(part for part in parts if part)


def active_analysis_lens_payload(*, include_lenses: bool = True) -> dict[str, Any]:
    lenses = get_active_analysis_lenses()
    payload: dict[str, Any] = {
        "analysis_lens_ids": [str(lens["id"]) for lens in lenses],
        "analysis_lens_signature": analysis_lens_signature(lenses),
    }
    if include_lenses:
        payload["analysis_lenses"] = lenses
    return payload


def _as_confidence(value: Any) -> float:
    try:
        confidence = float(value)
    except Exception:
        confidence = 0.0
    return max(0.0, min(1.0, confidence))


def normalize_lens_metadata(parsed: dict[str, Any], active_lenses: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    lenses = active_lenses if active_lenses is not None else get_active_analysis_lenses()
    active_ids = [str(lens["id"]) for lens in lenses]
    returned = parsed.get("matched_lenses") if isinstance(parsed, dict) else []
    matched: list[str] = []
    if isinstance(returned, list):
        for item in returned:
            lens_id = str(item or "").strip()
            if lens_id in active_ids and lens_id not in matched:
                matched.append(lens_id)

    signals: list[str] = []
    raw_signals = parsed.get("lens_signals") if isinstance(parsed, dict) else []
    if isinstance(raw_signals, list):
        for item in raw_signals:
            text = str(item or "").strip()
            if text and text not in signals:
                signals.append(text[:160])

    relevance = str(parsed.get("lens_relevance") or "").strip().lower() if isinstance(parsed, dict) else ""
    if relevance not in {"high", "medium", "low"}:
        relevance = "low" if not matched else "medium"

    return {
        "analysis_lens_ids": active_ids,
        "analysis_lens_signature": analysis_lens_signature(lenses),
        "lens_relevance": relevance,
        "matched_lenses": matched,
        "lens_signals": signals,
    }


def filter_topics_by_confidence(
    topics: list[Any],
    *,
    matched_lenses: list[str] | None = None,
    active_lenses: list[dict[str, Any]] | None = None,
    log_label: str = "analysis_lens",
) -> tuple[list[Any], str | None, dict[str, Any]]:
    lenses = active_lenses if active_lenses is not None else get_active_analysis_lenses()
    active_by_id = {str(lens["id"]): lens for lens in lenses}
    matched_ids = [lens_id for lens_id in (matched_lenses or []) if lens_id in active_by_id]
    threshold_lenses = [active_by_id[lens_id] for lens_id in matched_ids] or list(active_by_id.values())
    threshold = min(float(lens.get("confidence_threshold") or 0.70) for lens in threshold_lenses) if threshold_lenses else 0.70

    confidences = [
        _as_confidence(item.get("confidence") if isinstance(item, dict) else None)
        for item in topics
        if isinstance(item, dict)
    ]
    stats = {
        "count": len(confidences),
        "min": min(confidences) if confidences else None,
        "max": max(confidences) if confidences else None,
        "mean": mean(confidences) if confidences else None,
        "threshold": threshold,
    }
    logger.info(
        "{} topic confidence distribution | count={} min={} max={} mean={} threshold={}",
        log_label,
        stats["count"],
        round(stats["min"], 3) if stats["min"] is not None else None,
        round(stats["max"], 3) if stats["max"] is not None else None,
        round(stats["mean"], 3) if stats["mean"] is not None else None,
        round(threshold, 3),
    )

    if not topics:
        return [], None, stats

    kept: list[Any] = []
    for item in topics:
        if not isinstance(item, dict):
            kept.append(item)
            continue
        topic = dict(item)
        topic["confidence"] = _as_confidence(topic.get("confidence"))
        if topic["confidence"] >= threshold:
            kept.append(topic)

    if not kept:
        original: list[Any] = []
        for item in topics:
            if isinstance(item, dict):
                topic = dict(item)
                topic["confidence"] = _as_confidence(topic.get("confidence"))
                original.append(topic)
            else:
                original.append(item)
        return original, "low", stats

    return kept, "accepted", stats
