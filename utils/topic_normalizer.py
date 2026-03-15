from __future__ import annotations

"""Topic canonicalization and taxonomy lookups used before Neo4j writes."""

from difflib import get_close_matches
import re
from typing import Any

from utils.taxonomy import (
    TOPIC_ALIASES,
    build_topic_maps,
    canonical_category_name,
    canonical_domain_name,
    iter_topics,
)

TOPIC_CATEGORIES, TOPIC_DOMAINS = build_topic_maps()

# Lowercase variant -> canonical topic.
TOPIC_SYNONYMS: dict[str, str] = dict(TOPIC_ALIASES)
for _canonical_topic in iter_topics():
    TOPIC_SYNONYMS.setdefault(_canonical_topic.lower(), _canonical_topic)

DEFAULT_CATEGORY = "General"
DEFAULT_DOMAIN = "General"
MAX_TOPIC_WORDS = 4
MAX_MODEL_TOPICS = 6
_FUZZY_MATCH_CUTOFF = 0.88
_SEMANTIC_CATEGORY_MATCH_CUTOFF = 0.74

# Used when a canonical topic has no explicit category mapping.
_CATEGORY_KEYWORDS: list[tuple[str, str]] = [
    ("karabakh", "Nagorno-Karabakh & Artsakh"),
    ("artsakh", "Nagorno-Karabakh & Artsakh"),
    ("war", "Global Conflict"),
    ("military", "Military & Defense"),
    ("conflict", "Regional Security"),
    ("government", "Government & Leadership"),
    ("opposition", "Opposition & Protest"),
    ("protest", "Opposition & Protest"),
    ("election", "Democracy & Reform"),
    ("political", "Political Ideology"),
    ("parliament", "Government & Leadership"),
    ("corruption", "Democracy & Reform"),
    ("sanction", "Financial System"),
    ("bank", "Financial System"),
    ("payment", "Digital Services"),
    ("price", "Cost Of Living"),
    ("money", "Macroeconomic Condition"),
    ("business", "Business & Enterprise"),
    ("tax", "Financial System"),
    ("economic", "Macroeconomic Condition"),
    ("finance", "Financial System"),
    ("invest", "Business & Enterprise"),
    ("market", "Business & Enterprise"),
    ("inflation", "Macroeconomic Condition"),
    ("education", "Education"),
    ("school", "Education"),
    ("health", "Healthcare"),
    ("hospital", "Healthcare"),
    ("medicine", "Healthcare"),
    ("family", "Family & Relationships"),
    ("parenting", "Family & Relationships"),
    ("housing", "Housing & Infrastructure"),
    ("rent", "Housing & Infrastructure"),
    ("apartment", "Housing & Infrastructure"),
    ("real estate", "Housing & Infrastructure"),
    ("social service", "Social Services"),
    ("sport", "Arts & Entertainment"),
    ("football", "Arts & Entertainment"),
    ("culture", "National Identity"),
    ("humor", "Community Life"),
    ("religious", "Religion"),
    ("celebration", "Community Life"),
    ("community", "Community Life"),
    ("diaspora", "Diaspora"),
    ("migration", "Emigration"),
    ("expat", "Immigration To Armenia"),
    ("relocation", "Emigration"),
    ("homeland", "Diaspora"),
    ("media", "Media Landscape"),
    ("news", "Media Landscape"),
    ("propaganda", "Information Integrity"),
    ("claim", "Narrative & Frame"),
    ("telegram", "Media Landscape"),
    ("digital", "Digital Services"),
    ("privacy", "Digital Rights"),
    ("surveillance", "Digital Rights"),
    ("foreign policy", "Geopolitical Alignment"),
    ("foreign", "Geopolitical Alignment"),
    ("regional", "Regional Security"),
    ("tension", "Regional Security"),
    ("incident", "Regional Security"),
    ("ethnic", "National Identity"),
    ("identity", "National Identity"),
    ("historical", "National Identity"),
    ("history", "National Identity"),
    ("genocide", "National Identity"),
    ("social", "Community Life"),
    ("critique", "Community Life"),
    ("commentary", "Media Landscape"),
    ("policy", "Government & Leadership"),
    ("leadership", "Government & Leadership"),
    ("pashinyan", "Government & Leadership"),
    ("nikol", "Government & Leadership"),
    ("constitutional", "Democracy & Reform"),
    ("activism", "Community Life"),
    ("environment", "Housing & Infrastructure"),
    ("water", "Housing & Infrastructure"),
    ("donation", "Social Services"),
    ("aid", "Social Services"),
    ("humanitarian", "Social Services"),
    ("bot", "Information Integrity"),
    ("fake", "Information Integrity"),
    ("interethnic", "National Identity"),
    ("kurdish", "Regional Security"),
    ("iran", "Regional Security"),
    ("us", "Geopolitical Alignment"),
]

_CATEGORY_TO_DOMAIN: dict[str, str] = {}
for _topic, _category in TOPIC_CATEGORIES.items():
    _CATEGORY_TO_DOMAIN.setdefault(_category, TOPIC_DOMAINS.get(_topic, DEFAULT_DOMAIN))

_TRAILING_S = re.compile(r"(?<=[a-z]{3})s$", re.IGNORECASE)
_NON_WORD = re.compile(r"[^\w\s\-]+", re.UNICODE)
_MULTI_SPACE = re.compile(r"\s+")
_CANONICAL_TOPICS: tuple[str, ...] = tuple(iter_topics())
_CANONICAL_LOOKUP: dict[str, str] = {topic.lower(): topic for topic in _CANONICAL_TOPICS}
_CANONICAL_LOOKUP_KEYS: tuple[str, ...] = tuple(_CANONICAL_LOOKUP.keys())
_RUNTIME_TOPIC_ALIASES: dict[str, str] = {}

_GENERIC_TRAILING_TERMS: set[str] = {
    "issue",
    "issues",
    "discussion",
    "discussions",
    "discourse",
    "commentary",
    "engagement",
    "expression",
    "strategy",
    "performance",
    "policy",
    "violation",
    "threat",
    "tension",
    "study",
    "studie",
    "studies",
}


def set_runtime_topic_aliases(aliases: dict[str, str] | None) -> None:
    """Replace runtime alias map used for operator-approved topic promotions."""
    global _RUNTIME_TOPIC_ALIASES
    normalized: dict[str, str] = {}
    for alias, canonical in (aliases or {}).items():
        alias_key = str(alias or "").strip().lower()
        canonical_name = str(canonical or "").strip()
        if alias_key and canonical_name:
            normalized[alias_key] = canonical_name
    _RUNTIME_TOPIC_ALIASES = normalized


def runtime_topic_alias_count() -> int:
    return len(_RUNTIME_TOPIC_ALIASES)


def _infer_category_from_keywords(topic: str) -> str:
    lower = topic.lower()

    def _has_keyword(text: str, keyword: str) -> bool:
        if " " in keyword:
            return keyword in text
        return bool(re.search(rf"\b{re.escape(keyword)}\b", text))

    for keyword, category in _CATEGORY_KEYWORDS:
        if _has_keyword(lower, keyword):
            return canonical_category_name(category)
    return canonical_category_name(DEFAULT_CATEGORY)


def _sanitize_topic_text(raw: str) -> str:
    compact = _MULTI_SPACE.sub(" ", _NON_WORD.sub(" ", raw.strip()))
    return compact.strip()


def _normalize_title_topic(topic: str) -> str:
    title = topic.title()
    words = title.split()
    if words:
        words[-1] = _TRAILING_S.sub("", words[-1])
    if len(words) > MAX_TOPIC_WORDS:
        words = words[:MAX_TOPIC_WORDS]
    return " ".join(words)


def _closest_taxonomy_topic(candidate: str) -> str | None:
    matches = get_close_matches(candidate.lower(), _CANONICAL_LOOKUP_KEYS, n=1, cutoff=_FUZZY_MATCH_CUTOFF)
    if not matches:
        return None
    return _CANONICAL_LOOKUP.get(matches[0])


def _infer_category_domain_by_similarity(topic: str) -> tuple[str, str] | None:
    candidate = topic.lower().strip()
    if not candidate:
        return None
    matches = get_close_matches(
        candidate,
        _CANONICAL_LOOKUP_KEYS,
        n=1,
        cutoff=_SEMANTIC_CATEGORY_MATCH_CUTOFF,
    )
    if not matches:
        return None
    canonical_topic = _CANONICAL_LOOKUP.get(matches[0])
    if not canonical_topic:
        return None
    return get_topic_category(canonical_topic), get_topic_domain(canonical_topic)


def _strip_generic_suffix(topic: str) -> str:
    words = [word for word in topic.split() if word]
    while words and words[-1].lower() in _GENERIC_TRAILING_TERMS:
        words.pop()
    return " ".join(words).strip()


def _topic_candidates(raw: str) -> list[str]:
    candidates: list[str] = []

    def _append(value: str) -> None:
        text = _sanitize_topic_text(value)
        if not text:
            return
        if text.lower() in {item.lower() for item in candidates}:
            return
        candidates.append(text)

    _append(raw)
    _append(_normalize_title_topic(raw))
    stripped = _strip_generic_suffix(raw)
    if stripped:
        _append(stripped)
        _append(_normalize_title_topic(stripped))

    return candidates


def _lookup_canonical(candidate: str) -> str | None:
    key = candidate.lower()
    return (
        _RUNTIME_TOPIC_ALIASES.get(key)
        or TOPIC_SYNONYMS.get(key)
        or _CANONICAL_LOOKUP.get(key)
    )


def classify_topic(raw: str | None) -> dict[str, Any] | None:
    """Classify a topic as canonical taxonomy topic or proposed topic."""
    if raw is None:
        return None
    sanitized = _sanitize_topic_text(str(raw))
    if not sanitized:
        return None

    canonical = None
    candidates = _topic_candidates(sanitized)

    for candidate in candidates:
        canonical = _lookup_canonical(candidate)
        if canonical:
            break

    if not canonical:
        for candidate in candidates:
            canonical = _closest_taxonomy_topic(candidate)
            if canonical:
                break

    if canonical:
        category = get_topic_category(canonical)
        domain = get_topic_domain(canonical)
        return {
            "name": canonical,
            "taxonomy_topic": canonical,
            "proposed_topic": None,
            "proposed": False,
            "closest_category": category,
            "domain": domain,
        }

    proposed = _normalize_title_topic(sanitized)
    category = _infer_category_from_keywords(proposed)
    domain = _CATEGORY_TO_DOMAIN.get(category, DEFAULT_DOMAIN)
    if category == DEFAULT_CATEGORY or domain == DEFAULT_DOMAIN:
        inferred = _infer_category_domain_by_similarity(proposed)
        if inferred:
            category, domain = inferred
    return {
        "name": proposed,
        "taxonomy_topic": None,
        "proposed_topic": proposed,
        "proposed": True,
        "closest_category": category,
        "domain": domain,
    }


def _normalize_importance(value: Any) -> str:
    if not isinstance(value, str):
        return "secondary"
    normalized = value.strip().lower()
    if normalized in {"primary", "secondary", "tertiary"}:
        return normalized
    return "secondary"


def normalize_model_topics(raw_topics: list[Any]) -> list[dict[str, Any]]:
    """Normalize model topic objects to stable extraction contract."""
    if not isinstance(raw_topics, list):
        return []

    seen: set[str] = set()
    normalized: list[dict[str, Any]] = []

    for item in raw_topics:
        source_name = None
        importance = "secondary"
        evidence = None

        if isinstance(item, dict):
            source_name = item.get("taxonomy_topic") or item.get("name") or item.get("proposed_topic")
            importance = _normalize_importance(item.get("importance"))
            raw_evidence = item.get("evidence")
            if isinstance(raw_evidence, str):
                evidence = raw_evidence.strip()[:300] or None
        elif isinstance(item, str):
            source_name = item

        classified = classify_topic(source_name)
        if not classified:
            continue

        name = str(classified["name"])
        if name in seen:
            continue
        seen.add(name)

        entry = {
            **classified,
            "importance": importance,
            "evidence": evidence,
        }
        normalized.append(entry)

        if len(normalized) >= MAX_MODEL_TOPICS:
            break

    return normalized


def normalize_topic(raw: str) -> str:
    """Normalize a raw topic string to canonical graph node name."""
    classified = classify_topic(raw)
    if not classified:
        return DEFAULT_CATEGORY
    return str(classified["name"])


def normalize_topics(topics: list[str]) -> list[str]:
    """Normalize a list of topics, deduplicating after normalization."""
    seen: set[str] = set()
    result: list[str] = []
    for topic in topics:
        canonical = normalize_topic(topic)
        if canonical not in seen:
            seen.add(canonical)
            result.append(canonical)
    return result


def get_topic_category(canonical_topic: str) -> str:
    """Return topic category, falling back to keyword inference."""
    if canonical_topic in TOPIC_CATEGORIES:
        return canonical_category_name(TOPIC_CATEGORIES[canonical_topic])
    return _infer_category_from_keywords(canonical_topic)


def get_topic_domain(canonical_topic: str) -> str:
    """Return topic domain, inferring via category when needed."""
    if canonical_topic in TOPIC_DOMAINS:
        return canonical_domain_name(TOPIC_DOMAINS[canonical_topic])
    category = get_topic_category(canonical_topic)
    return canonical_domain_name(_CATEGORY_TO_DOMAIN.get(category, DEFAULT_DOMAIN))


def normalize_topic_category(category_name: str | None) -> str:
    """Normalize category labels (supports legacy aliases)."""
    return canonical_category_name(category_name)


def normalize_topic_domain(domain_name: str | None) -> str:
    """Normalize domain labels (supports legacy aliases)."""
    return canonical_domain_name(domain_name)
