from __future__ import annotations

import json
import time
from typing import Any

from loguru import logger
from openai import OpenAI

import config
from utils.ai_usage import log_openai_usage
from utils.taxonomy import get_topic_role
from utils.topic_normalizer import normalize_model_topics


def _trimmed(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _truncate_text(value: Any, limit: int) -> str | None:
    text = _trimmed(value)
    if not text:
        return None
    if len(text) <= limit:
        return text
    return f"{text[:limit].rstrip()}..."


SYSTEM_PROMPT = """
You analyze public social-media threads for semantic graph storage. The domain may be politics,
public figures, business, services, finance, civic issues, campaigns, or community discussion.

Each input item is one parent post/ad with optional comments as audience evidence. Use the parent
item for context. Use comments to detect audience sentiment, questions, objections, support,
pain points, trust signals, and repeated issues. Do not treat comments as separate output items.

Return STRICT JSON only. No markdown. No prose.

Language and graph rules:
1. All labels, topics, entities, and descriptions must be in English.
2. evidence_quotes must preserve the original language exactly.
3. Ground every insight in the provided evidence only. Do not invent facts.
4. Topics must be canonical English, title case, specific, issue-level, singular where natural,
   deduplicated, max 4 words.
5. Avoid platform/source labels like Facebook Page, Media And News, Social Media Trend.
6. Keep arrays short: topics max 6, entities max 8, evidence_quotes max 3, pain_points max 5.
7. Use empty arrays, null, or "unknown" when a signal is absent.

Allowed values:
- primary_intent: Information Seeking | Opinion Sharing | Emotional Venting | Celebration |
  Debate / Argumentation | Coordination | Promotion / Advocacy | Support / Help |
  Humor / Sarcasm | Observation / Monitoring | Complaint | Praise | Call To Action
- sentiment: Positive | Negative | Neutral | Mixed | Urgent | Sarcastic
- social_sentiment_tags: Anxious | Frustrated | Angry | Confused | Hopeful | Trusting |
  Distrustful | Solidarity | Exhausted | Grief
- behavioral_pattern.community_role: Leader | Influencer | Engaged_Participant |
  Passive_Observer | Agitator | Helper | Troll | Lurker | Newcomer | Informant | unknown
- behavioral_pattern.communication_style: Formal | Informal | Aggressive | Passive |
  Analytical | Emotional | Persuasive | Ironic | unknown
- authority_attitude: Deferential | Critical | Dismissive | Fearful | Admiring |
  Humorous | Neutral | unknown
- trust_level: low | medium | high | hostile | unknown
- language: ru | hy | en | mixed | unknown

Return exactly one item per input activity_uid, same order:
{
  "items": [
    {
      "activity_uid": "<input activity_uid>",
      "primary_intent": "<allowed intent>",
      "marketing_intent": "<publisher/communication intent, for compatibility>",
      "marketing_tactic": "<communication tactic or null>",
      "summary": "<1-2 sentence grounded summary>",
      "evidence_quotes": ["<original language quote>"],
      "sentiment": "Positive|Negative|Neutral|Mixed|Urgent|Sarcastic",
      "sentiment_score": <-1.0 to 1.0>,
      "emotional_tone": "<precise emotion or unknown>",
      "social_sentiment_tags": ["<allowed tag>"],
      "topics": [{
        "name": "<Canonical English Topic>",
        "taxonomy_topic": "<canonical topic or null>",
        "proposed_topic": "<proposed topic or null>",
        "proposed": false,
        "closest_category": "<category or unknown>",
        "domain": "<domain or unknown>",
        "importance": "primary|secondary|tertiary",
        "evidence": "<short quote or grounded observation>"
      }],
      "message_topics": [{
        "message_ref": "<comment id or parent>",
        "comment_id": "<comment id or null>",
        "topics": [{"name": "<Canonical English Topic>", "importance": "primary|secondary|tertiary", "evidence": "<short evidence>"}]
      }],
      "message_sentiments": [{
        "message_ref": "<comment id or parent>",
        "comment_id": "<comment id or null>",
        "sentiment": "Positive|Negative|Neutral|Mixed|Urgent|Sarcastic",
        "sentiment_score": <-1.0 to 1.0>
      }],
      "entities": [{
        "name": "<Canonical English Name>",
        "type": "person|group|organization|place|concept|media|product|policy|program",
        "sentiment_toward": "positive|negative|neutral|ambiguous|fearful|admiring|mocking"
      }],
      "products": [{"name": "<product, service, policy, program, initiative, person, or offer>"}],
      "audience_segments": [{"name": "<audience group>"}],
      "pain_points": [{"name": "<concrete issue or complaint>"}],
      "value_propositions": [{"claim": "<promise, benefit, policy claim, or argument>"}],
      "competitive_signals": [{"name": "<competitor, opponent, alternative, or comparison target>", "domain": null}],
      "customer_intent": "<audience need, question, demand, or intent>",
      "urgency_indicators": ["<deadline, risk, escalation, repeated demand, or null>"],
      "behavioral_pattern": {"community_role": "<allowed role>", "communication_style": "<allowed style>"},
      "social_signals": {
        "authority_attitude": "<allowed attitude>",
        "institutional_trust": "<trust_level>",
        "collective_memory": "<historical/social reference or null>",
        "mobilization_signal": "Yes|No|Implied",
        "public_alignment": "<supportive|opposed|mixed|neutral|ambiguous|unknown>"
      },
      "demographics": {"language": "<language>", "inferred_gender": "male|female|unknown", "inferred_age_bracket": "13-17|18-24|25-34|35-44|45-54|55+|unknown"},
      "business_opportunity": {"opportunity_type": "Business_Idea|Investment_Interest|Job_Seeking|Hiring|Partnership_Request|Market_Gap_Observed|Service_Demand|Product_Demand|Real_Estate|Import_Export|none", "description": "<opportunity/economic signal or null>"},
      "psychographic": {"locus_of_control": "internal|external|mixed|unknown", "coping_style": "action_oriented|resigned|dark_humor|denial|seeking_support|unknown", "security_vs_freedom": "security|freedom|balanced|unknown"},
      "trust_landscape": {"trust_government": "<trust_level>", "trust_media": "<trust_level>", "trust_peers": "<trust_level>", "trust_foreign": "<trust_level>"},
      "linguistic_intelligence": {"code_switching": "high|medium|low|none", "certainty_level": "dogmatic|confident|uncertain|questioning|unknown", "rhetorical_strategy": "emotional|logical|anecdotal|authoritative|humorous|mixed|unknown", "pronoun_pattern": "individual|collective|mixed|unknown"},
      "financial_signals": {"financial_distress_level": "none|mild|moderate|severe|unknown", "price_sensitivity": "high|medium|low|unknown"}
    }
  ]
}
""".strip()


def _normalize_issue_topics(raw_topics: Any) -> list[str]:
    normalized_items = normalize_model_topics(raw_topics if isinstance(raw_topics, list) else [])
    topics: list[str] = []
    seen: set[str] = set()
    for item in normalized_items:
        name = _trimmed(item.get("name"))
        if not name or get_topic_role(name) != "issue":
            continue
        if name in seen:
            continue
        seen.add(name)
        topics.append(name)
    return topics


class SocialActivityAnalyzer:
    def __init__(self) -> None:
        if not config.OPENAI_API_KEY:
            raise RuntimeError("OPENAI_API_KEY is not configured")
        self.client = OpenAI(api_key=config.OPENAI_API_KEY)

    def analyze_batch(self, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not items:
            return []
        if len(items) > max(1, int(config.SOCIAL_ANALYSIS_BATCH_SIZE)):
            raise ValueError("Batch exceeds configured social analysis batch size")
        raw = self._request(self._build_batch_payload(items))
        parsed = self._parse_batch_response(raw)
        if len(parsed) != len(items):
            raise ValueError(f"Batch response length mismatch: expected {len(items)}, got {len(parsed)}")

        normalized: list[dict[str, Any]] = []
        for index, item in enumerate(items):
            result = parsed[index]
            batch_index = result.get("batch_index")
            activity_uid = _trimmed(result.get("activity_uid"))
            if batch_index is not None and batch_index != index:
                raise ValueError("Batch response did not preserve item ordering")
            if activity_uid != item["activity_uid"]:
                raise ValueError("Batch response did not preserve item ordering and identity")
            normalized.append(
                {
                    "activity_id": item["id"],
                    "entity_id": item["entity_id"],
                    "platform": item["platform"],
                    "activity_uid": item["activity_uid"],
                    "analysis_payload": self._normalize_result(result),
                    "raw_model_output": result,
                    "model": config.SOCIAL_ANALYSIS_MODEL,
                    "prompt_version": config.SOCIAL_ANALYSIS_PROMPT_VERSION,
                    "analysis_version": config.SOCIAL_ANALYSIS_PROMPT_VERSION,
                }
            )
        return normalized

    def analyze_one(self, item: dict[str, Any]) -> dict[str, Any]:
        return self.analyze_batch([item])[0]

    def _request(self, payload: dict[str, Any]) -> str:
        retry_limit = max(0, int(config.AI_REQUEST_MAX_RETRIES))
        backoff = max(0.0, float(config.AI_REQUEST_RETRY_BACKOFF_SECONDS))
        last_error: Exception | None = None
        for attempt in range(retry_limit + 1):
            try:
                request_started_at = time.perf_counter()
                item_count = max(1, len(payload.get("items") or []))
                max_completion_tokens = max(1800, 1200 * item_count)
                response = self.client.chat.completions.create(
                    model=config.SOCIAL_ANALYSIS_MODEL,
                    messages=[
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
                    ],
                    max_completion_tokens=max_completion_tokens,
                    timeout=config.AI_REQUEST_TIMEOUT_SECONDS,
                )
                log_openai_usage(
                    feature="social_analysis",
                    model=config.SOCIAL_ANALYSIS_MODEL,
                    response=response,
                    started_at=request_started_at,
                    extra={
                        "batch_size": len(payload.get("items") or []),
                        "thread_comments": sum(len(item.get("comments") or []) for item in payload.get("items") or []),
                        "max_completion_tokens": max_completion_tokens,
                    },
                )
                return _trimmed(response.choices[0].message.content)
            except Exception as exc:  # pragma: no cover - network/provider failures
                last_error = exc
                if attempt >= retry_limit:
                    break
                logger.warning("Social AI request failed on attempt {}: {}", attempt + 1, exc)
                time.sleep(backoff * (attempt + 1))
        raise RuntimeError(f"Social AI request failed: {last_error}")

    def _build_batch_payload(self, items: list[dict[str, Any]]) -> dict[str, Any]:
        platform = items[0].get("platform")
        entity = items[0].get("entity") or {}
        comment_limit = max(0, int(config.SOCIAL_THREAD_COMMENT_LIMIT))

        def _comment_payload(comment: dict[str, Any]) -> dict[str, Any]:
            return {
                "comment_id": comment.get("activity_uid") or comment.get("provider_item_id"),
                "provider_item_id": comment.get("provider_item_id"),
                "published_at": comment.get("published_at"),
                "author_handle": comment.get("author_handle"),
                "engagement_metrics": comment.get("engagement_metrics") or {},
                "text_content": _truncate_text(comment.get("text_content"), 500),
            }

        return {
            "contract": {
                "type": "ordered_thread_json_array",
                "required_length": len(items),
                "prompt_version": config.SOCIAL_ANALYSIS_PROMPT_VERSION,
                "comment_limit_per_item": comment_limit,
            },
            "entity": {
                "id": entity.get("id"),
                "name": entity.get("name"),
                "industry": entity.get("industry"),
            },
            "platform": platform,
            "items": [
                {
                    "batch_index": index,
                    "activity_uid": item["activity_uid"],
                    "source_kind": item.get("source_kind"),
                    "published_at": item.get("published_at"),
                    "source_url": item.get("source_url"),
                    "author_handle": item.get("author_handle"),
                    "cta_type": item.get("cta_type"),
                    "content_format": item.get("content_format"),
                    "region_name": item.get("region_name"),
                    "engagement_metrics": item.get("engagement_metrics") or {},
                    "text_content": _truncate_text(item.get("text_content"), 1200),
                    "comments": [
                        _comment_payload(comment)
                        for comment in (item.get("thread_comments") or [])[:comment_limit]
                    ],
                }
                for index, item in enumerate(items)
            ],
            "response_instructions": {
                "root_type": "object_with_items_array",
                "same_order_as_input": True,
                "include_activity_uid": True,
            },
        }

    @staticmethod
    def _parse_batch_response(raw: str) -> list[dict[str, Any]]:
        if not raw:
            raise ValueError("Empty AI response")
        text = raw.strip()
        start = text.find("[")
        end = text.rfind("]")
        parsed: Any
        if start != -1 and end != -1 and end > start:
            parsed = json.loads(text[start:end + 1])
        else:
            candidate = json.loads(text)
            if isinstance(candidate, dict) and isinstance(candidate.get("items"), list):
                parsed = candidate["items"]
            else:
                parsed = candidate
        if not isinstance(parsed, list):
            raise ValueError("AI batch response was not a JSON array")
        return [item for item in parsed if isinstance(item, dict)]

    @staticmethod
    def _normalize_result(result: dict[str, Any]) -> dict[str, Any]:
        def _as_list(key: str) -> list[Any]:
            value = result.get(key)
            return value if isinstance(value, list) else []

        normalized = {
            "batch_index": result.get("batch_index"),
            "activity_uid": result.get("activity_uid"),
            "summary": _trimmed(result.get("summary")),
            "primary_intent": _trimmed(result.get("primary_intent")),
            "marketing_intent": _trimmed(result.get("marketing_intent")) or _trimmed(result.get("primary_intent")),
            "evidence_quotes": _as_list("evidence_quotes"),
            "emotional_tone": _trimmed(result.get("emotional_tone")),
            "social_sentiment_tags": _as_list("social_sentiment_tags"),
            "products": _as_list("products"),
            "audience_segments": _as_list("audience_segments"),
            "pain_points": _as_list("pain_points"),
            "value_propositions": _as_list("value_propositions"),
            "competitive_signals": _as_list("competitive_signals"),
            "customer_intent": _trimmed(result.get("customer_intent")),
            "urgency_indicators": _as_list("urgency_indicators"),
            "topics": _normalize_issue_topics(result.get("topics")),
            "message_topics": _as_list("message_topics"),
            "message_sentiments": _as_list("message_sentiments"),
            "entities": _as_list("entities"),
            "sentiment": _trimmed(result.get("sentiment")) or "Neutral",
            "sentiment_score": SocialActivityAnalyzer._clamp_score(result.get("sentiment_score")),
            "marketing_tactic": _trimmed(result.get("marketing_tactic")),
            "behavioral_pattern": result.get("behavioral_pattern") if isinstance(result.get("behavioral_pattern"), dict) else {},
            "social_signals": result.get("social_signals") if isinstance(result.get("social_signals"), dict) else {},
            "demographics": result.get("demographics") if isinstance(result.get("demographics"), dict) else {},
            "business_opportunity": result.get("business_opportunity") if isinstance(result.get("business_opportunity"), dict) else {},
            "psychographic": result.get("psychographic") if isinstance(result.get("psychographic"), dict) else {},
            "trust_landscape": result.get("trust_landscape") if isinstance(result.get("trust_landscape"), dict) else {},
            "linguistic_intelligence": result.get("linguistic_intelligence") if isinstance(result.get("linguistic_intelligence"), dict) else {},
            "financial_signals": result.get("financial_signals") if isinstance(result.get("financial_signals"), dict) else {},
        }
        return normalized

    @staticmethod
    def _clamp_score(value: Any) -> float:
        try:
            score = float(value)
        except Exception:
            score = 0.0
        return max(-1.0, min(1.0, score))
