from __future__ import annotations

import asyncio
import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol
from urllib.parse import urlparse, urlunparse

import config
from api.ai_helper import AIHelperError, OpenClawAiHelperProvider


PROMOTION_ANALYSIS_VERSION = "website-promotions-v1"


class WebsitePromotionResearchError(RuntimeError):
    """Raised when OpenClaw cannot return validated website promotion facts."""


class WebsitePromotionProvider(Protocol):
    async def chat(
        self,
        message: str,
        *,
        session_id: str | None = None,
        request_id: str | None = None,
    ) -> Any:
        ...


@dataclass(frozen=True)
class WebsitePromotionResearchResult:
    company_name: str
    website: str
    checked_at: str
    promotions: list[dict[str, Any]]
    raw_text: str
    raw_payload: dict[str, Any]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _trimmed(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _clean_optional(value: Any) -> str | None:
    text = _trimmed(value)
    return text or None


def _normalize_url(value: Any) -> str | None:
    raw = _trimmed(value)
    if not raw:
        return None
    if not re.match(r"^[a-z][a-z0-9+.-]*://", raw, flags=re.I):
        raw = f"https://{raw}"
    parsed = urlparse(raw)
    if parsed.scheme.lower() not in {"http", "https"}:
        return None
    host = (parsed.hostname or "").strip().lower()
    if not host or "." not in host:
        return None
    path = parsed.path or "/"
    normalized = parsed._replace(
        scheme=parsed.scheme.lower(),
        netloc=host,
        path=path,
        fragment="",
    )
    return urlunparse(normalized).rstrip("/")


def _host(value: str | None) -> str:
    host = (urlparse(value or "").hostname or "").lower()
    return host[4:] if host.startswith("www.") else host


def _same_domain(url: str, base_url: str) -> bool:
    candidate = _host(url)
    base = _host(base_url)
    return bool(candidate and base and (candidate == base or candidate.endswith(f".{base}")))


def _extract_json_object(text: str) -> dict[str, Any]:
    raw = _trimmed(text)
    if not raw:
        raise ValueError("OpenClaw returned an empty response")
    fenced = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", raw, flags=re.I | re.S)
    if fenced:
        raw = fenced.group(1).strip()
    else:
        start = raw.find("{")
        end = raw.rfind("}")
        if start >= 0 and end > start:
            raw = raw[start : end + 1]
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise ValueError("OpenClaw response must be a JSON object")
    return parsed


def _as_string_list(value: Any) -> list[str]:
    if isinstance(value, list):
        values = value
    elif value is None:
        values = []
    else:
        values = [value]
    out: list[str] = []
    for item in values:
        text = _trimmed(item)
        if text:
            out.append(text[:500])
    return out


def _clamp_confidence(value: Any) -> float:
    try:
        number = float(value)
    except Exception:
        return 0.0
    return max(0.0, min(1.0, number))


def _normalize_promotion(item: Any, *, website: str) -> dict[str, Any] | None:
    if not isinstance(item, dict):
        return None
    title = _trimmed(item.get("title") or item.get("name"))[:240]
    evidence = _trimmed(item.get("evidence_text") or item.get("evidence") or item.get("claim"))[:1200]
    source_url = _normalize_url(item.get("source_url") or item.get("url"))
    if not title or not evidence or not source_url or not _same_domain(source_url, website):
        return None
    conditions = item.get("conditions")
    if isinstance(conditions, list):
        conditions_value: str | list[str] | None = _as_string_list(conditions)
    else:
        conditions_value = _clean_optional(conditions)
    return {
        "title": title,
        "source_url": source_url,
        "evidence_text": evidence,
        "valid_from": _clean_optional(item.get("valid_from")),
        "valid_until": _clean_optional(item.get("valid_until")),
        "conditions": conditions_value,
        "detected_offer_type": _clean_optional(item.get("detected_offer_type") or item.get("offer_type")),
        "confidence": _clamp_confidence(item.get("confidence")),
    }


def _normalize_research_payload(payload: dict[str, Any], *, company_name: str, website: str, checked_at: str) -> dict[str, Any]:
    promotions_raw = payload.get("promotions")
    if promotions_raw is None and isinstance(payload.get("items"), list):
        promotions_raw = payload.get("items")
    if not isinstance(promotions_raw, list):
        raise ValueError("OpenClaw response must include a promotions array")
    max_items = max(1, int(config.SOCIAL_WEBSITE_PROMOTION_MAX_ITEMS))
    promotions: list[dict[str, Any]] = []
    for item in promotions_raw:
        normalized = _normalize_promotion(item, website=website)
        if normalized:
            promotions.append(normalized)
        if len(promotions) >= max_items:
            break
    return {
        "company_name": _trimmed(payload.get("company") or payload.get("company_name")) or company_name,
        "website": _normalize_url(payload.get("website")) or website,
        "checked_at": _clean_optional(payload.get("checked_at")) or checked_at,
        "promotions": promotions,
    }


def build_website_promotion_activity(
    *,
    entity: dict[str, Any],
    promotion: dict[str, Any],
    lifecycle_status: str,
    checked_at: str,
) -> dict[str, Any]:
    entity_id = _trimmed(entity.get("id"))
    title = _trimmed(promotion.get("title"))
    source_url = _normalize_url(promotion.get("source_url")) or _trimmed(promotion.get("source_url"))
    offer_type = _trimmed(promotion.get("detected_offer_type")).lower()
    fingerprint_seed = "|".join(
        [
            entity_id,
            source_url.lower().rstrip("/"),
            re.sub(r"\s+", " ", title.lower()),
            offer_type,
        ]
    )
    fingerprint = hashlib.sha1(fingerprint_seed.encode("utf-8")).hexdigest()[:24]
    valid_from = _clean_optional(promotion.get("valid_from"))
    valid_until = _clean_optional(promotion.get("valid_until"))
    conditions = promotion.get("conditions")
    condition_text = ", ".join(_as_string_list(conditions)) if isinstance(conditions, list) else _trimmed(conditions)
    lines = [
        f"Promotion: {title}",
        f"Evidence: {_trimmed(promotion.get('evidence_text'))}",
    ]
    if offer_type:
        lines.append(f"Offer type: {offer_type}")
    if valid_from:
        lines.append(f"Valid from: {valid_from}")
    if valid_until:
        lines.append(f"Valid until: {valid_until}")
    if condition_text:
        lines.append(f"Conditions: {condition_text}")
    return {
        "entity_id": entity_id,
        "account_id": None,
        "activity_uid": f"website:promotion:{entity_id}:{fingerprint}",
        "platform": "website",
        "source_kind": "ad",
        "provider_item_id": fingerprint,
        "source_url": source_url,
        "text_content": "\n".join(lines),
        "published_at": checked_at,
        "author_handle": _host(source_url),
        "cta_type": "promotion",
        "content_format": "website_promotion",
        "region_name": None,
        "engagement_metrics": {},
        "assets": [],
        "provider_payload": {
            "provider": "openclaw",
            "promotion": promotion,
            "website_monitor": {
                "status": lifecycle_status,
                "fingerprint": fingerprint,
                "checked_at": checked_at,
                "missed_scans": 0,
            },
        },
        "normalization_version": PROMOTION_ANALYSIS_VERSION,
        "analysis_version": config.SOCIAL_ANALYSIS_PROMPT_VERSION,
        "ingest_status": "normalized",
    }


def build_default_website_promotion_provider() -> OpenClawAiHelperProvider:
    transport = config.OPENCLAW_GATEWAY_TRANSPORT
    is_cli_bridge = str(transport or "").strip().lower() == "cli_bridge"
    return OpenClawAiHelperProvider(
        base_url=config.OPENCLAW_BRIDGE_BASE_URL if is_cli_bridge else config.OPENCLAW_GATEWAY_BASE_URL,
        gateway_token=config.OPENCLAW_BRIDGE_TOKEN if is_cli_bridge else config.OPENCLAW_GATEWAY_TOKEN,
        agent_id=config.OPENCLAW_BRIDGE_AGENT_ID if is_cli_bridge else config.OPENCLAW_ANALYTICS_AGENT_ID,
        session_key=config.OPENCLAW_WEBSITE_SESSION_KEY,
        timeout_seconds=config.OPENCLAW_WEBSITE_RESEARCH_TIMEOUT_SECONDS,
        connect_timeout_seconds=config.OPENCLAW_HELPER_CONNECT_TIMEOUT_SECONDS,
        read_timeout_seconds=config.OPENCLAW_WEBSITE_RESEARCH_TIMEOUT_SECONDS,
        retry_attempts=config.OPENCLAW_WEBSITE_RESEARCH_RETRY_ATTEMPTS,
        transport=transport,
        model=config.OPENCLAW_GATEWAY_MODEL,
        manage_transcript=False,
    )


class WebsitePromotionResearcher:
    def __init__(self, provider: WebsitePromotionProvider | None = None) -> None:
        self.provider = provider or build_default_website_promotion_provider()

    def research_sync(self, entity: dict[str, Any]) -> WebsitePromotionResearchResult:
        return asyncio.run(self.research(entity))

    async def research(self, entity: dict[str, Any]) -> WebsitePromotionResearchResult:
        company_name = _trimmed(entity.get("name")) or "Unknown Company"
        website = _normalize_url(entity.get("website"))
        if not website:
            raise WebsitePromotionResearchError("Company website is missing or invalid")
        checked_at = _utc_now_iso()
        prompt = self._build_prompt(company_name=company_name, website=website, checked_at=checked_at)
        request_id = f"website-promotion:{_trimmed(entity.get('id')) or _host(website)}"
        try:
            reply = await self.provider.chat(
                prompt,
                session_id=_trimmed(entity.get("id")) or _host(website),
                request_id=request_id,
            )
            raw_text = _trimmed(getattr(reply, "text", reply))
            payload = _extract_json_object(raw_text)
        except AIHelperError as exc:
            raise WebsitePromotionResearchError(f"OpenClaw unavailable: {exc.message}") from exc
        except Exception as exc:
            try:
                repair_text = await self._repair_response(
                    raw_text=locals().get("raw_text", ""),
                    company_name=company_name,
                    website=website,
                    checked_at=checked_at,
                    request_id=f"{request_id}:repair",
                )
                raw_text = repair_text
                payload = _extract_json_object(raw_text)
            except Exception as repair_exc:
                raise WebsitePromotionResearchError(f"OpenClaw returned invalid promotion JSON: {exc}") from repair_exc

        try:
            normalized = _normalize_research_payload(
                payload,
                company_name=company_name,
                website=website,
                checked_at=checked_at,
            )
        except Exception as exc:
            raise WebsitePromotionResearchError(str(exc)) from exc

        return WebsitePromotionResearchResult(
            company_name=normalized["company_name"],
            website=normalized["website"],
            checked_at=normalized["checked_at"],
            promotions=normalized["promotions"],
            raw_text=raw_text,
            raw_payload=payload,
        )

    async def _repair_response(
        self,
        *,
        raw_text: str,
        company_name: str,
        website: str,
        checked_at: str,
        request_id: str,
    ) -> str:
        repair_prompt = f"""
Convert the previous OpenClaw website research response into valid JSON only.

Required JSON shape:
{{
  "company": "{company_name}",
  "website": "{website}",
  "checked_at": "{checked_at}",
  "promotions": [
    {{
      "title": "string",
      "source_url": "https://same-domain-url",
      "evidence_text": "fact found on the page",
      "valid_from": null,
      "valid_until": null,
      "conditions": null,
      "detected_offer_type": "string",
      "confidence": 0.0
    }}
  ]
}}

Rules:
- Return JSON only. No markdown.
- Keep only promotions that include a same-domain source_url and evidence_text.
- If there are no valid promotions, return an empty promotions array.

Previous response:
{raw_text[:6000]}
""".strip()
        reply = await self.provider.chat(repair_prompt, session_id=_host(website), request_id=request_id)
        return _trimmed(getattr(reply, "text", reply))

    @staticmethod
    def _build_prompt(*, company_name: str, website: str, checked_at: str) -> str:
        return f"""
You are doing factual website research for a daily financial promotion monitor.

Task:
Research company "{company_name}" on website {website}.
Find only current promotions, campaigns, special offers, bonuses, limited-time deals, product offers, fee discounts, rewards, or client acquisition offers.

Where to look:
- Start with the homepage.
- Then visit only same-domain pages that are relevant to promotions, offers, cards, loans, deposits, tariffs, news, campaigns, landing pages, bonuses, rewards, or special offers.
- Do not browse competitor domains or unrelated external websites.
- Keep the crawl small and targeted.

Fact rules:
- Return only facts found on the website.
- Every promotion must include source_url and evidence_text.
- Do not guess dates, prices, rates, conditions, eligibility, or conclusions.
- If a date or condition is not found, use null.
- If nothing is found, return an empty promotions array.
- Do not include analysis, recommendations, or opinions.

Return JSON only, with this exact shape:
{{
  "company": "{company_name}",
  "website": "{website}",
  "checked_at": "{checked_at}",
  "promotions": [
    {{
      "title": "short factual title",
      "source_url": "https://same-domain-page",
      "evidence_text": "specific factual evidence from the page",
      "valid_from": null,
      "valid_until": null,
      "conditions": null,
      "detected_offer_type": "cashback|discount|bonus|rate|fee|loan|card|deposit|other",
      "confidence": 0.0
    }}
  ]
}}
""".strip()
