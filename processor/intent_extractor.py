"""
intent_extractor.py — Expert-grade behavioral intelligence extraction via the configured OpenAI model.

Expert Panel:
  1. Behavioral Intelligence Analyst     — psychological profile, desires, hidden signals
  2. Graph Database Architect (Neo4j)    — canonical English labels, dedup, clean graph nodes
  3. CIS/Caucasus Social Scientist       — sarcasm detection, collective memory, geopolitical alignment

Strategy:
  - Groups comments by (user_id, channel_id, post_id) for strict post isolation
  - Processes channel posts in strict micro-batches keyed by post_id
  - Returns 13-dimension structured JSON per user batch
  - Full output stored in raw_llm_response JSONB for flexibility
  - Standard columns (primary_intent, sentiment_score, topics, language) kept for Neo4j compat
"""
from __future__ import annotations
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from openai import OpenAI
from openai.types.chat import ChatCompletionMessageParam
from loguru import logger
from collections import defaultdict
import json
import time
import config
from utils.taxonomy import TAXONOMY_VERSION, compact_taxonomy_prompt
from utils.topic_normalizer import normalize_model_topics

client = OpenAI(api_key=config.OPENAI_API_KEY)

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

_DEFAULT_TAGS_BY_SENTIMENT = {
    "positive": ["Hopeful"],
    "negative": ["Frustrated"],
    "urgent": ["Anxious"],
    "sarcastic": ["Distrustful"],
}

_TONE_TO_TAGS = [
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

# ── System Prompt ─────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """### EXPERT PANEL & OBJECTIVE
You are THREE experts working together to analyze Telegram user messages:

**Expert 1 — Behavioral Intelligence Analyst**
Build a deep psychological and behavioral profile. Reveal what the user truly wants,
fears, believes, and signals — reading between the lines, not just the surface text.

**Expert 2 — Graph Database Architect (Neo4j)**
Ensure all extracted entities and topics are:
- In canonical English form (normalize: "Putin", "Vladimir Putin", "В.Путин" → "Vladimir Putin")
- Title Case, max 4 words, no duplicates across users
- Specific enough to be useful, not generic ("Armenian Opposition Politics" not "Politics")
- Consistent so the same concept always maps to the same graph node
- Singular nouns for topics ("Economic Crisis" not "Economic Crises")

**Expert 3 — CIS/Caucasus Social Scientist**
Apply deep understanding of Russian-Armenian cultural, political, and social context:
- Sarcasm and dark humor are DOMINANT in Russian Telegram — never misclassify as positive
- Understatement is typical in Armenian discourse — what seems mild may be intense
- Collective trauma is frequently referenced obliquely (wars, occupation, genocide, USSR collapse)
- Code-switching (Russian + Armenian mixed) signals identity and community belonging
- Geopolitical alignment shapes every political comment in this region
- Economic anxiety, migration intent, and diaspora identity are constant undercurrents

---

### LANGUAGE RULES (NON-NEGOTIABLE)
1. ALL taxonomy label values → ENGLISH ("Opinion Sharing", "Agitator", "Negative", etc.)
2. Topic names → ENGLISH, canonical, title case ("Armenian-Azerbaijani Conflict", not "Карабах")
3. Entity names → ENGLISH canonical form ("Nikol Pashinyan", not "Пашинян" or "Никол")
4. evidence_quotes → PRESERVE ORIGINAL LANGUAGE EXACTLY (Russian/Armenian verbatim)
5. Descriptions (events, desires, signals) → precise ENGLISH translation preserving full meaning
6. Russian sarcasm: flag in emotional_tone as "bitter sarcasm" / "dark humor"; sarcastic praise = NEGATIVE score
7. Armenian understatement: treat restrained criticism as potentially stronger than it appears
8. Do NOT guess — if signal is absent, use null or "unknown" rather than fabricate
9. USER PROFILE is provided when available — use first_name, last_name, username, bio as STRONG signals:
   - Russian/Armenian names → definitive language and cultural background
   - Male/female names → definitive gender (Эраст, Арам, Тигран = male; Анна, Мариам = female)
   - Username style (gaming handles, professional names) → age/personality hints
   - Bio → stated occupation, location, interests

---

### ANALYTICAL DIMENSIONS

#### 1. PRIMARY INTENT
Dominant goal behind user participation. Choose ONE:
Information Seeking | Opinion Sharing | Emotional Venting | Celebration |
Debate / Argumentation | Coordination | Promotion / Spam |
Support / Help | Humor / Sarcasm | Observation / Monitoring

#### 2. EVIDENCE QUOTES
1-3 exact verbatim quotes in ORIGINAL LANGUAGE proving the primary intent.

#### 3. SENTIMENT & EMOTION
- sentiment: Positive | Negative | Neutral | Mixed | Urgent | Sarcastic
- sentiment_score: float -1.0 to 1.0 (sarcastic praise = negative score)
- emotional_tone: precise label (e.g. "bitter sarcasm", "anxious", "indignant", "nostalgic", "defiant", "hopeful")
- social_sentiment_tags: zero to three from this controlled list only:
  Anxious | Frustrated | Angry | Confused | Hopeful | Trusting | Distrustful | Solidarity | Exhausted | Grief

#### 4. TOPICS
2-6 specific topics. Graph Architect rules: canonical English, title case, deduplicated.
Examples: "Military Recruitment", "Armenian Diaspora Identity", "Inflation And Prices",
"Nagorno-Karabakh Conflict", "Government Corruption", "Russian Propaganda", "Migration Intent",
"Post-Soviet Identity", "Political Prisoner", "Ethnic Tension", "Orthodox Christianity",
"Social Media Censorship", "Currency Devaluation", "Border Closure"

#### 5. DESIRES & NEEDS
- explicit: what user directly states they want
- implicit: inferred from tone and context
- underlying_need: security | belonging | status | knowledge | justice | autonomy | validation | safety | recognition

#### 6. HIDDEN SIGNALS & SUBTEXT
What is implied but NOT stated?
- Coded community language or insider references specific to Russian/Armenian Telegram
- What the user conspicuously avoids saying
- Implicit ideological or group allegiance signals
- Disguised anger, loyalty tests, or mobilization signals

#### 7. NEGATIVE EVENTS
Problems, complaints, fears, threats referenced by the user.

#### 8. POSITIVE EVENTS
Wins, endorsements, celebrations, hopeful references.

#### 9. ENTITIES
People, groups, organizations, places. Apply canonical English names.
sentiment_toward options: positive | negative | neutral | ambiguous | fearful | admiring | mocking

#### 10. BEHAVIORAL PATTERN
- community_role: Leader | Influencer | Engaged_Participant | Passive_Observer | Agitator | Helper | Troll | Lurker | Newcomer | Informant
- communication_style: Formal | Informal | Aggressive | Passive | Analytical | Emotional | Persuasive | Ironic
- engagement_depth: Deep | Moderate | Shallow
- urgency: boolean — does user express time-sensitive concerns?

#### 11. CIS/CAUCASUS SOCIAL SIGNALS
- geopolitical_alignment: Pro_Russia | Pro_West | Pro_Armenia | Pro_Azerbaijan | Nationalist | Anti_Government | Neutral | Ambiguous
- collective_memory: reference to historical events (Armenian Genocide, Karabakh Wars, USSR collapse, 2022 Ukraine invasion) or null
- in_out_group: describe who user identifies as "us" and who as "them", or null
- migration_intent: Yes | No | Implied — is user signaling desire/plan to leave the country?
- diaspora_signals: Yes | No — does user signal they live abroad or identify as diaspora?
- authority_attitude: Deferential | Critical | Dismissive | Fearful | Admiring | Humorous

#### 12. INFORMATION ECOSYSTEM
- media_references: media sources mentioned or clearly implied (Russian state TV, RFE/RL, local channels, etc.)
- conspiracy_signals: conspiracy theory adoption — describe if present, null if absent
- information_warfare: boolean — signs of coordinated messaging, bot-like repetition, or narrative push

#### 13. DEMOGRAPHICS
- language: ISO 639-1 code (ru | hy | en | mixed)
- inferred_gender: male | female | unknown
- inferred_age_bracket: 13-17 | 18-24 | 25-34 | 35-44 | 45-54 | 55+ | unknown
  IMPORTANT: Use USER PROFILE name as primary signal (Russian male/female names are definitive).
  Then infer from: vocabulary complexity, cultural references (soviet nostalgia → 35+, gaming slang → under 30),
  topic type (childcare/school → 28-45, retirement → 55+), writing style (emoji-heavy → younger).
  Use "unknown" ONLY if there is ZERO evidence — always attempt an inference with appropriate confidence level.
- confidence: high | medium | low

#### 14. DAILY LIFE & COMMUNITY NEEDS
Capture the civilian pulse — everyday life concerns that reveal social infrastructure quality and personal life stage.
- category: Education | Healthcare | Housing | Childcare | Employment | Transportation | Food | Legal | Religion | Leisure | Family | Relationships | Personal_Finance | none
- need_expressed: precise description of what the person is seeking or struggling with
  Examples:
  - "Looking for a private math tutor for a 12-year-old in Yerevan"
  - "Asking for recommendations for a good dentist who accepts cash"
  - "Complaining about school quality in their district"
  - "Seeking apartment rental advice in a specific neighborhood"
  - "Asking where to find affordable baby products"
- urgency: high | medium | low | none
- life_stage_signal: what life stage does this suggest? (Parent_School_Age_Child | Young_Professional | New_Parent | Elderly | Student | Job_Seeker | Homeowner | etc.)

#### 15. BUSINESS & ECONOMIC OPPORTUNITY SIGNALS
Capture signals of entrepreneurial activity, market observations, and economic opportunity awareness.
- opportunity_type: Business_Idea | Investment_Interest | Job_Seeking | Hiring | Partnership_Request | Market_Gap_Observed | Service_Demand | Product_Demand | Real_Estate | Import_Export | none
- description: what opportunity or economic signal is present
  Examples:
  - "Asking if anyone wants to partner on a small import business"
  - "Observing that there are no good Armenian restaurants in the area"
  - "Looking for investors for a tech startup"
  - "Posting a job offer for a driver or cleaner"
  - "Discussing potential in agricultural exports"
- market_context: local | regional | international | online
- urgency: high | medium | low | none

---

### OUTPUT SCHEMA (STRICT JSON — no markdown, no preamble, no explanation)
{
  "primary_intent": "<intent>",
  "intent_confidence": <0.0-1.0>,

  "evidence_quotes": ["<original language verbatim>", "<second quote if available>"],

  "sentiment": "Positive|Negative|Neutral|Mixed|Urgent|Sarcastic",
  "sentiment_score": <-1.0 to 1.0>,
  "emotional_tone": "<precise emotion label>",
  "social_sentiment_tags": ["Anxious|Frustrated|Angry|Confused|Hopeful|Trusting|Distrustful|Solidarity|Exhausted|Grief"],

  "topics": [
    {"name": "<Canonical English Topic>", "importance": "primary|secondary|tertiary", "evidence": "<quote or observation>"}
  ],

  "message_topics": [
    {
      "message_ref": "MSG 1",
      "comment_id": "<comment UUID if provided in input, otherwise null>",
      "topics": [
        {"name": "<Canonical English Topic>", "importance": "primary|secondary|tertiary", "evidence": "<quote or observation>"}
      ]
    }
  ],

  "desires": {
    "explicit": "<stated desire or null>",
    "implicit": "<inferred desire>",
    "underlying_need": "<human need>"
  },

  "hidden_signals": ["<subtext, implication, or coded signal>"],

  "negative_events": [
    {"description": "<English description>", "severity": "high|medium|low", "scope": "personal|local|national|global"}
  ],

  "positive_events": [
    {"description": "<English description>", "scope": "personal|local|national|global"}
  ],

  "entities": [
    {"name": "<Canonical English Name>", "type": "person|group|organization|place|concept|media", "sentiment_toward": "positive|negative|neutral|ambiguous|fearful|admiring|mocking"}
  ],

  "behavioral_pattern": {
    "community_role": "<role>",
    "communication_style": "<style>",
    "engagement_depth": "Deep|Moderate|Shallow",
    "urgency": false
  },

  "social_signals": {
    "geopolitical_alignment": "<alignment>",
    "collective_memory": "<historical reference or null>",
    "in_out_group": "<'us' vs 'them' framing or null>",
    "migration_intent": "Yes|No|Implied",
    "diaspora_signals": "Yes|No",
    "authority_attitude": "<attitude>"
  },

  "information_ecosystem": {
    "media_references": ["<source name or type>"],
    "conspiracy_signals": "<description or null>",
    "information_warfare": false
  },

  "demographics": {
    "language": "<ISO 639-1>",
    "inferred_gender": "male|female|unknown",
    "inferred_age_bracket": "<bracket>",
    "confidence": "high|medium|low"
  },

  "daily_life": {
    "category": "Education|Healthcare|Housing|Childcare|Employment|Transportation|Food|Legal|Religion|Leisure|Family|Relationships|Personal_Finance|none",
    "need_expressed": "<precise description of what they seek or struggle with, or null>",
    "urgency": "high|medium|low|none",
    "life_stage_signal": "<life stage inferred, e.g. Parent_School_Age_Child, Young_Professional, or null>"
  },

  "business_opportunity": {
    "opportunity_type": "Business_Idea|Investment_Interest|Job_Seeking|Hiring|Partnership_Request|Market_Gap_Observed|Service_Demand|Product_Demand|Real_Estate|Import_Export|none",
    "description": "<what opportunity or economic signal is present, or null>",
    "market_context": "local|regional|international|online|null",
    "urgency": "high|medium|low|none"
  },

  "psychographic": {
    "soviet_nostalgia": <0.0-1.0>,
    "locus_of_control": "internal|external|mixed",
    "coping_style": "action_oriented|resigned|dark_humor|denial|seeking_support",
    "security_vs_freedom": "security|freedom|balanced"
  },

  "trust_landscape": {
    "trust_government": "low|medium|high|hostile|unknown",
    "trust_media": "low|medium|high|hostile|unknown",
    "trust_peers": "low|medium|high|hostile|unknown",
    "trust_foreign": "low|medium|high|hostile|unknown"
  },

  "linguistic_intelligence": {
    "code_switching": "high|medium|low|none",
    "certainty_level": "dogmatic|confident|uncertain|questioning",
    "rhetorical_strategy": "emotional|logical|anecdotal|authoritative|humorous|mixed",
    "pronoun_pattern": "individual|collective|mixed"
  },

  "financial_signals": {
    "financial_distress_level": "none|mild|moderate|severe",
    "purchase_intent": "<what user wants to acquire or null>",
    "price_sensitivity": "high|medium|low|unknown",
    "economic_fear_trigger": "<specific trigger or null>"
  }
}"""

STRICT_TAXONOMY_PROMPT = f"""### STRICT TAXONOMY CONTRACT (VERSION {TAXONOMY_VERSION})
You MUST prioritize canonical taxonomy topics. For each topic object:
- Use `taxonomy_topic` when a taxonomy match exists (preferred path)
- Use `proposed_topic` only when no good taxonomy match exists
- Prefer taxonomy topics over proposed topics whenever possible
- You may propose at most ONE topic per analyzed item
- Set `proposed=true` only for non-taxonomy topics
- Always provide `closest_category` and `domain`

Taxonomy reference:
{compact_taxonomy_prompt(max_topics_per_category=4)}

Required topics object shape:
{{
  "name": "<Canonical Or Proposed Name>",
  "taxonomy_topic": "<Canonical topic or null>",
  "proposed_topic": "<Proposed topic or null>",
  "proposed": false,
  "closest_category": "<taxonomy category>",
  "domain": "<taxonomy domain>",
  "importance": "primary|secondary|tertiary",
  "evidence": "<quote or observation>"
}}
"""


def _safe_json_object(raw: str | None) -> dict:
    raw_text = raw.strip() if isinstance(raw, str) else ""
    if not raw_text:
        raise json.JSONDecodeError("empty response", "", 0)
    parsed = json.loads(raw_text)
    if not isinstance(parsed, dict):
        raise ValueError("Model response is not a JSON object")
    return parsed


def _clamp_score(value, default: float = 0.0) -> float:
    try:
        score = float(value)
    except Exception:
        score = default
    return max(-1.0, min(1.0, score))


def _normalize_social_sentiment_tags(parsed: dict) -> list[str]:
    tags: list[str] = []
    seen: set[str] = set()

    def _add(tag: str | None) -> None:
        if not tag:
            return
        norm = str(tag).strip().title()
        if norm not in _SOCIAL_SENTIMENT_TAGS or norm in seen:
            return
        seen.add(norm)
        tags.append(norm)

    raw_tags = parsed.get("social_sentiment_tags")
    if isinstance(raw_tags, list):
        for item in raw_tags:
            if isinstance(item, str):
                _add(item)

    tone = str(parsed.get("emotional_tone") or "").strip().lower()
    if tone:
        for needle, tag in _TONE_TO_TAGS:
            if needle in tone:
                _add(tag)

    sentiment = str(parsed.get("sentiment") or "").strip().lower()
    for tag in _DEFAULT_TAGS_BY_SENTIMENT.get(sentiment, []):
        _add(tag)

    return tags


def _trim_text(value: str | None, limit: int) -> str:
    if not value:
        return ""
    text = str(value).strip()
    if len(text) <= limit:
        return text
    return text[:limit].rstrip() + "..."


def _chunked(items: list[dict], size: int) -> list[list[dict]]:
    step = max(1, int(size))
    return [items[i:i + step] for i in range(0, len(items), step)]


def _normalize_payload(parsed: dict) -> dict:
    normalized = dict(parsed)
    normalized_topics = normalize_model_topics(parsed.get("topics") or [])
    normalized["topics"] = normalized_topics

    evidence_quotes = []
    for quote in parsed.get("evidence_quotes") or []:
        if isinstance(quote, str) and quote.strip():
            evidence_quotes.append(quote.strip()[:300])
        if len(evidence_quotes) >= 3:
            break
    normalized["evidence_quotes"] = evidence_quotes

    normalized["sentiment_score"] = _clamp_score(parsed.get("sentiment_score"), 0.0)
    normalized["social_sentiment_tags"] = _normalize_social_sentiment_tags(parsed)

    demographics = parsed.get("demographics")
    if not isinstance(demographics, dict):
        demographics = {}
    demographics.setdefault("language", "unknown")
    demographics.setdefault("inferred_gender", "unknown")
    demographics.setdefault("inferred_age_bracket", "unknown")
    normalized["demographics"] = demographics

    if config.FEATURE_EXTRACTION_V2:
        canonical_count = sum(1 for item in normalized_topics if item.get("taxonomy_topic"))
        proposed_count = sum(1 for item in normalized_topics if item.get("proposed"))
        normalized["extraction_contract"] = {
            "mode": "strict_taxonomy_primary",
            "taxonomy_version": TAXONOMY_VERSION,
            "canonical_topics": canonical_count,
            "proposed_topics": proposed_count,
        }

    message_topics: list[dict] = []
    aggregate_by_name: dict[str, dict] = {str(item.get("name")): dict(item) for item in normalized_topics if item.get("name")}
    for item in parsed.get("message_topics") or []:
        if not isinstance(item, dict):
            continue
        comment_id = str(item.get("comment_id") or "").strip()
        message_ref = str(item.get("message_ref") or "").strip()
        item_topics = normalize_model_topics(item.get("topics") or [])
        if not comment_id and not message_ref:
            continue
        message_topics.append({
            "comment_id": comment_id,
            "message_ref": message_ref,
            "topics": item_topics,
        })
        for topic in item_topics:
            name = str(topic.get("name") or "").strip()
            if name and name not in aggregate_by_name:
                aggregate_by_name[name] = dict(topic)

    if message_topics:
        normalized["message_topics"] = message_topics
        normalized["topics"] = list(aggregate_by_name.values())[:6]

    return normalized


def _extract_topic_names(parsed: dict) -> list[str]:
    names: list[str] = []
    for item in parsed.get("topics") or []:
        if isinstance(item, dict) and item.get("name"):
            names.append(str(item["name"]))
        elif isinstance(item, str):
            names.append(item)
    return names


def _request_json(*, system_prompt: str, user_context: str, max_tokens: int, request_label: str) -> dict:
    retry_limit = max(0, int(config.AI_REQUEST_MAX_RETRIES))
    retry_backoff_seconds = max(0.0, float(getattr(config, "AI_REQUEST_RETRY_BACKOFF_SECONDS", 0.0)))
    messages: list[ChatCompletionMessageParam] = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_context},
    ]

    for attempt in range(retry_limit + 1):
        try:
            response = client.chat.completions.create(
                model=config.OPENAI_MODEL,
                messages=messages,  # pyright: ignore[reportArgumentType]
                response_format={"type": "json_object"},
                temperature=0.1,
                max_tokens=max_tokens,
                timeout=config.AI_REQUEST_TIMEOUT_SECONDS,
            )
            logger.debug(
                f"{request_label}: AI response received id={getattr(response, 'id', 'unknown')} model={config.OPENAI_MODEL}"
            )
            raw = response.choices[0].message.content
            return _safe_json_object(raw)
        except Exception as exc:
            if attempt >= retry_limit:
                raise
            logger.warning(
                f"{request_label}: AI request failed on attempt {attempt + 1} ({type(exc).__name__}) — retrying ({exc})"
            )
            messages.append(
                {
                    "role": "user",
                    "content": (
                        "Return ONLY a strict JSON object matching the schema. "
                        "Do not include markdown, prose, or trailing text."
                    ),
                }
            )
            if retry_backoff_seconds > 0:
                time.sleep(retry_backoff_seconds * (attempt + 1))

    raise RuntimeError(f"{request_label}: AI request retries exhausted")
    return {}


def _comment_scope_key(telegram_user_id, channel_id, post_id) -> str:
    uid = str(telegram_user_id if telegram_user_id is not None else "anonymous")
    cid = str(channel_id or "unknown")
    pid = str(post_id or "unknown")
    return f"{uid}:{cid}:{pid}"


def _record_failure_scope(
    supabase_writer,
    *,
    scope_type: str,
    scope_key: str,
    channel_id: str | None,
    post_id: str | None,
    telegram_user_id,
    error: Exception | str,
) -> None:
    if not hasattr(supabase_writer, "record_processing_failure"):
        return
    try:
        user_id = int(telegram_user_id) if isinstance(telegram_user_id, int) else None
        supabase_writer.record_processing_failure(
            scope_type=scope_type,
            scope_key=scope_key,
            channel_id=channel_id,
            post_id=post_id,
            telegram_user_id=user_id,
            error=str(error),
        )
    except Exception:
        pass


def _clear_failure_scope(supabase_writer, *, scope_type: str, scope_key: str) -> None:
    if not hasattr(supabase_writer, "clear_processing_failure"):
        return
    try:
        supabase_writer.clear_processing_failure(scope_type, scope_key)
    except Exception:
        pass


def _blocked_scope_keys(supabase_writer, *, scope_type: str, scope_keys: list[str]) -> set[str]:
    if not hasattr(supabase_writer, "get_blocked_scopes"):
        return set()
    try:
        return set(supabase_writer.get_blocked_scopes(scope_type, scope_keys) or set())
    except Exception:
        return set()


def _analyze_comment_group_payload(payload: dict) -> dict:
    telegram_user_id = payload.get("telegram_user_id")
    post_id = payload.get("post_id")
    user_context = payload.get("user_context") or ""

    prompt_candidates = [SYSTEM_PROMPT]
    if config.FEATURE_EXTRACTION_V2:
        prompt_candidates = [
            f"{SYSTEM_PROMPT}\n\n{STRICT_TAXONOMY_PROMPT}",
            SYSTEM_PROMPT,
        ]

    parsed = None
    last_error = None
    for prompt_index, system_prompt in enumerate(prompt_candidates, start=1):
        try:
            parsed = _normalize_payload(
                _request_json(
                    system_prompt=system_prompt,
                    user_context=user_context,
                    max_tokens=max(300, int(config.AI_COMMENT_MAX_TOKENS)),
                    request_label=(
                        f"user {telegram_user_id} post {post_id or 'unknown'} "
                        f"prompt#{prompt_index}"
                    ),
                )
            )
            break
        except Exception as exc:
            last_error = exc
            if prompt_index < len(prompt_candidates):
                logger.warning(
                    f"Comment analysis fallback for user={telegram_user_id} post={post_id}: "
                    f"strict-taxonomy prompt failed ({exc}); retrying with compact base prompt"
                )
            else:
                raise

    if parsed is None:
        raise RuntimeError(f"Comment parsing failed: {last_error}")
    return parsed


# ── Comment Batch Analysis ────────────────────────────────────────────────────

def extract_intents(
    comments: list[dict],
    supabase_writer,
    deadline_epoch: float | None = None,
    *,
    include_stats: bool = False,
) -> int | dict:
    """
    Process unprocessed comments through the configured OpenAI model.
    Groups by (telegram_user_id, channel_id, post_id) — one API call per user per post.

    Returns: number of analysis records saved (default) or detailed stage stats.
    """
    started_at = time.monotonic()
    stats: dict[str, int | float] = {
        "workers": max(1, int(getattr(config, "AI_COMMENT_WORKERS", 1))),
        "inflight_limit": max(1, int(getattr(config, "AI_MAX_INFLIGHT_REQUESTS", 1))),
        "attempted_groups": 0,
        "blocked_groups": 0,
        "deferred_groups": 0,
        "succeeded_groups": 0,
        "failed_groups": 0,
        "saved": 0,
    }
    if not comments:
        stats["duration_seconds"] = round(max(0.0, time.monotonic() - started_at), 2)
        return stats if include_stats else 0

    # Prefetch parent post context for better per-post grounding.
    post_ids = [str(comment.get("post_id")) for comment in comments if comment.get("post_id")]
    post_map: dict[str, dict] = {}
    if post_ids:
        try:
            post_map = supabase_writer.get_posts_by_ids(post_ids)
        except Exception as e:
            logger.warning(f"Post context prefetch failed: {e}")

    # Group by (user, channel, post)
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for comment in comments:
        uid = comment.get("telegram_user_id") or "anonymous"
        cid = comment.get("channel_id", "unknown")
        pid = comment.get("post_id")
        groups[(uid, cid, pid)].append(comment)

    group_payloads: list[dict] = []
    for (telegram_user_id, channel_id, post_id), user_comments in groups.items():
        # Build numbered temporal message block
        message_char_limit = max(120, int(config.AI_MESSAGE_CHAR_LIMIT))
        messages_text = "\n\n".join([
            (
                f"[MSG {i+1} | COMMENT_ID {c.get('id')} | {c.get('posted_at', '')[:16]}]\n"
                f"{_trim_text(c.get('text', ''), message_char_limit)}"
            )
            for i, c in enumerate(user_comments[:config.AI_BATCH_SIZE])
        ])

        post_context_section = ""
        if post_id:
            post_context = post_map.get(str(post_id), {})
            post_excerpt = _trim_text(post_context.get("text", ""), max(180, message_char_limit))
            post_context_section = (
                f"\nPOST CONTEXT:\n"
                f"  Post ID            : {post_id}\n"
                f"  Telegram Message ID: {post_context.get('telegram_message_id')}\n"
                f"  Posted At          : {post_context.get('posted_at')}\n"
                f"  Parent Post Excerpt: {post_excerpt}\n"
            )

        # Fetch user profile to enrich AI context
        profile_section = ""
        if telegram_user_id != "anonymous":
            try:
                profile = supabase_writer.get_user_by_telegram_id(int(telegram_user_id))
                if profile:
                    name_parts = [p for p in [profile.get("first_name"), profile.get("last_name")] if p]
                    full_name = " ".join(name_parts) or "Unknown"
                    username = profile.get("username") or "no username"
                    bio = profile.get("bio") or "none"
                    profile_section = (
                        f"\nUSER PROFILE (use for precise demographic inference):\n"
                        f"  Full Name : {full_name}\n"
                        f"  Username  : @{username}\n"
                        f"  Bio       : {bio}\n"
                        f"  Is Bot    : {profile.get('is_bot', False)}\n"
                    )
            except Exception:
                pass

        scope_key = _comment_scope_key(telegram_user_id, channel_id, post_id)
        user_context = (
            f"Channel: Telegram public channel\n"
            f"Post ID: {post_id or 'unknown'}\n"
            f"Messages analyzed: {min(len(user_comments), config.AI_BATCH_SIZE)}\n"
            f"User ID: {telegram_user_id}\n"
            f"IMPORTANT: Return message_topics with one entry per message using the COMMENT_ID from each [MSG ...] header. "
            f"Only assign a topic to a message when that specific message clearly mentions it."
            f"{profile_section}"
            f"{post_context_section}\n"
            f"--- MESSAGES ---\n{messages_text}"
        )

        group_payloads.append(
            {
                "telegram_user_id": telegram_user_id,
                "channel_id": channel_id,
                "post_id": post_id,
                "user_comments": user_comments,
                "scope_key": scope_key,
                "user_context": user_context,
            }
        )

    blocked = _blocked_scope_keys(
        supabase_writer,
        scope_type="comment_group",
        scope_keys=[payload["scope_key"] for payload in group_payloads],
    )

    runnable_payloads: list[dict] = []
    for payload in group_payloads:
        if payload["scope_key"] in blocked:
            stats["blocked_groups"] = int(stats["blocked_groups"]) + 1
        else:
            runnable_payloads.append(payload)

    stats["attempted_groups"] = len(runnable_payloads)

    def _handle_success(payload: dict, parsed: dict):
        telegram_user_id = payload.get("telegram_user_id")
        channel_id = payload.get("channel_id")
        post_id = payload.get("post_id")
        user_comments = payload.get("user_comments") or []
        scope_key = payload.get("scope_key") or ""

        demographics = parsed.get("demographics") or {}
        content_id = str(post_id) if post_id else None

        if not post_id:
            logger.warning(
                f"Comment analysis fell back to legacy channel scope for user={telegram_user_id} "
                f"channel={channel_id} because post_id is missing"
            )

        analysis = {
            "channel_id": channel_id,
            "telegram_user_id": telegram_user_id if telegram_user_id != "anonymous" else None,
            "content_type": "batch",
            "content_id": content_id,
            "primary_intent": parsed.get("primary_intent"),
            "sentiment_score": parsed.get("sentiment_score"),
            "topics": _extract_topic_names(parsed),
            "language": demographics.get("language"),
            "inferred_gender": demographics.get("inferred_gender", "unknown"),
            "inferred_age_bracket": demographics.get("inferred_age_bracket", "unknown"),
            "raw_llm_response": parsed,
            "neo4j_synced": False,
        }

        supabase_writer.save_analysis(analysis)
        for c in user_comments:
            supabase_writer.mark_comment_processed(c["id"])

        _clear_failure_scope(supabase_writer, scope_type="comment_group", scope_key=scope_key)

        psycho = parsed.get("psychographic", {})
        trust = parsed.get("trust_landscape", {})
        fin = parsed.get("financial_signals", {})
        social = parsed.get("social_signals", {})
        logger.debug(
            f"User {telegram_user_id} | post={post_id or 'unknown'} | "
            f"intent={parsed.get('primary_intent')} | "
            f"sentiment={_clamp_score(parsed.get('sentiment_score'), 0.0):.2f} | "
            f"nostalgia={psycho.get('soviet_nostalgia', '?')} | "
            f"locus={psycho.get('locus_of_control', '?')} | "
            f"trust_gov={trust.get('trust_government', '?')} | "
            f"distress={fin.get('financial_distress_level', '?')} | "
            f"geo={social.get('geopolitical_alignment', '?')}"
        )

        stats["saved"] = int(stats["saved"]) + 1
        stats["succeeded_groups"] = int(stats["succeeded_groups"]) + 1

    def _handle_failure(payload: dict, error: Exception):
        telegram_user_id = payload.get("telegram_user_id")
        channel_id = payload.get("channel_id")
        post_id = payload.get("post_id")
        scope_key = payload.get("scope_key") or ""

        logger.error(f"AI processing error for user {telegram_user_id} post {post_id}: {error}")
        _record_failure_scope(
            supabase_writer,
            scope_type="comment_group",
            scope_key=scope_key,
            channel_id=str(channel_id) if channel_id else None,
            post_id=str(post_id) if post_id else None,
            telegram_user_id=telegram_user_id,
            error=error,
        )
        stats["failed_groups"] = int(stats["failed_groups"]) + 1

    workers = max(1, int(getattr(config, "AI_COMMENT_WORKERS", 1)))
    inflight_limit = max(1, int(getattr(config, "AI_MAX_INFLIGHT_REQUESTS", 1)))
    max_inflight = max(1, min(workers, inflight_limit))

    if workers <= 1:
        for index, payload in enumerate(runnable_payloads):
            if deadline_epoch is not None and time.monotonic() >= deadline_epoch:
                stats["deferred_groups"] = int(stats["deferred_groups"]) + (len(runnable_payloads) - index)
                logger.warning("Comment extraction stage deadline reached; deferring remaining users to next cycle")
                break
            try:
                parsed = _analyze_comment_group_payload(payload)
                _handle_success(payload, parsed)
            except Exception as e:
                _handle_failure(payload, e)
    else:
        next_index = 0
        deadline_reached = False

        with ThreadPoolExecutor(max_workers=workers) as executor:
            pending: dict = {}

            def _submit_available():
                nonlocal next_index, deadline_reached
                while next_index < len(runnable_payloads) and len(pending) < max_inflight:
                    if deadline_epoch is not None and time.monotonic() >= deadline_epoch:
                        deadline_reached = True
                        return
                    payload = runnable_payloads[next_index]
                    next_index += 1
                    future = executor.submit(_analyze_comment_group_payload, payload)
                    pending[future] = payload

            _submit_available()

            while pending:
                done, _ = wait(list(pending.keys()), timeout=1.0, return_when=FIRST_COMPLETED)
                if not done:
                    if deadline_epoch is not None and time.monotonic() >= deadline_epoch:
                        deadline_reached = True
                    continue

                for future in done:
                    payload = pending.pop(future)
                    try:
                        parsed = future.result()
                        _handle_success(payload, parsed)
                    except Exception as e:
                        _handle_failure(payload, e)

                if not deadline_reached:
                    _submit_available()

        if deadline_reached and next_index < len(runnable_payloads):
            remaining = len(runnable_payloads) - next_index
            stats["deferred_groups"] = int(stats["deferred_groups"]) + remaining
            logger.warning("Comment extraction stage deadline reached; deferring remaining users to next cycle")

    stats["duration_seconds"] = round(max(0.0, time.monotonic() - started_at), 2)
    logger.success(
        "AI analysis complete — "
        f"saved={int(stats['saved'])} "
        f"succeeded_groups={int(stats['succeeded_groups'])} "
        f"failed_groups={int(stats['failed_groups'])} "
        f"blocked_groups={int(stats['blocked_groups'])} "
        f"deferred_groups={int(stats['deferred_groups'])}"
    )
    return stats if include_stats else int(stats["saved"])


# ── Single Post Analysis ──────────────────────────────────────────────────────

POST_SYSTEM_PROMPT = """### ROLE
You are the same expert panel (Behavioral Analyst + Graph Architect + CIS Social Scientist).
Analyze this single Telegram CHANNEL POST as published content — not a user comment.
Focus on what the AUTHOR communicates, implies, and signals to their audience.

Apply the same language rules:
- All taxonomy labels → ENGLISH
- Topic names → canonical English, title case, singular
- evidence_quotes → preserve original language
- Descriptions → precise English

Return ONLY the JSON schema below, no preamble.

{
  "primary_intent": "<intent>",
  "intent_confidence": <0.0-1.0>,
  "evidence_quotes": ["<original language>"],
  "sentiment": "Positive|Negative|Neutral|Mixed|Urgent|Sarcastic",
  "sentiment_score": <-1.0 to 1.0>,
  "emotional_tone": "<emotion>",
  "social_sentiment_tags": ["Anxious|Frustrated|Angry|Confused|Hopeful|Trusting|Distrustful|Solidarity|Exhausted|Grief"],
  "topics": [{"name": "<Canonical English>", "importance": "primary|secondary|tertiary", "evidence": "<>"}],
  "desires": {"explicit": "<>", "implicit": "<>", "underlying_need": "<>"},
  "hidden_signals": ["<>"],
  "negative_events": [{"description": "<>", "severity": "high|medium|low", "scope": "personal|local|national|global"}],
  "positive_events": [{"description": "<>", "scope": "personal|local|national|global"}],
  "entities": [{"name": "<Canonical English>", "type": "person|group|organization|place|concept|media", "sentiment_toward": "positive|negative|neutral|ambiguous|fearful|admiring|mocking"}],
  "social_signals": {
    "geopolitical_alignment": "<>",
    "collective_memory": "<or null>",
    "in_out_group": "<or null>",
    "migration_intent": "Yes|No|Implied",
    "diaspora_signals": "Yes|No",
    "authority_attitude": "<>"
  },
  "information_ecosystem": {
    "media_references": [],
    "conspiracy_signals": "<or null>",
    "information_warfare": false
  },
  "demographics": {
    "language": "<ISO 639-1>",
    "inferred_gender": "male|female|unknown",
    "inferred_age_bracket": "<bracket>",
    "confidence": "high|medium|low"
  }
}"""


POST_SYSTEM_PROMPT_COMPACT = """You analyze one Telegram channel post.
Return STRICT JSON only (no markdown).

Schema:
{
  "primary_intent": "<intent>",
  "intent_confidence": <0.0-1.0>,
  "evidence_quotes": ["<original language>"],
  "sentiment": "Positive|Negative|Neutral|Mixed|Urgent|Sarcastic",
  "sentiment_score": <-1.0 to 1.0>,
  "emotional_tone": "<emotion>",
  "social_sentiment_tags": ["Anxious|Frustrated|Angry|Confused|Hopeful|Trusting|Distrustful|Solidarity|Exhausted|Grief"],
  "topics": [
    {"name": "<Canonical English>", "importance": "primary|secondary|tertiary", "evidence": "<short evidence>"}
  ],
  "entities": [
    {"name": "<Canonical English>", "type": "person|group|organization|place|concept|media", "sentiment_toward": "positive|negative|neutral|ambiguous|fearful|admiring|mocking"}
  ],
  "social_signals": {
    "geopolitical_alignment": "Pro_Russia|Pro_West|Pro_Armenia|Pro_Azerbaijan|Nationalist|Anti_Government|Neutral|Ambiguous|unknown",
    "collective_memory": "<or null>",
    "in_out_group": "<or null>",
    "migration_intent": "Yes|No|Implied",
    "diaspora_signals": "Yes|No",
    "authority_attitude": "Deferential|Critical|Dismissive|Fearful|Admiring|Humorous|unknown"
  },
  "demographics": {
    "language": "<ISO 639-1>",
    "inferred_gender": "male|female|unknown",
    "inferred_age_bracket": "13-17|18-24|25-34|35-44|45-54|55+|unknown",
    "confidence": "high|medium|low"
  }
}
"""


POST_BATCH_SYSTEM_PROMPT_COMPACT = """You analyze MULTIPLE Telegram channel posts.
Each post MUST be analyzed independently.
Never merge or transfer evidence between posts.

Return STRICT JSON only with this schema:
{
  "items": [
    {
      "post_id": "<post UUID from input>",
      "primary_intent": "<intent>",
      "intent_confidence": <0.0-1.0>,
      "evidence_quotes": ["<original language>"],
      "sentiment": "Positive|Negative|Neutral|Mixed|Urgent|Sarcastic",
      "sentiment_score": <-1.0 to 1.0>,
      "emotional_tone": "<emotion>",
      "social_sentiment_tags": ["Anxious|Frustrated|Angry|Confused|Hopeful|Trusting|Distrustful|Solidarity|Exhausted|Grief"],
      "topics": [
        {"name": "<Canonical English>", "importance": "primary|secondary|tertiary", "evidence": "<short evidence>"}
      ],
      "entities": [
        {"name": "<Canonical English>", "type": "person|group|organization|place|concept|media", "sentiment_toward": "positive|negative|neutral|ambiguous|fearful|admiring|mocking"}
      ],
      "social_signals": {
        "geopolitical_alignment": "Pro_Russia|Pro_West|Pro_Armenia|Pro_Azerbaijan|Nationalist|Anti_Government|Neutral|Ambiguous|unknown",
        "collective_memory": "<or null>",
        "in_out_group": "<or null>",
        "migration_intent": "Yes|No|Implied",
        "diaspora_signals": "Yes|No",
        "authority_attitude": "Deferential|Critical|Dismissive|Fearful|Admiring|Humorous|unknown"
      },
      "demographics": {
        "language": "<ISO 639-1>",
        "inferred_gender": "male|female|unknown",
        "inferred_age_bracket": "13-17|18-24|25-34|35-44|45-54|55+|unknown",
        "confidence": "high|medium|low"
      }
    }
  ]
}

Rules:
1) Return exactly one item for each provided post_id.
2) Do not include extra or missing post_ids.
3) Keep each item scoped to its own post text only.
"""


def _build_post_analysis_row(post: dict, parsed: dict) -> dict:
    demographics = parsed.get("demographics") or {}
    return {
        "channel_id":           post["channel_id"],
        "telegram_user_id":     None,
        "content_type":         "post",
        "content_id":           post["id"],
        "primary_intent":       parsed.get("primary_intent"),
        "sentiment_score":      parsed.get("sentiment_score"),
        "topics":               _extract_topic_names(parsed),
        "language":             demographics.get("language"),
        "inferred_gender":      "unknown",
        "inferred_age_bracket": "unknown",
        "raw_llm_response":     parsed,
        "neo4j_synced":         False,
    }


def _persist_post_analysis(post: dict, parsed: dict, supabase_writer) -> None:
    analysis = _build_post_analysis_row(post, parsed)
    supabase_writer.save_analysis(analysis)
    supabase_writer.mark_post_processed(post["id"])
    logger.debug(
        f"Post analyzed {post.get('id')} | intent={parsed.get('primary_intent')} | "
        f"tone={parsed.get('emotional_tone')} | "
        f"geo={parsed.get('social_signals', {}).get('geopolitical_alignment', '?')}"
    )


def _analyze_single_post_payload(post: dict) -> dict:
    prompt_style = (config.AI_POST_PROMPT_STYLE or "compact").strip().lower()
    base_prompt = POST_SYSTEM_PROMPT_COMPACT if prompt_style == "compact" else POST_SYSTEM_PROMPT
    prompt_candidates = [base_prompt]
    if base_prompt != POST_SYSTEM_PROMPT_COMPACT:
        prompt_candidates.append(POST_SYSTEM_PROMPT_COMPACT)

    text = post.get("text", "")
    post_text = _trim_text(text, max(200, int(config.AI_MESSAGE_CHAR_LIMIT) * 3))
    parsed = None
    last_error = None

    for index, candidate_prompt in enumerate(prompt_candidates, start=1):
        system_prompt = candidate_prompt
        if config.FEATURE_EXTRACTION_V2:
            system_prompt = f"{candidate_prompt}\n\n{STRICT_TAXONOMY_PROMPT}"

        try:
            parsed = _normalize_payload(
                _request_json(
                    system_prompt=system_prompt,
                    user_context=f"Analyze this channel post:\n\n{post_text}",
                    max_tokens=max(250, int(config.AI_POST_MAX_TOKENS)),
                    request_label=f"post {post.get('id')} prompt#{index}",
                )
            )
            break
        except Exception as exc:
            last_error = exc
            if index < len(prompt_candidates):
                logger.warning(
                    f"Post {post.get('id')}: primary prompt failed ({exc}); retrying with compact fallback"
                )
            else:
                raise

    if parsed is None:
        raise RuntimeError(f"Post parsing failed: {last_error}")
    return parsed


def _validate_post_batch_payload(parsed: dict, posts: list[dict]) -> dict[str, dict]:
    items = parsed.get("items")
    if not isinstance(items, list):
        raise ValueError("Batch post response missing 'items' list")

    expected_ids = [str(post.get("id") or "") for post in posts if post.get("id")]
    expected_set = set(expected_ids)
    if len(expected_ids) != len(posts) or len(expected_set) != len(expected_ids):
        raise ValueError("Invalid input posts for batch validation")

    output: dict[str, dict] = {}
    for item in items:
        if not isinstance(item, dict):
            raise ValueError("Batch post item is not an object")

        post_id = str(item.get("post_id") or "").strip()
        if not post_id:
            raise ValueError("Batch post item missing post_id")
        if post_id not in expected_set:
            raise ValueError(f"Batch post item has unknown post_id={post_id}")
        if post_id in output:
            raise ValueError(f"Batch post response has duplicate post_id={post_id}")

        payload = {k: v for k, v in item.items() if k != "post_id"}
        output[post_id] = _normalize_payload(payload)

    missing = expected_set - set(output.keys())
    if missing:
        raise ValueError(f"Batch post response missing ids: {sorted(missing)}")

    return output


def _analyze_post_batch_payload(posts: list[dict]) -> dict[str, dict]:
    if not posts:
        return {}

    item_char_limit = max(220, int(config.AI_MESSAGE_CHAR_LIMIT) * 2)
    sections: list[str] = []
    for index, post in enumerate(posts, start=1):
        post_id = str(post.get("id") or "")
        if not post_id:
            raise ValueError("Post batch payload contains item without id")
        sections.append(
            f"[POST {index}]\n"
            f"post_id: {post_id}\n"
            f"channel_id: {post.get('channel_id')}\n"
            f"telegram_message_id: {post.get('telegram_message_id')}\n"
            f"posted_at: {post.get('posted_at')}\n"
            f"text:\n{_trim_text(post.get('text', ''), item_char_limit)}"
        )

    system_prompt = POST_BATCH_SYSTEM_PROMPT_COMPACT
    if config.FEATURE_EXTRACTION_V2:
        system_prompt = f"{POST_BATCH_SYSTEM_PROMPT_COMPACT}\n\n{STRICT_TAXONOMY_PROMPT}"

    parsed = _request_json(
        system_prompt=system_prompt,
        user_context=(
            "Analyze each post independently and return EXACTLY one item per post_id.\n\n"
            f"Posts count: {len(posts)}\n\n"
            + "\n\n---\n\n".join(sections)
        ),
        max_tokens=max(700, int(config.AI_POST_BATCH_MAX_TOKENS)),
        request_label=f"post-batch size={len(posts)}",
    )
    return _validate_post_batch_payload(parsed, posts)


def _process_single_post(post: dict, supabase_writer) -> bool:
    text = post.get("text", "")
    if not text or len(text.strip()) < 20:
        supabase_writer.mark_post_processed(post["id"])
        return False

    try:
        parsed = _analyze_single_post_payload(post)
        _persist_post_analysis(post, parsed, supabase_writer)
        _clear_failure_scope(supabase_writer, scope_type="post", scope_key=str(post["id"]))
        return True
    except Exception as e:
        logger.error(f"Post intent extraction failed for post {post['id']}: {e}")
        _record_failure_scope(
            supabase_writer,
            scope_type="post",
            scope_key=str(post["id"]),
            channel_id=str(post.get("channel_id")) if post.get("channel_id") else None,
            post_id=str(post.get("id")) if post.get("id") else None,
            telegram_user_id=None,
            error=e,
        )
        # Do NOT mark as processed — retry on the next cycle
        return False


def extract_post_intents(
    posts: list[dict],
    supabase_writer,
    *,
    deadline_epoch: float | None = None,
    include_stats: bool = False,
) -> int | dict:
    """Analyze posts with strict micro-batching, bounded concurrency, and safe fallback."""
    started_at = time.monotonic()
    stats: dict[str, int | float] = {
        "workers": max(1, int(getattr(config, "AI_POST_WORKERS", 1))),
        "inflight_limit": max(1, int(getattr(config, "AI_MAX_INFLIGHT_REQUESTS", 1))),
        "attempted_posts": 0,
        "blocked_posts": 0,
        "deferred_posts": 0,
        "succeeded_posts": 0,
        "failed_posts": 0,
        "batch_failures": 0,
        "saved": 0,
    }

    if not posts:
        stats["duration_seconds"] = round(max(0.0, time.monotonic() - started_at), 2)
        return stats if include_stats else 0

    processable_posts: list[dict] = []
    for post in posts:
        text = post.get("text", "")
        if not text or len(text.strip()) < 20:
            supabase_writer.mark_post_processed(post["id"])
            continue
        processable_posts.append(post)

    if not processable_posts:
        stats["duration_seconds"] = round(max(0.0, time.monotonic() - started_at), 2)
        return stats if include_stats else 0

    blocked = _blocked_scope_keys(
        supabase_writer,
        scope_type="post",
        scope_keys=[str(post.get("id")) for post in processable_posts if post.get("id")],
    )
    runnable_posts = []
    for post in processable_posts:
        key = str(post.get("id") or "")
        if key and key in blocked:
            stats["blocked_posts"] = int(stats["blocked_posts"]) + 1
            continue
        runnable_posts.append(post)

    stats["attempted_posts"] = len(runnable_posts)
    if not runnable_posts:
        stats["duration_seconds"] = round(max(0.0, time.monotonic() - started_at), 2)
        return stats if include_stats else 0

    batch_size = max(1, int(config.AI_POST_BATCH_SIZE))
    chunks = _chunked(runnable_posts, batch_size)
    workers = max(1, int(getattr(config, "AI_POST_WORKERS", 1)))
    inflight_limit = max(1, int(getattr(config, "AI_MAX_INFLIGHT_REQUESTS", 1)))
    max_inflight = max(1, min(workers, inflight_limit))

    def _handle_chunk_success(chunk: list[dict], parsed_by_post_id: dict[str, dict]):
        for post in chunk:
            post_id = str(post.get("id") or "")
            parsed = parsed_by_post_id.get(post_id)
            if not parsed:
                stats["failed_posts"] = int(stats["failed_posts"]) + 1
                _record_failure_scope(
                    supabase_writer,
                    scope_type="post",
                    scope_key=post_id,
                    channel_id=str(post.get("channel_id")) if post.get("channel_id") else None,
                    post_id=post_id or None,
                    telegram_user_id=None,
                    error="missing parsed payload for post id",
                )
                continue
            _persist_post_analysis(post, parsed, supabase_writer)
            _clear_failure_scope(supabase_writer, scope_type="post", scope_key=post_id)
            stats["saved"] = int(stats["saved"]) + 1
            stats["succeeded_posts"] = int(stats["succeeded_posts"]) + 1

    def _fallback_chunk_to_single(chunk: list[dict]):
        for index, post in enumerate(chunk):
            if deadline_epoch is not None and time.monotonic() >= deadline_epoch:
                stats["deferred_posts"] = int(stats["deferred_posts"]) + (len(chunk) - index)
                logger.warning("Post extraction deadline reached during fallback; deferring remaining posts")
                return
            if _process_single_post(post, supabase_writer):
                stats["saved"] = int(stats["saved"]) + 1
                stats["succeeded_posts"] = int(stats["succeeded_posts"]) + 1
            else:
                stats["failed_posts"] = int(stats["failed_posts"]) + 1

    if workers <= 1:
        for chunk_index, chunk in enumerate(chunks):
            if deadline_epoch is not None and time.monotonic() >= deadline_epoch:
                remaining_posts = sum(len(item) for item in chunks[chunk_index:])
                stats["deferred_posts"] = int(stats["deferred_posts"]) + remaining_posts
                logger.warning("Post extraction stage deadline reached; deferring remaining posts to next cycle")
                break

            if len(chunk) == 1:
                if _process_single_post(chunk[0], supabase_writer):
                    stats["saved"] = int(stats["saved"]) + 1
                    stats["succeeded_posts"] = int(stats["succeeded_posts"]) + 1
                else:
                    stats["failed_posts"] = int(stats["failed_posts"]) + 1
                continue

            try:
                parsed_by_post_id = _analyze_post_batch_payload(chunk)
                _handle_chunk_success(chunk, parsed_by_post_id)
            except Exception as batch_error:
                stats["batch_failures"] = int(stats["batch_failures"]) + 1
                logger.warning(
                    f"Post batch analysis failed for {len(chunk)} posts ({batch_error}); falling back to single-post mode"
                )
                _fallback_chunk_to_single(chunk)
    else:
        next_chunk = 0
        deadline_reached = False

        def _analyze_chunk(chunk: list[dict]) -> dict[str, dict]:
            if len(chunk) == 1:
                post = chunk[0]
                return {str(post.get("id")): _analyze_single_post_payload(post)}
            return _analyze_post_batch_payload(chunk)

        with ThreadPoolExecutor(max_workers=workers) as executor:
            pending: dict = {}

            def _submit_available():
                nonlocal next_chunk, deadline_reached
                while next_chunk < len(chunks) and len(pending) < max_inflight:
                    if deadline_epoch is not None and time.monotonic() >= deadline_epoch:
                        deadline_reached = True
                        return
                    chunk = chunks[next_chunk]
                    next_chunk += 1
                    future = executor.submit(_analyze_chunk, chunk)
                    pending[future] = chunk

            _submit_available()

            while pending:
                done, _ = wait(list(pending.keys()), timeout=1.0, return_when=FIRST_COMPLETED)
                if not done:
                    if deadline_epoch is not None and time.monotonic() >= deadline_epoch:
                        deadline_reached = True
                    continue

                for future in done:
                    chunk = pending.pop(future)
                    try:
                        parsed_by_post_id = future.result()
                        _handle_chunk_success(chunk, parsed_by_post_id)
                    except Exception as batch_error:
                        stats["batch_failures"] = int(stats["batch_failures"]) + 1
                        logger.warning(
                            f"Post batch analysis failed for {len(chunk)} posts ({batch_error}); "
                            "falling back to single-post mode"
                        )
                        _fallback_chunk_to_single(chunk)

                if not deadline_reached:
                    _submit_available()

        if deadline_reached and next_chunk < len(chunks):
            remaining_posts = sum(len(item) for item in chunks[next_chunk:])
            stats["deferred_posts"] = int(stats["deferred_posts"]) + remaining_posts
            logger.warning("Post extraction stage deadline reached; deferring remaining posts to next cycle")

    stats["duration_seconds"] = round(max(0.0, time.monotonic() - started_at), 2)
    logger.success(
        "Post AI analysis complete — "
        f"saved={int(stats['saved'])} "
        f"succeeded_posts={int(stats['succeeded_posts'])} "
        f"failed_posts={int(stats['failed_posts'])} "
        f"blocked_posts={int(stats['blocked_posts'])} "
        f"deferred_posts={int(stats['deferred_posts'])} "
        f"batch_failures={int(stats['batch_failures'])}"
    )
    return stats if include_stats else int(stats["saved"])


def extract_post_intent(post: dict, supabase_writer) -> bool:
    """Analyze a single channel post with full behavioral intelligence framework."""
    return _process_single_post(post, supabase_writer)
