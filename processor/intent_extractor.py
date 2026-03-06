"""
intent_extractor.py — Expert-grade behavioral intelligence extraction via GPT-4o-mini.

Expert Panel:
  1. Behavioral Intelligence Analyst     — psychological profile, desires, hidden signals
  2. Graph Database Architect (Neo4j)    — canonical English labels, dedup, clean graph nodes
  3. CIS/Caucasus Social Scientist       — sarcasm detection, collective memory, geopolitical alignment

Strategy:
  - Groups up to 50 comments by (user_id, channel_id) for cost-effective batching
  - Returns 13-dimension structured JSON per user batch
  - Full output stored in raw_llm_response JSONB for flexibility
  - Standard columns (primary_intent, sentiment_score, topics, language) kept for Neo4j compat
"""
from __future__ import annotations
from openai import OpenAI
from loguru import logger
from collections import defaultdict
import json
import config

client = OpenAI(api_key=config.OPENAI_API_KEY)

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

  "topics": [
    {"name": "<Canonical English Topic>", "importance": "primary|secondary|tertiary", "evidence": "<quote or observation>"}
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


# ── Comment Batch Analysis ────────────────────────────────────────────────────

def extract_intents(comments: list[dict], supabase_writer) -> int:
    """
    Process unprocessed comments through GPT-4o-mini.
    Groups by (telegram_user_id, channel_id) — one API call per user per channel.

    Returns: number of analysis records saved
    """
    if not comments:
        return 0

    # Group by (user, channel)
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for comment in comments:
        uid = comment.get("telegram_user_id") or "anonymous"
        cid = comment.get("channel_id", "unknown")
        groups[(uid, cid)].append(comment)

    saved = 0

    for (telegram_user_id, channel_id), user_comments in groups.items():
        # Build numbered temporal message block
        messages_text = "\n\n".join([
            f"[MSG {i+1} | {c.get('posted_at', '')[:16]}]\n{c.get('text', '')}"
            for i, c in enumerate(user_comments[:config.AI_BATCH_SIZE])
        ])

        # Fetch user profile to enrich AI context
        profile_section = ""
        if telegram_user_id != "anonymous":
            try:
                profile = supabase_writer.get_user_by_telegram_id(int(telegram_user_id))
                if profile:
                    name_parts = [p for p in [profile.get("first_name"), profile.get("last_name")] if p]
                    full_name  = " ".join(name_parts) or "Unknown"
                    username   = profile.get("username") or "no username"
                    bio        = profile.get("bio") or "none"
                    profile_section = (
                        f"\nUSER PROFILE (use for precise demographic inference):\n"
                        f"  Full Name : {full_name}\n"
                        f"  Username  : @{username}\n"
                        f"  Bio       : {bio}\n"
                        f"  Is Bot    : {profile.get('is_bot', False)}\n"
                    )
            except Exception:
                pass  # Profile lookup failure is non-critical

        user_context = (
            f"Channel: Telegram public channel\n"
            f"Messages analyzed: {min(len(user_comments), config.AI_BATCH_SIZE)}\n"
            f"User ID: {telegram_user_id}"
            f"{profile_section}\n"
            f"--- MESSAGES ---\n{messages_text}"
        )

        try:
            response = client.chat.completions.create(
                model=config.OPENAI_MODEL,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user",   "content": user_context}
                ],
                response_format={"type": "json_object"},
                temperature=0.1,
                max_tokens=1600,   # Rich 13-dimension schema needs more tokens
            )

            raw    = response.choices[0].message.content
            parsed = json.loads(raw)

            demographics = parsed.get("demographics", {})
            analysis = {
                "channel_id":           channel_id,
                "telegram_user_id":     telegram_user_id if telegram_user_id != "anonymous" else None,
                "content_type":         "batch",
                "primary_intent":       parsed.get("primary_intent"),
                "sentiment_score":      parsed.get("sentiment_score"),
                "topics":               [
                    t["name"] if isinstance(t, dict) else t
                    for t in parsed.get("topics", [])
                    if t and (isinstance(t, str) or (isinstance(t, dict) and t.get("name")))
                ],
                "language":             demographics.get("language"),
                "inferred_gender":      demographics.get("inferred_gender", "unknown"),
                "inferred_age_bracket": demographics.get("inferred_age_bracket", "unknown"),
                "raw_llm_response":     parsed,   # Full 19-dimension analysis in JSONB
                "neo4j_synced":         False,
            }

            supabase_writer.save_analysis(analysis)
            saved += 1

            for c in user_comments:
                supabase_writer.mark_comment_processed(c["id"])

            psycho = parsed.get("psychographic", {})
            trust  = parsed.get("trust_landscape", {})
            fin    = parsed.get("financial_signals", {})
            social = parsed.get("social_signals", {})
            logger.debug(
                f"User {telegram_user_id} | "
                f"intent={parsed.get('primary_intent')} | "
                f"sentiment={parsed.get('sentiment_score', 0):.2f} | "
                f"nostalgia={psycho.get('soviet_nostalgia', '?')} | "
                f"locus={psycho.get('locus_of_control', '?')} | "
                f"trust_gov={trust.get('trust_government', '?')} | "
                f"distress={fin.get('financial_distress_level', '?')} | "
                f"geo={social.get('geopolitical_alignment', '?')}"
            )

        except json.JSONDecodeError:
            logger.error(f"GPT returned invalid JSON for user {telegram_user_id}")
        except Exception as e:
            logger.error(f"AI processing error for user {telegram_user_id}: {e}")

    logger.success(f"AI analysis complete — {saved} records saved")
    return saved


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


def extract_post_intent(post: dict, supabase_writer) -> bool:
    """
    Analyze a single channel post with full behavioral intelligence framework.
    """
    text = post.get("text", "")
    if not text or len(text.strip()) < 20:
        supabase_writer.mark_post_processed(post["id"])
        return False

    try:
        response = client.chat.completions.create(
            model=config.OPENAI_MODEL,
            messages=[
                {"role": "system", "content": POST_SYSTEM_PROMPT},
                {"role": "user",   "content": f"Analyze this channel post:\n\n{text}"}
            ],
            response_format={"type": "json_object"},
            temperature=0.1,
            max_tokens=1000,
        )

        raw    = response.choices[0].message.content
        parsed = json.loads(raw)
        demographics = parsed.get("demographics", {})

        analysis = {
            "channel_id":           post["channel_id"],
            "telegram_user_id":     None,
            "content_type":         "post",
            "content_id":           post["id"],
            "primary_intent":       parsed.get("primary_intent"),
            "sentiment_score":      parsed.get("sentiment_score"),
            "topics":               [t["name"] for t in parsed.get("topics", []) if isinstance(t, dict)],
            "language":             demographics.get("language"),
            "inferred_gender":      "unknown",
            "inferred_age_bracket": "unknown",
            "raw_llm_response":     parsed,
            "neo4j_synced":         False,
        }

        supabase_writer.save_analysis(analysis)
        supabase_writer.mark_post_processed(post["id"])

        logger.debug(
            f"Post analyzed | intent={parsed.get('primary_intent')} | "
            f"tone={parsed.get('emotional_tone')} | "
            f"geo={parsed.get('social_signals', {}).get('geopolitical_alignment', '?')}"
        )
        return True

    except Exception as e:
        logger.error(f"Post intent extraction failed for {post['id']}: {e}")
        # Do NOT mark as processed — retry on the next cycle
        return False
