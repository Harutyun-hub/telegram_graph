# DeepGraph — Telegram Intelligence Pipeline

> **Version:** 1.0.0 | **Status:** Active Development | **Classification:** OSINT / Behavioral Analytics

---

## Executive Summary

DeepGraph is an end-to-end intelligence pipeline that collects public Telegram channel data, extracts behavioral signals using large language models, and represents the results as an interactive knowledge graph. The system enables analysts to understand **who is talking, what they care about, how they interact, and why** — across any number of public Telegram channels simultaneously.

The pipeline is designed around a **decoupled, async-first architecture** that separates data collection, AI enrichment, and graph storage into independent layers. Each layer can operate, scale, and fail independently without affecting the others.

---

## Problem Statement

Public Telegram channels represent one of the richest sources of unstructured behavioral data available today. However, the raw data — millions of posts and comments — is impossible to analyze manually. Three core challenges exist:

1. **Volume** — Active channels produce thousands of messages per day
2. **Context Loss** — Flat text exports destroy the conversational thread structure
3. **Meaning Gap** — Raw text doesn't reveal *intent*, *sentiment*, or *topic clusters*

DeepGraph solves all three by combining Telegram's MTProto API, a structured PostgreSQL buffer, GPT-4o-mini batch processing, and a Neo4j graph database.

---

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          DeepGraph Pipeline                             │
│                                                                         │
│  ┌──────────────────┐     ┌──────────────────┐     ┌────────────────┐  │
│  │  EXTRACTION      │     │  INTELLIGENCE    │     │  GRAPH LAYER   │  │
│  │  LAYER           │────▶│  LAYER           │────▶│                │  │
│  │                  │     │                  │     │                │  │
│  │  Telethon        │     │  GPT-4o-mini     │     │  Neo4j AuraDB  │  │
│  │  (MTProto API)   │     │  Batch Processor │     │  Knowledge     │  │
│  │                  │     │                  │     │  Graph         │  │
│  │  • Channel posts │     │  • Intent extract│     │                │  │
│  │  • Comments      │     │  • Sentiment     │     │  • User nodes  │  │
│  │  • User profiles │     │  • Topic tagging │     │  • Topic nodes │  │
│  │  • Reply threads │     │  • Demographics  │     │  • Intent nodes│  │
│  └────────┬─────────┘     └────────┬─────────┘     └───────┬────────┘  │
│           │                        │                        │           │
│           ▼                        ▼                        ▼           │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │                    BUFFER LAYER (Supabase / PostgreSQL)            │ │
│  │  telegram_channels │ telegram_posts │ telegram_comments │          │ │
│  │  telegram_users    │ ai_analysis                                   │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│                                    │                                    │
│                                    ▼                                    │
│                        ┌───────────────────────┐                        │
│                        │  VISUALIZATION LAYER  │                        │
│                        │  Graph Dashboard      │                        │
│                        │  (Neo4j Bloom / D3.js)│                        │
│                        └───────────────────────┘                        │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Technology Stack

| Layer | Technology | Purpose | Why Chosen |
|---|---|---|---|
| **Extraction** | Python 3.8+ / Telethon 1.42 | Async Telegram API client | Most stable MTProto library; async-first; handles FloodWait natively |
| **Buffer** | Supabase (PostgreSQL) | Staging database + processing queue | Managed Postgres with REST API; built-in auth; no infra management |
| **AI Processing** | OpenAI GPT-4o-mini | Intent, sentiment, topic extraction | Best cost/quality ratio; structured JSON output; 128K context window |
| **Graph Database** | Neo4j AuraDB | Behavioral knowledge graph | Native graph traversal; Cypher query language; built-in visualization |
| **Scheduling** | APScheduler 3.11 | Periodic pipeline execution | Async-compatible; lightweight; no external queue dependency |
| **Logging** | Loguru | Structured application logging | Zero-config; colored output; file rotation built-in |
| **Config** | python-dotenv | Environment variable management | Simple; widely adopted; no external dependencies |

---

## Data Model

### PostgreSQL Schema (Buffer Layer)

```
telegram_channels          telegram_posts              telegram_comments
─────────────────          ──────────────              ─────────────────
id (UUID) PK          ┌──▶ id (UUID) PK           ┌──▶ id (UUID) PK
channel_username       │    channel_id (FK) ────────┘    post_id (FK)
channel_title          │    telegram_message_id           channel_id (FK)
telegram_channel_id    │    text                          user_id (FK)
member_count           │    media_type                    telegram_message_id
is_active              │    views / forwards              reply_to_message_id ◀─┐
scrape_depth_days      │    reactions (JSONB)             text                   │
scrape_comments        │    has_comments                  telegram_user_id       │
last_scraped_at        │    comment_count                 posted_at              │
                       │    is_processed ◀── AI queue     is_processed           │
                       │    neo4j_synced ◀── Graph queue  neo4j_synced           │
                       │                                  (self-referential) ────┘
                       │
                       │    telegram_users              ai_analysis
                       │    ──────────────              ───────────
                       └──  id (UUID) PK               id (UUID) PK
                            telegram_user_id (UNIQUE)   channel_id (FK)
                            username / first_name       telegram_user_id
                            last_name / bio             primary_intent
                            is_bot                      sentiment_score (-1→1)
                            first_seen_at               topics (JSONB array)
                            last_seen_at                language (ISO 639-1)
                                                        inferred_gender
                                                        inferred_age_bracket
                                                        raw_llm_response (JSONB)
                                                        neo4j_synced
```

### Neo4j Graph Schema (Intelligence Layer)

```
Node Types:                    Relationship Types:
───────────                    ──────────────────
(:Channel)                     (Post)-[:IN_CHANNEL]->(Channel)
(:Post)                        (Comment)-[:ON_POST]->(Post)
(:Comment)                     (Comment)-[:REPLIED_TO]->(Comment)
(:User)                        (User)-[:COMMENTED]->(Comment)
(:Topic)                       (User)-[:EXHIBITS]->(Intent)
(:Intent)                      (User)-[:INTERESTED_IN]->(Topic)
                               (Channel)-[:DISCUSSES]->(Topic)

Relationship Properties:
  EXHIBITS    → { count: int, sentiment: float }
  INTERESTED_IN → { count: int }
  DISCUSSES   → { count: int }
```

---

## Project Structure

```
DeepGraph/
│
├── .env                        # Credentials (never committed)
├── .gitignore                  # Protects .env, .session, __pycache__
├── requirements.txt            # Python dependencies
├── config.py                   # Environment loader + validation
├── main.py                     # Pipeline entry point + APScheduler
│
├── scraper/                    # EXTRACTION LAYER
│   ├── session_manager.py      # Telethon auth (SMS/app code → session file)
│   ├── channel_scraper.py      # iter_messages() with FloodWait handling
│   └── comment_scraper.py      # GetDiscussionMessage for threaded comments
│
├── buffer/                     # BUFFER LAYER
│   └── supabase_writer.py      # All Supabase CRUD (upsert with dedup)
│
├── processor/                  # INTELLIGENCE LAYER
│   └── intent_extractor.py     # GPT-4o-mini batch processor (50 msgs/call)
│
└── ingester/                   # GRAPH LAYER
    └── neo4j_writer.py         # Cypher MERGE statements + constraint setup
```

---

## Pipeline Execution Flow

```
Every 15 minutes — SCRAPE JOB
  1. Read telegram_channels WHERE is_active = TRUE
  2. For each channel:
     a. Fetch messages newer than last_scraped_at
     b. Upsert to telegram_posts (dedup on channel_id + message_id)
     c. For posts with has_comments = TRUE:
        → Fetch replies via GetDiscussionMessage
        → Preserve reply_to_message_id for thread structure
        → Upsert to telegram_comments
     d. Update last_scraped_at = NOW()

Every 60 minutes — AI PROCESS JOB
  1. Fetch comments WHERE is_processed = FALSE (batch 200)
  2. Group by (telegram_user_id, channel_id)
  3. For each user group:
     → Send up to 50 messages as single GPT-4o-mini prompt
     → Receive structured JSON: { intent, sentiment, topics, language,
                                   inferred_gender, inferred_age_bracket }
     → Save to ai_analysis table
     → Mark comments as is_processed = TRUE
  4. Process standalone posts (no comments) similarly

Every 60 minutes — NEO4J SYNC JOB
  1. Sync all active Channel nodes
  2. Fetch ai_analysis WHERE neo4j_synced = FALSE
  3. For each record:
     → MERGE User node (by telegram_user_id)
     → MERGE Intent node → create EXHIBITS relationship (accumulate count)
     → MERGE Topic nodes → create INTERESTED_IN relationships
     → MERGE Channel → DISCUSSES → Topic
     → Mark as neo4j_synced = TRUE
```

---

## AI Intelligence Layer

### Prompt Strategy

Each GPT-4o-mini call receives **up to 50 messages from the same user** in the same channel, grouped together. This approach:

- **Reduces API cost by ~80%** vs. per-message processing
- **Improves accuracy** — the model sees conversational context, not isolated messages
- **Extracts richer signals** — repeated topics across messages are more reliably detected

### Output Schema

```json
{
  "primary_intent": "Information Seeking",
  "sentiment_score": 0.65,
  "topics": ["cryptocurrency", "privacy", "regulation"],
  "language": "en",
  "inferred_gender": "male",
  "inferred_age_bracket": "25-34"
}
```

### Intent Taxonomy

| Intent | Description |
|---|---|
| Information Seeking | Asking questions, requesting clarification |
| Opinion Sharing | Expressing views, making statements |
| Promotion/Spam | Advertising products, services, or channels |
| Emotional Expression | Reactions, celebration, frustration |
| Debate/Argument | Contradicting others, defending positions |
| Humor/Sarcasm | Jokes, memes, ironic statements |
| Support/Help | Answering questions, offering assistance |
| Coordination | Organizing events, calling for action |

---

## Graph Intelligence Queries (Cypher Examples)

```cypher
// Top topics in a channel
MATCH (c:Channel {username: "russianteaminarmenia"})-[r:DISCUSSES]->(t:Topic)
RETURN t.name, r.count ORDER BY r.count DESC LIMIT 10

// Most active users by comment count
MATCH (u:User)-[:COMMENTED]->(c:Comment)-[:ON_POST]->(p:Post)
RETURN u.telegram_user_id, count(c) AS comments ORDER BY comments DESC LIMIT 20

// Users who exhibit a specific intent
MATCH (u:User)-[r:EXHIBITS]->(i:Intent {name: "Promotion/Spam"})
WHERE r.count > 3
RETURN u.username, r.count, r.sentiment ORDER BY r.count DESC

// Conversation thread (who replied to whom)
MATCH path = (c1:Comment)-[:REPLIED_TO*1..5]->(c2:Comment)
RETURN path LIMIT 25

// Users interested in multiple topics (cross-interest analysis)
MATCH (u:User)-[:INTERESTED_IN]->(t:Topic)
WITH u, collect(t.name) AS topics
WHERE size(topics) >= 3
RETURN u.telegram_user_id, topics
```

---

## Security & Compliance

| Principle | Implementation |
|---|---|
| **No credentials in code** | All secrets loaded from `.env` via python-dotenv |
| **No private data scraped** | Only public channels accessed |
| **Session isolation** | `.session` file stored locally, never shared |
| **Data minimization** | Only text content and public profile fields stored |
| **Rate limit compliance** | `wait_time=2` between API calls; exponential backoff on FloodWait |
| **Telegram ToS** | Scraping public channels via official MTProto API is authorized |

---

## Environment Variables Reference

```env
# Telegram (from my.telegram.org)
TELEGRAM_API_ID=<numeric app ID>
TELEGRAM_API_HASH=<32-char hex string>
TELEGRAM_PHONE=<+country_code_number>

# Supabase
SUPABASE_URL=https://<project>.supabase.co
SUPABASE_SERVICE_ROLE_KEY=<jwt_service_role_key>
SUPABASE_ANON_KEY=<jwt_anon_key>

# Neo4j AuraDB
NEO4J_URI=neo4j+s://<instance>.databases.neo4j.io
NEO4J_USERNAME=<username>
NEO4J_PASSWORD=<password>
NEO4J_DATABASE=<database_name>

# OpenAI
OpenAI_API=sk-proj-<key>
```

---

## Roadmap

| Phase | Feature | Status |
|---|---|---|
| ✅ v1.0 | Core scraper + AI processor + Neo4j ingestion | **Complete** |
| 🔄 v1.1 | Multi-account session rotation for higher throughput | Planned |
| 🔄 v1.2 | User bio enrichment (GetFullUser) for better demographics | Planned |
| 🔄 v1.3 | Real-time dashboard integration via Neo4j Bloom | Planned |
| 🔄 v2.0 | Cross-channel user linkage (same user, multiple channels) | Planned |
| 🔄 v2.1 | Influence scoring (centrality analysis in Neo4j) | Planned |
| 🔄 v2.2 | Anomaly detection (sudden topic spikes, bot detection) | Planned |

---

*Built with Telethon · Supabase · OpenAI GPT-4o-mini · Neo4j AuraDB*
