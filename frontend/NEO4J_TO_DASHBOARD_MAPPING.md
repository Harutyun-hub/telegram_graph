# Neo4j to Dashboard Widget Mapping Guide
## Community Intelligence Platform - Data Wiring Strategy

**Date:** February 24, 2026
**Purpose:** Map every dashboard widget to its Neo4j data source, define the required schema, Cypher queries, Supabase edge function endpoints, and transformation logic.

---

## Table of Contents

1. [Schema Evolution: Old vs New](#1-schema-evolution)
2. [Proposed Neo4j Schema for Community Intelligence](#2-proposed-schema)
3. [GPT-4o-mini Extraction Pipeline](#3-extraction-pipeline)
4. [Data Flow Architecture](#4-data-flow)
5. [Widget-to-Query Mapping (All 32 Widgets)](#5-widget-mapping)
6. [Supabase Edge Function Endpoints](#6-endpoints)
7. [Implementation Phases](#7-phases)

---

## 1. Schema Evolution: Old vs New <a id="1-schema-evolution"></a>

### OLD Schema (Competitive Ad Intelligence)
```
(:Brand)-[:PUBLISHED]->(:Ad)-[:COVERS_TOPIC]->(:Topic)
                        |
                  [:HAS_SENTIMENT]->(:Sentiment)
```
- Designed for: Armenian bank advertising analysis
- 4 node types: Brand, Ad, Topic, Sentiment
- 3 relationship types: PUBLISHED, COVERS_TOPIC, HAS_SENTIMENT

### NEW Schema (Community Intelligence)
The platform now analyzes **Telegram community conversations** (not ads). We need a fundamentally different graph model that captures:
- Who is talking (authors, personas)
- Where they talk (channels)
- What they say (topics, problems, questions, recommendations)
- How they feel (sentiment, mood, intent)
- Where they are (locations, origin cities)
- What they care about (interests)

---

## 2. Proposed Neo4j Schema for Community Intelligence <a id="2-proposed-schema"></a>

### 2.1 Node Types (12)

```cypher
// ─── PEOPLE ───
(:Author {
  id: string,              // Telegram user ID (hashed for privacy)
  handle: string,          // @username or display name
  firstSeen: datetime,     // When they first appeared
  lastSeen: datetime,      // Most recent message
  messageCount: int,       // Total messages
  helpScore: float,        // How helpful they are (0-100)
  role: string             // "member" | "moderator" | "admin" | "bot"
})

(:Persona {
  id: string,              // "persona_it_relocant"
  name: string,            // "IT Relocant"
  description: string,     // "Tech worker who relocated for remote work"
  estimatedShare: float,   // 0.32 = 32% of community
  topInterests: string[],  // ["coworking", "visa", "apartments"]
  avgIntegration: string   // "bilingual_bubble"
})

// ─── CONTENT ───
(:Message {
  id: string,              // Telegram message ID
  text: string,            // Message content (or summary)
  timestamp: datetime,     // When posted
  replyCount: int,         // Number of replies
  reactionCount: int,      // Emoji reactions
  forwardCount: int,       // Times forwarded
  language: string,        // "ru" | "en" | "hy" | "mixed"
  messageType: string      // "question" | "recommendation" | "complaint" | "info" | "discussion"
})

(:Channel {
  id: string,              // Telegram channel/group ID
  name: string,            // "Русские в Ереване"
  memberCount: int,        // Total members
  dailyMessages: float,    // Average daily message volume
  type: string,            // "group" | "channel" | "supergroup"
  category: string,        // "general" | "housing" | "jobs" | "expat" | "marketplace"
  engagementRate: float    // replies+reactions / messages
})

// ─── EXTRACTED ENTITIES ───
(:Topic {
  id: string,              // "topic_housing"
  name: string,            // "Housing & Rent"
  category: string,        // "living" | "work" | "family" | "finance" | "lifestyle" | "integration"
  mentionCount: int,       // Total mentions across all messages
  weeklyTrend: float       // +0.15 = 15% increase this week
})

(:Problem {
  id: string,              // "problem_high_rent"
  name: string,            // "High Rent Prices"
  severity: string,        // "critical" | "high" | "medium" | "low"
  category: string,        // "housing" | "bureaucracy" | "services" | "language" | "safety"
  mentionCount: int,
  resolved: boolean        // Whether community has found solutions
})

(:Question {
  id: string,              // "question_how_open_bank_account"
  text: string,            // "How do I open a bank account without Armenian?"
  category: string,        // "getting_started" | "daily_life" | "work" | "family"
  answerCount: int,        // How many replies answered it
  isAnswered: boolean,
  firstAsked: datetime,
  lastAsked: datetime
})

(:Recommendation {
  id: string,              // "rec_green_bean_cafe"
  name: string,            // "Green Bean Cafe"
  category: string,        // "food" | "service" | "healthcare" | "housing" | "education"
  recommendCount: int,     // Times recommended
  avgRating: float,        // Community consensus rating
  location: string         // "Yerevan, Kentron"
})

(:Sentiment {
  id: string,              // "sentiment_positive"
  label: string,           // "positive" | "negative" | "neutral" | "mixed"
  emotion: string          // "excited" | "frustrated" | "grateful" | "anxious" | "satisfied"
})

(:Intent {
  id: string,              // "intent_settling"
  name: string,            // "Settling In"
  stage: string,           // "researching" | "planning" | "arriving" | "settling" | "established"
  description: string
})

(:Location {
  id: string,              // "loc_yerevan_kentron"
  name: string,            // "Kentron"
  type: string,            // "neighborhood" | "city" | "country"
  parentLocation: string,  // "Yerevan"
  avgRent: float,          // Average monthly rent mentioned
  satisfactionScore: float // Community satisfaction 0-100
})

(:Interest {
  id: string,              // "interest_hiking"
  name: string,            // "Hiking & Outdoor"
  category: string,        // "outdoor" | "food" | "tech" | "culture" | "fitness" | "nightlife" | "kids"
  mentionCount: int,
  weeklyTrend: float
})
```

### 2.2 Relationship Types (16)

```cypher
// ─── AUTHORSHIP ───
(Author)-[:WROTE]->(Message)
(Author)-[:ACTIVE_IN]->(Channel)           // Author participates in channel
(Author)-[:REPLIED_TO]->(Message)          // Reply chain tracking

// ─── CONTENT LOCATION ───
(Message)-[:POSTED_IN]->(Channel)

// ─── GPT-4o-mini EXTRACTIONS ───
(Message)-[:DISCUSSES]->(Topic)            // Message is about this topic
(Message)-[:REPORTS_PROBLEM]->(Problem)    // Message describes this problem
(Message)-[:ASKS]->(Question)              // Message contains this question
(Message)-[:RECOMMENDS]->(Recommendation)  // Message recommends this
(Message)-[:HAS_SENTIMENT]->(Sentiment)    // Message has this sentiment/emotion
(Message)-[:SHOWS_INTENT]->(Intent)        // Message reveals this intent

// ─── PERSON ATTRIBUTES ───
(Author)-[:BELONGS_TO]->(Persona)          // Behavioral cluster assignment
(Author)-[:FROM]->(Location)               // Origin city
(Author)-[:LIVES_IN]->(Location)           // Current location
(Author)-[:INTERESTED_IN]->(Interest)      // Expressed interests

// ─── CROSS-ENTITY ───
(Question)-[:ABOUT]->(Topic)               // Question relates to topic
(Problem)-[:RELATED_TO]->(Topic)           // Problem relates to topic
(Recommendation)-[:FOR]->(Topic)           // Recommendation is for this topic area
```

### 2.3 Neo4j Index Strategy

```cypher
// Performance-critical indexes
CREATE INDEX msg_timestamp FOR (m:Message) ON (m.timestamp);
CREATE INDEX msg_type FOR (m:Message) ON (m.messageType);
CREATE INDEX author_id FOR (a:Author) ON (a.id);
CREATE INDEX channel_id FOR (c:Channel) ON (c.id);
CREATE INDEX topic_name FOR (t:Topic) ON (t.name);
CREATE INDEX topic_category FOR (t:Topic) ON (t.category);
CREATE INDEX problem_severity FOR (p:Problem) ON (p.severity);
CREATE INDEX location_type FOR (l:Location) ON (l.type);
CREATE INDEX persona_name FOR (p:Persona) ON (p.name);

// Full-text search for natural language queries
CREATE FULLTEXT INDEX message_text FOR (m:Message) ON EACH [m.text];
CREATE FULLTEXT INDEX question_text FOR (q:Question) ON EACH [q.text];
```

---

## 3. GPT-4o-mini Extraction Pipeline <a id="3-extraction-pipeline"></a>

### 3.1 What GPT-4o-mini Extracts Per Message

For every Telegram message, the extraction pipeline should output:

```json
{
  "message_id": "msg_12345",
  "channel_id": "ch_001",
  "author_id": "auth_hash_abc",
  "timestamp": "2026-02-24T14:30:00Z",
  "text": "Кто-нибудь знает хорошего русскоязычного стоматолога в Кентроне?",
  
  "extractions": {
    "topics": ["healthcare", "dental", "russian_speaking_services"],
    "message_type": "question",
    "question": "Where to find Russian-speaking dentist in Kentron?",
    "question_category": "daily_life",
    "problems": [],
    "recommendations": [],
    "sentiment": "neutral",
    "emotion": "seeking_help",
    "intent": "settling",
    "intent_stage": "settling",
    "locations_mentioned": ["Kentron"],
    "interests": ["healthcare"],
    "language": "ru",
    
    "persona_signals": {
      "likely_persona": "young_family",
      "confidence": 0.7,
      "signals": ["seeking family services", "specific neighborhood"]
    }
  }
}
```

### 3.2 GPT-4o-mini System Prompt (Extraction)

```
You are a community intelligence analyst for a Russian-speaking expat community in Armenia.
For each Telegram message, extract:

1. TOPICS (1-3): From this list: housing, education, healthcare, banking, jobs, visa, 
   language, food, transport, coworking, kids, nightlife, legal, safety, shopping, 
   culture, tech, fitness, nature, volunteering. Add new ones if needed.

2. MESSAGE_TYPE: question | recommendation | complaint | info_sharing | discussion | greeting

3. PROBLEMS (0-2): Only if the message describes a pain point. Include severity (critical/high/medium/low).

4. QUESTIONS (0-1): Only if the message asks something. Normalize to a canonical form.

5. RECOMMENDATIONS (0-2): Only if recommending a specific place, service, or person.

6. SENTIMENT: positive | negative | neutral | mixed
   EMOTION: excited | satisfied | grateful | neutral | frustrated | anxious | angry | confused

7. INTENT & STAGE:
   - researching: Asking about Armenia before arriving
   - planning: Making concrete plans to move
   - arriving: First 1-2 weeks questions
   - settling: 1-3 months, establishing routines
   - established: 3+ months, helping others

8. PERSONA SIGNALS: Which persona does this message suggest?
   - it_relocant: Tech worker, remote work, coworking
   - young_family: Kids, schools, pediatricians
   - digital_nomad: Short-term, lifestyle, cafes
   - entrepreneur: Business, registration, hiring
   - established_expat: Helping others, local knowledge
   - retiree: Healthcare, quiet life, cost of living

Respond in JSON format only.
```

### 3.3 Ingestion to Neo4j

After extraction, each message creates/updates these nodes and relationships:

```cypher
// 1. Create/update Author
MERGE (a:Author {id: $authorId})
SET a.lastSeen = datetime($timestamp),
    a.messageCount = COALESCE(a.messageCount, 0) + 1

// 2. Create Message
CREATE (m:Message {
  id: $messageId,
  text: $text,
  timestamp: datetime($timestamp),
  messageType: $messageType,
  language: $language
})

// 3. Link Author -> Message -> Channel
MERGE (ch:Channel {id: $channelId})
CREATE (a)-[:WROTE]->(m)
CREATE (m)-[:POSTED_IN]->(ch)
MERGE (a)-[:ACTIVE_IN]->(ch)

// 4. Create Topic links
UNWIND $topics AS topicName
MERGE (t:Topic {name: topicName})
ON CREATE SET t.id = 'topic_' + replace(toLower(topicName), ' ', '_'),
             t.mentionCount = 1
ON MATCH SET t.mentionCount = t.mentionCount + 1
CREATE (m)-[:DISCUSSES]->(t)

// 5. Create Problem links (if any)
UNWIND $problems AS prob
MERGE (p:Problem {name: prob.name})
ON CREATE SET p.id = 'problem_' + replace(toLower(prob.name), ' ', '_'),
             p.severity = prob.severity,
             p.mentionCount = 1
ON MATCH SET p.mentionCount = p.mentionCount + 1
CREATE (m)-[:REPORTS_PROBLEM]->(p)

// 6. Create Question links (if any)
// ... similar MERGE pattern

// 7. Create Sentiment link
MERGE (s:Sentiment {label: $sentiment})
CREATE (m)-[:HAS_SENTIMENT {emotion: $emotion}]->(s)

// 8. Create Intent link
MERGE (i:Intent {name: $intentStage})
CREATE (m)-[:SHOWS_INTENT]->(i)

// 9. Update Persona assignment (periodic, not per-message)
// Run as batch job after accumulating enough signals per author
```

---

## 4. Data Flow Architecture <a id="4-data-flow"></a>

```
┌─────────────────────────────────────────────────────┐
│  TELEGRAM CHANNELS (source)                          │
│  ~20 channels, ~5000 messages/day                    │
└──────────────┬──────────────────────────────────────┘
               │ Telegram Bot API / scraper
               ▼
┌─────────────────────────────────────────────────────┐
│  GPT-4o-mini EXTRACTION PIPELINE                     │
│  - Batch: every 15 min or on-demand                  │
│  - Input: raw message text                           │
│  - Output: structured JSON (topics, sentiment, etc.) │
│  - Cost: ~$0.15/1000 messages                        │
└──────────────┬──────────────────────────────────────┘
               │ Structured JSON
               ▼
┌─────────────────────────────────────────────────────┐
│  NEO4J (graph database)                              │
│  - 12 node types, 16 relationship types              │
│  - AuraDB Free or self-hosted                        │
│  - ~100K nodes, ~500K relationships (at scale)       │
└──────────────┬──────────────────────────────────────┘
               │ Cypher queries
               ▼
┌─────────────────────────────────────────────────────┐
│  SUPABASE EDGE FUNCTIONS                             │
│  /make-server-14007ead/...                           │
│  - Each widget has 1-2 dedicated endpoints           │
│  - Executes Cypher, transforms data, returns JSON    │
│  - Caching: 5-15 min for expensive queries           │
└──────────────┬──────────────────────────────────────┘
               │ JSON API responses
               ▼
┌─────────────────────────────────────────────────────┐
│  REACT DASHBOARD (frontend)                          │
│  - 32 widgets in 8 tiers                             │
│  - Each widget calls its API endpoint on mount       │
│  - GlobalFiltersLight passes channel/time filters    │
│  - Recharts for visualization                        │
└─────────────────────────────────────────────────────┘
```

---

## 5. Widget-to-Query Mapping (All 32 Widgets) <a id="5-widget-mapping"></a>

### TIER 1: Community Pulse

---

#### W1: CommunityHealthScore
**What it shows:** Composite score (0-100) from engagement, growth, sentiment, velocity.
**Update frequency:** Every 15 min

**Cypher Queries:**

```cypher
// Engagement Rate (replies+reactions / total messages, last 7 days)
MATCH (m:Message)
WHERE m.timestamp > datetime() - duration('P7D')
WITH count(m) AS totalMessages,
     sum(m.replyCount) AS totalReplies,
     sum(m.reactionCount) AS totalReactions
RETURN toFloat(totalReplies + totalReactions) / totalMessages * 100 AS engagementRate

// Community Growth (new authors this week vs last week)
MATCH (a:Author)
WHERE a.firstSeen > datetime() - duration('P7D')
WITH count(a) AS newThisWeek
MATCH (a2:Author)
WHERE a2.firstSeen > datetime() - duration('P14D')
  AND a2.firstSeen <= datetime() - duration('P7D')
WITH newThisWeek, count(a2) AS newLastWeek
RETURN newThisWeek, newLastWeek,
       CASE WHEN newLastWeek > 0 
            THEN toFloat(newThisWeek - newLastWeek) / newLastWeek * 100 
            ELSE 0 END AS growthPct

// Positive Sentiment (% of positive messages, last 7 days)
MATCH (m:Message)-[:HAS_SENTIMENT]->(s:Sentiment)
WHERE m.timestamp > datetime() - duration('P7D')
WITH count(m) AS total,
     sum(CASE WHEN s.label = 'positive' THEN 1 ELSE 0 END) AS positive
RETURN toFloat(positive) / total * 100 AS positivePct

// Content Velocity (messages per day, this week vs last week)
MATCH (m:Message)
WHERE m.timestamp > datetime() - duration('P7D')
WITH count(m) / 7.0 AS msgsPerDayThisWeek
MATCH (m2:Message)
WHERE m2.timestamp > datetime() - duration('P14D')
  AND m2.timestamp <= datetime() - duration('P7D')
WITH msgsPerDayThisWeek, count(m2) / 7.0 AS msgsPerDayLastWeek
RETURN msgsPerDayThisWeek, msgsPerDayLastWeek
```

**Composite formula:**
```
healthScore = (engagementRate * 0.30) + (growthScore * 0.25) + (positivePct * 0.25) + (velocityScore * 0.20)
```
Where growthScore and velocityScore are normalized to 0-100 based on week-over-week delta.

**Edge function endpoint:** `GET /community-health`
**Response shape:**
```json
{
  "score": 71,
  "components": [
    {"label": "Engagement Rate", "value": 74, "trend": 6},
    {"label": "Community Growth", "value": 68, "trend": 12},
    {"label": "Positive Sentiment", "value": 62, "trend": 3},
    {"label": "Content Velocity", "value": 78, "trend": 8}
  ],
  "history": [{"time": "6h ago", "score": 64}, ...]
}
```

---

#### W2: TrendingTopicsFeed
**What it shows:** Top 7 trending topics with sample quotes, sentiment, category.
**Update frequency:** Every 15 min

```cypher
// Top trending topics by mention growth (this week vs last week)
MATCH (m:Message)-[:DISCUSSES]->(t:Topic)
WHERE m.timestamp > datetime() - duration('P7D')
WITH t, count(m) AS mentionsThisWeek,
     collect(m.text)[0..2] AS sampleQuotes
OPTIONAL MATCH (m2:Message)-[:DISCUSSES]->(t)
WHERE m2.timestamp > datetime() - duration('P14D')
  AND m2.timestamp <= datetime() - duration('P7D')
WITH t, mentionsThisWeek, sampleQuotes, count(m2) AS mentionsLastWeek
WITH t, mentionsThisWeek, sampleQuotes,
     CASE WHEN mentionsLastWeek > 0
          THEN toFloat(mentionsThisWeek - mentionsLastWeek) / mentionsLastWeek * 100
          ELSE 100 END AS growthPct

// Get dominant sentiment for each topic
OPTIONAL MATCH (m3:Message)-[:DISCUSSES]->(t)
WHERE m3.timestamp > datetime() - duration('P7D')
OPTIONAL MATCH (m3)-[:HAS_SENTIMENT]->(s:Sentiment)
WITH t, mentionsThisWeek, sampleQuotes, growthPct,
     s.label AS sentLabel, count(s) AS sentCount
ORDER BY sentCount DESC
WITH t, mentionsThisWeek, sampleQuotes, growthPct,
     collect(sentLabel)[0] AS dominantSentiment

ORDER BY growthPct DESC
LIMIT 7
RETURN t.name AS topic, t.category AS category,
       mentionsThisWeek AS mentions, growthPct AS growth,
       dominantSentiment AS sentiment, sampleQuotes
```

**Edge function endpoint:** `GET /trending-topics`

---

#### W3: CommunityBrief
**What it shows:** AI-generated natural language summary + 4 KPI cards.
**Update frequency:** Every hour (cached, expensive Gemini call)

```cypher
// Gather stats for Gemini to summarize
// Active members (posted in last 7 days)
MATCH (a:Author)-[:WROTE]->(m:Message)
WHERE m.timestamp > datetime() - duration('P7D')
WITH count(DISTINCT a) AS activeMembers

// Messages today
MATCH (m:Message)
WHERE m.timestamp > datetime() - duration('P1D')
WITH activeMembers, count(m) AS messagesToday

// Positive mood %
MATCH (m:Message)-[:HAS_SENTIMENT]->(s:Sentiment)
WHERE m.timestamp > datetime() - duration('P7D')
WITH activeMembers, messagesToday,
     toFloat(sum(CASE WHEN s.label = 'positive' THEN 1 ELSE 0 END)) / count(m) * 100 AS positivePct

// New members this week
MATCH (a:Author)
WHERE a.firstSeen > datetime() - duration('P7D')
WITH activeMembers, messagesToday, positivePct, count(a) AS newMembers

// Top 3 topics
MATCH (m2:Message)-[:DISCUSSES]->(t:Topic)
WHERE m2.timestamp > datetime() - duration('P7D')
WITH activeMembers, messagesToday, positivePct, newMembers,
     t.name AS topicName, count(m2) AS topicCount
ORDER BY topicCount DESC
LIMIT 3

RETURN activeMembers, messagesToday, positivePct, newMembers,
       collect(topicName) AS topTopics
```

Then pass this data to Gemini to generate a natural-language brief.

**Edge function endpoint:** `GET /community-brief`

---

### TIER 2: What People Talk About

---

#### W4: TopicLandscape
**What it shows:** Bubble visualization of all topics, sized by mention volume.

```cypher
MATCH (m:Message)-[:DISCUSSES]->(t:Topic)
WHERE m.timestamp > datetime() - duration('P30D')
WITH t, count(m) AS mentions
ORDER BY mentions DESC
LIMIT 20
RETURN t.name AS topic, t.category AS category, mentions
```

**Edge function endpoint:** `GET /topic-landscape`

---

#### W5: ConversationTrends
**What it shows:** Line chart of topic mention counts per week over 7 weeks.

```cypher
// For each of top 6 topics, get weekly mention counts
MATCH (m:Message)-[:DISCUSSES]->(t:Topic)
WHERE m.timestamp > datetime() - duration('P49D')
WITH t, 
     duration.between(m.timestamp, datetime()).weeks AS weeksAgo,
     count(m) AS mentions
WHERE weeksAgo <= 6
WITH t.name AS topic, weeksAgo, mentions
ORDER BY weeksAgo

// Pivot: Return as {topic: "housing", data: [{week: 0, count: 120}, ...]}
RETURN topic, collect({week: weeksAgo, count: mentions}) AS weeklyData
```

**Edge function endpoint:** `GET /conversation-trends?weeks=7&topN=6`

---

#### W6: ContentEngagementHeatmap
**What it shows:** Matrix of content_type x topic with average engagement.

```cypher
MATCH (m:Message)-[:DISCUSSES]->(t:Topic)
WHERE m.timestamp > datetime() - duration('P30D')
WITH t.name AS topic, m.messageType AS contentType,
     avg(m.replyCount + m.reactionCount) AS avgEngagement,
     count(m) AS volume
WHERE volume >= 3  // Minimum sample size
RETURN contentType, topic, avgEngagement, volume
ORDER BY avgEngagement DESC
```

**Edge function endpoint:** `GET /engagement-heatmap`

---

#### W7: QuestionCloud
**What it shows:** Most asked questions grouped by category with answered/unanswered status.

```cypher
MATCH (m:Message)-[:ASKS]->(q:Question)
WHERE m.timestamp > datetime() - duration('P30D')
OPTIONAL MATCH (reply:Message)-[:REPLIED_TO]->(m)
WITH q, count(DISTINCT m) AS timesAsked,
     count(DISTINCT reply) AS answerCount,
     q.category AS category
RETURN q.text AS question, category, timesAsked,
       answerCount, answerCount > 0 AS isAnswered
ORDER BY timesAsked DESC
LIMIT 20
```

**Edge function endpoint:** `GET /top-questions`

---

### TIER 3: Problems & Satisfaction

---

#### W8: ProblemTracker
**What it shows:** Categorized pain points with severity, mentions, sample quotes.

```cypher
MATCH (m:Message)-[:REPORTS_PROBLEM]->(p:Problem)
WHERE m.timestamp > datetime() - duration('P30D')
WITH p, count(m) AS mentions,
     collect(m.text)[0..2] AS sampleQuotes

// Get trend (this week vs last week)
OPTIONAL MATCH (m2:Message)-[:REPORTS_PROBLEM]->(p)
WHERE m2.timestamp > datetime() - duration('P7D')
WITH p, mentions, sampleQuotes, count(m2) AS mentionsThisWeek
OPTIONAL MATCH (m3:Message)-[:REPORTS_PROBLEM]->(p)
WHERE m3.timestamp > datetime() - duration('P14D')
  AND m3.timestamp <= datetime() - duration('P7D')
WITH p, mentions, sampleQuotes, mentionsThisWeek, count(m3) AS mentionsLastWeek,
     CASE WHEN mentionsLastWeek > 0
          THEN toFloat(mentionsThisWeek - mentionsLastWeek) / mentionsLastWeek * 100
          ELSE 0 END AS trendPct

ORDER BY mentions DESC
LIMIT 12
RETURN p.name AS problem, p.category AS category,
       p.severity AS severity, mentions, trendPct, sampleQuotes
```

**Edge function endpoint:** `GET /problems`

---

#### W9: ServiceGapDetector
**What it shows:** Unmet service needs (high demand, low supply of recommendations).

```cypher
// Find topics with many questions but few recommendations
MATCH (m:Message)-[:ASKS]->(q:Question)-[:ABOUT]->(t:Topic)
WHERE m.timestamp > datetime() - duration('P30D')
WITH t, count(DISTINCT q) AS demandCount

OPTIONAL MATCH (m2:Message)-[:RECOMMENDS]->(r:Recommendation)-[:FOR]->(t)
WHERE m2.timestamp > datetime() - duration('P30D')
WITH t, demandCount, count(DISTINCT r) AS supplyCount

WITH t, demandCount, supplyCount,
     toFloat(demandCount - supplyCount) / demandCount * 100 AS gapPct
WHERE demandCount > 3
ORDER BY gapPct DESC
LIMIT 10

RETURN t.name AS topic, demandCount AS demand,
       supplyCount AS supply, gapPct AS gap
```

**Edge function endpoint:** `GET /service-gaps`

---

#### W10: SatisfactionByArea
**What it shows:** Life areas ranked by sentiment satisfaction score.

```cypher
// For each life-area topic, calculate positive sentiment percentage
MATCH (m:Message)-[:DISCUSSES]->(t:Topic)-[:HAS_SENTIMENT]->(s:Sentiment)
WHERE m.timestamp > datetime() - duration('P30D')
  AND t.category IN ['living', 'work', 'family', 'finance', 'lifestyle', 'integration']
WITH t.name AS area,
     count(m) AS total,
     sum(CASE WHEN s.label = 'positive' THEN 1 ELSE 0 END) AS positive,
     sum(CASE WHEN s.label = 'negative' THEN 1 ELSE 0 END) AS negative
WITH area, total,
     toFloat(positive) / total * 100 AS satisfactionPct
ORDER BY satisfactionPct DESC
RETURN area, satisfactionPct, total
```

**Note:** The `(m)-[:HAS_SENTIMENT]->(s)` path is on Message, so the join is:
```cypher
MATCH (m:Message)-[:DISCUSSES]->(t:Topic)
MATCH (m)-[:HAS_SENTIMENT]->(s:Sentiment)
```

**Edge function endpoint:** `GET /satisfaction-by-area`

---

#### W11: MoodOverTime
**What it shows:** Stacked area chart of 5 emotional states over 7 weeks.

```cypher
MATCH (m:Message)-[hs:HAS_SENTIMENT]->(s:Sentiment)
WHERE m.timestamp > datetime() - duration('P49D')
WITH duration.between(m.timestamp, datetime()).weeks AS weeksAgo,
     hs.emotion AS emotion,
     count(m) AS msgCount
WHERE weeksAgo <= 6
  AND emotion IN ['excited', 'satisfied', 'neutral', 'frustrated', 'anxious']
RETURN weeksAgo AS week, emotion, msgCount
ORDER BY weeksAgo, emotion
```

**Edge function endpoint:** `GET /mood-over-time?weeks=7`

---

### TIER 4: Channels, Voices & Activity

---

#### W12: TopChannels
**What it shows:** Top 8 channels ranked by engagement rate.

```cypher
MATCH (m:Message)-[:POSTED_IN]->(ch:Channel)
WHERE m.timestamp > datetime() - duration('P7D')
WITH ch, count(m) AS messagesThisWeek,
     avg(m.replyCount + m.reactionCount) AS avgEngagement

// Get member count and growth
MATCH (a:Author)-[:ACTIVE_IN]->(ch)
WITH ch, messagesThisWeek, avgEngagement,
     count(DISTINCT a) AS activeMembers

OPTIONAL MATCH (a2:Author)-[:ACTIVE_IN]->(ch)
WHERE a2.firstSeen > datetime() - duration('P7D')
WITH ch, messagesThisWeek, avgEngagement, activeMembers,
     count(DISTINCT a2) AS newMembersThisWeek

ORDER BY avgEngagement DESC
LIMIT 8
RETURN ch.name AS channel, ch.category AS category, ch.type AS type,
       ch.memberCount AS totalMembers, activeMembers,
       messagesThisWeek, avgEngagement AS engagementRate,
       newMembersThisWeek AS growth
```

**Edge function endpoint:** `GET /top-channels`

---

#### W13: KeyVoices
**What it shows:** 6 most influential community members with help scores.

```cypher
MATCH (a:Author)-[:WROTE]->(m:Message)
WHERE m.timestamp > datetime() - duration('P30D')
WITH a, count(m) AS totalPosts,
     sum(m.replyCount) AS totalReplies,
     sum(m.reactionCount) AS totalReactions

// Calculate help score (replies to others' questions)
OPTIONAL MATCH (a)-[:REPLIED_TO]->(q:Message {messageType: 'question'})
WHERE q.timestamp > datetime() - duration('P30D')
WITH a, totalPosts, totalReplies, totalReactions,
     count(DISTINCT q) AS questionsAnswered

// Get their top topics
OPTIONAL MATCH (a)-[:WROTE]->(m2:Message)-[:DISCUSSES]->(t:Topic)
WITH a, totalPosts, totalReplies, totalReactions, questionsAnswered,
     t.name AS topicName, count(m2) AS topicCount
ORDER BY topicCount DESC
WITH a, totalPosts, totalReplies, totalReactions, questionsAnswered,
     collect(topicName)[0..3] AS topTopics

// Help score = weighted combination
WITH a, totalPosts, totalReplies, totalReactions, questionsAnswered, topTopics,
     (questionsAnswered * 3 + totalReplies * 0.1 + totalReactions * 0.05) AS helpScore

ORDER BY helpScore DESC
LIMIT 6
RETURN a.handle AS name, a.role AS role,
       totalPosts, questionsAnswered, helpScore, topTopics
```

**Edge function endpoint:** `GET /key-voices`

---

#### W23: ActivityTimeline
**What it shows:** Message volume by hour-of-day and day-of-week.

```cypher
// By hour of day
MATCH (m:Message)
WHERE m.timestamp > datetime() - duration('P7D')
WITH m.timestamp.hour AS hour, count(m) AS msgCount
RETURN hour, msgCount
ORDER BY hour

// By day of week
MATCH (m:Message)
WHERE m.timestamp > datetime() - duration('P30D')
WITH m.timestamp.dayOfWeek AS dayOfWeek, count(m) AS msgCount
RETURN dayOfWeek, msgCount
ORDER BY dayOfWeek
```

**Edge function endpoint:** `GET /activity-patterns`

---

#### W24: RecommendationTracker
**What it shows:** Top 8 most recommended places/services.

```cypher
MATCH (m:Message)-[:RECOMMENDS]->(r:Recommendation)
WHERE m.timestamp > datetime() - duration('P30D')
WITH r, count(m) AS recommendCount,
     collect(m.text)[0] AS sampleQuote
ORDER BY recommendCount DESC
LIMIT 8
RETURN r.name AS name, r.category AS category,
       r.avgRating AS rating, recommendCount, sampleQuote
```

**Edge function endpoint:** `GET /top-recommendations`

---

#### W25: NewcomerFlow
**What it shows:** Journey stages with question examples and answer rates.

```cypher
// Count authors at each intent stage
MATCH (a:Author)-[:WROTE]->(m:Message)-[:SHOWS_INTENT]->(i:Intent)
WHERE m.timestamp > datetime() - duration('P30D')
WITH i.stage AS stage, count(DISTINCT a) AS authorCount

// Get sample questions per stage
OPTIONAL MATCH (m2:Message)-[:SHOWS_INTENT]->(i2:Intent {stage: stage})
WHERE m2.messageType = 'question'
  AND m2.timestamp > datetime() - duration('P30D')
OPTIONAL MATCH (reply:Message)-[:REPLIED_TO]->(m2)
WITH stage, authorCount,
     collect(m2.text)[0..3] AS sampleQuestions,
     CASE WHEN count(m2) > 0
          THEN toFloat(sum(CASE WHEN reply IS NOT NULL THEN 1 ELSE 0 END)) / count(m2) * 100
          ELSE 0 END AS answerRate

RETURN stage, authorCount, sampleQuestions, answerRate
ORDER BY CASE stage
  WHEN 'researching' THEN 1
  WHEN 'planning' THEN 2
  WHEN 'arriving' THEN 3
  WHEN 'settling' THEN 4
  WHEN 'established' THEN 5
END
```

**Edge function endpoint:** `GET /newcomer-journey`

---

### TIER 5: Who Are They

---

#### W14: PersonaGallery
**What it shows:** 6 persona clusters with population share and details.

```cypher
MATCH (a:Author)-[:BELONGS_TO]->(p:Persona)
WITH p, count(a) AS memberCount
WITH sum(memberCount) AS totalMembers, collect({persona: p, count: memberCount}) AS personas
UNWIND personas AS pc
WITH pc.persona AS p, pc.count AS memberCount, totalMembers

// Get top interests per persona
OPTIONAL MATCH (a:Author)-[:BELONGS_TO]->(p)
OPTIONAL MATCH (a)-[:INTERESTED_IN]->(i:Interest)
WITH p, memberCount, totalMembers, i.name AS interest, count(a) AS intCount
ORDER BY intCount DESC
WITH p, memberCount, totalMembers, collect(interest)[0..5] AS topInterests

RETURN p.name AS persona, p.description AS description,
       memberCount, toFloat(memberCount) / totalMembers * 100 AS sharePct,
       topInterests
ORDER BY memberCount DESC
```

**Edge function endpoint:** `GET /personas`

---

#### W15: InterestRadar
**What it shows:** Radar chart of 8 interest categories.

```cypher
MATCH (a:Author)-[:INTERESTED_IN]->(i:Interest)
WITH i.category AS category, count(DISTINCT a) AS authorCount
WITH collect({category: category, count: authorCount}) AS interests,
     max(authorCount) AS maxCount
UNWIND interests AS int
RETURN int.category AS category,
       toFloat(int.count) / maxCount * 100 AS normalizedScore,
       int.count AS rawCount
ORDER BY normalizedScore DESC
```

**Edge function endpoint:** `GET /interest-radar`

---

#### W16: OriginMap
**What it shows:** Where community members came from (origin cities).

```cypher
MATCH (a:Author)-[:FROM]->(l:Location)
WHERE l.type = 'city'
WITH l.name AS city, count(a) AS memberCount
WITH sum(memberCount) AS total, collect({city: city, count: memberCount}) AS cities
UNWIND cities AS c
RETURN c.city AS city, c.count AS members,
       toFloat(c.count) / total * 100 AS sharePct
ORDER BY c.count DESC
LIMIT 10
```

**Edge function endpoint:** `GET /origin-cities`

---

#### W17: IntegrationSpectrum
**What it shows:** Distribution across integration levels.

```cypher
// Infer integration level from language usage and content patterns
MATCH (a:Author)-[:WROTE]->(m:Message)
WHERE m.timestamp > datetime() - duration('P30D')
WITH a,
     sum(CASE WHEN m.language = 'hy' THEN 1 ELSE 0 END) AS armenianMsgs,
     sum(CASE WHEN m.language = 'mixed' THEN 1 ELSE 0 END) AS mixedMsgs,
     sum(CASE WHEN m.language = 'ru' THEN 1 ELSE 0 END) AS russianMsgs,
     count(m) AS totalMsgs
WITH a,
     CASE
       WHEN toFloat(armenianMsgs) / totalMsgs > 0.3 THEN 'fully_integrated'
       WHEN toFloat(mixedMsgs) / totalMsgs > 0.3 THEN 'learning_mixing'
       WHEN toFloat(armenianMsgs + mixedMsgs) / totalMsgs > 0.1 THEN 'bilingual_bubble'
       ELSE 'russian_only'
     END AS integrationLevel
WITH integrationLevel, count(a) AS memberCount
RETURN integrationLevel, memberCount
```

**Edge function endpoint:** `GET /integration-spectrum`

---

#### W18: LocationDistribution
**What it shows:** Where people live now in Armenia, with satisfaction and rent.

```cypher
MATCH (a:Author)-[:LIVES_IN]->(l:Location)
WHERE l.type = 'neighborhood'
WITH l, count(a) AS residents

// Get satisfaction from sentiment of messages mentioning this location
OPTIONAL MATCH (m:Message)-[:DISCUSSES]->(t:Topic)
WHERE m.text CONTAINS l.name
  AND m.timestamp > datetime() - duration('P30D')
OPTIONAL MATCH (m)-[:HAS_SENTIMENT]->(s:Sentiment)
WITH l, residents,
     toFloat(sum(CASE WHEN s.label = 'positive' THEN 1 ELSE 0 END)) / 
       CASE WHEN count(s) > 0 THEN count(s) ELSE 1 END * 100 AS satisfactionPct

RETURN l.name AS area, l.parentLocation AS city,
       residents, satisfactionPct,
       l.avgRent AS avgRent
ORDER BY residents DESC
LIMIT 10
```

**Edge function endpoint:** `GET /location-distribution`

---

### TIER 6: Growth, Retention & Journey

---

#### W19: EmergingInterests
**What it shows:** Topics that appeared < 14 days ago with growth rates.

```cypher
// Find topics with first mention in last 14 days
MATCH (m:Message)-[:DISCUSSES]->(t:Topic)
WITH t, min(m.timestamp) AS firstMention, count(m) AS totalMentions
WHERE firstMention > datetime() - duration('P14D')
  AND totalMentions >= 3  // Minimum threshold

// Calculate daily growth rate
WITH t, firstMention, totalMentions,
     duration.between(firstMention, datetime()).days AS daysOld
WITH t, totalMentions, daysOld,
     toFloat(totalMentions) / CASE WHEN daysOld > 0 THEN daysOld ELSE 1 END AS mentionsPerDay

// Get origin channel
OPTIONAL MATCH (m2:Message)-[:DISCUSSES]->(t)
OPTIONAL MATCH (m2)-[:POSTED_IN]->(ch:Channel)
WITH t, totalMentions, daysOld, mentionsPerDay,
     ch.name AS originChannel, count(m2) AS channelCount
ORDER BY channelCount DESC
WITH t, totalMentions, daysOld, mentionsPerDay,
     collect(originChannel)[0] AS topChannel

ORDER BY mentionsPerDay DESC
LIMIT 6
RETURN t.name AS topic, totalMentions, daysOld,
       mentionsPerDay, topChannel
```

**Edge function endpoint:** `GET /emerging-interests`

---

#### W20: RetentionRiskGauge
**What it shows:** Retention factors and churn signals.

```cypher
// Churn signals: Authors who were active but stopped
MATCH (a:Author)
WHERE a.lastSeen < datetime() - duration('P14D')
  AND a.lastSeen > datetime() - duration('P60D')
  AND a.messageCount > 5  // Were somewhat active
WITH a

// Get their last messages to understand why they left
OPTIONAL MATCH (a)-[:WROTE]->(m:Message)
WITH a, m ORDER BY m.timestamp DESC LIMIT 1
OPTIONAL MATCH (m)-[:DISCUSSES]->(t:Topic)
OPTIONAL MATCH (m)-[:HAS_SENTIMENT]->(s:Sentiment)
WITH count(a) AS churnedCount,
     collect(t.name)[0..5] AS churnTopics,
     collect(s.label)[0..5] AS churnSentiments,
     collect(m.text)[0..3] AS lastMessages

RETURN churnedCount, churnTopics, churnSentiments, lastMessages
```

**Edge function endpoint:** `GET /retention-risk`

---

#### W21: CommunityGrowthFunnel
**What it shows:** 6-stage funnel (Joined -> Read -> Asked -> Helped -> Contributor -> Leader).

```cypher
// Total who ever joined
MATCH (a:Author) 
WITH count(a) AS joined

// Those who posted at least 1 message (read -> asked)
MATCH (a:Author) WHERE a.messageCount >= 1
WITH joined, count(a) AS posted

// Those who asked a question
MATCH (a:Author)-[:WROTE]->(m:Message {messageType: 'question'})
WITH joined, posted, count(DISTINCT a) AS asked

// Those who answered someone else's question
MATCH (a:Author)-[:REPLIED_TO]->(m:Message {messageType: 'question'})
WITH joined, posted, asked, count(DISTINCT a) AS helped

// Contributors (10+ messages)
MATCH (a:Author) WHERE a.messageCount >= 10
WITH joined, posted, asked, helped, count(a) AS contributors

// Leaders (50+ messages AND high help score)
MATCH (a:Author) WHERE a.messageCount >= 50 AND a.helpScore > 50
WITH joined, posted, asked, helped, contributors, count(a) AS leaders

RETURN joined, posted, asked, helped, contributors, leaders
```

**Edge function endpoint:** `GET /growth-funnel`

---

#### W22: DecisionStageTracker
**What it shows:** Members at each journey stage.

```cypher
MATCH (a:Author)-[:WROTE]->(m:Message)-[:SHOWS_INTENT]->(i:Intent)
WHERE m.timestamp > datetime() - duration('P30D')
WITH a, i.stage AS stage, count(m) AS msgCount
// Assign author to their most frequent stage
ORDER BY msgCount DESC
WITH a, collect(stage)[0] AS primaryStage
WITH primaryStage, count(a) AS memberCount
RETURN primaryStage AS stage, memberCount
ORDER BY CASE primaryStage
  WHEN 'researching' THEN 1
  WHEN 'planning' THEN 2
  WHEN 'arriving' THEN 3
  WHEN 'settling' THEN 4
  WHEN 'established' THEN 5
END
```

**Edge function endpoint:** `GET /decision-stages`

---

### TIER 7: Business & Opportunity Intelligence

---

#### W26: BusinessOpportunityTracker
**What it shows:** Business ideas derived from unmet community needs.

```cypher
// Combine: high-demand topics + many problems + few recommendations = opportunity
MATCH (m:Message)-[:DISCUSSES]->(t:Topic)
WHERE m.timestamp > datetime() - duration('P30D')
WITH t, count(m) AS demand

OPTIONAL MATCH (m2:Message)-[:REPORTS_PROBLEM]->(p:Problem)-[:RELATED_TO]->(t)
WHERE m2.timestamp > datetime() - duration('P30D')
WITH t, demand, count(DISTINCT p) AS problemCount

OPTIONAL MATCH (m3:Message)-[:RECOMMENDS]->(r:Recommendation)-[:FOR]->(t)
WHERE m3.timestamp > datetime() - duration('P30D')
WITH t, demand, problemCount, count(DISTINCT r) AS existingSolutions

WITH t, demand, problemCount, existingSolutions,
     (demand * 0.4 + problemCount * 10 * 0.4 - existingSolutions * 5 * 0.2) AS opportunityScore
WHERE opportunityScore > 0
ORDER BY opportunityScore DESC
LIMIT 8

RETURN t.name AS opportunity, t.category AS category,
       demand, problemCount, existingSolutions, opportunityScore
```

**Edge function endpoint:** `GET /business-opportunities`

---

#### W27: JobMarketPulse
**What it shows:** Work-related discussion patterns.

```cypher
// Messages about jobs/work
MATCH (m:Message)-[:DISCUSSES]->(t:Topic)
WHERE t.category = 'work'
  AND m.timestamp > datetime() - duration('P30D')
WITH t.name AS workTopic, count(m) AS mentions,
     collect(m.messageType) AS types

// Breakdown of asking vs offering
WITH workTopic, mentions,
     size([t IN types WHERE t = 'question']) AS seekingCount,
     size([t IN types WHERE t = 'recommendation']) AS offeringCount

ORDER BY mentions DESC
LIMIT 8
RETURN workTopic, mentions, seekingCount, offeringCount
```

**Edge function endpoint:** `GET /job-market`

---

#### W28: HousingMarketPulse
**What it shows:** Housing types, prices mentioned, satisfaction, hot discussions.

```cypher
// Housing-related messages with price mentions
MATCH (m:Message)-[:DISCUSSES]->(t:Topic)
WHERE t.name IN ['housing', 'rent', 'apartments', 'buying_property']
  AND m.timestamp > datetime() - duration('P30D')

// Extract location context
OPTIONAL MATCH (m)-[:MENTIONS_LOCATION]->(l:Location)
WITH m, t, l.name AS area, l.avgRent AS avgRent

// Sentiment about housing
OPTIONAL MATCH (m)-[:HAS_SENTIMENT]->(s:Sentiment)
WITH t.name AS housingTopic, area, avgRent,
     count(m) AS mentions,
     toFloat(sum(CASE WHEN s.label = 'positive' THEN 1 ELSE 0 END)) / count(m) * 100 AS satisfactionPct

RETURN housingTopic, area, avgRent, mentions, satisfactionPct
ORDER BY mentions DESC
LIMIT 10
```

**Edge function endpoint:** `GET /housing-pulse`

---

### TIER 8: Performance & Analytics

---

#### W29: WeekOverWeekShifts
**What it shows:** 12 key metrics as delta cards comparing this week to last.

```cypher
// This is a compound query - run multiple simple counts
// Combine results in the edge function, not in a single Cypher

// Example for one metric: Active Members
MATCH (a:Author)-[:WROTE]->(m:Message)
WHERE m.timestamp > datetime() - duration('P7D')
WITH count(DISTINCT a) AS thisWeek
MATCH (a2:Author)-[:WROTE]->(m2:Message)
WHERE m2.timestamp > datetime() - duration('P14D')
  AND m2.timestamp <= datetime() - duration('P7D')
WITH thisWeek, count(DISTINCT a2) AS lastWeek
RETURN 'activeMembers' AS metric, thisWeek AS current, lastWeek AS previous,
       thisWeek - lastWeek AS delta,
       CASE WHEN lastWeek > 0
            THEN toFloat(thisWeek - lastWeek) / lastWeek * 100
            ELSE 0 END AS deltaPct
```

Run similar queries for: messages/day, new joins, questions asked, positive sentiment %, 
problems reported, recommendations made, answered questions, channels active, etc.

**Edge function endpoint:** `GET /week-over-week`
**Implementation note:** Run 12 parallel small queries rather than one giant query.

---

#### W30: SentimentByTopic
**What it shows:** Stacked bars showing positive/neutral/negative per topic.

```cypher
MATCH (m:Message)-[:DISCUSSES]->(t:Topic)
WHERE m.timestamp > datetime() - duration('P30D')
MATCH (m)-[:HAS_SENTIMENT]->(s:Sentiment)
WITH t.name AS topic, s.label AS sentiment, count(m) AS msgCount
WITH topic, collect({sentiment: sentiment, count: msgCount}) AS sentiments,
     sum(msgCount) AS total
WHERE total >= 10  // Minimum sample
ORDER BY total DESC
LIMIT 10
RETURN topic, sentiments, total
```

**Edge function endpoint:** `GET /sentiment-by-topic`

---

#### W31: ContentPerformance
**What it shows:** Average engagement per content type + top posts.

```cypher
// Content format performance
MATCH (m:Message)
WHERE m.timestamp > datetime() - duration('P30D')
WITH m.messageType AS format,
     avg(m.replyCount + m.reactionCount + m.forwardCount) AS avgEngagement,
     count(m) AS volume
RETURN format, avgEngagement, volume
ORDER BY avgEngagement DESC

// Top 5 performing posts
UNION

MATCH (m:Message)
WHERE m.timestamp > datetime() - duration('P30D')
WITH m, (m.replyCount + m.reactionCount + m.forwardCount) AS engagement
ORDER BY engagement DESC
LIMIT 5
OPTIONAL MATCH (m)-[:POSTED_IN]->(ch:Channel)
RETURN m.text AS text, engagement, ch.name AS channel, m.messageType AS format
```

**Edge function endpoint:** `GET /content-performance`

---

#### W32: CommunityVitalityScorecard
**What it shows:** 8 health indicators with scores, trends, benchmarks.

This is a composite of queries already used by other widgets. The edge function aggregates:

| Indicator | Source Query | Benchmark |
|-----------|-------------|-----------|
| Question Answer Rate | W7 (QuestionCloud) | 70% |
| Helper-to-Lurker Ratio | W21 (Funnel) | 15% |
| Newcomer Retention (7d) | W20 (Retention) | 60% |
| Cross-Channel Activity | W12 (Channels) | 2.5 channels/user |
| Recommendation Density | W24 (Recs) | 5/day |
| Problem Resolution Rate | W8 (Problems) | 40% |
| Sentiment Health | W1 (Health) | 60% positive |
| Content Diversity | W6 (Heatmap) | 5+ types active |

**Edge function endpoint:** `GET /vitality-scorecard`
**Implementation note:** This endpoint calls the other endpoints internally and normalizes scores to 0-100. Cache for 30 minutes.

---

## 6. Supabase Edge Function Endpoints <a id="6-endpoints"></a>

### 6.1 New Endpoint Summary

| Endpoint | Method | Tier | Widgets | Cache TTL |
|----------|--------|------|---------|-----------|
| `/community-health` | GET | 1 | W1 | 5 min |
| `/trending-topics` | GET | 1 | W2 | 5 min |
| `/community-brief` | GET | 1 | W3 | 60 min |
| `/topic-landscape` | GET | 2 | W4 | 15 min |
| `/conversation-trends` | GET | 2 | W5 | 15 min |
| `/engagement-heatmap` | GET | 2 | W6 | 15 min |
| `/top-questions` | GET | 2 | W7 | 15 min |
| `/problems` | GET | 3 | W8 | 15 min |
| `/service-gaps` | GET | 3 | W9 | 15 min |
| `/satisfaction-by-area` | GET | 3 | W10 | 15 min |
| `/mood-over-time` | GET | 3 | W11 | 15 min |
| `/top-channels` | GET | 4 | W12 | 5 min |
| `/key-voices` | GET | 4 | W13 | 30 min |
| `/activity-patterns` | GET | 4 | W23 | 15 min |
| `/top-recommendations` | GET | 4 | W24 | 15 min |
| `/newcomer-journey` | GET | 4 | W25 | 30 min |
| `/personas` | GET | 5 | W14 | 60 min |
| `/interest-radar` | GET | 5 | W15 | 30 min |
| `/origin-cities` | GET | 5 | W16 | 60 min |
| `/integration-spectrum` | GET | 5 | W17 | 30 min |
| `/location-distribution` | GET | 5 | W18 | 30 min |
| `/emerging-interests` | GET | 6 | W19 | 15 min |
| `/retention-risk` | GET | 6 | W20 | 30 min |
| `/growth-funnel` | GET | 6 | W21 | 30 min |
| `/decision-stages` | GET | 6 | W22 | 30 min |
| `/business-opportunities` | GET | 7 | W26 | 60 min |
| `/job-market` | GET | 7 | W27 | 30 min |
| `/housing-pulse` | GET | 7 | W28 | 30 min |
| `/week-over-week` | GET | 8 | W29 | 15 min |
| `/sentiment-by-topic` | GET | 8 | W30 | 15 min |
| `/content-performance` | GET | 8 | W31 | 15 min |
| `/vitality-scorecard` | GET | 8 | W32 | 30 min |

### 6.2 Global Filter Parameters

All endpoints should accept these optional query parameters:

```
?channels=ch1,ch2        // Filter by specific channels
?since=2026-01-01        // Start date
?until=2026-02-24        // End date
?personas=it_relocant    // Filter by persona
?language=ru             // Filter by message language
```

These map to additional WHERE clauses in every Cypher query:

```cypher
// Channel filter
AND (ch.id IN $channels OR $channels IS NULL)

// Date filter
AND m.timestamp >= datetime($since)
AND m.timestamp <= datetime($until)

// Persona filter (requires join)
AND EXISTS {
  MATCH (a:Author)-[:WROTE]->(m)
  MATCH (a)-[:BELONGS_TO]->(p:Persona)
  WHERE p.name IN $personas
}
```

### 6.3 Edge Function Template

```typescript
// /supabase/functions/server/community-widgets.tsx

import * as neo4jHttp from "./neo4j-http.tsx";

// Simple in-memory cache
const cache = new Map<string, { data: any; expiry: number }>();

function getCached(key: string, ttlMs: number): any | null {
  const entry = cache.get(key);
  if (entry && Date.now() < entry.expiry) return entry.data;
  return null;
}

function setCache(key: string, data: any, ttlMs: number) {
  cache.set(key, { data, expiry: Date.now() + ttlMs });
}

// Example: Community Health endpoint
export async function getCommunityHealth(filters: any = {}) {
  const cacheKey = `health_${JSON.stringify(filters)}`;
  const cached = getCached(cacheKey, 5 * 60 * 1000); // 5 min
  if (cached) return cached;

  // Run 4 parallel queries
  const [engagement, growth, sentiment, velocity] = await Promise.all([
    neo4jHttp.executeQuery(`
      MATCH (m:Message)
      WHERE m.timestamp > datetime() - duration('P7D')
      RETURN toFloat(sum(m.replyCount + m.reactionCount)) / count(m) * 100 AS rate
    `),
    neo4jHttp.executeQuery(`
      MATCH (a:Author) WHERE a.firstSeen > datetime() - duration('P7D')
      WITH count(a) AS thisWeek
      MATCH (a2:Author) 
      WHERE a2.firstSeen > datetime() - duration('P14D')
        AND a2.firstSeen <= datetime() - duration('P7D')
      RETURN thisWeek, count(a2) AS lastWeek
    `),
    neo4jHttp.executeQuery(`
      MATCH (m:Message)-[:HAS_SENTIMENT]->(s:Sentiment)
      WHERE m.timestamp > datetime() - duration('P7D')
      RETURN toFloat(sum(CASE WHEN s.label='positive' THEN 1 ELSE 0 END)) / count(m) * 100 AS pct
    `),
    neo4jHttp.executeQuery(`
      MATCH (m:Message) WHERE m.timestamp > datetime() - duration('P7D')
      RETURN count(m) / 7.0 AS perDay
    `),
  ]);

  const result = {
    score: Math.round(
      (engagement[0]?.rate || 0) * 0.3 +
      Math.min(100, (growth[0]?.thisWeek || 0) / (growth[0]?.lastWeek || 1) * 50) * 0.25 +
      (sentiment[0]?.pct || 0) * 0.25 +
      Math.min(100, (velocity[0]?.perDay || 0) / 100 * 100) * 0.2
    ),
    components: [
      { label: "Engagement Rate", value: Math.round(engagement[0]?.rate || 0) },
      { label: "Community Growth", value: Math.round(growth[0]?.thisWeek || 0) },
      { label: "Positive Sentiment", value: Math.round(sentiment[0]?.pct || 0) },
      { label: "Content Velocity", value: Math.round(velocity[0]?.perDay || 0) },
    ],
  };

  setCache(cacheKey, result, 5 * 60 * 1000);
  return result;
}
```

---

## 7. Implementation Phases <a id="7-phases"></a>

### Phase 1: Schema & Seed Data (3-5 days)
1. Create Neo4j schema with all 12 node types and 16 relationship types
2. Write GPT-4o-mini extraction prompt (section 3.2)
3. Process 1,000 sample messages through pipeline
4. Load extracted data into Neo4j
5. Verify with diagnostic queries

### Phase 2: Core Endpoints - Tier 1 & 2 (3-4 days)
1. Implement 7 edge function endpoints (W1-W7)
2. Wire DashboardPage widgets to live API
3. Replace mock data with API calls
4. Add loading states and error handling
5. Test with real data

### Phase 3: Behavioral & Network - Tier 3 & 4 (3-4 days)
1. Implement 9 edge function endpoints (W8-W13, W23-W25)
2. Wire widgets
3. Test sentiment accuracy
4. Tune GPT-4o-mini extraction for problems and recommendations

### Phase 4: Psychographic & Growth - Tier 5 & 6 (3-4 days)
1. Implement 9 edge function endpoints (W14-W22)
2. Build persona assignment batch job
3. Wire widgets
4. Test integration spectrum logic

### Phase 5: Business & Analytics - Tier 7 & 8 (2-3 days)
1. Implement 7 edge function endpoints (W26-W32)
2. Wire widgets
3. Implement composite scorecard (W32)
4. Add caching layer

### Phase 6: GlobalFilters Integration (1-2 days)
1. Add filter parameters to all endpoints
2. Connect GlobalFiltersLight to dashboard widgets
3. Add channel/date/persona filter propagation
4. Test filter combinations

### Phase 7: Polish & Optimization (2-3 days)
1. Add Neo4j indexes for slow queries
2. Implement edge function caching
3. Add error boundaries per widget
4. Loading skeleton states
5. Performance audit (target: < 3s full dashboard load)

---

## Key Principles

1. **One widget = one endpoint.** Don't combine widgets into mega-queries. Keep each Cypher focused and cacheable.

2. **Filter at the database level.** Don't fetch everything and filter in JS. Push WHERE clauses to Neo4j.

3. **Parallel queries over compound queries.** For composite widgets (W1, W32), run 4+ simple queries in Promise.all() rather than one complex Cypher.

4. **Cache aggressively.** Persona data changes rarely (60 min TTL). Trending topics change often (5 min TTL). Match cache TTL to data volatility.

5. **Graceful degradation.** If a query fails, the widget should show "Data unavailable" with a retry button, not crash the whole dashboard.

6. **Mock data stays until wired.** Each widget currently has self-contained mock data. Remove mock data only after the live endpoint is confirmed working.

7. **Neo4j datetime functions.** Use `datetime() - duration('P7D')` for "last 7 days", `duration('P30D')` for "last 30 days". The `P` stands for "period" in ISO 8601 duration format.

---

## Appendix: Existing Backend That Needs Migration

The current `/supabase/functions/server/` has:
- `neo4j.tsx` - Bolt driver, queries for Brand->Ad->Topic (OLD schema)
- `neo4j-http.tsx` - HTTP API, same queries (OLD schema)
- `gemini.tsx` - AI query translation (keep, update prompts)
- `diagnostics.tsx` - Data quality checks (update for new schema)
- `index.tsx` - Hono router with all endpoints

**Migration strategy:**
1. Create new file `community-widgets.tsx` with all 32 widget query functions
2. Add new routes in `index.tsx` under `/make-server-14007ead/community/...`
3. Keep old endpoints working until Graph page is migrated
4. Update `api.ts` on frontend to call new endpoints
