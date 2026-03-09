# Backend Integration Guide — Radar Obshchiny (Community Radar)

> **Audience:** Backend/data engineers connecting Neo4j + Supabase to the existing React frontend.
> **Last updated:** 2026-02-28
> **Frontend stack:** React 18, Vite, Tailwind CSS v4, React Router (data mode), Recharts, Motion.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Data Flow & Swap Points](#2-data-flow--swap-points)
3. [The `AppData` Contract — Master Interface](#3-the-appdata-contract--master-interface)
4. [Bilingual Data Pattern](#4-bilingual-data-pattern)
5. [Dashboard Widgets — Full Data Map](#5-dashboard-widgets--full-data-map)
6. [Detail Pages — Full Data Map](#6-detail-pages--full-data-map)
7. [Graph Page — Extracted to Separate Application](#7-graph-page--extracted-to-separate-application)
8. [AI Assistant](#8-ai-assistant)
9. [API Endpoint Design Recommendations](#9-api-endpoint-design-recommendations)
10. [Authentication & Session Management](#10-authentication--session-management)
11. [Export Functionality](#11-export-functionality)
12. [Error Handling & Loading States](#12-error-handling--loading-states)
13. [Known Architectural Notes & TODOs](#13-known-architectural-notes--todos)
14. [Frontend Tasks Deferred to Integration Time](#14-frontend-tasks-deferred-to-integration-time)
15. [Quick Start Checklist](#15-quick-start-checklist)

---

## 1. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        FRONTEND (React)                         │
│                                                                 │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────┐   │
│  │ AdminLayout   │    │LanguageCtx   │    │   DataContext     │   │
│  │ (sidebar +    │    │ {lang, setLang}│   │ {data, loading,  │   │
│  │  Outlet)      │    │ 'ru' | 'en'  │    │  error, refresh} │   │
│  └──────────────┘    └──────────────┘    └────────┬─────────┘   │
│         │                                          │             │
│         ▼                                          ▼             │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │                    7 PAGES                                 │  │
│  │  /          → DashboardPage  (30 widgets from AppData)     │  │
│  │  /topics    → TopicsPage     (data.allTopics)              │  │
│  │  /channels  → ChannelsPage   (data.allChannels)            │  │
│  │  /audience  → AudiencePage   (data.allAudience)            │  │
│  │  /graph     → GraphPage      (shell — embeds external app)  │  │
│  │  /sources   → SourcesPage    (inline mock data, types in   │  │
│  │                                data.ts)                     │  │
│  │  /settings  → SettingsPage   (frontend-only config)        │  │
│  └────────────────────────────────────────────────────────────┘  │
│                                                                 │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │              ~30 WIDGET COMPONENTS                         │  │
│  │  All use: const { data } = useData();                      │  │
│  │  All use: const { lang } = useLanguage();                  │  │
│  │  All display <EmptyWidget /> when data is empty/null       │  │
│  │  ZERO inline data — everything from DataContext            │  │
│  └────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                              │
                    fetchData(signal)
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      BACKEND (to build)                         │
│                                                                 │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────┐   │
│  │ Supabase Auth │    │ REST/RPC API │    │  Neo4j / Data    │   │
│  │ (JWT tokens)  │    │ /api/...     │    │  Processing      │   │
│  └──────────────┘    └──────────────┘    └──────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

### Key Principle: **Zero Widget Changes**

Every widget reads from `useData()` and uses `data.<key>[lang]` or `data.<key>`. When you connect a real backend, you only modify **one function**: `fetchData()` in `/src/app/contexts/DataContext.tsx`. All 30+ widgets automatically receive live data.

---

## 2. Data Flow & Swap Points

### Primary Swap Point: `DataContext.tsx`

**File:** `/src/app/contexts/DataContext.tsx`
**Function:** `fetchData(signal?: AbortSignal): Promise<AppData>`

Currently returns `mockAppData` synchronously. To connect:

```typescript
// BEFORE (mock):
async function fetchData(_signal?: AbortSignal): Promise<AppData> {
  return mockAppData;
}

// AFTER (real):
async function fetchData(signal?: AbortSignal): Promise<AppData> {
  const response = await fetch('/api/dashboard', { signal });
  if (!response.ok) throw new Error(`API ${response.status}`);
  return await response.json() as AppData;
}
```

**Built-in features already wired:**
- `AbortController` for cleanup on unmount
- `refresh()` triggers full re-fetch (stale-while-revalidate: old data stays visible during load)
- `loading` boolean for skeleton states
- `error` string for error UI
- Debounce-safe: calling `refresh()` multiple times cancels previous in-flight requests

### Secondary Swap Point: `api.ts`

**File:** `/src/app/services/api.ts`

Lightweight API service layer. Graph-specific functions have been removed (graph app is separate). Remaining:
- `apiFetch<T>(path, options)` — centralized fetch wrapper with timeout, auth header, error normalization
- `getAuthToken()` — returns `null` now; replace with Supabase session token
- `askAI(query)` — AI assistant query (mock; replace with real API call)
- `healthCheck()` — backend connectivity check
- `API_BASE_URL` — change from `/api` to your real backend URL

---

## 3. The `AppData` Contract — Master Interface

**File:** `/src/app/types/data.ts`

Your API must return a JSON object conforming to the `AppData` interface. Below is every field with its type, which widget(s) consume it, and what the backend needs to compute.

> **Critical rule:** If the shape differs from what's below, add a **transform step in `fetchData()`** rather than changing widget code.

### Type Legend

| Pattern | Meaning |
|---------|---------|
| `BilingualData<T>` | `{ en: T[], ru: T[] }` — array of items in both languages |
| `BilingualValue<T>` | `{ en: T, ru: T }` — single value in both languages |
| `T[]` | Language-independent array (no bilingual wrapper) |
| `T` | Single language-independent object |

---

## 4. Bilingual Data Pattern

Most data keys use `BilingualData<T>` = `Record<'en' | 'ru', T[]>`. The frontend selects the correct language:

```typescript
const topics = data.trendingTopics[lang]; // lang is 'en' or 'ru'
```

**Backend implication:** Your API should return data in both languages simultaneously. Options:
1. **Pre-compute both languages** at ingestion time (recommended)
2. **Accept `?lang=ru` query param** and return only one language (requires DataContext changes)
3. **Use translation keys** and a separate i18n layer (complex, not recommended for data-heavy items)

**Exception fields** that are NOT bilingual (language-independent):
- `communityChannels` — uses `topTopicEN` / `topTopicRU` inline fields instead
- `hourlyActivity`, `weeklyActivity` — numeric time series
- `origins` — uses `city` (RU) + `cityEN` fields inline
- `integrationData` — numeric time series
- `integrationSeriesConfig` — uses `label` (EN) + `labelRu` fields inline
- `moodData`, `trendData`, `voiceData` — numeric time series
- `allTopics`, `allChannels`, `allAudience` — use inline `name`/`nameRu`, `description`/`descriptionRu` fields

---

## 5. Dashboard Widgets — Full Data Map

The Dashboard page (`/`) renders ~30 widgets organized in 8 collapsible tiers. Every widget is listed below with its exact data key(s), data type, and field descriptions.

---

### TIER 1: Community Pulse (Executive Glance)

#### W1: CommunityHealthScore

**Data key:** `communityHealth`
**Type:** `CommunityHealthData`

```typescript
interface CommunityHealthData {
  currentScore: number;          // 0-100, the main health index
  weekAgoScore: number;          // 0-100, for computing delta
  history: HealthHistoryPoint[]; // Mini sparkline data (6-10 points)
  components: BilingualData<HealthComponent>; // Sub-scores breakdown
}

interface HealthHistoryPoint {
  time: string;   // Label like "6h ago", "Now"
  score: number;  // 0-100
}

interface HealthComponent {
  label: string;  // e.g. "Engagement Rate", "Вовлечённость"
  value: number;  // 0-100 (displayed as progress bar)
  trend: number;  // Signed integer: +5, -3, etc.
  desc: string;   // Short description
}
```

**Backend computation:** Aggregate engagement rate, growth rate, content quality, sentiment from message analysis. The `currentScore` is a weighted composite.

**Frontend behavior:**
- Animated ring chart for `currentScore`
- Color coding: >=70 emerald (Thriving), >=50 blue (Growing), >=30 amber (Stagnant), <30 red (Declining)
- Delta = `currentScore - weekAgoScore`, shown with conditional sign and icon color
- Components rendered as progress bars with trend indicators

---

#### W2: TrendingTopicsFeed

**Data key:** `trendingTopics`
**Type:** `BilingualData<TrendingTopic>`

```typescript
interface TrendingTopic {
  id: number;           // Unique identifier
  topic: string;        // Topic name (in current language)
  mentions: number;     // Total mention count
  trend: number;        // % change (signed: +15, -8)
  category: string;     // e.g. "Housing", "Education" / "Жильё", "Образование"
  sentiment: string;    // Emoji key: "frustrated", "seeking", "curious",
                        //   "motivated", "concerned", "excited", "confused"
  sampleQuote: string;  // Representative quote from messages
}
```

**Backend computation:** NLP topic extraction + sentiment classification from last 24h messages. Group by topic, compute mention counts and trend vs. prior period.

**Frontend behavior:**
- Sentiment mapped to emoji: `frustrated` = frustrated face, `seeking` = magnifying glass, etc.
- Category color-coded with a shared `categoryColors` map
- Scrollable list, max height 360px

---

#### W3: CommunityBrief (AI Summary)

**Data key:** `communityBrief`
**Type:** `CommunityBriefData`

```typescript
interface CommunityBriefData {
  messagesAnalyzed: number;        // Total messages processed
  updatedMinutesAgo: number;       // Minutes since last update
  activeMembers: string;           // Display string, e.g. "8,247"
  messagesToday: string;           // Display string, e.g. "2,847"
  positiveMood: string;            // Display string, e.g. "68%"
  newMembersGrowth: string;        // Display string, e.g. "+127"
  mainBrief: BilingualValue<string>;        // 1-2 paragraph AI summary
  expandedBrief: BilingualValue<string[]>;  // Additional paragraphs (expandable)
}
```

**Backend computation:** LLM-generated summary of community activity. Auto-refreshes every 6 hours.

**Frontend behavior:**
- Purple/violet gradient styling (AI branding)
- Expandable: shows `mainBrief` by default, reveals `expandedBrief` on click
- 4-column stat cards for quick KPIs

---

### TIER 2: Strategic / Topics

#### W4: TopicLandscape

**Data key:** `topicBubbles`
**Type:** `BilingualData<TopicBubble>`

```typescript
interface TopicBubble {
  name: string;      // Topic name
  value: number;     // Mention count (drives bubble size)
  category: string;  // Category label
  color: string;     // Hex color, e.g. "#ef4444"
  growth: number;    // % growth (signed)
}
```

**Frontend behavior:** Bubble-like cards sized by `sqrt(value) * 2`. Categories derived dynamically from data. Links to `/topics` page.

---

#### W5: ConversationTrends

**Data keys:** `trendLines` + `trendData`

```typescript
// trendLines: BilingualData<TrendLine>
interface TrendLine {
  key: string;     // Must match a key in TrendDataPoint, e.g. "housing"
  label: string;   // Display label, e.g. "Housing & Rent"
  color: string;   // Line color hex
  current: number; // Current value
  change: number;  // % change (signed)
}

// trendData: TrendDataPoint[] (NOT bilingual — numeric time series)
interface TrendDataPoint {
  week: string;                    // X-axis label, e.g. "W1", "W2"
  [key: string]: number | string;  // Dynamic keys matching TrendLine.key
}
```

**Important:** `trendData` keys must match `trendLines[].key`. Example: if `trendLines` has `key: "housing"`, then every `TrendDataPoint` must have a `housing` numeric field.

**Frontend behavior:** Recharts `<LineChart>` with dynamic lines. Auto-computes "fastest growing" from `trendLines`.

---

#### W6: ContentEngagementHeatmap

**Data key:** `heatmap`
**Type:** `BilingualValue<HeatmapData>`

```typescript
// Note: BilingualValue, not BilingualData — single object per language, not array
interface HeatmapData {
  contentTypes: string[];   // Row labels, e.g. ["Guides", "Polls", "Stories"]
  topicCols: string[];      // Column labels, e.g. ["Housing", "Jobs", "Education"]
  engagement: Record<string, Record<string, number>>;
    // engagement[contentType][topic] = engagement score (0-100)
    // e.g. engagement["Guides"]["Housing"] = 85
}
```

**Frontend behavior:** Table-based heatmap with color-coded cells (green = high engagement). Auto-finds top performer cell.

---

#### W7: QuestionCloud

**Data key:** `questionCategories`
**Type:** `BilingualData<QuestionCategory>`

```typescript
interface QuestionCategory {
  category: string;  // e.g. "Housing", "Жильё"
  color: string;     // Hex color
  questions: {
    q: string;          // Question text
    count: number;      // Times asked
    answered: boolean;  // Whether a guide/answer exists
  }[];
}
```

**Frontend behavior:** Grouped by category, 2-column grid. Unanswered questions highlighted in amber. Count of unanswered computed dynamically.

---

#### W7b: QuestionAnswerGap

**Data key:** `qaGap`
**Type:** `BilingualData<QAGapItem>`

```typescript
interface QAGapItem {
  topic: string;  // Topic name
  asked: number;  // Total questions asked
  rate: number;   // Answer rate percentage (0-100)
}
```

**Frontend behavior:** Sorted by answer rate (worst first). Colors: <35% red (critical), 35-55% amber, >55% green.

---

#### W7c: TopicLifecycle

**Data key:** `lifecycleStages`
**Type:** `BilingualData<LifecycleStage>`

```typescript
interface LifecycleStage {
  stage: string;       // e.g. "Emerging", "Rising", "Peak", "Fading"
  color: string;       // Hex dot color
  bgColor: string;     // Tailwind bg class, e.g. "bg-emerald-50"
  borderColor: string; // Tailwind border class
  textColor: string;   // Tailwind text class
  desc: string;        // Short stage description
  topics: LifecycleTopic[];
}

interface LifecycleTopic {
  name: string;       // Topic name
  daysActive: number; // How many days the topic has been active
  momentum: number;   // % momentum (signed)
  volume: number;     // Weekly message volume
}
```

**Frontend behavior:** 4-column stage header, then grouped topic lists with momentum indicators.

---

### TIER 3: Behavioral / Problems

#### W8: ProblemTracker

**Data key:** `problems`
**Type:** `BilingualData<ProblemCategory>`

```typescript
interface ProblemCategory {
  category: string;    // Category name
  problems: Problem[];
}

interface Problem {
  name: string;       // Problem name
  mentions: number;   // Mention count
  severity: string;   // "high" | "medium" | "low"
  trend: number;      // % trend (signed); positive = getting worse
  quote: string;      // Representative quote
}
```

**Frontend behavior:** For problems, positive trend is shown in RED (getting worse), negative in GREEN (improving). This is inverse from normal widgets.

---

#### W9: ServiceGapDetector

**Data key:** `serviceGaps`
**Type:** `BilingualData<ServiceGap>`

```typescript
interface ServiceGap {
  service: string;      // Service name
  demand: number;       // Number of asks/requests
  supply: string;       // Display label for supply level (localized)
  gap: number;          // Gap percentage (0-100)
  growth: number;       // % growth (signed)
  supplyLevel: 'none' | 'very_low' | 'low' | 'moderate' | 'adequate';
    // REQUIRED: semantic level used for badge coloring
    // The `supply` string is only for display — never matched programmatically
}
```

**Critical:** The `supplyLevel` field is **mandatory**. It replaced brittle localized string matching in the widget. The `supply` field is display-only.

---

#### W10: SatisfactionByArea

**Data key:** `satisfactionAreas`
**Type:** `BilingualData<SatisfactionArea>`

```typescript
interface SatisfactionArea {
  area: string;          // Life area name
  satisfaction: number;  // 0-100 satisfaction %
  mentions: number;      // Mention count
  trend: number;         // Signed trend
  emoji: string;         // Display emoji, e.g. "food_emoji"
}
```

---

#### W11: MoodOverTime

**Data keys:** `moodData` + `moodConfig`

```typescript
// moodData: MoodDataPoint[] (NOT bilingual)
interface MoodDataPoint {
  week: string;       // X-axis label
  excited: number;    // Value for each mood key
  satisfied: number;
  neutral: number;
  frustrated: number;
  anxious: number;
}

// moodConfig: BilingualData<MoodConfig>
interface MoodConfig {
  key: string;      // Must match a key in MoodDataPoint, e.g. "excited"
  label: string;    // Display label
  color: string;    // Hex color for the area fill
  emoji: string;    // Display emoji
  polarity: 'positive' | 'negative' | 'neutral';
    // REQUIRED: determines how the widget computes positive/negative share
    // 'positive' moods: excited, satisfied
    // 'negative' moods: frustrated, anxious
    // 'neutral' moods: neutral
}
```

**Important:** `moodData` keys must align with `moodConfig[].key`. The `polarity` field is **mandatory** — the widget uses it to compute positive vs. negative shares generically, not by hardcoded key names.

**Frontend behavior:** Recharts stacked `<AreaChart>`. Computes positive share %, checks if positive is growing and negative declining for trend summary.

---

#### W12: EmotionalUrgencyIndex

**Data key:** `urgencySignals`
**Type:** `BilingualData<UrgencySignal>`

```typescript
interface UrgencySignal {
  message: string;   // The actual urgent message/quote
  topic: string;     // Related topic
  urgency: string;   // "critical" | "high"
  count: number;     // Number of similar posts this week
  action: string;    // Recommended action
}
```

---

### TIER 4: Network / Channels

#### W13: TopChannels

**Data key:** `communityChannels`
**Type:** `CommunityChannel[]` (NOT bilingual)

```typescript
interface CommunityChannel {
  name: string;         // Channel name (displayed as-is)
  type: string;         // "General" | "Work" | "Family" | "Housing" | etc.
  members: number;      // Member count
  dailyMessages: number;// Daily message count
  engagement: number;   // Engagement % (0-100)
  growth: number;       // Signed growth number
  topTopicEN: string;   // Top topic in English
  topTopicRU: string;   // Top topic in Russian
}
```

**Note:** This is one of the few non-bilingual data keys. Language selection happens inline via `topTopicEN`/`topTopicRU`.

---

#### W14: KeyVoices

**Data key:** `keyVoices`
**Type:** `BilingualData<KeyVoice>`

```typescript
interface KeyVoice {
  name: string;         // Display name
  role: string;         // Role description
  followers: number;    // Follower count
  helpScore: number;    // Community help score
  topics: string[];     // Array of topic tags
  postsPerWeek: number; // Publishing frequency
  replyRate: number;    // Reply rate %
  type: string;         // "Helper" | "Organizer" | "Content Creator" |
                        //   "Influencer" | "Expert"
}
```

---

#### W15: ActivityTimeline

**Data keys:** `hourlyActivity` + `weeklyActivity`

```typescript
// hourlyActivity: HourlyActivityPoint[] (NOT bilingual)
interface HourlyActivityPoint {
  hour: string;      // e.g. "00", "01", ... "23"
  messages: number;  // Message count for that hour
}

// weeklyActivity: WeeklyActivityPoint[] (NOT bilingual)
interface WeeklyActivityPoint {
  day: string;      // Russian day name, e.g. "Пн"
  dayEN: string;    // English day name, e.g. "Mon"
  messages: number; // Message count for that day
}
```

---

#### W16: RecommendationTracker

**Data key:** `recommendations`
**Type:** `BilingualData<Recommendation>`

```typescript
interface Recommendation {
  item: string;       // What's being recommended
  category: string;   // Category
  mentions: number;   // Times mentioned
  rating: number;     // Rating score
  sentiment: string;  // "positive" | "negative" | "neutral"
}
```

---

#### W17: NewcomerFlow

**Data key:** `newcomerJourney`
**Type:** `BilingualData<NewcomerJourneyStage>`

```typescript
interface NewcomerJourneyStage {
  stage: string;       // Stage name, e.g. "Pre-arrival", "First week"
  questions: string[]; // Common questions at this stage
  volume: number;      // Question volume
  resolved: number;    // % answered (0-100)
}
```

---

#### W18: InformationVelocity

**Data key:** `viralTopics`
**Type:** `BilingualData<ViralTopic>`

```typescript
interface ViralTopic {
  topic: string;            // Topic name
  originator: string;       // Source channel/person
  spreadHours: number;      // Hours to spread
  channelsReached: number;  // Number of channels reached
  amplifiers: string[];     // Channel names that amplified
  totalReach: number;       // Total reach count
  velocity: string;         // "explosive" | "fast" | "normal"
}
```

---

### TIER 5: Psychographic

#### W19: PersonaGallery

**Data key:** `personas`
**Type:** `BilingualData<Persona>`

```typescript
interface Persona {
  name: string;       // Persona name, e.g. "The IT Relocant"
  size: number;       // % of community (for bar height)
  count: number;      // Absolute count
  color: string;      // Hex color
  profile: string;    // Short profile description
  needs: string;      // Key needs
  interests: string;  // Key interests
  pain: string;       // Pain points
  desc: string;       // Detailed description
}
```

---

#### W20: InterestRadar

**Data key:** `interests`
**Type:** `BilingualData<InterestItem>`

```typescript
interface InterestItem {
  interest: string;  // Interest name (used as radar axis label)
  score: number;     // 0-100 interest score
}
```

**Frontend behavior:** Recharts `<RadarChart>`. `interest` is used as the `PolarAngleAxis` label.

---

#### W21: OriginMap

**Data key:** `origins`
**Type:** `OriginCity[]` (NOT bilingual)

```typescript
interface OriginCity {
  city: string;    // City name in Russian
  cityEN: string;  // City name in English
  count: number;   // Person count from this city
  pct: number;     // Percentage of community
  color: string;   // Hex bar color
}
```

---

#### W22: IntegrationSpectrum

**Data keys:** `integrationLevels` + `integrationData` + `integrationSeriesConfig`

```typescript
// integrationLevels: BilingualData<IntegrationLevel>
interface IntegrationLevel {
  level: string;   // Level name
  pct: number;     // % of community at this level
  color: string;   // Hex segment color
  desc: string;    // Short description
}

// integrationData: IntegrationDataPoint[] (NOT bilingual — numeric)
interface IntegrationDataPoint {
  month: string;        // X-axis label
  learning: number;     // Value for "learning" series
  bilingual: number;    // Value for "bilingual" series
  russianOnly: number;  // Value for "russian only" series
  integrated: number;   // Value for "integrated" series
}

// integrationSeriesConfig: IntegrationSeriesConfig[] (NOT bilingual)
interface IntegrationSeriesConfig {
  key: keyof Omit<IntegrationDataPoint, 'month'>; // e.g. "learning"
  color: string;     // Hex color for the area fill
  label: string;     // English label
  labelRu: string;   // Russian label
  polarity: 'positive' | 'negative' | 'neutral';
    // 'positive' = integration progress (good when growing)
    // 'negative' = non-integration (good when shrinking)
}
```

**Important:** `integrationSeriesConfig` drives the chart generically. The `key` field must match keys in `IntegrationDataPoint`.

---

### TIER 6: Predictive

#### W23: EmergingInterests

**Data key:** `emergingInterests`
**Type:** `BilingualData<EmergingInterest>`

```typescript
interface EmergingInterest {
  topic: string;           // Topic name
  firstSeen: string;       // Date string when first detected
  growthRate: number;       // % growth rate (signed)
  currentVolume: number;   // Current mention count
  originChannel: string;   // Channel where it originated
  mood: string;            // Overall mood
  opportunity: string;     // "high" | "medium" | "low"
}
```

---

#### W24: RetentionRiskGauge

**Data keys:** `retentionFactors` + `churnSignals`

```typescript
// retentionFactors: BilingualData<RetentionFactor>
interface RetentionFactor {
  factor: string;  // What keeps people (e.g. "Community support")
  score: number;   // 0-100 score
  weight: number;  // Weight for composite calculation
}

// churnSignals: BilingualData<ChurnSignal>
interface ChurnSignal {
  signal: string;    // Signal description
  count: number;     // Occurrence count
  trend: number;     // % trend (signed); positive = worsening
  severity: string;  // "rising" | "watch" | "stable"
}
```

**Frontend behavior:** Composite retention score = weighted average of `score * weight / sum(weight)`.

---

#### W25: CommunityGrowthFunnel

**Data key:** `growthFunnel`
**Type:** `BilingualData<GrowthFunnelStage>`

```typescript
interface GrowthFunnelStage {
  stage: string;   // Stage name
  count: number;   // People in this stage
  pct: number;     // % of total community
  color: string;   // Hex bar color
  role: 'all' | 'reads' | 'asks' | 'helps' | 'contributes' | 'leads';
    // REQUIRED: semantic identifier for stage lookup
    // Replaces fragile positional indexing (growthFunnel[1], etc.)
    // Widget uses: growthFunnel.find(s => s.role === 'reads')
}
```

**Critical:** The `role` field is **mandatory**. The widget looks up stages by semantic role, not by array position.

---

#### W26: DecisionStageTracker

**Data key:** `decisionStages`
**Type:** `BilingualData<DecisionStage>`

```typescript
interface DecisionStage {
  stage: string;   // Stage name
  count: number;   // People in this stage
  pct: number;     // % of total
  trend: number;   // % trend (signed)
  color: string;   // Hex color
  needs: string;   // What this segment needs
}
```

---

#### W27: NewVsReturningVoice

**Data keys:** `voiceData` + `topNewTopics`

```typescript
// voiceData: VoiceDataPoint[] (NOT bilingual)
interface VoiceDataPoint {
  week: string;       // X-axis label
  newVoices: number;  // Count of new unique speakers
  returning: number;  // Count of returning speakers
}

// topNewTopics: BilingualData<TopNewVoiceTopic>
interface TopNewVoiceTopic {
  topic: string;     // Topic name
  newVoices: number; // New voice count for this topic
  pct: number;       // Percentage
}
```

---

### TIER 7: Actionable

#### W28: BusinessOpportunityTracker

**Data key:** `businessOpportunities`
**Type:** `BilingualData<BusinessOpportunity>`

```typescript
interface BusinessOpportunity {
  need: string;          // Business need description
  mentions: number;      // Mention count
  growth: number;        // % growth (signed)
  sector: string;        // Business sector
  readiness: string;     // Market readiness description
  sampleQuote: string;   // Representative quote
  revenue: string;       // Revenue potential: "$", "$$", "$$$", "$$$$"
}
```

---

#### W29: JobMarketPulse

**Data keys:** `jobSeeking` + `jobTrends`

```typescript
// jobSeeking: BilingualData<JobSeekingItem>
interface JobSeekingItem {
  role: string;   // Job role name
  pct: number;    // % of community
  count: number;  // Absolute count
}

// jobTrends: BilingualData<JobTrend>
interface JobTrend {
  trend: string;  // Trend description text
  type: string;   // "hot" | "growing" | "concern" | "stable"
}
```

---

#### W30: HousingMarketPulse

**Data keys:** `housingData` + `housingHotTopics`

```typescript
// housingData: BilingualData<HousingItem>
interface HousingItem {
  type: string;          // Housing type, e.g. "1-bedroom apartment"
  avgPrice: string;      // Display price string, e.g. "$650/mo"
  trend: number;         // Price trend % (signed; positive = price rising)
  satisfaction: number;  // Satisfaction % (0-100)
  volume: number;        // Discussion volume
}

// housingHotTopics: BilingualData<HousingHotTopic>
interface HousingHotTopic {
  topic: string;      // Hot topic name
  count: number;      // Mention count
  sentiment: string;  // "angry" | "worried" | "seeking" | "neutral"
}
```

---

### TIER 8: Comparative

#### W31: WeekOverWeekShifts

**Data key:** `weeklyShifts`
**Type:** `BilingualData<WeeklyShiftItem>`

```typescript
interface WeeklyShiftItem {
  metric: string;     // Metric name
  current: number;    // Current week value
  previous: number;   // Previous week value
  unit: string;       // Unit suffix, e.g. "%", "K", ""
  category: string;   // Category for grouping
  isInverse?: boolean;
    // When true, a DECREASE is "good" (e.g. churn signals)
    // Replaces brittle string-matching against metric names
}
```

**Critical:** The `isInverse` field determines color coding direction. Without it, the widget defaults to "increase = good".

---

#### W32: SentimentByTopic

**Data key:** `sentimentByTopic`
**Type:** `BilingualData<SentimentByTopicItem>`

```typescript
interface SentimentByTopicItem {
  topic: string;     // Topic name
  positive: number;  // % positive (0-100)
  neutral: number;   // % neutral (0-100)
  negative: number;  // % negative (0-100)
  volume: number;    // Total message volume
}
```

**Invariant:** `positive + neutral + negative` should equal 100.

---

#### W33: ContentPerformance

**Data keys:** `topPosts` + `contentTypePerformance`

```typescript
// topPosts: BilingualData<TopPost>
interface TopPost {
  title: string;       // Post title
  type: string;        // Content type, e.g. "Guide", "Poll"
  shares: number;      // Share count
  reactions: number;   // Reaction count
  comments: number;    // Comment count
  engagement: number;  // Composite engagement score
}

// contentTypePerformance: BilingualData<ContentTypePerf>
interface ContentTypePerf {
  type: string;           // Content type name
  avgEngagement: number;  // Average engagement score (0-100)
  count: number;          // Number of posts of this type
}
```

---

#### W34: CommunityVitalityScorecard

**Data key:** `vitalityIndicators`
**Type:** `BilingualData<VitalityIndicator>`

```typescript
interface VitalityIndicator {
  indicator: string;    // Indicator name
  score: number;        // 0-100 score
  trend: number;        // Signed trend value
  benchmark: string;    // Display benchmark label (localized)
  emoji: string;        // Display emoji
  benchmarkLevel: 'excellent' | 'good' | 'above_avg' | 'average' | 'below_avg' | 'poor';
    // REQUIRED: semantic level for badge coloring
    // Replaces brittle localized string matching
    // 'excellent' → emerald, 'good'|'above_avg' → blue,
    // 'average' → gray, 'below_avg'|'poor' → amber
}
```

**Critical:** The `benchmarkLevel` field is **mandatory**. The `benchmark` string is display-only.

---

## 6. Detail Pages — Full Data Map

### TopicsPage (`/topics`)

**Data key:** `allTopics`
**Type:** `TopicDetail[]` (NOT bilingual — uses inline name/nameRu)

```typescript
interface TopicDetail {
  id: string;
  name: string;              // English name
  nameRu: string;            // Russian name
  category: string;          // Category key (English), e.g. "Living", "Work"
  color: string;             // Hex category color
  mentions: number;          // Total mentions
  growth: number;            // % growth (signed)
  sentiment: {
    positive: number;        // % positive
    neutral: number;
    negative: number;
  };
  weeklyData: {              // Time series for the sparkline chart
    week: string;
    count: number;
  }[];
  topChannels: string[];     // Channel names where this topic is discussed
  description: string;       // English description
  descriptionRu: string;     // Russian description
  evidence: TopicEvidence[]; // Supporting messages
}

interface TopicEvidence {
  id: string;
  type: string;              // "message" | "reply" | "reaction"
  author: string;
  channel: string;
  text: string;
  timestamp: string;         // ISO datetime
  reactions: number;
  replies: number;
}
```

**Frontend behavior:** Paginated table (6 per page), category filter tabs, search by name, detail side panel with weekly chart, sentiment breakdown, and evidence messages.

---

### ChannelsPage (`/channels`)

**Data key:** `allChannels`
**Type:** `ChannelDetail[]` (NOT bilingual)

```typescript
interface ChannelDetail {
  id: string;
  name: string;
  type: string;             // "General" | "Work" | etc.
  members: number;
  dailyMessages: number;
  engagement: number;       // 0-100
  growth: number;           // Signed
  topTopic: string;
  description: string;
  weeklyData: { day: string; msgs: number }[];
  hourlyData: { hour: string; msgs: number }[];
  topTopics: { name: string; mentions: number; pct: number }[];
  sentimentBreakdown: { positive: number; neutral: number; negative: number };
  messageTypes: { type: string; count: number; pct: number }[];
  topVoices: { name: string; posts: number; helpScore: number }[];
  recentPosts: {
    id: string;
    author: string;
    text: string;
    timestamp: string;
    reactions: number;
    replies: number;
  }[];
}
```

---

### AudiencePage (`/audience`)

**Data key:** `allAudience`
**Type:** `AudienceMember[]` (NOT bilingual)

```typescript
type Gender = 'Male' | 'Female' | 'Unknown';

interface AudienceMember {
  id: string;
  username: string;
  displayName: string;
  gender: Gender;
  age: string;               // e.g. "28", "35"
  origin: string;            // City of origin
  location: string;          // Current location
  joinedDate: string;        // ISO date
  lastActive: string;        // ISO date
  totalMessages: number;
  totalReactions: number;
  helpScore: number;
  interests: string[];
  channels: {
    name: string;
    type: string;
    role: string;            // "Admin" | "Moderator" | "Active" | "Member"
    messageCount: number;
  }[];
  topTopics: { name: string; count: number }[];
  sentiment: { positive: number; neutral: number; negative: number };
  activityData: { week: string; msgs: number }[];
  recentMessages: {
    text: string;
    channel: string;
    timestamp: string;
    reactions: number;
    replies: number;
  }[];
  persona: string;           // Persona name, e.g. "IT Relocant"
  integrationLevel: string;  // e.g. "Bilingual", "Learning"
}
```

**Frontend behavior:** Sortable table, search by name/username, 8-per-page pagination, detail side panel with activity chart, channel participation, recent messages, sentiment pie.

---

## 7. Graph Page — Extracted to Separate Application

> **Updated 2026-02-28:** The graph visualization has been extracted into a separate dedicated application. This dashboard's GraphPage is now a **shell/mount point**.

### What Was Removed (2026-02-28)

**7 components deleted** from `/src/app/components/`:
- `GraphVisualization.tsx`, `EmptyGraphState.tsx`, `FloatingControls.tsx`
- `GraphLegend.tsx`, `NodeInspector.tsx`, `ExportButton.tsx`, `GlobalFiltersLight.tsx`

**9 API functions removed** from `api.ts`:
- `getGraphData`, `getNodeDetails`, `searchGraph`, `getTrendingTopics`
- `getTopBrands`, `getAllBrands`, `getSentimentDistribution`, `getGraphInsights`, `getDailyBriefing`

**8 types removed** from `api.ts`:
- `GraphNode`, `GraphLink`, `GraphData`, `NodeDetails`, `SearchResult`
- `GraphTrendingTopic`, `GraphTopBrand`, `SentimentData`

### Current GraphPage Shell

**File:** `/src/app/pages/GraphPage.tsx`

Two integration options:

1. **iframe embed** — Set `GRAPH_APP_URL` constant:
   ```typescript
   const GRAPH_APP_URL = 'https://your-graph-app.url';
   ```

2. **Micro-frontend mount** — Mount at `#graph-mount-point`:
   ```typescript
   const mountPoint = document.getElementById('graph-mount-point');
   ReactDOM.createRoot(mountPoint).render(<GraphApp />);
   ```

The shell preserves the mobile gate (desktop-only notice) and bilingual support.

### Graph API Reference (consumed by the separate graph app)

| Endpoint | Method | Purpose |
|----------|--------|--------|
| `POST /api/graph` | POST | Fetch graph data with filters |
| `GET /api/graph/node/:id` | GET | Node details (`?type=brand\|topic`) |
| `GET /api/graph/search` | GET | Full-text search (`?q=...`) |
| `GET /api/graph/trending` | GET | Trending topics (`?limit=10`) |
| `GET /api/graph/brands` | GET | Top brands (`?limit=10`) |
| `GET /api/graph/brands/all` | GET | All brands for filter dropdown |
| `GET /api/graph/sentiment` | GET | Sentiment distribution |
| `POST /api/ai/query` | POST | AI natural language query |
| `GET /api/graph/insights` | GET | Graph insights |
| `GET /api/health` | GET | Backend health check |

These endpoints are consumed by the **separate graph application**, not this dashboard. The Supabase edge functions for these endpoints remain in `/supabase/functions/server/`.

---

## 8. AI Assistant

**File:** `/src/app/components/AIAssistant.tsx`

### Current State
- Uses mock responses mapped to keyword patterns
- Supports suggested prompts in both languages
- Has message history with user/assistant roles

### Backend Requirements

**Endpoint:** `POST /api/ai/query`
```typescript
// Request:
{ query: string }

// Response:
{
  query: string;
  answer: string;      // Markdown-formatted response
  timestamp: string;   // ISO datetime
}
```

**Considerations for production:**
1. **SSE streaming** — Currently waits for full response. For better UX, stream tokens via Server-Sent Events
2. **AbortController** — Already prepared in `apiFetch()` for cancellation
3. **Rate limiting** — Handle HTTP 429 with exponential backoff
4. **Context window** — Send relevant graph context to GPT, not the entire database
5. **Messages array** — The `messages` state tracks conversation history; consider sending history for multi-turn context

---

## 9. API Endpoint Design Recommendations

### Option A: Single Aggregating Endpoint (Recommended for MVP)

```
GET /api/dashboard → returns full AppData JSON
```

**Pros:** One request, matches current `fetchData()` exactly, simple to implement.
**Cons:** Large payload (~50-100KB), loads everything even if user only visits one tier.

### Option B: Per-Tier Endpoints (Recommended for Production)

```
GET /api/dashboard/pulse        → communityHealth + trendingTopics + communityBrief
GET /api/dashboard/strategic    → topicBubbles + trendLines + trendData + heatmap + ...
GET /api/dashboard/behavioral   → problems + serviceGaps + satisfactionAreas + ...
GET /api/dashboard/network      → communityChannels + keyVoices + hourlyActivity + ...
GET /api/dashboard/psychographic → personas + interests + origins + integration...
GET /api/dashboard/predictive   → emergingInterests + retentionFactors + ...
GET /api/dashboard/actionable   → businessOpportunities + jobSeeking + housing...
GET /api/dashboard/comparative  → weeklyShifts + sentimentByTopic + topPosts + ...
GET /api/topics                 → allTopics (with pagination)
GET /api/channels               → allChannels (with pagination)
GET /api/audience               → allAudience (with pagination)
```

**To implement Option B:** Modify `fetchData()` to make parallel requests and merge results into a single `AppData` object. Widgets don't change.

### Option C: Supabase RPC

```sql
-- Single RPC that returns the full dashboard
CREATE OR REPLACE FUNCTION get_dashboard_data()
RETURNS jsonb AS $$
  -- aggregate all dashboard data into AppData shape
$$ LANGUAGE plpgsql;
```

Frontend call:
```typescript
const { data } = await supabase.rpc('get_dashboard_data');
return data as AppData;
```

---

## 10. Authentication & Session Management

### Current State

`getAuthToken()` in `api.ts` returns `null`. This is the single integration point.

### Integration Steps

1. **Set up Supabase Auth** (or your auth provider)
2. **Replace `getAuthToken()`:**

```typescript
import { supabase } from './supabaseClient';

async function getAuthToken(): Promise<string | null> {
  const { data: { session } } = await supabase.auth.getSession();
  return session?.access_token ?? null;
}
```

3. **`apiFetch()` already injects the token** as `Authorization: Bearer <token>` header
4. **Add session check** in `AdminLayout.tsx` or `App.tsx` to redirect to login if no session

### Protected Routes

Currently all routes are unprotected. Add auth guards:

```typescript
// In routes.tsx — add a loader that checks auth
{
  path: "/",
  Component: AdminLayout,
  loader: async () => {
    const { data: { session } } = await supabase.auth.getSession();
    if (!session) throw redirect('/login');
    return null;
  },
  children: [...]
}
```

---

## 11. Export Functionality

> **Updated 2026-02-28:** The `ExportButton.tsx` component has been removed along with other graph components. Export functionality now lives in the **separate graph application**.

The dashboard itself does not currently have export functionality. If needed for dashboard data, consider:
1. **CSV export** — Client-side generation from `AppData` using `data.allTopics`, `data.allChannels`, etc.
2. **PDF reports** — Server-side generation via a backend endpoint
3. **Clipboard copy** — Per-widget "copy data" action

---

## 12. Error Handling & Loading States

### DataContext

| State | UI Behavior |
|-------|-------------|
| `loading: true` | Widgets can show skeletons (currently no skeleton implemented — widgets show normally with stale data during refresh) |
| `error: string` | Error message available — currently not displayed by any widget (TODO: add error banner) |
| `data: AppData` | Always has a value (falls back to mockData, preserves stale data on error) |

### Graph Page Components

| Component | Error Handling |
|-----------|---------------|
| `NodeInspector` | Shows error message + retry button; `cancelled` flag prevents state updates after unmount |
| `GlobalFiltersLight` | Shows error message; `cancelled` flag for cleanup |
| `GraphVisualization` | Shows `EmptyGraphState` when no brands selected |
| `EmptyGraphState` | Bilingual placeholder (RU/EN) with animated gradient background |

### `apiFetch()` Error Normalization

All API errors are normalized to `Error` objects with descriptive messages:
- Timeout: `"Request to /path timed out after 15000ms"`
- HTTP error: `"API 404: Not Found"`
- AbortError: Silently ignored (component unmounted)

---

## 13. Known Architectural Notes & TODOs

### ~~1. `GraphPage.filters.dateRange` Not Passed to `GraphVisualization`~~ (RESOLVED — 2026-02-28)

No longer applicable. Graph page has been extracted to a separate application. See Section 7.

### 2. DataContext Loads All Data at Once

Currently `fetchData()` returns the entire `AppData` in one call. For production:
- Consider lazy loading per-tier data
- Or use an aggregating backend endpoint that computes everything server-side
- The frontend architecture supports either approach — only `fetchData()` changes

### ~~3. Legacy Dead-Code Components~~ (DONE — 2026-02-28)

All legacy dead-code components have been **deleted**:
- `GlobalFilters.tsx`, `DateRangePicker.tsx`, `AIQueryBar.tsx` — replaced by `AdminLayout` inline date picker and `AIAssistant`
- `SampleDataLoader.tsx`, `ConnectionTest.tsx` — development utilities, no longer needed
- `SettingsPanel.tsx` — replaced by `SettingsPage`
- `DotMatrixBackground.tsx` — decorative component, unused
- 2 backup files (`*.backup`) — cleaned up

Total: 9 files removed. All imports verified clean, no dangling references.

### 4. AIAssistant Needs SSE Streaming

Currently waits for full AI response. For production GPT integration:
- Implement SSE/WebSocket streaming
- Add rate-limit handling (HTTP 429)
- Consider token counting for context window management

### 5. No Auth/Session Management Yet

`getAuthToken()` returns `null`. See [Section 10](#10-authentication--session-management) for integration steps.

### 6. SourcesPage — TrackedChannel Type Centralized (DONE — 2026-02-28)

The `TrackedChannel` interface and `ChannelStatus` type have been moved from inline in `SourcesPage.tsx` to `/src/app/types/data.ts`. SourcesPage now imports them: `import type { TrackedChannel, ChannelStatus } from '../types/data'`.

The Sources page still uses **inline mock data** (`MOCK_CHANNELS` array in `SourcesPage.tsx`).

**To connect to backend:**
1. Add CRUD endpoints: `GET /api/sources`, `POST /api/sources`, `PATCH /api/sources/:id`, `DELETE /api/sources/:id`
2. Either add `trackedChannels` to `AppData` (read-only list) or keep a separate API layer (recommended, since Sources has write operations)
3. Wire Telegram Bot API integration for channel metadata/validation

### ~~7. Graph Page Components Now Fully Bilingual~~ (SUPERSEDED — 2026-02-28)

No longer applicable. All graph components have been removed. The graph page is now a shell for the separate graph application. See Section 7.

### 8. Unused Imports Cleaned (2026-02-28)

All unused imports across widget files have been removed (ComparativeWidgets, PredictiveWidgets, NodeInspector, SourcesPage, ExportButton). No functional changes — only cleaner builds with zero warnings.

---

## 14. Frontend Tasks Deferred to Integration Time

> These items are **intentionally not implemented** in the mock-only frontend. They require a real backend to test and should be done when connecting the API.

### 14.1 Loading & Error UI

**Why deferred:** Mock data loads synchronously — `loading` is never `true`, `error` is never set. Any loading/error UI added now would be untestable dead code.

**What to implement when connecting:**

1. **Global error banner** in `AdminLayout.tsx`:
   ```tsx
   const { error, refresh } = useData();
   {error && (
     <div className="bg-red-50 border-b border-red-200 px-4 py-2 flex items-center justify-between">
       <span className="text-sm text-red-700">{error}</span>
       <button onClick={refresh} className="text-sm text-red-600 underline">Retry</button>
     </div>
   )}
   ```

2. **Per-widget loading skeletons** — Replace `EmptyWidget` fallback with shimmer/skeleton when `loading` is true:
   ```tsx
   const { data, loading } = useData();
   if (loading && !data.trendingTopics[lang].length) return <WidgetSkeleton />;
   ```
   Note: DataContext preserves stale data during refresh (stale-while-revalidate), so skeletons only appear on initial load.

3. **Refresh button** — Add a manual refresh button to the dashboard header or AdminLayout:
   ```tsx
   const { refresh, loading } = useData();
   <button onClick={refresh} disabled={loading}>
     <RefreshCw className={loading ? 'animate-spin' : ''} />
   </button>
   ```

4. **Per-widget error boundaries** — Wrap each widget in React error boundaries to prevent one broken widget from crashing the entire dashboard.

### 14.2 AI Assistant — SSE Streaming

**File:** `/src/app/components/AIAssistant.tsx`

**Current state:** Uses `getMockResponse()` — a local function that maps keywords to pre-written responses. The `askAI()` function in `api.ts` exists but is not called.

**What to implement when connecting:**

1. **Replace `getMockResponse()` with `askAI()`** from `api.ts`:
   ```tsx
   // In sendMessage():
   const response = await askAI(text.trim());
   setMessages(prev => [...prev, {
     id: (Date.now() + 1).toString(),
     role: 'assistant',
     text: response.answer,
     timestamp: new Date(),
   }]);
   ```

2. **Add SSE streaming** for token-by-token display:
   ```tsx
   async function streamAI(query: string, onToken: (token: string) => void) {
     const response = await fetch('/api/ai/query', {
       method: 'POST',
       headers: { 'Content-Type': 'application/json' },
       body: JSON.stringify({ query, stream: true }),
     });
     const reader = response.body!.getReader();
     const decoder = new TextDecoder();
     while (true) {
       const { done, value } = await reader.read();
       if (done) break;
       const chunk = decoder.decode(value, { stream: true });
       // Parse SSE: "data: {token}\n\n"
       for (const line of chunk.split('\n')) {
         if (line.startsWith('data: ')) {
           const data = JSON.parse(line.slice(6));
           if (data.token) onToken(data.token);
           if (data.done) return;
         }
       }
     }
   }
   ```

3. **Add AbortController** for cancel-on-unmount and user cancellation (stop generating button).

4. **Add rate-limit handling** — Catch HTTP 429, show "Please wait" message, retry with exponential backoff.

5. **Multi-turn context** — Send the `messages` array as conversation history to enable follow-up questions.

### 14.3 Authentication & Session Management

**Current state:** `getAuthToken()` in `api.ts` returns `null`. No login page, no route guards.

**What to implement when connecting:**

1. **Login page** — Create `/src/app/pages/LoginPage.tsx` with Supabase Auth UI or custom form.

2. **Route guards** in `routes.tsx`:
   ```tsx
   {
     path: "/",
     Component: AdminLayout,
     loader: async () => {
       const { data: { session } } = await supabase.auth.getSession();
       if (!session) throw redirect('/login');
       return null;
     },
     children: [...]
   }
   ```

3. **Replace `getAuthToken()`** in `api.ts`:
   ```tsx
   async function getAuthToken(): Promise<string | null> {
     const { data: { session } } = await supabase.auth.getSession();
     return session?.access_token ?? null;
   }
   ```

4. **Session expiry handling** — Listen for `onAuthStateChange` events, redirect to login on sign-out.

### 14.4 Notifications (Real-time)

**Current state:** AdminLayout has hardcoded notification items.

**What to implement when connecting:**

1. **WebSocket/SSE subscription** for real-time alerts (new urgent topics, channel errors, sync completions).
2. **Notification store** — Replace hardcoded items with a notification context or Zustand store.
3. **Badge counter** — Update the bell icon badge with unread count from the store.
4. **Mark as read** — API call to mark notifications as read.

### 14.5 Vite Proxy / CORS Configuration

**Current state:** `API_BASE_URL = '/api'` — no proxy configured in `vite.config.ts`.

**What to implement when connecting:**

```typescript
// vite.config.ts
export default defineConfig({
  server: {
    proxy: {
      '/api': {
        target: 'https://your-backend.fly.dev',
        changeOrigin: true,
        // rewrite: (path) => path.replace(/^\/api/, ''),
      },
    },
  },
});
```

For production: Configure CORS headers on the backend, or use a reverse proxy (nginx, Cloudflare Workers, etc.).

### 14.6 Sources Page — Backend CRUD

**Current state:** SourcesPage uses inline `MOCK_CHANNELS` array. CRUD operations (add, pause, remove) mutate local state only.

**What to implement when connecting:**

1. **Replace `MOCK_CHANNELS`** with `useEffect` fetch from `GET /api/sources`
2. **Wire CRUD operations:**
   - Add channel: `POST /api/sources` → Telegram Bot API lookup → create record
   - Pause/Resume: `PATCH /api/sources/:id { status: 'paused' | 'active' }`
   - Remove: `DELETE /api/sources/:id`
   - Retry sync: `POST /api/sources/:id/retry`
3. **Real-time sync status** — WebSocket or polling for channel sync progress
4. **Validation** — Server-side Telegram channel resolution (replace mock `handleResolve`)

---

## 15. Quick Start Checklist

### Phase 1: Get Dashboard Working

- [ ] Deploy a backend that returns `AppData` JSON at `GET /api/dashboard`
- [ ] Match every field in the `AppData` interface (Section 3-5)
- [ ] Pay special attention to **mandatory semantic fields**: `supplyLevel`, `polarity`, `role`, `benchmarkLevel`, `isInverse`
- [ ] Test bilingual data: every `BilingualData<T>` key must have both `en` and `ru` arrays
- [ ] Update `API_BASE_URL` in `api.ts`
- [ ] Update `fetchData()` in `DataContext.tsx` to call your API
- [ ] Configure Vite proxy for development (Section 14.5)
- [ ] Add loading skeletons and error banner (Section 14.1)

### Phase 2: Connect Graph App

- [ ] Deploy the separate graph application
- [ ] Set `GRAPH_APP_URL` in `GraphPage.tsx` (or configure micro-frontend mount)
- [ ] Ensure shared authentication between dashboard and graph app

### Phase 3: Auth & AI

- [ ] Set up Supabase Auth (Section 14.3)
- [ ] Create login page and route guards
- [ ] Replace `getAuthToken()` with real token retrieval
- [ ] Connect AI assistant — replace `getMockResponse()` with `askAI()` (Section 14.2)
- [ ] Add SSE streaming for AI responses
- [ ] Add rate-limit handling

### Phase 4: Sources Page Backend

- [ ] Implement CRUD endpoints (Section 14.6)
- [ ] Replace `MOCK_CHANNELS` in `SourcesPage.tsx` with API calls
- [ ] Add Telegram Bot API integration for channel metadata/validation
- [ ] Wire channel sync status to real pipeline state

### Phase 5: Production Hardening

- [x] ~~Delete legacy components~~ (done 2026-02-28)
- [x] ~~Clean unused imports across all files~~ (done 2026-02-28)
- [x] ~~Extract graph page to separate app~~ (done 2026-02-28)
- [x] ~~Centralize TrackedChannel type in data.ts~~ (done 2026-02-28)
- [x] ~~Remove dead code: getDailyBriefing, graph API functions~~ (done 2026-02-28)
- [ ] Implement per-tier lazy loading or aggregating RPC
- [ ] Add pagination support for detail pages (`allTopics`, `allChannels`, `allAudience`)
- [ ] Connect real-time notifications (Section 14.4)
- [ ] Per-widget React error boundaries

---

## Appendix A: Complete `AppData` Key Reference Table

| Key | Type | Bilingual? | Widget(s) | Notes |
|-----|------|-----------|-----------|-------|
| `communityHealth` | `CommunityHealthData` | Partial (`components` only) | W1: CommunityHealthScore | Main score not bilingual |
| `trendingTopics` | `BilingualData<TrendingTopic>` | Yes | W2: TrendingTopicsFeed | |
| `communityBrief` | `CommunityBriefData` | Partial (`mainBrief`, `expandedBrief`) | W3: CommunityBrief | KPI strings not bilingual |
| `topicBubbles` | `BilingualData<TopicBubble>` | Yes | W4: TopicLandscape | |
| `trendLines` | `BilingualData<TrendLine>` | Yes | W5: ConversationTrends | Keys must match `trendData` |
| `trendData` | `TrendDataPoint[]` | No | W5: ConversationTrends | Keys from `trendLines` |
| `heatmap` | `BilingualValue<HeatmapData>` | Yes | W6: ContentEngagementHeatmap | `BilingualValue` not `Data` |
| `questionCategories` | `BilingualData<QuestionCategory>` | Yes | W7: QuestionCloud | |
| `qaGap` | `BilingualData<QAGapItem>` | Yes | W7b: QuestionAnswerGap | |
| `lifecycleStages` | `BilingualData<LifecycleStage>` | Yes | W7c: TopicLifecycle | Includes Tailwind classes |
| `problems` | `BilingualData<ProblemCategory>` | Yes | W8: ProblemTracker | Trend inverted: + = worse |
| `serviceGaps` | `BilingualData<ServiceGap>` | Yes | W9: ServiceGapDetector | Needs `supplyLevel` |
| `satisfactionAreas` | `BilingualData<SatisfactionArea>` | Yes | W10: SatisfactionByArea | |
| `moodData` | `MoodDataPoint[]` | No | W11: MoodOverTime | Keys from `moodConfig` |
| `moodConfig` | `BilingualData<MoodConfig>` | Yes | W11: MoodOverTime | Needs `polarity` |
| `urgencySignals` | `BilingualData<UrgencySignal>` | Yes | W12: EmotionalUrgencyIndex | |
| `communityChannels` | `CommunityChannel[]` | No | W13: TopChannels | Uses `topTopicEN`/`topTopicRU` |
| `keyVoices` | `BilingualData<KeyVoice>` | Yes | W14: KeyVoices | |
| `hourlyActivity` | `HourlyActivityPoint[]` | No | W15: ActivityTimeline | |
| `weeklyActivity` | `WeeklyActivityPoint[]` | No | W15: ActivityTimeline | Uses `day`/`dayEN` |
| `recommendations` | `BilingualData<Recommendation>` | Yes | W16: RecommendationTracker | |
| `newcomerJourney` | `BilingualData<NewcomerJourneyStage>` | Yes | W17: NewcomerFlow | |
| `viralTopics` | `BilingualData<ViralTopic>` | Yes | W18: InformationVelocity | |
| `personas` | `BilingualData<Persona>` | Yes | W19: PersonaGallery | |
| `interests` | `BilingualData<InterestItem>` | Yes | W20: InterestRadar | `interest` = axis label |
| `origins` | `OriginCity[]` | No | W21: OriginMap | Uses `city`/`cityEN` |
| `integrationData` | `IntegrationDataPoint[]` | No | W22: IntegrationSpectrum | Keys from config |
| `integrationLevels` | `BilingualData<IntegrationLevel>` | Yes | W22: IntegrationSpectrum | |
| `integrationSeriesConfig` | `IntegrationSeriesConfig[]` | No | W22: IntegrationSpectrum | `label`/`labelRu` inline |
| `emergingInterests` | `BilingualData<EmergingInterest>` | Yes | W23: EmergingInterests | |
| `retentionFactors` | `BilingualData<RetentionFactor>` | Yes | W24: RetentionRiskGauge | |
| `churnSignals` | `BilingualData<ChurnSignal>` | Yes | W24: RetentionRiskGauge | |
| `growthFunnel` | `BilingualData<GrowthFunnelStage>` | Yes | W25: CommunityGrowthFunnel | Needs `role` |
| `decisionStages` | `BilingualData<DecisionStage>` | Yes | W26: DecisionStageTracker | |
| `voiceData` | `VoiceDataPoint[]` | No | W27: NewVsReturningVoice | |
| `topNewTopics` | `BilingualData<TopNewVoiceTopic>` | Yes | W27: NewVsReturningVoice | |
| `businessOpportunities` | `BilingualData<BusinessOpportunity>` | Yes | W28: BusinessOpportunityTracker | |
| `jobSeeking` | `BilingualData<JobSeekingItem>` | Yes | W29: JobMarketPulse | |
| `jobTrends` | `BilingualData<JobTrend>` | Yes | W29: JobMarketPulse | |
| `housingData` | `BilingualData<HousingItem>` | Yes | W30: HousingMarketPulse | |
| `housingHotTopics` | `BilingualData<HousingHotTopic>` | Yes | W30: HousingMarketPulse | |
| `weeklyShifts` | `BilingualData<WeeklyShiftItem>` | Yes | W31: WeekOverWeekShifts | Needs `isInverse` |
| `sentimentByTopic` | `BilingualData<SentimentByTopicItem>` | Yes | W32: SentimentByTopic | pos+neu+neg = 100 |
| `topPosts` | `BilingualData<TopPost>` | Yes | W33: ContentPerformance | |
| `contentTypePerformance` | `BilingualData<ContentTypePerf>` | Yes | W33: ContentPerformance | |
| `vitalityIndicators` | `BilingualData<VitalityIndicator>` | Yes | W34: CommunityVitalityScorecard | Needs `benchmarkLevel` |
| `allTopics` | `TopicDetail[]` | No | TopicsPage | `name`/`nameRu` inline |
| `allChannels` | `ChannelDetail[]` | No | ChannelsPage | Full detail objects |
| `allAudience` | `AudienceMember[]` | No | AudiencePage | Full member profiles |

---

*End of integration guide. For questions, refer to the TypeScript source in `/src/app/types/data.ts` as the single source of truth.*