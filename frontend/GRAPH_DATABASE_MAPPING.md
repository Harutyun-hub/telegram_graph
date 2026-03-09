# Graph Database to Frontend Mapping Documentation

## Overview
This document describes the complete data mapping logic between the Neo4j graph database and the frontend visualization dashboard. It specifies how data should be structured, queried, and transformed for optimal visualization and analysis.

**🆕 UPDATED FOR 15-NODE-TYPE SYSTEM**: The dashboard now supports a rich multi-dimensional graph with 15 entity types instead of just 2 (brand/topic).

---

## 1. Data Model Architecture

### 1.1 Core Entity Types (15 Node Types)

The graph database contains **15 primary node types** for comprehensive competitive intelligence:

#### **Brand Nodes** ⭐ (Primary)
- **Label**: `Brand`
- **Purpose**: Represents banks/financial institutions being analyzed
- **Frontend Type**: `'brand'`
- **Visualization**: Cyan/teal glowing orbs (18px base size)
- **Color**: #06b6d4 (Cyan-500)

#### **Product Nodes** 📦
- **Label**: `Product`
- **Purpose**: Financial products (credit cards, loans, accounts)
- **Frontend Type**: `'product'`
- **Visualization**: Green glowing orbs (14px base size)
- **Color**: #10b981 (Green-500)
- **Required Properties**: `category` (e.g., "Credit_Card", "Savings_Account")

#### **Audience Nodes** 👥
- **Label**: `Audience`
- **Purpose**: Target customer segments
- **Frontend Type**: `'audience'`
- **Visualization**: Purple glowing orbs (12px base size)
- **Color**: #a855f7 (Purple-500)
- **Required Properties**: `segmentType` (e.g., "behavioral", "demographic")

#### **Pain Point Nodes** ⚠️
- **Label**: `PainPoint`
- **Purpose**: Customer problems/frustrations
- **Frontend Type**: `'painpoint'`
- **Visualization**: Red glowing orbs (12px base size)
- **Color**: #ef4444 (Red-500)
- **Required Properties**: `severity` ("high" | "medium" | "low")

#### **Value Proposition Nodes** 💡
- **Label**: `ValueProp`
- **Purpose**: Product benefits/solutions
- **Frontend Type**: `'valueprop'`
- **Visualization**: Yellow glowing orbs (10px base size)
- **Color**: #eab308 (Yellow-500)

#### **Topic Nodes** 🏷️ (Legacy, still supported)
- **Label**: `Topic`
- **Purpose**: Discussion themes, hashtags (less important now)
- **Frontend Type**: `'topic'`
- **Visualization**: Orange/coral glowing orbs (8px base size)
- **Color**: #f97316 (Orange-500)

#### **Intent Nodes** 🎯
- **Label**: `Intent`
- **Purpose**: User goals/motivations
- **Frontend Type**: `'intent'`
- **Visualization**: Indigo glowing orbs (10px base size)
- **Color**: #6366f1 (Indigo-500)

#### **Competitor Nodes** 🥊
- **Label**: `Competitor`
- **Purpose**: Competing brands (not primary focus)
- **Frontend Type**: `'competitor'`
- **Visualization**: Pink glowing orbs (16px base size)
- **Color**: #ec4899 (Pink-500)

#### **CTA Nodes** 📢
- **Label**: `CTA`
- **Purpose**: Call-to-action messaging
- **Frontend Type**: `'cta'`
- **Visualization**: Violet glowing orbs (10px base size)
- **Color**: #8b5cf6 (Violet-500)

#### **Platform Nodes** 📱
- **Label**: `Platform`
- **Purpose**: Social media/advertising platforms
- **Frontend Type**: `'platform'`
- **Visualization**: Cyan glowing orbs (8px base size)
- **Color**: #06b6d4 (Cyan-500)

#### **Format Nodes** 🎨
- **Label**: `Format`
- **Purpose**: Ad creative formats (video, image, carousel)
- **Frontend Type**: `'format'`
- **Visualization**: Teal glowing orbs (8px base size)
- **Color**: #14b8a6 (Teal-500)

#### **Engagement Nodes** 📊
- **Label**: `Engagement`
- **Purpose**: Engagement metrics/patterns
- **Frontend Type**: `'engagement'`
- **Visualization**: Lime green glowing orbs (8px base size)
- **Color**: #84cc16 (Lime-500)

#### **Sentiment Nodes** 😊
- **Label**: `Sentiment`
- **Purpose**: Emotional tone/sentiment data
- **Frontend Type**: `'sentiment'`
- **Visualization**: Amber glowing orbs (8px base size)
- **Color**: #f59e0b (Amber-500)

#### **Time Period Nodes** 📅
- **Label**: `TimePeriod`
- **Purpose**: Temporal contexts (seasons, quarters)
- **Frontend Type**: `'timeperiod'`
- **Visualization**: Grey glowing orbs (6px base size)
- **Color**: #71717a (Zinc-500)

#### **Ad Nodes** (Not visualized in graph, used for details)
- **Label**: `Ad`
- **Purpose**: Individual advertisements
- **Frontend Type**: `'ad'`
- **Visualization**: Not shown in graph, appears in Node Inspector sidebar only

### 1.2 Relationship Types

#### **Primary Relationships**:
```cypher
(Brand)-[r:OFFERS]->(Product)           // Brand offers product
(Product)-[r:POSITIONED_FOR]->(Audience) // Product targets audience
(Product)-[r:ADDRESSES]->(PainPoint)     // Product solves pain point
(Product)-[r:DELIVERS]->(ValueProp)      // Product provides value
(Audience)-[r:EXPERIENCES]->(PainPoint)  // Audience has pain point
(Product)-[r:TRIGGERS]->(Intent)         // Product drives intent
(Brand)-[r:COMPETES_WITH]->(Competitor)  // Brand competes with competitor
```

#### **Legacy Relationships** (backwards compatible):
```cypher
(Brand)-[r:MENTIONS]->(Topic)            // Old system, still supported
```

#### **Metadata Relationships**:
```cypher
(Brand)-[r:ADVERTISES_ON]->(Platform)
(Ad)-[r:USES_FORMAT]->(Format)
(Ad)-[r:GENERATES]->(Engagement)
(Ad)-[r:HAS_SENTIMENT]->(Sentiment)
```

**Weight Property**: All relationships have `r.weight` (integer 1-100) representing strength/frequency

---

## 2. Required Node Properties

### 2.1 Brand Node Properties

```javascript
{
  id: string,              // e.g., "brand_arm econombank"
  name: string,            // e.g., "Armeconom Bank"
  type: "brand",
  connectionCount: number, // Total connections
  createdAt: timestamp     // Optional
}
```

### 2.2 Product Node Properties

```javascript
{
  id: string,              // e.g., "product_credit_card"
  name: string,            // e.g., "Credit Card"
  type: "product",
  category: string,        // e.g., "Credit_Card"
  connectionCount: number, // Total connections
  createdAt: timestamp     // Optional
}
```

### 2.3 Audience Node Properties

```javascript
{
  id: string,              // e.g., "audience_young_professionals"
  name: string,            // e.g., "Young Professionals"
  type: "audience",
  segmentType: string,     // e.g., "demographic"
  connectionCount: number, // Total connections
  createdAt: timestamp     // Optional
}
```

### 2.4 Pain Point Node Properties

```javascript
{
  id: string,              // e.g., "painpoint_high_interest_rates"
  name: string,            // e.g., "High Interest Rates"
  type: "painpoint",
  severity: string,        // e.g., "high"
  connectionCount: number, // Total connections
  createdAt: timestamp     // Optional
}
```

### 2.5 Value Proposition Node Properties

```javascript
{
  id: string,              // e.g., "valueprop_low_interest_rates"
  name: string,            // e.g., "Low Interest Rates"
  type: "valueprop",
  connectionCount: number, // Total connections
  createdAt: timestamp     // Optional
}
```

### 2.6 Topic Node Properties

```javascript
{
  id: string,              // e.g., "topic_sustainability"
  name: string,            // e.g., "#Sustainability"
  type: "topic",
  connectionCount: number, // Total connections
  createdAt: timestamp     // Optional
}
```

### 2.7 Intent Node Properties

```javascript
{
  id: string,              // e.g., "intent_purchase_credit_card"
  name: string,            // e.g., "Purchase Credit Card"
  type: "intent",
  connectionCount: number, // Total connections
  createdAt: timestamp     // Optional
}
```

### 2.8 Competitor Node Properties

```javascript
{
  id: string,              // e.g., "competitor_bank_of_america"
  name: string,            // e.g., "Bank of America"
  type: "competitor",
  connectionCount: number, // Total connections
  createdAt: timestamp     // Optional
}
```

### 2.9 CTA Node Properties

```javascript
{
  id: string,              // e.g., "cta_apply_now"
  name: string,            // e.g., "Apply Now"
  type: "cta",
  connectionCount: number, // Total connections
  createdAt: timestamp     // Optional
}
```

### 2.10 Platform Node Properties

```javascript
{
  id: string,              // e.g., "platform_facebook"
  name: string,            // e.g., "Facebook"
  type: "platform",
  connectionCount: number, // Total connections
  createdAt: timestamp     // Optional
}
```

### 2.11 Format Node Properties

```javascript
{
  id: string,              // e.g., "format_video"
  name: string,            // e.g., "Video"
  type: "format",
  connectionCount: number, // Total connections
  createdAt: timestamp     // Optional
}
```

### 2.12 Engagement Node Properties

```javascript
{
  id: string,              // e.g., "engagement_high_likes"
  name: string,            // e.g., "High Likes"
  type: "engagement",
  connectionCount: number, // Total connections
  createdAt: timestamp     // Optional
}
```

### 2.13 Sentiment Node Properties

```javascript
{
  id: string,              // e.g., "sentiment_positive"
  name: string,            // e.g., "Positive"
  type: "sentiment",
  connectionCount: number, // Total connections
  createdAt: timestamp     // Optional
}
```

### 2.14 Time Period Node Properties

```javascript
{
  id: string,              // e.g., "timeperiod_q1_2024"
  name: string,            // e.g., "Q1 2024"
  type: "timeperiod",
  connectionCount: number, // Total connections
  createdAt: timestamp     // Optional
}
```

### 2.15 Ad Node Properties (For Inspector Sidebar)

```javascript
{
  id: string,              // Unique identifier
  title: string,           // Ad title/headline
  description: string,     // Ad copy/content
  platform: string,        // "Instagram" | "Facebook" | "Twitter" | etc.
  imageUrl: string,        // Optional: URL to ad creative
  videoUrl: string,        // Optional: URL to video content
  impressions: number,     // Optional: View count
  engagement: number,      // Optional: Likes/comments/shares
  sentiment: string,       // "positive" | "negative" | "neutral"
  publishedAt: timestamp,  // When ad was published
  topics: string[]         // Array of topic IDs this ad discusses
}
```

---

## 3. API Endpoints & Data Mapping

### 3.1 Graph Data Endpoint

**Endpoint**: `GET /make-server-14007ead/graph-data`

**Query Parameters**:
```typescript
{
  brands?: string[],         // Array of brand names to filter (e.g., ["Nike", "Adidas"])
  topics?: string[],         // Array of topic names to filter
  sentiments?: string[],     // Array of sentiments to filter
  connectionStrength?: number, // Minimum connection weight (1-10)
  startDate?: string,        // ISO date string (e.g., "2024-01-01")
  endDate?: string,          // ISO date string (e.g., "2024-12-31")
  limit?: number             // Max number of nodes to return (default: 100)
}
```

**Response Format**:
```typescript
{
  nodes: Array<{
    id: string,              // Unique node ID
    name: string,            // Display name
    type: "brand" | "topic", // Node type
    val: number,             // Size multiplier for visualization (1-10)
    connectionCount: number, // Number of connections
    sentiment?: string       // Optional sentiment
  }>,
  links: Array<{
    source: string,          // Source node ID (brand ID)
    target: string,          // Target node ID (topic ID)
    weight: number,          // Connection strength (1-100)
    value: number            // Synonym for weight (used by force-graph)
  }>
}
```

**Neo4j Query Logic**:
```cypher
// Step 1: Filter brands based on query parameters
MATCH (b:Brand)
WHERE ($brands IS NULL OR b.name IN $brands)
  AND ($startDate IS NULL OR b.createdAt >= datetime($startDate))
  AND ($endDate IS NULL OR b.createdAt <= datetime($endDate))

// Step 2: Find connected topics with weight filtering
MATCH (b)-[r:MENTIONS]->(t:Topic)
WHERE ($connectionStrength IS NULL OR r.weight >= $connectionStrength)
  AND ($topics IS NULL OR t.name IN $topics)

// Step 3: Aggregate and return nodes
WITH COLLECT(DISTINCT {
  id: b.id,
  name: b.name,
  type: 'brand',
  val: b.connectionCount / 10.0,
  connectionCount: b.connectionCount,
  sentiment: b.sentiment
}) AS brandNodes,
COLLECT(DISTINCT {
  id: t.id,
  name: t.name,
  type: 'topic',
  val: t.connectionCount / 10.0,
  connectionCount: t.connectionCount
}) AS topicNodes

// Step 4: Collect relationships
MATCH (b:Brand)-[r:MENTIONS]->(t:Topic)
WHERE ($brands IS NULL OR b.name IN $brands)
  AND ($topics IS NULL OR t.name IN $topics)
  AND ($connectionStrength IS NULL OR r.weight >= $connectionStrength)

WITH brandNodes, topicNodes, COLLECT({
  source: b.id,
  target: t.id,
  weight: r.weight,
  value: r.weight
}) AS links

RETURN {
  nodes: brandNodes + topicNodes,
  links: links
}
```

**Important Mapping Rules**:
1. **Node Size (`val`)**: Calculate as `connectionCount / 10.0` (range 0.1-10)
2. **Link Thickness**: Directly maps to `weight` property (1-100 scale)
3. **Top 15 Topics**: When no filters applied, return only top 15 most-discussed topics per brand
4. **Deduplication**: Use `COLLECT(DISTINCT {...})` to avoid duplicate nodes

---

### 3.2 Top Brands Endpoint

**Endpoint**: `GET /make-server-14007ead/top-brands`

**Response Format**:
```typescript
Array<{
  name: string,           // Brand name
  connectionCount: number, // Number of topic connections
  adsCount: number        // Number of ads created
}>
```

**Neo4j Query**:
```cypher
MATCH (b:Brand)-[r:MENTIONS]->(t:Topic)
WITH b, COUNT(DISTINCT t) AS connectionCount

OPTIONAL MATCH (b)-[:CREATED]->(a:Ad)
WITH b, connectionCount, COUNT(a) AS adsCount

RETURN {
  name: b.name,
  connectionCount: connectionCount,
  adsCount: adsCount
}
ORDER BY connectionCount DESC
LIMIT 10
```

---

### 3.3 Trending Topics Endpoint

**Endpoint**: `GET /make-server-14007ead/trending-topics`

**Response Format**:
```typescript
Array<{
  name: string,           // Topic name
  mentionCount: number,   // Total mentions
  brandCount: number      // Number of brands discussing this topic
}>
```

**Neo4j Query**:
```cypher
MATCH (b:Brand)-[r:MENTIONS]->(t:Topic)
WITH t, SUM(r.weight) AS mentionCount, COUNT(DISTINCT b) AS brandCount
RETURN {
  name: t.name,
  mentionCount: mentionCount,
  brandCount: brandCount
}
ORDER BY mentionCount DESC
LIMIT 20
```

---

### 3.4 Node Details Endpoint

**Endpoint**: `GET /make-server-14007ead/node/:id`

**Response Format**:
```typescript
{
  node: {
    id: string,
    name: string,
    type: "brand" | "topic",
    connectionCount: number,
    sentiment?: string
  },
  connections: Array<{
    id: string,
    name: string,
    type: "brand" | "topic",
    weight: number
  }>,
  ads?: Array<{           // Only for brand nodes
    id: string,
    title: string,
    description: string,
    platform: string,
    imageUrl?: string,
    sentiment: string,
    publishedAt: string
  }>,
  insights?: {            // AI-generated insights
    summary: string,
    keyThemes: string[],
    sentiment: string
  }
}
```

**Neo4j Query for Brand Node**:
```cypher
// Get brand details
MATCH (b:Brand {id: $nodeId})

// Get connected topics
OPTIONAL MATCH (b)-[r:MENTIONS]->(t:Topic)
WITH b, COLLECT({
  id: t.id,
  name: t.name,
  type: 'topic',
  weight: r.weight
}) AS connections

// Get brand's ads
OPTIONAL MATCH (b)-[:CREATED]->(a:Ad)
WITH b, connections, COLLECT({
  id: a.id,
  title: a.title,
  description: a.description,
  platform: a.platform,
  imageUrl: a.imageUrl,
  sentiment: a.sentiment,
  publishedAt: toString(a.publishedAt)
}) AS ads

RETURN {
  node: {
    id: b.id,
    name: b.name,
    type: 'brand',
    connectionCount: SIZE(connections),
    sentiment: b.sentiment
  },
  connections: connections,
  ads: ads
}
```

---

### 3.5 AI Query Endpoint

**Endpoint**: `POST /make-server-14007ead/ai-query`

**Request Body**:
```typescript
{
  query: string  // Natural language query (e.g., "Show me Nike's sustainability topics")
}
```

**Response Format**:
```typescript
{
  answer: string,         // AI-generated natural language answer
  graphData?: {           // Optional filtered graph data
    nodes: [...],
    links: [...]
  },
  suggestions?: string[]  // Follow-up query suggestions
}
```

**Processing Logic**:
1. Parse natural language query using Gemini AI
2. Extract entities (brand names, topic keywords, time ranges)
3. Convert to Cypher query parameters
4. Execute graph query with extracted filters
5. Generate natural language summary of results
6. Return both answer and graph data

---

## 4. Filter Application Logic

### 4.1 Brand Filtering
When brands are selected in filters:
- **Action**: Include ONLY selected brands and their connected topics
- **Cypher**: `WHERE b.name IN $brands`
- **Frontend**: Highlights selected brand nodes, dims others

### 4.2 Topic Filtering
When topics are selected:
- **Action**: Include ONLY selected topics and brands connected to them
- **Cypher**: `WHERE t.name IN $topics`
- **Frontend**: Highlights selected topic nodes, dims others

### 4.3 Sentiment Filtering
When sentiments are selected:
- **Action**: Include only brands/topics with matching sentiment
- **Cypher**: `WHERE b.sentiment IN $sentiments`
- **Frontend**: Filters nodes before visualization

### 4.4 Connection Strength Filtering
When connection strength slider is adjusted (1-10):
- **Action**: Hide relationships below threshold
- **Cypher**: `WHERE r.weight >= $connectionStrength * 10`
- **Frontend**: Removes weak links, isolates nodes with no connections

### 4.5 Date Range Filtering
When date range is selected:
- **Action**: Include only data within time window
- **Cypher**: 
  ```cypher
  WHERE a.publishedAt >= datetime($startDate) 
    AND a.publishedAt <= datetime($endDate)
  ```
- **Frontend**: Updates graph data completely

---

## 5. Data Transformation Pipeline

### 5.1 Backend → Frontend Flow

```
┌─────────────────┐
│   Neo4j Graph   │
│   - Brands      │
│   - Topics      │
│   - Ads         │
└────────┬────────┘
         │
         ↓ Cypher Query
         │
┌────────┴────────┐
│  Neo4j Service  │
│  - Execute Query│
│  - Map Results  │
└────────┬────────┘
         │
         ↓ Raw Neo4j Records
         │
┌────────┴────────┐
│  API Endpoint   │
│  - Transform    │
│  - Validate     │
│  - Add Metadata │
└────────┬────────┘
         │
         ↓ JSON Response
         │
┌────────┴────────┐
│  Frontend API   │
│  - Fetch Data   │
│  - Cache        │
└────────┬────────┘
         │
         ↓ Graph Data Object
         │
┌────────┴────────────┐
│ GraphVisualization  │
│ - Force-Graph       │
│ - 3D Rendering      │
│ - Physics Sim       │
└─────────────────────┘
```

### 5.2 Node ID Generation

**Convention**: Use descriptive prefixes
```javascript
// Brand IDs
`brand_${brandName.toLowerCase().replace(/\s+/g, '_')}`
// Example: "brand_nike", "brand_adidas"

// Topic IDs
`topic_${topicName.toLowerCase().replace(/\s+/g, '_').replace('#', '')}`
// Example: "topic_sustainability", "topic_innovation"

// Ad IDs
`ad_${platform}_${uniqueId}`
// Example: "ad_instagram_abc123"
```

### 5.3 Weight Calculation

**Connection Weight** (1-100 scale):
```javascript
// Based on mention frequency
weight = Math.min(100, mentionCount * 2)

// Or based on engagement
weight = Math.min(100, (likes + comments * 2 + shares * 3) / 100)
```

**Node Size** (0.1-10 scale):
```javascript
val = Math.max(0.1, Math.min(10, connectionCount / 10.0))
```

---

## 6. Performance Optimization

### 6.1 Query Limits
- **Max Nodes**: 200 (100 brands + 100 topics)
- **Max Links**: 500
- **Top Topics**: 15 per brand (when unfiltered)
- **Query Timeout**: 5 seconds

### 6.2 Indexing Requirements

Create these indexes in Neo4j:
```cypher
// Node lookup optimization
CREATE INDEX brand_id FOR (b:Brand) ON (b.id);
CREATE INDEX brand_name FOR (b:Brand) ON (b.name);
CREATE INDEX topic_id FOR (t:Topic) ON (t.id);
CREATE INDEX topic_name FOR (t:Topic) ON (t.name);

// Date range queries
CREATE INDEX ad_published FOR (a:Ad) ON (a.publishedAt);

// Relationship weight filtering
CREATE INDEX mentions_weight FOR ()-[r:MENTIONS]-() ON (r.weight);
```

### 6.3 Caching Strategy
- **Frontend Cache**: 5 minutes for graph data
- **Backend Cache**: None (always fresh data from Neo4j)
- **Search Autocomplete**: Client-side filtering of loaded nodes

---

## 7. Example Data Scenarios

### 7.1 Initial Load (No Filters)

**Query**:
```
GET /make-server-14007ead/graph-data
```

**Expected Response**:
```json
{
  "nodes": [
    {
      "id": "brand_nike",
      "name": "Nike",
      "type": "brand",
      "val": 3.5,
      "connectionCount": 35,
      "sentiment": "positive"
    },
    {
      "id": "topic_sustainability",
      "name": "#Sustainability",
      "type": "topic",
      "val": 2.8,
      "connectionCount": 28
    }
  ],
  "links": [
    {
      "source": "brand_nike",
      "target": "topic_sustainability",
      "weight": 45,
      "value": 45
    }
  ]
}
```

### 7.2 Filtered by Brand

**Query**:
```
GET /make-server-14007ead/graph-data?brands=Nike,Adidas
```

**Expected Behavior**:
- Return only Nike and Adidas brand nodes
- Return only topics connected to these brands
- Include all connection weights

### 7.3 AI Query Processing

**Request**:
```json
{
  "query": "What sustainability topics is Nike focused on?"
}
```

**Backend Processing**:
1. Extract: brand="Nike", topicKeyword="sustainability"
2. Generate Cypher:
   ```cypher
   MATCH (b:Brand {name: 'Nike'})-[r:MENTIONS]->(t:Topic)
   WHERE t.name CONTAINS 'sustainability' OR t.name CONTAINS 'eco'
   RETURN b, r, t
   ORDER BY r.weight DESC
   ```
3. Generate AI summary from results

**Expected Response**:
```json
{
  "answer": "Nike is actively discussing 3 main sustainability topics: #Sustainability (45 mentions), #CircularEconomy (32 mentions), and #ZeroWaste (28 mentions). Their focus appears strongest on general sustainability initiatives.",
  "graphData": {
    "nodes": [...],
    "links": [...]
  },
  "suggestions": [
    "Compare Nike's sustainability focus with Adidas",
    "Show Nike's sustainability ads from last month"
  ]
}
```

---

## 8. Error Handling

### 8.1 Missing Data
```json
{
  "nodes": [],
  "links": [],
  "message": "No data found matching filters"
}
```

### 8.2 Invalid Filters
```json
{
  "error": "Invalid brand name",
  "code": "INVALID_FILTER"
}
```

### 8.3 Database Connection Error
```json
{
  "error": "Failed to connect to graph database",
  "code": "DB_CONNECTION_ERROR"
}
```

---

## 9. Testing Data Requirements

For proper frontend testing, provide:

### Minimum Dataset:
- **5-10 brands** with varied connection counts (10-50 each)
- **20-30 topics** with varied popularity
- **50-100 ads** distributed across brands
- **100-200 relationships** with varied weights (1-100)

### Recommended Distribution:
```
Brands:
- 2-3 high-activity brands (40+ topics each)
- 3-4 medium-activity brands (20-30 topics)
- 2-3 low-activity brands (5-10 topics)

Topics:
- 5 "trending" topics (connected to 5+ brands)
- 10 "popular" topics (connected to 2-4 brands)
- 15 "niche" topics (connected to 1 brand)

Sentiments:
- 60% Positive
- 20% Neutral
- 15% Negative
- 5% Urgent
```

---

## 10. Implementation Checklist

For the new graph database implementation:

- [ ] Create node types: `Brand`, `Topic`, `Ad`
- [ ] Create relationship types: `MENTIONS`, `CREATED`, `DISCUSSES`
- [ ] Add required properties to all nodes
- [ ] Create indexes for performance
- [ ] Implement `/graph-data` endpoint with all filter parameters
- [ ] Implement `/top-brands` endpoint
- [ ] Implement `/trending-topics` endpoint
- [ ] Implement `/node/:id` endpoint for detailed views
- [ ] Implement `/ai-query` endpoint with Gemini integration
- [ ] Add date range filtering support
- [ ] Add connection strength filtering
- [ ] Test with minimum dataset
- [ ] Verify response formats match specifications
- [ ] Add error handling for all edge cases
- [ ] Implement query timeouts (5s max)
- [ ] Add pagination support for large result sets

---

## 11. Contact & Questions

If the data structure needs clarification or adjustments:
1. Check this document first for specifications
2. Verify your Cypher queries return exact format specified
3. Test with sample data before full implementation
4. Ensure all IDs are consistent across queries

**Critical Success Factors**:
- Node IDs must be stable and consistent
- Weights must be on 1-100 scale
- Node type must be exactly `"brand"` or `"topic"` (lowercase)
- All timestamps must be ISO 8601 format
- Response must include both `nodes` and `links` arrays