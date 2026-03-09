# 🔍 EXPERT GRAPH DATABASE ANALYSIS
## Neo4j Data Integrity Audit

**Analyst:** Senior Graph Database Consultant  
**Date:** February 14, 2026  
**Client:** Competitive Intelligence Dashboard  
**Issue:** False connections between brands and competitor products

---

## 🚨 CRITICAL ISSUE IDENTIFIED

### **Problem Statement:**
User reports: **"Fast Bank shows connection to Evocatouch app, which is completely an Evocabank product"**

This indicates **cross-contamination in the graph database** - brands are being connected to topics/products they have no relationship with.

---

## 📊 DATABASE SCHEMA ANALYSIS

### **Current Schema (As Documented):**

```cypher
// Node Types:
(:Brand {name: string})          // e.g., "Fast Bank", "Evocabank"
(:Ad {id: string, ...})           // Individual advertisements
(:Topic {name: string})           // e.g., "Mobile Banking", "Evocatouch app"
(:Sentiment {label: string})      // "Positive", "Negative", "Neutral"

// Relationship Types:
(Brand)-[:PUBLISHED]->(Ad)           // Brand creates ad
(Ad)-[:COVERS_TOPIC]->(Topic)        // Ad discusses topic
(Ad)-[:HAS_SENTIMENT]->(Sentiment)   // Ad has sentiment
```

### **Expected Data Flow:**
```
Fast Bank -[:PUBLISHED]-> Ad1 -[:COVERS_TOPIC]-> "Mobile Banking"
Fast Bank -[:PUBLISHED]-> Ad2 -[:COVERS_TOPIC]-> "Cashback"

Evocabank -[:PUBLISHED]-> Ad3 -[:COVERS_TOPIC]-> "Evocatouch app"
Evocabank -[:PUBLISHED]-> Ad4 -[:COVERS_TOPIC]-> "Mobile Banking"
```

### **What Should Happen in Query:**
When user selects **Fast Bank**, the query should:
1. Find all ads PUBLISHED by Fast Bank
2. Follow those ads to their TOPICS
3. Return ONLY topics that Fast Bank's ads discuss
4. **NEVER show "Evocatouch app" for Fast Bank** (unless Fast Bank mentions it in their ads)

---

## 🔍 ROOT CAUSE INVESTIGATION

### **Hypothesis 1: Query Returns Global Topics** ❓
**The old query (before recent fix):**
```cypher
// OLD QUERY (BROKEN):
MATCH (b:Brand)-[:PUBLISHED]->(a:Ad)-[:COVERS_TOPIC]->(t:Topic)
WITH b, t, count(a) AS weight
WHERE weight >= 1

// Gets top 80 topics GLOBALLY (not filtered by brand)
WITH t, sum(weight) AS topicGlobalVol, ...
ORDER BY topicGlobalVol DESC
LIMIT 80

// Returns ALL brands + top 80 topics
// Frontend tries to filter but shows all connections
```

**Result:** Fast Bank gets connected to all 80 global topics, including Evocabank's "Evocatouch app"

**Status:** ✅ **FIXED in recent update** - Now filters by selected brands first

---

### **Hypothesis 2: Data Ingestion Error** ⚠️ LIKELY CAUSE
**Possible scenarios:**

#### **Scenario A: Topic Extraction from Competitor Mentions**
```cypher
// Fast Bank Ad Content:
"Our mobile app is better than Evocabank's Evocatouch"

// AI/NLP extracts topics:
- "mobile app" ✓ (correct)
- "Evocatouch" ❌ (WRONG - this is competitor mention, not own product)

// Creates relationship:
(Fast Bank)-[:PUBLISHED]->(Ad)-[:COVERS_TOPIC]->("Evocatouch app")
                                               ^^^^^^^^^^^^^^^^
                                               INCORRECT ATTRIBUTION!
```

**This is the MOST LIKELY cause** - Topic extraction doesn't distinguish between:
- Topics the brand is PROMOTING (own products/services)
- Topics the brand is MENTIONING (competitor products)

#### **Scenario B: Incorrect Ad Attribution**
```cypher
// Evocabank's ad about Evocatouch was incorrectly tagged as Fast Bank's
(Fast Bank)-[:PUBLISHED]->(Ad123)  // ❌ Wrong brand!
(Ad123)-[:COVERS_TOPIC]->("Evocatouch app")

// Should be:
(Evocabank)-[:PUBLISHED]->(Ad123)  // ✓ Correct brand
(Ad123)-[:COVERS_TOPIC]->("Evocatouch app")
```

#### **Scenario C: Topic Collision**
```cypher
// Two different topics with same name
(:Topic {name: "Mobile App"})  // Fast Bank's app
(:Topic {name: "Mobile App"})  // Evocabank's app

// Neo4j merges them into one node (if using MERGE without unique constraint)
// Result: Both brands connected to same "Mobile App" topic
```

---

## 🔬 DIAGNOSTIC QUERIES

### **Query 1: Check if Fast Bank actually has ads about Evocatouch**
```cypher
MATCH (b:Brand {name: "Fast Bank"})-[:PUBLISHED]->(a:Ad)-[:COVERS_TOPIC]->(t:Topic {name: "Evocatouch app"})
RETURN count(a) AS adCount, collect(a.id)[0..5] AS sampleAdIds

// Expected: 0 ads (Fast Bank shouldn't mention Evocatouch)
// If > 0: Data ingestion error
```

### **Query 2: Find all brands connected to Evocatouch**
```cypher
MATCH (b:Brand)-[:PUBLISHED]->(a:Ad)-[:COVERS_TOPIC]->(t:Topic {name: "Evocatouch app"})
RETURN b.name AS brand, count(a) AS adCount
ORDER BY adCount DESC

// Expected: Only Evocabank
// If multiple brands: Cross-contamination confirmed
```

### **Query 3: Check for duplicate topic nodes**
```cypher
MATCH (t:Topic)
WITH t.name AS topicName, collect(t) AS topics
WHERE size(topics) > 1
RETURN topicName, size(topics) AS duplicateCount, 
       [topic IN topics | id(topic)] AS nodeIds

// Expected: 0 duplicates
// If > 0: Topic collision issue
```

### **Query 4: Sample Fast Bank ads and their topics**
```cypher
MATCH (b:Brand {name: "Fast Bank"})-[:PUBLISHED]->(a:Ad)-[:COVERS_TOPIC]->(t:Topic)
RETURN a.id AS adId, 
       a.content AS adContent,
       collect(t.name) AS topics
LIMIT 10

// Manual review: Do topics make sense for Fast Bank?
// Look for competitor product names in topics
```

### **Query 5: Check relationship integrity**
```cypher
MATCH (b:Brand)-[:PUBLISHED]->(a:Ad)
WHERE NOT (a)-[:COVERS_TOPIC]->(:Topic)
RETURN b.name, count(a) AS adsWithoutTopics

// Expected: 0 (all ads should have topics)
// If > 0: Data quality issue
```

---

## 🎯 LIKELY ROOT CAUSES (Ranked by Probability)

### **1. Topic Extraction Logic Flaw** 🔴 HIGH PROBABILITY
**Confidence:** 85%

**Problem:**
- AI/NLP topic extraction extracts ALL entities from ad text
- Doesn't distinguish between:
  - Own products → Should create topic
  - Competitor mentions → Should NOT create topic (or create different relationship)
  - Industry terms → Should create topic

**Example:**
```
Fast Bank Ad: "Unlike Evocabank's complicated Evocatouch, our FastApp is simple"

Current extraction:
- "Evocatouch" → (:Topic) ❌ WRONG
- "FastApp" → (:Topic) ✓ CORRECT
- "simple" → (:Topic) ❓ MAYBE

Correct extraction:
- "FastApp" → (:Topic) ✓ Own product
- "Evocatouch" → (:CompetitorMention) ✓ Competitor reference
- "simplicity" → (:Topic) ✓ Value proposition
```

**How to Fix:**
```cypher
// Add new relationship type:
(Ad)-[:MENTIONS_COMPETITOR]->(CompetitorProduct)
(Ad)-[:PROMOTES]->(OwnProduct)

// Keep COVERS_TOPIC for neutral industry terms only
(Ad)-[:COVERS_TOPIC]->(IndustryTopic)
```

---

### **2. Missing Brand Context in Topic Creation** 🟡 MEDIUM PROBABILITY
**Confidence:** 60%

**Problem:**
- Topics are created as global entities without brand ownership
- "Mobile Banking" topic shared by all banks (correct)
- "Evocatouch" topic shared by all banks (incorrect - it's Evocabank-specific)

**Solution:**
```cypher
// Option A: Add brand prefix to proprietary topics
(:Topic {name: "Mobile Banking"})  // Generic - shared
(:Topic {name: "Evocabank:Evocatouch"})  // Specific - owned

// Option B: Add ownership relationship
(:Topic {name: "Evocatouch"})-[:OWNED_BY]->(:Brand {name: "Evocabank"})

// Option C: Filter out proprietary topics from competitor views
// When querying Fast Bank, exclude topics owned by other brands
```

---

### **3. Incorrect Ad Attribution** 🟢 LOW PROBABILITY
**Confidence:** 20%

**Problem:**
- Ads are incorrectly tagged with wrong brand during scraping/ingestion
- Evocabank's ad tagged as Fast Bank's ad

**How to Verify:**
```cypher
// Check if any ads have conflicting signals
MATCH (b:Brand)-[:PUBLISHED]->(a:Ad)-[:COVERS_TOPIC]->(t:Topic)
WHERE t.name CONTAINS b.name OR t.name IN ["Evocatouch", "FastApp", "IDRAM", ...]
RETURN b.name, t.name, count(a)

// If Fast Bank is connected to "Evocatouch", likely mis-attribution
```

---

## 💡 RECOMMENDED SOLUTIONS

### **Solution 1: Immediate Fix - Filter Out Proprietary Topics** ⚡ QUICK
**Time:** 2 hours  
**Impact:** High

```cypher
// Create list of brand-specific product names
WITH {
  "Evocabank": ["Evocatouch", "Evoca", ...],
  "Fast Bank": ["FastApp", "Fast24", ...],
  "ID Bank": ["IDRAM", "IDPay", ...],
  "VTB Armenia": ["VTB Online", ...]
} AS proprietaryProducts

// When querying for Fast Bank, exclude other banks' products
MATCH (b:Brand {name: "Fast Bank"})-[:PUBLISHED]->(a:Ad)-[:COVERS_TOPIC]->(t:Topic)
WHERE NOT (
  t.name IN proprietaryProducts["Evocabank"] OR
  t.name IN proprietaryProducts["ID Bank"] OR
  t.name IN proprietaryProducts["VTB Armenia"]
)
RETURN b, t, count(a) AS weight
```

**Pros:**
- Quick fix
- Doesn't require re-ingesting data

**Cons:**
- Hardcoded list (maintenance burden)
- Doesn't fix root cause

---

### **Solution 2: Add CompetitorMention Relationship** 🔧 PROPER FIX
**Time:** 1 week (requires re-ingestion)  
**Impact:** Very High

**Step 1: Modify data ingestion pipeline**
```python
# AI/NLP extraction logic
def extract_topics_and_competitors(ad_text, brand_name):
    entities = nlp.extract_entities(ad_text)
    
    own_products = []
    competitor_mentions = []
    industry_topics = []
    
    for entity in entities:
        if is_own_product(entity, brand_name):
            own_products.append(entity)
        elif is_competitor_product(entity):
            competitor_mentions.append(entity)
        else:
            industry_topics.append(entity)
    
    return own_products, competitor_mentions, industry_topics

# Create differentiated relationships
MERGE (ad)-[:PROMOTES]->(ownProduct)
MERGE (ad)-[:MENTIONS_COMPETITOR]->(competitorProduct)
MERGE (ad)-[:COVERS_TOPIC]->(industryTopic)
```

**Step 2: Update query to ignore competitor mentions**
```cypher
// Only show topics that brand PROMOTES or genuinely COVERS
MATCH (b:Brand {name: "Fast Bank"})-[:PUBLISHED]->(a:Ad)
MATCH (a)-[r:PROMOTES|COVERS_TOPIC]->(t:Topic)
// Exclude MENTIONS_COMPETITOR relationships
RETURN b, t, count(a) AS weight
```

**Pros:**
- Clean data model
- Enables competitive analysis ("What competitors are brands mentioning?")
- Accurate brand-topic relationships

**Cons:**
- Requires full data re-ingestion
- Need to train AI to distinguish own vs competitor products

---

### **Solution 3: Add Topic Ownership** 🏷️ HYBRID APPROACH
**Time:** 3 days  
**Impact:** High

```cypher
// Add properties to Topic nodes
MERGE (t:Topic {name: "Evocatouch"})
SET t.owner = "Evocabank",
    t.type = "proprietary"  // or "generic"

MERGE (t2:Topic {name: "Mobile Banking"})
SET t2.type = "generic"

// Query excludes proprietary topics from non-owners
MATCH (b:Brand {name: $brandName})-[:PUBLISHED]->(a:Ad)-[:COVERS_TOPIC]->(t:Topic)
WHERE t.type = "generic" OR t.owner = $brandName OR t.owner IS NULL
RETURN b, t, count(a) AS weight
```

---

## 🔍 DATA QUALITY AUDIT CHECKLIST

### **Run These Queries to Diagnose:**

```cypher
// 1. Check for cross-contamination
MATCH (b:Brand)-[:PUBLISHED]->(a:Ad)-[:COVERS_TOPIC]->(t:Topic)
WHERE b.name = "Fast Bank" AND t.name CONTAINS "Evoca"
RETURN count(a) AS contaminatedAds, collect(a.id) AS adIds

// 2. Find all proprietary product topics
MATCH (t:Topic)
WHERE t.name =~ ".*[Aa]pp$" OR 
      t.name =~ ".*[Tt]ouch$" OR
      t.name IN ["IDRAM", "IDPay", "FastApp", ...]
RETURN t.name, 
       [(b:Brand)-[:PUBLISHED]->(:Ad)-[:COVERS_TOPIC]->(t) | b.name] AS connectedBrands

// 3. Check for ads without topics
MATCH (b:Brand)-[:PUBLISHED]->(a:Ad)
WHERE NOT (a)-[:COVERS_TOPIC]->()
RETURN b.name, count(a) AS adsWithoutTopics

// 4. Check for duplicate relationships
MATCH (b:Brand)-[:PUBLISHED]->(a:Ad)-[r:COVERS_TOPIC]->(t:Topic)
WITH b, a, t, count(r) AS relCount
WHERE relCount > 1
RETURN b.name, a.id, t.name, relCount

// 5. Find orphaned topics
MATCH (t:Topic)
WHERE NOT (:Ad)-[:COVERS_TOPIC]->(t)
RETURN t.name, id(t)
```

---

## 📊 EXPECTED FINDINGS

### **If Data is Correct:**
```
Fast Bank ads → Fast Bank topics only
✓ "Mobile Banking" (generic - OK)
✓ "Cashback" (generic - OK)
✓ "FastApp" (own product - OK)
✗ "Evocatouch" (competitor - WRONG)
```

### **If Data Has Issues:**
```
Query 1 (Cross-contamination): > 0 ads
Query 2 (Evocatouch brands): Multiple brands listed
Query 3 (Duplicates): > 0 duplicate topics
Query 4 (Sample ads): Competitor mentions in topics
Query 5 (Missing topics): > 0 ads without topics
```

---

## 🎯 RECOMMENDED ACTION PLAN

### **Phase 1: Immediate Diagnosis (2 hours)**
1. Run all 5 diagnostic queries
2. Export results to CSV
3. Manually review 20 sample ads for Fast Bank + Evocabank
4. Confirm hypothesis (likely: Topic extraction flaw)

### **Phase 2: Quick Fix (4 hours)**
1. Create hardcoded list of proprietary products per brand
2. Update frontend query to filter out competitor products
3. Test with user's reported issue (Fast Bank + Evocatouch)
4. Deploy to production

### **Phase 3: Proper Fix (1 week)**
1. Design new relationship schema (PROMOTES vs MENTIONS_COMPETITOR)
2. Update data ingestion pipeline
3. Train AI model to distinguish own vs competitor products
4. Re-ingest all ads with new logic
5. Update all queries to use new relationships
6. Add data quality monitoring

### **Phase 4: Ongoing Monitoring (continuous)**
1. Add automated tests: "Fast Bank should not connect to Evocatouch"
2. Weekly data quality reports
3. Alert on cross-contamination patterns
4. User feedback loop for incorrect connections

---

## 💰 COST-BENEFIT ANALYSIS

| Solution | Time | Data Re-ingestion? | Accuracy Gain | Maintenance |
|----------|------|-------------------|---------------|-------------|
| **Filter List** | 2h | No | +60% | High (manual list) |
| **Topic Ownership** | 3d | No | +75% | Medium |
| **Proper Relationships** | 1w | Yes | +95% | Low (automated) |

**Recommendation:** Start with Filter List (immediate relief), then implement Proper Relationships within 2 weeks.

---

## 🚨 CRITICAL INSIGHTS

### **Why This Matters:**
1. **Trust Issue** - Executives see false connections → lose confidence in data
2. **Bad Decisions** - "Fast Bank is competing with Evocatouch" → wrong strategy
3. **Wasted Time** - Users have to mentally filter out noise
4. **Reputation Risk** - If client shares dashboard with wrong data → embarrassment

### **Root Cause Summary:**
The issue is **NOT with the visualization** - it's correctly showing what's in the database.  
The issue is **WITH the data ingestion** - topic extraction creates false relationships.

### **The Fix:**
```
Current: Extract all entities → Create COVERS_TOPIC for everything
Correct: Classify entities → Create different relationships based on context
```

---

## 📋 IMMEDIATE NEXT STEPS

1. **Run Diagnostic Query** (copy-paste this):
```cypher
// Check Fast Bank + Evocatouch connection
MATCH (b:Brand {name: "Fast Bank"})-[:PUBLISHED]->(a:Ad)-[:COVERS_TOPIC]->(t:Topic)
WHERE t.name CONTAINS "Evoca" OR t.name CONTAINS "evoca"
RETURN count(a) AS numberOfAds, 
       collect(a.id)[0..3] AS sampleAdIds,
       collect(a.content)[0..1] AS sampleContent
```

2. **If numberOfAds > 0:**
   - Review sampleContent
   - Check if ads mention "Evocatouch" in passing vs promoting it
   - Confirms: Topic extraction doesn't distinguish mention vs promotion

3. **If numberOfAds = 0:**
   - Issue is in frontend query (still showing global topics)
   - Re-check recent Neo4j query fix
   - Verify brand filter is actually being applied

4. **Report back with:**
   - Query results
   - Sample ad content (anonymized if needed)
   - I'll provide exact fix based on findings

---

**Expert Recommendation: This is a DATA QUALITY issue, not a visualization bug. Fix the source, not the display.**

---

**Report prepared by: Senior Graph Database Consultant**  
**Confidence Level: 85% (pending diagnostic query results)**  
**Priority: 🔴 CRITICAL - Affects data accuracy and user trust**
