# Neo4j Setup & Troubleshooting Guide

## 🎯 Expected Database Schema

Your Neo4j database should have this structure:

### Node Labels
```cypher
// Check your labels
CALL db.labels()

Expected output:
- Brand
- Ad
- Topic  
- Sentiment
```

### Relationship Types
```cypher
// Check your relationships
CALL db.relationshipTypes()

Expected output:
- PUBLISHED
- COVERS_TOPIC
- HAS_SENTIMENT
```

### Required Node Properties

#### Brand Nodes
```cypher
CREATE (b:Brand {
  id: "fast_bank",              // Required: Unique identifier
  name: "Fast Bank",            // Required: Display name
  industry: "Banking",          // Optional
  country: "Armenia"            // Optional
})
```

#### Topic Nodes
```cypher
CREATE (t:Topic {
  id: "student_loans",          // Required: Unique identifier
  name: "Student Loans",        // Required: Display name
  category: "Financial Products" // Optional
})
```

#### Ad Nodes
```cypher
CREATE (a:Ad {
  id: "ad_12345",               // Required: Unique identifier
  text: "Special student rate...", // Required: Ad content
  timestamp: datetime()         // Optional but recommended
})
```

#### Sentiment Nodes
```cypher
CREATE (s:Sentiment {
  label: "positive",            // Required: positive/negative/neutral
  score: 0.85                   // Required: -1 to 1
})
```

---

## 🔗 Expected Relationships

```cypher
// Brand publishes ads
(Brand)-[:PUBLISHED]->(Ad)

// Ads cover topics
(Ad)-[:COVERS_TOPIC]->(Topic)

// Ads have sentiment
(Ad)-[:HAS_SENTIMENT]->(Sentiment)
```

### Full Example
```cypher
// Create sample data
CREATE (fb:Brand {id: "fast_bank", name: "Fast Bank"})
CREATE (sl:Topic {id: "student_loans", name: "Student Loans"})
CREATE (ad:Ad {id: "ad_001", text: "Special 2% rate for students!"})
CREATE (sent:Sentiment {label: "positive", score: 0.9})

CREATE (fb)-[:PUBLISHED]->(ad)
CREATE (ad)-[:COVERS_TOPIC]->(sl)
CREATE (ad)-[:HAS_SENTIMENT]->(sent)
```

---

## ✅ Data Validation Queries

### 1. Check if you have data
```cypher
MATCH (n)
RETURN labels(n)[0] as nodeType, count(n) as count
ORDER BY count DESC
```

**Expected**: Should show counts for Brand, Ad, Topic, Sentiment

### 2. Verify relationships exist
```cypher
MATCH ()-[r]->()
RETURN type(r) as relationshipType, count(r) as count
ORDER BY count DESC
```

**Expected**: Should show counts for PUBLISHED, COVERS_TOPIC, HAS_SENTIMENT

### 3. Test the main query (what the dashboard uses)
```cypher
MATCH (brand:Brand)-[pub:PUBLISHED]->(ad:Ad)
OPTIONAL MATCH (ad)-[ct:COVERS_TOPIC]->(topic:Topic)
OPTIONAL MATCH (ad)-[hs:HAS_SENTIMENT]->(sentiment:Sentiment)

WITH brand, topic, sentiment, 
     count(DISTINCT ad) as adVolume,
     avg(sentiment.score) as avgSentiment

WHERE topic IS NOT NULL

RETURN 
  brand.name as brandName,
  brand.id as brandId,
  topic.name as topicName,
  topic.id as topicId,
  adVolume,
  avgSentiment,
  sentiment.label as sentimentLabel
ORDER BY adVolume DESC
LIMIT 20
```

**Expected**: Should return rows with brand-topic connections

### 4. Check for missing IDs
```cypher
// Brands without IDs
MATCH (b:Brand)
WHERE b.id IS NULL OR b.name IS NULL
RETURN b

// Topics without IDs
MATCH (t:Topic)
WHERE t.id IS NULL OR t.name IS NULL
RETURN t
```

**Expected**: Should return empty (no results)

---

## 🚨 Common Issues & Fixes

### Issue 1: "Graph shows no nodes"

**Diagnosis**: Empty database or no relationships
```cypher
// Check total nodes
MATCH (n) RETURN count(n)

// Check if brands have ads
MATCH (b:Brand)-[:PUBLISHED]->(a:Ad)
RETURN b.name, count(a) as ads
```

**Fix**: Add sample data
```cypher
// Quick test data
CREATE (fb:Brand {id: "test_brand", name: "Test Brand"})
CREATE (t1:Topic {id: "test_topic", name: "Test Topic"})
CREATE (a1:Ad {id: "test_ad_1", text: "Test ad content"})
CREATE (s1:Sentiment {label: "positive", score: 0.8})

CREATE (fb)-[:PUBLISHED]->(a1)
CREATE (a1)-[:COVERS_TOPIC]->(t1)
CREATE (a1)-[:HAS_SENTIMENT]->(s1)
```

---

### Issue 2: "Nodes exist but no connections"

**Diagnosis**: Missing relationships
```cypher
// Find orphan brands (no ads)
MATCH (b:Brand)
WHERE NOT (b)-[:PUBLISHED]->()
RETURN b.name

// Find orphan topics (no ads)
MATCH (t:Topic)
WHERE NOT ()-[:COVERS_TOPIC]->(t)
RETURN t.name
```

**Fix**: Create relationships
```cypher
// Connect existing nodes
MATCH (b:Brand {name: "Fast Bank"})
MATCH (a:Ad {id: "ad_001"})
CREATE (b)-[:PUBLISHED]->(a)
```

---

### Issue 3: "Missing IDs on nodes"

**Diagnosis**: Nodes don't have `id` property
```cypher
MATCH (b:Brand)
WHERE b.id IS NULL
RETURN b.name
```

**Fix**: Add IDs to existing nodes
```cypher
// Auto-generate IDs based on names
MATCH (b:Brand)
WHERE b.id IS NULL
SET b.id = toLower(replace(b.name, ' ', '_'))

MATCH (t:Topic)
WHERE t.id IS NULL
SET t.id = toLower(replace(t.name, ' ', '_'))
```

---

### Issue 4: "Sentiment data not showing"

**Diagnosis**: Missing HAS_SENTIMENT relationships
```cypher
// Find ads without sentiment
MATCH (a:Ad)
WHERE NOT (a)-[:HAS_SENTIMENT]->()
RETURN a.id, a.text
LIMIT 10
```

**Fix**: Add sentiment nodes and relationships
```cypher
// Create sentiment and connect to ads
MATCH (a:Ad)
WHERE NOT (a)-[:HAS_SENTIMENT]->()
CREATE (s:Sentiment {label: "neutral", score: 0.0})
CREATE (a)-[:HAS_SENTIMENT]->(s)
```

---

### Issue 5: "Duplicate nodes appearing"

**Diagnosis**: Multiple nodes with same name but different IDs
```cypher
// Find duplicate brand names
MATCH (b:Brand)
WITH b.name as name, collect(b.id) as ids, count(*) as cnt
WHERE cnt > 1
RETURN name, ids, cnt
```

**Fix**: Merge duplicates
```cypher
// Example: Merge duplicate brands
MATCH (b1:Brand {name: "Fast Bank"})
MATCH (b2:Brand {name: "Fast Bank"})
WHERE b1.id < b2.id
WITH b1, b2
MATCH (b2)-[r]->(other)
CREATE (b1)-[r2:PUBLISHED]->(other)
SET r2 = properties(r)
DETACH DELETE b2
```

---

## 🔄 Sample Data Generator

If you need to populate your database with test data:

```cypher
// Create 4 brands
CREATE (fb:Brand {id: "fast_bank", name: "Fast Bank"})
CREATE (ab:Brand {id: "ameriabank", name: "Ameriabank"})
CREATE (ib:Brand {id: "id_bank", name: "ID Bank"})
CREATE (vb:Brand {id: "vtb_armenia", name: "VTB Armenia"})

// Create 10 topics
CREATE (sl:Topic {id: "student_loans", name: "Student Loans"})
CREATE (mr:Topic {id: "mortgage_rates", name: "Mortgage Rates"})
CREATE (bc:Topic {id: "business_credit", name: "Business Credit"})
CREATE (sa:Topic {id: "savings_account", name: "Savings Account"})
CREATE (cc:Topic {id: "credit_cards", name: "Credit Cards"})
CREATE (ip:Topic {id: "investment_plans", name: "Investment Plans"})
CREATE (pl:Topic {id: "personal_loans", name: "Personal Loans"})
CREATE (mb:Topic {id: "mobile_banking", name: "Mobile Banking"})
CREATE (os:Topic {id: "online_security", name: "Online Security"})
CREATE (cs:Topic {id: "customer_service", name: "Customer Service"})

// Create sentiments
CREATE (pos:Sentiment {label: "positive", score: 0.8})
CREATE (neg:Sentiment {label: "negative", score: -0.6})
CREATE (neu:Sentiment {label: "neutral", score: 0.0})

// Create 20 ads for Fast Bank
UNWIND range(1, 20) as i
CREATE (a:Ad {
  id: "fb_ad_" + i, 
  text: "Fast Bank offers competitive rates on " + ["student loans", "mortgages", "business credit"][i % 3]
})
WITH a, i
MATCH (fb:Brand {id: "fast_bank"})
MATCH (t:Topic)
WHERE t.id IN ["student_loans", "mortgage_rates", "business_credit"]
WITH a, fb, collect(t) as topics, i
CREATE (fb)-[:PUBLISHED]->(a)
WITH a, topics[i % 3] as topic, i
CREATE (a)-[:COVERS_TOPIC]->(topic)
WITH a, i
MATCH (s:Sentiment)
WHERE s.label = ["positive", "negative", "neutral"][i % 3]
CREATE (a)-[:HAS_SENTIMENT]->(s)

// Verify data was created
MATCH (n)
RETURN labels(n)[0] as type, count(n) as count
```

---

## 🧪 Testing the Dashboard Integration

### Step 1: Verify Neo4j is accessible
```bash
# From your terminal
curl -u neo4j:your_password \
  -H "Accept: application/json" \
  -H "Content-Type: application/json" \
  -X POST \
  -d '{"statements":[{"statement":"MATCH (n) RETURN count(n)"}]}' \
  https://050a26bc.databases.neo4j.io/db/neo4j/tx/commit
```

### Step 2: Test the dashboard query locally
```cypher
// Run this in Neo4j Browser - it's the exact query the dashboard uses
MATCH (brand:Brand)-[pub:PUBLISHED]->(ad:Ad)
OPTIONAL MATCH (ad)-[ct:COVERS_TOPIC]->(topic:Topic)
OPTIONAL MATCH (ad)-[hs:HAS_SENTIMENT]->(sentiment:Sentiment)

WITH brand, topic, sentiment, 
     count(DISTINCT ad) as adVolume,
     collect(DISTINCT ad.text) as adTexts,
     avg(sentiment.score) as avgSentiment

WHERE topic IS NOT NULL

RETURN 
  brand.name as brandName,
  brand.id as brandId,
  topic.name as topicName,
  topic.id as topicId,
  adVolume,
  avgSentiment,
  sentiment.label as sentimentLabel
ORDER BY adVolume DESC
```

**Expected**: Should return rows. If not, see issues above.

### Step 3: Check node detail query
```cypher
// Test brand details query
MATCH (brand:Brand {id: "fast_bank"})
OPTIONAL MATCH (brand)-[:PUBLISHED]->(ad:Ad)-[:COVERS_TOPIC]->(topic:Topic)
OPTIONAL MATCH (ad)-[:HAS_SENTIMENT]->(sentiment:Sentiment)

WITH brand, topic, sentiment, count(DISTINCT ad) as adCount

RETURN 
  brand.name as name,
  brand.id as id,
  collect(DISTINCT {
    topic: topic.name,
    topicId: topic.id,
    adCount: adCount,
    sentiment: sentiment.label,
    sentimentScore: sentiment.score
  }) as topics
```

---

## 📋 Pre-Launch Checklist

Before opening your dashboard:

- [ ] Nodes have `id` and `name` properties
- [ ] Relationships exist: PUBLISHED, COVERS_TOPIC, HAS_SENTIMENT
- [ ] Test query returns results in Neo4j Browser
- [ ] Brand nodes connected to Ad nodes
- [ ] Ad nodes connected to Topic nodes
- [ ] Ad nodes connected to Sentiment nodes
- [ ] Environment variables set in `.env`
- [ ] Neo4j database is accessible from internet
- [ ] Supabase Edge Functions deployed

---

## 🎯 Recommended Indexes

For better performance:

```cypher
// Create indexes
CREATE INDEX brand_id IF NOT EXISTS FOR (b:Brand) ON (b.id)
CREATE INDEX brand_name IF NOT EXISTS FOR (b:Brand) ON (b.name)
CREATE INDEX topic_id IF NOT EXISTS FOR (t:Topic) ON (t.id)
CREATE INDEX topic_name IF NOT EXISTS FOR (t:Topic) ON (t.name)
CREATE INDEX ad_id IF NOT EXISTS FOR (a:Ad) ON (a.id)

// Verify indexes
SHOW INDEXES
```

---

## 🔍 Debugging Queries

### See what nodes will appear in graph
```cypher
MATCH (brand:Brand)-[:PUBLISHED]->(:Ad)-[:COVERS_TOPIC]->(topic:Topic)
RETURN DISTINCT brand.id, brand.name, topic.id, topic.name
ORDER BY brand.name, topic.name
```

### Count connections per brand
```cypher
MATCH (brand:Brand)-[:PUBLISHED]->(:Ad)-[:COVERS_TOPIC]->(topic:Topic)
RETURN brand.name, count(DISTINCT topic) as topics, count(*) as connections
ORDER BY connections DESC
```

### Find most advertised topics
```cypher
MATCH (:Brand)-[:PUBLISHED]->(ad:Ad)-[:COVERS_TOPIC]->(topic:Topic)
RETURN topic.name, count(DISTINCT ad) as adCount
ORDER BY adCount DESC
```

---

## 💡 Pro Tips

1. **Use consistent ID format**: Lowercase with underscores (e.g., `fast_bank`, not `FastBank`)
2. **Always set both `id` and `name`**: Dashboard uses `id` for linking, `name` for display
3. **Test queries in Neo4j Browser first**: Verify data before checking dashboard
4. **Check relationship direction**: Arrows matter! `(Brand)-[:PUBLISHED]->(Ad)` not `(Ad)<-[:PUBLISHED]-(Brand)`
5. **Use timestamps**: Add `timestamp` property to Ads for time-based filtering later

---

## 📞 Still Having Issues?

1. Export your schema: `CALL db.schema.visualization()`
2. Export sample data: `MATCH (n)-[r]->(m) RETURN n, r, m LIMIT 25`
3. Check Supabase Edge Function logs in Supabase dashboard
4. Look for errors in browser console (F12 → Console tab)
5. Test API endpoints directly (see QUICK_START.md)

---

**Once your Neo4j database is set up correctly, the dashboard will display your data beautifully!** 🎉
