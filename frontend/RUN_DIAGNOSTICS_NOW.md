# 🚀 RUN DIAGNOSTICS NOW - Quick Start Guide

**Time Required:** 5 minutes  
**What You'll Learn:** Whether your data has cross-contamination issues

---

## ⚡ QUICK START

### **Option 1: Use Browser Console** (Easiest)

1. Open your dashboard in browser
2. Press `F12` to open developer console
3. Paste this code and press Enter:

```javascript
// Run diagnostics for Fast Bank
const projectId = 'YOUR_PROJECT_ID'; // Replace with your project ID
const publicAnonKey = 'YOUR_ANON_KEY'; // Replace with your anon key

fetch(`https://${projectId}.supabase.co/functions/v1/make-server-14007ead/diagnostics`, {
  method: 'POST',
  headers: {
    'Authorization': `Bearer ${publicAnonKey}`,
    'Content-Type': 'application/json'
  },
  body: JSON.stringify({ brandName: 'Fast Bank' })
})
.then(r => r.json())
.then(data => {
  console.log('📊 DIAGNOSTIC RESULTS:');
  console.log('═══════════════════════════════════');
  console.log(`Total Tests: ${data.data.summary.total}`);
  console.log(`✅ Passed: ${data.data.summary.passed}`);
  console.log(`⚠️  Warnings: ${data.data.summary.warnings}`);
  console.log(`❌ Failed: ${data.data.summary.failed}`);
  console.log('═══════════════════════════════════');
  console.log('\nDETAILED RESULTS:');
  data.data.results.forEach(r => {
    const icon = r.status === 'pass' ? '✅' : r.status === 'warning' ? '⚠️' : '❌';
    console.log(`\n${icon} ${r.queryName}`);
    console.log(`   ${r.description}`);
    console.log(`   Status: ${r.status.toUpperCase()}`);
    console.log(`   ${r.recommendation}`);
    if (r.result && typeof r.result === 'object') {
      console.log('   Result:', r.result);
    }
  });
});
```

---

### **Option 2: Use Your API Service** (Recommended)

Add this to `/src/app/services/api.ts`:

```typescript
// Run data quality diagnostics
export async function runDiagnostics(brandName?: string) {
  return apiFetch('/diagnostics', {
    method: 'POST',
    body: JSON.stringify({ brandName }),
  });
}

// Analyze specific connection
export async function analyzeConnection(brandName: string, topicName: string) {
  return apiFetch('/diagnostics/connection', {
    method: 'POST',
    body: JSON.stringify({ brandName, topicName }),
  });
}

// Get proprietary products
export async function getProprietaryProducts() {
  return apiFetch('/diagnostics/proprietary-products');
}
```

Then use it in your component:
```typescript
import { runDiagnostics } from '@/app/services/api';

const results = await runDiagnostics('Fast Bank');
console.log(results);
```

---

### **Option 3: Use curl** (Terminal)

```bash
# Replace these with your values
PROJECT_ID="your-project-id"
ANON_KEY="your-anon-key"

# Run diagnostics
curl -X POST \
  "https://${PROJECT_ID}.supabase.co/functions/v1/make-server-14007ead/diagnostics" \
  -H "Authorization: Bearer ${ANON_KEY}" \
  -H "Content-Type: application/json" \
  -d '{"brandName": "Fast Bank"}' \
  | jq '.'
```

---

## 📊 INTERPRETING RESULTS

### **Result 1: Cross-Contamination Check**

```json
{
  "queryName": "Cross-Contamination Check",
  "status": "fail",
  "result": {
    "contaminatedAds": 23,
    "sampleAdIds": ["ad123", "ad456"],
    "topicName": "Evocatouch"
  },
  "recommendation": "Found 23 ads connecting Fast Bank to Evocabank products"
}
```

**What it means:**
- ❌ **FAIL** = Fast Bank IS connected to Evocatouch in your database
- **23 ads** = 23 Fast Bank ads incorrectly linked to Evocatouch
- **Root cause:** Topic extraction doesn't distinguish competitor mentions
- **Action:** Implement filter solution (see below)

---

### **Result 2: Proprietary Product Check**

```json
{
  "queryName": "Proprietary Product Check",
  "status": "fail",
  "result": [
    {
      "topicName": "Evocatouch",
      "connectedBrands": ["Evocabank", "Fast Bank", "ID Bank"],
      "totalAds": 145
    }
  ]
}
```

**What it means:**
- ❌ **FAIL** = Evocatouch is connected to 3 brands (should be 1)
- **Cross-contamination confirmed**
- **Action:** See "Quick Fix" section below

---

### **Result 3: Sample Ads**

```json
{
  "queryName": "Sample Ads",
  "status": "pass",
  "result": [
    {
      "adId": "ad123",
      "topics": ["Mobile Banking", "Cashback", "Evocatouch", "FastApp"]
    }
  ]
}
```

**What it means:**
- Ad includes both "Evocatouch" (competitor) and "FastApp" (own product)
- Confirms: AI extracted all entities without context
- **Action:** Update topic extraction logic

---

## 🔧 QUICK FIX (If Results Show Contamination)

### **Step 1: Create Proprietary Product List**

Create `/supabase/functions/server/proprietary-products.tsx`:

```typescript
// Brand-specific proprietary products
export const PROPRIETARY_PRODUCTS: Record<string, string[]> = {
  "Evocabank": [
    "Evocatouch",
    "Evoca",
    "EvocaApp",
  ],
  "Fast Bank": [
    "FastApp",
    "Fast24",
    "FastPay",
  ],
  "ID Bank": [
    "IDRAM",
    "IDPay",
    "ID Mobile",
  ],
  "VTB Armenia": [
    "VTB Online",
    "VTB Mobile",
  ],
};

// Get products owned by OTHER brands (to exclude)
export function getCompetitorProducts(brandName: string): string[] {
  const competitors: string[] = [];
  
  for (const [brand, products] of Object.entries(PROPRIETARY_PRODUCTS)) {
    if (brand !== brandName) {
      competitors.push(...products);
    }
  }
  
  return competitors;
}
```

---

### **Step 2: Update Neo4j Query**

Update `/supabase/functions/server/neo4j-http.tsx`:

```typescript
import { getCompetitorProducts } from './proprietary-products.tsx';

export async function getGraphData(filters = {}) {
  const brandNames = filters.brandSource || [];
  
  // Get competitor products to exclude
  const excludeProducts = brandNames.length > 0
    ? getCompetitorProducts(brandNames[0]) // For single brand
    : [];

  const cypher = `
    MATCH (b:Brand)-[:PUBLISHED]->(a:Ad)-[:COVERS_TOPIC]->(t:Topic)
    WHERE b.name IN $brandNames
      AND NOT t.name IN $excludeProducts  // ← NEW: Filter out competitors
    
    WITH b, t, count(a) AS weight
    WHERE weight >= 1
    
    WITH t, sum(weight) AS topicTotalAds, 
         collect({source: elementId(b), target: elementId(t), value: weight}) AS brandLinks
    ORDER BY topicTotalAds DESC
    LIMIT $topicLimit
    
    // ... rest of query
  `;

  return executeQuery(cypher, {
    brandNames,
    excludeProducts,  // ← NEW parameter
    topicLimit: filters.topN || 15,
  });
}
```

---

### **Step 3: Test the Fix**

```javascript
// In browser console:
const graphData = await getGraphData({
  brandSource: ['Fast Bank'],
  topN: 15
});

// Check if Evocatouch is still present
const hasEvoca = graphData.nodes.some(n => 
  n.name.toLowerCase().includes('evoca')
);

console.log('Evocatouch present:', hasEvoca); // Should be FALSE
```

---

## 🎯 EXPECTED OUTCOMES

### **Before Fix:**
```
SELECT Fast Bank
→ Shows: 15 topics including "Evocatouch", "Evoca", etc.
→ Status: ❌ Incorrect
```

### **After Fix:**
```
SELECT Fast Bank
→ Shows: 15 topics, ONLY Fast Bank's own products
→ "Evocatouch" filtered out ✓
→ Status: ✅ Correct
```

---

## 📈 VERIFICATION CHECKLIST

After implementing the fix, verify:

- [ ] Run diagnostics again
- [ ] Check: contaminatedAds = 0
- [ ] Check: Each proprietary product connected to 1 brand only
- [ ] Test: Select Fast Bank → No Evocatouch visible
- [ ] Test: Select Evocabank → Evocatouch visible
- [ ] Test: Select ID Bank → IDRAM visible, but not Evocatouch

---

## 🚨 IF DIAGNOSTICS SHOW ZERO ISSUES

If all tests PASS, then:

1. **Data is correct** - No cross-contamination
2. **Issue is in visualization** - Already fixed in recent updates
3. **Clear browser cache** and test again
4. **Check brand filter** is being applied correctly

Run this to verify:
```javascript
// Check current query
console.log('Current filters:', filters);

// Check backend response
const response = await getGraphData(filters);
console.log('Nodes returned:', response.nodes.length);
console.log('Topic nodes:', response.nodes.filter(n => n.type === 'topic'));
```

---

## 💡 LONG-TERM SOLUTION

Once quick fix is working, plan for proper solution:

### **Week 1-2: Design**
- Design new relationship types
- PROMOTES (own products)
- MENTIONS_COMPETITOR (competitor products)
- COVERS_TOPIC (industry terms)

### **Week 3-4: Implementation**
- Update data ingestion pipeline
- Train AI to classify entities by context
- Re-ingest historical data

### **Week 5: Validation**
- Run diagnostics (should be 100% pass)
- Remove quick filter workaround
- User acceptance testing

---

## 📞 SUPPORT

If you need help interpreting results:

1. **Share diagnostic output** in chat
2. **Include specific brand-topic pair** that's incorrect
3. **Provide sample ad content** (if available)
4. **Mention your Neo4j database size** (number of nodes/relationships)

---

## 🎉 SUCCESS CRITERIA

You'll know it's fixed when:

✅ Cross-Contamination Check = PASS (0 ads)  
✅ Proprietary Products = 1 brand per product  
✅ Fast Bank + Evocatouch = No connection  
✅ Executives trust the data  
✅ No false insights  

---

**Time to run diagnostics: 5 minutes**  
**Time to implement fix: 2 hours**  
**Impact: High - Restores data integrity**

---

**RUN DIAGNOSTICS NOW** → See Option 1 above ☝️
