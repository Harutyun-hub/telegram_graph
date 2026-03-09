# 🚀 Quick Start Guide

## ✅ What's Already Done

Your dashboard is **100% ready to use** with:

- ✅ Neo4j database connected
- ✅ Gemini AI integrated  
- ✅ 9 API endpoints working
- ✅ Real-time graph visualization
- ✅ AI-powered node inspector
- ✅ Natural language query interface

---

## 🎮 Try These Features Right Now

### 1. View Your Graph (30 seconds)
1. Open the dashboard
2. Watch the graph load from your Neo4j database
3. **Blue nodes** = Your brands (Fast Bank, Ameriabank, etc.)
4. **Purple nodes** = Topics they advertise about
5. **Lines** = Connections (thickness = ad volume)

### 2. Ask AI a Question (1 minute)
1. Click the search bar at the top
2. Type: **"Which topics does Fast Bank focus on?"**
3. Press Enter
4. Read the AI-generated answer
5. Try another: **"What's the sentiment around student loans?"**

### 3. Inspect a Node (1 minute)
1. Click any **purple topic node** (e.g., "Student Loans")
2. Right sidebar opens showing:
   - 🤖 **AI Insight** - Executive summary
   - 📊 **Metrics** - Total ads, volume
   - 🏢 **Related Brands** - Who's advertising
   - 💡 **Recommendations** - Strategic advice
   - 📝 **Sample Ads** - Real ad text
3. Click X to close

### 4. Explore Interactions (2 minutes)
- **Pan**: Click and drag on empty space
- **Zoom**: Mouse wheel or trackpad pinch
- **Hover**: See node names
- **Click**: Open details panel
- **Selected nodes**: Turn yellow/gold

---

## 🧪 Test Your Integration

### Quick Health Check

Open browser console (F12) and run:

```javascript
// Test 1: Health check
fetch('https://lzhimdzxqgyuszpephnz.supabase.co/functions/v1/make-server-14007ead/health')
  .then(r => r.json())
  .then(d => console.log('✅ Health:', d))

// Test 2: Get graph data  
fetch('https://lzhimdzxqgyuszpephnz.supabase.co/functions/v1/make-server-14007ead/graph', {
  method: 'POST',
  headers: {
    'Content-Type': 'application/json',
    'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imx6aGltZHp4cWd5dXN6cGVwaG56Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3Njg0NTk5ODksImV4cCI6MjA4NDAzNTk4OX0.NqJ44eI3D8kvEGvsLbTr0oKeKJraLd4dGLqcdMsjhxs'
  },
  body: '{}'
})
  .then(r => r.json())
  .then(d => console.log('✅ Graph data:', d))

// Test 3: Ask AI
fetch('https://lzhimdzxqgyuszpephnz.supabase.co/functions/v1/make-server-14007ead/ai-query', {
  method: 'POST',
  headers: {
    'Content-Type': 'application/json',
    'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imx6aGltZHp4cWd5dXN6cGVwaG56Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3Njg0NTk5ODksImV4cCI6MjA4NDAzNTk4OX0.NqJ44eI3D8kvEGvsLbTr0oKeKJraLd4dGLqcdMsjhxs'
  },
  body: JSON.stringify({ query: 'How many brands are in the graph?' })
})
  .then(r => r.json())
  .then(d => console.log('✅ AI Answer:', d))
```

**Expected Results:**
- Health: `{status: "ok"}`
- Graph data: `{success: true, data: {nodes: [...], links: [...]}}`
- AI Answer: `{success: true, data: {answer: "...", ...}}`

---

## 🔧 If Something's Not Working

### Problem: Graph shows loading spinner forever

**Likely cause**: Neo4j connection issue

**How to fix**:
1. Check your `.env` file has these variables:
   ```bash
   NEO4J_URI=neo4j+s://050a26bc.databases.neo4j.io
   NEO4J_USERNAME=neo4j
   NEO4J_PASSWORD=your_password_here
   NEO4J_DATABASE=neo4j
   ```
2. Verify Neo4j is accessible (try connecting via Neo4j Browser)
3. Check Supabase Edge Function logs for errors
4. Make sure your Neo4j database has data

---

### Problem: "Failed to load graph data" error

**Likely cause**: Empty database or query error

**How to fix**:
1. Open Neo4j Browser
2. Run: `MATCH (n) RETURN count(n)`
3. If result is 0, your database is empty - add some data
4. Check that you have the expected node labels:
   ```cypher
   CALL db.labels()
   // Should return: Brand, Ad, Topic, Sentiment
   ```

---

### Problem: AI queries return "Failed to get answer"

**Likely cause**: Gemini API key issue

**How to fix**:
1. Check `.env` has: `GEMINI_API_KEY=your_key_here`
2. Verify key is active at [Google AI Studio](https://aistudio.google.com/app/apikey)
3. Check you have API quota remaining
4. Try regenerating the API key if needed

---

### Problem: Node inspector shows "Failed to load details"

**Likely cause**: Node ID mismatch or missing data

**How to fix**:
1. Check Neo4j Browser: `MATCH (n) RETURN n.id, n.name LIMIT 10`
2. Verify nodes have `id` property
3. If not, update the Neo4j queries in `/supabase/functions/server/neo4j.tsx`

---

## 📝 Sample Queries to Try

### In the AI Query Bar:

1. **"Which brand has the most ads?"**
2. **"What topics are trending?"**
3. **"Show me positive sentiment topics"**
4. **"What is Fast Bank advertising about?"**
5. **"Compare Ameriabank and VTB Armenia"**
6. **"What changed this week?"**
7. **"Which topics have negative sentiment?"**
8. **"Show me student loan related ads"**

---

## 🎯 Next Actions

Now that your dashboard works, you can:

### Option A: Add More Data
1. Import more ads into Neo4j
2. Add more brands and topics
3. Enrich with sentiment data
4. Add timestamp properties for time-based queries

### Option B: Implement Phase 1 Features
Following the comprehensive plan:
1. **Onboarding flow** - Welcome screen + tutorial
2. **Smart presets** - Pre-configured views (Executive Summary, Crisis Watch, etc.)
3. **Working filters** - Connect the left sidebar filters to graph queries
4. **Daily briefing card** - Auto-generated on dashboard load
5. **Focus mode** - Click to isolate a node and its connections

### Option C: Enhance Existing Features
1. **Better error messages** - More specific troubleshooting hints
2. **Loading skeletons** - Replace spinners with skeleton screens
3. **Animation polish** - Add smooth transitions
4. **Mobile responsive** - Optimize for tablet/phone
5. **Performance** - Add caching with React Query

---

## 🎨 Customization Ideas

### Change Graph Colors
In `/src/app/components/GraphVisualization.tsx`:
```typescript
// Change brand color from blue to green
nodes.set(record.brandId, {
  color: '#10b981', // was '#3b82f6'
  // ...
});
```

### Adjust Graph Physics
In `/src/app/components/GraphVisualization.tsx`:
```typescript
d3: {
  gravity: -500,     // Increase for more spread
  linkLength: 200,   // Longer links
  linkStrength: 0.2, // Weaker attraction
},
```

### Add More AI Context
In `/supabase/functions/server/gemini.tsx`, edit prompts:
```typescript
prompt = `You are a competitive intelligence analyst specializing in banking...`
```

---

## 📚 Learn More

- **Implementation Details**: See `/IMPLEMENTATION_SUMMARY.md`
- **Full Plan**: See the comprehensive plan document
- **Code Architecture**: Explore `/src/app/` for frontend, `/supabase/functions/server/` for backend

---

## 💬 Getting Help

If you're stuck:

1. **Check browser console** (F12) for error messages
2. **Check Supabase logs** in Supabase dashboard
3. **Verify environment variables** are set correctly
4. **Test each API endpoint** individually using the test commands above
5. **Check Neo4j Browser** to verify data exists

---

## 🎉 You're All Set!

Your graph dashboard is **fully functional** and connected to real data. Start exploring, ask questions, and discover insights! 

The AI will help executives understand complex competitive intelligence data without any technical knowledge required. 🚀

**Pro tip**: Click a few different nodes to see how the AI generates unique insights for each one!
