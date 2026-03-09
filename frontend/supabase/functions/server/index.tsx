import { Hono } from "npm:hono";
import { cors } from "npm:hono/cors";
import { logger } from "npm:hono/logger";
import * as kv from "./kv_store.tsx";
import * as neo4j from "./neo4j.tsx";
import * as neo4jHttp from "./neo4j-http.tsx";
import * as gemini from "./gemini.tsx";
import * as diagnostics from "./diagnostics.tsx";

const app = new Hono();

// Enable logger
app.use('*', logger(console.log));

// Enable CORS for all routes and methods
app.use(
  "/*",
  cors({
    origin: "*",
    allowHeaders: ["Content-Type", "Authorization"],
    allowMethods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    exposeHeaders: ["Content-Length"],
    maxAge: 600,
  }),
);

// Health check endpoint
app.get("/make-server-14007ead/health", (c) => {
  return c.json({ 
    status: "ok",
    timestamp: new Date().toISOString()
  });
});

// Debug endpoint to check if secrets are loaded (remove after testing)
app.get("/make-server-14007ead/debug-secrets", (c) => {
  const neo4jUri = Deno.env.get("NEO4J_URI") || "";
  const secrets = {
    neo4jUri: neo4jUri ? `✅ ${neo4jUri.substring(0, 50)}...` : "❌ Missing",
    neo4jUrl: Deno.env.get("NEO4J_URL") ? `✅ ${Deno.env.get("NEO4J_URL")?.substring(0, 50)}...` : "❌ Missing",
    neo4jUsername: Deno.env.get("NEO4J_USERNAME") ? `✅ ${Deno.env.get("NEO4J_USERNAME")}` : "❌ Missing",
    neo4jPassword: Deno.env.get("NEO4J_PASSWORD") ? "✅ Set (hidden)" : "❌ Missing",
    neo4jDatabase: Deno.env.get("NEO4J_DATABASE") ? `✅ ${Deno.env.get("NEO4J_DATABASE")}` : "❌ Missing (will use default 'neo4j')",
    geminiApiKey: Deno.env.get("GEMINI_API_KEY") ? "✅ Set (hidden)" : "❌ Missing",
  };
  return c.json({ secrets });
});

// Force reset Neo4j connection (useful when credentials change)
app.post("/make-server-14007ead/reset-neo4j", (c) => {
  try {
    neo4j.resetDriver();
    return c.json({ 
      success: true, 
      message: "Neo4j driver connection reset. Next query will use fresh credentials." 
    });
  } catch (error) {
    return c.json({ 
      success: false, 
      error: error.message 
    }, 500);
  }
});

// Test Neo4j connection endpoint
app.get("/make-server-14007ead/test-neo4j", async (c) => {
  try {
    console.log("Testing Neo4j connection...");
    
    // Test basic query
    const result = await neo4j.executeQuery("RETURN 1 as test");
    console.log("✅ Neo4j connection successful!");
    
    // Count nodes
    const counts = await neo4j.executeQuery(`
      MATCH (n)
      RETURN labels(n)[0] as label, count(n) as count
    `);
    
    return c.json({ 
      success: true, 
      message: "Neo4j connected successfully",
      nodeCounts: counts
    });
  } catch (error) {
    console.error("❌ Neo4j connection failed:", error);
    return c.json({ 
      success: false, 
      error: error.message,
      stack: error.stack,
      credentials: {
        uri: Deno.env.get("NEO4J_URI")?.substring(0, 30) + "...",
        username: Deno.env.get("NEO4J_USERNAME"),
        database: Deno.env.get("NEO4J_DATABASE")
      }
    }, 500);
  }
});

// Test Neo4j HTTP API connection (alternative for AuraDB Free)
app.get("/make-server-14007ead/test-neo4j-http", async (c) => {
  try {
    console.log("Testing Neo4j HTTP API connection...");
    
    // Test basic query using HTTP API
    const result = await neo4jHttp.executeQuery("RETURN 1 as test");
    console.log("✅ Neo4j HTTP API connection successful!");
    
    // Count nodes
    const counts = await neo4jHttp.executeQuery(`
      MATCH (n)
      RETURN labels(n)[0] as label, count(n) as count
    `);
    
    return c.json({ 
      success: true, 
      message: "Neo4j HTTP API connected successfully",
      nodeCounts: counts
    });
  } catch (error) {
    console.error("❌ Neo4j HTTP API connection failed:", error);
    return c.json({ 
      success: false, 
      error: error.message,
      stack: error.stack,
      credentials: {
        uri: Deno.env.get("NEO4J_URI")?.substring(0, 30) + "...",
        username: Deno.env.get("NEO4J_USERNAME"),
        database: Deno.env.get("NEO4J_DATABASE")
      }
    }, 500);
  }
});

// Inspect Neo4j schema - relationships and structure
app.get("/make-server-14007ead/inspect-schema", async (c) => {
  try {
    console.log("Inspecting Neo4j schema...");
    
    // Get all relationship types
    const relationships = await neo4jHttp.executeQuery(`
      MATCH ()-[r]->()
      RETURN DISTINCT type(r) as relationshipType, count(r) as count
      ORDER BY count DESC
    `);
    
    // Get sample nodes of each type
    const brandSample = await neo4jHttp.executeQuery(`
      MATCH (n:Brand)
      RETURN n
      LIMIT 1
    `);
    
    const topicSample = await neo4jHttp.executeQuery(`
      MATCH (n:Topic)
      RETURN n
      LIMIT 1
    `);
    
    const adSample = await neo4jHttp.executeQuery(`
      MATCH (n:Ad)
      RETURN n
      LIMIT 1
    `);
    
    // Get sample relationships
    const relationshipSamples = await neo4jHttp.executeQuery(`
      MATCH (a)-[r]->(b)
      RETURN labels(a)[0] as sourceType, type(r) as relType, labels(b)[0] as targetType
      LIMIT 10
    `);
    
    return c.json({ 
      success: true, 
      data: {
        relationships,
        samples: {
          brand: brandSample[0] || null,
          topic: topicSample[0] || null,
          ad: adSample[0] || null,
        },
        relationshipSamples,
      }
    });
  } catch (error) {
    console.error("❌ Schema inspection failed:", error);
    return c.json({ 
      success: false, 
      error: error.message,
    }, 500);
  }
});

// Get graph data endpoint
app.post("/make-server-14007ead/graph", async (c) => {
  try {
    const filters = await c.req.json();
    const graphData = await neo4j.getGraphData(filters);
    return c.json({ success: true, data: graphData });
  } catch (error) {
    console.error("Error fetching graph data:", error);
    return c.json({ 
      success: false, 
      error: `Failed to fetch graph data: ${error.message}` 
    }, 500);
  }
});

// Get graph data with filters - HTTP version
app.post("/make-server-14007ead/graph-http", async (c) => {
  try {
    const filters = await c.req.json();
    console.log("📊 Fetching graph data via HTTP with filters:", JSON.stringify(filters));
    
    const graphData = await neo4jHttp.getGraphData(filters);
    console.log(`✅ Graph data fetched: ${graphData.nodes?.length || 0} nodes, ${graphData.links?.length || 0} links`);
    
    // Log first few nodes for debugging
    if (graphData.nodes && graphData.nodes.length > 0) {
      console.log("📋 Sample nodes:", graphData.nodes.slice(0, 3));
    }
    
    return c.json({ success: true, data: graphData });
  } catch (error) {
    console.error("❌ Error fetching graph data via HTTP:", error);
    console.error("❌ Error stack:", error.stack);
    return c.json({ 
      success: false, 
      error: error.message || "Failed to fetch graph data" 
    }, 500);
  }
});

// Get node details
app.get("/make-server-14007ead/node/:nodeId/:nodeType", async (c) => {
  try {
    const nodeId = c.req.param("nodeId");
    const nodeType = c.req.param("nodeType") as 'brand' | 'topic';
    
    const nodeDetails = await neo4j.getNodeDetails(nodeId, nodeType);
    
    if (!nodeDetails) {
      return c.json({ 
        success: false, 
        error: "Node not found" 
      }, 404);
    }
    
    // Generate AI insight for the node
    const insight = await gemini.generateInsight({
      type: 'node',
      context: nodeDetails
    });
    
    // Generate recommendations
    const recommendations = await gemini.generateRecommendations(nodeDetails, nodeType);
    
    return c.json({ 
      success: true, 
      data: {
        ...nodeDetails,
        insight,
        recommendations
      }
    });
  } catch (error) {
    console.error("Error fetching node details:", error);
    return c.json({ 
      success: false, 
      error: error.message || "Failed to fetch node details" 
    }, 500);
  }
});

// Search graph
app.post("/make-server-14007ead/search", async (c) => {
  try {
    const { query } = await c.req.json();
    
    if (!query || query.trim().length === 0) {
      return c.json({ 
        success: false, 
        error: "Search query is required" 
      }, 400);
    }
    
    const results = await neo4j.searchGraph(query);
    return c.json({ success: true, data: results });
  } catch (error) {
    console.error("Error searching graph:", error);
    return c.json({ 
      success: false, 
      error: error.message || "Search failed" 
    }, 500);
  }
});

// AI Query - Natural language question answering
app.post("/make-server-14007ead/ai-query", async (c) => {
  try {
    const { query } = await c.req.json();
    
    if (!query || query.trim().length === 0) {
      return c.json({ 
        success: false, 
        error: "Query is required" 
      }, 400);
    }
    
    console.log('🔍 AI Query received:', query);
    
    // Get current graph context (with error handling)
    let graphData;
    try {
      graphData = await neo4j.getGraphData({ topN: 30 });
      console.log('📊 Graph data fetched:', {
        nodes: graphData?.nodes?.length || 0,
        links: graphData?.links?.length || 0,
      });
    } catch (graphError) {
      console.error('⚠️ Failed to fetch graph data, using empty context:', graphError);
      // Continue with empty context rather than failing completely
      graphData = { nodes: [], links: [] };
    }
    
    // Get answer from Gemini
    console.log('🤖 Calling Gemini AI...');
    const answer = await gemini.answerQuery(query, graphData);
    console.log('✅ Gemini response received');
    
    return c.json({ 
      success: true, 
      data: { 
        query, 
        answer,
        timestamp: new Date().toISOString()
      } 
    });
  } catch (error) {
    console.error("❌ Error processing AI query:", error);
    console.error("Error stack:", error.stack);
    return c.json({ 
      success: false, 
      error: error.message || "Failed to process query" 
    }, 500);
  }
});

// Get trending topics
app.get("/make-server-14007ead/trending", async (c) => {
  try {
    const limit = parseInt(c.req.query("limit") || "10");
    const trending = await neo4j.getTrendingTopics(limit);
    return c.json({ success: true, data: trending });
  } catch (error) {
    console.error("Error fetching trending topics:", error);
    return c.json({ 
      success: false, 
      error: error.message || "Failed to fetch trending topics" 
    }, 500);
  }
});

// Get top brands
app.get("/make-server-14007ead/top-brands", async (c) => {
  try {
    const limit = parseInt(c.req.query("limit") || "10");
    const topBrands = await neo4j.getTopBrands(limit);
    return c.json({ success: true, data: topBrands });
  } catch (error) {
    console.error("Error fetching top brands:", error);
    return c.json({ 
      success: false, 
      error: error.message || "Failed to fetch top brands" 
    }, 500);
  }
});

// Get all brands for filter dropdown (NEW SCHEMA)
app.get("/make-server-14007ead/brands", async (c) => {
  try {
    console.log("📋 Fetching all brands for filter dropdown...");
    const brands = await neo4jHttp.getAllBrands();
    console.log(`✅ Found ${brands.length} brands`);
    return c.json({ success: true, data: brands });
  } catch (error) {
    console.error("❌ Error fetching brands:", error);
    return c.json({ 
      success: false, 
      error: error.message || "Failed to fetch brands" 
    }, 500);
  }
});

// Get sentiment distribution
app.get("/make-server-14007ead/sentiment", async (c) => {
  try {
    const distribution = await neo4j.getSentimentDistribution();
    return c.json({ success: true, data: distribution });
  } catch (error) {
    console.error("Error fetching sentiment distribution:", error);
    return c.json({ 
      success: false, 
      error: error.message || "Failed to fetch sentiment data" 
    }, 500);
  }
});

// Generate daily briefing
app.get("/make-server-14007ead/daily-briefing", async (c) => {
  try {
    // Get recent data for briefing
    const graphData = await neo4j.getGraphData();
    const trending = await neo4j.getTrendingTopics(5);
    const sentiment = await neo4j.getSentimentDistribution();
    
    const recentData = {
      graphSummary: {
        totalNodes: graphData.nodes.length,
        totalLinks: graphData.links.length
      },
      trending,
      sentiment
    };
    
    const briefing = await gemini.generateDailyBriefing(recentData);
    
    return c.json({ 
      success: true, 
      data: { 
        briefing,
        timestamp: new Date().toISOString()
      } 
    });
  } catch (error) {
    console.error("Error generating daily briefing:", error);
    return c.json({ 
      success: false, 
      error: error.message || "Failed to generate briefing" 
    }, 500);
  }
});

// Graph insights
app.get("/make-server-14007ead/insights", async (c) => {
  try {
    const graphData = await neo4j.getGraphData();
    const trending = await neo4j.getTrendingTopics(5);
    const topBrands = await neo4j.getTopBrands(5);
    
    const insight = await gemini.generateInsight({
      type: 'graph',
      context: {
        graphData: {
          nodes: graphData.nodes.length,
          links: graphData.links.length
        },
        trending,
        topBrands
      }
    });
    
    return c.json({ 
      success: true, 
      data: { 
        insight,
        timestamp: new Date().toISOString()
      } 
    });
  } catch (error) {
    console.error("Error generating insights:", error);
    return c.json({ 
      success: false, 
      error: error.message || "Failed to generate insights" 
    }, 500);
  }
});

// ========================================
// DATA QUALITY DIAGNOSTIC ENDPOINTS
// ========================================

// Run comprehensive data quality diagnostics
app.post("/make-server-14007ead/diagnostics", async (c) => {
  try {
    const { brandName } = await c.req.json().catch(() => ({}));
    
    console.log(`🔍 Running diagnostics${brandName ? ` for ${brandName}` : ''}...`);
    
    const results = await diagnostics.runDataQualityDiagnostics(brandName);
    
    const summary = {
      total: results.length,
      passed: results.filter(r => r.status === 'pass').length,
      warnings: results.filter(r => r.status === 'warning').length,
      failed: results.filter(r => r.status === 'fail').length,
    };
    
    return c.json({ 
      success: true, 
      data: {
        summary,
        results,
        timestamp: new Date().toISOString()
      }
    });
  } catch (error) {
    console.error("❌ Diagnostics failed:", error);
    return c.json({ 
      success: false, 
      error: error.message || "Diagnostics failed" 
    }, 500);
  }
});

// Analyze specific brand-topic connection
app.post("/make-server-14007ead/diagnostics/connection", async (c) => {
  try {
    const { brandName, topicName } = await c.req.json();
    
    if (!brandName || !topicName) {
      return c.json({ 
        success: false, 
        error: "brandName and topicName are required" 
      }, 400);
    }
    
    const analysis = await diagnostics.analyzeConnection(brandName, topicName);
    
    return c.json({ 
      success: true, 
      data: analysis
    });
  } catch (error) {
    console.error("❌ Connection analysis failed:", error);
    return c.json({ 
      success: false, 
      error: error.message || "Connection analysis failed" 
    }, 500);
  }
});

// Get proprietary products report
app.get("/make-server-14007ead/diagnostics/proprietary-products", async (c) => {
  try {
    const products = await diagnostics.getProprietaryProducts();
    
    return c.json({ 
      success: true, 
      data: products
    });
  } catch (error) {
    console.error("❌ Proprietary products query failed:", error);
    return c.json({ 
      success: false, 
      error: error.message || "Query failed" 
    }, 500);
  }
});

// DIAGNOSTIC: Check what exists in Neo4j
app.get("/make-server-14007ead/diagnose-neo4j", async (c) => {
  try {
    console.log("🔍 Running Neo4j diagnostics...");
    
    // Query 1: Count all nodes
    const countQuery = "MATCH (n) RETURN count(n) AS total";
    const countResult = await neo4jHttp.executeQuery(countQuery);
    console.log("Total nodes:", countResult);
    
    // Query 2: Get all node labels
    const labelsQuery = "MATCH (n) RETURN DISTINCT labels(n) AS labels, count(*) AS count";
    const labelsResult = await neo4jHttp.executeQuery(labelsQuery);
    console.log("Node labels:", labelsResult);
    
    // Query 3: Get sample Brand nodes with all properties
    const brandQuery = "MATCH (b:Brand) RETURN b LIMIT 1";
    const brandResult = await neo4jHttp.executeQuery(brandQuery);
    console.log("Sample Brand node:", brandResult);
    
    return c.json({
      success: true,
      totalNodes: countResult,
      nodeLabels: labelsResult,
      sampleBrand: brandResult
    });
  } catch (error) {
    console.error("❌ Diagnostic error:", error);
    return c.json({ 
      success: false, 
      error: error.message 
    }, 500);
  }
});

Deno.serve(app.fetch);