// Neo4j Database Service
import neo4j from "npm:neo4j-driver";

// Helper function to convert Neo4j integers (BigInt) to regular numbers
function toBigIntSafe(value: any): number {
  if (value === null || value === undefined) return 0;
  if (typeof value === 'bigint') return Number(value);
  if (neo4j.isInt(value)) return value.toNumber();
  if (typeof value === 'number') return value;
  return 0;
}

// Helper function to recursively convert all BigInt values in an object
function convertBigInts(obj: any): any {
  if (obj === null || obj === undefined) return obj;
  
  if (Array.isArray(obj)) {
    return obj.map(item => convertBigInts(item));
  }
  
  if (typeof obj === 'object') {
    const converted: any = {};
    for (const [key, value] of Object.entries(obj)) {
      if (typeof value === 'bigint' || neo4j.isInt(value)) {
        converted[key] = toBigIntSafe(value);
      } else if (typeof value === 'object') {
        converted[key] = convertBigInts(value);
      } else {
        converted[key] = value;
      }
    }
    return converted;
  }
  
  return obj;
}

// Create Neo4j driver singleton
let driver: any = null;

function getDriver() {
  if (!driver) {
    const uri = Deno.env.get("NEO4J_URI");
    const username = Deno.env.get("NEO4J_USERNAME");
    const password = Deno.env.get("NEO4J_PASSWORD");

    if (!uri || !username || !password) {
      throw new Error("Missing Neo4j credentials in environment variables");
    }

    console.log(`🔌 Creating Neo4j driver connection to: ${uri.substring(0, 40)}...`);
    
    driver = neo4j.driver(uri, neo4j.auth.basic(username, password), {
      maxConnectionPoolSize: 50,
      connectionAcquisitionTimeout: 30000,
    });
  }
  return driver;
}

// Force driver reset (useful when credentials change)
export function resetDriver() {
  if (driver) {
    console.log("♻️ Resetting Neo4j driver connection...");
    driver.close();
    driver = null;
  }
}

// Execute a Cypher query
export async function executeQuery(cypher: string, params: Record<string, any> = {}) {
  const driver = getDriver();
  const session = driver.session({
    database: Deno.env.get("NEO4J_DATABASE") || "neo4j",
  });

  try {
    const result = await session.run(cypher, params);
    const records = result.records.map((record: any) => record.toObject());
    // Convert all BigInt values to regular numbers
    return records.map((record: any) => convertBigInts(record));
  } catch (error) {
    console.error("Neo4j query error:", error);
    throw new Error(`Neo4j query failed: ${error.message}`);
  } finally {
    await session.close();
  }
}

// Get full graph data (brands, topics, ads with relationships)
// Uses the proven pattern: Brand -> Ad -> Topic collapsed into Brand <-> Topic with weights
export async function getGraphData(filters: {
  timeframe?: string;
  brandSource?: string[];
  connectionStrength?: number;
  sentiment?: string[];
  topics?: string[];
  topN?: number; // Number of top topics to return
} = {}) {
  console.log('🔍 Neo4j getGraphData called with filters:', JSON.stringify(filters));

  // Default to top 15 topics for usability (can be overridden)
  const topicLimit = filters.topN || 15;
  
  // Build brand filter if brands are specified
  const brandFilter = filters.brandSource && filters.brandSource.length > 0
    ? 'WHERE b.name IN $brandNames'
    : '';

  // Optimized query: Filter by brands FIRST, then get top topics for THOSE brands
  const cypher = `
    // 1. MATCH: Traverse Brand -> Ad -> Topic paths (with optional brand filter)
    MATCH (b:Brand)-[:PUBLISHED]->(a:Ad)-[:COVERS_TOPIC]->(t:Topic)
    ${brandFilter}
    
    // 2. AGGREGATE: Collapse Ads into edge weights (count of ads per brand-topic pair)
    WITH b, t, count(a) AS weight
    WHERE weight >= 1
    
    // 3. RANKING: For selected brands, get top N topics by total ad volume
    WITH t, 
         sum(weight) AS topicTotalAds, 
         collect({source: elementId(b), target: elementId(t), value: weight, brandName: b.name}) AS brandLinks
    ORDER BY topicTotalAds DESC
    LIMIT $topicLimit
    
    // 4. FORMATTING: Build flat lists
    WITH collect(t) AS topics, 
         collect(brandLinks) AS linkGroups, 
         collect(topicTotalAds) AS volumes
    WITH topics, volumes, 
         [link IN reduce(acc=[], g IN linkGroups | acc + g) | link] AS allLinks
    
    // 5. CLEANUP: Get only brands involved in these top topics
    UNWIND allLinks AS l
    MATCH (b:Brand) WHERE elementId(b) = l.source
    WITH topics, volumes, allLinks, collect(DISTINCT b) AS brands
    
    // 6. RETURN: Pre-formatted JSON for React
    RETURN {
      nodes: 
        [brand IN brands | {
          id: elementId(brand), 
          group: 'Brand', 
          label: brand.name, 
          radius: 30,
          color: '#3b82f6'
        }] + 
        [i IN range(0, size(topics)-1) | {
          id: elementId(topics[i]), 
          group: 'Topic', 
          label: topics[i].name, 
          radius: 5 + toInteger(log(volumes[i]) * 5),
          color: '#a855f7',
          adCount: volumes[i]
        }],
      links: allLinks
    } AS graphData
  `;

  const params: Record<string, any> = {
    topicLimit: topicLimit
  };

  if (filters.brandSource && filters.brandSource.length > 0) {
    params.brandNames = filters.brandSource;
  }

  console.log('📊 Executing Neo4j query with params:', JSON.stringify(params));

  const records = await executeQuery(cypher, params);
  
  // Extract the pre-formatted graph data
  if (!records || records.length === 0) {
    console.warn('No graph data returned from query');
    return { nodes: [], links: [] };
  }

  // executeQuery already converts to plain objects via toObject()
  const graphData = records[0].graphData;
  
  if (!graphData || !graphData.nodes || !graphData.links) {
    console.warn('Invalid graph data structure:', graphData);
    return { nodes: [], links: [] };
  }

  // Process Neo4j integers - critical step!
  const processedNodes = graphData.nodes.map((node: any) => ({
    id: node.id,
    name: node.label, // Map 'label' to 'name' for react-d3-graph
    type: node.group.toLowerCase(), // 'Brand' -> 'brand', 'Topic' -> 'topic'
    color: node.color,
    size: typeof node.radius === 'object' ? toBigIntSafe(node.radius) : node.radius,
  }));

  const processedLinks = graphData.links.map((link: any) => ({
    source: link.source,
    target: link.target,
    value: typeof link.value === 'object' ? toBigIntSafe(link.value) : link.value,
  }));

  console.log(`✅ Processed graph: ${processedNodes.length} nodes, ${processedLinks.length} links`);

  return {
    nodes: processedNodes,
    links: processedLinks,
  };
}

// Get detailed node information
export async function getNodeDetails(nodeId: string, nodeType: 'brand' | 'topic') {
  if (nodeType === 'brand') {
    const cypher = `
      MATCH (brand:Brand {id: $nodeId})
      OPTIONAL MATCH (brand)-[:PUBLISHED]->(ad:Ad)-[:COVERS_TOPIC]->(topic:Topic)
      OPTIONAL MATCH (ad)-[:HAS_SENTIMENT]->(sentiment:Sentiment)
      
      WITH brand, topic, sentiment, count(DISTINCT ad) as adCount
      
      RETURN 
        brand.name as name,
        brand.id as id,
        brand.industry as industry,
        brand.country as country,
        collect(DISTINCT {
          topic: topic.name,
          topicId: topic.id,
          adCount: adCount,
          sentiment: sentiment.label,
          sentimentScore: sentiment.score
        }) as topics
    `;
    
    const result = await executeQuery(cypher, { nodeId });
    return result[0] || null;
  } else {
    const cypher = `
      MATCH (topic:Topic {id: $nodeId})
      OPTIONAL MATCH (brand:Brand)-[:PUBLISHED]->(ad:Ad)-[:COVERS_TOPIC]->(topic)
      OPTIONAL MATCH (ad)-[:HAS_SENTIMENT]->(sentiment:Sentiment)
      
      WITH topic, brand, sentiment, ad
      WITH topic, brand, sentiment, count(DISTINCT ad) as brandAdCount, collect(ad.text)[0] as sampleAdText
      
      RETURN 
        topic.name as name,
        topic.id as id,
        topic.category as category,
        sum(brandAdCount) as totalAds,
        collect(DISTINCT {
          brand: brand.name,
          brandId: brand.id,
          adCount: brandAdCount,
          sentiment: sentiment.label,
          sentimentScore: sentiment.score,
          adText: sampleAdText
        }) as brands
    `;
    
    const result = await executeQuery(cypher, { nodeId });
    return result[0] || null;
  }
}

// Search graph by natural language
export async function searchGraph(query: string) {
  const cypher = `
    MATCH (n)
    WHERE n.name =~ $pattern OR n.text =~ $pattern
    RETURN 
      labels(n)[0] as type,
      n.id as id,
      n.name as name,
      n.text as text
    LIMIT 20
  `;
  
  const pattern = `(?i).*${query}.*`; // Case-insensitive regex
  const results = await executeQuery(cypher, { pattern });
  return results;
}

// Get trending topics (most ads in last period)
export async function getTrendingTopics(limit: number = 10) {
  const cypher = `
    MATCH (ad:Ad)-[:COVERS_TOPIC]->(topic:Topic)
    WITH topic, count(ad) as adCount
    ORDER BY adCount DESC
    LIMIT $limit
    RETURN 
      topic.name as name,
      topic.id as id,
      adCount
  `;
  
  const results = await executeQuery(cypher, { limit: neo4j.int(limit) });
  return results;
}

// Get sentiment distribution
export async function getSentimentDistribution() {
  const cypher = `
    MATCH (ad:Ad)-[:HAS_SENTIMENT]->(sentiment:Sentiment)
    WITH sentiment.label as label, count(ad) as count
    RETURN label, count
    ORDER BY count DESC
  `;
  
  const results = await executeQuery(cypher);
  return results;
}

// Get brands with most ad volume
export async function getTopBrands(limit: number = 10) {
  const cypher = `
    MATCH (brand:Brand)-[:PUBLISHED]->(ad:Ad)
    WITH brand, count(ad) as adCount
    ORDER BY adCount DESC
    LIMIT $limit
    RETURN 
      brand.name as name,
      brand.id as id,
      adCount
  `;
  
  const results = await executeQuery(cypher, { limit: neo4j.int(limit) });
  return results;
}

// Close driver on shutdown
export async function closeDriver() {
  if (driver) {
    await driver.close();
    driver = null;
  }
}