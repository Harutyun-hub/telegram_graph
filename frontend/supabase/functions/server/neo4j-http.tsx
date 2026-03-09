// Neo4j Query API (HTTP) Service - Alternative to Bolt driver
// Use this for AuraDB Free tier if Bolt protocol is blocked

// Helper function to convert Neo4j integers to regular numbers
function toBigIntSafe(value: any): number {
  if (value === null || value === undefined) return 0;
  if (typeof value === 'bigint') return Number(value);
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
      if (typeof value === 'bigint') {
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

// Execute a Cypher query using Neo4j Query API (HTTP)
export async function executeQuery(cypher: string, params: Record<string, any> = {}) {
  const uri = Deno.env.get("NEO4J_URI");
  const username = Deno.env.get("NEO4J_USERNAME");
  const password = Deno.env.get("NEO4J_PASSWORD");
  const database = Deno.env.get("NEO4J_DATABASE") || "neo4j";

  if (!uri || !username || !password) {
    throw new Error("Missing Neo4j credentials in environment variables");
  }

  // Convert bolt URI to HTTP Query API URL
  // neo4j+s://050a26bc.databases.neo4j.io -> https://050a26bc.databases.neo4j.io
  const httpUri = uri.replace("neo4j+s://", "https://").replace("neo4j://", "http://");
  const queryUrl = `${httpUri}/db/${database}/query/v2`;

  console.log("Executing Neo4j query via HTTP API:", queryUrl);

  try {
    const response = await fetch(queryUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Basic ${btoa(`${username}:${password}`)}`,
        "Accept": "application/json",
      },
      body: JSON.stringify({
        statement: cypher,
        parameters: params,
      }),
    });

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`Neo4j Query API error (${response.status}): ${errorText}`);
    }

    const result = await response.json();

    // Parse Neo4j Query API response format
    // Response format: { data: { fields: [...], values: [[...], [...]] } }
    if (!result.data || !result.data.fields || !result.data.values) {
      return [];
    }

    const fields = result.data.fields;
    const values = result.data.values;

    // Convert to array of objects
    const records = values.map((row: any[]) => {
      const record: any = {};
      fields.forEach((field: string, index: number) => {
        record[field] = row[index];
      });
      return record;
    });

    // Convert all BigInt values to regular numbers
    return records.map((record: any) => convertBigInts(record));
  } catch (error) {
    console.error("Neo4j HTTP query error:", error);
    throw new Error(`Neo4j query failed: ${error.message}`);
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
} = {}) {
  // STEP 2: Get Simple Graph Data (Brands Only)
  // Remove leading/trailing whitespace that causes syntax errors
  const cypher = `MATCH (b:Brand)
RETURN COLLECT({
  id: b.id,
  name: b.name,
  type: 'brand',
  val: 3.0
}) AS nodes`;

  console.log("🔍 STEP 2: Executing brands-only query...");
  console.log("📝 Query:", cypher);
  
  const records = await executeQuery(cypher);
  
  if (!records || records.length === 0) {
    console.warn('⚠️ No records returned');
    return { nodes: [], links: [] };
  }

  const nodes = records[0].nodes;
  
  console.log(`✅ STEP 2: Found ${nodes.length} brand nodes`);
  console.log('✅ Sample nodes:', nodes.slice(0, 2));
  
  return { 
    nodes: nodes,
    links: [] 
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
      
      WITH topic, brand, ad, sentiment
      
      RETURN 
        topic.name as name,
        topic.id as id,
        topic.category as category,
        count(DISTINCT ad) as totalAds,
        collect(DISTINCT {
          brand: brand.name,
          brandId: brand.id,
          adCount: count(ad),
          sentiment: sentiment.label,
          sentimentScore: sentiment.score,
          adText: ad.text
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
  
  // Ensure limit is an integer, not a float
  const results = await executeQuery(cypher, { limit: Math.floor(limit) });
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
  
  // Ensure limit is an integer, not a float
  const results = await executeQuery(cypher, { limit: Math.floor(limit) });
  return results;
}

// Get all brands for filter dropdown (NEW SCHEMA)
export async function getAllBrands() {
  const cypher = `
    MATCH (b:Brand)
    RETURN b.id AS id, b.name AS name
    ORDER BY b.name
  `;
  
  const results = await executeQuery(cypher);
  return results;
}