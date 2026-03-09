// Neo4j Data Quality Diagnostic Tool
import { executeQuery } from "./neo4j.tsx";

export interface DiagnosticResult {
  queryName: string;
  description: string;
  status: 'pass' | 'warning' | 'fail';
  result: any;
  recommendation: string;
}

// Run comprehensive data quality diagnostics
export async function runDataQualityDiagnostics(brandName?: string): Promise<DiagnosticResult[]> {
  const results: DiagnosticResult[] = [];

  console.log('🔍 Starting data quality diagnostics...');

  // Query 1: Check for cross-contamination (Fast Bank connected to Evocatouch)
  try {
    const query1 = `
      MATCH (b:Brand)-[:PUBLISHED]->(a:Ad)-[:COVERS_TOPIC]->(t:Topic)
      WHERE b.name = $brandName AND (t.name CONTAINS "Evoca" OR t.name CONTAINS "evoca")
      RETURN count(a) AS contaminatedAds, 
             collect(a.id)[0..3] AS sampleAdIds,
             t.name AS topicName
    `;
    
    const result1 = await executeQuery(query1, { brandName: brandName || "Fast Bank" });
    const contaminatedAds = result1[0]?.contaminatedAds || 0;
    
    results.push({
      queryName: 'Cross-Contamination Check',
      description: `Checking if ${brandName || "Fast Bank"} is incorrectly connected to Evocabank's products`,
      status: contaminatedAds > 0 ? 'fail' : 'pass',
      result: {
        contaminatedAds,
        sampleAdIds: result1[0]?.sampleAdIds || [],
        topicName: result1[0]?.topicName || null,
      },
      recommendation: contaminatedAds > 0 
        ? `Found ${contaminatedAds} ads connecting ${brandName} to Evocabank products. This indicates topic extraction is including competitor mentions.`
        : 'No cross-contamination detected for this brand.',
    });
  } catch (err) {
    console.error('Query 1 failed:', err);
    results.push({
      queryName: 'Cross-Contamination Check',
      description: 'Failed to execute query',
      status: 'fail',
      result: { error: String(err) },
      recommendation: 'Check Neo4j connection and query syntax.',
    });
  }

  // Query 2: Find all brands connected to proprietary products
  try {
    const query2 = `
      MATCH (b:Brand)-[:PUBLISHED]->(a:Ad)-[:COVERS_TOPIC]->(t:Topic)
      WHERE t.name IN ["Evocatouch", "Evoca", "FastApp", "Fast24", "IDRAM", "IDPay", "VTB Online"]
      RETURN t.name AS topicName, 
             collect(DISTINCT b.name) AS connectedBrands,
             count(a) AS totalAds
      ORDER BY totalAds DESC
    `;
    
    const result2 = await executeQuery(query2);
    const proprietaryTopics = result2 || [];
    
    const hasIssues = proprietaryTopics.some((topic: any) => 
      topic.connectedBrands && topic.connectedBrands.length > 1
    );
    
    results.push({
      queryName: 'Proprietary Product Check',
      description: 'Checking if brand-specific products are connected to multiple brands',
      status: hasIssues ? 'fail' : 'pass',
      result: proprietaryTopics,
      recommendation: hasIssues
        ? 'Multiple brands are connected to the same proprietary product. This confirms cross-contamination.'
        : 'Each proprietary product is correctly connected to only one brand.',
    });
  } catch (err) {
    console.error('Query 2 failed:', err);
    results.push({
      queryName: 'Proprietary Product Check',
      description: 'Failed to execute query',
      status: 'fail',
      result: { error: String(err) },
      recommendation: 'Check Neo4j connection.',
    });
  }

  // Query 3: Check for duplicate topic nodes
  try {
    const query3 = `
      MATCH (t:Topic)
      WITH t.name AS topicName, collect(t) AS topics
      WHERE size(topics) > 1
      RETURN topicName, size(topics) AS duplicateCount
      LIMIT 20
    `;
    
    const result3 = await executeQuery(query3);
    const duplicates = result3 || [];
    
    results.push({
      queryName: 'Duplicate Topic Check',
      description: 'Checking for duplicate topic nodes with same name',
      status: duplicates.length > 0 ? 'warning' : 'pass',
      result: duplicates,
      recommendation: duplicates.length > 0
        ? `Found ${duplicates.length} topics with duplicate nodes. Consider merging using APOC or adding unique constraints.`
        : 'No duplicate topic nodes found.',
    });
  } catch (err) {
    console.error('Query 3 failed:', err);
    results.push({
      queryName: 'Duplicate Topic Check',
      description: 'Failed to execute query',
      status: 'fail',
      result: { error: String(err) },
      recommendation: 'Check Neo4j connection.',
    });
  }

  // Query 4: Sample ads for manual review
  try {
    const query4 = `
      MATCH (b:Brand)-[:PUBLISHED]->(a:Ad)-[:COVERS_TOPIC]->(t:Topic)
      WHERE b.name = $brandName
      WITH a, collect(t.name) AS topics
      RETURN a.id AS adId, topics
      LIMIT 10
    `;
    
    const result4 = await executeQuery(query4, { brandName: brandName || "Fast Bank" });
    
    results.push({
      queryName: 'Sample Ads',
      description: `Sample ads from ${brandName || "Fast Bank"} with their topics`,
      status: 'pass',
      result: result4 || [],
      recommendation: 'Manual review: Check if topics include competitor product names.',
    });
  } catch (err) {
    console.error('Query 4 failed:', err);
    results.push({
      queryName: 'Sample Ads',
      description: 'Failed to execute query',
      status: 'fail',
      result: { error: String(err) },
      recommendation: 'Check Neo4j connection.',
    });
  }

  // Query 5: Ads without topics
  try {
    const query5 = `
      MATCH (b:Brand)-[:PUBLISHED]->(a:Ad)
      WHERE NOT (a)-[:COVERS_TOPIC]->(:Topic)
      RETURN b.name AS brand, count(a) AS adsWithoutTopics
      ORDER BY adsWithoutTopics DESC
    `;
    
    const result5 = await executeQuery(query5);
    const adsWithoutTopics = result5 || [];
    const totalWithoutTopics = adsWithoutTopics.reduce((sum: number, item: any) => sum + (item.adsWithoutTopics || 0), 0);
    
    results.push({
      queryName: 'Ads Without Topics',
      description: 'Checking for ads that have no topic relationships',
      status: totalWithoutTopics > 0 ? 'warning' : 'pass',
      result: adsWithoutTopics,
      recommendation: totalWithoutTopics > 0
        ? `Found ${totalWithoutTopics} ads without topics. These ads won't appear in the graph.`
        : 'All ads have at least one topic.',
    });
  } catch (err) {
    console.error('Query 5 failed:', err);
    results.push({
      queryName: 'Ads Without Topics',
      description: 'Failed to execute query',
      status: 'fail',
      result: { error: String(err) },
      recommendation: 'Check Neo4j connection.',
    });
  }

  // Query 6: Overall data statistics
  try {
    const query6 = `
      MATCH (b:Brand)
      WITH count(b) AS brandCount
      MATCH (a:Ad)
      WITH brandCount, count(a) AS adCount
      MATCH (t:Topic)
      WITH brandCount, adCount, count(t) AS topicCount
      MATCH ()-[r:PUBLISHED]->()
      WITH brandCount, adCount, topicCount, count(r) AS publishedCount
      MATCH ()-[r2:COVERS_TOPIC]->()
      RETURN brandCount, adCount, topicCount, publishedCount, count(r2) AS coversTopicCount
    `;
    
    const result6 = await executeQuery(query6);
    
    results.push({
      queryName: 'Database Statistics',
      description: 'Overall graph database statistics',
      status: 'pass',
      result: result6[0] || {},
      recommendation: 'Review statistics to understand database size and relationships.',
    });
  } catch (err) {
    console.error('Query 6 failed:', err);
    results.push({
      queryName: 'Database Statistics',
      description: 'Failed to execute query',
      status: 'fail',
      result: { error: String(err) },
      recommendation: 'Check Neo4j connection.',
    });
  }

  console.log(`✅ Completed ${results.length} diagnostic checks`);
  return results;
}

// Get detailed analysis for a specific brand-topic connection
export async function analyzeConnection(brandName: string, topicName: string) {
  console.log(`🔍 Analyzing connection: ${brandName} → ${topicName}`);

  const query = `
    MATCH (b:Brand {name: $brandName})-[:PUBLISHED]->(a:Ad)-[:COVERS_TOPIC]->(t:Topic {name: $topicName})
    RETURN a.id AS adId,
           a.publishedDate AS publishedDate,
           a.platform AS platform,
           [(a)-[:HAS_SENTIMENT]->(s:Sentiment) | s.label][0] AS sentiment
    LIMIT 20
  `;

  const ads = await executeQuery(query, { brandName, topicName });

  return {
    brandName,
    topicName,
    connectionCount: ads.length,
    ads: ads || [],
    verdict: ads.length > 0 
      ? 'Connection exists in database'
      : 'No direct connection found (may be inferred or frontend issue)',
  };
}

// Get list of all proprietary products per brand
export async function getProprietaryProducts() {
  const query = `
    MATCH (t:Topic)
    WHERE t.name =~ ".*[Aa]pp$" OR 
          t.name =~ ".*[Tt]ouch$" OR
          t.name =~ ".*Pay$" OR
          t.name =~ ".*RAM$" OR
          t.name IN ["Fast24", "VTB Online", "Evoca"]
    WITH t
    MATCH (b:Brand)-[:PUBLISHED]->(:Ad)-[:COVERS_TOPIC]->(t)
    WITH t.name AS product, collect(DISTINCT b.name) AS brands, count(b) AS brandCount
    RETURN product, brands, brandCount
    ORDER BY brandCount DESC
  `;

  const result = await executeQuery(query);
  return result || [];
}
