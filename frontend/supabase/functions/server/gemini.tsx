// Gemini AI Service for generating insights
import { GoogleGenerativeAI } from "npm:@google/generative-ai";

let genAI: any = null;

function getGeminiClient() {
  if (!genAI) {
    const apiKey = Deno.env.get("GEMINI_API_KEY");
    if (!apiKey) {
      console.error("❌ GEMINI_API_KEY not found in environment variables");
      throw new Error("Missing GEMINI_API_KEY in environment variables");
    }
    console.log("✅ Initializing Gemini AI client");
    genAI = new GoogleGenerativeAI(apiKey);
  }
  return genAI;
}

// Generate natural language insight from graph data
export async function generateInsight(data: {
  type: 'node' | 'graph' | 'comparison' | 'trend';
  context: any;
}) {
  const genAI = getGeminiClient();
  const model = genAI.getGenerativeModel({ model: "gemini-1.5-flash" });

  let prompt = "";

  switch (data.type) {
    case 'node':
      prompt = `You are a marketing intelligence analyst. Analyze this data about ${data.context.name}:

${JSON.stringify(data.context, null, 2)}

Provide a concise 2-3 sentence executive summary highlighting:
1. Key patterns or insights
2. Notable trends or anomalies
3. Actionable recommendations

Be specific with numbers and concrete observations.`;
      break;

    case 'graph':
      prompt = `You are a marketing intelligence analyst. Analyze this competitive intelligence graph data:

${JSON.stringify(data.context, null, 2)}

Provide 3 key insights in bullet points:
- Most active brand and their focus areas
- Trending topics across the market
- Sentiment patterns and what they indicate

Keep each insight to one sentence.`;
      break;

    case 'comparison':
      prompt = `Compare these two entities:

${JSON.stringify(data.context, null, 2)}

Highlight:
1. Main differences
2. Competitive advantages
3. Strategic recommendations

Be concise and actionable.`;
      break;

    case 'trend':
      prompt = `Analyze this trend data:

${JSON.stringify(data.context, null, 2)}

Explain:
1. What's driving this trend
2. Which brands are leading
3. Strategic implications

Keep it executive-level and actionable.`;
      break;
  }

  try {
    const result = await model.generateContent(prompt);
    const response = await result.response;
    const text = response.text();
    return text;
  } catch (error) {
    console.error("Gemini API error:", error);
    throw new Error(`Failed to generate insight: ${error.message}`);
  }
}

// Answer natural language query about the graph
export async function answerQuery(query: string, graphContext: any) {
  try {
    const genAI = getGeminiClient();
    const model = genAI.getGenerativeModel({ model: "gemini-1.5-flash" });

    // Simplify graph context to avoid token limits
    const simplifiedContext = {
      totalNodes: graphContext?.nodes?.length || 0,
      totalLinks: graphContext?.links?.length || 0,
      brands: graphContext?.nodes?.filter((n: any) => n.type === 'brand').map((n: any) => n.name).slice(0, 10) || [],
      topics: graphContext?.nodes?.filter((n: any) => n.type === 'topic').map((n: any) => n.name).slice(0, 20) || [],
    };

    const prompt = `You are a marketing intelligence assistant with access to competitive advertising data.

Current Graph Overview:
- Total nodes: ${simplifiedContext.totalNodes}
- Total connections: ${simplifiedContext.totalLinks}
- Brands: ${simplifiedContext.brands.join(', ') || 'None currently selected'}
- Top topics: ${simplifiedContext.topics.slice(0, 10).join(', ') || 'None available'}

User Question: "${query}"

Provide a clear, concise answer based on the available data. If you need more specific data, suggest what filters the user should apply. Be conversational and helpful.`;

    console.log('🤖 Sending query to Gemini:', query);
    console.log('📊 Context:', simplifiedContext);

    const result = await model.generateContent(prompt);
    const response = await result.response;
    const text = response.text();
    
    console.log('✅ Gemini response received');
    return text;
  } catch (error) {
    console.error("❌ Gemini query error:", error);
    console.error("Error details:", {
      message: error.message,
      stack: error.stack,
      name: error.name,
    });
    throw new Error(`Failed to answer query: ${error.message}`);
  }
}

// Generate daily briefing
export async function generateDailyBriefing(recentData: any) {
  const genAI = getGeminiClient();
  const model = genAI.getGenerativeModel({ model: "gemini-1.5-flash" });

  const prompt = `You are a marketing intelligence analyst creating a daily executive briefing.

Recent Activity:
${JSON.stringify(recentData, null, 2)}

Create a briefing with:

📊 Top 3 Changes Today:
1. [Most significant change with numbers]
2. [Second notable change]
3. [Third important observation]

🚨 What Requires Attention:
[1-2 sentences on what executives should act on]

Keep it concise, specific, and actionable. Use bullet points.`;

  try {
    const result = await model.generateContent(prompt);
    const response = await result.response;
    const text = response.text();
    return text;
  } catch (error) {
    console.error("Gemini briefing error:", error);
    return "Unable to generate briefing at this time. Please check back later.";
  }
}

// Generate recommendations based on patterns
export async function generateRecommendations(nodeData: any, nodeType: string) {
  const genAI = getGeminiClient();
  const model = genAI.getGenerativeModel({ model: "gemini-1.5-flash" });

  const prompt = `Based on this ${nodeType} data:

${JSON.stringify(nodeData, null, 2)}

Generate 3 specific, actionable recommendations for marketing strategy. Format as:

🎯 Recommendation 1: [Title]
[1-2 sentences why and how]

🎯 Recommendation 2: [Title]
[1-2 sentences why and how]

🎯 Recommendation 3: [Title]
[1-2 sentences why and how]

Be specific and tie directly to the data.`;

  try {
    const result = await model.generateContent(prompt);
    const response = await result.response;
    const text = response.text();
    return text;
  } catch (error) {
    console.error("Gemini recommendations error:", error);
    return "Unable to generate recommendations at this time.";
  }
}