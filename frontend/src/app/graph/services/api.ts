import type {
  GraphNodeType,
  GraphData,
  GraphNode,
  GraphLink,
  NodeDetails,
  DataFreshnessSnapshot,
} from '@/app/graph/services/types';
import type {
  AIClientFilters,
  AIEvidenceItem,
  AIGraphInstruction,
  AIQueryResult,
  AIDataScope,
  SearchResult,
  TrendingTopic,
  TopChannel,
  SentimentData,
} from '@/app/graph/contracts';

export type {
  GraphNodeType,
  GraphData,
  GraphNode,
  GraphLink,
  NodeDetails,
  DataFreshnessSnapshot,
  AIClientFilters,
  AIEvidenceItem,
  AIGraphInstruction,
  AIQueryResult,
  AIDataScope,
  SearchResult,
  TrendingTopic,
  TopChannel,
  SentimentData,
};

function normalizeNodeType(type: unknown): string {
  const normalized = String(type || 'topic').toLowerCase();
  return normalized === 'brand' ? 'channel' : normalized;
}

function normalizeGraphData(data: GraphData): GraphData {
  return {
    nodes: (data.nodes || []).map((node) => ({
      ...node,
      type: normalizeNodeType(node.type),
    })),
    links: data.links || [],
    meta: data.meta,
  };
}

function normalizeNodeDetails(details: NodeDetails): NodeDetails {
  const channels = Array.isArray(details.channels)
    ? details.channels
    : Array.isArray(details.brands)
      ? details.brands
      : [];

  return {
    ...details,
    type: normalizeNodeType(details.type),
    channels,
    relatedChannels: Array.isArray(details.relatedChannels)
      ? details.relatedChannels
      : Array.isArray(details.relatedBrands)
        ? details.relatedBrands
        : channels,
    channelCount: Number(details.channelCount ?? details.brandCount ?? channels.length ?? 0),
  };
}

function getApiBaseUrl(): string {
  const configured = (import.meta.env.VITE_API_BASE_URL || '').trim();
  if (!configured) return '';
  return configured.endsWith('/') ? configured.slice(0, -1) : configured;
}

function apiUrl(path: string): string {
  const base = getApiBaseUrl();
  const normalized = path.startsWith('/') ? path : `/${path}`;
  if (!base) return normalized;

  const baseEndsWithApi = /\/api$/i.test(base);
  const pathStartsWithApi = /^\/api(?:\/|$)/i.test(normalized);
  const normalizedPath = baseEndsWithApi && pathStartsWithApi
    ? normalized.replace(/^\/api/i, '') || '/'
    : normalized;

  return `${base}${normalizedPath}`;
}

async function readError(response: Response): Promise<string> {
  try {
    const payload = await response.json();
    if (typeof payload?.detail === 'string' && payload.detail.trim()) {
      return payload.detail;
    }
    if (typeof payload?.error === 'string' && payload.error.trim()) {
      return payload.error;
    }
    if (typeof payload?.message === 'string' && payload.message.trim()) {
      return payload.message;
    }
    return JSON.stringify(payload);
  } catch {
    const text = await response.text();
    return text || `${response.status} ${response.statusText}`;
  }
}

async function requestJson<T>(path: string, init: RequestInit = {}): Promise<T> {
  const response = await fetch(apiUrl(path), {
    ...init,
    headers: {
      'Content-Type': 'application/json',
      Accept: 'application/json',
      ...(init.headers || {}),
    },
  });

  if (!response.ok) {
    const message = await readError(response);
    throw new Error(message || `Request failed (${response.status})`);
  }

  return response.json() as Promise<T>;
}

function isMissingEndpointError(error: unknown): boolean {
  const message = String((error as any)?.message || '').toLowerCase();
  return (
    message.includes('404') ||
    message.includes('not found') ||
    message.includes('405') ||
    message.includes('method not allowed') ||
    message.includes('failed to fetch')
  );
}

export async function getGraphData(filters: {
  timeframe?: string;
  channels?: string[];
  brandSource?: string[];
  connectionStrength?: number;
  sentiment?: string[];
  topics?: string[];
  topN?: number;
  layers?: string[];
  insightMode?: string;
  sourceProfile?: string;
  confidenceThreshold?: number;
} = {}): Promise<GraphData> {
  const payload = {
    ...filters,
    channels: filters.channels || filters.brandSource || [],
    brandSource: filters.brandSource || filters.channels || [],
  };
  try {
    const data = await requestJson<GraphData>('/api/graph', {
      method: 'POST',
      body: JSON.stringify(payload),
    });
    return normalizeGraphData(data);
  } catch (error) {
    if (isMissingEndpointError(error)) {
      console.warn('[graph] /api/graph endpoint is not available yet. Returning empty graph.');
      return { nodes: [], links: [] };
    }
    throw error;
  }
}

export async function getNodeDetails(
  nodeId: string,
  nodeType: GraphNodeType | string,
  context: { timeframe?: string; channels?: string[] } = {},
): Promise<NodeDetails> {
  try {
    const params = new URLSearchParams({ nodeId, nodeType: String(nodeType) });
    if (context.timeframe) {
      params.set('timeframe', context.timeframe);
    }
    if (Array.isArray(context.channels) && context.channels.length > 0) {
      params.set('channels', context.channels.join(','));
    }
    const details = await requestJson<NodeDetails>(`/api/node-details?${params.toString()}`);
    return normalizeNodeDetails(details);
  } catch (error) {
    if (isMissingEndpointError(error)) {
      return {
        id: nodeId,
        name: nodeId,
        type: String(nodeType),
        insight: 'Node details endpoint is not connected yet.',
        recommendations: 'Backend graph node details API will be wired in the next step.',
      };
    }
    throw error;
  }
}

export async function searchGraph(query: string): Promise<SearchResult[]> {
  if (!query.trim()) return [];
  try {
    const params = new URLSearchParams({ query });
    const results = await requestJson<SearchResult[]>(`/api/search?${params.toString()}`);
    return results.map((result) => ({
      ...result,
      type: normalizeNodeType(result.type),
    }));
  } catch (error) {
    if (isMissingEndpointError(error)) {
      return [];
    }
    throw error;
  }
}

export async function askAI(
  query: string,
  context: { filters?: AIClientFilters } = {},
): Promise<AIQueryResult> {
  const payload = await requestJson<any>('/api/ai/query', {
    method: 'POST',
    body: JSON.stringify({
      query,
      filters: context.filters || {},
    }),
  });

  return {
    query,
    answer: String(payload?.answer || payload?.message || 'No answer returned.'),
    timestamp: String(payload?.timestamp || new Date().toISOString()),
    confidence: Number(payload?.confidence ?? 75),
    evidence: Array.isArray(payload?.evidence) ? payload.evidence : [],
    intent: String(payload?.intent || 'marketMap'),
    dataScope: (payload?.dataScope === 'full_db' ? 'full_db' : 'current_view') as AIDataScope,
    responseMode: (payload?.responseMode === 'aura' || payload?.responseMode === 'fallback')
      ? payload.responseMode
      : 'gemini',
    model: payload?.model,
    runtimeNote: payload?.runtimeNote,
    graphInstruction: payload?.graphInstruction,
  };
}

export async function getTrendingTopics(limit: number = 10, timeframe?: string): Promise<TrendingTopic[]> {
  try {
    const params = new URLSearchParams({ limit: String(limit) });
    if (timeframe) params.set('timeframe', timeframe);
    return await requestJson<TrendingTopic[]>(`/api/trending-topics?${params.toString()}`);
  } catch (error) {
    if (!isMissingEndpointError(error)) throw error;
  }

  try {
    const params = new URLSearchParams({ page: '0', size: String(Math.max(10, limit)) });
    const fallback = await requestJson<{ items?: Array<{ name?: string; mentions?: number }> }>(`/api/topics?${params.toString()}`);
    return (fallback.items || []).slice(0, limit).map((item, index) => ({
      id: `topic-${index}`,
      name: String(item.name || 'Unknown Topic'),
      adCount: Number(item.mentions || 0),
    }));
  } catch {
    return [];
  }
}

export async function getTopBrands(limit: number = 10, timeframe?: string): Promise<TopChannel[]> {
  return getTopChannels(limit, timeframe);
}

export async function getTopChannels(limit: number = 10, timeframe?: string): Promise<TopChannel[]> {
  try {
    const params = new URLSearchParams({ limit: String(limit) });
    if (timeframe) params.set('timeframe', timeframe);
    return await requestJson<TopChannel[]>(`/api/top-channels?${params.toString()}`);
  } catch (error) {
    if (!isMissingEndpointError(error)) throw error;
  }

  try {
    const params = new URLSearchParams({ page: '0', size: String(Math.max(10, limit)) });
      const fallback = await requestJson<{ items?: Array<{ title?: string }> }>(`/api/channels?${params.toString()}`);
    return (fallback.items || []).slice(0, limit).map((item, index) => ({
      id: `channel-${index}`,
      name: String(item.title || 'Unknown Channel'),
      adCount: 0,
    }));
  } catch {
    return [];
  }
}

export async function getAllBrands(): Promise<TopChannel[]> {
  return getAllChannels();
}

export async function getAllChannels(): Promise<TopChannel[]> {
  try {
    return await requestJson<TopChannel[]>('/api/all-channels');
  } catch (error) {
    if (!isMissingEndpointError(error)) throw error;
  }

  try {
    const fallback = await requestJson<{ items?: Array<{ title?: string }> }>('/api/channels?page=0&size=200');
    return (fallback.items || []).map((item, index) => ({
      id: `channel-${index}`,
      name: String(item.title || 'Unknown Channel'),
      adCount: 0,
    }));
  } catch {
    return [];
  }
}

export async function getSentimentDistribution(): Promise<SentimentData[]> {
  try {
    return await requestJson<SentimentData[]>('/api/sentiment-distribution');
  } catch (error) {
    if (!isMissingEndpointError(error)) throw error;
  }

  try {
    const dashboard = await requestJson<any>('/api/dashboard');
    const mood = Array.isArray(dashboard?.moodData) ? dashboard.moodData : [];
    return mood.map((row: any) => ({
      label: String(row.sentiment || row.label || 'Unknown'),
      count: Number(row.count || row.value || 0),
    }));
  } catch {
    return [];
  }
}

export async function getDailyBriefing(): Promise<{ briefing: string; timestamp: string }> {
  try {
    return await requestJson<{ briefing: string; timestamp: string }>('/api/daily-briefing');
  } catch {
    return {
      briefing: 'Daily briefing endpoint is not connected yet.',
      timestamp: new Date().toISOString(),
    };
  }
}

export async function getDataFreshness(forceRefresh: boolean = false): Promise<DataFreshnessSnapshot> {
  const params = new URLSearchParams();
  if (forceRefresh) params.set('force', 'true');
  const suffix = params.toString() ? `?${params.toString()}` : '';
  return requestJson<DataFreshnessSnapshot>(`/api/freshness${suffix}`);
}

export async function getGraphInsights(): Promise<{ insight: string; timestamp: string }> {
  try {
    return await requestJson<{ insight: string; timestamp: string }>('/api/graph-insights');
  } catch {
    return {
      insight: 'Graph insights endpoint is not connected yet.',
      timestamp: new Date().toISOString(),
    };
  }
}

export async function healthCheck(): Promise<{ status: string; nodeCount?: number }> {
  return requestJson<{ status: string; nodeCount?: number }>('/api/health');
}
