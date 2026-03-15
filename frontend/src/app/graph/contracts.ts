export type AIDataScope = 'full_db' | 'current_view';

export interface AIClientFilters {
  channels?: string[];
  sentiments?: string[];
  timeframe?: string;
  topics?: string[];
  connectionStrength?: number;
  layers?: string[];
  insightMode?: string;
  sourceProfile?: string;
  confidenceThreshold?: number;
}

export interface AIEvidenceItem {
  label: string;
  value: string;
}

export interface AIGraphInstruction {
  mode: 'filter_patch';
  filters: AIClientFilters;
}

export interface AIQueryResult {
  query: string;
  answer: string;
  timestamp: string;
  confidence: number;
  evidence: AIEvidenceItem[];
  intent: string;
  dataScope: AIDataScope;
  responseMode: 'aura' | 'gemini' | 'fallback';
  model?: string;
  runtimeNote?: string;
  graphInstruction?: AIGraphInstruction;
}

export interface SearchResult {
  type: string;
  id: string;
  name: string;
  text?: string;
}

export interface TrendingTopic {
  name: string;
  id: string;
  adCount: number;
}

export interface TopChannel {
  name: string;
  id: string;
  adCount: number;
}

export interface SentimentData {
  label: string;
  count: number;
}

export type InsightAudience = 'executive' | 'analyst';

export interface InsightEvidenceItem {
  query_id: string;
  metric: string;
  value: number | string | null;
  note?: string;
}

export interface InsightCard {
  id: string;
  title: string;
  summary: string;
  why_it_matters: string;
  confidence: number;
  priority: 'high' | 'medium' | 'low';
  audience: InsightAudience;
  evidence: InsightEvidenceItem[];
  generated_at: string;
}

export interface InsightCardsResponse {
  cards: InsightCard[];
  source: 'ai' | 'deterministic_fallback';
  generated_at: string;
}
