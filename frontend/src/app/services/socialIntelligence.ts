import { apiFetch } from './api';

export type SocialPlatform = 'all' | 'facebook' | 'instagram' | 'google' | 'tiktok';

export interface SocialEntityOption {
  id: string;
  name: string;
  is_active: boolean;
}

export interface SocialRuntimeStatus {
  status: 'active' | 'stopped';
  is_active: boolean;
  interval_minutes: number;
  running_now: boolean;
  last_run_started_at: string | null;
  last_run_finished_at: string | null;
  last_success_at: string | null;
  next_run_at: string | null;
  last_error: string | null;
  last_result?: {
    accounts_processed?: number;
    activities_collected?: number;
    activities_analyzed?: number;
    activities_graph_synced?: number;
    collect_failures?: number;
    analysis_failures?: number;
    graph_failures?: number;
  } | null;
  postgres_worker_enabled?: boolean;
}

export interface SocialOverviewResponse {
  entities_total: number;
  entities_active: number;
  activities_total: number;
  platform_counts: Record<string, number>;
  analysis_status_counts: Record<string, number>;
  account_health_counts?: Record<string, number>;
  queue_depth?: {
    analysis?: number;
    graph?: number;
  };
  dead_letter_failures: number;
  stale_entities: Array<{ entity_id: string; name: string; reason: string; age_hours?: number }>;
  runtime: SocialRuntimeStatus;
}

export interface SocialSummaryResponse {
  trackedCompetitors: number;
  postsCollected: number;
  adsDetected: number;
  averageSentimentScore: number;
  averageSentimentPct: number;
  dominantTopic: {
    name: string;
    count: number;
  };
}

export interface SocialTimelineBucket {
  bucket: string;
  total: number;
  positive: number;
  neutral: number;
  negative: number;
}

export interface SocialTopicItem {
  topic: string;
  count: number;
  avgSentimentScore: number;
  sentimentCounts: {
    positive: number;
    neutral: number;
    negative: number;
  };
  topEntities: string[];
  topPlatforms: string[];
  sampleSummary: string;
}

export interface SocialTopicsResponse {
  items: SocialTopicItem[];
  meta?: {
    degradedSections?: string[];
    error?: string | null;
    [key: string]: unknown;
  };
}

export interface SocialAdCard {
  id: string;
  activity_uid: string;
  platform: string;
  source_kind: string;
  source_url: string | null;
  text_content: string | null;
  published_at: string | null;
  cta_type: string | null;
  content_format: string | null;
  engagement_metrics?: Record<string, number> | null;
  engagementTotal: number;
  entity?: {
    id: string;
    name: string;
  } | null;
  analysis?: {
    summary?: string | null;
    analysis_payload?: Record<string, unknown> | null;
  } | null;
  analysisHighlights: {
    marketingIntent: string | null;
    products: string[];
    valuePropositions: string[];
    urgencyIndicators: string[];
  };
}

export interface SocialAudienceEntityRow {
  entityId: string | null;
  entityName: string;
  total: number;
  positive: number;
  neutral: number;
  negative: number;
  avgSentimentScore: number;
}

export interface SocialAudienceSignalRow {
  label: string;
  count: number;
  entities: string[];
  dominantSentiment: 'positive' | 'neutral' | 'negative';
}

export interface SocialCompetitorRow {
  entityId: string;
  entityName: string;
  posts: number;
  adsRunning: number;
  avgSentimentScore: number;
  topMarketingIntent: string | null;
  keyTopics: string[];
  valueProps: string[];
  productsPromoted: string[];
  recentActivities: SocialEvidenceItem[];
}

export interface SocialEvidenceItem {
  id: string;
  activity_uid: string;
  platform: string;
  source_kind: string;
  source_url: string | null;
  text_content: string | null;
  published_at: string | null;
  author_handle?: string | null;
  cta_type?: string | null;
  content_format?: string | null;
  entity?: {
    id: string;
    name: string;
  } | null;
  analysis?: {
    summary?: string | null;
    marketing_intent?: string | null;
    sentiment?: string | null;
    sentiment_score?: number | null;
    analysis_payload?: Record<string, unknown> | null;
  } | null;
}

export interface SocialAccount {
  id?: string;
  platform: 'facebook' | 'instagram' | 'google' | 'tiktok';
  account_handle: string | null;
  account_external_id: string | null;
  domain: string | null;
  is_active: boolean;
  health_status?: string | null;
  last_health_error?: string | null;
  last_health_checked_at?: string | null;
  last_collected_at?: string | null;
}

export interface SocialEntity {
  id: string;
  legacy_company_id: string;
  name: string;
  industry: string | null;
  website: string | null;
  is_active: boolean;
  platform_accounts: Record<string, SocialAccount | null>;
  accounts: SocialAccount[];
}

export interface SocialActivityAnalysis {
  summary?: string | null;
  marketing_intent?: string | null;
  sentiment?: string | null;
  sentiment_score?: number | null;
  analysis_payload?: {
    summary?: string | null;
    marketing_intent?: string | null;
    topics?: Array<string | { name?: string }>;
    audience_segments?: Array<string | { name?: string }>;
  } | null;
}

export interface SocialActivity {
  id: string;
  activity_uid: string;
  platform: string;
  source_kind: string;
  source_url: string;
  text_content: string | null;
  published_at: string | null;
  author_handle: string | null;
  cta_type: string | null;
  content_format: string | null;
  region_name: string | null;
  ingest_status: string;
  analysis_status: string;
  graph_status: string;
  entity?: {
    id: string;
    name: string;
  } | null;
  analysis?: SocialActivityAnalysis | null;
}

export interface SocialRuntimeFailure {
  id: string;
  stage: string;
  scope_key: string;
  last_error: string | null;
  is_dead_letter: boolean;
  last_failed_at: string;
}

export interface SocialEntityAccountUpdateInput {
  platform: 'facebook' | 'instagram' | 'google';
  account_external_id?: string | null;
  account_handle?: string | null;
  domain?: string | null;
  is_active: boolean;
}

export interface SocialEntityUpdateInput {
  is_active: boolean;
  accounts: SocialEntityAccountUpdateInput[];
}

export interface SocialIntelligenceFilters {
  from: string;
  to: string;
  entityId?: string;
  platform?: SocialPlatform;
}

function buildQuery(params: Record<string, string | number | undefined | null>) {
  const search = new URLSearchParams();
  Object.entries(params).forEach(([key, value]) => {
    if (value === undefined || value === null || value === '' || value === 'all') return;
    search.set(key, String(value));
  });
  const query = search.toString();
  return query ? `?${query}` : '';
}

function withRange(filters: SocialIntelligenceFilters, extras: Record<string, string | number | undefined | null> = {}) {
  return buildQuery({
    from: filters.from,
    to: filters.to,
    entity_id: filters.entityId,
    platform: filters.platform,
    ...extras,
  });
}

export async function getSocialEntities() {
  return apiFetch<{ count: number; items: SocialEntityOption[] }>('/social/entities', { includeUserAuth: true });
}

export async function getSocialOverview() {
  return apiFetch<SocialOverviewResponse>('/social/overview', { includeUserAuth: true });
}

export async function getSocialAdminEntities() {
  return apiFetch<{ count: number; items: SocialEntity[] }>('/social/entities', { includeUserAuth: true });
}

export async function getSocialActivities(options: { limit?: number } = {}) {
  return apiFetch<{ items: SocialActivity[] }>(
    `/social/activities${buildQuery({ limit: options.limit ?? 80 })}`,
    { includeUserAuth: true },
  );
}

export async function getSocialRuntimeFailures(options: { deadLetterOnly?: boolean; limit?: number } = {}) {
  return apiFetch<{ items: SocialRuntimeFailure[] }>(
    `/social/runtime/failures${buildQuery({
      dead_letter_only: options.deadLetterOnly ? 'true' : undefined,
      limit: options.limit ?? 20,
    })}`,
    { includeUserAuth: true },
  );
}

export async function runSocialRuntimeOnce() {
  return apiFetch<{ ok: boolean }>('/social/runtime/run-once', {
    method: 'POST',
    includeUserAuth: true,
  });
}

export async function updateSocialEntity(entityId: string, payload: SocialEntityUpdateInput) {
  return apiFetch<{ ok: boolean; item?: SocialEntity }>(`/social/entities/${entityId}`, {
    method: 'PATCH',
    includeUserAuth: true,
    body: JSON.stringify(payload),
  });
}

export async function retrySocialRuntimeFailure(stage: string, scopeKey: string) {
  return apiFetch<{ ok: boolean }>('/social/runtime/retry', {
    method: 'POST',
    includeUserAuth: true,
    body: JSON.stringify({
      stage,
      scope_key: scopeKey,
    }),
  });
}

export async function replaySocialActivities(stage: 'analysis' | 'graph', activityUids: string[]) {
  return apiFetch<{ ok: boolean }>('/social/runtime/replay', {
    method: 'POST',
    includeUserAuth: true,
    body: JSON.stringify({
      stage,
      activity_uids: activityUids,
    }),
  });
}

export async function getSocialSummary(filters: SocialIntelligenceFilters) {
  return apiFetch<SocialSummaryResponse>(`/social/intelligence/summary${withRange(filters)}`, { includeUserAuth: true });
}

export async function getSocialTopicTimeline(filters: SocialIntelligenceFilters, topic?: string | null) {
  return apiFetch<{ items: SocialTimelineBucket[] }>(`/social/intelligence/topic-timeline${withRange(filters, { topic })}`, { includeUserAuth: true });
}

export async function getSocialTopics(filters: SocialIntelligenceFilters, options: { limit?: number } = {}) {
  return apiFetch<SocialTopicsResponse>(
    `/social/intelligence/topics${withRange(filters, { limit: options.limit })}`,
    { includeUserAuth: true },
  );
}

export async function getSocialAds(
  filters: SocialIntelligenceFilters,
  options: { sort?: string; ctaType?: string; contentFormat?: string } = {},
) {
  return apiFetch<{ count: number; items: SocialAdCard[]; summary: { topMarketingIntent: string | null; topCtaType: string | null; topProduct: string | null } }>(
    `/social/intelligence/ads${withRange(filters, {
      sort: options.sort,
      cta_type: options.ctaType,
      content_format: options.contentFormat,
    })}`,
    { includeUserAuth: true },
  );
}

export async function getSocialAudienceResponse(filters: SocialIntelligenceFilters) {
  return apiFetch<{
    entitySentiment: SocialAudienceEntityRow[];
    painPoints: SocialAudienceSignalRow[];
    customerIntent: SocialAudienceSignalRow[];
  }>(`/social/intelligence/audience-response${withRange(filters)}`, { includeUserAuth: true });
}

export async function getSocialCompetitors(
  filters: SocialIntelligenceFilters,
  options: { sortBy?: string; sortDir?: string } = {},
) {
  return apiFetch<{ items: SocialCompetitorRow[] }>(
    `/social/intelligence/competitors${withRange(filters, {
      sort_by: options.sortBy,
      sort_dir: options.sortDir,
    })}`,
    { includeUserAuth: true },
  );
}

export async function getSocialEvidence(
  filters: SocialIntelligenceFilters,
  options: {
    activityUid?: string;
    topic?: string;
    entityId?: string;
    marketingIntent?: string;
    painPoint?: string;
    customerIntent?: string;
    sourceKind?: string;
    ctaType?: string;
    contentFormat?: string;
    sentiment?: string;
    page?: number;
    size?: number;
  } = {},
) {
  return apiFetch<{ count: number; items: SocialEvidenceItem[] }>(
    `/social/intelligence/evidence${withRange(filters, {
      activity_uid: options.activityUid,
      topic: options.topic,
      entity_id: options.entityId,
      marketing_intent: options.marketingIntent,
      pain_point: options.painPoint,
      customer_intent: options.customerIntent,
      source_kind: options.sourceKind,
      cta_type: options.ctaType,
      content_format: options.contentFormat,
      sentiment: options.sentiment,
      page: options.page,
      size: options.size,
    })}`,
    { includeUserAuth: true, timeoutMs: 20_000 },
  );
}
