import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { isAccessDeniedError } from './apiErrors';
import {
  getSocialAds,
  getSocialAudienceResponse,
  getSocialCompetitors,
  getSocialEntities,
  getSocialEvidence,
  getSocialOverview,
  getSocialSummary,
  getSocialTopicTimeline,
  getSocialTopics,
  type SocialAdCard,
  type SocialAudienceEntityRow,
  type SocialAudienceSignalRow,
  type SocialCompetitorRow,
  type SocialEntityOption,
  type SocialEvidenceItem,
  type SocialIntelligenceFilters,
  type SocialOverviewResponse,
  type SocialSummaryResponse,
  type SocialTimelineBucket,
  type SocialTopicItem,
  type SocialTopicsResponse,
} from './socialIntelligence';
import { addDays, differenceInDaysInclusive, formatDateInput, parseDateInput } from '@/app/utils/dashboardDateRange';

export type SocialAdSort = 'recent' | 'engagement' | 'entity';
export type SocialCompetitorSort = 'posts' | 'adsRunning' | 'avgSentimentScore';
export type SocialSortDir = 'asc' | 'desc';

export interface SocialTopicListItem extends SocialTopicItem {
  previousCount: number;
  deltaCount: number;
  growthPct: number;
}

export interface SocialTopicTrendSeries {
  topic: string;
  items: SocialTimelineBucket[];
}

interface SocialDashboardState {
  overview: SocialOverviewResponse | null;
  entities: SocialEntityOption[];
  summary: SocialSummaryResponse | null;
  previousSummary: SocialSummaryResponse | null;
  topics: SocialTopicListItem[];
  previousTopics: SocialTopicItem[];
  topicTrendSeries: SocialTopicTrendSeries[];
  ads: SocialAdCard[];
  adsSummary: {
    topMarketingIntent: string | null;
    topCtaType: string | null;
    topProduct: string | null;
  } | null;
  audienceResponse: {
    entitySentiment: SocialAudienceEntityRow[];
    painPoints: SocialAudienceSignalRow[];
    customerIntent: SocialAudienceSignalRow[];
  } | null;
  competitors: SocialCompetitorRow[];
  loading: boolean;
  refreshing: boolean;
  error: string | null;
  accessDenied: boolean;
}

interface SocialTopicListState {
  entities: SocialEntityOption[];
  topics: SocialTopicListItem[];
  previousTopics: SocialTopicItem[];
  hasLiveData: boolean;
  loading: boolean;
  refreshing: boolean;
  error: string | null;
  accessDenied: boolean;
}

interface SocialTopicDetailState {
  timeline: SocialTimelineBucket[];
  evidenceItems: SocialEvidenceItem[];
  evidenceCount: number;
  page: number;
  hasLiveData: boolean;
  loading: boolean;
  loadingMore: boolean;
  error: string | null;
  accessDenied: boolean;
}

interface SocialTopicListSnapshot {
  entities: SocialEntityOption[];
  topics: SocialTopicListItem[];
  previousTopics: SocialTopicItem[];
  savedAt: string;
}

interface SocialTopicDetailSnapshot {
  timeline: SocialTimelineBucket[];
  evidenceItems: SocialEvidenceItem[];
  evidenceCount: number;
  page: number;
  savedAt: string;
}

const EMPTY_DASHBOARD_STATE: SocialDashboardState = {
  overview: null,
  entities: [],
  summary: null,
  previousSummary: null,
  topics: [],
  previousTopics: [],
  topicTrendSeries: [],
  ads: [],
  adsSummary: null,
  audienceResponse: null,
  competitors: [],
  loading: true,
  refreshing: false,
  error: null,
  accessDenied: false,
};

const EMPTY_TOPIC_LIST_STATE: SocialTopicListState = {
  entities: [],
  topics: [],
  previousTopics: [],
  hasLiveData: false,
  loading: true,
  refreshing: false,
  error: null,
  accessDenied: false,
};

const EMPTY_TOPIC_DETAIL_STATE: SocialTopicDetailState = {
  timeline: [],
  evidenceItems: [],
  evidenceCount: 0,
  page: 1,
  hasLiveData: false,
  loading: true,
  loadingMore: false,
  error: null,
  accessDenied: false,
};

function readSocialSnapshot<T>(key: string): T | null {
  if (typeof window === 'undefined') return null;
  try {
    const raw = window.sessionStorage.getItem(key);
    if (!raw) return null;
    return JSON.parse(raw) as T;
  } catch {
    return null;
  }
}

function writeSocialSnapshot<T>(key: string, value: T): void {
  if (typeof window === 'undefined') return;
  try {
    window.sessionStorage.setItem(key, JSON.stringify(value));
  } catch {
    // Snapshot cache is best-effort only.
  }
}

function socialTopicListCacheKey(filters: SocialIntelligenceFilters) {
  return `social-topics:list:${filters.from}:${filters.to}:${filters.entityId ?? 'all'}:${filters.platform ?? 'all'}`;
}

function socialTopicDetailCacheKey(filters: SocialIntelligenceFilters, topic: string) {
  return `social-topics:detail:${filters.from}:${filters.to}:${filters.entityId ?? 'all'}:${filters.platform ?? 'all'}:${normalizeTopicKey(topic)}`;
}

function buildPreviousFilters(filters: SocialIntelligenceFilters): SocialIntelligenceFilters {
  const dayCount = differenceInDaysInclusive(filters.from, filters.to);
  const currentFrom = parseDateInput(filters.from);
  const previousTo = addDays(currentFrom, -1);
  const previousFrom = addDays(currentFrom, -dayCount);
  return {
    from: formatDateInput(previousFrom),
    to: formatDateInput(previousTo),
    entityId: filters.entityId,
    platform: filters.platform,
  };
}

function normalizeTopicKey(value: string) {
  return value.trim().toLowerCase();
}

function decorateTopicsWithChange(currentTopics: SocialTopicItem[], previousTopics: SocialTopicItem[]): SocialTopicListItem[] {
  const previousCounts = new Map(
    previousTopics.map((topic) => [normalizeTopicKey(topic.topic), topic.count] as const),
  );

  return currentTopics.map((topic) => {
    const previousCount = previousCounts.get(normalizeTopicKey(topic.topic)) ?? 0;
    const deltaCount = topic.count - previousCount;
    const growthPct = previousCount > 0
      ? Math.round((deltaCount / previousCount) * 100)
      : (topic.count > 0 ? 100 : 0);
    return {
      ...topic,
      previousCount,
      deltaCount,
      growthPct,
    };
  });
}

function firstRejectedMessage(results: PromiseSettledResult<unknown>[]): string | null {
  const rejected = results.find((result): result is PromiseRejectedResult => result.status === 'rejected');
  if (!rejected) return null;
  return rejected.reason instanceof Error ? rejected.reason.message : String(rejected.reason ?? 'Request failed');
}

function hasAccessDenied(results: PromiseSettledResult<unknown>[]): boolean {
  return results.some((result) => result.status === 'rejected' && isAccessDeniedError(result.reason));
}

function topicResponseError(response: SocialTopicsResponse | null | undefined): string | null {
  const degradedSections = response?.meta?.degradedSections ?? [];
  const semanticTopicSections = new Set(['topicLandscape', 'topicRanking', 'semanticTopics']);
  const hasTopicDegradation = degradedSections.some((section) => semanticTopicSections.has(section));
  if (hasTopicDegradation) return 'Social topics are temporarily unavailable. Showing the last saved topic view.';
  return response?.meta?.error ?? null;
}

export function useSocialDashboardData(
  filters: SocialIntelligenceFilters,
  options: {
    adSort: SocialAdSort;
    scorecardSort: SocialCompetitorSort;
    scorecardSortDir: SocialSortDir;
  },
) {
  const [state, setState] = useState<SocialDashboardState>(EMPTY_DASHBOARD_STATE);
  const requestIdRef = useRef(0);
  const previousFilters = useMemo(() => buildPreviousFilters(filters), [filters]);

  const load = useCallback(async (refresh = false) => {
    const requestId = requestIdRef.current + 1;
    requestIdRef.current = requestId;

    setState((current) => ({
      ...current,
      loading: refresh ? current.loading : true,
      refreshing: refresh,
      error: null,
      accessDenied: false,
    }));

    const coreResults = await Promise.allSettled([
      getSocialOverview(),
      getSocialEntities(),
      getSocialSummary(filters),
      getSocialTopics(filters),
      getSocialAds(filters, { sort: options.adSort }),
      getSocialAudienceResponse(filters),
      getSocialCompetitors(filters, {
        sortBy: options.scorecardSort,
        sortDir: options.scorecardSortDir,
      }),
    ]);

    if (requestIdRef.current !== requestId) return;

    if (hasAccessDenied(coreResults)) {
      setState((current) => ({
        ...current,
        loading: false,
        refreshing: false,
        accessDenied: true,
        error: null,
      }));
      return;
    }

    const coreError = firstRejectedMessage(coreResults);

    const overviewRes = coreResults[0];
    const entitiesRes = coreResults[1];
    const summaryRes = coreResults[2];
    const topicsRes = coreResults[3];
    const adsRes = coreResults[4];
    const audienceRes = coreResults[5];
    const competitorsRes = coreResults[6];

    const currentTopics = topicsRes.status === 'fulfilled' ? topicsRes.value.items : [];

    const secondaryResults = await Promise.allSettled([
      getSocialSummary(previousFilters),
      getSocialTopics(previousFilters),
      ...currentTopics.slice(0, 4).map((topic) => getSocialTopicTimeline(filters, topic.topic)),
    ]);

    if (requestIdRef.current !== requestId) return;

    if (hasAccessDenied(secondaryResults)) {
      setState((current) => ({
        ...current,
        loading: false,
        refreshing: false,
        accessDenied: true,
        error: null,
      }));
      return;
    }

    const previousSummaryRes = secondaryResults[0];
    const previousTopicsRes = secondaryResults[1];
    const trendSeriesResults = secondaryResults.slice(2);
    const previousTopics = previousTopicsRes.status === 'fulfilled' ? previousTopicsRes.value.items : [];
    const decoratedTopics = decorateTopicsWithChange(currentTopics, previousTopics);

    const topicTrendSeries: SocialTopicTrendSeries[] = trendSeriesResults.flatMap((result, index) => {
      if (result.status !== 'fulfilled') return [];
      const topic = currentTopics[index];
      if (!topic) return [];
      return [{ topic: topic.topic, items: result.value.items }];
    });

    const secondaryError = firstRejectedMessage(secondaryResults);

    setState({
      overview: overviewRes.status === 'fulfilled' ? overviewRes.value : null,
      entities: entitiesRes.status === 'fulfilled' ? entitiesRes.value.items : [],
      summary: summaryRes.status === 'fulfilled' ? summaryRes.value : null,
      previousSummary: previousSummaryRes.status === 'fulfilled' ? previousSummaryRes.value : null,
      topics: decoratedTopics,
      previousTopics,
      topicTrendSeries,
      ads: adsRes.status === 'fulfilled' ? adsRes.value.items : [],
      adsSummary: adsRes.status === 'fulfilled' ? adsRes.value.summary : null,
      audienceResponse: audienceRes.status === 'fulfilled' ? audienceRes.value : null,
      competitors: competitorsRes.status === 'fulfilled' ? competitorsRes.value.items : [],
      loading: false,
      refreshing: false,
      error: coreError || secondaryError,
      accessDenied: false,
    });
  }, [filters, options.adSort, options.scorecardSort, options.scorecardSortDir, previousFilters]);

  useEffect(() => {
    void load(false);
  }, [load]);

  const refresh = useCallback(() => {
    void load(true);
  }, [load]);

  return {
    ...state,
    refresh,
  };
}

export function useSocialTopicListData(filters: SocialIntelligenceFilters) {
  const [state, setState] = useState<SocialTopicListState>(EMPTY_TOPIC_LIST_STATE);
  const requestIdRef = useRef(0);
  const previousFilters = useMemo(() => buildPreviousFilters(filters), [filters]);
  const snapshotKey = useMemo(() => socialTopicListCacheKey(filters), [filters]);

  useEffect(() => {
    const snapshot = readSocialSnapshot<SocialTopicListSnapshot>(snapshotKey);
    if (!snapshot) {
      setState(EMPTY_TOPIC_LIST_STATE);
      return;
    }
    setState({
      entities: snapshot.entities,
      topics: snapshot.topics,
      previousTopics: snapshot.previousTopics,
      hasLiveData: true,
      loading: false,
      refreshing: true,
      error: null,
      accessDenied: false,
    });
  }, [snapshotKey]);

  const load = useCallback(async (refresh = false) => {
    const requestId = requestIdRef.current + 1;
    requestIdRef.current = requestId;

    setState((current) => ({
      ...current,
      loading: refresh ? current.loading : true,
      refreshing: refresh,
      error: null,
      accessDenied: false,
    }));

    const results = await Promise.allSettled([
      getSocialEntities(),
      getSocialTopics(filters, { limit: 100 }),
      getSocialTopics(previousFilters, { limit: 100 }),
    ]);

    if (requestIdRef.current !== requestId) return;

    if (hasAccessDenied(results)) {
      setState((current) => ({
        ...current,
        loading: false,
        refreshing: false,
        accessDenied: true,
        error: null,
      }));
      return;
    }

    const entitiesRes = results[0];
    const topicsRes = results[1];
    const previousTopicsRes = results[2];
    const currentTopicsError = topicsRes.status === 'fulfilled'
      ? topicResponseError(topicsRes.value)
      : (topicsRes.reason instanceof Error ? topicsRes.reason.message : String(topicsRes.reason ?? 'Failed to load social topics'));
    const previousTopicsError = previousTopicsRes.status === 'fulfilled'
      ? topicResponseError(previousTopicsRes.value)
      : null;

    setState((current) => {
      const hasFreshTopics = topicsRes.status === 'fulfilled' && !currentTopicsError;
      const currentTopics = hasFreshTopics ? topicsRes.value.items : current.topics;
      const previousTopics = previousTopicsRes.status === 'fulfilled' && !previousTopicsError
        ? previousTopicsRes.value.items
        : current.previousTopics;
      const decoratedTopics = hasFreshTopics
        ? decorateTopicsWithChange(currentTopics, previousTopics)
        : current.topics;
      const nextEntities = entitiesRes.status === 'fulfilled' ? entitiesRes.value.items : current.entities;
      const error = currentTopicsError || previousTopicsError || firstRejectedMessage(results);
      const nextState: SocialTopicListState = {
        entities: nextEntities,
        topics: decoratedTopics,
        previousTopics,
        hasLiveData: hasFreshTopics || current.hasLiveData,
        loading: false,
        refreshing: false,
        error,
        accessDenied: false,
      };

      if (hasFreshTopics) {
        writeSocialSnapshot<SocialTopicListSnapshot>(snapshotKey, {
          entities: nextEntities,
          topics: decoratedTopics,
          previousTopics,
          savedAt: new Date().toISOString(),
        });
      }

      return nextState;
    });
  }, [filters, previousFilters, snapshotKey]);

  useEffect(() => {
    void load(false);
  }, [load]);

  const refresh = useCallback(() => {
    void load(true);
  }, [load]);

  return {
    ...state,
    refresh,
  };
}

export function useSocialTopicDetailData(
  filters: SocialIntelligenceFilters,
  topic: string | null,
  enabled: boolean,
  pageSize = 20,
) {
  const [state, setState] = useState<SocialTopicDetailState>(EMPTY_TOPIC_DETAIL_STATE);
  const requestIdRef = useRef(0);
  const snapshotKey = useMemo(
    () => (topic ? socialTopicDetailCacheKey(filters, topic) : null),
    [filters, topic],
  );

  useEffect(() => {
    if (!enabled || !topic || !snapshotKey) {
      setState({
        ...EMPTY_TOPIC_DETAIL_STATE,
        loading: false,
      });
      return;
    }
    const snapshot = readSocialSnapshot<SocialTopicDetailSnapshot>(snapshotKey);
    if (!snapshot) return;
    setState({
      timeline: snapshot.timeline,
      evidenceItems: snapshot.evidenceItems,
      evidenceCount: snapshot.evidenceCount,
      page: snapshot.page,
      hasLiveData: true,
      loading: false,
      loadingMore: false,
      error: null,
      accessDenied: false,
    });
  }, [enabled, snapshotKey, topic]);

  const load = useCallback(async () => {
    if (!enabled || !topic) {
      setState({
        ...EMPTY_TOPIC_DETAIL_STATE,
        loading: false,
      });
      return;
    }

    const requestId = requestIdRef.current + 1;
    requestIdRef.current = requestId;

    setState((current) => ({
      ...current,
      loading: true,
      error: null,
      accessDenied: false,
      page: 1,
    }));

    const results = await Promise.allSettled([
      getSocialTopicTimeline(filters, topic),
      getSocialEvidence(filters, {
        topic,
        page: 1,
        size: pageSize,
      }),
    ]);

    if (requestIdRef.current !== requestId) return;

    if (hasAccessDenied(results)) {
      setState((current) => ({
        ...current,
        loading: false,
        accessDenied: true,
        error: null,
      }));
      return;
    }

    const timelineRes = results[0];
    const evidenceRes = results[1];
    setState((current) => {
      const hasFreshTimeline = timelineRes.status === 'fulfilled';
      const hasFreshEvidence = evidenceRes.status === 'fulfilled';
      const nextTimeline = hasFreshTimeline ? timelineRes.value.items : current.timeline;
      const nextEvidenceItems = hasFreshEvidence ? evidenceRes.value.items : current.evidenceItems;
      const nextEvidenceCount = hasFreshEvidence ? evidenceRes.value.count : current.evidenceCount;
      const hasFreshDetail = hasFreshTimeline && hasFreshEvidence;
      const nextState: SocialTopicDetailState = {
        timeline: nextTimeline,
        evidenceItems: nextEvidenceItems,
        evidenceCount: nextEvidenceCount,
        page: hasFreshEvidence ? 1 : current.page,
        hasLiveData: hasFreshDetail || current.hasLiveData,
        loading: false,
        loadingMore: false,
        error: firstRejectedMessage(results),
        accessDenied: false,
      };

      if (hasFreshDetail && snapshotKey) {
        writeSocialSnapshot<SocialTopicDetailSnapshot>(snapshotKey, {
          timeline: nextTimeline,
          evidenceItems: nextEvidenceItems,
          evidenceCount: nextEvidenceCount,
          page: 1,
          savedAt: new Date().toISOString(),
        });
      }

      return nextState;
    });
  }, [enabled, filters, pageSize, snapshotKey, topic]);

  useEffect(() => {
    void load();
  }, [load]);

  const refresh = useCallback(() => {
    void load();
  }, [load]);

  const loadMore = useCallback(async () => {
    if (!enabled || !topic) return;
    if (state.loading || state.loadingMore || state.evidenceItems.length >= state.evidenceCount) return;

    const nextPage = state.page + 1;
    setState((current) => ({
      ...current,
      loadingMore: true,
      error: null,
    }));

    try {
      const response = await getSocialEvidence(filters, {
        topic,
        page: nextPage,
        size: pageSize,
      });
      setState((current) => {
        const nextEvidenceItems = [...current.evidenceItems, ...response.items];
        const nextState = {
          ...current,
          evidenceItems: nextEvidenceItems,
          evidenceCount: response.count,
          page: nextPage,
          hasLiveData: true,
          loadingMore: false,
        };
        if (snapshotKey) {
          writeSocialSnapshot<SocialTopicDetailSnapshot>(snapshotKey, {
            timeline: nextState.timeline,
            evidenceItems: nextEvidenceItems,
            evidenceCount: response.count,
            page: nextPage,
            savedAt: new Date().toISOString(),
          });
        }
        return nextState;
      });
    } catch (error) {
      if (isAccessDeniedError(error)) {
        setState((current) => ({
          ...current,
          loadingMore: false,
          accessDenied: true,
          error: null,
        }));
        return;
      }
      setState((current) => ({
        ...current,
        loadingMore: false,
        error: error instanceof Error ? error.message : 'Failed to load more evidence',
      }));
    }
  }, [enabled, filters, pageSize, snapshotKey, state.evidenceCount, state.evidenceItems.length, state.loading, state.loadingMore, state.page, topic]);

  return {
    ...state,
    hasMore: state.evidenceItems.length < state.evidenceCount,
    refresh,
    loadMore,
  };
}
