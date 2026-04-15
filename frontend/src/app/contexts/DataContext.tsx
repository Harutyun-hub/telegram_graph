// ================================================================
// DATA CONTEXT — Production-ready single data entry point
// ================================================================
// Default mode fetches live dashboard data from backend /api/dashboard.
// If request fails, stale data is preserved and safe empty defaults remain.
//
// Backend payload is normalized via dashboardAdapter so widget contracts stay stable.
//
// ⚠️ BACKEND CONNECTIVITY NOTES:
//   - AbortController is wired up for cleanup on unmount
//   - refresh() triggers a full re-fetch (debounce-safe)
//   - Error state includes the error message for UI display
//   - Loading state is true during initial fetch AND refreshes
//   - Stale data is preserved during refresh (no flash of empty)
//
// Usage in any widget:
//   const { data, loading, error, refresh } = useData();
//   if (loading) return <Skeleton />;
//   const topics = data.trendingTopics[lang];
// ================================================================

import { createContext, useContext, useState, useEffect, useCallback, useRef } from 'react';
import type { ReactNode } from 'react';
import type { AppData } from '../types/data';
import { adaptDashboardPayload, createEmptyAppData } from '../services/dashboardAdapter';
import { apiFetch } from '../services/api';
import { useDashboardDateRange } from './DashboardDateRangeContext';
import { getDefaultDashboardWarmRetryDelay, shouldRetryDefaultDashboardWarm } from './dashboardRetryPolicy';

interface DashboardRangeRef {
  from: string;
  to: string;
}

interface DashboardMeta {
  trustedEndDate?: string;
  freshnessStatus?: string;
  rangeLabel?: string;
  requestedFrom?: string;
  requestedTo?: string;
  degradedTiers?: string[];
  suppressedDegradedTiers?: string[];
  skippedTiers?: string[];
  tierTimes?: Record<string, number | null>;
  snapshotBuiltAt?: string;
  cacheStatus?: string;
  cacheSource?: string;
  isStale?: boolean;
  refreshFailureCount?: number;
  responseBytes?: number;
  responseSerializeMs?: number;
}

const DASHBOARD_TIMEOUT_MS = 30_000;

interface DataContextValue {
  data: AppData;
  loading: boolean;
  isRefreshing: boolean;
  hasLiveData: boolean;
  isStaleForSelection: boolean;
  error: string | null;
  dashboardMeta: DashboardMeta | null;
  selectedRange: DashboardRangeRef | null;
  visibleRange: DashboardRangeRef | null;
  lastSuccessfulRange: DashboardRangeRef | null;
  /** Call this to manually refresh data from the API */
  refresh: () => void;
}

// Provides safe defaults so useData() always returns a usable object,
// even if called outside of DataProvider (e.g., during testing).
const DataContext = createContext<DataContextValue>({
  data: createEmptyAppData(),
  loading: false,
  isRefreshing: false,
  hasLiveData: false,
  isStaleForSelection: false,
  error: null,
  dashboardMeta: null,
  selectedRange: null,
  visibleRange: null,
  lastSuccessfulRange: null,
  refresh: () => {},
});

function snapshotKeyForRange(from: string, to: string): string {
  return `radar.dashboard.snapshot.v5:${from}:${to}`;
}

function sameRange(a: DashboardRangeRef | null, b: DashboardRangeRef | null): boolean {
  return Boolean(a && b && a.from === b.from && a.to === b.to);
}

function loadSnapshot(from: string, to: string): { data: AppData; meta: DashboardMeta | null } | null {
  if (typeof window === 'undefined') return null;
  try {
    const raw = window.sessionStorage.getItem(snapshotKeyForRange(from, to));
    if (!raw) return null;
    const parsed = JSON.parse(raw) as AppData | { data?: AppData; meta?: DashboardMeta | null };
    const snapshot = parsed && typeof parsed === 'object' && 'data' in parsed
      ? { data: parsed.data as AppData, meta: (parsed as { meta?: DashboardMeta | null }).meta ?? null }
      : { data: parsed as AppData, meta: null };
    const payload = snapshot.data;
    const jobItems = payload?.jobSeeking?.en ?? [];
    const hasJobDataWithoutEvidence = Array.isArray(jobItems)
      && jobItems.length > 0
      && !jobItems.some((item: any) => Array.isArray(item?.evidence) && item.evidence.length > 0);
    return hasJobDataWithoutEvidence ? null : snapshot;
  } catch {
    return null;
  }
}

function saveSnapshot(from: string, to: string, data: AppData, meta: DashboardMeta | null): void {
  if (typeof window === 'undefined') return;
  try {
    window.sessionStorage.setItem(snapshotKeyForRange(from, to), JSON.stringify({ data, meta }));
  } catch {
    // ignore storage errors
  }
}

// ── Data fetching logic ───────────────────────────────────────
// Live backend mode: fetch dashboard payload and normalize with adapter.
async function fetchData(from: string, to: string, signal?: AbortSignal): Promise<{ data: AppData; meta: DashboardMeta | null }> {
  const params = new URLSearchParams({ from, to });
  const payload = await apiFetch<any>(`/dashboard?${params.toString()}`, {
    method: 'GET',
    timeoutMs: DASHBOARD_TIMEOUT_MS,
    signal,
    headers: { Accept: 'application/json' },
    cache: 'no-store',
  });
  return {
    data: adaptDashboardPayload(payload),
    meta: {
      trustedEndDate: payload?.meta?.trustedEndDate,
      freshnessStatus: payload?.meta?.freshness?.status,
      rangeLabel: payload?.meta?.rangeLabel,
      requestedFrom: payload?.meta?.requestedFrom,
      requestedTo: payload?.meta?.requestedTo,
      degradedTiers: Array.isArray(payload?.meta?.degradedTiers) ? payload.meta.degradedTiers : [],
      suppressedDegradedTiers: Array.isArray(payload?.meta?.suppressedDegradedTiers) ? payload.meta.suppressedDegradedTiers : [],
      skippedTiers: Array.isArray(payload?.meta?.skippedTiers) ? payload.meta.skippedTiers : [],
      tierTimes: payload?.meta?.tierTimes && typeof payload.meta.tierTimes === 'object' ? payload.meta.tierTimes : {},
      snapshotBuiltAt: payload?.meta?.snapshotBuiltAt,
      cacheStatus: payload?.meta?.cacheStatus,
      cacheSource: payload?.meta?.cacheSource,
      isStale: Boolean(payload?.meta?.isStale),
      refreshFailureCount: Number.isFinite(Number(payload?.meta?.refreshFailureCount))
        ? Number(payload.meta.refreshFailureCount)
        : undefined,
      responseBytes: Number.isFinite(Number(payload?.meta?.responseBytes)) ? Number(payload.meta.responseBytes) : undefined,
      responseSerializeMs: Number.isFinite(Number(payload?.meta?.responseSerializeMs)) ? Number(payload.meta.responseSerializeMs) : undefined,
    },
  };
}

export function DataProvider({ children }: { children: ReactNode }) {
  const { range, ready, trustedEndDate } = useDashboardDateRange();
  const initialSnapshot = loadSnapshot(range.from, range.to);
  const [appData, setAppData] = useState<AppData>(initialSnapshot?.data ?? createEmptyAppData());
  const [hasLiveData, setHasLiveData] = useState(Boolean(initialSnapshot));
  const [loading, setLoading] = useState(!initialSnapshot || !ready);
  const [error, setError] = useState<string | null>(null);
  const [dashboardMeta, setDashboardMeta] = useState<DashboardMeta | null>(initialSnapshot?.meta ?? null);
  const [visibleRange, setVisibleRange] = useState<DashboardRangeRef | null>(initialSnapshot ? { from: range.from, to: range.to } : null);
  const [lastSuccessfulRange, setLastSuccessfulRange] = useState<DashboardRangeRef | null>(initialSnapshot ? { from: range.from, to: range.to } : null);
  const abortRef = useRef<AbortController | null>(null);
  const retryTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const fetchGenerationRef = useRef(0);

  const clearPendingRetry = useCallback(() => {
    if (retryTimeoutRef.current !== null) {
      clearTimeout(retryTimeoutRef.current);
      retryTimeoutRef.current = null;
    }
  }, []);

  const doFetch = useCallback(async () => {
    if (!ready) return;
    clearPendingRetry();
    abortRef.current?.abort();
    const fetchGeneration = ++fetchGenerationRef.current;
    setLoading(true);
    setError(null);

    const runAttempt = async (attemptIndex: number): Promise<void> => {
      const controller = new AbortController();
      abortRef.current = controller;

      try {
        const payload = await fetchData(range.from, range.to, controller.signal);
        if (controller.signal.aborted || fetchGenerationRef.current !== fetchGeneration) return;

        clearPendingRetry();
        setAppData(payload.data);
        setDashboardMeta(payload.meta);
        setVisibleRange({ from: range.from, to: range.to });
        setLastSuccessfulRange({ from: range.from, to: range.to });
        setHasLiveData(true);
        saveSnapshot(range.from, range.to, payload.data, payload.meta);
        setLoading(false);
      } catch (err: any) {
        if (err?.name === 'AbortError') return;
        console.error('[DataContext] fetch failed:', err);
        if (controller.signal.aborted || fetchGenerationRef.current !== fetchGeneration) return;

        const errorMessage = err?.message ?? 'Failed to load data';
        const nextDelay = getDefaultDashboardWarmRetryDelay(attemptIndex);
        const shouldRetry = nextDelay !== null && shouldRetryDefaultDashboardWarm({
          range,
          trustedEndDate,
          hasLiveData,
          lastSuccessfulRange,
          errorMessage,
        });

        if (shouldRetry) {
          retryTimeoutRef.current = setTimeout(() => {
            if (fetchGenerationRef.current !== fetchGeneration) return;
            void runAttempt(attemptIndex + 1);
          }, nextDelay);
          return;
        }

        setError(errorMessage);
        setLoading(false);
        // Keep stale data visible — don't clear appData
      }
    };

    await runAttempt(0);
  }, [clearPendingRetry, hasLiveData, lastSuccessfulRange, range, ready, trustedEndDate]);

  useEffect(() => {
    const snapshot = loadSnapshot(range.from, range.to);
    setError(null);
    if (snapshot) {
      setAppData(snapshot.data);
      setDashboardMeta(snapshot.meta);
      setVisibleRange({ from: range.from, to: range.to });
      setLastSuccessfulRange({ from: range.from, to: range.to });
      setHasLiveData(true);
    }
  }, [range.from, range.to]);

  // Initial fetch on mount
  useEffect(() => {
    if (!ready) return undefined;
    doFetch();
    return () => {
      clearPendingRetry();
      abortRef.current?.abort();
    };
  }, [clearPendingRetry, doFetch, ready]);

  const selectedRange: DashboardRangeRef = { from: range.from, to: range.to };
  const isStaleForSelection = hasLiveData && (
    error !== null
    || Boolean(dashboardMeta?.isStale)
    || !sameRange(visibleRange, selectedRange)
  );

  const value: DataContextValue = {
    data: appData,
    loading: ready ? loading : true,
    isRefreshing: ready && loading && hasLiveData,
    hasLiveData,
    isStaleForSelection,
    error,
    dashboardMeta,
    selectedRange,
    visibleRange,
    lastSuccessfulRange,
    refresh: doFetch,
  };

  return (
    <DataContext.Provider value={value}>
      {children}
    </DataContext.Provider>
  );
}

/**
 * useData — hook to access centralised app data.
 * Must be used inside <DataProvider>. Returns an empty-safe fallback otherwise.
 */
export function useData() {
  return useContext(DataContext);
}
