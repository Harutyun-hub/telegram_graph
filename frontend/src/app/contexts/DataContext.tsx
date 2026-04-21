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
import { resolveDisplayedDashboardRange, sameDashboardRange, type DashboardRangeRef } from '../utils/dashboardDisplayRange';
import { isPlaceholderDashboardSnapshot } from '../utils/dashboardSnapshotValidity';
import type { DashboardDateRange } from '../utils/dashboardDateRange';

interface DashboardMeta {
  trustedEndDate?: string;
  freshnessStatus?: string;
  rangeLabel?: string;
  requestedFrom?: string;
  requestedTo?: string;
  degradedTiers?: string[];
  suppressedDegradedTiers?: string[];
  tierTimes?: Record<string, number | null>;
  snapshotBuiltAt?: string;
  cacheStatus?: string;
  isStale?: boolean;
  responseBytes?: number;
  responseSerializeMs?: number;
}

// Keep the browser timeout above the backend cold-start budget so the client
// does not abort a request that the server is still about to satisfy.
const DASHBOARD_TIMEOUT_MS = 45_000;

interface DataContextValue {
  data: AppData;
  loading: boolean;
  isRefreshing: boolean;
  hasLiveData: boolean;
  isStaleForSelection: boolean;
  error: string | null;
  dashboardMeta: DashboardMeta | null;
  displayRange: DashboardDateRange | null;
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
  displayRange: null,
  selectedRange: null,
  visibleRange: null,
  lastSuccessfulRange: null,
  refresh: () => {},
});

export function snapshotKeyForRange(from: string, to: string): string {
  return `radar.dashboard.snapshot.v6:${from}:${to}`;
}

export function isSnapshotMetaForRequestedRange(
  meta: DashboardMeta | null | undefined,
  from: string,
  to: string,
): boolean {
  const requestedFrom = String(meta?.requestedFrom ?? '').trim();
  const requestedTo = String(meta?.requestedTo ?? '').trim();
  if (!requestedFrom || !requestedTo) {
    return true;
  }
  return requestedFrom === from && requestedTo === to;
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
    if (hasJobDataWithoutEvidence) return null;
    if (!isSnapshotMetaForRequestedRange(snapshot.meta, from, to)) return null;
    return isPlaceholderDashboardSnapshot(snapshot.data, snapshot.meta) ? null : snapshot;
  } catch {
    return null;
  }
}

function saveSnapshot(from: string, to: string, data: AppData, meta: DashboardMeta | null): void {
  if (typeof window === 'undefined') return;
  if (isPlaceholderDashboardSnapshot(data, meta)) return;
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
      tierTimes: payload?.meta?.tierTimes && typeof payload.meta.tierTimes === 'object' ? payload.meta.tierTimes : {},
      snapshotBuiltAt: payload?.meta?.snapshotBuiltAt,
      cacheStatus: payload?.meta?.cacheStatus,
      isStale: Boolean(payload?.meta?.isStale),
      responseBytes: Number.isFinite(Number(payload?.meta?.responseBytes)) ? Number(payload.meta.responseBytes) : undefined,
      responseSerializeMs: Number.isFinite(Number(payload?.meta?.responseSerializeMs)) ? Number(payload.meta.responseSerializeMs) : undefined,
    },
  };
}

export function DataProvider({ children }: { children: ReactNode }) {
  const { range, ready } = useDashboardDateRange();
  const initialSnapshot = loadSnapshot(range.from, range.to);
  const [appData, setAppData] = useState<AppData>(initialSnapshot?.data ?? createEmptyAppData());
  const [hasLiveData, setHasLiveData] = useState(Boolean(initialSnapshot));
  const [loading, setLoading] = useState(!initialSnapshot || !ready);
  const [error, setError] = useState<string | null>(null);
  const [dashboardMeta, setDashboardMeta] = useState<DashboardMeta | null>(initialSnapshot?.meta ?? null);
  const [visibleRange, setVisibleRange] = useState<DashboardRangeRef | null>(initialSnapshot ? { from: range.from, to: range.to } : null);
  const [lastSuccessfulRange, setLastSuccessfulRange] = useState<DashboardRangeRef | null>(initialSnapshot ? { from: range.from, to: range.to } : null);
  const abortRef = useRef<AbortController | null>(null);

  const doFetch = useCallback(async () => {
    if (!ready) return;
    // Cancel any in-flight request
    abortRef.current?.abort();
    const controller = new AbortController();
    abortRef.current = controller;

    setLoading(true);
    setError(null);

    try {
      const payload = await fetchData(range.from, range.to, controller.signal);
      // Only update if this request wasn't aborted
      if (!controller.signal.aborted) {
        setAppData(payload.data);
        setDashboardMeta(payload.meta);
        setVisibleRange({ from: range.from, to: range.to });
        setLastSuccessfulRange({ from: range.from, to: range.to });
        setHasLiveData(true);
        saveSnapshot(range.from, range.to, payload.data, payload.meta);
        setLoading(false);
      }
    } catch (err: any) {
      if (err?.name === 'AbortError') return; // Cancelled — ignore
      console.error('[DataContext] fetch failed:', err);
      if (!controller.signal.aborted) {
        setError(err?.message ?? 'Failed to load data');
        setLoading(false);
        // Keep stale data visible — don't clear appData
      }
    }
  }, [range.from, range.to, ready]);

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
    return () => { abortRef.current?.abort(); };
  }, [doFetch, ready]);

  const selectedRange: DashboardRangeRef = { from: range.from, to: range.to };
  const displayRange = resolveDisplayedDashboardRange(range, visibleRange);
  const isStaleForSelection = hasLiveData && (
    error !== null
    || Boolean(dashboardMeta?.isStale)
    || !sameDashboardRange(visibleRange, selectedRange)
  );

  const value: DataContextValue = {
    data: appData,
    loading: ready ? loading : true,
    isRefreshing: ready && loading && hasLiveData,
    hasLiveData,
    isStaleForSelection,
    error,
    dashboardMeta,
    displayRange,
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
