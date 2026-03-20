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

interface DataContextValue {
  data: AppData;
  loading: boolean;
  isRefreshing: boolean;
  hasLiveData: boolean;
  error: string | null;
  dashboardMeta: {
    trustedEndDate?: string;
    freshnessStatus?: string;
    rangeLabel?: string;
  } | null;
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
  error: null,
  dashboardMeta: null,
  refresh: () => {},
});

function snapshotKeyForRange(from: string, to: string): string {
  return `radar.dashboard.snapshot.v4:${from}:${to}`;
}

function loadSnapshot(from: string, to: string): AppData | null {
  if (typeof window === 'undefined') return null;
  try {
    const raw = window.sessionStorage.getItem(snapshotKeyForRange(from, to));
    if (!raw) return null;
    const parsed = JSON.parse(raw) as AppData;
    const jobItems = parsed?.jobSeeking?.en ?? [];
    const hasJobDataWithoutEvidence = Array.isArray(jobItems)
      && jobItems.length > 0
      && !jobItems.some((item: any) => Array.isArray(item?.evidence) && item.evidence.length > 0);
    return hasJobDataWithoutEvidence ? null : parsed;
  } catch {
    return null;
  }
}

function saveSnapshot(from: string, to: string, data: AppData): void {
  if (typeof window === 'undefined') return;
  try {
    window.sessionStorage.setItem(snapshotKeyForRange(from, to), JSON.stringify(data));
  } catch {
    // ignore storage errors
  }
}

// ── Data fetching logic ───────────────────────────────────────
// Live backend mode: fetch dashboard payload and normalize with adapter.
async function fetchData(from: string, to: string, signal?: AbortSignal): Promise<{ data: AppData; meta: DataContextValue['dashboardMeta'] }> {
  const params = new URLSearchParams({ from, to });
  const payload = await apiFetch<any>(`/dashboard?${params.toString()}`, {
    method: 'GET',
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
    },
  };
}

export function DataProvider({ children }: { children: ReactNode }) {
  const { range, ready } = useDashboardDateRange();
  const initialSnapshot = loadSnapshot(range.from, range.to);
  const [appData, setAppData] = useState<AppData>(initialSnapshot ?? createEmptyAppData());
  const [hasLiveData, setHasLiveData] = useState(Boolean(initialSnapshot));
  const [loading, setLoading] = useState(!initialSnapshot || !ready);
  const [error, setError] = useState<string | null>(null);
  const [dashboardMeta, setDashboardMeta] = useState<DataContextValue['dashboardMeta']>(null);
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
        setHasLiveData(true);
        saveSnapshot(range.from, range.to, payload.data);
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
    if (snapshot) {
      setAppData(snapshot);
      setHasLiveData(true);
    }
  }, [range.from, range.to]);

  // Initial fetch on mount
  useEffect(() => {
    if (!ready) return undefined;
    doFetch();
    return () => { abortRef.current?.abort(); };
  }, [doFetch, ready]);

  const value: DataContextValue = {
    data: appData,
    loading: ready ? loading : true,
    isRefreshing: ready && loading && hasLiveData,
    hasLiveData,
    error,
    dashboardMeta,
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
