// ================================================================
// DATA CONTEXT — Production-ready single data entry point
// ================================================================
// Default mode fetches live dashboard data from backend /api/dashboard.
// If request fails, stale data is preserved and safe mock defaults remain.
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
import { mockAppData } from '../data/mockData';
import { adaptDashboardPayload } from '../services/dashboardAdapter';
import { apiFetch } from '../services/api';

interface DataContextValue {
  data: AppData;
  loading: boolean;
  hasLiveData: boolean;
  error: string | null;
  /** Call this to manually refresh data from the API */
  refresh: () => void;
}

// Provides safe defaults so useData() always returns a usable object,
// even if called outside of DataProvider (e.g., during testing).
const DataContext = createContext<DataContextValue>({
  data: mockAppData,
  loading: false,
  hasLiveData: false,
  error: null,
  refresh: () => {},
});

const SNAPSHOT_KEY = 'radar.dashboard.snapshot.v2';

function loadSnapshot(): AppData | null {
  if (typeof window === 'undefined') return null;
  try {
    const raw = window.sessionStorage.getItem(SNAPSHOT_KEY);
    if (!raw) return null;
    return JSON.parse(raw) as AppData;
  } catch {
    return null;
  }
}

function saveSnapshot(data: AppData): void {
  if (typeof window === 'undefined') return;
  try {
    window.sessionStorage.setItem(SNAPSHOT_KEY, JSON.stringify(data));
  } catch {
    // ignore storage errors
  }
}

// ── Data fetching logic ───────────────────────────────────────
// Live backend mode: fetch dashboard payload and normalize with adapter.
async function fetchData(signal?: AbortSignal): Promise<AppData> {
  const payload = await apiFetch<any>('/dashboard', {
    method: 'GET',
    signal,
    headers: { Accept: 'application/json' },
    cache: 'no-store',
  });
  return adaptDashboardPayload(payload);
}

export function DataProvider({ children }: { children: ReactNode }) {
  const initialSnapshot = loadSnapshot();
  const [appData, setAppData] = useState<AppData>(initialSnapshot ?? mockAppData);
  const [hasLiveData, setHasLiveData] = useState(Boolean(initialSnapshot));
  const [loading, setLoading] = useState(!initialSnapshot);
  const [error, setError] = useState<string | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  const doFetch = useCallback(async () => {
    // Cancel any in-flight request
    abortRef.current?.abort();
    const controller = new AbortController();
    abortRef.current = controller;

    setLoading(true);
    setError(null);

    try {
      const data = await fetchData(controller.signal);
      // Only update if this request wasn't aborted
      if (!controller.signal.aborted) {
        setAppData(data);
        setHasLiveData(true);
        saveSnapshot(data);
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
  }, []);

  // Initial fetch on mount
  useEffect(() => {
    doFetch();
    return () => { abortRef.current?.abort(); };
  }, [doFetch]);

  const value: DataContextValue = {
    data: appData,
    loading,
    hasLiveData,
    error,
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
 * Must be used inside <DataProvider>. Returns mockAppData as a safe fallback otherwise.
 */
export function useData() {
  return useContext(DataContext);
}
