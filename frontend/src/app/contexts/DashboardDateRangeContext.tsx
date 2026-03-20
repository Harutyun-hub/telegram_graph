import { createContext, useCallback, useContext, useEffect, useMemo, useState } from 'react';
import type { ReactNode } from 'react';
import { apiFetch } from '../services/api';
import {
  addDays,
  buildPresetRange,
  clampCustomRange,
  createDashboardRange,
  formatDateInput,
  parseDateInput,
  shiftRangeToTrustedEnd,
  type DashboardDatePresetId,
  type DashboardDateRange,
} from '../utils/dashboardDateRange';

interface FreshnessSummary {
  healthStatus: string;
  trustedEndDate: string;
  trustedEndLabel: string;
  generatedAt?: string;
}

interface DashboardDateRangeContextValue {
  range: DashboardDateRange;
  ready: boolean;
  trustedEndDate: string;
  freshness: FreshnessSummary | null;
  setPreset: (presetId: Exclude<DashboardDatePresetId, 'custom'>) => void;
  setCustomRange: (from: string, to: string) => void;
}

const STORAGE_KEY = 'radar.dashboard.date-range.v1';

const fallbackTrustedEnd = addDays(new Date(), -1);
const fallbackRange = buildPresetRange('last_15_days', fallbackTrustedEnd);

const DashboardDateRangeContext = createContext<DashboardDateRangeContextValue>({
  range: fallbackRange,
  ready: false,
  trustedEndDate: formatDateInput(fallbackTrustedEnd),
  freshness: null,
  setPreset: () => {},
  setCustomRange: () => {},
});

function parseStoredRange(raw: string | null): DashboardDateRange | null {
  if (!raw) return null;
  try {
    const parsed = JSON.parse(raw) as Partial<DashboardDateRange>;
    if (!parsed.from || !parsed.to) return null;
    return createDashboardRange(
      String(parsed.from),
      String(parsed.to),
      parsed.presetId === 'custom' ? 'custom' : (parsed.presetId || 'custom'),
    );
  } catch {
    return null;
  }
}

function trustedEndFromFreshness(snapshot: any): { trustedEnd: Date; summary: FreshnessSummary | null } {
  const now = new Date();
  const syncLast = String(snapshot?.pipeline?.sync?.last_graph_sync_at || snapshot?.drift?.neo4j_last_post_at || snapshot?.drift?.supabase_last_post_at || snapshot?.generated_at || '').trim();
  const syncStatus = String(snapshot?.pipeline?.sync?.status || 'unknown').toLowerCase();
  const processStatus = String(snapshot?.pipeline?.process?.status || 'unknown').toLowerCase();
  const syncAgeMinutes = Number(snapshot?.pipeline?.sync?.age_minutes ?? Number.NaN);
  const processAgeMinutes = Number(snapshot?.pipeline?.process?.age_minutes ?? Number.NaN);

  let trustedEnd = fallbackTrustedEnd;
  if (syncLast) {
    const candidate = parseDateInput(formatDateInput(new Date(syncLast)));
    trustedEnd = candidate;
  }

  const today = formatDateInput(now);
  const candidateDay = formatDateInput(trustedEnd);
  const needsPreviousDayAnchor =
    candidateDay === today &&
    (
      syncStatus !== 'healthy' ||
      processStatus === 'warning' ||
      processStatus === 'stale' ||
      (Number.isFinite(syncAgeMinutes) && syncAgeMinutes > 180) ||
      (Number.isFinite(processAgeMinutes) && processAgeMinutes > 180)
    );

  if (needsPreviousDayAnchor) {
    trustedEnd = addDays(trustedEnd, -1);
  }

  const trustedEndDate = formatDateInput(trustedEnd);
  return {
    trustedEnd,
    summary: {
      healthStatus: String(snapshot?.health?.status || 'unknown'),
      trustedEndDate,
      trustedEndLabel: trustedEndDate === today ? 'Data through today' : `Data through ${trustedEndDate}`,
      generatedAt: snapshot?.generated_at,
    },
  };
}

export function DashboardDateRangeProvider({ children }: { children: ReactNode }) {
  const [ready, setReady] = useState(false);
  const [trustedEndDate, setTrustedEndDate] = useState(formatDateInput(fallbackTrustedEnd));
  const [freshness, setFreshness] = useState<FreshnessSummary | null>(null);
  const [range, setRange] = useState<DashboardDateRange>(fallbackRange);

  useEffect(() => {
    let cancelled = false;

    async function initialize() {
      const savedRange = parseStoredRange(typeof window === 'undefined' ? null : window.localStorage.getItem(STORAGE_KEY));
      try {
        const snapshot = await apiFetch<any>('/freshness');
        if (cancelled) return;
        const { trustedEnd, summary } = trustedEndFromFreshness(snapshot);
        const trustedEndInput = formatDateInput(trustedEnd);
        const nextRange = savedRange
          ? shiftRangeToTrustedEnd(savedRange, trustedEnd)
          : buildPresetRange('last_15_days', trustedEnd);
        setTrustedEndDate(trustedEndInput);
        setFreshness(summary);
        setRange(nextRange);
      } catch {
        if (cancelled) return;
        const nextRange = savedRange
          ? shiftRangeToTrustedEnd(savedRange, fallbackTrustedEnd)
          : fallbackRange;
        setTrustedEndDate(formatDateInput(fallbackTrustedEnd));
        setRange(nextRange);
      } finally {
        if (!cancelled) setReady(true);
      }
    }

    void initialize();
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (!ready || typeof window === 'undefined') return;
    window.localStorage.setItem(STORAGE_KEY, JSON.stringify(range));
  }, [range, ready]);

  const setPreset = useCallback((presetId: Exclude<DashboardDatePresetId, 'custom'>) => {
    setRange(buildPresetRange(presetId, parseDateInput(trustedEndDate)));
  }, [trustedEndDate]);

  const setCustomRange = useCallback((from: string, to: string) => {
    setRange(clampCustomRange(from, to, parseDateInput(trustedEndDate)));
  }, [trustedEndDate]);

  const value = useMemo<DashboardDateRangeContextValue>(() => ({
    range,
    ready,
    trustedEndDate,
    freshness,
    setPreset,
    setCustomRange,
  }), [freshness, range, ready, setCustomRange, setPreset, trustedEndDate]);

  return (
    <DashboardDateRangeContext.Provider value={value}>
      {children}
    </DashboardDateRangeContext.Provider>
  );
}

export function useDashboardDateRange() {
  return useContext(DashboardDateRangeContext);
}
