import { createContext, useCallback, useContext, useEffect, useMemo, useState } from 'react';
import type { ReactNode } from 'react';
import {
  buildPresetRange,
  clampCustomRange,
  createDashboardRange,
  formatDateInput,
  parseDateInput,
  type DashboardDatePresetId,
  type DashboardDateRange,
} from '../utils/dashboardDateRange';

interface SocialDateRangeContextValue {
  range: DashboardDateRange;
  ready: boolean;
  trustedEndDate: string;
  freshness: null;
  setPreset: (presetId: Exclude<DashboardDatePresetId, 'custom'>) => void;
  setCustomRange: (from: string, to: string) => void;
}

const STORAGE_KEY = 'radar.social.date-range.v1';

function todayRange() {
  return buildPresetRange('last_15_days', new Date());
}

const fallbackRange = todayRange();
const fallbackTrustedEnd = formatDateInput(new Date());

const SocialDateRangeContext = createContext<SocialDateRangeContextValue>({
  range: fallbackRange,
  ready: true,
  trustedEndDate: fallbackTrustedEnd,
  freshness: null,
  setPreset: () => {},
  setCustomRange: () => {},
});

function parseStoredRange(raw: string | null, trustedEndDate: string): DashboardDateRange | null {
  if (!raw) return null;
  try {
    const parsed = JSON.parse(raw) as Partial<DashboardDateRange>;
    if (!parsed.from || !parsed.to) return null;
    if (parsed.presetId === 'custom') {
      return clampCustomRange(String(parsed.from), String(parsed.to), parseDateInput(trustedEndDate));
    }
    if (parsed.presetId) {
      return buildPresetRange(parsed.presetId, parseDateInput(trustedEndDate));
    }
    return createDashboardRange(
      String(parsed.from),
      String(parsed.to),
      'custom',
    );
  } catch {
    return null;
  }
}

export function SocialDateRangeProvider({ children }: { children: ReactNode }) {
  const [trustedEndDate, setTrustedEndDate] = useState(fallbackTrustedEnd);
  const [range, setRange] = useState<DashboardDateRange>(fallbackRange);
  const [ready, setReady] = useState(false);

  useEffect(() => {
    const today = formatDateInput(new Date());
    const savedRange = parseStoredRange(
      typeof window === 'undefined' ? null : window.localStorage.getItem(STORAGE_KEY),
      today,
    );
    setTrustedEndDate(today);
    setRange(savedRange ?? buildPresetRange('last_15_days', parseDateInput(today)));
    setReady(true);
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

  const value = useMemo<SocialDateRangeContextValue>(() => ({
    range,
    ready,
    trustedEndDate,
    freshness: null,
    setPreset,
    setCustomRange,
  }), [range, ready, trustedEndDate, setPreset, setCustomRange]);

  return (
    <SocialDateRangeContext.Provider value={value}>
      {children}
    </SocialDateRangeContext.Provider>
  );
}

export function useSocialDateRange() {
  return useContext(SocialDateRangeContext);
}
