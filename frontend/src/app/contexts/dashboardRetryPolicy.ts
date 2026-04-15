import type { DashboardDateRange } from '../utils/dashboardDateRange';

export interface DashboardRangeRef {
  from: string;
  to: string;
}

export interface DashboardRetryPolicyInput {
  range: DashboardDateRange;
  trustedEndDate: string | null | undefined;
  hasLiveData: boolean;
  lastSuccessfulRange: DashboardRangeRef | null;
  errorMessage: string | null | undefined;
}

export const DEFAULT_DASHBOARD_WARM_RETRY_DELAYS_MS = [1000, 2000, 4000] as const;

function getApiErrorStatus(errorMessage: string | null | undefined): number | null {
  const text = String(errorMessage ?? '');
  const match = text.match(/\bAPI\s+(\d{3})\b/i);
  if (!match) return null;
  const status = Number(match[1]);
  return Number.isFinite(status) ? status : null;
}

function isKnownWarmingMessage(errorMessage: string | null | undefined): boolean {
  const text = String(errorMessage ?? '').toLowerCase();
  return text.includes('warming this date range');
}

function sameRange(a: DashboardRangeRef | null, b: DashboardRangeRef | null): boolean {
  return Boolean(a && b && a.from === b.from && a.to === b.to);
}

export function shouldRetryDefaultDashboardWarm(input: DashboardRetryPolicyInput): boolean {
  const {
    range,
    trustedEndDate,
    hasLiveData,
    lastSuccessfulRange,
    errorMessage,
  } = input;

  const selectedRange: DashboardRangeRef = { from: range.from, to: range.to };
  return (
    range.presetId === 'last_15_days'
    && Boolean(trustedEndDate)
    && range.to === trustedEndDate
    && hasLiveData === false
    && !sameRange(lastSuccessfulRange, selectedRange)
    && getApiErrorStatus(errorMessage) === 503
    && isKnownWarmingMessage(errorMessage)
  );
}

export function getDefaultDashboardWarmRetryDelay(attemptIndex: number): number | null {
  return DEFAULT_DASHBOARD_WARM_RETRY_DELAYS_MS[attemptIndex] ?? null;
}
