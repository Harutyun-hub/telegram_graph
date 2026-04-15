import { describe, expect, it } from 'vitest';

import {
  DEFAULT_DASHBOARD_WARM_RETRY_DELAYS_MS,
  getDefaultDashboardWarmRetryDelay,
  shouldRetryDefaultDashboardWarm,
} from './dashboardRetryPolicy';

describe('dashboardRetryPolicy', () => {
  const defaultRange = {
    from: '2026-04-01',
    to: '2026-04-15',
    days: 15,
    mode: 'intelligence' as const,
    presetId: 'last_15_days' as const,
  };

  it('retries only for the canonical default range warming miss with no live data', () => {
    expect(shouldRetryDefaultDashboardWarm({
      range: defaultRange,
      trustedEndDate: '2026-04-15',
      hasLiveData: false,
      lastSuccessfulRange: null,
      errorMessage: 'API 503: We’re still warming this date range. Please try again shortly.',
    })).toBe(true);
  });

  it('does not retry custom ranges', () => {
    expect(shouldRetryDefaultDashboardWarm({
      range: { ...defaultRange, presetId: 'custom', from: '2026-03-20', to: '2026-04-03' },
      trustedEndDate: '2026-04-15',
      hasLiveData: false,
      lastSuccessfulRange: null,
      errorMessage: 'API 503: We’re still warming this date range. Please try again shortly.',
    })).toBe(false);
  });

  it('does not retry when live data already exists', () => {
    expect(shouldRetryDefaultDashboardWarm({
      range: defaultRange,
      trustedEndDate: '2026-04-15',
      hasLiveData: true,
      lastSuccessfulRange: null,
      errorMessage: 'API 503: We’re still warming this date range. Please try again shortly.',
    })).toBe(false);
  });

  it('does not retry non-503 or non-warming errors', () => {
    expect(shouldRetryDefaultDashboardWarm({
      range: defaultRange,
      trustedEndDate: '2026-04-15',
      hasLiveData: false,
      lastSuccessfulRange: null,
      errorMessage: 'API 500: Internal server error',
    })).toBe(false);

    expect(shouldRetryDefaultDashboardWarm({
      range: defaultRange,
      trustedEndDate: '2026-04-15',
      hasLiveData: false,
      lastSuccessfulRange: null,
      errorMessage: 'API 403: Admin access required',
    })).toBe(false);
  });

  it('stops returning delays after the fixed retry window', () => {
    expect(DEFAULT_DASHBOARD_WARM_RETRY_DELAYS_MS).toEqual([1000, 2000, 4000]);
    expect(getDefaultDashboardWarmRetryDelay(0)).toBe(1000);
    expect(getDefaultDashboardWarmRetryDelay(1)).toBe(2000);
    expect(getDefaultDashboardWarmRetryDelay(2)).toBe(4000);
    expect(getDefaultDashboardWarmRetryDelay(3)).toBeNull();
  });
});
