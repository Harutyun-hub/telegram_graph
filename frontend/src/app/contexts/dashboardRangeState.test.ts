import { describe, expect, it } from 'vitest';

import {
  getRangeFromMeta,
  responseMatchesRequestedRange,
  sameRange,
  type DashboardMeta,
} from './dashboardRangeState';

describe('dashboardRangeState', () => {
  it('derives the visible range from backend meta', () => {
    const meta: DashboardMeta = { from: '2026-04-01', to: '2026-04-15' };

    expect(getRangeFromMeta(meta)).toEqual({ from: '2026-04-01', to: '2026-04-15' });
  });

  it('rejects a response when backend meta does not match the requested exact range', () => {
    const meta: DashboardMeta = { from: '2026-04-01', to: '2026-04-15' };

    expect(
      responseMatchesRequestedRange(meta, { from: '2026-04-05', to: '2026-04-15' }),
    ).toBe(false);
  });

  it('treats a response as successful only when the backend meta exactly matches the request', () => {
    const meta: DashboardMeta = { from: '2026-04-05', to: '2026-04-15' };

    expect(
      responseMatchesRequestedRange(meta, { from: '2026-04-05', to: '2026-04-15' }),
    ).toBe(true);
  });

  it('compares null ranges safely', () => {
    expect(sameRange(null, null)).toBe(false);
    expect(sameRange({ from: '2026-04-01', to: '2026-04-15' }, null)).toBe(false);
  });
});

