import { describe, expect, it } from 'vitest';

import { createDashboardRange } from './dashboardDateRange';
import { resolveDisplayedDashboardRange, sameDashboardRange } from './dashboardDisplayRange';

describe('dashboardDisplayRange', () => {
  it('uses the selected range when no visible snapshot is loaded yet', () => {
    const selectedRange = createDashboardRange('2026-04-01', '2026-04-15', 'last_15_days');

    expect(resolveDisplayedDashboardRange(selectedRange, null)).toEqual(selectedRange);
  });

  it('keeps the selected range when the visible snapshot already matches it', () => {
    const selectedRange = createDashboardRange('2026-01-16', '2026-04-15', 'last_3_months');

    expect(resolveDisplayedDashboardRange(selectedRange, {
      from: '2026-01-16',
      to: '2026-04-15',
    })).toEqual(selectedRange);
  });

  it('uses the visible snapshot window while a different range is still loading', () => {
    const selectedRange = createDashboardRange('2026-01-16', '2026-04-15', 'last_3_months');

    const displayedRange = resolveDisplayedDashboardRange(selectedRange, {
      from: '2026-04-01',
      to: '2026-04-15',
    });

    expect(displayedRange.from).toBe('2026-04-01');
    expect(displayedRange.to).toBe('2026-04-15');
    expect(displayedRange.days).toBe(15);
    expect(displayedRange.presetId).toBe('custom');
  });

  it('correctly compares range references by from/to values', () => {
    expect(sameDashboardRange(
      { from: '2026-04-01', to: '2026-04-15' },
      { from: '2026-04-01', to: '2026-04-15' },
    )).toBe(true);
    expect(sameDashboardRange(
      { from: '2026-04-01', to: '2026-04-15' },
      { from: '2026-01-16', to: '2026-04-15' },
    )).toBe(false);
  });
});
