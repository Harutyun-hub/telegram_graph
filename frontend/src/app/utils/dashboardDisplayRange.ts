import { createDashboardRange, type DashboardDateRange } from './dashboardDateRange';

export interface DashboardRangeRef {
  from: string;
  to: string;
}

export function sameDashboardRange(a: DashboardRangeRef | null, b: DashboardRangeRef | null): boolean {
  return Boolean(a && b && a.from === b.from && a.to === b.to);
}

export function resolveDisplayedDashboardRange(
  selectedRange: DashboardDateRange,
  visibleRange: DashboardRangeRef | null,
): DashboardDateRange {
  if (!visibleRange || sameDashboardRange(visibleRange, selectedRange)) {
    return selectedRange;
  }
  return createDashboardRange(visibleRange.from, visibleRange.to, 'custom');
}
