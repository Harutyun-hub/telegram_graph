export type DashboardDatePresetId =
  | 'today'
  | 'yesterday'
  | 'last_3_days'
  | 'last_7_days'
  | 'last_15_days'
  | 'last_30_days'
  | 'last_3_months'
  | 'last_6_months'
  | 'custom';

export type DashboardMode = 'operational' | 'intelligence';

export interface DashboardDateRange {
  from: string;
  to: string;
  days: number;
  mode: DashboardMode;
  presetId: DashboardDatePresetId;
}

const DAY_MS = 24 * 60 * 60 * 1000;

function atLocalNoon(date: Date): Date {
  return new Date(date.getFullYear(), date.getMonth(), date.getDate(), 12, 0, 0, 0);
}

export function formatDateInput(date: Date): string {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
}

export function parseDateInput(value: string): Date {
  const [year, month, day] = value.split('-').map((part) => Number(part));
  return atLocalNoon(new Date(year, Math.max(0, month - 1), Math.max(1, day)));
}

export function addDays(date: Date, delta: number): Date {
  return atLocalNoon(new Date(atLocalNoon(date).getTime() + (delta * DAY_MS)));
}

export function compareDateInputs(a: string, b: string): number {
  if (a === b) return 0;
  return a < b ? -1 : 1;
}

export function differenceInDaysInclusive(from: string, to: string): number {
  const fromDate = parseDateInput(from).getTime();
  const toDate = parseDateInput(to).getTime();
  return Math.max(1, Math.round((toDate - fromDate) / DAY_MS) + 1);
}

export function resolveDashboardMode(days: number): DashboardMode {
  return days >= 15 ? 'intelligence' : 'operational';
}

export function createDashboardRange(
  from: string,
  to: string,
  presetId: DashboardDatePresetId,
): DashboardDateRange {
  const normalizedFrom = compareDateInputs(from, to) <= 0 ? from : to;
  const normalizedTo = compareDateInputs(from, to) <= 0 ? to : from;
  const days = differenceInDaysInclusive(normalizedFrom, normalizedTo);
  return {
    from: normalizedFrom,
    to: normalizedTo,
    days,
    mode: resolveDashboardMode(days),
    presetId,
  };
}

export function buildPresetRange(presetId: Exclude<DashboardDatePresetId, 'custom'>, endDate: Date): DashboardDateRange {
  const end = formatDateInput(atLocalNoon(endDate));
  if (presetId === 'today') {
    return createDashboardRange(end, end, presetId);
  }
  if (presetId === 'yesterday') {
    const yesterday = formatDateInput(addDays(endDate, -1));
    return createDashboardRange(yesterday, yesterday, presetId);
  }

  const days = presetId === 'last_3_days'
    ? 3
    : presetId === 'last_7_days'
      ? 7
      : presetId === 'last_15_days'
        ? 15
        : presetId === 'last_30_days'
          ? 30
          : presetId === 'last_3_months'
            ? 90
            : 180;
  const from = formatDateInput(addDays(endDate, -(days - 1)));
  return createDashboardRange(from, end, presetId);
}

export function shiftRangeToTrustedEnd(range: DashboardDateRange, trustedEnd: Date): DashboardDateRange {
  const currentTo = parseDateInput(range.to);
  const normalizedTrustedEnd = atLocalNoon(trustedEnd);
  if (currentTo.getTime() <= normalizedTrustedEnd.getTime()) {
    return range;
  }
  const deltaDays = Math.round((currentTo.getTime() - normalizedTrustedEnd.getTime()) / DAY_MS);
  const shiftedFrom = formatDateInput(addDays(parseDateInput(range.from), -deltaDays));
  const shiftedTo = formatDateInput(normalizedTrustedEnd);
  return createDashboardRange(shiftedFrom, shiftedTo, range.presetId);
}

export function clampCustomRange(from: string, to: string, trustedEnd: Date): DashboardDateRange {
  const trustedEndInput = formatDateInput(trustedEnd);
  const safeTo = compareDateInputs(to, trustedEndInput) <= 0 ? to : trustedEndInput;
  const safeFrom = compareDateInputs(from, safeTo) <= 0 ? from : safeTo;
  return createDashboardRange(safeFrom, safeTo, 'custom');
}
