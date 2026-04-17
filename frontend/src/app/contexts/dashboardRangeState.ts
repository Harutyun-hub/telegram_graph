export interface DashboardRangeRef {
  from: string;
  to: string;
}

export interface DashboardMeta {
  from?: string;
  to?: string;
  trustedEndDate?: string;
  freshnessStatus?: string;
  rangeLabel?: string;
  requestedFrom?: string;
  requestedTo?: string;
  degradedTiers?: string[];
  suppressedDegradedTiers?: string[];
  tierTimes?: Record<string, number | null>;
  snapshotBuiltAt?: string;
  cacheStatus?: string;
  cacheSource?: string;
  isStale?: boolean;
  skippedTiers?: string[];
  responseBytes?: number;
  responseSerializeMs?: number;
  rangeResolutionPath?: string;
  defaultResolutionPath?: string;
}

export function sameRange(a: DashboardRangeRef | null, b: DashboardRangeRef | null): boolean {
  return Boolean(a && b && a.from === b.from && a.to === b.to);
}

export function getRangeFromMeta(meta: DashboardMeta | null | undefined): DashboardRangeRef | null {
  const from = typeof meta?.from === 'string' ? meta.from : '';
  const to = typeof meta?.to === 'string' ? meta.to : '';
  if (!from || !to) return null;
  return { from, to };
}

export function responseMatchesRequestedRange(
  meta: DashboardMeta | null | undefined,
  requestedRange: DashboardRangeRef,
): boolean {
  return sameRange(getRangeFromMeta(meta), requestedRange);
}

