import type { AIClientFilters } from '@/app/graph/contracts';
import type { GraphFilters, SourceDetail } from '@/app/graph/services/types';

const CONNECTION_STRENGTH_TO_MIN_MENTIONS: Record<number, number> = {
  1: 1,
  2: 2,
  3: 3,
  4: 5,
  5: 8,
};

function normalizeList(values?: string[]): string[] | undefined {
  if (!Array.isArray(values)) return undefined;
  const normalized = Array.from(
    new Set(
      values
        .map((value) => String(value || '').trim())
        .filter(Boolean),
    ),
  );
  return normalized.length > 0 ? normalized : [];
}

function mapSourceDetailToSourceProfile(sourceDetail?: SourceDetail): AIClientFilters['sourceProfile'] {
  switch (sourceDetail) {
    case 'minimal':
      return 'brandStrategy';
    case 'expanded':
      return 'performance';
    default:
      return 'balanced';
  }
}

function mapSourceProfileToSourceDetail(
  sourceProfile?: string,
  fallback: SourceDetail = 'standard',
): SourceDetail {
  switch ((sourceProfile || '').trim().toLowerCase()) {
    case 'brandstrategy':
      return 'minimal';
    case 'performance':
      return 'expanded';
    case 'balanced':
      return 'standard';
    default:
      return fallback;
  }
}

function mapMinMentionsToConnectionStrength(minMentions?: number): number {
  const safeValue = Math.max(1, Number(minMentions || 2));
  if (safeValue >= 8) return 5;
  if (safeValue >= 5) return 4;
  if (safeValue >= 3) return 3;
  if (safeValue >= 2) return 2;
  return 1;
}

function mapConnectionStrengthToMinMentions(connectionStrength?: number): number | undefined {
  const safeValue = Math.max(1, Math.min(5, Math.round(Number(connectionStrength || 0))));
  return CONNECTION_STRENGTH_TO_MIN_MENTIONS[safeValue];
}

export function graphFiltersToAIClientFilters(filters: GraphFilters): AIClientFilters {
  return {
    channels: normalizeList(filters.channels),
    sentiments: normalizeList(filters.sentiments),
    topics: normalizeList(filters.topics),
    layers: ['topic'],
    insightMode: 'marketMap',
    sourceProfile: mapSourceDetailToSourceProfile(filters.sourceDetail),
    connectionStrength: mapMinMentionsToConnectionStrength(filters.minMentions),
    confidenceThreshold: 35,
  };
}

export function mergeAIClientFiltersIntoGraphFilters(
  base: GraphFilters,
  patch: AIClientFilters,
): GraphFilters {
  const next: GraphFilters = {
    ...base,
  };

  if (Array.isArray(patch.channels)) {
    next.channels = normalizeList(patch.channels) || [];
  }

  if (Array.isArray(patch.sentiments)) {
    next.sentiments = normalizeList(patch.sentiments) || [];
  }

  if (Array.isArray(patch.topics)) {
    next.topics = normalizeList(patch.topics) || [];
  }

  const mappedMinMentions = mapConnectionStrengthToMinMentions(patch.connectionStrength);
  if (mappedMinMentions != null) {
    next.minMentions = mappedMinMentions;
  }

  if (patch.sourceProfile) {
    next.sourceDetail = mapSourceProfileToSourceDetail(
      patch.sourceProfile,
      base.sourceDetail || 'standard',
    );
  }

  return next;
}
