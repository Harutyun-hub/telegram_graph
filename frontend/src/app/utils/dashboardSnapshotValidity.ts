import type { AppData } from '../types/data';

interface DashboardSnapshotMetaLike {
  cacheStatus?: string | null;
}

export function isPlaceholderDashboardSnapshot(
  data: AppData | null | undefined,
  meta: DashboardSnapshotMetaLike | null | undefined,
): boolean {
  if (!data) return true;

  const cacheStatus = String(meta?.cacheStatus ?? '').trim().toLowerCase();
  if (cacheStatus === 'emergency_degraded') {
    return true;
  }

  const brief = data.communityBrief;
  const trendingTopicCount = (data.trendingTopics.en?.length ?? 0) + (data.trendingTopics.ru?.length ?? 0);

  return (
    brief.messagesAnalyzed === 0
    && brief.postsAnalyzed24h === 0
    && brief.commentScopesAnalyzed24h === 0
    && brief.updatedMinutesAgo === 5
    && trendingTopicCount === 0
  );
}
