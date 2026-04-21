import { describe, expect, it } from 'vitest';

import { createEmptyAppData } from '../services/dashboardAdapter';
import { isPlaceholderDashboardSnapshot } from './dashboardSnapshotValidity';

describe('dashboardSnapshotValidity', () => {
  it('flags the synthetic empty fallback snapshot shape as placeholder data', () => {
    const data = createEmptyAppData();
    data.communityBrief.updatedMinutesAgo = 5;

    expect(isPlaceholderDashboardSnapshot(data, { cacheStatus: 'refresh_success' })).toBe(true);
  });

  it('flags emergency degraded snapshots even when passed through the adapter', () => {
    const data = createEmptyAppData();
    data.communityBrief.updatedMinutesAgo = 42;

    expect(isPlaceholderDashboardSnapshot(data, { cacheStatus: 'emergency_degraded' })).toBe(true);
  });

  it('accepts real dashboard snapshots with analyzed volume', () => {
    const data = createEmptyAppData();
    data.communityBrief.messagesAnalyzed = 3306;
    data.communityBrief.postsAnalyzed24h = 1611;
    data.communityBrief.commentScopesAnalyzed24h = 1695;
    data.communityBrief.updatedMinutesAgo = 8393;
    data.trendingTopics.en = [{ id: 1, topic: 'Media', sourceTopic: 'Media', mentions: 190, deltaMentions: 104, trend: 54, trendReliable: true, growthSupport: 40, category: 'Media', sentiment: 'curious', sampleQuote: '', sampleEvidenceId: '', evidence: [], evidenceCount: 0, distinctUsers: 0, distinctChannels: 0, distinctPosts: 0, distinctComments: 0, qualityTier: 'high' }];

    expect(isPlaceholderDashboardSnapshot(data, { cacheStatus: 'memory_fresh' })).toBe(false);
  });
});
