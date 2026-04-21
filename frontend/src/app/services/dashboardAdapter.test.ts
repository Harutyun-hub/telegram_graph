import { describe, expect, it } from 'vitest';

import { adaptDashboardPayload } from './dashboardAdapter';

describe('dashboardAdapter exact-range normalization', () => {
  it('derives the selected window size from requestedFrom/requestedTo when meta.days is absent', () => {
    const app = adaptDashboardPayload({
      meta: {
        requestedFrom: '2026-04-13',
        requestedTo: '2026-04-15',
      },
      data: {
        communityBrief: {
          postsAnalyzed24h: 2246,
          commentScopesAnalyzed24h: 2543,
          totalAnalyses24h: 4789,
          topTopicRows: [{ name: 'Media And News' }],
        },
      },
    });

    expect(app.communityBrief.mainBrief.en).toContain('(3d)');
    expect(app.communityBrief.mainBrief.en).toContain('Media And News');
  });

  it('uses selected-window community brief fields when they are present', () => {
    const app = adaptDashboardPayload({
      meta: {
        days: 15,
      },
      data: {
        communityBrief: {
          postsAnalyzed24h: 10,
          commentScopesAnalyzed24h: 20,
          totalAnalyses24h: 30,
          postsAnalyzedInWindow: 100,
          commentScopesAnalyzedInWindow: 200,
          totalAnalysesInWindow: 300,
          positiveIntentPct24h: 45,
          negativeIntentPct24h: 25,
        },
      },
    });

    expect(app.communityBrief.postsAnalyzed24h).toBe(100);
    expect(app.communityBrief.commentScopesAnalyzed24h).toBe(200);
    expect(app.communityBrief.messagesAnalyzed).toBe(300);
    expect(app.communityBrief.mainBrief.en).toContain('(15d)');
  });

  it('does not fabricate a misleading 5-minute refresh age when the payload does not provide one', () => {
    const app = adaptDashboardPayload({
      meta: {
        requestedFrom: '2026-04-14',
        requestedTo: '2026-04-15',
      },
      data: {
        communityBrief: {
          postsAnalyzed24h: 1611,
          commentScopesAnalyzed24h: 1695,
          totalAnalyses24h: 3306,
        },
      },
    });

    expect(app.communityBrief.updatedMinutesAgo).toBe(0);
  });
});
