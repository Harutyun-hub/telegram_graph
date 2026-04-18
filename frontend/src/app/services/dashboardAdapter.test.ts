import { describe, expect, it } from 'vitest';

import { adaptDashboardPayload } from './dashboardAdapter';

describe('dashboardAdapter communityBrief', () => {
  it('prefers exact-window community brief fields when present', () => {
    const app = adaptDashboardPayload({
      data: {
        communityBrief: {
          postsAnalyzedInWindow: 12,
          commentScopesAnalyzedInWindow: 34,
          totalAnalysesInWindow: 46,
          positiveIntentPct24h: 40,
          negativeIntentPct24h: 20,
          topTopics: ['Road And Transit'],
        },
      },
      meta: {
        days: 3,
      },
    });

    expect(app.communityBrief.postsAnalyzed24h).toBe(12);
    expect(app.communityBrief.commentScopesAnalyzed24h).toBe(34);
    expect(app.communityBrief.messagesAnalyzed).toBe(46);
    expect(app.communityBrief.mainBrief.en).toContain('12 posts and 34 analyzed comment scopes');
  });

  it('falls back to legacy fields when explicit exact-window values are absent', () => {
    const app = adaptDashboardPayload({
      data: {
        communityBrief: {
          postsAnalyzed24h: 7,
          commentScopesAnalyzed24h: 9,
          totalAnalyses24h: 16,
          positiveIntentPct24h: 30,
          negativeIntentPct24h: 10,
          topTopics: ['Telegram Community'],
        },
      },
      meta: {
        days: 1,
      },
    });

    expect(app.communityBrief.postsAnalyzed24h).toBe(7);
    expect(app.communityBrief.commentScopesAnalyzed24h).toBe(9);
    expect(app.communityBrief.messagesAnalyzed).toBe(16);
    expect(app.communityBrief.mainBrief.en).toContain('7 posts and 9 analyzed comment scopes');
  });
});
