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

  it('builds exact-range question briefs from question categories instead of reusing stale materialized cards', () => {
    const app = adaptDashboardPayload({
      meta: {
        requestedFrom: '2026-04-01',
        requestedTo: '2026-04-15',
      },
      data: {
        questionBriefs: [
          {
            id: 'stale-one',
            topic: 'Old Topic',
            category: 'General',
            canonicalQuestionEn: 'Old question',
            summaryEn: 'Stale summary',
            confidence: 'medium',
            confidenceScore: 0.6,
            demandSignals: { messages: 99, uniqueUsers: 40, channels: 8, trend7dPct: 10 },
          },
        ],
        questionCategories: [
          { category: 'Admin', topic: 'Telegram Community', seekers: 12, respondedSeekers: 6, sampleQuestion: 'How do I report scam channels?', sampleQuestionId: 'q-1', coveragePct: 50 },
          { category: 'Work', topic: 'Job Search', seekers: 8, respondedSeekers: 2, sampleQuestion: 'Where can I find Armenian-speaking recruiters?', sampleQuestionId: 'q-2', coveragePct: 25 },
          { category: 'Finance', topic: 'Banking', seekers: 5, respondedSeekers: 4, sampleQuestion: 'Which bank is easiest for newcomers?', sampleQuestionId: 'q-3', coveragePct: 80 },
          { category: 'Family', topic: 'Schools', seekers: 4, respondedSeekers: 1, sampleQuestion: 'Which schools help Russian-speaking children adapt?', sampleQuestionId: 'q-4', coveragePct: 25 },
          { category: 'Lifestyle', topic: 'Healthcare', seekers: 7, respondedSeekers: 4, sampleQuestion: 'How do I register with a clinic?', sampleQuestionId: 'q-5', coveragePct: 57 },
        ],
      },
    });

    expect(app.questionBriefs.en).toHaveLength(5);
    expect(app.questionBriefs.en.map((item) => item.sourceTopic)).toEqual([
      'Telegram Community',
      'Job Search',
      'Healthcare',
      'Banking',
      'Schools',
    ]);
    expect(app.questionBriefs.en[0]?.question).toContain('How do I report scam channels');
    expect(app.questionBriefs.en[0]?.demandSignals.messages).toBe(12);
    expect(app.questionBriefs.en[0]?.demandSignals.channels).toBe(0);
  });

  it('builds exact-range problem briefs from problem rows instead of reusing stale materialized cards', () => {
    const app = adaptDashboardPayload({
      meta: {
        requestedFrom: '2026-04-01',
        requestedTo: '2026-04-15',
      },
      data: {
        problemBriefs: [
          {
            id: 'stale-problem',
            topic: 'Old Topic',
            category: 'General',
            problemEn: 'Old problem',
            summaryEn: 'Stale summary',
            severity: 'high',
            confidence: 'medium',
            confidenceScore: 0.7,
            demandSignals: { messages: 50, uniqueUsers: 20, channels: 6, trend7dPct: 5 },
            evidence: [{ id: 'ev-1', quote: 'Old quote', channel: 'chan', timestamp: '2026-04-01', kind: 'message' }],
          },
        ],
        problems: [
          { topic: 'Road And Transit', category: 'Living', affectedUsers: 18, affectedThisWeek: 10, affectedPrevWeek: 6, trendSupport: 16, sampleText: 'People keep saying transport is unreliable late at night.', severity: 'Urgent', trendPct: 44 },
          { topic: 'Visa And Residency', category: 'Admin', affectedUsers: 11, affectedThisWeek: 6, affectedPrevWeek: 4, trendSupport: 10, sampleText: 'Users still describe the residency process as confusing and inconsistent.', severity: 'Negative', trendPct: 20 },
        ],
      },
    });

    expect(app.problemBriefs.en).toHaveLength(2);
    expect(app.problemBriefs.en.map((item) => item.sourceTopic)).toEqual([
      'Road And Transit',
      'Visa And Residency',
    ]);
    expect(app.problemBriefs.en[0]?.problem).toBe('Road And Transit');
    expect(app.problemBriefs.en[0]?.summary).toContain('transport is unreliable');
    expect(app.problemBriefs.en[0]?.evidence).toEqual([]);
  });
});
