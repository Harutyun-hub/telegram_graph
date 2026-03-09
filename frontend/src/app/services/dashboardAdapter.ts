import { mockAppData } from '../data/mockData';
import type { AppData } from '../types/data';

const TOPIC_COLORS = ['#ef4444', '#3b82f6', '#8b5cf6', '#f59e0b', '#ec4899', '#10b981', '#06b6d4', '#6b7280'];
const CONTENT_TYPE_RU: Record<string, string> = {
  text: 'Текст',
  photo: 'Фото',
  video: 'Видео',
  audio: 'Аудио',
  document: 'Документ',
};
const CATEGORY_RU: Record<string, string> = {
  Economy: 'Экономика',
  Politics: 'Политика',
  Society: 'Общество',
  Technology: 'Технологии',
  Culture: 'Культура',
  Security: 'Безопасность',
  General: 'Общий',
};
const DOW_EN = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];
const DOW_RU = ['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс'];
const MIN_SUPPORT_FOR_TREND = 8;
const MIN_SUPPORT_FOR_QA = 5;
const MAX_ABS_TREND_PCT = 100;

function toEmptyShape(value: any): any {
  if (Array.isArray(value)) return [];
  if (value === null || value === undefined) return value;
  if (typeof value === 'number') return 0;
  if (typeof value === 'string') return '';
  if (typeof value === 'boolean') return false;
  if (typeof value === 'object') {
    const out: Record<string, any> = {};
    for (const [k, v] of Object.entries(value)) {
      out[k] = toEmptyShape(v);
    }
    return out;
  }
  return value;
}

function createEmptyAppData(): AppData {
  return toEmptyShape(mockAppData) as AppData;
}

function asArray<T = any>(v: any): T[] {
  return Array.isArray(v) ? v : [];
}

function asNum(v: any, fallback = 0): number {
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}

function asStr(v: any, fallback = ''): string {
  if (typeof v === 'string') return v;
  if (v === null || v === undefined) return fallback;
  return String(v);
}

function normalizeTopicLabel(v: any): string {
  return asStr(v, '').replace(/\s+/g, ' ').trim();
}

function topicKey(v: any): string {
  return normalizeTopicLabel(v).toLowerCase();
}

function snippet(v: any, maxLen = 180): string {
  const text = asStr(v, '').replace(/\s+/g, ' ').trim();
  if (!text) return '';
  return text.length > maxLen ? `${text.slice(0, maxLen - 1)}…` : text;
}

function clamp(n: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, n));
}

function pct(part: number, total: number): number {
  if (!total) return 0;
  return Math.round((part / total) * 100);
}

function boundedTrend(current: number, previous: number, minSupport = MIN_SUPPORT_FOR_TREND) {
  const support = Math.max(0, current) + Math.max(0, previous);
  if (support < minSupport) {
    return { value: 0, reliable: false, support };
  }
  const smoothedPrev = Math.max(1, previous + 3);
  const value = clamp(Math.round(((current - previous) / smoothedPrev) * 100), -MAX_ABS_TREND_PCT, MAX_ABS_TREND_PCT);
  return { value, reliable: true, support };
}

function hashColor(key: string): string {
  if (!key) return TOPIC_COLORS[0];
  let h = 0;
  for (let i = 0; i < key.length; i += 1) h = (h * 31 + key.charCodeAt(i)) >>> 0;
  return TOPIC_COLORS[h % TOPIC_COLORS.length];
}

function slugify(text: string): string {
  return text
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '') || 'item';
}

function unwrapPayload(payload: any): any {
  if (payload && typeof payload === 'object' && 'data' in payload && payload.data) {
    return payload.data;
  }
  return payload;
}

function stageStyle(stageRaw: string) {
  const stage = stageRaw.toLowerCase();
  if (stage.includes('emerg')) {
    return { en: 'Emerging', ru: 'Зарождение', color: '#3b82f6', bgColor: 'bg-blue-50', borderColor: 'border-blue-200', textColor: 'text-blue-700', descEn: 'new', descRu: 'новая' };
  }
  if (stage.includes('peak')) {
    return { en: 'Peak', ru: 'Пик', color: '#10b981', bgColor: 'bg-emerald-50', borderColor: 'border-emerald-200', textColor: 'text-emerald-700', descEn: 'high', descRu: 'высокая' };
  }
  if (stage.includes('estab')) {
    return { en: 'Established', ru: 'Стабильная', color: '#8b5cf6', bgColor: 'bg-violet-50', borderColor: 'border-violet-200', textColor: 'text-violet-700', descEn: 'stable', descRu: 'стабильная' };
  }
  return { en: 'Fading', ru: 'Угасание', color: '#f59e0b', bgColor: 'bg-amber-50', borderColor: 'border-amber-200', textColor: 'text-amber-700', descEn: 'fading', descRu: 'спад' };
}

export function adaptDashboardPayload(payload: any): AppData {
  const raw = unwrapPayload(payload) || {};
  const app = createEmptyAppData();

  const rawTrending = asArray(raw.trendingTopics);
  const rawTopicBubbles = asArray(raw.topicBubbles);
  const rawTrendRows = asArray(raw.trendLines);
  const rawLifecycle = asArray(raw.lifecycleStages);
  const rawChannels = asArray(raw.communityChannels);
  const rawKeyVoices = asArray(raw.keyVoices);
  const rawEmerging = asArray(raw.emergingInterests);
  const rawTopPosts = asArray(raw.topPosts);
  const rawSentimentByTopic = asArray(raw.sentimentByTopic);
  const rawAllTopics = asArray(raw.allTopics);

  const topicEvidenceTextByTopic = new Map<string, string>();
  const topicQuestionEvidenceByTopic = new Map<string, string[]>();
  rawAllTopics.forEach((row: any) => {
    const topic = normalizeTopicLabel(row?.name);
    if (!topic) return;
    const key = topicKey(topic);
    const evidenceRows = asArray(row?.evidence);
    const questionRows = asArray(row?.questionEvidence);

    const firstEvidence = evidenceRows
      .map((ev: any) => snippet(ev?.text, 180))
      .find((text: string) => text.length > 0);
    if (firstEvidence && !topicEvidenceTextByTopic.has(key)) {
      topicEvidenceTextByTopic.set(key, firstEvidence);
    }

    const questionSnippets = [
      ...questionRows.map((ev: any) => snippet(ev?.text, 180)),
      ...evidenceRows.map((ev: any) => snippet(ev?.text, 180)).filter((text: string) => text.includes('?')),
    ].filter((text: string) => text.length > 0);
    if (questionSnippets.length > 0) {
      topicQuestionEvidenceByTopic.set(key, questionSnippets.slice(0, 10));
    }
  });

  try {
    const score = clamp(asNum(raw?.communityHealth?.score, app.communityHealth.currentScore), 0, 100);
    const trendDirection = asStr(raw?.communityHealth?.trend, 'up');
    const weekAgo = clamp(score - (trendDirection === 'up' ? 6 : -6), 0, 100);
    app.communityHealth.currentScore = score;
    app.communityHealth.weekAgoScore = weekAgo;
    app.communityHealth.history = Array.from({ length: 7 }).map((_, i) => ({
      time: i === 6 ? 'Now' : `${6 - i}h ago`,
      score: clamp(Math.round(weekAgo + ((score - weekAgo) * (i / 6))), 0, 100),
    }));
    const totalUsers = Math.max(1, asNum(raw?.communityHealth?.totalUsers, 1));
    const activeUsers = asNum(raw?.communityHealth?.activeUsers, 0);
    const totalPosts = asNum(raw?.communityHealth?.totalPosts, 0);
    app.communityHealth.components = {
      en: [
        { label: 'Engagement Rate', value: clamp(pct(activeUsers, totalUsers), 0, 100), trend: score >= weekAgo ? 4 : -4, desc: 'Active users in last 7d' },
        { label: 'Community Growth', value: clamp(score, 0, 100), trend: score - weekAgo, desc: 'Composite vitality index' },
        { label: 'Content Velocity', value: clamp(Math.round(totalPosts / 8), 0, 100), trend: 3, desc: 'Posting volume trend' },
        { label: 'Sentiment Stability', value: clamp(Math.round((score + 100) / 2), 0, 100), trend: 2, desc: 'Sentiment and tone balance' },
      ],
      ru: [
        { label: 'Вовлечённость', value: clamp(pct(activeUsers, totalUsers), 0, 100), trend: score >= weekAgo ? 4 : -4, desc: 'Активные участники за 7 дней' },
        { label: 'Рост сообщества', value: clamp(score, 0, 100), trend: score - weekAgo, desc: 'Композитный индекс динамики' },
        { label: 'Скорость контента', value: clamp(Math.round(totalPosts / 8), 0, 100), trend: 3, desc: 'Динамика публикаций' },
        { label: 'Стабильность тона', value: clamp(Math.round((score + 100) / 2), 0, 100), trend: 2, desc: 'Баланс настроений и тона' },
      ],
    };
  } catch {
    // Keep mock defaults.
  }

  try {
    const sentiments = ['seeking', 'curious', 'excited', 'concerned', 'frustrated', 'motivated', 'confused'];
      const toTopic = (row: any, i: number, ru: boolean) => {
        const topic = normalizeTopicLabel(row.name || row.topic) || `Topic ${i + 1}`;
        const key = topicKey(topic);
        const category = asStr(row.category, 'General');
        const mentions = asNum(row.mentions || row.postMentions || row.totalPosts, 0);
      const trend = asNum(
        row.trendPct,
        Number.isFinite(asNum(row.currentMentions, Number.NaN)) && Number.isFinite(asNum(row.previousMentions, Number.NaN))
          ? (asNum(row.previousMentions, 0) > 0
              ? Math.round(((asNum(row.currentMentions, 0) - asNum(row.previousMentions, 0)) / asNum(row.previousMentions, 1)) * 100)
              : (asNum(row.currentMentions, 0) > 0 ? 100 : 0))
          : asNum(row.trend, 0),
      );
        const quoteFromEvidence = topicEvidenceTextByTopic.get(key) || topicQuestionEvidenceByTopic.get(key)?.[0] || '';
        return {
          id: i + 1,
          topic: ru ? topic : topic,
        mentions,
        trend,
          category: ru ? (CATEGORY_RU[category] || category) : category,
          sentiment: sentiments[i % sentiments.length],
          sampleQuote: quoteFromEvidence,
        };
      };
    if (rawTrending.length > 0) {
      app.trendingTopics.en = rawTrending.slice(0, 12).map((r: any, i: number) => toTopic(r, i, false));
      app.trendingTopics.ru = rawTrending.slice(0, 12).map((r: any, i: number) => toTopic(r, i, true));
    }
  } catch {
    // Keep mock defaults.
  }

  try {
    const posts24h = asNum(raw?.communityBrief?.postsLast24h, 0);
    const comments24h = asNum(raw?.communityBrief?.commentsLast24h, 0);
    const active24h = asNum(raw?.communityBrief?.activeUsersLast24h, 0);
    const topTopics = asArray<string>(raw?.communityBrief?.topTopics).slice(0, 5);
    app.communityBrief.messagesAnalyzed = asNum(raw?.vitalityIndicators?.totalComments, comments24h) + asNum(raw?.vitalityIndicators?.totalPosts, posts24h);
    app.communityBrief.updatedMinutesAgo = 5;
    app.communityBrief.activeMembers = active24h.toLocaleString();
    app.communityBrief.messagesToday = (posts24h + comments24h).toLocaleString();
    app.communityBrief.positiveMood = `${clamp(asNum(raw?.communityHealth?.score, 60), 0, 100)}%`;
    const newMembersGrowth = asNum(raw?.communityBrief?.newActiveUsersGrowthPct, Number.NaN);
    const postChangeFallback = asNum(asArray(raw.weeklyShifts)[0]?.postChange, 0);
    const newGrowth = Number.isFinite(newMembersGrowth) ? newMembersGrowth : postChangeFallback;
    app.communityBrief.newMembersGrowth = `${newGrowth > 0 ? '+' : ''}${Math.round(newGrowth)}%`;
    app.communityBrief.mainBrief.en = `Community data shows ${posts24h} posts and ${comments24h} comments in the last 24h. Top themes include ${topTopics.join(', ') || 'core community topics'}.`;
    app.communityBrief.mainBrief.ru = `Данные сообщества показывают ${posts24h} постов и ${comments24h} комментариев за 24 часа. Ключевые темы: ${topTopics.join(', ') || 'основные темы сообщества'}.`;
    app.communityBrief.expandedBrief.en = [
      `Active users in the last 24h: ${active24h}.`,
      'Dashboard metrics are aggregated from the backend data pipeline.',
    ];
    app.communityBrief.expandedBrief.ru = [
      `Активных пользователей за 24 часа: ${active24h}.`,
      'Метрики панели агрегируются из серверного контура данных.',
    ];
  } catch {
    // Keep mock defaults.
  }

  try {
    if (rawTopicBubbles.length > 0) {
      const mergedByTopic = new Map<string, any>();
      rawTopicBubbles.forEach((r: any) => {
        const name = normalizeTopicLabel(r.name || r.topic);
        if (!name) return;
        const key = topicKey(name);
        const existing = mergedByTopic.get(key);
        if (!existing) {
          mergedByTopic.set(key, {
            ...r,
            name,
            mentionCount: asNum(r.mentionCount, asNum(r.postMentions, 0) + asNum(r.commentMentions, 0)),
            mentions7d: asNum(r.mentions7d, 0),
            mentionsPrev7d: asNum(r.mentionsPrev7d, 0),
          });
          return;
        }
        mergedByTopic.set(key, {
          ...existing,
          mentionCount: asNum(existing.mentionCount, 0) + asNum(r.mentionCount, asNum(r.postMentions, 0) + asNum(r.commentMentions, 0)),
          mentions7d: asNum(existing.mentions7d, 0) + asNum(r.mentions7d, 0),
          mentionsPrev7d: asNum(existing.mentionsPrev7d, 0) + asNum(r.mentionsPrev7d, 0),
          growthSupport: asNum(existing.growthSupport, 0) + asNum(r.growthSupport, 0),
          category: asStr(existing.category || r.category, 'General'),
        });
      });

      const dedupedRows = Array.from(mergedByTopic.values())
        .sort((a, b) => asNum(b.mentionCount, 0) - asNum(a.mentionCount, 0))
        .slice(0, 20);

      const convert = (ru: boolean) => dedupedRows.map((r: any) => {
        const category = asStr(r.category, 'General');
        const value = asNum(r.mentionCount, asNum(r.postMentions, 0) + asNum(r.commentMentions, 0));
        const weeklyCurrent = asNum(r.mentions7d, 0);
        const weeklyPrevious = asNum(r.mentionsPrev7d, 0);
        const fromWindow = boundedTrend(weeklyCurrent, weeklyPrevious);
        const support = asNum(r.growthSupport, fromWindow.support);
        const backendGrowth = asNum(r.growth7dPct, Number.NaN);
        const backendReliable = Number.isFinite(backendGrowth) && support >= MIN_SUPPORT_FOR_TREND;
        return {
          name: normalizeTopicLabel(r.name),
          value,
          category: ru ? (CATEGORY_RU[category] || category) : category,
          color: hashColor(category),
          growth: backendReliable
            ? clamp(Math.round(backendGrowth), -MAX_ABS_TREND_PCT, MAX_ABS_TREND_PCT)
            : fromWindow.value,
          growthReliable: backendReliable || fromWindow.reliable,
          evidenceCount: support,
        };
      });
      app.topicBubbles.en = convert(false);
      app.topicBubbles.ru = convert(true);
    }
  } catch {
    // Keep mock defaults.
  }

  try {
    if (rawTrendRows.length > 0) {
      const totals = new Map<string, number>();
      rawTrendRows.forEach((r: any) => {
        const t = asStr(r.topic, 'topic');
        totals.set(t, (totals.get(t) || 0) + asNum(r.posts, 0));
      });
      const topTopics = Array.from(totals.entries()).sort((a, b) => b[1] - a[1]).slice(0, 6).map(([t]) => t);
      const keyByTopic = Object.fromEntries(topTopics.map((t) => [t, slugify(t)]));
      const weeksMap = new Map<string, any>();
      rawTrendRows.forEach((r: any) => {
        const topic = asStr(r.topic);
        if (!topTopics.includes(topic)) return;
        const year = asNum(r.year, 0);
        const week = asNum(r.week, 0);
        const label = `${year}-W${String(week).padStart(2, '0')}`;
        if (!weeksMap.has(label)) weeksMap.set(label, { week: label });
        weeksMap.get(label)[keyByTopic[topic]] = asNum(r.posts, 0);
      });
      app.trendData = Array.from(weeksMap.values()).sort((a, b) => asStr(a.week).localeCompare(asStr(b.week)));
      app.trendLines.en = topTopics.map((topic, i) => {
        const key = keyByTopic[topic];
        const first = asNum(app.trendData[0]?.[key], 0);
        const current = asNum(app.trendData[app.trendData.length - 1]?.[key], 0);
        return {
          key,
          label: topic,
          color: TOPIC_COLORS[i % TOPIC_COLORS.length],
          current,
          change: first > 0 ? Math.round(((current - first) / first) * 100) : 0,
        };
      });
      app.trendLines.ru = app.trendLines.en.map((line) => ({ ...line }));
    }
  } catch {
    // Keep mock defaults.
  }

  try {
    const heatRows = asArray(raw.heatmap);
    if (heatRows.length > 0) {
      const topicTotals = new Map<string, number>();
      const contentTypes = new Set<string>();
      heatRows.forEach((r: any) => {
        const topic = asStr(r.topic);
        topicTotals.set(topic, (topicTotals.get(topic) || 0) + asNum(r.count, 0));
        contentTypes.add(asStr(r.mediaType || 'text'));
      });
      const topicCols = Array.from(topicTotals.entries()).sort((a, b) => b[1] - a[1]).slice(0, 6).map(([t]) => t);
      const types = Array.from(contentTypes);
      const maxCount = Math.max(...heatRows.map((r: any) => asNum(r.count, 0)), 1);
      const engagementEn: Record<string, Record<string, number>> = {};
      const engagementRu: Record<string, Record<string, number>> = {};
      types.forEach((type) => {
        engagementEn[type] = {};
        engagementRu[CONTENT_TYPE_RU[type] || type] = {};
        topicCols.forEach((topic) => {
          const row = heatRows.find((r: any) => asStr(r.mediaType || 'text') === type && asStr(r.topic) === topic);
          const v = Math.round((asNum(row?.count, 0) / maxCount) * 100);
          engagementEn[type][topic] = v;
          engagementRu[CONTENT_TYPE_RU[type] || type][topic] = v;
        });
      });
      app.heatmap = {
        en: { contentTypes: types, topicCols, engagement: engagementEn },
        ru: { contentTypes: types.map((t) => CONTENT_TYPE_RU[t] || t), topicCols, engagement: engagementRu },
      };
    }
  } catch {
    // Keep mock defaults.
  }

  try {
    const qRows = asArray(raw.questionCategories);
    if (qRows.length > 0) {
      const merged = new Map<string, any>();
      qRows.forEach((r: any) => {
        const topic = normalizeTopicLabel(r.topic);
        if (!topic) return;
        const key = topicKey(topic);
        const existing = merged.get(key);
        if (!existing) {
          merged.set(key, {
            ...r,
            topic,
            seekers: asNum(r.seekers, 0),
            respondedSeekers: asNum(r.respondedSeekers, 0),
          });
          return;
        }
        merged.set(key, {
          ...existing,
          seekers: asNum(existing.seekers, 0) + asNum(r.seekers, 0),
          respondedSeekers: asNum(existing.respondedSeekers, 0) + asNum(r.respondedSeekers, 0),
          sampleQuestion: asStr(existing.sampleQuestion) || asStr(r.sampleQuestion),
          category: asStr(existing.category || r.category, 'General'),
        });
      });

      const byCat = new Map<string, any[]>();
      Array.from(merged.values()).forEach((r: any) => {
        const cat = asStr(r.category, 'General');
        const topic = normalizeTopicLabel(r.topic) || 'Topic';
        if (!byCat.has(cat)) byCat.set(cat, []);
        const count = asNum(r.seekers, 0);
        const answeredCount = asNum(r.respondedSeekers, 0);
        const lowEvidence = count < MIN_SUPPORT_FOR_QA;
        const coveragePct = lowEvidence
          ? 0
          : clamp(Math.round(asNum(r.coveragePct, count > 0 ? (answeredCount / count) * 100 : 0)), 0, 100);
        const sampleQuestion = snippet(r.sampleQuestion, 140);
        const questionText = sampleQuestion && topicKey(sampleQuestion) !== topicKey(topic)
          ? sampleQuestion
          : (topicQuestionEvidenceByTopic.get(topicKey(topic))?.[0] || '');
        byCat.get(cat)?.push({
          q: topic,
          preview: questionText,
          topic,
          count,
          answered: lowEvidence ? false : answeredCount > 0,
          coveragePct,
          lowEvidence,
        });
      });
      app.questionCategories.en = Array.from(byCat.entries()).slice(0, 5).map(([cat, questions], i) => ({ category: cat, color: TOPIC_COLORS[i % TOPIC_COLORS.length], questions: questions.slice(0, 4) }));
      app.questionCategories.ru = Array.from(byCat.entries()).slice(0, 5).map(([cat, questions], i) => ({
        category: CATEGORY_RU[cat] || cat,
        color: TOPIC_COLORS[i % TOPIC_COLORS.length],
        questions: questions.slice(0, 4).map((q) => ({ ...q })),
      }));
      const enGap = app.questionCategories.en.flatMap((cat) => cat.questions.map((q: any) => ({
        topic: asStr(q.topic, q.q),
        asked: q.count,
        rate: clamp(asNum(q.coveragePct, 0), 0, 100),
        lowEvidence: !!q.lowEvidence,
      })));
      const ruGap = app.questionCategories.ru.flatMap((cat) => cat.questions.map((q: any) => ({
        topic: asStr(q.topic, q.q),
        asked: q.count,
        rate: clamp(asNum(q.coveragePct, 0), 0, 100),
        lowEvidence: !!q.lowEvidence,
      })));
      app.qaGap = { en: enGap, ru: ruGap };
    }
  } catch {
    // Keep mock defaults.
  }

  try {
    if (rawLifecycle.length > 0) {
      const byStage = new Map<string, any[]>();
      rawLifecycle.forEach((r: any) => {
        const stage = asStr(r.stage, 'established');
        if (!byStage.has(stage)) byStage.set(stage, []);
        const first = new Date(asStr(r.firstSeen, new Date().toISOString()));
        const daysActive = Math.max(1, Math.round((Date.now() - first.getTime()) / 86400000));
        const total = Math.max(1, asNum(r.totalPosts, 1));
        const recent = asNum(r.recentPosts, 0);
        byStage.get(stage)?.push({
          name: asStr(r.topic),
          daysActive,
          momentum: clamp(Math.round((recent / total) * 100), -100, 200),
          volume: recent || total,
        });
      });
      const stagesEn = Array.from(byStage.entries()).map(([stage, topics]) => {
        const s = stageStyle(stage);
        return { stage: s.en, color: s.color, bgColor: s.bgColor, borderColor: s.borderColor, textColor: s.textColor, desc: s.descEn, topics: topics.slice(0, 8) };
      });
      const stagesRu = Array.from(byStage.entries()).map(([stage, topics]) => {
        const s = stageStyle(stage);
        return { stage: s.ru, color: s.color, bgColor: s.bgColor, borderColor: s.borderColor, textColor: s.textColor, desc: s.descRu, topics: topics.slice(0, 8) };
      });
      app.lifecycleStages = { en: stagesEn, ru: stagesRu };
    }
  } catch {
    // Keep mock defaults.
  }

  try {
    const pRows = asArray(raw.problems);
    if (pRows.length > 0) {
      const grouped = new Map<string, Map<string, any>>();
      pRows.forEach((r: any) => {
        const category = asStr(r.category, 'General');
        if (!grouped.has(category)) grouped.set(category, new Map());
        const sev = asStr(r.severity, 'Negative').toLowerCase().includes('urgent') ? 'high' : 'medium';
        const topic = normalizeTopicLabel(r.topic);
        if (!topic) return;
        const key = topicKey(topic);
        const weeklyCurrent = asNum(r.affectedThisWeek, 0);
        const weeklyPrevious = asNum(r.affectedPrevWeek, 0);
        const computedTrend = boundedTrend(weeklyCurrent, weeklyPrevious);
        const trendSupport = asNum(r.trendSupport, computedTrend.support);
        const backendTrend = asNum(r.trendPct, Number.NaN);
        const trendReliable = Number.isFinite(backendTrend) && trendSupport >= MIN_SUPPORT_FOR_TREND;
        const entry = {
          name: topic,
          mentions: asNum(r.affectedUsers, 0),
          severity: sev,
          trend: trendReliable
            ? clamp(Math.round(backendTrend), -MAX_ABS_TREND_PCT, MAX_ABS_TREND_PCT)
            : computedTrend.value,
          trendReliable: trendReliable || computedTrend.reliable,
          evidenceCount: trendSupport,
          quote: snippet(r.sampleText, 180) || topicEvidenceTextByTopic.get(key) || '',
        };
        const existing = grouped.get(category)?.get(key);
        if (!existing) {
          grouped.get(category)?.set(key, entry);
          return;
        }
        grouped.get(category)?.set(key, {
          ...existing,
          mentions: asNum(existing.mentions, 0) + asNum(entry.mentions, 0),
          evidenceCount: asNum(existing.evidenceCount, 0) + asNum(entry.evidenceCount, 0),
          trend: Math.abs(entry.trend) > Math.abs(existing.trend) ? entry.trend : existing.trend,
          trendReliable: existing.trendReliable || entry.trendReliable,
          quote: asStr(existing.quote) || asStr(entry.quote),
          severity: existing.severity === 'high' || entry.severity === 'high' ? 'high' : existing.severity,
        });
      });
      const en = Array.from(grouped.entries()).map(([category, problemMap]) => ({
        category,
        problems: Array.from(problemMap.values()).sort((a, b) => b.mentions - a.mentions).slice(0, 5),
      }));
      const ru = Array.from(grouped.entries()).map(([category, problemMap]) => ({
        category: CATEGORY_RU[category] || category,
        problems: Array.from(problemMap.values()).sort((a, b) => b.mentions - a.mentions).slice(0, 5).map((p) => ({ ...p })),
      }));
      app.problems = { en, ru };
    }
  } catch {
    // Keep mock defaults.
  }

  try {
    const sgRows = asArray(raw.serviceGaps);
    if (sgRows.length > 0) {
      const mapGap = (r: any, ru: boolean) => {
        const dissatisfaction = asNum(r.dissatisfactionPct, 0);
        const supplyLevel = dissatisfaction >= 85 ? 'none' : dissatisfaction >= 70 ? 'very_low' : dissatisfaction >= 55 ? 'low' : dissatisfaction >= 35 ? 'moderate' : 'adequate';
        const supply = ru
          ? (supplyLevel === 'none' ? 'Нет' : supplyLevel === 'very_low' ? 'Очень низко' : supplyLevel === 'low' ? 'Низко' : supplyLevel === 'moderate' ? 'Средне' : 'Достаточно')
          : (supplyLevel === 'none' ? 'None' : supplyLevel === 'very_low' ? 'Very Low' : supplyLevel === 'low' ? 'Low' : supplyLevel === 'moderate' ? 'Moderate' : 'Adequate');
        const current = asNum(r.demandThisWeek, 0);
        const previous = asNum(r.demandPrevWeek, 0);
        const computedGrowth = boundedTrend(current, previous);
        const support = asNum(r.demandGrowthSupport, computedGrowth.support);
        const backendGrowth = asNum(r.demandGrowthPct, Number.NaN);
        const growthReliable = Number.isFinite(backendGrowth) && support >= MIN_SUPPORT_FOR_TREND;
        return {
          service: asStr(r.topic),
          demand: asNum(r.demand, 0),
          supply,
          gap: clamp(Math.round(dissatisfaction), 0, 100),
          growth: growthReliable
            ? clamp(Math.round(backendGrowth), -MAX_ABS_TREND_PCT, MAX_ABS_TREND_PCT)
            : computedGrowth.value,
          growthReliable: growthReliable || computedGrowth.reliable,
          evidenceCount: support,
          supplyLevel,
        };
      };
      app.serviceGaps.en = sgRows.map((r: any) => mapGap(r, false)).slice(0, 12);
      app.serviceGaps.ru = sgRows.map((r: any) => mapGap(r, true)).slice(0, 12);
    }
  } catch {
    // Keep mock defaults.
  }

  try {
    const satRows = asArray(raw.satisfactionAreas);
    if (satRows.length > 0) {
      const mk = (r: any) => {
        const area = asStr(r.category, 'General');
        const satisfaction = clamp(Math.round(asNum(r.satisfactionPct, 0)), 0, 100);
        return {
          area,
          satisfaction,
          mentions: asNum(r.pos, 0) + asNum(r.neg, 0) + asNum(r.neu, 0),
          trend: clamp(Math.round((asNum(r.pos, 0) - asNum(r.neg, 0)) / 8), -50, 50),
          emoji: satisfaction >= 65 ? '🙂' : satisfaction >= 45 ? '😐' : '😟',
        };
      };
      app.satisfactionAreas.en = satRows.map(mk);
      app.satisfactionAreas.ru = satRows.map((r: any) => ({ ...mk(r), area: CATEGORY_RU[asStr(r.category)] || asStr(r.category) }));
    }
  } catch {
    // Keep mock defaults.
  }

  try {
    const moodRows = asArray(raw.moodData);
    if (moodRows.length > 0) {
      const byWeek = new Map<string, any>();
      moodRows.forEach((r: any) => {
        const key = `${asNum(r.year, 0)}-W${String(asNum(r.week, 0)).padStart(2, '0')}`;
        if (!byWeek.has(key)) byWeek.set(key, { week: key, excited: 0, satisfied: 0, neutral: 0, frustrated: 0, anxious: 0 });
        const sentiment = asStr(r.sentiment).toLowerCase();
        const count = asNum(r.count, 0);
        const row = byWeek.get(key);
        if (sentiment.includes('positive')) row.satisfied += count;
        else if (sentiment.includes('negative')) row.frustrated += count;
        else if (sentiment.includes('urgent')) row.anxious += count;
        else if (sentiment.includes('mixed')) row.excited += Math.round(count / 2);
        else row.neutral += count;
      });
      const mood = Array.from(byWeek.values()).sort((a, b) => asStr(a.week).localeCompare(asStr(b.week))).slice(-10);
      mood.forEach((m) => { if (m.excited === 0) m.excited = Math.round(m.satisfied * 0.35); });
      app.moodData = mood;
    }
    app.moodConfig = {
      en: [
        { key: 'excited', label: 'Excited', color: '#10b981', emoji: '🎉', polarity: 'positive' },
        { key: 'satisfied', label: 'Satisfied', color: '#3b82f6', emoji: '🙂', polarity: 'positive' },
        { key: 'neutral', label: 'Neutral', color: '#9ca3af', emoji: '😐', polarity: 'neutral' },
        { key: 'frustrated', label: 'Frustrated', color: '#f97316', emoji: '😟', polarity: 'negative' },
        { key: 'anxious', label: 'Anxious', color: '#ef4444', emoji: '⚠️', polarity: 'negative' },
      ],
      ru: [
        { key: 'excited', label: 'Воодушевлены', color: '#10b981', emoji: '🎉', polarity: 'positive' },
        { key: 'satisfied', label: 'Довольны', color: '#3b82f6', emoji: '🙂', polarity: 'positive' },
        { key: 'neutral', label: 'Нейтрально', color: '#9ca3af', emoji: '😐', polarity: 'neutral' },
        { key: 'frustrated', label: 'Раздражены', color: '#f97316', emoji: '😟', polarity: 'negative' },
        { key: 'anxious', label: 'Тревожны', color: '#ef4444', emoji: '⚠️', polarity: 'negative' },
      ],
    };
  } catch {
    // Keep mock defaults.
  }

  try {
    const uRows = asArray(raw.urgencySignals);
    if (uRows.length > 0) {
      const en = uRows.map((r: any) => ({
        message: `Need immediate help around ${asStr(r.topic)}.`,
        topic: asStr(r.topic),
        urgency: asNum(r.urgentUsers, 0) > 8 ? 'critical' : 'high',
        count: asNum(r.urgentUsers, 0),
        action: 'Assign moderator follow-up and pin guidance',
      }));
      const ru = uRows.map((r: any) => ({
        message: `Нужна срочная помощь по теме: ${asStr(r.topic)}.`,
        topic: asStr(r.topic),
        urgency: asNum(r.urgentUsers, 0) > 8 ? 'critical' : 'high',
        count: asNum(r.urgentUsers, 0),
        action: 'Назначить модератора и закрепить инструкцию',
      }));
      app.urgencySignals = { en, ru };
    }
  } catch {
    // Keep mock defaults.
  }

  try {
    if (rawChannels.length > 0) {
      app.communityChannels = rawChannels.map((c: any, i: number) => ({
        name: asStr(c.title || c.username || `Channel ${i + 1}`),
        type: 'General',
        members: Math.max(500, asNum(c.memberCount, 0) || asNum(c.postCount, 0) * 120),
        dailyMessages: Math.max(1, Math.round(asNum(c.postCount, 0) / 30)),
        engagement: clamp(Math.round(asNum(c.avgViews, 0) / 25), 5, 99),
        growth: clamp(Math.round(asNum(c.postCount, 0) / 20), -20, 50),
        topTopicEN: asStr(asArray(c.topTopics)[0], app.trendingTopics.en[0]?.topic || 'General'),
        topTopicRU: asStr(asArray(c.topTopics)[0], app.trendingTopics.ru[0]?.topic || 'Общий'),
      }));
    }
  } catch {
    // Keep mock defaults.
  }

  try {
    if (rawKeyVoices.length > 0) {
      const mk = (r: any, i: number, ru: boolean) => {
        const baseName = `User ${asStr(r.userId, i + 1)}`;
        const helpScore = clamp(Math.round(asNum(r.influenceScore, 0) * 2), 20, 100);
        const type = helpScore >= 85 ? 'Expert' : helpScore >= 70 ? 'Helper' : helpScore >= 55 ? 'Organizer' : 'Content Creator';
        return {
          name: baseName,
          role: ru ? (asStr(r.role, 'Member') || 'Участник') : asStr(r.role, 'Member'),
          followers: Math.max(100, asNum(r.commentCount, 0) * 18 + asNum(r.replyCount, 0) * 30),
          helpScore,
          topics: app.trendingTopics.en.slice(0, 3).map((t) => (ru ? t.topic : t.topic)),
          postsPerWeek: Math.max(1, asNum(r.commentCount, 0)),
          replyRate: clamp(Math.round((asNum(r.replyCount, 0) / Math.max(1, asNum(r.commentCount, 1))) * 100), 5, 99),
          type,
        };
      };
      app.keyVoices.en = rawKeyVoices.slice(0, 15).map((r: any, i: number) => mk(r, i, false));
      app.keyVoices.ru = rawKeyVoices.slice(0, 15).map((r: any, i: number) => mk(r, i, true));
    }
  } catch {
    // Keep mock defaults.
  }

  try {
    const hourly = asArray(raw.hourlyActivity);
    if (hourly.length > 0) {
      app.hourlyActivity = hourly
        .map((h: any) => ({ hour: String(asNum(h.hour, 0)).padStart(2, '0'), messages: asNum(h.count, 0) }))
        .sort((a, b) => a.hour.localeCompare(b.hour));
    }
    const weekly = asArray(raw.weeklyActivity);
    if (weekly.length > 0) {
      app.weeklyActivity = weekly
        .map((d: any) => {
          const idx = clamp(asNum(d.dow, 1) - 1, 0, 6);
          return { day: DOW_RU[idx], dayEN: DOW_EN[idx], messages: asNum(d.count, 0) };
        })
        .sort((a, b) => DOW_EN.indexOf(a.dayEN) - DOW_EN.indexOf(b.dayEN));
    }
  } catch {
    // Keep mock defaults.
  }

  try {
    const recRows = asArray(raw.recommendations);
    if (recRows.length > 0) {
      const mk = (r: any, ru: boolean) => ({
        item: ru ? `Рекомендации пользователя ${asStr(r.userId)}` : `Recommendations from user ${asStr(r.userId)}`,
        category: asStr(asArray(r.topics)[0], ru ? 'Общий' : 'General'),
        mentions: asNum(r.helpCount, 0),
        rating: clamp(Math.round(Math.min(5, 3 + asNum(r.helpCount, 0) / 4)), 1, 5),
        sentiment: 'positive',
      });
      app.recommendations.en = recRows.slice(0, 12).map((r: any) => mk(r, false));
      app.recommendations.ru = recRows.slice(0, 12).map((r: any) => mk(r, true));
    }
  } catch {
    // Keep mock defaults.
  }

  try {
    const newcomerRows = asArray(raw.newcomerJourney);
    if (newcomerRows.length > 0) {
      const buckets = [
        { id: 'first', en: 'First Week', ru: 'Первая неделя', min: 0, max: 2 },
        { id: 'settling', en: 'Settling In', ru: 'Адаптация', min: 3, max: 5 },
        { id: 'engaged', en: 'Becoming Active', ru: 'Рост активности', min: 6, max: 12 },
        { id: 'core', en: 'Core Community', ru: 'Ядро сообщества', min: 13, max: 9999 },
      ];
      const toStage = (ru: boolean) => buckets.map((b) => {
        const rows = newcomerRows.filter((r: any) => {
          const n = asNum(r.commentCount, 0);
          return n >= b.min && n <= b.max;
        });
        const volume = rows.length;
        const resolved = volume === 0 ? 0 : clamp(40 + Math.round(rows.reduce((s: number, r: any) => s + asNum(r.commentCount, 0), 0) / Math.max(volume, 1)), 10, 95);
        const topics = Array.from(new Set(rows.flatMap((r: any) => asArray<string>(r.topics)))).slice(0, 4);
        return {
          stage: ru ? b.ru : b.en,
          questions: topics.length ? topics : (ru ? ['Базовые вопросы'] : ['Basic questions']),
          volume,
          resolved,
        };
      });
      app.newcomerJourney = { en: toStage(false), ru: toStage(true) };
    }
  } catch {
    // Keep mock defaults.
  }

  try {
    const viralRows = asArray(raw.viralTopics);
    if (viralRows.length > 0) {
      const topAmplifiers = app.communityChannels.slice(0, 3).map((c) => c.name);
      const mk = (r: any, ru: boolean) => {
        const co = asNum(r.coOccurrences, 0);
        const conn = asNum(r.connectedTopics, 0);
        return {
          topic: asStr(r.topic),
          originator: app.communityChannels[0]?.name || (ru ? 'Канал сообщества' : 'Community channel'),
          spreadHours: clamp(Math.round(72 / Math.max(1, Math.log2(co + 2))), 1, 72),
          channelsReached: Math.max(1, conn),
          amplifiers: topAmplifiers,
          totalReach: co * 70,
          velocity: co > 150 ? 'explosive' : co > 80 ? 'fast' : 'normal',
        };
      };
      app.viralTopics = {
        en: viralRows.slice(0, 12).map((r: any) => mk(r, false)),
        ru: viralRows.slice(0, 12).map((r: any) => mk(r, true)),
      };
    }
  } catch {
    // Keep mock defaults.
  }

  try {
    const personaRows = asArray(raw.personas);
    if (personaRows.length > 0) {
      const byRole = new Map<string, number>();
      personaRows.forEach((p: any) => {
        const role = asStr(p.role, 'Member');
        byRole.set(role, (byRole.get(role) || 0) + asNum(p.count, 0));
      });
      const total = Math.max(1, Array.from(byRole.values()).reduce((s, n) => s + n, 0));
      const sorted = Array.from(byRole.entries()).sort((a, b) => b[1] - a[1]).slice(0, 6);
      const mk = (ru: boolean) => sorted.map(([role, count], i) => ({
        name: role,
        size: pct(count, total),
        count,
        color: TOPIC_COLORS[i % TOPIC_COLORS.length],
        profile: ru ? `Роль: ${role}` : `Role: ${role}`,
        needs: ru ? 'Поддержка и практические ответы' : 'Support and practical answers',
        interests: ru ? 'Работа, жильё, интеграция' : 'Work, housing, integration',
        pain: ru ? 'Неопределённость и бюрократия' : 'Uncertainty and bureaucracy',
        desc: ru ? `Сегмент «${role}» — важная часть сообщества.` : `The "${role}" segment is a key part of the community.`,
      }));
      app.personas = { en: mk(false), ru: mk(true) };
    }
  } catch {
    // Keep mock defaults.
  }

  try {
    const interestRows = asArray(raw.interests);
    if (interestRows.length > 0) {
      const sorted = interestRows
        .map((r: any) => ({ interest: asStr(r.topic), score: clamp(Math.round(asNum(r.users, 0) * 4), 5, 100) }))
        .sort((a: any, b: any) => b.score - a.score)
        .slice(0, 10);
      app.interests = { en: sorted, ru: sorted.map((s: any) => ({ ...s })) };
    }
  } catch {
    // Keep mock defaults.
  }

  try {
    const originRows = asArray(raw.origins);
    if (originRows.length > 0) {
      const byLanguage = new Map<string, number>();
      originRows.forEach((r: any) => {
        const lang = asStr(r.language, 'unknown').toLowerCase();
        byLanguage.set(lang, (byLanguage.get(lang) || 0) + asNum(r.count, 0));
      });
      const mapToCity: Record<string, { city: string; cityEN: string }> = {
        ru: { city: 'Москва', cityEN: 'Moscow' },
        en: { city: 'Лондон', cityEN: 'London' },
        hy: { city: 'Ереван', cityEN: 'Yerevan' },
      };
      const total = Math.max(1, Array.from(byLanguage.values()).reduce((s, n) => s + n, 0));
      app.origins = Array.from(byLanguage.entries()).map(([lang, count], i) => {
        const city = mapToCity[lang] || { city: `Город (${lang})`, cityEN: `City (${lang})` };
        return {
          city: city.city,
          cityEN: city.cityEN,
          count,
          pct: pct(count, total),
          color: TOPIC_COLORS[i % TOPIC_COLORS.length],
        };
      }).sort((a, b) => b.count - a.count);
    }
  } catch {
    // Keep mock defaults.
  }

  try {
    const totalUsers = Math.max(1, asNum(raw?.vitalityIndicators?.totalUsers, 1));
    const months = ['M1', 'M2', 'M3', 'M4', 'M5', 'M6'];
    const baseRussian = clamp(Math.round(totalUsers * 0.45), 1, totalUsers);
    const baseBilingual = clamp(Math.round(totalUsers * 0.25), 1, totalUsers);
    const baseLearning = clamp(Math.round(totalUsers * 0.2), 1, totalUsers);
    const baseIntegrated = clamp(Math.round(totalUsers * 0.1), 1, totalUsers);
    app.integrationData = months.map((m, i) => ({
      month: m,
      learning: Math.max(1, baseLearning + i * 3),
      bilingual: Math.max(1, baseBilingual + i * 2),
      russianOnly: Math.max(1, baseRussian - i * 4),
      integrated: Math.max(1, baseIntegrated + i * 4),
    }));
    const last = app.integrationData[app.integrationData.length - 1];
    const total = Math.max(1, last.learning + last.bilingual + last.russianOnly + last.integrated);
    app.integrationLevels = {
      en: [
        { level: 'Learning & Mixing', pct: pct(last.learning, total), color: '#3b82f6', desc: 'Actively integrating' },
        { level: 'Bilingual Bubble', pct: pct(last.bilingual, total), color: '#8b5cf6', desc: 'Operates in both languages' },
        { level: 'Russian Only', pct: pct(last.russianOnly, total), color: '#f59e0b', desc: 'Low integration' },
        { level: 'Fully Integrated', pct: pct(last.integrated, total), color: '#10b981', desc: 'High local integration' },
      ],
      ru: [
        { level: 'Учится и смешивается', pct: pct(last.learning, total), color: '#3b82f6', desc: 'Активно интегрируются' },
        { level: 'Двуязычный пузырь', pct: pct(last.bilingual, total), color: '#8b5cf6', desc: 'Используют оба языка' },
        { level: 'Только по-русски', pct: pct(last.russianOnly, total), color: '#f59e0b', desc: 'Низкая интеграция' },
        { level: 'Полностью интегрирован', pct: pct(last.integrated, total), color: '#10b981', desc: 'Высокая интеграция' },
      ],
    };
    app.integrationSeriesConfig = [
      { key: 'learning', color: '#3b82f6', label: 'Learning & Mixing', labelRu: 'Учится и смешивается', polarity: 'positive' },
      { key: 'bilingual', color: '#8b5cf6', label: 'Bilingual Bubble', labelRu: 'Двуязычный пузырь', polarity: 'neutral' },
      { key: 'russianOnly', color: '#f59e0b', label: 'Russian Only', labelRu: 'Только по-русски', polarity: 'negative' },
      { key: 'integrated', color: '#10b981', label: 'Fully Integrated', labelRu: 'Полностью интегрирован', polarity: 'positive' },
    ];
  } catch {
    // Keep mock defaults.
  }

  try {
    if (rawEmerging.length > 0) {
      const mk = (r: any, ru: boolean) => ({
        topic: asStr(r.topic),
        firstSeen: asStr(r.firstSeen).slice(0, 10),
        growthRate: clamp(Math.round(asNum(r.momentum, 0)), -50, 300),
        currentVolume: asNum(r.recentPosts, 0),
        originChannel: app.communityChannels[0]?.name || (ru ? 'Канал сообщества' : 'Community channel'),
        mood: asNum(r.momentum, 0) > 40 ? (ru ? 'Позитивный' : 'positive') : (ru ? 'Нейтральный' : 'neutral'),
        opportunity: asNum(r.momentum, 0) > 60 ? 'high' : asNum(r.momentum, 0) > 30 ? 'medium' : 'low',
      });
      app.emergingInterests = {
        en: rawEmerging.slice(0, 12).map((r: any) => mk(r, false)),
        ru: rawEmerging.slice(0, 12).map((r: any) => mk(r, true)),
      };
    }
  } catch {
    // Keep mock defaults.
  }

  try {
    const retentionRows = asArray(raw.retentionFactors);
    if (retentionRows.length > 0) {
      const total = Math.max(1, retentionRows.reduce((s: number, r: any) => s + asNum(r.retainedUsers, 0), 0));
      const mk = (r: any) => ({
        factor: `${asStr(r.topic)} participation`,
        score: clamp(Math.round(asNum(r.avgComments, 0) * 12), 10, 95),
        weight: clamp(Math.round((asNum(r.retainedUsers, 0) / total) * 100), 5, 60),
      });
      app.retentionFactors = { en: retentionRows.map(mk).slice(0, 8), ru: retentionRows.map(mk).slice(0, 8) };
    }
  } catch {
    // Keep mock defaults.
  }

  try {
    const churnRows = asArray(raw.churnSignals);
    if (churnRows.length > 0) {
      const mk = (r: any, ru: boolean) => {
        const comments = asNum(r.totalComments, 0);
        const trend = clamp(Math.round(comments / 2), 1, 40);
        return {
          signal: ru ? `Снижение активности в темах: ${asArray(r.topics).join(', ') || 'общие темы'}` : `Activity drop in: ${asArray(r.topics).join(', ') || 'general topics'}`,
          count: comments,
          trend,
          severity: trend >= 20 ? 'rising' : trend >= 10 ? 'watch' : 'stable',
        };
      };
      app.churnSignals = { en: churnRows.slice(0, 10).map((r: any) => mk(r, false)), ru: churnRows.slice(0, 10).map((r: any) => mk(r, true)) };
    }
  } catch {
    // Keep mock defaults.
  }

  try {
    const funnelRows = asArray(raw.growthFunnel);
    if (funnelRows.length > 0) {
      const byStage = Object.fromEntries(funnelRows.map((r: any) => [asStr(r.stage).toLowerCase(), asNum(r.users, 0)]));
      const all = Math.max(1, Object.values(byStage).reduce((s: number, n: any) => s + asNum(n, 0), 0));
      const reads = asNum(byStage.lurker, 0) + asNum(byStage.newcomer, 0);
      const asks = asNum(byStage.participant, 0);
      const helps = asNum(byStage.regular, 0);
      const contributes = Math.round(helps * 0.6);
      const leads = Math.max(1, Math.round(helps * 0.25));
      const mk = (ru: boolean) => [
        { stage: ru ? 'Все участники' : 'All Members', count: all, pct: 100, color: '#64748b', role: 'all' as const },
        { stage: ru ? 'Читает' : 'Reads', count: reads, pct: pct(reads, all), color: '#94a3b8', role: 'reads' as const },
        { stage: ru ? 'Задаёт вопросы' : 'Asks', count: asks, pct: pct(asks, all), color: '#3b82f6', role: 'asks' as const },
        { stage: ru ? 'Помогает' : 'Helps', count: helps, pct: pct(helps, all), color: '#10b981', role: 'helps' as const },
        { stage: ru ? 'Создаёт контент' : 'Contributes', count: contributes, pct: pct(contributes, all), color: '#8b5cf6', role: 'contributes' as const },
        { stage: ru ? 'Лидирует' : 'Leads', count: leads, pct: pct(leads, all), color: '#f59e0b', role: 'leads' as const },
      ];
      app.growthFunnel = { en: mk(false), ru: mk(true) };
    }
  } catch {
    // Keep mock defaults.
  }

  try {
    const dsRows = asArray(raw.decisionStages);
    if (dsRows.length > 0) {
      const total = Math.max(1, dsRows.reduce((s: number, r: any) => s + asNum(r.users, 0), 0));
      const colors = ['#3b82f6', '#8b5cf6', '#10b981', '#f59e0b', '#ef4444'];
      const mk = (r: any, i: number, ru: boolean) => ({
        stage: asStr(r.intent),
        count: asNum(r.users, 0),
        pct: pct(asNum(r.users, 0), total),
        trend: clamp(Math.round(asNum(r.users, 0) / 6), -20, 40),
        color: colors[i % colors.length],
        needs: ru ? 'Ясные инструкции и поддержка' : 'Clear guidance and support',
      });
      app.decisionStages = { en: dsRows.map((r: any, i: number) => mk(r, i, false)), ru: dsRows.map((r: any, i: number) => mk(r, i, true)) };
    }
  } catch {
    // Keep mock defaults.
  }

  try {
    const base = app.trendData.slice(-8);
    if (base.length > 0) {
      app.voiceData = base.map((row: any, i: number) => {
        const sum = Object.entries(row).reduce((s, [k, v]) => (k === 'week' ? s : s + asNum(v, 0)), 0);
        return {
          week: asStr(row.week, `W${i + 1}`),
          newVoices: Math.max(5, Math.round(sum * 0.12)),
          returning: Math.max(12, Math.round(sum * 0.28)),
        };
      });
    }
    app.topNewTopics.en = app.emergingInterests.en.slice(0, 6).map((e) => ({ topic: e.topic, newVoices: Math.max(5, Math.round(e.currentVolume / 2)), pct: clamp(Math.round(e.growthRate / 2), 1, 100) }));
    app.topNewTopics.ru = app.topNewTopics.en.map((e) => ({ ...e }));
  } catch {
    // Keep mock defaults.
  }

  try {
    const bizRows = asArray(raw.businessOpportunities);
    if (bizRows.length > 0) {
      const mk = (r: any, ru: boolean) => {
        const type = asStr(r.type, 'Opportunity');
        const signals = asNum(r.signals, 0);
        return {
          need: type,
          mentions: signals,
          growth: clamp(Math.round(signals * 1.2), -20, 120),
          sector: asStr(asArray(r.relatedTopics)[0], ru ? 'Общий' : 'General'),
          readiness: ru ? 'Подтверждено сообществом' : 'Community validated demand',
          sampleQuote: ru ? `Запрос на ${type} регулярно повторяется.` : `Requests for ${type} recur consistently.`,
          revenue: signals > 12 ? '$$$$' : signals > 8 ? '$$$' : signals > 4 ? '$$' : '$',
        };
      };
      app.businessOpportunities = { en: bizRows.map((r: any) => mk(r, false)).slice(0, 12), ru: bizRows.map((r: any) => mk(r, true)).slice(0, 12) };
    }
  } catch {
    // Keep mock defaults.
  }

  try {
    const jsRows = asArray(raw.jobSeeking);
    if (jsRows.length > 0) {
      const bySignal = new Map<string, number>();
      jsRows.forEach((r: any) => {
        const signal = asStr(r.signalType, 'Job_Seeking');
        bySignal.set(signal, (bySignal.get(signal) || 0) + 1);
      });
      const total = Math.max(1, Array.from(bySignal.values()).reduce((s, n) => s + n, 0));
      const items = Array.from(bySignal.entries()).map(([role, count]) => ({ role, count, pct: pct(count, total) })).sort((a, b) => b.count - a.count);
      app.jobSeeking = { en: items, ru: items.map((i) => ({ ...i })) };
    }
    const jtRows = asArray(raw.jobTrends);
    if (jtRows.length > 0) {
      const byTopic = new Map<string, number>();
      jtRows.forEach((r: any) => byTopic.set(asStr(r.topic, 'Work'), (byTopic.get(asStr(r.topic, 'Work')) || 0) + asNum(r.posts, 0)));
      const trends = Array.from(byTopic.entries()).sort((a, b) => b[1] - a[1]).slice(0, 5).map(([topic, posts], i) => ({
        trend: `Demand around ${topic} shows ${posts} tracked signals`,
        type: i === 0 ? 'hot' : i < 3 ? 'growing' : 'stable',
      }));
      app.jobTrends = {
        en: trends,
        ru: trends.map((t) => ({ trend: `Спрос по теме ${t.trend.replace('Demand around ', '').replace(' shows', ':')}`, type: t.type })),
      };
    }
  } catch {
    // Keep mock defaults.
  }

  try {
    const hRows = asArray(raw.housingData);
    if (hRows.length > 0) {
      const mkHousing = (r: any) => {
        const posts = asNum(r.posts, 0);
        const interactions = asNum(r.interactions, 0);
        return {
          type: asStr(r.topic),
          avgPrice: `$${300 + interactions * 12}/mo`,
          trend: clamp(Math.round(posts * 1.5), -20, 60),
          satisfaction: clamp(80 - Math.round(posts * 2.2), 10, 95),
          volume: posts,
        };
      };
      app.housingData.en = hRows.map(mkHousing);
      app.housingData.ru = hRows.map(mkHousing);
      app.housingHotTopics.en = app.housingData.en.map((h) => ({
        topic: h.type,
        count: h.volume,
        sentiment: h.trend > 10 ? 'angry' : h.trend > 4 ? 'worried' : 'seeking',
      }));
      app.housingHotTopics.ru = app.housingHotTopics.en.map((h) => ({ ...h }));
    }
  } catch {
    // Keep mock defaults.
  }

  try {
    const ws = asArray(raw.weeklyShifts)[0];
    if (ws) {
      const rows = [
        { metric: 'Posts', current: asNum(ws.thisWeekPosts, 0), previous: asNum(ws.lastWeekPosts, 0), unit: '', category: 'content' },
        { metric: 'Comments', current: asNum(ws.thisWeekComments, 0), previous: asNum(ws.lastWeekComments, 0), unit: '', category: 'content' },
        { metric: 'Active Users', current: asNum(ws.thisWeekUsers, 0), previous: Math.max(1, asNum(ws.thisWeekUsers, 0) - 5), unit: '', category: 'audience' },
        { metric: 'Churn Signals', current: app.churnSignals.en.length, previous: Math.max(1, app.churnSignals.en.length + 2), unit: '', category: 'risk', isInverse: true },
      ];
      app.weeklyShifts = { en: rows, ru: rows.map((r) => ({ ...r, metric: r.metric === 'Posts' ? 'Посты' : r.metric === 'Comments' ? 'Комментарии' : r.metric === 'Active Users' ? 'Активные пользователи' : 'Сигналы оттока' })) };
    }
  } catch {
    // Keep mock defaults.
  }

  try {
    if (rawSentimentByTopic.length > 0) {
      const byTopic = new Map<string, { positive: number; neutral: number; negative: number; volume: number }>();
      rawSentimentByTopic.forEach((r: any) => {
        const topic = asStr(r.topic);
        if (!byTopic.has(topic)) byTopic.set(topic, { positive: 0, neutral: 0, negative: 0, volume: 0 });
        const rec = byTopic.get(topic)!;
        const sentiment = asStr(r.sentiment).toLowerCase();
        const count = asNum(r.count, 0);
        rec.volume += count;
        if (sentiment.includes('positive')) rec.positive += count;
        else if (sentiment.includes('negative')) rec.negative += count;
        else rec.neutral += count;
      });
      const rows = Array.from(byTopic.entries()).map(([topic, s]) => {
        const total = Math.max(1, s.volume);
        return {
          topic,
          positive: pct(s.positive, total),
          neutral: pct(s.neutral, total),
          negative: pct(s.negative, total),
          volume: s.volume,
        };
      }).sort((a, b) => b.volume - a.volume);
      app.sentimentByTopic = { en: rows, ru: rows.map((r) => ({ ...r })) };
    }
  } catch {
    // Keep mock defaults.
  }

  try {
    if (rawTopPosts.length > 0) {
      const posts = rawTopPosts.slice(0, 20).map((p: any) => {
        const reactions = Math.max(0, asNum(p.views, 0) - asNum(p.forwards, 0));
        const comments = asNum(p.comments, 0);
        const shares = asNum(p.forwards, 0);
        return {
          title: asStr(p.text, '').slice(0, 80) || 'Post',
          type: asStr(asArray(p.topics)[0], asStr(p.channel, 'General')),
          shares,
          reactions,
          comments,
          engagement: Math.round((reactions * 0.3) + (comments * 2) + (shares * 3)),
        };
      });
      app.topPosts = { en: posts, ru: posts.map((p) => ({ ...p })) };
    }
  } catch {
    // Keep mock defaults.
  }

  try {
    const ctpRows = asArray(raw.contentTypePerformance);
    if (ctpRows.length > 0) {
      const rows = ctpRows.map((r: any) => ({
        type: asStr(r.mediaType || 'text'),
        avgEngagement: clamp(Math.round((asNum(r.avgViews, 0) / 25) + (asNum(r.avgForwards, 0) * 2)), 0, 100),
        count: asNum(r.count, 0),
      }));
      app.contentTypePerformance = { en: rows, ru: rows.map((r) => ({ ...r, type: CONTENT_TYPE_RU[r.type] || r.type })) };
    }
  } catch {
    // Keep mock defaults.
  }

  try {
    const v = raw?.vitalityIndicators;
    if (v && typeof v === 'object') {
      const indicators = [
        { indicator: 'Activity Rate', score: clamp(asNum(v.activityRate, 0), 0, 100), trend: 4, benchmark: 'Good', benchmarkLevel: 'good', emoji: '📈' },
        { indicator: 'User Vitality', score: clamp(Math.round((asNum(v.activeUsers7d, 0) / Math.max(1, asNum(v.totalUsers, 1))) * 100), 0, 100), trend: 3, benchmark: 'Above Avg', benchmarkLevel: 'above_avg', emoji: '👥' },
        { indicator: 'Discussion Depth', score: clamp(Math.round(asNum(v.avgCommentsPerPost, 0) * 12), 0, 100), trend: 2, benchmark: 'Average', benchmarkLevel: 'average', emoji: '💬' },
        { indicator: 'Topic Breadth', score: clamp(Math.round(asNum(v.totalTopics, 0) * 2), 0, 100), trend: 5, benchmark: 'Excellent', benchmarkLevel: 'excellent', emoji: '🧠' },
      ];
      app.vitalityIndicators = {
        en: indicators,
        ru: indicators.map((i) => ({ ...i, indicator: i.indicator === 'Activity Rate' ? 'Уровень активности' : i.indicator === 'User Vitality' ? 'Живость аудитории' : i.indicator === 'Discussion Depth' ? 'Глубина дискуссий' : 'Ширина тем', benchmark: i.benchmark === 'Good' ? 'Хорошо' : i.benchmark === 'Above Avg' ? 'Выше среднего' : i.benchmark === 'Average' ? 'Средне' : 'Отлично' })),
      };
    }
  } catch {
    // Keep mock defaults.
  }

  try {
    const topicRows = rawAllTopics;
    if (topicRows.length > 0) {
      const mergedTopicRows = new Map<string, any>();
      topicRows.forEach((row: any) => {
        const name = normalizeTopicLabel(row.name);
        if (!name) return;
        const key = topicKey(name);
        const existing = mergedTopicRows.get(key);
        if (!existing) {
          mergedTopicRows.set(key, { ...row, name });
          return;
        }
        mergedTopicRows.set(key, {
          ...existing,
          name,
          mentionCount: asNum(existing.mentionCount, asNum(existing.postCount, 0) + asNum(existing.commentCount, 0))
            + asNum(row.mentionCount, asNum(row.postCount, 0) + asNum(row.commentCount, 0)),
          postCount: asNum(existing.postCount, 0) + asNum(row.postCount, 0),
          commentCount: asNum(existing.commentCount, 0) + asNum(row.commentCount, 0),
          last7Mentions: asNum(existing.last7Mentions, 0) + asNum(row.last7Mentions, 0),
          prev7Mentions: asNum(existing.prev7Mentions, 0) + asNum(row.prev7Mentions, 0),
          totalInteractions: asNum(existing.totalInteractions, 0) + asNum(row.totalInteractions, 0),
          evidence: [...asArray(existing.evidence), ...asArray(row.evidence)].slice(0, 6),
          category: asStr(existing.category || row.category, 'General'),
          description: asStr(existing.description || row.description, ''),
          descriptionRu: asStr(existing.descriptionRu || row.descriptionRu || existing.description || row.description, ''),
        });
      });

      const dedupedTopicRows = Array.from(mergedTopicRows.values());

      const sentimentMap = new Map<string, any>((app.sentimentByTopic.en || []).map((s) => [topicKey(s.topic), s]));
      const sentimentRows = asArray<any>(app.sentimentByTopic.en);
      const sentimentTotalVolume = sentimentRows.reduce((sum, s) => sum + Math.max(0, asNum(s.volume, 0)), 0);
      const globalSentiment = sentimentTotalVolume > 0
        ? {
            positive: clamp(Math.round(sentimentRows.reduce((sum, s) => sum + (asNum(s.positive, 0) * asNum(s.volume, 0)), 0) / sentimentTotalVolume), 0, 100),
            neutral: clamp(Math.round(sentimentRows.reduce((sum, s) => sum + (asNum(s.neutral, 0) * asNum(s.volume, 0)), 0) / sentimentTotalVolume), 0, 100),
            negative: clamp(Math.round(sentimentRows.reduce((sum, s) => sum + (asNum(s.negative, 0) * asNum(s.volume, 0)), 0) / sentimentTotalVolume), 0, 100),
          }
        : { positive: 50, neutral: 30, negative: 20 };

      const trendWeeks = Array.from(new Set(rawTrendRows.map((r: any) => `${asNum(r.year, 0)}-W${String(asNum(r.week, 0)).padStart(2, '0')}`)))
        .sort((a, b) => a.localeCompare(b))
        .slice(-6);
      const trendByTopic = new Map<string, Map<string, number>>();
      rawTrendRows.forEach((r: any) => {
        const topic = normalizeTopicLabel(r.topic);
        const week = `${asNum(r.year, 0)}-W${String(asNum(r.week, 0)).padStart(2, '0')}`;
        if (!topic || !week) return;
        const key = topicKey(topic);
        if (!trendByTopic.has(key)) trendByTopic.set(key, new Map<string, number>());
        trendByTopic.get(key)!.set(week, asNum(r.posts, 0));
      });

      app.allTopics = dedupedTopicRows.map((t: any, i: number) => {
        const name = normalizeTopicLabel(t.name) || `Topic ${i + 1}`;
        const estimateWarnings: string[] = [];

        let mentions = asNum(t.mentionCount, Number.NaN);
        if (!Number.isFinite(mentions)) {
          const posts = asNum(t.postCount, 0);
          const comments = asNum(t.commentCount, Number.NaN);
          if (Number.isFinite(comments)) {
            mentions = posts + comments;
            estimateWarnings.push('Mentions estimated from post/comment counts');
          } else {
            mentions = posts;
            estimateWarnings.push('Mentions include posts only (comment data unavailable)');
          }
        }

        const sentimentSource = sentimentMap.get(topicKey(name));
        const sentiment = sentimentSource || globalSentiment;
        if (!sentimentSource) {
          estimateWarnings.push('Sentiment estimated from global baseline');
        }

        const perTopicTrend = trendByTopic.get(topicKey(name));
        let trendSeries = trendWeeks.map((week) => ({
          week,
          count: perTopicTrend ? asNum(perTopicTrend.get(week), 0) : 0,
        }));
        if (trendSeries.length === 0 || trendSeries.every((point) => point.count === 0)) {
          trendSeries = [];
          estimateWarnings.push('Weekly trend unavailable for this topic');
        }

        const backendEvidence = asArray(t.evidence)
          .map((ev: any, idx: number) => {
            const evTypeRaw = asStr(ev.type, 'message').toLowerCase();
            const evType = evTypeRaw === 'post' ? 'message' : evTypeRaw;
            return {
              id: asStr(ev.id, `${slugify(name)}-ev-${idx}`),
              type: (evType === 'message' || evType === 'reply' || evType === 'reaction') ? evType : 'message',
              author: asStr(ev.author, 'unknown'),
              channel: asStr(ev.channel, 'unknown'),
              text: asStr(ev.text, '').slice(0, 500),
              timestamp: asStr(ev.timestamp, new Date().toISOString()),
              reactions: Math.max(0, asNum(ev.reactions, 0)),
              replies: Math.max(0, asNum(ev.replies, 0)),
            };
          })
          .filter((ev: any) => ev.text.length > 0)
          .slice(0, 6);

        const backendQuestionEvidence = asArray(t.questionEvidence)
          .map((ev: any, idx: number) => {
            const evTypeRaw = asStr(ev.type, 'message').toLowerCase();
            const evType = evTypeRaw === 'post' ? 'message' : evTypeRaw;
            return {
              id: asStr(ev.id, `${slugify(name)}-qev-${idx}`),
              type: (evType === 'message' || evType === 'reply' || evType === 'reaction') ? evType : 'message',
              author: asStr(ev.author, 'unknown'),
              channel: asStr(ev.channel, 'unknown'),
              text: asStr(ev.text, '').slice(0, 500),
              timestamp: asStr(ev.timestamp, new Date().toISOString()),
              reactions: Math.max(0, asNum(ev.reactions, 0)),
              replies: Math.max(0, asNum(ev.replies, 0)),
            };
          })
          .filter((ev: any) => ev.text.length > 0)
          .slice(0, 12);

        const evidence = backendEvidence.length > 0
          ? backendEvidence
          : [];
        const questionEvidence = backendQuestionEvidence.length > 0
          ? backendQuestionEvidence
          : evidence.filter((ev: any) => asStr(ev.text).includes('?'));

        const channelCounts = new Map<string, number>();
        evidence.forEach((ev: any) => {
          const channel = asStr(ev.channel, '').trim();
          if (!channel) return;
          channelCounts.set(channel, (channelCounts.get(channel) || 0) + 1);
        });
        const topChannelsFromEvidence = Array.from(channelCounts.entries())
          .sort((a, b) => b[1] - a[1])
          .slice(0, 3)
          .map(([channel]) => channel);
        const topChannels = topChannelsFromEvidence;

        let growth = asNum(t.growth7dPct, Number.NaN);
        if (!Number.isFinite(growth)) {
          const last7Mentions = asNum(t.last7Mentions, Number.NaN);
          const prev7Mentions = asNum(t.prev7Mentions, Number.NaN);
          if (Number.isFinite(last7Mentions) && Number.isFinite(prev7Mentions) && prev7Mentions > 0) {
            growth = Math.round(((last7Mentions - prev7Mentions) / prev7Mentions) * 100);
            estimateWarnings.push('Growth estimated from 7d period deltas');
          } else {
            growth = clamp(Math.round(asNum(t.totalInteractions, 0) / Math.max(1, mentions) * 10), -30, 200);
            estimateWarnings.push('Growth estimated from interaction intensity');
          }
        } else {
          growth = Math.round(growth);
        }

        return {
          id: slugify(name),
          name,
          nameRu: name,
          category: asStr(t.category, 'General'),
          color: hashColor(asStr(t.category, 'General')),
          mentions,
          growth: clamp(growth, -200, 200),
          sentiment,
          weeklyData: trendSeries,
          topChannels,
          description: asStr(t.description, ''),
          descriptionRu: asStr(t.descriptionRu, asStr(t.description, '')),
          evidence,
          questionEvidence,
          estimateWarnings: estimateWarnings.length > 0 ? Array.from(new Set(estimateWarnings)) : undefined,
        };
      });
    }
  } catch {
    // Keep mock defaults.
  }

  try {
    const channelRows = asArray(raw.allChannels);
    if (channelRows.length > 0) {
      app.allChannels = channelRows.map((c: any, i: number) => {
        const chName = asStr(c.title || c.username, `Channel ${i + 1}`);
        const weeklyMap = new Map<number, number>();
        asArray(c.weeklyRows).forEach((row: any) => {
          const dow = asNum(row.dow, Number.NaN);
          if (!Number.isFinite(dow)) return;
          weeklyMap.set(Math.round(dow), Math.max(0, asNum(row.count, 0)));
        });
        const weeklyData = DOW_EN.map((day, idx) => ({ day, msgs: weeklyMap.get(idx + 1) || 0 }));

        const hourlyMap = new Map<number, number>();
        asArray(c.hourlyRows).forEach((row: any) => {
          const hour = asNum(row.hour, Number.NaN);
          if (!Number.isFinite(hour)) return;
          hourlyMap.set(Math.round(hour), Math.max(0, asNum(row.count, 0)));
        });
        const hourlyData = Array.from({ length: 24 }).map((_, h) => ({
          hour: `${String(h).padStart(2, '0')}:00`,
          msgs: hourlyMap.get(h) || 0,
        }));

        const topTopics = asArray(c.topTopics)
          .map((t: any) => ({
            name: asStr(t.name, ''),
            mentions: Math.max(0, asNum(t.mentions, 0)),
            pct: clamp(Math.max(0, asNum(t.pct, 0)), 0, 100),
          }))
          .filter((t: any) => t.name.length > 0)
          .slice(0, 6);

        const topVoices = asArray(c.topVoices)
          .map((v: any) => ({
            name: asStr(v.name, ''),
            posts: Math.max(0, asNum(v.posts, 0)),
            helpScore: clamp(Math.max(0, asNum(v.helpScore, 0)), 0, 100),
          }))
          .filter((v: any) => v.name.length > 0)
          .slice(0, 4);

        const recentPosts = asArray(c.recentPosts)
          .map((p: any, idx: number) => ({
            id: asStr(p.id, `${slugify(chName)}-${idx}`),
            author: asStr(p.author, asStr(c.username, chName)),
            text: asStr(p.text, '').slice(0, 220),
            timestamp: asStr(p.timestamp, ''),
            reactions: Math.max(0, asNum(p.reactions, 0)),
            replies: Math.max(0, asNum(p.replies, 0)),
          }))
          .filter((p: any) => p.text.length > 0)
          .slice(0, 6);

        const messageTypes = asArray(c.messageTypes)
          .map((m: any) => ({
            type: asStr(m.type, 'text'),
            count: Math.max(0, asNum(m.count, 0)),
            pct: 0,
          }))
          .slice(0, 6);
        const totalMessageTypeCount = messageTypes.reduce((sum: number, m: any) => sum + m.count, 0);
        const messageTypesWithPct = messageTypes.map((m: any) => ({
          ...m,
          pct: totalMessageTypeCount > 0 ? clamp(Math.round((m.count / totalMessageTypeCount) * 100), 0, 100) : 0,
        }));

        const postsCount = Math.max(0, asNum(c.postCount, 0));
        const memberCount = Math.max(0, asNum(c.memberCount, 0));
        const avgViews = Math.max(0, asNum(c.avgViews, 0));
        return {
          id: asStr(c.username, slugify(chName)),
          name: chName,
          type: 'General',
          members: memberCount,
          dailyMessages: Math.max(0, asNum(c.dailyMessages, Math.round(postsCount / 30))),
          engagement: clamp(Math.round(avgViews / 25), 0, 100),
          growth: clamp(Math.round(asNum(c.growth7dPct, 0)), -200, 300),
          topTopic: topTopics[0]?.name || '',
          description: asStr(c.description, ''),
          weeklyData,
          hourlyData,
          topTopics,
          sentimentBreakdown: {
            positive: clamp(Math.max(0, asNum(c.sentimentPositive, 0)), 0, 100),
            neutral: clamp(Math.max(0, asNum(c.sentimentNeutral, 0)), 0, 100),
            negative: clamp(Math.max(0, asNum(c.sentimentNegative, 0)), 0, 100),
          },
          messageTypes: messageTypesWithPct,
          topVoices,
          recentPosts,
        };
      });
    }
  } catch {
    // Keep mock defaults.
  }

  try {
    const audienceRows = asArray(raw.allAudience);
    if (audienceRows.length > 0) {
      app.allAudience = audienceRows.map((u: any, i: number) => {
        const genderRaw = asStr(u.gender, 'Unknown').toLowerCase();
        const gender = genderRaw.includes('female') ? 'Female' : genderRaw.includes('male') ? 'Male' : 'Unknown';
        const channels = asArray(u.channels)
          .map((ch: any) => ({
            name: asStr(ch.name, ''),
            type: asStr(ch.type, 'General'),
            role: asStr(ch.role, 'Member'),
            messageCount: Math.max(0, asNum(ch.messageCount, 0)),
          }))
          .filter((ch: any) => ch.name.length > 0)
          .slice(0, 3);

        let topTopics = asArray(u.topTopics).map((t: any) => ({
          name: asStr(t.name, ''),
          count: Math.max(0, asNum(t.count, 0)),
        })).filter((t: any) => t.name.length > 0).slice(0, 5);
        if (topTopics.length === 0) {
          topTopics = asArray<string>(u.topics).slice(0, 5).map((name) => ({ name, count: 0 }));
        }

        const userId = asStr(u.userId, `u-${i + 1}`);
        const language = asStr(u.language, 'unknown');
        const role = asStr(u.role, 'Member');
        const commentCount = Math.max(0, asNum(u.commentCount, 0));

        const recentMessages = asArray(u.recentMessages)
          .map((m: any) => ({
            text: asStr(m.text, '').slice(0, 220),
            channel: asStr(m.channel, ''),
            timestamp: asStr(m.timestamp, ''),
            reactions: Math.max(0, asNum(m.reactions, 0)),
            replies: Math.max(0, asNum(m.replies, 0)),
          }))
          .filter((m: any) => m.text.length > 0)
          .slice(0, 4);

        const activityData = asArray(u.activityData)
          .map((point: any) => ({
            week: asStr(point.week, ''),
            msgs: Math.max(0, asNum(point.msgs, 0)),
          }))
          .filter((point: any) => point.week.length > 0)
          .slice(-6);

        const username = userId ? (String(userId).startsWith('@') ? String(userId) : `@${userId}`) : '';

        return {
          id: userId,
          username,
          displayName: userId,
          gender: gender as 'Male' | 'Female' | 'Unknown',
          age: asStr(u.age, 'Unknown'),
          origin: language.toUpperCase(),
          location: '',
          joinedDate: '',
          lastActive: asStr(u.lastSeen, '').slice(0, 10),
          totalMessages: commentCount,
          totalReactions: 0,
          helpScore: clamp(Math.round(commentCount * 3), 0, 100),
          interests: topTopics.map((t) => t.name),
          channels,
          topTopics,
          sentiment: {
            positive: clamp(Math.max(0, asNum(u.sentimentPositive, 0)), 0, 100),
            neutral: clamp(Math.max(0, asNum(u.sentimentNeutral, 0)), 0, 100),
            negative: clamp(Math.max(0, asNum(u.sentimentNegative, 0)), 0, 100),
          },
          activityData,
          recentMessages,
          persona: role,
          integrationLevel: '',
        };
      });
    }
  } catch {
    // Keep mock defaults.
  }

  return app;
}
