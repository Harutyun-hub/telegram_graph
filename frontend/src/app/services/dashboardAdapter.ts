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
  'Government & Leadership': 'Государственное управление и лидерство',
  'Opposition & Protest': 'Оппозиция и протестная активность',
  'Nagorno-Karabakh & Artsakh': 'Нагорный Карабах и Арцах',
  'Global Conflict': 'Международные конфликты',
  'National Identity': 'Национальная идентичность',
  'Regional Security': 'Региональная безопасность',
  Employment: 'Рынок труда',
  'Democracy & Reform': 'Демократия и реформы',
  'Military & Defense': 'Армия и оборона',
  'Business & Enterprise': 'Бизнес и предпринимательство',
  'Arts & Entertainment': 'Культура и развлечения',
  'Macroeconomic Condition': 'Макроэкономическая ситуация',
  Emigration: 'Эмиграция',
  'Geopolitical Alignment': 'Геополитическая ориентация',
  'Financial System': 'Финансовая система',
  Education: 'Образование',
  Religion: 'Религия',
  'Social Services': 'Социальная поддержка',
  'Media Landscape': 'Медийная среда',
  'Information Integrity': 'Информационная достоверность',
  'Community Life': 'Жизнь сообщества',
  'Housing & Infrastructure': 'Жилье и инфраструктура',
};
const WORK_SIGNAL_LABELS_EN: Record<string, string> = {
  Job_Seeking: 'Job seeking',
  Hiring: 'Hiring',
  Partnership_Request: 'Partnership requests',
};
const WORK_SIGNAL_LABELS_RU: Record<string, string> = {
  Job_Seeking: 'Поиск работы',
  Hiring: 'Найм',
  Partnership_Request: 'Поиск партнерства',
};

const TOPIC_RU: Record<string, string> = {
  'Political Protest': 'Политические протесты',
  'Armenian Government Performance': 'Оценка работы правительства Армении',
  'Nagorno-Karabakh Conflict': 'Нагорно-Карабахский конфликт',
  'Middle East Conflict': 'Конфликт на Ближнем Востоке',
  'Armenian National Identity': 'Армянская национальная идентичность',
  'Armenian Genocide Remembrance': 'Память о Геноциде армян',
  'Armenian Opposition Movement': 'Армянское оппозиционное движение',
  'Azerbaijan Aggression': 'Агрессия Азербайджана',
  'Armenian Border Security': 'Безопасность границы Армении',
  'Political Prisoner': 'Политические заключенные',
  'Job Market Condition': 'Ситуация на рынке труда',
  'Collective Memory': 'Коллективная память',
  'Electoral Integrity': 'Честность выборов',
  'Armenian Armed Force': 'Вооруженные силы Армении',
  'Business Opportunity': 'Возможности для бизнеса',
  'Iranian-Armenian Relation': 'Ирано-армянские отношения',
  'Artsakh Sovereignty': 'Суверенитет Арцаха',
  'Cultural Festival': 'Культурный фестиваль',
  'Economic Growth': 'Экономический рост',
  'Visa And Residency': 'Визы и ВНЖ',
  'Pro-Western Orientation': 'Проевропейская ориентация',
  'Russian Sanctions Impact': 'Влияние антироссийских санкций',
  'Banking Sector': 'Банковский сектор',
  'School System Quality': 'Качество школьного образования',
  'Personal Insult': 'Личное оскорбление',
  'Armenian Political Discourse': 'Армянская политическая дискуссия',
  'Cognitive Style': 'Когнитивный стиль',
  'Fear Los': 'Страх потерь',
  'Language And Identity': 'Язык и идентичность',
  'Political Candidate Criticism': 'Критика политических кандидатов',
  'Information Clarity': 'Ясность информации',
  'Iranian Response': 'Реакция Ирана',
  'Ukraine Conflict': 'Конфликт в Украине',
  'Public Opinion Poll': 'Опрос общественного мнения',
  'Arts Entertainment': 'Культура и развлечения',
  'Genetic Testing': 'Генетическое тестирование',
  'Political Mythology': 'Политическая мифология',
  'Analytical Critique': 'Аналитическая критика',
  'Social Media Trend': 'Тренды в социальных сетях',
  'Russian Propaganda': 'Российская пропаганда',
  'Anti-Corruption Effort': 'Антикоррупционная повестка',
  'South Caucasus Stability': 'Стабильность Южного Кавказа',
  'Military Tactic': 'Военная тактика',
  'Multi-Vector Foreign Policy': 'Многовекторная внешняя политика',
  'Conspiracy Theory': 'Теория заговора',
  'Armenian Geopolitical Strategy': 'Геополитическая стратегия Армении',
  'Iranian Political Landscape': 'Политический ландшафт Ирана',
  'Iranian Military Action': 'Военные действия Ирана',
  'Political Insult': 'Политические оскорбления',
  'Iranian Political Climate': 'Политический климат Ирана',
  'War Commentary': 'Военные комментарии',
  'American Propaganda': 'Американская пропаганда',
  'Mental Health': 'Психическое здоровье',
  'Armenian Dram Exchange': 'Курс армянского драма',
  'Political Commentary': 'Политические комментарии',
  'Military Support': 'Военная поддержка',
  'Interfaith Dialogue': 'Межконфессиональный диалог',
  'Constitutional Reform': 'Конституционная реформа',
  'Post-Soviet Identity': 'Постсоветская идентичность',
  'Ukrainian Conflict': 'Украинский конфликт',
  'Culinary Culture': 'Кулинарная культура',
  'Telegram Community': 'Телеграм-сообщество',
  'Housing Market': 'Рынок жилья',
  'Family Life': 'Семейная жизнь',
  'Banking Service': 'Банковские услуги',
  'Military Conflict': 'Военный конфликт',
  'Social Infrastructure': 'Социальная инфраструктура',
  'Political Tension': 'Политическая напряжённость',
  'Public Space': 'Общественное пространство',
  'Religious Sentiment': 'Религиозные настроения',
  'Government Criticism': 'Критика правительства',
  'Political Disillusionment': 'Политическое разочарование',
  'Iranian Political Discourse': 'Политический дискурс Ирана',
  'Bot Activity': 'Активность ботов',
  'Historical Claim': 'Исторические заявления',
  'Urban Development': 'Городское развитие',
  'Migration Intent': 'Миграционные намерения',
  'Tech Startup': 'Технологические стартапы',
  'Geopolitical Discontent': 'Геополитическое недовольство',
  'Armenian-Turkish Relation': 'Армяно-турецкие отношения',
  'Evil Force': 'Враждебные силы',
  'Iranian Conflict': 'Иранский конфликт',
  'Healthcare Access': 'Доступ к здравоохранению',
  'Economic Sanctions': 'Экономические санкции',
  'Military Recruitment': 'Военный призыв',
  'Disinformation Campaign': 'Кампания дезинформации',
  'Orthodox Christianity': 'Православное христианство',
  'Russian Expat Community': 'Русскоязычная экспат-среда',
  'Armenian Diaspora Identity': 'Идентичность армянской диаспоры',
};
const DOW_EN = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];
const DOW_RU = ['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс'];
const MIN_SUPPORT_FOR_TREND = 8;
const COMPARE_BUCKET_COUNT = 7;
const MIN_SUPPORT_FOR_QA = 5;
const MAX_ABS_TREND_PCT = 100;

const HEALTH_LABEL_RU: Record<string, string> = {
  'Constructive Intent': 'Конструктивный интент',
  'Emotional Stability': 'Эмоциональная стабильность',
  'Discussion Diversity': 'Разнообразие дискуссий',
  'Conversation Depth': 'Глубина обсуждений',
};

const HEALTH_DESC_RU: Record<string, string> = {
  'Share of constructive intent in analyzed messages': 'Доля конструктивного интента в проанализированных сообщениях',
  'Inverse of negative-intent pressure': 'Обратный показатель давления негативного интента',
  'How concentrated discussions are around few topics': 'Насколько обсуждения сосредоточены вокруг малого числа тем',
  'Comment-scope depth per analyzed post': 'Глубина комментариев на один проанализированный пост',
};

const NOISY_TOPIC_KEYS = new Set(['', 'null', 'unknown', 'none', 'n/a', 'na']);

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

export function createEmptyAppData(): AppData {
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

function translateTopicRu(topicRaw: any): string {
  const topic = normalizeTopicLabel(topicRaw);
  if (!topic) return topic;

  const exact = TOPIC_RU[topic];
  if (exact) return exact;

  const normalized = topic.toLowerCase();
  if (normalized === 'null' || normalized === 'unknown' || normalized === 'none') {
    return 'Неопределенная тема';
  }

  return topic;
}

function translateCategory(categoryRaw: any, ru: boolean): string {
  const category = asStr(categoryRaw, 'General');
  if (!ru) return category;
  return CATEGORY_RU[category] || category;
}

function topicKey(v: any): string {
  return normalizeTopicLabel(v).toLowerCase();
}

function snippet(v: any, maxLen = 180): string {
  const text = asStr(v, '').replace(/\s+/g, ' ').trim();
  if (!text) return '';
  return text.length > maxLen ? `${text.slice(0, maxLen - 1)}…` : text;
}

function questionSnippet(v: any, maxLen = 160): string {
  const text = asStr(v, '').replace(/\s+/g, ' ').trim();
  if (!text) return '';

  const qPos = text.indexOf('?');
  if (qPos < 0) return '';

  const priorStops = [
    text.lastIndexOf('.', qPos),
    text.lastIndexOf('!', qPos),
    text.lastIndexOf('?', qPos - 1),
  ].filter((n) => n >= 0);
  const start = priorStops.length ? Math.max(...priorStops) + 1 : 0;

  const nextStops = [
    text.indexOf('?', qPos + 1),
    text.indexOf('.', qPos + 1),
    text.indexOf('!', qPos + 1),
  ].filter((n) => n >= 0);
  const end = nextStops.length ? Math.min(...nextStops) : qPos;

  const focused = text.slice(start, end + 1).trim();
  const fallback = text.slice(Math.max(0, qPos - 90), Math.min(text.length, qPos + 70)).trim();
  const candidate = focused.length >= 12 ? focused : fallback;
  return snippet(candidate, maxLen);
}

function clamp(n: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, n));
}

function pct(part: number, total: number): number {
  if (!total) return 0;
  return Math.round((part / total) * 100);
}

function formatWorkSignalLabel(signalRaw: any, ru = false): string {
  const signal = asStr(signalRaw, '').trim();
  if (!signal) return ru ? 'Рабочий сигнал' : 'Work signal';
  return ru
    ? (WORK_SIGNAL_LABELS_RU[signal] ?? signal)
    : (WORK_SIGNAL_LABELS_EN[signal] ?? signal);
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

function formatTrendBucketLabel(bucketRaw: any): string {
  const bucket = asStr(bucketRaw, '').trim();
  if (!bucket) return '';

  const isoDaily = /^(\d{4})-(\d{2})-(\d{2})$/;
  const weekly = /^(\d{4})-W(\d{2})$/;
  const dailyMatch = bucket.match(isoDaily);
  if (dailyMatch) {
    const [, year, month, day] = dailyMatch;
    return `${day}.${month}`;
  }
  const weeklyMatch = bucket.match(weekly);
  if (weeklyMatch) {
    return `${weeklyMatch[1]}-W${weeklyMatch[2]}`;
  }
  return bucket;
}

function unwrapPayload(payload: any): any {
  if (payload && typeof payload === 'object' && 'data' in payload && payload.data) {
    return payload.data;
  }
  return payload;
}

function stageStyle(stageRaw: string) {
  const stage = stageRaw.toLowerCase();
  if (stage.includes('grow') || stage.includes('emerg') || stage.includes('ris')) {
    return { key: 'growing', en: 'Growing', ru: 'Рост', color: '#3b82f6', bgColor: 'bg-blue-50', borderColor: 'border-blue-200', textColor: 'text-blue-700', descEn: 'uptrend', descRu: 'рост' };
  }
  return { key: 'declining', en: 'Declining', ru: 'Снижение', color: '#f59e0b', bgColor: 'bg-amber-50', borderColor: 'border-amber-200', textColor: 'text-amber-700', descEn: 'downtrend', descRu: 'снижение' };
}

export function adaptDashboardPayload(payload: any): AppData {
  const raw = unwrapPayload(payload) || {};
  const selectedRangeDays = Math.max(1, asNum(payload?.meta?.days, 1));
  const app = createEmptyAppData();

  const rawTrending = asArray(raw.trendingTopics);
  const rawTrendingNew = asArray(raw.trendingNewTopics);
  const rawTopicBubbles = asArray(raw.topicBubbles);
  const rawTrendRows = asArray(raw.trendLines);
  const rawLifecycle = asArray(raw.lifecycleStages);
  const rawChannels = asArray(raw.communityChannels);
  const rawKeyVoices = asArray(raw.keyVoices);
  const rawEmerging = asArray(raw.emergingInterests);
  const rawNewVsReturningWidget = raw && typeof raw.newVsReturningVoiceWidget === 'object' && raw.newVsReturningVoiceWidget
    ? raw.newVsReturningVoiceWidget
    : null;
  const rawTopPosts = asArray(raw.topPosts);
  const rawSentimentByTopic = asArray(raw.sentimentByTopic);
  const rawAllTopics = asArray(raw.allTopics);

  const topicEvidenceTextByTopic = new Map<string, string>();
  const topicQuestionEvidenceByTopic = new Map<string, string[]>();
  const topicLifecycleDetailsByTopic = new Map<string, {
    summary: string;
    topChannels: string[];
    evidence: { text: string; channel: string; timestamp: string }[];
  }>();
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
      ...questionRows.map((ev: any) => questionSnippet(ev?.text, 180)),
      ...evidenceRows.map((ev: any) => questionSnippet(ev?.text, 180)),
    ].filter((text: string) => text.length > 0);
    if (questionSnippets.length > 0) {
      topicQuestionEvidenceByTopic.set(key, questionSnippets.slice(0, 10));
    }

    const combinedEvidence = [...questionRows, ...evidenceRows]
      .map((ev: any) => ({
        text: snippet(ev?.text, 260),
        channel: asStr(ev?.channel, 'unknown'),
        timestamp: asStr(ev?.timestamp, ''),
      }))
      .filter((ev: any) => ev.text.length > 0)
      .slice(0, 4);

    const channelCounts = new Map<string, number>();
    [...questionRows, ...evidenceRows].forEach((ev: any) => {
      const channel = asStr(ev?.channel, '').trim();
      if (!channel) return;
      channelCounts.set(channel, (channelCounts.get(channel) || 0) + 1);
    });
    const topChannels = Array.from(channelCounts.entries())
      .sort((a, b) => b[1] - a[1])
      .slice(0, 3)
      .map(([channel]) => channel);

    if (combinedEvidence.length > 0 || topChannels.length > 0) {
      topicLifecycleDetailsByTopic.set(key, {
        summary: combinedEvidence[0]?.text || firstEvidence || '',
        topChannels,
        evidence: combinedEvidence,
      });
    }
  });

  try {
    const score = clamp(asNum(raw?.communityHealth?.score, app.communityHealth.currentScore), 0, 100);
    const previousScore = clamp(
      asNum(raw?.communityHealth?.previousScore, asNum(raw?.communityHealth?.weekAgoScore, score)),
      0,
      100,
    );
    app.communityHealth.currentScore = score;
    app.communityHealth.weekAgoScore = previousScore;

    const rawHistory = asArray(raw?.communityHealth?.history);
    if (rawHistory.length > 0) {
      app.communityHealth.history = rawHistory.slice(0, 7).map((row: any, i: number) => ({
        time: asStr(row?.time, i === 6 ? 'Now' : `${6 - i}h ago`),
        score: clamp(asNum(row?.score, previousScore), 0, 100),
      }));
    } else {
      app.communityHealth.history = Array.from({ length: 7 }).map((_, i) => ({
        time: i === 6 ? 'Now' : `${6 - i}h ago`,
        score: clamp(Math.round(previousScore + ((score - previousScore) * (i / 6))), 0, 100),
      }));
    }

    const rawComponents = asArray(raw?.communityHealth?.components);
    if (rawComponents.length > 0) {
      app.communityHealth.components = {
        en: rawComponents.slice(0, 4).map((comp: any) => ({
          label: asStr(comp?.label, 'Signal'),
          value: clamp(asNum(comp?.value, 0), 0, 100),
          trend: Math.round(asNum(comp?.trend, 0)),
          desc: asStr(comp?.desc, ''),
        })),
        ru: rawComponents.slice(0, 4).map((comp: any) => {
          const labelEn = asStr(comp?.label, 'Signal');
          const descEn = asStr(comp?.desc, '');
          return {
            label: HEALTH_LABEL_RU[labelEn] || labelEn,
            value: clamp(asNum(comp?.value, 0), 0, 100),
            trend: Math.round(asNum(comp?.trend, 0)),
            desc: HEALTH_DESC_RU[descEn] || descEn,
          };
        }),
      };
    }
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
        const currentMentions = asNum(row.currentMentions, mentions);
        const previousMentions = asNum(row.previousMentions, 0);
        const computedTrend = boundedTrend(currentMentions, previousMentions);
        const growthSupport = asNum(row.growthSupport, computedTrend.support);
        const backendTrend = asNum(row.trendPct, Number.NaN);
        const backendReliable = Boolean(row.trendReliable) && Number.isFinite(backendTrend) && growthSupport >= MIN_SUPPORT_FOR_TREND;
        const quoteFromEvidence = snippet(row.sampleQuote, 180) || topicEvidenceTextByTopic.get(key) || topicQuestionEvidenceByTopic.get(key)?.[0] || '';
        return {
          id: i + 1,
          topic: ru ? translateTopicRu(topic) : topic,
          sourceTopic: topic,
          mentions,
          deltaMentions: currentMentions - previousMentions,
          trend: backendReliable
            ? clamp(Math.round(backendTrend), -MAX_ABS_TREND_PCT, MAX_ABS_TREND_PCT)
            : computedTrend.value,
          trendReliable: backendReliable || computedTrend.reliable,
          growthSupport,
          category: translateCategory(category, ru),
          sentiment: sentiments[i % sentiments.length],
          sampleQuote: quoteFromEvidence,
        };
      };
    const filteredTrending = rawTrending.filter((row: any) => {
      const topic = normalizeTopicLabel(row?.name || row?.topic);
      if (!topic) return false;
      if (NOISY_TOPIC_KEYS.has(topic.toLowerCase())) return false;
      return asNum(row?.mentions || row?.postMentions || row?.totalPosts, 0) > 0;
    });

    if (filteredTrending.length > 0) {
      app.trendingTopics.en = filteredTrending.slice(0, 12).map((r: any, i: number) => toTopic(r, i, false));
      app.trendingTopics.ru = filteredTrending.slice(0, 12).map((r: any, i: number) => toTopic(r, i, true));
    }

    if (rawTrendingNew.length > 0) {
      app.trendingNewTopics.en = rawTrendingNew.slice(0, 12).map((r: any, i: number) => toTopic(r, i, false));
      app.trendingNewTopics.ru = rawTrendingNew.slice(0, 12).map((r: any, i: number) => toTopic(r, i, true));
    } else if (rawEmerging.length > 0) {
      app.trendingNewTopics.en = rawEmerging.slice(0, 12).map((r: any, i: number) => toTopic(r, i, false));
      app.trendingNewTopics.ru = rawEmerging.slice(0, 12).map((r: any, i: number) => toTopic(r, i, true));
    }
  } catch {
    // Keep mock defaults.
  }

  try {
    const posts24h = asNum(raw?.communityBrief?.postsAnalyzed24h, asNum(raw?.communityBrief?.postsLast24h, 0));
    const commentScopes24h = asNum(raw?.communityBrief?.commentScopesAnalyzed24h, asNum(raw?.communityBrief?.commentsLast24h, 0));
    const positiveIntentPct = clamp(asNum(raw?.communityBrief?.positiveIntentPct24h, 0), 0, 100);
    const negativeIntentPct = clamp(asNum(raw?.communityBrief?.negativeIntentPct24h, 0), 0, 100);
    const neutralIntentPct = clamp(100 - positiveIntentPct - negativeIntentPct, 0, 100);
    const topTopics = asArray<string>(raw?.communityBrief?.topTopics).slice(0, 5);
    const topTopicsRu = topTopics.map((t) => translateTopicRu(t));
    const totalAnalyses = asNum(raw?.communityBrief?.totalAnalyses24h, posts24h + commentScopes24h);
    app.communityBrief.messagesAnalyzed = totalAnalyses;
    app.communityBrief.updatedMinutesAgo = asNum(raw?.communityBrief?.refreshedMinutesAgo, 5);
    app.communityBrief.postsAnalyzed24h = posts24h;
    app.communityBrief.commentScopesAnalyzed24h = commentScopes24h;
    app.communityBrief.positiveIntentPct24h = positiveIntentPct;
    app.communityBrief.negativeIntentPct24h = negativeIntentPct;

    app.communityBrief.mainBrief.en = `Selected window snapshot (${selectedRangeDays}d): ${posts24h} posts and ${commentScopes24h} analyzed comment scopes. People talk mostly about ${topTopics.join(', ') || 'core community topics'}.`;
    app.communityBrief.mainBrief.ru = `Снимок выбранного окна (${selectedRangeDays} дн.): ${posts24h} постов и ${commentScopes24h} контекстных групп комментариев. Чаще всего обсуждают: ${topTopicsRu.join(', ') || 'ключевые темы сообщества'}.`;
    app.communityBrief.expandedBrief.en = [
      `Intent split: ${positiveIntentPct}% positive, ${negativeIntentPct}% negative, ${neutralIntentPct}% neutral.`,
      'Every top topic can be opened with real post/comment evidence snippets.',
    ];
    app.communityBrief.expandedBrief.ru = [
      `Распределение интента: ${positiveIntentPct}% позитивный, ${negativeIntentPct}% негативный, ${neutralIntentPct}% нейтральный.`,
      'Каждую топ-тему можно открыть и проверить по реальным цитатам постов/комментариев.',
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
        if (NOISY_TOPIC_KEYS.has(name.toLowerCase())) return;
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
        if (value <= 0) return null;
        const weeklyCurrent = asNum(r.mentions7d, 0);
        const weeklyPrevious = asNum(r.mentionsPrev7d, 0);
        const fromWindow = boundedTrend(weeklyCurrent, weeklyPrevious);
        const support = asNum(r.growthSupport, fromWindow.support);
        const backendGrowth = asNum(r.growth7dPct, Number.NaN);
        const backendReliable = Number.isFinite(backendGrowth) && support >= MIN_SUPPORT_FOR_TREND;
        const sourceTopic = normalizeTopicLabel(r.name);
        const sourceKey = topicKey(sourceTopic);
        return {
          name: ru ? translateTopicRu(sourceTopic) : sourceTopic,
          sourceTopic,
          value,
          category: translateCategory(category, ru),
          color: hashColor(category),
          growth: backendReliable
            ? clamp(Math.round(backendGrowth), -MAX_ABS_TREND_PCT, MAX_ABS_TREND_PCT)
            : fromWindow.value,
          growthReliable: backendReliable || fromWindow.reliable,
          evidenceCount: support,
          sampleQuote: topicEvidenceTextByTopic.get(sourceKey) || topicQuestionEvidenceByTopic.get(sourceKey)?.[0] || '',
        };
      }).filter((item): item is NonNullable<typeof item> => Boolean(item));
      app.topicBubbles.en = convert(false);
      app.topicBubbles.ru = convert(true);
    }
  } catch {
    // Keep mock defaults.
  }

  try {
    if (rawTrendRows.length > 0) {
      const perTopicPerBucket = new Map<string, Map<string, number>>();
      const bucketSet = new Set<string>();
      rawTrendRows.forEach((r: any) => {
        const topic = normalizeTopicLabel(r.topic);
        if (!topic) return;
        const bucket = asStr(r.bucket, '').trim() || (() => {
          const year = asNum(r.year, 0);
          const week = asNum(r.week, 0);
          if (!year || !week) return '';
          return `${year}-W${String(week).padStart(2, '0')}`;
        })();
        if (!bucket) return;
        bucketSet.add(bucket);
        if (!perTopicPerBucket.has(topic)) perTopicPerBucket.set(topic, new Map<string, number>());
        const bucketMap = perTopicPerBucket.get(topic)!;
        bucketMap.set(bucket, asNum(bucketMap.get(bucket), 0) + asNum(r.posts, 0));
      });

      const bucketsOrdered = Array.from(bucketSet).sort((a, b) => asStr(a).localeCompare(asStr(b)));
      if (bucketsOrdered.length === 0) {
        app.trendData = [];
        app.trendLines.en = [];
        app.trendLines.ru = [];
      } else {
        const recentBuckets = bucketsOrdered.slice(-COMPARE_BUCKET_COUNT);
        const topicScoreRows = Array.from(perTopicPerBucket.entries()).map(([topic, bucketMap]) => {
          const recentVolume = recentBuckets.reduce((sum, w) => sum + asNum(bucketMap.get(w), 0), 0);
          const totalVolume = bucketsOrdered.reduce((sum, w) => sum + asNum(bucketMap.get(w), 0), 0);
          return { topic, recentVolume, totalVolume };
        });

        const topTopics = topicScoreRows
          .sort((a, b) => (b.recentVolume - a.recentVolume) || (b.totalVolume - a.totalVolume))
          .slice(0, 6)
          .map((r) => r.topic);

        const keyByTopic = Object.fromEntries(topTopics.map((t) => [t, slugify(t)]));
        app.trendData = bucketsOrdered.map((bucket) => {
          const row: Record<string, string | number> = { week: formatTrendBucketLabel(bucket) };
          topTopics.forEach((topic) => {
            const key = keyByTopic[topic];
            row[key] = asNum(perTopicPerBucket.get(topic)?.get(bucket), 0);
          });
          return row;
        });

      app.trendLines.en = topTopics.map((topic, i) => {
        const key = keyByTopic[topic];
        const lastIdx = app.trendData.length - 1;
        const prevIdx = Math.max(0, lastIdx - 1);
        const current = asNum(app.trendData[lastIdx]?.[key], 0);
        const previous = asNum(app.trendData[prevIdx]?.[key], 0);
        return {
          key,
          label: topic,
          color: TOPIC_COLORS[i % TOPIC_COLORS.length],
          current,
          change: previous > 0 ? Math.round(((current - previous) / previous) * 100) : (current > 0 ? 100 : 0),
        };
      });
      app.trendLines.ru = app.trendLines.en.map((line) => ({ ...line }));
      }
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
    const briefRows = asArray(raw.questionBriefs);
    if (briefRows.length > 0) {
      const merged = new Map<string, any>();
      briefRows.forEach((r: any) => {
        const topic = normalizeTopicLabel(r.topic);
        if (!topic) return;
        const key = asStr(r.id, '').trim() || topicKey(topic);
        if (merged.has(key)) return;

        const evidenceRows = asArray(r.evidence)
          .map((ev: any) => ({
            id: asStr(ev.id, ''),
            quote: snippet(ev.quote, 500),
            channel: asStr(ev.channel, 'unknown'),
            timestamp: asStr(ev.timestamp, ''),
            kind: asStr(ev.kind, 'message'),
          }))
          .filter((ev: any) => ev.id && ev.quote)
          .slice(0, 4);

        merged.set(key, {
          id: key,
          topic,
          category: asStr(r.category, 'General'),
          questionEn: asStr(r.canonicalQuestionEn, asStr(r.questionEn, asStr(r.question, asStr(r.titleEn, asStr(r.title, topic))))),
          questionRu: asStr(r.canonicalQuestionRu, asStr(r.questionRu, asStr(r.canonicalQuestionEn, asStr(r.question, asStr(r.titleRu, asStr(r.titleEn, asStr(r.title, topic))))))),
          summaryEn: asStr(r.summaryEn, asStr(r.briefEn, asStr(r.summary, asStr(r.brief, '')))),
          summaryRu: asStr(r.summaryRu, asStr(r.briefRu, asStr(r.summaryEn, asStr(r.summary, asStr(r.briefEn, asStr(r.brief, '')))))),
          confidence: asStr(r.confidence, 'medium').toLowerCase(),
          confidenceScore: clamp(asNum(r.confidenceScore, 0.6), 0, 1),
          status: asStr(r.status, 'partially_answered'),
          resolvedPct: clamp(asNum(r.resolvedPct, 0), 0, 100),
          demandSignals: {
            messages: Math.max(0, asNum(r?.demandSignals?.messages, asNum(r.signalCount, 0))),
            uniqueUsers: Math.max(0, asNum(r?.demandSignals?.uniqueUsers, asNum(r.uniqueUsers, 0))),
            channels: Math.max(0, asNum(r?.demandSignals?.channels, asNum(r.channelCount, 0))),
            trend7dPct: clamp(asNum(r?.demandSignals?.trend7dPct, asNum(r.trend7dPct, 0)), -100, 100),
          },
          sampleEvidenceId: asStr(r.sampleEvidenceId, evidenceRows[0]?.id || ''),
          latestAt: asStr(r.latestAt, ''),
          evidence: evidenceRows,
        });
      });

      const en = Array.from(merged.values()).map((r: any) => ({
        id: r.id,
        topic: r.topic,
        sourceTopic: r.topic,
        category: r.category,
        question: r.questionEn || r.topic,
        summary: r.summaryEn || r.questionEn || r.topic,
        title: r.questionEn || r.topic,
        brief: r.summaryEn || r.questionEn || r.topic,
        confidence: (['high', 'medium', 'low'].includes(r.confidence) ? r.confidence : 'medium') as 'high' | 'medium' | 'low',
        confidenceScore: r.confidenceScore,
        status: (['needs_guide', 'partially_answered', 'well_covered'].includes(r.status) ? r.status : 'partially_answered') as 'needs_guide' | 'partially_answered' | 'well_covered',
        resolvedPct: r.resolvedPct,
        demandSignals: r.demandSignals,
        sampleEvidenceId: r.sampleEvidenceId,
        latestAt: r.latestAt,
        evidence: r.evidence,
      }));
      const ruRows = Array.from(merged.values()).map((r: any) => ({
        id: r.id,
        topic: translateTopicRu(r.topic),
        sourceTopic: r.topic,
        category: translateCategory(r.category, true),
        question: r.questionRu || r.questionEn || translateTopicRu(r.topic),
        summary: r.summaryRu || r.summaryEn || r.questionRu || r.questionEn || translateTopicRu(r.topic),
        title: r.questionRu || r.questionEn || translateTopicRu(r.topic),
        brief: r.summaryRu || r.summaryEn || r.questionRu || r.questionEn || translateTopicRu(r.topic),
        confidence: (['high', 'medium', 'low'].includes(r.confidence) ? r.confidence : 'medium') as 'high' | 'medium' | 'low',
        confidenceScore: r.confidenceScore,
        status: (['needs_guide', 'partially_answered', 'well_covered'].includes(r.status) ? r.status : 'partially_answered') as 'needs_guide' | 'partially_answered' | 'well_covered',
        resolvedPct: r.resolvedPct,
        demandSignals: r.demandSignals,
        sampleEvidenceId: r.sampleEvidenceId,
        latestAt: r.latestAt,
        evidence: r.evidence,
      }));

      app.questionBriefs.en = en.slice(0, 8);
      app.questionBriefs.ru = ruRows.slice(0, 8);
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
            sampleQuestionId: asStr(r.sampleQuestionId, ''),
          });
          return;
        }
        merged.set(key, {
          ...existing,
          seekers: asNum(existing.seekers, 0) + asNum(r.seekers, 0),
          respondedSeekers: asNum(existing.respondedSeekers, 0) + asNum(r.respondedSeekers, 0),
          sampleQuestion: asStr(existing.sampleQuestion) || asStr(r.sampleQuestion),
          sampleQuestionId: asStr(existing.sampleQuestionId) || asStr(r.sampleQuestionId),
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
        const sampleQuestion = questionSnippet(r.sampleQuestion, 140);
        const questionText = sampleQuestion;
        byCat.get(cat)?.push({
          q: topic,
          preview: questionText,
          topic,
          evidenceId: asStr(r.sampleQuestionId, ''),
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
      const stageOrder = ['growing', 'declining'];
      const byStage = new Map<string, any[]>();
      rawLifecycle.forEach((r: any) => {
        const topic = normalizeTopicLabel(r.topic);
        if (!topic) return;
        const topicKeyNorm = topic.trim().toLowerCase();
        if (!topicKeyNorm || ['null', 'unknown', 'none', 'n/a', 'na'].includes(topicKeyNorm)) return;

        const style = stageStyle(asStr(r.stage, 'peak'));
        const stage = style.key;
        if (!byStage.has(stage)) byStage.set(stage, []);

        const key = topicKey(topic);
        const details = topicLifecycleDetailsByTopic.get(key);

        const first = new Date(asStr(r.firstSeen, new Date().toISOString()));
        const fallbackDays = Math.max(1, Math.round((Date.now() - first.getTime()) / 86400000));
        const daysActive = Math.max(1, asNum(r.ageDays, fallbackDays));

        const weeklyCurrent = Math.max(0, asNum(r.weeklyCurrent, asNum(r.recentPosts, 0)));
        const weeklyPrev = Math.max(0, asNum(r.weeklyPrev, 0));
        const weeklyDelta = asNum(r.weeklyDelta, weeklyCurrent - weeklyPrev);

        byStage.get(stage)?.push({
          name: topic,
          sourceTopic: topic,
          daysActive,
          momentum: Math.round(weeklyDelta),
          volume: weeklyCurrent,
          summary: details?.summary || '',
          topChannels: details?.topChannels || [],
          evidence: details?.evidence || [],
        });
      });

      stageOrder.forEach((stage) => {
        if (!byStage.has(stage)) byStage.set(stage, []);
        byStage.set(
          stage,
          (byStage.get(stage) || [])
            .sort((a, b) => (b.volume - a.volume) || (b.momentum - a.momentum))
            .slice(0, 8),
        );
      });

      const stagesEn = stageOrder.map((stage) => {
        const s = stageStyle(stage);
        return { stage: s.en, color: s.color, bgColor: s.bgColor, borderColor: s.borderColor, textColor: s.textColor, desc: s.descEn, topics: byStage.get(stage) || [] };
      });
      const stagesRu = stageOrder.map((stage) => {
        const s = stageStyle(stage);
        return {
          stage: s.ru,
          color: s.color,
          bgColor: s.bgColor,
          borderColor: s.borderColor,
          textColor: s.textColor,
          desc: s.descRu,
          topics: (byStage.get(stage) || []).map((topic: any) => ({
            ...topic,
            name: translateTopicRu(topic.name),
          })),
        };
      });
      app.lifecycleStages = { en: stagesEn, ru: stagesRu };
    }
  } catch {
    // Keep mock defaults.
  }

  try {
    const pbRows = asArray(raw.problemBriefs);
    if (pbRows.length > 0) {
      const merged = new Map<string, any>();
      pbRows.forEach((r: any) => {
        const topic = normalizeTopicLabel(r.topic);
        if (!topic) return;
        const key = asStr(r.id, '').trim() || `pb-${topicKey(topic)}`;
        if (merged.has(key)) return;

        const evidenceRows = asArray(r.evidence)
          .map((ev: any) => ({
            id: asStr(ev.id, ''),
            quote: snippet(ev.quote, 500),
            channel: asStr(ev.channel, 'unknown'),
            timestamp: asStr(ev.timestamp, ''),
            kind: asStr(ev.kind, 'message'),
          }))
          .filter((ev: any) => ev.id && ev.quote)
          .slice(0, 4);

        const severity = asStr(r.severity, 'medium').toLowerCase();
        merged.set(key, {
          id: key,
          topic,
          category: asStr(r.category, 'General'),
          problemEn: asStr(r.problemEn, asStr(r.problem, topic)),
          problemRu: asStr(r.problemRu, asStr(r.problemEn, asStr(r.problem, topic))),
          summaryEn: asStr(r.summaryEn, asStr(r.summary, '')),
          summaryRu: asStr(r.summaryRu, asStr(r.summaryEn, asStr(r.summary, ''))),
          severity: (['critical', 'high', 'medium', 'low'].includes(severity) ? severity : 'medium') as 'critical' | 'high' | 'medium' | 'low',
          confidence: asStr(r.confidence, 'medium').toLowerCase(),
          confidenceScore: clamp(asNum(r.confidenceScore, 0.6), 0, 1),
          demandSignals: {
            messages: Math.max(0, asNum(r?.demandSignals?.messages, asNum(r.signalCount, 0))),
            uniqueUsers: Math.max(0, asNum(r?.demandSignals?.uniqueUsers, asNum(r.uniqueUsers, 0))),
            channels: Math.max(0, asNum(r?.demandSignals?.channels, asNum(r.channelCount, 0))),
            trend7dPct: clamp(asNum(r?.demandSignals?.trend7dPct, asNum(r.trend7dPct, 0)), -100, 100),
          },
          sampleEvidenceId: asStr(r.sampleEvidenceId, evidenceRows[0]?.id || ''),
          latestAt: asStr(r.latestAt, ''),
          evidence: evidenceRows,
        });
      });

      const en = Array.from(merged.values()).map((r: any) => ({
        id: r.id,
        topic: r.topic,
        sourceTopic: r.topic,
        category: r.category,
        problem: r.problemEn || r.topic,
        summary: r.summaryEn || r.problemEn || r.topic,
        severity: r.severity,
        confidence: (['high', 'medium', 'low'].includes(r.confidence) ? r.confidence : 'medium') as 'high' | 'medium' | 'low',
        confidenceScore: r.confidenceScore,
        demandSignals: r.demandSignals,
        sampleEvidenceId: r.sampleEvidenceId,
        latestAt: r.latestAt,
        evidence: r.evidence,
      }));
      const ruRows = Array.from(merged.values()).map((r: any) => ({
        id: r.id,
        topic: translateTopicRu(r.topic),
        sourceTopic: r.topic,
        category: translateCategory(r.category, true),
        problem: r.problemRu || r.problemEn || translateTopicRu(r.topic),
        summary: r.summaryRu || r.summaryEn || r.problemRu || r.problemEn || translateTopicRu(r.topic),
        severity: r.severity,
        confidence: (['high', 'medium', 'low'].includes(r.confidence) ? r.confidence : 'medium') as 'high' | 'medium' | 'low',
        confidenceScore: r.confidenceScore,
        demandSignals: r.demandSignals,
        sampleEvidenceId: r.sampleEvidenceId,
        latestAt: r.latestAt,
        evidence: r.evidence,
      }));

      app.problemBriefs.en = en.slice(0, 8);
      app.problemBriefs.ru = ruRows.slice(0, 8);
    }
  } catch {
    // Keep mock defaults.
  }

  try {
    const sgbRows = asArray(raw.serviceGapBriefs);
    if (sgbRows.length > 0) {
      const merged = new Map<string, any>();
      sgbRows.forEach((r: any) => {
        const topic = normalizeTopicLabel(r.topic);
        if (!topic) return;
        const key = asStr(r.id, '').trim() || `sg-${topicKey(topic)}`;
        if (merged.has(key)) return;

        const evidenceRows = asArray(r.evidence)
          .map((ev: any) => ({
            id: asStr(ev.id, ''),
            quote: snippet(ev.quote, 500),
            channel: asStr(ev.channel, 'unknown'),
            timestamp: asStr(ev.timestamp, ''),
            kind: asStr(ev.kind, 'message'),
          }))
          .filter((ev: any) => ev.id && ev.quote)
          .slice(0, 4);

        const urgency = asStr(r.urgency, 'medium').toLowerCase();
        merged.set(key, {
          id: key,
          topic,
          category: asStr(r.category, 'General'),
          serviceNeedEn: asStr(r.serviceNeedEn, asStr(r.serviceNeed, topic)),
          serviceNeedRu: asStr(r.serviceNeedRu, asStr(r.serviceNeedEn, asStr(r.serviceNeed, topic))),
          unmetReasonEn: asStr(r.unmetReasonEn, asStr(r.unmetReason, '')),
          unmetReasonRu: asStr(r.unmetReasonRu, asStr(r.unmetReasonEn, asStr(r.unmetReason, ''))),
          urgency: (['critical', 'high', 'medium', 'low'].includes(urgency) ? urgency : 'medium') as 'critical' | 'high' | 'medium' | 'low',
          unmetPct: clamp(asNum(r.unmetPct, asNum(r.dissatisfactionPct, 0)), 0, 100),
          confidence: asStr(r.confidence, 'medium').toLowerCase(),
          confidenceScore: clamp(asNum(r.confidenceScore, 0.6), 0, 1),
          demandSignals: {
            messages: Math.max(0, asNum(r?.demandSignals?.messages, asNum(r.signalCount, 0))),
            uniqueUsers: Math.max(0, asNum(r?.demandSignals?.uniqueUsers, asNum(r.uniqueUsers, 0))),
            channels: Math.max(0, asNum(r?.demandSignals?.channels, asNum(r.channelCount, 0))),
            trend7dPct: clamp(asNum(r?.demandSignals?.trend7dPct, asNum(r.trend7dPct, 0)), -100, 100),
          },
          sampleEvidenceId: asStr(r.sampleEvidenceId, evidenceRows[0]?.id || ''),
          latestAt: asStr(r.latestAt, ''),
          evidence: evidenceRows,
        });
      });

      const en = Array.from(merged.values()).map((r: any) => ({
        id: r.id,
        topic: r.topic,
        sourceTopic: r.topic,
        category: r.category,
        serviceNeed: r.serviceNeedEn || r.topic,
        unmetReason: r.unmetReasonEn || r.serviceNeedEn || r.topic,
        urgency: r.urgency,
        unmetPct: Math.round(r.unmetPct),
        confidence: (['high', 'medium', 'low'].includes(r.confidence) ? r.confidence : 'medium') as 'high' | 'medium' | 'low',
        confidenceScore: r.confidenceScore,
        demandSignals: r.demandSignals,
        sampleEvidenceId: r.sampleEvidenceId,
        latestAt: r.latestAt,
        evidence: r.evidence,
      }));
      const ruRows = Array.from(merged.values()).map((r: any) => ({
        id: r.id,
        topic: translateTopicRu(r.topic),
        sourceTopic: r.topic,
        category: translateCategory(r.category, true),
        serviceNeed: r.serviceNeedRu || r.serviceNeedEn || translateTopicRu(r.topic),
        unmetReason: r.unmetReasonRu || r.unmetReasonEn || r.serviceNeedRu || r.serviceNeedEn || translateTopicRu(r.topic),
        urgency: r.urgency,
        unmetPct: Math.round(r.unmetPct),
        confidence: (['high', 'medium', 'low'].includes(r.confidence) ? r.confidence : 'medium') as 'high' | 'medium' | 'low',
        confidenceScore: r.confidenceScore,
        demandSignals: r.demandSignals,
        sampleEvidenceId: r.sampleEvidenceId,
        latestAt: r.latestAt,
        evidence: r.evidence,
      }));

      app.serviceGapBriefs.en = en.slice(0, 8);
      app.serviceGapBriefs.ru = ruRows.slice(0, 8);
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
        if (NOISY_TOPIC_KEYS.has(topic.toLowerCase())) return;
        const key = topicKey(topic);
        const weeklyCurrent = asNum(r.affectedThisWeek, 0);
        const weeklyPrevious = asNum(r.affectedPrevWeek, 0);
        const computedTrend = boundedTrend(weeklyCurrent, weeklyPrevious);
        const trendSupport = asNum(r.trendSupport, computedTrend.support);
        const backendTrend = asNum(r.trendPct, Number.NaN);
        const trendReliable = Number.isFinite(backendTrend) && trendSupport >= MIN_SUPPORT_FOR_TREND;
        const entry = {
          name: topic,
          sourceTopic: topic,
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
          sourceTopic: asStr(existing.sourceTopic, '') || asStr(entry.sourceTopic, ''),
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
        problems: Array.from(problemMap.values()).sort((a, b) => b.mentions - a.mentions).slice(0, 5).map((p) => ({
          ...p,
          name: translateTopicRu(p.sourceTopic || p.name) || p.name,
        })),
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
        const topic = normalizeTopicLabel(r.topic);
        return {
          service: ru ? translateTopicRu(topic) : topic,
          sourceTopic: topic,
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
      const filtered = sgRows.filter((r: any) => {
        const topic = normalizeTopicLabel(r.topic);
        return topic.length > 0 && !NOISY_TOPIC_KEYS.has(topic.toLowerCase());
      });
      app.serviceGaps.en = filtered.map((r: any) => mapGap(r, false)).slice(0, 12);
      app.serviceGaps.ru = filtered.map((r: any) => mapGap(r, true)).slice(0, 12);
    }
  } catch {
    // Keep mock defaults.
  }

  try {
    const satRows = asArray(raw.satisfactionAreas);
    if (satRows.length > 0) {
      const mk = (r: any) => {
        const area = normalizeTopicLabel(r.category || r.topic || 'General');
        const satisfaction = clamp(Math.round(asNum(r.satisfactionPct, 0)), 0, 100);
        const mentions = asNum(r.volume, asNum(r.pos, 0) + asNum(r.neg, 0) + asNum(r.neu, 0));
        const trend = clamp(Math.round(asNum(r.trendPct, (asNum(r.pos, 0) - asNum(r.neg, 0)) / 8)), -50, 50);
        return {
          sourceArea: area,
          area,
          satisfaction,
          mentions,
          trend,
          emoji: satisfaction >= 60 ? '🙂' : satisfaction >= 40 ? '😐' : '😟',
        };
      };
      app.satisfactionAreas.en = satRows.map(mk).filter((r: any) => r.area && !NOISY_TOPIC_KEYS.has(r.area.toLowerCase()));
      app.satisfactionAreas.ru = satRows.map((r: any) => {
        const base = mk(r);
        return { ...base, area: translateTopicRu(base.sourceArea) || base.sourceArea };
      }).filter((r: any) => r.area && !NOISY_TOPIC_KEYS.has(r.sourceArea.toLowerCase()));
    }
  } catch {
    // Keep mock defaults.
  }

  try {
    const moodRows = asArray(raw.moodData);
    if (moodRows.length > 0) {
      const hasBucketShape = moodRows.some((r: any) =>
        ['excited', 'satisfied', 'neutral', 'frustrated', 'anxious'].some((key) => r && Object.prototype.hasOwnProperty.call(r, key))
      );

      const byWeek = new Map<string, any>();
      const ensureWeekRow = (weekKey: string) => {
        if (!byWeek.has(weekKey)) {
          byWeek.set(weekKey, { week: weekKey, excited: 0, satisfied: 0, neutral: 0, frustrated: 0, anxious: 0 });
        }
        return byWeek.get(weekKey);
      };

      if (hasBucketShape) {
        moodRows.forEach((r: any) => {
          const weekKey = asStr(r.bucket || r.week) || `${asNum(r.year, 0)}-W${String(asNum(r.week, 0)).padStart(2, '0')}`;
          const row = ensureWeekRow(weekKey);
          row.excited += asNum(r.excited, 0);
          row.satisfied += asNum(r.satisfied, 0);
          row.neutral += asNum(r.neutral, 0);
          row.frustrated += asNum(r.frustrated, 0);
          row.anxious += asNum(r.anxious, 0);
        });
      } else {
        moodRows.forEach((r: any) => {
          const weekKey = `${asNum(r.year, 0)}-W${String(asNum(r.week, 0)).padStart(2, '0')}`;
          const row = ensureWeekRow(weekKey);
          const sentiment = asStr(r.sentiment).toLowerCase();
          const count = asNum(r.count, 0);
          if (sentiment.includes('positive')) row.satisfied += count;
          else if (sentiment.includes('negative')) row.frustrated += count;
          else if (sentiment.includes('urgent')) row.anxious += count;
          else row.neutral += count;
        });
      }

      const mood = Array.from(byWeek.values())
        .sort((a, b) => asStr(a.week).localeCompare(asStr(b.week)))
        .slice(-10);
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
        message: asStr(r.messageEn) || `Need immediate help around ${asStr(r.topicEn || r.topic)}.`,
        topic: asStr(r.topicEn || r.topic),
        urgency: asStr(r.urgency, asNum(r.messages || r.count, 0) > 8 ? 'critical' : 'high').toLowerCase(),
        count: asNum(r.count, 0) || asNum(r.messages, 0) || asNum(r.urgentUsers, 0),
        action: asStr(r.actionEn) || 'Assign moderator follow-up and pin guidance',
      }));
      const ru = uRows.map((r: any) => ({
        message: asStr(r.messageRu) || `Нужна срочная помощь по теме: ${asStr(r.topicRu || r.topic)}.`,
        topic: asStr(r.topicRu || r.topic),
        urgency: asStr(r.urgency, asNum(r.messages || r.count, 0) > 8 ? 'critical' : 'high').toLowerCase(),
        count: asNum(r.count, 0) || asNum(r.messages, 0) || asNum(r.urgentUsers, 0),
        action: asStr(r.actionRu) || 'Назначить модератора и закрепить инструкцию',
      }));
      app.urgencySignals = { en, ru };
    }
  } catch {
    // Keep mock defaults.
  }

  try {
    if (rawChannels.length > 0) {
      app.communityChannels = rawChannels.map((c: any, i: number) => {
        // Use the new backend fields directly when available
        if (c.engagement !== undefined && c.type !== undefined && c.dailyMessages !== undefined) {
          // Backend already provides calculated metrics
          return {
            name: asStr(c.name || c.title || c.username || `Channel ${i + 1}`),
            type: asStr(c.type, 'General'),
            members: asNum(c.members || c.memberCount, 1000),
            dailyMessages: asNum(c.dailyMessages, 0),
            engagement: clamp(asNum(c.engagement, 0), 0, 100),
            growth: asNum(c.growth, 0),
            topTopicEN: asStr(c.topTopicEN || asArray(c.topTopics)[0], 'General'),
            topTopicRU: asStr(c.topTopicRU || asArray(c.topTopics)[0], 'General'),
          };
        }

        // Fallback to old calculation method if backend hasn't been updated
        const topTopic = asStr(asArray(c.topTopics)[0], 'General');

        let chType = 'General';
        const tLower = topTopic.toLowerCase();
        if (tLower.includes('job') || tLower.includes('work') || tLower.includes('career') || tLower.includes('employ')) chType = 'Work';
        else if (tLower.includes('hous') || tLower.includes('rent') || tLower.includes('apart') || tLower.includes('real estate')) chType = 'Housing';
        else if (tLower.includes('business') || tLower.includes('invest') || tLower.includes('tax') || tLower.includes('market')) chType = 'Business';
        else if (tLower.includes('fam') || tLower.includes('child') || tLower.includes('school') || tLower.includes('educa')) chType = 'Family';
        else if (tLower.includes('law') || tLower.includes('legal') || tLower.includes('visa') || tLower.includes('pass') || tLower.includes('resid')) chType = 'Legal';
        else if (tLower.includes('art') || tLower.includes('music') || tLower.includes('food') || tLower.includes('cultur') || tLower.includes('event')) chType = 'Lifestyle';

        const trueMembers = asNum(c.memberCount, 0) || Math.max(500, asNum(c.postCount, 0) * 120);
        const trueDaily = Math.max(1, Math.round(asNum(c.recentPosts, 0) / 14));
        const trueGrowth = asNum(c.posts7d, 0) - asNum(c.posts14to7d, 0);

        // Calculate a reasonable engagement score combining views, replies, and forwards relative to audience size over the recent period
        // If members are unknown, we fall back to a view-based scaling.
        const avgV = asNum(c.avgViews, 1);
        const avgF = asNum(c.avgForwards, 0);
        const avgC = asNum(c.avgComments, 0);

        let engagementPct = 0;
        if (trueMembers > 0) {
            // Give 1 point for a view, 5 for a forward, 10 for a comment
            const interactions = avgV + (avgF * 5) + (avgC * 10);
            engagementPct = (interactions / trueMembers) * 100;
            // Cap at 99% for channels whose views greatly exceed subscribers
            if (engagementPct > 99) engagementPct = 99;
        } else {
            engagementPct = clamp(Math.round(avgV / 25), 5, 99);
        }

        return {
          name: asStr(c.title || c.username || `Channel ${i + 1}`),
          type: chType,
          members: trueMembers,
          dailyMessages: trueDaily,
          engagement: clamp(Math.round(engagementPct), 1, 99),
          growth: trueGrowth,
          topTopicEN: topTopic,
          topTopicRU: topTopic, // Let frontend handle translation or fallback
        };
      });
    }
  } catch {
    // Keep mock defaults.
  }

  try {
    if (rawKeyVoices.length > 0) {
      const mk = (r: any, i: number, ru: boolean) => {
        // Use real displayName from backend
        const displayName = asStr(r.displayName, asStr(r.username, `User_${asStr(r.userId, i + 1)}`));

        // Use real topics from user's comments, fallback to trending if none
        const userTopics = asArray(r.topics).length > 0 ? asArray(r.topics) :
                          asArray(r.userTopics).length > 0 ? asArray(r.userTopics) :
                          app.trendingTopics.en.slice(0, 3).map((t) => t.topic);

        return {
          name: displayName,
          role: ru ? (asStr(r.role, 'Member') || 'Участник') : asStr(r.role, 'Member'),
          topics: userTopics.slice(0, 3),
          postsPerWeek: r.postsPerWeek ? asNum(r.postsPerWeek, 1) : Math.max(1, asNum(r.commentCount, 0)),
          replyRate: r.replyRate ? asNum(r.replyRate, 20) : clamp(Math.round((asNum(r.replyCount, 0) / Math.max(1, asNum(r.commentCount, 1))) * 100), 5, 99),
          topChannels: asArray(r.topChannels).slice(0, 3),
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
      const mk = (r: any, ru: boolean) => {
        // Use real recommendation text from AI extraction
        const itemText = asStr(r.item, '');

        // If no real recommendation text, show "No recommendations yet"
        if (!itemText || itemText === '') {
          return null;
        }

        return {
          item: itemText, // Actual recommendation text from AI
          category: asStr(r.category, ru ? 'Общий' : 'General'),
          mentions: asNum(r.mentions, asNum(r.helpCount, 1)),
          rating: asNum(r.rating, clamp(Math.round(Math.min(5, 3 + asNum(r.mentions, 0) / 3)), 1, 5)),
          sentiment: asStr(r.sentiment, 'positive'),
          // Add evidence fields for linking to source
          evidenceId: asStr(r.evidenceId, ''),
          evidenceText: asStr(r.evidenceText, ''),
          channel: asStr(r.channel, ''),
        };
      };

      const enRecs = recRows.slice(0, 12).map((r: any) => mk(r, false)).filter(Boolean);
      const ruRecs = recRows.slice(0, 12).map((r: any) => mk(r, true)).filter(Boolean);

      // If we have real recommendations, use them
      if (enRecs.length > 0) {
        app.recommendations.en = enRecs;
      }
      if (ruRecs.length > 0) {
        app.recommendations.ru = ruRecs;
      }
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
      const mk = (r: any, ru: boolean) => {
        // Use real data from get_information_velocity()
        const originator = asStr(r.originator, '');
        const spreadHours = asNum(r.spreadHours, 0);
        const channelsReached = asNum(r.channelsReached, 0);
        const amplifiers = asArray(r.amplifiers);
        const totalReach = asNum(r.totalReach, 0);
        const velocity = asStr(r.velocity, 'normal');

        // Fallback to old calculation if new fields not available
        const co = asNum(r.coOccurrences, 0);
        const conn = asNum(r.connectedTopics, 0);

        // If no real originator, don't make up fake data
        if (!originator && !co) {
          return null;
        }

        return {
          topic: asStr(r.topic),
          originator: originator || app.communityChannels[0]?.name || (ru ? 'Неизвестный источник' : 'Unknown source'),
          spreadHours: spreadHours > 0 ? spreadHours : clamp(Math.round(72 / Math.max(1, Math.log2(co + 2))), 1, 72),
          channelsReached: channelsReached > 0 ? channelsReached : Math.max(1, conn),
          amplifiers: amplifiers.length > 0 ? amplifiers : app.communityChannels.slice(1, 4).map((c) => c.name).filter(Boolean),
          totalReach: totalReach > 0 ? totalReach : co * 70,
          velocity: velocity || (co > 150 ? 'explosive' : co > 80 ? 'fast' : 'normal'),
        };
      };

      const enTopics = viralRows.slice(0, 12).map((r: any) => mk(r, false)).filter(Boolean);
      const ruTopics = viralRows.slice(0, 12).map((r: any) => mk(r, true)).filter(Boolean);

      if (enTopics.length > 0) {
        app.viralTopics.en = enTopics;
      }
      if (ruTopics.length > 0) {
        app.viralTopics.ru = ruTopics;
      }
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
      const maxUsers = Math.max(...interestRows.map((r: any) => asNum(r.users, 0)), 1);
      const deduped = new Map<string, { interestEn: string; interestRu: string; score: number; users: number }>();

      interestRows.forEach((r: any) => {
        const category = asStr(r.category, '').trim();
        const topic = normalizeTopicLabel(r.topic);
        const labelEn = category || topic;
        if (!labelEn) return;
        if (['general', 'null', 'none', 'unknown'].includes(labelEn.toLowerCase())) return;

        const users = asNum(r.users, 0);
        const explicitPct = Number(asNum(r.penetrationPct, Number.NaN));
        const fallbackPct = pct(users, maxUsers);
        const score = clamp(Math.round(Number.isFinite(explicitPct) ? explicitPct : fallbackPct), 0, 100);
        const interestRu = category ? translateCategory(category, true) : translateTopicRu(topic);
        const existing = deduped.get(labelEn.toLowerCase());

        if (!existing || score > existing.score || (score === existing.score && users > existing.users)) {
          deduped.set(labelEn.toLowerCase(), { interestEn: labelEn, interestRu, score, users });
        }
      });

      const sorted = Array.from(deduped.values())
        .sort((a, b) => (b.score - a.score) || (b.users - a.users) || a.interestEn.localeCompare(b.interestEn))
        .slice(0, 8);

      if (sorted.length > 0) {
        app.interests = {
          en: sorted.map(({ interestEn, score }) => ({ interest: interestEn, score })),
          ru: sorted.map(({ interestRu, score }) => ({ interest: interestRu, score })),
        };
      }
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
      const moodLabel = (value: string, ru: boolean) => {
        const normalized = value.toLowerCase();
        if (normalized.includes('negative') || normalized.includes('urgent') || normalized.includes('sarcastic')) {
          return ru ? 'Напряженный' : 'negative';
        }
        if (normalized.includes('positive')) {
          return ru ? 'Позитивный' : 'positive';
        }
        return ru ? 'Нейтральный' : 'neutral';
      };
      const mk = (r: any, ru: boolean) => {
        const current = asNum(r.currentMentions, asNum(r.currentPosts, asNum(r.recentPosts, 0)));
        const previous = asNum(r.previousPosts, 0);
        const computedGrowth = boundedTrend(current, previous);
        const support = asNum(r.growthSupport, computedGrowth.support);
        const backendGrowth = asNum(r.momentum, Number.NaN);
        const growthReliable = Number.isFinite(backendGrowth) && support >= MIN_SUPPORT_FOR_TREND;
        const emergenceScore = clamp(Math.round(asNum(r.emergenceScore, growthReliable ? backendGrowth : computedGrowth.value)), 0, 100);
        const backendOpportunity = asStr(r.opportunity, '').toLowerCase();
        const opportunity = backendOpportunity === 'high' || backendOpportunity === 'medium' || backendOpportunity === 'low'
          ? backendOpportunity
          : emergenceScore >= 75 ? 'high' : emergenceScore >= 55 ? 'medium' : 'low';
        return {
          topic: asStr(r.topic),
          firstSeen: asStr(r.firstSeen).slice(0, 10),
          growthRate: growthReliable
            ? clamp(Math.round(backendGrowth), -MAX_ABS_TREND_PCT, MAX_ABS_TREND_PCT)
            : computedGrowth.value,
          currentVolume: current,
          originChannel: asStr(r.originChannel, app.communityChannels[0]?.name || (ru ? 'Канал сообщества' : 'Community channel')),
          mood: moodLabel(asStr(r.mood, ''), ru),
          opportunity,
          emergenceScore,
        };
      };
      const rank = (a: any, b: any) => (
        asNum(b.emergenceScore, 0) - asNum(a.emergenceScore, 0)
        || b.growthRate - a.growthRate
        || b.currentVolume - a.currentVolume
      );
      app.emergingInterests = {
        en: rawEmerging.slice(0, 12).map((r: any) => mk(r, false)).sort(rank),
        ru: rawEmerging.slice(0, 12).map((r: any) => mk(r, true)).sort(rank),
      };
    }
  } catch {
    // Keep mock defaults.
  }

  try {
    const retentionRows = asArray(raw.retentionFactors);
    if (retentionRows.length > 0) {
      const overallScore = clamp(Math.round(asNum(retentionRows[0]?.baselineContinuityPct, 0)), 0, 100);
      const mk = (r: any, ru: boolean) => {
        const topic = normalizeTopicLabel(r.topic) || asStr(r.factor, ru ? 'Общие темы' : 'General topics');
        return {
          factor: ru ? (translateTopicRu(topic) || topic) : topic,
          score: clamp(Math.round(asNum(r.continuityPct, overallScore)), 0, 100),
          weight: clamp(Math.round(asNum(r.topicSharePct, 0)), 5, 60),
          overallScore,
          support: Math.max(0, asNum(r.previousUsers, 0)),
          lift: Math.round(asNum(r.liftPct, 0)),
        };
      };
      app.retentionFactors = {
        en: retentionRows.slice(0, 8).map((r: any) => mk(r, false)),
        ru: retentionRows.slice(0, 8).map((r: any) => mk(r, true)),
      };
    }
  } catch {
    // Keep mock defaults.
  }

  try {
    const churnRows = asArray(raw.churnSignals);
    if (churnRows.length > 0) {
      const mk = (r: any, ru: boolean) => {
        const lostUsers = asNum(r.lostUsers, asNum(r.count, 0));
        const previousUsers = asNum(r.previousUsers, 0);
        const baseline = clamp(Math.round(asNum(r.baselineDropoffPct, 0)), 0, 100);
        const rate = clamp(Math.round(asNum(r.dropoffPct, lostUsers > 0 && previousUsers > 0 ? (lostUsers / previousUsers) * 100 : 0)), 0, 100);
        const trend = clamp(Math.round(asNum(r.excessRiskPct, Math.max(0, rate - baseline))), 0, MAX_ABS_TREND_PCT);
        const topic = normalizeTopicLabel(r.topic) || asStr(r.signal, ru ? 'Общие темы' : 'General topics');
        return {
          signal: ru ? (translateTopicRu(topic) || topic) : topic,
          count: lostUsers,
          trend,
          severity: trend >= 15 ? 'rising' : trend >= 8 ? 'watch' : 'stable',
          baseline,
          rate,
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
      const hasDirectStages = ['all', 'reads', 'asks', 'helps', 'contributes', 'leads']
        .some((key) => Object.prototype.hasOwnProperty.call(byStage, key));
      const all = hasDirectStages
        ? Math.max(1, asNum(byStage.all, 0))
        : Math.max(1, Object.values(byStage).reduce((s: number, n: any) => s + asNum(n, 0), 0));
      const reads = hasDirectStages
        ? asNum(byStage.reads, 0)
        : asNum(byStage.lurker, 0) + asNum(byStage.newcomer, 0);
      const asks = hasDirectStages
        ? asNum(byStage.asks, 0)
        : asNum(byStage.participant, 0);
      const helps = hasDirectStages
        ? asNum(byStage.helps, 0)
        : asNum(byStage.regular, 0);
      const contributes = hasDirectStages
        ? asNum(byStage.contributes, 0)
        : Math.round(helps * 0.6);
      const leads = hasDirectStages
        ? asNum(byStage.leads, 0)
        : Math.max(1, Math.round(helps * 0.25));
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
        trend: boundedTrend(asNum(r.users, 0), asNum(r.previousUsers, 0)).value,
        color: colors[i % colors.length],
        needs: ru ? 'Ясные инструкции и поддержка' : 'Clear guidance and support',
      });
      app.decisionStages = { en: dsRows.map((r: any, i: number) => mk(r, i, false)), ru: dsRows.map((r: any, i: number) => mk(r, i, true)) };
    }
  } catch {
    // Keep mock defaults.
  }

  try {
    const widgetBuckets = asArray(rawNewVsReturningWidget?.buckets);
    const widgetTopTopics = asArray(rawNewVsReturningWidget?.topTopics);

    if (widgetBuckets.length > 0) {
      app.voiceData = widgetBuckets.map((row: any, i: number) => ({
        week: formatTrendBucketLabel(asStr(row.week || row.bucketStart, `W${i + 1}`)),
        newVoices: Math.max(0, asNum(row.newVoices, 0)),
        returning: Math.max(0, asNum(row.returning, 0)),
      }));
    } else {
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
    }

    if (widgetTopTopics.length > 0) {
      const mapped = widgetTopTopics.map((row: any) => ({
        topic: asStr(row.topic, 'Topic'),
        newVoices: Math.max(0, asNum(row.newVoices, 0)),
        pct: clamp(Math.round(asNum(row.pct, 0)), 0, 100),
      }));
      app.topNewTopics.en = mapped;
      app.topNewTopics.ru = mapped.map((item) => ({ ...item }));
    } else {
      app.topNewTopics.en = app.emergingInterests.en.slice(0, 6).map((e) => ({ topic: e.topic, newVoices: Math.max(5, Math.round(e.currentVolume / 2)), pct: clamp(Math.round(e.growthRate / 2), 1, 100) }));
      app.topNewTopics.ru = app.topNewTopics.en.map((e) => ({ ...e }));
    }
  } catch {
    // Keep mock defaults.
  }

  try {
    const bizRows = asArray(raw.businessOpportunities);
    if (bizRows.length > 0) {
      const mk = (r: any, ru: boolean) => {
        const type = asStr(r.type, 'Opportunity');
        const signals = asNum(r.signals, 0);
        const previousSignals = asNum(r.previousSignals, 0);
        const trend = boundedTrend(signals, previousSignals).value;
        return {
          need: type,
          mentions: signals,
          growth: trend,
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
      const buildItems = (ru: boolean) => Array.from(bySignal.entries())
        .map(([signal, count]) => ({ role: formatWorkSignalLabel(signal, ru), count, pct: pct(count, total) }))
        .sort((a, b) => b.count - a.count);
      app.jobSeeking = { en: buildItems(false), ru: buildItems(true) };
    }
    const jtRows = asArray(raw.jobTrends);
    if (jtRows.length > 0) {
      const maxCurrentUsers = jtRows.reduce((max: number, r: any) => Math.max(max, asNum(r.currentUsers, 0)), 0);
      const trends = jtRows
        .slice(0, 5)
        .map((r: any) => {
          const topic = asStr(r.topic, 'Job_Seeking');
          const currentUsers = asNum(r.currentUsers, 0);
          const previousUsers = asNum(r.previousUsers, 0);
          const trend = boundedTrend(currentUsers, previousUsers).value;
          const labelEn = formatWorkSignalLabel(topic, false);
          const labelRu = formatWorkSignalLabel(topic, true);
          const type = trend > 0
            ? (currentUsers >= maxCurrentUsers ? 'hot' : 'growing')
            : trend < 0 ? 'concern' : 'stable';
          return {
            en: {
              trend: `${labelEn} signals are ${trend > 0 ? 'up' : trend < 0 ? 'down' : 'stable'} ${trend > 0 ? '+' : ''}${trend}% (${currentUsers} users)`,
              type,
            },
            ru: {
              trend: `Сигналы «${labelRu}» ${trend > 0 ? 'выросли' : trend < 0 ? 'снизились' : 'стабильны'} ${trend > 0 ? '+' : ''}${trend}% (${currentUsers} пользователей)`,
              type,
            },
          };
        });
      app.jobTrends = {
        en: trends.map((t) => t.en),
        ru: trends.map((t) => t.ru),
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
        else if (sentiment.includes('negative') || sentiment.includes('urgent') || sentiment.includes('sarcastic')) rec.negative += count;
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
