import { useEffect, useMemo, useState } from 'react';
import { useSearchParams } from 'react-router';
import {
  Search,
  TrendingUp,
  TrendingDown,
  MessageCircle,
  ThumbsUp,
  Hash,
  X,
  Clock,
  User,
  ChevronLeft,
  ChevronRight,
  Sparkles,
} from 'lucide-react';
import { ResponsiveContainer, AreaChart, Area, XAxis, YAxis, Tooltip, CartesianGrid } from 'recharts';
import { useLanguage } from '@/app/contexts/LanguageContext';
import { useDashboardDateRange } from '@/app/contexts/DashboardDateRangeContext';
import { PageInfoButton, type PageInfoCopy } from '@/app/components/ui/PageInfoButton';
import { SocialAccessDeniedState } from '@/app/components/widgets/SocialShared';
import { TOPICS_PAGE_GROUPS_EN, translateCategory, translateTopicsPageGroup } from '@/app/services/topicPresentation';
import {
  formatSocialBucket,
  formatSocialDateLabel,
  socialActivitySummary,
  socialPlatformLabel,
} from '@/app/services/socialFormatting';
import type { SocialEvidenceItem, SocialIntelligenceFilters, SocialPlatform } from '@/app/services/socialIntelligence';
import { useSocialTopicDetailData, useSocialTopicListData, type SocialTopicListItem } from '@/app/services/socialTwinData';
import { differenceInDaysInclusive } from '@/app/utils/dashboardDateRange';

const categoryColors: Record<string, string> = {
  Living: '#ef4444',
  Work: '#3b82f6',
  Family: '#8b5cf6',
  Finance: '#f59e0b',
  Lifestyle: '#ec4899',
  Integration: '#10b981',
  Admin: '#6b7280',
  Tech: '#06b6d4',
};

type SocialTopicGroup = (typeof TOPICS_PAGE_GROUPS_EN)[number];

interface SocialTopicViewModel extends SocialTopicListItem {
  id: string;
  name: string;
  nameRu: string;
  sourceTopic: string;
  mentions: number;
  growth: number;
  topicGroup: SocialTopicGroup;
  category: string;
  description: string;
  descriptionRu: string;
  categoryLabelEn: string;
  categoryLabelRu: string;
  color: string;
  sentiment: {
    positive: number;
    neutral: number;
    negative: number;
  };
  positivePct: number;
  neutralPct: number;
  negativePct: number;
  weeklyData: Array<{ week: string; count: number }>;
  topChannels: string[];
  evidenceCount: number;
  displayPlatforms: string[];
}

interface SocialTopicOverview {
  status: 'ready' | 'fallback' | 'unavailable';
  summaryEn: string;
  summaryRu: string;
  signalsEn: string[];
  signalsRu: string[];
  generatedAt: string;
  windowStart: string;
  windowEnd: string;
  windowDays: number;
}

function normalizeTopicKey(value: string) {
  return value.trim().toLowerCase();
}

function socialTopicsInfoCopy(lang: 'en' | 'ru'): PageInfoCopy {
  return lang === 'ru'
    ? {
      summary: 'Страница Social Topics теперь полностью повторяет Telegram Topics по структуре и паттернам взаимодействия, но использует только social data.',
      title: 'Как устроены Social Topics',
      overview: 'Social Topics показывает темы social media в том же split-view формате: список тем слева, а справа статистика, тренд, AI overview shell и доказательства.',
      sectionTitle: 'Что используется',
      items: [
        'Темы строятся только из social topic intelligence и не смешиваются с Telegram-данными.',
        'Группы слева выводятся в таком же виде, как на основной Topics page, но для social используются вычисленные topic groups.',
        'Правая колонка повторяет тот же ритм: заголовок темы, 4 stat cards, topic trend, AI overview и evidence feed.',
        'URL поддерживает deep-link в тему, режим доказательств и конкретное evidence-сообщение.',
      ],
      noteTitle: 'Важно',
      note: 'Если social AI summary ещё неполный, страница сохраняет тот же Telegram shell и показывает честный social-aware fallback внутри него.',
      ariaLabel: 'О странице social topics',
      badgeLabel: 'О странице',
    }
    : {
      summary: 'Social Topics now mirrors Telegram Topics end-to-end while staying grounded in social-only topic, trend, and evidence data.',
      title: 'How Social Topics Works',
      overview: 'Social Topics uses the same split-view Topics experience: a topic list on the left, then stats, trend, AI overview shell, and evidence on the right.',
      sectionTitle: 'What it uses',
      items: [
        'Topics come only from the social topic intelligence flow and are never merged with Telegram data.',
        'The left-side topic groups follow the same Topics-page grouping pattern, but they are derived for social topics.',
        'The right column keeps the same rhythm: topic header, 4 stat cards, topic trend, AI overview, and evidence feed.',
        'The URL supports deep-linking into the topic, proof view, and a focused evidence item.',
      ],
      noteTitle: 'Important',
      note: 'If the social AI summary is still thin, the page keeps the same Telegram shell and shows an honest social-aware fallback inside it.',
      ariaLabel: 'About social topics',
      badgeLabel: 'Page guide',
    };
}

function dominantPlatform(topic: SocialTopicListItem): string {
  return normalizeTopicKey(topic.topPlatforms[0] || 'social');
}

function topicCorpus(topic: SocialTopicListItem): string {
  return [
    topic.topic,
    topic.sampleSummary,
    topic.topEntities.join(' '),
    topic.topPlatforms.join(' '),
  ]
    .join(' ')
    .toLowerCase();
}

function includesAny(text: string, needles: string[]) {
  return needles.some((needle) => text.includes(needle));
}

function inferSocialTopicGroup(topic: SocialTopicListItem): SocialTopicGroup {
  const corpus = topicCorpus(topic);

  if (includesAny(corpus, [
    'bank', 'banking', 'cashback', 'payment', 'pay', 'card', 'credit', 'loan', 'deposit',
    'mortgage', 'finance', 'financial', 'wallet', 'transfer', 'rewards', 'mastercard', 'visa', 'qr',
  ])) return 'Finance';

  if (includesAny(corpus, [
    'travel', 'shopping', 'retail', 'restaurant', 'food', 'beauty', 'fashion',
    'offer', 'benefit', 'lifestyle', 'entertainment', 'promo',
  ])) return 'Lifestyle';

  if (includesAny(corpus, [
    'app', 'mobile', 'login', 'service', 'support', 'feature', 'digital', 'online',
    'website', 'product', 'experience', 'ux', 'bug',
  ])) return 'Living';

  if (includesAny(corpus, [
    'trust', 'customer', 'family', 'child', 'safety', 'education', 'school', 'health', 'care',
  ])) return 'Family';

  if (includesAny(corpus, [
    'diaspora', 'migration', 'cross-border', 'cross border', 'regional', 'region',
    'international', 'global', 'remittance', 'community reach',
  ])) return 'Integration';

  if (includesAny(corpus, [
    'market', 'campaign', 'competitor', 'competition', 'brand', 'performance',
    'ads', 'advertising', 'sales', 'business', 'industry', 'benchmark',
  ])) return 'Work';

  return 'Admin';
}

function deriveSocialCategory(topic: SocialTopicListItem, ru: boolean) {
  const corpus = topicCorpus(topic);
  if (includesAny(corpus, ['bank', 'banking', 'payment', 'cashback', 'card', 'deposit', 'credit', 'wallet', 'qr'])) {
    return ru ? 'Банкинг и платежи' : 'Banking & Payments';
  }
  if (includesAny(corpus, ['travel', 'shopping', 'offer', 'benefit', 'retail', 'promo', 'rewards'])) {
    return ru ? 'Предложения и выгоды' : 'Offers & Benefits';
  }
  if (includesAny(corpus, ['app', 'mobile', 'login', 'digital', 'feature', 'website', 'product', 'experience'])) {
    return ru ? 'Продуктовый опыт' : 'Product Experience';
  }
  if (includesAny(corpus, ['trust', 'customer', 'support', 'care', 'safety'])) {
    return ru ? 'Доверие клиентов' : 'Customer Trust';
  }
  if (includesAny(corpus, ['regional', 'cross-border', 'global', 'community'])) {
    return ru ? 'Охват и интеграция' : 'Reach & Integration';
  }
  if (includesAny(corpus, ['market', 'campaign', 'competitor', 'brand', 'business', 'performance'])) {
    return ru ? 'Бренд и конкуренция' : 'Brand & Competition';
  }
  return ru ? 'Social операции' : 'Social Operations';
}

function sentimentPct(topic: SocialTopicListItem, key: 'positive' | 'neutral' | 'negative'): number {
  const total = topic.count || 0;
  if (total <= 0) return 0;
  return Math.round(((topic.sentimentCounts[key] || 0) / total) * 100);
}

function formatTopicTrendTick(bucket: string, lang: 'en' | 'ru'): string {
  return formatSocialBucket(bucket, lang);
}

function formatTopicTrendTooltip(bucket: string, lang: 'en' | 'ru'): string {
  return formatSocialDateLabel(bucket, lang);
}

function formatOverviewMetaDate(value: string, lang: 'en' | 'ru'): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat(lang === 'ru' ? 'ru-RU' : 'en-US', {
    year: 'numeric',
    month: 'short',
    day: 'numeric',
    timeZone: 'UTC',
  }).format(date);
}

function evidenceAuthor(item: SocialEvidenceItem) {
  return item.author_handle || item.entity?.name || 'Unknown';
}

function evidenceChannel(item: SocialEvidenceItem, lang: 'en' | 'ru') {
  return item.entity?.name || socialPlatformLabel(item.platform, lang);
}

function evidenceText(item: SocialEvidenceItem) {
  return item.text_content?.trim() || socialActivitySummary(item);
}

function isQuestionEvidence(item: SocialEvidenceItem) {
  const text = `${item.text_content || ''} ${item.analysis?.summary || ''}`.trim().toLowerCase();
  if (!text) return false;
  if (text.includes('?')) return true;
  return /^(how|what|why|when|where|who|can|should|is|are|do|does|как|что|почему|когда|где|кто|можно|нужно|стоит|есть)\b/.test(text);
}

function buildOverviewSignals(topic: SocialTopicViewModel, lang: 'en' | 'ru') {
  const ru = lang === 'ru';
  const entities = topic.topEntities.slice(0, 2).join(ru ? ' и ' : ' and ');
  const platforms = topic.displayPlatforms.slice(0, 2).join(', ');

  return [
    entities
      ? (ru
        ? `Тема чаще всего связана с ${entities}.`
        : `This topic is most often tied to ${entities}.`)
      : (ru
        ? 'Тема распределена между несколькими social-источниками.'
        : 'This topic is spread across multiple social sources.'),
    platforms
      ? (ru
        ? `Основные площадки обсуждения: ${platforms}.`
        : `The main discussion surfaces are ${platforms}.`)
      : (ru
        ? 'Основная social-площадка пока не выделилась явно.'
        : 'No single social surface dominates this topic yet.'),
    ru
      ? `В выбранном окне тема набрала ${topic.count.toLocaleString()} упоминаний и изменение ${topic.growthPct > 0 ? '+' : ''}${topic.growthPct}%.`
      : `In the selected window this topic generated ${topic.count.toLocaleString()} mentions with ${topic.growthPct > 0 ? '+' : ''}${topic.growthPct}% momentum.`,
  ];
}

function buildOverviewSummary(topic: SocialTopicViewModel, lang: 'en' | 'ru') {
  const ru = lang === 'ru';
  const strongestSentiment = topic.negativePct >= topic.positivePct
    ? (ru ? 'негативный' : 'negative')
    : (ru ? 'позитивный' : 'positive');

  if (ru) {
    return topic.sampleSummary?.trim() || `${topic.topic} сейчас формируется в social media как ${strongestSentiment} импульс вокруг темы ${topic.categoryLabelRu.toLowerCase()}, где внимание держится за счёт ${topic.count.toLocaleString()} упоминаний и наиболее заметных обсуждений на ${topic.displayPlatforms.slice(0, 2).join(', ') || 'social площадках'}.`;
  }

  return topic.sampleSummary?.trim() || `${topic.topic} is currently showing a ${strongestSentiment} social signal around ${topic.categoryLabelEn.toLowerCase()}, driven by ${topic.count.toLocaleString()} mentions and the strongest activity on ${topic.displayPlatforms.slice(0, 2).join(', ') || 'social surfaces'}.`;
}

function buildSocialOverview(
  topic: SocialTopicViewModel,
  evidenceItems: SocialEvidenceItem[],
  range: { from: string; to: string },
): SocialTopicOverview {
  const generatedAt = evidenceItems[0]?.published_at || `${range.to}T00:00:00Z`;
  const status: SocialTopicOverview['status'] = topic.sampleSummary?.trim()
    ? 'ready'
    : evidenceItems.length > 0
      ? 'fallback'
      : 'unavailable';

  return {
    status,
    summaryEn: buildOverviewSummary(topic, 'en'),
    summaryRu: buildOverviewSummary(topic, 'ru'),
    signalsEn: buildOverviewSignals(topic, 'en'),
    signalsRu: buildOverviewSignals(topic, 'ru'),
    generatedAt,
    windowStart: range.from,
    windowEnd: range.to,
    windowDays: differenceInDaysInclusive(range.from, range.to),
  };
}

export function SocialTopicsPage() {
  const { lang } = useLanguage();
  const { range } = useDashboardDateRange();
  const [searchParams, setSearchParams] = useSearchParams();
  const ru = lang === 'ru';

  const entityParam = searchParams.get('entity') || 'all';
  const platformParam = (searchParams.get('platform') || 'all') as SocialPlatform;
  const requestedTopic = (searchParams.get('topic') || '').trim();
  const requestedView = (searchParams.get('view') || '').trim();
  const requestedEvidenceId = (searchParams.get('evidenceId') || '').trim();

  const [searchQuery, setSearchQuery] = useState('');
  const [selectedCategory, setSelectedCategory] = useState<SocialTopicGroup>('All');
  const [sortBy, setSortBy] = useState<'mentions' | 'growth'>('mentions');
  const [proofView, setProofView] = useState<'evidence' | 'questions'>(requestedView === 'questions' ? 'questions' : 'evidence');
  const [highlightEvidenceId, setHighlightEvidenceId] = useState('');

  const filters: SocialIntelligenceFilters = useMemo(() => ({
    from: range.from,
    to: range.to,
    entityId: entityParam !== 'all' ? entityParam : undefined,
    platform: platformParam,
  }), [entityParam, platformParam, range.from, range.to]);

  const {
    topics,
    loading: topicsLoading,
    error: topicsError,
    accessDenied: listAccessDenied,
    refresh: refreshTopics,
  } = useSocialTopicListData(filters);

  const topicViewModels = useMemo<SocialTopicViewModel[]>(() => (
    topics.map((topic) => {
      const topicGroup = inferSocialTopicGroup(topic);
      const color = categoryColors[topicGroup] || categoryColors.Admin;
      const categoryLabelEn = deriveSocialCategory(topic, false);
      const categoryLabelRu = deriveSocialCategory(topic, true);
      const positivePct = sentimentPct(topic, 'positive');
      const neutralPct = sentimentPct(topic, 'neutral');
      const negativePct = sentimentPct(topic, 'negative');
      const displayPlatforms = topic.topPlatforms.map((item) => socialPlatformLabel(item, lang));
      const topChannels = (topic.topEntities.length > 0 ? topic.topEntities : displayPlatforms).slice(0, 4);
      return {
        ...topic,
        id: topic.topic,
        name: topic.topic,
        nameRu: topic.topic,
        sourceTopic: topic.topic,
        mentions: topic.count,
        growth: topic.growthPct,
        topicGroup,
        category: categoryLabelEn,
        description: topic.sampleSummary?.trim() || categoryLabelEn,
        descriptionRu: topic.sampleSummary?.trim() || categoryLabelRu,
        categoryLabelEn,
        categoryLabelRu,
        color,
        sentiment: {
          positive: positivePct,
          neutral: neutralPct,
          negative: negativePct,
        },
        positivePct,
        neutralPct,
        negativePct,
        weeklyData: [],
        topChannels,
        evidenceCount: topic.count,
        displayPlatforms,
      };
    })
  ), [lang, topics]);

  const selectedTopic = useMemo(() => {
    if (!requestedTopic) return null;
    const target = normalizeTopicKey(requestedTopic);
    return topicViewModels.find((topic) =>
      normalizeTopicKey(topic.sourceTopic || topic.name) === target
      || normalizeTopicKey(topic.name) === target
      || normalizeTopicKey(topic.nameRu) === target,
    ) || null;
  }, [requestedTopic, topicViewModels]);

  const {
    timeline,
    evidenceItems,
    evidenceCount,
    loading: detailLoading,
    loadingMore,
    error: detailError,
    accessDenied: detailAccessDenied,
    hasMore,
    refresh: refreshDetail,
    loadMore,
  } = useSocialTopicDetailData(filters, selectedTopic?.topic || null, Boolean(selectedTopic));

  useEffect(() => {
    setProofView(requestedView === 'questions' ? 'questions' : 'evidence');
  }, [requestedView]);

  const accessDenied = listAccessDenied || detailAccessDenied;
  const totalMentions = topicViewModels.reduce((sum, topic) => sum + topic.mentions, 0);
  const requestedTopicMissing = Boolean(requestedTopic && !topicsLoading && !selectedTopic);

  const filtered = topicViewModels
    .filter((topic) => {
      if (selectedCategory !== 'All' && topic.topicGroup !== selectedCategory) return false;
      const displayName = ru ? topic.nameRu : topic.name;
      if (searchQuery && !displayName.toLowerCase().includes(searchQuery.toLowerCase())) return false;
      return true;
    })
    .sort((a, b) => {
      if (sortBy === 'mentions') {
        return (
          (b.mentions - a.mentions)
          || (b.deltaCount - a.deltaCount)
          || (b.sentiment.negative - a.sentiment.negative)
          || a.name.localeCompare(b.name)
        );
      }
      return (
        (b.deltaCount - a.deltaCount)
        || (b.growth - a.growth)
        || (b.mentions - a.mentions)
        || a.name.localeCompare(b.name)
      );
    });

  const activeTimeline = timeline.map((item) => ({
    week: item.bucket,
    count: item.total,
  }));
  const activeTopic = selectedTopic
    ? {
      ...selectedTopic,
      weeklyData: activeTimeline,
      evidenceCount,
    }
    : null;
  const topicAccentColor = activeTopic?.color || selectedTopic?.color || '#64748b';
  const activeOverview = selectedTopic ? buildSocialOverview(selectedTopic, evidenceItems, range) : null;
  const overviewState = activeOverview?.status || 'unavailable';
  const visibleEvidence = proofView === 'questions'
    ? evidenceItems.filter(isQuestionEvidence)
    : evidenceItems;
  const visibleEvidenceCount = proofView === 'questions'
    ? visibleEvidence.length
    : evidenceCount;

  useEffect(() => {
    if (!selectedTopic || !requestedEvidenceId) return;
    if (!visibleEvidence.some((item) => item.id === requestedEvidenceId)) return;

    const domId = `${proofView}-evidence-${requestedEvidenceId}`;
    const scrollToEvidence = () => {
      const el = document.getElementById(domId);
      if (!el) return false;
      el.scrollIntoView({ behavior: 'smooth', block: 'center' });
      setHighlightEvidenceId(requestedEvidenceId);
      return true;
    };

    let timeoutId: number | null = null;
    if (!scrollToEvidence()) {
      timeoutId = window.setTimeout(scrollToEvidence, 120);
    }

    const clearId = window.setTimeout(() => setHighlightEvidenceId(''), 2600);
    return () => {
      if (timeoutId) window.clearTimeout(timeoutId);
      window.clearTimeout(clearId);
    };
  }, [proofView, requestedEvidenceId, selectedTopic, visibleEvidence]);

  const selectTopic = (topic: SocialTopicViewModel, view: 'evidence' | 'questions' = proofView, evidenceId?: string) => {
    const next = new URLSearchParams(searchParams);
    next.set('topic', topic.sourceTopic || topic.name);
    next.set('view', view);
    if (evidenceId) next.set('evidenceId', evidenceId);
    else next.delete('evidenceId');
    setSearchParams(next);
  };

  const clearTopicSelection = () => {
    const next = new URLSearchParams(searchParams);
    next.delete('topic');
    next.delete('view');
    next.delete('evidenceId');
    setSearchParams(next);
  };

  if (accessDenied) {
    return (
      <SocialAccessDeniedState
        title={ru ? 'Social topics доступны только оператору' : 'Social topics are operator-only'}
        description={ru
          ? 'Эта страница использует operator-only social endpoints. Войдите под операторской учётной записью, чтобы открыть social topic evidence.'
          : 'This page is backed by operator-only social endpoints. Sign in with an operator session to inspect social topic evidence.'}
      />
    );
  }

  return (
    <div className="flex flex-col md:flex-row h-full">
      <div className={`${selectedTopic ? 'hidden md:flex md:w-[420px]' : 'flex flex-1'} flex-col border-r border-gray-200 bg-white transition-all`}>
        <div className="px-6 py-5 border-b border-gray-200">
          <div className="flex items-center justify-between mb-3">
            <div>
              <div className="flex items-center gap-2">
                <h1 className="text-gray-900" style={{ fontSize: '1.25rem', fontWeight: 600 }}>
                  {ru ? 'Темы' : 'Topics'}
                </h1>
                <PageInfoButton copy={socialTopicsInfoCopy(lang)} />
              </div>
              <p className="text-xs text-gray-500 mt-0.5">
                {topicViewModels.length} {ru ? 'тем отслеживается' : 'topics tracked'} &middot; {totalMentions.toLocaleString()} {ru ? 'всего упоминаний' : 'total mentions'}
              </p>
            </div>
          </div>

          <div className="relative mb-3">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
            <input
              type="text"
              value={searchQuery}
              onChange={(event) => setSearchQuery(event.target.value)}
              placeholder={ru ? 'Поиск тем...' : 'Search topics...'}
              className="w-full pl-9 pr-4 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent bg-gray-50"
            />
          </div>

          <div className="flex gap-1.5 flex-wrap">
            {TOPICS_PAGE_GROUPS_EN.map((category) => (
              <button
                key={category}
                type="button"
                onClick={() => setSelectedCategory(category as SocialTopicGroup)}
                className={`px-2.5 py-1 rounded-full text-xs transition-colors ${
                  selectedCategory === category
                    ? 'bg-slate-800 text-white'
                    : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                }`}
                style={{ fontWeight: selectedCategory === category ? 500 : 400 }}
              >
                {category !== 'All' && (
                  <span className="inline-block w-2 h-2 rounded-full mr-1.5" style={{ backgroundColor: categoryColors[category] }} />
                )}
                {translateTopicsPageGroup(category, ru)}
              </button>
            ))}
          </div>

          <div className="flex items-center gap-2 mt-3">
            <span className="text-xs text-gray-400">{ru ? 'Сортировка:' : 'Sort by:'}</span>
            <button
              type="button"
              onClick={() => setSortBy('mentions')}
              className={`text-xs px-2 py-0.5 rounded ${sortBy === 'mentions' ? 'bg-blue-50 text-blue-700' : 'text-gray-500 hover:text-gray-700'}`}
              style={{ fontWeight: sortBy === 'mentions' ? 500 : 400 }}
            >
              {ru ? 'По обсуждаемости' : 'Most discussed'}
            </button>
            <button
              type="button"
              onClick={() => setSortBy('growth')}
              className={`text-xs px-2 py-0.5 rounded ${sortBy === 'growth' ? 'bg-blue-50 text-blue-700' : 'text-gray-500 hover:text-gray-700'}`}
              style={{ fontWeight: sortBy === 'growth' ? 500 : 400 }}
            >
              {ru ? 'По росту' : 'Fastest growing'}
            </button>
          </div>
        </div>

        <div className="flex-1 overflow-y-auto">
          {requestedTopicMissing && (
            <div className="px-6 py-3 border-b border-amber-100 bg-amber-50 flex items-center justify-between gap-3">
              <div className="min-w-0">
                <p className="text-xs text-amber-900" style={{ fontWeight: 600 }}>
                  {ru ? 'Эта тема не найдена в выбранном окне.' : 'This topic has no evidence in the selected date window.'}
                </p>
                <p className="text-[11px] text-amber-800 mt-0.5 truncate">
                  {ru ? `Период: ${range.from} — ${range.to}` : `Window: ${range.from} — ${range.to}`}
                </p>
              </div>
              <button
                type="button"
                onClick={clearTopicSelection}
                className="text-xs text-amber-900 hover:text-amber-950 underline"
                style={{ fontWeight: 600 }}
              >
                {ru ? 'Сбросить' : 'Clear'}
              </button>
            </div>
          )}

          {topicsError && (
            <div className="px-6 py-3 border-b border-red-100 bg-red-50 flex items-center justify-between gap-2">
              <span className="text-xs text-red-700 truncate">
                {ru ? 'Не удалось обновить темы. Показаны последние сохранённые данные.' : 'Unable to refresh topics. Showing last saved data.'}
              </span>
              <button
                type="button"
                onClick={refreshTopics}
                className="text-xs text-red-700 hover:text-red-800 underline"
                style={{ fontWeight: 600 }}
              >
                {ru ? 'Повторить' : 'Retry'}
              </button>
            </div>
          )}

          {topicsLoading && topicViewModels.length === 0 && (
            <div className="flex flex-col items-center justify-center py-12 text-gray-400">
              <Hash className="w-8 h-8 mb-2" />
              <p className="text-sm">{ru ? 'Загружаем темы...' : 'Loading topics...'}</p>
            </div>
          )}

          {filtered.map((topic) => (
            <button
              key={topic.id}
              type="button"
              onClick={() => selectTopic(topic, proofView)}
              className={`w-full text-left px-6 py-4 border-b border-gray-100 hover:bg-gray-50 transition-colors ${
                selectedTopic?.id === topic.id ? 'bg-blue-50 border-l-3 border-l-blue-500' : ''
              }`}
            >
              <div className="flex items-start justify-between">
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2">
                    <div className="w-2.5 h-2.5 rounded-full flex-shrink-0" style={{ backgroundColor: topic.color }} />
                    <span className="text-sm text-gray-900 truncate" style={{ fontWeight: 500 }}>{ru ? topic.nameRu : topic.name}</span>
                    <span className="text-xs px-1.5 py-0.5 rounded bg-gray-100 text-gray-500 flex-shrink-0">
                      {translateCategory(topic.category, ru)}
                    </span>
                  </div>
                  <div className="flex items-center gap-3 mt-1.5 ml-4.5">
                    <span className="text-xs text-gray-500">{topic.mentions.toLocaleString()} {ru ? 'упоминаний' : 'mentions'}</span>
                    <span className={`text-xs flex items-center gap-0.5 ${topic.growth > 0 ? 'text-emerald-600' : 'text-red-500'}`} style={{ fontWeight: 600 }}>
                      {topic.growth > 0 ? <TrendingUp className="w-3 h-3" /> : <TrendingDown className="w-3 h-3" />}
                      {topic.growth > 0 ? '+' : ''}{topic.growth}%
                    </span>
                    <div className="flex items-center gap-0.5 ml-auto">
                      <div className="flex h-1.5 w-16 rounded-full overflow-hidden">
                        <div className="bg-emerald-400" style={{ width: `${topic.sentiment.positive}%` }} />
                        <div className="bg-gray-300" style={{ width: `${topic.sentiment.neutral}%` }} />
                        <div className="bg-red-400" style={{ width: `${topic.sentiment.negative}%` }} />
                      </div>
                    </div>
                  </div>
                </div>
                <ChevronRight className="w-4 h-4 text-gray-300 mt-1 ml-2 flex-shrink-0" />
              </div>
            </button>
          ))}

          {!topicsLoading && filtered.length === 0 && (
            <div className="flex flex-col items-center justify-center py-12 text-gray-400">
              <Hash className="w-8 h-8 mb-2" />
              <p className="text-sm">{ru ? 'Темы не найдены' : 'No topics match your filters'}</p>
            </div>
          )}
        </div>
      </div>

      {selectedTopic ? (
        <div className="flex-1 flex flex-col overflow-hidden bg-gray-50">
          <div className="md:hidden flex items-center gap-2 px-4 py-3 bg-white border-b border-gray-200 flex-shrink-0">
            <button
              type="button"
              onClick={clearTopicSelection}
              className="flex items-center gap-1.5 text-sm text-blue-600 active:opacity-70 transition-opacity"
              style={{ fontWeight: 500 }}
            >
              <ChevronLeft className="w-4 h-4" />
              {ru ? 'Все темы' : 'All Topics'}
            </button>
          </div>

          <div className="bg-white border-b border-gray-200 px-6 py-5">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-3">
                <div className="w-10 h-10 rounded-xl flex items-center justify-center" style={{ backgroundColor: `${topicAccentColor}15` }}>
                  <Hash className="w-5 h-5" style={{ color: topicAccentColor }} />
                </div>
                <div>
                  <h2 className="text-gray-900" style={{ fontSize: '1.1rem', fontWeight: 600 }}>{ru ? selectedTopic.nameRu : selectedTopic.name}</h2>
                  <p className="text-xs text-gray-500">{activeTopic ? (ru ? activeTopic.descriptionRu : activeTopic.description) : ''}</p>
                </div>
              </div>
              <button type="button" onClick={clearTopicSelection} className="p-2 hover:bg-gray-100 rounded-lg transition-colors">
                <X className="w-5 h-5 text-gray-400" />
              </button>
            </div>

            <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mt-4">
              {[
                { label: ru ? 'Упоминания' : 'Mentions', value: (activeTopic?.mentions || 0).toLocaleString(), color: 'text-gray-900' },
                { label: ru ? 'Рост' : 'Growth', value: `${(activeTopic?.growth || 0) > 0 ? '+' : ''}${activeTopic?.growth || 0}%`, color: (activeTopic?.growth || 0) > 0 ? 'text-emerald-600' : 'text-red-500' },
                { label: ru ? 'Позитив' : 'Positive', value: `${activeTopic?.sentiment.positive || 0}%`, color: 'text-emerald-600' },
                { label: ru ? 'Негатив' : 'Negative', value: `${activeTopic?.sentiment.negative || 0}%`, color: 'text-red-500' },
              ].map((stat) => (
                <div key={stat.label} className="bg-gray-50 rounded-lg px-3 py-2.5">
                  <p className="text-xs text-gray-500">{stat.label}</p>
                  <p className={`text-lg ${stat.color}`} style={{ fontWeight: 600 }}>{stat.value}</p>
                </div>
              ))}
            </div>

            <div className="mt-4">
              <ResponsiveContainer width="100%" height={120}>
                <AreaChart data={activeTopic?.weeklyData || []}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" />
                  <XAxis
                    dataKey="week"
                    tick={{ fontSize: 10 }}
                    stroke="#9ca3af"
                    interval="preserveStartEnd"
                    minTickGap={28}
                    tickFormatter={(value) => formatTopicTrendTick(String(value || ''), lang)}
                  />
                  <YAxis tick={{ fontSize: 10 }} stroke="#9ca3af" hide />
                  <Tooltip labelFormatter={(value) => formatTopicTrendTooltip(String(value || ''), lang)} />
                  <Area type="monotone" dataKey="count" stroke={topicAccentColor} fill={`${topicAccentColor}20`} strokeWidth={2} />
                </AreaChart>
              </ResponsiveContainer>
            </div>

            <div className="mt-4 rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
              <div className="flex items-start justify-between gap-3">
                <div className="flex items-center gap-3">
                  <div
                    className="w-9 h-9 rounded-xl flex items-center justify-center flex-shrink-0"
                    style={{ backgroundColor: `${topicAccentColor}18` }}
                  >
                    <Sparkles className="w-4 h-4" style={{ color: topicAccentColor }} />
                  </div>
                  <div>
                    <p className="text-[11px] uppercase tracking-[0.16em] text-slate-500" style={{ fontWeight: 700 }}>
                      {ru ? 'AI-обзор темы' : 'AI Topic Overview'}
                    </p>
                    <p className="text-[11px] text-slate-400 mt-0.5">
                      {activeOverview?.windowStart && activeOverview?.windowEnd
                        ? (ru
                          ? `Скользящее AI-окно: ${activeOverview.windowStart} — ${activeOverview.windowEnd}`
                          : `Rolling AI window: ${activeOverview.windowStart} — ${activeOverview.windowEnd}`)
                        : (ru ? 'Скользящее AI-окно по последним данным' : 'Rolling AI window from trusted recent data')}
                    </p>
                  </div>
                </div>
                {overviewState === 'fallback' && (
                  <span className="text-[10px] px-2 py-1 rounded-full bg-amber-50 text-amber-700 border border-amber-200 flex-shrink-0" style={{ fontWeight: 600 }}>
                    {ru ? 'Резервный режим' : 'Fallback'}
                  </span>
                )}
              </div>

              {detailLoading && timeline.length === 0 && !selectedTopic.sampleSummary ? (
                <div className="mt-3 space-y-2 animate-pulse">
                  <div className="h-3 rounded bg-slate-100 w-full" />
                  <div className="h-3 rounded bg-slate-100 w-11/12" />
                  <div className="grid gap-2 pt-1">
                    <div className="h-9 rounded-xl bg-slate-50 border border-slate-100" />
                    <div className="h-9 rounded-xl bg-slate-50 border border-slate-100" />
                    <div className="h-9 rounded-xl bg-slate-50 border border-slate-100" />
                  </div>
                </div>
              ) : overviewState === 'unavailable' ? (
                <div className="mt-3 rounded-xl border border-slate-200 bg-slate-50 px-4 py-3">
                  <p className="text-sm text-slate-700" style={{ fontWeight: 600 }}>
                    {ru ? 'AI-обзор темы ещё не готов.' : 'AI Topic Overview is not ready yet.'}
                  </p>
                  <p className="text-xs text-slate-500 mt-1">
                    {ru
                      ? 'Детали темы, динамика и доказательства уже доступны. Обзор появится автоматически после следующего social refresh.'
                      : 'Topic evidence, trend, and detail data are already available. The overview will appear automatically after the next social refresh.'}
                  </p>
                </div>
              ) : (
                <>
                  <p className="mt-3 text-sm text-slate-700 leading-relaxed">
                    {ru ? activeOverview?.summaryRu : activeOverview?.summaryEn}
                  </p>
                  <div className="mt-3 space-y-2">
                    {(ru ? activeOverview?.signalsRu : activeOverview?.signalsEn)?.slice(0, 3).map((signal) => (
                      <div key={signal} className="rounded-xl border border-slate-200 bg-slate-50 px-3 py-2.5">
                        <p className="text-xs text-slate-700 leading-relaxed" style={{ fontWeight: 500 }}>
                          {signal}
                        </p>
                      </div>
                    ))}
                  </div>
                  <div className="mt-3 flex flex-wrap items-center justify-between gap-2 text-[11px] text-slate-400">
                    <span>
                      {ru
                        ? `Основано на скользящем AI-окне ${activeOverview?.windowDays || 0} дн.`
                        : `Based on a rolling ${activeOverview?.windowDays || 0}-day AI window.`}
                    </span>
                    <span>
                      {ru ? 'Обновлено ' : 'Updated '}
                      {activeOverview ? formatOverviewMetaDate(activeOverview.generatedAt, lang) : range.to}
                    </span>
                  </div>
                </>
              )}
            </div>

            <div className="flex items-center gap-2 mt-3">
              <span className="text-xs text-gray-400">{ru ? 'Ведущие каналы:' : 'Top channels:'}</span>
              {(activeTopic?.topChannels || []).map((item) => (
                <span key={item} className="text-xs px-2 py-0.5 bg-gray-100 text-gray-600 rounded-full">{item}</span>
              ))}
            </div>
          </div>

          <div className="flex-1 overflow-y-auto px-6 py-4">
            {detailError && (
              <div className="mb-3 px-4 py-3 border border-red-100 bg-red-50 rounded-xl flex items-center justify-between gap-3">
                <span className="text-xs text-red-700 truncate">
                  {ru ? 'Не удалось загрузить детали темы. Показаны краткие данные.' : 'Unable to load full topic details. Showing summary data.'}
                </span>
                <button
                  type="button"
                  onClick={refreshDetail}
                  className="text-xs text-red-700 hover:text-red-800 underline"
                  style={{ fontWeight: 600 }}
                >
                  {ru ? 'Повторить' : 'Retry'}
                </button>
              </div>
            )}
            {detailLoading && evidenceItems.length === 0 && (
              <div className="mb-3 px-4 py-3 border border-blue-100 bg-blue-50 rounded-xl text-xs text-blue-700">
                {ru ? 'Загружаем доказательства и динамику темы...' : 'Loading topic evidence and trend details...'}
              </div>
            )}

            <div className="flex items-center gap-2 mb-3">
              <button
                type="button"
                onClick={() => selectTopic(selectedTopic, 'evidence')}
                className={`text-xs px-2.5 py-1 rounded-full ${proofView === 'evidence' ? 'bg-blue-50 text-blue-700' : 'bg-gray-100 text-gray-600 hover:bg-gray-200'}`}
                style={{ fontWeight: proofView === 'evidence' ? 600 : 500 }}
              >
                {ru ? 'Все доказательства' : 'All evidence'}
              </button>
              <button
                type="button"
                onClick={() => selectTopic(selectedTopic, 'questions')}
                className={`text-xs px-2.5 py-1 rounded-full ${proofView === 'questions' ? 'bg-amber-50 text-amber-700' : 'bg-gray-100 text-gray-600 hover:bg-gray-200'}`}
                style={{ fontWeight: proofView === 'questions' ? 600 : 500 }}
              >
                {ru ? 'Вопросы (доказательства)' : 'Questions proof'}
              </button>
            </div>

            <div className="flex items-center justify-between mb-4">
              <h3 className="text-sm text-gray-900" style={{ fontWeight: 600 }}>
                {proofView === 'questions'
                  ? (ru ? `Вопросы по теме (${visibleEvidenceCount})` : `Questions for this topic (${visibleEvidenceCount})`)
                  : (ru ? `Доказательства (${visibleEvidenceCount} публикаций и комментариев)` : `Evidence (${visibleEvidenceCount} posts & comments)`)}
              </h3>
              <div className="flex items-center gap-3">
                <span className="text-xs text-gray-400">
                  {proofView === 'questions'
                    ? (ru ? 'Показаны реальные вопросы из сообщений' : 'Showing real question-style messages')
                    : (ru ? 'Сообщения по данной теме' : 'Messages mentioning this topic')}
                </span>
              </div>
            </div>

            {detailLoading && visibleEvidence.length === 0 ? (
              <div className="mb-3 px-4 py-3 border border-blue-100 bg-blue-50 rounded-xl text-xs text-blue-700">
                {proofView === 'questions'
                  ? (ru ? 'Загружаем вопросы по теме...' : 'Loading topic questions...')
                  : (ru ? 'Загружаем доказательства по теме...' : 'Loading topic evidence...')}
              </div>
            ) : visibleEvidence.length === 0 ? (
              <div className="flex flex-col items-center justify-center py-16 text-gray-400">
                <MessageCircle className="w-8 h-8 mb-2" />
                <p className="text-sm">
                  {proofView === 'questions'
                    ? (ru ? 'Реальные вопросы пока не найдены' : 'No real questions found yet')
                    : (ru ? 'Данные ещё не загружены' : 'No evidence loaded yet')}
                </p>
                <p className="text-xs mt-1">
                  {proofView === 'questions'
                    ? (ru ? 'Проверьте другие темы или расширьте период сбора' : 'Try another topic or expand collection period')
                    : (ru ? 'Ожидаем social evidence для этой темы' : 'Waiting for social evidence for this topic')}
                </p>
              </div>
            ) : (
              <div className="space-y-3">
                {visibleEvidence.map((item) => {
                  const type = item.source_kind === 'comment'
                    ? (ru ? 'комментарий' : 'comment')
                    : item.source_kind === 'ad'
                      ? (ru ? 'объявление' : 'ad')
                      : (ru ? 'публикация' : 'post');

                  return (
                    <div
                      id={`${proofView}-evidence-${item.id}`}
                      key={item.id}
                      className={`bg-white rounded-xl border p-4 hover:shadow-sm transition-shadow ${
                        highlightEvidenceId === item.id
                          ? 'border-amber-300 ring-2 ring-amber-200'
                          : 'border-gray-200'
                      }`}
                    >
                      <div className="flex items-center justify-between mb-2">
                        <div className="flex items-center gap-2">
                          <div className="w-7 h-7 rounded-full bg-slate-100 flex items-center justify-center">
                            <User className="w-3.5 h-3.5 text-slate-500" />
                          </div>
                          <div>
                            <span className="text-xs text-gray-900" style={{ fontWeight: 600 }}>{evidenceAuthor(item)}</span>
                            <span className="text-xs text-gray-400 mx-1.5">{ru ? 'в' : 'in'}</span>
                            <span className="text-xs text-blue-600" style={{ fontWeight: 500 }}>{evidenceChannel(item, lang)}</span>
                          </div>
                        </div>
                        <div className="flex items-center gap-1.5 text-xs text-gray-400">
                          <Clock className="w-3 h-3" />
                          {formatSocialDateLabel(item.published_at, lang)}
                        </div>
                      </div>
                      <p className="text-sm text-gray-700 leading-relaxed mb-3">{evidenceText(item)}</p>
                      <div className="flex items-center gap-4">
                        <div className="flex items-center gap-1 text-xs text-gray-400">
                          <ThumbsUp className="w-3 h-3" />
                          —
                        </div>
                        <div className="flex items-center gap-1 text-xs text-gray-400">
                          <MessageCircle className="w-3 h-3" />
                          — {ru ? 'ответов' : 'replies'}
                        </div>
                        <span className={`text-xs px-1.5 py-0.5 rounded ${item.source_kind === 'post' ? 'bg-blue-50 text-blue-600' : item.source_kind === 'ad' ? 'bg-amber-50 text-amber-700' : 'bg-gray-100 text-gray-500'}`}>
                          {type}
                        </span>
                      </div>
                    </div>
                  );
                })}
                {hasMore && (
                  <div className="pt-2">
                    <button
                      type="button"
                      onClick={loadMore}
                      disabled={loadingMore}
                      className="w-full rounded-xl border border-gray-200 bg-white px-4 py-2.5 text-sm text-gray-700 hover:bg-gray-50 disabled:cursor-not-allowed disabled:opacity-60"
                      style={{ fontWeight: 600 }}
                    >
                      {loadingMore
                        ? (ru ? 'Загружаем ещё...' : 'Loading more...')
                        : (ru ? 'Показать ещё' : 'Load more')}
                    </button>
                  </div>
                )}
              </div>
            )}
          </div>
        </div>
      ) : (
        <div className="hidden md:flex flex-1 flex-col items-center justify-center bg-gray-50 text-gray-400 px-4 md:px-8">
          <div className="max-w-md text-center">
            <Hash className="w-12 h-12 mx-auto mb-4 text-gray-300" />
            <h3 className="text-gray-600 mb-1" style={{ fontSize: '1rem', fontWeight: 500 }}>
              {ru ? 'Выберите тему для изучения' : 'Select a topic to explore'}
            </h3>
            <p className="text-sm text-gray-400">
              {ru
                ? 'Нажмите на любую тему из списка, чтобы увидеть данные о трендах, тональности и реальные сообщения сообщества.'
                : 'Click any topic from the list to see its trend data, sentiment breakdown, and the actual community messages that mention it.'}
            </p>
          </div>
        </div>
      )}
    </div>
  );
}
