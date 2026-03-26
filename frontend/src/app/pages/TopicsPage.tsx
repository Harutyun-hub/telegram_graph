import { useEffect, useState } from 'react';
import { Search, TrendingUp, TrendingDown, MessageCircle, ThumbsUp, Hash, X, Clock, User, ChevronLeft, ChevronRight } from 'lucide-react';
import { ResponsiveContainer, AreaChart, Area, XAxis, YAxis, Tooltip, CartesianGrid } from 'recharts';
import { useSearchParams } from 'react-router';
import { useLanguage } from '../contexts/LanguageContext';
import { useDashboardDateRange } from '../contexts/DashboardDateRangeContext';
import { useTopicDetail, useTopicEvidenceFeed, useTopicsDetailData } from '../services/detailData';
import { TOPICS_PAGE_GROUPS_EN, translateCategory, translateTopicsPageGroup } from '../services/topicPresentation';
import type { TopicDetail } from '../types/data';
import { PageInfoButton, type PageInfoCopy } from '../components/ui/PageInfoButton';

const categoryColors: Record<string, string> = {
  Living: '#ef4444', Work: '#3b82f6', Family: '#8b5cf6',
  Finance: '#f59e0b', Lifestyle: '#ec4899', Integration: '#10b981',
  Admin: '#6b7280', Tech: '#06b6d4',
};

function formatTopicTrendTick(bucket: string, lang: 'en' | 'ru'): string {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(bucket)) return bucket;
  const date = new Date(`${bucket}T00:00:00Z`);
  if (Number.isNaN(date.getTime())) return bucket;
  return new Intl.DateTimeFormat(lang === 'ru' ? 'ru-RU' : 'en-US', {
    month: 'short',
    day: 'numeric',
    timeZone: 'UTC',
  }).format(date);
}

function formatTopicTrendTooltip(bucket: string, lang: 'en' | 'ru'): string {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(bucket)) return bucket;
  const date = new Date(`${bucket}T00:00:00Z`);
  if (Number.isNaN(date.getTime())) return bucket;
  return new Intl.DateTimeFormat(lang === 'ru' ? 'ru-RU' : 'en-US', {
    year: 'numeric',
    month: 'long',
    day: 'numeric',
    timeZone: 'UTC',
  }).format(date);
}

function topicsInfoCopy(lang: 'en' | 'ru'): PageInfoCopy {
  return lang === 'ru'
    ? {
      summary: 'Объясняет, откуда берутся темы, как они группируются и по какой логике ранжируются на этой странице.',
      title: 'Как формируются темы',
      overview: 'Темы на этой странице не создаются вручную. Они приходят из аналитической темы сообщения, извлечённой из постов и комментариев за выбранный период, а затем собираются в карточки для удобного просмотра.',
      sectionTitle: 'Что лежит в основе',
      items: [
        'Базовая тема берётся из исходного идентификатора темы и аналитической классификации, связанной с проанализированными сообщениями.',
        'Для каждой темы считаются упоминания, рост, тональность, уникальные участники, каналы и примеры доказательств.',
        'Группы во вкладках нужны для навигации: они строятся из исходной категории темы, чтобы список было проще фильтровать.',
        'При открытии темы загружаются её динамика, ключевые каналы и лента сообщений или вопросов в том же выбранном диапазоне дат.',
      ],
      noteTitle: 'Как читать страницу',
      note: 'Сортировка по росту показывает недавний импульс, а не общий объём. Поэтому быстро растущая тема может идти выше, чем более крупная, но стабильная.',
      ariaLabel: 'Объяснить, как формируются темы',
      badgeLabel: 'О странице',
    }
    : {
      summary: 'Explains where topics come from, how they are grouped, and how this page ranks them.',
      title: 'How Topics Are Built',
      overview: 'Topics on this page are not created manually. They are derived from the themes detected in community posts and comments within the selected date range, then grouped into cards for easier review.',
      sectionTitle: 'What it uses',
      items: [
        'Each topic starts from the detected theme attached to analyzed messages.',
        'Every topic card combines mentions, growth, sentiment mix, distinct participants, channel spread, and evidence examples.',
        'The tab groups are navigation buckets built from the underlying topic category so the list is easier to filter.',
        'Opening a topic loads its trend history, top channels, and the evidence or question feed for the same selected date range.',
      ],
      noteTitle: 'How to read it',
      note: 'Sorting by growth highlights recent momentum, not lifetime volume. A fast-rising topic can rank above a larger but steadier one.',
      ariaLabel: 'Explain how topics are built',
      badgeLabel: 'Page guide',
    };
}

// ── COMPONENT ──

export function TopicsPage() {
  const { lang } = useLanguage();
  const { range } = useDashboardDateRange();
  const ru = lang === 'ru';
  const [searchParams, setSearchParams] = useSearchParams();
  const {
    data: allTopics,
    loading: topicsLoading,
    error: topicsError,
    refresh: refreshTopics,
  } = useTopicsDetailData();
  const [searchQuery, setSearchQuery] = useState('');
  const [selectedCategory, setSelectedCategory] = useState('All');
  const [selectedTopic, setSelectedTopic] = useState<TopicDetail | null>(null);
  const requestedTopic = (searchParams.get('topic') || '').trim();
  const requestedTopicKey = requestedTopic || null;
  const {
    data: selectedTopicDetail,
    loading: topicDetailLoading,
    error: topicDetailError,
    refresh: refreshTopicDetail,
  } = useTopicDetail(
    selectedTopic?.sourceTopic || selectedTopic?.name || requestedTopicKey,
    selectedTopic?.category || null,
  );
  const [sortBy, setSortBy] = useState<'mentions' | 'growth'>('mentions');
  const [proofView, setProofView] = useState<'evidence' | 'questions'>('evidence');
  const [focusedEvidenceId, setFocusedEvidenceId] = useState('');
  const [highlightEvidenceId, setHighlightEvidenceId] = useState('');

  const normalizeTopicKey = (value: string) => value.trim().toLowerCase();

  useEffect(() => {
    const viewParam = searchParams.get('view');
    if (viewParam === 'questions' || viewParam === 'evidence') {
      setProofView(viewParam);
    }

    const evidenceParam = (searchParams.get('evidenceId') || '').trim();
    setFocusedEvidenceId(evidenceParam);

    const topicParam = normalizeTopicKey(requestedTopic);
    if (!topicParam || allTopics.length === 0) return;

    const fromQuery = allTopics.find((t) =>
      normalizeTopicKey(t.sourceTopic || t.name) === topicParam
        || normalizeTopicKey(t.name) === topicParam
        || normalizeTopicKey(t.nameRu) === topicParam,
    );
    if (fromQuery && selectedTopic?.id !== fromQuery.id) {
      setSelectedTopic(fromQuery);
    }
  }, [requestedTopic, searchParams, allTopics, selectedTopic?.id]);

  useEffect(() => {
    if (!requestedTopic || selectedTopic || !selectedTopicDetail) return;
    setSelectedTopic(selectedTopicDetail);
  }, [requestedTopic, selectedTopic, selectedTopicDetail]);

  const selectTopic = (topic: TopicDetail, view: 'evidence' | 'questions' = proofView, evidenceId?: string) => {
    setSelectedTopic(topic);
    setProofView(view);
    setFocusedEvidenceId(evidenceId || '');
    const next = new URLSearchParams(searchParams);
    next.set('topic', topic.sourceTopic || topic.name);
    next.set('view', view);
    if (evidenceId) next.set('evidenceId', evidenceId);
    else next.delete('evidenceId');
    setSearchParams(next);
  };

  const clearTopicSelection = () => {
    setSelectedTopic(null);
    setFocusedEvidenceId('');
    setHighlightEvidenceId('');
    const next = new URLSearchParams(searchParams);
    next.delete('topic');
    next.delete('view');
    next.delete('evidenceId');
    setSearchParams(next);
  };

  useEffect(() => {
    if (!selectedTopic) return;
    const fresh = allTopics.find((topic) => topic.id === selectedTopic.id);
    if (!fresh) {
      setSelectedTopic(null);
      return;
    }
    if (fresh !== selectedTopic) {
      setSelectedTopic(fresh);
    }
  }, [allTopics, selectedTopic]);
  const activeTopic = selectedTopicDetail || selectedTopic;
  const {
    data: evidenceFeed,
    loading: evidenceLoading,
    loadingMore: evidenceLoadingMore,
    error: evidenceError,
    refresh: refreshEvidence,
    loadMore: loadMoreEvidence,
  } = useTopicEvidenceFeed(
    selectedTopic?.sourceTopic || selectedTopic?.name || requestedTopicKey,
    selectedTopic?.category || selectedTopicDetail?.category || null,
    proofView,
    focusedEvidenceId || null,
    Boolean(selectedTopic || requestedTopic),
  );

  useEffect(() => {
    if (!selectedTopic || !focusedEvidenceId) return;
    const visibleEvidence = evidenceFeed.items;
    if (!visibleEvidence.some((ev) => ev.id === focusedEvidenceId)) return;

    const domId = `${proofView}-evidence-${focusedEvidenceId}`;
    const scrollToEvidence = () => {
      const el = document.getElementById(domId);
      if (!el) return false;
      el.scrollIntoView({ behavior: 'smooth', block: 'center' });
      setHighlightEvidenceId(focusedEvidenceId);
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
  }, [selectedTopic, proofView, focusedEvidenceId, evidenceFeed.items]);

  const categories = TOPICS_PAGE_GROUPS_EN;

  const filtered = allTopics
    .filter((t) => {
      if (selectedCategory !== 'All' && t.topicGroup !== selectedCategory) return false;
      const displayName = ru ? t.nameRu : t.name;
      if (searchQuery && !displayName.toLowerCase().includes(searchQuery.toLowerCase())) return false;
      return true;
    })
    .sort((a, b) => {
      if (sortBy === 'mentions') {
        return (
          (b.mentions - a.mentions)
          || ((b.distinctUsers || 0) - (a.distinctUsers || 0))
          || ((b.evidenceCount || 0) - (a.evidenceCount || 0))
          || (a.sourceTopic || a.name).localeCompare(b.sourceTopic || b.name)
        );
      }
      return (
        ((b.deltaMentions || 0) - (a.deltaMentions || 0))
        || (b.growth - a.growth)
        || ((b.distinctUsers || 0) - (a.distinctUsers || 0))
        || (b.mentions - a.mentions)
        || (a.sourceTopic || a.name).localeCompare(b.sourceTopic || b.name)
      );
    });

  const totalMentions = allTopics.reduce((s, t) => s + t.mentions, 0);
  const requestedTopicMissing = Boolean(
    requestedTopic
    && !topicsLoading
    && !selectedTopic
    && allTopics.length >= 0
    && !allTopics.some((t) => (
      normalizeTopicKey(t.sourceTopic || t.name) === normalizeTopicKey(requestedTopic)
      || normalizeTopicKey(t.name) === normalizeTopicKey(requestedTopic)
      || normalizeTopicKey(t.nameRu) === normalizeTopicKey(requestedTopic)
    )),
  );

  return (
    <div className="flex flex-col md:flex-row h-full">
      {/* LEFT: Topic List — hidden on mobile when a topic is selected */}
      <div className={`${selectedTopic ? 'hidden md:flex md:w-[420px]' : 'flex flex-1'} flex-col border-r border-gray-200 bg-white transition-all`}>
        {/* Header */}
        <div className="px-6 py-5 border-b border-gray-200">
          <div className="flex items-center justify-between mb-3">
            <div>
              <div className="flex items-center gap-2">
                <h1 className="text-gray-900" style={{ fontSize: '1.25rem', fontWeight: 600 }}>
                  {ru ? 'Темы' : 'Topics'}
                </h1>
                <PageInfoButton copy={topicsInfoCopy(lang)} />
              </div>
              <p className="text-xs text-gray-500 mt-0.5">
                {allTopics.length} {ru ? 'тем отслеживается' : 'topics tracked'} &middot; {totalMentions.toLocaleString()} {ru ? 'всего упоминаний' : 'total mentions'}
              </p>
            </div>
          </div>

          {/* Search */}
          <div className="relative mb-3">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
            <input
              type="text"
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              placeholder={ru ? 'Поиск тем...' : 'Search topics...'}
              className="w-full pl-9 pr-4 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent bg-gray-50"
            />
          </div>

          {/* Category Tabs */}
          <div className="flex gap-1.5 flex-wrap">
            {categories.map((cat) => {
              return (
                <button
                  key={cat}
                  onClick={() => setSelectedCategory(cat)}
                  className={`px-2.5 py-1 rounded-full text-xs transition-colors ${
                    selectedCategory === cat
                      ? 'bg-slate-800 text-white'
                      : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                  }`}
                  style={{ fontWeight: selectedCategory === cat ? 500 : 400 }}
                >
                  {cat !== 'All' && (
                    <span className="inline-block w-2 h-2 rounded-full mr-1.5" style={{ backgroundColor: categoryColors[cat] }} />
                  )}
                  {translateTopicsPageGroup(cat, ru)}
                </button>
              );
            })}
          </div>

          {/* Sort */}
          <div className="flex items-center gap-2 mt-3">
            <span className="text-xs text-gray-400">{ru ? 'Сортировка:' : 'Sort by:'}</span>
            <button
              onClick={() => setSortBy('mentions')}
              className={`text-xs px-2 py-0.5 rounded ${sortBy === 'mentions' ? 'bg-blue-50 text-blue-700' : 'text-gray-500 hover:text-gray-700'}`}
              style={{ fontWeight: sortBy === 'mentions' ? 500 : 400 }}
            >
              {ru ? 'По обсуждаемости' : 'Most discussed'}
            </button>
            <button
              onClick={() => setSortBy('growth')}
              className={`text-xs px-2 py-0.5 rounded ${sortBy === 'growth' ? 'bg-blue-50 text-blue-700' : 'text-gray-500 hover:text-gray-700'}`}
              style={{ fontWeight: sortBy === 'growth' ? 500 : 400 }}
            >
              {ru ? 'По росту' : 'Fastest growing'}
            </button>
          </div>
        </div>

        {/* Topic List */}
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
                onClick={refreshTopics}
                className="text-xs text-red-700 hover:text-red-800 underline"
                style={{ fontWeight: 600 }}
              >
                {ru ? 'Повторить' : 'Retry'}
              </button>
            </div>
          )}
          {topicsLoading && allTopics.length === 0 && (
            <div className="flex flex-col items-center justify-center py-12 text-gray-400">
              <Hash className="w-8 h-8 mb-2" />
              <p className="text-sm">{ru ? 'Загружаем темы...' : 'Loading topics...'}</p>
            </div>
          )}
          {filtered.map((topic) => (
            <button
              key={topic.id}
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

      {/* RIGHT: Topic Detail + Evidence */}
      {selectedTopic ? (
        <div className="flex-1 flex flex-col overflow-hidden bg-gray-50">
          {/* Mobile back bar */}
          <div className="md:hidden flex items-center gap-2 px-4 py-3 bg-white border-b border-gray-200 flex-shrink-0">
            <button
              onClick={clearTopicSelection}
              className="flex items-center gap-1.5 text-sm text-blue-600 active:opacity-70 transition-opacity"
              style={{ fontWeight: 500 }}
            >
              <ChevronLeft className="w-4 h-4" />
              {ru ? 'Все темы' : 'All Topics'}
            </button>
          </div>
          {/* Topic Header */}
          <div className="bg-white border-b border-gray-200 px-6 py-5">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-3">
                <div className="w-10 h-10 rounded-xl flex items-center justify-center" style={{ backgroundColor: selectedTopic.color + '15' }}>
                  <Hash className="w-5 h-5" style={{ color: selectedTopic.color }} />
                </div>
                <div>
                  <h2 className="text-gray-900" style={{ fontSize: '1.1rem', fontWeight: 600 }}>{ru ? selectedTopic.nameRu : selectedTopic.name}</h2>
                  <p className="text-xs text-gray-500">{activeTopic ? (ru ? activeTopic.descriptionRu : activeTopic.description) : ''}</p>
                </div>
              </div>
              <button onClick={clearTopicSelection} className="p-2 hover:bg-gray-100 rounded-lg transition-colors">
                <X className="w-5 h-5 text-gray-400" />
              </button>
            </div>

            {/* Stats Row */}
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

            {/* Trend Chart */}
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
                  <Area type="monotone" dataKey="count" stroke={selectedTopic.color} fill={selectedTopic.color + '20'} strokeWidth={2} />
                </AreaChart>
              </ResponsiveContainer>
            </div>

            {/* Top Channels */}
            <div className="flex items-center gap-2 mt-3">
              <span className="text-xs text-gray-400">{ru ? 'Ведущие каналы:' : 'Top channels:'}</span>
              {(activeTopic?.topChannels || []).map((ch) => (
                <span key={ch} className="text-xs px-2 py-0.5 bg-gray-100 text-gray-600 rounded-full">{ch}</span>
              ))}
            </div>
          </div>

          {/* Evidence Feed */}
          <div className="flex-1 overflow-y-auto px-6 py-4">
            {topicDetailError && (
              <div className="mb-3 px-4 py-3 border border-red-100 bg-red-50 rounded-xl flex items-center justify-between gap-3">
                <span className="text-xs text-red-700 truncate">
                  {ru ? 'Не удалось загрузить детали темы. Показаны краткие данные.' : 'Unable to load full topic details. Showing summary data.'}
                </span>
                <button
                  onClick={refreshTopicDetail}
                  className="text-xs text-red-700 hover:text-red-800 underline"
                  style={{ fontWeight: 600 }}
                >
                  {ru ? 'Повторить' : 'Retry'}
                </button>
              </div>
            )}
            {topicDetailLoading && !selectedTopicDetail && (
              <div className="mb-3 px-4 py-3 border border-blue-100 bg-blue-50 rounded-xl text-xs text-blue-700">
                {ru ? 'Загружаем доказательства и динамику темы...' : 'Loading topic evidence and trend details...'}
              </div>
            )}
            <div className="flex items-center gap-2 mb-3">
              <button
                onClick={() => selectTopic(selectedTopic, 'evidence')}
                className={`text-xs px-2.5 py-1 rounded-full ${proofView === 'evidence' ? 'bg-blue-50 text-blue-700' : 'bg-gray-100 text-gray-600 hover:bg-gray-200'}`}
                style={{ fontWeight: proofView === 'evidence' ? 600 : 500 }}
              >
                {ru ? 'Все доказательства' : 'All evidence'}
              </button>
              <button
                onClick={() => selectTopic(selectedTopic, 'questions')}
                className={`text-xs px-2.5 py-1 rounded-full ${proofView === 'questions' ? 'bg-amber-50 text-amber-700' : 'bg-gray-100 text-gray-600 hover:bg-gray-200'}`}
                style={{ fontWeight: proofView === 'questions' ? 600 : 500 }}
              >
                {ru ? 'Вопросы (доказательства)' : 'Questions proof'}
              </button>
            </div>

            {(() => {
              const visibleEvidence = evidenceFeed.items;
              const totalEvidence = evidenceFeed.total;

              return (
                <>
            <div className="flex items-center justify-between mb-4">
              <h3 className="text-sm text-gray-900" style={{ fontWeight: 600 }}>
                {proofView === 'questions'
                  ? (ru ? `Вопросы по теме (${totalEvidence})` : `Questions for this topic (${totalEvidence})`)
                  : (ru ? `Доказательства (${totalEvidence} публикаций и комментариев)` : `Evidence (${totalEvidence} posts & comments)`)}
              </h3>
              <div className="flex items-center gap-3">
                <span className="text-xs text-gray-400">
                  {proofView === 'questions'
                    ? (ru ? 'Показаны реальные вопросы из сообщений' : 'Showing real question-style messages')
                    : (ru ? 'Сообщения по данной теме' : 'Messages mentioning this topic')}
                </span>
                {evidenceError && (
                  <button
                    onClick={refreshEvidence}
                    className="text-xs text-red-700 hover:text-red-800 underline"
                    style={{ fontWeight: 600 }}
                  >
                    {ru ? 'Повторить' : 'Retry'}
                  </button>
                )}
              </div>
            </div>

            {evidenceLoading && visibleEvidence.length === 0 ? (
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
                    : (ru ? 'Подключите Neo4j для загрузки реальных сообщений' : 'Connect to Neo4j to load real messages')}
                </p>
              </div>
            ) : (
              <div className="space-y-3">
                {visibleEvidence.map((ev) => (
                  <div
                    id={`${proofView}-evidence-${ev.id}`}
                    key={ev.id}
                    className={`bg-white rounded-xl border p-4 hover:shadow-sm transition-shadow ${
                      highlightEvidenceId === ev.id
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
                          <span className="text-xs text-gray-900" style={{ fontWeight: 600 }}>{ev.author}</span>
                          <span className="text-xs text-gray-400 mx-1.5">{ru ? 'в' : 'in'}</span>
                          <span className="text-xs text-blue-600" style={{ fontWeight: 500 }}>{ev.channel}</span>
                        </div>
                      </div>
                      <div className="flex items-center gap-1.5 text-xs text-gray-400">
                        <Clock className="w-3 h-3" />
                        {ev.timestamp}
                      </div>
                    </div>
                    <p className="text-sm text-gray-700 leading-relaxed mb-3">{ev.text}</p>
                    <div className="flex items-center gap-4">
                      <div className="flex items-center gap-1 text-xs text-gray-400">
                        <ThumbsUp className="w-3 h-3" />
                        {ev.reactions}
                      </div>
                      <div className="flex items-center gap-1 text-xs text-gray-400">
                        <MessageCircle className="w-3 h-3" />
                        {ev.replies} {ru ? 'ответов' : 'replies'}
                      </div>
                      <span className={`text-xs px-1.5 py-0.5 rounded ${ev.type === 'message' ? 'bg-blue-50 text-blue-600' : 'bg-gray-100 text-gray-500'}`}>
                        {ev.type === 'message' ? (ru ? 'публикация' : 'post') : (ru ? 'комментарий' : 'comment')}
                      </span>
                    </div>
                  </div>
                ))}
                {evidenceFeed.hasMore && (
                  <div className="pt-2">
                    <button
                      onClick={loadMoreEvidence}
                      disabled={evidenceLoadingMore}
                      className="w-full rounded-xl border border-gray-200 bg-white px-4 py-2.5 text-sm text-gray-700 hover:bg-gray-50 disabled:cursor-not-allowed disabled:opacity-60"
                      style={{ fontWeight: 600 }}
                    >
                      {evidenceLoadingMore
                        ? (ru ? 'Загружаем ещё...' : 'Loading more...')
                        : (ru ? 'Показать ещё' : 'Load more')}
                    </button>
                  </div>
                )}
              </div>
            )}
                </>
              );
            })()}
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
