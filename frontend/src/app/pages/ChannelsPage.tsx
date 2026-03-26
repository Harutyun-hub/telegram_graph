import { useEffect, useState } from 'react';
import { Search, TrendingUp, TrendingDown, MessageCircle, Users, Radio, ChevronRight, ChevronLeft, X, Clock, User, Hash, ThumbsUp, Zap } from 'lucide-react';
import { ResponsiveContainer, AreaChart, Area, BarChart, Bar, XAxis, YAxis, Tooltip, CartesianGrid } from 'recharts';
import { useLanguage } from '../contexts/LanguageContext';
import { useChannelDetail, useChannelPostsFeed, useChannelsDetailData } from '../services/detailData';
import type { ChannelDetail } from '../types/data';
import { PageInfoButton, type PageInfoCopy } from '../components/ui/PageInfoButton';

const typeColors: Record<string, string> = {
  General: '#3b82f6',
  Work: '#f59e0b',
  Family: '#ec4899',
  Housing: '#ef4444',
  Business: '#10b981',
  Lifestyle: '#8b5cf6',
  Legal: '#6b7280',
};

const typeFiltersEN = ['All', 'General', 'Work', 'Family', 'Housing', 'Business', 'Lifestyle', 'Legal'];
const typeFiltersRU = ['Все', 'Общий', 'Работа', 'Семья', 'Жильё', 'Бизнес', 'Досуг', 'Документы'];
const typeMapRev: Record<string, string> = {
  Все: 'All', Общий: 'General', Работа: 'Work', Семья: 'Family',
  Жильё: 'Housing', Бизнес: 'Business', Досуг: 'Lifestyle', Документы: 'Legal',
};
const typeMapFwd: Record<string, string> = {
  General: 'Общий', Work: 'Работа', Family: 'Семья',
  Housing: 'Жильё', Business: 'Бизнес', Lifestyle: 'Досуг', Legal: 'Документы',
};

const messageTypeMapRU: Record<string, string> = {
  Discussion: 'Обсуждение', Question: 'Вопрос', Recommendation: 'Рекомендация',
  Complaint: 'Жалоба', 'Info Sharing': 'Инфо', 'Photo/Video': 'Фото/Видео',
};

function channelsInfoCopy(lang: 'en' | 'ru'): PageInfoCopy {
  return lang === 'ru'
    ? {
      summary: 'Объясняет, как эта страница собирает каналы, считает метрики и ранжирует их.',
      title: 'Как формируются каналы',
      overview: 'Страница каналов собирает Telegram-группы из выбранного диапазона дат и превращает их в карточки с ключевыми сигналами активности, масштаба и вовлечённости.',
      sectionTitle: 'Что лежит в основе',
      items: [
        'Каждая строка канала строится из сводки по каналу за выбранное окно дат.',
        'Карточка объединяет число участников, средний дневной объём сообщений, 7-дневный рост и оценку вовлечённости.',
        'В деталях канала показываются ключевые темы, распределение типов сообщений, ведущие участники и недавние публикации.',
        'Сортировка переключает приоритет между вовлечённостью, размером канала и ростом, а не меняет сами исходные данные.',
      ],
      noteTitle: 'Как читать страницу',
      note: 'Вовлечённость здесь важнее одного только размера канала. Небольшая группа может быть выше в списке, если она активнее и быстрее растёт.',
      ariaLabel: 'Объяснить, как формируется страница каналов',
      badgeLabel: 'О странице',
    }
    : {
      summary: 'Explains how this page assembles channels, scores their metrics, and ranks them.',
      title: 'How Channels Are Built',
      overview: 'The Channels page turns Telegram group summaries from the selected date range into cards that show the main signals of scale, activity, and engagement.',
      sectionTitle: 'What it uses',
      items: [
        'Each channel row is built from a backend channel summary for the selected date window.',
        'The card combines member count, estimated daily message volume, 7-day growth, and engagement score.',
        'Channel detail expands into top topics, message-type mix, leading voices, and recent posts.',
        'Sorting changes the ranking priority between engagement, size, and growth without changing the underlying data.',
      ],
      noteTitle: 'How to read it',
      note: 'Engagement matters more than size alone on this page. A smaller group can rank above a bigger one if it is more active and growing faster.',
      ariaLabel: 'Explain how the channels page is built',
      badgeLabel: 'Page guide',
    };
}

// ── COMPONENT ──

export function ChannelsPage() {
  const { lang } = useLanguage();
  const ru = lang === 'ru';
  const {
    data: allChannels,
    loading: channelsLoading,
    error: channelsError,
    refresh: refreshChannels,
  } = useChannelsDetailData();
  const [searchQuery, setSearchQuery] = useState('');
  const [selectedType, setSelectedType] = useState('All');
  const [selectedChannel, setSelectedChannel] = useState<ChannelDetail | null>(null);
  const {
    data: selectedChannelDetail,
    loading: channelDetailLoading,
    error: channelDetailError,
    refresh: refreshChannelDetail,
  } = useChannelDetail(selectedChannel?.id || null);
  const [sortBy, setSortBy] = useState<'engagement' | 'members' | 'growth'>('engagement');
  const [activeTab, setActiveTab] = useState<'overview' | 'topics' | 'posts'>('overview');

  const typeFilters = ru ? typeFiltersRU : typeFiltersEN;
  const totalMembers = allChannels.reduce((s, c) => s + c.members, 0);

  const filtered = allChannels
    .filter((ch) => {
      const typeFilter = ru ? (typeMapRev[selectedType] || selectedType) : selectedType;
      if (typeFilter !== 'All' && ch.type !== typeFilter) return false;
      if (searchQuery && !ch.name.toLowerCase().includes(searchQuery.toLowerCase())) return false;
      return true;
    })
    .sort((a, b) => {
      if (sortBy === 'engagement') return b.engagement - a.engagement;
      if (sortBy === 'members') return b.members - a.members;
      return b.growth - a.growth;
    });

  const tabLabels = ru
    ? [['overview', 'Обзор'], ['topics', 'Темы'], ['posts', 'Публикации']] as const
    : [['overview', 'Overview'], ['topics', 'Topics'], ['posts', 'Recent Posts']] as const;

  const sortLabels = ru
    ? [['engagement', 'Вовлечённость'], ['members', 'Участники'], ['growth', 'Рост']] as const
    : [['engagement', 'Engagement'], ['members', 'Members'], ['growth', 'Growth']] as const;

  useEffect(() => {
    if (!selectedChannel) return;
    const fresh = allChannels.find((channel) => channel.id === selectedChannel.id);
    if (!fresh) {
      setSelectedChannel(null);
      return;
    }
    if (fresh !== selectedChannel) {
      setSelectedChannel(fresh);
    }
  }, [allChannels, selectedChannel]);
  const activeChannel = selectedChannelDetail || selectedChannel;
  const {
    data: channelPostsFeed,
    loading: channelPostsLoading,
    loadingMore: channelPostsLoadingMore,
    error: channelPostsError,
    refresh: refreshChannelPosts,
    loadMore: loadMoreChannelPosts,
  } = useChannelPostsFeed(selectedChannel?.id || null, Boolean(selectedChannel && activeTab === 'posts'));

  return (
    <div className="flex flex-col md:flex-row h-full">
      {/* LEFT: Channel List — hidden on mobile when a channel is selected */}
      <div className={`${selectedChannel ? 'hidden md:flex md:w-[380px]' : 'flex flex-1'} flex-col border-r border-gray-200 bg-white transition-all`}>
        {/* Header */}
        <div className="px-6 py-5 border-b border-gray-200">
          <div className="flex items-center justify-between mb-3">
            <div>
              <div className="flex items-center gap-2">
                <h1 className="text-gray-900" style={{ fontSize: '1.25rem', fontWeight: 600 }}>
                  {ru ? 'Каналы' : 'Channels'}
                </h1>
                <PageInfoButton copy={channelsInfoCopy(lang)} />
              </div>
              <p className="text-xs text-gray-500 mt-0.5">
                {allChannels.length} {ru ? 'Telegram-групп отслеживается' : 'Telegram groups tracked'} &middot; {(totalMembers / 1000).toFixed(1)}K {ru ? 'участников всего' : 'total members'}
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
              placeholder={ru ? 'Поиск каналов...' : 'Search channels...'}
              className="w-full pl-9 pr-4 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent bg-gray-50"
            />
          </div>

          {/* Type Tabs */}
          <div className="flex gap-1.5 flex-wrap">
            {typeFilters.map((t) => {
              const engType = ru ? (typeMapRev[t] || t) : t;
              return (
                <button
                  key={t}
                  onClick={() => setSelectedType(t)}
                  className={`px-2.5 py-1 rounded-full text-xs transition-colors ${
                    selectedType === t
                      ? 'bg-slate-800 text-white'
                      : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                  }`}
                  style={{ fontWeight: selectedType === t ? 500 : 400 }}
                >
                  {t !== (ru ? 'Все' : 'All') && (
                    <span className="inline-block w-2 h-2 rounded-full mr-1.5" style={{ backgroundColor: typeColors[engType] }} />
                  )}
                  {t}
                </button>
              );
            })}
          </div>

          {/* Sort */}
          <div className="flex items-center gap-2 mt-3">
            <span className="text-xs text-gray-400">{ru ? 'Сортировка:' : 'Sort by:'}</span>
            {sortLabels.map(([key, label]) => (
              <button
                key={key}
                onClick={() => setSortBy(key)}
                className={`text-xs px-2 py-0.5 rounded ${sortBy === key ? 'bg-blue-50 text-blue-700' : 'text-gray-500 hover:text-gray-700'}`}
                style={{ fontWeight: sortBy === key ? 500 : 400 }}
              >
                {label}
              </button>
            ))}
          </div>
        </div>

        {/* Channel List */}
        <div className="flex-1 overflow-y-auto">
          {channelsError && (
            <div className="px-6 py-3 border-b border-red-100 bg-red-50 flex items-center justify-between gap-2">
              <span className="text-xs text-red-700 truncate">
                {ru ? 'Не удалось обновить каналы. Показаны последние сохранённые данные.' : 'Unable to refresh channels. Showing last saved data.'}
              </span>
              <button
                onClick={refreshChannels}
                className="text-xs text-red-700 hover:text-red-800 underline"
                style={{ fontWeight: 600 }}
              >
                {ru ? 'Повторить' : 'Retry'}
              </button>
            </div>
          )}
          {channelsLoading && allChannels.length === 0 && (
            <div className="flex flex-col items-center justify-center py-12 text-gray-400">
              <Radio className="w-8 h-8 mb-2" />
              <p className="text-sm">{ru ? 'Загружаем каналы...' : 'Loading channels...'}</p>
            </div>
          )}
          {filtered.map((ch) => (
            <button
              key={ch.id}
              onClick={() => { setSelectedChannel(ch); setActiveTab('overview'); }}
              className={`w-full text-left px-6 py-4 border-b border-gray-100 hover:bg-gray-50 transition-colors ${
                selectedChannel?.id === ch.id ? 'bg-blue-50 border-l-3 border-l-blue-500' : ''
              }`}
            >
              <div className="flex items-center gap-3">
                <div className="w-10 h-10 rounded-full flex items-center justify-center flex-shrink-0"
                  style={{ backgroundColor: (typeColors[ch.type] || '#6b7280') + '15' }}>
                  <Radio className="w-4 h-4" style={{ color: typeColors[ch.type] || '#6b7280' }} />
                </div>
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2">
                    <span className="text-sm text-gray-900 truncate" style={{ fontWeight: 500 }}>{ch.name}</span>
                    <span className="text-xs px-1.5 py-0.5 rounded flex-shrink-0"
                      style={{ backgroundColor: (typeColors[ch.type] || '#6b7280') + '15', color: typeColors[ch.type] || '#6b7280', fontWeight: 500 }}>
                      {ru ? (typeMapFwd[ch.type] || ch.type) : ch.type}
                    </span>
                  </div>
                  <div className="flex items-center gap-3 mt-1 text-xs text-gray-400">
                    <span className="flex items-center gap-1"><Users className="w-3 h-3" />{(ch.members / 1000).toFixed(1)}K</span>
                    <span className="flex items-center gap-1"><MessageCircle className="w-3 h-3" />{ch.dailyMessages}/{ru ? 'день' : 'day'}</span>
                    <span className={`flex items-center gap-0.5 ${ch.growth >= 0 ? 'text-emerald-500' : 'text-red-500'}`} style={{ fontWeight: 600 }}>
                      {ch.growth >= 0 ? <TrendingUp className="w-3 h-3" /> : <TrendingDown className="w-3 h-3" />}{ch.growth > 0 ? '+' : ''}{ch.growth}
                    </span>
                  </div>
                </div>
                <div className="text-right flex-shrink-0">
                  <div className="text-sm text-gray-900" style={{ fontWeight: 600 }}>{ch.engagement}%</div>
                  <div className="text-xs text-gray-400">{ru ? 'вовлеч.' : 'engage'}</div>
                </div>
              </div>
            </button>
          ))}
          {!channelsLoading && filtered.length === 0 && (
            <div className="flex flex-col items-center justify-center py-12 text-gray-400">
              <Radio className="w-8 h-8 mb-2" />
              <p className="text-sm">{ru ? 'Каналы не найдены' : 'No channels match your filters'}</p>
            </div>
          )}
        </div>
      </div>

      {/* RIGHT: Channel Detail */}
      {selectedChannel ? (
        <div className="flex-1 flex flex-col overflow-hidden bg-gray-50">
          {/* Mobile back bar */}
          <div className="md:hidden flex items-center gap-2 px-4 py-3 bg-white border-b border-gray-200 flex-shrink-0">
            <button
              onClick={() => setSelectedChannel(null)}
              className="flex items-center gap-1.5 text-sm text-blue-600 active:opacity-70 transition-opacity"
              style={{ fontWeight: 500 }}
            >
              <ChevronLeft className="w-4 h-4" />
              {ru ? 'Все каналы' : 'All Channels'}
            </button>
          </div>
          {/* Channel Header */}
          <div className="bg-white border-b border-gray-200 px-6 py-5">
            <div className="flex items-center justify-between mb-3">
              <div className="flex items-center gap-3">
                <div className="w-12 h-12 rounded-xl flex items-center justify-center"
                  style={{ backgroundColor: (typeColors[selectedChannel.type] || '#6b7280') + '15' }}>
                  <Radio className="w-6 h-6" style={{ color: typeColors[selectedChannel.type] || '#6b7280' }} />
                </div>
                <div>
                  <h2 className="text-gray-900" style={{ fontSize: '1.1rem', fontWeight: 600 }}>{selectedChannel.name}</h2>
                  <p className="text-xs text-gray-500">{activeChannel?.description || ''}</p>
                </div>
              </div>
              <button onClick={() => setSelectedChannel(null)} className="p-2 hover:bg-gray-100 rounded-lg transition-colors">
                <X className="w-5 h-5 text-gray-400" />
              </button>
            </div>

            {/* Stats Row */}
            <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
              {[
                { label: ru ? 'Участников' : 'Members', value: `${((activeChannel?.members || 0) / 1000).toFixed(1)}K`, icon: Users },
                { label: ru ? 'Сообщений/день' : 'Daily Msgs', value: String(activeChannel?.dailyMessages || 0), icon: MessageCircle },
                { label: ru ? 'Вовлечённость' : 'Engagement', value: `${activeChannel?.engagement || 0}%`, icon: Zap },
                { label: ru ? 'Рост' : 'Growth', value: `${(activeChannel?.growth || 0) > 0 ? '+' : ''}${activeChannel?.growth || 0}`, icon: TrendingUp },
                { label: ru ? 'Горячая тема' : 'Hot Topic', value: activeChannel?.topTopic || '', icon: Hash },
              ].map((stat) => (
                <div key={stat.label} className="bg-gray-50 rounded-lg px-3 py-2.5">
                  <div className="flex items-center gap-1.5 mb-0.5">
                    <stat.icon className="w-3 h-3 text-gray-400" />
                    <p className="text-xs text-gray-500">{stat.label}</p>
                  </div>
                  <p className="text-sm text-gray-900 truncate" style={{ fontWeight: 600 }}>{stat.value}</p>
                </div>
              ))}
            </div>

            {/* Tabs */}
            <div className="flex gap-1 mt-4 border-b border-gray-100 -mx-6 px-6">
              {tabLabels.map(([key, label]) => (
                <button
                  key={key}
                  onClick={() => setActiveTab(key)}
                  className={`px-4 py-2 text-xs border-b-2 transition-colors ${
                    activeTab === key
                      ? 'border-blue-500 text-blue-700'
                      : 'border-transparent text-gray-500 hover:text-gray-700'
                  }`}
                  style={{ fontWeight: activeTab === key ? 600 : 400 }}
                >
                  {label}
                </button>
              ))}
            </div>
          </div>

          {/* Tab Content */}
          <div className="flex-1 overflow-y-auto px-6 py-4">
            {channelDetailError && (
              <div className="mb-4 px-4 py-3 border border-red-100 bg-red-50 rounded-xl flex items-center justify-between gap-3">
                <span className="text-xs text-red-700 truncate">
                  {ru ? 'Не удалось загрузить детали канала. Показаны краткие данные.' : 'Unable to load full channel details. Showing summary data.'}
                </span>
                <button
                  onClick={refreshChannelDetail}
                  className="text-xs text-red-700 hover:text-red-800 underline"
                  style={{ fontWeight: 600 }}
                >
                  {ru ? 'Повторить' : 'Retry'}
                </button>
              </div>
            )}
            {channelDetailLoading && !selectedChannelDetail && (
              <div className="mb-4 px-4 py-3 border border-blue-100 bg-blue-50 rounded-xl text-xs text-blue-700">
                {ru ? 'Загружаем детальную активность канала...' : 'Loading channel activity details...'}
              </div>
            )}
            {activeTab === 'overview' && (
              <div className="space-y-4">
                {/* Activity Chart */}
                <div className="bg-white rounded-xl border border-gray-200 p-4">
                  <h4 className="text-sm text-gray-900 mb-3" style={{ fontWeight: 600 }}>
                    {ru ? 'Активность за неделю' : 'Weekly Activity'}
                  </h4>
                  <ResponsiveContainer width="100%" height={160}>
                    <BarChart data={activeChannel?.weeklyData || []}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" />
                      <XAxis dataKey="day" tick={{ fontSize: 11 }} stroke="#9ca3af" />
                      <YAxis tick={{ fontSize: 11 }} stroke="#9ca3af" />
                      <Tooltip />
                      <Bar dataKey="msgs" fill={typeColors[selectedChannel.type] || '#3b82f6'} radius={[4, 4, 0, 0]} />
                    </BarChart>
                  </ResponsiveContainer>
                </div>

                {/* Hourly Pattern */}
                <div className="bg-white rounded-xl border border-gray-200 p-4">
                  <h4 className="text-sm text-gray-900 mb-3" style={{ fontWeight: 600 }}>
                    {ru ? 'Почасовая активность' : 'Hourly Pattern'}
                  </h4>
                  <ResponsiveContainer width="100%" height={120}>
                    <AreaChart data={activeChannel?.hourlyData || []}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" />
                      <XAxis dataKey="hour" tick={{ fontSize: 10 }} stroke="#9ca3af" />
                      <YAxis tick={{ fontSize: 10 }} stroke="#9ca3af" hide />
                      <Tooltip />
                      <Area type="monotone" dataKey="msgs" stroke={typeColors[selectedChannel.type] || '#3b82f6'} fill={(typeColors[selectedChannel.type] || '#3b82f6') + '20'} strokeWidth={2} />
                    </AreaChart>
                  </ResponsiveContainer>
                </div>

                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  {/* Sentiment */}
                  <div className="bg-white rounded-xl border border-gray-200 p-4">
                    <h4 className="text-sm text-gray-900 mb-3" style={{ fontWeight: 600 }}>
                      {ru ? 'Тональность' : 'Sentiment'}
                    </h4>
                    <div className="flex items-center gap-3 mb-3">
                      <div className="flex h-3 flex-1 rounded-full overflow-hidden">
                        <div className="bg-emerald-400" style={{ width: `${activeChannel?.sentimentBreakdown.positive || 0}%` }} />
                        <div className="bg-gray-300" style={{ width: `${activeChannel?.sentimentBreakdown.neutral || 0}%` }} />
                        <div className="bg-red-400" style={{ width: `${activeChannel?.sentimentBreakdown.negative || 0}%` }} />
                      </div>
                    </div>
                    <div className="flex items-center justify-between text-xs">
                      <span className="text-emerald-600">{activeChannel?.sentimentBreakdown.positive || 0}% {ru ? 'позит.' : 'positive'}</span>
                      <span className="text-gray-400">{activeChannel?.sentimentBreakdown.neutral || 0}% {ru ? 'нейтр.' : 'neutral'}</span>
                      <span className="text-red-500">{activeChannel?.sentimentBreakdown.negative || 0}% {ru ? 'негат.' : 'negative'}</span>
                    </div>
                  </div>

                  {/* Message Types */}
                  <div className="bg-white rounded-xl border border-gray-200 p-4">
                    <h4 className="text-sm text-gray-900 mb-3" style={{ fontWeight: 600 }}>
                      {ru ? 'Типы сообщений' : 'Message Types'}
                    </h4>
                    <div className="space-y-1.5">
                      {(activeChannel?.messageTypes || []).slice(0, 4).map((mt) => (
                        <div key={mt.type} className="flex items-center gap-2">
                          <span className="text-xs text-gray-600 w-28 truncate">
                            {ru ? (messageTypeMapRU[mt.type] || mt.type) : mt.type}
                          </span>
                          <div className="flex-1 h-2 bg-gray-100 rounded-full overflow-hidden">
                            <div className="h-full bg-blue-400 rounded-full" style={{ width: `${mt.pct}%` }} />
                          </div>
                          <span className="text-xs text-gray-400 w-8 text-right">{mt.pct}%</span>
                        </div>
                      ))}
                    </div>
                  </div>
                </div>

                {/* Top Voices */}
                {(activeChannel?.topVoices || []).length > 0 && (
                  <div className="bg-white rounded-xl border border-gray-200 p-4">
                    <h4 className="text-sm text-gray-900 mb-3" style={{ fontWeight: 600 }}>
                      {ru ? 'Ведущие участники' : 'Top Contributors'}
                    </h4>
                    <div className="space-y-2">
                      {(activeChannel?.topVoices || []).map((v, i) => (
                        <div key={v.name} className="flex items-center gap-3 py-1.5">
                          <div className="w-7 h-7 rounded-full bg-slate-100 flex items-center justify-center text-xs text-slate-600" style={{ fontWeight: 600 }}>
                            {i + 1}
                          </div>
                          <div className="flex-1">
                            <span className="text-xs text-gray-900" style={{ fontWeight: 500 }}>{v.name}</span>
                            <div className="text-xs text-gray-400">
                              {v.posts} {ru ? 'публикаций' : 'posts'} &middot; {ru ? 'Рейтинг помощи:' : 'Help score:'} {v.helpScore}
                            </div>
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            )}

            {activeTab === 'topics' && (
              <div className="space-y-2">
                <p className="text-xs text-gray-500 mb-3">
                  {ru ? 'Что обсуждают в этом канале:' : 'What people discuss in this channel:'}
                </p>
                {(activeChannel?.topTopics || []).map((t) => (
                  <div key={t.name} className="bg-white rounded-lg border border-gray-200 p-3">
                    <div className="flex items-center justify-between mb-1.5">
                      <span className="text-xs text-gray-900" style={{ fontWeight: 500 }}>{t.name}</span>
                      <span className="text-xs text-gray-500">{t.pct}%</span>
                    </div>
                    <div className="w-full bg-gray-100 rounded-full h-2">
                      <div className="h-2 bg-blue-400 rounded-full" style={{ width: `${t.pct}%` }} />
                    </div>
                    <span className="text-xs text-gray-400 mt-1 block">{t.mentions.toLocaleString()} {ru ? 'упоминаний' : 'mentions'}</span>
                  </div>
                ))}
              </div>
            )}

            {activeTab === 'posts' && (
              <div className="space-y-3">
                <div className="flex items-center justify-between">
                  <h4 className="text-sm text-gray-900" style={{ fontWeight: 600 }}>
                    {ru ? `Публикации за период (${channelPostsFeed.total})` : `Posts in range (${channelPostsFeed.total})`}
                  </h4>
                  {channelPostsError && (
                    <button
                      onClick={refreshChannelPosts}
                      className="text-xs text-red-700 hover:text-red-800 underline"
                      style={{ fontWeight: 600 }}
                    >
                      {ru ? 'Повторить' : 'Retry'}
                    </button>
                  )}
                </div>
                {channelPostsLoading && channelPostsFeed.items.length === 0 ? (
                  <div className="mb-2 px-4 py-3 border border-blue-100 bg-blue-50 rounded-xl text-xs text-blue-700">
                    {ru ? 'Загружаем публикации канала...' : 'Loading channel posts...'}
                  </div>
                ) : channelPostsFeed.items.length === 0 ? (
                  <div className="flex flex-col items-center justify-center py-16 text-gray-400">
                    <MessageCircle className="w-8 h-8 mb-2" />
                    <p className="text-sm">{ru ? 'Публикации за выбранный период не найдены' : 'No posts found in the selected range'}</p>
                    <p className="text-xs mt-1">{ru ? 'Попробуйте расширить период или выбрать другой канал' : 'Try expanding the date range or choosing another channel'}</p>
                  </div>
                ) : (
                  <>
                  {channelPostsFeed.items.map((post) => (
                    <div key={post.id} className="bg-white rounded-xl border border-gray-200 p-4">
                      <div className="flex items-center gap-2 mb-2">
                        <div className="w-6 h-6 rounded-full bg-slate-100 flex items-center justify-center">
                          <User className="w-3 h-3 text-slate-500" />
                        </div>
                        <span className="text-xs text-gray-900" style={{ fontWeight: 600 }}>{post.author}</span>
                        <span className="text-xs text-gray-400 ml-auto">{post.timestamp}</span>
                      </div>
                      <p className="text-sm text-gray-700 leading-relaxed mb-3">{post.text}</p>
                      <div className="flex items-center gap-4 text-xs text-gray-400">
                        <span className="flex items-center gap-1"><ThumbsUp className="w-3 h-3" />{post.reactions}</span>
                        <span className="flex items-center gap-1"><MessageCircle className="w-3 h-3" />{post.replies} {ru ? 'ответов' : 'replies'}</span>
                      </div>
                    </div>
                  ))}
                  {channelPostsFeed.hasMore && (
                    <button
                      onClick={loadMoreChannelPosts}
                      disabled={channelPostsLoadingMore}
                      className="w-full rounded-xl border border-gray-200 bg-white px-4 py-2.5 text-sm text-gray-700 hover:bg-gray-50 disabled:cursor-not-allowed disabled:opacity-60"
                      style={{ fontWeight: 600 }}
                    >
                      {channelPostsLoadingMore
                        ? (ru ? 'Загружаем ещё...' : 'Loading more...')
                        : (ru ? 'Показать ещё' : 'Load more')}
                    </button>
                  )}
                  </>
                )}
              </div>
            )}
          </div>
        </div>
      ) : (
        <div className="hidden md:flex flex-1 flex-col items-center justify-center bg-gray-50 text-gray-400 px-4 md:px-8">
          <div className="max-w-md text-center">
            <Radio className="w-12 h-12 mx-auto mb-4 text-gray-300" />
            <h3 className="text-gray-600 mb-1" style={{ fontSize: '1rem', fontWeight: 500 }}>
              {ru ? 'Выберите канал для анализа' : 'Select a channel to analyze'}
            </h3>
            <p className="text-sm text-gray-400">
              {ru
                ? 'Нажмите на любой канал из списка, чтобы увидеть его активность, темы, тональность и ведущих участников.'
                : 'Click any channel from the list to see its activity patterns, top topics, sentiment and key contributors.'}
            </p>
          </div>
        </div>
      )}
    </div>
  );
}
