import { useState, useEffect } from 'react';
import { Link } from 'react-router';
import { TrendingUp, Clock, ChevronRight, MessageCircle, Heart, Zap, BarChart3 } from 'lucide-react';
import { useLanguage } from '../../contexts/LanguageContext';
import { useData } from '../../contexts/DataContext';
import { EmptyWidget } from '../ui/EmptyWidget';

// ============================================================
// W1: COMMUNITY HEALTH SCORE
// ============================================================

function getHealthColor(score: number) {
  if (score >= 70) return { ring: '#10b981', bg: 'bg-emerald-50', text: 'text-emerald-700', en: 'Constructive', ru: 'Конструктивно' };
  if (score >= 50) return { ring: '#3b82f6', bg: 'bg-blue-50', text: 'text-blue-700', en: 'Balanced', ru: 'Сбалансировано' };
  if (score >= 30) return { ring: '#f59e0b', bg: 'bg-amber-50', text: 'text-amber-700', en: 'Fragile', ru: 'Хрупкий баланс' };
  return { ring: '#ef4444', bg: 'bg-red-50', text: 'text-red-700', en: 'Tense', ru: 'Напряжённо' };
}

export function CommunityHealthScore() {
  const { lang } = useLanguage();
  const { data } = useData();
  const ru = lang === 'ru';
  const [animatedScore, setAnimatedScore] = useState(0);
  const healthData = data.communityHealth;
  const currentScore = healthData.currentScore;
  const health = getHealthColor(currentScore);
  const weekAgoScore = healthData.weekAgoScore;
  const delta = currentScore - weekAgoScore;
  const healthComponents = healthData.components[lang] ?? [];
  const healthHistory = healthData.history;

  // ✅ FIX: useEffect MUST be before any early return (Rules of Hooks)
  // ✅ FIX: currentScore added to dependency array so animation re-runs on data change
  useEffect(() => {
    const timer = setTimeout(() => setAnimatedScore(currentScore), 100);
    return () => clearTimeout(timer);
  }, [currentScore]);

  if (!healthComponents.length) return <EmptyWidget title={ru ? 'Климат сообщества' : 'Community Climate'} />;

  const circumference = 2 * Math.PI * 54;
  const strokeDashoffset = circumference - (animatedScore / 100) * circumference;

  return (
    <div className="bg-white rounded-xl border border-gray-200 p-6">
      <div className="flex items-center justify-between mb-4">
        <div>
          <h3 className="text-gray-900" style={{ fontSize: '1.05rem' }}>
            {ru ? 'Климат сообщества' : 'Community Climate'}
          </h3>
          <p className="text-xs text-gray-500 mt-0.5">
            {ru ? 'Объяснимый индекс по интенту, тону и разнообразию тем' : 'Explainable index from intent, tone, and topic diversity'}
          </p>
        </div>
        <span className={`text-xs px-2.5 py-1 rounded-full ${health.bg} ${health.text}`}>
          {ru ? health.ru : health.en}
        </span>
      </div>

      <div className="flex items-center gap-6">
        <div className="relative flex-shrink-0">
          <svg width="128" height="128" viewBox="0 0 128 128">
            <circle cx="64" cy="64" r="54" stroke="#f3f4f6" strokeWidth="8" fill="none" />
            <circle
              cx="64" cy="64" r="54"
              stroke={health.ring} strokeWidth="8" fill="none"
              strokeLinecap="round"
              strokeDasharray={circumference}
              strokeDashoffset={strokeDashoffset}
              transform="rotate(-90 64 64)"
              style={{ transition: 'stroke-dashoffset 1.5s ease-out' }}
            />
          </svg>
          <div className="absolute inset-0 flex flex-col items-center justify-center">
            <span className="text-3xl text-gray-900" style={{ fontWeight: 600 }}>{animatedScore}</span>
            <span className="text-xs text-gray-500">/100</span>
          </div>
        </div>

        <div className="flex-1 space-y-2.5">
          {healthComponents.map((comp) => (
            <div key={comp.label}>
              <div className="flex items-center justify-between mb-1">
                <span className="text-xs text-gray-600">{comp.label}</span>
                <div className="flex items-center gap-1.5">
                  <span className="text-xs text-gray-900" style={{ fontWeight: 500 }}>{comp.value}</span>
                  {/* ✅ FIX: conditional sign — comp.trend can be negative */}
                  <span className={`text-xs ${comp.trend >= 0 ? 'text-emerald-500' : 'text-red-500'}`}>
                    {comp.trend > 0 ? '+' : ''}{comp.trend}
                  </span>
                </div>
              </div>
              <div className="w-full bg-gray-100 rounded-full h-1.5">
                <div
                  className="h-1.5 rounded-full transition-all duration-1000"
                  style={{ width: `${comp.value}%`, backgroundColor: getHealthColor(comp.value).ring }}
                />
              </div>
            </div>
          ))}
        </div>
      </div>

      <div className="mt-4 pt-3 border-t border-gray-100">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-1.5">
            {/* ✅ FIX: conditional icon and color based on delta direction */}
            <TrendingUp className={`w-3.5 h-3.5 ${delta >= 0 ? 'text-emerald-500' : 'text-red-500'}`} />
              <span className={`text-xs ${delta >= 0 ? 'text-emerald-600' : 'text-red-600'}`}>
               {ru ? `${delta > 0 ? '+' : ''}${delta} пунктов к предыдущим 24ч` : `${delta > 0 ? '+' : ''}${delta} pts vs previous 24h`}
              </span>
            </div>
          <div className="flex items-center gap-1">
            {healthHistory.map((point, i) => (
              <div
                key={i}
                className="w-2 rounded-sm"
                style={{
                  height: `${Math.max(4, point.score * 0.4)}px`,
                  backgroundColor: getHealthColor(point.score).ring,
                  opacity: 0.4 + (i / healthHistory.length) * 0.6,
                }}
              />
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}


// ============================================================
// W2: TRENDING TOPICS FEED
// ============================================================

const categoryColors: Record<string, string> = {
  Housing: '#ef4444', Education: '#8b5cf6', Business: '#3b82f6',
  Language: '#10b981', Healthcare: '#f97316', Lifestyle: '#ec4899', Finance: '#f59e0b',
  'Government & Leadership': '#0ea5e9',
  'Opposition & Protest': '#ef4444',
  'Regional Security': '#f59e0b',
  'Nagorno-Karabakh & Artsakh': '#f97316',
  'Geopolitical Alignment': '#14b8a6',
  'National Identity': '#8b5cf6',
  'Media Landscape': '#3b82f6',
  'Information Integrity': '#06b6d4',
  'Community Life': '#22c55e',
  'Social Services': '#10b981',
  'Military & Defense': '#dc2626',
  'Democracy & Reform': '#6366f1',
  'Housing & Infrastructure': '#ef4444',
  'Financial System': '#f59e0b',
  'Macroeconomic Condition': '#f97316',
  'Religion': '#a855f7',
  'Государственное управление и лидерство': '#0ea5e9',
  'Оппозиция и протестная активность': '#ef4444',
  'Нагорный Карабах и Арцах': '#f97316',
  'Международные конфликты': '#7c3aed',
  'Национальная идентичность': '#8b5cf6',
  'Региональная безопасность': '#f59e0b',
  'Рынок труда': '#22c55e',
  'Демократия и реформы': '#6366f1',
  'Армия и оборона': '#dc2626',
  'Бизнес и предпринимательство': '#3b82f6',
  'Культура и развлечения': '#ec4899',
  'Макроэкономическая ситуация': '#f97316',
  'Эмиграция': '#0ea5e9',
  'Геополитическая ориентация': '#14b8a6',
  'Финансовая система': '#f59e0b',
  'Социальная поддержка': '#10b981',
  'Медийная среда': '#3b82f6',
  'Информационная достоверность': '#06b6d4',
  'Жизнь сообщества': '#22c55e',
  'Жилье и инфраструктура': '#ef4444',
  'Жильё': '#ef4444', 'Образование': '#8b5cf6', 'Бизнес': '#3b82f6',
  'Язык': '#10b981', 'Медицина': '#f97316', 'Досуг': '#ec4899', 'Финансы': '#f59e0b',
};

const sentimentEmoji: Record<string, string> = {
  frustrated: '😤', seeking: '🔍', curious: '🤔',
  motivated: '💪', concerned: '😟', excited: '🎉', confused: '😕',
};

export function TrendingTopicsFeed() {
  const { lang } = useLanguage();
  const { data } = useData();
  const ru = lang === 'ru';
  const [mode, setMode] = useState<'trending' | 'new'>('trending');
  const trendingTopics = data.trendingTopics[lang] ?? [];
  const trendingNewTopics = data.trendingNewTopics[lang] ?? [];

  useEffect(() => {
    if (mode === 'new' && trendingNewTopics.length === 0 && trendingTopics.length > 0) {
      setMode('trending');
    }
  }, [mode, trendingNewTopics.length, trendingTopics.length]);

  const visibleTopics = mode === 'new' ? trendingNewTopics : trendingTopics;

  if (!visibleTopics.length) return <EmptyWidget title={ru ? 'Тренды прямо сейчас' : 'Trending Now'} />;

  return (
    <div className="bg-white rounded-xl border border-gray-200 p-6">
      <div className="flex items-center justify-between mb-4">
        <div className="flex items-center gap-2 flex-wrap">
          <div className="w-2 h-2 bg-emerald-500 rounded-full animate-pulse" />
          <h3 className="text-gray-900" style={{ fontSize: '1.05rem' }}>
            {ru ? 'Тренды прямо сейчас' : 'Trending Now'}
          </h3>
          <div className="flex items-center gap-1 ml-1">
            <button
              onClick={() => setMode('trending')}
              className={`text-xs px-2 py-0.5 rounded-full ${mode === 'trending' ? 'bg-slate-800 text-white' : 'bg-gray-100 text-gray-600 hover:bg-gray-200'}`}
              style={{ fontWeight: 500 }}
            >
              {ru ? 'Текущие' : 'Trending'}
            </button>
            <button
              onClick={() => setMode('new')}
              disabled={trendingNewTopics.length === 0}
              className={`text-xs px-2 py-0.5 rounded-full ${mode === 'new' ? 'bg-teal-700 text-white' : 'bg-teal-50 text-teal-700 hover:bg-teal-100'} disabled:opacity-50 disabled:cursor-not-allowed`}
              style={{ fontWeight: 500 }}
            >
              {ru ? 'Новые тренды' : 'Trending New'}
            </button>
          </div>
        </div>
        <span className="text-xs text-gray-500">{ru ? (mode === 'new' ? 'Новые сигналы' : 'Последние 24 ч') : (mode === 'new' ? 'Emerging signals' : 'Last 24h')}</span>
      </div>

      <div className="space-y-2.5 max-h-[360px] overflow-y-auto pr-1">
        {visibleTopics.map((topic) => (
          <Link
            key={topic.id}
            to={`/topics?topic=${encodeURIComponent(topic.sourceTopic || topic.topic)}`}
            className="block bg-gray-50 border border-gray-100 rounded-lg p-3 cursor-pointer hover:bg-gray-100 transition-colors"
          >
            <div className="flex items-start gap-2.5">
              <span className="text-base mt-0.5">{sentimentEmoji[topic.sentiment]}</span>
              <div className="flex-1 min-w-0">
                <p className="text-xs text-gray-900 leading-relaxed" style={{ fontWeight: 500 }}>{topic.topic}</p>
                <p className="text-xs text-gray-400 italic mt-0.5 truncate">
                  {topic.sampleQuote
                    ? `“${topic.sampleQuote}”`
                    : (ru ? 'Откройте тему, чтобы увидеть доказательства из сообщений' : 'Open topic to view message evidence')}
                </p>
                <div className="flex items-center gap-3 mt-1.5">
                  {mode === 'new' && (
                    <span className="text-xs px-1.5 py-0.5 rounded bg-teal-100 text-teal-700" style={{ fontWeight: 600 }}>
                      {ru ? 'Новая' : 'Emerging'}
                    </span>
                  )}
                  <span className="text-xs px-1.5 py-0.5 rounded" style={{
                    backgroundColor: (categoryColors[topic.category] ?? '#6b7280') + '15',
                    color: categoryColors[topic.category] ?? '#6b7280',
                    fontWeight: 500,
                  }}>{topic.category}</span>
                  <span className="text-xs text-gray-500">
                    {topic.mentions} {ru ? 'упоминаний' : 'mentions'}
                  </span>
                  <span className={`text-xs ${topic.trend >= 0 ? 'text-emerald-600' : 'text-red-500'}`} style={{ fontWeight: 600 }}>{topic.trend > 0 ? '+' : ''}{topic.trend}%</span>
                </div>
              </div>
            </div>
          </Link>
        ))}
      </div>
    </div>
  );
}


// ============================================================
// W3: COMMUNITY SNAPSHOT
// ============================================================

export function CommunityBrief() {
  const { lang } = useLanguage();
  const { data } = useData();
  const ru = lang === 'ru';
  const [expanded, setExpanded] = useState(false);
  const brief = data.communityBrief;

  return (
    <div className="bg-white rounded-xl border border-sky-200 p-6 relative overflow-hidden"
      style={{ boxShadow: '0 0 0 1px rgba(2,132,199,0.08), 0 4px 24px 0 rgba(2,132,199,0.07)' }}
    >
      <div className="absolute inset-0 bg-gradient-to-br from-sky-50/60 via-white to-white pointer-events-none rounded-xl" />

      <div className="relative">
        <div className="flex items-center justify-between mb-4">
          <div className="flex items-start gap-3">
            <div className="flex-shrink-0 w-9 h-9 rounded-xl bg-gradient-to-br from-sky-500 to-blue-600 flex items-center justify-center shadow-sm mt-0.5">
              <BarChart3 className="w-4.5 h-4.5 text-white" style={{ width: '18px', height: '18px' }} />
            </div>
            <div>
              <div className="flex items-center gap-2">
                <h3 className="text-gray-900" style={{ fontSize: '1.05rem' }}>
                  {ru ? 'Снимок сообщества' : 'Community Snapshot'}
                </h3>
              </div>
              <p className="text-xs text-gray-500 mt-0.5">
                {ru ? `Обновлено ${brief.updatedMinutesAgo} мин назад · проанализировано ${brief.messagesAnalyzed.toLocaleString()} единиц` : `Updated ${brief.updatedMinutesAgo} min ago · ${brief.messagesAnalyzed.toLocaleString()} analyzed units`}
              </p>
            </div>
          </div>
          <div className="hidden sm:flex items-center gap-1.5">
            <Clock className="w-3.5 h-3.5 text-blue-600" />
            <span className="text-xs text-blue-600">
              {ru ? 'Обновление по новым данным' : 'Refreshes from new data'}
            </span>
          </div>
        </div>

        <div className="bg-gradient-to-r from-sky-50/70 to-blue-50/40 border border-sky-100 rounded-lg p-4">
          <div className="flex items-center gap-1.5 mb-2">
            <BarChart3 className="w-3 h-3 text-sky-500" />
            <span className="text-xs text-sky-600" style={{ fontWeight: 600, letterSpacing: '0.03em' }}>
              {ru ? 'Данные за последние 24 часа' : 'Last 24h data snapshot'}
            </span>
          </div>
          <p className="text-sm text-gray-800 leading-relaxed">
            {brief.mainBrief[lang]}
          </p>

          {expanded && (
              <div className="mt-3 pt-3 border-t border-sky-100 space-y-3">
                {(brief.expandedBrief[lang] ?? []).map((paragraph, i) => (
                  <p key={i} className="text-sm text-gray-800 leading-relaxed">{paragraph}</p>
                ))}
              </div>
            )}
        </div>

        <div className="mt-3 grid grid-cols-2 md:grid-cols-4 gap-3">
          <div className="bg-blue-50 border border-blue-100 rounded-lg px-3 py-2 text-center">
            <MessageCircle className="w-4 h-4 text-blue-600 mx-auto mb-1" />
            <span className="text-xs text-blue-900 block" style={{ fontWeight: 600 }}>{brief.postsAnalyzed24h.toLocaleString()}</span>
            <span className="text-xs text-blue-600">{ru ? 'Постов проанализировано' : 'Posts analyzed'}</span>
          </div>
          <div className="bg-emerald-50 border border-emerald-100 rounded-lg px-3 py-2 text-center">
            <MessageCircle className="w-4 h-4 text-emerald-600 mx-auto mb-1" />
            <span className="text-xs text-emerald-900 block" style={{ fontWeight: 600 }}>{brief.commentScopesAnalyzed24h.toLocaleString()}</span>
            <span className="text-xs text-emerald-600">{ru ? 'Контекстных групп комментариев' : 'Comment scopes analyzed'}</span>
          </div>
          <div className="bg-purple-50 border border-purple-100 rounded-lg px-3 py-2 text-center">
            <Heart className="w-4 h-4 text-purple-600 mx-auto mb-1" />
            <span className="text-xs text-purple-900 block" style={{ fontWeight: 600 }}>{brief.positiveIntentPct24h}%</span>
            <span className="text-xs text-purple-600">{ru ? 'Позитивный интент' : 'Positive intent'}</span>
          </div>
          <div className="bg-amber-50 border border-amber-100 rounded-lg px-3 py-2 text-center">
            <Zap className="w-4 h-4 text-amber-600 mx-auto mb-1" />
            <span className="text-xs text-amber-900 block" style={{ fontWeight: 600 }}>{brief.negativeIntentPct24h}%</span>
            <span className="text-xs text-amber-600">{ru ? 'Негативный интент' : 'Negative intent'}</span>
          </div>
        </div>

        <button
          onClick={() => setExpanded(!expanded)}
          className="mt-3 flex items-center gap-1 text-xs text-blue-600 hover:text-blue-700"
        >
          {expanded
            ? (ru ? 'Свернуть' : 'Show less')
            : (ru ? 'Как читать эти метрики' : 'How to read these metrics')
          }
          <ChevronRight className={`w-3.5 h-3.5 transition-transform ${expanded ? 'rotate-90' : ''}`} />
        </button>
      </div>
    </div>
  );
}
