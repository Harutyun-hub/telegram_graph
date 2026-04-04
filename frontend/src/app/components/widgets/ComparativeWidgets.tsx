import { useState } from 'react';
import { ArrowUpRight, ArrowDownRight, Minus, Star } from 'lucide-react';
import { useLanguage } from '../../contexts/LanguageContext';
import { useData } from '../../contexts/DataContext';
import { useDashboardDateRange } from '../../contexts/DashboardDateRangeContext';
import { EmptyWidget } from '../ui/EmptyWidget';
import { WidgetTitle } from '../ui/WidgetTitle';

// ============================================================
// W29: WEEK-OVER-WEEK SHIFTS
// ============================================================

export function WeekOverWeekShifts() {
  const { lang } = useLanguage();
  const { data } = useData();
  const { range } = useDashboardDateRange();
  const ru = lang === 'ru';
  const weeklyShifts = data.weeklyShifts[lang] ?? [];

  if (!weeklyShifts.length) return <EmptyWidget widgetId="week_over_week_shifts" title={ru ? 'Динамика за неделю' : 'Week-over-Week Shifts'} />;

  // ✅ FIX: use item.isInverse flag from data instead of brittle metric-name string matching.
  // Previously: const churnLabels = ['Churn Signals', 'Сигналы оттока'];
  // Now any future metric can be marked isInverse in mockData/API without widget changes.

  return (
    <div className="bg-white rounded-xl border border-gray-200 p-6">
      <div className="mb-1 flex flex-col items-start gap-2 sm:flex-row sm:items-start sm:justify-between">
        <WidgetTitle widgetId="week_over_week_shifts">
          {ru ? 'Динамика за неделю' : 'Week-over-Week Shifts'}
        </WidgetTitle>
        <span className="w-full text-left text-xs text-gray-500 sm:w-auto sm:text-right">
          {ru ? `Сравнение с предыдущим окном (${range.days} дн.)` : `Compared with the previous ${range.days}-day window`}
        </span>
      </div>
      <p className="text-xs text-gray-500 mb-4">
        {ru
          ? 'Смотрите на изменения — что улучшается, а что требует внимания?'
          : 'Focus on the deltas — what\'s improving and what needs attention?'}
      </p>

      <div className="grid grid-cols-3 gap-2">
        {weeklyShifts.map((item) => {
          const diff = item.current - item.previous;
          const isUp = diff > 0;
          const isFlat = diff === 0;
          const pctChange = item.previous === 0
            ? (item.current === 0 ? '0.0%' : (ru ? 'нов.' : 'new'))
            : `${isUp ? '+' : ''}${((diff / item.previous) * 100).toFixed(1)}%`;
          // ✅ FIX: isInverse is now a typed field on WeeklyShiftItem — no string matching
          const isGood = !isFlat && (item.isInverse ? !isUp : isUp);
          const deltaClass = isFlat ? 'text-gray-400' : (isGood ? 'text-emerald-500' : 'text-red-500');

          return (
            <div key={item.metricKey || item.metric} className="bg-gray-50 rounded-lg p-2.5">
              <span className="text-xs text-gray-500 block truncate">{item.metric}</span>
              <div className="flex items-center justify-between mt-1">
                <span className="text-sm text-gray-900" style={{ fontWeight: 600 }}>
                  {item.current > 999 ? item.current.toLocaleString() : item.current}{item.unit}
                </span>
                <div className="flex items-center gap-0.5">
                  {isFlat
                    ? <Minus className="w-3 h-3 text-gray-400" />
                    : isUp
                      ? <ArrowUpRight className={`w-3 h-3 ${deltaClass}`} />
                      : <ArrowDownRight className={`w-3 h-3 ${deltaClass}`} />
                  }
                  <span className={deltaClass} style={{ fontWeight: 600, fontSize: '10px' }}>
                    {pctChange}
                  </span>
                </div>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}


// ============================================================
// W30: SENTIMENT BY TOPIC
// ============================================================

export function SentimentByTopic() {
  const { lang } = useLanguage();
  const { data } = useData();
  const ru = lang === 'ru';
  const [showAll, setShowAll] = useState(false);
  const sentimentByTopic = data.sentimentByTopic[lang] ?? [];

  if (!sentimentByTopic.length) return <EmptyWidget widgetId="sentiment_by_topic" title={ru ? 'Тональность по темам' : 'Sentiment by Topic'} />;

  // ✅ GENERIC: compute top positive and top negative topics dynamically
  const sortedByPos = [...sentimentByTopic].sort((a, b) => b.positive - a.positive);
  const sortedByNeg = [...sentimentByTopic].sort((a, b) => b.negative - a.negative);
  const top3pos = sortedByPos.slice(0, 3);
  const top2neg = sortedByNeg.slice(0, 2);
  const visibleRows = showAll ? sentimentByTopic : sentimentByTopic.slice(0, 10);

  return (
    <div className="bg-white rounded-xl border border-gray-200 p-6">
      <div className="mb-1 flex flex-col items-start gap-2 sm:flex-row sm:items-start sm:justify-between">
        <WidgetTitle widgetId="sentiment_by_topic">
          {ru ? 'Тональность по темам' : 'Sentiment by Topic'}
        </WidgetTitle>
        <span className="w-full text-left text-xs text-gray-500 sm:w-auto sm:text-right">
          {ru ? 'Что радует, а что раздражает' : 'What makes people happy vs. frustrated'}
        </span>
      </div>
      <p className="text-xs text-gray-500 mb-4">
        {ru
          ? 'Зелёный = позитивные разговоры. Красный = раздражение. Используйте для контент-стратегии.'
          : 'Green = positive conversations. Red = frustration. Use this to guide content strategy.'}
      </p>

      <div className="space-y-2.5">
        {visibleRows.map((item) => (
          <div key={item.topic}>
            <div className="flex items-center justify-between mb-1">
              <span className="text-xs text-gray-700" style={{ fontWeight: 500 }}>{item.topic}</span>
              <span className="text-xs text-gray-400">{item.volume.toLocaleString()}</span>
            </div>
            <div className="flex h-3 rounded-full overflow-hidden">
              <div style={{ width: `${item.positive}%`, backgroundColor: '#10b981' }} />
              <div style={{ width: `${item.neutral}%`, backgroundColor: '#d1d5db' }} />
              <div style={{ width: `${item.negative}%`, backgroundColor: '#ef4444' }} />
            </div>
          </div>
        ))}
      </div>

      {sentimentByTopic.length > 10 && (
        <div className="mt-3">
          <button
            type="button"
            onClick={() => setShowAll((prev) => !prev)}
            className="text-xs text-teal-700 hover:text-teal-800"
            style={{ fontWeight: 600 }}
          >
            {showAll
              ? (ru ? 'Свернуть' : 'See top 10')
              : (ru ? `Показать все ${sentimentByTopic.length}` : `See all ${sentimentByTopic.length}`)}
          </button>
        </div>
      )}

      <div className="flex items-center gap-4 mt-4 pt-3 border-t border-gray-100">
        <div className="flex items-center gap-1.5">
          <div className="w-3 h-2 rounded bg-emerald-500" />
          <span className="text-xs text-gray-500">{ru ? 'Позитив' : 'Positive'}</span>
        </div>
        <div className="flex items-center gap-1.5">
          <div className="w-3 h-2 rounded bg-gray-300" />
          <span className="text-xs text-gray-500">{ru ? 'Нейтрально' : 'Neutral'}</span>
        </div>
        <div className="flex items-center gap-1.5">
          <div className="w-3 h-2 rounded bg-red-500" />
          <span className="text-xs text-gray-500">{ru ? 'Негатив' : 'Negative'}</span>
        </div>
      </div>

      <div className="mt-3 grid grid-cols-2 gap-3">
        <div className="bg-emerald-50 border border-emerald-100 rounded-lg px-3 py-2">
          <span className="text-xs text-emerald-900 block" style={{ fontWeight: 600 }}>
            {ru ? 'Усиливайте эти темы' : 'Amplify these'}
          </span>
          <p className="text-xs text-emerald-700">
            {top3pos.map(t => `${t.topic} (${t.positive}%)`).join(', ')}
            {ru ? ' — максимум контента здесь' : ' — maximize content here'}
          </p>
        </div>
        <div className="bg-red-50 border border-red-100 rounded-lg px-3 py-2">
          <span className="text-xs text-red-900 block" style={{ fontWeight: 600 }}>
            {ru ? 'Решайте эти проблемы' : 'Help with these'}
          </span>
          <p className="text-xs text-red-700">
            {top2neg.map(t => `${t.topic} (${t.negative}% ${ru ? 'негатива' : 'negative'})`).join(', ')}
            {ru ? ' — создавайте решения' : ' — create solutions'}
          </p>
        </div>
      </div>
    </div>
  );
}


// ============================================================
// W31: CONTENT PERFORMANCE TRACKER
// ============================================================

export function ContentPerformance() {
  const { lang } = useLanguage();
  const { data } = useData();
  const ru = lang === 'ru';
  const topPosts = data.topPosts[lang] ?? [];
  const contentTypePerformance = data.contentTypePerformance[lang] ?? [];

  if (!topPosts.length && !contentTypePerformance.length) return <EmptyWidget widgetId="content_performance" title={ru ? 'Эффективность контента' : 'Content Performance'} />;

  // ✅ GENERIC: sort copies, not originals (avoid shared-state mutation)
  // ✅ GENERIC: compute strategy insight dynamically from data
  const sortedTypes = [...contentTypePerformance].sort((a, b) => b.avgEngagement - a.avgEngagement);
  const sortedPosts = [...topPosts].sort((a, b) => b.engagement - a.engagement);
  const top2types = sortedTypes.slice(0, 2);

  return (
    <div className="bg-white rounded-xl border border-gray-200 p-6">
      <div className="mb-1 flex flex-col items-start gap-2 sm:flex-row sm:items-start sm:justify-between">
        <WidgetTitle widgetId="content_performance">
          {ru ? 'Эффективность контента' : 'Content Performance'}
        </WidgetTitle>
        <span className="w-full text-left text-xs text-gray-500 sm:w-auto sm:text-right">
          {ru ? 'Какой контент работает лучше всего' : 'What content works best'}
        </span>
      </div>
      <p className="text-xs text-gray-500 mb-4">
        {ru
          ? 'Учитесь у лучших публикаций — создавайте больше того, что резонирует'
          : 'Learn from winners — create more of what resonates'}
      </p>

      {/* Content type average */}
      <div className="mb-4">
        <span className="text-xs text-gray-500 block mb-2" style={{ fontWeight: 500 }}>
          {ru ? 'Средняя вовлечённость по формату' : 'Avg engagement by format'}
        </span>
        <div className="space-y-1.5">
          {sortedTypes.map((ct) => (
            <div key={ct.type} className="flex items-center gap-2">
              <span className="text-xs text-gray-700 w-28" style={{ fontWeight: 500 }}>{ct.type}</span>
              <div className="flex-1 bg-gray-100 rounded-full h-2">
                <div
                  className="h-2 rounded-full bg-teal-500"
                  style={{ width: `${ct.avgEngagement}%`, opacity: 0.5 + (ct.avgEngagement / 100) * 0.5 }}
                />
              </div>
              <span className="text-xs text-gray-900 w-8 text-right" style={{ fontWeight: 600 }}>{ct.avgEngagement}</span>
              <span className="text-xs text-gray-400 w-16 text-right">
                {ct.count} {ru ? 'публ.' : 'posts'}
              </span>
            </div>
          ))}
        </div>
      </div>

      {/* Top posts */}
      <div className="pt-3 border-t border-gray-100">
        <span className="text-xs text-gray-500 block mb-2" style={{ fontWeight: 500 }}>
          {ru ? 'Лучшие публикации' : 'Top performing posts'}
        </span>
        <div className="space-y-2">
          {sortedPosts.slice(0, 5).map((post) => (
            <div key={post.title} className="flex items-center gap-2 py-1">
              <Star className="w-3 h-3 text-amber-400 flex-shrink-0" />
              <span className="text-xs text-gray-700 flex-1 truncate">{post.title}</span>
              <span className="text-xs px-1.5 py-0.5 rounded bg-gray-100 text-gray-500">{post.type}</span>
              <span className="text-xs text-gray-500">{post.shares} {ru ? 'репостов' : 'shares'}</span>
            </div>
          ))}
        </div>
      </div>

      <div className="mt-3 bg-blue-50 border border-blue-100 rounded-lg px-3 py-2">
        <p className="text-xs text-blue-800">
          {top2types.length >= 2 && (ru
            ? <><span style={{ fontWeight: 600 }}>Стратегия:</span> {top2types[0].type} ({top2types[0].avgEngagement} баллов) и {top2types[1].type} ({top2types[1].avgEngagement}) показывают лучший результат. Всего {top2types[0].count} и {top2types[1].count} публикаций — удвоение их числа, скорее всего, удвоит общую вовлечённость.</>
            : <><span style={{ fontWeight: 600 }}>Strategy:</span> {top2types[0].type} ({top2types[0].avgEngagement} avg) and {top2types[1].type} ({top2types[1].avgEngagement} avg) perform best. You only have {top2types[0].count} and {top2types[1].count} posts — doubling these would likely double overall engagement.</>
          )}
        </p>
      </div>
    </div>
  );
}


// ============================================================
// W32: COMMUNITY VITALITY SCORECARD
// ============================================================

export function CommunityVitalityScorecard() {
  const { lang } = useLanguage();
  const { data } = useData();
  const ru = lang === 'ru';
  const vitalityIndicators = data.vitalityIndicators[lang] ?? [];

  if (!vitalityIndicators.length) return <EmptyWidget title={ru ? 'Витальность сообщества' : 'Community Vitality Scorecard'} />;

  const compositeScore = Math.round(vitalityIndicators.reduce((acc, v) => acc + v.score, 0) / vitalityIndicators.length);

  // ✅ FIX: use benchmarkLevel enum field instead of brittle localized string matching.
  // Previously: benchmarkGood = ['Excellent', 'Top 10%'] / ['Отлично', 'Топ 10%'] — breaks if labels change.
  const getBenchmarkClass = (level: string) => {
    if (level === 'excellent')             return 'bg-emerald-100 text-emerald-700';
    if (level === 'good' || level === 'above_avg') return 'bg-blue-100 text-blue-700';
    if (level === 'average')               return 'bg-gray-100 text-gray-600';
    return 'bg-amber-100 text-amber-700'; // below_avg | poor
  };

  // ✅ GENERIC: compute top 3 strengths and bottom 2 focus areas dynamically
  const sortedByScore = [...vitalityIndicators].sort((a, b) => b.score - a.score);
  const top3 = sortedByScore.slice(0, 3);
  const bottom2 = sortedByScore.slice(-2).reverse();

  return (
    <div className="bg-white rounded-xl border border-gray-200 p-6">
      <div className="flex items-center justify-between mb-1">
        <h3 className="text-gray-900" style={{ fontSize: '1.05rem' }}>
          {ru ? 'Витальность сообщества' : 'Community Vitality Scorecard'}
        </h3>
        <div className="flex items-center gap-2">
          <span className="text-2xl text-gray-900" style={{ fontWeight: 600 }}>{compositeScore}</span>
          <span className="text-xs text-gray-500">/100</span>
        </div>
      </div>
      <p className="text-xs text-gray-500 mb-4">
        {ru
          ? 'Комплексная проверка здоровья — мы строим сообщество или просто чат?'
          : 'Holistic health check — are we building a community or just a chat group?'}
      </p>

      <div className="space-y-3">
        {vitalityIndicators.map((item) => (
          <div key={item.indicator}>
            <div className="flex items-center justify-between mb-1">
              <div className="flex items-center gap-2">
                <span className="text-sm">{item.emoji}</span>
                <span className="text-xs text-gray-700" style={{ fontWeight: 500 }}>{item.indicator}</span>
              </div>
              <div className="flex items-center gap-2">
                <span className={`text-xs px-1.5 py-0.5 rounded ${getBenchmarkClass(item.benchmarkLevel)}`}>{item.benchmark}</span>
                <span className={`text-xs ${item.trend > 0 ? 'text-emerald-500' : 'text-red-500'}`} style={{ fontWeight: 500 }}>
                  {item.trend > 0 ? '+' : ''}{item.trend}
                </span>
                <span className="text-xs text-gray-900" style={{ fontWeight: 600 }}>{item.score}</span>
              </div>
            </div>
            <div className="w-full bg-gray-100 rounded-full h-2">
              <div
                className="h-2 rounded-full transition-all"
                style={{
                  width: `${item.score}%`,
                  backgroundColor: item.score >= 70 ? '#10b981' : item.score >= 50 ? '#3b82f6' : '#f59e0b',
                }}
              />
            </div>
          </div>
        ))}
      </div>

      <div className="mt-4 pt-3 border-t border-gray-100 grid grid-cols-2 gap-3">
        <div className="bg-emerald-50 border border-emerald-100 rounded-lg px-3 py-2">
          <span className="text-xs text-emerald-900 block" style={{ fontWeight: 600 }}>
            {ru ? 'Сильные стороны' : 'Strengths'}
          </span>
          <p className="text-xs text-emerald-700">
            {top3.map(v => `${v.indicator} (${v.score})`).join(', ')}
            {ru ? ' — сообщество живёт и растёт' : ' — your community is alive and growing'}
          </p>
        </div>
        <div className="bg-amber-50 border border-amber-100 rounded-lg px-3 py-2">
          <span className="text-xs text-amber-900 block" style={{ fontWeight: 600 }}>
            {ru ? 'Фокус улучшений' : 'Focus areas'}
          </span>
          <p className="text-xs text-amber-700">
            {bottom2.map(v => `${v.indicator} (${v.score})`).join(', ')}
            {ru ? ' — требуют приоритетного внимания' : ' — need priority attention'}
          </p>
        </div>
      </div>
    </div>
  );
}
