import { useMemo, useState } from 'react';
import { Link } from 'react-router';
import { ResponsiveContainer, LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip } from 'recharts';
import { useLanguage } from '../../contexts/LanguageContext';
import { useData } from '../../contexts/DataContext';
import { EmptyWidget } from '../ui/EmptyWidget';
import { WidgetTitle } from '../ui/WidgetTitle';

// ============================================================
// W4: TOPIC LANDSCAPE
// ============================================================

// Dynamic category color fallback
const FALLBACK_COLORS = ['#ef4444', '#3b82f6', '#8b5cf6', '#f59e0b', '#ec4899', '#10b981', '#06b6d4', '#6b7280', '#f97316', '#14b8a6'];

function getCategoryColor(_category: string, index: number): string {
  return FALLBACK_COLORS[index % FALLBACK_COLORS.length];
}

function topicTileSpec(value: number, minValue: number, maxValue: number) {
  const range = Math.max(1, maxValue - minValue);
  const normalized = (value - minValue) / range;

  if (normalized >= 0.82) {
    return {
      tileClass: 'col-span-2 md:col-span-4 xl:col-span-5 row-span-3',
      titleClass: 'text-base md:text-[17px]',
      lineClamp: 2,
      compactMeta: false,
      showMeta: true,
      showSnippet: true,
    };
  }
  if (normalized >= 0.55) {
    return {
      tileClass: 'col-span-2 md:col-span-3 xl:col-span-4 row-span-2',
      titleClass: 'text-sm md:text-[15px]',
      lineClamp: 2,
      compactMeta: false,
      showMeta: true,
      showSnippet: false,
    };
  }
  if (normalized >= 0.30) {
    return {
      tileClass: 'col-span-1 md:col-span-3 xl:col-span-3 row-span-2',
      titleClass: 'text-xs md:text-sm',
      lineClamp: 2,
      compactMeta: false,
      showMeta: true,
      showSnippet: false,
    };
  }
  return {
    tileClass: 'col-span-1 md:col-span-2 xl:col-span-2 row-span-1',
    titleClass: 'text-[11px]',
    lineClamp: 1,
    compactMeta: true,
    showMeta: false,
    showSnippet: false,
  };
}

export function TopicLandscape() {
  const { lang } = useLanguage();
  const { data, displayRange } = useData();
  const ru = lang === 'ru';
  const topicBubbles = data.topicBubbles[lang] ?? [];
  const [activeCategory, setActiveCategory] = useState<string>('__all__');
  const rangeDays = displayRange?.days ?? 0;

  const totalMentions = topicBubbles.reduce((sum, t) => sum + (t.value || 0), 0);
  const lowEvidenceGrowth = topicBubbles.filter((t) => !t.growthReliable).length;

  const categoryLegend = useMemo(() => {
    const map = new Map<string, { color: string; topics: number; mentions: number }>();
    topicBubbles.forEach((item) => {
      const current = map.get(item.category);
      if (!current) {
        map.set(item.category, { color: item.color, topics: 1, mentions: item.value || 0 });
        return;
      }
      current.topics += 1;
      current.mentions += item.value || 0;
    });

    return Array.from(map.entries())
      .map(([category, meta]) => ({ category, ...meta }))
      .sort((a, b) => b.mentions - a.mentions);
  }, [topicBubbles]);

  const primaryCategories = categoryLegend.slice(0, 6);
  const extraCategories = categoryLegend.slice(6);

  const visibleTopics = useMemo(() => {
    if (activeCategory === '__all__') return topicBubbles;
    return topicBubbles.filter((item) => item.category === activeCategory);
  }, [topicBubbles, activeCategory]);

  if (!topicBubbles.length) return <EmptyWidget widgetId="topic_landscape" title={ru ? 'Карта тем' : 'Topic Landscape'} />;

  const maxValue = Math.max(...visibleTopics.map((t) => t.value || 0), 1);
  const minValue = Math.min(...visibleTopics.map((t) => t.value || 0), maxValue);

  return (
    <div className="bg-white rounded-xl border border-gray-200 p-6">
      <div className="mb-1 flex flex-col items-start gap-2 sm:flex-row sm:items-start sm:justify-between">
        <WidgetTitle widgetId="topic_landscape">
          {ru ? 'Карта тем' : 'Topic Landscape'}
        </WidgetTitle>
        <span className="w-full text-left text-xs text-gray-500 sm:w-auto sm:text-right">
          {ru ? 'Что обсуждают чаще всего' : 'Most discussed topics'}
        </span>
      </div>
      <p className="text-xs text-gray-500 mb-4">
        {ru
          ? `Площадь плитки = число прямых упоминаний в выбранном окне (${rangeDays} дн.). Рост = последние 7 дней к предыдущим 7 дням и показывается только при достаточной статистике.`
          : `Tile area = direct message mentions in the selected ${rangeDays}-day window. Growth = last 7 days vs previous 7 days, shown only with sufficient evidence.`}
      </p>

      <div className="flex flex-wrap items-center gap-2 mb-4">
        <button
          onClick={() => setActiveCategory('__all__')}
          className={`text-xs px-2.5 py-1 rounded-full border transition-colors ${
            activeCategory === '__all__' ? 'bg-slate-800 text-white border-slate-800' : 'bg-white text-gray-600 border-gray-200 hover:bg-gray-50'
          }`}
          style={{ fontWeight: 500 }}
        >
          {ru ? 'Все категории' : 'All categories'}
        </button>
        {primaryCategories.map((cat, i) => (
          <button
            key={cat.category}
            onClick={() => setActiveCategory(cat.category)}
            className={`text-xs px-2.5 py-1 rounded-full border transition-colors ${
              activeCategory === cat.category ? 'bg-slate-100 text-slate-900 border-slate-300' : 'bg-white text-gray-600 border-gray-200 hover:bg-gray-50'
            }`}
            style={{ fontWeight: 500 }}
          >
            <span className="inline-flex items-center gap-1.5">
              <span className="w-2 h-2 rounded-full" style={{ backgroundColor: cat.color ?? getCategoryColor(cat.category, i) }} />
              <span>{cat.category}</span>
              <span className="text-gray-400">· {cat.topics}</span>
            </span>
          </button>
        ))}

        {extraCategories.length > 0 && (
          <details className="relative">
            <summary className="list-none text-xs px-2.5 py-1 rounded-full border bg-white text-gray-600 border-gray-200 hover:bg-gray-50 cursor-pointer" style={{ fontWeight: 500 }}>
              {ru ? `Ещё категории (${extraCategories.length})` : `More categories (${extraCategories.length})`}
            </summary>
            <div className="absolute z-20 mt-2 w-[320px] max-w-[85vw] rounded-xl border border-gray-200 bg-white shadow-lg p-2">
              <div className="flex flex-wrap gap-1.5">
                {extraCategories.map((cat, i) => (
                  <button
                    key={cat.category}
                    onClick={() => setActiveCategory(cat.category)}
                    className={`text-xs px-2 py-1 rounded-full border transition-colors ${
                      activeCategory === cat.category ? 'bg-slate-100 text-slate-900 border-slate-300' : 'bg-white text-gray-600 border-gray-200 hover:bg-gray-50'
                    }`}
                    style={{ fontWeight: 500 }}
                  >
                    <span className="inline-flex items-center gap-1.5">
                      <span className="w-2 h-2 rounded-full" style={{ backgroundColor: cat.color ?? getCategoryColor(cat.category, i) }} />
                      <span>{cat.category}</span>
                    </span>
                  </button>
                ))}
              </div>
            </div>
          </details>
        )}
      </div>

      <div className="grid grid-cols-2 md:grid-cols-8 xl:grid-cols-12 auto-rows-[64px] gap-3">
        {visibleTopics.map((topic) => {
          const spec = topicTileSpec(topic.value || 0, minValue, maxValue);
          const growthLabel = topic.growthReliable
            ? `${topic.growth > 0 ? '+' : ''}${topic.growth}%`
            : (ru ? 'н/д' : 'n/a');
          const growthClass = topic.growthReliable
            ? (topic.growth >= 0 ? 'text-emerald-600' : 'text-red-500')
            : 'text-gray-400';
          const borderColor = `${topic.color}55`;
          const accentColor = `${topic.color}D9`;

          return (
            <Link
              key={`${topic.sourceTopic || topic.name}-${topic.category}`}
              to={`/topics?topic=${encodeURIComponent(topic.sourceTopic || topic.name)}`}
              className={`${spec.tileClass} relative rounded-xl border bg-white p-2 md:p-3 overflow-hidden hover:shadow-sm transition-shadow`}
              style={{ borderColor }}
            >
              <div className="absolute left-0 top-0 w-full h-1" style={{ backgroundColor: accentColor }} />

              <div className={spec.compactMeta ? 'grid h-full min-h-0 grid-rows-[auto_1fr_auto] gap-1' : 'flex h-full min-h-0 flex-col gap-1'}>
                <span
                  className={`${spec.titleClass} shrink-0 text-gray-900 ${spec.compactMeta ? 'block truncate leading-[1.2]' : 'break-words leading-tight'}`}
                  style={{
                    fontWeight: 600,
                    ...(spec.compactMeta
                      ? {}
                      : {
                          display: '-webkit-box',
                          WebkitLineClamp: spec.lineClamp,
                          WebkitBoxOrient: 'vertical',
                          overflow: 'hidden',
                        }),
                  }}
                  title={topic.name}
                >
                  {topic.name}
                </span>

                {spec.showSnippet && topic.sampleQuote && (
                  <span
                    className="text-[11px] text-gray-600 leading-tight"
                    style={{
                      display: '-webkit-box',
                      WebkitLineClamp: 2,
                      WebkitBoxOrient: 'vertical',
                      overflow: 'hidden',
                    }}
                    title={topic.sampleQuote}
                  >
                    {topic.sampleQuote}
                  </span>
                )}

                {spec.compactMeta ? (
                  <div className="mt-auto flex items-center justify-between gap-2">
                    <div className="min-w-0 text-[11px] text-gray-600">
                      {topic.value.toLocaleString()} {ru ? 'упоминаний' : 'mentions'}
                    </div>
                    <div className={`shrink-0 text-[11px] ${growthClass}`} style={{ fontWeight: 700 }}>
                      {growthLabel}
                    </div>
                  </div>
                ) : (
                  <div className="mt-auto flex items-end justify-between gap-2">
                    <div className="min-w-0">
                      <div className="text-[11px] text-gray-600">
                        {topic.value.toLocaleString()} {ru ? 'упоминаний' : 'mentions'}
                      </div>
                      {spec.showMeta && (
                        <div className="text-[10px] text-gray-500">
                          {ru ? 'категория' : 'category'}: {topic.category}
                        </div>
                      )}
                    </div>

                    <div className="text-right">
                      <div className="text-[10px] text-gray-500" style={{ fontWeight: 600 }}>
                        {ru ? '7д Δ' : '7d Δ'}
                      </div>
                      <div className={`text-xs ${growthClass}`} style={{ fontWeight: 700 }}>
                        {growthLabel}
                      </div>
                      {spec.showMeta && (
                        <div className="text-[10px] text-gray-500">
                          {topic.growthReliable ? (ru ? 'статистика достаточна' : 'evidence sufficient') : (ru ? 'мало данных' : 'low evidence')}
                        </div>
                      )}
                    </div>
                  </div>
                )}
              </div>
            </Link>
          );
        })}
      </div>

      <div className="flex flex-wrap items-center gap-4 mt-4 pt-3 border-t border-gray-100">
        <span className="text-xs text-gray-500">
          {ru ? 'Цвет полоски = категория темы' : 'Top strip color = topic category'}
        </span>
        <span className="text-xs text-gray-500">
          {ru ? '7д Δ = изменение за 7 дней к предыдущим 7 дням' : '7d Δ = last 7 days vs previous 7 days'}
        </span>
        <Link to="/topics" className="ml-auto text-xs text-blue-600 hover:text-blue-800 hover:underline transition-colors" style={{ fontWeight: 500 }}>
          {ru ? 'Все темы →' : 'See all topics →'}
        </Link>
      </div>

      <p className="text-xs text-gray-400 mt-2">
        {ru
          ? `Основа: ${totalMentions.toLocaleString()} прямых упоминаний в окне ${rangeDays} дн. по ${topicBubbles.length} темам. ${lowEvidenceGrowth > 0 ? `${lowEvidenceGrowth} тем с недостатком данных для роста.` : ''}`
          : `Evidence: ${totalMentions.toLocaleString()} direct mentions in the ${rangeDays}-day window across ${topicBubbles.length} topics. ${lowEvidenceGrowth > 0 ? `${lowEvidenceGrowth} topics have insufficient growth evidence.` : ''}`}
      </p>
    </div>
  );
}


// ============================================================
// W5: CONVERSATION TRENDS
// ============================================================

export function ConversationTrends() {
  const { lang } = useLanguage();
  const { data, displayRange } = useData();
  const ru = lang === 'ru';
  const trendLines = data.trendLines[lang] ?? [];
  const trendData = data.trendData;
  const rangeDays = displayRange?.days ?? 0;

  if (!trendLines.length || !trendData.length) return <EmptyWidget widgetId="conversation_trends" title={ru ? 'Динамика разговоров' : 'Conversation Trends'} />;

  // Compute fastest-growing topics dynamically
  const sorted = [...trendLines].sort((a, b) => b.change - a.change);
  const top2 = sorted.slice(0, 2);

  return (
    <div className="bg-white rounded-xl border border-gray-200 p-6">
      <div className="mb-1 flex flex-col items-start gap-2 sm:flex-row sm:items-start sm:justify-between">
        <WidgetTitle widgetId="conversation_trends">
          {ru ? 'Динамика разговоров' : 'Conversation Trends'}
        </WidgetTitle>
        <span className="w-full text-left text-xs text-gray-500 sm:w-auto sm:text-right">{ru ? `${trendData.length}-дневная траектория` : `${trendData.length}-day trajectory`}</span>
      </div>
      <p className="text-xs text-gray-500 mb-4">
        {ru ? `Какие темы растут? Что угасает? Траектория строится по выбранному окну (${rangeDays} дн.).` : `What topics are rising? What's fading? The trajectory follows the selected ${rangeDays}-day window.`}
      </p>

      <ResponsiveContainer width="100%" height={220}>
        <LineChart data={trendData}>
          <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" />
          <XAxis dataKey="week" tick={{ fontSize: 11 }} stroke="#9ca3af" />
          <YAxis tick={{ fontSize: 11 }} stroke="#9ca3af" />
          <Tooltip />
          {trendLines.map((line) => (
            <Line key={line.key} type="monotone" dataKey={line.key} stroke={line.color} strokeWidth={2} dot={false} />
          ))}
        </LineChart>
      </ResponsiveContainer>

      <div className="grid grid-cols-3 gap-2 mt-3">
        {trendLines.map((line) => (
          <div key={line.key} className="flex items-center gap-2">
            <div className="w-2.5 h-2.5 rounded-full flex-shrink-0" style={{ backgroundColor: line.color }} />
            <span className="text-xs text-gray-600 truncate">{line.label}</span>
            <span className={`text-xs ml-auto ${line.change > 0 ? 'text-emerald-500' : 'text-red-500'}`} style={{ fontWeight: 600 }}>
              {line.change > 0 ? '+' : ''}{line.change}%
            </span>
          </div>
        ))}
      </div>

      {top2.length >= 2 && (
        <div className="mt-3 pt-3 border-t border-gray-100">
          <div className="bg-emerald-50 border border-emerald-100 rounded-lg px-3 py-2">
            <p className="text-xs text-emerald-800">
              {ru
                ? <><span style={{ fontWeight: 600 }}>Быстрее всего растут:</span> {top2[0].label} ({top2[0].change > 0 ? '+' : ''}{top2[0].change}%) и {top2[1].label} ({top2[1].change > 0 ? '+' : ''}{top2[1].change}%)</>
                : <><span style={{ fontWeight: 600 }}>Fastest growing:</span> {top2[0].label} ({top2[0].change > 0 ? '+' : ''}{top2[0].change}%) and {top2[1].label} ({top2[1].change > 0 ? '+' : ''}{top2[1].change}%)</>
              }
            </p>
          </div>
        </div>
      )}
    </div>
  );
}


// ============================================================
// W6: CONTENT ENGAGEMENT HEATMAP
// ============================================================

function getEngColor(v: number) {
  if (v >= 80) return '#059669'; if (v >= 60) return '#10b981';
  if (v >= 40) return '#6ee7b7'; if (v >= 20) return '#d1fae5'; return '#f0fdf4';
}
function getEngText(v: number) { return v >= 60 ? '#ffffff' : '#374151'; }

export function ContentEngagementHeatmap() {
  const { lang } = useLanguage();
  const { data } = useData();
  const ru = lang === 'ru';
  const heatmapData = data.heatmap[lang];
  const contentTypes = heatmapData?.contentTypes ?? [];
  const topicCols = heatmapData?.topicCols ?? [];
  const engagement = heatmapData?.engagement ?? {};

  if (!contentTypes.length || !topicCols.length) return <EmptyWidget title={ru ? 'Карта вовлечённости по контенту' : 'Content Engagement Map'} />;

  // Compute top performer dynamically
  let maxVal = 0, maxType = '', maxTopic = '';
  contentTypes.forEach(type => {
    topicCols.forEach(topic => {
      const v = engagement[type]?.[topic] ?? 0;
      if (v > maxVal) { maxVal = v; maxType = type; maxTopic = topic; }
    });
  });

  return (
    <div className="bg-white rounded-xl border border-gray-200 p-6">
      <div className="flex items-center justify-between mb-1">
        <h3 className="text-gray-900" style={{ fontSize: '1.05rem' }}>
          {ru ? 'Карта вовлечённости по контенту' : 'Content Engagement Map'}
        </h3>
        <span className="text-xs text-gray-500">{ru ? 'Формат x Тема' : 'Format x Topic performance'}</span>
      </div>
      <p className="text-xs text-gray-500 mb-4">
        {ru
          ? 'Какой формат работает лучше для каждой темы? Используйте для планирования контент-стратегии.'
          : 'Which content format works best for each topic? Plan your content strategy.'}
      </p>

      <div className="overflow-x-auto">
        <table className="w-full">
          <thead>
            <tr>
              <th className="text-xs text-gray-500 text-left pb-2 pr-3" style={{ fontWeight: 500 }}></th>
              {topicCols.map((t) => (
                <th key={t} className="text-xs text-gray-500 pb-2 px-1 text-center" style={{ fontWeight: 500 }}>{t}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {contentTypes.map((type) => (
              <tr key={type}>
                <td className="text-xs text-gray-700 pr-3 py-1 whitespace-nowrap" style={{ fontWeight: 500 }}>{type}</td>
                {topicCols.map((topic) => {
                  const value = engagement[type]?.[topic] ?? 0;
                  return (
                    <td key={topic} className="px-1 py-1">
                      <div
                        className="rounded-md flex items-center justify-center h-9 cursor-pointer hover:ring-2 hover:ring-gray-900 transition-all"
                        style={{ backgroundColor: getEngColor(value), color: getEngText(value) }}
                      >
                        <span className="text-xs" style={{ fontWeight: 600 }}>{value}</span>
                      </div>
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="flex items-center gap-2 mt-4 pt-3 border-t border-gray-100">
        <span className="text-xs text-gray-500">{ru ? 'Низкое' : 'Low'}</span>
        <div className="flex gap-0.5">
          {[10, 30, 50, 70, 85].map((v) => (
            <div key={v} className="w-6 h-3 rounded-sm" style={{ backgroundColor: getEngColor(v) }} />
          ))}
        </div>
        <span className="text-xs text-gray-500">{ru ? 'Высокое вовлечение' : 'High engagement'}</span>
        {maxVal > 0 && (
          <span className="text-xs text-gray-400 ml-auto">
            {maxType} + {maxTopic} = {maxVal} ({ru ? 'лидер' : 'top performer'})
          </span>
        )}
      </div>
    </div>
  );
}


// ============================================================
// W7: QUESTION CLOUD
// ============================================================

export function QuestionCloud() {
  const { lang } = useLanguage();
  const { data } = useData();
  const ru = lang === 'ru';
  const questionBriefs = data.questionBriefs[lang] ?? [];

  if (!questionBriefs.length) {
    return <EmptyWidget widgetId="question_cloud" title={ru ? 'Самые частые вопросы' : 'Most Asked Questions'} />;
  }

  const needsGuideCount = questionBriefs.filter((b) => b.status === 'needs_guide').length;
  const totalSignals = questionBriefs.reduce((sum, b) => sum + (b.demandSignals?.messages || 0), 0);
  const avgConfidence = questionBriefs.length
    ? Math.round((questionBriefs.reduce((sum, b) => sum + (b.confidenceScore || 0), 0) / questionBriefs.length) * 100)
    : 0;

  const statusLabel = (status: string) => {
    if (status === 'needs_guide') return ru ? 'Нужен чёткий гайд' : 'Needs a clear guide';
    if (status === 'well_covered') return ru ? 'Тема в целом покрыта' : 'Mostly covered';
    return ru ? 'Частично покрыта' : 'Partially covered';
  };

  const statusClass = (status: string) => {
    if (status === 'needs_guide') return 'bg-amber-50 border-amber-200 text-amber-900';
    if (status === 'well_covered') return 'bg-emerald-50 border-emerald-200 text-emerald-900';
    return 'bg-blue-50 border-blue-200 text-blue-900';
  };

  const confidenceLabel = (value: string) => {
    if (value === 'high') return ru ? 'Высокая' : 'High';
    if (value === 'medium') return ru ? 'Средняя' : 'Medium';
    return ru ? 'Низкая' : 'Low';
  };

  return (
      <div className="bg-white rounded-xl border border-gray-200 p-6">
        <div className="mb-1 flex flex-col items-start gap-2 sm:flex-row sm:items-start sm:justify-between">
          <WidgetTitle widgetId="question_cloud">
            {ru ? 'Самые частые вопросы' : 'Most Asked Questions'}
          </WidgetTitle>
          <span className="w-full text-left text-xs text-gray-500 sm:w-auto sm:text-right">{ru ? 'AI + доказательства' : 'AI + evidence grounded'}</span>
        </div>
        <p className="text-xs text-gray-500 mb-4">
          {ru
            ? 'Показываем AI-сводку вопросов: похожие запросы объединяются, формулируются в виде понятного вопроса и привязываются к реальным сообщениям.'
            : 'Shows the AI question overview: similar asks are grouped, rewritten as a clear question, and tied back to real source messages.'}
        </p>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-2.5">
          {questionBriefs.map((brief) => (
            <Link
              key={brief.id}
              to={(() => {
                const params = new URLSearchParams();
                params.set('topic', brief.sourceTopic || brief.topic);
                params.set('view', 'questions');
                if (brief.sampleEvidenceId) params.set('evidenceId', brief.sampleEvidenceId);
                return `/topics?${params.toString()}`;
              })()}
              className={`rounded-xl border p-3 transition-colors hover:shadow-sm ${statusClass(brief.status)}`}
            >
              <div className="flex items-start justify-between gap-2">
                <div className="min-w-0">
                  <div className="text-[11px] opacity-80">{brief.category}</div>
                  <div className="text-sm leading-snug" style={{ fontWeight: 700 }}>{brief.question}</div>
                </div>
                <span className="text-[10px] px-1.5 py-0.5 rounded border border-current/20" style={{ fontWeight: 600 }}>
                  {confidenceLabel(brief.confidence)} {ru ? 'уверенность' : 'confidence'}
                </span>
              </div>

              <p className="text-xs leading-relaxed mt-1.5 opacity-90">{brief.summary}</p>

              <div className="text-[11px] mt-2 opacity-85">
                {brief.demandSignals.messages.toLocaleString()} {ru ? 'сигналов ·' : 'signals ·'} {brief.demandSignals.uniqueUsers.toLocaleString()} {ru ? 'людей ·' : 'people ·'} {brief.demandSignals.channels.toLocaleString()} {ru ? 'каналов' : 'channels'}
              </div>
              <div className="text-[11px] mt-0.5 opacity-85">
                {ru ? '7д тренд' : '7d trend'}: {brief.demandSignals.trend7dPct > 0 ? '+' : ''}{brief.demandSignals.trend7dPct}% · {statusLabel(brief.status)}
              </div>
            </Link>
          ))}
        </div>

        <div className="mt-3 pt-3 border-t border-gray-100 flex items-center gap-3">
          <div className="flex items-center gap-1.5">
            <div className="w-3 h-3 rounded bg-amber-50 border border-amber-200" />
            <span className="text-xs text-gray-500">{ru ? 'Нужен чёткий ответ/гайд' : 'Needs clear response/guide'}</span>
          </div>
          <div className="flex items-center gap-1.5">
            <div className="w-3 h-3 rounded bg-blue-50 border border-blue-200" />
            <span className="text-xs text-gray-500">{ru ? 'Частично покрыто' : 'Partially covered'}</span>
          </div>
          <div className="flex items-center gap-1.5">
            <div className="w-3 h-3 rounded bg-emerald-50 border border-emerald-200" />
            <span className="text-xs text-gray-500">{ru ? 'В целом покрыто' : 'Mostly covered'}</span>
          </div>
          <span className="text-xs text-amber-600 ml-auto" style={{ fontWeight: 500 }}>
            {needsGuideCount} {ru ? 'карточек требуют гайд' : 'cards need guides'}
          </span>
        </div>
        <p className="text-xs text-gray-400 mt-2">
          {ru
            ? `AI-сводка: ${totalSignals.toLocaleString()} сигналов в выборке. Показаны карточки со средней/высокой уверенностью (средняя уверенность ${avgConfidence}%).`
            : `AI overview: ${totalSignals.toLocaleString()} signals in sample. Showing medium/high-confidence cards (avg confidence ${avgConfidence}%).`}
        </p>
      </div>
  );
}


// ============================================================
// QUESTION-TO-ANSWER GAP BY TOPIC
// ============================================================

function getGapColor(rate: number) {
  if (rate < 35) return '#ef4444';
  if (rate < 56) return '#f59e0b';
  return '#10b981';
}

export function QuestionAnswerGap() {
  const { lang } = useLanguage();
  const { data } = useData();
  const ru = lang === 'ru';
  const qaGapData = data.qaGap[lang] ?? [];

  if (!qaGapData.length) return <EmptyWidget title={ru ? 'Разрыв "Вопрос - Ответ"' : 'Question-Answer Gap'} />;

  // Compute critical gaps and worst topic dynamically
  const criticalGaps = qaGapData.filter(item => !item.lowEvidence && item.rate < 35);
  const lowEvidenceItems = qaGapData.filter(item => item.lowEvidence);
  const sorted = [...qaGapData].sort((a, b) => {
    if (!!a.lowEvidence !== !!b.lowEvidence) return a.lowEvidence ? 1 : -1;
    return a.rate - b.rate;
  });
  const worst = sorted.find((item) => !item.lowEvidence);

  return (
    <div className="bg-white rounded-xl border border-gray-200 p-6">
      <div className="mb-1 flex flex-col items-start gap-2 sm:flex-row sm:items-start sm:justify-between">
        <h3 className="text-gray-900" style={{ fontSize: '1.05rem' }}>
          {ru ? 'Разрыв "Вопрос - Ответ"' : 'Question-Answer Gap'}
        </h3>
        <span className="text-xs text-red-600" style={{ fontWeight: 500 }}>
          {criticalGaps.length} {ru ? 'критических разрывов' : 'critical gaps'}
        </span>
      </div>
      <p className="text-xs text-gray-500 mb-4">
        {ru
          ? 'Покрытие ответами считаем только там, где хватает наблюдений. Иначе показываем как «мало данных».'
          : 'Answer coverage is scored only when evidence is sufficient; otherwise marked as low evidence.'}
      </p>

      <div className="space-y-2.5">
        {sorted.map((item) => (
          <div key={item.topic} className="flex items-center gap-3">
            <span className="text-xs text-gray-700 w-36 flex-shrink-0" style={{ fontWeight: 500 }}>{item.topic}</span>
            <div className="flex-1 relative bg-gray-100 rounded-full h-5">
              <div
                className="h-5 rounded-full flex items-center px-2"
                style={{ width: `${Math.max(10, item.lowEvidence ? 10 : item.rate)}%`, backgroundColor: item.lowEvidence ? '#cbd5e1' : getGapColor(item.rate) }}
              >
                {!item.lowEvidence && item.rate > 18 && (
                  <span className="text-xs text-white" style={{ fontWeight: 600, fontSize: '10px' }}>{item.rate}%</span>
                )}
                {item.lowEvidence && (
                  <span className="text-xs text-slate-700" style={{ fontWeight: 600, fontSize: '10px' }}>{ru ? 'н/д' : 'n/a'}</span>
                )}
              </div>
            </div>
            <span className="text-gray-500 w-24 text-right flex-shrink-0" style={{ fontSize: '10px' }}>
              {item.asked.toLocaleString()} {ru ? 'вопросов' : 'asked'}
            </span>
          </div>
        ))}
      </div>

      <div className="flex items-center gap-4 mt-3 pt-3 border-t border-gray-100">
        {[
          { color: '#ef4444', label: ru ? '<35% отвечено' : '<35% answered' },
          { color: '#f59e0b', label: ru ? '35-55%' : '35-55%' },
          { color: '#10b981', label: ru ? '>55% покрыто' : '>55% well served' },
        ].map((item) => (
          <div key={item.label} className="flex items-center gap-1.5">
            <div className="w-3 h-3 rounded" style={{ backgroundColor: item.color }} />
            <span className="text-xs text-gray-500">{item.label}</span>
          </div>
        ))}
      </div>

      {worst && (
        <div className="mt-3 bg-amber-50 border border-amber-100 rounded-lg px-3 py-2">
          <p className="text-xs text-amber-800">
            {ru
              ? <><span style={{ fontWeight: 600 }}>Приоритет:</span> {worst.topic} {'\u2014'} ответ только в {worst.rate}% случаев при {worst.asked.toLocaleString()}+ еженедельных запросах.</>
              : <><span style={{ fontWeight: 600 }}>Priority:</span> {worst.topic} questions are answered only {worst.rate}% of the time despite {worst.asked.toLocaleString()}+ weekly asks.</>
            }
          </p>
        </div>
      )}
      {lowEvidenceItems.length > 0 && (
        <p className="text-xs text-gray-400 mt-2">
          {ru
            ? `${lowEvidenceItems.length} тем отмечены как «мало данных» и не участвуют в приоритизации.`
            : `${lowEvidenceItems.length} topics are marked low evidence and excluded from prioritization.`}
        </p>
      )}
    </div>
  );
}


// ============================================================
// TOPIC LIFECYCLE
// ============================================================

export function TopicLifecycle() {
  const { lang } = useLanguage();
  const { data, displayRange } = useData();
  const ru = lang === 'ru';
  const lifecycleStages = data.lifecycleStages[lang] ?? [];
  const [expandedKey, setExpandedKey] = useState('');
  const rangeDays = displayRange?.days ?? 0;

  if (!lifecycleStages.length) return <EmptyWidget widgetId="topic_lifecycle" title={ru ? 'Жизненный цикл тем' : 'Topic Lifecycle'} />;

  return (
    <div className="bg-white rounded-xl border border-gray-200 p-6">
      <div className="mb-1 flex flex-col items-start gap-2 sm:flex-row sm:items-start sm:justify-between">
        <WidgetTitle widgetId="topic_lifecycle">
          {ru ? 'Жизненный цикл тем' : 'Topic Lifecycle'}
        </WidgetTitle>
        <span className="w-full text-left text-xs text-gray-500 sm:w-auto sm:text-right">
          {ru ? 'Рост -> Снижение' : 'Growing -> Declining'}
        </span>
      </div>
      <p className="text-xs text-gray-500 mb-4">
        {ru
          ? `Показывает, где внимание к теме растёт, а где снижается, на основе прямых сигналов сообщений в выбранном окне (${rangeDays} дн.).`
          : `Shows where attention is growing and where it is declining, based on direct message signals in the selected ${rangeDays}-day window.`}
      </p>
      <p className="text-xs text-gray-400 mb-4">
        {ru
          ? `X/7д — объём обсуждений за последние 7 дней; Δ — изменение к предыдущим 7 дням; д. — сколько дней тема была активна в текущем окне (${rangeDays} дн.).`
          : `X/7d = discussion volume in the last 7 days; Δ = change vs previous 7 days; d = how many days the topic was active in the current ${rangeDays}-day window.`}
      </p>

      <div className="grid grid-cols-2 gap-1 mb-4">
        {lifecycleStages.map((s) => (
          <div key={s.stage} className={`${s.bgColor} border ${s.borderColor} rounded-lg px-2 py-1.5 text-center`}>
            <div className="w-2 h-2 rounded-full mx-auto mb-1" style={{ backgroundColor: s.color }} />
            <span className={`text-xs block ${s.textColor}`} style={{ fontWeight: 600 }}>{s.stage}</span>
            <span className="text-gray-400" style={{ fontSize: '9px' }}>{s.desc}</span>
          </div>
        ))}
      </div>

      <div className="space-y-3">
        {lifecycleStages.map((stageGroup) => (
          <div key={stageGroup.stage}>
            <div className="flex items-center gap-2 mb-1.5">
              <div className="w-2 h-2 rounded-full flex-shrink-0" style={{ backgroundColor: stageGroup.color }} />
              <span className="text-xs text-gray-900" style={{ fontWeight: 600 }}>{stageGroup.stage}</span>
              <span className={`text-xs ${stageGroup.textColor}`} style={{ fontSize: '10px' }}>({stageGroup.topics.length})</span>
            </div>
            <div className="space-y-1.5 pl-4">
              {stageGroup.topics.map((topic) => (
                <div key={`${stageGroup.stage}-${topic.sourceTopic || topic.name}`}>
                  <button
                    type="button"
                    onClick={() => {
                      const key = `${stageGroup.stage}-${topic.sourceTopic || topic.name}`;
                      setExpandedKey((prev) => (prev === key ? '' : key));
                    }}
                    className={`w-full flex items-center justify-between px-3 py-2 rounded-lg ${stageGroup.bgColor} border ${stageGroup.borderColor} hover:brightness-[0.99] transition-colors`}
                  >
                      <span className="text-xs text-gray-900 text-left" style={{ fontWeight: 500 }}>{topic.name}</span>
                    <div className="flex items-center gap-3 text-xs">
                      <span className="text-gray-400">{topic.volume.toLocaleString()} {ru ? '/7д' : '/7d'}</span>
                      <span className={topic.momentum > 0 ? 'text-emerald-600' : (topic.momentum < 0 ? 'text-red-500' : 'text-gray-500')} style={{ fontWeight: 600 }}>
                        Δ {topic.momentum > 0 ? '+' : ''}{topic.momentum}
                      </span>
                      <span className="text-gray-400 w-8 text-right">{topic.daysActive}{ru ? 'д.' : 'd'}</span>
                      <span className="text-gray-400 w-4 text-right" style={{ fontWeight: 600 }}>
                        {expandedKey === `${stageGroup.stage}-${topic.sourceTopic || topic.name}` ? '−' : '+'}
                      </span>
                    </div>
                  </button>

                  {expandedKey === `${stageGroup.stage}-${topic.sourceTopic || topic.name}` && (
                    <div className="mt-1.5 ml-2 rounded-lg border border-gray-200 bg-white px-3 py-2">
                      <p className="text-xs text-gray-600 leading-relaxed">
                        {ru
                          ? `За последние 7 дней тема получила ${topic.volume.toLocaleString()} упоминаний. По сравнению с предыдущими 7 днями изменение составило ${topic.momentum > 0 ? '+' : ''}${topic.momentum} упоминаний.`
                          : `In the last 7 days this topic had ${topic.volume.toLocaleString()} mentions. Compared with the previous 7 days, the change is ${topic.momentum > 0 ? '+' : ''}${topic.momentum} mentions.`}
                      </p>

                      {topic.topChannels && topic.topChannels.length > 0 && (
                        <div className="mt-2 flex flex-wrap gap-1.5">
                          <span className="text-[11px] text-gray-500">{ru ? 'Каналы:' : 'Channels:'}</span>
                          {topic.topChannels.slice(0, 3).map((ch) => (
                            <span key={`${topic.name}-${ch}`} className="text-[11px] px-2 py-0.5 rounded-full bg-gray-100 text-gray-600">
                              {ch}
                            </span>
                          ))}
                        </div>
                      )}

                      {topic.evidence && topic.evidence.length > 0 ? (
                        <div className="mt-2 space-y-1.5">
                          {topic.evidence.slice(0, 2).map((ev, idx) => (
                            <div key={`${topic.name}-ev-${idx}`} className="text-[11px] text-gray-600 rounded border border-gray-100 bg-gray-50 px-2 py-1.5">
                              <div className="text-gray-700" style={{ fontWeight: 500 }}>{ev.channel || (ru ? 'канал' : 'channel')}</div>
                              <div className="mt-0.5 leading-relaxed">{ev.text}</div>
                              {ev.timestamp && <div className="mt-0.5 text-gray-400">{ev.timestamp.slice(0, 16).replace('T', ' ')}</div>}
                            </div>
                          ))}
                        </div>
                      ) : (
                        <p className="mt-2 text-[11px] text-gray-400">
                          {ru ? 'Пока нет сохранённых примеров сообщений для этой темы.' : 'No saved message evidence for this topic yet.'}
                        </p>
                      )}

                      <div className="mt-2">
                        <Link
                          to={`/topics?topic=${encodeURIComponent(topic.sourceTopic || topic.name)}&view=evidence`}
                          className="text-xs text-blue-600 hover:text-blue-800 hover:underline"
                          style={{ fontWeight: 500 }}
                        >
                          {ru ? 'Открыть полные доказательства →' : 'Open full evidence →'}
                        </Link>
                      </div>
                    </div>
                  )}
                </div>
              ))}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
