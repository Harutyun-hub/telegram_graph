import { useState } from 'react';
import { ResponsiveContainer, AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip } from 'recharts';
import { Link } from 'react-router';
import { AlertTriangle } from 'lucide-react';
import { useLanguage } from '../../contexts/LanguageContext';
import { useData } from '../../contexts/DataContext';
import { useDashboardDateRange } from '../../contexts/DashboardDateRangeContext';
import { EmptyWidget } from '../ui/EmptyWidget';

// ============================================================
// W8: PROBLEM TRACKER
// ============================================================

const severityColors: Record<string, { bg: string; text: string; border: string }> = {
  critical: { bg: 'bg-red-100', text: 'text-red-800', border: 'border-red-300' },
  high: { bg: 'bg-red-50', text: 'text-red-700', border: 'border-red-200' },
  medium: { bg: 'bg-amber-50', text: 'text-amber-700', border: 'border-amber-200' },
  low: { bg: 'bg-gray-50', text: 'text-gray-600', border: 'border-gray-200' },
};

export function ProblemTracker() {
  const { lang } = useLanguage();
  const { data } = useData();
  const { range } = useDashboardDateRange();
  const ru = lang === 'ru';
  const [expandedEvidenceKey, setExpandedEvidenceKey] = useState('');
  const [showAllProblems, setShowAllProblems] = useState(false);
  const problemGroups = data.problems?.[lang] ?? [];
  const problemRows = problemGroups.flatMap((group) =>
    (group.problems || []).map((problem) => ({
      ...problem,
      category: group.category,
      topic: problem.name,
      sourceTopic: problem.sourceTopic || problem.name,
    })),
  );

  if (!problemRows.length) return <EmptyWidget title={ru ? 'Трекер проблем' : 'Problem Tracker'} />;

  const topProblem = [...problemRows].sort((a, b) => b.mentions - a.mentions)[0];
  const lowEvidenceCount = problemRows.filter((problem) => !problem.trendReliable).length;
  const visibleProblems = showAllProblems ? problemRows : problemRows.slice(0, 5);

  return (
    <div className="bg-white rounded-xl border border-gray-200 p-6">
      <div className="flex items-center justify-between mb-1">
        <h3 className="text-gray-900" style={{ fontSize: '1.05rem' }}>
          {ru ? 'Трекер проблем' : 'Problem Tracker'}
        </h3>
        <span className="text-xs text-gray-500">
          {ru ? `Выбранное окно · ${range.days} дн.` : `Selected window · ${range.days}d`}
        </span>
      </div>
      <p className="text-xs text-gray-500 mb-4">
        {ru
          ? 'Показывает темы с негативными, срочными и стрессовыми сигналами в выбранном диапазоне. Динамика сравнивает текущее окно с предыдущим окном той же длины.'
          : 'Shows topics with negative, urgent, and stress signals in the selected range. Trend compares the current window with the previous window of the same length.'}
      </p>

      {problemRows.length > 0 && (
        <div className="mb-4 space-y-2.5">
          <p className="text-xs text-gray-500">
            {ru
              ? 'Живые проблемные темы сгруппированы по категориям и привязаны к реальным сигналам из текущего окна.'
              : 'Live problem themes are grouped by category and grounded in real signals from the current window.'}
          </p>
          {visibleProblems.map((problem) => {
            const sev = severityColors[problem.severity] ?? severityColors.medium;
            const key = `problem-${problem.category}-${problem.topic}`;
            const expanded = expandedEvidenceKey === key;
            return (
              <div key={key} className={`${sev.bg} ${sev.border} border rounded-lg p-3`}>
                <div className="flex items-start justify-between gap-3">
                  <div className="min-w-0">
                    <div className="text-[11px] opacity-80">{problem.category}</div>
                    <div className="text-sm leading-snug" style={{ fontWeight: 700 }}>{problem.topic}</div>
                    <p className="text-xs mt-1 opacity-90">
                      {problem.quote || (ru ? 'Цитата появится, когда в источниках будет найден релевантный пример.' : 'A quote will appear once a relevant source example is available.')}
                    </p>
                  </div>
                  <span className="text-[10px] px-1.5 py-0.5 rounded border border-current/20" style={{ fontWeight: 600 }}>
                    {problem.mentions.toLocaleString()} {ru ? 'людей' : 'people'}
                  </span>
                </div>

                <div className="text-[11px] mt-2 opacity-85">
                  {problem.evidenceCount?.toLocaleString() || 0} {ru ? 'сигналов ·' : 'signals ·'} {problem.mentions.toLocaleString()} {ru ? 'затронутых людей' : 'affected people'}
                </div>
                <div className="text-[11px] mt-0.5 opacity-85">
                  {problem.trendReliable
                    ? `${ru ? 'Динамика' : 'Trend'}: ${problem.trend > 0 ? '+' : ''}${problem.trend}%`
                    : (ru ? 'Динамика скрыта: недостаточно данных.' : 'Trend hidden: low evidence.')}
                </div>

                <button
                  type="button"
                  onClick={() => setExpandedEvidenceKey(expanded ? '' : key)}
                  className="mt-2 text-xs text-blue-700 hover:underline"
                >
                  {expanded
                    ? (ru ? 'Скрыть детали' : 'Hide details')
                    : (ru ? 'Показать детали' : 'Show details')}
                </button>

                {expanded && (
                  <div className="mt-2 space-y-2">
                    <div className="bg-white/80 rounded border border-gray-200 p-2">
                      <p className="text-xs text-gray-700 leading-relaxed">
                        {problem.quote
                          ? <>&ldquo;{problem.quote}&rdquo;</>
                          : (ru ? 'Для этой темы пока нет сохранённой цитаты, но сигналы уже зафиксированы в данных текущего окна.' : 'No saved quote is available for this topic yet, but the signals are already present in the selected-window data.')}
                      </p>
                      <div className="mt-1 flex items-center justify-between gap-2">
                        <span className="text-[11px] text-gray-500">
                          {problem.trendReliable
                            ? (ru ? 'Сравнение с предыдущим окном' : 'Compared with previous window')
                            : (ru ? 'Недостаточно выборки для тренда' : 'Insufficient sample for trend')}
                        </span>
                        <Link
                          to={`/topics?topic=${encodeURIComponent(problem.sourceTopic || problem.topic)}&view=evidence`}
                          className="text-[11px] text-blue-700 hover:underline"
                        >
                          {ru ? 'Открыть →' : 'Open →'}
                        </Link>
                      </div>
                    </div>
                  </div>
                )}
              </div>
            );
          })}

          {problemRows.length > 5 && (
            <button
              type="button"
              onClick={() => {
                setShowAllProblems((value) => !value);
                if (showAllProblems) setExpandedEvidenceKey('');
              }}
              className="text-xs text-blue-700 hover:underline"
              style={{ fontWeight: 600 }}
            >
              {showAllProblems
                ? (ru ? 'Показать меньше' : 'Show less')
                : (ru ? `Показать все (${problemRows.length})` : `See all (${problemRows.length})`)}
            </button>
          )}
        </div>
      )}

      {topProblem && (
        <div className="mt-3 pt-3 border-t border-gray-100">
          <div className="bg-red-50 border border-red-100 rounded-lg px-3 py-2">
            <p className="text-xs text-red-800">
              {ru
                ? <><span style={{ fontWeight: 600 }}>Главная точка напряжения:</span> {topProblem.topic} затронула {topProblem.mentions.toLocaleString()} человек{topProblem.mentions === 1 ? 'а' : ''} в текущем окне.</>
                : <><span style={{ fontWeight: 600 }}>Primary friction:</span> {topProblem.topic} affected {topProblem.mentions.toLocaleString()} people in the current window.</>}
            </p>
          </div>
        </div>
      )}

      <p className="text-xs text-gray-400 mt-2">
        {ru
          ? `${lowEvidenceCount.toLocaleString()} тем с малой выборкой не участвуют в сравнении динамики.`
          : `${lowEvidenceCount.toLocaleString()} low-evidence topics are excluded from trend comparison.`}
      </p>
    </div>
  );
}


// ============================================================
// W9: SERVICE GAP DETECTOR
// ============================================================

// ✅ FIX: use supplyLevel semantic field instead of brittle localized string matching.
// Previously: compared supply label against translated strings (same pattern as old VitalityScorecard bug).
const supplyLevelColors: Record<string, string> = {
  none: 'bg-red-100 text-red-700',
  very_low: 'bg-orange-100 text-orange-700',
  low: 'bg-amber-100 text-amber-700',
  moderate: 'bg-gray-100 text-gray-600',
  adequate: 'bg-emerald-100 text-emerald-600',
};

type ServiceGapBarRow = {
  id: string;
  topic: string;
  sourceTopic?: string;
  service: string;
  demand: number;
  supply: string;
  gap: number;
  growth: number;
  growthReliable: boolean;
  evidenceCount: number;
  supplyLevel: 'none' | 'very_low' | 'low' | 'moderate' | 'adequate';
  evidence: Array<{ id: string; quote: string; channel: string; timestamp: string; kind: string }>;
};

function supplyLevelFromGap(gap: number): 'none' | 'very_low' | 'low' | 'moderate' | 'adequate' {
  if (gap >= 85) return 'none';
  if (gap >= 70) return 'very_low';
  if (gap >= 55) return 'low';
  if (gap >= 35) return 'moderate';
  return 'adequate';
}

function supplyLabel(ru: boolean, level: 'none' | 'very_low' | 'low' | 'moderate' | 'adequate'): string {
  if (ru) {
    if (level === 'none') return 'Нет';
    if (level === 'very_low') return 'Очень низко';
    if (level === 'low') return 'Низко';
    if (level === 'moderate') return 'Средне';
    return 'Достаточно';
  }
  if (level === 'none') return 'None';
  if (level === 'very_low') return 'Very Low';
  if (level === 'low') return 'Low';
  if (level === 'moderate') return 'Moderate';
  return 'Adequate';
}

export function ServiceGapDetector() {
  const { lang } = useLanguage();
  const { data } = useData();
  const ru = lang === 'ru';
  const serviceGapBriefs = data.serviceGapBriefs?.[lang] ?? [];

  const aiRows: ServiceGapBarRow[] = serviceGapBriefs.map((brief: any) => {
    const gap = Math.max(0, Math.min(100, Math.round(Number(brief.unmetPct ?? 0))));
    const level = supplyLevelFromGap(gap);
    const messages = Math.max(0, Number(brief?.demandSignals?.messages ?? 0));
    const trend = Math.max(-100, Math.min(100, Math.round(Number(brief?.demandSignals?.trend7dPct ?? 0))));
    return {
      id: String(brief.id || brief.topic || Math.random()),
      topic: String(brief.topic || brief.serviceNeed || 'Service need'),
      sourceTopic: String(brief.sourceTopic || brief.topic || ''),
      service: String(brief.serviceNeed || brief.topic || 'Service need'),
      demand: messages,
      supply: supplyLabel(ru, level),
      gap,
      growth: trend,
      growthReliable: messages >= 8,
      evidenceCount: messages,
      supplyLevel: level,
      evidence: Array.isArray(brief.evidence) ? brief.evidence : [],
    };
  });
  const hasRenderableRows = aiRows.length > 0;
  const topOpp = [...aiRows]
    .filter((s) => s.growthReliable)
    .sort((a, b) => b.growth - a.growth)[0];

  return (
    <div className="bg-white rounded-xl border border-gray-200 p-6">
      <div className="flex items-center justify-between mb-1">
        <h3 className="text-gray-900" style={{ fontSize: '1.05rem' }}>
          {ru ? 'Детектор пробелов в услугах' : 'Service Gap Detector'}
        </h3>
        {hasRenderableRows && (
          <span className="text-xs text-emerald-600" style={{ fontWeight: 500 }}>
            {aiRows.filter((s) => s.gap >= 80).length} {ru ? 'критических пробелов' : 'critical gaps'}
          </span>
        )}
      </div>
      <p className="text-xs text-gray-500 mb-4">
        {ru
          ? 'Показываем только AI-сформулированные сервисные пробелы, подтверждённые реальными сообщениями. Если надёжных карточек нет, блок остаётся пустым.'
          : 'Shows only AI-generated service gaps grounded in real messages. If there are no reliable cards, this widget stays empty.'}
      </p>

      {!hasRenderableRows && (
        <p className="text-xs text-slate-600 bg-slate-50 border border-slate-200 rounded-lg px-3 py-2 mb-4">
          {ru
            ? 'AI пока не обнаружил надёжных сервисных пробелов.'
            : 'No AI-detected service gaps yet.'}
        </p>
      )}

      {hasRenderableRows && (
        <div className="space-y-2.5">
          {aiRows.map((item) => {
            return (
              <div key={item.id} className="space-y-1.5">
                <div className="flex items-center gap-3">
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center justify-between gap-2 mb-1">
                      <div className="flex items-center gap-2 min-w-0">
                        <span className="text-xs text-gray-900 truncate" style={{ fontWeight: 500 }}>{item.service}</span>
                        <span className={`text-xs px-1.5 py-0.5 rounded flex-shrink-0 ${supplyLevelColors[item.supplyLevel]}`}>{item.supply}</span>
                      </div>
                      <Link
                        to={`/topics?topic=${encodeURIComponent(item.sourceTopic || item.topic)}`}
                        className="text-[11px] text-blue-700 hover:underline flex-shrink-0"
                      >
                        {ru ? 'Открыть →' : 'Open →'}
                      </Link>
                    </div>
                    <div className="w-full bg-gray-100 rounded-full h-2">
                      <div
                        className="h-2 rounded-full"
                        style={{ width: `${item.gap}%`, backgroundColor: item.gap >= 90 ? '#dc2626' : item.gap >= 70 ? '#f97316' : '#f59e0b' }}
                      />
                    </div>
                  </div>
                  <div className="text-right flex-shrink-0 w-20">
                    <span className="text-xs text-gray-900 block" style={{ fontWeight: 600 }}>{item.demand} {ru ? 'запросов' : 'asks'}</span>
                    <span className={`text-xs ${item.growthReliable ? (item.growth >= 0 ? 'text-emerald-500' : 'text-red-500') : 'text-gray-400'}`}>
                      {item.growthReliable ? `${item.growth > 0 ? '+' : ''}${item.growth}%` : (ru ? 'Мало данных' : 'Low evidence')}
                    </span>
                  </div>
                </div>
                <div className="flex items-center justify-between gap-2 text-[11px] text-gray-500">
                  <span>
                    {item.evidenceCount.toLocaleString()} {ru ? 'сигналов спроса' : 'demand signals'}
                  </span>
                  <span>
                    {ru ? 'Пробел' : 'Gap'}: {item.gap}%
                  </span>
                </div>
              </div>
            );
          })}
        </div>
      )}

      {hasRenderableRows && topOpp && (
        <div className="mt-3 pt-3 border-t border-gray-100">
          <div className="bg-emerald-50 border border-emerald-100 rounded-lg px-3 py-2">
            <p className="text-xs text-emerald-800">
              {ru
                ? <><span style={{ fontWeight: 600 }}>Главная возможность:</span> {topOpp.service} {'\u2014'} пробел {topOpp.gap}%, рост {topOpp.growth > 0 ? '+' : ''}{topOpp.growth}%, {topOpp.demand} активных запросов.</>
                : <><span style={{ fontWeight: 600 }}>Top opportunity:</span> {topOpp.service} has {topOpp.gap}% gap, {topOpp.growth > 0 ? '+' : ''}{topOpp.growth}% growth, and {topOpp.demand} active seekers.</>
              }
            </p>
          </div>
        </div>
      )}

      {hasRenderableRows && (
        <p className="text-xs text-gray-400 mt-2">
          {ru
            ? `${aiRows.filter((s) => !s.growthReliable).length} пунктов с недостаточными данными не участвуют в ранжировании роста.`
            : `${aiRows.filter((s) => !s.growthReliable).length} low-evidence items are excluded from growth ranking.`}
        </p>
      )}
    </div>
  );
}


// ============================================================
// W10: SATISFACTION BY LIFE AREA
// ============================================================

export function SatisfactionByArea() {
  const { lang } = useLanguage();
  const { data } = useData();
  const ru = lang === 'ru';
  const satisfactionAreas = data.satisfactionAreas[lang] ?? [];

  if (!satisfactionAreas.length) return <EmptyWidget title={ru ? 'Радар удовлетворённости жизнью' : 'Life Satisfaction Radar'} />;

  const sorted = [...satisfactionAreas].sort((a, b) => b.satisfaction - a.satisfaction);
  // Compute top 3 loves and bottom 3 pain points dynamically
  const top3 = sorted.slice(0, 3);
  const bottom3 = sorted.slice(-3).reverse();

  return (
    <div className="bg-white rounded-xl border border-gray-200 p-6">
      <div className="flex items-center justify-between mb-1">
        <h3 className="text-gray-900" style={{ fontSize: '1.05rem' }}>
          {ru ? 'Радар удовлетворённости жизнью' : 'Life Satisfaction Radar'}
        </h3>
        <span className="text-xs text-gray-500">{ru ? 'По сферам жизни' : 'Community satisfaction by area'}</span>
      </div>
      <p className="text-xs text-gray-500 mb-4">
        {ru
          ? 'Что нравится людям и что их расстраивает в жизни в Армении?'
          : 'What do people love vs. what frustrates them about life in Armenia?'}
      </p>

      <div className="space-y-2.5">
        {sorted.map((item) => (
          <div key={item.area} className="flex items-center gap-3">
            <span className="text-base w-6 text-center">{item.emoji}</span>
            <span className="text-xs text-gray-700 w-36" style={{ fontWeight: 500 }}>{item.area}</span>
            <div className="flex-1 bg-gray-100 rounded-full h-3">
              <div
                className="h-3 rounded-full flex items-center justify-end pr-1.5 transition-all"
                style={{ width: `${item.satisfaction}%`, backgroundColor: item.satisfaction >= 70 ? '#10b981' : item.satisfaction >= 45 ? '#f59e0b' : '#ef4444' }}
              >
                {item.satisfaction >= 30 && (
                  <span className="text-xs text-white" style={{ fontWeight: 600, fontSize: '9px' }}>{item.satisfaction}%</span>
                )}
              </div>
            </div>
            <span className={`text-xs w-10 text-right ${item.trend > 0 ? 'text-emerald-500' : 'text-red-500'}`}>
              {item.trend > 0 ? '+' : ''}{item.trend}
            </span>
          </div>
        ))}
      </div>

      <div className="mt-3 pt-3 border-t border-gray-100 grid grid-cols-2 gap-3">
        <div className="bg-emerald-50 border border-emerald-100 rounded-lg px-3 py-2">
          <span className="text-xs text-emerald-900 block" style={{ fontWeight: 600 }}>
            {ru ? 'Сообщество ценит' : 'Community loves'}
          </span>
          <p className="text-xs text-emerald-700">
            {top3.map(a => `${a.area} (${a.satisfaction}%)`).join(', ')}
          </p>
        </div>
        <div className="bg-red-50 border border-red-100 rounded-lg px-3 py-2">
          <span className="text-xs text-red-900 block" style={{ fontWeight: 600 }}>
            {ru ? 'Болевые точки' : 'Pain points'}
          </span>
          <p className="text-xs text-red-700">
            {bottom3.map(a => `${a.area} (${a.satisfaction}%)`).join(', ')}
          </p>
        </div>
      </div>
    </div>
  );
}


// ============================================================
// W11: MOOD OVER TIME
// ============================================================

export function MoodOverTime() {
  const { lang } = useLanguage();
  const { data } = useData();
  const ru = lang === 'ru';
  const moodConfig = data.moodConfig[lang] ?? [];
  const moodData = data.moodData;

  if (!moodData.length || !moodConfig.length) return <EmptyWidget title={ru ? 'Настроения сообщества' : 'Community Mood'} />;

  const latestWeek = moodData[moodData.length - 1];
  const firstWeek = moodData[0];

  // ✅ GENERIC: use moodConfig.polarity to classify keys — no hardcoded field names
  const positiveKeys = moodConfig.filter(m => m.polarity === 'positive').map(m => m.key);
  const negativeKeys = moodConfig.filter(m => m.polarity === 'negative').map(m => m.key);

  const sumByKeys = (week: typeof moodData[0], keys: string[]) =>
    keys.reduce((s, k) => s + ((week as Record<string, number>)[k] ?? 0), 0);

  const totalLatest = moodConfig.reduce((s, m) => s + ((latestWeek as Record<string, number>)[m.key] ?? 0), 0);
  const posLatest = sumByKeys(latestWeek, positiveKeys);
  const positiveShare = totalLatest > 0 ? Math.round((posLatest / totalLatest) * 100) : 0;

  const positiveGrowing = sumByKeys(latestWeek, positiveKeys) > sumByKeys(firstWeek, positiveKeys);
  const negativeDecreasing = sumByKeys(latestWeek, negativeKeys) < sumByKeys(firstWeek, negativeKeys);
  const isPositiveTrend = positiveGrowing && negativeDecreasing;

  return (
    <div className="bg-white rounded-xl border border-gray-200 p-6">
      <div className="flex items-center justify-between mb-1">
        <h3 className="text-gray-900" style={{ fontSize: '1.05rem' }}>
          {ru ? 'Настроения сообщества' : 'Community Mood'}
        </h3>
        <span className="text-xs text-emerald-600" style={{ fontWeight: 500 }}>
          {positiveShare}% {ru ? 'позитивных' : 'positive'}
        </span>
      </div>
      <p className="text-xs text-gray-500 mb-4">
        {ru
          ? 'Эмоциональный градусник сообщества - насколько люди довольны жизнью здесь?'
          : 'Emotional temperature of the community - are people happy here?'}
      </p>

      <ResponsiveContainer width="100%" height={200}>
        <AreaChart data={moodData}>
          <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" />
          <XAxis dataKey="week" tick={{ fontSize: 11 }} stroke="#9ca3af" />
          <YAxis tick={{ fontSize: 11 }} stroke="#9ca3af" />
          <Tooltip />
          {moodConfig.map((m) => (
            <Area key={m.key} type="monotone" dataKey={m.key} stackId="1" stroke={m.color} fill={m.color} fillOpacity={0.7} />
          ))}
        </AreaChart>
      </ResponsiveContainer>

      <div className="flex flex-wrap items-center justify-center gap-x-3 gap-y-1 mt-2">
        {moodConfig.map((m) => (
          <div key={m.key} className="flex items-center gap-1">
            <span style={{ fontSize: '13px' }}>{m.emoji}</span>
            <span className="text-xs text-gray-500">{m.label}</span>
          </div>
        ))}
      </div>

      <div className="mt-3 pt-3 border-t border-gray-100">
        <div className={`${isPositiveTrend ? 'bg-blue-50 border-blue-100' : 'bg-amber-50 border-amber-100'} border rounded-lg px-3 py-2`}>
          <p className={`text-xs ${isPositiveTrend ? 'text-blue-800' : 'text-amber-800'}`}>
            {isPositiveTrend
              ? (ru
                  ? <><span style={{ fontWeight: 600 }}>Позитивный тренд:</span> Позитивные настроения растут, негативные снижаются. Сообщество становится более устоявшимся.</>
                  : <><span style={{ fontWeight: 600 }}>Positive trend:</span> Positive moods growing while negative moods decline. Community is becoming more settled over time.</>
                )
              : (ru
                  ? <><span style={{ fontWeight: 600 }}>Внимание:</span> Настроения сообщества требуют мониторинга. Обратите внимание на причины негативных сигналов.</>
                  : <><span style={{ fontWeight: 600 }}>Watch:</span> Community mood needs attention. Look into the drivers of negative sentiment.</>
                )
            }
          </p>
        </div>
      </div>
    </div>
  );
}


// ============================================================
// EMOTIONAL URGENCY INDEX
// ============================================================

export function EmotionalUrgencyIndex() {
  const { lang } = useLanguage();
  const { data } = useData();
  const ru = lang === 'ru';
  const urgencySignals = data.urgencySignals[lang] ?? [];

  if (!urgencySignals.length) return <EmptyWidget title={ru ? 'Индекс эмоциональной срочности' : 'Emotional Urgency Index'} />;

  const critical = urgencySignals.filter((s) => s.urgency === 'critical');
  const high = urgencySignals.filter((s) => s.urgency === 'high');
  const totalAffected = urgencySignals.reduce((sum, s) => sum + s.count, 0);

  return (
    <div className="bg-white rounded-xl border border-gray-200 p-6">
      <div className="flex items-center justify-between mb-1">
        <h3 className="text-gray-900" style={{ fontSize: '1.05rem' }}>
          {ru ? 'Индекс эмоциональной срочности' : 'Emotional Urgency Index'}
        </h3>
        <div className="flex items-center gap-2">
          <span className="px-2 py-0.5 rounded-full bg-red-100 text-red-700" style={{ fontWeight: 500, fontSize: '10px' }}>
            {critical.length} {ru ? 'критических' : 'critical'}
          </span>
          <span className="px-2 py-0.5 rounded-full bg-orange-100 text-orange-700" style={{ fontWeight: 500, fontSize: '10px' }}>
            {high.length} {ru ? 'высокая срочность' : 'high urgency'}
          </span>
        </div>
      </div>
      <p className="text-xs text-gray-500 mb-4">
        {ru
          ? <>Сигналы, где людям нужна помощь <span style={{ fontWeight: 600 }}>прямо сейчас</span> - не выплеск эмоций, а кризис.</>
          : <>Signals where people need help <span style={{ fontWeight: 600 }}>right now</span> - not venting, but in crisis.</>
        }
      </p>

      <div className="space-y-4">
        {critical.length > 0 && (
          <div>
            <span className="text-xs text-red-700 block mb-2" style={{ fontWeight: 600 }}>
              {ru ? 'Критические - помощь нужна сегодня' : 'Critical - needs help today'}
            </span>
            <div className="space-y-2">
              {critical.map((item) => (
                <div key={item.message} className="bg-red-50 border border-red-200 rounded-lg p-3">
                  <div className="flex items-start gap-2 mb-2">
                    <AlertTriangle className="w-3.5 h-3.5 text-red-500 flex-shrink-0 mt-0.5" />
                    <span className="text-xs text-gray-800 italic">&ldquo;{item.message}&rdquo;</span>
                  </div>
                  <div className="flex items-center gap-3 text-xs flex-wrap">
                    <span className="bg-red-100 text-red-700 px-1.5 py-0.5 rounded" style={{ fontWeight: 500 }}>{item.topic}</span>
                    <span className="text-gray-500">{item.count} {ru ? 'похожих публикаций за неделю' : 'similar posts this week'}</span>
                    <span className="text-red-600 ml-auto" style={{ fontWeight: 500 }}>{'\u2192'} {item.action}</span>
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}

        {high.length > 0 && (
          <div>
            <span className="text-xs text-orange-700 block mb-2" style={{ fontWeight: 600 }}>
              {ru ? 'Высокая срочность - помощь нужна на этой неделе' : 'High urgency - needs help this week'}
            </span>
            <div className="space-y-2">
              {high.map((item) => (
                <div key={item.message} className="bg-orange-50 border border-orange-200 rounded-lg p-3">
                  <div className="flex items-start gap-2 mb-2">
                    <AlertTriangle className="w-3.5 h-3.5 text-orange-500 flex-shrink-0 mt-0.5" />
                    <span className="text-xs text-gray-800 italic">&ldquo;{item.message}&rdquo;</span>
                  </div>
                  <div className="flex items-center gap-3 text-xs flex-wrap">
                    <span className="bg-orange-100 text-orange-700 px-1.5 py-0.5 rounded" style={{ fontWeight: 500 }}>{item.topic}</span>
                    <span className="text-gray-500">{item.count} {ru ? 'похожих публикаций' : 'similar posts'}</span>
                    <span className="text-orange-600 ml-auto" style={{ fontWeight: 500 }}>{'\u2192'} {item.action}</span>
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>

      <div className="mt-4 pt-3 border-t border-gray-100">
        <div className="bg-slate-50 border border-slate-200 rounded-lg px-3 py-2">
          <p className="text-xs text-slate-700">
            {ru
              ? <><span style={{ fontWeight: 600 }}>Почему это важно:</span> {urgencySignals.length} кластеров сигналов затрагивают <span style={{ fontWeight: 600 }}>{totalAffected} человек</span> только за эту неделю.</>
              : <><span style={{ fontWeight: 600 }}>Why this matters:</span> {urgencySignals.length} signal clusters affect <span style={{ fontWeight: 600 }}>{totalAffected} people</span> this week alone.</>
            }
          </p>
        </div>
      </div>
    </div>
  );
}
