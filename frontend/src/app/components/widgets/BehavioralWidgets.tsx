import { ResponsiveContainer, AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip } from 'recharts';
import { Link } from 'react-router';
import { AlertTriangle } from 'lucide-react';
import { useLanguage } from '../../contexts/LanguageContext';
import { useData } from '../../contexts/DataContext';
import { EmptyWidget } from '../ui/EmptyWidget';

// ============================================================
// W8: PROBLEM TRACKER
// ============================================================

const severityColors: Record<string, { bg: string; text: string; border: string }> = {
  high: { bg: 'bg-red-50', text: 'text-red-700', border: 'border-red-200' },
  medium: { bg: 'bg-amber-50', text: 'text-amber-700', border: 'border-amber-200' },
  low: { bg: 'bg-gray-50', text: 'text-gray-600', border: 'border-gray-200' },
};

export function ProblemTracker() {
  const { lang } = useLanguage();
  const { data } = useData();
  const ru = lang === 'ru';
  const problems = data.problems[lang] ?? [];
  const evidenceTotal = problems.reduce((sum, cat) => sum + cat.problems.reduce((inner, p) => inner + (p.evidenceCount || 0), 0), 0);

  if (!problems.length) return <EmptyWidget title={ru ? 'Трекер проблем' : 'Problem Tracker'} />;

  return (
    <div className="bg-white rounded-xl border border-gray-200 p-6">
      <div className="flex items-center justify-between mb-1">
        <h3 className="text-gray-900" style={{ fontSize: '1.05rem' }}>
          {ru ? 'Трекер проблем' : 'Problem Tracker'}
        </h3>
        <span className="text-xs text-gray-500">
          {ru ? 'Болевые точки из разговоров' : 'Pain points from community chatter'}
        </span>
      </div>
      <p className="text-xs text-gray-500 mb-4">
        {ru
          ? 'Показывает темы с негативными сигналами. Цитаты берутся из реальных сообщений, если они есть.'
          : 'Shows topics with negative signals. Quotes come from real messages when available.'}
      </p>

      <div className="space-y-4">
        {problems.map((cat) => (
          <div key={cat.category}>
            <span className="text-xs text-gray-900 block mb-2" style={{ fontWeight: 600 }}>{cat.category}</span>
            <div className="space-y-2">
              {cat.problems.map((p) => {
                const sev = severityColors[p.severity] ?? severityColors.low;
                return (
                  <div key={p.name} className={`${sev.bg} ${sev.border} border rounded-lg p-2.5`}>
                    <div className="flex items-center justify-between mb-1">
                      <Link
                        to={`/topics?topic=${encodeURIComponent(p.name)}`}
                        className="text-xs text-blue-700 hover:underline"
                        style={{ fontWeight: 500 }}
                      >
                        {p.name}
                      </Link>
                      <div className="flex items-center gap-2">
                        <span className="text-xs text-gray-500">{p.mentions.toLocaleString()}</span>
                        <span className={`text-xs ${p.trendReliable ? (p.trend > 0 ? 'text-red-500' : 'text-emerald-500') : 'text-gray-400'}`} style={{ fontWeight: 600 }}>
                          {p.trendReliable ? `${p.trend > 0 ? '+' : ''}${p.trend}%` : (ru ? 'н/д' : 'n/a')}
                        </span>
                      </div>
                    </div>
                    {p.quote ? (
                      <p className="text-xs text-gray-500 italic">&ldquo;{p.quote}&rdquo;</p>
                    ) : (
                      <p className="text-xs text-gray-400 italic">
                        {ru
                          ? 'Сигнал подтверждён метриками, но прямой цитаты за последние 30 дней не найдено.'
                          : 'Signal is metric-backed, but no direct quote was found in the last 30 days.'}
                      </p>
                    )}
                    <p className="text-xs text-gray-400 mt-1">
                      {ru ? `Основа: ${p.evidenceCount || 0} наблюдений за 14 дней` : `Evidence: ${p.evidenceCount || 0} observations over 14 days`}
                    </p>
                  </div>
                );
              })}
            </div>
          </div>
        ))}
      </div>
      <p className="text-xs text-gray-400 mt-3 pt-3 border-t border-gray-100">
        {ru ? `Итого доказательная база: ${evidenceTotal.toLocaleString()} наблюдений.` : `Total evidence base: ${evidenceTotal.toLocaleString()} observations.`}
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

export function ServiceGapDetector() {
  const { lang } = useLanguage();
  const { data } = useData();
  const ru = lang === 'ru';
  const serviceGaps = data.serviceGaps[lang] ?? [];

  if (!serviceGaps.length) return <EmptyWidget title={ru ? 'Детектор пробелов в услугах' : 'Service Gap Detector'} />;

  // Compute top opportunity dynamically
  const topOpp = [...serviceGaps]
    .filter((s) => s.growthReliable)
    .sort((a, b) => b.growth - a.growth)[0];

  return (
    <div className="bg-white rounded-xl border border-gray-200 p-6">
      <div className="flex items-center justify-between mb-1">
        <h3 className="text-gray-900" style={{ fontSize: '1.05rem' }}>
          {ru ? 'Детектор пробелов в услугах' : 'Service Gap Detector'}
        </h3>
        <span className="text-xs text-emerald-600" style={{ fontWeight: 500 }}>
          {serviceGaps.filter(s => s.gap >= 80).length} {ru ? 'критических пробелов' : 'critical gaps'}
        </span>
      </div>
      <p className="text-xs text-gray-500 mb-4">
        {ru
          ? 'Люди ищут это, но не могут найти - каждый пробел это бизнес-возможность'
          : 'People are asking for these but can\'t find them - each gap is a business opportunity'}
      </p>

      <div className="space-y-2.5">
        {serviceGaps.map((item) => (
          <div key={item.service} className="flex items-center gap-3">
            <div className="flex-1 min-w-0">
              <div className="flex items-center gap-2 mb-1">
                <span className="text-xs text-gray-900 truncate" style={{ fontWeight: 500 }}>{item.service}</span>
                <span className={`text-xs px-1.5 py-0.5 rounded flex-shrink-0 ${supplyLevelColors[item.supplyLevel]}`}>{item.supply}</span>
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
                {item.growthReliable ? `${item.growth > 0 ? '+' : ''}${item.growth}%` : (ru ? 'н/д' : 'n/a')}
              </span>
            </div>
          </div>
        ))}
      </div>

      {topOpp && (
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
      <p className="text-xs text-gray-400 mt-2">
        {ru
          ? `${serviceGaps.filter((s) => !s.growthReliable).length} пунктов с недостаточными данными не участвуют в ранжировании роста.`
          : `${serviceGaps.filter((s) => !s.growthReliable).length} low-evidence items are excluded from growth ranking.`}
      </p>
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
