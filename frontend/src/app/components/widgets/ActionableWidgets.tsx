import { Home } from 'lucide-react';
import { useLanguage } from '../../contexts/LanguageContext';
import { useData } from '../../contexts/DataContext';
import { EmptyWidget } from '../ui/EmptyWidget';

// ============================================================
// W26: BUSINESS OPPORTUNITY SIGNALS
// ============================================================

const revenueColors: Record<string, string> = {
  '$$$$': '#10b981', '$$$': '#3b82f6', '$$': '#f59e0b', '$': '#6b7280',
};

export function BusinessOpportunityTracker() {
  const { lang } = useLanguage();
  const { data } = useData();
  const ru = lang === 'ru';
  const opportunities = data.businessOpportunities[lang] ?? [];

  if (!opportunities.length) return <EmptyWidget title={ru ? 'Бизнес-возможности от сообщества' : 'Business Opportunity Signals'} />;

  return (
    <div className="bg-white rounded-xl border border-gray-200 p-6">
      <div className="flex items-center justify-between mb-1">
        <h3 className="text-gray-900" style={{ fontSize: '1.05rem' }}>
          {ru ? 'Бизнес-возможности от сообщества' : 'Business Opportunity Signals'}
        </h3>
        <span className="text-xs text-emerald-600" style={{ fontWeight: 500 }}>
          {opportunities.length} {ru ? 'возможностей' : 'opportunities'}
        </span>
      </div>
      <p className="text-xs text-gray-500 mb-4">
        {ru
          ? 'Сообщество само подсказывает, какой бизнес строить — слушайте и действуйте'
          : 'The community is telling you what businesses to build — listen and act'}
      </p>

      <div className="space-y-2.5">
        {opportunities.map((opp) => (
          <div key={opp.need} className="bg-gray-50 rounded-lg p-3 hover:bg-gray-100 transition-colors cursor-pointer">
            <div className="flex items-start justify-between mb-1.5">
              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-2">
                  <span className="text-xs text-gray-900" style={{ fontWeight: 600 }}>{opp.need}</span>
                  <span className="text-xs px-1.5 py-0.5 rounded" style={{
                    backgroundColor: (revenueColors[opp.revenue] ?? '#6b7280') + '20',
                    color: revenueColors[opp.revenue] ?? '#6b7280',
                    fontWeight: 600,
                  }}>{opp.revenue}</span>
                </div>
                <p className="text-xs text-gray-400 italic mt-0.5 truncate">&ldquo;{opp.sampleQuote}&rdquo;</p>
              </div>
            </div>
            <div className="flex items-center gap-4 text-xs">
              <span className="text-gray-500">{opp.mentions.toLocaleString()} {ru ? 'упоминаний' : 'mentions'}</span>
              <span className={`${opp.growth >= 0 ? 'text-emerald-600' : 'text-red-500'}`} style={{ fontWeight: 600 }}>{opp.growth > 0 ? '+' : ''}{opp.growth}%</span>
              <span className="text-gray-400">{opp.sector}</span>
              <span className="text-gray-400 ml-auto text-right truncate" style={{ fontSize: '10px', maxWidth: '100px' }}>{opp.readiness}</span>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}


// ============================================================
// W27: JOB MARKET PULSE
// ============================================================

export function JobMarketPulse() {
  const { lang } = useLanguage();
  const { data } = useData();
  const ru = lang === 'ru';
  const jobSeeking = data.jobSeeking[lang] ?? [];
  const jobTrends = data.jobTrends[lang] ?? [];

  if (!jobSeeking.length) return <EmptyWidget title={ru ? 'Рынок труда и занятость' : 'Job & Work Landscape'} />;

  // ✅ GENERIC: dynamic max divisor + top role computed from data
  const maxJobPct = Math.max(...jobSeeking.map(j => j.pct), 1);
  const topJob = [...jobSeeking].sort((a, b) => b.pct - a.pct)[0];

  return (
    <div className="bg-white rounded-xl border border-gray-200 p-6">
      <div className="flex items-center justify-between mb-1">
        <h3 className="text-gray-900" style={{ fontSize: '1.05rem' }}>
          {ru ? 'Рынок труда и занятость' : 'Job & Work Landscape'}
        </h3>
        <span className="text-xs text-gray-500">{ru ? 'Как работает сообщество' : 'How the community works'}</span>
      </div>
      <p className="text-xs text-gray-500 mb-4">
        {ru
          ? 'Понимание структуры занятости раскрывает стабильность и потребности сообщества'
          : 'Understanding employment patterns reveals community stability and needs'}
      </p>

      <div className="space-y-2 mb-4">
        {jobSeeking.map((job) => (
          <div key={job.role} className="flex items-center gap-3">
            <span className="text-xs text-gray-700 w-48" style={{ fontWeight: 500 }}>{job.role}</span>
            <div className="flex-1 bg-gray-100 rounded-full h-3">
              <div
                className="h-3 rounded-full bg-blue-500"
                style={{ width: `${(job.pct / maxJobPct) * 100}%`, opacity: 0.5 + (job.pct / maxJobPct) * 0.5 }}
              />
            </div>
            <span className="text-xs text-gray-900 w-8 text-right" style={{ fontWeight: 600 }}>{job.pct}%</span>
          </div>
        ))}
      </div>

      <div className="pt-3 border-t border-gray-100">
        <span className="text-xs text-gray-500 block mb-2" style={{ fontWeight: 500 }}>
          {ru ? 'Тренды занятости' : 'Employment trends'}
        </span>
        <div className="space-y-1.5">
          {jobTrends.map((t) => (
            <div key={t.trend} className={`flex items-center gap-2 text-xs px-2 py-1.5 rounded ${
              t.type === 'hot' ? 'bg-emerald-50 text-emerald-700' :
              t.type === 'growing' ? 'bg-blue-50 text-blue-700' :
              t.type === 'concern' ? 'bg-amber-50 text-amber-700' :
              'bg-gray-50 text-gray-600'
            }`}>
              <span className={`w-1.5 h-1.5 rounded-full flex-shrink-0 ${
                t.type === 'hot' ? 'bg-emerald-500' :
                t.type === 'growing' ? 'bg-blue-500' :
                t.type === 'concern' ? 'bg-amber-500' :
                'bg-gray-400'
              }`} />
              {t.trend}
            </div>
          ))}
        </div>
      </div>

      <div className="mt-3 bg-blue-50 border border-blue-100 rounded-lg px-3 py-2">
        <p className="text-xs text-blue-800">
          {topJob && (ru
            ? <><span style={{ fontWeight: 600 }}>Ключевой инсайт:</span> {topJob.pct}% работают в категории «{topJob.role}» ({topJob.count.toLocaleString()} человек). Это якорная группа сообщества — создавайте контент, ориентированный на их потребности.</>
            : <><span style={{ fontWeight: 600 }}>Key insight:</span> {topJob.pct}% are in &quot;{topJob.role}&quot; ({topJob.count.toLocaleString()} people). This is the community&apos;s anchor group — create content tailored to their needs.</>
          )}
        </p>
      </div>
    </div>
  );
}


// ============================================================
// W28: HOUSING MARKET PULSE
// ============================================================

export function HousingMarketPulse() {
  const { lang } = useLanguage();
  const { data } = useData();
  const ru = lang === 'ru';
  const housingData = data.housingData[lang] ?? [];
  const housingHotTopics = data.housingHotTopics[lang] ?? [];

  if (!housingData.length) return <EmptyWidget title={ru ? 'Пульс рынка жилья' : 'Housing Market Pulse'} />;

  // ✅ GENERIC: dynamically compute worst housing type — highest (trend − satisfaction) = most pain
  const worstHousing = [...housingData].sort(
    (a, b) => (b.trend - b.satisfaction) - (a.trend - a.satisfaction)
  )[0];

  // ✅ FIX: compute average trend from data — badge reflects real direction, not hardcoded label
  const avgTrend = housingData.length > 0
    ? housingData.reduce((s, h) => s + h.trend, 0) / housingData.length
    : 0;
  const trendBadge = avgTrend > 2
    ? { label: ru ? 'Цены растут' : 'Prices rising', color: 'text-red-500' }
    : avgTrend < -2
      ? { label: ru ? 'Цены снижаются' : 'Prices falling', color: 'text-emerald-600' }
      : { label: ru ? 'Цены стабильны' : 'Prices stable', color: 'text-amber-500' };

  return (
    <div className="bg-white rounded-xl border border-gray-200 p-6">
      <div className="flex items-center justify-between mb-1">
        <h3 className="text-gray-900" style={{ fontSize: '1.05rem' }}>
          {ru ? 'Пульс рынка жилья' : 'Housing Market Pulse'}
        </h3>
        {/* ✅ FIX: badge direction derived from data, not hardcoded */}
        <span className={`text-xs ${trendBadge.color}`} style={{ fontWeight: 500 }}>
          {trendBadge.label}
        </span>
      </div>
      <p className="text-xs text-gray-500 mb-4">
        {ru
          ? 'Болевая точка №1 — понимайте жилищную дискуссию, чтобы помочь сообществу'
          : 'The #1 pain point — understand the housing conversation to help your community'}
      </p>

      <div className="space-y-2 mb-4">
        {housingData.map((h) => (
          <div key={h.type} className="flex items-center gap-3">
            <Home className="w-3.5 h-3.5 text-gray-400 flex-shrink-0" />
            <span className="text-xs text-gray-700 w-36" style={{ fontWeight: 500 }}>{h.type}</span>
            <span className="text-xs text-gray-900 w-14" style={{ fontWeight: 600 }}>{h.avgPrice}</span>
            <span className={`text-xs w-10 text-right ${h.trend >= 0 ? 'text-red-500' : 'text-emerald-500'}`}>{h.trend > 0 ? '+' : ''}{h.trend}%</span>
            <div className="flex-1 bg-gray-100 rounded-full h-2">
              <div
                className="h-2 rounded-full"
                style={{ width: `${h.satisfaction}%`, backgroundColor: h.satisfaction >= 60 ? '#10b981' : h.satisfaction >= 40 ? '#f59e0b' : '#ef4444' }}
              />
            </div>
            <span className="text-xs text-gray-500 w-12 text-right">
              {h.satisfaction}% {ru ? 'уд.' : 'sat.'}
            </span>
          </div>
        ))}
      </div>

      <div className="pt-3 border-t border-gray-100">
        <span className="text-xs text-gray-500 block mb-2" style={{ fontWeight: 500 }}>
          {ru ? 'Горячие жилищные обсуждения' : 'Hot housing discussions'}
        </span>
        <div className="space-y-1.5">
          {housingHotTopics.map((topic) => (
            <div key={topic.topic} className="flex items-center gap-2 text-xs">
              <span className={`w-1.5 h-1.5 rounded-full flex-shrink-0 ${
                topic.sentiment === 'angry' ? 'bg-red-500' :
                topic.sentiment === 'worried' ? 'bg-amber-500' :
                topic.sentiment === 'seeking' ? 'bg-blue-500' : 'bg-gray-400'
              }`} />
              <span className="text-gray-700 flex-1 truncate">{topic.topic}</span>
              <span className="text-gray-400">{topic.count}x</span>
            </div>
          ))}
        </div>
      </div>

      <div className="mt-3 bg-red-50 border border-red-100 rounded-lg px-3 py-2">
        <p className="text-xs text-red-800">
          {worstHousing && (ru
            ? <><span style={{ fontWeight: 600 }}>Критично:</span> «{worstHousing.type}» — цена {worstHousing.trend > 0 ? '+' : ''}{worstHousing.trend}% при удовлетворённости {worstHousing.satisfaction}%. Это главная болевая точка и причина оттока.</>
            : <><span style={{ fontWeight: 600 }}>Critical:</span> &quot;{worstHousing.type}&quot; is {worstHousing.trend > 0 ? 'up' : 'down'} {worstHousing.trend > 0 ? '+' : ''}{worstHousing.trend}% with only {worstHousing.satisfaction}% satisfaction. This is the top pain point and churn driver.</>
          )}
        </p>
      </div>
    </div>
  );
}