import { Link } from 'react-router';
import { Home } from 'lucide-react';
import { useLanguage } from '../../contexts/LanguageContext';
import { useData } from '../../contexts/DataContext';
import { EmptyWidget } from '../ui/EmptyWidget';
import { WidgetTitle } from '../ui/WidgetTitle';

// ============================================================
// W26: BUSINESS OPPORTUNITY SIGNALS
// ============================================================

export function BusinessOpportunityTracker() {
  const { lang } = useLanguage();
  const { data } = useData();
  const ru = lang === 'ru';
  const opportunities = data.businessOpportunityBriefs[lang] ?? [];

  if (!opportunities.length) return <EmptyWidget widgetId="business_opportunity_tracker" title={ru ? 'Бизнес-возможности от сообщества' : 'Business Opportunity Signals'} />;

  const confidenceLabel = (value: string) => {
    if (value === 'high') return ru ? 'Высокая' : 'High';
    if (value === 'medium') return ru ? 'Средняя' : 'Medium';
    return ru ? 'Низкая' : 'Low';
  };

  const deliveryLabel = (value: string) => {
    if (ru) {
      if (value === 'product') return 'Продукт';
      if (value === 'marketplace') return 'Маркетплейс';
      if (value === 'content') return 'Контент';
      if (value === 'community_program') return 'Программа сообщества';
      return 'Сервис';
    }
    if (value === 'product') return 'Product';
    if (value === 'marketplace') return 'Marketplace';
    if (value === 'content') return 'Content';
    if (value === 'community_program') return 'Community program';
    return 'Service';
  };

  const readinessLabel = (value: string) => {
    if (value === 'pilot_ready') return ru ? 'Готово к пилоту' : 'Pilot ready';
    if (value === 'watchlist') return ru ? 'Наблюдать' : 'Watchlist';
    return ru ? 'Проверить сейчас' : 'Validate now';
  };

  return (
    <div className="bg-white rounded-xl border border-gray-200 p-6">
      <div className="mb-1 flex flex-col items-start gap-2 sm:flex-row sm:items-start sm:justify-between">
        <WidgetTitle widgetId="business_opportunity_tracker">
          {ru ? 'Бизнес-возможности от сообщества' : 'Business Opportunity Signals'}
        </WidgetTitle>
        <span className="w-full text-left text-xs text-gray-500 sm:w-auto sm:text-right">{ru ? 'AI + доказательства' : 'AI + evidence grounded'}</span>
      </div>
      <p className="text-xs text-gray-500 mb-4">
        {ru
          ? 'Показываем AI-сводку возможностей: повторяющиеся неудовлетворённые запросы объединяются в конкретные идеи, привязанные к реальным сообщениям из недавнего окна.'
          : 'Shows the AI opportunity overview: recurring unmet needs are grouped into concrete ideas and tied back to real source messages from the recent rolling window.'}
      </p>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-2.5">
        {opportunities.map((opp) => (
          <Link
            key={opp.id}
            to={(() => {
              const params = new URLSearchParams();
              params.set('topic', opp.sourceTopic || opp.topic);
              params.set('view', 'evidence');
              if (opp.sampleEvidenceId) params.set('evidenceId', opp.sampleEvidenceId);
              return `/topics?${params.toString()}`;
            })()}
            className="rounded-xl border border-blue-100 bg-blue-50/50 p-3 transition-colors hover:shadow-sm"
          >
            <div className="flex items-start justify-between gap-2">
              <div className="min-w-0">
                <div className="text-[11px] text-blue-800/80">{opp.category}</div>
                <div className="text-sm leading-snug text-gray-900" style={{ fontWeight: 700 }}>{opp.opportunity}</div>
              </div>
              <span className="text-[10px] px-1.5 py-0.5 rounded border border-blue-300 text-blue-900" style={{ fontWeight: 600 }}>
                {confidenceLabel(opp.confidence)} {ru ? 'уверенность' : 'confidence'}
              </span>
            </div>

            <p className="text-xs leading-relaxed text-gray-700 mt-1.5">{opp.summary}</p>

            <div className="mt-2 flex flex-wrap items-center gap-1.5 text-[10px]">
              <span className="px-1.5 py-0.5 rounded-full bg-white border border-blue-200 text-blue-900">{deliveryLabel(opp.deliveryModel)}</span>
              <span className="px-1.5 py-0.5 rounded-full bg-white border border-gray-200 text-gray-700">{readinessLabel(opp.readiness)}</span>
            </div>

            <div className="text-[11px] mt-2 text-gray-600">
              {opp.demandSignals.messages.toLocaleString()} {ru ? 'сигналов ·' : 'signals ·'} {opp.demandSignals.uniqueUsers.toLocaleString()} {ru ? 'людей ·' : 'people ·'} {opp.demandSignals.channels.toLocaleString()} {ru ? 'каналов' : 'channels'}
            </div>
            <div className="text-[11px] mt-0.5 text-gray-600">
              {ru ? '7д тренд' : '7d trend'}: {opp.demandSignals.trend7dPct > 0 ? '+' : ''}{opp.demandSignals.trend7dPct}%
            </div>
          </Link>
        ))}
      </div>

      <p className="text-xs text-gray-400 mt-3">
        {ru
          ? 'Карточки строятся по скользящему AI-окну, а не по выбранному диапазону дашборда. Пустой виджет означает, что надёжных AI-возможностей сейчас нет.'
          : 'These cards are built from a rolling AI analysis window, not the selected dashboard date range. An empty widget means there are no reliable AI-detected opportunities right now.'}
      </p>
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

  if (!jobSeeking.length) return <EmptyWidget widgetId="job_market_pulse" title={ru ? 'Рынок труда и занятость' : 'Job & Work Landscape'} />;

  // ✅ GENERIC: dynamic max divisor + top role computed from data
  const maxJobPct = Math.max(...jobSeeking.map(j => j.pct), 1);
  const topJob = [...jobSeeking].sort((a, b) => b.pct - a.pct)[0];
  const topEvidence = topJob?.evidence?.slice(0, 3) ?? [];

  return (
    <div className="bg-white rounded-xl border border-gray-200 p-6">
      <div className="mb-1 flex flex-col items-start gap-2 sm:flex-row sm:items-start sm:justify-between">
        <WidgetTitle widgetId="job_market_pulse">
          {ru ? 'Рынок труда и занятость' : 'Job & Work Landscape'}
        </WidgetTitle>
        <span className="w-full text-left text-xs text-gray-500 sm:w-auto sm:text-right">{ru ? 'Как работает сообщество' : 'How the community works'}</span>
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

      {topEvidence.length > 0 && (
        <div className="mt-3">
          <span className="text-xs text-gray-500 block mb-2" style={{ fontWeight: 500 }}>
            {ru ? 'Доказательства из сообщества' : 'Community evidence'}
          </span>
          <div className="space-y-2.5">
            {topEvidence.map((item, index) => (
              <Link
                key={`${item.sourceTopic || item.topic}-${item.id || index}`}
                to={(() => {
                  const params = new URLSearchParams();
                  params.set('topic', item.sourceTopic || item.topic);
                  params.set('view', 'evidence');
                  if (item.id) params.set('evidenceId', item.id);
                  return `/topics?${params.toString()}`;
                })()}
                className="block bg-gray-50 border border-gray-100 rounded-lg p-3 cursor-pointer hover:bg-gray-100 transition-colors"
              >
                <div className="flex items-start gap-2.5">
                  <div className="flex-1 min-w-0">
                    <p className="text-xs text-gray-900 leading-relaxed" style={{ fontWeight: 600 }}>{item.topic}</p>
                    <p className="text-xs text-gray-400 italic mt-0.5">
                      &ldquo;{item.text}&rdquo;
                    </p>
                    <div className="flex items-center gap-3 mt-1.5">
                      <span className="text-xs px-1.5 py-0.5 rounded bg-gray-100 text-gray-500">
                        {item.kind === 'post' ? (ru ? 'Пост' : 'Post') : (ru ? 'Комментарий' : 'Comment')}
                      </span>
                      <span className="text-xs text-gray-500 truncate">
                        {item.channel || (ru ? 'Открыть тему' : 'Open topic')}
                      </span>
                    </div>
                  </div>
                </div>
              </Link>
            ))}
          </div>
        </div>
      )}

      <div className="mt-3 bg-blue-50 border border-blue-100 rounded-lg px-3 py-2">
        <p className="text-xs text-blue-800">
          {topJob && (ru
            ? <><span style={{ fontWeight: 600 }}>Ключевой инсайт:</span> {topJob.pct}% сигналов относятся к категории «{topJob.role}» ({topJob.count.toLocaleString()} человек). Это подтверждено реальными обсуждениями выше, поэтому можно уверенно строить контент вокруг этой потребности.</>
            : <><span style={{ fontWeight: 600 }}>Key insight:</span> {topJob.pct}% of work-intent signals are in &quot;{topJob.role}&quot; ({topJob.count.toLocaleString()} people). The excerpts above show this is grounded in real community discussion, so it is a credible content and service signal.</>
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
