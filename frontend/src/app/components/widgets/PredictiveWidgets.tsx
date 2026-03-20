import { ResponsiveContainer, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip } from 'recharts';
import { useLanguage } from '../../contexts/LanguageContext';
import { useData } from '../../contexts/DataContext';
import { EmptyWidget } from '../ui/EmptyWidget';

// ============================================================
// W19: EMERGING INTERESTS RADAR
// ============================================================

const oppColors: Record<string, { bg: string; text: string; border: string }> = {
  high: { bg: 'bg-emerald-50', text: 'text-emerald-700', border: 'border-emerald-200' },
  medium: { bg: 'bg-blue-50', text: 'text-blue-700', border: 'border-blue-200' },
  low: { bg: 'bg-gray-50', text: 'text-gray-600', border: 'border-gray-200' },
};

const oppLabels = { en: { high: 'high', medium: 'medium' }, ru: { high: 'высокая', medium: 'средняя' } };

export function EmergingInterests() {
  const { lang } = useLanguage();
  const { data } = useData();
  const ru = lang === 'ru';
  const emergingInterests = data.emergingInterests[lang] ?? [];

  if (!emergingInterests.length) return <EmptyWidget title={ru ? 'Зарождающиеся интересы' : 'Emerging Interests'} />;

  const oLabels = oppLabels[lang];

  return (
    <div className="bg-white rounded-xl border border-gray-200 p-6">
      <div className="flex items-center justify-between mb-1">
        <div className="flex items-center gap-2">
          <h3 className="text-gray-900" style={{ fontSize: '1.05rem' }}>
            {ru ? 'Зарождающиеся интересы' : 'Emerging Interests'}
          </h3>
          <span className="bg-purple-100 text-purple-700 text-xs px-2 py-0.5 rounded-full" style={{ fontWeight: 500 }}>
            {emergingInterests.length} {ru ? 'новых' : 'new'}
          </span>
        </div>
        <span className="text-xs text-gray-500">{ru ? 'Темы < 14 дней' : 'Topics <14 days old'}</span>
      </div>
      <p className="text-xs text-gray-500 mb-4">
        {ru
          ? 'Новые разговоры, которые начинают набирать обороты — вступайте раньше других, чтобы возглавить дискуссию'
          : 'New conversations bubbling up — jump on these early to lead the discussion'}
      </p>

      <div className="space-y-2.5">
        {[...emergingInterests].sort((a, b) => b.growthRate - a.growthRate).map((item) => {
          const opp = oppColors[item.opportunity];
          const oppLabel = oLabels[item.opportunity as keyof typeof oLabels];
          return (
            <div key={item.topic} className={`${opp.bg} ${opp.border} border rounded-lg p-3`}>
              <div className="flex items-start justify-between mb-1.5">
                <span className="text-xs text-gray-900" style={{ fontWeight: 600 }}>{item.topic}</span>
                <span className={`text-xs ${opp.text} px-1.5 py-0.5 rounded`} style={{ fontWeight: 500 }}>{oppLabel}</span>
              </div>
              <div className="flex items-center gap-4 text-xs">
                <span className={`${item.growthRate >= 0 ? 'text-emerald-600' : 'text-red-500'}`} style={{ fontWeight: 600 }}>{item.growthRate > 0 ? '+' : ''}{item.growthRate}%</span>
                <span className="text-gray-500">{item.currentVolume} {ru ? 'упоминаний' : 'mentions'}</span>
                <span className="text-gray-400">{ru ? 'через' : 'via'} {item.originChannel}</span>
                <span className="text-gray-400 ml-auto">{item.firstSeen}</span>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}


// ============================================================
// W20: RETENTION RISK GAUGE
// ============================================================

export function RetentionRiskGauge() {
  const { lang } = useLanguage();
  const { data } = useData();
  const ru = lang === 'ru';
  const retentionFactors = data.retentionFactors[lang] ?? [];
  const churnSignals = data.churnSignals[lang] ?? [];

  if (!retentionFactors.length && !churnSignals.length) return <EmptyWidget title={ru ? 'Непрерывность активности и сигналы риска' : 'Activity Continuity & Risk Signals'} />;

  const continuityScore = retentionFactors.length > 0
    ? Math.round(retentionFactors[0]?.overallScore ?? (retentionFactors.reduce((acc, f) => acc + f.score * f.weight, 0) / retentionFactors.reduce((acc, f) => acc + f.weight, 0)))
    : 0;

  const risingSignal = churnSignals.length > 0
    ? [...churnSignals].filter(s => s.severity === 'rising').sort((a, b) => b.trend - a.trend)[0] ?? null
    : null;

  return (
    <div className="bg-white rounded-xl border border-gray-200 p-6">
      <div className="flex items-center justify-between mb-1">
        <h3 className="text-gray-900" style={{ fontSize: '1.05rem' }}>
          {ru ? 'Непрерывность активности и сигналы риска' : 'Activity Continuity & Risk Signals'}
        </h3>
        <span className={`text-xs px-2 py-0.5 rounded-full ${continuityScore >= 70 ? 'bg-emerald-100 text-emerald-700' : continuityScore >= 50 ? 'bg-amber-100 text-amber-700' : 'bg-red-100 text-red-700'}`} style={{ fontWeight: 600 }}>
          {ru ? 'Возврат:' : 'Continuity:'} {continuityScore}/100
        </span>
      </div>
      <p className="text-xs text-gray-500 mb-4">
        {ru
          ? 'Какая доля активных участников возвращается, и какие темы дают риск оттока выше базового уровня.'
          : 'Shows how many active members return and which topics carry above-baseline drop-off risk.'}
      </p>

      <div className="space-y-2 mb-4">
        <span className="text-xs text-gray-500 block" style={{ fontWeight: 500 }}>
          {ru ? 'Темы, связанные с повторной активностью' : 'Topics linked to repeat activity'}
        </span>
        {retentionFactors.map((f) => (
          <div key={f.factor} className="flex items-center gap-2">
            <span className="text-xs text-gray-600 w-44">{f.factor}</span>
            <div className="flex-1 bg-gray-100 rounded-full h-2">
              <div className="h-2 rounded-full" style={{ width: `${f.score}%`, backgroundColor: f.score >= 65 ? '#10b981' : f.score >= 45 ? '#f59e0b' : '#ef4444' }} />
            </div>
            <span className="text-xs text-gray-900 w-8 text-right" style={{ fontWeight: 500 }}>{f.score}</span>
          </div>
        ))}
      </div>

      <div className="pt-3 border-t border-gray-100 space-y-2">
        <span className="text-xs text-gray-500 block" style={{ fontWeight: 500 }}>
          {ru ? 'Сигналы риска выше базового уровня' : 'Above-baseline risk signals'}
        </span>
        {churnSignals.map((signal) => (
          <div key={signal.signal} className={`flex items-center gap-2 text-xs rounded-lg p-2 ${signal.severity === 'rising' ? 'bg-red-50' : signal.severity === 'watch' ? 'bg-amber-50' : 'bg-gray-50'}`}>
            <span className="text-gray-700 flex-1 italic">{signal.signal}</span>
            <span className="text-gray-500">{signal.count}x</span>
            <span className={`${signal.trend > 0 ? 'text-red-500' : 'text-emerald-500'}`} style={{ fontWeight: 600 }}>
              {signal.trend > 0 ? '+' : ''}{signal.trend}%
            </span>
          </div>
        ))}
      </div>

      <div className="mt-3 bg-amber-50 border border-amber-100 rounded-lg px-3 py-2">
        <p className="text-xs text-amber-800">
          {risingSignal ? (ru
            ? <><span style={{ fontWeight: 600 }}>Требует внимания:</span> тема {risingSignal.signal} даёт риск выше базового на {risingSignal.trend > 0 ? '+' : ''}{risingSignal.trend}%. Это главный приоритет для удержания активности.</>
            : <><span style={{ fontWeight: 600 }}>Action needed:</span> {risingSignal.signal} runs {risingSignal.trend > 0 ? '+' : ''}{risingSignal.trend}% above the community baseline. This is the top drop-off risk to address.</>
          ) : (ru ? 'Сигналы риска находятся в пределах базового уровня.' : 'Risk signals are within the community baseline.')}
        </p>
      </div>
    </div>
  );
}


// ============================================================
// W21: COMMUNITY GROWTH FUNNEL
// ============================================================

export function CommunityGrowthFunnel() {
  const { lang } = useLanguage();
  const { data } = useData();
  const ru = lang === 'ru';
  const growthFunnel = data.growthFunnel[lang] ?? [];

  if (!growthFunnel.length) return <EmptyWidget title={ru ? 'Воронка вовлечённости' : 'Engagement Funnel'} />;

  // ✅ FIX: use role field for semantic stage lookup — no more brittle positional indices
  // Previously: growthFunnel[1], growthFunnel[3] — breaks if stages are reordered or added
  const readers     = growthFunnel.find(s => s.role === 'reads')       ?? null;
  const askers      = growthFunnel.find(s => s.role === 'asks')        ?? null;
  const helpers     = growthFunnel.find(s => s.role === 'helps')       ?? null;
  const leaders     = growthFunnel.find(s => s.role === 'leads')       ?? growthFunnel[growthFunnel.length - 1];
  const lurkerCount = readers?.count ?? 0;

  const dropReadToAsk = readers && askers && readers.pct > 0
    ? Math.round(((askers.pct - readers.pct) / readers.pct) * 100)
    : null;
  const dropAskToHelp = askers && helpers && askers.pct > 0
    ? Math.round(((helpers.pct - askers.pct) / askers.pct) * 100)
    : null;

  return (
    <div className="bg-white rounded-xl border border-gray-200 p-6">
      <div className="flex items-center justify-between mb-1">
        <h3 className="text-gray-900" style={{ fontSize: '1.05rem' }}>
          {ru ? 'Воронка вовлечённости' : 'Engagement Funnel'}
        </h3>
        <span className="text-xs text-gray-500">{ru ? 'Прогресс участников' : 'Member progression'}</span>
      </div>
      <p className="text-xs text-gray-500 mb-4">
        {ru
          ? 'Насколько глубоко вовлечены люди? Превращайте наблюдателей в лидеров.'
          : 'How deep does engagement go? Turn lurkers into leaders.'}
      </p>

      <div className="space-y-2 mb-4">
        {growthFunnel.map((stage) => (
          <div key={stage.stage} className="flex items-center gap-3">
            <span className="text-xs text-gray-700 w-36 text-right" style={{ fontWeight: 500 }}>{stage.stage}</span>
            <div className="flex-1 relative">
              <div className="w-full bg-gray-100 rounded-full h-7">
                <div
                  className="h-7 rounded-full flex items-center px-2.5 transition-all"
                  style={{ width: `${Math.max(8, stage.pct)}%`, backgroundColor: stage.color }}
                >
                  <span className="text-xs text-white whitespace-nowrap" style={{ fontWeight: 600 }}>
                    {stage.count.toLocaleString()}
                  </span>
                </div>
              </div>
            </div>
            <span className="text-xs text-gray-500 w-8 text-right">{stage.pct}%</span>
          </div>
        ))}
      </div>

      <div className="grid grid-cols-3 gap-3 pt-3 border-t border-gray-100">
        <div className="text-center">
          <span className="text-xs text-gray-500 block">{ru ? 'Отсев' : 'Drop-off'}</span>
          <span className="text-sm text-gray-900" style={{ fontWeight: 600 }}>{ru ? 'Чит. → Спрос.' : 'Read → Ask'}</span>
          {dropReadToAsk !== null && (
            <span className="text-xs text-red-500 block">{dropReadToAsk}%</span>
          )}
        </div>
        <div className="text-center">
          <span className="text-xs text-gray-500 block">{ru ? 'Конверсия' : 'Conversion'}</span>
          <span className="text-sm text-gray-900" style={{ fontWeight: 600 }}>{ru ? 'Спрос. → Пом.' : 'Ask → Help'}</span>
          {dropAskToHelp !== null && (
            <span className="text-xs text-red-500 block">{dropAskToHelp}%</span>
          )}
        </div>
        <div className="text-center">
          <span className="text-xs text-gray-500 block">{ru ? 'Цель' : 'Target'}</span>
          <span className="text-sm text-gray-900" style={{ fontWeight: 600 }}>{ru ? 'Лидеры' : 'Leaders'}</span>
          {leaders && <span className="text-xs text-emerald-500 block">{leaders.count.toLocaleString()}</span>}
        </div>
      </div>

      <div className="mt-3 bg-blue-50 border border-blue-100 rounded-lg px-3 py-2">
        <p className="text-xs text-blue-800">
          {ru
            ? <><span style={{ fontWeight: 600 }}>Главная возможность:</span> {lurkerCount.toLocaleString()} человек читают, но никогда не задают вопросов. Запустите ветку «Новичок понедельника», чтобы снизить барьер первого взаимодействия.</>
            : <><span style={{ fontWeight: 600 }}>Biggest opportunity:</span> {lurkerCount.toLocaleString()} people read but never ask. Create &quot;New Member Monday&quot; threads to lower the barrier to first engagement.</>
          }
        </p>
      </div>
    </div>
  );
}


// ============================================================
// W22: DECISION STAGE TRACKER
// ============================================================

export function DecisionStageTracker() {
  const { lang } = useLanguage();
  const { data } = useData();
  const ru = lang === 'ru';
  const decisionStages = data.decisionStages[lang] ?? [];

  if (!decisionStages.length) return <EmptyWidget title={ru ? 'Этапы пути участников' : 'Member Journey Stages'} />;

  // ✅ GENERIC: compute fastest-growing stage and max pct dynamically
  const maxPct = Math.max(...decisionStages.map(s => s.pct), 1);
  const fastestStage = [...decisionStages].sort((a, b) => b.trend - a.trend)[0];

  return (
    <div className="bg-white rounded-xl border border-gray-200 p-6">
      <div className="flex items-center justify-between mb-1">
        <h3 className="text-gray-900" style={{ fontSize: '1.05rem' }}>
          {ru ? 'Этапы пути участников' : 'Member Journey Stages'}
        </h3>
        <span className="text-xs text-gray-500">
          {ru ? 'Где каждый находится в своей истории с Арменией?' : 'Where is everyone in their Armenia story?'}
        </span>
      </div>
      <p className="text-xs text-gray-500 mb-4">
        {ru
          ? 'Адаптируйте контент к каждому этапу — новичку и давнему резиденту нужны разные вещи'
          : 'Tailor content to each stage — a newcomer and an established expat need different things'}
      </p>

      <div className="space-y-3">
        {decisionStages.map((stage, i) => (
          <div key={stage.stage} className="flex items-center gap-3">
            <div className="w-6 h-6 rounded-full flex items-center justify-center flex-shrink-0" style={{ backgroundColor: stage.color }}>
              <span className="text-xs text-white" style={{ fontWeight: 700 }}>{i + 1}</span>
            </div>
            <div className="flex-1">
              <div className="flex items-center justify-between mb-1">
                <span className="text-xs text-gray-900" style={{ fontWeight: 500 }}>{stage.stage}</span>
                <div className="flex items-center gap-2">
                  <span className="text-xs text-gray-900" style={{ fontWeight: 600 }}>{stage.count.toLocaleString()}</span>
                  <span className={`text-xs ${stage.trend >= 0 ? 'text-emerald-500' : 'text-red-500'}`}>{stage.trend > 0 ? '+' : ''}{stage.trend}%</span>
                </div>
              </div>
              <div className="w-full bg-gray-100 rounded-full h-2">
                <div className="h-2 rounded-full" style={{ width: `${(stage.pct / maxPct) * 100}%`, backgroundColor: stage.color }} />
              </div>
              <span className="text-xs text-gray-400 mt-0.5 block">
                {ru ? 'Нужно:' : 'Needs:'} {stage.needs}
              </span>
            </div>
          </div>
        ))}
      </div>

      <div className="mt-3 bg-violet-50 border border-violet-100 rounded-lg px-3 py-2">
        <p className="text-xs text-violet-800">
          {fastestStage && (ru
            ? <><span style={{ fontWeight: 600 }}>Сигнал роста:</span> «{fastestStage.stage}» — самый быстрорастущий сегмент ({fastestStage.trend > 0 ? '+' : ''}{fastestStage.trend}%). {fastestStage.count.toLocaleString()} человек в этом сегменте.</>
            : <><span style={{ fontWeight: 600 }}>Growth signal:</span> &quot;{fastestStage.stage}&quot; is the fastest-growing segment ({fastestStage.trend > 0 ? '+' : ''}{fastestStage.trend}%). {fastestStage.count.toLocaleString()} people are in this stage.</>
          )}
        </p>
      </div>
    </div>
  );
}


// ============================================================
// NEW VS. RETURNING VOICE RATIO
// ============================================================

export function NewVsReturningVoice() {
  const { lang } = useLanguage();
  const { data } = useData();
  const ru = lang === 'ru';
  const topNewTopics = data.topNewTopics[lang] ?? [];
  const voiceData = data.voiceData;

  if (!voiceData.length) return <EmptyWidget title={ru ? 'Новые vs. постоянные голоса' : 'New vs. Returning Voices'} />;

  const latestWeek = voiceData[voiceData.length - 1];
  const total = latestWeek.newVoices + latestWeek.returning;
  const newPct = total > 0 ? Math.round((latestWeek.newVoices / total) * 100) : 0;

  const prevWeek = voiceData.length >= 2 ? voiceData[voiceData.length - 2] : null;
  const trend = prevWeek
    ? (() => {
        const prevTotal = prevWeek.newVoices + prevWeek.returning;
        const prevNewPct = prevTotal > 0 ? Math.round((prevWeek.newVoices / prevTotal) * 100) : 0;
        return newPct - prevNewPct;
      })()
    : 0;

  // ✅ GENERIC: compute growth from first to last data point dynamically
  const firstWeek = voiceData[0];
  const totalWeeks = voiceData.length;
  const voiceGrowthPct = firstWeek && firstWeek.newVoices > 0
    ? Math.round(((latestWeek.newVoices - firstWeek.newVoices) / firstWeek.newVoices) * 100)
    : null;

  // ✅ GENERIC: dynamic bar width — scale to max pct in topNewTopics
  const maxTopicPct = Math.max(...topNewTopics.map(t => t.pct), 1);

  return (
    <div className="bg-white rounded-xl border border-gray-200 p-6">
      <div className="flex items-center justify-between mb-1">
        <h3 className="text-gray-900" style={{ fontSize: '1.05rem' }}>
          {ru ? 'Н��вые vs. постоянные голоса' : 'New vs. Returning Voices'}
        </h3>
        <span className={`text-xs px-2 py-0.5 rounded-full ${trend > 0 ? 'bg-emerald-100 text-emerald-700' : 'bg-red-100 text-red-700'}`} style={{ fontWeight: 500 }}>
          {newPct}% {ru ? 'новых на этой неделе' : 'new this week'}
        </span>
      </div>
      <p className="text-xs text-gray-500 mb-4">
        {ru
          ? 'Рост новых голосов = рост сообщества. Падение — сигнал, что разговор превращается в эхо-камеру постоянных участников.'
          : 'Rising newcomers = community growth. A falling new-voice ratio signals the conversation is becoming an echo chamber of regulars.'}
      </p>

      <div className="grid grid-cols-3 gap-3 mb-4">
        <div className="bg-blue-50 rounded-lg p-3 text-center">
          <span className="text-xl text-blue-700 block" style={{ fontWeight: 700 }}>{latestWeek.newVoices}</span>
          <span className="text-xs text-gray-500">{ru ? 'Новые голоса' : 'New voices'}</span>
        </div>
        <div className="bg-slate-50 rounded-lg p-3 text-center">
          <span className="text-xl text-slate-700 block" style={{ fontWeight: 700 }}>{latestWeek.returning.toLocaleString()}</span>
          <span className="text-xs text-gray-500">{ru ? 'Постоянные' : 'Returning'}</span>
        </div>
        <div className={`rounded-lg p-3 text-center ${trend > 0 ? 'bg-emerald-50' : 'bg-red-50'}`}>
          <span className={`text-xl block ${trend > 0 ? 'text-emerald-700' : 'text-red-600'}`} style={{ fontWeight: 700 }}>
            {trend > 0 ? '+' : ''}{trend}%
          </span>
          <span className="text-xs text-gray-500">{ru ? 'Тренд новых' : 'New voice trend'}</span>
        </div>
      </div>

      <ResponsiveContainer width="100%" height={160}>
        <BarChart data={voiceData} barSize={20}>
          <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" />
          <XAxis dataKey="week" tick={{ fontSize: 11 }} stroke="#9ca3af" />
          <YAxis tick={{ fontSize: 11 }} stroke="#9ca3af" />
          <Tooltip />
          <Bar dataKey="returning" stackId="a" fill="#cbd5e1" name={ru ? 'Постоянные' : 'Returning'} />
          <Bar dataKey="newVoices" stackId="a" fill="#0d9488" name={ru ? 'Новые голоса' : 'New voices'} radius={[4, 4, 0, 0]} />
        </BarChart>
      </ResponsiveContainer>

      <div className="mt-4 pt-3 border-t border-gray-100">
        <span className="text-xs text-gray-700 block mb-2" style={{ fontWeight: 600 }}>
          {ru ? 'Где новые голоса говорят первыми' : 'Where new voices speak first'}
        </span>
        <div className="space-y-1.5">
          {topNewTopics.map((item) => (
            <div key={item.topic} className="flex items-center gap-2">
              <span className="text-xs text-gray-600 w-36">{item.topic}</span>
              <div className="flex-1 bg-gray-100 rounded-full h-2">
                {/* ✅ FIX: dynamic width — no more hardcoded * 4 multiplier */}
                <div className="h-2 rounded-full bg-blue-500" style={{ width: `${(item.pct / maxTopicPct) * 100}%` }} />
              </div>
              <span className="text-xs text-gray-900 w-8 text-right" style={{ fontWeight: 600 }}>{item.newVoices}</span>
            </div>
          ))}
        </div>
      </div>

      <div className="mt-3 bg-purple-50 border border-purple-100 rounded-lg px-3 py-2">
        <p className="text-xs text-purple-800">
          {voiceGrowthPct !== null ? (ru
            ? <><span style={{ fontWeight: 600 }}>Здоровый сигнал:</span> Новые голоса выросли с {firstWeek.newVoices} до {latestWeek.newVoices} за {totalWeeks} недель ({voiceGrowthPct > 0 ? '+' : ''}{voiceGrowthPct}%). Сообщество привлекает новых участников, а не только постоянных.</>
            : <><span style={{ fontWeight: 600 }}>Healthy signal:</span> New voices grew from {firstWeek.newVoices} to {latestWeek.newVoices} over {totalWeeks} weeks ({voiceGrowthPct > 0 ? '+' : ''}{voiceGrowthPct}%). The community is attracting fresh participants.</>
          ) : (ru ? 'Данных пока недостаточно для расчёта тренда.' : 'Not enough data to compute trend yet.')}
        </p>
      </div>
    </div>
  );
}
