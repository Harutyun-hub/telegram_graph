import { useState } from 'react';
import { ResponsiveContainer, RadarChart, PolarGrid, PolarAngleAxis, PolarRadiusAxis, Radar, AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip } from 'recharts';
import { User, MapPin } from 'lucide-react';
import { useLanguage } from '../../contexts/LanguageContext';
import { useData } from '../../contexts/DataContext';
import { useDashboardDateRange } from '../../contexts/DashboardDateRangeContext';
import { EmptyWidget } from '../ui/EmptyWidget';

// ============================================================
// W14: COMMUNITY PERSONA GALLERY
// ============================================================

const profileLabels = {
  en: { profile: 'Profile', needs: 'Needs', interests: 'Interests', pain: 'Pain points', people: 'people', clickPrompt: 'Click a bar above to explore persona details' },
  ru: { profile: 'Профиль', needs: 'Потребности', interests: 'Интересы', pain: 'Болевые точки', people: 'человек', clickPrompt: 'Нажмите на полосу выше для изучения персоны' },
};

export function PersonaGallery() {
  const { lang } = useLanguage();
  const { data } = useData();
  const ru = lang === 'ru';
  const [selectedPersona, setSelectedPersona] = useState<number | null>(null);
  const personas = data.personas[lang] ?? [];
  const labels = profileLabels[lang];

  if (!personas.length) return <EmptyWidget title={ru ? 'Персоны сообщества' : 'Community Personas'} />;

  // ✅ FIX: compute max size dynamically so bars never overflow
  const maxPersonaSize = Math.max(...personas.map(p => p.size), 1);

  return (
    <div className="bg-white rounded-xl border border-gray-200 p-6">
      <div className="flex items-center justify-between mb-1">
        <h3 className="text-gray-900" style={{ fontSize: '1.05rem' }}>
          {ru ? 'Персоны сообщества' : 'Community Personas'}
        </h3>
        {/* ✅ FIX: use personas.length instead of hardcoded "6" */}
        <span className="text-xs text-gray-500">
          {ru ? `${personas.length} поведенческих кластеров` : `${personas.length} behavioral clusters`}
        </span>
      </div>
      <p className="text-xs text-gray-500 mb-4">
        {ru ? 'Кто ваши участники? Нажмите на сегмент для изучения.' : 'Who are your community members? Click a segment to explore.'}
      </p>

      <div className="flex items-end gap-2 mb-4 h-20">
        {personas.map((p, i) => (
          <div
            key={p.name}
            className={`flex-1 rounded-t-lg cursor-pointer transition-all ${selectedPersona === i ? 'ring-2 ring-gray-900' : 'hover:opacity-80'}`}
            style={{ height: `${(p.size / maxPersonaSize) * 100}%`, backgroundColor: p.color, opacity: selectedPersona === null || selectedPersona === i ? 1 : 0.4 }}
            onClick={() => setSelectedPersona(selectedPersona === i ? null : i)}
          />
        ))}
      </div>
      <div className="flex gap-2 mb-3">
        {personas.map((p) => (
          <div key={p.name} className="flex-1 text-center">
            <span className="text-xs text-gray-600 block truncate" style={{ fontSize: '9px' }}>
              {ru ? p.name : p.name.replace('The ', '')}
            </span>
            <span className="text-xs text-gray-900" style={{ fontWeight: 600 }}>{p.size}%</span>
          </div>
        ))}
      </div>

      {selectedPersona !== null && (
        <div className="bg-gray-50 rounded-lg p-4 border border-gray-200">
          <div className="flex items-center gap-2 mb-2">
            <div className="w-8 h-8 rounded-full flex items-center justify-center" style={{ backgroundColor: personas[selectedPersona].color }}>
              <User className="w-4 h-4 text-white" />
            </div>
            <div>
              <span className="text-sm text-gray-900" style={{ fontWeight: 600 }}>{personas[selectedPersona].name}</span>
              <span className="text-xs text-gray-500 ml-2">{personas[selectedPersona].count.toLocaleString()} {labels.people}</span>
            </div>
          </div>
          <p className="text-xs text-gray-600 leading-relaxed mb-2">{personas[selectedPersona].desc}</p>
          <div className="grid grid-cols-2 gap-2 text-xs">
            <div><span className="text-gray-400">{labels.profile}:</span> <span className="text-gray-700" style={{ fontWeight: 500 }}>{personas[selectedPersona].profile}</span></div>
            <div><span className="text-gray-400">{labels.needs}:</span> <span className="text-gray-700" style={{ fontWeight: 500 }}>{personas[selectedPersona].needs}</span></div>
            <div><span className="text-gray-400">{labels.interests}:</span> <span className="text-gray-700" style={{ fontWeight: 500 }}>{personas[selectedPersona].interests}</span></div>
            <div><span className="text-gray-400">{labels.pain}:</span> <span className="text-gray-700" style={{ fontWeight: 500 }}>{personas[selectedPersona].pain}</span></div>
          </div>
        </div>
      )}

      {selectedPersona === null && (
        <div className="text-center py-2">
          <span className="text-xs text-gray-400">{labels.clickPrompt}</span>
        </div>
      )}
    </div>
  );
}


// ============================================================
// W15: INTERESTS RADAR
// ============================================================

export function InterestRadar() {
  const { lang } = useLanguage();
  const { data } = useData();
  const { range } = useDashboardDateRange();
  const ru = lang === 'ru';
  const interestData = data.interests[lang] ?? [];

  if (!interestData.length) return <EmptyWidget title={ru ? 'Интересы сообщества' : 'Community Interests'} />;

  // ✅ GENERIC: compute top 2 interests dynamically
  const sorted = [...interestData].sort((a, b) => b.score - a.score);
  const top2 = sorted.slice(0, 2);
  const chartData = interestData.map((item) => ({
    ...item,
    interestLabel: item.interest.replace(/\s*&\s*/g, ' & ').split(' ').reduce<string[]>((lines, word) => {
      const current = lines[lines.length - 1] || '';
      if (!current || `${current} ${word}`.trim().length <= 18) {
        if (current) {
          lines[lines.length - 1] = `${current} ${word}`;
        } else {
          lines.push(word);
        }
      } else if (lines.length < 3) {
        lines.push(word);
      } else {
        lines[lines.length - 1] = `${lines[lines.length - 1]} ${word}`;
      }
      return lines;
    }, []),
  }));
  const maxScore = Math.max(...interestData.map((item) => item.score), 0);
  const radialMax = Math.min(100, Math.max(25, Math.ceil((maxScore + 10) / 10) * 10));
  const radialTicks = Array.from(new Set([
    0,
    Math.round(radialMax * 0.25),
    Math.round(radialMax * 0.5),
    Math.round(radialMax * 0.75),
    radialMax,
  ])).sort((a, b) => a - b);

  return (
    <div className="bg-white rounded-xl border border-gray-200 p-6">
      <div className="flex items-center justify-between mb-1">
        <h3 className="text-gray-900" style={{ fontSize: '1.05rem' }}>
          {ru ? 'Интересы сообщества' : 'Community Interests'}
        </h3>
        <span className="text-xs text-gray-500">
          {ru ? `Выбранное окно · ${range.days} дн.` : `Selected window · ${range.days}d`}
        </span>
      </div>
      <p className="text-xs text-gray-500 mb-3">
        {ru
          ? `Доля активных участников, обсуждавших каждую тему интереса в выбранном ${range.days}-дневном окне`
          : `Share of active members discussing each interest area in the selected ${range.days}-day window`}
      </p>

      <div className="mt-4 rounded-2xl border border-slate-100 bg-slate-50/70 px-2 py-4">
        <ResponsiveContainer width="100%" height={360}>
          <RadarChart data={chartData} cx="50%" cy="50%" outerRadius="74%">
            <PolarGrid stroke="#dbe3ee" radialLines={true} />
            <PolarAngleAxis
              dataKey="interest"
              tick={({ x, y, payload, textAnchor }) => {
                const entry = chartData.find((item) => item.interest === payload.value);
                const lines = entry?.interestLabel || [String(payload.value)];
                const lineHeight = 14;
                const startY = y - ((lines.length - 1) * lineHeight) / 2;

                return (
                  <text x={x} y={startY} textAnchor={textAnchor} fill="#6b7280" fontSize={11} fontWeight={500}>
                    {lines.map((line, index) => (
                      <tspan key={`${payload.value}-${line}-${index}`} x={x} dy={index === 0 ? 0 : lineHeight}>
                        {line}
                      </tspan>
                    ))}
                  </text>
                );
              }}
            />
            <PolarRadiusAxis
              angle={18}
              domain={[0, radialMax]}
              ticks={radialTicks}
              tick={{ fontSize: 10, fill: '#94a3b8' }}
              axisLine={false}
              tickFormatter={(value) => `${value}%`}
            />
            <Radar
              name={ru ? 'Доля активных участников' : 'Active-member share'}
              dataKey="score"
              stroke="#0f766e"
              fill="#14b8a6"
              fillOpacity={0.24}
              strokeWidth={3}
              dot={{ r: 3, fill: '#0f766e', strokeWidth: 0 }}
            />
          </RadarChart>
        </ResponsiveContainer>

        <div className="mt-2 flex items-center justify-between px-2">
          <span className="text-[11px] text-slate-500">
            {ru ? `Шкала отображения: 0-${radialMax}%` : `Display scale: 0-${radialMax}%`}
          </span>
          <span className="text-[11px] text-slate-500">
            {ru ? `Пик окна: ${maxScore}%` : `Window peak: ${maxScore}%`}
          </span>
        </div>
      </div>

      <div className="grid grid-cols-2 gap-2 mt-2">
        {[...interestData].sort((a, b) => b.score - a.score).slice(0, 4).map((item) => (
          <div key={item.interest} className="flex items-center gap-2">
            <div className="w-2 h-2 rounded-full bg-teal-500" />
            <span className="text-xs text-gray-600">{item.interest}</span>
            <span className="text-xs text-gray-900 ml-auto" style={{ fontWeight: 600 }}>{item.score}%</span>
          </div>
        ))}
      </div>

      <div className="mt-3 bg-teal-50 border border-teal-100 rounded-lg px-3 py-2">
        <p className="text-xs text-teal-800">
          {top2.length >= 2 && (ru
            ? <><span style={{ fontWeight: 600 }}>Приоритет:</span> {top2[0].interest} ({top2[0].score}%) и {top2[1].interest} ({top2[1].score}%) имеют наибольший охват среди активных участников в текущем выбранном окне. Ставьте эти темы в приоритет для контента, событий и экспертных обсуждений.</>
            : <><span style={{ fontWeight: 600 }}>Priority signal:</span> {top2[0].interest} ({top2[0].score}%) and {top2[1].interest} ({top2[1].score}%) have the highest active-member penetration in the current selected window. Prioritize these areas for content, events, and expert participation.</>
          )}
        </p>
      </div>
    </div>
  );
}


// ============================================================
// W16: WHERE THEY CAME FROM
// ============================================================

export function OriginMap() {
  const { lang } = useLanguage();
  const { data } = useData();
  const ru = lang === 'ru';
  const origins = data.origins;

  if (!origins.length) return <EmptyWidget title={ru ? 'Откуда они приехали' : 'Where They Came From'} />;

  // ✅ GENERIC: compute max pct and top-2 cities dynamically
  const maxPct = Math.max(...origins.map(o => o.pct), 1);
  const sortedOrigins = [...origins].sort((a, b) => b.pct - a.pct);
  const top2Origins = sortedOrigins.slice(0, 2);
  const top2PctSum = top2Origins.reduce((s, o) => s + o.pct, 0);

  return (
    <div className="bg-white rounded-xl border border-gray-200 p-6">
      <div className="flex items-center justify-between mb-1">
        <h3 className="text-gray-900" style={{ fontSize: '1.05rem' }}>
          {ru ? 'Откуда они приехали' : 'Where They Came From'}
        </h3>
        <span className="text-xs text-gray-500">
          {ru ? 'Города происхождения из анализа переписки' : 'Origin cities inferred from messages'}
        </span>
      </div>
      <p className="text-xs text-gray-500 mb-4">
        {ru
          ? 'Понимание происхождения помогает адаптировать контент и находить общий язык'
          : 'Understanding origins helps tailor content and find common ground'}
      </p>

      <div className="space-y-2.5">
        {origins.map((origin) => (
          <div key={origin.city} className="flex items-center gap-3">
            <MapPin className="w-3.5 h-3.5 text-gray-400 flex-shrink-0" />
            <span className="text-xs text-gray-700 w-40" style={{ fontWeight: 500 }}>
              {ru ? origin.city : origin.cityEN}
            </span>
            <div className="flex-1 bg-gray-100 rounded-full h-3">
              <div
                className="h-3 rounded-full flex items-center justify-end pr-1.5"
                style={{ width: `${(origin.pct / maxPct) * 100}%`, backgroundColor: origin.color }}
              >
                {origin.pct >= (maxPct * 0.23) && (
                  <span className="text-xs text-white" style={{ fontWeight: 600, fontSize: '9px' }}>{origin.pct}%</span>
                )}
              </div>
            </div>
            <span className="text-xs text-gray-500 w-12 text-right">{(origin.count / 1000).toFixed(1)}K</span>
          </div>
        ))}
      </div>

      <div className="mt-3 pt-3 border-t border-gray-100">
        <div className="bg-blue-50 border border-blue-100 rounded-lg px-3 py-2">
          <p className="text-xs text-blue-800">
            {top2Origins.length >= 2 && (ru
              ? <><span style={{ fontWeight: 600 }}>{top2Origins[0].city} + {top2Origins[1].city} = {top2PctSum}%</span> сообщества. Контент, апеллирующий к этим городам (сравнения, ностальгия), резонирует максимально сильно.</>
              : <><span style={{ fontWeight: 600 }}>{top2Origins[0].cityEN} + {top2Origins[1].cityEN} = {top2PctSum}%</span> of the community. Content referencing these cities (comparisons, nostalgia) resonates strongly.</>
            )}
          </p>
        </div>
      </div>
    </div>
  );
}


// ============================================================
// W17: INTEGRATION SPECTRUM
// ============================================================

export function IntegrationSpectrum() {
  const { lang } = useLanguage();
  const { data } = useData();
  const ru = lang === 'ru';
  const integrationLevels = data.integrationLevels[lang] ?? [];
  const integrationData = data.integrationData;
  // ✅ FIX: use config-driven series instead of hardcoded Area dataKeys
  const seriesConfig = data.integrationSeriesConfig;

  if (!integrationLevels.length) return <EmptyWidget title={ru ? 'Спектр интеграции' : 'Integration Spectrum'} />;

  // ✅ FIX: compute trends generically using seriesConfig.polarity instead of hardcoded field names
  const intFirst = integrationData[0];
  const intLast = integrationData[integrationData.length - 1];

  // Find the primary "negative" series (e.g. Russian Only) and "positive" series (e.g. Learning & Mixing)
  const negSeries = seriesConfig.find(s => s.polarity === 'negative') ?? null;
  const posSeries = seriesConfig.find(s => s.polarity === 'positive') ?? null;

  const computeTrend = (key: string) => {
    if (!intFirst || !intLast) return null;
    const first = (intFirst as Record<string, number>)[key] ?? 0;
    const last  = (intLast  as Record<string, number>)[key] ?? 0;
    return first > 0 ? Math.round(((last - first) / first) * 100) : null;
  };

  const negTrend = negSeries ? computeTrend(negSeries.key) : null;
  const posTrend = posSeries ? computeTrend(posSeries.key) : null;

  const negLabel = negSeries ? (ru ? negSeries.labelRu : negSeries.label) : '';
  const posLabel = posSeries ? (ru ? posSeries.labelRu : posSeries.label) : '';

  return (
    <div className="bg-white rounded-xl border border-gray-200 p-6">
      <div className="flex items-center justify-between mb-1">
        <h3 className="text-gray-900" style={{ fontSize: '1.05rem' }}>
          {ru ? 'Спектр интеграции' : 'Integration Spectrum'}
        </h3>
        <span className="text-xs text-gray-500">
          {ru ? 'Насколько интегрировано сообщество?' : 'How integrated is the community?'}
        </span>
      </div>
      <p className="text-xs text-gray-500 mb-4">
        {ru
          ? 'Интеграция ≠ ассимиляция. Речь о том, чтобы чувствовать себя дома и строить мосты.'
          : 'Integration ≠ assimilation. It\'s about feeling at home and building bridges.'}
      </p>

      <div className="flex h-8 rounded-lg overflow-hidden mb-2">
        {integrationLevels.map((level) => (
          <div key={level.level} className="flex items-center justify-center" style={{ width: `${level.pct}%`, backgroundColor: level.color }}>
            {level.pct >= 15 && (<span className="text-xs text-white" style={{ fontWeight: 600 }}>{level.pct}%</span>)}
          </div>
        ))}
      </div>

      <div className="space-y-1.5 mb-4">
        {integrationLevels.map((level) => (
          <div key={level.level} className="flex items-center gap-2">
            <div className="w-2.5 h-2.5 rounded-full flex-shrink-0" style={{ backgroundColor: level.color }} />
            <span className="text-xs text-gray-700" style={{ fontWeight: 500 }}>{level.level}</span>
            <span className="text-xs text-gray-400 ml-auto">{level.desc}</span>
          </div>
        ))}
      </div>

      <div className="pt-3 border-t border-gray-100">
        <span className="text-xs text-gray-500 mb-2 block">
          {ru ? 'Тренд интеграции за 6 месяцев' : 'Integration trend over 6 months'}
        </span>
        <ResponsiveContainer width="100%" height={140}>
          <AreaChart data={integrationData}>
            <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" />
            <XAxis dataKey="month" tick={{ fontSize: 10 }} stroke="#9ca3af" />
            <YAxis tick={{ fontSize: 10 }} stroke="#9ca3af" />
            <Tooltip />
            {/* ✅ FIX: driven by integrationSeriesConfig — no hardcoded dataKey strings */}
            {seriesConfig.map((s) => (
              <Area
                key={s.key}
                type="monotone"
                dataKey={s.key}
                stackId="1"
                stroke={s.color}
                fill={s.color}
                fillOpacity={0.7}
              />
            ))}
          </AreaChart>
        </ResponsiveContainer>
      </div>

      <div className="mt-3 bg-blue-50 border border-blue-100 rounded-lg px-3 py-2">
        <p className="text-xs text-blue-800">
          {negTrend !== null && posTrend !== null && negLabel && posLabel ? (ru
            ? <><span style={{ fontWeight: 600 }}>Хорошая новость:</span> Группа «{negLabel}» {negTrend > 0 ? 'выросла' : 'сократилась'} ({negTrend > 0 ? '+' : ''}{negTrend}%), а «{posLabel}» {posTrend > 0 ? 'выросла' : 'сократилась'} ({posTrend > 0 ? '+' : ''}{posTrend}%). Контент о языковых курсах ускорит этот тренд.</>
            : <><span style={{ fontWeight: 600 }}>Good news:</span> The &quot;{negLabel}&quot; group {negTrend > 0 ? 'grew' : 'shrank'} ({negTrend > 0 ? '+' : ''}{negTrend}%), while &quot;{posLabel}&quot; {posTrend > 0 ? 'grew' : 'declined'} ({posTrend > 0 ? '+' : ''}{posTrend}%). Language course content would accelerate this trend.</>
          ) : null}
        </p>
      </div>
    </div>
  );
}
