import { useState } from 'react';
import { Link } from 'react-router';
import { Star, TrendingUp, Clock } from 'lucide-react';
import { ResponsiveContainer, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip } from 'recharts';
import { useLanguage } from '../../contexts/LanguageContext';
import { useData } from '../../contexts/DataContext';
import { useDashboardDateRange } from '../../contexts/DashboardDateRangeContext';
import { EmptyWidget } from '../ui/EmptyWidget';

// ============================================================
// W12: TOP CHANNELS & GROUPS
// ============================================================

const typeLabels = {
  en: { General: 'General', Work: 'Work', Family: 'Family', Housing: 'Housing', Business: 'Business', Lifestyle: 'Lifestyle', Legal: 'Legal' },
  ru: { General: 'Общий', Work: 'Работа', Family: 'Семья', Housing: 'Жильё', Business: 'Бизнес', Lifestyle: 'Досуг', Legal: 'Право' },
};

const typeColors: Record<string, string> = {
  General: '#3b82f6', Work: '#f59e0b', Family: '#ec4899',
  Housing: '#ef4444', Business: '#10b981', Lifestyle: '#8b5cf6', Legal: '#6b7280',
};

export function TopChannels() {
  const { lang } = useLanguage();
  const { data } = useData();
  const ru = lang === 'ru';
  const communityChannels = data.communityChannels;

  if (!communityChannels.length) return <EmptyWidget title={ru ? 'Топ-каналы сообщества' : 'Top Community Channels'} />;

  const labels = typeLabels[lang];

  // Filter out invalid channels and limit to top 10 most engaged
  const validChannels = communityChannels.filter(ch =>
    ch.name &&
    ch.members > 0 &&
    ch.engagement >= 0 &&
    ch.engagement <= 100
  );

  // Sort by engagement and take top 10
  const topChannels = [...validChannels]
    .sort((a, b) => b.engagement - a.engagement)
    .slice(0, 10);

  return (
    <div className="bg-white rounded-xl border border-gray-200 p-6">
      <div className="flex items-center justify-between mb-1">
        <h3 className="text-gray-900" style={{ fontSize: '1.05rem' }}>
          {ru ? 'Топ-каналы сообщества' : 'Top Community Channels'}
        </h3>
        <span className="text-xs text-gray-500">{topChannels.length} {ru ? 'активных групп' : 'active groups'}</span>
      </div>
      <p className="text-xs text-gray-500 mb-4">
        {ru
          ? 'Где живёт сообщество — следите за этими каналами, чтобы чувствовать его пульс'
          : 'Where the community lives — monitor these to understand the pulse'}
      </p>

      <div className="space-y-2">
        {topChannels.map((ch, i) => (
          <div key={ch.name} className="flex items-center gap-3 py-2 px-2 rounded-lg hover:bg-gray-50 transition-colors">
            <div className="w-8 h-8 rounded-full flex items-center justify-center flex-shrink-0"
              style={{ backgroundColor: typeColors[ch.type] + '20' }}>
              <span className="text-xs" style={{ fontWeight: 700, color: typeColors[ch.type] }}>{i + 1}</span>
            </div>
            <div className="flex-1 min-w-0">
              <div className="flex items-center gap-2">
                <span className="text-xs text-gray-900 truncate" style={{ fontWeight: 500 }}>{ch.name}</span>
                <span className="text-xs px-1.5 py-0.5 rounded" style={{
                  backgroundColor: typeColors[ch.type] + '15',
                  color: typeColors[ch.type], fontWeight: 500,
                }}>{labels[ch.type as keyof typeof labels]}</span>
              </div>
              <div className="flex items-center gap-3 mt-0.5 text-xs text-gray-400">
                <span>{ch.members >= 1000 ? `${(ch.members / 1000).toFixed(1)}K` : ch.members} {ru ? 'уч.' : 'members'}</span>
                <span>{ch.dailyMessages > 0 ? `${ch.dailyMessages}/${ru ? 'день' : 'day'}` : ru ? 'Неактивен' : 'Inactive'}</span>
                {ch.growth !== 0 && (
                  <span className={ch.growth > 0 ? 'text-emerald-500' : 'text-red-500'}>
                    {ch.growth > 0 ? '+' : ''}{ch.growth.toFixed(1)}%
                  </span>
                )}
              </div>
            </div>
            <div className="text-right flex-shrink-0">
              <div className="text-xs text-gray-900" style={{ fontWeight: 600 }}>{ch.engagement}%</div>
              <div className="text-xs text-gray-400">{ru ? 'вовл.' : 'engage'}</div>
            </div>
          </div>
        ))}
      </div>

      <div className="pt-3 mt-3 border-t border-gray-100 flex justify-end">
        <Link to="/channels" className="ml-auto text-xs text-blue-600 hover:text-blue-800 hover:underline transition-colors" style={{ fontWeight: 500 }}>
          {ru ? 'Все каналы →' : 'See all channels →'}
        </Link>
      </div>
    </div>
  );
}


// ============================================================
// W13: KEY VOICES
// ============================================================

export function KeyVoices() {
  const { lang } = useLanguage();
  const { data } = useData();
  const { range } = useDashboardDateRange();
  const ru = lang === 'ru';
  const keyVoices = data.keyVoices[lang] ?? [];

  if (!keyVoices.length) return <EmptyWidget title={ru ? 'Ключевые голоса сообщества' : 'Key Community Voices'} />;

  return (
    <div className="bg-white rounded-xl border border-gray-200 p-6">
      <div className="flex items-center justify-between mb-1">
        <h3 className="text-gray-900" style={{ fontSize: '1.05rem' }}>
          {ru ? 'Ключевые голоса сообщества' : 'Key Community Voices'}
        </h3>
        <span className="text-xs text-gray-500">
          {ru ? `Активные комментаторы за ${range.days} дн.` : `Active commenters in the selected ${range.days}-day window`}
        </span>
      </div>
      <p className="text-xs text-gray-500 mb-4">
        {ru
          ? 'Участники, которые чаще других появляются в обсуждениях в выбранном окне'
          : 'Participants who appear most often in discussions during the selected window'}
      </p>

      <div className="space-y-3">
        {keyVoices.map((voice) => (
          <div key={voice.name} className="bg-gray-50 rounded-lg p-3">
            <div className="flex items-center gap-3 mb-2">
              <div className="w-10 h-10 rounded-full bg-gradient-to-br from-blue-400 to-blue-600 flex items-center justify-center flex-shrink-0">
                <span className="text-white text-sm" style={{ fontWeight: 600 }}>{voice.name.charAt(0)}</span>
              </div>
              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-2 flex-wrap">
                  <span className="text-xs text-gray-900" style={{ fontWeight: 600 }}>{voice.name}</span>
                </div>
                <span className="text-xs text-gray-500">{voice.role}</span>
              </div>
              <div className="text-right flex-shrink-0">
                <span className="text-xs text-gray-900 block" style={{ fontWeight: 600 }}>{voice.postsPerWeek}</span>
                <span className="text-xs text-gray-400">{ru ? 'комм./нед.' : 'comments/wk'}</span>
              </div>
            </div>

            <div className="flex items-center gap-2 mb-1.5 flex-wrap">
              {voice.topics.map((topic) => (
                <span key={topic} className="text-xs bg-white border border-gray-200 rounded px-1.5 py-0.5 text-gray-600">{topic}</span>
              ))}
            </div>

            <div className="flex items-center gap-4 text-xs text-gray-400 flex-wrap">
              <span>{ru ? 'Участие в ответах:' : 'Reply participation:'} <span className="text-gray-700" style={{ fontWeight: 600 }}>{voice.replyRate}%</span></span>
              {voice.topChannels && voice.topChannels.length > 0 && (
                <span>
                  {ru ? 'Активен в:' : 'Active in:'} <span className="text-gray-700" style={{ fontWeight: 600 }}>{voice.topChannels.join(', ')}</span>
                </span>
              )}
            </div>
          </div>
        ))}
      </div>

      <div className="pt-3 mt-3 border-t border-gray-100 flex justify-end">
        <Link to="/audience" className="text-xs text-blue-600 hover:text-blue-800 hover:underline transition-colors" style={{ fontWeight: 500 }}>
          {ru ? 'Все участники →' : 'See all voices →'}
        </Link>
      </div>
    </div>
  );
}


// ============================================================
// W23: COMMUNITY ACTIVITY TIMELINE
// ============================================================

export function ActivityTimeline() {
  const { lang } = useLanguage();
  const { data } = useData();
  const ru = lang === 'ru';
  const [view, setView] = useState<'hourly' | 'weekly'>('hourly');
  const hourlyActivity = data.hourlyActivity;
  const weeklyActivity = data.weeklyActivity;

  if (!hourlyActivity.length && !weeklyActivity.length) return <EmptyWidget title={ru ? 'Паттерны активности' : 'Activity Patterns'} />;

  // Compute peak hour and best day dynamically
  const peakHourItem = hourlyActivity.length ? hourlyActivity.reduce((max, h) => h.messages > max.messages ? h : max, hourlyActivity[0]) : null;
  const bestDayItem = weeklyActivity.length ? weeklyActivity.reduce((max, d) => d.messages > max.messages ? d : max, weeklyActivity[0]) : null;
  // Compute maxVal dynamically
  const hourlyMax = hourlyActivity.length ? Math.max(...hourlyActivity.map(h => h.messages)) : 520;
  const weeklyMax = weeklyActivity.length ? Math.max(...weeklyActivity.map(d => d.messages)) : 4200;

  return (
    <div className="bg-white rounded-xl border border-gray-200 p-6">
      <div className="flex items-center justify-between mb-1">
        <h3 className="text-gray-900" style={{ fontSize: '1.05rem' }}>
          {ru ? 'Паттерны активности' : 'Activity Patterns'}
        </h3>
        <div className="flex gap-1">
          <button onClick={() => setView('hourly')}
            className={`text-xs px-2.5 py-1 rounded-full ${view === 'hourly' ? 'bg-slate-800 text-white' : 'bg-gray-100 text-gray-500'}`}>
            {ru ? 'По часам' : 'By Hour'}
          </button>
          <button onClick={() => setView('weekly')}
            className={`text-xs px-2.5 py-1 rounded-full ${view === 'weekly' ? 'bg-slate-800 text-white' : 'bg-gray-100 text-gray-500'}`}>
            {ru ? 'По дням' : 'By Day'}
          </button>
        </div>
      </div>
      <p className="text-xs text-gray-500 mb-4">
        {ru ? 'Когда публиковать для максимального охвата? Следуйте ритму сообщества.' : 'When to post for maximum engagement? Follow the rhythm.'}
      </p>

      <div className="flex items-end gap-1 h-36">
        {(view === 'hourly' ? hourlyActivity : weeklyActivity).map((item, i) => {
          const maxVal = view === 'hourly' ? hourlyMax : weeklyMax;
          const msgs = item.messages;
          const height = (msgs / maxVal) * 100;
          const isHot = msgs > maxVal * 0.8;
          const label = view === 'hourly' ? (item as typeof hourlyActivity[0]).hour : (ru ? (item as typeof weeklyActivity[0]).day : (item as typeof weeklyActivity[0]).dayEN);
          return (
            <div key={i} className="flex-1 flex flex-col items-center gap-1">
              <span className="text-xs text-gray-500" style={{ fontSize: '8px' }}>{msgs}</span>
              <div className="w-full rounded-t-sm transition-all" style={{ height: `${height}%`, backgroundColor: isHot ? '#0d9488' : '#99f6e4' }} />
              <span className="text-xs text-gray-400" style={{ fontSize: view === 'hourly' ? '8px' : '10px' }}>{label}</span>
            </div>
          );
        })}
      </div>

      <div className="mt-3 pt-3 border-t border-gray-100 grid grid-cols-2 gap-3">
        {peakHourItem && (
          <div className="flex items-center gap-2">
            <Clock className="w-3.5 h-3.5 text-blue-600" />
            <span className="text-xs text-gray-600">
              {ru ? 'Пик:' : 'Peak:'} <span style={{ fontWeight: 600 }} className="text-gray-900">
                {peakHourItem.hour}
              </span>
            </span>
          </div>
        )}
        {bestDayItem && (
          <div className="flex items-center gap-2">
            <TrendingUp className="w-3.5 h-3.5 text-blue-600" />
            <span className="text-xs text-gray-600">
              {ru ? 'Лучший день:' : 'Best day:'} <span style={{ fontWeight: 600 }} className="text-gray-900">
                {ru ? bestDayItem.day : bestDayItem.dayEN}
              </span>
            </span>
          </div>
        )}
      </div>
    </div>
  );
}


// ============================================================
// W24: RECOMMENDATION SHARING TRACKER
// ============================================================

export function RecommendationTracker() {
  const { lang } = useLanguage();
  const { data } = useData();
  const ru = lang === 'ru';
  const recommendations = data.recommendations[lang] ?? [];

  if (!recommendations.length) return <EmptyWidget title={ru ? 'Рекомендации сообщества' : 'Community Recommendations'} />;

  // ✅ GENERIC: compute top 2 categories by mention count dynamically
  const catTotals: Record<string, number> = {};
  recommendations.forEach(r => { catTotals[r.category] = (catTotals[r.category] ?? 0) + r.mentions; });
  const topCats = Object.entries(catTotals).sort((a, b) => b[1] - a[1]).slice(0, 2).map(([cat]) => cat);

  return (
    <div className="bg-white rounded-xl border border-gray-200 p-6">
      <div className="flex items-center justify-between mb-1">
        <h3 className="text-gray-900" style={{ fontSize: '1.05rem' }}>
          {ru ? 'Рекомендации сообщества' : 'Community Recommendations'}
        </h3>
        <span className="text-xs text-gray-500">{ru ? 'Самые популярные советы' : 'Most shared suggestions'}</span>
      </div>
      <p className="text-xs text-gray-500 mb-4">
        {ru
          ? 'Что люди советуют новичкам — органические сигналы доверия'
          : 'What people recommend to newcomers — organic trust signals'}
      </p>

      <div className="space-y-2">
        {recommendations.map((rec) => (
          <div key={rec.item} className="flex items-center gap-3 py-1.5 px-2 rounded-lg hover:bg-gray-50 transition-colors">
            <div className={`w-7 h-7 rounded-full flex items-center justify-center flex-shrink-0 ${rec.sentiment === 'positive' ? 'bg-emerald-100' : 'bg-amber-100'}`}>
              <Star className={`w-3.5 h-3.5 ${rec.sentiment === 'positive' ? 'text-emerald-600' : 'text-amber-600'}`} />
            </div>
            <div className="flex-1 min-w-0">
              <span className="text-xs text-gray-900 truncate block" style={{ fontWeight: 500 }}>{rec.item}</span>
              <span className="text-xs text-gray-400">{rec.category}</span>
            </div>
            <div className="flex items-center gap-1 flex-shrink-0">
              <Star className="w-3 h-3 text-amber-400 fill-amber-400" />
              <span className="text-xs text-gray-900" style={{ fontWeight: 600 }}>{rec.rating}</span>
            </div>
            <span className="text-xs text-gray-500 w-20 text-right">
              {rec.mentions}x {ru ? 'упомянуто' : 'shared'}
            </span>
          </div>
        ))}
      </div>

      <div className="mt-3 pt-3 border-t border-gray-100">
        <div className="bg-teal-50 border border-teal-100 rounded-lg px-3 py-2">
          <p className="text-xs text-teal-800">
            {ru
              ? <><span style={{ fontWeight: 600 }}>Инсайт:</span> {topCats.join(' и ')} получают больше всего органических рекомендаций. Рассмотрите создание курируемого каталога «Одобрено сообществом» на основе этих данных.</>
              : <><span style={{ fontWeight: 600 }}>Insight:</span> {topCats.join(' & ')} get the most organic recommendations. Consider creating a curated &quot;Community Approved&quot; directory from these signals.</>
            }
          </p>
        </div>
      </div>
    </div>
  );
}


// ============================================================
// W25: NEWCOMER QUESTIONS FLOW
// ============================================================

export function NewcomerFlow() {
  const { lang } = useLanguage();
  const { data } = useData();
  const ru = lang === 'ru';
  const newcomerJourney = data.newcomerJourney[lang] ?? [];

  if (!newcomerJourney.length) return <EmptyWidget title={ru ? 'Карта пути новичка' : 'Newcomer Journey Map'} />;

  // ✅ GENERIC: find the stage with the worst answer rate dynamically
  const worstStage = [...newcomerJourney].sort((a, b) => a.resolved - b.resolved)[0];

  return (
    <div className="bg-white rounded-xl border border-gray-200 p-6">
      <div className="flex items-center justify-between mb-1">
        <h3 className="text-gray-900" style={{ fontSize: '1.05rem' }}>
          {ru ? 'Карта пути новичка' : 'Newcomer Journey Map'}
        </h3>
        <span className="text-xs text-gray-500">{ru ? 'Воронка адаптации' : 'Onboarding funnel'}</span>
      </div>
      <p className="text-xs text-gray-500 mb-4">
        {ru
          ? 'Какие вопросы возникают на каждом этапе? Создайте гайды для каждой фазы.'
          : 'What questions arise at each stage? Build guides for each phase.'}
      </p>

      <div className="relative">
        <div className="absolute left-4 top-0 bottom-0 w-0.5 bg-blue-200" />
        <div className="space-y-4">
          {newcomerJourney.map((stage) => (
            <div key={stage.stage} className="relative pl-10">
              <div className="absolute left-2.5 w-3 h-3 rounded-full border-2 border-white bg-blue-500" />
              <div className="bg-gray-50 rounded-lg p-3">
                <div className="flex items-center justify-between mb-2">
                  <span className="text-xs text-gray-900" style={{ fontWeight: 600 }}>{stage.stage}</span>
                  <div className="flex items-center gap-2">
                    <span className="text-xs text-gray-500">{stage.volume} {ru ? 'вопросов' : 'questions'}</span>
                    <span className={`text-xs px-1.5 py-0.5 rounded ${stage.resolved >= 70 ? 'bg-emerald-100 text-emerald-700' : stage.resolved >= 50 ? 'bg-amber-100 text-amber-700' : 'bg-red-100 text-red-700'}`}>
                      {stage.resolved}% {ru ? 'отвечено' : 'answered'}
                    </span>
                  </div>
                </div>
                <div className="flex flex-wrap gap-1">
                  {stage.questions.map((q) => (
                    <span key={q} className="text-xs bg-white border border-gray-200 rounded px-2 py-0.5 text-gray-600">{q}</span>
                  ))}
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>

      <div className="mt-3 bg-amber-50 border border-amber-100 rounded-lg px-3 py-2">
        <p className="text-xs text-amber-800">
          {worstStage && (ru
            ? <><span style={{ fontWeight: 600 }}>Пробел в контенте:</span> Этап «{worstStage.stage}» отвечает только на {worstStage.resolved}% вопросов. Именно эти люди решают — остаться или уехать. Им нужна максимальная поддержка.</>
            : <><span style={{ fontWeight: 600 }}>Gap alert:</span> &quot;{worstStage.stage}&quot; only has a {worstStage.resolved}% answer rate. These are the people deciding whether to stay or leave — they need the most support.</>
          )}
        </p>
      </div>
    </div>
  );
}


// ============================================================
// INFORMATION VELOCITY & VIRALITY
// ============================================================

const velocityConfig = {
  en: {
    explosive: { bg: 'bg-red-50', text: 'text-red-700', border: 'border-red-200', label: 'Explosive' },
    fast: { bg: 'bg-amber-50', text: 'text-amber-700', border: 'border-amber-200', label: 'Fast' },
    normal: { bg: 'bg-gray-50', text: 'text-gray-600', border: 'border-gray-200', label: 'Normal' },
  },
  ru: {
    explosive: { bg: 'bg-red-50', text: 'text-red-700', border: 'border-red-200', label: 'Взрывной' },
    fast: { bg: 'bg-amber-50', text: 'text-amber-700', border: 'border-amber-200', label: 'Быстрый' },
    normal: { bg: 'bg-gray-50', text: 'text-gray-600', border: 'border-gray-200', label: 'Обычный' },
  },
};

export function InformationVelocity() {
  const { lang } = useLanguage();
  const { data } = useData();
  const ru = lang === 'ru';
  const viralTopics = data.viralTopics[lang] ?? [];

  if (!viralTopics.length) return <EmptyWidget title={ru ? 'Скорость распространения информации' : 'Information Velocity'} />;

  const velConfig = velocityConfig[lang];

  // ✅ GENERIC: find the amplifier channel that appears most across explosive topics
  const explosiveTopics = viralTopics.filter(t => t.velocity === 'explosive');
  const amplifierCount: Record<string, number> = {};
  explosiveTopics.forEach(t => t.amplifiers.forEach(a => { amplifierCount[a] = (amplifierCount[a] ?? 0) + 1; }));
  const topAmplifier = Object.entries(amplifierCount).sort((a, b) => b[1] - a[1])[0];
  const topAmplifierName = topAmplifier?.[0] ?? '';
  const topAmplifierPct = explosiveTopics.length > 0 && topAmplifier
    ? Math.round((topAmplifier[1] / explosiveTopics.length) * 100)
    : 0;

  return (
    <div className="bg-white rounded-xl border border-gray-200 p-6">
      <div className="flex items-center justify-between mb-1">
        <h3 className="text-gray-900" style={{ fontSize: '1.05rem' }}>
          {ru ? 'Скорость распространения информации' : 'Information Velocity'}
        </h3>
        <span className="text-xs text-gray-500">
          {ru ? 'Как быстро темы распространяются по каналам' : 'How fast topics spread across channels'}
        </span>
      </div>
      <p className="text-xs text-gray-500 mb-4">
        {ru
          ? 'Отслеживает источники нарративов и их распространение. Источник = откуда пришло; Усилители = кто разнёс дальше.'
          : 'Tracks where narratives are born and how they travel. Originator = where it started; Amplifiers = who spread it furthest.'}
      </p>

      <div className="space-y-3">
        {viralTopics.map((item) => {
          const vel = velConfig[item.velocity as keyof typeof velConfig];
          return (
            <div key={item.topic} className={`${vel.bg} border ${vel.border} rounded-lg p-3`}>
              <div className="flex items-start justify-between mb-2">
                <span className="text-xs text-gray-900" style={{ fontWeight: 600 }}>{item.topic}</span>
                <span className={`text-xs px-2 py-0.5 rounded-full ${vel.bg} ${vel.text} border ${vel.border} flex-shrink-0 ml-2`} style={{ fontWeight: 500 }}>
                  {vel.label}
                </span>
              </div>
              <div className="flex items-center gap-4 text-xs mb-2 flex-wrap">
                <div className="flex items-center gap-1">
                  <span className="text-gray-400">{ru ? 'Источник:' : 'Origin:'}</span>
                  <span className="text-gray-900" style={{ fontWeight: 500 }}>{item.originator}</span>
                </div>
                <div className="flex items-center gap-1">
                  <span className="text-gray-400">{ru ? 'Распр. за:' : 'Spread in:'}</span>
                  <span className="text-gray-900" style={{ fontWeight: 600 }}>{item.spreadHours}{ru ? ' ч' : 'h'}</span>
                </div>
                <div className="flex items-center gap-1">
                  <span className="text-gray-400">{ru ? 'Каналов:' : 'Channels:'}</span>
                  <span className="text-gray-900" style={{ fontWeight: 600 }}>{item.channelsReached}</span>
                </div>
                <span className="text-gray-500 ml-auto">{(item.totalReach / 1000).toFixed(1)}K {ru ? 'охват' : 'reach'}</span>
              </div>
              <div className="flex items-center gap-1 flex-wrap">
                <span className="text-xs text-gray-400">{ru ? 'Усилено через:' : 'Amplified by:'}</span>
                {item.amplifiers.map((amp) => (
                  <span key={amp} className="text-xs bg-white border border-gray-200 rounded px-1.5 py-0.5 text-gray-600">{amp}</span>
                ))}
              </div>
            </div>
          );
        })}
      </div>

      <div className="mt-3 pt-3 border-t border-gray-100">
        <div className="bg-teal-50 border border-teal-100 rounded-lg px-3 py-2">
          <p className="text-xs text-teal-800">
            {topAmplifierName ? (ru
              ? <><span style={{ fontWeight: 600 }}>Ключевой инсайт:</span> «{topAmplifierName}» усиливает {topAmplifierPct}% взрывных тем — это главный вещательный канал сообщества. Публикация там гарантирует максимальный охват в течение нескольких часов.</>
              : <><span style={{ fontWeight: 600 }}>Key insight:</span> &ldquo;{topAmplifierName}&rdquo; amplifies {topAmplifierPct}% of explosive topics — it is the community&apos;s main broadcast channel. Publishing there guarantees maximum reach within hours.</>
            ) : null}
          </p>
        </div>
      </div>
    </div>
  );
}
