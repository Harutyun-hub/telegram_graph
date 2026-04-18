import { type ReactNode, useEffect, useMemo, useState } from 'react';
import {
  AlertTriangle,
  Star,
  TrendingUp,
  User,
} from 'lucide-react';
import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  CartesianGrid,
  PolarAngleAxis,
  PolarGrid,
  PolarRadiusAxis,
  Radar,
  RadarChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import type { AdminWidgetId } from '@/app/admin/catalog';
import { WidgetTitle } from '@/app/components/ui/WidgetTitle';

type Lang = 'en' | 'ru';

function dashboardCard(widgetId: AdminWidgetId, title: string, subtitle: string, right?: string) {
  return { widgetId, title, subtitle, right };
}

function PlaceholderCard({
  widgetId,
  title,
  subtitle,
  right,
  children,
}: {
  widgetId: Parameters<typeof WidgetTitle>[0]['widgetId'];
  title: string;
  subtitle: string;
  right?: string;
  children: ReactNode;
}) {
  return (
    <div className="bg-white rounded-xl border border-gray-200 p-6">
      <div className="mb-1 flex flex-col items-start gap-2 sm:flex-row sm:items-start sm:justify-between">
        <WidgetTitle widgetId={widgetId}>{title}</WidgetTitle>
        {right ? (
          <span className="w-full text-left text-xs text-gray-500 sm:w-auto sm:text-right">{right}</span>
        ) : null}
      </div>
      <p className="text-xs text-gray-500 mb-4">{subtitle}</p>
      {children}
    </div>
  );
}

function SocialPendingNote({ lang, text }: { lang: Lang; text?: string }) {
  const ru = lang === 'ru';
  return (
    <div className="mt-3 rounded-lg border border-slate-200 bg-slate-50 px-3 py-2">
      <p className="text-xs text-slate-700">
        <span style={{ fontWeight: 600 }}>{ru ? 'Social note:' : 'Social note:'}</span>{' '}
        {text ?? (ru
          ? 'Эта карточка уже повторяет Telegram-дизайн, но ждёт отдельный social read model для заполнения реальными метриками.'
          : 'This card now mirrors the Telegram design, but it is still waiting for a dedicated social read model before real metrics can populate it.')}
      </p>
    </div>
  );
}

function SkeletonLine({ width, className = '' }: { width: string; className?: string }) {
  return <div className={`h-2 rounded-full bg-gray-200 ${className}`} style={{ width }} />;
}

function selectedWindowLabel(days: number, ru: boolean): string {
  if (days === 1) return ru ? '1 день' : '1-day window';
  return ru ? `${days} дней` : `${days}-day window`;
}

export function SocialCommunityHealthPlaceholder({ lang }: { lang: Lang }) {
  const ru = lang === 'ru';
  const [score, setScore] = useState(0);

  useEffect(() => {
    const timer = window.setTimeout(() => setScore(58), 100);
    return () => window.clearTimeout(timer);
  }, []);

  const circumference = 2 * Math.PI * 54;
  const strokeDashoffset = circumference - (score / 100) * circumference;

  return (
    <PlaceholderCard
      {...dashboardCard(
        'community_health_score',
        ru ? 'Климат сообщества' : 'Community Climate',
        ru ? 'Объяснимый индекс по интенту, тону и разнообразию тем' : 'Explainable index from intent, tone, and topic diversity',
        ru ? 'Social pending' : 'Social pending',
      )}
    >
      <div className="flex items-center justify-between mb-4">
        <span className="text-xs px-2.5 py-1 rounded-full bg-blue-50 text-blue-700">{ru ? 'Ожидает модель' : 'Model pending'}</span>
      </div>
      <div className="flex items-center gap-6">
        <div className="relative flex-shrink-0">
          <svg width="128" height="128" viewBox="0 0 128 128">
            <circle cx="64" cy="64" r="54" stroke="#f3f4f6" strokeWidth="8" fill="none" />
            <circle
              cx="64"
              cy="64"
              r="54"
              stroke="#3b82f6"
              strokeWidth="8"
              fill="none"
              strokeLinecap="round"
              strokeDasharray={circumference}
              strokeDashoffset={strokeDashoffset}
              transform="rotate(-90 64 64)"
              style={{ transition: 'stroke-dashoffset 1.5s ease-out' }}
            />
          </svg>
          <div className="absolute inset-0 flex flex-col items-center justify-center">
            <span className="text-3xl text-gray-900" style={{ fontWeight: 600 }}>--</span>
            <span className="text-xs text-gray-500">/100</span>
          </div>
        </div>
        <div className="flex-1 space-y-3">
          {[
            ru ? 'Доля конструктивных ответов' : 'Constructive reply share',
            ru ? 'Стабильность тона' : 'Tone stability',
            ru ? 'Разнообразие тем' : 'Topic diversity',
            ru ? 'Повторное участие' : 'Repeat participation',
          ].map((label, index) => (
            <div key={label}>
              <div className="flex items-center justify-between mb-1">
                <span className="text-xs text-gray-600">{label}</span>
                <span className="text-xs text-gray-400">--</span>
              </div>
              <div className="w-full bg-gray-100 rounded-full h-1.5">
                <div className="h-1.5 rounded-full bg-gray-300" style={{ width: `${70 - index * 12}%` }} />
              </div>
            </div>
          ))}
        </div>
      </div>
      <div className="mt-4 pt-3 border-t border-gray-100 flex items-center justify-between">
        <div className="flex items-center gap-1.5 text-xs text-gray-500">
          <TrendingUp className="w-3.5 h-3.5 text-blue-500" />
          <span>{ru ? 'Сигналы появятся после social scoring' : 'Signals appear once social scoring is ready'}</span>
        </div>
        <div className="flex items-end gap-1">
          {[20, 28, 24, 34, 31, 39].map((value, index) => (
            <div key={index} className="w-2 rounded-sm bg-blue-300/70" style={{ height: `${value}px` }} />
          ))}
        </div>
      </div>
    </PlaceholderCard>
  );
}

export function SocialQuestionCloudPlaceholder({ lang }: { lang: Lang }) {
  const ru = lang === 'ru';
  const cards = [
    { label: ru ? 'Нужен гайд' : 'Needs guide', tone: 'bg-amber-50 border-amber-200 text-amber-900' },
    { label: ru ? 'Частично покрыто' : 'Partially covered', tone: 'bg-blue-50 border-blue-200 text-blue-900' },
    { label: ru ? 'Ожидает сигналов' : 'Awaiting signals', tone: 'bg-emerald-50 border-emerald-200 text-emerald-900' },
    { label: ru ? 'Ожидает сигналов' : 'Awaiting signals', tone: 'bg-blue-50 border-blue-200 text-blue-900' },
  ];

  return (
    <PlaceholderCard
      {...dashboardCard(
        'question_cloud',
        ru ? 'Самые частые вопросы' : 'Most Asked Questions',
        ru ? 'Показываем AI-сводку вопросов: похожие запросы объединяются и привязываются к реальным сообщениям.' : 'Shows the AI question overview: similar asks are grouped and tied back to real source messages.',
        ru ? 'AI + доказательства' : 'AI + evidence grounded',
      )}
    >
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-2.5">
        {cards.map((card, index) => (
          <div key={index} className={`rounded-xl border p-3 ${card.tone}`}>
            <div className="flex items-start justify-between gap-2">
              <div className="min-w-0 flex-1">
                <div className="text-[11px] opacity-80">{ru ? 'Social AI pending' : 'Social AI pending'}</div>
                <div className="mt-1 space-y-2">
                  <SkeletonLine width="84%" />
                  <SkeletonLine width="68%" />
                </div>
              </div>
              <span className="text-[10px] px-1.5 py-0.5 rounded border border-current/20">{card.label}</span>
            </div>
            <div className="mt-3 space-y-2 opacity-80">
              <SkeletonLine width="100%" />
              <SkeletonLine width="92%" />
              <SkeletonLine width="72%" />
            </div>
            <div className="mt-3 flex items-center gap-3 text-[11px] opacity-80">
              <span>-- {ru ? 'сигналов' : 'signals'}</span>
              <span>-- {ru ? 'людей' : 'people'}</span>
              <span>-- {ru ? 'каналов' : 'channels'}</span>
            </div>
          </div>
        ))}
      </div>
      <div className="mt-3 pt-3 border-t border-gray-100 flex items-center gap-3 flex-wrap">
        {[
          ru ? 'Нужен чёткий ответ/гайд' : 'Needs clear response/guide',
          ru ? 'Частично покрыто' : 'Partially covered',
          ru ? 'В целом покрыто' : 'Mostly covered',
        ].map((item, index) => (
          <div key={item} className="flex items-center gap-1.5">
            <div className={`w-3 h-3 rounded ${index === 0 ? 'bg-amber-50 border border-amber-200' : index === 1 ? 'bg-blue-50 border border-blue-200' : 'bg-emerald-50 border border-emerald-200'}`} />
            <span className="text-xs text-gray-500">{item}</span>
          </div>
        ))}
      </div>
      <SocialPendingNote lang={lang} />
    </PlaceholderCard>
  );
}

export function SocialTopicLifecyclePlaceholder({ lang, rangeDays }: { lang: Lang; rangeDays: number }) {
  const ru = lang === 'ru';
  const [expandedKey, setExpandedKey] = useState('growing-0');
  const stages = [
    { stage: ru ? 'Растёт' : 'Growing', desc: ru ? 'Интерес ускоряется' : 'Interest is accelerating', color: '#10b981', bg: 'bg-emerald-50', border: 'border-emerald-200', text: 'text-emerald-700', topics: [ru ? 'Тема social 1' : 'Social topic 1', ru ? 'Тема social 2' : 'Social topic 2'] },
    { stage: ru ? 'Стабилизировалась' : 'Stable', desc: ru ? 'Разговор держится' : 'Discussion holds', color: '#3b82f6', bg: 'bg-blue-50', border: 'border-blue-200', text: 'text-blue-700', topics: [ru ? 'Тема social 3' : 'Social topic 3'] },
    { stage: ru ? 'Снижается' : 'Declining', desc: ru ? 'Внимание уходит' : 'Attention is fading', color: '#f59e0b', bg: 'bg-amber-50', border: 'border-amber-200', text: 'text-amber-700', topics: [ru ? 'Тема social 4' : 'Social topic 4'] },
    { stage: ru ? 'Наблюдать' : 'Watch', desc: ru ? 'Нужно больше данных' : 'Need more data', color: '#94a3b8', bg: 'bg-slate-50', border: 'border-slate-200', text: 'text-slate-700', topics: [ru ? 'Тема social 5' : 'Social topic 5'] },
  ];

  return (
    <PlaceholderCard
      {...dashboardCard(
        'topic_lifecycle',
        ru ? 'Жизненный цикл тем' : 'Topic Lifecycle',
        ru ? `Показывает, где внимание к теме растёт, а где снижается, на основе сигналов в выбранном окне (${rangeDays} дн.).` : `Shows where attention is growing and where it is declining in the selected ${rangeDays}-day window.`,
        ru ? 'Рост -> Снижение' : 'Growing -> Declining',
      )}
    >
      <p className="text-xs text-gray-400 mb-4">
        {ru ? `X/7д — объём за последние 7 дней; Δ — изменение к предыдущим 7 дням; д. — сколько дней тема была активна.` : 'X/7d = last 7 days volume; Δ = change vs previous 7 days; d = days active in the window.'}
      </p>
      <div className="grid grid-cols-2 gap-1 mb-4">
        {stages.map((stage) => (
          <div key={stage.stage} className={`${stage.bg} border ${stage.border} rounded-lg px-2 py-1.5 text-center`}>
            <div className="w-2 h-2 rounded-full mx-auto mb-1" style={{ backgroundColor: stage.color }} />
            <span className={`text-xs block ${stage.text}`} style={{ fontWeight: 600 }}>{stage.stage}</span>
            <span className="text-gray-400" style={{ fontSize: '9px' }}>{stage.desc}</span>
          </div>
        ))}
      </div>
      <div className="space-y-3">
        {stages.map((stage, groupIndex) => (
          <div key={stage.stage}>
            <div className="flex items-center gap-2 mb-1.5">
              <div className="w-2 h-2 rounded-full" style={{ backgroundColor: stage.color }} />
              <span className="text-xs text-gray-900" style={{ fontWeight: 600 }}>{stage.stage}</span>
              <span className={`text-xs ${stage.text}`} style={{ fontSize: '10px' }}>({stage.topics.length})</span>
            </div>
            <div className="space-y-1.5 pl-4">
              {stage.topics.map((topic, topicIndex) => {
                const key = `${groupIndex}-${topicIndex}`;
                const expanded = expandedKey === key;
                return (
                  <div key={key}>
                    <button
                      type="button"
                      onClick={() => setExpandedKey(expanded ? '' : key)}
                      className={`w-full flex items-center justify-between px-3 py-2 rounded-lg ${stage.bg} border ${stage.border}`}
                    >
                      <span className="text-xs text-gray-900 text-left" style={{ fontWeight: 500 }}>{topic}</span>
                      <div className="flex items-center gap-3 text-xs">
                        <span className="text-gray-400">-- /7d</span>
                        <span className="text-gray-500">Δ --</span>
                        <span className="text-gray-400 w-8 text-right">--d</span>
                        <span className="text-gray-400 w-4 text-right" style={{ fontWeight: 600 }}>{expanded ? '−' : '+'}</span>
                      </div>
                    </button>
                    {expanded ? (
                      <div className="mt-1.5 ml-2 rounded-lg border border-gray-200 bg-white px-3 py-2">
                        <div className="space-y-2">
                          <SkeletonLine width="100%" />
                          <SkeletonLine width="88%" />
                        </div>
                        <div className="mt-2 flex flex-wrap gap-1.5">
                          <span className="text-[11px] text-gray-500">{ru ? 'Платформы:' : 'Platforms:'}</span>
                          {['Facebook', 'Instagram', ru ? 'Комментарии' : 'Comments'].map((chip) => (
                            <span key={chip} className="text-[11px] px-2 py-0.5 rounded-full bg-gray-100 text-gray-600">{chip}</span>
                          ))}
                        </div>
                        <div className="mt-2 space-y-1.5">
                          {[0, 1].map((row) => (
                            <div key={row} className="text-[11px] text-gray-600 rounded border border-gray-100 bg-gray-50 px-2 py-1.5">
                              <div className="text-gray-700" style={{ fontWeight: 500 }}>{ru ? 'Источник' : 'Source'}</div>
                              <SkeletonLine width={row === 0 ? '96%' : '80%'} className="mt-1" />
                              <SkeletonLine width={row === 0 ? '74%' : '62%'} className="mt-1" />
                            </div>
                          ))}
                        </div>
                      </div>
                    ) : null}
                  </div>
                );
              })}
            </div>
          </div>
        ))}
      </div>
      <SocialPendingNote lang={lang} />
    </PlaceholderCard>
  );
}

export function SocialServiceGapPlaceholder({ lang }: { lang: Lang }) {
  const ru = lang === 'ru';
  const rows = [
    { name: ru ? 'Поддержка клиентов' : 'Customer support', gap: 82, tone: 'bg-red-500', supply: ru ? 'Очень низко' : 'Very low' },
    { name: ru ? 'Контент по продукту' : 'Product content', gap: 66, tone: 'bg-orange-500', supply: ru ? 'Низко' : 'Low' },
    { name: ru ? 'Путь к покупке' : 'Purchase guidance', gap: 48, tone: 'bg-amber-500', supply: ru ? 'Средне' : 'Moderate' },
  ];

  return (
    <PlaceholderCard
      {...dashboardCard(
        'service_gap_detector',
        ru ? 'Детектор пробелов в услугах' : 'Service Gap Detector',
        ru ? 'Показываем только AI-сформулированные сервисные пробелы, подтверждённые реальными сообщениями.' : 'Shows only AI-generated service gaps grounded in real messages.',
      )}
    >
      <div className="space-y-2.5">
        {rows.map((row) => (
          <div key={row.name} className="space-y-1.5">
            <div className="flex items-center gap-3">
              <div className="flex-1 min-w-0">
                <div className="flex items-center justify-between gap-2 mb-1">
                  <div className="flex items-center gap-2 min-w-0">
                    <span className="text-xs text-gray-900 truncate" style={{ fontWeight: 500 }}>{row.name}</span>
                    <span className="text-xs px-1.5 py-0.5 rounded flex-shrink-0 bg-slate-100 text-slate-700">{row.supply}</span>
                  </div>
                  <span className="text-[11px] text-blue-700">{ru ? 'Открыть →' : 'Open →'}</span>
                </div>
                <div className="w-full bg-gray-100 rounded-full h-2">
                  <div className={`h-2 rounded-full ${row.tone}`} style={{ width: `${row.gap}%` }} />
                </div>
              </div>
              <div className="text-right flex-shrink-0 w-20">
                <span className="text-xs text-gray-900 block" style={{ fontWeight: 600 }}>-- {ru ? 'запросов' : 'asks'}</span>
                <span className="text-xs text-gray-400">{ru ? 'Мало данных' : 'Low evidence'}</span>
              </div>
            </div>
            <div className="flex items-center justify-between gap-2 text-[11px] text-gray-500">
              <span>-- {ru ? 'сигналов спроса' : 'demand signals'}</span>
              <span>{ru ? 'Пробел' : 'Gap'}: {row.gap}%</span>
            </div>
          </div>
        ))}
      </div>
      <div className="mt-3 pt-3 border-t border-gray-100">
        <div className="bg-emerald-50 border border-emerald-100 rounded-lg px-3 py-2">
          <p className="text-xs text-emerald-800">
            <span style={{ fontWeight: 600 }}>{ru ? 'Главная возможность:' : 'Top opportunity:'}</span>{' '}
            {ru ? 'после появления social service-gap модели сюда будут выводиться реальные unmet signals.' : 'real unmet signals will appear here once the social service-gap model is ready.'}
          </p>
        </div>
      </div>
    </PlaceholderCard>
  );
}

export function SocialSatisfactionPlaceholder({ lang }: { lang: Lang }) {
  const ru = lang === 'ru';
  const items = [
    { emoji: '💬', area: ru ? 'Комментарии' : 'Comments', value: 74, trend: '+--' },
    { emoji: '🎯', area: ru ? 'Релевантность' : 'Relevance', value: 61, trend: '--' },
    { emoji: '📣', area: ru ? 'Реклама' : 'Ads', value: 44, trend: '--' },
    { emoji: '🛍️', area: ru ? 'Офферы' : 'Offers', value: 39, trend: '--' },
  ];

  return (
    <PlaceholderCard
      {...dashboardCard(
        'satisfaction_by_area',
        ru ? 'Радар удовлетворённости жизнью' : 'Life Satisfaction Radar',
        ru ? 'Что нравится людям и что их расстраивает?' : 'What do people love vs. what frustrates them?',
        ru ? 'По сферам жизни' : 'Community satisfaction by area',
      )}
    >
      <div className="space-y-2.5">
        {items.map((item) => (
          <div key={item.area} className="flex items-center gap-3">
            <span className="text-base w-6 text-center">{item.emoji}</span>
            <span className="text-xs text-gray-700 w-36" style={{ fontWeight: 500 }}>{item.area}</span>
            <div className="flex-1 bg-gray-100 rounded-full h-3">
              <div className="h-3 rounded-full flex items-center justify-end pr-1.5" style={{ width: `${item.value}%`, backgroundColor: item.value >= 70 ? '#10b981' : item.value >= 45 ? '#f59e0b' : '#ef4444' }}>
                {item.value >= 30 ? <span className="text-xs text-white" style={{ fontWeight: 600, fontSize: '9px' }}>--</span> : null}
              </div>
            </div>
            <span className="text-xs w-10 text-right text-gray-400">{item.trend}</span>
          </div>
        ))}
      </div>
      <div className="mt-3 pt-3 border-t border-gray-100 grid grid-cols-2 gap-3">
        <div className="bg-emerald-50 border border-emerald-100 rounded-lg px-3 py-2">
          <span className="text-xs text-emerald-900 block" style={{ fontWeight: 600 }}>{ru ? 'Сообщество ценит' : 'Community loves'}</span>
          <p className="text-xs text-emerald-700">{ru ? 'Появится после social-satisfaction scoring' : 'Will appear after social satisfaction scoring'}</p>
        </div>
        <div className="bg-red-50 border border-red-100 rounded-lg px-3 py-2">
          <span className="text-xs text-red-900 block" style={{ fontWeight: 600 }}>{ru ? 'Болевые точки' : 'Pain points'}</span>
          <p className="text-xs text-red-700">{ru ? 'Появится после social-satisfaction scoring' : 'Will appear after social satisfaction scoring'}</p>
        </div>
      </div>
    </PlaceholderCard>
  );
}

const moodPlaceholderData = [
  { week: 'W1', hopeful: 16, calm: 18, concerned: 12, frustrated: 10 },
  { week: 'W2', hopeful: 18, calm: 17, concerned: 11, frustrated: 9 },
  { week: 'W3', hopeful: 17, calm: 19, concerned: 10, frustrated: 8 },
  { week: 'W4', hopeful: 19, calm: 18, concerned: 11, frustrated: 7 },
];

export function SocialMoodPlaceholder({ lang }: { lang: Lang }) {
  const ru = lang === 'ru';
  const moodConfig = [
    { key: 'hopeful', label: ru ? 'Надежда' : 'Hopeful', emoji: '🙂', color: '#60a5fa' },
    { key: 'calm', label: ru ? 'Спокойствие' : 'Calm', emoji: '😌', color: '#34d399' },
    { key: 'concerned', label: ru ? 'Тревога' : 'Concerned', emoji: '😟', color: '#f59e0b' },
    { key: 'frustrated', label: ru ? 'Раздражение' : 'Frustrated', emoji: '😤', color: '#f87171' },
  ];

  return (
    <PlaceholderCard
      {...dashboardCard(
        'mood_over_time',
        ru ? 'Настроения сообщества' : 'Community Mood',
        ru ? 'Эмоциональный градусник сообщества.' : 'Emotional temperature of the community.',
        ru ? 'Social pending' : 'Social pending',
      )}
    >
      <span className="self-start text-xs text-emerald-600" style={{ fontWeight: 500 }}>-- {ru ? 'позитивных' : 'positive'}</span>
      <div className="mt-4">
        <ResponsiveContainer width="100%" height={200}>
          <AreaChart data={moodPlaceholderData}>
            <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" />
            <XAxis dataKey="week" tick={{ fontSize: 11 }} stroke="#9ca3af" />
            <YAxis tick={{ fontSize: 11 }} stroke="#9ca3af" />
            <Tooltip />
            {moodConfig.map((mood) => (
              <Area key={mood.key} type="monotone" dataKey={mood.key} stackId="1" stroke={mood.color} fill={mood.color} fillOpacity={0.7} />
            ))}
          </AreaChart>
        </ResponsiveContainer>
      </div>
      <div className="flex flex-wrap items-center justify-center gap-x-3 gap-y-1 mt-2">
        {moodConfig.map((mood) => (
          <div key={mood.key} className="flex items-center gap-1">
            <span style={{ fontSize: '13px' }}>{mood.emoji}</span>
            <span className="text-xs text-gray-500">{mood.label}</span>
          </div>
        ))}
      </div>
      <SocialPendingNote lang={lang} />
    </PlaceholderCard>
  );
}

export function SocialUrgencyPlaceholder({ lang }: { lang: Lang }) {
  const ru = lang === 'ru';
  const urgent = [
    { tone: 'bg-red-50 border-red-200 text-red-700', label: ru ? 'Критические' : 'Critical', items: 2 },
    { tone: 'bg-orange-50 border-orange-200 text-orange-700', label: ru ? 'Высокая срочность' : 'High urgency', items: 2 },
  ];

  return (
    <PlaceholderCard
      {...dashboardCard(
        'emotional_urgency_index',
        ru ? 'Индекс эмоциональной срочности' : 'Emotional Urgency Index',
        ru ? 'Сигналы, где людям нужна помощь прямо сейчас.' : 'Signals where people need help right now.',
      )}
    >
      <div className="flex flex-wrap items-center gap-2 mb-4">
        {urgent.map((item) => (
          <span key={item.label} className={`px-2 py-0.5 rounded-full text-[10px] ${item.tone}`}>{item.items} {item.label}</span>
        ))}
      </div>
      <div className="space-y-4">
        {urgent.map((section, sectionIndex) => (
          <div key={section.label}>
            <span className={`text-xs block mb-2 ${sectionIndex === 0 ? 'text-red-700' : 'text-orange-700'}`} style={{ fontWeight: 600 }}>{section.label}</span>
            <div className="space-y-2">
              {Array.from({ length: section.items }).map((_, index) => (
                <div key={index} className={`${sectionIndex === 0 ? 'bg-red-50 border-red-200' : 'bg-orange-50 border-orange-200'} border rounded-lg p-3`}>
                  <div className="flex items-start gap-2 mb-2">
                    <AlertTriangle className={`w-3.5 h-3.5 flex-shrink-0 mt-0.5 ${sectionIndex === 0 ? 'text-red-500' : 'text-orange-500'}`} />
                    <div className="flex-1 space-y-2">
                      <SkeletonLine width="96%" />
                      <SkeletonLine width="82%" />
                    </div>
                  </div>
                  <div className="flex items-center gap-3 text-xs flex-wrap">
                    <span className={`${sectionIndex === 0 ? 'bg-red-100 text-red-700' : 'bg-orange-100 text-orange-700'} px-1.5 py-0.5 rounded`}>{ru ? 'Тема social' : 'Social topic'}</span>
                    <span className="text-gray-500">-- {ru ? 'похожих публикаций' : 'similar posts'}</span>
                    <span className={`${sectionIndex === 0 ? 'text-red-600' : 'text-orange-600'} ml-auto`}>{ru ? '→ маршрут помощи' : '→ response path'}</span>
                  </div>
                </div>
              ))}
            </div>
          </div>
        ))}
      </div>
      <SocialPendingNote lang={lang} />
    </PlaceholderCard>
  );
}

export function SocialKeyVoicesPlaceholder({ lang, rangeDays }: { lang: Lang; rangeDays: number }) {
  const ru = lang === 'ru';
  const voices = [ru ? 'Эксперт' : 'Expert', ru ? 'Менеджер сообщества' : 'Community manager', ru ? 'Пользователь' : 'Member'];

  return (
    <PlaceholderCard
      {...dashboardCard(
        'key_voices',
        ru ? 'Ключевые голоса сообщества' : 'Key Community Voices',
        ru ? 'Участники, которые чаще других появляются в обсуждениях.' : 'Participants who appear most often in discussions.',
        ru ? `Активные комментаторы за ${rangeDays} дн.` : `Active commenters in the selected ${rangeDays}-day window`,
      )}
    >
      <div className="space-y-3">
        {voices.map((voice, index) => (
          <div key={voice} className="bg-gray-50 rounded-lg p-3">
            <div className="flex items-center gap-3 mb-2">
              <div className="w-10 h-10 rounded-full bg-gradient-to-br from-blue-400 to-blue-600 flex items-center justify-center flex-shrink-0">
                <span className="text-white text-sm" style={{ fontWeight: 600 }}>{String.fromCharCode(65 + index)}</span>
              </div>
              <div className="flex-1 min-w-0">
                <SkeletonLine width="44%" />
                <p className="text-xs text-gray-500 mt-1">{voice}</p>
              </div>
              <div className="text-right flex-shrink-0">
                <span className="text-xs text-gray-900 block" style={{ fontWeight: 600 }}>--</span>
                <span className="text-xs text-gray-400">{ru ? 'комм./нед.' : 'comments/wk'}</span>
              </div>
            </div>
            <div className="flex items-center gap-2 mb-1.5 flex-wrap">
              {[ru ? 'Тема A' : 'Topic A', ru ? 'Тема B' : 'Topic B', ru ? 'Тема C' : 'Topic C'].map((topic) => (
                <span key={topic} className="text-xs bg-white border border-gray-200 rounded px-1.5 py-0.5 text-gray-600">{topic}</span>
              ))}
            </div>
            <div className="flex items-center gap-4 text-xs text-gray-400 flex-wrap">
              <span>{ru ? 'Участие в ответах:' : 'Reply participation:'} <span className="text-gray-700" style={{ fontWeight: 600 }}>--</span></span>
              <span>{ru ? 'Активен в:' : 'Active in:'} <span className="text-gray-700" style={{ fontWeight: 600 }}>{ru ? 'social surface' : 'social surface'}</span></span>
            </div>
          </div>
        ))}
      </div>
      <div className="pt-3 mt-3 border-t border-gray-100 flex justify-end">
        <span className="text-xs text-blue-600" style={{ fontWeight: 500 }}>{ru ? 'Все участники →' : 'See all voices →'}</span>
      </div>
    </PlaceholderCard>
  );
}

export function SocialRecommendationPlaceholder({ lang }: { lang: Lang }) {
  const ru = lang === 'ru';
  const rows = [ru ? 'Рекомендуемый формат' : 'Recommended format', ru ? 'Лучший CTA' : 'Best CTA', ru ? 'Повторяющийся оффер' : 'Repeated offer'];

  return (
    <PlaceholderCard
      {...dashboardCard(
        'recommendation_tracker',
        ru ? 'Рекомендации сообщества' : 'Community Recommendations',
        ru ? 'Что люди советуют новичкам — органические сигналы доверия.' : 'What people recommend to newcomers — organic trust signals.',
        ru ? 'Самые популярные советы' : 'Most shared suggestions',
      )}
    >
      <div className="space-y-2">
        {rows.map((row, index) => (
          <div key={row} className="flex items-center gap-3 py-1.5 px-2 rounded-lg hover:bg-gray-50 transition-colors">
            <div className={`w-7 h-7 rounded-full flex items-center justify-center flex-shrink-0 ${index === 0 ? 'bg-emerald-100' : 'bg-amber-100'}`}>
              <Star className={`w-3.5 h-3.5 ${index === 0 ? 'text-emerald-600' : 'text-amber-600'}`} />
            </div>
            <div className="flex-1 min-w-0">
              <SkeletonLine width={index === 0 ? '56%' : '48%'} />
              <p className="text-xs text-gray-400 mt-1">{ru ? 'Категория social' : 'Social category'}</p>
            </div>
            <div className="flex items-center gap-1 flex-shrink-0">
              <Star className="w-3 h-3 text-amber-400 fill-amber-400" />
              <span className="text-xs text-gray-900" style={{ fontWeight: 600 }}>--</span>
            </div>
            <span className="text-xs text-gray-500 w-20 text-right">--x</span>
          </div>
        ))}
      </div>
      <div className="mt-3 pt-3 border-t border-gray-100">
        <div className="bg-teal-50 border border-teal-100 rounded-lg px-3 py-2">
          <p className="text-xs text-teal-800">
            <span style={{ fontWeight: 600 }}>{ru ? 'Инсайт:' : 'Insight:'}</span>{' '}
            {ru ? 'органические рекомендации появятся здесь после social recommendation extraction.' : 'organic recommendation signals will appear here after social recommendation extraction.'}
          </p>
        </div>
      </div>
    </PlaceholderCard>
  );
}

export function SocialInformationVelocityPlaceholder({ lang }: { lang: Lang }) {
  const ru = lang === 'ru';
  const items = [
    { tone: 'bg-red-50 text-red-700 border-red-200', label: ru ? 'Взрывной' : 'Explosive' },
    { tone: 'bg-amber-50 text-amber-700 border-amber-200', label: ru ? 'Быстрый' : 'Fast' },
    { tone: 'bg-gray-50 text-gray-600 border-gray-200', label: ru ? 'Обычный' : 'Normal' },
  ];

  return (
    <PlaceholderCard
      {...dashboardCard(
        'information_velocity',
        ru ? 'Скорость распространения информации' : 'Information Velocity',
        ru ? 'Отслеживает источники нарративов и их распространение.' : 'Tracks where narratives are born and how they travel.',
        ru ? 'Как быстро темы распространяются по каналам' : 'How fast topics spread across channels',
      )}
    >
      <div className="space-y-3">
        {items.map((item, index) => (
          <div key={item.label} className={`${item.tone} border rounded-lg p-3`}>
            <div className="flex items-start justify-between mb-2">
              <SkeletonLine width={index === 0 ? '34%' : '46%'} />
              <span className={`text-xs px-2 py-0.5 rounded-full ${item.tone} border`}>{item.label}</span>
            </div>
            <div className="flex items-center gap-4 text-xs mb-2 flex-wrap text-gray-500">
              <span>{ru ? 'Источник:' : 'Origin:'} <span className="text-gray-900">--</span></span>
              <span>{ru ? 'Распр. за:' : 'Spread in:'} <span className="text-gray-900">--</span></span>
              <span>{ru ? 'Каналов:' : 'Channels:'} <span className="text-gray-900">--</span></span>
              <span className="ml-auto">-- {ru ? 'охват' : 'reach'}</span>
            </div>
            <div className="flex items-center gap-1 flex-wrap">
              <span className="text-xs text-gray-400">{ru ? 'Усилено через:' : 'Amplified by:'}</span>
              {['Facebook', 'Instagram', 'TikTok'].map((amp) => (
                <span key={amp} className="text-xs bg-white border border-gray-200 rounded px-1.5 py-0.5 text-gray-600">{amp}</span>
              ))}
            </div>
          </div>
        ))}
      </div>
      <SocialPendingNote lang={lang} />
    </PlaceholderCard>
  );
}

export function SocialPersonaPlaceholder({ lang }: { lang: Lang }) {
  const ru = lang === 'ru';
  const [selected, setSelected] = useState<number | null>(0);
  const personas = [
    { name: ru ? 'Новый исследователь' : 'New explorer', size: 58, color: '#3b82f6' },
    { name: ru ? 'Ищущий выгоду' : 'Value seeker', size: 82, color: '#14b8a6' },
    { name: ru ? 'Лояльный клиент' : 'Loyal customer', size: 66, color: '#8b5cf6' },
    { name: ru ? 'Случайный наблюдатель' : 'Casual observer', size: 44, color: '#f59e0b' },
  ];
  const maxSize = Math.max(...personas.map((item) => item.size), 1);

  return (
    <PlaceholderCard
      {...dashboardCard(
        'persona_gallery',
        ru ? 'Персоны сообщества' : 'Community Personas',
        ru ? 'Кто ваши участники? Нажмите на сегмент для изучения.' : 'Who are your community members? Click a segment to explore.',
        ru ? '4 поведенческих кластера' : '4 behavioral clusters',
      )}
    >
      <div className="flex items-end gap-2 mb-4 h-20">
        {personas.map((persona, index) => (
          <button
            key={persona.name}
            type="button"
            className={`flex-1 rounded-t-lg transition-all ${selected === index ? 'ring-2 ring-gray-900' : 'hover:opacity-80'}`}
            style={{ height: `${(persona.size / maxSize) * 100}%`, backgroundColor: persona.color, opacity: selected === null || selected === index ? 1 : 0.4 }}
            onClick={() => setSelected(selected === index ? null : index)}
          />
        ))}
      </div>
      <div className="flex gap-2 mb-3">
        {personas.map((persona) => (
          <div key={persona.name} className="flex-1 text-center">
            <span className="text-xs text-gray-600 block truncate" style={{ fontSize: '9px' }}>{persona.name}</span>
            <span className="text-xs text-gray-900" style={{ fontWeight: 600 }}>--%</span>
          </div>
        ))}
      </div>
      {selected !== null ? (
        <div className="bg-gray-50 rounded-lg p-4 border border-gray-200">
          <div className="flex items-center gap-2 mb-2">
            <div className="w-8 h-8 rounded-full flex items-center justify-center" style={{ backgroundColor: personas[selected].color }}>
              <User className="w-4 h-4 text-white" />
            </div>
            <div>
              <span className="text-sm text-gray-900" style={{ fontWeight: 600 }}>{personas[selected].name}</span>
              <span className="text-xs text-gray-500 ml-2">-- {ru ? 'человек' : 'people'}</span>
            </div>
          </div>
          <div className="space-y-2">
            <SkeletonLine width="96%" />
            <SkeletonLine width="82%" />
          </div>
          <div className="grid grid-cols-2 gap-2 text-xs mt-3">
            {[
              ru ? 'Профиль' : 'Profile',
              ru ? 'Потребности' : 'Needs',
              ru ? 'Интересы' : 'Interests',
              ru ? 'Болевые точки' : 'Pain points',
            ].map((label) => (
              <div key={label}><span className="text-gray-400">{label}:</span> <span className="text-gray-700" style={{ fontWeight: 500 }}>--</span></div>
            ))}
          </div>
        </div>
      ) : (
        <div className="text-center py-2"><span className="text-xs text-gray-400">{ru ? 'Нажмите на полосу выше для изучения персоны' : 'Click a bar above to explore persona details'}</span></div>
      )}
    </PlaceholderCard>
  );
}

const radarPlaceholderData = [
  { interest: 'Offers', score: 56 },
  { interest: 'Pricing', score: 48 },
  { interest: 'Support', score: 42 },
  { interest: 'Features', score: 51 },
  { interest: 'Trust', score: 37 },
];

export function SocialInterestRadarPlaceholder({ lang, rangeDays }: { lang: Lang; rangeDays: number }) {
  const ru = lang === 'ru';
  const top2 = useMemo(() => [...radarPlaceholderData].sort((a, b) => b.score - a.score).slice(0, 2), []);
  const maxScore = Math.max(...radarPlaceholderData.map((item) => item.score), 0);
  const radialMax = 70;
  const radialTicks = [0, 18, 35, 52, 70];

  return (
    <PlaceholderCard
      {...dashboardCard(
        'interest_radar',
        ru ? 'Интересы сообщества' : 'Community Interests',
        ru ? `Доля активных участников, обсуждавших каждую тему интереса в выбранном ${rangeDays}-дневном окне` : `Share of active members discussing each interest area in the selected ${rangeDays}-day window`,
        ru ? `Выбранное окно · ${rangeDays} дн.` : `Selected window · ${rangeDays}d`,
      )}
    >
      <div className="mt-4 rounded-2xl border border-slate-100 bg-slate-50/70 px-2 py-4">
        <ResponsiveContainer width="100%" height={360}>
          <RadarChart data={radarPlaceholderData} cx="50%" cy="50%" outerRadius="74%">
            <PolarGrid stroke="#dbe3ee" radialLines />
            <PolarAngleAxis dataKey="interest" tick={{ fontSize: 11, fill: '#6b7280', fontWeight: 500 }} />
            <PolarRadiusAxis angle={18} domain={[0, radialMax]} ticks={radialTicks} tick={{ fontSize: 10, fill: '#94a3b8' }} axisLine={false} tickFormatter={(value) => `${value}%`} />
            <Radar name={ru ? 'Доля активных участников' : 'Active-member share'} dataKey="score" stroke="#0f766e" fill="#14b8a6" fillOpacity={0.24} strokeWidth={3} dot={{ r: 3, fill: '#0f766e', strokeWidth: 0 }} />
          </RadarChart>
        </ResponsiveContainer>
        <div className="mt-2 flex items-center justify-between px-2">
          <span className="text-[11px] text-slate-500">{ru ? `Шкала отображения: 0-${radialMax}%` : `Display scale: 0-${radialMax}%`}</span>
          <span className="text-[11px] text-slate-500">{ru ? `Пик окна: ${maxScore}%` : `Window peak: ${maxScore}%`}</span>
        </div>
      </div>
      <div className="grid grid-cols-2 gap-2 mt-2">
        {radarPlaceholderData.slice(0, 4).map((item) => (
          <div key={item.interest} className="flex items-center gap-2">
            <div className="w-2 h-2 rounded-full bg-teal-500" />
            <span className="text-xs text-gray-600">{item.interest}</span>
            <span className="text-xs text-gray-900 ml-auto" style={{ fontWeight: 600 }}>{item.score}%</span>
          </div>
        ))}
      </div>
      <div className="mt-3 bg-teal-50 border border-teal-100 rounded-lg px-3 py-2">
        <p className="text-xs text-teal-800">
          <span style={{ fontWeight: 600 }}>{ru ? 'Приоритет:' : 'Priority signal:'}</span>{' '}
          {top2[0].interest} ({top2[0].score}%) {ru ? 'и' : 'and'} {top2[1].interest} ({top2[1].score}%).
        </p>
      </div>
      <SocialPendingNote lang={lang} />
    </PlaceholderCard>
  );
}

export function SocialGrowthFunnelPlaceholder({ lang }: { lang: Lang }) {
  const ru = lang === 'ru';
  const stages = [
    { stage: ru ? 'Смотрят' : 'Read', pct: 100, color: '#94a3b8', count: '--' },
    { stage: ru ? 'Спрашивают' : 'Ask', pct: 54, color: '#60a5fa', count: '--' },
    { stage: ru ? 'Помогают' : 'Help', pct: 28, color: '#14b8a6', count: '--' },
    { stage: ru ? 'Лидируют' : 'Lead', pct: 12, color: '#8b5cf6', count: '--' },
  ];

  return (
    <PlaceholderCard
      {...dashboardCard(
        'community_growth_funnel',
        ru ? 'Воронка вовлечённости' : 'Engagement Funnel',
        ru ? 'Насколько глубоко вовлечены люди?' : 'How deep does engagement go?',
        ru ? 'Прогресс участников' : 'Member progression',
      )}
    >
      <div className="space-y-2 mb-4">
        {stages.map((stage) => (
          <div key={stage.stage} className="flex items-center gap-3">
            <span className="text-xs text-gray-700 w-36 text-right" style={{ fontWeight: 500 }}>{stage.stage}</span>
            <div className="flex-1 relative">
              <div className="w-full bg-gray-100 rounded-full h-7">
                <div className="h-7 rounded-full flex items-center px-2.5" style={{ width: `${Math.max(8, stage.pct)}%`, backgroundColor: stage.color }}>
                  <span className="text-xs text-white whitespace-nowrap" style={{ fontWeight: 600 }}>{stage.count}</span>
                </div>
              </div>
            </div>
            <span className="text-xs text-gray-500 w-8 text-right">{stage.pct}%</span>
          </div>
        ))}
      </div>
      <div className="grid grid-cols-3 gap-3 pt-3 border-t border-gray-100">
        {[
          { label: ru ? 'Отсев' : 'Drop-off', value: ru ? 'Чит. → Спрос.' : 'Read → Ask', tone: 'text-red-500', score: '--' },
          { label: ru ? 'Конверсия' : 'Conversion', value: ru ? 'Спрос. → Пом.' : 'Ask → Help', tone: 'text-emerald-500', score: '--' },
          { label: ru ? 'Цель' : 'Target', value: ru ? 'Лидеры' : 'Leaders', tone: 'text-emerald-500', score: '--' },
        ].map((item) => (
          <div key={item.label} className="text-center">
            <span className="text-xs text-gray-500 block">{item.label}</span>
            <span className="text-sm text-gray-900 block" style={{ fontWeight: 600 }}>{item.value}</span>
            <span className={`text-xs block ${item.tone}`}>{item.score}</span>
          </div>
        ))}
      </div>
      <SocialPendingNote lang={lang} />
    </PlaceholderCard>
  );
}

export function SocialRetentionPlaceholder({ lang }: { lang: Lang }) {
  const ru = lang === 'ru';
  const factors = [
    { factor: ru ? 'Повторные комментарии' : 'Repeat comments', score: 74 },
    { factor: ru ? 'Ответы бренда' : 'Brand replies', score: 58 },
    { factor: ru ? 'Стабильность тем' : 'Topic stability', score: 43 },
  ];
  const signals = [
    { signal: ru ? 'Падение ответов на complaints' : 'Drop in complaint replies', tone: 'bg-red-50', trend: '+--' },
    { signal: ru ? 'Низкая реакция на ads' : 'Weak reaction to ads', tone: 'bg-amber-50', trend: '--' },
  ];

  return (
    <PlaceholderCard
      {...dashboardCard(
        'retention_risk_gauge',
        ru ? 'Непрерывность активности и сигналы риска' : 'Activity Continuity & Risk Signals',
        ru ? 'Какая доля активных участников возвращается, и какие темы дают риск оттока.' : 'Shows how many active members return and which topics carry drop-off risk.',
      )}
    >
      <div className="mb-3 inline-flex rounded-full bg-amber-100 px-2 py-0.5 text-xs text-amber-700">{ru ? 'Возврат: --/100' : 'Continuity: --/100'}</div>
      <div className="space-y-2 mb-4">
        <span className="text-xs text-gray-500 block" style={{ fontWeight: 500 }}>{ru ? 'Темы, связанные с повторной активностью' : 'Topics linked to repeat activity'}</span>
        {factors.map((factor) => (
          <div key={factor.factor} className="flex items-center gap-2">
            <span className="text-xs text-gray-600 w-44">{factor.factor}</span>
            <div className="flex-1 bg-gray-100 rounded-full h-2">
              <div className="h-2 rounded-full" style={{ width: `${factor.score}%`, backgroundColor: factor.score >= 65 ? '#10b981' : factor.score >= 45 ? '#f59e0b' : '#ef4444' }} />
            </div>
            <span className="text-xs text-gray-900 w-8 text-right" style={{ fontWeight: 500 }}>--</span>
          </div>
        ))}
      </div>
      <div className="pt-3 border-t border-gray-100 space-y-2">
        <span className="text-xs text-gray-500 block" style={{ fontWeight: 500 }}>{ru ? 'Сигналы риска выше базового уровня' : 'Above-baseline risk signals'}</span>
        {signals.map((signal) => (
          <div key={signal.signal} className={`flex items-center gap-2 text-xs rounded-lg p-2 ${signal.tone}`}>
            <span className="text-gray-700 flex-1 italic">{signal.signal}</span>
            <span className="text-gray-500">--x</span>
            <span className="text-red-500" style={{ fontWeight: 600 }}>{signal.trend}</span>
          </div>
        ))}
      </div>
      <SocialPendingNote lang={lang} />
    </PlaceholderCard>
  );
}

export function SocialDecisionStagesPlaceholder({ lang }: { lang: Lang }) {
  const ru = lang === 'ru';
  const stages = [
    { stage: ru ? 'Осознание' : 'Aware', color: '#60a5fa' },
    { stage: ru ? 'Рассмотрение' : 'Considering', color: '#14b8a6' },
    { stage: ru ? 'Решение' : 'Deciding', color: '#8b5cf6' },
    { stage: ru ? 'Лояльность' : 'Loyal', color: '#f59e0b' },
  ];

  return (
    <PlaceholderCard
      {...dashboardCard(
        'decision_stage_tracker',
        ru ? 'Этапы пути участников' : 'Member Journey Stages',
        ru ? 'Адаптируйте контент к каждому этапу.' : 'Tailor content to each stage.',
        ru ? 'Где каждый находится в своей истории?' : 'Where is everyone in their story?',
      )}
    >
      <div className="space-y-3">
        {stages.map((stage, index) => (
          <div key={stage.stage} className="flex items-center gap-3">
            <div className="w-6 h-6 rounded-full flex items-center justify-center flex-shrink-0" style={{ backgroundColor: stage.color }}>
              <span className="text-xs text-white" style={{ fontWeight: 700 }}>{index + 1}</span>
            </div>
            <div className="flex-1">
              <div className="flex items-center justify-between mb-1">
                <span className="text-xs text-gray-900" style={{ fontWeight: 500 }}>{stage.stage}</span>
                <div className="flex items-center gap-2">
                  <span className="text-xs text-gray-900" style={{ fontWeight: 600 }}>--</span>
                  <span className="text-xs text-gray-400">--%</span>
                </div>
              </div>
              <div className="w-full bg-gray-100 rounded-full h-2">
                <div className="h-2 rounded-full" style={{ width: `${82 - index * 14}%`, backgroundColor: stage.color }} />
              </div>
              <span className="text-xs text-gray-400 mt-0.5 block">{ru ? 'Нужно:' : 'Needs:'} --</span>
            </div>
          </div>
        ))}
      </div>
      <div className="mt-3 bg-violet-50 border border-violet-100 rounded-lg px-3 py-2">
        <p className="text-xs text-violet-800"><span style={{ fontWeight: 600 }}>{ru ? 'Сигнал роста:' : 'Growth signal:'}</span> {ru ? 'появится после social journey staging.' : 'will appear after social journey staging.'}</p>
      </div>
    </PlaceholderCard>
  );
}

export function SocialEmergingInterestsPlaceholder({ lang }: { lang: Lang }) {
  const ru = lang === 'ru';
  const items = [
    { topic: ru ? 'Новый social сигнал' : 'Emerging social signal', opportunity: ru ? 'высокая' : 'high', tone: 'bg-emerald-50 text-emerald-700 border-emerald-200' },
    { topic: ru ? 'Ранний паттерн обсуждения' : 'Early discussion pattern', opportunity: ru ? 'средняя' : 'medium', tone: 'bg-blue-50 text-blue-700 border-blue-200' },
  ];

  return (
    <PlaceholderCard
      {...dashboardCard(
        'emerging_interests',
        ru ? 'Зарождающиеся интересы' : 'Emerging Interests',
        ru ? 'Новые разговоры, которые начинают набирать обороты.' : 'New conversations bubbling up.',
        ru ? 'Темы < 14 дней' : 'Topics <14 days old',
      )}
    >
      <div className="space-y-2.5">
        {items.map((item) => (
          <div key={item.topic} className={`${item.tone} border rounded-lg p-3`}>
            <div className="flex items-start justify-between mb-1.5">
              <span className="text-xs text-gray-900" style={{ fontWeight: 600 }}>{item.topic}</span>
              <span className={`text-xs px-1.5 py-0.5 rounded ${item.tone}`} style={{ fontWeight: 500 }}>{item.opportunity}</span>
            </div>
            <div className="flex items-center gap-4 text-xs text-gray-500">
              <span className="text-emerald-600">+--%</span>
              <span>-- {ru ? 'упоминаний' : 'mentions'}</span>
              <span>{ru ? 'через' : 'via'} --</span>
              <span className="ml-auto">--</span>
            </div>
          </div>
        ))}
      </div>
      <SocialPendingNote lang={lang} />
    </PlaceholderCard>
  );
}

const voicePlaceholderData = [
  { week: 'W1', returning: 24, newVoices: 8 },
  { week: 'W2', returning: 23, newVoices: 10 },
  { week: 'W3', returning: 26, newVoices: 9 },
  { week: 'W4', returning: 27, newVoices: 11 },
];

export function SocialNewVsReturningPlaceholder({ lang }: { lang: Lang }) {
  const ru = lang === 'ru';
  const topics = [ru ? 'Новый интерес' : 'New interest', ru ? 'Первая жалоба' : 'First complaint', ru ? 'Новый оффер' : 'New offer'];

  return (
    <PlaceholderCard
      {...dashboardCard(
        'new_vs_returning_voice',
        ru ? 'Новые vs. постоянные голоса' : 'New vs. Returning Voices',
        ru ? 'Рост новых голосов = рост сообщества.' : 'Rising newcomers = community growth.',
      )}
    >
      <div className="mb-1 inline-flex rounded-full bg-emerald-100 px-2 py-0.5 text-xs text-emerald-700">{ru ? '--% новых на этой неделе' : '--% new this week'}</div>
      <div className="grid grid-cols-3 gap-3 mb-4 mt-3">
        {[
          { label: ru ? 'Новые голоса' : 'New voices', value: '--', tone: 'bg-blue-50 text-blue-700' },
          { label: ru ? 'Постоянные' : 'Returning', value: '--', tone: 'bg-slate-50 text-slate-700' },
          { label: ru ? 'Тренд новых' : 'New voice trend', value: '--', tone: 'bg-emerald-50 text-emerald-700' },
        ].map((item) => (
          <div key={item.label} className={`${item.tone} rounded-lg p-3 text-center`}>
            <span className="text-xl block" style={{ fontWeight: 700 }}>{item.value}</span>
            <span className="text-xs text-gray-500">{item.label}</span>
          </div>
        ))}
      </div>
      <ResponsiveContainer width="100%" height={160}>
        <BarChart data={voicePlaceholderData} barSize={20}>
          <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" />
          <XAxis dataKey="week" tick={{ fontSize: 11 }} stroke="#9ca3af" />
          <YAxis tick={{ fontSize: 11 }} stroke="#9ca3af" />
          <Tooltip />
          <Bar dataKey="returning" stackId="a" fill="#cbd5e1" name={ru ? 'Постоянные' : 'Returning'} />
          <Bar dataKey="newVoices" stackId="a" fill="#0d9488" name={ru ? 'Новые голоса' : 'New voices'} radius={[4, 4, 0, 0]} />
        </BarChart>
      </ResponsiveContainer>
      <div className="mt-4 pt-3 border-t border-gray-100">
        <span className="text-xs text-gray-700 block mb-2" style={{ fontWeight: 600 }}>{ru ? 'Где новые голоса говорят первыми' : 'Where new voices speak first'}</span>
        <div className="space-y-1.5">
          {topics.map((topic, index) => (
            <div key={topic} className="flex items-center gap-2">
              <span className="text-xs text-gray-600 w-36">{topic}</span>
              <div className="flex-1 bg-gray-100 rounded-full h-2">
                <div className="h-2 rounded-full bg-blue-500" style={{ width: `${78 - index * 14}%` }} />
              </div>
              <span className="text-xs text-gray-900 w-8 text-right" style={{ fontWeight: 600 }}>--</span>
            </div>
          ))}
        </div>
      </div>
      <SocialPendingNote lang={lang} />
    </PlaceholderCard>
  );
}

export function SocialBusinessOpportunityPlaceholder({ lang }: { lang: Lang }) {
  const ru = lang === 'ru';

  return (
    <PlaceholderCard
      {...dashboardCard(
        'business_opportunity_tracker',
        ru ? 'Бизнес-возможности от сообщества' : 'Business Opportunity Signals',
        ru ? 'Показываем AI-сводку возможностей, привязанную к реальным сообщениям.' : 'Shows the AI opportunity overview tied back to real source messages.',
        ru ? 'AI + доказательства' : 'AI + evidence grounded',
      )}
    >
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-2.5">
        {Array.from({ length: 4 }).map((_, index) => (
          <div key={index} className="rounded-xl border border-blue-100 bg-blue-50/50 p-3">
            <div className="flex items-start justify-between gap-2">
              <div className="min-w-0 flex-1">
                <div className="text-[11px] text-blue-800/80">{ru ? 'Соц. возможность' : 'Social opportunity'}</div>
                <div className="mt-1 space-y-2">
                  <SkeletonLine width="86%" />
                  <SkeletonLine width="62%" />
                </div>
              </div>
              <span className="text-[10px] px-1.5 py-0.5 rounded border border-blue-300 text-blue-900">{ru ? 'Ожидает уверенность' : 'Confidence pending'}</span>
            </div>
            <div className="mt-3 space-y-2">
              <SkeletonLine width="100%" />
              <SkeletonLine width="88%" />
            </div>
            <div className="mt-2 flex flex-wrap items-center gap-1.5 text-[10px]">
              {[ru ? 'Продукт' : 'Product', ru ? 'Проверить сейчас' : 'Validate now'].map((chip) => (
                <span key={chip} className="px-1.5 py-0.5 rounded-full bg-white border border-gray-200 text-gray-700">{chip}</span>
              ))}
            </div>
            <div className="text-[11px] mt-2 text-gray-600">-- {ru ? 'сигналов · -- людей · -- каналов' : 'signals · people · channels'}</div>
            <div className="text-[11px] mt-0.5 text-gray-600">{ru ? '7д тренд' : '7d trend'}: --</div>
          </div>
        ))}
      </div>
      <SocialPendingNote lang={lang} />
    </PlaceholderCard>
  );
}

export function SocialJobMarketPlaceholder({ lang }: { lang: Lang }) {
  const ru = lang === 'ru';
  const jobs = [
    { role: ru ? 'Поддержка клиентов' : 'Customer support', pct: 86 },
    { role: ru ? 'Контент' : 'Content', pct: 72 },
    { role: ru ? 'Продажи' : 'Sales', pct: 58 },
  ];

  return (
    <PlaceholderCard
      {...dashboardCard(
        'job_market_pulse',
        ru ? 'Рынок труда и занятость' : 'Job & Work Landscape',
        ru ? 'Понимание структуры занятости раскрывает стабильность и потребности сообщества.' : 'Understanding employment patterns reveals community stability and needs.',
        ru ? 'Как работает сообщество' : 'How the community works',
      )}
    >
      <div className="space-y-2 mb-4">
        {jobs.map((job) => (
          <div key={job.role} className="flex items-center gap-3">
            <span className="text-xs text-gray-700 w-48" style={{ fontWeight: 500 }}>{job.role}</span>
            <div className="flex-1 bg-gray-100 rounded-full h-3">
              <div className="h-3 rounded-full bg-blue-500" style={{ width: `${job.pct}%`, opacity: 0.5 + (job.pct / 100) * 0.5 }} />
            </div>
            <span className="text-xs text-gray-900 w-8 text-right" style={{ fontWeight: 600 }}>--</span>
          </div>
        ))}
      </div>
      <div className="pt-3 border-t border-gray-100">
        <span className="text-xs text-gray-500 block mb-2" style={{ fontWeight: 500 }}>{ru ? 'Тренды занятости' : 'Employment trends'}</span>
        <div className="space-y-1.5">
          {[
            { label: ru ? 'Растёт спрос на ответы' : 'Rising support demand', tone: 'bg-emerald-50 text-emerald-700', dot: 'bg-emerald-500' },
            { label: ru ? 'Нужно больше educational content' : 'Need more educational content', tone: 'bg-blue-50 text-blue-700', dot: 'bg-blue-500' },
            { label: ru ? 'Есть concern around pricing' : 'Concern around pricing', tone: 'bg-amber-50 text-amber-700', dot: 'bg-amber-500' },
          ].map((trend) => (
            <div key={trend.label} className={`flex items-center gap-2 text-xs px-2 py-1.5 rounded ${trend.tone}`}>
              <span className={`w-1.5 h-1.5 rounded-full flex-shrink-0 ${trend.dot}`} />
              {trend.label}
            </div>
          ))}
        </div>
      </div>
      <div className="mt-3">
        <span className="text-xs text-gray-500 block mb-2" style={{ fontWeight: 500 }}>{ru ? 'Доказательства из сообщества' : 'Community evidence'}</span>
        <div className="space-y-2.5">
          {[0, 1].map((row) => (
            <div key={row} className="block bg-gray-50 border border-gray-100 rounded-lg p-3">
              <div className="flex items-start gap-2.5">
                <div className="flex-1 min-w-0">
                  <SkeletonLine width={row === 0 ? '52%' : '68%'} />
                  <SkeletonLine width="94%" className="mt-2" />
                  <SkeletonLine width="76%" className="mt-1" />
                  <div className="flex items-center gap-3 mt-1.5">
                    <span className="text-xs px-1.5 py-0.5 rounded bg-gray-100 text-gray-500">{ru ? 'Пост' : 'Post'}</span>
                    <span className="text-xs text-gray-500 truncate">{ru ? 'Открыть тему' : 'Open topic'}</span>
                  </div>
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>
      <div className="mt-3 bg-blue-50 border border-blue-100 rounded-lg px-3 py-2">
        <p className="text-xs text-blue-800"><span style={{ fontWeight: 600 }}>{ru ? 'Ключевой инсайт:' : 'Key insight:'}</span> {ru ? 'здесь появится grounded work-intent summary после social classification.' : 'a grounded work-intent summary will appear here after social classification.'}</p>
      </div>
    </PlaceholderCard>
  );
}
