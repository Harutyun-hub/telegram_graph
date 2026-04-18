import { type ReactNode, useMemo, useState } from 'react';
import {
  Activity,
  ArrowDownRight,
  ArrowRight,
  ArrowUpRight,
  BarChart3,
  Building2,
  ChevronRight,
  Clock,
  Heart,
  Inbox,
  Megaphone,
  MessageSquareText,
  Minus,
  Star,
  TrendingUp,
  Zap,
} from 'lucide-react';
import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import { cn } from '@/app/components/ui/utils';
import { WidgetTitle } from '@/app/components/ui/WidgetTitle';
import type { AdminWidgetId } from '@/app/admin/catalog';
import { SocialTopicBubbleMap } from './SocialShared';
import {
  formatSocialBucket,
  formatSocialDateLabel,
  formatSocialPercent,
  formatSocialRelativeTime,
  sentimentTone,
  socialActivitySummary,
  socialEntityColor,
  socialPlatformLabel,
} from '@/app/services/socialFormatting';
import type {
  SocialAdCard,
  SocialAudienceSignalRow,
  SocialCompetitorRow,
  SocialOverviewResponse,
  SocialSummaryResponse,
} from '@/app/services/socialIntelligence';
import type { SocialTopicListItem, SocialTopicTrendSeries } from '@/app/services/socialTwinData';

type Lang = 'en' | 'ru';

export interface SocialEvidenceRequestInput {
  title: string;
  description: string;
  filters: {
    activityUid?: string;
    topic?: string;
    entityId?: string;
    marketingIntent?: string;
    painPoint?: string;
    customerIntent?: string;
    sourceKind?: string;
    ctaType?: string;
    contentFormat?: string;
    sentiment?: string;
    page?: number;
    size?: number;
  };
}

interface DashboardCardProps {
  widgetId: AdminWidgetId;
  title: string;
  description: string;
  meta?: ReactNode;
  action?: ReactNode;
  className?: string;
  children: ReactNode;
}

function selectedWindowLabel(days: number, ru: boolean): string {
  if (days === 1) {
    return ru ? '1 день' : '1-day window';
  }
  return ru ? `${days} дней` : `${days}-day window`;
}

function DashboardCard({ widgetId, title, description, meta, action, className, children }: DashboardCardProps) {
  return (
    <div className={cn('bg-white rounded-xl border border-gray-200 p-6', className)}>
      <div className="mb-1 flex flex-col items-start gap-2 sm:flex-row sm:items-start sm:justify-between">
        <WidgetTitle widgetId={widgetId}>
          {title}
        </WidgetTitle>
        {meta ? (
          <span className="w-full text-left text-xs text-gray-500 sm:w-auto sm:text-right">
            {meta}
          </span>
        ) : null}
      </div>
      <p className="text-xs text-gray-500 mb-4">
        {description}
      </p>
      {action ? <div className="mb-4">{action}</div> : null}
      {children}
    </div>
  );
}

function CardEmptyState({ message }: { message: string }) {
  return (
    <div className="flex min-h-52 flex-col items-center justify-center rounded-xl border border-dashed border-border bg-muted/40 px-6 py-8 text-center">
      <Inbox className="mb-3 h-8 w-8 text-muted-foreground/50" />
      <p className="text-sm font-medium text-foreground">{message}</p>
    </div>
  );
}

function formatDelta(delta: number, ru: boolean) {
  if (delta === 0) return ru ? 'Без изменений' : 'No change';
  const sign = delta > 0 ? '+' : '';
  return `${sign}${delta}%`;
}

function formatPpDelta(delta: number, ru: boolean) {
  const rounded = Math.round(delta * 10) / 10;
  if (rounded === 0) return ru ? 'Без изменений' : 'No change';
  const sign = rounded > 0 ? '+' : '';
  return `${sign}${rounded}pp`;
}

function mergeTopicTrendData(series: SocialTopicTrendSeries[]) {
  const buckets = new Map<string, Record<string, number | string>>();
  series.forEach((entry) => {
    entry.items.forEach((item) => {
      const row = buckets.get(item.bucket) || { bucket: item.bucket };
      row[entry.topic] = item.total;
      buckets.set(item.bucket, row);
    });
  });

  return Array.from(buckets.values()).sort((a, b) => String(a.bucket).localeCompare(String(b.bucket)));
}

function computeFormatPerformance(ads: SocialAdCard[]) {
  const groups = new Map<string, { name: string; items: number; engagement: number; ctas: Map<string, number> }>();

  ads.forEach((ad) => {
    const key = ad.content_format || ad.source_kind || 'unknown';
    const group = groups.get(key) || {
      name: key,
      items: 0,
      engagement: 0,
      ctas: new Map<string, number>(),
    };
    group.items += 1;
    group.engagement += ad.engagementTotal || 0;
    if (ad.cta_type) {
      group.ctas.set(ad.cta_type, (group.ctas.get(ad.cta_type) || 0) + 1);
    }
    groups.set(key, group);
  });

  return Array.from(groups.values())
    .map((group) => ({
      ...group,
      avgEngagement: group.items > 0 ? Math.round(group.engagement / group.items) : 0,
      topCta: Array.from(group.ctas.entries()).sort((a, b) => b[1] - a[1])[0]?.[0] || null,
    }))
    .sort((a, b) => (b.items - a.items) || (b.avgEngagement - a.avgEngagement));
}

function growthTone(value: number) {
  if (value > 0) return 'text-emerald-700';
  if (value < 0) return 'text-rose-700';
  return 'text-slate-700';
}

const LANDSCAPE_COLORS = ['#ef4444', '#3b82f6', '#8b5cf6', '#f59e0b', '#ec4899', '#10b981', '#06b6d4', '#6b7280', '#f97316', '#14b8a6'];

function landscapeColor(index: number) {
  return LANDSCAPE_COLORS[index % LANDSCAPE_COLORS.length];
}

function socialTopicTileSpec(value: number, minValue: number, maxValue: number) {
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

export function SocialCommunityBriefCard({
  lang,
  overview,
  summary,
  rangeDays,
  onOpenEvidence,
  onOpenOps,
  onOpenTopic,
}: {
  lang: Lang;
  overview: SocialOverviewResponse | null;
  summary: SocialSummaryResponse | null;
  rangeDays: number;
  onOpenEvidence: (input: SocialEvidenceRequestInput) => void;
  onOpenOps: () => void;
  onOpenTopic: (topic: string) => void;
}) {
  const ru = lang === 'ru';
  const [expanded, setExpanded] = useState(false);
  const sentimentClass = summary
    ? sentimentTone(summary.averageSentimentScore).text
    : 'text-slate-700';

  if (!summary) {
    return (
      <DashboardCard
        widgetId="community_brief"
        title={ru ? 'Community Brief' : 'Community Brief'}
        description={ru ? 'Операционный обзор social-поверхности за выбранный период.' : 'Executive summary of the social surface for the selected window.'}
      >
        <CardEmptyState message={ru ? 'Пока нет social-данных для выбранного периода.' : 'No social activity is available for the selected period yet.'} />
      </DashboardCard>
    );
  }

  return (
    <div
      className="bg-white rounded-xl border border-sky-200 p-6 relative overflow-hidden"
      style={{ boxShadow: '0 0 0 1px rgba(2,132,199,0.08), 0 4px 24px 0 rgba(2,132,199,0.07)' }}
    >
      <div className="absolute inset-0 bg-gradient-to-br from-sky-50/60 via-white to-white pointer-events-none rounded-xl" />

      <div className="relative">
        <div className="flex items-center justify-between mb-4">
          <div className="flex items-start gap-3">
            <div className="flex-shrink-0 w-9 h-9 rounded-xl bg-gradient-to-br from-sky-500 to-blue-600 flex items-center justify-center shadow-sm mt-0.5">
              <BarChart3 className="w-4.5 h-4.5 text-white" style={{ width: '18px', height: '18px' }} />
            </div>
            <div>
              <WidgetTitle widgetId="community_brief">
                {ru ? 'Снимок сообщества' : 'Community Snapshot'}
              </WidgetTitle>
              <p className="text-xs text-gray-500 mt-0.5">
                {overview?.runtime?.last_success_at
                  ? (ru
                    ? `Последний цикл ${formatSocialRelativeTime(overview.runtime.last_success_at, lang)} · ${summary.postsCollected.toLocaleString()} активностей`
                    : `Last cycle ${formatSocialRelativeTime(overview.runtime.last_success_at, lang)} · ${summary.postsCollected.toLocaleString()} activities`)
                  : (ru
                    ? `${summary.postsCollected.toLocaleString()} активностей за выбранное окно`
                    : `${summary.postsCollected.toLocaleString()} activities in the selected window`)}
              </p>
            </div>
          </div>
          <div className="hidden sm:flex items-center gap-1.5">
            <Clock className="w-3.5 h-3.5 text-blue-600" />
            <span className="text-xs text-blue-600">
              {ru ? 'Обновление по новым данным' : 'Refreshes from new data'}
            </span>
          </div>
        </div>

        <div className="bg-gradient-to-r from-sky-50/70 to-blue-50/40 border border-sky-100 rounded-lg p-4">
          <div className="flex items-center gap-1.5 mb-2">
            <BarChart3 className="w-3 h-3 text-sky-500" />
            <span className="text-xs text-sky-600" style={{ fontWeight: 600, letterSpacing: '0.03em' }}>
              {ru ? `Снимок за ${selectedWindowLabel(rangeDays, ru)}` : `Snapshot for the ${selectedWindowLabel(rangeDays, ru)}`}
            </span>
          </div>
          <p className="text-sm text-gray-800 leading-relaxed">
            {ru
              ? `Social surface собрала ${summary.postsCollected.toLocaleString()} активностей по ${summary.trackedCompetitors} конкурентам. Доминирующая тема: ${summary.dominantTopic.name} (${summary.dominantTopic.count.toLocaleString()} упоминаний).`
              : `The social surface collected ${summary.postsCollected.toLocaleString()} activities across ${summary.trackedCompetitors} competitors. The dominant topic is ${summary.dominantTopic.name} with ${summary.dominantTopic.count.toLocaleString()} mentions.`}
          </p>

          {expanded ? (
            <div className="mt-3 pt-3 border-t border-sky-100 space-y-3">
              <p className="text-sm text-gray-800 leading-relaxed">
                {ru
                  ? `Средний sentiment держится на уровне ${formatSocialPercent(summary.averageSentimentPct)}, а рекламных сигналов в окне обнаружено ${summary.adsDetected.toLocaleString()}.`
                  : `Average sentiment is holding at ${formatSocialPercent(summary.averageSentimentPct)}, with ${summary.adsDetected.toLocaleString()} ad signals detected in the window.`}
              </p>
              <div className="flex flex-wrap gap-2">
                <button
                  type="button"
                  onClick={() => onOpenTopic(summary.dominantTopic.name)}
                  className="inline-flex items-center gap-1 rounded-full border border-sky-200 bg-white px-3 py-1 text-xs text-sky-700 hover:bg-sky-50"
                  style={{ fontWeight: 500 }}
                >
                  {ru ? 'Открыть тему' : 'Open topic'}
                  <ArrowRight className="h-3.5 w-3.5" />
                </button>
                <button
                  type="button"
                  onClick={onOpenOps}
                  className="inline-flex items-center gap-1 rounded-full border border-sky-200 bg-white px-3 py-1 text-xs text-sky-700 hover:bg-sky-50"
                  style={{ fontWeight: 500 }}
                >
                  {ru ? 'Открыть Ops' : 'Open Ops'}
                  <ArrowRight className="h-3.5 w-3.5" />
                </button>
              </div>
            </div>
          ) : null}
        </div>

        <div className="mt-3 grid grid-cols-2 md:grid-cols-4 gap-3">
          <button
            type="button"
            onClick={onOpenOps}
            className="bg-blue-50 border border-blue-100 rounded-lg px-3 py-2 text-center"
          >
            <MessageSquareText className="w-4 h-4 text-blue-600 mx-auto mb-1" />
            <span className="text-xs text-blue-900 block" style={{ fontWeight: 600 }}>{summary.trackedCompetitors.toLocaleString()}</span>
            <span className="text-xs text-blue-600">{ru ? 'Активных конкурентов' : 'Active competitors'}</span>
          </button>
          <button
            type="button"
            onClick={() => onOpenEvidence({
              title: ru ? 'Все активности' : 'All activities',
              description: ru ? 'Все собранные post / comment / ad активности за период.' : 'Every collected post, comment, and ad activity for this period.',
              filters: {},
            })}
            className="bg-emerald-50 border border-emerald-100 rounded-lg px-3 py-2 text-center"
          >
            <MessageSquareText className="w-4 h-4 text-emerald-600 mx-auto mb-1" />
            <span className="text-xs text-emerald-900 block" style={{ fontWeight: 600 }}>{summary.postsCollected.toLocaleString()}</span>
            <span className="text-xs text-emerald-600">{ru ? 'Активностей собрано' : 'Activities collected'}</span>
          </button>
          <button
            type="button"
            onClick={() => onOpenEvidence({
              title: ru ? 'Тональность' : 'Sentiment evidence',
              description: ru ? 'Активности, формирующие среднюю тональность окна.' : 'Evidence behind the average sentiment for this social window.',
              filters: {},
            })}
            className="bg-purple-50 border border-purple-100 rounded-lg px-3 py-2 text-center"
          >
            <Heart className="w-4 h-4 text-purple-600 mx-auto mb-1" />
            <span className={cn('text-xs block', sentimentClass)} style={{ fontWeight: 600 }}>{formatSocialPercent(summary.averageSentimentPct)}</span>
            <span className="text-xs text-purple-600">{ru ? 'Средний sentiment' : 'Average sentiment'}</span>
          </button>
          <button
            type="button"
            onClick={() => onOpenEvidence({
              title: ru ? 'Обнаруженные объявления' : 'Detected ads',
              description: ru ? 'Все ad-сигналы и рекламные материалы.' : 'All ad signals and ad-like creative captured in the period.',
              filters: { sourceKind: 'ad' },
            })}
            className="bg-amber-50 border border-amber-100 rounded-lg px-3 py-2 text-center"
          >
            <Zap className="w-4 h-4 text-amber-600 mx-auto mb-1" />
            <span className="text-xs text-amber-900 block" style={{ fontWeight: 600 }}>{summary.adsDetected.toLocaleString()}</span>
            <span className="text-xs text-amber-600">{ru ? 'Объявлений' : 'Ads detected'}</span>
          </button>
        </div>

        <button
          type="button"
          onClick={() => setExpanded((current) => !current)}
          className="mt-3 flex items-center gap-1 text-xs text-blue-600 hover:text-blue-700"
        >
          {expanded
            ? (ru ? 'Свернуть' : 'Show less')
            : (ru ? 'Как читать эти метрики' : 'How to read these metrics')}
          <ChevronRight className={`w-3.5 h-3.5 transition-transform ${expanded ? 'rotate-90' : ''}`} />
        </button>
      </div>
    </div>
  );
}

export function SocialTrendingTopicsCard({
  lang,
  topics,
  onOpenTopic,
}: {
  lang: Lang;
  topics: SocialTopicListItem[];
  onOpenTopic: (topic: string) => void;
}) {
  const ru = lang === 'ru';

  return (
    <DashboardCard
      widgetId="trending_topics_feed"
      title={ru ? 'Trending Topics' : 'Trending Topics'}
      description={ru ? 'Темы, которые задают текущий social-разговор.' : 'The themes currently driving the social conversation.'}
      meta={ru ? 'Текущие сигналы' : 'Current signals'}
    >
      <div className="mb-4 flex items-center justify-between gap-3">
        <div className="flex items-center gap-2">
          <div className="w-2 h-2 bg-emerald-500 rounded-full animate-pulse" />
          <span className="text-xs text-gray-500">
            {ru ? 'Темы, которые двигают окно прямо сейчас' : 'Themes moving the selected window right now'}
          </span>
        </div>
        <button
          type="button"
          onClick={() => onOpenTopic(topics[0]?.topic || '')}
          className="inline-flex items-center gap-1 text-xs text-blue-600 hover:text-blue-700"
          style={{ fontWeight: 500 }}
          disabled={!topics.length}
        >
          {ru ? 'Открыть темы' : 'Open topics'}
          <ArrowRight className="h-3.5 w-3.5" />
        </button>
      </div>
      {topics.length === 0 ? (
        <CardEmptyState message={ru ? 'Темы пока не обнаружены.' : 'No social topics have been detected yet.'} />
      ) : (
        <div className="space-y-3">
          {topics.slice(0, 6).map((topic, index) => (
            <button
              key={topic.topic}
              type="button"
              onClick={() => onOpenTopic(topic.topic)}
              className="flex w-full items-start justify-between gap-3 rounded-xl border border-border bg-muted/30 p-3 text-left transition hover:border-primary/30 hover:bg-accent/40"
            >
              <div className="min-w-0 flex-1">
                <div className="flex items-center gap-2">
                  <span className="h-2.5 w-2.5 rounded-full" style={{ backgroundColor: socialEntityColor(index) }} />
                  <p className="truncate text-sm font-semibold text-foreground">{topic.topic}</p>
                </div>
                <p className="mt-2 line-clamp-2 text-xs text-muted-foreground">
                  {topic.sampleSummary || (ru ? 'Краткое summary появится после следующего анализа.' : 'A brief summary will appear once more analyzed activity arrives.')}
                </p>
                <div className="mt-2 flex flex-wrap items-center gap-2 text-[11px] text-muted-foreground">
                  <span>{topic.count.toLocaleString()} {ru ? 'упоминаний' : 'mentions'}</span>
                  <span className={growthTone(topic.growthPct)}>{formatDelta(topic.growthPct, ru)}</span>
                </div>
              </div>
              <ArrowRight className="mt-1 h-4 w-4 flex-shrink-0 text-muted-foreground" />
            </button>
          ))}
        </div>
      )}
    </DashboardCard>
  );
}

export function SocialTopicLandscapeCard({
  lang,
  topics,
  onOpenTopic,
}: {
  lang: Lang;
  topics: SocialTopicListItem[];
  onOpenTopic: (topic: string) => void;
}) {
  const ru = lang === 'ru';
  const totalMentions = topics.reduce((sum, topic) => sum + topic.count, 0);
  const primaryCategories = useMemo(() => {
    const counts = new Map<string, { topics: number; mentions: number; color: string }>();
    topics.forEach((topic, index) => {
      const label = topic.topPlatforms[0] || (ru ? 'Смешанные платформы' : 'Mixed platforms');
      const current = counts.get(label) || { topics: 0, mentions: 0, color: landscapeColor(index) };
      current.topics += 1;
      current.mentions += topic.count;
      counts.set(label, current);
    });
    return Array.from(counts.entries())
      .map(([label, meta]) => ({ label, ...meta }))
      .sort((a, b) => b.mentions - a.mentions)
      .slice(0, 6);
  }, [topics, ru]);
  const maxValue = Math.max(...topics.map((topic) => topic.count), 1);
  const minValue = Math.min(...topics.map((topic) => topic.count), maxValue);

  return (
    <DashboardCard
      widgetId="topic_landscape"
      title={ru ? 'Topic Landscape' : 'Topic Landscape'}
      description={ru ? 'Карта тем по объёму, тону и вовлечённым сущностям.' : 'A landscape of social topics by volume, tone, and participating entities.'}
      meta={ru ? 'Что обсуждают чаще всего' : 'Most discussed topics'}
    >
      {topics.length === 0 ? (
        <CardEmptyState message={ru ? 'Ландшафт тем пока пуст.' : 'The topic landscape is empty for this time window.'} />
      ) : (
        <>
          <div className="flex flex-wrap items-center gap-2 mb-4">
            {primaryCategories.map((category) => (
              <span
                key={category.label}
                className="text-xs px-2.5 py-1 rounded-full border bg-white text-gray-600 border-gray-200"
                style={{ fontWeight: 500 }}
              >
                <span className="inline-flex items-center gap-1.5">
                  <span className="w-2 h-2 rounded-full" style={{ backgroundColor: category.color }} />
                  <span>{category.label}</span>
                  <span className="text-gray-400">· {category.topics}</span>
                </span>
              </span>
            ))}
          </div>

          <div className="grid grid-cols-2 md:grid-cols-8 xl:grid-cols-12 auto-rows-[64px] gap-3">
            {topics.map((topic, index) => {
              const spec = socialTopicTileSpec(topic.count, minValue, maxValue);
              const growthLabel = `${topic.growthPct > 0 ? '+' : ''}${topic.growthPct}%`;
              const growthClass = topic.growthPct >= 0 ? 'text-emerald-600' : 'text-red-500';
              const tagLabel = topic.topPlatforms[0] || topic.topEntities[0] || (ru ? 'Social' : 'Social');
              const borderColor = `${landscapeColor(index)}55`;
              const accentColor = `${landscapeColor(index)}D9`;

              return (
                <button
                  key={topic.topic}
                  type="button"
                  onClick={() => onOpenTopic(topic.topic)}
                  className={`${spec.tileClass} relative rounded-xl border bg-white p-2 md:p-3 overflow-hidden hover:shadow-sm transition-shadow text-left`}
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
                      title={topic.topic}
                    >
                      {topic.topic}
                    </span>

                    {spec.showSnippet && topic.sampleSummary && (
                      <span
                        className="text-[11px] text-gray-600 leading-tight"
                        style={{
                          display: '-webkit-box',
                          WebkitLineClamp: 2,
                          WebkitBoxOrient: 'vertical',
                          overflow: 'hidden',
                        }}
                        title={topic.sampleSummary}
                      >
                        {topic.sampleSummary}
                      </span>
                    )}

                    {spec.compactMeta ? (
                      <div className="mt-auto flex items-center justify-between gap-2">
                        <div className="min-w-0 text-[11px] text-gray-600">
                          {topic.count.toLocaleString()} {ru ? 'упоминаний' : 'mentions'}
                        </div>
                        <div className={`shrink-0 text-[11px] ${growthClass}`} style={{ fontWeight: 700 }}>
                          {growthLabel}
                        </div>
                      </div>
                    ) : (
                      <div className="mt-auto flex items-end justify-between gap-2">
                        <div className="min-w-0">
                          <div className="text-[11px] text-gray-600">
                            {topic.count.toLocaleString()} {ru ? 'упоминаний' : 'mentions'}
                          </div>
                          {spec.showMeta ? (
                            <div className="text-[10px] text-gray-500">
                              {ru ? 'платформа' : 'platform'}: {tagLabel}
                            </div>
                          ) : null}
                        </div>

                        <div className="text-right">
                          <div className="text-[10px] text-gray-500" style={{ fontWeight: 600 }}>
                            {ru ? '7д Δ' : '7d Δ'}
                          </div>
                          <div className={`text-xs ${growthClass}`} style={{ fontWeight: 700 }}>
                            {growthLabel}
                          </div>
                          {spec.showMeta ? (
                            <div className="text-[10px] text-gray-500">
                              {topic.previousCount > 0 ? (ru ? 'статистика достаточна' : 'evidence sufficient') : (ru ? 'мало данных' : 'low evidence')}
                            </div>
                          ) : null}
                        </div>
                      </div>
                    )}
                  </div>
                </button>
              );
            })}
          </div>

          <div className="flex flex-wrap items-center gap-4 mt-4 pt-3 border-t border-gray-100">
            <span className="text-xs text-gray-500">
              {ru ? 'Цвет полоски = ведущая платформа/кластер темы' : 'Top strip color = lead platform/topic cluster'}
            </span>
            <span className="text-xs text-gray-500">
              {ru ? '7д Δ = изменение к предыдущему окну' : '7d Δ = change vs previous window'}
            </span>
            <button
              type="button"
              onClick={() => onOpenTopic(topics[0]?.topic || '')}
              className="ml-auto text-xs text-blue-600 hover:text-blue-800 hover:underline transition-colors"
              style={{ fontWeight: 500 }}
            >
              {ru ? 'Все темы →' : 'See all topics →'}
            </button>
          </div>

          <p className="text-xs text-gray-400 mt-2">
            {ru
              ? `Основа: ${totalMentions.toLocaleString()} прямых упоминаний в окне по ${topics.length} темам.`
              : `Evidence: ${totalMentions.toLocaleString()} direct mentions in the selected window across ${topics.length} topics.`}
          </p>
        </>
      )}
    </DashboardCard>
  );
}

export function SocialConversationTrendsCard({
  lang,
  series,
  onOpenTopic,
}: {
  lang: Lang;
  series: SocialTopicTrendSeries[];
  onOpenTopic: (topic: string) => void;
}) {
  const ru = lang === 'ru';
  const data = useMemo(() => mergeTopicTrendData(series), [series]);
  const top2 = useMemo(() => {
    return [...series]
      .map((entry) => {
        const first = entry.items[0]?.total ?? 0;
        const last = entry.items[entry.items.length - 1]?.total ?? 0;
        const change = first > 0 ? Math.round(((last - first) / first) * 100) : (last > 0 ? 100 : 0);
        return { topic: entry.topic, change };
      })
      .sort((a, b) => b.change - a.change)
      .slice(0, 2);
  }, [series]);

  return (
    <DashboardCard
      widgetId="conversation_trends"
      title={ru ? 'Conversation Trends' : 'Conversation Trends'}
      description={ru ? 'Динамика по ведущим темам внутри выбранного social-окна.' : 'How the leading topics moved across the selected social window.'}
      meta={ru ? `${series.length} активных рядов` : `${series.length} active series`}
    >
      {series.length === 0 || data.length === 0 ? (
        <CardEmptyState message={ru ? 'Пока нет трендовых рядов по темам.' : 'No topic trend series are available yet.'} />
      ) : (
        <>
          <ResponsiveContainer width="100%" height={240}>
            <LineChart data={data} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
              <XAxis
                dataKey="bucket"
                tick={{ fontSize: 10 }}
                stroke="#94a3b8"
                tickFormatter={(value) => formatSocialBucket(String(value), lang)}
                interval="preserveStartEnd"
                minTickGap={24}
              />
              <YAxis hide />
              <Tooltip
                labelFormatter={(value) => formatSocialDateLabel(String(value), lang)}
                contentStyle={{ borderRadius: 12, borderColor: '#e2e8f0' }}
              />
              {series.map((entry, index) => (
                <Line
                  key={entry.topic}
                  type="monotone"
                  dataKey={entry.topic}
                  stroke={socialEntityColor(index)}
                  strokeWidth={2.25}
                  dot={false}
                  activeDot={{ r: 4 }}
                />
              ))}
            </LineChart>
          </ResponsiveContainer>
          <div className="mt-4 flex flex-wrap gap-2">
            {series.map((entry, index) => (
              <button
                key={entry.topic}
                type="button"
                onClick={() => onOpenTopic(entry.topic)}
                className="inline-flex items-center gap-2 rounded-full border border-border bg-background px-3 py-1 text-xs font-medium text-foreground transition hover:bg-accent hover:text-accent-foreground"
              >
                <span className="h-2 w-2 rounded-full" style={{ backgroundColor: socialEntityColor(index) }} />
                {entry.topic}
              </button>
            ))}
          </div>

          {top2.length >= 2 ? (
            <div className="mt-3 pt-3 border-t border-gray-100">
              <div className="bg-emerald-50 border border-emerald-100 rounded-lg px-3 py-2">
                <p className="text-xs text-emerald-800">
                  {ru
                    ? <><span style={{ fontWeight: 600 }}>Быстрее всего растут:</span> {top2[0].topic} ({top2[0].change > 0 ? '+' : ''}{top2[0].change}%) и {top2[1].topic} ({top2[1].change > 0 ? '+' : ''}{top2[1].change}%)</>
                    : <><span style={{ fontWeight: 600 }}>Fastest growing:</span> {top2[0].topic} ({top2[0].change > 0 ? '+' : ''}{top2[0].change}%) and {top2[1].topic} ({top2[1].change > 0 ? '+' : ''}{top2[1].change}%)</>
                  }
                </p>
              </div>
            </div>
          ) : null}
        </>
      )}
    </DashboardCard>
  );
}

export function SocialProblemSignalsCard({
  lang,
  painPoints,
  customerIntent,
  onOpenPainPoint,
  onOpenIntent,
}: {
  lang: Lang;
  painPoints: SocialAudienceSignalRow[];
  customerIntent: SocialAudienceSignalRow[];
  onOpenPainPoint: (label: string) => void;
  onOpenIntent: (label: string) => void;
}) {
  const ru = lang === 'ru';
  const problemCards = [...painPoints, ...customerIntent.slice(0, 3)].map((item) => {
    const severity = item.count >= 12 ? 'critical' : item.count >= 8 ? 'high' : item.count >= 4 ? 'medium' : 'low';
    const colors = severity === 'critical'
      ? { bg: 'bg-red-100', text: 'text-red-800', border: 'border-red-300' }
      : severity === 'high'
        ? { bg: 'bg-red-50', text: 'text-red-700', border: 'border-red-200' }
        : severity === 'medium'
          ? { bg: 'bg-amber-50', text: 'text-amber-700', border: 'border-amber-200' }
          : { bg: 'bg-gray-50', text: 'text-gray-600', border: 'border-gray-200' };
    return { ...item, severity, colors };
  });

  return (
    <DashboardCard
      widgetId="problem_tracker"
      title={ru ? 'Problem Signals' : 'Problem Signals'}
      description={ru ? 'Повторяющиеся боли и ожидания, подтверждённые social-evidence.' : 'Recurring pain points and customer intents backed by social evidence.'}
    >
      {problemCards.length === 0 ? (
        <CardEmptyState message={ru ? 'Пока нет выраженных pain-point сигналов.' : 'No strong social problem signals have surfaced yet.'} />
      ) : (
        <div className="mb-4 space-y-2.5">
          <p className="text-xs text-gray-500">
            {ru
              ? 'Social problem cards: формулируются простым языком и привязаны к реальным social signals.'
              : 'Social problem cards: plain-language statements grounded in real social signals.'}
          </p>
          {problemCards.slice(0, 6).map((brief) => {
            const isPainPoint = painPoints.some((item) => item.label === brief.label);
            const open = () => {
              if (isPainPoint) onOpenPainPoint(brief.label);
              else onOpenIntent(brief.label);
            };
            return (
              <div key={brief.label} className={`${brief.colors.bg} ${brief.colors.border} border rounded-lg p-3`}>
                <div className="flex items-start justify-between gap-3">
                  <div className="min-w-0">
                    <div className="text-[11px] opacity-80">
                      {isPainPoint ? (ru ? 'Pain point' : 'Pain point') : (ru ? 'Customer intent' : 'Customer intent')}
                    </div>
                    <div className="text-sm leading-snug" style={{ fontWeight: 700 }}>{brief.label}</div>
                    <p className="text-xs mt-1 opacity-90">
                      {(brief.entities || []).slice(0, 3).join(' · ') || (ru ? 'Сигнал без явно выделенной сущности.' : 'Signal without a dominant named entity.')}
                    </p>
                  </div>
                  <span className="text-[10px] px-1.5 py-0.5 rounded border border-current/20" style={{ fontWeight: 600 }}>
                    {brief.count} {ru ? 'сигн.' : 'signals'}
                  </span>
                </div>

                <div className="text-[11px] mt-2 opacity-85">
                  {(brief.entities || []).length} {ru ? 'затронутых сущностей' : 'entities involved'} · {brief.dominantSentiment}
                </div>

                <button
                  type="button"
                  onClick={open}
                  className="mt-2 text-xs text-blue-700 hover:underline"
                >
                  {ru ? 'Открыть доказательства' : 'Open evidence'}
                </button>
              </div>
            );
          })}
        </div>
      )}
    </DashboardCard>
  );
}

export function SocialTopEntitiesCard({
  lang,
  competitors,
  onOpenEntityEvidence,
  onOpenTopic,
}: {
  lang: Lang;
  competitors: SocialCompetitorRow[];
  onOpenEntityEvidence: (entityId: string, entityName: string) => void;
  onOpenTopic: (topic: string) => void;
}) {
  const ru = lang === 'ru';

  return (
    <DashboardCard
      widgetId="top_channels"
      title={ru ? 'Top Entities / Accounts' : 'Top Entities / Accounts'}
      description={ru ? 'Какие бренды и аккаунты генерируют больше всего активности.' : 'Which brands and accounts are driving the most social activity.'}
      meta={ru ? `${competitors.length} активных сущностей` : `${competitors.length} active entities`}
    >
      {competitors.length === 0 ? (
        <CardEmptyState message={ru ? 'Нет активных сущностей в social-окне.' : 'No active social entities were found in this window.'} />
      ) : (
        <div className="space-y-2">
          {competitors.slice(0, 6).map((row, index) => {
            const tone = sentimentTone(row.avgSentimentScore);
            const badge = row.topMarketingIntent || row.keyTopics[0] || (ru ? 'Social' : 'Social');
            return (
              <div
                key={row.entityId}
                className="flex items-center gap-3 py-2 px-2 rounded-lg hover:bg-gray-50 transition-colors"
              >
                <div className="w-8 h-8 rounded-full flex items-center justify-center flex-shrink-0"
                  style={{ backgroundColor: `${socialEntityColor(index)}20` }}>
                  <span className="text-xs" style={{ fontWeight: 700, color: socialEntityColor(index) }}>{index + 1}</span>
                </div>
                <div className="flex-1 min-w-0">
                  <button
                    type="button"
                    onClick={() => onOpenEntityEvidence(row.entityId, row.entityName)}
                    className="flex items-center gap-2 min-w-0 text-left"
                  >
                    <span className="text-xs text-gray-900 truncate" style={{ fontWeight: 500 }}>{row.entityName}</span>
                    <span
                      className="text-xs px-1.5 py-0.5 rounded"
                      style={{
                        backgroundColor: `${socialEntityColor(index)}15`,
                        color: socialEntityColor(index),
                        fontWeight: 500,
                      }}
                    >
                      {badge}
                    </span>
                  </button>
                  <div className="flex items-center gap-3 mt-0.5 text-xs text-gray-400">
                    <span>{row.posts >= 1000 ? `${(row.posts / 1000).toFixed(1)}K` : row.posts} {ru ? 'актив.' : 'activities'}</span>
                    <span>{row.adsRunning > 0 ? `${row.adsRunning} ${ru ? 'ads' : 'ads'}` : ru ? 'Без ads' : 'No ads'}</span>
                    {row.keyTopics[0] ? (
                      <button type="button" onClick={() => onOpenTopic(row.keyTopics[0])} className="text-emerald-500">
                        {row.keyTopics[0]}
                      </button>
                    ) : null}
                  </div>
                </div>
                <div className="text-right flex-shrink-0">
                  <div className="text-xs text-gray-900" style={{ fontWeight: 600 }}>
                    {formatSocialPercent(((row.avgSentimentScore + 1) / 2) * 100)}
                  </div>
                  <div className="text-xs text-gray-400">{ru ? 'тон' : 'tone'}</div>
                </div>
              </div>
            );
          })}
        </div>
      )}
    </DashboardCard>
  );
}

export function SocialWeekOverWeekCard({
  lang,
  summary,
  previousSummary,
  topics,
}: {
  lang: Lang;
  summary: SocialSummaryResponse | null;
  previousSummary: SocialSummaryResponse | null;
  topics: SocialTopicListItem[];
}) {
  const ru = lang === 'ru';
  const [showDetails, setShowDetails] = useState(false);
  const fastestMover = useMemo(() => [...topics].sort((a, b) => (b.deltaCount - a.deltaCount) || (b.count - a.count))[0] || null, [topics]);

  const activityDelta = summary && previousSummary?.postsCollected
    ? Math.round(((summary.postsCollected - previousSummary.postsCollected) / Math.max(1, previousSummary.postsCollected)) * 100)
    : (summary ? 100 : 0);
  const adsDelta = summary && previousSummary?.adsDetected
    ? Math.round(((summary.adsDetected - previousSummary.adsDetected) / Math.max(1, previousSummary.adsDetected)) * 100)
    : (summary ? 100 : 0);
  const sentimentDelta = summary && previousSummary
    ? (summary.averageSentimentPct - previousSummary.averageSentimentPct)
    : 0;

  return (
    <DashboardCard
      widgetId="week_over_week_shifts"
      title={ru ? 'Week-over-Week Shifts' : 'Week-over-Week Shifts'}
      description={ru ? 'Сравнение выбранного окна с предыдущим эквивалентным периодом.' : 'How the selected social window compares with the previous equivalent period.'}
    >
      {!summary || !previousSummary ? (
        <CardEmptyState message={ru ? 'Недостаточно данных для сравнения периодов.' : 'Not enough data is available to compare periods yet.'} />
      ) : (
        <>
          <div className="grid grid-cols-3 gap-2">
            {[
              {
                label: ru ? 'Объём активности' : 'Activity volume',
                current: summary.postsCollected,
                previous: previousSummary.postsCollected,
                delta: activityDelta,
              },
              {
                label: ru ? 'Ad pressure' : 'Ad pressure',
                current: summary.adsDetected,
                previous: previousSummary.adsDetected,
                delta: adsDelta,
              },
              {
                label: ru ? 'Тональность' : 'Sentiment shift',
                current: Math.round(summary.averageSentimentPct),
                previous: Math.round(previousSummary.averageSentimentPct),
                delta: Math.round(sentimentDelta),
              },
            ].map((item) => {
              const isUp = item.delta > 0;
              const isFlat = item.delta === 0;
              const icon = isFlat ? <Minus className="w-3 h-3 text-gray-400" /> : isUp ? <ArrowUpRight className={`w-3 h-3 ${growthTone(item.delta)}`} /> : <ArrowDownRight className={`w-3 h-3 ${growthTone(item.delta)}`} />;
              const pctChange = item.previous === 0
                ? (item.current === 0 ? '0.0%' : (ru ? 'нов.' : 'new'))
                : `${isUp ? '+' : ''}${((item.current - item.previous) / item.previous * 100).toFixed(1)}%`;
              return (
                <div key={item.label} className="bg-gray-50 rounded-lg p-2.5">
                  <span className="text-xs text-gray-500 block truncate">{item.label}</span>
                  <div className="flex items-center justify-between mt-1">
                    <span className="text-sm text-gray-900" style={{ fontWeight: 600 }}>
                      {item.current.toLocaleString()}
                    </span>
                    <div className="flex items-center gap-0.5">
                      {icon}
                      <span className={isFlat ? 'text-gray-400' : growthTone(item.delta)} style={{ fontWeight: 600, fontSize: '10px' }}>
                        {pctChange}
                      </span>
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
          <div className="mt-3">
            <button
              type="button"
              onClick={() => setShowDetails((current) => !current)}
              className="text-xs text-teal-700 hover:text-teal-800"
              style={{ fontWeight: 600 }}
            >
              {showDetails
                ? (ru ? 'Свернуть детали' : 'Hide details')
                : (ru ? 'Показать детали окна' : 'Show window details')}
            </button>
          </div>

          {showDetails ? (
            <div className="mt-3 grid grid-cols-2 gap-3">
              <div className="bg-emerald-50 border border-emerald-100 rounded-lg px-3 py-2">
                <span className="text-xs text-emerald-900 block" style={{ fontWeight: 600 }}>
                  {ru ? 'Самый быстрый рост' : 'Fastest mover'}
                </span>
                <p className="text-xs text-emerald-700">
                  {fastestMover
                    ? `${fastestMover.topic} (${fastestMover.deltaCount > 0 ? '+' : ''}${fastestMover.deltaCount} ${ru ? 'упоминаний' : 'mentions'})`
                    : (ru ? 'Нет достаточных изменений' : 'No material topic changes yet')}
                </p>
              </div>
              <div className="bg-blue-50 border border-blue-100 rounded-lg px-3 py-2">
                <span className="text-xs text-blue-900 block" style={{ fontWeight: 600 }}>
                  {ru ? 'Сравнение окна' : 'Window comparison'}
                </span>
                <p className="text-xs text-blue-700">
                  {ru
                    ? `${summary.postsCollected.toLocaleString()} vs ${previousSummary.postsCollected.toLocaleString()} активностей`
                    : `${summary.postsCollected.toLocaleString()} vs ${previousSummary.postsCollected.toLocaleString()} activities`}
                </p>
              </div>
            </div>
          ) : null}
        </>
      )}
    </DashboardCard>
  );
}

export function SocialSentimentByTopicCard({
  lang,
  topics,
  onOpenTopic,
}: {
  lang: Lang;
  topics: SocialTopicListItem[];
  onOpenTopic: (topic: string) => void;
}) {
  const ru = lang === 'ru';
  const [showAll, setShowAll] = useState(false);
  const sortedByPositive = [...topics].sort((a, b) => b.sentimentCounts.positive - a.sentimentCounts.positive);
  const sortedByNegative = [...topics].sort((a, b) => b.sentimentCounts.negative - a.sentimentCounts.negative);
  const topPositive = sortedByPositive.slice(0, 3);
  const topNegative = sortedByNegative.slice(0, 2);
  const visibleRows = showAll ? topics : topics.slice(0, 10);

  return (
    <DashboardCard
      widgetId="sentiment_by_topic"
      title={ru ? 'Sentiment by Topic' : 'Sentiment by Topic'}
      description={ru ? 'Как распределяется тональность внутри ведущих тем.' : 'How the sentiment mix breaks across the leading themes.'}
    >
      {topics.length === 0 ? (
        <CardEmptyState message={ru ? 'Тональность по темам пока недоступна.' : 'Topic-level sentiment is not available yet.'} />
      ) : (
        <>
          <div className="space-y-2.5">
          {visibleRows.map((topic) => {
            const total = topic.sentimentCounts.positive + topic.sentimentCounts.neutral + topic.sentimentCounts.negative || 1;
            return (
              <button
                key={topic.topic}
                type="button"
                onClick={() => onOpenTopic(topic.topic)}
                className="w-full rounded-xl border border-border bg-muted/30 p-3 text-left transition hover:border-primary/30 hover:bg-accent/40"
              >
                <div className="flex items-center justify-between gap-3">
                  <span className="text-xs text-gray-700" style={{ fontWeight: 500 }}>{topic.topic}</span>
                  <span className="text-xs text-gray-400">{topic.count.toLocaleString()}</span>
                </div>
                <div className="mt-3 flex h-2 overflow-hidden rounded-full bg-muted">
                  <div className="bg-emerald-400" style={{ width: `${(topic.sentimentCounts.positive / total) * 100}%` }} />
                  <div className="bg-slate-300" style={{ width: `${(topic.sentimentCounts.neutral / total) * 100}%` }} />
                  <div className="bg-rose-400" style={{ width: `${(topic.sentimentCounts.negative / total) * 100}%` }} />
                </div>
              </button>
            );
          })}
          </div>

          {topics.length > 10 ? (
            <div className="mt-3">
              <button
                type="button"
                onClick={() => setShowAll((prev) => !prev)}
                className="text-xs text-teal-700 hover:text-teal-800"
                style={{ fontWeight: 600 }}
              >
                {showAll
                  ? (ru ? 'Свернуть' : 'See top 10')
                  : (ru ? `Показать все ${topics.length}` : `See all ${topics.length}`)}
              </button>
            </div>
          ) : null}

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
                {topPositive.map((topic) => `${topic.topic} (${topic.sentimentCounts.positive})`).join(', ')}
              </p>
            </div>
            <div className="bg-red-50 border border-red-100 rounded-lg px-3 py-2">
              <span className="text-xs text-red-900 block" style={{ fontWeight: 600 }}>
                {ru ? 'Решайте эти проблемы' : 'Help with these'}
              </span>
              <p className="text-xs text-red-700">
                {topNegative.map((topic) => `${topic.topic} (${topic.sentimentCounts.negative} ${ru ? 'нег.' : 'negative'})`).join(', ')}
              </p>
            </div>
          </div>
        </>
      )}
    </DashboardCard>
  );
}

export function SocialContentPerformanceCard({
  lang,
  ads,
  topMarketingIntent,
  topCtaType,
  topProduct,
  onOpenEvidence,
}: {
  lang: Lang;
  ads: SocialAdCard[];
  topMarketingIntent: string | null;
  topCtaType: string | null;
  topProduct: string | null;
  onOpenEvidence: (input: SocialEvidenceRequestInput) => void;
}) {
  const ru = lang === 'ru';
  const formatPerformance = useMemo(() => computeFormatPerformance(ads), [ads]);
  const sortedFormats = [...formatPerformance].sort((a, b) => b.avgEngagement - a.avgEngagement);
  const topFormats = sortedFormats.slice(0, 2);
  const topAds = [...ads].sort((a, b) => b.engagementTotal - a.engagementTotal);

  return (
    <DashboardCard
      widgetId="content_performance"
      title={ru ? 'Content Performance' : 'Content Performance'}
      description={ru ? 'Какие форматы и креативы тянут social-результат.' : 'Which formats and creatives are carrying the strongest social signal.'}
    >
      {ads.length === 0 ? (
        <CardEmptyState message={ru ? 'Рекламные и контентные сигналы пока отсутствуют.' : 'No ad or content performance signals are available yet.'} />
      ) : (
        <>
          <div className="mb-4">
            <span className="text-xs text-gray-500 block mb-2" style={{ fontWeight: 500 }}>
              {ru ? 'Средняя вовлечённость по формату' : 'Avg engagement by format'}
            </span>
            <div className="space-y-1.5">
              {sortedFormats.map((item) => (
                <button
                  key={item.name}
                  type="button"
                  onClick={() => onOpenEvidence({
                    title: item.name,
                    description: ru ? 'Evidence для формата контента.' : 'Evidence filtered to this content format.',
                    filters: { contentFormat: item.name },
                  })}
                  className="flex items-center gap-2 w-full text-left"
                >
                  <span className="text-xs text-gray-700 w-28" style={{ fontWeight: 500 }}>{item.name}</span>
                  <div className="flex-1 bg-gray-100 rounded-full h-2">
                    <div
                      className="h-2 rounded-full bg-teal-500"
                      style={{ width: `${Math.min(item.avgEngagement, 100)}%`, opacity: 0.5 + (Math.min(item.avgEngagement, 100) / 100) * 0.5 }}
                    />
                  </div>
                  <span className="text-xs text-gray-900 w-8 text-right" style={{ fontWeight: 600 }}>{item.avgEngagement}</span>
                  <span className="text-xs text-gray-400 w-16 text-right">
                    {item.items} {ru ? 'публ.' : 'posts'}
                  </span>
                </button>
              ))}
            </div>
          </div>

          <div className="pt-3 border-t border-gray-100">
            <span className="text-xs text-gray-500 block mb-2" style={{ fontWeight: 500 }}>
              {ru ? 'Лучшие публикации' : 'Top performing posts'}
            </span>
            <div className="space-y-2">
              {topAds.slice(0, 5).map((ad) => (
                <button
                  key={ad.id}
                  type="button"
                  onClick={() => onOpenEvidence({
                    title: ad.entity?.name || (ru ? 'Креатив' : 'Creative'),
                    description: ru ? 'Детали social-актива.' : 'Inspect the evidence behind this social activity.',
                    filters: { activityUid: ad.activity_uid },
                  })}
                  className="flex items-center gap-2 py-1 w-full text-left"
                >
                  <Star className="w-3 h-3 text-amber-400 flex-shrink-0" />
                  <span className="text-xs text-gray-700 flex-1 truncate">{socialActivitySummary(ad)}</span>
                  <span className="text-xs px-1.5 py-0.5 rounded bg-gray-100 text-gray-500">{ad.content_format || ad.source_kind || 'post'}</span>
                  <span className="text-xs text-gray-500">{ad.engagementTotal} {ru ? 'eng.' : 'eng.'}</span>
                </button>
              ))}
            </div>
          </div>

          <div className="mt-3 bg-blue-50 border border-blue-100 rounded-lg px-3 py-2">
            <p className="text-xs text-blue-800">
              {topFormats.length >= 2 && (ru
                ? <><span style={{ fontWeight: 600 }}>Стратегия:</span> {topFormats[0].name} ({topFormats[0].avgEngagement}) и {topFormats[1].name} ({topFormats[1].avgEngagement}) работают лучше всего. Top intent: {topMarketingIntent || '—'} · Top CTA: {topCtaType || '—'} · Top product: {topProduct || '—'}.</>
                : <><span style={{ fontWeight: 600 }}>Strategy:</span> {topFormats[0].name} ({topFormats[0].avgEngagement}) and {topFormats[1].name} ({topFormats[1].avgEngagement}) perform best. Top intent: {topMarketingIntent || '—'} · Top CTA: {topCtaType || '—'} · Top product: {topProduct || '—'}.</>
              )}
            </p>
          </div>
        </>
      )}
    </DashboardCard>
  );
}

export function SocialStatusStrip({
  lang,
  overview,
}: {
  lang: Lang;
  overview: SocialOverviewResponse | null;
}) {
  const ru = lang === 'ru';

  if (!overview) return null;

  const statusLabel = overview.runtime.running_now
    ? (ru ? 'Runtime выполняется' : 'Runtime running')
    : overview.runtime.is_active
      ? (ru ? 'Runtime активен' : 'Runtime active')
      : (ru ? 'Runtime остановлен' : 'Runtime stopped');

  return (
    <div className="grid gap-3 rounded-2xl border border-border bg-card p-4 shadow-sm md:grid-cols-4">
      <div className="rounded-xl bg-muted/40 p-3">
        <div className="flex items-center gap-2 text-xs text-muted-foreground">
          <Activity className="h-3.5 w-3.5" />
          {statusLabel}
        </div>
        <p className="mt-2 text-sm font-semibold text-foreground">
          {overview.runtime.last_success_at
            ? formatSocialRelativeTime(overview.runtime.last_success_at, lang)
            : '—'}
        </p>
      </div>
      <div className="rounded-xl bg-muted/40 p-3">
        <div className="flex items-center gap-2 text-xs text-muted-foreground">
          <Building2 className="h-3.5 w-3.5" />
          {ru ? 'Активные сущности' : 'Active entities'}
        </div>
        <p className="mt-2 text-sm font-semibold text-foreground">{overview.entities_active} / {overview.entities_total}</p>
      </div>
      <div className="rounded-xl bg-muted/40 p-3">
        <div className="flex items-center gap-2 text-xs text-muted-foreground">
          <MessageSquareText className="h-3.5 w-3.5" />
          {ru ? 'Очереди' : 'Queues'}
        </div>
        <p className="mt-2 text-sm font-semibold text-foreground">
          {ru ? 'Анализ' : 'Analysis'} {overview.queue_depth?.analysis ?? 0} · {ru ? 'Граф' : 'Graph'} {overview.queue_depth?.graph ?? 0}
        </p>
      </div>
      <div className="rounded-xl bg-muted/40 p-3">
        <div className="flex items-center gap-2 text-xs text-muted-foreground">
          <BarChart3 className="h-3.5 w-3.5" />
          {ru ? 'Проблемы пайплайна' : 'Pipeline issues'}
        </div>
        <p className="mt-2 text-sm font-semibold text-foreground">
          {overview.dead_letter_failures} {ru ? 'dead-letter' : 'dead-letter'} · {overview.stale_entities.length} {ru ? 'stейл' : 'stale'}
        </p>
      </div>
    </div>
  );
}
