import type { ElementType } from 'react';
import { ChevronDown, ChevronUp, LockKeyhole, ShieldAlert } from 'lucide-react';
import type { AdminWidgetId } from '@/app/admin/catalog';
import { Card, CardContent } from '@/app/components/ui/card';
import { EmptyWidget } from '@/app/components/ui/EmptyWidget';
import { cn } from '@/app/components/ui/utils';
import type { SocialEntityOption, SocialPlatform, SocialTopicItem } from '@/app/services/socialIntelligence';
import { sentimentFill, socialEntityColor } from '@/app/services/socialFormatting';

interface SocialTierHeaderProps {
  icon: ElementType;
  title: string;
  subtitle: string;
  colorClass: string;
  bgClass: string;
  borderClass: string;
  isOpen: boolean;
  onToggle: () => void;
}

interface SocialFilterBarProps {
  entityValue: string;
  entities: SocialEntityOption[];
  platformValue: SocialPlatform;
  rangeLabel: string;
  statusSummary?: string;
  onEntityChange: (value: string) => void;
  onPlatformChange: (value: SocialPlatform) => void;
  ru: boolean;
}

interface SocialAccessDeniedStateProps {
  title: string;
  description: string;
}

interface SocialPlaceholderCardProps {
  title: string;
  message: string;
  widgetId?: AdminWidgetId;
}

const PLATFORM_OPTIONS: Array<{ value: SocialPlatform; label: string }> = [
  { value: 'all', label: 'All' },
  { value: 'facebook', label: 'Facebook' },
  { value: 'instagram', label: 'Instagram' },
  { value: 'google', label: 'Google' },
  { value: 'tiktok', label: 'TikTok' },
];

export function SocialTierHeader({
  icon: Icon,
  title,
  subtitle,
  colorClass,
  bgClass,
  borderClass,
  isOpen,
  onToggle,
}: SocialTierHeaderProps) {
  return (
    <button
      type="button"
      onClick={onToggle}
      className={cn('w-full flex items-center justify-between px-4 py-3 rounded-xl border transition-colors hover:shadow-sm', bgClass, borderClass)}
    >
      <div className="flex items-center gap-3">
        <div className={cn('w-8 h-8 rounded-lg flex items-center justify-center', bgClass)}>
          <Icon className={cn('w-4 h-4', colorClass)} />
        </div>
        <div className="text-left">
          <h2 className={cn('text-sm', colorClass)} style={{ fontWeight: 600 }}>{title}</h2>
          <p className="text-xs text-gray-500">{subtitle}</p>
        </div>
      </div>
      {isOpen ? <ChevronUp className="w-4 h-4 text-gray-400" /> : <ChevronDown className="w-4 h-4 text-gray-400" />}
    </button>
  );
}

export function SocialFilterBar({
  entityValue,
  entities,
  platformValue,
  rangeLabel,
  statusSummary,
  onEntityChange,
  onPlatformChange,
  ru,
}: SocialFilterBarProps) {
  return (
    <div className="rounded-xl border border-gray-200 bg-white p-4 shadow-sm">
      <div className="flex flex-wrap items-center gap-3">
        <div className="flex flex-wrap items-center gap-1.5">
          {PLATFORM_OPTIONS.map((option) => (
            <button
              key={option.value}
              type="button"
              onClick={() => onPlatformChange(option.value)}
              className={cn(
                'rounded-full px-3 py-1 text-xs transition-colors',
                platformValue === option.value
                  ? 'bg-slate-800 text-white'
                  : 'bg-gray-100 text-gray-600 hover:bg-gray-200',
              )}
              style={{ fontWeight: 500 }}
            >
              {option.value === 'all' ? (ru ? 'Все' : option.label) : option.label}
            </button>
          ))}
        </div>
        <div className="hidden h-4 w-px bg-gray-200 md:block" />
        <select
          value={entityValue}
          onChange={(event) => onEntityChange(event.target.value)}
          className="rounded-lg border border-gray-200 bg-white px-3 py-1.5 text-xs text-gray-900 outline-none transition focus:border-blue-500 focus:ring-2 focus:ring-blue-500/10"
        >
          <option value="all">{ru ? 'Все конкуренты' : 'All competitors'}</option>
          {entities.map((entity) => (
            <option key={entity.id} value={entity.id}>
              {entity.name}
            </option>
          ))}
        </select>
        <div className="ml-auto text-xs text-gray-500">{rangeLabel}</div>
      </div>
      {statusSummary ? (
        <p className="mt-3 text-xs text-gray-500">
          {statusSummary}
        </p>
      ) : null}
    </div>
  );
}

export function SocialInitialLoadingState({ ru }: { ru: boolean }) {
  return (
    <div className="min-h-[60vh] flex flex-col items-center justify-center px-6 text-center">
      <div className="w-9 h-9 rounded-full border-2 border-blue-200 border-t-blue-600 animate-spin mb-3" />
      <p className="text-sm text-gray-700" style={{ fontWeight: 500 }}>
        {ru ? 'Загружаем данные панели…' : 'Loading dashboard data...'}
      </p>
      <p className="text-xs text-gray-500 mt-1">
        {ru ? 'Показываем только реальные данные без мок-значений' : 'Showing only real backend data, no mock placeholders'}
      </p>
    </div>
  );
}

export function SocialRefreshingBanner({ ru }: { ru: boolean }) {
  return (
    <div className="rounded-xl border border-slate-200 bg-white px-4 py-3 shadow-sm">
      <div className="flex items-center gap-3">
        <div className="w-4 h-4 rounded-full border-2 border-blue-200 border-t-blue-600 animate-spin flex-shrink-0" />
        <div className="min-w-0">
          <p className="text-sm text-slate-900" style={{ fontWeight: 600 }}>
            {ru ? 'Обновляем данные панели…' : 'Refreshing dashboard data...'}
          </p>
          <p className="text-xs text-slate-500 mt-0.5">
            {ru ? 'Предыдущий снимок остаётся на экране, пока новый диапазон загружается.' : 'The previous snapshot stays visible while the new range is loading.'}
          </p>
        </div>
      </div>
    </div>
  );
}

export function SocialAccessDeniedState({ title, description }: SocialAccessDeniedStateProps) {
  return (
    <div className="mx-auto flex min-h-[50vh] max-w-2xl items-center justify-center px-4 py-10">
      <Card className="w-full border-border/70 shadow-sm">
        <CardContent className="flex flex-col items-center gap-4 py-10 text-center">
          <div className="flex h-14 w-14 items-center justify-center rounded-2xl bg-amber-50 text-amber-700">
            <ShieldAlert className="h-7 w-7" />
          </div>
          <div className="space-y-2">
            <h1 className="text-xl font-semibold text-foreground">{title}</h1>
            <p className="mx-auto max-w-xl text-sm text-muted-foreground">{description}</p>
          </div>
          <div className="inline-flex items-center gap-2 rounded-full border border-border bg-muted px-3 py-1 text-xs text-muted-foreground">
            <LockKeyhole className="h-3.5 w-3.5" />
            <span>Operator access required</span>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}

export function SocialPlaceholderCard({ title, message, widgetId }: SocialPlaceholderCardProps) {
  return (
    <EmptyWidget
      widgetId={widgetId}
      title={title}
      message={message}
    />
  );
}

export function SocialTopicBubbleMap({
  topics,
  onSelect,
}: {
  topics: SocialTopicItem[];
  onSelect: (topic: SocialTopicItem) => void;
}) {
  if (topics.length === 0) return null;

  const width = 640;
  const height = 320;
  const maxCount = Math.max(...topics.map((topic) => topic.count), 1);
  const minRadius = 28;
  const maxRadius = 72;
  const bubbles = topics.slice(0, 14).map((topic, index) => {
    const radius = minRadius + ((topic.count / maxCount) * (maxRadius - minRadius));
    const angle = (index / Math.min(topics.length, 14)) * 2 * Math.PI - (Math.PI / 2);
    const ring = index < 4 ? 0.28 : index < 9 ? 0.44 : 0.6;
    return {
      topic,
      radius,
      cx: width / 2 + Math.cos(angle) * width * ring,
      cy: height / 2 + Math.sin(angle) * height * ring * 0.85,
    };
  });

  return (
    <svg viewBox={`0 0 ${width} ${height}`} className="w-full" style={{ maxHeight: 320 }}>
      {bubbles.map(({ topic, radius, cx, cy }, index) => {
        const fill = sentimentFill(topic.avgSentimentScore);
        const label = topic.topic.length > 14 ? `${topic.topic.slice(0, 13)}…` : topic.topic;
        return (
          <g
            key={topic.topic}
            onClick={() => onSelect(topic)}
            style={{ cursor: 'pointer' }}
          >
            <circle cx={cx} cy={cy} r={radius} fill={`${fill}18`} stroke={fill} strokeWidth={1.5} />
            <circle cx={cx} cy={cy} r={radius} fill={`${socialEntityColor(index)}10`} />
            <text
              x={cx}
              y={cy - 4}
              textAnchor="middle"
              dominantBaseline="middle"
              fontSize={radius > 48 ? 12 : 10}
              fill="#0f172a"
              fontWeight={600}
              style={{ pointerEvents: 'none' }}
            >
              {label}
            </text>
            <text
              x={cx}
              y={cy + 12}
              textAnchor="middle"
              dominantBaseline="middle"
              fontSize={9}
              fill="#64748b"
              style={{ pointerEvents: 'none' }}
            >
              {topic.count}
            </text>
          </g>
        );
      })}
    </svg>
  );
}
