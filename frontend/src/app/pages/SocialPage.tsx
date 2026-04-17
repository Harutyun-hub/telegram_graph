import { type ElementType, useEffect, useMemo, useState } from 'react';
import { useNavigate, useSearchParams } from 'react-router';
import {
  BarChart3,
  Briefcase,
  Brain,
  Eye,
  GitBranch,
  Heart,
  Share2,
  Target,
  Users,
  X,
} from 'lucide-react';
import { Drawer, DrawerContent, DrawerDescription, DrawerHeader, DrawerTitle } from '@/app/components/ui/drawer';
import { EmptyWidget } from '@/app/components/ui/EmptyWidget';
import { useIsMobile } from '@/app/components/ui/use-mobile';
import {
  SocialCommunityBriefCard,
  SocialContentPerformanceCard,
  type SocialEvidenceRequestInput,
  SocialProblemSignalsCard,
  SocialSentimentByTopicCard,
  SocialTopEntitiesCard,
  SocialTopicLandscapeCard,
  SocialTrendingTopicsCard,
  SocialWeekOverWeekCard,
  SocialConversationTrendsCard,
} from '@/app/components/widgets/SocialDashboardWidgets';
import {
  SocialBusinessOpportunityPlaceholder,
  SocialCommunityHealthPlaceholder,
  SocialDecisionStagesPlaceholder,
  SocialEmergingInterestsPlaceholder,
  SocialGrowthFunnelPlaceholder,
  SocialInformationVelocityPlaceholder,
  SocialInterestRadarPlaceholder,
  SocialJobMarketPlaceholder,
  SocialKeyVoicesPlaceholder,
  SocialMoodPlaceholder,
  SocialNewVsReturningPlaceholder,
  SocialPersonaPlaceholder,
  SocialQuestionCloudPlaceholder,
  SocialRecommendationPlaceholder,
  SocialRetentionPlaceholder,
  SocialSatisfactionPlaceholder,
  SocialServiceGapPlaceholder,
  SocialTopicLifecyclePlaceholder,
  SocialUrgencyPlaceholder,
} from '@/app/components/widgets/SocialParityWidgets';
import {
  SocialAccessDeniedState,
  SocialFilterBar,
  SocialInitialLoadingState,
  SocialRefreshingBanner,
  SocialTierHeader,
} from '@/app/components/widgets/SocialShared';
import { useDashboardDateRange } from '@/app/contexts/DashboardDateRangeContext';
import { useLanguage } from '@/app/contexts/LanguageContext';
import { useAuth } from '@/app/contexts/AuthContext';
import {
  socialActivitySummary,
  formatSocialDateLabel,
  socialPayloadList,
  socialPlatformLabel,
} from '@/app/services/socialFormatting';
import {
  getSocialEvidence,
  type SocialEvidenceItem,
  type SocialIntelligenceFilters,
  type SocialPlatform,
} from '@/app/services/socialIntelligence';
import {
  useSocialDashboardData,
  type SocialAdSort,
  type SocialCompetitorSort,
  type SocialSortDir,
} from '@/app/services/socialTwinData';

type EvidenceRequest = SocialEvidenceRequestInput;

type TierConfig = {
  id: string;
  titleEn: string;
  titleRu: string;
  subtitleEn: string;
  subtitleRu: string;
  icon: ElementType;
  colorClass: string;
  bgClass: string;
  borderClass: string;
};

const TIERS: TierConfig[] = [
  {
    id: 'pulse',
    titleEn: 'Community Pulse',
    titleRu: 'Пульс social-поверхности',
    subtitleEn: 'A leadership summary grounded in collected social activity',
    subtitleRu: 'Руководящая сводка по собранной social-активности',
    icon: Eye,
    colorClass: 'text-blue-700',
    bgClass: 'bg-blue-50',
    borderClass: 'border-blue-200',
  },
  {
    id: 'topics',
    titleEn: 'What People Talk About',
    titleRu: 'О чём говорят',
    subtitleEn: 'Themes, momentum, and topic-level conversation structure',
    subtitleRu: 'Темы, импульс и структура разговора по темам',
    icon: Target,
    colorClass: 'text-blue-700',
    bgClass: 'bg-blue-50',
    borderClass: 'border-blue-200',
  },
  {
    id: 'problems',
    titleEn: 'Problems & Satisfaction',
    titleRu: 'Проблемы и удовлетворённость',
    subtitleEn: 'Pain points we can ground in social evidence today',
    subtitleRu: 'Болевые сигналы, которые уже можно подтвердить social-evidence',
    icon: Heart,
    colorClass: 'text-rose-700',
    bgClass: 'bg-rose-50',
    borderClass: 'border-rose-200',
  },
  {
    id: 'channels',
    titleEn: 'Channels, Voices & Activity',
    titleRu: 'Каналы, голоса и активность',
    subtitleEn: 'Which entities and accounts drive the visible volume',
    subtitleRu: 'Какие сущности и аккаунты тянут заметный объём',
    icon: GitBranch,
    colorClass: 'text-indigo-700',
    bgClass: 'bg-indigo-50',
    borderClass: 'border-indigo-200',
  },
  {
    id: 'who',
    titleEn: 'Who Are They',
    titleRu: 'Кто они',
    subtitleEn: 'Held as placeholders until a richer social audience model is ready',
    subtitleRu: 'Пока держим как плейсхолдеры до richer social audience model',
    icon: Users,
    colorClass: 'text-violet-700',
    bgClass: 'bg-violet-50',
    borderClass: 'border-violet-200',
  },
  {
    id: 'growth',
    titleEn: 'Growth, Retention & Journey',
    titleRu: 'Рост, удержание и путь',
    subtitleEn: 'Reserved for later once we materialize a social journey model',
    subtitleRu: 'Зарезервировано до появления отдельной social journey model',
    icon: Brain,
    colorClass: 'text-purple-700',
    bgClass: 'bg-purple-50',
    borderClass: 'border-purple-200',
  },
  {
    id: 'business',
    titleEn: 'Business & Opportunity Intelligence',
    titleRu: 'Бизнес-разведка и возможности',
    subtitleEn: 'Kept visible, but not fabricated before the social read model exists',
    subtitleRu: 'Оставляем видимым, но не выдумываем до готового social read model',
    icon: Briefcase,
    colorClass: 'text-emerald-700',
    bgClass: 'bg-emerald-50',
    borderClass: 'border-emerald-200',
  },
  {
    id: 'analytics',
    titleEn: 'Performance & Analytics',
    titleRu: 'Эффективность и аналитика',
    subtitleEn: 'Window-over-window shifts plus topic and content performance',
    subtitleRu: 'Сдвиги между окнами, тональность по темам и эффективность контента',
    icon: BarChart3,
    colorClass: 'text-slate-700',
    bgClass: 'bg-slate-50',
    borderClass: 'border-slate-200',
  },
];

export function SocialPage() {
  const navigate = useNavigate();
  const { lang } = useLanguage();
  const { isAuthenticated } = useAuth();
  const { range } = useDashboardDateRange();
  const [searchParams, setSearchParams] = useSearchParams();
  const isMobile = useIsMobile();
  const ru = lang === 'ru';

  const entityParam = searchParams.get('entity') || 'all';
  const platformParam = (searchParams.get('platform') || 'all') as SocialPlatform;

  const filters: SocialIntelligenceFilters = useMemo(() => ({
    from: range.from,
    to: range.to,
    entityId: entityParam !== 'all' ? entityParam : undefined,
    platform: platformParam,
  }), [entityParam, platformParam, range.from, range.to]);

  const [adSort] = useState<SocialAdSort>('engagement');
  const [scorecardSort] = useState<SocialCompetitorSort>('posts');
  const [scorecardSortDir] = useState<SocialSortDir>('desc');
  const [evidenceRequest, setEvidenceRequest] = useState<EvidenceRequest | null>(null);
  const [evidenceItems, setEvidenceItems] = useState<SocialEvidenceItem[]>([]);
  const [evidenceCount, setEvidenceCount] = useState(0);
  const [evidenceLoading, setEvidenceLoading] = useState(false);
  const [evidenceError, setEvidenceError] = useState<string | null>(null);
  const [openTiers, setOpenTiers] = useState<Record<string, boolean>>({
    pulse: true,
    topics: true,
    problems: true,
    channels: true,
    who: true,
    growth: true,
    business: true,
    analytics: true,
  });

  const {
    overview,
    entities,
    summary,
    previousSummary,
    topics,
    topicTrendSeries,
    ads,
    adsSummary,
    audienceResponse,
    competitors,
    loading,
    refreshing,
    error,
    accessDenied,
    refresh,
  } = useSocialDashboardData(filters, {
    adSort,
    scorecardSort,
    scorecardSortDir,
  });

  const rangeLabel = `${range.from} — ${range.to}`;
  const lastSuccessLabel = overview?.runtime?.last_success_at
    ? formatSocialDateLabel(overview.runtime.last_success_at, lang)
    : null;
  const visibleWidgetCount = 28;
  const headerSummary = summary
    ? (ru
      ? `${visibleWidgetCount} виджетов · ${summary.postsCollected.toLocaleString()} активностей · ${lastSuccessLabel ? `обновлено ${lastSuccessLabel}` : 'social-окно активно'}`
      : `${visibleWidgetCount} widgets · ${summary.postsCollected.toLocaleString()} activities · ${lastSuccessLabel ? `updated ${lastSuccessLabel}` : 'social window active'}`)
    : (ru ? 'Отдельная social data-поверхность' : 'Separate social data surface');
  const filterStatusSummary = overview
    ? (ru
      ? `Runtime ${overview.runtime.running_now ? 'выполняется' : overview.runtime.is_active ? 'активен' : 'остановлен'} · последний успех ${overview.runtime.last_success_at ? formatSocialDateLabel(overview.runtime.last_success_at, lang) : '—'} · очередь анализа ${overview.queue_depth?.analysis ?? 0}`
      : `Runtime ${overview.runtime.running_now ? 'running now' : overview.runtime.is_active ? 'active' : 'stopped'} · last success ${overview.runtime.last_success_at ? formatSocialDateLabel(overview.runtime.last_success_at, lang) : '—'} · analysis queue ${overview.queue_depth?.analysis ?? 0}`)
    : undefined;

  const openEvidence = (input: EvidenceRequest) => {
    setEvidenceRequest(input);
  };

  const openTopic = (topic: string) => {
    if (!topic) return;
    const next = new URLSearchParams();
    if (entityParam !== 'all') next.set('entity', entityParam);
    if (platformParam !== 'all') next.set('platform', platformParam);
    next.set('topic', topic);
    navigate({
      pathname: '/social/topics',
      search: next.toString(),
    });
  };

  const setEntity = (value: string) => {
    const next = new URLSearchParams(searchParams);
    if (value === 'all') next.delete('entity');
    else next.set('entity', value);
    setSearchParams(next);
  };

  const setPlatform = (value: SocialPlatform) => {
    const next = new URLSearchParams(searchParams);
    if (value === 'all') next.delete('platform');
    else next.set('platform', value);
    setSearchParams(next);
  };

  useEffect(() => {
    if (!evidenceRequest) {
      setEvidenceItems([]);
      setEvidenceCount(0);
      setEvidenceError(null);
      return;
    }

    let cancelled = false;
    setEvidenceLoading(true);
    setEvidenceError(null);

    getSocialEvidence(filters, evidenceRequest.filters)
      .then((response) => {
        if (cancelled) return;
        setEvidenceItems(response.items);
        setEvidenceCount(response.count);
      })
      .catch((loadError) => {
        if (cancelled) return;
        setEvidenceError(loadError instanceof Error ? loadError.message : (ru ? 'Не удалось загрузить evidence.' : 'Failed to load evidence.'));
      })
      .finally(() => {
        if (!cancelled) setEvidenceLoading(false);
      });

    return () => {
      cancelled = true;
    };
  }, [evidenceRequest, filters, ru]);

  if (!isAuthenticated) {
    return (
      <SocialAccessDeniedState
        title={ru ? 'Для Social нужен вход оператора' : 'Social requires an operator sign-in'}
        description={ru
          ? 'Войдите под операторской учётной записью, чтобы открыть social dashboard.'
          : 'Sign in with the operator credentials to open the social dashboard.'}
      />
    );
  }

  if (accessDenied) {
    return (
      <SocialAccessDeniedState
        title={ru ? 'Social dashboard доступен только оператору' : 'The social dashboard is operator-only'}
        description={ru
          ? 'Эта поверхность использует operator-only social endpoints. Войдите под операторской учётной записью, чтобы открыть social dashboard и social topics.'
          : 'This surface is backed by operator-only social endpoints. Sign in with an operator session to open the social dashboard and social topics.'}
      />
    );
  }

  if (loading && !summary && !overview && topics.length === 0) {
    return <SocialInitialLoadingState ru={ru} />;
  }

  return (
    <div className="p-4 md:p-6 space-y-4 md:space-y-6 max-w-[1600px] mx-auto">
      <div className="flex items-start justify-between gap-3">
        <div>
          <h1 className="text-gray-900" style={{ fontSize: '1.25rem', fontWeight: 600 }}>
            {ru ? 'Social Media Intelligence' : 'Social Media Intelligence'}
          </h1>
          <p className="text-xs text-gray-500 mt-0.5">
            {headerSummary}
          </p>
        </div>
        <div className="flex items-center gap-2 flex-shrink-0">
          <button
            type="button"
            onClick={() => setOpenTiers(Object.fromEntries(TIERS.map((tier) => [tier.id, true])))}
            className="text-xs px-2.5 py-1.5 bg-gray-100 text-gray-600 rounded-lg hover:bg-gray-200 transition-colors whitespace-nowrap"
          >
            {ru ? 'Раскрыть' : 'Expand'}
          </button>
          <button
            type="button"
            onClick={() => setOpenTiers(Object.fromEntries(TIERS.map((tier) => [tier.id, false])))}
            className="text-xs px-2.5 py-1.5 bg-gray-100 text-gray-600 rounded-lg hover:bg-gray-200 transition-colors whitespace-nowrap"
          >
            {ru ? 'Свернуть' : 'Collapse'}
          </button>
        </div>
      </div>

      <SocialFilterBar
        entityValue={entityParam}
        entities={entities}
        platformValue={platformParam}
        rangeLabel={rangeLabel}
        statusSummary={filterStatusSummary}
        onEntityChange={setEntity}
        onPlatformChange={setPlatform}
        ru={ru}
      />

      {refreshing && (summary || overview || topics.length > 0) ? (
        <SocialRefreshingBanner ru={ru} />
      ) : null}

      {error ? (
        <div className="rounded-xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-900">
          <div className="flex items-start justify-between gap-4">
            <div>
              <p className="font-semibold">{ru ? 'Некоторые social-карточки обновились не полностью' : 'Some social cards did not refresh completely'}</p>
              <p className="mt-1 text-xs">{error}</p>
            </div>
            <button
              type="button"
              onClick={refresh}
              className="inline-flex items-center gap-1 text-xs font-semibold text-amber-900 underline"
            >
              {ru ? 'Повторить' : 'Retry'}
            </button>
          </div>
        </div>
      ) : null}

      <SocialTierHeader
        icon={TIERS[0].icon}
        title={ru ? TIERS[0].titleRu : TIERS[0].titleEn}
        subtitle={ru ? TIERS[0].subtitleRu : TIERS[0].subtitleEn}
        colorClass={TIERS[0].colorClass}
        bgClass={TIERS[0].bgClass}
        borderClass={TIERS[0].borderClass}
        isOpen={openTiers.pulse}
        onToggle={() => setOpenTiers((current) => ({ ...current, pulse: !current.pulse }))}
      />
      {openTiers.pulse ? (
        <div className="space-y-4 md:space-y-6">
          <SocialCommunityBriefCard
            lang={lang}
            overview={overview}
            summary={summary}
            rangeDays={range.days}
            onOpenEvidence={openEvidence}
            onOpenOps={() => navigate('/social/ops')}
            onOpenTopic={openTopic}
          />
          <div className="grid grid-cols-1 gap-4 md:grid-cols-2 md:gap-6">
            <SocialCommunityHealthPlaceholder lang={lang} />
            <SocialTrendingTopicsCard lang={lang} topics={topics} onOpenTopic={openTopic} />
          </div>
        </div>
      ) : null}

      <SocialTierHeader
        icon={TIERS[1].icon}
        title={ru ? TIERS[1].titleRu : TIERS[1].titleEn}
        subtitle={ru ? TIERS[1].subtitleRu : TIERS[1].subtitleEn}
        colorClass={TIERS[1].colorClass}
        bgClass={TIERS[1].bgClass}
        borderClass={TIERS[1].borderClass}
        isOpen={openTiers.topics}
        onToggle={() => setOpenTiers((current) => ({ ...current, topics: !current.topics }))}
      />
      {openTiers.topics ? (
        <div className="space-y-4 md:space-y-6">
          <SocialTopicLandscapeCard lang={lang} topics={topics} onOpenTopic={openTopic} />
          <div className="grid grid-cols-1 gap-4 md:grid-cols-2 md:gap-6">
            <SocialConversationTrendsCard lang={lang} series={topicTrendSeries} onOpenTopic={openTopic} />
            <SocialQuestionCloudPlaceholder lang={lang} />
          </div>
          <SocialTopicLifecyclePlaceholder lang={lang} rangeDays={range.days} />
        </div>
      ) : null}

      <SocialTierHeader
        icon={TIERS[2].icon}
        title={ru ? TIERS[2].titleRu : TIERS[2].titleEn}
        subtitle={ru ? TIERS[2].subtitleRu : TIERS[2].subtitleEn}
        colorClass={TIERS[2].colorClass}
        bgClass={TIERS[2].bgClass}
        borderClass={TIERS[2].borderClass}
        isOpen={openTiers.problems}
        onToggle={() => setOpenTiers((current) => ({ ...current, problems: !current.problems }))}
      />
      {openTiers.problems ? (
        <div className="space-y-4 md:space-y-6">
          <div className="grid grid-cols-1 gap-4 md:grid-cols-2 md:gap-6">
            <SocialProblemSignalsCard
              lang={lang}
              painPoints={audienceResponse?.painPoints || []}
              customerIntent={audienceResponse?.customerIntent || []}
              onOpenPainPoint={(label) => openEvidence({
                title: label,
                description: ru ? 'Evidence для pain-point сигнала.' : 'Evidence filtered to this pain-point signal.',
                filters: { painPoint: label },
              })}
              onOpenIntent={(label) => openEvidence({
                title: label,
                description: ru ? 'Evidence для customer-intent сигнала.' : 'Evidence filtered to this customer intent.',
                filters: { customerIntent: label },
              })}
            />
            <SocialServiceGapPlaceholder lang={lang} />
          </div>
          <div className="grid grid-cols-1 gap-4 md:grid-cols-2 md:gap-6">
            <SocialSatisfactionPlaceholder lang={lang} />
            <SocialMoodPlaceholder lang={lang} />
          </div>
          <SocialUrgencyPlaceholder lang={lang} />
        </div>
      ) : null}

      <SocialTierHeader
        icon={TIERS[3].icon}
        title={ru ? TIERS[3].titleRu : TIERS[3].titleEn}
        subtitle={ru ? TIERS[3].subtitleRu : TIERS[3].subtitleEn}
        colorClass={TIERS[3].colorClass}
        bgClass={TIERS[3].bgClass}
        borderClass={TIERS[3].borderClass}
        isOpen={openTiers.channels}
        onToggle={() => setOpenTiers((current) => ({ ...current, channels: !current.channels }))}
      />
      {openTiers.channels ? (
        <div className="space-y-4 md:space-y-6">
          <div className="grid grid-cols-1 gap-4 md:grid-cols-2 md:gap-6">
            <SocialTopEntitiesCard
              lang={lang}
              competitors={competitors}
              onOpenEntityEvidence={(entityId, entityName) => openEvidence({
                title: entityName,
                description: ru ? 'Evidence по активности этой сущности.' : 'Evidence filtered to this entity.',
                filters: { entityId },
              })}
              onOpenTopic={openTopic}
            />
            <SocialKeyVoicesPlaceholder lang={lang} rangeDays={range.days} />
          </div>
          <SocialRecommendationPlaceholder lang={lang} />
          <SocialInformationVelocityPlaceholder lang={lang} />
        </div>
      ) : null}

      <SocialTierHeader
        icon={TIERS[4].icon}
        title={ru ? TIERS[4].titleRu : TIERS[4].titleEn}
        subtitle={ru ? TIERS[4].subtitleRu : TIERS[4].subtitleEn}
        colorClass={TIERS[4].colorClass}
        bgClass={TIERS[4].bgClass}
        borderClass={TIERS[4].borderClass}
        isOpen={openTiers.who}
        onToggle={() => setOpenTiers((current) => ({ ...current, who: !current.who }))}
      />
      {openTiers.who ? (
        <div className="grid grid-cols-1 gap-4 md:grid-cols-2 md:gap-6">
          <SocialPersonaPlaceholder lang={lang} />
          <SocialInterestRadarPlaceholder lang={lang} rangeDays={range.days} />
        </div>
      ) : null}

      <SocialTierHeader
        icon={TIERS[5].icon}
        title={ru ? TIERS[5].titleRu : TIERS[5].titleEn}
        subtitle={ru ? TIERS[5].subtitleRu : TIERS[5].subtitleEn}
        colorClass={TIERS[5].colorClass}
        bgClass={TIERS[5].bgClass}
        borderClass={TIERS[5].borderClass}
        isOpen={openTiers.growth}
        onToggle={() => setOpenTiers((current) => ({ ...current, growth: !current.growth }))}
      />
      {openTiers.growth ? (
        <div className="space-y-4 md:space-y-6">
          <div className="grid grid-cols-1 gap-4 md:grid-cols-2 md:gap-6">
            <SocialGrowthFunnelPlaceholder lang={lang} />
            <SocialRetentionPlaceholder lang={lang} />
          </div>
          <div className="grid grid-cols-1 gap-4 md:grid-cols-2 md:gap-6">
            <SocialDecisionStagesPlaceholder lang={lang} />
            <SocialEmergingInterestsPlaceholder lang={lang} />
          </div>
          <SocialNewVsReturningPlaceholder lang={lang} />
        </div>
      ) : null}

      <SocialTierHeader
        icon={TIERS[6].icon}
        title={ru ? TIERS[6].titleRu : TIERS[6].titleEn}
        subtitle={ru ? TIERS[6].subtitleRu : TIERS[6].subtitleEn}
        colorClass={TIERS[6].colorClass}
        bgClass={TIERS[6].bgClass}
        borderClass={TIERS[6].borderClass}
        isOpen={openTiers.business}
        onToggle={() => setOpenTiers((current) => ({ ...current, business: !current.business }))}
      />
      {openTiers.business ? (
        <div className="space-y-4 md:space-y-6">
          <SocialBusinessOpportunityPlaceholder lang={lang} />
          <SocialJobMarketPlaceholder lang={lang} />
        </div>
      ) : null}

      <SocialTierHeader
        icon={TIERS[7].icon}
        title={ru ? TIERS[7].titleRu : TIERS[7].titleEn}
        subtitle={ru ? TIERS[7].subtitleRu : TIERS[7].subtitleEn}
        colorClass={TIERS[7].colorClass}
        bgClass={TIERS[7].bgClass}
        borderClass={TIERS[7].borderClass}
        isOpen={openTiers.analytics}
        onToggle={() => setOpenTiers((current) => ({ ...current, analytics: !current.analytics }))}
      />
      {openTiers.analytics ? (
        <div className="space-y-4 md:space-y-6">
          <SocialWeekOverWeekCard lang={lang} summary={summary} previousSummary={previousSummary} topics={topics} />
          <div className="grid grid-cols-1 gap-4 md:grid-cols-2 md:gap-6">
            <SocialSentimentByTopicCard lang={lang} topics={topics} onOpenTopic={openTopic} />
            <SocialContentPerformanceCard
              lang={lang}
              ads={ads}
              topMarketingIntent={adsSummary?.topMarketingIntent || null}
              topCtaType={adsSummary?.topCtaType || null}
              topProduct={adsSummary?.topProduct || null}
              onOpenEvidence={openEvidence}
            />
          </div>
        </div>
      ) : null}

      <div className="h-4" />
      <Drawer
        open={Boolean(evidenceRequest)}
        onOpenChange={(open) => {
          if (!open) setEvidenceRequest(null);
        }}
        direction={isMobile ? 'bottom' : 'right'}
      >
        <DrawerContent className="data-[vaul-drawer-direction=right]:w-full data-[vaul-drawer-direction=right]:sm:max-w-[560px]">
          <DrawerHeader className="border-b border-border/70">
            <div className="flex items-center justify-between gap-3">
              <DrawerTitle>{evidenceRequest?.title || (ru ? 'Evidence' : 'Evidence')}</DrawerTitle>
              <button
                type="button"
                onClick={() => setEvidenceRequest(null)}
                className="rounded-lg p-1.5 transition hover:bg-accent hover:text-accent-foreground"
              >
                <X className="h-4 w-4 text-muted-foreground" />
              </button>
            </div>
            <DrawerDescription>{evidenceRequest?.description || ''}</DrawerDescription>
            <p className="mt-1 text-xs text-muted-foreground">
              {entityParam === 'all'
                ? (ru ? 'Все конкуренты' : 'All competitors')
                : (entities.find((entity) => entity.id === entityParam)?.name || entityParam)}
              {' · '}
              {platformParam === 'all'
                ? (ru ? 'Все платформы' : 'All platforms')
                : socialPlatformLabel(platformParam, lang)}
              {' · '}
              {rangeLabel}
            </p>
          </DrawerHeader>
          <div className="flex-1 overflow-y-auto p-4">
            {evidenceLoading ? (
              <div className="space-y-3">
                {Array.from({ length: 3 }).map((_, index) => (
                  <div key={index} className="h-28 animate-pulse rounded-2xl bg-muted" />
                ))}
              </div>
            ) : evidenceError ? (
              <div className="rounded-xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-900">
                {evidenceError}
              </div>
            ) : evidenceItems.length === 0 ? (
              <EmptyWidget
                title={ru ? 'Evidence' : 'Evidence'}
                message={ru ? 'Нет evidence для этого social-среза.' : 'There is no evidence for this social slice yet.'}
                compact
              />
            ) : (
              <div className="space-y-3">
                <p className="text-xs text-muted-foreground">
                  {evidenceCount} {ru ? 'совпадений' : 'matching items'}
                </p>
                {evidenceItems.map((item) => {
                  const payload = item.analysis?.analysis_payload ?? {};
                  return (
                    <div key={item.id} className="rounded-2xl border border-border bg-card p-4 shadow-sm">
                      <div className="flex flex-wrap items-center gap-2 text-[11px] text-muted-foreground">
                        <span className="rounded-full border border-border bg-muted px-2.5 py-1">{item.entity?.name || 'Unknown'}</span>
                        <span className="rounded-full border border-border bg-muted px-2.5 py-1">{socialPlatformLabel(item.platform, lang)}</span>
                        <span>{formatSocialDateLabel(item.published_at, lang)}</span>
                      </div>
                      <p className="mt-3 text-sm leading-relaxed text-foreground">{socialActivitySummary(item)}</p>
                      {item.analysis?.summary ? (
                        <div className="mt-3 rounded-xl border border-sky-100 bg-sky-50 px-3 py-2.5">
                          <p className="text-[10px] font-semibold uppercase tracking-[0.16em] text-sky-700">
                            {ru ? 'AI summary' : 'AI summary'}
                          </p>
                          <p className="mt-1 text-sm text-foreground">{item.analysis.summary}</p>
                        </div>
                      ) : null}
                      <div className="mt-3 flex flex-wrap gap-1.5">
                        {socialPayloadList(payload, 'topics').slice(0, 3).map((topic) => (
                          <span key={topic} className="rounded-full border border-border bg-muted px-2.5 py-1 text-[11px] text-muted-foreground">
                            {topic}
                          </span>
                        ))}
                        {socialPayloadList(payload, 'pain_points').slice(0, 2).map((itemLabel) => (
                          <span key={itemLabel} className="rounded-full bg-rose-50 px-2.5 py-1 text-[11px] text-rose-700">
                            {itemLabel}
                          </span>
                        ))}
                        {socialPayloadList(payload, 'value_propositions').slice(0, 2).map((itemLabel) => (
                          <span key={itemLabel} className="rounded-full bg-emerald-50 px-2.5 py-1 text-[11px] text-emerald-700">
                            {itemLabel}
                          </span>
                        ))}
                      </div>
                      {item.source_url ? (
                        <a
                          className="mt-3 inline-flex items-center gap-1 text-xs font-medium text-primary transition hover:text-primary/80"
                          href={item.source_url}
                          rel="noreferrer"
                          target="_blank"
                        >
                          <Share2 className="h-3.5 w-3.5" />
                          {ru ? 'Открыть источник' : 'Open source'}
                        </a>
                      ) : null}
                    </div>
                  );
                })}
              </div>
            )}
          </div>
        </DrawerContent>
      </Drawer>
    </div>
  );
}
