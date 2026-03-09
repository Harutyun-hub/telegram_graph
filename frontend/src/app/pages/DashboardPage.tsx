import { useState } from 'react';
import { Eye, Brain, Target, Users, BarChart3, GitBranch, Heart, ChevronDown, ChevronUp, Briefcase } from 'lucide-react';
import { useLanguage } from '../contexts/LanguageContext';
import { useData } from '../contexts/DataContext';

// Tier 1: Community Pulse
import { CommunityHealthScore, TrendingTopicsFeed, CommunityBrief } from '../components/widgets/ExecutiveGlance';

// Tier 2: What People Talk About
import { TopicLandscape, ConversationTrends, ContentEngagementHeatmap, QuestionCloud, QuestionAnswerGap, TopicLifecycle } from '../components/widgets/StrategicWidgets';

// Tier 3: Problems & Satisfaction
import { ProblemTracker, ServiceGapDetector, SatisfactionByArea, MoodOverTime, EmotionalUrgencyIndex } from '../components/widgets/BehavioralWidgets';

// Tier 4: Channels, Voices & Activity
import { TopChannels, KeyVoices, RecommendationTracker, InformationVelocity } from '../components/widgets/NetworkWidgets';

// Tier 5: Who Are They (Demographics & Psychographics)
import { PersonaGallery, InterestRadar } from '../components/widgets/PsychographicWidgets';

// Tier 6: Growth, Retention & Journey
import { EmergingInterests, RetentionRiskGauge, CommunityGrowthFunnel, DecisionStageTracker, NewVsReturningVoice } from '../components/widgets/PredictiveWidgets';

// Tier 7: Business & Opportunity Intelligence
import { BusinessOpportunityTracker, JobMarketPulse } from '../components/widgets/ActionableWidgets';

// Tier 8: Performance & Analytics
import { WeekOverWeekShifts, SentimentByTopic, ContentPerformance } from '../components/widgets/ComparativeWidgets';

interface TierConfig {
  id: string;
  title: string;
  subtitle: string;
  icon: React.ElementType;
  color: string;
  bgColor: string;
  borderColor: string;
}

function TierHeader({ tier, isOpen, onToggle }: { tier: TierConfig; isOpen: boolean; onToggle: () => void }) {
  const Icon = tier.icon;
  return (
    <button
      onClick={onToggle}
      className={`w-full flex items-center justify-between px-4 py-3 rounded-xl border transition-colors ${tier.bgColor} ${tier.borderColor} hover:shadow-sm`}
    >
      <div className="flex items-center gap-3">
        <div className={`w-8 h-8 rounded-lg flex items-center justify-center ${tier.bgColor}`}>
          <Icon className={`w-4 h-4 ${tier.color}`} />
        </div>
        <div className="text-left">
          <h2 className={`text-sm ${tier.color}`} style={{ fontWeight: 600 }}>{tier.title}</h2>
          <p className="text-xs text-gray-500">{tier.subtitle}</p>
        </div>
      </div>
      {isOpen
        ? <ChevronUp className="w-4 h-4 text-gray-400" />
        : <ChevronDown className="w-4 h-4 text-gray-400" />
      }
    </button>
  );
}

export function DashboardPage() {
  const { lang } = useLanguage();
  const { data } = useData();
  const ru = lang === 'ru';

  const tiers: TierConfig[] = [
    {
      id: 'pulse',
      title: ru ? 'Пульс сообщества' : 'Community Pulse',
      subtitle: ru ? 'Сводка для руководства за 30 секунд' : '30-second overview for leadership',
      icon: Eye, color: 'text-blue-700', bgColor: 'bg-blue-50', borderColor: 'border-blue-200',
    },
    {
      id: 'topics',
      title: ru ? 'О чём говорят' : 'What People Talk About',
      subtitle: ru ? 'Темы, тренды и вопросы' : 'Topics, trends & questions',
      icon: Target, color: 'text-blue-700', bgColor: 'bg-blue-50', borderColor: 'border-blue-200',
    },
    {
      id: 'problems',
      title: ru ? 'Проблемы и удовлетворённость' : 'Problems & Satisfaction',
      subtitle: ru ? 'Болевые точки и оценка жизни' : 'Pain points & how they feel about life',
      icon: Heart, color: 'text-rose-700', bgColor: 'bg-rose-50', borderColor: 'border-rose-200',
    },
    {
      id: 'channels',
      title: ru ? 'Каналы, голоса и активность' : 'Channels, Voices & Activity',
      subtitle: ru ? 'Где собирается сообщество и кто им управляет' : 'Where people gather & who leads',
      icon: GitBranch, color: 'text-indigo-700', bgColor: 'bg-indigo-50', borderColor: 'border-indigo-200',
    },
    {
      id: 'who',
      title: ru ? 'Кто они' : 'Who Are They',
      subtitle: ru ? 'Персоны, происхождение и интеграция' : 'Personas, origins & integration',
      icon: Users, color: 'text-violet-700', bgColor: 'bg-violet-50', borderColor: 'border-violet-200',
    },
    {
      id: 'growth',
      title: ru ? 'Рост, удержание и путь' : 'Growth, Retention & Journey',
      subtitle: ru ? 'Воронка вовлечённости и сигналы оттока' : 'Engagement funnel & churn signals',
      icon: Brain, color: 'text-purple-700', bgColor: 'bg-purple-50', borderColor: 'border-purple-200',
    },
    {
      id: 'business',
      title: ru ? 'Бизнес-разведка и возможности' : 'Business & Opportunity Intelligence',
      subtitle: ru ? 'Неудовлетворённые потребности, занятость и жильё' : 'Unmet needs, jobs & housing',
      icon: Briefcase, color: 'text-emerald-700', bgColor: 'bg-emerald-50', borderColor: 'border-emerald-200',
    },
    {
      id: 'analytics',
      title: ru ? 'Эффективность и аналитика' : 'Performance & Analytics',
      subtitle: ru ? 'Контент-стратегия и еженедельные изменения' : 'Content strategy & weekly deltas',
      icon: BarChart3, color: 'text-slate-700', bgColor: 'bg-slate-50', borderColor: 'border-slate-200',
    },
  ];

  const [openTiers, setOpenTiers] = useState<Record<string, boolean>>({
    pulse: true, topics: true, problems: true, channels: true,
    who: true, growth: true, business: true, analytics: true,
  });

  const toggleTier = (tierId: string) => {
    setOpenTiers((prev) => ({ ...prev, [tierId]: !prev[tierId] }));
  };

  return (
    <div className="p-4 md:p-6 space-y-4 md:space-y-6 max-w-[1600px] mx-auto">
      {/* Dashboard Header */}
      <div className="flex items-start justify-between gap-3">
        <div>
          <h1 className="text-gray-900" style={{ fontSize: '1.25rem', fontWeight: 600 }}>
            {ru ? 'Аналитика сообщества' : 'Community Intelligence'}
          </h1>
          <p className="text-xs text-gray-500 mt-0.5">
            {ru
              ? `30 виджетов · ${data.communityBrief.messagesAnalyzed.toLocaleString('ru-RU')} сообщений · ${data.communityBrief.updatedMinutesAgo} мин назад`
              : `30 widgets · ${data.communityBrief.messagesAnalyzed.toLocaleString()} analyzed · ${data.communityBrief.updatedMinutesAgo} min ago`}
          </p>
        </div>
        <div className="flex items-center gap-2 flex-shrink-0">
          <button
            onClick={() => setOpenTiers(Object.fromEntries(tiers.map(t => [t.id, true])))}
            className="text-xs px-2.5 py-1.5 bg-gray-100 text-gray-600 rounded-lg hover:bg-gray-200 transition-colors whitespace-nowrap"
          >
            {ru ? 'Раскрыть' : 'Expand'}
          </button>
          <button
            onClick={() => setOpenTiers(Object.fromEntries(tiers.map(t => [t.id, false])))}
            className="text-xs px-2.5 py-1.5 bg-gray-100 text-gray-600 rounded-lg hover:bg-gray-200 transition-colors whitespace-nowrap"
          >
            {ru ? 'Свернуть' : 'Collapse'}
          </button>
        </div>
      </div>

      {/* ═══ TIER 1 ═══ */}
      <TierHeader tier={tiers[0]} isOpen={openTiers.pulse} onToggle={() => toggleTier('pulse')} />
      {openTiers.pulse && (
        <div className="space-y-4 md:space-y-6">
          <CommunityBrief />
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4 md:gap-6">
            <CommunityHealthScore />
            <TrendingTopicsFeed />
          </div>
        </div>
      )}

      {/* ═══ TIER 2 ═══ */}
      <TierHeader tier={tiers[1]} isOpen={openTiers.topics} onToggle={() => toggleTier('topics')} />
      {openTiers.topics && (
        <div className="space-y-4 md:space-y-6">
          <TopicLandscape />
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4 md:gap-6">
            <ConversationTrends />
            <QuestionCloud />
          </div>
          <ContentEngagementHeatmap />
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4 md:gap-6">
            <QuestionAnswerGap />
            <TopicLifecycle />
          </div>
        </div>
      )}

      {/* ═══ TIER 3 ═══ */}
      <TierHeader tier={tiers[2]} isOpen={openTiers.problems} onToggle={() => toggleTier('problems')} />
      {openTiers.problems && (
        <div className="space-y-4 md:space-y-6">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4 md:gap-6">
            <ProblemTracker />
            <ServiceGapDetector />
          </div>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4 md:gap-6">
            <SatisfactionByArea />
            <MoodOverTime />
          </div>
          <EmotionalUrgencyIndex />
        </div>
      )}

      {/* ═══ TIER 4 ═══ */}
      <TierHeader tier={tiers[3]} isOpen={openTiers.channels} onToggle={() => toggleTier('channels')} />
      {openTiers.channels && (
        <div className="space-y-4 md:space-y-6">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4 md:gap-6">
            <TopChannels />
            <KeyVoices />
          </div>
          <RecommendationTracker />
          <InformationVelocity />
        </div>
      )}

      {/* ═══ TIER 5 ═══ */}
      <TierHeader tier={tiers[4]} isOpen={openTiers.who} onToggle={() => toggleTier('who')} />
      {openTiers.who && (
        <div className="space-y-4 md:space-y-6">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4 md:gap-6">
            <PersonaGallery />
            <InterestRadar />
          </div>
        </div>
      )}

      {/* ═══ TIER 6 ═══ */}
      <TierHeader tier={tiers[5]} isOpen={openTiers.growth} onToggle={() => toggleTier('growth')} />
      {openTiers.growth && (
        <div className="space-y-4 md:space-y-6">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4 md:gap-6">
            <CommunityGrowthFunnel />
            <RetentionRiskGauge />
          </div>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4 md:gap-6">
            <DecisionStageTracker />
            <EmergingInterests />
          </div>
          <NewVsReturningVoice />
        </div>
      )}

      {/* ═══ TIER 7 ═══ */}
      <TierHeader tier={tiers[6]} isOpen={openTiers.business} onToggle={() => toggleTier('business')} />
      {openTiers.business && (
        <div className="space-y-4 md:space-y-6">
          <BusinessOpportunityTracker />
          <JobMarketPulse />
        </div>
      )}

      {/* ═══ TIER 8 ═══ */}
      <TierHeader tier={tiers[7]} isOpen={openTiers.analytics} onToggle={() => toggleTier('analytics')} />
      {openTiers.analytics && (
        <div className="space-y-4 md:space-y-6">
          <WeekOverWeekShifts />
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4 md:gap-6">
            <SentimentByTopic />
            <ContentPerformance />
          </div>
        </div>
      )}

      <div className="h-4" />
    </div>
  );
}