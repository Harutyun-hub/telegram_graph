// ================================================================
// CENTRALIZED DATA TYPES FOR ALL WIDGETS AND PAGES
// ================================================================
// Every widget and page uses these types. When connecting a real
// backend (Neo4j API, etc.), the API responses should conform to
// these interfaces. The DataContext will then serve real data
// instead of mock data — widgets don't need to change.
// ================================================================

import type { Lang } from '../contexts/LanguageContext';

// ── Helper: Bilingual data wrapper ──
// Most data comes in { en: T[], ru: T[] } format
export type BilingualData<T> = Record<Lang, T[]>;
export type BilingualValue<T> = Record<Lang, T>;

// ── Tier 1: Community Pulse ──

export interface HealthHistoryPoint {
  time: string;
  score: number;
}

export interface HealthComponent {
  label: string;
  value: number;
  trend: number;
  desc: string;
}

export interface CommunityHealthData {
  currentScore: number;
  weekAgoScore: number;
  history: HealthHistoryPoint[];
  components: BilingualData<HealthComponent>;
}

export interface TrendingTopic {
  id: number;
  topic: string;
  sourceTopic?: string;
  mentions: number;
  trend: number;
  deltaMentions?: number;
  trendReliable?: boolean;
  growthSupport?: number;
  category: string;
  sentiment: string;
  sampleQuote: string;
  sampleEvidenceId?: string;
  evidence?: Array<{
    id: string;
    kind: string;
    text: string;
    channel: string;
    userId?: string;
    postedAt: string;
  }>;
  evidenceCount?: number;
  distinctUsers?: number;
  distinctChannels?: number;
  distinctPosts?: number;
  distinctComments?: number;
  qualityTier?: 'high' | 'medium' | 'low';
}

export interface CommunityBriefData {
  messagesAnalyzed: number;
  updatedMinutesAgo: number;
  postsAnalyzed24h: number;
  commentScopesAnalyzed24h: number;
  positiveIntentPct24h: number;
  negativeIntentPct24h: number;
  mainBrief: BilingualValue<string>;
  expandedBrief: BilingualValue<string[]>;
}

// ── Tier 2: Strategic / Topics ──

export interface TopicBubble {
  name: string;
  sourceTopic?: string;
  value: number;
  category: string;
  color: string;
  growth: number;
  growthReliable?: boolean;
  evidenceCount?: number;
  sampleQuote?: string;
}

export interface TrendLine {
  key: string;
  label: string;
  color: string;
  current: number;
  change: number;
}

export interface TrendDataPoint {
  week: string;
  [key: string]: number | string;
}

export interface QuestionCategory {
  category: string;
  color: string;
  questions: { q: string; topic?: string; preview?: string; count: number; answered: boolean; coveragePct?: number; lowEvidence?: boolean; evidenceId?: string }[];
}

export interface QuestionBriefEvidence {
  id: string;
  quote: string;
  channel: string;
  timestamp: string;
  kind: string;
}

export interface QuestionBrief {
  id: string;
  topic: string;
  sourceTopic?: string;
  category: string;
  question: string;
  summary: string;
  title?: string;
  brief?: string;
  confidence: 'high' | 'medium' | 'low';
  confidenceScore: number;
  status: 'needs_guide' | 'partially_answered' | 'well_covered';
  resolvedPct: number;
  demandSignals: {
    messages: number;
    uniqueUsers: number;
    channels: number;
    trend7dPct: number;
  };
  sampleEvidenceId?: string;
  latestAt?: string;
  evidence: QuestionBriefEvidence[];
}

export interface ProblemBriefCard {
  id: string;
  topic: string;
  sourceTopic?: string;
  category: string;
  problem: string;
  summary: string;
  severity: 'critical' | 'high' | 'medium' | 'low';
  confidence: 'high' | 'medium' | 'low';
  confidenceScore: number;
  demandSignals: {
    messages: number;
    uniqueUsers: number;
    channels: number;
    trend7dPct: number;
  };
  sampleEvidenceId?: string;
  latestAt?: string;
  evidence: QuestionBriefEvidence[];
}

export interface ServiceGapBriefCard {
  id: string;
  topic: string;
  sourceTopic?: string;
  category: string;
  serviceNeed: string;
  unmetReason: string;
  urgency: 'critical' | 'high' | 'medium' | 'low';
  unmetPct: number;
  confidence: 'high' | 'medium' | 'low';
  confidenceScore: number;
  demandSignals: {
    messages: number;
    uniqueUsers: number;
    channels: number;
    trend7dPct: number;
  };
  sampleEvidenceId?: string;
  latestAt?: string;
  evidence: QuestionBriefEvidence[];
}

export interface QAGapItem {
  topic: string;
  asked: number;
  rate: number;
  lowEvidence?: boolean;
}

export interface LifecycleTopic {
  name: string;
  sourceTopic?: string;
  daysActive: number;
  ageWeeks?: number;
  momentum: number;
  volume: number;
  support?: number;
  confidence?: number;
  rollingGrowth?: number;
  summary?: string;
  topChannels?: string[];
  evidence?: { text: string; channel: string; timestamp: string }[];
}

export interface LifecycleStage {
  stage: string;
  color: string;
  bgColor: string;
  borderColor: string;
  textColor: string;
  desc: string;
  topics: LifecycleTopic[];
}

export interface HeatmapData {
  contentTypes: string[];
  topicCols: string[];
  engagement: Record<string, Record<string, number>>;
}

// ── Tier 3: Behavioral / Problems ──

export interface Problem {
  name: string;
  sourceTopic?: string;
  mentions: number;
  severity: string;
  trend: number;
  quote: string;
  trendReliable?: boolean;
  evidenceCount?: number;
}

export interface ProblemCategory {
  category: string;
  problems: Problem[];
}

export interface ServiceGap {
  service: string;
  sourceTopic?: string;
  demand: number;
  supply: string;
  gap: number;
  growth: number;
  growthReliable?: boolean;
  evidenceCount?: number;
  /**
   * Stable semantic level for supply — replaces brittle localized string matching in ServiceGapDetector.
   * Values: 'none' | 'very_low' | 'low' | 'moderate' | 'adequate'
   */
  supplyLevel: 'none' | 'very_low' | 'low' | 'moderate' | 'adequate';
}

export interface SatisfactionArea {
  area: string;
  satisfaction: number;
  mentions: number;
  trend: number;
  emoji: string;
}

export interface MoodDataPoint {
  week: string;
  excited: number;
  satisfied: number;
  neutral: number;
  frustrated: number;
  anxious: number;
}

export interface MoodConfig {
  key: string;
  label: string;
  color: string;
  emoji: string;
  /** ✅ ADDED: polarity allows MoodOverTime to compute positive/negative share generically */
  polarity: 'positive' | 'negative' | 'neutral';
}

export interface UrgencySignal {
  message: string;
  topic: string;
  urgency: string;
  count: number;
  action: string;
}

// ── Tier 4: Network / Channels ──

export interface CommunityChannel {
  name: string;
  type: string;
  members: number;
  dailyMessages: number;
  engagement: number;
  growth: number;
  topTopicEN: string;
  topTopicRU: string;
}

export interface KeyVoice {
  name: string;
  role: string;
  topics: string[];
  postsPerWeek: number;
  replyRate: number;
  topChannels?: string[];
}

export interface HourlyActivityPoint {
  hour: string;
  messages: number;
}

export interface WeeklyActivityPoint {
  day: string;
  dayEN: string;
  messages: number;
}

export interface Recommendation {
  item: string;
  category: string;
  mentions: number;
  rating: number;
  sentiment: string;
}

export interface NewcomerJourneyStage {
  stage: string;
  questions: string[];
  volume: number;
  resolved: number;
}

export interface ViralTopic {
  topic: string;
  originator: string;
  spreadHours: number;
  channelsReached: number;
  amplifiers: string[];
  totalReach: number;
  velocity: string;
}

// ── Tier 5: Psychographic ──

export interface Persona {
  name: string;
  size: number;
  count: number;
  color: string;
  profile: string;
  needs: string;
  interests: string;
  pain: string;
  desc: string;
}

export interface InterestItem {
  interest: string;
  score: number;
}

export interface OriginCity {
  city: string;
  cityEN: string;
  count: number;
  pct: number;
  color: string;
}

export interface IntegrationDataPoint {
  month: string;
  learning: number;
  bilingual: number;
  russianOnly: number;
  integrated: number;
}

/** Drives the IntegrationSpectrum chart generically — same pattern as MoodConfig/moodConfig. */
export interface IntegrationSeriesConfig {
  /** Must match a key in IntegrationDataPoint (excluding 'month'). */
  key: keyof Omit<IntegrationDataPoint, 'month'>;
  color: string;
  label: string;    // EN display label
  labelRu: string;  // RU display label
  /** 'positive' = integration progress (good when growing), 'negative' = non-integration (good when shrinking) */
  polarity: 'positive' | 'negative' | 'neutral';
}

export interface IntegrationLevel {
  level: string;
  pct: number;
  color: string;
  desc: string;
}

// ── Tier 6: Predictive ──

export interface EmergingInterest {
  topic: string;
  firstSeen: string;
  growthRate: number;
  currentVolume: number;
  originChannel: string;
  mood: string;
  opportunity: string;
  emergenceScore?: number;
}

export interface RetentionFactor {
  factor: string;
  score: number;
  weight: number;
  overallScore?: number;
  support?: number;
  lift?: number;
}

export interface ChurnSignal {
  signal: string;
  count: number;
  trend: number;
  severity: string;
  baseline?: number;
  rate?: number;
}

export interface GrowthFunnelStage {
  stage: string;
  count: number;
  pct: number;
  color: string;
  /**
   * Stable semantic identifier for the funnel stage.
   * Replaces fragile positional-index lookups (growthFunnel[1], growthFunnel[3]).
   * Values: 'all' | 'reads' | 'asks' | 'helps' | 'contributes' | 'leads'
   */
  role: 'all' | 'reads' | 'asks' | 'helps' | 'contributes' | 'leads';
}

export interface DecisionStage {
  stage: string;
  count: number;
  pct: number;
  trend: number;
  color: string;
  needs: string;
}

export interface VoiceDataPoint {
  week: string;
  newVoices: number;
  returning: number;
}

export interface TopNewVoiceTopic {
  topic: string;
  newVoices: number;
  pct: number;
}

// ── Tier 7: Actionable ──

export interface BusinessOpportunity {
  need: string;
  mentions: number;
  growth: number;
  sector: string;
  readiness: string;
  sampleQuote: string;
  revenue: string;
}

export interface BusinessOpportunityBriefCard {
  id: string;
  topic: string;
  sourceTopic?: string;
  category: string;
  opportunity: string;
  summary: string;
  deliveryModel: 'service' | 'product' | 'marketplace' | 'content' | 'community_program';
  readiness: 'pilot_ready' | 'validate_now' | 'watchlist';
  confidence: 'high' | 'medium' | 'low';
  confidenceScore: number;
  demandSignals: {
    messages: number;
    uniqueUsers: number;
    channels: number;
    trend7dPct: number;
  };
  sampleEvidenceId?: string;
  latestAt?: string;
  evidence: QuestionBriefEvidence[];
}

export interface JobSeekingItem {
  role: string;
  pct: number;
  count: number;
  evidence?: JobEvidence[];
}

export interface JobTrend {
  trend: string;
  type: string;
}

export interface JobEvidence {
  id: string;
  text: string;
  kind: 'post' | 'comment';
  topic: string;
  sourceTopic?: string;
  channel?: string;
  postedAt?: string;
}

export interface HousingItem {
  type: string;
  avgPrice: string;
  trend: number;
  satisfaction: number;
  volume: number;
}

export interface HousingHotTopic {
  topic: string;
  count: number;
  sentiment: string;
}

// ── Tier 8: Comparative ──

export interface WeeklyShiftItem {
  metricKey?: string;
  metric: string;
  current: number;
  previous: number;
  unit: string;
  category: string;
  /** When true, a decrease is "good" (e.g. churn signals). Replaces brittle string-matching in the widget. */
  isInverse?: boolean;
}

export interface SentimentByTopicItem {
  topic: string;
  positive: number;
  neutral: number;
  negative: number;
  volume: number;
}

export interface TopPost {
  title: string;
  type: string;
  shares: number;
  reactions: number;
  comments: number;
  engagement: number;
}

export interface ContentTypePerf {
  type: string;
  avgEngagement: number;
  count: number;
}

export interface VitalityIndicator {
  indicator: string;
  score: number;
  trend: number;
  benchmark: string;
  emoji: string;
  /**
   * Stable semantic level — replaces brittle string-matching against localized benchmark labels.
   * 'excellent' → emerald, 'good' | 'above_avg' → blue, 'average' → gray, 'below_avg' | 'poor' → amber
   */
  benchmarkLevel: 'excellent' | 'good' | 'above_avg' | 'average' | 'below_avg' | 'poor';
}

// ── Pages: Topics ──

export interface TopicEvidence {
  id: string;
  type: string;
  author: string;
  channel: string;
  text: string;
  timestamp: string;
  reactions: number;
  replies: number;
}

export interface ChannelPost {
  id: string;
  author: string;
  text: string;
  timestamp: string;
  reactions: number;
  replies: number;
}

export interface AudienceMessage {
  id: string;
  text: string;
  channel: string;
  timestamp: string;
  reactions: number;
  replies: number;
}

export interface PaginatedFeed<T> {
  items: T[];
  total: number;
  page: number;
  size: number;
  hasMore: boolean;
  focusedItem?: T | null;
}

export interface TopicDetail {
  id: string;
  name: string;
  nameRu: string;
  sourceTopic?: string;
  topicGroup?: string;
  category: string;
  color: string;
  mentions: number;
  growth: number;
  currentMentions?: number;
  previousMentions?: number;
  deltaMentions?: number;
  trendReliable?: boolean;
  sampleEvidenceId?: string;
  sampleQuote?: string;
  evidenceCount?: number;
  distinctUsers?: number;
  distinctChannels?: number;
  sentiment: { positive: number; neutral: number; negative: number };
  weeklyData: { week: string; count: number; isoDate?: string }[];
  topChannels: string[];
  description: string;
  descriptionRu: string;
  evidence: TopicEvidence[];
  questionEvidence?: TopicEvidence[];
}

// ── Pages: Channels ──

export interface ChannelDetail {
  id: string;
  name: string;
  type: string;
  members: number;
  dailyMessages: number;
  engagement: number;
  growth: number;
  topTopic: string;
  description: string;
  weeklyData: { day: string; msgs: number }[];
  hourlyData: { hour: string; msgs: number }[];
  topTopics: { name: string; mentions: number; pct: number }[];
  sentimentBreakdown: { positive: number; neutral: number; negative: number };
  messageTypes: { type: string; count: number; pct: number }[];
  topVoices: { name: string; posts: number; helpScore: number }[];
  recentPosts: ChannelPost[];
}

// ── Pages: Audience ──

export type Gender = 'Male' | 'Female' | 'Unknown';

export interface AudienceMember {
  id: string;
  username: string;
  displayName: string;
  gender: Gender;
  age: string;
  origin: string;
  location: string;
  joinedDate: string;
  lastActive: string;
  totalMessages: number;
  totalReactions: number;
  helpScore: number;
  interests: string[];
  channels: { name: string; type: string; role: string; messageCount: number }[];
  topTopics: { name: string; count: number }[];
  sentiment: { positive: number; neutral: number; negative: number };
  activityData: { week: string; msgs: number }[];
  recentMessages: AudienceMessage[];
  persona: string;
  integrationLevel: string;
}

// ── Master Data Store Shape ──
// This is what DataContext provides. Each key maps to a widget or page.

export interface AppData {
  // Tier 1
  communityHealth: CommunityHealthData;
  trendingTopics: BilingualData<TrendingTopic>;
  trendingNewTopics: BilingualData<TrendingTopic>;
  communityBrief: CommunityBriefData;

  // Tier 2
  topicBubbles: BilingualData<TopicBubble>;
  trendLines: BilingualData<TrendLine>;
  trendData: TrendDataPoint[];
  heatmap: BilingualValue<HeatmapData>;
  questionCategories: BilingualData<QuestionCategory>;
  questionBriefs: BilingualData<QuestionBrief>;
  qaGap: BilingualData<QAGapItem>;
  lifecycleStages: BilingualData<LifecycleStage>;

  // Tier 3
  problemBriefs: BilingualData<ProblemBriefCard>;
  serviceGapBriefs: BilingualData<ServiceGapBriefCard>;
  problems: BilingualData<ProblemCategory>;
  serviceGaps: BilingualData<ServiceGap>;
  satisfactionAreas: BilingualData<SatisfactionArea>;
  moodData: MoodDataPoint[];
  moodConfig: BilingualData<MoodConfig>;
  urgencySignals: BilingualData<UrgencySignal>;

  // Tier 4
  communityChannels: CommunityChannel[];
  keyVoices: BilingualData<KeyVoice>;
  hourlyActivity: HourlyActivityPoint[];
  weeklyActivity: WeeklyActivityPoint[];
  recommendations: BilingualData<Recommendation>;
  newcomerJourney: BilingualData<NewcomerJourneyStage>;
  viralTopics: BilingualData<ViralTopic>;

  // Tier 5
  personas: BilingualData<Persona>;
  interests: BilingualData<InterestItem>;
  origins: OriginCity[];
  integrationData: IntegrationDataPoint[];
  integrationLevels: BilingualData<IntegrationLevel>;
  /** Config for the IntegrationSpectrum area chart — replaces hardcoded dataKey strings. */
  integrationSeriesConfig: IntegrationSeriesConfig[];

  // Tier 6
  emergingInterests: BilingualData<EmergingInterest>;
  retentionFactors: BilingualData<RetentionFactor>;
  churnSignals: BilingualData<ChurnSignal>;
  growthFunnel: BilingualData<GrowthFunnelStage>;
  decisionStages: BilingualData<DecisionStage>;
  voiceData: VoiceDataPoint[];
  topNewTopics: BilingualData<TopNewVoiceTopic>;

  // Tier 7
  businessOpportunities: BilingualData<BusinessOpportunity>;
  businessOpportunityBriefs: BilingualData<BusinessOpportunityBriefCard>;
  jobSeeking: BilingualData<JobSeekingItem>;
  jobTrends: BilingualData<JobTrend>;
  housingData: BilingualData<HousingItem>;
  housingHotTopics: BilingualData<HousingHotTopic>;

  // Tier 8
  weeklyShifts: BilingualData<WeeklyShiftItem>;
  sentimentByTopic: BilingualData<SentimentByTopicItem>;
  topPosts: BilingualData<TopPost>;
  contentTypePerformance: BilingualData<ContentTypePerf>;
  vitalityIndicators: BilingualData<VitalityIndicator>;

}

// ── Sources Page ──

export type ChannelStatus = 'active' | 'paused' | 'error' | 'pending';

export interface TrackedChannel {
  id: string;
  username: string;       // @channel_name
  title: string;          // Display name
  description?: string;
  members: number;
  dailyMessages: number;
  status: ChannelStatus;
  addedDate: string;      // ISO date
  lastSync: string;       // relative time string
  type: 'channel' | 'group' | 'supergroup';
  language: string;
  growth: number;         // % weekly growth
}
