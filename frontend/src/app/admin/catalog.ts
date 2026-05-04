import type { AdminConfig, AdminRuntimeConfig, AnalysisLensDefinition } from '../types/admin';

export interface AdminTierDefinition {
  id: string;
  labelEn: string;
  labelRu: string;
}

export interface AdminWidgetDefinition {
  id: string;
  tierId: string;
  labelEn: string;
  labelRu: string;
}

export interface AdminPromptDefinition {
  key: string;
  groupId: string;
  labelEn: string;
  labelRu: string;
  descriptionEn: string;
  descriptionRu: string;
}

export interface AdminPromptGroup {
  id: string;
  labelEn: string;
  labelRu: string;
  descriptionEn?: string;
  descriptionRu?: string;
  badgeEn?: string;
  badgeRu?: string;
}

export const ADMIN_TIERS: AdminTierDefinition[] = [
  { id: 'pulse', labelEn: 'Community Pulse', labelRu: 'Пульс сообщества' },
  { id: 'topics', labelEn: 'What People Talk About', labelRu: 'О чём говорят' },
  { id: 'problems', labelEn: 'Problems & Satisfaction', labelRu: 'Проблемы и удовлетворённость' },
  { id: 'channels', labelEn: 'Channels, Voices & Activity', labelRu: 'Каналы, голоса и активность' },
  { id: 'who', labelEn: 'Who Are They', labelRu: 'Кто они' },
  { id: 'growth', labelEn: 'Growth, Retention & Journey', labelRu: 'Рост, удержание и путь' },
  { id: 'business', labelEn: 'Business & Opportunity Intelligence', labelRu: 'Бизнес-разведка и возможности' },
  { id: 'analytics', labelEn: 'Performance & Analytics', labelRu: 'Эффективность и аналитика' },
];

export const ADMIN_WIDGET_DEFINITIONS = [
  { id: 'community_brief', tierId: 'pulse', labelEn: 'Community Brief', labelRu: 'Сводка сообщества' },
  { id: 'community_health_score', tierId: 'pulse', labelEn: 'Community Health Score', labelRu: 'Индекс здоровья сообщества' },
  { id: 'trending_topics_feed', tierId: 'pulse', labelEn: 'Trending Topics Feed', labelRu: 'Лента трендовых тем' },
  { id: 'topic_landscape', tierId: 'topics', labelEn: 'Topic Landscape', labelRu: 'Ландшафт тем' },
  { id: 'conversation_trends', tierId: 'topics', labelEn: 'Conversation Trends', labelRu: 'Тренды разговоров' },
  { id: 'question_cloud', tierId: 'topics', labelEn: 'Question Cloud', labelRu: 'Облако вопросов' },
  { id: 'topic_lifecycle', tierId: 'topics', labelEn: 'Topic Lifecycle', labelRu: 'Жизненный цикл тем' },
  { id: 'problem_tracker', tierId: 'problems', labelEn: 'Problem Tracker', labelRu: 'Трекер проблем' },
  { id: 'service_gap_detector', tierId: 'problems', labelEn: 'Service Gap Detector', labelRu: 'Детектор сервисных пробелов' },
  { id: 'satisfaction_by_area', tierId: 'problems', labelEn: 'Satisfaction by Area', labelRu: 'Удовлетворённость по сферам' },
  { id: 'mood_over_time', tierId: 'problems', labelEn: 'Mood Over Time', labelRu: 'Динамика настроений' },
  { id: 'emotional_urgency_index', tierId: 'problems', labelEn: 'Emotional Urgency Index', labelRu: 'Индекс срочности' },
  { id: 'top_channels', tierId: 'channels', labelEn: 'Top Channels', labelRu: 'Топ каналов' },
  { id: 'key_voices', tierId: 'channels', labelEn: 'Key Voices', labelRu: 'Ключевые голоса' },
  { id: 'recommendation_tracker', tierId: 'channels', labelEn: 'Recommendation Tracker', labelRu: 'Трекер рекомендаций' },
  { id: 'information_velocity', tierId: 'channels', labelEn: 'Information Velocity', labelRu: 'Скорость информации' },
  { id: 'persona_gallery', tierId: 'who', labelEn: 'Persona Gallery', labelRu: 'Галерея персон' },
  { id: 'interest_radar', tierId: 'who', labelEn: 'Interest Radar', labelRu: 'Радар интересов' },
  { id: 'community_growth_funnel', tierId: 'growth', labelEn: 'Community Growth Funnel', labelRu: 'Воронка роста сообщества' },
  { id: 'retention_risk_gauge', tierId: 'growth', labelEn: 'Activity Continuity & Risk Signals', labelRu: 'Непрерывность активности и сигналы риска' },
  { id: 'decision_stage_tracker', tierId: 'growth', labelEn: 'Decision Stage Tracker', labelRu: 'Трекер стадий решения' },
  { id: 'emerging_interests', tierId: 'growth', labelEn: 'Emerging Interests', labelRu: 'Новые интересы' },
  { id: 'new_vs_returning_voice', tierId: 'growth', labelEn: 'New vs Returning Voice', labelRu: 'Новые и возвращающиеся голоса' },
  { id: 'business_opportunity_tracker', tierId: 'business', labelEn: 'Business Opportunity Tracker', labelRu: 'Трекер возможностей для бизнеса' },
  { id: 'job_market_pulse', tierId: 'business', labelEn: 'Job Market Pulse', labelRu: 'Пульс рынка труда' },
  { id: 'week_over_week_shifts', tierId: 'analytics', labelEn: 'Week-over-Week Shifts', labelRu: 'Изменения неделя к неделе' },
  { id: 'sentiment_by_topic', tierId: 'analytics', labelEn: 'Sentiment by Topic', labelRu: 'Настроения по темам' },
  { id: 'content_performance', tierId: 'analytics', labelEn: 'Content Performance', labelRu: 'Эффективность контента' },
] as const satisfies readonly AdminWidgetDefinition[];

export type AdminWidgetId = (typeof ADMIN_WIDGET_DEFINITIONS)[number]['id'];

export const ADMIN_PROMPT_GROUPS: AdminPromptGroup[] = [
  { id: 'extraction', labelEn: 'Extraction', labelRu: 'Извлечение' },
  { id: 'question_briefs', labelEn: 'Question Briefs', labelRu: 'Карточки вопросов' },
  { id: 'behavioral_briefs', labelEn: 'Behavioral Briefs', labelRu: 'Поведенческие карточки' },
  { id: 'opportunity_briefs', labelEn: 'Opportunity Briefs', labelRu: 'Карточки возможностей' },
  {
    id: 'topic_overviews',
    labelEn: 'Topic Overviews',
    labelRu: 'Обзоры тем',
    descriptionEn: 'Controls the AI summary card shown on each topic detail page.',
    descriptionRu: 'Управляет AI-сводкой, которая показывается на странице каждой темы.',
    badgeEn: 'Topic Page AI',
    badgeRu: 'AI темы',
  },
  { id: 'recommendation_briefs', labelEn: 'Recommendations', labelRu: 'Рекомендации' },
];

export const ADMIN_PROMPT_DEFINITIONS: AdminPromptDefinition[] = [
  {
    key: 'extraction.system_prompt',
    groupId: 'extraction',
    labelEn: 'Comment analysis system prompt',
    labelRu: 'Системный промпт анализа комментариев',
    descriptionEn: 'Main extraction prompt for grouped user messages.',
    descriptionRu: 'Основной промпт извлечения для групп сообщений пользователя.',
  },
  {
    key: 'extraction.strict_taxonomy_prompt',
    groupId: 'extraction',
    labelEn: 'Strict taxonomy prompt',
    labelRu: 'Промпт строгой таксономии',
    descriptionEn: 'Extra taxonomy rules appended in extraction v2 mode.',
    descriptionRu: 'Дополнительные правила таксономии для режима extraction v2.',
  },
  {
    key: 'extraction.post_system_prompt',
    groupId: 'extraction',
    labelEn: 'Post prompt',
    labelRu: 'Промпт для постов',
    descriptionEn: 'Full post analysis prompt used outside compact mode.',
    descriptionRu: 'Полный промпт анализа постов вне compact-режима.',
  },
  {
    key: 'extraction.post_system_prompt_compact',
    groupId: 'extraction',
    labelEn: 'Compact post prompt',
    labelRu: 'Компактный промпт для постов',
    descriptionEn: 'Default compact post prompt.',
    descriptionRu: 'Основной компактный промпт для постов.',
  },
  {
    key: 'extraction.post_batch_system_prompt_compact',
    groupId: 'extraction',
    labelEn: 'Batch post prompt',
    labelRu: 'Промпт пакетного анализа постов',
    descriptionEn: 'Prompt for multi-post batch analysis.',
    descriptionRu: 'Промпт для пакетного анализа нескольких постов.',
  },
  {
    key: 'question_briefs.triage_prompt',
    groupId: 'question_briefs',
    labelEn: 'Question triage prompt',
    labelRu: 'Промпт триажа вопросов',
    descriptionEn: 'Accepts or rejects question-card clusters.',
    descriptionRu: 'Принимает или отклоняет кластеры карточек вопросов.',
  },
  {
    key: 'question_briefs.synthesis_prompt',
    groupId: 'question_briefs',
    labelEn: 'Question synthesis prompt',
    labelRu: 'Промпт синтеза вопросов',
    descriptionEn: 'Builds the final question cards.',
    descriptionRu: 'Формирует финальные карточки вопросов.',
  },
  {
    key: 'behavioral_briefs.problem_prompt',
    groupId: 'behavioral_briefs',
    labelEn: 'Problem card prompt',
    labelRu: 'Промпт карточек проблем',
    descriptionEn: 'Creates Problem Tracker cards.',
    descriptionRu: 'Создаёт карточки трекера проблем.',
  },
  {
    key: 'behavioral_briefs.service_gap_prompt',
    groupId: 'behavioral_briefs',
    labelEn: 'Service gap prompt',
    labelRu: 'Промпт сервисных пробелов',
    descriptionEn: 'Detects grounded service-gap requests.',
    descriptionRu: 'Выявляет подтверждённые сервисные запросы.',
  },
  {
    key: 'behavioral_briefs.urgency_prompt',
    groupId: 'behavioral_briefs',
    labelEn: 'Urgency prompt',
    labelRu: 'Промпт срочности',
    descriptionEn: 'Creates emotional urgency cards.',
    descriptionRu: 'Создаёт карточки срочности.',
  },
  {
    key: 'opportunity_briefs.triage_prompt',
    groupId: 'opportunity_briefs',
    labelEn: 'Opportunity triage prompt',
    labelRu: 'Промпт триажа возможностей',
    descriptionEn: 'Accepts or rejects demand-led opportunity clusters.',
    descriptionRu: 'Принимает или отклоняет кластеры возможностей на основе спроса.',
  },
  {
    key: 'opportunity_briefs.synthesis_prompt',
    groupId: 'opportunity_briefs',
    labelEn: 'Opportunity synthesis prompt',
    labelRu: 'Промпт синтеза возможностей',
    descriptionEn: 'Builds the final business opportunity cards.',
    descriptionRu: 'Формирует финальные карточки бизнес-возможностей.',
  },
  {
    key: 'topic_overviews.synthesis_prompt',
    groupId: 'topic_overviews',
    labelEn: 'Topic overview prompt',
    labelRu: 'Промпт обзора тем',
    descriptionEn: 'Builds the AI overview shown on the topic detail page.',
    descriptionRu: 'Формирует AI-обзор, который показывается на странице темы.',
  },
  {
    key: 'recommendation_briefs.extraction_prompt',
    groupId: 'recommendation_briefs',
    labelEn: 'Recommendation extraction prompt',
    labelRu: 'Промпт извлечения рекомендаций',
    descriptionEn: 'Extracts explicit recommendations from messages.',
    descriptionRu: 'Извлекает явные рекомендации из сообщений.',
  },
];

export const DEFAULT_ADMIN_RUNTIME: AdminRuntimeConfig = {
  openaiModel: 'gpt-5.4-mini',
  questionBriefsModel: 'gpt-5.4-mini',
  behavioralBriefsModel: 'gpt-5.4-mini',
  opportunityBriefsModel: 'gpt-5.4-mini',
  topicOverviewsModel: 'gpt-5.4-mini',
  questionBriefsPromptVersion: 'qcards-v2',
  behavioralBriefsPromptVersion: 'behavior-v2',
  opportunityBriefsPromptVersion: 'opportunity-v1',
  topicOverviewsPromptVersion: 'topic-overview-v2',
  topicOverviewsRefreshMinutes: '120',
  aiPostPromptStyle: 'compact',
  analysisLensIds: ['finance_markets'],
  featureQuestionBriefsAi: true,
  featureBehavioralBriefsAi: true,
  featureOpportunityBriefsAi: true,
  featureTopicOverviewsAi: true,
};

export const DEFAULT_ANALYSIS_LENS_CATALOG: AnalysisLensDefinition[] = [
  {
    id: 'finance_markets',
    version: 1,
    name: 'Finance & Markets',
    analyst_role: 'Market intelligence analyst for financial services, brokers, trading platforms, and investment research teams.',
    objective: 'Identify market narratives, trading themes, investor education, product positioning, risk messaging, acquisition tactics, or competitor movement in financial markets.',
    relevance_definition: 'Relevant content explains how a tracked company discusses markets, educates traders, promotes financial products, reacts to macro events, or positions itself against competitors.',
    priority_signals: [
      'asset class or instrument focus',
      'market driver explanation',
      'volatility or risk narrative',
      'macro event interpretation',
      'trading education or webinar funnel',
      'platform feature or tool promotion',
    ],
    topic_quality_rules: {
      prefer: [
        'specific market narrative',
        'specific instrument plus driver',
        'specific education or acquisition theme',
        'specific competitor or product positioning',
      ],
      avoid_generic: ['finance', 'investing', 'trading', 'stocks', 'geopolitics', 'market analysis', 'technical analysis'],
      good_examples: [
        'oil volatility from Middle East risk',
        'DAX pressure from tariff concerns',
        'broker webinar acquisition',
        'platform tools for market research',
      ],
    },
    confidence_threshold: 0.7,
    few_shot_examples: [
      {
        input_excerpt: 'Analysts expect oil to swing as Middle East tensions raise supply risk.',
        bad_output_example: 'geopolitics',
        good_output_example: 'oil volatility from Middle East risk',
        reason: 'Names the instrument and driver instead of a broad world-news category.',
      },
    ],
  },
  {
    id: 'competitor_analysis',
    version: 1,
    name: 'Competitor Analysis',
    analyst_role: 'Competitive intelligence analyst tracking how companies position, promote, differentiate, and compete.',
    objective: 'Identify competitor strategy, messaging, offers, product claims, audience targeting, channel usage, campaign mechanics, or positioning changes.',
    relevance_definition: 'Relevant content explains what a tracked company is trying to sell, who it is targeting, how it differentiates itself, and what strategic moves it is making.',
    priority_signals: [
      'new product or feature promotion',
      'pricing, discount, or offer message',
      'brand positioning claim',
      'audience segment targeted',
      'CTA or conversion tactic',
      'trust, credibility, or proof point',
    ],
    topic_quality_rules: {
      prefer: [
        'specific positioning move',
        'specific campaign or offer',
        'specific audience strategy',
        'specific product claim',
      ],
      avoid_generic: ['marketing', 'business', 'campaign', 'brand', 'competition', 'promotion'],
      good_examples: [
        'zero-commission acquisition offer',
        'premium platform positioning',
        'beginner audience targeting',
        'trust-led broker messaging',
      ],
    },
    confidence_threshold: 0.7,
    few_shot_examples: [
      {
        input_excerpt: 'Open an account this week and trade US shares with zero commission.',
        bad_output_example: 'promotion',
        good_output_example: 'zero-commission acquisition offer',
        reason: 'Identifies the offer and conversion purpose.',
      },
    ],
  },
  {
    id: 'business_analysis',
    version: 1,
    name: 'Business Analysis',
    analyst_role: 'Business analyst focused on customer needs, value propositions, growth signals, operational themes, and market-facing strategy.',
    objective: 'Identify customer problems, business opportunities, value propositions, demand patterns, product-market fit, growth levers, or operational risks.',
    relevance_definition: 'Relevant content helps a decision-maker understand what customers care about, what the company emphasizes, what opportunity exists, or what risk may affect performance.',
    priority_signals: [
      'customer pain point',
      'customer motivation',
      'value proposition',
      'purchase or adoption trigger',
      'trust or credibility signal',
      'customer objection or friction',
    ],
    topic_quality_rules: {
      prefer: [
        'specific customer need',
        'specific business opportunity',
        'specific value proposition',
        'specific operational or trust signal',
      ],
      avoid_generic: ['business', 'customers', 'growth', 'strategy', 'opportunity', 'service'],
      good_examples: [
        'trust barrier for new traders',
        'education need before conversion',
        'mobile-first customer acquisition',
        'low-fee value proposition',
      ],
    },
    confidence_threshold: 0.7,
    few_shot_examples: [
      {
        input_excerpt: 'People keep asking whether the broker is safe before they deposit.',
        bad_output_example: 'customers',
        good_output_example: 'trust barrier for new traders',
        reason: 'Names the customer friction and affected segment.',
      },
    ],
  },
];

export function createDefaultAdminConfig(): AdminConfig {
  const prompts = Object.fromEntries(ADMIN_PROMPT_DEFINITIONS.map((prompt) => [prompt.key, '']));
  return {
    widgets: Object.fromEntries(ADMIN_WIDGET_DEFINITIONS.map((widget) => [widget.id, { enabled: true }])),
    prompts,
    promptDefaults: { ...prompts },
    runtime: { ...DEFAULT_ADMIN_RUNTIME },
    analysisLensCatalog: DEFAULT_ANALYSIS_LENS_CATALOG,
    analysisLensSelectionSource: 'seeded_default',
  };
}
