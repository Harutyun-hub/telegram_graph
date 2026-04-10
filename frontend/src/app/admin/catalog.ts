import type { AdminConfig, AdminRuntimeConfig } from '../types/admin';

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
  featureQuestionBriefsAi: true,
  featureBehavioralBriefsAi: true,
  featureOpportunityBriefsAi: true,
  featureTopicOverviewsAi: true,
};

export function createDefaultAdminConfig(): AdminConfig {
  const prompts = Object.fromEntries(ADMIN_PROMPT_DEFINITIONS.map((prompt) => [prompt.key, '']));
  return {
    widgets: Object.fromEntries(ADMIN_WIDGET_DEFINITIONS.map((widget) => [widget.id, { enabled: true }])),
    prompts,
    promptDefaults: { ...prompts },
    runtime: { ...DEFAULT_ADMIN_RUNTIME },
  };
}
