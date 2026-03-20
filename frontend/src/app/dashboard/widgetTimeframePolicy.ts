export interface WidgetTimeframePolicy {
  minDays: number;
  rangeAware: boolean;
  lockedReason: 'minimum_window' | 'not_connected';
}

export const WIDGET_TIMEFRAME_POLICY: Record<string, WidgetTimeframePolicy> = {
  community_brief: { minDays: 1, rangeAware: true, lockedReason: 'minimum_window' },
  community_health_score: { minDays: 1, rangeAware: true, lockedReason: 'minimum_window' },
  trending_topics_feed: { minDays: 1, rangeAware: true, lockedReason: 'minimum_window' },
  topic_landscape: { minDays: 15, rangeAware: true, lockedReason: 'minimum_window' },
  conversation_trends: { minDays: 15, rangeAware: true, lockedReason: 'minimum_window' },
  question_cloud: { minDays: 15, rangeAware: true, lockedReason: 'minimum_window' },
  topic_lifecycle: { minDays: 15, rangeAware: true, lockedReason: 'minimum_window' },
  problem_tracker: { minDays: 15, rangeAware: true, lockedReason: 'minimum_window' },
  service_gap_detector: { minDays: 15, rangeAware: true, lockedReason: 'minimum_window' },
  satisfaction_by_area: { minDays: 15, rangeAware: true, lockedReason: 'minimum_window' },
  mood_over_time: { minDays: 15, rangeAware: true, lockedReason: 'minimum_window' },
  emotional_urgency_index: { minDays: 15, rangeAware: false, lockedReason: 'not_connected' },
  top_channels: { minDays: 15, rangeAware: true, lockedReason: 'minimum_window' },
  key_voices: { minDays: 15, rangeAware: true, lockedReason: 'minimum_window' },
  recommendation_tracker: { minDays: 15, rangeAware: false, lockedReason: 'not_connected' },
  information_velocity: { minDays: 1, rangeAware: true, lockedReason: 'minimum_window' },
  persona_gallery: { minDays: 15, rangeAware: true, lockedReason: 'minimum_window' },
  interest_radar: { minDays: 15, rangeAware: true, lockedReason: 'minimum_window' },
  community_growth_funnel: { minDays: 15, rangeAware: true, lockedReason: 'minimum_window' },
  retention_risk_gauge: { minDays: 15, rangeAware: true, lockedReason: 'minimum_window' },
  decision_stage_tracker: { minDays: 15, rangeAware: true, lockedReason: 'minimum_window' },
  emerging_interests: { minDays: 15, rangeAware: true, lockedReason: 'minimum_window' },
  new_vs_returning_voice: { minDays: 15, rangeAware: true, lockedReason: 'minimum_window' },
  business_opportunity_tracker: { minDays: 15, rangeAware: true, lockedReason: 'minimum_window' },
  job_market_pulse: { minDays: 15, rangeAware: true, lockedReason: 'minimum_window' },
  week_over_week_shifts: { minDays: 15, rangeAware: true, lockedReason: 'minimum_window' },
  sentiment_by_topic: { minDays: 15, rangeAware: true, lockedReason: 'minimum_window' },
  content_performance: { minDays: 15, rangeAware: true, lockedReason: 'minimum_window' },
};
