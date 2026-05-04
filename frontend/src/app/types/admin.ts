export interface AdminWidgetSetting {
  enabled: boolean;
}

export interface AnalysisLensExample {
  input_excerpt: string;
  bad_output_example: string;
  good_output_example: string;
  reason: string;
}

export interface AnalysisLensDefinition {
  id: string;
  version: number;
  name: string;
  analyst_role: string;
  objective: string;
  relevance_definition: string;
  priority_signals: string[];
  topic_quality_rules: {
    prefer: string[];
    avoid_generic: string[];
    good_examples: string[];
  };
  confidence_threshold: number;
  few_shot_examples: AnalysisLensExample[];
}

export interface AdminRuntimeConfig {
  openaiModel: string;
  questionBriefsModel: string;
  behavioralBriefsModel: string;
  opportunityBriefsModel: string;
  topicOverviewsModel: string;
  questionBriefsPromptVersion: string;
  behavioralBriefsPromptVersion: string;
  opportunityBriefsPromptVersion: string;
  topicOverviewsPromptVersion: string;
  topicOverviewsRefreshMinutes: string;
  aiPostPromptStyle: 'compact' | 'full';
  analysisLensIds: string[];
  featureQuestionBriefsAi: boolean;
  featureBehavioralBriefsAi: boolean;
  featureOpportunityBriefsAi: boolean;
  featureTopicOverviewsAi: boolean;
}

export interface AdminConfig {
  widgets: Record<string, AdminWidgetSetting>;
  prompts: Record<string, string>;
  promptDefaults?: Record<string, string>;
  runtime: AdminRuntimeConfig;
  analysisLensCatalog?: AnalysisLensDefinition[];
  analysisLensSelectionSource?: 'seeded_default' | 'operator';
}

export interface AdminConfigEnvelope extends Partial<AdminConfig> {
  warning?: string | null;
}

export interface AdminConfigResult {
  config: AdminConfig;
  warning: string | null;
}

export interface AdminConfigPatch {
  widgets?: Record<string, AdminWidgetSetting>;
  prompts?: Record<string, string>;
  runtime?: Partial<AdminRuntimeConfig>;
}
