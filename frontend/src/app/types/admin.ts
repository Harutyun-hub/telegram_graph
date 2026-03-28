export interface AdminWidgetSetting {
  enabled: boolean;
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
